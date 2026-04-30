# Databricks notebook source
# MAGIC %md
# MAGIC # Observability Discovery Job
# MAGIC
# MAGIC Discovers MLflow traces across all workspaces in the account using the
# MAGIC Databricks SDK's `WorkspaceClient(host=remote_host)`. The notebook
# MAGIC runner (account admin) can authenticate against any workspace.
# MAGIC
# MAGIC Also queries `system.mlflow.*` system tables for experiments and runs.
# MAGIC
# MAGIC **Data flow:** SDK cross-workspace APIs + System Tables → Delta tables → Lakebase (sync task)

# COMMAND ----------

# MAGIC %pip install databricks-sdk psycopg2-binary requests --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType,
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog name")
dbutils.widgets.text("schema", "", "Schema name")
dbutils.widgets.text("warehouse_id", "", "SQL warehouse ID for system table queries")
dbutils.widgets.text("account_id", "", "Databricks account ID")
dbutils.widgets.text("lakebase_dns", "", "Lakebase host (DNS)")
dbutils.widgets.text("lakebase_database", "control_plane", "Lakebase database name")
dbutils.widgets.text("lakebase_instance", "", "Lakebase instance name (Provisioned only)")
dbutils.widgets.text("lakebase_endpoint_path", "", "Lakebase endpoint path (Autoscaling only)")
dbutils.widgets.text("trace_retention_days", "90", "Trace cache retention window (days)")
# ── Tier 3 (parked) controls ─────────────────────────────────
# Cross-workspace MLflow REST fan-out is parked; default-off. The destination
# workspace's gateway rejects cross-workspace REST calls from Databricks
# Serverless / Apps Compute (HTTP 403 cert validation). Cross-workspace
# observability is served by the UC SQL paths in the sibling tasks
# (07_discover_uc_otel_traces, 08_discover_gateway_inference_logs) instead.
# Set this to "true" only for experimentation when platform constraints lift.
dbutils.widgets.text("enable_cross_workspace_rest_fanout", "false",
                     "Tier 3 prototype — default false. See module docstring.")
dbutils.widgets.text("discovery_sp_secret_scope", "", "Tier 3 only: secret scope with SP creds.")
dbutils.widgets.text("narrow_test_workspace_id", "", "Tier 3 only: limit fan-out to this workspace_id.")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
ACCOUNT_ID = dbutils.widgets.get("account_id")
LAKEBASE_DNS = dbutils.widgets.get("lakebase_dns")
LAKEBASE_DATABASE = dbutils.widgets.get("lakebase_database")
LAKEBASE_INSTANCE = dbutils.widgets.get("lakebase_instance")
LAKEBASE_ENDPOINT_PATH = dbutils.widgets.get("lakebase_endpoint_path")

if ACCOUNT_ID:
    os.environ["DATABRICKS_ACCOUNT_ID"] = ACCOUNT_ID
TRACES_TABLE = f"{CATALOG}.{SCHEMA}.observability_traces"
TRACE_DETAILS_TABLE = f"{CATALOG}.{SCHEMA}.observability_trace_details"

try:
    RETENTION_DAYS = max(1, int(dbutils.widgets.get("trace_retention_days") or "90"))
except ValueError:
    RETENTION_DAYS = 90
RETENTION_CUTOFF_MS = int((datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)).timestamp() * 1000)

CROSS_WS_FANOUT_ENABLED = (
    (dbutils.widgets.get("enable_cross_workspace_rest_fanout") or "false").strip().lower()
    in ("true", "1", "yes", "on")
)
SP_SECRET_SCOPE = dbutils.widgets.get("discovery_sp_secret_scope") or ""
NARROW_TEST_WS = (dbutils.widgets.get("narrow_test_workspace_id") or "").strip()

# Tier 3 SP creds are only relevant when cross-workspace fan-out is explicitly
# enabled. Skip the secret-scope read otherwise to keep the run output clean.
SP_CLIENT_ID = ""
SP_CLIENT_SECRET = ""
if CROSS_WS_FANOUT_ENABLED and SP_SECRET_SCOPE:
    try:
        SP_CLIENT_ID = dbutils.secrets.get(scope=SP_SECRET_SCOPE, key="client_id")
        SP_CLIENT_SECRET = dbutils.secrets.get(scope=SP_SECRET_SCOPE, key="client_secret")
        print(f"  Loaded discovery SP creds from secret scope '{SP_SECRET_SCOPE}'")
    except Exception as exc:
        print(f"  WARNING: could not load SP creds from '{SP_SECRET_SCOPE}': {exc}")

if not CATALOG or not SCHEMA:
    raise ValueError(f"catalog and schema required (got {CATALOG!r}, {SCHEMA!r})")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Target: {TRACES_TABLE}")
print(f"Detail target: {TRACE_DETAILS_TABLE}")
print(f"Retention: {RETENTION_DAYS} days (cutoff_ms={RETENTION_CUTOFF_MS})")
print(f"Warehouse: {WAREHOUSE_ID}")
print(f"Cross-workspace REST fan-out (Tier 3): {'ENABLED (experimental)' if CROSS_WS_FANOUT_ENABLED else 'disabled (default — local workspace only)'}")
if CROSS_WS_FANOUT_ENABLED:
    print(f"  SP auth: {'enabled' if SP_CLIENT_ID else 'disabled'}")
    print(f"  Narrow test workspace: {NARROW_TEST_WS or '(none, full fan-out)'}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table Schema

# COMMAND ----------

TRACES_SCHEMA = StructType([
    StructField("request_id", StringType(), False),
    StructField("workspace_id", StringType(), False),
    StructField("experiment_id", StringType(), True),
    StructField("trace_name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("request_time", StringType(), True),
    StructField("execution_duration", LongType(), True),
    StructField("model_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("trace_user", StringType(), True),
    StructField("source", StringType(), True),
    StructField("tags", StringType(), True),
    StructField("data_source", StringType(), True),
    StructField("discovered_at", TimestampType(), False),
])

TRACE_DETAILS_SCHEMA = StructType([
    StructField("workspace_id",  StringType(), False),
    StructField("request_id",    StringType(), False),
    StructField("experiment_id", StringType(), True),
    StructField("trace_info",    StringType(), True),
    StructField("trace_data",    StringType(), True),
    StructField("request_raw",   StringType(), True),
    StructField("response_raw",  StringType(), True),
    StructField("size_bytes",    LongType(),   True),
    StructField("source_type",   StringType(), True),
    StructField("cached_at",     TimestampType(), False),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discover Workspace Hosts
# MAGIC
# MAGIC Use the system table experiments to find which workspaces have MLflow data,
# MAGIC then resolve their hosts via the Accounts API.

# COMMAND ----------

def _execute_sql(sql):
    """Execute SQL via SQL Statements API."""
    if not WAREHOUSE_ID:
        print("  ⚠️  No warehouse ID — cannot query system tables")
        return []
    w = WorkspaceClient()
    body = {
        "warehouse_id": WAREHOUSE_ID,
        "statement": sql,
        "wait_timeout": "50s",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    }
    try:
        resp = w.api_client.do("POST", "/api/2.0/sql/statements", body=body)
    except Exception as exc:
        print(f"  ⚠️  SQL failed: {exc}")
        return []

    status = resp.get("status", {}).get("state", "")
    sid = resp.get("statement_id", "")
    if status in ("PENDING", "RUNNING") and sid:
        for _ in range(20):
            time.sleep(3)
            try:
                resp = w.api_client.do("GET", f"/api/2.0/sql/statements/{sid}")
            except Exception:
                continue
            status = resp.get("status", {}).get("state", "")
            if status not in ("PENDING", "RUNNING"):
                break
    if status != "SUCCEEDED":
        err = resp.get("status", {}).get("error", {})
        print(f"  ⚠️  SQL {status}: {err.get('message', '')[:200]}")
        return []
    cols = [c["name"] for c in resp.get("manifest", {}).get("schema", {}).get("columns", [])]
    return [dict(zip(cols, row)) for row in resp.get("result", {}).get("data_array", [])]

# COMMAND ----------

# Get workspace IDs that have experiments (from system tables)
print("▸ Finding workspaces with MLflow experiments ...")
ws_rows = _execute_sql("""
    SELECT DISTINCT CAST(workspace_id AS STRING) AS workspace_id
    FROM system.mlflow.experiments_latest
    WHERE delete_time IS NULL
""")
experiment_workspace_ids = set(r["workspace_id"] for r in ws_rows if r.get("workspace_id"))
print(f"  ✅ {len(experiment_workspace_ids)} workspaces have MLflow experiments")

# COMMAND ----------

# Resolve workspace hosts from Lakebase workspace_registry table
print("▸ Resolving workspace hosts from Lakebase workspace_registry ...")
workspace_hosts = {}
_host_resolution_log = []

import uuid
import psycopg2
import requests as _http

def _get_lakebase_conn():
    """Connect to Lakebase using SDK credentials. Supports both Autoscaling and Provisioned."""
    w = WorkspaceClient()
    me = w.current_user.me()
    pg_user = me.user_name
    pg_password = None
    host = w.config.host.rstrip("/")

    def _get_token():
        try:
            return w.config.authenticate().get("Authorization", "").replace("Bearer ", "")
        except Exception:
            return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()

    # Autoscaling Lakebase: explicit endpoint path
    if LAKEBASE_ENDPOINT_PATH:
        try:
            token = _get_token()
            resp = _http.post(f"{host}/api/2.0/postgres/credentials",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"endpoint": LAKEBASE_ENDPOINT_PATH})
            resp.raise_for_status()
            pg_password = resp.json().get("token", "")
            if pg_password:
                print(f"  Autoscaling credential OK (endpoint={LAKEBASE_ENDPOINT_PATH})")
        except Exception as e:
            print(f"  Autoscaling credential failed: {e}")

    # Provisioned SDK
    if not pg_password and LAKEBASE_INSTANCE and hasattr(w, "database"):
        try:
            creds = w.database.generate_database_credential(instance_names=[LAKEBASE_INSTANCE])
            pg_password = creds.token
        except Exception as e:
            print(f"  Provisioned SDK credential failed: {e}")

    # Fallback to Provisioned REST
    if not pg_password and LAKEBASE_INSTANCE:
        token = _get_token()
        resp = _http.post(f"{host}/api/2.0/database/credentials",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"instance_names": [LAKEBASE_INSTANCE], "request_id": str(uuid.uuid4())})
        resp.raise_for_status()
        pg_password = resp.json().get("token", "")

    if not pg_password:
        raise RuntimeError("Could not generate Lakebase credentials — set lakebase_endpoint_path (Autoscaling) or lakebase_instance (Provisioned)")

    return psycopg2.connect(host=LAKEBASE_DNS, port=5432, database=LAKEBASE_DATABASE,
        user=pg_user, password=pg_password, sslmode="require", connect_timeout=15)

# Cross-workspace host resolution (Tier 3) only runs when explicitly enabled.
# When disabled (default), the workflow processes only the local workspace —
# the Tier 1 path. Cross-workspace observability is served by the UC SQL
# discovery tasks (07, 08), not by this REST fan-out.
_lb_conn_for_refresh = None
if CROSS_WS_FANOUT_ENABLED:
    try:
        _lb_conn_for_refresh = _get_lakebase_conn()
        with _lb_conn_for_refresh.cursor() as cur:
            cur.execute("SELECT workspace_id, workspace_host FROM workspace_registry WHERE workspace_host IS NOT NULL")
            for row in cur.fetchall():
                ws_id = str(row[0])
                if ws_id in experiment_workspace_ids:
                    workspace_hosts[ws_id] = row[1]
        _host_resolution_log.append(f"lakebase: {len(workspace_hosts)} hosts matched")
        print(f"  ✅ Lakebase workspace_registry: {len(workspace_hosts)} hosts matched (of {len(experiment_workspace_ids)} with experiments)")
    except Exception as exc:
        _host_resolution_log.append(f"lakebase: {type(exc).__name__}: {str(exc)[:200]}")
        print(f"  ⚠️  Lakebase workspace_registry read failed: {exc}")

    # Fallback: enumerate workspaces via system.access.workspaces_latest (no
    # Accounts API auth scopes needed — queryable from any workspace with the SQL
    # warehouse). Fills in any workspace that has experiments but isn't in the
    # registry yet.
    _unresolved = experiment_workspace_ids - set(workspace_hosts.keys())
    _ws_to_upsert = []
    if _unresolved:
        print(f"▸ Resolving {len(_unresolved)} remaining workspace(s) via system.access.workspaces_latest ...")
        try:
            rows = _execute_sql(
                "SELECT CAST(workspace_id AS STRING) AS workspace_id, "
                "       workspace_url, workspace_name "
                "FROM system.access.workspaces_latest "
                "WHERE status = 'RUNNING' AND workspace_url IS NOT NULL"
            )
            added = 0
            for r in rows:
                ws_id = str(r.get("workspace_id") or "")
                host = (r.get("workspace_url") or "").rstrip("/")
                name = r.get("workspace_name") or ""
                if not ws_id or not host:
                    continue
                dep = host.replace("https://", "").split(".")[0]
                _ws_to_upsert.append((ws_id, host, name, dep))
                if ws_id in experiment_workspace_ids and ws_id not in workspace_hosts:
                    workspace_hosts[ws_id] = host
                    added += 1
            _host_resolution_log.append(f"system.access.workspaces_latest: +{added} hosts (of {len(_unresolved)} unresolved)")
            print(f"  ✅ system.access.workspaces_latest: added {added} hosts (of {len(_unresolved)} unresolved)")
        except Exception as exc:
            _host_resolution_log.append(f"system.access.workspaces_latest: {type(exc).__name__}: {str(exc)[:200]}")
            print(f"  ⚠️  system.access.workspaces_latest failed: {exc}")

        if _ws_to_upsert and _lb_conn_for_refresh is not None:
            try:
                with _lb_conn_for_refresh.cursor() as cur:
                    for ws_id, host, name, dep in _ws_to_upsert:
                        cur.execute(
                            """INSERT INTO workspace_registry (workspace_id, workspace_host, workspace_name, deployment_name, last_updated)
                               VALUES (%s, %s, %s, %s, NOW())
                               ON CONFLICT (workspace_id) DO UPDATE SET
                                   workspace_host = EXCLUDED.workspace_host,
                                   workspace_name = EXCLUDED.workspace_name,
                                   deployment_name = EXCLUDED.deployment_name,
                                   last_updated = NOW()""",
                            (ws_id, host, name, dep),
                        )
                    _lb_conn_for_refresh.commit()
                print(f"  ✅ Persisted {len(_ws_to_upsert)} workspaces to workspace_registry")
            except Exception as exc:
                print(f"  ⚠️  workspace_registry upsert failed: {exc}")

    if _lb_conn_for_refresh is not None:
        try:
            _lb_conn_for_refresh.close()
        except Exception:
            pass
else:
    _host_resolution_log.append("cross-workspace fan-out disabled (Tier 1 only)")
    print("  ℹ️  Cross-workspace fan-out disabled — local workspace only")

# Always include the current workspace — the workflow runs as the user
# who has full access to their own MLflow experiments and traces
try:
    _local_w = WorkspaceClient()
    _local_host = _local_w.config.host.rstrip("/")
    _local_ws_id = spark.conf.get("spark.databricks.workspaceUrl", "").replace("https://", "").split(".")[0]
    # Get workspace ID from the org ID in spark conf
    try:
        _local_ws_id = spark.conf.get("spark.databricks.clusterUsageTags.orgId", "")
    except Exception:
        pass
    if _local_host and _local_ws_id:
        workspace_hosts[_local_ws_id] = _local_host
        _host_resolution_log.append(f"current workspace: {_local_ws_id} → {_local_host}")
        print(f"  ✅ Added current workspace: {_local_ws_id} → {_local_host}")
    elif _local_host:
        # Use host as a fallback key if we can't get the workspace ID
        workspace_hosts["local"] = _local_host
        _host_resolution_log.append(f"current workspace: local → {_local_host}")
        print(f"  ✅ Added current workspace: local → {_local_host}")
except Exception as exc:
    print(f"  ⚠️  Could not add current workspace: {exc}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fetch Traces via SDK Cross-Workspace

# COMMAND ----------

def _get_sp_workspace_token(host: str) -> Optional[str]:
    """Mint a workspace-scoped SP token via M2M client_credentials. Returns None
    if SP creds aren't configured or exchange fails."""
    if not SP_CLIENT_ID or not SP_CLIENT_SECRET:
        return None
    try:
        r = _http.post(
            f"{host.rstrip('/')}/oidc/v1/token",
            data={
                "grant_type": "client_credentials",
                "client_id": SP_CLIENT_ID,
                "client_secret": SP_CLIENT_SECRET,
                "scope": "all-apis",
            },
            timeout=30,
        )
        if r.status_code == 200:
            return r.json().get("access_token") or None
    except Exception:
        pass
    return None


def fetch_traces_for_workspace(ws_id, host, max_traces=100):
    """Fetch traces + details from a remote workspace.

    Auth strategy: prefer an SP-minted workspace token (when SP creds + workspace
    grant are configured). Fall back to the runner's OAuth via WorkspaceClient(host=...).
    SP path returns data the runner can't see (other users' MLflow data); OAuth
    fallback only works for the local workspace.

    Uses raw `requests` (via `_http`) for remote MLflow calls because Databricks
    Serverless Compute's egress filter rejects cross-workspace SDK HTTP calls
    with "Cert validation failed". The /oidc/v1/token exchange and raw HTTP both
    bypass that filter.

    Returns (summaries, details, diag). Both filtered by RETENTION_CUTOFF_MS.
    """
    diag = {"ws_id": ws_id, "host": host, "status": "?", "exp_count": 0,
            "trace_count": 0, "detail_count": 0, "detail_errors": 0,
            "auth": "?", "error": None}
    try:
        sp_token = _get_sp_workspace_token(host)
        # Mask env vars so SDK uses the explicit host (not the local workspace)
        saved_env = {}
        for key in ["DATABRICKS_HOST", "DATABRICKS_ACCOUNT_ID"]:
            if key in os.environ:
                saved_env[key] = os.environ.pop(key)
        try:
            if sp_token:
                # Raw HTTP path with SP workspace token
                token = sp_token
                diag["auth"] = "sp"
                def _api_post(path, body):
                    r = _http.post(f"{host.rstrip('/')}{path}",
                        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                        json=body, timeout=30)
                    if r.status_code >= 400:
                        raise RuntimeError(f"HTTP {r.status_code} {path}: {r.text[:300]}")
                    return r.json()
                def _api_get(path, params):
                    r = _http.get(f"{host.rstrip('/')}{path}",
                        headers={"Authorization": f"Bearer {token}"},
                        params=params, timeout=30)
                    if r.status_code >= 400:
                        raise RuntimeError(f"HTTP {r.status_code} {path}: {r.text[:300]}")
                    return r.json()
            else:
                # SDK path (works for local workspace; remote calls fail under
                # serverless cert validation, but that's the runner-OAuth case
                # we hit when no SP creds are configured)
                w = WorkspaceClient(host=host)
                diag["auth"] = "runner_oauth"
                def _api_post(path, body):
                    return w.api_client.do("POST", path, body=body)
                def _api_get(path, params):
                    return w.api_client.do("GET", path, query=params)

            # Get experiments
            exp_resp = _api_post("/api/2.0/mlflow/experiments/search",
                {"max_results": 50, "order_by": ["last_update_time DESC"]})
            experiments = exp_resp.get("experiments", [])
            diag["exp_count"] = len(experiments)

            exp_ids = [e["experiment_id"] for e in experiments if e.get("experiment_id")]
            if not exp_ids:
                diag["status"] = "ok_no_experiments"
                return [], [], diag

            # Get traces per experiment (summaries first, details after)
            all_traces: List[Dict[str, Any]] = []
            all_details: List[Dict[str, Any]] = []
            for eid in exp_ids:
                try:
                    trace_resp = _api_get("/api/2.0/mlflow/traces",
                        {"experiment_ids": eid, "max_results": max_traces})
                    for t in trace_resp.get("traces", []):
                        info = t.get("info", {})
                        meta = info.get("trace_metadata", {})
                        tags = info.get("tags", {})
                        rid = t.get("request_id") or info.get("request_id", "")
                        if not rid:
                            continue
                        ts_ms = t.get("timestamp_ms") or info.get("timestamp_ms") or 0
                        try:
                            ts_ms = int(ts_ms)
                        except (TypeError, ValueError):
                            ts_ms = 0
                        # Time-window filter: skip traces older than retention cutoff
                        if ts_ms and ts_ms < RETENTION_CUTOFF_MS:
                            continue
                        all_traces.append({
                            "request_id": rid,
                            "workspace_id": ws_id,
                            "experiment_id": eid,
                            "trace_name": tags.get("mlflow.traceName", ""),
                            "state": info.get("state", t.get("state", "")),
                            "request_time": str(ts_ms or ""),
                            "execution_duration": info.get("execution_duration"),
                            "model_id": meta.get("mlflow.modelId", ""),
                            "session_id": meta.get("mlflow.trace.session", ""),
                            "trace_user": meta.get("mlflow.user", ""),
                            "source": meta.get("mlflow.source.name", ""),
                            "tags": json.dumps(tags),
                            "data_source": "rest_api",
                        })

                        # Fetch full detail (spans + payloads) for the cache.
                        try:
                            detail_resp = _api_get(f"/api/2.0/mlflow/traces/{rid}", {})
                            trace_obj = detail_resp.get("trace", {}) or detail_resp
                            trace_info_obj = trace_obj.get("trace_info", {}) or {}
                            trace_data = trace_obj.get("trace_data", {}) or {}
                            ti_json = json.dumps(trace_info_obj)
                            td_json = json.dumps(trace_data)
                            all_details.append({
                                "workspace_id": ws_id,
                                "request_id": rid,
                                "experiment_id": eid,
                                "trace_info": ti_json,
                                "trace_data": td_json,
                                "request_raw": trace_info_obj.get("request") or trace_data.get("request") or "",
                                "response_raw": trace_info_obj.get("response") or trace_data.get("response") or "",
                                "size_bytes": len(ti_json) + len(td_json),
                                "source_type": "mlflow_rest",
                            })
                        except Exception as detail_exc:
                            diag["detail_errors"] += 1
                            if not diag["error"]:
                                diag["error"] = f"detail rid={rid}: {str(detail_exc)[:120]}"
                except Exception as exc:
                    diag["error"] = f"trace query exp={eid}: {exc}"

            diag["trace_count"] = len(all_traces)
            diag["detail_count"] = len(all_details)
            diag["status"] = "ok"
            return all_traces, all_details, diag
        finally:
            os.environ.update(saved_env)
    except Exception as exc:
        diag["status"] = "error"
        diag["error"] = str(exc)[:200]
        return [], [], diag

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fan Out Across Workspaces

# COMMAND ----------

all_traces = []
all_details = []
all_diagnostics = []
now = datetime.now(timezone.utc)

# Narrow-scope filter (rollout testing): keep only the target workspace if set.
if NARROW_TEST_WS:
    workspace_hosts = {k: v for k, v in workspace_hosts.items() if str(k) == NARROW_TEST_WS}
    _host_resolution_log.append(f"narrow filter: {len(workspace_hosts)} host(s) match {NARROW_TEST_WS}")
    print(f"  ▸ Narrow filter active — fanning out to {len(workspace_hosts)} workspace(s) only")

print(f"▸ Fetching traces (+ details) from {len(workspace_hosts)} workspaces via SDK ...")

if workspace_hosts:
    with ThreadPoolExecutor(max_workers=min(len(workspace_hosts), 5)) as pool:
        futures = {
            pool.submit(fetch_traces_for_workspace, ws_id, host): ws_id
            for ws_id, host in workspace_hosts.items()
        }
        for fut in as_completed(futures, timeout=300):
            ws_id = futures[fut]
            try:
                traces, details, diag = fut.result()
                all_traces.extend(traces)
                all_details.extend(details)
                all_diagnostics.append(diag)
                msg = (f"ws={ws_id} auth={diag.get('auth','?')}: {diag['exp_count']} exps, "
                       f"{diag['trace_count']} traces, {diag['detail_count']} details "
                       f"({diag['detail_errors']} detail errors)")
                if diag.get("error"):
                    print(f"  ⚠️  {msg} — {diag['error'][:100]}")
                else:
                    print(f"  ✅ {msg}")
            except Exception as exc:
                print(f"  ⚠️  ws={ws_id} failed: {exc}")
                all_diagnostics.append({"ws_id": ws_id, "status": "exception", "error": str(exc)[:200]})

ws_with_traces = sum(1 for d in all_diagnostics if d.get("trace_count", 0) > 0)
ws_with_errors = sum(1 for d in all_diagnostics if d.get("status") == "error")
print(f"\n✅ Total: {len(all_traces)} traces, {len(all_details)} details "
      f"from {ws_with_traces} workspaces ({ws_with_errors} errors)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Table

# COMMAND ----------

if all_traces:
    trace_rows = []
    for t in all_traces:
        trace_rows.append((
            t["request_id"],
            t["workspace_id"],
            t.get("experiment_id", ""),
            t.get("trace_name", ""),
            t.get("state", ""),
            t.get("request_time", ""),
            t.get("execution_duration"),
            t.get("model_id", ""),
            t.get("session_id", ""),
            t.get("trace_user", ""),
            t.get("source", ""),
            t.get("tags", "{}"),
            t.get("data_source", "rest_api"),
            now,
        ))
    traces_df = spark.createDataFrame(trace_rows, TRACES_SCHEMA)
    traces_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TRACES_TABLE)
    count = spark.read.table(TRACES_TABLE).count()
    print(f"✅ Wrote {count} traces to Delta: {TRACES_TABLE}")
else:
    # Write empty table to ensure it exists
    empty_df = spark.createDataFrame([], TRACES_SCHEMA)
    empty_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TRACES_TABLE)
    print("ℹ️  No traces found — empty Delta table written")

# COMMAND ----------

# Write trace detail snapshot (full spans + payloads) for the cross-workspace cache.
if all_details:
    detail_rows = [(
        d["workspace_id"],
        d["request_id"],
        d.get("experiment_id", ""),
        d.get("trace_info", "{}"),
        d.get("trace_data", "{}"),
        d.get("request_raw", "") or "",
        d.get("response_raw", "") or "",
        int(d.get("size_bytes") or 0),
        d.get("source_type", "mlflow_rest"),
        now,
    ) for d in all_details]
    details_df = spark.createDataFrame(detail_rows, TRACE_DETAILS_SCHEMA)
    details_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TRACE_DETAILS_TABLE)
    detail_count = spark.read.table(TRACE_DETAILS_TABLE).count()
    print(f"✅ Wrote {detail_count} trace details to Delta: {TRACE_DETAILS_TABLE}")
else:
    empty_df = spark.createDataFrame([], TRACE_DETAILS_SCHEMA)
    empty_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TRACE_DETAILS_TABLE)
    print("ℹ️  No trace details to write — empty Delta table written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

# Diagnostic summary
error_types = {}
for d in all_diagnostics:
    if d.get("error"):
        err = d["error"]
        if "403" in err:
            error_types["403_forbidden"] = error_types.get("403_forbidden", 0) + 1
        elif "400" in err:
            error_types["400_bad_request"] = error_types.get("400_bad_request", 0) + 1
        elif "error" in d.get("status", ""):
            error_types["other"] = error_types.get("other", 0) + 1

total_detail_errors = sum(d.get("detail_errors", 0) for d in all_diagnostics)
auth_breakdown = {"sp": 0, "runner_oauth": 0, "?": 0}
for d in all_diagnostics:
    auth_breakdown[d.get("auth", "?")] = auth_breakdown.get(d.get("auth", "?"), 0) + 1
result = {
    "status": "success",
    "workspaces_with_experiments": len(experiment_workspace_ids),
    "workspaces_with_hosts": len(workspace_hosts),
    "workspaces_queried": len(all_diagnostics),
    "workspaces_with_traces": ws_with_traces,
    "total_traces": len(all_traces),
    "total_trace_details": len(all_details),
    "total_detail_errors": total_detail_errors,
    "retention_days": RETENTION_DAYS,
    "sp_auth_enabled": bool(SP_CLIENT_ID),
    "narrow_test_ws": NARROW_TEST_WS or None,
    "auth_breakdown": auth_breakdown,
    "error_breakdown": error_types,
    "sample_errors": [d["error"][:150] for d in all_diagnostics if d.get("error")][:5],
    "account_id_set": bool(ACCOUNT_ID),
    "sample_hosts": dict(list(workspace_hosts.items())[:3]),
    "host_resolution_log": _host_resolution_log,
    "discovered_at": now.isoformat(),
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
