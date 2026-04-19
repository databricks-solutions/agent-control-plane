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
from datetime import datetime, timezone
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
dbutils.widgets.text("lakebase_instance", "", "Lakebase instance name")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
ACCOUNT_ID = dbutils.widgets.get("account_id")
LAKEBASE_DNS = dbutils.widgets.get("lakebase_dns")
LAKEBASE_DATABASE = dbutils.widgets.get("lakebase_database")
LAKEBASE_INSTANCE = dbutils.widgets.get("lakebase_instance")

if ACCOUNT_ID:
    os.environ["DATABRICKS_ACCOUNT_ID"] = ACCOUNT_ID
TRACES_TABLE = f"{CATALOG}.{SCHEMA}.observability_traces"

if not CATALOG or not SCHEMA:
    raise ValueError(f"catalog and schema required (got {CATALOG!r}, {SCHEMA!r})")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Target: {TRACES_TABLE}")
print(f"Warehouse: {WAREHOUSE_ID}")

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

    # Try Autoscaling Lakebase first
    if LAKEBASE_INSTANCE:
        endpoint_path = f"projects/{LAKEBASE_INSTANCE}/branches/production/endpoints/primary"
        try:
            token = _get_token()
            resp = _http.post(f"{host}/api/2.0/postgres/credentials",
                headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
                json={"endpoint": endpoint_path})
            resp.raise_for_status()
            pg_password = resp.json().get("token", "")
            if pg_password:
                print(f"  Autoscaling credential OK (project={LAKEBASE_INSTANCE})")
        except Exception as e:
            print(f"  Autoscaling credential failed: {e}")

    # Fallback to Provisioned SDK
    if not pg_password and hasattr(w, "database"):
        try:
            creds = w.database.generate_database_credential(instance_names=[LAKEBASE_INSTANCE])
            pg_password = creds.token
        except Exception as e:
            print(f"  Provisioned SDK credential failed: {e}")

    # Fallback to Provisioned REST
    if not pg_password:
        token = _get_token()
        resp = _http.post(f"{host}/api/2.0/database/credentials",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json={"instance_names": [LAKEBASE_INSTANCE], "request_id": str(uuid.uuid4())})
        resp.raise_for_status()
        pg_password = resp.json().get("token", "")

    return psycopg2.connect(host=LAKEBASE_DNS, port=5432, database=LAKEBASE_DATABASE,
        user=pg_user, password=pg_password, sslmode="require", connect_timeout=15)

try:
    lb_conn = _get_lakebase_conn()
    with lb_conn.cursor() as cur:
        cur.execute("SELECT workspace_id, workspace_host FROM workspace_registry WHERE workspace_host IS NOT NULL")
        for row in cur.fetchall():
            ws_id = str(row[0])
            if ws_id in experiment_workspace_ids:
                workspace_hosts[ws_id] = row[1]
    lb_conn.close()
    _host_resolution_log.append(f"lakebase: {len(workspace_hosts)} hosts matched")
    print(f"  ✅ Lakebase workspace_registry: {len(workspace_hosts)} hosts matched (of {len(experiment_workspace_ids)} with experiments)")
except Exception as exc:
    _host_resolution_log.append(f"lakebase: {type(exc).__name__}: {str(exc)[:200]}")
    print(f"  ⚠️  Lakebase workspace_registry read failed: {exc}")

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

def fetch_traces_for_workspace(ws_id, host, max_traces=100):
    """Use SDK WorkspaceClient(host=...) to fetch traces from a remote workspace.

    The SDK authenticates using the notebook runner's OAuth credentials.
    As an account admin, this works across any workspace in the account.
    """
    diag = {"ws_id": ws_id, "host": host, "status": "?", "exp_count": 0, "trace_count": 0, "error": None}
    try:
        # Mask env vars so SDK uses the explicit host, not the local workspace
        saved_env = {}
        for key in ["DATABRICKS_HOST", "DATABRICKS_ACCOUNT_ID"]:
            if key in os.environ:
                saved_env[key] = os.environ.pop(key)
        try:
            w = WorkspaceClient(host=host)

            # Get experiments
            exp_resp = w.api_client.do("POST", "/api/2.0/mlflow/experiments/search",
                body={"max_results": 50, "order_by": ["last_update_time DESC"]})
            experiments = exp_resp.get("experiments", [])
            diag["exp_count"] = len(experiments)

            exp_ids = [e["experiment_id"] for e in experiments if e.get("experiment_id")]
            if not exp_ids:
                diag["status"] = "ok_no_experiments"
                return [], diag

            # Get traces per experiment
            all_traces = []
            for eid in exp_ids:
                try:
                    trace_resp = w.api_client.do("GET", "/api/2.0/mlflow/traces",
                        query={"experiment_ids": eid, "max_results": max_traces})
                    for t in trace_resp.get("traces", []):
                        info = t.get("info", {})
                        meta = info.get("trace_metadata", {})
                        tags = info.get("tags", {})
                        rid = t.get("request_id") or info.get("request_id", "")
                        if not rid:
                            continue
                        all_traces.append({
                            "request_id": rid,
                            "workspace_id": ws_id,
                            "experiment_id": eid,
                            "trace_name": tags.get("mlflow.traceName", ""),
                            "state": info.get("state", t.get("state", "")),
                            "request_time": str(t.get("timestamp_ms") or info.get("timestamp_ms", "")),
                            "execution_duration": info.get("execution_duration"),
                            "model_id": meta.get("mlflow.modelId", ""),
                            "session_id": meta.get("mlflow.trace.session", ""),
                            "trace_user": meta.get("mlflow.user", ""),
                            "source": meta.get("mlflow.source.name", ""),
                            "tags": json.dumps(tags),
                            "data_source": "rest_api",
                        })
                except Exception as exc:
                    diag["error"] = f"trace query exp={eid}: {exc}"

            diag["trace_count"] = len(all_traces)
            diag["status"] = "ok"
            return all_traces, diag
        finally:
            os.environ.update(saved_env)
    except Exception as exc:
        diag["status"] = "error"
        diag["error"] = str(exc)[:200]
        return [], diag

# COMMAND ----------

# MAGIC %md
# MAGIC ## Fan Out Across Workspaces

# COMMAND ----------

all_traces = []
all_diagnostics = []
now = datetime.now(timezone.utc)

print(f"▸ Fetching traces from {len(workspace_hosts)} workspaces via SDK ...")

if workspace_hosts:
    with ThreadPoolExecutor(max_workers=min(len(workspace_hosts), 5)) as pool:
        futures = {
            pool.submit(fetch_traces_for_workspace, ws_id, host): ws_id
            for ws_id, host in workspace_hosts.items()
        }
        for fut in as_completed(futures, timeout=120):
            ws_id = futures[fut]
            try:
                traces, diag = fut.result()
                all_traces.extend(traces)
                all_diagnostics.append(diag)
                if diag.get("error"):
                    print(f"  ⚠️  ws={ws_id}: {diag['exp_count']} exps, {diag['trace_count']} traces, error: {diag['error'][:100]}")
                else:
                    print(f"  ✅ ws={ws_id}: {diag['exp_count']} exps, {diag['trace_count']} traces")
            except Exception as exc:
                print(f"  ⚠️  ws={ws_id} failed: {exc}")
                all_diagnostics.append({"ws_id": ws_id, "status": "exception", "error": str(exc)[:200]})

ws_with_traces = sum(1 for d in all_diagnostics if d.get("trace_count", 0) > 0)
ws_with_errors = sum(1 for d in all_diagnostics if d.get("status") == "error")
print(f"\n✅ Total: {len(all_traces)} traces from {ws_with_traces} workspaces ({ws_with_errors} errors)")

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
    traces_df.write.mode("overwrite").saveAsTable(TRACES_TABLE)
    count = spark.read.table(TRACES_TABLE).count()
    print(f"✅ Wrote {count} traces to Delta: {TRACES_TABLE}")
else:
    # Write empty table to ensure it exists
    empty_df = spark.createDataFrame([], TRACES_SCHEMA)
    empty_df.write.mode("overwrite").saveAsTable(TRACES_TABLE)
    print("ℹ️  No traces found — empty Delta table written")

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

result = {
    "status": "success",
    "workspaces_with_experiments": len(experiment_workspace_ids),
    "workspaces_with_hosts": len(workspace_hosts),
    "workspaces_queried": len(all_diagnostics),
    "workspaces_with_traces": ws_with_traces,
    "total_traces": len(all_traces),
    "error_breakdown": error_types,
    "sample_errors": [d["error"][:150] for d in all_diagnostics if d.get("error")][:5],
    "account_id_set": bool(ACCOUNT_ID),
    "sample_hosts": dict(list(workspace_hosts.items())[:3]),
    "host_resolution_log": _host_resolution_log,
    "discovered_at": now.isoformat(),
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
