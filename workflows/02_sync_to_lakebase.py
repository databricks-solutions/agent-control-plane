# Databricks notebook source
# MAGIC %md
# MAGIC # Sync Discovered Agents: Delta → Lakebase
# MAGIC
# MAGIC Reads the latest snapshot from the Delta `discovered_agents` table
# MAGIC and upserts into the Lakebase PostgreSQL `discovered_agents` table.
# MAGIC
# MAGIC This runs as the second task in the discovery Workflow, after
# MAGIC `01_discover_agents` has written the Delta table.

# COMMAND ----------

# MAGIC %pip install psycopg2-binary databricks-sdk requests --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import os
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timezone

from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog name")
dbutils.widgets.text("schema", "", "Schema name")
dbutils.widgets.text("delta_table", "discovered_agents", "Delta table name")
dbutils.widgets.text("lakebase_dns", "", "Lakebase host (DNS)")
dbutils.widgets.text("lakebase_database", "", "Lakebase database name")
dbutils.widgets.text("lakebase_instance", "", "Lakebase instance name")
dbutils.widgets.text("account_id", "", "Databricks account ID")
dbutils.widgets.text("warehouse_id", "", "SQL warehouse ID for system table queries")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
DELTA_TABLE = f"{CATALOG}.{SCHEMA}.{dbutils.widgets.get('delta_table')}"
LAKEBASE_DNS = dbutils.widgets.get("lakebase_dns")
LAKEBASE_DATABASE = dbutils.widgets.get("lakebase_database")
LAKEBASE_INSTANCE = dbutils.widgets.get("lakebase_instance")
ACCOUNT_ID = dbutils.widgets.get("account_id")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")

# Set as env var so refresh_workspace_registry can find it
if ACCOUNT_ID:
    os.environ["DATABRICKS_ACCOUNT_ID"] = ACCOUNT_ID

if not CATALOG or not SCHEMA:
    raise ValueError(
        f"catalog and schema must be set via job parameters (got catalog={CATALOG!r}, schema={SCHEMA!r}). "
        "Deploy with: databricks bundle deploy -t <target>"
    )
if not LAKEBASE_DNS or not LAKEBASE_INSTANCE:
    raise ValueError(
        f"lakebase_dns and lakebase_instance must be set via job parameters "
        f"(got dns={LAKEBASE_DNS!r}, instance={LAKEBASE_INSTANCE!r}). "
        "Deploy with: databricks bundle deploy -t <target>"
    )

print(f"Source Delta table: {DELTA_TABLE}")
print(f"Target Lakebase: {LAKEBASE_DNS}/{LAKEBASE_DATABASE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generate Lakebase Credentials

# COMMAND ----------

def get_lakebase_connection():
    """Create a Lakebase PostgreSQL connection.

    Tries the Databricks SDK first (w.database.generate_database_credential),
    then falls back to the REST API if the SDK version is too old.
    """
    from databricks.sdk import WorkspaceClient
    import requests as http_requests
    import uuid

    w = WorkspaceClient()

    # Get current identity for PG username
    me = w.current_user.me()
    pg_user = me.user_name
    print(f"Lakebase user: {pg_user}")

    # Try SDK first (requires databricks-sdk >= 0.38)
    pg_password = None
    if hasattr(w, "database"):
        try:
            creds = w.database.generate_database_credential(
                instance_names=[LAKEBASE_INSTANCE]
            )
            pg_password = creds.token
            print("Credential generated via SDK")
        except Exception as e:
            print(f"SDK credential generation failed: {e}")

    # Fallback to REST API
    if not pg_password:
        print("Falling back to REST API for credential generation")
        # Get auth token — try SDK header provider, then fall back to notebook context
        try:
            header_factory = w.config.authenticate
            auth_headers = header_factory()
            token = auth_headers.get("Authorization", "").replace("Bearer ", "")
        except Exception:
            token = ""
        if not token:
            try:
                token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
            except Exception:
                token = getattr(w.config, "token", "") or ""
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        host = w.config.host.rstrip("/")

        cred_resp = http_requests.post(
            f"{host}/api/2.0/database/credentials",
            headers=headers,
            json={"instance_names": [LAKEBASE_INSTANCE], "request_id": str(uuid.uuid4())},
        )
        cred_resp.raise_for_status()
        pg_password = cred_resp.json().get("token", "")
        print("Credential generated via REST API")

    print(f"Connecting to Lakebase at: {LAKEBASE_DNS}")

    return psycopg2.connect(
        host=LAKEBASE_DNS,
        port=5432,
        database=LAKEBASE_DATABASE,
        user=pg_user,
        password=pg_password,
        sslmode="require",
        connect_timeout=15,
        options="-c statement_timeout=60000",
    )

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ensure Lakebase Table Exists

# COMMAND ----------

def ensure_lakebase_table(conn):
    """Create the discovered_agents table in Lakebase if it doesn't exist."""
    ddl = """
    CREATE TABLE IF NOT EXISTS discovered_agents (
        agent_id          TEXT PRIMARY KEY,
        workspace_id      TEXT NOT NULL,
        name              TEXT NOT NULL,
        type              TEXT,
        endpoint_name     TEXT,
        endpoint_status   TEXT,
        model_name        TEXT,
        served_entity_name TEXT,
        creator           TEXT,
        description       TEXT DEFAULT '',
        config            JSONB,
        last_synced       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        source            TEXT DEFAULT 'api',
        is_extensive      BOOLEAN DEFAULT FALSE
    );
    CREATE INDEX IF NOT EXISTS idx_da_ws ON discovered_agents (workspace_id);
    CREATE INDEX IF NOT EXISTS idx_da_type ON discovered_agents (type);
    DROP INDEX IF EXISTS idx_da_name_ws;
    CREATE INDEX IF NOT EXISTS idx_da_name_ws ON discovered_agents (name, workspace_id);
    """
    with conn.cursor() as cur:
        for stmt in ddl.split(";"):
            stmt = stmt.strip()
            if stmt:
                try:
                    cur.execute(stmt)
                except Exception as e:
                    print(f"  DDL warning: {e}")
    conn.commit()
    print("Lakebase table ensured")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Delta & Sync

# COMMAND ----------

# Read the latest snapshot from Delta
df = spark.read.table(DELTA_TABLE)
agent_count = df.count()
print(f"Read {agent_count} agents from Delta table")

if agent_count == 0:
    print("No agents to sync — exiting")
    dbutils.notebook.exit(json.dumps({"status": "skipped", "reason": "no_agents", "count": 0}))

# Collect to driver (discovery tables are small — typically < 1000 rows)
rows = df.collect()

# COMMAND ----------

# Connect to Lakebase and sync
conn = get_lakebase_connection()

try:
    ensure_lakebase_table(conn)

    with conn.cursor() as cur:
        # Drop the UNIQUE constraint on (name, workspace_id) that causes conflicts
        # with duplicate names from different discovery sources (e.g. audit log Genie Spaces).
        # Try multiple approaches since it could be an index or a table constraint.
        for drop_sql in [
            "DROP INDEX IF EXISTS idx_da_name_ws",
            "ALTER TABLE discovered_agents DROP CONSTRAINT IF EXISTS idx_da_name_ws",
        ]:
            try:
                cur.execute(drop_sql)
                conn.commit()
                print(f"  Executed: {drop_sql}")
            except Exception as e:
                conn.rollback()
                print(f"  {drop_sql} — skipped: {e}")

        # List remaining indexes/constraints for debugging
        try:
            cur.execute("""
                SELECT indexname, indexdef
                FROM pg_indexes
                WHERE tablename = 'discovered_agents'
            """)
            for row in cur.fetchall():
                print(f"  Index: {row[0]} -> {row[1]}")
        except Exception as e:
            print(f"  Could not list indexes: {e}")

        # Truncate and reload for a clean snapshot (matches the app's refresh_discovery behavior)
        cur.execute("TRUNCATE TABLE discovered_agents")
        conn.commit()
        print("Truncated existing Lakebase discovered_agents")

        # Bulk insert using execute_values with ON CONFLICT to handle
        # duplicate (name, workspace_id) pairs from different discovery sources
        insert_sql = """
            INSERT INTO discovered_agents
                (agent_id, workspace_id, name, type, endpoint_name,
                 endpoint_status, model_name, served_entity_name,
                 creator, description, config, last_synced, source, is_extensive)
            VALUES %s
            ON CONFLICT (agent_id) DO UPDATE SET
                name = EXCLUDED.name,
                type = EXCLUDED.type,
                endpoint_name = EXCLUDED.endpoint_name,
                endpoint_status = EXCLUDED.endpoint_status,
                model_name = EXCLUDED.model_name,
                served_entity_name = EXCLUDED.served_entity_name,
                creator = EXCLUDED.creator,
                description = EXCLUDED.description,
                config = EXCLUDED.config,
                last_synced = EXCLUDED.last_synced,
                source = EXCLUDED.source,
                is_extensive = EXCLUDED.is_extensive
        """

        values = []
        now = datetime.now(timezone.utc)
        for r in rows:
            config_val = r.config
            # Ensure config is valid JSON for JSONB column
            if config_val:
                try:
                    json.loads(config_val)
                except (json.JSONDecodeError, TypeError):
                    config_val = json.dumps({})
            else:
                config_val = json.dumps({})

            values.append((
                r.agent_id,
                r.workspace_id,
                r.name,
                r.type,
                r.endpoint_name or "",
                r.endpoint_status or "",
                r.model_name or "",
                r.served_entity_name or "",
                r.creator or "",
                r.description or "",
                config_val,
                now,
                r.source or "api",
                bool(r.is_extensive),
            ))

        execute_values(cur, insert_sql, values, page_size=100)
        conn.commit()

        # Verify
        cur.execute("SELECT COUNT(*) FROM discovered_agents")
        lb_count = cur.fetchone()[0]
        print(f"Synced {lb_count} agents to Lakebase")

finally:
    conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync Summary

# COMMAND ----------

agent_result = {
    "agents_delta_count": agent_count,
    "agents_lakebase_count": len(values),
}
print(json.dumps(agent_result, indent=2))

# COMMAND ----------

# MAGIC %md
# MAGIC # Phase 2: Sync MLflow Observability → Lakebase
# MAGIC
# MAGIC Three data sources:
# MAGIC - **System tables** (`system.mlflow.*`): experiments & runs (account-level, cross-workspace)
# MAGIC - **Delta table** (`observability_traces`): traces discovered by `04_discover_observability` task
# MAGIC
# MAGIC Each row is tagged with `data_source` for attribution.

# COMMAND ----------

import time as _time

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Statements API helper (for system tables)

# COMMAND ----------

def _find_warehouse_id():
    """Find a running SQL warehouse. Uses job parameter first, then SDK lookup."""
    if WAREHOUSE_ID:
        print(f"  Using warehouse from parameter: {WAREHOUSE_ID}")
        return WAREHOUSE_ID
    from databricks.sdk import WorkspaceClient as _WRC3
    w = _WRC3()
    try:
        warehouses = list(w.warehouses.list())
        for wh in warehouses:
            if wh.warehouse_type and "SERVERLESS" in str(wh.warehouse_type).upper() and str(wh.state) == "RUNNING":
                print(f"  Found serverless warehouse: {wh.id}")
                return wh.id
        for wh in warehouses:
            if str(wh.state) == "RUNNING":
                print(f"  Found warehouse: {wh.id}")
                return wh.id
    except Exception as exc:
        print(f"  ⚠️  Warehouse lookup failed: {exc}")
    return None


def _execute_system_sql(sql):
    """Execute SQL via SQL Statements API and return list of dicts."""
    wh_id = _find_warehouse_id()
    if not wh_id:
        print("  ⚠️  No running SQL warehouse found — cannot query system tables")
        return []

    from databricks.sdk import WorkspaceClient as _WRC4
    w = _WRC4()

    body = {
        "warehouse_id": wh_id,
        "statement": sql,
        "wait_timeout": "50s",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    }

    try:
        resp_json = w.api_client.do("POST", "/api/2.0/sql/statements", body=body)
    except Exception as exc:
        print(f"  ⚠️  SQL Statements API failed: {exc}")
        return []

    status = resp_json.get("status", {}).get("state", "")
    statement_id = resp_json.get("statement_id", "")

    # Poll if still running
    if status in ("PENDING", "RUNNING") and statement_id:
        for _ in range(20):
            _time.sleep(3)
            try:
                resp_json = w.api_client.do("GET", f"/api/2.0/sql/statements/{statement_id}")
            except Exception:
                continue
            status = resp_json.get("status", {}).get("state", "")
            if status not in ("PENDING", "RUNNING"):
                break

    if status != "SUCCEEDED":
        err = resp_json.get("status", {}).get("error", {})
        print(f"  ⚠️  SQL status: {status} — {err.get('message', '')}")
        return []

    columns = [c["name"] for c in resp_json.get("manifest", {}).get("schema", {}).get("columns", [])]
    rows = resp_json.get("result", {}).get("data_array", [])
    return [dict(zip(columns, row)) for row in rows]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 2a: System Tables — Experiments & Runs

# COMMAND ----------

print("▸ Querying system.mlflow.experiments_latest via SQL Statements API ...")
try:
    st_experiments = _execute_system_sql("""
        SELECT
            CAST(experiment_id AS STRING) AS experiment_id,
            CAST(workspace_id AS STRING) AS workspace_id,
            name,
            CASE WHEN delete_time IS NULL THEN 'active' ELSE 'deleted' END AS lifecycle_stage,
            CAST(UNIX_TIMESTAMP(COALESCE(update_time, create_time)) * 1000 AS BIGINT) AS last_update_time
        FROM system.mlflow.experiments_latest
        WHERE delete_time IS NULL
        ORDER BY COALESCE(update_time, create_time) DESC
        LIMIT 5000
    """)
    for e in st_experiments:
        e["data_source"] = "system_table"
    print(f"  ✅ {len(st_experiments)} experiments from system tables")
except Exception as exc:
    print(f"  ⚠️  system.mlflow.experiments_latest query failed: {exc}")
    st_experiments = []

# COMMAND ----------

print("▸ Querying system.mlflow.runs_latest via SQL Statements API ...")
try:
    st_runs = _execute_system_sql("""
        SELECT
            run_id,
            CAST(experiment_id AS STRING) AS experiment_id,
            CAST(workspace_id AS STRING) AS workspace_id,
            status,
            CAST(UNIX_TIMESTAMP(start_time) * 1000 AS BIGINT) AS start_time,
            CAST(UNIX_TIMESTAMP(end_time) * 1000 AS BIGINT) AS end_time,
            created_by AS user_id,
            run_name,
            TO_JSON(tags) AS tags,
            TO_JSON(params) AS params,
            TO_JSON(aggregated_metrics) AS metrics
        FROM system.mlflow.runs_latest
        WHERE delete_time IS NULL
        ORDER BY start_time DESC
        LIMIT 5000
    """)
    for r in st_runs:
        r["data_source"] = "system_table"
    print(f"  ✅ {len(st_runs)} runs from system tables")
except Exception as exc:
    print(f"  ⚠️  system.mlflow.runs_latest query failed: {exc}")
    st_runs = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 2b: Read Traces from Delta (populated by discover_observability task)

# COMMAND ----------

TRACES_DELTA = f"{CATALOG}.{SCHEMA}.observability_traces"
trace_count = 0

print(f"▸ Reading traces from Delta: {TRACES_DELTA} ...")
try:
    traces_df = spark.read.table(TRACES_DELTA)
    delta_trace_count = traces_df.count()
    print(f"  ✅ {delta_trace_count} traces in Delta table")
    delta_traces = traces_df.collect()
except Exception as exc:
    print(f"  ⚠️  Could not read traces Delta table: {exc}")
    delta_traces = []
    delta_trace_count = 0

# COMMAND ----------

# MAGIC %md
# MAGIC ## Upsert All Observability Data to Lakebase

# COMMAND ----------

obs_conn = get_lakebase_connection()
now = datetime.now(timezone.utc)

all_experiments = st_experiments

# Ensure observability tables
with obs_conn.cursor() as cur:
    for ddl in [
        """CREATE TABLE IF NOT EXISTS observability_experiments (
            experiment_id TEXT NOT NULL, workspace_id TEXT NOT NULL,
            name TEXT, lifecycle_stage TEXT, last_update_time BIGINT,
            artifact_location TEXT, tags JSONB,
            data_source TEXT DEFAULT 'system_table',
            last_synced TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (workspace_id, experiment_id))""",
        "CREATE INDEX IF NOT EXISTS idx_oe_ws ON observability_experiments (workspace_id)",
        "ALTER TABLE observability_experiments ADD COLUMN IF NOT EXISTS data_source TEXT DEFAULT 'system_table'",
        "ALTER TABLE observability_experiments ADD COLUMN IF NOT EXISTS tags JSONB",
        """CREATE TABLE IF NOT EXISTS observability_runs (
            run_id TEXT NOT NULL, workspace_id TEXT NOT NULL,
            experiment_id TEXT, status TEXT,
            start_time BIGINT, end_time BIGINT,
            user_id TEXT, run_name TEXT,
            tags JSONB, params JSONB, metrics JSONB,
            data_source TEXT DEFAULT 'system_table',
            last_synced TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (workspace_id, run_id))""",
        "CREATE INDEX IF NOT EXISTS idx_or_ws ON observability_runs (workspace_id)",
        "ALTER TABLE observability_runs ADD COLUMN IF NOT EXISTS data_source TEXT DEFAULT 'system_table'",
        "ALTER TABLE observability_runs ADD COLUMN IF NOT EXISTS tags JSONB",
        "ALTER TABLE observability_runs ADD COLUMN IF NOT EXISTS params JSONB",
        "ALTER TABLE observability_runs ADD COLUMN IF NOT EXISTS metrics JSONB",
        """CREATE TABLE IF NOT EXISTS observability_traces (
            request_id TEXT NOT NULL, workspace_id TEXT NOT NULL,
            experiment_id TEXT, trace_name TEXT, state TEXT,
            request_time TEXT, execution_duration BIGINT,
            user_message TEXT, response_preview TEXT, token_usage JSONB,
            model_id TEXT, session_id TEXT, trace_user TEXT, source TEXT,
            tags JSONB, data_source TEXT DEFAULT 'rest_api',
            last_synced TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (workspace_id, request_id))""",
        "CREATE INDEX IF NOT EXISTS idx_ot_ws ON observability_traces (workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_ot_time ON observability_traces (request_time DESC)",
        "ALTER TABLE observability_traces ADD COLUMN IF NOT EXISTS data_source TEXT DEFAULT 'rest_api'",
    ]:
        try:
            cur.execute(ddl)
        except Exception as e:
            print(f"  DDL warning: {e}")
    obs_conn.commit()
print("✅ Observability tables ensured")

# COMMAND ----------

# Upsert experiments (REST data_source wins on conflict — richer info with tags)
exp_count = 0
if all_experiments:
    exp_values = []
    for exp in all_experiments:
        # REST API returns tags as array of {key, value} objects
        # System table experiments don't have tags
        tags_raw = exp.get("tags", [])
        if isinstance(tags_raw, list):
            # Convert [{key: k, value: v}, ...] to {k: v} dict for JSONB
            tags_dict = {t.get("key", ""): t.get("value", "") for t in tags_raw if isinstance(t, dict)}
            tags_json = json.dumps(tags_dict) if tags_dict else None
        elif isinstance(tags_raw, dict):
            tags_json = json.dumps(tags_raw) if tags_raw else None
        else:
            tags_json = None

        exp_values.append((
            str(exp.get("experiment_id", "")),
            str(exp.get("workspace_id", "")),
            exp.get("name", ""),
            exp.get("lifecycle_stage", ""),
            int(exp.get("last_update_time") or 0),
            exp.get("artifact_location", ""),
            tags_json,
            exp.get("data_source", "system_table"),
            now,
        ))
    with obs_conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO observability_experiments
               (experiment_id, workspace_id, name, lifecycle_stage, last_update_time, artifact_location, tags, data_source, last_synced)
               VALUES %s
               ON CONFLICT (workspace_id, experiment_id) DO UPDATE SET
                   name = EXCLUDED.name, lifecycle_stage = EXCLUDED.lifecycle_stage,
                   last_update_time = EXCLUDED.last_update_time,
                   artifact_location = EXCLUDED.artifact_location,
                   tags = COALESCE(EXCLUDED.tags, observability_experiments.tags),
                   data_source = EXCLUDED.data_source, last_synced = EXCLUDED.last_synced""",
            exp_values, page_size=100,
        )
        obs_conn.commit()
        exp_count = len(exp_values)
print(f"✅ Upserted {exp_count} experiments (system_table)")

# COMMAND ----------

# Upsert runs (system table — includes tags, params, metrics)
run_count = 0
if st_runs:
    run_values = []
    for r in st_runs:
        # tags/params/metrics come as JSON strings from SQL or dicts — normalize to JSON strings
        tags_val = r.get("tags", {})
        params_val = r.get("params", {})
        metrics_val = r.get("metrics", [])
        if isinstance(tags_val, str):
            tags_json = tags_val
        else:
            tags_json = json.dumps(tags_val) if tags_val else json.dumps({})
        if isinstance(params_val, str):
            params_json = params_val
        else:
            params_json = json.dumps(params_val) if params_val else json.dumps({})
        if isinstance(metrics_val, str):
            metrics_json = metrics_val
        else:
            metrics_json = json.dumps(metrics_val) if metrics_val else json.dumps([])

        run_values.append((
            r.get("run_id", ""),
            str(r.get("workspace_id", "")),
            str(r.get("experiment_id", "")),
            r.get("status", ""),
            int(r.get("start_time") or 0),
            int(r.get("end_time") or 0) if r.get("end_time") else None,
            r.get("user_id", ""),
            r.get("run_name", ""),
            tags_json,
            params_json,
            metrics_json,
            r.get("data_source", "system_table"),
            now,
        ))
    with obs_conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO observability_runs
               (run_id, workspace_id, experiment_id, status, start_time, end_time, user_id, run_name, tags, params, metrics, data_source, last_synced)
               VALUES %s
               ON CONFLICT (workspace_id, run_id) DO UPDATE SET
                   experiment_id = EXCLUDED.experiment_id, status = EXCLUDED.status,
                   start_time = EXCLUDED.start_time, end_time = EXCLUDED.end_time,
                   user_id = EXCLUDED.user_id, run_name = EXCLUDED.run_name,
                   tags = EXCLUDED.tags, params = EXCLUDED.params, metrics = EXCLUDED.metrics,
                   data_source = EXCLUDED.data_source, last_synced = EXCLUDED.last_synced""",
            run_values, page_size=100,
        )
        obs_conn.commit()
        run_count = len(run_values)
print(f"✅ Upserted {run_count} runs (system_table)")

# COMMAND ----------

# Upsert traces (from Delta table populated by discover_observability task)
trace_count = 0
if delta_traces:
    trace_values = []
    for r in delta_traces:
        rid = r.request_id
        if not rid:
            continue
        trace_values.append((
            rid,
            r.workspace_id or "",
            r.experiment_id or "",
            r.trace_name or "",
            r.state or "",
            r.request_time or "",
            r.execution_duration,
            json.dumps({}),  # token_usage
            r.model_id or "",
            r.session_id or "",
            r.trace_user or "",
            r.source or "",
            r.tags or "{}",
            r.data_source or "rest_api",
            now,
        ))
    with obs_conn.cursor() as cur:
        execute_values(
            cur,
            """INSERT INTO observability_traces
               (request_id, workspace_id, experiment_id, trace_name, state,
                request_time, execution_duration, token_usage, model_id,
                session_id, trace_user, source, tags, data_source, last_synced)
               VALUES %s
               ON CONFLICT (workspace_id, request_id) DO UPDATE SET
                   trace_name = EXCLUDED.trace_name, state = EXCLUDED.state,
                   request_time = EXCLUDED.request_time, execution_duration = EXCLUDED.execution_duration,
                   token_usage = EXCLUDED.token_usage, model_id = EXCLUDED.model_id,
                   session_id = EXCLUDED.session_id, trace_user = EXCLUDED.trace_user,
                   source = EXCLUDED.source, tags = EXCLUDED.tags,
                   data_source = EXCLUDED.data_source, last_synced = EXCLUDED.last_synced""",
            trace_values, page_size=100,
        )
        obs_conn.commit()
        trace_count = len(trace_values)
print(f"✅ Upserted {trace_count} traces (from Delta)")

# COMMAND ----------

obs_conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 3: Sync Knowledge Bases (Delta → Lakebase)

# COMMAND ----------

VS_EP_TABLE = f"{CATALOG}.{SCHEMA}.vector_search_endpoints"
VS_IDX_TABLE = f"{CATALOG}.{SCHEMA}.vector_search_indexes"
LB_INST_TABLE = f"{CATALOG}.{SCHEMA}.lakebase_instances"

kb_conn = get_lakebase_connection()
vs_ep_count = 0
vs_idx_count = 0
lb_inst_count = 0

# Ensure tables
with kb_conn.cursor() as cur:
    for ddl in [
        """CREATE TABLE IF NOT EXISTS vector_search_endpoints (
            endpoint_name TEXT PRIMARY KEY, endpoint_id TEXT, status TEXT,
            endpoint_type TEXT, num_indexes INT DEFAULT 0, creator TEXT,
            workspace_id TEXT, created_at TIMESTAMP,
            last_synced TIMESTAMP WITH TIME ZONE DEFAULT NOW())""",
        """CREATE TABLE IF NOT EXISTS vector_search_indexes (
            index_name TEXT NOT NULL, endpoint_name TEXT NOT NULL,
            index_type TEXT, primary_key TEXT, creator TEXT, workspace_id TEXT,
            detailed_state TEXT, indexed_row_count INT DEFAULT 0,
            ready BOOLEAN DEFAULT FALSE, status_message TEXT,
            source_table TEXT, embedding_model TEXT, pipeline_type TEXT,
            last_synced TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (endpoint_name, index_name))""",
        """CREATE TABLE IF NOT EXISTS vector_search_health_history (
            endpoint_name TEXT NOT NULL, status TEXT NOT NULL,
            num_indexes INT DEFAULT 0,
            recorded_at TIMESTAMP WITH TIME ZONE DEFAULT NOW())""",
        """CREATE TABLE IF NOT EXISTS lakebase_instances (
            instance_name TEXT PRIMARY KEY, instance_id TEXT, state TEXT,
            capacity TEXT, pg_version TEXT, read_write_dns TEXT, read_only_dns TEXT,
            creator TEXT, created_at TEXT,
            last_synced TIMESTAMP WITH TIME ZONE DEFAULT NOW())""",
    ]:
        try:
            cur.execute(ddl)
        except Exception as e:
            print(f"  DDL warning: {e}")
    kb_conn.commit()
print("✅ Knowledge bases tables ensured")

# COMMAND ----------

# Sync VS endpoints
print(f"▸ Syncing Vector Search endpoints from Delta: {VS_EP_TABLE} ...")
try:
    ep_df = spark.read.table(VS_EP_TABLE)
    ep_rows = ep_df.collect()
    if ep_rows:
        with kb_conn.cursor() as cur:
            for r in ep_rows:
                cur.execute(
                    """INSERT INTO vector_search_endpoints
                       (endpoint_name, endpoint_id, status, endpoint_type, num_indexes, creator, created_at, last_synced)
                       VALUES (%s, %s, %s, %s, %s, %s, NULL, NOW())
                       ON CONFLICT (endpoint_name) DO UPDATE SET
                           status = EXCLUDED.status, endpoint_type = EXCLUDED.endpoint_type,
                           num_indexes = EXCLUDED.num_indexes, last_synced = NOW()""",
                    (r.endpoint_name, r.endpoint_id, r.status, r.endpoint_type,
                     r.num_indexes, r.creator))
                # Record health history
                cur.execute(
                    "INSERT INTO vector_search_health_history (endpoint_name, status, num_indexes) VALUES (%s, %s, %s)",
                    (r.endpoint_name, r.status, r.num_indexes))
                vs_ep_count += 1
            kb_conn.commit()
    print(f"  ✅ {vs_ep_count} endpoints synced")
except Exception as exc:
    print(f"  ⚠️  VS endpoints sync failed: {exc}")

# COMMAND ----------

# Sync VS indexes
print(f"▸ Syncing Vector Search indexes from Delta: {VS_IDX_TABLE} ...")
try:
    idx_df = spark.read.table(VS_IDX_TABLE)
    idx_rows = idx_df.collect()
    if idx_rows:
        with kb_conn.cursor() as cur:
            for r in idx_rows:
                cur.execute(
                    """INSERT INTO vector_search_indexes
                       (index_name, endpoint_name, index_type, primary_key, creator,
                        detailed_state, indexed_row_count, ready, status_message,
                        source_table, embedding_model, pipeline_type, last_synced)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                       ON CONFLICT (endpoint_name, index_name) DO UPDATE SET
                           index_type = EXCLUDED.index_type, detailed_state = EXCLUDED.detailed_state,
                           indexed_row_count = EXCLUDED.indexed_row_count, ready = EXCLUDED.ready,
                           status_message = EXCLUDED.status_message, source_table = EXCLUDED.source_table,
                           embedding_model = EXCLUDED.embedding_model, pipeline_type = EXCLUDED.pipeline_type,
                           last_synced = NOW()""",
                    (r.index_name, r.endpoint_name, r.index_type, r.primary_key,
                     r.creator, r.detailed_state, r.indexed_row_count, r.ready,
                     r.status_message, r.source_table, r.embedding_model, r.pipeline_type))
                vs_idx_count += 1
            kb_conn.commit()
    print(f"  ✅ {vs_idx_count} indexes synced")
except Exception as exc:
    print(f"  ⚠️  VS indexes sync failed: {exc}")

# COMMAND ----------

# Sync Lakebase instances
print(f"▸ Syncing Lakebase instances from Delta: {LB_INST_TABLE} ...")
try:
    lb_df = spark.read.table(LB_INST_TABLE)
    lb_rows = lb_df.collect()
    if lb_rows:
        with kb_conn.cursor() as cur:
            for r in lb_rows:
                cur.execute(
                    """INSERT INTO lakebase_instances
                       (instance_name, instance_id, state, capacity, pg_version,
                        read_write_dns, read_only_dns, creator, created_at, last_synced)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                       ON CONFLICT (instance_name) DO UPDATE SET
                           state = EXCLUDED.state, capacity = EXCLUDED.capacity,
                           pg_version = EXCLUDED.pg_version, last_synced = NOW()""",
                    (r.instance_name, r.instance_id, r.state, r.capacity,
                     r.pg_version, r.read_write_dns, r.read_only_dns,
                     r.creator, r.created_at))
                lb_inst_count += 1
            kb_conn.commit()
    print(f"  ✅ {lb_inst_count} instances synced")
except Exception as exc:
    print(f"  ⚠️  Lakebase instances sync failed: {exc}")

# COMMAND ----------

kb_conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Summary

# COMMAND ----------

result = {
    "status": "success",
    **agent_result,
    "observability_experiments": exp_count,
    "observability_runs": run_count,
    "observability_traces": trace_count,
    "traces_from_delta": delta_trace_count,
    "vs_endpoints": vs_ep_count,
    "vs_indexes": vs_idx_count,
    "lakebase_instances": lb_inst_count,
    "synced_at": datetime.now(timezone.utc).isoformat(),
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
