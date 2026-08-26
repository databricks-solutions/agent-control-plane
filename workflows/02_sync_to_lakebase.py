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
dbutils.widgets.text("lakebase_instance", "", "Lakebase instance name (Provisioned only)")
dbutils.widgets.text("lakebase_endpoint_path", "", "Lakebase endpoint path (Autoscaling only, e.g. projects/<name>/branches/<branch>/endpoints/<endpoint>)")
dbutils.widgets.text("account_id", "", "Databricks account ID")
dbutils.widgets.text("warehouse_id", "", "SQL warehouse ID for system table queries")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
DELTA_TABLE = f"{CATALOG}.{SCHEMA}.{dbutils.widgets.get('delta_table')}"
LAKEBASE_DNS = dbutils.widgets.get("lakebase_dns")
LAKEBASE_DATABASE = dbutils.widgets.get("lakebase_database")
LAKEBASE_INSTANCE = dbutils.widgets.get("lakebase_instance")
LAKEBASE_ENDPOINT_PATH = dbutils.widgets.get("lakebase_endpoint_path")
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
if not LAKEBASE_DNS:
    raise ValueError(
        f"lakebase_dns must be set via job parameters (got dns={LAKEBASE_DNS!r}). "
        "Deploy with: databricks bundle deploy -t <target>"
    )
if not LAKEBASE_ENDPOINT_PATH and not LAKEBASE_INSTANCE:
    raise ValueError(
        "Either lakebase_endpoint_path (Autoscaling) or lakebase_instance (Provisioned) must be set via job parameters."
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

    # Get auth token for REST API calls
    def _get_auth_token():
        try:
            header_factory = w.config.authenticate
            auth_headers = header_factory()
            return auth_headers.get("Authorization", "").replace("Bearer ", "")
        except Exception:
            pass
        try:
            return dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
        except Exception:
            return getattr(w.config, "token", "") or ""

    pg_password = None
    host = w.config.host.rstrip("/")

    # Autoscaling Lakebase: POST /api/2.0/postgres/credentials with explicit endpoint path
    if LAKEBASE_ENDPOINT_PATH:
        try:
            token = _get_auth_token()
            headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
            cred_resp = http_requests.post(
                f"{host}/api/2.0/postgres/credentials",
                headers=headers,
                json={"endpoint": LAKEBASE_ENDPOINT_PATH},
            )
            cred_resp.raise_for_status()
            pg_password = cred_resp.json().get("token", "")
            if pg_password:
                print(f"Credential generated via Autoscaling API (endpoint={LAKEBASE_ENDPOINT_PATH})")
        except Exception as e:
            print(f"Autoscaling credential generation failed: {e}")

    # Provisioned Lakebase SDK (w.database.generate_database_credential)
    if not pg_password and LAKEBASE_INSTANCE and hasattr(w, "database"):
        try:
            creds = w.database.generate_database_credential(
                instance_names=[LAKEBASE_INSTANCE]
            )
            pg_password = creds.token
            print("Credential generated via Provisioned SDK")
        except Exception as e:
            print(f"Provisioned SDK credential generation failed: {e}")

    # Fallback to Provisioned REST API
    if not pg_password and LAKEBASE_INSTANCE:
        print("Falling back to Provisioned REST API for credential generation")
        token = _get_auth_token()
        headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
        cred_resp = http_requests.post(
            f"{host}/api/2.0/database/credentials",
            headers=headers,
            json={"instance_names": [LAKEBASE_INSTANCE], "request_id": str(uuid.uuid4())},
        )
        cred_resp.raise_for_status()
        pg_password = cred_resp.json().get("token", "")
        print("Credential generated via Provisioned REST API")

    if not pg_password:
        raise RuntimeError("Could not generate Lakebase credentials via any method")

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
        is_extensive      BOOLEAN DEFAULT FALSE,
        workload_class    TEXT,
        subtype           TEXT,
        framework         TEXT,
        interface_task    TEXT,
        uses_llm          BOOLEAN,
        linked_endpoint   TEXT,
        confidence        TEXT,
        classified_by     TEXT,
        classifier_version TEXT,
        raw_signals       TEXT
    );
    ALTER TABLE discovered_agents ADD COLUMN IF NOT EXISTS workload_class TEXT;
    ALTER TABLE discovered_agents ADD COLUMN IF NOT EXISTS subtype TEXT;
    ALTER TABLE discovered_agents ADD COLUMN IF NOT EXISTS framework TEXT;
    ALTER TABLE discovered_agents ADD COLUMN IF NOT EXISTS interface_task TEXT;
    ALTER TABLE discovered_agents ADD COLUMN IF NOT EXISTS uses_llm BOOLEAN;
    ALTER TABLE discovered_agents ADD COLUMN IF NOT EXISTS linked_endpoint TEXT;
    ALTER TABLE discovered_agents ADD COLUMN IF NOT EXISTS confidence TEXT;
    ALTER TABLE discovered_agents ADD COLUMN IF NOT EXISTS classified_by TEXT;
    ALTER TABLE discovered_agents ADD COLUMN IF NOT EXISTS classifier_version TEXT;
    ALTER TABLE discovered_agents ADD COLUMN IF NOT EXISTS raw_signals TEXT;
    CREATE INDEX IF NOT EXISTS idx_da_ws ON discovered_agents (workspace_id);
    CREATE INDEX IF NOT EXISTS idx_da_type ON discovered_agents (type);
    CREATE INDEX IF NOT EXISTS idx_da_workload_class ON discovered_agents (workload_class);
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
                 creator, description, config, last_synced, source, is_extensive,
                 workload_class, subtype, framework, interface_task, uses_llm,
                 linked_endpoint, confidence, classified_by, classifier_version, raw_signals)
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
                is_extensive = EXCLUDED.is_extensive,
                workload_class = EXCLUDED.workload_class,
                subtype = EXCLUDED.subtype,
                framework = EXCLUDED.framework,
                interface_task = EXCLUDED.interface_task,
                uses_llm = EXCLUDED.uses_llm,
                linked_endpoint = EXCLUDED.linked_endpoint,
                confidence = EXCLUDED.confidence,
                classified_by = EXCLUDED.classified_by,
                classifier_version = EXCLUDED.classifier_version,
                raw_signals = EXCLUDED.raw_signals
        """

        # Migration-safe field access (classification columns may be absent if an
        # older Delta snapshot is read before 01 re-runs).
        def _rg(row, field):
            try:
                return row[field]
            except Exception:
                return None

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

            uses_llm = _rg(r, "uses_llm")
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
                _rg(r, "workload_class"),
                _rg(r, "subtype"),
                _rg(r, "framework"),
                _rg(r, "interface_task"),
                (None if uses_llm is None else bool(uses_llm)),
                _rg(r, "linked_endpoint"),
                _rg(r, "confidence"),
                _rg(r, "classified_by"),
                _rg(r, "classifier_version"),
                _rg(r, "raw_signals"),
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
TRACE_DETAILS_DELTA = f"{CATALOG}.{SCHEMA}.observability_trace_details"
TRACES_UC_OTEL_DELTA = f"{CATALOG}.{SCHEMA}.observability_traces_uc_otel"
TRACE_DETAILS_UC_OTEL_DELTA = f"{CATALOG}.{SCHEMA}.observability_trace_details_uc_otel"
trace_count = 0
trace_detail_count = 0

print(f"▸ Reading traces from Delta: {TRACES_DELTA} ...")
try:
    traces_df = spark.read.table(TRACES_DELTA)
    delta_trace_count = traces_df.count()
    print(f"  ✅ {delta_trace_count} traces in Delta table (default-backend, REST)")
    delta_traces = traces_df.collect()
except Exception as exc:
    print(f"  ⚠️  Could not read traces Delta table: {exc}")
    delta_traces = []
    delta_trace_count = 0

print(f"▸ Reading UC-OTel traces from Delta: {TRACES_UC_OTEL_DELTA} ...")
try:
    uc_traces_df = spark.read.table(TRACES_UC_OTEL_DELTA)
    delta_uc_trace_count = uc_traces_df.count()
    print(f"  ✅ {delta_uc_trace_count} traces in Delta table (Tier 2b: UC OTel)")
    delta_uc_traces = uc_traces_df.collect()
except Exception as exc:
    print(f"  ⚠️  Could not read UC OTel traces Delta table: {exc}")
    delta_uc_traces = []
    delta_uc_trace_count = 0

print(f"▸ Reading trace details from Delta: {TRACE_DETAILS_DELTA} ...")
try:
    details_df = spark.read.table(TRACE_DETAILS_DELTA)
    delta_detail_count = details_df.count()
    print(f"  ✅ {delta_detail_count} trace details in Delta table (default-backend, REST)")
    delta_trace_details = details_df.collect()
except Exception as exc:
    print(f"  ⚠️  Could not read trace details Delta table: {exc}")
    delta_trace_details = []
    delta_detail_count = 0

print(f"▸ Reading UC-OTel trace details from Delta: {TRACE_DETAILS_UC_OTEL_DELTA} ...")
try:
    uc_details_df = spark.read.table(TRACE_DETAILS_UC_OTEL_DELTA)
    delta_uc_detail_count = uc_details_df.count()
    print(f"  ✅ {delta_uc_detail_count} trace details in Delta table (Tier 2b: UC OTel)")
    delta_uc_trace_details = uc_details_df.collect()
except Exception as exc:
    print(f"  ⚠️  Could not read UC OTel trace details Delta table: {exc}")
    delta_uc_trace_details = []
    delta_uc_detail_count = 0

# Combine REST and UC OTel rows for the upsert path below — both go to the
# same Lakebase tables, with `data_source`/`source_type` distinguishing them.
delta_traces = list(delta_traces) + list(delta_uc_traces)
delta_trace_details = list(delta_trace_details) + list(delta_uc_trace_details)
print(f"▸ Combined: {len(delta_traces)} trace rows, {len(delta_trace_details)} detail rows for Lakebase upsert")

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
        """CREATE TABLE IF NOT EXISTS observability_trace_details (
            workspace_id TEXT NOT NULL, request_id TEXT NOT NULL,
            experiment_id TEXT, trace_info JSONB, trace_data JSONB,
            request_raw TEXT, response_raw TEXT,
            size_bytes INTEGER, source_type TEXT,
            cached_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (workspace_id, request_id))""",
        "CREATE INDEX IF NOT EXISTS idx_otd_ws     ON observability_trace_details (workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_otd_cached ON observability_trace_details (cached_at DESC)",
        """CREATE TABLE IF NOT EXISTS agent_tool_usage (
            experiment_id TEXT, tool_name TEXT NOT NULL, span_type TEXT NOT NULL,
            call_count BIGINT DEFAULT 0, trace_count BIGINT DEFAULT 0,
            error_count BIGINT DEFAULT 0, total_latency_ms DOUBLE PRECISION DEFAULT 0,
            last_seen TEXT,
            last_synced TIMESTAMP WITH TIME ZONE DEFAULT NOW())""",
        "CREATE INDEX IF NOT EXISTS idx_atu_exp ON agent_tool_usage (experiment_id)",
        # Reconcile the error/latency columns onto pre-existing tables (CREATE IF
        # NOT EXISTS is a no-op when the table already exists). Workflow-owned, so
        # the workflow can ALTER it.
        "ALTER TABLE agent_tool_usage ADD COLUMN IF NOT EXISTS error_count BIGINT DEFAULT 0",
        "ALTER TABLE agent_tool_usage ADD COLUMN IF NOT EXISTS total_latency_ms DOUBLE PRECISION DEFAULT 0",
        """CREATE TABLE IF NOT EXISTS mlflow_registered_models (
            name TEXT NOT NULL, workspace_id TEXT, user_id TEXT,
            last_updated_timestamp BIGINT, creation_timestamp BIGINT,
            description TEXT, aliases JSONB, latest_versions JSONB,
            last_synced TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (name))""",
        """CREATE TABLE IF NOT EXISTS mlflow_model_versions (
            name TEXT NOT NULL, version TEXT NOT NULL, workspace_id TEXT,
            user_id TEXT, creation_timestamp BIGINT, last_updated_timestamp BIGINT,
            status TEXT, description TEXT, source TEXT, run_id TEXT, aliases JSONB,
            last_synced TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (name, version))""",
        "CREATE INDEX IF NOT EXISTS idx_mmv_name ON mlflow_model_versions (name)",
        """CREATE TABLE IF NOT EXISTS agent_eval_scores (
            experiment_id TEXT, scorer_name TEXT NOT NULL, source_type TEXT,
            assessment_count BIGINT DEFAULT 0, pass_count BIGINT DEFAULT 0,
            fail_count BIGINT DEFAULT 0, pass_rate DOUBLE PRECISION,
            last_seen TEXT,
            last_synced TIMESTAMP WITH TIME ZONE DEFAULT NOW())""",
        "CREATE INDEX IF NOT EXISTS idx_aes_exp ON agent_eval_scores (experiment_id)",
        """CREATE TABLE IF NOT EXISTS ai_audit_summary (
            service_name TEXT NOT NULL, action_name TEXT NOT NULL,
            event_count BIGINT DEFAULT 0, actor_count BIGINT DEFAULT 0,
            error_count BIGINT DEFAULT 0, workspace_count BIGINT DEFAULT 0,
            last_seen TEXT,
            last_synced TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (service_name, action_name))""",
        "CREATE INDEX IF NOT EXISTS idx_aas_svc ON ai_audit_summary (service_name)",
        """CREATE TABLE IF NOT EXISTS ai_audit_recent (
            event_time TEXT, service_name TEXT, action_name TEXT, actor TEXT,
            status_code BIGINT, workspace_id TEXT,
            last_synced TIMESTAMP WITH TIME ZONE DEFAULT NOW())""",
    ]:
        # Each statement in its own savepoint — protects against the case
        # where the workflow runs as a non-owner of pre-existing tables (the
        # app's startup helpers create these as the SP). A failure rolls back
        # to the savepoint instead of poisoning the surrounding transaction.
        try:
            cur.execute("SAVEPOINT obs_ddl_sp")
            cur.execute(ddl)
            cur.execute("RELEASE SAVEPOINT obs_ddl_sp")
        except Exception as e:
            try: cur.execute("ROLLBACK TO SAVEPOINT obs_ddl_sp")
            except Exception: pass
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
# We enrich each slim trace row with fields parsed from the matching detail
# row's trace_info JSON — token_usage, user_message, response_preview — so the
# list endpoint surfaces them without an extra JSONB read per row.
trace_count = 0

# Build a lookup of detail rows by request_id (UC traces have empty workspace_id,
# default-backend traces are scoped — request_id is unique enough either way).
details_by_rid: dict = {}
for d in delta_trace_details:
    if d.request_id:
        details_by_rid[d.request_id] = d


def _enrich_from_trace_info(rid: str) -> tuple:
    """Return (token_usage_json, user_message, response_preview) for a trace."""
    d = details_by_rid.get(rid)
    if not d:
        return ("{}", "", "")
    ti_raw = d.trace_info or "{}"
    try:
        ti = json.loads(ti_raw) if isinstance(ti_raw, str) else (ti_raw or {})
    except Exception:
        ti = {}
    meta = ti.get("trace_metadata") or {}
    # token_usage lives under trace_metadata as a JSON-string value
    tu_str = meta.get("mlflow.trace.tokenUsage") or "{}"
    try:
        tu_obj = json.loads(tu_str) if isinstance(tu_str, str) else (tu_str or {})
    except Exception:
        tu_obj = {}
    tu_json = json.dumps(tu_obj) if isinstance(tu_obj, dict) else "{}"
    # Best-effort user message + response preview — populated by the parser for
    # default-backend traces (request_preview tag) and explicitly for UC traces.
    user_msg = ti.get("request_preview") or ""
    if not user_msg:
        # Fallback: pull first user-role text from the parsed request payload
        req_raw = d.request_raw or ""
        try:
            req_parsed = json.loads(req_raw) if req_raw else {}
            for inp in (req_parsed.get("input") or []):
                if isinstance(inp, dict) and inp.get("role") == "user":
                    c = inp.get("content")
                    if isinstance(c, str):
                        user_msg = c; break
                    if isinstance(c, list):
                        for piece in c:
                            if isinstance(piece, dict) and piece.get("text"):
                                user_msg = piece["text"]; break
                        if user_msg: break
        except Exception:
            pass
    resp_preview = ti.get("response_preview") or ""
    return (tu_json, user_msg or "", resp_preview or "")


if delta_traces:
    trace_values = []
    for r in delta_traces:
        rid = r.request_id
        if not rid:
            continue
        token_usage_json, user_msg, resp_preview = _enrich_from_trace_info(rid)
        trace_values.append((
            rid,
            r.workspace_id or "",
            r.experiment_id or "",
            r.trace_name or "",
            r.state or "",
            r.request_time or "",
            r.execution_duration,
            user_msg,
            resp_preview,
            token_usage_json,
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
                request_time, execution_duration, user_message, response_preview,
                token_usage, model_id, session_id, trace_user, source, tags,
                data_source, last_synced)
               VALUES %s
               ON CONFLICT (workspace_id, request_id) DO UPDATE SET
                   trace_name = EXCLUDED.trace_name, state = EXCLUDED.state,
                   request_time = EXCLUDED.request_time, execution_duration = EXCLUDED.execution_duration,
                   user_message = EXCLUDED.user_message, response_preview = EXCLUDED.response_preview,
                   token_usage = EXCLUDED.token_usage, model_id = EXCLUDED.model_id,
                   session_id = EXCLUDED.session_id, trace_user = EXCLUDED.trace_user,
                   source = EXCLUDED.source, tags = EXCLUDED.tags,
                   data_source = EXCLUDED.data_source, last_synced = EXCLUDED.last_synced""",
            trace_values, page_size=100,
        )
        obs_conn.commit()
        trace_count = len(trace_values)
print(f"✅ Upserted {trace_count} traces (from Delta, enriched from trace_info)")

# COMMAND ----------

# Upsert trace details (full spans + payloads) for cross-workspace cache.
# Note: UC-OTel traces have empty workspace_id (UC data is account-level, not
# workspace-scoped); we accept empty workspace_id here since the URI-form
# request_id is globally unique on its own.
if delta_trace_details:
    detail_values = []
    for r in delta_trace_details:
        rid = r.request_id
        if not rid:
            continue
        ws_id = r.workspace_id or ""
        detail_values.append((
            ws_id,
            rid,
            r.experiment_id or "",
            r.trace_info or "{}",
            r.trace_data or "{}",
            r.request_raw or "",
            r.response_raw or "",
            int(r.size_bytes or 0),
            r.source_type or "mlflow_rest",
            now,
        ))
    if detail_values:
        with obs_conn.cursor() as cur:
            execute_values(
                cur,
                """INSERT INTO observability_trace_details
                   (workspace_id, request_id, experiment_id, trace_info, trace_data,
                    request_raw, response_raw, size_bytes, source_type, cached_at)
                   VALUES %s
                   ON CONFLICT (workspace_id, request_id) DO UPDATE SET
                       experiment_id = EXCLUDED.experiment_id,
                       trace_info = EXCLUDED.trace_info,
                       trace_data = EXCLUDED.trace_data,
                       request_raw = EXCLUDED.request_raw,
                       response_raw = EXCLUDED.response_raw,
                       size_bytes = EXCLUDED.size_bytes,
                       source_type = EXCLUDED.source_type,
                       cached_at = EXCLUDED.cached_at""",
                detail_values, page_size=50,
            )
            obs_conn.commit()
            trace_detail_count = len(detail_values)
print(f"✅ Upserted {trace_detail_count} trace details (from Delta)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 2c: Sync AI Gateway / Inference Logs (Tier 2a, Delta → Lakebase)

# COMMAND ----------

GATEWAY_LOGS_DELTA = f"{CATALOG}.{SCHEMA}.gateway_inference_logs"
gw_log_count = 0

print(f"▸ Reading inference logs from Delta: {GATEWAY_LOGS_DELTA} ...")
try:
    gw_logs_df = spark.read.table(GATEWAY_LOGS_DELTA)
    gw_log_delta_count = gw_logs_df.count()
    print(f"  ✅ {gw_log_delta_count} inference-log rows in Delta")
    gw_log_rows = gw_logs_df.collect()
except Exception as exc:
    print(f"  ⚠️  Could not read inference logs Delta: {exc}")
    gw_log_rows = []
    gw_log_delta_count = 0

# Ensure Lakebase target table
with obs_conn.cursor() as cur:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS gateway_inference_logs (
            request_id          TEXT NOT NULL,
            source_table        TEXT NOT NULL,
            client_request_id   TEXT,
            request_time        TIMESTAMP WITH TIME ZONE,
            status_code         INTEGER,
            execution_ms        BIGINT,
            request_payload     TEXT,
            response_payload    TEXT,
            request_size_bytes  BIGINT,
            response_size_bytes BIGINT,
            served_entity_id    TEXT,
            requester           TEXT,
            last_synced         TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (source_table, request_id)
        )
    """)
    # Extracted-payload columns — populated at sync time by parsing the JSON
    # payload bodies. Best-effort across vendor shapes (OpenAI/Anthropic/etc.).
    # Each ALTER runs in its own savepoint so a single column failure
    # doesn't poison the surrounding transaction (psycopg2 does not auto-
    # rollback on caught exceptions, and any subsequent statement on the
    # same cursor would otherwise hit InFailedSqlTransaction).
    for col_ddl in (
        "ALTER TABLE gateway_inference_logs ADD COLUMN IF NOT EXISTS model TEXT",
        "ALTER TABLE gateway_inference_logs ADD COLUMN IF NOT EXISTS input_tokens BIGINT",
        "ALTER TABLE gateway_inference_logs ADD COLUMN IF NOT EXISTS output_tokens BIGINT",
        "ALTER TABLE gateway_inference_logs ADD COLUMN IF NOT EXISTS total_tokens BIGINT",
        "ALTER TABLE gateway_inference_logs ADD COLUMN IF NOT EXISTS finish_reason TEXT",
        "ALTER TABLE gateway_inference_logs ADD COLUMN IF NOT EXISTS tool_call_count INTEGER",
    ):
        try:
            cur.execute("SAVEPOINT gw_col_sp")
            cur.execute(col_ddl)
            cur.execute("RELEASE SAVEPOINT gw_col_sp")
        except Exception as e:
            try: cur.execute("ROLLBACK TO SAVEPOINT gw_col_sp")
            except Exception: pass
            print(f"  gw col DDL warn: {e}")
    # CREATE INDEX IF NOT EXISTS still requires table ownership in PG, even
    # when the index already exists. Wrap each in a savepoint so a non-owner
    # workflow run doesn't fail when the indexes were pre-created (typically
    # by the app's startup ensure_*_table() helpers running as the SP).
    for idx_ddl in (
        "CREATE INDEX IF NOT EXISTS idx_gil_time   ON gateway_inference_logs (request_time DESC)",
        "CREATE INDEX IF NOT EXISTS idx_gil_source ON gateway_inference_logs (source_table)",
        "CREATE INDEX IF NOT EXISTS idx_gil_status ON gateway_inference_logs (status_code)",
        "CREATE INDEX IF NOT EXISTS idx_gil_model  ON gateway_inference_logs (model)",
    ):
        try:
            cur.execute("SAVEPOINT gw_idx_sp")
            cur.execute(idx_ddl)
            cur.execute("RELEASE SAVEPOINT gw_idx_sp")
        except Exception as e:
            try: cur.execute("ROLLBACK TO SAVEPOINT gw_idx_sp")
            except Exception: pass
            print(f"  gw idx DDL warn: {e}")
    obs_conn.commit()


def _parse_gateway_payload(req_raw: str, resp_raw: str) -> dict:
    """Best-effort extraction of model + token usage + finish reason.

    Handles OpenAI-style (`response.model`, `response.usage.{prompt,completion,total}_tokens`)
    and Anthropic-passthrough variants (`completion_tokens` ↔ `output_tokens`).
    Returns dict with keys: model, input_tokens, output_tokens, total_tokens,
    finish_reason, tool_call_count. Any field missing is None.
    """
    out = {"model": None, "input_tokens": None, "output_tokens": None,
           "total_tokens": None, "finish_reason": None, "tool_call_count": None}
    try:
        rj = json.loads(req_raw) if req_raw else {}
    except Exception:
        rj = {}
    try:
        sj = json.loads(resp_raw) if resp_raw else {}
    except Exception:
        sj = {}
    # Model: response.model > request.model
    out["model"] = sj.get("model") or rj.get("model")
    # Usage: support both OpenAI shape and Anthropic-passthrough shape
    usage = sj.get("usage") or {}
    if isinstance(usage, dict):
        out["input_tokens"]  = usage.get("prompt_tokens") or usage.get("input_tokens")
        out["output_tokens"] = usage.get("completion_tokens") or usage.get("output_tokens")
        out["total_tokens"]  = usage.get("total_tokens")
        if out["total_tokens"] is None and (out["input_tokens"] or out["output_tokens"]):
            out["total_tokens"] = (out["input_tokens"] or 0) + (out["output_tokens"] or 0)
    # Finish reason + tool call count from first choice
    choices = sj.get("choices")
    if isinstance(choices, list) and choices:
        c0 = choices[0] or {}
        out["finish_reason"] = c0.get("finish_reason")
        msg = c0.get("message") or {}
        tcs = msg.get("tool_calls") or []
        if isinstance(tcs, list):
            out["tool_call_count"] = len(tcs)
    return out

if gw_log_rows:
    values = []
    for r in gw_log_rows:
        rid = r.request_id
        src = r.source_table
        if not rid or not src:
            continue
        parsed = _parse_gateway_payload(r.request_payload or "", r.response_payload or "")
        values.append((
            rid, src,
            r.client_request_id,
            r.request_time,
            int(r.status_code) if r.status_code is not None else None,
            int(r.execution_ms) if r.execution_ms is not None else None,
            r.request_payload,
            r.response_payload,
            int(r.request_size_bytes) if r.request_size_bytes is not None else None,
            int(r.response_size_bytes) if r.response_size_bytes is not None else None,
            r.served_entity_id,
            r.requester,
            parsed["model"],
            parsed["input_tokens"],
            parsed["output_tokens"],
            parsed["total_tokens"],
            parsed["finish_reason"],
            parsed["tool_call_count"],
            now,
        ))
    if values:
        with obs_conn.cursor() as cur:
            execute_values(
                cur,
                """INSERT INTO gateway_inference_logs
                   (request_id, source_table, client_request_id, request_time,
                    status_code, execution_ms, request_payload, response_payload,
                    request_size_bytes, response_size_bytes, served_entity_id,
                    requester, model, input_tokens, output_tokens, total_tokens,
                    finish_reason, tool_call_count, last_synced)
                   VALUES %s
                   ON CONFLICT (source_table, request_id) DO UPDATE SET
                       client_request_id = EXCLUDED.client_request_id,
                       request_time = EXCLUDED.request_time,
                       status_code = EXCLUDED.status_code,
                       execution_ms = EXCLUDED.execution_ms,
                       request_payload = EXCLUDED.request_payload,
                       response_payload = EXCLUDED.response_payload,
                       request_size_bytes = EXCLUDED.request_size_bytes,
                       response_size_bytes = EXCLUDED.response_size_bytes,
                       served_entity_id = EXCLUDED.served_entity_id,
                       requester = EXCLUDED.requester,
                       model = EXCLUDED.model,
                       input_tokens = EXCLUDED.input_tokens,
                       output_tokens = EXCLUDED.output_tokens,
                       total_tokens = EXCLUDED.total_tokens,
                       finish_reason = EXCLUDED.finish_reason,
                       tool_call_count = EXCLUDED.tool_call_count,
                       last_synced = EXCLUDED.last_synced""",
                values, page_size=200,
            )
            obs_conn.commit()
            gw_log_count = len(values)
print(f"✅ Upserted {gw_log_count} gateway inference-log rows (from Delta, payload-enriched)")

# COMMAND ----------

# Sync agent_tool_usage (TOOL/RETRIEVER span rollup from 07_discover_uc_otel_traces).
TOOL_USAGE_DELTA = f"{CATALOG}.{SCHEMA}.agent_tool_usage"
atu_count = 0
print(f"▸ Syncing {TOOL_USAGE_DELTA} → agent_tool_usage ...")
try:
    # Read first, then truncate+insert in ONE transaction (single commit): a failed
    # insert rolls back the truncate rather than leaving the table empty.
    atu_rows = spark.read.table(TOOL_USAGE_DELTA).collect()
    values = [(r.experiment_id, r.tool_name, r.span_type, int(r.call_count or 0),
               int(r.trace_count or 0), int(getattr(r, "error_count", 0) or 0),
               float(getattr(r, "total_latency_ms", 0.0) or 0.0), r.last_seen, now) for r in atu_rows]
    with obs_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE agent_tool_usage")
        if values:
            execute_values(cur,
                """INSERT INTO agent_tool_usage
                   (experiment_id, tool_name, span_type, call_count, trace_count,
                    error_count, total_latency_ms, last_seen, last_synced)
                   VALUES %s""",
                values, page_size=500)
    obs_conn.commit()
    atu_count = len(values)
    print(f"  ✅ {atu_count} agent tool-usage rows synced")
except Exception as exc:
    obs_conn.rollback()
    print(f"  ⚠️  agent_tool_usage sync failed: {exc}")

# COMMAND ----------

# Sync agent_eval_scores (F7 — MLflow-3 assessment rollup from 07_discover_uc_otel_traces).
EVAL_SCORES_DELTA = f"{CATALOG}.{SCHEMA}.agent_eval_scores"
aes_count = 0
print(f"▸ Syncing {EVAL_SCORES_DELTA} → agent_eval_scores ...")
try:
    # Read first, then truncate+insert in ONE transaction (single commit): a failed
    # insert rolls back the truncate rather than leaving the table empty.
    aes_rows = spark.read.table(EVAL_SCORES_DELTA).collect()
    values = [(r.experiment_id, r.scorer_name, r.source_type,
               int(r.assessment_count or 0), int(r.pass_count or 0), int(r.fail_count or 0),
               float(r.pass_rate) if r.pass_rate is not None else None,
               r.last_seen, now) for r in aes_rows]
    with obs_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE agent_eval_scores")
        if values:
            execute_values(cur,
                """INSERT INTO agent_eval_scores
                   (experiment_id, scorer_name, source_type, assessment_count,
                    pass_count, fail_count, pass_rate, last_seen, last_synced)
                   VALUES %s""",
                values, page_size=500)
    obs_conn.commit()
    aes_count = len(values)
    print(f"  ✅ {aes_count} agent eval-score rows synced")
except Exception as exc:
    obs_conn.rollback()
    print(f"  ⚠️  agent_eval_scores sync failed: {exc}")

# Sync ai_audit_summary (#8 — per service·action rollup from system.access.audit).
AI_AUDIT_SUMMARY_DELTA = f"{CATALOG}.{SCHEMA}.ai_audit_summary"
aas_count = 0
print(f"▸ Syncing {AI_AUDIT_SUMMARY_DELTA} → ai_audit_summary ...")
try:
    aas_rows = spark.read.table(AI_AUDIT_SUMMARY_DELTA).collect()
    values = [(r.service_name, r.action_name, int(r.event_count or 0), int(r.actor_count or 0),
               int(r.error_count or 0), int(r.workspace_count or 0), r.last_seen, now) for r in aas_rows]
    with obs_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE ai_audit_summary")
        if values:
            execute_values(cur,
                """INSERT INTO ai_audit_summary
                   (service_name, action_name, event_count, actor_count, error_count,
                    workspace_count, last_seen, last_synced)
                   VALUES %s""",
                values, page_size=500)
    obs_conn.commit()
    aas_count = len(values)
    print(f"  ✅ {aas_count} AI audit-summary rows synced")
except Exception as exc:
    obs_conn.rollback()
    print(f"  ⚠️  ai_audit_summary sync failed: {exc}")

# Sync ai_audit_recent (#8 — capped recent-event feed).
AI_AUDIT_RECENT_DELTA = f"{CATALOG}.{SCHEMA}.ai_audit_recent"
aar_count = 0
print(f"▸ Syncing {AI_AUDIT_RECENT_DELTA} → ai_audit_recent ...")
try:
    aar_rows = spark.read.table(AI_AUDIT_RECENT_DELTA).collect()
    values = [(r.event_time, r.service_name, r.action_name, r.actor,
               int(r.status_code) if r.status_code is not None else None,
               r.workspace_id, now) for r in aar_rows]
    with obs_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE ai_audit_recent")
        if values:
            execute_values(cur,
                """INSERT INTO ai_audit_recent
                   (event_time, service_name, action_name, actor, status_code,
                    workspace_id, last_synced)
                   VALUES %s""",
                values, page_size=500)
    obs_conn.commit()
    aar_count = len(values)
    print(f"  ✅ {aar_count} AI audit-recent rows synced")
except Exception as exc:
    obs_conn.rollback()
    print(f"  ⚠️  ai_audit_recent sync failed: {exc}")

# Sync mlflow_registered_models (UC registered models from 04_discover_observability).
MODEL_REGISTRY_DELTA = f"{CATALOG}.{SCHEMA}.mlflow_registered_models"
mr_count = 0
print(f"▸ Syncing {MODEL_REGISTRY_DELTA} → mlflow_registered_models ...")
try:
    mr_rows = spark.read.table(MODEL_REGISTRY_DELTA).collect()
    values = [(r.name, r.workspace_id, r.user_id,
               int(r.last_updated_timestamp) if r.last_updated_timestamp is not None else None,
               int(r.creation_timestamp) if r.creation_timestamp is not None else None,
               r.description, r.aliases, r.latest_versions, now) for r in mr_rows]
    with obs_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE mlflow_registered_models")
        if values:
            execute_values(cur,
                """INSERT INTO mlflow_registered_models
                   (name, workspace_id, user_id, last_updated_timestamp, creation_timestamp,
                    description, aliases, latest_versions, last_synced)
                   VALUES %s""",
                values, page_size=500)
    obs_conn.commit()
    mr_count = len(values)
    print(f"  ✅ {mr_count} registered-model rows synced")
except Exception as exc:
    obs_conn.rollback()
    print(f"  ⚠️  mlflow_registered_models sync failed: {exc}")

# COMMAND ----------

# Sync mlflow_model_versions (all versions per model from 04_discover_observability).
MODEL_VERSIONS_DELTA = f"{CATALOG}.{SCHEMA}.mlflow_model_versions"
mv_count = 0
print(f"▸ Syncing {MODEL_VERSIONS_DELTA} → mlflow_model_versions ...")
try:
    mv_rows = spark.read.table(MODEL_VERSIONS_DELTA).collect()
    values = [(r.name, r.version, r.workspace_id, r.user_id,
               int(r.creation_timestamp) if r.creation_timestamp is not None else None,
               int(r.last_updated_timestamp) if r.last_updated_timestamp is not None else None,
               r.status, r.description, r.source, r.run_id, r.aliases, now) for r in mv_rows]
    with obs_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE mlflow_model_versions")
        if values:
            execute_values(cur,
                """INSERT INTO mlflow_model_versions
                   (name, version, workspace_id, user_id, creation_timestamp,
                    last_updated_timestamp, status, description, source, run_id, aliases, last_synced)
                   VALUES %s""",
                values, page_size=500)
    obs_conn.commit()
    mv_count = len(values)
    print(f"  ✅ {mv_count} model-version rows synced")
except Exception as exc:
    obs_conn.rollback()
    print(f"  ⚠️  mlflow_model_versions sync failed: {exc}")

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
    with kb_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE vector_search_endpoints")
        cur.execute("TRUNCATE TABLE vector_search_indexes")
        # Don't truncate health_history — it's append-only for uptime tracking
        cur.execute("TRUNCATE TABLE lakebase_instances")
        kb_conn.commit()
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
# MAGIC ## Phase 4: Sync KB Billing (Delta → Lakebase)

# COMMAND ----------

KB_BILLING_DELTA = f"{CATALOG}.{SCHEMA}.kb_billing_daily"

kb_billing_conn = get_lakebase_connection()
vs_billing_count = 0
lb_billing_count = 0

# Ensure billing cache table
with kb_billing_conn.cursor() as cur:
    for ddl in [
        """CREATE TABLE IF NOT EXISTS kb_billing_daily (
            usage_date          DATE NOT NULL,
            product             TEXT NOT NULL,
            workspace_id        TEXT NOT NULL,
            endpoint_name       TEXT DEFAULT '',
            workload_type       TEXT DEFAULT 'other',
            total_dbus          NUMERIC(18,4) DEFAULT 0,
            total_cost_usd      NUMERIC(18,4) DEFAULT 0,
            last_synced         TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (usage_date, product, workspace_id, endpoint_name, workload_type)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_kbd_product ON kb_billing_daily (product)",
        "CREATE INDEX IF NOT EXISTS idx_kbd_ws ON kb_billing_daily (workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_kbd_date ON kb_billing_daily (usage_date DESC)",
    ]:
        try:
            cur.execute(ddl)
        except Exception as e:
            print(f"  DDL warning: {e}")
    kb_billing_conn.commit()

# Sync from Delta (populated by 07_discover_kb_billing task)
print(f"▸ Syncing KB billing from Delta: {KB_BILLING_DELTA} ...")
try:
    with kb_billing_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE kb_billing_daily")
        kb_billing_conn.commit()
    kb_df = spark.read.table(KB_BILLING_DELTA)
    kb_rows = kb_df.collect()
    if kb_rows:
        values = [(r.usage_date, r.product, r.workspace_id,
                   r.endpoint_name or "", r.workload_type or "other",
                   float(r.total_dbus or 0), float(r.total_cost_usd or 0))
                  for r in kb_rows]
        vs_billing_count = sum(1 for r in kb_rows if r.product == "VECTOR_SEARCH")
        lb_billing_count = len(kb_rows) - vs_billing_count
        with kb_billing_conn.cursor() as cur:
            execute_values(cur,
                """INSERT INTO kb_billing_daily
                   (usage_date, product, workspace_id, endpoint_name, workload_type, total_dbus, total_cost_usd, last_synced)
                   VALUES %s
                   ON CONFLICT (usage_date, product, workspace_id, endpoint_name, workload_type) DO UPDATE SET
                       total_dbus = EXCLUDED.total_dbus, total_cost_usd = EXCLUDED.total_cost_usd,
                       last_synced = NOW()""",
                [(v[0], v[1], v[2], v[3], v[4], v[5], v[6], now) for v in values],
                page_size=500)
            kb_billing_conn.commit()
    print(f"  ✅ Synced {vs_billing_count} VS + {lb_billing_count} Lakebase billing rows")
except Exception as exc:
    print(f"  ⚠️  KB billing sync failed: {exc}")

kb_billing_conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 5: Sync User Analytics (Delta → Lakebase)

# COMMAND ----------

UA_DAILY_TABLE = f"{CATALOG}.{SCHEMA}.user_analytics_daily"
UA_HEATMAP_TABLE = f"{CATALOG}.{SCHEMA}.user_analytics_heatmap"

ua_conn = get_lakebase_connection()
ua_daily_count = 0
ua_heatmap_count = 0

# Ensure cache tables
with ua_conn.cursor() as cur:
    for ddl in [
        """CREATE TABLE IF NOT EXISTS user_analytics_daily (
            usage_date      DATE NOT NULL,
            requester       TEXT NOT NULL,
            endpoint_name   TEXT NOT NULL,
            request_count   BIGINT DEFAULT 0,
            total_tokens    BIGINT DEFAULT 0,
            last_synced     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (usage_date, requester, endpoint_name)
        )""",
        "CREATE INDEX IF NOT EXISTS idx_uad_date ON user_analytics_daily (usage_date DESC)",
        "CREATE INDEX IF NOT EXISTS idx_uad_user ON user_analytics_daily (requester)",
        """CREATE TABLE IF NOT EXISTS user_analytics_heatmap (
            dow             INT NOT NULL,
            hour            INT NOT NULL,
            request_count   BIGINT DEFAULT 0,
            period_days     INT DEFAULT 30,
            last_synced     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (dow, hour, period_days)
        )""",
    ]:
        try:
            cur.execute(ddl)
        except Exception as e:
            print(f"  DDL warning: {e}")
    ua_conn.commit()

# COMMAND ----------

# Sync daily activity from Delta
print(f"▸ Syncing user analytics daily from Delta: {UA_DAILY_TABLE} ...")
try:
    with ua_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE user_analytics_daily")
        cur.execute("TRUNCATE TABLE user_analytics_heatmap")
        ua_conn.commit()
    ua_df = spark.read.table(UA_DAILY_TABLE)
    ua_rows = ua_df.collect()
    if ua_rows:
        values = [(r.usage_date, r.requester, r.endpoint_name, r.request_count, r.total_tokens, now)
                  for r in ua_rows]
        ua_daily_count = len(values)
        with ua_conn.cursor() as cur:
            execute_values(cur,
                """INSERT INTO user_analytics_daily
                   (usage_date, requester, endpoint_name, request_count, total_tokens, last_synced)
                   VALUES %s""",
                values, page_size=500)
            ua_conn.commit()
    print(f"  ✅ {ua_daily_count} daily rows synced")
except Exception as exc:
    print(f"  ⚠️  UA daily sync failed: {exc}")

# COMMAND ----------

# Sync heatmap from Delta
print(f"▸ Syncing heatmap from Delta: {UA_HEATMAP_TABLE} ...")
try:
    hm_df = spark.read.table(UA_HEATMAP_TABLE)
    hm_rows = hm_df.collect()
    if hm_rows:
        values = [(r.dow, r.hour, r.request_count, r.period_days, now) for r in hm_rows]
        ua_heatmap_count = len(values)
        with ua_conn.cursor() as cur:
            execute_values(cur,
                """INSERT INTO user_analytics_heatmap (dow, hour, request_count, period_days, last_synced)
                   VALUES %s""",
                values, page_size=500)
            ua_conn.commit()
    print(f"  ✅ {ua_heatmap_count} heatmap rows synced")
except Exception as exc:
    print(f"  ⚠️  UA heatmap sync failed: {exc}")

# COMMAND ----------

ua_conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 6: Sync Gateway Usage (Delta → Lakebase)

# COMMAND ----------

GW_DAILY_TABLE = f"{CATALOG}.{SCHEMA}.gateway_usage_daily"
GW_HOURLY_TABLE = f"{CATALOG}.{SCHEMA}.gateway_usage_hourly"

gw_conn = get_lakebase_connection()
gw_daily_count = 0
gw_hourly_count = 0

_gw_ddls = [
    """CREATE TABLE IF NOT EXISTS gateway_usage_daily (
        usage_date      DATE NOT NULL,
        endpoint_name   TEXT NOT NULL,
        requester       TEXT DEFAULT '',
        request_count   BIGINT DEFAULT 0,
        input_tokens    BIGINT DEFAULT 0,
        output_tokens   BIGINT DEFAULT 0,
        error_count     BIGINT DEFAULT 0,
        rate_limited_count BIGINT DEFAULT 0,
        last_synced     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        PRIMARY KEY (usage_date, endpoint_name, requester)
    )""",
    """CREATE TABLE IF NOT EXISTS gateway_usage_hourly (
        hour            TEXT NOT NULL,
        endpoint_name   TEXT DEFAULT '',
        request_count   BIGINT DEFAULT 0,
        input_tokens    BIGINT DEFAULT 0,
        output_tokens   BIGINT DEFAULT 0,
        error_count     BIGINT DEFAULT 0,
        rate_limited_count BIGINT DEFAULT 0,
        last_synced     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        PRIMARY KEY (hour, endpoint_name)
    )""",
    "CREATE INDEX IF NOT EXISTS idx_gud_date ON gateway_usage_daily (usage_date DESC)",
    "CREATE INDEX IF NOT EXISTS idx_gud_ep ON gateway_usage_daily (endpoint_name)",
    # ADD COLUMN for tables that pre-existed without rate_limited_count
    "ALTER TABLE gateway_usage_daily  ADD COLUMN IF NOT EXISTS rate_limited_count BIGINT DEFAULT 0",
    "ALTER TABLE gateway_usage_hourly ADD COLUMN IF NOT EXISTS rate_limited_count BIGINT DEFAULT 0",
]
# Run each DDL in its own transaction so a single failure cannot abort the
# whole batch (psycopg2 marks the transaction aborted on any error and
# silently no-ops subsequent statements until rollback).
for ddl in _gw_ddls:
    try:
        with gw_conn.cursor() as cur:
            cur.execute(ddl)
        gw_conn.commit()
    except Exception as e:
        gw_conn.rollback()
        print(f"  DDL warning: {e}")

# COMMAND ----------

# Sync daily
print(f"▸ Syncing gateway daily usage from Delta: {GW_DAILY_TABLE} ...")
try:
    with gw_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE gateway_usage_daily")
        cur.execute("TRUNCATE TABLE gateway_usage_hourly")
        gw_conn.commit()
    gw_df = spark.read.table(GW_DAILY_TABLE)
    gw_rows = gw_df.collect()
    if gw_rows:
        values = [(r.usage_date, r.endpoint_name, r.requester or "",
                   r.request_count, r.input_tokens, r.output_tokens, r.error_count,
                   getattr(r, "rate_limited_count", 0) or 0, now)
                  for r in gw_rows]
        gw_daily_count = len(values)
        with gw_conn.cursor() as cur:
            execute_values(cur,
                """INSERT INTO gateway_usage_daily
                   (usage_date, endpoint_name, requester, request_count, input_tokens, output_tokens, error_count, rate_limited_count, last_synced)
                   VALUES %s""",
                values, page_size=500)
            gw_conn.commit()
    print(f"  ✅ {gw_daily_count} daily rows synced")
except Exception as exc:
    print(f"  ⚠️  Gateway daily sync failed: {exc}")

# COMMAND ----------

# Sync hourly
print(f"▸ Syncing gateway hourly usage from Delta: {GW_HOURLY_TABLE} ...")
try:
    gh_df = spark.read.table(GW_HOURLY_TABLE)
    gh_rows = gh_df.collect()
    if gh_rows:
        values = [(r.hour, r.endpoint_name or "", r.request_count,
                   r.input_tokens, r.output_tokens, r.error_count,
                   getattr(r, "rate_limited_count", 0) or 0, now)
                  for r in gh_rows]
        gw_hourly_count = len(values)
        with gw_conn.cursor() as cur:
            execute_values(cur,
                """INSERT INTO gateway_usage_hourly
                   (hour, endpoint_name, request_count, input_tokens, output_tokens, error_count, rate_limited_count, last_synced)
                   VALUES %s""",
                values, page_size=500)
            gw_conn.commit()
    print(f"  ✅ {gw_hourly_count} hourly rows synced")
except Exception as exc:
    print(f"  ⚠️  Gateway hourly sync failed: {exc}")

# COMMAND ----------

gw_conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Sync Unity AI Gateway (v2) usage summary (Delta → Lakebase)
# MAGIC Mirrors `uag_usage_summary` from `11_discover_ai_gateway_usage`.

# COMMAND ----------

UAG_TABLE = f"{CATALOG}.{SCHEMA}.uag_usage_summary"
uag_conn = get_lakebase_connection()
uag_count = 0

with uag_conn.cursor() as cur:
    cur.execute(
        """CREATE TABLE IF NOT EXISTS uag_usage_summary (
            endpoint_name         TEXT NOT NULL,
            request_count         BIGINT DEFAULT 0,
            input_tokens          BIGINT DEFAULT 0,
            output_tokens         BIGINT DEFAULT 0,
            cache_read_tokens     BIGINT DEFAULT 0,
            cache_creation_tokens BIGINT DEFAULT 0,
            p50_latency_ms        BIGINT DEFAULT 0,
            p90_latency_ms        BIGINT DEFAULT 0,
            p95_latency_ms        BIGINT DEFAULT 0,
            p99_latency_ms        BIGINT DEFAULT 0,
            p95_ttfb_ms           BIGINT DEFAULT 0,
            error_count           BIGINT DEFAULT 0,
            unique_users          BIGINT DEFAULT 0,
            max_event_time        TEXT,
            last_synced           TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (endpoint_name)
        )""")
    # ADD COLUMN for tables that pre-existed without the p90/p99 percentiles.
    cur.execute("ALTER TABLE uag_usage_summary ADD COLUMN IF NOT EXISTS p90_latency_ms BIGINT DEFAULT 0")
    cur.execute("ALTER TABLE uag_usage_summary ADD COLUMN IF NOT EXISTS p99_latency_ms BIGINT DEFAULT 0")
    uag_conn.commit()

print(f"▸ Syncing {UAG_TABLE} → uag_usage_summary ...")
try:
    with uag_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE uag_usage_summary")
        uag_conn.commit()
    uag_rows = spark.read.table(UAG_TABLE).collect()
    if uag_rows:
        values = [(r.endpoint_name, int(r.request_count or 0), int(r.input_tokens or 0),
                   int(r.output_tokens or 0), int(r.cache_read_tokens or 0), int(r.cache_creation_tokens or 0),
                   int(r.p50_latency_ms or 0), int(r.p90_latency_ms or 0), int(r.p95_latency_ms or 0),
                   int(r.p99_latency_ms or 0), int(r.p95_ttfb_ms or 0),
                   int(r.error_count or 0), int(r.unique_users or 0), r.max_event_time, now)
                  for r in uag_rows]
        uag_count = len(values)
        with uag_conn.cursor() as cur:
            execute_values(cur,
                """INSERT INTO uag_usage_summary
                   (endpoint_name, request_count, input_tokens, output_tokens, cache_read_tokens,
                    cache_creation_tokens, p50_latency_ms, p90_latency_ms, p95_latency_ms, p99_latency_ms,
                    p95_ttfb_ms, error_count, unique_users, max_event_time, last_synced)
                   VALUES %s
                   ON CONFLICT (endpoint_name) DO UPDATE SET
                       request_count = EXCLUDED.request_count,
                       input_tokens = EXCLUDED.input_tokens,
                       output_tokens = EXCLUDED.output_tokens,
                       cache_read_tokens = EXCLUDED.cache_read_tokens,
                       cache_creation_tokens = EXCLUDED.cache_creation_tokens,
                       p50_latency_ms = EXCLUDED.p50_latency_ms,
                       p90_latency_ms = EXCLUDED.p90_latency_ms,
                       p95_latency_ms = EXCLUDED.p95_latency_ms,
                       p99_latency_ms = EXCLUDED.p99_latency_ms,
                       p95_ttfb_ms = EXCLUDED.p95_ttfb_ms,
                       error_count = EXCLUDED.error_count,
                       unique_users = EXCLUDED.unique_users,
                       max_event_time = EXCLUDED.max_event_time,
                       last_synced = NOW()""",
                values, page_size=500)
            uag_conn.commit()
    print(f"  ✅ {uag_count} UAG usage rows synced")
except Exception as exc:
    print(f"  ⚠️  uag_usage_summary sync failed: {exc}")

# Sync uag_usage_breakdown (agent-vs-human / by model / by api_type)
UAG_BREAKDOWN_TABLE = f"{CATALOG}.{SCHEMA}.uag_usage_breakdown"
uag_bd_count = 0
with uag_conn.cursor() as cur:
    cur.execute(
        """CREATE TABLE IF NOT EXISTS uag_usage_breakdown (
            dimension     TEXT NOT NULL,
            key           TEXT NOT NULL,
            request_count BIGINT DEFAULT 0,
            input_tokens  BIGINT DEFAULT 0,
            output_tokens BIGINT DEFAULT 0,
            cached_tokens BIGINT DEFAULT 0,
            last_synced   TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (dimension, key)
        )""")
    uag_conn.commit()
print(f"▸ Syncing {UAG_BREAKDOWN_TABLE} → uag_usage_breakdown ...")
try:
    with uag_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE uag_usage_breakdown")
        uag_conn.commit()
    bd_rows = spark.read.table(UAG_BREAKDOWN_TABLE).collect()
    if bd_rows:
        values = [(r.dimension, r.key, int(r.request_count or 0), int(r.input_tokens or 0),
                   int(r.output_tokens or 0), int(r.cached_tokens or 0), now)
                  for r in bd_rows]
        uag_bd_count = len(values)
        with uag_conn.cursor() as cur:
            execute_values(cur,
                """INSERT INTO uag_usage_breakdown
                   (dimension, key, request_count, input_tokens, output_tokens, cached_tokens, last_synced)
                   VALUES %s
                   ON CONFLICT (dimension, key) DO UPDATE SET
                       request_count = EXCLUDED.request_count,
                       input_tokens = EXCLUDED.input_tokens,
                       output_tokens = EXCLUDED.output_tokens,
                       cached_tokens = EXCLUDED.cached_tokens,
                       last_synced = NOW()""",
                values, page_size=500)
            uag_conn.commit()
    print(f"  ✅ {uag_bd_count} UAG breakdown rows synced")
except Exception as exc:
    print(f"  ⚠️  uag_usage_breakdown sync failed: {exc}")

# Sync uag_mcp_tool_daily (per-tool MCP activity). tool_name is nullable
# (server-level calls), so no composite PK — full-refresh via TRUNCATE + INSERT.
UAG_MCP_TOOL_TABLE = f"{CATALOG}.{SCHEMA}.uag_mcp_tool_daily"
uag_mcp_count = 0
with uag_conn.cursor() as cur:
    cur.execute(
        """CREATE TABLE IF NOT EXISTS uag_mcp_tool_daily (
            service_name   TEXT NOT NULL,
            tool_name      TEXT,
            server_type    TEXT,
            request_count  BIGINT DEFAULT 0,
            error_count    BIGINT DEFAULT 0,
            unique_users   BIGINT DEFAULT 0,
            max_event_time TEXT,
            last_synced    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_umt_service ON uag_mcp_tool_daily (service_name)")
    uag_conn.commit()
print(f"▸ Syncing {UAG_MCP_TOOL_TABLE} → uag_mcp_tool_daily ...")
try:
    # Read Delta BEFORE truncating: a transient read failure after an early
    # TRUNCATE would otherwise leave the Lakebase cache empty (silent, since
    # this table is EXPECTED-not-REQUIRED in the smoke check).
    mcp_rows = spark.read.table(UAG_MCP_TOOL_TABLE).collect()
    with uag_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE uag_mcp_tool_daily")
        uag_conn.commit()
    if mcp_rows:
        values = [(r.service_name, r.tool_name, r.server_type, int(r.request_count or 0),
                   int(r.error_count or 0), int(r.unique_users or 0), r.max_event_time, now)
                  for r in mcp_rows]
        uag_mcp_count = len(values)
        with uag_conn.cursor() as cur:
            execute_values(cur,
                """INSERT INTO uag_mcp_tool_daily
                   (service_name, tool_name, server_type, request_count, error_count,
                    unique_users, max_event_time, last_synced)
                   VALUES %s""",
                values, page_size=500)
            uag_conn.commit()
    print(f"  ✅ {uag_mcp_count} UAG MCP tool rows synced")
except Exception as exc:
    uag_conn.rollback()  # clear aborted txn so the next block's DDL doesn't crash
    print(f"  ⚠️  uag_mcp_tool_daily sync failed: {exc}")

# Sync uag_guardrail_daily (guardrail coverage per guarded endpoint).
UAG_GUARDRAIL_TABLE = f"{CATALOG}.{SCHEMA}.uag_guardrail_daily"
uag_gr_count = 0
with uag_conn.cursor() as cur:
    cur.execute(
        """CREATE TABLE IF NOT EXISTS uag_guardrail_daily (
            endpoint_name    TEXT NOT NULL,
            checked_requests BIGINT DEFAULT 0,
            unique_users     BIGINT DEFAULT 0,
            judge_models     TEXT,
            max_event_time   TEXT,
            last_synced      TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ugr_ep ON uag_guardrail_daily (endpoint_name)")
    uag_conn.commit()
print(f"▸ Syncing {UAG_GUARDRAIL_TABLE} → uag_guardrail_daily ...")
try:
    gr_rows = spark.read.table(UAG_GUARDRAIL_TABLE).collect()
    with uag_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE uag_guardrail_daily")
        uag_conn.commit()
    if gr_rows:
        values = [(r.endpoint_name, int(r.checked_requests or 0), int(r.unique_users or 0),
                   r.judge_models, r.max_event_time, now) for r in gr_rows]
        uag_gr_count = len(values)
        with uag_conn.cursor() as cur:
            execute_values(cur,
                """INSERT INTO uag_guardrail_daily
                   (endpoint_name, checked_requests, unique_users, judge_models, max_event_time, last_synced)
                   VALUES %s""",
                values, page_size=500)
            uag_conn.commit()
    print(f"  ✅ {uag_gr_count} UAG guardrail rows synced")
except Exception as exc:
    uag_conn.rollback()  # clear aborted txn so the next block's DDL doesn't crash
    print(f"  ⚠️  uag_guardrail_daily sync failed: {exc}")

# Sync uag_usage_timeseries_daily (daily requests/tokens for v2 trend charts).
UAG_TIMESERIES_TABLE = f"{CATALOG}.{SCHEMA}.uag_usage_timeseries_daily"
uag_ts_count = 0
with uag_conn.cursor() as cur:
    cur.execute(
        """CREATE TABLE IF NOT EXISTS uag_usage_timeseries_daily (
            usage_date     TEXT NOT NULL,
            request_count  BIGINT DEFAULT 0,
            input_tokens   BIGINT DEFAULT 0,
            output_tokens  BIGINT DEFAULT 0,
            last_synced    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (usage_date)
        )""")
    uag_conn.commit()
print(f"▸ Syncing {UAG_TIMESERIES_TABLE} → uag_usage_timeseries_daily ...")
try:
    ts_rows = spark.read.table(UAG_TIMESERIES_TABLE).collect()
    with uag_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE uag_usage_timeseries_daily")
        uag_conn.commit()
    if ts_rows:
        values = [(str(r.usage_date), int(r.request_count or 0), int(r.input_tokens or 0),
                   int(r.output_tokens or 0), now) for r in ts_rows]
        uag_ts_count = len(values)
        with uag_conn.cursor() as cur:
            execute_values(cur,
                """INSERT INTO uag_usage_timeseries_daily
                   (usage_date, request_count, input_tokens, output_tokens, last_synced)
                   VALUES %s""",
                values, page_size=500)
            uag_conn.commit()
    print(f"  ✅ {uag_ts_count} UAG time-series rows synced")
except Exception as exc:
    uag_conn.rollback()  # clear aborted txn so the next block's DDL doesn't crash
    print(f"  ⚠️  uag_usage_timeseries_daily sync failed: {exc}")

# Sync uag_coding_agent_usage (coding-agent activity classified by user_agent).
UAG_CODING_AGENT_TABLE = f"{CATALOG}.{SCHEMA}.uag_coding_agent_usage"
uag_ca_count = 0
with uag_conn.cursor() as cur:
    cur.execute(
        """CREATE TABLE IF NOT EXISTS uag_coding_agent_usage (
            coding_agent   TEXT NOT NULL,
            request_count  BIGINT DEFAULT 0,
            unique_users   BIGINT DEFAULT 0,
            active_days    BIGINT DEFAULT 0,
            total_tokens   BIGINT DEFAULT 0,
            max_event_time TEXT,
            last_synced    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (coding_agent))""")
    uag_conn.commit()
print(f"▸ Syncing {UAG_CODING_AGENT_TABLE} → uag_coding_agent_usage ...")
try:
    ca_rows = spark.read.table(UAG_CODING_AGENT_TABLE).collect()
    values = [(r.coding_agent, int(r.request_count or 0), int(r.unique_users or 0),
               int(r.active_days or 0), int(r.total_tokens or 0), r.max_event_time, now) for r in ca_rows]
    with uag_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE uag_coding_agent_usage")
        if values:
            execute_values(cur,
                """INSERT INTO uag_coding_agent_usage
                   (coding_agent, request_count, unique_users, active_days, total_tokens, max_event_time, last_synced)
                   VALUES %s""",
                values, page_size=500)
    uag_conn.commit()
    uag_ca_count = len(values)
    print(f"  ✅ {uag_ca_count} coding-agent rows synced")
except Exception as exc:
    uag_conn.rollback()
    print(f"  ⚠️  uag_coding_agent_usage sync failed: {exc}")

# Sync uag_throttling_daily (429/5xx per endpoint — reliability signal).
UAG_THROTTLING_TABLE = f"{CATALOG}.{SCHEMA}.uag_throttling_daily"
uag_th_count = 0
with uag_conn.cursor() as cur:
    cur.execute(
        """CREATE TABLE IF NOT EXISTS uag_throttling_daily (
            endpoint_name      TEXT NOT NULL,
            total_requests     BIGINT DEFAULT 0,
            throttled_count    BIGINT DEFAULT 0,
            server_error_count BIGINT DEFAULT 0,
            max_event_time     TEXT,
            last_synced        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (endpoint_name))""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_uth_ep ON uag_throttling_daily (endpoint_name)")
    uag_conn.commit()
print(f"▸ Syncing {UAG_THROTTLING_TABLE} → uag_throttling_daily ...")
try:
    th_rows = spark.read.table(UAG_THROTTLING_TABLE).collect()
    values = [(r.endpoint_name, int(r.total_requests or 0), int(r.throttled_count or 0),
               int(r.server_error_count or 0), r.max_event_time, now) for r in th_rows]
    with uag_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE uag_throttling_daily")
        if values:
            execute_values(cur,
                """INSERT INTO uag_throttling_daily
                   (endpoint_name, total_requests, throttled_count, server_error_count, max_event_time, last_synced)
                   VALUES %s""",
                values, page_size=500)
    uag_conn.commit()
    uag_th_count = len(values)
    print(f"  ✅ {uag_th_count} throttling rows synced")
except Exception as exc:
    uag_conn.rollback()
    print(f"  ⚠️  uag_throttling_daily sync failed: {exc}")

# Sync uag_fallback_routing_daily (smart-routing fallback per endpoint).
UAG_FALLBACK_TABLE = f"{CATALOG}.{SCHEMA}.uag_fallback_routing_daily"
uag_fb_count = 0
with uag_conn.cursor() as cur:
    cur.execute(
        """CREATE TABLE IF NOT EXISTS uag_fallback_routing_daily (
            endpoint_name         TEXT NOT NULL,
            total_requests        BIGINT DEFAULT 0,
            fallback_requests     BIGINT DEFAULT 0,
            fallback_recovered    BIGINT DEFAULT 0,
            fallback_destinations TEXT,
            max_event_time        TEXT,
            last_synced           TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (endpoint_name))""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ufb_ep ON uag_fallback_routing_daily (endpoint_name)")
    uag_conn.commit()
print(f"▸ Syncing {UAG_FALLBACK_TABLE} → uag_fallback_routing_daily ...")
try:
    fb_rows = spark.read.table(UAG_FALLBACK_TABLE).collect()
    values = [(r.endpoint_name, int(r.total_requests or 0), int(r.fallback_requests or 0),
               int(r.fallback_recovered or 0), r.fallback_destinations, r.max_event_time, now) for r in fb_rows]
    with uag_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE uag_fallback_routing_daily")
        if values:
            execute_values(cur,
                """INSERT INTO uag_fallback_routing_daily
                   (endpoint_name, total_requests, fallback_requests, fallback_recovered,
                    fallback_destinations, max_event_time, last_synced)
                   VALUES %s""",
                values, page_size=500)
    uag_conn.commit()
    uag_fb_count = len(values)
    print(f"  ✅ {uag_fb_count} fallback-routing rows synced")
except Exception as exc:
    uag_conn.rollback()
    print(f"  ⚠️  uag_fallback_routing_daily sync failed: {exc}")

# Sync uag_budget_status (account Budgets API config inventory — F5, read-only).
UAG_BUDGET_TABLE = f"{CATALOG}.{SCHEMA}.uag_budget_status"
uag_budget_count = 0
with uag_conn.cursor() as cur:
    cur.execute(
        """CREATE TABLE IF NOT EXISTS uag_budget_status (
            budget_id         TEXT NOT NULL,
            account_id        TEXT,
            display_name      TEXT,
            enforce           BOOLEAN DEFAULT FALSE,
            alerting          BOOLEAN DEFAULT FALSE,
            min_threshold_usd NUMERIC(18,2),
            max_threshold_usd NUMERIC(18,2),
            time_period       TEXT,
            filter_summary    TEXT,
            is_ai             BOOLEAN DEFAULT FALSE,
            spent_usd         NUMERIC(18,2),
            pct_used          DOUBLE PRECISION,
            discovered_at     TIMESTAMP WITH TIME ZONE,
            last_synced       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (budget_id))""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_ubs_ai ON uag_budget_status (is_ai)")
    # additive columns for pre-existing tables (workflow-owned, so ALTER is safe here)
    cur.execute("ALTER TABLE uag_budget_status ADD COLUMN IF NOT EXISTS spent_usd NUMERIC(18,2)")
    cur.execute("ALTER TABLE uag_budget_status ADD COLUMN IF NOT EXISTS pct_used DOUBLE PRECISION")
    uag_conn.commit()
print(f"▸ Syncing {UAG_BUDGET_TABLE} → uag_budget_status ...")
try:
    bg_rows = spark.read.table(UAG_BUDGET_TABLE).collect()
    values = [(r.budget_id, r.account_id, r.display_name, bool(r.enforce), bool(r.alerting),
               r.min_threshold_usd, r.max_threshold_usd, r.time_period, r.filter_summary,
               bool(r.is_ai), r.spent_usd, r.pct_used, r.discovered_at, now) for r in bg_rows]
    with uag_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE uag_budget_status")
        if values:
            execute_values(cur,
                """INSERT INTO uag_budget_status
                   (budget_id, account_id, display_name, enforce, alerting,
                    min_threshold_usd, max_threshold_usd, time_period, filter_summary,
                    is_ai, spent_usd, pct_used, discovered_at, last_synced)
                   VALUES %s""",
                values, page_size=500)
    uag_conn.commit()
    uag_budget_count = len(values)
    print(f"  ✅ {uag_budget_count} budget rows synced")
except Exception as exc:
    uag_conn.rollback()
    print(f"  ⚠️  uag_budget_status sync failed: {exc}")

# Sync serving_endpoints_inventory (account-wide served-entity inventory, read-only).
EP_INV_TABLE = f"{CATALOG}.{SCHEMA}.serving_endpoints_inventory"
ep_inv_count = 0
with uag_conn.cursor() as cur:
    cur.execute(
        """CREATE TABLE IF NOT EXISTS serving_endpoints_inventory (
            served_entity_id        TEXT NOT NULL,
            endpoint_id             TEXT,
            endpoint_name           TEXT,
            workspace_id            TEXT,
            served_entity_name      TEXT,
            entity_type             TEXT,
            entity_name             TEXT,
            entity_version          TEXT,
            provider                TEXT,
            task                    TEXT,
            created_by              TEXT,
            endpoint_config_version INTEGER,
            change_time             TIMESTAMP WITH TIME ZONE,
            discovered_at           TIMESTAMP WITH TIME ZONE,
            last_synced             TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (served_entity_id))""")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sei_ws ON serving_endpoints_inventory (workspace_id)")
    cur.execute("CREATE INDEX IF NOT EXISTS idx_sei_type ON serving_endpoints_inventory (entity_type)")
    uag_conn.commit()
print(f"▸ Syncing {EP_INV_TABLE} → serving_endpoints_inventory ...")
try:
    ei_rows = spark.read.table(EP_INV_TABLE).collect()
    values = [(r.served_entity_id, r.endpoint_id, r.endpoint_name, r.workspace_id,
               r.served_entity_name, r.entity_type, r.entity_name, r.entity_version,
               r.provider, r.task, r.created_by, r.endpoint_config_version,
               r.change_time, r.discovered_at, now) for r in ei_rows]
    with uag_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE serving_endpoints_inventory")
        if values:
            execute_values(cur,
                """INSERT INTO serving_endpoints_inventory
                   (served_entity_id, endpoint_id, endpoint_name, workspace_id,
                    served_entity_name, entity_type, entity_name, entity_version,
                    provider, task, created_by, endpoint_config_version,
                    change_time, discovered_at, last_synced)
                   VALUES %s""",
                values, page_size=1000)
    uag_conn.commit()
    ep_inv_count = len(values)
    print(f"  ✅ {ep_inv_count} endpoint-inventory rows synced")
except Exception as exc:
    uag_conn.rollback()
    print(f"  ⚠️  serving_endpoints_inventory sync failed: {exc}")

# Sync model_services_inventory (v3 UC model services — account-wide, read-only).
MS_INV_TABLE = f"{CATALOG}.{SCHEMA}.model_services_inventory"
ms_inv_count = 0
with uag_conn.cursor() as cur:
    cur.execute(
        """CREATE TABLE IF NOT EXISTS model_services_inventory (
            full_name           TEXT NOT NULL,
            owner               TEXT,
            supported_api_types TEXT,
            create_time         TEXT,
            discovered_at       TIMESTAMP WITH TIME ZONE,
            last_synced         TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (full_name))""")
    uag_conn.commit()
print(f"▸ Syncing {MS_INV_TABLE} → model_services_inventory ...")
try:
    ms_rows = spark.read.table(MS_INV_TABLE).collect()
    values = [(r.full_name, r.owner, r.supported_api_types, r.create_time, r.discovered_at, now) for r in ms_rows]
    with uag_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE model_services_inventory")
        if values:
            execute_values(cur,
                """INSERT INTO model_services_inventory
                   (full_name, owner, supported_api_types, create_time, discovered_at, last_synced)
                   VALUES %s""",
                values, page_size=500)
    uag_conn.commit()
    ms_inv_count = len(values)
    print(f"  ✅ {ms_inv_count} model-service rows synced")
except Exception as exc:
    uag_conn.rollback()
    print(f"  ⚠️  model_services_inventory sync failed: {exc}")

uag_conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 7: Sync Billing (Delta → Lakebase)
# MAGIC
# MAGIC Mirrors the four billing tables populated by `09_discover_billing`:
# MAGIC - `billing_serving_daily`
# MAGIC - `billing_token_daily`
# MAGIC - `billing_product_daily`
# MAGIC - `billing_user_endpoint_daily`
# MAGIC
# MAGIC Also stamps `billing_cache_meta` so the app's `get_cache_status()` surfaces fresh `last_refreshed` timestamps.

# COMMAND ----------

BSD_TABLE  = f"{CATALOG}.{SCHEMA}.billing_serving_daily"
BTD_TABLE  = f"{CATALOG}.{SCHEMA}.billing_token_daily"
BPD_TABLE  = f"{CATALOG}.{SCHEMA}.billing_product_daily"
BUED_TABLE = f"{CATALOG}.{SCHEMA}.billing_user_endpoint_daily"
BUCD_TABLE = f"{CATALOG}.{SCHEMA}.billing_user_cost_daily"
BTAG_TABLE = f"{CATALOG}.{SCHEMA}.billing_cost_by_tag"
BEXT_TABLE = f"{CATALOG}.{SCHEMA}.billing_external_model_spend"

billing_conn = get_lakebase_connection()
bsd_count = 0
btd_count = 0
bpd_count = 0
bued_count = 0
bucd_count = 0
btag_count = 0
bext_count = 0

# Ensure billing tables (idempotent — also created by app's ensure_billing_tables on startup)
with billing_conn.cursor() as cur:
    for ddl in [
        """CREATE TABLE IF NOT EXISTS billing_serving_daily (
            usage_date    DATE          NOT NULL,
            workspace_id  TEXT          NOT NULL,
            endpoint_name TEXT          NOT NULL,
            sku_name      TEXT          NOT NULL DEFAULT '',
            total_dbus    NUMERIC(18,4) NOT NULL DEFAULT 0,
            total_cost_usd NUMERIC(18,4) NOT NULL DEFAULT 0,
            last_synced   TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (usage_date, workspace_id, endpoint_name, sku_name)
        )""",
        """CREATE TABLE IF NOT EXISTS billing_token_daily (
            usage_date       DATE    NOT NULL,
            workspace_id     TEXT    NOT NULL,
            endpoint_name    TEXT    NOT NULL,
            request_count    BIGINT  NOT NULL DEFAULT 0,
            input_tokens     BIGINT  NOT NULL DEFAULT 0,
            output_tokens    BIGINT  NOT NULL DEFAULT 0,
            avg_input_tokens NUMERIC(12,2) NOT NULL DEFAULT 0,
            avg_output_tokens NUMERIC(12,2) NOT NULL DEFAULT 0,
            last_synced      TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (usage_date, workspace_id, endpoint_name)
        )""",
        """CREATE TABLE IF NOT EXISTS billing_product_daily (
            usage_date              DATE          NOT NULL,
            workspace_id            TEXT          NOT NULL,
            billing_origin_product  TEXT          NOT NULL,
            total_dbus              NUMERIC(18,4) NOT NULL DEFAULT 0,
            total_cost_usd          NUMERIC(18,4) NOT NULL DEFAULT 0,
            last_synced             TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (usage_date, workspace_id, billing_origin_product)
        )""",
        """CREATE TABLE IF NOT EXISTS billing_user_endpoint_daily (
            usage_date     DATE    NOT NULL,
            workspace_id   TEXT    NOT NULL,
            endpoint_name  TEXT    NOT NULL,
            user_identity  TEXT    NOT NULL DEFAULT '',
            request_count  BIGINT  NOT NULL DEFAULT 0,
            input_tokens   BIGINT  NOT NULL DEFAULT 0,
            output_tokens  BIGINT  NOT NULL DEFAULT 0,
            last_synced    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (usage_date, workspace_id, endpoint_name, user_identity)
        )""",
        """CREATE TABLE IF NOT EXISTS billing_user_cost_daily (
            usage_date     DATE          NOT NULL,
            workspace_id   TEXT          NOT NULL,
            endpoint_id    TEXT          NOT NULL DEFAULT '',
            endpoint_name  TEXT          NOT NULL DEFAULT '',
            run_by         TEXT          NOT NULL DEFAULT '',
            total_dbus     NUMERIC(18,4) NOT NULL DEFAULT 0,
            total_cost_usd NUMERIC(18,4) NOT NULL DEFAULT 0,
            last_synced    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (usage_date, workspace_id, endpoint_id, run_by)
        )""",
        """CREATE TABLE IF NOT EXISTS billing_cost_by_tag (
            tag_key        TEXT          NOT NULL,
            tag_value      TEXT          NOT NULL,
            total_cost_usd NUMERIC(18,4) NOT NULL DEFAULT 0,
            last_synced    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (tag_key, tag_value)
        )""",
        # billing_external_model_spend is workflow-OWNED (created here, not by the
        # app's ensure_billing_tables — like agent_tool_usage / agent_eval_scores).
        # The workflow therefore owns it and can TRUNCATE/INSERT freely with no
        # owner-side ALTER and no app-restart-before-discovery ordering dependency.
        # last_synced is in the CREATE, so no reconcile ALTER is needed below.
        """CREATE TABLE IF NOT EXISTS billing_external_model_spend (
            provider       TEXT          NOT NULL DEFAULT '',
            model          TEXT          NOT NULL DEFAULT '',
            endpoint_name  TEXT          NOT NULL DEFAULT '',
            call_count     BIGINT        NOT NULL DEFAULT 0,
            total_cost_usd NUMERIC(18,6) NOT NULL DEFAULT 0,
            last_seen      TEXT,
            last_synced    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (provider, model, endpoint_name)
        )""",
        """CREATE TABLE IF NOT EXISTS billing_cache_meta (
            cache_key      TEXT PRIMARY KEY,
            last_refreshed TIMESTAMP WITH TIME ZONE,
            rows_loaded    INTEGER NOT NULL DEFAULT 0
        )""",
        "ALTER TABLE billing_cache_meta ADD COLUMN IF NOT EXISTS value_text TEXT",
        # ALTERs for tables that pre-existed without last_synced — CREATE TABLE
        # IF NOT EXISTS above is a no-op when the table already exists, so these
        # ADD COLUMN statements are needed to bring older schemas up to date.
        "ALTER TABLE billing_serving_daily        ADD COLUMN IF NOT EXISTS last_synced TIMESTAMP WITH TIME ZONE DEFAULT NOW()",
        "ALTER TABLE billing_token_daily          ADD COLUMN IF NOT EXISTS last_synced TIMESTAMP WITH TIME ZONE DEFAULT NOW()",
        "ALTER TABLE billing_product_daily        ADD COLUMN IF NOT EXISTS last_synced TIMESTAMP WITH TIME ZONE DEFAULT NOW()",
        "ALTER TABLE billing_user_endpoint_daily  ADD COLUMN IF NOT EXISTS last_synced TIMESTAMP WITH TIME ZONE DEFAULT NOW()",
        # Reconcile last_synced on billing_cost_by_tag when the workflow owns the
        # table. When the app SP owns it instead, this ALTER is permission-denied
        # (only the owner can ALTER) and rolled back harmlessly — the app's own
        # ensure_billing_tables runs the matching reconcile as the owner.
        "ALTER TABLE billing_cost_by_tag          ADD COLUMN IF NOT EXISTS last_synced TIMESTAMP WITH TIME ZONE DEFAULT NOW()",
        # (no billing_external_model_spend ALTER — workflow-owned, CREATE already has last_synced)
        "CREATE INDEX IF NOT EXISTS idx_bsd_ws  ON billing_serving_daily  (workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_btd_ws  ON billing_token_daily    (workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_bpd_ws  ON billing_product_daily  (workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_bued_ws ON billing_user_endpoint_daily (workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_bucd_ws ON billing_user_cost_daily (workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_btag_key ON billing_cost_by_tag (tag_key)",
    ]:
        # Commit each statement independently: these are all idempotent
        # CREATE/ALTER/INDEX ... IF NOT EXISTS, so one failure must not poison
        # the shared transaction and silently roll back every later statement
        # (that is exactly how the billing_cost_by_tag `last_synced` ALTER got
        # dropped, leaving the sync INSERT to fail on a missing column).
        try:
            cur.execute(ddl)
            billing_conn.commit()
        except Exception as e:
            billing_conn.rollback()
            print(f"  DDL warning: {e}")


def _stamp_cache_meta(conn, cache_key: str, rows_loaded: int) -> None:
    """Update billing_cache_meta so the app's get_cache_status sees fresh timestamps."""
    try:
        with conn.cursor() as c:
            c.execute(
                """INSERT INTO billing_cache_meta (cache_key, last_refreshed, rows_loaded)
                   VALUES (%s, NOW(), %s)
                   ON CONFLICT (cache_key) DO UPDATE
                   SET last_refreshed = NOW(), rows_loaded = EXCLUDED.rows_loaded""",
                (cache_key, rows_loaded),
            )
            conn.commit()
    except Exception as exc:
        print(f"  ⚠️  cache_meta update failed for {cache_key}: {exc}")


# Sync billing_serving_daily
print(f"▸ Syncing {BSD_TABLE} → billing_serving_daily ...")
try:
    with billing_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE billing_serving_daily")
        billing_conn.commit()
    bsd_rows = spark.read.table(BSD_TABLE).collect()
    if bsd_rows:
        values = [(r.usage_date, r.workspace_id, r.endpoint_name, r.sku_name or "",
                   float(r.total_dbus or 0), float(r.total_cost_usd or 0))
                  for r in bsd_rows]
        bsd_count = len(values)
        with billing_conn.cursor() as cur:
            execute_values(cur,
                """INSERT INTO billing_serving_daily
                   (usage_date, workspace_id, endpoint_name, sku_name, total_dbus, total_cost_usd, last_synced)
                   VALUES %s
                   ON CONFLICT (usage_date, workspace_id, endpoint_name, sku_name) DO UPDATE SET
                       total_dbus = EXCLUDED.total_dbus,
                       total_cost_usd = EXCLUDED.total_cost_usd,
                       last_synced = NOW()""",
                [(v[0], v[1], v[2], v[3], v[4], v[5], now) for v in values],
                page_size=500)
            billing_conn.commit()
    print(f"  ✅ {bsd_count} serving-cost rows synced")
    _stamp_cache_meta(billing_conn, "serving_daily", bsd_count)
except Exception as exc:
    print(f"  ⚠️  billing_serving_daily sync failed: {exc}")

# Sync billing_token_daily
print(f"▸ Syncing {BTD_TABLE} → billing_token_daily ...")
try:
    with billing_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE billing_token_daily")
        billing_conn.commit()
    btd_rows = spark.read.table(BTD_TABLE).collect()
    if btd_rows:
        values = [(r.usage_date, r.workspace_id, r.endpoint_name,
                   int(r.request_count or 0),
                   int(r.input_tokens or 0), int(r.output_tokens or 0),
                   float(r.avg_input_tokens or 0), float(r.avg_output_tokens or 0))
                  for r in btd_rows]
        btd_count = len(values)
        with billing_conn.cursor() as cur:
            execute_values(cur,
                """INSERT INTO billing_token_daily
                   (usage_date, workspace_id, endpoint_name, request_count,
                    input_tokens, output_tokens, avg_input_tokens, avg_output_tokens, last_synced)
                   VALUES %s
                   ON CONFLICT (usage_date, workspace_id, endpoint_name) DO UPDATE SET
                       request_count = EXCLUDED.request_count,
                       input_tokens = EXCLUDED.input_tokens,
                       output_tokens = EXCLUDED.output_tokens,
                       avg_input_tokens = EXCLUDED.avg_input_tokens,
                       avg_output_tokens = EXCLUDED.avg_output_tokens,
                       last_synced = NOW()""",
                [(v[0], v[1], v[2], v[3], v[4], v[5], v[6], v[7], now) for v in values],
                page_size=500)
            billing_conn.commit()
    print(f"  ✅ {btd_count} token-usage rows synced")
    _stamp_cache_meta(billing_conn, "token_daily", btd_count)
except Exception as exc:
    print(f"  ⚠️  billing_token_daily sync failed: {exc}")

# Sync billing_product_daily
print(f"▸ Syncing {BPD_TABLE} → billing_product_daily ...")
try:
    with billing_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE billing_product_daily")
        billing_conn.commit()
    bpd_rows = spark.read.table(BPD_TABLE).collect()
    if bpd_rows:
        values = [(r.usage_date, r.workspace_id, r.billing_origin_product,
                   float(r.total_dbus or 0), float(r.total_cost_usd or 0))
                  for r in bpd_rows]
        bpd_count = len(values)
        with billing_conn.cursor() as cur:
            execute_values(cur,
                """INSERT INTO billing_product_daily
                   (usage_date, workspace_id, billing_origin_product, total_dbus, total_cost_usd, last_synced)
                   VALUES %s
                   ON CONFLICT (usage_date, workspace_id, billing_origin_product) DO UPDATE SET
                       total_dbus = EXCLUDED.total_dbus,
                       total_cost_usd = EXCLUDED.total_cost_usd,
                       last_synced = NOW()""",
                [(v[0], v[1], v[2], v[3], v[4], now) for v in values],
                page_size=500)
            billing_conn.commit()
    print(f"  ✅ {bpd_count} product-cost rows synced")
    _stamp_cache_meta(billing_conn, "product_daily", bpd_count)
except Exception as exc:
    print(f"  ⚠️  billing_product_daily sync failed: {exc}")

# Sync billing_user_endpoint_daily
print(f"▸ Syncing {BUED_TABLE} → billing_user_endpoint_daily ...")
try:
    with billing_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE billing_user_endpoint_daily")
        billing_conn.commit()
    bued_rows = spark.read.table(BUED_TABLE).collect()
    if bued_rows:
        values = [(r.usage_date, r.workspace_id, r.endpoint_name,
                   r.user_identity or "unknown",
                   int(r.request_count or 0),
                   int(r.input_tokens or 0), int(r.output_tokens or 0))
                  for r in bued_rows]
        bued_count = len(values)
        with billing_conn.cursor() as cur:
            execute_values(cur,
                """INSERT INTO billing_user_endpoint_daily
                   (usage_date, workspace_id, endpoint_name, user_identity,
                    request_count, input_tokens, output_tokens, last_synced)
                   VALUES %s
                   ON CONFLICT (usage_date, workspace_id, endpoint_name, user_identity) DO UPDATE SET
                       request_count = EXCLUDED.request_count,
                       input_tokens = EXCLUDED.input_tokens,
                       output_tokens = EXCLUDED.output_tokens,
                       last_synced = NOW()""",
                [(v[0], v[1], v[2], v[3], v[4], v[5], v[6], now) for v in values],
                page_size=500)
            billing_conn.commit()
    print(f"  ✅ {bued_count} user-endpoint rows synced")
    _stamp_cache_meta(billing_conn, "user_endpoint_daily", bued_count)
except Exception as exc:
    print(f"  ⚠️  billing_user_endpoint_daily sync failed: {exc}")

# Sync billing_user_cost_daily (actual per-user $ — UAG v2 attribution)
print(f"▸ Syncing {BUCD_TABLE} → billing_user_cost_daily ...")
try:
    with billing_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE billing_user_cost_daily")
        billing_conn.commit()
    bucd_rows = spark.read.table(BUCD_TABLE).collect()
    if bucd_rows:
        values = [(r.usage_date, r.workspace_id, r.endpoint_id or "", r.endpoint_name or "",
                   r.run_by or "unknown",
                   float(r.total_dbus or 0), float(r.total_cost_usd or 0))
                  for r in bucd_rows]
        bucd_count = len(values)
        with billing_conn.cursor() as cur:
            execute_values(cur,
                """INSERT INTO billing_user_cost_daily
                   (usage_date, workspace_id, endpoint_id, endpoint_name, run_by,
                    total_dbus, total_cost_usd, last_synced)
                   VALUES %s
                   ON CONFLICT (usage_date, workspace_id, endpoint_id, run_by) DO UPDATE SET
                       endpoint_name = EXCLUDED.endpoint_name,
                       total_dbus = EXCLUDED.total_dbus,
                       total_cost_usd = EXCLUDED.total_cost_usd,
                       last_synced = NOW()""",
                [(v[0], v[1], v[2], v[3], v[4], v[5], v[6], now) for v in values],
                page_size=500)
            billing_conn.commit()
    print(f"  ✅ {bucd_count} user-cost rows synced")
    _stamp_cache_meta(billing_conn, "user_cost_daily", bucd_count)
except Exception as exc:
    # Rollback so a failed INSERT here (e.g. billing_user_cost_daily is app-owned
    # and may be missing last_synced) does not leave the shared connection in an
    # aborted-transaction state that poisons the next sync block below.
    billing_conn.rollback()
    print(f"  ⚠️  billing_user_cost_daily sync failed: {exc}")

# Sync billing_cost_by_tag (MODEL_SERVING $ attributed by custom_tag — window aggregate)
print(f"▸ Syncing {BTAG_TABLE} → billing_cost_by_tag ...")
try:
    # Defense-in-depth: clear any aborted transaction inherited from a prior
    # block so this block's TRUNCATE isn't rejected with "transaction is aborted".
    billing_conn.rollback()
    with billing_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE billing_cost_by_tag")
        billing_conn.commit()
    btag_rows = spark.read.table(BTAG_TABLE).collect()
    if btag_rows:
        values = [(r.tag_key or "", r.tag_value or "", float(r.total_cost_usd or 0))
                  for r in btag_rows]
        btag_count = len(values)
        with billing_conn.cursor() as cur:
            execute_values(cur,
                """INSERT INTO billing_cost_by_tag
                   (tag_key, tag_value, total_cost_usd, last_synced)
                   VALUES %s
                   ON CONFLICT (tag_key, tag_value) DO UPDATE SET
                       total_cost_usd = EXCLUDED.total_cost_usd,
                       last_synced = NOW()""",
                [(v[0], v[1], v[2], now) for v in values],
                page_size=500)
            billing_conn.commit()
    print(f"  ✅ {btag_count} cost-by-tag rows synced")
    _stamp_cache_meta(billing_conn, "cost_by_tag", btag_count)
except Exception as exc:
    # Clear any aborted transaction so a future sync block appended after this
    # one doesn't inherit a poisoned connection (see the uag_conn rollback guards).
    billing_conn.rollback()
    print(f"  ⚠️  billing_cost_by_tag sync failed: {exc}")

# Sync billing_external_model_spend (external LLM $ via AI Gateway — window aggregate)
print(f"▸ Syncing {BEXT_TABLE} → billing_external_model_spend ...")
try:
    billing_conn.rollback()  # start clean regardless of any prior block's state
    with billing_conn.cursor() as cur:
        cur.execute("TRUNCATE TABLE billing_external_model_spend")
        billing_conn.commit()
    bext_rows = spark.read.table(BEXT_TABLE).collect()
    if bext_rows:
        values = [(r.provider or "", r.model or "", r.endpoint_name or "",
                   int(r.call_count or 0), float(r.total_cost_usd or 0), r.last_seen)
                  for r in bext_rows]
        bext_count = len(values)
        with billing_conn.cursor() as cur:
            execute_values(cur,
                """INSERT INTO billing_external_model_spend
                   (provider, model, endpoint_name, call_count, total_cost_usd, last_seen, last_synced)
                   VALUES %s
                   ON CONFLICT (provider, model, endpoint_name) DO UPDATE SET
                       call_count = EXCLUDED.call_count,
                       total_cost_usd = EXCLUDED.total_cost_usd,
                       last_seen = EXCLUDED.last_seen,
                       last_synced = NOW()""",
                [(v[0], v[1], v[2], v[3], v[4], v[5], now) for v in values],
                page_size=500)
            billing_conn.commit()
    print(f"  ✅ {bext_count} external-model-spend rows synced")
    _stamp_cache_meta(billing_conn, "external_model_spend", bext_count)
except Exception as exc:
    billing_conn.rollback()
    print(f"  ⚠️  billing_external_model_spend sync failed: {exc}")

billing_conn.close()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 7: ensure app-managed tables
# MAGIC
# MAGIC `tool_registry` and `request_logs` are populated lazily by the deployed
# MAGIC app (Tools page discovery + per-request audit logging). The app's
# MAGIC startup hook tries to `CREATE TABLE IF NOT EXISTS` these on first boot,
# MAGIC but the app's service principal does not always have Lakebase DDL
# MAGIC privileges — when it doesn't, the failure is swallowed in a daemon
# MAGIC thread and the Tools page renders empty with a 500 from `/tools/overview`.
# MAGIC
# MAGIC The workflow run-as identity is `databricks_superuser`, so DDL here is
# MAGIC always safe. Creating the tables upfront makes the app independent of
# MAGIC its own DDL grants.

# COMMAND ----------

print("\n" + "═" * 70)
print("Phase 7: app-managed tables (tool_registry, request_logs)")
print("═" * 70)

_app_ddls = [
    """CREATE TABLE IF NOT EXISTS tool_registry (
        tool_id         TEXT PRIMARY KEY,
        name            TEXT NOT NULL,
        type            TEXT NOT NULL,
        sub_type        TEXT,
        endpoint_name   TEXT,
        catalog_name    TEXT,
        schema_name     TEXT,
        description     TEXT,
        status          TEXT,
        config          JSONB,
        last_synced     TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    )""",
    "CREATE INDEX IF NOT EXISTS idx_tr_type ON tool_registry (type)",
    """CREATE TABLE IF NOT EXISTS request_logs (
        request_id      TEXT PRIMARY KEY,
        agent_id        TEXT,
        model_id        TEXT,
        user_id         TEXT,
        timestamp       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        query_text      TEXT,
        response_text   TEXT,
        latency_ms      NUMERIC(12,2),
        status_code     INTEGER,
        input_tokens    INTEGER DEFAULT 0,
        output_tokens   INTEGER DEFAULT 0,
        cost_usd        NUMERIC(12,6) DEFAULT 0,
        error_message   TEXT,
        endpoint_type   TEXT
    )""",
    "CREATE INDEX IF NOT EXISTS idx_rl_agent ON request_logs (agent_id)",
    "CREATE INDEX IF NOT EXISTS idx_rl_ts    ON request_logs (timestamp DESC)",
    "CREATE INDEX IF NOT EXISTS idx_rl_user  ON request_logs (user_id)",
]

app_conn = get_lakebase_connection()
for ddl in _app_ddls:
    try:
        with app_conn.cursor() as cur:
            cur.execute(ddl)
        app_conn.commit()
        print(f"  ✅ {ddl.split()[2:5]}")
    except Exception as e:
        app_conn.rollback()
        print(f"  ⚠️  DDL warning: {e}")
app_conn.close()
print("  Phase 7 complete")

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
    "observability_trace_details": trace_detail_count,
    "traces_from_delta_default": delta_trace_count,
    "traces_from_delta_uc_otel": delta_uc_trace_count,
    "trace_details_from_delta_default": delta_detail_count,
    "trace_details_from_delta_uc_otel": delta_uc_detail_count,
    "gateway_inference_logs": gw_log_count,
    "gateway_inference_logs_from_delta": gw_log_delta_count,
    "vs_endpoints": vs_ep_count,
    "vs_indexes": vs_idx_count,
    "lakebase_instances": lb_inst_count,
    "kb_billing_vs": vs_billing_count,
    "kb_billing_lakebase": lb_billing_count,
    "ua_daily_rows": ua_daily_count,
    "ua_heatmap_rows": ua_heatmap_count,
    "gw_daily_rows": gw_daily_count,
    "gw_hourly_rows": gw_hourly_count,
    "uag_usage_rows": uag_count,
    "billing_serving_daily_rows": bsd_count,
    "billing_token_daily_rows": btd_count,
    "billing_product_daily_rows": bpd_count,
    "billing_user_endpoint_daily_rows": bued_count,
    "billing_user_cost_daily_rows": bucd_count,
    "billing_cost_by_tag_rows": btag_count,
    "billing_external_model_spend_rows": bext_count,
    "synced_at": datetime.now(timezone.utc).isoformat(),
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
