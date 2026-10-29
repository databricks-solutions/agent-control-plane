# Databricks notebook source
# MAGIC %md
# MAGIC # Unity Gateway (v2) Usage Discovery Job
# MAGIC
# MAGIC Queries `system.ai_gateway.usage` (v2 Unity Gateway traffic only) for a
# MAGIC per-endpoint usage summary and writes a Delta table for `02_sync_to_lakebase`.
# MAGIC
# MAGIC **Why a separate source:** `system.ai_gateway.usage` is ~20-min fresh (vs
# MAGIC ~2 hr for `system.billing.usage`) and carries data the others lack — cached
# MAGIC tokens (`token_details`), time-to-first-byte, per-request tags. It covers
# MAGIC ONLY requests routed through Unity Gateway v2 endpoints (a subset of all
# MAGIC serving), so it is additive — not a replacement for billing (dollar cost)
# MAGIC or serving.endpoint_usage (broad coverage + rate-limit hits).
# MAGIC
# MAGIC **Tables written:** `uag_usage_summary`, `uag_usage_breakdown`, `uag_mcp_tool_daily`

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, TimestampType,
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog name")
dbutils.widgets.text("schema", "", "Schema name")
dbutils.widgets.text("warehouse_id", "", "SQL warehouse ID")
dbutils.widgets.text("uag_retention_days", "7", "Days of UAG usage to summarize")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
RETENTION_DAYS = int(dbutils.widgets.get("uag_retention_days") or "7")

if not CATALOG or not SCHEMA:
    raise ValueError(f"catalog and schema required (got {CATALOG!r}, {SCHEMA!r})")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

UAG_TABLE = f"{CATALOG}.{SCHEMA}.uag_usage_summary"
UAG_BREAKDOWN_TABLE = f"{CATALOG}.{SCHEMA}.uag_usage_breakdown"
UAG_MCP_TOOL_TABLE = f"{CATALOG}.{SCHEMA}.uag_mcp_tool_daily"
UAG_GUARDRAIL_TABLE = f"{CATALOG}.{SCHEMA}.uag_guardrail_daily"
UAG_TIMESERIES_TABLE = f"{CATALOG}.{SCHEMA}.uag_usage_timeseries_daily"
UAG_CODING_AGENT_TABLE = f"{CATALOG}.{SCHEMA}.uag_coding_agent_usage"
UAG_THROTTLING_TABLE = f"{CATALOG}.{SCHEMA}.uag_throttling_daily"
UAG_FALLBACK_TABLE = f"{CATALOG}.{SCHEMA}.uag_fallback_routing_daily"
UAG_SERVICE_USAGE_TABLE = f"{CATALOG}.{SCHEMA}.uag_service_usage"
UAG_ENDPOINT_SERVICE_MAP_TABLE = f"{CATALOG}.{SCHEMA}.uag_endpoint_service_map"
print(f"Target tables: {UAG_TABLE}, {UAG_BREAKDOWN_TABLE}, {UAG_MCP_TOOL_TABLE}, {UAG_GUARDRAIL_TABLE}, {UAG_TIMESERIES_TABLE}, {UAG_THROTTLING_TABLE}, {UAG_FALLBACK_TABLE} | retention {RETENTION_DAYS}d")

# COMMAND ----------

UAG_SCHEMA = StructType([
    StructField("endpoint_name", StringType(), False),
    StructField("request_count", LongType(), True),
    StructField("input_tokens", LongType(), True),
    StructField("output_tokens", LongType(), True),
    StructField("cache_read_tokens", LongType(), True),
    StructField("cache_creation_tokens", LongType(), True),
    StructField("p50_latency_ms", LongType(), True),
    StructField("p90_latency_ms", LongType(), True),
    StructField("p95_latency_ms", LongType(), True),
    StructField("p99_latency_ms", LongType(), True),
    StructField("p95_ttfb_ms", LongType(), True),
    StructField("error_count", LongType(), True),
    StructField("unique_users", LongType(), True),
    StructField("max_event_time", StringType(), True),
    StructField("discovered_at", TimestampType(), False),
])

# Flexible breakdown: one row per (dimension, key). dimension ∈
# {requester_type, destination_model, api_type, service_type, route_action} — additive v2 cuts.
UAG_BREAKDOWN_SCHEMA = StructType([
    StructField("dimension", StringType(), False),
    StructField("key", StringType(), False),
    StructField("request_count", LongType(), True),
    StructField("input_tokens", LongType(), True),
    StructField("output_tokens", LongType(), True),
    StructField("cached_tokens", LongType(), True),
    StructField("discovered_at", TimestampType(), False),
])

# Per-tool MCP activity (service_type = MCP_SERVICE). One row per
# (service_name, tool_name, server_type) over the retention window.
UAG_MCP_TOOL_SCHEMA = StructType([
    StructField("service_name", StringType(), False),
    StructField("tool_name", StringType(), True),
    StructField("server_type", StringType(), True),
    StructField("request_count", LongType(), True),
    StructField("error_count", LongType(), True),
    StructField("unique_users", LongType(), True),
    StructField("max_event_time", StringType(), True),
    StructField("discovered_at", TimestampType(), False),
])

# Coding-agent activity: classify traffic by user_agent (claude-cli → Claude Code,
# codex, cursor, gemini-cli). One row per coding agent. NOTE: activity only
# (requests/tokens/users/active-days) — sessions/commits/lines-of-code are NOT in
# system.ai_gateway.usage and would need coding-agent-specific telemetry.
UAG_CODING_AGENT_SCHEMA = StructType([
    StructField("coding_agent",  StringType(), False),
    StructField("request_count", LongType(), True),
    StructField("unique_users",  LongType(), True),
    StructField("active_days",   LongType(), True),
    StructField("total_tokens",  LongType(), True),
    StructField("max_event_time", StringType(), True),
    StructField("discovered_at", TimestampType(), False),
])

# Daily usage time-series (one row per day) for trend charts on the v2 tab.
UAG_TIMESERIES_SCHEMA = StructType([
    StructField("usage_date", StringType(), False),
    StructField("request_count", LongType(), True),
    StructField("input_tokens", LongType(), True),
    StructField("output_tokens", LongType(), True),
    StructField("discovered_at", TimestampType(), False),
])

# Guardrail COVERAGE/ACTIVITY, one row per guarded endpoint. NOTE: this is
# coverage (which endpoints have guardrails running, how often) — NOT block/mask
# outcomes. The verdict is not in system.ai_gateway.usage (GUARDRAIL rows are the
# judge-model invocations, which return 200 = "check ran"); outcomes require the
# enrollment-gated UAG feature-results surface.
UAG_GUARDRAIL_SCHEMA = StructType([
    StructField("endpoint_name", StringType(), False),
    StructField("checked_requests", LongType(), True),
    StructField("unique_users", LongType(), True),
    StructField("judge_models", StringType(), True),
    StructField("max_event_time", StringType(), True),
    StructField("discovered_at", TimestampType(), False),
])

# Throttling / reliability, one row per endpoint over the window: rate-limited
# (HTTP 429) and server-error (5xx) request counts vs total, so the UI can show
# a throttle rate per endpoint. Sourced from status_code on system.ai_gateway.usage.
UAG_THROTTLING_SCHEMA = StructType([
    StructField("endpoint_name", StringType(), False),
    StructField("total_requests", LongType(), True),
    StructField("throttled_count", LongType(), True),   # HTTP 429
    StructField("server_error_count", LongType(), True),  # HTTP 5xx
    StructField("max_event_time", StringType(), True),
    StructField("discovered_at", TimestampType(), False),
])

# Fallback routing, one row per endpoint: how often smart-routing had to fall
# back to a backup model (routing_information.attempts has >1 entry, i.e. the
# primary attempt failed and a FALLBACK attempt followed), how many of those
# ultimately recovered (final attempt < 400), and which backup destinations were
# used. Reliability signal for Unity Gateway smart-routing.
UAG_FALLBACK_SCHEMA = StructType([
    StructField("endpoint_name", StringType(), False),
    StructField("total_requests", LongType(), True),
    StructField("fallback_requests", LongType(), True),
    StructField("fallback_recovered", LongType(), True),
    StructField("fallback_destinations", StringType(), True),
    StructField("max_event_time", StringType(), True),
    StructField("discovered_at", TimestampType(), False),
])

# Per-service usage, keyed by the UC service full name (= service inventory
# full_name). Unlike the endpoint-keyed tables this also covers MCP services,
# which have no endpoint_name.
UAG_SERVICE_USAGE_SCHEMA = StructType([
    StructField("service_name", StringType(), False),
    StructField("service_type", StringType(), True),
    StructField("request_count", LongType(), True),
    StructField("error_count", LongType(), True),
    StructField("throttled_count", LongType(), True),
    StructField("unique_users", LongType(), True),
    StructField("input_tokens", LongType(), True),
    StructField("output_tokens", LongType(), True),
    StructField("p50_latency_ms", LongType(), True),
    StructField("p95_latency_ms", LongType(), True),
    StructField("max_event_time", StringType(), True),
    StructField("discovered_at", TimestampType(), False),
])

# endpoint_id → service. Lets the app attribute gateway-attributed billing
# (billing_user_cost_daily keys on usage_metadata.ai_gateway.endpoint_id) to a UC
# service, i.e. cost per model service and per user × service.
UAG_ENDPOINT_SERVICE_MAP_SCHEMA = StructType([
    StructField("endpoint_id", StringType(), False),
    StructField("service_name", StringType(), True),
    StructField("service_type", StringType(), True),
    StructField("discovered_at", TimestampType(), False),
])

# COMMAND ----------

_RATE_LIMIT_BACKOFF_S = (30, 60, 120)
# Whether the most recent _execute_sql call FAILED (vs succeeded with 0 rows). A
# failed query returns [] like an empty one, so each block consults this before
# overwriting its table — see _empty_or_keep.
_LAST_QUERY_FAILED = False
_KEPT_PREVIOUS: List[str] = []


def _is_rate_limited(msg: str) -> bool:
    m = (msg or "").upper()
    return "429" in m or "REQUEST_LIMIT_EXCEEDED" in m or "TOO MANY REQUESTS" in m


def _execute_sql(sql: str) -> List[Dict[str, Any]]:
    global _LAST_QUERY_FAILED
    _LAST_QUERY_FAILED = True  # cleared only on success
    if not WAREHOUSE_ID:
        print("  No warehouse ID")
        return []
    w = WorkspaceClient()
    body = {"warehouse_id": WAREHOUSE_ID, "statement": sql,
            "wait_timeout": "50s", "disposition": "INLINE", "format": "JSON_ARRAY"}
    # system.ai_gateway.usage is served over Delta Sharing; under org-wide UC load a
    # statement can fail with 429 / UC_REQUEST_LIMIT_EXCEEDED. Transient → back off
    # and resubmit before giving up.
    for attempt in range(len(_RATE_LIMIT_BACKOFF_S) + 1):
        try:
            resp = w.api_client.do("POST", "/api/2.0/sql/statements", body=body)
        except Exception as exc:
            if attempt < len(_RATE_LIMIT_BACKOFF_S) and _is_rate_limited(str(exc)):
                print(f"  rate-limited on submit; retrying in {_RATE_LIMIT_BACKOFF_S[attempt]}s")
                time.sleep(_RATE_LIMIT_BACKOFF_S[attempt])
                continue
            print(f"  SQL failed: {exc}")
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
        msg = str(resp.get("status", {}).get("error", {}).get("message", ""))
        if status == "FAILED" and attempt < len(_RATE_LIMIT_BACKOFF_S) and _is_rate_limited(msg):
            print(f"  rate-limited (429); retrying in {_RATE_LIMIT_BACKOFF_S[attempt]}s")
            time.sleep(_RATE_LIMIT_BACKOFF_S[attempt])
            continue
        break
    if status != "SUCCEEDED":
        print(f"  SQL {status}: {msg[:300]}")
        return []
    _LAST_QUERY_FAILED = False
    cols = [c["name"] for c in resp.get("manifest", {}).get("schema", {}).get("columns", [])]
    return [dict(zip(cols, row)) for row in resp.get("result", {}).get("data_array", [])]


def _empty_or_keep(schema, table: str) -> None:
    """Called when a block got no rows. If the query genuinely returned 0 rows,
    overwrite with an empty table (truthful). If it FAILED (or raised), keep the
    previous snapshot instead — one transient error must not blank a dashboard
    card and fail the smoke check. On a first run (no table yet) write the empty
    table anyway so downstream sync/smoke find it."""
    if _LAST_QUERY_FAILED and spark.catalog.tableExists(table):
        _KEPT_PREVIOUS.append(table.split(".")[-1])
        print(f"  ↺ query failed — keeping previous snapshot of {table}")
        return
    spark.createDataFrame([], schema).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)

# COMMAND ----------

print(f"▸ Querying system.ai_gateway.usage (v2 UAG, {RETENTION_DAYS}d) …")
try:
    rows_raw = _execute_sql(f"""
        SELECT
            endpoint_name,
            COUNT(*)                                                   AS request_count,
            COALESCE(SUM(input_tokens), 0)                             AS input_tokens,
            COALESCE(SUM(output_tokens), 0)                            AS output_tokens,
            COALESCE(SUM(token_details.cache_read_input_tokens), 0)    AS cache_read_tokens,
            COALESCE(SUM(token_details.cache_creation_input_tokens), 0) AS cache_creation_tokens,
            CAST(PERCENTILE_APPROX(latency_ms, 0.5) AS LONG)           AS p50_latency_ms,
            CAST(PERCENTILE_APPROX(latency_ms, 0.9) AS LONG)           AS p90_latency_ms,
            CAST(PERCENTILE_APPROX(latency_ms, 0.95) AS LONG)          AS p95_latency_ms,
            CAST(PERCENTILE_APPROX(latency_ms, 0.99) AS LONG)          AS p99_latency_ms,
            CAST(PERCENTILE_APPROX(time_to_first_byte_ms, 0.95) AS LONG) AS p95_ttfb_ms,
            SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END)        AS error_count,
            COUNT(DISTINCT requester)                                  AS unique_users,
            CAST(MAX(event_time) AS STRING)                            AS max_event_time
        FROM system.ai_gateway.usage
        WHERE event_time >= current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS
          AND endpoint_name IS NOT NULL
        GROUP BY endpoint_name
        ORDER BY request_count DESC
    """)
    print(f"  ✅ {len(rows_raw)} UAG endpoint rows")
except Exception as exc:
    # Table may be inaccessible (account-admin scoping varies by workspace) — degrade.
    rows_raw = []
    print(f"  ⚠️  system.ai_gateway.usage unavailable: {exc}")

# COMMAND ----------

now = datetime.now(timezone.utc)


# NOTE: don't name this `_i` — IPython/Databricks reserves `_i`, `_ii`, `_iii`
# for input history and rebinds them to strings between cells.
def to_int(x):
    return int(x) if x not in (None, "") else 0


if rows_raw:
    rows = [(r.get("endpoint_name", ""), to_int(r.get("request_count")), to_int(r.get("input_tokens")),
             to_int(r.get("output_tokens")), to_int(r.get("cache_read_tokens")), to_int(r.get("cache_creation_tokens")),
             to_int(r.get("p50_latency_ms")), to_int(r.get("p90_latency_ms")), to_int(r.get("p95_latency_ms")),
             to_int(r.get("p99_latency_ms")), to_int(r.get("p95_ttfb_ms")),
             to_int(r.get("error_count")), to_int(r.get("unique_users")), r.get("max_event_time"), now)
            for r in rows_raw]
    spark.createDataFrame(rows, UAG_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(UAG_TABLE)
else:
    _empty_or_keep(UAG_SCHEMA, UAG_TABLE)
print(f"✅ Wrote {len(rows_raw)} rows to {UAG_TABLE}")

# COMMAND ----------

# Breakdowns: agent-vs-human (requester_type), model (destination_model), api_type,
# service_type (model vs MCP vs provider), and route_action (routing outcomes).
print("▸ Querying ai_gateway.usage breakdowns (requester_type / destination_model / api_type / source / service_type / route_action) …")
try:
    bd_rows = _execute_sql(f"""
        WITH base AS (
            SELECT requester_type, destination_model, api_type, service_type,
                   invocation_metadata.source AS source, workspace_id, auth_mode,
                   mcp_metadata.json_rpc_method AS mcp_method,
                   session_metadata.coding_agent AS coding_agent,
                   session_metadata.reasoning_effort AS reasoning_effort,
                   CAST(status_code AS STRING) AS status_code, input_tokens, output_tokens,
                   COALESCE(token_details.cache_read_input_tokens, 0)
                 + COALESCE(token_details.cache_creation_input_tokens, 0) AS cached
            FROM system.ai_gateway.usage
            WHERE event_time >= current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS
        )
        SELECT 'requester_type' AS dimension, COALESCE(requester_type, 'unknown') AS key,
               COUNT(*) AS request_count, COALESCE(SUM(input_tokens), 0) AS input_tokens,
               COALESCE(SUM(output_tokens), 0) AS output_tokens, COALESCE(SUM(cached), 0) AS cached_tokens
        FROM base GROUP BY requester_type
        UNION ALL
        SELECT 'destination_model', COALESCE(destination_model, 'unknown'),
               COUNT(*), COALESCE(SUM(input_tokens), 0), COALESCE(SUM(output_tokens), 0), COALESCE(SUM(cached), 0)
        FROM base GROUP BY destination_model
        UNION ALL
        -- api_type is only set for external-client LLM traffic; ai_query()/AI Functions
        -- (source=AI_QUERY) carry no api_type, so label them rather than dump into 'unknown'.
        SELECT 'api_type',
               CASE WHEN COALESCE(api_type,'') != '' THEN api_type
                    WHEN source = 'AI_QUERY' THEN 'ai_query (SQL)'
                    ELSE 'unknown' END,
               COUNT(*), COALESCE(SUM(input_tokens), 0), COALESCE(SUM(output_tokens), 0), COALESCE(SUM(cached), 0)
        FROM base GROUP BY 2
        UNION ALL
        -- source: where the traffic originates (AI_QUERY / EXTERNAL_CLIENT / GUARDRAIL) —
        -- explains the api_type mix (most volume is ai_query, which has no api_type).
        SELECT 'source', COALESCE(source, 'unknown'),
               COUNT(*), COALESCE(SUM(input_tokens), 0), COALESCE(SUM(output_tokens), 0), COALESCE(SUM(cached), 0)
        FROM base GROUP BY source
        UNION ALL
        SELECT 'status_code', COALESCE(status_code, 'unknown'),
               COUNT(*), COALESCE(SUM(input_tokens), 0), COALESCE(SUM(output_tokens), 0), COALESCE(SUM(cached), 0)
        FROM base GROUP BY status_code
        UNION ALL
        SELECT 'workspace_id', COALESCE(NULLIF(CAST(workspace_id AS STRING), ''), 'unknown'),
               COUNT(*), COALESCE(SUM(input_tokens), 0), COALESCE(SUM(output_tokens), 0), COALESCE(SUM(cached), 0)
        FROM base GROUP BY workspace_id
        UNION ALL
        -- auth_mode: PAT vs OAUTH — a credential-hygiene signal (PATs are long-lived)
        SELECT 'auth_mode', COALESCE(NULLIF(auth_mode, ''), 'unknown'),
               COUNT(*), COALESCE(SUM(input_tokens), 0), COALESCE(SUM(output_tokens), 0), COALESCE(SUM(cached), 0)
        FROM base GROUP BY 2
        UNION ALL
        -- mcp_method: tools/call vs tools/list vs initialize (MCP rows only)
        SELECT 'mcp_method', COALESCE(NULLIF(mcp_method, ''), 'unknown'),
               COUNT(*), 0, 0, 0
        FROM base WHERE service_type = 'MCP_SERVICE' GROUP BY 2
        UNION ALL
        -- coding_agent / reasoning_effort: coding-agent session traffic only
        SELECT 'coding_agent', coding_agent,
               COUNT(*), COALESCE(SUM(input_tokens), 0), COALESCE(SUM(output_tokens), 0), COALESCE(SUM(cached), 0)
        FROM base WHERE coding_agent IS NOT NULL AND coding_agent != '' GROUP BY coding_agent
        UNION ALL
        SELECT 'reasoning_effort', reasoning_effort,
               COUNT(*), COALESCE(SUM(input_tokens), 0), COALESCE(SUM(output_tokens), 0), COALESCE(SUM(cached), 0)
        FROM base WHERE reasoning_effort IS NOT NULL AND reasoning_effort != '' GROUP BY reasoning_effort
        UNION ALL
        -- service_type: exclude legacy untyped rows so the model/MCP/provider split is meaningful
        SELECT 'service_type', service_type,
               COUNT(*), COALESCE(SUM(input_tokens), 0), COALESCE(SUM(output_tokens), 0), COALESCE(SUM(cached), 0)
        FROM base WHERE service_type IS NOT NULL AND service_type != '' GROUP BY service_type
        UNION ALL
        -- route_action: per-ATTEMPT outcome, not per-request — a fallback adds a
        -- second attempt row, so counts are attempt-grain (the UI labels this
        -- "attempts", not requests, and it won't tie out to the request KPI).
        -- Plain explode drops rows with no routing attempts, which scopes this to
        -- v2-routed traffic — parallel to the service_type cut excluding untyped
        -- legacy rows (explode_outer would flood it with a ~96% 'unknown' bucket).
        -- Tokens N/A (explode fans out rows).
        SELECT 'route_action', COALESCE(a.action, 'unknown'),
               COUNT(*), 0, 0, 0
        FROM system.ai_gateway.usage
             LATERAL VIEW explode(routing_information.attempts) t AS a
        WHERE event_time >= current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS
        GROUP BY a.action
    """)
    print(f"  ✅ {len(bd_rows)} breakdown rows")
except Exception as exc:
    bd_rows = []
    print(f"  ⚠️  breakdown query unavailable: {exc}")

if bd_rows:
    rows = [(r.get("dimension", ""), r.get("key", "") or "unknown", to_int(r.get("request_count")),
             to_int(r.get("input_tokens")), to_int(r.get("output_tokens")), to_int(r.get("cached_tokens")), now)
            for r in bd_rows]
    spark.createDataFrame(rows, UAG_BREAKDOWN_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(UAG_BREAKDOWN_TABLE)
else:
    _empty_or_keep(UAG_BREAKDOWN_SCHEMA, UAG_BREAKDOWN_TABLE)
print(f"✅ Wrote {len(bd_rows)} rows to {UAG_BREAKDOWN_TABLE}")

# COMMAND ----------

# Daily usage time-series (requests + tokens per day) for trend charts.
print("▸ Querying ai_gateway.usage daily time-series …")
try:
    ts_rows_raw = _execute_sql(f"""
        SELECT CAST(event_time AS DATE)                                 AS usage_date,
               COUNT(*)                                                 AS request_count,
               COALESCE(SUM(input_tokens), 0)                           AS input_tokens,
               COALESCE(SUM(output_tokens), 0)                          AS output_tokens
        FROM system.ai_gateway.usage
        WHERE event_time >= current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS
          AND endpoint_name IS NOT NULL   -- match the summary/KPI population so the trend reconciles
        GROUP BY CAST(event_time AS DATE)
        ORDER BY usage_date
    """)
    print(f"  ✅ {len(ts_rows_raw)} time-series rows")
except Exception as exc:
    ts_rows_raw = []
    print(f"  ⚠️  time-series query unavailable: {exc}")

if ts_rows_raw:
    rows = [(str(r.get("usage_date")), to_int(r.get("request_count")), to_int(r.get("input_tokens")),
             to_int(r.get("output_tokens")), now)
            for r in ts_rows_raw]
    spark.createDataFrame(rows, UAG_TIMESERIES_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(UAG_TIMESERIES_TABLE)
else:
    _empty_or_keep(UAG_TIMESERIES_SCHEMA, UAG_TIMESERIES_TABLE)
print(f"✅ Wrote {len(ts_rows_raw)} rows to {UAG_TIMESERIES_TABLE}")

# COMMAND ----------

# Coding-agent activity — classify by user_agent (Claude Code / Codex / Cursor /
# Gemini CLI). Activity only; no sessions/commits/LOC (not in this table).
print("▸ Querying ai_gateway.usage coding-agent activity …")
try:
    ca_rows_raw = _execute_sql(f"""
        SELECT CASE
                 WHEN lower(user_agent) LIKE 'claude-cli%' OR lower(user_agent) LIKE '%claude-code%' THEN 'Claude Code'
                 WHEN lower(user_agent) LIKE '%codex/%'                                               THEN 'Codex'
                 WHEN lower(user_agent) LIKE 'cursor%'  OR lower(api_type) LIKE 'cursor%'             THEN 'Cursor'
                 WHEN lower(user_agent) LIKE '%gemini-cli%'                                           THEN 'Gemini CLI'
                 ELSE NULL END                                        AS coding_agent,
               COUNT(*)                                               AS request_count,
               COUNT(DISTINCT requester)                              AS unique_users,
               COUNT(DISTINCT CAST(event_time AS DATE))               AS active_days,
               COALESCE(SUM(total_tokens), 0)                         AS total_tokens,
               CAST(MAX(event_time) AS STRING)                        AS max_event_time
        FROM system.ai_gateway.usage
        WHERE event_time >= current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS
        GROUP BY 1
        HAVING coding_agent IS NOT NULL
        ORDER BY request_count DESC
    """)
    print(f"  ✅ {len(ca_rows_raw)} coding-agent rows")
except Exception as exc:
    ca_rows_raw = []
    print(f"  ⚠️  coding-agent query unavailable: {exc}")

if ca_rows_raw:
    rows = [(r.get("coding_agent", ""), to_int(r.get("request_count")), to_int(r.get("unique_users")),
             to_int(r.get("active_days")), to_int(r.get("total_tokens")), r.get("max_event_time"), now)
            for r in ca_rows_raw]
    spark.createDataFrame(rows, UAG_CODING_AGENT_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(UAG_CODING_AGENT_TABLE)
else:
    _empty_or_keep(UAG_CODING_AGENT_SCHEMA, UAG_CODING_AGENT_TABLE)
print(f"✅ Wrote {len(ca_rows_raw)} rows to {UAG_CODING_AGENT_TABLE}")

# COMMAND ----------

# Per-tool MCP activity — service_type = MCP_SERVICE rows carry service_name (UC FQN)
# and mcp_metadata.{tool_name, server_type}. tool_name can be null (server-level call).
print("▸ Querying ai_gateway.usage MCP tool activity (service_type = MCP_SERVICE) …")
try:
    mcp_rows_raw = _execute_sql(f"""
        SELECT
            service_name,
            mcp_metadata.tool_name                              AS tool_name,
            mcp_metadata.server_type                            AS server_type,
            COUNT(*)                                            AS request_count,
            SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END) AS error_count,
            COUNT(DISTINCT requester)                           AS unique_users,
            CAST(MAX(event_time) AS STRING)                     AS max_event_time
        FROM system.ai_gateway.usage
        WHERE event_time >= current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS
          AND service_type = 'MCP_SERVICE'
          AND service_name IS NOT NULL
        GROUP BY service_name, mcp_metadata.tool_name, mcp_metadata.server_type
        ORDER BY request_count DESC
    """)
    print(f"  ✅ {len(mcp_rows_raw)} MCP tool rows")
except Exception as exc:
    mcp_rows_raw = []
    print(f"  ⚠️  MCP tool query unavailable: {exc}")

if mcp_rows_raw:
    rows = [(r.get("service_name", ""), r.get("tool_name"), r.get("server_type"),
             to_int(r.get("request_count")), to_int(r.get("error_count")), to_int(r.get("unique_users")),
             r.get("max_event_time"), now)
            for r in mcp_rows_raw]
    spark.createDataFrame(rows, UAG_MCP_TOOL_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(UAG_MCP_TOOL_TABLE)
else:
    _empty_or_keep(UAG_MCP_TOOL_SCHEMA, UAG_MCP_TOOL_TABLE)
print(f"✅ Wrote {len(mcp_rows_raw)} rows to {UAG_MCP_TOOL_TABLE}")

# COMMAND ----------

# Guardrail COVERAGE per guarded endpoint. GUARDRAIL-source rows are the judge-model
# invocations; the guarded endpoint is the PRIMARY request sharing the request_id, so
# we join back to attribute checks to the endpoint being protected (its judge model(s)
# come along). This is coverage/activity only — not block/mask outcomes.
print("▸ Querying ai_gateway.usage guardrail coverage (invocation_metadata.source = GUARDRAIL) …")
try:
    gr_rows_raw = _execute_sql(f"""
        -- (verified live on fevm 2026-07-06: GUARDRAIL judge invocations share the
        -- guarded request's request_id, so the join attributes checks to the
        -- protected primary endpoint.) collect_set keeps ALL judge models when a
        -- request runs multiple guardrails; COUNT(DISTINCT request_id) counts
        -- guarded requests (a request_id can span multiple primary invocation rows).
        WITH g AS (
            SELECT request_id, collect_set(destination_model) AS judges
            FROM system.ai_gateway.usage
            WHERE invocation_metadata.source = 'GUARDRAIL'
              AND event_time >= current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS
            GROUP BY request_id
        )
        SELECT p.endpoint_name                                   AS endpoint_name,
               COUNT(DISTINCT p.request_id)                      AS checked_requests,
               COUNT(DISTINCT p.requester)                       AS unique_users,
               concat_ws(', ', array_sort(array_distinct(flatten(collect_list(g.judges))))) AS judge_models,
               CAST(MAX(p.event_time) AS STRING)                 AS max_event_time
        FROM system.ai_gateway.usage p
             JOIN g ON p.request_id = g.request_id
        WHERE p.invocation_metadata.source != 'GUARDRAIL'
          AND p.event_time >= current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS
          AND p.endpoint_name IS NOT NULL
        GROUP BY p.endpoint_name
        ORDER BY checked_requests DESC
    """)
    print(f"  ✅ {len(gr_rows_raw)} guarded-endpoint rows")
except Exception as exc:
    gr_rows_raw = []
    print(f"  ⚠️  guardrail coverage query unavailable: {exc}")

if gr_rows_raw:
    rows = [(r.get("endpoint_name", ""), to_int(r.get("checked_requests")), to_int(r.get("unique_users")),
             r.get("judge_models"), r.get("max_event_time"), now)
            for r in gr_rows_raw]
    spark.createDataFrame(rows, UAG_GUARDRAIL_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(UAG_GUARDRAIL_TABLE)
else:
    _empty_or_keep(UAG_GUARDRAIL_SCHEMA, UAG_GUARDRAIL_TABLE)
print(f"✅ Wrote {len(gr_rows_raw)} rows to {UAG_GUARDRAIL_TABLE}")

# COMMAND ----------

# Throttling / reliability per endpoint: 429 (rate-limited) and 5xx counts vs total.
print("▸ Querying ai_gateway.usage throttling / errors by endpoint …")
try:
    th_rows_raw = _execute_sql(f"""
        SELECT endpoint_name,
               COUNT(*)                                                  AS total_requests,
               SUM(CASE WHEN status_code = 429 THEN 1 ELSE 0 END)        AS throttled_count,
               SUM(CASE WHEN status_code >= 500 AND status_code < 600
                        THEN 1 ELSE 0 END)                               AS server_error_count,
               CAST(MAX(event_time) AS STRING)                          AS max_event_time
        FROM system.ai_gateway.usage
        WHERE event_time >= current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS
          AND endpoint_name IS NOT NULL
        GROUP BY endpoint_name
        -- keep only endpoints that actually saw throttling or server errors, so the
        -- view is a reliability signal rather than a full endpoint dump
        HAVING SUM(CASE WHEN status_code = 429 OR (status_code >= 500 AND status_code < 600)
                        THEN 1 ELSE 0 END) > 0
        ORDER BY throttled_count DESC
    """)
    print(f"  ✅ {len(th_rows_raw)} throttled/erroring endpoint rows")
except Exception as exc:
    th_rows_raw = []
    print(f"  ⚠️  throttling query unavailable: {exc}")

if th_rows_raw:
    rows = [(r.get("endpoint_name", ""), to_int(r.get("total_requests")), to_int(r.get("throttled_count")),
             to_int(r.get("server_error_count")), r.get("max_event_time"), now)
            for r in th_rows_raw]
    spark.createDataFrame(rows, UAG_THROTTLING_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(UAG_THROTTLING_TABLE)
else:
    _empty_or_keep(UAG_THROTTLING_SCHEMA, UAG_THROTTLING_TABLE)
print(f"✅ Wrote {len(th_rows_raw)} rows to {UAG_THROTTLING_TABLE}")

# COMMAND ----------

# Fallback routing per endpoint: how often smart-routing fell back to a backup
# model (routing_information.attempts has >1 entry), how many recovered (final
# attempt < 400), and which backup destinations were used.
print("▸ Querying ai_gateway.usage fallback routing by endpoint …")
try:
    fb_rows_raw = _execute_sql(f"""
        SELECT endpoint_name,
               COUNT(*)                                                       AS total_requests,
               SUM(CASE WHEN size(routing_information.attempts) > 1
                        THEN 1 ELSE 0 END)                                     AS fallback_requests,
               SUM(CASE WHEN size(routing_information.attempts) > 1
                         AND element_at(routing_information.attempts, -1).status_code < 400
                        THEN 1 ELSE 0 END)                                     AS fallback_recovered,
               concat_ws(', ', array_sort(array_distinct(flatten(collect_list(
                   CASE WHEN size(routing_information.attempts) > 1
                        THEN array(element_at(routing_information.attempts, -1).destination)
                        END))))) AS fallback_destinations,
               CAST(MAX(event_time) AS STRING)                                AS max_event_time
        FROM system.ai_gateway.usage
        WHERE event_time >= current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS
          AND routing_information IS NOT NULL
          AND endpoint_name IS NOT NULL
        GROUP BY endpoint_name
        -- only endpoints that actually fell back (reliability signal, not a dump)
        HAVING SUM(CASE WHEN size(routing_information.attempts) > 1 THEN 1 ELSE 0 END) > 0
        ORDER BY fallback_requests DESC
    """)
    print(f"  ✅ {len(fb_rows_raw)} fallback-routing endpoint rows")
except Exception as exc:
    fb_rows_raw = []
    print(f"  ⚠️  fallback routing query unavailable: {exc}")

if fb_rows_raw:
    rows = [(r.get("endpoint_name", ""), to_int(r.get("total_requests")), to_int(r.get("fallback_requests")),
             to_int(r.get("fallback_recovered")), r.get("fallback_destinations"), r.get("max_event_time"), now)
            for r in fb_rows_raw]
    spark.createDataFrame(rows, UAG_FALLBACK_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(UAG_FALLBACK_TABLE)
else:
    _empty_or_keep(UAG_FALLBACK_SCHEMA, UAG_FALLBACK_TABLE)
print(f"✅ Wrote {len(fb_rows_raw)} rows to {UAG_FALLBACK_TABLE}")

# COMMAND ----------

# Per-service usage (model / MCP / provider services) — the traffic side of the
# Unity Gateway services inventory (workflow 15). Untyped legacy rows excluded.
print("▸ Querying ai_gateway.usage by service …")
try:
    sv_rows_raw = _execute_sql(f"""
        SELECT service_name,
               MAX(service_type)                                          AS service_type,
               COUNT(*)                                                   AS request_count,
               SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END)        AS error_count,
               SUM(CASE WHEN status_code = 429 THEN 1 ELSE 0 END)         AS throttled_count,
               COUNT(DISTINCT requester)                                  AS unique_users,
               COALESCE(SUM(input_tokens), 0)                             AS input_tokens,
               COALESCE(SUM(output_tokens), 0)                            AS output_tokens,
               CAST(PERCENTILE_APPROX(latency_ms, 0.5) AS LONG)           AS p50_latency_ms,
               CAST(PERCENTILE_APPROX(latency_ms, 0.95) AS LONG)          AS p95_latency_ms,
               CAST(MAX(event_time) AS STRING)                            AS max_event_time
        FROM system.ai_gateway.usage
        WHERE event_time >= current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS
          AND service_name IS NOT NULL AND service_name != ''
        GROUP BY service_name
        ORDER BY request_count DESC
    """)
    print(f"  ✅ {len(sv_rows_raw)} service rows")
except Exception as exc:
    sv_rows_raw = []
    print(f"  ⚠️  service usage query unavailable: {exc}")

if sv_rows_raw:
    rows = [(r.get("service_name", ""), r.get("service_type"), to_int(r.get("request_count")),
             to_int(r.get("error_count")), to_int(r.get("throttled_count")), to_int(r.get("unique_users")),
             to_int(r.get("input_tokens")), to_int(r.get("output_tokens")),
             to_int(r.get("p50_latency_ms")), to_int(r.get("p95_latency_ms")), r.get("max_event_time"), now)
            for r in sv_rows_raw]
    spark.createDataFrame(rows, UAG_SERVICE_USAGE_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(UAG_SERVICE_USAGE_TABLE)
else:
    _empty_or_keep(UAG_SERVICE_USAGE_SCHEMA, UAG_SERVICE_USAGE_TABLE)
print(f"✅ Wrote {len(sv_rows_raw)} rows to {UAG_SERVICE_USAGE_TABLE}")

# COMMAND ----------

# endpoint_id → service map over the billing window (90d, matching the billing
# workflow), so older gateway-attributed cost still resolves to a service.
print("▸ Querying ai_gateway.usage endpoint_id → service map (90d) …")
try:
    map_rows_raw = _execute_sql("""
        SELECT endpoint_id, MAX(service_name) AS service_name, MAX(service_type) AS service_type
        FROM system.ai_gateway.usage
        WHERE event_time >= current_timestamp() - INTERVAL 90 DAYS
          AND endpoint_id IS NOT NULL AND service_name IS NOT NULL AND service_name != ''
        GROUP BY endpoint_id
    """)
    print(f"  ✅ {len(map_rows_raw)} endpoint→service rows")
except Exception as exc:
    map_rows_raw = []
    print(f"  ⚠️  endpoint→service map query unavailable: {exc}")

if map_rows_raw:
    rows = [(str(r.get("endpoint_id")), r.get("service_name"), r.get("service_type"), now) for r in map_rows_raw]
    spark.createDataFrame(rows, UAG_ENDPOINT_SERVICE_MAP_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(UAG_ENDPOINT_SERVICE_MAP_TABLE)
else:
    _empty_or_keep(UAG_ENDPOINT_SERVICE_MAP_SCHEMA, UAG_ENDPOINT_SERVICE_MAP_TABLE)
print(f"✅ Wrote {len(map_rows_raw)} rows to {UAG_ENDPOINT_SERVICE_MAP_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Usage-dashboard parity (coding sessions, unit cost, tag spend, model performance, filters, skills)
# MAGIC
# MAGIC Mirrors the built-in Unity Gateway usage dashboard's tabs that the app lacked. These
# MAGIC run through `spark.sql` and write DataFrames straight to Delta (the daily fact is too
# MAGIC large for the Statement API's INLINE result). Each block keeps the previous snapshot
# MAGIC if its query fails; on a first run it writes an empty table from the explicit schema.
# MAGIC
# MAGIC **Estimated spend** = tokens × a blended $/token per gateway endpoint, derived from
# MAGIC `system.billing.usage` (MODEL_SERVING, attributed via `usage_metadata.ai_gateway.endpoint_id`)
# MAGIC over the same window. Input and output tokens are weighted equally (blended rate).

# COMMAND ----------

from pyspark.sql import functions as F

DETAIL_DAYS = 30  # window for the filterable fact, sessions, unit cost, tag spend, skills

_U = f"system.ai_gateway.usage WHERE event_time >= current_timestamp() - INTERVAL {DETAIL_DAYS} DAYS"

# $/token per gateway endpoint from attributed billing (blended in+out).
_RATE_CTE = f"""
    bill AS (
        SELECT u.usage_metadata.ai_gateway.endpoint_id AS endpoint_id,
               SUM(u.usage_quantity * COALESCE(lp.pricing.effective_list.default, lp.pricing.default)) AS cost
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices lp
            ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
            AND u.usage_unit = lp.usage_unit AND lp.price_end_time IS NULL
        WHERE u.billing_origin_product = 'MODEL_SERVING'
          AND u.usage_date >= current_date() - INTERVAL {DETAIL_DAYS} DAYS
          AND u.usage_metadata.ai_gateway.endpoint_id IS NOT NULL
        GROUP BY 1
    ),
    toks AS (
        SELECT endpoint_id, SUM(COALESCE(input_tokens, 0) + COALESCE(output_tokens, 0)) AS tokens
        FROM {_U} AND endpoint_id IS NOT NULL
        GROUP BY 1
    ),
    rate AS (
        SELECT b.endpoint_id, b.cost, b.cost / NULLIF(t.tokens, 0) AS usd_per_token
        FROM bill b JOIN toks t ON b.endpoint_id = t.endpoint_id
    )"""


def _spark_snapshot(sql: str, table: str, empty_schema: str, label: str) -> int:
    """Run sql → overwrite `table`. On failure keep the previous snapshot (or, on a first
    run, create an empty table from `empty_schema` so sync/smoke find it)."""
    print(f"▸ {label} …")
    try:
        df = spark.sql(sql).withColumn("discovered_at", F.lit(now))
        df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
        n = spark.table(table).count()
        print(f"  ✅ {n} rows → {table}")
        return n
    except Exception as exc:
        print(f"  ⚠️  {label} unavailable: {str(exc)[:300]}")
        if spark.catalog.tableExists(table):
            _KEPT_PREVIOUS.append(table.split(".")[-1])
            print(f"  ↺ keeping previous snapshot of {table}")
        else:
            spark.createDataFrame([], f"{empty_schema}, discovered_at TIMESTAMP") \
                .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
        return 0


T = lambda name: f"{CATALOG}.{SCHEMA}.{name}"  # noqa: E731

# 1. Filterable daily fact — drives the Usage tab's date / workspace / service-tag filters.
n_fact = _spark_snapshot(f"""
    SELECT CAST(CAST(event_time AS DATE) AS STRING)               AS usage_date,
           COALESCE(CAST(workspace_id AS STRING), '')            AS workspace_id,
           COALESCE(service_type, '')                            AS service_type,
           COALESCE(service_name, '')                            AS service_name,
           COALESCE(destination_model, '')                       AS destination_model,
           COALESCE(requester_type, '')                          AS requester_type,
           COALESCE(invocation_metadata.source, '')              AS source,
           COALESCE(api_type, '')                                AS api_type,
           COALESCE(auth_mode, '')                               AS auth_mode,
           COUNT(*)                                              AS request_count,
           SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END)   AS error_count,
           SUM(CASE WHEN status_code = 429 THEN 1 ELSE 0 END)    AS throttled_count,
           COALESCE(SUM(input_tokens), 0)                        AS input_tokens,
           COALESCE(SUM(output_tokens), 0)                       AS output_tokens,
           COALESCE(SUM(COALESCE(token_details.cache_read_input_tokens, 0)
                      + COALESCE(token_details.cache_creation_input_tokens, 0)), 0) AS cached_tokens
    FROM {_U}
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
""", T("uag_usage_daily_fact"),
    "usage_date STRING, workspace_id STRING, service_type STRING, service_name STRING, destination_model STRING, "
    "requester_type STRING, source STRING, api_type STRING, auth_mode STRING, request_count BIGINT, error_count BIGINT, "
    "throttled_count BIGINT, input_tokens BIGINT, output_tokens BIGINT, cached_tokens BIGINT",
    "Usage daily fact (filterable)")

# 2. Service tags (for the service-tag filter + tag spend).
n_tags = _spark_snapshot(f"""
    SELECT DISTINCT service_name, k AS tag_key, v AS tag_value
    FROM system.ai_gateway.usage LATERAL VIEW explode(service_tags) t AS k, v
    WHERE event_time >= current_timestamp() - INTERVAL {DETAIL_DAYS} DAYS
      AND service_name IS NOT NULL AND service_name != ''
""", T("uag_service_tags"), "service_name STRING, tag_key STRING, tag_value STRING", "Service tags")

# 3. Daily activity (non-additive distinct counts the fact can't provide).
n_activity = _spark_snapshot(f"""
    SELECT CAST(CAST(event_time AS DATE) AS STRING)                                         AS usage_date,
           COUNT(DISTINCT requester)                                                       AS active_users,
           SUM(CASE WHEN service_type = 'MCP_SERVICE' THEN 1 ELSE 0 END)                   AS mcp_requests,
           COUNT(DISTINCT CASE WHEN service_type = 'MCP_SERVICE' THEN requester END)       AS mcp_users,
           COUNT(DISTINCT session_metadata.client_session_id)                              AS coding_sessions,
           COUNT(DISTINCT CASE WHEN session_metadata.client_session_id IS NOT NULL
                               THEN requester END)                                         AS coding_users
    FROM {_U}
    GROUP BY 1
""", T("uag_daily_activity"),
    "usage_date STRING, active_users BIGINT, mcp_requests BIGINT, mcp_users BIGINT, coding_sessions BIGINT, coding_users BIGINT",
    "Daily activity")

# 4. Coding-agent sessions per day × harness × surface (session_metadata).
n_coding_daily = _spark_snapshot(f"""
    WITH {_RATE_CTE}
    SELECT CAST(CAST(u.event_time AS DATE) AS STRING)                    AS usage_date,
           COALESCE(u.session_metadata.coding_agent, 'unknown')         AS coding_agent,
           COALESCE(u.session_metadata.surface, 'unknown')              AS surface,
           COUNT(DISTINCT u.session_metadata.client_session_id)         AS sessions,
           COUNT(DISTINCT u.requester)                                  AS users,
           COUNT(DISTINCT u.session_metadata.client_subagent_id)        AS subagents,
           COUNT(*)                                                     AS requests,
           COALESCE(SUM(u.input_tokens), 0)                             AS input_tokens,
           COALESCE(SUM(u.output_tokens), 0)                            AS output_tokens,
           COALESCE(SUM(u.token_details.output_reasoning_tokens), 0)    AS reasoning_tokens,
           ROUND(COALESCE(SUM((COALESCE(u.input_tokens, 0) + COALESCE(u.output_tokens, 0)) * r.usd_per_token), 0), 4) AS est_cost_usd
    FROM system.ai_gateway.usage u
    LEFT JOIN rate r ON r.endpoint_id = u.endpoint_id
    WHERE u.event_time >= current_timestamp() - INTERVAL {DETAIL_DAYS} DAYS
      AND u.session_metadata.client_session_id IS NOT NULL
    GROUP BY 1, 2, 3
""", T("uag_coding_sessions_daily"),
    "usage_date STRING, coding_agent STRING, surface STRING, sessions BIGINT, users BIGINT, subagents BIGINT, requests BIGINT, "
    "input_tokens BIGINT, output_tokens BIGINT, reasoning_tokens BIGINT, est_cost_usd DOUBLE",
    "Coding sessions daily")

# 5. Top sessions by estimated spend (session drilldown).
n_coding_top = _spark_snapshot(f"""
    WITH {_RATE_CTE},
    s AS (
        SELECT u.session_metadata.client_session_id                            AS session_id,
               MAX(u.requester)                                                AS requester,
               MAX(u.session_metadata.coding_agent)                            AS coding_agent,
               MAX(u.session_metadata.surface)                                 AS surface,
               MAX(u.session_metadata.agent_version)                           AS agent_version,
               MAX(u.session_metadata.reasoning_effort)                        AS reasoning_effort,
               concat_ws(', ', slice(array_sort(collect_set(u.destination_model)), 1, 5)) AS models,
               CAST(MIN(u.event_time) AS STRING)                               AS first_seen,
               CAST(MAX(u.event_time) AS STRING)                               AS last_seen,
               COUNT(*)                                                        AS requests,
               COUNT(DISTINCT u.session_metadata.client_subagent_id)           AS subagents,
               COALESCE(SUM(u.input_tokens), 0)                                AS input_tokens,
               COALESCE(SUM(u.output_tokens), 0)                               AS output_tokens,
               COALESCE(SUM(u.token_details.output_reasoning_tokens), 0)       AS reasoning_tokens,
               ROUND(COALESCE(SUM((COALESCE(u.input_tokens, 0) + COALESCE(u.output_tokens, 0)) * r.usd_per_token), 0), 4) AS est_cost_usd
        FROM system.ai_gateway.usage u
        LEFT JOIN rate r ON r.endpoint_id = u.endpoint_id
        WHERE u.event_time >= current_timestamp() - INTERVAL {DETAIL_DAYS} DAYS
          AND u.session_metadata.client_session_id IS NOT NULL
        GROUP BY 1
    )
    SELECT * FROM s ORDER BY est_cost_usd DESC, requests DESC LIMIT 200
""", T("uag_coding_sessions_top"),
    "session_id STRING, requester STRING, coding_agent STRING, surface STRING, agent_version STRING, reasoning_effort STRING, "
    "models STRING, first_seen STRING, last_seen STRING, requests BIGINT, subagents BIGINT, input_tokens BIGINT, "
    "output_tokens BIGINT, reasoning_tokens BIGINT, est_cost_usd DOUBLE",
    "Top coding sessions")

# 6. Unit economics per service ($ / 1M tokens, $ / 1k requests) from attributed billing.
_SVC_COST_CTE = f"""
    {_RATE_CTE},
    ep_svc AS (
        SELECT endpoint_id, MAX(service_name) AS service_name, MAX(service_type) AS service_type,
               COUNT(*) AS requests,
               COALESCE(SUM(input_tokens), 0) AS input_tokens, COALESCE(SUM(output_tokens), 0) AS output_tokens
        FROM {_U} AND endpoint_id IS NOT NULL AND service_name IS NOT NULL AND service_name != ''
        GROUP BY 1
    ),
    svc AS (
        SELECT e.service_name, MAX(e.service_type) AS service_type,
               SUM(COALESCE(r.cost, 0)) AS cost_usd, SUM(e.requests) AS requests,
               SUM(e.input_tokens) AS input_tokens, SUM(e.output_tokens) AS output_tokens
        FROM ep_svc e LEFT JOIN rate r ON r.endpoint_id = e.endpoint_id
        GROUP BY 1
    )"""
n_unit = _spark_snapshot(f"""
    WITH {_SVC_COST_CTE}
    SELECT service_name, service_type, ROUND(cost_usd, 4) AS cost_usd, requests, input_tokens, output_tokens,
           ROUND(cost_usd * 1e6 / NULLIF(input_tokens + output_tokens, 0), 4) AS usd_per_1m_tokens,
           ROUND(cost_usd * 1e3 / NULLIF(requests, 0), 4)                     AS usd_per_1k_requests
    FROM svc WHERE cost_usd > 0
""", T("uag_service_unit_cost"),
    "service_name STRING, service_type STRING, cost_usd DOUBLE, requests BIGINT, input_tokens BIGINT, output_tokens BIGINT, "
    "usd_per_1m_tokens DOUBLE, usd_per_1k_requests DOUBLE",
    "Service unit cost")

# 7. Spend by service tag (attributed cost of each service, rolled up by its tags).
n_tag_cost = _spark_snapshot(f"""
    WITH {_SVC_COST_CTE},
    tags AS (
        SELECT DISTINCT service_name, k AS tag_key, v AS tag_value
        FROM system.ai_gateway.usage LATERAL VIEW explode(service_tags) t AS k, v
        WHERE event_time >= current_timestamp() - INTERVAL {DETAIL_DAYS} DAYS AND service_name IS NOT NULL
    )
    SELECT t.tag_key, t.tag_value, ROUND(SUM(s.cost_usd), 4) AS cost_usd,
           COUNT(DISTINCT s.service_name) AS services, SUM(s.requests) AS requests
    FROM svc s JOIN tags t ON t.service_name = s.service_name
    WHERE s.cost_usd > 0
    GROUP BY 1, 2
""", T("uag_cost_by_service_tag"),
    "tag_key STRING, tag_value STRING, cost_usd DOUBLE, services BIGINT, requests BIGINT",
    "Spend by service tag")

# 8. Performance per model (7d): latency, streaming TTFB, output tokens/sec, cache, errors.
# Throughput only on streaming responses — non-streaming latency ≈ TTFB and yields
# meaningless tokens/sec.
n_perf = _spark_snapshot(f"""
    SELECT destination_model,
           COUNT(*)                                                                    AS requests,
           SUM(CASE WHEN status_code >= 400 THEN 1 ELSE 0 END)                         AS error_count,
           SUM(CASE WHEN status_code = 429 THEN 1 ELSE 0 END)                          AS throttled_count,
           SUM(CASE WHEN response_content_type LIKE 'text/event-stream%' THEN 1 ELSE 0 END) AS streaming_requests,
           CAST(PERCENTILE_APPROX(latency_ms, 0.5) AS BIGINT)                          AS p50_latency_ms,
           CAST(PERCENTILE_APPROX(latency_ms, 0.95) AS BIGINT)                         AS p95_latency_ms,
           CAST(PERCENTILE_APPROX(CASE WHEN response_content_type LIKE 'text/event-stream%'
                                       THEN time_to_first_byte_ms END, 0.5) AS BIGINT)  AS p50_ttfb_ms,
           CAST(PERCENTILE_APPROX(CASE WHEN response_content_type LIKE 'text/event-stream%'
                                       THEN time_to_first_byte_ms END, 0.95) AS BIGINT) AS p95_ttfb_ms,
           ROUND(PERCENTILE_APPROX(CASE WHEN response_content_type LIKE 'text/event-stream%'
                                         AND output_tokens > 50 AND latency_ms > time_to_first_byte_ms
                                        THEN output_tokens * 1000.0 / (latency_ms - time_to_first_byte_ms) END, 0.5), 1)
                                                                                       AS p50_output_tps,
           COALESCE(SUM(input_tokens), 0)                                              AS input_tokens,
           COALESCE(SUM(token_details.cache_read_input_tokens), 0)                     AS cache_read_tokens,
           COALESCE(SUM(token_details.output_reasoning_tokens), 0)                     AS reasoning_tokens
    FROM system.ai_gateway.usage
    WHERE event_time >= current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS
      AND destination_model IS NOT NULL AND destination_model != ''
    GROUP BY 1
""", T("uag_model_performance"),
    "destination_model STRING, requests BIGINT, error_count BIGINT, throttled_count BIGINT, streaming_requests BIGINT, "
    "p50_latency_ms BIGINT, p95_latency_ms BIGINT, p50_ttfb_ms BIGINT, p95_ttfb_ms BIGINT, p50_output_tps DOUBLE, "
    "input_tokens BIGINT, cache_read_tokens BIGINT, reasoning_tokens BIGINT",
    "Model performance")

# 9. Skills activity — not in the usage table; audit service `aiGatewaySkills`.
_SKILL_SRC = f"""
    system.access.audit
    WHERE event_date >= current_date() - INTERVAL {DETAIL_DAYS} DAYS
      AND service_name = 'aiGatewaySkills'
      AND request_params['fully_qualified_name'] IS NOT NULL"""
n_skills = _spark_snapshot(f"""
    SELECT request_params['fully_qualified_name']                                       AS skill_name,
           SUM(CASE WHEN action_name = 'skillLoad' THEN 1 ELSE 0 END)                   AS loads,
           SUM(CASE WHEN action_name = 'skillFetchBundleFiles' THEN 1 ELSE 0 END)       AS bundle_fetches,
           COUNT(DISTINCT user_identity.email)                                          AS users,
           CAST(MAX(event_time) AS STRING)                                              AS last_used
    FROM {_SKILL_SRC}
    GROUP BY 1
""", T("uag_skill_usage"),
    "skill_name STRING, loads BIGINT, bundle_fetches BIGINT, users BIGINT, last_used STRING", "Skill usage")
n_skill_users = _spark_snapshot(f"""
    SELECT request_params['fully_qualified_name'] AS skill_name,
           COALESCE(user_identity.email, 'unknown') AS user_name,
           SUM(CASE WHEN action_name = 'skillLoad' THEN 1 ELSE 0 END) AS loads,
           CAST(MAX(event_time) AS STRING) AS last_used
    FROM {_SKILL_SRC}
    GROUP BY 1, 2
""", T("uag_skill_users"), "skill_name STRING, user_name STRING, loads BIGINT, last_used STRING", "Skill users")

parity_counts = {"usage_daily_fact": n_fact, "service_tags": n_tags, "daily_activity": n_activity,
                 "coding_sessions_daily": n_coding_daily, "coding_sessions_top": n_coding_top,
                 "service_unit_cost": n_unit, "cost_by_service_tag": n_tag_cost,
                 "model_performance": n_perf, "skill_usage": n_skills, "skill_users": n_skill_users}

# COMMAND ----------

result = {"status": "success", "parity": parity_counts, "uag_service_rows": len(sv_rows_raw), "uag_endpoint_service_map_rows": len(map_rows_raw), "kept_previous": _KEPT_PREVIOUS, "uag_endpoint_rows": len(rows_raw), "uag_breakdown_rows": len(bd_rows),
          "uag_mcp_tool_rows": len(mcp_rows_raw), "uag_guardrail_rows": len(gr_rows_raw),
          "uag_timeseries_rows": len(ts_rows_raw), "uag_coding_agent_rows": len(ca_rows_raw),
          "uag_throttling_rows": len(th_rows_raw), "uag_fallback_rows": len(fb_rows_raw),
          "retention_days": RETENTION_DAYS, "discovered_at": now.isoformat()}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
