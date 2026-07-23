# Databricks notebook source
# MAGIC %md
# MAGIC # Unity AI Gateway (v2) Usage Discovery Job
# MAGIC
# MAGIC Queries `system.ai_gateway.usage` (v2 Unity AI Gateway traffic only) for a
# MAGIC per-endpoint usage summary and writes a Delta table for `02_sync_to_lakebase`.
# MAGIC
# MAGIC **Why a separate source:** `system.ai_gateway.usage` is ~20-min fresh (vs
# MAGIC ~2 hr for `system.billing.usage`) and carries data the others lack — cached
# MAGIC tokens (`token_details`), time-to-first-byte, per-request tags. It covers
# MAGIC ONLY requests routed through Unity AI Gateway v2 endpoints (a subset of all
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
print(f"Target tables: {UAG_TABLE}, {UAG_BREAKDOWN_TABLE}, {UAG_MCP_TOOL_TABLE}, {UAG_GUARDRAIL_TABLE}, {UAG_TIMESERIES_TABLE}, {UAG_THROTTLING_TABLE} | retention {RETENTION_DAYS}d")

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

# COMMAND ----------

def _execute_sql(sql: str) -> List[Dict[str, Any]]:
    if not WAREHOUSE_ID:
        print("  No warehouse ID")
        return []
    w = WorkspaceClient()
    body = {"warehouse_id": WAREHOUSE_ID, "statement": sql,
            "wait_timeout": "50s", "disposition": "INLINE", "format": "JSON_ARRAY"}
    try:
        resp = w.api_client.do("POST", "/api/2.0/sql/statements", body=body)
    except Exception as exc:
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
    if status != "SUCCEEDED":
        err = resp.get("status", {}).get("error", {})
        print(f"  SQL {status}: {err.get('message', '')[:300]}")
        return []
    cols = [c["name"] for c in resp.get("manifest", {}).get("schema", {}).get("columns", [])]
    return [dict(zip(cols, row)) for row in resp.get("result", {}).get("data_array", [])]

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
    spark.createDataFrame([], UAG_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(UAG_TABLE)
print(f"✅ Wrote {len(rows_raw)} rows to {UAG_TABLE}")

# COMMAND ----------

# Breakdowns: agent-vs-human (requester_type), model (destination_model), api_type,
# service_type (model vs MCP vs provider), and route_action (routing outcomes).
print("▸ Querying ai_gateway.usage breakdowns (requester_type / destination_model / api_type / source / service_type / route_action) …")
try:
    bd_rows = _execute_sql(f"""
        WITH base AS (
            SELECT requester_type, destination_model, api_type, service_type,
                   invocation_metadata.source AS source, workspace_id,
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
    spark.createDataFrame([], UAG_BREAKDOWN_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(UAG_BREAKDOWN_TABLE)
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
    spark.createDataFrame([], UAG_TIMESERIES_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(UAG_TIMESERIES_TABLE)
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
    spark.createDataFrame([], UAG_CODING_AGENT_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(UAG_CODING_AGENT_TABLE)
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
    spark.createDataFrame([], UAG_MCP_TOOL_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(UAG_MCP_TOOL_TABLE)
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
    spark.createDataFrame([], UAG_GUARDRAIL_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(UAG_GUARDRAIL_TABLE)
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
    spark.createDataFrame([], UAG_THROTTLING_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(UAG_THROTTLING_TABLE)
print(f"✅ Wrote {len(th_rows_raw)} rows to {UAG_THROTTLING_TABLE}")

# COMMAND ----------

result = {"status": "success", "uag_endpoint_rows": len(rows_raw), "uag_breakdown_rows": len(bd_rows),
          "uag_mcp_tool_rows": len(mcp_rows_raw), "uag_guardrail_rows": len(gr_rows_raw),
          "uag_timeseries_rows": len(ts_rows_raw), "uag_coding_agent_rows": len(ca_rows_raw),
          "uag_throttling_rows": len(th_rows_raw),
          "retention_days": RETENTION_DAYS, "discovered_at": now.isoformat()}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
