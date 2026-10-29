# Databricks notebook source
# MAGIC %md
# MAGIC # Gateway Usage Discovery Job
# MAGIC
# MAGIC Queries `system.ai_gateway.usage` plus the `system.serving.endpoint_usage`
# MAGIC rows the gateway did not log, and writes results to Delta tables for Lakebase sync.
# MAGIC
# MAGIC **Data flow:** system.ai_gateway.usage + system.serving.endpoint_usage → Delta tables → Lakebase (sync task)

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

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog name")
dbutils.widgets.text("schema", "", "Schema name")
dbutils.widgets.text("warehouse_id", "", "SQL warehouse ID")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")

if not CATALOG or not SCHEMA:
    raise ValueError(f"catalog and schema required (got {CATALOG!r}, {SCHEMA!r})")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

GW_USAGE_DAILY_TABLE = f"{CATALOG}.{SCHEMA}.gateway_usage_daily"
GW_USAGE_HOURLY_TABLE = f"{CATALOG}.{SCHEMA}.gateway_usage_hourly"

print(f"Target tables: {GW_USAGE_DAILY_TABLE}, {GW_USAGE_HOURLY_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schemas

# COMMAND ----------

GW_DAILY_SCHEMA = StructType([
    StructField("usage_date", StringType(), False),
    StructField("endpoint_name", StringType(), False),
    StructField("requester", StringType(), True),
    StructField("request_count", LongType(), True),
    StructField("input_tokens", LongType(), True),
    StructField("output_tokens", LongType(), True),
    StructField("error_count", LongType(), True),
    StructField("rate_limited_count", LongType(), True),
    StructField("discovered_at", TimestampType(), False),
])

GW_HOURLY_SCHEMA = StructType([
    StructField("hour", StringType(), False),
    StructField("endpoint_name", StringType(), True),
    StructField("request_count", LongType(), True),
    StructField("input_tokens", LongType(), True),
    StructField("output_tokens", LongType(), True),
    StructField("error_count", LongType(), True),
    StructField("rate_limited_count", LongType(), True),
    StructField("discovered_at", TimestampType(), False),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Helper

# COMMAND ----------

def _execute_sql(sql):
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
        print(f"  SQL {status}: {err.get('message', '')[:200]}")
        return []
    cols = [c["name"] for c in resp.get("manifest", {}).get("schema", {}).get("columns", [])]
    return [dict(zip(cols, row)) for row in resp.get("result", {}).get("data_array", [])]


def _gateway_usage_readable() -> bool:
    """One probe per run: can this run read system.ai_gateway.usage?

    The table is account-admin scoped by default, so on many accounts the
    workflow identity can't read it. Without this probe the combined query would
    fail outright and (in these notebooks) overwrite good tables with empty ones
    or fail the task. Polls so a cold warehouse isn't mistaken for "no access".
    """
    try:
        client = WorkspaceClient().api_client
        resp = client.do("POST", "/api/2.0/sql/statements", body={
            "warehouse_id": WAREHOUSE_ID,
            "statement": "SELECT 1 FROM system.ai_gateway.usage LIMIT 1",
            "wait_timeout": "30s", "disposition": "INLINE", "format": "JSON_ARRAY"})
        sid = resp.get("statement_id")
        for _ in range(30):
            state = resp.get("status", {}).get("state", "")
            if state not in ("PENDING", "RUNNING") or not sid:
                break
            time.sleep(3)
            resp = client.do("GET", f"/api/2.0/sql/statements/{sid}")
        ok = resp.get("status", {}).get("state") == "SUCCEEDED"
        if not ok:
            err = resp.get("status", {}).get("error", {}).get("message", "")
            print(f"  ⚠️  system.ai_gateway.usage not readable ({err[:160]}) — counting endpoint_usage only")
        return ok
    except Exception as exc:
        print(f"  ⚠️  system.ai_gateway.usage probe failed ({exc}) — counting endpoint_usage only")
        return False


GATEWAY_USAGE_READABLE = _gateway_usage_readable()


def _model_requests_cte(since: str) -> str:
    """WITH clause defining `model_requests`: one row per model call since `since`.

    system.ai_gateway.usage logs every call through Unity Gateway, including the
    system.ai.* model services and ai_query retries that endpoint_usage misses.
    endpoint_usage adds the traffic that bypasses the gateway; its rows whose
    request id the gateway also logged are dropped, so nothing counts twice.

    If the gateway table isn't readable, falls back to endpoint_usage alone (the
    pre-gateway behaviour) rather than failing. Gateway rows without an
    endpoint_name are excluded (endpoint_name is a key column downstream).
    """
    eu = f"""
        SELECT u.request_time, u.workspace_id, se.endpoint_name, u.requester,
               u.input_token_count AS input_tokens, u.output_token_count AS output_tokens, u.status_code
        FROM system.serving.endpoint_usage u
        JOIN system.serving.served_entities se
            ON u.served_entity_id = se.served_entity_id
        {{anti}}
        WHERE u.request_time >= {since}"""
    if not GATEWAY_USAGE_READABLE:
        return f"""
    WITH model_requests AS ({eu.format(anti="")}
    )
    """
    return f"""
    WITH gw AS (
        SELECT request_id, event_time AS request_time, workspace_id, endpoint_name,
               requester, input_tokens, output_tokens, status_code
        FROM system.ai_gateway.usage
        WHERE event_time >= {since}
          AND service_type IS DISTINCT FROM 'MCP_SERVICE'  -- tool calls, no tokens
          AND endpoint_name IS NOT NULL
    ),
    model_requests AS (
        SELECT request_time, workspace_id, endpoint_name, requester,
               input_tokens, output_tokens, status_code
        FROM gw
        UNION ALL{eu.format(anti="LEFT ANTI JOIN gw ON gw.request_id = u.databricks_request_id")}
    )
    """

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Gateway Usage (Last 90 Days)

# COMMAND ----------

now = datetime.now(timezone.utc)

# Daily per-endpoint, per-user usage
print("▸ Querying system.ai_gateway.usage + endpoint_usage for daily gateway usage ...")
daily_rows = _execute_sql(_model_requests_cte("date_sub(current_date(), 90)") + """
    SELECT
        CAST(DATE(u.request_time) AS STRING) AS usage_date,
        u.endpoint_name,
        u.requester,
        COUNT(*) AS request_count,
        COALESCE(SUM(u.input_tokens), 0) AS input_tokens,
        COALESCE(SUM(u.output_tokens), 0) AS output_tokens,
        SUM(CASE WHEN u.status_code >= 400 THEN 1 ELSE 0 END) AS error_count,
        SUM(CASE WHEN u.status_code = 429 THEN 1 ELSE 0 END) AS rate_limited_count
    FROM model_requests u
    GROUP BY DATE(u.request_time), u.endpoint_name, u.requester
""")
print(f"  ✅ {len(daily_rows)} daily usage rows")

# COMMAND ----------

# Hourly per-endpoint usage (last 7 days for time series)
print("▸ Querying hourly usage (last 7 days) ...")
hourly_rows = _execute_sql(_model_requests_cte("date_sub(current_date(), 7)") + """
    SELECT
        CAST(DATE_TRUNC('HOUR', u.request_time) AS STRING) AS hour,
        u.endpoint_name,
        COUNT(*) AS request_count,
        COALESCE(SUM(u.input_tokens), 0) AS input_tokens,
        COALESCE(SUM(u.output_tokens), 0) AS output_tokens,
        SUM(CASE WHEN u.status_code >= 400 THEN 1 ELSE 0 END) AS error_count,
        SUM(CASE WHEN u.status_code = 429 THEN 1 ELSE 0 END) AS rate_limited_count
    FROM model_requests u
    GROUP BY DATE_TRUNC('HOUR', u.request_time), u.endpoint_name
    ORDER BY hour
""")
print(f"  ✅ {len(hourly_rows)} hourly usage rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta

# COMMAND ----------

if daily_rows:
    rows = [(r.get("usage_date",""), r.get("endpoint_name",""), r.get("requester",""),
             int(r.get("request_count") or 0), int(r.get("input_tokens") or 0),
             int(r.get("output_tokens") or 0), int(r.get("error_count") or 0),
             int(r.get("rate_limited_count") or 0), now)
            for r in daily_rows]
    spark.createDataFrame(rows, GW_DAILY_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GW_USAGE_DAILY_TABLE)
    print(f"✅ Wrote {len(rows)} rows to {GW_USAGE_DAILY_TABLE}")
else:
    spark.createDataFrame([], GW_DAILY_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GW_USAGE_DAILY_TABLE)

if hourly_rows:
    rows = [(r.get("hour",""), r.get("endpoint_name",""),
             int(r.get("request_count") or 0), int(r.get("input_tokens") or 0),
             int(r.get("output_tokens") or 0), int(r.get("error_count") or 0),
             int(r.get("rate_limited_count") or 0), now)
            for r in hourly_rows]
    spark.createDataFrame(rows, GW_HOURLY_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GW_USAGE_HOURLY_TABLE)
    print(f"✅ Wrote {len(rows)} rows to {GW_USAGE_HOURLY_TABLE}")
else:
    spark.createDataFrame([], GW_HOURLY_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(GW_USAGE_HOURLY_TABLE)

# COMMAND ----------

result = {
    "status": "success",
    "daily_rows": len(daily_rows),
    "hourly_rows": len(hourly_rows),
    "discovered_at": now.isoformat(),
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
