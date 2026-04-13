# Databricks notebook source
# MAGIC %md
# MAGIC # User Analytics Discovery Job
# MAGIC
# MAGIC Queries `system.serving.endpoint_usage` for user activity data
# MAGIC and writes results to Delta tables for Lakebase sync.
# MAGIC
# MAGIC **Data flow:** system.serving.endpoint_usage → Delta tables → Lakebase (sync task)

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

UA_DAILY_TABLE = f"{CATALOG}.{SCHEMA}.user_analytics_daily"
UA_HEATMAP_TABLE = f"{CATALOG}.{SCHEMA}.user_analytics_heatmap"

print(f"Target tables: {UA_DAILY_TABLE}, {UA_HEATMAP_TABLE}")
print(f"Warehouse: {WAREHOUSE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table Schemas

# COMMAND ----------

UA_DAILY_SCHEMA = StructType([
    StructField("usage_date", StringType(), False),
    StructField("requester", StringType(), False),
    StructField("endpoint_name", StringType(), False),
    StructField("request_count", LongType(), True),
    StructField("total_tokens", LongType(), True),
    StructField("discovered_at", TimestampType(), False),
])

UA_HEATMAP_SCHEMA = StructType([
    StructField("dow", IntegerType(), False),
    StructField("hour", IntegerType(), False),
    StructField("request_count", LongType(), True),
    StructField("period_days", IntegerType(), False),
    StructField("discovered_at", TimestampType(), False),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Statements API Helper

# COMMAND ----------

def _execute_sql(sql):
    """Execute SQL via SQL Statements API."""
    if not WAREHOUSE_ID:
        print("  No warehouse ID — cannot query system tables")
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query User Activity (Last 90 Days)

# COMMAND ----------

now = datetime.now(timezone.utc)

print("▸ Querying system.serving.endpoint_usage for daily user activity ...")
daily_rows = _execute_sql("""
    SELECT
        CAST(DATE(u.request_time) AS STRING) AS usage_date,
        u.requester,
        se.endpoint_name,
        COUNT(*) AS request_count,
        COALESCE(SUM(u.input_token_count + u.output_token_count), 0) AS total_tokens
    FROM system.serving.endpoint_usage u
    JOIN system.serving.served_entities se
        ON u.served_entity_id = se.served_entity_id
    WHERE u.request_time >= date_sub(current_date(), 90)
      AND u.requester IS NOT NULL
    GROUP BY DATE(u.request_time), u.requester, se.endpoint_name
""")
print(f"  ✅ {len(daily_rows)} daily user activity rows")

# COMMAND ----------

print("▸ Querying heatmap data ...")
heatmap_rows_30 = _execute_sql("""
    SELECT
        DAYOFWEEK(request_time) - 1 AS dow,
        HOUR(request_time) AS hour,
        COUNT(*) AS request_count
    FROM system.serving.endpoint_usage
    WHERE request_time >= date_sub(current_date(), 30)
      AND requester IS NOT NULL
    GROUP BY DAYOFWEEK(request_time), HOUR(request_time)
""")
print(f"  ✅ {len(heatmap_rows_30)} heatmap buckets (30d)")

heatmap_rows_90 = _execute_sql("""
    SELECT
        DAYOFWEEK(request_time) - 1 AS dow,
        HOUR(request_time) AS hour,
        COUNT(*) AS request_count
    FROM system.serving.endpoint_usage
    WHERE request_time >= date_sub(current_date(), 90)
      AND requester IS NOT NULL
    GROUP BY DAYOFWEEK(request_time), HOUR(request_time)
""")
print(f"  ✅ {len(heatmap_rows_90)} heatmap buckets (90d)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Tables

# COMMAND ----------

# Write daily activity
if daily_rows:
    rows = [(
        r.get("usage_date", ""),
        r.get("requester", ""),
        r.get("endpoint_name", ""),
        int(r.get("request_count") or 0),
        int(r.get("total_tokens") or 0),
        now,
    ) for r in daily_rows]
    df = spark.createDataFrame(rows, UA_DAILY_SCHEMA)
    df.write.mode("overwrite").saveAsTable(UA_DAILY_TABLE)
    print(f"✅ Wrote {len(rows)} rows to {UA_DAILY_TABLE}")
else:
    spark.createDataFrame([], UA_DAILY_SCHEMA).write.mode("overwrite").saveAsTable(UA_DAILY_TABLE)
    print("ℹ️  No daily data — empty table written")

# COMMAND ----------

# Write heatmap (combine 30d and 90d)
heatmap_all = []
for r in heatmap_rows_30:
    heatmap_all.append((int(r.get("dow") or 0), int(r.get("hour") or 0),
                        int(r.get("request_count") or 0), 30, now))
for r in heatmap_rows_90:
    heatmap_all.append((int(r.get("dow") or 0), int(r.get("hour") or 0),
                        int(r.get("request_count") or 0), 90, now))

if heatmap_all:
    df = spark.createDataFrame(heatmap_all, UA_HEATMAP_SCHEMA)
    df.write.mode("overwrite").saveAsTable(UA_HEATMAP_TABLE)
    print(f"✅ Wrote {len(heatmap_all)} rows to {UA_HEATMAP_TABLE}")
else:
    spark.createDataFrame([], UA_HEATMAP_SCHEMA).write.mode("overwrite").saveAsTable(UA_HEATMAP_TABLE)
    print("ℹ️  No heatmap data — empty table written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

result = {
    "status": "success",
    "daily_rows": len(daily_rows),
    "heatmap_30d": len(heatmap_rows_30),
    "heatmap_90d": len(heatmap_rows_90),
    "discovered_at": now.isoformat(),
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
