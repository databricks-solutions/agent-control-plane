# Databricks notebook source
# MAGIC %md
# MAGIC # Billing Discovery Job
# MAGIC
# MAGIC Queries `system.billing.usage`, `system.billing.list_prices`, and
# MAGIC `system.serving.endpoint_usage` and writes four Delta billing tables
# MAGIC for the sync task (`02_sync_to_lakebase`) to mirror into Lakebase.
# MAGIC
# MAGIC **Tables written:**
# MAGIC - `billing_serving_daily` — model-serving cost by date × workspace × endpoint × SKU
# MAGIC - `billing_token_daily` — token usage by date × workspace × endpoint
# MAGIC - `billing_product_daily` — all-product costs by date × workspace × product
# MAGIC - `billing_user_endpoint_daily` — per-user token usage by date × workspace × endpoint
# MAGIC
# MAGIC Replaces the in-app refresh that used to run on app startup in
# MAGIC `backend/services/billing_service.py`. The app now only reads.
# MAGIC
# MAGIC **Data flow:** system tables → Delta → Lakebase (sync task) → app reads

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
    StructType, StructField, StringType, LongType, IntegerType,
    DecimalType, TimestampType,
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog name")
dbutils.widgets.text("schema", "", "Schema name")
dbutils.widgets.text("warehouse_id", "", "SQL warehouse ID")
dbutils.widgets.text("billing_retention_days", "90", "Days of billing history to load")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
RETENTION_DAYS = int(dbutils.widgets.get("billing_retention_days") or "90")

if not CATALOG or not SCHEMA:
    raise ValueError(f"catalog and schema required (got {CATALOG!r}, {SCHEMA!r})")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

SERVING_TABLE  = f"{CATALOG}.{SCHEMA}.billing_serving_daily"
TOKEN_TABLE    = f"{CATALOG}.{SCHEMA}.billing_token_daily"
PRODUCT_TABLE  = f"{CATALOG}.{SCHEMA}.billing_product_daily"
USER_EP_TABLE  = f"{CATALOG}.{SCHEMA}.billing_user_endpoint_daily"

print(f"Target tables:")
print(f"  {SERVING_TABLE}")
print(f"  {TOKEN_TABLE}")
print(f"  {PRODUCT_TABLE}")
print(f"  {USER_EP_TABLE}")
print(f"Retention: {RETENTION_DAYS} days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schemas

# COMMAND ----------

SERVING_SCHEMA = StructType([
    StructField("usage_date", StringType(), False),
    StructField("workspace_id", StringType(), False),
    StructField("endpoint_name", StringType(), False),
    StructField("sku_name", StringType(), True),
    StructField("total_dbus", DecimalType(18, 4), True),
    StructField("total_cost_usd", DecimalType(18, 4), True),
    StructField("discovered_at", TimestampType(), False),
])

TOKEN_SCHEMA = StructType([
    StructField("usage_date", StringType(), False),
    StructField("workspace_id", StringType(), False),
    StructField("endpoint_name", StringType(), False),
    StructField("request_count", LongType(), True),
    StructField("input_tokens", LongType(), True),
    StructField("output_tokens", LongType(), True),
    StructField("avg_input_tokens", DecimalType(12, 2), True),
    StructField("avg_output_tokens", DecimalType(12, 2), True),
    StructField("discovered_at", TimestampType(), False),
])

PRODUCT_SCHEMA = StructType([
    StructField("usage_date", StringType(), False),
    StructField("workspace_id", StringType(), False),
    StructField("billing_origin_product", StringType(), False),
    StructField("total_dbus", DecimalType(18, 4), True),
    StructField("total_cost_usd", DecimalType(18, 4), True),
    StructField("discovered_at", TimestampType(), False),
])

USER_EP_SCHEMA = StructType([
    StructField("usage_date", StringType(), False),
    StructField("workspace_id", StringType(), False),
    StructField("endpoint_name", StringType(), False),
    StructField("user_identity", StringType(), False),
    StructField("request_count", LongType(), True),
    StructField("input_tokens", LongType(), True),
    StructField("output_tokens", LongType(), True),
    StructField("discovered_at", TimestampType(), False),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Helper

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
        for _ in range(40):  # ~2 min total
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

# MAGIC %md
# MAGIC ## Query 1: billing_serving_daily (model-serving cost by endpoint × SKU)

# COMMAND ----------

now = datetime.now(timezone.utc)

print(f"▸ Querying system.billing.usage for model-serving costs ({RETENTION_DAYS} days) …")
serving_rows = _execute_sql(f"""
    SELECT
        CAST(u.usage_date AS STRING)                AS usage_date,
        u.workspace_id,
        u.usage_metadata.endpoint_name              AS endpoint_name,
        u.sku_name,
        ROUND(SUM(u.usage_quantity), 4)             AS total_dbus,
        ROUND(SUM(u.usage_quantity *
            COALESCE(lp.pricing.effective_list.default, lp.pricing.default, 0.07)
        ), 4)                                        AS total_cost_usd
    FROM system.billing.usage u
    LEFT JOIN system.billing.list_prices lp
        ON u.sku_name = lp.sku_name
        AND u.cloud   = lp.cloud
        AND u.usage_unit = lp.usage_unit
        AND lp.price_end_time IS NULL
    WHERE u.billing_origin_product = 'MODEL_SERVING'
      AND u.usage_date >= current_date() - INTERVAL {RETENTION_DAYS} DAYS
      AND u.usage_metadata.endpoint_name IS NOT NULL
      AND u.workspace_id IS NOT NULL
    GROUP BY u.usage_date, u.workspace_id, u.usage_metadata.endpoint_name, u.sku_name
""")
print(f"  ✅ {len(serving_rows)} serving-cost rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 2: billing_token_daily (token usage by endpoint)

# COMMAND ----------

print(f"▸ Querying system.serving.endpoint_usage for token usage ({RETENTION_DAYS} days) …")
token_rows = _execute_sql(f"""
    SELECT
        CAST(DATE(eu.request_time) AS STRING)       AS usage_date,
        eu.workspace_id,
        se.endpoint_name,
        COUNT(*)                                     AS request_count,
        COALESCE(SUM(eu.input_token_count), 0)       AS input_tokens,
        COALESCE(SUM(eu.output_token_count), 0)      AS output_tokens,
        ROUND(AVG(eu.input_token_count), 2)          AS avg_input_tokens,
        ROUND(AVG(eu.output_token_count), 2)         AS avg_output_tokens
    FROM system.serving.endpoint_usage eu
    JOIN system.serving.served_entities se
        ON eu.served_entity_id = se.served_entity_id
    WHERE eu.request_time >= current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS
      AND eu.workspace_id IS NOT NULL
    GROUP BY DATE(eu.request_time), eu.workspace_id, se.endpoint_name
""")
print(f"  ✅ {len(token_rows)} token-usage rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 3: billing_product_daily (all-product costs by workspace)

# COMMAND ----------

print(f"▸ Querying system.billing.usage for all-product costs ({RETENTION_DAYS} days) …")
product_rows = _execute_sql(f"""
    SELECT
        CAST(u.usage_date AS STRING)                AS usage_date,
        u.workspace_id,
        u.billing_origin_product,
        ROUND(SUM(u.usage_quantity), 4)             AS total_dbus,
        ROUND(SUM(u.usage_quantity *
            COALESCE(lp.pricing.effective_list.default, lp.pricing.default, 0)
        ), 4)                                        AS total_cost_usd
    FROM system.billing.usage u
    LEFT JOIN system.billing.list_prices lp
        ON u.sku_name = lp.sku_name
        AND u.cloud   = lp.cloud
        AND u.usage_unit = lp.usage_unit
        AND lp.price_end_time IS NULL
    WHERE u.usage_date >= current_date() - INTERVAL {RETENTION_DAYS} DAYS
      AND u.workspace_id IS NOT NULL
    GROUP BY u.usage_date, u.workspace_id, u.billing_origin_product
""")
print(f"  ✅ {len(product_rows)} product-cost rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 4: billing_user_endpoint_daily (per-user token usage by endpoint)

# COMMAND ----------

print(f"▸ Querying system.serving.endpoint_usage for per-user usage ({RETENTION_DAYS} days) …")
user_ep_rows = _execute_sql(f"""
    SELECT
        CAST(DATE(eu.request_time) AS STRING)        AS usage_date,
        eu.workspace_id,
        se.endpoint_name,
        COALESCE(eu.requester, 'unknown')             AS user_identity,
        COUNT(*)                                       AS request_count,
        COALESCE(SUM(eu.input_token_count), 0)         AS input_tokens,
        COALESCE(SUM(eu.output_token_count), 0)        AS output_tokens
    FROM system.serving.endpoint_usage eu
    JOIN system.serving.served_entities se
        ON eu.served_entity_id = se.served_entity_id
    WHERE eu.request_time >= current_timestamp() - INTERVAL {RETENTION_DAYS} DAYS
      AND eu.workspace_id IS NOT NULL
    GROUP BY DATE(eu.request_time), eu.workspace_id, se.endpoint_name, eu.requester
""")
print(f"  ✅ {len(user_ep_rows)} user-endpoint rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta (overwrite — Delta is the canonical source)

# COMMAND ----------

from decimal import Decimal


def _dec(x, places=4):
    return Decimal(str(round(float(x or 0), places)))


# Serving
if serving_rows:
    rows = [(r.get("usage_date",""), r.get("workspace_id",""), r.get("endpoint_name",""),
             r.get("sku_name","") or "",
             _dec(r.get("total_dbus"), 4), _dec(r.get("total_cost_usd"), 4), now)
            for r in serving_rows]
    spark.createDataFrame(rows, SERVING_SCHEMA).write.mode("overwrite").saveAsTable(SERVING_TABLE)
else:
    spark.createDataFrame([], SERVING_SCHEMA).write.mode("overwrite").saveAsTable(SERVING_TABLE)
print(f"✅ Wrote {len(serving_rows)} rows to {SERVING_TABLE}")

# Token
if token_rows:
    rows = [(r.get("usage_date",""), r.get("workspace_id",""), r.get("endpoint_name",""),
             int(r.get("request_count") or 0),
             int(r.get("input_tokens") or 0), int(r.get("output_tokens") or 0),
             _dec(r.get("avg_input_tokens"), 2), _dec(r.get("avg_output_tokens"), 2), now)
            for r in token_rows]
    spark.createDataFrame(rows, TOKEN_SCHEMA).write.mode("overwrite").saveAsTable(TOKEN_TABLE)
else:
    spark.createDataFrame([], TOKEN_SCHEMA).write.mode("overwrite").saveAsTable(TOKEN_TABLE)
print(f"✅ Wrote {len(token_rows)} rows to {TOKEN_TABLE}")

# Product
if product_rows:
    rows = [(r.get("usage_date",""), r.get("workspace_id",""), r.get("billing_origin_product",""),
             _dec(r.get("total_dbus"), 4), _dec(r.get("total_cost_usd"), 4), now)
            for r in product_rows]
    spark.createDataFrame(rows, PRODUCT_SCHEMA).write.mode("overwrite").saveAsTable(PRODUCT_TABLE)
else:
    spark.createDataFrame([], PRODUCT_SCHEMA).write.mode("overwrite").saveAsTable(PRODUCT_TABLE)
print(f"✅ Wrote {len(product_rows)} rows to {PRODUCT_TABLE}")

# User × Endpoint
if user_ep_rows:
    rows = [(r.get("usage_date",""), r.get("workspace_id",""), r.get("endpoint_name",""),
             r.get("user_identity","unknown") or "unknown",
             int(r.get("request_count") or 0),
             int(r.get("input_tokens") or 0), int(r.get("output_tokens") or 0), now)
            for r in user_ep_rows]
    spark.createDataFrame(rows, USER_EP_SCHEMA).write.mode("overwrite").saveAsTable(USER_EP_TABLE)
else:
    spark.createDataFrame([], USER_EP_SCHEMA).write.mode("overwrite").saveAsTable(USER_EP_TABLE)
print(f"✅ Wrote {len(user_ep_rows)} rows to {USER_EP_TABLE}")

# COMMAND ----------

result = {
    "status": "success",
    "serving_rows": len(serving_rows),
    "token_rows": len(token_rows),
    "product_rows": len(product_rows),
    "user_endpoint_rows": len(user_ep_rows),
    "retention_days": RETENTION_DAYS,
    "discovered_at": now.isoformat(),
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
