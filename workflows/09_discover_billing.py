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
USER_COST_TABLE = f"{CATALOG}.{SCHEMA}.billing_user_cost_daily"
TAG_COST_TABLE  = f"{CATALOG}.{SCHEMA}.billing_cost_by_tag"
EXT_SPEND_TABLE = f"{CATALOG}.{SCHEMA}.billing_external_model_spend"

print(f"Target tables:")
print(f"  {SERVING_TABLE}")
print(f"  {TOKEN_TABLE}")
print(f"  {PRODUCT_TABLE}")
print(f"  {USER_EP_TABLE}")
print(f"  {USER_COST_TABLE}")
print(f"  {EXT_SPEND_TABLE}")
print(f"Retention: {RETENTION_DAYS} days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schemas

# COMMAND ----------

TAG_COST_SCHEMA = StructType([
    StructField("tag_key", StringType(), False),
    StructField("tag_value", StringType(), False),
    StructField("total_cost_usd", DecimalType(18, 4), True),
    StructField("discovered_at", TimestampType(), False),
])

# External-model spend: per provider·model·endpoint $ for external LLMs routed
# through the AI Gateway (OpenAI, Microsoft Foundry, …), from the native
# system.ai_gateway.external_model_spend table — actual billed cost, not an
# estimate. Window aggregate (retention window owned here); no date grain.
EXT_SPEND_SCHEMA = StructType([
    StructField("provider", StringType(), True),
    StructField("model", StringType(), True),
    StructField("endpoint_name", StringType(), True),
    StructField("call_count", LongType(), True),
    StructField("total_cost_usd", DecimalType(18, 6), True),
    StructField("last_seen", StringType(), True),
    StructField("discovered_at", TimestampType(), False),
])

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

# Actual per-user dollar cost from system.billing.usage v2 attribution
# (identity_metadata.run_by + usage_metadata.ai_gateway.endpoint_id). Unlike
# billing_user_endpoint_daily (tokens only, cost estimated by token-share),
# this carries real cost attributed by the platform — Unity AI Gateway PPT FM.
USER_COST_SCHEMA = StructType([
    StructField("usage_date", StringType(), False),
    StructField("workspace_id", StringType(), False),
    StructField("endpoint_id", StringType(), True),
    StructField("endpoint_name", StringType(), True),
    StructField("run_by", StringType(), False),
    StructField("total_dbus", DecimalType(18, 4), True),
    StructField("total_cost_usd", DecimalType(18, 4), True),
    StructField("discovered_at", TimestampType(), False),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## SQL Helper

# COMMAND ----------

def _execute_sql(sql: str) -> List[Dict[str, Any]]:
    """Run SQL via the Statement Execution API with EXTERNAL_LINKS disposition
    so large result sets (e.g. the all-products query that returns ~800k rows)
    don't get truncated by INLINE's ~16MB response cap.

    Chunks are fetched by index from 0 to manifest.total_chunk_count - 1.
    The API does NOT chain ``next_chunk_index`` reliably for multi-chunk
    EXTERNAL_LINKS responses — we have to iterate explicitly. The initial
    response contains chunk 0's link; chunks 1..N-1 are fetched via
    ``/api/2.0/sql/statements/{sid}/result/chunks/{i}``.
    """
    if not WAREHOUSE_ID:
        print("  No warehouse ID")
        return []
    w = WorkspaceClient()
    body = {
        "warehouse_id": WAREHOUSE_ID,
        "statement": sql,
        "wait_timeout": "50s",
        "disposition": "EXTERNAL_LINKS",
        "format": "JSON_ARRAY",
    }
    try:
        resp = w.api_client.do("POST", "/api/2.0/sql/statements", body=body)
    except Exception as exc:
        raise RuntimeError(f"SQL submission failed: {exc}") from exc
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
        # Promote to a task failure. Returning [] silently here is what masked
        # the missing SELECT on system.billing.list_prices for weeks — every
        # billing_*_daily Lakebase table downstream of these queries was
        # written empty and the discovery task still reported SUCCESS.
        raise RuntimeError(
            f"SQL {status}: {err.get('error_code','')} {err.get('message','')[:500]}"
        )

    cols = [c["name"] for c in resp.get("manifest", {}).get("schema", {}).get("columns", [])]
    total_chunks = int(resp.get("manifest", {}).get("total_chunk_count") or 0)
    total_rows = int(resp.get("manifest", {}).get("total_row_count") or 0)
    print(f"  manifest: {total_rows} rows in {total_chunks} chunk(s)")

    import json as _json
    import urllib.request as _ureq
    rows: List[Dict[str, Any]] = []

    def _download_chunk_links(chunk_obj: dict, chunk_label: str):
        for link in chunk_obj.get("external_links") or []:
            url = link.get("external_link") or ""
            if not url:
                continue
            try:
                with _ureq.urlopen(url, timeout=60) as r:
                    data = _json.loads(r.read())
                for row in data:
                    rows.append(dict(zip(cols, row)))
            except Exception as exc:
                print(f"  ⚠️  {chunk_label} download failed: {exc}")

    # Chunk 0's link is in the initial result. Re-use it.
    _download_chunk_links(resp.get("result") or {}, "chunk 0")

    # Chunks 1..N-1 must be fetched explicitly by index.
    for i in range(1, total_chunks):
        try:
            chunk = w.api_client.do("GET", f"/api/2.0/sql/statements/{sid}/result/chunks/{i}")
            _download_chunk_links(chunk, f"chunk {i}")
        except Exception as exc:
            print(f"  ⚠️  chunk {i} fetch failed: {exc}")

    print(f"  collected {len(rows)}/{total_rows} rows")
    return rows

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
# MAGIC ## Query 5: billing_user_cost_daily (ACTUAL per-user $ via UAG v2 attribution)
# MAGIC
# MAGIC Uses `identity_metadata.run_by` + `usage_metadata.ai_gateway.endpoint_id`,
# MAGIC available for Pay-Per-Token Foundation Models queried via Unity AI Gateway
# MAGIC endpoints. Degrades gracefully (empty) on workspaces where these v2 fields
# MAGIC are not yet populated.

# COMMAND ----------

print(f"▸ Querying system.billing.usage for actual per-user cost ({RETENTION_DAYS} days) …")
try:
    user_cost_rows = _execute_sql(f"""
        SELECT
            CAST(u.usage_date AS STRING)                       AS usage_date,
            u.workspace_id,
            u.usage_metadata.ai_gateway.endpoint_id            AS endpoint_id,
            -- endpoint_name is NOT part of the downstream PK
            -- (usage_date, workspace_id, endpoint_id, run_by); a single
            -- endpoint_id can carry more than one name in the window (renames,
            -- null vs populated), so aggregate it rather than grouping by it —
            -- otherwise the query emits duplicate-PK rows and the Lakebase
            -- ON CONFLICT upsert fails with "cannot affect row a second time".
            MAX(u.usage_metadata.endpoint_name)                AS endpoint_name,
            COALESCE(u.identity_metadata.run_by, 'unknown')    AS run_by,
            ROUND(SUM(u.usage_quantity), 4)                    AS total_dbus,
            ROUND(SUM(u.usage_quantity *
                COALESCE(lp.pricing.effective_list.default, lp.pricing.default, 0.07)
            ), 4)                                              AS total_cost_usd
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices lp
            ON u.sku_name = lp.sku_name
            AND u.cloud   = lp.cloud
            AND u.usage_unit = lp.usage_unit
            AND lp.price_end_time IS NULL
        WHERE u.billing_origin_product = 'MODEL_SERVING'
          AND u.usage_date >= current_date() - INTERVAL {RETENTION_DAYS} DAYS
          AND u.identity_metadata.run_by IS NOT NULL
          AND u.workspace_id IS NOT NULL
        GROUP BY u.usage_date, u.workspace_id,
                 u.usage_metadata.ai_gateway.endpoint_id,
                 u.identity_metadata.run_by
    """)
    print(f"  ✅ {len(user_cost_rows)} actual per-user cost rows")
except Exception as exc:
    # v2 attribution fields (usage_metadata.ai_gateway / identity_metadata.run_by)
    # may not exist on older system.billing.usage schemas — degrade gracefully.
    user_cost_rows = []
    print(f"  ⚠️  per-user cost query unavailable (v2 attribution not present?): {exc}")

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

# User cost (actual, v2 attribution)
if user_cost_rows:
    rows = [(r.get("usage_date",""), r.get("workspace_id",""),
             r.get("endpoint_id"), r.get("endpoint_name"),
             r.get("run_by","unknown") or "unknown",
             _dec(r.get("total_dbus"), 4), _dec(r.get("total_cost_usd"), 4), now)
            for r in user_cost_rows]
    spark.createDataFrame(rows, USER_COST_SCHEMA).write.mode("overwrite").saveAsTable(USER_COST_TABLE)
else:
    spark.createDataFrame([], USER_COST_SCHEMA).write.mode("overwrite").saveAsTable(USER_COST_TABLE)
print(f"✅ Wrote {len(user_cost_rows)} rows to {USER_COST_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 6: billing_cost_by_tag (MODEL_SERVING $ attributed by custom_tag)
# MAGIC Explodes custom_tags for an allowlist of business/cost-attribution keys and
# MAGIC joins list_prices for USD. System tags (EndpointId, ServingType, …) are
# MAGIC excluded so the view is meaningful cost attribution, not tag noise.

# COMMAND ----------

print(f"▸ Querying system.billing.usage for cost-by-tag ({RETENTION_DAYS} days) …")
tag_cost_rows = _execute_sql(f"""
    WITH priced AS (
        SELECT u.custom_tags,
               u.usage_quantity * COALESCE(lp.pricing.effective_list.default, lp.pricing.default, 0) AS usd
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices lp
            ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
            AND u.usage_unit = lp.usage_unit AND lp.price_end_time IS NULL
        WHERE u.billing_origin_product = 'MODEL_SERVING'
          AND u.usage_date >= current_date() - INTERVAL {RETENTION_DAYS} DAYS
          AND u.custom_tags IS NOT NULL
    )
    SELECT lower(k)              AS tag_key,
           custom_tags[k]        AS tag_value,
           ROUND(SUM(usd), 4)    AS total_cost_usd
    FROM priced LATERAL VIEW explode(map_keys(custom_tags)) t AS k
    WHERE lower(k) IN ('project','team','environment','app','agent','owner',
                       'cost_center','business_unit','use_case','source')
      AND custom_tags[k] IS NOT NULL AND custom_tags[k] != ''
    GROUP BY lower(k), custom_tags[k]
    HAVING SUM(usd) > 0
    ORDER BY total_cost_usd DESC
""")
print(f"  ✅ {len(tag_cost_rows)} cost-by-tag rows")

if tag_cost_rows:
    rows = [(r.get("tag_key", ""), r.get("tag_value", ""), _dec(r.get("total_cost_usd"), 4), now)
            for r in tag_cost_rows]
    spark.createDataFrame(rows, TAG_COST_SCHEMA).write.mode("overwrite").saveAsTable(TAG_COST_TABLE)
else:
    spark.createDataFrame([], TAG_COST_SCHEMA).write.mode("overwrite").saveAsTable(TAG_COST_TABLE)
print(f"✅ Wrote {len(tag_cost_rows)} rows to {TAG_COST_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query 7: billing_external_model_spend (external LLM $ via AI Gateway)
# MAGIC Actual billed cost for external models (OpenAI, Microsoft Foundry, …) routed
# MAGIC through the AI Gateway, from the native `system.ai_gateway.external_model_spend`
# MAGIC table. Not an estimate — real dollars. Rolled up per provider·model·endpoint.

# COMMAND ----------

print(f"▸ Querying system.ai_gateway.external_model_spend ({RETENTION_DAYS} days) …")
try:
    ext_spend_rows = _execute_sql(f"""
        SELECT usage_metadata.provider              AS provider,
               usage_metadata.model                 AS model,
               usage_metadata.endpoint_name         AS endpoint_name,
               COUNT(*)                             AS call_count,
               ROUND(SUM(usage_quantity), 6)        AS total_cost_usd,
               CAST(MAX(usage_end_time) AS STRING)  AS last_seen
        FROM system.ai_gateway.external_model_spend
        WHERE usage_date >= current_date() - INTERVAL {RETENTION_DAYS} DAYS
        GROUP BY 1, 2, 3
        HAVING SUM(usage_quantity) > 0
        ORDER BY total_cost_usd DESC
    """)
except Exception as exc:
    # Fail-open: the table is newish (AI Gateway external-model routing) and may
    # not exist / be readable on every account. Degrade to empty rather than fail
    # the whole billing discovery run.
    print(f"  ⚠️  external_model_spend query unavailable: {exc}")
    ext_spend_rows = []
print(f"  ✅ {len(ext_spend_rows)} external-model-spend rows")

if ext_spend_rows:
    rows = [(r.get("provider"), r.get("model"), r.get("endpoint_name"),
             int(r.get("call_count") or 0), _dec(r.get("total_cost_usd"), 6),
             r.get("last_seen"), now)
            for r in ext_spend_rows]
    spark.createDataFrame(rows, EXT_SPEND_SCHEMA).write.mode("overwrite").saveAsTable(EXT_SPEND_TABLE)
else:
    spark.createDataFrame([], EXT_SPEND_SCHEMA).write.mode("overwrite").saveAsTable(EXT_SPEND_TABLE)
print(f"✅ Wrote {len(ext_spend_rows)} rows to {EXT_SPEND_TABLE}")

# COMMAND ----------

result = {
    "status": "success",
    "serving_rows": len(serving_rows),
    "token_rows": len(token_rows),
    "product_rows": len(product_rows),
    "user_endpoint_rows": len(user_ep_rows),
    "user_cost_rows": len(user_cost_rows),
    "tag_cost_rows": len(tag_cost_rows),
    "ext_spend_rows": len(ext_spend_rows),
    "retention_days": RETENTION_DAYS,
    "discovered_at": now.isoformat(),
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
