# Databricks notebook source
# MAGIC %md
# MAGIC # Knowledge Bases Discovery Job
# MAGIC
# MAGIC Discovers Vector Search endpoints/indexes and Lakebase instances,
# MAGIC then writes results to Delta tables for Lakebase sync.
# MAGIC
# MAGIC **Data flow:** REST APIs → Delta tables → Lakebase (sync task)

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType, TimestampType,
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog name")
dbutils.widgets.text("schema", "", "Schema name")
dbutils.widgets.text("warehouse_id", "", "SQL warehouse ID for billing queries")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")

if not CATALOG or not SCHEMA:
    raise ValueError(f"catalog and schema required (got {CATALOG!r}, {SCHEMA!r})")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

VS_ENDPOINTS_TABLE = f"{CATALOG}.{SCHEMA}.vector_search_endpoints"
VS_INDEXES_TABLE = f"{CATALOG}.{SCHEMA}.vector_search_indexes"
LAKEBASE_INSTANCES_TABLE = f"{CATALOG}.{SCHEMA}.lakebase_instances"
KB_BILLING_TABLE = f"{CATALOG}.{SCHEMA}.kb_billing_daily"

print(f"Target tables: {VS_ENDPOINTS_TABLE}, {VS_INDEXES_TABLE}, {LAKEBASE_INSTANCES_TABLE}, {KB_BILLING_TABLE}")
print(f"Warehouse: {WAREHOUSE_ID}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table Schemas

# COMMAND ----------

VS_ENDPOINTS_SCHEMA = StructType([
    StructField("endpoint_name", StringType(), False),
    StructField("endpoint_id", StringType(), True),
    StructField("status", StringType(), True),
    StructField("endpoint_type", StringType(), True),
    StructField("num_indexes", IntegerType(), True),
    StructField("creator", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("discovered_at", TimestampType(), False),
])

VS_INDEXES_SCHEMA = StructType([
    StructField("index_name", StringType(), False),
    StructField("endpoint_name", StringType(), False),
    StructField("index_type", StringType(), True),
    StructField("primary_key", StringType(), True),
    StructField("creator", StringType(), True),
    StructField("detailed_state", StringType(), True),
    StructField("indexed_row_count", IntegerType(), True),
    StructField("ready", BooleanType(), True),
    StructField("status_message", StringType(), True),
    StructField("source_table", StringType(), True),
    StructField("embedding_model", StringType(), True),
    StructField("pipeline_type", StringType(), True),
    StructField("discovered_at", TimestampType(), False),
])

LAKEBASE_SCHEMA = StructType([
    StructField("instance_name", StringType(), False),
    StructField("instance_id", StringType(), True),
    StructField("state", StringType(), True),
    StructField("capacity", StringType(), True),
    StructField("pg_version", StringType(), True),
    StructField("read_write_dns", StringType(), True),
    StructField("read_only_dns", StringType(), True),
    StructField("creator", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("discovered_at", TimestampType(), False),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discover Vector Search

# COMMAND ----------

w = WorkspaceClient()
now = datetime.now(timezone.utc)

# Discover endpoints
print("▸ Discovering Vector Search endpoints ...")
try:
    ep_resp = w.api_client.do("GET", "/api/2.0/vector-search/endpoints")
    vs_endpoints = ep_resp.get("endpoints", [])
    print(f"  ✅ {len(vs_endpoints)} endpoints found")
except Exception as exc:
    print(f"  ⚠️  Vector Search endpoint discovery failed: {exc}")
    vs_endpoints = []

# COMMAND ----------

# Discover indexes per endpoint (with detailed status)
print("▸ Discovering Vector Search indexes ...")
vs_indexes = []

for ep in vs_endpoints:
    ep_name = ep.get("name", "")
    if not ep_name:
        continue
    try:
        idx_resp = w.api_client.do("GET", "/api/2.0/vector-search/indexes",
                                    query={"endpoint_name": ep_name})
        for idx in idx_resp.get("vector_indexes", []):
            idx_name = idx.get("name", "")
            # Fetch detailed index status
            detailed_state = ""
            indexed_row_count = 0
            ready = False
            status_message = ""
            source_table = ""
            embedding_model = ""
            pipeline_type = ""
            try:
                if idx_name:
                    detail = w.api_client.do("GET", f"/api/2.0/vector-search/indexes/{idx_name}")
                    st = detail.get("status", {})
                    detailed_state = st.get("detailed_state", "")
                    indexed_row_count = st.get("indexed_row_count", 0) or 0
                    ready = bool(st.get("ready", False))
                    status_message = (st.get("message", "") or "")[:500]
                    ds = detail.get("delta_sync_index_spec", {})
                    source_table = ds.get("source_table", "")
                    emb_cols = ds.get("embedding_source_columns", [])
                    if emb_cols and isinstance(emb_cols, list) and len(emb_cols) > 0:
                        embedding_model = emb_cols[0].get("embedding_model_endpoint_name", "")
                    pipeline_type = ds.get("pipeline_type", "")
            except Exception as exc:
                print(f"    ⚠️  Index detail failed for {idx_name}: {exc}")

            vs_indexes.append({
                "index_name": idx_name,
                "endpoint_name": ep_name,
                "index_type": idx.get("index_type", ""),
                "primary_key": idx.get("primary_key", ""),
                "creator": idx.get("creator", ""),
                "detailed_state": detailed_state,
                "indexed_row_count": indexed_row_count,
                "ready": ready,
                "status_message": status_message,
                "source_table": source_table,
                "embedding_model": embedding_model,
                "pipeline_type": pipeline_type,
            })
    except Exception as exc:
        print(f"  ⚠️  Index discovery failed for {ep_name}: {exc}")

print(f"  ✅ {len(vs_indexes)} indexes found")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discover Lakebase Instances

# COMMAND ----------

print("▸ Discovering Lakebase instances ...")
try:
    lb_resp = w.api_client.do("GET", "/api/2.0/database/instances")
    lb_instances = lb_resp.get("database_instances", [])
    print(f"  ✅ {len(lb_instances)} instances found")
except Exception as exc:
    print(f"  ⚠️  Lakebase instance discovery failed: {exc}")
    lb_instances = []

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Tables

# COMMAND ----------

# Write VS endpoints
ep_rows = [(
    ep.get("name", ""),
    ep.get("id", ""),
    ep.get("endpoint_status", {}).get("state", "UNKNOWN"),
    ep.get("endpoint_type", ""),
    ep.get("num_indexes", 0),
    ep.get("creator", ""),
    str(ep.get("creation_timestamp", "")),
    now,
) for ep in vs_endpoints]

if ep_rows:
    ep_df = spark.createDataFrame(ep_rows, VS_ENDPOINTS_SCHEMA)
    ep_df.write.mode("overwrite").saveAsTable(VS_ENDPOINTS_TABLE)
    print(f"✅ Wrote {len(ep_rows)} endpoints to {VS_ENDPOINTS_TABLE}")
else:
    spark.createDataFrame([], VS_ENDPOINTS_SCHEMA).write.mode("overwrite").saveAsTable(VS_ENDPOINTS_TABLE)
    print(f"ℹ️  No endpoints — empty table written")

# COMMAND ----------

# Write VS indexes
idx_rows = [(
    idx["index_name"], idx["endpoint_name"], idx["index_type"],
    idx["primary_key"], idx["creator"], idx["detailed_state"],
    idx["indexed_row_count"], idx["ready"], idx["status_message"],
    idx["source_table"], idx["embedding_model"], idx["pipeline_type"],
    now,
) for idx in vs_indexes]

if idx_rows:
    idx_df = spark.createDataFrame(idx_rows, VS_INDEXES_SCHEMA)
    idx_df.write.mode("overwrite").saveAsTable(VS_INDEXES_TABLE)
    print(f"✅ Wrote {len(idx_rows)} indexes to {VS_INDEXES_TABLE}")
else:
    spark.createDataFrame([], VS_INDEXES_SCHEMA).write.mode("overwrite").saveAsTable(VS_INDEXES_TABLE)
    print(f"ℹ️  No indexes — empty table written")

# COMMAND ----------

# Write Lakebase instances
lb_rows = [(
    inst.get("name", ""),
    inst.get("uid", ""),
    inst.get("state", "UNKNOWN"),
    inst.get("capacity", ""),
    inst.get("pg_version", ""),
    inst.get("read_write_dns", ""),
    inst.get("read_only_dns", ""),
    inst.get("creator", ""),
    inst.get("creation_time", ""),
    now,
) for inst in lb_instances]

if lb_rows:
    lb_df = spark.createDataFrame(lb_rows, LAKEBASE_SCHEMA)
    lb_df.write.mode("overwrite").saveAsTable(LAKEBASE_INSTANCES_TABLE)
    print(f"✅ Wrote {len(lb_rows)} instances to {LAKEBASE_INSTANCES_TABLE}")
else:
    spark.createDataFrame([], LAKEBASE_SCHEMA).write.mode("overwrite").saveAsTable(LAKEBASE_INSTANCES_TABLE)
    print(f"ℹ️  No instances — empty table written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Query Billing Data (system.billing.usage)

# COMMAND ----------

import time

def _execute_sql(sql):
    """Execute via SQL Statements API."""
    if not WAREHOUSE_ID:
        print("  No warehouse ID — cannot query billing")
        return []
    try:
        resp = w.api_client.do("POST", "/api/2.0/sql/statements", body={
            "warehouse_id": WAREHOUSE_ID, "statement": sql,
            "wait_timeout": "50s", "disposition": "INLINE", "format": "JSON_ARRAY"})
    except Exception as exc:
        print(f"  SQL failed: {exc}")
        return []
    status = resp.get("status", {}).get("state", "")
    sid = resp.get("statement_id", "")
    if status in ("PENDING", "RUNNING") and sid:
        for _ in range(30):
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

from pyspark.sql.types import DoubleType as _DoubleType

KB_BILLING_SCHEMA = StructType([
    StructField("usage_date", StringType(), False),
    StructField("product", StringType(), False),
    StructField("workspace_id", StringType(), False),
    StructField("endpoint_name", StringType(), True),
    StructField("workload_type", StringType(), True),
    StructField("total_dbus", _DoubleType(), True),
    StructField("total_cost_usd", _DoubleType(), True),
    StructField("discovered_at", TimestampType(), False),
])

all_billing_rows = []

# Query in two batches to avoid result size limits
# Split into smaller chunks to avoid SQL Statements API result size limits
for product_filter, label, days_back in [
    ("'VECTOR_SEARCH'", "Vector Search (recent 30d)", 30),
    ("'VECTOR_SEARCH'", "Vector Search (31-90d)", "31_90"),
    ("'LAKEBASE'", "Lakebase (recent 30d)", 30),
    ("'LAKEBASE'", "Lakebase (31-90d)", "31_90"),
    ("'DATABASE'", "Database (recent 30d)", 30),
    ("'DATABASE'", "Database (31-90d)", "31_90"),
]:
    if days_back == "31_90":
        date_filter = "AND u.usage_date >= current_date() - INTERVAL 90 DAYS AND u.usage_date < current_date() - INTERVAL 30 DAYS"
    else:
        date_filter = f"AND u.usage_date >= current_date() - INTERVAL {days_back} DAYS"

    print(f"▸ Querying system.billing.usage for {label} ...")
    rows = _execute_sql(f"""
        SELECT
            CAST(u.usage_date AS STRING) AS usage_date,
            u.billing_origin_product AS product,
            CAST(u.workspace_id AS STRING) AS workspace_id,
            COALESCE(u.usage_metadata.endpoint_name, '') AS endpoint_name,
            CASE
                WHEN u.sku_name LIKE '%STORAGE%' THEN 'storage'
                WHEN u.sku_name LIKE '%INFERENCE%' OR u.sku_name LIKE '%SERVING%' THEN 'serving'
                WHEN u.sku_name LIKE '%JOBS%' THEN 'jobs'
                ELSE 'compute'
            END AS workload_type,
            ROUND(SUM(u.usage_quantity), 4) AS total_dbus,
            ROUND(SUM(u.usage_quantity *
                COALESCE(lp.pricing.effective_list.default, lp.pricing.default, 0.07)
            ), 4) AS total_cost_usd
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices lp
            ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
            AND u.usage_unit = lp.usage_unit AND lp.price_end_time IS NULL
        WHERE u.billing_origin_product IN ({product_filter})
          {date_filter}
        GROUP BY u.usage_date, u.billing_origin_product, u.workspace_id,
                 u.usage_metadata.endpoint_name, workload_type
    """)
    print(f"  ✅ {len(rows)} rows for {label}")
    all_billing_rows.extend(rows)

print(f"Total billing rows: {len(all_billing_rows)}")

# COMMAND ----------

# Write billing to Delta
if all_billing_rows:
    billing_data = [(
        r.get("usage_date", ""), r.get("product", ""), r.get("workspace_id", ""),
        r.get("endpoint_name", ""), r.get("workload_type", "other"),
        float(r.get("total_dbus") or 0), float(r.get("total_cost_usd") or 0), now,
    ) for r in all_billing_rows]
    spark.createDataFrame(billing_data, KB_BILLING_SCHEMA).write.mode("overwrite").saveAsTable(KB_BILLING_TABLE)
    print(f"✅ Wrote {len(billing_data)} billing rows to {KB_BILLING_TABLE}")
else:
    spark.createDataFrame([], KB_BILLING_SCHEMA).write.mode("overwrite").saveAsTable(KB_BILLING_TABLE)
    print("ℹ️  No billing data — empty table written")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

result = {
    "status": "success",
    "vector_search_endpoints": len(vs_endpoints),
    "vector_search_indexes": len(vs_indexes),
    "lakebase_instances": len(lb_instances),
    "kb_billing_rows": len(all_billing_rows),
    "discovered_at": now.isoformat(),
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
