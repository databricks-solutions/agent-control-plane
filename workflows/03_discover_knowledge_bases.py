# Databricks notebook source
# MAGIC %md
# MAGIC # Knowledge Bases / Retrieval & Memory Discovery Job
# MAGIC
# MAGIC Discovers Vector Search endpoints/indexes and Lakebase instances,
# MAGIC then writes results to Delta tables for Lakebase sync. Also aggregates
# MAGIC Vector Search query usage (system.access.audit), Lakebase health
# MAGIC (system.lakebase.*) and per-project Lakebase cost (system.billing.usage).
# MAGIC
# MAGIC **Data flow:** REST APIs + system tables → Delta tables → Lakebase (sync task)

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, List
from urllib.parse import quote

from databricks.sdk import WorkspaceClient
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, BooleanType, TimestampType,
    LongType, DoubleType,
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog name")
dbutils.widgets.text("schema", "", "Schema name")
dbutils.widgets.text("warehouse_id", "", "SQL warehouse ID for billing queries")
# Retrieval & Memory windows. Each is bounded so the system-table scans stay cheap:
# audit is partitioned on event_date, gauges / wait counters are scanned on ts.
dbutils.widgets.text("usage_window_days", "30", "Vector Search audit usage window (days)")
dbutils.widgets.text("lakebase_window_days", "14", "Lakebase gauge window (days)")
dbutils.widgets.text("wait_window_days", "7", "Lakebase wait-event window (days)")
dbutils.widgets.text("lakebase_cost_window_days", "90", "Lakebase per-project cost window (days)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
WAREHOUSE_ID = dbutils.widgets.get("warehouse_id")
USAGE_DAYS = int(dbutils.widgets.get("usage_window_days") or "30")
LB_HEALTH_DAYS = int(dbutils.widgets.get("lakebase_window_days") or "14")
WAIT_DAYS = int(dbutils.widgets.get("wait_window_days") or "7")
LB_COST_DAYS = int(dbutils.widgets.get("lakebase_cost_window_days") or "90")

if not CATALOG or not SCHEMA:
    raise ValueError(f"catalog and schema required (got {CATALOG!r}, {SCHEMA!r})")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

VS_ENDPOINTS_TABLE = f"{CATALOG}.{SCHEMA}.vector_search_endpoints"
VS_INDEXES_TABLE = f"{CATALOG}.{SCHEMA}.vector_search_indexes"
LAKEBASE_INSTANCES_TABLE = f"{CATALOG}.{SCHEMA}.lakebase_instances"
KB_BILLING_TABLE = f"{CATALOG}.{SCHEMA}.kb_billing_daily"
VS_USAGE_TABLE = f"{CATALOG}.{SCHEMA}.vs_index_usage_daily"
VS_CALLERS_TABLE = f"{CATALOG}.{SCHEMA}.vs_index_callers"
LB_HEALTH_TABLE = f"{CATALOG}.{SCHEMA}.lakebase_health_daily"
LB_WAIT_TABLE = f"{CATALOG}.{SCHEMA}.lakebase_wait_events"
LB_COST_PROJECT_TABLE = f"{CATALOG}.{SCHEMA}.lakebase_cost_by_project"

print(f"Target tables: {VS_ENDPOINTS_TABLE}, {VS_INDEXES_TABLE}, {LAKEBASE_INSTANCES_TABLE}, {KB_BILLING_TABLE}")
print(f"Retrieval & Memory tables: {VS_USAGE_TABLE}, {VS_CALLERS_TABLE}, {LB_HEALTH_TABLE}, {LB_WAIT_TABLE}, {LB_COST_PROJECT_TABLE}")
print(f"Warehouse: {WAREHOUSE_ID} | windows: usage={USAGE_DAYS}d lakebase={LB_HEALTH_DAYS}d wait={WAIT_DAYS}d lb_cost={LB_COST_DAYS}d")

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


def _normalize_lb_project(p: dict) -> dict:
    """Map an Autoscaling `/postgres/projects` record into the Provisioned
    instance dict shape (the keys the `lb_rows` builder reads)."""
    st = p.get("status", {}) or {}
    lim = st.get("default_endpoint_settings", {}) or {}
    min_cu = lim.get("autoscaling_limit_min_cu")
    cap = ""
    if min_cu is not None:
        # max may be absent OR present-but-null — fall back to min in both cases
        # (a bare dict default only covers the missing-key case, so `int(None)` would raise).
        max_cu = lim.get("autoscaling_limit_max_cu") or min_cu
        cap = f"CU_{int(min_cu)}-{int(max_cu)}"
    return {
        # Use the UNIQUE project_id (not the free-form display_name) — this becomes
        # lakebase_instances.instance_name, the PRIMARY KEY upserted in 02_sync, so a
        # non-unique name would silently overwrite another backend's row.
        "name": p.get("project_id") or p.get("name", "").split("/")[-1] or st.get("display_name", ""),
        "uid": p.get("uid", ""),
        "state": "AVAILABLE",  # projects API has no simple state; presence ⇒ live
        "capacity": cap,
        "pg_version": f"PG_VERSION_{st.get('pg_version')}" if st.get("pg_version") else "",
        "read_write_dns": "",  # not in the projects list response (needs a branch/endpoint call)
        "read_only_dns": "",
        "creator": st.get("owner", ""),
        "creation_time": p.get("create_time", ""),
    }


def _discover_lakebase_instances():
    """List Lakebase via BOTH the Provisioned (`/database/instances`) and the
    Autoscaling (`/postgres/projects`) APIs, merged + deduped by uid.

    Autoscaling-only workspaces expose nothing under `/database/instances`, so
    the Provisioned-only path found 0 and the smoke check then failed on an empty
    `lakebase_instances` table (gh #73). Provisioned entries are richer (DNS,
    capacity, state), so when the same backend appears under both APIs the
    Provisioned shape wins and the project is skipped as a duplicate."""
    out, seen_uids = [], set()

    # Provisioned instances (preferred — richer fields).
    try:
        resp = w.api_client.do("GET", "/api/2.0/database/instances")
        for inst in resp.get("database_instances", []):
            out.append(inst)
            if inst.get("uid"):
                seen_uids.add(inst["uid"])
        print(f"  ✅ {len(out)} Provisioned instance(s)")
    except Exception as exc:
        print(f"  ⚠️  Provisioned instance discovery failed: {exc}")

    # Autoscaling projects (normalized into the Provisioned dict shape).
    added, page = 0, None
    try:
        for _ in range(50):  # hard page cap — defends against a non-advancing cursor
            path = "/api/2.0/postgres/projects" + (f"?page_token={quote(page, safe='')}" if page else "")
            resp = w.api_client.do("GET", path)
            projects = resp.get("projects", [])
            if not projects:
                break
            for p in projects:
                if p.get("uid") in seen_uids:
                    continue  # same backend already listed as Provisioned
                try:
                    out.append(_normalize_lb_project(p))
                except Exception as exc:  # one malformed record must not drop the rest
                    print(f"    ⚠️  skipped a project record: {exc}")
                    continue
                if p.get("uid"):
                    seen_uids.add(p["uid"])
                added += 1
            nxt = resp.get("next_page_token")
            if not nxt or nxt == page:  # cursor didn't advance → stop
                break
            page = nxt
        if added:
            print(f"  ✅ {added} Autoscaling project(s)")
    except Exception as exc:
        print(f"  ⚠️  Autoscaling project discovery failed: {exc}")

    return out


lb_instances = _discover_lakebase_instances()
print(f"  ✅ {len(lb_instances)} Lakebase backend(s) total")

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

def _all_chunks(resp: dict) -> list:
    """All result rows of a SUCCEEDED INLINE statement, following next_chunk_index.

    INLINE results are paged: the response carries only chunk 0 (~43k rows here) and
    the rest must be fetched from /result/chunks/{i}. Reading only data_array
    silently truncated large results. A failed page raises (the task retries)
    rather than letting a truncated table be written."""
    result = resp.get("result", {}) or {}
    rows = list(result.get("data_array") or [])
    nxt, sid = result.get("next_chunk_index"), resp.get("statement_id")
    if nxt is not None and sid:
        client = WorkspaceClient().api_client
        while nxt is not None:
            chunk = client.do("GET", f"/api/2.0/sql/statements/{sid}/result/chunks/{nxt}")
            rows.extend(chunk.get("data_array") or [])
            nxt = chunk.get("next_chunk_index")
    expected = (resp.get("manifest") or {}).get("total_row_count")
    if expected is not None and len(rows) != int(expected):
        raise RuntimeError(f"SQL result incomplete: got {len(rows)} of {expected} rows")
    return rows


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
    return [dict(zip(cols, row)) for row in _all_chunks(resp)]

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
            -- List price = COALESCE(effective_list.default, default) with NO
            -- hardcoded fallback. This drops the fabricated $0.07/DBU that made
            -- KB billing disagree with Cost Overview / budgets for the same SKU;
            -- an unpriced SKU now yields NULL (excluded from cost) while
            -- total_dbus still counts the usage. Matches 09/13.
            ROUND(SUM(u.usage_quantity *
                COALESCE(lp.pricing.effective_list.default, lp.pricing.default)
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
# MAGIC ## Retrieval & Memory: Vector Search usage, Lakebase health and cost
# MAGIC
# MAGIC Read-only aggregates for the "Retrieval & Memory" page, all read with
# MAGIC `spark.sql` (serverless) from system tables and bounded by a window.
# MAGIC Tables written: `vs_index_usage_daily`, `vs_index_callers`,
# MAGIC `lakebase_health_daily`, `lakebase_wait_events`, `lakebase_cost_by_project`.
# MAGIC
# MAGIC Failure contract (same as `11_discover_ai_gateway_usage`): a query that
# MAGIC FAILS keeps the previous snapshot; a query that genuinely returns 0 rows
# MAGIC writes an empty table (truthful); the very first run writes empty so
# MAGIC downstream sync / smoke check still find the table.

# COMMAND ----------

VS_USAGE_SCHEMA = StructType([
    StructField("usage_date", StringType(), False),
    StructField("workspace_id", StringType(), False),
    StructField("index_name", StringType(), False),
    StructField("action", StringType(), False),
    StructField("request_count", LongType(), True),
    StructField("error_count", LongType(), True),
    StructField("distinct_callers", LongType(), True),
    StructField("discovered_at", TimestampType(), False),
])

VS_CALLERS_SCHEMA = StructType([
    StructField("workspace_id", StringType(), False),
    StructField("index_name", StringType(), False),
    StructField("caller", StringType(), False),
    StructField("request_count", LongType(), True),
    StructField("error_count", LongType(), True),
    StructField("last_seen", StringType(), True),
    StructField("discovered_at", TimestampType(), False),
])

LB_HEALTH_SCHEMA = StructType([
    StructField("usage_date", StringType(), False),
    StructField("workspace_id", StringType(), False),
    StructField("project_id", StringType(), True),
    StructField("project_name", StringType(), True),
    StructField("branch_id", StringType(), True),
    StructField("branch_name", StringType(), True),
    StructField("endpoint_id", StringType(), True),
    StructField("endpoint_name", StringType(), True),
    StructField("avg_connections", DoubleType(), True),
    StructField("max_connections", LongType(), True),
    StructField("max_db_size_bytes", LongType(), True),
    StructField("discovered_at", TimestampType(), False),
])

LB_WAIT_SCHEMA = StructType([
    StructField("workspace_id", StringType(), False),
    StructField("project_id", StringType(), True),
    StructField("project_name", StringType(), True),
    StructField("branch_name", StringType(), True),
    StructField("wait_class_name", StringType(), True),
    StructField("wait_event_name", StringType(), True),
    StructField("wait_event_count", LongType(), True),
    StructField("wait_event_time", DoubleType(), True),
    StructField("discovered_at", TimestampType(), False),
])

LB_COST_PROJECT_SCHEMA = StructType([
    StructField("window_days", IntegerType(), False),   # one of LB_COST_WINDOWS
    StructField("workspace_id", StringType(), False),
    StructField("project_id", StringType(), False),
    StructField("branch_id", StringType(), True),
    StructField("total_dbus", DoubleType(), True),
    StructField("total_cost_usd", DoubleType(), True),
    StructField("discovered_at", TimestampType(), False),
])

_KEPT_PREVIOUS: List[str] = []
# Per-index caller cap for vs_index_callers — the drilldown shows the top callers.
VS_CALLERS_PER_INDEX = 50
# Per-workspace cap for lakebase_wait_events (top by wait time).
LB_WAITS_PER_WORKSPACE = 50
# Agent Bricks / managed index traffic is audited as the same vectorSearch service.
_VS_DATA_PLANE_ACTIONS = "('queryVectorIndex', 'scanVectorIndex', 'upsertDataVectorIndex', 'deleteDataVectorIndex')"


def _spark_rows(sql: str, label: str):
    """Bounded spark.sql read. Returns dict rows, [] for a genuine empty result,
    or None when the query FAILED (the caller then keeps the previous snapshot)."""
    try:
        return [r.asDict() for r in spark.sql(sql).collect()]
    except Exception as exc:
        print(f"  ⚠️  {label} unavailable: {str(exc)[:300]}")
        return None


def _empty_or_keep(schema, table: str, failed: bool) -> None:
    """Failed query + previous snapshot → keep it (one transient error must not
    blank the page). Otherwise overwrite with an empty table (truthful, or first run)."""
    if failed and spark.catalog.tableExists(table):
        _KEPT_PREVIOUS.append(table.split(".")[-1])
        print(f"  ↺ query failed — keeping previous snapshot of {table}")
        return
    spark.createDataFrame([], schema).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)


def _write_snapshot(rows, schema, table: str, to_row) -> int:
    """Write a query result to Delta, or fall back to _empty_or_keep."""
    if rows:
        data = [to_row(r) for r in rows]
        spark.createDataFrame(data, schema).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(table)
        print(f"✅ Wrote {len(data)} rows to {table}")
        return len(data)
    _empty_or_keep(schema, table, failed=rows is None)
    if rows is not None:
        print(f"ℹ️  0 rows for {table} — empty table written")
    return 0


def _s(v) -> str:
    return "" if v is None else str(v)


def _n(v) -> int:
    return int(v) if v is not None else 0


def _f(v) -> float:
    return float(v) if v is not None else 0.0

# COMMAND ----------

# Vector Search data-plane usage per (day, workspace, index, action). System audit is
# account-wide but carries workspace_id, so the app can scope it per caller.
print(f"▸ Querying system.access.audit vectorSearch usage (last {USAGE_DAYS}d) ...")
vs_usage_raw = _spark_rows(f"""
    SELECT CAST(event_date AS STRING)                                  AS usage_date,
           COALESCE(CAST(workspace_id AS STRING), '')                  AS workspace_id,
           request_params['name']                                      AS index_name,
           action_name                                                 AS action,
           COUNT(*)                                                    AS request_count,
           SUM(CASE WHEN CAST(response.status_code AS INT) >= 400
                    THEN 1 ELSE 0 END)                                 AS error_count,
           COUNT(DISTINCT user_identity.email)                         AS distinct_callers
    FROM system.access.audit
    WHERE service_name = 'vectorSearch'
      AND action_name IN {_VS_DATA_PLANE_ACTIONS}
      AND event_date >= current_date() - INTERVAL {USAGE_DAYS} DAYS
      AND request_params['name'] IS NOT NULL
    GROUP BY 1, 2, 3, 4
""", "Vector Search audit usage")
_n_vs_usage = _write_snapshot(vs_usage_raw, VS_USAGE_SCHEMA, VS_USAGE_TABLE, lambda r: (
    _s(r["usage_date"]), _s(r["workspace_id"]), _s(r["index_name"]), _s(r["action"]),
    _n(r["request_count"]), _n(r["error_count"]), _n(r["distinct_callers"]), now,
))

# COMMAND ----------

# Top callers per index over the usage window (drilldown). Capped per index.
print(f"▸ Querying system.access.audit top callers per index (last {USAGE_DAYS}d) ...")
vs_callers_raw = _spark_rows(f"""
    WITH agg AS (
        SELECT COALESCE(CAST(workspace_id AS STRING), '')              AS workspace_id,
               request_params['name']                                  AS index_name,
               COALESCE(user_identity.email, 'unknown')                AS caller,
               COUNT(*)                                                AS request_count,
               SUM(CASE WHEN CAST(response.status_code AS INT) >= 400
                        THEN 1 ELSE 0 END)                             AS error_count,
               CAST(MAX(event_time) AS STRING)                         AS last_seen
        FROM system.access.audit
        WHERE service_name = 'vectorSearch'
          AND action_name IN {_VS_DATA_PLANE_ACTIONS}
          AND event_date >= current_date() - INTERVAL {USAGE_DAYS} DAYS
          AND request_params['name'] IS NOT NULL
        GROUP BY 1, 2, 3
    )
    SELECT workspace_id, index_name, caller, request_count, error_count, last_seen
    FROM (
        SELECT agg.*,
               ROW_NUMBER() OVER (PARTITION BY workspace_id, index_name
                                  ORDER BY request_count DESC) AS rn
        FROM agg
    ) ranked
    WHERE rn <= {VS_CALLERS_PER_INDEX}
""", "Vector Search top callers")
_n_vs_callers = _write_snapshot(vs_callers_raw, VS_CALLERS_SCHEMA, VS_CALLERS_TABLE, lambda r: (
    _s(r["workspace_id"]), _s(r["index_name"]), _s(r["caller"]),
    _n(r["request_count"]), _n(r["error_count"]), _s(r["last_seen"]), now,
))

# COMMAND ----------

# Lakebase system tables (database_gauges, wait_event_counters) leave workspace_id NULL,
# so the workspace is derived from LAKEBASE billing: each project_id maps to exactly one
# workspace there (checked on prod). Projects with no billing in 90d get '' and are
# only visible to account admins (fail closed in the app).
LB_PROJECT_WS_CTE = """
    project_ws AS (
        SELECT usage_metadata.project_id                AS project_id,
               CAST(MAX(workspace_id) AS STRING)        AS workspace_id
        FROM system.billing.usage
        WHERE billing_origin_product = 'LAKEBASE'
          AND usage_metadata.project_id IS NOT NULL
          AND usage_date >= current_date() - INTERVAL 90 DAYS
        GROUP BY usage_metadata.project_id
    )"""

# Lakebase connections + database size, per project/branch/endpoint per day.
# gauges are sampled per compute; sum across databases and computes per sample,
# then take avg/max of that per-sample total over the day.
print(f"▸ Querying system.lakebase.database_gauges health (last {LB_HEALTH_DAYS}d) ...")
lb_health_raw = _spark_rows(f"""
    WITH {LB_PROJECT_WS_CTE},
    per_sample AS (
        SELECT CAST(g.ts AS DATE)                                                       AS usage_date,
               COALESCE(NULLIF(CAST(g.workspace_id AS STRING), ''), pw.workspace_id, '') AS workspace_id,
               COALESCE(g.project_id, '')                                               AS project_id,
               COALESCE(g.project_name, '')                                             AS project_name,
               COALESCE(g.branch_id, '')                                                AS branch_id,
               COALESCE(g.branch_name, '')                                              AS branch_name,
               COALESCE(g.endpoint_id, '')                                              AS endpoint_id,
               COALESCE(g.endpoint_name, '')                                            AS endpoint_name,
               g.ts                                                                     AS ts,
               SUM(COALESCE(g.numbackends, 0))                                          AS conns,
               SUM(COALESCE(g.size, 0))                                                 AS db_bytes
        FROM system.lakebase.database_gauges g
        LEFT JOIN project_ws pw ON pw.project_id = g.project_id
        WHERE g.ts >= current_timestamp() - INTERVAL {LB_HEALTH_DAYS} DAYS
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
    )
    SELECT CAST(usage_date AS STRING)     AS usage_date,
           workspace_id, project_id, project_name, branch_id, branch_name,
           endpoint_id, endpoint_name,
           ROUND(AVG(conns), 2)           AS avg_connections,
           MAX(conns)                     AS max_connections,
           MAX(db_bytes)                  AS max_db_size_bytes
    FROM per_sample
    GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
""", "Lakebase health gauges")
_n_lb_health = _write_snapshot(lb_health_raw, LB_HEALTH_SCHEMA, LB_HEALTH_TABLE, lambda r: (
    _s(r["usage_date"]), _s(r["workspace_id"]), _s(r["project_id"]), _s(r["project_name"]),
    _s(r["branch_id"]), _s(r["branch_name"]), _s(r["endpoint_id"]), _s(r["endpoint_name"]),
    _f(r["avg_connections"]), _n(r["max_connections"]), _n(r["max_db_size_bytes"]), now,
))

# COMMAND ----------

# Lakebase wait events. wait_event_counters rows are per-interval deltas
# (previous_ts → ts), so SUM over the window is the true total. Top N per workspace.
print(f"▸ Querying system.lakebase.wait_event_counters (last {WAIT_DAYS}d) ...")
lb_wait_raw = _spark_rows(f"""
    WITH {LB_PROJECT_WS_CTE},
    agg AS (
        SELECT COALESCE(NULLIF(CAST(w.workspace_id AS STRING), ''), pw.workspace_id, '') AS workspace_id,
               COALESCE(w.project_id, '')                   AS project_id,
               COALESCE(w.project_name, '')                 AS project_name,
               COALESCE(w.branch_name, '')                  AS branch_name,
               COALESCE(w.wait_class_name, 'unknown')       AS wait_class_name,
               COALESCE(w.wait_event_name, 'unknown')       AS wait_event_name,
               SUM(COALESCE(w.wait_event_count, 0))         AS wait_event_count,
               SUM(COALESCE(w.wait_event_time, 0))          AS wait_event_time
        FROM system.lakebase.wait_event_counters w
        LEFT JOIN project_ws pw ON pw.project_id = w.project_id
        WHERE w.ts >= current_timestamp() - INTERVAL {WAIT_DAYS} DAYS
        GROUP BY 1, 2, 3, 4, 5, 6
    )
    SELECT workspace_id, project_id, project_name, branch_name, wait_class_name,
           wait_event_name, wait_event_count, wait_event_time
    FROM (
        SELECT agg.*,
               ROW_NUMBER() OVER (PARTITION BY workspace_id
                                  ORDER BY wait_event_time DESC) AS rn
        FROM agg
    ) ranked
    WHERE rn <= {LB_WAITS_PER_WORKSPACE}
""", "Lakebase wait events")
_n_lb_wait = _write_snapshot(lb_wait_raw, LB_WAIT_SCHEMA, LB_WAIT_TABLE, lambda r: (
    _s(r["workspace_id"]), _s(r["project_id"]), _s(r["project_name"]), _s(r["branch_name"]),
    _s(r["wait_class_name"]), _s(r["wait_event_name"]),
    _n(r["wait_event_count"]), _f(r["wait_event_time"]), now,
))

# COMMAND ----------

# Lakebase cost per project/branch, pre-aggregated for fixed windows. A day grain is
# far too large to mirror on big accounts (~1.08M rows / 90d on the FE account) and the
# app only ever shows a window total, so store one row per (window, workspace,
# project, branch) — ~36k rows there. Same list-price join as the KB billing block
# above. Rows without a project_id (serverless SKUs mis-tagged LAKEBASE) are excluded
# here — they stay in kb_billing_daily for the product total.
LB_COST_WINDOWS = sorted({7, 30, LB_COST_DAYS})
print(f"▸ Querying system.billing.usage LAKEBASE cost by project (windows {LB_COST_WINDOWS}d) ...")
lb_cost_raw = _spark_rows(f"""
    WITH daily AS (
        SELECT u.usage_date,
               COALESCE(CAST(u.workspace_id AS STRING), '')              AS workspace_id,
               u.usage_metadata.project_id                               AS project_id,
               COALESCE(u.usage_metadata.branch_id, '')                  AS branch_id,
               SUM(u.usage_quantity)                                     AS dbus,
               SUM(u.usage_quantity *
                   COALESCE(lp.pricing.effective_list.default, lp.pricing.default)) AS cost
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices lp
            ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
            AND u.usage_unit = lp.usage_unit AND lp.price_end_time IS NULL
        WHERE u.billing_origin_product = 'LAKEBASE'
          AND u.usage_metadata.project_id IS NOT NULL
          AND u.usage_date >= current_date() - INTERVAL {max(LB_COST_WINDOWS)} DAYS
        GROUP BY 1, 2, 3, 4
    )
    SELECT w.window_days, d.workspace_id, d.project_id, d.branch_id,
           ROUND(SUM(d.dbus), 4) AS total_dbus,
           ROUND(SUM(d.cost), 4) AS total_cost_usd
    FROM daily d
    CROSS JOIN (SELECT explode(array({", ".join(str(x) for x in LB_COST_WINDOWS)})) AS window_days) w
    WHERE d.usage_date >= current_date() - make_interval(0, 0, 0, w.window_days)
    GROUP BY 1, 2, 3, 4
""", "Lakebase cost by project")
_n_lb_cost = _write_snapshot(lb_cost_raw, LB_COST_PROJECT_SCHEMA, LB_COST_PROJECT_TABLE, lambda r: (
    int(r["window_days"]), _s(r["workspace_id"]), _s(r["project_id"]), _s(r["branch_id"]),
    _f(r["total_dbus"]), _f(r["total_cost_usd"]), now,
))

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
    "vs_index_usage_rows": _n_vs_usage,
    "vs_index_caller_rows": _n_vs_callers,
    "lakebase_health_rows": _n_lb_health,
    "lakebase_wait_event_rows": _n_lb_wait,
    "lakebase_cost_by_project_rows": _n_lb_cost,
    "kept_previous": _KEPT_PREVIOUS,
    "discovered_at": now.isoformat(),
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
