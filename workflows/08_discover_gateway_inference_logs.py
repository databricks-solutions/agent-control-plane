# Databricks notebook source
# MAGIC %md
# MAGIC # Discover AI Gateway / Inference Logs (Tier 2a)
# MAGIC
# MAGIC Pure SQL discovery: scans `system.information_schema.tables` for any
# MAGIC `*_payload` table in Unity Catalog (AI Gateway request-logging output
# MAGIC and Model Serving inference tables share this convention), then queries
# MAGIC each one over the retention window. UC governance is the only auth
# MAGIC boundary — no per-endpoint API calls or per-workspace setup.
# MAGIC
# MAGIC Two schema variants are tolerated:
# MAGIC   • newer AI Gateway: `request_time` (TIMESTAMP), `execution_duration_ms` (LONG)
# MAGIC   • legacy Model Serving: `timestamp_ms` (LONG), `execution_time_ms` (LONG)
# MAGIC
# MAGIC Writes to Delta table `gateway_inference_logs`, which the sync workflow
# MAGIC upserts into the Lakebase cache.

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Set

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType, TimestampType,
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog name (Delta target)")
dbutils.widgets.text("schema", "", "Schema name (Delta target)")
dbutils.widgets.text("retention_days", "30", "Inference-log retention window (days)")
dbutils.widgets.text("max_rows_per_table", "10000",
                     "Cap on rows pulled per source table per run")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

try:
    RETENTION_DAYS = max(1, int(dbutils.widgets.get("retention_days") or "30"))
except ValueError:
    RETENTION_DAYS = 30

try:
    MAX_ROWS_PER_TABLE = max(100, int(dbutils.widgets.get("max_rows_per_table") or "10000"))
except ValueError:
    MAX_ROWS_PER_TABLE = 10000

NOW = datetime.now(timezone.utc)
CUTOFF_TS = NOW - timedelta(days=RETENTION_DAYS)
CUTOFF_MS = int(CUTOFF_TS.timestamp() * 1000)

if not CATALOG or not SCHEMA:
    raise ValueError(f"catalog and schema required (got {CATALOG!r}, {SCHEMA!r})")

OUTPUT_TABLE = f"{CATALOG}.{SCHEMA}.gateway_inference_logs"
print(f"Output Delta: {OUTPUT_TABLE}")
print(f"Retention: {RETENTION_DAYS} days  cutoff_ts={CUTOFF_TS.isoformat()}  cutoff_ms={CUTOFF_MS}")
print(f"Cap per table: {MAX_ROWS_PER_TABLE} rows")

# COMMAND ----------

OUTPUT_SCHEMA = StructType([
    StructField("request_id",          StringType(), False),  # databricks_request_id
    StructField("source_table",        StringType(), False),  # "<catalog>.<schema>.<name>"
    StructField("client_request_id",   StringType(), True),
    StructField("request_time",        TimestampType(), True),
    StructField("status_code",         IntegerType(), True),
    StructField("execution_ms",        LongType(), True),
    StructField("request_payload",     StringType(), True),
    StructField("response_payload",    StringType(), True),
    StructField("request_size_bytes",  LongType(), True),
    StructField("response_size_bytes", LongType(), True),
    StructField("served_entity_id",    StringType(), True),
    StructField("requester",           StringType(), True),
    StructField("discovered_at",       TimestampType(), False),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discover `*_payload` tables (account-wide)

# COMMAND ----------

print("▸ Scanning system.information_schema.tables for *_payload ...")
discovered = spark.sql("""
    SELECT table_catalog, table_schema, table_name
    FROM system.information_schema.tables
    WHERE table_name LIKE '%\\_payload' ESCAPE '\\\\'
      AND table_type IN ('MANAGED', 'EXTERNAL')
    ORDER BY table_catalog, table_schema, table_name
""").collect()

candidate_tables = [(r["table_catalog"], r["table_schema"], r["table_name"]) for r in discovered]
print(f"  found {len(candidate_tables)} candidate `_payload` table(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema-aware extraction
# MAGIC
# MAGIC Column sets vary across tables. We always select the same logical fields
# MAGIC into the output but build the SELECT per-table based on what's available.

# COMMAND ----------

def get_columns(catalog: str, schema: str, table: str) -> Set[str]:
    """Return the set of column names for a given UC table (lowercased)."""
    rows = spark.sql(f"""
        SELECT column_name FROM system.information_schema.columns
        WHERE table_catalog = '{catalog}'
          AND table_schema = '{schema}'
          AND table_name = '{table}'
    """).collect()
    return {r["column_name"].lower() for r in rows}


# Required columns; if a table doesn't have these, skip it.
REQUIRED = {"databricks_request_id", "request", "response"}

per_table_stats: List[Dict[str, Any]] = []
all_rows: List[tuple] = []

for cat, sch, tbl in candidate_tables:
    full = f"{cat}.{sch}.{tbl}"
    try:
        cols = get_columns(cat, sch, tbl)
    except Exception as exc:
        per_table_stats.append({"table": full, "error": f"col-list: {exc}"[:200]})
        print(f"  ⚠️  {full}: cannot list columns ({exc})")
        continue

    if not REQUIRED.issubset(cols):
        per_table_stats.append({"table": full, "skipped": "missing required cols",
                                "cols_present": sorted(cols)})
        continue

    # Build a tolerant SELECT. CAST timestamps via the column that exists.
    if "request_time" in cols:
        ts_expr = "request_time"
    elif "timestamp_ms" in cols:
        ts_expr = "TIMESTAMP_MILLIS(timestamp_ms)"
    else:
        ts_expr = "CAST(NULL AS TIMESTAMP)"

    if "execution_duration_ms" in cols:
        exec_expr = "execution_duration_ms"
    elif "execution_time_ms" in cols:
        exec_expr = "execution_time_ms"
    else:
        exec_expr = "CAST(NULL AS BIGINT)"

    served_expr = "served_entity_id" if "served_entity_id" in cols else "CAST(NULL AS STRING)"
    requester_expr = "requester" if "requester" in cols else "CAST(NULL AS STRING)"
    client_rid_expr = "client_request_id" if "client_request_id" in cols else "CAST(NULL AS STRING)"
    status_expr = "status_code" if "status_code" in cols else "CAST(NULL AS INT)"

    where_clause = f"{ts_expr} >= TIMESTAMP '{CUTOFF_TS.strftime('%Y-%m-%d %H:%M:%S')}'"

    # Backtick-quote each identifier so tables with hyphens or leading digits
    # (e.g. `mlops-serving-endpoint_payload`, `databricks-claude-sonnet-4-5_payload`)
    # parse correctly.
    quoted = f"`{cat}`.`{sch}`.`{tbl}`"

    query = f"""
        SELECT
          databricks_request_id      AS request_id,
          {client_rid_expr}          AS client_request_id,
          {ts_expr}                  AS request_time,
          {status_expr}              AS status_code,
          {exec_expr}                AS execution_ms,
          request                    AS request_payload,
          response                   AS response_payload,
          {served_expr}              AS served_entity_id,
          {requester_expr}           AS requester
        FROM {quoted}
        WHERE {where_clause}
        ORDER BY request_time DESC
        LIMIT {MAX_ROWS_PER_TABLE}
    """
    try:
        rows = spark.sql(query).collect()
    except Exception as exc:
        msg = str(exc)
        # Permission denials are expected for catalogs we don't have grants on;
        # surface them as `skipped` rather than `error` to keep the run output clean.
        # The Spark exception class can be either AnalysisException or
        # UnauthorizedAccessException depending on where the check fires.
        if any(s in msg for s in (
            "INSUFFICIENT_PERMISSIONS",
            "PERMISSION_DENIED",
            "UnauthorizedAccessException",
            "USE CATALOG",
            "USE SCHEMA",
        )):
            per_table_stats.append({"table": full, "skipped": "no UC grants"})
        else:
            per_table_stats.append({"table": full, "error": msg[:200]})
            print(f"  ⚠️  {full}: select failed: {msg[:120]}")
        continue

    n = 0
    for r in rows:
        rid = r["request_id"]
        if not rid:
            continue
        req_str = r["request_payload"] or ""
        resp_str = r["response_payload"] or ""
        all_rows.append((
            rid,
            full,
            r["client_request_id"],
            r["request_time"],
            int(r["status_code"]) if r["status_code"] is not None else None,
            int(r["execution_ms"]) if r["execution_ms"] is not None else None,
            req_str,
            resp_str,
            len(req_str.encode("utf-8")),
            len(resp_str.encode("utf-8")),
            r["served_entity_id"],
            r["requester"],
            NOW,
        ))
        n += 1
    per_table_stats.append({"table": full, "rows": n})
    print(f"  ✅ {full}: {n} row(s)")

print(f"\n▸ Total: {len(all_rows)} inference-log rows from {len([s for s in per_table_stats if s.get('rows', 0) > 0])} non-empty table(s)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Delta snapshot

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

if all_rows:
    df = spark.createDataFrame(all_rows, OUTPUT_SCHEMA)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(OUTPUT_TABLE)
    n = spark.read.table(OUTPUT_TABLE).count()
    print(f"✅ Wrote {n} rows to {OUTPUT_TABLE}")
else:
    spark.createDataFrame([], OUTPUT_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(OUTPUT_TABLE)
    print(f"ℹ️  No rows — empty {OUTPUT_TABLE}")

# COMMAND ----------

result = {
    "status": "success",
    "candidate_tables_found": len(candidate_tables),
    "tables_with_data": len([s for s in per_table_stats if s.get("rows", 0) > 0]),
    "total_rows": len(all_rows),
    "retention_days": RETENTION_DAYS,
    "max_rows_per_table": MAX_ROWS_PER_TABLE,
    "per_table_sample": per_table_stats[:30],
    "discovered_at": NOW.isoformat(),
}
print(json.dumps(result, indent=2)[:3000])
dbutils.notebook.exit(json.dumps(result))
