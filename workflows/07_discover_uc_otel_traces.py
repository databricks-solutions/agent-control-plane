# Databricks notebook source
# MAGIC %md
# MAGIC # Discover UC-stored MLflow Traces (Tier 2b)
# MAGIC
# MAGIC Discovery routes by table-name suffix over `system.information_schema.tables`.
# MAGIC The two MLflow trace storage formats produce distinctive table-name
# MAGIC patterns (`*_otel_spans` and `trace_logs_*`), so suffix routing is
# MAGIC reliable. Pure schema-based scanning over `information_schema.columns`
# MAGIC is exhaustive in theory but too slow at metastore scale, and per-table
# MAGIC `DESCRIBE` calls also blow up the run time. The data-query paths are
# MAGIC tolerant of column variations, so a name-matching table with an unexpected
# MAGIC schema is reported as an error and skipped, not a crash.
# MAGIC
# MAGIC Two shapes recognized today:
# MAGIC
# MAGIC   - **OTel spans (row per span)** — `<prefix>_otel_spans` Delta tables
# MAGIC     produced by MLflow 3.11+ with `trace_location=UnityCatalog(...)`.
# MAGIC     Columns include `trace_id`, `span_id`, `start_time_unix_nano`.
# MAGIC
# MAGIC   - **Row per trace** — `trace_logs_<experiment_id>` Delta tables
# MAGIC     produced by Databricks-native MLflow UC trace storage. Columns
# MAGIC     include `trace_id`, `spans` (ARRAY), `request_time`, `request`,
# MAGIC     `response`, `assessments`, etc.
# MAGIC
# MAGIC Adding a new format = add one name pattern + extend the shape verifier.
# MAGIC UC governance is the only auth boundary — no per-workspace API calls,
# MAGIC no SP-on-each-workspace setup. Writes to Delta tables
# MAGIC `observability_traces_uc_otel` (summary) and
# MAGIC `observability_trace_details_uc_otel` (full span data) which the sync
# MAGIC workflow upserts into the shared Lakebase cache.

# COMMAND ----------

# MAGIC %pip install databricks-sdk --upgrade --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, TimestampType,
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog name (Delta target)")
dbutils.widgets.text("schema", "", "Schema name (Delta target)")
dbutils.widgets.text("trace_retention_days", "90", "Trace retention window (days)")
dbutils.widgets.text("max_traces_per_table", "1000",
                     "Cap on traces pulled per source table per run (prevents runaway).")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")

try:
    RETENTION_DAYS = max(1, int(dbutils.widgets.get("trace_retention_days") or "90"))
except ValueError:
    RETENTION_DAYS = 90

try:
    MAX_TRACES_PER_TABLE = max(10, int(dbutils.widgets.get("max_traces_per_table") or "1000"))
except ValueError:
    MAX_TRACES_PER_TABLE = 1000

NOW = datetime.now(timezone.utc)
RETENTION_CUTOFF_NS = int((NOW - timedelta(days=RETENTION_DAYS)).timestamp() * 1_000_000_000)
RETENTION_CUTOFF_TS = (NOW - timedelta(days=RETENTION_DAYS))

if not CATALOG or not SCHEMA:
    raise ValueError(f"catalog and schema required (got {CATALOG!r}, {SCHEMA!r})")

TRACES_TABLE = f"{CATALOG}.{SCHEMA}.observability_traces_uc_otel"
DETAILS_TABLE = f"{CATALOG}.{SCHEMA}.observability_trace_details_uc_otel"

print(f"Trace summary target: {TRACES_TABLE}")
print(f"Trace detail target:  {DETAILS_TABLE}")
print(f"Retention: {RETENTION_DAYS} days")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta schemas (shared with REST-discovered defaults)

# COMMAND ----------

TRACES_SCHEMA = StructType([
    StructField("request_id", StringType(), False),
    StructField("workspace_id", StringType(), False),
    StructField("experiment_id", StringType(), True),
    StructField("trace_name", StringType(), True),
    StructField("state", StringType(), True),
    StructField("request_time", StringType(), True),
    StructField("execution_duration", LongType(), True),
    StructField("model_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("trace_user", StringType(), True),
    StructField("source", StringType(), True),
    StructField("tags", StringType(), True),
    StructField("data_source", StringType(), True),
    StructField("discovered_at", TimestampType(), False),
])

DETAILS_SCHEMA = StructType([
    StructField("workspace_id",  StringType(), False),
    StructField("request_id",    StringType(), False),
    StructField("experiment_id", StringType(), True),
    StructField("trace_info",    StringType(), True),
    StructField("trace_data",    StringType(), True),
    StructField("request_raw",   StringType(), True),
    StructField("response_raw",  StringType(), True),
    StructField("size_bytes",    LongType(),   True),
    StructField("source_type",   StringType(), True),
    StructField("cached_at",     TimestampType(), False),
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Schema-based discovery

# COMMAND ----------

# Discovery: name patterns over `system.information_schema.tables`.
# The two patterns are distinctive enough that we can route to the right
# normalizer based on suffix alone — no per-table column lookup needed.
# The data queries themselves are tolerant of column variations (try-fallback
# in the trace_logs path), so a table that matches the name pattern but has
# an unexpected schema will be reported as an error and skipped, not crash
# the run.

print("▸ Scanning system.information_schema.tables for trace-shaped tables ...")

candidate_rows = spark.sql("""
    SELECT table_catalog, table_schema, table_name
    FROM system.information_schema.tables
    WHERE (table_name LIKE '%\\_otel\\_spans' ESCAPE '\\\\'
        OR table_name LIKE 'trace\\_logs\\_%' ESCAPE '\\\\')
      AND table_type IN ('MANAGED', 'EXTERNAL')
    ORDER BY table_catalog, table_schema, table_name
""").collect()

print(f"  candidate tables (by name): {len(candidate_rows)}")

otel_tables: List[Tuple[str, str, str]] = []
trace_log_tables: List[Tuple[str, str, str]] = []

for r in candidate_rows:
    cat, sch, tbl = r["table_catalog"], r["table_schema"], r["table_name"]
    if tbl.endswith("_otel_spans"):
        otel_tables.append((cat, sch, tbl))
    elif tbl.startswith("trace_logs_"):
        trace_log_tables.append((cat, sch, tbl))

print(f"  ✅ OTel-spans-shape tables:    {len(otel_tables)}")
for t in otel_tables: print(f"     - {t[0]}.{t[1]}.{t[2]}")
print(f"  ✅ row-per-trace-shape tables: {len(trace_log_tables)}")
for t in trace_log_tables: print(f"     - {t[0]}.{t[1]}.{t[2]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process OTel-shape tables (row per span → group into traces)

# COMMAND ----------

all_trace_rows: List[tuple] = []
all_detail_rows: List[tuple] = []
per_table_stats: List[Dict[str, Any]] = []


def _classify_perm_error(msg: str) -> bool:
    return any(s in msg for s in (
        "INSUFFICIENT_PERMISSIONS",
        "PERMISSION_DENIED",
        "UnauthorizedAccessException",
        "USE CATALOG",
        "USE SCHEMA",
    ))


for cat, sch, tbl in otel_tables:
    full = f"`{cat}`.`{sch}`.`{tbl}`"
    label = f"{cat}.{sch}.{tbl}"
    prefix = tbl[:-len("_otel_spans")] if tbl.endswith("_otel_spans") else tbl

    try:
        traces_df = spark.sql(f"""
            WITH recent_trace_ids AS (
              SELECT trace_id, MAX(start_time_unix_nano) AS latest_ns
              FROM {full}
              WHERE start_time_unix_nano >= {RETENTION_CUTOFF_NS}
              GROUP BY trace_id
              ORDER BY latest_ns DESC
              LIMIT {MAX_TRACES_PER_TABLE}
            ),
            spans AS (
              SELECT s.trace_id, s.span_id, s.parent_span_id, s.name,
                     s.start_time_unix_nano, s.end_time_unix_nano,
                     CAST(s.attributes AS STRING) AS attributes_json,
                     CAST(s.status AS STRING)     AS status_json,
                     s.kind
              FROM {full} s
              JOIN recent_trace_ids r USING (trace_id)
              WHERE s.start_time_unix_nano >= {RETENTION_CUTOFF_NS}
            ),
            roots AS (
              SELECT trace_id, name AS root_name,
                     start_time_unix_nano AS root_start_ns,
                     end_time_unix_nano AS root_end_ns,
                     attributes_json AS root_attrs,
                     status_json AS root_status
              FROM spans
              WHERE parent_span_id IS NULL OR parent_span_id = ''
            ),
            agg AS (
              SELECT trace_id,
                     COLLECT_LIST(NAMED_STRUCT(
                       'span_id', span_id, 'parent_span_id', parent_span_id,
                       'name', name, 'kind', kind,
                       'start_time_unix_nano', start_time_unix_nano,
                       'end_time_unix_nano', end_time_unix_nano,
                       'attributes', attributes_json, 'status', status_json
                     )) AS span_list,
                     COUNT(*) AS span_count
              FROM spans GROUP BY trace_id
            )
            SELECT roots.trace_id, roots.root_name, roots.root_start_ns,
                   roots.root_end_ns, roots.root_attrs, roots.root_status,
                   agg.span_list, agg.span_count
            FROM roots JOIN agg USING (trace_id)
        """).collect()
    except Exception as exc:
        if _classify_perm_error(str(exc)):
            per_table_stats.append({"table": label, "shape": "otel_spans", "skipped": "no UC grants"})
        else:
            per_table_stats.append({"table": label, "shape": "otel_spans", "error": str(exc)[:200]})
            print(f"  ⚠️  {label}: {str(exc)[:100]}")
        continue

    n = 0
    for row in traces_df:
        trace_id = row["trace_id"]
        request_id = f"trace:/{cat}.{sch}.{prefix}/{trace_id}"
        root_attrs = {}
        try:
            root_attrs = json.loads(row["root_attrs"]) if row["root_attrs"] else {}
        except Exception:
            pass
        request_raw = json.dumps(root_attrs.get("mlflow.spanInputs")) if "mlflow.spanInputs" in root_attrs else ""
        response_raw = json.dumps(root_attrs.get("mlflow.spanOutputs")) if "mlflow.spanOutputs" in root_attrs else ""
        state = "UNKNOWN"
        try:
            status_obj = json.loads(row["root_status"]) if row["root_status"] else {}
            code = status_obj.get("status_code") or status_obj.get("code") or ""
            if code:
                state = "OK" if str(code).upper() in ("OK","STATUS_CODE_OK","1") else \
                        "ERROR" if str(code).upper() in ("ERROR","STATUS_CODE_ERROR","2") else str(code)
        except Exception:
            pass
        start_ms = int(row["root_start_ns"]) // 1_000_000 if row["root_start_ns"] else 0
        end_ms   = int(row["root_end_ns"]) // 1_000_000 if row["root_end_ns"] else 0
        duration_ms = max(0, end_ms - start_ms) if (start_ms and end_ms) else None

        all_trace_rows.append((
            request_id, "", "", row["root_name"] or "", state,
            str(start_ms) if start_ms else "", duration_ms,
            "", "", "", "",
            json.dumps({"otel_table": label, "trace_id": trace_id}),
            "uc_otel", NOW,
        ))

        spans_list = []
        for s in (row["span_list"] or []):
            d = s.asDict() if hasattr(s, "asDict") else dict(s)
            for k in ("start_time_unix_nano", "end_time_unix_nano"):
                if k in d and d[k] is not None:
                    d[k] = int(d[k])
            spans_list.append(d)
        td = json.dumps({"spans": spans_list, "trace_id": trace_id, "trace_uri": request_id, "otel_table": label})
        ti = json.dumps({"trace_id": trace_id, "name": row["root_name"] or "",
                         "start_time_unix_nano": int(row["root_start_ns"]) if row["root_start_ns"] else 0,
                         "end_time_unix_nano": int(row["root_end_ns"]) if row["root_end_ns"] else 0,
                         "span_count": int(row["span_count"]), "state": state})
        all_detail_rows.append((
            "", request_id, "", ti, td, request_raw, response_raw,
            len(ti) + len(td), "uc_otel", NOW,
        ))
        n += 1

    print(f"  ✅ otel_spans  {label}: {n} trace(s)")
    per_table_stats.append({"table": label, "shape": "otel_spans", "traces": n})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Process row-per-trace tables (`trace_logs_*` shape)

# COMMAND ----------

for cat, sch, tbl in trace_log_tables:
    full = f"`{cat}`.`{sch}`.`{tbl}`"
    label = f"{cat}.{sch}.{tbl}"

    try:
        # Schema reference (from inspection): trace_id, request_time (TIMESTAMP),
        # state, execution_duration_ms (LONG), request, response, request_preview,
        # response_preview, trace_metadata (MAP), tags (MAP), trace_location (STRUCT),
        # assessments (ARRAY), spans (ARRAY).
        # Some columns may be missing on older variants; we use try/except pattern
        # via tolerant column projection.
        rows = spark.sql(f"""
            SELECT trace_id,
                   client_request_id,
                   request_time,
                   state,
                   execution_duration_ms,
                   request,
                   response,
                   request_preview,
                   response_preview,
                   trace_metadata,
                   tags,
                   to_json(trace_location) AS trace_location_json,
                   to_json(assessments)    AS assessments_json,
                   to_json(spans)          AS spans_json
            FROM {full}
            WHERE request_time >= TIMESTAMP '{RETENTION_CUTOFF_TS.strftime('%Y-%m-%d %H:%M:%S')}'
            ORDER BY request_time DESC
            LIMIT {MAX_TRACES_PER_TABLE}
        """).collect()
    except Exception as exc:
        msg = str(exc)
        if _classify_perm_error(msg):
            per_table_stats.append({"table": label, "shape": "trace_logs", "skipped": "no UC grants"})
            continue
        # If columns differ, retry with a minimal projection
        try:
            rows = spark.sql(f"""
                SELECT trace_id, request_time, state, execution_duration_ms,
                       request, response,
                       to_json(spans) AS spans_json
                FROM {full}
                WHERE request_time >= TIMESTAMP '{RETENTION_CUTOFF_TS.strftime('%Y-%m-%d %H:%M:%S')}'
                ORDER BY request_time DESC
                LIMIT {MAX_TRACES_PER_TABLE}
            """).collect()
            # Pad missing columns with None for downstream code
            class _Row:
                def __init__(self, src):
                    self._d = {**{k: None for k in (
                        'client_request_id','request_preview','response_preview',
                        'trace_metadata','tags','trace_location_json','assessments_json'
                    )}, **{k: src[k] for k in src.asDict()}}
                def __getitem__(self, k): return self._d.get(k)
                def asDict(self): return self._d
            rows = [_Row(r) for r in rows]
        except Exception as exc2:
            per_table_stats.append({"table": label, "shape": "trace_logs", "error": str(exc2)[:200]})
            print(f"  ⚠️  {label}: {str(exc2)[:100]}")
            continue

    n = 0
    for row in rows:
        trace_id = row["trace_id"]
        if not trace_id:
            continue
        # Use the table-qualified URI as the cache key — globally unique
        request_id = f"trace:/{cat}.{sch}.{tbl}/{trace_id}"

        rt = row["request_time"]
        start_ms = int(rt.timestamp() * 1000) if rt else 0
        duration_ms = int(row["execution_duration_ms"]) if row["execution_duration_ms"] is not None else None

        # Best-effort root span name + span count from spans JSON
        root_name = ""
        n_spans = 0
        spans_str = row["spans_json"] or ""
        try:
            spans_arr = json.loads(spans_str) if spans_str else []
            if isinstance(spans_arr, list):
                n_spans = len(spans_arr)
                if spans_arr:
                    root = next((s for s in spans_arr if not s.get("parent_id") and not s.get("parent_span_id")), spans_arr[0])
                    root_name = root.get("name", "") or ""
        except Exception:
            pass

        state = (row["state"] or "UNKNOWN").upper() if isinstance(row["state"], str) else "UNKNOWN"

        # MAP columns come back from collect() as Python dicts. Use them as-is
        # for the parser (which expects `trace_info.trace_metadata` and `tags`
        # to be dicts), and prefer the `mlflow.traceName` tag for the display
        # name when present.
        meta_dict = dict(row["trace_metadata"] or {})
        tags_dict = dict(row["tags"] or {})
        if not root_name and tags_dict.get("mlflow.traceName"):
            root_name = tags_dict["mlflow.traceName"]

        # Synthesize sizeStats so the deep-dive "Spans" tile populates. If the
        # producer already wrote one, leave it alone.
        if "mlflow.trace.sizeStats" not in meta_dict:
            meta_dict["mlflow.trace.sizeStats"] = json.dumps({
                "num_spans": n_spans,
                "total_size_bytes": len(spans_str),
            })

        all_trace_rows.append((
            request_id, "", "", root_name, state,
            str(start_ms) if start_ms else "", duration_ms,
            meta_dict.get("mlflow.modelId", ""),
            meta_dict.get("mlflow.trace.session", ""),
            meta_dict.get("mlflow.user", ""),
            meta_dict.get("mlflow.source.name", ""),
            json.dumps({"trace_log_table": label, "trace_id": trace_id, **{k: v for k, v in tags_dict.items() if isinstance(v, str)}}),
            "uc_trace_logs", NOW,
        ))

        request_raw = row["request"] or ""
        response_raw = row["response"] or ""

        # Populate the standard MLflow trace_info shape so the existing parser
        # surfaces token usage / model id / span count / state without UC-specific
        # branching. Fields the parser reads:
        #   trace_metadata.mlflow.trace.tokenUsage   → token_usage tile
        #   trace_metadata.mlflow.trace.sizeStats    → spans tile (num_spans)
        #   trace_metadata.mlflow.modelId            → model tile
        #   tags.mlflow.traceName                    → header title
        ti = json.dumps({
            "trace_id": trace_id,
            "name": root_name,
            "request_time": str(start_ms) if start_ms else "",
            "request_time_ms": start_ms,
            "execution_duration": duration_ms,
            "duration_ms": duration_ms,
            "state": state,
            "client_request_id": row["client_request_id"] or "",
            "request_preview": row["request_preview"] or "",
            "response_preview": row["response_preview"] or "",
            "request": request_raw,
            "response": response_raw,
            "trace_metadata": meta_dict,
            "tags": tags_dict,
        })

        td_payload = {
            "trace_id": trace_id,
            "trace_uri": request_id,
            "trace_logs_table": label,
            "spans_json": spans_str,
            "trace_metadata": meta_dict,
            "tags": tags_dict,
            "trace_location_json": row["trace_location_json"] or "",
            "assessments_json": row["assessments_json"] or "",
        }
        td = json.dumps(td_payload)
        all_detail_rows.append((
            "", request_id, "", ti, td, request_raw, response_raw,
            len(ti) + len(td), "uc_trace_logs", NOW,
        ))
        n += 1

    print(f"  ✅ trace_logs  {label}: {n} trace(s)")
    per_table_stats.append({"table": label, "shape": "trace_logs", "traces": n})

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write Delta snapshots

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

if all_trace_rows:
    df = spark.createDataFrame(all_trace_rows, TRACES_SCHEMA)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TRACES_TABLE)
    n = spark.read.table(TRACES_TABLE).count()
    print(f"✅ Wrote {n} trace summaries to {TRACES_TABLE}")
else:
    spark.createDataFrame([], TRACES_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TRACES_TABLE)
    print(f"ℹ️  No UC traces — empty {TRACES_TABLE}")

if all_detail_rows:
    df = spark.createDataFrame(all_detail_rows, DETAILS_SCHEMA)
    df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(DETAILS_TABLE)
    n = spark.read.table(DETAILS_TABLE).count()
    print(f"✅ Wrote {n} trace details to {DETAILS_TABLE}")
else:
    spark.createDataFrame([], DETAILS_SCHEMA).write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(DETAILS_TABLE)
    print(f"ℹ️  No UC trace details — empty {DETAILS_TABLE}")

# COMMAND ----------

result = {
    "status": "success",
    "otel_tables_found": len(otel_tables),
    "trace_log_tables_found": len(trace_log_tables),
    "traces": len(all_trace_rows),
    "details": len(all_detail_rows),
    "retention_days": RETENTION_DAYS,
    "per_table": per_table_stats[:40],
    "discovered_at": NOW.isoformat(),
}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
