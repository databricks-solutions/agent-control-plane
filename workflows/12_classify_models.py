# Databricks notebook source
# MAGIC %md
# MAGIC # Model Classification Producer (serving Stage 3)
# MAGIC
# MAGIC Resolves the dormant `unknown` tail of agent discovery — empty-task
# MAGIC `CUSTOM_MODEL` serving endpoints that Stage 1 (task) and Stage 2 (behavior)
# MAGIC could not classify — by reading each model's **MLflow artifact** (flavor +
# MAGIC pyfunc loader + signature). This is the authoritative signal, but it is
# MAGIC UC-grant gated and slow, so it runs as a **decoupled side-channel**:
# MAGIC
# MAGIC   this job  →  `model_classification` (Delta)  ←  01_discover_agents LEFT-JOINs
# MAGIC
# MAGIC `01_discover_agents` never reads model artifacts and never waits on this job;
# MAGIC it upgrades `unknown` rows from whatever this table already holds (fail-open).
# MAGIC Versions are immutable, so each (model, version) is probed once and cached —
# MAGIC including the negative result (`forbidden`/`error`), so reruns don't re-probe.
# MAGIC
# MAGIC **Table written:** `model_classification`

# COMMAND ----------

# MAGIC %pip install mlflow --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
from datetime import datetime, timezone

import mlflow
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

spark = SparkSession.builder.getOrCreate()
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog name")
dbutils.widgets.text("schema", "", "Schema name")
dbutils.widgets.text("max_models", "500", "Max new models to probe per run")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
MAX_MODELS = int(dbutils.widgets.get("max_models") or "500")

if not CATALOG or not SCHEMA:
    raise ValueError(f"catalog and schema required (got {CATALOG!r}, {SCHEMA!r})")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
TABLE = f"{CATALOG}.{SCHEMA}.model_classification"
print(f"Target table: {TABLE} | max_models/run {MAX_MODELS}")

# COMMAND ----------

SCHEMA_DEF = StructType([
    StructField("model_full_name", StringType(), False),
    StructField("model_version", StringType(), False),
    StructField("workload_class", StringType(), True),   # traditional_ml | genai_agent | llm | multimodal | unknown
    StructField("framework", StringType(), True),
    StructField("loader_module", StringType(), True),
    StructField("signature_kind", StringType(), True),   # tabular | messages | text | null
    StructField("confidence", StringType(), True),
    StructField("source_signal", StringType(), True),    # flavor | loader | signature
    StructField("probe_status", StringType(), True),     # resolved | forbidden | error
    StructField("probed_at", TimestampType(), False),
])

_ML_FLAVORS = {"sklearn", "xgboost", "lightgbm", "catboost", "spark", "onnx",
               "tensorflow", "pytorch", "keras", "statsmodels", "prophet", "h2o", "fastai"}
_AGENT_FLAVORS = {"langchain", "langgraph", "llama_index", "openai", "crewai", "dspy"}
_AGENT_LOADER_HINTS = ("responses_agent", "chat_agent", "chat_model", "langchain",
                       "langgraph", "agent", "openai")


def _signature_kind(s):
    """Coarse input-signature kind: messages | image | audio | text | tabular | None."""
    s = (s or "").lower()
    if "messages" in s or "conversation_id" in s or "custom_inputs" in s:
        return "messages"
    if any(k in s for k in ("image", "pixel", "vision")) or "binary" in s:
        return "image"
    if "audio" in s or "speech" in s:
        return "audio"
    if "string" in s and "[" in s:
        return "text"
    return "tabular" if s else None


def _classify(flavors, loader, sig_inputs):
    """Return (workload_class, framework, signature_kind, confidence, source_signal).

    Classes: genai_agent | llm | multimodal | traditional_ml | unknown.
    `llm` = text-generative model (not an agent); `multimodal` = vision/audio model.
    """
    fl = set(flavors or {})
    sig_kind = _signature_kind(sig_inputs)

    agent_fl = fl & _AGENT_FLAVORS
    if agent_fl:
        return ("genai_agent", sorted(agent_fl)[0], sig_kind, "high", "flavor")

    # Vision/audio signature → multimodal, regardless of producing flavor.
    if sig_kind in ("image", "audio"):
        fw = "transformers" if "transformers" in fl else (sorted(fl & _ML_FLAVORS)[0]
                                                          if (fl & _ML_FLAVORS) else "pyfunc")
        return ("multimodal", fw, sig_kind, "high" if fl else "med", "signature")

    ml_fl = fl & _ML_FLAVORS
    if ml_fl:
        return ("traditional_ml", sorted(ml_fl)[0], sig_kind, "high", "flavor")

    loader_l = (loader or "").lower()
    if any(h in loader_l for h in _AGENT_LOADER_HINTS):
        return ("genai_agent", "pyfunc:" + loader_l.split(".")[-1], sig_kind, "high", "loader")

    if "transformers" in fl:
        # text/chat → llm; anything else non-text already handled above → multimodal fallback
        return (("llm" if sig_kind in ("messages", "text") else "multimodal"),
                "transformers", sig_kind, "med", "signature")
    if sig_kind in ("messages", "text"):
        # generative text I/O without an agent flavor/loader: LLM, agent-ness unconfirmed.
        return ("llm", "pyfunc", sig_kind, "med", "signature")
    if sig_kind == "tabular":
        return ("traditional_ml", "pyfunc", sig_kind, "med", "signature")
    return ("unknown", None, sig_kind, "low", "indeterminate")

# COMMAND ----------

# Candidate models: empty-task CUSTOM_MODEL serving endpoints (the dormant `unknown`
# bucket). One row per (model, version).
candidates = spark.sql("""
    WITH latest AS (
        SELECT *, ROW_NUMBER() OVER (
            PARTITION BY workspace_id, endpoint_name
            ORDER BY CAST(endpoint_config_version AS INT) DESC) rn
        FROM system.serving.served_entities
        WHERE endpoint_delete_time IS NULL AND entity_type = 'CUSTOM_MODEL'
    )
    SELECT DISTINCT entity_name AS model_full_name, entity_version AS model_version
    FROM latest
    WHERE rn = 1 AND (task IS NULL OR task = '')
      AND entity_name IS NOT NULL AND entity_version IS NOT NULL
""").collect()
candidates = [(r.model_full_name, str(r.model_version)) for r in candidates]
print(f"Candidate empty-task custom models: {len(candidates)}")

# Cache: skip (model, version) already probed (immutable). Keep their rows.
cached_rows = []
done = set()
if spark.catalog.tableExists(TABLE):
    for r in spark.table(TABLE).collect():
        cached_rows.append(tuple(r))
        done.add((r["model_full_name"], r["model_version"]))
    print(f"Already cached: {len(done)}")

todo = [(m, v) for (m, v) in candidates if (m, v) not in done][:MAX_MODELS]
print(f"Probing this run: {len(todo)} (cap {MAX_MODELS})")

# COMMAND ----------

now = datetime.now(timezone.utc)
new_rows = []
for model_full_name, version in todo:
    try:
        info = mlflow.models.get_model_info(f"models:/{model_full_name}/{version}")
        flavors = list((info.flavors or {}).keys())
        loader = (info.flavors or {}).get("python_function", {}).get("loader_module")
        sig_inputs = str(info.signature.inputs) if (info.signature and info.signature.inputs) else None
        wc, fw, sig_kind, conf, src = _classify(flavors, loader, sig_inputs)
        new_rows.append((model_full_name, version, wc, fw, loader, sig_kind, conf, src, "resolved", now))
    except Exception as exc:
        msg = str(exc).lower()
        status = "forbidden" if ("forbidden" in msg or "permission" in msg or "does not have" in msg) else "error"
        # Negative cache so reruns don't re-probe (until a grant changes the picture).
        new_rows.append((model_full_name, version, "unknown", None, None, None, "low", None, status, now))

resolved = sum(1 for r in new_rows if r[8] == "resolved")
print(f"Probed {len(new_rows)} models: {resolved} resolved, {len(new_rows) - resolved} forbidden/error")

# COMMAND ----------

all_rows = cached_rows + new_rows
if all_rows:
    spark.createDataFrame(all_rows, SCHEMA_DEF) \
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)
else:
    spark.createDataFrame([], SCHEMA_DEF) \
        .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)
print(f"✅ Wrote {len(all_rows)} rows to {TABLE}")

if all_rows:
    display(spark.table(TABLE).groupBy("workload_class", "probe_status").count()
            .orderBy("workload_class", "probe_status"))

# COMMAND ----------

result = {"status": "success", "candidates": len(candidates),
          "probed_this_run": len(new_rows), "resolved_this_run": resolved,
          "total_rows": len(all_rows), "classified_at": now.isoformat()}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
