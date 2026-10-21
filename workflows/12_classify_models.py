# Databricks notebook source
# MAGIC %md
# MAGIC # Model Classification Producer (serving Stage 3 — unified)
# MAGIC
# MAGIC Resolves the dormant `unknown` tail of agent discovery — empty-task
# MAGIC `CUSTOM_MODEL` serving endpoints that Stage 1 (task) and Stage 2 (behavior)
# MAGIC could not classify — with a **tiered** classifier, strongest signal first:
# MAGIC
# MAGIC   1. **artifact** — the MLflow model's flavor + pyfunc loader + signature. The
# MAGIC      authoritative signal, but UC-grant gated and slow (~96% forbidden on shared
# MAGIC      accounts).
# MAGIC   2. **name regex** — canonical framework/model-family tokens (`xgboost`, `whisper`,
# MAGIC      `yolo`, `bge`, `esmfold`…); also test/demo exclusion. Exact, zero-maintenance.
# MAGIC   3. **LLM** — a (small, cheap) foundation-model endpoint classifies the long tail of
# MAGIC      business-purpose + domain-jargon names regex can't anticipate. Flags names that
# MAGIC      look like an agent rather than a model.
# MAGIC   4. **compute_type** — GPU → deep_learning, CPU → traditional_ml (coarse family that
# MAGIC      covers whatever's left).
# MAGIC
# MAGIC Decoupled side-channel (01_discover_agents reads fail-open, never waits):
# MAGIC
# MAGIC   this job → `model_classification` (Delta) ← 01_discover_agents upgrades `unknown` rows
# MAGIC
# MAGIC Each (model, version) is classified once and cached — including the tier/signal that
# MAGIC decided it — so reruns only touch new models and the LLM cost is a bounded backfill.
# MAGIC
# MAGIC **Table written:** `model_classification`

# COMMAND ----------

# MAGIC %pip install mlflow --upgrade
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import json
import re
from datetime import datetime, timezone

import mlflow
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

spark = SparkSession.builder.getOrCreate()
mlflow.set_registry_uri("databricks-uc")

# COMMAND ----------

dbutils.widgets.text("catalog", "", "Unity Catalog name")
dbutils.widgets.text("schema", "", "Schema name")
dbutils.widgets.text("max_models", "500", "Max new models to classify per run")
dbutils.widgets.text("llm_endpoint", "databricks-claude-sonnet-5-5",
                     "Foundation-model endpoint for the name-LLM tier (benchmark winner: 100% family / "
                     "93% subtype vs opus-5-5 reference; empty disables the tier)")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
MAX_MODELS = int(dbutils.widgets.get("max_models") or "500")
LLM_ENDPOINT = (dbutils.widgets.get("llm_endpoint") or "").strip()

if not CATALOG or not SCHEMA:
    raise ValueError(f"catalog and schema required (got {CATALOG!r}, {SCHEMA!r})")

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
TABLE = f"{CATALOG}.{SCHEMA}.model_classification"
print(f"Target table: {TABLE} | llm={LLM_ENDPOINT or '(disabled)'} | max_models/run {MAX_MODELS}")

# COMMAND ----------

SCHEMA_DEF = StructType([
    StructField("model_full_name", StringType(), False),
    StructField("model_version", StringType(), False),
    StructField("workload_class", StringType(), True),   # traditional_ml | deep_learning | genai_agent | llm | multimodal | unknown
    StructField("subtype", StringType(), True),          # forecasting | computer_vision | scientific_bio | risk_scoring | ...
    StructField("framework", StringType(), True),
    StructField("loader_module", StringType(), True),
    StructField("signature_kind", StringType(), True),   # tabular | messages | text | image | audio | null
    StructField("confidence", StringType(), True),
    StructField("source_signal", StringType(), True),    # flavor | loader | signature | name_regex | name_llm | compute_type | test_demo
    StructField("probe_status", StringType(), True),     # resolved | forbidden | error  (the ARTIFACT attempt outcome)
    StructField("probed_at", TimestampType(), False),
])

# ── Tier 1 (artifact) flavor/loader/signature knowledge ──────────────────────
_ML_FLAVORS = {"sklearn", "xgboost", "lightgbm", "catboost", "spark", "onnx",
               "tensorflow", "pytorch", "keras", "statsmodels", "prophet", "h2o", "fastai"}
_AGENT_FLAVORS = {"langchain", "langgraph", "llama_index", "openai", "crewai", "dspy"}
_AGENT_LOADER_HINTS = ("responses_agent", "chat_agent", "chat_model", "langchain",
                       "langgraph", "agent", "openai")


def _signature_kind(s):
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


def _classify_artifact(flavors, loader, sig_inputs):
    """Tier 1: (workload_class, framework, signature_kind, confidence, source_signal) or
    (None, ...) when the artifact gives no usable signal."""
    fl = set(flavors or {})
    sig_kind = _signature_kind(sig_inputs)
    agent_fl = fl & _AGENT_FLAVORS
    if agent_fl:
        return ("genai_agent", sorted(agent_fl)[0], sig_kind, "high", "flavor")
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
        return (("llm" if sig_kind in ("messages", "text") else "multimodal"),
                "transformers", sig_kind, "med", "signature")
    if sig_kind in ("messages", "text"):
        return ("llm", "pyfunc", sig_kind, "med", "signature")
    if sig_kind == "tabular":
        return ("traditional_ml", "pyfunc", sig_kind, "med", "signature")
    return (None, None, sig_kind, None, None)


# ── Tier 2 (name regex): canonical, high-precision tokens ────────────────────
_CANONICAL = [
    (re.compile(r"xgboost|lightgbm|catboost|sklearn|scikit|random.?forest|\bgbm\b", re.I), "traditional_ml", "tabular"),
    (re.compile(r"prophet|\barima\b|sarima", re.I), "traditional_ml", "forecasting"),
    (re.compile(r"whisper|wav2vec", re.I), "deep_learning", "speech_audio"),
    (re.compile(r"yolo|\bsam\d?\b|segment.?anything|resnet|efficientnet|\bvit\b|detectron|mask.?rcnn|\bunet\b|monai", re.I), "deep_learning", "computer_vision"),
    (re.compile(r"\bbert\b|roberta|\bner\b", re.I), "deep_learning", "nlp"),
    (re.compile(r"\bbge\b|\bgte\b|\be5\b|minilm|sentence.?transformer|nomic|\bembed", re.I), "deep_learning", "embedding"),
    (re.compile(r"esmfold|\besm\d|rfdiffusion|\bboltz\b|chemprop|scimilar|alphafold", re.I), "deep_learning", "scientific_bio"),
]
_TESTDEMO = re.compile(r"\btest\b|dbdemos|scratch|\btmp\b|\bsample\b|dummy|hello.?world|poc_", re.I)


def _name_segment(name):
    """The model name (last UC segment) with separators normalised to spaces and padded,
    so `\\b` boundaries fire on snake_case (`bge_large` → ` bge large `) and only the model
    name is matched — not the catalog/schema (`dbdemos.x.fraud_model`)."""
    seg = (name or "").lower().split(".")[-1]
    return " " + re.sub(r"[^a-z0-9]+", " ", seg).strip() + " "


def _classify_name_regex(name):
    """Tier 2. Returns (class, subtype, confidence, source_signal) or None."""
    seg = _name_segment(name)
    if _TESTDEMO.search(seg):
        return ("unknown", "test_demo", "low", "test_demo")
    for rx, cls, sub in _CANONICAL:
        if rx.search(seg):
            return (cls, sub, "high", "name_regex")
    return None


def _classify_compute(compute_type):
    """Tier 4 (coarse family)."""
    ct = (compute_type or "").upper()
    if ct.startswith("GPU") or ct.startswith("MULTIGPU"):
        return ("deep_learning", "deep_learning_unspecified", "low", "compute_type")
    if ct.startswith("CPU"):
        return ("traditional_ml", "classic_ml_unspecified", "low", "compute_type")
    return None

# COMMAND ----------

# MAGIC %md
# MAGIC ## Tier 3 — name-LLM for the long tail (fail-open, batched)

# COMMAND ----------

_FAMILY_TO_CLASS = {"classic_ml": "traditional_ml", "deep_learning": "deep_learning",
                    "llm": "llm", "agent": "genai_agent", "unknown": "unknown"}
_LLM_SYSTEM = (
    "You classify machine-learning models from their Unity Catalog name "
    "(catalog.schema.model) and serving compute type, using world knowledge of ML model "
    "and library names (e.g. esmfold/boltz = protein/scientific deep learning, "
    "cat_bond_spread = tabular regression). Return strict JSON only."
)
_LLM_INSTRUCTIONS = (
    "Return ONLY a JSON array, one object per input model. Echo the model's leading number "
    'as "idx" so results can be matched even if some are omitted or reordered:\n'
    '[{"idx":1,"family":"classic_ml|deep_learning|llm|agent|unknown",'
    '"subtype":"forecasting|anomaly_detection|risk_scoring|recommender|computer_vision|'
    'nlp|speech_audio|scientific_bio|tabular_classification|tabular_regression|embedding|other",'
    '"is_agent":true|false,"confidence":"high|med|low"}]\n'
    "family=agent or is_agent=true only when the name/schema clearly denotes an agent "
    "(tool use / orchestration), not a plain predictive model. No prose, no markdown."
)


def _llm_client():
    try:
        import mlflow.deployments
        return mlflow.deployments.get_deploy_client("databricks")
    except Exception as exc:
        print(f"  LLM tier disabled — deploy client unavailable ({type(exc).__name__}: {exc})")
        return None


def _llm_classify_batch(client, items):
    """items: [(model_full_name, compute_type)] → {name: (class, subtype, conf, 'name_llm')}.
    Matched to inputs by the echoed `idx` (robust to dropped/reordered results), never by
    position. Fail-open → {} on any error (callers fall back to compute_type)."""
    listing = "\n".join(f"{i+1}. {name}  [compute={ct or 'n/a'}]" for i, (name, ct) in enumerate(items))
    try:
        # No `temperature` — some reasoning endpoints reject it; keep the payload minimal.
        resp = client.predict(endpoint=LLM_ENDPOINT, inputs={
            "messages": [{"role": "system", "content": _LLM_SYSTEM},
                         {"role": "user", "content": f"{_LLM_INSTRUCTIONS}\n\nModels:\n{listing}"}],
            "max_tokens": 4000,
        })
        content = resp["choices"][0]["message"]["content"]
        if isinstance(content, list):   # reasoning endpoints return a list of parts
            content = "".join(p.get("text", "") for p in content
                              if isinstance(p, dict) and p.get("type") in (None, "text"))
        content = (content or "").strip()
        arr = json.loads(content[content.find("["): content.rfind("]") + 1])
        if not isinstance(arr, list):
            return {}
        out = {}
        for obj in arr:
            if not isinstance(obj, dict) or "family" not in obj:
                continue   # not our schema (e.g. a reasoning block)
            try:
                idx = int(obj.get("idx"))
            except (TypeError, ValueError):
                continue   # no usable index → skip (that model falls back + gets retried)
            if not (1 <= idx <= len(items)):
                continue
            name = items[idx - 1][0]
            fam = str(obj.get("family", "unknown")).lower()
            cls = "genai_agent" if obj.get("is_agent") else _FAMILY_TO_CLASS.get(fam, "unknown")
            sub = (str(obj.get("subtype")) or None) if obj.get("subtype") else None
            conf = str(obj.get("confidence") or "med").lower()
            conf = conf if conf in ("high", "med", "low") else "med"
            out[name] = (cls, sub, "med" if conf == "high" else conf, "name_llm")  # LLM caps at med
        return out
    except Exception as exc:
        print(f"  LLM batch failed (fail-open → compute_type): {type(exc).__name__}: {exc}")
        return {}

# COMMAND ----------

# Candidate models: empty-task CUSTOM_MODEL serving endpoints, with compute_type.
candidates = spark.sql("""
    WITH latest AS (
        SELECT entity_name, entity_version, task, custom_model_config.compute_type AS compute_type,
               ROW_NUMBER() OVER (PARTITION BY workspace_id, endpoint_name
                                  ORDER BY CAST(endpoint_config_version AS INT) DESC) rn
        FROM system.serving.served_entities
        WHERE endpoint_delete_time IS NULL AND entity_type = 'CUSTOM_MODEL'
    )
    SELECT DISTINCT entity_name AS model_full_name, entity_version AS model_version,
           FIRST(compute_type) OVER (PARTITION BY entity_name, entity_version) AS compute_type
    FROM latest
    WHERE rn = 1 AND (task IS NULL OR task = '')
      AND entity_name IS NOT NULL AND entity_version IS NOT NULL
""").collect()
candidates = [(r.model_full_name, str(r.model_version), r.compute_type) for r in candidates]
print(f"Candidate empty-task custom models: {len(candidates)}")

now = datetime.now(timezone.utc)

# Cache: keep only *settled* verdicts — authoritative artifact signals, a name-regex hit,
# an answered name-LLM verdict, or a test/demo exclusion. Rows that are compute_type
# fallbacks, still `unknown`, or from the old artifact-only negative cache are NOT kept:
# they get re-classified so a recovered LLM / new grant can upgrade them (addresses
# "transient failure cached forever" + "old unknowns never reach the new tiers"). Reads
# columns by name so a pre-`subtype` table migrates cleanly.
_CACHE_KEEP_SIGNALS = ("flavor", "loader", "signature", "name_regex", "name_llm", "test_demo")
cached_rows, done = [], set()
if spark.catalog.tableExists(TABLE):
    existing = spark.table(TABLE)
    cols = set(existing.columns)
    g = lambda r, c, d=None: (r[c] if c in cols else d)
    total = 0
    for r in existing.collect():
        total += 1
        if g(r, "source_signal") not in _CACHE_KEEP_SIGNALS:
            continue
        cached_rows.append((
            r["model_full_name"], r["model_version"], g(r, "workload_class"), g(r, "subtype"),
            g(r, "framework"), g(r, "loader_module"), g(r, "signature_kind"), g(r, "confidence"),
            g(r, "source_signal"), g(r, "probe_status", "resolved"), g(r, "probed_at", now),
        ))
        done.add((r["model_full_name"], r["model_version"]))
    print(f"Cached settled verdicts: {len(done)} of {total}; re-classifying {total - len(done)} unsettled")

todo = [(m, v, ct) for (m, v, ct) in candidates if (m, v) not in done][:MAX_MODELS]
print(f"Classifying this run: {len(todo)} (cap {MAX_MODELS})")

# COMMAND ----------

new_rows, llm_queue = [], []   # llm_queue holds (model, version, compute_type) needing Tier 3/4

for model_full_name, version, ct in todo:
    # Tier 1 — artifact probe (authoritative when readable).
    probe_status, wc = "resolved", None
    framework = loader = sig_kind = conf = src = sub = None
    try:
        info = mlflow.models.get_model_info(f"models:/{model_full_name}/{version}")
        flavors = list((info.flavors or {}).keys())
        loader = (info.flavors or {}).get("python_function", {}).get("loader_module")
        sig_inputs = str(info.signature.inputs) if (info.signature and info.signature.inputs) else None
        wc, framework, sig_kind, conf, src = _classify_artifact(flavors, loader, sig_inputs)
    except Exception as exc:
        msg = str(exc).lower()
        probe_status = "forbidden" if ("forbidden" in msg or "permission" in msg or "does not have" in msg) else "error"

    if wc:   # artifact gave a class
        new_rows.append((model_full_name, version, wc, sub, framework, loader, sig_kind, conf, src, probe_status, now))
        continue

    # Tier 2 — name regex.
    hit = _classify_name_regex(model_full_name)
    if hit:
        cls, sub, conf, src = hit
        new_rows.append((model_full_name, version, cls, sub, None, loader, sig_kind, conf, src, probe_status, now))
        continue

    # Defer to Tier 3 (LLM) / Tier 4 (compute).
    llm_queue.append((model_full_name, version, ct, probe_status, loader, sig_kind))

print(f"Tier 1+2 resolved {len(new_rows)} | deferred to LLM/compute: {len(llm_queue)}")

# Tier 3 — LLM (batched, capped) + Tier 4 — compute_type fallback.
llm_results = {}
client = _llm_client() if (LLM_ENDPOINT and llm_queue) else None
if client:
    batch, sent = [], 0
    for (m, v, ct, _ps, _ld, _sk) in llm_queue:
        batch.append((m, ct))
        if len(batch) >= 40:
            llm_results.update(_llm_classify_batch(client, batch)); sent += len(batch); batch = []
    if batch:
        llm_results.update(_llm_classify_batch(client, batch)); sent += len(batch)
    print(f"Tier 3 (LLM): sent {sent}, classified {sum(1 for x in llm_results.values() if x[0] != 'unknown')}")

for (m, v, ct, ps, ld, sk) in llm_queue:
    r = llm_results.get(m)
    if not r or r[0] == "unknown":
        r = _classify_compute(ct) or ("unknown", None, "low", "compute_type")
    cls, sub, conf, src = r
    new_rows.append((m, v, cls, sub, None, ld, sk, conf, src, ps, now))

# COMMAND ----------

all_rows = cached_rows + new_rows
spark.createDataFrame(all_rows if all_rows else [], SCHEMA_DEF) \
    .write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(TABLE)
print(f"✅ Wrote {len(all_rows)} rows to {TABLE}")
if all_rows:
    display(spark.table(TABLE).groupBy("workload_class", "source_signal").count()
            .orderBy("workload_class", "source_signal"))

# COMMAND ----------

resolved = sum(1 for r in new_rows if r[2] not in (None, "unknown"))
result = {"status": "success", "candidates": len(candidates), "classified_this_run": len(new_rows),
          "resolved_this_run": resolved, "llm_used": bool(client), "total_rows": len(all_rows),
          "classified_at": now.isoformat()}
print(json.dumps(result, indent=2))
dbutils.notebook.exit(json.dumps(result))
