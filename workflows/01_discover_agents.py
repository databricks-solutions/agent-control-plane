# Databricks notebook source
# MAGIC %md
# MAGIC # Agent Discovery Job
# MAGIC
# MAGIC Scheduled Workflow task that discovers all agents across the workspace
# MAGIC (serving endpoints, Databricks Apps, Genie Spaces, Agent Bricks)
# MAGIC and writes results to a Delta table.
# MAGIC
# MAGIC **Data flow:** Databricks APIs + System Tables → Delta table → Lakebase (next task)

# COMMAND ----------

# MAGIC %pip install httpx
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

import hashlib
import httpx
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, StringType, BooleanType, TimestampType,
)

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration

# COMMAND ----------

# Widgets for parameterization via Workflows
dbutils.widgets.text("catalog", "your_catalog", "Unity Catalog name")
dbutils.widgets.text("schema", "control_plane", "Schema name")
dbutils.widgets.text("delta_table", "discovered_agents", "Delta table name")

CATALOG = dbutils.widgets.get("catalog")
SCHEMA = dbutils.widgets.get("schema")
DELTA_TABLE = f"{CATALOG}.{SCHEMA}.{dbutils.widgets.get('delta_table')}"

# Ensure schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")

print(f"Target Delta table: {DELTA_TABLE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Delta Table Schema

# COMMAND ----------

DISCOVERED_AGENTS_SCHEMA = StructType([
    StructField("agent_id", StringType(), False),
    StructField("workspace_id", StringType(), False),
    StructField("name", StringType(), False),
    StructField("type", StringType(), True),
    StructField("endpoint_name", StringType(), True),
    StructField("endpoint_status", StringType(), True),
    StructField("model_name", StringType(), True),
    StructField("served_entity_name", StringType(), True),
    StructField("creator", StringType(), True),
    StructField("description", StringType(), True),
    StructField("config", StringType(), True),       # JSON string
    StructField("source", StringType(), True),        # api | cross_workspace_api | system_table | audit_log | user_api
    StructField("is_extensive", BooleanType(), True),
    StructField("discovered_at", TimestampType(), False),
    # ── Workload classification (Stage 1+) ──────────────────────────────
    StructField("workload_class", StringType(), True),   # genai_agent | llm | multimodal | embedding_model | foundation_model | external_llm | external_embedding | feature_serving | traditional_ml | agent_app | agent_frontend_app | ai_enabled_app | regular_app | genie_space | unknown
    StructField("subtype", StringType(), True),          # knowledge_assistant | multi_agent_supervisor | custom_llm | information_extraction | ...
    StructField("framework", StringType(), True),        # sklearn | xgboost | langgraph | openai-agents | ... (Stage 3 / apps)
    StructField("interface_task", StringType(), True),   # the serving task that decided it (agent/v1/responses, llm/v1/chat, ...)
    StructField("uses_llm", BooleanType(), True),        # behavioral: emits tokens / calls LLM endpoints (Stage 2)
    StructField("linked_endpoint", StringType(), True),  # apps → backing serving endpoint (dedup link)
    StructField("confidence", StringType(), True),       # high | med | low
    StructField("classified_by", StringType(), True),    # signal that decided: tile_metadata | task | entity_type | behavior_* | insufficient_signal
    StructField("classifier_version", StringType(), True),
    StructField("raw_signals", StringType(), True),       # JSON of inputs for audit / re-classification
])

# COMMAND ----------

# MAGIC %md
# MAGIC ## Helpers

# COMMAND ----------

_REST_TIMEOUT = 30.0

# Agent type taxonomy — matches the control plane app
_VALID_AGENT_TYPES = (
    "custom_agent", "custom_app", "custom_llm", "external_agent",
    "genie_space", "information_extraction",
    "knowledge_assistant", "multi_agent_supervisor",
)

_TILE_PROBLEM_TYPE_MAP = {
    "KNOWLEDGE_ASSISTANT": "knowledge_assistant",
    "MULTI_AGENT_SUPERVISOR": "multi_agent_supervisor",
    "CUSTOM_LLM": "custom_llm",
    "INFORMATION_EXTRACTION": "information_extraction",
}

# Types persisted to the agent inventory. Agents + `unknown` (the empty-task
# custom-model review queue). Definitively-non-agent serving classes
# (foundation_model, embedding_model, feature_serving) are not
# persisted here. Once Stage 2/3 land, `unknown` rows resolve in place.
_PERSISTED_TYPES = _VALID_AGENT_TYPES + (
    "unknown", "ml_model", "llm", "multimodal", "external_llm", "external_embedding")

# Back-compat `type` (from non-serving producers) → workload_class, so the new
# column is populated everywhere. Apps are refined to their precise class in the
# apps step; for now custom_app maps to agent_app.
_TYPE_TO_CLASS_FALLBACK = {
    "custom_agent": "genai_agent",
    "knowledge_assistant": "genai_agent",
    "multi_agent_supervisor": "genai_agent",
    "custom_llm": "genai_agent",
    "information_extraction": "genai_agent",
    "external_agent": "external_llm",
    "custom_app": "agent_app",
    "genie_space": "genie_space",
    "unknown": "unknown",
}

_ENTITY_TYPE_MAP = {
    "CUSTOM_MODEL": "custom_agent",
    "EXTERNAL_MODEL": "external_agent",
}

# ── Stage 1 workload classifier ─────────────────────────────────────────
# Classify the *artifact*, not the infrastructure. Validated against the live
# account: agents declare an `agent/*` task; LLMs `llm/*` / `<provider>/v1/*`;
# embeddings `llm/v1/embeddings`; traditional ML + feature specs have empty task.
# Empty-task CUSTOM_MODEL is genuinely ambiguous (ML vs no-task agent) → `unknown`,
# resolved later by Stage 2 (behavior) / Stage 3 (artifact). NEVER default to agent.
_CLASSIFIER_VERSION = "2026.06.stage1"

_AGENT_TASK_PREFIX = "agent/"
_EMBEDDING_TASKS = ("llm/v1/embeddings",)
_LLM_TASK_PREFIXES = ("llm/", "anthropic/", "responses/")

# workload_class → back-compat `type` (what the app / Lakebase already expect).
# Non-agent classes keep their own class string and are dropped from the agent
# inventory at persist time (they are not in _PERSISTED_TYPES).
_CLASS_TO_TYPE = {
    "genai_agent": "custom_agent",
    "external_llm": "external_llm",
    "external_embedding": "external_embedding",
    "llm": "llm",
    "multimodal": "multimodal",
    "traditional_ml": "ml_model",
    "deep_learning": "ml_model",   # non-LLM neural models (CV / speech / bio / NLP)
    "unknown": "unknown",
}


def classify_served_entity(entity_type, task, tile_problem_type=None, entity_name=""):
    """Deterministic Stage-1 classification from entity_type + task (+ Agent Bricks tile).

    Returns (workload_class, subtype, interface_task, confidence, classified_by).
    """
    et = (entity_type or "").upper()
    tk = (task or "").strip().lower()

    # Agent Bricks tile metadata is the most precise signal when present.
    sub = _TILE_PROBLEM_TYPE_MAP.get((tile_problem_type or "").upper())
    if sub:
        return ("genai_agent", sub, tk or "agent/*", "high", "tile_metadata")

    if et == "FEATURE_SPEC":
        return ("feature_serving", None, tk, "high", "entity_type")
    if et == "EXTERNAL_MODEL":
        # External provider proxy — split by task into LLM vs embedding routes.
        cls = "external_embedding" if tk in _EMBEDDING_TASKS else "external_llm"
        return (cls, None, tk, "high", "entity_type")

    # Agent task (prefix — covers agent/v1/responses, agent/v1/chat, agent/v2/chat)
    if tk.startswith(_AGENT_TASK_PREFIX):
        return ("genai_agent", None, tk, "high", "task")

    if tk in _EMBEDDING_TASKS:
        return ("embedding_model", None, tk, "high", "task")
    if any(tk.startswith(p) for p in _LLM_TASK_PREFIXES):
        # FM = stock platform model (dropped); custom LLM-task model = `llm`.
        cls = "foundation_model" if et == "FOUNDATION_MODEL" else "llm"
        return (cls, None, tk, "high", "task")

    if et == "FOUNDATION_MODEL":
        return ("foundation_model", None, tk, "high", "entity_type")
    if et == "CUSTOM_MODEL":
        # empty / non-LLM task on a custom model: cannot tell ML from a no-task agent.
        return ("unknown", None, tk, "low", "insufficient_signal")

    return ("unknown", None, tk, "low", "insufficient_signal")


def _workload_to_type(workload_class, subtype):
    """Map a workload_class to the back-compat `type` value."""
    if workload_class == "genai_agent":
        return subtype or "custom_agent"
    return _CLASS_TO_TYPE.get(workload_class, workload_class)


def _classification_fields(workload_class, subtype, interface_task, confidence,
                           classified_by, raw_signals):
    """Common new columns shared by every classified record."""
    return {
        "workload_class": workload_class,
        "subtype": subtype,
        "framework": None,
        "interface_task": interface_task or None,
        "uses_llm": None,
        "linked_endpoint": None,
        "confidence": confidence,
        "classified_by": classified_by,
        "classifier_version": _CLASSIFIER_VERSION,
        "raw_signals": json.dumps(raw_signals) if raw_signals else None,
    }


def _make_id(name: str, workspace: str) -> str:
    """Deterministic agent ID from name + workspace."""
    raw = f"{name}:{workspace}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _get_headers() -> dict:
    """Get auth headers using the notebook's context token."""
    token = dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()
    return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}


def _get_host() -> str:
    """Get the workspace host URL."""
    host = spark.conf.get("spark.databricks.workspaceUrl", "")
    if host and not host.startswith("https://"):
        host = f"https://{host}"
    return host


_workspace_id_cache: Optional[str] = None


def _get_workspace_id() -> str:
    """Get the numeric workspace ID of the current workspace.

    The notebook-context ``orgId`` tag is not readable on serverless compute,
    so also try the cluster usage tag and the ``x-databricks-org-id`` response
    header before the host-hash fallback. A hash fallback stores API-sourced
    rows under a workspace_id that matches no other source, which breaks
    dedup against audit-log rows and hides them from workspace-scoped users.
    """
    global _workspace_id_cache
    if _workspace_id_cache:
        return _workspace_id_cache

    org_id = ""
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        org_id = ctx.tags().get("orgId").getOrElse(lambda: "")
    except Exception:
        pass
    if not org_id:
        try:
            org_id = spark.conf.get("spark.databricks.clusterUsageTags.orgId", "")
        except Exception:
            pass
    if not org_id:
        try:
            resp = httpx.get(
                f"{_get_host()}/api/2.0/preview/scim/v2/Me",
                headers=_get_headers(), timeout=_REST_TIMEOUT,
            )
            org_id = resp.headers.get("x-databricks-org-id", "")
        except Exception as e:
            print(f"  WARNING: could not read x-databricks-org-id: {e}")
    if org_id:
        _workspace_id_cache = str(org_id)
        return _workspace_id_cache

    # Fallback: hash the host
    print("  WARNING: workspace ID unresolved — falling back to a host hash")
    return hashlib.sha256(_get_host().encode()).hexdigest()[:16]

# COMMAND ----------

# MAGIC %md
# MAGIC ## Discovery Functions

# COMMAND ----------

def discover_serving_endpoints() -> List[Dict[str, Any]]:
    """Discover custom/external agents from serving endpoints via REST API."""
    base = _get_host()
    headers = _get_headers()
    agents = []

    try:
        resp = httpx.get(f"{base}/api/2.0/serving-endpoints", headers=headers, timeout=_REST_TIMEOUT)
        if resp.status_code != 200:
            print(f"  WARNING: serving-endpoints returned {resp.status_code}")
            return []

        ws_id = _get_workspace_id()
        for ep in resp.json().get("endpoints", []):
            name = ep.get("name", "")
            served = ep.get("config", {}).get("served_entities", []) or []
            # `task` is an ENDPOINT-level field in the REST payload (served-entity
            # task is null) — read it here, not from served_entities.
            endpoint_task = ep.get("task", "") or ""

            # Agent Bricks tile metadata (KA/MAS often have zero served_entities).
            tile = ep.get("tile_endpoint_metadata") or {}
            problem_type = (tile.get("problem_type") or "").upper()

            if not served and not problem_type and not endpoint_task:
                continue  # nothing to classify

            # Fallback: fetch the endpoint detail for tile metadata when it shows an
            # agent task but the list payload lacked the tile block.
            if not problem_type and endpoint_task.startswith(_AGENT_TASK_PREFIX):
                try:
                    det = httpx.get(
                        f"{base}/api/2.0/serving-endpoints/{name}",
                        headers=headers, timeout=_REST_TIMEOUT,
                    )
                    if det.status_code == 200:
                        tile = det.json().get("tile_endpoint_metadata") or tile
                        problem_type = (tile.get("problem_type") or "").upper()
                except Exception:
                    pass

            # Derive entity_type for the classifier from served-entity config.
            se0 = served[0] if served else {}
            if se0.get("external_model") is not None:
                entity_type = "EXTERNAL_MODEL"
            elif se0.get("foundation_model") is not None:
                entity_type = "FOUNDATION_MODEL"
            elif se0.get("entity_name"):
                if str(se0.get("entity_name", "")).startswith("system.ai."):
                    continue  # system-provided FM building block — not an endpoint we own
                entity_type = "CUSTOM_MODEL"
            else:
                entity_type = ""  # tile-only (KA/MAS) — classifier uses the tile

            workload_class, subtype, interface_task, confidence, classified_by = (
                classify_served_entity(entity_type, endpoint_task, problem_type,
                                       se0.get("entity_name", "")))

            # Plain foundation / embedding models are not agents — skip from the
            # agent inventory (consistent with prior behavior).
            if workload_class in ("foundation_model", "embedding_model", "feature_serving"):
                continue

            agent_type = _workload_to_type(workload_class, subtype)
            state = (ep.get("state", {}) or {}).get("ready", "") or ""
            creator = ep.get("creator", "") or ""

            # served may be empty for Agent Bricks (KA/MAS) endpoints
            se0 = served[0] if served else {}
            model_name = se0.get("entity_name", "") or ""
            if se0.get("foundation_model"):
                model_name = se0["foundation_model"].get("name", "") or model_name
            # For Agent Bricks with no served_entities, use the tile model name
            if not model_name and tile:
                model_name = tile.get("tile_model_name", "")

            config_dict = {"deployment_type": agent_type}
            if tile:
                config_dict["tile_id"] = tile.get("tile_id", "")
                config_dict["problem_type"] = problem_type
                if tile.get("deployment_type"):
                    config_dict["tile_deployment_type"] = tile["deployment_type"]
            tags_list = ep.get("tags", [])
            if tags_list:
                config_dict["tags"] = {t.get("key", ""): t.get("value", "") for t in tags_list}

            agents.append({
                "agent_id": _make_id(name, ws_id),
                "workspace_id": ws_id,
                "name": name,
                "type": agent_type,
                "endpoint_name": name,
                "endpoint_status": state,
                "model_name": model_name,
                "served_entity_name": se0.get("name", ""),
                "creator": creator,
                "description": tile.get("tile_model_name", "") if tile else "",
                "config": json.dumps(config_dict),
                "source": "api",
                "is_extensive": False,
                **_classification_fields(
                    workload_class, subtype, interface_task, confidence, classified_by,
                    {"entity_type": entity_type, "task": endpoint_task,
                     "problem_type": problem_type or None}),
            })

        print(f"  Serving endpoints: {len(agents)} agents")
    except Exception as e:
        print(f"  ERROR discovering serving endpoints: {e}")
    return agents


# ── App classification (Stage 1+2+3) ────────────────────────────────────
# Authoritative signal is the app's own source: pyproject.toml / requirements.txt
# (agent frameworks) + app.yaml (MLflow AgentServer pattern). "uses an LLM" is NOT
# "is an agent" — a dashboard with a Genie feature is ai_enabled_app, not an agent.
_AGENT_FRAMEWORKS = (
    "databricks-agents", "databricks_agents", "langgraph", "openai-agents",
    "crewai", "autogen", "pyautogen", "dspy", "llama-index", "llama_index",
)
# LLM client libs (LLM use without agent orchestration → ai_enabled_app)
_LLM_CLIENT_LIBS = (
    "openai", "anthropic", "databricks-openai", "langchain", "databricks-langchain",
    "litellm", "mistralai", "cohere",
)
_APP_CLASSES = ("agent_app", "agent_frontend_app", "candidate_agent_app",
                "ai_enabled_app", "regular_app")

# Resource types (declared in the app spec) that act as agent *tools* — their
# presence alongside an LLM is a structural agent signal (Tier 2). Genie = NL→SQL
# data tool; job = workflow/tool; vector_search = RAG retrieval.
_AGENT_TOOL_RESOURCE_TYPES = {
    "genie_space", "genie", "job", "vector_search_index", "vector_search",
    "uc_function", "function",
}
_AGENTIC_NAME_RE = re.compile(
    r"agent|assistant|copilot|chatbot|\brag\b|genai|\bbot\b", re.IGNORECASE)
# A serving-endpoint name that denotes retrieval (embeddings / reranker) — pairing one
# with an LLM endpoint is a RAG-agent architecture signal.
_RETRIEVAL_ENDPOINT_RE = re.compile(
    r"embed|bge|gte|arctic-embed|minilm|e5-|nomic|rerank", re.IGNORECASE)


def _app_class_to_type(app_class):
    """Map an app_class to the back-compat `type`. Confirmed + candidate agent apps all
    map to `custom_app` so they flow through the same app-agent plumbing in the backend
    (permissions, operations); the agent-vs-candidate distinction lives in `workload_class`
    + `confidence`, not in `type`."""
    if app_class in ("agent_app", "agent_frontend_app", "candidate_agent_app"):
        return "custom_app"
    if app_class == "unknown":
        return "app_unknown"      # distinct from serving `unknown`; not persisted
    return app_class               # ai_enabled_app / regular_app — not persisted


def _parse_app_resources(resources):
    """Parse an app's declared resources into (resources_out, features) — shared by
    local (`_build_app_record`) and audit (`discover_apps_from_audit_logs`) discovery so
    both score through the same multi-signal taxonomy. `features` carries the Tier 1/2
    signals: serving-endpoint names, MLflow experiment id, and the set of resource types
    (for tool/orchestration signals)."""
    out = []
    feat = {"endpoint_names": [], "experiment_id": None, "resource_types": []}
    for r in (resources or []):
        if not isinstance(r, dict):
            continue
        # The resource type is the sole key that isn't metadata.
        rtype = next((k for k in r.keys()
                      if k not in ("name", "description", "permission")), "")
        entry = {"name": r.get("name", ""), "type": rtype}
        nested = r.get(rtype) if rtype else None
        if rtype == "serving_endpoint" and isinstance(nested, dict) and nested.get("name"):
            entry["endpoint_name"] = nested["name"]
            feat["endpoint_names"].append(nested["name"])
        elif rtype == "experiment" and isinstance(nested, dict):
            feat["experiment_id"] = nested.get("experiment_id") or nested.get("id")
        if rtype:
            feat["resource_types"].append(rtype)
        out.append(entry)
    return out, feat


def _agent_runtime_experiments(window_days=90):
    """Set of MLflow experiment ids with agent *runtime* activity, account-wide —
    the union of `agentEvaluation` monitoring (scheduled scorers / review apps /
    assessments) and `mlflowTrace` trace search (traces logged against an
    experiment). An app whose experiment resource is in this set is a confirmed
    agent (Tier 1). Fail-open → empty set. (Trace-search experiments overlap the
    eval set heavily; the union is a cheap superset, never smaller.)"""
    exps = set()
    try:
        rows = spark.sql(f"""
            SELECT DISTINCT COALESCE(
                       request_params['experiment_id'],
                       get_json_object(request_params['review_app'], '$.experiment_id')
                   ) AS experiment_id
            FROM system.access.audit
            WHERE service_name = 'agentEvaluation'
              AND event_date >= CURRENT_DATE - {int(window_days)}
              AND COALESCE(request_params['experiment_id'],
                           get_json_object(request_params['review_app'], '$.experiment_id')) IS NOT NULL
        """).collect()
        exps |= {str(r.experiment_id) for r in rows if r.experiment_id}
    except Exception as e:
        print(f"  Agent-eval experiments unavailable (fail-open): {e}")
    try:
        rows = spark.sql(f"""
            SELECT DISTINCT get_json_object(request_params['locations'],
                              '$[0].mlflow_experiment.experiment_id') AS experiment_id
            FROM system.access.audit
            WHERE service_name = 'mlflowTrace'
              AND action_name IN ('searchUCTraces', 'searchUCTracesLongRunning')
              AND event_date >= CURRENT_DATE - {int(window_days)}
              AND get_json_object(request_params['locations'],
                     '$[0].mlflow_experiment.experiment_id') IS NOT NULL
        """).collect()
        exps |= {str(r.experiment_id) for r in rows if r.experiment_id}
    except Exception as e:
        print(f"  Agent-trace experiments unavailable (fail-open): {e}")
    return exps


_EXPORT_TIMEOUT = 8.0  # short — many source files are absent / cross-ws ACL-blocked


def _export_ws_text(base, headers, path):
    """Best-effort export of a workspace file as text (fail-open → None)."""
    try:
        resp = httpx.get(f"{base}/api/2.0/workspace/export", headers=headers,
                         params={"path": path, "direct_download": "true"},
                         timeout=_EXPORT_TIMEOUT)
        if resp.status_code == 200:
            return resp.text
    except Exception:
        pass
    return None


def _read_app_signals(base, headers, source_path):
    """Read dependency + app.yaml text from an app's source dir (workspace-local)."""
    if not source_path:
        return {"deps": "", "app_yaml": ""}
    deps = []
    for fn in ("requirements.txt", "pyproject.toml"):
        t = _export_ws_text(base, headers, f"{source_path}/{fn}")
        if t:
            deps.append(t)
    return {"deps": "\n".join(deps), "app_yaml": _export_ws_text(base, headers, f"{source_path}/app.yaml") or ""}


def _endpoint_from_app_yaml(app_yaml):
    """Extract a backing serving endpoint referenced via app.yaml env (e.g.
    MODEL_SERVING_ENDPOINT), which is NOT a declared resource."""
    lines = (app_yaml or "").splitlines()
    for i, ln in enumerate(lines):
        low = ln.lower()
        if "name:" in low and "serving_endpoint" in low:
            for j in range(i + 1, min(i + 3, len(lines))):
                if "value:" in lines[j].lower():
                    return lines[j].split("value:", 1)[1].strip().strip('"\'')
    return ""


def classify_app_source(deps_text, app_yaml_text):
    """Source-only app classification (Stage 3). Endpoint-link + behavior are applied
    later in resolve_apps(). Returns (app_class, framework, confidence, classified_by)."""
    deps = (deps_text or "").lower()
    ay = (app_yaml_text or "").lower()
    framework = next((f for f in _AGENT_FRAMEWORKS if f in deps), None)
    agent_server = ("invocations" in ay) or ("agent_server" in ay) or ("agent_server" in deps)
    if framework or agent_server:
        return ("agent_app", framework or "mlflow_agentserver", "high",
                "app_source" if framework else "app_yaml")
    if any(c in deps for c in _LLM_CLIENT_LIBS):
        return ("ai_enabled_app", None, "med", "app_source")
    if deps or ay:
        return ("regular_app", None, "high", "app_source")
    return ("unknown", None, "low", "insufficient_signal")


def _build_app_record(app, ws_id, source, base, headers, read_source=True):
    """Build one classified app record (shared by local + cross-workspace discovery).

    `read_source` reads the app's pyproject/app.yaml (Stage 3). Disabled for
    cross-workspace apps — remote file exports are slow and usually ACL-blocked, so
    those rely on endpoint-link (resource) + behavior instead.
    """
    name = app.get("name", "")
    cs = app.get("compute_status") or {}
    status = cs.get("state", "")
    if not status:
        dep_status = (app.get("active_deployment") or {}).get("status") or {}
        status = dep_status.get("state", "")
    raw_creator = app.get("creator", "")
    creator = raw_creator.get("username", "") if isinstance(raw_creator, dict) else str(raw_creator or "")

    config_dict = {
        "url": app.get("url", ""),
        "app_id": app.get("id", ""),
        "compute_size": app.get("compute_size", ""),
        "deployment_type": "databricks_app",
        "deployment_method": "app",
        "sp_client_id": app.get("service_principal_client_id", "") or "",
    }

    resources_out, app_features = _parse_app_resources(app.get("resources") or [])
    resource_endpoint = app_features["endpoint_names"][0] if app_features["endpoint_names"] else ""
    if resources_out:
        config_dict["resources"] = resources_out
    config_dict["app_features"] = app_features

    # Stage 3: read the app's own source (workspace-local; fail-open if unreadable).
    signals = (_read_app_signals(base, headers, app.get("default_source_code_path", ""))
               if read_source else {"deps": "", "app_yaml": ""})
    env_endpoint = _endpoint_from_app_yaml(signals["app_yaml"])
    linked_endpoint = resource_endpoint or env_endpoint or ""
    app_class, framework, confidence, classified_by = classify_app_source(
        signals["deps"], signals["app_yaml"])

    if linked_endpoint:
        config_dict["linked_endpoint"] = linked_endpoint

    return {
        "agent_id": _make_id(name, ws_id),
        "workspace_id": ws_id,
        "name": name,
        "type": _app_class_to_type(app_class),
        "endpoint_name": name,
        "endpoint_status": status,
        "model_name": "",
        "served_entity_name": "",
        "creator": creator,
        "description": app.get("description", ""),
        "config": json.dumps(config_dict),
        "source": source,
        "is_extensive": source != "api",
        "workload_class": app_class,
        "subtype": None,
        "framework": framework,
        "interface_task": None,
        "uses_llm": None,
        "linked_endpoint": linked_endpoint or None,
        "confidence": confidence,
        "classified_by": classified_by,
        "classifier_version": _CLASSIFIER_VERSION,
        "raw_signals": json.dumps({"source_readable": bool(signals["deps"] or signals["app_yaml"])}),
    }


def discover_apps() -> List[Dict[str, Any]]:
    """Discover Databricks Apps via REST API (classified; refined in resolve_apps())."""
    base = _get_host()
    headers = _get_headers()
    agents = []
    next_page_token = None

    try:
        while True:
            params = {"page_size": 100}
            if next_page_token:
                params["page_token"] = next_page_token

            resp = httpx.get(f"{base}/api/2.0/apps", headers=headers, params=params, timeout=_REST_TIMEOUT)
            if resp.status_code != 200:
                print(f"  WARNING: /api/2.0/apps returned {resp.status_code}")
                break

            body = resp.json()
            ws_id = _get_workspace_id()

            for app in body.get("apps", []):
                # Classify every app (no serving_endpoint gate); resolve_apps()
                # finalizes endpoint-frontend + behavioral signals afterward.
                agents.append(_build_app_record(app, ws_id, "api", base, headers))

            next_page_token = body.get("next_page_token")
            if not next_page_token:
                break

        print(f"  Databricks Apps: {len(agents)} apps classified")
    except Exception as e:
        print(f"  ERROR discovering apps: {e}")
    return agents


def discover_genie_spaces() -> List[Dict[str, Any]]:
    """Discover Genie Spaces via REST API."""
    base = _get_host()
    headers = _get_headers()
    agents = []

    try:
        resp = httpx.get(
            f"{base}/api/2.0/genie/spaces", headers=headers,
            params={"page_size": 200}, timeout=_REST_TIMEOUT,
        )
        if resp.status_code != 200:
            print(f"  WARNING: genie/spaces returned {resp.status_code}")
            return []

        ws_id = _get_workspace_id()
        for s in resp.json().get("spaces", []):
            space_id = s.get("space_id", "")
            if not space_id:
                continue
            title = s.get("title", "") or s.get("display_name", "")
            name = title or space_id

            # Use space_id as canonical key so API + audit log dedup correctly
            agents.append({
                "agent_id": _make_id(f"genie:{space_id}", ws_id),
                "workspace_id": ws_id,
                "name": name,
                "type": "genie_space",
                "endpoint_name": space_id,
                "endpoint_status": "ACTIVE",
                "model_name": "",
                "served_entity_name": "",
                "creator": s.get("creator_name", "") or str(s.get("creator", "") or ""),
                "description": s.get("description", ""),
                "config": json.dumps({"space_id": space_id, "deployment_type": "genie_space"}),
                "source": "api",
                "is_extensive": False,
            })

        print(f"  Genie Spaces (API): {len(agents)}")
    except Exception as e:
        print(f"  ERROR discovering Genie Spaces: {e}")
    return agents

# COMMAND ----------

# MAGIC %md
# MAGIC ## System Tables Discovery (Cross-Workspace)

# COMMAND ----------

def discover_from_system_tables() -> List[Dict[str, Any]]:
    """Query system.serving.served_entities for cross-workspace agents."""
    agents = []
    try:
        rows = spark.sql("""
            SELECT
                workspace_id,
                endpoint_name,
                endpoint_id,
                served_entity_name,
                entity_type,
                entity_name AS model_name,
                entity_version,
                task,
                created_by AS creator,
                change_time
            FROM system.serving.served_entities
            WHERE endpoint_delete_time IS NULL
              AND entity_type IN ('CUSTOM_MODEL', 'EXTERNAL_MODEL', 'FOUNDATION_MODEL')
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY workspace_id, endpoint_name
                ORDER BY CAST(endpoint_config_version AS INT) DESC
            ) = 1
            ORDER BY workspace_id, endpoint_name
        """).collect()

        for r in rows:
            ws = str(r.workspace_id or "")
            ename = r.endpoint_name or ""
            entity_type = r.entity_type or ""
            task_type = r.task or ""
            creator = str(r.creator or "")

            # Stage-1 classification from entity_type + task. System tables lack
            # tile metadata, so Agent Bricks (genai_agent) gets its precise subtype
            # from the API source via dedup priority. Empty-task CUSTOM_MODEL → unknown
            # (NOT a blanket custom_agent — that was the false-positive bug).
            workload_class, subtype, interface_task, confidence, classified_by = (
                classify_served_entity(entity_type, task_type, None, r.model_name or ""))

            # Stock platform models (foundation/embedding) and feature specs are not
            # part of the inventory — skip. Custom models (traditional_ml / llm / multimodal)
            # ARE kept and labeled (not dropped).
            if workload_class in ("foundation_model", "embedding_model", "feature_serving"):
                continue

            agent_type = _workload_to_type(workload_class, subtype)

            config_dict = {
                "entity_type": entity_type,
                "entity_version": str(r.entity_version or ""),
                "task": task_type,
                "endpoint_id": r.endpoint_id or "",
            }

            agents.append({
                "agent_id": _make_id(ename, ws),
                "workspace_id": ws,
                "name": ename,
                "type": agent_type,
                "endpoint_name": ename,
                "endpoint_status": entity_type,
                "model_name": r.model_name or "",
                "served_entity_name": r.served_entity_name or "",
                "creator": creator,
                "description": "",
                "config": json.dumps(config_dict),
                "source": "system_table",
                "is_extensive": True,
                **_classification_fields(
                    workload_class, subtype, interface_task, confidence, classified_by,
                    {"entity_type": entity_type, "task": task_type}),
            })

        print(f"  System tables: {len(agents)} agents")
    except Exception as e:
        print(f"  ERROR querying system tables: {e}")
    return agents


def enrich_unknown_with_behavior(agents, window_days=30):
    """Stage 2: refine empty-task CUSTOM_MODEL `unknown` rows using runtime behavior.

    Joins `system.serving.endpoint_usage` (by served_entity_id → endpoint) over a
    recent window:
      - co-located `feedback` served entity (Agent Framework marker) → genai_agent
      - emits tokens / streams                                       → llm (uses_llm)
      - traffic but zero tokens                                      → traditional_ml
      - no traffic in window                                         → stays unknown

    Behavior proves "uses an LLM", not "is an agent" (the dashboard lesson), so token
    emission yields `llm` — not genai_agent — unless the feedback marker fires.
    Fail-open: any error leaves the unknown rows unchanged. Mutates `agents` in place.
    """
    unknown = [a for a in agents if a.get("workload_class") == "unknown"]
    if not unknown:
        return
    try:
        beh_rows = spark.sql(f"""
            WITH u AS (
                SELECT served_entity_id,
                       SUM(COALESCE(input_token_count, 0) + COALESCE(output_token_count, 0)) AS toks,
                       MAX(CASE WHEN request_streaming THEN 1 ELSE 0 END) AS streamed,
                       COUNT(*) AS reqs
                FROM system.serving.endpoint_usage
                WHERE request_time >= current_timestamp() - INTERVAL {int(window_days)} DAYS
                GROUP BY served_entity_id
            ),
            se AS (
                SELECT served_entity_id, workspace_id, endpoint_name
                FROM system.serving.served_entities
                WHERE endpoint_delete_time IS NULL
            )
            SELECT se.workspace_id AS ws, se.endpoint_name AS ep,
                   SUM(u.toks) AS toks, MAX(u.streamed) AS streamed, SUM(u.reqs) AS reqs
            FROM u JOIN se USING (served_entity_id)
            GROUP BY se.workspace_id, se.endpoint_name
        """).collect()
        behavior = {(str(r.ws), r.ep): (int(r.toks or 0), int(r.streamed or 0), int(r.reqs or 0))
                    for r in beh_rows}

        fb_rows = spark.sql("""
            SELECT DISTINCT workspace_id AS ws, endpoint_name AS ep
            FROM system.serving.served_entities
            WHERE endpoint_delete_time IS NULL
              AND (lower(served_entity_name) LIKE '%feedback%' OR lower(entity_name) LIKE '%feedback%')
        """).collect()
        feedback = {(str(r.ws), r.ep) for r in fb_rows}
    except Exception as e:
        print(f"  Stage 2 behavioral enrichment unavailable (fail-open): {e}")
        return

    counts = {"genai_agent": 0, "llm": 0, "traditional_ml": 0, "unknown": 0}
    for a in unknown:
        key = (str(a.get("workspace_id", "")), a.get("endpoint_name", ""))
        if key in feedback:
            a.update(workload_class="genai_agent", type="custom_agent",
                     confidence="high", classified_by="feedback_sibling", uses_llm=True)
            counts["genai_agent"] += 1
            continue
        beh = behavior.get(key)
        if not beh or beh[2] == 0:
            counts["unknown"] += 1            # no traffic — leave for Stage 3
            continue
        toks, streamed, _reqs = beh
        if toks > 0 or streamed:
            a.update(workload_class="llm", type=_workload_to_type("llm", None),
                     uses_llm=True, confidence="high", classified_by="behavior_tokens")
            counts["llm"] += 1
        else:
            a.update(workload_class="traditional_ml", type=_workload_to_type("traditional_ml", None),
                     uses_llm=False, confidence="med", classified_by="behavior_no_tokens")
            counts["traditional_ml"] += 1
    print(f"  Stage 2 behavioral: refined {len(unknown)} unknown → {counts}")


def resolve_apps(agents, window_days=30):
    """Finalize app classification via the multi-signal taxonomy. Runs after serving
    classification (so the genai_agent set is final). Mutates `agents` in place.

    Each app is scored over an ensemble of account-wide signals, not a single
    endpoint-class check (which misses app-native agents). The verdict carries a
    confidence and the evidence that decided it (in `raw_signals`), and is upgradeable.

      agent_app (high)          : source-detected frameworks, OR an experiment resource
                                  under agentEvaluation monitoring (confirmed, app-native)
      agent_frontend_app (high) : fronts a genai_agent serving endpoint (UI over a known agent)
      candidate_agent_app (med) : structural signals (multi-endpoint orchestration, Genie/
                                  job/vector tools, experiment resource) — review queue
      ai_enabled_app (low)      : a lone plain-LLM endpoint with no other agent signal
      regular_app / unknown     : no serving endpoint and no signal

    "Uses an LLM" alone never promotes to agent; an agent-looking NAME alone never does
    either (it only tips a structural candidate).
    """
    apps = []
    for a in agents:
        try:
            cfg = json.loads(a.get("config") or "{}")
        except Exception:
            cfg = {}
        if cfg.get("deployment_method") == "app":
            apps.append((a, cfg))
    if not apps:
        return

    agent_eps = {(str(a.get("workspace_id", "")), a.get("endpoint_name", ""))
                 for a in agents if a.get("workload_class") == "genai_agent"}
    runtime_exps = _agent_runtime_experiments()   # experiments w/ agent eval or trace activity (Tier 1)

    # App-SP behavioral: which app service principals call serving endpoints w/ tokens.
    llm_sps = set()
    sp_ids = {cfg.get("sp_client_id") for _a, cfg in apps if cfg.get("sp_client_id")}
    if sp_ids:
        try:
            in_list = ",".join("'" + s.replace("'", "") + "'" for s in sp_ids)
            rows = spark.sql(f"""
                SELECT requester,
                       SUM(COALESCE(input_token_count, 0) + COALESCE(output_token_count, 0)) AS toks
                FROM system.serving.endpoint_usage
                WHERE requester IN ({in_list})
                  AND request_time >= current_timestamp() - INTERVAL {int(window_days)} DAYS
                GROUP BY requester
            """).collect()
            llm_sps = {r.requester for r in rows if int(r.toks or 0) > 0}
        except Exception as e:
            print(f"  App-SP behavioral unavailable (fail-open): {e}")

    counts = {}
    for a, cfg in apps:
        ws = str(a.get("workspace_id", ""))
        feat = cfg.get("app_features") or {}
        ep_names = feat.get("endpoint_names") or ([cfg.get("linked_endpoint")] if cfg.get("linked_endpoint") else [])
        rtypes = set(feat.get("resource_types") or [])
        exp_id = feat.get("experiment_id")
        spid = cfg.get("sp_client_id")
        sp_llm = bool(spid) and spid in llm_sps
        cur = a.get("workload_class")
        name_blob = f"{a.get('name', '')} {a.get('description', '')}"

        valid_eps = [e for e in ep_names if e]
        fronts_agent = any((ws, ep) in agent_eps for ep in valid_eps)
        exp_confirmed = bool(exp_id) and str(exp_id) in runtime_exps
        # Endpoint architecture: retrieval (embedding/reranker) + LLM = RAG; ≥2 LLMs = router.
        retrieval_eps = [e for e in valid_eps if _RETRIEVAL_ENDPOINT_RE.search(e)]
        llm_eps = [e for e in valid_eps if not _RETRIEVAL_ENDPOINT_RE.search(e)]
        has_retrieval = bool(retrieval_eps) or bool(rtypes & {"vector_search_index", "vector_search"})
        rag_wiring = has_retrieval and bool(llm_eps)
        router_wiring = len(llm_eps) >= 2

        # Collect evidence (which signals fired), then decide by precedence.
        evidence = []
        if cur == "agent_app":
            evidence.append("app_source")                 # T1: source frameworks (local only)
        if exp_confirmed:
            evidence.append("experiment_runtime")         # T1: eval/trace activity on its experiment
        if fronts_agent:
            evidence.append("fronts_genai_agent")          # T1: UI over a known agent
        if rag_wiring:
            evidence.append("rag_wiring")                 # strong architecture: retrieval + LLM
        if router_wiring:
            evidence.append("router_wiring")              # strong architecture: multi-LLM
        if len(valid_eps) > 1:
            evidence.append("multi_endpoint")             # T2: orchestration
        if exp_id:
            evidence.append("experiment_resource")        # T2: agent tracing
        if rtypes & {"genie_space", "genie"}:
            evidence.append("genie_tool")                 # T2: data tool
        if "job" in rtypes:
            evidence.append("job_tool")                   # T2: workflow tool
        if rtypes & {"vector_search_index", "vector_search"}:
            evidence.append("vector_rag")                 # T2: RAG retrieval
        name_agentic = bool(_AGENTIC_NAME_RE.search(name_blob))
        if name_agentic:
            evidence.append("agentic_name")               # T4: weak prior

        # Structural / architecture signals are *inferential* (not direct proof) → they
        # drive the med-confidence candidate queue, never a high-confidence agent_app.
        # High confidence (agent_app) is reserved for direct evidence: source frameworks,
        # an eval/trace-monitored experiment, or a genai_agent endpoint link.
        has_candidate_signal = (
            rag_wiring or router_wiring
            or any(e in evidence for e in
                   ("multi_endpoint", "experiment_resource", "genie_tool", "job_tool", "vector_rag"))
            or (name_agentic and valid_eps))

        if "app_source" in evidence or exp_confirmed:
            final = "agent_app"
            a["confidence"], a["classified_by"] = "high", (
                "app_source" if "app_source" in evidence else "experiment_runtime")
        elif fronts_agent:
            final = "agent_frontend_app"
            a["confidence"], a["classified_by"] = "high", "endpoint_link"
        elif has_candidate_signal:
            final = "candidate_agent_app"
            a["confidence"], a["classified_by"] = "med", (
                "architecture_rag" if rag_wiring else
                "architecture_router" if router_wiring else "multi_signal")
        elif cur == "ai_enabled_app" or sp_llm or valid_eps:
            final = "ai_enabled_app"
            if sp_llm and cur != "ai_enabled_app":
                a["confidence"], a["classified_by"] = "med", "behavior_sp_tokens"
            else:
                a["confidence"], a["classified_by"] = "low", "llm_use"
        elif cur == "regular_app":
            final = "regular_app"
        else:
            final = "unknown"

        a["workload_class"] = final
        a["type"] = _app_class_to_type(final)
        a["uses_llm"] = final in ("agent_app", "agent_frontend_app", "candidate_agent_app",
                                  "ai_enabled_app") or sp_llm
        # Record the signals that decided this verdict (auditable + upgradeable).
        try:
            rs = json.loads(a.get("raw_signals") or "{}")
        except Exception:
            rs = {}
        rs["app_evidence"] = evidence
        a["raw_signals"] = json.dumps(rs)
        counts[final] = counts.get(final, 0) + 1
    print(f"  Apps resolved (multi-signal): {counts} (from {len(apps)} apps)")


_ARTIFACT_SIGNALS = ("flavor", "loader", "signature")


def apply_model_classification(agents):
    """Serving Stage 3 (consumer): upgrade dormant serving rows from the unified
    `model_classification` side-channel table (produced by 12_classify_models, which
    classifies by artifact → name regex → LLM → compute_type).

    Decoupled + fail-open + upgrade-only: no-op if the table is absent/empty, never
    overrides a high-confidence task/tile/feedback verdict, and only touches the dormant
    `unknown` (and behavioral-medium) serving customs. Carries `subtype` and records the
    deciding tier in `classified_by` (`artifact` | `name_regex` | `name_llm` |
    `compute_type`). Mutates `agents` in place.
    """
    table = f"{CATALOG}.{SCHEMA}.model_classification"
    try:
        if not spark.catalog.tableExists(table):
            print("  Stage 3: model_classification not present yet (no-op)")
            return
        rows = spark.sql(f"""
            SELECT model_full_name, workload_class, subtype, framework, confidence, source_signal
            FROM {table}
            WHERE workload_class IS NOT NULL AND workload_class != 'unknown'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY model_full_name
                ORDER BY CASE confidence WHEN 'high' THEN 0 WHEN 'med' THEN 1 ELSE 2 END
            ) = 1
        """).collect()
    except Exception as e:
        print(f"  Stage 3 model_classification unavailable (fail-open): {e}")
        return

    lookup = {r.model_full_name: (r.workload_class, r.subtype, r.framework, r.confidence, r.source_signal)
              for r in rows}
    if not lookup:
        return

    upgraded = 0
    for a in agents:
        # Don't override settled high-confidence verdicts (task/tile/feedback/behavior).
        if a.get("confidence") == "high" and (
                a.get("classified_by") in ("task", "tile_metadata", "feedback_sibling")
                or str(a.get("classified_by") or "").startswith("behavior")):
            continue
        hit = lookup.get(a.get("model_name") or "")
        if not hit:
            continue
        wc, sub, fw, conf, src = hit
        if wc not in ("genai_agent", "llm", "multimodal", "traditional_ml", "deep_learning"):
            continue
        cur = a.get("workload_class")
        if src in _ARTIFACT_SIGNALS:
            # Artifact is authoritative — may refine the dormant tail + behavioral-medium customs.
            if cur not in ("unknown", "llm", "traditional_ml", "multimodal"):
                continue
        else:
            # Name/LLM/compute_type are heuristic — only fill genuinely-`unknown` rows; never
            # downgrade a Stage-1/2 verdict (e.g. a behavioral llm/high to traditional_ml/low).
            if cur != "unknown":
                continue
        a["workload_class"] = wc
        if wc == "genai_agent":
            # LLM flagged this empty-task model as an agent; its ML `subtype` doesn't apply.
            a["type"], a["subtype"] = "custom_agent", None
        else:
            a["type"], a["subtype"] = _workload_to_type(wc, None), (sub or a.get("subtype"))
        a["framework"] = fw or a.get("framework")
        a["confidence"] = conf or "low"
        a["classified_by"] = "artifact" if src in _ARTIFACT_SIGNALS else (src or "artifact")
        if wc in ("llm", "multimodal"):
            a["uses_llm"] = True
        upgraded += 1
    print(f"  Stage 3: upgraded {upgraded} rows ({len(lookup)} classified models in table)")


def discover_genie_from_audit_logs() -> List[Dict[str, Any]]:
    """Discover live Genie Spaces across workspaces via audit logs.

    Excludes spaces whose latest lifecycle event is a trash/delete (soft-deleted
    and not restored). Genie logs no explicit restore action, so a space coming
    back out of Trash shows up only as a later create/update — hence
    *latest-event-wins*: a space is dropped only when its last trash is at or
    after its last create/update. This is re-evaluated every run, so it tracks
    deletions (and restores) live at the discovery cadence. Spaces with zero
    conversations (created but unused, yet live) are intentionally kept. Bounded
    to the last 90 days of create/update activity (same window as before)."""
    agents = []
    try:
        rows = spark.sql("""
            WITH space_meta AS (
                SELECT
                    COALESCE(
                        request_params['space_id'],
                        get_json_object(response.result, '$.space_id')
                    ) AS space_id,
                    request_params['display_name'] AS display_name,
                    request_params['table_identifiers'] AS table_ids,
                    request_params['warehouse_id'] AS warehouse_id,
                    workspace_id,
                    user_identity.email AS creator_email,
                    event_time,
                    -- Latest create/update time per space (same scan, no extra pass).
                    -- Compared against the last trash below for latest-event-wins.
                    MAX(event_time) OVER (
                        PARTITION BY COALESCE(
                            request_params['space_id'],
                            get_json_object(response.result, '$.space_id')
                        )
                    ) AS last_cu,
                    ROW_NUMBER() OVER (
                        PARTITION BY COALESCE(
                            request_params['space_id'],
                            get_json_object(response.result, '$.space_id')
                        )
                        ORDER BY
                            CASE WHEN request_params['display_name'] IS NOT NULL THEN 0 ELSE 1 END,
                            event_time DESC
                    ) AS rn
                FROM system.access.audit
                WHERE service_name IN ('aibiGenie', 'genie')
                  AND action_name IN ('createSpace', 'genieCreateSpace', 'updateSpace', 'genieUpdateSpace')
                  AND event_date >= CURRENT_DATE - 90
                  AND response.status_code = '200'
            ),
            usage AS (
                SELECT
                    request_params['space_id'] AS space_id,
                    COUNT(*) AS conversation_count,
                    COUNT(DISTINCT user_identity.email) AS unique_users,
                    MAX(event_time) AS last_active
                FROM system.access.audit
                WHERE service_name IN ('aibiGenie', 'genie')
                  AND action_name IN ('createConversation', 'createConversationMessage', 'genieCreateConversationMessage')
                  AND event_date >= CURRENT_DATE - 90
                  AND request_params['space_id'] IS NOT NULL
                GROUP BY 1
            ),
            -- Latest trash/delete per space. Latest-event-wins: a space is excluded
            -- only when its last trash is at/after its last create/update (still in
            -- Trash); a restore+edit after a trash keeps it (last_cu wins).
            trashed AS (
                SELECT
                    request_params['space_id'] AS space_id,
                    MAX(event_time) AS last_trash
                FROM system.access.audit
                WHERE service_name IN ('aibiGenie', 'genie')
                  AND action_name IN ('genieTrashSpace', 'trashSpace')
                  AND event_date >= CURRENT_DATE - 90
                  AND response.status_code = '200'
                  AND request_params['space_id'] IS NOT NULL
                GROUP BY 1
            )
            SELECT
                m.space_id, m.display_name, m.table_ids, m.warehouse_id,
                m.workspace_id, m.creator_email AS creator,
                u.conversation_count, u.unique_users, u.last_active
            FROM space_meta m
            LEFT JOIN usage u ON m.space_id = u.space_id
            LEFT JOIN trashed t ON m.space_id = t.space_id
            WHERE m.rn = 1 AND m.space_id IS NOT NULL AND LENGTH(m.space_id) > 10
              AND (t.last_trash IS NULL OR t.last_trash < m.last_cu)
            ORDER BY COALESCE(u.last_active, m.event_time) DESC
        """).collect()

        for r in rows:
            space_id = str(r.space_id or "")
            ws = str(r.workspace_id or "")
            if not space_id:
                continue

            display_name = r.display_name or ""
            name = display_name or f"Genie Space {space_id[:8]}"
            conv_count = int(r.conversation_count or 0)
            unique_users = int(r.unique_users or 0)

            config_dict = {"space_id": space_id, "deployment_type": "genie_space"}
            if r.table_ids:
                try:
                    config_dict["tables"] = json.loads(r.table_ids)
                except (json.JSONDecodeError, TypeError):
                    config_dict["tables"] = [t.strip().strip('"') for t in r.table_ids.split(",") if t.strip()]
            if r.warehouse_id:
                config_dict["warehouse_id"] = r.warehouse_id
            if conv_count:
                config_dict["conversation_count"] = conv_count
            if unique_users:
                config_dict["unique_users"] = unique_users

            # Use same canonical key as Genie API source for dedup
            agents.append({
                "agent_id": _make_id(f"genie:{space_id}", ws),
                "workspace_id": ws,
                "name": name,
                "type": "genie_space",
                "endpoint_name": space_id,
                "endpoint_status": "ACTIVE" if conv_count > 0 else "DISCOVERED",
                "model_name": "",
                "served_entity_name": "",
                "creator": str(r.creator or ""),
                "description": f"{conv_count} conversations, {unique_users} users" if conv_count else "",
                "config": json.dumps(config_dict),
                "source": "audit_log",
                "is_extensive": True,
            })

        print(f"  Genie Spaces (audit logs): {len(agents)}")
    except Exception as e:
        print(f"  ERROR querying audit logs for Genie: {e}")
    return agents


def discover_apps_from_audit_logs() -> List[Dict[str, Any]]:
    """Discover app-served agents across all workspaces via audit logs.

    Account-wide replacement for the per-workspace SP fan-out
    (`discover_apps_cross_workspace`), which needs the discovery SP registered as
    an OAuth client in every workspace and otherwise yields zero cross-workspace
    coverage. The `apps` service logs to `system.access.audit` account-wide, and
    the create/update actions carry the full app spec in `request_params['app']`
    — including `resources[].serving_endpoint`, the same endpoint link the live
    `GET /api/2.0/apps` exposed. So apps are discovered + classified account-wide
    with no fan-out and no SP propagation (same pattern as the Genie audit source).

    Scope: apps whose latest create/update spec declares a `serving_endpoint` OR an
    MLflow `experiment` resource (agent candidates). `resolve_apps()` scores the class
    afterward via the multi-signal taxonomy — fronting a `genai_agent` →
    `agent_frontend_app`; an experiment under agent-eval monitoring or source-detected
    frameworks → `agent_app`; structural signals (multi-endpoint, Genie/job/vector
    tools) → `candidate_agent_app`; a lone plain LLM with no other signal →
    `ai_enabled_app`.

    Lifecycle: latest-event-wins on `deleteApp` — an app is excluded only when its
    last delete is at/after its last create/update (same rule as Genie trash).
    Spec actions carry `request_params['app']` (full JSON); `deleteApp` carries
    `request_params['name']`.

    Window: bounded to the last 90 days of create/update activity — a *cross-workspace*
    app untouched for >90d drops out (the known retention-window limitation the persistent
    inventory plan fixes; not a regression — the fan-out this replaces had zero cross-ws
    coverage). Local-workspace apps are unaffected: `discover_apps()` still enumerates them
    live via `GET /api/2.0/apps` every run.
    """
    agents = []
    try:
        rows = spark.sql("""
            WITH app_spec AS (
                SELECT
                    workspace_id,
                    get_json_object(request_params['app'], '$.name') AS app_name,
                    request_params['app'] AS spec,
                    user_identity.email AS creator,
                    event_time,
                    -- Latest create/update time per app (same scan), compared to the
                    -- last delete below for latest-event-wins.
                    MAX(event_time) OVER (
                        PARTITION BY workspace_id, get_json_object(request_params['app'], '$.name')
                    ) AS last_cu,
                    ROW_NUMBER() OVER (
                        PARTITION BY workspace_id, get_json_object(request_params['app'], '$.name')
                        ORDER BY event_time DESC
                    ) AS rn
                FROM system.access.audit
                WHERE service_name = 'apps'
                  AND action_name IN ('createApp', 'createUpdate', 'updateApp')
                  AND event_date >= CURRENT_DATE - 90
                  AND response.status_code = '200'
                  AND get_json_object(request_params['app'], '$.name') IS NOT NULL
                  AND get_json_object(request_params['app'], '$.name') != ''
                -- NB: the resource filter is applied in the OUTER query on the rn=1
                -- (latest) spec — filtering here would corrupt ROW_NUMBER, letting an
                -- older spec win when the newest update removed the resource.
            ),
            -- Latest delete per app. Latest-event-wins: an app is excluded only when
            -- its last delete is at/after its last create/update (a later redeploy /
            -- spec edit keeps it, mirroring the Genie trash rule). deleteApp keys the
            -- app via request_params['name'].
            trashed AS (
                SELECT
                    workspace_id,
                    request_params['name'] AS app_name,
                    MAX(event_time) AS last_trash
                FROM system.access.audit
                WHERE service_name = 'apps'
                  AND action_name = 'deleteApp'
                  AND event_date >= CURRENT_DATE - 90
                  AND response.status_code = '200'
                  AND request_params['name'] IS NOT NULL
                GROUP BY 1, 2
            )
            SELECT s.workspace_id, s.app_name, s.spec, s.creator, s.event_time
            FROM app_spec s
            LEFT JOIN trashed t
              ON s.workspace_id = t.workspace_id AND s.app_name = t.app_name
            WHERE s.rn = 1
              AND (t.last_trash IS NULL OR t.last_trash < s.last_cu)
              -- Keep only if the LATEST spec still declares a serving_endpoint / experiment
              -- resource (applied post-ROW_NUMBER so a stale older spec can't slip through).
              AND (s.spec LIKE '%serving_endpoint%' OR s.spec LIKE '%experiment_id%')
            ORDER BY s.event_time DESC
        """).collect()

        for r in rows:
            ws = str(r.workspace_id or "")
            name = str(r.app_name or "")
            if not name:
                continue

            # Parse the app spec → resources + multi-signal features (shared parser).
            try:
                spec = json.loads(r.spec) if r.spec else {}
            except (json.JSONDecodeError, TypeError):
                spec = {}
            resources_out, app_features = _parse_app_resources(spec.get("resources") or [])
            resource_endpoint = app_features["endpoint_names"][0] if app_features["endpoint_names"] else ""

            config_dict = {
                "url": "",
                "app_id": "",
                "deployment_type": "databricks_app",
                "deployment_method": "app",
                "sp_client_id": "",
                "resources": resources_out,
                "app_features": app_features,
            }
            if resource_endpoint:
                config_dict["linked_endpoint"] = resource_endpoint

            # Candidate app record — classified "unknown" here; resolve_apps() promotes
            # it to agent_frontend_app when the linked endpoint is a genai_agent.
            agents.append({
                "agent_id": _make_id(name, ws),
                "workspace_id": ws,
                "name": name,
                "type": _app_class_to_type("unknown"),
                "endpoint_name": name,
                "endpoint_status": "ACTIVE",
                "model_name": "",
                "served_entity_name": "",
                "creator": str(r.creator or ""),
                "description": "",
                "config": json.dumps(config_dict),
                "source": "apps_audit",
                "is_extensive": True,
                "workload_class": "unknown",
                "subtype": None,
                "framework": None,
                "interface_task": None,
                "uses_llm": None,
                "linked_endpoint": resource_endpoint or None,
                "confidence": "low",
                "classified_by": "insufficient_signal",
                "classifier_version": _CLASSIFIER_VERSION,
                "raw_signals": json.dumps({"source_readable": False, "from": "apps_audit"}),
            })

        print(f"  App-served agents (audit logs): {len(agents)} candidate apps w/ serving-endpoint links")
    except Exception as e:
        print(f"  ERROR querying audit logs for apps: {e}")
    return agents

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-Workspace App Discovery (API Enumeration — legacy, replaced by audit logs)

# COMMAND ----------

def _get_all_workspace_hosts() -> Dict[str, str]:
    """Resolve all workspace hosts via the Accounts API.

    Tries the notebook context token first, then falls back to SP M2M
    credentials against the Accounts API.  Returns {workspace_id: host_url}.
    """
    ws_hosts: Dict[str, str] = {}

    account_id = ""
    try:
        account_id = spark.conf.get("spark.databricks.clusterUsageTags.accountId", "")
    except Exception:
        pass

    if not account_id:
        print("  WARNING: Could not determine account ID from Spark config")
        return ws_hosts

    def _parse_workspaces(data) -> Dict[str, str]:
        hosts: Dict[str, str] = {}
        for ws in data:
            ws_id_str = str(ws.get("workspace_id", ""))
            dep_name = ws.get("deployment_name", "")
            ws_url = ws.get("workspace_url", "")
            host = f"https://{dep_name}.cloud.databricks.com" if dep_name else ws_url
            if ws_id_str and host:
                hosts[ws_id_str] = host
        return hosts

    # Strategy 1: Notebook context token → Accounts API
    try:
        acct_resp = httpx.get(
            f"https://accounts.cloud.databricks.com/api/2.0/accounts/{account_id}/workspaces",
            headers=_get_headers(),
            timeout=30,
        )
        if acct_resp.status_code == 200:
            ws_hosts = _parse_workspaces(acct_resp.json())
            if ws_hosts:
                return ws_hosts
    except Exception:
        pass

    # Strategy 2: SP M2M credentials → Accounts API
    client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "")
    if client_id and client_secret:
        try:
            token_resp = httpx.post(
                "https://accounts.cloud.databricks.com/oidc/v1/token",
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "scope": "all-apis",
                },
                timeout=30,
            )
            if token_resp.status_code == 200:
                sp_token = token_resp.json().get("access_token", "")
                if sp_token:
                    acct_resp = httpx.get(
                        f"https://accounts.cloud.databricks.com/api/2.0/accounts/{account_id}/workspaces",
                        headers={"Authorization": f"Bearer {sp_token}"},
                        timeout=30,
                    )
                    if acct_resp.status_code == 200:
                        ws_hosts = _parse_workspaces(acct_resp.json())
        except Exception as e:
            print(f"  WARNING: SP Accounts API fallback failed: {e}")

    return ws_hosts


def _get_sp_token_for_host(host: str) -> Optional[str]:
    """Exchange SP M2M credentials for a token on a remote workspace."""
    client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "")
    if not client_id or not client_secret:
        return None
    try:
        resp = httpx.post(
            f"{host}/oidc/v1/token",
            data={
                "grant_type": "client_credentials",
                "client_id": client_id,
                "client_secret": client_secret,
                "scope": "all-apis",
            },
            timeout=30,
        )
        if resp.status_code == 200:
            return resp.json().get("access_token", "")
    except Exception as exc:
        print(f"  WARNING: SP token exchange for {host} failed: {exc}")
    return None


def _fetch_apps_for_workspace(
    ws_id: str, host: str, token: str,
) -> List[Dict[str, Any]]:
    """Fetch apps from a single remote workspace and filter to agent-serving apps."""
    agents: List[Dict[str, Any]] = []
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    next_page_token: Optional[str] = None

    try:
        while True:
            params: Dict[str, Any] = {"page_size": 100}
            if next_page_token:
                params["page_token"] = next_page_token

            resp = httpx.get(
                f"{host}/api/2.0/apps",
                headers=headers,
                params=params,
                timeout=_REST_TIMEOUT,
            )
            if resp.status_code != 200:
                break

            body = resp.json()
            for app in body.get("apps", []):
                # Skip remote source reads (slow + usually ACL-blocked); cross-ws apps
                # rely on endpoint-link (resource) + behavior in resolve_apps().
                agents.append(_build_app_record(app, ws_id, "cross_workspace_api", host,
                                                headers, read_source=False))

            next_page_token = body.get("next_page_token")
            if not next_page_token:
                break

    except Exception as exc:
        print(f"  WARNING: Failed to fetch apps from {host}: {exc}")

    return agents


def discover_apps_cross_workspace() -> List[Dict[str, Any]]:
    """Discover agent-serving apps across all workspaces via direct API enumeration.

    1. Resolve all workspace hosts via Accounts API
    2. SP M2M token exchange + GET /api/2.0/apps on each workspace (10 concurrent)
    3. Filter to apps with serving_endpoint resources

    With 10 concurrent workers, ~736 workspaces completes in ~1-2 min.
    """
    # Step 1: Get all workspace hosts
    all_ws_hosts = _get_all_workspace_hosts()
    if not all_ws_hosts:
        print("  Cross-workspace apps: no workspace hosts resolved — skipping")
        return []

    # Exclude local workspace (already covered by discover_apps())
    local_ws_id = _get_workspace_id()
    ws_hosts = {ws: host for ws, host in all_ws_hosts.items() if ws != local_ws_id}

    if not ws_hosts:
        print("  Cross-workspace apps: only local workspace found — skipping")
        return []

    print(f"  Cross-workspace apps: enumerating {len(ws_hosts)} workspaces …")

    # Step 2: Parallel SP M2M token exchange + app enumeration
    all_agents: List[Dict[str, Any]] = []
    success_count = 0
    fail_count = 0

    def _process_workspace(ws_id: str) -> Tuple[List[Dict[str, Any]], bool]:
        host = ws_hosts[ws_id]
        token = _get_sp_token_for_host(host)
        if not token:
            return ([], False)
        return (_fetch_apps_for_workspace(ws_id, host, token), True)

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(_process_workspace, ws): ws for ws in ws_hosts}
        for future in as_completed(futures):
            try:
                agents, ok = future.result()
                all_agents.extend(agents)
                if ok:
                    success_count += 1
                else:
                    fail_count += 1
            except Exception:
                fail_count += 1

    print(f"  Cross-workspace apps: {len(all_agents)} agent apps from {success_count}/{len(ws_hosts)} workspaces ({fail_count} failed)")
    return all_agents

# COMMAND ----------

# MAGIC %md
# MAGIC ## Run Discovery & Write to Delta

# COMMAND ----------

from pyspark.sql import Row

now = datetime.now(timezone.utc)
print(f"Starting agent discovery at {now.isoformat()}")
print("=" * 60)

# Run all discovery sources
all_agents = []
all_agents.extend(discover_serving_endpoints())
all_agents.extend(discover_apps())
# Cross-workspace apps via account-wide audit logs (replaces the per-workspace SP
# fan-out, which yields ~0 coverage unless the SP is propagated to every workspace).
# discover_apps_cross_workspace() is kept defined for optional propagated-SP use.
all_agents.extend(discover_apps_from_audit_logs())
all_agents.extend(discover_genie_spaces())
all_agents.extend(discover_from_system_tables())
all_agents.extend(discover_genie_from_audit_logs())

print("=" * 60)
print(f"Total discovered: {len(all_agents)} agents")

# Stage 2: refine empty-task CUSTOM_MODEL `unknown` rows from runtime behavior.
enrich_unknown_with_behavior(all_agents)

# Apps: finalize endpoint-frontend links + app-SP behavioral (after serving
# classification so the genai_agent set is complete).
resolve_apps(all_agents)

# Stage 3: upgrade dormant `unknown` serving rows from the unified model_classification
# side-channel (artifact → name regex → LLM → compute_type). Fail-open, no-op until
# 12_classify_models has populated it.
apply_model_classification(all_agents)

# Pre-filter classification distribution (the Stage-1 flip diff vs the old taxonomy)
from collections import Counter as _Counter
_pre = _Counter((a.get("workload_class") or a.get("type") or "?") for a in all_agents)
print("Workload-class distribution (pre-filter):")
for _cls, _n in sorted(_pre.items(), key=lambda kv: -kv[1]):
    print(f"  {_n:6d}  {_cls}")

# Persist agents + the `unknown` review queue; drop definitively-non-agent serving.
all_agents = [a for a in all_agents if a.get("type") in _PERSISTED_TYPES]
print(f"After persist filter: {len(all_agents)} rows")

# Deduplicate: prefer API > system_table > audit_log sources.
# For agents with the same agent_id, keep the best source.
# Also deduplicate by (name, workspace_id) keeping the best source.
_SOURCE_PRIORITY = {"api": 0, "user_api": 1, "cross_workspace_api": 2, "system_table": 3, "audit_log": 4, "apps_audit": 5}

def _source_rank(a):
    return _SOURCE_PRIORITY.get(a.get("source", ""), 99)

_GENIE_USAGE_KEYS = ("conversation_count", "unique_users", "tables", "warehouse_id")

def _merge_genie_usage(keep, drop):
    """Carry audit-log usage stats onto the Genie row that wins dedup.

    The Genie list API has the real title but no conversation/user counts;
    the audit-log row has the counts but only a placeholder name.
    """
    if keep.get("type") != "genie_space" or drop.get("type") != "genie_space":
        return
    try:
        keep_cfg = json.loads(keep.get("config") or "{}")
        drop_cfg = json.loads(drop.get("config") or "{}")
    except (json.JSONDecodeError, TypeError):
        return
    for k in _GENIE_USAGE_KEYS:
        if k in drop_cfg and k not in keep_cfg:
            keep_cfg[k] = drop_cfg[k]
    keep["config"] = json.dumps(keep_cfg)
    if not keep.get("description") and drop.get("description"):
        keep["description"] = drop["description"]

# First pass: dedup by agent_id (keeps best source)
by_id = {}
for a in all_agents:
    aid = a["agent_id"]
    prev = by_id.get(aid)
    if prev is None:
        by_id[aid] = a
    elif _source_rank(a) < _source_rank(prev):
        _merge_genie_usage(a, prev)
        by_id[aid] = a
    else:
        _merge_genie_usage(prev, a)

# Second pass: dedup by (name, workspace_id) keeping best source.
# This eliminates duplicate names across discovery sources (e.g. an agent
# found via both API and system_table).
by_name_ws = {}
for a in by_id.values():
    key = (a["name"], a["workspace_id"])
    if key not in by_name_ws or _source_rank(a) < _source_rank(by_name_ws[key]):
        by_name_ws[key] = a

# Third pass: dedup by (endpoint_name, workspace_id) for serving endpoints.
# Same endpoint discovered via API + system_table may have different names
# but the same endpoint_name — prefer the API source (precise tile metadata).
by_endpoint_ws = {}
final = []
for a in by_name_ws.values():
    ep = a.get("endpoint_name", "")
    ws = a.get("workspace_id", "")
    if ep and a.get("type") not in ("genie_space", "custom_app"):
        key = (ep, ws)
        if key not in by_endpoint_ws or _source_rank(a) < _source_rank(by_endpoint_ws[key]):
            by_endpoint_ws[key] = a
    else:
        final.append(a)

final.extend(by_endpoint_ws.values())
deduped = final
print(f"After dedup: {len(deduped)} unique agents (from {len(all_agents)} raw)")

# Add discovery timestamp + normalize classification columns so every row carries
# the full schema (serving rows already set these; others get derived defaults).
for a in deduped:
    a["discovered_at"] = now
    a.setdefault("workload_class",
                 _TYPE_TO_CLASS_FALLBACK.get(a.get("type", ""), a.get("type", "")))
    a.setdefault("subtype", None)
    a.setdefault("framework", None)
    a.setdefault("interface_task", None)
    a.setdefault("uses_llm", None)
    a.setdefault("linked_endpoint", None)
    a.setdefault("confidence", None)
    a.setdefault("classified_by", None)
    a.setdefault("classifier_version", _CLASSIFIER_VERSION)
    a.setdefault("raw_signals", None)

# COMMAND ----------

# Convert to Spark DataFrame and write as Delta
if deduped:
    # Build rows as tuples in exact schema order (positional, length-safe) — robust
    # to dict key order / missing optional fields across heterogeneous producers.
    _field_names = [f.name for f in DISCOVERED_AGENTS_SCHEMA.fields]
    rows = [tuple(a.get(fn) for fn in _field_names) for a in deduped]
    df = spark.createDataFrame(rows, schema=DISCOVERED_AGENTS_SCHEMA)

    # Overwrite the table each run — this is the latest snapshot
    df.write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(DELTA_TABLE)

    print(f"Wrote {df.count()} agents to {DELTA_TABLE}")
    display(df.groupBy("workload_class", "type", "source").count()
              .orderBy("workload_class", "type", "source"))
else:
    print("No agents discovered — skipping write")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary

# COMMAND ----------

if spark.catalog.tableExists(DELTA_TABLE):
    summary = spark.sql(f"""
        SELECT
            workload_class,
            type,
            source,
            COUNT(*) as agent_count,
            COUNT(DISTINCT workspace_id) as workspace_count,
            MIN(discovered_at) as earliest,
            MAX(discovered_at) as latest
        FROM {DELTA_TABLE}
        GROUP BY workload_class, type, source
        ORDER BY agent_count DESC
    """)
    display(summary)
