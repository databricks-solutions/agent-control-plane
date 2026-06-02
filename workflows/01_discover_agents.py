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
    StructField("workload_class", StringType(), True),   # genai_agent | genai_model | embedding_model | foundation_model | external_model | feature_serving | traditional_ml | agent_app | agent_frontend_app | ai_enabled_app | regular_app | genie_space | unknown
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
# (foundation_model, embedding_model, genai_model, feature_serving) are not
# persisted here. Once Stage 2/3 land, `unknown` rows resolve in place.
_PERSISTED_TYPES = _VALID_AGENT_TYPES + ("unknown",)

# Back-compat `type` (from non-serving producers) → workload_class, so the new
# column is populated everywhere. Apps are refined to their precise class in the
# apps step; for now custom_app maps to agent_app.
_TYPE_TO_CLASS_FALLBACK = {
    "custom_agent": "genai_agent",
    "knowledge_assistant": "genai_agent",
    "multi_agent_supervisor": "genai_agent",
    "custom_llm": "genai_agent",
    "information_extraction": "genai_agent",
    "external_agent": "external_model",
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
    "external_model": "external_agent",
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
        return ("external_model", None, tk, "high", "entity_type")

    # Agent task (prefix — covers agent/v1/responses, agent/v1/chat, agent/v2/chat)
    if tk.startswith(_AGENT_TASK_PREFIX):
        return ("genai_agent", None, tk, "high", "task")

    if tk in _EMBEDDING_TASKS:
        return ("embedding_model", None, tk, "high", "task")
    if any(tk.startswith(p) for p in _LLM_TASK_PREFIXES):
        cls = "foundation_model" if et == "FOUNDATION_MODEL" else "genai_model"
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


def _get_workspace_id() -> str:
    """Get the numeric workspace ID from the notebook context."""
    try:
        ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        org_id = ctx.tags().get("orgId").getOrElse(lambda: "")
        if org_id:
            return str(org_id)
    except Exception:
        pass
    # Fallback: hash the host
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


def discover_apps() -> List[Dict[str, Any]]:
    """Discover Databricks Apps via REST API."""
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
                name = app.get("name", "")

                # Status: prefer compute_status over deployment status
                cs = app.get("compute_status") or {}
                status = cs.get("state", "")
                if not status:
                    active = app.get("active_deployment") or {}
                    dep_status = active.get("status") or {}
                    status = dep_status.get("state", "")

                url = app.get("url", "")
                description = app.get("description", "")
                raw_creator = app.get("creator", "")
                creator = raw_creator.get("username", "") if isinstance(raw_creator, dict) else str(raw_creator or "")

                config_dict = {
                    "url": url,
                    "app_id": app.get("id", ""),
                    "compute_size": app.get("compute_size", ""),
                    "deployment_type": "databricks_app",
                    "deployment_method": "app",
                }

                resources = app.get("resources") or []
                if resources:
                    config_dict["resources"] = []
                    for r in resources:
                        r_entry = {"name": r.get("name", ""), "description": r.get("description", "")}
                        for rtype in ("serving_endpoint", "sql_warehouse", "experiment", "secret"):
                            if rtype in r:
                                r_entry["type"] = rtype
                                if rtype == "serving_endpoint":
                                    nested = r.get("serving_endpoint") or {}
                                    if isinstance(nested, dict) and nested.get("name"):
                                        r_entry["endpoint_name"] = nested["name"]
                                break
                        config_dict["resources"].append(r_entry)

                # Only include apps that have a serving_endpoint resource
                if not any(r.get("type") == "serving_endpoint" for r in config_dict.get("resources", [])):
                    continue

                agents.append({
                    "agent_id": _make_id(name, ws_id),
                    "workspace_id": ws_id,
                    "name": name,
                    "type": "custom_app",
                    "endpoint_name": name,
                    "endpoint_status": status,
                    "model_name": "",
                    "served_entity_name": "",
                    "creator": creator,
                    "description": description,
                    "config": json.dumps(config_dict),
                    "source": "api",
                    "is_extensive": False,
                })

            next_page_token = body.get("next_page_token")
            if not next_page_token:
                break

        print(f"  Databricks Apps: {len(agents)} agent apps")
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

            # Non-agent serving (plain foundation / embedding / custom LLM / feature
            # specs) is not part of the agent inventory — skip from this account-wide
            # source (consistent with prior behavior of skipping foundation models).
            if workload_class in ("foundation_model", "embedding_model",
                                  "genai_model", "feature_serving"):
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
      - emits tokens / streams                                       → genai_model (uses_llm)
      - traffic but zero tokens                                      → traditional_ml
      - no traffic in window                                         → stays unknown

    Behavior proves "uses an LLM", not "is an agent" (the dashboard lesson), so token
    emission yields genai_model — not genai_agent — unless the feedback marker fires.
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

    counts = {"genai_agent": 0, "genai_model": 0, "traditional_ml": 0, "unknown": 0}
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
            a.update(workload_class="genai_model", type="genai_model",
                     uses_llm=True, confidence="high", classified_by="behavior_tokens")
            counts["genai_model"] += 1
        else:
            a.update(workload_class="traditional_ml", type="traditional_ml",
                     uses_llm=False, confidence="med", classified_by="behavior_no_tokens")
            counts["traditional_ml"] += 1
    print(f"  Stage 2 behavioral: refined {len(unknown)} unknown → {counts}")


def discover_genie_from_audit_logs() -> List[Dict[str, Any]]:
    """Discover Genie Spaces across workspaces via audit logs."""
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
            )
            SELECT
                m.space_id, m.display_name, m.table_ids, m.warehouse_id,
                m.workspace_id, m.creator_email AS creator,
                u.conversation_count, u.unique_users, u.last_active
            FROM space_meta m
            LEFT JOIN usage u ON m.space_id = u.space_id
            WHERE m.rn = 1 AND m.space_id IS NOT NULL AND LENGTH(m.space_id) > 10
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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cross-Workspace App Discovery (API Enumeration)

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
                name = app.get("name", "")

                # Status
                cs = app.get("compute_status") or {}
                status = cs.get("state", "")
                if not status:
                    active = app.get("active_deployment") or {}
                    dep_status = active.get("status") or {}
                    status = dep_status.get("state", "")

                url = app.get("url", "")
                description = app.get("description", "")
                raw_creator = app.get("creator", "")
                creator = raw_creator.get("username", "") if isinstance(raw_creator, dict) else str(raw_creator or "")

                config_dict: Dict[str, Any] = {
                    "url": url,
                    "app_id": app.get("id", ""),
                    "compute_size": app.get("compute_size", ""),
                    "deployment_type": "databricks_app",
                    "deployment_method": "app",
                }

                resources = app.get("resources") or []
                if resources:
                    config_dict["resources"] = []
                    for r in resources:
                        r_entry: Dict[str, Any] = {"name": r.get("name", ""), "description": r.get("description", "")}
                        for rtype in ("serving_endpoint", "sql_warehouse", "experiment", "secret"):
                            if rtype in r:
                                r_entry["type"] = rtype
                                if rtype == "serving_endpoint":
                                    nested = r.get("serving_endpoint") or {}
                                    if isinstance(nested, dict) and nested.get("name"):
                                        r_entry["endpoint_name"] = nested["name"]
                                break
                        config_dict["resources"].append(r_entry)

                # Only include apps that have a serving_endpoint resource
                if not any(r.get("type") == "serving_endpoint" for r in config_dict.get("resources", [])):
                    continue

                agents.append({
                    "agent_id": _make_id(name, ws_id),
                    "workspace_id": ws_id,
                    "name": name,
                    "type": "custom_app",
                    "endpoint_name": name,
                    "endpoint_status": status,
                    "model_name": "",
                    "served_entity_name": "",
                    "creator": creator,
                    "description": description,
                    "config": json.dumps(config_dict),
                    "source": "cross_workspace_api",
                    "is_extensive": True,
                })

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
all_agents.extend(discover_apps_cross_workspace())
all_agents.extend(discover_genie_spaces())
all_agents.extend(discover_from_system_tables())
all_agents.extend(discover_genie_from_audit_logs())

print("=" * 60)
print(f"Total discovered: {len(all_agents)} agents")

# Stage 2: refine empty-task CUSTOM_MODEL `unknown` rows from runtime behavior.
enrich_unknown_with_behavior(all_agents)

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
_SOURCE_PRIORITY = {"api": 0, "user_api": 1, "cross_workspace_api": 2, "system_table": 3, "audit_log": 4}

def _source_rank(a):
    return _SOURCE_PRIORITY.get(a.get("source", ""), 99)

# First pass: dedup by agent_id (keeps best source)
by_id = {}
for a in all_agents:
    aid = a["agent_id"]
    if aid not in by_id or _source_rank(a) < _source_rank(by_id[aid]):
        by_id[aid] = a

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
    if ep and a.get("type") != "genie_space" and a.get("type") != "custom_app":
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
    rows = [Row(**a) for a in deduped]
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
