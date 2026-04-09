"""Cross-workspace agent discovery — live API + system tables, cached in Lakebase.

Architecture:
  • Live API discovery (current workspace): uses WorkspaceClient to list
    serving endpoints (custom + external agents only), Databricks Apps,
    Genie Spaces, and Agent Bricks agents (KA/MAS/CustomLLM/IE via endpoint patterns).
  • System tables discovery (all workspaces): queries
    system.serving.served_entities for CUSTOM_MODEL and EXTERNAL_MODEL
    entities only (including KA/MAS endpoint patterns).
  • Foundation models, feature specs, and embedding endpoints are excluded.
  • Results are cached in a Lakebase ``discovered_agents`` table.
  • A background thread refreshes when data is >1 h stale.

Agent type taxonomy
-------------------
  ``custom_agent``           – Custom Agents on Serving Endpoint or Databricks App
  ``custom_llm``             – Agent Bricks Custom LLM (tile problem_type=CUSTOM_LLM)
  ``external_agent``         – External Agents via MCP or external model
  ``genie_space``            – Genie data rooms / spaces
  ``information_extraction`` – Agent Bricks IE (tile problem_type=INFORMATION_EXTRACTION)
  ``knowledge_assistant``    – Agent Bricks Knowledge Assistants (KA)
  ``multi_agent_supervisor`` – Agent Bricks Multi-Agent Supervisors (MAS)
"""
from __future__ import annotations

import hashlib
import httpx
import json
import os
import traceback
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from backend.config import (
    _get_workspace_client,
    get_databricks_host,
    get_databricks_headers,
)
from backend.database import execute_query, execute_one, execute_update, DatabasePool

# Re-use the SQL-warehouse execution helper from billing_service
from backend.services.billing_service import _execute_system_sql, _find_warehouse_id

import logging

logger = logging.getLogger(__name__)

_REST_TIMEOUT = 30.0

_STALE_SECONDS = 3600  # 1 hour

# The only agent types we surface
_VALID_AGENT_TYPES = (
    "custom_agent",
    "custom_app",
    "custom_llm",
    "external_agent",
    "genie_space",
    "information_extraction",
    "knowledge_assistant",
    "multi_agent_supervisor",
)

# Module-level cache for the current workspace numeric ID
_current_workspace_id: Optional[str] = None

# Maps tile_endpoint_metadata.problem_type → agent type taxonomy
_TILE_PROBLEM_TYPE_MAP: dict = {
    "KNOWLEDGE_ASSISTANT": "knowledge_assistant",
    "MULTI_AGENT_SUPERVISOR": "multi_agent_supervisor",
    "CUSTOM_LLM": "custom_llm",
    "INFORMATION_EXTRACTION": "information_extraction",
}

# Regex for identifying KA/MAS endpoints by name
import re
_KA_ENDPOINT_RE = re.compile(r"^ka-[a-f0-9]+-endpoint$", re.IGNORECASE)
_MAS_ENDPOINT_RE = re.compile(r"^mas-[a-f0-9]+-endpoint$", re.IGNORECASE)

def _get_current_workspace_id() -> str:
    """Return the numeric workspace ID of the current workspace.

    Calls GET /api/2.0/token/list (HEAD request) to read the
    ``x-databricks-org-id`` response header.  Falls back to a
    hash of the host URL if the header is unavailable.

    The result is cached in the module-level ``_current_workspace_id``
    variable so the HTTP round-trip only happens once per process.
    """
    global _current_workspace_id
    if _current_workspace_id:
        return _current_workspace_id

    try:
        base = get_databricks_host()
        headers = get_databricks_headers()
        resp = httpx.head(
            f"{base}/api/2.0/token/list",
            headers=headers,
            timeout=_REST_TIMEOUT,
        )
        org_id = resp.headers.get("x-databricks-org-id", "")
        if org_id:
            _current_workspace_id = str(org_id)
            return _current_workspace_id
    except Exception as exc:
        logger.warning("   Could not fetch x-databricks-org-id: %s", exc)

    # Fallback: derive a stable ID from the host URL
    try:
        w = _get_workspace_client()
        host = (w.config.host if w else get_databricks_host()) or ""
        _current_workspace_id = hashlib.sha256(host.encode()).hexdigest()[:16]
    except Exception:
        _current_workspace_id = "unknown"
    return _current_workspace_id


# ── Concurrency guard ─────────────────────────────────────────────
_refresh_lock = threading.Lock()
_refresh_in_progress = False
_last_sync_had_obo = False  # True if the most recent sync used OBO (user token)


# =====================================================================
# DDL: ensure Lakebase tables exist
# =====================================================================

def ensure_discovery_tables():
    """Create the discovered_agents table and metadata row if missing."""
    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS discovered_agents (
            agent_id          TEXT PRIMARY KEY,
            workspace_id      TEXT NOT NULL,
            name              TEXT NOT NULL,
            type              TEXT,
            endpoint_name     TEXT,
            endpoint_status   TEXT,
            model_name        TEXT,
            served_entity_name TEXT,
            creator           TEXT,
            description       TEXT DEFAULT '',
            config            JSONB,
            last_synced       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            source            TEXT DEFAULT 'api',
            is_extensive      BOOLEAN DEFAULT FALSE
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_da_ws ON discovered_agents (workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_da_type ON discovered_agents (type)",
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_da_name_ws ON discovered_agents (name, workspace_id)",
        # Add description column if it doesn't exist (migration for existing tables)
        """
        DO $$ BEGIN
            ALTER TABLE discovered_agents ADD COLUMN IF NOT EXISTS description TEXT DEFAULT '';
        EXCEPTION WHEN others THEN NULL;
        END $$
        """,
        # Add is_extensive column if it doesn't exist (migration for existing tables)
        """
        DO $$ BEGIN
            ALTER TABLE discovered_agents ADD COLUMN IF NOT EXISTS is_extensive BOOLEAN DEFAULT FALSE;
        EXCEPTION WHEN others THEN NULL;
        END $$
        """,
    ]
    for stmt in ddl_statements:
        try:
            execute_update(stmt)
        except Exception as exc:
            logger.warning("Discovery DDL warning: %s", exc)
    logger.info("Discovery tables ensured")


# =====================================================================
# LIVE API discovery (current workspace)
# =====================================================================

def _classify_served_entity_sdk(se: Any, ep_tags: Optional[Dict[str, str]] = None, ep_task: str = "") -> Optional[str]:
    """Return the agent type for an SDK ServedEntity, or None to skip.

    Returns ``"external_agent"``, ``"custom_agent"``, or ``None`` (skip).
    Foundation models with tile_endpoint_metadata (KA/MAS/CustomLLM) are
    skipped here and handled by _discover_agent_bricks_from_endpoints via REST.
    """
    # External models have an explicit external_model attribute
    if getattr(se, "external_model", None) is not None:
        return "external_agent"

    # Foundation models: KA/MAS use foundation_model entities.
    # They are discovered properly via REST (tile_endpoint_metadata).
    if getattr(se, "foundation_model", None) is not None:
        return None

    # system.ai.* namespace = Databricks-managed foundation models
    entity_name = getattr(se, "entity_name", "") or ""
    if entity_name.startswith("system.ai."):
        return None

    # Feature specs
    if getattr(se, "entity_type", None) and "FEATURE" in str(getattr(se, "entity_type", "")):
        return None

    return "custom_agent"


def _discover_serving_endpoints() -> List[Dict[str, Any]]:
    """List serving endpoints from the current workspace via SDK.

    Only custom models and external models are returned; foundation models
    and feature specs are skipped.
    """
    w = _get_workspace_client()
    if not w:
        return []
    agents: List[Dict[str, Any]] = []
    try:
        for ep in w.serving_endpoints.list():
            name = ep.name or ""

            # Skip endpoints with no served entities (e.g. provisioning-only)
            if not (ep.config and ep.config.served_entities):
                continue

            se = ep.config.served_entities[0]
            ep_tags_dict: Dict[str, str] = {t.key: t.value for t in ep.tags} if ep.tags else {}
            agent_type = _classify_served_entity_sdk(se, ep_tags_dict)
            if agent_type is None:
                continue  # foundation model / system.ai / feature spec — skip

            state = ""
            if ep.state and hasattr(ep.state, "ready"):
                state = str(ep.state.ready) if ep.state.ready else ""
            elif ep.state and hasattr(ep.state, "config_update"):
                state = str(ep.state.config_update) if ep.state.config_update else ""

            model_name = se.entity_name or ""
            served_entity = se.name or ""
            config_dict: Dict[str, Any] = {
                "served_entities": [
                    {"name": s.name, "entity_name": s.entity_name}
                    for s in (ep.config.served_entities or [])
                ],
            }
            task = getattr(se, "task", "") or ""
            if task:
                config_dict["task"] = task
            if ep_tags_dict:
                config_dict["tags"] = ep_tags_dict

            creator = getattr(ep, "creator", None) or ""
            ws_id = _get_current_workspace_id()
            aid = _make_id(name, ws_id)
            agents.append({
                "agent_id": aid,
                "workspace_id": ws_id,
                "name": name,
                "type": agent_type,
                "endpoint_name": name,
                "endpoint_status": state,
                "model_name": model_name,
                "served_entity_name": served_entity,
                "creator": creator,
                "config": config_dict,
                "source": "api",
            })
    except Exception as exc:
        logger.warning("Could not list serving endpoints: %s", exc)
    return agents


def _discover_apps() -> List[Dict[str, Any]]:
    """List Databricks Apps from the current workspace.

    Tries the SDK first, then falls back to the REST API so that app
    discovery works even when the SDK version doesn't expose ``w.apps``.
    """
    agents = _discover_apps_sdk()
    if agents:
        logger.info("   SDK app discovery returned %s apps", len(agents))
        return agents
    logger.info("   SDK app discovery returned 0 apps, trying REST fallback …")
    agents = _discover_apps_rest()
    if agents:
        logger.info("   REST app discovery returned %s apps", len(agents))
    else:
        logger.warning("   REST app discovery also returned 0 apps")
    return agents


def _discover_apps_sdk() -> List[Dict[str, Any]]:
    """List Databricks Apps via the SDK."""
    w = _get_workspace_client()
    if not w:
        logger.warning("   SDK app discovery: no workspace client")
        return []
    agents: List[Dict[str, Any]] = []
    try:
        if not hasattr(w, "apps"):
            logger.warning("   SDK app discovery: w.apps not available (SDK too old)")
            return []

        for app in w.apps.list():
            name = getattr(app, "name", "") or ""

            # Prefer compute_status.state (ACTIVE/STOPPED) over deployment
            # status (SUCCEEDED/FAILED) because it reflects runtime state.
            status = ""
            compute_status = getattr(app, "compute_status", None)
            if compute_status:
                cs_state = getattr(compute_status, "state", None)
                status = str(cs_state) if cs_state else ""
                # Strip enum prefix if present (e.g. "ComputeState.ACTIVE" → "ACTIVE")
                if "." in status:
                    status = status.rsplit(".", 1)[-1]

            # Fall back to deployment status if compute_status isn't available
            if not status:
                ad = getattr(app, "active_deployment", None)
                if ad:
                    ad_status = getattr(ad, "status", None)
                    if ad_status:
                        ds = getattr(ad_status, "state", None)
                        status = str(ds) if ds else ""
                        if "." in status:
                            status = status.rsplit(".", 1)[-1]

            url = getattr(app, "url", "") or ""
            creator = getattr(app, "creator", "") or ""
            description = getattr(app, "description", "") or ""
            app_id = getattr(app, "id", "") or ""
            compute_size = getattr(app, "compute_size", "") or ""
            default_src = getattr(app, "default_source_code_path", "") or ""

            config_dict: Dict[str, Any] = {
                "url": url,
                "app_id": app_id,
                "compute_size": str(compute_size),
                "deployment_type": "databricks_app",
            }
            if default_src:
                config_dict["default_source_code_path"] = default_src

            ad = getattr(app, "active_deployment", None)
            if ad:
                config_dict["deployment_id"] = getattr(ad, "deployment_id", "") or ""
                config_dict["source_code_path"] = getattr(ad, "source_code_path", "") or ""
                mode = getattr(ad, "mode", "")
                config_dict["mode"] = str(mode) if mode else ""

            resources = getattr(app, "resources", None) or []
            if resources:
                config_dict["resources"] = []
                for r in resources:
                    r_name = getattr(r, "name", "") or ""
                    r_desc = getattr(r, "description", "") or ""
                    # Try to get the resource type from the object
                    r_type = ""
                    r_endpoint_name = ""  # actual serving endpoint name (if applicable)
                    for attr in ("serving_endpoint", "sql_warehouse", "experiment", "secret"):
                        nested = getattr(r, attr, None)
                        if nested is not None:
                            r_type = attr
                            if attr == "serving_endpoint":
                                # The nested object holds the actual endpoint name
                                r_endpoint_name = getattr(nested, "name", "") or ""
                            break
                    entry: Dict[str, Any] = {
                        "name": r_name,
                        "description": r_desc,
                        "type": r_type,
                    }
                    if r_endpoint_name:
                        entry["endpoint_name"] = r_endpoint_name
                    config_dict["resources"].append(entry)

            sp_name = getattr(app, "service_principal_name", "") or ""
            if sp_name:
                config_dict["service_principal_name"] = sp_name

            config_dict["deployment_method"] = "app"
            # Only include apps that serve agents (have a serving_endpoint resource)
            if not any(r.get("type") == "serving_endpoint" for r in config_dict.get("resources", [])):
                continue
            ws_id = _get_current_workspace_id()
            aid = _make_id(name, ws_id)
            agents.append({
                "agent_id": aid,
                "workspace_id": ws_id,
                "name": name,
                "type": "custom_app",
                "endpoint_name": name,
                "endpoint_status": status,
                "model_name": "",
                "served_entity_name": "",
                "creator": creator,
                "description": description,
                "config": config_dict,
                "source": "api",
            })

    except AttributeError as exc:
        logger.warning("   SDK app discovery AttributeError (SDK too old?): %s", exc)
    except Exception as exc:
        logger.warning("   SDK app discovery failed (%s): %s", type(exc).__name__, exc)
    return agents


def _discover_apps_rest() -> List[Dict[str, Any]]:
    """List Databricks Apps via the REST API (fallback when SDK fails)."""
    base = get_databricks_host()
    if not base:
        logger.warning("   REST app discovery: no Databricks host configured")
        return []

    agents: List[Dict[str, Any]] = []
    next_page_token: Optional[str] = None

    try:
        while True:
            params: Dict[str, Any] = {"page_size": 100}
            if next_page_token:
                params["page_token"] = next_page_token

            resp = httpx.get(
                f"{base}/api/2.0/apps",
                headers=get_databricks_headers(),
                params=params,
                timeout=_REST_TIMEOUT,
            )

            if resp.status_code != 200:
                logger.warning("   REST /api/2.0/apps returned %s: %s", resp.status_code, resp.text[:300])
                return agents

            body = resp.json()

            for app in body.get("apps", []):
                name = app.get("name", "")

                # Prefer compute_status.state over deployment status
                compute_state = ""
                cs = app.get("compute_status") or {}
                compute_state = cs.get("state", "")

                # Fall back to active_deployment.status.state
                deploy_state = ""
                active = app.get("active_deployment") or {}
                dep_status = active.get("status") or {}
                deploy_state = dep_status.get("state", "")

                status = compute_state or deploy_state

                url = app.get("url", "")
                description = app.get("description", "")
                app_id = app.get("id", "")
                compute_size = app.get("compute_size", "")
                default_src = app.get("default_source_code_path", "")

                # creator can be a string or a dict with "username"
                raw_creator = app.get("creator", "")
                if isinstance(raw_creator, dict):
                    creator = raw_creator.get("username", "") or raw_creator.get("display_name", "")
                else:
                    creator = str(raw_creator) if raw_creator else ""

                config_dict: Dict[str, Any] = {
                    "url": url,
                    "app_id": app_id,
                    "compute_size": compute_size,
                    "deployment_type": "databricks_app",
                }
                if default_src:
                    config_dict["default_source_code_path"] = default_src
                if active:
                    config_dict["deployment_id"] = active.get("deployment_id", "")
                    config_dict["source_code_path"] = active.get("source_code_path", "")
                    config_dict["mode"] = active.get("mode", "")

                resources = app.get("resources") or []
                if resources:
                    config_dict["resources"] = []
                    for r in resources:
                        r_entry: Dict[str, Any] = {
                            "name": r.get("name", ""),
                            "description": r.get("description", ""),
                        }
                        # Detect resource type from known top-level keys
                        for rtype in ("serving_endpoint", "sql_warehouse", "experiment", "secret"):
                            if rtype in r:
                                r_entry["type"] = rtype
                                if rtype == "serving_endpoint":
                                    # REST payload: {"serving_endpoint": {"name": "<actual-endpoint>"}}
                                    nested = r.get("serving_endpoint") or {}
                                    ep_name = nested.get("name", "") if isinstance(nested, dict) else ""
                                    if ep_name:
                                        r_entry["endpoint_name"] = ep_name
                                break
                        config_dict["resources"].append(r_entry)

                sp_name = app.get("service_principal_name", "")
                if sp_name:
                    config_dict["service_principal_name"] = sp_name

                config_dict["deployment_method"] = "app"
                # Only include apps that serve agents (have a serving_endpoint resource)
                if not any(r.get("type") == "serving_endpoint" for r in config_dict.get("resources", [])):
                    continue
                ws_id = _get_current_workspace_id()
                aid = _make_id(name, ws_id)
                agents.append({
                    "agent_id": aid,
                    "workspace_id": ws_id,
                    "name": name,
                    "type": "custom_app",
                    "endpoint_name": name,
                    "endpoint_status": status,
                    "model_name": "",
                    "served_entity_name": "",
                    "creator": creator,
                    "description": description,
                    "config": config_dict,
                    "source": "api",
                })

            next_page_token = body.get("next_page_token")
            if not next_page_token:
                break

    except httpx.HTTPStatusError as exc:
        logger.warning("   REST app discovery HTTP %s: %s", exc.response.status_code, exc.response.text[:300])
    except Exception as exc:
        logger.warning("   REST app discovery failed (%s): %s", type(exc).__name__, exc)

    return agents


# =====================================================================
# GENIE SPACE discovery (current workspace)
# =====================================================================

def _discover_genie_spaces() -> List[Dict[str, Any]]:
    """List Genie Spaces from the current workspace.

    Tries the SDK first (``w.genie.list_spaces()``), then falls back to
    the REST API.
    """
    agents = _discover_genie_sdk()
    if agents:
        return agents
    return _discover_genie_rest()


def _discover_genie_sdk() -> List[Dict[str, Any]]:
    """List Genie Spaces via the SDK."""
    w = _get_workspace_client()
    if not w:
        return []
    agents: List[Dict[str, Any]] = []
    try:
        if not hasattr(w, "genie"):
            logger.warning("   Genie SDK: w.genie not available")
            return []
        resp = w.genie.list_spaces()

        # The SDK may return a response object with a .spaces attribute,
        # or the response object itself may be iterable (paginated iterator).
        spaces: list = []
        if hasattr(resp, "spaces") and resp.spaces:
            spaces = list(resp.spaces)
        elif hasattr(resp, "__iter__"):
            spaces = list(resp)
        logger.info("   → Genie SDK: found %s spaces", len(spaces))

        for s in spaces:
            space_id = getattr(s, "space_id", "") or ""
            title = getattr(s, "title", "") or getattr(s, "display_name", "") or ""
            desc = getattr(s, "description", "") or ""
            creator = getattr(s, "creator_name", "") or getattr(s, "creator", "") or ""

            if not space_id:
                continue

            config_dict: Dict[str, Any] = {
                "space_id": space_id,
                "deployment_type": "genie_space",
            }

            ws_id = _get_current_workspace_id()
            aid = _make_id(title or space_id, ws_id)
            agents.append({
                "agent_id": aid,
                "workspace_id": ws_id,
                "name": title or space_id,
                "type": "genie_space",
                "endpoint_name": space_id,
                "endpoint_status": "ACTIVE",
                "model_name": "",
                "served_entity_name": "",
                "creator": str(creator),
                "description": desc,
                "config": config_dict,
                "source": "api",
            })
    except Exception as exc:
        logger.warning("Genie SDK discovery failed: %s", exc, exc_info=True)
    return agents


def _discover_genie_rest() -> List[Dict[str, Any]]:
    """List Genie Spaces via the REST API."""
    base = get_databricks_host()
    if not base:
        logger.warning("   Genie REST: no host configured")
        return []
    agents: List[Dict[str, Any]] = []
    try:
        resp = httpx.get(
            f"{base}/api/2.0/genie/spaces",
            headers=get_databricks_headers(),
            params={"page_size": 200},
            timeout=_REST_TIMEOUT,
        )
        if resp.status_code != 200:
            logger.warning("   Genie REST: HTTP %s — %s", resp.status_code, resp.text[:200])
            return []
        body = resp.json()
        logger.info("   → Genie REST: found %s spaces", len(body.get('spaces', [])))
        for s in body.get("spaces", []):
            space_id = s.get("space_id", "")
            title = s.get("title", "") or s.get("display_name", "")
            desc = s.get("description", "")
            creator = s.get("creator_name", "") or s.get("creator", "")

            if not space_id:
                continue

            config_dict: Dict[str, Any] = {
                "space_id": space_id,
                "deployment_type": "genie_space",
            }

            ws_id = _get_current_workspace_id()
            aid = _make_id(title or space_id, ws_id)
            agents.append({
                "agent_id": aid,
                "workspace_id": ws_id,
                "name": title or space_id,
                "type": "genie_space",
                "endpoint_name": space_id,
                "endpoint_status": "ACTIVE",
                "model_name": "",
                "served_entity_name": "",
                "creator": str(creator) if creator else "",
                "description": desc,
                "config": config_dict,
                "source": "api",
            })
    except Exception as exc:
        logger.warning("   Genie REST discovery failed: %s", exc)
    return agents


# =====================================================================
# GENIE SPACE discovery — cross-workspace via audit logs
# =====================================================================

def _discover_genie_from_audit_logs() -> List[Dict[str, Any]]:
    """Discover Genie Spaces across all workspaces via system.access.audit.

    The Genie list-spaces API is workspace-scoped and often returns nothing
    for service principals. Audit logs capture createSpace/updateSpace events
    across every workspace in the account, giving us space IDs, names,
    connected tables, creators, and workspace IDs.

    We also join with conversation events to surface usage metrics
    (conversation count, unique users, last active time).
    """
    sql = """
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
            action_name,
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
          AND action_name IN (
              'createSpace', 'genieCreateSpace',
              'updateSpace', 'genieUpdateSpace'
          )
          AND event_date >= CURRENT_DATE - 90
          AND response.status_code = '200'
    ),
    creators AS (
        SELECT
            COALESCE(
                request_params['space_id'],
                get_json_object(response.result, '$.space_id')
            ) AS space_id,
            FIRST_VALUE(user_identity.email) OVER (
                PARTITION BY COALESCE(
                    request_params['space_id'],
                    get_json_object(response.result, '$.space_id')
                )
                ORDER BY event_time ASC
            ) AS original_creator
        FROM system.access.audit
        WHERE service_name IN ('aibiGenie', 'genie')
          AND action_name IN ('createSpace', 'genieCreateSpace')
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
          AND action_name IN (
              'createConversation', 'createConversationMessage',
              'genieCreateConversationMessage'
          )
          AND event_date >= CURRENT_DATE - 90
          AND request_params['space_id'] IS NOT NULL
        GROUP BY 1
    )
    SELECT
        m.space_id,
        m.display_name,
        m.table_ids,
        m.warehouse_id,
        m.workspace_id,
        COALESCE(c.original_creator, m.creator_email) AS creator,
        u.conversation_count,
        u.unique_users,
        u.last_active
    FROM space_meta m
    LEFT JOIN (SELECT DISTINCT space_id, original_creator FROM creators) c
        ON m.space_id = c.space_id
    LEFT JOIN usage u
        ON m.space_id = u.space_id
    WHERE m.rn = 1
      AND m.space_id IS NOT NULL
      AND LENGTH(m.space_id) > 10
    ORDER BY COALESCE(u.last_active, m.event_time) DESC
    """

    rows = _execute_system_sql(sql)
    agents: List[Dict[str, Any]] = []

    for r in rows:
        space_id = str(r.get("space_id", "") or "")
        ws = str(r.get("workspace_id", "") or "")
        display_name = r.get("display_name") or ""
        creator = str(r.get("creator", "") or "")
        table_ids_raw = r.get("table_ids") or ""
        warehouse_id = r.get("warehouse_id") or ""

        if not space_id:
            continue

        tables: List[str] = []
        if table_ids_raw:
            try:
                tables = json.loads(table_ids_raw)
            except (json.JSONDecodeError, TypeError):
                tables = [t.strip().strip('"') for t in table_ids_raw.split(",") if t.strip()]

        conv_count = int(r.get("conversation_count") or 0)
        unique_users = int(r.get("unique_users") or 0)
        last_active = r.get("last_active")
        last_active_str = ""
        if last_active:
            last_active_str = last_active.isoformat() if hasattr(last_active, "isoformat") else str(last_active)

        name = display_name or f"Genie Space {space_id[:8]}…"

        config_dict: Dict[str, Any] = {
            "space_id": space_id,
            "deployment_type": "genie_space",
        }
        if tables:
            config_dict["tables"] = tables
        if warehouse_id:
            config_dict["warehouse_id"] = warehouse_id
        if conv_count:
            config_dict["conversation_count"] = conv_count
        if unique_users:
            config_dict["unique_users"] = unique_users
        if last_active_str:
            config_dict["last_active"] = last_active_str

        aid = _make_id(space_id, ws)
        agents.append({
            "agent_id": aid,
            "workspace_id": ws,
            "name": name,
            "type": "genie_space",
            "endpoint_name": space_id,
            "endpoint_status": "ACTIVE" if conv_count > 0 else "DISCOVERED",
            "model_name": "",
            "served_entity_name": "",
            "creator": creator,
            "description": f"{conv_count} conversations, {unique_users} users" if conv_count else "",
            "config": config_dict,
            "source": "audit_log",
            "is_extensive": True,
        })

    return agents


# =====================================================================
# OBO (On Behalf Of) discovery — user token path
# =====================================================================

def _discover_endpoints_as_user(user_token: str) -> List[Dict[str, Any]]:
    """Discover serving endpoints visible to the logged-in user (OBO).

    Uses the user's OAuth token (from ``x-forwarded-access-token`` request
    header) to list serving endpoints.  This surfaces endpoints the user has
    CAN_VIEW on that the app's service principal may not be able to see
    (e.g. privately-created Agent Bricks tiles).

    Results are tagged ``source="user_api"``, ``is_extensive=False`` so they
    appear in the default (non-extensive) view alongside SP-discovered agents.
    """
    base = get_databricks_host()
    if not base or not user_token:
        return []

    hdrs = {
        "Authorization": f"Bearer {user_token}",
        "Content-Type": "application/json",
    }
    agents: List[Dict[str, Any]] = []

    try:
        resp = httpx.get(
            f"{base}/api/2.0/serving-endpoints",
            headers=hdrs,
            timeout=_REST_TIMEOUT,
        )
        if resp.status_code != 200:
            logger.warning("   User-context (OBO) discovery: HTTP %s", resp.status_code)
            return []

        endpoints = resp.json().get("endpoints", [])
        ws_id = _get_current_workspace_id()
        logger.info("   → User-context (OBO): scanning %s endpoints", len(endpoints))

        for ep in endpoints:
            name = ep.get("name", "") or ""

            # ── Agent Bricks: tile_endpoint_metadata.problem_type ──────────────
            tile = ep.get("tile_endpoint_metadata") or {}
            problem_type = (tile.get("problem_type") or "").upper()
            agent_type: Optional[str] = _TILE_PROBLEM_TYPE_MAP.get(problem_type)

            # Fallback: served_entity task="agent/v1/responses" → fetch individual
            if agent_type is None:
                entities = ep.get("config", {}).get("served_entities", [])
                has_agent_task = any(
                    (e.get("task") or "") == "agent/v1/responses" for e in entities
                )
                if has_agent_task:
                    try:
                        det = httpx.get(
                            f"{base}/api/2.0/serving-endpoints/{name}",
                            headers=hdrs,
                            timeout=_REST_TIMEOUT,
                        )
                        if det.status_code == 200:
                            tile = det.json().get("tile_endpoint_metadata") or {}
                            problem_type = (tile.get("problem_type") or "").upper()
                            agent_type = _TILE_PROBLEM_TYPE_MAP.get(problem_type)
                    except Exception:
                        pass

            # ── Custom / external agents ────────────────────────────────────────
            if agent_type is None:
                served = ep.get("config", {}).get("served_entities", [])
                if not served:
                    continue
                se = served[0]
                if se.get("external_model") is not None:
                    agent_type = "external_agent"
                elif se.get("foundation_model") is not None:
                    continue  # non-Agent-Bricks foundation model — skip
                else:
                    ename = se.get("entity_name", "") or ""
                    if ename.startswith("system.ai."):
                        continue
                    agent_type = "custom_agent"

            state = (ep.get("state", {}) or {}).get("ready", "") or ""
            creator = ep.get("creator", "") or ""

            entities = ep.get("config", {}).get("served_entities", [])
            entity_name = ""
            if entities:
                fm = entities[0].get("foundation_model") or {}
                entity_name = (
                    fm.get("name", "")
                    or entities[0].get("entity_name", "")
                    or ""
                )

            config_dict: Dict[str, Any] = {"deployment_type": agent_type}
            if tile:
                config_dict["tile_id"] = tile.get("tile_id", "")
                config_dict["problem_type"] = problem_type

            aid = _make_id(name, ws_id)
            agents.append({
                "agent_id": aid,
                "workspace_id": ws_id,
                "name": name,
                "type": agent_type,
                "endpoint_name": name,
                "endpoint_status": state,
                "model_name": entity_name,
                "served_entity_name": entity_name,
                "creator": creator,
                "description": tile.get("tile_model_name", "") if tile else "",
                "config": config_dict,
                "source": "user_api",
                "is_extensive": False,
            })

        logger.info("   → User-context (OBO): found %s agents", len(agents))
    except Exception as exc:
        logger.warning("   User-context (OBO) discovery failed: %s", exc)

    return agents


# =====================================================================
# AGENT BRICKS discovery (KA / MAS from serving endpoints)
# =====================================================================

def _discover_agent_bricks_from_endpoints() -> List[Dict[str, Any]]:
    """Identify Agent Bricks endpoints (KA, MAS, Custom LLM, Info Extraction).

    Uses the REST API so we can access ``tile_endpoint_metadata``, which is
    the ONLY authoritative indicator that an endpoint is an Agent Bricks tile.
    Prefix/name-pattern matching is intentionally NOT used — it produces false
    positives for user-named endpoints that happen to start with "ka-" or "mas-".

    Strategy:
      1. GET /api/2.0/serving-endpoints (list) — already includes tile_endpoint_metadata
         for most Agent Bricks endpoints.
      2. For endpoints that have task="agent/v1/responses" in served_entities but
         no tile_endpoint_metadata in the list response, fetch the individual endpoint
         (GET /api/2.0/serving-endpoints/{name}) to get the full metadata.
      3. Only endpoints with a recognized tile problem_type are classified as
         Agent Bricks. Everything else is left for other discovery paths.
    """
    base = get_databricks_host()
    if not base:
        return []

    headers = get_databricks_headers()
    agents: List[Dict[str, Any]] = []
    try:
        resp = httpx.get(
            f"{base}/api/2.0/serving-endpoints",
            headers=headers,
            timeout=_REST_TIMEOUT,
        )
        if resp.status_code != 200:
            logger.warning("   Agent Bricks REST: HTTP %s: %s", resp.status_code, resp.text[:200])
            return []

        endpoints = resp.json().get("endpoints", [])
        logger.info("   → Agent Bricks REST: scanning %s endpoints", len(endpoints))

        for ep in endpoints:
            name = ep.get("name", "") or ""

            # ── Primary: tile_endpoint_metadata.problem_type ──────────────────
            tile = ep.get("tile_endpoint_metadata") or {}
            problem_type = (tile.get("problem_type") or "").upper()
            agent_type: Optional[str] = _TILE_PROBLEM_TYPE_MAP.get(problem_type)

            # ── Fallback: served_entity task="agent/v1/responses" ────────────
            # Some endpoints don't include tile_endpoint_metadata in the list
            # response — fetch the individual endpoint to get the full metadata.
            if agent_type is None:
                entities = ep.get("config", {}).get("served_entities", [])
                has_agent_task = any(
                    (e.get("task") or "") == "agent/v1/responses"
                    for e in entities
                )
                if has_agent_task:
                    try:
                        detail = httpx.get(
                            f"{base}/api/2.0/serving-endpoints/{name}",
                            headers=headers,
                            timeout=_REST_TIMEOUT,
                        )
                        if detail.status_code == 200:
                            tile = detail.json().get("tile_endpoint_metadata") or {}
                            problem_type = (tile.get("problem_type") or "").upper()
                            agent_type = _TILE_PROBLEM_TYPE_MAP.get(problem_type)
                    except Exception as exc:
                        logger.warning("   Could not fetch individual endpoint %s: %s", name, exc)

            if agent_type is None:
                continue  # not an Agent Bricks endpoint

            state_info = ep.get("state", {})
            state = state_info.get("ready", "") or ""
            creator = ep.get("creator", "") or ""

            entities = ep.get("config", {}).get("served_entities", [])
            entity_name = ""
            if entities:
                fm = entities[0].get("foundation_model") or {}
                entity_name = fm.get("name", "") or entities[0].get("entity_name", "") or ""

            config_dict: Dict[str, Any] = {
                "deployment_type": agent_type,
                "endpoint_id": ep.get("id", "") or "",
            }
            if tile:
                config_dict["tile_id"] = tile.get("tile_id", "")
                config_dict["problem_type"] = problem_type
                tile_model = tile.get("tile_model_name", "")
            else:
                tile_model = ""
            tags_list = ep.get("tags", [])
            if tags_list:
                config_dict["tags"] = {t.get("key", ""): t.get("value", "") for t in tags_list}

            ws_id = _get_current_workspace_id()
            aid = _make_id(name, ws_id)
            agents.append({
                "agent_id": aid,
                "workspace_id": ws_id,
                "name": name,
                "type": agent_type,
                "endpoint_name": name,
                "endpoint_status": state,
                "model_name": entity_name,
                "served_entity_name": entity_name,
                "creator": creator,
                "description": tile_model,
                "config": config_dict,
                "source": "api",
            })

        logger.info("   → Agent Bricks REST: found %s KA/MAS/CustomLLM/InfoExtract endpoints", len(agents))
    except Exception as exc:
        logger.warning("Agent Bricks endpoint discovery failed: %s", exc, exc_info=True)
    return agents


# =====================================================================
# SYSTEM TABLE discovery (cross-workspace)
# =====================================================================

_ENTITY_TYPE_MAP = {
    "CUSTOM_MODEL": "custom_agent",
    "EXTERNAL_MODEL": "external_agent",
}


def _discover_from_system_tables() -> List[Dict[str, Any]]:
    """Query system.serving.served_entities for cross-workspace agents.

    Only CUSTOM_MODEL and EXTERNAL_MODEL entities are returned.
    FOUNDATION_MODEL and FEATURE_SPEC are excluded.

    Additionally, endpoints matching KA/MAS naming patterns
    (``ka-<hex>-endpoint``, ``mas-<hex>-endpoint``) are classified
    as ``knowledge_assistant`` or ``multi_agent_supervisor`` respectively.

    We dedup by (workspace_id, endpoint_name) keeping the latest config
    version.
    """
    agents: List[Dict[str, Any]] = []

    rows = _execute_system_sql("""
        SELECT
            workspace_id,
            endpoint_name,
            endpoint_id,
            served_entity_name,
            entity_type,
            entity_name    AS model_name,
            entity_version,
            task,
            created_by     AS creator,
            change_time
        FROM system.serving.served_entities
        WHERE endpoint_delete_time IS NULL
          AND entity_type IN ('CUSTOM_MODEL', 'EXTERNAL_MODEL', 'FOUNDATION_MODEL')
        QUALIFY ROW_NUMBER() OVER (
            PARTITION BY workspace_id, endpoint_name
            ORDER BY CAST(endpoint_config_version AS INT) DESC
        ) = 1
        ORDER BY workspace_id, endpoint_name
    """)

    for r in rows:
        ws = str(r.get("workspace_id", ""))
        ename = r.get("endpoint_name", "") or ""
        entity_type = r.get("entity_type", "") or ""
        task_type = r.get("task", "") or ""
        creator = str(r.get("creator", "") or "")

        # Agent Bricks KA/MAS: foundation model entity + agent/v1/responses task
        if entity_type == "FOUNDATION_MODEL":
            if task_type == "agent/v1/responses":
                if _KA_ENDPOINT_RE.match(ename) or ename.lower().startswith("ka-"):
                    agent_type = "knowledge_assistant"
                elif _MAS_ENDPOINT_RE.match(ename) or ename.lower().startswith("mas-"):
                    agent_type = "multi_agent_supervisor"
                else:
                    continue  # unrecognized Agent Bricks type — skip (handled by live REST)
            else:
                continue  # non-Agent Bricks foundation model — skip
        # Custom/external agents: classify by name pattern first, then entity_type
        elif _KA_ENDPOINT_RE.match(ename) or ename.lower().startswith("ka-"):
            agent_type = "knowledge_assistant"
        elif _MAS_ENDPOINT_RE.match(ename) or ename.lower().startswith("mas-"):
            agent_type = "multi_agent_supervisor"
        else:
            agent_type = _ENTITY_TYPE_MAP.get(entity_type, "custom_agent")

        aid = _make_id(ename, ws)
        agents.append({
            "agent_id": aid,
            "workspace_id": ws,
            "name": ename,
            "type": agent_type,
            "endpoint_name": ename,
            "endpoint_status": entity_type,      # e.g. CUSTOM_MODEL
            "model_name": r.get("model_name", "") or "",
            "served_entity_name": r.get("served_entity_name", "") or "",
            "creator": creator,
            "config": {
                "entity_type": entity_type,
                "entity_version": str(r.get("entity_version", "")),
                "task": task_type,
                "endpoint_id": r.get("endpoint_id", ""),
            },
            "source": "system_table",
            "is_extensive": True,
        })

    return agents


# =====================================================================
# CROSS-WORKSPACE APP discovery (direct API enumeration)
# =====================================================================

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
    except Exception:
        pass
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
                        r_entry: Dict[str, Any] = {
                            "name": r.get("name", ""),
                            "description": r.get("description", ""),
                        }
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
                    "config": config_dict,
                    "source": "cross_workspace_api",
                    "is_extensive": True,
                })

            next_page_token = body.get("next_page_token")
            if not next_page_token:
                break

    except Exception:
        pass

    return agents


def _discover_apps_cross_workspace() -> List[Dict[str, Any]]:
    """Discover agent-serving apps across all workspaces via direct API enumeration.

    1. Get all workspace hosts from the workspace registry
    2. SP M2M token exchange + GET /api/2.0/apps on each workspace (10 concurrent)
    3. Filter to apps with serving_endpoint resources

    With 10 concurrent workers, ~736 workspaces completes in ~1-2 min.
    """
    from backend.services.workspace_registry import get_all_workspace_hosts

    # Step 1: Get all workspace hosts from registry
    all_hosts = get_all_workspace_hosts()
    if not all_hosts:
        return []

    # Exclude local workspace (already covered by _discover_apps())
    local_ws_id = _get_current_workspace_id()
    ws_hosts = {ws: host for ws, host in all_hosts.items() if ws != local_ws_id}

    if not ws_hosts:
        return []

    # Step 2: Parallel token exchange + app enumeration
    all_agents: List[Dict[str, Any]] = []

    def _process_workspace(ws_id: str) -> List[Dict[str, Any]]:
        host = ws_hosts[ws_id]
        token = _get_sp_token_for_host(host)
        if not token:
            return []
        return _fetch_apps_for_workspace(ws_id, host, token)

    with ThreadPoolExecutor(max_workers=10) as pool:
        futures = {pool.submit(_process_workspace, ws): ws for ws in ws_hosts}
        for future in as_completed(futures):
            try:
                all_agents.extend(future.result())
            except Exception:
                pass

    return all_agents


# =====================================================================
# CACHE helpers
# =====================================================================

def _make_id(name: str, workspace: str) -> str:
    raw = f"{name}:{workspace}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]


def _upsert_discovered(agents: List[Dict[str, Any]]):
    """Upsert a list of discovered agents into Lakebase."""
    if not agents:
        return
    sql = """
        INSERT INTO discovered_agents
            (agent_id, workspace_id, name, type, endpoint_name,
             endpoint_status, model_name, served_entity_name,
             creator, description, config, last_synced, source, is_extensive)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s)
        ON CONFLICT (agent_id) DO UPDATE SET
            workspace_id = EXCLUDED.workspace_id,
            name = EXCLUDED.name,
            type = EXCLUDED.type,
            endpoint_name = EXCLUDED.endpoint_name,
            endpoint_status = EXCLUDED.endpoint_status,
            model_name = EXCLUDED.model_name,
            served_entity_name = EXCLUDED.served_entity_name,
            creator = EXCLUDED.creator,
            description = EXCLUDED.description,
            config = EXCLUDED.config,
            last_synced = NOW(),
            source = EXCLUDED.source,
            is_extensive = EXCLUDED.is_extensive
    """
    with DatabasePool.get_connection() as conn:
        with conn.cursor() as cur:
            for a in agents:
                cur.execute(
                    "DELETE FROM discovered_agents WHERE name = %s AND workspace_id = %s AND agent_id != %s",
                    (a["name"], a["workspace_id"], a["agent_id"])
                )
                cur.execute(sql, (
                    a["agent_id"],
                    a["workspace_id"],
                    a["name"],
                    a["type"],
                    a.get("endpoint_name", ""),
                    a.get("endpoint_status", ""),
                    a.get("model_name", ""),
                    a.get("served_entity_name", ""),
                    a.get("creator", ""),
                    a.get("description", ""),
                    json.dumps(a.get("config") or {}),
                    a.get("source", "api"),
                    a.get("is_extensive", False),
                ))
            conn.commit()


def _purge_stale_types():
    """Delete cached entries whose type is no longer in our taxonomy.

    This cleans up foundation models and feature specs that were cached
    under the old ``serving_endpoint`` type before the filtering was added.
    """
    try:
        execute_update(
            "DELETE FROM discovered_agents WHERE type NOT IN %s",
            (_VALID_AGENT_TYPES,),
        )
    except Exception as exc:
        logger.warning("Could not purge stale agent types: %s", exc)


def _is_stale() -> bool:
    row = execute_one(
        "SELECT last_synced FROM discovered_agents ORDER BY last_synced DESC LIMIT 1"
    )
    if not row or not row.get("last_synced"):
        return True
    last = row["last_synced"]
    if last.tzinfo is None:
        last = last.replace(tzinfo=timezone.utc)
    age = (datetime.now(timezone.utc) - last).total_seconds()
    return age > _STALE_SECONDS


# =====================================================================
# PUBLIC API
# =====================================================================

def refresh_discovery(user_token: Optional[str] = None):  # noqa: C901
    """Run full discovery (API + system tables) and cache results.

    Always runs both API-native and extensive (system tables / audit log)
    discovery.  Agents are tagged with ``is_extensive=True`` when they come
    from system tables or audit logs so the frontend can filter by view.

    If ``user_token`` is provided (extracted from ``x-forwarded-access-token``
    request header), an additional OBO discovery pass is run using the user's
    credentials.  This surfaces endpoints the app's service principal cannot
    see (e.g. privately-created Agent Bricks tiles the user owns).
    """
    global _refresh_in_progress, _last_sync_had_obo
    if not _refresh_lock.acquire(blocking=False):
        return
    try:
        _refresh_in_progress = True
        obo = bool(user_token)
        logger.info("Starting agent discovery refresh (OBO=%s) …", 'yes' if obo else 'no')

        # 0) Mark existing rows as stale (don't truncate — preserve agents
        #    found by prior syncs that system tables might not re-discover)
        try:
            execute_update("UPDATE discovered_agents SET source = COALESCE(source, 'prior_sync')")
        except Exception:
            pass

        # 1) Live API discovery — serving endpoints + apps (current workspace, SP creds)
        serving_agents = _discover_serving_endpoints()
        app_agents = _discover_apps()
        api_agents = serving_agents + app_agents
        _upsert_discovered(api_agents)
        logger.info("   → API discovery: %s custom/external agents, %s apps", len(serving_agents), len(app_agents))

        # 1b) Cross-workspace app discovery (direct API enumeration)
        xws_apps: List[Dict[str, Any]] = []
        try:
            xws_apps = _discover_apps_cross_workspace()
            _upsert_discovered(xws_apps)
            logger.info("   → Cross-workspace apps (API enumeration): %s", len(xws_apps))
        except Exception as exc:
            logger.warning("   Cross-workspace app discovery failed: %s", exc)

        # 2a) Genie Spaces (current workspace — live API, always)
        genie_agents = _discover_genie_spaces()
        _upsert_discovered(genie_agents)
        logger.info("   → Genie spaces (API): %s", len(genie_agents))

        # 2b) Genie Spaces (cross-workspace — audit logs, always; tagged is_extensive)
        try:
            genie_audit = _discover_genie_from_audit_logs()
            _upsert_discovered(genie_audit)
            logger.info("   → Genie spaces (audit logs): %s", len(genie_audit))
        except Exception as exc:
            genie_audit = []
            logger.warning("   Genie audit log discovery failed: %s", exc)

        # 3) System tables discovery — cross-workspace (always; tagged is_extensive)
        try:
            sys_agents = _discover_from_system_tables()
            _upsert_discovered(sys_agents)
            logger.info("   → System table discovery: %s agents", len(sys_agents))
        except Exception as exc:
            sys_agents = []
            logger.warning("   System table discovery failed: %s", exc)

        # 4) Agent Bricks KA/MAS from serving endpoints (SP creds, runs after system tables
        #    so it always wins over system-table classifications)
        ab_agents = _discover_agent_bricks_from_endpoints()
        _upsert_discovered(ab_agents)
        logger.info("   → Agent Bricks (KA/MAS/CustomLLM/IE): %s", len(ab_agents))

        # 5) OBO discovery — user-visible endpoints (runs last; supplements SP discovery)
        user_agents: List[Dict[str, Any]] = []
        if user_token:
            user_agents = _discover_endpoints_as_user(user_token)
            _upsert_discovered(user_agents)
            logger.info("   → User-context (OBO): %s agents", len(user_agents))

        # Remove any stale entries with old types
        _purge_stale_types()

        total = (
            len(api_agents) + len(xws_apps) + len(genie_agents)
            + len(genie_audit) + len(sys_agents) + len(ab_agents)
            + len(user_agents)
        )
        _last_sync_had_obo = obo
        logger.info("Agent discovery refresh complete — %s total agents", total)
    except Exception as exc:
        logger.warning("Discovery refresh failed: %s", exc, exc_info=True)
    finally:
        _refresh_in_progress = False
        _refresh_lock.release()


def maybe_refresh_async():
    """No-op: discovery data is now populated by the scheduled Workflow job.

    The Workflow (workflows/01_discover_agents + 02_sync_to_lakebase) runs
    every 30 min and writes fresh data to Lakebase.  Manual refresh is still
    available via ``refresh_discovery()`` (POST /api/agents/sync).

    This function is kept as a no-op so existing callers (workspace_service,
    etc.) don't need changes.
    """
    pass


def get_discovered_agents(workspace_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return all discovered agents, optionally filtered by workspace.

    Reads directly from Lakebase (populated by the Workflow job).
    """
    clauses = []
    params: list = []
    if workspace_id:
        clauses.append("workspace_id = %s")
        params.append(workspace_id)
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    rows = execute_query(f"SELECT * FROM discovered_agents {where} ORDER BY name", tuple(params) if params else None)
    result = []
    for r in rows:
        d = dict(r)
        if isinstance(d.get("config"), str):
            try:
                d["config"] = json.loads(d["config"])
            except Exception:
                pass
        if d.get("last_synced") and hasattr(d["last_synced"], "isoformat"):
            d["last_synced"] = d["last_synced"].isoformat()
        result.append(d)
    return result


def get_all_agents_merged(workspace_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return discovered agents only (registered agents are excluded)."""
    discovered = get_discovered_agents(workspace_id)
    for d in discovered:
        d["_source"] = "discovered"
    return discovered


def get_discovery_status() -> Dict[str, Any]:
    """Return the current discovery cache status.

    Data is populated by the scheduled Workflow job (every 30 min).
    Manual refresh via POST /api/agents/sync is still available.
    """
    row = execute_one(
        "SELECT COUNT(*) AS total, MAX(last_synced) AS last_synced FROM discovered_agents"
    )
    total = int(row["total"]) if row and row.get("total") else 0
    last = row["last_synced"] if row else None

    # Check type breakdown for richer status
    type_counts = {}
    try:
        rows = execute_query(
            "SELECT type, COUNT(*) AS cnt FROM discovered_agents GROUP BY type ORDER BY cnt DESC"
        )
        type_counts = {r["type"]: int(r["cnt"]) for r in rows}
    except Exception:
        pass

    # Check source breakdown (api vs system_table vs audit_log)
    source_counts = {}
    try:
        rows = execute_query(
            "SELECT source, COUNT(*) AS cnt FROM discovered_agents GROUP BY source ORDER BY cnt DESC"
        )
        source_counts = {r["source"]: int(r["cnt"]) for r in rows}
    except Exception:
        pass

    return {
        "total_discovered": total,
        "last_synced": last.isoformat() if last and hasattr(last, "isoformat") else None,
        "is_refreshing": _refresh_in_progress,
        "obo_enabled": _last_sync_had_obo,
        "by_type": type_counts,
        "by_source": source_counts,
    }


def get_app_discovery_diagnostics() -> Dict[str, Any]:
    """Run agent discovery in isolation and return raw diagnostics.

    This helps debug why agents may not appear in the Agents list by
    showing what each discovery path returns or its error.
    """
    result: Dict[str, Any] = {
        "sdk_apps": {"status": "not_run", "count": 0, "items": [], "error": None},
        "rest_apps": {"status": "not_run", "count": 0, "items": [], "error": None},
        "cross_workspace_apps": {"status": "not_run", "count": 0, "items": [], "error": None},
        "sdk_endpoints": {"status": "not_run", "count": 0, "items": [], "error": None},
        "genie_spaces": {"status": "not_run", "count": 0, "items": [], "error": None},
        "genie_audit_logs": {"status": "not_run", "count": 0, "items": [], "error": None},
        "agent_bricks": {"status": "not_run", "count": 0, "items": [], "error": None},
        "system_table": {"status": "not_run", "count": 0, "items": [], "error": None},
    }

    # ── SDK apps ──
    try:
        apps = _discover_apps_sdk()
        result["sdk_apps"] = {
            "status": "ok",
            "count": len(apps),
            "items": [{"name": a["name"], "type": a["type"], "status": a["endpoint_status"]} for a in apps],
            "error": None,
        }
    except Exception as exc:
        result["sdk_apps"]["error"] = f"{type(exc).__name__}: {exc}"

    # ── REST apps ──
    try:
        apps = _discover_apps_rest()
        result["rest_apps"] = {
            "status": "ok",
            "count": len(apps),
            "items": [{"name": a["name"], "type": a["type"], "status": a["endpoint_status"]} for a in apps],
            "error": None,
        }
    except Exception as exc:
        result["rest_apps"]["error"] = f"{type(exc).__name__}: {exc}"

    # ── SDK serving endpoints (filtered) ──
    try:
        eps = _discover_serving_endpoints()
        result["sdk_endpoints"] = {
            "status": "ok",
            "count": len(eps),
            "items": [{"name": a["name"], "type": a["type"], "status": a["endpoint_status"]} for a in eps],
            "error": None,
        }
    except Exception as exc:
        result["sdk_endpoints"]["error"] = f"{type(exc).__name__}: {exc}"

    # ── Genie Spaces ──
    try:
        genie = _discover_genie_spaces()
        result["genie_spaces"] = {
            "status": "ok",
            "count": len(genie),
            "items": [{"name": a["name"], "type": a["type"], "space_id": a["endpoint_name"]} for a in genie],
            "error": None,
        }
    except Exception as exc:
        result["genie_spaces"]["error"] = f"{type(exc).__name__}: {exc}"

    # ── Genie Spaces (audit logs — cross-workspace) ──
    try:
        genie_audit = _discover_genie_from_audit_logs()
        result["genie_audit_logs"] = {
            "status": "ok",
            "count": len(genie_audit),
            "items": [
                {
                    "name": a["name"],
                    "type": a["type"],
                    "workspace_id": a["workspace_id"],
                    "space_id": a["endpoint_name"],
                    "conversations": (a.get("config") or {}).get("conversation_count", 0),
                }
                for a in genie_audit[:50]
            ],
            "error": None,
        }
    except Exception as exc:
        result["genie_audit_logs"]["error"] = f"{type(exc).__name__}: {exc}"

    # ── Agent Bricks (KA/MAS) ──
    try:
        ab = _discover_agent_bricks_from_endpoints()
        result["agent_bricks"] = {
            "status": "ok",
            "count": len(ab),
            "items": [{"name": a["name"], "type": a["type"], "status": a["endpoint_status"]} for a in ab],
            "error": None,
        }
    except Exception as exc:
        result["agent_bricks"]["error"] = f"{type(exc).__name__}: {exc}"

    # ── System table ──
    try:
        agents = _discover_from_system_tables()
        result["system_table"] = {
            "status": "ok",
            "count": len(agents),
            "items": [{"name": a["name"], "type": a["type"], "workspace_id": a["workspace_id"]} for a in agents[:50]],
            "error": None,
        }
    except Exception as exc:
        result["system_table"]["error"] = f"{type(exc).__name__}: {exc}"

    # ── Cross-workspace apps (API enumeration) ──
    try:
        xws = _discover_apps_cross_workspace()
        result["cross_workspace_apps"] = {
            "status": "ok",
            "count": len(xws),
            "items": [
                {"name": a["name"], "type": a["type"], "workspace_id": a["workspace_id"], "status": a["endpoint_status"]}
                for a in xws[:50]
            ],
            "error": None,
        }
    except Exception as exc:
        result["cross_workspace_apps"]["error"] = f"{type(exc).__name__}: {exc}"

    return result
