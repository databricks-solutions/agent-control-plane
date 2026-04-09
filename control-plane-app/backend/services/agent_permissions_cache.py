"""Agent Permissions Cache — stores agent ACLs in Lakebase for fast page loads.

Populated during discovery sync (POST /api/agents/sync).  The
``/agents/with-permissions`` endpoint reads from Lakebase instead of
making N live API calls per agent.

Auto-refresh: if the cache is older than ``_STALE_AFTER_SECONDS``, a
background thread refreshes it on the next read request.
"""
from __future__ import annotations

import json
import threading
import time
import traceback
from typing import Any, Dict, List, Optional

import httpx

from backend.database import execute_query, execute_update, execute_one

import logging

logger = logging.getLogger(__name__)

# ── Staleness config ─────────────────────────────────────────────
_STALE_AFTER_SECONDS = 30 * 60  # 30 minutes
_bg_refresh_lock = threading.Lock()
_bg_refresh_running = False


# ── DDL ──────────────────────────────────────────────────────────

def ensure_agent_permissions_table():
    """Create the agent_permissions cache table if it doesn't exist."""
    ddl = """
    CREATE TABLE IF NOT EXISTS agent_permissions_cache (
        agent_id         TEXT PRIMARY KEY,
        name             TEXT NOT NULL DEFAULT '',
        type             TEXT DEFAULT '',
        endpoint_name    TEXT DEFAULT '',
        endpoint_status  TEXT DEFAULT '',
        created_by       TEXT DEFAULT '',
        is_active        BOOLEAN DEFAULT FALSE,
        has_endpoint     BOOLEAN DEFAULT FALSE,
        resource_type    TEXT DEFAULT '',
        workspace_id     TEXT DEFAULT '',
        is_cross_workspace BOOLEAN DEFAULT FALSE,
        workspace_active BOOLEAN DEFAULT TRUE,
        acl              JSONB DEFAULT '[]',
        last_refreshed   TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    )
    """
    try:
        execute_update(ddl)
        logger.info("agent_permissions_cache table ensured")
    except Exception as exc:
        logger.warning("agent_permissions_cache DDL warning: %s", exc)


# ── Staleness helpers ──────────────────────────────────────────

def _is_cache_stale() -> bool:
    """Return True if the newest row is older than _STALE_AFTER_SECONDS."""
    try:
        row = execute_one(
            "SELECT MAX(last_refreshed) AS last_refreshed FROM agent_permissions_cache"
        )
        if not row or not row.get("last_refreshed"):
            return True
        from datetime import datetime, timezone
        last = row["last_refreshed"]
        if hasattr(last, "timestamp"):
            age = time.time() - last.timestamp()
        else:
            return True
        return age > _STALE_AFTER_SECONDS
    except Exception:
        return False  # don't trigger refresh on DB errors


def maybe_refresh_in_background():
    """If the cache is stale, kick off a background thread to refresh it.

    Returns immediately — the caller always gets the current (possibly stale)
    cached data while the refresh runs in the background.
    """
    global _bg_refresh_running
    if not _is_cache_stale():
        return
    with _bg_refresh_lock:
        if _bg_refresh_running:
            return  # another thread is already refreshing
        _bg_refresh_running = True

    def _do_refresh():
        global _bg_refresh_running
        try:
            logger.info("Background refresh of agent permissions cache (stale) …")
            refresh_agent_permissions()
        except Exception as exc:
            logger.warning("Background agent permissions refresh failed: %s", exc)
        finally:
            with _bg_refresh_lock:
                _bg_refresh_running = False

    t = threading.Thread(target=_do_refresh, daemon=True)
    t.start()


# ── Read (fast path) ────────────────────────────────────────────

def get_cached_agent_permissions() -> List[Dict[str, Any]]:
    """Read all agent permissions from Lakebase cache.

    If the cache is stale (>30 min), a background thread is spawned
    to refresh it.  The caller always gets the current data immediately.
    """
    maybe_refresh_in_background()
    try:
        rows = execute_query(
            "SELECT * FROM agent_permissions_cache ORDER BY name"
        )
        results = []
        for r in rows:
            d = dict(r)
            # Parse JSON ACL
            acl = d.get("acl")
            if isinstance(acl, str):
                try:
                    d["acl"] = json.loads(acl)
                except Exception:
                    d["acl"] = []
            if d.get("last_refreshed") and hasattr(d["last_refreshed"], "isoformat"):
                d["last_refreshed"] = d["last_refreshed"].isoformat()
            results.append(d)
        return results
    except Exception as exc:
        logger.warning("Failed to read agent_permissions_cache: %s", exc)
        return []


def get_cache_status() -> Dict[str, Any]:
    """Return cache metadata (count + last refresh time)."""
    try:
        row = execute_one(
            "SELECT COUNT(*) AS total, MAX(last_refreshed) AS last_refreshed FROM agent_permissions_cache"
        )
        return {
            "total": int(row["total"]) if row and row.get("total") else 0,
            "last_refreshed": (
                row["last_refreshed"].isoformat()
                if row and row.get("last_refreshed") and hasattr(row["last_refreshed"], "isoformat")
                else None
            ),
        }
    except Exception:
        return {"total": 0, "last_refreshed": None}


def update_cached_acl_for_endpoint(endpoint_name: str, workspace_id: str = ""):
    """Re-fetch and update the cached ACL for a single endpoint (after grant/revoke).

    Finds all agents in the cache with this endpoint_name and refreshes
    their ACL from the live API.  This avoids a full cache rebuild.

    For cross-workspace agents, uses SP M2M OAuth to fetch from the remote workspace.
    """
    from backend.services.gateway_service import (
        _get_endpoint_permissions,
        _get_app_permissions,
        _get_genie_permissions,
        _get_workspace_client,
        get_endpoint,
    )

    try:
        rows = execute_query(
            "SELECT agent_id, type, resource_type, is_cross_workspace, workspace_id "
            "FROM agent_permissions_cache WHERE endpoint_name = %s",
            (endpoint_name,),
        )
    except Exception:
        return

    for row in rows:
        agent_id = row["agent_id"]
        agent_type = row.get("type", "")
        resource_type = row.get("resource_type", "")
        is_cross_ws = row.get("is_cross_workspace", False)
        row_ws = row.get("workspace_id", "") or workspace_id
        acl: list = []

        if is_cross_ws and row_ws:
            # Cross-workspace: use SP M2M OAuth to fetch ACL from remote
            acl = _fetch_remote_acl(row_ws, endpoint_name, resource_type, agent_type)
        elif resource_type == "serving_endpoint":
            ep = get_endpoint(endpoint_name)
            if ep:
                eid = ep.get("endpoint_id", "")
                if eid:
                    acl = _get_endpoint_permissions(eid)
        elif resource_type == "genie_space":
            acl = _get_genie_permissions(endpoint_name)
        elif resource_type == "app":
            acl = _get_app_permissions(endpoint_name)

        try:
            execute_update(
                "UPDATE agent_permissions_cache SET acl = %s, last_refreshed = NOW() WHERE agent_id = %s",
                (json.dumps(acl), agent_id),
            )
        except Exception as exc:
            logger.warning("Failed to update cached ACL for %s: %s", agent_id, exc)


def _fetch_remote_acl(workspace_id: str, endpoint_name: str, resource_type: str, agent_type: str) -> list:
    """Fetch ACL from a remote workspace using SP M2M OAuth."""
    import os
    from backend.services.workspace_registry import get_workspace_host

    host = get_workspace_host(str(workspace_id))
    if not host:
        return []

    client_id = os.environ.get("DATABRICKS_CLIENT_ID", "")
    client_secret = os.environ.get("DATABRICKS_CLIENT_SECRET", "")
    if not (client_id and client_secret):
        return []

    try:
        resp = httpx.post(
            f"{host}/oidc/v1/token",
            data={"grant_type": "client_credentials", "client_id": client_id,
                  "client_secret": client_secret, "scope": "all-apis"},
            timeout=30,
        )
        if resp.status_code != 200:
            return []
        token = resp.json().get("access_token", "")
        if not token:
            return []
    except Exception:
        return []

    if resource_type == "serving_endpoint" or (not resource_type and agent_type not in ("genie_space", "custom_app")):
        return _fetch_remote_endpoint_permissions(host, token, endpoint_name)
    elif resource_type == "genie_space" or agent_type == "genie_space":
        return _fetch_remote_permissions(host, token, "genie", endpoint_name)
    elif resource_type == "app" or agent_type == "custom_app":
        return _fetch_remote_permissions(host, token, "apps", endpoint_name)
    return []


# ── Write (refresh path) ────────────────────────────────────────

def refresh_agent_permissions(user_token: Optional[str] = None):
    """Fetch permissions for all agents and cache in Lakebase.

    Called at the end of discovery sync.  For local workspace agents, uses
    the SP client.  For cross-workspace agents, uses the OBO token if available.
    """
    from backend.services.gateway_service import (
        _get_endpoint_permissions,
        _get_app_permissions,
        _get_genie_permissions,
        _get_workspace_client,
        get_endpoint,
    )
    from backend.services.discovery_service import (
        get_all_agents_merged,
        _get_current_workspace_id,
    )
    from backend.services.workspace_registry import get_workspace_host

    current_ws = _get_current_workspace_id()
    agents = get_all_agents_merged(workspace_id=None)

    # Also include agent_registry entries not already in discovered_agents
    # (agent_registry may have agents from prior syncs / workflow runs)
    # Dedup by both agent_id AND name to avoid duplicates across tables
    try:
        discovered_ids = {a.get("agent_id") for a in agents}
        discovered_names = {a.get("name") for a in agents}
        registry_rows = execute_query(
            "SELECT agent_id, name, type, endpoint_name, endpoint_status, "
            "created_by, is_active, config FROM agent_registry"
        )
        for r in registry_rows:
            if r["agent_id"] not in discovered_ids and r.get("name") not in discovered_names:
                cfg = r.get("config") or {}
                if isinstance(cfg, str):
                    try:
                        cfg = json.loads(cfg)
                    except Exception:
                        cfg = {}
                agents.append({
                    "agent_id": r["agent_id"],
                    "name": r["name"],
                    "type": r.get("type", ""),
                    "endpoint_name": r.get("endpoint_name", ""),
                    "endpoint_status": r.get("endpoint_status", ""),
                    "created_by": r.get("created_by", ""),
                    "is_active": r.get("is_active", False),
                    "workspace_id": cfg.get("workspace_id", ""),
                    "_source": "registry",
                })
    except Exception as exc:
        logger.warning("   Could not merge agent_registry: %s", exc)

    rows: List[Dict[str, Any]] = []

    logger.info("Refreshing agent permissions cache for %s agents …", len(agents))

    for agent in agents:
        ep_name = agent.get("endpoint_name") or ""
        agent_type = agent.get("type", "")
        agent_ws = agent.get("workspace_id", "")
        is_cross_ws = bool(agent_ws and agent_ws != current_ws)
        acl: list = []
        has_endpoint = False
        resource_type = ""

        # Check if the cross-workspace target is reachable
        workspace_active = True
        if is_cross_ws:
            remote_host_check = get_workspace_host(agent_ws)
            workspace_active = bool(remote_host_check)

        if ep_name and not is_cross_ws:
            # ── Local workspace ──
            ep = get_endpoint(ep_name)
            if ep:
                has_endpoint = True
                resource_type = "serving_endpoint"
                eid = ep.get("endpoint_id", "")
                if eid:
                    acl = _get_endpoint_permissions(eid)
            elif agent_type == "genie_space":
                has_endpoint = True
                resource_type = "genie_space"
                acl = _get_genie_permissions(ep_name)
            else:
                try:
                    w = _get_workspace_client()
                    if w:
                        app_obj = w.apps.get(ep_name)
                        if app_obj:
                            has_endpoint = True
                            resource_type = "app"
                            acl = _get_app_permissions(ep_name)
                except Exception:
                    pass

        elif ep_name and is_cross_ws and workspace_active:
            # ── Cross-workspace via SP M2M OAuth ──
            has_endpoint = True
            if agent_type == "genie_space":
                resource_type = "genie_space"
            elif agent_type == "custom_app":
                resource_type = "app"
            else:
                resource_type = "serving_endpoint"
            acl = _fetch_remote_acl(agent_ws, ep_name, resource_type, agent_type)

        rows.append({
            "agent_id": agent.get("agent_id", ""),
            "name": agent.get("name", ""),
            "type": agent_type,
            "endpoint_name": ep_name,
            "endpoint_status": agent.get("endpoint_status", ""),
            "created_by": agent.get("created_by", ""),
            "is_active": agent.get("is_active", False),
            "has_endpoint": has_endpoint,
            "resource_type": resource_type,
            "workspace_id": agent_ws,
            "is_cross_workspace": is_cross_ws,
            "workspace_active": workspace_active,
            "acl": acl,
        })

    # Write to Lakebase (truncate + insert for clean rebuild)
    try:
        execute_update("TRUNCATE TABLE agent_permissions_cache")
        for r in rows:
            execute_update(
                """INSERT INTO agent_permissions_cache
                   (agent_id, name, type, endpoint_name, endpoint_status, created_by,
                    is_active, has_endpoint, resource_type, workspace_id, is_cross_workspace,
                    workspace_active, acl, last_refreshed)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())""",
                (
                    r["agent_id"], r["name"], r["type"], r["endpoint_name"],
                    r["endpoint_status"], r["created_by"], r["is_active"],
                    r["has_endpoint"], r["resource_type"], r["workspace_id"],
                    r["is_cross_workspace"], r["workspace_active"], json.dumps(r["acl"]),
                ),
            )
        logger.info("Agent permissions cache refreshed: %s agents", len(rows))
    except Exception as exc:
        logger.warning("Failed to write agent_permissions_cache: %s", exc, exc_info=True)


# ── Remote permission helpers (same as agents.py but reusable) ──

def _fetch_remote_permissions(
    host: str, token: str, resource_type: str, resource_id: str,
) -> list:
    """Fetch permissions from a remote workspace via REST API."""
    try:
        resp = httpx.get(
            f"{host}/api/2.0/permissions/{resource_type}/{resource_id}",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            timeout=15,
        )
        if resp.status_code != 200:
            return []
        data = resp.json()
        results = []
        for acl in data.get("access_control_list", []):
            principal = (
                acl.get("user_name")
                or acl.get("group_name")
                or acl.get("service_principal_name")
                or "unknown"
            )
            principal_type = (
                "user" if acl.get("user_name")
                else "group" if acl.get("group_name")
                else "service_principal" if acl.get("service_principal_name")
                else "unknown"
            )
            permissions = []
            for p in acl.get("all_permissions", []):
                permissions.append({
                    "permission_level": p.get("permission_level", ""),
                    "inherited": p.get("inherited", False),
                    "inherited_from_object": (
                        p["inherited_from_object"][0] if p.get("inherited_from_object") else None
                    ),
                })
            results.append({
                "principal": principal,
                "principal_type": principal_type,
                "permissions": permissions,
            })
        return results
    except Exception:
        return []


def _fetch_remote_endpoint_permissions(
    host: str, token: str, endpoint_name: str,
) -> list:
    """Fetch serving endpoint permissions from a remote workspace."""
    try:
        resp = httpx.get(
            f"{host}/api/2.0/serving-endpoints/{endpoint_name}",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            timeout=15,
        )
        if resp.status_code != 200:
            return []
        ep = resp.json()
        eid = ep.get("id", "")
        if not eid:
            return []
        return _fetch_remote_permissions(host, token, "serving-endpoints", eid)
    except Exception:
        return []
