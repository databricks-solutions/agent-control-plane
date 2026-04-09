"""API routes for agents."""
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from backend.utils.auth import get_current_user
from typing import List, Optional, Dict, Any
from backend.models.agent import AgentOut, AgentListOut, AgentUpdate
from backend.services import agent_service
from backend.services.discovery_service import (
    get_discovered_agents,
    get_all_agents_merged,
    refresh_discovery,
    get_discovery_status,
    get_app_discovery_diagnostics,
)

import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/agents", tags=["agents"], dependencies=[Depends(get_current_user)])


@router.get("", response_model=List[AgentListOut])
def list_agents(active_only: bool = Query(default=False)):
    """List all agents from the registry."""
    return agent_service.get_all_agents(active_only=active_only)


@router.get("/full")
def list_agents_full(active_only: bool = Query(default=False)):
    """List all agents with full detail (tags, config, description)."""
    return agent_service.get_all_agents_full(active_only=active_only)


@router.get("/discovered")
def list_discovered_agents(
    workspace_id: Optional[str] = Query(default=None),
):
    """List agents discovered from live API + system tables."""
    return get_discovered_agents(workspace_id)


@router.get("/all")
def list_all_agents(
    workspace_id: Optional[str] = Query(default=None),
):
    """Merged view: registered + discovered agents, filterable by workspace."""
    return get_all_agents_merged(workspace_id)


@router.get("/discovery/status")
def discovery_status(request: Request):
    """Current discovery cache status."""
    # Check if the current request has an OBO token — this means user auth
    # is enabled on the app, regardless of whether the last sync used OBO.
    has_obo = bool(request.headers.get("x-forwarded-access-token"))
    status = get_discovery_status()
    status["obo_enabled"] = has_obo or status.get("obo_enabled", False)
    return status


@router.post("/sync")
def sync_agents(request: Request):
    """Trigger a full discovery refresh.

    Extracts the ``x-forwarded-access-token`` header injected by Databricks Apps
    so that OBO (on-behalf-of) discovery can run using the logged-in user's
    credentials.  This surfaces endpoints the app SP cannot see (e.g.
    privately-created Agent Bricks tiles owned by the user).

    Note: OBO discovery requires "User authorization" to be enabled for the
    app in the Databricks Apps UI (add the ``all-apis`` OAuth scope).
    """
    user_token: Optional[str] = request.headers.get("x-forwarded-access-token")
    if user_token:
        logger.info("   OBO token present — will run user-context discovery")
    else:
        logger.info("   No OBO token — running SP-only discovery (enable User Authorization in the app settings to discover user-owned endpoints)")
    refresh_discovery(user_token=user_token)

    # Refresh workspace registry + permissions cache in background thread
    # to avoid HTTP timeout (these can take 60+ seconds with 700+ workspaces)
    import threading
    from backend.services.workspace_registry import refresh_workspace_registry, get_all_workspace_hosts

    ws_count = len(get_all_workspace_hosts())

    def _bg_refresh():
        try:
            cnt = refresh_workspace_registry(user_token=user_token)
            logger.info("   Background workspace registry: %s workspaces", cnt)
        except Exception as exc:
            logger.warning("   Background workspace registry refresh failed: %s", exc)
        try:
            from backend.services.agent_permissions_cache import refresh_agent_permissions
            refresh_agent_permissions(user_token=user_token)
        except Exception as exc:
            logger.warning("   Background agent permissions cache refresh failed: %s", exc)

    t = threading.Thread(target=_bg_refresh, daemon=True)
    t.start()

    return {
        "status": "ok",
        "message": "Discovery refresh complete. Workspace registry & permissions cache refreshing in background.",
        "obo_enabled": bool(user_token),
        "workspaces_registered": ws_count,
    }


@router.post("/workspace-registry")
def populate_workspace_registry(request: Request, body: Dict[str, Any] = {}):
    """Populate the workspace registry from an external source.

    Accepts either:
      - ``{"hosts": "ws_id1=https://host1,ws_id2=https://host2,..."}``  (CSV string)
      - ``{"workspaces": [{"workspace_id": "...", "host": "..."},...]}``  (JSON array)

    Typically called from a setup script that has account-level API access.
    """
    from backend.services.workspace_registry import _upsert_workspace, get_all_workspace_hosts
    count = 0

    # CSV string format
    hosts_str = body.get("hosts", "")
    if hosts_str:
        for pair in hosts_str.split(","):
            pair = pair.strip()
            if "=" not in pair:
                continue
            ws_id, host = pair.split("=", 1)
            ws_id, host = ws_id.strip(), host.strip()
            if ws_id and host:
                _upsert_workspace(ws_id, host)
                count += 1

    # JSON array format
    for ws in body.get("workspaces", []):
        ws_id = str(ws.get("workspace_id", ""))
        host = ws.get("host", "")
        name = ws.get("name", "")
        deployment = ws.get("deployment_name", "")
        if ws_id and host:
            _upsert_workspace(ws_id, host, name, deployment)
            count += 1

    total = len(get_all_workspace_hosts())
    return {"status": "ok", "upserted": count, "total": total}


@router.get("/discovery/diagnostics")
def discovery_diagnostics():
    """Run app-discovery paths in isolation and return raw diagnostics."""
    return get_app_discovery_diagnostics()


@router.get("/with-permissions")
def agents_with_permissions(request: Request):
    """List all agents with their cached permissions (from Lakebase).

    Data is refreshed during Sync (POST /api/agents/sync).
    Falls back to live fetching if the cache is empty (first load).
    """
    from backend.services.agent_permissions_cache import (
        get_cached_agent_permissions,
        refresh_agent_permissions,
        get_cache_status,
    )

    # If cache is empty, populate it on-demand (first load after deploy)
    status = get_cache_status()
    if status["total"] == 0:
        user_token: Optional[str] = request.headers.get("x-forwarded-access-token")
        try:
            refresh_agent_permissions(user_token=user_token)
        except Exception as exc:
            logger.warning("On-demand agent permissions cache fill failed: %s", exc)

    return get_cached_agent_permissions()


@router.get("/{agent_id}", response_model=AgentOut)
def get_agent(agent_id: str):
    """Get agent details."""
    agent = agent_service.get_agent_by_id(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    return agent


@router.put("/{agent_id}", response_model=AgentOut)
def update_agent(agent_id: str, update: AgentUpdate):
    """Update an agent."""
    success = agent_service.update_agent(agent_id, update)
    if not success:
        raise HTTPException(status_code=400, detail="Failed to update agent")
    agent = agent_service.get_agent_by_id(agent_id)
    if not agent:
        raise HTTPException(status_code=404, detail="Agent not found")
    return agent


@router.get("/{agent_id}/metrics")
def get_agent_metrics(agent_id: str, hours: int = Query(default=24, ge=1, le=168)):
    """Get performance metrics for an agent."""
    metrics = agent_service.get_agent_metrics(agent_id, hours)
    if not metrics:
        raise HTTPException(status_code=404, detail="Agent not found or no metrics available")
    return {"data": metrics, "meta": {"agent_id": agent_id, "hours": hours}}
