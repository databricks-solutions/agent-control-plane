"""FastAPI routes for AI Gateway — powered by real Databricks APIs."""
from fastapi import APIRouter, HTTPException, Query, Depends, Request
from pydantic import BaseModel
from typing import Optional, List
from backend.config import settings
from backend.services import gateway_service
from backend.utils.auth import get_current_user, require_admin, require_account_admin, UserInfo

router = APIRouter(prefix="/gateway", tags=["ai-gateway"], dependencies=[Depends(get_current_user)])


class PermissionUpdate(BaseModel):
    endpoint_name: str
    principal: str
    principal_type: str  # "user" | "group" | "service_principal"
    permission_level: str  # "CAN_QUERY" | "CAN_MANAGE" | "CAN_VIEW"
    resource_type: Optional[str] = None  # "serving_endpoint" | "app" | "genie_space"
    workspace_id: Optional[str] = None  # for cross-workspace operations


class PermissionRemove(BaseModel):
    endpoint_name: str
    principal: str
    principal_type: str
    resource_type: Optional[str] = None  # "serving_endpoint" | "app" | "genie_space"
    workspace_id: Optional[str] = None  # for cross-workspace operations


@router.post("/cache/refresh")
def refresh_cache():
    """Clear the AI Gateway in-memory cache so the next request fetches fresh data."""
    gateway_service.clear_cache()
    return {"status": "ok", "message": "Gateway cache cleared — fresh data will be fetched on next request"}


@router.get("/page-data")
def gateway_page_data():
    """Composite: overview + endpoints in a single request (avoids waterfall)."""
    return gateway_service.get_page_data()


@router.get("/overview")
def gateway_overview():
    """AI Gateway KPI overview."""
    return gateway_service.get_overview()


@router.get("/endpoints")
def list_endpoints():
    """List all serving endpoints with AI Gateway config."""
    return gateway_service.get_all_endpoints()


@router.get("/endpoints/{name}")
def get_endpoint(name: str):
    """Get a single endpoint by name."""
    ep = gateway_service.get_endpoint(name)
    if not ep:
        from fastapi import HTTPException
        raise HTTPException(status_code=404, detail="Endpoint not found")
    return ep


@router.get("/permissions")
def list_permissions(endpoint_name: Optional[str] = Query(None)):
    """List permissions (optionally by endpoint)."""
    return gateway_service.get_permissions(endpoint_name)


@router.get("/endpoints-permissions")
def endpoints_with_permissions():
    """List ALL endpoints with their full ACL (for the permissions editor)."""
    return gateway_service.get_endpoints_with_permissions()


@router.post("/permissions/update")
def update_permission(body: PermissionUpdate, request: Request, user: UserInfo = Depends(require_admin)):
    """Grant or update a permission on a serving endpoint, app, or genie space.

    For local workspace: requires workspace admin.
    For cross-workspace (workspace_id set): requires account admin + OBO token.
    """
    if body.workspace_id:
        # Cross-workspace — require account admin
        if not user.is_account_admin:
            return {"error": "Account admin access required for cross-workspace permission management"}
        user_token = request.headers.get("x-forwarded-access-token", "")
        if not user_token:
            return {"error": "OBO token required for cross-workspace operations (enable User Authorization)"}
        result = gateway_service.update_remote_permission(
            body.workspace_id, body.endpoint_name, body.principal,
            body.principal_type, body.permission_level,
            resource_type=body.resource_type, user_token=user_token,
        )
    else:
        result = gateway_service.update_endpoint_permission(
            body.endpoint_name, body.principal, body.principal_type, body.permission_level,
            resource_type=body.resource_type,
        )
    # Update Lakebase cache for this endpoint
    if result.get("ok"):
        try:
            from backend.services.agent_permissions_cache import update_cached_acl_for_endpoint
            update_cached_acl_for_endpoint(body.endpoint_name, workspace_id=body.workspace_id or "")
        except Exception:
            pass
    return result


@router.post("/permissions/remove")
def remove_permission(body: PermissionRemove, request: Request, user: UserInfo = Depends(require_admin)):
    """Remove a principal's direct permissions from a serving endpoint, app, or genie space.

    For local workspace: requires workspace admin.
    For cross-workspace (workspace_id set): requires account admin + OBO token.
    """
    if body.workspace_id:
        if not user.is_account_admin:
            return {"error": "Account admin access required for cross-workspace permission management"}
        user_token = request.headers.get("x-forwarded-access-token", "")
        if not user_token:
            return {"error": "OBO token required for cross-workspace operations (enable User Authorization)"}
        result = gateway_service.remove_remote_permission(
            body.workspace_id, body.endpoint_name, body.principal,
            body.principal_type, resource_type=body.resource_type, user_token=user_token,
        )
    else:
        result = gateway_service.remove_endpoint_permission(
            body.endpoint_name, body.principal, body.principal_type,
            resource_type=body.resource_type,
        )
    # Update Lakebase cache for this endpoint
    if result.get("ok"):
        try:
            from backend.services.agent_permissions_cache import update_cached_acl_for_endpoint
            update_cached_acl_for_endpoint(body.endpoint_name, workspace_id=body.workspace_id or "")
        except Exception:
            pass
    return result


@router.get("/rate-limits")
def list_rate_limits(endpoint_name: Optional[str] = Query(None)):
    """List rate limits from AI Gateway config."""
    return gateway_service.get_rate_limits(endpoint_name)


@router.get("/guardrails")
def list_guardrails(endpoint_name: Optional[str] = Query(None)):
    """List guardrails config from AI Gateway."""
    return gateway_service.get_guardrails(endpoint_name)


@router.get("/usage/summary")
def usage_summary(days: int = Query(default=7, le=90)):
    """Per-endpoint usage summary from system tables."""
    return gateway_service.get_usage_summary(days)


@router.get("/usage/timeseries")
def usage_timeseries(
    days: int = Query(default=7, le=90),
    endpoint_name: Optional[str] = Query(None),
):
    """Hourly usage time series from system tables."""
    return gateway_service.get_usage_timeseries(days, endpoint_name)


@router.get("/usage/by-user")
def usage_by_user(days: int = Query(default=7, le=90)):
    """Per-user usage summary from system tables."""
    return gateway_service.get_usage_by_user(days)


@router.get("/uag-v2-usage")
def uag_v2_usage():
    """Unity AI Gateway (v2) usage summary from system.ai_gateway.usage —
    v2-routed endpoints only, ~20-min fresh (cached tokens + latency/TTFB).
    Breakdowns include requester_type, destination_model, api_type,
    service_type (model/MCP/provider) and route_action (routing outcomes)."""
    return gateway_service.get_uag_v2_usage()


class ModelServiceGrant(BaseModel):
    full_name: str
    principal: str
    add: List[str] = []
    remove: List[str] = []


@router.get("/model-services")
def model_services(user: UserInfo = Depends(get_current_user)):
    """v3 Unity Gateway UC model services (metastore-wide). Read via the caller's
    OBO token so UC scopes what they can see."""
    return gateway_service.list_model_services(user_token=user.token)


@router.get("/model-services/grants")
def model_service_grants(full_name: str = Query(...), user: UserInfo = Depends(get_current_user)):
    """UC grants on one model service securable."""
    return gateway_service.get_model_service_grants(full_name, user_token=user.token)


@router.post("/model-services/grant")
def model_service_grant(body: ModelServiceGrant, request: Request, user: UserInfo = Depends(require_admin)):
    """Add/remove UC privileges on a model service. Admin-gated (workspace admin),
    mirroring the app's v1 local permission writes: the write prefers the user's OBO
    token (UC-enforced) but OBO can't carry the `unity-catalog` scope, so it falls
    back to the app SP, which has MANAGE. The require_admin gate is the authorization
    boundary; writes are SP-attributed in the audit trail."""
    token = request.headers.get("x-forwarded-access-token", "") or user.token
    return gateway_service.set_model_service_grant(
        body.full_name, body.principal, body.add, body.remove, user_token=token,
    )


@router.get("/endpoint-inventory")
def endpoint_inventory():
    """Account-wide served-entity inventory (read-only) from
    system.serving.served_entities — all serving endpoints across every workspace in
    the metastore, not just the deploy workspace. Live management stays per-workspace."""
    return gateway_service.get_endpoint_inventory()


@router.get("/uag-budget-status")
def uag_budget_status():
    """Read-only budget configuration inventory from the account Budgets API
    (/api/2.1/accounts/{id}/budgets): per-budget cap thresholds, enforce
    (BLOCK_USAGE) vs alert-only, filter, and AI-relevance. Fleet-wide view;
    enforcement stays platform-side. Empty when the workflow had no account
    credentials to read the (account-scoped) API."""
    return gateway_service.get_uag_budget_status()


@router.get("/uag-mcp-tools")
def uag_mcp_tools():
    """Per-tool MCP activity from system.ai_gateway.usage (service_type =
    MCP_SERVICE): service name, tool, server type, calls, users."""
    return gateway_service.get_uag_mcp_tools()


@router.get("/uag-v2-timeseries")
def uag_v2_timeseries():
    """Daily UAG v2 usage series (requests + tokens) for trend charts."""
    return gateway_service.get_uag_v2_timeseries()


@router.get("/uag-coding-agents")
def uag_coding_agents():
    """Coding-agent activity (Claude Code / Codex / Cursor / Gemini CLI) from
    Unity AI Gateway usage — requests, users, active days, tokens."""
    return gateway_service.get_uag_coding_agents()


@router.get("/guardrail-coverage")
def guardrail_coverage():
    """Guardrail coverage/activity per endpoint from Unity AI Gateway v2
    (which endpoints are guarded, check volume, judge models). Coverage only —
    not block/mask outcomes (those need the gated UAG feature-results surface)."""
    return gateway_service.get_guardrail_coverage()


@router.get("/throttling")
def throttling():
    """Throttling / reliability per endpoint from Unity AI Gateway usage —
    HTTP 429 (rate-limited) and 5xx (server-error) counts + throttle rate."""
    return gateway_service.get_throttling()


@router.get("/fallback-routing")
def fallback_routing():
    """Smart-routing fallback per endpoint from Unity AI Gateway usage — how often
    routing fell back to a backup model, recovery rate, and backup destinations."""
    return gateway_service.get_fallback_routing()


@router.get("/inference-logs")
def inference_logs(
    limit: int = Query(default=50, le=500),
    endpoint_name: Optional[str] = Query(None),
):
    """Recent request logs from system tables."""
    return gateway_service.get_inference_logs(limit, endpoint_name)


@router.get("/metrics")
def operational_metrics(hours: int = Query(default=24, le=168)):
    """Operational metrics from system tables."""
    return gateway_service.get_operational_metrics(hours)
