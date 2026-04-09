"""FastAPI routes for AI Gateway — powered by real Databricks APIs."""
from fastapi import APIRouter, Query, Depends, Request
from pydantic import BaseModel
from typing import Optional
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
