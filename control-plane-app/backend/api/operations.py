"""FastAPI routes for real-time Operations — live endpoint health from Databricks APIs."""
from fastapi import APIRouter, Depends, Query
from backend.utils.auth import get_current_user, require_admin, UserInfo
from backend.utils.access_scope import get_allowed_workspace_ids, sees_deploy_workspace
from backend.services import operations_service

router = APIRouter(prefix="/operations", tags=["operations"], dependencies=[Depends(get_current_user)])


@router.get("/status")
def realtime_status(user: UserInfo = Depends(get_current_user)):
    """Real-time health status for all serving endpoints (30s cache)."""
    return operations_service.get_realtime_status(
        allowed_workspace_ids=get_allowed_workspace_ids(user),
    )


@router.get("/endpoints/{endpoint_name}")
def endpoint_detail(endpoint_name: str, user: UserInfo = Depends(get_current_user)):
    """Live detail for a single serving endpoint (no cache)."""
    allowed = get_allowed_workspace_ids(user)
    if allowed is not None and not sees_deploy_workspace(allowed):
        return {"error": "not found", "endpoint_name": endpoint_name}
    return operations_service.get_endpoint_detail(endpoint_name)


@router.get("/usage")
def recent_usage(
    hours: int = Query(default=1, le=24),
    user: UserInfo = Depends(get_current_user),
):
    """Recent per-endpoint usage from system.serving tables (30s cache)."""
    if get_allowed_workspace_ids(user) is not None:
        return {"usage": [], "hours": hours, "last_refreshed": None}
    return operations_service.get_recent_usage(hours)


@router.post("/cache/refresh")
def refresh_cache(user: UserInfo = Depends(require_admin)):
    """Clear the operations in-memory cache."""
    operations_service.clear_cache()
    return {"status": "ok", "message": "Operations cache cleared"}
