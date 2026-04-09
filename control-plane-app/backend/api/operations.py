"""FastAPI routes for real-time Operations — live endpoint health from Databricks APIs."""
from fastapi import APIRouter, Depends, Query
from backend.utils.auth import get_current_user
from backend.services import operations_service

router = APIRouter(prefix="/operations", tags=["operations"], dependencies=[Depends(get_current_user)])


@router.get("/status")
def realtime_status():
    """Real-time health status for all serving endpoints (30s cache)."""
    return operations_service.get_realtime_status()


@router.get("/endpoints/{endpoint_name}")
def endpoint_detail(endpoint_name: str):
    """Live detail for a single endpoint (no cache)."""
    return operations_service.get_endpoint_detail(endpoint_name)


@router.get("/usage")
def recent_usage(hours: int = Query(default=1, le=24)):
    """Recent per-endpoint usage from system.serving tables (30s cache)."""
    return operations_service.get_recent_usage(hours)


@router.post("/cache/refresh")
def refresh_cache():
    """Clear the operations in-memory cache."""
    operations_service.clear_cache()
    return {"status": "ok", "message": "Operations cache cleared"}
