"""API routes for Vector Search monitoring."""
from fastapi import APIRouter, Depends, Query
from backend.utils.auth import get_current_user
from backend.services import vector_search_service

router = APIRouter(prefix="/vector-search", tags=["vector-search"],
                   dependencies=[Depends(get_current_user)])


@router.get("/overview")
def overview():
    """KPI overview: endpoint/index counts, online/offline status."""
    return vector_search_service.get_overview()


@router.get("/endpoints")
def list_endpoints():
    """All vector search endpoints with status."""
    return vector_search_service.get_endpoints()


@router.get("/indexes")
def list_indexes(endpoint_name: str = Query(None)):
    """All vector search indexes, optionally filtered by endpoint."""
    return vector_search_service.get_indexes(endpoint_name)


@router.get("/cost/summary")
def cost_summary(days: int = Query(30, ge=1, le=365)):
    """Total vector search cost for the last N days."""
    return vector_search_service.get_cost_summary(days)


@router.get("/cost/trend")
def cost_trend(days: int = Query(30, ge=1, le=365)):
    """Daily cost trend."""
    return vector_search_service.get_cost_trend(days)


@router.get("/cost/by-endpoint")
def cost_by_endpoint(days: int = Query(30, ge=1, le=365)):
    """Cost breakdown per endpoint."""
    return vector_search_service.get_cost_by_endpoint(days)


@router.get("/cost/by-workspace")
def cost_by_workspace(days: int = Query(30, ge=1, le=365)):
    """Cost breakdown per workspace."""
    return vector_search_service.get_cost_by_workspace(days)


@router.get("/cost/by-workload")
def cost_by_workload(days: int = Query(30, ge=1, le=365)):
    """Cost split: ingest vs serving vs storage."""
    return vector_search_service.get_cost_by_workload_type(days)


@router.post("/refresh")
def refresh():
    """Trigger manual discovery refresh."""
    counts = vector_search_service.discover_vector_search()
    return {"status": "ok", **counts}
