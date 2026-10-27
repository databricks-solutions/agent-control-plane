"""API routes for Vector Search monitoring."""
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from backend.utils.auth import get_current_user, require_admin, UserInfo
from backend.utils.access_scope import get_allowed_workspace_ids
from backend.services import vector_search_service

router = APIRouter(prefix="/vector-search", tags=["vector-search"],
                   dependencies=[Depends(get_current_user)])


@router.get("/page-data")
def page_data(days: int = Query(30, ge=1, le=365), user: UserInfo = Depends(get_current_user)):
    """All Vector Search data in one request for the page, scoped to the
    caller's workspace access (account admin = all, workspace admin = their
    workspaces, everyone else = empty)."""
    allowed = get_allowed_workspace_ids(user)
    return {
        "cost_summary": vector_search_service.get_cost_summary(days, allowed_workspace_ids=allowed),
        "cost_trend_by_workload": vector_search_service.get_cost_trend_by_workload(days, allowed_workspace_ids=allowed),
        "cost_by_workspace": vector_search_service.get_cost_by_workspace(days, allowed_workspace_ids=allowed),
        "cost_by_endpoint": vector_search_service.get_cost_by_endpoint(days, allowed_workspace_ids=allowed),
        "cost_by_workload": vector_search_service.get_cost_by_workload_type(days, allowed_workspace_ids=allowed),
        "overview": vector_search_service.get_overview(allowed_workspace_ids=allowed),
    }


@router.get("/overview")
def overview(user: UserInfo = Depends(get_current_user)):
    """KPI overview: endpoint/index counts, online/offline status."""
    return vector_search_service.get_overview(allowed_workspace_ids=get_allowed_workspace_ids(user))


@router.get("/endpoints")
def list_endpoints(user: UserInfo = Depends(get_current_user)):
    """All vector search endpoints with status."""
    return vector_search_service.get_endpoints(allowed_workspace_ids=get_allowed_workspace_ids(user))


@router.get("/indexes")
def list_indexes(endpoint_name: str = Query(None), user: UserInfo = Depends(get_current_user)):
    """All vector search indexes, optionally filtered by endpoint."""
    return vector_search_service.get_indexes(endpoint_name, allowed_workspace_ids=get_allowed_workspace_ids(user))


@router.get("/indexes/details")
def index_details(user: UserInfo = Depends(get_current_user)):
    """All indexes with detailed sync status (row count, state, source table, embedding model)."""
    return vector_search_service.get_index_details(allowed_workspace_ids=get_allowed_workspace_ids(user))


@router.get("/health/history")
def health_history(days: int = Query(7, ge=1, le=90), user: UserInfo = Depends(get_current_user)):
    """Endpoint health snapshots over time."""
    return vector_search_service.get_health_history(days, allowed_workspace_ids=get_allowed_workspace_ids(user))


@router.get("/cost/summary")
def cost_summary(days: int = Query(30, ge=1, le=365), user: UserInfo = Depends(get_current_user)):
    """Total vector search cost for the last N days."""
    return vector_search_service.get_cost_summary(days, allowed_workspace_ids=get_allowed_workspace_ids(user))


@router.get("/cost/trend")
def cost_trend(days: int = Query(30, ge=1, le=365), user: UserInfo = Depends(get_current_user)):
    """Daily cost trend."""
    return vector_search_service.get_cost_trend(days, allowed_workspace_ids=get_allowed_workspace_ids(user))


@router.get("/cost/by-endpoint")
def cost_by_endpoint(days: int = Query(30, ge=1, le=365), user: UserInfo = Depends(get_current_user)):
    """Cost breakdown per endpoint."""
    return vector_search_service.get_cost_by_endpoint(days, allowed_workspace_ids=get_allowed_workspace_ids(user))


@router.get("/cost/by-workspace")
def cost_by_workspace(days: int = Query(30, ge=1, le=365), user: UserInfo = Depends(get_current_user)):
    """Cost breakdown per workspace."""
    return vector_search_service.get_cost_by_workspace(days, allowed_workspace_ids=get_allowed_workspace_ids(user))


@router.get("/cost/by-workload")
def cost_by_workload(days: int = Query(30, ge=1, le=365), user: UserInfo = Depends(get_current_user)):
    """Cost split: ingest vs serving vs storage."""
    return vector_search_service.get_cost_by_workload_type(days, allowed_workspace_ids=get_allowed_workspace_ids(user))


@router.get("/cost/trend-by-workload")
def cost_trend_by_workload(days: int = Query(30, ge=1, le=365), user: UserInfo = Depends(get_current_user)):
    """Daily cost trend broken down by workload type (for stacked chart)."""
    return vector_search_service.get_cost_trend_by_workload(days, allowed_workspace_ids=get_allowed_workspace_ids(user))


@router.post("/refresh")
def refresh(user: UserInfo = Depends(require_admin)):
    """Trigger manual discovery refresh."""
    counts = vector_search_service.discover_vector_search()
    return {"status": "ok", **counts}


# ── Lakebase ────────────────────────────────────────────────────

@router.get("/lakebase/instances")
def lakebase_instances(user: UserInfo = Depends(get_current_user)):
    """Lakebase database instances (from cache, populated by workflow)."""
    return vector_search_service.get_lakebase_instances(allowed_workspace_ids=get_allowed_workspace_ids(user))


@router.get("/lakebase/cost/summary")
def lakebase_cost_summary(days: int = Query(30, ge=1, le=365), user: UserInfo = Depends(get_current_user)):
    """Total Lakebase cost."""
    return vector_search_service.get_lakebase_cost_summary(days, allowed_workspace_ids=get_allowed_workspace_ids(user))


@router.get("/lakebase/cost/trend")
def lakebase_cost_trend(days: int = Query(30, ge=1, le=365), user: UserInfo = Depends(get_current_user)):
    """Daily Lakebase cost trend."""
    return vector_search_service.get_lakebase_cost_trend(days, allowed_workspace_ids=get_allowed_workspace_ids(user))


@router.get("/lakebase/cost/by-workspace")
def lakebase_cost_by_workspace(days: int = Query(30, ge=1, le=365), user: UserInfo = Depends(get_current_user)):
    """Lakebase cost per workspace."""
    return vector_search_service.get_lakebase_cost_by_workspace(days, allowed_workspace_ids=get_allowed_workspace_ids(user))


@router.get("/lakebase/cost/by-type")
def lakebase_cost_by_type(days: int = Query(30, ge=1, le=365), user: UserInfo = Depends(get_current_user)):
    """Lakebase cost split: compute vs storage."""
    return vector_search_service.get_lakebase_cost_by_type(days, allowed_workspace_ids=get_allowed_workspace_ids(user))


# ── Combined (Knowledge Bases overview) ─────────────────────────

@router.get("/combined/overview")
def combined_overview(days: int = Query(30, ge=1, le=365), user: UserInfo = Depends(get_current_user)):
    """Combined overview for VS + Lakebase."""
    return vector_search_service.get_combined_overview(days, allowed_workspace_ids=get_allowed_workspace_ids(user))


@router.get("/combined/top-workspaces-daily")
def combined_top_workspaces_daily(days: int = Query(30, ge=1, le=365), user: UserInfo = Depends(get_current_user)):
    """Daily cost trend for top 5 workspaces (combined VS + Lakebase)."""
    return vector_search_service.get_top_workspaces_daily_trend(days, allowed_workspace_ids=get_allowed_workspace_ids(user))


@router.get("/cost/top-workspaces-daily")
def vs_top_workspaces_daily(days: int = Query(30, ge=1, le=365), user: UserInfo = Depends(get_current_user)):
    """Daily VS cost for top 5 workspaces."""
    return vector_search_service.get_vs_top_workspaces_daily(days, allowed_workspace_ids=get_allowed_workspace_ids(user))


@router.get("/lakebase/cost/top-workspaces-daily")
def lb_top_workspaces_daily(days: int = Query(30, ge=1, le=365), user: UserInfo = Depends(get_current_user)):
    """Daily Lakebase cost for top 5 workspaces."""
    return vector_search_service.get_lb_top_workspaces_daily(days, allowed_workspace_ids=get_allowed_workspace_ids(user))


@router.get("/combined/cost-trend")
def combined_cost_trend(days: int = Query(30, ge=1, le=365), user: UserInfo = Depends(get_current_user)):
    """Daily cost trend for both products."""
    return vector_search_service.get_combined_cost_trend(days, allowed_workspace_ids=get_allowed_workspace_ids(user))


# ── Retrieval & Memory: Vector Search index usage ───────────────

def _scope(user: UserInfo, workspace_ids: Optional[str]) -> Optional[List[str]]:
    """Caller's access scope, optionally narrowed by the page's workspace selector
    (comma-separated). The selection is INTERSECTED with the scope, so it can only
    narrow what the caller may see — never widen it."""
    allowed = get_allowed_workspace_ids(user)
    selected = [w.strip() for w in (workspace_ids or "").split(",") if w.strip()]
    if not selected:
        return allowed
    if allowed is None:
        return selected
    return [w for w in selected if w in set(allowed)]

@router.get("/usage/trend")
def usage_trend(days: int = Query(30, ge=1, le=365), workspace_ids: Optional[str] = Query(None), user: UserInfo = Depends(get_current_user)):
    """Daily Vector Search requests + errors, split by action (from audit)."""
    return vector_search_service.get_vs_usage_trend(days, allowed_workspace_ids=_scope(user, workspace_ids))


@router.get("/usage/indexes")
def usage_indexes(days: int = Query(30, ge=1, le=365), workspace_ids: Optional[str] = Query(None), user: UserInfo = Depends(get_current_user)):
    """Per-index query/error/caller usage with an Agent Bricks managed_by label."""
    return vector_search_service.get_vs_index_usage(days, allowed_workspace_ids=_scope(user, workspace_ids))


@router.get("/usage/callers")
def usage_callers(
    index_name: str = Query(..., min_length=1),
    workspace_id: str = Query(None),
    user: UserInfo = Depends(get_current_user),
):
    """Top callers of one index (drilldown)."""
    return vector_search_service.get_vs_index_callers(
        index_name, workspace_id, allowed_workspace_ids=get_allowed_workspace_ids(user))


# ── Retrieval & Memory: Lakebase operational health ─────────────

@router.get("/lakebase/health")
def lakebase_health(days: int = Query(14, ge=1, le=90), workspace_ids: Optional[str] = Query(None), user: UserInfo = Depends(get_current_user)):
    """Daily connections + database size per project/branch."""
    return vector_search_service.get_lakebase_health(days, allowed_workspace_ids=_scope(user, workspace_ids))


@router.get("/lakebase/wait-events")
def lakebase_wait_events(limit: int = Query(25, ge=1, le=100), workspace_ids: Optional[str] = Query(None), user: UserInfo = Depends(get_current_user)):
    """Top Lakebase wait events by total wait time."""
    return vector_search_service.get_lakebase_wait_events(limit, allowed_workspace_ids=_scope(user, workspace_ids))


@router.get("/lakebase/cost/by-project")
def lakebase_cost_by_project(days: int = Query(30, ge=1, le=365), workspace_ids: Optional[str] = Query(None), user: UserInfo = Depends(get_current_user)):
    """Lakebase cost per project/branch."""
    return vector_search_service.get_lakebase_cost_by_project(days, allowed_workspace_ids=_scope(user, workspace_ids))
