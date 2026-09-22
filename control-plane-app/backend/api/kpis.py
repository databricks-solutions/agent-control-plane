"""API routes for KPIs."""
from fastapi import APIRouter, Depends
from backend.utils.auth import get_current_user, UserInfo
from backend.utils.access_scope import get_allowed_workspace_ids, sees_deploy_workspace
from backend.models.kpi import KPIOverview
from backend.services.query_service import get_kpi_overview
from datetime import datetime

router = APIRouter(prefix="/kpis", tags=["kpis"], dependencies=[Depends(get_current_user)])


_EMPTY_KPIS = {
    "total_requests_24h": 0,
    "total_requests_7d": 0,
    "total_requests_30d": 0,
    "avg_latency": None,
    "p50_latency": None,
    "p95_latency": None,
    "error_rate": None,
    "total_cost_24h": 0,
    "total_cost_7d": 0,
    "total_cost_30d": 0,
    "active_agents": 0,
    "total_agents": 0,
    "active_users_24h": 0,
}


@router.get("", response_model=KPIOverview)
async def get_kpis(user: UserInfo = Depends(get_current_user)):
    """Get all KPI values."""
    allowed = get_allowed_workspace_ids(user)
    if allowed is not None and not sees_deploy_workspace(allowed):
        kpis = {**_EMPTY_KPIS, "timestamp": datetime.now()}
    else:
        kpis = get_kpi_overview()
    return KPIOverview(
        total_requests_24h=kpis.get("total_requests_24h") or 0,
        total_requests_7d=kpis.get("total_requests_7d") or 0,
        total_requests_30d=kpis.get("total_requests_30d") or 0,
        avg_latency_ms=kpis.get("avg_latency"),
        p50_latency_ms=kpis.get("p50_latency"),
        p95_latency_ms=kpis.get("p95_latency"),
        error_rate_pct=kpis.get("error_rate"),
        total_cost_24h=kpis.get("total_cost_24h"),
        total_cost_7d=kpis.get("total_cost_7d"),
        total_cost_30d=kpis.get("total_cost_30d"),
        active_agents=kpis.get("active_agents") or 0,
        total_agents=kpis.get("total_agents") or 0,
        active_users_24h=kpis.get("active_users_24h") or 0,
        timestamp=kpis["timestamp"],
    )


@router.get("/overview")
async def get_overview(user: UserInfo = Depends(get_current_user)):
    """Get overview KPIs (all in one response)."""
    allowed = get_allowed_workspace_ids(user)
    if allowed is not None and not sees_deploy_workspace(allowed):
        kpis = {**_EMPTY_KPIS, "timestamp": datetime.now().isoformat()}
    else:
        kpis = get_kpi_overview()
    return {"data": kpis, "meta": {"timestamp": kpis["timestamp"]}}
