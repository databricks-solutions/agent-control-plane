"""API routes for KPIs."""
from fastapi import APIRouter, Depends
from backend.utils.auth import get_current_user
from backend.models.kpi import KPIOverview
from backend.services.query_service import get_kpi_overview

router = APIRouter(prefix="/kpis", tags=["kpis"], dependencies=[Depends(get_current_user)])


@router.get("", response_model=KPIOverview)
async def get_kpis():
    """Get all KPI values."""
    kpis = get_kpi_overview()
    return KPIOverview(**kpis)


@router.get("/overview")
async def get_overview():
    """Get overview KPIs (all in one response)."""
    kpis = get_kpi_overview()
    return {"data": kpis, "meta": {"timestamp": kpis["timestamp"]}}
