"""API routes for analytics."""
from fastapi import APIRouter, Depends, Query
from backend.utils.auth import get_current_user
from backend.models.analytics import PerformanceMetrics, UsageMetrics, CostMetrics, HealthMetrics
from backend.services.analytics_service import (
    get_performance_metrics,
    get_usage_metrics,
    get_cost_metrics,
    get_health_metrics
)

router = APIRouter(prefix="/analytics", tags=["analytics"], dependencies=[Depends(get_current_user)])


@router.get("/performance", response_model=PerformanceMetrics)
async def get_performance(days: int = Query(default=30, ge=1, le=365)):
    """Get performance analytics."""
    return get_performance_metrics(days)


@router.get("/usage", response_model=UsageMetrics)
async def get_usage(days: int = Query(default=30, ge=1, le=365)):
    """Get usage analytics."""
    return get_usage_metrics(days)


@router.get("/cost", response_model=CostMetrics)
async def get_cost(days: int = Query(default=30, ge=1, le=365)):
    """Get cost analytics."""
    return get_cost_metrics(days)


@router.get("/health", response_model=HealthMetrics)
async def get_health():
    """Get health metrics."""
    return get_health_metrics()
