"""API routes for analytics."""
from fastapi import APIRouter, Depends, Query
from backend.utils.auth import get_current_user, UserInfo
from backend.utils.access_scope import get_allowed_workspace_ids, sees_deploy_workspace
from backend.models.analytics import PerformanceMetrics, UsageMetrics, CostMetrics, HealthMetrics
from backend.services.analytics_service import (
    get_performance_metrics,
    get_usage_metrics,
    get_cost_metrics,
    get_health_metrics
)

router = APIRouter(prefix="/analytics", tags=["analytics"], dependencies=[Depends(get_current_user)])


def _sees_deploy(user: UserInfo) -> bool:
    return sees_deploy_workspace(get_allowed_workspace_ids(user))


@router.get("/performance", response_model=PerformanceMetrics)
async def get_performance(
    days: int = Query(default=30, ge=1, le=365),
    user: UserInfo = Depends(get_current_user),
):
    """Get performance analytics."""
    if not _sees_deploy(user):
        return PerformanceMetrics(
            response_time_series=[], response_time_by_agent_type={},
            throughput_series=[], p50_series=[], p95_series=[], p99_series=[],
        )
    return get_performance_metrics(days)


@router.get("/usage", response_model=UsageMetrics)
async def get_usage(
    days: int = Query(default=30, ge=1, le=365),
    user: UserInfo = Depends(get_current_user),
):
    """Get usage analytics."""
    if not _sees_deploy(user):
        return UsageMetrics(
            requests_by_agent={}, usage_over_time=[], usage_by_hour={},
            usage_by_day={}, top_agents=[], active_users_over_time=[],
        )
    return get_usage_metrics(days)


@router.get("/cost", response_model=CostMetrics)
async def get_cost(
    days: int = Query(default=30, ge=1, le=365),
    user: UserInfo = Depends(get_current_user),
):
    """Get cost analytics."""
    if not _sees_deploy(user):
        return CostMetrics(
            total_cost=0, cost_by_agent={}, cost_by_agent_type={},
            cost_trend=[], cost_per_request=0,
        )
    return get_cost_metrics(days)


@router.get("/health", response_model=HealthMetrics)
async def get_health(user: UserInfo = Depends(get_current_user)):
    """Get health metrics."""
    if not _sees_deploy(user):
        return HealthMetrics(
            agent_health=[], error_rate_trend=[], error_types={},
            errors_by_agent={}, recent_errors=[],
        )
    return get_health_metrics()
