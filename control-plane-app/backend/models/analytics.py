"""Pydantic models for Analytics data."""
from pydantic import BaseModel
from typing import List, Optional, Dict, Any
from datetime import datetime


class TimeSeriesPoint(BaseModel):
    """A single point in a time series."""
    timestamp: datetime
    value: float
    label: Optional[str] = None


class PerformanceMetrics(BaseModel):
    """Performance analytics data."""
    response_time_series: List[TimeSeriesPoint]
    response_time_by_agent_type: Dict[str, float]
    throughput_series: List[TimeSeriesPoint]
    p50_series: List[TimeSeriesPoint]
    p95_series: List[TimeSeriesPoint]
    p99_series: List[TimeSeriesPoint]


class UsageMetrics(BaseModel):
    """Usage analytics data."""
    requests_by_agent: Dict[str, int]
    usage_over_time: List[TimeSeriesPoint]
    usage_by_hour: Dict[int, int]
    usage_by_day: Dict[str, int]
    top_agents: List[Dict[str, Any]]
    active_users_over_time: List[TimeSeriesPoint]


class CostMetrics(BaseModel):
    """Cost analytics data."""
    total_cost: float
    cost_by_agent: Dict[str, float]
    cost_by_agent_type: Dict[str, float]
    cost_trend: List[TimeSeriesPoint]
    cost_per_request: float
    cost_forecast: Optional[List[TimeSeriesPoint]] = None


class HealthMetrics(BaseModel):
    """Health monitoring data."""
    agent_health: List[Dict[str, Any]]
    error_rate_trend: List[TimeSeriesPoint]
    error_types: Dict[str, int]
    errors_by_agent: Dict[str, int]
    recent_errors: List[Dict[str, Any]]
