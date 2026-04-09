"""Pydantic models for KPI metrics."""
from pydantic import BaseModel
from typing import Optional
from datetime import datetime


class KPIOverview(BaseModel):
    """Overview KPIs for the dashboard."""
    total_requests_24h: int
    total_requests_7d: int
    total_requests_30d: int
    avg_latency_ms: Optional[float] = None
    p50_latency_ms: Optional[float] = None
    p95_latency_ms: Optional[float] = None
    error_rate_pct: Optional[float] = None
    total_cost_24h: Optional[float] = None
    total_cost_7d: Optional[float] = None
    total_cost_30d: Optional[float] = None
    active_agents: int
    total_agents: int
    active_users_24h: int
    timestamp: datetime


class KPITrend(BaseModel):
    """KPI with trend comparison."""
    current: float
    previous: float
    change_pct: float
    trend: str  # "up", "down", "stable"
