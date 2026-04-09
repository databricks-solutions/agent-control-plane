"""Pydantic models for AI Gateway data."""
from typing import Optional, List, Dict, Any
from pydantic import BaseModel
from datetime import datetime


class GatewayEndpointOut(BaseModel):
    endpoint_id: str
    name: str
    endpoint_type: str
    provider: Optional[str] = None
    model_name: Optional[str] = None
    status: Optional[str] = "ACTIVE"
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    config: Optional[Dict[str, Any]] = None
    tags: Optional[Dict[str, Any]] = None


class GatewayPermissionOut(BaseModel):
    permission_id: int
    endpoint_id: str
    principal_type: str
    principal_name: str
    permission_level: str
    granted_by: Optional[str] = None
    granted_at: Optional[datetime] = None


class GatewayRateLimitOut(BaseModel):
    rate_limit_id: int
    endpoint_id: str
    scope: str
    scope_value: Optional[str] = None
    max_requests_per_minute: Optional[int] = None
    max_tokens_per_minute: Optional[int] = None
    max_requests_per_day: Optional[int] = None
    renewal_period: Optional[str] = "minute"
    is_active: Optional[bool] = True
    created_at: Optional[datetime] = None


class GatewayFallbackOut(BaseModel):
    fallback_id: int
    endpoint_id: str
    fallback_order: int
    fallback_provider: str
    fallback_model: str
    trigger_on: Optional[str] = "error"
    timeout_ms: Optional[int] = 30000
    is_active: Optional[bool] = True
    created_at: Optional[datetime] = None


class GatewayUsageSummary(BaseModel):
    endpoint_id: str
    endpoint_name: Optional[str] = None
    total_requests: int = 0
    total_input_tokens: int = 0
    total_output_tokens: int = 0
    total_tokens: int = 0
    total_cost: float = 0.0
    total_errors: int = 0
    avg_latency_ms: Optional[float] = None
    p95_latency_ms: Optional[float] = None


class GatewayUsageTimeSeries(BaseModel):
    hour: str
    request_count: int = 0
    total_tokens: int = 0
    cost_usd: float = 0.0
    error_count: int = 0
    avg_latency_ms: Optional[float] = None


class GatewayInferenceLogOut(BaseModel):
    log_id: str
    endpoint_id: str
    timestamp: Optional[datetime] = None
    user_id: Optional[str] = None
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    latency_ms: Optional[int] = None
    status_code: Optional[int] = None
    cost_usd: Optional[float] = None
    model_name: Optional[str] = None
    provider: Optional[str] = None
    error_message: Optional[str] = None


class GatewayOverview(BaseModel):
    total_endpoints: int = 0
    active_endpoints: int = 0
    degraded_endpoints: int = 0
    total_requests_24h: int = 0
    total_tokens_24h: int = 0
    total_cost_24h: float = 0.0
    error_rate_24h: float = 0.0
    avg_latency_24h: Optional[float] = None
    providers: Dict[str, int] = {}
    endpoint_types: Dict[str, int] = {}
