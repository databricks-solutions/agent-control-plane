"""Pydantic models for Request log entities."""
from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class RequestOut(BaseModel):
    """Output model for a request log."""
    request_id: str
    agent_id: Optional[str] = None
    model_id: Optional[str] = None
    user_id: Optional[str] = None
    timestamp: datetime
    query_text: Optional[str] = None
    response_text: Optional[str] = None
    latency_ms: Optional[int] = None
    status_code: Optional[int] = None
    input_tokens: Optional[int] = None
    output_tokens: Optional[int] = None
    cost_usd: Optional[float] = None
    error_message: Optional[str] = None
    endpoint_type: Optional[str] = None
    
    class Config:
        from_attributes = True


class RequestListOut(BaseModel):
    """Summary model for request list."""
    request_id: str
    agent_id: Optional[str] = None
    user_id: Optional[str] = None
    timestamp: datetime
    latency_ms: Optional[int] = None
    status_code: Optional[int] = None
    cost_usd: Optional[float] = None
    
    class Config:
        from_attributes = True


class RequestFilters(BaseModel):
    """Filters for querying requests."""
    agent_id: Optional[str] = None
    user_id: Optional[str] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    status_code: Optional[int] = None
    limit: int = Field(default=100, le=1000)
    offset: int = Field(default=0, ge=0)
