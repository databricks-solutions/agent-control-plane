"""API routes for request logs."""
from fastapi import APIRouter, Depends, HTTPException, Query
from backend.utils.auth import get_current_user
from typing import List, Optional
from datetime import datetime
from backend.models.request import RequestOut, RequestListOut, RequestFilters
from backend.services.query_service import get_recent_requests, get_requests_with_filters
from backend.database import execute_one

router = APIRouter(prefix="/requests", tags=["requests"], dependencies=[Depends(get_current_user)])


@router.get("")
async def list_requests(
    agent_id: Optional[str] = Query(None),
    user_id: Optional[str] = Query(None),
    start_time: Optional[datetime] = Query(None),
    end_time: Optional[datetime] = Query(None),
    status_code: Optional[int] = Query(None),
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0)
):
    """List requests with optional filters."""
    filters = RequestFilters(
        agent_id=agent_id,
        user_id=user_id,
        start_time=start_time,
        end_time=end_time,
        status_code=status_code,
        limit=limit,
        offset=offset
    )
    requests = get_requests_with_filters(filters)
    return [RequestListOut(**req) for req in requests]


@router.get("/recent")
async def get_recent(limit: int = Query(default=20, le=100)):
    """Get recent requests."""
    return {"data": get_recent_requests(limit), "meta": {"count": limit}}


@router.get("/{request_id}", response_model=RequestOut)
async def get_request(request_id: str):
    """Get request details."""
    query = """
        SELECT request_id, agent_id, model_id, user_id, timestamp,
               query_text, response_text, latency_ms, status_code,
               input_tokens, output_tokens, cost_usd, error_message, endpoint_type
        FROM request_logs
        WHERE request_id = %s
    """
    result = execute_one(query, (request_id,))
    if not result:
        raise HTTPException(status_code=404, detail="Request not found")
    return RequestOut(**result)
