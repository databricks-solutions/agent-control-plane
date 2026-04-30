"""API routes for AI Gateway / Model Serving inference logs (Tier 2a)."""
from typing import Optional
from fastapi import APIRouter, Depends, HTTPException, Query

from backend.services import gateway_logs_service
from backend.utils.auth import get_current_user

router = APIRouter(
    prefix="/gateway-logs",
    tags=["gateway-logs"],
    dependencies=[Depends(get_current_user)],
)


@router.get("")
async def list_logs(
    source_table: Optional[str] = Query(None, description="Filter to a single source table"),
    window_days: Optional[int] = Query(None, ge=1, le=365),
    limit: int = Query(500, le=10000),
):
    """List gateway inference-log rows (lightweight — payload sizes only)."""
    try:
        return gateway_logs_service.list_gateway_logs(
            source_table=source_table,
            window_days=window_days,
            limit=limit,
            include_payload=False,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Lakebase query failed: {e}")


@router.get("/sources")
async def list_sources():
    """List distinct source tables with row counts + recency."""
    try:
        return gateway_logs_service.list_source_tables()
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Lakebase query failed: {e}")


@router.get("/timeseries")
async def gateway_timeseries(
    source_table: Optional[str] = Query(None),
    window_days: int = Query(7, ge=1, le=365),
    bucket: str = Query("hour", pattern="^(hour|day)$"),
):
    """Return per-bucket aggregates for the Gateway Requests time-series chart."""
    try:
        return gateway_logs_service.gateway_timeseries(
            source_table=source_table,
            window_days=window_days,
            bucket=bucket,
        )
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Lakebase query failed: {e}")


@router.get("/{source_table:path}/{request_id}")
async def get_log(source_table: str, request_id: str):
    """Get a single inference-log row including the full request and response payloads."""
    try:
        row = gateway_logs_service.get_gateway_log(source_table, request_id)
        if not row:
            raise HTTPException(status_code=404, detail="Log row not found")
        return row
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=502, detail=f"Lakebase query failed: {e}")
