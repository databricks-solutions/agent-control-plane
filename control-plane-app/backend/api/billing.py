"""API routes for billing / cost data – reads from Lakebase cache.

IMPORTANT: All routes are plain ``def`` (not ``async def``) so that FastAPI
automatically runs them in a thread-pool.  This prevents synchronous
psycopg2 calls from blocking the event loop and allows parallel execution.
"""
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, Depends, Query
from backend.utils.auth import get_current_user
from backend.services.billing_service import (
    get_serving_cost_summary,
    get_serving_cost_trend,
    get_serving_cost_by_sku,
    get_serving_token_usage,
    get_serving_daily_tokens,
    get_all_product_costs,
    get_current_workspace_id,
    get_available_workspaces,
    get_cache_status,
    force_refresh_async,
    maybe_refresh_async,
    get_all_page_data,
)

router = APIRouter(prefix="/billing", tags=["billing"], dependencies=[Depends(get_current_user)])


# ── composite: all data in one request ───────────────────────────

@router.get("/page-data")
def page_data(
    days: int = Query(default=30, ge=1, le=365),
    workspace_id: Optional[str] = Query(default=None),
):
    """Return ALL billing data the Governance page needs in one round-trip.

    This avoids 7+ parallel HTTP requests that each open a new Lakebase
    connection (each taking ~1 s SSL handshake from local dev).
    """
    return get_all_page_data(days, workspace_id=workspace_id)


# ── cache management ─────────────────────────────────────────────

@router.get("/cache/status")
def cache_status() -> Dict[str, Any]:
    """Return cache freshness info."""
    return get_cache_status()


@router.post("/cache/refresh")
def cache_refresh(days: int = Query(default=90, ge=1, le=365)):
    """Kick off a background cache refresh from system tables (non-blocking).

    The refresh runs in a daemon thread.  Poll ``GET /billing/cache/status``
    to check progress (``is_refreshing`` flag).
    """
    force_refresh_async(days)
    return {"status": "accepted", "message": "Refresh started in background"}


# ── workspace helpers ────────────────────────────────────────────

@router.get("/current-workspace")
def current_workspace():
    """Return the workspace_id of the current workspace."""
    ws_id = get_current_workspace_id()
    return {"workspace_id": ws_id}


@router.get("/workspaces")
def list_workspaces(days: int = Query(default=30, ge=1, le=365)):
    """Return all workspaces visible in billing data."""
    return get_available_workspaces(days)


# ── billing queries (all read from Lakebase cache) ───────────────

@router.get("/serving/summary")
def serving_summary(
    days: int = Query(default=30, ge=1, le=365),
    workspace_id: Optional[str] = Query(default=None),
):
    """Cost summary for model-serving endpoints."""
    return get_serving_cost_summary(days, workspace_id=workspace_id)


@router.get("/serving/trend")
def serving_trend(
    days: int = Query(default=30, ge=1, le=365),
    workspace_id: Optional[str] = Query(default=None),
):
    """Daily cost trend for model serving."""
    return get_serving_cost_trend(days, workspace_id=workspace_id)


@router.get("/serving/by-sku")
def serving_by_sku(
    days: int = Query(default=30, ge=1, le=365),
    workspace_id: Optional[str] = Query(default=None),
):
    """Cost broken down by SKU."""
    return get_serving_cost_by_sku(days, workspace_id=workspace_id)


@router.get("/serving/tokens")
def serving_tokens(
    days: int = Query(default=30, ge=1, le=365),
    workspace_id: Optional[str] = Query(default=None),
):
    """Per-endpoint token usage."""
    return get_serving_token_usage(days, workspace_id=workspace_id)


@router.get("/serving/daily-tokens")
def serving_daily_tokens(
    days: int = Query(default=30, ge=1, le=365),
    workspace_id: Optional[str] = Query(default=None),
):
    """Daily aggregated token counts."""
    return get_serving_daily_tokens(days, workspace_id=workspace_id)


@router.get("/products")
def product_costs(
    days: int = Query(default=30, ge=1, le=365),
    workspace_id: Optional[str] = Query(default=None),
):
    """Total cost per Databricks product."""
    return get_all_product_costs(days, workspace_id=workspace_id)
