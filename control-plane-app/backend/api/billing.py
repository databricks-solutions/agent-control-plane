"""API routes for billing / cost data – reads from Lakebase cache.

IMPORTANT: All routes are plain ``def`` (not ``async def``) so that FastAPI
automatically runs them in a thread-pool.  This prevents synchronous
psycopg2 calls from blocking the event loop and allows parallel execution.

Access scope: every route below is filtered to the caller's workspace
access via ``resolve_scope`` (see ``backend.utils.access_scope``). Real
account admins are unrestricted; workspace admins see only the workspaces
they administer; everyone else sees empty results. This must stay in sync
with the identical filtering in ``workspaces.py`` / ``agents.py``.
"""
from typing import Optional, Dict, Any, List
from fastapi import APIRouter, Depends, Query
from backend.utils.auth import get_current_user, require_admin, UserInfo
from backend.utils.access_scope import resolve_scope
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
    get_cost_by_user,
)

router = APIRouter(prefix="/billing", tags=["billing"], dependencies=[Depends(get_current_user)])


# ── composite: all data in one request ───────────────────────────

@router.get("/page-data")
def page_data(
    days: int = Query(default=30, ge=1, le=365),
    workspace_id: Optional[str] = Query(default=None),
    workspace_ids: Optional[str] = Query(
        default=None,
        description="Comma-separated workspace ids for multi-select; when present, "
                    "overrides workspace_id. Empty/omitted = all workspaces.",
    ),
    user: UserInfo = Depends(get_current_user),
):
    """Return ALL billing data the Governance page needs in one round-trip.

    This avoids 7+ parallel HTTP requests that each open a new Lakebase
    connection (each taking ~1 s SSL handshake from local dev).
    """
    # Multi-select ids take precedence over single workspace_id; the selection
    # is then intersected with the caller's access scope in get_all_page_data.
    ids = [w for w in (workspace_ids.split(",") if workspace_ids else []) if w]
    allowed = resolve_scope(user, None)
    return get_all_page_data(days, workspace_id=(ids or workspace_id), allowed_workspace_ids=allowed)


# ── cache management ─────────────────────────────────────────────

@router.get("/cache/status")
def cache_status(user: UserInfo = Depends(get_current_user)) -> Dict[str, Any]:
    """Return cache freshness info."""
    status = get_cache_status()
    # rows_loaded is an account-wide count — hide it from scoped callers.
    # Keep last_refreshed so the Shared workspace tab can show job age.
    if resolve_scope(user, None) is not None:
        caches = {
            k: {"last_refreshed": (v or {}).get("last_refreshed")}
            for k, v in (status.get("caches") or {}).items()
        }
        return {"is_refreshing": status.get("is_refreshing", False), "caches": caches}
    return status


@router.post("/cache/refresh")
def cache_refresh(days: int = Query(default=90, ge=1, le=365), user: UserInfo = Depends(require_admin)):
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
def list_workspaces(
    days: int = Query(default=30, ge=1, le=365),
    user: UserInfo = Depends(get_current_user),
):
    """Return all workspaces visible in billing data, scoped to the caller."""
    allowed = resolve_scope(user, None)
    return get_available_workspaces(days, allowed_workspace_ids=allowed)


# ── billing queries (all read from Lakebase cache) ───────────────

@router.get("/serving/summary")
def serving_summary(
    days: int = Query(default=30, ge=1, le=365),
    workspace_id: Optional[str] = Query(default=None),
    user: UserInfo = Depends(get_current_user),
):
    """Cost summary for model-serving endpoints."""
    allowed = resolve_scope(user, workspace_id)
    return get_serving_cost_summary(days, workspace_id=workspace_id, allowed_workspace_ids=allowed)


@router.get("/serving/trend")
def serving_trend(
    days: int = Query(default=30, ge=1, le=365),
    workspace_id: Optional[str] = Query(default=None),
    user: UserInfo = Depends(get_current_user),
):
    """Daily cost trend for model serving."""
    allowed = resolve_scope(user, workspace_id)
    return get_serving_cost_trend(days, workspace_id=workspace_id, allowed_workspace_ids=allowed)


@router.get("/serving/by-sku")
def serving_by_sku(
    days: int = Query(default=30, ge=1, le=365),
    workspace_id: Optional[str] = Query(default=None),
    user: UserInfo = Depends(get_current_user),
):
    """Cost broken down by SKU."""
    allowed = resolve_scope(user, workspace_id)
    return get_serving_cost_by_sku(days, workspace_id=workspace_id, allowed_workspace_ids=allowed)


@router.get("/serving/tokens")
def serving_tokens(
    days: int = Query(default=30, ge=1, le=365),
    workspace_id: Optional[str] = Query(default=None),
    user: UserInfo = Depends(get_current_user),
):
    """Per-endpoint token usage."""
    allowed = resolve_scope(user, workspace_id)
    return get_serving_token_usage(days, workspace_id=workspace_id, allowed_workspace_ids=allowed)


@router.get("/serving/daily-tokens")
def serving_daily_tokens(
    days: int = Query(default=30, ge=1, le=365),
    workspace_id: Optional[str] = Query(default=None),
    user: UserInfo = Depends(get_current_user),
):
    """Daily aggregated token counts."""
    allowed = resolve_scope(user, workspace_id)
    return get_serving_daily_tokens(days, workspace_id=workspace_id, allowed_workspace_ids=allowed)


@router.get("/products")
def product_costs(
    days: int = Query(default=30, ge=1, le=365),
    workspace_id: Optional[str] = Query(default=None),
    user: UserInfo = Depends(get_current_user),
):
    """Total cost per Databricks product."""
    allowed = resolve_scope(user, workspace_id)
    return get_all_product_costs(days, workspace_id=workspace_id, allowed_workspace_ids=allowed)


@router.get("/cost-by-user")
def cost_by_user(
    days: int = Query(default=30, ge=1, le=365),
    workspace_id: Optional[str] = Query(default=None),
    user: UserInfo = Depends(get_current_user),
):
    """Top users by cost. Prefers ACTUAL Unity Gateway v2 attribution and
    falls back to the token-share estimate; response ``source`` says which."""
    allowed = resolve_scope(user, workspace_id)
    return get_cost_by_user(days, workspace_id=workspace_id, allowed_workspace_ids=allowed)
