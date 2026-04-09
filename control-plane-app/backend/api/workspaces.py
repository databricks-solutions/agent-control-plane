"""API routes for the Multi-Workspace Federation page.

IMPORTANT: All routes are plain ``def`` (not ``async def``) so that FastAPI
automatically runs them in a thread-pool.  This prevents synchronous
psycopg2 calls from blocking the event loop.
"""
from fastapi import APIRouter, Depends, Query
from backend.utils.auth import get_current_user
from backend.services.workspace_service import get_workspaces_page_data

router = APIRouter(prefix="/workspaces", tags=["workspaces"], dependencies=[Depends(get_current_user)])


@router.get("/page-data")
def page_data(
    days: int = Query(default=30, ge=1, le=365),
):
    """Return ALL workspace-federation data the Workspaces page needs."""
    return get_workspaces_page_data(days)
