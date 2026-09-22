"""API routes for live serving inventory (read-only)."""
from fastapi import APIRouter, Depends
from backend.utils.auth import require_user, UserInfo
from backend.utils.access_scope import get_allowed_workspace_ids, sees_deploy_workspace

from backend.services import serving_service

router = APIRouter(prefix="/serving", tags=["serving"], dependencies=[Depends(require_user)])


@router.get("/queryable-endpoints")
def list_queryable_endpoints(user: UserInfo = Depends(require_user)):
    """Return serving endpoints the app can query (READY + running Apps).

    Deploy-workspace-scoped: only callers who can see the deploy workspace get
    the live list; everyone else gets an empty list.
    """
    if not sees_deploy_workspace(get_allowed_workspace_ids(user)):
        return []
    return serving_service.list_queryable_endpoints()
