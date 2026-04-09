"""API routes for Admin access management — UC grants on AI resources.

All write endpoints require workspace-admin privileges (OBO auth).
Read endpoints require any authenticated user.
"""
from fastapi import APIRouter, Query, HTTPException, Depends, Request
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
from backend.services.access_service import (
    get_resource_permissions,
    grant_permission,
    revoke_permission,
    get_all_principals,
)
from backend.utils.auth import get_current_user, require_admin, UserInfo

router = APIRouter(prefix="/admin", tags=["admin"], dependencies=[Depends(get_current_user)])


class GrantRequest(BaseModel):
    resource_type: str
    resource_name: str
    principal: str
    privileges: List[str]
    principal_type: str = "user"


class RevokeRequest(BaseModel):
    resource_type: str
    resource_name: str
    principal: str
    privileges: List[str]


@router.get("/permissions")
def list_permissions(
    resource_type: str = Query(..., description="serving_endpoint, function, table, schema, catalog"),
    resource_name: str = Query(..., description="Endpoint name or full UC name"),
    user: UserInfo = Depends(get_current_user),
) -> List[Dict[str, Any]]:
    """Get permissions on a specific resource."""
    return get_resource_permissions(resource_type, resource_name)


@router.post("/permissions/grant")
def grant(body: GrantRequest, user: UserInfo = Depends(require_admin)) -> Dict[str, Any]:
    """Grant permissions on a resource (admin only)."""
    ok = grant_permission(
        body.resource_type,
        body.resource_name,
        body.principal,
        body.privileges,
        body.principal_type,
    )
    if not ok:
        raise HTTPException(status_code=400, detail="Grant failed — check logs")
    return {"status": "ok", "message": f"Granted {body.privileges} to {body.principal}", "granted_by": user.username}


@router.post("/permissions/revoke")
def revoke(body: RevokeRequest, user: UserInfo = Depends(require_admin)) -> Dict[str, Any]:
    """Revoke permissions from a resource (admin only)."""
    ok = revoke_permission(
        body.resource_type,
        body.resource_name,
        body.principal,
        body.privileges,
    )
    if not ok:
        raise HTTPException(status_code=400, detail="Revoke failed — check logs")
    return {"status": "ok", "message": f"Revoked {body.privileges} from {body.principal}", "revoked_by": user.username}


@router.get("/search-principals")
def search_principals_route(
    q: str = Query(..., min_length=1, description="Search query"),
    type: Optional[str] = Query(None, description="user, group, service_principal"),
    limit: int = Query(20, le=100),
    user: UserInfo = Depends(get_current_user),
) -> List[Dict[str, Any]]:
    """Search for principals (users/groups/SPs) by name — for autocomplete."""
    from backend.services.access_service import search_principals
    return search_principals(q, type, limit)


@router.get("/principals")
def list_principals(
    days: int = Query(default=30, ge=1, le=365),
    user: UserInfo = Depends(get_current_user),
) -> List[Dict[str, Any]]:
    """Get all principals with access to AI resources (serving endpoints + usage)."""
    return get_all_principals(days=days)
