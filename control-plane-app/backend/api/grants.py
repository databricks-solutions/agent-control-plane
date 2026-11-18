"""Access management routes — who can use an object, read and changed as the caller.

With OBO the caller's own token is used, so Unity Catalog / the Permissions API
enforce their privileges (no app-level admin gate is needed — or wanted: object
owners who aren't workspace admins can manage their own objects). In a no-OBO
deployment the app SP acts instead, and writes are admin-gated.
"""
from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from backend.services import grants_service
from backend.utils.auth import (
    UserInfo, _is_sp_fallback, _sp_fallback_unrestricted, get_current_user, require_user,
)

router = APIRouter(prefix="/grants", tags=["grants"], dependencies=[Depends(get_current_user)])


class GrantChange(BaseModel):
    kind: str
    name: str
    principal: str
    add: List[str] = []
    remove: List[str] = []


def _caller_token(user: UserInfo) -> str:
    """The caller's OBO token, or "" to act as the app SP. The SP only acts in a
    no-OBO deployment, where it is the single, unrestricted identity (the same
    rule ``require_admin`` applies to mutations)."""
    if not _is_sp_fallback(user) and user.token:
        return user.token
    if _sp_fallback_unrestricted(user):
        return ""
    raise HTTPException(status_code=401, detail="Sign in to manage access")


@router.get("/kinds")
def grant_kinds():
    """Supported object kinds and their suggested privileges."""
    return grants_service.kinds()


@router.get("")
def get_grants(kind: str = Query(...), name: str = Query(...), user: UserInfo = Depends(require_user)):
    """Grants on one object, read as the caller."""
    return grants_service.get_grants(kind, name, user_token=_caller_token(user))


@router.post("")
def update_grants(body: GrantChange, user: UserInfo = Depends(require_user)):
    """Grant and/or revoke privileges for one principal, as the caller."""
    return grants_service.update_grants(
        body.kind, body.name, body.principal, body.add, body.remove,
        user_token=_caller_token(user),
    )
