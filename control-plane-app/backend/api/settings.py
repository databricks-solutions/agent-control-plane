"""API routes for app-level runtime settings (admin-editable)."""
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from backend.utils.auth import require_user, UserInfo
from backend.services import settings_service

router = APIRouter(prefix="/settings", tags=["settings"], dependencies=[Depends(require_user)])


class AccessModeUpdate(BaseModel):
    access_mode: str


def _can_edit(user: UserInfo) -> bool:
    """Workspace admins (of the deploy workspace) or account admins may edit."""
    return bool(user.is_admin or user.is_account_admin)


@router.get("")
def get_settings(user: UserInfo = Depends(require_user)):
    """Return current settings + whether this caller may edit them.

    ``can_edit`` lets the frontend show the toggle as read-only for non-admins;
    the real enforcement is on ``PUT`` below.
    """
    return {
        "access_mode": settings_service.get_access_mode(),
        "valid_access_modes": list(settings_service.VALID_ACCESS_MODES),
        "can_edit": _can_edit(user),
    }


@router.put("")
def update_settings(body: AccessModeUpdate, user: UserInfo = Depends(require_user)):
    """Change the access mode. Only workspace or account admins may do so."""
    if not _can_edit(user):
        raise HTTPException(
            status_code=403,
            detail="Workspace or account admin access required to change settings",
        )
    try:
        mode = settings_service.set_access_mode(body.access_mode, updated_by=user.username)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return {"access_mode": mode, "can_edit": True}
