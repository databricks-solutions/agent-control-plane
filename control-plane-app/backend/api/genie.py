"""Genie Ask tab — exposes the curated Genie space URL to the frontend.

The frontend iframes the URL returned by ``/api/v1/genie/space-info``.
The actual Genie chat UI is hosted by Databricks; ACP just provides the
deep-link to a workspace-curated space (created by ``setup_genie_space.py``).

Feature-flagged on ``FEATURE_GENIE_ENABLED``; routes 404 when off so the
frontend doesn't need to special-case missing config.
"""
from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException

from backend.config import get_databricks_host, settings
from backend.utils.auth import get_current_user

router = APIRouter(prefix="/genie", tags=["genie"], dependencies=[Depends(get_current_user)])


def _require_genie_enabled() -> None:
    if not settings.feature_genie_enabled:
        raise HTTPException(status_code=404, detail="Not found")


@router.get("/space-info")
def get_space_info() -> dict:
    """Return the configured Genie space's id + workspace URL.

    Frontend iframes ``space_url`` directly. If the env var
    ``GENIE_SPACE_ID`` isn't set the route returns ``available=false``
    so the frontend can render a "not configured" state.
    """
    _require_genie_enabled()

    space_id = (settings.genie_space_id or "").strip()
    if not space_id:
        return {"available": False, "space_id": None, "space_url": None}

    host = get_databricks_host()
    if not host:
        return {"available": False, "space_id": space_id, "space_url": None}

    return {
        "available": True,
        "space_id": space_id,
        "space_url": f"{host.rstrip('/')}/genie/spaces/{space_id}",
    }
