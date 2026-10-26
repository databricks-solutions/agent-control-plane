"""Genie Ask Genie overlay — backend.

Two responsibilities:

1. ``/genie/space-info`` — returns the configured space metadata. Frontend
   uses ``space_url`` for the "open in new tab" affordance only; the chat
   itself is built inline.

2. ``/genie/conversations*`` — thin OBO proxy onto the Databricks Genie
   Conversations API so the frontend doesn't have to handle cross-origin
   auth. We forward the inbound ``x-forwarded-access-token`` header (the
   user's OBO bearer) verbatim. Genie sees the user's identity for UC
   permission checks; the App SP is not used.

Feature-flagged on ``FEATURE_GENIE_ENABLED``; all routes 404 when off.
"""
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from backend.config import get_databricks_host, settings
from backend.database import execute_query
from backend.utils.auth import get_current_user, require_user, UserInfo

logger = logging.getLogger(__name__)


def _resolved_space_id() -> str:
    """Genie space id, resolved with precedence:
      1. GENIE_SPACE_ID env (explicit pin — wins), then
      2. the workflow-provisioned id in billing_cache_meta (setup_genie_space task).
    So a deployment auto-picks up the space the discovery workflow creates, with no
    redeploy, while an explicit env pin still overrides. Fail-open → "" (tab hidden)."""
    sid = (settings.genie_space_id or "").strip()
    if sid:
        return sid
    try:
        rows = execute_query(
            "SELECT value_text FROM billing_cache_meta WHERE cache_key = 'genie_space_id'")
        if rows and rows[0].get("value_text"):
            return str(rows[0]["value_text"]).strip()
    except Exception:
        pass
    return ""

router = APIRouter(prefix="/genie", tags=["genie"], dependencies=[Depends(get_current_user)])

_TIMEOUT = 30.0  # Genie's first response is async (just SUBMITTED) — fast.


def _require_genie_enabled() -> None:
    if not settings.feature_genie_enabled:
        raise HTTPException(status_code=404, detail="Not found")


def _space_id_or_503() -> str:
    sid = _resolved_space_id()
    if not sid:
        raise HTTPException(status_code=503, detail="Genie space not configured (GENIE_SPACE_ID unset)")
    return sid


def _obo_token_or_401(request: Request) -> str:
    token = request.headers.get("x-forwarded-access-token", "")
    if not token:
        # Local dev fallback: accept Authorization header so we can curl.
        auth = request.headers.get("authorization", "")
        if auth.lower().startswith("bearer "):
            token = auth[7:]
    if not token:
        raise HTTPException(status_code=401, detail="OBO token required")
    return token


def _proxy(
    method: str,
    path: str,
    token: str,
    body: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """Forward a request to the Databricks Genie REST API with the user's OBO token."""
    host = get_databricks_host()
    if not host:
        raise HTTPException(status_code=503, detail="Databricks host not configured")
    url = f"{host.rstrip('/')}{path}"
    headers = {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    try:
        resp = httpx.request(method, url, headers=headers, json=body, timeout=_TIMEOUT)
    except httpx.HTTPError as exc:
        logger.warning("Genie proxy failed: %s %s -> %s", method, path, exc)
        raise HTTPException(status_code=502, detail=f"Genie upstream error: {exc}")
    if resp.status_code >= 400:
        # Propagate the upstream status (returning the body alone would ship it as
        # HTTP 200, and the client would then poll undefined ids until it timed out).
        # Keep Databricks's message so the frontend can show it.
        try:
            body = resp.json()
            detail = body.get("message") or body.get("error_code") or str(body)[:500]
        except Exception:
            detail = resp.text[:500]
        raise HTTPException(status_code=resp.status_code, detail=f"Genie upstream {resp.status_code}: {detail}")
    return resp.json()


# ── Public routes ────────────────────────────────────────────────

@router.get("/space-info")
def get_space_info() -> dict:
    """Space metadata for the frontend (used for "open in new tab" link)."""
    _require_genie_enabled()
    space_id = _resolved_space_id()
    if not space_id:
        return {"available": False, "space_id": None, "space_url": None}
    host = get_databricks_host()
    if not host:
        return {"available": False, "space_id": space_id, "space_url": None}
    return {
        "available": True,
        "space_id": space_id,
        # UI route is /genie/rooms/{id}; the REST API uses /genie/spaces/{id} —
        # different paths for the same object due to legacy naming overlap.
        "space_url": f"{host.rstrip('/')}/genie/rooms/{space_id}",
    }


class StartConversationBody(BaseModel):
    content: str


@router.post("/conversations")
def start_conversation(body: StartConversationBody, request: Request, user: UserInfo = Depends(require_user)) -> dict:
    """Start a new Genie conversation with the user's question.

    Returns immediately with ``conversation_id`` and ``message_id`` —
    the actual answer is async. Frontend polls ``/messages/{mid}`` until
    status is COMPLETED.
    """
    _require_genie_enabled()
    space_id = _space_id_or_503()
    token = _obo_token_or_401(request)
    return _proxy(
        "POST",
        f"/api/2.0/genie/spaces/{space_id}/start-conversation",
        token,
        {"content": body.content},
    )


@router.get("/conversations/{conversation_id}/messages/{message_id}")
def get_message(conversation_id: str, message_id: str, request: Request) -> dict:
    """Poll a Genie message for status + attachments."""
    _require_genie_enabled()
    space_id = _space_id_or_503()
    token = _obo_token_or_401(request)
    return _proxy(
        "GET",
        f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}",
        token,
    )


class FollowUpBody(BaseModel):
    content: str


@router.post("/conversations/{conversation_id}/messages")
def post_message(conversation_id: str, body: FollowUpBody, request: Request, user: UserInfo = Depends(require_user)) -> dict:
    """Send a follow-up message in an existing conversation."""
    _require_genie_enabled()
    space_id = _space_id_or_503()
    token = _obo_token_or_401(request)
    return _proxy(
        "POST",
        f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages",
        token,
        {"content": body.content},
    )


@router.get("/conversations/{conversation_id}/messages/{message_id}/query-result")
def get_query_result(conversation_id: str, message_id: str, request: Request) -> dict:
    """Fetch the materialized query result rows for a completed message.

    Returns the standard Databricks ``statement_response`` shape so the
    frontend can render columns + data_array as a table.
    """
    _require_genie_enabled()
    space_id = _space_id_or_503()
    token = _obo_token_or_401(request)
    return _proxy(
        "GET",
        f"/api/2.0/genie/spaces/{space_id}/conversations/{conversation_id}/messages/{message_id}/query-result",
        token,
    )
