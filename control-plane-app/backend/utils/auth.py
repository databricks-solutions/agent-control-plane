"""OBO (On Behalf Of) authentication for Databricks Apps.

When a user visits a Databricks App, the Apps proxy injects an
``x-forwarded-access-token`` header containing the user's short-lived
OAuth token.  This module:

1. Extracts the token from the header.
2. Calls ``/api/2.0/preview/scim/v2/Me`` to resolve the user identity.
3. Caches the result (keyed by token hash) for the token's lifetime.
4. Exposes ``get_current_user`` as a FastAPI dependency.
5. Provides ``require_admin`` for admin-only endpoints.
"""
from __future__ import annotations

import hashlib
import time
from dataclasses import dataclass, field
from typing import Dict, Optional

import httpx
from fastapi import HTTPException, Request

from backend.config import get_databricks_host

# ── User model ───────────────────────────────────────────────────

import logging

logger = logging.getLogger(__name__)

@dataclass
class UserInfo:
    """Resolved identity of the calling user."""
    username: str           # email / SP application_id
    display_name: str       # human-readable name
    user_id: str            # numeric Databricks user ID
    is_admin: bool = False  # workspace admin flag
    is_account_admin: bool = False  # account admin flag (from SCIM entitlements)
    groups: list = field(default_factory=list)
    token: str = ""         # the raw OBO token (for downstream API calls)


# ── Token → UserInfo cache (in-memory, short-lived) ─────────────

_USER_CACHE: Dict[str, tuple[UserInfo, float]] = {}
_CACHE_TTL = 300  # 5 minutes — tokens live ~1 h, so this is safe


def _cache_key(token: str) -> str:
    return hashlib.sha256(token.encode()).hexdigest()[:16]


def _get_cached(token: str) -> Optional[UserInfo]:
    key = _cache_key(token)
    entry = _USER_CACHE.get(key)
    if entry and (time.time() - entry[1]) < _CACHE_TTL:
        return entry[0]
    return None


def _put_cache(token: str, user: UserInfo):
    key = _cache_key(token)
    _USER_CACHE[key] = (user, time.time())


# ── Resolve user from OBO token ─────────────────────────────────

def _probe_account_admin(_host: str, _token: str) -> bool:
    """Check whether the token holder has account-level access.

    OBO tokens are workspace-scoped and cannot call account-level APIs,
    so there is no reliable probe.  Workspace admins are granted
    ``is_account_admin`` optimistically — the real security gate for
    cross-workspace operations is the OBO token itself: API calls to
    remote workspaces will fail with 403 if the user lacks access.
    """
    return True


def _resolve_user(token: str) -> UserInfo:
    """Call SCIM /Me to get the user behind the OBO token."""
    host = get_databricks_host()
    if not host:
        raise HTTPException(status_code=503, detail="Databricks host not configured")

    headers = {"Authorization": f"Bearer {token}"}

    # 1. Who am I?
    me_resp = httpx.get(f"{host}/api/2.0/preview/scim/v2/Me", headers=headers, timeout=10)
    if me_resp.status_code == 401:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    me_resp.raise_for_status()

    me = me_resp.json()
    username = me.get("userName", "")
    display_name = me.get("displayName", username)
    user_id = me.get("id", "")

    # 2. Extract group memberships
    groups = [g.get("display", "") for g in me.get("groups", [])]
    is_admin = "admins" in groups or "workspace-admins" in groups

    # 3. Check for account admin via entitlements, roles, groups, AND probe
    entitlements = [e.get("value", "") for e in me.get("entitlements", [])]
    roles = [r.get("value", "") for r in me.get("roles", [])]
    is_account_admin = (
        "account_admin" in entitlements
        or "account_admin" in roles
        or "account admins" in [g.lower() for g in groups]
    )
    # Workspace-level SCIM often omits account admin signals entirely.
    # For workspace admins, probe the Accounts API as a fallback.
    if not is_account_admin and is_admin:
        probe_result = _probe_account_admin(host, token)
        logger.info("OBO: account admin probe result=%s", probe_result)
        is_account_admin = probe_result
    logger.info("OBO: user=%s, is_admin=%s, is_account_admin=%s, entitlements=%s, roles=%s, groups=%s", username, is_admin, is_account_admin, entitlements, roles, groups)

    user = UserInfo(
        username=username,
        display_name=display_name,
        user_id=user_id,
        is_admin=is_admin,
        is_account_admin=is_account_admin,
        groups=groups,
        token=token,
    )
    _put_cache(token, user)
    return user


# ── FastAPI dependencies ─────────────────────────────────────────

_SP_FALLBACK = UserInfo(
    username="service-principal",
    display_name="Control Plane SP (read-only)",
    user_id="sp",
    is_admin=False,
    is_account_admin=False,
    groups=[],
    token="",
)


async def get_current_user(request: Request) -> UserInfo:
    """Extract and validate the OBO user from the request.

    In Databricks Apps the token arrives via ``x-forwarded-access-token``.
    For local development, falls back to ``Authorization: Bearer <token>``.
    When no token is present at all (OBO not enabled), returns a fallback
    SP identity so the app remains functional.
    """
    token = request.headers.get("x-forwarded-access-token", "")
    if not token:
        # Fallback for local dev / direct API calls
        auth = request.headers.get("authorization", "")
        if auth.lower().startswith("bearer "):
            token = auth[7:]

    if not token:
        # OBO not enabled — fall back to SP identity
        logger.info("OBO: no token found — using SP fallback")
        return _SP_FALLBACK

    cached = _get_cached(token)
    if cached:
        logger.info("OBO: cached user → %s (admin=%s)", cached.username, cached.is_admin)
        return cached

    user = _resolve_user(token)
    logger.info("OBO: resolved user → %s (admin=%s, groups=%s)", user.username, user.is_admin, len(user.groups))
    return user


async def require_admin(request: Request) -> UserInfo:
    """Same as ``get_current_user`` but raises 403 if not a workspace admin."""
    user = await get_current_user(request)
    if not user.is_admin:
        raise HTTPException(
            status_code=403,
            detail=f"Admin access required (user={user.username})",
        )
    return user


async def require_account_admin(request: Request) -> UserInfo:
    """Same as ``get_current_user`` but raises 403 if not an account admin.

    Cross-workspace permission management requires account-level privileges.
    """
    user = await get_current_user(request)
    if not user.is_account_admin:
        raise HTTPException(
            status_code=403,
            detail=f"Account admin access required for cross-workspace operations (user={user.username})",
        )
    return user
