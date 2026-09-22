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

import base64
import hashlib
import json
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

# ── Real account-admin lookup (decoupled from local workspace admin) ─────
#
# Workspace SCIM /Me often omits account-level roles entirely, so a
# workspace admin and an account admin can look identical from that one
# call. We used to "solve" this by assuming every workspace admin was also
# an account admin (a stub that always returned True) — that is exactly the
# bug this replaces: it collapsed "admin of this workspace" into "admin of
# the whole account". Account-admin status is now resolved independently,
# for every user (not only local workspace admins), via the account-level
# SCIM Users search using the app's service principal (already used the
# same way in workspace_registry.py to list account workspaces).

_ACCOUNT_ADMIN_CACHE: Dict[str, tuple[bool, float]] = {}
_ACCOUNT_ADMIN_CACHE_TTL = 600  # 10 minutes


def _lookup_account_admin(username: str) -> bool:
    """Return True iff ``username`` is a real Databricks *account* admin.

    Resolved via the account SCIM Users API with the app service principal's
    token — never inferred from local workspace-admin status. Fails closed
    (False) on any error: an account-admin check that can't be verified must
    not silently grant "see everything".
    """
    if not username:
        return False

    key = username.lower()
    cached = _ACCOUNT_ADMIN_CACHE.get(key)
    if cached and (time.time() - cached[1]) < _ACCOUNT_ADMIN_CACHE_TTL:
        return cached[0]

    result = False
    try:
        # Lazy import — avoids a hard dependency / import cycle at module load.
        from backend.services.workspace_registry import _get_account_id
        from backend.config import get_databricks_headers, get_databricks_account_host

        account_id = _get_account_id()
        if not account_id:
            logger.info("Account-admin lookup skipped: DATABRICKS_ACCOUNT_ID not resolvable")
            return False

        sp_headers = get_databricks_headers()
        url = f"{get_databricks_account_host()}/api/2.0/accounts/{account_id}/scim/v2/Users"
        resp = httpx.get(
            url,
            headers=sp_headers,
            params={"filter": f'userName eq "{username}"'},
            timeout=10,
        )
        if resp.status_code == 200:
            resources = resp.json().get("Resources", [])
            if resources:
                u = resources[0]
                groups = [g.get("display", "").lower() for g in u.get("groups", [])]
                entitlements = [e.get("value", "") for e in u.get("entitlements", [])]
                roles = [r.get("value", "") for r in u.get("roles", [])]
                result = (
                    "account admins" in groups
                    or "account_admin" in entitlements
                    or "account_admin" in roles
                )
        else:
            logger.info("Account-admin lookup: account SCIM returned HTTP %s for %s", resp.status_code, username)
    except Exception as exc:
        logger.info("Account-admin lookup failed for %s (treating as non-admin): %s", username, exc)
        result = False

    _ACCOUNT_ADMIN_CACHE[key] = (result, time.time())
    return result


def _decode_jwt_payload(token: str) -> Dict[str, object]:
    """Best-effort decode of a JWT's claims. No signature verification —
    the Apps proxy already validated the token before forwarding it."""
    try:
        parts = token.split(".")
        if len(parts) != 3:
            return {}
        payload = parts[1] + "=" * (-len(parts[1]) % 4)
        return json.loads(base64.urlsafe_b64decode(payload))
    except Exception:
        return {}


def _resolve_user(token: str) -> UserInfo:
    """Call SCIM /Me to get the user behind the OBO token.

    If SCIM /Me returns 403 — i.e. the caller has CAN_USE on the app but
    isn't a provisioned member of this workspace — fall back to JWT claims
    so they can still read data (everything in the app is served by the
    SP-backed Lakebase pool, not by the user's token). Admin writes
    remain blocked because ``is_admin`` stays False.
    """
    host = get_databricks_host()
    if not host:
        raise HTTPException(status_code=503, detail="Databricks host not configured")

    headers = {"Authorization": f"Bearer {token}"}

    me_resp = httpx.get(f"{host}/api/2.0/preview/scim/v2/Me", headers=headers, timeout=10)
    if me_resp.status_code == 401:
        raise HTTPException(status_code=401, detail="Invalid or expired token")
    if me_resp.status_code == 403:
        claims = _decode_jwt_payload(token)
        username = (
            claims.get("email")
            or claims.get("preferred_username")
            or claims.get("sub")
            or "unknown-user"
        )
        display_name = claims.get("name") or username
        user_id = str(claims.get("sub", ""))
        logger.info(
            "OBO: SCIM 403 for %s — falling back to JWT claims (non-workspace member, read-only)",
            username,
        )
        # Not a member of this workspace, so is_admin (workspace-local) is
        # always False here — but account-admin status is account-wide and
        # does not depend on workspace membership, so it is still resolved.
        is_account_admin = _lookup_account_admin(str(username))
        user = UserInfo(
            username=str(username),
            display_name=str(display_name),
            user_id=user_id,
            is_admin=False,
            is_account_admin=is_account_admin,
            groups=[],
            token=token,
        )
        _put_cache(token, user)
        return user
    me_resp.raise_for_status()

    me = me_resp.json()
    username = me.get("userName", "")
    display_name = me.get("displayName", username)
    user_id = me.get("id", "")

    # 2. Extract group memberships
    groups = [g.get("display", "") for g in me.get("groups", [])]
    is_admin = "admins" in groups or "workspace-admins" in groups

    # 3. Check for account admin via entitlements/roles/groups on this
    # workspace's SCIM record first (cheap, no extra call when present).
    entitlements = [e.get("value", "") for e in me.get("entitlements", [])]
    roles = [r.get("value", "") for r in me.get("roles", [])]
    is_account_admin = (
        "account_admin" in entitlements
        or "account_admin" in roles
        or "account admins" in [g.lower() for g in groups]
    )
    # Workspace-level SCIM often omits account-admin signals entirely, and —
    # crucially — this must be resolved for EVERY user, not only local
    # workspace admins: account-admin status is independent of whether this
    # person happens to also be an admin of the workspace the app lives in.
    if not is_account_admin:
        is_account_admin = _lookup_account_admin(username)
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
