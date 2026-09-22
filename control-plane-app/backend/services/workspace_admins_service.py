"""Workspace Admins — maps user_name → workspaces they are an ADMIN of.

Populated via the Databricks Account API
(``GET /api/2.0/accounts/{account_id}/workspaces/{workspace_id}/permissionassignments``)
for every workspace already known to ``workspace_registry``, and cached in
Lakebase for fast read-time lookup.

This is the access-control source of truth for non-account-admins: anyone
who is not a real account admin (see ``backend.utils.auth._lookup_account_admin``)
is scoped to exactly the workspaces where a row exists here for them. No row
→ no access, not "fall back to everything".

Deliberately scoped to *admins only* (not "everyone who can log into a
workspace") — see ``get_allowed_workspace_ids`` in ``backend.utils.access_scope``
for how this is combined with the account-admin check.
"""
from __future__ import annotations

import threading
import time
from typing import Dict, List, Optional, Set

import httpx

from backend.config import get_databricks_headers, get_databricks_account_host
from backend.database import DatabasePool, execute_query

import logging

logger = logging.getLogger(__name__)

# ── In-memory cache ──────────────────────────────────────────────
# user_name (lowercased) → set of workspace_ids they administer.
_admin_cache: Dict[str, Set[str]] = {}
_cache_ts: float = 0.0
_CACHE_TTL = 600  # 10 minutes
_cache_lock = threading.Lock()


# ── Lakebase DDL ─────────────────────────────────────────────────

def ensure_workspace_admins_table():
    """Create the workspace_admins table (empty until the first refresh)."""
    ddl = """
    CREATE TABLE IF NOT EXISTS workspace_admins (
        workspace_id TEXT NOT NULL,
        user_name    TEXT NOT NULL,
        last_updated TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
        PRIMARY KEY (workspace_id, user_name)
    )
    """
    try:
        from backend.database import execute_update
        execute_update(ddl)
        execute_update("CREATE INDEX IF NOT EXISTS idx_wa_user ON workspace_admins (user_name)")
        logger.info("workspace_admins table ensured")
    except Exception as exc:
        logger.warning("workspace_admins DDL warning: %s", exc)
    _load_cache_from_lakebase()


def _load_cache_from_lakebase():
    """Warm the in-memory cache from whatever is already in Lakebase, so a
    restart doesn't leave every non-account-admin with an empty allow-list
    until the next refresh job runs."""
    try:
        rows = execute_query("SELECT workspace_id, user_name FROM workspace_admins")
    except Exception as exc:
        logger.warning("workspace_admins cache warm skipped: %s", exc)
        return
    built: Dict[str, Set[str]] = {}
    for r in rows:
        key = (r.get("user_name") or "").lower()
        if not key:
            continue
        built.setdefault(key, set()).add(str(r["workspace_id"]))
    with _cache_lock:
        _admin_cache.clear()
        _admin_cache.update(built)
    global _cache_ts
    _cache_ts = time.time()
    logger.info("workspace_admins cache warmed: %s users, %s rows", len(built), len(rows))


# ── Read-time lookup ─────────────────────────────────────────────

def get_admin_workspace_ids(user_name: str) -> List[str]:
    """Return the workspace_ids ``user_name`` is an ADMIN of.

    Empty list means "no workspace admin access anywhere" — callers must
    treat that as "show nothing", not "show everything".
    """
    if not user_name:
        return []
    key = user_name.lower()

    # The in-memory map is per-process. A refresh on another gunicorn worker
    # writes Lakebase but does not update this dict — expire and reload so
    # revoked admins don't keep access until this process restarts.
    if time.time() - _cache_ts > _CACHE_TTL:
        _load_cache_from_lakebase()

    with _cache_lock:
        cached = _admin_cache.get(key)
    if cached is not None:
        return sorted(cached)

    # Cache miss (e.g. process just started and warm-load hasn't run) — one
    # direct Lakebase lookup rather than assuming "not an admin".
    try:
        rows = execute_query(
            "SELECT workspace_id FROM workspace_admins WHERE lower(user_name) = %s",
            (key,),
        )
        ids = [str(r["workspace_id"]) for r in rows]
        with _cache_lock:
            _admin_cache[key] = set(ids)
        return sorted(ids)
    except Exception as exc:
        logger.warning("workspace_admins lookup failed for %s (failing closed → []): %s", user_name, exc)
        return []


# ── Populate via Account API ──────────────────────────────────────

def refresh_workspace_admins(user_token: Optional[str] = None) -> int:
    """Refresh the workspace_admins cache from the Account API.

    For every workspace already known to ``workspace_registry``, calls
    ``permissionassignments`` and keeps principals with an ADMIN permission
    that are individual users (group/service-principal admin grants are not
    expanded here — a user must appear directly to be scoped in).

    Writes with TRUNCATE + INSERT in ONE transaction, matching the fix
    applied to the billing sync (workflows/02_sync_to_lakebase.py): a
    partial failure must roll back rather than leave the table (and
    therefore everyone's access) empty until the next run.

    Returns the number of (workspace_id, user_name) rows written.
    """
    from backend.services.workspace_registry import _get_account_id, get_all_workspace_hosts

    account_id = _get_account_id()
    if not account_id:
        logger.warning("Cannot refresh workspace_admins: DATABRICKS_ACCOUNT_ID not set")
        return 0

    ws_ids = list(get_all_workspace_hosts().keys())
    if not ws_ids:
        logger.warning("Cannot refresh workspace_admins: workspace_registry is empty (run its refresh first)")
        return 0

    token = user_token
    if not token:
        sp_headers = get_databricks_headers()
        token = sp_headers.get("Authorization", "").replace("Bearer ", "")
    if not token:
        logger.warning("Cannot refresh workspace_admins: no token available (OBO or SP)")
        return 0

    rows: List[tuple] = []
    for ws_id in ws_ids:
        for user_name in _fetch_workspace_admin_users(account_id, ws_id, token):
            rows.append((ws_id, user_name))

    _write_rows_in_one_transaction(rows)

    built: Dict[str, Set[str]] = {}
    for ws_id, user_name in rows:
        built.setdefault(user_name.lower(), set()).add(ws_id)
    with _cache_lock:
        _admin_cache.clear()
        _admin_cache.update(built)
    global _cache_ts
    _cache_ts = time.time()

    logger.info("workspace_admins refreshed: %s rows across %s workspaces", len(rows), len(ws_ids))
    return len(rows)


def _fetch_workspace_admin_users(account_id: str, workspace_id: str, token: str) -> List[str]:
    """Return usernames with an ADMIN permission on one workspace."""
    url = f"{get_databricks_account_host()}/api/2.0/accounts/{account_id}/workspaces/{workspace_id}/permissionassignments"
    try:
        resp = httpx.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
        if resp.status_code != 200:
            logger.warning("permissionassignments for %s: HTTP %s", workspace_id, resp.status_code)
            return []
        data = resp.json()
    except Exception as exc:
        logger.warning("permissionassignments for %s failed: %s", workspace_id, exc)
        return []

    admins: List[str] = []
    for assignment in data.get("permission_assignments", []):
        perms = assignment.get("permissions", [])
        if "ADMIN" not in perms:
            continue
        principal = assignment.get("principal", {}) or {}
        # Only direct user principals — group/SP admin grants aren't expanded.
        user_name = principal.get("user_name")
        if user_name:
            admins.append(user_name)
    return admins


def _write_rows_in_one_transaction(rows: List[tuple]) -> None:
    """TRUNCATE + INSERT workspace_admins in a single transaction.

    Deliberately does NOT commit the TRUNCATE separately (that was the bug
    fixed in the billing sync): if the INSERT fails, the whole transaction
    rolls back and the table keeps its previous (still-valid) contents
    instead of being left empty.
    """
    with DatabasePool.get_connection() as conn:
        try:
            with conn.cursor() as cur:
                cur.execute("TRUNCATE TABLE workspace_admins")
                if rows:
                    from psycopg2.extras import execute_values
                    execute_values(
                        cur,
                        "INSERT INTO workspace_admins (workspace_id, user_name) VALUES %s",
                        rows,
                        page_size=200,
                    )
            conn.commit()
        except Exception:
            conn.rollback()
            raise
