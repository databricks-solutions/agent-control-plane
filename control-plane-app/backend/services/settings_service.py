"""App-level runtime settings, admin-editable and persisted in Lakebase.

Currently holds ``access_mode``, which controls how much a non-admin can see:

  • ``open``   (default) — any authenticated user sees all workspaces read-only.
                 Broad visibility for teams that want a shared dashboard.
  • ``strict`` — per-user workspace scoping (see backend.utils.access_scope):
                 account admins see everything, workspace admins see the
                 workspaces they administer, everyone else sees nothing.

This is a *read-visibility* switch only. It never affects write authorization —
every mutation stays gated by require_admin / require_account_admin regardless
of the mode — nor the cross-workspace SP-credential paths.

Distinct from the deploy-time ``OBO_ENABLED`` env flag: this is a runtime
setting a workspace/account admin flips from the Admin page.
"""
from __future__ import annotations

import threading
import time
import logging

from backend.database import execute_query, execute_update

logger = logging.getLogger(__name__)

VALID_ACCESS_MODES = ("open", "strict")
DEFAULT_ACCESS_MODE = "open"

_cache: dict = {}
_cache_ts: float = 0.0
_CACHE_TTL = 30  # seconds — access_mode is read on every request via access_scope
_lock = threading.Lock()


def ensure_settings_table() -> None:
    """Create the app_settings key/value table if it doesn't exist."""
    execute_update(
        """CREATE TABLE IF NOT EXISTS app_settings (
               key         TEXT PRIMARY KEY,
               value       TEXT NOT NULL,
               updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
               updated_by  TEXT
           )"""
    )


def _load() -> dict:
    global _cache, _cache_ts
    with _lock:
        if _cache and (time.time() - _cache_ts) < _CACHE_TTL:
            return dict(_cache)
    try:
        rows = execute_query("SELECT key, value FROM app_settings")
        data = {r["key"]: r["value"] for r in rows}
    except Exception as exc:
        # Table missing / DB unavailable → fall back to defaults (which is
        # 'open' for access_mode, the configured out-of-the-box behaviour).
        logger.warning("app_settings load failed (using defaults): %s", exc)
        data = {}
    with _lock:
        _cache = data
        _cache_ts = time.time()
    return data


def get_access_mode() -> str:
    """Return the current access mode ('open' or 'strict'; default 'open')."""
    mode = _load().get("access_mode", DEFAULT_ACCESS_MODE)
    return mode if mode in VALID_ACCESS_MODES else DEFAULT_ACCESS_MODE


def set_access_mode(mode: str, updated_by: str = "") -> str:
    """Persist a new access mode. Raises ValueError on an invalid value."""
    if mode not in VALID_ACCESS_MODES:
        raise ValueError(f"invalid access_mode {mode!r} (expected one of {VALID_ACCESS_MODES})")
    execute_update(
        """INSERT INTO app_settings (key, value, updated_at, updated_by)
           VALUES ('access_mode', %s, NOW(), %s)
           ON CONFLICT (key) DO UPDATE SET
               value = EXCLUDED.value, updated_at = NOW(), updated_by = EXCLUDED.updated_by""",
        (mode, updated_by),
    )
    global _cache_ts
    with _lock:
        _cache_ts = 0.0  # invalidate so the next read reflects the change
    logger.info("access_mode set to %s by %s", mode, updated_by or "?")
    return mode
