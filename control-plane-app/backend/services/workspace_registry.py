"""Workspace Registry — maps workspace_id → host URL for cross-workspace operations.

Populated via the Databricks Account API (GET /api/2.0/accounts/{account_id}/workspaces)
during discovery refresh, and cached in Lakebase for fast lookup.

Cross-workspace permission management uses the OBO token + resolved host URL
to create a WorkspaceClient targeting the remote workspace.
"""
from __future__ import annotations

import threading
import time
from typing import Any, Dict, List, Optional

import httpx
from databricks.sdk import WorkspaceClient

from backend.config import get_databricks_host, get_databricks_headers, _get_workspace_client
from backend.database import execute_query, execute_update, execute_one

import logging

logger = logging.getLogger(__name__)

# ── In-memory cache ──────────────────────────────────────────────
_registry_cache: Dict[str, str] = {}  # workspace_id → host_url
_cache_ts: float = 0.0
_CACHE_TTL = 600  # 10 minutes
_cache_lock = threading.Lock()


# ── Lakebase DDL ─────────────────────────────────────────────────

def ensure_workspace_registry_table():
    """Create the workspace_registry table and seed from WORKSPACE_HOSTS env var."""
    ddl = """
    CREATE TABLE IF NOT EXISTS workspace_registry (
        workspace_id   TEXT PRIMARY KEY,
        workspace_host TEXT NOT NULL,
        workspace_name TEXT DEFAULT '',
        deployment_name TEXT DEFAULT '',
        last_updated   TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    )
    """
    try:
        execute_update(ddl)
        logger.info("workspace_registry table ensured")
    except Exception as exc:
        logger.warning("workspace_registry DDL warning: %s", exc)

    # Seed from WORKSPACE_HOSTS env var (set at deploy time via Accounts API)
    _seed_from_env()


def _seed_from_env():
    """Seed workspace registry from the WORKSPACE_HOSTS env var.

    Format: ``ws_id1=https://host1,ws_id2=https://host2,...``
    Set automatically by deploy-sandbox.sh from the Accounts API.

    Loads directly into in-memory cache (fast, no Lakebase round-trips).
    Lakebase seeding is done lazily in the background to avoid startup delays.
    """
    import os
    hosts_str = os.environ.get("WORKSPACE_HOSTS", "")
    if not hosts_str:
        logger.info("   WORKSPACE_HOSTS env var not set — workspace registry will be empty")
        return
    count = 0
    with _cache_lock:
        for pair in hosts_str.split(","):
            pair = pair.strip()
            if "=" not in pair:
                continue
            ws_id, host = pair.split("=", 1)
            ws_id = ws_id.strip()
            host = host.strip()
            if ws_id and host:
                _registry_cache[ws_id] = host
                count += 1
    if count:
        global _cache_ts
        _cache_ts = time.time()
        logger.info("Workspace registry seeded from env: %s workspaces (in-memory)", count)
    else:
        logger.warning("WORKSPACE_HOSTS env var present but parsed 0 entries (len=%s)", len(hosts_str))


# ── Resolve workspace_id → host URL ─────────────────────────────

def get_workspace_host(workspace_id: str) -> Optional[str]:
    """Return the host URL for a workspace, or None if unknown.

    Checks in-memory cache (seeded from WORKSPACE_HOSTS env) → Lakebase → returns None.
    """
    if not workspace_id:
        return None

    ws_str = str(workspace_id).strip()

    # In-memory cache first (fast path — seeded from WORKSPACE_HOSTS env var)
    with _cache_lock:
        if ws_str in _registry_cache:
            return _registry_cache[ws_str]

    # Lakebase lookup
    try:
        row = execute_one(
            "SELECT workspace_host FROM workspace_registry WHERE workspace_id = %s",
            (ws_str,),
        )
        if row and row.get("workspace_host"):
            host = row["workspace_host"]
            with _cache_lock:
                _registry_cache[ws_str] = host
            return host
    except Exception:
        pass

    logger.warning("Workspace %s not found in registry (cache has %s entries, sample keys: %s)", ws_str, len(_registry_cache), list(_registry_cache.keys())[:5])
    return None


def get_all_workspace_hosts() -> Dict[str, str]:
    """Return all workspace_id → host mappings (in-memory cache + Lakebase)."""
    result: Dict[str, str] = {}
    # Start with Lakebase
    try:
        rows = execute_query("SELECT workspace_id, workspace_host FROM workspace_registry")
        result = {r["workspace_id"]: r["workspace_host"] for r in rows}
    except Exception:
        pass
    # Overlay in-memory cache (includes WORKSPACE_HOSTS env entries)
    with _cache_lock:
        result.update(_registry_cache)
    return result


# ── Populate registry via Account API ────────────────────────────

def _get_account_id() -> Optional[str]:
    """Resolve the Databricks account ID.

    Checks (in order):
    1. DATABRICKS_ACCOUNT_ID env var (set in app.yaml)
    2. WorkspaceClient config
    3. Workspace config API
    """
    import os
    env_id = os.environ.get("DATABRICKS_ACCOUNT_ID")
    if env_id:
        return env_id

    w = _get_workspace_client()
    if not w:
        return None
    try:
        aid = getattr(w.config, "account_id", None)
        if aid:
            return aid
    except Exception:
        pass

    try:
        host = get_databricks_host()
        headers = get_databricks_headers()
        resp = httpx.get(f"{host}/api/2.0/workspace-conf?keys=accountId", headers=headers, timeout=10)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("accountId")
    except Exception:
        pass

    return None


def refresh_workspace_registry(user_token: Optional[str] = None) -> int:
    """Populate the workspace registry.

    Tries strategies in order until one succeeds:
    1. Accounts API with OBO token (direct)
    2. Accounts API via workspace proxy
    3. Accounts API with SP token

    Returns the number of workspaces registered.
    """
    account_id = _get_account_id()
    if not account_id:
        logger.warning("Cannot refresh workspace registry: DATABRICKS_ACCOUNT_ID not set")
        return 0

    account_url = f"https://accounts.cloud.databricks.com/api/2.0/accounts/{account_id}/workspaces"
    workspaces = None

    # Strategy 1: OBO token → Accounts API (direct)
    if user_token:
        workspaces = _try_list_workspaces(account_url, user_token, "OBO direct")

    # Strategy 2: OBO token → workspace proxy for accounts API
    if workspaces is None and user_token:
        host = get_databricks_host()
        if host:
            proxy_url = f"{host}/api/2.0/account/workspaces"
            workspaces = _try_list_workspaces(proxy_url, user_token, "OBO proxy")

    # Strategy 3: SP token → Accounts API
    if workspaces is None:
        sp_headers = get_databricks_headers()
        sp_token = sp_headers.get("Authorization", "").replace("Bearer ", "")
        if sp_token:
            workspaces = _try_list_workspaces(account_url, sp_token, "SP")

    if workspaces is None:
        logger.warning("Workspace registry: all strategies failed")
        return 0

    count = 0
    for ws in workspaces:
        ws_id = str(ws.get("workspace_id", ""))
        deployment_name = ws.get("deployment_name", "")
        ws_name = ws.get("workspace_name", "")
        host = f"https://{deployment_name}.cloud.databricks.com" if deployment_name else ws.get("workspace_url", "")
        if ws_id and host:
            _upsert_workspace(ws_id, host, ws_name, deployment_name)
            count += 1

    global _cache_ts
    _cache_ts = time.time()
    logger.info("Workspace registry refreshed: %s workspaces", count)
    return count


def _try_list_workspaces(url: str, token: str, label: str) -> Optional[list]:
    """Try listing workspaces from a URL. Returns list on success, None on failure."""
    try:
        resp = httpx.get(url, headers={"Authorization": f"Bearer {token}"}, timeout=30)
        if resp.status_code == 200:
            data = resp.json()
            if isinstance(data, list) and len(data) > 0:
                logger.info("   Workspace list via %s: %s workspaces", label, len(data))
                return data
        logger.warning("   Workspace list via %s: HTTP %s", label, resp.status_code)
    except Exception as exc:
        logger.warning("   Workspace list via %s: %s", label, exc)
    return None


def _upsert_workspace(ws_id: str, host: str, name: str = "", deployment: str = ""):
    """Insert or update a single workspace in the registry."""
    try:
        execute_update(
            """INSERT INTO workspace_registry (workspace_id, workspace_host, workspace_name, deployment_name, last_updated)
               VALUES (%s, %s, %s, %s, NOW())
               ON CONFLICT (workspace_id) DO UPDATE SET
                   workspace_host = EXCLUDED.workspace_host,
                   workspace_name = EXCLUDED.workspace_name,
                   deployment_name = EXCLUDED.deployment_name,
                   last_updated = NOW()""",
            (ws_id, host, name, deployment),
        )
        with _cache_lock:
            _registry_cache[ws_id] = host
    except Exception as exc:
        logger.warning("Failed to upsert workspace %s: %s", ws_id, exc)


# ── Cross-workspace WorkspaceClient factory ──────────────────────

def get_remote_workspace_client(
    workspace_id: str,
    user_token: str,
) -> Optional[WorkspaceClient]:
    """Create a WorkspaceClient targeting a remote workspace using the OBO token.

    The OBO token from an account admin can authenticate against any workspace
    in the account.  The host URL is resolved from the workspace registry.

    Returns None if the workspace host is unknown or client creation fails.
    """
    host = get_workspace_host(workspace_id)
    if not host:
        logger.warning("No host URL for workspace %s — run workspace registry refresh", workspace_id)
        return None

    import os
    mask_keys = ["DATABRICKS_HOST", "DATABRICKS_ACCOUNT_ID", "DATABRICKS_WORKSPACE_ID"]
    saved = {}
    try:
        for k in mask_keys:
            if k in os.environ:
                saved[k] = os.environ.pop(k)
        client = WorkspaceClient(host=host)  # SP OAuth creds from env
        return client
    except Exception as exc:
        logger.warning("Failed to create WorkspaceClient for %s (%s): %s", workspace_id, host, exc)
        return None
    finally:
        os.environ.update(saved)
