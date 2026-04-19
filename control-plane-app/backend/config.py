"""Configuration settings for the control plane app."""
import os
from typing import Optional, List
from pydantic_settings import BaseSettings
from pydantic import Field


import logging

logger = logging.getLogger(__name__)

class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Lakebase PostgreSQL connection
    lakebase_dns: str = "localhost"
    lakebase_database: str = "control_plane"
    lakebase_user: str = ""
    lakebase_password: str = Field(default="", alias="LAKEBASE_TOKEN")
    lakebase_port: int = 5432

    # Databricks settings
    databricks_host: Optional[str] = None
    databricks_token: Optional[str] = None

    # API settings
    api_title: str = "AI Control Plane API"
    api_version: str = "1.0.0"
    api_prefix: str = "/api/v1"

    # CORS settings — empty by default (same-origin, no CORS needed in production).
    # For local dev, set CORS_ORIGINS=http://localhost:3000,http://localhost:5173 in .env
    cors_origins: List[str] = []

    model_config = {
        "env_file": ".env",
        "case_sensitive": False,
        "extra": "ignore",
        "populate_by_name": True,
    }


settings = Settings()


# ── Databricks SDK auth helper (works inside Databricks Apps) ────
_workspace_client = None
_ws_init_attempted = False


def _get_workspace_client():
    """
    Lazily initialise a Databricks WorkspaceClient.

    Inside a Databricks App the SDK picks up credentials automatically via
    the app service principal.  Locally it uses ~/.databrickscfg or env vars.
    We do NOT validate auth here – each caller handles failures gracefully.
    """
    global _workspace_client, _ws_init_attempted
    if not _ws_init_attempted:
        _ws_init_attempted = True
        try:
            from databricks.sdk import WorkspaceClient
            _workspace_client = WorkspaceClient()
            host = getattr(_workspace_client.config, "host", "unknown")
            logger.info("Databricks SDK initialised (host=%s)", host)
        except Exception as exc:
            logger.warning("Databricks SDK init failed: %s", exc)
            _workspace_client = None
    return _workspace_client


def _sdk_auth_headers() -> Optional[dict]:
    """
    Get auth headers from the Databricks SDK (handles multiple SDK versions).
    Returns None on any failure so callers can fall back.
    """
    w = _get_workspace_client()
    if w is None:
        return None
    try:
        # Modern SDK (>= 0.20): authenticate(headers_dict) populates in-place
        h: dict = {}
        result = w.config.authenticate(h)
        # Some SDK versions return the dict instead of populating in-place
        if isinstance(result, dict) and result:
            h = result
        if h.get("Authorization"):
            return h
    except TypeError as exc:
        logger.debug("SDK authenticate(dict) not supported: %s", exc)
    except Exception as exc:
        logger.debug("SDK authenticate(dict) failed: %s", exc)
    try:
        # Alternative: authenticate() returns headers dict directly
        result = w.config.authenticate()
        if isinstance(result, dict) and result.get("Authorization"):
            return result
    except Exception as exc:
        logger.debug("SDK authenticate() failed: %s", exc)
    try:
        # Last resort: build the header from the token provider
        token = w.config.token
        if token:
            return {"Authorization": f"Bearer {token}"}
    except Exception as exc:
        logger.debug("SDK token property failed: %s", exc)
    return None


# ── Cached identity for Lakebase connections ─────────────────────
_cached_lakebase_user: Optional[str] = None


def _resolve_lakebase_user() -> str:
    """Determine the Lakebase PostgreSQL user for the current identity."""
    global _cached_lakebase_user
    if _cached_lakebase_user:
        return _cached_lakebase_user
    w = _get_workspace_client()
    if w:
        try:
            me = w.current_user.me()
            # For users: user_name is the email
            # For service principals: user_name is the application_id (UUID)
            identity = me.user_name
            if identity:
                _cached_lakebase_user = identity
                logger.info("Resolved Lakebase user via SDK: %s", identity)
                return identity
        except Exception as exc:
            logger.warning("Could not resolve user via SDK: %s", exc)
    fallback = settings.lakebase_user or os.environ.get("USER", "")
    _cached_lakebase_user = fallback
    return fallback


def get_databricks_host() -> str:
    """Return the workspace host URL (always includes https:// scheme)."""
    raw = ""
    if settings.databricks_host:
        raw = settings.databricks_host.rstrip("/")
    else:
        w = _get_workspace_client()
        if w:
            raw = (w.config.host or "").rstrip("/")
    if raw and not raw.startswith(("https://", "http://")):
        raw = f"https://{raw}"
    return raw


def get_databricks_headers() -> dict:
    """
    Return auth headers for Databricks REST API calls.

    When running inside a Databricks App the SDK authenticates automatically
    via the app's service principal – no static token needed.
    When running locally, falls back to the configured personal access token.
    """
    sdk_headers = _sdk_auth_headers()
    if sdk_headers:
        sdk_headers["Content-Type"] = "application/json"
        return sdk_headers
    # Fallback: static token from env
    return {
        "Authorization": f"Bearer {settings.databricks_token or ''}",
        "Content-Type": "application/json",
    }


def get_lakebase_password() -> str:
    """
    Return a valid Lakebase password.

    Supports both Lakebase Autoscaling (projects/branches/endpoints) and
    Provisioned (instance_names) credential generation.
    Falls back to the static LAKEBASE_TOKEN env var.
    """
    w = _get_workspace_client()
    if w:
        # Try Autoscaling Lakebase first (POST /api/2.0/postgres/credentials)
        endpoint_path = os.environ.get("LAKEBASE_ENDPOINT_PATH", "")
        if endpoint_path:
            try:
                import httpx
                auth_headers = _sdk_auth_headers() or {}
                host = get_databricks_host()
                resp = httpx.post(
                    f"{host}/api/2.0/postgres/credentials",
                    headers={**auth_headers, "Content-Type": "application/json"},
                    json={"endpoint": endpoint_path},
                    timeout=10,
                )
                resp.raise_for_status()
                token = resp.json().get("token", "")
                if token:
                    return token
            except Exception as exc:
                logger.warning("Lakebase Autoscaling credential generation failed: %s", exc)

        # Try Provisioned Lakebase (w.database.generate_database_credential)
        try:
            creds = w.database.generate_database_credential(
                instance_names=[os.environ.get("LAKEBASE_INSTANCE", "ai-control-plane-db")]
            )
            return creds.token
        except Exception as exc:
            logger.warning("Lakebase Provisioned credential generation failed: %s", exc)
    return settings.lakebase_password


def get_lakebase_user() -> str:
    """
    Return the correct Lakebase user identity.

    The user must match the identity that generated the token.
    """
    return _resolve_lakebase_user()


# ── Serverless SQL warehouse helper ───────────────────────────────
import time as _time

_warehouse_id_cache: Optional[str] = None
_warehouse_id_ts: float = 0.0
_WAREHOUSE_CACHE_TTL = 300  # seconds


def find_serverless_warehouse_id(force_refresh: bool = False) -> Optional[str]:
    """Find the best SQL warehouse, strongly preferring serverless.

    Selection priority:
      1. Running serverless warehouse
      2. Any (non-running) serverless warehouse (will auto-start instantly)
      3. Running classic/pro warehouse
      4. Any warehouse as last resort

    Result is cached for 5 minutes.
    """
    global _warehouse_id_cache, _warehouse_id_ts
    if (
        not force_refresh
        and _warehouse_id_cache
        and (_time.time() - _warehouse_id_ts) < _WAREHOUSE_CACHE_TTL
    ):
        return _warehouse_id_cache

    w = _get_workspace_client()
    if not w:
        return None
    try:
        warehouses = list(w.warehouses.list())
    except Exception as exc:
        logger.warning("Could not list warehouses: %s", exc)
        return _warehouse_id_cache  # return stale cache if any

    if not warehouses:
        return None

    def _is_serverless(wh) -> bool:
        if getattr(wh, "enable_serverless_compute", False):
            return True
        # Some SDK versions expose it differently
        wt = getattr(wh, "warehouse_type", None)
        if wt and hasattr(wt, "value") and "SERVERLESS" in str(wt.value).upper():
            return True
        return False

    def _is_running(wh) -> bool:
        return wh.state and wh.state.value == "RUNNING"

    # Sort by priority
    running_serverless = [wh for wh in warehouses if _is_serverless(wh) and _is_running(wh)]
    any_serverless = [wh for wh in warehouses if _is_serverless(wh)]
    running_any = [wh for wh in warehouses if _is_running(wh)]

    pick = (
        running_serverless[0] if running_serverless
        else any_serverless[0] if any_serverless
        else running_any[0] if running_any
        else warehouses[0]
    )
    _warehouse_id_cache = pick.id
    _warehouse_id_ts = _time.time()
    sl = "serverless" if _is_serverless(pick) else "classic"
    st = pick.state.value if pick.state else "?"
    logger.info("Using SQL warehouse: %s (%s, %s, id=%s)", pick.name, sl, st, pick.id)
    return _warehouse_id_cache


# ── Startup diagnostics ─────────────────────────────────────────
_user = _resolve_lakebase_user()
logger.info(
    "Lakebase config: host=%s, db=%s, user=%s",
    settings.lakebase_dns, settings.lakebase_database, _user,
)
