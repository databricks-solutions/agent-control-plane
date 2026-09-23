"""FastAPI application entry point."""
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(name)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from backend.config import settings, get_databricks_host
from backend.api import agents, requests, kpis, analytics, health, websocket, gateway, mlflow, billing, tools, access, serving, workspaces, user_analytics, topology, operations, vector_search, gateway_logs, genie, settings as settings_api
from backend.utils.auth import get_current_user


# ── Startup / shutdown lifecycle ─────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Run on startup: ensure cache tables exist & kick off first refresh.

    Each init block has its own try/except so one failing service
    doesn't block the others or the entire app from starting.
    """
    import threading

    def _init_billing():
        try:
            from backend.services.billing_service import ensure_billing_tables
            ensure_billing_tables()
            # Refresh now runs in the discovery workflow (workflows/09_discover_billing).
        except Exception as exc:
            logger.warning("Billing startup init skipped: %s", exc)

    def _init_discovery():
        try:
            from backend.services.discovery_service import ensure_discovery_tables
            ensure_discovery_tables()
            # Discovery data is populated by the scheduled Workflow job
            # (workflows/01_discover_agents → Delta → 02_sync_to_lakebase → Lakebase).
            # Manual refresh is still available via POST /api/agents/sync.
            logger.info("Discovery tables ready (data populated by Workflow job)")
        except Exception as exc:
            logger.warning("Discovery startup init skipped: %s", exc)
        try:
            from backend.services.workspace_registry import ensure_workspace_registry_table
            ensure_workspace_registry_table()
        except Exception as exc:
            logger.warning("Workspace registry init skipped: %s", exc)
        try:
            from backend.services.agent_permissions_cache import ensure_agent_permissions_table
            ensure_agent_permissions_table()
        except Exception as exc:
            logger.warning("Agent permissions cache init skipped: %s", exc)
        try:
            # Access-control source of truth for non-account-admins — see
            # backend/utils/access_scope.py. Ensures the table exists and
            # warms the in-memory cache from Lakebase; the Account API
            # refresh itself runs from POST /agents/sync (needs a token).
            from backend.services.workspace_admins_service import ensure_workspace_admins_table
            ensure_workspace_admins_table()
        except Exception as exc:
            logger.warning("Workspace admins cache init skipped: %s", exc)

    def _init_tools():
        try:
            from backend.services.tools_service import ensure_tools_tables, maybe_refresh_async as tools_refresh
            ensure_tools_tables()
            tools_refresh()
        except Exception as exc:
            logger.warning("Tools startup init skipped: %s", exc)

    def _init_request_logs():
        try:
            from backend.database import execute_update
            ddl_statements = [
                """
                CREATE TABLE IF NOT EXISTS request_logs (
                    request_id      TEXT PRIMARY KEY,
                    agent_id        TEXT,
                    model_id        TEXT,
                    user_id         TEXT,
                    timestamp       TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
                    query_text      TEXT,
                    response_text   TEXT,
                    latency_ms      NUMERIC(12,2),
                    status_code     INTEGER,
                    input_tokens    INTEGER DEFAULT 0,
                    output_tokens   INTEGER DEFAULT 0,
                    cost_usd        NUMERIC(12,6) DEFAULT 0,
                    error_message   TEXT,
                    endpoint_type   TEXT
                )
                """,
                "CREATE INDEX IF NOT EXISTS idx_rl_agent   ON request_logs (agent_id)",
                "CREATE INDEX IF NOT EXISTS idx_rl_ts      ON request_logs (timestamp DESC)",
                "CREATE INDEX IF NOT EXISTS idx_rl_user    ON request_logs (user_id)",
            ]
            for stmt in ddl_statements:
                try:
                    execute_update(stmt)
                except Exception as exc:
                    logger.warning("request_logs DDL warning: %s", exc)
            logger.info("Request logs table ensured")
        except Exception as exc:
            logger.warning("Request logs startup init skipped: %s", exc)

    def _init_app_registries():
        # App-managed tables that the discovery workflow does NOT create — they
        # otherwise exist only via the standalone setup_lakebase_tables.py script.
        # If that script is skipped on a fresh deploy, /agents, /agents/full,
        # /kpis, /kpis/overview and /analytics/cost fail with
        # `UndefinedTable: agent_registry`. Ensuring them here makes deploy order
        # (workflow vs app) and running the setup script optional.
        try:
            from backend.database import execute_update
            from backend.app_schema import APP_REGISTRY_DDL  # single source of truth
            for stmt in APP_REGISTRY_DDL:
                try:
                    execute_update(stmt)
                except Exception as exc:
                    logger.warning("app registries DDL warning: %s", exc)
            logger.info("App registry tables ensured (agent_registry, model_registry, gateway_budgets)")
        except Exception as exc:
            logger.warning("App registries startup init skipped: %s", exc)

    def _init_gateway():
        try:
            from backend.services.gateway_service import prewarm_cache, ensure_gateway_usage_columns
            ensure_gateway_usage_columns()
            prewarm_cache()
        except Exception as exc:
            logger.warning("AI Gateway startup init skipped: %s", exc)

    def _init_observability():
        try:
            from backend.services.mlflow_service import ensure_observability_tables
            ensure_observability_tables()
        except Exception as exc:
            logger.warning("Observability startup init skipped: %s", exc)

    def _init_vector_search():
        try:
            from backend.services.vector_search_service import ensure_vector_search_tables
            ensure_vector_search_tables()
        except Exception as exc:
            logger.warning("Vector search startup init skipped: %s", exc)

    def _init_gateway_logs():
        try:
            from backend.services.gateway_logs_service import ensure_gateway_logs_table
            ensure_gateway_logs_table()
        except Exception as exc:
            logger.warning("Gateway logs startup init skipped: %s", exc)

    # Run all init in a background thread with a timeout so a hanging
    # DB connection doesn't prevent the server from starting.
    def _run_all_inits():
        try:
            from backend.config import log_lakebase_config
            log_lakebase_config()
        except Exception as exc:
            logger.warning("Lakebase config diagnostic skipped: %s", exc)
        try:
            from backend.services.settings_service import ensure_settings_table
            ensure_settings_table()
        except Exception as exc:
            logger.warning("Settings table init skipped: %s", exc)
        _init_billing()
        _init_discovery()
        _init_tools()
        _init_request_logs()
        _init_app_registries()
        _init_gateway()
        _init_observability()
        _init_vector_search()
        _init_gateway_logs()

    t = threading.Thread(target=_run_all_inits, daemon=True)
    t.start()
    t.join(timeout=120)  # Wait at most 120 s, then let the server start anyway
    if t.is_alive():
        logger.warning("Startup init still running in background (timed out after 120 s)")

    yield  # app runs
    # (nothing to tear down)


app = FastAPI(
    title=settings.api_title,
    version=settings.api_version,
    docs_url="/api/v1/docs",
    redoc_url="/api/v1/redoc",
    lifespan=lifespan,
)


# ── Security headers middleware ───────────────────────────────────
@app.middleware("http")
async def add_security_headers(request: Request, call_next):
    """Add security headers to all responses."""
    response = await call_next(request)
    response.headers["X-Frame-Options"] = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
    response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
    # CSP: allow self + inline styles (Tailwind) + data: URIs (icons)
    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self'; "
        "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com; "
        "img-src 'self' data: https:; "
        "font-src 'self' data: https://fonts.gstatic.com; "
        "connect-src 'self' https://*.cloud.databricks.com https://accounts.cloud.databricks.com; "
        "frame-ancestors 'none'"
    )
    return response


# ── Request audit logging middleware ─────────────────────────────
import time as _time

@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log every API request with user, method, path, status, and duration."""
    if request.url.path.startswith("/api/"):
        start = _time.monotonic()
        response = await call_next(request)
        elapsed = _time.monotonic() - start

        # Extract user from OBO header (lightweight — no SCIM call)
        user = "anonymous"
        token = request.headers.get("x-forwarded-access-token", "")
        if token:
            # Try to get from cache without resolving
            from backend.utils.auth import _get_cached
            cached = _get_cached(token)
            user = cached.username if cached else "obo-user"

        level = logging.WARNING if response.status_code >= 400 else logging.INFO
        logger.log(level, "API %s %s user=%s status=%d %.2fs",
                   request.method, request.url.path, user, response.status_code, elapsed)
        return response
    return await call_next(request)


# CORS middleware — only added when origins are explicitly configured (local dev).
# In production (Databricks Apps), frontend and backend share the same origin.
if settings.cors_origins:
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

# API root endpoint (must be before static files)
@app.get("/api/v1")
async def root():
    """Root endpoint."""
    return {
        "message": "AI Control Plane API",
        "version": settings.api_version,
        "docs": "/api/v1/docs"
    }


@app.get("/api/v1/config")
async def get_config():
    """Return public configuration for the frontend."""
    return {
        "databricks_host": get_databricks_host(),
        "features": {
            "genie_enabled": settings.feature_genie_enabled,
        },
    }


@app.get("/api/v1/me")
async def get_me(request: Request):
    """Return the authenticated user's identity, role, and workspace access
    scope. ``has_workspace_access`` / ``allowed_workspace_count`` /
    ``sees_deploy_workspace`` reflect the SAME rule every data endpoint
    enforces server-side (see backend/utils/access_scope.py) — the frontend
    uses them only to decide whether to render a "no access" state or the
    Shared workspace live-resources section; the real enforcement is on the
    API, not here.
    """
    try:
        user = await get_current_user(request)
        from backend.utils.access_scope import get_allowed_workspace_ids, sees_deploy_workspace
        allowed = get_allowed_workspace_ids(user)
        current_ws = None
        try:
            from backend.services.billing_service import get_current_workspace_id
            current_ws = get_current_workspace_id()
        except Exception:
            current_ws = None
        return {
            "username": user.username,
            "display_name": user.display_name,
            "user_id": user.user_id,
            "is_admin": user.is_admin,
            "is_account_admin": user.is_account_admin,
            "groups": user.groups,
            "has_workspace_access": allowed is None or len(allowed) > 0,
            "allowed_workspace_count": None if allowed is None else len(allowed),
            "sees_deploy_workspace": sees_deploy_workspace(allowed),
            "current_workspace_id": current_ws,
        }
    except Exception as exc:
        logger.warning("/api/v1/me failed: %s", exc)
        return {
            "username": "anonymous",
            "display_name": "Anonymous",
            "user_id": "",
            "is_admin": False,
            "is_account_admin": False,
            "groups": [],
            "has_workspace_access": False,
            "allowed_workspace_count": 0,
            "sees_deploy_workspace": False,
            "current_workspace_id": None,
        }


# Include routers
app.include_router(agents.router, prefix=settings.api_prefix)
app.include_router(requests.router, prefix=settings.api_prefix)
app.include_router(kpis.router, prefix=settings.api_prefix)
app.include_router(analytics.router, prefix=settings.api_prefix)
app.include_router(health.router, prefix=settings.api_prefix)
app.include_router(gateway.router, prefix=settings.api_prefix)
app.include_router(mlflow.router, prefix=settings.api_prefix)
app.include_router(billing.router, prefix=settings.api_prefix)
app.include_router(tools.router, prefix=settings.api_prefix)
app.include_router(access.router, prefix=settings.api_prefix)
app.include_router(serving.router, prefix=settings.api_prefix)
app.include_router(workspaces.router, prefix=settings.api_prefix)
app.include_router(user_analytics.router, prefix=settings.api_prefix)
app.include_router(topology.router, prefix=settings.api_prefix)
app.include_router(operations.router, prefix=settings.api_prefix)
app.include_router(vector_search.router, prefix=settings.api_prefix)
app.include_router(gateway_logs.router, prefix=settings.api_prefix)
app.include_router(genie.router, prefix=settings.api_prefix)
app.include_router(settings_api.router, prefix=settings.api_prefix)
app.include_router(websocket.router)

# ── Serve React SPA from the built dist/ folder ──────────────────
# Mount actual assets (JS, CSS, images) under /assets so they resolve
# by filename, then add a catch-all that returns index.html for every
# other path that isn't an /api or /ws route.  This is the standard
# approach for client-side routing (react-router-dom, etc.).
import os as _os
from fastapi.responses import FileResponse

_dist_dir = _os.path.join(_os.path.dirname(__file__), "..", "dist")
_index_html = _os.path.join(_dist_dir, "index.html")

if _os.path.isdir(_dist_dir):
    # Serve hashed JS/CSS bundles under /assets
    _assets_dir = _os.path.join(_dist_dir, "assets")
    if _os.path.isdir(_assets_dir):
        app.mount("/assets", StaticFiles(directory=_assets_dir), name="assets")

    # Serve individual static files at their own paths (logo, etc.)
    @app.get("/databricks-logo.svg")
    async def _logo():
        return FileResponse(_os.path.join(_dist_dir, "databricks-logo.svg"))

    # SPA catch-all: any non-API path → index.html so React Router handles it
    @app.get("/{full_path:path}")
    async def _spa_fallback(full_path: str):
        # An unknown /api or /ws path must NOT fall through to the SPA HTML with
        # a 200 — that masks typos/removed routes as "working" and can confuse
        # clients. Return a real JSON 404 for those namespaces instead. (Known
        # routes are registered before this catch-all, so they never reach here.)
        if full_path in ("api", "ws") or full_path.startswith(("api/", "ws/")):
            from fastapi.responses import JSONResponse
            return JSONResponse(status_code=404, content={"detail": "Not Found"})
        # If the exact file exists in dist/ (e.g. favicon), serve it
        candidate = _os.path.join(_dist_dir, full_path)
        if full_path and _os.path.isfile(candidate):
            return FileResponse(candidate)
        # Otherwise return index.html for client-side routing
        return FileResponse(_index_html)
else:
    logger.warning("dist/ folder not found — frontend not built yet.")


