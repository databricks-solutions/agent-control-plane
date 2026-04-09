"""Service that proxies MLflow / Databricks REST API calls.

Uses the Databricks SDK ApiClient (auto-authenticated) when running inside a
Databricks App, and falls back to raw httpx + static PAT for local dev.

Cross-workspace support: when a workspace_id is provided, the service resolves
the host URL via workspace_registry and makes OBO calls against the remote
workspace.  The fan-out for "all workspaces" uses ThreadPoolExecutor for
parallel requests.
"""
import json as _json
import httpx
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional
from backend.config import (
    _get_workspace_client,
    get_databricks_host,
    get_databricks_headers,
    find_serverless_warehouse_id,
)
from backend.database import execute_query, execute_update, execute_many

import logging

logger = logging.getLogger(__name__)

_TIMEOUT = 15.0
_FANOUT_WORKERS = 5
_FANOUT_TIMEOUT = 20.0
_SQL_POLL_INTERVAL = 3
_SQL_POLL_MAX = 20


# ── Lakebase DDL for observability cache ──────────────────────────

def ensure_observability_tables():
    """Create observability cache tables in Lakebase."""
    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS observability_traces (
            request_id         TEXT NOT NULL,
            workspace_id       TEXT NOT NULL,
            experiment_id      TEXT,
            trace_name         TEXT,
            state              TEXT,
            request_time       TEXT,
            execution_duration BIGINT,
            user_message       TEXT,
            response_preview   TEXT,
            token_usage        JSONB,
            model_id           TEXT,
            session_id         TEXT,
            trace_user         TEXT,
            source             TEXT,
            tags               JSONB,
            data_source        TEXT DEFAULT 'rest_api',
            last_synced        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (workspace_id, request_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_ot_ws   ON observability_traces (workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_ot_time ON observability_traces (request_time DESC)",
        """
        CREATE TABLE IF NOT EXISTS observability_experiments (
            experiment_id      TEXT NOT NULL,
            workspace_id       TEXT NOT NULL,
            name               TEXT,
            lifecycle_stage    TEXT,
            last_update_time   BIGINT,
            artifact_location  TEXT,
            data_source        TEXT DEFAULT 'system_table',
            last_synced        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (workspace_id, experiment_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_oe_ws ON observability_experiments (workspace_id)",
        """
        CREATE TABLE IF NOT EXISTS observability_runs (
            run_id             TEXT NOT NULL,
            workspace_id       TEXT NOT NULL,
            experiment_id      TEXT,
            status             TEXT,
            start_time         BIGINT,
            end_time           BIGINT,
            user_id            TEXT,
            run_name           TEXT,
            tags               JSONB,
            params             JSONB,
            metrics            JSONB,
            data_source        TEXT DEFAULT 'system_table',
            last_synced        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (workspace_id, run_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_or_ws ON observability_runs (workspace_id)",
        # Add columns to existing tables (idempotent)
        "ALTER TABLE observability_experiments ADD COLUMN IF NOT EXISTS data_source TEXT DEFAULT 'system_table'",
        "ALTER TABLE observability_experiments ADD COLUMN IF NOT EXISTS tags JSONB",
        "ALTER TABLE observability_runs ADD COLUMN IF NOT EXISTS tags JSONB",
        "ALTER TABLE observability_runs ADD COLUMN IF NOT EXISTS params JSONB",
        "ALTER TABLE observability_runs ADD COLUMN IF NOT EXISTS metrics JSONB",
        "ALTER TABLE observability_runs ADD COLUMN IF NOT EXISTS data_source TEXT DEFAULT 'system_table'",
        "ALTER TABLE observability_traces ADD COLUMN IF NOT EXISTS data_source TEXT DEFAULT 'rest_api'",
    ]
    for stmt in ddl_statements:
        try:
            execute_update(stmt)
        except Exception as exc:
            logger.warning("Observability DDL warning: %s", exc)
    logger.info("Observability cache tables ensured")


# ── System table SQL execution ─────────────────────────────────

def _execute_system_sql(sql: str) -> List[Dict[str, Any]]:
    """Execute SQL against Databricks system tables via the SQL Statements API.

    Same pattern as billing_service._execute_system_sql: SDK-first with httpx
    fallback, async polling for long-running statements.
    """
    import time

    wh_id = find_serverless_warehouse_id()
    if not wh_id:
        logger.warning("No SQL warehouse found for system table query")
        return []

    path = "/api/2.0/sql/statements"
    body = {
        "warehouse_id": wh_id,
        "statement": sql,
        "wait_timeout": "50s",
        "disposition": "INLINE",
        "format": "JSON_ARRAY",
    }

    w = _get_workspace_client()
    resp_json: Optional[dict] = None
    if w:
        try:
            resp_json = w.api_client.do("POST", path, body=body)
        except Exception as exc:
            logger.warning("SDK SQL exec failed: %s", exc)

    if resp_json is None:
        base = get_databricks_host()
        if not base:
            return []
        try:
            resp = httpx.post(
                f"{base}{path}",
                headers=get_databricks_headers(),
                json=body,
                timeout=_TIMEOUT,
            )
            resp.raise_for_status()
            resp_json = resp.json()
        except Exception as exc:
            logger.warning("httpx SQL exec failed: %s", exc)
            return []

    if not resp_json:
        return []

    status = resp_json.get("status", {}).get("state", "")
    statement_id = resp_json.get("statement_id", "")

    # Poll if the statement is still running
    if status in ("PENDING", "RUNNING") and statement_id:
        poll_path = f"/api/2.0/sql/statements/{statement_id}"
        for attempt in range(_SQL_POLL_MAX):
            time.sleep(_SQL_POLL_INTERVAL)
            try:
                if w:
                    resp_json = w.api_client.do("GET", poll_path)
                else:
                    base = get_databricks_host()
                    r = httpx.get(
                        f"{base}{poll_path}",
                        headers=get_databricks_headers(),
                        timeout=_TIMEOUT,
                    )
                    r.raise_for_status()
                    resp_json = r.json()
            except Exception as exc:
                logger.warning("Poll attempt %s failed: %s", attempt + 1, exc)
                continue
            status = resp_json.get("status", {}).get("state", "")
            if status not in ("PENDING", "RUNNING"):
                break
        else:
            logger.warning("Statement %s timed out", statement_id)
            return []

    if status != "SUCCEEDED":
        err = resp_json.get("status", {}).get("error", {})
        logger.warning("SQL status: %s — %s", status, err.get('message', ''))
        return []

    manifest = resp_json.get("manifest", {})
    columns = [c["name"] for c in manifest.get("schema", {}).get("columns", [])]
    data_array = resp_json.get("result", {}).get("data_array", [])
    return [dict(zip(columns, row)) for row in data_array]


# ── System table queries (cross-workspace) ─────────────────────

def search_experiments_system_tables(max_results: int = 5000) -> List[Dict[str, Any]]:
    """Query system.mlflow.experiments_latest for cross-workspace experiments.

    Schema: account_id, update_time, delete_time, workspace_id, experiment_id, name, create_time
    """
    sql = f"""
    SELECT
        CAST(experiment_id AS STRING) AS experiment_id,
        CAST(workspace_id AS STRING) AS workspace_id,
        name,
        CASE WHEN delete_time IS NULL THEN 'active' ELSE 'deleted' END AS lifecycle_stage,
        CAST(UNIX_TIMESTAMP(COALESCE(update_time, create_time)) * 1000 AS BIGINT) AS last_update_time,
        'system_table' AS data_source
    FROM system.mlflow.experiments_latest
    WHERE delete_time IS NULL
    ORDER BY COALESCE(update_time, create_time) DESC
    LIMIT {int(max_results)}
    """
    rows = _execute_system_sql(sql)
    logger.info("system.mlflow.experiments_latest: %s experiments", len(rows))
    return rows


def search_runs_system_tables(max_results: int = 5000) -> List[Dict[str, Any]]:
    """Query system.mlflow.runs_latest for cross-workspace runs.

    Schema: account_id, update_time, delete_time, workspace_id, run_id, experiment_id,
            created_by, start_time, end_time, run_name, status, params, tags, aggregated_metrics
    """
    sql = f"""
    SELECT
        run_id,
        CAST(experiment_id AS STRING) AS experiment_id,
        CAST(workspace_id AS STRING) AS workspace_id,
        status,
        CAST(UNIX_TIMESTAMP(start_time) * 1000 AS BIGINT) AS start_time,
        CAST(UNIX_TIMESTAMP(end_time) * 1000 AS BIGINT) AS end_time,
        created_by AS user_id,
        run_name,
        TO_JSON(tags) AS tags,
        TO_JSON(params) AS params,
        TO_JSON(aggregated_metrics) AS metrics,
        'system_table' AS data_source
    FROM system.mlflow.runs_latest
    WHERE delete_time IS NULL
    ORDER BY start_time DESC
    LIMIT {int(max_results)}
    """
    rows = _execute_system_sql(sql)
    # Parse JSON string fields back to dicts/lists
    for r in rows:
        for field in ("tags", "params", "metrics"):
            if isinstance(r.get(field), str):
                try:
                    r[field] = _json.loads(r[field])
                except Exception:
                    pass
    logger.info("system.mlflow.runs_latest: %s runs", len(rows))
    return rows


# ── HTTP helpers (SDK first, httpx fallback) ─────────────────────

def _sdk_get(path: str, query: Optional[dict] = None) -> Optional[dict]:
    """GET via the Databricks SDK ApiClient (handles auth automatically)."""
    w = _get_workspace_client()
    if w is None:
        return None
    try:
        return w.api_client.do("GET", path, query=query)
    except Exception as exc:
        logger.warning("SDK GET %s failed: %s", path, exc)
        return None


def _sdk_post(path: str, body: Optional[dict] = None) -> Optional[dict]:
    """POST via the Databricks SDK ApiClient."""
    w = _get_workspace_client()
    if w is None:
        return None
    try:
        return w.api_client.do("POST", path, body=body)
    except Exception as exc:
        logger.warning("SDK POST %s failed: %s", path, exc)
        return None


def _httpx_get(path: str, params: Optional[dict] = None) -> Optional[dict]:
    """GET via raw httpx + PAT (local dev fallback)."""
    base = get_databricks_host()
    if not base:
        return None
    try:
        resp = httpx.get(
            f"{base}{path}", headers=get_databricks_headers(),
            params=params, timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.warning("httpx GET %s failed: %s", path, exc)
        return None


def _httpx_post(path: str, body: Optional[dict] = None) -> Optional[dict]:
    """POST via raw httpx + PAT (local dev fallback)."""
    base = get_databricks_host()
    if not base:
        return None
    try:
        resp = httpx.post(
            f"{base}{path}", headers=get_databricks_headers(),
            json=body, timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.warning("httpx POST %s failed: %s", path, exc)
        return None


def _get(path: str, query: Optional[dict] = None, *, user_token: Optional[str] = None) -> dict:
    """GET with OBO-first, SDK-fallback, httpx-fallback.  Returns {} on total failure.

    If *user_token* is provided (OBO), tries the user's token first.
    On failure (e.g. 403 scope mismatch), falls back to the app SP.
    """
    if user_token:
        result = _obo_get(path, query, user_token)
        if result is not None:
            return result
        # OBO failed (scope missing?) — fall back to SP
    result = _sdk_get(path, query)
    if result is not None:
        return result
    return _httpx_get(path, query) or {}


def _post(path: str, body: Optional[dict] = None, *, user_token: Optional[str] = None) -> dict:
    """POST with OBO-first, SDK-fallback, httpx-fallback.  Returns {} on total failure."""
    if user_token:
        result = _obo_post(path, body, user_token)
        if result is not None:
            return result
        # OBO failed — fall back to SP
    result = _sdk_post(path, body)
    if result is not None:
        return result
    return _httpx_post(path, body) or {}


def _obo_get(path: str, params: Optional[dict], token: str, base_url: Optional[str] = None) -> Optional[dict]:
    """GET using the user's OBO token.  *base_url* overrides the default host (for cross-workspace)."""
    base = base_url or get_databricks_host()
    if not base:
        return None
    try:
        resp = httpx.get(
            f"{base}{path}",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            params=params, timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.warning("OBO GET %s%s failed: %s", base, path, exc)
        return None


def _obo_post(path: str, body: Optional[dict], token: str, base_url: Optional[str] = None) -> Optional[dict]:
    """POST using the user's OBO token.  *base_url* overrides the default host (for cross-workspace)."""
    base = base_url or get_databricks_host()
    if not base:
        return None
    try:
        resp = httpx.post(
            f"{base}{path}",
            headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
            json=body, timeout=_TIMEOUT,
        )
        resp.raise_for_status()
        return resp.json()
    except Exception as exc:
        logger.warning("OBO POST %s%s failed: %s", base, path, exc)
        return None


# ── helpers ─────────────────────────────────────────────────────

def _all_experiment_ids(user_token: Optional[str] = None) -> List[str]:
    """Return all experiment ids in the workspace."""
    exps = search_experiments(200, user_token=user_token)
    return [e["experiment_id"] for e in exps if e.get("experiment_id")]


# ── Experiments ─────────────────────────────────────────────────

def search_experiments(max_results: int = 50, *, user_token: Optional[str] = None) -> List[Dict[str, Any]]:
    """Search MLflow experiments, ordered by most recently updated first."""
    data = _post("/api/2.0/mlflow/experiments/search", {
        "max_results": max_results,
        "order_by": ["last_update_time DESC"],
    }, user_token=user_token)
    return data.get("experiments", [])


def get_experiment(experiment_id: str) -> Optional[Dict[str, Any]]:
    """Get a single experiment by ID."""
    data = _get("/api/2.0/mlflow/experiments/get", {"experiment_id": experiment_id})
    return data.get("experiment")


# ── Runs (evaluation runs live here) ───────────────────────────

def search_runs(
    experiment_ids: Optional[List[str]] = None,
    filter_string: str = "",
    max_results: int = 50,
    order_by: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Search MLflow runs across experiments."""
    body: Dict[str, Any] = {"max_results": max_results}
    body["experiment_ids"] = experiment_ids or _all_experiment_ids()
    if filter_string:
        body["filter"] = filter_string
    body["order_by"] = order_by or ["start_time DESC"]
    data = _post("/api/2.0/mlflow/runs/search", body)
    return data.get("runs", [])


# ── Traces ──────────────────────────────────────────────────────

def search_traces(
    experiment_ids: Optional[List[str]] = None,
    max_results: int = 50,
    filter_string: str = "",
    order_by: Optional[List[str]] = None,
    *,
    user_token: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Search MLflow traces.  Auto-discovers experiment_ids if omitted."""
    ids = experiment_ids or _all_experiment_ids(user_token=user_token)
    if not ids:
        return []

    all_traces: List[Dict[str, Any]] = []
    for eid in ids:
        params: Dict[str, Any] = {
            "experiment_ids": eid,
            "max_results": max_results,
        }
        if filter_string:
            params["filter"] = filter_string
        if order_by:
            params["order_by"] = order_by

        data = _get("/api/2.0/mlflow/traces", params, user_token=user_token)
        for trace in data.get("traces", []):
            # Ensure experiment_id is always available at the top level
            if "experiment_id" not in trace:
                trace["experiment_id"] = eid
            all_traces.append(trace)

    # Sort by timestamp descending
    all_traces.sort(
        key=lambda t: t.get("timestamp_ms") or t.get("info", {}).get("timestamp_ms", 0),
        reverse=True,
    )
    return all_traces[:max_results]


# ── Registered Models (Unity Catalog) ──────────────────────────

def search_registered_models(max_results: int = 100) -> List[Dict[str, Any]]:
    """Search UC registered models."""
    data = _get(
        "/api/2.0/mlflow/unity-catalog/registered-models/search",
        {"max_results": max_results},
    )
    return data.get("registered_models", [])


def search_model_versions(
    name: str, max_results: int = 20
) -> List[Dict[str, Any]]:
    """Search versions for a registered model."""
    data = _get(
        "/api/2.0/mlflow/unity-catalog/model-versions/search",
        {"filter": f"name='{name}'", "max_results": max_results},
    )
    return data.get("model_versions", [])


# ── Trace detail ────────────────────────────────────────────────

def get_trace_spans(request_id: str) -> List[Dict[str, Any]]:
    """Return the spans list for a single trace.

    The search endpoint sometimes omits span data; this fetches the full
    trace record which always includes spans in trace_data.
    """
    data = _get(f"/api/2.0/mlflow/traces/{request_id}")
    # Individual trace endpoint: {"trace": {"trace_info": {…}, "trace_data": {"spans": […]}}}
    spans = data.get("trace", {}).get("trace_data", {}).get("spans", [])
    if spans:
        return spans
    # Fallback: search-style format {"info": {…}, "data": {"spans": […]}}
    return data.get("data", {}).get("spans", [])


def get_trace_detail(request_id: str, *, user_token: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Get full trace info with parsed metadata for a single trace."""
    data = _get(f"/api/2.0/mlflow/traces/{request_id}", user_token=user_token)
    trace_info = data.get("trace", {}).get("trace_info", {})
    if not trace_info:
        return None

    meta = trace_info.get("trace_metadata", {})
    tags = trace_info.get("tags", {})

    # Parse token usage
    token_usage = {}
    try:
        token_usage = _json.loads(meta.get("mlflow.trace.tokenUsage", "{}"))
    except Exception:
        pass

    # Parse size stats
    size_stats = {}
    try:
        size_stats = _json.loads(meta.get("mlflow.trace.sizeStats", "{}"))
    except Exception:
        pass

    # Parse request JSON to extract user messages
    request_raw = trace_info.get("request", "")
    user_message = None
    try:
        request_parsed = _json.loads(request_raw)
        inputs = request_parsed.get("input", [])
        for inp in inputs:
            if isinstance(inp, dict) and inp.get("role") == "user":
                content = inp.get("content", [])
                if isinstance(content, list):
                    for c in content:
                        if isinstance(c, dict) and c.get("text"):
                            user_message = c["text"]
                            break
                elif isinstance(content, str):
                    user_message = content
    except Exception:
        pass

    return {
        "request_id": request_id,
        "trace_name": tags.get("mlflow.traceName", "—"),
        "experiment_id": trace_info.get("trace_location", {}).get(
            "mlflow_experiment", {}
        ).get("experiment_id"),
        "state": trace_info.get("state", "—"),
        "request_time": trace_info.get("request_time"),
        "execution_duration": trace_info.get("execution_duration"),
        "user_message": user_message,
        "response": trace_info.get("response", ""),
        "response_preview": trace_info.get("response_preview", ""),
        "request_raw": request_raw,
        "token_usage": token_usage,
        "size_stats": size_stats,
        "model_id": meta.get("mlflow.modelId"),
        "session_id": meta.get("mlflow.trace.session"),
        "user": meta.get("mlflow.user"),
        "source": meta.get("mlflow.source.name"),
        "trace_schema_version": meta.get("mlflow.trace_schema.version"),
        "artifact_location": tags.get("mlflow.artifactLocation"),
        "tags": tags,
        "metadata": meta,
    }


# ── Cross-workspace helpers ────────────────────────────────────

def _get_for_workspace(
    path: str,
    query: Optional[dict],
    workspace_id: str,
    user_token: str,
) -> dict:
    """GET against a remote workspace using OBO token.  Returns {} on failure."""
    from backend.services.workspace_registry import get_workspace_host
    host = get_workspace_host(workspace_id)
    if not host:
        return {}
    return _obo_get(path, query, user_token, base_url=host) or {}


def _post_for_workspace(
    path: str,
    body: Optional[dict],
    workspace_id: str,
    user_token: str,
) -> dict:
    """POST against a remote workspace using OBO token.  Returns {} on failure."""
    from backend.services.workspace_registry import get_workspace_host
    host = get_workspace_host(workspace_id)
    if not host:
        return {}
    return _obo_post(path, body, user_token, base_url=host) or {}


# ── Cross-workspace: Experiments ────────────────────────────────

def search_experiments_for_workspace(
    workspace_id: str,
    max_results: int = 50,
    *,
    user_token: str,
) -> List[Dict[str, Any]]:
    """Search experiments in a specific workspace."""
    data = _post_for_workspace(
        "/api/2.0/mlflow/experiments/search",
        {"max_results": max_results, "order_by": ["last_update_time DESC"]},
        workspace_id,
        user_token,
    )
    exps = data.get("experiments", [])
    for exp in exps:
        exp["workspace_id"] = workspace_id
    return exps


def search_experiments_all_workspaces(
    max_results: int = 50,
    *,
    user_token: str,
) -> List[Dict[str, Any]]:
    """Fan out experiment search across all registered workspaces."""
    from backend.services.workspace_registry import get_all_workspace_hosts
    hosts = get_all_workspace_hosts()
    if not hosts:
        return []

    all_exps: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(len(hosts), _FANOUT_WORKERS)) as pool:
        futures = {
            pool.submit(search_experiments_for_workspace, ws_id, max_results, user_token=user_token): ws_id
            for ws_id in hosts
        }
        for fut in as_completed(futures, timeout=_FANOUT_TIMEOUT):
            try:
                all_exps.extend(fut.result())
            except Exception as exc:
                ws_id = futures[fut]
                logger.warning("Experiment fan-out failed for workspace %s: %s", ws_id, exc)

    all_exps.sort(key=lambda e: int(e.get("last_update_time", 0)), reverse=True)
    return all_exps[:max_results]


# ── Cross-workspace: Traces ────────────────────────────────────

def search_traces_for_workspace(
    workspace_id: str,
    max_results: int = 50,
    filter_string: str = "",
    *,
    user_token: str,
) -> List[Dict[str, Any]]:
    """Search traces in a specific remote workspace."""
    # First discover experiment IDs in that workspace
    exps = search_experiments_for_workspace(workspace_id, 200, user_token=user_token)
    exp_ids = [e["experiment_id"] for e in exps if e.get("experiment_id")]
    if not exp_ids:
        return []

    all_traces: List[Dict[str, Any]] = []
    for eid in exp_ids:
        params: Dict[str, Any] = {"experiment_ids": eid, "max_results": max_results}
        if filter_string:
            params["filter"] = filter_string
        data = _get_for_workspace("/api/2.0/mlflow/traces", params, workspace_id, user_token)
        for trace in data.get("traces", []):
            if "experiment_id" not in trace:
                trace["experiment_id"] = eid
            trace["workspace_id"] = workspace_id
            all_traces.append(trace)

    all_traces.sort(
        key=lambda t: t.get("timestamp_ms") or t.get("info", {}).get("timestamp_ms", 0),
        reverse=True,
    )
    return all_traces[:max_results]


def search_traces_all_workspaces(
    max_results: int = 50,
    filter_string: str = "",
    *,
    user_token: str,
) -> List[Dict[str, Any]]:
    """Fan out trace search across all registered workspaces."""
    from backend.services.workspace_registry import get_all_workspace_hosts
    hosts = get_all_workspace_hosts()
    if not hosts:
        return []

    all_traces: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(len(hosts), _FANOUT_WORKERS)) as pool:
        futures = {
            pool.submit(
                search_traces_for_workspace, ws_id, max_results, filter_string, user_token=user_token,
            ): ws_id
            for ws_id in hosts
        }
        for fut in as_completed(futures, timeout=_FANOUT_TIMEOUT):
            try:
                all_traces.extend(fut.result())
            except Exception as exc:
                ws_id = futures[fut]
                logger.warning("Trace fan-out failed for workspace %s: %s", ws_id, exc)

    all_traces.sort(
        key=lambda t: t.get("timestamp_ms") or t.get("info", {}).get("timestamp_ms", 0),
        reverse=True,
    )
    return all_traces[:max_results]


# ── Cross-workspace: Trace detail ──────────────────────────────

def get_trace_detail_for_workspace(
    request_id: str,
    workspace_id: str,
    *,
    user_token: str,
) -> Optional[Dict[str, Any]]:
    """Get full trace detail from a specific remote workspace."""
    data = _get_for_workspace(
        f"/api/2.0/mlflow/traces/{request_id}", None, workspace_id, user_token,
    )
    trace_info = data.get("trace", {}).get("trace_info", {})
    if not trace_info:
        return None

    # Reuse the same parsing logic as get_trace_detail
    meta = trace_info.get("trace_metadata", {})
    tags = trace_info.get("tags", {})
    token_usage = {}
    try:
        token_usage = _json.loads(meta.get("mlflow.trace.tokenUsage", "{}"))
    except Exception:
        pass
    size_stats = {}
    try:
        size_stats = _json.loads(meta.get("mlflow.trace.sizeStats", "{}"))
    except Exception:
        pass
    request_raw = trace_info.get("request", "")
    user_message = None
    try:
        request_parsed = _json.loads(request_raw)
        inputs = request_parsed.get("input", [])
        for inp in inputs:
            if isinstance(inp, dict) and inp.get("role") == "user":
                content = inp.get("content", [])
                if isinstance(content, list):
                    for c in content:
                        if isinstance(c, dict) and c.get("text"):
                            user_message = c["text"]
                            break
                elif isinstance(content, str):
                    user_message = content
    except Exception:
        pass

    result = {
        "request_id": request_id,
        "workspace_id": workspace_id,
        "trace_name": tags.get("mlflow.traceName", "—"),
        "experiment_id": trace_info.get("trace_location", {}).get(
            "mlflow_experiment", {}
        ).get("experiment_id"),
        "state": trace_info.get("state", "—"),
        "request_time": trace_info.get("request_time"),
        "execution_duration": trace_info.get("execution_duration"),
        "user_message": user_message,
        "response": trace_info.get("response", ""),
        "response_preview": trace_info.get("response_preview", ""),
        "request_raw": request_raw,
        "token_usage": token_usage,
        "size_stats": size_stats,
        "model_id": meta.get("mlflow.modelId"),
        "session_id": meta.get("mlflow.trace.session"),
        "user": meta.get("mlflow.user"),
        "source": meta.get("mlflow.source.name"),
        "trace_schema_version": meta.get("mlflow.trace_schema.version"),
        "artifact_location": tags.get("mlflow.artifactLocation"),
        "tags": tags,
        "metadata": meta,
    }
    return result


# ── Cross-workspace: Models ────────────────────────────────────

def search_models_for_workspace(
    workspace_id: str,
    max_results: int = 100,
    *,
    user_token: str,
) -> List[Dict[str, Any]]:
    """Search UC registered models in a specific workspace."""
    data = _get_for_workspace(
        "/api/2.0/mlflow/unity-catalog/registered-models/search",
        {"max_results": max_results},
        workspace_id,
        user_token,
    )
    models = data.get("registered_models", [])
    for m in models:
        m["workspace_id"] = workspace_id
    return models


def search_models_all_workspaces(
    max_results: int = 100,
    *,
    user_token: str,
) -> List[Dict[str, Any]]:
    """Fan out model search across all registered workspaces."""
    from backend.services.workspace_registry import get_all_workspace_hosts
    hosts = get_all_workspace_hosts()
    if not hosts:
        return []

    all_models: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(len(hosts), _FANOUT_WORKERS)) as pool:
        futures = {
            pool.submit(search_models_for_workspace, ws_id, max_results, user_token=user_token): ws_id
            for ws_id in hosts
        }
        for fut in as_completed(futures, timeout=_FANOUT_TIMEOUT):
            try:
                all_models.extend(fut.result())
            except Exception as exc:
                ws_id = futures[fut]
                logger.warning("Model fan-out failed for workspace %s: %s", ws_id, exc)

    return all_models[:max_results]


# ── Cross-workspace: Runs ──────────────────────────────────────

def search_runs_for_workspace(
    workspace_id: str,
    max_results: int = 50,
    filter_string: str = "",
    *,
    user_token: str,
) -> List[Dict[str, Any]]:
    """Search MLflow runs in a specific workspace."""
    exps = search_experiments_for_workspace(workspace_id, 200, user_token=user_token)
    exp_ids = [e["experiment_id"] for e in exps if e.get("experiment_id")]
    if not exp_ids:
        return []

    body: Dict[str, Any] = {
        "max_results": max_results,
        "experiment_ids": exp_ids,
        "order_by": ["start_time DESC"],
    }
    if filter_string:
        body["filter"] = filter_string
    data = _post_for_workspace("/api/2.0/mlflow/runs/search", body, workspace_id, user_token)
    runs = data.get("runs", [])
    for r in runs:
        r["workspace_id"] = workspace_id
    return runs


def search_runs_all_workspaces(
    max_results: int = 50,
    filter_string: str = "",
    *,
    user_token: str,
) -> List[Dict[str, Any]]:
    """Fan out run search across all registered workspaces."""
    from backend.services.workspace_registry import get_all_workspace_hosts
    hosts = get_all_workspace_hosts()
    if not hosts:
        return []

    all_runs: List[Dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=min(len(hosts), _FANOUT_WORKERS)) as pool:
        futures = {
            pool.submit(
                search_runs_for_workspace, ws_id, max_results, filter_string, user_token=user_token,
            ): ws_id
            for ws_id in hosts
        }
        for fut in as_completed(futures, timeout=_FANOUT_TIMEOUT):
            try:
                all_runs.extend(fut.result())
            except Exception as exc:
                ws_id = futures[fut]
                logger.warning("Run fan-out failed for workspace %s: %s", ws_id, exc)

    all_runs.sort(
        key=lambda r: int(r.get("info", {}).get("start_time", 0)),
        reverse=True,
    )
    return all_runs[:max_results]


# ── Lakebase cache: write ──────────────────────────────────────

def refresh_observability_cache(*, user_token: str) -> Dict[str, int]:
    """Refresh the Lakebase observability cache from all workspaces.

    Returns counts of cached rows per entity type.
    """
    counts: Dict[str, int] = {"traces": 0, "experiments": 0}

    # Cache experiments
    try:
        exps = search_experiments_all_workspaces(200, user_token=user_token)
        if exps:
            for exp in exps:
                try:
                    execute_update(
                        """INSERT INTO observability_experiments
                           (experiment_id, workspace_id, name, lifecycle_stage, last_update_time, artifact_location, last_synced)
                           VALUES (%s, %s, %s, %s, %s, %s, NOW())
                           ON CONFLICT (workspace_id, experiment_id) DO UPDATE SET
                               name = EXCLUDED.name,
                               lifecycle_stage = EXCLUDED.lifecycle_stage,
                               last_update_time = EXCLUDED.last_update_time,
                               artifact_location = EXCLUDED.artifact_location,
                               last_synced = NOW()""",
                        (
                            exp.get("experiment_id"),
                            exp.get("workspace_id"),
                            exp.get("name"),
                            exp.get("lifecycle_stage"),
                            int(exp.get("last_update_time", 0)),
                            exp.get("artifact_location"),
                        ),
                    )
                    counts["experiments"] += 1
                except Exception as exc:
                    logger.warning("Cache upsert experiment failed: %s", exc)
    except Exception as exc:
        logger.warning("Experiment cache refresh failed: %s", exc)

    # Cache traces
    try:
        traces = search_traces_all_workspaces(200, user_token=user_token)
        if traces:
            for t in traces:
                request_id = (
                    t.get("request_id")
                    or t.get("info", {}).get("request_id", "")
                )
                if not request_id:
                    continue
                try:
                    execute_update(
                        """INSERT INTO observability_traces
                           (request_id, workspace_id, experiment_id, trace_name, state,
                            request_time, execution_duration, token_usage, model_id,
                            trace_user, source, tags, last_synced)
                           VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW())
                           ON CONFLICT (workspace_id, request_id) DO UPDATE SET
                               trace_name = EXCLUDED.trace_name,
                               state = EXCLUDED.state,
                               request_time = EXCLUDED.request_time,
                               execution_duration = EXCLUDED.execution_duration,
                               token_usage = EXCLUDED.token_usage,
                               model_id = EXCLUDED.model_id,
                               trace_user = EXCLUDED.trace_user,
                               source = EXCLUDED.source,
                               tags = EXCLUDED.tags,
                               last_synced = NOW()""",
                        (
                            request_id,
                            t.get("workspace_id", ""),
                            t.get("experiment_id", ""),
                            t.get("info", {}).get("tags", {}).get("mlflow.traceName", ""),
                            t.get("info", {}).get("state", t.get("state", "")),
                            str(t.get("timestamp_ms") or t.get("info", {}).get("timestamp_ms", "")),
                            t.get("info", {}).get("execution_duration"),
                            _json.dumps({}),  # token_usage not in search results
                            t.get("info", {}).get("trace_metadata", {}).get("mlflow.modelId", ""),
                            t.get("info", {}).get("trace_metadata", {}).get("mlflow.user", ""),
                            t.get("info", {}).get("trace_metadata", {}).get("mlflow.source.name", ""),
                            _json.dumps(t.get("info", {}).get("tags", {})),
                        ),
                    )
                    counts["traces"] += 1
                except Exception as exc:
                    logger.warning("Cache upsert trace failed: %s", exc)
    except Exception as exc:
        logger.warning("Trace cache refresh failed: %s", exc)

    logger.info("Observability cache refreshed: %s", counts)
    return counts


# ── Lakebase cache: read ───────────────────────────────────────

def get_cached_traces(workspace_id: Optional[str] = None, limit: int = 10000) -> List[Dict[str, Any]]:
    """Read traces from the Lakebase cache, optionally filtered by workspace."""
    if workspace_id:
        rows = execute_query(
            "SELECT * FROM observability_traces WHERE workspace_id = %s ORDER BY request_time DESC LIMIT %s",
            (workspace_id, limit),
        )
    else:
        rows = execute_query(
            "SELECT * FROM observability_traces ORDER BY request_time DESC LIMIT %s",
            (limit,),
        )
    return rows


def get_cached_experiments(workspace_id: Optional[str] = None, limit: int = 10000) -> List[Dict[str, Any]]:
    """Read experiments from the Lakebase cache, optionally filtered by workspace."""
    if workspace_id:
        rows = execute_query(
            "SELECT * FROM observability_experiments WHERE workspace_id = %s ORDER BY last_update_time DESC LIMIT %s",
            (workspace_id, limit),
        )
    else:
        rows = execute_query(
            "SELECT * FROM observability_experiments ORDER BY last_update_time DESC LIMIT %s",
            (limit,),
        )
    return rows


def get_cached_runs(workspace_id: Optional[str] = None, limit: int = 10000) -> List[Dict[str, Any]]:
    """Read runs from the Lakebase cache, optionally filtered by workspace."""
    if workspace_id:
        rows = execute_query(
            "SELECT * FROM observability_runs WHERE workspace_id = %s ORDER BY start_time DESC LIMIT %s",
            (workspace_id, limit),
        )
    else:
        rows = execute_query(
            "SELECT * FROM observability_runs ORDER BY start_time DESC LIMIT %s",
            (limit,),
        )
    return rows


def get_observability_workspaces() -> List[Dict[str, Any]]:
    """Return workspace IDs that have cached observability data, with counts."""
    try:
        rows = execute_query(
            """SELECT workspace_id,
                      COUNT(*) AS trace_count,
                      MAX(last_synced) AS last_synced
               FROM observability_traces
               GROUP BY workspace_id
               ORDER BY trace_count DESC"""
        )
        return rows
    except Exception:
        return []
