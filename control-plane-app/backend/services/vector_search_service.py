"""Service for Knowledge Bases monitoring — Vector Search + Lakebase discovery, caching, and billing."""
import logging
import json as _json
from typing import List, Dict, Any, Optional
from urllib.parse import quote

from backend.config import _get_workspace_client, get_databricks_host, get_databricks_headers, find_serverless_warehouse_id
from backend.database import execute_query, execute_update

logger = logging.getLogger(__name__)

_TIMEOUT = 15.0
_SQL_POLL_INTERVAL = 3
_SQL_POLL_MAX = 20


# ── Lakebase DDL ──────────────────────────────────────────────

def ensure_vector_search_tables():
    """Create vector search cache tables in Lakebase."""
    ddl_statements = [
        """
        CREATE TABLE IF NOT EXISTS vector_search_endpoints (
            endpoint_name  TEXT PRIMARY KEY,
            endpoint_id    TEXT,
            status         TEXT,
            endpoint_type  TEXT,
            num_indexes    INT DEFAULT 0,
            creator        TEXT,
            workspace_id   TEXT,
            created_at     TIMESTAMP,
            last_synced    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_vse_status ON vector_search_endpoints (status)",
        """
        CREATE TABLE IF NOT EXISTS vector_search_indexes (
            index_name     TEXT NOT NULL,
            endpoint_name  TEXT NOT NULL,
            index_type     TEXT,
            primary_key    TEXT,
            creator        TEXT,
            workspace_id   TEXT,
            detailed_state TEXT,
            indexed_row_count INT DEFAULT 0,
            ready          BOOLEAN DEFAULT FALSE,
            status_message TEXT,
            source_table   TEXT,
            embedding_model TEXT,
            pipeline_type  TEXT,
            last_synced    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (endpoint_name, index_name)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_vsi_endpoint ON vector_search_indexes (endpoint_name)",
        "CREATE INDEX IF NOT EXISTS idx_vsi_type ON vector_search_indexes (index_type)",
        # Add columns to existing tables (idempotent)
        "ALTER TABLE vector_search_indexes ADD COLUMN IF NOT EXISTS detailed_state TEXT",
        "ALTER TABLE vector_search_indexes ADD COLUMN IF NOT EXISTS indexed_row_count INT DEFAULT 0",
        "ALTER TABLE vector_search_indexes ADD COLUMN IF NOT EXISTS ready BOOLEAN DEFAULT FALSE",
        "ALTER TABLE vector_search_indexes ADD COLUMN IF NOT EXISTS status_message TEXT",
        "ALTER TABLE vector_search_indexes ADD COLUMN IF NOT EXISTS source_table TEXT",
        "ALTER TABLE vector_search_indexes ADD COLUMN IF NOT EXISTS embedding_model TEXT",
        "ALTER TABLE vector_search_indexes ADD COLUMN IF NOT EXISTS pipeline_type TEXT",
        # Endpoint health history (one row per discovery run per endpoint)
        """
        CREATE TABLE IF NOT EXISTS vector_search_health_history (
            endpoint_name  TEXT NOT NULL,
            status         TEXT NOT NULL,
            num_indexes    INT DEFAULT 0,
            recorded_at    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_vshh_ep ON vector_search_health_history (endpoint_name)",
        "CREATE INDEX IF NOT EXISTS idx_vshh_ts ON vector_search_health_history (recorded_at DESC)",
        """
        CREATE TABLE IF NOT EXISTS kb_billing_daily (
            usage_date          DATE NOT NULL,
            product             TEXT NOT NULL,
            workspace_id        TEXT NOT NULL,
            endpoint_name       TEXT DEFAULT '',
            workload_type       TEXT DEFAULT 'other',
            total_dbus          NUMERIC(18,4) DEFAULT 0,
            total_cost_usd      NUMERIC(18,4) DEFAULT 0,
            last_synced         TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (usage_date, product, workspace_id, endpoint_name, workload_type)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_kbd_product ON kb_billing_daily (product)",
        "CREATE INDEX IF NOT EXISTS idx_kbd_date ON kb_billing_daily (usage_date DESC)",
        """
        CREATE TABLE IF NOT EXISTS gateway_usage_daily (
            usage_date      DATE NOT NULL,
            endpoint_name   TEXT NOT NULL,
            requester       TEXT DEFAULT '',
            request_count   BIGINT DEFAULT 0,
            input_tokens    BIGINT DEFAULT 0,
            output_tokens   BIGINT DEFAULT 0,
            error_count     BIGINT DEFAULT 0,
            last_synced     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (usage_date, endpoint_name, requester)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_gud_date ON gateway_usage_daily (usage_date DESC)",
        "CREATE INDEX IF NOT EXISTS idx_gud_ep ON gateway_usage_daily (endpoint_name)",
        """
        CREATE TABLE IF NOT EXISTS gateway_usage_hourly (
            hour            TEXT NOT NULL,
            endpoint_name   TEXT DEFAULT '',
            request_count   BIGINT DEFAULT 0,
            input_tokens    BIGINT DEFAULT 0,
            output_tokens   BIGINT DEFAULT 0,
            error_count     BIGINT DEFAULT 0,
            last_synced     TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (hour, endpoint_name)
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS lakebase_instances (
            instance_name  TEXT PRIMARY KEY,
            instance_id    TEXT,
            state          TEXT,
            capacity       TEXT,
            pg_version     TEXT,
            read_write_dns TEXT,
            read_only_dns  TEXT,
            creator        TEXT,
            created_at     TEXT,
            last_synced    TIMESTAMP WITH TIME ZONE DEFAULT NOW()
        )
        """,
    ]
    for stmt in ddl_statements:
        try:
            execute_update(stmt)
        except Exception as exc:
            logger.warning("Vector search DDL warning: %s", exc)
    logger.info("Vector search cache tables ensured")


# ── Discovery (REST API → Lakebase) ──────────────────────────

def discover_vector_search() -> Dict[str, int]:
    """Discover vector search endpoints and indexes via REST API, cache in Lakebase."""
    import httpx

    w = _get_workspace_client()
    counts = {"endpoints": 0, "indexes": 0}

    # Fetch endpoints
    endpoints = []
    try:
        if w:
            resp = w.api_client.do("GET", "/api/2.0/vector-search/endpoints")
        else:
            base = get_databricks_host()
            r = httpx.get(f"{base}/api/2.0/vector-search/endpoints",
                          headers=get_databricks_headers(), timeout=_TIMEOUT)
            r.raise_for_status()
            resp = r.json()
        endpoints = resp.get("endpoints", [])
    except Exception as exc:
        logger.warning("Vector search endpoint discovery failed: %s", exc)

    # Upsert endpoints to Lakebase
    for ep in endpoints:
        try:
            execute_update(
                """INSERT INTO vector_search_endpoints
                   (endpoint_name, endpoint_id, status, endpoint_type, num_indexes, creator, created_at, last_synced)
                   VALUES (%s, %s, %s, %s, %s, %s, TO_TIMESTAMP(%s / 1000.0), NOW())
                   ON CONFLICT (endpoint_name) DO UPDATE SET
                       status = EXCLUDED.status, endpoint_type = EXCLUDED.endpoint_type,
                       num_indexes = EXCLUDED.num_indexes, last_synced = NOW()""",
                (
                    ep.get("name", ""),
                    ep.get("id", ""),
                    ep.get("endpoint_status", {}).get("state", "UNKNOWN"),
                    ep.get("endpoint_type", ""),
                    ep.get("num_indexes", 0),
                    ep.get("creator", ""),
                    ep.get("creation_timestamp", 0),
                ),
            )
            counts["endpoints"] += 1
        except Exception as exc:
            logger.warning("Vector search endpoint upsert failed for %s: %s", ep.get("name"), exc)

    # Fetch indexes per endpoint
    for ep in endpoints:
        ep_name = ep.get("name", "")
        if not ep_name:
            continue
        try:
            if w:
                idx_resp = w.api_client.do("GET", "/api/2.0/vector-search/indexes",
                                           query={"endpoint_name": ep_name})
            else:
                base = get_databricks_host()
                r = httpx.get(f"{base}/api/2.0/vector-search/indexes",
                              headers=get_databricks_headers(),
                              params={"endpoint_name": ep_name}, timeout=_TIMEOUT)
                r.raise_for_status()
                idx_resp = r.json()

            for idx in idx_resp.get("vector_indexes", []):
                idx_name = idx.get("name", "")
                # Fetch detailed index status
                detailed_state = ""
                indexed_row_count = 0
                ready = False
                status_message = ""
                source_table = ""
                embedding_model = ""
                pipeline_type = ""
                try:
                    if w and idx_name:
                        detail = w.api_client.do("GET", f"/api/2.0/vector-search/indexes/{idx_name}")
                        st = detail.get("status", {})
                        detailed_state = st.get("detailed_state", "")
                        indexed_row_count = st.get("indexed_row_count", 0) or 0
                        ready = bool(st.get("ready", False))
                        status_message = st.get("message", "")[:500]
                        ds = detail.get("delta_sync_index_spec", {})
                        source_table = ds.get("source_table", "")
                        emb_cols = ds.get("embedding_source_columns", [])
                        if emb_cols and isinstance(emb_cols, list) and len(emb_cols) > 0:
                            embedding_model = emb_cols[0].get("embedding_model_endpoint_name", "")
                        pipeline_type = ds.get("pipeline_type", "")
                except Exception as exc:
                    logger.warning("Index detail fetch failed for %s: %s", idx_name, exc)

                try:
                    execute_update(
                        """INSERT INTO vector_search_indexes
                           (index_name, endpoint_name, index_type, primary_key, creator,
                            detailed_state, indexed_row_count, ready, status_message,
                            source_table, embedding_model, pipeline_type, last_synced)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                           ON CONFLICT (endpoint_name, index_name) DO UPDATE SET
                               index_type = EXCLUDED.index_type, primary_key = EXCLUDED.primary_key,
                               creator = EXCLUDED.creator, detailed_state = EXCLUDED.detailed_state,
                               indexed_row_count = EXCLUDED.indexed_row_count, ready = EXCLUDED.ready,
                               status_message = EXCLUDED.status_message, source_table = EXCLUDED.source_table,
                               embedding_model = EXCLUDED.embedding_model, pipeline_type = EXCLUDED.pipeline_type,
                               last_synced = NOW()""",
                        (
                            idx_name, ep_name, idx.get("index_type", ""),
                            idx.get("primary_key", ""), idx.get("creator", ""),
                            detailed_state, indexed_row_count, ready, status_message,
                            source_table, embedding_model, pipeline_type,
                        ),
                    )
                    counts["indexes"] += 1
                except Exception as exc:
                    logger.warning("Vector search index upsert failed: %s", exc)
        except Exception as exc:
            logger.warning("Vector search index discovery failed for %s: %s", ep_name, exc)

    # Record health history snapshot
    for ep in endpoints:
        try:
            execute_update(
                "INSERT INTO vector_search_health_history (endpoint_name, status, num_indexes) VALUES (%s, %s, %s)",
                (ep.get("name", ""), ep.get("endpoint_status", {}).get("state", "UNKNOWN"), ep.get("num_indexes", 0)),
            )
        except Exception as exc:
            logger.warning("Health history insert failed: %s", exc)

    logger.info("Vector search discovery: %s endpoints, %s indexes", counts["endpoints"], counts["indexes"])
    return counts


# ── Cache reads ───────────────────────────────────────────────

def get_endpoints(allowed_workspace_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Return all vector search endpoints from cache, scoped to the caller's
    workspace access (see backend.utils.access_scope)."""
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    ws_filter = "WHERE workspace_id = ANY(%(ws)s)" if allowed_workspace_ids is not None else ""
    return execute_query(
        f"SELECT * FROM vector_search_endpoints {ws_filter} ORDER BY endpoint_name",
        {"ws": allowed_workspace_ids},
    )


def get_indexes(
    endpoint_name: Optional[str] = None,
    allowed_workspace_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Return vector search indexes, optionally filtered by endpoint and scoped
    to the caller's workspace access."""
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    clauses = []
    params: Dict[str, Any] = {}
    if endpoint_name:
        clauses.append("endpoint_name = %(ep)s")
        params["ep"] = endpoint_name
    if allowed_workspace_ids is not None:
        clauses.append("workspace_id = ANY(%(ws)s)")
        params["ws"] = allowed_workspace_ids
    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    return execute_query(
        f"SELECT * FROM vector_search_indexes {where} ORDER BY endpoint_name, index_name",
        params,
    )


def get_index_details(allowed_workspace_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Return all indexes with detailed sync status, scoped to the caller's
    workspace access."""
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    ws_filter = "WHERE i.workspace_id = ANY(%(ws)s)" if allowed_workspace_ids is not None else ""
    return execute_query(f"""
        SELECT i.*, e.status AS endpoint_status
        FROM vector_search_indexes i
        LEFT JOIN vector_search_endpoints e ON i.endpoint_name = e.endpoint_name
        {ws_filter}
        ORDER BY i.endpoint_name, i.index_name
    """, {"ws": allowed_workspace_ids})


def get_health_history(days: int = 7, allowed_workspace_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Return endpoint health snapshots for the last N days, scoped to the
    caller's workspace access.

    ``vector_search_health_history`` has no workspace_id column of its own —
    joins to ``vector_search_endpoints`` (which does) to filter.
    """
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    ws_join = (
        "JOIN vector_search_endpoints e ON e.endpoint_name = h.endpoint_name AND e.workspace_id = ANY(%(ws)s)"
        if allowed_workspace_ids is not None else ""
    )
    return execute_query(
        f"""SELECT h.endpoint_name, h.status, h.num_indexes, h.recorded_at
           FROM vector_search_health_history h
           {ws_join}
           WHERE h.recorded_at >= NOW() - INTERVAL '1 day' * %(days)s
           ORDER BY h.recorded_at DESC""",
        {"days": days, "ws": allowed_workspace_ids},
    )


def get_overview(allowed_workspace_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """Return KPI overview for vector search, scoped to the caller's
    workspace access."""
    empty = {"total_endpoints": 0, "online_endpoints": 0, "offline_endpoints": 0,
              "total_indexes": 0, "by_status": {}, "by_index_type": {}}
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return empty
    ws_filter_e = "WHERE workspace_id = ANY(%(ws)s)" if allowed_workspace_ids is not None else ""
    ws_filter_i = "WHERE workspace_id = ANY(%(ws)s)" if allowed_workspace_ids is not None else ""
    params = {"ws": allowed_workspace_ids}
    try:
        endpoints = execute_query(f"SELECT status, COUNT(*) as cnt FROM vector_search_endpoints {ws_filter_e} GROUP BY status", params)
        indexes = execute_query(f"SELECT index_type, COUNT(*) as cnt FROM vector_search_indexes {ws_filter_i} GROUP BY index_type", params)

        total_endpoints = sum(r["cnt"] for r in endpoints)
        online = sum(r["cnt"] for r in endpoints if r["status"] == "ONLINE")
        total_indexes = sum(r["cnt"] for r in indexes)

        return {
            "total_endpoints": total_endpoints,
            "online_endpoints": online,
            "offline_endpoints": total_endpoints - online,
            "total_indexes": total_indexes,
            "by_status": {r["status"]: r["cnt"] for r in endpoints},
            "by_index_type": {r["index_type"]: r["cnt"] for r in indexes},
        }
    except Exception as exc:
        logger.warning("Vector search overview failed: %s", exc)
        return empty


# ── Billing queries (system.billing.usage) ────────────────────

def _execute_billing_sql(sql: str) -> List[Dict[str, Any]]:
    """Execute SQL against system tables via SQL Statements API."""
    import time
    import httpx

    wh_id = find_serverless_warehouse_id()
    if not wh_id:
        logger.warning("No SQL warehouse found for vector search billing query")
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
    resp_json = None
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
            resp = httpx.post(f"{base}{path}", headers=get_databricks_headers(),
                              json=body, timeout=_TIMEOUT)
            resp.raise_for_status()
            resp_json = resp.json()
        except Exception as exc:
            logger.warning("httpx SQL exec failed: %s", exc)
            return []

    if not resp_json:
        return []

    status = resp_json.get("status", {}).get("state", "")
    statement_id = resp_json.get("statement_id", "")

    if status in ("PENDING", "RUNNING") and statement_id:
        poll_path = f"/api/2.0/sql/statements/{statement_id}"
        for _ in range(_SQL_POLL_MAX):
            time.sleep(_SQL_POLL_INTERVAL)
            try:
                if w:
                    resp_json = w.api_client.do("GET", poll_path)
                else:
                    r = httpx.get(f"{base}{poll_path}", headers=get_databricks_headers(), timeout=_TIMEOUT)
                    r.raise_for_status()
                    resp_json = r.json()
            except Exception:
                continue
            status = resp_json.get("status", {}).get("state", "")
            if status not in ("PENDING", "RUNNING"):
                break

    if status != "SUCCEEDED":
        err = resp_json.get("status", {}).get("error", {})
        logger.warning("Vector search billing SQL %s: %s", status, err.get("message", "")[:200])
        return []

    columns = [c["name"] for c in resp_json.get("manifest", {}).get("schema", {}).get("columns", [])]
    return [dict(zip(columns, row)) for row in resp_json.get("result", {}).get("data_array", [])]


def _kb_ws_clause(allowed_workspace_ids: Optional[List[str]], alias: str = "") -> str:
    """Shared ``AND workspace_id = ANY(%(ws)s)`` fragment for kb_billing_daily
    queries below — named param so it composes safely across CTEs that also
    bind %(days)s / %(limit)s."""
    if allowed_workspace_ids is None:
        return ""
    col = f"{alias}.workspace_id" if alias else "workspace_id"
    return f"AND {col} = ANY(%(ws)s)"


def get_cost_summary(days: int = 30, allowed_workspace_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """Total vector search cost for the last N days (from Lakebase cache)."""
    empty = {"total_dbus": 0, "total_cost_usd": 0, "endpoint_count": 0, "workspace_count": 0, "days": days}
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return empty
    ws = _kb_ws_clause(allowed_workspace_ids)
    rows = execute_query(
        f"""SELECT COALESCE(SUM(total_dbus), 0) AS total_dbus,
                  COALESCE(SUM(total_cost_usd), 0) AS total_cost_usd,
                  COUNT(DISTINCT NULLIF(endpoint_name, '')) AS endpoint_count,
                  COUNT(DISTINCT workspace_id) AS workspace_count
           FROM kb_billing_daily
           WHERE product = 'VECTOR_SEARCH'
             AND usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s {ws}""",
        {"days": days, "ws": allowed_workspace_ids},
    )
    if rows:
        r = rows[0]
        return {
            "total_dbus": float(r.get("total_dbus") or 0),
            "total_cost_usd": float(r.get("total_cost_usd") or 0),
            "endpoint_count": int(r.get("endpoint_count") or 0),
            "workspace_count": int(r.get("workspace_count") or 0),
            "days": days,
        }
    return empty


def get_cost_trend(days: int = 30, allowed_workspace_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Daily cost trend for vector search (from Lakebase cache)."""
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    ws = _kb_ws_clause(allowed_workspace_ids)
    return execute_query(
        f"""SELECT CAST(usage_date AS TEXT) AS usage_date,
                  SUM(total_dbus) AS total_dbus, SUM(total_cost_usd) AS total_cost_usd
           FROM kb_billing_daily WHERE product = 'VECTOR_SEARCH'
             AND usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s {ws}
           GROUP BY usage_date ORDER BY usage_date""",
        {"days": days, "ws": allowed_workspace_ids},
    )


def get_cost_by_endpoint(days: int = 30, allowed_workspace_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Cost breakdown per vector search endpoint (from cache)."""
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    ws = _kb_ws_clause(allowed_workspace_ids)
    return execute_query(
        f"""SELECT endpoint_name, workspace_id,
                  SUM(total_dbus) AS total_dbus, SUM(total_cost_usd) AS total_cost_usd
           FROM kb_billing_daily WHERE product = 'VECTOR_SEARCH'
             AND usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s {ws}
             AND endpoint_name != ''
           GROUP BY endpoint_name, workspace_id ORDER BY total_cost_usd DESC""",
        {"days": days, "ws": allowed_workspace_ids},
    )


def get_cost_by_workspace(days: int = 30, allowed_workspace_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Cost breakdown per workspace (from cache)."""
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    ws = _kb_ws_clause(allowed_workspace_ids)
    return execute_query(
        f"""SELECT workspace_id, SUM(total_dbus) AS total_dbus,
                  SUM(total_cost_usd) AS total_cost_usd,
                  COUNT(DISTINCT NULLIF(endpoint_name, '')) AS endpoint_count
           FROM kb_billing_daily WHERE product = 'VECTOR_SEARCH'
             AND usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s {ws}
           GROUP BY workspace_id ORDER BY total_cost_usd DESC""",
        {"days": days, "ws": allowed_workspace_ids},
    )


def get_cost_trend_by_workload(days: int = 30, allowed_workspace_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Daily cost trend broken down by workload type (from cache)."""
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    ws = _kb_ws_clause(allowed_workspace_ids)
    return execute_query(
        f"""SELECT CAST(usage_date AS TEXT) AS usage_date, workload_type,
                  SUM(total_dbus) AS total_units, SUM(total_cost_usd) AS total_cost_usd
           FROM kb_billing_daily WHERE product = 'VECTOR_SEARCH'
             AND usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s {ws}
           GROUP BY usage_date, workload_type ORDER BY usage_date, workload_type""",
        {"days": days, "ws": allowed_workspace_ids},
    )


def get_vs_top_workspaces_daily(
    days: int = 30, limit: int = 5, allowed_workspace_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Daily VS cost for top N workspaces."""
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    ws = _kb_ws_clause(allowed_workspace_ids)
    ws_b = _kb_ws_clause(allowed_workspace_ids, alias="b")
    return execute_query(
        f"""WITH top_ws AS (
              SELECT workspace_id FROM kb_billing_daily
              WHERE product = 'VECTOR_SEARCH' AND usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s {ws}
              GROUP BY workspace_id ORDER BY SUM(total_cost_usd) DESC LIMIT %(limit)s
           )
           SELECT CAST(b.usage_date AS TEXT) AS usage_date, b.workspace_id,
                  SUM(b.total_cost_usd) AS total_cost_usd
           FROM kb_billing_daily b JOIN top_ws ON b.workspace_id = top_ws.workspace_id
           WHERE b.product = 'VECTOR_SEARCH' AND b.usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s {ws_b}
           GROUP BY b.usage_date, b.workspace_id ORDER BY b.usage_date""",
        {"days": days, "limit": limit, "ws": allowed_workspace_ids},
    )


def get_cost_by_workload_type(days: int = 30, allowed_workspace_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Cost split by workload type (from cache)."""
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    ws = _kb_ws_clause(allowed_workspace_ids)
    return execute_query(
        f"""SELECT workload_type, SUM(total_dbus) AS total_units,
                  SUM(total_cost_usd) AS total_cost_usd
           FROM kb_billing_daily WHERE product = 'VECTOR_SEARCH'
             AND usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s {ws}
           GROUP BY workload_type ORDER BY total_cost_usd DESC""",
        {"days": days, "ws": allowed_workspace_ids},
    )


# ══════════════════════════════════════════════════════════════
# Lakebase Monitoring
# ══════════════════════════════════════════════════════════════

def get_lakebase_instances(allowed_workspace_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Return Lakebase instances from cache (populated by workflow).

    ``lakebase_instances`` has no workspace_id column (account-wide instance
    inventory) — suppressed entirely for a scoped (non-account-admin) caller,
    same reasoning as billing_service's cost_by_tag / external_model_spend.
    """
    if allowed_workspace_ids is not None:
        return []
    try:
        return execute_query("SELECT * FROM lakebase_instances ORDER BY instance_name")
    except Exception as exc:
        logger.warning("Lakebase instances cache read failed: %s", exc)
        return []


def _normalize_lb_project(p: Dict[str, Any]) -> Dict[str, Any]:
    """Map an Autoscaling `/postgres/projects` record into the Provisioned
    instance dict shape (so both API sources yield the same keys)."""
    st = p.get("status", {}) or {}
    lim = st.get("default_endpoint_settings", {}) or {}
    min_cu = lim.get("autoscaling_limit_min_cu")
    cap = ""
    if min_cu is not None:
        # max may be absent OR present-but-null — fall back to min in both cases
        # (a bare dict default only covers the missing-key case, so int(None) would raise).
        max_cu = lim.get("autoscaling_limit_max_cu") or min_cu
        cap = f"CU_{int(min_cu)}-{int(max_cu)}"
    return {
        # Use the UNIQUE project_id (not the free-form display_name) — this becomes
        # lakebase_instances.instance_name, a PRIMARY KEY, so a non-unique name would
        # let two backends overwrite each other's row.
        "name": p.get("project_id") or p.get("name", "").split("/")[-1] or st.get("display_name", ""),
        "uid": p.get("uid", ""),
        "state": "AVAILABLE",  # projects API has no simple state; presence ⇒ live
        "capacity": cap,
        "pg_version": f"PG_VERSION_{st.get('pg_version')}" if st.get("pg_version") else "",
        "read_write_dns": "",  # not in the projects list response (needs a branch/endpoint call)
        "read_only_dns": "",
        "creator": st.get("owner", ""),
        "creation_time": p.get("create_time", ""),
    }


def discover_lakebase_instances() -> List[Dict[str, Any]]:
    """Discover Lakebase backends via REST, across BOTH the Provisioned
    (``/database/instances``) and Autoscaling (``/postgres/projects``) APIs,
    merged + deduped by uid. Autoscaling-only workspaces expose nothing under
    the Provisioned API (gh #73); Provisioned entries are richer so they win on
    a uid collision. Used by the refresh button."""
    import httpx
    w = _get_workspace_client()

    def _get(path: str) -> Dict[str, Any]:
        if w:
            return w.api_client.do("GET", path)
        base = get_databricks_host()
        r = httpx.get(f"{base}{path}", headers=get_databricks_headers(), timeout=_TIMEOUT)
        r.raise_for_status()
        return r.json()

    out: List[Dict[str, Any]] = []
    seen: set = set()

    # Provisioned instances (preferred — richer fields).
    try:
        for inst in _get("/api/2.0/database/instances").get("database_instances", []):
            out.append(inst)
            if inst.get("uid"):
                seen.add(inst["uid"])
    except Exception as exc:
        logger.warning("Provisioned Lakebase discovery failed: %s", exc)

    # Autoscaling projects (normalized into the Provisioned dict shape).
    page = None
    try:
        for _ in range(50):  # hard page cap — defends against a non-advancing cursor
            path = "/api/2.0/postgres/projects" + (f"?page_token={quote(page, safe='')}" if page else "")
            resp = _get(path)
            projects = resp.get("projects", [])
            if not projects:
                break
            for p in projects:
                if p.get("uid") in seen:
                    continue  # same backend already listed as Provisioned
                try:
                    out.append(_normalize_lb_project(p))
                except Exception as exc:  # one malformed record must not drop the rest
                    logger.warning("Skipped a Lakebase project record: %s", exc)
                    continue
                if p.get("uid"):
                    seen.add(p["uid"])
            nxt = resp.get("next_page_token")
            if not nxt or nxt == page:  # cursor didn't advance → stop
                break
            page = nxt
    except Exception as exc:
        logger.warning("Autoscaling Lakebase discovery failed: %s", exc)

    return out


def get_lakebase_cost_summary(days: int = 30, allowed_workspace_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """Total Lakebase cost (from cache)."""
    empty = {"total_dbus": 0, "total_cost_usd": 0, "workspace_count": 0, "days": days}
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return empty
    ws = _kb_ws_clause(allowed_workspace_ids)
    rows = execute_query(
        f"""SELECT COALESCE(SUM(total_dbus), 0) AS total_dbus,
                  COALESCE(SUM(total_cost_usd), 0) AS total_cost_usd,
                  COUNT(DISTINCT workspace_id) AS workspace_count
           FROM kb_billing_daily
           WHERE product IN ('LAKEBASE', 'DATABASE')
             AND usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s {ws}""",
        {"days": days, "ws": allowed_workspace_ids},
    )
    if rows:
        r = rows[0]
        return {
            "total_dbus": float(r.get("total_dbus") or 0),
            "total_cost_usd": float(r.get("total_cost_usd") or 0),
            "workspace_count": int(r.get("workspace_count") or 0),
            "days": days,
        }
    return empty


def get_lakebase_cost_trend(days: int = 30, allowed_workspace_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Daily Lakebase cost trend (from cache)."""
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    ws = _kb_ws_clause(allowed_workspace_ids)
    return execute_query(
        f"""SELECT CAST(usage_date AS TEXT) AS usage_date,
                  SUM(total_dbus) AS total_dbus, SUM(total_cost_usd) AS total_cost_usd
           FROM kb_billing_daily WHERE product IN ('LAKEBASE', 'DATABASE')
             AND usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s {ws}
           GROUP BY usage_date ORDER BY usage_date""",
        {"days": days, "ws": allowed_workspace_ids},
    )


def get_lakebase_cost_by_workspace(days: int = 30, allowed_workspace_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Lakebase cost per workspace (from cache)."""
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    ws = _kb_ws_clause(allowed_workspace_ids)
    return execute_query(
        f"""SELECT workspace_id, SUM(total_dbus) AS total_dbus,
                  SUM(total_cost_usd) AS total_cost_usd
           FROM kb_billing_daily WHERE product IN ('LAKEBASE', 'DATABASE')
             AND usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s {ws}
           GROUP BY workspace_id ORDER BY total_cost_usd DESC""",
        {"days": days, "ws": allowed_workspace_ids},
    )


def get_lb_top_workspaces_daily(
    days: int = 30, limit: int = 5, allowed_workspace_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Daily Lakebase cost for top N workspaces."""
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    ws = _kb_ws_clause(allowed_workspace_ids)
    ws_b = _kb_ws_clause(allowed_workspace_ids, alias="b")
    return execute_query(
        f"""WITH top_ws AS (
              SELECT workspace_id FROM kb_billing_daily
              WHERE product IN ('LAKEBASE', 'DATABASE') AND usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s {ws}
              GROUP BY workspace_id ORDER BY SUM(total_cost_usd) DESC LIMIT %(limit)s
           )
           SELECT CAST(b.usage_date AS TEXT) AS usage_date, b.workspace_id,
                  SUM(b.total_cost_usd) AS total_cost_usd
           FROM kb_billing_daily b JOIN top_ws ON b.workspace_id = top_ws.workspace_id
           WHERE b.product IN ('LAKEBASE', 'DATABASE') AND b.usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s {ws_b}
           GROUP BY b.usage_date, b.workspace_id ORDER BY b.usage_date""",
        {"days": days, "limit": limit, "ws": allowed_workspace_ids},
    )


def get_lakebase_cost_by_type(days: int = 30, allowed_workspace_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Lakebase cost split: compute vs storage (from cache)."""
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    ws = _kb_ws_clause(allowed_workspace_ids)
    return execute_query(
        f"""SELECT workload_type AS cost_type, SUM(total_dbus) AS total_units,
                  SUM(total_cost_usd) AS total_cost_usd
           FROM kb_billing_daily WHERE product IN ('LAKEBASE', 'DATABASE')
             AND usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s {ws}
           GROUP BY workload_type ORDER BY total_cost_usd DESC""",
        {"days": days, "ws": allowed_workspace_ids},
    )


def get_combined_overview(days: int = 30, allowed_workspace_ids: Optional[List[str]] = None) -> Dict[str, Any]:
    """Combined overview for the Knowledge Bases page."""
    return {
        "vector_search": get_cost_summary(days, allowed_workspace_ids),
        "lakebase": get_lakebase_cost_summary(days, allowed_workspace_ids),
        "top_workspaces": get_combined_top_workspaces(days, allowed_workspace_ids=allowed_workspace_ids),
    }


def get_combined_top_workspaces(
    days: int = 30, limit: int = 20, allowed_workspace_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Top workspaces by combined VS + Lakebase spend."""
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    ws = _kb_ws_clause(allowed_workspace_ids)
    return execute_query(
        f"""SELECT workspace_id,
                  SUM(CASE WHEN product = 'VECTOR_SEARCH' THEN total_cost_usd ELSE 0 END) AS vs_cost,
                  SUM(CASE WHEN product IN ('LAKEBASE', 'DATABASE') THEN total_cost_usd ELSE 0 END) AS lb_cost,
                  SUM(total_cost_usd) AS total_cost
           FROM kb_billing_daily
           WHERE usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s {ws}
           GROUP BY workspace_id ORDER BY total_cost DESC LIMIT %(limit)s""",
        {"days": days, "limit": limit, "ws": allowed_workspace_ids},
    )


def get_top_workspaces_daily_trend(
    days: int = 30, limit: int = 5, allowed_workspace_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Daily cost trend for the top N workspaces (for line chart)."""
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    ws = _kb_ws_clause(allowed_workspace_ids)
    ws_b = _kb_ws_clause(allowed_workspace_ids, alias="b")
    return execute_query(
        f"""WITH top_ws AS (
              SELECT workspace_id FROM kb_billing_daily
              WHERE usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s {ws}
              GROUP BY workspace_id ORDER BY SUM(total_cost_usd) DESC LIMIT %(limit)s
           )
           SELECT CAST(b.usage_date AS TEXT) AS usage_date,
                  b.workspace_id,
                  SUM(b.total_cost_usd) AS total_cost_usd
           FROM kb_billing_daily b
           JOIN top_ws ON b.workspace_id = top_ws.workspace_id
           WHERE b.usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s {ws_b}
           GROUP BY b.usage_date, b.workspace_id
           ORDER BY b.usage_date""",
        {"days": days, "limit": limit, "ws": allowed_workspace_ids},
    )


def get_combined_cost_trend(days: int = 30, allowed_workspace_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Daily cost trend for both products (from cache)."""
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    ws = _kb_ws_clause(allowed_workspace_ids)
    return execute_query(
        f"""SELECT CAST(usage_date AS TEXT) AS usage_date, product,
                  SUM(total_cost_usd) AS total_cost_usd
           FROM kb_billing_daily
           WHERE usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s {ws}
           GROUP BY usage_date, product ORDER BY usage_date""",
        {"days": days, "ws": allowed_workspace_ids},
    )


# ══════════════════════════════════════════════════════════════
# Retrieval & Memory — Vector Search index usage + Lakebase health
# (Delta → Lakebase mirrors written by 03/02; see workflows/03 "Retrieval & Memory")
#
# Every read here fails OPEN: a table that isn't synced yet (or a read error)
# returns empty, never a 500. Scoping follows the rest of this module —
# all five tables carry workspace_id, so a scoped caller gets rows for their
# workspaces only; an empty allow-list short-circuits without querying.
# ══════════════════════════════════════════════════════════════

_KA_INDEX_PREFIX = "__databricks_internal_catalog_tiles_"


def classify_managed_by(endpoint_name: Optional[str], index_name: Optional[str]) -> Optional[str]:
    """Label indexes owned by Agent Bricks.

    Knowledge Assistant indexes live under the internal tiles catalog
    (``__databricks_internal_catalog_tiles_<...>.ka_<...>_index``) and sit on
    ``ka-*`` endpoints. Multi-agent supervisors sit on ``mas-*`` endpoints.
    Anything else is user-managed (None).
    """
    ep = (endpoint_name or "").lower()
    name = index_name or ""
    if ep.startswith("ka-") or (name.startswith(_KA_INDEX_PREFIX) and ".ka_" in name):
        return "knowledge_assistant"
    if ep.startswith("mas-"):
        return "supervisor"
    return None


def _ws_pred(allowed_workspace_ids: Optional[List[str]], alias: str = "") -> Optional[str]:
    """``workspace_id = ANY(%(ws)s)`` predicate, or None when unrestricted."""
    if allowed_workspace_ids is None:
        return None
    col = f"{alias}.workspace_id" if alias else "workspace_id"
    return f"{col} = ANY(%(ws)s)"


def _where(clauses: List[Optional[str]]) -> str:
    live = [c for c in clauses if c]
    return ("WHERE " + " AND ".join(live)) if live else ""


def _as_int(v: Any) -> int:
    try:
        return int(v or 0)
    except (TypeError, ValueError):
        return 0


def _as_float(v: Any) -> float:
    try:
        return float(v or 0)
    except (TypeError, ValueError):
        return 0.0


def _safe_query(sql: str, params: Dict[str, Any], label: str) -> List[Dict[str, Any]]:
    """execute_query that fails open: a missing/unsynced table yields []."""
    try:
        return execute_query(sql, params)
    except Exception as exc:
        logger.warning("%s cache read failed (table may not be synced yet): %s", label, exc)
        return []


def get_vs_usage_trend(days: int = 30, allowed_workspace_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Daily Vector Search data-plane requests + errors, split by action."""
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    where = _where([
        "usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s",
        _ws_pred(allowed_workspace_ids),
    ])
    rows = _safe_query(
        f"""SELECT CAST(usage_date AS TEXT) AS usage_date, action,
                   SUM(request_count) AS request_count, SUM(error_count) AS error_count
            FROM vs_index_usage_daily {where}
            GROUP BY usage_date, action ORDER BY usage_date, action""",
        {"days": days, "ws": allowed_workspace_ids},
        "vs_index_usage_daily",
    )
    return [
        {"usage_date": r["usage_date"], "action": r["action"],
         "request_count": _as_int(r.get("request_count")), "error_count": _as_int(r.get("error_count"))}
        for r in rows
    ]


def get_vs_index_usage(days: int = 30, allowed_workspace_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Per-index usage summary over the window, with the owning endpoint (from the
    Vector Search cache) and an Agent Bricks ``managed_by`` label.

    ``query_count`` / ``peak_daily_callers`` are read-path metrics
    (queryVectorIndex). ``peak_daily_callers`` is the highest distinct-caller count
    seen on any single day — a daily peak, not a window-wide distinct count.
    """
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    where = _where(["usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s",
                    _ws_pred(allowed_workspace_ids)])
    rows = _safe_query(
        f"""WITH u AS (
               SELECT workspace_id, index_name,
                      SUM(CASE WHEN action = 'queryVectorIndex' THEN request_count ELSE 0 END) AS query_count,
                      SUM(CASE WHEN action = 'scanVectorIndex' THEN request_count ELSE 0 END) AS scan_count,
                      SUM(CASE WHEN action IN ('upsertDataVectorIndex', 'deleteDataVectorIndex')
                               THEN request_count ELSE 0 END) AS write_count,
                      SUM(error_count) AS error_count,
                      MAX(CASE WHEN action = 'queryVectorIndex' THEN distinct_callers ELSE 0 END) AS peak_daily_callers,
                      CAST(MAX(usage_date) AS TEXT) AS last_active_date
               FROM vs_index_usage_daily {where}
               GROUP BY workspace_id, index_name
            ),
            ep AS (
               SELECT index_name, MAX(endpoint_name) AS endpoint_name
               FROM vector_search_indexes GROUP BY index_name
            )
            SELECT u.*, ep.endpoint_name
            FROM u LEFT JOIN ep ON ep.index_name = u.index_name
            ORDER BY u.query_count DESC, u.index_name""",
        {"days": days, "ws": allowed_workspace_ids},
        "vs_index_usage_daily",
    )
    out = []
    for r in rows:
        name = r.get("index_name") or ""
        ep_name = r.get("endpoint_name") or ""
        out.append({
            "workspace_id": r.get("workspace_id") or "",
            "index_name": name,
            "endpoint_name": ep_name,
            "managed_by": classify_managed_by(ep_name, name),
            "query_count": _as_int(r.get("query_count")),
            "scan_count": _as_int(r.get("scan_count")),
            "write_count": _as_int(r.get("write_count")),
            "error_count": _as_int(r.get("error_count")),
            "peak_daily_callers": _as_int(r.get("peak_daily_callers")),
            "last_active_date": r.get("last_active_date"),
        })
    return out


def get_vs_index_callers(
    index_name: str,
    workspace_id: Optional[str] = None,
    allowed_workspace_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Top callers of one index over the usage window (drilldown).

    The callers table has no date column — its window is the workflow's usage
    window (30d), so this endpoint takes no ``days`` argument.
    """
    if not index_name:
        return []
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    params: Dict[str, Any] = {"idx": index_name, "ws": allowed_workspace_ids}
    clauses: List[Optional[str]] = ["index_name = %(idx)s", _ws_pred(allowed_workspace_ids)]
    if workspace_id:
        clauses.append("workspace_id = %(wsid)s")
        params["wsid"] = workspace_id
    rows = _safe_query(
        f"""SELECT caller, SUM(request_count) AS request_count, SUM(error_count) AS error_count,
                   MAX(last_seen) AS last_seen
            FROM vs_index_callers {_where(clauses)}
            GROUP BY caller ORDER BY request_count DESC, caller LIMIT 50""",
        params,
        "vs_index_callers",
    )
    return [
        {"caller": r.get("caller") or "unknown", "request_count": _as_int(r.get("request_count")),
         "error_count": _as_int(r.get("error_count")), "last_seen": r.get("last_seen")}
        for r in rows
    ]


def get_lakebase_health(days: int = 14, allowed_workspace_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Daily Lakebase connections and database size per project/branch."""
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    where = _where(["usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s",
                    _ws_pred(allowed_workspace_ids)])
    rows = _safe_query(
        f"""SELECT CAST(usage_date AS TEXT) AS usage_date, project_id, branch_id,
                   MAX(project_name) AS project_name, MAX(branch_name) AS branch_name,
                   SUM(avg_connections) AS avg_connections,
                   MAX(max_connections) AS max_connections,
                   MAX(max_db_size_bytes) AS max_db_size_bytes
            FROM lakebase_health_daily {where}
            GROUP BY usage_date, project_id, branch_id
            ORDER BY usage_date, project_name""",
        {"days": days, "ws": allowed_workspace_ids},
        "lakebase_health_daily",
    )
    return [
        {"usage_date": r["usage_date"], "project_id": r.get("project_id") or "",
         "project_name": r.get("project_name") or r.get("project_id") or "",
         "branch_id": r.get("branch_id") or "", "branch_name": r.get("branch_name") or "",
         "avg_connections": round(_as_float(r.get("avg_connections")), 2),
         "max_connections": _as_int(r.get("max_connections")),
         "max_db_size_bytes": _as_int(r.get("max_db_size_bytes"))}
        for r in rows
    ]


def get_lakebase_wait_events(
    limit: int = 25, allowed_workspace_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Top Lakebase wait events by total wait time over the workflow window."""
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    where = _where([_ws_pred(allowed_workspace_ids)])
    rows = _safe_query(
        f"""SELECT wait_class_name, wait_event_name, project_name, branch_name,
                   SUM(wait_event_count) AS wait_event_count,
                   SUM(wait_event_time) AS wait_event_time
            FROM lakebase_wait_events {where}
            GROUP BY wait_class_name, wait_event_name, project_name, branch_name
            ORDER BY wait_event_time DESC LIMIT %(limit)s""",
        {"limit": limit, "ws": allowed_workspace_ids},
        "lakebase_wait_events",
    )
    return [
        {"wait_class_name": r.get("wait_class_name") or "unknown",
         "wait_event_name": r.get("wait_event_name") or "unknown",
         "project_name": r.get("project_name") or "", "branch_name": r.get("branch_name") or "",
         "wait_event_count": _as_int(r.get("wait_event_count")),
         "wait_event_time": round(_as_float(r.get("wait_event_time")), 2)}
        for r in rows
    ]


def get_lakebase_cost_by_project(days: int = 30, allowed_workspace_ids: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    """Lakebase $ per project/branch over the window, with names from the health cache."""
    if allowed_workspace_ids is not None and not allowed_workspace_ids:
        return []
    ws_c = _ws_pred(allowed_workspace_ids, alias="c")
    ws_n = _ws_pred(allowed_workspace_ids)
    rows = _safe_query(
        f"""WITH names AS (
               SELECT project_id, branch_id,
                      MAX(project_name) AS project_name, MAX(branch_name) AS branch_name
               FROM lakebase_health_daily {_where([ws_n])}
               GROUP BY project_id, branch_id
            )
            SELECT c.project_id, c.branch_id,
                   MAX(n.project_name) AS project_name, MAX(n.branch_name) AS branch_name,
                   SUM(c.total_dbus) AS total_dbus, SUM(c.total_cost_usd) AS total_cost_usd
            FROM lakebase_cost_by_project c
            LEFT JOIN names n ON n.project_id = c.project_id AND n.branch_id = c.branch_id
            {_where(["c.usage_date >= CURRENT_DATE - INTERVAL '1 day' * %(days)s", ws_c])}
            GROUP BY c.project_id, c.branch_id
            ORDER BY total_cost_usd DESC LIMIT 50""",
        {"days": days, "ws": allowed_workspace_ids},
        "lakebase_cost_by_project",
    )
    return [
        {"project_id": r.get("project_id") or "", "branch_id": r.get("branch_id") or "",
         "project_name": r.get("project_name") or r.get("project_id") or "",
         "branch_name": r.get("branch_name") or "",
         "total_dbus": round(_as_float(r.get("total_dbus")), 4),
         "total_cost_usd": round(_as_float(r.get("total_cost_usd")), 4)}
        for r in rows
    ]
