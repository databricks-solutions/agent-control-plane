"""Service for Knowledge Bases monitoring — Vector Search + Lakebase discovery, caching, and billing."""
import logging
import json as _json
from typing import List, Dict, Any, Optional

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

def get_endpoints() -> List[Dict[str, Any]]:
    """Return all vector search endpoints from cache."""
    return execute_query("SELECT * FROM vector_search_endpoints ORDER BY endpoint_name")


def get_indexes(endpoint_name: Optional[str] = None) -> List[Dict[str, Any]]:
    """Return vector search indexes, optionally filtered by endpoint."""
    if endpoint_name:
        return execute_query(
            "SELECT * FROM vector_search_indexes WHERE endpoint_name = %s ORDER BY index_name",
            (endpoint_name,),
        )
    return execute_query("SELECT * FROM vector_search_indexes ORDER BY endpoint_name, index_name")


def get_index_details() -> List[Dict[str, Any]]:
    """Return all indexes with detailed sync status."""
    return execute_query("""
        SELECT i.*, e.status AS endpoint_status
        FROM vector_search_indexes i
        LEFT JOIN vector_search_endpoints e ON i.endpoint_name = e.endpoint_name
        ORDER BY i.endpoint_name, i.index_name
    """)


def get_health_history(days: int = 7) -> List[Dict[str, Any]]:
    """Return endpoint health snapshots for the last N days."""
    return execute_query(
        """SELECT endpoint_name, status, num_indexes, recorded_at
           FROM vector_search_health_history
           WHERE recorded_at >= NOW() - INTERVAL '%s days'
           ORDER BY recorded_at DESC""",
        (days,),
    )


def get_overview() -> Dict[str, Any]:
    """Return KPI overview for vector search."""
    try:
        endpoints = execute_query("SELECT status, COUNT(*) as cnt FROM vector_search_endpoints GROUP BY status")
        indexes = execute_query("SELECT index_type, COUNT(*) as cnt FROM vector_search_indexes GROUP BY index_type")

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
        return {"total_endpoints": 0, "online_endpoints": 0, "offline_endpoints": 0,
                "total_indexes": 0, "by_status": {}, "by_index_type": {}}


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


def get_cost_summary(days: int = 30) -> Dict[str, Any]:
    """Total vector search cost for the last N days (from Lakebase cache)."""
    rows = execute_query(
        """SELECT COALESCE(SUM(total_dbus), 0) AS total_dbus,
                  COALESCE(SUM(total_cost_usd), 0) AS total_cost_usd,
                  COUNT(DISTINCT NULLIF(endpoint_name, '')) AS endpoint_count,
                  COUNT(DISTINCT workspace_id) AS workspace_count
           FROM kb_billing_daily
           WHERE product = 'VECTOR_SEARCH'
             AND usage_date >= CURRENT_DATE - INTERVAL '%s days'""",
        (days,),
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
    return {"total_dbus": 0, "total_cost_usd": 0, "endpoint_count": 0, "workspace_count": 0, "days": days}


def get_cost_trend(days: int = 30) -> List[Dict[str, Any]]:
    """Daily cost trend for vector search (from Lakebase cache)."""
    return execute_query(
        """SELECT CAST(usage_date AS TEXT) AS usage_date,
                  SUM(total_dbus) AS total_dbus, SUM(total_cost_usd) AS total_cost_usd
           FROM kb_billing_daily WHERE product = 'VECTOR_SEARCH'
             AND usage_date >= CURRENT_DATE - INTERVAL '%s days'
           GROUP BY usage_date ORDER BY usage_date""",
        (days,),
    )


def get_cost_by_endpoint(days: int = 30) -> List[Dict[str, Any]]:
    """Cost breakdown per vector search endpoint (from cache)."""
    return execute_query(
        """SELECT endpoint_name, workspace_id,
                  SUM(total_dbus) AS total_dbus, SUM(total_cost_usd) AS total_cost_usd
           FROM kb_billing_daily WHERE product = 'VECTOR_SEARCH'
             AND usage_date >= CURRENT_DATE - INTERVAL '%s days'
             AND endpoint_name != ''
           GROUP BY endpoint_name, workspace_id ORDER BY total_cost_usd DESC""",
        (days,),
    )


def get_cost_by_workspace(days: int = 30) -> List[Dict[str, Any]]:
    """Cost breakdown per workspace (from cache)."""
    return execute_query(
        """SELECT workspace_id, SUM(total_dbus) AS total_dbus,
                  SUM(total_cost_usd) AS total_cost_usd,
                  COUNT(DISTINCT NULLIF(endpoint_name, '')) AS endpoint_count
           FROM kb_billing_daily WHERE product = 'VECTOR_SEARCH'
             AND usage_date >= CURRENT_DATE - INTERVAL '%s days'
           GROUP BY workspace_id ORDER BY total_cost_usd DESC""",
        (days,),
    )


def get_cost_trend_by_workload(days: int = 30) -> List[Dict[str, Any]]:
    """Daily cost trend broken down by workload type (from cache)."""
    return execute_query(
        """SELECT CAST(usage_date AS TEXT) AS usage_date, workload_type,
                  SUM(total_dbus) AS total_units, SUM(total_cost_usd) AS total_cost_usd
           FROM kb_billing_daily WHERE product = 'VECTOR_SEARCH'
             AND usage_date >= CURRENT_DATE - INTERVAL '%s days'
           GROUP BY usage_date, workload_type ORDER BY usage_date, workload_type""",
        (days,),
    )


def get_cost_by_workload_type(days: int = 30) -> List[Dict[str, Any]]:
    """Cost split by workload type (from cache)."""
    return execute_query(
        """SELECT workload_type, SUM(total_dbus) AS total_units,
                  SUM(total_cost_usd) AS total_cost_usd
           FROM kb_billing_daily WHERE product = 'VECTOR_SEARCH'
             AND usage_date >= CURRENT_DATE - INTERVAL '%s days'
           GROUP BY workload_type ORDER BY total_cost_usd DESC""",
        (days,),
    )


# ══════════════════════════════════════════════════════════════
# Lakebase Monitoring
# ══════════════════════════════════════════════════════════════

def get_lakebase_instances() -> List[Dict[str, Any]]:
    """Return Lakebase instances from cache (populated by workflow)."""
    try:
        return execute_query("SELECT * FROM lakebase_instances ORDER BY instance_name")
    except Exception as exc:
        logger.warning("Lakebase instances cache read failed: %s", exc)
        return []


def discover_lakebase_instances() -> List[Dict[str, Any]]:
    """Discover Lakebase instances via REST API (used by refresh button)."""
    import httpx
    w = _get_workspace_client()
    try:
        if w:
            resp = w.api_client.do("GET", "/api/2.0/database/instances")
        else:
            base = get_databricks_host()
            r = httpx.get(f"{base}/api/2.0/database/instances",
                          headers=get_databricks_headers(), timeout=_TIMEOUT)
            r.raise_for_status()
            resp = r.json()
        return resp.get("database_instances", [])
    except Exception as exc:
        logger.warning("Lakebase instance discovery failed: %s", exc)
        return []


def get_lakebase_cost_summary(days: int = 30) -> Dict[str, Any]:
    """Total Lakebase cost (from cache)."""
    rows = execute_query(
        """SELECT COALESCE(SUM(total_dbus), 0) AS total_dbus,
                  COALESCE(SUM(total_cost_usd), 0) AS total_cost_usd,
                  COUNT(DISTINCT workspace_id) AS workspace_count
           FROM kb_billing_daily
           WHERE product IN ('LAKEBASE', 'DATABASE')
             AND usage_date >= CURRENT_DATE - INTERVAL '%s days'""",
        (days,),
    )
    if rows:
        r = rows[0]
        return {
            "total_dbus": float(r.get("total_dbus") or 0),
            "total_cost_usd": float(r.get("total_cost_usd") or 0),
            "workspace_count": int(r.get("workspace_count") or 0),
            "days": days,
        }
    return {"total_dbus": 0, "total_cost_usd": 0, "workspace_count": 0, "days": days}


def get_lakebase_cost_trend(days: int = 30) -> List[Dict[str, Any]]:
    """Daily Lakebase cost trend (from cache)."""
    return execute_query(
        """SELECT CAST(usage_date AS TEXT) AS usage_date,
                  SUM(total_dbus) AS total_dbus, SUM(total_cost_usd) AS total_cost_usd
           FROM kb_billing_daily WHERE product IN ('LAKEBASE', 'DATABASE')
             AND usage_date >= CURRENT_DATE - INTERVAL '%s days'
           GROUP BY usage_date ORDER BY usage_date""",
        (days,),
    )


def get_lakebase_cost_by_workspace(days: int = 30) -> List[Dict[str, Any]]:
    """Lakebase cost per workspace (from cache)."""
    return execute_query(
        """SELECT workspace_id, SUM(total_dbus) AS total_dbus,
                  SUM(total_cost_usd) AS total_cost_usd
           FROM kb_billing_daily WHERE product IN ('LAKEBASE', 'DATABASE')
             AND usage_date >= CURRENT_DATE - INTERVAL '%s days'
           GROUP BY workspace_id ORDER BY total_cost_usd DESC""",
        (days,),
    )


def get_lakebase_cost_by_type(days: int = 30) -> List[Dict[str, Any]]:
    """Lakebase cost split: compute vs storage (from cache)."""
    return execute_query(
        """SELECT workload_type AS cost_type, SUM(total_dbus) AS total_units,
                  SUM(total_cost_usd) AS total_cost_usd
           FROM kb_billing_daily WHERE product IN ('LAKEBASE', 'DATABASE')
             AND usage_date >= CURRENT_DATE - INTERVAL '%s days'
           GROUP BY workload_type ORDER BY total_cost_usd DESC""",
        (days,),
    )


def get_combined_overview(days: int = 30) -> Dict[str, Any]:
    """Combined overview for the Knowledge Bases page."""
    return {
        "vector_search": get_cost_summary(days),
        "lakebase": get_lakebase_cost_summary(days),
    }


def get_combined_cost_trend(days: int = 30) -> List[Dict[str, Any]]:
    """Daily cost trend for both products (from cache)."""
    return execute_query(
        """SELECT CAST(usage_date AS TEXT) AS usage_date, product,
                  SUM(total_cost_usd) AS total_cost_usd
           FROM kb_billing_daily
           WHERE usage_date >= CURRENT_DATE - INTERVAL '%s days'
           GROUP BY usage_date, product ORDER BY usage_date""",
        (days,),
    )
