"""Service for Vector Search monitoring — discovery, caching, and billing."""
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
            last_synced    TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (endpoint_name, index_name)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_vsi_endpoint ON vector_search_indexes (endpoint_name)",
        "CREATE INDEX IF NOT EXISTS idx_vsi_type ON vector_search_indexes (index_type)",
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
                try:
                    execute_update(
                        """INSERT INTO vector_search_indexes
                           (index_name, endpoint_name, index_type, primary_key, creator, last_synced)
                           VALUES (%s, %s, %s, %s, %s, NOW())
                           ON CONFLICT (endpoint_name, index_name) DO UPDATE SET
                               index_type = EXCLUDED.index_type, primary_key = EXCLUDED.primary_key,
                               creator = EXCLUDED.creator, last_synced = NOW()""",
                        (
                            idx.get("name", ""),
                            ep_name,
                            idx.get("index_type", ""),
                            idx.get("primary_key", ""),
                            idx.get("creator", ""),
                        ),
                    )
                    counts["indexes"] += 1
                except Exception as exc:
                    logger.warning("Vector search index upsert failed: %s", exc)
        except Exception as exc:
            logger.warning("Vector search index discovery failed for %s: %s", ep_name, exc)

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
    """Total vector search cost for the last N days."""
    rows = _execute_billing_sql(f"""
        SELECT
            ROUND(SUM(u.usage_quantity), 4) AS total_dbus,
            ROUND(SUM(u.usage_quantity *
                COALESCE(lp.pricing.effective_list.default, lp.pricing.default, 0.07)
            ), 4) AS total_cost_usd,
            COUNT(DISTINCT u.usage_metadata.endpoint_name) AS endpoint_count,
            COUNT(DISTINCT u.workspace_id) AS workspace_count
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices lp
            ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
            AND u.usage_unit = lp.usage_unit AND lp.price_end_time IS NULL
        WHERE u.billing_origin_product = 'VECTOR_SEARCH'
          AND u.usage_date >= current_date() - INTERVAL {int(days)} DAYS
    """)
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
    """Daily cost trend for vector search."""
    return _execute_billing_sql(f"""
        SELECT
            CAST(u.usage_date AS STRING) AS usage_date,
            ROUND(SUM(u.usage_quantity), 4) AS total_dbus,
            ROUND(SUM(u.usage_quantity *
                COALESCE(lp.pricing.effective_list.default, lp.pricing.default, 0.07)
            ), 4) AS total_cost_usd
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices lp
            ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
            AND u.usage_unit = lp.usage_unit AND lp.price_end_time IS NULL
        WHERE u.billing_origin_product = 'VECTOR_SEARCH'
          AND u.usage_date >= current_date() - INTERVAL {int(days)} DAYS
        GROUP BY u.usage_date
        ORDER BY u.usage_date
    """)


def get_cost_by_endpoint(days: int = 30) -> List[Dict[str, Any]]:
    """Cost breakdown per vector search endpoint."""
    return _execute_billing_sql(f"""
        SELECT
            u.usage_metadata.endpoint_name AS endpoint_name,
            CAST(u.workspace_id AS STRING) AS workspace_id,
            ROUND(SUM(u.usage_quantity), 4) AS total_dbus,
            ROUND(SUM(u.usage_quantity *
                COALESCE(lp.pricing.effective_list.default, lp.pricing.default, 0.07)
            ), 4) AS total_cost_usd
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices lp
            ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
            AND u.usage_unit = lp.usage_unit AND lp.price_end_time IS NULL
        WHERE u.billing_origin_product = 'VECTOR_SEARCH'
          AND u.usage_date >= current_date() - INTERVAL {int(days)} DAYS
          AND u.usage_metadata.endpoint_name IS NOT NULL
        GROUP BY u.usage_metadata.endpoint_name, u.workspace_id
        ORDER BY total_cost_usd DESC
    """)


def get_cost_by_workspace(days: int = 30) -> List[Dict[str, Any]]:
    """Cost breakdown per workspace."""
    return _execute_billing_sql(f"""
        SELECT
            CAST(u.workspace_id AS STRING) AS workspace_id,
            ROUND(SUM(u.usage_quantity), 4) AS total_dbus,
            ROUND(SUM(u.usage_quantity *
                COALESCE(lp.pricing.effective_list.default, lp.pricing.default, 0.07)
            ), 4) AS total_cost_usd,
            COUNT(DISTINCT u.usage_metadata.endpoint_name) AS endpoint_count
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices lp
            ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
            AND u.usage_unit = lp.usage_unit AND lp.price_end_time IS NULL
        WHERE u.billing_origin_product = 'VECTOR_SEARCH'
          AND u.usage_date >= current_date() - INTERVAL {int(days)} DAYS
        GROUP BY u.workspace_id
        ORDER BY total_cost_usd DESC
    """)


def get_cost_by_workload_type(days: int = 30) -> List[Dict[str, Any]]:
    """Cost split by workload type: ingest, serving, storage."""
    return _execute_billing_sql(f"""
        SELECT
            CASE
                WHEN u.sku_name LIKE '%STORAGE%' THEN 'storage'
                WHEN u.sku_name LIKE '%INFERENCE%' OR u.sku_name LIKE '%SERVING%' THEN 'serving'
                ELSE 'ingest'
            END AS workload_type,
            ROUND(SUM(u.usage_quantity), 4) AS total_units,
            ROUND(SUM(u.usage_quantity *
                COALESCE(lp.pricing.effective_list.default, lp.pricing.default, 0.07)
            ), 4) AS total_cost_usd
        FROM system.billing.usage u
        LEFT JOIN system.billing.list_prices lp
            ON u.sku_name = lp.sku_name AND u.cloud = lp.cloud
            AND u.usage_unit = lp.usage_unit AND lp.price_end_time IS NULL
        WHERE u.billing_origin_product = 'VECTOR_SEARCH'
          AND u.usage_date >= current_date() - INTERVAL {int(days)} DAYS
        GROUP BY workload_type
        ORDER BY total_cost_usd DESC
    """)
