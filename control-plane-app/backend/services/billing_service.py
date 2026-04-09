"""Billing / cost data cached in Lakebase, refreshed from Databricks system tables.

Architecture:
  • Five Lakebase tables hold daily-grain billing data:
      billing_serving_daily        – model-serving costs by endpoint/SKU/workspace
      billing_token_daily          – token usage by endpoint/workspace
      billing_product_daily        – all-product costs by workspace
      billing_serving_user_daily   – model-serving costs by user/workspace
      billing_token_user_daily     – token usage by user/workspace
  • A metadata table (billing_cache_meta) tracks last-refresh timestamps.
  • On first request (or when data is >24 h stale) we trigger a background
    refresh that queries the system tables via the SQL Statement Execution API
    and upserts results into Lakebase.
  • All read queries hit Lakebase (< 100 ms) instead of system tables (~10+ s).
"""
from __future__ import annotations

import threading
import httpx
from datetime import datetime, timezone
from typing import List, Dict, Any, Optional

from backend.config import (
    _get_workspace_client,
    get_databricks_host,
    get_databricks_headers,
    find_serverless_warehouse_id,
)
from backend.database import (
    execute_query,
    execute_one,
    execute_update,
    DatabasePool,
)

import logging

logger = logging.getLogger(__name__)

_TIMEOUT = 120.0  # system-table queries can be slow
_SQL_POLL_INTERVAL = 3  # seconds between polls for async statements
_SQL_POLL_MAX = 40  # max polls (~2 min total)
_STALE_SECONDS = 24 * 3600  # 24 hours
_REFRESH_DAYS = 90  # how many days of history to load

# ── lock to prevent concurrent refreshes ─────────────────────────
_refresh_lock = threading.Lock()
_refresh_in_progress = False


# =====================================================================
# LOW-LEVEL: execute SQL on Databricks SQL warehouse
# =====================================================================

def _find_warehouse_id() -> Optional[str]:
    """Find the best SQL warehouse (prefers serverless)."""
    return find_serverless_warehouse_id()


def _execute_system_sql(sql: str, warehouse_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Execute SQL against Databricks system tables.

    Handles async execution: if the initial request returns PENDING or
    RUNNING, polls the statement status until it completes or times out.
    """
    import time

    wh_id = warehouse_id or _find_warehouse_id()
    if not wh_id:
        logger.warning("No SQL warehouse found")
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
            logger.warning("Statement %s timed out after %ss of polling", statement_id, _SQL_POLL_MAX * _SQL_POLL_INTERVAL)
            return []

    if status != "SUCCEEDED":
        logger.warning("SQL status: %s", status)
        err = resp_json.get("status", {}).get("error", {})
        if err:
            logger.info("    Error: %s", err.get('message', ''))
        return []

    manifest = resp_json.get("manifest", {})
    columns = [c["name"] for c in manifest.get("schema", {}).get("columns", [])]
    data_array = resp_json.get("result", {}).get("data_array", [])
    return [dict(zip(columns, row)) for row in data_array]


# =====================================================================
# DDL: ensure Lakebase cache tables exist
# =====================================================================

def ensure_billing_tables():
    """Create the billing cache tables in Lakebase if they don't already exist.

    Called once at app startup so the read-path queries always have a valid
    target, even before the first refresh completes.
    """
    ddl_statements = [
        # Daily model-serving cost data (from system.billing.usage)
        """
        CREATE TABLE IF NOT EXISTS billing_serving_daily (
            usage_date    DATE          NOT NULL,
            workspace_id  TEXT          NOT NULL,
            endpoint_name TEXT          NOT NULL,
            sku_name      TEXT          NOT NULL DEFAULT '',
            total_dbus    NUMERIC(18,4) NOT NULL DEFAULT 0,
            total_cost_usd NUMERIC(18,4) NOT NULL DEFAULT 0,
            PRIMARY KEY (usage_date, workspace_id, endpoint_name, sku_name)
        )
        """,
        # Daily token usage data (from system.serving.endpoint_usage)
        """
        CREATE TABLE IF NOT EXISTS billing_token_daily (
            usage_date       DATE    NOT NULL,
            workspace_id     TEXT    NOT NULL,
            endpoint_name    TEXT    NOT NULL,
            request_count    BIGINT  NOT NULL DEFAULT 0,
            input_tokens     BIGINT  NOT NULL DEFAULT 0,
            output_tokens    BIGINT  NOT NULL DEFAULT 0,
            avg_input_tokens NUMERIC(12,2) NOT NULL DEFAULT 0,
            avg_output_tokens NUMERIC(12,2) NOT NULL DEFAULT 0,
            PRIMARY KEY (usage_date, workspace_id, endpoint_name)
        )
        """,
        # Daily all-product cost data (from system.billing.usage)
        """
        CREATE TABLE IF NOT EXISTS billing_product_daily (
            usage_date              DATE          NOT NULL,
            workspace_id            TEXT          NOT NULL,
            billing_origin_product  TEXT          NOT NULL,
            total_dbus              NUMERIC(18,4) NOT NULL DEFAULT 0,
            total_cost_usd          NUMERIC(18,4) NOT NULL DEFAULT 0,
            PRIMARY KEY (usage_date, workspace_id, billing_origin_product)
        )
        """,
        # Refresh metadata – one row per cache key
        """
        CREATE TABLE IF NOT EXISTS billing_cache_meta (
            cache_key      TEXT PRIMARY KEY,
            last_refreshed TIMESTAMP WITH TIME ZONE,
            rows_loaded    INTEGER NOT NULL DEFAULT 0
        )
        """,
        # Per-user, per-endpoint token usage (from system.serving.endpoint_usage)
        # This is the authoritative source for user identity (requester field).
        # Cost-by-user is computed at read time by joining with billing_serving_daily.
        """
        CREATE TABLE IF NOT EXISTS billing_user_endpoint_daily (
            usage_date     DATE    NOT NULL,
            workspace_id   TEXT    NOT NULL,
            endpoint_name  TEXT    NOT NULL,
            user_identity  TEXT    NOT NULL DEFAULT '',
            request_count  BIGINT  NOT NULL DEFAULT 0,
            input_tokens   BIGINT  NOT NULL DEFAULT 0,
            output_tokens  BIGINT  NOT NULL DEFAULT 0,
            PRIMARY KEY (usage_date, workspace_id, endpoint_name, user_identity)
        )
        """,
        # Indexes for fast workspace-filtered reads
        "CREATE INDEX IF NOT EXISTS idx_bsd_ws  ON billing_serving_daily  (workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_btd_ws  ON billing_token_daily    (workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_bpd_ws  ON billing_product_daily  (workspace_id)",
        "CREATE INDEX IF NOT EXISTS idx_bued_ws ON billing_user_endpoint_daily (workspace_id)",
        # Add value_text column (idempotent) for storing non-numeric metadata
        "ALTER TABLE billing_cache_meta ADD COLUMN IF NOT EXISTS value_text TEXT",
    ]

    for stmt in ddl_statements:
        try:
            execute_update(stmt)
        except Exception as exc:
            # Table may already exist or DDL may fail in edge cases – log and continue
            logger.warning("DDL warning: %s", exc)

    logger.info("Billing cache tables ensured")


# =====================================================================
# REFRESH: system tables → Lakebase
# =====================================================================

def _is_stale(cache_key: str) -> bool:
    """Check if a cache entry is stale (>24 h old or missing)."""
    row = execute_one(
        "SELECT last_refreshed FROM billing_cache_meta WHERE cache_key = %s",
        (cache_key,),
    )
    if not row:
        return True
    age = (datetime.now(timezone.utc) - row["last_refreshed"].replace(tzinfo=timezone.utc)).total_seconds()
    return age > _STALE_SECONDS


def _update_meta(cache_key: str, rows_loaded: int):
    execute_update(
        """INSERT INTO billing_cache_meta (cache_key, last_refreshed, rows_loaded)
           VALUES (%s, NOW(), %s)
           ON CONFLICT (cache_key) DO UPDATE
           SET last_refreshed = NOW(), rows_loaded = EXCLUDED.rows_loaded""",
        (cache_key, rows_loaded),
    )


def refresh_serving_daily(days: int = _REFRESH_DAYS) -> int:
    """Load model-serving cost data from system.billing.usage into Lakebase."""
    sql = f"""
    SELECT
        CAST(u.usage_date AS STRING)              AS usage_date,
        u.workspace_id,
        u.usage_metadata.endpoint_name            AS endpoint_name,
        u.sku_name,
        ROUND(SUM(u.usage_quantity), 4)           AS total_dbus,
        ROUND(SUM(u.usage_quantity *
            COALESCE(lp.pricing.effective_list.default, lp.pricing.default, 0.07)
        ), 4)                                      AS total_cost_usd
    FROM system.billing.usage u
    LEFT JOIN system.billing.list_prices lp
        ON u.sku_name = lp.sku_name
        AND u.cloud   = lp.cloud
        AND u.usage_unit = lp.usage_unit
        AND lp.price_end_time IS NULL
    WHERE u.billing_origin_product = 'MODEL_SERVING'
      AND u.usage_date >= current_date() - INTERVAL {days} DAYS
      AND u.usage_metadata.endpoint_name IS NOT NULL
      AND u.workspace_id IS NOT NULL
    GROUP BY u.usage_date, u.workspace_id, u.usage_metadata.endpoint_name, u.sku_name
    """
    rows = _execute_system_sql(sql)
    if not rows:
        _update_meta("serving_daily", 0)
        logger.info("Refreshed billing_serving_daily: 0 rows (no new data)")
        return 0

    with DatabasePool.get_connection() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """INSERT INTO billing_serving_daily
                       (usage_date, workspace_id, endpoint_name, sku_name, total_dbus, total_cost_usd)
                       VALUES (%s, %s, %s, %s, %s, %s)
                       ON CONFLICT (usage_date, workspace_id, endpoint_name, sku_name)
                       DO UPDATE SET total_dbus = EXCLUDED.total_dbus,
                                     total_cost_usd = EXCLUDED.total_cost_usd""",
                    (
                        r["usage_date"],
                        r["workspace_id"],
                        r["endpoint_name"],
                        r.get("sku_name", ""),
                        float(r.get("total_dbus") or 0),
                        float(r.get("total_cost_usd") or 0),
                    ),
                )
            conn.commit()

    _update_meta("serving_daily", len(rows))
    logger.info("Refreshed billing_serving_daily: %s rows", len(rows))
    return len(rows)


def refresh_token_daily(days: int = _REFRESH_DAYS) -> int:
    """Load token usage from system.serving.endpoint_usage into Lakebase."""
    sql = f"""
    SELECT
        CAST(DATE(eu.request_time) AS STRING)     AS usage_date,
        eu.workspace_id,
        se.endpoint_name,
        COUNT(*)                                   AS request_count,
        SUM(eu.input_token_count)                  AS input_tokens,
        SUM(eu.output_token_count)                 AS output_tokens,
        ROUND(AVG(eu.input_token_count), 2)        AS avg_input_tokens,
        ROUND(AVG(eu.output_token_count), 2)       AS avg_output_tokens
    FROM system.serving.endpoint_usage eu
    JOIN system.serving.served_entities se
        ON eu.served_entity_id = se.served_entity_id
    WHERE eu.request_time >= current_timestamp() - INTERVAL {days} DAYS
      AND eu.workspace_id IS NOT NULL
    GROUP BY DATE(eu.request_time), eu.workspace_id, se.endpoint_name
    """
    rows = _execute_system_sql(sql)
    if not rows:
        _update_meta("token_daily", 0)
        logger.info("Refreshed billing_token_daily: 0 rows (no new data)")
        return 0

    with DatabasePool.get_connection() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """INSERT INTO billing_token_daily
                       (usage_date, workspace_id, endpoint_name, request_count,
                        input_tokens, output_tokens, avg_input_tokens, avg_output_tokens)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (usage_date, workspace_id, endpoint_name)
                       DO UPDATE SET request_count = EXCLUDED.request_count,
                                     input_tokens  = EXCLUDED.input_tokens,
                                     output_tokens  = EXCLUDED.output_tokens,
                                     avg_input_tokens  = EXCLUDED.avg_input_tokens,
                                     avg_output_tokens  = EXCLUDED.avg_output_tokens""",
                    (
                        r["usage_date"],
                        r["workspace_id"],
                        r["endpoint_name"],
                        int(r.get("request_count") or 0),
                        int(r.get("input_tokens") or 0),
                        int(r.get("output_tokens") or 0),
                        float(r.get("avg_input_tokens") or 0),
                        float(r.get("avg_output_tokens") or 0),
                    ),
                )
            conn.commit()

    _update_meta("token_daily", len(rows))
    logger.info("Refreshed billing_token_daily: %s rows", len(rows))
    return len(rows)


def refresh_product_daily(days: int = _REFRESH_DAYS) -> int:
    """Load all-product costs from system.billing.usage into Lakebase."""
    sql = f"""
    SELECT
        CAST(u.usage_date AS STRING)               AS usage_date,
        u.workspace_id,
        u.billing_origin_product,
        ROUND(SUM(u.usage_quantity), 4)             AS total_dbus,
        ROUND(SUM(u.usage_quantity *
            COALESCE(lp.pricing.effective_list.default, lp.pricing.default, 0)
        ), 4)                                        AS total_cost_usd
    FROM system.billing.usage u
    LEFT JOIN system.billing.list_prices lp
        ON u.sku_name = lp.sku_name
        AND u.cloud   = lp.cloud
        AND u.usage_unit = lp.usage_unit
        AND lp.price_end_time IS NULL
    WHERE u.usage_date >= current_date() - INTERVAL {days} DAYS
      AND u.workspace_id IS NOT NULL
    GROUP BY u.usage_date, u.workspace_id, u.billing_origin_product
    """
    rows = _execute_system_sql(sql)
    if not rows:
        _update_meta("product_daily", 0)
        logger.info("Refreshed billing_product_daily: 0 rows (no new data)")
        return 0

    with DatabasePool.get_connection() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """INSERT INTO billing_product_daily
                       (usage_date, workspace_id, billing_origin_product, total_dbus, total_cost_usd)
                       VALUES (%s, %s, %s, %s, %s)
                       ON CONFLICT (usage_date, workspace_id, billing_origin_product)
                       DO UPDATE SET total_dbus = EXCLUDED.total_dbus,
                                     total_cost_usd = EXCLUDED.total_cost_usd""",
                    (
                        r["usage_date"],
                        r["workspace_id"],
                        r["billing_origin_product"],
                        float(r.get("total_dbus") or 0),
                        float(r.get("total_cost_usd") or 0),
                    ),
                )
            conn.commit()

    _update_meta("product_daily", len(rows))
    logger.info("Refreshed billing_product_daily: %s rows", len(rows))
    return len(rows)


def refresh_user_endpoint_daily(days: int = _REFRESH_DAYS) -> int:
    """Load per-user, per-endpoint token usage from system.serving.endpoint_usage.

    This uses the `requester` field (the actual user who sent the request)
    and groups by endpoint_name so cost attribution at read time is precise:
    each endpoint's billing cost is split across users by their exact share
    of that endpoint's tokens.
    """
    sql = f"""
    SELECT
        CAST(DATE(eu.request_time) AS STRING)          AS usage_date,
        eu.workspace_id,
        se.endpoint_name,
        COALESCE(eu.requester, 'unknown')              AS user_identity,
        COUNT(*)                                        AS request_count,
        COALESCE(SUM(eu.input_token_count), 0)          AS input_tokens,
        COALESCE(SUM(eu.output_token_count), 0)         AS output_tokens
    FROM system.serving.endpoint_usage eu
    JOIN system.serving.served_entities se
        ON eu.served_entity_id = se.served_entity_id
    WHERE eu.request_time >= current_timestamp() - INTERVAL {days} DAYS
      AND eu.workspace_id IS NOT NULL
    GROUP BY DATE(eu.request_time), eu.workspace_id, se.endpoint_name, eu.requester
    """
    rows = _execute_system_sql(sql)
    if not rows:
        _update_meta("user_endpoint_daily", 0)
        logger.info("Refreshed billing_user_endpoint_daily: 0 rows (no new data)")
        return 0

    with DatabasePool.get_connection() as conn:
        with conn.cursor() as cur:
            for r in rows:
                cur.execute(
                    """INSERT INTO billing_user_endpoint_daily
                       (usage_date, workspace_id, endpoint_name, user_identity,
                        request_count, input_tokens, output_tokens)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)
                       ON CONFLICT (usage_date, workspace_id, endpoint_name, user_identity)
                       DO UPDATE SET request_count = EXCLUDED.request_count,
                                     input_tokens  = EXCLUDED.input_tokens,
                                     output_tokens  = EXCLUDED.output_tokens""",
                    (
                        r["usage_date"],
                        r["workspace_id"],
                        r["endpoint_name"],
                        r.get("user_identity", "unknown"),
                        int(r.get("request_count") or 0),
                        int(r.get("input_tokens") or 0),
                        int(r.get("output_tokens") or 0),
                    ),
                )
            conn.commit()

    _update_meta("user_endpoint_daily", len(rows))
    logger.info("Refreshed billing_user_endpoint_daily: %s rows", len(rows))
    return len(rows)


def refresh_all(days: int = _REFRESH_DAYS) -> Dict[str, int]:
    """Refresh all billing caches. Returns row counts per table."""
    return {
        "serving_daily": refresh_serving_daily(days),
        "token_daily": refresh_token_daily(days),
        "product_daily": refresh_product_daily(days),
        "user_endpoint_daily": refresh_user_endpoint_daily(days),
    }


def _start_background_refresh(days: int = _REFRESH_DAYS):
    """Spin up a daemon thread that refreshes all billing caches."""
    global _refresh_in_progress
    _refresh_in_progress = True          # set BEFORE thread start so callers see it immediately

    def _do():
        global _refresh_in_progress
        with _refresh_lock:
            try:
                logger.info("Starting background billing cache refresh …")
                result = refresh_all(days)
                logger.info("Background refresh complete: %s", result)
            except Exception as exc:
                logger.warning("Background refresh failed: %s", exc)
            finally:
                _refresh_in_progress = False

    t = threading.Thread(target=_do, daemon=True)
    t.start()


def _any_stale() -> bool:
    """Single-query check for staleness across all billing cache keys."""
    keys = ("serving_daily", "token_daily", "product_daily", "user_endpoint_daily")
    rows = execute_query(
        "SELECT cache_key, last_refreshed FROM billing_cache_meta WHERE cache_key = ANY(%s)",
        (list(keys),),
    )
    if len(rows) < len(keys):
        return True
    now = datetime.now(timezone.utc)
    return any(
        (now - r["last_refreshed"].replace(tzinfo=timezone.utc)).total_seconds() > _STALE_SECONDS
        for r in rows
    )


def maybe_refresh_async():
    """If any cache is stale, trigger a background refresh (non-blocking)."""
    if _refresh_in_progress:
        return
    try:
        if _any_stale():
            _start_background_refresh()
    except Exception:
        pass


def force_refresh_async(days: int = _REFRESH_DAYS):
    """Force a background refresh regardless of staleness (non-blocking).

    Returns immediately.  The caller can poll ``get_cache_status()`` to
    check progress via the ``is_refreshing`` flag.
    """
    if _refresh_in_progress:
        return  # already running, skip
    _start_background_refresh(days)


# =====================================================================
# READ: fast queries from Lakebase
# =====================================================================

def _ws_clause(alias: str, workspace_id: Optional[str]) -> str:
    """Return a SQL WHERE fragment for workspace filtering."""
    if workspace_id:
        return f"AND {alias}.workspace_id = %s"
    return ""


def _ws_params(workspace_id: Optional[str]) -> tuple:
    return (workspace_id,) if workspace_id else ()


# ── workspace helpers ────────────────────────────────────────────

_cached_ws_id: Optional[str] = None


def _persist_workspace_id(ws_id: str):
    """Save the workspace ID in billing_cache_meta so it survives restarts."""
    try:
        execute_update(
            """INSERT INTO billing_cache_meta (cache_key, last_refreshed, rows_loaded, value_text)
               VALUES ('current_workspace_id', NOW(), 0, %s)
               ON CONFLICT (cache_key) DO UPDATE
               SET value_text = EXCLUDED.value_text, last_refreshed = NOW()""",
            (ws_id,),
        )
    except Exception as exc:
        logger.warning("Could not persist workspace_id: %s", exc)


def get_current_workspace_id() -> Optional[str]:
    """Return the workspace ID of the workspace this app is running on.

    Priority:
      1. In-memory cache (instant)
      2. Databricks SDK – w.get_workspace_id() (authoritative)
      3. Persisted value in billing_cache_meta (fast, survives restarts)
    """
    global _cached_ws_id
    if _cached_ws_id:
        return _cached_ws_id

    # --- Step 2: ask the SDK (authoritative source) ---
    w = _get_workspace_client()
    if w:
        try:
            ws_id = w.get_workspace_id()
            if ws_id:
                _cached_ws_id = str(ws_id)
                _persist_workspace_id(_cached_ws_id)
                logger.info("Resolved workspace ID from SDK: %s", _cached_ws_id)
                return _cached_ws_id
        except Exception as exc:
            logger.warning("SDK get_workspace_id() failed: %s", exc)

    # --- Step 3: persisted value_text in cache_meta (fast) ---
    try:
        row = execute_one(
            "SELECT value_text FROM billing_cache_meta WHERE cache_key = 'current_workspace_id'"
        )
        if row and row.get("value_text"):
            _cached_ws_id = str(row["value_text"])
            return _cached_ws_id
    except Exception:
        pass

    return None


def get_available_workspaces(days: int = 90) -> List[Dict[str, Any]]:
    """List workspaces from the Lakebase cache (fast)."""
    maybe_refresh_async()
    return execute_query(
        """SELECT workspace_id,
                  SUM(total_dbus)::NUMERIC(18,2)           AS total_dbus,
                  COUNT(DISTINCT endpoint_name)::INT       AS endpoint_count
           FROM billing_serving_daily
           WHERE usage_date >= CURRENT_DATE - %s
           GROUP BY workspace_id
           ORDER BY SUM(total_dbus) DESC""",
        (days,),
    )


# ── serving cost ─────────────────────────────────────────────────

def get_serving_cost_summary(days: int = 30, workspace_id: Optional[str] = None) -> Dict[str, Any]:
    maybe_refresh_async()

    ws = "AND s.workspace_id = %s" if workspace_id else ""
    params: tuple = (days, workspace_id) if workspace_id else (days,)

    rows = execute_query(
        f"""SELECT endpoint_name,
                   SUM(total_dbus)::NUMERIC(18,2)      AS total_dbus,
                   SUM(total_cost_usd)::NUMERIC(18,2)  AS total_cost_usd
            FROM billing_serving_daily s
            WHERE s.usage_date >= CURRENT_DATE - %s
              {ws}
            GROUP BY endpoint_name
            ORDER BY total_cost_usd DESC""",
        params,
    )

    total_cost = sum(float(r.get("total_cost_usd") or 0) for r in rows)
    total_dbus = sum(float(r.get("total_dbus") or 0) for r in rows)
    cost_by_ep = {r["endpoint_name"]: float(r["total_cost_usd"] or 0) for r in rows}
    dbus_by_ep = {r["endpoint_name"]: float(r["total_dbus"] or 0) for r in rows}

    return {
        "total_cost_usd": round(total_cost, 2),
        "total_dbus": round(total_dbus, 2),
        "endpoint_count": len(rows),
        "cost_by_endpoint": cost_by_ep,
        "dbus_by_endpoint": dbus_by_ep,
    }


def get_serving_cost_trend(days: int = 30, workspace_id: Optional[str] = None) -> List[Dict[str, Any]]:
    maybe_refresh_async()

    ws = "AND s.workspace_id = %s" if workspace_id else ""
    params: tuple = (days, workspace_id) if workspace_id else (days,)

    return execute_query(
        f"""SELECT usage_date::TEXT                         AS day,
                   SUM(total_dbus)::NUMERIC(18,2)           AS total_dbus,
                   SUM(total_cost_usd)::NUMERIC(18,2)       AS total_cost_usd
            FROM billing_serving_daily s
            WHERE s.usage_date >= CURRENT_DATE - %s
              {ws}
            GROUP BY usage_date
            ORDER BY usage_date""",
        params,
    )


def get_serving_cost_by_sku(days: int = 30, workspace_id: Optional[str] = None) -> List[Dict[str, Any]]:
    maybe_refresh_async()

    ws = "AND s.workspace_id = %s" if workspace_id else ""
    params: tuple = (days, workspace_id) if workspace_id else (days,)

    return execute_query(
        f"""SELECT sku_name,
                   SUM(total_dbus)::NUMERIC(18,2)           AS total_dbus,
                   SUM(total_cost_usd)::NUMERIC(18,2)       AS total_cost_usd
            FROM billing_serving_daily s
            WHERE s.usage_date >= CURRENT_DATE - %s
              {ws}
            GROUP BY sku_name
            ORDER BY total_cost_usd DESC""",
        params,
    )


# ── token usage ──────────────────────────────────────────────────

def get_serving_token_usage(days: int = 30, workspace_id: Optional[str] = None) -> List[Dict[str, Any]]:
    maybe_refresh_async()

    ws = "AND t.workspace_id = %s" if workspace_id else ""
    params: tuple = (days, workspace_id) if workspace_id else (days,)

    return execute_query(
        f"""SELECT endpoint_name,
                   SUM(request_count)::BIGINT               AS request_count,
                   SUM(input_tokens)::BIGINT                AS total_input_tokens,
                   SUM(output_tokens)::BIGINT               AS total_output_tokens,
                   SUM(input_tokens + output_tokens)::BIGINT AS total_tokens,
                   ROUND(AVG(avg_input_tokens), 0)::BIGINT  AS avg_input_tokens,
                   ROUND(AVG(avg_output_tokens), 0)::BIGINT AS avg_output_tokens
            FROM billing_token_daily t
            WHERE t.usage_date >= CURRENT_DATE - %s
              {ws}
            GROUP BY endpoint_name
            ORDER BY total_tokens DESC
            LIMIT 50""",
        params,
    )


def get_serving_daily_tokens(days: int = 30, workspace_id: Optional[str] = None) -> List[Dict[str, Any]]:
    maybe_refresh_async()

    ws = "AND t.workspace_id = %s" if workspace_id else ""
    params: tuple = (days, workspace_id) if workspace_id else (days,)

    return execute_query(
        f"""SELECT usage_date::TEXT                         AS day,
                   SUM(request_count)::BIGINT               AS request_count,
                   SUM(input_tokens)::BIGINT                AS total_input_tokens,
                   SUM(output_tokens)::BIGINT               AS total_output_tokens
            FROM billing_token_daily t
            WHERE t.usage_date >= CURRENT_DATE - %s
              {ws}
            GROUP BY usage_date
            ORDER BY usage_date""",
        params,
    )


# ── cost by user ─────────────────────────────────────────────────

# ── cost by user (precise per-endpoint attribution) ──────────────

def get_serving_cost_by_user(days: int = 30, workspace_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Top users by model-serving cost.

    Joins billing_user_endpoint_daily (per-user, per-endpoint token counts
    from system.serving.endpoint_usage) with billing_serving_daily (per-endpoint
    costs from system.billing.usage).  Each endpoint's cost is split across
    users by their exact share of that endpoint's tokens on each day.
    """
    maybe_refresh_async()

    ws = "AND workspace_id = %s" if workspace_id else ""
    single_p: tuple = (days, workspace_id) if workspace_id else (days,)
    params = single_p * 3

    return execute_query(
        f"""WITH ep_costs AS (
                SELECT usage_date, workspace_id, endpoint_name,
                       SUM(total_cost_usd) AS ep_cost,
                       SUM(total_dbus)     AS ep_dbus
                FROM billing_serving_daily
                WHERE usage_date >= CURRENT_DATE - %s {ws}
                GROUP BY usage_date, workspace_id, endpoint_name
            ),
            ep_totals AS (
                SELECT usage_date, workspace_id, endpoint_name,
                       SUM(input_tokens + output_tokens) AS ep_total_tokens
                FROM billing_user_endpoint_daily
                WHERE usage_date >= CURRENT_DATE - %s {ws}
                GROUP BY usage_date, workspace_id, endpoint_name
            )
            SELECT
                u.user_identity,
                SUM(CASE WHEN et.ep_total_tokens > 0
                    THEN ec.ep_dbus * (u.input_tokens + u.output_tokens)::NUMERIC
                         / et.ep_total_tokens
                    ELSE 0 END)::NUMERIC(18,2) AS total_dbus,
                SUM(CASE WHEN et.ep_total_tokens > 0
                    THEN ec.ep_cost * (u.input_tokens + u.output_tokens)::NUMERIC
                         / et.ep_total_tokens
                    ELSE 0 END)::NUMERIC(18,2) AS total_cost_usd
            FROM billing_user_endpoint_daily u
            JOIN ep_costs ec
                ON u.usage_date = ec.usage_date
                AND u.workspace_id = ec.workspace_id
                AND u.endpoint_name = ec.endpoint_name
            JOIN ep_totals et
                ON u.usage_date = et.usage_date
                AND u.workspace_id = et.workspace_id
                AND u.endpoint_name = et.endpoint_name
            WHERE u.usage_date >= CURRENT_DATE - %s {ws}
            GROUP BY u.user_identity
            ORDER BY total_cost_usd DESC
            LIMIT 25""",
        params,
    )


# ── token usage by user ─────────────────────────────────────────

def get_token_usage_by_user(days: int = 30, workspace_id: Optional[str] = None) -> List[Dict[str, Any]]:
    """Top users by token consumption, from Lakebase cache."""
    maybe_refresh_async()

    ws = "AND u.workspace_id = %s" if workspace_id else ""
    params: tuple = (days, workspace_id) if workspace_id else (days,)

    return execute_query(
        f"""SELECT user_identity,
                   SUM(request_count)::BIGINT               AS request_count,
                   SUM(input_tokens)::BIGINT                AS total_input_tokens,
                   SUM(output_tokens)::BIGINT               AS total_output_tokens,
                   SUM(input_tokens + output_tokens)::BIGINT AS total_tokens
            FROM billing_user_endpoint_daily u
            WHERE u.usage_date >= CURRENT_DATE - %s
              {ws}
            GROUP BY user_identity
            ORDER BY total_tokens DESC
            LIMIT 25""",
        params,
    )


# ── product costs ────────────────────────────────────────────────

def get_all_product_costs(days: int = 30, workspace_id: Optional[str] = None) -> List[Dict[str, Any]]:
    maybe_refresh_async()

    ws = "AND p.workspace_id = %s" if workspace_id else ""
    params: tuple = (days, workspace_id) if workspace_id else (days,)

    return execute_query(
        f"""SELECT billing_origin_product,
                   SUM(total_dbus)::NUMERIC(18,2)            AS total_dbus,
                   SUM(total_cost_usd)::NUMERIC(18,2)        AS total_cost_usd
            FROM billing_product_daily p
            WHERE p.usage_date >= CURRENT_DATE - %s
              {ws}
            GROUP BY billing_origin_product
            ORDER BY total_cost_usd DESC""",
        params,
    )


# ── cache status ─────────────────────────────────────────────────

def get_cache_status() -> Dict[str, Any]:
    """Return info about cache freshness."""
    rows = execute_query("SELECT * FROM billing_cache_meta ORDER BY cache_key")
    is_refreshing = _refresh_in_progress
    entries = {}
    for r in rows:
        if r["cache_key"] == "current_workspace_id":
            continue
        entries[r["cache_key"]] = {
            "last_refreshed": r["last_refreshed"].isoformat() if r["last_refreshed"] else None,
            "rows_loaded": r["rows_loaded"],
        }
    return {
        "is_refreshing": is_refreshing,
        "caches": entries,
    }


# =====================================================================
# COMPOSITE: single-request endpoint for the Governance page
# =====================================================================

def get_all_page_data(days: int = 30, workspace_id: Optional[str] = None) -> Dict[str, Any]:
    """Return ALL billing data the Governance page needs in a single DB connection.

    Instead of 7+ parallel HTTP requests each opening a connection (~1 s SSL
    handshake each from local dev), we run 8 small queries sequentially on ONE
    connection.  Total time: ~0.8 s instead of 12–17 s.
    """
    from psycopg2.extras import RealDictCursor

    maybe_refresh_async()

    # Build workspace filter fragment + params
    ws_filter = "AND workspace_id = %s" if workspace_id else ""
    _p = lambda extra_days=True: (  # noqa: E731
        (days, workspace_id) if (workspace_id and extra_days)
        else (days,) if extra_days
        else (workspace_id,) if workspace_id
        else ()
    )

    with DatabasePool.get_connection() as conn:
        cur = conn.cursor(cursor_factory=RealDictCursor)

        # 1. workspaces (cache_meta moved to end so timestamps match data)
        cur.execute(
            f"""SELECT workspace_id,
                       SUM(total_dbus)::NUMERIC(18,2) AS total_dbus,
                       COUNT(DISTINCT endpoint_name)::INT AS endpoint_count
                FROM billing_serving_daily
                WHERE usage_date >= CURRENT_DATE - %s
                GROUP BY workspace_id
                ORDER BY SUM(total_dbus) DESC""",
            (days,),
        )
        workspaces = [dict(r) for r in cur.fetchall()]
        # 3. serving summary (by endpoint)
        cur.execute(
            f"""SELECT endpoint_name,
                       SUM(total_dbus)::NUMERIC(18,2)     AS total_dbus,
                       SUM(total_cost_usd)::NUMERIC(18,2) AS total_cost_usd
                FROM billing_serving_daily s
                WHERE s.usage_date >= CURRENT_DATE - %s {ws_filter}
                GROUP BY endpoint_name
                ORDER BY total_cost_usd DESC""",
            _p(),
        )
        summary_rows = [dict(r) for r in cur.fetchall()]

        # 4. serving trend (daily)
        cur.execute(
            f"""SELECT usage_date::TEXT AS day,
                       SUM(total_dbus)::NUMERIC(18,2) AS total_dbus,
                       SUM(total_cost_usd)::NUMERIC(18,2) AS total_cost_usd
                FROM billing_serving_daily s
                WHERE s.usage_date >= CURRENT_DATE - %s {ws_filter}
                GROUP BY usage_date ORDER BY usage_date""",
            _p(),
        )
        trend = [dict(r) for r in cur.fetchall()]

        # 5. serving by SKU
        cur.execute(
            f"""SELECT sku_name,
                       SUM(total_dbus)::NUMERIC(18,2) AS total_dbus,
                       SUM(total_cost_usd)::NUMERIC(18,2) AS total_cost_usd
                FROM billing_serving_daily s
                WHERE s.usage_date >= CURRENT_DATE - %s {ws_filter}
                GROUP BY sku_name ORDER BY total_cost_usd DESC""",
            _p(),
        )
        by_sku = [dict(r) for r in cur.fetchall()]

        # 6. token usage by endpoint
        cur.execute(
            f"""SELECT endpoint_name,
                       SUM(request_count)::BIGINT AS request_count,
                       SUM(input_tokens)::BIGINT AS total_input_tokens,
                       SUM(output_tokens)::BIGINT AS total_output_tokens,
                       SUM(input_tokens + output_tokens)::BIGINT AS total_tokens,
                       ROUND(AVG(avg_input_tokens), 0)::BIGINT AS avg_input_tokens,
                       ROUND(AVG(avg_output_tokens), 0)::BIGINT AS avg_output_tokens
                FROM billing_token_daily t
                WHERE t.usage_date >= CURRENT_DATE - %s {ws_filter}
                GROUP BY endpoint_name ORDER BY total_tokens DESC LIMIT 50""",
            _p(),
        )
        tokens = [dict(r) for r in cur.fetchall()]

        # 7. daily token totals
        cur.execute(
            f"""SELECT usage_date::TEXT AS day,
                       SUM(request_count)::BIGINT AS request_count,
                       SUM(input_tokens)::BIGINT AS total_input_tokens,
                       SUM(output_tokens)::BIGINT AS total_output_tokens
                FROM billing_token_daily t
                WHERE t.usage_date >= CURRENT_DATE - %s {ws_filter}
                GROUP BY usage_date ORDER BY usage_date""",
            _p(),
        )
        daily_tokens = [dict(r) for r in cur.fetchall()]

        # 8. all-product costs
        cur.execute(
            f"""SELECT billing_origin_product,
                       SUM(total_dbus)::NUMERIC(18,2) AS total_dbus,
                       SUM(total_cost_usd)::NUMERIC(18,2) AS total_cost_usd
                FROM billing_product_daily p
                WHERE p.usage_date >= CURRENT_DATE - %s {ws_filter}
                GROUP BY billing_origin_product ORDER BY total_cost_usd DESC""",
            _p(),
        )
        products = [dict(r) for r in cur.fetchall()]

        # 9. serving cost by user (precise per-endpoint attribution)
        # The outer query JOINs 3 sources that all have workspace_id,
        # so we must qualify with the table alias to avoid ambiguity.
        ws_filter_u = "AND u.workspace_id = %s" if workspace_id else ""
        cur.execute(
            f"""WITH ep_costs AS (
                    SELECT usage_date, workspace_id, endpoint_name,
                           SUM(total_cost_usd) AS ep_cost,
                           SUM(total_dbus)     AS ep_dbus
                    FROM billing_serving_daily
                    WHERE usage_date >= CURRENT_DATE - %s {ws_filter}
                    GROUP BY usage_date, workspace_id, endpoint_name
                ),
                ep_totals AS (
                    SELECT usage_date, workspace_id, endpoint_name,
                           SUM(input_tokens + output_tokens) AS ep_total_tokens
                    FROM billing_user_endpoint_daily
                    WHERE usage_date >= CURRENT_DATE - %s {ws_filter}
                    GROUP BY usage_date, workspace_id, endpoint_name
                )
                SELECT
                    u.user_identity,
                    SUM(CASE WHEN et.ep_total_tokens > 0
                        THEN ec.ep_dbus * (u.input_tokens + u.output_tokens)::NUMERIC
                             / et.ep_total_tokens
                        ELSE 0 END)::NUMERIC(18,2) AS total_dbus,
                    SUM(CASE WHEN et.ep_total_tokens > 0
                        THEN ec.ep_cost * (u.input_tokens + u.output_tokens)::NUMERIC
                             / et.ep_total_tokens
                        ELSE 0 END)::NUMERIC(18,2) AS total_cost_usd
                FROM billing_user_endpoint_daily u
                JOIN ep_costs ec
                    ON u.usage_date = ec.usage_date
                    AND u.workspace_id = ec.workspace_id
                    AND u.endpoint_name = ec.endpoint_name
                JOIN ep_totals et
                    ON u.usage_date = et.usage_date
                    AND u.workspace_id = et.workspace_id
                    AND u.endpoint_name = et.endpoint_name
                WHERE u.usage_date >= CURRENT_DATE - %s {ws_filter_u}
                GROUP BY u.user_identity
                ORDER BY total_cost_usd DESC
                LIMIT 25""",
            _p() * 3,
        )
        cost_by_user = [dict(r) for r in cur.fetchall()]

        # 10. token usage by user
        cur.execute(
            f"""SELECT user_identity,
                       SUM(request_count)::BIGINT AS request_count,
                       SUM(input_tokens)::BIGINT AS total_input_tokens,
                       SUM(output_tokens)::BIGINT AS total_output_tokens,
                       SUM(input_tokens + output_tokens)::BIGINT AS total_tokens
                FROM billing_user_endpoint_daily u
                WHERE u.usage_date >= CURRENT_DATE - %s {ws_filter}
                GROUP BY user_identity ORDER BY total_tokens DESC LIMIT 25""",
            _p(),
        )
        tokens_by_user = [dict(r) for r in cur.fetchall()]

        # Read cache_meta LAST so timestamps reflect the same state as the data
        cur.execute("SELECT * FROM billing_cache_meta ORDER BY cache_key")
        cache_rows = [dict(r) for r in cur.fetchall()]

        cur.close()

    # Assemble cache_status (exclude non-cache metadata entries)
    cache_entries = {}
    for r in cache_rows:
        if r["cache_key"] == "current_workspace_id":
            continue
        cache_entries[r["cache_key"]] = {
            "last_refreshed": r["last_refreshed"].isoformat() if r["last_refreshed"] else None,
            "rows_loaded": r["rows_loaded"],
        }

    # Assemble summary
    total_cost = sum(float(r.get("total_cost_usd") or 0) for r in summary_rows)
    total_dbus = sum(float(r.get("total_dbus") or 0) for r in summary_rows)
    cost_by_ep = {r["endpoint_name"]: float(r["total_cost_usd"] or 0) for r in summary_rows}
    dbus_by_ep = {r["endpoint_name"]: float(r["total_dbus"] or 0) for r in summary_rows}

    return {
        "current_workspace_id": get_current_workspace_id(),
        "cache_status": {"is_refreshing": _refresh_in_progress, "caches": cache_entries},
        "workspaces": workspaces,
        "summary": {
            "total_cost_usd": round(total_cost, 2),
            "total_dbus": round(total_dbus, 2),
            "endpoint_count": len(summary_rows),
            "cost_by_endpoint": cost_by_ep,
            "dbus_by_endpoint": dbus_by_ep,
        },
        "trend": trend,
        "by_sku": by_sku,
        "tokens": tokens,
        "daily_tokens": daily_tokens,
        "products": products,
        "cost_by_user": cost_by_user,
        "tokens_by_user": tokens_by_user,
    }
