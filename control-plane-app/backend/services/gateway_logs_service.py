"""Reads AI Gateway / Model Serving inference-log rows cached in Lakebase
(`gateway_inference_logs` table, populated by the discovery workflow's Tier 2a
SQL discovery — see workflows/08_discover_gateway_inference_logs.py).

The cache is populated account-wide via Unity Catalog; rows here originate
from any UC `<prefix>_payload` table the discovery principal could SELECT.
"""
from typing import Any, Dict, List, Optional
import logging
import time as _time

from backend.database import execute_query, execute_update

logger = logging.getLogger(__name__)


def ensure_gateway_logs_table() -> None:
    """Idempotent DDL — mirrors the schema written by the workflow sync task."""
    ddl = [
        """
        CREATE TABLE IF NOT EXISTS gateway_inference_logs (
            request_id          TEXT NOT NULL,
            source_table        TEXT NOT NULL,
            client_request_id   TEXT,
            request_time        TIMESTAMP WITH TIME ZONE,
            status_code         INTEGER,
            execution_ms        BIGINT,
            request_payload     TEXT,
            response_payload    TEXT,
            request_size_bytes  BIGINT,
            response_size_bytes BIGINT,
            served_entity_id    TEXT,
            requester           TEXT,
            last_synced         TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            PRIMARY KEY (source_table, request_id)
        )
        """,
        "CREATE INDEX IF NOT EXISTS idx_gil_time   ON gateway_inference_logs (request_time DESC)",
        "CREATE INDEX IF NOT EXISTS idx_gil_source ON gateway_inference_logs (source_table)",
        "CREATE INDEX IF NOT EXISTS idx_gil_status ON gateway_inference_logs (status_code)",
    ]
    for stmt in ddl:
        try:
            execute_update(stmt)
        except Exception as exc:
            logger.warning("gateway_inference_logs DDL warning: %s", exc)


def list_gateway_logs(
    *,
    source_table: Optional[str] = None,
    window_days: Optional[int] = None,
    limit: int = 500,
    include_payload: bool = False,
) -> List[Dict[str, Any]]:
    """List gateway inference-log rows with optional filters.

    `include_payload=False` (default) returns lightweight rows with payload
    sizes only — suitable for the list view. Use `get_gateway_log` for the
    full request/response on a single row.
    """
    where: List[str] = []
    params: List[Any] = []
    if source_table:
        where.append("source_table = %s")
        params.append(source_table)
    if window_days:
        cutoff = int((_time.time() - window_days * 86400) * 1000)
        where.append("EXTRACT(EPOCH FROM request_time) * 1000 >= %s")
        params.append(cutoff)
    where_sql = (" WHERE " + " AND ".join(where)) if where else ""

    cols = (
        "request_id, source_table, client_request_id, request_time, status_code, "
        "execution_ms, request_size_bytes, response_size_bytes, served_entity_id, "
        "requester, model, input_tokens, output_tokens, total_tokens, "
        "finish_reason, tool_call_count, last_synced"
    )
    if include_payload:
        cols += ", request_payload, response_payload"

    params.append(limit)
    return execute_query(
        f"SELECT {cols} FROM gateway_inference_logs{where_sql} "
        f"ORDER BY request_time DESC NULLS LAST LIMIT %s",
        tuple(params),
    )


def get_gateway_log(source_table: str, request_id: str) -> Optional[Dict[str, Any]]:
    """Return a single inference-log row with full payloads."""
    rows = execute_query(
        "SELECT * FROM gateway_inference_logs "
        "WHERE source_table = %s AND request_id = %s",
        (source_table, request_id),
    )
    return rows[0] if rows else None


def gateway_timeseries(
    *,
    source_table: Optional[str] = None,
    window_days: int = 7,
    bucket: str = "hour",
) -> List[Dict[str, Any]]:
    """Return per-bucket aggregates over `gateway_inference_logs`.

    One row per (bucket, source_table). Used by the Gateway Requests panel
    chart to plot request rate / P95 latency / error rate / token volume
    over time. `bucket` is one of 'hour' | 'day'.
    """
    if bucket not in ("hour", "day"):
        bucket = "hour"
    where = ["request_time >= NOW() - %s::interval"]
    params: List[Any] = [f"{int(window_days)} days"]
    if source_table:
        where.append("source_table = %s")
        params.append(source_table)
    where_sql = " WHERE " + " AND ".join(where)
    sql = f"""
        SELECT date_trunc('{bucket}', request_time) AS bucket,
               source_table,
               COUNT(*)                                   AS requests,
               COUNT(*) FILTER (WHERE status_code >= 400) AS errors,
               PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY execution_ms) AS p50_ms,
               PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY execution_ms) AS p95_ms,
               AVG(execution_ms)                          AS avg_ms,
               COALESCE(SUM(input_tokens),  0)            AS input_tokens,
               COALESCE(SUM(output_tokens), 0)            AS output_tokens,
               COALESCE(SUM(total_tokens),  0)            AS total_tokens,
               COALESCE(SUM(request_size_bytes),  0)      AS request_bytes,
               COALESCE(SUM(response_size_bytes), 0)      AS response_bytes
        FROM gateway_inference_logs
        {where_sql}
        GROUP BY bucket, source_table
        ORDER BY bucket
    """
    return execute_query(sql, tuple(params))


def list_source_tables() -> List[Dict[str, Any]]:
    """Return distinct source tables with row counts and recency stats —
    useful for a high-level breakdown by served endpoint."""
    return execute_query(
        """
        SELECT source_table,
               COUNT(*)         AS row_count,
               MAX(request_time) AS last_request_time,
               COUNT(*) FILTER (WHERE status_code >= 400) AS error_count
        FROM gateway_inference_logs
        GROUP BY source_table
        ORDER BY last_request_time DESC NULLS LAST
        """
    )
