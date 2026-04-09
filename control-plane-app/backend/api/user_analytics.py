"""API routes for User Analytics & RBAC Dashboard.

Provides user activity metrics, heatmap data, and RBAC matrix views
by querying system.serving.endpoint_usage (Databricks system tables).
"""
from fastapi import APIRouter, Depends, Query
from backend.utils.auth import get_current_user
from typing import Dict, Any, List
from backend.services.gateway_service import _execute_system_sql
from backend.services.access_service import get_all_principals

router = APIRouter(prefix="/user-analytics", tags=["user-analytics"], dependencies=[Depends(get_current_user)])


def _get_user_kpis(days: int) -> Dict[str, Any]:
    """Aggregate user-level KPIs from system.serving.endpoint_usage."""
    rows = _execute_system_sql(f"""
        SELECT
            COUNT(DISTINCT CASE WHEN request_time >= current_timestamp() - INTERVAL 24 HOURS THEN requester END) AS active_users_24h,
            COUNT(DISTINCT CASE WHEN request_time >= current_timestamp() - INTERVAL 7 DAYS  THEN requester END) AS active_users_7d,
            COUNT(DISTINCT requester)                                                                             AS active_users_period,
            COUNT(*)                                                                                              AS total_requests,
            COALESCE(SUM(input_token_count + output_token_count), 0)                                              AS total_tokens
        FROM system.serving.endpoint_usage
        WHERE request_time >= date_sub(current_date(), {days})
          AND requester IS NOT NULL
    """)
    if not rows:
        return {
            "active_users_24h": 0, "active_users_7d": 0, "active_users_period": 0,
            "total_requests": 0, "unique_agents": 0, "total_tokens": 0, "total_cost": 0,
        }
    r = rows[0]
    return {
        "active_users_24h": int(r.get("active_users_24h") or 0),
        "active_users_7d": int(r.get("active_users_7d") or 0),
        "active_users_period": int(r.get("active_users_period") or 0),
        "total_requests": int(r.get("total_requests") or 0),
        "unique_agents": 0,
        "total_tokens": int(r.get("total_tokens") or 0),
        "total_cost": 0,
    }


def _get_top_users(days: int, limit: int = 50) -> List[Dict[str, Any]]:
    """Top users ranked by request volume from system tables."""
    rows = _execute_system_sql(f"""
        SELECT
            u.requester                                                   AS user_id,
            COUNT(*)                                                      AS request_count,
            COUNT(DISTINCT se.endpoint_name)                              AS agents_used,
            COALESCE(SUM(u.input_token_count + u.output_token_count), 0)  AS total_tokens,
            MAX(u.request_time)                                           AS last_active
        FROM system.serving.endpoint_usage u
        JOIN system.serving.served_entities se
            ON u.served_entity_id = se.served_entity_id
        WHERE u.request_time >= date_sub(current_date(), {days})
          AND u.requester IS NOT NULL
        GROUP BY u.requester
        ORDER BY request_count DESC
        LIMIT {limit}
    """)
    return [
        {
            "user_id": r.get("user_id", ""),
            "request_count": int(r.get("request_count") or 0),
            "agents_used": int(r.get("agents_used") or 0),
            "total_tokens": int(r.get("total_tokens") or 0),
            "total_cost": 0,
            "avg_latency_ms": 0,
            "last_active": r.get("last_active", ""),
            "agent_list": [],
        }
        for r in rows
    ]


def _get_activity_heatmap(days: int) -> List[Dict[str, Any]]:
    """Request counts bucketed by day-of-week and hour-of-day from system tables."""
    rows = _execute_system_sql(f"""
        SELECT
            DAYOFWEEK(request_time) - 1  AS dow,
            HOUR(request_time)           AS hour,
            COUNT(*)                     AS count
        FROM system.serving.endpoint_usage
        WHERE request_time >= date_sub(current_date(), {days})
          AND requester IS NOT NULL
        GROUP BY DAYOFWEEK(request_time), HOUR(request_time)
        ORDER BY dow, hour
    """)
    return [
        {
            "dow": int(r.get("dow") or 0),
            "hour": int(r.get("hour") or 0),
            "count": int(r.get("count") or 0),
        }
        for r in rows
    ]


def _get_daily_active_users(days: int) -> List[Dict[str, Any]]:
    """Daily active user count for trend line from system tables."""
    rows = _execute_system_sql(f"""
        SELECT
            DATE(request_time)        AS day,
            COUNT(DISTINCT requester)  AS active_users,
            COUNT(*)                   AS requests
        FROM system.serving.endpoint_usage
        WHERE request_time >= date_sub(current_date(), {days})
          AND requester IS NOT NULL
        GROUP BY DATE(request_time)
        ORDER BY day
    """)
    return [
        {
            "day": r.get("day", ""),
            "active_users": int(r.get("active_users") or 0),
            "requests": int(r.get("requests") or 0),
        }
        for r in rows
    ]


def _get_user_agent_matrix(days: int) -> List[Dict[str, Any]]:
    """Which users use which agents from system tables."""
    rows = _execute_system_sql(f"""
        SELECT
            u.requester       AS user_id,
            se.endpoint_name  AS agent_id,
            COUNT(*)          AS request_count
        FROM system.serving.endpoint_usage u
        JOIN system.serving.served_entities se
            ON u.served_entity_id = se.served_entity_id
        WHERE u.request_time >= date_sub(current_date(), {days})
          AND u.requester IS NOT NULL
        GROUP BY u.requester, se.endpoint_name
        ORDER BY request_count DESC
        LIMIT 500
    """)
    return [
        {
            "user_id": r.get("user_id", ""),
            "agent_id": r.get("agent_id", ""),
            "request_count": int(r.get("request_count") or 0),
        }
        for r in rows
    ]


def _get_requests_per_user_distribution(days: int) -> List[Dict[str, Any]]:
    """Distribution buckets from system tables."""
    rows = _execute_system_sql(f"""
        WITH per_user AS (
            SELECT requester, COUNT(*) AS cnt
            FROM system.serving.endpoint_usage
            WHERE request_time >= date_sub(current_date(), {days})
              AND requester IS NOT NULL
            GROUP BY requester
        )
        SELECT
            CASE
                WHEN cnt = 1    THEN '1'
                WHEN cnt <= 5   THEN '2-5'
                WHEN cnt <= 20  THEN '6-20'
                WHEN cnt <= 100 THEN '21-100'
                ELSE '100+'
            END AS bucket,
            COUNT(*) AS user_count
        FROM per_user
        GROUP BY bucket
        ORDER BY MIN(cnt)
    """)
    return [
        {
            "bucket": r.get("bucket", ""),
            "user_count": int(r.get("user_count") or 0),
        }
        for r in rows
    ]


@router.get("/page-data")
def user_analytics_page_data(
    days: int = Query(default=30, ge=1, le=365),
) -> Dict[str, Any]:
    """Composite endpoint returning all data for the User Analytics page."""
    principals = []
    try:
        principals = get_all_principals(days=days)
    except Exception:
        pass

    return {
        "kpis": _get_user_kpis(days),
        "top_users": _get_top_users(days),
        "heatmap": _get_activity_heatmap(days),
        "daily_active_users": _get_daily_active_users(days),
        "user_agent_matrix": _get_user_agent_matrix(days),
        "distribution": _get_requests_per_user_distribution(days),
        "principals": principals,
    }
