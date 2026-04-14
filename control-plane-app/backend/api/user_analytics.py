"""API routes for User Analytics & RBAC Dashboard.

Reads from Lakebase cache tables (populated by scheduled workflow from
system.serving.endpoint_usage). No live system table queries.
"""
import logging
from fastapi import APIRouter, Depends, Query
from backend.utils.auth import get_current_user
from typing import Dict, Any, List
from backend.database import execute_query
from backend.services.access_service import get_all_principals

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/user-analytics", tags=["user-analytics"], dependencies=[Depends(get_current_user)])


def _get_user_kpis(days: int) -> Dict[str, Any]:
    """Aggregate user-level KPIs from Lakebase cache."""
    try:
        rows = execute_query(
            """SELECT
                COUNT(DISTINCT CASE WHEN usage_date >= CURRENT_DATE - INTERVAL '1 day' THEN requester END) AS active_users_24h,
                COUNT(DISTINCT CASE WHEN usage_date >= CURRENT_DATE - INTERVAL '7 days' THEN requester END) AS active_users_7d,
                COUNT(DISTINCT requester) AS active_users_period,
                COALESCE(SUM(request_count), 0) AS total_requests,
                COALESCE(SUM(total_tokens), 0) AS total_tokens
               FROM user_analytics_daily
               WHERE usage_date >= CURRENT_DATE - INTERVAL '%s days'""",
            (days,),
        )
    except Exception as exc:
        logger.warning("User KPIs cache read failed: %s", exc)
        rows = []
    if not rows:
        return {"active_users_24h": 0, "active_users_7d": 0, "active_users_period": 0,
                "total_requests": 0, "unique_agents": 0, "total_tokens": 0, "total_cost": 0}
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
    """Top users ranked by request volume from cache."""
    try:
        rows = execute_query(
            """SELECT requester AS user_id,
                      SUM(request_count) AS request_count,
                      COUNT(DISTINCT endpoint_name) AS agents_used,
                      SUM(total_tokens) AS total_tokens,
                      MAX(usage_date) AS last_active
               FROM user_analytics_daily
               WHERE usage_date >= CURRENT_DATE - INTERVAL '%s days'
               GROUP BY requester ORDER BY request_count DESC LIMIT %s""",
            (days, limit),
        )
    except Exception as exc:
        logger.warning("Top users cache read failed: %s", exc)
        rows = []
    return [
        {
            "user_id": r.get("user_id", ""),
            "request_count": int(r.get("request_count") or 0),
            "agents_used": int(r.get("agents_used") or 0),
            "total_tokens": int(r.get("total_tokens") or 0),
            "total_cost": 0,
            "avg_latency_ms": 0,
            "last_active": str(r.get("last_active", "")),
            "agent_list": [],
        }
        for r in rows
    ]


def _get_activity_heatmap(days: int) -> List[Dict[str, Any]]:
    """Request counts by day-of-week and hour from cache."""
    # Use the closest cached period (30 or 90)
    period = 30 if days <= 30 else 90
    try:
        rows = execute_query(
            """SELECT dow, hour, request_count AS count
               FROM user_analytics_heatmap
               WHERE period_days = %s ORDER BY dow, hour""",
            (period,),
        )
    except Exception as exc:
        logger.warning("Heatmap cache read failed: %s", exc)
        rows = []
    return [{"dow": int(r.get("dow") or 0), "hour": int(r.get("hour") or 0),
             "count": int(r.get("count") or 0)} for r in rows]


def _get_daily_active_users(days: int) -> List[Dict[str, Any]]:
    """Daily active user count from cache."""
    try:
        rows = execute_query(
            """SELECT CAST(usage_date AS TEXT) AS day,
                      COUNT(DISTINCT requester) AS active_users,
                      SUM(request_count) AS requests
               FROM user_analytics_daily
               WHERE usage_date >= CURRENT_DATE - INTERVAL '%s days'
               GROUP BY usage_date ORDER BY usage_date""",
            (days,),
        )
    except Exception as exc:
        logger.warning("Daily active users cache read failed: %s", exc)
        rows = []
    return [{"day": r.get("day", ""), "active_users": int(r.get("active_users") or 0),
             "requests": int(r.get("requests") or 0)} for r in rows]


def _get_user_agent_matrix(days: int) -> List[Dict[str, Any]]:
    """Which users use which agents from cache."""
    try:
        rows = execute_query(
            """SELECT requester AS user_id, endpoint_name AS agent_id,
                      SUM(request_count) AS request_count
               FROM user_analytics_daily
               WHERE usage_date >= CURRENT_DATE - INTERVAL '%s days'
               GROUP BY requester, endpoint_name
               ORDER BY request_count DESC LIMIT 500""",
            (days,),
        )
    except Exception as exc:
        logger.warning("User agent matrix cache read failed: %s", exc)
        rows = []
    return [{"user_id": r.get("user_id", ""), "agent_id": r.get("agent_id", ""),
             "request_count": int(r.get("request_count") or 0)} for r in rows]


def _get_requests_per_user_distribution(days: int) -> List[Dict[str, Any]]:
    """Distribution buckets from cache."""
    try:
        rows = execute_query(
            """WITH per_user AS (
                SELECT requester, SUM(request_count) AS cnt
                FROM user_analytics_daily
                WHERE usage_date >= CURRENT_DATE - INTERVAL '%s days'
                GROUP BY requester
            )
            SELECT
                CASE WHEN cnt = 1 THEN '1' WHEN cnt <= 5 THEN '2-5'
                     WHEN cnt <= 20 THEN '6-20' WHEN cnt <= 100 THEN '21-100'
                     WHEN cnt <= 500 THEN '101-500' WHEN cnt <= 1000 THEN '501-1K'
                     WHEN cnt <= 5000 THEN '1K-5K' WHEN cnt <= 10000 THEN '5K-10K'
                     WHEN cnt <= 50000 THEN '10K-50K' WHEN cnt <= 100000 THEN '50K-100K'
                     ELSE '100K+' END AS bucket,
                COUNT(*) AS user_count
            FROM per_user GROUP BY bucket ORDER BY MIN(cnt)""",
            (days,),
        )
    except Exception as exc:
        logger.warning("Distribution cache read failed: %s", exc)
        rows = []
    return [{"bucket": r.get("bucket", ""), "user_count": int(r.get("user_count") or 0)} for r in rows]


@router.get("/page-data")
def user_analytics_page_data(
    days: int = Query(default=30, ge=1, le=365),
) -> Dict[str, Any]:
    """Composite endpoint returning all data for the User Analytics page (from cache)."""
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
