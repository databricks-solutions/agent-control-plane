"""Service for database queries."""
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from backend.database import execute_query, execute_one
from backend.models.request import RequestFilters


def get_recent_requests(limit: int = 20) -> List[Dict[str, Any]]:
    """Get recent requests."""
    query = """
        SELECT request_id, agent_id, user_id, timestamp, latency_ms,
               status_code, cost_usd, error_message,
               input_tokens, output_tokens
        FROM request_logs
        ORDER BY timestamp DESC
        LIMIT %s
    """
    return [dict(row) for row in execute_query(query, (limit,))]


def get_requests_with_filters(filters: RequestFilters) -> List[Dict[str, Any]]:
    """Get requests with filters."""
    conditions = []
    params = []
    
    if filters.agent_id:
        conditions.append("agent_id = %s")
        params.append(filters.agent_id)
    if filters.user_id:
        conditions.append("user_id = %s")
        params.append(filters.user_id)
    if filters.start_time:
        conditions.append("timestamp >= %s")
        params.append(filters.start_time)
    if filters.end_time:
        conditions.append("timestamp <= %s")
        params.append(filters.end_time)
    if filters.status_code:
        conditions.append("status_code = %s")
        params.append(filters.status_code)
    
    where_clause = " AND ".join(conditions) if conditions else "1=1"
    
    query = f"""
        SELECT request_id, agent_id, user_id, timestamp, latency_ms,
               status_code, cost_usd, error_message
        FROM request_logs
        WHERE {where_clause}
        ORDER BY timestamp DESC
        LIMIT %s OFFSET %s
    """
    params.extend([filters.limit, filters.offset])
    
    return [dict(row) for row in execute_query(query, tuple(params))]


def get_kpi_overview() -> Dict[str, Any]:
    """Get overview KPIs."""
    queries = {
        "total_requests_24h": "SELECT COUNT(*) as count FROM request_logs WHERE timestamp >= NOW() - INTERVAL '24 hours'",
        "total_requests_7d": "SELECT COUNT(*) as count FROM request_logs WHERE timestamp >= NOW() - INTERVAL '7 days'",
        "total_requests_30d": "SELECT COUNT(*) as count FROM request_logs WHERE timestamp >= NOW() - INTERVAL '30 days'",
        "avg_latency": """
            SELECT AVG(latency_ms) as value
            FROM request_logs
            WHERE timestamp >= NOW() - INTERVAL '24 hours' AND latency_ms IS NOT NULL
        """,
        "p50_latency": """
            SELECT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY latency_ms) as value
            FROM request_logs
            WHERE timestamp >= NOW() - INTERVAL '24 hours' AND latency_ms IS NOT NULL
        """,
        "p95_latency": """
            SELECT PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) as value
            FROM request_logs
            WHERE timestamp >= NOW() - INTERVAL '24 hours' AND latency_ms IS NOT NULL
        """,
        "error_rate": """
            SELECT COUNT(*) FILTER (WHERE status_code >= 400) * 100.0 / NULLIF(COUNT(*), 0) as value
            FROM request_logs
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
        """,
        "total_cost_24h": """
            SELECT COALESCE(SUM(cost_usd), 0) as value
            FROM request_logs
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
        """,
        "total_cost_7d": """
            SELECT COALESCE(SUM(cost_usd), 0) as value
            FROM request_logs
            WHERE timestamp >= NOW() - INTERVAL '7 days'
        """,
        "total_cost_30d": """
            SELECT COALESCE(SUM(cost_usd), 0) as value
            FROM request_logs
            WHERE timestamp >= NOW() - INTERVAL '30 days'
        """,
        "active_agents": """
            SELECT COUNT(*) as count
            FROM agent_registry
            WHERE is_active = TRUE AND endpoint_status = 'ONLINE'
        """,
        "total_agents": "SELECT COUNT(*) as count FROM agent_registry WHERE is_active = TRUE",
        "active_users_24h": """
            SELECT COUNT(DISTINCT user_id) as count
            FROM request_logs
            WHERE timestamp >= NOW() - INTERVAL '24 hours' AND user_id IS NOT NULL
        """,
    }
    
    results = {}
    for key, query in queries.items():
        result = execute_one(query)
        if result:
            if 'count' in result:
                results[key] = result['count']
            elif 'value' in result:
                results[key] = result['value']
            else:
                results[key] = list(result.values())[0] if result else 0
        else:
            results[key] = 0
    
    results['timestamp'] = datetime.now().isoformat()
    return results


def get_agent_performance_summary(hours: int = 24) -> List[Dict[str, Any]]:
    """Get performance summary for all agents."""
    query = """
        SELECT 
            a.agent_id,
            a.name,
            COUNT(r.request_id) as request_count,
            AVG(r.latency_ms) as avg_latency,
            COUNT(*) FILTER (WHERE r.status_code >= 400) * 100.0 / NULLIF(COUNT(*), 0) as error_rate,
            COALESCE(SUM(r.cost_usd), 0) as total_cost
        FROM agent_registry a
        LEFT JOIN request_logs r ON a.agent_id = r.agent_id 
          AND r.timestamp >= NOW() - (INTERVAL '1 hour' * %s)
        WHERE a.is_active = TRUE
        GROUP BY a.agent_id, a.name
        ORDER BY request_count DESC
    """
    return [dict(row) for row in execute_query(query, (hours,))]
