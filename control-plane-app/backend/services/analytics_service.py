"""Service for analytics calculations."""
from typing import List, Dict, Any
from datetime import datetime, timedelta
from backend.database import execute_query, execute_one
from backend.models.analytics import TimeSeriesPoint, PerformanceMetrics, UsageMetrics, CostMetrics, HealthMetrics


def get_performance_metrics(days: int = 30) -> PerformanceMetrics:
    """Get performance analytics."""
    # Response time time series
    query = """
        SELECT 
            DATE_TRUNC('day', timestamp) as day,
            AVG(latency_ms) as avg_latency,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY latency_ms) as p50,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) as p95,
            PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms) as p99
        FROM request_logs
        WHERE timestamp >= NOW() - (INTERVAL '1 day' * %s)
          AND latency_ms IS NOT NULL
        GROUP BY DATE_TRUNC('day', timestamp)
        ORDER BY day
    """
    results = execute_query(query, (days,))
    
    response_time_series = [
        TimeSeriesPoint(timestamp=dict(row)['day'].isoformat() if hasattr(dict(row)['day'], 'isoformat') else str(dict(row)['day']), value=float(dict(row)['avg_latency']))
        for row in results if dict(row).get('avg_latency')
    ]
    p50_series = [
        TimeSeriesPoint(timestamp=dict(row)['day'].isoformat() if hasattr(dict(row)['day'], 'isoformat') else str(dict(row)['day']), value=float(dict(row)['p50']))
        for row in results if dict(row).get('p50')
    ]
    p95_series = [
        TimeSeriesPoint(timestamp=dict(row)['day'].isoformat() if hasattr(dict(row)['day'], 'isoformat') else str(dict(row)['day']), value=float(dict(row)['p95']))
        for row in results if dict(row).get('p95')
    ]
    p99_series = [
        TimeSeriesPoint(timestamp=dict(row)['day'].isoformat() if hasattr(dict(row)['day'], 'isoformat') else str(dict(row)['day']), value=float(dict(row)['p99']))
        for row in results if dict(row).get('p99')
    ]
    
    # Response time by agent type
    query = """
        SELECT 
            a.type,
            AVG(r.latency_ms) as avg_latency
        FROM request_logs r
        JOIN agent_registry a ON r.agent_id = a.agent_id
        WHERE r.timestamp >= NOW() - (INTERVAL '1 day' * %s)
          AND r.latency_ms IS NOT NULL
        GROUP BY a.type
    """
    results = execute_query(query, (days,))
    response_time_by_agent_type = {dict(row)['type']: float(dict(row)['avg_latency']) for row in results}
    
    # Throughput series
    query = """
        SELECT 
            DATE_TRUNC('hour', timestamp) as hour,
            COUNT(*) as request_count
        FROM request_logs
        WHERE timestamp >= NOW() - (INTERVAL '1 day' * %s)
        GROUP BY DATE_TRUNC('hour', timestamp)
        ORDER BY hour
    """
    results = execute_query(query, (days,))
    throughput_series = [
        TimeSeriesPoint(timestamp=row['hour'].isoformat() if hasattr(row['hour'], 'isoformat') else str(row['hour']), value=int(row['request_count']))
        for row in results
    ]
    
    return PerformanceMetrics(
        response_time_series=response_time_series,
        response_time_by_agent_type=response_time_by_agent_type,
        throughput_series=throughput_series,
        p50_series=p50_series,
        p95_series=p95_series,
        p99_series=p99_series
    )


def get_usage_metrics(days: int = 30) -> UsageMetrics:
    """Get usage analytics."""
    # Requests by agent
    query = """
        SELECT 
            agent_id,
            COUNT(*) as request_count
        FROM request_logs
        WHERE timestamp >= NOW() - (INTERVAL '1 day' * %s)
        GROUP BY agent_id
        ORDER BY request_count DESC
    """
    results = execute_query(query, (days,))
    requests_by_agent = {dict(row)['agent_id']: int(dict(row)['request_count']) for row in results}
    
    # Usage over time
    query = """
        SELECT 
            DATE_TRUNC('day', timestamp) as day,
            COUNT(*) as request_count
        FROM request_logs
        WHERE timestamp >= NOW() - (INTERVAL '1 day' * %s)
        GROUP BY DATE_TRUNC('day', timestamp)
        ORDER BY day
    """
    results = execute_query(query, (days,))
    usage_over_time = [
        TimeSeriesPoint(timestamp=row['day'].isoformat() if hasattr(row['day'], 'isoformat') else str(row['day']), value=int(row['request_count']))
        for row in results
    ]
    
    # Top agents
    query = """
        SELECT 
            a.agent_id,
            a.name,
            COUNT(*) as request_count
        FROM request_logs r
        JOIN agent_registry a ON r.agent_id = a.agent_id
        WHERE r.timestamp >= NOW() - (INTERVAL '1 day' * %s)
        GROUP BY a.agent_id, a.name
        ORDER BY request_count DESC
        LIMIT 10
    """
    top_agents = [dict(row) for row in execute_query(query, (days,))]
    
    return UsageMetrics(
        requests_by_agent=requests_by_agent,
        usage_over_time=usage_over_time,
        usage_by_hour={},
        usage_by_day={},
        top_agents=top_agents,
        active_users_over_time=[]
    )


def get_cost_metrics(days: int = 30) -> CostMetrics:
    """Get cost analytics."""
    # Total cost
    query = """
        SELECT COALESCE(SUM(cost_usd), 0) as total_cost
        FROM request_logs
        WHERE timestamp >= NOW() - (INTERVAL '1 day' * %s)
    """
    result = execute_one(query, (days,))
    total_cost = float(dict(result)['total_cost']) if result else 0.0
    
    # Cost by agent
    query = """
        SELECT 
            agent_id,
            COALESCE(SUM(cost_usd), 0) as total_cost
        FROM request_logs
        WHERE timestamp >= NOW() - (INTERVAL '1 day' * %s)
        GROUP BY agent_id
    """
    results = execute_query(query, (days,))
    cost_by_agent = {dict(row)['agent_id']: float(dict(row)['total_cost']) for row in results}
    
    # Cost by agent type
    query = """
        SELECT 
            a.type,
            COALESCE(SUM(r.cost_usd), 0) as total_cost
        FROM request_logs r
        JOIN agent_registry a ON r.agent_id = a.agent_id
        WHERE r.timestamp >= NOW() - (INTERVAL '1 day' * %s)
        GROUP BY a.type
    """
    results = execute_query(query, (days,))
    cost_by_agent_type = {dict(row)['type']: float(dict(row)['total_cost']) for row in results}
    
    # Cost trend
    query = """
        SELECT 
            DATE_TRUNC('day', timestamp) as day,
            COALESCE(SUM(cost_usd), 0) as daily_cost
        FROM request_logs
        WHERE timestamp >= NOW() - (INTERVAL '1 day' * %s)
        GROUP BY DATE_TRUNC('day', timestamp)
        ORDER BY day
    """
    results = execute_query(query, (days,))
    cost_trend = [
        TimeSeriesPoint(timestamp=row['day'].isoformat() if hasattr(row['day'], 'isoformat') else str(row['day']), value=float(row['daily_cost']))
        for row in results
    ]
    
    # Cost per request
    query = """
        SELECT 
            COUNT(*) as request_count,
            COALESCE(SUM(cost_usd), 0) as total_cost
        FROM request_logs
        WHERE timestamp >= NOW() - (INTERVAL '1 day' * %s)
    """
    result = execute_one(query, (days,))
    result_dict = dict(result) if result else {}
    if result_dict and result_dict.get('request_count', 0) > 0:
        cost_per_request = float(result_dict['total_cost']) / int(result_dict['request_count'])
    else:
        cost_per_request = 0.0
    
    return CostMetrics(
        total_cost=total_cost,
        cost_by_agent=cost_by_agent,
        cost_by_agent_type=cost_by_agent_type,
        cost_trend=cost_trend,
        cost_per_request=cost_per_request
    )


def get_health_metrics() -> HealthMetrics:
    """Get health monitoring data."""
    # Agent health
    query = """
        SELECT 
            a.agent_id,
            a.name,
            a.endpoint_status,
            COUNT(r.request_id) as request_count,
            AVG(r.latency_ms) as avg_latency,
            COUNT(*) FILTER (WHERE r.status_code >= 400) * 100.0 / NULLIF(COUNT(*), 0) as error_rate
        FROM agent_registry a
        LEFT JOIN request_logs r ON a.agent_id = r.agent_id 
          AND r.timestamp >= NOW() - INTERVAL '24 hours'
        WHERE a.is_active = TRUE
        GROUP BY a.agent_id, a.name, a.endpoint_status
    """
    agent_health = [dict(row) for row in execute_query(query)]
    
    # Error rate trend
    query = """
        SELECT 
            DATE_TRUNC('hour', timestamp) as hour,
            COUNT(*) FILTER (WHERE status_code >= 400) * 100.0 / NULLIF(COUNT(*), 0) as error_rate
        FROM request_logs
        WHERE timestamp >= NOW() - INTERVAL '7 days'
        GROUP BY DATE_TRUNC('hour', timestamp)
        ORDER BY hour
    """
    results = execute_query(query)
    error_rate_trend = [
        TimeSeriesPoint(timestamp=row['hour'].isoformat() if hasattr(row['hour'], 'isoformat') else str(row['hour']), value=float(row['error_rate'] or 0))
        for row in results
    ]
    
    # Recent errors
    query = """
        SELECT request_id, agent_id, timestamp, status_code, error_message
        FROM request_logs
        WHERE status_code >= 400
        ORDER BY timestamp DESC
        LIMIT 50
    """
    recent_errors = [dict(row) for row in execute_query(query)]
    
    return HealthMetrics(
        agent_health=agent_health,
        error_rate_trend=error_rate_trend,
        error_types={},
        errors_by_agent={},
        recent_errors=recent_errors
    )
