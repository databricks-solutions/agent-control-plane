"""Service for agent operations."""
from typing import List, Optional, Dict, Any
from backend.database import execute_query, execute_one, execute_update
from backend.models.agent import AgentOut, AgentListOut, AgentUpdate
import json


def get_all_agents(active_only: bool = False) -> List[AgentListOut]:
    """Get all agents."""
    query = "SELECT agent_id, name, type, endpoint_type, endpoint_status, app_url, is_active FROM agent_registry"
    if active_only:
        query += " WHERE is_active = TRUE"
    query += " ORDER BY name"
    
    results = execute_query(query)
    return [AgentListOut(**dict(row)) for row in results]


def get_all_agents_full(active_only: bool = False) -> List[Dict[str, Any]]:
    """Get all agents with full detail (tags, config, description, etc.)."""
    query = """
        SELECT agent_id, name, type, description, endpoint_name, endpoint_type,
               endpoint_status, app_id, app_url, version, created_at, updated_at,
               created_by, tags, config, is_active
        FROM agent_registry
    """
    if active_only:
        query += " WHERE is_active = TRUE"
    query += " ORDER BY name"

    results = execute_query(query)
    agents = []
    for row in results:
        d = dict(row)
        if d.get('tags') and isinstance(d['tags'], str):
            d['tags'] = json.loads(d['tags'])
        if d.get('config') and isinstance(d['config'], str):
            d['config'] = json.loads(d['config'])
        agents.append(d)
    return agents


def get_agent_by_id(agent_id: str) -> Optional[AgentOut]:
    """Get a single agent by ID — checks agent_registry first, then discovered_agents."""
    query = """
        SELECT agent_id, name, type, description, endpoint_name, endpoint_type,
               endpoint_status, app_id, app_url, version, created_at, updated_at,
               created_by, tags, config, is_active
        FROM agent_registry
        WHERE agent_id = %s
    """
    result = execute_one(query, (agent_id,))
    if result:
        agent_dict = dict(result)
        if agent_dict.get('tags') and isinstance(agent_dict['tags'], str):
            agent_dict['tags'] = json.loads(agent_dict['tags'])
        if agent_dict.get('config') and isinstance(agent_dict['config'], str):
            agent_dict['config'] = json.loads(agent_dict['config'])
        return AgentOut(**agent_dict)

    # Fall back to discovered_agents (auto-discovered, not manually registered)
    discovered = execute_one(
        "SELECT * FROM discovered_agents WHERE agent_id = %s", (agent_id,)
    )
    if discovered:
        d = dict(discovered)
        if d.get('config') and isinstance(d['config'], str):
            d['config'] = json.loads(d['config'])
        return AgentOut(
            agent_id=d['agent_id'],
            name=d.get('name', ''),
            type=d.get('type', ''),
            description=d.get('description'),
            endpoint_name=d.get('endpoint_name'),
            endpoint_type=None,
            endpoint_status=d.get('endpoint_status'),
            app_id=None,
            app_url=None,
            version=None,
            created_at=d.get('last_synced'),
            updated_at=d.get('last_synced'),
            created_by=d.get('creator'),
            tags={},
            config=d.get('config'),
            is_active=True,
        )
    return None


def update_agent(agent_id: str, update_data: AgentUpdate) -> bool:
    """Update an agent."""
    updates = []
    params = []
    
    if update_data.name is not None:
        updates.append("name = %s")
        params.append(update_data.name)
    if update_data.description is not None:
        updates.append("description = %s")
        params.append(update_data.description)
    if update_data.endpoint_status is not None:
        updates.append("endpoint_status = %s")
        params.append(update_data.endpoint_status)
    if update_data.is_active is not None:
        updates.append("is_active = %s")
        params.append(update_data.is_active)
    if update_data.tags is not None:
        updates.append("tags = %s::jsonb")
        params.append(json.dumps(update_data.tags))
    if update_data.config is not None:
        updates.append("config = %s::jsonb")
        params.append(json.dumps(update_data.config))
    
    if not updates:
        return False
    
    updates.append("updated_at = CURRENT_TIMESTAMP")
    params.append(agent_id)
    
    query = f"""
        UPDATE agent_registry
        SET {', '.join(updates)}
        WHERE agent_id = %s
    """
    
    rowcount = execute_update(query, tuple(params))
    return rowcount > 0


def get_agent_metrics(agent_id: str, hours: int = 24) -> Dict[str, Any]:
    """Get performance metrics for an agent."""
    query = """
        SELECT 
            COUNT(*) as request_count,
            AVG(latency_ms) as avg_latency,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY latency_ms) as p50_latency,
            PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms) as p95_latency,
            COUNT(*) FILTER (WHERE status_code >= 400) * 100.0 / NULLIF(COUNT(*), 0) as error_rate,
            SUM(cost_usd) as total_cost
        FROM request_logs
        WHERE agent_id = %s
          AND timestamp >= NOW() - (INTERVAL '1 hour' * %s)
    """
    result = execute_one(query, (agent_id, hours))
    return dict(result) if result else {}
