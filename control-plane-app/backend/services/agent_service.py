"""Service for agent operations."""
from typing import List, Optional, Dict, Any
from backend.database import execute_query, execute_one, execute_update
from backend.models.agent import AgentOut, AgentListOut, AgentUpdate
from backend.utils.access_scope import sees_deploy_workspace, workspace_is_allowed
import json


def get_all_agents(
    active_only: bool = False,
    allowed_workspace_ids: Optional[List[str]] = None,
) -> List[AgentListOut]:
    """Get all agents from the local registry (deploy workspace)."""
    if allowed_workspace_ids is not None and not sees_deploy_workspace(allowed_workspace_ids):
        return []
    query = "SELECT agent_id, name, type, endpoint_type, endpoint_status, app_url, is_active FROM agent_registry"
    if active_only:
        query += " WHERE is_active = TRUE"
    query += " ORDER BY name"
    
    results = execute_query(query)
    return [AgentListOut(**dict(row)) for row in results]


def get_all_agents_full(
    active_only: bool = False,
    allowed_workspace_ids: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """Get all agents with full detail (tags, config, description, etc.)."""
    if allowed_workspace_ids is not None and not sees_deploy_workspace(allowed_workspace_ids):
        return []
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


def get_agent_by_id(
    agent_id: str,
    allowed_workspace_ids: Optional[List[str]] = None,
) -> Optional[AgentOut]:
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
        if allowed_workspace_ids is not None and not sees_deploy_workspace(allowed_workspace_ids):
            return None
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
        if not workspace_is_allowed(d.get("workspace_id"), allowed_workspace_ids):
            return None
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


def get_agent_metrics(
    agent_id: str,
    hours: int = 24,
    allowed_workspace_ids: Optional[List[str]] = None,
) -> Dict[str, Any]:
    """Performance metrics for an agent, keyed by its serving endpoint.

    Built from tables the discovery workflow actually populates. (This used to
    read ``request_logs``, which nothing writes, so the card was always empty.)

    Endpoint names are unique only *within* a workspace (e.g. foundation-model
    endpoint names repeat across every workspace), so every number is scoped to
    the agent's own workspace:
      - requests:  billing_token_daily  (system.serving.endpoint_usage) — by workspace_id
      - cost:      billing_serving_daily (system.billing.usage)         — by workspace_id
      - error rate / p50 / p95 latency: gateway_usage_daily and uag_usage_summary
        have NO workspace_id, so they're used only when the endpoint name exists in
        exactly one workspace; otherwise None rather than a cross-workspace blend.
        Latency is also only present for Unity Gateway-routed endpoints (30d).
    Tables are day-grain, so the window is ``hours`` rounded up to whole days.
    A metric with no trustworthy source is None (rendered "—"), never a fake 0.
    """
    agent = get_agent_by_id(agent_id, allowed_workspace_ids=allowed_workspace_ids)
    if not agent:
        return {}
    days = max(1, -(-hours // 24))
    endpoint = getattr(agent, "endpoint_name", None)
    out: Dict[str, Any] = {
        "endpoint_name": endpoint, "workspace_id": None, "window_days": days,
        "request_count": None, "error_count": None, "error_rate": None,
        "p50_latency": None, "p95_latency": None, "total_cost": None,
    }
    if not endpoint:
        return out

    def _one(sql: str, params: tuple) -> Dict[str, Any]:
        try:
            row = execute_one(sql, params)
            return dict(row) if row else {}
        except Exception:
            return {}  # table not synced yet → leave the metric as None

    # Agent's workspace: discovered agents carry it; registry agents live in the
    # deploy workspace.
    ws = _one("SELECT workspace_id FROM discovered_agents WHERE agent_id = %s", (agent_id,)).get("workspace_id")
    if not ws:
        try:
            from backend.services.billing_service import get_current_workspace_id
            ws = get_current_workspace_id()
        except Exception:
            ws = None
    ws = str(ws) if ws else None
    out["workspace_id"] = ws

    if ws:
        usage = _one(
            """SELECT SUM(request_count) AS req FROM billing_token_daily
               WHERE workspace_id = %s AND endpoint_name = %s AND usage_date >= CURRENT_DATE - %s""",
            (ws, endpoint, days),
        )
        if usage.get("req") is not None:
            out["request_count"] = int(usage["req"] or 0)
        cost = _one(
            """SELECT SUM(total_cost_usd) AS cost FROM billing_serving_daily
               WHERE workspace_id = %s AND endpoint_name = %s AND usage_date >= CURRENT_DATE - %s""",
            (ws, endpoint, days),
        )
        if cost.get("cost") is not None:
            out["total_cost"] = float(cost["cost"])

    # Only trust the workspace-less tables when the name provably can't collide:
    # it must appear in exactly one workspace, and that workspace must be this
    # agent's. (0 rows = unknown, not "unique" — the tables sync independently.)
    # Uniqueness is checked across serving_endpoints_inventory (every current
    # endpoint in every workspace, traffic or not) AND billing_token_daily (recent
    # usage, incl. since-deleted endpoints), so an idle same-named endpoint
    # elsewhere still counts as a collision.
    # TODO(phase B): carry workspace_id into gateway_usage_daily / uag_usage_summary
    # and filter on it directly instead of this guard.
    owner = _one(
        """SELECT COUNT(DISTINCT workspace_id) AS n, MIN(workspace_id) AS w FROM (
               SELECT workspace_id FROM billing_token_daily WHERE endpoint_name = %s
               UNION
               SELECT workspace_id FROM serving_endpoints_inventory WHERE endpoint_name = %s
           ) s""",
        (endpoint, endpoint),
    )
    if ws and owner.get("n") is not None and int(owner["n"]) == 1 and str(owner.get("w")) == ws:
        errs = _one(
            """SELECT SUM(request_count) AS req, SUM(error_count) AS err FROM gateway_usage_daily
               WHERE endpoint_name = %s AND usage_date >= CURRENT_DATE - %s""",
            (endpoint, days),
        )
        if errs.get("req"):
            err = int(errs.get("err") or 0)
            out["error_count"] = err
            out["error_rate"] = round(err * 100.0 / int(errs["req"]), 2)
        lat = _one(
            "SELECT p50_latency_ms, p95_latency_ms FROM uag_usage_summary WHERE endpoint_name = %s",
            (endpoint,),
        )
        if lat.get("p95_latency_ms"):
            out["p50_latency"] = float(lat.get("p50_latency_ms") or 0) or None
            out["p95_latency"] = float(lat["p95_latency_ms"])
    return out
