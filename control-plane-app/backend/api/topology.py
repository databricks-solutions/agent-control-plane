"""Topology API — exposes the agent dependency graph."""
from fastapi import APIRouter, Depends
from backend.utils.auth import get_current_user
from backend.services.topology_service import (
    build_topology,
    _fetch_agents,
    _fetch_tools,
    _build_agent_lookup,
    _match_lut,
)
from backend.services import mlflow_service

router = APIRouter(prefix="/topology", tags=["topology"], dependencies=[Depends(get_current_user)])


@router.get("")
async def get_topology(force: bool = False):
    """Return the full agent dependency graph as {nodes, edges, stats}."""
    return build_topology(force=force)


@router.get("/debug")
async def debug_topology():
    """Return raw agent configs and tool data for topology debugging."""
    agents = _fetch_agents()
    tools = _fetch_tools()
    return {
        "agents": [
            {
                "agent_id": a["agent_id"],
                "name": a["name"],
                "type": a.get("type"),
                "endpoint_name": a.get("endpoint_name"),
                "config_keys": list((a.get("config") or {}).keys()),
                "config": a.get("config"),
            }
            for a in agents
        ],
        "tools": [
            {
                "tool_id": t["tool_id"],
                "name": t["name"],
                "type": t["type"],
                "endpoint_name": t.get("endpoint_name"),
                "config_keys": list((t.get("config") or {}).keys()),
            }
            for t in tools
        ],
    }


@router.get("/debug/traces")
async def debug_topology_traces():
    """Diagnose why traces are or aren't producing edges.

    Returns:
      - experiments searched
      - sample traces with their raw structure (top-level keys, info fields,
        data keys, span count, span types)
      - which traces matched a known agent
    """
    agents = _fetch_agents()
    agent_lut = _build_agent_lookup(agents)
    agent_names = {a["endpoint_name"] or a["name"]: a["agent_id"] for a in agents}

    experiments = mlflow_service.search_experiments(200)
    traces = mlflow_service.search_traces(max_results=50)

    sample_traces = []
    for t in traces[:20]:
        info = t.get("info", {})
        data = t.get("data", {})
        meta = info.get("trace_metadata", {}) or {}
        model_id = meta.get("mlflow.modelId", "")
        serving_ep = meta.get("databricks.serving_endpoint_name", "")
        spans_inline = data.get("spans", [])

        matched_agent = None
        for candidate in (model_id, serving_ep):
            if candidate:
                nid = _match_lut(candidate, agent_lut)
                if nid:
                    matched_agent = nid
                    break

        # Try fetching spans for the first trace if inline is empty
        fetched_spans = []
        request_id = info.get("request_id", "")
        if not spans_inline and request_id:
            try:
                fetched_spans = mlflow_service.get_trace_spans(request_id)
            except Exception as e:
                fetched_spans = [{"error": str(e)}]

        sample_traces.append({
            "request_id": request_id,
            "experiment_id": info.get("experiment_id") or t.get("experiment_id"),
            "top_level_keys": list(t.keys()),
            "info_keys": list(info.keys()),
            "data_keys": list(data.keys()),
            "model_id": model_id,
            "serving_ep": serving_ep,
            "matched_agent": matched_agent,
            "spans_inline_count": len(spans_inline),
            "spans_inline_types": list({
                s.get("attributes", {}).get("mlflow.spanType", "?")
                for s in spans_inline[:10]
            }),
            "spans_fetched_count": len(fetched_spans),
            "spans_fetched_types": list({
                s.get("attributes", {}).get("mlflow.spanType", "?")
                for s in fetched_spans[:10]
                if isinstance(s, dict) and "error" not in s
            }),
        })

    return {
        "agent_count": len(agents),
        "agent_lut_size": len(agent_lut),
        "agent_endpoint_names": list(agent_names.keys())[:30],
        "experiments_found": len(experiments),
        "sample_experiments": [
            {"id": e.get("experiment_id"), "name": e.get("name")}
            for e in experiments[:20]
        ],
        "traces_found": len(traces),
        "sample_traces": sample_traces,
    }
