"""Builds the Agent Dependency Graph topology.

Returns a {nodes, edges, stats} dict suitable for React Flow.

Data sources:
  1. agent_registry  – all known agents + endpoint_name
  2. tool_registry   – MCP servers + UC functions
  3. MLflow traces   – actual agent→tool call edges via span analysis
  4. agent config    – MAS→child-agent routing edges from config JSONB
  5. app resources   – app agent→serving_endpoint edges from config.resources
"""
from __future__ import annotations

import json
import re
import time
from typing import Any, Dict, List, Optional, Set

from backend.database import execute_query
from backend.services import mlflow_service

import logging

logger = logging.getLogger(__name__)

# ── In-memory TTL cache (5 min) ───────────────────────────────────────────────
_cache: Optional[Dict[str, Any]] = None
_cache_ts: float = 0.0
_CACHE_TTL = 300  # seconds


def _fresh() -> bool:
    return _cache is not None and (time.time() - _cache_ts) < _CACHE_TTL


# ── ID helpers ────────────────────────────────────────────────────────────────

def _nid(kind: str, key: str) -> str:
    return f"{kind}:{key}"


def _eid(source: str, target: str) -> str:
    return f"{source}--{target}"


def _resolve_agent_type(agent: Dict[str, Any]) -> str:
    """Resolve agent type using endpoint_name prefix conventions.

    Agent Bricks naming convention:
      kie* → information_extraction
      mas* → multi_agent_supervisor
      ka*  → knowledge_assistant
    Falls back to the stored type field, or 'custom_agent' if absent.
    """
    ep = (agent.get("endpoint_name") or "").lower()
    if ep.startswith("kie"):
        return "information_extraction"
    if ep.startswith("mas"):
        return "multi_agent_supervisor"
    if ep.startswith("ka"):
        return "knowledge_assistant"
    return agent.get("type") or "custom_agent"


# ── Data fetchers ─────────────────────────────────────────────────────────────

def _fetch_agents() -> List[Dict[str, Any]]:
    """Fetch agents from discovered_agents (auto-discovered) table."""
    rows = execute_query("""
        SELECT agent_id, name, type, description, endpoint_name,
               endpoint_status, creator, config, model_name
        FROM discovered_agents
        ORDER BY name
    """)
    result = []
    for r in rows:
        d = dict(r)
        if isinstance(d.get("config"), str):
            try:
                d["config"] = json.loads(d["config"])
            except Exception:
                d["config"] = {}
        result.append(d)
    return result


def _fetch_tools() -> List[Dict[str, Any]]:
    rows = execute_query("SELECT * FROM tool_registry ORDER BY name")
    result = []
    for r in rows:
        d = dict(r)
        if isinstance(d.get("config"), str):
            try:
                d["config"] = json.loads(d["config"])
            except Exception:
                d["config"] = {}
        result.append(d)
    return result


# ── Edge extraction: agent→tool/agent via MLflow traces ───────────────────────

# Span types that represent calls to child agents (e.g. MAS → KA routing)
_AGENT_SPAN_TYPES = {"CHAIN", "AGENT"}
# Span types that represent tool/function calls (KA → UC function / MCP)
_TOOL_SPAN_TYPES = {"TOOL", "FUNCTION", "RETRIEVER"}


def _build_agent_lookup(agents: List[Dict[str, Any]]) -> Dict[str, str]:
    """Map every known identifier for an agent to its node_id.

    Indexes endpoint_name, name, and model_name (served entity path) so
    that span names like ``ka-abc-endpoint`` or ``agents.ka_abc_endpoint``
    both resolve to the same node.
    """
    lut: Dict[str, str] = {}
    for a in agents:
        nid = _nid("agent", a["agent_id"])
        for field in ("endpoint_name", "name", "model_name"):
            val = (a.get(field) or "").strip().lower()
            if not val:
                continue
            lut[val] = nid
            # Last slash-segment  (e.g. "endpoints:/ka-abc" → "ka-abc")
            seg = val.split("/")[-1]
            if seg and seg != val:
                lut[seg] = nid
            # Last dot-segment  (e.g. "agents.ka_abc_endpoint" → "ka_abc_endpoint")
            dot = val.split(".")[-1]
            if dot and dot != val:
                lut[dot] = nid
    return lut


def _build_tool_lookup(tools: List[Dict[str, Any]]) -> Dict[str, str]:
    """Map every known identifier for a tool to its node_id."""
    lut: Dict[str, str] = {}
    for t in tools:
        nid = _nid("tool", t["tool_id"])
        name = (t.get("name") or "").strip().lower()
        if name:
            lut[name] = nid
            # Last dot-segment  (e.g. "catalog.schema.func" → "func")
            dot = name.split(".")[-1]
            if dot and dot != name:
                lut[dot] = nid
        # endpoint_name (MCP server URL / serving endpoint)
        ep = (t.get("endpoint_name") or "").strip().lower()
        if ep:
            lut[ep] = nid
            seg = ep.split("/")[-1]
            if seg and seg != ep:
                lut[seg] = nid
        # full_name in config
        full = ((t.get("config") or {}).get("full_name") or "").strip().lower()
        if full:
            lut[full] = nid
            dot = full.split(".")[-1]
            if dot and dot != full:
                lut[dot] = nid
    return lut


def _match_lut(sn: str, lut: Dict[str, str]) -> Optional[str]:
    """Return node_id for sn using exact → suffix/prefix matching."""
    sn = sn.strip().lower()
    if not sn:
        return None
    # 1. Exact
    nid = lut.get(sn)
    if nid:
        return nid
    # 2. sn is a suffix of a lut key  (e.g. span "func" matches key "schema.func")
    for key, nid in lut.items():
        if key and (sn == key or key.endswith("." + sn) or key.endswith("/" + sn)):
            return nid
    # 3. a lut key is a suffix of sn  (e.g. span "catalog.schema.func" matches key "func")
    for key, nid in lut.items():
        if key and (sn.endswith("." + key) or sn.endswith("/" + key)):
            return nid
    return None


def _span_candidate_names(span: Dict[str, Any]) -> List[str]:
    """Return all strings from a span that could be an agent/tool name.

    Checks span name + several common attribute keys where Databricks /
    LangChain / LlamaIndex store the callable name.
    """
    candidates: List[str] = []
    name = span.get("name", "")
    if name:
        candidates.append(name)
    attrs = span.get("attributes", {}) or {}
    for key in (
        "mlflow.spanFunctionName",
        "db.operation.name",
        "gen_ai.request.model",
        "tool_name",
        "function_name",
        "name",
    ):
        v = attrs.get(key)
        if v and isinstance(v, str):
            candidates.append(v)
    # spanInputs sometimes contains {"name": "..."} JSON
    inputs_raw = attrs.get("mlflow.spanInputs", "")
    if inputs_raw:
        try:
            inp = json.loads(inputs_raw) if isinstance(inputs_raw, str) else inputs_raw
            if isinstance(inp, dict):
                for k in ("name", "function_name", "tool_name", "agent_name"):
                    v = inp.get(k)
                    if v and isinstance(v, str):
                        candidates.append(v)
        except Exception:
            pass
    return candidates


def _build_experiment_agent_map(
    agents: List[Dict[str, Any]],
) -> Dict[str, str]:
    """Build a map from MLflow experiment_id → agent node_id.

    Databricks serving endpoints auto-create MLflow experiments whose names
    contain or end with the endpoint name (e.g. "/Users/x/my-endpoint" or
    "/mlflow/experiments/serving-endpoints/my-endpoint").  We use this to
    attribute traces to the correct agent when mlflow.modelId is absent.
    """
    agent_lut = _build_agent_lookup(agents)
    exp_to_agent: Dict[str, str] = {}
    try:
        exps = mlflow_service.search_experiments(200)
        for exp in exps:
            eid = exp.get("experiment_id", "")
            name = (exp.get("name") or "").strip()
            if not eid or not name:
                continue
            # Try the last path segment first (most specific)
            segment = name.rstrip("/").split("/")[-1]
            nid = _match_lut(segment, agent_lut) or _match_lut(name, agent_lut)
            if nid:
                exp_to_agent[eid] = nid
    except Exception as exc:
        logger.warning("Topology experiment→agent map failed: %s", exc)
    return exp_to_agent


def _extract_trace_edges(
    agents: List[Dict[str, Any]],
    tools: List[Dict[str, Any]],
    seen_node_ids: Set[str],
    synthetic_nodes: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Parse recent MLflow traces to find agent→tool and agent→agent call edges.

    Strategy per trace:
      1. Identify which agent produced the trace via mlflow.modelId metadata.
         If that fails, fall back to the experiment name (serving endpoint
         experiments are often named after the endpoint).
      2. For each span:
         - CHAIN / AGENT span types → try to match to a known child agent
           (handles MAS routing to KA children).
         - TOOL / FUNCTION / RETRIEVER spans → match to known tools
           (handles KA → UC function / MCP server calls).
         - Unmatched tool-like spans → emit a synthetic span_tool node.
    """
    agent_lut = _build_agent_lookup(agents)
    tool_lut = _build_tool_lookup(tools)
    exp_agent_map = _build_experiment_agent_map(agents)

    # (source, target) → edge accumulator
    edge_acc: Dict[tuple, Dict[str, Any]] = {}

    def _acc(src: str, tgt: str, label: str, animated: bool) -> None:
        key = (src, tgt)
        if key not in edge_acc:
            edge_acc[key] = {
                "id": _eid(src, tgt),
                "source": src,
                "target": tgt,
                "label": label,
                "call_count": 0,
                "animated": animated,
            }
        edge_acc[key]["call_count"] += 1

    matched_traces = 0
    total_traces = 0
    try:
        traces = mlflow_service.search_traces(max_results=200)
        total_traces = len(traces)
        for trace in traces:
            info = trace.get("info", {})
            data = trace.get("data", {})
            meta = info.get("trace_metadata", {}) or {}

            # ── Identify which agent produced this trace ──────────────────────
            # mlflow.modelId formats:
            #   "endpoints:/my-endpoint-name"     Databricks serving endpoint
            #   "models:/catalog.schema.model/1"  UC registered model
            #   "my-endpoint-name"                bare name
            model_id = meta.get("mlflow.modelId", "")
            # Also check serving endpoint metadata some agents emit
            serving_ep = (
                meta.get("databricks.serving_endpoint_name")
                or meta.get("serving_endpoint")
                or ""
            )

            agent_nid: Optional[str] = None
            for candidate in (model_id, serving_ep):
                if not candidate:
                    continue
                agent_nid = _match_lut(candidate, agent_lut)
                if agent_nid:
                    break

            # Fallback: match by MLflow experiment name (serving endpoint
            # experiments are typically named after the endpoint)
            if not agent_nid:
                exp_id = info.get("experiment_id") or trace.get("experiment_id", "")
                if exp_id:
                    agent_nid = exp_agent_map.get(str(exp_id))

            if not agent_nid:
                continue
            matched_traces += 1

            # ── Get spans ─────────────────────────────────────────────────────
            # The search endpoint often omits span data; fetch the full trace
            # record when spans are missing so we always process span-level edges.
            spans = data.get("spans", [])
            if not spans:
                request_id = info.get("request_id", "")
                if request_id:
                    try:
                        spans = mlflow_service.get_trace_spans(request_id)
                    except Exception:
                        spans = []

            # ── Process each span ─────────────────────────────────────────────
            for span in spans:
                attrs = span.get("attributes", {}) or {}
                span_type = attrs.get("mlflow.spanType", "")
                span_name = span.get("name", "")

                # Skip spans that ARE the root agent span itself (avoid self-loops)
                # The root span typically shares its name with the agent
                if _match_lut(span_name, agent_lut) == agent_nid:
                    continue

                is_agent_span = span_type in _AGENT_SPAN_TYPES
                is_tool_span = span_type in _TOOL_SPAN_TYPES or (
                    not span_type and "tool" in span_name.lower()
                )

                # Gather all candidate names from this span
                candidates = _span_candidate_names(span)

                if is_agent_span:
                    # Try to resolve to a known child agent (MAS → KA)
                    matched = False
                    for c in candidates:
                        tgt = _match_lut(c, agent_lut)
                        if tgt and tgt != agent_nid:
                            _acc(agent_nid, tgt, "routes to", animated=True)
                            matched = True
                            break
                    if not matched and span_name:
                        # Unknown agent-type span → synthetic agent node
                        synth_id = f"agent:span:{span_name}"
                        if synth_id not in seen_node_ids:
                            seen_node_ids.add(synth_id)
                            synthetic_nodes.append({
                                "id": synth_id,
                                "node_type": "span_tool",
                                "label": span_name,
                                "status": "ACTIVE",
                                "meta": {
                                    "description": "Child agent discovered from MLflow trace",
                                    "span_name": span_name,
                                    "span_type": span_type,
                                },
                            })
                        _acc(agent_nid, synth_id, "routes to", animated=True)

                elif is_tool_span:
                    # Try to resolve to a known tool (KA → UC function / MCP)
                    matched = False
                    for c in candidates:
                        tgt = _match_lut(c, tool_lut)
                        if tgt:
                            _acc(agent_nid, tgt, "calls", animated=False)
                            matched = True
                            break
                    if not matched and span_name:
                        # Unknown tool → synthetic span_tool node
                        synth_id = f"tool:span:{span_name}"
                        if synth_id not in seen_node_ids:
                            seen_node_ids.add(synth_id)
                            synthetic_nodes.append({
                                "id": synth_id,
                                "node_type": "span_tool",
                                "label": span_name,
                                "status": "ACTIVE",
                                "meta": {
                                    "description": "Tool discovered from MLflow trace spans",
                                    "span_name": span_name,
                                    "span_type": span_type,
                                },
                            })
                        _acc(agent_nid, synth_id, "calls", animated=False)

                else:
                    # For any other span type, check if the name matches a known agent
                    # (catches miscategorised routing spans)
                    for c in candidates:
                        tgt = _match_lut(c, agent_lut)
                        if tgt and tgt != agent_nid:
                            _acc(agent_nid, tgt, "calls", animated=False)
                            break

        logger.info("Topology traces: %s/%s matched agents → %s edges", matched_traces, total_traces, len(edge_acc))
    except Exception as exc:
        logger.warning("Topology trace extraction failed: %s", exc, exc_info=True)

    return list(edge_acc.values())


# ── Edge extraction: MAS→child agents via config JSONB ───────────────────────

_CHILD_KEYS = {"agents", "child_agents", "routes", "routing_agents", "children", "sub_agents"}


def _extract_mas_edges(agents: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Find MAS→child-agent routing edges from agent config JSONB.

    Searches both the top-level config keys and the nested config['tags'] dict,
    because KA/MAS agents store their endpoint tags under config['tags'].
    Also handles comma/semicolon-separated endpoint lists in tag values.
    """
    ep_to_id: Dict[str, str] = {}
    for a in agents:
        ep = (a.get("endpoint_name") or "").strip()
        if ep:
            ep_to_id[ep] = a["agent_id"]
            ep_to_id[ep.lower()] = a["agent_id"]

    edges: List[Dict[str, Any]] = []

    for mas in [a for a in agents if _resolve_agent_type(a) == "multi_agent_supervisor"]:
        src = _nid("agent", mas["agent_id"])
        cfg = mas.get("config") or {}
        if not isinstance(cfg, dict):
            continue

        child_eps: List[str] = []

        # Search top-level config keys
        for k, v in cfg.items():
            if k.lower() in _CHILD_KEYS:
                if isinstance(v, list):
                    child_eps.extend(str(x) for x in v)
                elif isinstance(v, str):
                    child_eps.append(v)

        # Also search inside config['tags'] — KA/MAS agents store tags there
        tags = cfg.get("tags") or {}
        if isinstance(tags, dict):
            for k, v in tags.items():
                if k.lower() in _CHILD_KEYS:
                    raw = str(v) if v else ""
                    # Split comma or semicolon separated lists
                    for part in re.split(r"[,;]", raw):
                        part = part.strip()
                        if part:
                            child_eps.append(part)

        for ep in child_eps:
            ep = ep.strip()
            child_id = ep_to_id.get(ep) or ep_to_id.get(ep.lower())
            if child_id:
                target = _nid("agent", child_id)
                eid = _eid(src, target)
                edges.append({
                    "id": eid,
                    "source": src,
                    "target": target,
                    "label": "routes to",
                    "call_count": None,
                    "animated": True,
                })

    return edges


# ── Edge extraction: app agent→resource via config.resources ─────────────────
#
# App-type agents store their serving_endpoint dependencies in config.resources:
#   {"resources": [{"name": "<endpoint_name>", "type": "serving_endpoint"}, ...]}
# We match resource names against tool_registry.endpoint_name (→ app→tool edge)
# and against other agents' endpoint_name (→ app→agent edge).
#
# Also handles served_entities for MAS agents that list child model endpoints.

def _extract_resource_edges(
    agents: List[Dict[str, Any]],
    tools: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Derive edges from app agent config.resources and served_entities declarations."""
    # Build endpoint_name → agent node_id lookup (for app→agent edges)
    ep_to_agent_nid: Dict[str, str] = {}
    for a in agents:
        ep = (a.get("endpoint_name") or "").strip().lower()
        if ep:
            ep_to_agent_nid[ep] = _nid("agent", a["agent_id"])
            ep_to_agent_nid[ep.split("/")[-1]] = _nid("agent", a["agent_id"])

    # Build endpoint_name / name → tool node_id lookup
    ep_to_tool_nid: Dict[str, str] = {}
    for t in tools:
        nid = _nid("tool", t["tool_id"])
        ep = (t.get("endpoint_name") or "").strip().lower()
        if ep:
            ep_to_tool_nid[ep] = nid
            ep_to_tool_nid[ep.split("/")[-1]] = nid
        name = (t.get("name") or "").strip().lower()
        if name:
            ep_to_tool_nid[name] = nid
            last = name.split(".")[-1]
            if last and last != name:
                ep_to_tool_nid[last] = nid

    edges: List[Dict[str, Any]] = []
    seen: Set[tuple] = set()

    def _match_name(ref: str) -> Optional[str]:
        """Return node_id matching ref — checks tools then agents."""
        r = ref.strip().lower()
        if not r:
            return None
        # Exact
        nid = ep_to_tool_nid.get(r) or ep_to_agent_nid.get(r)
        if nid:
            return nid
        # Suffix / prefix
        for key, n in {**ep_to_tool_nid, **ep_to_agent_nid}.items():
            if key and (r.endswith(key) or key.endswith(r)):
                return n
        return None

    def _add_edge(src: str, target_nid: str, label: str = "uses") -> None:
        if target_nid and target_nid != src:
            key_t = (src, target_nid)
            if key_t not in seen:
                seen.add(key_t)
                edges.append({
                    "id": _eid(src, target_nid),
                    "source": src,
                    "target": target_nid,
                    "label": label,
                    "call_count": None,
                    "animated": False,
                })

    for agent in agents:
        src = _nid("agent", agent["agent_id"])
        cfg = agent.get("config") or {}
        if not isinstance(cfg, dict):
            continue

        # ── App agents: config.resources list ────────────────────────────────
        resources = cfg.get("resources") or []
        if isinstance(resources, list):
            for r in resources:
                if not isinstance(r, dict):
                    continue
                r_type = r.get("type") or ""
                # Only follow serving_endpoint references (skip sql_warehouse etc.)
                if r_type != "serving_endpoint":
                    continue
                # Prefer "endpoint_name" (actual endpoint) over "name" (app alias)
                ref = r.get("endpoint_name") or r.get("name") or ""
                if ref:
                    target = _match_name(ref)
                    if target:
                        _add_edge(src, target, "uses")

        # ── Serving-endpoint agents: served_entities list ─────────────────────
        # served_entities describes which model entity backs this endpoint; those
        # entity_names are MLflow model paths, not other agent endpoints, so we
        # skip them — they don't map to topology nodes.

        # ── Tags that list related endpoints (KA/MAS store tags under config["tags"]) ──
        tags = cfg.get("tags") or {}
        if isinstance(tags, dict):
            for k, v in tags.items():
                if "agent" in k.lower() or "endpoint" in k.lower() or "tool" in k.lower():
                    raw = str(v).strip() if v else ""
                    # Handle comma/semicolon separated lists
                    for ref in re.split(r"[,;]", raw):
                        ref = ref.strip()
                        if ref:
                            target = _match_name(ref)
                            if target:
                                _add_edge(src, target, "routes to")

    return edges


# ── Public API ────────────────────────────────────────────────────────────────

def build_topology(force: bool = False) -> Dict[str, Any]:
    """Build and return the full topology graph. Cached for _CACHE_TTL seconds."""
    global _cache, _cache_ts

    if not force and _fresh():
        return _cache  # type: ignore[return-value]

    agents = _fetch_agents()
    tools = _fetch_tools()

    nodes: List[Dict[str, Any]] = []
    seen_ids: Set[str] = set()

    # ── Agent nodes
    for a in agents:
        nid = _nid("agent", a["agent_id"])
        seen_ids.add(nid)
        resolved_type = _resolve_agent_type(a)
        nodes.append({
            "id": nid,
            "node_type": resolved_type,
            "label": a["name"],
            "status": a.get("endpoint_status") or "UNKNOWN",
            "meta": {
                "agent_id": a["agent_id"],
                "description": a.get("description") or "",
                "endpoint_name": a.get("endpoint_name") or "",
                "creator": a.get("creator") or "",
                "type": resolved_type,
            },
        })

    # ── Tool nodes
    for t in tools:
        nid = _nid("tool", t["tool_id"])
        seen_ids.add(nid)
        nodes.append({
            "id": nid,
            "node_type": t["type"],  # 'mcp_server' or 'uc_function'
            "label": t["name"],
            "status": t.get("status") or "ACTIVE",
            "meta": {
                "tool_id": t["tool_id"],
                "description": t.get("description") or "",
                "sub_type": t.get("sub_type") or "",
                "endpoint_name": t.get("endpoint_name") or "",
                "catalog_name": t.get("catalog_name") or "",
                "schema_name": t.get("schema_name") or "",
                "config": t.get("config") or {},
            },
        })

    # ── Edges
    synthetic_nodes: List[Dict[str, Any]] = []
    edges: List[Dict[str, Any]] = []

    # 1. MAS → child agents (static config)
    edges.extend(_extract_mas_edges(agents))

    # 2. App agent → serving_endpoint resources (static config.resources analysis)
    edges.extend(_extract_resource_edges(agents, tools))

    # 3. Agent → tools (from MLflow traces — adds call counts + span-tool nodes)
    trace_edges = _extract_trace_edges(agents, tools, seen_ids, synthetic_nodes)
    edges.extend(trace_edges)
    nodes.extend(synthetic_nodes)

    # Deduplicate edges by id
    seen_eids: Set[str] = set()
    deduped: List[Dict[str, Any]] = []
    for e in edges:
        if e["id"] not in seen_eids:
            seen_eids.add(e["id"])
            deduped.append(e)

    result: Dict[str, Any] = {
        "nodes": nodes,
        "edges": deduped,
        "stats": {
            "total_nodes": len(nodes),
            "agent_nodes": sum(1 for n in nodes if "agent" in n["node_type"] or n["node_type"] in (
                "multi_agent_supervisor", "knowledge_assistant", "custom_agent", "custom_app",
                "external_agent", "genie_space", "custom_llm", "information_extraction",
            )),
            "tool_nodes": sum(1 for n in nodes if n["node_type"] in ("mcp_server", "uc_function", "span_tool")),
            "total_edges": len(deduped),
        },
    }

    _cache = result
    _cache_ts = time.time()
    return result
