"""agent_service.get_agent_metrics — regression: it read request_logs, which
nothing writes, so AgentDetail always showed zeros. Now built from synced tables,
scoped to the agent's workspace (endpoint names repeat across workspaces)."""
from types import SimpleNamespace
from unittest.mock import patch


def _agent(endpoint="ep1"):
    return SimpleNamespace(endpoint_name=endpoint)


def _run(answers, endpoint="ep1", hours=168):
    from backend.services import agent_service as svc
    with patch.object(svc, "get_agent_by_id", return_value=_agent(endpoint)), \
         patch.object(svc, "execute_one", side_effect=answers) as q:
        return svc.get_agent_metrics("a1", hours=hours), q


def test_metrics_scoped_to_agent_workspace():
    m, q = _run([
        {"workspace_id": "ws1"},                          # discovered_agents → workspace
        {"req": 200},                                     # billing_token_daily (ws-scoped)
        {"cost": 3.5},                                    # billing_serving_daily (ws-scoped)
        {"n": 1, "w": "ws1"},                             # name lives only in this workspace
        {"req": 200, "err": 10},                          # gateway_usage_daily
        {"p50_latency_ms": 120, "p95_latency_ms": 900},   # uag_usage_summary
    ])
    assert m["workspace_id"] == "ws1" and m["window_days"] == 7
    assert m["request_count"] == 200 and m["total_cost"] == 3.5
    assert m["error_rate"] == 5.0
    assert (m["p50_latency"], m["p95_latency"]) == (120.0, 900.0)
    # requests and cost filtered by the agent's workspace
    assert q.call_args_list[1].args[1][0] == "ws1"
    assert q.call_args_list[2].args[1][0] == "ws1"
    # collision check spans idle endpoints too, not just ones with recent usage
    owner_sql = q.call_args_list[3].args[0]
    assert "serving_endpoints_inventory" in owner_sql and "billing_token_daily" in owner_sql


def test_colliding_endpoint_name_skips_workspace_less_tables():
    """e.g. a foundation-model endpoint name present in many workspaces: error rate
    and latency must not be summed across them."""
    m, q = _run([
        {"workspace_id": "ws1"}, {"req": 50}, {"cost": 1.0},
        {"n": 754, "w": "a"},                             # name collides account-wide
    ])
    assert m["request_count"] == 50 and m["total_cost"] == 1.0
    assert m["error_rate"] is None and m["p95_latency"] is None
    assert q.call_count == 4                              # never queried the ambiguous tables


def test_missing_sources_are_none_not_zero():
    m, _ = _run([{"workspace_id": "ws1"}, None, Exception("no table"), {"n": 0, "w": None}], hours=24)
    assert m["window_days"] == 1
    for k in ("request_count", "error_rate", "total_cost", "p95_latency"):
        assert m[k] is None


def test_agent_without_endpoint():
    m, q = _run([], endpoint=None)
    q.assert_not_called()
    assert m["endpoint_name"] is None and m["request_count"] is None


def test_unknown_agent_returns_empty():
    from backend.services import agent_service as svc
    with patch.object(svc, "get_agent_by_id", return_value=None):
        assert svc.get_agent_metrics("nope") == {}


def test_name_unique_but_owned_by_another_workspace_is_not_trusted():
    m, q = _run([{"workspace_id": "ws1"}, {"req": 5}, {"cost": 0.5}, {"n": 1, "w": "ws2"}])
    assert m["error_rate"] is None and m["p95_latency"] is None
    assert q.call_count == 4


def test_zero_rows_in_owner_check_is_unknown_not_unique():
    m, q = _run([{"workspace_id": "ws1"}, None, None, {"n": 0, "w": None}])
    assert m["error_rate"] is None and m["p95_latency"] is None
    assert q.call_count == 4


def test_registry_agent_falls_back_to_deploy_workspace():
    with patch("backend.services.billing_service.get_current_workspace_id", return_value="ws9"):
        m, q = _run([None, {"req": 3}, {"cost": 0.1}, {"n": 1, "w": "ws9"}, {"req": 3, "err": 0}, None])
    assert m["workspace_id"] == "ws9"
    assert q.call_args_list[1].args[1][0] == "ws9"
    assert m["error_rate"] == 0.0


def test_unresolvable_workspace_skips_all_scoped_metrics():
    with patch("backend.services.billing_service.get_current_workspace_id", side_effect=Exception("no ws")):
        m, q = _run([None, {"n": 1, "w": "ws1"}])
    assert m["workspace_id"] is None
    for k in ("request_count", "total_cost", "error_rate", "p95_latency"):
        assert m[k] is None
    sqls = " ".join(c.args[0] for c in q.call_args_list)
    assert "billing_serving_daily" not in sqls and "gateway_usage_daily" not in sqls


def test_owner_check_failure_fails_closed():
    m, _ = _run([{"workspace_id": "ws1"}, {"req": 1}, {"cost": 0.0}, Exception("inventory table missing")])
    assert m["error_rate"] is None and m["p95_latency"] is None
