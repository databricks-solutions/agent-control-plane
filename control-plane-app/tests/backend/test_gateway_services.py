"""Unity Gateway services hub (get_gateway_services) + generalized UC grants."""
from datetime import datetime, timezone
from unittest.mock import patch


def _inv(name, stype="MODEL_SERVICE", **kw):
    row = {"full_name": name, "service_type": stype, "owner": "o", "comment": "", "supported_api_types": "a,b",
           "provider_type": "", "source_connection": "", "destinations": "", "rate_limit_count": 0,
           "rate_limits": "", "inference_table": "", "inference_table_enabled": None, "update_time": None,
           "discovered_at": datetime(2026, 10, 27, tzinfo=timezone.utc)}
    row.update(kw)
    return row


def _use(name, stype="MODEL_SERVICE", req=100, err=5, thr=2):
    return {"service_name": name, "service_type": stype, "request_count": req, "error_count": err,
            "throttled_count": thr, "unique_users": 3, "input_tokens": 10, "output_tokens": 20,
            "p50_latency_ms": 100, "p95_latency_ms": 900, "max_event_time": "2026-10-27"}


def test_merges_inventory_with_traffic_and_traffic_only_services():
    from backend.services import gateway_service as g
    inv = [_inv("cat.sch.m1", rate_limits='[{"key":"RATE_LIMIT_KEY_SERVICE","requests":"300"}]',
                inference_table="cat.sch.m1_payload", inference_table_enabled=True),
           _inv("system.ai.slack", "MCP_SERVICE")]
    use = [_use("cat.sch.m1"), _use("cat.sch.unlisted", req=50, err=0, thr=0)]
    with patch("backend.database.execute_query", side_effect=[inv, use]):
        out = g.get_gateway_services(allowed_workspace_ids=None)

    by = {s["full_name"]: s for s in out["services"]}
    m1 = by["cat.sch.m1"]
    assert m1["request_count"] == 100 and m1["error_rate"] == 5.0 and m1["throttled_count"] == 2
    assert m1["rate_limits"] == [{"key": "RATE_LIMIT_KEY_SERVICE", "requests": "300"}]
    assert m1["supported_api_types"] == ["a", "b"] and m1["in_inventory"] is True
    assert by["system.ai.slack"]["request_count"] is None          # no usage → None, not 0
    assert by["cat.sch.unlisted"]["in_inventory"] is False          # traffic-only service still shown
    assert out["services"][0]["full_name"] == "cat.sch.m1"          # sorted by traffic
    t = out["totals"]
    assert t["services"] == 3 and t["by_type"]["MCP_SERVICE"] == 1
    assert t["with_rate_limits"] == 1 and t["with_inference_table"] == 1 and t["throttled"] == 1
    assert out["traffic_visible"] is True and out["as_of"].startswith("2026-10-27")


def test_scoped_caller_gets_inventory_but_no_traffic():
    from backend.services import gateway_service as g
    with patch("backend.database.execute_query", side_effect=[[_inv("cat.sch.m1")]]) as q:
        out = g.get_gateway_services(allowed_workspace_ids=["ws1"])
    assert q.call_count == 1                                       # usage table never read
    assert out["traffic_visible"] is False
    assert out["services"][0]["request_count"] is None


def test_missing_tables_degrade_to_empty():
    from backend.services import gateway_service as g
    with patch("backend.database.execute_query", side_effect=Exception("relation does not exist")):
        out = g.get_gateway_services(allowed_workspace_ids=None)
    assert out["services"] == [] and out["totals"]["services"] == 0


def test_bad_rate_limit_json_is_tolerated():
    from backend.services import gateway_service as g
    with patch("backend.database.execute_query", side_effect=[[_inv("cat.sch.m1", rate_limits="{not json")], []]):
        out = g.get_gateway_services(allowed_workspace_ids=None)
    assert out["services"][0]["rate_limits"] == []


class TestServiceGrants:
    def test_mcp_service_uses_mcp_securable(self):
        from backend.services import gateway_service as g
        with patch.object(g, "_uc_get_json", return_value={"privilege_assignments": [
                {"principal": "account users", "privileges": ["EXECUTE"]}]}) as get:
            out = g.get_service_grants("system.ai.slack", "MCP_SERVICE")
        assert get.call_args.args[0] == "/api/2.1/unity-catalog/permissions/mcp_service/system.ai.slack"
        assert out["grants"] == [{"principal": "account users", "privileges": ["EXECUTE"]}]

    def test_rejects_names_that_could_alter_the_path(self):
        from backend.services import gateway_service as g
        with patch.object(g, "_uc_get_json") as get:
            for bad in ("../../permissions/catalog/x", "a.b", "cat.sch.x?y=1", "cat/sch.x.y", ""):
                assert g.get_service_grants(bad, "MODEL_SERVICE")["error"] == "invalid service name"
        get.assert_not_called()

    def test_unknown_type(self):
        from backend.services import gateway_service as g
        assert "unknown service type" in g.get_service_grants("a.b.c", "TABLE")["error"]

    def test_model_service_wrapper_still_works(self):
        from backend.services import gateway_service as g
        with patch.object(g, "_uc_get_json", return_value=None):
            assert g.get_model_service_grants("a.b.c")["error"].startswith("grants not readable")


def test_legacy_route_aliases_still_resolve(app_client):
    """Old /uag-v2-* paths must keep working for frontends cached mid-deploy."""
    with patch("backend.services.gateway_service.get_unity_usage", return_value={"ok": 1}), \
         patch("backend.services.gateway_service.get_unity_timeseries", return_value={"ok": 2}):
        for path, val in (("/api/v1/gateway/uag-v2-usage", 1), ("/api/v1/gateway/unity-usage", 1),
                          ("/api/v1/gateway/uag-v2-timeseries", 2), ("/api/v1/gateway/unity-timeseries", 2)):
            r = app_client.get(path)
            assert r.status_code == 200 and r.json() == {"ok": val}, path


def test_services_route_passes_caller_scope(app_client):
    """Route wiring: a scoped caller's allow-list reaches the service → no traffic."""
    with patch("backend.api.gateway.get_allowed_workspace_ids", return_value=["ws1"]), \
         patch("backend.database.execute_query", return_value=[]) as q:
        r = app_client.get("/api/v1/gateway/services")
    assert r.status_code == 200
    assert r.json()["traffic_visible"] is False
    assert q.call_count == 1  # inventory only — account-wide usage never read
