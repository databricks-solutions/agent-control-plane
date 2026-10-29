"""gateway_insights_service — usage-dashboard parity views."""
from datetime import date, timedelta
from unittest.mock import patch

SVC = "backend.services.gateway_insights_service"


class _Recorder:
    """execute_query stub: records (sql, params); returns canned rows by substring."""
    def __init__(self, answers=None, fail=False):
        self.calls, self.answers, self.fail = [], answers or {}, fail

    def __call__(self, sql, params=None):
        self.calls.append((" ".join(sql.split()), params or {}))
        if self.fail:
            raise Exception("relation does not exist")
        for needle, rows in self.answers.items():
            if needle in sql:
                return rows
        return []


class TestUsageExplorer:
    def test_admin_selection_narrows_and_dates_clamp(self):
        from backend.services import gateway_insights_service as g
        rec = _Recorder({"COUNT(DISTINCT NULLIF(service_name":
                         [{"requests": 200, "errors": 10, "throttled": 4, "input_tokens": 80, "output_tokens": 20,
                           "cached_tokens": 20, "services": 3}]})
        with patch(f"{SVC}.execute_query", rec):
            out = g.get_usage_explorer("2000-01-01", "2999-01-01", ["ws1"], None, None, allowed_workspace_ids=None)
        t = out["totals"]
        assert t["requests"] == 200 and t["error_rate"] == 5.0 and t["cache_share"] == 20.0
        lo, hi = date.fromisoformat(out["filters"]["start_date"]), date.fromisoformat(out["filters"]["end_date"])
        assert lo == date.today() - timedelta(days=g.FACT_DAYS) and hi == date.today()
        sql, params = rec.calls[0]
        assert "workspace_id = ANY(%(ws)s)" in sql and params["ws"] == ["ws1"]

    def test_scoped_caller_cannot_widen_with_selection(self):
        from backend.services import gateway_insights_service as g
        rec = _Recorder()
        with patch(f"{SVC}.execute_query", rec):
            out = g.get_usage_explorer(None, None, ["ws9"], None, None, allowed_workspace_ids=["ws1"])
        assert rec.calls == [] and out["totals"] == {}          # ws9 not allowed → empty, no query

    def test_scoped_caller_defaults_to_own_workspaces(self):
        from backend.services import gateway_insights_service as g
        rec = _Recorder()
        with patch(f"{SVC}.execute_query", rec):
            g.get_usage_explorer(None, None, [], None, None, allowed_workspace_ids=["ws1", "ws2"])
        assert all(c[1].get("ws") in (["ws1", "ws2"], None) for c in rec.calls if "uag_usage_daily_fact f" in c[0])
        assert any(sorted(c[1].get("ws") or []) == ["ws1", "ws2"] for c in rec.calls)

    def test_workspace_options_are_scoped(self):
        """The filter's workspace list must not leak other workspaces' ids to a workspace admin."""
        from backend.services import gateway_insights_service as g
        rec = _Recorder()
        with patch(f"{SVC}.execute_query", rec):
            g.get_usage_explorer(None, None, [], None, None, allowed_workspace_ids=["ws1"])
        opt = [c for c in rec.calls if "SELECT DISTINCT workspace_id" in c[0]]
        assert len(opt) == 1
        assert "workspace_id = ANY(%(ws)s)" in opt[0][0] and opt[0][1]["ws"] == ["ws1"]

    def test_tag_filter_is_parameterised(self):
        from backend.services import gateway_insights_service as g
        rec = _Recorder()
        with patch(f"{SVC}.execute_query", rec):
            g.get_usage_explorer(None, None, [], "team", "x'; DROP TABLE y;--", allowed_workspace_ids=None)
        sql, params = rec.calls[0]
        assert "tag_key = %(tk)s AND tag_value = %(tv)s" in sql and "DROP" not in sql
        assert params["tv"] == "x'; DROP TABLE y;--"

    def test_fails_open(self):
        from backend.services import gateway_insights_service as g
        with patch(f"{SVC}.execute_query", _Recorder(fail=True)):
            out = g.get_usage_explorer(None, None, [], None, None, allowed_workspace_ids=None)
        assert out["totals"]["requests"] == 0 and out["trend"] == [] and out["top_services"] == []


class TestAccountWideViewsSuppressedForScoped:
    def test_all_restricted_without_querying(self):
        from backend.services import gateway_insights_service as g
        rec = _Recorder()
        with patch(f"{SVC}.execute_query", rec):
            for fn in (g.get_daily_activity, g.get_coding_sessions, g.get_unit_economics,
                       g.get_cost_by_service_tag, g.get_model_performance, g.get_skills):
                assert fn(allowed_workspace_ids=["ws1"])["restricted"] is True
        assert rec.calls == []


def test_coding_sessions_rollup():
    from backend.services import gateway_insights_service as g
    daily = [
        {"usage_date": "2026-10-27", "coding_agent": "claude-code", "surface": "cli", "sessions": 5, "users": 3,
         "subagents": 2, "requests": 100, "input_tokens": 10, "output_tokens": 5, "reasoning_tokens": 1, "est_cost_usd": 2.5},
        {"usage_date": "2026-10-28", "coding_agent": "claude-code", "surface": "cli", "sessions": 7, "users": 4,
         "subagents": 1, "requests": 50, "input_tokens": 1, "output_tokens": 1, "reasoning_tokens": 0, "est_cost_usd": 1.0},
        {"usage_date": "2026-10-28", "coding_agent": "codex", "surface": "ucode codex", "sessions": 1, "users": 1,
         "subagents": 0, "requests": 10, "input_tokens": 1, "output_tokens": 1, "reasoning_tokens": 0, "est_cost_usd": 0.5},
    ]
    rec = _Recorder({"FROM uag_coding_sessions_daily": daily, "FROM uag_coding_sessions_top": []})
    with patch(f"{SVC}.execute_query", rec):
        out = g.get_coding_sessions(allowed_workspace_ids=None)
    assert out["totals"] == {"session_days": 13, "requests": 160, "est_cost_usd": 4.0, "reasoning_tokens": 1, "harnesses": 2}
    cc = out["by_harness"][0]
    assert (cc["coding_agent"], cc["requests"], cc["peak_daily_users"]) == ("claude-code", 150, 4)
    assert [t["usage_date"] for t in out["trend"]] == ["2026-10-27", "2026-10-28"]
    assert out["trend"][1]["sessions"] == 8


def test_model_performance_derived_fields():
    from backend.services import gateway_insights_service as g
    rows = [{"destination_model": "m", "requests": 200, "error_count": 10, "throttled_count": 4, "streaming_requests": 50,
             "p50_latency_ms": 100, "p95_latency_ms": 900, "p50_ttfb_ms": 50, "p95_ttfb_ms": 300, "p50_output_tps": 120.5,
             "input_tokens": 300, "cache_read_tokens": 100, "reasoning_tokens": 7}]
    with patch(f"{SVC}.execute_query", _Recorder({"FROM uag_model_performance": rows})):
        m = g.get_model_performance(allowed_workspace_ids=None)["models"][0]
    assert (m["error_rate"], m["throttle_rate"], m["cache_read_share"], m["p50_output_tps"]) == (5.0, 2.0, 25.0, 120.5)


def test_routes_pass_scope(app_client):
    with patch("backend.api.gateway.get_allowed_workspace_ids", return_value=["ws1"]), \
         patch(f"{SVC}.execute_query", _Recorder()) as _:
        for path in ("/api/v1/gateway/coding-sessions", "/api/v1/gateway/unit-economics", "/api/v1/gateway/tag-spend",
                     "/api/v1/gateway/model-performance", "/api/v1/gateway/skills", "/api/v1/gateway/daily-activity"):
            r = app_client.get(path)
            assert r.status_code == 200 and r.json()["restricted"] is True, path
        r = app_client.get("/api/v1/gateway/usage-explorer?workspace_ids=ws9")
        assert r.status_code == 200 and r.json()["totals"] == {}
