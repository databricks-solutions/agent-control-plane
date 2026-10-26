"""Tests for vector_search_service — workspace-access scoping added to every
cache read (kb_billing_daily cost queries + endpoint/index/health/overview
reads), matching the pattern in billing_service / mlflow_service."""
import pytest
from unittest.mock import patch


class TestCostQueriesScope:
    """kb_billing_daily-backed cost functions."""

    def test_get_cost_summary_no_access_returns_zeroed_without_query(self):
        with patch("backend.services.vector_search_service.execute_query") as mock_q:
            from backend.services.vector_search_service import get_cost_summary
            result = get_cost_summary(30, allowed_workspace_ids=[])
            assert result["total_cost_usd"] == 0
            mock_q.assert_not_called()

    def test_get_cost_summary_scoped_uses_any_filter(self):
        with patch("backend.services.vector_search_service.execute_query", return_value=[]) as mock_q:
            from backend.services.vector_search_service import get_cost_summary
            get_cost_summary(30, allowed_workspace_ids=["ws1", "ws2"])
            sql = mock_q.call_args[0][0]
            params = mock_q.call_args[0][1]
            assert "workspace_id = ANY(%(ws)s)" in sql
            assert params["ws"] == ["ws1", "ws2"]

    def test_get_cost_summary_unrestricted_omits_filter(self):
        with patch("backend.services.vector_search_service.execute_query", return_value=[]) as mock_q:
            from backend.services.vector_search_service import get_cost_summary
            get_cost_summary(30, allowed_workspace_ids=None)
            sql = mock_q.call_args[0][0]
            assert "ANY" not in sql

    def test_get_cost_by_workspace_no_access(self):
        with patch("backend.services.vector_search_service.execute_query") as mock_q:
            from backend.services.vector_search_service import get_cost_by_workspace
            assert get_cost_by_workspace(30, allowed_workspace_ids=[]) == []
            mock_q.assert_not_called()

    def test_get_vs_top_workspaces_daily_scoped_filters_both_cte_and_outer(self):
        with patch("backend.services.vector_search_service.execute_query", return_value=[]) as mock_q:
            from backend.services.vector_search_service import get_vs_top_workspaces_daily
            get_vs_top_workspaces_daily(30, 5, allowed_workspace_ids=["ws1"])
            sql = mock_q.call_args[0][0]
            assert sql.count("ANY(%(ws)s)") == 2

    def test_get_lakebase_cost_summary_no_access(self):
        with patch("backend.services.vector_search_service.execute_query") as mock_q:
            from backend.services.vector_search_service import get_lakebase_cost_summary
            result = get_lakebase_cost_summary(30, allowed_workspace_ids=[])
            assert result["total_cost_usd"] == 0
            mock_q.assert_not_called()

    def test_get_combined_cost_trend_no_access(self):
        with patch("backend.services.vector_search_service.execute_query") as mock_q:
            from backend.services.vector_search_service import get_combined_cost_trend
            assert get_combined_cost_trend(30, allowed_workspace_ids=[]) == []
            mock_q.assert_not_called()


class TestEndpointIndexScope:
    """vector_search_endpoints / vector_search_indexes reads."""

    def test_get_endpoints_no_access_returns_empty(self):
        with patch("backend.services.vector_search_service.execute_query") as mock_q:
            from backend.services.vector_search_service import get_endpoints
            assert get_endpoints(allowed_workspace_ids=[]) == []
            mock_q.assert_not_called()

    def test_get_endpoints_scoped_filters(self):
        with patch("backend.services.vector_search_service.execute_query", return_value=[]) as mock_q:
            from backend.services.vector_search_service import get_endpoints
            get_endpoints(allowed_workspace_ids=["ws1"])
            sql = mock_q.call_args[0][0]
            assert "workspace_id = ANY(%(ws)s)" in sql

    def test_get_indexes_no_access_returns_empty(self):
        with patch("backend.services.vector_search_service.execute_query") as mock_q:
            from backend.services.vector_search_service import get_indexes
            assert get_indexes("ep1", allowed_workspace_ids=[]) == []
            mock_q.assert_not_called()

    def test_get_health_history_joins_endpoints_for_scope(self):
        with patch("backend.services.vector_search_service.execute_query", return_value=[]) as mock_q:
            from backend.services.vector_search_service import get_health_history
            get_health_history(7, allowed_workspace_ids=["ws1"])
            sql = mock_q.call_args[0][0]
            assert "JOIN vector_search_endpoints" in sql

    def test_get_overview_no_access_returns_empty_shape(self):
        with patch("backend.services.vector_search_service.execute_query") as mock_q:
            from backend.services.vector_search_service import get_overview
            result = get_overview(allowed_workspace_ids=[])
            assert result["total_endpoints"] == 0
            mock_q.assert_not_called()


class TestLakebaseInstancesScope:
    """lakebase_instances has no workspace_id column — must be suppressed
    entirely for any scoped (non-account-admin) caller."""

    def test_suppressed_when_restricted(self):
        with patch("backend.services.vector_search_service.execute_query") as mock_q:
            from backend.services.vector_search_service import get_lakebase_instances
            assert get_lakebase_instances(allowed_workspace_ids=["ws1"]) == []
            mock_q.assert_not_called()

    def test_unrestricted_still_queries(self):
        with patch("backend.services.vector_search_service.execute_query", return_value=[{"instance_name": "x"}]):
            from backend.services.vector_search_service import get_lakebase_instances
            result = get_lakebase_instances(allowed_workspace_ids=None)
            assert result == [{"instance_name": "x"}]


class TestDiscoverLakebaseMerge:
    """discover_lakebase_instances must list BOTH the Provisioned and Autoscaling
    APIs and dedup by uid (gh #73 — Autoscaling-only workspaces were invisible)."""

    class _FakeApi:
        def do(self, method, path):
            if path == "/api/2.0/database/instances":
                return {"database_instances": [
                    {"name": "prov", "uid": "u1", "state": "AVAILABLE"}]}
            if path.startswith("/api/2.0/postgres/projects"):
                return {"projects": [
                    {"uid": "u1", "project_id": "prov", "status": {}},  # dup of Provisioned
                    {"uid": "u2", "project_id": "auto", "create_time": "2026-01-01T00:00:00Z",
                     "status": {"display_name": "auto", "pg_version": 16, "owner": "me",
                                "default_endpoint_settings": {
                                    "autoscaling_limit_min_cu": 8.0,
                                    "autoscaling_limit_max_cu": 16.0}}},
                ]}  # no next_page_token → single page
            return {}

    class _FakeClient:
        api_client = None

    def test_merges_and_dedups_by_uid(self):
        client = self._FakeClient()
        client.api_client = self._FakeApi()
        with patch("backend.services.vector_search_service._get_workspace_client", return_value=client):
            from backend.services.vector_search_service import discover_lakebase_instances
            result = discover_lakebase_instances()
        uids = [r.get("uid") for r in result]
        assert uids == ["u1", "u2"]  # Provisioned first, Autoscaling dup dropped
        auto = next(r for r in result if r["uid"] == "u2")
        assert auto["name"] == "auto"
        assert auto["capacity"] == "CU_8-16"
        assert auto["pg_version"] == "PG_VERSION_16"
        assert auto["creator"] == "me"

    def test_autoscaling_only_still_discovers(self):
        class AutoOnlyApi:
            def do(self, method, path):
                if path == "/api/2.0/database/instances":
                    return {"database_instances": []}  # Provisioned API empty (Azure case)
                if path.startswith("/api/2.0/postgres/projects"):
                    return {"projects": [{"uid": "u9", "project_id": "only",
                                          "status": {"display_name": "only"}}]}
                return {}
        client = self._FakeClient()
        client.api_client = AutoOnlyApi()
        with patch("backend.services.vector_search_service._get_workspace_client", return_value=client):
            from backend.services.vector_search_service import discover_lakebase_instances
            result = discover_lakebase_instances()
        assert len(result) == 1 and result[0]["uid"] == "u9"

    def test_autoscaling_paginates(self):
        """page1 carries next_page_token=t1; page2 is fetched at ?page_token=t1,
        and projects from BOTH pages end up in the result."""
        class PagedApi:
            def do(self, method, path):
                if path == "/api/2.0/database/instances":
                    return {"database_instances": []}
                if path == "/api/2.0/postgres/projects":
                    return {"projects": [{"uid": "p1", "project_id": "a", "status": {}}],
                            "next_page_token": "t1"}
                if path == "/api/2.0/postgres/projects?page_token=t1":
                    return {"projects": [{"uid": "p2", "project_id": "b", "status": {}}]}
                return {}
        client = self._FakeClient()
        client.api_client = PagedApi()
        with patch("backend.services.vector_search_service._get_workspace_client", return_value=client):
            from backend.services.vector_search_service import discover_lakebase_instances
            result = discover_lakebase_instances()
        assert sorted(r["uid"] for r in result) == ["p1", "p2"]

    def test_non_advancing_cursor_stops(self):
        """A cursor that never changes must not loop — the projects path is hit
        exactly twice (initial page + one repeat that trips the guard)."""
        calls = {"n": 0}

        class StuckApi:
            def do(self, method, path):
                if path == "/api/2.0/database/instances":
                    return {"database_instances": []}
                if path.startswith("/api/2.0/postgres/projects"):
                    calls["n"] += 1
                    return {"projects": [{"uid": f"x{calls['n']}", "status": {}}],
                            "next_page_token": "same"}
                return {}
        client = self._FakeClient()
        client.api_client = StuckApi()
        with patch("backend.services.vector_search_service._get_workspace_client", return_value=client):
            from backend.services.vector_search_service import discover_lakebase_instances
            discover_lakebase_instances()
        assert calls["n"] == 2

    def test_null_max_cu_does_not_abort(self):
        """A present-but-null autoscaling_limit_max_cu must not raise/abort the
        whole scan (gh #74-review): capacity falls back to the min value."""
        class NullMaxApi:
            def do(self, method, path):
                if path == "/api/2.0/database/instances":
                    return {"database_instances": []}
                if path.startswith("/api/2.0/postgres/projects"):
                    return {"projects": [
                        {"uid": "n1", "project_id": "bad", "status": {
                            "default_endpoint_settings": {
                                "autoscaling_limit_min_cu": 4.0,
                                "autoscaling_limit_max_cu": None}}},
                        {"uid": "n2", "project_id": "good", "status": {}},
                    ]}
                return {}
        client = self._FakeClient()
        client.api_client = NullMaxApi()
        with patch("backend.services.vector_search_service._get_workspace_client", return_value=client):
            from backend.services.vector_search_service import discover_lakebase_instances
            result = discover_lakebase_instances()
        uids = [r["uid"] for r in result]
        assert uids == ["n1", "n2"]  # both survive; first record didn't abort the loop
        bad = next(r for r in result if r["uid"] == "n1")
        assert bad["capacity"] == "CU_4-4"  # max fell back to min

    def test_httpx_fallback_when_no_client(self):
        """With no WorkspaceClient, both APIs are fetched via httpx and merged."""
        class Resp:
            def __init__(self, payload):
                self._p = payload

            def raise_for_status(self):
                pass

            def json(self):
                return self._p

        def fake_get(url, headers=None, timeout=None):
            if url == "https://h/api/2.0/database/instances":
                return Resp({"database_instances": [{"name": "prov", "uid": "u1"}]})
            if url == "https://h/api/2.0/postgres/projects":
                return Resp({"projects": [{"uid": "u2", "project_id": "auto", "status": {}}]})
            return Resp({})

        with patch("backend.services.vector_search_service._get_workspace_client", return_value=None), \
             patch("backend.services.vector_search_service.get_databricks_host", return_value="https://h"), \
             patch("backend.services.vector_search_service.get_databricks_headers", return_value={}), \
             patch("httpx.get", side_effect=fake_get) as mock_get:
            from backend.services.vector_search_service import discover_lakebase_instances
            result = discover_lakebase_instances()
        urls = [c.args[0] for c in mock_get.call_args_list]
        assert "https://h/api/2.0/database/instances" in urls
        assert "https://h/api/2.0/postgres/projects" in urls
        assert sorted(r["uid"] for r in result) == ["u1", "u2"]
