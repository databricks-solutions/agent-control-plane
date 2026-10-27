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


# ══════════════════════════════════════════════════════════════
# Retrieval & Memory: Vector Search index usage + Lakebase health
# ══════════════════════════════════════════════════════════════

_SVC = "backend.services.vector_search_service"


class TestManagedByClassification:
    """Agent Bricks labels — KA / supervisor are recognised, user indexes are not."""

    def test_ka_endpoint_is_knowledge_assistant(self):
        from backend.services.vector_search_service import classify_managed_by
        assert classify_managed_by("ka-534eb6ba-vs-endpoint", "main.x.idx") == "knowledge_assistant"

    def test_ka_internal_tiles_index_is_knowledge_assistant(self):
        from backend.services.vector_search_service import classify_managed_by
        name = "__databricks_internal_catalog_tiles_arclight_7474660053069733.20a2325a_af780f6cf9184208.ka_20a2325a_4ddde994_index"
        assert classify_managed_by("vs-endpoint-x", name) == "knowledge_assistant"

    def test_mas_endpoint_is_supervisor(self):
        from backend.services.vector_search_service import classify_managed_by
        assert classify_managed_by("mas-abc-vs", "main.x.idx") == "supervisor"

    def test_user_index_with_ka_substring_is_not_labelled(self):
        # a customer table named *.ka_* on a user endpoint must not be mislabelled
        from backend.services.vector_search_service import classify_managed_by
        assert classify_managed_by("my-endpoint", "main.sales.ka_accounts_index") is None
        assert classify_managed_by(None, None) is None


class TestRetrievalMemoryScope:
    """Every Retrieval & Memory read: no access → no query; scoped → workspace predicate."""

    CASES = [
        ("get_vs_usage_trend", {}),
        ("get_vs_index_usage", {}),
        ("get_vs_index_callers", {"index_name": "main.x.idx"}),
        ("get_lakebase_health", {}),
        ("get_lakebase_wait_events", {}),
        ("get_lakebase_cost_by_project", {}),
    ]

    @pytest.mark.parametrize("fn_name,extra", CASES)
    def test_no_access_returns_empty_without_query(self, fn_name, extra):
        import importlib
        fn = getattr(importlib.import_module(_SVC), fn_name)
        with patch(f"{_SVC}.execute_query") as mock_q:
            assert fn(**extra, allowed_workspace_ids=[]) == []
            mock_q.assert_not_called()

    @pytest.mark.parametrize("fn_name,extra", CASES)
    def test_scoped_adds_workspace_predicate(self, fn_name, extra):
        import importlib
        fn = getattr(importlib.import_module(_SVC), fn_name)
        with patch(f"{_SVC}.execute_query", return_value=[]) as mock_q:
            fn(**extra, allowed_workspace_ids=["ws1", "ws2"])
            sql = mock_q.call_args[0][0]
            params = mock_q.call_args[0][1]
            assert "workspace_id = ANY(%(ws)s)" in sql or "c.workspace_id = ANY(%(ws)s)" in sql
            assert params["ws"] == ["ws1", "ws2"]

    @pytest.mark.parametrize("fn_name,extra", CASES)
    def test_unrestricted_has_no_workspace_predicate(self, fn_name, extra):
        import importlib
        fn = getattr(importlib.import_module(_SVC), fn_name)
        with patch(f"{_SVC}.execute_query", return_value=[]) as mock_q:
            fn(**extra, allowed_workspace_ids=None)
            assert "ANY" not in mock_q.call_args[0][0]

    def test_cost_by_project_scopes_names_and_main_query(self):
        # the names CTE and the main join both need the predicate (no cross-workspace names)
        from backend.services.vector_search_service import get_lakebase_cost_by_project
        with patch(f"{_SVC}.execute_query", return_value=[]) as mock_q:
            get_lakebase_cost_by_project(30, allowed_workspace_ids=["ws1"])
            assert mock_q.call_args[0][0].count("ANY(%(ws)s)") == 2

    def test_cost_by_project_picks_covering_window(self):
        """Table holds pre-aggregated 7/30/90-day totals; days maps to the smallest covering window."""
        from backend.services.vector_search_service import get_lakebase_cost_by_project
        for days, window in ((1, 7), (7, 7), (14, 30), (30, 30), (60, 90), (365, 90)):
            with patch(f"{_SVC}.execute_query", return_value=[]) as mock_q:
                get_lakebase_cost_by_project(days, allowed_workspace_ids=None)
            assert "window_days = %(window)s" in mock_q.call_args[0][0]
            assert mock_q.call_args[0][1]["window"] == window, days

    def test_callers_workspace_filter_is_parameterised(self):
        from backend.services.vector_search_service import get_vs_index_callers
        with patch(f"{_SVC}.execute_query", return_value=[]) as mock_q:
            get_vs_index_callers("main.x.idx", "ws9", allowed_workspace_ids=None)
            sql = mock_q.call_args[0][0]
            params = mock_q.call_args[0][1]
            assert "workspace_id = %(wsid)s" in sql
            assert params["wsid"] == "ws9"
            assert params["idx"] == "main.x.idx"

    def test_callers_empty_index_name_short_circuits(self):
        from backend.services.vector_search_service import get_vs_index_callers
        with patch(f"{_SVC}.execute_query") as mock_q:
            assert get_vs_index_callers("", allowed_workspace_ids=None) == []
            mock_q.assert_not_called()


class TestRetrievalMemoryFailOpen:
    """A table that isn't synced yet (or any read error) yields empty — never raises."""

    CASES = [
        ("get_vs_usage_trend", {}),
        ("get_vs_index_usage", {}),
        ("get_vs_index_callers", {"index_name": "main.x.idx"}),
        ("get_lakebase_health", {}),
        ("get_lakebase_wait_events", {}),
        ("get_lakebase_cost_by_project", {}),
    ]

    @pytest.mark.parametrize("fn_name,extra", CASES)
    def test_query_error_returns_empty_list(self, fn_name, extra):
        import importlib
        fn = getattr(importlib.import_module(_SVC), fn_name)
        with patch(f"{_SVC}.execute_query", side_effect=Exception('relation "vs_index_usage_daily" does not exist')):
            assert fn(**extra, allowed_workspace_ids=None) == []


class TestRetrievalMemoryShapes:
    """Row shaping: Decimal aggregates become ints/floats, managed_by is attached."""

    def test_index_usage_coerces_and_labels(self):
        from decimal import Decimal
        from backend.services.vector_search_service import get_vs_index_usage
        rows = [{
            "workspace_id": "ws1",
            "index_name": "__databricks_internal_catalog_tiles_a.ka_b_index",
            "endpoint_name": "vs-ep",
            "query_count": Decimal("120"), "scan_count": Decimal("3"), "write_count": Decimal("0"),
            "error_count": Decimal("7"), "peak_daily_callers": Decimal("4"),
            "last_active_date": "2026-10-26",
        }]
        with patch(f"{_SVC}.execute_query", return_value=rows):
            out = get_vs_index_usage(30, allowed_workspace_ids=None)
        assert out[0]["query_count"] == 120 and isinstance(out[0]["query_count"], int)
        assert out[0]["error_count"] == 7
        assert out[0]["managed_by"] == "knowledge_assistant"

    def test_index_usage_sql_joins_endpoint_cache(self):
        from backend.services.vector_search_service import get_vs_index_usage
        with patch(f"{_SVC}.execute_query", return_value=[]) as mock_q:
            get_vs_index_usage(30, allowed_workspace_ids=None)
            assert "FROM vector_search_indexes" in mock_q.call_args[0][0]

    def test_lakebase_health_coerces_numerics(self):
        from decimal import Decimal
        from backend.services.vector_search_service import get_lakebase_health
        rows = [{"usage_date": "2026-10-26", "project_id": "p1", "branch_id": "b1",
                 "project_name": "acme-db", "branch_name": "production",
                 "avg_connections": Decimal("1.2345"), "max_connections": Decimal("7"),
                 "max_db_size_bytes": Decimal("106930176")}]
        with patch(f"{_SVC}.execute_query", return_value=rows):
            out = get_lakebase_health(14, allowed_workspace_ids=None)
        assert out[0]["avg_connections"] == 1.23
        assert out[0]["max_connections"] == 7
        assert out[0]["max_db_size_bytes"] == 106930176
        assert out[0]["project_name"] == "acme-db"

    def test_wait_events_fall_back_to_unknown_labels(self):
        from backend.services.vector_search_service import get_lakebase_wait_events
        rows = [{"wait_class_name": None, "wait_event_name": None, "project_name": None,
                 "branch_name": None, "wait_event_count": 5, "wait_event_time": 12.5}]
        with patch(f"{_SVC}.execute_query", return_value=rows):
            out = get_lakebase_wait_events(allowed_workspace_ids=None)
        assert out[0]["wait_class_name"] == "unknown"
        assert out[0]["wait_event_time"] == 12.5

    def test_cost_by_project_falls_back_to_project_id_for_name(self):
        from decimal import Decimal
        from backend.services.vector_search_service import get_lakebase_cost_by_project
        rows = [{"project_id": "p9", "branch_id": "b9", "project_name": None, "branch_name": None,
                 "total_dbus": Decimal("10.5"), "total_cost_usd": Decimal("3.25")}]
        with patch(f"{_SVC}.execute_query", return_value=rows):
            out = get_lakebase_cost_by_project(30, allowed_workspace_ids=None)
        assert out[0]["project_name"] == "p9"
        assert out[0]["total_cost_usd"] == 3.25

    def test_trend_empty_when_nothing_synced(self):
        from backend.services.vector_search_service import get_vs_usage_trend
        with patch(f"{_SVC}.execute_query", return_value=[]):
            assert get_vs_usage_trend(30, allowed_workspace_ids=None) == []


class TestRetrievalMemoryRoutes:
    """Route wiring: the caller's allow-list reaches the service, and a missing
    index_name is rejected before any query runs."""

    def test_callers_requires_index_name(self, app_client):
        with patch(f"{_SVC}.execute_query") as mock_q:
            resp = app_client.get("/api/v1/vector-search/usage/callers")
            assert resp.status_code == 422
            mock_q.assert_not_called()

    def test_scoped_caller_gets_empty_without_query(self, app_client):
        # a non-account-admin with no workspace admin rights resolves to [] → no query
        with patch("backend.api.vector_search.get_allowed_workspace_ids", return_value=[]), \
             patch(f"{_SVC}.execute_query") as mock_q:
            for path in ("/api/v1/vector-search/usage/trend",
                         "/api/v1/vector-search/usage/indexes",
                         "/api/v1/vector-search/lakebase/health",
                         "/api/v1/vector-search/lakebase/wait-events",
                         "/api/v1/vector-search/lakebase/cost/by-project"):
                resp = app_client.get(path)
                assert resp.status_code == 200, path
                assert resp.json() == [], path
            mock_q.assert_not_called()

    def test_account_admin_scope_is_unrestricted(self, app_client):
        with patch("backend.api.vector_search.get_allowed_workspace_ids", return_value=None), \
             patch(f"{_SVC}.execute_query", return_value=[]) as mock_q:
            resp = app_client.get("/api/v1/vector-search/usage/indexes?days=7")
            assert resp.status_code == 200
            assert "ANY" not in mock_q.call_args[0][0]
            assert mock_q.call_args[0][1]["days"] == 7


class TestWorkspaceSelectorScope:
    """The page's workspace selector narrows the Retrieval & Memory views, but is
    intersected with the caller's scope so it can never widen access."""

    def _scope(self, allowed, selected):
        from unittest.mock import MagicMock
        from backend.api import vector_search as vs
        with patch.object(vs, "get_allowed_workspace_ids", return_value=allowed):
            return vs._scope(MagicMock(), selected)

    def test_no_selection_keeps_caller_scope(self):
        assert self._scope(None, None) is None
        assert self._scope(["ws1"], "") == ["ws1"]

    def test_admin_selection_narrows(self):
        assert self._scope(None, "ws1, ws2") == ["ws1", "ws2"]

    def test_scoped_selection_is_intersected(self):
        assert self._scope(["ws1", "ws2"], "ws2,ws9") == ["ws2"]   # ws9 not allowed → dropped
        assert self._scope(["ws1"], "ws9") == []                   # nothing allowed → empty, not all

    def test_route_passes_narrowed_scope(self, app_client):
        with patch("backend.api.vector_search.get_allowed_workspace_ids", return_value=None), \
             patch("backend.services.vector_search_service.get_lakebase_health", return_value=[]) as fn:
            r = app_client.get("/api/v1/vector-search/lakebase/health?days=14&workspace_ids=ws1,ws2")
        assert r.status_code == 200
        assert fn.call_args.kwargs["allowed_workspace_ids"] == ["ws1", "ws2"]


class TestCallersRouteScope:
    """/usage/callers takes a free-form workspace_id; the caller's allow-list must
    still bound it (caller identities are sensitive)."""

    def test_no_access_returns_empty_without_query(self, app_client):
        with patch("backend.api.vector_search.get_allowed_workspace_ids", return_value=[]), \
             patch(f"{_SVC}.execute_query") as q:
            r = app_client.get("/api/v1/vector-search/usage/callers?index_name=a.b.c&workspace_id=ws9")
        assert r.status_code == 200 and r.json() == []
        q.assert_not_called()

    def test_scoped_caller_is_bounded_by_allow_list(self, app_client):
        with patch("backend.api.vector_search.get_allowed_workspace_ids", return_value=["ws1"]), \
             patch(f"{_SVC}.execute_query", return_value=[]) as q:
            r = app_client.get("/api/v1/vector-search/usage/callers?index_name=a.b.c&workspace_id=ws9")
        assert r.status_code == 200
        sql, params = q.call_args[0][0], q.call_args[0][1]
        assert "ANY(%(ws)s)" in sql and params["ws"] == ["ws1"]
