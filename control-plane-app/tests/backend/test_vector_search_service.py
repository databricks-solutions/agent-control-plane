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
