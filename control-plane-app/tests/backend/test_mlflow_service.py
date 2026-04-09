"""Tests for MLflow service — system tables, caching, cross-workspace."""
import pytest
from unittest.mock import patch, MagicMock


class TestSystemTableQueries:
    """System table query functions."""

    def test_search_experiments_system_tables_returns_list(self):
        mock_rows = [
            {"experiment_id": "1", "workspace_id": "ws1", "name": "exp1",
             "lifecycle_stage": "active", "last_update_time": "1000", "data_source": "system_table"}
        ]
        with patch("backend.services.mlflow_service._execute_system_sql", return_value=mock_rows):
            from backend.services.mlflow_service import search_experiments_system_tables
            result = search_experiments_system_tables(10)
            assert len(result) == 1
            assert result[0]["data_source"] == "system_table"

    def test_search_runs_system_tables_parses_json(self):
        import json
        mock_rows = [
            {"run_id": "r1", "experiment_id": "1", "workspace_id": "ws1",
             "status": "FINISHED", "start_time": "1000", "end_time": "2000",
             "user_id": "alice", "run_name": "test-run",
             "tags": json.dumps({"mlflow.runName": "test-run"}),
             "params": json.dumps({"lr": "0.01"}),
             "metrics": json.dumps([{"metric_name": "accuracy", "latest_value": 0.95}]),
             "data_source": "system_table"}
        ]
        with patch("backend.services.mlflow_service._execute_system_sql", return_value=mock_rows):
            from backend.services.mlflow_service import search_runs_system_tables
            result = search_runs_system_tables(10)
            assert len(result) == 1
            assert result[0]["tags"]["mlflow.runName"] == "test-run"
            assert result[0]["params"]["lr"] == "0.01"
            assert result[0]["metrics"][0]["metric_name"] == "accuracy"

    def test_search_experiments_empty_on_failure(self):
        with patch("backend.services.mlflow_service._execute_system_sql", return_value=[]):
            from backend.services.mlflow_service import search_experiments_system_tables
            result = search_experiments_system_tables(10)
            assert result == []


class TestCachedReads:
    """Lakebase cache read functions."""

    def test_get_cached_experiments_all(self):
        mock_rows = [{"experiment_id": "1", "workspace_id": "ws1", "name": "exp1"}]
        with patch("backend.services.mlflow_service.execute_query", return_value=mock_rows):
            from backend.services.mlflow_service import get_cached_experiments
            result = get_cached_experiments(None, 50)
            assert len(result) == 1

    def test_get_cached_experiments_by_workspace(self):
        mock_rows = [{"experiment_id": "1", "workspace_id": "ws1"}]
        with patch("backend.services.mlflow_service.execute_query", return_value=mock_rows) as mock_q:
            from backend.services.mlflow_service import get_cached_experiments
            get_cached_experiments("ws1", 50)
            # Should include workspace_id in the query
            call_args = mock_q.call_args[0][0]
            assert "workspace_id" in call_args

    def test_get_cached_traces_all(self):
        with patch("backend.services.mlflow_service.execute_query", return_value=[]):
            from backend.services.mlflow_service import get_cached_traces
            result = get_cached_traces(None, 50)
            assert result == []

    def test_get_cached_runs_all(self):
        with patch("backend.services.mlflow_service.execute_query", return_value=[]):
            from backend.services.mlflow_service import get_cached_runs
            result = get_cached_runs(None, 50)
            assert result == []

    def test_get_observability_workspaces(self):
        with patch("backend.services.mlflow_service.execute_query", return_value=[]):
            from backend.services.mlflow_service import get_observability_workspaces
            result = get_observability_workspaces()
            assert result == []


class TestCurrentWorkspaceQueries:
    """Current workspace MLflow REST API queries."""

    def test_search_experiments_calls_mlflow_api(self):
        with patch("backend.services.mlflow_service._post", return_value={"experiments": []}):
            from backend.services.mlflow_service import search_experiments
            result = search_experiments(50)
            assert result == []

    def test_search_traces_with_no_experiments(self):
        with patch("backend.services.mlflow_service._all_experiment_ids", return_value=[]):
            from backend.services.mlflow_service import search_traces
            result = search_traces(None, 50)
            assert result == []

    def test_get_trace_detail_returns_none_on_empty(self):
        with patch("backend.services.mlflow_service._get", return_value={}):
            from backend.services.mlflow_service import get_trace_detail
            result = get_trace_detail("nonexistent-id")
            assert result is None

    def test_search_registered_models(self):
        with patch("backend.services.mlflow_service._get", return_value={"registered_models": []}):
            from backend.services.mlflow_service import search_registered_models
            result = search_registered_models(100)
            assert result == []


class TestHTTPHelpers:
    """HTTP helper fallback chain."""

    def test_get_obo_first_then_sdk(self):
        """_get with user_token tries OBO first."""
        with patch("backend.services.mlflow_service._obo_get", return_value={"data": "obo"}) as mock_obo:
            from backend.services.mlflow_service import _get
            result = _get("/test", user_token="token123")
            assert result == {"data": "obo"}
            mock_obo.assert_called_once()

    def test_get_falls_back_to_sdk(self):
        """_get without user_token uses SDK."""
        with patch("backend.services.mlflow_service._sdk_get", return_value={"data": "sdk"}):
            from backend.services.mlflow_service import _get
            result = _get("/test")
            assert result == {"data": "sdk"}

    def test_get_returns_empty_on_total_failure(self):
        """_get returns {} when all methods fail."""
        with patch("backend.services.mlflow_service._sdk_get", return_value=None), \
             patch("backend.services.mlflow_service._httpx_get", return_value=None):
            from backend.services.mlflow_service import _get
            result = _get("/test")
            assert result == {}
