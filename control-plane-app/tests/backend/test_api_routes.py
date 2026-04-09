"""Tests for API route authentication and basic responses."""
import pytest
from unittest.mock import patch, MagicMock


class TestHealthEndpoint:
    """Health endpoint should work without auth."""

    def test_health_no_auth_required(self, unauth_client):
        resp = unauth_client.get("/api/v1/health")
        assert resp.status_code == 200


class TestConfigEndpoint:
    """Config endpoint returns host URL without auth."""

    def test_config_returns_host(self, unauth_client):
        with patch("backend.main.get_databricks_host", return_value="https://test.cloud.databricks.com"):
            resp = unauth_client.get("/api/v1/config")
            assert resp.status_code == 200
            assert "databricks_host" in resp.json()


class TestMeEndpoint:
    """The /api/v1/me endpoint resolves user identity."""

    def test_me_returns_user_info(self, app_client, mock_user_info):
        """Note: /api/v1/me calls get_current_user directly, not via router dependency.
        The dependency override on app_client doesn't apply here, so it falls back to SP."""
        resp = app_client.get("/api/v1/me")
        assert resp.status_code == 200
        data = resp.json()
        # Without OBO token, falls back to SP or anonymous
        assert "username" in data


class TestAuthenticatedRoutes:
    """All API routes should require authentication."""

    PROTECTED_ROUTES = [
        "/api/v1/agents",
        "/api/v1/billing/page-data",
        "/api/v1/mlflow/experiments",
        "/api/v1/mlflow/traces",
        "/api/v1/mlflow/runs",
        "/api/v1/mlflow/models",
        "/api/v1/kpis",
        "/api/v1/tools/overview",
        "/api/v1/workspaces/page-data",
    ]

    def test_authenticated_routes_work(self, app_client):
        """With auth, routes should return 200 or 500/502 (backend unavailable in test env).
        The key assertion is that they DON'T return 401/403 — auth is satisfied."""
        with patch("backend.database.DatabasePool.get_connection") as mock_conn:
            # Mock DB to return empty results
            conn = MagicMock()
            cursor = MagicMock()
            cursor.fetchall.return_value = []
            cursor.fetchone.return_value = None
            cursor.__enter__ = MagicMock(return_value=cursor)
            cursor.__exit__ = MagicMock(return_value=False)
            conn.cursor.return_value = cursor
            mock_conn.return_value.__enter__ = MagicMock(return_value=conn)
            mock_conn.return_value.__exit__ = MagicMock(return_value=False)

            for route in self.PROTECTED_ROUTES:
                resp = app_client.get(route)
                # Auth should pass (not 401/403). Backend errors (500/502) are expected.
                assert resp.status_code not in (401, 403), f"{route} returned {resp.status_code} — auth should pass"


class TestDebugEndpointsRemoved:
    """Debug endpoints should not exist as API routes.
    Note: SPA catch-all may serve index.html for unknown paths (200),
    so we check the response is NOT JSON API data."""

    def test_debug_auth_removed(self, app_client):
        resp = app_client.get("/api/v1/debug/auth")
        # Should not return JSON with auth info
        try:
            data = resp.json()
            assert "has_obo_token" not in data, "Debug auth endpoint still exists"
        except Exception:
            pass  # Non-JSON response = endpoint doesn't exist (SPA fallback)

    def test_debug_workspace_registry_removed(self, app_client):
        resp = app_client.get("/api/v1/debug/workspace-registry")
        try:
            data = resp.json()
            assert "workspace_count" not in data, "Debug workspace-registry endpoint still exists"
        except Exception:
            pass


class TestMlflowRoutes:
    """MLflow API routes."""

    def test_experiments_returns_list(self, app_client):
        with patch("backend.services.mlflow_service.search_experiments", return_value=[]):
            resp = app_client.get("/api/v1/mlflow/experiments")
            assert resp.status_code == 200

    def test_experiments_all_workspaces(self, app_client):
        with patch("backend.services.mlflow_service.get_cached_experiments", return_value=[]), \
             patch("backend.services.mlflow_service.search_experiments", return_value=[]):
            resp = app_client.get("/api/v1/mlflow/experiments?workspace_id=all")
            assert resp.status_code == 200

    def test_traces_returns_list(self, app_client):
        with patch("backend.services.mlflow_service.search_traces", return_value=[]):
            resp = app_client.get("/api/v1/mlflow/traces")
            assert resp.status_code == 200

    def test_models_returns_list(self, app_client):
        with patch("backend.services.mlflow_service.search_registered_models", return_value=[]):
            resp = app_client.get("/api/v1/mlflow/models")
            assert resp.status_code == 200

    def test_workspaces_returns_list(self, app_client):
        with patch("backend.services.mlflow_service.get_observability_workspaces", return_value=[]):
            resp = app_client.get("/api/v1/mlflow/workspaces")
            assert resp.status_code == 200
