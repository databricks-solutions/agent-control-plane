"""Tests for /api/v1/genie/space-info."""
from unittest.mock import patch


class TestGenieSpaceInfo:
    """Feature-flag gating + URL composition."""

    def test_404_when_feature_off(self, app_client):
        with patch("backend.api.genie.settings") as mock_settings:
            mock_settings.feature_genie_enabled = False
            r = app_client.get("/api/v1/genie/space-info")
            assert r.status_code == 404

    def test_returns_available_false_when_no_space_id(self, app_client):
        with patch("backend.api.genie.settings") as mock_settings, \
             patch("backend.api.genie.execute_query", return_value=[]):
            mock_settings.feature_genie_enabled = True
            mock_settings.genie_space_id = ""
            r = app_client.get("/api/v1/genie/space-info")
            assert r.status_code == 200
            assert r.json() == {"available": False, "space_id": None, "space_url": None}

    def test_space_info_uses_cache_meta_when_env_empty(self, app_client):
        """Env pin empty → resolve the workflow-provisioned id from billing_cache_meta
        (the PR's core behavior: no redeploy needed). value is stripped."""
        host = "https://fevm-serverless-b4nc10.cloud.databricks.com"
        with patch("backend.api.genie.settings") as mock_settings, \
             patch("backend.api.genie.get_databricks_host", return_value=host), \
             patch("backend.api.genie.execute_query", return_value=[{"value_text": " abc123 "}]):
            mock_settings.feature_genie_enabled = True
            mock_settings.genie_space_id = ""
            r = app_client.get("/api/v1/genie/space-info")
            assert r.status_code == 200
            body = r.json()
            assert body["available"] is True
            assert body["space_id"] == "abc123"
            assert body["space_url"] == f"{host}/genie/rooms/abc123"

    def test_env_pin_overrides_cache_meta(self, app_client):
        """An explicit GENIE_SPACE_ID wins over the meta table (execute_query not consulted)."""
        host = "https://x.cloud.databricks.com"
        with patch("backend.api.genie.settings") as mock_settings, \
             patch("backend.api.genie.get_databricks_host", return_value=host), \
             patch("backend.api.genie.execute_query", side_effect=AssertionError("should not be called")):
            mock_settings.feature_genie_enabled = True
            mock_settings.genie_space_id = "pinned-id"
            r = app_client.get("/api/v1/genie/space-info")
            assert r.status_code == 200
            assert r.json()["space_id"] == "pinned-id"

    def test_space_info_unavailable_when_cache_query_raises(self, app_client):
        """The meta-table lookup fails open → available:false, not a 500."""
        with patch("backend.api.genie.settings") as mock_settings, \
             patch("backend.api.genie.execute_query", side_effect=Exception("db down")):
            mock_settings.feature_genie_enabled = True
            mock_settings.genie_space_id = ""
            r = app_client.get("/api/v1/genie/space-info")
            assert r.status_code == 200
            assert r.json() == {"available": False, "space_id": None, "space_url": None}

    def test_returns_url_when_configured(self, app_client):
        host = "https://fevm-serverless-b4nc10.cloud.databricks.com"
        with patch("backend.api.genie.settings") as mock_settings, \
             patch("backend.api.genie.get_databricks_host", return_value=host):
            mock_settings.feature_genie_enabled = True
            mock_settings.genie_space_id = "01f15b51d23812d695fd6c12d448cdf4"
            r = app_client.get("/api/v1/genie/space-info")
            assert r.status_code == 200
            assert r.json() == {
                "available": True,
                "space_id": "01f15b51d23812d695fd6c12d448cdf4",
                # /rooms/ is the UI route; /spaces/ is the REST API.
                "space_url": f"{host}/genie/rooms/01f15b51d23812d695fd6c12d448cdf4",
            }

    def test_unavailable_when_host_missing(self, app_client):
        with patch("backend.api.genie.settings") as mock_settings, \
             patch("backend.api.genie.get_databricks_host", return_value=""):
            mock_settings.feature_genie_enabled = True
            mock_settings.genie_space_id = "01f15..."
            r = app_client.get("/api/v1/genie/space-info")
            assert r.status_code == 200
            body = r.json()
            assert body["available"] is False
            assert body["space_id"] == "01f15..."
            assert body["space_url"] is None


class TestGenieProxyErrors:
    def test_upstream_error_keeps_status_instead_of_200(self, app_client):
        """Regression: a 4xx/5xx from Genie was returned as HTTP 200, so the overlay
        polled undefined ids for 90s and showed 'took too long'."""
        class Resp:
            status_code = 403
            text = "forbidden"
            def json(self):
                return {"error_code": "PERMISSION_DENIED", "message": "no access to space"}

        with patch("backend.api.genie.settings") as s, \
             patch("backend.api.genie.get_databricks_host", return_value="https://h"), \
             patch("backend.api.genie.httpx.request", return_value=Resp()):
            s.feature_genie_enabled = True
            s.genie_space_id = "sp1"
            r = app_client.post("/api/v1/genie/conversations", json={"content": "hi"},
                                headers={"x-forwarded-access-token": "tok"})
        assert r.status_code == 403
        assert "no access to space" in r.json()["detail"]
