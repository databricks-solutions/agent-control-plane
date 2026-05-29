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
        with patch("backend.api.genie.settings") as mock_settings:
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
