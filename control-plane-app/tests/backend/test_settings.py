"""App-settings service + API (the admin-toggleable access mode)."""
from unittest.mock import patch

import pytest


class TestSettingsService:
    def test_default_access_mode_is_open(self):
        from backend.services import settings_service
        with patch("backend.services.settings_service.execute_query", return_value=[]):
            settings_service._cache_ts = 0.0  # force reload
            assert settings_service.get_access_mode() == "open"

    def test_stored_strict_is_returned(self):
        from backend.services import settings_service
        with patch(
            "backend.services.settings_service.execute_query",
            return_value=[{"key": "access_mode", "value": "strict"}],
        ):
            settings_service._cache_ts = 0.0
            assert settings_service.get_access_mode() == "strict"

    def test_invalid_stored_value_falls_back_to_default(self):
        from backend.services import settings_service
        with patch(
            "backend.services.settings_service.execute_query",
            return_value=[{"key": "access_mode", "value": "bogus"}],
        ):
            settings_service._cache_ts = 0.0
            assert settings_service.get_access_mode() == "open"

    def test_set_access_mode_rejects_invalid(self):
        from backend.services import settings_service
        with pytest.raises(ValueError):
            settings_service.set_access_mode("nonsense", updated_by="x@databricks.com")

    def test_set_access_mode_persists_and_invalidates_cache(self):
        from backend.services import settings_service
        with patch("backend.services.settings_service.execute_update") as upd:
            settings_service._cache = {"access_mode": "open"}
            settings_service._cache_ts = 9e18  # pretend fresh
            settings_service.set_access_mode("strict", updated_by="a@databricks.com")
            upd.assert_called_once()
            assert settings_service._cache_ts == 0.0  # invalidated


class TestSettingsApi:
    def test_get_returns_mode_and_can_edit(self, app_client):
        with patch("backend.services.settings_service.get_access_mode", return_value="open"):
            resp = app_client.get("/api/v1/settings")
        assert resp.status_code == 200
        body = resp.json()
        assert body["access_mode"] == "open"
        assert body["can_edit"] is True  # mock_user_info is_admin=True

    def test_put_forbidden_for_non_admin(self, unauth_client, monkeypatch):
        from backend.config import settings
        monkeypatch.setattr(settings, "obo_enabled", True)
        # unauth_client → SP fallback (is_admin False, is_account_admin False)
        resp = unauth_client.put("/api/v1/settings", json={"access_mode": "strict"})
        assert resp.status_code in (401, 403)

    def test_put_allowed_for_admin(self, app_client):
        with patch("backend.services.settings_service.set_access_mode", return_value="strict") as setter:
            resp = app_client.put("/api/v1/settings", json={"access_mode": "strict"})
        assert resp.status_code == 200
        assert resp.json()["access_mode"] == "strict"
        setter.assert_called_once()

    def test_put_rejects_invalid_mode(self, app_client):
        with patch("backend.services.settings_service.set_access_mode", side_effect=ValueError("bad")):
            resp = app_client.put("/api/v1/settings", json={"access_mode": "bogus"})
        assert resp.status_code == 400
