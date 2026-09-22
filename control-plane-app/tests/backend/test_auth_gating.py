"""Strict auth dependencies + write-gating (security review findings #3, #6).

Covers:
  • require_user — 401 for the SP fallback when OBO is enabled, allowed when
    OBO is disabled (single-identity deployments).
  • Mutation endpoints gated behind require_admin (403 for non-admins).
  • Unknown /api paths return a real JSON 404, not the SPA HTML with 200.
"""
from unittest.mock import patch

import pytest
from fastapi import HTTPException


class TestRequireUser:
    async def test_sp_fallback_401_when_obo_enabled(self, mock_sp_user, monkeypatch):
        from backend.config import settings
        from backend.utils.auth import require_user
        monkeypatch.setattr(settings, "obo_enabled", True)
        with pytest.raises(HTTPException) as ei:
            await require_user(mock_sp_user)
        assert ei.value.status_code == 401

    async def test_sp_fallback_allowed_when_obo_disabled(self, mock_sp_user, monkeypatch):
        from backend.config import settings
        from backend.utils.auth import require_user
        monkeypatch.setattr(settings, "obo_enabled", False)
        assert await require_user(mock_sp_user) is mock_sp_user

    async def test_real_user_allowed(self, mock_user_info):
        from backend.utils.auth import require_user
        assert await require_user(mock_user_info) is mock_user_info


class TestWriteGating:
    def test_serving_requires_auth(self, unauth_client, monkeypatch):
        from backend.config import settings
        monkeypatch.setattr(settings, "obo_enabled", True)
        resp = unauth_client.get("/api/v1/serving/queryable-endpoints")
        assert resp.status_code == 401

    def test_billing_refresh_forbidden_for_non_admin(self, unauth_client, monkeypatch):
        from backend.config import settings
        monkeypatch.setattr(settings, "obo_enabled", True)
        resp = unauth_client.post("/api/v1/billing/cache/refresh")
        # SP fallback is not an admin → gated (403), never silently allowed.
        assert resp.status_code in (401, 403)

    def test_billing_refresh_allowed_for_admin(self, app_client):
        with patch("backend.services.billing_service.force_refresh_async"), \
             patch("backend.services.billing_service.maybe_refresh_async"):
            resp = app_client.post("/api/v1/billing/cache/refresh")
            assert resp.status_code not in (401, 403)

    def test_agents_sync_gated_for_non_admin(self, unauth_client, monkeypatch):
        from backend.config import settings
        monkeypatch.setattr(settings, "obo_enabled", True)
        resp = unauth_client.post("/api/v1/agents/sync")
        assert resp.status_code in (401, 403)


class TestUnknownApiRoute:
    def test_unknown_api_returns_json_404(self, unauth_client):
        resp = unauth_client.get("/api/v1/nonexistent-xyz")
        assert resp.status_code == 404
        # Real JSON 404, not the SPA HTML page.
        assert resp.json().get("detail") == "Not Found"
