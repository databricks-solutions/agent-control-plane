"""Tests for OBO authentication and authorization."""
import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from fastapi import Request
from backend.utils.auth import (
    get_current_user,
    require_admin,
    require_account_admin,
    _resolve_user,
    _cache_key,
    _get_cached,
    _put_cache,
    _SP_FALLBACK,
    UserInfo,
)


class TestSPFallback:
    """SP fallback should be read-only (non-admin)."""

    def test_sp_fallback_is_not_admin(self):
        assert _SP_FALLBACK.is_admin is False

    def test_sp_fallback_is_not_account_admin(self):
        assert _SP_FALLBACK.is_account_admin is False

    def test_sp_fallback_has_no_groups(self):
        assert _SP_FALLBACK.groups == []

    def test_sp_fallback_has_no_token(self):
        assert _SP_FALLBACK.token == ""

    def test_sp_fallback_username(self):
        assert _SP_FALLBACK.username == "service-principal"


class TestTokenCache:
    """Token → UserInfo cache."""

    def test_cache_miss_returns_none(self):
        assert _get_cached("nonexistent-token") is None

    def test_cache_hit_after_put(self, mock_user_info):
        token = "cache-test-token"
        _put_cache(token, mock_user_info)
        cached = _get_cached(token)
        assert cached is not None
        assert cached.username == mock_user_info.username

    def test_cache_key_is_deterministic(self):
        assert _cache_key("abc") == _cache_key("abc")
        assert _cache_key("abc") != _cache_key("xyz")


class TestGetCurrentUser:
    """get_current_user dependency."""

    @pytest.mark.asyncio
    async def test_no_token_returns_sp_fallback(self):
        request = MagicMock(spec=Request)
        request.headers = {}
        user = await get_current_user(request)
        assert user.username == "service-principal"
        assert user.is_admin is False

    @pytest.mark.asyncio
    async def test_obo_token_resolves_user(self):
        request = MagicMock(spec=Request)
        request.headers = {"x-forwarded-access-token": "test-token-123"}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "userName": "alice@databricks.com",
            "displayName": "Alice",
            "id": "99",
            "groups": [{"display": "users"}],
            "entitlements": [],
            "roles": [],
        }
        mock_response.raise_for_status = MagicMock()

        with patch("backend.utils.auth.httpx") as mock_httpx, \
             patch("backend.utils.auth.get_databricks_host", return_value="https://test.cloud.databricks.com"):
            mock_httpx.get.return_value = mock_response
            user = await get_current_user(request)

        assert user.username == "alice@databricks.com"
        assert user.is_admin is False  # not in admins group

    @pytest.mark.asyncio
    async def test_bearer_token_fallback(self):
        """Authorization: Bearer header works when no OBO token."""
        request = MagicMock(spec=Request)
        request.headers = {"authorization": "Bearer fallback-token"}

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "userName": "bob@databricks.com",
            "displayName": "Bob",
            "id": "100",
            "groups": [{"display": "admins"}],
            "entitlements": [],
            "roles": [],
        }
        mock_response.raise_for_status = MagicMock()

        with patch("backend.utils.auth.httpx") as mock_httpx, \
             patch("backend.utils.auth.get_databricks_host", return_value="https://test.cloud.databricks.com"):
            mock_httpx.get.return_value = mock_response
            user = await get_current_user(request)

        assert user.username == "bob@databricks.com"
        assert user.is_admin is True  # in admins group


class TestRequireAdmin:
    """require_admin raises 403 for non-admins."""

    @pytest.mark.asyncio
    async def test_admin_passes(self, mock_user_info):
        request = MagicMock(spec=Request)
        request.headers = {"x-forwarded-access-token": "admin-token"}

        with patch("backend.utils.auth.get_current_user", new_callable=AsyncMock, return_value=mock_user_info):
            user = await require_admin(request)
            assert user.is_admin is True

    @pytest.mark.asyncio
    async def test_non_admin_raises_403(self, mock_sp_user):
        request = MagicMock(spec=Request)
        request.headers = {}

        with patch("backend.utils.auth.get_current_user", new_callable=AsyncMock, return_value=mock_sp_user):
            from fastapi import HTTPException
            with pytest.raises(HTTPException) as exc_info:
                await require_admin(request)
            assert exc_info.value.status_code == 403


class TestRequireAccountAdmin:
    """require_account_admin raises 403 for non-account-admins."""

    @pytest.mark.asyncio
    async def test_non_account_admin_raises_403(self, mock_user_info):
        # mock_user_info has is_account_admin=False
        request = MagicMock(spec=Request)
        with patch("backend.utils.auth.get_current_user", new_callable=AsyncMock, return_value=mock_user_info):
            from fastapi import HTTPException
            with pytest.raises(HTTPException) as exc_info:
                await require_account_admin(request)
            assert exc_info.value.status_code == 403
