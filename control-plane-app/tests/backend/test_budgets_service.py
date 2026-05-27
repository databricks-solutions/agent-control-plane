"""Tests for backend.services.budgets_service."""
from datetime import date
from unittest.mock import patch

import pytest


# ── Period boundaries ────────────────────────────────────────────

class TestPeriodStart:
    def test_day_returns_today(self):
        from backend.services.budgets_service import _period_start
        assert _period_start("day", date(2026, 5, 27)) == date(2026, 5, 27)

    def test_month_returns_first_of_month(self):
        from backend.services.budgets_service import _period_start
        assert _period_start("month", date(2026, 5, 27)) == date(2026, 5, 1)

    def test_quarter_returns_first_of_quarter(self):
        from backend.services.budgets_service import _period_start
        assert _period_start("quarter", date(2026, 1, 15)) == date(2026, 1, 1)
        assert _period_start("quarter", date(2026, 5, 27)) == date(2026, 4, 1)
        assert _period_start("quarter", date(2026, 8, 1)) == date(2026, 7, 1)
        assert _period_start("quarter", date(2026, 12, 31)) == date(2026, 10, 1)

    def test_year_returns_jan_1(self):
        from backend.services.budgets_service import _period_start
        assert _period_start("year", date(2026, 5, 27)) == date(2026, 1, 1)

    def test_unknown_period_falls_back_to_month(self):
        from backend.services.budgets_service import _period_start
        assert _period_start("fortnight", date(2026, 5, 27)) == date(2026, 5, 1)


# ── Alert status thresholds ─────────────────────────────────────

class TestAlertStatus:
    def test_below_threshold_is_ok(self):
        from backend.services.budgets_service import _alert_status
        assert _alert_status(0.0, 80) == "ok"
        assert _alert_status(79.9, 80) == "ok"

    def test_at_or_above_threshold_is_warning(self):
        from backend.services.budgets_service import _alert_status
        assert _alert_status(80.0, 80) == "warning"
        assert _alert_status(99.9, 80) == "warning"

    def test_at_or_above_100_is_breached(self):
        from backend.services.budgets_service import _alert_status
        assert _alert_status(100.0, 80) == "breached"
        assert _alert_status(250.0, 80) == "breached"

    def test_custom_threshold(self):
        from backend.services.budgets_service import _alert_status
        assert _alert_status(50.0, 50) == "warning"
        assert _alert_status(49.9, 50) == "ok"


# ── compute_spent ────────────────────────────────────────────────

class TestComputeSpent:
    def _budget(self, **overrides):
        base = {
            "principal": "alice@example.com",
            "period": "month",
            "budget_tokens": 1_000_000,
            "alert_at_percent": 80,
            "endpoint_name": None,
        }
        base.update(overrides)
        return base

    def test_ok_when_well_under_cap(self):
        from backend.services import budgets_service
        with patch.object(budgets_service, "execute_one", return_value={"spent_tokens": 100_000}):
            r = budgets_service.compute_spent(self._budget())
        assert r["spent_tokens"] == 100_000
        assert r["percent_of_cap"] == 10.0
        assert r["alert_status"] == "ok"

    def test_warning_when_at_threshold(self):
        from backend.services import budgets_service
        with patch.object(budgets_service, "execute_one", return_value={"spent_tokens": 850_000}):
            r = budgets_service.compute_spent(self._budget())
        assert r["alert_status"] == "warning"

    def test_breached_when_over_cap(self):
        from backend.services import budgets_service
        with patch.object(budgets_service, "execute_one", return_value={"spent_tokens": 1_200_000}):
            r = budgets_service.compute_spent(self._budget())
        assert r["alert_status"] == "breached"
        assert r["percent_of_cap"] == 120.0

    def test_empty_usage_returns_zero(self):
        from backend.services import budgets_service
        with patch.object(budgets_service, "execute_one", return_value=None):
            r = budgets_service.compute_spent(self._budget())
        assert r["spent_tokens"] == 0
        assert r["percent_of_cap"] == 0.0
        assert r["alert_status"] == "ok"

    def test_endpoint_scope_added_to_query(self):
        """When endpoint_name is set on the budget, query filters by it."""
        from backend.services import budgets_service
        captured = {}

        def fake_execute_one(sql, params):
            captured["sql"] = sql
            captured["params"] = params
            return {"spent_tokens": 0}

        with patch.object(budgets_service, "execute_one", side_effect=fake_execute_one):
            budgets_service.compute_spent(self._budget(endpoint_name="databricks-gemini-3-5-flash"))

        assert "endpoint_name = %s" in captured["sql"]
        assert "databricks-gemini-3-5-flash" in captured["params"]

    def test_no_endpoint_scope_omits_filter(self):
        """When endpoint_name is None on the budget, query does NOT filter by endpoint."""
        from backend.services import budgets_service
        captured = {}

        def fake_execute_one(sql, params):
            captured["sql"] = sql
            captured["params"] = params
            return {"spent_tokens": 0}

        with patch.object(budgets_service, "execute_one", side_effect=fake_execute_one):
            budgets_service.compute_spent(self._budget(endpoint_name=None))

        assert "endpoint_name = %s" not in captured["sql"]


# ── Validation ───────────────────────────────────────────────────

class TestValidation:
    def test_invalid_principal_type_rejected(self):
        from backend.services.budgets_service import _validate_payload
        with pytest.raises(ValueError, match="principal_type"):
            _validate_payload({"principal_type": "robot"}, require_all=False)

    def test_invalid_period_rejected(self):
        from backend.services.budgets_service import _validate_payload
        with pytest.raises(ValueError, match="period"):
            _validate_payload({"period": "fortnight"}, require_all=False)

    def test_alert_percent_out_of_range_rejected(self):
        from backend.services.budgets_service import _validate_payload
        with pytest.raises(ValueError, match="alert_at_percent"):
            _validate_payload({"alert_at_percent": 0}, require_all=False)
        with pytest.raises(ValueError, match="alert_at_percent"):
            _validate_payload({"alert_at_percent": 101}, require_all=False)

    def test_zero_or_negative_budget_rejected(self):
        from backend.services.budgets_service import _validate_payload
        with pytest.raises(ValueError, match="budget_tokens"):
            _validate_payload({"budget_tokens": 0}, require_all=False)
        with pytest.raises(ValueError, match="budget_tokens"):
            _validate_payload({"budget_tokens": -1}, require_all=False)

    def test_missing_required_fields_rejected_on_create(self):
        from backend.services.budgets_service import _validate_payload
        with pytest.raises(ValueError, match="Missing required field"):
            _validate_payload({"principal": "alice"}, require_all=True)
        with pytest.raises(ValueError, match="Missing required field"):
            _validate_payload(
                {"principal": "alice", "principal_type": "user"},
                require_all=True,
            )

    def test_valid_payload_passes(self):
        from backend.services.budgets_service import _validate_payload
        _validate_payload(
            {
                "principal": "alice@example.com",
                "principal_type": "user",
                "budget_tokens": 1_000_000,
                "period": "month",
                "alert_at_percent": 80,
            },
            require_all=True,
        )
