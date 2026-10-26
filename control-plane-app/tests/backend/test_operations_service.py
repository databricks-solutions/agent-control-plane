"""Tests for operations_service._fetch_usage_metrics.

Regression guard for gh #74: the per-endpoint usage query referenced
``system.serving.endpoint_usage.execution_time_ms``, a column that does not
exist, so the whole statement failed and the Operations page silently showed no
usage. Counts now come from endpoint_usage (no latency column) and latency is
enriched fail-open from system.ai_gateway.usage.
"""
from unittest.mock import patch


def _counts_row():
    return {"endpoint_name": "ep1", "request_count": 100, "error_count": 5, "total_tokens": 1000}


class TestFetchUsageMetrics:

    def test_counts_query_has_no_execution_time_ms(self):
        """The endpoint_usage query must never select the non-existent latency column."""
        seen = {}

        def fake_exec(sql, *a, **k):
            if "endpoint_usage" in sql:
                seen["counts_sql"] = sql
                return [_counts_row()]
            if "ai_gateway.usage" in sql:
                return [{"endpoint_name": "ep1", "avg_latency_ms": 12.5, "p95_latency_ms": 40.0},
                        {"endpoint_name": "other", "avg_latency_ms": 1.0, "p95_latency_ms": 2.0}]
            return []

        with patch("backend.services.operations_service._execute_system_sql", side_effect=fake_exec):
            from backend.services.operations_service import _fetch_usage_metrics
            result = _fetch_usage_metrics(hours=1)

        assert "execution_time_ms" not in seen["counts_sql"]
        assert result["ep1"]["request_count"] == 100
        assert result["ep1"]["error_count"] == 5
        # latency enriched from the gateway for the endpoint we counted …
        assert result["ep1"]["avg_latency_ms"] == 12.5
        assert result["ep1"]["p95_latency_ms"] == 40.0
        # … and a gateway-only endpoint we never counted is NOT invented
        assert "other" not in result

    def test_latency_enrichment_is_fail_open(self):
        """If the gateway latency query fails, counts still come back (no crash)."""
        def fake_exec(sql, *a, **k):
            if "endpoint_usage" in sql:
                return [_counts_row()]
            raise Exception("ai_gateway.usage not present on this account")

        with patch("backend.services.operations_service._execute_system_sql", side_effect=fake_exec):
            from backend.services.operations_service import _fetch_usage_metrics
            result = _fetch_usage_metrics(hours=1)

        assert result["ep1"]["request_count"] == 100
        assert "avg_latency_ms" not in result["ep1"]  # absent → downstream treats as None/0

    def test_counts_query_failure_returns_empty(self):
        """A failure on the primary (counts) query degrades to {}."""
        with patch("backend.services.operations_service._execute_system_sql",
                   side_effect=Exception("warehouse unavailable")):
            from backend.services.operations_service import _fetch_usage_metrics
            assert _fetch_usage_metrics(hours=1) == {}
