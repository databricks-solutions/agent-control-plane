"""Per-user / per-group token budgets for AI Gateway endpoints.

Budgets are measured in TOKENS (input + output combined), not dollars. Real-time
spend is computed on read from ``gateway_usage_daily`` — no separate spent column
is stored, which keeps the displayed figure in sync with the latest sync.

For authoritative dollar spend, see the Governance tab (sourced from
``system.billing.usage``, ~24h lag).

The ``workspace_id`` column on a budget is forward-compat: ``gateway_usage_daily``
does not currently carry workspace, so the filter is a no-op. The column is kept
so callers can already pass the field and we don't have to migrate the API later.
"""
from __future__ import annotations

import logging
import uuid
from datetime import date, datetime, timezone
from typing import Any, Dict, List, Optional

from backend.database import execute_one, execute_query, execute_update

logger = logging.getLogger(__name__)


# ── Schema bootstrap (called from main.py lifespan) ──────────────

_DDL = [
    """
    CREATE TABLE IF NOT EXISTS gateway_budgets (
      budget_id         TEXT PRIMARY KEY,
      principal         TEXT NOT NULL,
      principal_type    TEXT NOT NULL,
      endpoint_name     TEXT,
      workspace_id      TEXT,
      budget_tokens     BIGINT NOT NULL,
      period            TEXT NOT NULL DEFAULT 'month',
      alert_at_percent  INTEGER NOT NULL DEFAULT 80,
      is_active         BOOLEAN NOT NULL DEFAULT TRUE,
      created_by        TEXT NOT NULL,
      created_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
      updated_at        TIMESTAMP WITH TIME ZONE DEFAULT NOW()
    )
    """,
    "CREATE INDEX IF NOT EXISTS idx_gateway_budgets_principal ON gateway_budgets (principal)",
    "CREATE INDEX IF NOT EXISTS idx_gateway_budgets_endpoint  ON gateway_budgets (endpoint_name) WHERE endpoint_name IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS idx_gateway_budgets_active    ON gateway_budgets (is_active) WHERE is_active = TRUE",
]


def ensure_budgets_table() -> None:
    """Idempotent DDL — runs at app startup. Safe to call multiple times."""
    for stmt in _DDL:
        try:
            execute_update(stmt)
        except Exception as exc:
            logger.warning("gateway_budgets DDL warning: %s", exc)

# ── Period helpers ───────────────────────────────────────────────

_VALID_PERIODS = {"day", "month", "quarter", "year"}
_VALID_PRINCIPAL_TYPES = {"user", "group", "service_principal"}


def _period_start(period: str, today: Optional[date] = None) -> date:
    """Return the first day of the current period (calendar-aligned)."""
    d = today or date.today()
    if period == "day":
        return d
    if period == "month":
        return d.replace(day=1)
    if period == "quarter":
        quarter_first_month = ((d.month - 1) // 3) * 3 + 1
        return d.replace(month=quarter_first_month, day=1)
    if period == "year":
        return d.replace(month=1, day=1)
    # Unknown period — fail-safe to month
    logger.warning("Unknown budget period %r, defaulting to month", period)
    return d.replace(day=1)


def _alert_status(percent: float, alert_at: int) -> str:
    """ok | warning | breached."""
    if percent >= 100.0:
        return "breached"
    if percent >= float(alert_at):
        return "warning"
    return "ok"


# ── Compute spent ────────────────────────────────────────────────

def compute_spent(budget: Dict[str, Any]) -> Dict[str, Any]:
    """Sum input+output tokens for the budget's principal scope since period start.

    Returns ``{spent_tokens, percent_of_cap, alert_status, period_start}``.
    """
    period_start = _period_start(budget["period"])
    sql = """
        SELECT COALESCE(SUM(input_tokens + output_tokens), 0) AS spent_tokens
          FROM gateway_usage_daily
         WHERE requester = %s
           AND usage_date >= %s
    """
    params: List[Any] = [budget["principal"], period_start]

    if budget.get("endpoint_name"):
        sql += " AND endpoint_name = %s"
        params.append(budget["endpoint_name"])

    row = execute_one(sql, tuple(params)) or {}
    spent = int(row.get("spent_tokens") or 0)
    cap = int(budget["budget_tokens"]) or 0
    percent = (spent * 100.0 / cap) if cap > 0 else 0.0
    return {
        "spent_tokens": spent,
        "percent_of_cap": round(percent, 2),
        "alert_status": _alert_status(percent, int(budget["alert_at_percent"])),
        "period_start": period_start.isoformat(),
    }


# ── CRUD ─────────────────────────────────────────────────────────

_BASE_SELECT = """
    SELECT budget_id, principal, principal_type, endpoint_name, workspace_id,
           budget_tokens, period, alert_at_percent, is_active, created_by,
           created_at, updated_at
      FROM gateway_budgets
"""


def _annotate(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Attach computed spend fields to each row."""
    out = []
    for r in rows:
        out.append({**r, **compute_spent(r)})
    return out


def list_budgets(
    principal: Optional[str] = None,
    endpoint_name: Optional[str] = None,
    include_inactive: bool = False,
) -> List[Dict[str, Any]]:
    """List budgets with computed spend. Filters are optional."""
    sql = _BASE_SELECT + " WHERE 1=1"
    params: List[Any] = []
    if not include_inactive:
        sql += " AND is_active = TRUE"
    if principal:
        sql += " AND principal = %s"
        params.append(principal)
    if endpoint_name:
        sql += " AND endpoint_name = %s"
        params.append(endpoint_name)
    sql += " ORDER BY created_at DESC"
    rows = execute_query(sql, tuple(params))
    return _annotate(rows)


def get_budget(budget_id: str) -> Optional[Dict[str, Any]]:
    """Fetch one budget by id, with computed spend."""
    row = execute_one(_BASE_SELECT + " WHERE budget_id = %s", (budget_id,))
    if not row:
        return None
    return {**row, **compute_spent(row)}


def create_budget(payload: Dict[str, Any], created_by: str) -> Dict[str, Any]:
    """Insert a new budget. Caller is responsible for admin gating."""
    _validate_payload(payload, require_all=True)
    budget_id = str(uuid.uuid4())
    execute_update(
        """
        INSERT INTO gateway_budgets
            (budget_id, principal, principal_type, endpoint_name, workspace_id,
             budget_tokens, period, alert_at_percent, is_active, created_by)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s)
        """,
        (
            budget_id,
            payload["principal"],
            payload["principal_type"],
            payload.get("endpoint_name"),
            payload.get("workspace_id"),
            int(payload["budget_tokens"]),
            payload.get("period", "month"),
            int(payload.get("alert_at_percent", 80)),
            created_by,
        ),
    )
    created = get_budget(budget_id)
    assert created is not None, "freshly created budget should exist"
    return created


def update_budget(budget_id: str, payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Patch fields on an existing budget. Returns the updated row, or None if absent."""
    _validate_payload(payload, require_all=False)
    sets: List[str] = []
    params: List[Any] = []
    for field in ("principal", "principal_type", "endpoint_name", "workspace_id",
                  "period", "alert_at_percent", "budget_tokens", "is_active"):
        if field in payload:
            sets.append(f"{field} = %s")
            params.append(payload[field])
    if not sets:
        return get_budget(budget_id)
    sets.append("updated_at = %s")
    params.append(datetime.now(timezone.utc))
    params.append(budget_id)
    rowcount = execute_update(
        f"UPDATE gateway_budgets SET {', '.join(sets)} WHERE budget_id = %s",
        tuple(params),
    )
    if rowcount == 0:
        return None
    return get_budget(budget_id)


def delete_budget(budget_id: str) -> bool:
    """Soft-delete: set is_active=false. Returns True if a row was updated."""
    rowcount = execute_update(
        "UPDATE gateway_budgets SET is_active = FALSE, updated_at = %s WHERE budget_id = %s",
        (datetime.now(timezone.utc), budget_id),
    )
    return rowcount > 0


# ── Alerts ───────────────────────────────────────────────────────

def list_alerts() -> List[Dict[str, Any]]:
    """Active budgets currently in warning or breached state."""
    return [b for b in list_budgets() if b["alert_status"] != "ok"]


# ── Validation ───────────────────────────────────────────────────

def _validate_payload(payload: Dict[str, Any], require_all: bool) -> None:
    """Light validation. Pydantic handles types upstream; this catches enum + range issues."""
    if require_all:
        for required in ("principal", "principal_type", "budget_tokens"):
            if not payload.get(required):
                raise ValueError(f"Missing required field: {required}")
    if "principal_type" in payload and payload["principal_type"] not in _VALID_PRINCIPAL_TYPES:
        raise ValueError(
            f"principal_type must be one of {sorted(_VALID_PRINCIPAL_TYPES)}, got {payload['principal_type']!r}"
        )
    if "period" in payload and payload["period"] not in _VALID_PERIODS:
        raise ValueError(
            f"period must be one of {sorted(_VALID_PERIODS)}, got {payload['period']!r}"
        )
    if "alert_at_percent" in payload:
        v = int(payload["alert_at_percent"])
        if not 1 <= v <= 100:
            raise ValueError(f"alert_at_percent must be 1..100, got {v}")
    if "budget_tokens" in payload:
        v = int(payload["budget_tokens"])
        if v <= 0:
            raise ValueError(f"budget_tokens must be > 0, got {v}")
