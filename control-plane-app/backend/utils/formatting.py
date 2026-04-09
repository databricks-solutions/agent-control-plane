"""Data formatting utilities."""
from datetime import datetime
from typing import Any, Dict


def format_timestamp(dt: datetime) -> str:
    """Format datetime to ISO string."""
    return dt.isoformat() if dt else None


def format_decimal(value: Any) -> float:
    """Format decimal value to float."""
    if value is None:
        return 0.0
    return float(value)


def format_percentage(value: Any) -> float:
    """Format percentage value."""
    if value is None:
        return 0.0
    return round(float(value), 2)
