"""Calendar metadata helpers for fiscal-local model periods."""

from __future__ import annotations

import calendar
import re
from datetime import date
from typing import Mapping

from .models import (
    PERIOD_MODE_QUARTERLY5,
    PERIOD_MODE_YEARLY,
    FiscalPeriodMetadata,
    TimeStructure,
    decode_period,
)

DateLike = date | str

_MONTH_ALIASES = {
    month.lower(): idx
    for idx, month in enumerate(calendar.month_name)
    if month
}
_MONTH_ALIASES.update(
    {
        month.lower(): idx
        for idx, month in enumerate(calendar.month_abbr)
        if month
    }
)
_MONTH_ALIASES["sept"] = 9


def parse_fiscal_year_end_month(fiscal_year_end: str) -> int:
    """Parse a fiscal-year-end string into a month number."""

    raw = str(fiscal_year_end or "").strip()
    if not raw:
        raise ValueError("fiscal_year_end is required to derive calendar period metadata")

    tokens = re.findall(r"[A-Za-z]+|\d+", raw)
    for token in tokens:
        month = _MONTH_ALIASES.get(token.lower())
        if month is not None:
            return month

    numbers = [int(token) for token in tokens if token.isdigit()]
    if not numbers:
        raise ValueError(f"Could not parse fiscal year end month: {fiscal_year_end!r}")

    month = numbers[1] if len(numbers) >= 3 and numbers[0] > 31 else numbers[0]
    if month < 1 or month > 12:
        raise ValueError(f"Fiscal year end month out of range: {fiscal_year_end!r}")
    return month


def calendar_quarter(dt: date) -> str:
    """Return the calendar quarter bucket for a date, e.g. 2024Q1."""

    quarter = ((dt.month - 1) // 3) + 1
    return f"{dt.year}Q{quarter}"


def metadata_for_fiscal_period(
    period_key: int,
    fiscal_year_end: str,
    period_mode: str,
    *,
    reported_period_end: DateLike | None = None,
) -> FiscalPeriodMetadata:
    """Build calendar metadata for one fiscal-local model period.

    The fiscal period key stays model-local. Peer comparisons should align on
    `calendar_quarter` / `comparison_key` for quarterly periods, or use an
    explicit annual policy instead of joining annual fiscal-year labels.
    """

    fiscal_year, fiscal_slot = decode_period(int(period_key), period_mode)
    period_type = _period_type(period_mode, fiscal_slot)
    duration_months = 12 if period_type == "annual" else 3
    calendar_end = _coerce_date(reported_period_end)
    period_end_source = "reported_period_end"

    if calendar_end is None:
        fye_month = parse_fiscal_year_end_month(fiscal_year_end)
        calendar_end = _derived_fiscal_period_end(fiscal_year, fiscal_slot, fye_month, period_type)
        period_end_source = "fiscal_year_end_month"

    quarter = calendar_quarter(calendar_end) if period_type == "quarter" else None
    return FiscalPeriodMetadata(
        period_key=int(period_key),
        period_mode=period_mode,
        fiscal_year=fiscal_year,
        fiscal_slot=fiscal_slot,
        period_type=period_type,
        calendar_start=_period_start_from_end(calendar_end, duration_months),
        calendar_end=calendar_end,
        calendar_year=calendar_end.year,
        calendar_quarter=quarter,
        comparison_key=quarter,
        duration_months=duration_months,
        period_end_source=period_end_source,
    )


def build_period_metadata(
    time_structure: TimeStructure,
    *,
    reported_period_ends: Mapping[int, DateLike] | None = None,
) -> dict[int, FiscalPeriodMetadata]:
    """Derive calendar metadata for every period in a TimeStructure."""

    period_ends = reported_period_ends or {}
    historical = time_structure.historical_periods or time_structure.historical_years
    projection = time_structure.projection_periods or time_structure.projection_years
    periods = list(dict.fromkeys(historical + projection))
    metadata: dict[int, FiscalPeriodMetadata] = {}
    fiscal_year_end = str(time_structure.fiscal_year_end or "").strip()
    for period in periods:
        reported_period_end = period_ends.get(int(period))
        if reported_period_end is None and not fiscal_year_end:
            continue
        metadata[int(period)] = metadata_for_fiscal_period(
            int(period),
            time_structure.fiscal_year_end,
            time_structure.period_mode,
            reported_period_end=reported_period_end,
        )
    return metadata


def _period_type(period_mode: str, fiscal_slot: int) -> str:
    if period_mode == PERIOD_MODE_YEARLY or fiscal_slot == 5:
        return "annual"
    if period_mode == PERIOD_MODE_QUARTERLY5:
        return "quarter"
    raise ValueError(f"Unknown period mode: {period_mode}")


def _coerce_date(value: DateLike | None) -> date | None:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    return date.fromisoformat(value)


def _derived_fiscal_period_end(
    fiscal_year: int,
    fiscal_slot: int,
    fiscal_year_end_month: int,
    period_type: str,
) -> date:
    if period_type == "annual":
        month = fiscal_year_end_month
        year = fiscal_year
    else:
        month = ((fiscal_year_end_month - (4 - fiscal_slot) * 3 - 1) % 12) + 1
        year = fiscal_year if month <= fiscal_year_end_month else fiscal_year - 1

    return date(year, month, calendar.monthrange(year, month)[1])


def _period_start_from_end(period_end: date, duration_months: int) -> date:
    start_month = ((period_end.month - duration_months) % 12) + 1
    start_year = period_end.year if start_month <= period_end.month else period_end.year - 1
    return date(start_year, start_month, 1)


__all__ = [
    "build_period_metadata",
    "calendar_quarter",
    "metadata_for_fiscal_period",
    "parse_fiscal_year_end_month",
]
