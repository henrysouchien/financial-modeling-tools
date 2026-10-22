"""Reported-period metadata helpers for schema build orchestration."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime
import logging
from typing import Any

from .periods import build_period_metadata
from .provider_port.contracts import ReportedPeriod


def _refresh_period_metadata(
    model: Any,
    reported_period_ends: dict[int, str],
) -> None:
    model.time_structure.period_metadata = build_period_metadata(
        model.time_structure,
        reported_period_ends=reported_period_ends,
    )


def _collect_reported_period_ends(
    reported_periods: Iterable[ReportedPeriod],
    historical_periods: Iterable[int],
) -> dict[int, str]:
    """Collect provider-neutral filing period ends for the workbook axis."""

    reported: dict[int, str] = {}
    period_set = {int(period) for period in historical_periods}
    for period in reported_periods:
        if int(period.fiscal_year) not in period_set:
            continue
        _record_reported_period_end(
            reported,
            int(period.fiscal_year),
            period.period_end,
            source=f"{period.source_provider or 'provider'}:statement_facts",
        )
    return reported




def _record_reported_period_end(
    reported: dict[int, str],
    period: int,
    value: object,
    *,
    source: str,
) -> None:
    reported_period_end = _reported_period_end_value(value)
    if reported_period_end is None:
        return
    existing = reported.get(int(period))
    if existing is None:
        reported[int(period)] = reported_period_end
    elif existing != reported_period_end:
        logging.warning(
            "reported_period_end_conflict period=%s existing=%s incoming=%s "
            "source=%s; keeping existing",
            period,
            existing,
            reported_period_end,
            source,
        )


def _reported_period_end_value(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    raw = str(value).strip()
    if not raw or raw.lower() in {"none", "nan", "nat"}:
        return None
    candidate = raw[:10] if len(raw) >= 10 else raw
    try:
        return date.fromisoformat(candidate).isoformat()
    except ValueError:
        return None


def _entry_reported_period_end(entry: dict) -> str | None:
    for key in ("period_end", "calendar_end", "end_current"):
        reported_period_end = _reported_period_end_value(entry.get(key))
        if reported_period_end is not None:
            return reported_period_end
    return None
