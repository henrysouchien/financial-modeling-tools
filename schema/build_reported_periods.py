"""Reported-period metadata helpers for schema build orchestration."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import date, datetime
import logging
import sys
from typing import Any

from .periods import build_period_metadata as _periods_build_period_metadata


def _parent_attr(name: str, fallback):
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    return getattr(parent, name, fallback) if parent is not None else fallback


def _refresh_period_metadata(
    model: Any,
    reported_period_ends: dict[int, str],
) -> None:
    build_period_metadata = _parent_attr(
        "build_period_metadata",
        _periods_build_period_metadata,
    )
    model.time_structure.period_metadata = build_period_metadata(
        model.time_structure,
        reported_period_ends=reported_period_ends,
    )


def _collect_fmp_reported_period_ends_from_lookup(
    fmp_lookup: dict[str, dict[int, dict[str, Any]]],
    historical_periods: Iterable[int],
) -> dict[int, str]:
    reported: dict[int, str] = {}
    period_set = {int(period) for period in historical_periods}
    record_reported_period_end = _parent_attr(
        "_record_reported_period_end",
        _record_reported_period_end,
    )
    for endpoint in sorted(fmp_lookup):
        records_by_year = fmp_lookup.get(endpoint, {})
        for period in sorted(period_set):
            record = records_by_year.get(period)
            if record is None:
                continue
            record_reported_period_end(
                reported,
                period,
                record.get("date"),
                source=f"fmp:{endpoint}",
            )
    return reported


def _collect_edgar_reported_period_ends(
    results: Iterable[Any],
    historical_periods: Iterable[int],
) -> dict[int, str]:
    reported: dict[int, str] = {}
    period_set = {int(period) for period in historical_periods}
    record_reported_period_end = _parent_attr(
        "_record_reported_period_end",
        _record_reported_period_end,
    )
    for result in results:
        for period, reported_period_end in result.reported_period_ends_by_year.items():
            if int(period) not in period_set or int(period) not in result.values_dict:
                continue
            record_reported_period_end(
                reported,
                int(period),
                reported_period_end,
                source="edgar",
            )
    return reported


def _collect_routed_reported_period_ends(
    fmp_buffer: dict[str, Any],
    edgar_buffer: dict[str, Any],
    source_by_concept_year: dict[str, dict[int, str | None]],
    historical_periods: Iterable[int],
) -> dict[int, str]:
    reported: dict[int, str] = {}
    period_set = {int(period) for period in historical_periods}
    record_reported_period_end = _parent_attr(
        "_record_reported_period_end",
        _record_reported_period_end,
    )
    for concept_id, source_by_year in source_by_concept_year.items():
        for period, source in source_by_year.items():
            period = int(period)
            if period not in period_set or source is None:
                continue
            if source == "fmp":
                result = fmp_buffer.get(concept_id)
            else:
                result = edgar_buffer.get(concept_id)
            if result is None:
                continue
            record_reported_period_end(
                reported,
                period,
                result.reported_period_ends_by_year.get(period),
                source=f"{source}:{concept_id}",
            )
    return reported


def _record_reported_period_end(
    reported: dict[int, str],
    period: int,
    value: object,
    *,
    source: str,
) -> None:
    reported_period_end_value = _parent_attr(
        "_reported_period_end_value",
        _reported_period_end_value,
    )
    reported_period_end = reported_period_end_value(value)
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
    reported_period_end_value = _parent_attr(
        "_reported_period_end_value",
        _reported_period_end_value,
    )
    for key in ("period_end", "calendar_end", "end_current"):
        reported_period_end = reported_period_end_value(entry.get(key))
        if reported_period_end is not None:
            return reported_period_end
    return None
