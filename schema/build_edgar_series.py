"""EDGAR series parsing helpers for schema build orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field, replace
import logging
import sys
from typing import Any, Dict

from .build_formula_refs import _safe_int as _safe_int_fallback
from .build_reported_periods import _entry_reported_period_end as _entry_reported_period_end_fallback
from .models import DataSourceMapping, EdgarProvenance
from .registry_cache import EquivalenceGroup, get_registry_cache as _get_registry_cache_fallback
from .source_values import (
    SourceValue,
    choose_preferred_source_value as _choose_preferred_source_value_fallback,
    normalize_edgar_source_value as _normalize_edgar_source_value_fallback,
)


@dataclass
class ParsedEdgarSeriesResult:
    values_dict: Dict[int, float]
    failed_years: set[int]
    entry_failed: int
    provenance_by_year: Dict[int, EdgarProvenance] = field(default_factory=dict)
    source_values_by_year: Dict[int, SourceValue] = field(default_factory=dict)
    reported_period_ends_by_year: Dict[int, str] = field(default_factory=dict)


def _parent_attr(name: str, fallback: Any) -> Any:
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    if parent is None:
        from . import build as parent
    return getattr(parent, name, fallback)


def _required_parent_attr(name: str) -> Any:
    helper = _parent_attr(name, None)
    if helper is None:
        raise RuntimeError(f"schema.build helper '{name}' is unavailable")
    return helper


def _parse_edgar_series(
    response: dict,
    concept_id: str,
    concept: DataSourceMapping,
    *,
    ticker: str,
    requested_tag: str,
    allow_equivalent_tags: bool = False,
) -> tuple[Dict[int, float], set[int], int]:
    values_dict, failed_years, entry_failed, _provenance_by_year = _parse_edgar_series_result(
        response,
        concept_id,
        concept,
        ticker=ticker,
        requested_tag=requested_tag,
        allow_equivalent_tags=allow_equivalent_tags,
    )
    return values_dict, failed_years, entry_failed


def _parse_edgar_series_result(
    response: dict,
    concept_id: str,
    concept: DataSourceMapping,
    *,
    ticker: str,
    requested_tag: str | None = None,
    requested_registry_group_id: str | None = None,
    requested_registry_group: EquivalenceGroup | None = None,
    allow_equivalent_tags: bool = False,
) -> tuple[Dict[int, float], set[int], int, Dict[int, EdgarProvenance]]:
    """Parse EDGAR series response with legacy tuple return shape."""

    parsed = _parse_edgar_series_source_result(
        response,
        concept_id,
        concept,
        ticker=ticker,
        requested_tag=requested_tag,
        requested_registry_group_id=requested_registry_group_id,
        requested_registry_group=requested_registry_group,
        allow_equivalent_tags=allow_equivalent_tags,
    )
    return (
        parsed.values_dict,
        parsed.failed_years,
        parsed.entry_failed,
        parsed.provenance_by_year,
    )


def _parse_edgar_series_source_result(
    response: dict,
    concept_id: str,
    concept: DataSourceMapping,
    *,
    ticker: str,
    requested_tag: str | None = None,
    requested_registry_group_id: str | None = None,
    requested_registry_group: EquivalenceGroup | None = None,
    allow_equivalent_tags: bool = False,
) -> ParsedEdgarSeriesResult:
    """Parse EDGAR series response with raw source-value metadata."""

    safe_int = _parent_attr("_safe_int", _safe_int_fallback)
    tags_equivalent = _required_parent_attr("_tags_equivalent")
    equivalent_tag_validated_by_registry = _required_parent_attr(
        "_equivalent_tag_validated_by_registry"
    )
    validated_registry_group_for_metric_tag = _required_parent_attr(
        "_validated_registry_group_for_metric_tag"
    )
    entry_reported_period_end = _parent_attr(
        "_entry_reported_period_end",
        _entry_reported_period_end_fallback,
    )
    edgar_negate_enabled = _required_parent_attr("_edgar_negate_enabled")
    registry_equivalence_value_conflict = _required_parent_attr(
        "_registry_equivalence_value_conflict"
    )
    normalize_edgar_source_value = _parent_attr(
        "normalize_edgar_source_value",
        _normalize_edgar_source_value_fallback,
    )
    choose_preferred_source_value = _parent_attr(
        "choose_preferred_source_value",
        _choose_preferred_source_value_fallback,
    )
    get_registry_cache = _parent_attr("get_registry_cache", _get_registry_cache_fallback)
    registry_equivalence_value_tolerance_pct = _parent_attr(
        "_REGISTRY_EQUIVALENCE_VALUE_TOLERANCE_PCT",
        0.01,
    )
    registry_equivalence_value_tolerance_abs_m = _parent_attr(
        "_REGISTRY_EQUIVALENCE_VALUE_TOLERANCE_ABS_M",
        0.1,
    )

    series = response.get("series")
    if not isinstance(series, list):
        series = []

    values_dict: Dict[int, float] = {}
    failed_years: set[int] = set()
    entry_failed = 0
    provenance_by_year: Dict[int, EdgarProvenance] = {}
    source_values_by_year: Dict[int, SourceValue] = {}
    reported_period_ends_by_year: Dict[int, str] = {}
    registry_value_conflict_years: set[int] = set()

    for entry in series:
        if not isinstance(entry, dict):
            continue
        entry_status = str(entry.get("status") or "").lower()
        year = safe_int(entry.get("year"), default=None)
        if entry_status in {"error", "locked"}:
            if year is not None:
                failed_years.add(year)
            entry_failed += 1
            continue
        if entry_status != "ok":
            continue
        returned_tag = entry.get("metric_tag")
        matched_registry_group: EquivalenceGroup | None = None
        if requested_registry_group_id:
            if not isinstance(returned_tag, str) or not returned_tag:
                if year is not None:
                    failed_years.add(year)
                entry_failed += 1
                continue
            returned_group_id = entry.get("equivalence_group_id")
            if not isinstance(returned_group_id, str) or not returned_group_id:
                logging.warning(
                    "edgar_registry_group_mismatch ticker=%s requested=%s returned=%s",
                    ticker,
                    requested_registry_group_id,
                    returned_group_id,
                )
                if year is not None:
                    failed_years.add(year)
                entry_failed += 1
                continue
            matched_registry_group = validated_registry_group_for_metric_tag(
                ticker=ticker,
                requested_group_id=requested_registry_group_id,
                returned_group_id=returned_group_id,
                returned_tag=returned_tag,
                requested_group=requested_registry_group,
            )
            if matched_registry_group is None:
                if year is not None:
                    failed_years.add(year)
                entry_failed += 1
                continue
        elif (
            not isinstance(returned_tag, str)
            or not returned_tag
            or (
                not tags_equivalent(requested_tag or "", returned_tag)
                and not (
                    allow_equivalent_tags
                    and equivalent_tag_validated_by_registry(
                        ticker=ticker,
                        requested_tag=requested_tag,
                        returned_tag=returned_tag,
                        returned_group_id=(
                            entry.get("equivalence_group_id")
                            if isinstance(entry.get("equivalence_group_id"), str)
                            else None
                        ),
                    )
                )
            )
        ):
            logging.warning(
                "edgar_tag_mismatch ticker=%s requested=%s returned=%s year=%s",
                ticker,
                requested_tag,
                returned_tag,
                year,
            )
            if year is not None:
                failed_years.add(year)
            entry_failed += 1
            continue
        if year is None or entry.get("value") is None:
            continue

        reported_period_end = entry_reported_period_end(entry)
        source_value = normalize_edgar_source_value(
            entry["value"],
            entry.get("scale"),
            concept_id,
            source="edgar",
            tag=returned_tag,
            year=year,
            source_ref=entry.get("source_ref"),
        )
        scaled_value = source_value.normalized_value
        if scaled_value is None:
            continue
        if edgar_negate_enabled(concept):
            scaled_value = -abs(scaled_value)
            source_value = replace(source_value, normalized_value=scaled_value)
        provenance = EdgarProvenance(
            registry_group_id=(
                matched_registry_group.group_id
                if matched_registry_group is not None
                else None
            ),
            metric_tag=returned_tag,
            registry_revision=(
                get_registry_cache().registry_revision
                if matched_registry_group is not None
                else None
            ),
        )
        existing_source_value = source_values_by_year.get(year)
        if (
            matched_registry_group is not None
            and existing_source_value is not None
            and registry_equivalence_value_conflict(existing_source_value, source_value)
        ):
            logging.warning(
                "edgar_registry_equivalence_value_conflict ticker=%s group=%s year=%s "
                "existing_tag=%s existing_value=%s incoming_tag=%s incoming_value=%s "
                "tolerance_pct=%.4f tolerance_abs_m=%.4f",
                ticker,
                matched_registry_group.group_id,
                year,
                existing_source_value.tag,
                existing_source_value.normalized_value,
                source_value.tag,
                source_value.normalized_value,
                registry_equivalence_value_tolerance_pct,
                registry_equivalence_value_tolerance_abs_m,
            )
            registry_value_conflict_years.add(year)
            failed_years.add(year)
            entry_failed += 1
            values_dict.pop(year, None)
            provenance_by_year.pop(year, None)
            source_values_by_year.pop(year, None)
            reported_period_ends_by_year.pop(year, None)
            continue
        if year in registry_value_conflict_years:
            continue
        chosen_source_value = choose_preferred_source_value(
            existing_source_value,
            source_value,
        )
        if chosen_source_value is source_value:
            values_dict[year] = scaled_value
            provenance_by_year[year] = provenance
            source_values_by_year[year] = source_value
            if reported_period_end is not None:
                reported_period_ends_by_year[year] = reported_period_end

    return ParsedEdgarSeriesResult(
        values_dict=values_dict,
        failed_years=failed_years,
        entry_failed=entry_failed,
        provenance_by_year=provenance_by_year,
        source_values_by_year=source_values_by_year,
        reported_period_ends_by_year=reported_period_ends_by_year,
    )
