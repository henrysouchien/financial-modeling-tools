"""FMP quality observation helpers for schema build orchestration."""

from __future__ import annotations

import sys

from .models import DataSourceMapping


_FMP_QUALITY_BUCKET_FIELDS = frozenset(
    {
        ("balance_sheet", "otherCurrentAssets"),
        ("balance_sheet", "otherNonCurrentAssets"),
        ("balance_sheet", "otherCurrentLiabilities"),
        ("balance_sheet", "otherNonCurrentLiabilities"),
        ("cash_flow", "otherWorkingCapital"),
        ("cash_flow", "otherNonCashItems"),
        ("cash_flow", "otherInvestingActivities"),
        ("cash_flow", "otherFinancingActivities"),
    }
)
_FMP_QUALITY_INFORMATIONAL_BUCKET_CONCEPTS = frozenset(
    {
        "change_in_other_working_capital",
        "cf_other_non_cash_items",
    }
)
_FMP_QUALITY_YOY_RATIO_THRESHOLD = 3.0
_FMP_QUALITY_ABS_DELTA_M = 100.0


def _parent_attr(name: str, fallback):
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    return getattr(parent, name, fallback) if parent is not None else fallback


def _record_fmp_quality_observation(
    observations: dict[tuple[str, str, str], dict[int, float]],
    concept: DataSourceMapping,
    field_used: str | None,
    year: int,
    value: float,
) -> None:
    endpoint = concept.fmp_endpoint
    if not endpoint or not field_used:
        return
    bucket_fields = _parent_attr("_FMP_QUALITY_BUCKET_FIELDS", _FMP_QUALITY_BUCKET_FIELDS)
    if (endpoint, field_used) not in bucket_fields:
        return
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return
    observations.setdefault(
        (str(concept.concept_id), str(endpoint), str(field_used)),
        {},
    )[int(year)] = numeric_value


def _fmp_quality_warnings_from_observations(
    observations: dict[tuple[str, str, str], dict[int, float]],
) -> list[dict[str, object]]:
    warnings: list[dict[str, object]] = []
    abs_delta_m = _parent_attr("_FMP_QUALITY_ABS_DELTA_M", _FMP_QUALITY_ABS_DELTA_M)
    ratio_threshold = _parent_attr(
        "_FMP_QUALITY_YOY_RATIO_THRESHOLD",
        _FMP_QUALITY_YOY_RATIO_THRESHOLD,
    )
    informational_bucket_concepts = _parent_attr(
        "_FMP_QUALITY_INFORMATIONAL_BUCKET_CONCEPTS",
        _FMP_QUALITY_INFORMATIONAL_BUCKET_CONCEPTS,
    )
    for (concept_id, endpoint, fmp_field), values_by_year in sorted(observations.items()):
        years = sorted(values_by_year)
        for prior_year, year in zip(years, years[1:]):
            if int(year) != int(prior_year) + 1:
                continue
            prior_value = values_by_year[prior_year]
            value = values_by_year[year]
            delta = value - prior_value
            if abs(delta) < abs_delta_m:
                continue

            prior_abs = abs(prior_value)
            ratio = None if prior_abs < 1e-9 else abs(value) / prior_abs
            if ratio is None:
                extreme_change = abs(value) >= abs_delta_m
            else:
                extreme_change = (
                    ratio >= ratio_threshold
                    or ratio <= 1.0 / ratio_threshold
                )
            if not extreme_change:
                continue

            severity = "warning"
            classification = None
            if (
                endpoint == "cash_flow"
                and concept_id in informational_bucket_concepts
            ):
                severity = "info"
                classification = "broad_cash_flow_bucket_reclassification"

            warning: dict[str, object] = {
                "kind": "fmp_bucket_yoy_jump",
                "severity": severity,
                "concept_id": concept_id,
                "endpoint": endpoint,
                "field": fmp_field,
                "prior_year": int(prior_year),
                "year": int(year),
                "prior_value": prior_value,
                "value": value,
                "delta": delta,
                "ratio": ratio,
                "ratio_threshold": ratio_threshold,
                "abs_delta_threshold": abs_delta_m,
            }
            if classification is not None:
                warning["classification"] = classification
            warnings.append(warning)
    return warnings


__all__ = [
    "_FMP_QUALITY_ABS_DELTA_M",
    "_FMP_QUALITY_BUCKET_FIELDS",
    "_FMP_QUALITY_INFORMATIONAL_BUCKET_CONCEPTS",
    "_FMP_QUALITY_YOY_RATIO_THRESHOLD",
    "_fmp_quality_warnings_from_observations",
    "_record_fmp_quality_observation",
]
