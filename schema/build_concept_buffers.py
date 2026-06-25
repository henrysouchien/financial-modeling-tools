"""Concept-buffer fetch helpers for schema build orchestration."""

from __future__ import annotations

import concurrent.futures
import sys
from typing import TYPE_CHECKING, Any, Dict, Iterable

from .models import DataSourceMapping

if TYPE_CHECKING:
    from .build import EdgarConceptFetchResult, EdgarFetcher, FmpConceptFetchResult


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


def _concept_can_fetch_edgar(concept: DataSourceMapping | None) -> bool:
    return bool(concept is not None and (concept.edgar_tags or concept.registry_group_id))


def _all_edgar_concepts_failed_message(results: Iterable[EdgarConceptFetchResult]) -> str:
    message = "EDGAR API returned errors for all concepts — check auth/connectivity"
    details = sorted(
        {
            str(result.error_message).strip()
            for result in results
            if result.status == "failed" and result.error_message
        }
    )
    if details:
        return f"{message}: {'; '.join(details[:3])}"
    return message


def _empty_fmp_concept_result(concept_id: str) -> FmpConceptFetchResult:
    fmp_concept_fetch_result = _required_parent_attr("FmpConceptFetchResult")
    return fmp_concept_fetch_result(
        concept_id=concept_id,
        values={},
        field_used_by_year={},
        fallback_field_years=set(),
        missing=True,
    )


def _empty_edgar_concept_result(
    *,
    status: str = "missing",
    historical_periods: list[int] | None = None,
) -> EdgarConceptFetchResult:
    edgar_concept_fetch_result = _required_parent_attr("EdgarConceptFetchResult")
    return edgar_concept_fetch_result(
        values_dict={},
        failed_years=set(historical_periods or []) if status == "failed" else set(),
        status=status,
        periods_failed=len(historical_periods or []) if status == "failed" else 0,
        api_calls=0,
    )


def _fetch_fmp_concept_buffer(
    fmp_data: Dict,
    concept_ids: set[str],
    taxonomy: Dict[str, DataSourceMapping],
    historical_periods: list[int],
) -> dict[str, FmpConceptFetchResult]:
    build_fmp_lookup = _required_parent_attr("_build_fmp_lookup")
    fmp_concept_fetch_result = _required_parent_attr("FmpConceptFetchResult")
    raw_fmp_value_for_concept = _required_parent_attr("_raw_fmp_value_for_concept")
    reported_period_end_value = _required_parent_attr("_reported_period_end_value")
    scale_fmp_value = _required_parent_attr("_scale_fmp_value")

    fmp_lookup = build_fmp_lookup(fmp_data or {})
    buffer: dict[str, FmpConceptFetchResult] = {}

    for concept_id in concept_ids:
        concept = taxonomy.get(concept_id)
        values: dict[int, float] = {}
        field_used_by_year: dict[int, str] = {}
        fallback_field_years: set[int] = set()
        reported_period_ends_by_year: dict[int, str] = {}

        if concept is None or not concept.fmp_endpoint or not concept.fmp_field:
            buffer[concept_id] = fmp_concept_fetch_result(concept_id, {}, {}, set(), True)
            continue

        for year in historical_periods:
            record = fmp_lookup.get(concept.fmp_endpoint, {}).get(year)
            raw_value, field_used, fallback_used = raw_fmp_value_for_concept(
                concept,
                record,
            )
            if fallback_used:
                fallback_field_years.add(year)

            if raw_value is None:
                continue

            values[year] = scale_fmp_value(concept_id, raw_value, concept=concept)
            field_used_by_year[year] = field_used
            reported_period_end = reported_period_end_value(record.get("date") if record else None)
            if reported_period_end is not None:
                reported_period_ends_by_year[year] = reported_period_end

        buffer[concept_id] = fmp_concept_fetch_result(
            concept_id=concept_id,
            values=values,
            field_used_by_year=field_used_by_year,
            fallback_field_years=fallback_field_years,
            missing=not values,
            reported_period_ends_by_year=reported_period_ends_by_year,
        )

    return buffer


def _fetch_edgar_concept_buffer(
    ticker: str,
    concept_ids: set[str],
    taxonomy: Dict[str, DataSourceMapping],
    historical_periods: list[int],
    edgar_fetcher: EdgarFetcher,
) -> dict[str, EdgarConceptFetchResult]:
    if not historical_periods:
        return {}

    concept_can_fetch_edgar = _parent_attr("_concept_can_fetch_edgar", _concept_can_fetch_edgar)
    edgar_concept_fetch_result = _required_parent_attr("EdgarConceptFetchResult")
    fetch_edgar_concept_result = _required_parent_attr("_fetch_edgar_concept_result")

    most_recent_fy = max(historical_periods)
    n_historical = len(historical_periods)
    buffer: dict[str, EdgarConceptFetchResult] = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {}
        for concept_id in concept_ids:
            concept = taxonomy.get(concept_id)
            if not concept_can_fetch_edgar(concept):
                buffer[concept_id] = edgar_concept_fetch_result(
                    values_dict={},
                    failed_years=set(),
                    status="missing",
                    periods_failed=0,
                    api_calls=0,
                )
                continue
            futures[
                executor.submit(
                    fetch_edgar_concept_result,
                    ticker=ticker,
                    concept_id=concept_id,
                    concept=concept,
                    most_recent_fy=most_recent_fy,
                    n_historical=n_historical,
                    edgar_fetcher=edgar_fetcher,
                )
            ] = concept_id

        for future in concurrent.futures.as_completed(futures):
            buffer[futures[future]] = future.result()

    return buffer
