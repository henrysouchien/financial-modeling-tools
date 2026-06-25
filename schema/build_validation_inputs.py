"""Validation input construction helpers for schema build orchestration."""

from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING, Any, Dict, Optional

from .models import DataSourceMapping
from .validation_input import ValidationInput

if TYPE_CHECKING:
    from .build import (
        EdgarConceptFetchResult,
        EdgarFetcher,
        FmpConceptFetchResult,
        PopulateStats,
    )

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


def _fetch_validation_fmp_concept_buffer(
    fmp_data: Optional[Dict],
    concept_ids: set[str],
    taxonomy: Dict[str, DataSourceMapping],
    historical_periods: list[int],
) -> dict[str, FmpConceptFetchResult]:
    fetch_fmp_concept_buffer = _required_parent_attr("_fetch_fmp_concept_buffer")
    empty_fmp_concept_result = _required_parent_attr("_empty_fmp_concept_result")

    buffer: dict[str, FmpConceptFetchResult] = {}
    for concept_id in sorted(concept_ids):
        try:
            buffer.update(
                fetch_fmp_concept_buffer(
                    fmp_data or {},
                    {concept_id},
                    taxonomy,
                    historical_periods,
                )
            )
        except Exception as exc:
            logging.warning(
                "Cross-source validation FMP extraction failed for concept '%s': %s",
                concept_id,
                exc,
            )
            buffer[concept_id] = empty_fmp_concept_result(concept_id)
    return buffer


def _fetch_validation_edgar_concept_buffer(
    ticker: str,
    concept_ids: set[str],
    taxonomy: Dict[str, DataSourceMapping],
    historical_periods: list[int],
    edgar_fetcher: Optional[EdgarFetcher],
) -> dict[str, EdgarConceptFetchResult]:
    fetch_edgar_concept_buffer = _required_parent_attr("_fetch_edgar_concept_buffer")
    empty_edgar_concept_result = _required_parent_attr("_empty_edgar_concept_result")

    buffer: dict[str, EdgarConceptFetchResult] = {}
    for concept_id in sorted(concept_ids):
        if edgar_fetcher is None:
            logging.warning(
                "Cross-source validation EDGAR fetch skipped for concept '%s': no fetcher",
                concept_id,
            )
            buffer[concept_id] = empty_edgar_concept_result()
            continue
        try:
            buffer.update(
                fetch_edgar_concept_buffer(
                    ticker,
                    {concept_id},
                    taxonomy,
                    historical_periods,
                    edgar_fetcher,
                )
            )
        except Exception as exc:
            logging.warning(
                "Cross-source validation EDGAR fetch failed for concept '%s': %s",
                concept_id,
                exc,
            )
            buffer[concept_id] = empty_edgar_concept_result(
                status="failed",
                historical_periods=historical_periods,
            )
    return buffer


def _validation_opt_in_concepts(taxonomy: Dict[str, DataSourceMapping]) -> set[str]:
    return {
        concept_id
        for concept_id, mapping in taxonomy.items()
        if mapping.validation_tolerance_pct is not None
    }


def _make_validation_input(
    *,
    ticker: str,
    taxonomy: Dict[str, DataSourceMapping],
    historical_periods: list[int],
    fmp_data: Optional[Dict],
    edgar_fetcher: Optional[EdgarFetcher],
    stats: PopulateStats,
) -> ValidationInput:
    validation_opt_in_concepts = _parent_attr(
        "_validation_opt_in_concepts",
        _validation_opt_in_concepts,
    )
    fetch_validation_fmp_concept_buffer = _parent_attr(
        "_fetch_validation_fmp_concept_buffer",
        _fetch_validation_fmp_concept_buffer,
    )
    fetch_validation_edgar_concept_buffer = _parent_attr(
        "_fetch_validation_edgar_concept_buffer",
        _fetch_validation_edgar_concept_buffer,
    )

    opted_in_concepts = validation_opt_in_concepts(taxonomy)
    if not opted_in_concepts:
        return ValidationInput(
            opted_in_concepts=[],
            historical_years=historical_periods,
            served_source_by_concept_year=dict(stats.served_source_by_concept_year or {}),
        )

    fmp_buffer = fetch_validation_fmp_concept_buffer(
        fmp_data,
        opted_in_concepts,
        taxonomy,
        historical_periods,
    )
    edgar_buffer = fetch_validation_edgar_concept_buffer(
        ticker,
        opted_in_concepts,
        taxonomy,
        historical_periods,
        edgar_fetcher,
    )

    return ValidationInput(
        fmp_buffer=fmp_buffer,
        edgar_buffer=edgar_buffer,
        opted_in_concepts=sorted(opted_in_concepts),
        historical_years=historical_periods,
        served_source_by_concept_year=dict(stats.served_source_by_concept_year or {}),
    )
