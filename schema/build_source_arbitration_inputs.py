"""Build diagnostic source observations for source-arbitration shadow mode."""

from __future__ import annotations

from typing import TYPE_CHECKING, Dict, Optional

from .build_source_arbitration import SourceArbitrationMode
from .build_validation_inputs import (
    _fetch_validation_edgar_concept_buffer,
    _fetch_validation_fmp_concept_buffer,
)
from .models import DataSourceMapping
from .source_arbitration_input import SourceArbitrationDiagnosticInput

if TYPE_CHECKING:
    from .build import EdgarFetcher, PopulateStats


def _source_arbitration_opt_in_concepts(
    taxonomy: Dict[str, DataSourceMapping],
) -> set[str]:
    return {
        concept_id
        for concept_id, mapping in taxonomy.items()
        if mapping.validation_tolerance_pct is not None
        or mapping.source_arbitration_policy is not None
    }


def _make_source_arbitration_input(
    *,
    ticker: str,
    taxonomy: Dict[str, DataSourceMapping],
    historical_periods: list[int],
    fmp_data: Optional[Dict],
    edgar_fetcher: Optional["EdgarFetcher"],
    stats: "PopulateStats",
    mode: SourceArbitrationMode,
) -> SourceArbitrationDiagnosticInput:
    opted_in_concepts = _source_arbitration_opt_in_concepts(taxonomy)
    if not opted_in_concepts:
        return SourceArbitrationDiagnosticInput(
            mode=mode,
            opted_in_concepts=[],
            historical_years=historical_periods,
            served_source_by_concept_year=dict(
                stats.served_source_by_concept_year or {}
            ),
        )

    return SourceArbitrationDiagnosticInput(
        mode=mode,
        fmp_buffer=_fetch_validation_fmp_concept_buffer(
            fmp_data,
            opted_in_concepts,
            taxonomy,
            historical_periods,
        ),
        edgar_buffer=_fetch_validation_edgar_concept_buffer(
            ticker,
            opted_in_concepts,
            taxonomy,
            historical_periods,
            edgar_fetcher,
        ),
        opted_in_concepts=sorted(opted_in_concepts),
        historical_years=historical_periods,
        served_source_by_concept_year=dict(stats.served_source_by_concept_year or {}),
    )


__all__ = [
    "_make_source_arbitration_input",
    "_source_arbitration_opt_in_concepts",
]
