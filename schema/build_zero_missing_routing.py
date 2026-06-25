"""Zero-missing FMP routing helpers for schema build orchestration."""

from __future__ import annotations

import sys

from .build_fmp_values import _build_fmp_lookup, _raw_fmp_value_for_concept
from .build_types import EdgarFetcher
from .model_build_context import HistoricalSources
from .models import DataSourceMapping


def _parent_attr(name: str, fallback):
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    return getattr(parent, name, fallback) if parent is not None else fallback


def _zero_missing_edgar_fallback_routing(
    *,
    source: str,
    taxonomy: dict[str, DataSourceMapping],
    historical_sources: HistoricalSources | None,
    edgar_fetcher: EdgarFetcher | None,
    fmp_data: dict | None,
) -> tuple[HistoricalSources | None, dict[str, DataSourceMapping]]:
    """Route FMP concepts whose zero values mean missing through EDGAR fallback."""

    if historical_sources is not None:
        return historical_sources, taxonomy
    if str(source).lower() != "fmp" or edgar_fetcher is None:
        return historical_sources, taxonomy
    if not fmp_data:
        return historical_sources, taxonomy

    build_fmp_lookup = _parent_attr("_build_fmp_lookup", _build_fmp_lookup)
    raw_fmp_value_for_concept = _parent_attr(
        "_raw_fmp_value_for_concept",
        _raw_fmp_value_for_concept,
    )
    fmp_lookup = build_fmp_lookup(fmp_data)

    overrides: list[dict[str, object]] = []
    for concept_id, concept in sorted(taxonomy.items()):
        if not concept.treat_zero_as_missing:
            continue
        if concept.preferred_source != "fmp":
            continue
        if not concept.fmp_endpoint or not concept.fmp_field:
            continue
        if not (concept.edgar_tags or concept.registry_group_id):
            continue
        records_by_year = fmp_lookup.get(concept.fmp_endpoint, {})
        if not records_by_year:
            continue
        if not any(
            raw_fmp_value_for_concept(concept, record)[0] is None
            for record in records_by_year.values()
        ):
            continue
        overrides.append(
            {
                "concept_id": concept_id,
                "preferred": "fmp",
                "fallback_order": ["fmp", "edgar"],
            }
        )

    if not overrides:
        return historical_sources, taxonomy

    routing_taxonomy = {
        concept_id: concept.model_copy(update={"preferred_source": None})
        for concept_id, concept in taxonomy.items()
    }
    return (
        HistoricalSources.model_validate(
            {
                "default_source": "fmp",
                "default_fallback_enabled": False,
                "overrides": overrides,
            }
        ),
        routing_taxonomy,
    )


__all__ = ["_zero_missing_edgar_fallback_routing"]
