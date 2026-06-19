"""Resolve per-concept historical data source routing."""

from __future__ import annotations

import logging
from typing import Literal, cast, get_args

from pydantic import BaseModel

from .model_build_context import BuildSource, HistoricalSources
from .models import DataSourceMapping


class ConceptSourceRoute(BaseModel):
    concept_id: str
    primary: BuildSource
    fallback_order: list[BuildSource]
    layer_decided: Literal["taxonomy", "mbc_default", "mbc_override"]


class UnsupportedSourceForConcept(ValueError):
    """Raised when an explicit override targets a source that cannot serve a concept."""


def resolve_source_for_concept(
    concept_id: str,
    historical_sources: HistoricalSources,
    taxonomy_concept: DataSourceMapping | None,
) -> ConceptSourceRoute:
    for override in historical_sources.overrides:
        if override.concept_id == concept_id:
            return ConceptSourceRoute(
                concept_id=concept_id,
                primary=override.preferred,
                fallback_order=list(override.fallback_order),
                layer_decided="mbc_override",
            )

    fallback_enabled = getattr(historical_sources, "default_fallback_enabled", False)

    if taxonomy_concept is not None and taxonomy_concept.preferred_source:
        raw = taxonomy_concept.preferred_source
        if raw not in get_args(BuildSource):
            raise ValueError(
                f"Taxonomy concept {concept_id!r} has unrecognized preferred_source "
                f"{raw!r} (must be one of {get_args(BuildSource)})"
            )
        primary = cast(BuildSource, raw)
        return ConceptSourceRoute(
            concept_id=concept_id,
            primary=primary,
            fallback_order=_derive_fallback_order(primary, taxonomy_concept, fallback_enabled),
            layer_decided="taxonomy",
        )

    primary = historical_sources.default_source
    return ConceptSourceRoute(
        concept_id=concept_id,
        primary=primary,
        fallback_order=_derive_fallback_order(primary, taxonomy_concept, fallback_enabled),
        layer_decided="mbc_default",
    )


def _derive_fallback_order(
    primary: BuildSource,
    taxonomy_concept: DataSourceMapping | None,
    fallback_enabled: bool,
) -> list[BuildSource]:
    if not fallback_enabled or taxonomy_concept is None:
        return [primary]

    secondary: BuildSource = "fmp" if primary == "edgar" else "edgar"
    if not _source_can_serve(primary, taxonomy_concept):
        return [primary]
    if _source_can_serve(secondary, taxonomy_concept):
        return [primary, secondary]
    return [primary]


def validate_route_eligibility(
    route: ConceptSourceRoute,
    taxonomy_concept: DataSourceMapping | None,
    *,
    is_explicit_override: bool,
) -> None:
    unsupported = [
        source
        for source in route.fallback_order
        if not _source_can_serve(source, taxonomy_concept)
    ]
    if not unsupported:
        return

    message = (
        f"Concept {route.concept_id!r} cannot be served by source(s) "
        f"{unsupported!r} for route {route.fallback_order!r}"
    )
    if is_explicit_override:
        raise UnsupportedSourceForConcept(message)
    logging.warning(message)


def _source_can_serve(source: BuildSource, taxonomy_concept: DataSourceMapping | None) -> bool:
    if taxonomy_concept is None:
        return False
    if source == "fmp":
        return bool(taxonomy_concept.fmp_endpoint and taxonomy_concept.fmp_field)
    if source == "edgar":
        return bool(taxonomy_concept.edgar_tags or taxonomy_concept.registry_group_id)
    return False
