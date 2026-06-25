"""Routed historical write helpers for schema build orchestration."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any, Dict

from .model_build_context import BuildSource
from .models import DataSourceMapping, FinancialModel, ValueProvenance
from .source_routing import ConceptSourceRoute

if TYPE_CHECKING:
    from .build import (
        EdgarConceptFetchResult,
        FmpConceptFetchResult,
        PopulateStats,
        ServedByBreakdown,
        SourceResolutionEntry,
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


def _write_routed_historicals(
    model: FinancialModel,
    routes: dict[str, ConceptSourceRoute],
    fmp_buffer: dict[str, FmpConceptFetchResult],
    edgar_buffer: dict[str, EdgarConceptFetchResult],
    taxonomy: Dict[str, DataSourceMapping],
    *,
    legacy_treasury_route: ConceptSourceRoute | None = None,
) -> PopulateStats:
    has_existing_imported_historicals = _required_parent_attr(
        "_has_existing_imported_historicals"
    )
    iter_items = _required_parent_attr("_iter_items")
    make_fmp_provenance = _required_parent_attr("_make_fmp_provenance")
    record_fmp_quality_observation = _required_parent_attr("_record_fmp_quality_observation")
    set_constant_override = _required_parent_attr("_set_constant_override")
    set_imported_value = _required_parent_attr("_set_imported_value")
    populate_treasury_stock_row_from_fmp_result = _required_parent_attr(
        "_populate_treasury_stock_row_from_fmp_result"
    )
    seed_cash_beginning_of_period = _required_parent_attr("_seed_cash_beginning_of_period")
    refresh_period_metadata = _required_parent_attr("_refresh_period_metadata")
    collect_routed_reported_period_ends = _required_parent_attr(
        "_collect_routed_reported_period_ends"
    )
    fmp_quality_warnings_from_observations = _required_parent_attr(
        "_fmp_quality_warnings_from_observations"
    )
    source_resolution_entry_type = _required_parent_attr("SourceResolutionEntry")
    served_by_breakdown_type = _required_parent_attr("ServedByBreakdown")
    populate_stats_type = _required_parent_attr("PopulateStats")
    treasury_stock_concept_id = _parent_attr(
        "_TREASURY_STOCK_CONCEPT_ID",
        "treasury_stock",
    )

    historical_periods = [int(period) for period in model.time_structure.historical_periods]
    missing_concepts: set[str] = set()
    edgar_errors: set[str] = set()
    edgar_partial_failures: set[str] = set()
    source_resolution: list[SourceResolutionEntry] = []
    year_source_by_concept: dict[str, dict[int, BuildSource | None]] = {}
    fmp_quality_observations: Dict[tuple[str, str, str], Dict[int, float]] = {}
    items_populated = 0
    items_skipped = 0
    periods_populated = 0
    is_overlay = has_existing_imported_historicals(model, historical_periods)

    for concept_id, result in edgar_buffer.items():
        if result.status == "failed":
            edgar_errors.add(concept_id)
        elif result.status == "ok" and result.periods_failed > 0:
            edgar_partial_failures.add(concept_id)

    for item in iter_items(model):
        concept_id = item.data_concept_id
        if not concept_id:
            continue

        if concept_id not in taxonomy:
            items_skipped += 1
            continue

        route = routes.get(concept_id)
        if route is None:
            items_skipped += 1
            continue

        route_touches_edgar = "edgar" in route.fallback_order
        item_has_actuals = False
        for year in historical_periods:
            year_source: BuildSource | None = None
            value: float | None = None
            edgar_provenance = None
            fmp_provenance = None
            attempted_edgar_failed_for_year = False

            for candidate_source in route.fallback_order:
                if candidate_source == "fmp":
                    result = fmp_buffer.get(concept_id)
                    if result is not None and year in result.values:
                        year_source = "fmp"
                        value = result.values[year]
                        concept = taxonomy.get(concept_id)
                        field_used = result.field_used_by_year.get(year)
                        if concept is not None and field_used:
                            fmp_provenance = make_fmp_provenance(concept, field_used)
                            record_fmp_quality_observation(
                                fmp_quality_observations,
                                concept,
                                field_used,
                                year,
                                value,
                            )
                        break
                    continue

                result = edgar_buffer.get(concept_id)
                if result is not None:
                    if result.status == "failed" or year in result.failed_years:
                        attempted_edgar_failed_for_year = True
                    if year in result.values_dict:
                        year_source = "edgar"
                        value = result.values_dict[year]
                        edgar_provenance = result.provenance_by_year.get(year)
                        break

            if year_source is None or value is None:
                missing_concepts.add(concept_id)
                if (
                    item.historical is not None
                    and not attempted_edgar_failed_for_year
                    and (not is_overlay or not route_touches_edgar)
                ):
                    set_constant_override(item, year, 0, synthetic=True)
                year_source_by_concept.setdefault(concept_id, {}).setdefault(year, None)
                continue

            year_source_by_concept.setdefault(concept_id, {}).setdefault(year, year_source)

            provenance = (
                ValueProvenance.imported_edgar
                if year_source == "edgar"
                else ValueProvenance.imported_fmp
            )

            if item.historical is None:
                set_imported_value(
                    item,
                    year,
                    value,
                    provenance=provenance,
                    edgar_provenance=edgar_provenance,
                    fmp_provenance=fmp_provenance,
                )
            else:
                set_constant_override(
                    item,
                    year,
                    value,
                    edgar_provenance=edgar_provenance,
                    fmp_provenance=fmp_provenance,
                )

            item_has_actuals = True
            periods_populated += 1

        if item_has_actuals:
            items_populated += 1
        else:
            items_skipped += 1

    treasury_years = populate_treasury_stock_row_from_fmp_result(
        model,
        fmp_result=fmp_buffer.get(treasury_stock_concept_id),
        taxonomy=taxonomy,
        historical_periods=historical_periods,
    )
    if treasury_years:
        items_populated += 1
        periods_populated += len(treasury_years)
    if legacy_treasury_route is not None:
        treasury_year_set = set(treasury_years)
        year_source_by_concept[treasury_stock_concept_id] = {
            year: "fmp" if year in treasury_year_set else None
            for year in historical_periods
        }
        if len(treasury_years) < len(historical_periods):
            missing_concepts.add(treasury_stock_concept_id)
        if not treasury_years:
            items_skipped += 1

    served_by_breakdown: dict[str, ServedByBreakdown] = {}
    fallback_engaged_concepts: list[str] = []
    fallback_engaged_cells = 0

    for concept_id, route in routes.items():
        breakdown = served_by_breakdown_type(primary_source=route.primary)
        source_by_year = year_source_by_concept.get(concept_id, {})
        for year in historical_periods:
            year_source = source_by_year.get(year)
            if year_source is None:
                breakdown.years_unserved.append(year)
            elif year_source == route.primary:
                breakdown.years_via_primary.append(year)
            else:
                breakdown.years_via_fallback.append(year)

        if breakdown.years_via_fallback:
            fallback_engaged_concepts.append(concept_id)
            fallback_engaged_cells += len(breakdown.years_via_fallback)

        served_by_breakdown[concept_id] = breakdown
        served_by: BuildSource | None = None
        if breakdown.years_via_primary:
            served_by = route.primary
        elif breakdown.years_via_fallback:
            for candidate_source in route.fallback_order:
                if candidate_source != route.primary:
                    served_by = candidate_source
                    break
        served_year_count = len(breakdown.years_via_primary) + len(breakdown.years_via_fallback)

        source_resolution.append(
            source_resolution_entry_type(
                concept_id=concept_id,
                requested_primary=route.primary,
                requested_fallback_order=list(route.fallback_order),
                layer_decided=route.layer_decided,
                served_by=served_by,
                fallback_used=bool(breakdown.years_via_fallback),
                served_year_count=served_year_count,
            )
        )

        if served_year_count == 0:
            missing_concepts.add(concept_id)

    seed_cash_beginning_of_period(model)
    refresh_period_metadata(
        model,
        collect_routed_reported_period_ends(
            fmp_buffer,
            edgar_buffer,
            year_source_by_concept,
            historical_periods,
        ),
    )

    return populate_stats_type(
        source="routed",
        items_populated=items_populated,
        items_skipped=items_skipped,
        periods_populated=periods_populated,
        missing_concepts=sorted(missing_concepts),
        edgar_api_calls=sum(result.api_calls for result in edgar_buffer.values()),
        edgar_errors=sorted(edgar_errors),
        edgar_partial_failures=sorted(edgar_partial_failures),
        source_resolution=sorted(source_resolution, key=lambda entry: entry.concept_id),
        fallback_engaged_concepts=sorted(fallback_engaged_concepts),
        fallback_engaged_cells=fallback_engaged_cells,
        served_by_breakdown=dict(sorted(served_by_breakdown.items())),
        fmp_quality_warnings=fmp_quality_warnings_from_observations(
            fmp_quality_observations
        ),
        served_source_by_concept_year={
            concept_id: {
                int(year): source
                for year, source in sorted(source_by_year.items())
                if source is not None
            }
            for concept_id, source_by_year in sorted(year_source_by_concept.items())
        },
    )
