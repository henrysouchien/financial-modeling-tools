"""Historical population helpers for schema build orchestration."""

from __future__ import annotations

import concurrent.futures
import logging
import sys
from typing import TYPE_CHECKING, Any, Dict, Optional

from .build_cash_historicals import (
    _has_existing_imported_historicals as _has_existing_imported_historicals_fallback,
    _seed_cash_beginning_of_period as _seed_cash_beginning_of_period_fallback,
)
from .build_concept_buffers import (
    _all_edgar_concepts_failed_message as _all_edgar_concepts_failed_message_fallback,
    _concept_can_fetch_edgar as _concept_can_fetch_edgar_fallback,
    _fetch_edgar_concept_buffer as _fetch_edgar_concept_buffer_fallback,
    _fetch_fmp_concept_buffer as _fetch_fmp_concept_buffer_fallback,
)
from .build_fmp_quality import (
    _fmp_quality_warnings_from_observations as _fmp_quality_warnings_from_observations_fallback,
    _record_fmp_quality_observation as _record_fmp_quality_observation_fallback,
)
from .build_fmp_values import (
    _build_fmp_lookup as _build_fmp_lookup_fallback,
    _make_fmp_provenance as _make_fmp_provenance_fallback,
    _raw_fmp_value_for_concept as _raw_fmp_value_for_concept_fallback,
    _scale_fmp_value as _scale_fmp_value_fallback,
)
from .build_model_items import _iter_items as _iter_items_fallback
from .build_reported_periods import (
    _collect_edgar_reported_period_ends as _collect_edgar_reported_period_ends_fallback,
    _collect_fmp_reported_period_ends_from_lookup as _collect_fmp_reported_period_ends_from_lookup_fallback,
    _refresh_period_metadata as _refresh_period_metadata_fallback,
)
from .build_routed_historicals import (
    _write_routed_historicals as _write_routed_historicals_fallback,
)
from .build_value_writers import (
    _set_constant_override as _set_constant_override_fallback,
    _set_imported_value as _set_imported_value_fallback,
)
from .model_build_context import BuildSource, HistoricalSources
from .models import DataSourceMapping, FinancialModel, ItemType, ValueProvenance
from .source_routing import (
    ConceptSourceRoute,
    resolve_source_for_concept as resolve_source_for_concept_fallback,
    validate_route_eligibility as validate_route_eligibility_fallback,
)

if TYPE_CHECKING:
    from .build import (
        EdgarConceptFetchResult,
        EdgarFetcher,
        FmpConceptFetchResult,
        PopulateStats,
    )


_TREASURY_STOCK_CONCEPT_ID = "treasury_stock"
_TREASURY_STOCK_ITEM_ID = "tpl.a.dividends_shares.treasury_stock"


def _parent_attr(name: str, default: Any) -> Any:
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    if parent is None:
        return default
    return getattr(parent, name, default)


def _required_parent_attr(name: str) -> Any:
    helper = _parent_attr(name, None)
    if helper is None:
        raise RuntimeError(f"schema.build helper '{name}' is unavailable")
    return helper


def populate_from_fmp(
    model: FinancialModel,
    fmp_data: Optional[Dict],
    taxonomy: Dict[str, DataSourceMapping],
) -> PopulateStats:
    """Populate historical values from pre-fetched FMP statements."""

    build_fmp_lookup = _parent_attr("_build_fmp_lookup", _build_fmp_lookup_fallback)
    collect_fmp_reported_period_ends_from_lookup = _parent_attr(
        "_collect_fmp_reported_period_ends_from_lookup",
        _collect_fmp_reported_period_ends_from_lookup_fallback,
    )
    fmp_quality_warnings_from_observations = _parent_attr(
        "_fmp_quality_warnings_from_observations",
        _fmp_quality_warnings_from_observations_fallback,
    )
    iter_items = _parent_attr("_iter_items", _iter_items_fallback)
    make_fmp_provenance = _parent_attr("_make_fmp_provenance", _make_fmp_provenance_fallback)
    populate_stats_type = _required_parent_attr("PopulateStats")
    populate_treasury_stock_row_from_fmp = _parent_attr(
        "_populate_treasury_stock_row_from_fmp",
        _populate_treasury_stock_row_from_fmp,
    )
    raw_fmp_value_for_concept = _parent_attr(
        "_raw_fmp_value_for_concept",
        _raw_fmp_value_for_concept_fallback,
    )
    record_fmp_quality_observation = _parent_attr(
        "_record_fmp_quality_observation",
        _record_fmp_quality_observation_fallback,
    )
    refresh_period_metadata = _parent_attr(
        "_refresh_period_metadata",
        _refresh_period_metadata_fallback,
    )
    scale_fmp_value = _parent_attr("_scale_fmp_value", _scale_fmp_value_fallback)
    seed_cash_beginning_of_period = _parent_attr(
        "_seed_cash_beginning_of_period",
        _seed_cash_beginning_of_period_fallback,
    )
    set_constant_override = _parent_attr(
        "_set_constant_override",
        _set_constant_override_fallback,
    )
    set_imported_value = _parent_attr("_set_imported_value", _set_imported_value_fallback)
    treasury_stock_concept_id = _parent_attr(
        "_TREASURY_STOCK_CONCEPT_ID",
        _TREASURY_STOCK_CONCEPT_ID,
    )
    treasury_stock_item_id = _parent_attr(
        "_TREASURY_STOCK_ITEM_ID",
        _TREASURY_STOCK_ITEM_ID,
    )

    historical_periods = [int(period) for period in model.time_structure.historical_periods]
    fmp_lookup = build_fmp_lookup(fmp_data or {})
    missing_concepts: set[str] = set()
    items_populated = 0
    items_skipped = 0
    periods_populated = 0
    served_source_by_concept_year: dict[str, dict[int, BuildSource]] = {}
    fmp_quality_observations: Dict[tuple[str, str, str], Dict[int, float]] = {}

    for item in iter_items(model):
        concept_id = item.data_concept_id
        if not concept_id:
            continue

        concept = taxonomy.get(concept_id)
        if concept is None:
            items_skipped += 1
            continue

        item_has_actuals = False

        for year in historical_periods:
            record = fmp_lookup.get(concept.fmp_endpoint, {}).get(year)
            raw_value, field_used, _fallback_used = raw_fmp_value_for_concept(
                concept,
                record,
            )

            if raw_value is None:
                missing_concepts.add(concept_id)
                if item.historical is not None:
                    set_constant_override(item, year, 0, synthetic=True)
                continue

            value = scale_fmp_value(concept_id, raw_value, concept=concept)
            fmp_provenance = make_fmp_provenance(concept, field_used)
            record_fmp_quality_observation(
                fmp_quality_observations,
                concept,
                field_used,
                year,
                value,
            )
            if item.historical is None:
                set_imported_value(item, year, value, fmp_provenance=fmp_provenance)
            else:
                set_constant_override(item, year, value, fmp_provenance=fmp_provenance)

            item_has_actuals = True
            periods_populated += 1
            served_source_by_concept_year.setdefault(concept_id, {})[year] = "fmp"

        if item_has_actuals:
            items_populated += 1
        else:
            items_skipped += 1

    try:
        treasury_item = model.get_item(treasury_stock_item_id)
    except KeyError:
        treasury_item = None
    treasury_applicable = (
        bool(fmp_data)
        and taxonomy.get(treasury_stock_concept_id) is not None
        and treasury_item is not None
        and treasury_item.data_concept_id is None
    )
    treasury_years = populate_treasury_stock_row_from_fmp(
        model,
        fmp_data=fmp_data,
        taxonomy=taxonomy,
        historical_periods=historical_periods,
    )
    if treasury_years:
        items_populated += 1
        periods_populated += len(treasury_years)
        served_source_by_concept_year[treasury_stock_concept_id] = {
            year: "fmp" for year in treasury_years
        }
    if treasury_applicable and len(treasury_years) < len(historical_periods):
        missing_concepts.add(treasury_stock_concept_id)
        if not treasury_years:
            items_skipped += 1

    seed_cash_beginning_of_period(model)
    refresh_period_metadata(
        model,
        collect_fmp_reported_period_ends_from_lookup(fmp_lookup, historical_periods),
    )

    return populate_stats_type(
        source="fmp",
        items_populated=items_populated,
        items_skipped=items_skipped,
        periods_populated=periods_populated,
        missing_concepts=sorted(missing_concepts),
        fmp_quality_warnings=fmp_quality_warnings_from_observations(
            fmp_quality_observations
        ),
        served_source_by_concept_year=served_source_by_concept_year,
    )


def populate_from_edgar(
    model: FinancialModel,
    ticker: str,
    taxonomy: Dict[str, DataSourceMapping],
    edgar_fetcher: EdgarFetcher,
) -> PopulateStats:
    """Populate historical values from EDGAR metric series data."""

    all_edgar_concepts_failed_message = _parent_attr(
        "_all_edgar_concepts_failed_message",
        _all_edgar_concepts_failed_message_fallback,
    )
    collect_edgar_reported_period_ends = _parent_attr(
        "_collect_edgar_reported_period_ends",
        _collect_edgar_reported_period_ends_fallback,
    )
    edgar_concept_fetch_result = _required_parent_attr("EdgarConceptFetchResult")
    fetch_edgar_concept_result = _required_parent_attr("_fetch_edgar_concept_result")
    has_existing_imported_historicals = _parent_attr(
        "_has_existing_imported_historicals",
        _has_existing_imported_historicals_fallback,
    )
    iter_items = _parent_attr("_iter_items", _iter_items_fallback)
    populate_stats_type = _required_parent_attr("PopulateStats")
    refresh_period_metadata = _parent_attr(
        "_refresh_period_metadata",
        _refresh_period_metadata_fallback,
    )
    seed_cash_beginning_of_period = _parent_attr(
        "_seed_cash_beginning_of_period",
        _seed_cash_beginning_of_period_fallback,
    )
    set_constant_override = _parent_attr(
        "_set_constant_override",
        _set_constant_override_fallback,
    )
    set_imported_value = _parent_attr("_set_imported_value", _set_imported_value_fallback)

    historical_periods = [int(period) for period in model.time_structure.historical_periods]
    if not historical_periods:
        return populate_stats_type(
            source="edgar",
            items_populated=0,
            items_skipped=0,
            periods_populated=0,
            missing_concepts=[],
        )

    most_recent_fy = max(historical_periods)
    n_historical = len(historical_periods)
    is_overlay = has_existing_imported_historicals(model, historical_periods)

    missing_concepts: set[str] = set()
    edgar_errors: set[str] = set()
    edgar_partial_failures: set[str] = set()
    items_populated = 0
    items_skipped = 0
    periods_populated = 0
    edgar_api_calls = 0
    served_source_by_concept_year: dict[str, dict[int, BuildSource]] = {}
    fetched_concepts = 0
    failed_fetches = 0
    concept_cache: Dict[str, EdgarConceptFetchResult] = {}
    exception_failures: set[str] = set()

    concepts_to_fetch: Dict[str, DataSourceMapping] = {}
    for item in iter_items(model):
        concept_id = item.data_concept_id
        if not concept_id or concept_id in concepts_to_fetch:
            continue

        concept = taxonomy.get(concept_id)
        if concept is None:
            continue

        concepts_to_fetch[concept_id] = concept
        if concept.preferred_source == "fmp" and (concept.edgar_tags or concept.registry_group_id):
            logging.warning("Concept '%s' prefers FMP but source='edgar' was requested", concept_id)

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(
                fetch_edgar_concept_result,
                ticker=ticker,
                concept_id=concept_id,
                concept=concept,
                most_recent_fy=most_recent_fy,
                n_historical=n_historical,
                edgar_fetcher=edgar_fetcher,
            ): concept_id
            for concept_id, concept in concepts_to_fetch.items()
        }
        for future in concurrent.futures.as_completed(futures):
            concept_id = futures[future]
            try:
                concept_cache[concept_id] = future.result()
            except Exception as exc:
                logging.warning("EDGAR fetch failed for concept '%s': %s", concept_id, exc)
                concept_cache[concept_id] = edgar_concept_fetch_result(
                    values_dict={},
                    failed_years=set(),
                    status="missing",
                    periods_failed=0,
                    api_calls=0,
                )
                exception_failures.add(concept_id)

    for concept_id, concept in concepts_to_fetch.items():
        fetch_result = concept_cache.get(
            concept_id,
            edgar_concept_fetch_result(
                values_dict={},
                failed_years=set(),
                status="missing",
                periods_failed=0,
                api_calls=0,
            ),
        )
        if concept.edgar_tags or concept.registry_group_id:
            edgar_api_calls += fetch_result.api_calls
            fetched_concepts += 1
            if fetch_result.status == "failed" or concept_id in exception_failures:
                failed_fetches += 1

        if fetch_result.status == "failed":
            edgar_errors.add(concept_id)
        elif fetch_result.status == "missing":
            missing_concepts.add(concept_id)
        elif fetch_result.status == "ok":
            if fetch_result.periods_failed > 0:
                edgar_partial_failures.add(concept_id)
            if len(fetch_result.values_dict) < n_historical:
                missing_concepts.add(concept_id)

    for item in iter_items(model):
        concept_id = item.data_concept_id
        if not concept_id:
            continue

        concept = taxonomy.get(concept_id)
        if concept is None:
            items_skipped += 1
            continue

        fetch_result = concept_cache.get(
            concept_id,
            edgar_concept_fetch_result(
                values_dict={},
                failed_years=set(),
                status="missing",
                periods_failed=0,
                api_calls=0,
            ),
        )
        item_has_actuals = False

        for year in historical_periods:
            if year in fetch_result.values_dict:
                value = fetch_result.values_dict[year]
                edgar_provenance = fetch_result.provenance_by_year.get(year)
                if item.historical is None:
                    set_imported_value(
                        item,
                        year,
                        value,
                        provenance=ValueProvenance.imported_edgar,
                        edgar_provenance=edgar_provenance,
                    )
                else:
                    set_constant_override(item, year, value, edgar_provenance=edgar_provenance)

                item_has_actuals = True
                periods_populated += 1
                served_source_by_concept_year.setdefault(concept_id, {})[year] = "edgar"
                continue

            if (
                item.historical is not None
                and not is_overlay
                and fetch_result.status != "failed"
                and year not in fetch_result.failed_years
            ):
                set_constant_override(item, year, 0, synthetic=True)

        if item_has_actuals:
            items_populated += 1
        else:
            items_skipped += 1

    if fetched_concepts > 0 and failed_fetches == fetched_concepts:
        raise RuntimeError(all_edgar_concepts_failed_message(concept_cache.values()))

    seed_cash_beginning_of_period(model)
    refresh_period_metadata(
        model,
        collect_edgar_reported_period_ends(concept_cache.values(), historical_periods),
    )

    return populate_stats_type(
        source="edgar",
        items_populated=items_populated,
        items_skipped=items_skipped,
        periods_populated=periods_populated,
        missing_concepts=sorted(missing_concepts),
        edgar_api_calls=edgar_api_calls,
        edgar_errors=sorted(edgar_errors),
        edgar_partial_failures=sorted(edgar_partial_failures),
        served_source_by_concept_year=served_source_by_concept_year,
    )


def populate_historicals(
    model: FinancialModel,
    source: str,
    ticker: str,
    taxonomy: Dict[str, DataSourceMapping],
    most_recent_fy: int,
    n_historical: int = 5,
    fmp_data: Optional[Dict] = None,
    edgar_fetcher: Optional[EdgarFetcher] = None,
    historical_sources: HistoricalSources | None = None,
) -> PopulateStats:
    """Populate template historicals from legacy single-source or routed sources."""

    populate_from_fmp_fn = _parent_attr("populate_from_fmp", populate_from_fmp)
    populate_from_edgar_fn = _parent_attr("populate_from_edgar", populate_from_edgar)
    populate_routed = _parent_attr("_populate_routed", _populate_routed)

    if historical_sources is not None:
        if str(source).lower() != "fmp":
            logging.debug(
                "populate_historicals: both source=%s and historical_sources passed; using routed",
                source,
            )
        return populate_routed(
            model,
            historical_sources,
            ticker=ticker,
            taxonomy=taxonomy,
            most_recent_fy=most_recent_fy,
            n_historical=n_historical,
            fmp_data=fmp_data,
            edgar_fetcher=edgar_fetcher,
        )

    del most_recent_fy, n_historical
    source = str(source).lower()

    if source == "fmp":
        if fmp_data is None:
            raise ValueError("fmp_data is required when source='fmp'")
        return populate_from_fmp_fn(model, fmp_data, taxonomy)
    if source == "edgar":
        if edgar_fetcher is None:
            raise ValueError("edgar_fetcher is required when source='edgar'")
        return populate_from_edgar_fn(model, ticker, taxonomy, edgar_fetcher)
    if source == "both":
        raise ValueError("'both' mode not supported — use 'fmp' or 'edgar'")
    raise ValueError(f"Unsupported source: {source}")


def _populate_routed(
    model: FinancialModel,
    historical_sources: HistoricalSources,
    *,
    ticker: str,
    taxonomy: Dict[str, DataSourceMapping],
    most_recent_fy: int,
    n_historical: int,
    fmp_data: Optional[Dict],
    edgar_fetcher: Optional[EdgarFetcher],
) -> PopulateStats:
    all_edgar_concepts_failed_message = _parent_attr(
        "_all_edgar_concepts_failed_message",
        _all_edgar_concepts_failed_message_fallback,
    )
    concept_can_fetch_edgar = _parent_attr(
        "_concept_can_fetch_edgar",
        _concept_can_fetch_edgar_fallback,
    )
    fetch_edgar_concept_buffer = _parent_attr(
        "_fetch_edgar_concept_buffer",
        _fetch_edgar_concept_buffer_fallback,
    )
    fetch_fmp_concept_buffer = _parent_attr(
        "_fetch_fmp_concept_buffer",
        _fetch_fmp_concept_buffer_fallback,
    )
    iter_items = _parent_attr("_iter_items", _iter_items_fallback)
    resolve_source = _parent_attr(
        "resolve_source_for_concept",
        resolve_source_for_concept_fallback,
    )
    treasury_stock_concept_id = _parent_attr(
        "_TREASURY_STOCK_CONCEPT_ID",
        _TREASURY_STOCK_CONCEPT_ID,
    )
    treasury_stock_item_id = _parent_attr(
        "_TREASURY_STOCK_ITEM_ID",
        _TREASURY_STOCK_ITEM_ID,
    )
    validate_route = _parent_attr(
        "validate_route_eligibility",
        validate_route_eligibility_fallback,
    )
    write_routed_historicals = _parent_attr(
        "_write_routed_historicals",
        _write_routed_historicals_fallback,
    )

    del most_recent_fy, n_historical
    routes: dict[str, ConceptSourceRoute] = {}
    explicit_overrides = {override.concept_id for override in historical_sources.overrides}
    legacy_treasury_route: ConceptSourceRoute | None = None

    for item in iter_items(model):
        concept_id = item.data_concept_id
        if not concept_id or concept_id in routes:
            continue
        taxonomy_concept = taxonomy.get(concept_id)
        route = resolve_source(concept_id, historical_sources, taxonomy_concept)
        validate_route(
            route,
            taxonomy_concept,
            is_explicit_override=concept_id in explicit_overrides,
        )
        routes[concept_id] = route

    treasury_concept = taxonomy.get(treasury_stock_concept_id)
    if treasury_concept is not None and fmp_data is not None:
        try:
            treasury_item = model.get_item(treasury_stock_item_id)
        except KeyError:
            treasury_item = None
        if treasury_item is not None and treasury_item.data_concept_id is None:
            resolved_treasury_route = resolve_source(
                treasury_stock_concept_id,
                historical_sources,
                treasury_concept,
            )
            if resolved_treasury_route.primary == "fmp":
                legacy_treasury_route = resolved_treasury_route.model_copy(
                    update={"fallback_order": ["fmp"]}
                )
                validate_route(
                    legacy_treasury_route,
                    treasury_concept,
                    is_explicit_override=treasury_stock_concept_id in explicit_overrides,
                )
                routes[treasury_stock_concept_id] = legacy_treasury_route

    required_sources = {
        source
        for route in routes.values()
        for source in route.fallback_order
    }

    fmp_buffer: dict[str, FmpConceptFetchResult] = {}
    edgar_buffer: dict[str, EdgarConceptFetchResult] = {}

    if "fmp" in required_sources:
        if fmp_data is None:
            raise ValueError("fmp_data is required when routed historical_sources can use FMP")
        fmp_concepts = {
            concept_id
            for concept_id, route in routes.items()
            if "fmp" in route.fallback_order
        }
        fmp_buffer = fetch_fmp_concept_buffer(
            fmp_data,
            fmp_concepts,
            taxonomy,
            [int(period) for period in model.time_structure.historical_periods],
        )

    if "edgar" in required_sources:
        if edgar_fetcher is None:
            raise ValueError("edgar_fetcher is required when routed historical_sources can use EDGAR")
        edgar_concepts = {
            concept_id
            for concept_id, route in routes.items()
            if "edgar" in route.fallback_order
        }
        edgar_buffer = fetch_edgar_concept_buffer(
            ticker,
            edgar_concepts,
            taxonomy,
            [int(period) for period in model.time_structure.historical_periods],
            edgar_fetcher,
        )
        edgar_fetched_count = len(
            [
                concept_id
                for concept_id in edgar_concepts
                if concept_can_fetch_edgar(taxonomy.get(concept_id))
            ]
        )
        edgar_failed_count = len(
            [
                result
                for result in edgar_buffer.values()
                if result.status == "failed"
            ]
        )
        if edgar_fetched_count > 0 and edgar_failed_count == edgar_fetched_count:
            raise RuntimeError(all_edgar_concepts_failed_message(edgar_buffer.values()))

    return write_routed_historicals(
        model,
        routes,
        fmp_buffer,
        edgar_buffer,
        taxonomy,
        legacy_treasury_route=legacy_treasury_route,
    )


def _extract_first_numeric(
    fmp_data: Optional[Dict],
    endpoints: tuple[str, ...],
    fields: tuple[str, ...],
) -> Optional[float]:
    if not fmp_data:
        return None
    for endpoint in endpoints:
        records = fmp_data.get(endpoint)
        if isinstance(records, dict):
            iterable = [records]
        else:
            iterable = list(records or [])
        for record in iterable:
            if not isinstance(record, dict):
                continue
            for field_name in fields:
                raw_value = record.get(field_name)
                if raw_value is None:
                    continue
                try:
                    return float(raw_value)
                except (TypeError, ValueError):
                    continue
    return None


def _populate_treasury_stock_row_from_fmp(
    model: FinancialModel,
    *,
    fmp_data: Optional[Dict],
    taxonomy: Dict[str, DataSourceMapping],
    historical_periods: list[int],
) -> list[int]:
    """Populate the legacy assumptions Treasury Stock row from FMP when present."""

    fetch_fmp_concept_buffer = _parent_attr(
        "_fetch_fmp_concept_buffer",
        _fetch_fmp_concept_buffer_fallback,
    )
    populate_treasury_stock_row_from_fmp_result = _parent_attr(
        "_populate_treasury_stock_row_from_fmp_result",
        _populate_treasury_stock_row_from_fmp_result,
    )
    treasury_stock_concept_id = _parent_attr(
        "_TREASURY_STOCK_CONCEPT_ID",
        _TREASURY_STOCK_CONCEPT_ID,
    )

    concept = taxonomy.get(treasury_stock_concept_id)
    if (
        concept is None
        or not concept.fmp_endpoint
        or not concept.fmp_field
        or not fmp_data
    ):
        return []

    fmp_buffer = fetch_fmp_concept_buffer(
        fmp_data,
        {treasury_stock_concept_id},
        taxonomy,
        historical_periods,
    )
    return populate_treasury_stock_row_from_fmp_result(
        model,
        fmp_result=fmp_buffer.get(treasury_stock_concept_id),
        taxonomy=taxonomy,
        historical_periods=historical_periods,
    )


def _populate_treasury_stock_row_from_fmp_result(
    model: FinancialModel,
    *,
    fmp_result: FmpConceptFetchResult | None,
    taxonomy: Dict[str, DataSourceMapping],
    historical_periods: list[int],
) -> list[int]:
    item_type = _parent_attr("ItemType", ItemType)
    make_fmp_provenance = _parent_attr("_make_fmp_provenance", _make_fmp_provenance_fallback)
    set_constant_override = _parent_attr(
        "_set_constant_override",
        _set_constant_override_fallback,
    )
    set_imported_value = _parent_attr("_set_imported_value", _set_imported_value_fallback)
    treasury_stock_concept_id = _parent_attr(
        "_TREASURY_STOCK_CONCEPT_ID",
        _TREASURY_STOCK_CONCEPT_ID,
    )
    treasury_stock_item_id = _parent_attr(
        "_TREASURY_STOCK_ITEM_ID",
        _TREASURY_STOCK_ITEM_ID,
    )

    concept = taxonomy.get(treasury_stock_concept_id)
    if concept is None or fmp_result is None or fmp_result.missing:
        return []

    try:
        item = model.get_item(treasury_stock_item_id)
    except KeyError:
        return []

    if item.data_concept_id is not None:
        return []

    populated_years: list[int] = []
    original_concept_id = item.data_concept_id
    original_item_type = item.item_type

    for year in historical_periods:
        value = fmp_result.values.get(year)
        if value is None:
            continue

        if not populated_years:
            item.data_concept_id = treasury_stock_concept_id
            item.item_type = item_type.input
        field_used = fmp_result.field_used_by_year.get(year)
        fmp_provenance = make_fmp_provenance(concept, field_used)
        if item.historical is None:
            set_imported_value(item, year, value, fmp_provenance=fmp_provenance)
        else:
            set_constant_override(item, year, value, fmp_provenance=fmp_provenance)
        populated_years.append(year)

    if not populated_years:
        item.data_concept_id = original_concept_id
        item.item_type = original_item_type
    return populated_years


__all__ = [
    "_TREASURY_STOCK_CONCEPT_ID",
    "_TREASURY_STOCK_ITEM_ID",
    "_extract_first_numeric",
    "_populate_routed",
    "_populate_treasury_stock_row_from_fmp",
    "_populate_treasury_stock_row_from_fmp_result",
    "populate_from_edgar",
    "populate_from_fmp",
    "populate_historicals",
]
