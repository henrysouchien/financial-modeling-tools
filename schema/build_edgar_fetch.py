"""EDGAR fetch helpers for schema build orchestration."""

from __future__ import annotations

import logging
import sys
from typing import TYPE_CHECKING, Any, Dict

from api.credentials import (
    get_equivalence_flag as _get_equivalence_flag_fallback,
    is_analyst_cron_mode as _is_analyst_cron_mode_fallback,
)

from .build_edgar_series import (
    _parse_edgar_series_source_result as _parse_edgar_series_source_result_fallback,
)
from .build_edgar_registry import (
    _SEEN_DEPRECATED_REGISTRY_GROUPS as _SEEN_DEPRECATED_REGISTRY_GROUPS,
    _edgar_negate_enabled as _edgar_negate_enabled,
    _equivalent_tag_validated_by_registry as _equivalent_tag_validated_by_registry,
    _log_deprecated_registry_group_once as _log_deprecated_registry_group_once,
    _metric_tag_matches_group as _metric_tag_matches_group,
    _registry_equivalence_value_conflict as _registry_equivalence_value_conflict,
    _resolve_registry_response_group as _resolve_registry_response_group,
    _tags_equivalent as _tags_equivalent,
    _validated_registry_group_for_metric_tag as _validated_registry_group_for_metric_tag,
)
from .build_edgar_fetch_utils import (
    _call_edgar_metric_fetcher as _call_edgar_metric_fetcher,
    _edgar_fetch_error_message as _edgar_fetch_error_message,
    _edgar_tag_lookup_candidates as _edgar_tag_lookup_candidates,
    _registry_failed_result as _registry_failed_result,
    _requested_years_for_fetch as _requested_years_for_fetch,
)
from .build_formula_refs import _safe_int as _safe_int_fallback
from .models import DataSourceMapping, EdgarProvenance, NonadmissibleReasonCode
from .registry_cache import get_registry_cache as _get_registry_cache_fallback
from .source_values import SourceValue

if TYPE_CHECKING:
    from .build import EdgarConceptFetchResult, EdgarFetcher


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


def _fetch_edgar_concept(
    ticker: str,
    concept_id: str,
    concept: DataSourceMapping,
    most_recent_fy: int,
    n_historical: int,
    edgar_fetcher: EdgarFetcher,
) -> tuple[Dict[int, float], set[int], str, int, int]:
    fetch_edgar_concept_result = _parent_attr(
        "_fetch_edgar_concept_result",
        _fetch_edgar_concept_result,
    )
    return fetch_edgar_concept_result(
        ticker=ticker,
        concept_id=concept_id,
        concept=concept,
        most_recent_fy=most_recent_fy,
        n_historical=n_historical,
        edgar_fetcher=edgar_fetcher,
    ).as_tuple()


def _fetch_edgar_concept_result(
    ticker: str,
    concept_id: str,
    concept: DataSourceMapping,
    most_recent_fy: int,
    n_historical: int,
    edgar_fetcher: EdgarFetcher,
) -> EdgarConceptFetchResult:
    get_equivalence_flag = _parent_attr(
        "get_equivalence_flag",
        _get_equivalence_flag_fallback,
    )
    fetch_via_registry = _parent_attr("_fetch_via_registry", _fetch_via_registry)
    fetch_legacy_edgar_concept = _parent_attr(
        "_fetch_legacy_edgar_concept",
        _fetch_legacy_edgar_concept,
    )
    run_shadow_compare = _parent_attr("_run_shadow_compare", _run_shadow_compare)
    maybe_total_equity_derived_fallback = _required_parent_attr(
        "_maybe_total_equity_derived_fallback"
    )
    is_analyst_cron_mode = _parent_attr(
        "is_analyst_cron_mode",
        _is_analyst_cron_mode_fallback,
    )
    flag = get_equivalence_flag()

    if flag == "true" and concept.registry_group_id:
        result = fetch_via_registry(
            ticker=ticker,
            concept_id=concept_id,
            concept=concept,
            most_recent_fy=most_recent_fy,
            n_historical=n_historical,
            edgar_fetcher=edgar_fetcher,
        )
    elif flag == "shadow":
        if concept.registry_group_id and concept.edgar_tags:
            result = fetch_legacy_edgar_concept(
                ticker=ticker,
                concept_id=concept_id,
                concept=concept,
                most_recent_fy=most_recent_fy,
                n_historical=n_historical,
                edgar_fetcher=edgar_fetcher,
            )
            if not is_analyst_cron_mode():
                run_shadow_compare(
                    ticker=ticker,
                    concept_id=concept_id,
                    concept=concept,
                    legacy_result=result,
                    most_recent_fy=most_recent_fy,
                    n_historical=n_historical,
                    edgar_fetcher=edgar_fetcher,
                )
        elif concept.registry_group_id:
            result = fetch_via_registry(
                ticker=ticker,
                concept_id=concept_id,
                concept=concept,
                most_recent_fy=most_recent_fy,
                n_historical=n_historical,
                edgar_fetcher=edgar_fetcher,
            )
        else:
            result = fetch_legacy_edgar_concept(
                ticker=ticker,
                concept_id=concept_id,
                concept=concept,
                most_recent_fy=most_recent_fy,
                n_historical=n_historical,
                edgar_fetcher=edgar_fetcher,
            )
    else:
        result = fetch_legacy_edgar_concept(
            ticker=ticker,
            concept_id=concept_id,
            concept=concept,
            most_recent_fy=most_recent_fy,
            n_historical=n_historical,
            edgar_fetcher=edgar_fetcher,
        )

    return maybe_total_equity_derived_fallback(
        result=result,
        ticker=ticker,
        concept_id=concept_id,
        concept=concept,
        most_recent_fy=most_recent_fy,
        n_historical=n_historical,
        edgar_fetcher=edgar_fetcher,
    )


def _run_shadow_compare(
    *,
    ticker: str,
    concept_id: str,
    concept: DataSourceMapping,
    legacy_result: EdgarConceptFetchResult,
    most_recent_fy: int,
    n_historical: int,
    edgar_fetcher: EdgarFetcher,
) -> None:
    from .equivalence_shadow import log_shadow_diffs

    fetch_via_registry = _parent_attr("_fetch_via_registry", _fetch_via_registry)
    upstream_result = fetch_via_registry(
        ticker=ticker,
        concept_id=concept_id,
        concept=concept,
        most_recent_fy=most_recent_fy,
        n_historical=n_historical,
        edgar_fetcher=edgar_fetcher,
    )
    log_shadow_diffs(
        concept_id=concept_id,
        ticker=ticker,
        legacy_values=legacy_result.values_dict,
        legacy_provenance=legacy_result.provenance_by_year,
        upstream_values=upstream_result.values_dict,
        upstream_provenance=upstream_result.provenance_by_year,
    )


def _fetch_via_registry(
    *,
    ticker: str,
    concept_id: str,
    concept: DataSourceMapping,
    most_recent_fy: int,
    n_historical: int,
    edgar_fetcher: EdgarFetcher,
) -> EdgarConceptFetchResult:
    edgar_concept_fetch_result = _required_parent_attr("EdgarConceptFetchResult")
    requested_years_for_fetch = _parent_attr(
        "_requested_years_for_fetch",
        _requested_years_for_fetch,
    )
    registry_failed_result = _parent_attr("_registry_failed_result", _registry_failed_result)
    call_edgar_metric_fetcher = _parent_attr(
        "_call_edgar_metric_fetcher",
        _call_edgar_metric_fetcher,
    )
    edgar_fetch_error_message = _parent_attr(
        "_edgar_fetch_error_message",
        _edgar_fetch_error_message,
    )
    parse_edgar_series_source_result = _parent_attr(
        "_parse_edgar_series_source_result",
        _parse_edgar_series_source_result_fallback,
    )
    safe_int = _parent_attr("_safe_int", _safe_int_fallback)
    get_registry_cache = _parent_attr("get_registry_cache", _get_registry_cache_fallback)
    requested_group_id = str(concept.registry_group_id or "").strip()
    requested_years = requested_years_for_fetch(most_recent_fy, n_historical)
    if not requested_group_id:
        return edgar_concept_fetch_result(
            values_dict={},
            failed_years=set(),
            status="missing",
            periods_failed=0,
            api_calls=0,
        )

    cache = get_registry_cache()
    requested_group = cache.get_group(requested_group_id)
    if requested_group is None:
        cache.refresh()
        requested_group = cache.get_group(requested_group_id)
    if requested_group is None:
        if getattr(cache, "last_error", None) is not None and cache.registry_revision is None:
            return registry_failed_result(requested_years, api_calls=0)
        logging.error(
            "Registry group '%s' missing for concept '%s'",
            requested_group_id,
            concept_id,
        )
        return registry_failed_result(requested_years, api_calls=0)
    if requested_group.deprecated and requested_group.split_into:
        logging.error(
            "Registry group '%s' was split into %s; concept '%s' requires manual migration",
            requested_group_id,
            ", ".join(requested_group.split_into),
            concept_id,
        )
        return registry_failed_result(requested_years, api_calls=0)

    api_calls = 1
    response = call_edgar_metric_fetcher(
        edgar_fetcher,
        ticker,
        requested_group_id,
        most_recent_fy,
        n_historical,
        include_equivalents=True,
    )
    top_level_status = str(response.get("status") or "").lower()
    if top_level_status == "error":
        reason = str(response.get("reason") or "").lower()
        if reason == "group_split":
            logging.error(
                "Registry group '%s' returned group_split for concept '%s'",
                requested_group_id,
                concept_id,
            )
        return registry_failed_result(
            requested_years,
            api_calls=api_calls,
            error_message=edgar_fetch_error_message(response),
        )

    parsed = parse_edgar_series_source_result(
        response,
        concept_id,
        concept,
        ticker=ticker,
        requested_registry_group_id=requested_group_id,
        requested_registry_group=requested_group,
    )
    values_dict = {
        year: value
        for year, value in parsed.values_dict.items()
        if year in requested_years
    }
    provenance_by_year = {
        year: provenance
        for year, provenance in parsed.provenance_by_year.items()
        if year in requested_years
    }
    source_values_by_year = {
        year: source_value
        for year, source_value in parsed.source_values_by_year.items()
        if year in requested_years
    }
    reported_period_ends_by_year = {
        year: reported_period_end
        for year, reported_period_end in parsed.reported_period_ends_by_year.items()
        if year in requested_years
    }

    top_level_failed = safe_int(response.get("periods_failed")) or 0
    unresolved_failed_years = {
        year for year in parsed.failed_years
        if year in requested_years and year not in values_dict
    }
    if top_level_failed > parsed.entry_failed:
        unresolved_failed_years |= (requested_years - set(values_dict))

    unresolved_failed_years -= set(values_dict)
    unresolved_failed_years &= requested_years

    if not values_dict:
        if top_level_failed > 0 or parsed.entry_failed > 0 or unresolved_failed_years:
            return registry_failed_result(requested_years, api_calls=api_calls)
        return edgar_concept_fetch_result(
            values_dict={},
            failed_years=set(),
            status="missing",
            periods_failed=0,
            api_calls=api_calls,
        )

    return edgar_concept_fetch_result(
        values_dict=values_dict,
        failed_years=unresolved_failed_years,
        status="ok",
        periods_failed=len(unresolved_failed_years),
        api_calls=api_calls,
        provenance_by_year=provenance_by_year,
        source_values_by_year=source_values_by_year,
        reported_period_ends_by_year=reported_period_ends_by_year,
    )


def _fetch_dimensional_edgar_concept(
    *,
    ticker: str,
    concept_id: str,
    concept: DataSourceMapping,
    axis_key: str,
    most_recent_fy: int,
    n_historical: int,
    edgar_fetcher: EdgarFetcher,
    include_equivalents: bool = False,
    allow_equivalent_tags: bool = False,
    include_local_tag_candidates: bool = False,
) -> EdgarConceptFetchResult:
    edgar_concept_fetch_result = _required_parent_attr("EdgarConceptFetchResult")
    edgar_tag_lookup_candidates = _parent_attr(
        "_edgar_tag_lookup_candidates",
        _edgar_tag_lookup_candidates,
    )
    requested_years_for_fetch = _parent_attr(
        "_requested_years_for_fetch",
        _requested_years_for_fetch,
    )
    call_edgar_metric_fetcher = _parent_attr(
        "_call_edgar_metric_fetcher",
        _call_edgar_metric_fetcher,
    )
    edgar_fetch_error_message = _parent_attr(
        "_edgar_fetch_error_message",
        _edgar_fetch_error_message,
    )
    parse_edgar_series_source_result = _parent_attr(
        "_parse_edgar_series_source_result",
        _parse_edgar_series_source_result_fallback,
    )
    safe_int = _parent_attr("_safe_int", _safe_int_fallback)
    tags = concept.edgar_tags or []
    if not tags:
        return edgar_concept_fetch_result(
            values_dict={},
            failed_years=set(),
            status="missing",
            periods_failed=0,
            api_calls=0,
        )

    tags_to_try = edgar_tag_lookup_candidates(tags) if include_local_tag_candidates else tags[:1]
    requested_years = requested_years_for_fetch(most_recent_fy, n_historical)
    accumulated: Dict[int, float] = {}
    accumulated_provenance: Dict[int, EdgarProvenance] = {}
    accumulated_source_values: Dict[int, SourceValue] = {}
    accumulated_reported_period_ends: Dict[int, str] = {}
    unresolved_failed_years: set[int] = set()
    api_calls = 0

    for tag in tags_to_try:
        api_calls += 1
        response = call_edgar_metric_fetcher(
            edgar_fetcher,
            ticker,
            tag,
            most_recent_fy,
            n_historical,
            include_equivalents=include_equivalents,
            axis_key=axis_key,
        )
        top_level_status = str(response.get("status") or "").lower()
        top_level_failed = safe_int(response.get("periods_failed")) or 0

        if top_level_status == "error":
            error_message = edgar_fetch_error_message(response)
            if (
                top_level_failed > 0
                and not accumulated
                and not include_local_tag_candidates
            ):
                return edgar_concept_fetch_result(
                    values_dict={},
                    failed_years=set(),
                    status="failed",
                    periods_failed=top_level_failed,
                    api_calls=api_calls,
                    error_message=error_message,
                )
            unresolved_failed_years |= (requested_years - set(accumulated))
            continue

        parsed = parse_edgar_series_source_result(
            response,
            concept_id,
            concept,
            ticker=ticker,
            requested_tag=tag,
            allow_equivalent_tags=allow_equivalent_tags,
        )
        for year, value in parsed.values_dict.items():
            if year in requested_years and year not in accumulated:
                accumulated[year] = value
                if year in parsed.provenance_by_year:
                    accumulated_provenance[year] = parsed.provenance_by_year[year]
                if year in parsed.source_values_by_year:
                    accumulated_source_values[year] = parsed.source_values_by_year[year]
                if year in parsed.reported_period_ends_by_year:
                    accumulated_reported_period_ends[year] = parsed.reported_period_ends_by_year[year]
        unresolved_failed_years |= {
            year for year in parsed.failed_years
            if year in requested_years and year not in accumulated
        }
        if top_level_failed > parsed.entry_failed:
            unresolved_failed_years |= (requested_years - set(accumulated))
        if requested_years.issubset(accumulated):
            break

    unresolved_failed_years -= set(accumulated)
    unresolved_failed_years &= requested_years

    return edgar_concept_fetch_result(
        values_dict=accumulated,
        failed_years=unresolved_failed_years,
        status="ok" if accumulated else "missing",
        periods_failed=len(unresolved_failed_years),
        api_calls=api_calls,
        provenance_by_year=accumulated_provenance,
        source_values_by_year=accumulated_source_values,
        reported_period_ends_by_year=accumulated_reported_period_ends,
    )


def _fetch_legacy_edgar_concept(
    *,
    ticker: str,
    concept_id: str,
    concept: DataSourceMapping,
    most_recent_fy: int,
    n_historical: int,
    edgar_fetcher: EdgarFetcher,
    include_equivalents: bool = False,
    allow_equivalent_tags: bool = False,
    include_local_tag_candidates: bool = False,
) -> EdgarConceptFetchResult:
    edgar_concept_fetch_result = _required_parent_attr("EdgarConceptFetchResult")
    edgar_tag_lookup_candidates = _parent_attr(
        "_edgar_tag_lookup_candidates",
        _edgar_tag_lookup_candidates,
    )
    requested_years_for_fetch = _parent_attr(
        "_requested_years_for_fetch",
        _requested_years_for_fetch,
    )
    single_tag_lookup = _parent_attr("_single_tag_lookup", _single_tag_lookup)
    select_single_scope_edgar_tag_result = _parent_attr(
        "_select_single_scope_edgar_tag_result",
        _select_single_scope_edgar_tag_result,
    )
    nonadmissible_reason_code = _parent_attr(
        "NonadmissibleReasonCode",
        NonadmissibleReasonCode,
    )
    tags = concept.edgar_tags or []
    if not tags:
        return edgar_concept_fetch_result(
            values_dict={},
            failed_years=set(),
            status="missing",
            periods_failed=0,
            api_calls=0,
        )

    boundary = concept.non_equivalent_after
    if boundary is not None:
        tags_to_try = tags[: max(0, boundary) + 1]
    else:
        tags_to_try = tags[:1]
    if include_local_tag_candidates:
        tags_to_try = edgar_tag_lookup_candidates(tags_to_try)

    choose_single_tag = (
        len(tags_to_try) > 1
        and concept.nonadmissible_reason_code == nonadmissible_reason_code.broader_or_narrower_scope
    )
    requested_years = requested_years_for_fetch(most_recent_fy, n_historical)
    accumulated: Dict[int, float] = {}
    accumulated_provenance: Dict[int, EdgarProvenance] = {}
    accumulated_source_values: Dict[int, SourceValue] = {}
    accumulated_reported_period_ends: Dict[int, str] = {}
    unresolved_failed_years: set[int] = set()
    api_calls = 0
    tag_results: list[EdgarConceptFetchResult] = []

    for tag in tags_to_try:
        tag_result = single_tag_lookup(
            ticker,
            tag,
            most_recent_fy,
            n_historical,
            edgar_fetcher,
            concept_id=concept_id,
            concept=concept,
            include_equivalents=include_equivalents,
            allow_equivalent_tags=allow_equivalent_tags,
        )
        api_calls += tag_result.api_calls

        if choose_single_tag:
            tag_results.append(tag_result)
            continue

        if tag_result.status == "failed" and not tag_result.values_dict:
            if include_local_tag_candidates:
                unresolved_failed_years |= (requested_years - set(accumulated))
                continue
            if accumulated:
                unresolved_failed_years |= (requested_years - set(accumulated))
                break
            tag_result.api_calls = api_calls
            return tag_result

        for year, value in tag_result.values_dict.items():
            if year in requested_years and year not in accumulated:
                accumulated[year] = value
                if year in tag_result.provenance_by_year:
                    accumulated_provenance[year] = tag_result.provenance_by_year[year]
                if year in tag_result.source_values_by_year:
                    accumulated_source_values[year] = tag_result.source_values_by_year[year]
                if year in tag_result.reported_period_ends_by_year:
                    accumulated_reported_period_ends[year] = (
                        tag_result.reported_period_ends_by_year[year]
                    )

        unresolved_failed_years |= {
            year for year in tag_result.failed_years
            if year in requested_years and year not in accumulated
        }

        if requested_years.issubset(accumulated):
            break

    if choose_single_tag:
        return select_single_scope_edgar_tag_result(
            tag_results,
            requested_years=requested_years,
            api_calls=api_calls,
        )

    unresolved_failed_years -= set(accumulated)
    unresolved_failed_years &= requested_years
    unresolved_periods_failed = len(unresolved_failed_years)

    if not accumulated:
        return edgar_concept_fetch_result(
            values_dict={},
            failed_years=unresolved_failed_years,
            status="missing",
            periods_failed=unresolved_periods_failed,
            api_calls=api_calls,
        )

    return edgar_concept_fetch_result(
        values_dict=accumulated,
        failed_years=unresolved_failed_years,
        status="ok",
        periods_failed=unresolved_periods_failed,
        api_calls=api_calls,
        provenance_by_year=accumulated_provenance,
        source_values_by_year=accumulated_source_values,
        reported_period_ends_by_year=accumulated_reported_period_ends,
    )


def _select_single_scope_edgar_tag_result(
    tag_results: list[EdgarConceptFetchResult],
    *,
    requested_years: set[int],
    api_calls: int,
) -> EdgarConceptFetchResult:
    edgar_concept_fetch_result = _required_parent_attr("EdgarConceptFetchResult")
    best_result: EdgarConceptFetchResult | None = None
    best_coverage = -1
    for result in tag_results:
        coverage = len(set(result.values_dict).intersection(requested_years))
        if coverage > best_coverage:
            best_result = result
            best_coverage = coverage

    if best_result is not None and best_coverage > 0:
        values = {
            year: value
            for year, value in best_result.values_dict.items()
            if year in requested_years
        }
        failed_years = (
            set(best_result.failed_years)
            | (requested_years - set(values))
        ) - set(values)
        return edgar_concept_fetch_result(
            values_dict=values,
            failed_years=failed_years,
            status="ok",
            periods_failed=len(failed_years),
            api_calls=api_calls,
            provenance_by_year={
                year: provenance
                for year, provenance in best_result.provenance_by_year.items()
                if year in values
            },
            source_values_by_year={
                year: source_value
                for year, source_value in best_result.source_values_by_year.items()
                if year in values
            },
            reported_period_ends_by_year={
                year: period_end
                for year, period_end in best_result.reported_period_ends_by_year.items()
                if year in values
            },
        )

    failed_result = next(
        (result for result in tag_results if result.status == "failed" and not result.values_dict),
        None,
    )
    if failed_result is not None:
        failed_result.api_calls = api_calls
        return failed_result

    unresolved_failed_years = (
        set().union(*(result.failed_years for result in tag_results))
        if tag_results
        else set()
    )
    unresolved_failed_years &= requested_years
    return edgar_concept_fetch_result(
        values_dict={},
        failed_years=unresolved_failed_years,
        status="missing",
        periods_failed=len(unresolved_failed_years),
        api_calls=api_calls,
    )


def _single_tag_lookup(
    ticker: str,
    tag: str,
    most_recent_fy: int,
    n_historical: int,
    edgar_fetcher: EdgarFetcher,
    *,
    concept_id: str,
    concept: DataSourceMapping,
    include_equivalents: bool = False,
    allow_equivalent_tags: bool = False,
) -> EdgarConceptFetchResult:
    edgar_concept_fetch_result = _required_parent_attr("EdgarConceptFetchResult")
    requested_years_for_fetch = _parent_attr(
        "_requested_years_for_fetch",
        _requested_years_for_fetch,
    )
    call_edgar_metric_fetcher = _parent_attr(
        "_call_edgar_metric_fetcher",
        _call_edgar_metric_fetcher,
    )
    edgar_fetch_error_message = _parent_attr(
        "_edgar_fetch_error_message",
        _edgar_fetch_error_message,
    )
    parse_edgar_series_source_result = _parent_attr(
        "_parse_edgar_series_source_result",
        _parse_edgar_series_source_result_fallback,
    )
    safe_int = _parent_attr("_safe_int", _safe_int_fallback)
    requested_years = requested_years_for_fetch(most_recent_fy, n_historical)
    api_calls = 1
    response = call_edgar_metric_fetcher(
        edgar_fetcher,
        ticker,
        tag,
        most_recent_fy,
        n_historical,
        include_equivalents=include_equivalents,
    )
    top_level_status = str(response.get("status") or "").lower()
    top_level_failed = safe_int(response.get("periods_failed"))

    if top_level_status == "error":
        error_message = edgar_fetch_error_message(response)
        if top_level_failed > 0:
            return edgar_concept_fetch_result(
                values_dict={},
                failed_years=set(),
                status="failed",
                periods_failed=top_level_failed,
                api_calls=api_calls,
                error_message=error_message,
            )
        return edgar_concept_fetch_result(
            values_dict={},
            failed_years=set(),
            status="missing",
            periods_failed=0,
            api_calls=api_calls,
            error_message=error_message,
        )

    parsed = parse_edgar_series_source_result(
        response,
        concept_id,
        concept,
        ticker=ticker,
        requested_tag=tag,
        allow_equivalent_tags=allow_equivalent_tags,
    )
    values_dict = {
        year: value
        for year, value in parsed.values_dict.items()
        if year in requested_years
    }
    provenance_by_year = {
        year: provenance
        for year, provenance in parsed.provenance_by_year.items()
        if year in requested_years
    }
    source_values_by_year = {
        year: source_value
        for year, source_value in parsed.source_values_by_year.items()
        if year in requested_years
    }
    reported_period_ends_by_year = {
        year: reported_period_end
        for year, reported_period_end in parsed.reported_period_ends_by_year.items()
        if year in requested_years
    }

    unresolved_failed_years = {
        year for year in parsed.failed_years
        if year in requested_years and year not in values_dict
    }
    if top_level_failed > parsed.entry_failed:
        unresolved_failed_years |= (requested_years - set(values_dict))

    unresolved_failed_years -= set(values_dict)
    unresolved_failed_years &= requested_years
    unresolved_periods_failed = len(unresolved_failed_years)

    if not values_dict:
        return edgar_concept_fetch_result(
            values_dict={},
            failed_years=unresolved_failed_years,
            status="missing",
            periods_failed=unresolved_periods_failed,
            api_calls=api_calls,
        )

    return edgar_concept_fetch_result(
        values_dict=values_dict,
        failed_years=unresolved_failed_years,
        status="ok",
        periods_failed=unresolved_periods_failed,
        api_calls=api_calls,
        provenance_by_year=provenance_by_year,
        source_values_by_year=source_values_by_year,
        reported_period_ends_by_year=reported_period_ends_by_year,
    )


__all__ = [
    "_SEEN_DEPRECATED_REGISTRY_GROUPS",
    "_call_edgar_metric_fetcher",
    "_edgar_fetch_error_message",
    "_edgar_negate_enabled",
    "_edgar_tag_lookup_candidates",
    "_equivalent_tag_validated_by_registry",
    "_fetch_dimensional_edgar_concept",
    "_fetch_edgar_concept",
    "_fetch_edgar_concept_result",
    "_fetch_legacy_edgar_concept",
    "_fetch_via_registry",
    "_log_deprecated_registry_group_once",
    "_metric_tag_matches_group",
    "_registry_equivalence_value_conflict",
    "_registry_failed_result",
    "_requested_years_for_fetch",
    "_resolve_registry_response_group",
    "_run_shadow_compare",
    "_select_single_scope_edgar_tag_result",
    "_single_tag_lookup",
    "_tags_equivalent",
    "_validated_registry_group_for_metric_tag",
]
