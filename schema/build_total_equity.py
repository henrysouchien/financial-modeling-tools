"""Total-equity EDGAR fallback helpers for schema build orchestration."""

from __future__ import annotations

import sys
from typing import TYPE_CHECKING, Any

from .models import DataSourceMapping, EdgarProvenance
from .source_values import normalize_edgar_source_value as _normalize_edgar_source_value_fallback

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


def _total_equity_derived_fallback(
    ticker: str,
    concept: DataSourceMapping,
    base_result: EdgarConceptFetchResult,
    most_recent_fy: int,
    n_historical: int,
    edgar_fetcher: EdgarFetcher,
) -> EdgarConceptFetchResult:
    requested_years_for_fetch = _required_parent_attr("_requested_years_for_fetch")
    single_tag_lookup = _required_parent_attr("_single_tag_lookup")
    edgar_concept_fetch_result = _required_parent_attr("EdgarConceptFetchResult")
    edgar_provenance = _parent_attr("EdgarProvenance", EdgarProvenance)
    normalize_edgar_source_value = _parent_attr(
        "normalize_edgar_source_value",
        _normalize_edgar_source_value_fallback,
    )

    requested_years = requested_years_for_fetch(most_recent_fy, n_historical)
    missing_years = requested_years - set(base_result.values_dict)
    if not missing_years:
        return base_result

    augmented_values = dict(base_result.values_dict)
    augmented_provenance = dict(base_result.provenance_by_year or {})
    augmented_source_values = dict(base_result.source_values_by_year or {})
    augmented_reported_period_ends = dict(base_result.reported_period_ends_by_year or {})
    api_calls = base_result.api_calls

    parent_alt = single_tag_lookup(
        ticker,
        "StockholdersEquityAttributableToParent",
        most_recent_fy,
        n_historical,
        edgar_fetcher,
        concept_id="total_equity",
        concept=concept,
    )
    api_calls += parent_alt.api_calls

    parent_alt_failed_years: set[int] = set()
    if parent_alt.status == "failed" and not parent_alt.values_dict:
        parent_alt_failed_years = set(missing_years)
    elif parent_alt.status not in {"ok", "missing"} and not parent_alt.values_dict:
        parent_alt_failed_years = set(missing_years)
    elif parent_alt.failed_years:
        parent_alt_failed_years = set(parent_alt.failed_years) & set(missing_years)

    for year in list(missing_years):
        value = parent_alt.values_dict.get(year)
        if value is None:
            continue
        augmented_values[year] = value
        if year in parent_alt.provenance_by_year:
            augmented_provenance[year] = parent_alt.provenance_by_year[year]
        if year in parent_alt.source_values_by_year:
            augmented_source_values[year] = parent_alt.source_values_by_year[year]
        if year in parent_alt.reported_period_ends_by_year:
            augmented_reported_period_ends[year] = parent_alt.reported_period_ends_by_year[year]
        missing_years.discard(year)
        parent_alt_failed_years.discard(year)

    if not missing_years:
        final_failed = base_result.failed_years - set(augmented_values)
        return edgar_concept_fetch_result(
            values_dict=augmented_values,
            failed_years=final_failed,
            status="ok",
            periods_failed=len(final_failed),
            api_calls=api_calls,
            provenance_by_year=augmented_provenance,
            source_values_by_year=augmented_source_values,
            reported_period_ends_by_year=augmented_reported_period_ends,
        )

    with_nci = single_tag_lookup(
        ticker,
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        most_recent_fy,
        n_historical,
        edgar_fetcher,
        concept_id="total_equity",
        concept=concept,
    )
    api_calls += with_nci.api_calls

    if not with_nci.values_dict:
        derived_failed_years_pre: set[int] = set()
        if with_nci.status == "failed":
            derived_failed_years_pre = set(missing_years)
        elif with_nci.status not in {"ok", "missing"}:
            derived_failed_years_pre = set(missing_years)
        elif with_nci.failed_years:
            derived_failed_years_pre = set(with_nci.failed_years) & set(missing_years)
        final_failed_pre = (
            base_result.failed_years
            | parent_alt_failed_years
            | derived_failed_years_pre
        ) - set(augmented_values)
        return edgar_concept_fetch_result(
            values_dict=augmented_values,
            failed_years=final_failed_pre,
            status=base_result.status if not augmented_values else "ok",
            periods_failed=len(final_failed_pre),
            api_calls=api_calls,
            provenance_by_year=augmented_provenance,
            source_values_by_year=augmented_source_values,
            reported_period_ends_by_year=augmented_reported_period_ends,
        )

    mi = single_tag_lookup(
        ticker,
        "MinorityInterest",
        most_recent_fy,
        n_historical,
        edgar_fetcher,
        concept_id="total_equity",
        concept=concept,
    )
    api_calls += mi.api_calls

    derived_failed_years: set[int] = set()
    for year in list(missing_years):
        nci_value = with_nci.values_dict.get(year)
        if nci_value is None:
            if year in with_nci.failed_years:
                derived_failed_years.add(year)
            continue

        if mi.status == "failed" or year in mi.failed_years:
            derived_failed_years.add(year)
            continue
        if mi.values_dict.get(year) is not None:
            mi_value = float(mi.values_dict[year])
        elif mi.status in {"ok", "missing"}:
            mi_value = 0.0
        else:
            derived_failed_years.add(year)
            continue

        derived_value = float(nci_value) - mi_value
        augmented_values[year] = derived_value
        augmented_provenance[year] = edgar_provenance(
            metric_tag="derived:WithNCI_minus_MinorityInterest",
        )
        augmented_source_values[year] = normalize_edgar_source_value(
            value=derived_value,
            scale="millions",
            concept_id="total_equity",
            source="edgar",
            tag="derived:WithNCI_minus_MinorityInterest",
            year=year,
            source_ref={
                "derivation": "WithNCI - MinorityInterest",
                "withnci_value": float(nci_value),
                "mi_value": mi_value,
            },
        )
        if year in with_nci.reported_period_ends_by_year:
            augmented_reported_period_ends[year] = with_nci.reported_period_ends_by_year[year]

    final_failed = (
        base_result.failed_years
        | parent_alt_failed_years
        | derived_failed_years
    ) - set(augmented_values)
    return edgar_concept_fetch_result(
        values_dict=augmented_values,
        failed_years=final_failed,
        status="ok" if augmented_values else base_result.status,
        periods_failed=len(final_failed),
        api_calls=api_calls,
        provenance_by_year=augmented_provenance,
        source_values_by_year=augmented_source_values,
        reported_period_ends_by_year=augmented_reported_period_ends,
    )


def _maybe_total_equity_derived_fallback(
    *,
    result: EdgarConceptFetchResult,
    ticker: str,
    concept_id: str,
    concept: DataSourceMapping,
    most_recent_fy: int,
    n_historical: int,
    edgar_fetcher: EdgarFetcher,
) -> EdgarConceptFetchResult:
    if concept_id != "total_equity":
        return result
    if result.status not in {"ok", "missing"}:
        return result

    requested_years_for_fetch = _required_parent_attr("_requested_years_for_fetch")
    requested_years = requested_years_for_fetch(most_recent_fy, n_historical)
    if requested_years.issubset(set(result.values_dict)):
        return result

    total_equity_derived_fallback = _parent_attr(
        "_total_equity_derived_fallback",
        _total_equity_derived_fallback,
    )
    return total_equity_derived_fallback(
        ticker=ticker,
        concept=concept,
        base_result=result,
        most_recent_fy=most_recent_fy,
        n_historical=n_historical,
        edgar_fetcher=edgar_fetcher,
    )
