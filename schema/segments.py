"""Dynamic revenue segment discovery, expansion, and population helpers."""

from __future__ import annotations

from collections import defaultdict
import concurrent.futures
from dataclasses import dataclass, field
import logging
import re
from typing import Callable, Dict, Iterable, List, Optional, Sequence

from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    LineItem,
    LineItemRef,
    Section,
    SheetType,
    ValueCell,
    ValueProvenance,
    ValueSeries,
)
from .model_build_context import SegmentRevenueObservation
from .source_values import normalize_edgar_value


EdgarFinancialsFetcher = Callable[[str, int, int, bool], Dict]


_SCENARIO_SELECTOR_ID = "tpl.a.header.scenario_value"
_SCENARIO_VOLUME_GROWTH_LABEL_ID = "tpl.a.scenario_tables.scenario_volume_growth_label"
_COLUMN_OFFSET_MODE_PERIOD_RELATIVE = "period_relative"

SEGMENT_ITEM_ID_ROLES = (
    "volume_driver",
    "volume_growth",
    "price_driver",
    "price_growth",
    "revenue",
    "growth",
    "revenue_fm",
    "margin_fm",
    "growth_fm",
)

REVENUE_TAGS_PRIORITY = [
    "RevenueFromContractWithCustomerExcludingAssessedTax",
    "SalesRevenueNet",
    "Revenues",
]

SEGMENT_AXES_PRIORITY = [
    "StatementBusinessSegmentsAxis",
    "ProductOrServiceAxis",
    "StatementGeographicalAxis",
]

EXCLUDED_MEMBER_SUFFIXES = {
    "CorporateNonSegmentMember",
    "CorporateMember",
    "IntersegmentEliminationMember",
    "EliminationOfIntersegmentAmountsMember",
}

MATERIALITY_THRESHOLD = 0.02

_REVENUE_TAGS = set(REVENUE_TAGS_PRIORITY)
_ASSUMPTIONS_SEGMENT_IDS = {
    "tpl.a.revenue_drivers.volume_driver_1",
    "tpl.a.revenue_drivers.volume_1_growth",
    "tpl.a.revenue_drivers.volume_driver_2",
    "tpl.a.revenue_drivers.volume_2_growth",
    "tpl.a.revenue_drivers.volume_driver_3",
    "tpl.a.revenue_drivers.volume_3_growth",
    "tpl.a.revenue_drivers.price_driver_1",
    "tpl.a.revenue_drivers.price_1_growth",
    "tpl.a.revenue_drivers.operating_metric",
    "tpl.a.revenue_drivers.operating_metric_growth",
    "tpl.a.revenue_drivers.business_segment_1_revenue",
    "tpl.a.revenue_drivers.business_segment_1_growth",
    "tpl.a.revenue_drivers.business_segment_2_volume_driver_1",
    "tpl.a.revenue_drivers.business_segment_2_volume_growth",
    "tpl.a.revenue_drivers.business_segment_2_price_driver_1",
    "tpl.a.revenue_drivers.business_segment_2_price_growth",
    "tpl.a.revenue_drivers.business_segment_2_revenue",
    "tpl.a.revenue_drivers.business_segment_2_growth",
}
_FM_SEGMENT_IDS = {
    "tpl.fm.income_statement.business_segment_1_revenue",
    "tpl.fm.income_statement.business_segment_2_revenue",
    "tpl.fm.margins.business_segment_1_pct_revenue",
    "tpl.fm.margins.business_segment_2_pct_revenue",
    "tpl.fm.growth_rates.business_segment_1_growth",
    "tpl.fm.growth_rates.business_segment_2_growth",
}
_ASSUMPTIONS_SEGMENT_START_ROW = 7
_ASSUMPTIONS_TOTAL_REVENUE_ROW = 27
_FM_SECTION_START_ROWS = {
    "income_statement": 5,
    "margins": 41,
    "growth_rates": 53,
}
_FM_SECTION_SHIFT_ROWS = {
    "income_statement": 7,
    "margins": 43,
    "growth_rates": 55,
}
_SCENARIO_TABLE_OLD_GROWTH_ID = "tpl.a.revenue_drivers.volume_1_growth"
_SCENARIO_TABLE_NEW_GROWTH_ID = "tpl.a.revenue_drivers.business_segment_1_growth"


def _rebuild_later_ids() -> frozenset[str]:
    """Items whose formulas reference segment IDs but are rebuilt later in the pipeline.

    Skip these during generic dangling-ref repair -- they will be fixed by
    their dedicated rebuild step.
    """

    return frozenset(
        {
            "tpl.a.revenue_drivers.total_revenue",  # rebuilt by _update_total_revenue_formulas()
            "tpl.fm.income_statement.total_revenue",  # rebuilt by _update_total_revenue_formulas()
        }
    )


_REBUILD_LATER_IDS = _rebuild_later_ids()


@dataclass
class SegmentInfo:
    name: str
    edgar_member: Optional[str] = None
    revenue_observations: Optional[Dict[int, SegmentRevenueObservation]] = None
    volume_label: Optional[str] = None
    price_label: Optional[str] = None
    item_ids: Optional[Dict[str, str]] = None


@dataclass
class SegmentProfile:
    ticker: str
    segments: List[SegmentInfo]
    source: str
    axis_used: Optional[str] = None
    total_revenue_check: Optional[Dict[int, float]] = None


def segment_revenue_values(segment: SegmentInfo) -> Dict[int, float]:
    """Return model-ready revenue values derived from typed observations."""

    observations = segment.revenue_observations or {}
    values: Dict[int, float] = {}
    for raw_year, observation in observations.items():
        year = int(getattr(observation, "fiscal_year", raw_year))
        value = getattr(observation, "value", None)
        if value is None:
            continue
        values[year] = float(value)
    return dict(sorted(values.items()))


def segment_revenue_observation_list(segment: SegmentInfo) -> list[SegmentRevenueObservation] | None:
    observations = segment.revenue_observations or {}
    if not observations:
        return None
    return [
        observation
        for _year, observation in sorted(
            ((int(year), observation) for year, observation in observations.items()),
            key=lambda item: item[0],
        )
    ]


def revenue_observations_to_values(
    observations: Sequence[SegmentRevenueObservation] | None,
) -> Dict[int, float]:
    values: Dict[int, float] = {}
    for observation in observations or []:
        values[int(observation.fiscal_year)] = float(observation.value)
    return dict(sorted(values.items()))


def segment_revenue_observations_from_snapshot(
    snapshot_segment: object,
) -> Dict[int, SegmentRevenueObservation] | None:
    observations = getattr(snapshot_segment, "revenue_observations", None)
    if not observations:
        return None
    return {
        int(observation.fiscal_year): observation
        for observation in sorted(observations, key=lambda item: int(item.fiscal_year))
    }


def _derived_revenue_observations(
    values: Dict[int, float],
    *,
    source: str,
    note: str,
) -> Dict[int, SegmentRevenueObservation]:
    observations: Dict[int, SegmentRevenueObservation] = {}
    for year, value in sorted(values.items()):
        observations[int(year)] = SegmentRevenueObservation(
            fiscal_year=int(year),
            value=float(value),
            source=source,
            comparable_with_prior="unknown",
            comparability_note=note,
        )
    return _annotate_revenue_comparability(observations)


@dataclass
class MultiAxisResult:
    ticker: str
    profiles: List[SegmentProfile]
    total_revenue_check: Optional[Dict[int, float]] = None
    payloads_by_year: Dict[int, dict] = field(default_factory=dict)


@dataclass
class ExpandResult:
    segments_created: int
    items_added: int
    items_relabeled: int


def discover_all_axes(
    ticker: str,
    fetcher: EdgarFinancialsFetcher,
    most_recent_fy: int,
    n_historical: int = 5,
) -> MultiAxisResult:
    """Auto-discover revenue segments from EDGAR dimensional financial facts."""

    historical_years = list(range(most_recent_fy - n_historical + 1, most_recent_fy + 1))
    all_consolidated: List[Dict] = []
    all_dimensional: List[Dict] = []
    payloads_by_year: Dict[int, dict] = {}

    def _fetch_year(year: int) -> tuple[int, dict, List[Dict], List[Dict]]:
        try:
            payload = fetcher(ticker, year, 4, True) or {}
        except Exception as exc:  # pragma: no cover - exercised by callers
            raise RuntimeError(f"EDGAR financials fetch failed for {ticker} {year}: {exc}") from exc

        if not isinstance(payload, dict):
            raise RuntimeError(f"EDGAR financials fetch failed for {ticker} {year}: malformed response")

        status = str(payload.get("status") or "").lower()
        if status == "error":
            message = payload.get("message") or payload.get("error") or "unknown error"
            raise RuntimeError(f"EDGAR financials fetch failed for {ticker} {year}: {message}")

        facts = _extract_fact_rows(payload)
        annotated = []
        for fact in facts:
            copied = dict(fact)
            copied["_segment_year"] = int(year)
            annotated.append(copied)

        consolidated, dimensional = _filter_revenue_facts(annotated)
        return int(year), payload, consolidated, dimensional

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(_fetch_year, year): year
            for year in historical_years
        }
        for future in concurrent.futures.as_completed(futures):
            year, payload, consolidated, dimensional = future.result()
            if payload:
                payloads_by_year[year] = payload
            all_consolidated.extend(consolidated)
            all_dimensional.extend(dimensional)

    consolidated_values = _consolidated_values(all_consolidated)
    if not all_dimensional or not consolidated_values:
        return MultiAxisResult(
            ticker=ticker,
            profiles=[],
            total_revenue_check=consolidated_values or None,
            payloads_by_year=payloads_by_year,
        )

    profiles: List[SegmentProfile] = []
    for axis in SEGMENT_AXES_PRIORITY:
        segments = _try_axis_decomposition(
            all_dimensional,
            consolidated_values,
            axis,
            tolerance=0.05,
        )
        if not segments:
            continue
        segments.sort(key=_segment_sort_key, reverse=True)
        profiles.append(
            SegmentProfile(
                ticker=ticker,
                segments=segments,
                source="edgar_auto",
                axis_used=axis,
                total_revenue_check=consolidated_values,
            )
        )

    return MultiAxisResult(
        ticker=ticker,
        profiles=profiles,
        total_revenue_check=consolidated_values or None,
        payloads_by_year=payloads_by_year,
    )


def discover_segments(
    ticker: str,
    fetcher: EdgarFinancialsFetcher,
    most_recent_fy: int,
    n_historical: int = 5,
) -> SegmentProfile:
    """Auto-discover revenue segments from EDGAR dimensional financial facts."""

    result = discover_all_axes(
        ticker=ticker,
        fetcher=fetcher,
        most_recent_fy=most_recent_fy,
        n_historical=n_historical,
    )
    if result.profiles:
        return result.profiles[0]

    return SegmentProfile(
        ticker=ticker,
        segments=[],
        source="edgar_auto",
        total_revenue_check=result.total_revenue_check,
    )


def discover_segments_with_payloads(
    ticker: str,
    fetcher: EdgarFinancialsFetcher,
    most_recent_fy: int,
    n_historical: int = 5,
) -> tuple[SegmentProfile, dict[int, dict]]:
    """Like discover_segments() but also returns the raw per-year payloads."""

    result = discover_all_axes(
        ticker=ticker,
        fetcher=fetcher,
        most_recent_fy=most_recent_fy,
        n_historical=n_historical,
    )
    if result.profiles:
        return result.profiles[0], result.payloads_by_year

    return (
        SegmentProfile(
            ticker=ticker,
            segments=[],
            source="edgar_auto",
            total_revenue_check=result.total_revenue_check,
        ),
        result.payloads_by_year,
    )


def apply_segment_overrides(discovered: SegmentProfile, mapping: List[Dict]) -> SegmentProfile:
    """Apply caller-provided naming, KPI label, and ordering overrides."""

    discovered_by_member = {
        segment.edgar_member: segment
        for segment in discovered.segments
        if segment.edgar_member
    }
    matched_members: set[str] = set()
    segments: List[SegmentInfo] = []
    other_values: Dict[int, float] = defaultdict(float)

    for entry in list(mapping or []):
        member = str(entry.get("edgar_member") or "").strip()
        if not member:
            continue
        if member in matched_members:
            logging.warning("Ignoring duplicate segment_mapping entry for '%s'", member)
            continue

        discovered_segment = discovered_by_member.get(member)
        if discovered_segment is None:
            logging.warning("Ignoring unmatched segment_mapping entry for '%s'", member)
            continue

        matched_members.add(member)
        segments.append(
            SegmentInfo(
                name=str(entry.get("name") or discovered_segment.name),
                edgar_member=discovered_segment.edgar_member,
                revenue_observations=dict(discovered_segment.revenue_observations or {}),
                volume_label=entry.get("volume_label") or discovered_segment.volume_label,
                price_label=entry.get("price_label") or discovered_segment.price_label,
            )
        )

    for segment in discovered.segments:
        if segment.edgar_member and segment.edgar_member in matched_members:
            continue
        for year, value in segment_revenue_values(segment).items():
            other_values[int(year)] += float(value)

    if any(abs(value) > 1e-9 for value in other_values.values()):
        segments.append(
            SegmentInfo(
                name="Other",
                revenue_observations=_derived_revenue_observations(
                    other_values,
                    source="derived_other",
                    note="Aggregated from unmatched segment members after caller override.",
                ),
            )
        )

    return SegmentProfile(
        ticker=discovered.ticker,
        segments=segments,
        source="caller_override",
        axis_used=discovered.axis_used,
        total_revenue_check=dict(discovered.total_revenue_check or {}),
    )


def expand_segments(model: FinancialModel, profile: SegmentProfile) -> ExpandResult:
    """Replace the template's hardcoded PCTY segment block with canonical segments."""

    if not profile.segments:
        raise ValueError("expand_segments requires at least one segment")

    model.build_index()
    prototypes = {
        "volume_driver": model.get_item("tpl.a.revenue_drivers.volume_driver_1").model_copy(deep=True),
        "volume_growth": model.get_item("tpl.a.revenue_drivers.business_segment_2_volume_growth").model_copy(deep=True),
        "price_driver": model.get_item("tpl.a.revenue_drivers.price_driver_1").model_copy(deep=True),
        "price_growth": model.get_item("tpl.a.revenue_drivers.business_segment_2_price_growth").model_copy(deep=True),
        "revenue": model.get_item("tpl.a.revenue_drivers.business_segment_1_revenue").model_copy(deep=True),
        "growth": model.get_item("tpl.a.revenue_drivers.business_segment_2_volume_growth").model_copy(deep=True),
        "revenue_fm": model.get_item("tpl.fm.income_statement.business_segment_1_revenue").model_copy(deep=True),
        "margin_fm": model.get_item("tpl.fm.margins.business_segment_1_pct_revenue").model_copy(deep=True),
        "growth_fm": model.get_item("tpl.fm.growth_rates.business_segment_1_growth").model_copy(deep=True),
    }

    assumptions_rows, fm_rows = _remove_existing_segment_items(model)
    assumptions_delta = max(0, len(profile.segments) * 6 - assumptions_rows["freed"])
    if assumptions_delta:
        _shift_rows(_iter_sheet_items(model, "Assumptions"), _ASSUMPTIONS_TOTAL_REVENUE_ROW, assumptions_delta)

    fm_delta = max(0, len(profile.segments) - fm_rows["freed"])
    if fm_delta:
        _shift_rows(_iter_sheet_items(model, "Financial_model"), _FM_SECTION_SHIFT_ROWS["growth_rates"], fm_delta)
        _shift_rows(_iter_sheet_items(model, "Financial_model"), _FM_SECTION_SHIFT_ROWS["margins"], fm_delta)
        _shift_rows(_iter_sheet_items(model, "Financial_model"), _FM_SECTION_SHIFT_ROWS["income_statement"], fm_delta)

    fm_base_rows = {
        "income_statement": fm_rows["income_statement"],
        "margins": fm_rows["margins"] + fm_delta,
        "growth_rates": fm_rows["growth_rates"] + fm_delta * 2,
    }

    assumptions_section = _get_section(model, "Assumptions", "revenue_drivers")
    income_statement_section = _get_section(model, "Financial_model", "income_statement")
    margins_section = _get_section(model, "Financial_model", "margins")
    growth_rates_section = _get_section(model, "Financial_model", "growth_rates")

    assumptions_items: List[LineItem] = []
    income_statement_items: List[LineItem] = []
    margin_items: List[LineItem] = []
    growth_items: List[LineItem] = []

    for index, segment in enumerate(profile.segments, start=1):
        base_row = assumptions_rows["start"] + (index - 1) * 6
        fm_target_rows = {
            "income_statement": fm_base_rows["income_statement"] + index - 1,
            "margins": fm_base_rows["margins"] + index - 1,
            "growth_rates": fm_base_rows["growth_rates"] + index - 1,
        }
        built_assumptions, built_fm = _build_canonical_segment(
            model,
            prototypes,
            seg_index=index,
            seg_info=segment,
            assumptions_base_row=base_row,
            fm_rows=fm_target_rows,
        )
        assumptions_items.extend(built_assumptions)
        income_statement_items.append(built_fm[0])
        margin_items.append(built_fm[1])
        growth_items.append(built_fm[2])

    assumptions_section.line_items.extend(assumptions_items)
    income_statement_section.line_items.extend(income_statement_items)
    margins_section.line_items.extend(margin_items)
    growth_rates_section.line_items.extend(growth_items)

    assumptions_section.line_items.sort(key=lambda item: (int(item.row), item.id))
    income_statement_section.line_items.sort(key=lambda item: (int(item.row), item.id))
    margins_section.line_items.sort(key=lambda item: (int(item.row), item.id))
    growth_rates_section.line_items.sort(key=lambda item: (int(item.row), item.id))

    model.build_index()
    _update_total_revenue_formulas(model, len(profile.segments))
    label_item = model.get_item("tpl.a.scenario_tables.scenario_volume_growth_label")
    seg_name = profile.segments[0].name
    label_item.label = "Revenue growth %" if seg_name == "Total Revenue" else f"{seg_name} revenue growth %"
    _assert_rows_unique(model)
    _assert_no_duplicate_ids(model)

    return ExpandResult(
        segments_created=len(profile.segments),
        items_added=len(profile.segments) * 9,
        items_relabeled=len(profile.segments) * 6,
    )


def populate_segment_historicals(
    model: FinancialModel,
    profile: SegmentProfile,
    historical_periods: List[int],
) -> int:
    """Write discovered segment revenue values into FM segment revenue rows."""

    if not model._index:
        model.build_index()

    periods = {int(period) for period in historical_periods}
    writes = 0
    for segment in profile.segments:
        revenue_values = segment_revenue_values(segment)
        if not segment.item_ids or not revenue_values:
            continue
        item = model.get_item(segment.item_ids["revenue_fm"])
        for year, value in revenue_values.items():
            if int(year) not in periods:
                continue
            _set_imported_value(item, int(year), float(value))
            writes += 1
    return writes


def prune_segment_formula_periods(model: FinancialModel, profile: SegmentProfile) -> None:
    """Prune segment-derived formulas for partial historical coverage."""

    if not profile.segments or not model._index:
        if not model._index:
            model.build_index()
        if not profile.segments:
            return

    historical_periods = [int(period) for period in model.time_structure.historical_periods]
    projection_periods = [int(period) for period in model.time_structure.projection_periods]
    if not historical_periods:
        return

    last_historical = historical_periods[-1]
    all_segment_roles = list(SEGMENT_ITEM_ID_ROLES)

    for segment in profile.segments:
        revenue_values = segment_revenue_values(segment)
        if not segment.item_ids or not revenue_values:
            continue

        available_years = {int(year) for year in revenue_values}
        has_projection_base = last_historical in available_years

        for role in all_segment_roles:
            item_id = segment.item_ids.get(role)
            if not item_id:
                continue
            item = model.get_item(item_id)
            if item.formula_periods is None:
                continue

            kept: List[int] = []
            for period in list(item.formula_periods):
                year = int(period)
                if year in projection_periods:
                    if has_projection_base:
                        kept.append(year)
                    continue

                if role in {"margin_fm", "revenue"}:
                    if year in available_years:
                        kept.append(year)
                    continue

                if role in {"growth", "growth_fm"}:
                    if year in available_years and (year - 1) in available_years:
                        kept.append(year)
                    continue

                kept.append(year)

            item.formula_periods = kept


def _normalize_qname(qname: str) -> str:
    if not qname:
        return ""
    text = str(qname)
    if ":" in text:
        return text.split(":", 1)[1]
    return text


def _normalize_scale(value: float, scale: Optional[str]) -> float:
    return normalize_edgar_value(value, scale, concept_id="")


def _filter_revenue_facts(facts: List[Dict]) -> tuple[List[Dict], List[Dict]]:
    consolidated: List[Dict] = []
    dimensional: List[Dict] = []

    for fact in facts:
        tag = _normalize_qname(str(fact.get("tag") or fact.get("metric_tag") or ""))
        if tag not in _REVENUE_TAGS:
            continue
        value = _fact_value(fact)
        if value is None:
            continue
        axis_key = str(fact.get("axis_key") or "__NONE__")
        if axis_key == "__NONE__":
            consolidated.append(fact)
        else:
            dimensional.append(fact)

    return consolidated, dimensional


def _dedupe_member_facts(
    facts: List[Dict],
    tag_priority: List[str],
) -> Dict[tuple[str, int], Dict]:
    priority_index = {tag: index for index, tag in enumerate(tag_priority)}
    deduped: Dict[tuple[str, int], Dict] = {}

    for fact in facts:
        dimension = _single_segment_dimension(fact)
        if dimension is None:
            continue

        member = str(dimension["member"])
        year = int(fact["_segment_year"])
        key = (member, year)
        existing = deduped.get(key)
        current_priority = priority_index.get(_normalize_qname(str(fact.get("tag") or "")), len(tag_priority))
        if existing is None:
            deduped[key] = fact
            continue

        existing_priority = priority_index.get(_normalize_qname(str(existing.get("tag") or "")), len(tag_priority))
        if current_priority < existing_priority:
            deduped[key] = fact

    return deduped


def _try_axis_decomposition(
    dimensional_facts: List[Dict],
    consolidated_values: Dict[int, float],
    axis: str,
    tolerance: float = 0.05,
) -> Optional[List[SegmentInfo]]:
    axis_facts = []
    for fact in dimensional_facts:
        dimension = _single_segment_dimension(fact)
        if dimension is None or _normalize_qname(dimension["axis"]) != axis:
            continue
        axis_facts.append(fact)

    if not axis_facts:
        return None

    deduped = _dedupe_member_facts(axis_facts, REVENUE_TAGS_PRIORITY)
    if not deduped:
        return None

    current_year = max(int(year) for year in consolidated_values)
    members_by_year: Dict[int, set[str]] = defaultdict(set)
    revenue_by_member: Dict[str, Dict[int, float]] = defaultdict(dict)
    observations_by_member: Dict[str, Dict[int, SegmentRevenueObservation]] = defaultdict(dict)
    labels_by_member: Dict[str, str] = {}

    for fact in deduped.values():
        dimension = _single_segment_dimension(fact)
        if dimension is None:
            continue

        member = str(dimension["member"])
        member_suffix = _normalize_qname(member)
        if member_suffix in EXCLUDED_MEMBER_SUFFIXES:
            continue

        year = int(fact["_segment_year"])
        value = _normalize_scale(_fact_value(fact), fact.get("scale"))
        revenue_by_member[member][year] = value
        labels_by_member.setdefault(member, _segment_label(dimension))
        observations_by_member[member][year] = _observation_from_fact(
            fact,
            dimension=dimension,
            year=year,
            value=value,
        )
        members_by_year[year].add(member)

    current_members = members_by_year.get(current_year, set())
    if not current_members:
        return None

    rollup_members = _detect_rollup_members(revenue_by_member, current_members, rollup_tolerance=0.01)
    if not _rollup_exclusion_preserves_total(
        revenue_by_member,
        current_members,
        rollup_members,
        consolidated_values,
        tolerance=tolerance,
    ):
        rollup_members = set()
    current_members = {member for member in current_members if member not in rollup_members}

    named_segments: List[SegmentInfo] = []
    other_values: Dict[int, float] = defaultdict(float)

    for member, values in revenue_by_member.items():
        if member in rollup_members:
            continue
        if member not in current_members:
            for year, value in values.items():
                other_values[int(year)] += float(value)
            continue

        share = _materiality_share(values, consolidated_values)
        if share < MATERIALITY_THRESHOLD:
            for year, value in values.items():
                other_values[int(year)] += float(value)
            continue

        named_segments.append(
            SegmentInfo(
                name=labels_by_member.get(member, _normalize_qname(member)),
                edgar_member=member,
                revenue_observations=_annotate_revenue_comparability(
                    observations_by_member.get(member, {})
                ),
            )
        )

    member_sum_by_year: Dict[int, float] = defaultdict(float)
    for segment in named_segments:
        for year, value in segment_revenue_values(segment).items():
            member_sum_by_year[int(year)] += float(value)
    for year, value in other_values.items():
        member_sum_by_year[int(year)] += float(value)

    reconciliation_values: Dict[int, float] = {}
    for year, consolidated_value in consolidated_values.items():
        member_sum = float(member_sum_by_year.get(int(year), 0.0))
        diff = float(consolidated_value) - member_sum
        if consolidated_value:
            if abs(diff) / abs(consolidated_value) > tolerance:
                return None
        elif abs(diff) > 1e-6:
            return None
        if abs(diff) > 0.01:
            reconciliation_values[int(year)] = diff

    if any(abs(value) > 1e-9 for value in other_values.values()):
        named_segments.append(
            SegmentInfo(
                name="Other",
                revenue_observations=_derived_revenue_observations(
                    other_values,
                    source="derived_other",
                    note="Aggregated from immaterial or non-current segment members.",
                ),
            )
        )

    if reconciliation_values:
        named_segments.append(
            SegmentInfo(
                name="Reconciliation",
                revenue_observations=_derived_revenue_observations(
                    reconciliation_values,
                    source="reconciliation",
                    note="Consolidated revenue less discovered segment member sum.",
                ),
            )
        )

    return named_segments or None


def _detect_rollup_members(
    revenue_by_member: Dict[str, Dict[int, float]],
    current_members: Sequence[str],
    rollup_tolerance: float = 0.01,
) -> set[str]:
    """Identify current-year members that are rollups of other current-year members."""

    active_members = [member for member in current_members if revenue_by_member.get(member)]
    if len(active_members) < 3:
        return set()

    current_year = max(
        year
        for member in active_members
        for year in revenue_by_member.get(member, {})
    )
    current_values = {
        member: float(revenue_by_member[member][current_year])
        for member in active_members
        if current_year in revenue_by_member.get(member, {})
    }
    if len(current_values) < 3:
        return set()

    rollup_members: set[str] = set()
    tolerance_for = lambda target: max(abs(float(target)) * float(rollup_tolerance), 0.01)

    for candidate, target in current_values.items():
        if target <= 0:
            continue

        child_candidates = sorted(
            (
                (member, value)
                for member, value in current_values.items()
                if member != candidate and value > 0
            ),
            key=lambda entry: entry[1],
            reverse=True,
        )
        if len(child_candidates) < 2:
            continue

        suffix_sums = [0.0] * (len(child_candidates) + 1)
        for index in range(len(child_candidates) - 1, -1, -1):
            suffix_sums[index] = suffix_sums[index + 1] + child_candidates[index][1]

        tolerance = tolerance_for(target)

        def backtrack(start: int, remaining: float, chosen: int) -> bool:
            if chosen >= 2 and abs(remaining) <= tolerance:
                return True
            if start >= len(child_candidates):
                return False
            if remaining < -tolerance:
                return False
            if remaining > suffix_sums[start] + tolerance:
                return False

            for index in range(start, len(child_candidates)):
                value = child_candidates[index][1]
                if value > remaining + tolerance:
                    continue
                if backtrack(index + 1, remaining - value, chosen + 1):
                    return True
            return False

        if backtrack(0, target, 0):
            rollup_members.add(candidate)

    return rollup_members


def _rollup_exclusion_preserves_total(
    revenue_by_member: Dict[str, Dict[int, float]],
    current_members: Sequence[str],
    rollup_members: set[str],
    consolidated_values: Dict[int, float],
    *,
    tolerance: float,
) -> bool:
    """Only trust detected rollups when removing them still reconciles the axis."""

    if not rollup_members:
        return True
    if not consolidated_values:
        return False

    current_year = max(int(year) for year in consolidated_values)
    kept_members = [member for member in current_members if member not in rollup_members]
    if not kept_members:
        return False

    member_sum = sum(
        float(revenue_by_member.get(member, {}).get(current_year, 0.0))
        for member in kept_members
    )
    consolidated_value = float(consolidated_values.get(current_year, 0.0))
    diff = consolidated_value - member_sum
    if consolidated_value:
        return abs(diff) / abs(consolidated_value) <= float(tolerance)
    return abs(diff) <= 1e-6


def _remove_existing_segment_items(model: FinancialModel) -> tuple[Dict[str, int], Dict[str, int]]:
    deleted_ids = set(_ASSUMPTIONS_SEGMENT_IDS) | set(_FM_SEGMENT_IDS)

    _rewire_scenario_table_refs(model, _SCENARIO_TABLE_OLD_GROWTH_ID, _SCENARIO_TABLE_NEW_GROWTH_ID)

    assumptions_section = _get_section(model, "Assumptions", "revenue_drivers")
    income_statement_section = _get_section(model, "Financial_model", "income_statement")
    margins_section = _get_section(model, "Financial_model", "margins")
    growth_rates_section = _get_section(model, "Financial_model", "growth_rates")

    assumptions_section.line_items = [
        item for item in assumptions_section.line_items if item.id not in _ASSUMPTIONS_SEGMENT_IDS
    ]
    income_statement_section.line_items = [
        item for item in income_statement_section.line_items if item.id not in _FM_SEGMENT_IDS
    ]
    margins_section.line_items = [
        item for item in margins_section.line_items if item.id not in _FM_SEGMENT_IDS
    ]
    growth_rates_section.line_items = [
        item for item in growth_rates_section.line_items if item.id not in _FM_SEGMENT_IDS
    ]

    for _sheet_name, section_id, item in _iter_items_with_section(model):
        if section_id == "scenario_tables":
            continue
        if item.id in _REBUILD_LATER_IDS:
            continue
        if _repair_deleted_refs_in_item(item, deleted_ids):
            logging.warning("Rewired dangling segment refs on '%s' to carry-forward fallback", item.id)

    return (
        {"start": _ASSUMPTIONS_SEGMENT_START_ROW, "freed": 18},
        {
            "income_statement": _FM_SECTION_START_ROWS["income_statement"],
            "margins": _FM_SECTION_START_ROWS["margins"],
            "growth_rates": _FM_SECTION_START_ROWS["growth_rates"],
            "freed": 2,
        },
    )


def _build_canonical_segment(
    model: FinancialModel,
    prototypes: Dict[str, LineItem],
    *,
    seg_index: int,
    seg_info: SegmentInfo,
    assumptions_base_row: int,
    fm_rows: Dict[str, int],
) -> tuple[List[LineItem], List[LineItem]]:
    historical_periods = list(model.time_structure.historical_periods or model.time_structure.historical_years)
    projection_periods = list(model.time_structure.projection_periods or model.time_structure.projection_years)
    all_periods = [int(period) for period in historical_periods + projection_periods]
    fallback_single = seg_info.revenue_observations is None

    volume_driver_id = f"tpl.a.revenue_drivers.business_segment_{seg_index}_volume_driver_1"
    volume_growth_id = f"tpl.a.revenue_drivers.business_segment_{seg_index}_volume_growth"
    price_driver_id = f"tpl.a.revenue_drivers.business_segment_{seg_index}_price_driver_1"
    price_growth_id = f"tpl.a.revenue_drivers.business_segment_{seg_index}_price_growth"
    revenue_id = f"tpl.a.revenue_drivers.business_segment_{seg_index}_revenue"
    growth_id = f"tpl.a.revenue_drivers.business_segment_{seg_index}_growth"
    revenue_fm_id = f"tpl.fm.income_statement.business_segment_{seg_index}_revenue"
    margin_fm_id = f"tpl.fm.margins.business_segment_{seg_index}_pct_revenue"
    growth_fm_id = f"tpl.fm.growth_rates.business_segment_{seg_index}_growth"

    volume_label = seg_info.volume_label or f"{seg_info.name} Volume"
    price_label = seg_info.price_label or f"{seg_info.name} Price"
    revenue_label = seg_info.name
    fm_label = f" {seg_info.name}"

    volume_driver = prototypes["volume_driver"].model_copy(
        deep=True,
        update={
            "id": volume_driver_id,
            "label": volume_label,
            "row": assumptions_base_row,
            "historical": None,
            "projected": _growth_formula(volume_driver_id, volume_growth_id),
            "formula_periods": list(projection_periods),
            "overrides": None,
            "values": None,
            "template_token": None,
            "build_notes": "Optional segment KPI slot. Leave blank unless the analyst decomposes revenue into volume and price drivers.",
        },
    )
    volume_growth = prototypes["volume_growth"].model_copy(
        deep=True,
        update={
            "id": volume_growth_id,
            "label": " y/y % chg.",
            "row": assumptions_base_row + 1,
            "historical": None,
            "projected": _carry_forward_formula(volume_growth_id),
            "formula_periods": list(projection_periods),
            "overrides": None,
            "values": None,
            "template_token": None,
            "build_notes": None,
        },
    )
    price_driver = prototypes["price_driver"].model_copy(
        deep=True,
        update={
            "id": price_driver_id,
            "label": price_label,
            "row": assumptions_base_row + 2,
            "historical": None,
            "projected": _growth_formula(price_driver_id, price_growth_id),
            "formula_periods": list(projection_periods),
            "overrides": None,
            "values": None,
            "template_token": None,
            "build_notes": "Optional segment KPI slot. Leave blank unless the analyst decomposes revenue into volume and price drivers.",
        },
    )
    price_growth = prototypes["price_growth"].model_copy(
        deep=True,
        update={
            "id": price_growth_id,
            "label": " y/y % chg.",
            "row": assumptions_base_row + 3,
            "historical": None,
            "projected": _carry_forward_formula(price_growth_id),
            "formula_periods": list(projection_periods),
            "overrides": None,
            "values": None,
            "template_token": None,
            "build_notes": None,
        },
    )
    revenue = prototypes["revenue"].model_copy(
        deep=True,
        update={
            "id": revenue_id,
            "label": revenue_label,
            "row": assumptions_base_row + 4,
            "historical": _ref_formula(revenue_fm_id),
            "projected": _growth_formula(revenue_id, growth_id),
            "formula_periods": list(all_periods),
            "overrides": None,
            "values": None,
            "template_token": None,
            "build_notes": None,
        },
    )
    growth = prototypes["growth"].model_copy(
        deep=True,
        update={
            "id": growth_id,
            "label": " y/y % chg.",
            "row": assumptions_base_row + 5,
            "historical": _yoy_formula(revenue_id),
            "projected": _offset_scenario_formula() if seg_index == 1 else _carry_forward_formula(growth_id),
            "formula_periods": list(all_periods),
            "overrides": None,
            "values": None,
            "template_token": None,
            "build_notes": "Primary segment growth is scenario-linked; other segment growth rows carry forward unless the analyst overrides them.",
        },
    )

    revenue_fm = prototypes["revenue_fm"].model_copy(
        deep=True,
        update={
            "id": revenue_fm_id,
            "label": fm_label,
            "row": fm_rows["income_statement"],
            "historical": _ref_formula("tpl.fm.income_statement.total_revenue") if fallback_single else None,
            "projected": _ref_formula(revenue_id),
            "data_concept_id": None,
            "formula_periods": list(all_periods if fallback_single else projection_periods),
            "overrides": None,
            "values": None,
            "template_token": None,
            "build_notes": None,
        },
    )
    margin_fm = prototypes["margin_fm"].model_copy(
        deep=True,
        update={
            "id": margin_fm_id,
            "label": fm_label,
            "row": fm_rows["margins"],
            "historical": _ratio_formula(revenue_fm_id, "tpl.fm.income_statement.total_revenue"),
            "projected": _ratio_formula(revenue_fm_id, "tpl.fm.income_statement.total_revenue"),
            "formula_periods": list(all_periods),
            "overrides": None,
            "values": None,
            "template_token": None,
            "build_notes": None,
        },
    )
    growth_fm = prototypes["growth_fm"].model_copy(
        deep=True,
        update={
            "id": growth_fm_id,
            "label": fm_label,
            "row": fm_rows["growth_rates"],
            "historical": _yoy_formula(revenue_fm_id),
            "projected": _yoy_formula(revenue_fm_id),
            "formula_periods": list(all_periods),
            "overrides": None,
            "values": None,
            "template_token": None,
            "build_notes": None,
        },
    )

    seg_info.item_ids = {
        "volume_driver": volume_driver_id,
        "volume_growth": volume_growth_id,
        "price_driver": price_driver_id,
        "price_growth": price_growth_id,
        "revenue": revenue_id,
        "growth": growth_id,
        "revenue_fm": revenue_fm_id,
        "margin_fm": margin_fm_id,
        "growth_fm": growth_fm_id,
    }

    return [volume_driver, volume_growth, price_driver, price_growth, revenue, growth], [
        revenue_fm,
        margin_fm,
        growth_fm,
    ]


def _shift_rows(items: Iterable[LineItem], at_or_after: int, delta: int) -> None:
    if not delta:
        return
    for item in items:
        if int(item.row) >= int(at_or_after):
            item.row = int(item.row) + int(delta)


def _update_total_revenue_formulas(model: FinancialModel, n_segments: int) -> None:
    assumptions_total = model.get_item("tpl.a.revenue_drivers.total_revenue")
    fm_total = model.get_item("tpl.fm.income_statement.total_revenue")
    historical_periods = list(model.time_structure.historical_periods or model.time_structure.historical_years)
    projection_periods = list(model.time_structure.projection_periods or model.time_structure.projection_years)
    all_periods = [int(period) for period in historical_periods + projection_periods]

    assumptions_segment_refs = [
        LineItemRef(id=f"tpl.a.revenue_drivers.business_segment_{index}_revenue")
        for index in range(1, n_segments + 1)
    ]
    fm_segment_refs = [
        LineItemRef(id=f"tpl.fm.income_statement.business_segment_{index}_revenue")
        for index in range(1, n_segments + 1)
    ]

    assumptions_total.projected = _sum_or_ref_formula(assumptions_segment_refs)
    fallback_single = (
        n_segments == 1
        and _ref_source_id(model.get_item(fm_segment_refs[0].id).historical)
        == "tpl.fm.income_statement.total_revenue"
    )
    fm_total.historical = None if fallback_single else _sum_or_ref_formula(fm_segment_refs)
    fm_total.projected = _sum_or_ref_formula(fm_segment_refs)
    fm_total.formula_periods = list(all_periods)


def _assert_rows_unique(model: FinancialModel) -> None:
    for sheet_name, sheet in model.sheets.items():
        if sheet.sheet_type in {SheetType.valuation, SheetType.scenarios}:
            continue
        rows = [int(item.row) for section in sheet.sections for item in section.line_items]
        duplicates = {row for row in rows if rows.count(row) > 1}
        if duplicates:
            raise ValueError(f"Duplicate rows detected in sheet '{sheet_name}': {sorted(duplicates)}")


def _assert_no_duplicate_ids(model: FinancialModel) -> None:
    model.build_index()


def _extract_fact_rows(payload: Dict) -> List[Dict]:
    rows: List[Dict] = []

    def walk(obj) -> None:
        if isinstance(obj, dict):
            if "tag" in obj and (
                "current_period_value" in obj or "visual_current_value" in obj or "value" in obj
            ):
                rows.append(obj)
                return
            for value in obj.values():
                walk(value)
            return
        if isinstance(obj, list):
            for value in obj:
                walk(value)

    walk(payload)
    return rows


def _consolidated_values(facts: Sequence[Dict]) -> Dict[int, float]:
    priority_index = {tag: index for index, tag in enumerate(REVENUE_TAGS_PRIORITY)}
    best_by_year: Dict[int, Dict] = {}

    for fact in facts:
        year = int(fact["_segment_year"])
        existing = best_by_year.get(year)
        if existing is None:
            best_by_year[year] = fact
            continue
        current_priority = priority_index.get(_normalize_qname(str(fact.get("tag") or "")), len(REVENUE_TAGS_PRIORITY))
        existing_priority = priority_index.get(_normalize_qname(str(existing.get("tag") or "")), len(REVENUE_TAGS_PRIORITY))
        if current_priority < existing_priority:
            best_by_year[year] = fact

    values: Dict[int, float] = {}
    for year, fact in best_by_year.items():
        value = _fact_value(fact)
        if value is None:
            continue
        values[int(year)] = _normalize_scale(value, fact.get("scale"))
    return dict(sorted(values.items()))


def _single_segment_dimension(fact: Dict) -> Optional[Dict[str, str]]:
    dimensions = _fact_dimensions(fact)
    relevant = [
        dimension
        for dimension in dimensions
        if _normalize_qname(dimension["axis"]) in SEGMENT_AXES_PRIORITY
    ]
    if len(relevant) != 1:
        return None
    return relevant[0]


def _fact_dimensions(fact: Dict) -> List[Dict[str, str]]:
    dimensions: List[Dict[str, str]] = []
    raw_dimensions = fact.get("dimensions")
    if isinstance(raw_dimensions, list):
        for entry in raw_dimensions:
            if not isinstance(entry, dict):
                continue
            axis = _normalize_qname(str(entry.get("axis") or entry.get("axis_key") or ""))
            member = str(entry.get("member") or "")
            if not axis or not member:
                continue
            dimensions.append(
                {
                    "axis": axis,
                    "member": member,
                    "member_label": str(entry.get("member_label") or _pretty_member_label(member)),
                }
            )
        if dimensions:
            return dimensions

    axis_key = str(fact.get("axis_key") or "")
    if not axis_key or axis_key == "__NONE__":
        return dimensions

    for part in re.split(r"[|;]", axis_key):
        if "=" not in part:
            continue
        axis_name, member = part.split("=", 1)
        axis = _normalize_qname(axis_name.strip())
        member = member.strip()
        if not axis or not member:
            continue
        dimensions.append(
            {
                "axis": axis,
                "member": member,
                "member_label": _pretty_member_label(member),
            }
        )
    return dimensions


def _observation_from_fact(
    fact: Dict,
    *,
    dimension: Dict[str, str],
    year: int,
    value: float,
) -> SegmentRevenueObservation:
    return SegmentRevenueObservation(
        fiscal_year=int(year),
        value=float(value),
        source="edgar_fact",
        tag=_clean_optional_string(fact.get("tag") or fact.get("metric_tag")),
        scale=_clean_optional_string(fact.get("scale")),
        axis=_clean_optional_string(dimension.get("axis")),
        member=_clean_optional_string(dimension.get("member")),
        member_label=_clean_optional_string(dimension.get("member_label")),
        source_filing_accession=_clean_optional_string(
            fact.get("source_filing_accession")
            or fact.get("filing_accession")
            or fact.get("accession")
            or fact.get("adsh")
        ),
        source_form=_clean_optional_string(fact.get("source_form") or fact.get("form")),
        filed_at=_clean_optional_string(fact.get("filed_at") or fact.get("filed")),
        period_end=_clean_optional_string(
            fact.get("period_end")
            or fact.get("reported_period_end")
            or fact.get("end_date")
            or fact.get("end")
        ),
        basis_key=_clean_optional_string(
            fact.get("segment_basis_key")
            or fact.get("basis_key")
            or fact.get("recast_basis")
            or fact.get("presentation_basis")
            or fact.get("statement_basis")
        ),
    )


def _annotate_revenue_comparability(
    observations: Dict[int, SegmentRevenueObservation],
) -> Dict[int, SegmentRevenueObservation]:
    annotated: Dict[int, SegmentRevenueObservation] = {}
    prior: SegmentRevenueObservation | None = None
    for year, observation in sorted((int(year), obs) for year, obs in observations.items()):
        if prior is None:
            comparable = "not_applicable"
            note = observation.comparability_note
        elif prior.basis_key and observation.basis_key:
            comparable = "comparable" if prior.basis_key == observation.basis_key else "not_comparable"
            note = (
                observation.comparability_note
                if comparable == "comparable"
                else f"segment basis changed from {prior.basis_key!r} to {observation.basis_key!r}"
            )
        else:
            comparable = "unknown"
            note = observation.comparability_note or "segment basis provenance is missing for adjacent-year comparability"
        updated = observation.model_copy(
            update={
                "comparable_with_prior": comparable,
                "comparability_note": note,
            }
        )
        annotated[int(year)] = updated
        prior = updated
    return annotated


def _clean_optional_string(value: object | None) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _materiality_share(values: Dict[int, float], consolidated_values: Dict[int, float]) -> float:
    numerator = sum(float(values.get(year, 0.0)) for year in consolidated_values)
    denominator = sum(abs(float(value)) for value in consolidated_values.values())
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _segment_label(dimension: Dict[str, str]) -> str:
    label = str(dimension.get("member_label") or "").strip()
    if label:
        return label
    return _pretty_member_label(str(dimension.get("member") or ""))


def _pretty_member_label(member: str) -> str:
    label = _normalize_qname(member)
    label = re.sub(r"Member$", "", label)
    label = re.sub(r"(?<!^)([A-Z])", r" \1", label)
    return label.strip() or member


def _segment_sort_key(segment: SegmentInfo) -> tuple[float, str]:
    values = list(segment_revenue_values(segment).values())
    average = sum(float(value) for value in values) / len(values) if values else float("-inf")
    return (average, segment.name)


def _repair_deleted_refs_in_item(item: LineItem, deleted_ids: set[str]) -> bool:
    changed = False

    for attr in ("historical", "projected"):
        spec = getattr(item, attr)
        if spec is None:
            continue
        new_params, updated = _rewrite_refs(
            spec.params,
            lambda ref: _carry_forward_ref(item.id) if ref.get("id") in deleted_ids else None,
        )
        if updated:
            spec.params = new_params
            changed = True

    if item.overrides:
        for period, spec in item.overrides.items():
            new_params, updated = _rewrite_refs(
                spec.params,
                lambda ref: _carry_forward_ref(item.id) if ref.get("id") in deleted_ids else None,
            )
            if updated:
                item.overrides[period] = spec.model_copy(update={"params": new_params})
                changed = True

    return changed


def _rewire_scenario_table_refs(model: FinancialModel, old_id: str, new_id: str) -> None:
    scenario_section = _get_section(model, "Assumptions", "scenario_tables")
    for item in scenario_section.line_items:
        for attr in ("historical", "projected"):
            spec = getattr(item, attr)
            if spec is None:
                continue
            new_params, updated = _rewrite_refs(
                spec.params,
                lambda ref: _line_ref_dict(new_id, int(ref.get("t", 0))) if ref.get("id") == old_id else None,
            )
            if updated:
                spec.params = new_params

        if item.overrides:
            kept_overrides: Dict[int, FormulaSpec] = {}
            for period, spec in item.overrides.items():
                new_params, updated = _rewrite_refs(
                    spec.params,
                    lambda ref: _line_ref_dict(new_id, int(ref.get("t", 0))) if ref.get("id") == old_id else None,
                )
                new_spec = spec.model_copy(update={"params": new_params}) if updated else spec
                if _ref_target_id(new_spec) == new_id:
                    continue
                kept_overrides[int(period)] = new_spec
            item.overrides = kept_overrides or None


def _ref_target_id(spec: FormulaSpec) -> Optional[str]:
    if spec.type != FormulaType.ref:
        return None

    source = spec.params.get("source")
    if isinstance(source, LineItemRef):
        return source.id
    if isinstance(source, dict):
        return source.get("id")
    return None


def _rewrite_refs(obj, replacer):
    if isinstance(obj, LineItemRef):
        replacement = replacer({"id": obj.id, "t": obj.t, "resolved": obj.resolved})
        if replacement is not None:
            return replacement, True
        return obj, False

    if isinstance(obj, dict):
        if "id" in obj and set(obj.keys()) >= {"id"}:
            replacement = replacer(obj)
            if replacement is not None:
                return replacement, True
            return obj, False

        changed = False
        new_obj = {}
        for key, value in obj.items():
            new_value, updated = _rewrite_refs(value, replacer)
            new_obj[key] = new_value
            changed = changed or updated
        return new_obj, changed

    if isinstance(obj, list):
        changed = False
        new_list = []
        for value in obj:
            new_value, updated = _rewrite_refs(value, replacer)
            new_list.append(new_value)
            changed = changed or updated
        return new_list, changed

    if isinstance(obj, tuple):
        changed = False
        new_values = []
        for value in obj:
            new_value, updated = _rewrite_refs(value, replacer)
            new_values.append(new_value)
            changed = changed or updated
        return tuple(new_values), changed

    return obj, False


def _carry_forward_ref(item_id: str) -> Dict[str, object]:
    return _line_ref_dict(item_id, -1)


def _line_ref_dict(item_id: str, t: int = 0) -> Dict[str, object]:
    return {"id": item_id, "t": int(t), "resolved": True}


def _ref_source_id(spec: FormulaSpec | None) -> str | None:
    if spec is None or spec.type is not FormulaType.ref:
        return None
    source = (spec.params or {}).get("source")
    if isinstance(source, LineItemRef):
        return source.id
    if isinstance(source, dict) and isinstance(source.get("id"), str):
        return str(source["id"])
    return None


def _ref_formula(item_id: str, t: int = 0) -> FormulaSpec:
    return FormulaSpec(type=FormulaType.ref, params={"source": LineItemRef(id=item_id, t=int(t))})


def _carry_forward_formula(item_id: str) -> FormulaSpec:
    return _ref_formula(item_id, t=-1)


def _growth_formula(base_id: str, rate_id: str) -> FormulaSpec:
    return FormulaSpec(
        type=FormulaType.growth,
        params={
            "base": LineItemRef(id=base_id, t=-1),
            "rate": LineItemRef(id=rate_id),
        },
    )


def _yoy_formula(item_id: str) -> FormulaSpec:
    return FormulaSpec(
        type=FormulaType.ratio,
        subtype="yoy_growth",
        params={
            "numerator": LineItemRef(id=item_id),
            "denominator": LineItemRef(id=item_id, t=-1),
            "subtract_one": True,
        },
    )


def _ratio_formula(numerator_id: str, denominator_id: str) -> FormulaSpec:
    return FormulaSpec(
        type=FormulaType.ratio,
        params={
            "numerator": LineItemRef(id=numerator_id),
            "denominator": LineItemRef(id=denominator_id),
        },
    )


def _sum_or_ref_formula(refs: List[LineItemRef]) -> FormulaSpec:
    if len(refs) == 1:
        ref = refs[0]
        return FormulaSpec(type=FormulaType.ref, params={"source": ref})
    return FormulaSpec(type=FormulaType.arithmetic, params={"operands": ["+", *refs]})


def _offset_scenario_formula(
    *,
    anchor_id: str = _SCENARIO_VOLUME_GROWTH_LABEL_ID,
    selector_id: str = _SCENARIO_SELECTOR_ID,
) -> FormulaSpec:
    return FormulaSpec(
        type=FormulaType.valuation,
        subtype="offset_scenario",
        params={
            "anchor": LineItemRef(id=anchor_id),
            "selector": LineItemRef(id=selector_id),
            "column_offset_mode": _COLUMN_OFFSET_MODE_PERIOD_RELATIVE,
        },
    )


def _fact_value(fact: Dict):
    value = fact.get("current_period_value")
    if value is None:
        value = fact.get("visual_current_value")
    if value is None:
        value = fact.get("value")
    return value


def _get_section(model: FinancialModel, sheet_name: str, section_id: str) -> Section:
    sheet = model.sheets[sheet_name]
    for section in sheet.sections:
        if section.id == section_id:
            return section
    raise KeyError((sheet_name, section_id))


def _iter_sheet_items(model: FinancialModel, sheet_name: str) -> Iterable[LineItem]:
    for section in model.sheets[sheet_name].sections:
        for item in section.line_items:
            yield item


def _iter_items_with_section(model: FinancialModel):
    for sheet_name, sheet in model.sheets.items():
        for section in sheet.sections:
            for item in section.line_items:
                yield sheet_name, section.id, item


def _set_imported_value(
    item: LineItem,
    year: int,
    value: float,
    provenance: ValueProvenance = ValueProvenance.imported_edgar,
) -> None:
    if item.values is None:
        item.values = ValueSeries()
    item.values.values[int(year)] = ValueCell(
        period=int(year),
        value=float(value),
        provenance=provenance,
    )


__all__ = [
    "EdgarFinancialsFetcher",
    "ExpandResult",
    "EXCLUDED_MEMBER_SUFFIXES",
    "MATERIALITY_THRESHOLD",
    "MultiAxisResult",
    "REVENUE_TAGS_PRIORITY",
    "SEGMENT_AXES_PRIORITY",
    "SEGMENT_ITEM_ID_ROLES",
    "SegmentInfo",
    "SegmentProfile",
    "apply_segment_overrides",
    "discover_all_axes",
    "discover_segments",
    "discover_segments_with_payloads",
    "expand_segments",
    "populate_segment_historicals",
    "prune_segment_formula_periods",
]
