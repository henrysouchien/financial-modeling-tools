"""Dynamic revenue segment discovery, expansion, and population helpers."""

from __future__ import annotations

from collections import defaultdict
import concurrent.futures
import logging
from typing import Callable, Dict, List, Optional, Sequence

from .models import (
    FinancialModel,
    LineItem,
    LineItemRef,
)
from .model_build_context import SegmentRevenueObservation
from .segment_fact_helpers import (
    REVENUE_TAGS_PRIORITY as REVENUE_TAGS_PRIORITY,
    SEGMENT_AXES_PRIORITY as SEGMENT_AXES_PRIORITY,
    _REVENUE_TAGS as _REVENUE_TAGS,
    _annotate_revenue_comparability as _annotate_revenue_comparability,
    _clean_optional_string as _clean_optional_string,
    _consolidated_values as _consolidated_values,
    _largest_consolidated_values as _largest_consolidated_values,
    _dedupe_member_facts as _dedupe_member_facts,
    _extract_fact_rows as _extract_fact_rows,
    _fact_dimensions as _fact_dimensions,
    _fact_value as _fact_value,
    _filter_revenue_facts as _filter_revenue_facts,
    _materiality_share as _materiality_share,
    _normalize_qname as _normalize_qname,
    _normalize_scale as _normalize_scale,
    _observation_from_fact as _observation_from_fact,
    _pretty_member_label as _pretty_member_label,
    _segment_label as _segment_label,
    _single_segment_dimension as _single_segment_dimension,
)
from .segment_formula_helpers import (
    _COLUMN_OFFSET_MODE_PERIOD_RELATIVE as _COLUMN_OFFSET_MODE_PERIOD_RELATIVE,
    _SCENARIO_SELECTOR_ID as _SCENARIO_SELECTOR_ID,
    _SCENARIO_VOLUME_GROWTH_LABEL_ID as _SCENARIO_VOLUME_GROWTH_LABEL_ID,
    _carry_forward_formula as _carry_forward_formula,
    _carry_forward_ref as _carry_forward_ref,
    _growth_formula as _growth_formula,
    _line_ref_dict as _line_ref_dict,
    _offset_scenario_formula as _offset_scenario_formula,
    _ratio_formula as _ratio_formula,
    _ref_formula as _ref_formula,
    _ref_source_id as _ref_source_id,
    _ref_target_id as _ref_target_id,
    _rewrite_refs as _rewrite_refs,
    _sum_or_ref_formula as _sum_or_ref_formula,
    _yoy_formula as _yoy_formula,
)
from .segment_model_helpers import (
    _assert_no_duplicate_ids as _assert_no_duplicate_ids,
    _assert_rows_unique as _assert_rows_unique,
    _get_section as _get_section,
    _iter_items_with_section as _iter_items_with_section,
    _iter_sheet_items as _iter_sheet_items,
    _repair_deleted_refs_in_item as _repair_deleted_refs_in_item,
    _rewire_scenario_table_refs as _rewire_scenario_table_refs,
    _set_imported_value as _set_imported_value,
    _shift_rows as _shift_rows,
)
from .segment_history_helpers import (
    populate_segment_historicals as populate_segment_historicals,
    prune_segment_formula_periods as prune_segment_formula_periods,
)
from .segment_override_helpers import (
    apply_segment_overrides as apply_segment_overrides,
)
from .segment_profile_helpers import (
    ExpandResult as ExpandResult,
    MultiAxisResult as MultiAxisResult,
    SegmentInfo as SegmentInfo,
    SegmentProfile as SegmentProfile,
    _derived_component_basis_by_year as _derived_component_basis_by_year,
    _derived_revenue_observations as _derived_revenue_observations,
    _segment_sort_key as _segment_sort_key,
    revenue_observations_to_values as revenue_observations_to_values,
    segment_revenue_observation_list as segment_revenue_observation_list,
    segment_revenue_observations_from_snapshot as segment_revenue_observations_from_snapshot,
    segment_revenue_values as segment_revenue_values,
)
from .segment_template_constants import (
    EXCLUDED_MEMBER_SUFFIXES as EXCLUDED_MEMBER_SUFFIXES,
    MATERIALITY_THRESHOLD as MATERIALITY_THRESHOLD,
    SEGMENT_ITEM_ID_ROLES as SEGMENT_ITEM_ID_ROLES,
    _ASSUMPTIONS_SEGMENT_IDS as _ASSUMPTIONS_SEGMENT_IDS,
    _ASSUMPTIONS_SEGMENT_START_ROW as _ASSUMPTIONS_SEGMENT_START_ROW,
    _ASSUMPTIONS_TOTAL_REVENUE_ROW as _ASSUMPTIONS_TOTAL_REVENUE_ROW,
    _FM_SECTION_SHIFT_ROWS as _FM_SECTION_SHIFT_ROWS,
    _FM_SECTION_START_ROWS as _FM_SECTION_START_ROWS,
    _FM_SEGMENT_IDS as _FM_SEGMENT_IDS,
    _REBUILD_LATER_IDS as _REBUILD_LATER_IDS,
    _SCENARIO_TABLE_NEW_GROWTH_ID as _SCENARIO_TABLE_NEW_GROWTH_ID,
    _SCENARIO_TABLE_OLD_GROWTH_ID as _SCENARIO_TABLE_OLD_GROWTH_ID,
    _rebuild_later_ids as _rebuild_later_ids,
)


EdgarFinancialsFetcher = Callable[[str, int, int, bool], Dict]


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

    decomposition_values = _consolidated_values(all_consolidated)
    consolidated_values = _largest_consolidated_values(all_consolidated)
    if not all_dimensional or not decomposition_values:
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
            decomposition_values,
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
    other_basis_parts_by_year: Dict[int, set[str]] = defaultdict(set)

    def add_other_member_value(member: str, year: int, value: float) -> None:
        other_values[int(year)] += float(value)
        observation = observations_by_member.get(member, {}).get(int(year))
        basis_key = getattr(observation, "basis_key", None)
        if basis_key:
            other_basis_parts_by_year[int(year)].add(str(basis_key))

    for member, values in revenue_by_member.items():
        if member in rollup_members:
            continue
        if member not in current_members:
            for year, value in values.items():
                add_other_member_value(member, int(year), value)
            continue

        share = _materiality_share(values, consolidated_values)
        if share < MATERIALITY_THRESHOLD:
            for year, value in values.items():
                add_other_member_value(member, int(year), value)
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
        other_basis_by_year = _derived_component_basis_by_year(
            "derived_other",
            other_values,
            other_basis_parts_by_year,
        )
        named_segments.append(
            SegmentInfo(
                name="Other",
                revenue_observations=_derived_revenue_observations(
                    other_values,
                    source="derived_other",
                    note="Aggregated from immaterial or non-current segment members.",
                    basis_by_year=other_basis_by_year,
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
    def tolerance_for(target: float) -> float:
        return max(abs(float(target)) * float(rollup_tolerance), 0.01)

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
