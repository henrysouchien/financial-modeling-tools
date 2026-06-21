from __future__ import annotations

from dataclasses import dataclass, field
import re
from typing import Any

from .business_model import (
    BusinessModel,
    DriverExpr,
    DriverNode,
    NodeRef,
    derive_driver_assumption_plan,
)
from .driver_resolver import resolve_driver_key
from .model_build_context import ModelBuildContext, SegmentProfileSnapshot
from .models import FinancialModel, FormulaSpec, FormulaType, ItemType, LineItem, LineItemRef
from .segments import (
    SegmentInfo,
    SegmentProfile,
    _assert_no_duplicate_ids,
    _assert_rows_unique,
    _carry_forward_formula,
    _get_section,
    _growth_formula,
    _iter_sheet_items,
    _offset_scenario_formula,
    _ratio_formula,
    _ref_formula,
    _remove_existing_segment_items,
    _rewire_scenario_table_refs,
    _shift_rows,
    _sum_or_ref_formula,
    _yoy_formula,
    segment_revenue_observations_from_snapshot,
)
from .templates import load_sia_generic_template


_SCENARIO_TABLE_INTERMEDIATE_GROWTH_ID = "tpl.a.revenue_drivers.business_segment_1_growth"
_SCENARIO_LABEL_ID = "tpl.a.scenario_tables.scenario_volume_growth_label"
_NORMALIZE_NAME_RE = re.compile(r"[^a-z0-9]+")


class BusinessModelCompileError(ValueError):
    """Raised when a BusinessModel cannot be compiled onto a template."""


@dataclass
class CompiledDriverRegistry:
    validation_model: FinancialModel
    driver_keys: dict[str, str] = field(default_factory=dict)
    node_items: dict[str, str] = field(default_factory=dict)
    segment_mapping: dict[str, int] = field(default_factory=dict)
    segment_profile: SegmentProfile = field(
        default_factory=lambda: SegmentProfile(ticker="TEMPLATE", segments=[], source="caller_override")
    )


@dataclass(frozen=True)
class _SegmentCompilePlan:
    segment: Any | None
    segment_index: int
    info: SegmentInfo
    segment_id: str | None = None
    unmanaged_snapshot: bool = False


def compile_business_model(
    model: FinancialModel,
    business_model: BusinessModel,
    *,
    edgar_snapshot: SegmentProfileSnapshot | None = None,
) -> CompiledDriverRegistry:
    """Compile a BusinessModel onto a FinancialModel template."""

    model.build_index()
    prototypes = _load_prototypes(model)
    assumptions_rows, fm_rows = _remove_existing_segment_items(model)

    plans, segment_mapping, segment_profile = _reconcile_segments(business_model, edgar_snapshot)
    if not plans:
        raise BusinessModelCompileError("business model must define at least one segment")

    projection_periods = _projection_periods(model)
    historical_periods = _historical_periods(model)
    all_periods = [int(period) for period in historical_periods + projection_periods]

    bm_rows_used = _count_bm_rows(plans)
    assumptions_delta = max(0, bm_rows_used - assumptions_rows["freed"])
    if assumptions_delta:
        _shift_rows(_iter_sheet_items(model, "Assumptions"), 27, assumptions_delta)

    fm_delta = max(0, len(plans) - fm_rows["freed"])
    if fm_delta:
        _shift_rows(_iter_sheet_items(model, "Financial_model"), 55, fm_delta)
        _shift_rows(_iter_sheet_items(model, "Financial_model"), 43, fm_delta)
        _shift_rows(_iter_sheet_items(model, "Financial_model"), 7, fm_delta)

    fm_base_rows = {
        "income_statement": fm_rows["income_statement"],
        "margins": fm_rows["margins"] + fm_delta,
        "growth_rates": fm_rows["growth_rates"] + fm_delta * 2,
    }

    assumptions_section = _get_section(model, "Assumptions", "revenue_drivers")
    income_statement_section = _get_section(model, "Financial_model", "income_statement")
    margins_section = _get_section(model, "Financial_model", "margins")
    growth_rates_section = _get_section(model, "Financial_model", "growth_rates")

    registry = CompiledDriverRegistry(
        validation_model=model,
        driver_keys={},
        node_items={},
        segment_mapping=segment_mapping,
        segment_profile=segment_profile,
    )

    assumptions_items: list[LineItem] = []
    income_statement_items: list[LineItem] = []
    margin_items: list[LineItem] = []
    growth_items: list[LineItem] = []
    segment_revenue_ids: list[str] = []
    segment_fm_revenue_ids: list[str] = []

    current_row = assumptions_rows["start"]
    primary_plan: _SegmentCompilePlan | None = None

    for plan in plans:
        if plan.segment is None:
            revenue_id = f"bm.{plan.segment_id}.__rev"
            growth_id = f"bm.{plan.segment_id}.__growth"
            revenue_fm_id = f"tpl.fm.income_statement.business_segment_{plan.segment_index}_revenue"
            margin_fm_id = f"tpl.fm.margins.business_segment_{plan.segment_index}_pct_revenue"
            growth_fm_id = f"tpl.fm.growth_rates.business_segment_{plan.segment_index}_growth"
            fm_rows_for_segment = {
                "income_statement": fm_base_rows["income_statement"] + plan.segment_index - 1,
                "margins": fm_base_rows["margins"] + plan.segment_index - 1,
                "growth_rates": fm_base_rows["growth_rates"] + plan.segment_index - 1,
            }
            segment_revenue_ids.append(revenue_id)
            segment_fm_revenue_ids.append(revenue_fm_id)
            current_row = _materialize_unmanaged_snapshot_segment(
                plan=plan,
                prototypes=prototypes,
                all_periods=all_periods,
                projection_periods=projection_periods,
                assumptions_items=assumptions_items,
                income_statement_items=income_statement_items,
                margin_items=margin_items,
                growth_items=growth_items,
                current_row=current_row,
                revenue_id=revenue_id,
                growth_id=growth_id,
                revenue_fm_id=revenue_fm_id,
                margin_fm_id=margin_fm_id,
                growth_fm_id=growth_fm_id,
                fm_rows_for_segment=fm_rows_for_segment,
            )
            plan.info.item_ids = {
                "revenue": revenue_id,
                "growth": growth_id,
                "revenue_fm": revenue_fm_id,
                "margin_fm": margin_fm_id,
                "growth_fm": growth_fm_id,
            }
            continue

        if plan.segment_index == 1:
            primary_plan = plan

        revenue_id = f"bm.{plan.segment.id}.__rev"
        growth_id = f"bm.{plan.segment.id}.__growth"
        revenue_fm_id = f"tpl.fm.income_statement.business_segment_{plan.segment_index}_revenue"
        margin_fm_id = f"tpl.fm.margins.business_segment_{plan.segment_index}_pct_revenue"
        growth_fm_id = f"tpl.fm.growth_rates.business_segment_{plan.segment_index}_growth"
        node_lookup, non_materialized = _build_node_lookup(plan.segment)
        scenario_growth_node_id = (
            _primary_scenario_growth_node_id(plan.segment)
            if plan.segment_index == 1
            else None
        )
        direct_revenue_growth_node_ids = _direct_consolidation_growth_node_ids(plan.segment)
        current_row = _materialize_segment_tree(
            segment=plan.segment,
            node_lookup=node_lookup,
            non_materialized=non_materialized,
            prototypes=prototypes,
            projection_periods=projection_periods,
            all_periods=all_periods,
            registry=registry,
            assumptions_items=assumptions_items,
            current_row=current_row,
            scenario_growth_node_id=scenario_growth_node_id,
            direct_revenue_growth_node_ids=direct_revenue_growth_node_ids,
            segment_revenue_fm_id=revenue_fm_id,
            has_segment_revenue_observations=bool(plan.info.revenue_observations),
        )

        segment_revenue_ids.append(revenue_id)
        segment_fm_revenue_ids.append(revenue_fm_id)

        consolidation_formula = _compile_formula(
            plan.segment.revenue_model.consolidation_formula,
            segment_id=plan.segment.id,
            referencing_node_id="__rev",
            node_lookup=node_lookup,
            non_materialized=non_materialized,
            current_item_id=revenue_id,
        )

        segment_revenue = prototypes["segment_revenue"].model_copy(
            deep=True,
            update={
                "id": revenue_id,
                "label": plan.segment.label,
                "row": current_row,
                "item_type": ItemType.derived,
                "historical": _ref_formula(revenue_fm_id),
                "projected": consolidation_formula,
                "formula_periods": list(all_periods),
                "overrides": None,
                "values": None,
                "template_token": None,
                "build_notes": None,
            },
        )
        assumptions_items.append(segment_revenue)
        current_row += 1

        segment_growth = prototypes["segment_growth"].model_copy(
            deep=True,
            update={
                "id": growth_id,
                "label": " y/y % chg.",
                "row": current_row,
                "item_type": ItemType.derived,
                "historical": _yoy_formula(revenue_id),
                "projected": (
                    _yoy_formula(revenue_id)
                    if scenario_growth_node_id is not None
                    else _offset_scenario_formula(anchor_id=_SCENARIO_LABEL_ID)
                    if plan.segment_index == 1
                    else _carry_forward_formula(growth_id)
                ),
                "formula_periods": list(all_periods),
                "overrides": None,
                "values": None,
                "template_token": None,
                "build_notes": None,
            },
        )
        assumptions_items.append(segment_growth)
        current_row += 1

        fallback_single = len(plans) == 1 and not plan.info.revenue_observations
        fm_rows_for_segment = {
            "income_statement": fm_base_rows["income_statement"] + plan.segment_index - 1,
            "margins": fm_base_rows["margins"] + plan.segment_index - 1,
            "growth_rates": fm_base_rows["growth_rates"] + plan.segment_index - 1,
        }

        income_statement_items.append(
            prototypes["revenue_fm"].model_copy(
                deep=True,
                update={
                    "id": revenue_fm_id,
                    "label": f" {plan.segment.label}",
                    "row": fm_rows_for_segment["income_statement"],
                    "item_type": ItemType.derived,
                    "historical": _ref_formula("tpl.fm.income_statement.total_revenue") if fallback_single else None,
                    "projected": _ref_formula(revenue_id),
                    "formula_periods": list(all_periods if fallback_single else projection_periods),
                    "data_concept_id": None,
                    "overrides": None,
                    "values": None,
                    "template_token": None,
                    "build_notes": None,
                },
            )
        )
        margin_items.append(
            prototypes["margin_fm"].model_copy(
                deep=True,
                update={
                    "id": margin_fm_id,
                    "label": f" {plan.segment.label}",
                    "row": fm_rows_for_segment["margins"],
                    "item_type": ItemType.derived,
                    "historical": _ratio_formula(revenue_fm_id, "tpl.fm.income_statement.total_revenue"),
                    "projected": _ratio_formula(revenue_fm_id, "tpl.fm.income_statement.total_revenue"),
                    "formula_periods": list(all_periods),
                    "overrides": None,
                    "values": None,
                    "template_token": None,
                    "build_notes": None,
                },
            )
        )
        growth_items.append(
            prototypes["growth_fm"].model_copy(
                deep=True,
                update={
                    "id": growth_fm_id,
                    "label": f" {plan.segment.label}",
                    "row": fm_rows_for_segment["growth_rates"],
                    "item_type": ItemType.derived,
                    "historical": _yoy_formula(revenue_fm_id),
                    "projected": _yoy_formula(revenue_fm_id),
                    "formula_periods": list(all_periods),
                    "overrides": None,
                    "values": None,
                    "template_token": None,
                    "build_notes": None,
                },
            )
        )

        plan.info.item_ids = {
            "revenue": revenue_id,
            "growth": growth_id,
            "revenue_fm": revenue_fm_id,
            "margin_fm": margin_fm_id,
            "growth_fm": growth_fm_id,
        }

    if primary_plan is None:
        raise BusinessModelCompileError("compiled segments must include a primary segment with segment_index == 1")

    assumptions_section.line_items.extend(assumptions_items)
    income_statement_section.line_items.extend(income_statement_items)
    margins_section.line_items.extend(margin_items)
    growth_rates_section.line_items.extend(growth_items)

    _sort_section_items(assumptions_section)
    _sort_section_items(income_statement_section)
    _sort_section_items(margins_section)
    _sort_section_items(growth_rates_section)

    assumptions_total = model.get_item("tpl.a.revenue_drivers.total_revenue")
    assumptions_total.projected = _sum_or_ref_formula([LineItemRef(id=item_id) for item_id in segment_revenue_ids])

    fm_total = model.get_item("tpl.fm.income_statement.total_revenue")
    fm_segment_refs = [LineItemRef(id=item_id) for item_id in segment_fm_revenue_ids]
    fm_total_fallback_single = len(plans) == 1 and not plans[0].info.revenue_observations
    fm_total.historical = None if fm_total_fallback_single else _sum_or_ref_formula(fm_segment_refs)
    fm_total.projected = _sum_or_ref_formula(fm_segment_refs)
    fm_total.formula_periods = list(all_periods)

    primary_growth_id = (
        _primary_scenario_owner_rate_id(primary_plan.segment)
        or f"bm.{primary_plan.segment.id}.__growth"
    )
    _rewire_scenario_table_refs(
        model,
        old_id=_SCENARIO_TABLE_INTERMEDIATE_GROWTH_ID,
        new_id=primary_growth_id,
    )
    model.get_item(_SCENARIO_LABEL_ID).label = _scenario_label(primary_plan.segment.label)

    _assert_rows_unique(model)
    _assert_no_duplicate_ids(model)
    registry.validation_model = model
    registry.segment_profile = segment_profile
    _register_driver_assumption_plan_keys(registry, business_model)
    return registry


def compile_bm_for_validation(
    business_model: BusinessModel,
    mbc: ModelBuildContext,
) -> CompiledDriverRegistry:
    """Compile BM against a validation-only template for MBC validation."""

    model = load_sia_generic_template()
    registry = compile_business_model(
        model,
        business_model,
        edgar_snapshot=mbc.segment_config.segment_profile_snapshot if mbc.segment_config else None,
    )
    model.build_index()
    registry.validation_model = model
    return registry


def _load_prototypes(model: FinancialModel) -> dict[str, LineItem]:
    return {
        "input_row": model.get_item("tpl.a.revenue_drivers.operating_metric").model_copy(deep=True),
        "rate_row": model.get_item("tpl.a.revenue_drivers.business_segment_2_volume_growth").model_copy(deep=True),
        "derived_row": model.get_item("tpl.a.revenue_drivers.business_segment_1_revenue").model_copy(deep=True),
        "segment_revenue": model.get_item("tpl.a.revenue_drivers.business_segment_1_revenue").model_copy(deep=True),
        "segment_growth": model.get_item("tpl.a.revenue_drivers.business_segment_1_growth").model_copy(deep=True),
        "revenue_fm": model.get_item("tpl.fm.income_statement.business_segment_1_revenue").model_copy(deep=True),
        "margin_fm": model.get_item("tpl.fm.margins.business_segment_1_pct_revenue").model_copy(deep=True),
        "growth_fm": model.get_item("tpl.fm.growth_rates.business_segment_1_growth").model_copy(deep=True),
    }


def _historical_periods(model: FinancialModel) -> list[int]:
    periods = model.time_structure.historical_periods or model.time_structure.historical_years
    return [int(period) for period in periods]


def _projection_periods(model: FinancialModel) -> list[int]:
    periods = model.time_structure.projection_periods or model.time_structure.projection_years
    return [int(period) for period in periods]


def _reconcile_segments(
    business_model: BusinessModel,
    edgar_snapshot: SegmentProfileSnapshot | None,
) -> tuple[list[_SegmentCompilePlan], dict[str, int], SegmentProfile]:
    if edgar_snapshot is None:
        plans = [
            _SegmentCompilePlan(
                segment=segment,
                segment_index=index,
                info=SegmentInfo(
                    name=segment.label,
                    edgar_member=segment.edgar_member,
                ),
            )
            for index, segment in enumerate(business_model.segments, start=1)
        ]
        return (
            plans,
            {segment.id: index for index, segment in enumerate(business_model.segments, start=1)},
            SegmentProfile(
                ticker=business_model.company.ticker,
                segments=[plan.info for plan in plans],
                source="caller_override",
            ),
        )

    snapshot_by_name: dict[str, Any] = {}
    snapshot_by_index: dict[int, Any] = {}
    for snapshot_segment in edgar_snapshot.segments:
        normalized = _normalize_name(snapshot_segment.name)
        if normalized in snapshot_by_name:
            raise BusinessModelCompileError(
                f"snapshot contains duplicate segment names after normalization: {snapshot_segment.name!r}"
            )
        if snapshot_segment.segment_index in snapshot_by_index:
            raise BusinessModelCompileError(
                f"snapshot contains duplicate segment_index values: {snapshot_segment.segment_index}"
            )
        snapshot_by_name[normalized] = snapshot_segment
        snapshot_by_index[int(snapshot_segment.segment_index)] = snapshot_segment

    plans: list[_SegmentCompilePlan] = []
    matched_snapshot_names: set[str] = set()
    segment_mapping: dict[str, int] = {}
    used_segment_ids = {str(segment.id) for segment in business_model.segments}
    next_supplemental_index = max(snapshot_by_index) + 1 if snapshot_by_index else 1

    for segment in business_model.segments:
        normalized = _normalize_name(segment.match_name)
        snapshot_segment = snapshot_by_name.get(normalized)
        if snapshot_segment is None:
            if segment.edgar_axis or segment.edgar_member:
                raise BusinessModelCompileError(
                    f"no EDGAR snapshot match for BM segment {segment.id!r} (match_name={segment.match_name!r})"
                )
            # A BusinessModel can include source-backed revenue streams that
            # are not EDGAR axis members, e.g. interest income in total revenue.
            segment_index = next_supplemental_index
            next_supplemental_index += 1
            segment_mapping[segment.id] = segment_index
            plans.append(
                _SegmentCompilePlan(
                    segment=segment,
                    segment_index=segment_index,
                    info=SegmentInfo(
                        name=segment.label,
                        edgar_member=None,
                    ),
                    segment_id=segment.id,
                )
            )
            continue
        if normalized in matched_snapshot_names:
            raise BusinessModelCompileError(f"snapshot segment {snapshot_segment.name!r} matched more than once")
        if segment.edgar_member and snapshot_segment.edgar_member and segment.edgar_member != snapshot_segment.edgar_member:
            raise BusinessModelCompileError(
                "EDGAR member conflict for BM segment "
                f"{segment.id!r}: business_model={segment.edgar_member!r}, "
                f"snapshot={snapshot_segment.edgar_member!r}"
            )

        matched_snapshot_names.add(normalized)
        segment_mapping[segment.id] = int(snapshot_segment.segment_index)
        plans.append(
            _SegmentCompilePlan(
                segment=segment,
                segment_index=int(snapshot_segment.segment_index),
                info=SegmentInfo(
                    name=segment.label,
                    edgar_member=segment.edgar_member or snapshot_segment.edgar_member,
                    revenue_observations=segment_revenue_observations_from_snapshot(snapshot_segment),
                    volume_label=snapshot_segment.volume_label,
                    price_label=snapshot_segment.price_label,
                ),
                segment_id=segment.id,
            )
        )

    unmatched_snapshot_segments = [
        snapshot_segment
        for snapshot_segment in edgar_snapshot.segments
        if _normalize_name(snapshot_segment.name) not in matched_snapshot_names
    ]
    residual_snapshot_segments = [
        snapshot_segment
        for snapshot_segment in unmatched_snapshot_segments
        if _is_unmanaged_residual_snapshot_segment(snapshot_segment)
    ]
    residual_snapshot_segment_ids = {id(snapshot_segment) for snapshot_segment in residual_snapshot_segments}
    blocking_unmatched_snapshot_names = sorted(
        snapshot_segment.name
        for snapshot_segment in unmatched_snapshot_segments
        if id(snapshot_segment) not in residual_snapshot_segment_ids
    )
    for snapshot_segment in residual_snapshot_segments:
        segment_id = _unmanaged_snapshot_segment_id(
            snapshot_segment.name,
            int(snapshot_segment.segment_index),
            used_segment_ids,
        )
        plans.append(
            _SegmentCompilePlan(
                segment=None,
                segment_index=int(snapshot_segment.segment_index),
                info=SegmentInfo(
                    name=snapshot_segment.name,
                    edgar_member=None,
                    revenue_observations=segment_revenue_observations_from_snapshot(snapshot_segment),
                    volume_label=snapshot_segment.volume_label,
                    price_label=snapshot_segment.price_label,
                ),
                segment_id=segment_id,
                unmanaged_snapshot=True,
            )
        )
    unmatched_snapshot_names = blocking_unmatched_snapshot_names
    if unmatched_snapshot_names:
        raise BusinessModelCompileError(
            f"EDGAR snapshot has unmatched segments: {unmatched_snapshot_names}"
        )

    plans.sort(key=lambda plan: plan.segment_index)
    if not plans or plans[0].segment_index != 1:
        raise BusinessModelCompileError("EDGAR reconciliation must produce a primary segment with segment_index == 1")

    return (
        plans,
        segment_mapping,
        SegmentProfile(
            ticker=business_model.company.ticker,
            segments=[plan.info for plan in plans],
            source=edgar_snapshot.source,
            axis_used=edgar_snapshot.axis_used,
            total_revenue_check=dict(edgar_snapshot.total_revenue_check) if edgar_snapshot.total_revenue_check else None,
        ),
    )


def _is_unmanaged_residual_snapshot_segment(snapshot_segment: Any) -> bool:
    if str(getattr(snapshot_segment, "edgar_member", "") or "").strip():
        return False
    if _normalize_name(getattr(snapshot_segment, "name", "")) not in {
        "other",
        "other segments",
        "all other",
    }:
        return False
    observations = list(getattr(snapshot_segment, "revenue_observations", None) or [])
    if not observations:
        return False
    return all(str(getattr(observation, "source", "") or "") == "derived_other" for observation in observations)


def _unmanaged_snapshot_segment_id(name: str, segment_index: int, used_segment_ids: set[str]) -> str:
    normalized = _NORMALIZE_NAME_RE.sub("_", str(name or "").strip().casefold()).strip("_")
    normalized = "_".join(part for part in normalized.split("_") if part) or "segment"
    base = f"unmodeled_{normalized}_{int(segment_index)}"
    candidate = base
    suffix = 2
    while candidate in used_segment_ids:
        candidate = f"{base}_{suffix}"
        suffix += 1
    used_segment_ids.add(candidate)
    return candidate


def _materialize_unmanaged_snapshot_segment(
    *,
    plan: _SegmentCompilePlan,
    prototypes: dict[str, LineItem],
    all_periods: list[int],
    projection_periods: list[int],
    assumptions_items: list[LineItem],
    income_statement_items: list[LineItem],
    margin_items: list[LineItem],
    growth_items: list[LineItem],
    current_row: int,
    revenue_id: str,
    growth_id: str,
    revenue_fm_id: str,
    margin_fm_id: str,
    growth_fm_id: str,
    fm_rows_for_segment: dict[str, int],
) -> int:
    revenue = prototypes["segment_revenue"].model_copy(
        deep=True,
        update={
            "id": revenue_id,
            "label": plan.info.name,
            "row": current_row,
            "item_type": ItemType.derived,
            "historical": _ref_formula(revenue_fm_id),
            "projected": _carry_forward_formula(revenue_id),
            "formula_periods": list(all_periods),
            "overrides": None,
            "values": None,
            "template_token": None,
            "build_notes": (
                "Unmodeled residual segment from source snapshot; projected flat unless "
                "the analyst replaces it with a modeled BusinessModel segment."
            ),
        },
    )
    assumptions_items.append(revenue)
    current_row += 1

    growth = prototypes["segment_growth"].model_copy(
        deep=True,
        update={
            "id": growth_id,
            "label": " y/y % chg.",
            "row": current_row,
            "item_type": ItemType.derived,
            "historical": _yoy_formula(revenue_id),
            "projected": _yoy_formula(revenue_id),
            "formula_periods": list(all_periods),
            "overrides": None,
            "values": None,
            "template_token": None,
            "build_notes": "Derived from the unmodeled residual revenue row.",
        },
    )
    assumptions_items.append(growth)
    current_row += 1

    income_statement_items.append(
        prototypes["revenue_fm"].model_copy(
            deep=True,
            update={
                "id": revenue_fm_id,
                "label": f" {plan.info.name}",
                "row": fm_rows_for_segment["income_statement"],
                "item_type": ItemType.derived,
                "historical": None,
                "projected": _ref_formula(revenue_id),
                "formula_periods": list(projection_periods),
                "data_concept_id": None,
                "overrides": None,
                "values": None,
                "template_token": None,
                "build_notes": None,
            },
        )
    )
    margin_items.append(
        prototypes["margin_fm"].model_copy(
            deep=True,
            update={
                "id": margin_fm_id,
                "label": f" {plan.info.name}",
                "row": fm_rows_for_segment["margins"],
                "item_type": ItemType.derived,
                "historical": _ratio_formula(revenue_fm_id, "tpl.fm.income_statement.total_revenue"),
                "projected": _ratio_formula(revenue_fm_id, "tpl.fm.income_statement.total_revenue"),
                "formula_periods": list(all_periods),
                "overrides": None,
                "values": None,
                "template_token": None,
                "build_notes": None,
            },
        )
    )
    growth_items.append(
        prototypes["growth_fm"].model_copy(
            deep=True,
            update={
                "id": growth_fm_id,
                "label": f" {plan.info.name}",
                "row": fm_rows_for_segment["growth_rates"],
                "item_type": ItemType.derived,
                "historical": _yoy_formula(revenue_fm_id),
                "projected": _yoy_formula(revenue_fm_id),
                "formula_periods": list(all_periods),
                "overrides": None,
                "values": None,
                "template_token": None,
                "build_notes": None,
            },
        )
    )
    return current_row


def _count_bm_rows(plans: list[_SegmentCompilePlan]) -> int:
    rows = 0
    for plan in plans:
        if plan.segment is None:
            rows += 2
            continue
        rows += _count_tree_rows(plan.segment.revenue_model.decomposition)
        rows += 2
    return rows


def _count_tree_rows(nodes: list[DriverNode]) -> int:
    rows = 0
    for node in nodes:
        rows += _count_tree_rows(node.children or [])
        target_type = node.compile_to.target_type
        if target_type not in {"assumption_row", "derived_row"}:
            continue
        expr = _node_expr(node)
        rows += 2 if expr and expr.type == "growth" else 1
    return rows


def _iter_driver_nodes(nodes: list[DriverNode]):
    for node in nodes:
        yield node
        if node.children:
            yield from _iter_driver_nodes(node.children)


def _driver_expr_node_refs(expr: DriverExpr | None) -> set[str]:
    if expr is None:
        return set()
    if expr.type == "growth":
        node_id = expr.params.base.node_id
        return set() if node_id == "self" else {node_id}
    if expr.type in {"product", "sum"}:
        return {ref.node_id for ref in expr.params.operands if ref.node_id != "self"}
    if expr.type == "derived":
        return {
            ref.node_id
            for ref in (expr.params.numerator, expr.params.denominator)
            if ref.node_id != "self"
        }
    if expr.type == "roll_forward":
        refs = [expr.params.beginning, *expr.params.additions, *expr.params.subtractions]
        return {ref.node_id for ref in refs if ref.node_id != "self"}
    return set()


def _consolidation_dependency_node_ids(segment: Any) -> set[str]:
    nodes_by_id = {
        node.id: node
        for node in _iter_driver_nodes(segment.revenue_model.decomposition)
    }
    dependencies: set[str] = set()
    pending = list(_driver_expr_node_refs(segment.revenue_model.consolidation_formula))
    while pending:
        node_id = pending.pop()
        if node_id in dependencies:
            continue
        dependencies.add(node_id)
        node = nodes_by_id.get(node_id)
        if node is not None:
            pending.extend(_driver_expr_node_refs(_node_expr(node)) - dependencies)
    return dependencies


def _direct_consolidation_growth_node_ids(segment: Any) -> set[str]:
    nodes_by_id = {
        node.id: node
        for node in _iter_driver_nodes(segment.revenue_model.decomposition)
    }
    result: set[str] = set()
    for node_id in _driver_expr_node_refs(segment.revenue_model.consolidation_formula):
        node = nodes_by_id.get(node_id)
        expr = _node_expr(node) if node is not None else None
        if expr is not None and expr.type == "growth":
            result.add(node_id)
    return result


def _primary_scenario_growth_node_id(segment: Any) -> str | None:
    consolidation_dependencies = _consolidation_dependency_node_ids(segment)
    first_growth_node_id: str | None = None
    for node in _iter_driver_nodes(segment.revenue_model.decomposition):
        expr = _node_expr(node)
        if expr is None or expr.type != "growth" or node.compile_to.target_type != "assumption_row":
            continue
        if node.id not in consolidation_dependencies:
            continue
        first_growth_node_id = first_growth_node_id or node.id
        factor_names = {str(factor).lower() for factor in (node.factors or [])}
        if "volume" in factor_names:
            return node.id
    return first_growth_node_id


def _primary_scenario_owner_rate_id(segment: Any) -> str | None:
    node_id = _primary_scenario_growth_node_id(segment)
    if node_id is None:
        return None
    for node in _iter_driver_nodes(segment.revenue_model.decomposition):
        if node.id != node_id:
            continue
        expr = _node_expr(node)
        if expr is None or expr.type != "growth":
            return None
        return f"bm.{segment.id}.{node.id}__{expr.params.rate_key}"
    return None


def _build_node_lookup(segment: Any) -> tuple[dict[str, str], dict[str, str]]:
    node_lookup: dict[str, str] = {}
    non_materialized: dict[str, str] = {}

    def walk(nodes: list[DriverNode]) -> None:
        for node in nodes:
            target_type = node.compile_to.target_type
            if target_type in {"assumption_row", "derived_row"}:
                node_lookup[node.id] = f"bm.{segment.id}.{node.id}"
            else:
                non_materialized[node.id] = target_type
            if node.children:
                walk(node.children)

    walk(segment.revenue_model.decomposition)
    return node_lookup, non_materialized


def _register_driver_assumption_plan_keys(
    registry: CompiledDriverRegistry,
    business_model: BusinessModel,
) -> None:
    try:
        driver_plan = derive_driver_assumption_plan(business_model)
    except ValueError:
        return

    for entry in driver_plan.entries:
        primary_item_id = _resolve_driver_plan_alias_item_id(
            registry,
            f"{entry.segment_id}.{entry.driver_node_id}",
        )
        if primary_item_id is not None:
            registry.driver_keys.setdefault(entry.driver_key, primary_item_id)
        for alias in entry.aliases:
            item_id = _resolve_driver_plan_alias_item_id(registry, alias)
            if item_id is not None:
                registry.driver_keys.setdefault(alias, item_id)
            elif primary_item_id is not None:
                registry.driver_keys.setdefault(alias, primary_item_id)


def _resolve_driver_plan_alias_item_id(
    registry: CompiledDriverRegistry,
    alias: str | None,
) -> str | None:
    normalized = str(alias or "").strip()
    if not normalized:
        return None

    item_id = registry.driver_keys.get(normalized)
    if item_id is not None:
        return item_id

    direct_item_id = _compiled_input_item_id(registry, normalized)
    if direct_item_id is not None:
        return direct_item_id

    try:
        resolved_item_id = resolve_driver_key(normalized, compiled_registry=registry)
    except Exception:
        return None
    return _compiled_input_item_id(registry, resolved_item_id)


def _compiled_input_item_id(
    registry: CompiledDriverRegistry,
    item_id: str,
) -> str | None:
    try:
        item = registry.validation_model.get_item(str(item_id))
    except KeyError:
        return None
    return item.id if item.item_type is ItemType.input else None


def _materialize_segment_tree(
    *,
    segment: Any,
    node_lookup: dict[str, str],
    non_materialized: dict[str, str],
    prototypes: dict[str, LineItem],
    projection_periods: list[int],
    all_periods: list[int],
    registry: CompiledDriverRegistry,
    assumptions_items: list[LineItem],
    current_row: int,
    scenario_growth_node_id: str | None,
    direct_revenue_growth_node_ids: set[str],
    segment_revenue_fm_id: str,
    has_segment_revenue_observations: bool,
) -> int:
    for node in segment.revenue_model.decomposition:
        current_row = _materialize_node(
            segment=segment,
            node=node,
            node_lookup=node_lookup,
            non_materialized=non_materialized,
            prototypes=prototypes,
            projection_periods=projection_periods,
            all_periods=all_periods,
            registry=registry,
            assumptions_items=assumptions_items,
            current_row=current_row,
            scenario_growth_node_id=scenario_growth_node_id,
            direct_revenue_growth_node_ids=direct_revenue_growth_node_ids,
            segment_revenue_fm_id=segment_revenue_fm_id,
            has_segment_revenue_observations=has_segment_revenue_observations,
        )
    return current_row


def _materialize_node(
    *,
    segment: Any,
    node: DriverNode,
    node_lookup: dict[str, str],
    non_materialized: dict[str, str],
    prototypes: dict[str, LineItem],
    projection_periods: list[int],
    all_periods: list[int],
    registry: CompiledDriverRegistry,
    assumptions_items: list[LineItem],
    current_row: int,
    scenario_growth_node_id: str | None,
    direct_revenue_growth_node_ids: set[str],
    segment_revenue_fm_id: str,
    has_segment_revenue_observations: bool,
) -> int:
    for child in node.children or []:
        current_row = _materialize_node(
            segment=segment,
            node=child,
            node_lookup=node_lookup,
            non_materialized=non_materialized,
            prototypes=prototypes,
            projection_periods=projection_periods,
            all_periods=all_periods,
            registry=registry,
            assumptions_items=assumptions_items,
            current_row=current_row,
            scenario_growth_node_id=scenario_growth_node_id,
            direct_revenue_growth_node_ids=direct_revenue_growth_node_ids,
            segment_revenue_fm_id=segment_revenue_fm_id,
            has_segment_revenue_observations=has_segment_revenue_observations,
        )

    target = node.compile_to
    expr = _node_expr(node)
    registry_key = f"{segment.id}.{node.id}"

    if target.target_type == "commentary":
        return current_row

    if target.target_type == "existing_row":
        try:
            registry.driver_keys[registry_key] = resolve_driver_key(target.existing_driver_key or "")
        except Exception as exc:  # pragma: no cover - propagated in tests as compile failure
            raise BusinessModelCompileError(
                f"failed to resolve existing_driver_key for node {registry_key!r}: {target.existing_driver_key!r}"
            ) from exc
        return current_row

    if expr is None:
        raise BusinessModelCompileError(f"node {registry_key!r} has no compilable driver expression")

    item_id = node_lookup[node.id]
    registry.node_items[registry_key] = item_id

    if expr.type == "external":
        if target.target_type != "assumption_row":
            raise BusinessModelCompileError(f"external node {registry_key!r} must compile to assumption_row")
        assumptions_items.append(
            _node_item(
                prototypes["input_row"],
                item_id=item_id,
                label=node.label,
                row=current_row,
                unit=node.unit,
                item_type=ItemType.input,
                historical=None,
                projected=None,
                formula_periods=None,
            )
        )
        registry.driver_keys[registry_key] = item_id
        return current_row + 1

    if expr.type == "growth":
        if target.target_type != "assumption_row":
            raise BusinessModelCompileError(f"growth node {registry_key!r} must compile to assumption_row")

        rate_id = f"{item_id}__{expr.params.rate_key}"
        direct_revenue_growth = node.id in direct_revenue_growth_node_ids
        historical = (
            _ref_formula(segment_revenue_fm_id)
            if direct_revenue_growth and not has_segment_revenue_observations
            else None
        )
        assumptions_items.append(
            _node_item(
                prototypes["input_row"],
                item_id=item_id,
                label=node.label,
                row=current_row,
                unit=node.unit,
                item_type=ItemType.input,
                historical=historical,
                projected=_growth_formula(item_id, rate_id),
                formula_periods=list(all_periods if historical is not None else projection_periods),
            )
        )
        assumptions_items.append(
            _node_item(
                prototypes["rate_row"],
                item_id=rate_id,
                label=" y/y % chg.",
                row=current_row + 1,
                unit=prototypes["rate_row"].unit,
                item_type=ItemType.input,
                historical=None,
                projected=(
                    _offset_scenario_formula(anchor_id=_SCENARIO_LABEL_ID)
                    if node.id == scenario_growth_node_id
                    else None
                ),
                formula_periods=list(all_periods) if node.id == scenario_growth_node_id else None,
            )
        )
        registry.driver_keys[registry_key] = item_id
        registry.driver_keys[f"{registry_key}.{expr.params.rate_key}"] = rate_id
        return current_row + 2

    formula = _compile_formula(
        expr,
        segment_id=segment.id,
        referencing_node_id=node.id,
        node_lookup=node_lookup,
        non_materialized=non_materialized,
        current_item_id=item_id,
    )

    if target.target_type == "derived_row":
        assumptions_items.append(
            _node_item(
                prototypes["derived_row"],
                item_id=item_id,
                label=node.label,
                row=current_row,
                unit=node.unit,
                item_type=ItemType.derived,
                historical=formula,
                projected=formula,
                formula_periods=list(all_periods),
            )
        )
        return current_row + 1

    assumptions_items.append(
        _node_item(
            prototypes["derived_row"],
            item_id=item_id,
            label=node.label,
            row=current_row,
            unit=node.unit,
            item_type=ItemType.derived,
            historical=None,
            projected=formula,
            formula_periods=list(projection_periods),
        )
    )
    return current_row + 1


def _node_item(
    prototype: LineItem,
    *,
    item_id: str,
    label: str,
    row: int,
    unit: Any,
    item_type: ItemType,
    historical: FormulaSpec | None,
    projected: FormulaSpec | None,
    formula_periods: list[int] | None,
) -> LineItem:
    return prototype.model_copy(
        deep=True,
        update={
            "id": item_id,
            "label": label,
            "row": row,
            "unit": unit,
            "item_type": item_type,
            "historical": historical,
            "projected": projected,
            "formula_periods": formula_periods,
            "values": None,
            "overrides": None,
            "template_token": None,
            "build_notes": None,
            "repeat_group_id": None,
            "repeat_group_role": None,
        },
    )


def _compile_formula(
    expr: DriverExpr,
    *,
    segment_id: str,
    referencing_node_id: str,
    node_lookup: dict[str, str],
    non_materialized: dict[str, str],
    current_item_id: str,
) -> FormulaSpec:
    if expr.type == "product":
        return FormulaSpec(
            type=FormulaType.arithmetic,
            params={
                "operands": [
                    "*",
                    *[
                        _resolve_ref_expr(
                            operand,
                            segment_id=segment_id,
                            referencing_node_id=referencing_node_id,
                            node_lookup=node_lookup,
                            non_materialized=non_materialized,
                            current_item_id=current_item_id,
                        )
                        for operand in expr.params.operands
                    ],
                ]
            },
        )

    if expr.type == "sum":
        return FormulaSpec(
            type=FormulaType.arithmetic,
            params={
                "operands": [
                    "+",
                    *[
                        _resolve_ref_expr(
                            operand,
                            segment_id=segment_id,
                            referencing_node_id=referencing_node_id,
                            node_lookup=node_lookup,
                            non_materialized=non_materialized,
                            current_item_id=current_item_id,
                        )
                        for operand in expr.params.operands
                    ],
                ]
            },
        )

    if expr.type == "derived":
        numerator = _resolve_line_item_ref(
            expr.params.numerator,
            segment_id=segment_id,
            referencing_node_id=referencing_node_id,
            node_lookup=node_lookup,
            non_materialized=non_materialized,
            current_item_id=current_item_id,
        )
        denominator = _resolve_line_item_ref(
            expr.params.denominator,
            segment_id=segment_id,
            referencing_node_id=referencing_node_id,
            node_lookup=node_lookup,
            non_materialized=non_materialized,
            current_item_id=current_item_id,
        )
        if numerator.t == 0 and denominator.t == 0:
            return _ratio_formula(numerator.id, denominator.id)
        return FormulaSpec(
            type=FormulaType.ratio,
            params={"numerator": numerator, "denominator": denominator},
        )

    if expr.type == "roll_forward":
        return FormulaSpec(
            type=FormulaType.roll_forward,
            params={
                "beginning": _resolve_line_item_ref(
                    expr.params.beginning,
                    segment_id=segment_id,
                    referencing_node_id=referencing_node_id,
                    node_lookup=node_lookup,
                    non_materialized=non_materialized,
                    current_item_id=current_item_id,
                ),
                "additions": [
                    _resolve_line_item_ref(
                        ref,
                        segment_id=segment_id,
                        referencing_node_id=referencing_node_id,
                        node_lookup=node_lookup,
                        non_materialized=non_materialized,
                        current_item_id=current_item_id,
                    )
                    for ref in expr.params.additions
                ],
                "subtractions": [
                    _resolve_line_item_ref(
                        ref,
                        segment_id=segment_id,
                        referencing_node_id=referencing_node_id,
                        node_lookup=node_lookup,
                        non_materialized=non_materialized,
                        current_item_id=current_item_id,
                    )
                    for ref in expr.params.subtractions
                ],
            },
        )

    if expr.type == "growth":
        raise BusinessModelCompileError(
            f"growth expression on {segment_id}.{referencing_node_id} must be handled as an assumption_row pair"
        )
    if expr.type == "external":
        raise BusinessModelCompileError(
            f"external expression on {segment_id}.{referencing_node_id} does not compile to a FormulaSpec"
        )
    raise BusinessModelCompileError(f"unsupported driver expression {expr.type!r}")


def _resolve_ref_expr(
    ref: NodeRef,
    *,
    segment_id: str,
    referencing_node_id: str,
    node_lookup: dict[str, str],
    non_materialized: dict[str, str],
    current_item_id: str,
) -> Any:
    resolved = _resolve_line_item_ref(
        ref,
        segment_id=segment_id,
        referencing_node_id=referencing_node_id,
        node_lookup=node_lookup,
        non_materialized=non_materialized,
        current_item_id=current_item_id,
    )
    if ref.sign == -1:
        return {"op": "NEG", "arg": resolved}
    return resolved


def _resolve_line_item_ref(
    ref: NodeRef,
    *,
    segment_id: str,
    referencing_node_id: str,
    node_lookup: dict[str, str],
    non_materialized: dict[str, str],
    current_item_id: str,
) -> LineItemRef:
    if ref.node_id == "self":
        return LineItemRef(id=current_item_id, t=ref.t)

    target_id = node_lookup.get(ref.node_id)
    if target_id is None:
        reason = "target is not materialized"
        if ref.node_id in non_materialized:
            reason = f"target compiles to {non_materialized[ref.node_id]!r} and is not materialized"
        raise BusinessModelCompileError(
            f"node {segment_id}.{referencing_node_id!r} cannot resolve NodeRef {ref.node_id!r}: {reason}"
        )
    return LineItemRef(id=target_id, t=ref.t)


def _node_expr(node: DriverNode) -> DriverExpr | None:
    if node.children_role == "decomposition":
        return node.children_formula
    return node.driver


def _normalize_name(value: str) -> str:
    normalized = _NORMALIZE_NAME_RE.sub(" ", str(value or "").strip().casefold())
    return " ".join(normalized.split())


def _scenario_label(segment_name: str) -> str:
    return "Revenue growth %" if segment_name == "Total Revenue" else f"{segment_name} revenue growth %"


def _sort_section_items(section: Any) -> None:
    section.line_items.sort(key=lambda item: (int(item.row), item.id))


__all__ = [
    "BusinessModelCompileError",
    "CompiledDriverRegistry",
    "compile_bm_for_validation",
    "compile_business_model",
]
