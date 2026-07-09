from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .business_model import BusinessModel, DriverNode, derive_driver_assumption_plan
from .business_model_compiler_errors import BusinessModelCompileError
from .business_model_compiler_formula import _compile_formula, _node_expr
from .business_model_compiler_nodes import (
    _build_node_lookup as _build_node_lookup,  # noqa: F401 - compatibility alias
    _consolidation_dependency_node_ids as _consolidation_dependency_node_ids,  # noqa: F401 - compatibility alias
    _count_bm_rows,
    _count_tree_rows as _count_tree_rows,  # noqa: F401 - compatibility alias
    _direct_consolidation_growth_node_ids,
    _driver_expr_node_refs as _driver_expr_node_refs,  # noqa: F401 - compatibility alias
    _iter_driver_nodes as _iter_driver_nodes,  # noqa: F401 - compatibility alias
    _primary_scenario_growth_node_id,
    _primary_scenario_owner_rate_id,
)
from .business_model_compiler_plans import _SegmentCompilePlan, _reconcile_segments
from .driver_resolver import resolve_driver_key
from .model_build_context import ModelBuildContext, SegmentProfileSnapshot
from .models import FinancialModel, FormulaSpec, ItemType, LineItem, LineItemRef, ModelScale, Unit
from .segments import (
    SegmentInfo as SegmentInfo,
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
    segment_revenue_observations_from_snapshot as segment_revenue_observations_from_snapshot,
)
from .templates import load_sia_generic_template


_SCENARIO_TABLE_INTERMEDIATE_GROWTH_ID = "tpl.a.revenue_drivers.business_segment_1_growth"
_SCENARIO_LABEL_ID = "tpl.a.scenario_tables.scenario_volume_growth_label"


@dataclass
class CompiledDriverRegistry:
    validation_model: FinancialModel
    driver_keys: dict[str, str] = field(default_factory=dict)
    node_items: dict[str, str] = field(default_factory=dict)
    segment_mapping: dict[str, int] = field(default_factory=dict)
    segment_profile: SegmentProfile = field(
        default_factory=lambda: SegmentProfile(ticker="TEMPLATE", segments=[], source="caller_override")
    )


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
        direct_consolidation_node_ids = _driver_expr_node_refs(
            plan.segment.revenue_model.consolidation_formula
        )
        direct_revenue_growth_node_ids = (
            _direct_consolidation_growth_node_ids(plan.segment)
            if len(direct_consolidation_node_ids) == 1
            else set()
        )
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
        if primary_item_id is None and entry.existing_driver_key:
            # Consolidated-opex drivers (and any entry whose namespaced
            # `{segment_id}.{driver_node_id}` identity has no independent
            # resolution path) bind to their declared `existing_driver_key`.
            # Falling back to it lets the alias loop below register the
            # `consolidated.<node>` / `bm.consolidated.<node>` forms the
            # model-engine looks drivers up by. Conservative: only fires when
            # the declared binding itself resolves, so a genuinely-unmapped
            # driver still leaves the namespaced key unregistered (fails loud).
            primary_item_id = _resolve_driver_plan_alias_item_id(
                registry,
                entry.existing_driver_key,
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
                model_scale=_node_model_scale(node),
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
        direct_revenue_growth = (
            node.id in direct_revenue_growth_node_ids
            and len(direct_revenue_growth_node_ids) == 1
        )
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
                model_scale=_node_model_scale(node),
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
                model_scale=None,
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
                model_scale=_node_model_scale(node),
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
            model_scale=_node_model_scale(node),
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
    model_scale: ModelScale | None,
    item_type: ItemType,
    historical: FormulaSpec | None,
    projected: FormulaSpec | None,
    formula_periods: list[int] | None,
) -> LineItem:
    updates: dict[str, Any] = {
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
    }
    if model_scale is not None:
        updates["model_scale"] = model_scale
    return prototype.model_copy(deep=True, update=updates)


def _node_model_scale(node: DriverNode) -> ModelScale:
    if node.model_scale is not None:
        return node.model_scale
    if node.unit is not Unit.dollars:
        return "units"
    if node.driver is not None and node.driver.type == "external" and set(node.factors) == {"price"}:
        return "units"
    return "millions"


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
