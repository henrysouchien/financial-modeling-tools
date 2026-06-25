"""Projected non-GAAP add-back seed helpers for schema build orchestration."""

from __future__ import annotations

from collections.abc import Iterable
import math
import sys
from typing import Any

from .dependency_graph import DependencyGraph
from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    ValueCell,
    ValueProvenance,
    ValueSeries,
)


_REVENUE_ITEM_ID = "tpl.fm.income_statement.total_revenue"
_PROJECTED_DA_TOTAL_ID = "tpl.a.depreciation_amortization.depreciation_and_amortization_m"
_PROJECTED_DEPRECIATION_ID = "tpl.a.depreciation_amortization.depreciation"
_PROJECTED_DA_RATE_ID = "tpl.a.depreciation_amortization.depreciation_as_of_beginning_property_and_equipment"
_PROJECTED_DA_BASE_ID = "tpl.a.depreciation_amortization.beg_property_and_equipment"
_PROJECTED_SBC_TOTAL_ID = "tpl.a.stock_based_compensation.stock_based_compensation"
_PROJECTED_SBC_RATE_IDS = (
    "tpl.a.stock_based_compensation.cost_of_revenues_pct_line_item",
    "tpl.a.stock_based_compensation.sales_and_marketing_pct_line_item",
    "tpl.a.stock_based_compensation.research_and_development_pct_line_item",
    "tpl.a.stock_based_compensation.general_and_administrative_pct_line_item",
)
_PROJECTED_SBC_COMPONENT_IDS = (
    "tpl.a.stock_based_compensation.cost_of_revenues",
    "tpl.a.stock_based_compensation.sales_and_marketing",
    "tpl.a.stock_based_compensation.research_and_development",
    "tpl.a.stock_based_compensation.general_and_administrative",
)
_PROJECTED_SBC_BASE_IDS = (
    "tpl.a.unit_economics.costs_of_goods_sold",
    "tpl.a.operating_leverage.sales_and_marketing",
    "tpl.a.operating_leverage.research_and_development",
    "tpl.a.operating_leverage.general_and_administrative",
)


def _parent_attr(name: str, fallback):
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    return getattr(parent, name, fallback) if parent is not None else fallback


def _coerce_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        coerced = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(coerced):
        return None
    return coerced


def _computed_model_values(model: FinancialModel) -> dict[str, dict[int, float]]:
    dependency_graph_cls = _parent_attr("DependencyGraph", DependencyGraph)
    item_type = _parent_attr("ItemType", ItemType)
    graph = dependency_graph_cls()
    graph.build(model)
    derived_ids = {
        item.id
        for item in model._index.values()
        if item.item_type == item_type.derived
    }
    return graph.compute({}, recompute=derived_ids)


def _computed_value(
    computed_values: dict[str, dict[int, float]],
    item_id: str,
    period: int,
) -> float | None:
    coerce_optional_float = _parent_attr("_coerce_optional_float", _coerce_optional_float)
    value = computed_values.get(item_id, {}).get(int(period))
    return coerce_optional_float(value)


def _latest_ratio_from_computed_values(
    computed_values: dict[str, dict[int, float]],
    numerator_id: str,
    denominator_id: str,
    periods: Iterable[int],
) -> float | None:
    computed_value = _parent_attr("_computed_value", _computed_value)
    for period in sorted({int(period) for period in periods}, reverse=True):
        numerator = computed_value(computed_values, numerator_id, period)
        denominator = computed_value(computed_values, denominator_id, period)
        if numerator is None or denominator is None or denominator <= 0:
            continue
        ratio = numerator / denominator
        if math.isfinite(ratio) and ratio > 0:
            return ratio
    return None


def _missing_projection_periods(
    computed_values: dict[str, dict[int, float]],
    item_id: str,
    projection_periods: Iterable[int],
) -> list[int]:
    computed_value = _parent_attr("_computed_value", _computed_value)
    return [
        int(period)
        for period in projection_periods
        if computed_value(computed_values, item_id, int(period)) is None
    ]


def _set_projection_input_values(
    model: FinancialModel,
    item_id: str,
    values_by_period: dict[int, float],
    *,
    note: str,
) -> bool:
    if not values_by_period:
        return False
    try:
        item_obj = model.get_item(item_id)
    except KeyError:
        return False
    value_series_cls = _parent_attr("ValueSeries", ValueSeries)
    value_cell_cls = _parent_attr("ValueCell", ValueCell)
    value_provenance = _parent_attr("ValueProvenance", ValueProvenance)
    formula_spec_cls = _parent_attr("FormulaSpec", FormulaSpec)
    formula_type = _parent_attr("FormulaType", FormulaType)
    if item_obj.values is None:
        item_obj.values = value_series_cls()
    if item_obj.overrides is None:
        item_obj.overrides = {}
    if item_obj.formula_periods is not None:
        item_obj.formula_periods = sorted(
            {int(period) for period in item_obj.formula_periods}
            | {int(period) for period in values_by_period}
        )
    for period, value in values_by_period.items():
        item_obj.values.values[int(period)] = value_cell_cls(
            period=int(period),
            value=float(value),
            provenance=value_provenance.derived,
            note=note,
        )
        item_obj.overrides[int(period)] = formula_spec_cls(
            type=formula_type.constant,
            params={"value": float(value)},
            note=note,
        )
    return True


def _seed_projected_non_gaap_addbacks(model: FinancialModel) -> dict[str, Any]:
    """Seed forward D&A/SBC add-back drivers from latest historical ratios when blank."""

    if not model._index:
        model.build_index()
    historical_periods = [int(period) for period in model.time_structure.historical_periods]
    projection_periods = [int(period) for period in model.time_structure.projection_periods]
    if not historical_periods or not projection_periods:
        return {"seeded": []}

    computed_model_values = _parent_attr("_computed_model_values", _computed_model_values)
    missing_projection_periods = _parent_attr("_missing_projection_periods", _missing_projection_periods)
    latest_ratio_from_computed_values = _parent_attr(
        "_latest_ratio_from_computed_values",
        _latest_ratio_from_computed_values,
    )
    computed_value = _parent_attr("_computed_value", _computed_value)
    set_projection_input_values = _parent_attr(
        "_set_projection_input_values",
        _set_projection_input_values,
    )

    revenue_item_id = _parent_attr("_REVENUE_ITEM_ID", _REVENUE_ITEM_ID)
    projected_depreciation_id = _parent_attr("_PROJECTED_DEPRECIATION_ID", _PROJECTED_DEPRECIATION_ID)
    projected_da_rate_id = _parent_attr("_PROJECTED_DA_RATE_ID", _PROJECTED_DA_RATE_ID)
    projected_da_base_id = _parent_attr("_PROJECTED_DA_BASE_ID", _PROJECTED_DA_BASE_ID)
    projected_sbc_total_id = _parent_attr("_PROJECTED_SBC_TOTAL_ID", _PROJECTED_SBC_TOTAL_ID)
    projected_sbc_rate_ids = _parent_attr("_PROJECTED_SBC_RATE_IDS", _PROJECTED_SBC_RATE_IDS)
    projected_sbc_component_ids = _parent_attr(
        "_PROJECTED_SBC_COMPONENT_IDS",
        _PROJECTED_SBC_COMPONENT_IDS,
    )
    projected_sbc_base_ids = _parent_attr("_PROJECTED_SBC_BASE_IDS", _PROJECTED_SBC_BASE_IDS)

    computed_values = computed_model_values(model)
    seeded: list[str] = []
    skipped: list[dict[str, Any]] = []

    missing_da_periods = missing_projection_periods(
        computed_values,
        projected_depreciation_id,
        projection_periods,
    )
    if missing_da_periods:
        depreciation_revenue_ratio = latest_ratio_from_computed_values(
            computed_values,
            projected_depreciation_id,
            revenue_item_id,
            historical_periods,
        )
        values_by_period: dict[int, float] = {}
        if depreciation_revenue_ratio is not None:
            for period in missing_da_periods:
                revenue = computed_value(computed_values, revenue_item_id, period)
                base = computed_value(computed_values, projected_da_base_id, period)
                if revenue is None or base is None or base <= 0:
                    continue
                value = (depreciation_revenue_ratio * revenue) / base
                if math.isfinite(value) and value > 0:
                    values_by_period[int(period)] = value
        if set_projection_input_values(
            model,
            projected_da_rate_id,
            values_by_period,
            note="build fallback: latest historical depreciation/revenue ratio applied to projected revenue",
        ):
            seeded.append(projected_da_rate_id)
        else:
            skipped.append({"item_id": projected_da_rate_id, "reason": "insufficient_history_or_projection_base"})

    missing_sbc_periods_by_component = {
        component_id: missing_projection_periods(computed_values, component_id, projection_periods)
        for component_id in projected_sbc_component_ids
    }
    if any(missing_sbc_periods_by_component.values()):
        sbc_revenue_ratio = latest_ratio_from_computed_values(
            computed_values,
            projected_sbc_total_id,
            revenue_item_id,
            historical_periods,
        )
        common_values_by_period: dict[int, float] = {}
        if sbc_revenue_ratio is not None:
            for period in projection_periods:
                revenue = computed_value(computed_values, revenue_item_id, period)
                if revenue is None:
                    continue
                denominator = sum(
                    value
                    for item_id in projected_sbc_base_ids
                    if (value := computed_value(computed_values, item_id, period)) is not None
                )
                if denominator <= 0:
                    continue
                value = (sbc_revenue_ratio * revenue) / denominator
                if math.isfinite(value) and value > 0:
                    common_values_by_period[int(period)] = value
        seeded_sbc = False
        for item_id, component_id, base_id in zip(
            projected_sbc_rate_ids,
            projected_sbc_component_ids,
            projected_sbc_base_ids,
        ):
            values_by_period = {
                period: common_values_by_period[period]
                for period in missing_sbc_periods_by_component[component_id]
                if period in common_values_by_period
                and (base := computed_value(computed_values, base_id, period)) is not None
                and base > 0
            }
            if set_projection_input_values(
                model,
                item_id,
                values_by_period,
                note="build fallback: latest historical SBC/revenue ratio allocated across operating line items",
            ):
                seeded.append(item_id)
                seeded_sbc = True
        if not seeded_sbc:
            skipped.append({"item_id": projected_sbc_total_id, "reason": "insufficient_history_or_projection_base"})

    return {"seeded": seeded, "skipped": skipped}


__all__ = [
    "_PROJECTED_DA_BASE_ID",
    "_PROJECTED_DA_RATE_ID",
    "_PROJECTED_DA_TOTAL_ID",
    "_PROJECTED_DEPRECIATION_ID",
    "_PROJECTED_SBC_BASE_IDS",
    "_PROJECTED_SBC_COMPONENT_IDS",
    "_PROJECTED_SBC_RATE_IDS",
    "_PROJECTED_SBC_TOTAL_ID",
    "_REVENUE_ITEM_ID",
    "_coerce_optional_float",
    "_computed_model_values",
    "_computed_value",
    "_latest_ratio_from_computed_values",
    "_missing_projection_periods",
    "_seed_projected_non_gaap_addbacks",
    "_set_projection_input_values",
]
