"""Projected working-capital policy helpers for schema build orchestration."""

from __future__ import annotations

from collections.abc import Iterable
import math
import sys
from typing import Any

from .build_formula_refs import _formula_period_is_valid as _formula_period_is_valid_fallback
from .build_non_gaap_addbacks import (
    _coerce_optional_float as _shared_coerce_optional_float,
    _computed_model_values as _shared_computed_model_values,
    _computed_value as _shared_computed_value,
    _missing_projection_periods as _shared_missing_projection_periods,
    _set_projection_input_values as _shared_set_projection_input_values,
)
from .models import FinancialModel


_INVENTORY_ASSUMPTION_ITEM_ID = "tpl.a.balance_sheet_wc.current_asset_2"
_INVENTORY_DRIVER_ITEM_ID = "tpl.a.balance_sheet_wc.of_cogs_and_sg_a"
_INVENTORY_BALANCE_ITEM_ID = "tpl.fm.balance_sheet.current_asset_2"
_INVENTORY_CHANGE_ITEM_ID = "tpl.fm.cash_flow.current_asset_2"
_INVENTORY_REVENUE_ITEM_ID = "tpl.fm.income_statement.total_revenue"
_INVENTORY_MATERIALITY_REVENUE_RATIO = 0.01
_INVENTORY_POLICY_NOTE = "build fallback: latest historical inventory/(COGS+SG&A) ratio carried forward"


def _parent_attr(name: str, fallback):
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    return getattr(parent, name, fallback) if parent is not None else fallback


def _period_list(values: Iterable[int]) -> list[int]:
    return [int(period) for period in values]


def _latest_positive_computed_value(
    computed_values: dict[str, dict[int, float]],
    item_id: str,
    periods: Iterable[int],
) -> float | None:
    computed_value = _parent_attr("_computed_value", _shared_computed_value)
    coerce_optional_float = _parent_attr("_coerce_optional_float", _shared_coerce_optional_float)
    for period in sorted({int(period) for period in periods}, reverse=True):
        value = coerce_optional_float(computed_value(computed_values, item_id, period))
        if value is not None and math.isfinite(value) and value > 0:
            return value
    return None


def _inventory_is_material(
    computed_values: dict[str, dict[int, float]],
    historical_periods: Iterable[int],
) -> bool:
    computed_value = _parent_attr("_computed_value", _shared_computed_value)
    coerce_optional_float = _parent_attr("_coerce_optional_float", _shared_coerce_optional_float)
    inventory_values: list[float] = []
    revenue_values: list[float] = []
    inventory_item_id = _parent_attr("_INVENTORY_BALANCE_ITEM_ID", _INVENTORY_BALANCE_ITEM_ID)
    revenue_item_id = _parent_attr("_INVENTORY_REVENUE_ITEM_ID", _INVENTORY_REVENUE_ITEM_ID)
    for period in historical_periods:
        inventory = coerce_optional_float(computed_value(computed_values, inventory_item_id, int(period)))
        revenue = coerce_optional_float(computed_value(computed_values, revenue_item_id, int(period)))
        if inventory is not None and inventory > 0:
            inventory_values.append(inventory)
        if revenue is not None and revenue > 0:
            revenue_values.append(revenue)
    if not inventory_values:
        return False
    if not revenue_values:
        return max(inventory_values) > 1.0
    return (max(inventory_values) / max(revenue_values)) >= float(
        _parent_attr("_INVENTORY_MATERIALITY_REVENUE_RATIO", _INVENTORY_MATERIALITY_REVENUE_RATIO)
    )


def _extend_projected_formula_periods(
    model: FinancialModel,
    item_id: str,
    projection_periods: Iterable[int],
) -> bool:
    try:
        item = model.get_item(item_id)
    except KeyError:
        return False
    if item.projected is None or item.formula_periods is None:
        return False

    historical_set = {
        int(period)
        for period in (
            model.time_structure.historical_periods
            or model.time_structure.historical_years
            or []
        )
    }
    projection_set = {
        int(period)
        for period in (
            model.time_structure.projection_periods
            or model.time_structure.projection_years
            or []
        )
    }
    available_periods = historical_set | projection_set
    formula_period_is_valid = _parent_attr(
        "_formula_period_is_valid",
        _formula_period_is_valid_fallback,
    )
    existing = {int(period) for period in item.formula_periods}
    additions = {
        int(period)
        for period in projection_periods
        if formula_period_is_valid(
            model,
            item,
            int(period),
            historical_set=historical_set,
            projection_set=projection_set,
            available_periods=available_periods,
        )
    }
    if not additions - existing:
        return False
    item.formula_periods = sorted(existing | additions)
    return True


def _apply_projected_inventory_policy(model: FinancialModel) -> dict[str, Any]:
    """Carry material inventory through the projection horizon.

    The generic template already models inventory as a percentage of COGS plus
    G&A. Builds can drift when the driver row is available through the horizon
    but dependent inventory rows remain clipped to the first projected years.
    This repair keeps the template policy and only extends/seeds the projection
    surface needed for inventory and the cash-flow delta to compute.
    """

    if not model._index:
        model.build_index()
    historical_periods = _period_list(
        model.time_structure.historical_periods or model.time_structure.historical_years
    )
    projection_periods = _period_list(
        model.time_structure.projection_periods or model.time_structure.projection_years
    )
    if not historical_periods or not projection_periods:
        return {"seeded": [], "extended": [], "skipped": []}

    computed_model_values = _parent_attr("_computed_model_values", _shared_computed_model_values)
    missing_projection_periods = _parent_attr(
        "_missing_projection_periods",
        _shared_missing_projection_periods,
    )
    set_projection_input_values = _parent_attr(
        "_set_projection_input_values",
        _shared_set_projection_input_values,
    )
    inventory_driver_item_id = _parent_attr("_INVENTORY_DRIVER_ITEM_ID", _INVENTORY_DRIVER_ITEM_ID)
    inventory_assumption_item_id = _parent_attr(
        "_INVENTORY_ASSUMPTION_ITEM_ID",
        _INVENTORY_ASSUMPTION_ITEM_ID,
    )
    inventory_balance_item_id = _parent_attr("_INVENTORY_BALANCE_ITEM_ID", _INVENTORY_BALANCE_ITEM_ID)
    inventory_change_item_id = _parent_attr("_INVENTORY_CHANGE_ITEM_ID", _INVENTORY_CHANGE_ITEM_ID)

    computed_values = computed_model_values(model)
    if not _inventory_is_material(computed_values, historical_periods):
        return {"seeded": [], "extended": [], "skipped": []}

    seeded: list[str] = []
    extended: list[str] = []
    skipped: list[dict[str, Any]] = []

    missing_driver_periods = missing_projection_periods(
        computed_values,
        inventory_driver_item_id,
        projection_periods,
    )
    if missing_driver_periods:
        latest_driver = _latest_positive_computed_value(
            computed_values,
            inventory_driver_item_id,
            historical_periods,
        )
        values_by_period = (
            {int(period): latest_driver for period in missing_driver_periods}
            if latest_driver is not None
            else {}
        )
        if set_projection_input_values(
            model,
            inventory_driver_item_id,
            values_by_period,
            note=_INVENTORY_POLICY_NOTE,
        ):
            seeded.append(inventory_driver_item_id)
        else:
            skipped.append(
                {
                    "item_id": inventory_driver_item_id,
                    "reason": "insufficient_inventory_driver_history",
                }
            )

    for item_id in (
        inventory_assumption_item_id,
        inventory_balance_item_id,
        inventory_change_item_id,
    ):
        if _extend_projected_formula_periods(model, item_id, projection_periods):
            extended.append(item_id)

    if seeded or extended:
        computed_values = computed_model_values(model)
    for item_id in (inventory_balance_item_id, inventory_change_item_id):
        missing_periods = missing_projection_periods(
            computed_values,
            item_id,
            projection_periods,
        )
        if missing_periods:
            skipped.append(
                {
                    "item_id": item_id,
                    "reason": "missing_projection_values",
                    "periods": missing_periods,
                }
            )

    return {"seeded": seeded, "extended": extended, "skipped": skipped}


__all__ = [
    "_INVENTORY_ASSUMPTION_ITEM_ID",
    "_INVENTORY_BALANCE_ITEM_ID",
    "_INVENTORY_CHANGE_ITEM_ID",
    "_INVENTORY_DRIVER_ITEM_ID",
    "_INVENTORY_MATERIALITY_REVENUE_RATIO",
    "_INVENTORY_POLICY_NOTE",
    "_INVENTORY_REVENUE_ITEM_ID",
    "_apply_projected_inventory_policy",
    "_extend_projected_formula_periods",
    "_inventory_is_material",
    "_latest_positive_computed_value",
]
