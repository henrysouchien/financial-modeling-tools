"""Projected working-capital policy helpers for schema build orchestration."""

from __future__ import annotations

from collections.abc import Iterable
import math
from typing import Any

from .build_formula_refs import _formula_period_is_valid as _formula_period_is_valid_fallback
from .build_non_gaap_addbacks import (
    _coerce_optional_float as _shared_coerce_optional_float,
    _computed_model_values as _shared_computed_model_values,
    _computed_value as _shared_computed_value,
    _missing_projection_periods as _shared_missing_projection_periods,
    _set_projection_input_values as _shared_set_projection_input_values,
)
from .handle import ComputedValues
from .required_role_catalog import load_required_role_catalog
from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItemRef,
)
from industry_slice_core.industry_types import Unit


_INVENTORY_ASSUMPTION_ITEM_ID = "tpl.a.balance_sheet_wc.current_asset_2"
_INVENTORY_DRIVER_ITEM_ID = "tpl.a.balance_sheet_wc.of_cogs_and_sg_a"
_INVENTORY_BALANCE_ITEM_ID = "tpl.fm.balance_sheet.current_asset_2"
_INVENTORY_CHANGE_ITEM_ID = "tpl.fm.cash_flow.current_asset_2"
_INVENTORY_REVENUE_ITEM_ID = "tpl.fm.income_statement.total_revenue"
_INVENTORY_MATERIALITY_REVENUE_RATIO = 0.01
_INVENTORY_POLICY_NOTE = (
    "company-grounded fallback: latest reported inventory/(COGS+SG&A) "
    "ratio carried forward for material inventory"
)
_DEFERRED_REVENUE_ASSUMPTION_ITEM_ID = (
    "tpl.a.balance_sheet_wc.deferred_revenues"
)
_DEFERRED_REVENUE_RATE_ITEM_ID = "tpl.a.balance_sheet_wc.of_rev"
_WORKING_CAPITAL_REVENUE_ITEM_ID = "tpl.fm.income_statement.total_revenue"


def _own_line_working_capital_items() -> tuple[tuple[str, ...], tuple[str, ...]]:
    """The operating balances the cash-flow statement carries on their own rows.

    Returned as (balance items, cash-flow items), from the catalog's cash-flow
    roles that declare ``change_of``. Every other operating balance is other
    working capital (`RequiredRoleCatalog.other_working_capital_leaves`), whose
    aggregate `_apply_other_working_capital_derivation` owns.
    """

    catalog = load_required_role_catalog()
    cash_roles = tuple(
        row for row in catalog.required_roles if row.change_of is not None
    )
    return (
        tuple(
            item_id
            for row in cash_roles
            for item_id in catalog.role(str(row.change_of)).template_items
        ),
        tuple(item_id for row in cash_roles for item_id in row.template_items),
    )


def _period_list(values: Iterable[int]) -> list[int]:
    return [int(period) for period in values]


def _latest_positive_computed_value(
    computed_values: ComputedValues,
    item_id: str,
    periods: Iterable[int],
) -> float | None:
    for period in sorted({int(period) for period in periods}, reverse=True):
        value = _shared_coerce_optional_float(
            _shared_computed_value(computed_values, item_id, period)
        )
        if value is not None and math.isfinite(value) and value > 0:
            return value
    return None


def _inventory_is_material(
    computed_values: ComputedValues,
    historical_periods: Iterable[int],
) -> bool:
    inventory_values: list[float] = []
    revenue_values: list[float] = []
    for period in historical_periods:
        inventory = _shared_coerce_optional_float(
            _shared_computed_value(
                computed_values, _INVENTORY_BALANCE_ITEM_ID, int(period)
            )
        )
        revenue = _shared_coerce_optional_float(
            _shared_computed_value(
                computed_values, _INVENTORY_REVENUE_ITEM_ID, int(period)
            )
        )
        if inventory is not None and inventory > 0:
            inventory_values.append(inventory)
        if revenue is not None and revenue > 0:
            revenue_values.append(revenue)
    if not inventory_values:
        return False
    if not revenue_values:
        return max(inventory_values) > 1.0
    return (max(inventory_values) / max(revenue_values)) >= float(
        _INVENTORY_MATERIALITY_REVENUE_RATIO
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
    existing = {int(period) for period in item.formula_periods}
    additions = {
        int(period)
        for period in projection_periods
        if _formula_period_is_valid_fallback(
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
    for day_control_id in (
        "tpl.a.balance_sheet_wc.days_sales_outstanding_dso",
        "tpl.a.balance_sheet_wc.days_payable_outstanding_dpo",
    ):
        if day_control_id in model._index:
            model.get_item(day_control_id).unit = Unit.days
    if _DEFERRED_REVENUE_RATE_ITEM_ID in model._index:
        deferred_revenue_rate_item = model.get_item(
            _DEFERRED_REVENUE_RATE_ITEM_ID
        )
        deferred_revenue_rate_item.item_type = ItemType.input
        deferred_revenue_rate_item.unit = Unit.percentage
    historical_periods = _period_list(
        model.time_structure.historical_periods or model.time_structure.historical_years
    )
    projection_periods = _period_list(
        model.time_structure.projection_periods or model.time_structure.projection_years
    )
    if not historical_periods or not projection_periods:
        return {"seeded": [], "extended": [], "skipped": []}

    computed_values = _shared_computed_model_values(model)
    if not _inventory_is_material(computed_values, historical_periods):
        return {"seeded": [], "extended": [], "skipped": []}

    seeded: list[str] = []
    extended: list[str] = []
    skipped: list[dict[str, Any]] = []

    missing_driver_periods = _shared_missing_projection_periods(
        computed_values,
        _INVENTORY_DRIVER_ITEM_ID,
        projection_periods,
    )
    if missing_driver_periods:
        # This fallback clears the company-specific-basis bar: it is enabled
        # only when this company's reported inventory is material, and the
        # carried value is calculated from this company's latest reported
        # inventory divided by its own COGS plus SG&A.  The dedicated driver
        # row remains available for an authored override.
        latest_driver = _latest_positive_computed_value(
            computed_values,
            _INVENTORY_DRIVER_ITEM_ID,
            historical_periods,
        )
        values_by_period = (
            {int(period): latest_driver for period in missing_driver_periods}
            if latest_driver is not None
            else {}
        )
        if _shared_set_projection_input_values(
            model,
            _INVENTORY_DRIVER_ITEM_ID,
            values_by_period,
            note=_INVENTORY_POLICY_NOTE,
        ):
            seeded.append(_INVENTORY_DRIVER_ITEM_ID)
        else:
            skipped.append(
                {
                    "item_id": _INVENTORY_DRIVER_ITEM_ID,
                    "reason": "insufficient_inventory_driver_history",
                }
            )

    for item_id in (
        _INVENTORY_ASSUMPTION_ITEM_ID,
        _INVENTORY_BALANCE_ITEM_ID,
        _INVENTORY_CHANGE_ITEM_ID,
    ):
        if _extend_projected_formula_periods(model, item_id, projection_periods):
            extended.append(item_id)

    if seeded or extended:
        computed_values = _shared_computed_model_values(model)
    for item_id in (_INVENTORY_BALANCE_ITEM_ID, _INVENTORY_CHANGE_ITEM_ID):
        missing_periods = _shared_missing_projection_periods(
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


def _complete_working_capital_schedule(model: FinancialModel) -> dict[str, Any]:
    """Compile working-capital schedules and their own-line operating-cash deltas.

    The existing template owns the row identities. This function fills the
    deferred-revenue schedule from its filing-native disclosure seed and
    extends the own-line balances and their cash-flow deltas through the
    requested horizon. The other-working-capital aggregate is not built here:
    its leaf set is the catalog's, filtered by reported balances, and
    `_apply_other_working_capital_derivation` owns it for every year.
    """

    if not model._index:
        model.build_index()
    projection_periods = _period_list(
        model.time_structure.projection_periods
        or model.time_structure.projection_years
    )
    if not projection_periods:
        return {"wired": [], "extended": [], "declared_missing": []}

    wired: list[str] = []
    extended: list[str] = []
    declared_missing: list[str] = []

    def _optional_item(item_id: str):
        try:
            return model.get_item(item_id)
        except KeyError:
            return None

    deferred_revenue = _optional_item(_DEFERRED_REVENUE_ASSUMPTION_ITEM_ID)
    deferred_revenue_rate = _optional_item(_DEFERRED_REVENUE_RATE_ITEM_ID)
    if deferred_revenue is not None and deferred_revenue_rate is not None:
        # Deferred revenue is a schedule input; the baseline face row
        # ``tpl.fm.balance_sheet.deferred_revenue_current`` presents it in
        # every period the schedule holds a reported balance.
        deferred_revenue.item_type = ItemType.input
        deferred_revenue.historical = None
        deferred_revenue.projected = FormulaSpec(
            type=FormulaType.driver,
            subtype="base_x_rate",
            params={
                "base": LineItemRef(id=_WORKING_CAPITAL_REVENUE_ITEM_ID),
                "rate": LineItemRef(id=_DEFERRED_REVENUE_RATE_ITEM_ID),
            },
            note="canonical_deferred_revenue_working_capital_driver",
        )
        deferred_revenue_rate.historical = FormulaSpec(
            type=FormulaType.ratio,
            subtype="divide",
            params={
                "numerator": LineItemRef(
                    id=_DEFERRED_REVENUE_ASSUMPTION_ITEM_ID
                ),
                "denominator": LineItemRef(
                    id=_WORKING_CAPITAL_REVENUE_ITEM_ID
                ),
            },
        )
        # This carry-forward is company-grounded, not a blind default: the rate
        # it carries is computed above from this company's own reported
        # deferred revenue over its own reported revenue, and the template
        # provides a dedicated rate row for an analyst to override.  It is a
        # stated modeling assumption about a real observed ratio, which is the
        # bar a compiler-applied carry-forward has to clear.
        deferred_revenue_rate.projected = FormulaSpec(
            type=FormulaType.ref,
            subtype="carry_forward",
            params={
                "source": LineItemRef(
                    id=_DEFERRED_REVENUE_RATE_ITEM_ID,
                    t=-1,
                )
            },
            note="carry_forward_observed_company_deferred_revenue_ratio",
        )
        # These template inputs already have an unrestricted period axis.
        # Company-grounded role retirement owns any later restriction, including
        # an empty axis when another reported balance holds deferred revenue.
        wired.extend(
            [
                _DEFERRED_REVENUE_ASSUMPTION_ITEM_ID,
                _DEFERRED_REVENUE_RATE_ITEM_ID,
            ]
        )

    # This presentation row carries an expected concept but no projection
    # driver, so the compiler has no company-specific basis for forecasting
    # it.  A flat carry-forward would be a blind heuristic default.  The
    # current_liability_2 slot is no longer in this category: the canonical
    # lease schedule now owns it as current operating lease liabilities.
    current_asset_3 = _optional_item("tpl.fm.balance_sheet.current_asset_3")
    if current_asset_3 is None or current_asset_3.projected is None:
        declared_missing.append("tpl.fm.balance_sheet.current_asset_3")

    balance_items, cash_items = _own_line_working_capital_items()
    for item_id in balance_items:
        item = _optional_item(item_id)
        if item is None or item.projected is None:
            if item is not None:
                declared_missing.append(item_id)
            continue
        if _extend_projected_formula_periods(model, item_id, projection_periods):
            extended.append(item_id)

    for item_id in cash_items:
        if _extend_projected_formula_periods(model, item_id, projection_periods):
            extended.append(item_id)

    return {
        "wired": sorted(set(wired)),
        "extended": sorted(set(extended)),
        "declared_missing": sorted(set(declared_missing)),
    }


__all__ = [
    "_INVENTORY_ASSUMPTION_ITEM_ID",
    "_INVENTORY_BALANCE_ITEM_ID",
    "_INVENTORY_CHANGE_ITEM_ID",
    "_INVENTORY_DRIVER_ITEM_ID",
    "_INVENTORY_MATERIALITY_REVENUE_RATIO",
    "_INVENTORY_POLICY_NOTE",
    "_INVENTORY_REVENUE_ITEM_ID",
    "_own_line_working_capital_items",
    "_complete_working_capital_schedule",
    "_apply_projected_inventory_policy",
    "_extend_projected_formula_periods",
    "_inventory_is_material",
    "_latest_positive_computed_value",
]
