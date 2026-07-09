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
_FM_ACQUIRED_INTANGIBLE_AMORTIZATION_ID = "tpl.fm.adjusted_earnings.amortization_of_acquired_intangibles"
_TAX_ACQUIRED_INTANGIBLE_AMORTIZATION_ID = "tpl.a.tax_net_income.amortization_of_acquired_intangibles"
_PROJECTED_ACQUIRED_INTANGIBLE_AMORTIZATION_ID = (
    "tpl.a.intangibles_goodwill.less_amortization_for_acquired_intangible_assets"
)
_PROJECTED_ACQUIRED_INTANGIBLE_EXPENSE_ID = (
    "tpl.a.depreciation_amortization.amortization_expense_for_acquired_intangible_assets"
)
_INTANGIBLE_ASSETS_NET_ID = "tpl.a.intangibles_goodwill.intangible_assets_net"
_INTANGIBLE_ADDITIONS_ID = "tpl.a.intangibles_goodwill.add_acquisitions_of_businesses"


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


def _latest_intangible_runoff_ratio_from_computed_values(
    computed_values: dict[str, dict[int, float]],
    intangible_assets_net_id: str,
    revenue_item_id: str,
    periods: Iterable[int],
    *,
    cap_ratio: float | None = None,
) -> float | None:
    computed_value = _parent_attr("_computed_value", _computed_value)
    ordered_periods = sorted({int(period) for period in periods})
    for previous_period, period in reversed(list(zip(ordered_periods, ordered_periods[1:]))):
        previous_intangibles = computed_value(computed_values, intangible_assets_net_id, previous_period)
        current_intangibles = computed_value(computed_values, intangible_assets_net_id, period)
        revenue = computed_value(computed_values, revenue_item_id, period)
        if (
            previous_intangibles is None
            or current_intangibles is None
            or revenue is None
            or previous_intangibles <= 0
            or revenue <= 0
        ):
            continue
        runoff = previous_intangibles - current_intangibles
        if runoff <= 0:
            continue
        ratio = runoff / revenue
        if cap_ratio is not None and cap_ratio > 0:
            ratio = min(ratio, cap_ratio)
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


_FALLBACK_NOTE_PREFIX = "build fallback:"


def _set_projection_revenue_driver_values(
    model: FinancialModel,
    item_id: str,
    values_by_period: dict[int, float],
    *,
    revenue_item_id: str,
    ratio: float,
    note: str,
) -> bool:
    """Seed a derived row with a live ``revenue * ratio`` driver override.

    Writes a snapshot value cell (the build-time ratio*revenue product) plus a
    per-period driver override that references the revenue row, so the value tracks
    revenue on every recompute instead of freezing as a constant. ``item_id`` must be a
    derived row; the generated code emits compute functions for derived rows, so no
    codegen change is needed.
    """

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
            type=formula_type.driver,
            subtype="base_x_rate",
            params={"base": {"id": revenue_item_id}, "rate": float(ratio)},
            note=note,
        )
    return True


def _stale_fallback_periods(
    model: FinancialModel,
    item_id: str,
    projection_periods: Iterable[int],
) -> list[int]:
    """Projection periods where item_id holds a module-authored fallback constant.

    Identifies overrides this module wrote (``FormulaType.constant`` with a
    ``build fallback:`` note) so a rebuild can re-derive them from current model values.
    Agent-authored repairs (other notes) and human overrides (no note) never match.
    """

    formula_type = _parent_attr("FormulaType", FormulaType)
    try:
        item_obj = model.get_item(item_id)
    except KeyError:
        return []
    if not item_obj.overrides:
        return []
    stale: list[int] = []
    for period in {int(period) for period in projection_periods}:
        spec = item_obj.overrides.get(period)
        if spec is None:
            continue
        if spec.type == formula_type.constant and str(getattr(spec, "note", "") or "").startswith(
            _FALLBACK_NOTE_PREFIX
        ):
            stale.append(period)
    return sorted(stale)


def _drop_protected_periods(
    model: FinancialModel,
    item_id: str,
    values_by_period: dict[int, float],
) -> dict[int, float]:
    """Drop periods where item_id already holds a non-fallback override.

    Agent-authored (other-note) and human (no-note) overrides on a fallback target must
    never be clobbered by seeding — only this module's own ``build fallback:`` overrides
    are refreshable. Returns a filtered copy.
    """

    try:
        item_obj = model.get_item(item_id)
    except KeyError:
        return values_by_period
    if not item_obj.overrides:
        return values_by_period
    filtered: dict[int, float] = {}
    for period, value in values_by_period.items():
        spec = item_obj.overrides.get(int(period))
        if spec is not None:
            # Own fallback overrides (this module's, "build fallback:" note — constant OR
            # the new revenue driver) are refreshable; anything else is agent/human and
            # must be preserved.
            is_own_fallback = str(getattr(spec, "note", "") or "").startswith(_FALLBACK_NOTE_PREFIX)
            if not is_own_fallback:
                continue  # protected: agent/human override — do not overwrite
        filtered[int(period)] = value
    return filtered


def _clear_fallback_overrides(
    model: FinancialModel,
    item_id: str,
    periods: Iterable[int],
) -> None:
    """Remove this module's fallback override + snapshot cell for the given periods."""

    try:
        item_obj = model.get_item(item_id)
    except KeyError:
        return
    for period in {int(period) for period in periods}:
        if item_obj.overrides is not None:
            item_obj.overrides.pop(period, None)
        if item_obj.values is not None:
            item_obj.values.values.pop(period, None)


def _latest_positive_balance_from_computed_values(
    computed_values: dict[str, dict[int, float]],
    item_id: str,
    periods: Iterable[int],
) -> float | None:
    computed_value = _parent_attr("_computed_value", _computed_value)
    for period in sorted({int(period) for period in periods}, reverse=True):
        value = computed_value(computed_values, item_id, period)
        if value is not None and value > 0:
            return value
    return None


def _project_acquired_intangible_amortization_values(
    computed_values: dict[str, dict[int, float]],
    projection_periods: Iterable[int],
    *,
    revenue_item_id: str,
    intangible_additions_id: str,
    amortization_revenue_ratio: float,
    beginning_balance: float | None,
) -> dict[int, float]:
    computed_value = _parent_attr("_computed_value", _computed_value)
    values_by_period: dict[int, float] = {}
    remaining_balance = beginning_balance
    for period in sorted({int(period) for period in projection_periods}):
        revenue = computed_value(computed_values, revenue_item_id, period)
        if revenue is None:
            continue
        raw_value = amortization_revenue_ratio * revenue
        if not math.isfinite(raw_value) or raw_value < 0:
            continue
        value = raw_value
        if remaining_balance is not None:
            additions = computed_value(computed_values, intangible_additions_id, period) or 0.0
            if additions > 0:
                remaining_balance += additions
            value = min(value, max(remaining_balance, 0.0))
            remaining_balance = max(remaining_balance - value, 0.0)
        if math.isfinite(value):
            values_by_period[int(period)] = float(value)
    return values_by_period


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
    projected_da_total_id = _parent_attr("_PROJECTED_DA_TOTAL_ID", _PROJECTED_DA_TOTAL_ID)
    projected_da_rate_id = _parent_attr("_PROJECTED_DA_RATE_ID", _PROJECTED_DA_RATE_ID)
    projected_da_base_id = _parent_attr("_PROJECTED_DA_BASE_ID", _PROJECTED_DA_BASE_ID)
    projected_sbc_total_id = _parent_attr("_PROJECTED_SBC_TOTAL_ID", _PROJECTED_SBC_TOTAL_ID)
    projected_sbc_rate_ids = _parent_attr("_PROJECTED_SBC_RATE_IDS", _PROJECTED_SBC_RATE_IDS)
    projected_sbc_component_ids = _parent_attr(
        "_PROJECTED_SBC_COMPONENT_IDS",
        _PROJECTED_SBC_COMPONENT_IDS,
    )
    projected_sbc_base_ids = _parent_attr("_PROJECTED_SBC_BASE_IDS", _PROJECTED_SBC_BASE_IDS)
    fm_acquired_intangible_amortization_id = _parent_attr(
        "_FM_ACQUIRED_INTANGIBLE_AMORTIZATION_ID",
        _FM_ACQUIRED_INTANGIBLE_AMORTIZATION_ID,
    )
    tax_acquired_intangible_amortization_id = _parent_attr(
        "_TAX_ACQUIRED_INTANGIBLE_AMORTIZATION_ID",
        _TAX_ACQUIRED_INTANGIBLE_AMORTIZATION_ID,
    )
    projected_acquired_intangible_amortization_id = _parent_attr(
        "_PROJECTED_ACQUIRED_INTANGIBLE_AMORTIZATION_ID",
        _PROJECTED_ACQUIRED_INTANGIBLE_AMORTIZATION_ID,
    )
    projected_acquired_intangible_expense_id = _parent_attr(
        "_PROJECTED_ACQUIRED_INTANGIBLE_EXPENSE_ID",
        _PROJECTED_ACQUIRED_INTANGIBLE_EXPENSE_ID,
    )
    intangible_assets_net_id = _parent_attr("_INTANGIBLE_ASSETS_NET_ID", _INTANGIBLE_ASSETS_NET_ID)
    intangible_additions_id = _parent_attr("_INTANGIBLE_ADDITIONS_ID", _INTANGIBLE_ADDITIONS_ID)
    latest_intangible_runoff_ratio_from_computed_values = _parent_attr(
        "_latest_intangible_runoff_ratio_from_computed_values",
        _latest_intangible_runoff_ratio_from_computed_values,
    )
    latest_positive_balance_from_computed_values = _parent_attr(
        "_latest_positive_balance_from_computed_values",
        _latest_positive_balance_from_computed_values,
    )
    project_acquired_intangible_amortization_values = _parent_attr(
        "_project_acquired_intangible_amortization_values",
        _project_acquired_intangible_amortization_values,
    )
    set_projection_revenue_driver_values = _parent_attr(
        "_set_projection_revenue_driver_values",
        _set_projection_revenue_driver_values,
    )
    stale_fallback_periods = _parent_attr("_stale_fallback_periods", _stale_fallback_periods)
    clear_fallback_overrides = _parent_attr("_clear_fallback_overrides", _clear_fallback_overrides)
    drop_protected_periods = _parent_attr("_drop_protected_periods", _drop_protected_periods)

    # Refresh-on-rebuild + legacy heal: clear this module's own stale fallback constants
    # (build-fallback-noted constant overrides) on every fallback TARGET row so the
    # seeding below re-derives them from current model values. Scoped by target id, so the
    # working-capital inventory fallbacks (same note prefix, different ids) are untouched.
    # Intangible stale periods are captured before clearing because that path's trigger is
    # the FM bridge, not the target rows it writes (Codex r3 F2).
    fallback_target_ids = [
        projected_da_total_id,
        projected_da_rate_id,
        projected_depreciation_id,
        *projected_sbc_rate_ids,
        projected_acquired_intangible_amortization_id,
        projected_acquired_intangible_expense_id,
    ]
    stale_intangible_periods = sorted(
        set(stale_fallback_periods(model, projected_acquired_intangible_amortization_id, projection_periods))
        | set(stale_fallback_periods(model, projected_acquired_intangible_expense_id, projection_periods))
    )
    for target_id in fallback_target_ids:
        stale_periods = stale_fallback_periods(model, target_id, projection_periods)
        if stale_periods:
            clear_fallback_overrides(model, target_id, stale_periods)

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
        # 1st choice: seed the DERIVED depreciation row directly with a live
        # revenue * (historical depreciation/revenue ratio) driver. This is economically
        # identical to the prior rate-row seeding (rate = ratio*revenue/base;
        # depreciation = rate*base = ratio*revenue) but tracks revenue instead of freezing.
        # The base gate is retained purely for coverage parity with the prior behavior —
        # the driver formula does not need the PPE base (Codex r4 F1).
        values_by_period: dict[int, float] = {}
        if depreciation_revenue_ratio is not None:
            for period in missing_da_periods:
                revenue = computed_value(computed_values, revenue_item_id, period)
                base = computed_value(computed_values, projected_da_base_id, period)
                if revenue is None or base is None or base <= 0:
                    continue
                value = depreciation_revenue_ratio * revenue
                if math.isfinite(value) and value > 0:
                    values_by_period[int(period)] = value
        values_by_period = drop_protected_periods(model, projected_depreciation_id, values_by_period)
        if depreciation_revenue_ratio is not None and set_projection_revenue_driver_values(
            model,
            projected_depreciation_id,
            values_by_period,
            revenue_item_id=revenue_item_id,
            ratio=depreciation_revenue_ratio,
            note="build fallback: latest historical depreciation/revenue ratio applied to projected revenue",
        ):
            seeded.append(projected_depreciation_id)
        else:
            # Fall-through: no per-component depreciation seed possible (e.g. missing PPE
            # base) — seed the DERIVED total-D&A row directly with the revenue driver.
            da_revenue_ratio = latest_ratio_from_computed_values(
                computed_values,
                projected_da_total_id,
                revenue_item_id,
                historical_periods,
            )
            values_by_period = {}
            if da_revenue_ratio is not None:
                for period in missing_da_periods:
                    revenue = computed_value(computed_values, revenue_item_id, period)
                    if revenue is None:
                        continue
                    value = da_revenue_ratio * revenue
                    if math.isfinite(value) and value > 0:
                        values_by_period[int(period)] = value
            values_by_period = drop_protected_periods(model, projected_da_total_id, values_by_period)
            if da_revenue_ratio is not None and set_projection_revenue_driver_values(
                model,
                projected_da_total_id,
                values_by_period,
                revenue_item_id=revenue_item_id,
                ratio=da_revenue_ratio,
                note="build fallback: latest historical total D&A/revenue ratio applied to projected revenue",
            ):
                seeded.append(projected_da_total_id)
            else:
                skipped.append({"item_id": projected_depreciation_id, "reason": "insufficient_history_or_projection_base"})

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
            values_by_period = drop_protected_periods(model, item_id, values_by_period)
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

    has_acquired_intangible_bridge = (
        fm_acquired_intangible_amortization_id in model._index
        and projected_acquired_intangible_amortization_id in model._index
    )
    # "needs reseed" = FM-bridge missing (as before) UNION periods whose target rows held
    # a stale fallback constant we just cleared. The trigger checks the bridge but writes
    # the target rows, so the stale set must be folded in explicitly (Codex r3 F2).
    missing_acquired_intangible_periods = (
        sorted(
            set(
                missing_projection_periods(
                    computed_values,
                    fm_acquired_intangible_amortization_id,
                    projection_periods,
                )
            )
            | set(stale_intangible_periods)
        )
        if has_acquired_intangible_bridge
        else []
    )
    if has_acquired_intangible_bridge and missing_acquired_intangible_periods:
        direct_acquired_intangible_ratio = latest_ratio_from_computed_values(
            computed_values,
            fm_acquired_intangible_amortization_id,
            revenue_item_id,
            historical_periods,
        )
        if direct_acquired_intangible_ratio is None:
            direct_acquired_intangible_ratio = latest_ratio_from_computed_values(
                computed_values,
                tax_acquired_intangible_amortization_id,
                revenue_item_id,
                historical_periods,
            )
        da_revenue_cap = latest_ratio_from_computed_values(
            computed_values,
            projected_da_total_id,
            revenue_item_id,
            historical_periods,
        )
        acquired_intangible_ratio = direct_acquired_intangible_ratio
        source_note = "latest historical acquired-intangible amortization/revenue ratio applied to projected revenue"
        if acquired_intangible_ratio is None:
            acquired_intangible_ratio = latest_intangible_runoff_ratio_from_computed_values(
                computed_values,
                intangible_assets_net_id,
                revenue_item_id,
                historical_periods,
                cap_ratio=da_revenue_cap,
            )
            source_note = "latest historical net-intangible runoff/revenue ratio applied to projected revenue"

        beginning_balance = latest_positive_balance_from_computed_values(
            computed_values,
            intangible_assets_net_id,
            historical_periods,
        )
        values_by_period: dict[int, float] = {}
        if acquired_intangible_ratio is not None:
            projected_values = project_acquired_intangible_amortization_values(
                computed_values,
                projection_periods,
                revenue_item_id=revenue_item_id,
                intangible_additions_id=intangible_additions_id,
                amortization_revenue_ratio=acquired_intangible_ratio,
                beginning_balance=beginning_balance,
            )
            values_by_period = {
                int(period): projected_values[int(period)]
                for period in missing_acquired_intangible_periods
                if int(period) in projected_values
            }
        seeded_acquired_intangible = False
        note = f"build fallback: {source_note}, capped at remaining acquired intangible balance"
        amortization_values = drop_protected_periods(
            model, projected_acquired_intangible_amortization_id, values_by_period
        )
        if set_projection_input_values(
            model,
            projected_acquired_intangible_amortization_id,
            amortization_values,
            note=note,
        ):
            seeded.append(projected_acquired_intangible_amortization_id)
            seeded_acquired_intangible = True
        expense_values = drop_protected_periods(
            model, projected_acquired_intangible_expense_id, values_by_period
        )
        if set_projection_input_values(
            model,
            projected_acquired_intangible_expense_id,
            expense_values,
            note=note,
        ):
            seeded.append(projected_acquired_intangible_expense_id)
            seeded_acquired_intangible = True
        if not seeded_acquired_intangible:
            skipped.append(
                {
                    "item_id": projected_acquired_intangible_amortization_id,
                    "reason": "insufficient_history_or_projection_base",
                }
            )

    return {"seeded": seeded, "skipped": skipped}


__all__ = [
    "_FM_ACQUIRED_INTANGIBLE_AMORTIZATION_ID",
    "_INTANGIBLE_ADDITIONS_ID",
    "_INTANGIBLE_ASSETS_NET_ID",
    "_PROJECTED_ACQUIRED_INTANGIBLE_AMORTIZATION_ID",
    "_PROJECTED_ACQUIRED_INTANGIBLE_EXPENSE_ID",
    "_PROJECTED_DA_BASE_ID",
    "_PROJECTED_DA_RATE_ID",
    "_PROJECTED_DA_TOTAL_ID",
    "_PROJECTED_DEPRECIATION_ID",
    "_PROJECTED_SBC_BASE_IDS",
    "_PROJECTED_SBC_COMPONENT_IDS",
    "_PROJECTED_SBC_RATE_IDS",
    "_PROJECTED_SBC_TOTAL_ID",
    "_REVENUE_ITEM_ID",
    "_TAX_ACQUIRED_INTANGIBLE_AMORTIZATION_ID",
    "_clear_fallback_overrides",
    "_drop_protected_periods",
    "_coerce_optional_float",
    "_computed_model_values",
    "_computed_value",
    "_latest_intangible_runoff_ratio_from_computed_values",
    "_latest_positive_balance_from_computed_values",
    "_latest_ratio_from_computed_values",
    "_missing_projection_periods",
    "_project_acquired_intangible_amortization_values",
    "_seed_projected_non_gaap_addbacks",
    "_set_projection_input_values",
    "_set_projection_revenue_driver_values",
    "_stale_fallback_periods",
]
