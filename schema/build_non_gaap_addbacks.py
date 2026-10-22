"""Projected non-GAAP add-back seed helpers for schema build orchestration."""

from __future__ import annotations

from collections.abc import Iterable
import math

from typing import Any, Literal

from industry_slice_core.industry_types import Unit

from .build_value_writers import _write_default_unless_authored
from .dependency_graph import DependencyGraph
from .handle import ComputedValues
from .models import (
    FinancialModel,
    FormulaDerivation,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
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
_PROJECTED_SBC_ALLOCATED_TOTAL_ID = "tpl.a.stock_based_compensation.total"
# House revenue ratios held on their own input rows (W1), never as formula
# literals: written ``default`` unless an authored row holds the rate.
_PROJECTED_SBC_REVENUE_RATE_ID = (
    "tpl.a.stock_based_compensation.stock_based_compensation_pct_revenue"
)
_PROJECTED_DEPRECIATION_RATE_ID = "tpl.a.depreciation_amortization.depreciation_pct_revenue"
_PROJECTED_DA_REVENUE_RATE_ID = (
    "tpl.a.depreciation_amortization.depreciation_and_amortization_pct_revenue"
)
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

_NonGaapFallbackStrategy = Literal[
    "revenue_ratio",
    "sbc_allocation",
    "acquired_intangible_roll_forward",
]





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


def _computed_model_values(model: FinancialModel) -> ComputedValues:
    dependency_graph_cls = DependencyGraph
    item_type = ItemType
    graph = dependency_graph_cls()
    graph.build(model)
    derived_ids = {
        item.id
        for item in model._index.values()
        if item.item_type == item_type.derived
    }
    return graph.compute({}, recompute=derived_ids)


def _computed_value(
    computed_values: ComputedValues,
    item_id: str,
    period: int,
) -> float | None:
    coerce_optional_float = _coerce_optional_float
    value = computed_values.get(item_id, {}).get(int(period))
    return coerce_optional_float(value)


def _latest_ratio_basis_from_computed_values(
    computed_values: ComputedValues,
    numerator_id: str,
    denominator_id: str,
    periods: Iterable[int],
) -> tuple[int, float] | None:
    """Return the latest period with a positive finite ratio, and that ratio."""

    computed_value = _computed_value
    for period in sorted({int(period) for period in periods}, reverse=True):
        numerator = computed_value(computed_values, numerator_id, period)
        denominator = computed_value(computed_values, denominator_id, period)
        if numerator is None or denominator is None or denominator <= 0:
            continue
        ratio = numerator / denominator
        if math.isfinite(ratio) and ratio > 0:
            return period, ratio
    return None


def _latest_ratio_from_computed_values(
    computed_values: ComputedValues,
    numerator_id: str,
    denominator_id: str,
    periods: Iterable[int],
) -> float | None:
    basis = _latest_ratio_basis_from_computed_values(
        computed_values, numerator_id, denominator_id, periods
    )
    return None if basis is None else basis[1]


def _latest_intangible_runoff_basis_from_computed_values(
    computed_values: ComputedValues,
    intangible_assets_net_id: str,
    revenue_item_id: str,
    periods: Iterable[int],
    *,
    cap_ratio: float | None = None,
) -> tuple[int, int, float] | None:
    """Return the latest (opening period, period, runoff/revenue ratio)."""

    computed_value = _computed_value
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
            return previous_period, period, ratio
    return None


def _ratio_expr(numerator_id: str, denominator_id: str, period: int) -> dict[str, Any]:
    """One reported period's ratio, operands held at their absolute period."""

    return {
        "op": "/",
        "left": {"id": numerator_id, "period": period},
        "right": {"id": denominator_id, "period": period},
    }


def _placed(expr: Any, positions: dict[int, int], position: int) -> Any:
    """Place absolute-period operands as refs offset from ``position``."""

    if isinstance(expr, dict):
        if "period" in expr:
            return {"id": expr["id"], "t": positions[expr["period"]] - position}
        return {key: _placed(value, positions, position) for key, value in expr.items()}
    if isinstance(expr, list):
        return [_placed(value, positions, position) for value in expr]
    return expr


def _missing_projection_periods(
    computed_values: ComputedValues,
    item_id: str,
    projection_periods: Iterable[int],
) -> list[int]:
    computed_value = _computed_value
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
    """Write a house value onto a projection driver row, kind ``default``.

    The caller supplies only periods no authored row holds; each lands as
    ``ValueCell(provenance=default, note=note)`` beside its constant override,
    so ``judgment_absent_default_used`` reports the row.
    """

    if not values_by_period:
        return False
    try:
        item_obj = model.get_item(item_id)
    except KeyError:
        return False
    value_series_cls = ValueSeries
    value_cell_cls = ValueCell
    value_provenance = ValueProvenance
    formula_spec_cls = FormulaSpec
    formula_type = FormulaType
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
            provenance=value_provenance.default,
            note=note,
        )
        item_obj.overrides[int(period)] = formula_spec_cls(
            type=formula_type.constant,
            params={"value": float(value)},
            note=note,
        )
    return True


_FALLBACK_NOTE_PREFIX = "build fallback:"


def _fallback_derivation(
    strategy: _NonGaapFallbackStrategy,
    dependencies: Iterable[str],
) -> FormulaDerivation:
    derivation_cls = FormulaDerivation
    return derivation_cls(
        owner="build_non_gaap_addbacks",
        strategy=strategy,
        dependencies=tuple(dict.fromkeys(str(item_id) for item_id in dependencies)),
    )


def _is_owned_fallback(spec: FormulaSpec | None) -> bool:
    """Return whether a formula belongs to this module, including legacy payloads."""

    if spec is None:
        return False
    derivation = getattr(spec, "derivation", None)
    if getattr(derivation, "owner", None) == "build_non_gaap_addbacks":
        return True
    return str(getattr(spec, "note", "") or "").startswith(_FALLBACK_NOTE_PREFIX)


def _set_projection_formula_values(
    model: FinancialModel,
    item_id: str,
    values_by_period: dict[int, float],
    *,
    formulas_by_period: dict[int, FormulaSpec],
) -> bool:
    """Install live formulas without a second, potentially stale numeric owner."""

    writable_periods = sorted(set(values_by_period) & set(formulas_by_period))
    if not writable_periods:
        return False
    try:
        item_obj = model.get_item(item_id)
    except KeyError:
        return False
    if item_obj.overrides is None:
        item_obj.overrides = {}
    if item_obj.formula_periods is not None:
        item_obj.formula_periods = sorted(
            {int(period) for period in item_obj.formula_periods} | set(writable_periods)
        )
    for period in writable_periods:
        if item_obj.values is not None:
            item_obj.values.values.pop(int(period), None)
        item_obj.overrides[int(period)] = formulas_by_period[period]
    return True


def _set_projection_revenue_driver_values(
    model: FinancialModel,
    item_id: str,
    values_by_period: dict[int, float],
    *,
    revenue_item_id: str,
    ratio: float,
    rate_item_id: str,
    rate_label: str,
    note: str,
) -> bool:
    """Seed a derived row with a live ``revenue * rate`` driver override.

    Writes a snapshot value cell (the build-time ratio*revenue product) plus a
    per-period driver override that references the revenue row and the rate
    row ``rate_item_id``, so the value tracks revenue on every recompute. The
    house ratio is a typed parameter, not a formula literal: it lands
    ``default`` on the rate row (``revenue_ratio``) unless an authored row
    holds that rate. ``item_id`` must be a derived row.
    """

    if not values_by_period:
        return False
    try:
        item_obj = model.get_item(item_id)
    except KeyError:
        return False
    if not _bind_default_rate_row(
        model,
        rate_item_id=rate_item_id,
        anchor_item_id=item_id,
        label=rate_label,
        ratio=ratio,
        periods=values_by_period,
        strategy="revenue_ratio",
    ):
        return False
    value_series_cls = ValueSeries
    value_cell_cls = ValueCell
    value_provenance = ValueProvenance
    formula_spec_cls = FormulaSpec
    formula_type = FormulaType
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
            params={"base": {"id": revenue_item_id}, "rate": {"id": rate_item_id}},
            note=note,
            derivation=_fallback_derivation(
                "revenue_ratio", [revenue_item_id, rate_item_id]
            ),
        )
    return True


def _bind_default_rate_row(
    model: FinancialModel,
    *,
    rate_item_id: str,
    anchor_item_id: str,
    label: str,
    ratio: float,
    periods: Iterable[int],
    strategy: _NonGaapFallbackStrategy,
) -> bool:
    """Hold a house ratio on its own input row, written ``default``.

    The row is inserted directly below ``anchor_item_id`` unless it already
    exists; a period an authored row lowered keeps its ``input`` rate.
    """

    from .build_semantic_rows import _insert_line_item_after

    try:
        rate = model.get_item(rate_item_id)
    except KeyError:
        try:
            model.get_item(anchor_item_id)
        except KeyError:
            return False
        rate = _insert_line_item_after(
            model,
            anchor_item_id=anchor_item_id,
            new_item=LineItem(
                id=rate_item_id,
                label=label,
                row=0,
                item_type=ItemType.input,
                unit=Unit.percentage,
                model_scale="units",
            ),
        )
    written = _write_default_unless_authored(
        rate,
        periods,
        ratio,
        owner="build_non_gaap_addbacks",
        strategy=strategy,
    )
    rate.formula_periods = sorted(set(rate.formula_periods or ()) | set(written))
    return True


def _project_unallocated_sbc_aggregate(
    model: FinancialModel,
    periods: Iterable[int],
    *,
    ratio: float,
    revenue_item_id: str,
    sbc_total_id: str,
    unallocated_ids: Iterable[str],
) -> bool:
    """Project aggregate SBC at its revenue rate, its line split unallocated.

    D24: the issuer disclosed no SBC line split and no authored ``sbc_pct``
    row set one, so the component lanes, their rates and their sum carry no
    value in ``periods``; the aggregate is revenue times the rate on
    ``_PROJECTED_SBC_REVENUE_RATE_ID``.
    """

    requested = sorted({int(period) for period in periods})
    try:
        sbc_total = model.get_item(sbc_total_id)
    except KeyError:
        return False
    if not _bind_default_rate_row(
        model,
        rate_item_id=_PROJECTED_SBC_REVENUE_RATE_ID,
        anchor_item_id=sbc_total_id,
        label=" Stock-based compensation as % of revenue",
        ratio=ratio,
        periods=requested,
        strategy="sbc_allocation",
    ):
        return False
    if sbc_total.overrides is None:
        sbc_total.overrides = {}
    for period in requested:
        sbc_total.overrides[period] = FormulaSpec(
            type=FormulaType.driver,
            subtype="base_x_rate",
            params={
                "base": {"id": revenue_item_id},
                "rate": {"id": _PROJECTED_SBC_REVENUE_RATE_ID},
            },
            note="aggregate SBC at its revenue rate; line split unallocated (D24)",
            derivation=_fallback_derivation(
                "sbc_allocation", [revenue_item_id, _PROJECTED_SBC_REVENUE_RATE_ID]
            ),
        )
    if sbc_total.formula_periods is not None:
        sbc_total.formula_periods = sorted(set(sbc_total.formula_periods) | set(requested))
    all_periods = [
        *(int(period) for period in model.time_structure.historical_periods),
        *(int(period) for period in model.time_structure.projection_periods),
    ]
    for item_id in unallocated_ids:
        try:
            item = model.get_item(item_id)
        except KeyError:
            continue
        current = all_periods if item.formula_periods is None else item.formula_periods
        item.formula_periods = sorted(
            {int(period) for period in current} - set(requested)
        )
        # A carry override (the template's first-projection rate carry)
        # would otherwise still render the line in these periods.
        for period in requested:
            if item.overrides is not None:
                item.overrides.pop(period, None)
            if item.values is not None:
                item.values.values.pop(period, None)
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

    formula_type = FormulaType
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
        if spec.type == formula_type.constant and _is_owned_fallback(spec):
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
            is_own_fallback = _is_owned_fallback(spec)
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
    computed_values: ComputedValues,
    item_id: str,
    periods: Iterable[int],
) -> float | None:
    computed_value = _computed_value
    for period in sorted({int(period) for period in periods}, reverse=True):
        value = computed_value(computed_values, item_id, period)
        if value is not None and value > 0:
            return value
    return None


def _project_acquired_intangible_amortization_values(
    computed_values: ComputedValues,
    projection_periods: Iterable[int],
    *,
    revenue_item_id: str,
    intangible_additions_id: str,
    amortization_revenue_ratio: float,
    beginning_balance: float | None,
) -> dict[int, float]:
    computed_value = _computed_value
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
            additions = computed_value(computed_values, intangible_additions_id, period)
            if additions is None:
                continue
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

    computed_model_values = _computed_model_values
    missing_projection_periods = _missing_projection_periods
    latest_ratio_from_computed_values = _latest_ratio_from_computed_values
    computed_value = _computed_value
    set_projection_formula_values = _set_projection_formula_values
    formula_spec_cls = FormulaSpec
    formula_type = FormulaType

    revenue_item_id = _REVENUE_ITEM_ID
    projected_depreciation_id = _PROJECTED_DEPRECIATION_ID
    projected_da_total_id = _PROJECTED_DA_TOTAL_ID
    projected_da_rate_id = _PROJECTED_DA_RATE_ID
    projected_da_base_id = _PROJECTED_DA_BASE_ID
    projected_sbc_total_id = _PROJECTED_SBC_TOTAL_ID
    projected_sbc_rate_ids = _PROJECTED_SBC_RATE_IDS
    projected_sbc_component_ids = _PROJECTED_SBC_COMPONENT_IDS
    fm_acquired_intangible_amortization_id = _FM_ACQUIRED_INTANGIBLE_AMORTIZATION_ID
    tax_acquired_intangible_amortization_id = _TAX_ACQUIRED_INTANGIBLE_AMORTIZATION_ID
    projected_acquired_intangible_amortization_id = _PROJECTED_ACQUIRED_INTANGIBLE_AMORTIZATION_ID
    projected_acquired_intangible_expense_id = _PROJECTED_ACQUIRED_INTANGIBLE_EXPENSE_ID
    intangible_assets_net_id = _INTANGIBLE_ASSETS_NET_ID
    intangible_additions_id = _INTANGIBLE_ADDITIONS_ID
    latest_positive_balance_from_computed_values = _latest_positive_balance_from_computed_values
    project_acquired_intangible_amortization_values = _project_acquired_intangible_amortization_values
    set_projection_revenue_driver_values = _set_projection_revenue_driver_values
    stale_fallback_periods = _stale_fallback_periods
    clear_fallback_overrides = _clear_fallback_overrides
    drop_protected_periods = _drop_protected_periods

    # Refresh-on-rebuild + legacy heal: clear this module's own stale fallback constants
    # (build-fallback-noted constant overrides) on every fallback TARGET row so the
    # seeding below re-derives them from current model values. Scoped by target id, so the
    # working-capital inventory fallbacks (same note prefix, different ids) are untouched.
    # Intangible stale periods are captured before clearing because that path's trigger is
    # the FM bridge, not the target rows it writes (Codex r3 F2).
    non_sbc_fallback_target_ids = [
        projected_da_total_id,
        projected_da_rate_id,
        projected_depreciation_id,
        projected_acquired_intangible_amortization_id,
        projected_acquired_intangible_expense_id,
    ]
    stale_intangible_periods = sorted(
        set(stale_fallback_periods(model, projected_acquired_intangible_amortization_id, projection_periods))
        | set(stale_fallback_periods(model, projected_acquired_intangible_expense_id, projection_periods))
    )
    for target_id in [*non_sbc_fallback_target_ids, *projected_sbc_rate_ids]:
        stale_periods = stale_fallback_periods(model, target_id, projection_periods)
        if stale_periods:
            clear_fallback_overrides(model, target_id, stale_periods)

    seeded: list[str] = []
    skipped: list[dict[str, Any]] = []

    computed_values = computed_model_values(model)
    # D24: SBC is allocated across lines only as the issuer discloses it or an
    # authored ``sbc_pct`` row sets the component rates; it is never spread
    # evenly. A projection period with neither leaves the lines unallocated and
    # projects the aggregate alone, at the latest historical SBC/revenue rate
    # held on its own row (``default``; ``sbc_allocation``).
    unallocated_sbc_periods = sorted(
        {
            period
            for component_id in projected_sbc_component_ids
            for period in missing_projection_periods(
                computed_values, component_id, projection_periods
            )
        }
    )
    if unallocated_sbc_periods:
        sbc_revenue_ratio = latest_ratio_from_computed_values(
            computed_values,
            projected_sbc_total_id,
            revenue_item_id,
            historical_periods,
        )
        if sbc_revenue_ratio is not None and _project_unallocated_sbc_aggregate(
            model,
            unallocated_sbc_periods,
            ratio=sbc_revenue_ratio,
            revenue_item_id=revenue_item_id,
            sbc_total_id=projected_sbc_total_id,
            unallocated_ids=(
                *projected_sbc_rate_ids,
                *projected_sbc_component_ids,
                _PROJECTED_SBC_ALLOCATED_TOTAL_ID,
            ),
        ):
            seeded.append(projected_sbc_total_id)
        else:
            skipped.append(
                {
                    "item_id": projected_sbc_total_id,
                    "reason": "insufficient_history_or_projection_base",
                }
            )

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
            rate_item_id=_PROJECTED_DEPRECIATION_RATE_ID,
            rate_label="  Depreciation as % of revenue",
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
                rate_item_id=_PROJECTED_DA_REVENUE_RATE_ID,
                rate_label="  Depreciation and amortization as % of revenue",
                note="build fallback: latest historical total D&A/revenue ratio applied to projected revenue",
            ):
                seeded.append(projected_da_total_id)
            else:
                skipped.append({"item_id": projected_depreciation_id, "reason": "insufficient_history_or_projection_base"})

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
        # Each selection keeps the expression its ratio was computed from, with
        # operands held at their absolute reported periods until placed.
        acquired_intangible_basis: tuple[float, dict[str, Any]] | None = None
        source_note = "latest historical acquired-intangible amortization/revenue ratio applied to projected revenue"
        for numerator_id in (
            fm_acquired_intangible_amortization_id,
            tax_acquired_intangible_amortization_id,
        ):
            direct_basis = _latest_ratio_basis_from_computed_values(
                computed_values,
                numerator_id,
                revenue_item_id,
                historical_periods,
            )
            if direct_basis is not None:
                basis_period, ratio = direct_basis
                acquired_intangible_basis = (
                    ratio,
                    _ratio_expr(numerator_id, revenue_item_id, basis_period),
                )
                break
        da_revenue_cap_basis = _latest_ratio_basis_from_computed_values(
            computed_values,
            projected_da_total_id,
            revenue_item_id,
            historical_periods,
        )
        da_revenue_cap = None if da_revenue_cap_basis is None else da_revenue_cap_basis[1]
        if acquired_intangible_basis is None:
            runoff_basis = _latest_intangible_runoff_basis_from_computed_values(
                computed_values,
                intangible_assets_net_id,
                revenue_item_id,
                historical_periods,
                cap_ratio=da_revenue_cap,
            )
            if runoff_basis is not None:
                opening_period, runoff_period, ratio = runoff_basis
                runoff_expr: dict[str, Any] = {
                    "op": "/",
                    "left": {
                        "op": "-",
                        "left": {"id": intangible_assets_net_id, "period": opening_period},
                        "right": {"id": intangible_assets_net_id, "period": runoff_period},
                    },
                    "right": {"id": revenue_item_id, "period": runoff_period},
                }
                if da_revenue_cap_basis is not None and da_revenue_cap_basis[1] > 0:
                    runoff_expr = {
                        "op": "MIN",
                        "args": [
                            runoff_expr,
                            _ratio_expr(
                                projected_da_total_id,
                                revenue_item_id,
                                da_revenue_cap_basis[0],
                            ),
                        ],
                    }
                acquired_intangible_basis = (ratio, runoff_expr)
            source_note = "latest historical net-intangible runoff/revenue ratio applied to projected revenue"
        acquired_intangible_ratio = (
            None if acquired_intangible_basis is None else acquired_intangible_basis[0]
        )

        beginning_balance = latest_positive_balance_from_computed_values(
            computed_values,
            intangible_assets_net_id,
            historical_periods,
        )
        beginning_balance_period = next(
            (
                int(period)
                for period in sorted({int(period) for period in historical_periods}, reverse=True)
                if (balance := computed_value(computed_values, intangible_assets_net_id, int(period)))
                is not None
                and balance > 0
            ),
            None,
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
        intangible_derivation = _fallback_derivation(
            "acquired_intangible_roll_forward",
            [revenue_item_id, intangible_assets_net_id, intangible_additions_id],
        )
        ordered_periods = [
            *sorted({int(period) for period in historical_periods}),
            *sorted({int(period) for period in projection_periods}),
        ]
        period_positions = {period: index for index, period in enumerate(ordered_periods)}

        def intangible_formulas_for(target_id: str) -> dict[int, FormulaSpec]:
            if acquired_intangible_basis is None or beginning_balance_period is None:
                return {}
            ratio_value, ratio_expr = acquired_intangible_basis
            formulas: dict[int, FormulaSpec] = {}
            projection_start = len(sorted({int(period) for period in historical_periods}))
            for period in values_by_period:
                position = period_positions.get(int(period))
                anchor_position = period_positions.get(beginning_balance_period)
                if position is None or anchor_position is None:
                    continue
                projection_offset = position - projection_start
                additions = [
                    {
                        "op": "IFERROR",
                        "expr": {"id": intangible_additions_id, "t": offset},
                        "fallback": 0.0,
                    }
                    for offset in range(-projection_offset, 1)
                ]
                prior_amortization = [
                    {
                        "op": "NEG",
                        "arg": {
                            "op": "IFERROR",
                            "expr": {"id": target_id, "t": offset},
                            "fallback": 0.0,
                        },
                    }
                    for offset in range(-projection_offset, 0)
                ]
                available_balance = {
                    "op": "+",
                    "args": [
                        {
                            "id": intangible_assets_net_id,
                            "t": anchor_position - position,
                        },
                        *additions,
                        *prior_amortization,
                    ],
                }
                formulas[int(period)] = formula_spec_cls(
                    type=formula_type.arithmetic,
                    subtype="capped_acquired_intangible_amortization",
                    params={
                        "expr": {
                            "op": "MIN",
                            "args": [
                                {
                                    "op": "*",
                                    "args": [
                                        float(ratio_value),
                                        {"id": revenue_item_id},
                                    ],
                                },
                                {"op": "MAX", "args": [0.0, available_balance]},
                            ],
                        },
                        # The ratio literal stays; its reported-year operands
                        # ride beside it, as a constant keeps its expression.
                        "lineage": {
                            "value": float(ratio_value),
                            "expr": _placed(ratio_expr, period_positions, position),
                        },
                    },
                    note=note,
                    derivation=intangible_derivation,
                )
            return formulas

        amortization_formulas = intangible_formulas_for(
            projected_acquired_intangible_amortization_id
        )
        if set_projection_formula_values(
            model,
            projected_acquired_intangible_amortization_id,
            amortization_values,
            formulas_by_period={
                period: amortization_formulas[period]
                for period in amortization_values
                if period in amortization_formulas
            },
        ):
            seeded.append(projected_acquired_intangible_amortization_id)
            seeded_acquired_intangible = True
        expense_values = drop_protected_periods(
            model, projected_acquired_intangible_expense_id, values_by_period
        )
        expense_formulas = intangible_formulas_for(projected_acquired_intangible_expense_id)
        if set_projection_formula_values(
            model,
            projected_acquired_intangible_expense_id,
            expense_values,
            formulas_by_period={
                period: expense_formulas[period]
                for period in expense_values
                if period in expense_formulas
            },
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
    "_latest_positive_balance_from_computed_values",
    "_latest_ratio_from_computed_values",
    "_missing_projection_periods",
    "_project_acquired_intangible_amortization_values",
    "_seed_projected_non_gaap_addbacks",
    "_set_projection_input_values",
    "_set_projection_formula_values",
    "_set_projection_revenue_driver_values",
    "_stale_fallback_periods",
]
