"""ModelBuildContext entrypoint helpers for schema build orchestration."""

from __future__ import annotations

import math
import logging
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Mapping, TypeGuard, assert_never

from .business_model import (
    BusinessModel,
    _iter_decomposition_nodes,
    assumption_unit_matches,
)
from .driver_resolver import load_scale_correction_rules, resolve_driver_key
from .forecast_control_profile import disclosed_mix_methods_by_projection_key
from .model_build_context import MbcDriverSpec
from industry_slice_core.thesis_shared_sources import ScalarValue
from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ValueCell,
    ValueProvenance,
    ValueSeries,
)
if TYPE_CHECKING:
    from .business_model_compiler import CompiledDriverRegistry
    from .model_kind_capabilities import EquationOwnedControlTreatment


_SCALE_CORRECTION_MIN_RATIO = 1_000.0
_SCALE_CORRECTION_MIN_COMPARATOR_ABS = 1.0


def _all_driver_values_present(
    values: tuple[ScalarValue | None, ...],
) -> TypeGuard[tuple[ScalarValue, ...]]:
    return all(value is not None for value in values)


def _apply_model_spec_assumptions(
    model: FinancialModel,
    assumptions: dict[str, MbcDriverSpec],
    compiled_registry: "CompiledDriverRegistry | None",
    business_model: BusinessModel | None,
    *,
    equation_owned_controls: Mapping[
        str, EquationOwnedControlTreatment
    ] = MappingProxyType({}),
) -> list[dict[str, Any]]:
    """Write every MBC driver into canonical model state before rendering.

    The authored business model supplies each compiled node's stored unit;
    registry ``node_items`` binds that declaration to its resolved model row.

    ``equation_owned_controls`` is the model kind's declared control-key to
    lowering-treatment map (plan §3 row A10): the values of those controls are
    owned by equations, so lowering keeps their typed assumption/driver chain
    in the coverage manifest without replacing the computed row with a
    constant input. A model kind that declares none lowers every control the
    ordinary way.

    A key on a disclosed-mix row (stock-based compensation) records the SBC
    forecast method it carries in the row's lineage (``aggregate_scale`` or
    ``per_line``), so the compile reads the method from the row and scales
    only the ``aggregate_scale`` rows by the disclosed mix. The compiler
    projection sends at most one key per row.
    """

    from .scenario_bridge import normalize_value_for_model

    historical_periods = (
        model.time_structure.historical_periods
        or model.time_structure.historical_years
        or []
    )
    projection_periods = (
        model.time_structure.projection_periods
        or model.time_structure.projection_years
        or []
    )

    coverage: list[dict[str, Any]] = []
    mix_methods = disclosed_mix_methods_by_projection_key()
    for driver_key, driver in assumptions.items():
        normalized_driver_key = str(driver_key or "").strip()
        item_id = resolve_driver_key(
            normalized_driver_key,
            compiled_registry=compiled_registry,
        )
        item = model.get_item(item_id)

        if item.unit is None:
            raise ValueError(
                "incomplete_model_spec: resolved assumption target "
                f"{item_id!r} has no canonical unit"
            )
        node = (
            next(
                (
                    node
                    for segment in business_model.segments
                    for node in _iter_decomposition_nodes(
                        segment.revenue_model.decomposition
                    )
                    if compiled_registry.node_items.get(
                        f"{segment.id}.{node.id}"
                    ) == item_id
                ),
                None,
            )
            if compiled_registry is not None and business_model is not None
            else None
        )
        # Only an originating BM node can authorize its stored legacy spelling.
        unit_matches = (
            assumption_unit_matches(driver.unit, node)
            if node is not None
            else driver.unit == item.unit
        )
        if not unit_matches:
            raise ValueError(
                "incomplete_model_spec: assumption unit conflict for "
                f"{normalized_driver_key!r}: declared {driver.unit.value!r}, "
                f"target {item_id!r} requires {item.unit.value!r}"
            )

        if item.values is None:
            item.values = ValueSeries()

        requested = [int(period) for period in (driver.periods or [])]
        if not requested:
            requested = [int(period) for period in projection_periods]

        # Declared equation-owned controls: their values are owned by
        # equations, so keep the typed assumption/driver chain in the coverage
        # manifest without replacing the computed row with a constant input.
        # Integrity: an equation-owned control is never written as a constant
        # input. This dispatch owns that — every EquationOwnedControlTreatment
        # member has an arm and ``assert_never`` closes it, so a new member
        # without one is a pyright error here and raises before any write,
        # never falling through to the constant lowering below.
        equation_owned_treatment = equation_owned_controls.get(
            normalized_driver_key
        )
        if equation_owned_treatment is not None:
            if equation_owned_treatment == "coverage_only":
                pass
            elif equation_owned_treatment == "carry_forward_basis":
                raw_values = tuple(
                    driver.period_values.get(period)
                    if driver.period_values is not None
                    else driver.value
                    for period in requested
                )
                if not raw_values or not _all_driver_values_present(raw_values):
                    raise ValueError(
                        "equation-owned carry-forward assumption "
                        f"{normalized_driver_key!r} requires every period"
                    )
                normalized_values = tuple(float(value) for value in raw_values)
                if len(set(normalized_values)) != 1:
                    raise ValueError(
                        "equation-owned carry-forward assumption "
                        f"{normalized_driver_key!r} requires a flat authored curve"
                    )
                basis_period = min(requested) - 1
                item.values.values[basis_period] = ValueCell(
                    period=basis_period,
                    value=normalized_values[0],
                    provenance=ValueProvenance.computed,
                )
                if item.overrides is None:
                    item.overrides = {}
                item.overrides[basis_period] = FormulaSpec(
                    type=FormulaType.constant,
                    params={"value": normalized_values[0]},
                    note="filing_ratio_basis_for_equation_owned_control",
                )
                for period in requested:
                    item.values.values.pop(period, None)
                    item.overrides.pop(period, None)
            else:
                assert_never(equation_owned_treatment)
            coverage.append(
                {
                    "canonical_driver_key": normalized_driver_key,
                    "resolved_item_id": item_id,
                    "periods": sorted(set(requested)),
                    "source_kind": "model_spec_assumption",
                    "assumption_id": driver.assumption_id,
                }
            )
            continue

        value_scale = driver.value_scale
        fields_set = (
            getattr(driver, "model_fields_set", None)
            or getattr(driver, "__fields_set__", set())
            or set()
        )
        value_scale_field_present = "value_scale" in fields_set
        value_scale_explicit = value_scale == "display" and value_scale_field_present
        for period in requested:
            raw_value = (
                driver.period_values.get(period)
                if driver.period_values is not None
                else driver.value
            )
            if raw_value is None:
                raise ValueError(
                    f"driver {normalized_driver_key!r} has no value for FY{period}"
                )
            normalized_value = normalize_value_for_model(
                model,
                item_id,
                float(raw_value),
                value_scale=value_scale,
                value_scale_explicit=value_scale_explicit,
                pct_decimal_passthrough=False,
            )
            normalized_value = _maybe_auto_correct_mbc_seed_scale(
                model,
                item,
                driver_key=normalized_driver_key,
                item_id=item_id,
                value=normalized_value,
                value_scale_field_present=value_scale_field_present,
                historical_periods=historical_periods,
            )
            item.values.values[period] = ValueCell(
                period=period,
                value=normalized_value,
                provenance=ValueProvenance.input,
            )
            if item.overrides is None:
                item.overrides = {}
            params: dict[str, Any] = {"value": normalized_value}
            mix_method = mix_methods.get(normalized_driver_key)
            if mix_method is not None:
                control_id, method = mix_method
                params["lineage"] = {
                    "forecast_method": method,
                    "control": control_id,
                }
            item.overrides[period] = FormulaSpec(
                type=FormulaType.constant,
                params=params,
                note="model_spec_assumption",
            )
        if requested:
            coverage.append(
                {
                    "canonical_driver_key": normalized_driver_key,
                    "resolved_item_id": item_id,
                    "periods": sorted(set(requested)),
                    "source_kind": "model_spec_assumption",
                    "assumption_id": driver.assumption_id,
                }
            )
    return coverage


def _maybe_auto_correct_mbc_seed_scale(
    model: FinancialModel,
    item: Any,
    *,
    driver_key: str,
    item_id: str,
    value: Any,
    value_scale_field_present: bool,
    historical_periods: list[int],
) -> Any:
    if _unit_text(getattr(item, "unit", None)) != "dollars":
        return value
    if str(getattr(item, "model_scale", "units") or "units") != "millions":
        return value
    try:
        numeric = float(value)
    except (TypeError, ValueError):
        return value
    if not math.isfinite(numeric):
        return value

    rule = _scale_correction_rule(driver_key, item_id)
    if rule is None:
        return value
    comparator = _latest_historical_value(model, rule["comparator_item_id"], historical_periods)
    if comparator is None or abs(comparator) < _SCALE_CORRECTION_MIN_COMPARATOR_ABS:
        return value
    ratio = abs(numeric) / abs(comparator)
    if ratio < _SCALE_CORRECTION_MIN_RATIO:
        return value

    corrected = numeric / 1_000_000.0
    logging.warning(
        "Auto-scaled MBC driver seed by magnitude from display dollars to model millions "
        "for %s (%s): raw=%s corrected=%s comparator=%s latest_historical=%s "
        "ratio=%s value_scale_present=%s",
        driver_key,
        item_id,
        numeric,
        corrected,
        rule["comparator_item_id"],
        comparator,
        ratio,
        value_scale_field_present,
    )
    return corrected


def _scale_correction_rule(driver_key: str, item_id: str) -> dict[str, str] | None:
    rules = load_scale_correction_rules()
    rule = rules.get(driver_key)
    if rule is not None and rule.get("target_item_id") == item_id:
        return rule
    return next((candidate for candidate in rules.values() if candidate.get("target_item_id") == item_id), None)


def _latest_historical_value(
    model: FinancialModel,
    item_id: str,
    historical_periods: list[int],
) -> float | None:
    try:
        item = model.get_item(item_id)
    except KeyError:
        return None
    values = item.values
    if values is None:
        return None
    available_periods = set(historical_periods).intersection(values.values)
    for period in sorted(available_periods, reverse=True):
        numeric = values.values[period].value
        if numeric is not None and math.isfinite(numeric):
            return numeric
    return None


def _unit_text(value: Any) -> str:
    return str(getattr(value, "value", value) or "").strip()


__all__ = ["_apply_model_spec_assumptions"]
