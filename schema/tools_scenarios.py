"""Scenario execution helpers for schema model tools."""

from __future__ import annotations

import sys
from typing import Any, Dict, List, Literal, Optional

from .analysis import _default_period
from .tools_items import _sample_values, _unknown_item_error
from .tools_periods import _all_periods
from .tools_sensitivity import (
    _SENSITIVITY_RECOMPUTE_POLICIES,
    _apply_scenario_case_selection,
    _compute_scenario_results,
    _scenario_case_recompute_ids,
    _scenario_case_selection_for_overrides,
    _scenario_recompute_ids,
)
from .tools_summary import _find_key_metrics, _formula_type


_PARENT_MODULE = "schema.tools"

_POSITIVE_SCENARIO_OUTPUT_TOKENS = (
    "adjusted_eps",
    "adj_eps",
    "eps",
    "fcf",
    "free_cash_flow",
    "revenue",
    "sales",
    "gross_profit",
    "operating_income",
    "ebit",
    "ebitda",
    "net_income",
    "margin",
)


_NEGATIVE_SCENARIO_OUTPUT_TOKENS = (
    "expense",
    "expenses",
    "cost",
    "costs",
    "cogs",
    "opex",
    "operating_expense",
    "sales_and_marketing",
    "s&m",
    "spend",
    "spending",
)


def _compat(name: str, default: Any) -> Any:
    original = _ORIGINALS.get(name, default)
    parent = sys.modules.get(_PARENT_MODULE)
    if parent is None:
        return default
    value = getattr(parent, name, original)
    return value if value is not original else default


def _is_positive_scenario_output(comparison: Dict[str, Any]) -> bool:
    haystack = " ".join(
        str(comparison.get(key) or "").lower()
        for key in ("id", "label")
    )
    negative_tokens = _compat("_NEGATIVE_SCENARIO_OUTPUT_TOKENS", _NEGATIVE_SCENARIO_OUTPUT_TOKENS)
    positive_tokens = _compat("_POSITIVE_SCENARIO_OUTPUT_TOKENS", _POSITIVE_SCENARIO_OUTPUT_TOKENS)
    if any(token in haystack for token in negative_tokens):
        return False
    return any(token in haystack for token in positive_tokens)


def _scenario_case_direction_warnings(
    *,
    scenario_case_selection: Optional[Dict[str, Any]],
    comparisons: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    if not scenario_case_selection or scenario_case_selection.get("status") != "auto_selected":
        return []
    case = str(scenario_case_selection.get("case") or "").strip().lower()
    if case not in {"bull", "bear"}:
        return []

    selected_periods: set[int] = set()
    case_row_overrides = scenario_case_selection.get("case_row_overrides")
    if isinstance(case_row_overrides, dict):
        for by_period in case_row_overrides.values():
            if not isinstance(by_period, dict):
                continue
            for raw_period in by_period:
                try:
                    selected_periods.add(int(raw_period))
                except (TypeError, ValueError):
                    continue

    warnings: List[Dict[str, Any]] = []
    is_positive_scenario_output = _compat("_is_positive_scenario_output", _is_positive_scenario_output)
    for comparison in comparisons:
        if not is_positive_scenario_output(comparison):
            continue
        base_values = comparison.get("base_values")
        scenario_values = comparison.get("scenario_values")
        if not isinstance(base_values, dict) or not isinstance(scenario_values, dict):
            continue
        wrong_periods: List[int] = []
        for raw_period, base_value in base_values.items():
            scenario_value = scenario_values.get(raw_period)
            if scenario_value is None:
                scenario_value = scenario_values.get(str(raw_period))
            if not isinstance(base_value, (int, float)) or isinstance(base_value, bool):
                continue
            if not isinstance(scenario_value, (int, float)) or isinstance(scenario_value, bool):
                continue
            try:
                period = int(raw_period)
            except (TypeError, ValueError):
                continue
            if selected_periods and period not in selected_periods:
                continue
            delta = float(scenario_value) - float(base_value)
            if case == "bull" and delta < -1e-9:
                wrong_periods.append(period)
            elif case == "bear" and delta > 1e-9:
                wrong_periods.append(period)
        if not wrong_periods:
            continue
        warnings.append(
            {
                "code": "scenario_output_direction_mismatch",
                "kind": "scenario_output_direction_mismatch",
                "case": case,
                "item_id": comparison.get("id"),
                "label": comparison.get("label"),
                "periods": wrong_periods,
                "message": (
                    f"{case} case row readback moved this positive financial output "
                    "opposite the case label for one or more periods; treat as a "
                    "model/scenario bridge quality issue before using the scenario."
                ),
            }
        )
    return warnings


def run_scenario(
    bundle: Any,
    overrides: Dict[str, Dict[int, float]],
    compare_items: Optional[List[str]] = None,
    *,
    recompute_policy: Literal["projection_safe", "legacy_global"] = "projection_safe",
) -> Dict:
    model_obj = bundle.model
    recompute_policies = _compat("_SENSITIVITY_RECOMPUTE_POLICIES", _SENSITIVITY_RECOMPUTE_POLICIES)
    if recompute_policy not in recompute_policies:
        allowed = ", ".join(sorted(recompute_policies))
        raise ValueError(f"recompute_policy must be one of: {allowed}")

    unknown_item_error = _compat("_unknown_item_error", _unknown_item_error)
    normalized: Dict[str, Dict[int, float]] = {}
    for item_id, values in (overrides or {}).items():
        if item_id not in model_obj._index:
            raise unknown_item_error(model_obj, item_id, "override item_id")
        normalized[item_id] = {int(period): float(value) for period, value in values.items()}
    scenario_case_selection = _compat(
        "_scenario_case_selection_for_overrides",
        _scenario_case_selection_for_overrides,
    )(model_obj, normalized)
    normalized = _compat("_apply_scenario_case_selection", _apply_scenario_case_selection)(
        normalized,
        scenario_case_selection,
    )

    if compare_items is None:
        compare_ids = [item.id for item in _compat("_find_key_metrics", _find_key_metrics)(bundle.all_items)]
    else:
        compare_ids = compare_items
        for item_id in compare_ids:
            if item_id not in model_obj._index:
                raise unknown_item_error(model_obj, item_id, "compare item_id")

    recompute_ids = _compat("_scenario_recompute_ids", _scenario_recompute_ids)(
        bundle,
        normalized,
        recompute_policy=recompute_policy,
    )
    recompute_ids |= _compat("_scenario_case_recompute_ids", _scenario_case_recompute_ids)(
        bundle,
        scenario_case_selection,
    )
    propagate_roots = (
        set()
        if scenario_case_selection and scenario_case_selection.get("status") == "auto_selected"
        else None
    )

    scenario_results = _compat("_compute_scenario_results", _compute_scenario_results)(
        bundle,
        normalized,
        recompute_ids,
        recompute_policy=recompute_policy,
        propagate_roots=propagate_roots,
    )
    period = _compat("_default_period", _default_period)(model_obj)
    all_periods = _compat("_all_periods", _all_periods)(model_obj)

    comparisons = []
    formula_type = _compat("_formula_type", _formula_type)
    sample_values = _compat("_sample_values", _sample_values)
    for item_id in compare_ids:
        item = model_obj.get_item(item_id)
        base_item_values = bundle.base_results.get(item_id, {})
        scenario_item_values = scenario_results.get(item_id, {})
        base_val = base_item_values.get(period)
        scenario_val = scenario_item_values.get(period)
        delta = None
        pct_change = None
        if base_val is not None and scenario_val is not None:
            delta = scenario_val - base_val
            if base_val != 0:
                pct_change = delta / base_val
        comparisons.append(
            {
                "id": item_id,
                "label": item.label,
                "item_type": item.item_type.value,
                "formula_type": formula_type(item),
                "base": base_val,
                "scenario": scenario_val,
                "delta": delta,
                "pct_change": pct_change,
                "base_values": {value_period: base_item_values.get(value_period) for value_period in all_periods},
                "scenario_values": {
                    value_period: scenario_item_values.get(value_period)
                    for value_period in all_periods
                },
                "base_sample_values": sample_values(base_item_values, all_periods),
                "scenario_sample_values": sample_values(scenario_item_values, all_periods),
            }
        )

    result = {
        "period": period,
        "recompute_policy": recompute_policy,
        "overrides": normalized,
        "comparisons": comparisons,
    }
    if scenario_case_selection is not None:
        result["scenario_case_selection"] = scenario_case_selection
    warnings = _compat("_scenario_case_direction_warnings", _scenario_case_direction_warnings)(
        scenario_case_selection=scenario_case_selection,
        comparisons=comparisons,
    )
    if warnings:
        result["warnings"] = warnings
        result["next_actions"] = [
            (
                "Treat wrong-way bull/bear readback as a model/scenario bridge quality issue, "
                "not as permission for extra diagnostic reads."
            ),
            (
                "If this is an FMS skill run, persist INSUFFICIENT_DATA with the warning details "
                "and recommended model repair."
            ),
        ]
    return result


_ORIGINALS = {
    "_NEGATIVE_SCENARIO_OUTPUT_TOKENS": _NEGATIVE_SCENARIO_OUTPUT_TOKENS,
    "_POSITIVE_SCENARIO_OUTPUT_TOKENS": _POSITIVE_SCENARIO_OUTPUT_TOKENS,
    "_SENSITIVITY_RECOMPUTE_POLICIES": _SENSITIVITY_RECOMPUTE_POLICIES,
    "_all_periods": _all_periods,
    "_apply_scenario_case_selection": _apply_scenario_case_selection,
    "_compute_scenario_results": _compute_scenario_results,
    "_default_period": _default_period,
    "_find_key_metrics": _find_key_metrics,
    "_formula_type": _formula_type,
    "_is_positive_scenario_output": _is_positive_scenario_output,
    "_sample_values": _sample_values,
    "_scenario_case_direction_warnings": _scenario_case_direction_warnings,
    "_scenario_case_recompute_ids": _scenario_case_recompute_ids,
    "_scenario_case_selection_for_overrides": _scenario_case_selection_for_overrides,
    "_scenario_recompute_ids": _scenario_recompute_ids,
    "_unknown_item_error": _unknown_item_error,
}


__all__ = [
    "_NEGATIVE_SCENARIO_OUTPUT_TOKENS",
    "_POSITIVE_SCENARIO_OUTPUT_TOKENS",
    "_is_positive_scenario_output",
    "_scenario_case_direction_warnings",
    "run_scenario",
]
