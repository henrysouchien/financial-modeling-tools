"""Formula value evaluation helpers for schema build orchestration."""

from __future__ import annotations

import sys
from typing import Dict, Optional

from .build_formula_refs import _available_periods, _extract_single_ref
from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    LineItem,
    LineItemRef,
    shift_period as _model_shift_period,
)


_MAX_REF_CHAIN_DEPTH = 16


def _shift_period(period: int, t: int, mode: str):
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    parent_shift_period = getattr(parent, "shift_period", None) if parent is not None else None
    if parent_shift_period is not None:
        return parent_shift_period(period, t, mode)
    return _model_shift_period(period, t, mode)


def _constant_override_value(spec: Optional[FormulaSpec]) -> Optional[float]:
    if spec is None or spec.type is not FormulaType.constant:
        return None
    value = spec.params.get("value")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _lookup_formula_value(
    model: FinancialModel,
    item_id: str,
    period: int,
    computed_values: Dict[tuple[str, int], float],
    *,
    t: int = 0,
    _depth: int = 0,
    _seen: Optional[set[tuple[str, int]]] = None,
) -> Optional[float]:
    shifted = _shift_period(int(period), int(t), model.time_structure.period_mode)
    if shifted is None:
        return None

    available_periods = _available_periods(model)
    if available_periods is not None and int(shifted) not in available_periods:
        return None

    key = (str(item_id), int(shifted))
    if key in computed_values:
        return computed_values[key]

    try:
        item = model.get_item(str(item_id))
    except KeyError:
        return None

    if item.overrides is not None and int(shifted) in item.overrides:
        spec_override = item.overrides[int(shifted)]
        if spec_override.note != "synthetic":
            value = _constant_override_value(spec_override)
            if value is not None:
                return value

    if item.values is not None and int(shifted) in item.values.values:
        value_cell = item.values.values[int(shifted)]
        if value_cell.value is not None:
            return float(value_cell.value)

    if item.historical is not None and item.historical.type is FormulaType.ref:
        if _depth >= _MAX_REF_CHAIN_DEPTH:
            return None
        if key in (_seen or set()):
            return None
        seen = (_seen or set()) | {key}
        params = item.historical.params or {}
        ref_target = _extract_single_ref(params)
        if ref_target is not None:
            target_id, target_t = ref_target
            value = _lookup_formula_value(
                model,
                target_id,
                int(shifted),
                computed_values,
                t=target_t,
                _depth=_depth + 1,
                _seen=seen,
            )
            if value is None:
                return None
            adjustment = params.get("adjustment")
            if adjustment is not None:
                try:
                    value += float(adjustment)
                except (TypeError, ValueError):
                    pass
            if params.get("negate"):
                value = -value
            return value

    if item.historical is not None and item.historical.type is FormulaType.constant:
        value = _constant_override_value(item.historical)
        if value is not None:
            return value

    return None


def _evaluate_expr_simple(
    model: FinancialModel,
    expr,
    period: int,
    computed_values: Dict[tuple[str, int], float],
) -> Optional[float]:
    if expr is None:
        return None
    if isinstance(expr, bool):
        return float(int(expr))
    if isinstance(expr, (int, float)):
        return float(expr)
    if isinstance(expr, LineItemRef):
        return _lookup_formula_value(model, expr.id, period, computed_values, t=expr.t)
    if isinstance(expr, dict):
        if "id" in expr and isinstance(expr["id"], str):
            try:
                ref_t = int(expr.get("t", 0))
            except (TypeError, ValueError):
                ref_t = 0
            return _lookup_formula_value(model, expr["id"], period, computed_values, t=ref_t)

        op = expr.get("op")
        args = list(expr.get("args", []) or [])
        if op in {"SUM", "AVERAGE"}:
            values = [_evaluate_expr_simple(model, arg, period, computed_values) for arg in args]
            if any(value is None for value in values):
                return None
            if not values:
                return 0.0
            if op == "AVERAGE":
                return sum(values) / len(values)
            return sum(values)
        if op == "+":
            values = [_evaluate_expr_simple(model, arg, period, computed_values) for arg in args]
            if any(value is None for value in values):
                return None
            return sum(values)
        if op == "*":
            values = [_evaluate_expr_simple(model, arg, period, computed_values) for arg in args]
            if any(value is None for value in values):
                return None
            result = 1.0
            for value in values:
                result *= value
            return result
        if op == "-":
            left = _evaluate_expr_simple(model, expr.get("left"), period, computed_values)
            right = _evaluate_expr_simple(model, expr.get("right"), period, computed_values)
            if left is None or right is None:
                return None
            return left - right
        if op == "/":
            left = _evaluate_expr_simple(model, expr.get("left"), period, computed_values)
            right = _evaluate_expr_simple(model, expr.get("right"), period, computed_values)
            if left is None or right is None or abs(right) < 1e-12:
                return None
            return left / right
        if op == "NEG":
            value = _evaluate_expr_simple(model, expr.get("arg"), period, computed_values)
            if value is None:
                return None
            return -value
    return None


def _evaluate_formula_simple(
    model: FinancialModel,
    item: LineItem,
    period: int,
    computed_values: Dict[tuple[str, int], float],
) -> Optional[float]:
    """Evaluate a subset of same-period formulas for reconciliation."""

    spec = item.historical
    if spec is None:
        return None

    params = spec.params or {}

    if spec.type == FormulaType.constant:
        return _constant_override_value(spec)

    if spec.type == FormulaType.ref:
        value = _evaluate_expr_simple(model, params.get("source"), period, computed_values)
        if value is None:
            return None
        adjustment = params.get("adjustment")
        if adjustment is not None:
            value += float(adjustment)
        if params.get("negate"):
            value = -value
        return value

    if spec.type == FormulaType.arithmetic:
        if "expr" in params:
            return _evaluate_expr_simple(model, params.get("expr"), period, computed_values)

        function = params.get("function")
        if function in {"SUM", "AVERAGE"}:
            values = [
                _evaluate_expr_simple(model, expr, period, computed_values)
                for expr in list(params.get("items", []) or [])
            ]
            if any(value is None for value in values):
                return None
            if not values:
                return 0.0
            if function == "AVERAGE":
                return sum(values) / len(values)
            return sum(values)

        operands = params.get("operands")
        if isinstance(operands, list) and operands:
            args = list(operands)
            operator = "+"
            if isinstance(args[0], str) and args[0] in {"+", "-", "*", "/"}:
                operator = args.pop(0)
            values = [_evaluate_expr_simple(model, expr, period, computed_values) for expr in args]
            if any(value is None for value in values):
                return None
            if not values:
                return 0.0
            if operator == "+":
                return sum(values)
            if operator == "-":
                result = values[0]
                for value in values[1:]:
                    result -= value
                return result
            if operator == "*":
                result = 1.0
                for value in values:
                    result *= value
                return result
            if operator == "/":
                result = values[0]
                for value in values[1:]:
                    if abs(value) < 1e-12:
                        return None
                    result /= value
                return result
            return None

        values = [
            _evaluate_expr_simple(model, expr, period, computed_values)
            for expr in list(params.get("items", []) or [])
        ]
        if any(value is None for value in values):
            return None
        return sum(values) if values else 0.0

    if spec.type == FormulaType.ratio:
        numerator = _evaluate_expr_simple(model, params.get("numerator"), period, computed_values)
        denominator = _evaluate_expr_simple(model, params.get("denominator"), period, computed_values)
        if numerator is None or denominator is None or abs(denominator) < 1e-12:
            return None
        result = numerator / denominator
        if params.get("subtract_one"):
            result -= 1
        return result

    return None


__all__ = [
    "_MAX_REF_CHAIN_DEPTH",
    "_constant_override_value",
    "_evaluate_expr_simple",
    "_evaluate_formula_simple",
    "_lookup_formula_value",
    "_shift_period",
]
