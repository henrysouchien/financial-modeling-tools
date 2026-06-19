"""Formula evaluation helpers for :mod:`schema.dependency_graph`."""

from __future__ import annotations

import logging
import math
from typing import Any, Dict, List, Optional

from .dependency_graph_helpers import _offset_column
from .models import FormulaSpec, FormulaType, LineItemRef
from .refs import line_item_ref_from_obj

logger = logging.getLogger("schema.dependency_graph")


def _eval(
    self: Any,
    line_item_id: str,
    period: int,
    results: Dict[str, Dict[int, float]],
    time_index: Dict[int, int],
    time_order: List[int],
) -> Optional[float]:
    """Evaluate a single line item for a given period.

    Uses FormulaType-specific handlers and delegates nested expressions
    to _eval_expr.
    """
    item = self.model.get_item(line_item_id)
    spec = self._spec_for_period(item, period)
    if spec is None:
        return None

    params = spec.params or {}
    spec_type = spec.type

    if spec_type == FormulaType.constant:
        if params.get("value") is None:
            return None
        try:
            return float(params.get("value"))
        except (TypeError, ValueError):
            return None

    if spec_type == FormulaType.ref:
        source = params.get("source")
        value = self._value_of(source, period, results, time_index, time_order)
        if value is None:
            return None
        adjustment = params.get("adjustment")
        if adjustment is not None:
            value += float(adjustment)
        if params.get("negate"):
            value = -value
        return value

    if spec_type == FormulaType.arithmetic:
        if "expr" in params:
            return self._eval_expr(params.get("expr"), period, results, time_index, time_order)

        if params.get("function") == "SUM_RANGE":
            target_obj = params.get("target")
            target_ref = line_item_ref_from_obj(target_obj)
            if target_ref is None:
                raise ValueError(
                    f"SUM_RANGE in {line_item_id!r} has malformed 'target' "
                    f"(expected LineItemRef-coercible; got {type(target_obj).__name__}: "
                    f"{target_obj!r})"
                )
            try:
                target_item = self.model.get_item(target_ref.id)
            except KeyError as exc:
                raise KeyError(f"SUM_RANGE target {target_ref.id!r} not found in model") from exc

            if target_item.column is not None:
                return self._value_of(target_ref, period, results, time_index, time_order)

            values: List[Optional[float]] = []
            for target_period in self._sum_range_target_periods(target_ref.id):
                values.append(self._read_with_fallback(target_ref.id, target_period, results))
            non_none = [v for v in values if v is not None]
            if not non_none:
                return None
            return sum(non_none)

        if params.get("function") in {"SUM", "AVERAGE", "MEDIAN"}:
            items = params.get("items", [])
            values = [self._eval_expr(i, period, results, time_index, time_order) for i in items]
            non_none = [v for v in values if v is not None]
            # If ALL values are None, return None (no data; AVERAGE/MEDIAN ignore this).
            # Otherwise SUM treats None as 0, matching blank Excel cells.
            if not non_none:
                return None
            if params.get("function") == "AVERAGE":
                return sum(non_none) / len(non_none)
            if params.get("function") == "MEDIAN":
                sorted_values = sorted(non_none)
                midpoint = len(sorted_values) // 2
                if len(sorted_values) % 2:
                    return sorted_values[midpoint]
                return (sorted_values[midpoint - 1] + sorted_values[midpoint]) / 2
            return sum(non_none)

        operands = params.get("operands")
        if isinstance(operands, list) and operands:
            operator = "+"
            start_index = 0
            if isinstance(operands[0], str) and operands[0] in {"+", "-", "*", "/"}:
                operator = operands[0]
                start_index = 1
            values = [
                self._eval_expr(i, period, results, time_index, time_order)
                for i in operands[start_index:]
            ]
            if not values:
                return None
            non_none = [v for v in values if v is not None]
            if not non_none:
                return None
            if operator == "+":
                return sum(non_none)
            if any(v is None for v in values):
                return None
            if operator == "-":
                return values[0] - sum(values[1:])
            if operator == "*":
                result = 1.0
                for v in values:
                    result *= v
                return result
            if operator == "/":
                result = values[0]
                for v in values[1:]:
                    if v == 0:
                        return None
                    result /= v
                return result

        items = params.get("items")
        if isinstance(items, list):
            values = [self._eval_expr(i, period, results, time_index, time_order) for i in items]
            if any(v is None for v in values):
                return None
            return sum(values)

    if spec_type == FormulaType.driver:
        base = self._eval_expr(params.get("base"), period, results, time_index, time_order)
        rate = self._eval_expr(params.get("rate"), period, results, time_index, time_order)
        if base is None or rate is None:
            return None
        result = base * rate
        scale = params.get("scale")
        if scale:
            result /= float(scale)
        scale_fn = params.get("scale_fn")
        if isinstance(scale_fn, str):
            result = self._apply_scale_fn(result, scale_fn)
        return result

    if spec_type == FormulaType.ratio:
        numerator = self._eval_expr(params.get("numerator"), period, results, time_index, time_order)
        denominator = self._eval_expr(params.get("denominator"), period, results, time_index, time_order)
        if numerator is None or denominator is None:
            return None
        if denominator == 0:
            if self._ratio_cached_fallback_enabled():
                cached = self._cached_value_for_period(line_item_id, period, results)
                if cached is not None:
                    logger.debug(
                        "Ratio denominator-zero cached fallback for %s period=%s",
                        line_item_id,
                        period,
                    )
                    return cached
            return None
        result = numerator / denominator
        if params.get("subtract_one"):
            result -= 1
        return result

    if spec_type == FormulaType.growth:
        base = self._eval_expr(params.get("base"), period, results, time_index, time_order)
        rate = self._eval_expr(params.get("rate"), period, results, time_index, time_order)
        if base is None or rate is None:
            return None
        return base * (1 + rate)

    if spec_type == FormulaType.roll_forward:
        beginning = self._eval_expr(params.get("beginning"), period, results, time_index, time_order)
        additions = params.get("additions", [])
        subtractions = params.get("subtractions", [])
        add_values = [self._eval_expr(i, period, results, time_index, time_order) for i in additions]
        sub_values = [self._eval_expr(i, period, results, time_index, time_order) for i in subtractions]
        if beginning is None:
            return None
        return beginning + sum(v or 0 for v in add_values) - sum(v or 0 for v in sub_values)

    if spec_type == FormulaType.valuation:
        return self._eval_valuation(spec, period, results, time_index, time_order)

    return None


def _eval_valuation(
    self: Any,
    spec: FormulaSpec,
    period: int,
    results: Dict[str, Dict[int, float]],
    time_index: Dict[int, int],
    time_order: List[int],
) -> Optional[float]:
    """Evaluate FormulaType.valuation formulas that have graph semantics."""

    subtype = spec.subtype or ""
    params = spec.params or {}

    if subtype == "dcf_discount":
        cf = self._eval_expr(params.get("cash_flow"), period, results, time_index, time_order)
        rate = self._eval_expr(params.get("discount_rate"), period, results, time_index, time_order)
        t = self._eval_expr(params.get("period"), period, results, time_index, time_order)
        if cf is None or rate is None or t is None:
            return None
        denom = (1 + rate) ** t
        return cf / denom if denom != 0 else None

    if subtype == "terminal_value":
        final_cf = self._eval_expr(params.get("final_cf"), period, results, time_index, time_order)
        growth = self._eval_expr(params.get("growth"), period, results, time_index, time_order)
        discount = self._eval_expr(params.get("discount"), period, results, time_index, time_order)
        if final_cf is None or growth is None or discount is None:
            return None
        denom = discount - growth
        if denom == 0:
            return None
        return (final_cf * (1 + growth)) / denom

    if subtype == "capm":
        rf = self._eval_expr(params.get("risk_free"), period, results, time_index, time_order)
        beta = self._eval_expr(params.get("beta"), period, results, time_index, time_order)
        erp = self._eval_expr(params.get("erp"), period, results, time_index, time_order)
        if rf is None or beta is None or erp is None:
            return None
        return rf + (beta * erp)

    if subtype == "wacc":
        ke = self._eval_expr(params.get("cost_equity"), period, results, time_index, time_order)
        we = self._eval_expr(params.get("weight_equity"), period, results, time_index, time_order)
        kd = self._eval_expr(params.get("cost_debt"), period, results, time_index, time_order)
        wd = self._eval_expr(params.get("weight_debt"), period, results, time_index, time_order)
        if ke is None or we is None or kd is None or wd is None:
            return None
        return (ke * we) + (kd * wd)

    if subtype == "multiple":
        mult = self._eval_expr(params.get("multiple"), period, results, time_index, time_order)
        metric = self._eval_expr(params.get("metric"), period, results, time_index, time_order)
        if mult is None or metric is None:
            return None
        return mult * metric

    if subtype == "probability_weighted":
        value = self._eval_expr(params.get("value"), period, results, time_index, time_order)
        current = self._eval_expr(params.get("current"), period, results, time_index, time_order)
        prob = self._eval_expr(params.get("probability"), period, results, time_index, time_order)
        if value is None or current is None or prob is None:
            return None
        return (value - current) * prob

    if subtype == "kelly":
        ev = self._eval_expr(params.get("expected_value"), period, results, time_index, time_order)
        tw = self._eval_expr(params.get("total_win"), period, results, time_index, time_order)
        if ev is None or tw is None or tw == 0:
            return None
        return ev / tw

    if subtype == "offset_scenario":
        return self._eval_offset_scenario(spec, period, results, time_index, time_order)

    return None


def _eval_offset_scenario(
    self: Any,
    spec: FormulaSpec,
    period: int,
    results: Dict[str, Dict[int, float]],
    time_index: Dict[int, int],
    time_order: List[int],
) -> Optional[float]:
    """Evaluate renderer-compatible OFFSET scenario table lookups.

    Scenario formulas use a selector value as an Excel row offset from an
    anchor row. Period-relative formulas select the row at the current time
    column. Fixed-column formulas select the exact table cell.
    """

    if not self.model:
        return None

    params = spec.params or {}
    anchor_ref = line_item_ref_from_obj(params.get("anchor"))
    selector_ref = line_item_ref_from_obj(params.get("selector"))
    if anchor_ref is None or selector_ref is None:
        return None

    selector_value = self._value_of(selector_ref, period, results, time_index, time_order)
    if selector_value is None or not math.isfinite(selector_value):
        return None
    row_offset = int(selector_value)

    try:
        anchor_item = self.model.get_item(anchor_ref.id)
    except KeyError:
        return None

    anchor_sheet = self._item_sheet_map.get(anchor_ref.id)
    if anchor_sheet is None:
        return None
    target_row = int(anchor_item.row) + row_offset

    column_offset_mode = params.get("column_offset_mode")
    if column_offset_mode == "period_relative":
        target_id = self._row_item_id(anchor_sheet, target_row)
        if target_id is None:
            return None
        return self._read_with_fallback(target_id, int(period), results)

    if column_offset_mode is not None:
        return None

    anchor_col = self._item_column_map.get(anchor_ref.id)
    if anchor_col is None:
        anchor_col = self._column_for_period(period)
    if anchor_col is None:
        return None

    try:
        column_offset = int(params.get("column_offset", 0) or 0)
    except (TypeError, ValueError):
        return None

    try:
        target_col = _offset_column(anchor_col, column_offset)
    except ValueError:
        return None
    target_id = self._cell_item_map.get((anchor_sheet, target_row, target_col))
    if target_id is None:
        target_id = self._row_item_id(anchor_sheet, target_row)
    if target_id is None:
        return None

    target_item = self.model.get_item(target_id)
    target_period = (
        self._fixed_cell_anchor_period(target_item)
        if target_item.column is not None
        else int(period)
    )
    if target_period is None:
        return None
    return self._read_with_fallback(target_id, target_period, results)


def _eval_expr(
    self: Any,
    expr: Any,
    period: int,
    results: Dict[str, Dict[int, float]],
    time_index: Dict[int, int],
    time_order: List[int],
) -> Optional[float]:
    """Evaluate a small expression tree from the pattern matcher."""
    if expr is None:
        return None
    if isinstance(expr, LineItemRef):
        return self._value_of(expr, period, results, time_index, time_order)
    if isinstance(expr, dict):
        coerced = line_item_ref_from_obj(expr)
        if coerced is not None:
            return self._value_of(coerced, period, results, time_index, time_order)
        op = expr.get("op")
        if op in {"+", "*", "SUM", "AVG", "MAX"}:
            args = expr.get("args", [])
            values = [self._eval_expr(arg, period, results, time_index, time_order) for arg in args]
            non_none = [v for v in values if v is not None]
            if op == "+":
                return sum(v or 0 for v in values)
            if op == "SUM":
                if not non_none:
                    return None
                return sum(non_none)
            if op == "AVG":
                if not non_none:
                    return None
                return sum(non_none) / len(non_none)
            if op == "MAX":
                if not non_none:
                    return None
                return max(non_none)
            if any(v is None for v in values):
                return None
            if op == "*":
                result = 1.0
                for value in values:
                    result *= value
                return result
        if op == "IFERROR":
            value = self._eval_expr(expr.get("expr"), period, results, time_index, time_order)
            if value is not None:
                return value
            return self._eval_expr(expr.get("fallback", 0), period, results, time_index, time_order)
        if op in {"-", "/", "^"}:
            left = self._eval_expr(expr.get("left"), period, results, time_index, time_order)
            right = self._eval_expr(expr.get("right"), period, results, time_index, time_order)
            if left is None or right is None:
                return None
            if op == "-":
                return left - right
            if op == "/":
                if right == 0:
                    return None
                return left / right
            if op == "^":
                try:
                    return left ** right
                except (ValueError, OverflowError):
                    return None
        if op == "NEG":
            inner = self._eval_expr(expr.get("arg"), period, results, time_index, time_order)
            return None if inner is None else -inner
    if isinstance(expr, (int, float)):
        return float(expr)
    return None


def _apply_scale_fn(self: Any, value: float, scale_fn: str) -> float:
    scale_fn = scale_fn.strip()
    if not scale_fn:
        return value
    if scale_fn.startswith("/"):
        return value / float(scale_fn[1:])
    if scale_fn.startswith("*"):
        return value * float(scale_fn[1:])
    return value
