"""Formula evaluation helpers for :mod:`schema.dependency_graph`."""

from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING, Any, Dict, List, Optional, TypeGuard

if TYPE_CHECKING:
    from .dependency_graph import DependencyGraphResults

from .dependency_graph_helpers import _offset_column
from .driver_formula import apply_driver_scale_fn, evaluate_driver_value
from .models import (
    COLUMN_OFFSET_MODE_PERIOD_RELATIVE,
    FormulaSpec,
    FormulaType,
    LineItemRef,
)
from .refs import line_item_ref_from_obj

logger = logging.getLogger("schema.dependency_graph")

#: The ``FormulaSpec.subtype`` of a constant that snapshots a scenario output; ``params.expr`` is
#: the source row it was read from.
SCENARIO_SNAPSHOT_SUBTYPE = "scenario_snapshot"

# ``DECLARED_DOMAIN_VALUATION_SUBTYPES`` lives in :mod:`schema.dependency_graph`,
# where the typed classifier's declared-domain law reads it.


def _values_are_numeric(
    values: List[Optional[float]],
) -> TypeGuard[List[float]]:
    """Return whether every existing value is a concrete number."""
    return all(value is not None for value in values)


def _real_power(base: float, exponent: float) -> Optional[float]:
    """``base ** exponent`` as a real number, ``None`` where it has none: a negative base to a
    fractional exponent (Python returns a ``complex``), zero to a negative power, an overflow. The
    graph represents ``None`` as the invalid value, the path a non-finite result already takes."""
    try:
        result = base ** exponent
    except (ValueError, OverflowError, ZeroDivisionError):
        return None
    return None if isinstance(result, complex) else result


def _eval(
    self: Any,
    line_item_id: str,
    period: int,
    results: DependencyGraphResults,
    time_index: Dict[int, int],
    time_order: List[int],
) -> Optional[float]:
    """Evaluate a single line item for a given period.

    Uses FormulaType-specific handlers and delegates nested expressions
    to _eval_expr.
    """
    item = self.nodes[line_item_id]
    spec = self._spec_for_period(item, period)
    if spec is None:
        return None

    prev_eval_cell = getattr(self, "_current_eval_cell", None)
    self._current_eval_cell = (line_item_id, int(period))
    try:
        return _eval_inner(self, line_item_id, period, spec, results, time_index, time_order)
    finally:
        self._current_eval_cell = prev_eval_cell


def _eval_inner(
    self: Any,
    line_item_id: str,
    period: int,
    spec: FormulaSpec,
    results: DependencyGraphResults,
    time_index: Dict[int, int],
    time_order: List[int],
) -> Optional[float]:
    params = spec.params or {}
    spec_type = spec.type
    if spec_type == FormulaType.constant:
        if spec.subtype == SCENARIO_SNAPSHOT_SUBTYPE:
            # The snapshot's source row is read for its lineage, never its number.
            self._value_of(params.get("expr"), period, results, time_index, time_order)
        raw_value = params.get("value")
        if raw_value is None:
            return None
        try:
            return float(raw_value)
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
                logger.warning(
                    "SUM_RANGE in %r has a target that is not a line-item "
                    "ref (%s: %r); the cell is left unresolved",
                    line_item_id,
                    type(target_obj).__name__,
                    target_obj,
                )
                return None
            target_item = self.nodes.get(target_ref.id)
            if target_item is None:
                raise KeyError(f"SUM_RANGE target {target_ref.id!r} not found in model")

            if target_item.column is not None:
                return self._value_of(target_ref, period, results, time_index, time_order)

            values: List[Optional[float]] = []
            for target_period in self._sum_range_target_periods(target_ref.id):
                values.append(self._read_with_fallback(target_ref.id, target_period, results))
            if not values or not _values_are_numeric(values):
                return None
            return sum(values)

        if params.get("function") in {"SUM", "AVERAGE", "MEDIAN"}:
            items = params.get("items", [])
            if params.get("function") == "SUM" and not items:
                return 0.0
            values = [self._eval_expr(i, period, results, time_index, time_order) for i in items]
            if not values or not _values_are_numeric(values):
                return None
            if params.get("function") == "AVERAGE":
                return sum(values) / len(values)
            if params.get("function") == "MEDIAN":
                sorted_values = sorted(values)
                midpoint = len(sorted_values) // 2
                if len(sorted_values) % 2:
                    return sorted_values[midpoint]
                return (sorted_values[midpoint - 1] + sorted_values[midpoint]) / 2
            return sum(values)

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
            if not values or not _values_are_numeric(values):
                return None
            if operator == "+":
                return sum(values)
            if operator == "-":
                return values[0] - sum(values[1:])
            if operator == "*":
                result = 1.0
                for value in values:
                    result *= value
                return result
            if operator == "/":
                result = values[0]
                for value in values[1:]:
                    if value == 0:
                        return None
                    result /= value
                return result

        items = params.get("items")
        if isinstance(items, list):
            if not items:
                return None
            values = [self._eval_expr(i, period, results, time_index, time_order) for i in items]
            if not _values_are_numeric(values):
                return None
            return sum(values)

    if spec_type == FormulaType.driver:
        base = self._eval_expr(params.get("base"), period, results, time_index, time_order)
        rate = self._eval_expr(params.get("rate"), period, results, time_index, time_order)
        return evaluate_driver_value(base, rate, params)

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
        if (
            beginning is None
            or not _values_are_numeric(add_values)
            or not _values_are_numeric(sub_values)
        ):
            return None
        return beginning + sum(add_values) - sum(sub_values)

    if spec_type == FormulaType.valuation:
        return self._eval_valuation(spec, period, results, time_index, time_order)

    return None


def _eval_valuation(
    self: Any,
    spec: FormulaSpec,
    period: int,
    results: DependencyGraphResults,
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
        denom = _real_power(1 + rate, t)
        return cf / denom if denom else None

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
        we = self._eval_expr(
            params.get("weight_equity"), period, results, time_index, time_order
        )
        wd = self._eval_expr(
            params.get("weight_debt"), period, results, time_index, time_order
        )
        if we is None or wd is None:
            return None
        equity_term = 0.0
        if we != 0.0:
            ke = self._eval_expr(
                params.get("cost_equity"), period, results, time_index, time_order
            )
            if ke is None:
                return None
            equity_term = ke * we
        debt_term = 0.0
        if wd != 0.0:
            kd = self._eval_expr(
                params.get("cost_debt"), period, results, time_index, time_order
            )
            if kd is None:
                return None
            debt_term = kd * wd
        return equity_term + debt_term

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

    if subtype == "break_even_probability":
        # Market-implied probability of the upside case, D/(U+D), where U is
        # the gain to the upside price and D the loss to the downside price.
        # The identity is defined only when the upside is a gain and the
        # downside a loss; otherwise the break-even probability does not exist.
        up = self._eval_expr(params.get("upside_delta"), period, results, time_index, time_order)
        down = self._eval_expr(params.get("downside_delta"), period, results, time_index, time_order)
        if up is None or down is None or up <= 0 or down >= 0:
            return None
        return down / (down - up)

    if subtype == "offset_scenario":
        return self._eval_offset_scenario(spec, period, results, time_index, time_order)

    if subtype == "scenario_select":
        params = spec.params or {}
        selector_ref = line_item_ref_from_obj(params.get("selector"))
        if selector_ref is None:
            return None
        selector = self._value_of(
            selector_ref, period, results, time_index, time_order
        )
        if selector is None or not math.isfinite(selector):
            return None
        selector_int = int(selector)
        if selector != selector_int or selector_int not in {1, 2, 3}:
            return None
        case = {1: "bull", 2: "base", 3: "bear"}[selector_int]
        selected_ref = line_item_ref_from_obj(params.get(case))
        if selected_ref is None:
            return None
        return self._value_of(
            selected_ref, period, results, time_index, time_order
        )

    return None


def _eval_offset_scenario(
    self: Any,
    spec: FormulaSpec,
    period: int,
    results: DependencyGraphResults,
    time_index: Dict[int, int],
    time_order: List[int],
) -> Optional[float]:
    """Evaluate renderer-compatible INDEX scenario table lookups.

    The renderer lowers the legacy offset_scenario semantic expression to a
    three-row INDEX range immediately below the anchor. Period-relative
    formulas select that row in the current time column; fixed-column formulas
    select it after applying the declared column offset.
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
    if selector_value != row_offset or row_offset not in {1, 2, 3}:
        return None

    anchor_item = self.nodes.get(anchor_ref.id)
    if anchor_item is None:
        return None

    anchor_sheet = self._item_sheet_map.get(anchor_ref.id)
    if anchor_sheet is None:
        return None
    target_row = int(anchor_item.row) + row_offset

    column_offset_mode = params.get("column_offset_mode")
    if column_offset_mode == COLUMN_OFFSET_MODE_PERIOD_RELATIVE:
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

    target_item = self.nodes[target_id]
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
    results: DependencyGraphResults,
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
        if op == "IF":
            args = expr.get("args", [])
            if len(args) != 3:
                return None
            condition = self._eval_expr(args[0], period, results, time_index, time_order)
            if condition is None:
                return None
            branch = args[1] if condition != 0 else args[2]
            return self._eval_expr(branch, period, results, time_index, time_order)
        if op in {"+", "*", "SUM", "AVG", "MAX", "MIN"}:
            args = expr.get("args", [])
            if op in {"+", "SUM"} and not args:
                return 0.0
            values = [self._eval_expr(arg, period, results, time_index, time_order) for arg in args]
            if not values or not _values_are_numeric(values):
                return None
            if op == "+":
                return sum(values)
            if op == "SUM":
                return sum(values)
            if op == "AVG":
                return sum(values) / len(values)
            if op == "MAX":
                return max(values)
            if op == "MIN":
                return min(values)
            if op == "*":
                result = 1.0
                for value in values:
                    result *= value
                return result
        if op == "IFERROR":
            value = self._eval_expr(expr.get("expr"), period, results, time_index, time_order)
            # The legacy graph has no authenticated FallbackPolicy.  Treat the
            # primary expression as required instead of silently authorizing a
            # numeric fallback from an untyped expression dictionary.
            return value
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
                return _real_power(left, right)
        if op == "NEG":
            inner = self._eval_expr(expr.get("arg"), period, results, time_index, time_order)
            return None if inner is None else -inner
    if isinstance(expr, (int, float)):
        return float(expr)
    return None


def _apply_scale_fn(self: Any, value: float, scale_fn: str) -> float:
    return apply_driver_scale_fn(value, scale_fn)
