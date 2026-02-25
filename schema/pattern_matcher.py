"""Excel formula pattern matcher.

Purpose:
- Convert Excel formulas into schema FormulaSpec objects with semantic references.
- Recognize common financial-model patterns so downstream tools can reason
  about dependencies without cell addresses.

High-level flow:
1. Parse the formula into an AST.
2. If the AST is constant-only, emit a constant FormulaSpec.
3. Try pattern matchers in order (ref → growth → valuation → ratio → roll_forward
   → driver → arithmetic). The order encodes precedence for ambiguous patterns.
4. Fall back to a raw FormulaSpec when no pattern matches.

Key idea:
- Cell references are mapped to LineItemRef(id, t) using sheet/row/column context,
  where t is a time offset derived from the model period headers.

Examples:
Formula: =H7-H9
Schema:  FormulaSpec(type=arithmetic, params={"operands": ["-", LineItemRef("revenue"), LineItemRef("gross_profit")]})

Formula: =G14*(1+H15)
Schema:  FormulaSpec(type=growth, params={"base": LineItemRef("revenue", t=-1), "rate": LineItemRef("growth_rate")})

Formula: =E35/((1+$E$68)^E37)
Schema:  FormulaSpec(type=valuation, subtype="dcf_discount",
                    params={"cash_flow": LineItemRef("fcf"), "discount_rate": LineItemRef("wacc"), "period": LineItemRef("period")})
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

from .formula_ast import (
    BinaryOp,
    Empty,
    FormulaParseError,
    FormulaParser,
    FuncCall,
    Node,
    Number,
    Range,
    Ref,
    UnaryOp,
)
from .models import FormulaSpec, FormulaType, LineItemRef


@dataclass
class CellContext:
    """Context needed to interpret cell references semantically.

    sheet_row_to_item maps row numbers to line_item_ids per sheet.
    sheet_col_to_period maps column indices to period keys per sheet.
    time_order is the ordered list of period keys used to derive t offsets.
    """
    sheet: str
    row: int
    col: int
    sheet_row_to_item: Dict[str, Dict[int, str]]
    time_order: List[int]
    sheet_col_to_period: Optional[Dict[str, Dict[int, int]]] = None
    sheet_col_to_year: Optional[Dict[str, Dict[int, int]]] = None

    def __post_init__(self) -> None:
        if self.sheet_col_to_period is None and self.sheet_col_to_year is not None:
            self.sheet_col_to_period = self.sheet_col_to_year
        if self.sheet_col_to_period is None:
            self.sheet_col_to_period = {}


class FormulaPatternMatcher:
    def classify(self, formula: str, context: CellContext) -> FormulaSpec:
        """Classify a raw Excel formula string into a FormulaSpec.

        The matcher tries fast-path patterns first (ref/growth/valuation/etc.)
        and falls back to a raw FormulaSpec if no pattern matches.
        """
        try:
            parser = FormulaParser(formula)
            ast = parser.parse()
        except FormulaParseError:
            return FormulaSpec(type=FormulaType.raw, params={"formula": formula})

        # Strip unary plus — legacy Excel convention (=+Cell) common in institutional models
        while isinstance(ast, UnaryOp) and ast.op == "+":
            ast = ast.expr

        constant_value = self._constant_value(ast)
        if constant_value is not None:
            return FormulaSpec(type=FormulaType.constant, params={"value": constant_value})

        ref_spec = self._match_ref(ast, context)
        if ref_spec:
            return ref_spec

        growth_spec = self._match_growth(ast, context)
        if growth_spec:
            return growth_spec

        valuation_spec = self._match_valuation(ast, context)
        if valuation_spec:
            return valuation_spec

        ratio_spec = self._match_ratio(ast, context)
        if ratio_spec:
            return ratio_spec

        roll_spec = self._match_roll_forward(ast, context)
        if roll_spec:
            return roll_spec

        driver_spec = self._match_driver(ast, context)
        if driver_spec:
            return driver_spec

        arithmetic_spec = self._match_arithmetic(ast, context)
        if arithmetic_spec:
            return arithmetic_spec

        # Note: Some models still surface raw formulas (e.g., IF/IFERROR wrappers or #REF! broken links).
        return FormulaSpec(type=FormulaType.raw, params={"formula": formula})

    def _match_ref(self, ast: Node, context: CellContext) -> Optional[FormulaSpec]:
        """Match a direct reference (with optional adjustment/negation).

        Examples:
        - =H10            → ref(source=H10)
        - =-H10           → ref(source=H10, negate=True)
        - =H10+0.002      → ref(source=H10, adjustment=0.002)
        """
        if isinstance(ast, Ref):
            ref = self._to_line_item_ref(ast, context)
            if ref is None:
                return None
            return FormulaSpec(type=FormulaType.ref, params={"source": ref})

        if isinstance(ast, UnaryOp) and ast.op == "-" and isinstance(ast.expr, Ref):
            ref = self._to_line_item_ref(ast.expr, context)
            if ref is None:
                return None
            return FormulaSpec(type=FormulaType.ref, params={"source": ref, "negate": True})

        if isinstance(ast, BinaryOp) and ast.op in {"+", "-"}:
            left_ref = ast.left if isinstance(ast.left, Ref) else None
            right_ref = ast.right if isinstance(ast.right, Ref) else None
            left_num = ast.left if isinstance(ast.left, Number) else None
            right_num = ast.right if isinstance(ast.right, Number) else None
            if left_ref and right_num:
                ref = self._to_line_item_ref(left_ref, context)
                if ref is None:
                    return None
                adjustment = right_num.value if ast.op == "+" else -right_num.value
                return FormulaSpec(type=FormulaType.ref, params={"source": ref, "adjustment": adjustment})
            if right_ref and left_num:
                if ast.op == "-":
                    # Number - Ref is NOT a ref+adjustment pattern.
                    # It means -(Ref) + Number, which is semantically different.
                    # Let it fall through to arithmetic matching.
                    pass
                else:
                    ref = self._to_line_item_ref(right_ref, context)
                    if ref is None:
                        return None
                    adjustment = left_num.value
                    return FormulaSpec(type=FormulaType.ref, params={"source": ref, "adjustment": adjustment})
        return None

    def _match_growth(self, ast: Node, context: CellContext) -> Optional[FormulaSpec]:
        """Match growth compound patterns: base * (1 + rate)."""
        if not isinstance(ast, BinaryOp) or ast.op != "*":
            return None

        base, rate = self._extract_growth_operands(ast.left, ast.right)
        if base is None or rate is None:
            base, rate = self._extract_growth_operands(ast.right, ast.left)
        if base is None or rate is None:
            return None

        base_ref = self._to_line_item_ref(base, context)
        rate_ref = self._to_line_item_ref(rate, context)
        if base_ref is None or rate_ref is None:
            return None

        return FormulaSpec(type=FormulaType.growth, params={"base": base_ref, "rate": rate_ref})

    def _extract_growth_operands(self, base_candidate: Node, rate_candidate: Node) -> Tuple[Optional[Ref], Optional[Ref]]:
        """Extract base and rate from base * (1 + rate) pattern.

        Only matches + operator. The - operator (base * (1 - rate)) is a different
        pattern (complement/driver) and should fall through to driver matching.
        """
        if not isinstance(base_candidate, Ref):
            return None, None
        # Only match (1 + rate), not (1 - rate)
        if isinstance(rate_candidate, BinaryOp) and rate_candidate.op == "+":
            if isinstance(rate_candidate.left, Number) and rate_candidate.left.value == 1 and isinstance(rate_candidate.right, Ref):
                return base_candidate, rate_candidate.right
            if isinstance(rate_candidate.right, Number) and rate_candidate.right.value == 1 and isinstance(rate_candidate.left, Ref):
                return base_candidate, rate_candidate.left
        return None, None

    def _match_ratio(self, ast: Node, context: CellContext) -> Optional[FormulaSpec]:
        """Match ratio patterns, including YoY growth and incremental margins.

        Supports nested expressions in numerator or denominator.
        """
        if isinstance(ast, BinaryOp) and ast.op == "-":
            if isinstance(ast.right, Number) and ast.right.value == 1 and isinstance(ast.left, BinaryOp) and ast.left.op == "/":
                numerator = self._expr_from_node(ast.left.left, context)
                denominator = self._expr_from_node(ast.left.right, context)
                if numerator and denominator:
                    return FormulaSpec(
                        type=FormulaType.ratio,
                        subtype="yoy_growth",
                        params={"numerator": numerator, "denominator": denominator, "subtract_one": True},
                    )

        if isinstance(ast, BinaryOp) and ast.op == "/":
            numerator = self._expr_from_node(ast.left, context)
            denominator = self._expr_from_node(ast.right, context)
            if numerator and denominator:
                subtype = None
                if self._is_delta_expr(ast.left, context) and self._is_delta_expr(ast.right, context):
                    subtype = "incremental_margin"
                return FormulaSpec(
                    type=FormulaType.ratio,
                    subtype=subtype,
                    params={"numerator": numerator, "denominator": denominator},
                )
        return None

    def _match_roll_forward(self, ast: Node, context: CellContext) -> Optional[FormulaSpec]:
        """Match roll-forward schedules: beginning + adds - subs.

        Adds/subs can be empty, but at least one non-beginning term must exist.
        """
        terms = self._flatten_add_sub(ast)
        if terms is None:
            return None

        additions: List[LineItemRef] = []
        subtractions: List[LineItemRef] = []
        beginning: Optional[LineItemRef] = None
        for sign, node in terms:
            ref = self._to_line_item_ref(node, context)
            if ref is None:
                return None
            if beginning is None:
                beginning = ref
                continue
            if sign == "+":
                additions.append(ref)
            else:
                subtractions.append(ref)

        if beginning and (additions or subtractions):
            if not self._has_same_row_ref(context, [beginning] + additions + subtractions):
                return None
            return FormulaSpec(
                type=FormulaType.roll_forward,
                params={"beginning": beginning, "additions": additions, "subtractions": subtractions},
            )
        return None

    def _match_driver(self, ast: Node, context: CellContext) -> Optional[FormulaSpec]:
        """Match driver formulas (base * rate), with nested arithmetic support."""
        if not isinstance(ast, BinaryOp) or ast.op != "*":
            return None

        # Handle (ref / number) * ref patterns (e.g., revenue/365 * dso)
        left_div = ast.left if isinstance(ast.left, BinaryOp) and ast.left.op == "/" else None
        right_div = ast.right if isinstance(ast.right, BinaryOp) and ast.right.op == "/" else None
        if left_div and isinstance(left_div.left, Ref) and isinstance(left_div.right, Number) and isinstance(ast.right, Ref):
            base_ref = self._to_line_item_ref(left_div.left, context)
            rate_ref = self._to_line_item_ref(ast.right, context)
            if base_ref and rate_ref:
                return FormulaSpec(
                    type=FormulaType.driver,
                    params={
                        "base": base_ref,
                        "rate": rate_ref,
                        "scale": left_div.right.value,
                    },
                )

        if right_div and isinstance(right_div.left, Ref) and isinstance(right_div.right, Number) and isinstance(ast.left, Ref):
            base_ref = self._to_line_item_ref(ast.left, context)
            rate_ref = self._to_line_item_ref(right_div.left, context)
            if base_ref and rate_ref:
                return FormulaSpec(
                    type=FormulaType.driver,
                    params={
                        "base": base_ref,
                        "rate": rate_ref,
                        "scale": right_div.right.value,
                    },
                )
        left_ref = self._to_line_item_ref(ast.left, context)
        right_ref = self._to_line_item_ref(ast.right, context)
        left_num = ast.left.value if isinstance(ast.left, Number) else None
        right_num = ast.right.value if isinstance(ast.right, Number) else None

        if left_ref and right_ref:
            return FormulaSpec(type=FormulaType.driver, params={"base": left_ref, "rate": right_ref})
        if left_ref and right_num is not None:
            return FormulaSpec(type=FormulaType.driver, params={"base": left_ref, "rate": right_num})
        if right_ref and left_num is not None:
            return FormulaSpec(type=FormulaType.driver, params={"base": right_ref, "rate": left_num})

        left_expr = self._expr_from_node(ast.left, context)
        right_expr = self._expr_from_node(ast.right, context)
        if left_expr is None or right_expr is None:
            return None
        if not (self._contains_ref(left_expr) or self._contains_ref(right_expr)):
            return None
        return FormulaSpec(type=FormulaType.driver, params={"base": left_expr, "rate": right_expr})

    def _match_arithmetic(self, ast: Node, context: CellContext) -> Optional[FormulaSpec]:
        """Match arithmetic patterns (SUM/AVERAGE, add/sub/mul/div chains).

        Falls back to a generic expression tree for mixed chains like
        SUM(range)+term or D29-D25+D33.
        """
        if isinstance(ast, FuncCall) and ast.name in {"SUM", "AVERAGE"}:
            items = []
            for arg in ast.args:
                if isinstance(arg, Empty):
                    continue
                if isinstance(arg, Range):
                    items.extend(r for r in self._range_to_refs(arg, context) if r.resolved)
                    continue
                ref = self._to_line_item_ref(arg, context)
                if ref is not None and ref.resolved:
                    items.append(ref)
                    continue
                if ref is not None and not ref.resolved:
                    continue
                expr = self._expr_from_node(arg, context)
                if expr is None:
                    return None
                items.append(expr)
            if items:
                return FormulaSpec(type=FormulaType.arithmetic, params={"function": ast.name, "items": items})

        if isinstance(ast, BinaryOp) and ast.op in {"+", "-", "*", "/"}:
            values = self._flatten_binary(ast, ast.op)
            if values is None:
                values = None
            if values is not None:
                operands = [ast.op]
                ok = True
                for node in values:
                    ref = self._to_line_item_ref(node, context)
                    if ref is None or not ref.resolved:
                        ok = False
                        break
                    operands.append(ref)
                if ok:
                    return FormulaSpec(type=FormulaType.arithmetic, params={"operands": operands})

        expr = self._expr_from_node(ast, context)
        if expr is not None and not isinstance(expr, (LineItemRef, int, float)):
            return FormulaSpec(type=FormulaType.arithmetic, params={"expr": expr})

        return None

    def _match_valuation(self, ast: Node, context: CellContext) -> Optional[FormulaSpec]:
        """Match valuation-specific patterns (DCF, terminal value, CAPM, WACC, multiples).

        These are sheet-scoped or shape-scoped; if matched, they should win
        over generic ratio/driver patterns.
        """
        if isinstance(ast, FuncCall):
            if ast.name in {"OFFSET", "NPV", "XNPV", "IRR"}:
                subtype = ast.name.lower()
                if ast.name == "OFFSET":
                    subtype = "offset_scenario"
                return FormulaSpec(type=FormulaType.valuation, subtype=subtype, params={"formula": ast.name})

        # DCF discount: CF / ((1 + r)^t)
        if isinstance(ast, BinaryOp) and ast.op == "/":
            cash_flow = self._to_line_item_ref(ast.left, context) if isinstance(ast.left, Ref) else None
            if cash_flow and isinstance(ast.right, BinaryOp) and ast.right.op == "^":
                rate_ref = self._extract_one_plus_ref(ast.right.left, context)
                period_ref = self._to_line_item_ref(ast.right.right, context) if isinstance(ast.right.right, Ref) else None
                if rate_ref and period_ref:
                    return FormulaSpec(
                        type=FormulaType.valuation,
                        subtype="dcf_discount",
                        params={"cash_flow": cash_flow, "discount_rate": rate_ref, "period": period_ref},
                    )

        # Terminal value: (CF * (1 + g)) / (r - g)
        if isinstance(ast, BinaryOp) and ast.op == "/":
            numerator = ast.left
            denominator = ast.right
            if isinstance(numerator, BinaryOp) and numerator.op == "*" and isinstance(denominator, BinaryOp) and denominator.op == "-":
                cf_ref = None
                growth_ref = None
                if isinstance(numerator.left, Ref):
                    cf_ref = self._to_line_item_ref(numerator.left, context)
                    growth_ref = self._extract_one_plus_ref(numerator.right, context)
                if cf_ref is None and isinstance(numerator.right, Ref):
                    cf_ref = self._to_line_item_ref(numerator.right, context)
                    growth_ref = self._extract_one_plus_ref(numerator.left, context)

                discount_ref = self._to_line_item_ref(denominator.left, context) if isinstance(denominator.left, Ref) else None
                growth_ref2 = self._to_line_item_ref(denominator.right, context) if isinstance(denominator.right, Ref) else None

                if cf_ref and growth_ref and discount_ref and growth_ref2 and growth_ref.id == growth_ref2.id:
                    return FormulaSpec(
                        type=FormulaType.valuation,
                        subtype="terminal_value",
                        params={"final_cf": cf_ref, "growth": growth_ref, "discount": discount_ref},
                    )

        # CAPM: risk_free + (beta * erp)
        if isinstance(ast, BinaryOp) and ast.op == "+":
            left_ref = self._to_line_item_ref(ast.left, context) if isinstance(ast.left, Ref) else None
            right_ref = self._to_line_item_ref(ast.right, context) if isinstance(ast.right, Ref) else None
            left_mul = ast.left if isinstance(ast.left, BinaryOp) and ast.left.op == "*" else None
            right_mul = ast.right if isinstance(ast.right, BinaryOp) and ast.right.op == "*" else None
            if left_ref and right_mul:
                beta_ref = self._to_line_item_ref(right_mul.left, context) if isinstance(right_mul.left, Ref) else None
                erp_ref = self._to_line_item_ref(right_mul.right, context) if isinstance(right_mul.right, Ref) else None
                if beta_ref and erp_ref:
                    return FormulaSpec(
                        type=FormulaType.valuation,
                        subtype="capm",
                        params={"risk_free": left_ref, "beta": beta_ref, "erp": erp_ref},
                    )
            if right_ref and left_mul:
                beta_ref = self._to_line_item_ref(left_mul.left, context) if isinstance(left_mul.left, Ref) else None
                erp_ref = self._to_line_item_ref(left_mul.right, context) if isinstance(left_mul.right, Ref) else None
                if beta_ref and erp_ref:
                    return FormulaSpec(
                        type=FormulaType.valuation,
                        subtype="capm",
                        params={"risk_free": right_ref, "beta": beta_ref, "erp": erp_ref},
                    )

        # WACC: (cost_eq*weight_eq) + (cost_debt*weight_debt)
        if isinstance(ast, BinaryOp) and ast.op == "+":
            left_mul = ast.left if isinstance(ast.left, BinaryOp) and ast.left.op == "*" else None
            right_mul = ast.right if isinstance(ast.right, BinaryOp) and ast.right.op == "*" else None
            if left_mul and right_mul:
                cost_equity = self._to_line_item_ref(left_mul.left, context) if isinstance(left_mul.left, Ref) else None
                weight_equity = self._to_line_item_ref(left_mul.right, context) if isinstance(left_mul.right, Ref) else None
                cost_debt = self._to_line_item_ref(right_mul.left, context) if isinstance(right_mul.left, Ref) else None
                weight_debt = self._to_line_item_ref(right_mul.right, context) if isinstance(right_mul.right, Ref) else None
                if cost_equity and weight_equity and cost_debt and weight_debt:
                    return FormulaSpec(
                        type=FormulaType.valuation,
                        subtype="wacc",
                        params={
                            "cost_equity": cost_equity,
                            "weight_equity": weight_equity,
                            "cost_debt": cost_debt,
                            "weight_debt": weight_debt,
                        },
                    )

        # Probability-weighted: (value - current) * probability
        if isinstance(ast, BinaryOp) and ast.op == "*" and context.sheet == "Scenarios":
            if isinstance(ast.left, BinaryOp) and ast.left.op == "-":
                value_ref = self._to_line_item_ref(ast.left.left, context) if isinstance(ast.left.left, Ref) else None
                current_ref = self._to_line_item_ref(ast.left.right, context) if isinstance(ast.left.right, Ref) else None
                prob_ref = self._to_line_item_ref(ast.right, context) if isinstance(ast.right, Ref) else None
                if value_ref and current_ref and prob_ref:
                    return FormulaSpec(
                        type=FormulaType.valuation,
                        subtype="probability_weighted",
                        params={"value": value_ref, "current": current_ref, "probability": prob_ref},
                    )

        # Kelly criterion: expected / total win
        if isinstance(ast, BinaryOp) and ast.op == "/" and context.sheet == "Scenarios":
            left_ref = self._to_line_item_ref(ast.left, context) if isinstance(ast.left, Ref) else None
            right_ref = self._to_line_item_ref(ast.right, context) if isinstance(ast.right, Ref) else None
            if left_ref and right_ref:
                return FormulaSpec(
                    type=FormulaType.valuation,
                    subtype="kelly",
                    params={"expected_value": left_ref, "total_win": right_ref},
                )

        # Valuation multiple (sheet-scoped)
        if isinstance(ast, BinaryOp) and ast.op == "*" and context.sheet in {"Valuation", "Scenarios"}:
            left_ref = self._to_line_item_ref(ast.left, context) if isinstance(ast.left, Ref) else None
            right_ref = self._to_line_item_ref(ast.right, context) if isinstance(ast.right, Ref) else None
            if left_ref and right_ref:
                return FormulaSpec(
                    type=FormulaType.valuation,
                    subtype="multiple",
                    params={"multiple": left_ref, "metric": right_ref},
                )

        return None

    def _constant_value(self, ast: Node) -> Optional[float]:
        """Evaluate constant-only formulas (no refs).

        If any ref appears, returns None so other matchers can handle it.
        """
        if isinstance(ast, Number):
            return ast.value
        if isinstance(ast, UnaryOp) and ast.op in {"+", "-"}:
            inner = self._constant_value(ast.expr)
            if inner is None:
                return None
            return inner if ast.op == "+" else -inner
        if isinstance(ast, BinaryOp) and ast.op in {"+", "-", "*", "/", "^"}:
            left = self._constant_value(ast.left)
            right = self._constant_value(ast.right)
            if left is None or right is None:
                return None
            if ast.op == "+":
                return left + right
            if ast.op == "-":
                return left - right
            if ast.op == "*":
                return left * right
            if ast.op == "/":
                return left / right if right != 0 else None
            if ast.op == "^":
                try:
                    return left ** right
                except (ValueError, OverflowError):
                    return None
        if isinstance(ast, FuncCall) and ast.name in {"SUM", "AVERAGE"}:
            total = 0.0
            for arg in ast.args:
                value = self._constant_value(arg)
                if value is None:
                    return None
                total += value
            if ast.name == "AVERAGE":
                return total / len(ast.args) if ast.args else None
            return total
        return None

    def _to_line_item_ref(self, node: Node, context: CellContext) -> Optional[LineItemRef]:
        """Resolve a cell reference to a semantic LineItemRef with time offset.

        The time offset t is computed by comparing the target column's period
        to the current cell's period.
        """
        if isinstance(node, Ref):
            sheet = node.sheet or context.sheet
            row_map = context.sheet_row_to_item.get(sheet, {})
            line_item_id = row_map.get(node.row)
            if not line_item_id:
                return None

            col_idx = _col_to_index(node.col)
            t, resolved = self._period_offset(
                context=context,
                target_sheet=sheet,
                target_col=col_idx,
            )

            return LineItemRef(id=line_item_id, t=t, resolved=resolved)

        if isinstance(node, Number):
            return None

        return None

    def _range_to_refs(self, node: Range, context: CellContext) -> List[LineItemRef]:
        """Resolve a range into a list of LineItemRefs (vertical or horizontal).

        Vertical ranges are expanded across rows with a fixed period.
        Horizontal ranges are expanded across columns for a single row.
        """
        sheet = node.start.sheet or context.sheet
        row_map = context.sheet_row_to_item.get(sheet, {})
        refs: List[LineItemRef] = []

        start_col_idx = _col_to_index(node.start.col)
        end_col_idx = _col_to_index(node.end.col)
        start_row = min(node.start.row, node.end.row)
        end_row = max(node.start.row, node.end.row)

        # Vertical range (same column)
        if node.start.col == node.end.col:
            t, resolved = self._period_offset(
                context=context,
                target_sheet=sheet,
                target_col=start_col_idx,
            )
            for row in range(start_row, end_row + 1):
                line_item_id = row_map.get(row)
                if line_item_id:
                    refs.append(LineItemRef(id=line_item_id, t=t, resolved=resolved))
            return refs

        # Horizontal range (same row)
        if node.start.row == node.end.row:
            line_item_id = row_map.get(node.start.row)
            if not line_item_id:
                return []
            for col_idx in range(min(start_col_idx, end_col_idx), max(start_col_idx, end_col_idx) + 1):
                t, resolved = self._period_offset(
                    context=context,
                    target_sheet=sheet,
                    target_col=col_idx,
                )
                if not resolved:
                    continue
                refs.append(LineItemRef(id=line_item_id, t=t, resolved=resolved))
            return refs

        return refs

    def _period_offset(self, context: CellContext, target_sheet: str, target_col: int) -> Tuple[int, bool]:
        """Return time offset when both columns resolve to known periods.

        Unmapped columns/periods are marked unresolved and keep `t=0` for
        runtime evaluation compatibility.
        """
        target_period = context.sheet_col_to_period.get(target_sheet, {}).get(target_col)
        current_period = context.sheet_col_to_period.get(context.sheet, {}).get(context.col)
        if target_period is None or current_period is None:
            return 0, False
        time_index = {period: idx for idx, period in enumerate(context.time_order)}
        if target_period not in time_index or current_period not in time_index:
            return 0, False
        return time_index[target_period] - time_index[current_period], True

    def _expr_from_node(self, node: Node, context: CellContext):
        """Convert an AST node into a small evaluable expression tree.

        Expression nodes are simple dicts that the dependency graph can eval.

        Example:
        Excel: (D15*1000000)/D9
        Expr: {"op": "/", "left": {"op": "*", "args": [LineItemRef("metric_a"), 1000000]}, "right": LineItemRef("metric_b")}
        """
        if isinstance(node, Ref):
            ref = self._to_line_item_ref(node, context)
            if ref is not None and not ref.resolved:
                return 0.0
            return ref
        if isinstance(node, Number):
            return float(node.value)
        if isinstance(node, UnaryOp):
            expr = self._expr_from_node(node.expr, context)
            if expr is None:
                return None
            if node.op == "+":
                return expr
            if node.op == "-":
                return {"op": "NEG", "arg": expr}
            return None
        if isinstance(node, BinaryOp):
            left = self._expr_from_node(node.left, context)
            right = self._expr_from_node(node.right, context)
            if left is None or right is None:
                return None
            if node.op in {"+", "*"}:
                args: List = []
                self._append_expr_arg(args, left, node.op)
                self._append_expr_arg(args, right, node.op)
                return {"op": node.op, "args": args}
            if node.op in {"-", "/", "^"}:
                return {"op": node.op, "left": left, "right": right}
            return None
        if isinstance(node, FuncCall) and node.name in {"SUM", "AVERAGE"}:
            args: List = []
            for arg in node.args:
                if isinstance(arg, Empty):
                    continue
                if isinstance(arg, Range):
                    args.extend(r for r in self._range_to_refs(arg, context) if r.resolved)
                    continue
                expr = self._expr_from_node(arg, context)
                if expr is None:
                    return None
                if isinstance(expr, float) and expr == 0.0 and isinstance(arg, Ref):
                    ref = self._to_line_item_ref(arg, context)
                    if ref is not None and not ref.resolved:
                        continue
                args.append(expr)
            if not args:
                if node.name == "SUM":
                    return 0.0
                return None
            return {"op": "SUM" if node.name == "SUM" else "AVG", "args": args}
        return None

    def _append_expr_arg(self, args: List, expr, op: str) -> None:
        """Flatten associative operations in expression trees."""
        if isinstance(expr, dict) and expr.get("op") == op and isinstance(expr.get("args"), list):
            args.extend(expr["args"])
        else:
            args.append(expr)

    def _contains_ref(self, expr) -> bool:
        """Check whether an expression tree contains any LineItemRef."""
        if isinstance(expr, LineItemRef):
            return True
        if isinstance(expr, dict):
            for value in expr.values():
                if self._contains_ref(value):
                    return True
        if isinstance(expr, (list, tuple, set)):
            for value in expr:
                if self._contains_ref(value):
                    return True
        return False

    def _extract_one_plus_ref(self, node: Node, context: CellContext) -> Optional[LineItemRef]:
        """Extract the ref from (1 + ref) or (ref + 1) expressions."""
        if not isinstance(node, BinaryOp) or node.op != "+":
            return None
        if isinstance(node.left, Number) and node.left.value == 1 and isinstance(node.right, Ref):
            return self._to_line_item_ref(node.right, context)
        if isinstance(node.right, Number) and node.right.value == 1 and isinstance(node.left, Ref):
            return self._to_line_item_ref(node.left, context)
        return None

    def _is_delta_expr(self, node: Node, context: CellContext) -> bool:
        """Check for (A[t] - A[t-1]) shapes for incremental ratios."""
        if not isinstance(node, BinaryOp) or node.op != "-":
            return False
        if not isinstance(node.left, Ref) or not isinstance(node.right, Ref):
            return False
        left_ref = self._to_line_item_ref(node.left, context)
        right_ref = self._to_line_item_ref(node.right, context)
        if not left_ref or not right_ref:
            return False
        return left_ref.id == right_ref.id and left_ref.t != right_ref.t

    def _flatten_add_sub(self, ast: Node) -> Optional[List[Tuple[str, Node]]]:
        """Flatten add/sub trees into signed term lists."""
        if isinstance(ast, BinaryOp) and ast.op in {"+", "-"}:
            left = self._flatten_add_sub(ast.left)
            right = self._flatten_add_sub(ast.right)
            if left is None or right is None:
                return None
            if ast.op == "+":
                return left + right
            inverted = [("-" if sign == "+" else "+", node) for sign, node in right]
            return left + inverted
        return [("+", ast)]

    def _flatten_binary(self, ast: Node, op: str) -> Optional[List[Node]]:
        """Flatten associative binary operators into a single list."""
        if isinstance(ast, BinaryOp) and ast.op == op:
            left = self._flatten_binary(ast.left, op)
            right = self._flatten_binary(ast.right, op)
            if left is None or right is None:
                return None
            return left + right
        return [ast]

    def _has_same_row_ref(self, context: CellContext, refs: List[LineItemRef]) -> bool:
        row_map = context.sheet_row_to_item.get(context.sheet, {})
        if not row_map:
            return False
        id_to_row = {item_id: row for row, item_id in row_map.items()}
        for ref in refs:
            row = id_to_row.get(ref.id)
            if row == context.row:
                return True
        return False


def _col_to_index(col: str) -> int:
    col = col.upper()
    index = 0
    for ch in col:
        if not ch.isalpha():
            continue
        index = index * 26 + (ord(ch) - ord("A") + 1)
    return index
