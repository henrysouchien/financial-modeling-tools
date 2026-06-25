from __future__ import annotations

from collections.abc import Callable
from typing import Any, Optional

from .formula_ast import BinaryOp, Empty, FuncCall, Node, Number, Ref
from .models import FormulaSpec, FormulaType, LineItemRef


ToLineItemRef = Callable[[Node, Any], Optional[LineItemRef]]
ExtractOnePlusRef = Callable[[Node, Any], Optional[LineItemRef]]
MatchOffsetScenario = Callable[[FuncCall, Any], Optional[FormulaSpec]]


def match_valuation(
    ast: Node,
    context: Any,
    *,
    to_line_item_ref_fn: ToLineItemRef,
    extract_one_plus_ref_fn: ExtractOnePlusRef,
    match_offset_scenario_fn: MatchOffsetScenario,
) -> Optional[FormulaSpec]:
    """Match valuation-specific patterns: DCF, terminal value, CAPM, WACC, multiples."""
    if isinstance(ast, FuncCall):
        if ast.name == "OFFSET":
            offset_spec = match_offset_scenario_fn(ast, context)
            if offset_spec:
                return offset_spec
            return None
        if ast.name in {"NPV", "XNPV", "IRR"}:
            # Template-authored valuation specs carry structured params and
            # remain renderable. Parsed workbook functions do not, so keep
            # the original Excel formula as raw instead of inventing an
            # incomplete valuation spec that cannot be re-rendered.
            return None

    # DCF discount: CF / ((1 + r)^t)
    if isinstance(ast, BinaryOp) and ast.op == "/":
        cash_flow = (
            to_line_item_ref_fn(ast.left, context)
            if isinstance(ast.left, Ref)
            else None
        )
        if cash_flow and isinstance(ast.right, BinaryOp) and ast.right.op == "^":
            rate_ref = extract_one_plus_ref_fn(ast.right.left, context)
            period_ref = (
                to_line_item_ref_fn(ast.right.right, context)
                if isinstance(ast.right.right, Ref)
                else None
            )
            if rate_ref and period_ref:
                return FormulaSpec(
                    type=FormulaType.valuation,
                    subtype="dcf_discount",
                    params={
                        "cash_flow": cash_flow,
                        "discount_rate": rate_ref,
                        "period": period_ref,
                    },
                )

    # Terminal value: (CF * (1 + g)) / (r - g)
    if isinstance(ast, BinaryOp) and ast.op == "/":
        numerator = ast.left
        denominator = ast.right
        if (
            isinstance(numerator, BinaryOp)
            and numerator.op == "*"
            and isinstance(denominator, BinaryOp)
            and denominator.op == "-"
        ):
            cf_ref = None
            growth_ref = None
            if isinstance(numerator.left, Ref):
                cf_ref = to_line_item_ref_fn(numerator.left, context)
                growth_ref = extract_one_plus_ref_fn(numerator.right, context)
            if cf_ref is None and isinstance(numerator.right, Ref):
                cf_ref = to_line_item_ref_fn(numerator.right, context)
                growth_ref = extract_one_plus_ref_fn(numerator.left, context)

            discount_ref = (
                to_line_item_ref_fn(denominator.left, context)
                if isinstance(denominator.left, Ref)
                else None
            )
            growth_ref2 = (
                to_line_item_ref_fn(denominator.right, context)
                if isinstance(denominator.right, Ref)
                else None
            )

            if (
                cf_ref
                and growth_ref
                and discount_ref
                and growth_ref2
                and growth_ref.id == growth_ref2.id
            ):
                return FormulaSpec(
                    type=FormulaType.valuation,
                    subtype="terminal_value",
                    params={
                        "final_cf": cf_ref,
                        "growth": growth_ref,
                        "discount": discount_ref,
                    },
                )

    # CAPM: risk_free + (beta * erp)
    if isinstance(ast, BinaryOp) and ast.op == "+":
        left_ref = (
            to_line_item_ref_fn(ast.left, context) if isinstance(ast.left, Ref) else None
        )
        right_ref = (
            to_line_item_ref_fn(ast.right, context)
            if isinstance(ast.right, Ref)
            else None
        )
        left_mul = ast.left if isinstance(ast.left, BinaryOp) and ast.left.op == "*" else None
        right_mul = (
            ast.right if isinstance(ast.right, BinaryOp) and ast.right.op == "*" else None
        )
        if left_ref and right_mul:
            beta_ref = (
                to_line_item_ref_fn(right_mul.left, context)
                if isinstance(right_mul.left, Ref)
                else None
            )
            erp_ref = (
                to_line_item_ref_fn(right_mul.right, context)
                if isinstance(right_mul.right, Ref)
                else None
            )
            if beta_ref and erp_ref:
                return FormulaSpec(
                    type=FormulaType.valuation,
                    subtype="capm",
                    params={"risk_free": left_ref, "beta": beta_ref, "erp": erp_ref},
                )
        if right_ref and left_mul:
            beta_ref = (
                to_line_item_ref_fn(left_mul.left, context)
                if isinstance(left_mul.left, Ref)
                else None
            )
            erp_ref = (
                to_line_item_ref_fn(left_mul.right, context)
                if isinstance(left_mul.right, Ref)
                else None
            )
            if beta_ref and erp_ref:
                return FormulaSpec(
                    type=FormulaType.valuation,
                    subtype="capm",
                    params={"risk_free": right_ref, "beta": beta_ref, "erp": erp_ref},
                )

    # WACC: (cost_eq*weight_eq) + (cost_debt*weight_debt)
    if isinstance(ast, BinaryOp) and ast.op == "+":
        left_mul = ast.left if isinstance(ast.left, BinaryOp) and ast.left.op == "*" else None
        right_mul = (
            ast.right if isinstance(ast.right, BinaryOp) and ast.right.op == "*" else None
        )
        if left_mul and right_mul:
            cost_equity = (
                to_line_item_ref_fn(left_mul.left, context)
                if isinstance(left_mul.left, Ref)
                else None
            )
            weight_equity = (
                to_line_item_ref_fn(left_mul.right, context)
                if isinstance(left_mul.right, Ref)
                else None
            )
            cost_debt = (
                to_line_item_ref_fn(right_mul.left, context)
                if isinstance(right_mul.left, Ref)
                else None
            )
            weight_debt = (
                to_line_item_ref_fn(right_mul.right, context)
                if isinstance(right_mul.right, Ref)
                else None
            )
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
            value_ref = (
                to_line_item_ref_fn(ast.left.left, context)
                if isinstance(ast.left.left, Ref)
                else None
            )
            current_ref = (
                to_line_item_ref_fn(ast.left.right, context)
                if isinstance(ast.left.right, Ref)
                else None
            )
            prob_ref = (
                to_line_item_ref_fn(ast.right, context)
                if isinstance(ast.right, Ref)
                else None
            )
            if value_ref and current_ref and prob_ref:
                return FormulaSpec(
                    type=FormulaType.valuation,
                    subtype="probability_weighted",
                    params={
                        "value": value_ref,
                        "current": current_ref,
                        "probability": prob_ref,
                    },
                )

    # Kelly criterion: expected / total win
    if isinstance(ast, BinaryOp) and ast.op == "/" and context.sheet == "Scenarios":
        left_ref = (
            to_line_item_ref_fn(ast.left, context) if isinstance(ast.left, Ref) else None
        )
        right_ref = (
            to_line_item_ref_fn(ast.right, context)
            if isinstance(ast.right, Ref)
            else None
        )
        if left_ref and right_ref:
            return FormulaSpec(
                type=FormulaType.valuation,
                subtype="kelly",
                params={"expected_value": left_ref, "total_win": right_ref},
            )

    # Valuation multiple (sheet-scoped)
    if (
        isinstance(ast, BinaryOp)
        and ast.op == "*"
        and context.sheet in {"Valuation", "Scenarios"}
    ):
        left_ref = (
            to_line_item_ref_fn(ast.left, context) if isinstance(ast.left, Ref) else None
        )
        right_ref = (
            to_line_item_ref_fn(ast.right, context)
            if isinstance(ast.right, Ref)
            else None
        )
        if left_ref and right_ref:
            return FormulaSpec(
                type=FormulaType.valuation,
                subtype="multiple",
                params={"multiple": left_ref, "metric": right_ref},
            )

    return None


def match_offset_scenario(
    ast: FuncCall,
    context: Any,
    *,
    to_line_item_ref_fn: ToLineItemRef,
) -> Optional[FormulaSpec]:
    if ast.name != "OFFSET" or len(ast.args) not in {2, 3}:
        return None
    anchor_node, selector_node = ast.args[0], ast.args[1]
    if not isinstance(anchor_node, Ref) or not isinstance(selector_node, Ref):
        return None

    anchor_ref = to_line_item_ref_fn(anchor_node, context)
    selector_ref = to_line_item_ref_fn(selector_node, context)
    if anchor_ref is None or selector_ref is None:
        return None

    column_offset = 0
    if len(ast.args) == 3:
        column_arg = ast.args[2]
        if isinstance(column_arg, Empty):
            column_offset = 0
        elif isinstance(column_arg, Number) and float(column_arg.value).is_integer():
            column_offset = int(column_arg.value)
        else:
            return None

    return FormulaSpec(
        type=FormulaType.valuation,
        subtype="offset_scenario",
        params={
            "anchor": anchor_ref,
            "selector": selector_ref,
            "column_offset": column_offset,
        },
    )


def extract_one_plus_ref(
    node: Node,
    context: Any,
    *,
    to_line_item_ref_fn: ToLineItemRef,
) -> Optional[LineItemRef]:
    """Extract the ref from (1 + ref) or (ref + 1) expressions."""
    if not isinstance(node, BinaryOp) or node.op != "+":
        return None
    if isinstance(node.left, Number) and node.left.value == 1 and isinstance(node.right, Ref):
        return to_line_item_ref_fn(node.right, context)
    if isinstance(node.right, Number) and node.right.value == 1 and isinstance(node.left, Ref):
        return to_line_item_ref_fn(node.left, context)
    return None


__all__ = [
    "extract_one_plus_ref",
    "match_offset_scenario",
    "match_valuation",
]
