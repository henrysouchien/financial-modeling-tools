from __future__ import annotations

from collections.abc import Callable
from typing import Any, Optional

from .formula_ast import BinaryOp, Node, Number, Ref
from .models import FormulaSpec, FormulaType, LineItemRef


ToLineItemRef = Callable[[Node, Any], Optional[LineItemRef]]
ExtractGrowthOperands = Callable[[Node, Node], tuple[Optional[Ref], Optional[Ref]]]


def match_growth(
    ast: Node,
    context: Any,
    *,
    extract_growth_operands_fn: ExtractGrowthOperands,
    to_line_item_ref_fn: ToLineItemRef,
) -> Optional[FormulaSpec]:
    """Match growth compound patterns: base * (1 + rate)."""
    if not isinstance(ast, BinaryOp) or ast.op != "*":
        return None

    base, rate = extract_growth_operands_fn(ast.left, ast.right)
    if base is None or rate is None:
        base, rate = extract_growth_operands_fn(ast.right, ast.left)
    if base is None or rate is None:
        return None

    base_ref = to_line_item_ref_fn(base, context)
    rate_ref = to_line_item_ref_fn(rate, context)
    if base_ref is None or rate_ref is None:
        return None

    return FormulaSpec(
        type=FormulaType.growth,
        subtype="compound",
        params={"base": base_ref, "rate": rate_ref},
    )


def extract_growth_operands(
    base_candidate: Node,
    rate_candidate: Node,
) -> tuple[Optional[Ref], Optional[Ref]]:
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
