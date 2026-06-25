from __future__ import annotations

from typing import Any

from .business_model import DriverExpr, DriverNode, NodeRef
from .business_model_compiler_errors import BusinessModelCompileError
from .models import FormulaSpec, FormulaType, LineItemRef
from .segments import _ratio_formula


def _compile_formula(
    expr: DriverExpr,
    *,
    segment_id: str,
    referencing_node_id: str,
    node_lookup: dict[str, str],
    non_materialized: dict[str, str],
    current_item_id: str,
) -> FormulaSpec:
    if expr.type == "product":
        return FormulaSpec(
            type=FormulaType.arithmetic,
            params={
                "operands": [
                    "*",
                    *[
                        _resolve_ref_expr(
                            operand,
                            segment_id=segment_id,
                            referencing_node_id=referencing_node_id,
                            node_lookup=node_lookup,
                            non_materialized=non_materialized,
                            current_item_id=current_item_id,
                        )
                        for operand in expr.params.operands
                    ],
                ]
            },
        )

    if expr.type == "sum":
        return FormulaSpec(
            type=FormulaType.arithmetic,
            params={
                "operands": [
                    "+",
                    *[
                        _resolve_ref_expr(
                            operand,
                            segment_id=segment_id,
                            referencing_node_id=referencing_node_id,
                            node_lookup=node_lookup,
                            non_materialized=non_materialized,
                            current_item_id=current_item_id,
                        )
                        for operand in expr.params.operands
                    ],
                ]
            },
        )

    if expr.type == "derived":
        numerator = _resolve_line_item_ref(
            expr.params.numerator,
            segment_id=segment_id,
            referencing_node_id=referencing_node_id,
            node_lookup=node_lookup,
            non_materialized=non_materialized,
            current_item_id=current_item_id,
        )
        denominator = _resolve_line_item_ref(
            expr.params.denominator,
            segment_id=segment_id,
            referencing_node_id=referencing_node_id,
            node_lookup=node_lookup,
            non_materialized=non_materialized,
            current_item_id=current_item_id,
        )
        if numerator.t == 0 and denominator.t == 0:
            return _ratio_formula(numerator.id, denominator.id)
        return FormulaSpec(
            type=FormulaType.ratio,
            params={"numerator": numerator, "denominator": denominator},
        )

    if expr.type == "roll_forward":
        return FormulaSpec(
            type=FormulaType.roll_forward,
            params={
                "beginning": _resolve_line_item_ref(
                    expr.params.beginning,
                    segment_id=segment_id,
                    referencing_node_id=referencing_node_id,
                    node_lookup=node_lookup,
                    non_materialized=non_materialized,
                    current_item_id=current_item_id,
                ),
                "additions": [
                    _resolve_line_item_ref(
                        ref,
                        segment_id=segment_id,
                        referencing_node_id=referencing_node_id,
                        node_lookup=node_lookup,
                        non_materialized=non_materialized,
                        current_item_id=current_item_id,
                    )
                    for ref in expr.params.additions
                ],
                "subtractions": [
                    _resolve_line_item_ref(
                        ref,
                        segment_id=segment_id,
                        referencing_node_id=referencing_node_id,
                        node_lookup=node_lookup,
                        non_materialized=non_materialized,
                        current_item_id=current_item_id,
                    )
                    for ref in expr.params.subtractions
                ],
            },
        )

    if expr.type == "growth":
        raise BusinessModelCompileError(
            f"growth expression on {segment_id}.{referencing_node_id} must be handled as an assumption_row pair"
        )
    if expr.type == "external":
        raise BusinessModelCompileError(
            f"external expression on {segment_id}.{referencing_node_id} does not compile to a FormulaSpec"
        )
    raise BusinessModelCompileError(f"unsupported driver expression {expr.type!r}")


def _resolve_ref_expr(
    ref: NodeRef,
    *,
    segment_id: str,
    referencing_node_id: str,
    node_lookup: dict[str, str],
    non_materialized: dict[str, str],
    current_item_id: str,
) -> Any:
    resolved = _resolve_line_item_ref(
        ref,
        segment_id=segment_id,
        referencing_node_id=referencing_node_id,
        node_lookup=node_lookup,
        non_materialized=non_materialized,
        current_item_id=current_item_id,
    )
    if ref.sign == -1:
        return {"op": "NEG", "arg": resolved}
    return resolved


def _resolve_line_item_ref(
    ref: NodeRef,
    *,
    segment_id: str,
    referencing_node_id: str,
    node_lookup: dict[str, str],
    non_materialized: dict[str, str],
    current_item_id: str,
) -> LineItemRef:
    if ref.node_id == "self":
        return LineItemRef(id=current_item_id, t=ref.t)

    target_id = node_lookup.get(ref.node_id)
    if target_id is None:
        reason = "target is not materialized"
        if ref.node_id in non_materialized:
            reason = f"target compiles to {non_materialized[ref.node_id]!r} and is not materialized"
        raise BusinessModelCompileError(
            f"node {segment_id}.{referencing_node_id!r} cannot resolve NodeRef {ref.node_id!r}: {reason}"
        )
    return LineItemRef(id=target_id, t=ref.t)


def _node_expr(node: DriverNode) -> DriverExpr | None:
    if node.children_role == "decomposition":
        return node.children_formula
    return node.driver
