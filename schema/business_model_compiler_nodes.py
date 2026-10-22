"""Node traversal helpers for :mod:`schema.business_model_compiler`."""

from __future__ import annotations

from typing import Any

from . import business_model_compiler_formula as compiler_formula
from .business_model import (
    UNIT_DIMENSION,
    Dimension,
    DriverExpr,
    DriverNode,
    NodeMeasure,
    node_measure,
    product_dimension,
    product_scale_exponent,
)
from .business_model_compiler_errors import BusinessModelCompileError
from .business_model_compiler_ratio import ratio_averages_operand, ratio_scale_exponent
from .business_model_compiler_plans import _SegmentCompilePlan


def _product_measure_exponent(
    expr: DriverExpr,
    *,
    output: NodeMeasure,
    output_node_id: str,
    nodes_by_id: dict[str, DriverNode],
    current_node: DriverNode | None = None,
) -> int:
    """Check the authored product's dimensions and derive its numeric scaling."""
    if expr.type != "product":
        return 0
    operands: list[DriverNode] = []
    for ref in expr.params.operands:
        operand = current_node if ref.node_id == "self" else nodes_by_id.get(ref.node_id)
        if operand is None:
            raise BusinessModelCompileError(
                f"product {output_node_id!r} cannot resolve operand node {ref.node_id!r}"
            )
        operands.append(operand)
    measures = [node_measure(node) for node in operands]
    dimensions: list[Dimension] = [UNIT_DIMENSION[measure.unit] for measure in measures]
    dimension = product_dimension(dimensions)
    if dimension is None or dimension != UNIT_DIMENSION[output.unit]:
        operand_details = " × ".join(
            f"{node.id!r} ({measure.unit.value}, {measure.model_scale}, basis={measure.basis})"
            for node, measure in zip(operands, measures)
        )
        repair_unit = (
            "per_count" if "entity" in dimensions else "per_day" if "time" in dimensions else None
        )
        repairs = (
            "; " + "; ".join(
                f"declare {node.id!r} unit {repair_unit!r}"
                for node, operand_dimension in zip(operands, dimensions)
                if operand_dimension == "money"
            )
            if repair_unit is not None and "money" in dimensions else ""
        )
        raise BusinessModelCompileError(
            f"product {output_node_id!r}: {operand_details} cannot produce "
            f"({output.unit.value}, {output.model_scale}, basis={output.basis}); "
            f"operand dimensions {dimensions!r}, output dimension {UNIT_DIMENSION[output.unit]!r}{repairs}"
        )
    return product_scale_exponent(measures, output)


def _ratio_projection_terms(
    expr: DriverExpr,
    *,
    output: NodeMeasure,
    output_node_id: str,
    nodes_by_id: dict[str, DriverNode],
    current_node: DriverNode | None = None,
) -> tuple[float, bool]:
    """A derived node's unit/scale conversion and whether its denominator is averaged.

    The conversion (`ratio_scale_exponent`) and the average
    (`ratio_averages_operand`) are the ones the node's history applies
    (`schema.build_bm_node_history.RegisteredBmFormula`).
    """
    if expr.type != "derived":
        return 1.0, False
    operands: list[DriverNode] = []
    for ref in (expr.params.numerator, expr.params.denominator):
        operand = current_node if ref.node_id == "self" else nodes_by_id.get(ref.node_id)
        if operand is None:
            raise BusinessModelCompileError(
                f"derived {output_node_id!r} cannot resolve operand node {ref.node_id!r}"
            )
        operands.append(operand)
    numerator, denominator = operands
    exponent = ratio_scale_exponent(
        node_measure(numerator), node_measure(denominator), output
    )
    return float(10**exponent), ratio_averages_operand(denominator)


def _count_bm_rows(plans: list[_SegmentCompilePlan]) -> int:
    rows = 0
    for plan in plans:
        if plan.segment is None:
            rows += 2
            continue
        rows += _count_tree_rows(plan.segment.revenue_model.decomposition)
        rows += 2
    return rows


def _count_tree_rows(nodes: list[DriverNode]) -> int:
    rows = 0
    for node in nodes:
        rows += _count_tree_rows(node.children or [])
        target_type = node.compile_to.target_type
        if target_type not in {"assumption_row", "derived_row"}:
            continue
        expr = compiler_formula._node_expr(node)
        rows += 2 if expr and expr.type == "growth" else 1
    return rows


def _iter_driver_nodes(nodes: list[DriverNode]):
    for node in nodes:
        yield node
        if node.children:
            yield from _iter_driver_nodes(node.children)


def _driver_expr_node_refs(expr: DriverExpr | None) -> set[str]:
    if expr is None:
        return set()
    if expr.type == "growth":
        node_id = expr.params.base.node_id
        return set() if node_id == "self" else {node_id}
    if expr.type == "product" or expr.type == "sum":
        return {ref.node_id for ref in expr.params.operands if ref.node_id != "self"}
    if expr.type == "derived":
        return {
            ref.node_id
            for ref in (expr.params.numerator, expr.params.denominator)
            if ref.node_id != "self"
        }
    if expr.type == "roll_forward":
        refs = [expr.params.beginning, *expr.params.additions, *expr.params.subtractions]
        return {ref.node_id for ref in refs if ref.node_id != "self"}
    return set()


def _consolidation_dependency_node_ids(segment: Any) -> set[str]:
    nodes_by_id = {
        node.id: node
        for node in _iter_driver_nodes(segment.revenue_model.decomposition)
    }
    dependencies: set[str] = set()
    pending = list(_driver_expr_node_refs(segment.revenue_model.consolidation_formula))
    while pending:
        node_id = pending.pop()
        if node_id in dependencies:
            continue
        dependencies.add(node_id)
        node = nodes_by_id.get(node_id)
        if node is not None:
            pending.extend(_driver_expr_node_refs(compiler_formula._node_expr(node)) - dependencies)
    return dependencies


def _direct_consolidation_growth_node_ids(segment: Any) -> set[str]:
    nodes_by_id = {
        node.id: node
        for node in _iter_driver_nodes(segment.revenue_model.decomposition)
    }
    result: set[str] = set()
    for node_id in _driver_expr_node_refs(segment.revenue_model.consolidation_formula):
        node = nodes_by_id.get(node_id)
        expr = compiler_formula._node_expr(node) if node is not None else None
        if expr is not None and expr.type == "growth":
            result.add(node_id)
    return result


def _primary_scenario_growth_node_id(segment: Any) -> str | None:
    consolidation_dependencies = _consolidation_dependency_node_ids(segment)
    first_growth_node_id: str | None = None
    for node in _iter_driver_nodes(segment.revenue_model.decomposition):
        expr = compiler_formula._node_expr(node)
        if expr is None or expr.type != "growth" or node.compile_to.target_type != "assumption_row":
            continue
        if node.id not in consolidation_dependencies:
            continue
        first_growth_node_id = first_growth_node_id or node.id
        factor_names = {str(factor).lower() for factor in (node.factors or [])}
        if "volume" in factor_names:
            return node.id
    return first_growth_node_id


def _primary_scenario_owner_rate_id(segment: Any) -> str | None:
    node_id = _primary_scenario_growth_node_id(segment)
    if node_id is None:
        return None
    for node in _iter_driver_nodes(segment.revenue_model.decomposition):
        if node.id != node_id:
            continue
        expr = compiler_formula._node_expr(node)
        if expr is None or expr.type != "growth":
            return None
        return f"bm.{segment.id}.{node.id}__{expr.params.rate_key}"
    return None


def _build_node_lookup(segment: Any) -> tuple[dict[str, str], dict[str, str]]:
    node_lookup: dict[str, str] = {}
    non_materialized: dict[str, str] = {}

    def walk(nodes: list[DriverNode]) -> None:
        for node in nodes:
            target_type = node.compile_to.target_type
            if target_type in {"assumption_row", "derived_row"}:
                node_lookup[node.id] = f"bm.{segment.id}.{node.id}"
            else:
                non_materialized[node.id] = target_type
            if node.children:
                walk(node.children)

    walk(segment.revenue_model.decomposition)
    return node_lookup, non_materialized


__all__ = [
    "_build_node_lookup",
    "_consolidation_dependency_node_ids",
    "_count_bm_rows",
    "_count_tree_rows",
    "_direct_consolidation_growth_node_ids",
    "_driver_expr_node_refs",
    "_iter_driver_nodes",
    "_primary_scenario_growth_node_id",
    "_primary_scenario_owner_rate_id",
]
