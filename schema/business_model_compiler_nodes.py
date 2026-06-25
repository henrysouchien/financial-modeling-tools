"""Node traversal helpers for :mod:`schema.business_model_compiler`."""

from __future__ import annotations

import sys
from typing import Any

from .business_model import DriverExpr, DriverNode
from .business_model_compiler_formula import _node_expr
from .business_model_compiler_plans import _SegmentCompilePlan


def _parent_attr(name: str, default: Any) -> Any:
    parent = sys.modules.get("schema.business_model_compiler")
    if parent is None:
        parent = sys.modules.get("business_model_compiler")
    return getattr(parent, name, default) if parent is not None else default


def _count_bm_rows(plans: list[_SegmentCompilePlan]) -> int:
    count_tree_rows = _parent_attr("_count_tree_rows", _count_tree_rows)

    rows = 0
    for plan in plans:
        if plan.segment is None:
            rows += 2
            continue
        rows += count_tree_rows(plan.segment.revenue_model.decomposition)
        rows += 2
    return rows


def _count_tree_rows(nodes: list[DriverNode]) -> int:
    count_tree_rows = _parent_attr("_count_tree_rows", _count_tree_rows)
    node_expr = _parent_attr("_node_expr", _node_expr)

    rows = 0
    for node in nodes:
        rows += count_tree_rows(node.children or [])
        target_type = node.compile_to.target_type
        if target_type not in {"assumption_row", "derived_row"}:
            continue
        expr = node_expr(node)
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
    if expr.type in {"product", "sum"}:
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
    iter_driver_nodes = _parent_attr("_iter_driver_nodes", _iter_driver_nodes)
    driver_expr_node_refs = _parent_attr("_driver_expr_node_refs", _driver_expr_node_refs)
    node_expr = _parent_attr("_node_expr", _node_expr)

    nodes_by_id = {
        node.id: node
        for node in iter_driver_nodes(segment.revenue_model.decomposition)
    }
    dependencies: set[str] = set()
    pending = list(driver_expr_node_refs(segment.revenue_model.consolidation_formula))
    while pending:
        node_id = pending.pop()
        if node_id in dependencies:
            continue
        dependencies.add(node_id)
        node = nodes_by_id.get(node_id)
        if node is not None:
            pending.extend(driver_expr_node_refs(node_expr(node)) - dependencies)
    return dependencies


def _direct_consolidation_growth_node_ids(segment: Any) -> set[str]:
    iter_driver_nodes = _parent_attr("_iter_driver_nodes", _iter_driver_nodes)
    driver_expr_node_refs = _parent_attr("_driver_expr_node_refs", _driver_expr_node_refs)
    node_expr = _parent_attr("_node_expr", _node_expr)

    nodes_by_id = {
        node.id: node
        for node in iter_driver_nodes(segment.revenue_model.decomposition)
    }
    result: set[str] = set()
    for node_id in driver_expr_node_refs(segment.revenue_model.consolidation_formula):
        node = nodes_by_id.get(node_id)
        expr = node_expr(node) if node is not None else None
        if expr is not None and expr.type == "growth":
            result.add(node_id)
    return result


def _primary_scenario_growth_node_id(segment: Any) -> str | None:
    consolidation_dependency_node_ids = _parent_attr(
        "_consolidation_dependency_node_ids",
        _consolidation_dependency_node_ids,
    )
    iter_driver_nodes = _parent_attr("_iter_driver_nodes", _iter_driver_nodes)
    node_expr = _parent_attr("_node_expr", _node_expr)

    consolidation_dependencies = consolidation_dependency_node_ids(segment)
    first_growth_node_id: str | None = None
    for node in iter_driver_nodes(segment.revenue_model.decomposition):
        expr = node_expr(node)
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
    primary_scenario_growth_node_id = _parent_attr(
        "_primary_scenario_growth_node_id",
        _primary_scenario_growth_node_id,
    )
    iter_driver_nodes = _parent_attr("_iter_driver_nodes", _iter_driver_nodes)
    node_expr = _parent_attr("_node_expr", _node_expr)

    node_id = primary_scenario_growth_node_id(segment)
    if node_id is None:
        return None
    for node in iter_driver_nodes(segment.revenue_model.decomposition):
        if node.id != node_id:
            continue
        expr = node_expr(node)
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
