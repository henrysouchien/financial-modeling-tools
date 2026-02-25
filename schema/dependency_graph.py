"""Dependency graph engine for schema-based financial models.

Purpose:
- Build a directed graph of line-item dependencies from FormulaSpecs.
- Compute values period-by-period without relying on Excel.

Core mechanics:
- Same-period edges (t=0) form a DAG plus possible cycles.
- Prior-period refs (t=-1) are treated as time recursion, not graph cycles.
- Strongly connected components (SCCs) are solved via fixed-point iteration.

Example:
If gross_profit = revenue - cogs and operating_income = gross_profit - opex,
the graph orders revenue/cogs/opex before gross_profit and operating_income.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import math
from typing import Dict, Iterable, List, Literal, Optional, Set, Tuple

from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
    ValueProvenance,
    PERIOD_MODE_YEARLY,
    shift_period,
)

logger = logging.getLogger(__name__)

@dataclass
class CycleBlock:
    nodes: List[str]
    max_iter: int = 100
    tol: float = 1e-6


@dataclass
class _Component:
    nodes: List[str]
    is_cycle: bool


class DependencyGraph:
    def __init__(self) -> None:
        self.model: Optional[FinancialModel] = None
        self.nodes: Set[str] = set()
        self.adj: Dict[str, Set[str]] = {}
        self.time_edges: Dict[str, Set[LineItemRef]] = {}
        self.cycle_blocks: List[CycleBlock] = []
        self.components: List[_Component] = []
        self.component_order: List[int] = []
        self.missing_refs: Set[str] = set()
        self._compute_has_recompute: bool = False
        self._compute_recompute: Optional[Set[str]] = None
        self._compute_propagate: Optional[Set[str]] = None  # downstream of user inputs
        self._ratio_zero_denominator_policy: str = "strict"
        self._cycle_fallback_policy: str = "auto"
        self._compute_periods: Optional[Set[int]] = None
        self._compute_seed_results: Optional[Dict[str, Dict[int, float]]] = None
        self._global_cycle_node_ids: Set[str] = set()

    def build(self, model: FinancialModel) -> None:
        """Build the dependency graph from FormulaSpecs.

        Steps:
        - Extract all LineItemRef dependencies from each FormulaSpec.
        - Build adjacency lists for same-period refs.
        - Detect SCCs and build a component DAG for ordering.
        """
        self.model = model
        self._global_cycle_node_ids = set()
        if not model.sheets:
            self.nodes = set()
            return

        model.build_index()
        self.nodes = set(model._index.keys())
        self.adj = {node: set() for node in self.nodes}
        self.time_edges = {node: set() for node in self.nodes}
        self.missing_refs = set()

        for item in model._index.values():
            for spec in self._iter_formula_specs(item):
                for ref in self._extract_refs(spec.params):
                    if ref.id not in self.nodes:
                        self.missing_refs.add(ref.id)
                        continue
                    if ref.t == 0:
                        self.adj[ref.id].add(item.id)
                    else:
                        self.time_edges[item.id].add(ref)

        self.components, self.component_order = self._components_from_adj(self.adj)
        self.cycle_blocks = [
            CycleBlock(nodes=comp.nodes)
            for comp in self.components
            if comp.is_cycle
        ]
        for cb in self.cycle_blocks:
            self._global_cycle_node_ids.update(cb.nodes)

    def compute(
        self,
        inputs: Dict[str, Dict[int, float]],
        recompute: Optional[Set[str]] = None,
        cycle_fallback_policy: Literal["off", "auto", "on", "auto_propagate"] = "auto",
        ratio_zero_denominator_policy: Literal[
            "strict", "auto_fallback_cached", "fallback_cached"
        ] = "strict",
        periods: Optional[Set[int]] = None,
        seed_results: Optional[Dict[str, Dict[int, float]]] = None,
    ) -> Dict[str, Dict[int, float]]:
        """Compute model values period-by-period given input overrides.

        For each period:
        - Seed input values and any stored ValueSeries (unless recompute).
        - Evaluate DAG components in topological order.
        - Solve any cycle blocks with fixed-point iteration.

        recompute:
        - Optional set of line_item_ids to force recomputation even if
          ValueSeries has existing values.

        periods:
        - Optional subset of periods to actively evaluate. Periods outside this
          set are resolved from input/value cache and optional seed_results.

        seed_results:
        - Optional baseline matrix used as fallback when a scoped compute needs
          values from non-evaluated periods.
        """
        if not self.model:
            raise ValueError("DependencyGraph.build() must be called before compute().")
        if cycle_fallback_policy not in {"off", "auto", "on", "auto_propagate"}:
            raise ValueError("cycle_fallback_policy must be one of: off, auto, on, auto_propagate")
        if ratio_zero_denominator_policy not in {"strict", "auto_fallback_cached", "fallback_cached"}:
            raise ValueError(
                "ratio_zero_denominator_policy must be one of: strict, auto_fallback_cached, fallback_cached"
            )

        time_order = self._time_order()
        time_index = {period: idx for idx, period in enumerate(time_order)}
        eval_periods = time_order if not periods else [period for period in time_order if period in periods]
        results: Dict[str, Dict[int, float]] = {}

        self._compute_has_recompute = bool(recompute)
        self._compute_recompute = recompute
        self._compute_periods = set(eval_periods) if periods else None
        self._compute_seed_results = seed_results
        # Compute downstream set of user input overrides — these items should
        # have their constant overrides bypassed so formula propagation works.
        if inputs and recompute:
            self._compute_propagate = self._downstream_of(set(inputs.keys()))
        else:
            self._compute_propagate = None
        self._ratio_zero_denominator_policy = ratio_zero_denominator_policy
        self._cycle_fallback_policy = cycle_fallback_policy

        try:
            for period in eval_periods:
                self._seed_inputs(period, inputs, results, recompute)
                components, order = self._components_for_period(period)
                singleton_node_ids: List[str] = []
                for comp_id in order:
                    comp = components[comp_id]
                    if comp.is_cycle:
                        self._solve_cycle_block(
                            comp.nodes,
                            period,
                            results,
                            time_index,
                            time_order,
                            inputs=inputs,
                        )
                        continue
                    node_id = comp.nodes[0]
                    singleton_node_ids.append(node_id)
                    self._eval_singleton_node(
                        node_id, period, results, time_index, time_order
                    )

                # Re-sweep unresolved singleton nodes to recover values when
                # unresolved ordering edges force an arbitrary first pass.
                for _ in range(len(singleton_node_ids)):
                    unresolved = 0
                    progressed = False
                    for node_id in singleton_node_ids:
                        if results.get(node_id, {}).get(period) is not None:
                            continue
                        unresolved += 1
                        resolved_now = self._eval_singleton_node(
                            node_id, period, results, time_index, time_order
                        )
                        if resolved_now:
                            progressed = True
                    if unresolved == 0 or not progressed:
                        break

            for period in eval_periods:
                for item_id, item in self.model._index.items():
                    if not item.overrides or period not in item.overrides:
                        continue
                    spec = item.overrides[period]
                    if spec.type == FormulaType.constant:
                        continue
                    value = self._eval(item_id, period, results, time_index, time_order)
                    if value is not None:
                        results.setdefault(item_id, {})[period] = value
                    else:
                        logger.debug("Override for %s period=%s returned None (missing deps)", item_id, period)
        finally:
            self._compute_periods = None
            self._compute_seed_results = None

        return results

    def _eval_singleton_node(
        self,
        node_id: str,
        period: int,
        results: Dict[str, Dict[int, float]],
        time_index: Dict[int, int],
        time_order: List[int],
    ) -> bool:
        """Evaluate a singleton component node for one period.

        Returns True when the node has a non-None value after evaluation.
        """
        existing = results.get(node_id, {}).get(period)
        if existing is not None:
            return True

        if node_id in self._global_cycle_node_ids and self._cycle_cached_fallback_enabled():
            if self._compute_has_recompute and not self._compute_propagate:
                cached_forced = self._cached_value_for_period(node_id, period, results)
                if cached_forced is not None:
                    results.setdefault(node_id, {})[period] = cached_forced
                    return True
            item = self.model.get_item(node_id)
            spec = self._spec_for_period(item, period)
            missing_dep = False
            if spec is not None:
                for ref in self._extract_refs(spec.params):
                    if self._value_of(ref, period, results, time_index, time_order) is None:
                        missing_dep = True
                        break
            value = self._eval(node_id, period, results, time_index, time_order)
            is_invalid = value is None or not math.isfinite(value)
            if (missing_dep or is_invalid):
                if not (
                    self._cycle_fallback_policy == "auto_propagate"
                    and self._compute_propagate
                    and node_id in self._compute_propagate
                ):
                    cached = self._cached_value_for_period(node_id, period, results)
                    if cached is not None:
                        results.setdefault(node_id, {})[period] = cached
                        return True
            if value is not None:
                results.setdefault(node_id, {})[period] = value
                return True
            return False

        item = self.model.get_item(node_id)
        spec = self._spec_for_period(item, period)
        missing_dep = False
        if spec is not None:
            for ref in self._extract_refs(spec.params):
                if self._value_of(ref, period, results, time_index, time_order) is None:
                    missing_dep = True
                    break
        value = self._eval(node_id, period, results, time_index, time_order)
        is_invalid = value is None or not math.isfinite(value)
        cached_computed = self._cached_computed_value_for_period(node_id, period)
        if (
            cached_computed is not None
            and self._compute_has_recompute
            and not self._compute_propagate
        ):
            results.setdefault(node_id, {})[period] = cached_computed
            return True
        if missing_dep or is_invalid:
            if cached_computed is not None:
                results.setdefault(node_id, {})[period] = cached_computed
                return True
        results.setdefault(node_id, {})[period] = value
        return results.get(node_id, {}).get(period) is not None

    def _solve_period_active_cycle_component(
        self,
        nodes: List[str],
        period: int,
        results: Dict[str, Dict[int, float]],
        time_index: Dict[int, int],
        time_order: List[int],
        inputs: Optional[Dict[str, Dict[int, float]]] = None,
    ) -> None:
        """Solve a global cycle component using period-active subcomponents."""
        order_adj, cycle_adj = self._active_adjs_for_period_subset(period, nodes)
        components, order = self._components_from_adj(cycle_adj, order_adj=order_adj)
        for comp_id in order:
            comp = components[comp_id]
            if comp.is_cycle:
                self._solve_cycle_block(comp.nodes, period, results, time_index, time_order, inputs)
                continue
            node_id = comp.nodes[0]
            if results.get(node_id, {}).get(period) is not None:
                continue
            item = self.model.get_item(node_id)
            spec = self._spec_for_period(item, period)
            missing_dep = False
            if spec is not None:
                for ref in self._extract_refs(spec.params):
                    if self._value_of(ref, period, results, time_index, time_order) is None:
                        missing_dep = True
                        break

            value = self._eval(node_id, period, results, time_index, time_order)
            is_invalid = value is None or not math.isfinite(value)
            if (missing_dep or is_invalid) and self._cycle_cached_fallback_enabled():
                if (
                    self._cycle_fallback_policy == "auto_propagate"
                    and self._compute_propagate
                    and node_id in self._compute_propagate
                ):
                    pass
                else:
                    cached = self._cached_value_for_period(node_id, period, results)
                    if cached is not None:
                        results.setdefault(node_id, {})[period] = cached
                        continue
            if value is not None:
                results.setdefault(node_id, {})[period] = value

    def _components_for_period(self, period: int) -> Tuple[List[_Component], List[int]]:
        """Build component order from formulas active in a single period."""
        order_adj, cycle_adj = self._active_adjs_for_period(period)
        return self._components_from_adj(cycle_adj, order_adj=order_adj)

    def _active_adjs_for_period(self, period: int) -> Tuple[Dict[str, Set[str]], Dict[str, Set[str]]]:
        """Build per-period same-period adjacency maps.

        Returns:
        - order adjacency: all same-period refs (resolved + unresolved)
        - cycle adjacency: resolved same-period refs only
        """
        order_adj: Dict[str, Set[str]] = {node: set() for node in self.nodes}
        cycle_adj: Dict[str, Set[str]] = {node: set() for node in self.nodes}
        if not self.model:
            return order_adj, cycle_adj
        for item in self.model._index.values():
            spec = self._spec_for_period(item, period)
            if spec is None:
                continue
            for ref in self._extract_refs(spec.params):
                if ref.id not in self.nodes:
                    continue
                if ref.t != 0:
                    continue
                order_adj[ref.id].add(item.id)
                if ref.resolved:
                    cycle_adj[ref.id].add(item.id)
        return order_adj, cycle_adj

    def _active_adjs_for_period_subset(
        self,
        period: int,
        nodes: List[str],
    ) -> Tuple[Dict[str, Set[str]], Dict[str, Set[str]]]:
        """Build per-period adjacency maps restricted to `nodes`."""
        node_set = set(nodes)
        order_adj: Dict[str, Set[str]] = {node: set() for node in node_set}
        cycle_adj: Dict[str, Set[str]] = {node: set() for node in node_set}
        if not self.model:
            return order_adj, cycle_adj
        for node in node_set:
            item = self.model.get_item(node)
            spec = self._spec_for_period(item, period)
            if spec is None:
                continue
            for ref in self._extract_refs(spec.params):
                if ref.id not in node_set or ref.t != 0:
                    continue
                order_adj[ref.id].add(node)
                if ref.resolved:
                    cycle_adj[ref.id].add(node)
        return order_adj, cycle_adj

    def _components_from_adj(
        self,
        cycle_adj: Dict[str, Set[str]],
        order_adj: Optional[Dict[str, Set[str]]] = None,
    ) -> Tuple[List[_Component], List[int]]:
        """Build SCC components and topological component order from adjacency."""
        if order_adj is None:
            order_adj = cycle_adj
        sccs = self._tarjan_sccs(cycle_adj)
        node_to_component: Dict[str, int] = {}
        components: List[_Component] = []
        for scc in sccs:
            if len(scc) == 1:
                node = scc[0]
                is_cycle = node in cycle_adj.get(node, set())
            else:
                is_cycle = True
            comp_id = len(components)
            for node in scc:
                node_to_component[node] = comp_id
            components.append(_Component(nodes=scc, is_cycle=is_cycle))

        comp_adj: Dict[int, Set[int]] = {i: set() for i in range(len(components))}
        comp_indegree: Dict[int, int] = {i: 0 for i in range(len(components))}
        for src, dsts in order_adj.items():
            for dst in dsts:
                c_src = node_to_component[src]
                c_dst = node_to_component[dst]
                if c_src == c_dst:
                    continue
                if c_dst not in comp_adj[c_src]:
                    comp_adj[c_src].add(c_dst)
                    comp_indegree[c_dst] += 1

        queue: List[int] = [i for i, deg in comp_indegree.items() if deg == 0]
        order: List[int] = []
        while queue:
            comp = queue.pop(0)
            order.append(comp)
            for nxt in comp_adj[comp]:
                comp_indegree[nxt] -= 1
                if comp_indegree[nxt] == 0:
                    queue.append(nxt)
        if len(order) != len(components):
            # Some ordering edges can still create cycles (e.g., unresolved
            # references). Keep partial topological order and append remaining.
            seen = set(order)
            for comp in range(len(components)):
                if comp not in seen:
                    order.append(comp)

        return components, order

    def _downstream_of(self, start_ids: Set[str]) -> Set[str]:
        """Return all nodes reachable downstream from start_ids via the adjacency graph."""
        visited: Set[str] = set()
        stack = list(start_ids)
        while stack:
            node = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            for dep in self.adj.get(node, set()):
                if dep not in visited:
                    stack.append(dep)
        return visited

    def get_dependents(self, line_item_id: str) -> List[str]:
        """Return downstream dependents for a line item."""
        return sorted(self.adj.get(line_item_id, set()))

    def get_dependencies(self, line_item_id: str) -> List[str]:
        """Return upstream dependencies for a line item."""
        deps = set()
        for src, dsts in self.adj.items():
            if line_item_id in dsts:
                deps.add(src)
        return sorted(deps)

    def _solve_cycle_block(
        self,
        nodes: List[str],
        period: int,
        results: Dict[str, Dict[int, float]],
        time_index: Dict[int, int],
        time_order: List[int],
        inputs: Optional[Dict[str, Dict[int, float]]] = None,
    ) -> None:
        """Iteratively solve a cycle block until convergence."""
        frozen_nodes: Set[str] = set()
        for node in nodes:
            # If the user provided an explicit override for this node+period,
            # keep it and freeze — don't let the model's constant spec overwrite it.
            if inputs and node in inputs and period in inputs[node]:
                frozen_nodes.add(node)
                continue
            item = self.model.get_item(node)
            spec = self._spec_for_period(item, period)
            if spec and spec.type == FormulaType.constant:
                value = self._eval(node, period, results, time_index, time_order)
                if value is not None:
                    results.setdefault(node, {})[period] = value
                frozen_nodes.add(node)
                continue
            if spec is None and results.get(node, {}).get(period) is not None:
                frozen_nodes.add(node)

        active_nodes = [node for node in nodes if node not in frozen_nodes]
        for node in active_nodes:
            if results.get(node, {}).get(period) is None:
                cached = self._cached_value_for_period(node, period, results)
                results.setdefault(node, {})[period] = cached if cached is not None else 0.0

        if not active_nodes:
            return

        converged = False
        residual = None
        for _ in range(100):
            prev = {n: results.get(n, {}).get(period) for n in active_nodes}
            for n in active_nodes:
                results.setdefault(n, {})[period] = self._eval(n, period, results, time_index, time_order)
            converged, residual = self._converged(prev, active_nodes, period, results, 1e-6)
            if converged:
                break

        pathological = all(results.get(node, {}).get(period) is None for node in active_nodes)
        if (not converged or pathological) and self._cycle_cached_fallback_enabled():
            fallback_count = 0
            for node in active_nodes:
                if (
                    self._cycle_fallback_policy == "auto_propagate"
                    and self._compute_propagate
                    and node in self._compute_propagate
                ):
                    # Keep propagated nodes on their recomputed path; don't snap
                    # them back to cache during sensitivity scenarios.
                    continue
                cached = self._cached_value_for_period(node, period, results)
                if cached is None:
                    continue
                results.setdefault(node, {})[period] = cached
                fallback_count += 1
            if fallback_count:
                logger.debug(
                    "Cycle fallback applied for period=%s nodes=%s converged=%s residual=%s count=%s",
                    period,
                    active_nodes,
                    converged,
                    residual,
                    fallback_count,
                )

    def _converged(
        self,
        prev: Dict[str, Optional[float]],
        nodes: List[str],
        period: int,
        results: Dict[str, Dict[int, float]],
        tol: float,
    ) -> Tuple[bool, Optional[float]]:
        max_residual = 0.0
        for n in nodes:
            current = results.get(n, {}).get(period)
            prior = prev.get(n)
            if prior is None or current is None:
                return False, None
            residual = abs(current - prior)
            if residual > max_residual:
                max_residual = residual
            if residual > tol:
                return False, max_residual
        return True, max_residual

    def _eval(
        self,
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
            return float(params.get("value")) if params.get("value") is not None else None

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

            if params.get("function") in {"SUM", "AVERAGE"}:
                items = params.get("items", [])
                values = [self._eval_expr(i, period, results, time_index, time_order) for i in items]
                non_none = [v for v in values if v is not None]
                # If ALL values are None, return None (no data — AVERAGE will ignore this)
                # Otherwise treat None as 0 in SUM (matches Excel: blank cells are 0)
                if not non_none:
                    return None
                if params.get("function") == "AVERAGE":
                    return sum(non_none) / len(non_none)
                return sum(non_none)

            operands = params.get("operands")
            if isinstance(operands, list) and operands:
                operator = "+"
                start_index = 0
                if isinstance(operands[0], str) and operands[0] in {"+", "-", "*", "/"}:
                    operator = operands[0]
                    start_index = 1
                values = [self._eval_expr(i, period, results, time_index, time_order) for i in operands[start_index:]]
                if not values:
                    return None
                non_none = [v for v in values if v is not None]
                # If ALL values are None, return None (no data)
                if not non_none:
                    return None
                # For + operator, sum non-None values (None treated as 0)
                if operator == "+":
                    return sum(non_none)
                # For other operators, None propagates
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
            # Treat None additions/subtractions as 0 (empty cells in Excel)
            return beginning + sum(v or 0 for v in add_values) - sum(v or 0 for v in sub_values)

        return None

    def _seed_inputs(
        self,
        period: int,
        inputs: Dict[str, Dict[int, float]],
        results: Dict[str, Dict[int, float]],
        recompute: Optional[Set[str]] = None,
    ) -> None:
        """Seed inputs and existing value series into the results matrix."""
        if inputs:
            for line_item_id, by_period in inputs.items():
                if period in by_period:
                    results.setdefault(line_item_id, {})[period] = by_period[period]

        if not self.model:
            return
        for item_id, item in self.model._index.items():
            if results.get(item_id, {}).get(period) is not None:
                continue
            if recompute and item_id in recompute:
                spec = self._spec_for_period(item, period)
                if spec is not None and spec.type != FormulaType.constant:
                    continue
            if not item.values:
                continue
            value_cell = item.values.values.get(period)
            if value_cell is None:
                continue
            results.setdefault(item_id, {})[period] = value_cell.value

    def _time_order(self) -> List[int]:
        if not self.model:
            return []
        ts = self.model.time_structure
        if ts.historical_periods or ts.projection_periods:
            return list(ts.historical_periods) + list(ts.projection_periods)
        return list(ts.historical_years) + list(ts.projection_years)

    def _spec_for_period(self, item: LineItem, period: int) -> Optional[FormulaSpec]:
        """Select the appropriate FormulaSpec for a period (override/historical/projected)."""
        recompute_skip = False
        if item.overrides and period in item.overrides:
            override = item.overrides[period]
            # When this item is downstream of user input overrides and the
            # override is just a constant snapshot, prefer the real formula
            # so upstream changes can propagate.
            if (override.type == FormulaType.constant
                    and self._compute_propagate and item.id in self._compute_propagate
                    and item.projected and item.projected.type != FormulaType.constant):
                recompute_skip = True  # fall through to projected/historical below
            else:
                return override

        if item.formula_periods is not None and period not in item.formula_periods:
            # If we skipped a constant override due to recompute, the formula_periods
            # mask was built assuming those periods had constants.  Allow the
            # projected formula to apply anyway.
            if not recompute_skip:
                return None

        if not self.model:
            return item.projected or item.historical

        ts = self.model.time_structure
        historical_periods = list(ts.historical_periods) or list(ts.historical_years)
        projection_periods = list(ts.projection_periods) or list(ts.projection_years)
        if period in historical_periods:
            # Don't fall back to projected formula for historical years
            # Empty historical cells should evaluate to None (0 in arithmetic)
            return item.historical
        if period in projection_periods:
            # Don't fall back to historical formula for projection years
            # Empty projection cells should evaluate to None (0 in arithmetic)
            return item.projected
        return item.projected or item.historical

    def _spec_for_year(self, item: LineItem, year: int) -> Optional[FormulaSpec]:
        """Backward-compatible alias for migration tooling."""
        return self._spec_for_period(item, year)

    def _value_of(
        self,
        obj,
        period: int,
        results: Dict[str, Dict[int, float]],
        time_index: Dict[int, int],
        time_order: List[int],
    ) -> Optional[float]:
        """Resolve a LineItemRef or primitive value for a given period."""
        if obj is None:
            return None
        if isinstance(obj, dict):
            return self._eval_expr(obj, period, results, time_index, time_order)
        if isinstance(obj, LineItemRef):
            idx = time_index.get(period)
            if idx is None:
                return None
            target_idx = idx + obj.t
            if target_idx < 0:
                target_period = self._bootstrap_period(period, obj.t)
                if target_period is None:
                    return None
                return self._input_value_fallback(obj.id, target_period)
            if target_idx >= len(time_order):
                return None
            target_period = time_order[target_idx]
            val = results.get(obj.id, {}).get(target_period)
            if (
                val is None
                and self._compute_seed_results is not None
                and self._compute_periods is not None
                and target_period not in self._compute_periods
            ):
                val = self._compute_seed_results.get(obj.id, {}).get(target_period)
            if val is None:
                # Forward ref to a period not yet evaluated — fall back to
                # input-provenance cached value (safe for Q4 plug patterns
                # where Annual is hard-coded imported data with no formula).
                val = self._input_value_fallback(obj.id, target_period)
            return val
        if isinstance(obj, (int, float)):
            return float(obj)
        return None

    def _input_value_fallback(self, line_item_id: str, target_period: int) -> Optional[float]:
        """Look up a genuine input value for bootstrapping prior-period references.

        Only returns values with input provenance (imported_other, input,
        imported_edgar, imported_fmp). Never falls back to derived/computed
        cached values — that would mask formula bugs.
        """
        item = self.model.get_item(line_item_id)
        if not item or not item.values:
            return None
        vc = item.values.values.get(target_period)
        if vc is None or vc.value is None:
            return None
        if vc.provenance in (
            ValueProvenance.input,
            ValueProvenance.imported_other,
            ValueProvenance.imported_edgar,
            ValueProvenance.imported_fmp,
        ):
            return vc.value
        # provenance == computed → derived cached value, don't use
        return None

    def _bootstrap_period(self, period: int, t: int) -> Optional[int]:
        if not self.model:
            return None
        period_mode = self.model.time_structure.period_mode or PERIOD_MODE_YEARLY
        return shift_period(period, t, period_mode)

    def _cached_value_for_period(
        self,
        item_id: str,
        period: int,
        results: Dict[str, Dict[int, float]],
    ) -> Optional[float]:
        existing = results.get(item_id, {}).get(period)
        if not self.model:
            return existing
        item = self.model.get_item(item_id)
        if item.values:
            value_cell = item.values.values.get(period)
            if value_cell is not None and value_cell.value is not None:
                return value_cell.value
        if self._compute_seed_results is not None:
            seeded = self._compute_seed_results.get(item_id, {}).get(period)
            if seeded is not None:
                return seeded
        return existing

    def _cached_computed_value_for_period(
        self,
        item_id: str,
        period: int,
    ) -> Optional[float]:
        if not self.model:
            return None
        item = self.model.get_item(item_id)
        if not item.values:
            return None
        value_cell = item.values.values.get(period)
        if value_cell is None or value_cell.value is None:
            return None
        if value_cell.provenance == ValueProvenance.computed:
            return value_cell.value
        period_mode = self.model.time_structure.period_mode or PERIOD_MODE_YEARLY
        if period_mode != PERIOD_MODE_YEARLY and value_cell.provenance in (
            ValueProvenance.imported_other,
            ValueProvenance.imported_edgar,
            ValueProvenance.imported_fmp,
        ):
            return value_cell.value
        return None

    def _ratio_cached_fallback_enabled(self) -> bool:
        if self._ratio_zero_denominator_policy == "fallback_cached":
            return True
        if self._ratio_zero_denominator_policy == "auto_fallback_cached":
            return self._compute_has_recompute
        return False

    def _cycle_cached_fallback_enabled(self) -> bool:
        if self._cycle_fallback_policy == "on":
            return True
        if self._cycle_fallback_policy in {"auto", "auto_propagate"}:
            return self._compute_has_recompute
        return False

    def _eval_expr(
        self,
        expr,
        period: int,
        results: Dict[str, Dict[int, float]],
        time_index: Dict[int, int],
        time_order: List[int],
    ) -> Optional[float]:
        """Evaluate a small expression tree (from pattern matcher).

        Supported ops: +, -, *, /, ^, SUM, AVG, NEG.

        Example:
        Expr: {"op": "*", "args": [LineItemRef("revenue"), 0.2]}
        Result: revenue * 0.2
        """
        if expr is None:
            return None
        if isinstance(expr, LineItemRef):
            return self._value_of(expr, period, results, time_index, time_order)
        if isinstance(expr, (int, float)):
            return float(expr)
        if isinstance(expr, dict):
            op = expr.get("op")
            if op in {"+", "*", "SUM", "AVG"}:
                args = expr.get("args", [])
                values = [self._eval_expr(arg, period, results, time_index, time_order) for arg in args]
                non_none = [v for v in values if v is not None]
                # For inline + expressions (e.g., in AVERAGE args), blank+blank = 0 in Excel
                # This differs from top-level SUM which returns None if all None
                if op == "+":
                    return sum(v or 0 for v in values)
                # For SUM, if ALL values are None, return None (no data)
                if op == "SUM":
                    if not non_none:
                        return None
                    return sum(non_none)
                # For AVG, average non-None values (matches Excel AVERAGE behavior)
                if op == "AVG":
                    if not non_none:
                        return None
                    return sum(non_none) / len(non_none)
                # For *, None propagates (can't multiply with missing value)
                if any(v is None for v in values):
                    return None
                if op == "*":
                    result = 1.0
                    for value in values:
                        result *= value
                    return result
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
        return None

    def _apply_scale_fn(self, value: float, scale_fn: str) -> float:
        scale_fn = scale_fn.strip()
        if not scale_fn:
            return value
        if scale_fn.startswith("/"):
            return value / float(scale_fn[1:])
        if scale_fn.startswith("*"):
            return value * float(scale_fn[1:])
        return value

    def _iter_formula_specs(self, item: LineItem) -> Iterable[FormulaSpec]:
        if item.historical:
            yield item.historical
        if item.projected:
            yield item.projected
        if item.overrides:
            for spec in item.overrides.values():
                if spec.type != FormulaType.constant:
                    yield spec

    def _extract_refs(self, obj) -> List[LineItemRef]:
        refs: List[LineItemRef] = []
        if obj is None:
            return refs
        if isinstance(obj, LineItemRef):
            return [obj]
        if isinstance(obj, dict):
            for value in obj.values():
                refs.extend(self._extract_refs(value))
            return refs
        if isinstance(obj, (list, tuple, set)):
            for value in obj:
                refs.extend(self._extract_refs(value))
        return refs

    def _tarjan_sccs(self, adj: Optional[Dict[str, Set[str]]] = None) -> List[List[str]]:
        if adj is None:
            adj = self.adj
        index = 0
        stack: List[str] = []
        indices: Dict[str, int] = {}
        lowlinks: Dict[str, int] = {}
        onstack: Set[str] = set()
        sccs: List[List[str]] = []

        def strongconnect(node: str) -> None:
            nonlocal index
            indices[node] = index
            lowlinks[node] = index
            index += 1
            stack.append(node)
            onstack.add(node)

            for neighbor in adj.get(node, set()):
                if neighbor not in indices:
                    strongconnect(neighbor)
                    lowlinks[node] = min(lowlinks[node], lowlinks[neighbor])
                elif neighbor in onstack:
                    lowlinks[node] = min(lowlinks[node], indices[neighbor])

            if lowlinks[node] == indices[node]:
                component: List[str] = []
                while True:
                    w = stack.pop()
                    onstack.remove(w)
                    component.append(w)
                    if w == node:
                        break
                sccs.append(component)

        for node in self.nodes:
            if node not in indices:
                strongconnect(node)

        return sccs
