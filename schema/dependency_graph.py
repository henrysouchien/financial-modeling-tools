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
from typing import Any, Dict, Iterable, Iterator, List, Literal, Optional, Set, Tuple

from .dependency_graph_algorithms import (
    _Component,
    _components_from_adj,
    _downstream_of,
    _tarjan_sccs,
)
from .dependency_graph_eval import (
    _apply_scale_fn,
    _eval,
    _eval_expr,
    _eval_offset_scenario,
    _eval_valuation,
)
from .dependency_graph_helpers import (
    _col_to_index,  # noqa: F401 - compatibility alias for schema.dependency_graph imports
    _index_to_col,  # noqa: F401 - compatibility alias for schema.dependency_graph imports
    _is_input_provenance,  # noqa: F401 - compatibility alias for schema.dependency_graph imports
    _numeric_label_value,  # noqa: F401 - compatibility alias for schema.dependency_graph imports
    _offset_column,  # noqa: F401 - compatibility alias for schema.dependency_graph imports
)
from .dependency_graph_values import (
    _bootstrap_period,
    _cached_computed_value_for_period,
    _cached_value_for_period,
    _column_for_period,
    _cycle_cached_fallback_enabled,
    _fixed_cell_anchor_period,
    _historical_periods,
    _input_value_fallback,
    _period_for_anchor_last,
    _projection_periods,
    _ratio_cached_fallback_enabled,
    _read_with_fallback,
    _row_item_id,
    _seed_inputs,
    _spec_for_period,
    _sum_range_target_periods,
    _target_sheet_all_periods,
    _target_sheet_projection_periods,
    _time_order,
    _value_of,
)
from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,  # noqa: F401 - compatibility alias for schema.dependency_graph imports
    LineItem,
    LineItemRef,
    ValueProvenance,  # noqa: F401 - compatibility alias for schema.dependency_graph imports
    PERIOD_MODE_YEARLY,  # noqa: F401 - compatibility alias for schema.dependency_graph imports
    shift_period,  # noqa: F401 - compatibility alias for schema.dependency_graph imports
)
from .refs import line_item_ref_from_obj

logger = logging.getLogger(__name__)


@dataclass
class CycleBlock:
    nodes: List[str]
    max_iter: int = 100
    tol: float = 1e-6


class DependencyGraph:
    _apply_scale_fn = _apply_scale_fn
    _bootstrap_period = _bootstrap_period
    _cached_computed_value_for_period = _cached_computed_value_for_period
    _cached_value_for_period = _cached_value_for_period
    _column_for_period = _column_for_period
    _components_from_adj = _components_from_adj
    _cycle_cached_fallback_enabled = _cycle_cached_fallback_enabled
    _downstream_of = _downstream_of
    _eval = _eval
    _eval_expr = _eval_expr
    _eval_offset_scenario = _eval_offset_scenario
    _eval_valuation = _eval_valuation
    _fixed_cell_anchor_period = _fixed_cell_anchor_period
    _historical_periods = _historical_periods
    _input_value_fallback = _input_value_fallback
    _period_for_anchor_last = _period_for_anchor_last
    _projection_periods = _projection_periods
    _ratio_cached_fallback_enabled = _ratio_cached_fallback_enabled
    _read_with_fallback = _read_with_fallback
    _row_item_id = _row_item_id
    _seed_inputs = _seed_inputs
    _spec_for_period = _spec_for_period
    _sum_range_target_periods = _sum_range_target_periods
    _tarjan_sccs = _tarjan_sccs
    _target_sheet_all_periods = _target_sheet_all_periods
    _target_sheet_projection_periods = _target_sheet_projection_periods
    _time_order = _time_order
    _value_of = _value_of

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
        self._item_sheet_map: Dict[str, str] = {}
        self._item_row_map: Dict[str, int] = {}
        self._item_column_map: Dict[str, str] = {}
        self._row_item_map: Dict[Tuple[str, int], List[str]] = {}
        self._cell_item_map: Dict[Tuple[str, int, str], str] = {}
        self._deferred_nodes: Set[str] = set()
        self._deferred_cone: Set[str] = set()
        self._sum_range_nodes: Set[str] = set()
        self._period_anchor_last_nodes: Set[str] = set()

    def build(self, model: FinancialModel) -> None:
        """Build the dependency graph from FormulaSpecs.

        Steps:
        - Extract all LineItemRef dependencies from each FormulaSpec.
        - Build adjacency lists for same-period refs.
        - Detect SCCs and build a component DAG for ordering.
        """
        self.model = model
        self._global_cycle_node_ids = set()
        self._item_sheet_map = {}
        self._item_row_map = {}
        self._item_column_map = {}
        self._row_item_map = {}
        self._cell_item_map = {}
        self._deferred_nodes = set()
        self._deferred_cone = set()
        self._sum_range_nodes = set()
        self._period_anchor_last_nodes = set()
        if not model.sheets:
            self.nodes = set()
            return

        model.build_index()
        self.nodes = set(model._index.keys())
        self._item_sheet_map = {
            item.id: sheet_name
            for sheet_name, sheet in model.sheets.items()
            for section in sheet.sections
            for item in section.line_items
        }
        for sheet_name, sheet in model.sheets.items():
            for section in sheet.sections:
                for item in section.line_items:
                    row = int(item.row)
                    self._item_row_map[item.id] = row
                    self._row_item_map.setdefault((sheet_name, row), []).append(item.id)
                    if item.column:
                        column = str(item.column).upper()
                        self._item_column_map[item.id] = column
                        self._cell_item_map[(sheet_name, row, column)] = item.id
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

        self._identify_deferred_nodes()
        self._validate_deferred_cone_invariant()

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
        propagate_roots: Optional[Set[str]] = None,
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

        propagate_roots:
        - Optional subset of input ids whose downstream constant overrides
          should be bypassed. Defaults to all input ids.
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
            roots = set(inputs.keys()) if propagate_roots is None else set(propagate_roots)
            self._compute_propagate = self._downstream_of(roots) if roots else set()
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
                        active_nodes = [node for node in comp.nodes if node not in self._deferred_nodes]
                        if not active_nodes:
                            continue
                        if len(active_nodes) != len(comp.nodes):
                            self._solve_period_active_cycle_component(
                                active_nodes,
                                period,
                                results,
                                time_index,
                                time_order,
                                inputs=inputs,
                            )
                            singleton_node_ids.extend(active_nodes)
                            continue
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
                    if node_id in self._deferred_nodes:
                        continue
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
                    if item_id in self._deferred_nodes:
                        continue
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

            self._evaluate_deferred_nodes(
                results,
                time_index,
                time_order,
                eval_periods,
                inputs,
            )
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
            if self._compute_has_recompute and self._compute_propagate is None:
                cached_forced = self._cached_value_for_period(node_id, period, results)
                if cached_forced is not None:
                    results.setdefault(node_id, {})[period] = cached_forced
                    return True
            item = self.model.get_item(node_id)
            spec = self._spec_for_period(item, period)
            missing_dep = False
            if spec is not None and not self._is_sum_range_spec(spec):
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
        if spec is not None and not self._is_sum_range_spec(spec):
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
            and self._compute_propagate is None
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
            if spec is not None and not self._is_sum_range_spec(spec):
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

    def downstream_for_periods(
        self,
        start_ids: Iterable[str],
        periods: Iterable[int],
    ) -> Set[str]:
        """Return item ids reachable through formulas active in any period.

        This is intentionally independent from compute-time propagation state:
        constant period overrides stay pinned, and historical fallback formulas
        are not treated as projection-period edges.
        """
        return self._reachable_for_periods(start_ids, periods, direction="downstream")

    def upstream_for_periods(
        self,
        target_ids: Iterable[str],
        periods: Iterable[int],
    ) -> Set[str]:
        """Return item ids feeding targets through formulas active in any period."""
        return self._reachable_for_periods(target_ids, periods, direction="upstream")

    def active_adjacency_for_periods(self, periods: Iterable[int]) -> Dict[str, Set[str]]:
        """Build item-level adjacency for formulas active in any supplied period."""
        active_periods = {int(period) for period in periods}
        adj: Dict[str, Set[str]] = {node: set() for node in self.nodes}
        if not self.model or not active_periods:
            return adj

        for item in self.model._index.values():
            for period in active_periods:
                spec = self._static_spec_for_period(item, period)
                if spec is None:
                    continue
                for ref in self._extract_refs(spec.params):
                    if ref.id not in self.nodes:
                        continue
                    adj[ref.id].add(item.id)
        return adj

    def _reachable_for_periods(
        self,
        start_ids: Iterable[str],
        periods: Iterable[int],
        *,
        direction: Literal["downstream", "upstream"],
    ) -> Set[str]:
        adj = self.active_adjacency_for_periods(periods)
        if direction == "upstream":
            reverse: Dict[str, Set[str]] = {node: set() for node in self.nodes}
            for src, dsts in adj.items():
                for dst in dsts:
                    reverse.setdefault(dst, set()).add(src)
            adj = reverse

        visited: Set[str] = set()
        stack = [item_id for item_id in start_ids if item_id in self.nodes]
        while stack:
            node = stack.pop()
            if node in visited:
                continue
            visited.add(node)
            for dep in adj.get(node, set()):
                if dep not in visited:
                    stack.append(dep)
        return visited

    def _static_spec_for_period(self, item: LineItem, period: int) -> Optional[FormulaSpec]:
        """Resolve the formula active for a period without compute fallbacks."""
        if item.overrides and period in item.overrides:
            return item.overrides[period]

        if item.formula_periods is not None and period not in item.formula_periods:
            return None

        ts = self.model.time_structure if self.model else None
        projection_periods = set(ts.projection_periods if ts else [])
        historical_periods = set(ts.historical_periods if ts else [])
        if not projection_periods and ts is not None:
            projection_periods = set(ts.projection_years)
        if not historical_periods and ts is not None:
            historical_periods = set(ts.historical_years)

        if period in historical_periods:
            return item.historical
        if period in projection_periods:
            return item.projected
        if item.projected is not None:
            return item.projected
        if item.historical is not None:
            return item.historical
        return None

    def _identify_deferred_nodes(self) -> None:
        """Classify SUM_RANGE and period_anchor='last' nodes plus downstream closure."""
        if not self.model:
            return
        sum_range_nodes: Set[str] = set()
        period_anchor_last_nodes: Set[str] = set()

        for item in self.model._index.values():
            for path, spec in self._iter_all_formula_specs_with_paths(item):
                if self._is_sum_range_spec(spec):
                    self._coerce_sum_range_target(spec, item.id, path)
                    sum_range_nodes.add(item.id)

                for ref, _ref_path in self._iter_refs_with_paths(spec.params, f"{path}.params"):
                    if ref.period_anchor != "last":
                        continue
                    try:
                        target_item = self.model.get_item(ref.id)
                    except KeyError:
                        continue
                    if target_item.column is None:
                        period_anchor_last_nodes.add(item.id)

        direct = sum_range_nodes | period_anchor_last_nodes
        deferred = self._downstream_of(direct) if direct else set()
        self._sum_range_nodes = sum_range_nodes
        self._period_anchor_last_nodes = period_anchor_last_nodes
        self._deferred_nodes = deferred
        self._deferred_cone = deferred - direct

    def _evaluate_deferred_nodes(
        self,
        results: Dict[str, Dict[int, float]],
        time_index: Dict[int, int],
        time_order: List[int],
        eval_periods: List[int],
        inputs: Dict[str, Dict[int, float]],
    ) -> None:
        if not self._deferred_nodes:
            return

        eval_set = set(self._compute_periods) if self._compute_periods is not None else set(eval_periods)
        if not eval_set:
            return

        for node_id in self._deferred_nodes:
            inputs_for_node = (inputs or {}).get(node_id, {})
            if node_id not in results:
                continue
            for period in list(results[node_id].keys()):
                if period not in inputs_for_node:
                    del results[node_id][period]
            if not results[node_id]:
                del results[node_id]

        ordered_nodes = self._ordered_deferred_nodes()
        ordered_periods = sorted(eval_set, key=lambda p: time_index.get(p, len(time_order)))
        for node_id in ordered_nodes:
            for period in ordered_periods:
                if period not in time_index:
                    continue
                self._eval_singleton_node(node_id, period, results, time_index, time_order)

    def _ordered_deferred_nodes(self) -> List[str]:
        deferred = set(self._deferred_nodes)
        if not deferred:
            return []
        order_adj: Dict[str, Set[str]] = {node: set() for node in deferred}
        indegree: Dict[str, int] = {node: 0 for node in deferred}
        for src in sorted(deferred):
            for dst in self.adj.get(src, set()):
                if dst in deferred:
                    order_adj[src].add(dst)
                    indegree[dst] += 1

        queue = sorted(node for node, degree in indegree.items() if degree == 0)
        ordered: List[str] = []
        while queue:
            node = queue.pop(0)
            ordered.append(node)
            for dst in sorted(order_adj.get(node, set())):
                indegree[dst] -= 1
                if indegree[dst] == 0:
                    queue.append(dst)
                    queue.sort()

        if len(ordered) != len(deferred):
            ordered.extend(sorted(deferred - set(ordered)))
        return ordered

    def _validate_deferred_cone_invariant(self) -> None:
        if not self.model:
            return
        for node_id in sorted(self._deferred_nodes):
            try:
                item = self.model.get_item(node_id)
            except KeyError:
                continue
            if item.column is None:
                continue
            for path, spec in self._iter_all_formula_specs_with_paths(item):
                self._validate_period_invariant_obj(spec, node_id, path)

    def _validate_period_invariant_obj(self, obj: Any, node_id: str, path: str) -> None:
        if obj is None:
            return
        if isinstance(obj, FormulaSpec):
            params = obj.params or {}
            if self._is_sum_range_spec(obj):
                self._coerce_sum_range_target(obj, node_id, path)
                for key, value in params.items():
                    if key == "target":
                        continue
                    self._validate_period_invariant_obj(value, node_id, f"{path}.params.{key}")
                return
            self._validate_period_invariant_obj(params, node_id, f"{path}.params")
            return

        ref = line_item_ref_from_obj(obj)
        if ref is not None:
            if ref.period_anchor == "last":
                return
            try:
                target_item = self.model.get_item(ref.id)
            except KeyError:
                return
            if target_item is not None and target_item.column is not None:
                return
            raise ValueError(
                f"Fixed-cell SUM_RANGE cone member {node_id!r} uses first-anchor "
                f"time-axis ref to {ref.id!r} via {path}; fixed-cell cone members "
                "must be period-invariant."
            )

        if isinstance(obj, dict):
            for key, value in obj.items():
                self._validate_period_invariant_obj(value, node_id, f"{path}.{key}")
            return
        if isinstance(obj, (list, tuple, set)):
            for index, value in enumerate(obj):
                self._validate_period_invariant_obj(value, node_id, f"{path}[{index}]")

    def _coerce_sum_range_target(
        self,
        spec: FormulaSpec,
        item_id: str,
        path: str,
    ) -> LineItemRef:
        target_obj = (spec.params or {}).get("target")
        target_ref = line_item_ref_from_obj(target_obj)
        if target_ref is None:
            raise ValueError(
                f"SUM_RANGE in {item_id!r} has malformed 'target' at {path}.params.target "
                f"(expected single LineItemRef-coercible; got {type(target_obj).__name__}: "
                f"{target_obj!r})"
            )
        return target_ref

    def _is_sum_range_spec(self, spec: Optional[FormulaSpec]) -> bool:
        return (
            spec is not None
            and spec.type == FormulaType.arithmetic
            and (spec.params or {}).get("function") == "SUM_RANGE"
        )

    def _iter_all_formula_specs(self, item: LineItem) -> Iterator[FormulaSpec]:
        for _path, spec in self._iter_all_formula_specs_with_paths(item):
            yield spec

    def _iter_all_formula_specs_with_paths(
        self,
        item: LineItem,
    ) -> Iterator[Tuple[str, FormulaSpec]]:
        if item.historical is not None:
            yield "historical", item.historical
        if item.projected is not None:
            yield "projected", item.projected
        if item.overrides:
            for period, spec in item.overrides.items():
                if spec is not None:
                    yield f"overrides[{int(period)}]", spec

    def _iter_refs_with_paths(self, obj: Any, path: str) -> Iterator[Tuple[LineItemRef, str]]:
        if obj is None:
            return
        if isinstance(obj, FormulaSpec):
            yield from self._iter_refs_with_paths(obj.params, f"{path}.params")
            return
        coerced = line_item_ref_from_obj(obj)
        if coerced is not None:
            yield coerced, path
            return
        if isinstance(obj, dict):
            for key, value in obj.items():
                yield from self._iter_refs_with_paths(value, f"{path}.{key}")
            return
        if isinstance(obj, (list, tuple, set)):
            for index, value in enumerate(obj):
                yield from self._iter_refs_with_paths(value, f"{path}[{index}]")

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
        if isinstance(obj, FormulaSpec):
            return self._extract_refs(obj.params)
        coerced = line_item_ref_from_obj(obj)
        if coerced is not None:
            return [coerced]
        if isinstance(obj, dict):
            for value in obj.values():
                refs.extend(self._extract_refs(value))
            return refs
        if isinstance(obj, (list, tuple, set)):
            for value in obj:
                refs.extend(self._extract_refs(value))
        return refs
