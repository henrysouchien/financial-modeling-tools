"""Analysis tools built on top of the schema dependency graph.

These helpers run scenarios, sweep inputs, and trace sensitivities by
recomputing model outputs with the DependencyGraph.
"""

from __future__ import annotations

from itertools import product
from typing import Dict, Iterable, List, Optional, Set

from .dependency_graph import DependencyGraph
from .models import PERIOD_MODE_QUARTERLY5, FinancialModel, ItemType, encode_period


def run_scenario(
    model: FinancialModel,
    inputs: Dict[str, Dict[int, float]],
    outputs: Optional[List[str]] = None,
) -> Dict[str, Dict[int, float]]:
    """Set assumptions, propagate through the graph, and return outputs.

    If outputs is None, returns all derived line items.
    """
    if not model._index:
        model.build_index()
    graph = DependencyGraph()
    graph.build(model)
    results = graph.compute(inputs or {})

    if outputs is None:
        output_ids = [item_id for item_id, item in model._index.items() if item.item_type == ItemType.derived]
    else:
        output_ids = outputs

    return {output_id: results.get(output_id, {}) for output_id in output_ids}


def run_scenarios(
    model: FinancialModel,
    scenarios: Dict[str, Dict[str, Dict[int, float]]],
    outputs: Optional[List[str]] = None,
) -> Dict[str, Dict[str, Dict[int, float]]]:
    """Run multiple scenarios and return results keyed by scenario name."""
    if not model._index:
        model.build_index()
    results: Dict[str, Dict[str, Dict[int, float]]] = {}
    for name, inputs in scenarios.items():
        results[name] = run_scenario(model, inputs, outputs)
    return results


def sensitivity(
    model: FinancialModel,
    vary: Dict[str, List[float]],
    outputs: List[str],
    base_period: Optional[int] = None,
    base_year: Optional[int] = None,
) -> Dict:
    """Sweep one or more inputs across ranges and return an output grid.

    Returns a list of runs with explicit input values and outputs at the base time key.
    """
    period = base_period if base_period is not None else _resolve_base_year(model, base_year)
    input_ids = list(vary.keys())
    runs = []

    for values in product(*(vary[i] for i in input_ids)):
        inputs = {input_id: {period: value} for input_id, value in zip(input_ids, values)}
        scenario_results = run_scenario(model, inputs, outputs)
        outputs_at_period = {
            output_id: scenario_results.get(output_id, {}).get(period)
            for output_id in outputs
        }
        runs.append({"inputs": dict(zip(input_ids, values)), "outputs": outputs_at_period})

    return {
        "base_year": period,
        "base_period": period,
        "inputs": input_ids,
        "outputs": outputs,
        "runs": runs,
    }


def trace_sensitivity(
    model: FinancialModel,
    input_id: str,
    delta: float,
    output_id: Optional[str] = None,
) -> List[Dict]:
    """Trace the impact of a delta on one input through the dependency chain.

    If output_id is provided, the chain is filtered to nodes on the path
    from input_id to output_id.
    """
    if not model._index:
        model.build_index()
    graph = DependencyGraph()
    graph.build(model)
    period = _default_period(model)

    base_results = graph.compute({})
    base_value = base_results.get(input_id, {}).get(period)
    if base_value is None:
        adjusted_value = delta
    else:
        adjusted_value = base_value + delta

    # Recompute downstream nodes from formulas instead of using cached values
    downstream = _downstream_nodes(graph, input_id)
    after_results = graph.compute({input_id: {period: adjusted_value}}, recompute=downstream)

    impacted = downstream
    if output_id:
        impacted &= _upstream_nodes(graph, output_id)
        impacted.add(output_id)
        impacted.add(input_id)

    ordered = _order_nodes(graph, impacted)

    chain = []
    for node_id in ordered:
        before = base_results.get(node_id, {}).get(period)
        after = after_results.get(node_id, {}).get(period)
        change = None
        pct_change = None
        if before is not None and after is not None:
            change = after - before
            if before != 0:
                pct_change = change / before
        chain.append(
            {
                "id": node_id,
                "label": model.get_item(node_id).label if node_id in model._index else node_id,
                "before": before,
                "after": after,
                "change": change,
                "pct_change": pct_change,
            }
        )

    return chain


# Helpers


def _resolve_base_year(model: FinancialModel, base_year: Optional[int]) -> int:
    """Map a base_year to the correct period key, handling quarterly mode.

    In quarterly5 mode, base_year=2024 maps to the Annual slot (20245).
    In yearly mode or when base_year is None, falls through to _default_period.
    """
    if base_year is None:
        return _default_period(model)
    mode = model.time_structure.period_mode
    if mode == PERIOD_MODE_QUARTERLY5:
        # If already a period key (e.g. 20245), use as-is
        if base_year > 9999:
            return base_year
        return encode_period(base_year, 5, mode)
    return base_year


def _default_period(model: FinancialModel) -> int:
    """Pick a reasonable default period (last projection, else last historical)."""
    ts = model.time_structure
    periods = list(ts.projection_periods) or list(ts.historical_periods)
    if not periods:
        periods = list(ts.projection_years) or list(ts.historical_years)
    if not periods:
        raise ValueError("Model has no time structure periods")
    return periods[-1]


def _default_year(model: FinancialModel) -> int:
    """Backward-compatible alias for callers expecting year naming."""
    return _default_period(model)


def _downstream_nodes(graph: DependencyGraph, start: str) -> Set[str]:
    """Return all downstream nodes reachable from the start node."""
    visited: Set[str] = set()
    stack = [start]
    while stack:
        node = stack.pop()
        if node in visited:
            continue
        visited.add(node)
        for dep in graph.adj.get(node, set()):
            if dep not in visited:
                stack.append(dep)
    return visited


def _upstream_nodes(graph: DependencyGraph, target: str) -> Set[str]:
    """Return all upstream nodes that feed the target node."""
    reverse: Dict[str, Set[str]] = {node: set() for node in graph.nodes}
    for src, dsts in graph.adj.items():
        for dst in dsts:
            reverse[dst].add(src)

    visited: Set[str] = set()
    stack = [target]
    while stack:
        node = stack.pop()
        if node in visited:
            continue
        visited.add(node)
        for dep in reverse.get(node, set()):
            if dep not in visited:
                stack.append(dep)
    return visited


def _order_nodes(graph: DependencyGraph, nodes: Iterable[str]) -> List[str]:
    """Order nodes using the graph's component topological order."""
    node_set = set(nodes)
    ordered: List[str] = []
    if graph.components and graph.component_order:
        for comp_id in graph.component_order:
            for node in graph.components[comp_id].nodes:
                if node in node_set:
                    ordered.append(node)
    else:
        ordered = sorted(node_set)
    return ordered
