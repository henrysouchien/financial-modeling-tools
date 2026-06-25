"""Period-active graph traversal helpers for :mod:`schema.dependency_graph`."""

from __future__ import annotations

from typing import Any, Dict, Iterable, Literal, List, Optional, Set, Tuple

from .dependency_graph_algorithms import _Component
from .models import FormulaSpec, LineItem


def _components_for_period(self: Any, period: int) -> Tuple[List[_Component], List[int]]:
    """Build component order from formulas active in a single period."""
    order_adj, cycle_adj = self._active_adjs_for_period(period)
    return self._components_from_adj(cycle_adj, order_adj=order_adj)


def _active_adjs_for_period(self: Any, period: int) -> Tuple[Dict[str, Set[str]], Dict[str, Set[str]]]:
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
    self: Any,
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
    self: Any,
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
    self: Any,
    target_ids: Iterable[str],
    periods: Iterable[int],
) -> Set[str]:
    """Return item ids feeding targets through formulas active in any period."""
    return self._reachable_for_periods(target_ids, periods, direction="upstream")


def active_adjacency_for_periods(self: Any, periods: Iterable[int]) -> Dict[str, Set[str]]:
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
    self: Any,
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


def _static_spec_for_period(self: Any, item: LineItem, period: int) -> Optional[FormulaSpec]:
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


__all__ = [
    "_active_adjs_for_period",
    "_active_adjs_for_period_subset",
    "_components_for_period",
    "_reachable_for_periods",
    "_static_spec_for_period",
    "active_adjacency_for_periods",
    "downstream_for_periods",
    "upstream_for_periods",
]
