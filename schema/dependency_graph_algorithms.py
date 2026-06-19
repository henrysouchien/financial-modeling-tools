"""Graph ordering algorithms for dependency graph evaluation."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Set, Tuple


@dataclass
class _Component:
    nodes: List[str]
    is_cycle: bool


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


__all__ = [
    "_Component",
    "_components_from_adj",
    "_downstream_of",
    "_tarjan_sccs",
]
