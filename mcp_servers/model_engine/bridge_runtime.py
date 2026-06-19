from __future__ import annotations

from collections.abc import Callable
from typing import Any


def graph_downstream_ids(graph: Any, roots: set[str]) -> set[str]:
  downstream: set[str] = set()
  stack = list(roots)
  while stack:
    node = stack.pop()
    for child in graph.adj.get(node, set()):
      if child in downstream:
        continue
      downstream.add(child)
      stack.append(child)
  return downstream


def bridge_model_bundle(
  file_path: str,
  cutoff: int,
  *,
  model_cache: Any,
  load_model_cache_fn: Callable[..., Any],
) -> Any | None:
  bundle = model_cache.get((file_path, cutoff))
  if bundle is not None:
    return bundle
  try:
    return load_model_cache_fn(file_path, historical_cutoff_year=cutoff)
  except Exception:
    return None


__all__ = [
  "bridge_model_bundle",
  "graph_downstream_ids",
]
