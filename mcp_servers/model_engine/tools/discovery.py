from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from typing import Any, get_type_hints


@dataclass(frozen=True)
class ModelDiscoveryDeps:
  discover_all_axes: Callable[..., Any]
  make_edgar_financials_fetcher: Callable[[], Any]
  axis_priority: Callable[[str | None], int | None]
  segment_revenue_observation_list: Callable[[Any], list[Any] | None]
  serialize_year_values: Callable[[dict[Any, Any]], dict[str, Any]]


def model_discover_segments_handler(
  *,
  deps: ModelDiscoveryDeps,
  ticker: str,
  most_recent_fy: int,
  n_historical: int = 5,
) -> dict[str, Any]:
  try:
    result = deps.discover_all_axes(
      ticker=ticker,
      fetcher=deps.make_edgar_financials_fetcher(),
      most_recent_fy=most_recent_fy,
      n_historical=n_historical,
    )
    axes = []
    for profile in result.profiles:
      axes.append(
        {
          "axis": profile.axis_used,
          "priority": deps.axis_priority(profile.axis_used),
          "segment_count": len(profile.segments),
          "segments": [
            {
              "name": segment.name,
              "edgar_member": segment.edgar_member,
              "revenue_observations": [
                observation.model_dump(mode="json")
                for observation in (
                  deps.segment_revenue_observation_list(segment) or []
                )
              ],
            }
            for segment in profile.segments
          ],
        }
      )

    return {
      "status": "ok",
      "ticker": result.ticker,
      "consolidated_revenue": deps.serialize_year_values(result.total_revenue_check),
      "axes_found": len(axes),
      "axes": axes,
    }
  except Exception as exc:
    return {"status": "error", "error": str(exc)}


ParentNamespaceProvider = Callable[[], dict[str, Any]]


@dataclass(frozen=True)
class ModelDiscoveryFunctions:
  model_discover_segments: Callable[..., dict[str, Any]]


def _parent_model_discovery_deps(
  parent_namespace: ParentNamespaceProvider,
) -> ModelDiscoveryDeps:
  return parent_namespace()["_model_discovery_deps"]()


def _parent_handler(
  parent_namespace: ParentNamespaceProvider,
  name: str,
) -> Callable[..., dict[str, Any]]:
  return parent_namespace()[name]


def _bind_parent_module(
  function: Callable[..., dict[str, Any]],
  parent_namespace: ParentNamespaceProvider,
) -> Callable[..., dict[str, Any]]:
  parent_module = parent_namespace().get("__name__")
  if isinstance(parent_module, str):
    function.__module__ = parent_module
    function.__qualname__ = function.__name__
    function.__annotations__ = {**get_type_hints(function), "return": dict}
  return function


def _register_tool(
  mcp: Any,
  function: Callable[..., dict[str, Any]],
) -> Callable[..., dict[str, Any]]:
  registered = mcp.tool()(function)
  return registered or function


def register_model_discovery_tools(
  mcp: Any,
  *,
  parent_namespace: ParentNamespaceProvider,
  tool_names: Sequence[str] | None = None,
  functions: ModelDiscoveryFunctions | None = None,
) -> ModelDiscoveryFunctions:
  functions = functions or build_model_discovery_tool_functions(
    parent_namespace=parent_namespace,
  )
  selected_tool_names = tool_names or tuple(functions.__dict__)
  for name in selected_tool_names:
    functions = replace(
      functions,
      **{
        name: _register_tool(
          mcp,
          _bind_parent_module(getattr(functions, name), parent_namespace),
        )
      },
    )
  return functions


def build_model_discovery_tool_functions(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> ModelDiscoveryFunctions:
  def model_discover_segments(
    ticker: str,
    most_recent_fy: int,
    n_historical: int = 5,
  ) -> dict:
    """Discover all validated EDGAR segment axes for a ticker."""
    return _parent_handler(parent_namespace, "_model_discover_segments_handler")(
      deps=_parent_model_discovery_deps(parent_namespace),
      ticker=ticker,
      most_recent_fy=most_recent_fy,
      n_historical=n_historical,
    )

  return ModelDiscoveryFunctions(
    model_discover_segments=model_discover_segments,
  )


__all__ = [
  "ModelDiscoveryFunctions",
  "ModelDiscoveryDeps",
  "build_model_discovery_tool_functions",
  "model_discover_segments_handler",
  "register_model_discovery_tools",
]
