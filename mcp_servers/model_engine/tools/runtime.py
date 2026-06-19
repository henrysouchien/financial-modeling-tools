from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from typing import Any, get_type_hints


ParentNamespaceProvider = Callable[[], dict[str, Any]]


@dataclass(frozen=True)
class RuntimeToolFunctions:
  annotate_model_with_research: Callable[..., dict[str, Any]]
  model_clear_cache: Callable[..., dict[str, Any]]


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


def register_runtime_tools(
  mcp: Any,
  *,
  parent_namespace: ParentNamespaceProvider,
  tool_names: Sequence[str] | None = None,
  functions: RuntimeToolFunctions | None = None,
) -> RuntimeToolFunctions:
  functions = functions or build_runtime_tool_functions(
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


def build_runtime_tool_functions(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> RuntimeToolFunctions:
  def annotate_model_with_research(
    model_path: str,
    handoff_id: int,
    user_id: int,
  ) -> dict:
    """Annotate an existing workbook with assumptions and research context from a handoff.

    Discovery: model_path is the workbook path from model_build or the user's
    workspace, handoff_id comes from the research handoff system, and user_id must
    match the handoff owner.
    """
    try:
      result = parent_namespace()["_annotate_model_with_research"](
        model_path,
        handoff_id,
        user_id,
      )
      return {"status": "ok", **result}
    except Exception as exc:
      return {"status": "error", "error": str(exc)}

  def model_clear_cache(include_disk: bool = False) -> dict:
    """Clear the in-memory model cache. Pass include_disk=True to also wipe disk cache."""
    try:
      parent_namespace()["_clear_cache"](disk=include_disk)
      return {"status": "ok", "message": f"Model cache cleared (disk={include_disk})"}
    except Exception as exc:
      return {"status": "error", "error": str(exc)}

  return RuntimeToolFunctions(
    annotate_model_with_research=annotate_model_with_research,
    model_clear_cache=model_clear_cache,
  )


__all__ = [
  "RuntimeToolFunctions",
  "build_runtime_tool_functions",
  "register_runtime_tools",
]
