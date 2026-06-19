from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
from typing import Any, get_type_hints

from mcp_servers.model_engine.modify import ModelModifyDeps, model_modify_handler


ParentNamespaceProvider = Callable[[], dict[str, Any]]


@dataclass(frozen=True)
class ModelModifyToolFunctions:
  model_modify: Callable[..., dict[str, Any]]


def _parent_model_modify_deps(
  parent_namespace: ParentNamespaceProvider,
) -> ModelModifyDeps:
  return parent_namespace()["_model_modify_deps"]()


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


def register_model_modify_tools(
  mcp: Any,
  *,
  parent_namespace: ParentNamespaceProvider,
  tool_names: Sequence[str] | None = None,
  functions: ModelModifyToolFunctions | None = None,
) -> ModelModifyToolFunctions:
  functions = functions or build_model_modify_tool_functions(
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


def build_model_modify_tool_functions(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> ModelModifyToolFunctions:
  def model_modify(
    file_path: str,
    operations: list[dict] | str,
    target: str = "file",
    conflict_strategy: str = "fail_on_collision",
    force_overwrite: bool = False,
    best_effort: bool = False,
    historical_cutoff_year: int | None = None,
  ) -> dict:
    """Apply persistent modifications to a built financial model.

    Discovery: file_path must be a model_build output whose model cache is warm in
    this session. Use model_find/model_values/model_drivers to identify item ids
    before constructing operations.

    Requires model_build to have run for this file_path in the current session
    (model cache must be warm). Cache miss returns structured error.

    Operations are applied in order on a deep-copy snapshot. All-or-nothing by
    default; best_effort=True continues past per-op errors (still deep-copies).

    target='file' writes atomically to file_path.
    target='workbook' dispatches the rendered plan to the live Excel add-in
                      via apply_render_plan (F2f path).
    target='both' does both.

    force_overwrite=True bypasses the sha256 stale-file check.

    Persistence caveat: edits to bm.* rows or custom_concept target_item_ids
    persist in the rendered workbook but are clobbered by the next model_build
    (which re-runs F2i populate or BM compile). To persist durably, use the
    model_override tool.

    Historical overrides on derivable items (total_opex, net_income,
    pretax_income, eps_diluted) may be removed by formula-first reconciliation
    at next model_build. Modify the upstream inputs for durable changes.
    """
    return _parent_handler(parent_namespace, "_model_modify_handler")(
      deps=_parent_model_modify_deps(parent_namespace),
      file_path=file_path,
      operations=operations,
      target=target,
      conflict_strategy=conflict_strategy,
      force_overwrite=force_overwrite,
      best_effort=best_effort,
      historical_cutoff_year=historical_cutoff_year,
    )

  return ModelModifyToolFunctions(model_modify=model_modify)


__all__ = [
  "ModelModifyDeps",
  "ModelModifyToolFunctions",
  "build_model_modify_tool_functions",
  "model_modify_handler",
  "register_model_modify_tools",
]
