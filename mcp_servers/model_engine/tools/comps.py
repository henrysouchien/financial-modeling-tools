from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
import os
from typing import Any, Optional, get_type_hints


@dataclass(frozen=True)
class CompsBuildDeps:
  new_comps_build_id: Callable[[], str]
  coerce_json_dict_arg: Callable[..., dict[str, Any]]
  normalize_comps_payload: Callable[..., Any]
  render_comps_rows: Callable[[Any], list[Any]]
  comps_grid_payload: Callable[[Any], dict[str, Any]]
  render_comps_plan: Callable[..., Any]
  write_xlsx: Callable[[Any, str], None]
  render_plan_to_addin_payload: Callable[..., dict[str, Any]]
  dispatch_to_addin: Callable[[str, dict[str, Any]], Any]
  addin_dispatch_error_status: Callable[[Exception], dict[str, Any]]
  validation_error_type: type[Exception]
  comps_build_error_payload: Callable[..., dict[str, Any]]


def comps_build_handler(
  *,
  deps: CompsBuildDeps,
  comps_payload: dict,
  focal_ticker: str | None = None,
  kpi_values: dict | None = None,
  target: str = "file",
  output_path: str | None = None,
  sheet_name: str | None = None,
  conflict_strategy: str = "fail_on_collision",
) -> dict[str, Any]:
  comps_build_id = deps.new_comps_build_id()
  try:
    if target not in {"file", "workbook", "grid"}:
      return {
        "status": "error",
        "error": f"Unsupported target: {target!r}",
        "comps_build_id": comps_build_id,
        "recovery": {"next_actions": ["Use target='file', 'workbook', or 'grid'."]},
      }
    if conflict_strategy not in ("fail_on_collision", "overwrite"):
      return {
        "status": "error",
        "error": (
          f"Invalid conflict_strategy {conflict_strategy!r}; "
          "supported: 'fail_on_collision' (default) or 'overwrite'."
        ),
        "comps_build_id": comps_build_id,
        "recovery": {
          "next_actions": [
            "Retry comps_build with conflict_strategy='fail_on_collision' or 'overwrite'."
          ]
        },
      }

    resolved_output_path = os.path.expanduser(output_path) if output_path is not None else None
    if target == "file" and resolved_output_path is None:
      return {
        "status": "error",
        "error": "output_path is required when target='file'",
        "comps_build_id": comps_build_id,
        "recovery": {"next_actions": ["Pass output_path for target='file'."]},
      }

    parsed_payload = deps.coerce_json_dict_arg(comps_payload, name="comps_payload")
    parsed_kpis = (
      deps.coerce_json_dict_arg(kpi_values, name="kpi_values")
      if kpi_values is not None
      else None
    )
    grid = deps.normalize_comps_payload(
      parsed_payload,
      focal_ticker=focal_ticker,
      kpi_values=parsed_kpis,
    )
    rows = deps.render_comps_rows(grid)
    grid_payload = deps.comps_grid_payload(grid)
    sheet = sheet_name or "Comps"

    workbook_result = None
    workbook_dispatch_failed = False
    if target == "file":
      plan = deps.render_comps_plan(grid, sheet_name=sheet)
      deps.write_xlsx(plan, resolved_output_path)
    elif target == "workbook":
      plan = deps.render_comps_plan(grid, sheet_name=sheet)
      payload = deps.render_plan_to_addin_payload(
        plan,
        conflict_strategy=conflict_strategy,
      )
      try:
        workbook_result = deps.dispatch_to_addin("apply_render_plan", payload)
      except Exception as exc:
        workbook_dispatch_failed = True
        workbook_result = deps.addin_dispatch_error_status(exc)

    response: dict[str, Any] = {
      "status": "partial" if workbook_dispatch_failed else "ok",
      "comps_build_id": comps_build_id,
      "template_id": grid.template_id,
      "tickers": list(grid.tickers),
      "focal_ticker": grid.focal_ticker,
      "years": list(grid.years) if grid.years is not None else None,
      "output_path": resolved_output_path if target == "file" else None,
      "workbook_result": workbook_result,
      "grid": grid_payload,
      "warnings": list(grid.warnings),
    }
    if target == "grid":
      response["render_rows"] = rows
    if workbook_dispatch_failed:
      response["reason"] = "live_workbook_unavailable"
      response["addin_dispatch_error_status"] = workbook_result
    return response
  except deps.validation_error_type as exc:
    return deps.comps_build_error_payload(exc, comps_build_id=comps_build_id)
  except Exception as exc:
    return deps.comps_build_error_payload(exc, comps_build_id=comps_build_id)


ParentNamespaceProvider = Callable[[], dict[str, Any]]


@dataclass(frozen=True)
class CompsBuildToolFunctions:
  comps_build: Callable[..., dict[str, Any]]


def _parent_comps_build_deps(
  parent_namespace: ParentNamespaceProvider,
) -> CompsBuildDeps:
  return parent_namespace()["_comps_build_deps"]()


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


def register_comps_build_tools(
  mcp: Any,
  *,
  parent_namespace: ParentNamespaceProvider,
  tool_names: Sequence[str] | None = None,
  functions: CompsBuildToolFunctions | None = None,
) -> CompsBuildToolFunctions:
  functions = functions or build_comps_build_tool_functions(
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


def build_comps_build_tool_functions(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> CompsBuildToolFunctions:
  def comps_build(
    comps_payload: dict,
    *,
    focal_ticker: Optional[str] = None,
    kpi_values: Optional[dict] = None,
    target: str = "file",
    output_path: Optional[str] = None,
    sheet_name: Optional[str] = None,
    conflict_strategy: str = "fail_on_collision",
  ) -> dict:
    """Render an industry_peer_comparison payload into a comp-sheet artifact.

    This is a pure renderer over the v1.2 industry_peer_comparison payload. It
    performs no market-data fetching and rejects legacy/gate-off payloads that
    omit the sections structure.
    """
    return _parent_handler(parent_namespace, "_comps_build_handler")(
      deps=_parent_comps_build_deps(parent_namespace),
      comps_payload=comps_payload,
      focal_ticker=focal_ticker,
      kpi_values=kpi_values,
      target=target,
      output_path=output_path,
      sheet_name=sheet_name,
      conflict_strategy=conflict_strategy,
    )

  return CompsBuildToolFunctions(comps_build=comps_build)


__all__ = [
  "CompsBuildDeps",
  "CompsBuildToolFunctions",
  "build_comps_build_tool_functions",
  "comps_build_handler",
  "register_comps_build_tools",
]
