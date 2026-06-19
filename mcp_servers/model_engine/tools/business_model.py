from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass, replace
import json
import os
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class BusinessModelToolDeps:
  business_model_cls: Any
  validation_error_type: type[Exception]
  validate_schema: Any
  load_business_model_from_path: Callable[[str], tuple[Any, str, Path, list[str]]]
  resolve_business_model_path: Callable[[str], tuple[Path, list[str]]]
  coerce_json_dict_arg: Callable[..., dict[str, Any]]
  coerce_json_list_arg: Callable[..., list[Any]]
  make_edgar_financials_fetcher: Callable[[], Any]
  make_edgar_financials_fetcher_override: Callable[[], Any]
  discover_all_axes: Callable[..., Any]
  safe_pydantic_errors: Callable[[Exception], list[dict[str, Any]]]
  model_tool_error_payload: Callable[[Exception], dict[str, Any]]
  bridge_report_cls: Any
  business_model_to_overrides: Callable[..., Any]
  serialize_overrides_write_report: Callable[[Any], dict[str, Any]]
  bridge_kpi_catalog: Callable[..., tuple[dict[str, Any], Any]]


def business_model_validate_handler(
  *,
  deps: BusinessModelToolDeps,
  business_model: dict | None = None,
  business_model_path: str | None = None,
  most_recent_fy: int | None = None,
  n_historical: int = 5,
  segment_discovery: dict | None = None,
  assumption_driver_keys: list[str] | None = None,
) -> dict[str, Any]:
  try:
    if business_model is not None and business_model_path is not None:
      return deps.validate_schema.validation_error(
        "business_model and business_model_path are mutually exclusive"
      )
    if business_model is None and business_model_path is None:
      return deps.validate_schema.validation_error(
        "one of business_model or business_model_path is required"
      )

    if business_model is not None:
      parsed = deps.business_model_cls.model_validate(business_model)
      input_format = "object"
    else:
      try:
        parsed, input_format, _path, _searched_paths = (
          deps.load_business_model_from_path(str(business_model_path))
        )
      except FileNotFoundError:
        _path, searched_paths = deps.resolve_business_model_path(
          str(business_model_path)
        )
        return deps.validate_schema.validation_error(
          "BusinessModel artifact not found",
          details={
            "business_model_path": business_model_path,
            "resolved_path": str(_path),
            "searched_paths": searched_paths,
          },
        )

    if segment_discovery is not None:
      segment_discovery = deps.coerce_json_dict_arg(
        segment_discovery,
        name="segment_discovery",
      )

    if assumption_driver_keys is not None:
      assumption_driver_keys = [
        str(key)
        for key in deps.coerce_json_list_arg(
          assumption_driver_keys,
          name="assumption_driver_keys",
        )
      ]
    fetcher = deps.make_edgar_financials_fetcher()
    if deps.make_edgar_financials_fetcher_override is not deps.make_edgar_financials_fetcher:
      fetcher = deps.make_edgar_financials_fetcher_override()
    deps.validate_schema.discover_all_axes = deps.discover_all_axes
    result = deps.validate_schema.validate(
      parsed,
      segment_discovery=segment_discovery,
      most_recent_fy=most_recent_fy,
      n_historical=n_historical,
      assumption_driver_keys=assumption_driver_keys,
      fetcher=fetcher,
    )
    if result.get("status") == "ok":
      result["input_format"] = input_format
    return result
  except deps.validation_error_type as exc:
    return deps.validate_schema.validation_error(
      "BusinessModel validation failed",
      details={"pydantic_errors": deps.safe_pydantic_errors(exc)},
    )
  except Exception as exc:
    payload = deps.model_tool_error_payload(exc)
    payload.setdefault("status", "error")
    payload.setdefault("error_code", "business_model_validation_error")
    return payload


def business_model_to_overrides_handler(
  *,
  deps: BusinessModelToolDeps,
  ticker: str,
  business_model: dict | None = None,
  business_model_path: str | None = None,
  bridge_report_path: str | None = None,
  dry_run: bool = False,
  prune: bool = False,
) -> dict[str, Any]:
  if business_model is not None and business_model_path is not None:
    raise ValueError("business_model and business_model_path are mutually exclusive")
  if business_model is None and business_model_path is None:
    raise ValueError("one of business_model or business_model_path is required")

  if business_model is not None:
    parsed_business_model = deps.business_model_cls.model_validate(business_model)
  else:
    parsed_business_model, _input_format, _path, _searched_paths = (
      deps.load_business_model_from_path(str(business_model_path))
    )

  bridge_report = None
  if bridge_report_path is not None:
    bridge_report = deps.bridge_report_cls.model_validate_json(
      Path(str(bridge_report_path)).read_text(encoding="utf-8")
    )

  report = deps.business_model_to_overrides(
    parsed_business_model,
    ticker,
    dry_run=dry_run,
    prune=prune,
    bridge_report=bridge_report,
  )
  return deps.serialize_overrides_write_report(report)


def bridge_kpis_to_business_model_draft_handler(
  *,
  deps: BusinessModelToolDeps,
  ticker: str,
  catalog_path: str,
  output_dir: str | None = None,
  primary_axis_qname: str | None = None,
  primary_axis_family: str | None = None,
) -> dict[str, Any]:
  try:
    user = os.getenv("USER", "default")
    resolved_output_dir = output_dir or f"./data/users/{user}/workspace/notes/business_models"
    output_path = Path(os.path.expanduser(resolved_output_dir))
    output_path.mkdir(parents=True, exist_ok=True)

    draft, report = deps.bridge_kpi_catalog(
      catalog_path,
      ticker,
      primary_axis_qname=primary_axis_qname,
      primary_axis_family=primary_axis_family,
    )
    ticker_normalized = str(ticker or "").upper()
    draft_path = output_path / f"{ticker_normalized}_business_model_draft.json"
    report_path = output_path / f"{ticker_normalized}_bridge_report.json"

    draft_path.write_text(
      json.dumps(draft, indent=2, sort_keys=True) + "\n",
      encoding="utf-8",
    )
    report_path.write_text(
      json.dumps(report.model_dump(mode="json"), indent=2, sort_keys=True) + "\n",
      encoding="utf-8",
    )

    return {
      "status": "ok",
      "ticker": ticker_normalized,
      "draft_path": str(draft_path),
      "report_path": str(report_path),
      "summary": report.summary.model_dump(mode="json"),
      "readiness_status": report.readiness_status,
      "primary_axis": report.primary_axis,
      "primary_axis_qname": report.primary_axis_qname,
      "axis_inventory": [
        entry.model_dump(mode="json") for entry in report.axis_inventory
      ],
    }
  except Exception as exc:
    return {"status": "error", "error": str(exc)}


ParentNamespaceProvider = Callable[[], dict[str, Any]]


@dataclass(frozen=True)
class BusinessModelToolFunctions:
  business_model_validate: Callable[..., dict[str, Any]]
  business_model_to_overrides: Callable[..., dict[str, Any]]
  bridge_kpis_to_business_model_draft: Callable[..., dict[str, Any]]


def _parent_business_model_deps(
  parent_namespace: ParentNamespaceProvider,
) -> BusinessModelToolDeps:
  return parent_namespace()["_business_model_tool_deps"]()


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
    function.__annotations__ = {**function.__annotations__, "return": dict}
  return function


def _register_tool(
  mcp: Any,
  function: Callable[..., dict[str, Any]],
) -> Callable[..., dict[str, Any]]:
  registered = mcp.tool()(function)
  return registered or function


def register_business_model_tools(
  mcp: Any,
  *,
  parent_namespace: ParentNamespaceProvider,
  tool_names: Sequence[str] | None = None,
  functions: BusinessModelToolFunctions | None = None,
) -> BusinessModelToolFunctions:
  functions = functions or build_business_model_tool_functions(
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


def build_business_model_tool_functions(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> BusinessModelToolFunctions:
  def business_model_validate(
    business_model: dict | None = None,
    business_model_path: str | None = None,
    most_recent_fy: int | None = None,
    n_historical: int = 5,
    segment_discovery: dict | None = None,
    assumption_driver_keys: list[str] | None = None,
  ) -> dict:
    """Validate a BusinessModel JSON object or markdown/JSON artifact path.

    Discovery: pass an inline business_model from a producer judgment, or use
    business_model_path for a saved sidecar/artifact. Run this before
    business_model_to_overrides when turning an approved BusinessModel into model
    override entries.

    Pass most_recent_fy or segment_discovery to validate EDGAR segment
    axis/member bindings. Pass assumption_driver_keys to validate handoff
    assumption keys against canonical and compiled BusinessModel driver keys.
    """
    return _parent_handler(parent_namespace, "_business_model_validate_handler")(
      deps=_parent_business_model_deps(parent_namespace),
      business_model=business_model,
      business_model_path=business_model_path,
      most_recent_fy=most_recent_fy,
      n_historical=n_historical,
      segment_discovery=segment_discovery,
      assumption_driver_keys=assumption_driver_keys,
    )

  def business_model_to_overrides(
    ticker: str,
    business_model: dict | None = None,
    business_model_path: str | None = None,
    bridge_report_path: str | None = None,
    dry_run: bool = False,
    prune: bool = False,
  ) -> dict:
    """Convert a BusinessModel into per-ticker override JSON entries.

    Discovery: pass the same inline business_model or business_model_path shape
    accepted by business_model_validate. Validate first with
    business_model_validate when the BusinessModel came from an agent judgment or
    untrusted artifact; use dry_run=True to preview generated override entries
    before writing them.
    """
    return _parent_handler(parent_namespace, "_business_model_to_overrides_handler")(
      deps=_parent_business_model_deps(parent_namespace),
      ticker=ticker,
      business_model=business_model,
      business_model_path=business_model_path,
      bridge_report_path=bridge_report_path,
      dry_run=dry_run,
      prune=prune,
    )

  def bridge_kpis_to_business_model_draft(
    ticker: str,
    catalog_path: str,
    output_dir: str | None = None,
    primary_axis_qname: str | None = None,
    primary_axis_family: str | None = None,
  ) -> dict:
    """Bridge an Edgar KPI catalog JSONL into BusinessModel draft + report files.

    Discovery: catalog_path is produced by the Edgar KPI extraction/catalog flow.
    Use primary_axis_qname to select a concrete company/XBRL axis, or
    primary_axis_family for family-only catalogs without axis QNames.
    Use the returned draft_path and report_path as inputs to later model_build or
    business-model override review.
    """
    return _parent_handler(
      parent_namespace,
      "_bridge_kpis_to_business_model_draft_handler",
    )(
      deps=_parent_business_model_deps(parent_namespace),
      ticker=ticker,
      catalog_path=catalog_path,
      output_dir=output_dir,
      primary_axis_qname=primary_axis_qname,
      primary_axis_family=primary_axis_family,
    )

  return BusinessModelToolFunctions(
    business_model_validate=business_model_validate,
    business_model_to_overrides=business_model_to_overrides,
    bridge_kpis_to_business_model_draft=bridge_kpis_to_business_model_draft,
  )


__all__ = [
  "BusinessModelToolFunctions",
  "BusinessModelToolDeps",
  "build_business_model_tool_functions",
  "bridge_kpis_to_business_model_draft_handler",
  "business_model_to_overrides_handler",
  "business_model_validate_handler",
  "register_business_model_tools",
]
