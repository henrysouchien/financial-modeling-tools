from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Optional
from fastmcp.server.context import Context
from pydantic import ValidationError




@dataclass(frozen=True)
class BusinessModelToolDeps:
  business_model_cls: Any
  validation_error_type: type[ValidationError]
  validate_schema: Any
  load_business_model_from_path: Callable[..., tuple[Any, str, Any, list[str]]]
  resolve_business_model_path: Callable[..., tuple[Any, list[str]]]
  coerce_json_dict_arg: Callable[..., dict[str, Any]]
  coerce_json_list_arg: Callable[..., list[Any]]
  make_edgar_financials_fetcher: Callable[[], Any]
  discover_all_axes: Callable[..., Any]
  safe_pydantic_errors: Callable[[ValidationError], list[dict[str, Any]]]
  model_tool_error_payload: Callable[[Exception], dict[str, Any]]


def business_model_validate_handler(
  *,
  deps: BusinessModelToolDeps,
  business_model: dict | None = None,
  business_model_path: str | None = None,
  most_recent_fy: int | None = None,
  n_historical: int = 5,
  segment_discovery: dict | None = None,
  assumption_driver_keys: list[str] | None = None,
  user_id: str | None = None,
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
          deps.load_business_model_from_path(str(business_model_path), user_id=user_id)
        )
      except FileNotFoundError:
        path, searched_paths = deps.resolve_business_model_path(
          str(business_model_path),
          user_id=user_id,
        )
        return deps.validate_schema.validation_error(
          "BusinessModel artifact not found",
          details={
            "business_model_path": business_model_path,
            "resolved_path": str(path),
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
    deps.validate_schema.discover_all_axes = deps.discover_all_axes
    result = deps.validate_schema.validate(
      parsed,
      segment_discovery=segment_discovery,
      most_recent_fy=most_recent_fy,
      n_historical=n_historical,
      assumption_driver_keys=assumption_driver_keys,
      fetcher=deps.make_edgar_financials_fetcher(),
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


ParentNamespaceProvider = Callable[[], dict[str, Any]]


@dataclass(frozen=True)
class BusinessModelToolFunctions:
  business_model_validate: Callable[..., dict[str, Any]]


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
    user_id: Optional[str] = None,
    ctx: Optional[Context] = None,
  ) -> dict:
    """Validate a BusinessModel JSON object or saved markdown/JSON artifact.

    Pass most_recent_fy or segment_discovery to validate EDGAR segment
    axis/member bindings. Pass assumption_driver_keys to validate handoff
    assumption keys against canonical and compiled BusinessModel driver keys.
    This tool performs no durable model-state mutation.
    """
    authenticated_user_id = parent_namespace()["_business_model_user"](
      ctx=ctx,
      supplied_user_id=user_id,
    )
    return parent_namespace()["_business_model_validate_handler"](
      deps=parent_namespace()["_business_model_tool_deps"](),
      business_model=business_model,
      business_model_path=business_model_path,
      most_recent_fy=most_recent_fy,
      n_historical=n_historical,
      segment_discovery=segment_discovery,
      assumption_driver_keys=assumption_driver_keys,
      user_id=authenticated_user_id,
    )

  return BusinessModelToolFunctions(
    business_model_validate=business_model_validate,
  )


def register_business_model_tools(
  mcp: Any,
  *,
  parent_namespace: ParentNamespaceProvider,
) -> BusinessModelToolFunctions:
  functions = build_business_model_tool_functions(
    parent_namespace=parent_namespace,
  )
  function = functions.business_model_validate
  parent_module = parent_namespace().get("__name__")
  if isinstance(parent_module, str):
    function.__module__ = parent_module
    function.__qualname__ = function.__name__
    function.__annotations__ = {**function.__annotations__, "return": dict}
  registered = mcp.tool(meta={
    "hank/code-disposition": "read_composable",
    "hank/code-reason": "Validates a BusinessModel object or artifact from a cell, including EDGAR segment bindings; a cold fetch that persists financials cache is a side effect of the read.",
  })(function)
  return BusinessModelToolFunctions(
    business_model_validate=registered or function,
  )


__all__ = [
  "BusinessModelToolDeps",
  "BusinessModelToolFunctions",
  "build_business_model_tool_functions",
  "business_model_validate_handler",
  "register_business_model_tools",
]
