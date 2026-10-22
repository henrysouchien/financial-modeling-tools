from __future__ import annotations

from collections.abc import Callable
from dataclasses import asdict, dataclass, replace
from typing import Any, Protocol, get_type_hints

from fastmcp.server.context import Context

from mcp_servers.model_engine.tools import bridge_topology as _bridge_topology
from mcp_servers.model_engine.tools.read import ModelReadSource
from research.current_model_version_resolver import CurrentModelResolutionError
from schema.load_core import _ModelBundle


_jsonable = _bridge_topology._jsonable
_parse_optional_factor_list = _bridge_topology._parse_optional_factor_list
_resolve_topology_factor = _bridge_topology._resolve_topology_factor
_topology_next_actions = _bridge_topology._topology_next_actions


class _BridgeTokenError(Protocol):
  def to_payload(self) -> dict[str, Any]: ...


@dataclass(frozen=True)
class _BridgeBundleSuccess:
  bundle: _ModelBundle
  file_path: str
  cutoff: int


@dataclass(frozen=True)
class _BridgeBundleError:
  payload: dict[str, Any]


_BridgeBundleResult = _BridgeBundleSuccess | _BridgeBundleError


@dataclass(frozen=True)
class ModelFindScenarioAnchorDeps:
  current_year: Callable[[], int]
  bridge_model_bundle: Callable[[str, int], _ModelBundle]
  readiness_owner_anchor_payload: Callable[[Any, str], dict[str, Any] | None]
  find_scenario_anchor: Callable[..., Any]
  anchor_recovery: Callable[[dict[str, Any]], dict[str, Any] | None]
  model_tool_error_payload: Callable[[Exception], dict[str, Any]]
  validate_model_handle_token: Callable[..., Any] | None = None
  model_handle_token_cls: Any | None = None
  model_handle_token_error_type: type[_BridgeTokenError] | None = None


@dataclass(frozen=True)
class ModelScenarioTopologyDeps:
  current_year: Callable[[], int]
  bridge_model_bundle: Callable[[str, int], _ModelBundle]
  coerce_json_list_arg: Callable[..., list[Any]]
  compute_scenario_bridge_readiness: Callable[..., Any]
  readiness_owner_anchor_payload: Callable[[Any, str], dict[str, Any] | None]
  find_scenario_anchor: Callable[..., Any]
  anchor_recovery: Callable[[dict[str, Any]], dict[str, Any] | None]
  model_tool_error_payload: Callable[[Exception], dict[str, Any]]
  validate_model_handle_token: Callable[..., Any] | None = None
  model_handle_token_cls: Any | None = None
  model_handle_token_error_type: type[_BridgeTokenError] | None = None


def _bridge_token_error_payload(exc: _BridgeTokenError) -> dict[str, Any]:
  if hasattr(exc, "to_payload"):
    return exc.to_payload()
  return {
    "status": "error",
    "error": str(exc),
    "error_code": getattr(exc, "error_code", "invalid_model_handle_token"),
  }


def _hosted_bridge_error(exc: Exception) -> dict[str, Any]:
  if isinstance(exc, CurrentModelResolutionError):
    return {
      "status": "error",
      "error": str(exc),
      "error_code": exc.code,
      "retryable": exc.retryable,
      **({"fix": exc.fix} if exc.fix else {}),
    }
  return {
    "status": "error",
    "error": "The authenticated current model could not be read.",
    "error_code": "current_model_read_failed",
  }


def _bridge_bundle_from_token(
  *,
  deps: ModelFindScenarioAnchorDeps | ModelScenarioTopologyDeps,
  file_path: str,
  model_handle_token: dict[str, Any],
) -> _BridgeBundleResult:
  if deps.validate_model_handle_token is None or deps.model_handle_token_cls is None:
    return _BridgeBundleError(
      {
        "status": "error",
        "error": "model_handle_token validation is unavailable",
        "error_code": "model_handle_token_unavailable",
      }
    )
  try:
    handle = deps.validate_model_handle_token(model_handle_token, file_path=file_path)
    parsed_token = deps.model_handle_token_cls.model_validate(model_handle_token)
  except Exception as exc:
    if deps.model_handle_token_error_type is not None and isinstance(
      exc,
      deps.model_handle_token_error_type,
    ):
      return _BridgeBundleError(_bridge_token_error_payload(exc))
    return _BridgeBundleError(
      {
        "status": "error",
        "error": str(exc),
        "error_code": "invalid_model_handle_token",
      }
    )
  return _BridgeBundleSuccess(
    bundle=handle.to_bundle(),
    file_path=parsed_token.workbook_path,
    cutoff=parsed_token.historical_cutoff_year,
  )


def model_scenario_topology_handler(
  *,
  deps: ModelScenarioTopologyDeps,
  file_path: str,
  factors: list[str] | str | None = None,
  target_item_id: str = "tpl.fm.adjusted_earnings.adjusted_eps",
  model_handle_token: dict[str, Any] | None = None,
) -> dict[str, Any]:
  if model_handle_token is not None:
    bundle_result = _bridge_bundle_from_token(
      deps=deps,
      file_path=file_path,
      model_handle_token=model_handle_token,
    )
    if isinstance(bundle_result, _BridgeBundleError):
      return bundle_result.payload
    bundle = bundle_result.bundle
  else:
    bundle = deps.bridge_model_bundle(file_path, deps.current_year())
  try:
    parsed_factors = _parse_optional_factor_list(deps, factors)
    readiness = deps.compute_scenario_bridge_readiness(
      bundle.model,
      target_item_id=target_item_id,
    )
    return {
      "status": "ok",
      "topology": _jsonable(readiness),
      "factor_matches": [
        _resolve_topology_factor(
          deps=deps,
          model=bundle.model,
          factor=factor,
          readiness=readiness,
        )
        for factor in parsed_factors
      ],
      "next_actions": _topology_next_actions(),
    }
  except Exception as exc:
    return deps.model_tool_error_payload(exc)


def model_find_scenario_anchor_handler(
  *,
  deps: ModelFindScenarioAnchorDeps,
  file_path: str,
  factor: str,
  hint: str | None = None,
  model_handle_token: dict[str, Any] | None = None,
) -> dict[str, Any]:
  if model_handle_token is not None:
    bundle_result = _bridge_bundle_from_token(
      deps=deps,
      file_path=file_path,
      model_handle_token=model_handle_token,
    )
    if isinstance(bundle_result, _BridgeBundleError):
      return bundle_result.payload
    bundle = bundle_result.bundle
  else:
    bundle = deps.bridge_model_bundle(file_path, deps.current_year())
  try:
    if not str(hint or "").strip():
      readiness_payload = deps.readiness_owner_anchor_payload(bundle.model, factor)
      if readiness_payload is not None:
        return readiness_payload
    resolution = deps.find_scenario_anchor(bundle.model, factor, hint=hint)
    payload = {"status": "ok", **asdict(resolution)}
    recovery = deps.anchor_recovery(payload)
    if recovery is not None:
      payload["recovery"] = recovery
    return payload
  except Exception as exc:
    return deps.model_tool_error_payload(exc)


ParentNamespaceProvider = Callable[[], dict[str, Any]]


@dataclass(frozen=True)
class BridgeToolFunctions:
  model_scenario_topology: Callable[..., dict[str, Any]]
  model_find_scenario_anchor: Callable[..., dict[str, Any]]


def _parent_model_find_scenario_anchor_deps(
  parent_namespace: ParentNamespaceProvider,
) -> ModelFindScenarioAnchorDeps:
  return parent_namespace()["_model_find_scenario_anchor_deps"]()


def _parent_model_scenario_topology_deps(
  parent_namespace: ParentNamespaceProvider,
) -> ModelScenarioTopologyDeps:
  return parent_namespace()["_model_scenario_topology_deps"]()


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
  registered = mcp.tool(meta={
    "hank/code-disposition": "read_composable",
    "hank/code-reason": "Reads an explicit trusted-local workbook or authenticated current model to resolve scenario topology and factor anchors without writing model or thesis state.",
  })(function)
  return registered or function


def build_bridge_tool_functions(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> BridgeToolFunctions:
  def model_scenario_topology(
    file_path: str | None = None,
    factors: list[str] | str | None = None,
    target_item_id: str = "tpl.fm.adjusted_earnings.adjusted_eps",
    model_handle_token: dict[str, Any] | None = None,
    current_model: dict[str, Any] | None = None,
    *,
    ctx: Context | None = None,
  ) -> dict:
    """Return workbook scenario owner, anchor, and case-row topology.

    Hosted: copy only research_file_id, ticker, model_id, version, pointer_epoch,
    workbook_xlsx_sha256, and model_spec_sha256 from get_current_model into
    current_model, and omit file_path and
    model_handle_token. Trusted-local operator calls may instead pass an explicit
    file_path and reuse a read-tool token. Returned owner IDs are durable anchors.
    """
    deps = _parent_model_scenario_topology_deps(parent_namespace)
    if current_model is not None:
      deps = replace(deps, model_tool_error_payload=_hosted_bridge_error)
    try:
      with parent_namespace()["_resolved_model_read_source"](
        file_path=file_path,
        current_model=current_model,
        ctx=ctx,
      ) as source:
        if not isinstance(source, ModelReadSource):
          raise TypeError("model read source must be exact ModelReadSource")
        if source.model is not None:
          deps = replace(
            deps,
            bridge_model_bundle=lambda file_path, cutoff: parent_namespace()[
              "_bridge_model_bundle_from_model"
            ](file_path, cutoff, source.model),
          )
        return _parent_handler(
          parent_namespace,
          "_model_scenario_topology_handler",
        )(
          deps=deps,
          file_path=source.file_path,
          factors=factors,
          target_item_id=target_item_id,
          model_handle_token=(None if current_model is not None else model_handle_token),
        )
    except Exception as exc:
      return (
        _hosted_bridge_error(exc)
        if current_model is not None
        else deps.model_tool_error_payload(exc)
      )

  def model_find_scenario_anchor(
    file_path: str | None = None,
    factor: str = "",
    hint: str | None = None,
    model_handle_token: dict[str, Any] | None = None,
    current_model: dict[str, Any] | None = None,
    *,
    ctx: Context | None = None,
  ) -> dict:
    """Resolve one thesis factor to a workbook scenario anchor without writing.

    Hosted: copy only research_file_id, ticker, model_id, version, pointer_epoch,
    workbook_xlsx_sha256, and model_spec_sha256 from get_current_model into
    current_model, and omit file_path and
    model_handle_token. Trusted-local operator calls may instead pass an explicit
    file_path and reuse a read-tool token. Use model_find for generic lookup.
    """
    deps = _parent_model_find_scenario_anchor_deps(parent_namespace)
    if current_model is not None:
      deps = replace(deps, model_tool_error_payload=_hosted_bridge_error)
    try:
      with parent_namespace()["_resolved_model_read_source"](
        file_path=file_path,
        current_model=current_model,
        ctx=ctx,
      ) as source:
        if not isinstance(source, ModelReadSource):
          raise TypeError("model read source must be exact ModelReadSource")
        if source.model is not None:
          deps = replace(
            deps,
            bridge_model_bundle=lambda file_path, cutoff: parent_namespace()[
              "_bridge_model_bundle_from_model"
            ](file_path, cutoff, source.model),
          )
        return _parent_handler(
          parent_namespace,
          "_model_find_scenario_anchor_handler",
        )(
          deps=deps,
          file_path=source.file_path,
          factor=factor,
          hint=hint,
          model_handle_token=(None if current_model is not None else model_handle_token),
        )
    except Exception as exc:
      return (
        _hosted_bridge_error(exc)
        if current_model is not None
        else deps.model_tool_error_payload(exc)
      )

  return BridgeToolFunctions(
    model_scenario_topology=model_scenario_topology,
    model_find_scenario_anchor=model_find_scenario_anchor,
  )


def register_bridge_tools(
  mcp: Any,
  *,
  parent_namespace: ParentNamespaceProvider,
  functions: BridgeToolFunctions | None = None,
) -> BridgeToolFunctions:
  functions = functions or build_bridge_tool_functions(
    parent_namespace=parent_namespace,
  )
  return BridgeToolFunctions(
    model_scenario_topology=_register_tool(
      mcp,
      _bind_parent_module(functions.model_scenario_topology, parent_namespace),
    ),
    model_find_scenario_anchor=_register_tool(
      mcp,
      _bind_parent_module(functions.model_find_scenario_anchor, parent_namespace),
    ),
  )


__all__ = [
  "BridgeToolFunctions",
  "ModelFindScenarioAnchorDeps",
  "ModelScenarioTopologyDeps",
  "build_bridge_tool_functions",
  "model_find_scenario_anchor_handler",
  "model_scenario_topology_handler",
  "register_bridge_tools",
]
