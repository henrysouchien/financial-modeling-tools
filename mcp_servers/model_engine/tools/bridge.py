from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, replace
from typing import Any, get_type_hints

from mcp_servers.model_engine.tools import bridge_topology as _bridge_topology

_candidate_owner_scores = _bridge_topology._candidate_owner_scores
_jsonable = _bridge_topology._jsonable
_object_value = _bridge_topology._object_value
_parse_optional_factor_list = _bridge_topology._parse_optional_factor_list
_readiness_owners = _bridge_topology._readiness_owners
_resolve_topology_factor = _bridge_topology._resolve_topology_factor
_topology_next_actions = _bridge_topology._topology_next_actions
_topology_owner_complete = _bridge_topology._topology_owner_complete
_topology_owner_score = _bridge_topology._topology_owner_score
_unresolved_topology_factor_payload = _bridge_topology._unresolved_topology_factor_payload


@dataclass(frozen=True)
class ModelFindScenarioAnchorDeps:
  current_year: Callable[[], int]
  bridge_model_bundle: Callable[[str, int], Any]
  bridge_recovery: Callable[[str], dict[str, Any]]
  readiness_owner_anchor_payload: Callable[[Any, str], dict[str, Any] | None]
  find_scenario_anchor: Callable[..., Any]
  anchor_recovery: Callable[[dict[str, Any]], dict[str, Any] | None]
  model_tool_error_payload: Callable[[Exception], dict[str, Any]]
  validate_model_handle_token: Callable[..., Any] | None = None
  model_handle_token_cls: Any | None = None
  model_handle_token_error_type: type[Exception] | None = None


@dataclass(frozen=True)
class ModelScenarioTopologyDeps:
  current_year: Callable[[], int]
  bridge_model_bundle: Callable[[str, int], Any]
  bridge_recovery: Callable[[str], dict[str, Any]]
  coerce_json_list_arg: Callable[..., list[Any]]
  compute_scenario_bridge_readiness: Callable[..., Any]
  readiness_owner_anchor_payload: Callable[[Any, str], dict[str, Any] | None]
  find_scenario_anchor: Callable[..., Any]
  anchor_recovery: Callable[[dict[str, Any]], dict[str, Any] | None]
  model_tool_error_payload: Callable[[Exception], dict[str, Any]]
  validate_model_handle_token: Callable[..., Any] | None = None
  model_handle_token_cls: Any | None = None
  model_handle_token_error_type: type[Exception] | None = None


@dataclass(frozen=True)
class ModelBridgeScenariosDeps:
  current_year: Callable[[], int]
  coerce_json_list_arg: Callable[..., list[Any]]
  coerce_json_dict_arg: Callable[..., dict[str, Any]]
  bridge_base_result: Callable[..., dict[str, Any]]
  bridge_model_bundle: Callable[[str, int], Any]
  readiness_owner_anchor_hints: Callable[[Any, list[dict], dict | None], dict | None]
  build_bridge_operations: Callable[..., tuple[list[Any], list[dict], list[dict]]]
  bridge_inert_anchor_warnings: Callable[[Any, list[dict]], list[dict]]
  bridge_split_operations: Callable[[list[Any]], tuple[list[Any], list[Any], dict[str, Any]]]
  bridge_validate_scenario_propagation: Callable[..., dict[str, Any]]
  bridge_apply_ops_to_model_copy: Callable[..., tuple[Any, list[Any], bool]]
  bridge_operation_result_dicts: Callable[[list[Any]], list[dict[str, Any]]]
  compute_scenario_outputs: Callable[[Any], dict[str, Any]]
  build_snapshot_operations: Callable[..., tuple[list[Any], list[dict]]]
  bridge_projection_persistence: Callable[..., dict[str, Any]]
  apply_modify_request: Callable[..., Any]
  modify_error_type: type[Exception]
  load_model_bundle: Callable[..., Any]
  populate_scenario_eps: Callable[[Any, dict[str, Any]], None]
  bridge_workbook_outputs: Callable[..., dict[str, Any]]
  render_model: Callable[[Any], Any]
  write_xlsx: Callable[[Any, str], None]
  render_plan_to_addin_payload: Callable[..., dict[str, Any]]
  dispatch_to_addin: Callable[[str, dict[str, Any]], Any]
  addin_dispatch_error_status: Callable[[Exception], dict[str, Any]]
  bridge_unresolved_factors: Callable[[list[dict]], list[dict]]
  bridge_low_confidence_factors: Callable[[list[dict]], list[dict]]
  bridge_missing_snapshot_fields: Callable[[list[dict]], list[dict]]
  bridge_final_status: Callable[..., str]
  bridge_live_dispatch_only_reason: Callable[..., str | None]
  bridge_non_workbook_partial_reason: Callable[..., str | None]
  bridge_durable_file_mode: Callable[..., dict[str, Any]]
  validate_model_handle_token: Callable[..., Any] | None = None
  model_handle_token_cls: Any | None = None
  model_handle_token_error_type: type[Exception] | None = None
  model_handle_token_payload: Callable[..., dict[str, Any]] | None = None


def _bridge_token_error_payload(exc: Exception) -> dict[str, Any]:
  if hasattr(exc, "to_payload"):
    return exc.to_payload()
  return {
    "status": "error",
    "error": str(exc),
    "error_code": getattr(exc, "error_code", "invalid_model_handle_token"),
  }


def _bridge_bundle_from_token(
  *,
  deps: Any,
  file_path: str,
  model_handle_token: dict[str, Any],
) -> tuple[Any | None, str, int, dict[str, Any] | None]:
  if deps.validate_model_handle_token is None or deps.model_handle_token_cls is None:
    return (
      None,
      file_path,
      deps.current_year(),
      {
        "status": "error",
        "error": "model_handle_token validation is unavailable",
        "error_code": "model_handle_token_unavailable",
      },
    )
  try:
    handle = deps.validate_model_handle_token(model_handle_token, file_path=file_path)
    parsed_token = deps.model_handle_token_cls.model_validate(model_handle_token)
  except Exception as exc:
    if deps.model_handle_token_error_type is not None and isinstance(
      exc,
      deps.model_handle_token_error_type,
    ):
      return None, file_path, deps.current_year(), _bridge_token_error_payload(exc)
    return (
      None,
      file_path,
      deps.current_year(),
      {
        "status": "error",
        "error": str(exc),
        "error_code": "invalid_model_handle_token",
      },
    )
  return (
    handle.to_bundle(),
    parsed_token.workbook_path,
    parsed_token.historical_cutoff_year,
    None,
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
    bundle, file_path, cutoff, token_error = _bridge_bundle_from_token(
      deps=deps,
      file_path=file_path,
      model_handle_token=model_handle_token,
    )
    if token_error is not None:
      return token_error
  else:
    cutoff = deps.current_year()
    bundle = deps.bridge_model_bundle(file_path, cutoff)
  if bundle is None:
    return {
      "status": "failed",
      "reason": "model_not_in_cache",
      "error_code": "model_not_loaded",
      "factors": factors,
      "target_item_id": target_item_id,
      "recovery": deps.bridge_recovery("model_not_in_cache"),
    }
  try:
    parsed_factors = _parse_optional_factor_list(deps, factors)
    readiness = deps.compute_scenario_bridge_readiness(
      bundle.model,
      target_item_id=target_item_id,
    )
    payload = {
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
    return payload
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
    bundle, file_path, cutoff, token_error = _bridge_bundle_from_token(
      deps=deps,
      file_path=file_path,
      model_handle_token=model_handle_token,
    )
    if token_error is not None:
      return token_error
  else:
    cutoff = deps.current_year()
    bundle = deps.bridge_model_bundle(file_path, cutoff)
  if bundle is None:
    return {
      "status": "failed",
      "reason": "model_not_in_cache",
      "error_code": "model_not_loaded",
      "factor": factor,
      "hint": hint,
      "recovery": deps.bridge_recovery("model_not_in_cache"),
    }
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


def model_bridge_scenarios_handler(
  *,
  deps: ModelBridgeScenariosDeps,
  file_path: str,
  assumptions_by_factor: list[dict] | str,
  scenarios: dict | str,
  terminal_year: int,
  target: str = "both",
  conflict_strategy: str = "overwrite",
  best_effort: bool = False,
  factor_anchor_hints: dict[str, str] | str | None = None,
  ticker: str | None = None,
  skill_run_id: str | None = None,
  model_build_id: str | None = None,
  model_handle_token: dict[str, Any] | None = None,
) -> dict[str, Any]:
  if target not in {"file", "workbook", "both"}:
    return deps.bridge_base_result(status="failed", reason=f"unsupported_target:{target}")
  if conflict_strategy not in {"fail_on_collision", "overwrite"}:
    return deps.bridge_base_result(
      status="failed",
      reason=f"unsupported_conflict_strategy:{conflict_strategy}",
    )

  try:
    assumptions = deps.coerce_json_list_arg(
      assumptions_by_factor,
      name="assumptions_by_factor",
    )
    scenario_payload = deps.coerce_json_dict_arg(scenarios, name="scenarios")
    hints = (
      deps.coerce_json_dict_arg(factor_anchor_hints, name="factor_anchor_hints")
      if factor_anchor_hints is not None
      else None
    )
  except Exception as exc:
    return deps.bridge_base_result(status="failed", reason=f"invalid_input:{exc}")

  if not assumptions:
    return deps.bridge_base_result(status="skipped", reason="no_assumptions_by_factor")
  if any(scenario_payload.get(case) is None for case in ("bull", "base", "bear")):
    return deps.bridge_base_result(status="skipped", reason="missing_scenario_outputs")

  if model_handle_token is not None:
    bundle, file_path, cutoff, token_error = _bridge_bundle_from_token(
      deps=deps,
      file_path=file_path,
      model_handle_token=model_handle_token,
    )
    if token_error is not None:
      return token_error
  else:
    cutoff = deps.current_year()
    bundle = deps.bridge_model_bundle(file_path, cutoff)
  if bundle is None:
    return deps.bridge_base_result(status="failed", reason="model_not_in_cache")

  projection_periods = [int(period) for period in bundle.model.time_structure.projection_periods]
  hints = deps.readiness_owner_anchor_hints(bundle.model, assumptions, hints)
  try:
    ops, resolutions, warnings = deps.build_bridge_operations(
      bundle.model,
      assumptions,
      scenario_payload,
      int(terminal_year),
      projection_periods,
      factor_anchor_hints=hints,
      include_snapshot_ops=False,
    )
  except Exception as exc:
    return deps.bridge_base_result(status="failed", reason=f"build_operations_failed:{exc}")
  warnings.extend(deps.bridge_inert_anchor_warnings(bundle.model, resolutions))
  driver_ops, snapshot_ops, operation_groups = deps.bridge_split_operations(ops)

  bridge_validation = deps.bridge_validate_scenario_propagation(
    model=bundle.model,
    file_path=file_path,
    driver_ops=driver_ops,
    snapshot_ops=snapshot_ops,
    warnings=warnings,
    scenario_payload=scenario_payload,
    terminal_year=int(terminal_year),
    projection_periods=projection_periods,
    best_effort=best_effort,
  )
  if bridge_validation.get("status") != "ok":
    return deps.bridge_base_result(
      status="partial",
      reason="scenario_not_decision_usable",
      resolutions=resolutions,
      warnings=warnings,
      bridge_validation=bridge_validation,
      operation_groups=operation_groups,
    )

  try:
    snapshot_model, snapshot_dry_run_results, snapshot_rolled_back = (
      deps.bridge_apply_ops_to_model_copy(
        bundle.model,
        driver_ops,
        file_path=file_path,
        best_effort=best_effort,
      )
    )
    if snapshot_rolled_back:
      bridge_validation = {
        **bridge_validation,
        "status": "failed",
        "reason": "snapshot_dry_run_operation_failed",
        "snapshot_dry_run_results": deps.bridge_operation_result_dicts(
          snapshot_dry_run_results
        ),
      }
      return deps.bridge_base_result(
        status="partial",
        reason="scenario_not_decision_usable",
        resolutions=resolutions,
        warnings=warnings,
        bridge_validation=bridge_validation,
        operation_groups=operation_groups,
      )
    scenario_outputs_by_case = deps.compute_scenario_outputs(snapshot_model)
    computed_snapshot_ops, snapshot_warnings = deps.build_snapshot_operations(
      snapshot_model,
      scenario_outputs_by_case,
      int(terminal_year),
    )
  except Exception as exc:
    return deps.bridge_base_result(
      status="failed",
      reason=f"scenario_output_compute_failed:{exc}",
      resolutions=resolutions,
      warnings=warnings,
      bridge_validation=bridge_validation,
      operation_groups=operation_groups,
    )
  warnings.extend(snapshot_warnings)
  ops = [*driver_ops, *computed_snapshot_ops]
  driver_ops, snapshot_ops, operation_groups = deps.bridge_split_operations(ops)

  persistence = deps.bridge_projection_persistence(
    ticker=ticker or getattr(bundle.model.company, "ticker", ""),
    assumptions=assumptions,
    scenario_payload=scenario_payload,
    projection_periods=projection_periods,
    resolutions=resolutions,
    warnings=warnings,
    skill_run_id=skill_run_id,
    model_build_id=model_build_id,
    strict_through_period=int(terminal_year),
  )
  if persistence["failures"]:
    return deps.bridge_base_result(
      status="failed",
      reason="projection_persistence_failed",
      resolutions=resolutions,
      warnings=warnings,
      projection_persistence=persistence,
      bridge_validation=bridge_validation,
      operation_groups=operation_groups,
    )

  try:
    modify_result = deps.apply_modify_request(
      file_path=file_path,
      operations=ops,
      target="file",
      best_effort=best_effort,
      historical_cutoff_year=cutoff,
    )
  except deps.modify_error_type:
    return deps.bridge_base_result(
      status="failed",
      reason="model_not_in_cache",
      resolutions=resolutions,
      warnings=warnings,
      projection_persistence=persistence,
      bridge_validation=bridge_validation,
      operation_groups=operation_groups,
    )
  except Exception as exc:
    return deps.bridge_base_result(
      status="failed",
      reason=f"modify_failed:{exc}",
      resolutions=resolutions,
      warnings=warnings,
      projection_persistence=persistence,
      bridge_validation=bridge_validation,
      operation_groups=operation_groups,
    )

  if modify_result.rolled_back:
    return deps.bridge_base_result(
      status="failed",
      reason="modify_rolled_back",
      operations_applied=modify_result.operations_applied,
      operations_failed=modify_result.operations_failed,
      rolled_back=modify_result.rolled_back,
      output_path=modify_result.output_path,
      workbook_status=modify_result.workbook_status,
      results=[result.model_dump() for result in modify_result.results],
      stale_file_detected=modify_result.stale_file_detected,
      resolutions=resolutions,
      warnings=warnings,
      projection_persistence=persistence,
      bridge_validation=bridge_validation,
      operation_groups=operation_groups,
    )

  workbook_status = modify_result.workbook_status
  workbook_outputs = None
  try:
    post_bundle = deps.load_model_bundle(file_path, historical_cutoff_year=cutoff)
    mutated_model = post_bundle.model
    scenario_outputs_by_case = deps.compute_scenario_outputs(mutated_model)
    eps_by_case = {
      case: dict(fields.get("adj_eps", {}))
      for case, fields in scenario_outputs_by_case.items()
    }
    deps.populate_scenario_eps(mutated_model, eps_by_case)
    workbook_outputs = deps.bridge_workbook_outputs(
      scenario_outputs_by_case,
      terminal_year=int(terminal_year),
    )
    new_plan = deps.render_model(mutated_model)
    deps.write_xlsx(new_plan, file_path)
    if target in {"workbook", "both"}:
      payload = deps.render_plan_to_addin_payload(
        new_plan,
        conflict_strategy=conflict_strategy,
      )
      try:
        workbook_status = deps.dispatch_to_addin("apply_render_plan", payload)
      except Exception as exc:
        workbook_status = deps.addin_dispatch_error_status(exc)
    deps.load_model_bundle(
      file_path,
      model=mutated_model,
      historical_cutoff_year=cutoff,
      persist=True,
    )
  except Exception as exc:
    return deps.bridge_base_result(
      status="failed",
      reason=f"post_modify_failed:{exc}",
      operations_applied=modify_result.operations_applied,
      operations_failed=modify_result.operations_failed,
      rolled_back=modify_result.rolled_back,
      output_path=modify_result.output_path,
      workbook_status=workbook_status,
      results=[result.model_dump() for result in modify_result.results],
      stale_file_detected=modify_result.stale_file_detected,
      resolutions=resolutions,
      warnings=warnings,
      projection_persistence=persistence,
      bridge_validation=bridge_validation,
      operation_groups=operation_groups,
    )

  unresolved_factors = deps.bridge_unresolved_factors(resolutions)
  low_confidence_factors = deps.bridge_low_confidence_factors(resolutions)
  missing_snapshot_fields = deps.bridge_missing_snapshot_fields(warnings)
  status = deps.bridge_final_status(
    operations_failed=modify_result.operations_failed,
    unresolved_factors=unresolved_factors,
    low_confidence_factors=low_confidence_factors,
    missing_snapshot_fields=missing_snapshot_fields,
    warnings=warnings,
    workbook_status=workbook_status,
  )
  reason = deps.bridge_live_dispatch_only_reason(
    operations_failed=modify_result.operations_failed,
    unresolved_factors=unresolved_factors,
    low_confidence_factors=low_confidence_factors,
    missing_snapshot_fields=missing_snapshot_fields,
    warnings=warnings,
    workbook_status=workbook_status,
    workbook_outputs=workbook_outputs,
  )
  if reason is None and status == "partial":
    reason = deps.bridge_non_workbook_partial_reason(
      operations_failed=modify_result.operations_failed,
      unresolved_factors=unresolved_factors,
      low_confidence_factors=low_confidence_factors,
      missing_snapshot_fields=missing_snapshot_fields,
      warnings=warnings,
    )
  if workbook_outputs is None:
    status = "partial"
  response = deps.bridge_base_result(
    status=status,
    reason=reason,
    operations_applied=modify_result.operations_applied,
    operations_failed=modify_result.operations_failed,
    rolled_back=modify_result.rolled_back,
    output_path=modify_result.output_path,
    workbook_status=workbook_status,
    results=[result.model_dump() for result in modify_result.results],
    stale_file_detected=modify_result.stale_file_detected,
    resolutions=resolutions,
    warnings=warnings,
    projection_persistence=persistence,
    workbook_outputs=workbook_outputs,
    durable_file_mode=deps.bridge_durable_file_mode(
      reason=reason,
      output_path=modify_result.output_path,
      workbook_outputs=workbook_outputs,
    ),
    bridge_validation=bridge_validation,
    operation_groups=operation_groups,
  )
  if (
    modify_result.output_path
    and not modify_result.rolled_back
    and deps.model_handle_token_payload is not None
  ):
    try:
      response["model_handle_token"] = deps.model_handle_token_payload(
        file_path=modify_result.output_path,
        historical_cutoff_year=cutoff,
        issued_by="model_bridge_scenarios",
      )
    except Exception as exc:
      response["model_handle_token_error"] = {
        "error": str(exc),
        "error_code": getattr(exc, "error_code", "model_handle_token_unavailable"),
      }
  return response


ParentNamespaceProvider = Callable[[], dict[str, Any]]


@dataclass(frozen=True)
class BridgeToolFunctions:
  model_scenario_topology: Callable[..., dict[str, Any]]
  model_find_scenario_anchor: Callable[..., dict[str, Any]]
  model_bridge_scenarios: Callable[..., dict[str, Any]]


def _parent_model_find_scenario_anchor_deps(
  parent_namespace: ParentNamespaceProvider,
) -> ModelFindScenarioAnchorDeps:
  return parent_namespace()["_model_find_scenario_anchor_deps"]()


def _parent_model_scenario_topology_deps(
  parent_namespace: ParentNamespaceProvider,
) -> ModelScenarioTopologyDeps:
  return parent_namespace()["_model_scenario_topology_deps"]()


def _parent_model_bridge_scenarios_deps(
  parent_namespace: ParentNamespaceProvider,
) -> ModelBridgeScenariosDeps:
  return parent_namespace()["_model_bridge_scenarios_deps"]()


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


def register_bridge_tools(
  mcp: Any,
  *,
  parent_namespace: ParentNamespaceProvider,
  tool_names: Sequence[str] | None = None,
  functions: BridgeToolFunctions | None = None,
) -> BridgeToolFunctions:
  functions = functions or build_bridge_tool_functions(
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


def build_bridge_tool_functions(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> BridgeToolFunctions:
  def model_scenario_topology(
    file_path: str,
    factors: list[str] | str | None = None,
    target_item_id: str = "tpl.fm.adjusted_earnings.adjusted_eps",
    model_handle_token: dict[str, Any] | None = None,
  ) -> dict:
    """Return workbook scenario owner/anchor/case-row topology without writing.

    Discovery: file_path comes from model_build output or the current model.
    Use this before model_scenario, model_values, or model_bridge_scenarios when
    you need to understand bull/base/bear case rows. The returned topology is
    authoritative for scenario owners: owner_id is the durable factor anchor,
    anchor_id is the header row, and bull_id/base_id/bear_id are the case rows
    to inspect or override for one-off sensitivity.

    Pass factors as a list to also receive factor_matches that map thesis factor
    labels to owner_id hints. Do not use broad model_find searches to infer this
    topology.
    Pass returned model_handle_token from model_build/model_summarize unchanged
    when available; do not hand-write token fields.
    """
    return _parent_handler(parent_namespace, "_model_scenario_topology_handler")(
      deps=_parent_model_scenario_topology_deps(parent_namespace),
      file_path=file_path,
      factors=factors,
      target_item_id=target_item_id,
      model_handle_token=model_handle_token,
    )

  def model_find_scenario_anchor(
    file_path: str,
    factor: str,
    hint: str | None = None,
    model_handle_token: dict[str, Any] | None = None,
  ) -> dict:
    """Resolve one thesis factor to a workbook scenario anchor without writing.

    Discovery: file_path comes from model_build output and must still be present
    in the model cache. Use model_find for generic line-item lookup; use this
    tool when mapping a thesis factor such as pricing, volume, or margin to the
    workbook anchor expected by model_bridge_scenarios.
    Pass returned model_handle_token from model_build/model_summarize unchanged
    when available; do not hand-write token fields.
    """
    return _parent_handler(parent_namespace, "_model_find_scenario_anchor_handler")(
      deps=_parent_model_find_scenario_anchor_deps(parent_namespace),
      file_path=file_path,
      factor=factor,
      hint=hint,
      model_handle_token=model_handle_token,
    )

  def model_bridge_scenarios(
    file_path: str,
    assumptions_by_factor: list[dict] | str,
    scenarios: dict | str,
    terminal_year: int,
    target: str = "both",
    conflict_strategy: str = "overwrite",
    best_effort: bool = False,
    factor_anchor_hints: dict[str, str] | str | None = None,
    ticker: str | None = None,
    skill_run_id: str | None = None,
    model_build_id: str | None = None,
    model_handle_token: dict[str, Any] | None = None,
  ) -> dict:
    """Write thesis scenario assumptions and snapshot outputs into a workbook.

    Discovery: file_path comes from model_build output and should be the same
    workbook used to generate the scenario assumptions. Use
    model_find_scenario_anchor first when a factor needs an explicit anchor hint.

    Valid target values: both | file | workbook.
    Valid conflict_strategy values: overwrite | fail_on_collision. The default
    overwrite makes repeated target=both runs idempotent for the already-rendered
    bridge workbook sheets.
    Pass returned model_handle_token from model_build/model_summarize unchanged
    when available; do not hand-write token fields.
    """
    return _parent_handler(parent_namespace, "_model_bridge_scenarios_handler")(
      deps=_parent_model_bridge_scenarios_deps(parent_namespace),
      file_path=file_path,
      assumptions_by_factor=assumptions_by_factor,
      scenarios=scenarios,
      terminal_year=terminal_year,
      target=target,
      conflict_strategy=conflict_strategy,
      best_effort=best_effort,
      factor_anchor_hints=factor_anchor_hints,
      ticker=ticker,
      skill_run_id=skill_run_id,
      model_build_id=model_build_id,
      model_handle_token=model_handle_token,
    )

  return BridgeToolFunctions(
    model_scenario_topology=model_scenario_topology,
    model_find_scenario_anchor=model_find_scenario_anchor,
    model_bridge_scenarios=model_bridge_scenarios,
  )


__all__ = [
  "BridgeToolFunctions",
  "ModelBridgeScenariosDeps",
  "ModelFindScenarioAnchorDeps",
  "ModelScenarioTopologyDeps",
  "build_bridge_tool_functions",
  "model_bridge_scenarios_handler",
  "model_find_scenario_anchor_handler",
  "model_scenario_topology_handler",
  "register_bridge_tools",
]
