from __future__ import annotations

import copy
from dataclasses import asdict
from typing import Any

from schema.build import compute_scenario_eps
from schema.scenario_bridge import BridgeWarning

from .recovery import bridge_period_coverage_recovery, bridge_recovery


BRIDGE_PARTIAL_WARNING_KINDS = {
  "missing_factor_curve",
  "non_numeric_factor_value",
  "non_numeric_snapshot_field",
  "period_coverage_gap",
  "invalid_hint",
  "unit_shape_mismatch",
  "scenario_ordering_violation",
  "inert_scenario_anchor",
}


def bridge_warning_dicts(warnings: list[BridgeWarning]) -> list[dict]:
  return [asdict(warning) for warning in warnings]


def bridge_resolution_dicts(resolutions: list) -> list[dict]:
  return [asdict(resolution) for resolution in resolutions]


def bridge_unresolved_factors(resolutions: list) -> list[dict]:
  return [
    {
      "factor": resolution.factor,
      "match_reason": resolution.match_reason,
      "candidates": list(resolution.candidates),
    }
    for resolution in resolutions
    if resolution.match_reason == "unresolved"
  ]


def bridge_low_confidence_factors(resolutions: list) -> list[dict]:
  return [
    {
      "factor": resolution.factor,
      "owner_id": resolution.owner_id,
      "anchor_id": resolution.anchor_id,
      "score": resolution.score,
    }
    for resolution in resolutions
    if resolution.match_reason == "label_match_low_confidence"
  ]


def bridge_missing_snapshot_fields(warnings: list[BridgeWarning]) -> list[dict]:
  missing: list[dict] = []
  for warning in warnings:
    if warning.kind not in {"missing_snapshot_field", "non_numeric_snapshot_field"}:
      continue
    case = None
    field = warning.field
    if warning.field and "." in warning.field:
      case, field = warning.field.split(".", 1)
    reason = warning.detail
    if warning.kind == "non_numeric_snapshot_field" and reason == "value_was_string":
      reason = "value_was_string"
    elif warning.kind == "non_numeric_snapshot_field":
      reason = "value_was_non_numeric"
    missing.append({"case": case, "field": field, "reason": reason})
  return missing


def bridge_stringify_period_values(values: dict[int, float] | None) -> dict[str, float]:
  return {
    str(int(period)): float(value)
    for period, value in sorted((values or {}).items())
    if value is not None
  }


def bridge_workbook_outputs(
  scenario_outputs_by_case: dict[str, dict[str, dict[int, float]]] | None,
  *,
  terminal_year: int | None,
) -> dict | None:
  if not scenario_outputs_by_case:
    return None
  scenario_outputs: dict[str, dict[str, dict[str, float]]] = {}
  scenario_eps: dict[str, dict[str, float]] = {}
  for case in ("bull", "base", "bear"):
    fields = scenario_outputs_by_case.get(case) or {}
    field_payload = {
      field: payload
      for field, values in sorted(fields.items())
      if (payload := bridge_stringify_period_values(values))
    }
    if field_payload:
      scenario_outputs[case] = field_payload
    eps_values = field_payload.get("adj_eps")
    if eps_values:
      scenario_eps[case] = eps_values
  if not scenario_outputs and not scenario_eps:
    return None
  return {
    "source": "model_graph_recompute",
    "terminal_year": int(terminal_year) if terminal_year is not None else None,
    "scenario_eps": scenario_eps,
    "scenario_outputs": scenario_outputs,
  }


def empty_bridge_projection_persistence() -> dict:
  return {
    "ordering": "persist_first_then_workbook",
    "attempts": 0,
    "successes": 0,
    "failures": 0,
    "schema_version_bumped": False,
    "cross_skill_notes": [],
  }


def bridge_base_result(
  *,
  status: str,
  reason: str | None = None,
  operations_applied: int = 0,
  operations_failed: int = 0,
  rolled_back: bool = False,
  output_path: str | None = None,
  workbook_status: dict | None = None,
  results: list | None = None,
  stale_file_detected: bool = False,
  resolutions: list | None = None,
  warnings: list[BridgeWarning] | None = None,
  projection_persistence: dict | None = None,
  workbook_outputs: dict | None = None,
  durable_file_mode: dict | None = None,
  recovery: dict | None = None,
  bridge_validation: dict | None = None,
  operation_groups: dict | None = None,
) -> dict:
  resolutions = resolutions or []
  warnings = warnings or []
  payload = {
    "status": status,
    "operations_applied": operations_applied,
    "operations_failed": operations_failed,
    "workbook_status": workbook_status,
    "unresolved_factors": bridge_unresolved_factors(resolutions),
    "low_confidence_factors": bridge_low_confidence_factors(resolutions),
    "missing_snapshot_fields": bridge_missing_snapshot_fields(warnings),
    "rolled_back": rolled_back,
    "stale_file_detected": stale_file_detected,
    "output_path": output_path,
    "results": results or [],
    "resolutions": bridge_resolution_dicts(resolutions),
    "warnings": bridge_warning_dicts(warnings),
    "projection_persistence": projection_persistence
    or empty_bridge_projection_persistence(),
  }
  if operation_groups is not None:
    payload["operation_groups"] = operation_groups
  if bridge_validation is not None:
    payload["bridge_validation"] = bridge_validation
  if workbook_outputs is not None:
    payload["workbook_outputs"] = workbook_outputs
  if durable_file_mode is not None:
    payload["durable_file_mode"] = durable_file_mode
  if reason is not None:
    payload["reason"] = reason
  recovery_payload = recovery or bridge_period_coverage_recovery(warnings) or bridge_recovery(reason)
  if recovery_payload is not None:
    payload["recovery"] = recovery_payload
  return payload


def bridge_final_status(
  *,
  operations_failed: int,
  unresolved_factors: list[dict],
  low_confidence_factors: list[dict],
  missing_snapshot_fields: list[dict],
  warnings: list[BridgeWarning],
  workbook_status: dict | None,
) -> str:
  if bridge_has_non_workbook_partial_cause(
    operations_failed=operations_failed,
    unresolved_factors=unresolved_factors,
    low_confidence_factors=low_confidence_factors,
    missing_snapshot_fields=missing_snapshot_fields,
    warnings=warnings,
  ):
    return "partial"
  if bridge_workbook_dispatch_failed(workbook_status):
    return "partial"
  return "ok"


def bridge_has_non_workbook_partial_cause(
  *,
  operations_failed: int,
  unresolved_factors: list[dict],
  low_confidence_factors: list[dict],
  missing_snapshot_fields: list[dict],
  warnings: list[BridgeWarning],
) -> bool:
  if operations_failed > 0:
    return True
  if unresolved_factors or low_confidence_factors or missing_snapshot_fields:
    return True
  return any(warning.kind in BRIDGE_PARTIAL_WARNING_KINDS for warning in warnings)


def bridge_workbook_dispatch_failed(workbook_status: dict | None) -> bool:
  return bool(workbook_status and workbook_status.get("status") == "error")


def bridge_live_dispatch_only_reason(
  *,
  operations_failed: int,
  unresolved_factors: list[dict],
  low_confidence_factors: list[dict],
  missing_snapshot_fields: list[dict],
  warnings: list[BridgeWarning],
  workbook_status: dict | None,
  workbook_outputs: dict | None,
) -> str | None:
  if workbook_outputs is None:
    return "scenario_not_decision_usable"
  if not bridge_workbook_dispatch_failed(workbook_status):
    return None
  if bridge_has_non_workbook_partial_cause(
    operations_failed=operations_failed,
    unresolved_factors=unresolved_factors,
    low_confidence_factors=low_confidence_factors,
    missing_snapshot_fields=missing_snapshot_fields,
    warnings=warnings,
  ):
    return None
  return "live_workbook_unavailable"


def bridge_non_workbook_partial_reason(
  *,
  operations_failed: int,
  unresolved_factors: list[dict],
  low_confidence_factors: list[dict],
  missing_snapshot_fields: list[dict],
  warnings: list[BridgeWarning],
) -> str | None:
  if operations_failed > 0:
    return "operation_failed"
  if unresolved_factors:
    return "unresolved_factor"
  if low_confidence_factors:
    return "low_confidence_factor"
  if missing_snapshot_fields:
    return "snapshot_incomplete"
  for warning in warnings:
    if warning.kind in BRIDGE_PARTIAL_WARNING_KINDS:
      return warning.kind
  return None


def bridge_durable_file_mode(
  *,
  reason: str | None,
  output_path: str | None,
  workbook_outputs: dict | None,
) -> dict | None:
  if reason != "live_workbook_unavailable" or workbook_outputs is None:
    return None
  return {
    "status": "ok",
    "reason": "file_write_and_recompute_succeeded",
    "output_path": output_path,
    "workbook_outputs_source": workbook_outputs.get("source"),
  }


def bridge_is_snapshot_op(op: Any) -> bool:
  return str(getattr(op, "item_id", "") or "").startswith("tpl.s.thesis_snapshot.")


def bridge_split_operations(ops: list[Any]) -> tuple[list[Any], list[Any], dict]:
  driver_ops = [op for op in ops if not bridge_is_snapshot_op(op)]
  snapshot_ops = [op for op in ops if bridge_is_snapshot_op(op)]
  return driver_ops, snapshot_ops, {
    "driver_ops_planned": len(driver_ops),
    "snapshot_ops_planned": len(snapshot_ops),
  }


def bridge_operation_result_dicts(results: list[Any]) -> list[dict]:
  return [
    result.model_dump(mode="json") if hasattr(result, "model_dump") else dict(result)
    for result in results
  ]


def bridge_apply_ops_to_model_copy(
  model: Any,
  ops: list[Any],
  *,
  file_path: str,
  best_effort: bool,
) -> tuple[Any, list[Any], bool]:
  from schema.modify import _assert_layout_integrity, _dispatch_op

  snapshot = copy.deepcopy(model)
  results: list[Any] = []
  rolled_back = False
  for op in ops:
    try:
      result = _dispatch_op(snapshot, op, file_path=file_path)
      results.append(result)
      if getattr(result, "status", None) == "error" and not best_effort:
        rolled_back = True
        break
    except Exception as exc:
      from schema.modify import OperationResult

      results.append(
        OperationResult(
          op_type=op.type,
          item_id=op.item_id,
          status="error",
          reason=str(exc),
        )
      )
      if not best_effort:
        rolled_back = True
        break

  if not rolled_back:
    snapshot.build_index()
    try:
      _assert_layout_integrity(snapshot)
    except Exception as exc:
      from schema.modify import OperationResult, OperationType

      results.append(
        OperationResult(
          op_type=OperationType.set_value,
          item_id=None,
          status="error",
          reason=f"validation_failed: {exc}",
        )
      )
      rolled_back = True
  return snapshot, results, rolled_back


def bridge_terminal_analytical_eps(scenario_payload: dict) -> dict[str, float]:
  analytical: dict[str, float] = {}
  for case in ("bull", "base", "bear"):
    case_payload = scenario_payload.get(case)
    if not isinstance(case_payload, dict):
      continue
    for key in ("adj_eps", "eps", "eps_non_gaap", "eps_gaap"):
      value = case_payload.get(key)
      if isinstance(value, (int, float)) and not isinstance(value, bool):
        analytical[case] = float(value)
        break
  return analytical


def bridge_eps_validation_issues(
  eps_by_case: dict[str, dict[int, float]],
  *,
  terminal_year: int,
  projection_periods: list[int],
) -> dict:
  required_cases = ("bull", "base", "bear")
  missing_cases = [case for case in required_cases if not eps_by_case.get(case)]
  terminal = int(terminal_year)
  terminal_required = terminal in {int(period) for period in projection_periods}
  missing_terminal_cases = [
    case
    for case in required_cases
    if terminal_required and terminal not in eps_by_case.get(case, {})
  ]

  common_periods = sorted(
    set(eps_by_case.get("bull", {}))
    & set(eps_by_case.get("base", {}))
    & set(eps_by_case.get("bear", {}))
  )
  ordering_issues: list[str] = []
  delta_issues: list[str] = []
  bull_moved = False
  bear_moved = False
  for period in common_periods:
    bull = float(eps_by_case["bull"][period])
    base = float(eps_by_case["base"][period])
    bear = float(eps_by_case["bear"][period])
    if not (bull >= base >= bear):
      ordering_issues.append(
        f"{period}:bull={bull:g},base={base:g},bear={bear:g},expected=bull>=base>=bear"
      )
    bull_moved = bull_moved or abs(bull - base) > 1e-9
    bear_moved = bear_moved or abs(bear - base) > 1e-9
  if common_periods and not bull_moved:
    delta_issues.append("bull_eps_did_not_move_from_base")
  if common_periods and not bear_moved:
    delta_issues.append("bear_eps_did_not_move_from_base")

  return {
    "missing_cases": missing_cases,
    "terminal_required": terminal_required,
    "missing_terminal_cases": missing_terminal_cases,
    "common_periods": common_periods,
    "eps_ordering_issues": ordering_issues,
    "eps_delta_issues": delta_issues,
  }


def bridge_validate_scenario_propagation(
  *,
  model: Any,
  file_path: str,
  driver_ops: list[Any],
  snapshot_ops: list[Any],
  warnings: list[BridgeWarning],
  scenario_payload: dict,
  terminal_year: int,
  projection_periods: list[int],
  best_effort: bool,
  compute_scenario_eps_fn: Any = None,
) -> dict:
  validation = {
    "status": "failed",
    "reason": None,
    "driver_ops_planned": len(driver_ops),
    "snapshot_ops_blocked_until_driver_validation": len(snapshot_ops),
    "terminal_year": int(terminal_year),
    "scenario_eps": {},
    "analytical_terminal_eps": bridge_terminal_analytical_eps(scenario_payload),
    "dry_run_results": [],
  }

  blocking_warnings = [
    warning
    for warning in warnings
    if warning.kind in {"inert_scenario_anchor", "scenario_ordering_violation", "unit_shape_mismatch"}
  ]
  if blocking_warnings:
    validation["reason"] = "blocking_bridge_warnings"
    validation["blocking_warnings"] = bridge_warning_dicts(blocking_warnings)
    return validation
  if not driver_ops:
    validation["reason"] = "no_driver_operations"
    return validation

  dry_run_model, dry_run_results, rolled_back = bridge_apply_ops_to_model_copy(
    model,
    driver_ops,
    file_path=file_path,
    best_effort=best_effort,
  )
  validation["dry_run_results"] = bridge_operation_result_dicts(dry_run_results)
  if rolled_back:
    validation["reason"] = "dry_run_operation_failed"
    return validation

  eps_compute = compute_scenario_eps_fn or compute_scenario_eps
  eps_by_case = eps_compute(dry_run_model)
  workbook_outputs = bridge_workbook_outputs(
    {case: {"adj_eps": values} for case, values in eps_by_case.items()},
    terminal_year=int(terminal_year),
  )
  validation["scenario_eps"] = (workbook_outputs or {}).get("scenario_eps", {})
  issues = bridge_eps_validation_issues(
    eps_by_case,
    terminal_year=int(terminal_year),
    projection_periods=projection_periods,
  )
  validation.update(issues)
  if issues["missing_cases"]:
    validation["reason"] = "missing_recomputed_eps_cases"
    return validation
  if issues["missing_terminal_cases"]:
    validation["reason"] = "missing_terminal_year_eps"
    return validation
  if not issues["common_periods"]:
    validation["reason"] = "no_common_recomputed_eps_periods"
    return validation
  if issues["eps_ordering_issues"]:
    validation["reason"] = "eps_ordering_violation"
    return validation
  if issues["eps_delta_issues"]:
    validation["reason"] = "scenario_eps_did_not_move"
    return validation

  validation["status"] = "ok"
  validation["reason"] = None
  return validation


__all__ = [
  "BRIDGE_PARTIAL_WARNING_KINDS",
  "bridge_apply_ops_to_model_copy",
  "bridge_base_result",
  "bridge_durable_file_mode",
  "bridge_eps_validation_issues",
  "bridge_final_status",
  "bridge_has_non_workbook_partial_cause",
  "bridge_is_snapshot_op",
  "bridge_live_dispatch_only_reason",
  "bridge_low_confidence_factors",
  "bridge_missing_snapshot_fields",
  "bridge_non_workbook_partial_reason",
  "bridge_operation_result_dicts",
  "bridge_resolution_dicts",
  "bridge_stringify_period_values",
  "bridge_split_operations",
  "bridge_terminal_analytical_eps",
  "bridge_unresolved_factors",
  "bridge_validate_scenario_propagation",
  "bridge_warning_dicts",
  "bridge_workbook_dispatch_failed",
  "bridge_workbook_outputs",
  "empty_bridge_projection_persistence",
]
