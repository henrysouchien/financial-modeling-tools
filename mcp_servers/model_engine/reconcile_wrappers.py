from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any, get_type_hints


ParentNamespaceProvider = Callable[[], dict[str, Any]]


@dataclass(frozen=True)
class ReconcileWrapperFunctions:
  validate_reconcile_mode: Callable[[str], str | dict]
  validate_reconcile_build_kwargs: Callable[[dict], dict | dict[str, Any]]
  validate_reconcile_file_path_match: Callable[[str, dict], dict | None]
  validate_inputs_field: Callable[[dict, str], dict | None]
  validate_reconcile_diagnostic_report: Callable[[dict], dict | dict[str, Any]]
  flagged_reconcile_entries: Callable[[dict[str, Any]], list[dict[str, Any]] | dict]
  group_payload: Callable[[str, tuple[str, str], list[dict[str, Any]]], dict]
  worst_severity: Callable[[list[dict[str, Any]]], str]
  before_summary: Callable[[list[dict[str, Any]]], dict]
  after_summary: Callable[[dict | None, str, str], dict]
  diagnostic_group_cleared: Callable[[dict | None, str, str], bool]
  list_metrics: Callable[..., list[dict]]
  metrics_by_tag_for_concept: Callable[[list[dict], str, int], dict[str, list[float]]]
  best_value_for_target: Callable[[list[float], float, float], tuple[float, float]]
  evaluate_reconcile_candidates: Callable[..., dict]
  current_registry_revision: Callable[[], str | dict]
  override_directory: Callable[[], Path]
  resolve_reconcile_override_path: Callable[[str], Path]
  new_override_field: Callable[..., dict]
  override_conflict_response: Callable[[str, list[dict[str, Any]], str, dict, dict], dict]
  ticker_override_lock: Callable[[str], Any]


def _bind_parent_private(
  function: Callable[..., Any],
  parent_namespace: ParentNamespaceProvider,
  *,
  annotations: dict[str, Any] | None = None,
) -> Callable[..., Any]:
  parent_module = parent_namespace().get("__name__")
  if isinstance(parent_module, str):
    function.__module__ = parent_module
    function.__qualname__ = function.__name__
    function.__annotations__ = (
      annotations if annotations is not None else get_type_hints(function)
    )
  return function


def build_reconcile_wrapper_functions(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> ReconcileWrapperFunctions:
  def _validate_reconcile_mode(mode: str) -> str | dict:
    ns = parent_namespace()
    return ns["_validate_reconcile_mode_impl"](
      mode,
      reconcile_error_fn=ns["_reconcile_error"],
    )

  def _validate_reconcile_build_kwargs(build_kwargs: dict) -> dict | dict[str, Any]:
    ns = parent_namespace()
    signature = ns["_MODEL_BUILD_SIGNATURE"] or ns["inspect"].signature(ns["model_build"])
    return ns["_validate_reconcile_build_kwargs_impl"](
      build_kwargs,
      coerce_json_dict_arg_fn=ns["_coerce_json_dict_arg"],
      signature=signature,
      reconcile_error_fn=ns["_reconcile_error"],
      inspect_module=ns["inspect"],
    )

  def _validate_reconcile_file_path_match(
    file_path: str,
    build_kwargs: dict,
  ) -> dict | None:
    ns = parent_namespace()
    return ns["_validate_reconcile_file_path_match_impl"](
      file_path,
      build_kwargs,
      reconcile_error_fn=ns["_reconcile_error"],
      normalized_reconcile_path_fn=ns["_normalized_reconcile_path"],
    )

  def _validate_inputs_field(entry: dict, where: str) -> dict | None:
    ns = parent_namespace()
    return ns["_validate_inputs_field_impl"](
      entry,
      where,
      reconcile_error_fn=ns["_reconcile_error"],
    )

  def _validate_reconcile_diagnostic_report(
    diagnostic_report: dict,
  ) -> dict | dict[str, Any]:
    ns = parent_namespace()
    return ns["_validate_reconcile_diagnostic_report_impl"](
      diagnostic_report,
      diagnostics_checked=ns["_RECONCILE_DIAGNOSTICS_CHECKED"],
      reconcile_error_fn=ns["_reconcile_error"],
    )

  def _flagged_reconcile_entries(
    diagnostics: dict[str, Any],
  ) -> list[dict[str, Any]] | dict:
    ns = parent_namespace()
    return ns["_flagged_reconcile_entries_impl"](
      diagnostics,
      validate_inputs_field_fn=ns["_validate_inputs_field"],
    )

  def _group_payload(
    ticker: str,
    key: tuple[str, str],
    entries: list[dict[str, Any]],
  ) -> dict:
    ns = parent_namespace()
    return ns["_group_payload_impl"](
      ticker,
      key,
      entries,
      delta_by_year_fn=ns["_delta_by_year"],
      filing_pointer_fn=ns["_filing_pointer"],
    )

  def _worst_severity(entries: list[dict[str, Any]]) -> str:
    ns = parent_namespace()
    return ns["_worst_severity_impl"](
      entries,
      severity_order=ns["_RECONCILE_SEVERITY_ORDER"],
    )

  def _before_summary(entries: list[dict[str, Any]]) -> dict:
    ns = parent_namespace()
    return ns["_before_summary_impl"](
      entries,
      worst_severity_fn=ns["_worst_severity"],
      delta_by_year_fn=ns["_delta_by_year"],
    )

  def _after_summary(
    report: dict | None,
    diagnostic_type: str,
    subtotal_name: str,
  ) -> dict:
    ns = parent_namespace()
    return ns["_after_summary_impl"](
      report,
      diagnostic_type,
      subtotal_name,
      serialize_diagnostic_report_fn=ns["_serialize_diagnostic_report"],
      entries_from_serialized_report_fn=ns["_entries_from_serialized_report"],
      worst_severity_fn=ns["_worst_severity"],
      delta_by_year_fn=ns["_delta_by_year"],
    )

  def _diagnostic_group_cleared(
    report: dict | None,
    diagnostic_type: str,
    subtotal_name: str,
  ) -> bool:
    ns = parent_namespace()
    return ns["_diagnostic_group_cleared_impl"](
      report,
      diagnostic_type,
      subtotal_name,
      serialize_diagnostic_report_fn=ns["_serialize_diagnostic_report"],
      entries_from_serialized_report_fn=ns["_entries_from_serialized_report"],
    )

  def _list_metrics(
    ticker: str,
    year: int,
    quarter: int = 4,
    *,
    date_type: str = "FY",
    include_values: bool = True,
    limit: int = 1000,
  ) -> list[dict]:
    ns = parent_namespace()
    return ns["_list_metrics_impl"](
      ticker,
      year,
      quarter,
      date_type=date_type,
      include_values=include_values,
      limit=limit,
      os_module=ns["os"],
      urllib_parse_module=ns["urllib"].parse,
      urllib_request_module=ns["urllib"].request,
      json_module=ns["json"],
    )

  def _metrics_by_tag_for_concept(
    entries: list[dict],
    concept_id: str,
    year: int,
  ) -> dict[str, list[float]]:
    ns = parent_namespace()
    return ns["_metrics_by_tag_for_concept_impl"](
      entries,
      concept_id,
      year,
      metric_value_for_concept_fn=ns["_metric_value_for_concept"],
    )

  def _best_value_for_target(
    values: list[float],
    target: float,
    floor_abs_m: float,
  ) -> tuple[float, float]:
    ns = parent_namespace()
    return ns["_best_value_for_target_impl"](
      values,
      target,
      floor_abs_m,
      relative_distance_fn=ns["_relative_distance"],
    )

  def _evaluate_reconcile_candidates(
    *,
    diagnostic_type: str,
    subtotal_name: str,
    entries: list[dict[str, Any]],
    tags_by_year: dict[int, list[dict]],
    match_tolerance_pct: float,
    match_floor_abs_m: float,
  ) -> dict:
    ns = parent_namespace()
    return ns["_evaluate_reconcile_candidates_impl"](
      diagnostic_type=diagnostic_type,
      subtotal_name=subtotal_name,
      entries=entries,
      tags_by_year=tags_by_year,
      match_tolerance_pct=match_tolerance_pct,
      match_floor_abs_m=match_floor_abs_m,
      candidate_concepts_by_key=ns["_RECONCILE_CANDIDATE_CONCEPTS"],
      delta_by_year_fn=ns["_delta_by_year"],
      current_values_from_diagnostic_inputs_fn=ns[
        "_current_values_from_diagnostic_inputs"
      ],
      target_value_for_candidate_fn=ns["_target_value_for_candidate"],
      metrics_by_tag_for_concept_fn=ns["_metrics_by_tag_for_concept"],
      best_value_for_target_fn=ns["_best_value_for_target"],
    )

  def _current_registry_revision() -> str | dict:
    ns = parent_namespace()
    return ns["_current_registry_revision_impl"](
      get_registry_cache_fn=ns["get_registry_cache"],
      reconcile_error_fn=ns["_reconcile_error"],
    )

  def _override_directory() -> Path:
    ns = parent_namespace()
    return ns["_override_directory_impl"](
      schema_overrides_module=ns["schema_overrides"],
    )

  def _resolve_reconcile_override_path(ticker_upper: str) -> Path:
    ns = parent_namespace()
    return ns["_resolve_reconcile_override_path_impl"](
      ticker_upper,
      schema_overrides_module=ns["schema_overrides"],
      override_directory_fn=ns["_override_directory"],
    )

  def _new_override_field(
    *,
    matched_tag: str,
    diagnostic_type: str,
    subtotal_name: str,
    entries: list[dict[str, Any]],
    registry_revision: str,
  ) -> dict:
    ns = parent_namespace()
    return ns["_new_override_field_impl"](
      matched_tag=matched_tag,
      diagnostic_type=diagnostic_type,
      subtotal_name=subtotal_name,
      entries=entries,
      registry_revision=registry_revision,
      delta_by_year_fn=ns["_delta_by_year"],
      tag_local_part_fn=ns["_tag_local_part"],
    )

  def _override_conflict_response(
    ticker: str,
    entries: list[dict[str, Any]],
    affected_input_concept: str,
    existing_override: dict,
    proposed_override: dict,
  ) -> dict:
    ns = parent_namespace()
    return ns["_override_conflict_response_impl"](
      ticker,
      entries,
      affected_input_concept,
      existing_override,
      proposed_override,
      filing_pointer_fn=ns["_filing_pointer"],
    )

  def _ticker_override_lock(ticker_upper: str):
    ns = parent_namespace()
    return ns["_ticker_override_lock_impl"](
      ticker_upper,
      override_directory_fn=ns["_override_directory"],
      fcntl_module=ns["fcntl"],
    )

  return ReconcileWrapperFunctions(
    validate_reconcile_mode=_bind_parent_private(
      _validate_reconcile_mode,
      parent_namespace,
    ),
    validate_reconcile_build_kwargs=_bind_parent_private(
      _validate_reconcile_build_kwargs,
      parent_namespace,
    ),
    validate_reconcile_file_path_match=_bind_parent_private(
      _validate_reconcile_file_path_match,
      parent_namespace,
    ),
    validate_inputs_field=_bind_parent_private(
      _validate_inputs_field,
      parent_namespace,
    ),
    validate_reconcile_diagnostic_report=_bind_parent_private(
      _validate_reconcile_diagnostic_report,
      parent_namespace,
    ),
    flagged_reconcile_entries=_bind_parent_private(
      _flagged_reconcile_entries,
      parent_namespace,
    ),
    group_payload=_bind_parent_private(_group_payload, parent_namespace),
    worst_severity=_bind_parent_private(_worst_severity, parent_namespace),
    before_summary=_bind_parent_private(_before_summary, parent_namespace),
    after_summary=_bind_parent_private(_after_summary, parent_namespace),
    diagnostic_group_cleared=_bind_parent_private(
      _diagnostic_group_cleared,
      parent_namespace,
    ),
    list_metrics=_bind_parent_private(_list_metrics, parent_namespace),
    metrics_by_tag_for_concept=_bind_parent_private(
      _metrics_by_tag_for_concept,
      parent_namespace,
    ),
    best_value_for_target=_bind_parent_private(
      _best_value_for_target,
      parent_namespace,
    ),
    evaluate_reconcile_candidates=_bind_parent_private(
      _evaluate_reconcile_candidates,
      parent_namespace,
    ),
    current_registry_revision=_bind_parent_private(
      _current_registry_revision,
      parent_namespace,
    ),
    override_directory=_bind_parent_private(_override_directory, parent_namespace),
    resolve_reconcile_override_path=_bind_parent_private(
      _resolve_reconcile_override_path,
      parent_namespace,
    ),
    new_override_field=_bind_parent_private(_new_override_field, parent_namespace),
    override_conflict_response=_bind_parent_private(
      _override_conflict_response,
      parent_namespace,
    ),
    ticker_override_lock=_bind_parent_private(
      _ticker_override_lock,
      parent_namespace,
    ),
  )


__all__ = [
  "ParentNamespaceProvider",
  "ReconcileWrapperFunctions",
  "build_reconcile_wrapper_functions",
]
