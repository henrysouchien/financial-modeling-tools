from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, get_type_hints


ParentNamespaceProvider = Callable[[], dict[str, Any]]


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


def build_model_build_deps_function(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> Callable[[], Any]:
  def _model_build_deps() -> Any:
    ns = parent_namespace()
    return ns["_ModelBuildDeps"](
      new_model_build_id=lambda: ns["uuid"].uuid4().hex,
      expanduser=ns["os"].path.expanduser,
      historical_sources_cls=ns["HistoricalSources"],
      business_model_cls=ns["BusinessModel"],
      coerce_json_dict_arg=ns["_coerce_json_dict_arg"],
      load_business_model_from_path=ns["_load_business_model_from_path"],
      logger=ns["logging"],
      historical_sources_touch_fmp=ns["_historical_sources_touch_fmp"],
      historical_sources_touch_edgar=ns["_historical_sources_touch_edgar"],
      fmp_zero_missing_edgar_fallback_needed=ns["_fmp_zero_missing_edgar_fallback_needed"],
      asyncio_run=ns["asyncio"].run,
      fetch_fmp_financials=ns["fetch_fmp_financials"],
      build_valuation_comps_fallback=ns["_build_valuation_comps_fallback"],
      make_edgar_fetcher=ns["_make_edgar_fetcher"],
      make_edgar_financials_fetcher=ns["_make_edgar_financials_fetcher"],
      warm_edgar_cache=ns["warm_edgar_cache"],
      accumulate_tree=ns["_accumulate_tree"],
      build_model=ns["_build_model"],
      load_model_bundle=ns["_load_model_bundle"],
      render_plan_to_addin_payload=ns["render_plan_to_addin_payload"],
      dispatch_to_addin=ns["_dispatch_to_addin"],
      addin_dispatch_error_status=ns["addin_dispatch_error_status"],
      seed_projections_result_cls=ns["SeedProjectionsResult"],
      serialize_diagnostic_report=ns["_serialize_diagnostic_report"],
      asdict=ns["asdict"],
      is_dataclass=ns["is_dataclass"],
      model_build_error_payload=ns["_model_build_error_payload"],
      model_handle_token_payload=ns["_model_handle_token_payload"],
    )

  return _bind_parent_private(
    _model_build_deps,
    parent_namespace,
    annotations={"return": parent_namespace()["_ModelBuildDeps"]},
  )


def build_comps_build_deps_function(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> Callable[[], Any]:
  def _comps_build_deps() -> Any:
    ns = parent_namespace()
    return ns["_CompsBuildDeps"](
      new_comps_build_id=lambda: ns["uuid"].uuid4().hex,
      coerce_json_dict_arg=ns["_coerce_json_dict_arg"],
      normalize_comps_payload=ns["normalize_comps_payload"],
      render_comps_rows=ns["render_comps_rows"],
      comps_grid_payload=ns["comps_grid_payload"],
      render_comps_plan=ns["render_comps_plan"],
      write_xlsx=ns["write_xlsx"],
      render_plan_to_addin_payload=ns["render_plan_to_addin_payload"],
      dispatch_to_addin=ns["_dispatch_to_addin"],
      addin_dispatch_error_status=ns["addin_dispatch_error_status"],
      validation_error_type=ns["ValidationError"],
      comps_build_error_payload=ns["_comps_build_error_payload"],
    )

  return _bind_parent_private(
    _comps_build_deps,
    parent_namespace,
    annotations={"return": parent_namespace()["_CompsBuildDeps"]},
  )


def build_reconcile_subtotal_integrity_deps_function(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> Callable[[], Any]:
  def _reconcile_subtotal_integrity_deps() -> Any:
    ns = parent_namespace()
    return ns["_ReconcileSubtotalIntegrityDeps"](
      validate_reconcile_mode=ns["_validate_reconcile_mode"],
      validate_reconcile_build_kwargs=ns["_validate_reconcile_build_kwargs"],
      validate_reconcile_file_path_match=ns["_validate_reconcile_file_path_match"],
      validate_reconcile_diagnostic_report=ns["_validate_reconcile_diagnostic_report"],
      reconcile_error=ns["_reconcile_error"],
      flagged_reconcile_entries=ns["_flagged_reconcile_entries"],
      diagnostics_checked=ns["_RECONCILE_DIAGNOSTICS_CHECKED"],
      group_reconcile_entries=ns["_group_reconcile_entries"],
      group_payload=ns["_group_payload"],
      delta_by_year=ns["_delta_by_year"],
      filing_pointer=ns["_filing_pointer"],
      unsupported_subtotals=ns["_RECONCILE_UNSUPPORTED_SUBTOTALS"],
      candidate_concepts_by_key=ns["_RECONCILE_CANDIDATE_CONCEPTS"],
      normalize_metric_entries=ns["_normalize_metric_entries"],
      list_metrics=ns["_list_metrics"],
      evaluate_reconcile_candidates=ns["_evaluate_reconcile_candidates"],
      current_registry_revision=ns["_current_registry_revision"],
      new_override_field=ns["_new_override_field"],
      resolve_reconcile_override_path=ns["_resolve_reconcile_override_path"],
      load_ticker_overrides=ns["load_ticker_overrides"],
      override_conflict=ns["_override_conflict"],
      merged_ticker_overrides=ns["_merged_ticker_overrides"],
      override_conflict_response=ns["_override_conflict_response"],
      serialize_ticker_overrides=ns["_serialize_ticker_overrides"],
      existing_override_preserved=ns["_existing_override_preserved"],
      ticker_override_lock=ns["_ticker_override_lock"],
      save_ticker_overrides=ns["save_ticker_overrides"],
      model_build=ns["model_build"],
      before_summary=ns["_before_summary"],
      after_summary=ns["_after_summary"],
      diagnostic_group_cleared=ns["_diagnostic_group_cleared"],
      rebuild_summary=ns["_rebuild_summary"],
    )

  return _bind_parent_private(
    _reconcile_subtotal_integrity_deps,
    parent_namespace,
    annotations={"return": parent_namespace()["_ReconcileSubtotalIntegrityDeps"]},
  )


def build_model_override_deps_function(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> Callable[[], Any]:
  def _model_override_deps() -> Any:
    ns = parent_namespace()
    return ns["_ModelOverrideDeps"](
      list_ticker_overrides=ns["list_ticker_overrides"],
      load_ticker_overrides=ns["load_ticker_overrides"],
      save_ticker_overrides=ns["save_ticker_overrides"],
      serialize_ticker_overrides=ns["_serialize_ticker_overrides"],
      coerce_json_dict_arg=ns["_coerce_json_dict_arg"],
      coerce_json_list_arg=ns["_coerce_json_list_arg"],
      normalize_projection_entry_payload=ns["_normalize_projection_entry_payload"],
      projection_entry_cls=ns["ProjectionEntry"],
      projection_entry_validation_payload=ns["_projection_entry_validation_payload"],
      validation_error_type=ns["ValidationError"],
      ticker_override_lock=ns["_ticker_override_lock"],
      ticker_overrides_cls=ns["TickerOverrides"],
      merge_projection_scenarios=ns["_merge_projection_scenarios"],
      get_projection_data=ns["_get_projection_data"],
      delete_projection_entry=ns["_delete_projection_entry"],
      projection_summaries=ns["_projection_summaries"],
      prune_projection_scenarios=ns["_prune_projection_scenarios"],
      merge_override_fields=ns["_merge_override_fields"],
      custom_concept_entry=ns["_custom_concept_entry"],
      remove_override_concept=ns["_remove_override_concept"],
    )

  return _bind_parent_private(
    _model_override_deps,
    parent_namespace,
    annotations={"return": parent_namespace()["_ModelOverrideDeps"]},
  )


def build_model_modify_deps_function(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> Callable[[], Any]:
  def _model_modify_deps() -> Any:
    from schema import modify as modify_module

    ns = parent_namespace()
    return ns["_ModelModifyDeps"](
      coerce_json_list_arg=ns["_coerce_json_list_arg"],
      current_year=lambda: ns["datetime"].now().year,
      normalize_modify_operation_payload=ns["_normalize_modify_operation_payload"],
      operation_cls=modify_module.Operation,
      apply_modify_request=modify_module.apply_modify_request,
      modify_error_type=modify_module.ModifyError,
      validate_model_handle_token=ns["validate_model_handle_token"],
      model_handle_token_cls=ns["ModelHandleToken"],
      model_handle_token_error_type=ns["ModelHandleTokenError"],
      model_handle_token_payload=ns["_model_handle_token_payload"],
      peek_handle=ns["_peek_handle"],
      load_handle=ns["_load_handle"],
      load_on_cache_miss_enabled=modify_module._load_on_cache_miss_enabled,
    )

  return _bind_parent_private(
    _model_modify_deps,
    parent_namespace,
    annotations={"return": parent_namespace()["_ModelModifyDeps"]},
  )


@dataclass(frozen=True)
class BridgePrivateFunctions:
  graph_downstream_ids: Callable[..., Any]
  bridge_inert_anchor_warnings: Callable[..., Any]
  bridge_validate_scenario_propagation: Callable[..., Any]
  bridge_projection_persistence: Callable[..., Any]
  bridge_model_bundle: Callable[..., Any]
  readiness_owner_anchor_payload: Callable[..., Any]
  readiness_owner_anchor_hints: Callable[..., Any]


def build_bridge_private_functions(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> BridgePrivateFunctions:
  def _graph_downstream_ids(graph: Any, roots: set[str]) -> set[str]:
    ns = parent_namespace()
    return ns["_graph_downstream_ids_impl"](graph, roots)

  def _bridge_inert_anchor_warnings(model: Any, resolutions: list) -> list:
    ns = parent_namespace()
    return ns["_bridge_inert_anchor_warnings_impl"](
      model,
      resolutions,
      compute_readiness_fn=ns["compute_model_scenario_bridge_readiness"],
    )

  def _bridge_validate_scenario_propagation(**kwargs) -> dict:
    ns = parent_namespace()
    return ns["_bridge_validate_scenario_propagation_impl"](
      **kwargs,
      compute_scenario_eps_fn=ns["compute_scenario_eps"],
    )

  def _bridge_projection_persistence(**kwargs) -> dict:
    ns = parent_namespace()
    return ns["_bridge_projection_persistence_impl"](
      **kwargs,
      model_override_fn=ns["model_override"],
    )

  def _bridge_model_bundle(file_path: str, cutoff: int):
    ns = parent_namespace()
    return ns["_bridge_model_bundle_impl"](
      file_path,
      cutoff,
      load_handle_fn=ns["_load_handle"],
    )

  def _readiness_owner_anchor_payload(model: Any, factor: str) -> dict | None:
    ns = parent_namespace()
    return ns["_readiness_owner_anchor_payload_impl"](
      model,
      factor,
      compute_readiness_fn=ns["compute_model_scenario_bridge_readiness"],
      find_anchor_fn=ns["find_scenario_anchor"],
    )

  def _readiness_owner_anchor_hints(
    model: Any,
    assumptions: list[dict],
    hints: dict | None,
  ) -> dict | None:
    ns = parent_namespace()
    return ns["_readiness_owner_anchor_hints_impl"](
      model,
      assumptions,
      hints,
      compute_readiness_fn=ns["compute_model_scenario_bridge_readiness"],
      find_anchor_fn=ns["find_scenario_anchor"],
    )

  ns = parent_namespace()
  bridge_warning_cls = ns["BridgeWarning"]
  return BridgePrivateFunctions(
    graph_downstream_ids=_bind_parent_private(
      _graph_downstream_ids,
      parent_namespace,
      annotations={
        "graph": ns["DependencyGraph"],
        "roots": set[str],
        "return": set[str],
      },
    ),
    bridge_inert_anchor_warnings=_bind_parent_private(
      _bridge_inert_anchor_warnings,
      parent_namespace,
      annotations={
        "model": ns["Any"],
        "resolutions": list,
        "return": list[bridge_warning_cls],
      },
    ),
    bridge_validate_scenario_propagation=_bind_parent_private(
      _bridge_validate_scenario_propagation,
      parent_namespace,
    ),
    bridge_projection_persistence=_bind_parent_private(
      _bridge_projection_persistence,
      parent_namespace,
    ),
    bridge_model_bundle=_bind_parent_private(_bridge_model_bundle, parent_namespace),
    readiness_owner_anchor_payload=_bind_parent_private(
      _readiness_owner_anchor_payload,
      parent_namespace,
    ),
    readiness_owner_anchor_hints=_bind_parent_private(
      _readiness_owner_anchor_hints,
      parent_namespace,
    ),
  )


def build_model_find_scenario_anchor_deps_function(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> Callable[[], Any]:
  def _model_find_scenario_anchor_deps() -> Any:
    ns = parent_namespace()
    return ns["_ModelFindScenarioAnchorDeps"](
      current_year=lambda: ns["datetime"].now().year,
      bridge_model_bundle=ns["_bridge_model_bundle"],
      bridge_recovery=ns["_bridge_recovery"],
      readiness_owner_anchor_payload=ns["_readiness_owner_anchor_payload"],
      find_scenario_anchor=ns["find_scenario_anchor"],
      anchor_recovery=ns["_anchor_recovery"],
      model_tool_error_payload=ns["_model_tool_error_payload"],
      validate_model_handle_token=ns["validate_model_handle_token"],
      model_handle_token_cls=ns["ModelHandleToken"],
      model_handle_token_error_type=ns["ModelHandleTokenError"],
    )

  return _bind_parent_private(
    _model_find_scenario_anchor_deps,
    parent_namespace,
    annotations={"return": parent_namespace()["_ModelFindScenarioAnchorDeps"]},
  )


def build_model_scenario_topology_deps_function(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> Callable[[], Any]:
  def _model_scenario_topology_deps() -> Any:
    ns = parent_namespace()
    return ns["_ModelScenarioTopologyDeps"](
      current_year=lambda: ns["datetime"].now().year,
      bridge_model_bundle=ns["_bridge_model_bundle"],
      bridge_recovery=ns["_bridge_recovery"],
      coerce_json_list_arg=ns["_coerce_json_list_arg"],
      compute_scenario_bridge_readiness=ns["compute_model_scenario_bridge_readiness"],
      readiness_owner_anchor_payload=ns["_readiness_owner_anchor_payload"],
      find_scenario_anchor=ns["find_scenario_anchor"],
      anchor_recovery=ns["_anchor_recovery"],
      model_tool_error_payload=ns["_model_tool_error_payload"],
      validate_model_handle_token=ns["validate_model_handle_token"],
      model_handle_token_cls=ns["ModelHandleToken"],
      model_handle_token_error_type=ns["ModelHandleTokenError"],
    )

  return _bind_parent_private(
    _model_scenario_topology_deps,
    parent_namespace,
    annotations={"return": parent_namespace()["_ModelScenarioTopologyDeps"]},
  )


def build_model_bridge_scenarios_deps_function(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> Callable[[], Any]:
  def _model_bridge_scenarios_deps() -> Any:
    from schema import modify as modify_module

    ns = parent_namespace()
    return ns["_ModelBridgeScenariosDeps"](
      current_year=lambda: ns["datetime"].now().year,
      coerce_json_list_arg=ns["_coerce_json_list_arg"],
      coerce_json_dict_arg=ns["_coerce_json_dict_arg"],
      bridge_base_result=ns["_bridge_base_result"],
      bridge_model_bundle=ns["_bridge_model_bundle"],
      readiness_owner_anchor_hints=ns["_readiness_owner_anchor_hints"],
      build_bridge_operations=ns["build_bridge_operations"],
      bridge_inert_anchor_warnings=ns["_bridge_inert_anchor_warnings"],
      bridge_split_operations=ns["_bridge_split_operations"],
      bridge_validate_scenario_propagation=ns["_bridge_validate_scenario_propagation"],
      bridge_apply_ops_to_model_copy=ns["_bridge_apply_ops_to_model_copy"],
      bridge_operation_result_dicts=ns["_bridge_operation_result_dicts"],
      compute_scenario_outputs=ns["compute_scenario_outputs"],
      build_snapshot_operations=ns["build_snapshot_operations"],
      bridge_projection_persistence=ns["_bridge_projection_persistence"],
      apply_modify_request=modify_module.apply_modify_request,
      modify_error_type=modify_module.ModifyError,
      load_model_bundle=ns["_load_model_bundle"],
      populate_scenario_eps=ns["_populate_scenario_eps"],
      bridge_workbook_outputs=ns["_bridge_workbook_outputs"],
      render_model=ns["render_model"],
      write_xlsx=ns["write_xlsx"],
      render_plan_to_addin_payload=ns["render_plan_to_addin_payload"],
      dispatch_to_addin=ns["_dispatch_to_addin"],
      addin_dispatch_error_status=ns["addin_dispatch_error_status"],
      bridge_unresolved_factors=ns["_bridge_unresolved_factors"],
      bridge_low_confidence_factors=ns["_bridge_low_confidence_factors"],
      bridge_missing_snapshot_fields=ns["_bridge_missing_snapshot_fields"],
      bridge_final_status=ns["_bridge_final_status"],
      bridge_live_dispatch_only_reason=ns["_bridge_live_dispatch_only_reason"],
      bridge_non_workbook_partial_reason=ns["_bridge_non_workbook_partial_reason"],
      bridge_durable_file_mode=ns["_bridge_durable_file_mode"],
      validate_model_handle_token=ns["validate_model_handle_token"],
      model_handle_token_cls=ns["ModelHandleToken"],
      model_handle_token_error_type=ns["ModelHandleTokenError"],
      model_handle_token_payload=ns["_model_handle_token_payload"],
    )

  return _bind_parent_private(
    _model_bridge_scenarios_deps,
    parent_namespace,
    annotations={"return": parent_namespace()["_ModelBridgeScenariosDeps"]},
  )


__all__ = [
  "BridgePrivateFunctions",
  "ParentNamespaceProvider",
  "build_bridge_private_functions",
  "build_comps_build_deps_function",
  "build_model_bridge_scenarios_deps_function",
  "build_model_build_deps_function",
  "build_model_find_scenario_anchor_deps_function",
  "build_model_scenario_topology_deps_function",
  "build_model_modify_deps_function",
  "build_model_override_deps_function",
  "build_reconcile_subtotal_integrity_deps_function",
]
