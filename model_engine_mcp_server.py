#!/usr/bin/env python3
"""Model Engine MCP Server — exposes schema financial model tools via MCP."""

import asyncio
from dataclasses import asdict, is_dataclass
from datetime import datetime, timezone
import fcntl
import inspect
import json
import logging
import os
from pathlib import Path
import re
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import Any, Optional

def _model_engine_root() -> Path:
  path = Path(__file__).resolve()
  for candidate in (path.parent, *path.parents):
    if (candidate / "schema").is_dir() and (candidate / "mcp_servers" / "model_engine").is_dir():
      return candidate
  return path.parents[1]


ROOT = _model_engine_root()
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "api"))
# Repo-root alone does not expose excel_mcp; add the package path for
# addin_dispatch's lazy auth helper import.
sys.path.insert(0, str(ROOT / "packages" / "excel-mcp" / "python"))

_real_stdout = sys.stdout
sys.stdout = sys.stderr

import bootstrap_env
_REQUIRES_ADDIN_DISPATCH = (ROOT / "packages" / "excel-mcp" / "python").is_dir()
_REQUIRED_ENV = ["EDGAR_API_KEY"]
if _REQUIRES_ADDIN_DISPATCH:
  _REQUIRED_ENV.append("ADDIN_DISPATCH_API_KEY")
bootstrap_env.bootstrap(required=_REQUIRED_ENV)
if _REQUIRES_ADDIN_DISPATCH and not os.environ.get("ADDIN_DISPATCH_API_KEY"):
  raise SystemExit("ADDIN_DISPATCH_API_KEY must be set")

try:
  import fmp
except ModuleNotFoundError:  # pragma: no cover - depends on optional package env
  fmp = None
from pydantic import ValidationError

from api.addin_dispatch import addin_dispatch_error_status, _dispatch_to_addin  # noqa: E402
from api.research.financials_fetch import fetch_fmp_financials, make_edgar_financials_fetcher  # noqa: E402
import schema.business_model_validate as business_model_validate_schema  # noqa: E402
from schema.business_model import BusinessModel  # noqa: E402
from schema.comps_build import (  # noqa: E402
  comps_grid_payload,
  normalize_comps_payload,
  render_comps_plan,
  render_rows as render_comps_rows,
)
from schema.dependency_graph import DependencyGraph  # noqa: E402
from schema.build import (  # noqa: E402
  EdgarWarmResult,
  SeedProjectionsResult,
  _populate_scenario_eps,
  build_model as _build_model,
  compute_scenario_eps,
  compute_scenario_outputs,
  warm_edgar_cache,
  write_xlsx,
)
from schema.kpi_bridge import bridge_kpi_catalog as _bridge_kpi_catalog  # noqa: E402
from schema.kpi_bridge_report import BridgeReport  # noqa: E402
from schema.kpi_overrides_writer import (  # noqa: E402
  business_model_to_overrides as _business_model_to_overrides,
)
from schema.model_build_context import HistoricalSources  # noqa: E402
from schema.model_semantics import ModelSemantics  # noqa: E402
from schema.model_readiness import compute_model_scenario_bridge_readiness  # noqa: E402
import schema.overrides as schema_overrides  # noqa: E402
from schema.overrides import (  # noqa: E402
  TickerOverrides,
  derive_ticker_overrides_schema_version,
  list_ticker_overrides,
  load_ticker_overrides,
  save_ticker_overrides,
)
from schema.overrides_projections import ProjectionEntry  # noqa: E402
from schema.presentation_tree import _accumulate_tree  # noqa: E402
from schema.registry_cache import get_registry_cache  # noqa: E402
from schema.renderer import render_model, render_plan_to_addin_payload  # noqa: E402
from schema.scenario_bridge import (  # noqa: E402
  BridgeWarning,
  build_bridge_operations,
  build_snapshot_operations,
  find_scenario_anchor,
)
from schema.workbook_presentation import (  # noqa: E402
  workbook_presentation_fingerprint,
  workbook_presentation_gap,
)
from schema import CurrentModelRef  # noqa: E402
from schema.annotate import annotate_model_with_research as _annotate_model_with_research  # noqa: E402
from schema.segments import (  # noqa: E402
  SEGMENT_AXES_PRIORITY as _SEGMENT_AXES_PRIORITY,
  discover_all_axes as _discover_all_axes,
  segment_revenue_observation_list as _segment_revenue_observation_list,
)
from schema.handle import (  # noqa: E402
  load_handle as _load_handle,
  peek_handle as _peek_handle,
)
from schema.handle_token import (  # noqa: E402
  ModelHandleToken,
  ModelHandleTokenError,
  validate_model_handle_token,
)
from schema.tools import (  # noqa: E402
  clear_cache as _clear_cache,
  drivers as _drivers,
  find as _find,
  invalid_override_period_error as _invalid_override_period_error,
  invalid_override_value_error as _invalid_override_value_error,
  load as _load_model_bundle,
  model_tool_error_payload as _model_tool_error_payload,
  scenario as _scenario,
  sensitivity as _sensitivity,
  summarize as _summarize,
  values as _values,
)
from mcp_servers.model_engine.args import (  # noqa: E402
  coerce_json_arg as _coerce_json_arg,
  coerce_json_dict_arg as _coerce_json_dict_arg,
  coerce_json_list_arg as _coerce_json_list_arg,
  safe_pydantic_errors as _safe_pydantic_errors,
  valid_workspace_user_id as _valid_workspace_user_id,
)
from mcp_servers.model_engine.business_model_artifacts import (  # noqa: E402
  business_model_workspace_candidates as _business_model_workspace_candidates,
  load_business_model_from_path as _load_business_model_from_path,
  resolve_business_model_path as _resolve_business_model_path,
)
from mcp_servers.model_engine.build_helpers import (  # noqa: E402
  axis_priority as _axis_priority_impl,
  fmp_zero_missing_edgar_fallback_needed as _fmp_zero_missing_edgar_fallback_needed,
  historical_sources_touch_edgar as _historical_sources_touch_edgar,
  historical_sources_touch_fmp as _historical_sources_touch_fmp,
  model_build_error_payload as _model_build_error_payload_impl,
  rebuild_summary as _rebuild_summary,
  serialize_diagnostic_report as _serialize_diagnostic_report,
  serialize_overrides_write_report as _serialize_overrides_write_report,
  serialize_ticker_overrides as _serialize_ticker_overrides,
  serialize_year_values as _serialize_year_values,
)
from mcp_servers.model_engine.handle_tokens import (  # noqa: E402
  model_handle_token_payload as _model_handle_token_payload_impl,
)
from mcp_servers.model_engine.bridge import (  # noqa: E402
  BRIDGE_PARTIAL_WARNING_KINDS as _BRIDGE_PARTIAL_WARNING_KINDS,
  bridge_apply_ops_to_model_copy as _bridge_apply_ops_to_model_copy,
  bridge_base_result as _bridge_base_result,
  bridge_durable_file_mode as _bridge_durable_file_mode,
  bridge_eps_validation_issues as _bridge_eps_validation_issues,
  bridge_final_status as _bridge_final_status,
  bridge_has_non_workbook_partial_cause as _bridge_has_non_workbook_partial_cause,
  bridge_is_snapshot_op as _bridge_is_snapshot_op,
  bridge_live_dispatch_only_reason as _bridge_live_dispatch_only_reason,
  bridge_low_confidence_factors as _bridge_low_confidence_factors,
  bridge_missing_snapshot_fields as _bridge_missing_snapshot_fields,
  bridge_non_workbook_partial_reason as _bridge_non_workbook_partial_reason,
  bridge_operation_result_dicts as _bridge_operation_result_dicts,
  bridge_resolution_dicts as _bridge_resolution_dicts,
  bridge_stringify_period_values as _bridge_stringify_period_values,
  bridge_split_operations as _bridge_split_operations,
  bridge_terminal_analytical_eps as _bridge_terminal_analytical_eps,
  bridge_unresolved_factors as _bridge_unresolved_factors,
  bridge_validate_scenario_propagation as _bridge_validate_scenario_propagation_impl,
  bridge_warning_dicts as _bridge_warning_dicts,
  bridge_workbook_dispatch_failed as _bridge_workbook_dispatch_failed,
  bridge_workbook_outputs as _bridge_workbook_outputs,
  empty_bridge_projection_persistence as _empty_bridge_projection_persistence,
)
from mcp_servers.model_engine.bridge_persistence import (  # noqa: E402
  bridge_confidence as _bridge_confidence,
  bridge_factor_values_by_period as _bridge_factor_values_by_period,
  bridge_normalize_sources as _bridge_normalize_sources,
  bridge_projection_persistence as _bridge_projection_persistence_impl,
  bridge_scenario_entry as _bridge_scenario_entry,
  bridge_source_id_slug as _bridge_source_id_slug,
  bridge_source_provider as _bridge_source_provider,
  bridge_source_refs_by_period as _bridge_source_refs_by_period,
  bridge_source_text as _bridge_source_text,
)
from mcp_servers.model_engine.bridge_runtime import (  # noqa: E402
  bridge_model_bundle as _bridge_model_bundle_impl,
  graph_downstream_ids as _graph_downstream_ids_impl,
)
from mcp_servers.model_engine.modify import (  # noqa: E402
  ModelModifyDeps as _ModelModifyDeps,
  model_modify_handler as _model_modify_handler,
  normalize_modify_operation_payload as _normalize_modify_operation_payload,
)
from mcp_servers.model_engine.override_store import (  # noqa: E402
  current_registry_revision as _current_registry_revision_impl,
  custom_concept_entry as _custom_concept_entry,
  merge_override_fields as _merge_override_fields,
  override_directory as _override_directory_impl,
  remove_override_concept as _remove_override_concept,
  resolve_reconcile_override_path as _resolve_reconcile_override_path_impl,
  ticker_override_lock as _ticker_override_lock_impl,
)
from mcp_servers.model_engine.projections import (  # noqa: E402
  PROJECTION_SCENARIO_DISPLAY_METADATA_KEYS as _PROJECTION_SCENARIO_DISPLAY_METADATA_KEYS,
  PROJECTION_SCENARIOS as _PROJECTION_SCENARIOS,
  delete_projection_entry as _delete_projection_entry,
  get_projection_data as _get_projection_data,
  merge_projection_scenarios as _merge_projection_scenarios,
  normalize_projection_entry_payload as _normalize_projection_entry_payload,
  normalized_projection_scenario_entry as _normalized_projection_scenario_entry,
  normalized_projection_scenario_value as _normalized_projection_scenario_value,
  prune_projection_scenarios as _prune_projection_scenarios,
  projection_entry_to_dict as _projection_entry_to_dict,
  projection_scenario_dump as _projection_scenario_dump,
  projection_scenarios as _projection_scenarios,
  projection_summaries as _projection_summaries,
  scenario_source_skill as _scenario_source_skill,
  scenario_written_at as _scenario_written_at,
)
from mcp_servers.model_engine.reconcile import (  # noqa: E402
  RECONCILE_CANDIDATE_CONCEPTS as _RECONCILE_CANDIDATE_CONCEPTS,
  RECONCILE_DIAGNOSTICS_CHECKED as _RECONCILE_DIAGNOSTICS_CHECKED,
  RECONCILE_SEVERITY_ORDER as _RECONCILE_SEVERITY_ORDER,
  RECONCILE_UNSUPPORTED_SUBTOTALS as _RECONCILE_UNSUPPORTED_SUBTOTALS,
  after_summary as _after_summary_impl,
  best_value_for_target as _best_value_for_target_impl,
  before_summary as _before_summary_impl,
  current_values_from_diagnostic_inputs as _current_values_from_diagnostic_inputs,
  delta_by_year as _delta_by_year,
  diagnostic_group_cleared as _diagnostic_group_cleared_impl,
  entries_from_serialized_report as _entries_from_serialized_report,
  evaluate_reconcile_candidates as _evaluate_reconcile_candidates_impl,
  existing_override_preserved as _existing_override_preserved,
  filing_pointer as _filing_pointer,
  flagged_reconcile_entries as _flagged_reconcile_entries_impl,
  group_payload as _group_payload_impl,
  group_reconcile_entries as _group_reconcile_entries,
  list_metrics as _list_metrics_impl,
  merged_ticker_overrides as _merged_ticker_overrides,
  metric_value_for_concept as _metric_value_for_concept,
  metrics_by_tag_for_concept as _metrics_by_tag_for_concept_impl,
  new_override_field as _new_override_field_impl,
  normalized_reconcile_path as _normalized_reconcile_path,
  normalize_metric_entries as _normalize_metric_entries,
  override_conflict as _override_conflict,
  override_conflict_response as _override_conflict_response_impl,
  reconcile_error as _reconcile_error,
  reconcile_historical_years as _reconcile_historical_years,
  relative_distance as _relative_distance,
  tag_local_part as _tag_local_part,
  target_value_for_candidate as _target_value_for_candidate,
  validate_inputs_field as _validate_inputs_field_impl,
  validate_reconcile_build_kwargs as _validate_reconcile_build_kwargs_impl,
  validate_reconcile_diagnostic_report as _validate_reconcile_diagnostic_report_impl,
  validate_reconcile_file_path_match as _validate_reconcile_file_path_match_impl,
  validate_reconcile_mode as _validate_reconcile_mode_impl,
  worst_severity as _worst_severity_impl,
)
from mcp_servers.model_engine.recovery import (  # noqa: E402
  anchor_recovery as _anchor_recovery,
  bridge_period_coverage_recovery as _bridge_period_coverage_recovery,
  bridge_recovery as _bridge_recovery,
  projection_entry_validation_payload as _projection_entry_validation_payload,
)
from mcp_servers.model_engine.runtime import (  # noqa: E402
  apply_override_directory_env as _apply_override_directory_env_impl,
  make_edgar_fetcher as _make_edgar_fetcher_impl,
  probe_edgar_axis_filter_support as _probe_edgar_axis_filter_support_impl,
  validate_file_path as _validate_file_path_impl,
)
from mcp_servers.model_engine.reconcile_wrappers import (  # noqa: E402
  build_reconcile_wrapper_functions as _build_reconcile_wrapper_functions,
)
from mcp_servers.model_engine.scenario_readiness import (  # noqa: E402
  READINESS_OWNER_MIN_GAP as _READINESS_OWNER_MIN_GAP,
  READINESS_OWNER_MIN_SCORE as _READINESS_OWNER_MIN_SCORE,
  READINESS_OWNER_STRONG_SCORE as _READINESS_OWNER_STRONG_SCORE,
  READINESS_OWNER_TOKEN_NOISE as _READINESS_OWNER_TOKEN_NOISE,
  bridge_inert_anchor_warnings as _bridge_inert_anchor_warnings_impl,
  readiness_owner_anchor_hints as _readiness_owner_anchor_hints_impl,
  readiness_owner_anchor_payload as _readiness_owner_anchor_payload_impl,
  readiness_owner_complete as _readiness_owner_complete,
  readiness_owner_score as _readiness_owner_score,
  readiness_owner_tokens as _readiness_owner_tokens,
)
from mcp_servers.model_engine.server import (  # noqa: E402
  FastMCP,
  create_mcp as _create_mcp,
  main as _run_mcp,
)
from mcp_servers.model_engine import tool_deps as _tool_deps  # noqa: E402
from mcp_servers.model_engine.tools.override import (  # noqa: E402
  MODEL_OVERRIDE_ACTIONS as _MODEL_OVERRIDE_ACTIONS,
  ModelOverrideDeps as _ModelOverrideDeps,
  ModelOverrideToolFunctions as _ModelOverrideToolFunctions,
  build_model_override_tool_functions as _build_model_override_tool_functions,
  concept_required_payload as _concept_required_payload,
  concept_result_payload as _concept_result_payload,
  model_override_get_payload as _model_override_get_payload,
  model_override_handler as _model_override_handler,
  model_override_list_payload as _model_override_list_payload,
  model_override_star_action_error_payload as _model_override_star_action_error_payload,
  model_override_star_get_payload as _model_override_star_get_payload,
  normalize_model_override_action as _normalize_model_override_action,
  normalize_model_override_ticker as _normalize_model_override_ticker,
  register_model_override_tools as _register_model_override_tools,
  ticker_required_payload as _ticker_required_payload,
  unsupported_model_override_action_payload as _unsupported_model_override_action_payload,
)
from mcp_servers.model_engine.tools.bridge import (  # noqa: E402
  BridgeToolFunctions as _BridgeToolFunctions,
  ModelBridgeScenariosDeps as _ModelBridgeScenariosDeps,
  ModelFindScenarioAnchorDeps as _ModelFindScenarioAnchorDeps,
  ModelScenarioTopologyDeps as _ModelScenarioTopologyDeps,
  build_bridge_tool_functions as _build_bridge_tool_functions,
  model_bridge_scenarios_handler as _model_bridge_scenarios_handler,
  model_find_scenario_anchor_handler as _model_find_scenario_anchor_handler,
  model_scenario_topology_handler as _model_scenario_topology_handler,
  register_bridge_tools as _register_bridge_tools,
)
from mcp_servers.model_engine.tools.business_model import (  # noqa: E402
  BusinessModelToolDeps as _BusinessModelToolDeps,
  bridge_kpis_to_business_model_draft_handler as _bridge_kpis_to_business_model_draft_handler,
  business_model_to_overrides_handler as _business_model_to_overrides_handler,
  business_model_validate_handler as _business_model_validate_handler,
  register_business_model_tools as _register_business_model_tools,
)
from mcp_servers.model_engine.tools.comps import (  # noqa: E402
  CompsBuildDeps as _CompsBuildDeps,
  comps_build_handler as _comps_build_handler,
  register_comps_build_tools as _register_comps_build_tools,
)
from mcp_servers.model_engine.tools.model_build import (  # noqa: E402
  ModelBuildDeps as _ModelBuildDeps,
  model_build_handler as _model_build_handler,
  register_model_build_tools as _register_model_build_tools,
)
from mcp_servers.model_engine.tools.reconcile import (  # noqa: E402
  ReconcileSubtotalIntegrityDeps as _ReconcileSubtotalIntegrityDeps,
  ReconcileToolFunctions as _ReconcileToolFunctions,
  build_reconcile_tool_functions as _build_reconcile_tool_functions,
  register_reconcile_tools as _register_reconcile_tools,
  reconcile_subtotal_integrity_handler as _reconcile_subtotal_integrity_handler,
)
from mcp_servers.model_engine.tools.modify import (  # noqa: E402
  ModelModifyToolFunctions as _ModelModifyToolFunctions,
  build_model_modify_tool_functions as _build_model_modify_tool_functions,
  register_model_modify_tools as _register_model_modify_tools,
)
from mcp_servers.model_engine.tools.runtime import (  # noqa: E402
  RuntimeToolFunctions as _RuntimeToolFunctions,
  build_runtime_tool_functions as _build_runtime_tool_functions,
  register_runtime_tools as _register_runtime_tools,
)
from mcp_servers.model_engine.tools.discovery import (  # noqa: E402
  ModelDiscoveryDeps as _ModelDiscoveryDeps,
  model_discover_segments_handler as _model_discover_segments_handler,
  register_model_discovery_tools as _register_model_discovery_tools,
)
from mcp_servers.model_engine.tools.model_semantics import (  # noqa: E402
  ModelSemanticsDeps as _ModelSemanticsDeps,
  model_semantics_handler as _model_semantics_handler,
  register_model_semantics_tools as _register_model_semantics_tools,
)
from mcp_servers.model_engine.tools.read import (  # noqa: E402
  ModelReadDeps as _ModelReadDeps,
  model_drivers_handler as _model_drivers_handler,
  model_find_handler as _model_find_handler,
  model_presentation_compare_handler as _model_presentation_compare_handler,
  model_presentation_fingerprint_handler as _model_presentation_fingerprint_handler,
  model_scenario_handler as _model_scenario_handler,
  model_sensitivity_handler as _model_sensitivity_handler,
  model_summarize_handler as _model_summarize_handler,
  model_valuation_summary_handler as _model_valuation_summary_handler,
  model_values_handler as _model_values_handler,
  register_model_read_tools as _register_model_read_tools,
)
from mcp_servers.model_engine.valuation_comps import (  # noqa: E402
  FMP_EPS_AVG_FIELDS as _FMP_EPS_AVG_FIELDS,
  FMP_EV_EBITDA_FIELDS as _FMP_EV_EBITDA_FIELDS,
  FMP_PE_FIELDS as _FMP_PE_FIELDS,
  FMP_PEG_FIELDS as _FMP_PEG_FIELDS,
  FMP_PRICE_FIELDS as _FMP_PRICE_FIELDS,
  build_valuation_comps_fallback as _build_valuation_comps_fallback_impl,
  comps_build_error_payload as _comps_build_error_payload_impl,
  comps_build_recovery as _comps_build_recovery,
  coerce_float as _coerce_float,
  extract_peer_symbols as _extract_peer_symbols,
  fetch_fmp_records as _fetch_fmp_records,
  financials_records as _financials_records,
  first_numeric as _first_numeric,
  fmp_frame_records as _fmp_frame_records,
  fy1_eps_avg as _fy1_eps_avg,
  latest_numeric as _latest_numeric,
  parse_record_date as _parse_record_date,
  quote_prices_by_symbol as _quote_prices_by_symbol,
  record_symbol as _record_symbol,
  trailing_pe_range as _trailing_pe_range,
  valuation_comp_entry_from_fmp as _valuation_comp_entry_from_fmp,
)
from mcp_servers.model_engine.valuation_summary import (  # noqa: E402
  VALUATION_READBACK_BY_ID as _VALUATION_READBACK_BY_ID,
  VALUATION_READBACK_FIELD_KEYS as _VALUATION_READBACK_FIELD_KEYS,
  VALUATION_READBACK_ITEM_IDS as _VALUATION_READBACK_ITEM_IDS,
  VALUATION_READBACK_ITEM_TO_FIELD as _VALUATION_READBACK_ITEM_TO_FIELD,
  VALUATION_READBACK_ITEMS as _VALUATION_READBACK_ITEMS,
  enrich_valuation_rows as _enrich_valuation_rows,
  group_valuation_rows as _group_valuation_rows,
  latest_numeric_value as _latest_numeric_value,
  nearly_equal as _nearly_equal,
  policy_number as _policy_number,
  ratio_metric as _ratio_metric,
  row_latest_value as _row_latest_value,
  sort_period_key as _sort_period_key,
  valuation_derived_metrics as _valuation_derived_metrics,
  valuation_input_readiness as _valuation_input_readiness,
  valuation_policy_summary as _valuation_policy_summary,
)

_MODEL_READ_HANDLER_COMPAT_ALIASES = (
  _model_drivers_handler,
  _model_find_handler,
  _model_presentation_compare_handler,
  _model_presentation_fingerprint_handler,
  _model_scenario_handler,
  _model_sensitivity_handler,
  _model_summarize_handler,
  _model_valuation_summary_handler,
  _model_values_handler,
)

_MODEL_ENGINE_OVERRIDES_DIR_ENV = schema_overrides.MODEL_ENGINE_OVERRIDES_DIR_ENV
_make_edgar_financials_fetcher = make_edgar_financials_fetcher


def _apply_override_directory_env() -> Path | None:
  return _apply_override_directory_env_impl(
    overrides_dir_env=_MODEL_ENGINE_OVERRIDES_DIR_ENV,
    schema_overrides_module=schema_overrides,
    os_module=os,
    path_cls=Path,
  )


_APPLIED_MODEL_ENGINE_OVERRIDES_DIR = _apply_override_directory_env()

sys.stdout = _real_stdout

mcp = _create_mcp()


def _make_edgar_fetcher():
  return _make_edgar_fetcher_impl(
    probe_axis_filter_support_fn=_probe_edgar_axis_filter_support,
    os_module=os,
    urllib_module=urllib,
    json_module=json,
    logging_module=logging,
  )


def _probe_edgar_axis_filter_support(base_url: str, api_key: str) -> bool:
  return _probe_edgar_axis_filter_support_impl(
    base_url,
    api_key,
    urllib_module=urllib,
    json_module=json,
    logging_module=logging,
  )


def _validate_file_path(file_path: str) -> str:
  """Expand ~ and verify the file exists. Returns resolved path."""
  return _validate_file_path_impl(file_path, os_path=os.path)


def _model_build_error_payload(exc: Exception, *, model_build_id: str) -> dict:
  return _model_build_error_payload_impl(
    exc,
    model_build_id=model_build_id,
    model_tool_error_payload_fn=_model_tool_error_payload,
    safe_pydantic_errors_fn=_safe_pydantic_errors,
  )


def _axis_priority(axis: Optional[str]) -> Optional[int]:
  return _axis_priority_impl(axis, axes_priority=_SEGMENT_AXES_PRIORITY)


_MODEL_BUILD_SIGNATURE = None


_reconcile_wrapper_functions = _build_reconcile_wrapper_functions(
  parent_namespace=lambda: globals(),
)
_validate_reconcile_mode = _reconcile_wrapper_functions.validate_reconcile_mode
_validate_reconcile_build_kwargs = (
  _reconcile_wrapper_functions.validate_reconcile_build_kwargs
)
_validate_reconcile_file_path_match = (
  _reconcile_wrapper_functions.validate_reconcile_file_path_match
)
_validate_inputs_field = _reconcile_wrapper_functions.validate_inputs_field
_validate_reconcile_diagnostic_report = (
  _reconcile_wrapper_functions.validate_reconcile_diagnostic_report
)
_flagged_reconcile_entries = _reconcile_wrapper_functions.flagged_reconcile_entries
_group_payload = _reconcile_wrapper_functions.group_payload
_worst_severity = _reconcile_wrapper_functions.worst_severity
_before_summary = _reconcile_wrapper_functions.before_summary
_after_summary = _reconcile_wrapper_functions.after_summary
_diagnostic_group_cleared = _reconcile_wrapper_functions.diagnostic_group_cleared
_list_metrics = _reconcile_wrapper_functions.list_metrics
_metrics_by_tag_for_concept = _reconcile_wrapper_functions.metrics_by_tag_for_concept
_best_value_for_target = _reconcile_wrapper_functions.best_value_for_target
_evaluate_reconcile_candidates = (
  _reconcile_wrapper_functions.evaluate_reconcile_candidates
)
_current_registry_revision = _reconcile_wrapper_functions.current_registry_revision
_override_directory = _reconcile_wrapper_functions.override_directory
_resolve_reconcile_override_path = (
  _reconcile_wrapper_functions.resolve_reconcile_override_path
)
_new_override_field = _reconcile_wrapper_functions.new_override_field
_override_conflict_response = _reconcile_wrapper_functions.override_conflict_response
_ticker_override_lock = _reconcile_wrapper_functions.ticker_override_lock


def _build_valuation_comps_fallback(
  ticker: str,
  financials: dict | None = None,
  *,
  max_peers: int = 6,
  fetcher: Any | None = None,
) -> dict[str, Any] | None:
  if fetcher is None and fmp is None:
    raise RuntimeError(
      "FMP fallback data requires the optional fmp-mcp package. "
      "Install financial-model-engine[fmp] or pass an explicit fetcher."
    )
  return _build_valuation_comps_fallback_impl(
    ticker,
    financials,
    max_peers=max_peers,
    fetcher=fetcher or fmp.fetch,
  )


def _model_handle_token_payload(
  *,
  file_path: str,
  historical_cutoff_year: int | None,
  issued_by: str,
) -> dict[str, Any]:
  return _model_handle_token_payload_impl(
    file_path=file_path,
    historical_cutoff_year=historical_cutoff_year,
    issued_by=issued_by,
    load_handle=_load_handle,
  )


def _model_read_deps() -> _ModelReadDeps:
  return _ModelReadDeps(
    validate_file_path=_validate_file_path,
    model_tool_error_payload=_model_tool_error_payload,
    summarize=_summarize,
    values=_values,
    enrich_valuation_rows=_enrich_valuation_rows,
    group_valuation_rows=_group_valuation_rows,
    valuation_derived_metrics=_valuation_derived_metrics,
    valuation_policy_summary=_valuation_policy_summary,
    valuation_input_readiness=_valuation_input_readiness,
    valuation_readback_items=_VALUATION_READBACK_ITEMS,
    valuation_readback_item_ids=_VALUATION_READBACK_ITEM_IDS,
    workbook_presentation_fingerprint=workbook_presentation_fingerprint,
    workbook_presentation_gap=workbook_presentation_gap,
    find=_find,
    coerce_json_list_arg=_coerce_json_list_arg,
    drivers=_drivers,
    sensitivity=_sensitivity,
    coerce_json_dict_arg=_coerce_json_dict_arg,
    invalid_override_period_error=_invalid_override_period_error,
    invalid_override_value_error=_invalid_override_value_error,
    scenario=_scenario,
    model_handle_token_payload=_model_handle_token_payload,
  )


def _model_discovery_deps() -> _ModelDiscoveryDeps:
  return _ModelDiscoveryDeps(
    discover_all_axes=_discover_all_axes,
    make_edgar_financials_fetcher=_make_edgar_financials_fetcher,
    axis_priority=_axis_priority,
    segment_revenue_observation_list=_segment_revenue_observation_list,
    serialize_year_values=_serialize_year_values,
  )


def _model_semantics_deps() -> _ModelSemanticsDeps:
  from research.model_state import resolve_current_model_ref
  from research.repository import get_repository_factory

  return _ModelSemanticsDeps(
    load_ticker_overrides=load_ticker_overrides,
    ticker_overrides_cls=TickerOverrides,
    model_semantics_cls=ModelSemantics,
    derive_ticker_overrides_schema_version=derive_ticker_overrides_schema_version,
    get_repository_factory=get_repository_factory,
    resolve_current_model_ref=resolve_current_model_ref,
    current_model_ref_cls=CurrentModelRef,
    valid_workspace_user_id=_valid_workspace_user_id,
  )


def _business_model_tool_deps() -> _BusinessModelToolDeps:
  return _BusinessModelToolDeps(
    business_model_cls=BusinessModel,
    validation_error_type=ValidationError,
    validate_schema=business_model_validate_schema,
    load_business_model_from_path=_load_business_model_from_path,
    resolve_business_model_path=_resolve_business_model_path,
    coerce_json_dict_arg=_coerce_json_dict_arg,
    coerce_json_list_arg=_coerce_json_list_arg,
    make_edgar_financials_fetcher=make_edgar_financials_fetcher,
    make_edgar_financials_fetcher_override=_make_edgar_financials_fetcher,
    discover_all_axes=_discover_all_axes,
    safe_pydantic_errors=_safe_pydantic_errors,
    model_tool_error_payload=_model_tool_error_payload,
    bridge_report_cls=BridgeReport,
    business_model_to_overrides=_business_model_to_overrides,
    serialize_overrides_write_report=_serialize_overrides_write_report,
    bridge_kpi_catalog=_bridge_kpi_catalog,
  )

_read_tool_functions = _register_model_read_tools(
  mcp,
  parent_namespace=lambda: globals(),
  tool_names=("model_summarize", "model_valuation_summary"),
)
model_summarize = _read_tool_functions.model_summarize
model_valuation_summary = _read_tool_functions.model_valuation_summary


_model_semantics_tool_functions = _register_model_semantics_tools(
  mcp,
  parent_namespace=lambda: globals(),
)
model_semantics = _model_semantics_tool_functions.model_semantics


_read_tool_functions = _register_model_read_tools(
  mcp,
  parent_namespace=lambda: globals(),
  tool_names=(
    "model_presentation_fingerprint",
    "model_presentation_compare",
    "model_find",
    "model_values",
    "model_drivers",
    "model_sensitivity",
  ),
  functions=_read_tool_functions,
)
model_presentation_fingerprint = _read_tool_functions.model_presentation_fingerprint
model_presentation_compare = _read_tool_functions.model_presentation_compare
model_find = _read_tool_functions.model_find
model_values = _read_tool_functions.model_values
model_drivers = _read_tool_functions.model_drivers
model_sensitivity = _read_tool_functions.model_sensitivity

_model_discovery_tool_functions = _register_model_discovery_tools(
  mcp,
  parent_namespace=lambda: globals(),
)
model_discover_segments = _model_discovery_tool_functions.model_discover_segments


_read_tool_functions = _register_model_read_tools(
  mcp,
  parent_namespace=lambda: globals(),
  tool_names=("model_scenario",),
  functions=_read_tool_functions,
)
model_scenario = _read_tool_functions.model_scenario

_business_model_tool_functions = _register_business_model_tools(
  mcp,
  parent_namespace=lambda: globals(),
  tool_names=("business_model_validate", "business_model_to_overrides"),
)
business_model_validate = _business_model_tool_functions.business_model_validate
business_model_to_overrides = _business_model_tool_functions.business_model_to_overrides


_model_build_deps = _tool_deps.build_model_build_deps_function(
  parent_namespace=lambda: globals(),
)


_model_build_tool_functions = _register_model_build_tools(
  mcp,
  parent_namespace=lambda: globals(),
)
model_build = _model_build_tool_functions.model_build


_MODEL_BUILD_SIGNATURE = inspect.signature(model_build)


def _comps_build_error_payload(exc: Exception, *, comps_build_id: str) -> dict:
  return _comps_build_error_payload_impl(
    exc,
    comps_build_id=comps_build_id,
    validation_error_type=ValidationError,
    safe_pydantic_errors_fn=_safe_pydantic_errors,
  )


_comps_build_deps = _tool_deps.build_comps_build_deps_function(
  parent_namespace=lambda: globals(),
)


_comps_build_tool_functions = _register_comps_build_tools(
  mcp,
  parent_namespace=lambda: globals(),
)
comps_build = _comps_build_tool_functions.comps_build


_reconcile_subtotal_integrity_deps = _tool_deps.build_reconcile_subtotal_integrity_deps_function(
  parent_namespace=lambda: globals(),
)


_reconcile_tool_functions = _register_reconcile_tools(
  mcp,
  parent_namespace=lambda: globals(),
)
reconcile_subtotal_integrity = (
  _reconcile_tool_functions.reconcile_subtotal_integrity
)


_model_override_deps = _tool_deps.build_model_override_deps_function(
  parent_namespace=lambda: globals(),
)


_model_modify_deps = _tool_deps.build_model_modify_deps_function(
  parent_namespace=lambda: globals(),
)


_model_override_tool_functions = _register_model_override_tools(
  mcp,
  parent_namespace=lambda: globals(),
)
model_override = _model_override_tool_functions.model_override

_model_modify_tool_functions = _register_model_modify_tools(
  mcp,
  parent_namespace=lambda: globals(),
)
model_modify = _model_modify_tool_functions.model_modify


_bridge_private_functions = _tool_deps.build_bridge_private_functions(
  parent_namespace=lambda: globals(),
)
_graph_downstream_ids = _bridge_private_functions.graph_downstream_ids
_bridge_inert_anchor_warnings = _bridge_private_functions.bridge_inert_anchor_warnings
_bridge_validate_scenario_propagation = (
  _bridge_private_functions.bridge_validate_scenario_propagation
)
_bridge_projection_persistence = _bridge_private_functions.bridge_projection_persistence
_bridge_model_bundle = _bridge_private_functions.bridge_model_bundle
_readiness_owner_anchor_payload = _bridge_private_functions.readiness_owner_anchor_payload
_readiness_owner_anchor_hints = _bridge_private_functions.readiness_owner_anchor_hints

_model_find_scenario_anchor_deps = _tool_deps.build_model_find_scenario_anchor_deps_function(
  parent_namespace=lambda: globals(),
)

_model_scenario_topology_deps = _tool_deps.build_model_scenario_topology_deps_function(
  parent_namespace=lambda: globals(),
)

_model_bridge_scenarios_deps = _tool_deps.build_model_bridge_scenarios_deps_function(
  parent_namespace=lambda: globals(),
)


_bridge_tool_functions = _register_bridge_tools(
  mcp,
  parent_namespace=lambda: globals(),
)
model_scenario_topology = _bridge_tool_functions.model_scenario_topology
model_find_scenario_anchor = _bridge_tool_functions.model_find_scenario_anchor
model_bridge_scenarios = _bridge_tool_functions.model_bridge_scenarios


_runtime_tool_functions = _register_runtime_tools(
  mcp,
  parent_namespace=lambda: globals(),
  tool_names=("annotate_model_with_research",),
)
annotate_model_with_research = _runtime_tool_functions.annotate_model_with_research


_business_model_tool_functions = _register_business_model_tools(
  mcp,
  parent_namespace=lambda: globals(),
  tool_names=("bridge_kpis_to_business_model_draft",),
  functions=_business_model_tool_functions,
)
bridge_kpis_to_business_model_draft = (
  _business_model_tool_functions.bridge_kpis_to_business_model_draft
)


_runtime_tool_functions = _register_runtime_tools(
  mcp,
  parent_namespace=lambda: globals(),
  tool_names=("model_clear_cache",),
  functions=_runtime_tool_functions,
)
model_clear_cache = _runtime_tool_functions.model_clear_cache


def main() -> None:
  _run_mcp(mcp)


if __name__ == "__main__":
  main()
