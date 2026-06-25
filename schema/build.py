"""Build populated financial models from the bundled generic SIA template."""

from __future__ import annotations

import concurrent.futures  # noqa: F401 - compatibility facade for schema.build imports
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Literal as Literal, Optional  # noqa: F401 - compatibility facade for moved type annotations

from api.credentials import (
    get_equivalence_flag as get_equivalence_flag,  # noqa: F401 - compatibility facade
    is_analyst_cron_mode as is_analyst_cron_mode,  # noqa: F401 - compatibility facade
)

from .build_diagnostics import (
    BS_SECTIONS,  # noqa: F401 - compatibility facade for moved helpers
    DiagnosticReport,
    _build_taxonomy_tag_index,  # noqa: F401 - compatibility facade for moved helpers
    _effective_section_members,  # noqa: F401 - compatibility facade for moved helpers
    _select_non_overlapping_presentation_children,  # noqa: F401 - compatibility facade for moved helpers
    _write_diagnostic_log,
    run_build_diagnostic,
)
from .build_formula_refs import (
    _all_refs_same_period as _all_refs_same_period,
    _available_periods as _available_periods,
    _collect_ref_offsets as _collect_ref_offsets,
    _extract_ref_ids as _extract_ref_ids,
    _extract_single_ref as _extract_single_ref,
    _formula_period_is_valid as _formula_period_is_valid,
    _safe_int as _safe_int,
    _spec_for_period_validation as _spec_for_period_validation,
)
from .build_time_structure import (
    ROLLING_HEADER_EXCLUSIONS as ROLLING_HEADER_EXCLUSIONS,
    _is_self_carry_forward_projection as _is_self_carry_forward_projection,
    remap_time_structure as remap_time_structure,
)
from . import build_population as _build_population_module
from .build_population import (
    _TREASURY_STOCK_CONCEPT_ID as _TREASURY_STOCK_CONCEPT_ID,
    _TREASURY_STOCK_ITEM_ID as _TREASURY_STOCK_ITEM_ID,
    _extract_first_numeric as _extract_first_numeric,
    _populate_routed as _populate_routed,
    _populate_treasury_stock_row_from_fmp as _populate_treasury_stock_row_from_fmp,
    _populate_treasury_stock_row_from_fmp_result as _populate_treasury_stock_row_from_fmp_result,
    populate_from_edgar as populate_from_edgar,
    populate_from_fmp as populate_from_fmp,
    populate_historicals as populate_historicals,
)
from .build_mbc_entry import (
    _apply_mbc_seeds as _apply_mbc_seeds,
    _is_business_model_rate_driver_key as _is_business_model_rate_driver_key,
    _is_default_historical_sources as _is_default_historical_sources,
    _segment_profile_from_snapshot as _segment_profile_from_snapshot,
    build_model_from_mbc as build_model_from_mbc,
)
from .build_formula_eval import (
    _MAX_REF_CHAIN_DEPTH as _MAX_REF_CHAIN_DEPTH,
    _constant_override_value as _constant_override_value,
    _evaluate_expr_simple as _evaluate_expr_simple,
    _evaluate_formula_simple as _evaluate_formula_simple,
    _lookup_formula_value as _lookup_formula_value,
)
from .build_real_data import (
    _item_has_direct_real_data as _item_has_direct_real_data,
    _item_has_real_data as _item_has_real_data,
)
from .build_formula_first import (
    _FORMULA_FIRST_EXCLUDED_ITEM_IDS as _FORMULA_FIRST_EXCLUDED_ITEM_IDS,
    _SYNTHETIC_FAST_PATH_TYPES as _SYNTHETIC_FAST_PATH_TYPES,
    _active_formula_first_periods as _active_formula_first_periods,
    _compute_derivable_periods as _compute_derivable_periods,
    _formula_first_node_periods as _formula_first_node_periods,
    _reconcile_override as _reconcile_override,
    apply_formula_first as apply_formula_first,
)
from .build_presentation_residuals import (
    _NON_CURRENT_ASSET_CATCH_ALL_ITEM_ID as _NON_CURRENT_ASSET_CATCH_ALL_ITEM_ID,
    _PRESENTATION_RESIDUAL_NOTE as _PRESENTATION_RESIDUAL_NOTE,
    _apply_presentation_catch_all_residuals as _apply_presentation_catch_all_residuals,
    _bs_section_subline_sum as _bs_section_subline_sum,
    _bs_section_total_value as _bs_section_total_value,
    _residualize_bs_section_catch_all as _residualize_bs_section_catch_all,
    _select_bs_section_presentation_basis as _select_bs_section_presentation_basis,
    _set_residualized_value as _set_residualized_value,
)
from .build_company_info import (
    _COMPANY_NAME_TOKEN as _COMPANY_NAME_TOKEN,
    _MONTH_NUMBERS as _MONTH_NUMBERS,
    _YEAR_HEADER_ID as _YEAR_HEADER_ID,
    _fiscal_year_end_date as _fiscal_year_end_date,
    _gregorian_to_excel_serial as _gregorian_to_excel_serial,
    update_company_info as update_company_info,
)
from .build_model_items import _iter_items as _iter_items
from .build_value_writers import (
    _set_constant_override as _set_constant_override,
    _set_imported_value as _set_imported_value,
)
from .build_valuation_comps import (
    ValuationCompEntry as ValuationCompEntry,
    ValuationCompsPayload as ValuationCompsPayload,
    _VALUATION_COMP_BLANK_FORMULA as _VALUATION_COMP_BLANK_FORMULA,
    _VALUATION_COMP_CLEAR_COLUMNS as _VALUATION_COMP_CLEAR_COLUMNS,
    _VALUATION_COMP_PEER_ROLES as _VALUATION_COMP_PEER_ROLES,
    _VALUATION_COMP_PE_CLEAR_ROWS as _VALUATION_COMP_PE_CLEAR_ROWS,
    _VALUATION_COMP_PEG_CLEAR_ROWS as _VALUATION_COMP_PEG_CLEAR_ROWS,
    _VALUATION_COMP_RENDERED_VALUE_KEYS as _VALUATION_COMP_RENDERED_VALUE_KEYS,
    _VALUATION_COMP_ROLES as _VALUATION_COMP_ROLES,
    _append_valuation_comp_clear_writes as _append_valuation_comp_clear_writes,
    _clear_valuation_comp_peer_rows as _clear_valuation_comp_peer_rows,
    _set_fixed_numeric_value as _set_fixed_numeric_value,
    _set_fixed_text_formula as _set_fixed_text_formula,
    _valuation_comp_entries as _valuation_comp_entries,
    _valuation_comp_entry_has_rendered_payload as _valuation_comp_entry_has_rendered_payload,
    _valuation_comp_periods as _valuation_comp_periods,
    _valuation_comps_provenance as _valuation_comps_provenance,
    _write_valuation_comp_row as _write_valuation_comp_row,
    populate_valuation_comps as populate_valuation_comps,
)
from .build_valuation_inputs import (
    _BLOOMBERG_STYLE_BETA_WEIGHT as _BLOOMBERG_STYLE_BETA_WEIGHT,
    _MARKET_BETA_ANCHOR as _MARKET_BETA_ANCHOR,
    _VALUATION_DEFAULTS as _VALUATION_DEFAULTS,
    _VALUATION_ECONOMIC_INPUTS as _VALUATION_ECONOMIC_INPUTS,
    _VALUATION_PLACEHOLDER_VALUES as _VALUATION_PLACEHOLDER_VALUES,
    _VALUATION_REQUIRED_INPUTS as _VALUATION_REQUIRED_INPUTS,
    _VALUATION_TERMINAL_INPUTS as _VALUATION_TERMINAL_INPUTS,
    _VALUATION_TERMINAL_INPUT_PREFIXES as _VALUATION_TERMINAL_INPUT_PREFIXES,
    _adjust_raw_beta as _adjust_raw_beta,
    _clear_valuation_input_values as _clear_valuation_input_values,
    _derive_credit_spread as _derive_credit_spread,
    _extract_first_numeric_with_source as _extract_first_numeric_with_source,
    _read_value as _read_value,
    _set_valuation_input_value as _set_valuation_input_value,
    _stored_valuation_inputs as _stored_valuation_inputs,
    populate_valuation_inputs as populate_valuation_inputs,
)
from .valuation_schema_invariant import assert_valuation_template_schema
from . import build_validation_inputs as _build_validation_inputs_module
from .build_validation_inputs import (
    _fetch_validation_edgar_concept_buffer as _fetch_validation_edgar_concept_buffer,
    _fetch_validation_fmp_concept_buffer as _fetch_validation_fmp_concept_buffer,
    _make_validation_input as _make_validation_input,
    _validation_opt_in_concepts as _validation_opt_in_concepts,
)
from .build_types import (
    BuildResult as BuildResult,
    EdgarConceptFetchResult as EdgarConceptFetchResult,
    EdgarFetcher as EdgarFetcher,
    FmpConceptFetchResult as FmpConceptFetchResult,
    PopulateStats as PopulateStats,
    ServedByBreakdown as ServedByBreakdown,
    SourceResolutionEntry as SourceResolutionEntry,
)
from . import build_routed_historicals as _build_routed_historicals_module
from .build_routed_historicals import (
    _write_routed_historicals as _write_routed_historicals,
)
from .build_edgar_series import (
    ParsedEdgarSeriesResult as ParsedEdgarSeriesResult,
    _parse_edgar_series as _parse_edgar_series,
    _parse_edgar_series_result as _parse_edgar_series_result,
    _parse_edgar_series_source_result as _parse_edgar_series_source_result,
)
from .build_edgar_warm import (
    EdgarWarmResult as EdgarWarmResult,
    _edgar_warm_message as _edgar_warm_message,
    warm_edgar_cache as warm_edgar_cache,
)
from . import build_concept_buffers as _build_concept_buffers_module
from .build_concept_buffers import (
    _all_edgar_concepts_failed_message as _all_edgar_concepts_failed_message,
    _concept_can_fetch_edgar as _concept_can_fetch_edgar,
    _empty_edgar_concept_result as _empty_edgar_concept_result,
    _empty_fmp_concept_result as _empty_fmp_concept_result,
    _fetch_edgar_concept_buffer as _fetch_edgar_concept_buffer,
    _fetch_fmp_concept_buffer as _fetch_fmp_concept_buffer,
)
from . import build_mbc_seeds as _build_mbc_seeds_module
from .build_mbc_seeds import (
    _SEGMENT_REVENUE_KPI_SOURCE_TAGS as _SEGMENT_REVENUE_KPI_SOURCE_TAGS,
    _bm_revenue_share_inline_values as _bm_revenue_share_inline_values,
    _bm_segment_snapshot_inline_values as _bm_segment_snapshot_inline_values,
    _fmp_total_revenue_by_year as _fmp_total_revenue_by_year,
    _is_segment_revenue_kpi_node as _is_segment_revenue_kpi_node,
    _iter_business_model_nodes as _iter_business_model_nodes,
)
from . import build_projection_seeds as _build_projection_seeds_module
from .build_projection_seeds import (
    OrphanedProjection as OrphanedProjection,
    SeedProjectionWarning as SeedProjectionWarning,
    SeedProjectionsResult as SeedProjectionsResult,
    _SCENARIO_ORDERING_EPS as _SCENARIO_ORDERING_EPS,
    _extract_last_provenance as _extract_last_provenance,
    _item_seeded_values_by_period as _item_seeded_values_by_period,
    _projection_base_values_by_period as _projection_base_values_by_period,
    _projection_entry_value_for_model as _projection_entry_value_for_model,
    _projection_entry_values_by_period as _projection_entry_values_by_period,
    _projection_owner_base_entry_for_case_row as _projection_owner_base_entry_for_case_row,
    _projection_provenance_to_dict as _projection_provenance_to_dict,
    _resolve_scenario_case_row as _resolve_scenario_case_row,
    _resolve_scenario_flex_row as _resolve_scenario_flex_row,
    _scenario_ordering_violations_by_case as _scenario_ordering_violations_by_case,
    _schema_version_seen as _schema_version_seen,
    _seed_projections_from_overrides as _seed_projections_from_overrides,
)
from . import build_total_equity as _build_total_equity_module
from .build_total_equity import (
    _maybe_total_equity_derived_fallback as _maybe_total_equity_derived_fallback,
    _total_equity_derived_fallback as _total_equity_derived_fallback,
)
from .build_zero_missing_routing import (
    _zero_missing_edgar_fallback_routing as _zero_missing_edgar_fallback_routing,
)
from .build_cash_historicals import (
    _CASH_BEGINNING_ITEM_ID as _CASH_BEGINNING_ITEM_ID,
    _CASH_END_ITEM_ID as _CASH_END_ITEM_ID,
    _NET_CHANGE_IN_CASH_ITEM_ID as _NET_CHANGE_IN_CASH_ITEM_ID,
    _has_existing_imported_historicals as _has_existing_imported_historicals,
    _seed_cash_beginning_of_period as _seed_cash_beginning_of_period,
)
from .build_fmp_quality import (
    _FMP_QUALITY_ABS_DELTA_M as _FMP_QUALITY_ABS_DELTA_M,
    _FMP_QUALITY_BUCKET_FIELDS as _FMP_QUALITY_BUCKET_FIELDS,
    _FMP_QUALITY_INFORMATIONAL_BUCKET_CONCEPTS as _FMP_QUALITY_INFORMATIONAL_BUCKET_CONCEPTS,
    _FMP_QUALITY_YOY_RATIO_THRESHOLD as _FMP_QUALITY_YOY_RATIO_THRESHOLD,
    _fmp_quality_warnings_from_observations as _fmp_quality_warnings_from_observations,
    _record_fmp_quality_observation as _record_fmp_quality_observation,
)
from .build_fmp_values import (
    _build_fmp_lookup as _build_fmp_lookup,
    _combined_sga_residual_fallback as _combined_sga_residual_fallback,
    _fmp_fallback_value_for_concept as _fmp_fallback_value_for_concept,
    _fmp_float_value as _fmp_float_value,
    _is_fmp_zero_value as _is_fmp_zero_value,
    _make_fmp_provenance as _make_fmp_provenance,
    _operating_expenses_residual_fallback as _operating_expenses_residual_fallback,
    _prefer_record as _prefer_record,
    _raw_fmp_value_for_concept as _raw_fmp_value_for_concept,
    _record_year as _record_year,
    _scale_fmp_value as _scale_fmp_value,
)
from .build_reported_periods import (
    _collect_edgar_reported_period_ends as _collect_edgar_reported_period_ends,
    _collect_fmp_reported_period_ends_from_lookup as _collect_fmp_reported_period_ends_from_lookup,
    _collect_routed_reported_period_ends as _collect_routed_reported_period_ends,
    _entry_reported_period_end as _entry_reported_period_end,
    _record_reported_period_end as _record_reported_period_end,
    _refresh_period_metadata as _refresh_period_metadata,
    _reported_period_end_value as _reported_period_end_value,
)
from .build_non_gaap_addbacks import (
    _PROJECTED_DA_BASE_ID as _PROJECTED_DA_BASE_ID,
    _PROJECTED_DA_RATE_ID as _PROJECTED_DA_RATE_ID,
    _PROJECTED_DA_TOTAL_ID as _PROJECTED_DA_TOTAL_ID,
    _PROJECTED_DEPRECIATION_ID as _PROJECTED_DEPRECIATION_ID,
    _PROJECTED_SBC_BASE_IDS as _PROJECTED_SBC_BASE_IDS,
    _PROJECTED_SBC_COMPONENT_IDS as _PROJECTED_SBC_COMPONENT_IDS,
    _PROJECTED_SBC_RATE_IDS as _PROJECTED_SBC_RATE_IDS,
    _PROJECTED_SBC_TOTAL_ID as _PROJECTED_SBC_TOTAL_ID,
    _REVENUE_ITEM_ID as _REVENUE_ITEM_ID,
    _coerce_optional_float as _coerce_optional_float,
    _computed_model_values as _computed_model_values,
    _computed_value as _computed_value,
    _latest_ratio_from_computed_values as _latest_ratio_from_computed_values,
    _missing_projection_periods as _missing_projection_periods,
    _seed_projected_non_gaap_addbacks as _seed_projected_non_gaap_addbacks,
    _set_projection_input_values as _set_projection_input_values,
)
from .build_scenarios import (
    _SCENARIO_CASE_PATTERNS as _SCENARIO_CASE_PATTERNS,
    _SCENARIO_CASE_SELECTOR as _SCENARIO_CASE_SELECTOR,
    _SCENARIO_EPS_ITEM_RE as _SCENARIO_EPS_ITEM_RE,
    _SCENARIO_EPS_LIMIT as _SCENARIO_EPS_LIMIT,
    _build_scenario_overrides as _build_scenario_overrides,
    _downstream_item_ids as _downstream_item_ids,
    _find_item_location as _find_item_location,
    _find_scenario_value_row as _find_scenario_value_row,
    _finite_projection_values as _finite_projection_values,
    _normalize_scenario_label as _normalize_scenario_label,
    _offset_anchor_ids as _offset_anchor_ids,
    _populate_scenario_eps as _populate_scenario_eps,
    _populate_scenario_inputs as _populate_scenario_inputs,
    _projected_formula_refs_any as _projected_formula_refs_any,
    _scenario_compute_inputs as _scenario_compute_inputs,
    _select_scenario_output_item_id as _select_scenario_output_item_id,
    compute_scenario_eps as compute_scenario_eps,
    compute_scenario_outputs as compute_scenario_outputs,
)
from .build_semantic_rows import (
    SemanticRowsResult as SemanticRowsResult,
    _BS_TOTAL_FORMULA_BY_SECTION as _BS_TOTAL_FORMULA_BY_SECTION,
    _CUSTOM_CONCEPT_CARRY_FORWARD_STRATEGIES as _CUSTOM_CONCEPT_CARRY_FORWARD_STRATEGIES,
    _add_formula_ref_after as _add_formula_ref_after,
    _apply_semantic_cash_flow_linkages as _apply_semantic_cash_flow_linkages,
    _apply_semantic_row_metadata as _apply_semantic_row_metadata,
    _apply_semantic_valuation_linkages as _apply_semantic_valuation_linkages,
    _bind_or_insert_cf_linkage_row as _bind_or_insert_cf_linkage_row,
    _bind_or_insert_semantic_row as _bind_or_insert_semantic_row,
    _create_semantic_line_item as _create_semantic_line_item,
    _formula_contains_ref as _formula_contains_ref,
    _insert_line_item_after as _insert_line_item_after,
    _insert_ref_after_in_obj as _insert_ref_after_in_obj,
    _item_has_any_real_historical_data as _item_has_any_real_historical_data,
    _locate_model_item as _locate_model_item,
    _materialize_client_funds_subtotal_bridges as _materialize_client_funds_subtotal_bridges,
    _materialize_semantic_rows as _materialize_semantic_rows,
    _periods_for_delta_formula as _periods_for_delta_formula,
    _semantic_append_note as _semantic_append_note,
    _semantic_forecast_type as _semantic_forecast_type,
    _semantic_role as _semantic_role,
    _semantic_row_stable_id as _semantic_row_stable_id,
    _semantic_slug as _semantic_slug,
    _semantic_source_custom_entry as _semantic_source_custom_entry,
    _semantic_target_is_empty as _semantic_target_is_empty,
    _wire_semantic_bs_row as _wire_semantic_bs_row,
)
from .build_custom_concepts import (
    _AXIS_QNAME_RE as _AXIS_QNAME_RE,
    _DSM_EXCLUDED_FROM_OVERRIDE_ENTRY as _DSM_EXCLUDED_FROM_OVERRIDE_ENTRY,
    _DSM_FIELD_NAMES as _DSM_FIELD_NAMES,
    _EDGAR_AXIS_FAMILY_LABELS as _EDGAR_AXIS_FAMILY_LABELS,
    _apply_custom_concept_target_metadata as _apply_custom_concept_target_metadata,
    _populate_custom_concepts as _populate_custom_concepts,
    _validate_axis_key as _validate_axis_key,
    _validate_inline_values as _validate_inline_values,
)
from . import build_edgar_fetch as _build_edgar_fetch_module
from .build_edgar_fetch import (
    _SEEN_DEPRECATED_REGISTRY_GROUPS as _SEEN_DEPRECATED_REGISTRY_GROUPS,
    _call_edgar_metric_fetcher as _call_edgar_metric_fetcher,
    _edgar_fetch_error_message as _edgar_fetch_error_message,
    _edgar_negate_enabled as _edgar_negate_enabled,
    _edgar_tag_lookup_candidates as _edgar_tag_lookup_candidates,
    _equivalent_tag_validated_by_registry as _equivalent_tag_validated_by_registry,
    _fetch_dimensional_edgar_concept as _fetch_dimensional_edgar_concept,
    _fetch_edgar_concept as _fetch_edgar_concept,
    _fetch_edgar_concept_result as _fetch_edgar_concept_result,
    _fetch_legacy_edgar_concept as _fetch_legacy_edgar_concept,
    _fetch_via_registry as _fetch_via_registry,
    _log_deprecated_registry_group_once as _log_deprecated_registry_group_once,
    _metric_tag_matches_group as _metric_tag_matches_group,
    _registry_equivalence_value_conflict as _registry_equivalence_value_conflict,
    _registry_failed_result as _registry_failed_result,
    _requested_years_for_fetch as _requested_years_for_fetch,
    _resolve_registry_response_group as _resolve_registry_response_group,
    _run_shadow_compare as _run_shadow_compare,
    _select_single_scope_edgar_tag_result as _select_single_scope_edgar_tag_result,
    _single_tag_lookup as _single_tag_lookup,
    _tags_equivalent as _tags_equivalent,
    _validated_registry_group_for_metric_tag as _validated_registry_group_for_metric_tag,
)
from .dependency_graph import DependencyGraph  # noqa: F401 - compatibility facade for moved helper modules
from .excel_writer import write_xlsx
from .formatter import ModelFormatter
from .kpi_overrides_writer import business_model_to_ticker_overrides
from .model_build_context import (
    BuildSource as BuildSource,  # noqa: F401 - compatibility facade for build type imports
    Driver,
    HistoricalSources,
    ModelBuildContext as ModelBuildContext,  # noqa: F401 - compatibility facade
    SegmentConfig,
)
from .model_readiness import (
    ModelQualityReadiness as ModelQualityReadiness,  # noqa: F401 - compatibility facade
    ModelProjectionReadiness as ModelProjectionReadiness,  # noqa: F401 - compatibility facade
    ModelScenarioBridgeReadiness as ModelScenarioBridgeReadiness,  # noqa: F401 - compatibility facade
    ModelScenarioOutputReadiness as ModelScenarioOutputReadiness,  # noqa: F401 - compatibility facade
    ValuationInputReadiness as ValuationInputReadiness,  # noqa: F401 - compatibility facade
    _SCENARIO_OUTPUT_REQUIREMENTS,  # noqa: F401 - compatibility facade for moved scenario helpers
    compute_model_quality_readiness,
    compute_model_projection_readiness,
    compute_model_scenario_bridge_readiness,
    compute_model_scenario_output_readiness,
)
from .model_semantics import (
    ValuationArtifact as ValuationArtifact,  # noqa: F401 - compatibility facade
    ValuationInputValue as ValuationInputValue,  # noqa: F401 - compatibility facade
)
from .models import (
    DataSourceMapping,
    EdgarProvenance,
    FmpProvenance as FmpProvenance,  # noqa: F401 - compatibility facade
    FinancialModel,
    FormulaSpec as FormulaSpec,  # noqa: F401 - compatibility facade
    FormulaType as FormulaType,  # noqa: F401 - compatibility facade
    ItemType as ItemType,  # noqa: F401 - compatibility facade
    LineItem as LineItem,  # noqa: F401 - compatibility facade
    LineItemRef as LineItemRef,  # noqa: F401 - compatibility facade
    NonadmissibleReasonCode,
    PERIOD_MODE_YEARLY as PERIOD_MODE_YEARLY,  # noqa: F401 - compatibility facade
    ScenarioInputs as ScenarioInputs,  # noqa: F401 - compatibility facade
    Section,  # noqa: F401 - compatibility facade for schema.build imports
    Unit as Unit,  # noqa: F401 - compatibility facade
    ValueCell as ValueCell,  # noqa: F401 - compatibility facade
    ValueProvenance as ValueProvenance,  # noqa: F401 - compatibility facade
    ValueSeries as ValueSeries,  # noqa: F401 - compatibility facade
    shift_period as shift_period,  # noqa: F401 - compatibility facade
)
from .overrides import TickerOverrides, load_ticker_overrides, merge_overrides
from .periods import build_period_metadata as build_period_metadata
from .presentation_tree import PresentationTree, _accumulate_tree
from .refs import line_item_ref_from_obj as line_item_ref_from_obj  # noqa: F401 - compatibility facade
from .renderer import RenderPlan, _index_to_col as _index_to_col, render_model  # noqa: F401 - compatibility facade
from .scaling import _edgar_scale_to_millions, _PER_SHARE_CONCEPTS  # noqa: F401
from .segments import (
    EdgarFinancialsFetcher,
    SegmentInfo,
    SegmentProfile,
    apply_segment_overrides,
    discover_all_axes,
    discover_segments_with_payloads,
    expand_segments,
    populate_segment_historicals,
    prune_segment_formula_periods,
    revenue_observations_to_values as revenue_observations_to_values,
    segment_revenue_observations_from_snapshot as segment_revenue_observations_from_snapshot,  # noqa: F401 - compatibility facade
)
from .source_routing import (
    ConceptSourceRoute as ConceptSourceRoute,  # noqa: F401 - compatibility facade
    resolve_source_for_concept as resolve_source_for_concept,  # noqa: F401 - compatibility facade
    validate_route_eligibility as validate_route_eligibility,  # noqa: F401 - compatibility facade
)
from .source_values import (
    SourceValue,
    choose_preferred_source_value as choose_preferred_source_value,
    normalize_edgar_source_value as normalize_edgar_source_value,
)
from .registry_cache import EquivalenceGroup, get_registry_cache as get_registry_cache  # noqa: F401 - compatibility facade
from .templates import load_data_taxonomy, load_sia_generic_template
from .validation_input import ValidationInput

if TYPE_CHECKING:
    from .business_model import BusinessModel


_REGISTRY_EQUIVALENCE_VALUE_TOLERANCE_PCT = 0.01
_REGISTRY_EQUIVALENCE_VALUE_TOLERANCE_ABS_M = 0.1


def _seed_validation_input_type_hint_globals() -> None:
    from .business_model import BusinessModel as _BusinessModel
    from .business_model_compiler import CompiledDriverRegistry as _CompiledDriverRegistry

    _build_edgar_fetch_module.DataSourceMapping = DataSourceMapping
    _build_edgar_fetch_module.EdgarConceptFetchResult = EdgarConceptFetchResult
    _build_edgar_fetch_module.EdgarFetcher = EdgarFetcher
    _build_edgar_fetch_module.EdgarProvenance = EdgarProvenance
    _build_edgar_fetch_module.EquivalenceGroup = EquivalenceGroup
    _build_edgar_fetch_module.NonadmissibleReasonCode = NonadmissibleReasonCode
    _build_edgar_fetch_module.SourceValue = SourceValue
    _build_population_module.EdgarConceptFetchResult = EdgarConceptFetchResult
    _build_population_module.EdgarFetcher = EdgarFetcher
    _build_population_module.FmpConceptFetchResult = FmpConceptFetchResult
    _build_population_module.PopulateStats = PopulateStats
    _build_validation_inputs_module.EdgarConceptFetchResult = EdgarConceptFetchResult
    _build_validation_inputs_module.EdgarFetcher = EdgarFetcher
    _build_validation_inputs_module.FmpConceptFetchResult = FmpConceptFetchResult
    _build_validation_inputs_module.PopulateStats = PopulateStats
    _build_routed_historicals_module.EdgarConceptFetchResult = EdgarConceptFetchResult
    _build_routed_historicals_module.FmpConceptFetchResult = FmpConceptFetchResult
    _build_routed_historicals_module.PopulateStats = PopulateStats
    _build_routed_historicals_module.ServedByBreakdown = ServedByBreakdown
    _build_routed_historicals_module.SourceResolutionEntry = SourceResolutionEntry
    _build_total_equity_module.EdgarConceptFetchResult = EdgarConceptFetchResult
    _build_total_equity_module.EdgarFetcher = EdgarFetcher
    _build_concept_buffers_module.EdgarConceptFetchResult = EdgarConceptFetchResult
    _build_concept_buffers_module.EdgarFetcher = EdgarFetcher
    _build_concept_buffers_module.FmpConceptFetchResult = FmpConceptFetchResult
    _build_mbc_seeds_module.BusinessModel = _BusinessModel
    _build_mbc_seeds_module.SegmentConfig = SegmentConfig
    _build_projection_seeds_module.CompiledDriverRegistry = _CompiledDriverRegistry


_seed_validation_input_type_hint_globals()


def load_template() -> FinancialModel:
    """Load the bundled generic SIA template and build its item index."""

    model = load_sia_generic_template()
    model.build_index()
    return model


def build_model(
    ticker: str,
    company_name: str,
    fiscal_year_end: str,
    most_recent_fy: int,
    output_path: Optional[str] = None,
    source: str = "fmp",
    fmp_data: Optional[Dict] = None,
    sector: Optional[str] = None,
    n_historical: int = 5,
    n_projection: int = 12,
    formatter: Optional[ModelFormatter] = None,
    edgar_fetcher: Optional[EdgarFetcher] = None,
    segment_mapping: Optional[List[Dict]] = None,
    edgar_financials_fetcher: Optional[EdgarFinancialsFetcher] = None,
    axis: Optional[str] = None,
    formula_first: bool = True,
    mbc_segment_config: SegmentConfig | None = None,
    business_model: "BusinessModel | None" = None,
    mbc_drivers: dict[str, Driver] | None = None,
    historical_sources: HistoricalSources | None = None,
    presentation_tree: PresentationTree | None = None,
    validation_mode: bool = False,
    run_diagnostics: bool = False,
    equity_risk_premium: float | None = None,
    valuation_comps: ValuationCompsPayload | dict[str, Any] | None = None,
    overrides_dir: Path | None = None,
) -> BuildResult:
    """Build a populated model and optional workbook from the SIA template."""

    model = load_template()
    source = str(source).lower()
    profile: Optional[SegmentProfile] = None
    compiled_registry = None

    if mbc_segment_config is not None and segment_mapping is not None:
        raise ValueError("mbc_segment_config supplants the positional segment_mapping parameter")
    if mbc_segment_config is not None and axis is not None:
        raise ValueError("mbc_segment_config supplants the positional axis parameter")
    if axis is not None and edgar_financials_fetcher is None:
        raise ValueError("axis requires edgar_financials_fetcher")
    if segment_mapping is not None and edgar_financials_fetcher is None:
        raise ValueError("segment_mapping requires edgar_financials_fetcher")

    if business_model is not None:
        from .business_model_compiler import compile_business_model

        edgar_snapshot = None
        if mbc_segment_config is not None and hasattr(mbc_segment_config, "segment_profile_snapshot"):
            edgar_snapshot = mbc_segment_config.segment_profile_snapshot
        compiled_registry = compile_business_model(
            model,
            business_model,
            edgar_snapshot=edgar_snapshot,
        )
        profile = compiled_registry.segment_profile
    elif mbc_segment_config is not None:
        profile = _segment_profile_from_snapshot(ticker, mbc_segment_config)
        expand_segments(model, profile)
    elif edgar_financials_fetcher is not None:
        if source != "edgar":
            logging.warning("Segment mode requires EDGAR source; forcing source='edgar'")
            source = "edgar"
        if edgar_fetcher is None:
            raise ValueError("segment mode requires edgar_fetcher for consolidated populate")

        if axis is not None:
            multi_axis_result = discover_all_axes(
                ticker=ticker,
                fetcher=edgar_financials_fetcher,
                most_recent_fy=most_recent_fy,
                n_historical=n_historical,
            )
            discovered = next(
                (
                    candidate
                    for candidate in multi_axis_result.profiles
                    if candidate.axis_used == axis
                ),
                None,
            )
            if discovered is None:
                available_axes = [
                    candidate.axis_used
                    for candidate in multi_axis_result.profiles
                    if candidate.axis_used
                ]
                available_display = ", ".join(available_axes) if available_axes else "none"
                raise ValueError(
                    f"Requested axis '{axis}' did not pass validation. Available: {available_display}"
                )
            if presentation_tree is not None:
                logging.info(
                    "Segment mode: overriding caller-passed presentation_tree with segment-derived tree (mode-exclusive)"
                )
            presentation_tree = _accumulate_tree(multi_axis_result.payloads_by_year)
        else:
            discovered, segment_payloads = discover_segments_with_payloads(
                ticker=ticker,
                fetcher=edgar_financials_fetcher,
                most_recent_fy=most_recent_fy,
                n_historical=n_historical,
            )
            if presentation_tree is not None:
                logging.info(
                    "Segment mode: overriding caller-passed presentation_tree with segment-derived tree (mode-exclusive)"
                )
            presentation_tree = _accumulate_tree(segment_payloads)
        profile = (
            apply_segment_overrides(discovered, segment_mapping)
            if segment_mapping is not None
            else discovered
        )

        if profile.segments:
            expand_segments(model, profile)
        else:
            profile = SegmentProfile(
                ticker=ticker,
                segments=[SegmentInfo(name="Total Revenue")],
                source="fallback_single",
                total_revenue_check=dict(discovered.total_revenue_check or {}),
            )
            expand_segments(model, profile)

    remap_time_structure(
        model,
        most_recent_fy=most_recent_fy,
        n_historical=n_historical,
        n_projection=n_projection,
    )
    if profile and profile.segments and compiled_registry is None:
        prune_segment_formula_periods(model, profile)

    taxonomy = load_data_taxonomy()
    ticker_overrides = load_ticker_overrides(ticker, overrides_dir=overrides_dir)
    if business_model is not None:
        bm_inline_values = _bm_segment_snapshot_inline_values(business_model, mbc_segment_config)
        bm_inline_values.update(
            {
                key: values
                for key, values in _bm_revenue_share_inline_values(
                    business_model,
                    fmp_data=fmp_data,
                    most_recent_fy=most_recent_fy,
                    n_historical=n_historical,
                ).items()
                if key not in bm_inline_values
            }
        )
        ticker_overrides, bm_override_report = business_model_to_ticker_overrides(
            business_model,
            ticker,
            existing=ticker_overrides,
            inline_values_by_node=bm_inline_values,
            generated_conflict_strategy="prefer_new_unlocked",
        )
        if bm_override_report.conflicts:
            raise ValueError(
                f"BusinessModel custom concept conflicts for {ticker}: "
                f"{', '.join(bm_override_report.conflicts)}"
            )
    overrides_applied_count = 0
    if ticker_overrides is not None:
        taxonomy, overrides_applied_count = merge_overrides(taxonomy, ticker_overrides)

    populate_historical_sources, historical_taxonomy = _zero_missing_edgar_fallback_routing(
        source=source,
        taxonomy=taxonomy,
        historical_sources=historical_sources,
        edgar_fetcher=edgar_fetcher,
        fmp_data=fmp_data,
    )

    stats = populate_historicals(
        model,
        source=source,
        ticker=ticker,
        taxonomy=historical_taxonomy,
        most_recent_fy=most_recent_fy,
        n_historical=n_historical,
        fmp_data=fmp_data,
        edgar_fetcher=edgar_fetcher,
        historical_sources=populate_historical_sources,
    )
    semantic_rows_result = SemanticRowsResult()
    semantic_rows_applied_count = 0
    if ticker_overrides is not None and ticker_overrides.semantic_rows:
        semantic_rows_result = _materialize_semantic_rows(model, ticker, ticker_overrides)
        if semantic_rows_result.source_custom_concepts:
            semantic_overrides = TickerOverrides(
                ticker=ticker,
                overrides={},
                custom_concepts=semantic_rows_result.source_custom_concepts,
                file_meta={"ticker": ticker, "schema_version": "3"},
            )
            semantic_rows_applied_count = _populate_custom_concepts(
                model,
                ticker,
                semantic_overrides,
                source=source,
                edgar_fetcher=edgar_fetcher,
                fmp_data=fmp_data,
                # Semantic rows carry their source policy in the row contract;
                # per-concept routed-build overrides still apply to ordinary
                # custom_concepts below.
                historical_sources=None,
                business_model=business_model,
                most_recent_fy=most_recent_fy,
                n_historical=n_historical,
            )
    custom_concepts_applied_count = 0
    if ticker_overrides is not None and ticker_overrides.custom_concepts:
        custom_concepts_applied_count = _populate_custom_concepts(
            model,
            ticker,
            ticker_overrides,
            source=source,
            edgar_fetcher=edgar_fetcher,
            fmp_data=fmp_data,
            historical_sources=historical_sources,
            business_model=business_model,
            most_recent_fy=most_recent_fy,
            n_historical=n_historical,
        )
    if semantic_rows_applied_count:
        logging.info(
            "Applied %d semantic row historical cells for %s",
            semantic_rows_applied_count,
            ticker,
        )
    if profile and profile.segments:
        historical_periods = list(range(most_recent_fy - n_historical + 1, most_recent_fy + 1))
        populate_segment_historicals(model, profile, historical_periods)
    if compiled_registry is not None and mbc_drivers:
        _apply_mbc_seeds(model, mbc_drivers, compiled_registry)
    historical_periods = [int(period) for period in model.time_structure.historical_periods]
    if formula_first and presentation_tree is not None:
        _apply_presentation_catch_all_residuals(
            model,
            taxonomy=taxonomy,
            presentation_tree=presentation_tree,
            historical_periods=historical_periods,
        )

    derivable_items: Dict[str, set[int]] = {}
    if formula_first:
        derivable_items = apply_formula_first(
            model,
            historical_periods,
        )
        semantic_rows_result = _materialize_client_funds_subtotal_bridges(
            model,
            ticker,
            historical_periods,
            semantic_rows_result,
            business_model=business_model,
        )
    update_company_info(
        model,
        ticker=ticker,
        name=company_name,
        fye=fiscal_year_end,
        sector=sector,
    )
    model.build_index()
    valuation_input_readiness = populate_valuation_inputs(
        model,
        fmp_data,
        equity_risk_premium=equity_risk_premium,
        valuation=ticker_overrides.valuation if ticker_overrides is not None else None,
    )
    populate_valuation_comps(model, valuation_comps)
    seed_projections_result = _seed_projections_from_overrides(
        model,
        ticker_overrides,
        compiled_registry,
        most_recent_fy,
    )
    for orphan in seed_projections_result.orphans:
        logging.warning(
            "Projection override orphan for %s: %s (%s)",
            orphan.rate_key,
            orphan.reason,
            orphan.detail,
        )
    for warning in seed_projections_result.warnings:
        logging.warning(
            "Projection override warning for %s: %s (%s)",
            warning.rate_key,
            warning.kind,
            warning.detail,
        )
    non_gaap_addback_seed_result = _seed_projected_non_gaap_addbacks(model)
    for seeded_item_id in non_gaap_addback_seed_result.get("seeded", []):
        logging.info("Seeded projected non-GAAP add-back fallback for %s", seeded_item_id)
    for skipped in non_gaap_addback_seed_result.get("skipped", []):
        logging.warning(
            "Skipped projected non-GAAP add-back fallback for %s: %s",
            skipped.get("item_id"),
            skipped.get("reason"),
        )
    _populate_scenario_eps(model, compute_scenario_eps(model))
    _populate_scenario_inputs(model)
    model.build_index()
    projection_readiness = compute_model_projection_readiness(
        model,
        compiled_registry=compiled_registry,
        segment_profile=profile if compiled_registry is None else None,
        seed_projections=seed_projections_result,
    )
    scenario_output_readiness = compute_model_scenario_output_readiness(model)
    scenario_bridge_readiness = compute_model_scenario_bridge_readiness(model)
    model_quality_readiness = compute_model_quality_readiness(
        model,
        valuation_input_readiness=valuation_input_readiness,
        segment_profile=profile,
    )
    diagnostic: DiagnosticReport | None = None
    validation_input: ValidationInput | None = None
    if validation_mode:
        validation_input = _make_validation_input(
            ticker=ticker,
            taxonomy=taxonomy,
            historical_periods=historical_periods,
            fmp_data=fmp_data,
            edgar_fetcher=edgar_fetcher,
            stats=stats,
        )
    should_run_diagnostics = (
        source == "edgar"
        or validation_mode
        or run_diagnostics
        or presentation_tree is not None
    )
    if should_run_diagnostics:
        try:
            diagnostic = run_build_diagnostic(
                model,
                ticker=ticker,
                fy=most_recent_fy,
                taxonomy=taxonomy,
                stats=stats,
                derivable_items=derivable_items,
                presentation_tree=presentation_tree,
                validation_input=validation_input,
            )
            _write_diagnostic_log(diagnostic)
        except Exception:
            logging.exception(
                "Build diagnostic failed for %s fy=%s",
                ticker,
                most_recent_fy,
            )
    plan = render_model(model, formatter=formatter)
    _append_valuation_comp_clear_writes(plan, valuation_comps)
    assert_valuation_template_schema(
        model,
        origin="build_model",
        workbook_path=output_path,
        module_path=__file__,
    )

    if output_path:
        write_xlsx(plan, output_path)

    return BuildResult(
        model=model,
        render_plan=plan,
        output_path=output_path,
        stats=stats,
        diagnostic=diagnostic,
        segment_profile=profile,
        compiled_registry=compiled_registry,
        derivable_items=derivable_items,
        presentation_tree=presentation_tree,
        overrides_applied=overrides_applied_count,
        custom_concepts_applied=custom_concepts_applied_count,
        seed_projections=seed_projections_result,
        semantic_rows=semantic_rows_result,
        projection_readiness=projection_readiness,
        scenario_output_readiness=scenario_output_readiness,
        scenario_bridge_readiness=scenario_bridge_readiness,
        model_quality_readiness=model_quality_readiness,
        valuation_input_readiness=valuation_input_readiness,
    )


__all__ = [
    "BuildResult",
    "EdgarFetcher",
    "EdgarWarmResult",
    "PopulateStats",
    "ValuationCompEntry",
    "ValuationCompsPayload",
    "apply_formula_first",
    "build_model",
    "build_model_from_mbc",
    "compute_scenario_eps",
    "compute_scenario_outputs",
    "load_template",
    "populate_valuation_comps",
    "populate_valuation_inputs",
    "populate_from_edgar",
    "populate_from_fmp",
    "populate_historicals",
    "remap_time_structure",
    "update_company_info",
]
