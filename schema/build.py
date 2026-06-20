"""Build populated financial models from the bundled generic SIA template."""

from __future__ import annotations

import calendar
import concurrent.futures
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timezone
import logging
import math
import re
from typing import TYPE_CHECKING, Any, Callable, Dict, Iterable, List, Literal, Optional, Protocol, TypedDict

from api.credentials import get_equivalence_flag, is_analyst_cron_mode

from .build_diagnostics import (
    BS_SECTIONS,
    DiagnosticReport,
    _build_taxonomy_tag_index,
    _effective_section_members,
    _select_non_overlapping_presentation_children,
    _write_diagnostic_log,
    run_build_diagnostic,
)
from .dependency_graph import DependencyGraph
from .excel_writer import write_xlsx
from .formatter import ModelFormatter
from .kpi_overrides_writer import business_model_to_ticker_overrides
from .model_build_context import BuildSource, Driver, HistoricalSources, ModelBuildContext, SegmentConfig
from .model_readiness import (
    ModelQualityReadiness,
    ModelProjectionReadiness,
    ModelScenarioBridgeReadiness,
    ModelScenarioOutputReadiness,
    ValuationInputReadiness,
    _SCENARIO_OUTPUT_REQUIREMENTS,
    compute_model_quality_readiness,
    compute_model_projection_readiness,
    compute_model_scenario_bridge_readiness,
    compute_model_scenario_output_readiness,
)
from .model_semantics import ValuationArtifact, ValuationInputValue
from .models import (
    BuildStatus,
    DataSourceMapping,
    EdgarProvenance,
    FmpProvenance,
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
    NonadmissibleReasonCode,
    PERIOD_MODE_YEARLY,
    ScenarioInputs,
    Unit,
    ValueCell,
    ValueProvenance,
    ValueSeries,
    shift_period,
)
from .overrides import TickerOverrides, load_ticker_overrides, merge_overrides
from .periods import build_period_metadata
from .presentation_tree import PresentationTree, _accumulate_tree
from .refs import line_item_ref_from_obj
from .renderer import CellWrite, RenderPlan, _index_to_col, render_model
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
    revenue_observations_to_values,
    segment_revenue_observations_from_snapshot,
)
from .source_routing import ConceptSourceRoute, resolve_source_for_concept, validate_route_eligibility
from .source_values import (
    SourceValue,
    choose_preferred_source_value,
    normalize_edgar_source_value,
)
from .registry_cache import EquivalenceGroup, get_registry_cache
from .templates import load_data_taxonomy, load_sia_generic_template
from .validation_input import ValidationInput

if TYPE_CHECKING:
    from .business_model import BusinessModel
    from .business_model_compiler import CompiledDriverRegistry


_SYNTHETIC_FAST_PATH_TYPES = frozenset({
    FormulaType.arithmetic,
    FormulaType.ref,
    FormulaType.ratio,
})
_FORMULA_FIRST_EXCLUDED_ITEM_IDS = frozenset({
    # Direct CF ending-cash actuals are statement-basis values. The historical
    # formula is a no-source fallback bridge from BS cash, not an equivalence
    # proof when restricted cash moves separately.
    "tpl.fm.cash_flow.cash_and_cash_equivalents_end_of_period",
})
_MAX_REF_CHAIN_DEPTH = 16
_COMPANY_NAME_TOKEN = "[Company Name] ([TICKER])"
_DSM_EXCLUDED_FROM_OVERRIDE_ENTRY = {"notes"}
_DSM_FIELD_NAMES = set(DataSourceMapping.model_fields.keys()) - _DSM_EXCLUDED_FROM_OVERRIDE_ENTRY
_AXIS_QNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*:[A-Za-z][A-Za-z0-9_-]*$")
_EDGAR_AXIS_FAMILY_LABELS = frozenset({"business_segment", "product", "geography"})
ROLLING_HEADER_EXCLUSIONS = {
    "tpl.a.header.year_header",
}
_REGISTRY_EQUIVALENCE_VALUE_TOLERANCE_PCT = 0.01
_REGISTRY_EQUIVALENCE_VALUE_TOLERANCE_ABS_M = 0.1


class EdgarFetcher(Protocol):
    def __call__(
        self,
        ticker: str,
        metric_name: str,
        end_year: int,
        n_periods: int,
        *,
        include_equivalents: bool = False,
        axis_key: str | None = None,
    ) -> Dict: ...


@dataclass
class SourceResolutionEntry:
    concept_id: str
    requested_primary: BuildSource
    requested_fallback_order: list[BuildSource]
    layer_decided: Literal["taxonomy", "mbc_default", "mbc_override"]
    served_by: BuildSource | None
    fallback_used: bool
    served_year_count: int


@dataclass
class ServedByBreakdown:
    primary_source: BuildSource
    years_via_primary: List[int] = field(default_factory=list)
    years_via_fallback: List[int] = field(default_factory=list)
    years_unserved: List[int] = field(default_factory=list)


_FMP_QUALITY_BUCKET_FIELDS = frozenset(
    {
        ("balance_sheet", "otherCurrentAssets"),
        ("balance_sheet", "otherNonCurrentAssets"),
        ("balance_sheet", "otherCurrentLiabilities"),
        ("balance_sheet", "otherNonCurrentLiabilities"),
        ("cash_flow", "otherWorkingCapital"),
        ("cash_flow", "otherNonCashItems"),
        ("cash_flow", "otherInvestingActivities"),
        ("cash_flow", "otherFinancingActivities"),
    }
)
_FMP_QUALITY_INFORMATIONAL_BUCKET_CONCEPTS = frozenset(
    {
        "change_in_other_working_capital",
        "cf_other_non_cash_items",
    }
)
_FMP_QUALITY_YOY_RATIO_THRESHOLD = 3.0
_FMP_QUALITY_ABS_DELTA_M = 100.0


@dataclass
class PopulateStats:
    source: str
    items_populated: int
    items_skipped: int
    periods_populated: int
    missing_concepts: List[str]
    edgar_api_calls: int = 0
    edgar_errors: List[str] = None
    edgar_partial_failures: List[str] = None
    source_resolution: list[SourceResolutionEntry] = field(default_factory=list)
    fallback_engaged_concepts: List[str] = field(default_factory=list)
    fallback_engaged_cells: int = 0
    served_by_breakdown: Dict[str, ServedByBreakdown] = field(default_factory=dict)
    fmp_quality_warnings: List[Dict[str, object]] = field(default_factory=list)
    served_source_by_concept_year: dict[str, dict[int, BuildSource]] = field(
        default_factory=dict
    )

    def __post_init__(self) -> None:
        if self.edgar_errors is None:
            self.edgar_errors = []
        if self.edgar_partial_failures is None:
            self.edgar_partial_failures = []
        if self.fmp_quality_warnings is None:
            self.fmp_quality_warnings = []


@dataclass
class OrphanedProjection:
    rate_key: str
    reason: Literal[
        "bm_key_not_in_registry",
        "tpl_item_not_found",
        "item_not_found",
        "bull_flex_row_not_found",
        "bear_flex_row_not_found",
    ]
    detail: str
    last_provenance: dict[str, Any] | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "rate_key": self.rate_key,
            "reason": self.reason,
            "detail": self.detail,
            "last_provenance": self.last_provenance,
        }


@dataclass
class SeedProjectionWarning:
    rate_key: str
    kind: Literal[
        "base_flex_row_not_found",
        "base_period_values_missing",
        "scenario_ordering_violation",
    ]
    detail: str
    last_provenance: dict[str, Any] | None
    scenario: Literal["bull", "bear"] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "rate_key": self.rate_key,
            "kind": self.kind,
            "scenario": self.scenario,
            "detail": self.detail,
            "last_provenance": self.last_provenance,
        }


@dataclass
class SeedProjectionsResult:
    seeded_count: int = 0
    orphans: list[OrphanedProjection] = field(default_factory=list)
    warnings: list[SeedProjectionWarning] = field(default_factory=list)
    total_rate_keys: int = 0
    schema_version_seen: str | None = None
    validation_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "seeded_count": self.seeded_count,
            "orphans": [orphan.to_dict() for orphan in self.orphans],
            "warnings": [warning.to_dict() for warning in self.warnings],
            "total_rate_keys": self.total_rate_keys,
            "schema_version_seen": self.schema_version_seen,
            "validation_error": self.validation_error,
        }


@dataclass
class SemanticRowsResult:
    materialized: list[dict[str, Any]] = field(default_factory=list)
    linkages: list[dict[str, Any]] = field(default_factory=list)
    collisions: list[dict[str, Any]] = field(default_factory=list)
    gaps: list[dict[str, Any]] = field(default_factory=list)
    source_custom_concepts: dict[str, dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "materialized": list(self.materialized),
            "linkages": list(self.linkages),
            "collisions": list(self.collisions),
            "gaps": list(self.gaps),
        }


@dataclass
class BuildResult:
    model: FinancialModel
    render_plan: RenderPlan
    output_path: Optional[str]
    stats: PopulateStats
    diagnostic: Optional[DiagnosticReport] = None
    segment_profile: Optional[SegmentProfile] = None
    compiled_registry: "CompiledDriverRegistry | None" = None
    derivable_items: Dict[str, set[int]] = field(default_factory=dict)
    presentation_tree: PresentationTree | None = None
    overrides_applied: int = 0
    custom_concepts_applied: int = 0
    seed_projections: SeedProjectionsResult = field(default_factory=SeedProjectionsResult)
    semantic_rows: SemanticRowsResult = field(default_factory=SemanticRowsResult)
    projection_readiness: ModelProjectionReadiness = field(
        default_factory=ModelProjectionReadiness
    )
    scenario_output_readiness: ModelScenarioOutputReadiness = field(
        default_factory=ModelScenarioOutputReadiness
    )
    scenario_bridge_readiness: ModelScenarioBridgeReadiness = field(
        default_factory=ModelScenarioBridgeReadiness
    )
    model_quality_readiness: ModelQualityReadiness = field(
        default_factory=ModelQualityReadiness
    )
    valuation_input_readiness: ValuationInputReadiness = field(
        default_factory=ValuationInputReadiness
    )


class ValuationCompEntry(TypedDict, total=False):
    """One valuation-comps row consumed by build_model's fixed-cell populator.

    Required-by-convention field: ticker. Optional numeric fields:
    - forward_pe: FY1-forward P/E snapshot.
    - peg: current NTM PEG snapshot.
    - ev_ebitda: current EV/EBITDA snapshot, retained for downstream callers.
    - trailing_low/trailing_median/trailing_high: trailing-basis P/E range.
    """

    ticker: str
    forward_pe: float | int | str | None
    peg: float | int | str | None
    ev_ebitda: float | int | str | None
    trailing_low: float | int | str | None
    trailing_median: float | int | str | None
    trailing_high: float | int | str | None


class ValuationCompsPayload(TypedDict, total=False):
    """Payload contract for build-time valuation comps population.

    Shape:
      {
        "source": "peer_comparison" | "build_fallback" | str,
        "basis": "forward_ntm_fy1" | str,
        "target": ValuationCompEntry,
        "peers": [ValuationCompEntry, ...],
      }

    The primary production source is the caller's bridge from
    Thesis.industry_analysis.peer_comparison. schema/build.py only writes this
    payload into the template; it must not fetch from MCP or risk_module.
    """

    source: str
    basis: str
    target: ValuationCompEntry
    peers: list[ValuationCompEntry]


@dataclass(frozen=True)
class EdgarWarmResult:
    status: str
    payload: dict | None
    message: str | None = None


@dataclass
class EdgarConceptFetchResult:
    values_dict: Dict[int, float]
    failed_years: set[int]
    status: str
    periods_failed: int
    api_calls: int
    provenance_by_year: Dict[int, EdgarProvenance] = field(default_factory=dict)
    source_values_by_year: Dict[int, SourceValue] = field(default_factory=dict)
    reported_period_ends_by_year: Dict[int, str] = field(default_factory=dict)
    error_message: str | None = None

    def as_tuple(self) -> tuple[Dict[int, float], set[int], str, int, int]:
        return (
            self.values_dict,
            self.failed_years,
            self.status,
            self.periods_failed,
            self.api_calls,
        )


@dataclass
class ParsedEdgarSeriesResult:
    values_dict: Dict[int, float]
    failed_years: set[int]
    entry_failed: int
    provenance_by_year: Dict[int, EdgarProvenance] = field(default_factory=dict)
    source_values_by_year: Dict[int, SourceValue] = field(default_factory=dict)
    reported_period_ends_by_year: Dict[int, str] = field(default_factory=dict)


@dataclass
class FmpConceptFetchResult:
    concept_id: str
    values: dict[int, float]
    field_used_by_year: dict[int, str]
    fallback_field_years: set[int]
    missing: bool
    reported_period_ends_by_year: dict[int, str] = field(default_factory=dict)


def _segment_profile_from_snapshot(
    ticker: str,
    segment_config: SegmentConfig,
) -> SegmentProfile:
    snapshot = segment_config.segment_profile_snapshot
    sorted_snapshot_segments = sorted(
        snapshot.segments,
        key=lambda segment: segment.segment_index,
    )
    return SegmentProfile(
        ticker=ticker,
        segments=[
            SegmentInfo(
                name=segment.name,
                edgar_member=segment.edgar_member,
                revenue_observations=segment_revenue_observations_from_snapshot(segment),
                volume_label=segment.volume_label,
                price_label=segment.price_label,
            )
            for segment in sorted_snapshot_segments
        ],
        source=snapshot.source,
        axis_used=snapshot.axis_used,
        total_revenue_check=dict(snapshot.total_revenue_check) if snapshot.total_revenue_check is not None else None,
    )


def load_template() -> FinancialModel:
    """Load the bundled generic SIA template and build its item index."""

    model = load_sia_generic_template()
    model.build_index()
    return model


def remap_time_structure(
    model: FinancialModel,
    most_recent_fy: int,
    n_historical: int = 5,
    n_projection: int = 12,
) -> None:
    """Remap the template time axis to the requested fiscal year window."""

    old_hist = [int(period) for period in (model.time_structure.historical_periods or model.time_structure.historical_years)]
    old_proj = [int(period) for period in (model.time_structure.projection_periods or model.time_structure.projection_years)]
    template_projection_periods = list(old_proj)

    max_historical = len(old_hist)
    if n_historical > max_historical:
        raise ValueError(f"n_historical cannot exceed template maximum of {max_historical}")
    if n_projection > len(old_proj):
        if not old_proj:
            raise ValueError("n_projection cannot be extended because the template has no projection periods")
        period_mode = model.time_structure.period_mode or PERIOD_MODE_YEARLY
        while len(old_proj) < n_projection:
            next_period = shift_period(old_proj[-1], 1, period_mode)
            if next_period is None:
                next_period = old_proj[-1] + 1
            old_proj.append(int(next_period))

    new_hist = list(range(most_recent_fy - n_historical + 1, most_recent_fy + 1))
    new_proj = list(range(most_recent_fy + 1, most_recent_fy + n_projection + 1))

    year_map: Dict[int, int] = {}
    year_map.update(zip(old_hist[-n_historical:] if n_historical else [], new_hist))
    year_map.update(zip(old_proj[:n_projection], new_proj))

    all_periods = new_hist + new_proj
    column_map = {
        period: _index_to_col(index)
        for index, period in enumerate(all_periods, start=1)
    }
    historical_set = set(new_hist)
    projection_set = set(new_proj)
    available_periods = set(all_periods)

    model.time_structure.historical_periods = list(new_hist)
    model.time_structure.projection_periods = list(new_proj)
    model.time_structure.historical_years = list(new_hist)
    model.time_structure.projection_years = list(new_proj)
    model.time_structure.column_map = dict(column_map)
    model.time_structure.period_column_map = dict(column_map)

    for item in _iter_items(model):
        if item.formula_periods is not None:
            original_formula_periods = {int(period) for period in item.formula_periods}
            spans_full_template_projection = (
                bool(template_projection_periods)
                and set(template_projection_periods).issubset(original_formula_periods)
            )
            projection_sentinel_set = (
                set(template_projection_periods[-2:])
                if len(template_projection_periods) >= 2
                else set(template_projection_periods)
            )
            # Sparse projection sentinels use the last two template projection
            # years as compact markers for "apply through the projection
            # horizon." Keep this branch conservative: it only applies to
            # non-fixed-cell items with projected formulas, excludes the dense
            # projection pattern, and carves out rolling date headers whose
            # year-specific overrides should not be naively extended.
            ends_with_projection_sentinel = (
                bool(projection_sentinel_set.intersection(original_formula_periods))
                and not spans_full_template_projection
                and item.column is None
                and item.projected is not None
                and item.id not in ROLLING_HEADER_EXCLUSIONS
            )
            is_period_relative_offset_scenario = (
                item.column is None
                and item.projected is not None
                and item.projected.type == FormulaType.valuation
                and item.projected.subtype == "offset_scenario"
                and (item.projected.params or {}).get("column_offset_mode") == "period_relative"
            )
            is_self_carry_forward_projection = _is_self_carry_forward_projection(
                item,
                template_projection_periods=template_projection_periods,
                original_formula_periods=original_formula_periods,
            )
            remapped_periods = [
                year_map[int(period)]
                for period in item.formula_periods
                if int(period) in year_map
            ]
            if (
                spans_full_template_projection
                or ends_with_projection_sentinel
                or is_period_relative_offset_scenario
                or is_self_carry_forward_projection
            ):
                remapped_periods.extend(new_proj)
            item.formula_periods = [
                period
                for period in sorted(set(remapped_periods))
                if _formula_period_is_valid(
                    model,
                    item,
                    period,
                    historical_set=historical_set,
                    projection_set=projection_set,
                    available_periods=available_periods,
                )
            ]

        if item.overrides is not None:
            remapped_overrides = {
                year_map[int(period)]: spec
                for period, spec in item.overrides.items()
                if int(period) in year_map
            }
            item.overrides = remapped_overrides or None

    for item in _iter_items(model):
        if item.historical is None or item.formula_periods is None:
            continue

        existing = set(item.formula_periods)
        extended = False
        for year in new_hist:
            if year in existing:
                continue
            if not _formula_period_is_valid(
                model,
                item,
                year,
                historical_set=historical_set,
                projection_set=projection_set,
                available_periods=available_periods,
            ):
                continue
            item.formula_periods.append(year)
            extended = True

        if extended:
            item.formula_periods.sort()


def _is_self_carry_forward_projection(
    item: LineItem,
    *,
    template_projection_periods: Iterable[int],
    original_formula_periods: set[int],
) -> bool:
    if (
        item.column is not None
        or item.projected is None
        or item.projected.type != FormulaType.ref
        or item.id in ROLLING_HEADER_EXCLUSIONS
    ):
        return False
    if not set(template_projection_periods).intersection(original_formula_periods):
        return False
    source_ref = line_item_ref_from_obj((item.projected.params or {}).get("source"))
    return source_ref is not None and source_ref.id == item.id and int(source_ref.t) == -1


def populate_from_fmp(
    model: FinancialModel,
    fmp_data: Optional[Dict],
    taxonomy: Dict[str, DataSourceMapping],
) -> PopulateStats:
    """Populate historical values from pre-fetched FMP statements."""

    historical_periods = [int(period) for period in model.time_structure.historical_periods]
    fmp_lookup = _build_fmp_lookup(fmp_data or {})
    missing_concepts: set[str] = set()
    items_populated = 0
    items_skipped = 0
    periods_populated = 0
    served_source_by_concept_year: dict[str, dict[int, BuildSource]] = {}
    fmp_quality_observations: Dict[tuple[str, str, str], Dict[int, float]] = {}

    for item in _iter_items(model):
        concept_id = item.data_concept_id
        if not concept_id:
            continue

        concept = taxonomy.get(concept_id)
        if concept is None:
            items_skipped += 1
            continue

        endpoint = concept.fmp_endpoint
        field = concept.fmp_field
        item_has_actuals = False

        for year in historical_periods:
            record = fmp_lookup.get(endpoint, {}).get(year)
            raw_value, field_used, _fallback_used = _raw_fmp_value_for_concept(
                concept,
                record,
            )

            if raw_value is None:
                missing_concepts.add(concept_id)
                if item.historical is not None:
                    _set_constant_override(item, year, 0, synthetic=True)
                continue

            value = _scale_fmp_value(concept_id, raw_value, concept=concept)
            fmp_provenance = _make_fmp_provenance(concept, field_used)
            _record_fmp_quality_observation(
                fmp_quality_observations,
                concept,
                field_used,
                year,
                value,
            )
            if item.historical is None:
                _set_imported_value(item, year, value, fmp_provenance=fmp_provenance)
            else:
                _set_constant_override(item, year, value, fmp_provenance=fmp_provenance)

            item_has_actuals = True
            periods_populated += 1
            served_source_by_concept_year.setdefault(concept_id, {})[year] = "fmp"

        if item_has_actuals:
            items_populated += 1
        else:
            items_skipped += 1

    _seed_cash_beginning_of_period(model)
    _refresh_period_metadata(
        model,
        _collect_fmp_reported_period_ends_from_lookup(fmp_lookup, historical_periods),
    )

    return PopulateStats(
        source="fmp",
        items_populated=items_populated,
        items_skipped=items_skipped,
        periods_populated=periods_populated,
        missing_concepts=sorted(missing_concepts),
        fmp_quality_warnings=_fmp_quality_warnings_from_observations(
            fmp_quality_observations
        ),
        served_source_by_concept_year=served_source_by_concept_year,
    )


def populate_from_edgar(
    model: FinancialModel,
    ticker: str,
    taxonomy: Dict[str, DataSourceMapping],
    edgar_fetcher: EdgarFetcher,
) -> PopulateStats:
    """Populate historical values from EDGAR metric series data."""

    historical_periods = [int(period) for period in model.time_structure.historical_periods]
    if not historical_periods:
        return PopulateStats(
            source="edgar",
            items_populated=0,
            items_skipped=0,
            periods_populated=0,
            missing_concepts=[],
        )

    most_recent_fy = max(historical_periods)
    n_historical = len(historical_periods)
    is_overlay = _has_existing_imported_historicals(model, historical_periods)

    missing_concepts: set[str] = set()
    edgar_errors: set[str] = set()
    edgar_partial_failures: set[str] = set()
    items_populated = 0
    items_skipped = 0
    periods_populated = 0
    edgar_api_calls = 0
    served_source_by_concept_year: dict[str, dict[int, BuildSource]] = {}
    fetched_concepts = 0
    failed_fetches = 0
    concept_cache: Dict[str, EdgarConceptFetchResult] = {}
    exception_failures: set[str] = set()

    concepts_to_fetch: Dict[str, DataSourceMapping] = {}
    for item in _iter_items(model):
        concept_id = item.data_concept_id
        if not concept_id or concept_id in concepts_to_fetch:
            continue

        concept = taxonomy.get(concept_id)
        if concept is None:
            continue

        concepts_to_fetch[concept_id] = concept
        if concept.preferred_source == "fmp" and (concept.edgar_tags or concept.registry_group_id):
            logging.warning("Concept '%s' prefers FMP but source='edgar' was requested", concept_id)

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(
                _fetch_edgar_concept_result,
                ticker=ticker,
                concept_id=concept_id,
                concept=concept,
                most_recent_fy=most_recent_fy,
                n_historical=n_historical,
                edgar_fetcher=edgar_fetcher,
            ): concept_id
            for concept_id, concept in concepts_to_fetch.items()
        }
        for future in concurrent.futures.as_completed(futures):
            concept_id = futures[future]
            try:
                concept_cache[concept_id] = future.result()
            except Exception as exc:
                logging.warning("EDGAR fetch failed for concept '%s': %s", concept_id, exc)
                concept_cache[concept_id] = EdgarConceptFetchResult(
                    values_dict={},
                    failed_years=set(),
                    status="missing",
                    periods_failed=0,
                    api_calls=0,
                )
                exception_failures.add(concept_id)

    for concept_id, concept in concepts_to_fetch.items():
        fetch_result = concept_cache.get(
            concept_id,
            EdgarConceptFetchResult(
                values_dict={},
                failed_years=set(),
                status="missing",
                periods_failed=0,
                api_calls=0,
            ),
        )
        if concept.edgar_tags or concept.registry_group_id:
            edgar_api_calls += fetch_result.api_calls
            fetched_concepts += 1
            if fetch_result.status == "failed" or concept_id in exception_failures:
                failed_fetches += 1

        if fetch_result.status == "failed":
            edgar_errors.add(concept_id)
        elif fetch_result.status == "missing":
            missing_concepts.add(concept_id)
        elif fetch_result.status == "ok":
            if fetch_result.periods_failed > 0:
                edgar_partial_failures.add(concept_id)
            if len(fetch_result.values_dict) < n_historical:
                missing_concepts.add(concept_id)

    for item in _iter_items(model):
        concept_id = item.data_concept_id
        if not concept_id:
            continue

        concept = taxonomy.get(concept_id)
        if concept is None:
            items_skipped += 1
            continue

        fetch_result = concept_cache.get(
            concept_id,
            EdgarConceptFetchResult(
                values_dict={},
                failed_years=set(),
                status="missing",
                periods_failed=0,
                api_calls=0,
            ),
        )
        item_has_actuals = False

        for year in historical_periods:
            if year in fetch_result.values_dict:
                value = fetch_result.values_dict[year]
                edgar_provenance = fetch_result.provenance_by_year.get(year)
                if item.historical is None:
                    _set_imported_value(
                        item,
                        year,
                        value,
                        provenance=ValueProvenance.imported_edgar,
                        edgar_provenance=edgar_provenance,
                    )
                else:
                    _set_constant_override(item, year, value, edgar_provenance=edgar_provenance)

                item_has_actuals = True
                periods_populated += 1
                served_source_by_concept_year.setdefault(concept_id, {})[year] = "edgar"
                continue

            if (
                item.historical is not None
                and not is_overlay
                and fetch_result.status != "failed"
                and year not in fetch_result.failed_years
            ):
                _set_constant_override(item, year, 0, synthetic=True)

        if item_has_actuals:
            items_populated += 1
        else:
            items_skipped += 1

    if fetched_concepts > 0 and failed_fetches == fetched_concepts:
        raise RuntimeError(_all_edgar_concepts_failed_message(concept_cache.values()))

    _seed_cash_beginning_of_period(model)
    _refresh_period_metadata(
        model,
        _collect_edgar_reported_period_ends(concept_cache.values(), historical_periods),
    )

    return PopulateStats(
        source="edgar",
        items_populated=items_populated,
        items_skipped=items_skipped,
        periods_populated=periods_populated,
        missing_concepts=sorted(missing_concepts),
        edgar_api_calls=edgar_api_calls,
        edgar_errors=sorted(edgar_errors),
        edgar_partial_failures=sorted(edgar_partial_failures),
        served_source_by_concept_year=served_source_by_concept_year,
    )


def warm_edgar_cache(
    ticker: str,
    historical_years: List[int],
    financials_fetcher: EdgarFinancialsFetcher,
) -> Dict[int, EdgarWarmResult]:
    """Warm the EDGAR FY financials cache for each historical year.

    Calls `/api/financials` for each year so subsequent `/api/metric/series`
    lookups hit a populated cache instead of silently returning empty.

    Returns {year: EdgarWarmResult}. Payload is retained for "success" and
    "partial" results; partial filings usually still have the balance-sheet
    face needed for presentation-tree diagnostics. The raw upstream status and
    message are preserved, plus the synthesized sentinel "exception" when the
    fetcher itself raises.
    Never raises — warming is best-effort; populate_from_edgar() will
    surface genuinely unavailable data as `missing_concepts`.
    """
    results: Dict[int, EdgarWarmResult] = {}
    if not historical_years:
        return results

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {
            executor.submit(financials_fetcher, ticker, int(year), 4, True): int(year)
            for year in historical_years
        }
        for future in concurrent.futures.as_completed(futures):
            year = futures[future]
            try:
                payload = future.result() or {}
                status = str(payload.get("status", "unknown"))
                retained_payload = payload if status in {"success", "partial"} else None
                message = _edgar_warm_message(payload)
                results[year] = EdgarWarmResult(status=status, payload=retained_payload, message=message)
                if status != "success":
                    logging.warning(
                        "EDGAR cache warm returned non-success for %s FY%d: %s%s",
                        ticker,
                        year,
                        status,
                        f" ({message})" if message else "",
                    )
            except Exception as exc:
                logging.warning("EDGAR cache warm failed for %s FY%d: %s", ticker, year, exc)
                results[year] = EdgarWarmResult(status="exception", payload=None, message=str(exc))
    return results


def _edgar_warm_message(payload: dict) -> str | None:
    if not isinstance(payload, dict):
        return None
    for key in ("message", "error", "detail"):
        value = payload.get(key)
        if value is None:
            continue
        if isinstance(value, str):
            stripped = value.strip()
            if stripped:
                return stripped
        elif isinstance(value, (int, float, bool)):
            return str(value)
    return None


def populate_historicals(
    model: FinancialModel,
    source: str,
    ticker: str,
    taxonomy: Dict[str, DataSourceMapping],
    most_recent_fy: int,
    n_historical: int = 5,
    fmp_data: Optional[Dict] = None,
    edgar_fetcher: Optional[EdgarFetcher] = None,
    historical_sources: HistoricalSources | None = None,
) -> PopulateStats:
    """Populate template historicals from legacy single-source or routed sources."""

    if historical_sources is not None:
        if str(source).lower() != "fmp":
            logging.debug(
                "populate_historicals: both source=%s and historical_sources passed; using routed",
                source,
            )
        return _populate_routed(
            model,
            historical_sources,
            ticker=ticker,
            taxonomy=taxonomy,
            most_recent_fy=most_recent_fy,
            n_historical=n_historical,
            fmp_data=fmp_data,
            edgar_fetcher=edgar_fetcher,
        )

    del most_recent_fy, n_historical
    source = str(source).lower()

    if source == "fmp":
        if fmp_data is None:
            raise ValueError("fmp_data is required when source='fmp'")
        return populate_from_fmp(model, fmp_data, taxonomy)
    if source == "edgar":
        if edgar_fetcher is None:
            raise ValueError("edgar_fetcher is required when source='edgar'")
        return populate_from_edgar(model, ticker, taxonomy, edgar_fetcher)
    if source == "both":
        raise ValueError("'both' mode not supported — use 'fmp' or 'edgar'")
    raise ValueError(f"Unsupported source: {source}")


def _validate_axis_key(axis_key: str, concept_id: str) -> None:
    if not isinstance(axis_key, str):
        raise ValueError(f"custom_concept {concept_id!r} axis_key must be a string")
    if "|" in axis_key:
        raise ValueError(
            f"custom_concept {concept_id!r} axis_key must contain exactly one axis/member "
            "pair; multi-axis '|' keys are not supported"
        )
    if axis_key.count("=") != 1:
        raise ValueError(
            f"custom_concept {concept_id!r} axis_key must have shape "
            "'axis_qname=member_qname'"
        )

    axis_qname, member_qname = axis_key.split("=", 1)
    if not axis_qname or not member_qname:
        raise ValueError(
            f"custom_concept {concept_id!r} axis_key must have non-empty axis and member"
        )
    if axis_qname in _EDGAR_AXIS_FAMILY_LABELS:
        raise ValueError(
            f"custom_concept {concept_id!r} axis_key axis {axis_qname!r} is a family "
            "label; expected an XBRL axis QName"
        )
    if not _AXIS_QNAME_RE.match(axis_qname):
        raise ValueError(
            f"custom_concept {concept_id!r} axis_key axis {axis_qname!r} must be an "
            "XBRL QName"
        )
    if not _AXIS_QNAME_RE.match(member_qname):
        raise ValueError(
            f"custom_concept {concept_id!r} axis_key member {member_qname!r} must be an "
            "XBRL QName"
        )


def _validate_inline_values(inline_values: dict, concept_id: str) -> None:
    if not isinstance(inline_values, dict):
        raise ValueError(f"custom_concept {concept_id!r} inline_values must be a dict")
    for year, value in inline_values.items():
        if not isinstance(year, str):
            raise ValueError(
                f"custom_concept {concept_id!r} inline_values keys must be fiscal-year strings"
            )
        try:
            int(year)
        except ValueError as exc:
            raise ValueError(
                f"custom_concept {concept_id!r} inline_values key {year!r} must be a "
                "fiscal-year string"
            ) from exc
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError(
                f"custom_concept {concept_id!r} inline_values[{year!r}] must be numeric"
            )


_CUSTOM_CONCEPT_CARRY_FORWARD_STRATEGIES = frozenset(
    {"carry_forward", "prior_period", "flat"}
)


def _apply_custom_concept_target_metadata(
    item: LineItem,
    concept_id: str,
    entry: dict[str, Any],
) -> None:
    existing_concept_id = item.data_concept_id
    allow_replace = (
        entry.get("replace_existing") is True
        or (entry.get("_meta") or {}).get("replace_existing") is True
    )
    if (
        item.id.startswith("tpl.fm.")
        and existing_concept_id
        and existing_concept_id != concept_id
        and not allow_replace
    ):
        raise ValueError(
            f"custom_concept {concept_id!r} cannot overwrite occupied financial-model "
            f"row {item.id!r} already mapped to {existing_concept_id!r}; use "
            "semantic_rows row_policy to bind or insert a reviewed semantic row"
        )
    item.data_concept_id = concept_id

    label = entry.get("target_label") or entry.get("label")
    if isinstance(label, str) and label.strip():
        item.label = label.strip()

    unit = entry.get("unit")
    if unit is not None:
        item.unit = Unit(str(unit))

    notes = entry.get("analyst_notes") or entry.get("notes")
    if isinstance(notes, str) and notes.strip():
        item.build_notes = notes.strip()

    strategy = entry.get("projection_strategy")
    if strategy is not None and strategy not in _CUSTOM_CONCEPT_CARRY_FORWARD_STRATEGIES:
        raise ValueError(
            f"custom_concept {concept_id!r} projection_strategy {strategy!r} is not supported"
        )
    if strategy in _CUSTOM_CONCEPT_CARRY_FORWARD_STRATEGIES:
        item.projected = FormulaSpec(
            type=FormulaType.ref,
            params={"source": LineItemRef(id=item.id, t=-1)},
        )


def _semantic_slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower())
    return slug.strip("_") or "row"


def _semantic_row_stable_id(ticker: str, concept_id: str, row_policy: dict[str, Any]) -> str:
    configured = row_policy.get("stable_item_id")
    if isinstance(configured, str) and configured.strip():
        return configured.strip()
    return f"fm.semantic.{ticker.lower()}.{_semantic_slug(concept_id)}"


def _locate_model_item(
    model: FinancialModel,
    item_id: str,
) -> tuple[str, Any, int, LineItem]:
    for sheet_name, sheet in model.sheets.items():
        for section in sheet.sections:
            for index, item in enumerate(section.line_items):
                if item.id == item_id:
                    return sheet_name, section, index, item
    raise KeyError(item_id)


def _item_has_any_real_historical_data(model: FinancialModel, item: LineItem) -> bool:
    for period in model.time_structure.historical_periods or model.time_structure.historical_years:
        if _item_has_real_data(item, int(period), model=model):
            return True
    return False


def _semantic_target_is_empty(model: FinancialModel, item: LineItem) -> bool:
    return not item.data_concept_id and not _item_has_any_real_historical_data(model, item)


def _semantic_append_note(item: LineItem, note: str) -> None:
    note = note.strip()
    if not note:
        return
    if item.build_notes and note not in item.build_notes:
        item.build_notes = f"{item.build_notes}\n{note}"
    elif not item.build_notes:
        item.build_notes = note


def _semantic_forecast_type(entry: dict[str, Any]) -> str | None:
    policy = entry.get("forecast_policy")
    if isinstance(policy, dict):
        policy_type = policy.get("type")
        if isinstance(policy_type, str) and policy_type.strip():
            return policy_type.strip()
    strategy = entry.get("projection_strategy")
    if isinstance(strategy, str) and strategy.strip():
        return strategy.strip()
    return None


def _apply_semantic_row_metadata(
    item: LineItem,
    *,
    concept_id: str,
    entry: dict[str, Any],
) -> None:
    item.data_concept_id = concept_id
    label = entry.get("target_label") or entry.get("label")
    if isinstance(label, str) and label.strip():
        item.label = label.strip()
    unit = entry.get("unit")
    if unit is not None:
        item.unit = Unit(str(unit))
    semantic_role = entry.get("semantic_role")
    forecast_policy = entry.get("forecast_policy")
    notes = entry.get("analyst_notes") or entry.get("notes")
    note_parts: list[str] = []
    if isinstance(notes, str) and notes.strip():
        note_parts.append(notes.strip())
    if isinstance(semantic_role, str) and semantic_role.strip():
        note_parts.append(f"Semantic role: {semantic_role.strip()}.")
    if isinstance(forecast_policy, dict):
        policy_type = forecast_policy.get("type")
        rationale = forecast_policy.get("rationale")
        if policy_type:
            note = f"Forecast policy: {policy_type}"
            if rationale:
                note += f" ({rationale})"
            note_parts.append(f"{note}.")
    for note in note_parts:
        _semantic_append_note(item, note)

    forecast_type = _semantic_forecast_type(entry)
    if forecast_type in _CUSTOM_CONCEPT_CARRY_FORWARD_STRATEGIES:
        item.projected = FormulaSpec(
            type=FormulaType.ref,
            params={"source": LineItemRef(id=item.id, t=-1)},
        )
    elif forecast_type in {None, ""}:
        return
    elif forecast_type == "zero":
        item.projected = FormulaSpec(type=FormulaType.constant, params={"value": 0})
    else:
        raise ValueError(
            f"semantic_row {concept_id!r} forecast_policy type {forecast_type!r} "
            "is not supported"
        )


def _insert_line_item_after(
    model: FinancialModel,
    *,
    anchor_item_id: str,
    new_item: LineItem,
) -> LineItem:
    if any(item.id == new_item.id for item in _iter_items(model)):
        raise ValueError(f"semantic row item_id {new_item.id!r} already exists")

    sheet_name, section, anchor_index, anchor = _locate_model_item(model, anchor_item_id)
    insert_row = int(anchor.row) + 1
    for sheet_item in (
        item
        for section_obj in model.sheets[sheet_name].sections
        for item in section_obj.line_items
    ):
        if int(sheet_item.row) >= insert_row:
            sheet_item.row = int(sheet_item.row) + 1
    new_item.row = insert_row
    section.line_items.insert(anchor_index + 1, new_item)
    model.build_index()
    return new_item


def _formula_contains_ref(obj: Any, item_id: str) -> bool:
    return item_id in _extract_ref_ids(obj)


def _insert_ref_after_in_obj(obj: Any, *, after_id: str, new_id: str) -> bool:
    if isinstance(obj, list):
        inserted = False
        index = 0
        while index < len(obj):
            value = obj[index]
            if _insert_ref_after_in_obj(value, after_id=after_id, new_id=new_id):
                inserted = True
            if isinstance(value, LineItemRef) and value.id == after_id:
                obj.insert(index + 1, LineItemRef(id=new_id))
                inserted = True
                index += 1
            index += 1
        return inserted
    if isinstance(obj, dict):
        inserted = False
        for value in obj.values():
            if _insert_ref_after_in_obj(value, after_id=after_id, new_id=new_id):
                inserted = True
        return inserted
    return False


def _add_formula_ref_after(
    model: FinancialModel,
    *,
    formula_item_id: str,
    after_id: str,
    new_id: str,
) -> bool:
    item = model.get_item(formula_item_id)
    inserted_any = False
    for spec in (item.historical, item.projected):
        if spec is None:
            continue
        if _formula_contains_ref(spec.params, new_id):
            inserted_any = True
            continue
        inserted_any = (
            _insert_ref_after_in_obj(spec.params, after_id=after_id, new_id=new_id)
            or inserted_any
        )
    return inserted_any


_BS_TOTAL_FORMULA_BY_SECTION = {
    "current_assets": "tpl.fm.balance_sheet.total_current_assets",
    "current_liabilities": "tpl.fm.balance_sheet.total_current_liabilities",
}


def _wire_semantic_bs_row(
    model: FinancialModel,
    *,
    section: str,
    row_policy: dict[str, Any],
    item_id: str,
) -> None:
    formula_item_id = _BS_TOTAL_FORMULA_BY_SECTION.get(section)
    if formula_item_id is None:
        return
    after_id = row_policy.get("formula_insert_after_item_id") or row_policy.get("insert_after_item_id")
    if not isinstance(after_id, str) or not after_id.strip():
        return
    if not _add_formula_ref_after(
        model,
        formula_item_id=formula_item_id,
        after_id=after_id.strip(),
        new_id=item_id,
    ):
        raise ValueError(
            f"semantic row {item_id!r} could not be wired into {formula_item_id!r} "
            f"after {after_id!r}"
        )


def _create_semantic_line_item(
    ticker: str,
    concept_id: str,
    entry: dict[str, Any],
    row_policy: dict[str, Any],
) -> LineItem:
    return LineItem(
        id=_semantic_row_stable_id(ticker, concept_id, row_policy),
        label=str(entry.get("target_label") or entry.get("label") or concept_id),
        row=0,
        item_type=ItemType.derived,
        unit=Unit(str(entry.get("unit") or "dollars")),
        data_concept_id=concept_id,
    )


def _semantic_source_custom_entry(
    concept_id: str,
    entry: dict[str, Any],
    item_id: str,
) -> dict[str, Any]:
    source = entry.get("source") if isinstance(entry.get("source"), dict) else {}
    payload: dict[str, Any] = {
        "concept_id": concept_id,
        "target_item_id": item_id,
        "target_label": entry.get("target_label") or entry.get("label"),
        "statement": entry.get("statement"),
        "unit": entry.get("unit", "dollars"),
        "notes": entry.get("notes"),
        "analyst_notes": entry.get("analyst_notes"),
        "_meta": {"source": "semantic_rows"},
    }
    if isinstance(source, dict):
        tags = source.get("xbrl_tags") or source.get("edgar_tags")
        if tags is not None:
            payload["edgar_tags"] = tags
        for key in (
            "preferred_source",
            "fmp_endpoint",
            "fmp_field",
            "registry_group_id",
            "canonical_tag",
            "axis_key",
            "inline_values",
        ):
            if key in source:
                payload[key] = source[key]
    forecast_type = _semantic_forecast_type(entry)
    if forecast_type in _CUSTOM_CONCEPT_CARRY_FORWARD_STRATEGIES:
        payload["projection_strategy"] = forecast_type
    return {key: value for key, value in payload.items() if value is not None}


def _bind_or_insert_semantic_row(
    model: FinancialModel,
    *,
    ticker: str,
    concept_id: str,
    entry: dict[str, Any],
    result: SemanticRowsResult,
) -> LineItem | None:
    row_policy = entry.get("row_policy")
    if not isinstance(row_policy, dict):
        raise ValueError(f"semantic_row {concept_id!r} missing row_policy object")
    mode = str(row_policy.get("mode") or "").strip()
    if not mode:
        raise ValueError(f"semantic_row {concept_id!r} missing row_policy.mode")

    stable_id = _semantic_row_stable_id(ticker, concept_id, row_policy)
    try:
        item = model.get_item(stable_id)
        _apply_semantic_row_metadata(item, concept_id=concept_id, entry=entry)
        result.materialized.append(
            {"concept_id": concept_id, "action": "bound_existing", "item_id": item.id}
        )
        return item
    except KeyError:
        pass

    for candidate in _iter_items(model):
        if candidate.data_concept_id == concept_id:
            _apply_semantic_row_metadata(candidate, concept_id=concept_id, entry=entry)
            result.materialized.append(
                {"concept_id": concept_id, "action": "bound_same_semantic", "item_id": candidate.id}
            )
            return candidate

    preferred_target_id = row_policy.get("preferred_target_item_id")
    preferred_item: LineItem | None = None
    if isinstance(preferred_target_id, str) and preferred_target_id.strip():
        try:
            preferred_item = model.get_item(preferred_target_id.strip())
        except KeyError:
            result.gaps.append(
                {
                    "concept_id": concept_id,
                    "kind": "preferred_target_missing",
                    "item_id": preferred_target_id,
                }
            )

    if preferred_item is not None:
        if mode in {"bind_if_empty", "bind_if_empty_or_insert"} and _semantic_target_is_empty(model, preferred_item):
            _apply_semantic_row_metadata(preferred_item, concept_id=concept_id, entry=entry)
            result.materialized.append(
                {"concept_id": concept_id, "action": "bound_empty", "item_id": preferred_item.id}
            )
            return preferred_item
        if mode == "bind_if_same_semantic" and preferred_item.data_concept_id == concept_id:
            _apply_semantic_row_metadata(preferred_item, concept_id=concept_id, entry=entry)
            result.materialized.append(
                {"concept_id": concept_id, "action": "bound_same_semantic", "item_id": preferred_item.id}
            )
            return preferred_item
        if mode in {"bind_if_empty", "bind_if_same_semantic"}:
            result.collisions.append(
                {
                    "concept_id": concept_id,
                    "item_id": preferred_item.id,
                    "existing_concept_id": preferred_item.data_concept_id,
                    "mode": mode,
                }
            )
            raise ValueError(
                f"semantic_row {concept_id!r} cannot bind occupied target "
                f"{preferred_item.id!r} mapped to {preferred_item.data_concept_id!r}"
            )

    if mode not in {"bind_if_empty_or_insert", "insert", "insert_or_bind_same_semantic"}:
        result.gaps.append({"concept_id": concept_id, "kind": "unsupported_row_policy", "mode": mode})
        raise ValueError(f"semantic_row {concept_id!r} unsupported row_policy.mode {mode!r}")

    insert_after_id = row_policy.get("insert_after_item_id")
    if not isinstance(insert_after_id, str) or not insert_after_id.strip():
        raise ValueError(f"semantic_row {concept_id!r} insert mode missing insert_after_item_id")
    item = _create_semantic_line_item(ticker, concept_id, entry, row_policy)
    _apply_semantic_row_metadata(item, concept_id=concept_id, entry=entry)
    _insert_line_item_after(model, anchor_item_id=insert_after_id.strip(), new_item=item)
    _wire_semantic_bs_row(
        model,
        section=str(entry.get("section") or ""),
        row_policy=row_policy,
        item_id=item.id,
    )
    result.materialized.append(
        {"concept_id": concept_id, "action": "inserted", "item_id": item.id}
    )
    return item


def _periods_for_delta_formula(model: FinancialModel) -> list[int]:
    historical = [int(period) for period in model.time_structure.historical_periods]
    projection = [int(period) for period in model.time_structure.projection_periods]
    return historical[1:] + projection


def _bind_or_insert_cf_linkage_row(
    model: FinancialModel,
    *,
    target_policy: dict[str, Any],
    default_id: str,
    label: str,
) -> LineItem:
    preferred_target_id = target_policy.get("preferred_target_item_id")
    if isinstance(preferred_target_id, str) and preferred_target_id.strip():
        item = model.get_item(preferred_target_id.strip())
        if item.data_concept_id not in (None, default_id):
            raise ValueError(
                f"cash-flow linkage target {item.id!r} is occupied by "
                f"{item.data_concept_id!r}"
            )
        item.label = label
        return item

    stable_id = str(target_policy.get("stable_item_id") or default_id)
    try:
        item = model.get_item(stable_id)
        item.label = label
        return item
    except KeyError:
        pass

    insert_after_id = target_policy.get("insert_after_item_id")
    if not isinstance(insert_after_id, str) or not insert_after_id.strip():
        raise ValueError("cash-flow linkage insert target missing insert_after_item_id")
    item = LineItem(
        id=stable_id,
        label=label,
        row=0,
        item_type=ItemType.derived,
        unit=Unit.dollars,
    )
    return _insert_line_item_after(model, anchor_item_id=insert_after_id.strip(), new_item=item)


def _apply_semantic_cash_flow_linkages(
    model: FinancialModel,
    *,
    ticker: str,
    overrides: TickerOverrides,
    materialized_by_concept: dict[str, str],
    result: SemanticRowsResult,
) -> None:
    for concept_id, entry in sorted((overrides.semantic_rows or {}).items()):
        source_item_id = materialized_by_concept.get(concept_id)
        if not source_item_id:
            continue
        linkage = entry.get("cash_flow_linkage")
        if not isinstance(linkage, dict):
            continue
        linkage_type = str(linkage.get("type") or "").strip()
        target_policy = linkage.get("target_row_policy")
        if not isinstance(target_policy, dict):
            target_policy = {}

        if linkage_type == "cash_bridge_adjustment":
            label = str(target_policy.get("target_label") or linkage.get("target_label") or entry.get("target_label") or concept_id)
            target = _bind_or_insert_cf_linkage_row(
                model,
                target_policy=target_policy,
                default_id=f"fm.semantic.{ticker.lower()}.{_semantic_slug(concept_id)}.cash_bridge",
                label=label,
            )
            target.historical = FormulaSpec(
                type=FormulaType.ref,
                params={"source": LineItemRef(id=source_item_id, t=0)},
            )
            target.projected = FormulaSpec(
                type=FormulaType.ref,
                params={"source": LineItemRef(id=source_item_id, t=0)},
            )
            target.formula_periods = sorted(
                {
                    int(period)
                    for period in (
                        list(model.time_structure.historical_periods)
                        + list(model.time_structure.projection_periods)
                    )
                }
            )
            _semantic_append_note(target, f"Semantic cash-flow linkage from {source_item_id}.")
            result.linkages.append(
                {
                    "concept_id": concept_id,
                    "type": linkage_type,
                    "item_id": target.id,
                    "source_item_id": source_item_id,
                }
            )
            continue

        if linkage_type == "financing_liability_delta":
            label = str(target_policy.get("target_label") or linkage.get("target_label") or "Net change in liability")
            target = _bind_or_insert_cf_linkage_row(
                model,
                target_policy=target_policy,
                default_id=f"fm.semantic.{ticker.lower()}.net_change_in_{_semantic_slug(concept_id)}",
                label=label,
            )
            target.historical = FormulaSpec(
                type=FormulaType.arithmetic,
                params={
                    "operands": [
                        "-",
                        LineItemRef(id=source_item_id, t=0),
                        LineItemRef(id=source_item_id, t=-1),
                    ]
                },
            )
            target.projected = target.historical.model_copy(deep=True)
            target.formula_periods = _periods_for_delta_formula(model)
            _semantic_append_note(
                target,
                f"Semantic financing liability delta from {source_item_id}; sign=current_minus_prior.",
            )
            insert_after = target_policy.get("formula_insert_after_item_id") or target_policy.get("insert_after_item_id")
            if not isinstance(insert_after, str) or not insert_after.strip():
                insert_after = "tpl.fm.cash_flow.other_cash_flows_from_financing"
            _add_formula_ref_after(
                model,
                formula_item_id="tpl.fm.cash_flow.financing_cash_flow",
                after_id=insert_after.strip(),
                new_id=target.id,
            )
            result.linkages.append(
                {
                    "concept_id": concept_id,
                    "type": linkage_type,
                    "item_id": target.id,
                    "source_item_id": source_item_id,
                }
            )
            continue

        result.gaps.append(
            {"concept_id": concept_id, "kind": "unsupported_cash_flow_linkage", "type": linkage_type}
        )


def _semantic_role(entry: dict[str, Any]) -> str:
    role = entry.get("semantic_role")
    if isinstance(role, str):
        return role.strip().lower()
    return ""


def _apply_semantic_valuation_linkages(
    model: FinancialModel,
    *,
    ticker: str,
    overrides: TickerOverrides,
    materialized_by_concept: dict[str, str],
    result: SemanticRowsResult,
) -> None:
    client_fund_obligation_id: str | None = None
    client_funds_asset_id: str | None = None
    for concept_id, entry in sorted((overrides.semantic_rows or {}).items()):
        if not isinstance(entry, dict):
            continue
        item_id = materialized_by_concept.get(concept_id)
        if not item_id:
            continue
        role = _semantic_role(entry)
        if role == "client_fund_obligation":
            client_fund_obligation_id = item_id
        elif role == "client_funds_asset":
            client_funds_asset_id = item_id

    adjustment_item_id = client_fund_obligation_id or client_funds_asset_id
    if not adjustment_item_id:
        return

    try:
        current_net_debt = model.get_item("tpl.v.current_valuation.net_debt")
        forward_net_cash = model.get_item("tpl.v.forward_ev_ebitda.net_debt_fy2")
    except KeyError as exc:
        result.gaps.append(
            {
                "concept_id": adjustment_item_id,
                "kind": "semantic_valuation_linkage_missing_target",
                "missing_item_id": str(exc),
            }
        )
        return

    current_net_debt.projected = FormulaSpec(
        type=FormulaType.arithmetic,
        params={
            "operands": [
                "-",
                LineItemRef(id=adjustment_item_id),
                LineItemRef(id="tpl.fm.balance_sheet.net_cash"),
            ]
        },
    )
    _semantic_append_note(
        current_net_debt,
        (
            "Semantic valuation linkage: client-funds cash is not free cash; "
            f"net debt adds back {adjustment_item_id}."
        ),
    )

    forward_net_cash.projected = FormulaSpec(
        type=FormulaType.arithmetic,
        params={
            "operands": [
                "-",
                LineItemRef(id="tpl.fm.balance_sheet.net_cash"),
                LineItemRef(id=adjustment_item_id),
            ]
        },
    )
    _semantic_append_note(
        forward_net_cash,
        (
            "Semantic valuation linkage: implied equity bridge uses net cash "
            f"after client-funds adjustment from {adjustment_item_id}."
        ),
    )
    result.linkages.append(
        {
            "concept_id": adjustment_item_id,
            "type": "valuation_client_funds_net_debt_adjustment",
            "item_id": "tpl.v.current_valuation.net_debt",
            "source_item_id": adjustment_item_id,
            "ticker": ticker,
        }
    )


def _materialize_semantic_rows(
    model: FinancialModel,
    ticker: str,
    overrides: TickerOverrides,
) -> SemanticRowsResult:
    result = SemanticRowsResult()
    materialized_by_concept: dict[str, str] = {}
    for concept_id, entry in sorted((overrides.semantic_rows or {}).items()):
        if not isinstance(entry, dict):
            raise ValueError(f"semantic_row {concept_id!r} must be an object")
        if (entry.get("_meta") or {}).get("disabled") is True:
            continue
        statement = str(entry.get("statement") or "")
        if statement != "balance_sheet":
            result.gaps.append(
                {"concept_id": concept_id, "kind": "unsupported_statement", "statement": statement}
            )
            continue
        item = _bind_or_insert_semantic_row(
            model,
            ticker=ticker,
            concept_id=concept_id,
            entry=entry,
            result=result,
        )
        if item is None:
            continue
        materialized_by_concept[concept_id] = item.id
        result.source_custom_concepts[concept_id] = _semantic_source_custom_entry(
            concept_id,
            entry,
            item.id,
        )

    _apply_semantic_cash_flow_linkages(
        model,
        ticker=ticker,
        overrides=overrides,
        materialized_by_concept=materialized_by_concept,
        result=result,
    )
    _apply_semantic_valuation_linkages(
        model,
        ticker=ticker,
        overrides=overrides,
        materialized_by_concept=materialized_by_concept,
        result=result,
    )
    return result


def _populate_custom_concepts(
    model: FinancialModel,
    ticker: str,
    overrides: TickerOverrides,
    source: str,
    edgar_fetcher: EdgarFetcher | None,
    fmp_data: dict | None,
    historical_sources: HistoricalSources | None,
    business_model: "BusinessModel | None",
    most_recent_fy: int,
    n_historical: int,
) -> int:
    """Fetch + populate custom_concepts into their target rows."""

    if (
        historical_sources is not None
        and historical_sources.overrides
        and overrides.custom_concepts
    ):
        raise NotImplementedError(
            "custom_concepts under routed builds with per-concept overrides "
            "(historical_sources.overrides) not supported in v1"
        )

    base_effective_source = (
        historical_sources.default_source if historical_sources is not None else source
    )
    base_effective_source = str(base_effective_source).lower()
    populated = 0
    historical_periods = list(range(most_recent_fy - n_historical + 1, most_recent_fy + 1))

    for concept_id, entry in sorted(overrides.custom_concepts.items()):
        meta = entry.get("_meta") or {}
        if meta.get("disabled") is True:
            continue

        target_id = entry.get("target_item_id")
        if not target_id:
            logging.warning("custom_concept %r missing target_item_id; skipping", concept_id)
            continue

        try:
            item = model.get_item(target_id)
        except KeyError:
            if target_id.startswith("bm.") and business_model is not None:
                raise KeyError(
                    f"custom_concept {concept_id!r} targets BM row {target_id!r} "
                    "which the compiler did not emit"
                )
            logging.warning(
                "custom_concept %r targets missing item %r; skipping",
                concept_id,
                target_id,
            )
            continue

        dsm_fields = {key: value for key, value in entry.items() if key in _DSM_FIELD_NAMES}
        dsm_fields["concept_id"] = concept_id
        mapping = DataSourceMapping.model_validate(dsm_fields)

        if mapping.registry_group_id:
            logging.warning(
                "custom_concept %r uses registry_group_id; not supported in v1, skipping",
                concept_id,
            )
            continue

        target_id = str(target_id)
        effective_source = base_effective_source
        is_bm_generated_concept = (
            business_model is not None
            and target_id.startswith("bm.")
            and meta.get("source") == "f2h"
            and bool(mapping.edgar_tags)
        )
        if is_bm_generated_concept:
            effective_source = "edgar"
        elif mapping.preferred_source:
            effective_source = str(mapping.preferred_source).lower()

        axis_key = entry.get("axis_key")
        inline_values = entry.get("inline_values")
        if axis_key is not None:
            _validate_axis_key(axis_key, concept_id)
        if inline_values is not None:
            _validate_inline_values(inline_values, concept_id)

        _apply_custom_concept_target_metadata(item, concept_id, entry)

        values_by_year: dict[int, float] = {}
        path_taken: str | None = None
        path_a_eligible = (
            effective_source == "edgar"
            and edgar_fetcher is not None
            and axis_key is not None
            and getattr(edgar_fetcher, "supports_axis_filter", False)
            and bool(mapping.edgar_tags)
        )

        if path_a_eligible:
            fetch_result = _fetch_dimensional_edgar_concept(
                ticker=ticker,
                concept_id=concept_id,
                concept=mapping,
                axis_key=axis_key,
                most_recent_fy=most_recent_fy,
                n_historical=n_historical,
                edgar_fetcher=edgar_fetcher,
                include_equivalents=is_bm_generated_concept,
                allow_equivalent_tags=is_bm_generated_concept,
                include_local_tag_candidates=is_bm_generated_concept,
            )
            values_by_year = dict(fetch_result.values_dict)
            path_taken = "path_a"
            if not values_by_year and inline_values is not None:
                logging.warning(
                    "custom_concept %r Path A (axis_key=%r) returned no values "
                    "(status=%s, periods_failed=%d); falling back to inline_values",
                    concept_id,
                    axis_key,
                    fetch_result.status,
                    fetch_result.periods_failed,
                )
                path_taken = None

        if path_taken is None and effective_source == "edgar" and inline_values is not None:
            for year_str, value in inline_values.items():
                year = int(year_str)
                if year in historical_periods:
                    values_by_year[year] = float(value)
            path_taken = "path_b"
        elif (
            path_taken is None
            and effective_source == "edgar"
            and edgar_fetcher is not None
            and mapping.edgar_tags
        ):
            if meta.get("dimensional_intent") is True or axis_key is not None:
                logging.warning(
                    "custom_concept %r has dimensional intent (marker=%s, axis_key=%r) "
                    "but no Path A/B values available; refusing v1 unfiltered EDGAR "
                    "fallback (would write aggregate values). Re-bridge or supply "
                    "inline_values.",
                    concept_id,
                    meta.get("dimensional_intent"),
                    axis_key,
                )
                continue
            fetch_result = _fetch_legacy_edgar_concept(
                ticker=ticker,
                concept_id=concept_id,
                concept=mapping,
                most_recent_fy=most_recent_fy,
                n_historical=n_historical,
                edgar_fetcher=edgar_fetcher,
                include_equivalents=is_bm_generated_concept,
                allow_equivalent_tags=is_bm_generated_concept,
                include_local_tag_candidates=is_bm_generated_concept,
            )
            values_by_year = dict(fetch_result.values_dict)
            path_taken = "v1_edgar"
        elif (
            path_taken is None
            and effective_source == "fmp"
            and fmp_data is not None
            and mapping.fmp_field
            and mapping.fmp_endpoint
        ):
            fmp_lookup = _build_fmp_lookup(fmp_data)
            for year in historical_periods:
                record = fmp_lookup.get(mapping.fmp_endpoint, {}).get(year)
                raw = record.get(mapping.fmp_field) if record else None
                if raw is not None:
                    values_by_year[year] = _scale_fmp_value(concept_id, raw, concept=mapping)
            path_taken = "v1_fmp"

        if path_taken in ("path_a", "path_b", "v1_edgar"):
            provenance = ValueProvenance.imported_edgar
        elif path_taken == "v1_fmp":
            provenance = ValueProvenance.imported_fmp
        else:
            continue
        for year in historical_periods:
            if year not in values_by_year:
                continue
            value = values_by_year[year]
            if item.historical is None:
                _set_imported_value(item, year, value, provenance=provenance)
            else:
                _set_constant_override(item, year, value)
            populated += 1

    return populated


_SEGMENT_REVENUE_KPI_SOURCE_TAGS = frozenset({
    "revenuefromcontractwithcustomerexcludingassessedtax",
    "revenues",
    "salesrevenuegoodsnet",
    "salesrevenuenet",
})


def _bm_segment_snapshot_inline_values(
    business_model: "BusinessModel | None",
    segment_config: SegmentConfig | None,
) -> dict[str, dict[str, float]]:
    if business_model is None or segment_config is None:
        return {}
    snapshot = segment_config.segment_profile_snapshot
    axis = segment_config.axis or snapshot.axis_used
    if not axis:
        return {}

    snapshot_by_member = {
        str(segment.edgar_member or "").strip(): segment
        for segment in snapshot.segments
        if str(segment.edgar_member or "").strip()
    }
    if not snapshot_by_member:
        return {}

    inline_values: dict[str, dict[str, float]] = {}
    for segment in business_model.segments:
        if not (segment.edgar_axis and segment.edgar_member):
            continue
        if not _tags_equivalent(str(segment.edgar_axis), str(axis)):
            continue
        snapshot_segment = next(
            (
                candidate
                for member, candidate in snapshot_by_member.items()
                if _tags_equivalent(member, str(segment.edgar_member))
            ),
            None,
        )
        if snapshot_segment is None or not snapshot_segment.revenue_observations:
            continue
        for node in _iter_business_model_nodes(segment.revenue_model.decomposition):
            if not _is_segment_revenue_kpi_node(node):
                continue
            inline_values[f"{segment.id}:{node.id}"] = {
                str(int(year)): float(value)
                for year, value in sorted(
                    revenue_observations_to_values(snapshot_segment.revenue_observations).items()
                )
                if value is not None and math.isfinite(float(value))
            }
    return {
        key: values
        for key, values in inline_values.items()
        if values
    }


def _bm_revenue_share_inline_values(
    business_model: "BusinessModel | None",
    *,
    fmp_data: dict | None,
    most_recent_fy: int,
    n_historical: int,
) -> dict[str, dict[str, float]]:
    if business_model is None or not isinstance(fmp_data, dict):
        return {}

    total_revenue_by_year = _fmp_total_revenue_by_year(
        fmp_data,
        most_recent_fy=most_recent_fy,
        n_historical=n_historical,
    )
    if not total_revenue_by_year:
        return {}

    inline_values: dict[str, dict[str, float]] = {}
    for segment in business_model.segments:
        try:
            share = float(segment.revenue_share)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(share) or share <= 0:
            continue
        for node in _iter_business_model_nodes(segment.revenue_model.decomposition):
            if not _is_segment_revenue_kpi_node(node):
                continue
            inline_values[f"{segment.id}:{node.id}"] = {
                str(year): float(total_revenue) * share
                for year, total_revenue in sorted(total_revenue_by_year.items())
            }

    return inline_values


def _fmp_total_revenue_by_year(
    fmp_data: dict,
    *,
    most_recent_fy: int,
    n_historical: int,
) -> dict[int, float]:
    lookup = _build_fmp_lookup(fmp_data)
    income_statement = lookup.get("income_statement") or {}
    if not income_statement:
        return {}

    allowed_years = set(range(int(most_recent_fy) - int(n_historical) + 1, int(most_recent_fy) + 1))
    revenue_fields = ("revenue", "totalRevenue", "total_revenue")
    values: dict[int, float] = {}
    for year, record in sorted(income_statement.items()):
        if year not in allowed_years:
            continue
        raw_value = next((record.get(field) for field in revenue_fields if record.get(field) is not None), None)
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue
        if not math.isfinite(value):
            continue
        values[int(year)] = value / 1_000_000.0
    return values


def _iter_business_model_nodes(nodes: list[Any]):
    for node in nodes:
        yield node
        yield from _iter_business_model_nodes(getattr(node, "children", None) or [])


def _is_segment_revenue_kpi_node(node: Any) -> bool:
    if str(getattr(node, "id", "") or "") != "segment_revenue":
        return False
    if not bool(getattr(node, "kpi", False)):
        return False
    source = str(getattr(node, "kpi_source", "") or "").rsplit(":", 1)[-1].lower()
    return source in _SEGMENT_REVENUE_KPI_SOURCE_TAGS


def _populate_routed(
    model: FinancialModel,
    historical_sources: HistoricalSources,
    *,
    ticker: str,
    taxonomy: Dict[str, DataSourceMapping],
    most_recent_fy: int,
    n_historical: int,
    fmp_data: Optional[Dict],
    edgar_fetcher: Optional[EdgarFetcher],
) -> PopulateStats:
    routes: dict[str, ConceptSourceRoute] = {}
    explicit_overrides = {override.concept_id for override in historical_sources.overrides}

    for item in _iter_items(model):
        concept_id = item.data_concept_id
        if not concept_id or concept_id in routes:
            continue
        taxonomy_concept = taxonomy.get(concept_id)
        route = resolve_source_for_concept(concept_id, historical_sources, taxonomy_concept)
        validate_route_eligibility(
            route,
            taxonomy_concept,
            is_explicit_override=concept_id in explicit_overrides,
        )
        routes[concept_id] = route

    required_sources = {
        source
        for route in routes.values()
        for source in route.fallback_order
    }

    fmp_buffer: dict[str, FmpConceptFetchResult] = {}
    edgar_buffer: dict[str, EdgarConceptFetchResult] = {}

    if "fmp" in required_sources:
        if fmp_data is None:
            raise ValueError("fmp_data is required when routed historical_sources can use FMP")
        fmp_concepts = {
            concept_id
            for concept_id, route in routes.items()
            if "fmp" in route.fallback_order
        }
        fmp_buffer = _fetch_fmp_concept_buffer(
            fmp_data,
            fmp_concepts,
            taxonomy,
            [int(period) for period in model.time_structure.historical_periods],
        )

    if "edgar" in required_sources:
        if edgar_fetcher is None:
            raise ValueError("edgar_fetcher is required when routed historical_sources can use EDGAR")
        edgar_concepts = {
            concept_id
            for concept_id, route in routes.items()
            if "edgar" in route.fallback_order
        }
        edgar_buffer = _fetch_edgar_concept_buffer(
            ticker,
            edgar_concepts,
            taxonomy,
            [int(period) for period in model.time_structure.historical_periods],
            edgar_fetcher,
        )
        edgar_fetched_count = len(
            [
                concept_id
                for concept_id in edgar_concepts
                if _concept_can_fetch_edgar(taxonomy.get(concept_id))
            ]
        )
        edgar_failed_count = len(
            [
                result
                for result in edgar_buffer.values()
                if result.status == "failed"
            ]
        )
        if edgar_fetched_count > 0 and edgar_failed_count == edgar_fetched_count:
            raise RuntimeError(_all_edgar_concepts_failed_message(edgar_buffer.values()))

    return _write_routed_historicals(
        model,
        routes,
        fmp_buffer,
        edgar_buffer,
        taxonomy,
    )


def _concept_can_fetch_edgar(concept: DataSourceMapping | None) -> bool:
    return bool(concept is not None and (concept.edgar_tags or concept.registry_group_id))


def _all_edgar_concepts_failed_message(results: Iterable[EdgarConceptFetchResult]) -> str:
    message = "EDGAR API returned errors for all concepts — check auth/connectivity"
    details = sorted(
        {
            str(result.error_message).strip()
            for result in results
            if result.status == "failed" and result.error_message
        }
    )
    if details:
        return f"{message}: {'; '.join(details[:3])}"
    return message


def _empty_fmp_concept_result(concept_id: str) -> FmpConceptFetchResult:
    return FmpConceptFetchResult(
        concept_id=concept_id,
        values={},
        field_used_by_year={},
        fallback_field_years=set(),
        missing=True,
    )


def _empty_edgar_concept_result(
    *,
    status: str = "missing",
    historical_periods: list[int] | None = None,
) -> EdgarConceptFetchResult:
    return EdgarConceptFetchResult(
        values_dict={},
        failed_years=set(historical_periods or []) if status == "failed" else set(),
        status=status,
        periods_failed=len(historical_periods or []) if status == "failed" else 0,
        api_calls=0,
    )


def _fetch_fmp_concept_buffer(
    fmp_data: Dict,
    concept_ids: set[str],
    taxonomy: Dict[str, DataSourceMapping],
    historical_periods: list[int],
) -> dict[str, FmpConceptFetchResult]:
    fmp_lookup = _build_fmp_lookup(fmp_data or {})
    buffer: dict[str, FmpConceptFetchResult] = {}

    for concept_id in concept_ids:
        concept = taxonomy.get(concept_id)
        values: dict[int, float] = {}
        field_used_by_year: dict[int, str] = {}
        fallback_field_years: set[int] = set()
        reported_period_ends_by_year: dict[int, str] = {}

        if concept is None or not concept.fmp_endpoint or not concept.fmp_field:
            buffer[concept_id] = FmpConceptFetchResult(concept_id, {}, {}, set(), True)
            continue

        for year in historical_periods:
            record = fmp_lookup.get(concept.fmp_endpoint, {}).get(year)
            raw_value, field_used, fallback_used = _raw_fmp_value_for_concept(
                concept,
                record,
            )
            if fallback_used:
                fallback_field_years.add(year)

            if raw_value is None:
                continue

            values[year] = _scale_fmp_value(concept_id, raw_value, concept=concept)
            field_used_by_year[year] = field_used
            reported_period_end = _reported_period_end_value(record.get("date") if record else None)
            if reported_period_end is not None:
                reported_period_ends_by_year[year] = reported_period_end

        buffer[concept_id] = FmpConceptFetchResult(
            concept_id=concept_id,
            values=values,
            field_used_by_year=field_used_by_year,
            fallback_field_years=fallback_field_years,
            missing=not values,
            reported_period_ends_by_year=reported_period_ends_by_year,
        )

    return buffer


def _fetch_validation_fmp_concept_buffer(
    fmp_data: Optional[Dict],
    concept_ids: set[str],
    taxonomy: Dict[str, DataSourceMapping],
    historical_periods: list[int],
) -> dict[str, FmpConceptFetchResult]:
    buffer: dict[str, FmpConceptFetchResult] = {}
    for concept_id in sorted(concept_ids):
        try:
            buffer.update(
                _fetch_fmp_concept_buffer(
                    fmp_data or {},
                    {concept_id},
                    taxonomy,
                    historical_periods,
                )
            )
        except Exception as exc:
            logging.warning(
                "Cross-source validation FMP extraction failed for concept '%s': %s",
                concept_id,
                exc,
            )
            buffer[concept_id] = _empty_fmp_concept_result(concept_id)
    return buffer


def _fetch_edgar_concept_buffer(
    ticker: str,
    concept_ids: set[str],
    taxonomy: Dict[str, DataSourceMapping],
    historical_periods: list[int],
    edgar_fetcher: EdgarFetcher,
) -> dict[str, EdgarConceptFetchResult]:
    if not historical_periods:
        return {}

    most_recent_fy = max(historical_periods)
    n_historical = len(historical_periods)
    buffer: dict[str, EdgarConceptFetchResult] = {}

    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        futures = {}
        for concept_id in concept_ids:
            concept = taxonomy.get(concept_id)
            if concept is None or not (concept.edgar_tags or concept.registry_group_id):
                buffer[concept_id] = EdgarConceptFetchResult(
                    values_dict={},
                    failed_years=set(),
                    status="missing",
                    periods_failed=0,
                    api_calls=0,
                )
                continue
            futures[
                executor.submit(
                    _fetch_edgar_concept_result,
                    ticker=ticker,
                    concept_id=concept_id,
                    concept=concept,
                    most_recent_fy=most_recent_fy,
                    n_historical=n_historical,
                    edgar_fetcher=edgar_fetcher,
                )
            ] = concept_id

        for future in concurrent.futures.as_completed(futures):
            buffer[futures[future]] = future.result()

    return buffer


def _fetch_validation_edgar_concept_buffer(
    ticker: str,
    concept_ids: set[str],
    taxonomy: Dict[str, DataSourceMapping],
    historical_periods: list[int],
    edgar_fetcher: Optional[EdgarFetcher],
) -> dict[str, EdgarConceptFetchResult]:
    buffer: dict[str, EdgarConceptFetchResult] = {}
    for concept_id in sorted(concept_ids):
        if edgar_fetcher is None:
            logging.warning(
                "Cross-source validation EDGAR fetch skipped for concept '%s': no fetcher",
                concept_id,
            )
            buffer[concept_id] = _empty_edgar_concept_result()
            continue
        try:
            buffer.update(
                _fetch_edgar_concept_buffer(
                    ticker,
                    {concept_id},
                    taxonomy,
                    historical_periods,
                    edgar_fetcher,
                )
            )
        except Exception as exc:
            logging.warning(
                "Cross-source validation EDGAR fetch failed for concept '%s': %s",
                concept_id,
                exc,
            )
            buffer[concept_id] = _empty_edgar_concept_result(
                status="failed",
                historical_periods=historical_periods,
            )
    return buffer


def _refresh_period_metadata(
    model: FinancialModel,
    reported_period_ends: dict[int, str],
) -> None:
    model.time_structure.period_metadata = build_period_metadata(
        model.time_structure,
        reported_period_ends=reported_period_ends,
    )


def _collect_fmp_reported_period_ends_from_lookup(
    fmp_lookup: Dict[str, Dict[int, Dict]],
    historical_periods: Iterable[int],
) -> dict[int, str]:
    reported: dict[int, str] = {}
    period_set = {int(period) for period in historical_periods}
    for endpoint in sorted(fmp_lookup):
        records_by_year = fmp_lookup.get(endpoint, {})
        for period in sorted(period_set):
            record = records_by_year.get(period)
            if record is None:
                continue
            _record_reported_period_end(
                reported,
                period,
                record.get("date"),
                source=f"fmp:{endpoint}",
            )
    return reported


def _collect_edgar_reported_period_ends(
    results: Iterable[EdgarConceptFetchResult],
    historical_periods: Iterable[int],
) -> dict[int, str]:
    reported: dict[int, str] = {}
    period_set = {int(period) for period in historical_periods}
    for result in results:
        for period, reported_period_end in result.reported_period_ends_by_year.items():
            if int(period) not in period_set or int(period) not in result.values_dict:
                continue
            _record_reported_period_end(
                reported,
                int(period),
                reported_period_end,
                source="edgar",
            )
    return reported


def _collect_routed_reported_period_ends(
    fmp_buffer: dict[str, FmpConceptFetchResult],
    edgar_buffer: dict[str, EdgarConceptFetchResult],
    source_by_concept_year: dict[str, dict[int, BuildSource | None]],
    historical_periods: Iterable[int],
) -> dict[int, str]:
    reported: dict[int, str] = {}
    period_set = {int(period) for period in historical_periods}
    for concept_id, source_by_year in source_by_concept_year.items():
        for period, source in source_by_year.items():
            period = int(period)
            if period not in period_set or source is None:
                continue
            if source == "fmp":
                result = fmp_buffer.get(concept_id)
            else:
                result = edgar_buffer.get(concept_id)
            if result is None:
                continue
            _record_reported_period_end(
                reported,
                period,
                result.reported_period_ends_by_year.get(period),
                source=f"{source}:{concept_id}",
            )
    return reported


def _record_reported_period_end(
    reported: dict[int, str],
    period: int,
    value: object,
    *,
    source: str,
) -> None:
    reported_period_end = _reported_period_end_value(value)
    if reported_period_end is None:
        return
    existing = reported.get(int(period))
    if existing is None:
        reported[int(period)] = reported_period_end
    elif existing != reported_period_end:
        logging.warning(
            "reported_period_end_conflict period=%s existing=%s incoming=%s "
            "source=%s; keeping existing",
            period,
            existing,
            reported_period_end,
            source,
        )


def _reported_period_end_value(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.date().isoformat()
    if isinstance(value, date):
        return value.isoformat()
    raw = str(value).strip()
    if not raw or raw.lower() in {"none", "nan", "nat"}:
        return None
    candidate = raw[:10] if len(raw) >= 10 else raw
    try:
        return date.fromisoformat(candidate).isoformat()
    except ValueError:
        return None


def _entry_reported_period_end(entry: dict) -> str | None:
    for key in ("period_end", "calendar_end", "end_current"):
        reported_period_end = _reported_period_end_value(entry.get(key))
        if reported_period_end is not None:
            return reported_period_end
    return None


def _validation_opt_in_concepts(taxonomy: Dict[str, DataSourceMapping]) -> set[str]:
    return {
        concept_id
        for concept_id, mapping in taxonomy.items()
        if mapping.validation_tolerance_pct is not None
    }


def _make_validation_input(
    *,
    ticker: str,
    taxonomy: Dict[str, DataSourceMapping],
    historical_periods: list[int],
    fmp_data: Optional[Dict],
    edgar_fetcher: Optional[EdgarFetcher],
    stats: PopulateStats,
) -> ValidationInput:
    opted_in_concepts = _validation_opt_in_concepts(taxonomy)
    if not opted_in_concepts:
        return ValidationInput(
            opted_in_concepts=[],
            historical_years=historical_periods,
            served_source_by_concept_year=dict(stats.served_source_by_concept_year or {}),
        )

    fmp_buffer = _fetch_validation_fmp_concept_buffer(
        fmp_data,
        opted_in_concepts,
        taxonomy,
        historical_periods,
    )
    edgar_buffer = _fetch_validation_edgar_concept_buffer(
        ticker,
        opted_in_concepts,
        taxonomy,
        historical_periods,
        edgar_fetcher,
    )

    return ValidationInput(
        fmp_buffer=fmp_buffer,
        edgar_buffer=edgar_buffer,
        opted_in_concepts=sorted(opted_in_concepts),
        historical_years=historical_periods,
        served_source_by_concept_year=dict(stats.served_source_by_concept_year or {}),
    )


def _record_fmp_quality_observation(
    observations: Dict[tuple[str, str, str], Dict[int, float]],
    concept: DataSourceMapping,
    field_used: str | None,
    year: int,
    value: float,
) -> None:
    endpoint = concept.fmp_endpoint
    if not endpoint or not field_used:
        return
    if (endpoint, field_used) not in _FMP_QUALITY_BUCKET_FIELDS:
        return
    try:
        numeric_value = float(value)
    except (TypeError, ValueError):
        return
    observations.setdefault(
        (str(concept.concept_id), str(endpoint), str(field_used)),
        {},
    )[int(year)] = numeric_value


def _fmp_quality_warnings_from_observations(
    observations: Dict[tuple[str, str, str], Dict[int, float]],
) -> List[Dict[str, object]]:
    warnings: List[Dict[str, object]] = []
    for (concept_id, endpoint, fmp_field), values_by_year in sorted(observations.items()):
        years = sorted(values_by_year)
        for prior_year, year in zip(years, years[1:]):
            if int(year) != int(prior_year) + 1:
                continue
            prior_value = values_by_year[prior_year]
            value = values_by_year[year]
            delta = value - prior_value
            if abs(delta) < _FMP_QUALITY_ABS_DELTA_M:
                continue

            prior_abs = abs(prior_value)
            ratio = None if prior_abs < 1e-9 else abs(value) / prior_abs
            if ratio is None:
                extreme_change = abs(value) >= _FMP_QUALITY_ABS_DELTA_M
            else:
                extreme_change = (
                    ratio >= _FMP_QUALITY_YOY_RATIO_THRESHOLD
                    or ratio <= 1.0 / _FMP_QUALITY_YOY_RATIO_THRESHOLD
                )
            if not extreme_change:
                continue

            severity = "warning"
            classification = None
            if (
                endpoint == "cash_flow"
                and concept_id in _FMP_QUALITY_INFORMATIONAL_BUCKET_CONCEPTS
            ):
                severity = "info"
                classification = "broad_cash_flow_bucket_reclassification"

            warning: Dict[str, object] = {
                "kind": "fmp_bucket_yoy_jump",
                "severity": severity,
                "concept_id": concept_id,
                "endpoint": endpoint,
                "field": fmp_field,
                "prior_year": int(prior_year),
                "year": int(year),
                "prior_value": prior_value,
                "value": value,
                "delta": delta,
                "ratio": ratio,
                "ratio_threshold": _FMP_QUALITY_YOY_RATIO_THRESHOLD,
                "abs_delta_threshold": _FMP_QUALITY_ABS_DELTA_M,
            }
            if classification is not None:
                warning["classification"] = classification
            warnings.append(
                warning
            )
    return warnings


def _write_routed_historicals(
    model: FinancialModel,
    routes: dict[str, ConceptSourceRoute],
    fmp_buffer: dict[str, FmpConceptFetchResult],
    edgar_buffer: dict[str, EdgarConceptFetchResult],
    taxonomy: Dict[str, DataSourceMapping],
) -> PopulateStats:
    historical_periods = [int(period) for period in model.time_structure.historical_periods]
    missing_concepts: set[str] = set()
    edgar_errors: set[str] = set()
    edgar_partial_failures: set[str] = set()
    source_resolution: list[SourceResolutionEntry] = []
    year_source_by_concept: dict[str, dict[int, BuildSource | None]] = {}
    fmp_quality_observations: Dict[tuple[str, str, str], Dict[int, float]] = {}
    items_populated = 0
    items_skipped = 0
    periods_populated = 0
    is_overlay = _has_existing_imported_historicals(model, historical_periods)

    for concept_id, result in edgar_buffer.items():
        if result.status == "failed":
            edgar_errors.add(concept_id)
        elif result.status == "ok" and result.periods_failed > 0:
            edgar_partial_failures.add(concept_id)

    for item in _iter_items(model):
        concept_id = item.data_concept_id
        if not concept_id:
            continue

        if concept_id not in taxonomy:
            items_skipped += 1
            continue

        route = routes.get(concept_id)
        if route is None:
            items_skipped += 1
            continue

        route_touches_edgar = "edgar" in route.fallback_order
        item_has_actuals = False
        for year in historical_periods:
            year_source: BuildSource | None = None
            value: float | None = None
            edgar_provenance: EdgarProvenance | None = None
            fmp_provenance: FmpProvenance | None = None
            attempted_edgar_failed_for_year = False

            for candidate_source in route.fallback_order:
                if candidate_source == "fmp":
                    result = fmp_buffer.get(concept_id)
                    if result is not None and year in result.values:
                        year_source = "fmp"
                        value = result.values[year]
                        concept = taxonomy.get(concept_id)
                        field_used = result.field_used_by_year.get(year)
                        if concept is not None and field_used:
                            fmp_provenance = _make_fmp_provenance(concept, field_used)
                            _record_fmp_quality_observation(
                                fmp_quality_observations,
                                concept,
                                field_used,
                                year,
                                value,
                            )
                        break
                    continue

                result = edgar_buffer.get(concept_id)
                if result is not None:
                    if result.status == "failed" or year in result.failed_years:
                        attempted_edgar_failed_for_year = True
                    if year in result.values_dict:
                        year_source = "edgar"
                        value = result.values_dict[year]
                        edgar_provenance = result.provenance_by_year.get(year)
                        break

            if year_source is None or value is None:
                missing_concepts.add(concept_id)
                if (
                    item.historical is not None
                    and not attempted_edgar_failed_for_year
                    and (not is_overlay or not route_touches_edgar)
                ):
                    _set_constant_override(item, year, 0, synthetic=True)
                year_source_by_concept.setdefault(concept_id, {}).setdefault(year, None)
                continue

            year_source_by_concept.setdefault(concept_id, {}).setdefault(year, year_source)

            provenance = (
                ValueProvenance.imported_edgar
                if year_source == "edgar"
                else ValueProvenance.imported_fmp
            )

            if item.historical is None:
                _set_imported_value(
                    item,
                    year,
                    value,
                    provenance=provenance,
                    edgar_provenance=edgar_provenance,
                    fmp_provenance=fmp_provenance,
                )
            else:
                _set_constant_override(
                    item,
                    year,
                    value,
                    edgar_provenance=edgar_provenance,
                    fmp_provenance=fmp_provenance,
                )

            item_has_actuals = True
            periods_populated += 1

        if item_has_actuals:
            items_populated += 1
        else:
            items_skipped += 1

    served_by_breakdown: dict[str, ServedByBreakdown] = {}
    fallback_engaged_concepts: list[str] = []
    fallback_engaged_cells = 0

    for concept_id, route in routes.items():
        breakdown = ServedByBreakdown(primary_source=route.primary)
        source_by_year = year_source_by_concept.get(concept_id, {})
        for year in historical_periods:
            year_source = source_by_year.get(year)
            if year_source is None:
                breakdown.years_unserved.append(year)
            elif year_source == route.primary:
                breakdown.years_via_primary.append(year)
            else:
                breakdown.years_via_fallback.append(year)

        if breakdown.years_via_fallback:
            fallback_engaged_concepts.append(concept_id)
            fallback_engaged_cells += len(breakdown.years_via_fallback)

        served_by_breakdown[concept_id] = breakdown
        served_by: BuildSource | None = None
        if breakdown.years_via_primary:
            served_by = route.primary
        elif breakdown.years_via_fallback:
            for candidate_source in route.fallback_order:
                if candidate_source != route.primary:
                    served_by = candidate_source
                    break
        served_year_count = len(breakdown.years_via_primary) + len(breakdown.years_via_fallback)

        source_resolution.append(
            SourceResolutionEntry(
                concept_id=concept_id,
                requested_primary=route.primary,
                requested_fallback_order=list(route.fallback_order),
                layer_decided=route.layer_decided,
                served_by=served_by,
                fallback_used=bool(breakdown.years_via_fallback),
                served_year_count=served_year_count,
            )
        )

        if served_year_count == 0:
            missing_concepts.add(concept_id)

    _seed_cash_beginning_of_period(model)
    _refresh_period_metadata(
        model,
        _collect_routed_reported_period_ends(
            fmp_buffer,
            edgar_buffer,
            year_source_by_concept,
            historical_periods,
        ),
    )

    return PopulateStats(
        source="routed",
        items_populated=items_populated,
        items_skipped=items_skipped,
        periods_populated=periods_populated,
        missing_concepts=sorted(missing_concepts),
        edgar_api_calls=sum(result.api_calls for result in edgar_buffer.values()),
        edgar_errors=sorted(edgar_errors),
        edgar_partial_failures=sorted(edgar_partial_failures),
        source_resolution=sorted(source_resolution, key=lambda entry: entry.concept_id),
        fallback_engaged_concepts=sorted(fallback_engaged_concepts),
        fallback_engaged_cells=fallback_engaged_cells,
        served_by_breakdown=dict(sorted(served_by_breakdown.items())),
        fmp_quality_warnings=_fmp_quality_warnings_from_observations(
            fmp_quality_observations
        ),
        served_source_by_concept_year={
            concept_id: {
                int(year): source
                for year, source in sorted(source_by_year.items())
                if source is not None
            }
            for concept_id, source_by_year in sorted(year_source_by_concept.items())
        },
    )


_MONTH_NUMBERS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}

_YEAR_HEADER_ID = "tpl.a.header.year_header"


def _fiscal_year_end_date(month_name: str, year: int) -> date:
    """Return the last day of the fiscal year ending in the given month."""
    month_num = _MONTH_NUMBERS.get(month_name.strip().lower())
    if month_num is None:
        raise ValueError(f"Unknown fiscal year end month: {month_name!r}")
    last_day = calendar.monthrange(year, month_num)[1]
    return date(year, month_num, last_day)


def _gregorian_to_excel_serial(dt: date) -> int:
    """Convert a Python date to an Excel serial number (Lotus 1-2-3 compatible)."""
    delta = dt - date(1899, 12, 31)
    return delta.days + (1 if dt >= date(1900, 3, 1) else 0)


def update_company_info(
    model: FinancialModel,
    ticker: str,
    name: str,
    fye: str,
    sector: Optional[str] = None,
) -> None:
    """Apply company metadata, resolve the company-name template token, and seed the year header."""

    model.company.ticker = ticker
    model.company.name = name
    model.company.fiscal_year_end = fye
    model.company.sector = sector

    model.time_structure.fiscal_year_end = fye

    model.metadata.build_status = BuildStatus.historicals_populated
    model.metadata.is_template = False
    model.metadata.created_at = datetime.now(timezone.utc).isoformat()

    for item in _iter_items(model):
        if item.template_token == _COMPANY_NAME_TOKEN:
            item.label = f"{name} ({ticker})"

    # Seed the year header with the FY end date for the first historical period.
    # The formula ref(self[t-1], adjustment=365) chains forward from this seed.
    if fye and model.time_structure.historical_periods:
        first_hist = int(model.time_structure.historical_periods[0])
        try:
            seed_date = _fiscal_year_end_date(fye, first_hist)
            serial = _gregorian_to_excel_serial(seed_date)
            if not model._index:
                model.build_index()
            year_header = model.get_item(_YEAR_HEADER_ID)
            if year_header.values is None:
                year_header.values = ValueSeries()
            year_header.values.values[first_hist] = ValueCell(
                period=first_hist,
                value=float(serial),
                provenance=ValueProvenance.imported_other,
            )
            # Remove the first period from formula_periods so the value takes precedence
            if year_header.formula_periods and first_hist in year_header.formula_periods:
                year_header.formula_periods = [
                    p for p in year_header.formula_periods if int(p) != first_hist
                ]
        except (ValueError, KeyError):
            logging.warning("Could not seed year_header for FYE=%s, year=%d", fye, first_hist)


# Legacy template placeholders. These values are retained only for readback
# diagnostics and stale-workbook detection; build-time population must not use
# them as valuation inputs.
_VALUATION_DEFAULTS = {
    "tpl.v.cost_of_equity.risk_free_rate": 0.04,
    "tpl.v.cost_of_equity.equity_risk_premium": 0.045,
    "tpl.v.wacc.sofr_rate": 0.05,
    "tpl.v.wacc.credit_spread": 0.01,
}
_VALUATION_PLACEHOLDER_VALUES = {
    **_VALUATION_DEFAULTS,
    "tpl.v.current_valuation.stock_price": 100.0,
    "tpl.v.cost_of_equity.raw_beta": 1.0,
    "tpl.v.cost_of_equity.beta_floor": 1.0,
}
_VALUATION_ECONOMIC_INPUTS = {
    "stock_price": "tpl.v.current_valuation.stock_price",
    "risk_free_rate": "tpl.v.cost_of_equity.risk_free_rate",
    "equity_risk_premium": "tpl.v.cost_of_equity.equity_risk_premium",
    "raw_beta": "tpl.v.cost_of_equity.raw_beta",
    "beta_floor": "tpl.v.cost_of_equity.beta_floor",
    "adjusted_beta": "tpl.v.cost_of_equity.beta",
    "sofr_rate": "tpl.v.wacc.sofr_rate",
    "credit_spread": "tpl.v.wacc.credit_spread",
}
_VALUATION_TERMINAL_INPUTS = {
    "terminal_growth_rate": "tpl.v.dcf.terminal_growth_base",
    "exit_multiple": "tpl.v.dcf.exit_multiple_base",
}
_VALUATION_TERMINAL_INPUT_PREFIXES = (
    "tpl.v.dcf.terminal_growth_",
    "tpl.v.dcf.exit_multiple_",
)
_VALUATION_REQUIRED_INPUTS = (
    "stock_price",
    "raw_beta",
    "adjusted_beta",
    "risk_free_rate",
    "equity_risk_premium",
    "sofr_rate",
    "credit_spread",
)
_BLOOMBERG_STYLE_BETA_WEIGHT = 0.67
_MARKET_BETA_ANCHOR = 1.0
_SCENARIO_EPS_LIMIT = 4
_VALUATION_COMP_PEER_ROLES = tuple(f"comp_{index}" for index in range(1, 7))
_VALUATION_COMP_ROLES = ("target", *_VALUATION_COMP_PEER_ROLES)
_VALUATION_COMP_PE_CLEAR_ROWS = {
    role: 9 + index
    for index, role in enumerate(_VALUATION_COMP_PEER_ROLES)
}
_VALUATION_COMP_PEG_CLEAR_ROWS = {
    role: 21 + index
    for index, role in enumerate(_VALUATION_COMP_PEER_ROLES)
}
_VALUATION_COMP_CLEAR_COLUMNS = ("B", "D", "E", "F")
_VALUATION_COMP_BLANK_FORMULA = '=""'
_VALUATION_COMP_RENDERED_VALUE_KEYS = (
    "trailing_low",
    "trailing_median",
    "trailing_high",
    "forward_pe",
    "peg",
    "peg_low",
    "peg_median",
    "peg_high",
)
_REVENUE_ITEM_ID = "tpl.fm.income_statement.total_revenue"
_PROJECTED_DA_TOTAL_ID = "tpl.a.depreciation_amortization.depreciation_and_amortization_m"
_PROJECTED_DEPRECIATION_ID = "tpl.a.depreciation_amortization.depreciation"
_PROJECTED_DA_RATE_ID = "tpl.a.depreciation_amortization.depreciation_as_of_beginning_property_and_equipment"
_PROJECTED_DA_BASE_ID = "tpl.a.depreciation_amortization.beg_property_and_equipment"
_PROJECTED_SBC_TOTAL_ID = "tpl.a.stock_based_compensation.stock_based_compensation"
_PROJECTED_SBC_RATE_IDS = (
    "tpl.a.stock_based_compensation.cost_of_revenues_pct_line_item",
    "tpl.a.stock_based_compensation.sales_and_marketing_pct_line_item",
    "tpl.a.stock_based_compensation.research_and_development_pct_line_item",
    "tpl.a.stock_based_compensation.general_and_administrative_pct_line_item",
)
_PROJECTED_SBC_COMPONENT_IDS = (
    "tpl.a.stock_based_compensation.cost_of_revenues",
    "tpl.a.stock_based_compensation.sales_and_marketing",
    "tpl.a.stock_based_compensation.research_and_development",
    "tpl.a.stock_based_compensation.general_and_administrative",
)
_PROJECTED_SBC_BASE_IDS = (
    "tpl.a.unit_economics.costs_of_goods_sold",
    "tpl.a.operating_leverage.sales_and_marketing",
    "tpl.a.operating_leverage.research_and_development",
    "tpl.a.operating_leverage.general_and_administrative",
)
_SCENARIO_CASE_SELECTOR = {"bull": 1.0, "base": 2.0, "bear": 3.0}
_SCENARIO_CASE_PATTERNS = {
    "bull": {"bull", "bullcase", "upside", "bullscenario"},
    "base": {"base", "basecase", "central", "basescenario"},
    "bear": {"bear", "bearcase", "downside", "bearscenario"},
}
_SCENARIO_EPS_ITEM_RE = re.compile(r"^tpl\.s\.earnings_scenarios\.eps_(bull|base|bear)_(\d+)$")
_SCENARIO_ORDERING_EPS = 1e-12


def _read_value(model: FinancialModel, item_id: str, period: int) -> float | None:
    """Read a populated model value; return None when the cell is absent."""

    try:
        item = model.get_item(item_id)
    except KeyError:
        return None
    if item.values is None:
        return None
    cell = item.values.values.get(int(period))
    if cell is None or cell.value is None:
        return None
    return float(cell.value)


def _derive_credit_spread(model: FinancialModel, sofr: float | None) -> float | None:
    """Derive spread as effective yield minus SOFR for WACC consistency."""

    if sofr is None:
        return None
    try:
        hist_periods = sorted(int(period) for period in model.time_structure.historical_periods)
        if len(hist_periods) < 2:
            return None
        current_year, prior_year = hist_periods[-1], hist_periods[-2]

        interest_expense = _read_value(model, "tpl.fm.adjusted_earnings.interest_expense", current_year)
        ltd_current = _read_value(model, "tpl.fm.balance_sheet.long_term_debt", current_year)
        ltd_prior = _read_value(model, "tpl.fm.balance_sheet.long_term_debt", prior_year)
        if interest_expense is None or ltd_current is None or ltd_prior is None:
            return None
        if ltd_current <= 0 or ltd_prior <= 0:
            return None

        avg_debt = (ltd_current + ltd_prior) / 2.0
        if avg_debt <= 0:
            return None

        effective_yield = abs(interest_expense) / avg_debt
        spread = effective_yield - sofr
        if spread <= 0 or spread > 0.20:
            return None

        return float(spread)
    except (KeyError, AttributeError, ZeroDivisionError):
        return None


def _adjust_raw_beta(raw_beta: float, beta_floor: float | None = None) -> float:
    adjusted = (_BLOOMBERG_STYLE_BETA_WEIGHT * raw_beta) + (
        (1.0 - _BLOOMBERG_STYLE_BETA_WEIGHT) * _MARKET_BETA_ANCHOR
    )
    if beta_floor is None:
        return float(adjusted)
    return max(float(adjusted), float(beta_floor))


def _clear_valuation_input_values(model: FinancialModel) -> None:
    item_ids = set(_VALUATION_ECONOMIC_INPUTS.values()) | set(_VALUATION_TERMINAL_INPUTS.values())
    for item in _iter_items(model):
        if item.item_type is ItemType.input and any(
            item.id.startswith(prefix)
            for prefix in _VALUATION_TERMINAL_INPUT_PREFIXES
        ):
            item_ids.add(item.id)

    for item_id in item_ids:
        try:
            model.get_item(item_id).values = None
        except KeyError:
            continue


def _set_valuation_input_value(
    model: FinancialModel,
    item_id: str,
    value: float,
    *,
    projection_periods: list[int],
    provenance: ValueProvenance,
) -> bool:
    try:
        target = model.get_item(item_id)
    except KeyError:
        return False
    target.values = ValueSeries()
    for period in projection_periods:
        target.values.values[int(period)] = ValueCell(
            period=int(period),
            value=float(value),
            provenance=provenance,
        )
    return True


def _extract_first_numeric_with_source(
    fmp_data: Optional[Dict],
    endpoints: tuple[str, ...],
    fields: tuple[str, ...],
) -> tuple[float | None, str | None]:
    if not fmp_data:
        return None, None
    for endpoint in endpoints:
        records = fmp_data.get(endpoint)
        if isinstance(records, dict):
            iterable = [records]
        else:
            iterable = list(records or [])
        for record in iterable:
            if not isinstance(record, dict):
                continue
            for field in fields:
                raw_value = record.get(field)
                if raw_value is None:
                    continue
                try:
                    return float(raw_value), f"fmp.{endpoint}.{field}"
                except (TypeError, ValueError):
                    continue
    return None, None


def populate_valuation_inputs(
    model: FinancialModel,
    fmp_data: Optional[Dict],
    equity_risk_premium: float | None = None,
    valuation: dict[str, Any] | None = None,
) -> ValuationInputReadiness:
    """Populate only sourced/explicit fixed-cell valuation inputs."""

    if equity_risk_premium is not None and not (0 < equity_risk_premium < 1):
        raise ValueError(
            f"equity_risk_premium must be a decimal between 0 and 1; got {equity_risk_premium}"
        )
    if not model._index:
        model.build_index()
    projection_periods = [int(period) for period in model.time_structure.projection_periods]
    if not projection_periods:
        return ValuationInputReadiness(
            status="incomplete",
            missing=list(_VALUATION_REQUIRED_INPUTS),
            flags=[
                {
                    "code": "valuation_projection_periods_missing",
                    "severity": "error",
                    "message": "Cannot populate valuation inputs because the model has no projection periods.",
                }
            ],
        )

    _clear_valuation_input_values(model)
    sources: dict[str, str] = {}
    flags: list[dict[str, Any]] = []
    populated: set[str] = set()
    stored_inputs = _stored_valuation_inputs(valuation)

    stock_price, stock_price_source = _extract_first_numeric_with_source(
        fmp_data,
        ("quote", "quotes", "profile", "company_profile"),
        ("price", "currentPrice"),
    )
    raw_beta, raw_beta_source = _extract_first_numeric_with_source(
        fmp_data,
        ("profile", "company_profile", "key_metrics", "company-key-metrics", "company_key_metrics"),
        ("beta", "Beta"),
    )

    if stock_price is not None:
        if _set_valuation_input_value(
            model,
            _VALUATION_ECONOMIC_INPUTS["stock_price"],
            stock_price,
            projection_periods=projection_periods,
            provenance=ValueProvenance.imported_fmp,
        ):
            populated.add("stock_price")
            sources["stock_price"] = stock_price_source or "fmp"
    if raw_beta is not None:
        if _set_valuation_input_value(
            model,
            _VALUATION_ECONOMIC_INPUTS["raw_beta"],
            raw_beta,
            projection_periods=projection_periods,
            provenance=ValueProvenance.imported_fmp,
        ):
            populated.add("raw_beta")
            sources["raw_beta"] = raw_beta_source or "fmp"

    for field in ("risk_free_rate", "sofr_rate", "credit_spread"):
        stored = stored_inputs.get(field)
        if stored is None:
            continue
        if _set_valuation_input_value(
            model,
            _VALUATION_ECONOMIC_INPUTS[field],
            stored.value_decimal,
            projection_periods=projection_periods,
            provenance=ValueProvenance.input,
        ):
            populated.add(field)
            sources[field] = stored.source or f"ticker_overrides.valuation.{field}"

    stored_erp = stored_inputs.get("equity_risk_premium")
    if stored_erp is not None:
        if _set_valuation_input_value(
            model,
            _VALUATION_ECONOMIC_INPUTS["equity_risk_premium"],
            stored_erp.value_decimal,
            projection_periods=projection_periods,
            provenance=ValueProvenance.input,
        ):
            populated.add("equity_risk_premium")
            sources["equity_risk_premium"] = stored_erp.source or "ticker_overrides.valuation.equity_risk_premium"
    elif equity_risk_premium is not None:
        if _set_valuation_input_value(
            model,
            _VALUATION_ECONOMIC_INPUTS["equity_risk_premium"],
            equity_risk_premium,
            projection_periods=projection_periods,
            provenance=ValueProvenance.input,
        ):
            populated.add("equity_risk_premium")
            sources["equity_risk_premium"] = "explicit.model_build_context"

    stored_beta_floor = stored_inputs.get("beta_floor")
    beta_floor_value = stored_beta_floor.value_decimal if stored_beta_floor is not None else None
    if stored_beta_floor is not None:
        if _set_valuation_input_value(
            model,
            _VALUATION_ECONOMIC_INPUTS["beta_floor"],
            stored_beta_floor.value_decimal,
            projection_periods=projection_periods,
            provenance=ValueProvenance.input,
        ):
            populated.add("beta_floor")
            sources["beta_floor"] = stored_beta_floor.source or "ticker_overrides.valuation.beta_floor"

    if raw_beta is not None:
        if _set_valuation_input_value(
            model,
            _VALUATION_ECONOMIC_INPUTS["adjusted_beta"],
            _adjust_raw_beta(raw_beta, beta_floor_value),
            projection_periods=projection_periods,
            provenance=ValueProvenance.derived,
        ):
            populated.add("adjusted_beta")
            sources["adjusted_beta"] = (
                "derived.blume_adjusted_raw_beta_with_beta_floor"
                if beta_floor_value is not None
                else "derived.blume_adjusted_raw_beta"
            )

    for field, item_id in _VALUATION_TERMINAL_INPUTS.items():
        stored = stored_inputs.get(field)
        if stored is None:
            continue
        target_item_id = stored.item_id or item_id
        if _set_valuation_input_value(
            model,
            target_item_id,
            stored.value_decimal,
            projection_periods=projection_periods,
            provenance=ValueProvenance.input,
        ):
            populated.add(field)
            sources[field] = stored.source or f"ticker_overrides.valuation.{field}"

    missing = [field for field in _VALUATION_REQUIRED_INPUTS if field not in populated]
    if any(field in missing for field in ("stock_price", "raw_beta", "adjusted_beta")):
        flags.append(
            {
                "code": "market_valuation_inputs_missing",
                "severity": "warning",
                "message": "Quote/profile valuation inputs are missing; stock price and beta rows were left blank.",
            }
        )
    if any(field in missing for field in ("risk_free_rate", "sofr_rate", "credit_spread")):
        flags.append(
            {
                "code": "macro_valuation_inputs_missing",
                "severity": "warning",
                "message": "Macro valuation inputs are missing; run valuation-inputs or supply explicit inputs before clean DCF acceptance.",
            }
        )
    if "equity_risk_premium" in missing:
        flags.append(
            {
                "code": "equity_risk_premium_missing",
                "severity": "warning",
                "message": "Equity risk premium was not supplied; the build leaves ERP blank instead of defaulting it.",
            }
        )

    try:
        discount_period = model.get_item("tpl.v.dcf.discount_period")
    except KeyError:
        discount_period = None
    if discount_period is not None:
        if discount_period.values is None:
            discount_period.values = ValueSeries()
        for index, period in enumerate(projection_periods):
            discount_period.values.values[int(period)] = ValueCell(
                period=int(period),
                value=float(0.5 + index),
                provenance=ValueProvenance.input,
            )

    try:
        ticker_item = model.get_item("tpl.v.current_valuation.ticker")
    except KeyError:
        ticker_item = None
    if ticker_item is not None:
        ticker_item.projected = FormulaSpec(
            type=FormulaType.constant,
            params={"value": model.company.ticker},
        )

    return ValuationInputReadiness(
        status="complete" if not missing else "incomplete",
        populated=sorted(populated),
        missing=missing,
        sources=dict(sorted(sources.items())),
        flags=flags,
    )


def _stored_valuation_inputs(valuation: dict[str, Any] | None) -> dict[str, ValuationInputValue]:
    if not valuation:
        return {}
    artifact = ValuationArtifact.model_validate(valuation)
    result: dict[str, ValuationInputValue] = {}
    for field in ("risk_free_rate", "sofr_rate", "credit_spread", "equity_risk_premium", "beta_floor"):
        value = getattr(artifact.wacc, field)
        if value is not None:
            result[field] = value
    if artifact.terminal_growth_rate is not None:
        result["terminal_growth_rate"] = artifact.terminal_growth_rate
    if artifact.exit_multiple is not None:
        result["exit_multiple"] = artifact.exit_multiple
    return result


def _coerce_optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        coerced = float(value)
    except (TypeError, ValueError):
        return None
    if not math.isfinite(coerced):
        return None
    return coerced


def _computed_model_values(model: FinancialModel) -> dict[str, dict[int, float]]:
    graph = DependencyGraph()
    graph.build(model)
    derived_ids = {
        item.id
        for item in model._index.values()
        if item.item_type == ItemType.derived
    }
    return graph.compute({}, recompute=derived_ids)


def _computed_value(
    computed_values: dict[str, dict[int, float]],
    item_id: str,
    period: int,
) -> float | None:
    value = computed_values.get(item_id, {}).get(int(period))
    return _coerce_optional_float(value)


def _latest_ratio_from_computed_values(
    computed_values: dict[str, dict[int, float]],
    numerator_id: str,
    denominator_id: str,
    periods: Iterable[int],
) -> float | None:
    for period in sorted({int(period) for period in periods}, reverse=True):
        numerator = _computed_value(computed_values, numerator_id, period)
        denominator = _computed_value(computed_values, denominator_id, period)
        if numerator is None or denominator is None or denominator <= 0:
            continue
        ratio = numerator / denominator
        if math.isfinite(ratio) and ratio > 0:
            return ratio
    return None


def _missing_projection_periods(
    computed_values: dict[str, dict[int, float]],
    item_id: str,
    projection_periods: Iterable[int],
) -> list[int]:
    return [
        int(period)
        for period in projection_periods
        if _computed_value(computed_values, item_id, int(period)) is None
    ]


def _set_projection_input_values(
    model: FinancialModel,
    item_id: str,
    values_by_period: dict[int, float],
    *,
    note: str,
) -> bool:
    if not values_by_period:
        return False
    try:
        item_obj = model.get_item(item_id)
    except KeyError:
        return False
    if item_obj.values is None:
        item_obj.values = ValueSeries()
    if item_obj.overrides is None:
        item_obj.overrides = {}
    if item_obj.formula_periods is not None:
        item_obj.formula_periods = sorted(
            {int(period) for period in item_obj.formula_periods}
            | {int(period) for period in values_by_period}
        )
    for period, value in values_by_period.items():
        item_obj.values.values[int(period)] = ValueCell(
            period=int(period),
            value=float(value),
            provenance=ValueProvenance.derived,
            note=note,
        )
        item_obj.overrides[int(period)] = FormulaSpec(
            type=FormulaType.constant,
            params={"value": float(value)},
            note=note,
        )
    return True


def _seed_projected_non_gaap_addbacks(model: FinancialModel) -> dict[str, Any]:
    """Seed forward D&A/SBC add-back drivers from latest historical ratios when blank."""

    if not model._index:
        model.build_index()
    historical_periods = [int(period) for period in model.time_structure.historical_periods]
    projection_periods = [int(period) for period in model.time_structure.projection_periods]
    if not historical_periods or not projection_periods:
        return {"seeded": []}

    computed_values = _computed_model_values(model)
    seeded: list[str] = []
    skipped: list[dict[str, Any]] = []

    missing_da_periods = _missing_projection_periods(
        computed_values,
        _PROJECTED_DEPRECIATION_ID,
        projection_periods,
    )
    if missing_da_periods:
        depreciation_revenue_ratio = _latest_ratio_from_computed_values(
            computed_values,
            _PROJECTED_DEPRECIATION_ID,
            _REVENUE_ITEM_ID,
            historical_periods,
        )
        values_by_period: dict[int, float] = {}
        if depreciation_revenue_ratio is not None:
            for period in missing_da_periods:
                revenue = _computed_value(computed_values, _REVENUE_ITEM_ID, period)
                base = _computed_value(computed_values, _PROJECTED_DA_BASE_ID, period)
                if revenue is None or base is None or base <= 0:
                    continue
                value = (depreciation_revenue_ratio * revenue) / base
                if math.isfinite(value) and value > 0:
                    values_by_period[int(period)] = value
        if _set_projection_input_values(
            model,
            _PROJECTED_DA_RATE_ID,
            values_by_period,
            note="build fallback: latest historical depreciation/revenue ratio applied to projected revenue",
        ):
            seeded.append(_PROJECTED_DA_RATE_ID)
        else:
            skipped.append({"item_id": _PROJECTED_DA_RATE_ID, "reason": "insufficient_history_or_projection_base"})

    missing_sbc_periods_by_component = {
        component_id: _missing_projection_periods(computed_values, component_id, projection_periods)
        for component_id in _PROJECTED_SBC_COMPONENT_IDS
    }
    if any(missing_sbc_periods_by_component.values()):
        sbc_revenue_ratio = _latest_ratio_from_computed_values(
            computed_values,
            _PROJECTED_SBC_TOTAL_ID,
            _REVENUE_ITEM_ID,
            historical_periods,
        )
        common_values_by_period: dict[int, float] = {}
        if sbc_revenue_ratio is not None:
            for period in projection_periods:
                revenue = _computed_value(computed_values, _REVENUE_ITEM_ID, period)
                if revenue is None:
                    continue
                denominator = sum(
                    value
                    for item_id in _PROJECTED_SBC_BASE_IDS
                    if (value := _computed_value(computed_values, item_id, period)) is not None
                )
                if denominator <= 0:
                    continue
                value = (sbc_revenue_ratio * revenue) / denominator
                if math.isfinite(value) and value > 0:
                    common_values_by_period[int(period)] = value
        seeded_sbc = False
        for item_id, component_id, base_id in zip(
            _PROJECTED_SBC_RATE_IDS,
            _PROJECTED_SBC_COMPONENT_IDS,
            _PROJECTED_SBC_BASE_IDS,
        ):
            values_by_period = {
                period: common_values_by_period[period]
                for period in missing_sbc_periods_by_component[component_id]
                if period in common_values_by_period
                and (base := _computed_value(computed_values, base_id, period)) is not None
                and base > 0
            }
            if _set_projection_input_values(
                model,
                item_id,
                values_by_period,
                note="build fallback: latest historical SBC/revenue ratio allocated across operating line items",
            ):
                seeded.append(item_id)
                seeded_sbc = True
        if not seeded_sbc:
            skipped.append({"item_id": _PROJECTED_SBC_TOTAL_ID, "reason": "insufficient_history_or_projection_base"})

    return {"seeded": seeded, "skipped": skipped}


def _valuation_comp_periods(model: FinancialModel) -> list[int]:
    projection_periods = [int(period) for period in model.time_structure.projection_periods]
    if projection_periods:
        return projection_periods
    return [int(period) for period in model.time_structure.historical_periods[-1:]]


def _valuation_comps_provenance(source: str | None) -> ValueProvenance:
    normalized = str(source or "").strip().lower()
    if normalized in {"build_fallback", "fmp", "fmp_peer_comparison"}:
        return ValueProvenance.imported_fmp
    return ValueProvenance.imported_other


def _set_fixed_numeric_value(
    model: FinancialModel,
    item_id: str,
    value: float | None,
    *,
    periods: list[int],
    provenance: ValueProvenance,
    note: str | None,
) -> None:
    try:
        item_obj = model.get_item(item_id)
    except KeyError:
        return
    item_obj.values = None
    if value is None or not periods:
        return
    item_obj.values = ValueSeries()
    for period in periods:
        item_obj.values.values[int(period)] = ValueCell(
            period=int(period),
            value=float(value),
            provenance=provenance,
            note=note,
        )


def _set_fixed_text_formula(model: FinancialModel, item_id: str, value: str | None) -> None:
    try:
        item_obj = model.get_item(item_id)
    except KeyError:
        return
    item_obj.values = None
    item_obj.projected = FormulaSpec(
        type=FormulaType.constant,
        params={"value": str(value).upper() if value is not None else ""},
    )


def _clear_valuation_comp_peer_rows(model: FinancialModel) -> None:
    for role in _VALUATION_COMP_PEER_ROLES:
        _set_fixed_text_formula(model, f"tpl.s.comp_table_pe.{role}_ticker", None)
        _set_fixed_text_formula(model, f"tpl.s.comp_table_peg.{role}_ticker", None)
        for table in ("comp_table_pe", "comp_table_peg"):
            for suffix in ("low", "high", "median"):
                try:
                    item_obj = model.get_item(f"tpl.s.{table}.{role}_{suffix}")
                except KeyError:
                    continue
                item_obj.label = ""
                item_obj.values = None


def _valuation_comp_entries(
    valuation_comps: dict[str, Any],
) -> list[tuple[str, dict[str, Any]]]:
    entries: list[tuple[str, dict[str, Any]]] = []
    target = valuation_comps.get("target")
    if isinstance(target, dict):
        entries.append(("target", target))
    peers = valuation_comps.get("peers")
    if isinstance(peers, list):
        for role, peer in zip(_VALUATION_COMP_PEER_ROLES, peers):
            if isinstance(peer, dict):
                entries.append((role, peer))
    return entries


def _write_valuation_comp_row(
    model: FinancialModel,
    role: str,
    entry: dict[str, Any],
    *,
    periods: list[int],
    provenance: ValueProvenance,
    note: str | None,
) -> None:
    ticker = entry.get("ticker")
    if role != "target" and isinstance(ticker, str) and ticker.strip():
        _set_fixed_text_formula(model, f"tpl.s.comp_table_pe.{role}_ticker", ticker.strip())
        _set_fixed_text_formula(model, f"tpl.s.comp_table_peg.{role}_ticker", ticker.strip())

    trailing_low = _coerce_optional_float(entry.get("trailing_low"))
    trailing_median = _coerce_optional_float(entry.get("trailing_median"))
    trailing_high = _coerce_optional_float(entry.get("trailing_high"))
    forward_pe = _coerce_optional_float(entry.get("forward_pe"))
    pe_values = {
        "low": trailing_low,
        "high": trailing_high,
        "median": trailing_median if trailing_median is not None else forward_pe,
    }
    for suffix, value in pe_values.items():
        _set_fixed_numeric_value(
            model,
            f"tpl.s.comp_table_pe.{role}_{suffix}",
            value,
            periods=periods,
            provenance=provenance,
            note=note,
        )

    peg_value = _coerce_optional_float(entry.get("peg"))
    explicit_peg_range = any(
        key in entry for key in ("peg_low", "peg_median", "peg_high")
    )
    if explicit_peg_range:
        peg_values = {
            "low": _coerce_optional_float(entry.get("peg_low")),
            "high": _coerce_optional_float(entry.get("peg_high")),
            "median": _coerce_optional_float(entry.get("peg_median")) or peg_value,
        }
    else:
        # v1 payloads carry PEG as a current snapshot, not a constructed range.
        # Repeat the observed point across the evidence row so legacy PEG
        # cross-check formulas have an input while selected P/E stays blank.
        peg_values = {"low": peg_value, "high": peg_value, "median": peg_value}
    for suffix, value in peg_values.items():
        _set_fixed_numeric_value(
            model,
            f"tpl.s.comp_table_peg.{role}_{suffix}",
            value,
            periods=periods,
            provenance=provenance,
            note=note,
        )


def populate_valuation_comps(
    model: FinancialModel,
    valuation_comps: ValuationCompsPayload | dict[str, Any] | None,
) -> None:
    """Populate Scenarios comp tables from a caller-supplied valuation_comps payload.

    The build boundary is intentionally narrow: callers pass peer-comparison
    data in, and this function only writes fixed cells. Primary production
    bridging from Thesis.industry_analysis.peer_comparison happens in the
    skill/agent layer, not inside schema/build.py.
    """

    if valuation_comps is None:
        return
    if not isinstance(valuation_comps, dict):
        raise ValueError("valuation_comps must be a dict payload when supplied")
    entries = _valuation_comp_entries(valuation_comps)
    if not entries:
        return
    if not model._index:
        model.build_index()
    periods = _valuation_comp_periods(model)
    source = str(valuation_comps.get("source") or "").strip() or "valuation_comps"
    provenance = _valuation_comps_provenance(source)
    note = f"valuation_comps source={source}"

    _clear_valuation_comp_peer_rows(model)
    for role, entry in entries:
        _write_valuation_comp_row(
            model,
            role,
            entry,
            periods=periods,
            provenance=provenance,
            note=note,
        )


def _valuation_comp_entry_has_rendered_payload(entry: dict[str, Any]) -> bool:
    ticker = str(entry.get("ticker") or "").strip()
    if ticker:
        return True
    return any(
        _coerce_optional_float(entry.get(key)) is not None
        for key in _VALUATION_COMP_RENDERED_VALUE_KEYS
    )


def _append_valuation_comp_clear_writes(
    plan: RenderPlan,
    valuation_comps: ValuationCompsPayload | dict[str, Any] | None,
) -> None:
    if not isinstance(valuation_comps, dict):
        return
    entries = _valuation_comp_entries(valuation_comps)
    if not entries:
        return
    used_peer_roles = {
        role
        for role, entry in entries
        if role != "target" and _valuation_comp_entry_has_rendered_payload(entry)
    }
    for role in _VALUATION_COMP_PEER_ROLES:
        if role in used_peer_roles:
            continue
        for row in (
            _VALUATION_COMP_PE_CLEAR_ROWS[role],
            _VALUATION_COMP_PEG_CLEAR_ROWS[role],
        ):
            for column in _VALUATION_COMP_CLEAR_COLUMNS:
                plan.writes.append(
                    CellWrite(
                        sheet="Scenarios",
                        cell=f"{column}{row}",
                        value=_VALUATION_COMP_BLANK_FORMULA,
                    )
                )


def _finite_projection_values(
    values: Dict[int, Any],
    projection_periods: Iterable[int],
) -> Dict[int, float]:
    projected: Dict[int, float] = {}
    for period in projection_periods:
        raw_value = values.get(int(period))
        if raw_value is None:
            continue
        try:
            value = float(raw_value)
        except (TypeError, ValueError):
            continue
        if math.isfinite(value):
            projected[int(period)] = value
    return projected


def _select_scenario_output_item_id(
    model: FinancialModel,
    values: Dict[str, Dict[int, float]],
    candidate_item_ids: Iterable[str],
    projection_periods: Iterable[int],
) -> str | None:
    present_item_ids = [item_id for item_id in candidate_item_ids if item_id in model._index]
    if not present_item_ids:
        return None
    periods = [int(period) for period in projection_periods]
    for item_id in present_item_ids:
        if _finite_projection_values(values.get(item_id, {}), periods):
            return item_id
    return present_item_ids[0]


def compute_scenario_outputs(model: FinancialModel) -> Dict[str, Dict[str, Dict[int, float]]]:
    """Run Bull/Base/Bear scenario computations for decision-critical outputs."""

    if not model._index:
        model.build_index()
    graph = DependencyGraph()
    graph.build(model)
    base_results = graph.compute({})
    scenario_outputs: Dict[str, Dict[str, Dict[int, float]]] = {}
    projection_periods = [int(period) for period in model.time_structure.projection_periods]

    for case in ("bull", "base", "bear"):
        overrides = _build_scenario_overrides(model, case, base_results=base_results)
        recompute = _downstream_item_ids(graph, set(overrides))
        scenario_inputs = _scenario_compute_inputs(model, overrides)
        results = graph.compute(
            scenario_inputs,
            recompute=recompute,
            seed_results=base_results,
            propagate_roots=set(overrides),
        )
        case_outputs: Dict[str, Dict[int, float]] = {}
        for field_name, candidate_item_ids in _SCENARIO_OUTPUT_REQUIREMENTS:
            item_id = _select_scenario_output_item_id(
                model,
                results,
                candidate_item_ids,
                projection_periods,
            )
            if item_id is None:
                continue
            field_values = _finite_projection_values(results.get(item_id, {}), projection_periods)
            if field_values:
                case_outputs[field_name] = field_values
        scenario_outputs[case] = case_outputs

    return scenario_outputs


def compute_scenario_eps(model: FinancialModel) -> Dict[str, Dict[int, float]]:
    """Run Bull/Base/Bear EPS computations using direct scenario-table overrides."""

    return {
        case: dict(fields.get("adj_eps", {}))
        for case, fields in compute_scenario_outputs(model).items()
    }


def _build_scenario_overrides(
    model: FinancialModel,
    case: str,
    base_results: Dict[str, Dict[int, float]] | None = None,
) -> Dict[str, Dict[int, float]]:
    """Build per-period direct overrides for OFFSET-targeted assumption rows."""

    if not model._index:
        model.build_index()
    case = case.lower()
    if case not in _SCENARIO_CASE_SELECTOR:
        raise ValueError(f"Unknown scenario case: {case}")

    projection_periods = [int(period) for period in model.time_structure.projection_periods]
    if not projection_periods:
        return {}

    if base_results is None:
        scenario_graph = DependencyGraph()
        scenario_graph.build(model)
        base_results = scenario_graph.compute({})

    overrides: Dict[str, Dict[int, float]] = {}
    for sheet in model.sheets.values():
        if sheet.name != "Assumptions":
            continue
        for section in sheet.sections:
            for item_obj in section.line_items:
                spec = item_obj.projected
                if spec is None or spec.type != FormulaType.valuation or spec.subtype != "offset_scenario":
                    continue
                params = spec.params or {}
                anchor_ref = line_item_ref_from_obj(params.get("anchor"))
                if anchor_ref is None:
                    raise ValueError(f"OFFSET scenario {item_obj.id} is missing anchor ref")
                anchor_id = anchor_ref.id
                value_row_id = _find_scenario_value_row(model, anchor_id, case)
                if value_row_id is None:
                    continue
                period_values = {
                    period: value
                    for period in projection_periods
                    if (value := base_results.get(value_row_id, {}).get(period)) is not None
                }
                if period_values:
                    overrides[item_obj.id] = period_values
    return overrides


def _scenario_compute_inputs(
    model: FinancialModel,
    overrides: Dict[str, Dict[int, float]],
) -> Dict[str, Dict[int, float]]:
    """Pin explicit assumption constants while scenario drivers propagate."""

    scenario_inputs: Dict[str, Dict[int, float]] = {
        item_id: {int(period): float(value) for period, value in period_values.items()}
        for item_id, period_values in overrides.items()
    }
    projection_periods = {int(period) for period in model.time_structure.projection_periods}
    if not projection_periods:
        return scenario_inputs
    scenario_root_ids = set(overrides)

    for sheet in model.sheets.values():
        if sheet.name != "Assumptions":
            continue
        for section in sheet.sections:
            for item_obj in section.line_items:
                if item_obj.id in scenario_inputs or not item_obj.overrides:
                    continue
                if _projected_formula_refs_any(item_obj, scenario_root_ids):
                    continue
                constants: Dict[int, float] = {}
                for raw_period, spec in item_obj.overrides.items():
                    period = int(raw_period)
                    if period not in projection_periods:
                        continue
                    if spec.type != FormulaType.constant:
                        continue
                    value = (spec.params or {}).get("value")
                    if value is None:
                        continue
                    try:
                        constants[period] = float(value)
                    except (TypeError, ValueError):
                        continue
                if constants:
                    scenario_inputs[item_obj.id] = constants

    return scenario_inputs


def _projected_formula_refs_any(item_obj: LineItem, item_ids: set[str]) -> bool:
    """Return true when an item's projected formula directly consumes a scenario root."""

    spec = item_obj.projected
    if spec is None or not item_ids:
        return False
    return bool(_extract_ref_ids(spec.params or {}) & item_ids)


def _find_scenario_value_row(model: FinancialModel, anchor_id: str, case: str) -> Optional[str]:
    """Find the Bull/Base/Bear value row immediately below a scenario-table anchor."""

    anchor_location = _find_item_location(model, anchor_id)
    if anchor_location is None:
        logging.warning("Scenario override anchor not found: %s", anchor_id)
        return None
    _sheet_name, section, anchor_index = anchor_location
    anchor_row = int(section.line_items[anchor_index].row)

    all_anchor_ids = _offset_anchor_ids(model)
    candidates: list[LineItem] = []
    for candidate in section.line_items[anchor_index + 1:]:
        if int(candidate.row) <= anchor_row:
            continue
        if candidate.id in all_anchor_ids:
            break
        if candidate.item_type in {ItemType.header, ItemType.spacer}:
            break
        candidates.append(candidate)

    patterns = _SCENARIO_CASE_PATTERNS[case]
    for candidate in candidates:
        if _normalize_scenario_label(candidate.label) in patterns:
            return candidate.id

    if len(candidates) != 3:
        logging.warning(
            "Scenario override anchor %s is ambiguous for %s: observed %d candidate rows",
            anchor_id,
            case,
            len(candidates),
        )
        return None

    sorted_candidates = sorted(candidates, key=lambda item_obj: int(item_obj.row))
    rows = [int(item_obj.row) for item_obj in sorted_candidates]
    expected_rows = list(range(rows[0], rows[0] + 3))
    if rows != expected_rows:
        logging.warning(
            "Scenario override anchor %s is ambiguous for %s: candidate rows are not contiguous (%s)",
            anchor_id,
            case,
            rows,
        )
        return None

    position = {"bull": 0, "base": 1, "bear": 2}[case]
    return sorted_candidates[position].id


def _schema_version_seen(ticker_overrides: TickerOverrides | None) -> str | None:
    if ticker_overrides is None:
        return None
    value = (ticker_overrides.file_meta or {}).get("schema_version")
    return str(value) if value is not None else None


def _projection_provenance_to_dict(provenance: Any) -> dict[str, Any] | None:
    if provenance is None:
        return None
    if hasattr(provenance, "model_dump"):
        return provenance.model_dump(mode="json")
    if isinstance(provenance, dict):
        return dict(provenance)
    return None


def _extract_last_provenance(projection_entry: Any) -> dict[str, Any] | None:
    latest: tuple[str, dict[str, Any] | None] | None = None
    for scenario in getattr(projection_entry, "scenarios", {}).values():
        provenance = _projection_provenance_to_dict(getattr(scenario, "provenance", None))
        if provenance is None:
            continue
        written_at = str(provenance.get("written_at") or "")
        if latest is None or written_at >= latest[0]:
            latest = (written_at, provenance)
    return latest[1] if latest is not None else None


def _resolve_scenario_flex_row(
    model: FinancialModel,
    item_id: str,
    scenario_name: Literal["bull", "bear"],
) -> LineItem | None:
    """Resolve a scenario-table flex row for an offset-scenario owner or anchor."""

    try:
        item = model.get_item(item_id)
    except KeyError:
        return None

    anchor_ids: list[str] = []
    spec = item.projected
    if spec is not None and spec.type == FormulaType.valuation and spec.subtype == "offset_scenario":
        anchor_ref = line_item_ref_from_obj((spec.params or {}).get("anchor"))
        if anchor_ref is not None:
            anchor_ids.append(anchor_ref.id)

    anchor_ids.append(item_id)

    seen: set[str] = set()
    for anchor_id in anchor_ids:
        if anchor_id in seen:
            continue
        seen.add(anchor_id)
        value_row_id = _find_scenario_value_row(model, anchor_id, scenario_name)
        if value_row_id is None:
            continue
        try:
            return model.get_item(value_row_id)
        except KeyError:
            continue
    return None


def _resolve_scenario_case_row(
    model: FinancialModel,
    item_id: str,
    scenario_name: Literal["bull", "base", "bear"],
) -> LineItem | None:
    if scenario_name in {"bull", "bear"}:
        return _resolve_scenario_flex_row(model, item_id, scenario_name)

    try:
        item = model.get_item(item_id)
    except KeyError:
        return None

    anchor_ids: list[str] = []
    spec = item.projected
    if spec is not None and spec.type == FormulaType.valuation and spec.subtype == "offset_scenario":
        anchor_ref = line_item_ref_from_obj((spec.params or {}).get("anchor"))
        if anchor_ref is not None:
            anchor_ids.append(anchor_ref.id)
    anchor_ids.append(item_id)

    seen: set[str] = set()
    for anchor_id in anchor_ids:
        if anchor_id in seen:
            continue
        seen.add(anchor_id)
        value_row_id = _find_scenario_value_row(model, anchor_id, "base")
        if value_row_id is None:
            continue
        try:
            return model.get_item(value_row_id)
        except KeyError:
            continue
    return None


def _projection_entry_values_by_period(
    *,
    model: FinancialModel,
    item_id: str,
    scenario_entry: Any,
    most_recent_fy: int,
    percent_normalize: Callable[..., float],
) -> dict[int, float]:
    return {
        int(year_str): _projection_entry_value_for_model(
            model=model,
            item_id=item_id,
            scenario_entry=scenario_entry,
            raw_value=raw_value,
            percent_normalize=percent_normalize,
        )
        for year_str, raw_value in getattr(scenario_entry, "values", {}).items()
        if int(year_str) > most_recent_fy
    }


def _projection_entry_value_for_model(
    *,
    model: FinancialModel,
    item_id: str,
    scenario_entry: Any,
    raw_value: Any,
    percent_normalize: Callable[..., float],
) -> float:
    value_scale = getattr(scenario_entry, "value_scale", "display")
    if value_scale == "model":
        return float(raw_value)
    fields_set = (
        getattr(scenario_entry, "model_fields_set", None)
        or getattr(scenario_entry, "__fields_set__", set())
        or set()
    )
    explicit_display_scale = value_scale == "display" and "value_scale" in fields_set
    return percent_normalize(
        model,
        item_id,
        raw_value,
        decimal_passthrough=explicit_display_scale,
    )


def _item_seeded_values_by_period(item: LineItem, most_recent_fy: int) -> dict[int, float]:
    values: dict[int, float] = {}
    if item.values is not None:
        for period, cell in item.values.values.items():
            if int(period) > most_recent_fy and cell.value is not None:
                values[int(period)] = float(cell.value)
    if item.overrides is not None:
        for period, spec in item.overrides.items():
            if int(period) <= most_recent_fy:
                continue
            if spec.type != FormulaType.constant:
                continue
            value = (spec.params or {}).get("value")
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                values[int(period)] = float(value)
    return values


def _projection_base_values_by_period(
    *,
    model: FinancialModel,
    projections: Any,
    projection_entry: Any,
    rate_key: str,
    base_item: LineItem | None,
    most_recent_fy: int,
    percent_normalize: Callable[..., float],
) -> dict[int, float]:
    if base_item is None:
        return {}

    base_entry = projection_entry.scenarios.get("base")
    if base_entry is None:
        base_projection_entry = projections.root.get(base_item.id)
        if base_projection_entry is None and base_item.id != rate_key:
            base_projection_entry = projections.root.get(rate_key)
        if base_projection_entry is not None:
            base_entry = base_projection_entry.scenarios.get("base")
    if base_entry is None:
        base_entry = _projection_owner_base_entry_for_case_row(
            model=model,
            projections=projections,
            rate_key=rate_key,
            base_item=base_item,
        )
    if base_entry is not None:
        return _projection_entry_values_by_period(
            model=model,
            item_id=base_item.id,
            scenario_entry=base_entry,
            most_recent_fy=most_recent_fy,
            percent_normalize=percent_normalize,
        )
    return _item_seeded_values_by_period(base_item, most_recent_fy)


def _projection_owner_base_entry_for_case_row(
    *,
    model: FinancialModel,
    projections: Any,
    rate_key: str,
    base_item: LineItem,
) -> Any | None:
    """Find an owner-row base projection for an anchor-level scenario entry."""

    for candidate_key, candidate_entry in getattr(projections, "root", {}).items():
        if candidate_key in {rate_key, base_item.id}:
            continue
        candidate_base_entry = candidate_entry.scenarios.get("base")
        if candidate_base_entry is None:
            continue
        try:
            model.get_item(candidate_key)
        except KeyError:
            continue
        candidate_base_item = _resolve_scenario_case_row(model, candidate_key, "base")
        if candidate_base_item is None or candidate_base_item.id != base_item.id:
            continue
        return candidate_base_entry
    return None


def _scenario_ordering_violations_by_case(
    *,
    bull_values: dict[int, float],
    base_values: dict[int, float],
    bear_values: dict[int, float],
) -> dict[Literal["bull", "bear"], list[str]]:
    violations: dict[Literal["bull", "bear"], list[str]] = {"bull": [], "bear": []}
    for period in sorted(set(bull_values) & set(base_values) & set(bear_values)):
        bull = bull_values[period]
        base = base_values[period]
        bear = bear_values[period]
        detail = f"{period}:bull={bull:g},base={base:g},bear={bear:g}"
        if abs(bull - bear) <= _SCENARIO_ORDERING_EPS:
            violations["bull"].append(f"{detail},expected=bull/base/bear_distinct")
            violations["bear"].append(f"{detail},expected=bull/base/bear_distinct")
        elif bull > bear:
            if bull <= base + _SCENARIO_ORDERING_EPS:
                violations["bull"].append(f"{detail},expected=bull>base")
            if bear >= base - _SCENARIO_ORDERING_EPS:
                violations["bear"].append(f"{detail},expected=bear<base")
        else:
            if bull >= base - _SCENARIO_ORDERING_EPS:
                violations["bull"].append(f"{detail},expected=bull<base")
            if bear <= base + _SCENARIO_ORDERING_EPS:
                violations["bear"].append(f"{detail},expected=bear>base")
    return {case: details for case, details in violations.items() if details}


def _seed_projections_from_overrides(
    model: FinancialModel,
    ticker_overrides: TickerOverrides | None,
    compiled_registry: "CompiledDriverRegistry | None",
    most_recent_fy: int,
) -> SeedProjectionsResult:
    """Seed durable projection override values into renderer-priority overrides."""

    schema_version_seen = _schema_version_seen(ticker_overrides)
    if ticker_overrides is None or not ticker_overrides.projections:
        return SeedProjectionsResult(
            total_rate_keys=0,
            schema_version_seen=schema_version_seen,
        )

    from pydantic import ValidationError

    from .overrides_projections import ProjectionsSection
    from .scenario_bridge import percent_normalize_via_formatter

    def _projection_percent_normalize(
        model: FinancialModel,
        target_item_id: str,
        value: Any,
        *,
        decimal_passthrough: bool = False,
    ) -> float:
        return percent_normalize_via_formatter(
            model,
            target_item_id,
            value,
            decimal_passthrough=decimal_passthrough,
        )

    try:
        projections = ProjectionsSection.model_validate(ticker_overrides.projections)
    except ValidationError as exc:
        logging.error("Projections section failed validation: %s", exc)
        return SeedProjectionsResult(
            seeded_count=0,
            orphans=[],
            total_rate_keys=0,
            schema_version_seen=schema_version_seen,
            validation_error=str(exc),
        )

    seeded_count = 0
    orphans: list[OrphanedProjection] = []
    warnings: list[SeedProjectionWarning] = []
    total_rate_keys = len(projections.root)

    bm_registry_items: set[str] = set()
    if compiled_registry is not None:
        bm_registry_items = set(compiled_registry.node_items.values()) | set(
            compiled_registry.driver_keys.values()
        )

    for rate_key, projection_entry in projections.root.items():
        try:
            item = model.get_item(rate_key)
        except KeyError:
            item = None

        if item is None:
            if (
                rate_key.startswith("bm.")
                and compiled_registry is not None
                and rate_key not in bm_registry_items
            ):
                reason: Literal[
                    "bm_key_not_in_registry",
                    "tpl_item_not_found",
                    "item_not_found",
                    "bull_flex_row_not_found",
                    "bear_flex_row_not_found",
                ] = "bm_key_not_in_registry"
                detail = (
                    f"BM-driven key {rate_key!r} is not in this build's compiled registry; "
                    "the BM artifact may have changed since the projection was written"
                )
            elif rate_key.startswith("tpl."):
                reason = "tpl_item_not_found"
                detail = (
                    f"Template-driven key {rate_key!r} is not present in the model; "
                    "the SIA template may have been renamed or restructured"
                )
            else:
                reason = "item_not_found"
                detail = (
                    f"Projection key {rate_key!r} is not present in the current model; "
                    "it may be a manual edit or stale key"
                )
            orphans.append(
                OrphanedProjection(
                    rate_key=rate_key,
                    reason=reason,
                    detail=detail,
                    last_provenance=_extract_last_provenance(projection_entry),
                )
            )
            continue

        base = projection_entry.scenarios.get("base")
        if base is not None:
            if item.overrides is None:
                item.overrides = {}
            for year_str, raw_value in base.values.items():
                period = int(year_str)
                if period <= most_recent_fy:
                    continue
                normalized = _projection_entry_value_for_model(
                    model=model,
                    item_id=item.id,
                    scenario_entry=base,
                    raw_value=raw_value,
                    percent_normalize=_projection_percent_normalize,
                )
                item.overrides[period] = FormulaSpec(
                    type=FormulaType.constant,
                    params={"value": normalized},
                )
            seeded_count += 1
            base_scenario_item = _resolve_scenario_case_row(model, item.id, "base")
            if (
                base_scenario_item is not None
                and base_scenario_item.id != item.id
                and projections.root.get(base_scenario_item.id) is None
            ):
                base_scenario_values = _projection_entry_values_by_period(
                    model=model,
                    item_id=base_scenario_item.id,
                    scenario_entry=base,
                    most_recent_fy=most_recent_fy,
                    percent_normalize=_projection_percent_normalize,
                )
                if base_scenario_values:
                    if base_scenario_item.overrides is None:
                        base_scenario_item.overrides = {}
                    for period, normalized in base_scenario_values.items():
                        base_scenario_item.overrides[period] = FormulaSpec(
                            type=FormulaType.constant,
                            params={"value": normalized},
                        )
                    seeded_count += 1

        skip_scenario_seed: set[str] = set()
        bull_entry = projection_entry.scenarios.get("bull")
        bear_entry = projection_entry.scenarios.get("bear")
        scenario_values: dict[str, dict[int, float]] = {}
        scenario_items: dict[str, LineItem] = {}
        if bull_entry is not None or bear_entry is not None:
            base_item = _resolve_scenario_case_row(model, item.id, "base")
            if base_item is None:
                for scenario_name, scenario_entry in (
                    ("bull", bull_entry),
                    ("bear", bear_entry),
                ):
                    if scenario_entry is None:
                        continue
                    warnings.append(
                        SeedProjectionWarning(
                            rate_key=rate_key,
                            kind="base_flex_row_not_found",
                            scenario=scenario_name,
                            detail=f"No base scenario flex row found for {rate_key!r}; skipped {scenario_name} seed",
                            last_provenance=_projection_provenance_to_dict(
                                scenario_entry.provenance
                            ),
                        )
                    )
                    skip_scenario_seed.add(scenario_name)
            else:
                base_values = _projection_base_values_by_period(
                    model=model,
                    projections=projections,
                    projection_entry=projection_entry,
                    rate_key=rate_key,
                    base_item=base_item,
                    most_recent_fy=most_recent_fy,
                    percent_normalize=_projection_percent_normalize,
                )
                for scenario_name, scenario_entry in (
                    ("bull", bull_entry),
                    ("bear", bear_entry),
                ):
                    if scenario_entry is None:
                        continue
                    scenario_item = _resolve_scenario_case_row(model, item.id, scenario_name)
                    if scenario_item is None:
                        continue
                    scenario_items[scenario_name] = scenario_item
                    values = _projection_entry_values_by_period(
                        model=model,
                        item_id=scenario_item.id,
                        scenario_entry=scenario_entry,
                        most_recent_fy=most_recent_fy,
                        percent_normalize=_projection_percent_normalize,
                    )
                    scenario_values[scenario_name] = values
                    missing_base_periods = sorted(set(values) - set(base_values))
                    if missing_base_periods:
                        warnings.append(
                            SeedProjectionWarning(
                                rate_key=rate_key,
                                kind="base_period_values_missing",
                                scenario=scenario_name,
                                detail=(
                                    f"Base scenario row {base_item.id!r} lacks periods "
                                    f"{','.join(str(period) for period in missing_base_periods)}; "
                                    f"skipped {scenario_name} seed"
                                ),
                                last_provenance=_projection_provenance_to_dict(
                                    scenario_entry.provenance
                                ),
                            )
                        )
                        skip_scenario_seed.add(scenario_name)
                if {"bull", "bear"} <= set(scenario_values):
                    ordering_issues = _scenario_ordering_violations_by_case(
                        bull_values=scenario_values["bull"],
                        base_values=base_values,
                        bear_values=scenario_values["bear"],
                    )
                    for scenario_name, details in ordering_issues.items():
                        scenario_entry = projection_entry.scenarios[scenario_name]
                        warnings.append(
                            SeedProjectionWarning(
                                rate_key=rate_key,
                                kind="scenario_ordering_violation",
                                scenario=scenario_name,
                                detail=";".join(details[:5]),
                                last_provenance=_projection_provenance_to_dict(
                                    scenario_entry.provenance
                                ),
                            )
                        )
                        skip_scenario_seed.add(scenario_name)

        for scenario_name in ("bull", "bear"):
            if scenario_name in skip_scenario_seed:
                continue
            scenario_entry = projection_entry.scenarios.get(scenario_name)
            if scenario_entry is None:
                continue
            scenario_item = scenario_items.get(scenario_name)
            if scenario_item is None:
                scenario_item = _resolve_scenario_flex_row(model, item.id, scenario_name)
            if scenario_item is None:
                orphans.append(
                    OrphanedProjection(
                        rate_key=rate_key,
                        reason=f"{scenario_name}_flex_row_not_found",
                        detail=f"No Scenarios sheet flex row found for {scenario_name} of {rate_key!r}",
                        last_provenance=_projection_provenance_to_dict(
                            scenario_entry.provenance
                        ),
                    )
                )
                continue
            if scenario_item.overrides is None:
                scenario_item.overrides = {}
            values = scenario_values.get(scenario_name)
            if values is None:
                values = _projection_entry_values_by_period(
                    model=model,
                    item_id=scenario_item.id,
                    scenario_entry=scenario_entry,
                    most_recent_fy=most_recent_fy,
                    percent_normalize=_projection_percent_normalize,
                )
            for period, normalized in values.items():
                scenario_item.overrides[period] = FormulaSpec(
                    type=FormulaType.constant,
                    params={"value": normalized},
                )
            seeded_count += 1

    return SeedProjectionsResult(
        seeded_count=seeded_count,
        orphans=orphans,
        warnings=warnings,
        total_rate_keys=total_rate_keys,
        schema_version_seen=schema_version_seen,
    )


def _populate_scenario_eps(model: FinancialModel, eps_by_case: Dict[str, Dict[int, float]]) -> None:
    if not model._index:
        model.build_index()
    projection_periods = [int(period) for period in model.time_structure.projection_periods[:_SCENARIO_EPS_LIMIT]]

    for item_obj in _iter_items(model):
        match = _SCENARIO_EPS_ITEM_RE.match(item_obj.id)
        if match is None:
            continue
        case = match.group(1)
        index = int(match.group(2)) - 1
        if index < 0 or index >= len(projection_periods):
            continue
        period = projection_periods[index]
        value = eps_by_case.get(case, {}).get(period)
        if value is None:
            continue
        if item_obj.values is None:
            item_obj.values = ValueSeries()
        item_obj.values.values[int(period)] = ValueCell(
            period=int(period),
            value=float(value),
            provenance=ValueProvenance.computed,
        )


def _populate_scenario_inputs(model: FinancialModel) -> None:
    model.scenarios = {
        "bull": ScenarioInputs(
            name="Bull",
            assumptions={"tpl.a.header.scenario_value": 1},
            description="Bull case: higher growth, higher multiples",
        ),
        "base": ScenarioInputs(
            name="Base",
            assumptions={"tpl.a.header.scenario_value": 2},
            description="Base case: consensus growth, median multiples",
        ),
        "bear": ScenarioInputs(
            name="Bear",
            assumptions={"tpl.a.header.scenario_value": 3},
            description="Bear case: lower growth, lower multiples",
        ),
    }


def _extract_first_numeric(
    fmp_data: Optional[Dict],
    endpoints: tuple[str, ...],
    fields: tuple[str, ...],
) -> Optional[float]:
    if not fmp_data:
        return None
    for endpoint in endpoints:
        records = fmp_data.get(endpoint)
        if isinstance(records, dict):
            iterable = [records]
        else:
            iterable = list(records or [])
        for record in iterable:
            if not isinstance(record, dict):
                continue
            for field in fields:
                raw_value = record.get(field)
                if raw_value is None:
                    continue
                try:
                    return float(raw_value)
                except (TypeError, ValueError):
                    continue
    return None


def _find_item_location(model: FinancialModel, item_id: str) -> tuple[str, Section, int] | None:
    for sheet_name, sheet in model.sheets.items():
        for section in sheet.sections:
            for index, item_obj in enumerate(section.line_items):
                if item_obj.id == item_id:
                    return sheet_name, section, index
    return None


def _offset_anchor_ids(model: FinancialModel) -> set[str]:
    anchors: set[str] = set()
    for item_obj in _iter_items(model):
        for spec in (item_obj.historical, item_obj.projected):
            if spec is None or spec.type != FormulaType.valuation or spec.subtype != "offset_scenario":
                continue
            anchor_ref = line_item_ref_from_obj((spec.params or {}).get("anchor"))
            if anchor_ref is not None:
                anchors.add(anchor_ref.id)
    return anchors


def _normalize_scenario_label(label: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(label or "").strip().lower())


def _downstream_item_ids(graph: DependencyGraph, roots: set[str]) -> set[str]:
    downstream: set[str] = set()
    stack = list(roots)
    while stack:
        node = stack.pop()
        for child in graph.adj.get(node, set()):
            if child in downstream:
                continue
            downstream.add(child)
            stack.append(child)
    return downstream


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
    ticker_overrides = load_ticker_overrides(ticker)
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

    stats = populate_historicals(
        model,
        source=source,
        ticker=ticker,
        taxonomy=taxonomy,
        most_recent_fy=most_recent_fy,
        n_historical=n_historical,
        fmp_data=fmp_data,
        edgar_fetcher=edgar_fetcher,
        historical_sources=historical_sources,
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


def build_model_from_mbc(
    mbc: ModelBuildContext,
    *,
    output_path: str | None = None,
    formatter: ModelFormatter | None = None,
    edgar_fetcher: Callable | None = None,
    edgar_financials_fetcher: EdgarFinancialsFetcher | None = None,
    fmp_data: dict | None = None,
    business_model: "BusinessModel | None" = None,
    valuation_comps: ValuationCompsPayload | dict[str, Any] | None = None,
    validation_mode: bool = False,
    run_diagnostics: bool = False,
) -> BuildResult:
    """Build a model using ModelBuildContext as the authoritative scalar input source."""

    is_default_hs = (
        "historical_sources" not in mbc.model_fields_set
        or _is_default_historical_sources(mbc.historical_sources)
    )
    historical_sources = None
    if not is_default_hs:
        taxonomy = load_data_taxonomy()
        for override in mbc.historical_sources.overrides:
            route = ConceptSourceRoute(
                concept_id=override.concept_id,
                primary=override.preferred,
                fallback_order=list(override.fallback_order),
                layer_decided="mbc_override",
            )
            validate_route_eligibility(
                route,
                taxonomy.get(override.concept_id),
                is_explicit_override=True,
            )
        historical_sources = mbc.historical_sources

    return build_model(
        ticker=mbc.company.ticker,
        company_name=mbc.company.name,
        fiscal_year_end=mbc.company.fiscal_year_end,
        most_recent_fy=mbc.company.most_recent_fy,
        output_path=output_path,
        source=mbc.source,
        fmp_data=fmp_data,
        sector=mbc.sector,
        n_historical=mbc.n_historical,
        n_projection=mbc.n_projection,
        formatter=formatter,
        edgar_fetcher=edgar_fetcher,
        segment_mapping=None,
        edgar_financials_fetcher=edgar_financials_fetcher,
        axis=None,
        formula_first=mbc.formula_first,
        mbc_segment_config=mbc.segment_config,
        business_model=business_model,
        mbc_drivers=mbc.drivers if business_model else None,
        historical_sources=historical_sources,
        validation_mode=validation_mode,
        run_diagnostics=run_diagnostics,
        equity_risk_premium=mbc.valuation.inputs.equity_risk_premium,
        valuation_comps=valuation_comps,
    )


def _is_default_historical_sources(historical_sources: HistoricalSources) -> bool:
    # Persisted MBC JSON includes schema defaults; after reload Pydantic marks
    # them as supplied even though callers intended the legacy source path.
    return (
        historical_sources.default_source == "fmp"
        and not historical_sources.default_fallback_enabled
        and not historical_sources.overrides
    )


def _apply_mbc_seeds(
    model: FinancialModel,
    mbc_drivers: dict[str, Driver],
    compiled_registry: "CompiledDriverRegistry",
) -> None:
    """Write MBC driver values into ValueSeries before formula-first derivation."""

    historical_periods = model.time_structure.historical_periods or model.time_structure.historical_years or []
    projection_periods = model.time_structure.projection_periods or model.time_structure.projection_years or []
    historical_set = {int(period) for period in historical_periods}

    for driver_key, driver in mbc_drivers.items():
        normalized_driver_key = str(driver_key or "").strip()
        item_id = compiled_registry.driver_keys.get(normalized_driver_key)
        if item_id is None:
            continue
        try:
            item = model.get_item(item_id)
        except KeyError:
            continue

        if item.values is None:
            item.values = ValueSeries()

        has_formula = item.projected is not None
        is_bm_rate_row = _is_business_model_rate_driver_key(normalized_driver_key, item_id)
        requested = [int(period) for period in (driver.periods or [])]
        if not requested:
            if has_formula and not is_bm_rate_row:
                historical_years = sorted(historical_set)
                requested = [historical_years[-1]] if historical_years else []
            else:
                requested = [int(period) for period in projection_periods]

        if has_formula and not is_bm_rate_row:
            requested = [period for period in requested if period in historical_set]

        for period in requested:
            item.values.values[period] = ValueCell(
                period=period,
                value=driver.value,
                provenance=ValueProvenance.input,
            )


def _is_business_model_rate_driver_key(driver_key: str, item_id: str) -> bool:
    if not item_id.startswith("bm.") or "__" not in item_id:
        return False
    rate_key = item_id.rsplit("__", 1)[-1]
    normalized = str(driver_key or "").strip()
    return normalized.endswith(f".{rate_key}") or normalized.endswith(f"__{rate_key}")


def _iter_items(model: FinancialModel) -> Iterable[LineItem]:
    for sheet in model.sheets.values():
        for section in sheet.sections:
            for item in section.line_items:
                yield item


def _build_fmp_lookup(fmp_data: Dict) -> Dict[str, Dict[int, Dict]]:
    lookup: Dict[str, Dict[int, Dict]] = {}

    for endpoint, records in (fmp_data or {}).items():
        endpoint_lookup: Dict[int, Dict] = {}
        for record in records or []:
            if not isinstance(record, dict):
                continue
            year = _record_year(record)
            if year is None:
                continue
            existing = endpoint_lookup.get(year)
            if _prefer_record(record, existing):
                endpoint_lookup[year] = record
        lookup[str(endpoint)] = endpoint_lookup

    return lookup


def _record_year(record: Dict) -> Optional[int]:
    raw_year = record.get("calendarYear")
    if raw_year is None:
        date_value = record.get("date")
        if isinstance(date_value, str) and len(date_value) >= 4:
            raw_year = date_value[:4]
    try:
        return int(raw_year) if raw_year is not None else None
    except (TypeError, ValueError):
        return None


def _prefer_record(candidate: Dict, existing: Optional[Dict]) -> bool:
    if existing is None:
        return True
    candidate_period = str(candidate.get("period") or "").upper()
    existing_period = str(existing.get("period") or "").upper()
    return candidate_period == "FY" and existing_period != "FY"


def _make_fmp_provenance(
    concept: DataSourceMapping,
    field_used: str | None,
) -> FmpProvenance | None:
    endpoint = concept.fmp_endpoint
    if not endpoint or not field_used:
        return None
    return FmpProvenance(
        endpoint=endpoint,
        field=field_used,
        fallback_field_used=(
            field_used
            if concept.fallback_fmp_field and field_used != concept.fmp_field
            else None
        ),
    )


def _scale_fmp_value(
    concept_id: str,
    raw_value,
    *,
    concept: DataSourceMapping | None = None,
) -> float:
    value = float(raw_value)
    if concept_id in _PER_SHARE_CONCEPTS:
        scaled = value
    else:
        scaled = value / 1_000_000.0
    if concept is not None and concept.fmp_negate:
        scaled = -abs(scaled)
    return scaled


def _is_fmp_zero_value(raw_value) -> bool:
    if isinstance(raw_value, bool) or raw_value is None:
        return False
    try:
        return float(raw_value) == 0.0
    except (TypeError, ValueError):
        return False


def _fmp_float_value(raw_value: object | None) -> float | None:
    if isinstance(raw_value, bool) or raw_value is None:
        return None
    try:
        value = float(raw_value)
    except (TypeError, ValueError):
        return None
    return value if math.isfinite(value) else None


def _raw_fmp_value_for_concept(
    concept: DataSourceMapping,
    record: dict | None,
) -> tuple[object | None, str | None, bool]:
    field = concept.fmp_field
    raw_value = record.get(field) if record and field else None
    field_used = field

    if concept.treat_zero_as_missing and _is_fmp_zero_value(raw_value):
        raw_value = None

    fallback_field = concept.fallback_fmp_field
    fallback_value = record.get(fallback_field) if record and fallback_field else None
    fallback_value, fallback_field_used = _fmp_fallback_value_for_concept(
        concept,
        record,
        fallback_value,
        fallback_field,
    )
    zero_split_with_combined_sga_fallback = (
        fallback_field == "sellingGeneralAndAdministrativeExpenses"
        and _is_fmp_zero_value(raw_value)
        and fallback_value is not None
        and not _is_fmp_zero_value(fallback_value)
    )
    if (raw_value is None or zero_split_with_combined_sga_fallback) and fallback_field:
        if fallback_value is not None:
            return fallback_value, fallback_field_used, True
    return raw_value, field_used, False


def _fmp_fallback_value_for_concept(
    concept: DataSourceMapping,
    record: dict | None,
    fallback_value: object | None,
    fallback_field: str | None,
) -> tuple[object | None, str | None]:
    if (
        concept.fmp_field != "sellingAndMarketingExpenses"
        or concept.fallback_fmp_field != "sellingGeneralAndAdministrativeExpenses"
        or not record
    ):
        return fallback_value, fallback_field

    combined_sga_reported = fallback_value is not None and not _is_fmp_zero_value(fallback_value)
    combined_sga_fallback = _combined_sga_residual_fallback(record, fallback_value)
    if combined_sga_reported:
        return combined_sga_fallback, fallback_field

    operating_expenses_fallback = _operating_expenses_residual_fallback(record)
    if operating_expenses_fallback is not None:
        return operating_expenses_fallback

    if combined_sga_fallback is not None:
        return combined_sga_fallback, fallback_field
    return None, fallback_field


def _combined_sga_residual_fallback(
    record: dict,
    fallback_value: object | None,
) -> object | None:
    if fallback_value is None:
        return None

    gna_value = record.get("generalAndAdministrativeExpenses")
    if gna_value is None or _is_fmp_zero_value(gna_value):
        return fallback_value

    fallback_numeric = _fmp_float_value(fallback_value)
    gna_numeric = _fmp_float_value(gna_value)
    if fallback_numeric is None or gna_numeric is None:
        return fallback_value
    derived_value = fallback_numeric - gna_numeric
    if derived_value < 0:
        return None
    return derived_value


def _operating_expenses_residual_fallback(record: dict) -> tuple[object, str] | None:
    total_opex = _fmp_float_value(record.get("operatingExpenses"))
    source_field = "operatingExpenses"
    if total_opex is None or total_opex == 0.0:
        gross_profit = _fmp_float_value(record.get("grossProfit"))
        operating_income = _fmp_float_value(record.get("operatingIncome"))
        if gross_profit is None or operating_income is None:
            return None
        total_opex = gross_profit - operating_income
        source_field = "grossProfit-operatingIncome"

    if total_opex <= 0:
        return None

    residual = total_opex
    for component_field in (
        "researchAndDevelopmentExpenses",
        "generalAndAdministrativeExpenses",
    ):
        component = _fmp_float_value(record.get(component_field))
        if component is None or component <= 0:
            continue
        residual -= component

    if residual < 0:
        return None
    return residual, source_field


def _edgar_negate_enabled(concept: DataSourceMapping) -> bool:
    if concept.edgar_negate is not None:
        return bool(concept.edgar_negate)
    return bool(concept.negate)


def _tags_equivalent(requested: str, returned: str) -> bool:
    def norm(t: str) -> str:
        return t.split(":", 1)[-1].lower()

    return norm(requested) == norm(returned)


def _edgar_tag_lookup_candidates(tags: list[str]) -> list[str]:
    candidates: list[str] = []
    seen: set[str] = set()
    for tag in tags:
        raw = str(tag or "").strip()
        if not raw:
            continue
        for candidate in (raw, raw.rsplit(":", 1)[-1]):
            if candidate and candidate not in seen:
                seen.add(candidate)
                candidates.append(candidate)
    return candidates


def _call_edgar_metric_fetcher(
    edgar_fetcher: EdgarFetcher,
    ticker: str,
    metric_name: str,
    most_recent_fy: int,
    n_historical: int,
    *,
    include_equivalents: bool = False,
    axis_key: str | None = None,
) -> dict:
    kwargs: dict[str, object] = {}
    if include_equivalents:
        kwargs["include_equivalents"] = True
    if axis_key is not None:
        kwargs["axis_key"] = axis_key
    try:
        return edgar_fetcher(ticker, metric_name, most_recent_fy, n_historical, **kwargs) or {}
    except TypeError:
        try:
            if include_equivalents and axis_key is not None:
                return edgar_fetcher(
                    ticker,
                    metric_name,
                    most_recent_fy,
                    n_historical,
                    axis_key=axis_key,
                ) or {}
            return edgar_fetcher(ticker, metric_name, most_recent_fy, n_historical) or {}
        except Exception as exc:
            logging.warning("EDGAR fetch failed for metric '%s': %s", metric_name, exc)
            return {
                "status": "error",
                "periods_failed": n_historical,
                "error": str(exc),
            }
    except Exception as exc:
        logging.warning("EDGAR fetch failed for metric '%s': %s", metric_name, exc)
        return {
            "status": "error",
            "periods_failed": n_historical,
            "error": str(exc),
        }


def _edgar_fetch_error_message(response: dict) -> str | None:
    for key in ("error", "message", "detail", "reason"):
        value = response.get(key)
        if value:
            return str(value)
    return None


def _requested_years_for_fetch(most_recent_fy: int, n_historical: int) -> set[int]:
    return set(range(most_recent_fy - n_historical + 1, most_recent_fy + 1))


def _registry_failed_result(
    requested_years: set[int],
    *,
    api_calls: int,
    error_message: str | None = None,
) -> EdgarConceptFetchResult:
    return EdgarConceptFetchResult(
        values_dict={},
        failed_years=set(requested_years),
        status="failed",
        periods_failed=len(requested_years),
        api_calls=api_calls,
        error_message=error_message,
    )


_SEEN_DEPRECATED_REGISTRY_GROUPS: set[str] = set()


def _log_deprecated_registry_group_once(requested_group_id: str, replacement_group_id: str) -> None:
    key = f"{requested_group_id}->{replacement_group_id}"
    if key in _SEEN_DEPRECATED_REGISTRY_GROUPS:
        return
    _SEEN_DEPRECATED_REGISTRY_GROUPS.add(key)
    logging.warning(
        "Registry group '%s' is deprecated; update taxonomy to '%s'",
        requested_group_id,
        replacement_group_id,
    )


def _resolve_registry_response_group(
    *,
    ticker: str,
    requested_group_id: str,
    returned_group_id: str | None,
    requested_group: EquivalenceGroup | None,
    allow_refresh: bool,
) -> EquivalenceGroup | None:
    cache = get_registry_cache()
    current_requested_group = requested_group or cache.get_group(requested_group_id)
    replacement_group_id = (
        current_requested_group.replaced_by
        if current_requested_group is not None
        else None
    )
    if returned_group_id == requested_group_id and current_requested_group is not None:
        return current_requested_group
    if (
        replacement_group_id
        and returned_group_id == replacement_group_id
    ):
        replacement_group = cache.get_group(replacement_group_id)
        if replacement_group is not None:
            _log_deprecated_registry_group_once(requested_group_id, replacement_group_id)
            return replacement_group

    if allow_refresh:
        cache.refresh()
        refreshed_requested_group = cache.get_group(requested_group_id)
        return _resolve_registry_response_group(
            ticker=ticker,
            requested_group_id=requested_group_id,
            returned_group_id=returned_group_id,
            requested_group=refreshed_requested_group,
            allow_refresh=False,
        )

    logging.warning(
        "edgar_registry_group_mismatch ticker=%s requested=%s returned=%s",
        ticker,
        requested_group_id,
        returned_group_id,
    )
    return None


def _metric_tag_matches_group(
    returned_tag: str,
    *,
    group: EquivalenceGroup,
    ticker: str,
) -> bool:
    return any(
        _tags_equivalent(candidate, returned_tag)
        for candidate in group.effective_merge_candidates(ticker)
    )


def _equivalent_tag_validated_by_registry(
    *,
    ticker: str,
    requested_tag: str | None,
    returned_tag: str,
    returned_group_id: str | None,
) -> bool:
    if not requested_tag or not returned_group_id:
        return False
    cache = get_registry_cache()
    group = cache.get_group(returned_group_id)
    if group is None:
        cache.refresh()
        group = cache.get_group(returned_group_id)
    if group is None:
        return False
    return _metric_tag_matches_group(
        requested_tag,
        group=group,
        ticker=ticker,
    ) and _metric_tag_matches_group(
        returned_tag,
        group=group,
        ticker=ticker,
    )


def _validated_registry_group_for_metric_tag(
    *,
    ticker: str,
    requested_group_id: str,
    returned_group_id: str | None,
    returned_tag: str,
    requested_group: EquivalenceGroup | None,
) -> EquivalenceGroup | None:
    matched_group = _resolve_registry_response_group(
        ticker=ticker,
        requested_group_id=requested_group_id,
        returned_group_id=returned_group_id,
        requested_group=requested_group,
        allow_refresh=False,
    )
    if matched_group is not None and _metric_tag_matches_group(
        returned_tag,
        group=matched_group,
        ticker=ticker,
    ):
        return matched_group

    cache = get_registry_cache()
    cache.refresh()
    refreshed_requested_group = cache.get_group(requested_group_id)
    matched_group = _resolve_registry_response_group(
        ticker=ticker,
        requested_group_id=requested_group_id,
        returned_group_id=returned_group_id,
        requested_group=refreshed_requested_group,
        allow_refresh=False,
    )
    if matched_group is not None and _metric_tag_matches_group(
        returned_tag,
        group=matched_group,
        ticker=ticker,
    ):
        return matched_group

    logging.warning(
        "edgar_registry_tag_mismatch ticker=%s requested=%s returned_group=%s returned_tag=%s",
        ticker,
        requested_group_id,
        returned_group_id,
        returned_tag,
    )
    return None


def _registry_equivalence_value_conflict(
    existing: SourceValue,
    incoming: SourceValue,
) -> bool:
    if existing.normalized_value is None or incoming.normalized_value is None:
        return False
    if _tags_equivalent(existing.tag or "", incoming.tag or ""):
        return False
    existing_value = float(existing.normalized_value)
    incoming_value = float(incoming.normalized_value)
    tolerance = max(
        _REGISTRY_EQUIVALENCE_VALUE_TOLERANCE_ABS_M,
        max(abs(existing_value), abs(incoming_value))
        * _REGISTRY_EQUIVALENCE_VALUE_TOLERANCE_PCT,
    )
    return abs(existing_value - incoming_value) > tolerance


def _fetch_edgar_concept(
    ticker: str,
    concept_id: str,
    concept: DataSourceMapping,
    most_recent_fy: int,
    n_historical: int,
    edgar_fetcher: EdgarFetcher,
) -> tuple[Dict[int, float], set[int], str, int, int]:
    return _fetch_edgar_concept_result(
        ticker=ticker,
        concept_id=concept_id,
        concept=concept,
        most_recent_fy=most_recent_fy,
        n_historical=n_historical,
        edgar_fetcher=edgar_fetcher,
    ).as_tuple()


def _fetch_edgar_concept_result(
    ticker: str,
    concept_id: str,
    concept: DataSourceMapping,
    most_recent_fy: int,
    n_historical: int,
    edgar_fetcher: EdgarFetcher,
) -> EdgarConceptFetchResult:
    flag = get_equivalence_flag()

    if flag == "true" and concept.registry_group_id:
        result = _fetch_via_registry(
            ticker=ticker,
            concept_id=concept_id,
            concept=concept,
            most_recent_fy=most_recent_fy,
            n_historical=n_historical,
            edgar_fetcher=edgar_fetcher,
        )
    elif flag == "shadow":
        if concept.registry_group_id and concept.edgar_tags:
            result = _fetch_legacy_edgar_concept(
                ticker=ticker,
                concept_id=concept_id,
                concept=concept,
                most_recent_fy=most_recent_fy,
                n_historical=n_historical,
                edgar_fetcher=edgar_fetcher,
            )
            if not is_analyst_cron_mode():
                _run_shadow_compare(
                    ticker=ticker,
                    concept_id=concept_id,
                    concept=concept,
                    legacy_result=result,
                    most_recent_fy=most_recent_fy,
                    n_historical=n_historical,
                    edgar_fetcher=edgar_fetcher,
                )
        elif concept.registry_group_id:
            result = _fetch_via_registry(
                ticker=ticker,
                concept_id=concept_id,
                concept=concept,
                most_recent_fy=most_recent_fy,
                n_historical=n_historical,
                edgar_fetcher=edgar_fetcher,
            )
        else:
            result = _fetch_legacy_edgar_concept(
                ticker=ticker,
                concept_id=concept_id,
                concept=concept,
                most_recent_fy=most_recent_fy,
                n_historical=n_historical,
                edgar_fetcher=edgar_fetcher,
            )
    else:
        result = _fetch_legacy_edgar_concept(
            ticker=ticker,
            concept_id=concept_id,
            concept=concept,
            most_recent_fy=most_recent_fy,
            n_historical=n_historical,
            edgar_fetcher=edgar_fetcher,
        )

    return _maybe_total_equity_derived_fallback(
        result=result,
        ticker=ticker,
        concept_id=concept_id,
        concept=concept,
        most_recent_fy=most_recent_fy,
        n_historical=n_historical,
        edgar_fetcher=edgar_fetcher,
    )


def _run_shadow_compare(
    *,
    ticker: str,
    concept_id: str,
    concept: DataSourceMapping,
    legacy_result: EdgarConceptFetchResult,
    most_recent_fy: int,
    n_historical: int,
    edgar_fetcher: EdgarFetcher,
) -> None:
    from .equivalence_shadow import log_shadow_diffs

    upstream_result = _fetch_via_registry(
        ticker=ticker,
        concept_id=concept_id,
        concept=concept,
        most_recent_fy=most_recent_fy,
        n_historical=n_historical,
        edgar_fetcher=edgar_fetcher,
    )
    log_shadow_diffs(
        concept_id=concept_id,
        ticker=ticker,
        legacy_values=legacy_result.values_dict,
        legacy_provenance=legacy_result.provenance_by_year,
        upstream_values=upstream_result.values_dict,
        upstream_provenance=upstream_result.provenance_by_year,
    )


def _fetch_via_registry(
    *,
    ticker: str,
    concept_id: str,
    concept: DataSourceMapping,
    most_recent_fy: int,
    n_historical: int,
    edgar_fetcher: EdgarFetcher,
) -> EdgarConceptFetchResult:
    requested_group_id = str(concept.registry_group_id or "").strip()
    requested_years = _requested_years_for_fetch(most_recent_fy, n_historical)
    if not requested_group_id:
        return EdgarConceptFetchResult(
            values_dict={},
            failed_years=set(),
            status="missing",
            periods_failed=0,
            api_calls=0,
        )

    cache = get_registry_cache()
    requested_group = cache.get_group(requested_group_id)
    if requested_group is None:
        cache.refresh()
        requested_group = cache.get_group(requested_group_id)
    if requested_group is None:
        if getattr(cache, "last_error", None) is not None and cache.registry_revision is None:
            return _registry_failed_result(requested_years, api_calls=0)
        logging.error(
            "Registry group '%s' missing for concept '%s'",
            requested_group_id,
            concept_id,
        )
        return _registry_failed_result(requested_years, api_calls=0)
    if requested_group.deprecated and requested_group.split_into:
        logging.error(
            "Registry group '%s' was split into %s; concept '%s' requires manual migration",
            requested_group_id,
            ", ".join(requested_group.split_into),
            concept_id,
        )
        return _registry_failed_result(requested_years, api_calls=0)

    api_calls = 1
    response = _call_edgar_metric_fetcher(
        edgar_fetcher,
        ticker,
        requested_group_id,
        most_recent_fy,
        n_historical,
        include_equivalents=True,
    )
    top_level_status = str(response.get("status") or "").lower()
    if top_level_status == "error":
        reason = str(response.get("reason") or "").lower()
        if reason == "group_split":
            logging.error(
                "Registry group '%s' returned group_split for concept '%s'",
                requested_group_id,
                concept_id,
            )
        return _registry_failed_result(
            requested_years,
            api_calls=api_calls,
            error_message=_edgar_fetch_error_message(response),
        )

    parsed = _parse_edgar_series_source_result(
        response,
        concept_id,
        concept,
        ticker=ticker,
        requested_registry_group_id=requested_group_id,
        requested_registry_group=requested_group,
    )
    values_dict = {
        year: value
        for year, value in parsed.values_dict.items()
        if year in requested_years
    }
    provenance_by_year = {
        year: provenance
        for year, provenance in parsed.provenance_by_year.items()
        if year in requested_years
    }
    source_values_by_year = {
        year: source_value
        for year, source_value in parsed.source_values_by_year.items()
        if year in requested_years
    }
    reported_period_ends_by_year = {
        year: reported_period_end
        for year, reported_period_end in parsed.reported_period_ends_by_year.items()
        if year in requested_years
    }

    top_level_failed = _safe_int(response.get("periods_failed")) or 0
    unresolved_failed_years = {
        year for year in parsed.failed_years
        if year in requested_years and year not in values_dict
    }
    if top_level_failed > parsed.entry_failed:
        unresolved_failed_years |= (requested_years - set(values_dict))

    unresolved_failed_years -= set(values_dict)
    unresolved_failed_years &= requested_years

    if not values_dict:
        if top_level_failed > 0 or parsed.entry_failed > 0 or unresolved_failed_years:
            return _registry_failed_result(requested_years, api_calls=api_calls)
        return EdgarConceptFetchResult(
            values_dict={},
            failed_years=set(),
            status="missing",
            periods_failed=0,
            api_calls=api_calls,
        )

    return EdgarConceptFetchResult(
        values_dict=values_dict,
        failed_years=unresolved_failed_years,
        status="ok",
        periods_failed=len(unresolved_failed_years),
        api_calls=api_calls,
        provenance_by_year=provenance_by_year,
        source_values_by_year=source_values_by_year,
        reported_period_ends_by_year=reported_period_ends_by_year,
    )


def _fetch_dimensional_edgar_concept(
    *,
    ticker: str,
    concept_id: str,
    concept: DataSourceMapping,
    axis_key: str,
    most_recent_fy: int,
    n_historical: int,
    edgar_fetcher: EdgarFetcher,
    include_equivalents: bool = False,
    allow_equivalent_tags: bool = False,
    include_local_tag_candidates: bool = False,
) -> EdgarConceptFetchResult:
    tags = concept.edgar_tags or []
    if not tags:
        return EdgarConceptFetchResult(
            values_dict={},
            failed_years=set(),
            status="missing",
            periods_failed=0,
            api_calls=0,
        )

    tags_to_try = _edgar_tag_lookup_candidates(tags) if include_local_tag_candidates else tags[:1]
    requested_years = _requested_years_for_fetch(most_recent_fy, n_historical)
    accumulated: Dict[int, float] = {}
    accumulated_provenance: Dict[int, EdgarProvenance] = {}
    accumulated_source_values: Dict[int, SourceValue] = {}
    accumulated_reported_period_ends: Dict[int, str] = {}
    unresolved_failed_years: set[int] = set()
    api_calls = 0

    for tag in tags_to_try:
        api_calls += 1
        response = _call_edgar_metric_fetcher(
            edgar_fetcher,
            ticker,
            tag,
            most_recent_fy,
            n_historical,
            include_equivalents=include_equivalents,
            axis_key=axis_key,
        )
        top_level_status = str(response.get("status") or "").lower()
        top_level_failed = _safe_int(response.get("periods_failed")) or 0

        if top_level_status == "error":
            error_message = _edgar_fetch_error_message(response)
            if (
                top_level_failed > 0
                and not accumulated
                and not include_local_tag_candidates
            ):
                return EdgarConceptFetchResult(
                    values_dict={},
                    failed_years=set(),
                    status="failed",
                    periods_failed=top_level_failed,
                    api_calls=api_calls,
                    error_message=error_message,
                )
            unresolved_failed_years |= (requested_years - set(accumulated))
            continue

        parsed = _parse_edgar_series_source_result(
            response,
            concept_id,
            concept,
            ticker=ticker,
            requested_tag=tag,
            allow_equivalent_tags=allow_equivalent_tags,
        )
        for year, value in parsed.values_dict.items():
            if year in requested_years and year not in accumulated:
                accumulated[year] = value
                if year in parsed.provenance_by_year:
                    accumulated_provenance[year] = parsed.provenance_by_year[year]
                if year in parsed.source_values_by_year:
                    accumulated_source_values[year] = parsed.source_values_by_year[year]
                if year in parsed.reported_period_ends_by_year:
                    accumulated_reported_period_ends[year] = parsed.reported_period_ends_by_year[year]
        unresolved_failed_years |= {
            year for year in parsed.failed_years
            if year in requested_years and year not in accumulated
        }
        if top_level_failed > parsed.entry_failed:
            unresolved_failed_years |= (requested_years - set(accumulated))
        if requested_years.issubset(accumulated):
            break

    unresolved_failed_years -= set(accumulated)
    unresolved_failed_years &= requested_years

    return EdgarConceptFetchResult(
        values_dict=accumulated,
        failed_years=unresolved_failed_years,
        status="ok" if accumulated else "missing",
        periods_failed=len(unresolved_failed_years),
        api_calls=api_calls,
        provenance_by_year=accumulated_provenance,
        source_values_by_year=accumulated_source_values,
        reported_period_ends_by_year=accumulated_reported_period_ends,
    )


def _fetch_legacy_edgar_concept(
    *,
    ticker: str,
    concept_id: str,
    concept: DataSourceMapping,
    most_recent_fy: int,
    n_historical: int,
    edgar_fetcher: EdgarFetcher,
    include_equivalents: bool = False,
    allow_equivalent_tags: bool = False,
    include_local_tag_candidates: bool = False,
) -> EdgarConceptFetchResult:
    tags = concept.edgar_tags or []
    if not tags:
        return EdgarConceptFetchResult(
            values_dict={},
            failed_years=set(),
            status="missing",
            periods_failed=0,
            api_calls=0,
        )

    boundary = concept.non_equivalent_after
    if boundary is not None:
        tags_to_try = tags[: max(0, boundary) + 1]
    else:
        tags_to_try = tags[:1]
    if include_local_tag_candidates:
        tags_to_try = _edgar_tag_lookup_candidates(tags_to_try)

    choose_single_tag = (
        len(tags_to_try) > 1
        and concept.nonadmissible_reason_code == NonadmissibleReasonCode.broader_or_narrower_scope
    )
    requested_years = _requested_years_for_fetch(most_recent_fy, n_historical)
    accumulated: Dict[int, float] = {}
    accumulated_provenance: Dict[int, EdgarProvenance] = {}
    accumulated_source_values: Dict[int, SourceValue] = {}
    accumulated_reported_period_ends: Dict[int, str] = {}
    unresolved_failed_years: set[int] = set()
    api_calls = 0
    tag_results: list[EdgarConceptFetchResult] = []

    for tag in tags_to_try:
        tag_result = _single_tag_lookup(
            ticker,
            tag,
            most_recent_fy,
            n_historical,
            edgar_fetcher,
            concept_id=concept_id,
            concept=concept,
            include_equivalents=include_equivalents,
            allow_equivalent_tags=allow_equivalent_tags,
        )
        api_calls += tag_result.api_calls

        if choose_single_tag:
            tag_results.append(tag_result)
            continue

        if tag_result.status == "failed" and not tag_result.values_dict:
            if include_local_tag_candidates:
                unresolved_failed_years |= (requested_years - set(accumulated))
                continue
            if accumulated:
                unresolved_failed_years |= (requested_years - set(accumulated))
                break
            tag_result.api_calls = api_calls
            return tag_result

        for year, value in tag_result.values_dict.items():
            if year in requested_years and year not in accumulated:
                accumulated[year] = value
                if year in tag_result.provenance_by_year:
                    accumulated_provenance[year] = tag_result.provenance_by_year[year]
                if year in tag_result.source_values_by_year:
                    accumulated_source_values[year] = tag_result.source_values_by_year[year]
                if year in tag_result.reported_period_ends_by_year:
                    accumulated_reported_period_ends[year] = (
                        tag_result.reported_period_ends_by_year[year]
                    )

        unresolved_failed_years |= {
            year for year in tag_result.failed_years
            if year in requested_years and year not in accumulated
        }

        if requested_years.issubset(accumulated):
            break

    if choose_single_tag:
        return _select_single_scope_edgar_tag_result(
            tag_results,
            requested_years=requested_years,
            api_calls=api_calls,
        )

    unresolved_failed_years -= set(accumulated)
    unresolved_failed_years &= requested_years
    unresolved_periods_failed = len(unresolved_failed_years)

    if not accumulated:
        return EdgarConceptFetchResult(
            values_dict={},
            failed_years=unresolved_failed_years,
            status="missing",
            periods_failed=unresolved_periods_failed,
            api_calls=api_calls,
        )

    return EdgarConceptFetchResult(
        values_dict=accumulated,
        failed_years=unresolved_failed_years,
        status="ok",
        periods_failed=unresolved_periods_failed,
        api_calls=api_calls,
        provenance_by_year=accumulated_provenance,
        source_values_by_year=accumulated_source_values,
        reported_period_ends_by_year=accumulated_reported_period_ends,
    )


def _select_single_scope_edgar_tag_result(
    tag_results: list[EdgarConceptFetchResult],
    *,
    requested_years: set[int],
    api_calls: int,
) -> EdgarConceptFetchResult:
    best_result: EdgarConceptFetchResult | None = None
    best_coverage = -1
    for result in tag_results:
        coverage = len(set(result.values_dict).intersection(requested_years))
        if coverage > best_coverage:
            best_result = result
            best_coverage = coverage

    if best_result is not None and best_coverage > 0:
        values = {
            year: value
            for year, value in best_result.values_dict.items()
            if year in requested_years
        }
        failed_years = (
            set(best_result.failed_years)
            | (requested_years - set(values))
        ) - set(values)
        return EdgarConceptFetchResult(
            values_dict=values,
            failed_years=failed_years,
            status="ok",
            periods_failed=len(failed_years),
            api_calls=api_calls,
            provenance_by_year={
                year: provenance
                for year, provenance in best_result.provenance_by_year.items()
                if year in values
            },
            source_values_by_year={
                year: source_value
                for year, source_value in best_result.source_values_by_year.items()
                if year in values
            },
            reported_period_ends_by_year={
                year: period_end
                for year, period_end in best_result.reported_period_ends_by_year.items()
                if year in values
            },
        )

    failed_result = next(
        (result for result in tag_results if result.status == "failed" and not result.values_dict),
        None,
    )
    if failed_result is not None:
        failed_result.api_calls = api_calls
        return failed_result

    unresolved_failed_years = (
        set().union(*(result.failed_years for result in tag_results))
        if tag_results
        else set()
    )
    unresolved_failed_years &= requested_years
    return EdgarConceptFetchResult(
        values_dict={},
        failed_years=unresolved_failed_years,
        status="missing",
        periods_failed=len(unresolved_failed_years),
        api_calls=api_calls,
    )


def _single_tag_lookup(
    ticker: str,
    tag: str,
    most_recent_fy: int,
    n_historical: int,
    edgar_fetcher: EdgarFetcher,
    *,
    concept_id: str,
    concept: DataSourceMapping,
    include_equivalents: bool = False,
    allow_equivalent_tags: bool = False,
) -> EdgarConceptFetchResult:
    requested_years = _requested_years_for_fetch(most_recent_fy, n_historical)
    api_calls = 1
    response = _call_edgar_metric_fetcher(
        edgar_fetcher,
        ticker,
        tag,
        most_recent_fy,
        n_historical,
        include_equivalents=include_equivalents,
    )
    top_level_status = str(response.get("status") or "").lower()
    top_level_failed = _safe_int(response.get("periods_failed"))

    if top_level_status == "error":
        error_message = _edgar_fetch_error_message(response)
        if top_level_failed > 0:
            return EdgarConceptFetchResult(
                values_dict={},
                failed_years=set(),
                status="failed",
                periods_failed=top_level_failed,
                api_calls=api_calls,
                error_message=error_message,
            )
        return EdgarConceptFetchResult(
            values_dict={},
            failed_years=set(),
            status="missing",
            periods_failed=0,
            api_calls=api_calls,
            error_message=error_message,
        )

    parsed = _parse_edgar_series_source_result(
        response,
        concept_id,
        concept,
        ticker=ticker,
        requested_tag=tag,
        allow_equivalent_tags=allow_equivalent_tags,
    )
    values_dict = {
        year: value
        for year, value in parsed.values_dict.items()
        if year in requested_years
    }
    provenance_by_year = {
        year: provenance
        for year, provenance in parsed.provenance_by_year.items()
        if year in requested_years
    }
    source_values_by_year = {
        year: source_value
        for year, source_value in parsed.source_values_by_year.items()
        if year in requested_years
    }
    reported_period_ends_by_year = {
        year: reported_period_end
        for year, reported_period_end in parsed.reported_period_ends_by_year.items()
        if year in requested_years
    }

    unresolved_failed_years = {
        year for year in parsed.failed_years
        if year in requested_years and year not in values_dict
    }
    if top_level_failed > parsed.entry_failed:
        unresolved_failed_years |= (requested_years - set(values_dict))

    unresolved_failed_years -= set(values_dict)
    unresolved_failed_years &= requested_years
    unresolved_periods_failed = len(unresolved_failed_years)

    if not values_dict:
        return EdgarConceptFetchResult(
            values_dict={},
            failed_years=unresolved_failed_years,
            status="missing",
            periods_failed=unresolved_periods_failed,
            api_calls=api_calls,
        )

    return EdgarConceptFetchResult(
        values_dict=values_dict,
        failed_years=unresolved_failed_years,
        status="ok",
        periods_failed=unresolved_periods_failed,
        api_calls=api_calls,
        provenance_by_year=provenance_by_year,
        source_values_by_year=source_values_by_year,
        reported_period_ends_by_year=reported_period_ends_by_year,
    )


def _total_equity_derived_fallback(
    ticker: str,
    concept: DataSourceMapping,
    base_result: EdgarConceptFetchResult,
    most_recent_fy: int,
    n_historical: int,
    edgar_fetcher: EdgarFetcher,
) -> EdgarConceptFetchResult:
    requested_years = _requested_years_for_fetch(most_recent_fy, n_historical)
    missing_years = requested_years - set(base_result.values_dict)
    if not missing_years:
        return base_result

    augmented_values = dict(base_result.values_dict)
    augmented_provenance = dict(base_result.provenance_by_year or {})
    augmented_source_values = dict(base_result.source_values_by_year or {})
    augmented_reported_period_ends = dict(base_result.reported_period_ends_by_year or {})
    api_calls = base_result.api_calls

    parent_alt = _single_tag_lookup(
        ticker,
        "StockholdersEquityAttributableToParent",
        most_recent_fy,
        n_historical,
        edgar_fetcher,
        concept_id="total_equity",
        concept=concept,
    )
    api_calls += parent_alt.api_calls

    parent_alt_failed_years: set[int] = set()
    if parent_alt.status == "failed" and not parent_alt.values_dict:
        parent_alt_failed_years = set(missing_years)
    elif parent_alt.status not in {"ok", "missing"} and not parent_alt.values_dict:
        parent_alt_failed_years = set(missing_years)
    elif parent_alt.failed_years:
        parent_alt_failed_years = set(parent_alt.failed_years) & set(missing_years)

    for year in list(missing_years):
        value = parent_alt.values_dict.get(year)
        if value is None:
            continue
        augmented_values[year] = value
        if year in parent_alt.provenance_by_year:
            augmented_provenance[year] = parent_alt.provenance_by_year[year]
        if year in parent_alt.source_values_by_year:
            augmented_source_values[year] = parent_alt.source_values_by_year[year]
        if year in parent_alt.reported_period_ends_by_year:
            augmented_reported_period_ends[year] = parent_alt.reported_period_ends_by_year[year]
        missing_years.discard(year)
        parent_alt_failed_years.discard(year)

    if not missing_years:
        final_failed = base_result.failed_years - set(augmented_values)
        return EdgarConceptFetchResult(
            values_dict=augmented_values,
            failed_years=final_failed,
            status="ok",
            periods_failed=len(final_failed),
            api_calls=api_calls,
            provenance_by_year=augmented_provenance,
            source_values_by_year=augmented_source_values,
            reported_period_ends_by_year=augmented_reported_period_ends,
        )

    with_nci = _single_tag_lookup(
        ticker,
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
        most_recent_fy,
        n_historical,
        edgar_fetcher,
        concept_id="total_equity",
        concept=concept,
    )
    api_calls += with_nci.api_calls

    if not with_nci.values_dict:
        derived_failed_years_pre: set[int] = set()
        if with_nci.status == "failed":
            derived_failed_years_pre = set(missing_years)
        elif with_nci.status not in {"ok", "missing"}:
            derived_failed_years_pre = set(missing_years)
        elif with_nci.failed_years:
            derived_failed_years_pre = set(with_nci.failed_years) & set(missing_years)
        final_failed_pre = (
            base_result.failed_years
            | parent_alt_failed_years
            | derived_failed_years_pre
        ) - set(augmented_values)
        return EdgarConceptFetchResult(
            values_dict=augmented_values,
            failed_years=final_failed_pre,
            status=base_result.status if not augmented_values else "ok",
            periods_failed=len(final_failed_pre),
            api_calls=api_calls,
            provenance_by_year=augmented_provenance,
            source_values_by_year=augmented_source_values,
            reported_period_ends_by_year=augmented_reported_period_ends,
        )

    mi = _single_tag_lookup(
        ticker,
        "MinorityInterest",
        most_recent_fy,
        n_historical,
        edgar_fetcher,
        concept_id="total_equity",
        concept=concept,
    )
    api_calls += mi.api_calls

    derived_failed_years: set[int] = set()
    for year in list(missing_years):
        nci_value = with_nci.values_dict.get(year)
        if nci_value is None:
            if year in with_nci.failed_years:
                derived_failed_years.add(year)
            continue

        if mi.status == "failed" or year in mi.failed_years:
            derived_failed_years.add(year)
            continue
        if mi.values_dict.get(year) is not None:
            mi_value = float(mi.values_dict[year])
        elif mi.status in {"ok", "missing"}:
            mi_value = 0.0
        else:
            derived_failed_years.add(year)
            continue

        derived_value = float(nci_value) - mi_value
        augmented_values[year] = derived_value
        augmented_provenance[year] = EdgarProvenance(
            metric_tag="derived:WithNCI_minus_MinorityInterest",
        )
        augmented_source_values[year] = normalize_edgar_source_value(
            value=derived_value,
            scale="millions",
            concept_id="total_equity",
            source="edgar",
            tag="derived:WithNCI_minus_MinorityInterest",
            year=year,
            source_ref={
                "derivation": "WithNCI - MinorityInterest",
                "withnci_value": float(nci_value),
                "mi_value": mi_value,
            },
        )
        if year in with_nci.reported_period_ends_by_year:
            augmented_reported_period_ends[year] = with_nci.reported_period_ends_by_year[year]

    final_failed = (
        base_result.failed_years
        | parent_alt_failed_years
        | derived_failed_years
    ) - set(augmented_values)
    return EdgarConceptFetchResult(
        values_dict=augmented_values,
        failed_years=final_failed,
        status="ok" if augmented_values else base_result.status,
        periods_failed=len(final_failed),
        api_calls=api_calls,
        provenance_by_year=augmented_provenance,
        source_values_by_year=augmented_source_values,
        reported_period_ends_by_year=augmented_reported_period_ends,
    )


def _maybe_total_equity_derived_fallback(
    *,
    result: EdgarConceptFetchResult,
    ticker: str,
    concept_id: str,
    concept: DataSourceMapping,
    most_recent_fy: int,
    n_historical: int,
    edgar_fetcher: EdgarFetcher,
) -> EdgarConceptFetchResult:
    if concept_id != "total_equity":
        return result
    if result.status not in {"ok", "missing"}:
        return result
    requested_years = _requested_years_for_fetch(most_recent_fy, n_historical)
    if requested_years.issubset(set(result.values_dict)):
        return result
    return _total_equity_derived_fallback(
        ticker=ticker,
        concept=concept,
        base_result=result,
        most_recent_fy=most_recent_fy,
        n_historical=n_historical,
        edgar_fetcher=edgar_fetcher,
    )


def _parse_edgar_series(
    response: dict,
    concept_id: str,
    concept: DataSourceMapping,
    *,
    ticker: str,
    requested_tag: str,
    allow_equivalent_tags: bool = False,
) -> tuple[Dict[int, float], set[int], int]:
    values_dict, failed_years, entry_failed, _provenance_by_year = _parse_edgar_series_result(
        response,
        concept_id,
        concept,
        ticker=ticker,
        requested_tag=requested_tag,
        allow_equivalent_tags=allow_equivalent_tags,
    )
    return values_dict, failed_years, entry_failed


def _parse_edgar_series_result(
    response: dict,
    concept_id: str,
    concept: DataSourceMapping,
    *,
    ticker: str,
    requested_tag: str | None = None,
    requested_registry_group_id: str | None = None,
    requested_registry_group: EquivalenceGroup | None = None,
    allow_equivalent_tags: bool = False,
) -> tuple[Dict[int, float], set[int], int, Dict[int, EdgarProvenance]]:
    """Parse EDGAR series response with legacy tuple return shape."""

    parsed = _parse_edgar_series_source_result(
        response,
        concept_id,
        concept,
        ticker=ticker,
        requested_tag=requested_tag,
        requested_registry_group_id=requested_registry_group_id,
        requested_registry_group=requested_registry_group,
        allow_equivalent_tags=allow_equivalent_tags,
    )
    return (
        parsed.values_dict,
        parsed.failed_years,
        parsed.entry_failed,
        parsed.provenance_by_year,
    )


def _parse_edgar_series_source_result(
    response: dict,
    concept_id: str,
    concept: DataSourceMapping,
    *,
    ticker: str,
    requested_tag: str | None = None,
    requested_registry_group_id: str | None = None,
    requested_registry_group: EquivalenceGroup | None = None,
    allow_equivalent_tags: bool = False,
) -> ParsedEdgarSeriesResult:
    """Parse EDGAR series response with raw source-value metadata."""

    series = response.get("series")
    if not isinstance(series, list):
        series = []

    values_dict: Dict[int, float] = {}
    failed_years: set[int] = set()
    entry_failed = 0
    provenance_by_year: Dict[int, EdgarProvenance] = {}
    source_values_by_year: Dict[int, SourceValue] = {}
    reported_period_ends_by_year: Dict[int, str] = {}
    registry_value_conflict_years: set[int] = set()

    for entry in series:
        if not isinstance(entry, dict):
            continue
        entry_status = str(entry.get("status") or "").lower()
        year = _safe_int(entry.get("year"), default=None)
        if entry_status in {"error", "locked"}:
            if year is not None:
                failed_years.add(year)
            entry_failed += 1
            continue
        if entry_status != "ok":
            continue
        returned_tag = entry.get("metric_tag")
        matched_registry_group: EquivalenceGroup | None = None
        if requested_registry_group_id:
            if not isinstance(returned_tag, str) or not returned_tag:
                if year is not None:
                    failed_years.add(year)
                entry_failed += 1
                continue
            returned_group_id = entry.get("equivalence_group_id")
            if not isinstance(returned_group_id, str) or not returned_group_id:
                logging.warning(
                    "edgar_registry_group_mismatch ticker=%s requested=%s returned=%s",
                    ticker,
                    requested_registry_group_id,
                    returned_group_id,
                )
                if year is not None:
                    failed_years.add(year)
                entry_failed += 1
                continue
            matched_registry_group = _validated_registry_group_for_metric_tag(
                ticker=ticker,
                requested_group_id=requested_registry_group_id,
                returned_group_id=returned_group_id,
                returned_tag=returned_tag,
                requested_group=requested_registry_group,
            )
            if matched_registry_group is None:
                if year is not None:
                    failed_years.add(year)
                entry_failed += 1
                continue
        elif (
            not isinstance(returned_tag, str)
            or not returned_tag
            or (
                not _tags_equivalent(requested_tag or "", returned_tag)
                and not (
                    allow_equivalent_tags
                    and _equivalent_tag_validated_by_registry(
                        ticker=ticker,
                        requested_tag=requested_tag,
                        returned_tag=returned_tag,
                        returned_group_id=(
                            entry.get("equivalence_group_id")
                            if isinstance(entry.get("equivalence_group_id"), str)
                            else None
                        ),
                    )
                )
            )
        ):
            logging.warning(
                "edgar_tag_mismatch ticker=%s requested=%s returned=%s year=%s",
                ticker,
                requested_tag,
                returned_tag,
                year,
            )
            if year is not None:
                failed_years.add(year)
            entry_failed += 1
            continue
        if year is None or entry.get("value") is None:
            continue

        reported_period_end = _entry_reported_period_end(entry)
        source_value = normalize_edgar_source_value(
            entry["value"],
            entry.get("scale"),
            concept_id,
            source="edgar",
            tag=returned_tag,
            year=year,
            source_ref=entry.get("source_ref"),
        )
        scaled_value = source_value.normalized_value
        if scaled_value is None:
            continue
        if _edgar_negate_enabled(concept):
            scaled_value = -abs(scaled_value)
            source_value = replace(source_value, normalized_value=scaled_value)
        provenance = EdgarProvenance(
            registry_group_id=(
                matched_registry_group.group_id
                if matched_registry_group is not None
                else None
            ),
            metric_tag=returned_tag,
            registry_revision=(
                get_registry_cache().registry_revision
                if matched_registry_group is not None
                else None
            ),
        )
        existing_source_value = source_values_by_year.get(year)
        if (
            matched_registry_group is not None
            and existing_source_value is not None
            and _registry_equivalence_value_conflict(existing_source_value, source_value)
        ):
            logging.warning(
                "edgar_registry_equivalence_value_conflict ticker=%s group=%s year=%s "
                "existing_tag=%s existing_value=%s incoming_tag=%s incoming_value=%s "
                "tolerance_pct=%.4f tolerance_abs_m=%.4f",
                ticker,
                matched_registry_group.group_id,
                year,
                existing_source_value.tag,
                existing_source_value.normalized_value,
                source_value.tag,
                source_value.normalized_value,
                _REGISTRY_EQUIVALENCE_VALUE_TOLERANCE_PCT,
                _REGISTRY_EQUIVALENCE_VALUE_TOLERANCE_ABS_M,
            )
            registry_value_conflict_years.add(year)
            failed_years.add(year)
            entry_failed += 1
            values_dict.pop(year, None)
            provenance_by_year.pop(year, None)
            source_values_by_year.pop(year, None)
            reported_period_ends_by_year.pop(year, None)
            continue
        if year in registry_value_conflict_years:
            continue
        chosen_source_value = choose_preferred_source_value(
            existing_source_value,
            source_value,
        )
        if chosen_source_value is source_value:
            values_dict[year] = scaled_value
            provenance_by_year[year] = provenance
            source_values_by_year[year] = source_value
            if reported_period_end is not None:
                reported_period_ends_by_year[year] = reported_period_end

    return ParsedEdgarSeriesResult(
        values_dict=values_dict,
        failed_years=failed_years,
        entry_failed=entry_failed,
        provenance_by_year=provenance_by_year,
        source_values_by_year=source_values_by_year,
        reported_period_ends_by_year=reported_period_ends_by_year,
    )


def _set_imported_value(
    item: LineItem,
    year: int,
    value: float,
    provenance: ValueProvenance = ValueProvenance.imported_fmp,
    *,
    edgar_provenance: EdgarProvenance | None = None,
    fmp_provenance: FmpProvenance | None = None,
) -> None:
    if item.values is None:
        item.values = ValueSeries()
    item.values.values[int(year)] = ValueCell(
        period=int(year),
        value=float(value),
        provenance=provenance,
        edgar_provenance=edgar_provenance,
        fmp_provenance=fmp_provenance,
    )


def _set_constant_override(
    item: LineItem,
    year: int,
    value: float,
    *,
    synthetic: bool = False,
    edgar_provenance: EdgarProvenance | None = None,
    fmp_provenance: FmpProvenance | None = None,
) -> None:
    if item.overrides is None:
        item.overrides = {}
    item.overrides[int(year)] = FormulaSpec(
        type=FormulaType.constant,
        params={"value": float(value) if isinstance(value, int) else value},
        note="synthetic" if synthetic else None,
        edgar_provenance=edgar_provenance,
        fmp_provenance=fmp_provenance,
    )


_CASH_BEGINNING_ITEM_ID = "tpl.fm.cash_flow.cash_and_cash_equivalents_beginning_of_period"
_CASH_END_ITEM_ID = "tpl.fm.cash_flow.cash_and_cash_equivalents_end_of_period"
_NET_CHANGE_IN_CASH_ITEM_ID = "tpl.fm.cash_flow.net_change_in_cash_and_cash_equivalents"


def _seed_cash_beginning_of_period(model: FinancialModel) -> bool:
    """Seed the oldest historical beginning cash from ending cash and net change."""

    historical_periods = [
        int(period)
        for period in (
            model.time_structure.historical_periods
            or model.time_structure.historical_years
            or []
        )
    ]
    if not historical_periods:
        return False
    first_year = historical_periods[0]

    try:
        beginning_item = model.get_item(_CASH_BEGINNING_ITEM_ID)
        end_item = model.get_item(_CASH_END_ITEM_ID)
        net_change_item = model.get_item(_NET_CHANGE_IN_CASH_ITEM_ID)
    except KeyError:
        return False

    existing_beginning = _lookup_formula_value(
        model,
        _CASH_BEGINNING_ITEM_ID,
        first_year,
        {},
    )
    if existing_beginning is not None:
        return False

    end_cash = _lookup_formula_value(model, _CASH_END_ITEM_ID, first_year, {})
    if end_cash is None:
        end_cash = _evaluate_formula_simple(model, end_item, first_year, {})
    if end_cash is None:
        return False

    net_change = _lookup_formula_value(model, _NET_CHANGE_IN_CASH_ITEM_ID, first_year, {})
    if net_change is None:
        net_change = _evaluate_formula_simple(model, net_change_item, first_year, {})
    if net_change is None:
        return False

    _set_constant_override(beginning_item, first_year, float(end_cash) - float(net_change))
    return True


def _has_existing_imported_historicals(model: FinancialModel, historical_periods: List[int]) -> bool:
    historical_set = {int(period) for period in historical_periods}
    for item in _iter_items(model):
        if not item.data_concept_id:
            continue
        if item.values is not None:
            for year, cell in item.values.values.items():
                if (
                    int(year) in historical_set
                    and cell.provenance in {
                        ValueProvenance.imported_fmp,
                        ValueProvenance.imported_edgar,
                    }
                ):
                    return True
        if item.overrides:
            for year, spec in item.overrides.items():
                if int(year) in historical_set and spec.type is FormulaType.constant:
                    return True
    return False


def _safe_int(value, default: Optional[int] = 0) -> Optional[int]:
    try:
        return int(value) if value is not None else default
    except (TypeError, ValueError):
        return default


def _formula_period_is_valid(
    model: FinancialModel,
    item: LineItem,
    period: int,
    *,
    historical_set: set[int],
    projection_set: set[int],
    available_periods: set[int],
) -> bool:
    spec = _spec_for_period_validation(item, period, historical_set, projection_set)
    if spec is None:
        return True

    mode = model.time_structure.period_mode
    for offset in _collect_ref_offsets(spec.params):
        shifted = shift_period(int(period), int(offset), mode)
        if shifted is None or int(shifted) not in available_periods:
            return False
    return True


def _spec_for_period_validation(
    item: LineItem,
    period: int,
    historical_set: set[int],
    projection_set: set[int],
) -> Optional[FormulaSpec]:
    if item.overrides and int(period) in item.overrides:
        return item.overrides[int(period)]
    if int(period) in historical_set:
        return item.historical
    if int(period) in projection_set:
        return item.projected
    return item.projected or item.historical


def _collect_ref_offsets(obj) -> List[int]:
    offsets: List[int] = []
    if obj is None:
        return offsets
    if isinstance(obj, LineItemRef):
        return [int(obj.t)]
    if isinstance(obj, list):
        for value in obj:
            offsets.extend(_collect_ref_offsets(value))
        return offsets
    if isinstance(obj, tuple):
        for value in obj:
            offsets.extend(_collect_ref_offsets(value))
        return offsets
    if isinstance(obj, dict):
        if "id" in obj:
            try:
                return [int(obj.get("t", 0))]
            except (TypeError, ValueError):
                return []
        for value in obj.values():
            offsets.extend(_collect_ref_offsets(value))
    return offsets


def _available_periods(model: FinancialModel) -> Optional[set[int]]:
    """Return the model time-axis periods, or None when no axis is defined."""

    ts = model.time_structure
    historical = (
        getattr(ts, "historical_periods", None)
        or getattr(ts, "historical_years", None)
        or []
    )
    projection = (
        getattr(ts, "projection_periods", None)
        or getattr(ts, "projection_years", None)
        or []
    )
    if not historical and not projection:
        return None
    return {int(p) for p in historical} | {int(p) for p in projection}


def _all_refs_same_period(params) -> bool:
    offsets = _collect_ref_offsets(params)
    return bool(offsets) and all(offset == 0 for offset in offsets)


def _extract_single_ref(params) -> Optional[tuple[str, int]]:
    """Pull the single source ref from a ref-type formula's params."""

    if not isinstance(params, dict):
        return None
    source = params.get("source")
    if isinstance(source, LineItemRef):
        return (source.id, int(source.t))
    if isinstance(source, dict) and isinstance(source.get("id"), str):
        try:
            t_val = int(source.get("t", 0))
        except (TypeError, ValueError):
            t_val = 0
        return (source["id"], t_val)
    return None


def _extract_ref_ids(obj) -> set[str]:
    """Extract all line-item IDs referenced in formula params."""

    ids: set[str] = set()
    if obj is None:
        return ids
    if isinstance(obj, LineItemRef):
        return {obj.id}
    if isinstance(obj, dict):
        if "id" in obj and isinstance(obj["id"], str):
            return {obj["id"]}
        for value in obj.values():
            ids |= _extract_ref_ids(value)
        return ids
    if isinstance(obj, (list, tuple)):
        for value in obj:
            ids |= _extract_ref_ids(value)
    return ids


def _item_has_real_data(
    item: LineItem,
    period: int,
    *,
    model: Optional[FinancialModel] = None,
    _depth: int = 0,
    _seen: Optional[set[tuple[str, int]]] = None,
) -> bool:
    """Return True when an item has real data for a specific period."""

    if model is not None:
        available_periods = _available_periods(model)
        if available_periods is not None and int(period) not in available_periods:
            return False

    if item.values is not None and int(period) in item.values.values:
        cell = item.values.values[int(period)]
        if cell.value is not None:
            return True
    if item.overrides is not None and int(period) in item.overrides:
        spec = item.overrides[int(period)]
        if spec.note != "synthetic":
            return True

    if (
        model is not None
        and item.historical is not None
        and item.historical.type is FormulaType.ref
    ):
        if _depth >= _MAX_REF_CHAIN_DEPTH:
            return False
        key = (str(item.id), int(period))
        if key in (_seen or set()):
            return False
        ref_target = _extract_single_ref(item.historical.params)
        if ref_target is None:
            return False
        target_id, target_t = ref_target
        shifted = shift_period(int(period), int(target_t), model.time_structure.period_mode)
        if shifted is None:
            return False
        try:
            target_item = model.get_item(target_id)
        except KeyError:
            return False
        seen = (_seen or set()) | {key}
        return _item_has_real_data(
            target_item,
            int(shifted),
            model=model,
            _depth=_depth + 1,
            _seen=seen,
        )

    if (
        model is not None
        and item.historical is not None
        and item.historical.type is FormulaType.constant
    ):
        if _constant_override_value(item.historical) is not None:
            return True

    return False


def _item_has_direct_real_data(item: LineItem, period: int) -> bool:
    if item.values is not None and int(period) in item.values.values:
        cell = item.values.values[int(period)]
        if cell.value is not None:
            return True
    if item.overrides is not None and int(period) in item.overrides:
        spec = item.overrides[int(period)]
        return spec.note != "synthetic"
    return False


_PRESENTATION_RESIDUAL_NOTE = "presentation_residual"
_NON_CURRENT_ASSET_CATCH_ALL_ITEM_ID = "tpl.fm.balance_sheet.long_term_asset_2"


def _apply_presentation_catch_all_residuals(
    model: FinancialModel,
    *,
    taxonomy: Dict[str, DataSourceMapping],
    presentation_tree: PresentationTree,
    historical_periods: list[int],
) -> dict[str, set[int]]:
    """Residualize broad catch-all rows when presentation data proves overlap.

    Some filers populate both a broad catch-all fact and dedicated sub-lines. The
    presentation-tree walker can reconcile the section by choosing a non-
    overlapping basis, but the static model formula would still add every
    populated template row. Residualizing the catch-all before formula-first keeps
    explicit rows visible while preserving section formulas.
    """

    return _residualize_bs_section_catch_all(
        model,
        taxonomy=taxonomy,
        presentation_tree=presentation_tree,
        historical_periods=historical_periods,
        section_name="non_current_assets",
        catch_all_item_id=_NON_CURRENT_ASSET_CATCH_ALL_ITEM_ID,
    )


def _residualize_bs_section_catch_all(
    model: FinancialModel,
    *,
    taxonomy: Dict[str, DataSourceMapping],
    presentation_tree: PresentationTree,
    historical_periods: list[int],
    section_name: str,
    catch_all_item_id: str,
) -> dict[str, set[int]]:
    definition = BS_SECTIONS.get(section_name)
    if not definition:
        return {}

    selected = _select_bs_section_presentation_basis(
        model,
        taxonomy=taxonomy,
        presentation_tree=presentation_tree,
        definition=definition,
        historical_periods=historical_periods,
    )
    if not selected:
        return {}

    catch_all_item = model.get_item(catch_all_item_id)
    tag_to_concept = _build_taxonomy_tag_index(taxonomy)
    adjusted: dict[str, set[int]] = {}

    for year, children in selected.items():
        selected_concepts = {
            concept_id
            for child in children
            for concept_id in [tag_to_concept.get(child.tag)]
            if concept_id is not None
        }
        if catch_all_item.data_concept_id not in selected_concepts:
            continue

        unselected_explicit_sum = 0.0
        for member in definition["sub_lines"]:
            if member.template_item_id == catch_all_item_id:
                continue
            if member.expected_concept_id is None:
                continue
            if member.expected_concept_id in selected_concepts:
                continue
            value = _lookup_formula_value(model, member.template_item_id, int(year), {})
            if value is not None:
                unselected_explicit_sum += float(value)

        if abs(unselected_explicit_sum) < 1e-9:
            continue

        catch_all_value = _lookup_formula_value(model, catch_all_item_id, int(year), {})
        section_total = _bs_section_total_value(model, definition, int(year))
        section_sum = _bs_section_subline_sum(model, definition, int(year))
        if catch_all_value is None or section_total is None or section_sum is None:
            continue

        overage = float(section_sum) - float(section_total)
        tolerance = max(1.0, abs(float(section_total)) * 0.001)
        if abs(overage - unselected_explicit_sum) > tolerance:
            continue

        residual_value = float(catch_all_value) - overage
        if residual_value < -tolerance or residual_value > float(catch_all_value) + tolerance:
            continue

        _set_residualized_value(catch_all_item, int(year), residual_value)
        adjusted.setdefault(catch_all_item.id, set()).add(int(year))

    return adjusted


def _select_bs_section_presentation_basis(
    model: FinancialModel,
    *,
    taxonomy: Dict[str, DataSourceMapping],
    presentation_tree: PresentationTree,
    definition: dict,
    historical_periods: list[int],
) -> dict[int, list]:
    parent_candidates = tuple(definition.get("xbrl_section_parents", ()))
    tag_to_concept = _build_taxonomy_tag_index(taxonomy)

    def parent_present(tag: str) -> bool:
        return bool(presentation_tree.immediate_children_of(tag))

    selected_candidate = None
    children = ()
    for candidate in parent_candidates:
        if candidate.requires_companion and not parent_present(candidate.requires_companion):
            continue
        candidate_children = presentation_tree.immediate_children_of(candidate.parent)
        if candidate_children:
            selected_candidate = candidate
            children = candidate_children
            break

    if selected_candidate is None:
        return {}

    return {
        int(year): _select_non_overlapping_presentation_children(
            children,
            year=int(year),
            section_total_tags=selected_candidate.exclude_tags,
            definition=definition,
            section_members=_effective_section_members(model, definition),
            tag_to_concept=tag_to_concept,
            model=model,
            template_value_memo={},
            parent_tag=selected_candidate.parent,
        )
        for year in historical_periods
    }


def _bs_section_total_value(
    model: FinancialModel,
    definition: dict,
    year: int,
) -> float | None:
    total = _lookup_formula_value(model, definition["total_item_id"], int(year), {})
    if total is None:
        return None
    included_subtotal_id = definition.get("also_includes_subtotal")
    if not included_subtotal_id:
        return float(total)
    included = _lookup_formula_value(model, included_subtotal_id, int(year), {})
    if included is None:
        return None
    return float(total) - float(included)


def _bs_section_subline_sum(
    model: FinancialModel,
    definition: dict,
    year: int,
) -> float | None:
    total = 0.0
    saw_value = False
    for member in definition["sub_lines"]:
        value = _lookup_formula_value(model, member.template_item_id, int(year), {})
        if value is None:
            continue
        saw_value = True
        total += float(value)
    return total if saw_value else None


def _set_residualized_value(item: LineItem, year: int, value: float) -> None:
    if item.values is not None and int(year) in item.values.values:
        cell = item.values.values[int(year)]
        item.values.values[int(year)] = cell.model_copy(
            update={
                "value": float(value),
                "note": _PRESENTATION_RESIDUAL_NOTE,
            }
        )
        return

    if item.overrides is not None and int(year) in item.overrides:
        spec = item.overrides[int(year)]
        item.overrides[int(year)] = spec.model_copy(
            update={
                "params": {**dict(spec.params or {}), "value": float(value)},
                "note": _PRESENTATION_RESIDUAL_NOTE,
            }
        )


def _active_formula_first_periods(
    item: LineItem,
    periods: list[int],
) -> set[int]:
    active_periods = None
    if item.formula_periods is not None:
        active_periods = {int(period) for period in item.formula_periods}
    return {
        int(period)
        for period in periods
        if active_periods is None or int(period) in active_periods
    }


def _formula_first_node_periods(
    model: FinancialModel,
    historical_periods: list[int],
) -> tuple[dict[str, set[int]], dict[str, set[int]]]:
    """Return removable override periods plus formula-only bridge periods."""

    model.build_index()
    periods = [int(period) for period in historical_periods]
    items_by_id = {item.id: item for item in _iter_items(model)}
    has_real_data = {
        item.id: {
            period
            for period in periods
            if _item_has_real_data(item, period, model=model)
        }
        for item in items_by_id.values()
    }
    removable_candidates: set[tuple[str, int]] = set()

    for item in items_by_id.values():
        if item.historical is None:
            continue
        if item.id in _FORMULA_FIRST_EXCLUDED_ITEM_IDS:
            continue

        active_periods = _active_formula_first_periods(item, periods)
        if not active_periods:
            continue

        if (
            item.historical.type in _SYNTHETIC_FAST_PATH_TYPES
            and not _all_refs_same_period(item.historical.params)
        ):
            continue

        if item.data_concept_id:
            is_constant_formula = item.historical.type is FormulaType.constant
            ref_ids = _extract_ref_ids(item.historical.params)
            if not ref_ids and not is_constant_formula:
                continue
            for period in active_periods:
                if item.overrides is None or int(period) not in item.overrides:
                    continue
                override = item.overrides[int(period)]
                if override.type is not FormulaType.constant:
                    continue
                if is_constant_formula:
                    formula_value = _constant_override_value(item.historical)
                    override_value = _constant_override_value(override)
                    if (
                        formula_value is None
                        or override_value is None
                        or abs(formula_value - override_value) >= 1e-6
                    ):
                        continue
                removable_candidates.add((item.id, int(period)))

    bridge_candidates: set[tuple[str, int]] = set()

    def add_bridge_dependency(item_id: str, period: int) -> None:
        if period in has_real_data.get(item_id, set()):
            return
        node = (item_id, int(period))
        if node in removable_candidates or node in bridge_candidates:
            return
        item = items_by_id.get(item_id)
        if item is None or item.data_concept_id or item.historical is None:
            return
        if int(period) not in _active_formula_first_periods(item, periods):
            return
        if (
            item.historical.type not in _SYNTHETIC_FAST_PATH_TYPES
            or not _all_refs_same_period(item.historical.params)
            or _item_has_direct_real_data(item, period)
        ):
            return

        bridge_candidates.add(node)
        for ref_id in _extract_ref_ids(item.historical.params):
            add_bridge_dependency(ref_id, int(period))

    for item_id, period in sorted(removable_candidates):
        item = items_by_id[item_id]
        if item.overrides is None or int(period) not in item.overrides:
            continue
        if item.overrides[int(period)].note != "synthetic":
            continue
        for ref_id in _extract_ref_ids(item.historical.params if item.historical else None):
            add_bridge_dependency(ref_id, int(period))

    removable: dict[str, set[int]] = {}
    bridges: dict[str, set[int]] = {}
    unresolved = removable_candidates | bridge_candidates

    while unresolved:
        newly_derivable: list[tuple[str, int]] = []
        for item_id, period in sorted(unresolved):
            item = items_by_id[item_id]
            ref_ids = _extract_ref_ids(item.historical.params if item.historical else None)
            if all(period in has_real_data.get(ref_id, set()) for ref_id in ref_ids):
                newly_derivable.append((item_id, period))

        if not newly_derivable:
            break

        for item_id, period in newly_derivable:
            unresolved.discard((item_id, period))
            if (item_id, period) in removable_candidates:
                removable.setdefault(item_id, set()).add(period)
            else:
                bridges.setdefault(item_id, set()).add(period)
            has_real_data.setdefault(item_id, set()).add(period)

    return removable, bridges


def _compute_derivable_periods(
    model: FinancialModel,
    historical_periods: list[int],
) -> dict[str, set[int]]:
    """Return candidate historical periods where formula-backed overrides can be removed."""

    removable, _bridges = _formula_first_node_periods(model, historical_periods)
    return removable


def _constant_override_value(spec: Optional[FormulaSpec]) -> Optional[float]:
    if spec is None or spec.type is not FormulaType.constant:
        return None
    value = spec.params.get("value")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _lookup_formula_value(
    model: FinancialModel,
    item_id: str,
    period: int,
    computed_values: Dict[tuple[str, int], float],
    *,
    t: int = 0,
    _depth: int = 0,
    _seen: Optional[set[tuple[str, int]]] = None,
) -> Optional[float]:
    shifted = shift_period(int(period), int(t), model.time_structure.period_mode)
    if shifted is None:
        return None

    available_periods = _available_periods(model)
    if available_periods is not None and int(shifted) not in available_periods:
        return None

    key = (str(item_id), int(shifted))
    if key in computed_values:
        return computed_values[key]

    try:
        item = model.get_item(str(item_id))
    except KeyError:
        return None

    if item.overrides is not None and int(shifted) in item.overrides:
        spec_override = item.overrides[int(shifted)]
        if spec_override.note != "synthetic":
            value = _constant_override_value(spec_override)
            if value is not None:
                return value

    if item.values is not None and int(shifted) in item.values.values:
        value_cell = item.values.values[int(shifted)]
        if value_cell.value is not None:
            return float(value_cell.value)

    if item.historical is not None and item.historical.type is FormulaType.ref:
        if _depth >= _MAX_REF_CHAIN_DEPTH:
            return None
        if key in (_seen or set()):
            return None
        seen = (_seen or set()) | {key}
        params = item.historical.params or {}
        ref_target = _extract_single_ref(params)
        if ref_target is not None:
            target_id, target_t = ref_target
            value = _lookup_formula_value(
                model,
                target_id,
                int(shifted),
                computed_values,
                t=target_t,
                _depth=_depth + 1,
                _seen=seen,
            )
            if value is None:
                return None
            adjustment = params.get("adjustment")
            if adjustment is not None:
                try:
                    value += float(adjustment)
                except (TypeError, ValueError):
                    pass
            if params.get("negate"):
                value = -value
            return value

    if item.historical is not None and item.historical.type is FormulaType.constant:
        value = _constant_override_value(item.historical)
        if value is not None:
            return value

    return None


def _evaluate_expr_simple(
    model: FinancialModel,
    expr,
    period: int,
    computed_values: Dict[tuple[str, int], float],
) -> Optional[float]:
    if expr is None:
        return None
    if isinstance(expr, bool):
        return float(int(expr))
    if isinstance(expr, (int, float)):
        return float(expr)
    if isinstance(expr, LineItemRef):
        return _lookup_formula_value(model, expr.id, period, computed_values, t=expr.t)
    if isinstance(expr, dict):
        if "id" in expr and isinstance(expr["id"], str):
            try:
                ref_t = int(expr.get("t", 0))
            except (TypeError, ValueError):
                ref_t = 0
            return _lookup_formula_value(model, expr["id"], period, computed_values, t=ref_t)

        op = expr.get("op")
        args = list(expr.get("args", []) or [])
        if op in {"SUM", "AVERAGE"}:
            values = [_evaluate_expr_simple(model, arg, period, computed_values) for arg in args]
            if any(value is None for value in values):
                return None
            if not values:
                return 0.0
            if op == "AVERAGE":
                return sum(values) / len(values)
            return sum(values)
        if op == "+":
            values = [_evaluate_expr_simple(model, arg, period, computed_values) for arg in args]
            if any(value is None for value in values):
                return None
            return sum(values)
        if op == "*":
            values = [_evaluate_expr_simple(model, arg, period, computed_values) for arg in args]
            if any(value is None for value in values):
                return None
            result = 1.0
            for value in values:
                result *= value
            return result
        if op == "-":
            left = _evaluate_expr_simple(model, expr.get("left"), period, computed_values)
            right = _evaluate_expr_simple(model, expr.get("right"), period, computed_values)
            if left is None or right is None:
                return None
            return left - right
        if op == "/":
            left = _evaluate_expr_simple(model, expr.get("left"), period, computed_values)
            right = _evaluate_expr_simple(model, expr.get("right"), period, computed_values)
            if left is None or right is None or abs(right) < 1e-12:
                return None
            return left / right
        if op == "NEG":
            value = _evaluate_expr_simple(model, expr.get("arg"), period, computed_values)
            if value is None:
                return None
            return -value
    return None


def _evaluate_formula_simple(
    model: FinancialModel,
    item: LineItem,
    period: int,
    computed_values: Dict[tuple[str, int], float],
) -> Optional[float]:
    """Evaluate a subset of same-period formulas for reconciliation."""

    spec = item.historical
    if spec is None:
        return None

    params = spec.params or {}

    if spec.type == FormulaType.constant:
        return _constant_override_value(spec)

    if spec.type == FormulaType.ref:
        value = _evaluate_expr_simple(model, params.get("source"), period, computed_values)
        if value is None:
            return None
        adjustment = params.get("adjustment")
        if adjustment is not None:
            value += float(adjustment)
        if params.get("negate"):
            value = -value
        return value

    if spec.type == FormulaType.arithmetic:
        if "expr" in params:
            return _evaluate_expr_simple(model, params.get("expr"), period, computed_values)

        function = params.get("function")
        if function in {"SUM", "AVERAGE"}:
            values = [
                _evaluate_expr_simple(model, expr, period, computed_values)
                for expr in list(params.get("items", []) or [])
            ]
            if any(value is None for value in values):
                return None
            if not values:
                return 0.0
            if function == "AVERAGE":
                return sum(values) / len(values)
            return sum(values)

        operands = params.get("operands")
        if isinstance(operands, list) and operands:
            args = list(operands)
            operator = "+"
            if isinstance(args[0], str) and args[0] in {"+", "-", "*", "/"}:
                operator = args.pop(0)
            values = [_evaluate_expr_simple(model, expr, period, computed_values) for expr in args]
            if any(value is None for value in values):
                return None
            if not values:
                return 0.0
            if operator == "+":
                return sum(values)
            if operator == "-":
                result = values[0]
                for value in values[1:]:
                    result -= value
                return result
            if operator == "*":
                result = 1.0
                for value in values:
                    result *= value
                return result
            if operator == "/":
                result = values[0]
                for value in values[1:]:
                    if abs(value) < 1e-12:
                        return None
                    result /= value
                return result
            return None

        values = [
            _evaluate_expr_simple(model, expr, period, computed_values)
            for expr in list(params.get("items", []) or [])
        ]
        if any(value is None for value in values):
            return None
        return sum(values) if values else 0.0

    if spec.type == FormulaType.ratio:
        numerator = _evaluate_expr_simple(model, params.get("numerator"), period, computed_values)
        denominator = _evaluate_expr_simple(model, params.get("denominator"), period, computed_values)
        if numerator is None or denominator is None or abs(denominator) < 1e-12:
            return None
        result = numerator / denominator
        if params.get("subtract_one"):
            result -= 1
        return result

    return None


def _reconcile_override(
    item: LineItem,
    period: int,
    computed_value: float,
    tolerance: float = 0.01,
) -> bool:
    """Return True when the formula value matches the override within tolerance."""

    if item.overrides is None or int(period) not in item.overrides:
        return False
    spec = item.overrides[int(period)]
    if (
        spec.note == "synthetic"
        and item.historical is not None
        and item.historical.type in _SYNTHETIC_FAST_PATH_TYPES
        and _all_refs_same_period(item.historical.params)
    ):
        # Synthetic markers are placeholders, not data. When the historical
        # formula is a real derivation over same-period refs and produced a
        # value, the formula should win. Cross-period refs fail closed because
        # derivability currently validates dependencies at the candidate period.
        return computed_value is not None

    override_value = _constant_override_value(spec)
    if override_value is None:
        return False

    diff = abs(float(computed_value) - override_value)
    if abs(override_value) < 1e-6:
        return diff < 1e-6
    return diff / abs(override_value) <= tolerance


def apply_formula_first(
    model: FinancialModel,
    historical_periods: list[int],
) -> dict[str, set[int]]:
    """Remove reconciled constant overrides from derivable formula-backed items."""

    model.build_index()
    candidate_periods, bridge_periods = _formula_first_node_periods(
        model,
        historical_periods,
    )
    if not candidate_periods and not bridge_periods:
        return {}

    removable_nodes = {
        (item_id, int(period))
        for item_id, periods in candidate_periods.items()
        for period in periods
    }
    bridge_nodes = {
        (item_id, int(period))
        for item_id, periods in bridge_periods.items()
        for period in periods
    }
    candidate_nodes = removable_nodes | bridge_nodes
    dependencies: Dict[tuple[str, int], set[tuple[str, int]]] = {}
    reverse_dependencies: Dict[tuple[str, int], set[tuple[str, int]]] = {}

    graph_periods: dict[str, set[int]] = {}
    for item_id, periods in candidate_periods.items():
        graph_periods.setdefault(item_id, set()).update({int(period) for period in periods})
    for item_id, periods in bridge_periods.items():
        graph_periods.setdefault(item_id, set()).update({int(period) for period in periods})

    for item_id, periods in graph_periods.items():
        item = model.get_item(item_id)
        ref_ids = _extract_ref_ids(item.historical.params if item.historical else None)
        for period in periods:
            node = (item_id, int(period))
            deps = {
                (ref_id, int(period))
                for ref_id in ref_ids
                if (ref_id, int(period)) in candidate_nodes
            }
            dependencies[node] = deps
            for dep in deps:
                reverse_dependencies.setdefault(dep, set()).add(node)

    indegree = {node: len(deps) for node, deps in dependencies.items()}
    ready = sorted(node for node, degree in indegree.items() if degree == 0)
    ordered: list[tuple[str, int]] = []

    while ready:
        node = ready.pop(0)
        ordered.append(node)
        for child in sorted(reverse_dependencies.get(node, set())):
            indegree[child] -= 1
            if indegree[child] == 0:
                ready.append(child)
                ready.sort()

    for node in sorted(candidate_nodes - set(ordered)):
        ordered.append(node)

    computed_values: Dict[tuple[str, int], float] = {}
    validated: dict[str, set[int]] = {}
    rejected: set[tuple[str, int]] = set()

    def reject_with_downstream(node: tuple[str, int]) -> None:
        stack = [node]
        while stack:
            current = stack.pop()
            if current in rejected:
                continue
            rejected.add(current)
            stack.extend(sorted(reverse_dependencies.get(current, set())))

    for node in ordered:
        if node in rejected:
            continue

        if any(dep not in computed_values for dep in dependencies.get(node, set())):
            logging.warning(
                "Formula-first reconciliation skipped for %s period=%s due to unresolved derivable deps",
                node[0],
                node[1],
            )
            reject_with_downstream(node)
            continue

        item_id, period = node
        item = model.get_item(item_id)
        computed_value = _evaluate_formula_simple(model, item, period, computed_values)
        if node in bridge_nodes:
            if computed_value is None:
                logging.warning(
                    "Formula-first bridge evaluation failed for %s period=%s",
                    item_id,
                    period,
                )
                reject_with_downstream(node)
                continue
            computed_values[node] = computed_value
            continue

        override_value = None
        if item.overrides is not None and int(period) in item.overrides:
            override_value = item.overrides[int(period)].params.get("value")

        if computed_value is None or not _reconcile_override(item, period, computed_value):
            logging.warning(
                "Formula-first reconciliation failed for %s period=%s: computed=%s override=%s",
                item_id,
                period,
                computed_value,
                override_value,
            )
            reject_with_downstream(node)
            continue

        computed_values[node] = computed_value
        validated.setdefault(item_id, set()).add(int(period))

    for item_id, periods in validated.items():
        item = model.get_item(item_id)
        if item.overrides is None:
            continue
        for period in periods:
            item.overrides.pop(int(period), None)
        if not item.overrides:
            item.overrides = None

    return validated


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
