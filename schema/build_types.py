"""Data types shared by schema build orchestration helpers."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Dict, List, Literal, Optional, Protocol

from .build_diagnostics import DiagnosticReport
from .build_diagnostic_types import SourceArbitrationCheck
from .build_projection_seeds import SeedProjectionsResult
from .build_semantic_rows import SemanticRowsResult
from .model_build_context import BuildSource
from .model_readiness import (
    ModelQualityReadiness,
    ModelProjectionReadiness,
    ModelScenarioBridgeReadiness,
    ModelScenarioOutputReadiness,
    ValuationInputReadiness,
)
from .models import EdgarProvenance, FinancialModel
from .presentation_tree import PresentationTree
from .renderer import RenderPlan
from .segments import SegmentProfile
from .source_values import SourceValue

if TYPE_CHECKING:
    from .business_model_compiler import CompiledDriverRegistry


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
    source_arbitration: SourceArbitrationCheck = field(
        default_factory=SourceArbitrationCheck
    )
    source_arbitration_final_source_by_concept_year: dict[str, dict[int, str]] = field(
        default_factory=dict
    )


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
class FmpConceptFetchResult:
    concept_id: str
    values: dict[int, float]
    field_used_by_year: dict[int, str]
    fallback_field_years: set[int]
    missing: bool
    reported_period_ends_by_year: dict[int, str] = field(default_factory=dict)


for _facade_type in (
    EdgarFetcher,
    SourceResolutionEntry,
    ServedByBreakdown,
    PopulateStats,
    BuildResult,
    EdgarConceptFetchResult,
    FmpConceptFetchResult,
):
    _facade_type.__module__ = "schema.build"
del _facade_type
