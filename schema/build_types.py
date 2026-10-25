"""Data types shared by schema build orchestration helpers."""

from __future__ import annotations

from dataclasses import InitVar, dataclass, field
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
)

from pydantic import ValidationInfo, model_validator

from .annual_presentation_guard import AnnualPresentationGuardResult
from .build_diagnostics import DiagnosticReport
from .build_projection_seeds import ControlSnapshotProjectionResult
from .build_provider_diagnostics import validate_quality_warnings_by_provider
from .build_semantic_rows import SemanticRowsResult
from .model_readiness import (
    ModelQualityReadiness,
    ModelProjectionReadiness,
    ModelScenarioBridgeReadiness,
    ModelScenarioOutputReadiness,
    ValuationInputReadiness,
)
from .models import FinancialModel
from .provider_port.registry import (
    ProviderId,
    ProviderRegistry,
    inject_provider_registry_validation_context,
    require_registered_provider_id,
)
from .renderer import RenderPlan
from .segments import SegmentProfile
from .statement_windows import StatementWindowsResult
from .build_model_product_contracts import ScheduleInputAudit, ValuationEvidenceStatus

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
        end_quarter: int = 4,
        full_year_mode: bool = True,
    ) -> Dict: ...


@dataclass
class SourceResolutionEntry:
    """Population outcome for one required role, keyed by served year."""

    required_role: str
    producer_by_year: dict[int, ProviderId] = field(default_factory=dict)
    provider_registry: InitVar[object] = None

    @model_validator(mode="before")
    @classmethod
    def _inject_registry_context(
        cls,
        value: object,
        info: ValidationInfo,
    ) -> object:
        return inject_provider_registry_validation_context(cls, value, info)

    def __post_init__(self, provider_registry: object) -> None:
        registry = (
            provider_registry
            if isinstance(provider_registry, ProviderRegistry)
            else None
        )
        self.producer_by_year = {
            int(year): require_registered_provider_id(
                producer,
                provider_registry=registry,
                entry_point="source-resolution producer",
            )
            for year, producer in self.producer_by_year.items()
        }


@dataclass
class ServedByBreakdown:
    """Per-year producers and unserved years for one required role."""

    producer_by_year: dict[int, ProviderId] = field(default_factory=dict)
    years_unserved: List[int] = field(default_factory=list)
    provider_registry: InitVar[object] = None

    @model_validator(mode="before")
    @classmethod
    def _inject_registry_context(
        cls,
        value: object,
        info: ValidationInfo,
    ) -> object:
        return inject_provider_registry_validation_context(cls, value, info)

    def __post_init__(self, provider_registry: object) -> None:
        registry = (
            provider_registry
            if isinstance(provider_registry, ProviderRegistry)
            else None
        )
        self.producer_by_year = {
            int(year): require_registered_provider_id(
                producer,
                provider_registry=registry,
                entry_point="served-by breakdown producer",
            )
            for year, producer in self.producer_by_year.items()
        }


@dataclass
class PopulateStats:
    source: str | None
    items_populated: int
    items_skipped: int
    periods_populated: int
    missing_roles: List[str]
    edgar_api_calls: int = 0
    edgar_errors: List[str] = field(default_factory=list)
    edgar_partial_failures: List[str] = field(default_factory=list)
    source_resolution: list[SourceResolutionEntry] = field(default_factory=list)
    served_by_role: Dict[str, ServedByBreakdown] = field(default_factory=dict)
    quality_warnings_by_provider: dict[
        ProviderId,
        list[dict[str, object]],
    ] = field(default_factory=dict)
    served_source_by_role_year: dict[str, dict[int, ProviderId]] = field(
        default_factory=dict
    )
    provider_registry: InitVar[object] = None

    @model_validator(mode="before")
    @classmethod
    def _inject_registry_context(
        cls,
        value: object,
        info: ValidationInfo,
    ) -> object:
        return inject_provider_registry_validation_context(cls, value, info)

    def __post_init__(self, provider_registry: object) -> None:
        if self.edgar_errors is None:
            self.edgar_errors = []
        if self.edgar_partial_failures is None:
            self.edgar_partial_failures = []
        registry = (
            provider_registry
            if isinstance(provider_registry, ProviderRegistry)
            else None
        )
        self.quality_warnings_by_provider = validate_quality_warnings_by_provider(
            self.quality_warnings_by_provider,
            provider_registry=registry,
        )
        self.served_source_by_role_year = {
            required_role: {
                int(year): require_registered_provider_id(
                    provider_id,
                    provider_registry=registry,
                    entry_point=(
                        f"population served source for required role "
                        f"{required_role!r} year {year}"
                    ),
                )
                for year, provider_id in served_by_year.items()
            }
            for required_role, served_by_year in self.served_source_by_role_year.items()
        }


@dataclass
class BuildResult:
    model: FinancialModel
    render_plan: RenderPlan
    output_path: Optional[str]
    stats: PopulateStats
    annual_presentation: AnnualPresentationGuardResult
    render_state: dict[str, object] = field(default_factory=dict)
    diagnostic: Optional[DiagnosticReport] = None
    segment_profile: Optional[SegmentProfile] = None
    compiled_registry: "CompiledDriverRegistry | None" = None
    derivable_items: Dict[str, set[int]] = field(default_factory=dict)
    overrides_applied: int = 0
    control_snapshot_projection: ControlSnapshotProjectionResult = field(
        default_factory=ControlSnapshotProjectionResult
    )
    semantic_rows: SemanticRowsResult = field(default_factory=SemanticRowsResult)
    statement_windows: StatementWindowsResult = field(
        default_factory=StatementWindowsResult
    )
    projection_readiness: ModelProjectionReadiness = field(
        default_factory=ModelProjectionReadiness
    )
    scenario_output_readiness: ModelScenarioOutputReadiness = field(
        default_factory=ModelScenarioOutputReadiness
    )
    scenario_bridge_readiness: ModelScenarioBridgeReadiness = field(
        default_factory=ModelScenarioBridgeReadiness
    )
    scenario_results: dict[str, object] = field(default_factory=dict)
    sensitivity_results: dict[str, object] = field(default_factory=dict)
    valuation_results: dict[str, object] = field(default_factory=dict)
    valuation_evidence_status: ValuationEvidenceStatus | None = None
    schedule_input_audit: ScheduleInputAudit = field(default_factory=ScheduleInputAudit)
    model_quality_readiness: ModelQualityReadiness = field(
        default_factory=ModelQualityReadiness
    )
    valuation_input_readiness: ValuationInputReadiness = field(
        default_factory=ValuationInputReadiness
    )
    workbook_semantics_hash: str = ""
    render_compiler_inputs: dict[str, object] = field(default_factory=dict)
    seed_coverage_manifest: Sequence[Mapping[str, Any]] = field(default_factory=list)
    evaluation_reads: Mapping[tuple[str, int], tuple[tuple[str, int], ...]] = field(default_factory=dict)
    provider_registry: ProviderRegistry | None = field(
        default=None,
        repr=False,
        compare=False,
    )





