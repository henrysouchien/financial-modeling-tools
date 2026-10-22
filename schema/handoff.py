from __future__ import annotations

from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import (
    Field,
    ValidationInfo,
    field_validator,
    model_serializer,
    model_validator,
)

from .annual_presentation_guard import AnnualPresentationGuardResult
from .models import ModelScale
from industry_slice_core.industry_types import Unit
from .model_build_context import _provider_registry_from_validation
from .provider_port.registry import ProviderId, require_registered_provider_id
from .model_readiness import (
    ModelQualityReadiness,
    ModelProjectionReadiness,
    ModelScenarioBridgeReadiness,
    ModelScenarioOutputReadiness,
    ValuationInputReadiness,
)
from .thesis import BusinessModelRef, QuantitativeFraming, SummaryStatus
from .thesis_shared_slice import (
    Assumption,
    BusinessOverview,
    Catalyst,
    CompanyField,
    ConsensusView,
    DataGap,
    DifferentiatedViewClaim,
    HistoricalCoincidence,
    IndustryAnalysis,
    InvalidationTrigger,
    MaterialityThreshold,
    Monitoring,
    Ownership,
    Peer,
    QualitativeFactor,
    Risk,
    SourceRecord,
    ThesisField,
    Valuation,
    _ContractModel,
    _normalize_optional_identifier,
)
AnnotationIdentifier = str
DiligenceCompletionState = Literal["empty", "draft", "confirmed"]
ModelArtifactStatus = Literal["present", "missing", "unknown"]
# Price-target copy older handoff model refs carried. The PriceTarget is its one
# owner (``schema.price_target``); stored refs that still hold a copy decode
# with it dropped.
_LEGACY_HANDOFF_MODEL_REF_PRICE_TARGET_KEYS = frozenset({"last_price_target"})


def _drop_legacy_keys(data: Any, legacy_keys: frozenset[str]) -> Any:
    if isinstance(data, dict) and not legacy_keys.isdisjoint(data):
        return {key: value for key, value in data.items() if key not in legacy_keys}
    return data


def _require_identifier(value: object | None, *, field_name: str) -> str:
    normalized = _normalize_optional_identifier(value)
    if normalized is None:
        raise ValueError(f"{field_name} is required")
    return normalized


class IdeaProvenance(_ContractModel):
    idea_id: str
    source_summary: str

    @field_validator("idea_id", mode="before")
    @classmethod
    def _normalize_idea_id(cls, value: object | None) -> str:
        return _require_identifier(value, field_name="idea_id")


class AssumptionLineageEntry(_ContractModel):
    assumption_id: str
    supporting_annotation_ids: list[AnnotationIdentifier] = Field(min_length=1)
    refutes_annotation_ids: list[AnnotationIdentifier] | None = None

    @field_validator("assumption_id", mode="before")
    @classmethod
    def _normalize_assumption_id(cls, value: object | None) -> str:
        return _require_identifier(value, field_name="assumption_id")


class ScorecardRef(_ContractModel):
    scorecard_id: str
    version: int = Field(ge=1)
    scored_at: str
    summary_status: SummaryStatus

    @field_validator("scorecard_id", mode="before")
    @classmethod
    def _normalize_scorecard_id(cls, value: object | None) -> str:
        return _require_identifier(value, field_name="scorecard_id")


class ThesisRef(_ContractModel):
    thesis_id: str
    version: int = Field(ge=1)
    markdown_path: str | None = None

    @field_validator("thesis_id", mode="before")
    @classmethod
    def _normalize_thesis_id(cls, value: object | None) -> str:
        return _require_identifier(value, field_name="thesis_id")


SeedCoverageSourceKind = Literal[
    "projection_override",
    "model_spec_assumption",
    "template_code_seed",
    "accepted_carry_forward",
]


class SeedCoverageRecord(_ContractModel):
    canonical_driver_key: str
    assumption_id: str | None = None
    resolved_item_id: str
    periods: list[int] = Field(min_length=1)
    source_kind: SeedCoverageSourceKind
    build_identity: str
    control_id: str | None = None
    binding_id: str | None = None
    scenario: Literal["base", "bull", "bear"] | None = None


class HistoricalDriverSeedCoverageRecord(_ContractModel):
    """Exact accepted-HFF lineage for one compiled historical opening value."""

    source_kind: Literal["historical_driver_seed"]
    historical_foundation_artifact_id: str = Field(
        pattern=r"^hff_[0-9a-f]{32}$"
    )
    historical_foundation_content_sha256: str = Field(
        pattern=r"^[0-9a-f]{64}$"
    )
    historical_driver_seed_denominator_sha256: str = Field(
        pattern=r"^[0-9a-f]{64}$"
    )
    driver_id: str = Field(pattern=r"^drv_[0-9a-f]{32}$")
    driver_key: str = Field(min_length=1)
    resolved_item_id: str = Field(min_length=1)
    periods: list[int] = Field(min_length=1)
    period_id: str = Field(pattern=r"^FY[1-9][0-9]{3}$")
    unit: Unit
    model_scale: ModelScale
    observation_basis: Literal["period_flow", "period_end", "period_average"]
    value: Decimal
    disposition: Literal[
        "observed",
        "derived",
        "proxy",
        "analyst_normalized",
    ]
    derivation_kind: Literal["direct", "arithmetic_mean", "allocation"]
    derivation_rationale: str = Field(min_length=1)
    confidence: Literal["high", "medium", "low"]
    limitations: list[str] = Field(default_factory=list)
    evidence_ids: list[str] = Field(default_factory=list)
    supporting_evidence_ids: list[str] = Field(default_factory=list)
    evidence_value_ids: list[str] = Field(default_factory=list)
    fact_ids: list[str] = Field(default_factory=list)
    legacy_anchor_reconciled: bool
    build_identity: str = Field(min_length=1)

    @field_validator(
        "limitations",
        "evidence_ids",
        "supporting_evidence_ids",
        "evidence_value_ids",
        "fact_ids",
    )
    @classmethod
    def _canonical_seed_lineage_refs(
        cls,
        value: list[str],
        info: ValidationInfo,
    ) -> list[str]:
        if any(not item for item in value):
            raise ValueError(f"{info.field_name} must be nonempty text")
        return sorted(set(value))

    @model_validator(mode="after")
    def _validate_seed_coverage(self) -> "HistoricalDriverSeedCoverageRecord":
        if not self.value.is_finite():
            raise ValueError("historical driver seed coverage value must be finite")
        expected_period = int(self.period_id.removeprefix("FY"))
        if self.periods != [expected_period]:
            raise ValueError(
                "historical driver seed coverage must name its one exact fiscal period"
            )
        if self.unit is not Unit.dollars and self.model_scale != "units":
            raise ValueError(
                "non-dollar historical driver seed coverage requires units scale"
            )
        return self


class HistoricalFoundationFactCoverageRecord(_ContractModel):
    """Exact accepted-HFF lineage for one compiled historical fact value."""

    source_kind: Literal["historical_foundation_fact"]
    historical_foundation_artifact_id: str = Field(
        pattern=r"^hff_[0-9a-f]{32}$"
    )
    historical_foundation_content_sha256: str = Field(
        pattern=r"^[0-9a-f]{64}$"
    )
    requirement_id: str = Field(min_length=1)
    semantic_id: str = Field(min_length=1)
    fact_id: str = Field(min_length=1)
    evidence_id: str = Field(min_length=1)
    source_id: str = Field(min_length=1)
    resolved_item_id: str = Field(min_length=1)
    periods: list[int] = Field(min_length=1, max_length=1)
    build_identity: str = Field(min_length=1)


class SourceBundleScheduleDisclosureCoverageRecord(_ContractModel):
    """Exact SourceBundle lineage for one code-owned schedule disclosure."""

    source_kind: Literal["source_bundle_schedule_disclosure"]
    fact_id: str = Field(min_length=1)
    source_id: str = Field(min_length=1)
    resolved_item_id: str = Field(min_length=1)
    periods: list[int] = Field(min_length=1, max_length=1)
    build_identity: str = Field(min_length=1)


class SourceBundleAdjustedEbitdaBridgeCoverageRecord(_ContractModel):
    """Exact SourceBundle lineage for one issuer EBITDA bridge fact."""

    source_kind: Literal["source_bundle_adjusted_ebitda_bridge"]
    fact_id: str = Field(min_length=1)
    source_id: str = Field(min_length=1)
    resolved_item_id: str = Field(min_length=1)
    periods: list[int] = Field(min_length=1, max_length=1)
    build_identity: str = Field(min_length=1)


class SourceBundleOpeningBalanceCoverageRecord(_ContractModel):
    """Exact SourceBundle lineage for one first-year cell's opening balance.

    The evidence is the pin's opening column: the role's value at the year
    before the first historical year, which the cell reads as a literal.
    """

    source_kind: Literal["source_bundle_opening_balance"]
    requirement_id: str = Field(min_length=1)
    evidence_id: str = Field(min_length=1)
    source_id: str = Field(min_length=1)
    resolved_item_id: str = Field(min_length=1)
    periods: list[int] = Field(min_length=1, max_length=1)
    build_identity: str = Field(min_length=1)


class BusinessModelNodeIdentityCoverageRecord(_ContractModel):
    """Identity-channel lineage for one BM-node historical value.

    Stage A step A-3: the retired binding executor's observation vocabulary
    migrates to the identity channel's rows — same exact SourceBundle
    lineage, produced by ``resolve_bm_node_history``'s identity arm rather
    than an authored join.
    """

    source_kind: Literal["business_model_node_identity"]
    requirement_id: str = Field(min_length=1)
    evidence_id: str = Field(min_length=1)
    source_id: str = Field(min_length=1)
    business_model_node_key: str = Field(min_length=1)
    resolved_item_id: str = Field(min_length=1)
    periods: list[int] = Field(min_length=1, max_length=1)
    build_identity: str = Field(min_length=1)


class BusinessModelNodeFormulaCoverageRecord(_ContractModel):
    """Formula-arm lineage: a registered ratio formula stands on a BM node."""

    source_kind: Literal["business_model_node_formula"]
    formula_id: str = Field(min_length=1)
    business_model_node_key: str = Field(min_length=1)
    resolved_item_id: str = Field(min_length=1)
    input_item_ids: list[str] = Field(min_length=1)
    periods: list[int] = Field(min_length=1)
    build_identity: str = Field(min_length=1)

    @field_validator("input_item_ids")
    @classmethod
    def _canonical_input_item_ids(cls, value: list[str]) -> list[str]:
        if any(not item for item in value):
            raise ValueError(
                "input_item_ids must be nonempty text"
            )
        return sorted(set(value))


class RetiredBusinessModelObservationCoverageRecord(_ContractModel):
    """Read-side decode of pre-Stage-A stored lineage rows (closed artifacts).

    No producer exists: the binding executor that wrote this vocabulary was
    deleted at Stage A step A-3. Published ModelVersions are closed artifacts
    replayed from stored bytes (plan §6.2), so their semantic sidecars'
    render states must keep decoding; new builds emit only the identity
    channel's vocabulary above.
    """

    source_kind: Literal["source_bundle_business_model_observation"]
    requirement_id: str = Field(min_length=1)
    evidence_id: str = Field(min_length=1)
    source_id: str = Field(min_length=1)
    business_model_node_key: str = Field(min_length=1)
    resolved_item_id: str = Field(min_length=1)
    periods: list[int] = Field(min_length=1, max_length=1)
    build_identity: str = Field(min_length=1)


SeedCoverageManifestRecord = Annotated[
    SeedCoverageRecord
    | HistoricalDriverSeedCoverageRecord
    | HistoricalFoundationFactCoverageRecord
    | BusinessModelNodeIdentityCoverageRecord
    | BusinessModelNodeFormulaCoverageRecord
    | RetiredBusinessModelObservationCoverageRecord
    | SourceBundleScheduleDisclosureCoverageRecord
    | SourceBundleAdjustedEbitdaBridgeCoverageRecord
    | SourceBundleOpeningBalanceCoverageRecord,
    Field(discriminator="source_kind"),
]


class CompilerInput(_ContractModel):
    ticker: str
    content_hash: str
    payload: dict[str, Any]
    bundle_id: str | None = Field(default=None, min_length=1)
    identity_digest: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )

    @field_validator("bundle_id", mode="before")
    @classmethod
    def _validate_bundle_id(cls, value: object | None) -> object | None:
        if value is None:
            return None
        if not isinstance(value, str) or not value or value != value.strip():
            raise ValueError("bundle_id must be canonical nonempty text")
        return value

    @field_validator("identity_digest", mode="before")
    @classmethod
    def _validate_identity_digest(cls, value: object | None) -> object | None:
        if value is None:
            return None
        if (
            not isinstance(value, str)
            or len(value) != 64
            or value != value.lower()
            or any(character not in "0123456789abcdef" for character in value)
        ):
            raise ValueError("identity_digest must be lowercase sha256 hex")
        return value

    @model_validator(mode="after")
    def _validate_source_bundle_lineage_pair(self) -> "CompilerInput":
        if (self.bundle_id is None) != (self.identity_digest is None):
            raise ValueError(
                "bundle_id and identity_digest must be supplied together"
            )
        return self

    @model_serializer(mode="wrap")
    def _omit_absent_legacy_lineage(self, handler: Any) -> dict[str, Any]:
        serialized = handler(self)
        if self.bundle_id is None:
            serialized.pop("bundle_id", None)
            serialized.pop("identity_digest", None)
        return serialized


class BusinessModelCompilerRef(_ContractModel):
    """Legacy logical ref retained for non-build current-state reads."""

    business_model_id: str
    revision: str
    content_hash: str


class SegmentProfileSnapshotCompilerRef(_ContractModel):
    """Legacy logical ref retained for offline acquisition reads."""

    snapshot_id: str
    content_hash: str


class RenderCompilerInputs(_ContractModel):
    source_bundle: CompilerInput
    model_spec: CompilerInput


class RenderStateIdentity(_ContractModel):
    build_id: str
    model_id: str
    model_version: int = Field(ge=1)
    source_bundle_digest: str
    model_spec_digest: str


class ModelRenderState(_ContractModel):
    """The one atomic rendered-from contract for a workbook revision."""

    schema_version: Literal["1.0"] = "1.0"
    workbook_semantics_hash: str
    compiler_inputs: RenderCompilerInputs
    seed_coverage_manifest: list[SeedCoverageManifestRecord]
    identity: RenderStateIdentity

    @model_validator(mode="after")
    def _identity_matches_compiler_inputs(self) -> "ModelRenderState":
        pairs = (
            (
                self.identity.source_bundle_digest,
                self.compiler_inputs.source_bundle.content_hash,
            ),
            (
                self.identity.model_spec_digest,
                self.compiler_inputs.model_spec.content_hash,
            ),
        )
        if any(left != right for left, right in pairs):
            raise ValueError("render-state identity must match compiler inputs")
        expected_build_id = (
            f"{self.identity.source_bundle_digest}:"
            f"{self.identity.model_spec_digest}:"
            f"{self.identity.model_version}:"
            f"{self.workbook_semantics_hash}"
        )
        if self.identity.build_id != expected_build_id:
            raise ValueError(
                "render-state build identity must stamp the exact atomic record"
            )
        if any(record.build_identity != self.identity.build_id for record in self.seed_coverage_manifest):
            raise ValueError("every seed-coverage record must match render-state build identity")
        return self


class HandoffModelRef(_ContractModel):
    model_id: str
    version: int = Field(ge=1)
    model_build_context_id: str
    model_build_context_version: int = Field(ge=1)
    file_path: str | None = None
    diagnostic_status: ModelArtifactStatus = "unknown"
    control_snapshot_projection_status: ModelArtifactStatus = "unknown"
    projection_readiness: ModelProjectionReadiness | None = None
    scenario_output_readiness: ModelScenarioOutputReadiness | None = None
    scenario_bridge_readiness: ModelScenarioBridgeReadiness | None = None
    model_quality_readiness: ModelQualityReadiness | None = None
    valuation_input_readiness: ValuationInputReadiness | None = None
    annual_presentation: AnnualPresentationGuardResult
    render_state: ModelRenderState | None = None
    workbook_semantics_hash: str | None = None

    @model_validator(mode="before")
    @classmethod
    def _drop_legacy_price_target(cls, data: Any) -> Any:
        return _drop_legacy_keys(data, _LEGACY_HANDOFF_MODEL_REF_PRICE_TARGET_KEYS)

    @model_validator(mode="after")
    def _derive_rendered_hash(self) -> "HandoffModelRef":
        if self.render_state is None:
            if self.workbook_semantics_hash is not None:
                raise ValueError("workbook_semantics_hash cannot be stamped without the complete render_state")
            return self
        if self.workbook_semantics_hash not in (None, self.render_state.workbook_semantics_hash):
            raise ValueError("workbook_semantics_hash must match render_state")
        identity = self.render_state.identity
        if (self.model_id, self.version) != (
            identity.model_id,
            identity.model_version,
        ):
            raise ValueError(
                "handoff model identity must match render_state.identity"
            )
        self.workbook_semantics_hash = self.render_state.workbook_semantics_hash
        return self

    @field_validator("model_id", mode="before")
    @classmethod
    def _normalize_model_id(cls, value: object | None) -> str:
        return _require_identifier(value, field_name="model_id")

    @field_validator("model_build_context_id", mode="before")
    @classmethod
    def _normalize_model_build_context_id(cls, value: object | None) -> str:
        return _require_identifier(value, field_name="model_build_context_id")


class Financials(_ContractModel):
    source: ProviderId = "fmp"
    data: dict[str, Any] = Field(default_factory=dict)

    @field_validator("source", mode="before")
    @classmethod
    def _normalize_source_provider(
        cls,
        value: object,
        info: ValidationInfo,
    ) -> ProviderId:
        return require_registered_provider_id(
            value,
            provider_registry=_provider_registry_from_validation(info),
            entry_point="handoff financials source provider",
        )


class HandoffMetadata(_ContractModel):
    diligence_completion: dict[str, DiligenceCompletionState] = Field(default_factory=dict)
    diligence_sections: dict[str, Any] = Field(default_factory=dict)
    next_factor_id: int = Field(default=1, ge=1)
    analyst_session_id: str | None = None
    modeling_workflow_state_cache: dict[str, Any] | None = None


class HandoffArtifactV1_1(_ContractModel):
    handoff_id: int = Field(ge=1)
    created_at: float = Field(ge=0)
    research_file_id: int = Field(ge=1)

    company: CompanyField
    thesis: ThesisField = Field(default_factory=ThesisField)
    consensus_view: ConsensusView | None = None
    differentiated_view: list[DifferentiatedViewClaim] = Field(default_factory=list)
    invalidation_triggers: list[InvalidationTrigger] = Field(default_factory=list)
    business_overview: BusinessOverview | None = None
    catalysts: list[Catalyst] = Field(default_factory=list)
    risks: list[Risk] = Field(default_factory=list)
    valuation: Valuation | None = None
    peers: list[Peer] = Field(default_factory=list)
    assumptions: list[Assumption] = Field(default_factory=list)
    qualitative_factors: list[QualitativeFactor] = Field(default_factory=list)
    ownership: Ownership | None = None
    monitoring: Monitoring | None = None
    sources: list[SourceRecord] = Field(default_factory=list)
    industry_analysis: IndustryAnalysis | None = None
    materiality: MaterialityThreshold | None = None
    historical_coincidences: list[HistoricalCoincidence] = Field(default_factory=list)
    data_gaps: list[DataGap] = Field(default_factory=list)
    quantitative_framing: QuantitativeFraming | None = None

    idea_provenance: IdeaProvenance | None = None
    assumption_lineage: list[AssumptionLineageEntry] | None = None
    process_template_id: str | None = None
    scorecard_ref: ScorecardRef | None = None
    thesis_ref: ThesisRef | None = None
    model_ref: HandoffModelRef | None = None
    business_model_ref: BusinessModelRef | None = None
    financials: Financials | None = None
    metadata: HandoffMetadata = Field(default_factory=HandoffMetadata)

    schema_version: Literal["1.1", "1.2"] = "1.2"

    @field_validator("process_template_id", mode="before")
    @classmethod
    def _normalize_process_template_id(cls, value: object | None) -> str | None:
        return _normalize_optional_identifier(value)


__all__ = [
    "AnnotationIdentifier",
    "AssumptionLineageEntry",
    "BusinessModelNodeFormulaCoverageRecord",
    "BusinessModelNodeIdentityCoverageRecord",
    "DiligenceCompletionState",
    "Financials",
    "HandoffArtifactV1_1",
    "HandoffMetadata",
    "HandoffModelRef",
    "HistoricalDriverSeedCoverageRecord",
    "HistoricalFoundationFactCoverageRecord",
    "IdeaProvenance",
    "ModelArtifactStatus",
    "ModelRenderState",
    "ModelProjectionReadiness",
    "ModelScenarioBridgeReadiness",
    "ModelScenarioOutputReadiness",
    "ScorecardRef",
    "RenderCompilerInputs",
    "RenderStateIdentity",
    "SeedCoverageRecord",
    "SeedCoverageManifestRecord",
    "RetiredBusinessModelObservationCoverageRecord",
    "SeedCoverageSourceKind",
    "SourceBundleAdjustedEbitdaBridgeCoverageRecord",
    "SourceBundleOpeningBalanceCoverageRecord",
    "SourceBundleScheduleDisclosureCoverageRecord",
    "ThesisRef",
]
