"""Bounded read projection for one accepted selected BusinessModel revision.

This contract is intentionally smaller than ``BusinessModelRevision``.  It gives
downstream agents semantic economic context and exact lineage references without
exposing workbook mappings, XBRL locators, or raw evidence.  It does carry each
driver's source-concept identity (``kpi_source``, EDGAR member/axis, the balance
it observes) and its compile target kind, so a statement mapper can tell which
disclosed line a driver names; the template-required statement roles are listed
apart from those BusinessModel-specific operating needs. Former ``MAX_*`` caps
are measurements on ``BusinessModelContextOverflow``, not refusals: identity
keys are returned whole, and ``key_decisions`` is the one prose page.
"""

from __future__ import annotations

from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .business_model import (
    AcquisitionPattern,
    BalanceRole,
    CapexModelAs,
    CompileTargetType,
    CostModelAs,
    DebtLevel,
    DilutionPattern,
    Factor,
    FundingModel,
    Intensity,
    KpiFrequency,
    NodeBehavior,
    ObservationBasis,
    RecommendedDepth,
    ReturnPriority,
    RevenueModelType,
    RevenueScope,
    SbcModelAs,
    WcModelAs,
)
from .finding_vocabulary import Finding
from .model_state_identity import validate_sha256_hex
from industry_slice_core.industry_types import Unit


MAX_SEGMENTS = 32
MAX_DRIVERS = 256
MAX_COSTS = 128
MAX_CAPITAL_ITEMS = 16
MAX_LIMITATIONS = 32
MAX_KEY_DECISIONS = 24
MAX_EVIDENCE_REFS = 32
MAX_UPSTREAM_REFS = 32
MAX_TEMPLATE_ROLES = 256

SemanticKey = Annotated[str, Field(min_length=1, max_length=256)]
ShortText = Annotated[str, Field(min_length=1, max_length=512)]
Prose = Annotated[str, Field(min_length=1, max_length=4096)]


class _ContextContract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


class BusinessModelContextRevisionRef(_ContextContract):
    business_model_id: ShortText
    revision: ShortText
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")


class BusinessModelContextStageReceiptRef(_ContextContract):
    stage: Literal["business-model-construction"]
    status: Literal["accepted"]
    receipt_digest: str

    @field_validator("receipt_digest")
    @classmethod
    def _digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="receipt_digest")


class BusinessModelContextMethodologyRef(_ContextContract):
    name: ShortText
    declared_version: ShortText
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")


class BusinessModelContextEvidenceSnapshotRef(_ContextContract):
    content_sha256: str
    evidence_count: int = Field(ge=0, le=4096)

    @field_validator("content_sha256")
    @classmethod
    def _digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")


class BusinessModelContextArtifactRef(_ContextContract):
    """Exact upstream identity with authenticated scope and provider internals removed."""

    artifact_kind: ShortText
    artifact_id: ShortText
    artifact_version: int = Field(gt=0)
    payload_schema_version: ShortText
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")


class BusinessModelContextHeader(_ContextContract):
    schema_version: Literal["business-model-context.v1"] = "business-model-context.v1"
    research_file_id: int = Field(gt=0)
    ticker: Annotated[str, Field(min_length=1, max_length=32)]
    selection_epoch: int | None = Field(default=None, ge=1)
    selected_revision_ref: BusinessModelContextRevisionRef | None = None
    stage_receipt_ref: BusinessModelContextStageReceiptRef | None = None
    stage_metadata_sha256: str | None = None
    evidence_snapshot_ref: BusinessModelContextEvidenceSnapshotRef | None = None
    methodology_ref: BusinessModelContextMethodologyRef | None = None
    upstream_artifact_refs: tuple[BusinessModelContextArtifactRef, ...] = ()

    @field_validator("stage_metadata_sha256")
    @classmethod
    def _digest(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return validate_sha256_hex(value, field_name="stage_metadata_sha256")

    @model_validator(mode="after")
    def _selected_revision_lineage(self) -> "BusinessModelContextHeader":
        selected_fields = (
            self.selection_epoch,
            self.selected_revision_ref,
            self.stage_receipt_ref,
            self.stage_metadata_sha256,
            self.evidence_snapshot_ref,
            self.methodology_ref,
        )
        if any(value is not None for value in selected_fields) and any(
            value is None for value in selected_fields
        ):
            raise ValueError("selected revision lineage must be all present or all absent")
        return self


class BusinessModelContextEvidenceRef(_ContextContract):
    evidence_id: str
    evidence_snapshot_sha256: str

    @field_validator("evidence_id", "evidence_snapshot_sha256")
    @classmethod
    def _digest(cls, value: str, info) -> str:
        return validate_sha256_hex(value, field_name=info.field_name)


class BusinessModelContextRelationship(_ContextContract):
    formula: Literal["growth", "product", "sum", "external", "derived", "roll_forward"]
    related_driver_keys: tuple[SemanticKey, ...]
    external_description: Prose | None = None


class BusinessModelContextSegmentItem(_ContextContract):
    item_kind: Literal["segment"] = "segment"
    key: SemanticKey
    label: ShortText
    is_default: bool
    revenue_share: float = Field(ge=0.0, le=1.0)
    revenue_model_type: RevenueModelType
    driver_keys: tuple[SemanticKey, ...]
    cost_keys: tuple[SemanticKey, ...]
    gross_margin_behavior: Literal["expanding", "stable", "compressing"] | None = None
    gross_margin_current_pct: float | None = None
    gross_margin_target_pct: float | None = None
    unit_definition: Prose | None = None
    scale_dynamics: Prose | None = None
    claim_keys: tuple[SemanticKey, ...]
    evidence_refs: tuple[BusinessModelContextEvidenceRef, ...]


class BusinessModelContextDriverItem(_ContextContract):
    item_kind: Literal["driver"] = "driver"
    key: SemanticKey
    segment_key: SemanticKey
    label: ShortText
    factors: tuple[Factor, ...]
    unit: Unit
    behavior: NodeBehavior | None = None
    economic_role: Literal["forecast_input", "derived", "context_only"]
    reported_kpi: bool
    kpi_frequency: KpiFrequency | None = None
    management_target_claim: Prose | None = None
    note: Prose | None = None
    relationship: BusinessModelContextRelationship | None = None
    kpi_source: ShortText | None = None
    edgar_member: ShortText | None = None
    edgar_axis: ShortText | None = None
    observation_basis: ObservationBasis | None = None
    balance_of: BalanceRole | None = None
    revenue_scope: RevenueScope | None = None
    compile_to: CompileTargetType | None = None
    claim_keys: tuple[SemanticKey, ...]
    evidence_refs: tuple[BusinessModelContextEvidenceRef, ...]


class BusinessModelContextCostItem(_ContextContract):
    item_kind: Literal["cost"] = "cost"
    key: SemanticKey
    scope_key: SemanticKey
    label: ShortText
    factors: tuple[Factor, ...]
    model_basis: CostModelAs
    leverage: bool
    observed_target_pct: float | None = None
    note: Prose | None = None
    economic_role: Literal["forecast_input"] = "forecast_input"
    claim_keys: tuple[SemanticKey, ...]
    evidence_refs: tuple[BusinessModelContextEvidenceRef, ...]


class BusinessModelContextCapitalItem(_ContextContract):
    item_kind: Literal["capital"] = "capital"
    key: SemanticKey
    category: Literal[
        "capex",
        "working_capital",
        "acquisitions",
        "funding_model",
        "debt",
        "equity",
        "capital_return",
    ]
    capex_model_as: CapexModelAs | None = None
    working_capital_model_as: WcModelAs | None = None
    intensity: Intensity | None = None
    components: tuple[ShortText, ...] = Field(default=(), max_length=32)
    depreciation_rate: float | None = None
    amortization_rate: float | None = None
    notes: tuple[Prose, ...] = Field(default=(), max_length=16)
    acquisition_pattern: AcquisitionPattern | None = None
    acquisition_note: Prose | None = None
    funding_model: FundingModel | None = None
    debt_level: DebtLevel | None = None
    debt_target_metric: Prose | None = None
    dilution_pattern: DilutionPattern | None = None
    sbc_model_as: SbcModelAs | None = None
    sbc_offset: Prose | None = None
    return_priority: ReturnPriority | None = None
    return_mechanisms: tuple[ShortText, ...] = Field(default=(), max_length=16)
    claim_keys: tuple[SemanticKey, ...]
    evidence_refs: tuple[BusinessModelContextEvidenceRef, ...]


class BusinessModelContextLimitationItem(_ContextContract):
    item_kind: Literal["limitation"] = "limitation"
    key: SemanticKey
    issue_type: Literal["limitation", "unresolved_question"]
    text: Prose
    claim_keys: tuple[SemanticKey, ...]
    evidence_refs: tuple[BusinessModelContextEvidenceRef, ...]


BusinessModelContextItem = Annotated[
    BusinessModelContextSegmentItem
    | BusinessModelContextDriverItem
    | BusinessModelContextCostItem
    | BusinessModelContextCapitalItem
    | BusinessModelContextLimitationItem,
    Field(discriminator="item_kind"),
]


class BusinessModelContextOverflow(_ContextContract):
    """How far each census exceeds its former read cap.

    Identity keys are returned whole; this is the measurement, not a cut.
    ``key_decisions`` is the count the summary page omitted.
    """

    segments: int = Field(default=0, ge=0)
    drivers: int = Field(default=0, ge=0)
    costs: int = Field(default=0, ge=0)
    capital_items: int = Field(default=0, ge=0)
    limitations: int = Field(default=0, ge=0)
    key_decisions: int = Field(default=0, ge=0)
    evidence_refs: int = Field(default=0, ge=0)
    template_required_roles: int = Field(default=0, ge=0)
    upstream_artifact_refs: int = Field(default=0, ge=0)
    operating_needs: int = Field(default=0, ge=0)


class BusinessModelContextCounts(_ContextContract):
    segments: int = Field(ge=0)
    drivers: int = Field(ge=0)
    costs: int = Field(ge=0)
    capital_items: int = Field(ge=0)
    limitations: int = Field(ge=0)


class BusinessModelContextSemanticKeys(_ContextContract):
    segments: tuple[SemanticKey, ...] = ()
    drivers: tuple[SemanticKey, ...] = ()
    costs: tuple[SemanticKey, ...] = ()
    capital: tuple[SemanticKey, ...] = ()
    limitations: tuple[SemanticKey, ...] = ()

    @field_validator("segments", "drivers", "costs", "capital", "limitations")
    @classmethod
    def _canonical_keys(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != tuple(sorted(set(value))):
            raise ValueError("semantic keys must be unique and sorted")
        return value


class BusinessModelContextTemplateRole(_ContextContract):
    """One statement role the model template requires of every issuer."""

    role: SemanticKey
    display_name: ShortText
    statement: ShortText
    optional_if_unreported: bool


class BusinessModelContextOperatingNeed(_ContextContract):
    """A driver of this BusinessModel that names a disclosed source.

    ``operating_role`` is ``operating.<kpi_source>`` when ``kpi_source`` is a bare
    disclosed-series name; a ``kpi_source`` that is already a role id (a template
    statement role), a taxonomy QName or a registered BM ratio formula id names
    no operating role.  ``edgar_member``/``edgar_axis`` name a dimensioned
    disclosure and ``balance_of`` the balance-sheet role the driver observes.
    ``unit`` carries the driver's declared unit; older projections without it
    retain their serialized bytes when read again.
    """

    driver_key: SemanticKey
    label: ShortText
    operating_role: SemanticKey | None = None
    kpi_source: ShortText | None = None
    edgar_member: ShortText | None = None
    edgar_axis: ShortText | None = None
    observation_basis: ObservationBasis | None = None
    balance_of: BalanceRole | None = None
    revenue_scope: RevenueScope | None = None
    unit: Unit | None = Field(
        default=None, exclude_if=lambda value: value is None
    )


class BusinessModelContextSummaryProjection(_ContextContract):
    kind: Literal["summary"] = "summary"
    economic_summary: Prose
    business_type: ShortText | None = None
    recommended_depth: RecommendedDepth | None = None
    confidence: Literal["low", "medium", "high"]
    key_decisions: tuple[Prose, ...] = Field(max_length=MAX_KEY_DECISIONS)
    recommended_next_stage: ShortText
    claim_keys: tuple[SemanticKey, ...] = ()
    evidence_refs: tuple[BusinessModelContextEvidenceRef, ...] = ()
    counts: BusinessModelContextCounts
    semantic_keys: BusinessModelContextSemanticKeys
    overflow: BusinessModelContextOverflow = Field(
        default_factory=BusinessModelContextOverflow
    )
    template_required_roles: tuple[BusinessModelContextTemplateRole, ...] = ()
    operating_needs: tuple[BusinessModelContextOperatingNeed, ...] = ()

    @model_validator(mode="after")
    def _counts_match_indexes(self) -> "BusinessModelContextSummaryProjection":
        actual = (
            len(self.semantic_keys.segments),
            len(self.semantic_keys.drivers),
            len(self.semantic_keys.costs),
            len(self.semantic_keys.capital),
            len(self.semantic_keys.limitations),
        )
        expected = (
            self.counts.segments,
            self.counts.drivers,
            self.counts.costs,
            self.counts.capital_items,
            self.counts.limitations,
        )
        if actual != expected:
            raise ValueError("summary counts must match semantic key indexes")
        return self


class BusinessModelContextItemProjection(_ContextContract):
    kind: Literal["item"] = "item"
    item: BusinessModelContextItem


BusinessModelContextProjection = Annotated[
    BusinessModelContextSummaryProjection | BusinessModelContextItemProjection,
    Field(discriminator="kind"),
]


class BusinessModelContextResponse(_ContextContract):
    status: Literal["success"] = "success"
    context: BusinessModelContextHeader
    projection: BusinessModelContextProjection | None = None
    findings: tuple[Finding, ...] = ()
    next_call: dict[str, Any] | None = Field(
        default=None, exclude_if=lambda value: value is None
    )

    @model_validator(mode="after")
    def _selected_revision_projection(self) -> "BusinessModelContextResponse":
        if self.context.selected_revision_ref is None:
            if self.projection is not None:
                raise ValueError("empty selected slot must not have a projection")
        elif self.projection is None:
            raise ValueError("selected revision must have a projection")
        return self


__all__ = [
    "BusinessModelContextArtifactRef",
    "BusinessModelContextCapitalItem",
    "BusinessModelContextCostItem",
    "BusinessModelContextCounts",
    "BusinessModelContextDriverItem",
    "BusinessModelContextEvidenceRef",
    "BusinessModelContextEvidenceSnapshotRef",
    "BusinessModelContextHeader",
    "BusinessModelContextItem",
    "BusinessModelContextItemProjection",
    "BusinessModelContextLimitationItem",
    "BusinessModelContextMethodologyRef",
    "BusinessModelContextOperatingNeed",
    "BusinessModelContextOverflow",
    "BusinessModelContextRelationship",
    "BusinessModelContextResponse",
    "BusinessModelContextRevisionRef",
    "BusinessModelContextSegmentItem",
    "BusinessModelContextSemanticKeys",
    "BusinessModelContextStageReceiptRef",
    "BusinessModelContextSummaryProjection",
    "BusinessModelContextTemplateRole",
    "MAX_CAPITAL_ITEMS",
    "MAX_COSTS",
    "MAX_DRIVERS",
    "MAX_EVIDENCE_REFS",
    "MAX_KEY_DECISIONS",
    "MAX_LIMITATIONS",
    "MAX_SEGMENTS",
    "MAX_TEMPLATE_ROLES",
    "MAX_UPSTREAM_REFS",
]
