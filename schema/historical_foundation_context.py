"""Paged producer context for deterministic historical-foundation review.

The model-facing response contains only natural issue/evidence keys. Exact
upstream lineage remains inside the copyable context locator and is resolved
again under authenticated scope before a targeted read or write may trust it.

Snapshot-derived collections and counts remain complete unless the producer
explicitly pages them; projection validation does not cap its own output.

Consumed by ``api/research/historical_foundation_context_service.py``.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
from typing import Annotated, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .finding_vocabulary import Finding
from .build_model_product_contracts import (
    HistoricalBasisFact,
    SourceEvidence,
    validate_historical_reconciliation_bindings,
)
from .business_model_revision_ref import BusinessModelRevisionRef
from .business_model_stage import BusinessModelSelectedEvidence
from .historical_driver_seeds import HistoricalDriverSeedDerivationKind
from .historical_foundation import HistoricalFactSnapshotRef, HistoricalFoundationGap
from .models import ModelScale
from industry_slice_core.industry_types import Unit
from .provider_port.contracts import FilingLineRef
from .stable_control_identity import FiscalPeriod
from .model_state_identity import validate_sha256_hex
from .versioned_artifact import canonical_json_bytes


MAX_BATCH_KEYS = 16
MAX_PERIODS = 64
# The period drilldown projects only this first page, ordered by fact_id.
MAX_PERIOD_FACTS = 160
# A seed requirement lists at most this many fact candidates, relevant first; the
# summary's ``fact_candidate_count`` against ``fact_candidates_shown`` measures the cut.
MAX_SEED_FACT_CANDIDATES = MAX_PERIOD_FACTS

SemanticKey = Annotated[str, Field(min_length=1, max_length=256)]
NaturalDriverKey = Annotated[str, Field(min_length=1, max_length=512)]
SeedRequirementKey = Annotated[str, Field(min_length=1, max_length=544)]
ExactLookupKey = Annotated[str, Field(min_length=1, max_length=1024)]
ShortText = Annotated[str, Field(min_length=1, max_length=512)]
Prose = Annotated[str, Field(min_length=1, max_length=4096)]


class _ContextContract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


class HistoricalFoundationContextBinding(_ContextContract):
    """Exact locator payload. An upstream the context could not resolve (no
    selected BusinessModel) is ``None`` on every parent field and a
    ``historical_foundation_upstream_unresolved`` Finding on the response."""

    schema_version: Literal[
        "historical-foundation-context-binding.v1",
        "historical-foundation-context-binding.v2",
    ] = (
        "historical-foundation-context-binding.v1"
    )
    user_id: str = Field(min_length=1)
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1, max_length=32)
    selected_business_model_revision_ref: BusinessModelRevisionRef | None = None
    business_model_selection_epoch: int | None = Field(default=None, gt=0)
    business_model_stage_receipt_digest: str | None = None
    historical_fact_snapshot_ref: HistoricalFactSnapshotRef | None = None
    mapping_plan_sha256: str | None = None
    context_content_sha256: str

    @field_validator(
        "business_model_stage_receipt_digest",
        "mapping_plan_sha256",
        "context_content_sha256",
    )
    @classmethod
    def _digests(cls, value: str | None, info) -> str | None:
        if value is None:
            return None
        return validate_sha256_hex(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _content_addressed(self) -> "HistoricalFoundationContextBinding":
        parents = (
            self.selected_business_model_revision_ref,
            self.business_model_selection_epoch,
            self.business_model_stage_receipt_digest,
            self.historical_fact_snapshot_ref,
            self.mapping_plan_sha256,
        )
        absent = tuple(parent is None for parent in parents)
        if any(absent) and not all(absent):
            raise ValueError(
                "historical-foundation context binding parents are all present or all absent"
            )
        snapshot_ref = self.historical_fact_snapshot_ref
        if snapshot_ref is not None:
            expected_version = (
                "historical-foundation-context-binding.v2"
                if snapshot_ref.schema_version == "historical-fact-snapshot.v2"
                else "historical-foundation-context-binding.v1"
            )
            if self.schema_version != expected_version:
                raise ValueError(
                    "historical-foundation context version must match snapshot"
                )
        expected = historical_foundation_context_content_digest(
            user_id=self.user_id,
            research_file_id=self.research_file_id,
            ticker=self.ticker,
            selected_business_model_revision_ref=(
                self.selected_business_model_revision_ref
            ),
            business_model_selection_epoch=self.business_model_selection_epoch,
            business_model_stage_receipt_digest=(
                self.business_model_stage_receipt_digest
            ),
            historical_fact_snapshot_ref=snapshot_ref,
            mapping_plan_sha256=self.mapping_plan_sha256,
        )
        if self.context_content_sha256 != expected:
            raise ValueError("historical-foundation context binding digest mismatch")
        return self


def historical_foundation_context_content_digest(
    *,
    user_id: str,
    research_file_id: int,
    ticker: str,
    selected_business_model_revision_ref: BusinessModelRevisionRef | None = None,
    business_model_selection_epoch: int | None = None,
    business_model_stage_receipt_digest: str | None = None,
    historical_fact_snapshot_ref: HistoricalFactSnapshotRef | None = None,
    mapping_plan_sha256: str | None = None,
) -> str:
    schema_version = (
        "historical-foundation-context.v2"
        if historical_fact_snapshot_ref is not None
        and historical_fact_snapshot_ref.schema_version == "historical-fact-snapshot.v2"
        else "historical-foundation-context.v1"
    )
    payload = {
        "schema_version": schema_version,
        "user_id": user_id,
        "research_file_id": research_file_id,
        "ticker": ticker,
        "selected_business_model_revision_ref": selected_business_model_revision_ref,
        "business_model_selection_epoch": business_model_selection_epoch,
        "business_model_stage_receipt_digest": business_model_stage_receipt_digest,
        "historical_fact_snapshot_ref": historical_fact_snapshot_ref,
        "mapping_plan_sha256": mapping_plan_sha256,
    }
    return hashlib.sha256(
        schema_version.encode("utf-8") + b"\0" + canonical_json_bytes(payload)
    ).hexdigest()


def encode_historical_foundation_context_ref(
    binding: HistoricalFoundationContextBinding,
) -> str:
    payload = canonical_json_bytes(binding.model_dump(mode="json"))
    encoded = base64.urlsafe_b64encode(payload).decode("ascii").rstrip("=")
    version = 2 if binding.schema_version.endswith(".v2") else 1
    checksum = hashlib.sha256(
        f"historical-foundation-context-ref.v{version}".encode("utf-8")
        + b"\0"
        + payload
    ).hexdigest()
    return f"hfcx{version}.{encoded}.{checksum}"


def decode_historical_foundation_context_ref(
    value: str,
) -> HistoricalFoundationContextBinding:
    try:
        prefix, encoded, checksum = value.split(".", 2)
        if prefix not in {"hfcx1", "hfcx2"} or len(checksum) != 64:
            raise ValueError
        version = prefix.removeprefix("hfcx")
        payload = base64.urlsafe_b64decode(encoded + "=" * (-len(encoded) % 4))
        expected = hashlib.sha256(
            f"historical-foundation-context-ref.v{version}".encode("utf-8")
            + b"\0"
            + payload
        ).hexdigest()
        if not hmac.compare_digest(checksum, expected):
            raise ValueError
        return HistoricalFoundationContextBinding.model_validate_json(payload)
    except (TypeError, ValueError) as exc:
        raise ValueError("historical-foundation context ref is invalid") from exc


class HistoricalFoundationContextHeader(_ContextContract):
    schema_version: Literal[
        "historical-foundation-context.v1",
        "historical-foundation-context.v2",
    ] = (
        "historical-foundation-context.v1"
    )
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1, max_length=32)
    context_ref: str = Field(min_length=1)
    # Measured, never bounded: the issued token's length, derived below.
    context_ref_length: int = 0

    @model_validator(mode="after")
    def _measure_context_ref(self) -> "HistoricalFoundationContextHeader":
        object.__setattr__(self, "context_ref_length", len(self.context_ref))
        return self


class HistoricalFoundationReadiness(_ContextContract):
    status: Literal["auto_accepted", "blocked_prerequisite", "judgment_required"]
    requires_judgment: bool
    next_action: Literal[
        "continue_with_accepted_foundation",
        "repair_deterministic_prerequisite",
        "resolve_material_issues",
        "resolve_historical_driver_seeds",
        "resolve_material_issues_and_historical_driver_seeds",
    ]
    message: Prose


class HistoricalFoundationCoverageSummary(_ContextContract):
    historical_period_ids: tuple[SemanticKey, ...]
    required_observations: int = Field(ge=0)
    covered_observations: int = Field(ge=0)
    unresolved_observations: int = Field(ge=0)
    fact_count: int = Field(ge=0)
    evidence_count: int = Field(ge=0)


class HistoricalFoundationSeedRequirementSummary(_ContextContract):
    """Natural historical opening-value requirement without mechanical identity."""

    requirement_key: SeedRequirementKey
    driver_key: NaturalDriverKey
    period_id: SemanticKey
    unit: Unit
    model_scale: ModelScale
    observation_basis: Literal["period_flow", "period_end", "period_average"]
    predecessor_period_id: SemanticKey | None = None
    allowed_derivation_kinds: tuple[HistoricalDriverSeedDerivationKind, ...] = Field(
        min_length=1,
    )
    fact_candidate_count: int = Field(ge=0)
    fact_candidates_shown: int = Field(ge=0)
    business_model_value_candidate_count: int = Field(ge=0)
    supporting_evidence_count: int = Field(ge=0)
    #: Ordered keys into the summary projection's shared, source-scale fact pool.
    fact_candidate_keys: tuple[ExactLookupKey, ...] = Field(default=(), max_length=5)
    business_model_value_candidates: tuple[
        HistoricalFoundationSeedBusinessModelValueCandidate, ...
    ] = ()


class HistoricalFoundationSeedFactCandidate(_ContextContract):
    operand_key: ExactLookupKey
    line: FilingLineRef
    period_id: SemanticKey
    value: str = Field(min_length=1)
    unit: str = Field(min_length=1, max_length=128)
    scale: str = Field(min_length=1, max_length=128)
    source_scale: str | None = None
    confidence: Literal["high", "medium", "low"]
    source_quality: Literal["primary", "fallback", "degraded"]


class HistoricalFoundationSeedBusinessModelValueCandidate(_ContextContract):
    operand_key: ExactLookupKey
    supporting_evidence_key: ExactLookupKey
    evidence_value_id: str = Field(min_length=1, max_length=128)
    period_id: SemanticKey
    series_id: NaturalDriverKey | None = None
    value: str = Field(min_length=1)
    unit: str = Field(min_length=1, max_length=128)
    scale: str = Field(min_length=1, max_length=128)
    source_scale: str | None = None


class HistoricalFoundationSeedRequirementProjection(_ContextContract):
    requirement: HistoricalFoundationSeedRequirementSummary
    fact_candidates: tuple[HistoricalFoundationSeedFactCandidate, ...]
    business_model_value_candidates: tuple[
        HistoricalFoundationSeedBusinessModelValueCandidate, ...
    ]
    supporting_evidence_keys: tuple[ExactLookupKey, ...]
    #: Why the read issues no supporting-evidence key for this requirement, and
    #: the channel that does admit an analyst literal's support; the context
    #: service sets it only when ``supporting_evidence_keys`` is empty.
    supporting_evidence_absent_reason: str | None = Field(
        default=None,
        min_length=1,
        exclude_if=lambda value: value is None,
    )

    @model_validator(mode="after")
    def _canonical_order(self) -> "HistoricalFoundationSeedRequirementProjection":
        # Fact candidates are in the producer's reading order (relevant first), not sorted.
        fact_keys = tuple(row.operand_key for row in self.fact_candidates)
        if len(fact_keys) != len(set(fact_keys)):
            raise ValueError("historical seed fact candidates must be unique")
        operand_keys = tuple(
            row.operand_key for row in self.business_model_value_candidates
        )
        if operand_keys != tuple(sorted(set(operand_keys))):
            raise ValueError("BusinessModel value operands must be sorted and unique")
        evidence_keys = self.supporting_evidence_keys
        if evidence_keys != tuple(sorted(set(evidence_keys))):
            raise ValueError("supporting evidence keys must be sorted and unique")
        return self


class HistoricalFoundationSeedRequirementReferences(_ContextContract):
    """One batch member, referring to the projection's shared source-scale facts."""

    requirement: HistoricalFoundationSeedRequirementSummary
    fact_candidate_keys: tuple[ExactLookupKey, ...]
    business_model_value_candidates: tuple[
        HistoricalFoundationSeedBusinessModelValueCandidate, ...
    ]
    supporting_evidence_keys: tuple[ExactLookupKey, ...]
    supporting_evidence_absent_reason: str | None = Field(
        default=None, exclude_if=lambda value: value is None,
    )


class HistoricalFoundationSeedRequirementsProjection(_ContextContract):
    kind: Literal["seed_requirements"] = "seed_requirements"
    requirements: tuple[HistoricalFoundationSeedRequirementReferences, ...] = Field(
        min_length=1,
        max_length=MAX_BATCH_KEYS,
    )
    #: Values retain the snapshot's scale; each requirement names its model_scale.
    fact_candidates: dict[ExactLookupKey, HistoricalFoundationSeedFactCandidate]


class HistoricalFoundationPeriodFactsProjection(_ContextContract):
    kind: Literal["period"] = "period"
    period: FiscalPeriod
    fact_candidates: tuple[HistoricalFoundationSeedFactCandidate, ...] = Field(
        max_length=MAX_PERIOD_FACTS
    )


class HistoricalFoundationContextSummaryProjection(_ContextContract):
    kind: Literal["summary"] = "summary"
    readiness: HistoricalFoundationReadiness
    coverage: HistoricalFoundationCoverageSummary
    material_issue_keys: tuple[SemanticKey, ...]
    seed_requirements: tuple[HistoricalFoundationSeedRequirementSummary, ...] = ()
    unresolved_seed_requirement_keys: tuple[SeedRequirementKey, ...] = ()
    #: Shared top-five tier-0/1 facts, in source scale, keyed by unchanged operand_key.
    fact_candidates: dict[ExactLookupKey, HistoricalFoundationSeedFactCandidate] = Field(
        default_factory=dict,
    )
    annual_period_keys: tuple[SemanticKey, ...] = Field(
        default=(),
        max_length=MAX_PERIODS,
    )
    #: The snapshot's gaps (an annual period the pin binds nothing for, a
    #: missing terminal seed, an uncited review filing), each with its repair.
    gaps: tuple[HistoricalFoundationGap, ...] = ()

    @model_validator(mode="after")
    def _issue_shape(self) -> "HistoricalFoundationContextSummaryProjection":
        if self.material_issue_keys != tuple(sorted(set(self.material_issue_keys))):
            raise ValueError("material issue keys must be sorted and unique")
        requirement_keys = tuple(row.requirement_key for row in self.seed_requirements)
        if requirement_keys != tuple(sorted(set(requirement_keys))):
            raise ValueError("historical seed requirements must be sorted and unique")
        if any(
            row.requirement_key != f"seed_requirement:{row.driver_key}"
            for row in self.seed_requirements
        ):
            raise ValueError("historical seed requirement key must match driver key")
        unresolved = self.unresolved_seed_requirement_keys
        if unresolved != tuple(sorted(set(unresolved))) or not set(
            unresolved
        ) <= set(requirement_keys):
            raise ValueError("unresolved seed keys must name exact requirements")
        period_keys = self.annual_period_keys
        if period_keys != tuple(sorted(set(period_keys))):
            raise ValueError("annual period keys must be sorted and unique")
        return self


class HistoricalFoundationMappingCandidateProjection(_ContextContract):
    candidate_key: SemanticKey
    label: ShortText
    economic_treatment: ShortText
    requirement_id: SemanticKey
    period_id: SemanticKey
    line: FilingLineRef
    required_role: SemanticKey
    model_item: SemanticKey
    unit: str = Field(min_length=1, max_length=128)
    scale: str = Field(min_length=1, max_length=128)
    sign: str = Field(min_length=1, max_length=128)
    rationale: Prose
    evidence_keys: tuple[ExactLookupKey, ...]


class HistoricalFoundationMaterialIssueProjection(_ContextContract):
    issue_key: SemanticKey
    issue_type: SemanticKey
    question: Prose
    rationale: Prose
    materiality_basis: Prose
    candidates: tuple[HistoricalFoundationMappingCandidateProjection, ...] = Field(
        min_length=1,
    )


class HistoricalFoundationIssuesProjection(_ContextContract):
    kind: Literal["issues"] = "issues"
    issues: tuple[HistoricalFoundationMaterialIssueProjection, ...] = Field(
        min_length=1,
        max_length=MAX_BATCH_KEYS,
    )


class HistoricalFoundationSourceEvidence(_ContextContract):
    evidence_kind: Literal["source_evidence"] = "source_evidence"
    evidence_key: ExactLookupKey
    evidence: SourceEvidence


class HistoricalFoundationFactEvidence(_ContextContract):
    evidence_kind: Literal["historical_fact"] = "historical_fact"
    evidence_key: ExactLookupKey
    fact: HistoricalBasisFact

class HistoricalFoundationBusinessModelEvidence(_ContextContract):
    evidence_kind: Literal["business_model_evidence"] = "business_model_evidence"
    evidence_key: ExactLookupKey
    evidence: BusinessModelSelectedEvidence


HistoricalFoundationEvidence = Annotated[
    HistoricalFoundationSourceEvidence
    | HistoricalFoundationFactEvidence
    | HistoricalFoundationBusinessModelEvidence,
    Field(discriminator="evidence_kind"),
]


class HistoricalFoundationEvidenceProjection(_ContextContract):
    kind: Literal["evidence"] = "evidence"
    items: tuple[HistoricalFoundationEvidence, ...] = Field(
        min_length=1,
    )

    @model_validator(mode="after")
    def _closed_reconciliation_dependencies(
        self,
    ) -> "HistoricalFoundationEvidenceProjection":
        facts = tuple(
            row.fact
            for row in self.items
            if isinstance(row, HistoricalFoundationFactEvidence)
        )
        validate_historical_reconciliation_bindings(
            facts,
            allow_reconciliations=True,
        )
        return self


HistoricalFoundationContextProjection = Annotated[
    HistoricalFoundationContextSummaryProjection
    | HistoricalFoundationSeedRequirementsProjection
    | HistoricalFoundationPeriodFactsProjection
    | HistoricalFoundationIssuesProjection
    | HistoricalFoundationEvidenceProjection,
    Field(discriminator="kind"),
]


class HistoricalFoundationContextResponse(_ContextContract):
    """One issued context. ``findings`` carries
    ``historical_foundation_upstream_unresolved`` when no BusinessModel is
    selected (binding parents are then ``None``). ``projection`` is absent
    until a fact snapshot resolves. ``next_call`` produces that BusinessModel."""

    status: Literal["success"] = "success"
    context: HistoricalFoundationContextHeader
    context_ref: str | None = Field(default=None, exclude_if=lambda value: value is None)
    judgment_skeleton: dict[str, Any] | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    projection: HistoricalFoundationContextProjection | None = None
    findings: tuple[Finding, ...] = ()
    next_call: dict[str, Any] | None = Field(
        default=None, exclude_if=lambda value: value is None
    )

    @model_validator(mode="after")
    def _versioned_reconciliation_projection(
        self,
    ) -> "HistoricalFoundationContextResponse":
        if self.context_ref is None:
            object.__setattr__(self, "context_ref", self.context.context_ref)
        projection = self.projection
        if projection is None:
            return self
        facts = (
            tuple(
                row.fact
                for row in projection.items
                if isinstance(row, HistoricalFoundationFactEvidence)
            )
            if isinstance(projection, HistoricalFoundationEvidenceProjection)
            else ()
        )
        has_reconciliation = any(row.reconciliation is not None for row in facts)
        if has_reconciliation and self.context.schema_version != (
            "historical-foundation-context.v2"
        ):
            raise ValueError("bound historical facts require context v2")
        return self


__all__ = [
    "HistoricalFoundationContextBinding",
    "HistoricalFoundationContextHeader",
    "HistoricalFoundationContextResponse",
    "HistoricalFoundationContextSummaryProjection",
    "HistoricalFoundationBusinessModelEvidence",
    "HistoricalFoundationCoverageSummary",
    "HistoricalFoundationEvidenceProjection",
    "HistoricalFoundationFactEvidence",
    "HistoricalFoundationIssuesProjection",
    "HistoricalFoundationMappingCandidateProjection",
    "HistoricalFoundationMaterialIssueProjection",
    "HistoricalFoundationPeriodFactsProjection",
    "HistoricalFoundationReadiness",
    "HistoricalFoundationSeedFactCandidate",
    "HistoricalFoundationSeedBusinessModelValueCandidate",
    "HistoricalFoundationSeedRequirementProjection",
    "HistoricalFoundationSeedRequirementReferences",
    "HistoricalFoundationSeedRequirementSummary",
    "HistoricalFoundationSeedRequirementsProjection",
    "HistoricalFoundationSourceEvidence",
    "MAX_BATCH_KEYS",
    "decode_historical_foundation_context_ref",
    "encode_historical_foundation_context_ref",
    "historical_foundation_context_content_digest",
]
