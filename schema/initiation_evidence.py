"""Shared evidence, coverage, and flexible numeric contracts for dossier v5."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType
from typing import Annotated, ClassVar, Generic, Literal, Self, TypeAlias, TypeVar, get_args

from pydantic import Field, model_validator

from value_semantics_core.decimal_wire import CanonicalDecimalWire
from .numeric_display import NumericDisplaySpec
from industry_slice_core._contract import _ContractModel
from .versioned_artifact import VersionedArtifactRef


ResearchStatus = Literal["completed", "partial", "not_started"]
EvidenceStatus = Literal[
    "available",
    "partial",
    "not_disclosed",
    "not_applicable",
    "insufficient",
    "conflicting",
]
DecisionRelevance = Literal["required", "material", "contextual", "not_material"]
InitiationSectionId = Literal[
    "investment_conclusion",
    "investment_considerations",
    "company",
    "industry",
    "competitive_comparison",
    "operating_history",
    "forecast_guidance",
    "forecast_consensus",
    "forecast_scenarios",
    "valuation",
    "expected_value",
    "thesis",
    "risks",
]
InitiationEvidenceCapability = Literal[
    "business_overview",
    "company_diligence",
    "industry_diligence",
    "competitive_comparison",
    "operating_history",
    "management_guidance",
    "consensus_comparison",
    "scenario_analysis",
    "valuation",
    "expected_value",
    "thesis_articulation",
    "risk_assessment",
    "financial_red_flags",
    "position_predecision",
    "position_plan",
]
CAPABILITIES: frozenset[InitiationEvidenceCapability] = frozenset(
    get_args(InitiationEvidenceCapability)
)

SemanticDossierContractName = Literal[
    "fundamental-research",
    "position-initiation-predecision",
    "managing-risk",
    "thesis-articulation",
    "comparative-analysis",
    "guidance-extraction",
    "model-vs-consensus",
    "scenario-set",
    "dcf-relative-valuation",
    "scenario-multiple-pricing",
    "expected-value-decision",
    "identifying-risk",
    "financial-red-flags",
    "competitive-position",
]

SECTION_IDS: tuple[InitiationSectionId, ...] = (
    "investment_conclusion",
    "investment_considerations",
    "company",
    "industry",
    "competitive_comparison",
    "operating_history",
    "forecast_guidance",
    "forecast_consensus",
    "forecast_scenarios",
    "valuation",
    "expected_value",
    "thesis",
    "risks",
)

SECTION_DECISION_RELEVANCE: dict[InitiationSectionId, DecisionRelevance] = {
    "investment_conclusion": "required",
    "investment_considerations": "material",
    "company": "material",
    "industry": "material",
    "competitive_comparison": "material",
    "operating_history": "material",
    "forecast_guidance": "contextual",
    "forecast_consensus": "contextual",
    "forecast_scenarios": "material",
    "valuation": "material",
    "expected_value": "contextual",
    "thesis": "material",
    "risks": "material",
}


def section_degrades_verdict(
    section_id: InitiationSectionId, evidence_status: EvidenceStatus
) -> bool:
    """Whether a section's evidence gap makes the dossier partially compiled.

    The single verdict rule for the compiler and the persisted-pack reader. A
    ``required`` or ``material`` section with a gap degrades the verdict; a
    ``contextual`` section informs the reader without gating the compile.
    """

    return SECTION_DECISION_RELEVANCE[section_id] in {
        "required",
        "material",
    } and evidence_status in {
        "partial",
        "not_disclosed",
        "insufficient",
        "conflicting",
    }


@dataclass(frozen=True)
class SectionRequirement:
    """Allowed capabilities and an available-section expression in DNF."""

    allowed_capabilities: frozenset[InitiationEvidenceCapability]
    any_of_clauses: tuple[frozenset[InitiationEvidenceCapability], ...]


SECTION_REQUIREMENTS: Mapping[InitiationSectionId, SectionRequirement] = MappingProxyType(
    {
        # The work-up's conclusion is its thesis plus the managing-risk position
        # plan; a position-initiation-predecision is optional enrichment.
        "investment_conclusion": SectionRequirement(
            frozenset(
                {
                    "thesis_articulation",
                    "position_plan",
                    "position_predecision",
                    "expected_value",
                    "valuation",
                }
            ),
            (
                frozenset({"thesis_articulation", "position_plan", "expected_value"}),
                frozenset({"thesis_articulation", "position_plan", "valuation"}),
            ),
        ),
        "investment_considerations": SectionRequirement(
            frozenset({"thesis_articulation", "risk_assessment", "financial_red_flags"}),
            (
                frozenset({"thesis_articulation", "risk_assessment"}),
                frozenset({"thesis_articulation", "financial_red_flags"}),
            ),
        ),
        "company": SectionRequirement(
            frozenset({"business_overview", "company_diligence"}),
            (frozenset({"business_overview", "company_diligence"}),),
        ),
        "industry": SectionRequirement(
            frozenset({"industry_diligence"}), (frozenset({"industry_diligence"}),)
        ),
        "competitive_comparison": SectionRequirement(
            frozenset({"competitive_comparison", "industry_diligence"}),
            (frozenset({"competitive_comparison"}),),
        ),
        "operating_history": SectionRequirement(
            frozenset({"operating_history"}), (frozenset({"operating_history"}),)
        ),
        "forecast_guidance": SectionRequirement(
            frozenset({"management_guidance"}), (frozenset({"management_guidance"}),)
        ),
        "forecast_consensus": SectionRequirement(
            frozenset({"consensus_comparison"}), (frozenset({"consensus_comparison"}),)
        ),
        "forecast_scenarios": SectionRequirement(
            frozenset({"scenario_analysis"}), (frozenset({"scenario_analysis"}),)
        ),
        "valuation": SectionRequirement(
            frozenset({"valuation"}), (frozenset({"valuation"}),)
        ),
        "expected_value": SectionRequirement(
            frozenset({"expected_value", "valuation", "scenario_analysis"}),
            (frozenset({"expected_value", "valuation", "scenario_analysis"}),),
        ),
        "thesis": SectionRequirement(
            frozenset({"thesis_articulation"}), (frozenset({"thesis_articulation"}),)
        ),
        "risks": SectionRequirement(
            frozenset({"risk_assessment", "financial_red_flags"}),
            (frozenset({"risk_assessment"}), frozenset({"financial_red_flags"})),
        ),
    }
)

SECTION_ALLOWED_CAPABILITIES: dict[
    InitiationSectionId, frozenset[InitiationEvidenceCapability]
] = {
    section_id: req.allowed_capabilities
    for section_id, req in SECTION_REQUIREMENTS.items()
}


class SemanticContractAdapterKey(_ContractModel):
    key_kind: Literal["semantic_contract"] = "semantic_contract"
    contract_name: SemanticDossierContractName
    schema_version: Literal["2.0"] = "2.0"


InitiationAdapterKey: TypeAlias = SemanticContractAdapterKey


class ProducerLocalEvidenceRef(_ContractModel):
    ref_kind: Literal["producer_local"] = "producer_local"
    source_id: str = Field(min_length=1)
    producer_locator: str = Field(min_length=1)
    claim_id: str | None = Field(default=None, min_length=1)


class CirculatedEvidenceRef(_ContractModel):
    ref_kind: Literal["circulated"] = "circulated"
    artifact_ref: VersionedArtifactRef
    source_id: str = Field(min_length=1)
    producer_locator: str = Field(min_length=1)
    claim_id: str | None = Field(default=None, min_length=1)


EvidenceReference: TypeAlias = Annotated[
    ProducerLocalEvidenceRef | CirculatedEvidenceRef,
    Field(discriminator="ref_kind"),
]


T = TypeVar("T")


class AttributedValue(_ContractModel, Generic[T]):
    value: T
    evidence_refs: list[EvidenceReference] = Field(min_length=1)


class AttributedClaim(_ContractModel):
    claim_id: str = Field(min_length=1)
    text: str = Field(min_length=1)
    evidence_refs: list[EvidenceReference] = Field(min_length=1)


class CapabilityBinding(_ContractModel):
    binding_id: str = Field(min_length=1)
    capability: InitiationEvidenceCapability
    artifact_ids: list[str] = Field(min_length=1)
    section_id: InitiationSectionId

    @model_validator(mode="after")
    def _validate_binding(self) -> "CapabilityBinding":
        if len(self.artifact_ids) != len(set(self.artifact_ids)):
            raise ValueError("capability binding artifact_ids must be unique")
        if self.capability not in SECTION_ALLOWED_CAPABILITIES[self.section_id]:
            raise ValueError(
                f"capability {self.capability!r} is not allowed for section "
                f"{self.section_id!r}"
            )
        return self


class SectionCoverage(_ContractModel):
    research_status: ResearchStatus
    evidence_status: EvidenceStatus
    rationale: str = Field(min_length=1)
    decision_relevance: DecisionRelevance
    binding_ids: list[str] = Field(default_factory=list)
    evidence_refs: list[CirculatedEvidenceRef] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_state(self) -> "SectionCoverage":
        legal = {
            "completed": {
                "available",
                "partial",
                "not_disclosed",
                "not_applicable",
                "insufficient",
                "conflicting",
            },
            "partial": {"partial", "insufficient", "conflicting"},
            "not_started": {"insufficient", "not_applicable"},
        }
        if self.evidence_status not in legal[self.research_status]:
            raise ValueError(
                f"illegal coverage state: {self.research_status}/{self.evidence_status}"
            )
        if len(self.binding_ids) != len(set(self.binding_ids)):
            raise ValueError("coverage binding_ids must be unique")
        evidence_keys = [
            (
                ref.artifact_ref.provider_id,
                ref.artifact_ref.artifact_id,
                ref.artifact_ref.artifact_version,
                ref.source_id,
                ref.claim_id,
            )
            for ref in self.evidence_refs
        ]
        if len(evidence_keys) != len(set(evidence_keys)):
            raise ValueError("coverage evidence_refs must be unique")

        requires_evidence = self.evidence_status in {
            "available",
            "partial",
            "not_disclosed",
            "conflicting",
        }
        if requires_evidence and (not self.binding_ids or not self.evidence_refs):
            raise ValueError(
                f"{self.evidence_status} coverage requires a binding and evidence ref"
            )
        if (
            self.evidence_status == "not_applicable"
            and self.decision_relevance in {"required", "material"}
            and (not self.binding_ids or not self.evidence_refs)
        ):
            raise ValueError(
                "required/material not_applicable coverage requires supporting evidence"
            )
        if (
            self.evidence_status == "insufficient"
            and self.research_status != "not_started"
            and (not self.binding_ids or not self.evidence_refs)
        ):
            raise ValueError(
                "reviewed insufficient coverage requires a binding and evidence ref"
            )
        return self


class EvidenceAssessment(_ContractModel):
    """Two-axis evidence state for a domain observation below report-section level."""

    research_status: ResearchStatus
    evidence_status: EvidenceStatus
    rationale: str = Field(min_length=1)
    evidence_refs: list[EvidenceReference] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_state(self) -> "EvidenceAssessment":
        legal = {
            "completed": {
                "available",
                "partial",
                "not_disclosed",
                "not_applicable",
                "insufficient",
                "conflicting",
            },
            "partial": {"partial", "insufficient", "conflicting"},
            "not_started": {"insufficient", "not_applicable"},
        }
        if self.evidence_status not in legal[self.research_status]:
            raise ValueError(
                f"illegal evidence state: {self.research_status}/{self.evidence_status}"
            )
        if (
            self.evidence_status
            in {
                "available",
                "partial",
                "not_disclosed",
                "conflicting",
            }
            and not self.evidence_refs
        ):
            raise ValueError(f"{self.evidence_status} evidence requires evidence_refs")
        if (
            self.evidence_status == "insufficient"
            and self.research_status != "not_started"
            and not self.evidence_refs
        ):
            raise ValueError("reviewed insufficient evidence requires evidence_refs")
        return self


EstimateKind = Literal[
    "point",
    "range",
    "bounded",
    "proxy",
    "conflicting",
    "not_disclosed",
    "not_applicable",
]
EstimateOrigin = Literal["management", "third_party", "regulatory", "analyst_derived"]


def _finite(value: Decimal | None, field_name: str) -> None:
    if value is not None and not value.is_finite():
        raise ValueError(f"{field_name} must be finite")


EstimatePeriod: TypeAlias = Annotated[str, Field(min_length=1)]
EstimatePeriodT = TypeVar("EstimatePeriodT", default=EstimatePeriod)


class EstimateAlternative(_ContractModel, Generic[EstimatePeriodT]):
    quantity_bindings: ClassVar[tuple[tuple[str, str], ...]] = (
        ("point", "display"),
        ("low", "display"),
        ("high", "display"),
    )

    label: str = Field(min_length=1)
    point: CanonicalDecimalWire | None = None
    low: CanonicalDecimalWire | None = None
    high: CanonicalDecimalWire | None = None
    display: NumericDisplaySpec
    period: EstimatePeriodT
    methodology: str = Field(min_length=1)
    origin: EstimateOrigin
    evidence_refs: list[EvidenceReference] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_shape(self) -> Self:
        for name in ("point", "low", "high"):
            _finite(getattr(self, name), name)
        has_point = self.point is not None
        has_range = self.low is not None and self.high is not None
        if has_point == has_range:
            raise ValueError("estimate alternative requires either point or low/high")
        if self.low is not None and self.high is not None and self.low > self.high:
            raise ValueError("estimate alternative low cannot exceed high")
        if (self.low is None) != (self.high is None):
            raise ValueError("estimate alternative range requires both low and high")
        return self


EstimateAlternativeT = TypeVar(
    "EstimateAlternativeT",
    default=EstimateAlternative,
)


class EstimateValue(
    _ContractModel,
    Generic[EstimatePeriodT, EstimateAlternativeT],
):
    quantity_bindings: ClassVar[tuple[tuple[str, str], ...]] = (
        ("point", "display"),
        ("low", "display"),
        ("high", "display"),
    )

    estimate_kind: EstimateKind
    point: CanonicalDecimalWire | None = None
    low: CanonicalDecimalWire | None = None
    high: CanonicalDecimalWire | None = None
    alternatives: list[EstimateAlternativeT] = Field(default_factory=list)
    display: NumericDisplaySpec | None = None
    period: EstimatePeriodT | None = None
    scope: str = Field(min_length=1)
    methodology: str | None = Field(default=None, min_length=1)
    origin: EstimateOrigin | None = None
    evidence_refs: list[EvidenceReference] = Field(default_factory=list)
    rationale: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _validate_shape(self) -> Self:
        for name in ("point", "low", "high"):
            _finite(getattr(self, name), name)
        kind = self.estimate_kind
        numeric = {"point", "range", "bounded", "proxy"}
        if kind in {"point", "proxy"}:
            if self.point is None or self.low is not None or self.high is not None:
                raise ValueError(f"{kind} estimate requires point and prohibits bounds")
        elif kind == "range":
            if self.point is not None or self.low is None or self.high is None:
                raise ValueError("range estimate requires low/high and prohibits point")
            if self.low > self.high:
                raise ValueError("range estimate low cannot exceed high")
        elif kind == "bounded":
            if self.point is not None or (self.low is None and self.high is None):
                raise ValueError("bounded estimate requires at least one bound")
            if self.low is not None and self.high is not None and self.low > self.high:
                raise ValueError("bounded estimate low cannot exceed high")
        elif kind == "conflicting":
            if any(value is not None for value in (self.point, self.low, self.high)):
                raise ValueError(
                    "conflicting estimate must not synthesize a numeric value"
                )
            if len(self.alternatives) < 2:
                raise ValueError(
                    "conflicting estimate requires at least two alternatives"
                )
        else:
            if any(value is not None for value in (self.point, self.low, self.high)):
                raise ValueError(f"{kind} estimate must omit numeric values")

        if kind != "conflicting" and self.alternatives:
            raise ValueError("alternatives are only valid for conflicting estimates")
        if kind in numeric:
            if not all((self.display, self.period, self.methodology, self.origin)):
                raise ValueError(
                    "numeric estimates require display, period, methodology, and origin"
                )
            if not self.evidence_refs:
                raise ValueError("numeric estimates require evidence_refs")
            if kind == "proxy" and not self.rationale:
                raise ValueError("proxy estimate requires rationale")
        elif kind == "conflicting":
            if not self.rationale:
                raise ValueError("conflicting estimate requires rationale")
            if any(
                value is not None
                for value in (
                    self.display,
                    self.period,
                    self.methodology,
                    self.origin,
                )
            ):
                raise ValueError(
                    "conflicting estimate metadata belongs on each alternative"
                )
        else:
            if not self.rationale:
                raise ValueError(f"{kind} estimate requires rationale")
            if any(
                value is not None
                for value in (
                    self.display,
                    self.period,
                    self.methodology,
                    self.origin,
                )
            ):
                raise ValueError(f"{kind} estimate must omit numeric metadata")
            if kind == "not_disclosed" and not self.evidence_refs:
                raise ValueError("not_disclosed estimate requires evidence_refs")
        return self


__all__ = [
    "AttributedClaim",
    "AttributedValue",
    "CapabilityBinding",
    "CAPABILITIES",
    "DecisionRelevance",
    "EstimateAlternative",
    "EstimateKind",
    "EstimateOrigin",
    "EstimateValue",
    "EvidenceAssessment",
    "CirculatedEvidenceRef",
    "EvidenceReference",
    "EvidenceStatus",
    "InitiationEvidenceCapability",
    "InitiationSectionId",
    "ProducerLocalEvidenceRef",
    "ResearchStatus",
    "SECTION_ALLOWED_CAPABILITIES",
    "SECTION_DECISION_RELEVANCE",
    "SECTION_IDS",
    "SECTION_REQUIREMENTS",
    "SectionCoverage",
    "SectionRequirement",
    "section_degrades_verdict",
]
