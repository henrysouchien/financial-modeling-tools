"""Exact v2 producer envelopes for the twelve dossier semantic lanes."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date, datetime
import hashlib
import json
from types import MappingProxyType
from typing import Annotated, ClassVar, Generic, Literal, TypeVar, get_args

from pydantic import BaseModel, ConfigDict, Field, JsonValue, model_validator
from pydantic_core import PydanticUndefined

from .industry_diligence import IndustryDiligencePack
from .semantic_invariants import evaluate_registered_semantic_invariants
from .initiation_evidence import (
    EvidenceStatus,
    InitiationEvidenceCapability,
    ProducerLocalEvidenceRef,
    ResearchStatus,
)


class _StrictContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class SemanticLaneSource(_StrictContract):
    source_id: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    source_identity: str = Field(min_length=1)
    text: str


EVIDENCE_STATUSES_REQUIRING_SOURCE: frozenset[EvidenceStatus] = frozenset({
    "available",
    "partial",
    "not_disclosed",
    "conflicting",
})
"""Evidence statuses whose declaration must name the registered source it rests on."""

SEMANTIC_LANE_CAPABILITY_SOURCE_CLOSURE_RULE = (
    "A capability source_id must be the id of one of this publication's sources, "
    "and a publication's sources are exactly the ids its source_refs names: use "
    "a source id listed in source_refs, never the corpus identity that source "
    "cites (such as edgar:<accession>)."
)
"""The evidence-closure rule the typed outputs enforce, in the producer's words."""

RiskAssessmentState = Annotated[
    Literal[
        "risks_identified",
        "no_material_risks_identified",
        "partial",
        "conflicting",
    ],
    Field(
        description=(
            "The dossier risk-section state this assessment publishes; "
            "no_material_risks_identified admits no risk or flag rows."
        )
    ),
]


class SemanticLaneCapabilityEvidence(_StrictContract):
    capability: InitiationEvidenceCapability
    research_status: ResearchStatus
    evidence_status: EvidenceStatus
    rationale: str = Field(min_length=1)
    source_id: str | None = Field(default=None, min_length=1)
    producer_locator: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _validate_evidence_identity(self) -> "SemanticLaneCapabilityEvidence":
        requires_evidence = (
            self.evidence_status in EVIDENCE_STATUSES_REQUIRING_SOURCE
        )
        if requires_evidence and (
            self.source_id is None or self.producer_locator is None
        ):
            raise ValueError(
                f"{self.evidence_status} capability evidence requires exact source identity"
            )
        if (self.source_id is None) != (self.producer_locator is None):
            raise ValueError(
                "capability source_id and producer_locator must be declared together"
            )
        return self

    def as_local_ref(self) -> ProducerLocalEvidenceRef | None:
        if self.source_id is None or self.producer_locator is None:
            return None
        return ProducerLocalEvidenceRef(
            source_id=self.source_id,
            producer_locator=self.producer_locator,
        )


class SemanticLaneTypedOutputs(_StrictContract):
    producer_authored_fields: ClassVar[tuple[str, ...]] = ()
    """Required outputs the door copies verbatim from the producer's judgment."""

    source_refs: tuple[str, ...] = ()
    sources: tuple[SemanticLaneSource, ...] = ()
    initiation_capability_evidence: tuple[
        SemanticLaneCapabilityEvidence, ...
    ] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_evidence_closure(self) -> "SemanticLaneTypedOutputs":
        source_ids = tuple(source.source_id for source in self.sources)
        if source_ids != tuple(sorted(set(source_ids))):
            raise ValueError("semantic lane sources must be sorted and unique")
        if self.source_refs != tuple(sorted(set(self.source_refs))):
            raise ValueError("semantic lane source_refs must be sorted and unique")
        if not set(self.source_refs) <= set(source_ids):
            raise ValueError("semantic lane source_refs do not close over sources")
        declarations = self.initiation_capability_evidence
        capability_ids = tuple(item.capability for item in declarations)
        if capability_ids != tuple(sorted(set(capability_ids))):
            raise ValueError(
                "semantic lane capability declarations must be sorted and unique"
            )
        for declaration in declarations:
            if (
                declaration.source_id is not None
                and declaration.source_id not in source_ids
            ):
                raise ValueError(
                    "semantic lane capability evidence does not close over "
                    f"sources: {SEMANTIC_LANE_CAPABILITY_SOURCE_CLOSURE_RULE}"
                )
        return self


class PositionInitiationPredecisionOutputs(SemanticLaneTypedOutputs):
    recommendation_package: dict[str, JsonValue]
    expected_value_checkpoint: JsonValue | None = None
    inputs_read: tuple[JsonValue, ...] = ()
    readiness_gaps: tuple[JsonValue, ...] = ()
    human_follow_up: tuple[JsonValue, ...] = ()
    assumptions_preview: tuple[JsonValue, ...] = ()
    risks_preview: tuple[JsonValue, ...] = ()
    invalidation_triggers_preview: tuple[JsonValue, ...] = ()
    catalysts_preview: tuple[JsonValue, ...] = ()
    portfolio_fit_preview: JsonValue | None = None
    position_size_preview: JsonValue | None = None
    data_gaps: tuple[JsonValue, ...] = ()
    citation_degradation_warnings: tuple[JsonValue, ...] = ()


class ThesisArticulationOutputs(SemanticLaneTypedOutputs):
    producer_authored_fields: ClassVar[tuple[str, ...]] = (
        "source_date",
        "thesis_statement",
    )

    source_date: date = Field(
        description=(
            "The YYYY-MM-DD research date this articulation rests on: the as-of "
            "date of the evidence it read, which the dossier thesis section "
            "publishes as its source date."
        )
    )
    thesis_statement: dict[str, JsonValue] = Field(
        description=(
            "The thesis statement object (recommendation, one_sentence, "
            "direction, strategy, conviction, timeframe, "
            "paired_invalidation_trigger, falsifiable), for every verdict."
        )
    )
    differentiated_view: tuple[JsonValue, ...] = ()
    asymmetry_ratio: JsonValue | None = None
    asymmetry_diagnostics: JsonValue | None = None
    expectations_channels: JsonValue | None = None
    catalysts: tuple[JsonValue, ...] = ()
    case_type: JsonValue | None = None
    case_type_failure_mode: JsonValue | None = None
    case_type_valuation_methodology: JsonValue | None = None
    ten_minute_outline: JsonValue | None = None
    elevator_90_seconds: JsonValue | None = None
    elevator_word_count: int | None = None
    ninety_second_test_passes: bool | None = None
    audience_emphasis: JsonValue | None = None
    data_gaps: tuple[JsonValue, ...] = ()
    unmatched_claims: tuple[str, ...] = ()
    diligence_stack_audit: JsonValue | None = None
    recommended_next_action: JsonValue | None = None
    memo_lifecycle: JsonValue | None = None


class ComparativeAnalysisOutputs(SemanticLaneTypedOutputs):
    producer_authored_fields: ClassVar[tuple[str, ...]] = ("operating_as_of",)

    operating_as_of: date = Field(
        description=(
            "The YYYY-MM-DD date the peer and operating comparison observes: "
            "the latest period its metrics read."
        )
    )
    peer_comparison: dict[str, JsonValue] | None = None
    operating_comparison: dict[str, JsonValue] | None = None

    @model_validator(mode="after")
    def _require_available_comparisons(self) -> "ComparativeAnalysisOutputs":
        statuses = {
            item.capability: item.evidence_status
            for item in self.initiation_capability_evidence
        }
        for capability, field_name in (
            ("competitive_comparison", "peer_comparison"),
            ("operating_history", "operating_comparison"),
        ):
            if statuses.get(capability) in {"available", "partial", "conflicting"}:
                if getattr(self, field_name) is None:
                    raise ValueError(
                        f"{capability} evidence requires {field_name}"
                    )
        return self


class GuidanceExtractionOutputs(SemanticLaneTypedOutputs):
    guidance_table: JsonValue | None = None
    comparison_table: JsonValue | None = None
    driver_mapping: JsonValue | None = None
    assumption_change_assessment: JsonValue | None = None
    guidance_summary: JsonValue | None = None
    comparison_summary: JsonValue | None = None
    driver_mapping_summary: JsonValue | None = None
    beat_miss_summary: JsonValue | None = None
    answer_summary: JsonValue | None = None
    model_guidance_summary: JsonValue | None = None
    three_way_table: JsonValue | None = None
    data_gaps: tuple[JsonValue, ...] = ()
    patch_candidates_preview: tuple[JsonValue, ...] = ()
    citation_degradation_warnings: tuple[JsonValue, ...] = ()

    @model_validator(mode="after")
    def _require_available_guidance(self) -> "GuidanceExtractionOutputs":
        declaration = next(
            (
                item
                for item in self.initiation_capability_evidence
                if item.capability == "management_guidance"
            ),
            None,
        )
        if (
            declaration is not None
            and declaration.evidence_status
            in {"available", "partial", "conflicting"}
            and self.guidance_table is None
        ):
            raise ValueError("management_guidance evidence requires guidance_table")
        return self


class ModelVsConsensusOutputs(SemanticLaneTypedOutputs):
    comparison: JsonValue
    current_model: JsonValue | None = None
    comparison_table: tuple[JsonValue, ...] = ()
    material_deviations: tuple[JsonValue, ...] = ()
    valuation_impact: JsonValue | None = None
    eps_fcf: JsonValue | None = None
    consensus_view: JsonValue | None = None
    basis_check: JsonValue | None = None
    data_gaps: tuple[JsonValue, ...] = ()
    state_discrepancies: tuple[JsonValue, ...] = ()
    citation_degradation_warnings: tuple[JsonValue, ...] = ()


RelativeMethodPreflightId = Literal[
    "tpl.v.forward_pe.method_price",
    "tpl.v.forward_ev_ebitda.method_price",
]
_RELATIVE_METHOD_PREFLIGHT_ORDER: tuple[RelativeMethodPreflightId, ...] = (
    "tpl.v.forward_pe.method_price",
    "tpl.v.forward_ev_ebitda.method_price",
)


class DcfRelativeValuationOutputs(SemanticLaneTypedOutputs):
    price_target: JsonValue | None = None
    targets_by_method: JsonValue
    targets_by_scenario: JsonValue | None = None
    method_spread: JsonValue | None = None
    data_gaps: tuple[JsonValue, ...] = ()
    verdict: JsonValue | None = None
    workbook_writes: tuple[JsonValue, ...] = ()
    workbook_write: JsonValue | None = None
    recommended_next_action: JsonValue | None = None
    canonical_price_target: JsonValue | None = None
    dcf_calibration_gate: JsonValue | None = None
    verification: JsonValue | None = None
    publication_state: JsonValue | None = None
    publication_blocking_code: JsonValue | None = None
    publication_caveats: tuple[JsonValue, ...] = ()
    uncalibrated_legs: tuple[RelativeMethodPreflightId, ...] | None = None
    stage: Literal["preflight"] | None = None

    @model_validator(mode="after")
    def _require_success_price_target(self) -> "DcfRelativeValuationOutputs":
        if self.price_target is None and self.verdict not in {
            "INSUFFICIENT_DATA",
            "MODEL_NOT_CALIBRATED",
        }:
            raise ValueError(
                "DCF price_target may be omitted only for an explicit "
                "INSUFFICIENT_DATA or MODEL_NOT_CALIBRATED verdict"
            )
        diagnostics_supplied = self.uncalibrated_legs is not None
        if diagnostics_supplied != (self.stage is not None):
            raise ValueError(
                "DCF uncalibrated_legs and preflight stage must be supplied together"
            )
        if diagnostics_supplied:
            assert self.uncalibrated_legs is not None
            if self.verdict != "MODEL_NOT_CALIBRATED":
                raise ValueError(
                    "DCF preflight diagnostics require MODEL_NOT_CALIBRATED verdict"
                )
            if not self.uncalibrated_legs:
                raise ValueError("DCF preflight uncalibrated_legs must be non-empty")
            expected = tuple(
                item
                for item in _RELATIVE_METHOD_PREFLIGHT_ORDER
                if item in self.uncalibrated_legs
            )
            if self.uncalibrated_legs != expected:
                raise ValueError(
                    "DCF preflight legs must be unique and in canonical order"
                )
        return self


class ScenarioMultiplePricingOutputs(SemanticLaneTypedOutputs):
    scenarios: dict[str, JsonValue]
    scenario_target_horizons: dict[str, JsonValue] | None = None
    currency: str = Field(pattern=r"^[A-Z]{3}$")
    verification: JsonValue | None = None
    data_gaps: tuple[JsonValue, ...] = ()
    publication_state: JsonValue | None = None
    publication_blocking_code: JsonValue | None = None
    publication_caveats: tuple[JsonValue, ...] = ()


class ExpectedValueDecisionOutputs(SemanticLaneTypedOutputs):
    scenario_probabilities: JsonValue | None = None
    expected_value: JsonValue | None = None
    current_price: JsonValue | None = None
    scenario_targets: JsonValue | None = None
    probability_evidence: JsonValue | None = None
    materiality_check: JsonValue | None = None
    data_gaps: tuple[JsonValue, ...] = ()
    mutation_needed: bool | None = None
    state_changed: bool | None = None
    checkpoint_summary: JsonValue | None = None
    citation_degradation_warnings: tuple[JsonValue, ...] = ()


class IdentifyingRiskOutputs(SemanticLaneTypedOutputs):
    producer_authored_fields: ClassVar[tuple[str, ...]] = ("assessment_state",)

    assessment_state: RiskAssessmentState
    risks_preview: tuple[JsonValue, ...]
    data_gaps: tuple[JsonValue, ...] = ()
    systematic_risk_register: JsonValue | None = None
    idiosyncratic_risk_register: JsonValue | None = None
    sub_industry_risk: JsonValue | None = None
    market_conditions_severity_overlay: JsonValue | None = None
    unmatched_claims: tuple[JsonValue, ...] = ()
    invalidation_triggers_preview: tuple[JsonValue, ...] = ()
    workflow_readiness: JsonValue | None = None
    chain_verdict: JsonValue | None = None
    citation_degradation_warnings: tuple[JsonValue, ...] = ()


class FinancialRedFlag(_StrictContract):
    """One screened warning sign, as the financial-red-flags page declares a flag.

    The door parses each judgment flag into this row and publishes it in
    `flag_table`; `pillar` + `label` are the flag's stable identity.
    """

    pillar: str = Field(min_length=1)
    severity: Literal["SEVERE", "MATERIAL", "ELEVATED"]
    label: str = Field(min_length=1)
    evidence: str = Field(min_length=1)
    explanation_offered: str
    explanation_credibility: Literal["CREDIBLE", "WEAK", "NONE"]


class FinancialRedFlagsOutputs(SemanticLaneTypedOutputs):
    producer_authored_fields: ClassVar[tuple[str, ...]] = ("assessment_state",)

    assessment_state: RiskAssessmentState
    verdict_summary: JsonValue
    flag_table: tuple[FinancialRedFlag, ...] = ()
    invalidation_triggers: tuple[JsonValue, ...] = ()
    data_gaps: tuple[JsonValue, ...] = ()
    risks: tuple[JsonValue, ...] = ()
    citation_degradation_warnings: tuple[JsonValue, ...] = ()


class CompetitivePositionOutputs(SemanticLaneTypedOutputs):
    industry_diligence: IndustryDiligencePack

    @model_validator(mode="before")
    @classmethod
    def _require_explicit_industry_version(cls, value: object) -> object:
        if (
            not isinstance(value, dict)
            or not isinstance(value.get("industry_diligence"), dict)
            or "schema_version" not in value["industry_diligence"]
        ):
            raise ValueError("industry_diligence.schema_version is required")
        return value


OutputsT = TypeVar(
    "OutputsT",
    bound=SemanticLaneTypedOutputs,
    covariant=True,
)
ContractNameT = TypeVar("ContractNameT", bound=str, covariant=True)
SkillT = TypeVar("SkillT", bound=str, covariant=True)
PatchOpsT = TypeVar(
    "PatchOpsT",
    bound=tuple[JsonValue, ...] | None,
    covariant=True,
)
DecisionsLogT = TypeVar(
    "DecisionsLogT",
    bound=tuple[JsonValue, ...] | None,
    covariant=True,
)


class SemanticLaneEnvelope(
    _StrictContract,
    Generic[
        OutputsT,
        ContractNameT,
        SkillT,
        PatchOpsT,
        DecisionsLogT,
    ],
):
    artifact_id: str = Field(min_length=1)
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    contract_name: ContractNameT
    control_run_id: str | None = Field(default=None, min_length=1)
    created_at: str = Field(min_length=1)
    data_source: Literal["live", "fixture"]
    provenance: dict[str, JsonValue]
    research_file_id: int | None = None
    schema_version: Literal["2.0"] = "2.0"
    skill: SkillT = Field(min_length=1)
    skill_run_id: str = Field(min_length=1)
    source_path: str | None = Field(default=None, min_length=1)
    ticker: str = Field(pattern=r"^[A-Z0-9.-]+$")
    typed_outputs: OutputsT
    user_id: str | None = None
    verdict: dict[str, JsonValue]
    patch_ops: PatchOpsT = Field(default=None, validate_default=True)
    decisions_log: DecisionsLogT = Field(default=None, validate_default=True)
    citation_degradation_warnings: tuple[JsonValue, ...] | None = None
    dashboard_ref: JsonValue | None = None
    binary_artifact_path: str | None = Field(default=None, min_length=1)
    proposal_id: str | None = Field(default=None, min_length=1)
    proposal_expires_at: float | None = None
    staged_proposal: JsonValue | None = None
    next_actions: tuple[JsonValue, ...] | None = None
    lineage: JsonValue | None = None
    origin_kind: str | None = Field(default=None, min_length=1)
    visibility: str | None = Field(default=None, min_length=1)
    origin_ref: JsonValue | None = None

    @model_validator(mode="before")
    @classmethod
    def _validate_content_hash(cls, value: object) -> object:
        if not isinstance(value, dict):
            raise ValueError("semantic lane envelope must be an object")
        expected = value.get("content_sha256")
        body = dict(value)
        body.pop("content_sha256", None)
        actual = hashlib.sha256(
            json.dumps(body, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
        if expected != actual:
            raise ValueError("semantic lane content_sha256 mismatch")
        return value

    @model_validator(mode="after")
    def _validate_identity_and_capabilities(
        self,
    ) -> SemanticLaneEnvelope[
        OutputsT,
        ContractNameT,
        SkillT,
        PatchOpsT,
        DecisionsLogT,
    ]:
        if self.contract_name != self.skill:
            raise ValueError("semantic lane contract_name/skill mismatch")
        try:
            parsed = datetime.fromisoformat(self.created_at.replace("Z", "+00:00"))
        except ValueError as exc:
            raise ValueError("semantic lane created_at must be ISO-8601") from exc
        if parsed.utcoffset() is None:
            raise ValueError("semantic lane created_at must include an offset")
        expected = SEMANTIC_LANE_CAPABILITIES_BY_CONTRACT[self.contract_name]
        declared = frozenset(
            item.capability
            for item in self.typed_outputs.initiation_capability_evidence
        )
        if declared != expected:
            raise ValueError(
                "semantic lane capability declarations must exactly match contract"
            )
        evaluate_registered_semantic_invariants(
            self.model_dump(mode="json"),
            contract_name=self.contract_name,
            schema_version=self.schema_version,
        )
        return self


class _OptionalSemanticLaneEnvelope(
    SemanticLaneEnvelope[
        OutputsT,
        ContractNameT,
        SkillT,
        tuple[JsonValue, ...] | None,
        tuple[JsonValue, ...] | None,
    ],
    Generic[OutputsT, ContractNameT, SkillT],
):
    patch_ops: tuple[JsonValue, ...] | None = None
    decisions_log: tuple[JsonValue, ...] | None = None


class PositionInitiationPredecisionEnvelope(
    _OptionalSemanticLaneEnvelope[
        PositionInitiationPredecisionOutputs,
        Literal["position-initiation-predecision"],
        Literal["position-initiation-predecision"],
    ]
):
    contract_name: Literal[
        "position-initiation-predecision"
    ] = "position-initiation-predecision"
    skill: Literal[
        "position-initiation-predecision"
    ] = "position-initiation-predecision"


class ThesisArticulationEnvelope(
    _OptionalSemanticLaneEnvelope[
        ThesisArticulationOutputs,
        Literal["thesis-articulation"],
        Literal["thesis-articulation"],
    ]
):
    contract_name: Literal["thesis-articulation"] = "thesis-articulation"
    skill: Literal["thesis-articulation"] = "thesis-articulation"


class ComparativeAnalysisEnvelope(
    SemanticLaneEnvelope[
        ComparativeAnalysisOutputs,
        Literal["comparative-analysis"],
        Literal["comparative-analysis"],
        tuple[JsonValue, ...],
        tuple[JsonValue, ...],
    ]
):
    contract_name: Literal["comparative-analysis"] = "comparative-analysis"
    skill: Literal["comparative-analysis"] = "comparative-analysis"
    patch_ops: tuple[JsonValue, ...] = Field(
        default=PydanticUndefined,
        validate_default=True,
    )
    decisions_log: tuple[JsonValue, ...] = Field(
        default=PydanticUndefined,
        validate_default=True,
    )


class GuidanceExtractionEnvelope(
    _OptionalSemanticLaneEnvelope[
        GuidanceExtractionOutputs,
        Literal["guidance-extraction"],
        Literal["guidance-extraction"],
    ]
):
    contract_name: Literal["guidance-extraction"] = "guidance-extraction"
    skill: Literal["guidance-extraction"] = "guidance-extraction"


class ModelVsConsensusEnvelope(
    _OptionalSemanticLaneEnvelope[
        ModelVsConsensusOutputs,
        Literal["model-vs-consensus"],
        Literal["model-vs-consensus"],
    ]
):
    contract_name: Literal["model-vs-consensus"] = "model-vs-consensus"
    skill: Literal["model-vs-consensus"] = "model-vs-consensus"


class DcfRelativeValuationEnvelope(
    _OptionalSemanticLaneEnvelope[
        DcfRelativeValuationOutputs,
        Literal["dcf-relative-valuation"],
        Literal["dcf-relative-valuation"],
    ]
):
    contract_name: Literal["dcf-relative-valuation"] = "dcf-relative-valuation"
    skill: Literal["dcf-relative-valuation"] = "dcf-relative-valuation"


class ScenarioMultiplePricingEnvelope(
    _OptionalSemanticLaneEnvelope[
        ScenarioMultiplePricingOutputs,
        Literal["scenario-multiple-pricing"],
        Literal["scenario-multiple-pricing"],
    ]
):
    contract_name: Literal[
        "scenario-multiple-pricing"
    ] = "scenario-multiple-pricing"
    skill: Literal["scenario-multiple-pricing"] = "scenario-multiple-pricing"

    @model_validator(mode="after")
    def _require_success_target_horizons(
        self,
    ) -> "ScenarioMultiplePricingEnvelope":
        if (
            not self.typed_outputs.scenario_target_horizons
            and self.verdict.get("verdict")
            not in {"INSUFFICIENT_DATA", "VALUATION_DIVERGED"}
        ):
            raise ValueError(
                "scenario_target_horizons may be omitted only for an explicit "
                "INSUFFICIENT_DATA or VALUATION_DIVERGED verdict"
            )
        return self


class ExpectedValueDecisionEnvelope(
    _OptionalSemanticLaneEnvelope[
        ExpectedValueDecisionOutputs,
        Literal["expected-value-decision"],
        Literal["expected-value-decision"],
    ]
):
    contract_name: Literal["expected-value-decision"] = "expected-value-decision"
    skill: Literal["expected-value-decision"] = "expected-value-decision"

    @model_validator(mode="after")
    def _require_success_decision_outputs(
        self,
    ) -> "ExpectedValueDecisionEnvelope":
        missing = [
            field_name
            for field_name in ("scenario_probabilities", "expected_value")
            if getattr(self.typed_outputs, field_name) is None
        ]
        if missing and self.verdict.get("verdict") != "INSUFFICIENT_DATA":
            raise ValueError(
                "expected-value decision outputs may be omitted only for an "
                f"explicit INSUFFICIENT_DATA verdict; missing={missing}"
            )
        return self


class IdentifyingRiskEnvelope(
    _OptionalSemanticLaneEnvelope[
        IdentifyingRiskOutputs,
        Literal["identifying-risk"],
        Literal["identifying-risk"],
    ]
):
    contract_name: Literal["identifying-risk"] = "identifying-risk"
    skill: Literal["identifying-risk"] = "identifying-risk"


class FinancialRedFlagsEnvelope(
    _OptionalSemanticLaneEnvelope[
        FinancialRedFlagsOutputs,
        Literal["financial-red-flags"],
        Literal["financial-red-flags"],
    ]
):
    contract_name: Literal["financial-red-flags"] = "financial-red-flags"
    skill: Literal["financial-red-flags"] = "financial-red-flags"


class CompetitivePositionEnvelope(
    _OptionalSemanticLaneEnvelope[
        CompetitivePositionOutputs,
        Literal["competitive-position"],
        Literal["competitive-position"],
    ]
):
    contract_name: Literal["competitive-position"] = "competitive-position"
    skill: Literal["competitive-position"] = "competitive-position"


SEMANTIC_LANE_CAPABILITIES_BY_CONTRACT: Mapping[
    str, frozenset[InitiationEvidenceCapability]
] = MappingProxyType({
    "position-initiation-predecision": frozenset({"position_predecision"}),
    "thesis-articulation": frozenset({"thesis_articulation"}),
    "comparative-analysis": frozenset(
        {"competitive_comparison", "operating_history"}
    ),
    "guidance-extraction": frozenset({"management_guidance"}),
    "model-vs-consensus": frozenset({"consensus_comparison"}),
    "dcf-relative-valuation": frozenset({"valuation"}),
    "scenario-multiple-pricing": frozenset({"valuation"}),
    "expected-value-decision": frozenset({"valuation"}),
    "identifying-risk": frozenset({"risk_assessment"}),
    "financial-red-flags": frozenset({"financial_red_flags"}),
    "competitive-position": frozenset({"industry_diligence"}),
})


def semantic_lane_capabilities(
    contract_name: str,
) -> tuple[InitiationEvidenceCapability, ...]:
    """Return the capabilities this contract publishes, sorted ascending."""

    declared = SEMANTIC_LANE_CAPABILITIES_BY_CONTRACT.get(contract_name)
    if declared is None:
        raise ValueError(f"unknown semantic lane contract: {contract_name!r}")
    return tuple(sorted(declared))


def semantic_lane_capability_example(contract_name: str) -> list[dict[str, str]]:
    """Return one exemplar declaration per capability this contract publishes.

    The single place a producer-facing example of `initiation_capability_evidence`
    is written. Every refusal and every advertised judgment schema renders this
    contract's own capabilities, never another lane's.
    """

    return [
        {
            "capability": capability,
            "research_status": "completed",
            "evidence_status": "available",
            "rationale": "The registered evidence supports this capability.",
            "source_id": "src_1",
            "producer_locator": "edgar:0000000000-26-000001",
        }
        for capability in semantic_lane_capabilities(contract_name)
    ]


def _inline_local_refs(node: JsonValue, defs: Mapping[str, JsonValue]) -> JsonValue:
    """Copy `node` with every `#/$defs/<name>` reference replaced by its definition.

    A property spliced into a tool's input schema no longer sits under the
    outputs model's `$defs`, so a reference left in it would dangle.
    """

    if isinstance(node, list):
        return [_inline_local_refs(item, defs) for item in node]
    if not isinstance(node, dict):
        return node
    inlined: dict[str, JsonValue] = {}
    ref = node.get("$ref")
    if isinstance(ref, str):
        definition = _inline_local_refs(defs[ref.removeprefix("#/$defs/")], defs)
        assert isinstance(definition, dict)
        inlined.update(definition)
    for key, value in node.items():
        if key != "$ref":
            inlined[key] = _inline_local_refs(value, defs)
    return inlined


def semantic_lane_publication_properties(
    contract_name: str,
) -> dict[str, JsonValue]:
    """Return the JSON-Schema properties a producer of this contract must declare.

    Derived from the lane's registered capability set, from
    `SemanticLaneCapabilityEvidence`, and from the lane's outputs model — its
    `producer_authored_fields` and the closure rule its validator enforces — so
    an agent-facing tool advertises exactly what the publication door validates.
    Every returned property is required.
    """

    capabilities = semantic_lane_capabilities(contract_name)
    outputs_model = (
        SEMANTIC_LANE_ENVELOPE_BY_CONTRACT[contract_name]
        .model_fields["typed_outputs"]
        .annotation
    )
    assert outputs_model is not None  # every lane envelope binds its outputs model
    outputs_schema = outputs_model.model_json_schema()
    outputs_properties = outputs_schema["properties"]
    outputs_defs = outputs_schema.get("$defs", {})
    requires_source: list[JsonValue] = [
        status for status in sorted(EVIDENCE_STATUSES_REQUIRING_SOURCE)
    ]
    capability_enum: list[JsonValue] = list(capabilities)
    research_status_enum: list[JsonValue] = list(get_args(ResearchStatus))
    evidence_status_enum: list[JsonValue] = list(get_args(EvidenceStatus))
    properties: dict[str, JsonValue] = {
        "source_refs": {
            "type": "array",
            "description": (
                "Sorted, unique ids of the sources this judgment registered and "
                "this publication rests on; declare [] when none."
            ),
            "items": {"type": "string", "minLength": 1},
            "uniqueItems": True,
        },
        "initiation_capability_evidence": {
            "type": "array",
            "description": (
                f"Exactly one honest coverage declaration per capability this "
                f"contract publishes: {', '.join(capabilities)}. Evidence statuses "
                f"{', '.join(sorted(EVIDENCE_STATUSES_REQUIRING_SOURCE))} require "
                "source_id and producer_locator; the others may omit both, but "
                "never declare one without the other. "
                + SEMANTIC_LANE_CAPABILITY_SOURCE_CLOSURE_RULE
            ),
            "minItems": len(capabilities),
            "maxItems": len(capabilities),
            "items": {
                "type": "object",
                "properties": {
                    "capability": {"type": "string", "enum": capability_enum},
                    "research_status": {
                        "type": "string",
                        "enum": research_status_enum,
                    },
                    "evidence_status": {
                        "type": "string",
                        "enum": evidence_status_enum,
                    },
                    "rationale": {"type": "string", "minLength": 1},
                    "source_id": {
                        "type": "string",
                        "minLength": 1,
                        "description": SEMANTIC_LANE_CAPABILITY_SOURCE_CLOSURE_RULE,
                    },
                    "producer_locator": {"type": "string", "minLength": 1},
                },
                "required": [
                    "capability",
                    "research_status",
                    "evidence_status",
                    "rationale",
                ],
                "allOf": [
                    {
                        "if": {
                            "properties": {
                                "evidence_status": {"enum": requires_source}
                            },
                            "required": ["evidence_status"],
                        },
                        "then": {"required": ["source_id", "producer_locator"]},
                    },
                    {
                        "if": {"required": ["source_id"]},
                        "then": {"required": ["producer_locator"]},
                    },
                    {
                        "if": {"required": ["producer_locator"]},
                        "then": {"required": ["source_id"]},
                    },
                ],
                "additionalProperties": False,
            },
        },
    }
    for name in outputs_model.producer_authored_fields:
        field_schema = _inline_local_refs(outputs_properties[name], outputs_defs)
        assert isinstance(field_schema, dict)
        field_schema.pop("title", None)
        properties[name] = field_schema
    return properties


SEMANTIC_LANE_ENVELOPE_BY_CONTRACT: dict[
    str,
    type[
        SemanticLaneEnvelope[
            SemanticLaneTypedOutputs,
            str,
            str,
            tuple[JsonValue, ...] | None,
            tuple[JsonValue, ...] | None,
        ]
    ],
] = {
    "position-initiation-predecision": PositionInitiationPredecisionEnvelope,
    "thesis-articulation": ThesisArticulationEnvelope,
    "comparative-analysis": ComparativeAnalysisEnvelope,
    "guidance-extraction": GuidanceExtractionEnvelope,
    "model-vs-consensus": ModelVsConsensusEnvelope,
    "dcf-relative-valuation": DcfRelativeValuationEnvelope,
    "scenario-multiple-pricing": ScenarioMultiplePricingEnvelope,
    "expected-value-decision": ExpectedValueDecisionEnvelope,
    "identifying-risk": IdentifyingRiskEnvelope,
    "financial-red-flags": FinancialRedFlagsEnvelope,
    "competitive-position": CompetitivePositionEnvelope,
}


__all__ = [
    "EVIDENCE_STATUSES_REQUIRING_SOURCE",
    "SEMANTIC_LANE_CAPABILITY_SOURCE_CLOSURE_RULE",
    "SEMANTIC_LANE_CAPABILITIES_BY_CONTRACT",
    "SEMANTIC_LANE_ENVELOPE_BY_CONTRACT",
    "FinancialRedFlag",
    "SemanticLaneCapabilityEvidence",
    "SemanticLaneEnvelope",
    "SemanticLaneSource",
    "semantic_lane_capabilities",
    "semantic_lane_capability_example",
    "semantic_lane_publication_properties",
]
