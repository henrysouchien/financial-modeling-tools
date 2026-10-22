"""Version 5 evidence-capability contract for the initiation dossier."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from math import isfinite
from typing import Any, Literal, TypeAlias

from pydantic import Field, field_validator, model_validator

from .company_diligence import CompanyDiligence
from .industry_diligence import IndustryDiligencePack
from .initiation_evidence import (
    AttributedClaim,
    AttributedValue,
    CapabilityBinding,
    InitiationSectionId,
    SECTION_DECISION_RELEVANCE,
    SECTION_IDS,
    SECTION_REQUIREMENTS,
    SectionCoverage,
    section_degrades_verdict,
)
from .numeric_display import NumericDisplaySpec
from .report_model_forecast import ReportModelForecastPack
from .semantic_invariants import evaluate_registered_semantic_invariants
from .thesis_shared_slice import DataGap, _ContractModel
from .versioned_artifact import VersionedArtifactRef
from .deliverable_binding import DeliverableModelBinding


InitiationDossierVerdict = Literal[
    "INITIATION_DOSSIER_COMPILED",
    "INITIATION_DOSSIER_PARTIALLY_COMPILED",
    "INSUFFICIENT_ARTIFACTS",
    "INSUFFICIENT_DATA",
]
InitiationDossierConfidence = Literal["HIGH", "MEDIUM", "LOW"]
CompetitiveDecisionStatus = Literal[
    "STRATEGY_WINNING",
    "STRATEGY_TRACKING",
    "STRATEGY_DRIFTING",
    "STRATEGY_FAILING",
]
InitiationScalar: TypeAlias = str | int | float
# The sizing layers the producer computed, cumulatively: ``standalone`` is layers 1-2 (standalone size
# bounded by the stock's own exposure caps); ``portfolio_adjusted`` is layers 1-3 (that size adjusted
# against the bound portfolio). The standalone layer is never absent; neither value states a binding.
PositionSizingBasis = Literal["standalone", "portfolio_adjusted"]
CIRCULATING_VERDICTS = frozenset(
    {"INITIATION_DOSSIER_COMPILED", "INITIATION_DOSSIER_PARTIALLY_COMPILED"}
)


class InitiationSourceRecord(_ContractModel):
    source_id: str = Field(min_length=1)
    provider: str | None = Field(default=None, min_length=1)
    source_identity: str = Field(min_length=1)
    text: str | None = Field(default=None, min_length=1)


class InitiationClaimRecord(_ContractModel):
    claim_id: str = Field(min_length=1)
    evidence_source_ids: list[str] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique_sources(self) -> "InitiationClaimRecord":
        if len(self.evidence_source_ids) != len(set(self.evidence_source_ids)):
            raise ValueError("claim evidence source ids must be unique")
        return self


class InitiationDossierSourceArtifact(_ContractModel):
    artifact_ref: VersionedArtifactRef
    skill: str = Field(min_length=1)
    artifact_id: str = Field(min_length=1)
    ticker: str = Field(min_length=1)
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")
    contract_name: str = Field(min_length=1)
    schema_version: str = Field(min_length=1)
    created_at: str = Field(min_length=1)
    verdict: str = Field(min_length=1)
    confidence: str | float | None = None
    source_registry: list[InitiationSourceRecord] = Field(min_length=1)
    claim_registry: list[InitiationClaimRecord] = Field(default_factory=list)
    freshness: Literal["current", "stale"] = "current"

    @field_validator("ticker")
    @classmethod
    def _uppercase_ticker(cls, value: str) -> str:
        return value.upper()

    @field_validator("confidence")
    @classmethod
    def _valid_confidence(cls, value: str | float | None) -> str | float | None:
        if isinstance(value, float) and (not isfinite(value) or not 0 <= value <= 1):
            raise ValueError(
                "numeric confidence must be finite and between zero and one"
            )
        return value

    @model_validator(mode="after")
    def _validate_registries(self) -> "InitiationDossierSourceArtifact":
        if (
            self.artifact_ref.artifact_id != self.artifact_id
            or self.artifact_ref.content_sha256 != self.content_sha256
            or self.artifact_ref.payload_schema_version != self.schema_version
        ):
            raise ValueError(
                "source artifact identity must match its complete artifact_ref"
            )
        source_ids = [source.source_id for source in self.source_registry]
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("artifact source_registry source ids must be unique")
        claims = [claim.claim_id for claim in self.claim_registry]
        if len(claims) != len(set(claims)):
            raise ValueError("artifact claim_registry claim ids must be unique")
        known_sources = set(source_ids)
        for claim in self.claim_registry:
            if not set(claim.evidence_source_ids) <= known_sources:
                raise ValueError(
                    "claim evidence must resolve inside artifact source_registry"
                )
        return self


# The dossier source key (its ``skill``) of a compiled ModelVersion's scenarios.
COMPILED_SCENARIOS_SOURCE = "compiled-scenarios"


class CompiledScenariosDossierSourceRef(_ContractModel):
    """The ModelVersion that compiled an accepted ScenarioSet's scenario EPS.

    The ``earnings-scenarios`` stage persists the ScenarioSet and ``build-model``
    compiles it; that ModelVersion's ModelSpec names the set it compiled
    (``scenario_set_binding``), so the ModelVersion alone identifies both. Its
    ``artifact_id`` is ``<model_id>-v<model_version>``, the id capability
    bindings name; the dossier door builds this ref from that id.
    """

    ref_kind: Literal["compiled_scenarios"] = "compiled_scenarios"
    model_id: str = Field(min_length=1)
    model_version: int = Field(gt=0)

    @property
    def artifact_id(self) -> str:
        return f"{self.model_id}-v{self.model_version}"


class InitiationDossierCompileRequest(_ContractModel):
    """The door-constructed exact-source request; the compiler alone derives judgment.

    The door builds the manifest from the sources it read and keeps only the
    bindings an adapter proved, so either list may be empty when nothing the
    judgment named could be read; the pack then withholds the dossier.
    """

    contract_name: Literal["initiation-dossier"] = "initiation-dossier"
    schema_version: Literal["5.0"] = "5.0"
    ticker: str = Field(min_length=1)
    source_manifest: list[InitiationDossierSourceArtifact] = Field(default_factory=list)
    capability_bindings: list[CapabilityBinding] = Field(default_factory=list)
    model_binding: DeliverableModelBinding | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )

    @field_validator("ticker")
    @classmethod
    def _uppercase_request_ticker(cls, value: str) -> str:
        return value.upper()

    @model_validator(mode="after")
    def _validate_request(self) -> "InitiationDossierCompileRequest":
        _validate_manifest_and_bindings(
            ticker=self.ticker,
            manifest=self.source_manifest,
            bindings=self.capability_bindings,
        )
        return self


class InitiationDossierPositionPlan(_ContractModel):
    """The work-up's position plan, labelled with the sizing basis its producer declared.

    A ``standalone`` size is Kelly / proportional / equal-weight sizing bounded by
    the stock's own exposure caps; it never carries a portfolio-fit statement.
    """

    sizing_basis: AttributedValue[PositionSizingBasis]
    sizing_method: AttributedValue[str]
    target_size_pct: AttributedValue[Decimal]
    max_size_pct: AttributedValue[Decimal] | None = None
    summary: AttributedClaim
    portfolio_fit: AttributedClaim | None = None


class InitiationDossierInvestmentConclusion(_ContractModel):
    recommendation: AttributedValue[str]
    summary: AttributedClaim
    position_plan: InitiationDossierPositionPlan
    action: AttributedValue[str] | None = None
    conviction: AttributedValue[int] | None = None

    @model_validator(mode="after")
    def _validate_conviction(self) -> "InitiationDossierInvestmentConclusion":
        if self.conviction is not None and not 1 <= self.conviction.value <= 5:
            raise ValueError("conviction must be between 1 and 5")
        return self


class InitiationDossierBottomLine(_ContractModel):
    thesis_call: AttributedValue[str]
    summary: AttributedClaim
    predecision_call: AttributedValue[str] | None = None


class InitiationDossierPeerMetric(_ContractModel):
    metric_id: str = Field(min_length=1)
    display_label: str = Field(min_length=1)
    display: NumericDisplaySpec
    value: AttributedValue[str | Decimal]


class InitiationDossierPeer(_ContractModel):
    ticker: str = Field(min_length=1)
    company_name: str = Field(min_length=1)
    is_focal: bool = False
    metrics: list[InitiationDossierPeerMetric] = Field(default_factory=list)
    relative_position: AttributedClaim | None = None

    @field_validator("ticker")
    @classmethod
    def _uppercase_peer_ticker(cls, value: str) -> str:
        return value.upper()


class InitiationDossierComparison(_ContractModel):
    verdict: AttributedValue[str]
    industry_context: AttributedClaim
    peer_set_rationale: AttributedClaim
    decision_status: AttributedValue[CompetitiveDecisionStatus]
    decision_context: AttributedClaim
    peers: list[InitiationDossierPeer] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_peers(self) -> "InitiationDossierComparison":
        tickers = [peer.ticker for peer in self.peers]
        if len(tickers) != len(set(tickers)):
            raise ValueError("comparison peer tickers must be unique")
        if sum(peer.is_focal for peer in self.peers) != 1:
            raise ValueError("comparison requires exactly one focal company")
        return self


class InitiationDossierOperatingMetric(_ContractModel):
    metric_id: str = Field(min_length=1)
    display_label: str = Field(min_length=1)
    display: NumericDisplaySpec
    series: dict[str, dict[int, AttributedValue[Decimal]]] = Field(default_factory=dict)


class InitiationDossierOperatingObservation(_ContractModel):
    observation_id: str = Field(min_length=1)
    display_label: str = Field(min_length=1)
    assessment: AttributedClaim


class InitiationDossierOperatingHistory(_ContractModel):
    as_of: date
    metrics: list[InitiationDossierOperatingMetric] = Field(min_length=1)
    observations: list[InitiationDossierOperatingObservation] = Field(
        default_factory=list
    )


class InitiationDossierCatalyst(_ContractModel):
    catalyst_id: str = Field(min_length=1)
    description: AttributedClaim
    expected_date: AttributedValue[str] | None = None
    expected_revision: AttributedClaim | None = None
    severity: AttributedValue[str]


class InitiationDossierThesis(_ContractModel):
    source_date: date
    recommendation: AttributedValue[str]
    one_sentence: AttributedClaim
    elevator_90_seconds: AttributedClaim | None = None
    case_type: AttributedValue[str] | None = None
    asymmetry_ratio: AttributedValue[Decimal] | None = None
    differentiated_view: list[AttributedClaim] = Field(default_factory=list)
    catalysts: list[InitiationDossierCatalyst] = Field(default_factory=list)
    recommended_next_action: AttributedValue[str] | None = None


class InitiationDossierFinancialFlag(_ContractModel):
    flag_id: str = Field(min_length=1)
    pillar: str = Field(min_length=1)
    severity: AttributedValue[str]
    finding: AttributedClaim
    threshold: AttributedValue[str] | None = None


class InitiationDossierRiskSection(_ContractModel):
    assessment_state: Literal[
        "risks_identified", "no_material_risks_identified", "partial", "conflicting"
    ]
    summary: AttributedClaim
    risks: list[AttributedClaim] = Field(default_factory=list)
    financial_flags: list[InitiationDossierFinancialFlag] = Field(default_factory=list)
    monitoring_triggers: list[AttributedClaim] = Field(default_factory=list)
    data_gaps: list[DataGap] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_assessment_state(self) -> "InitiationDossierRiskSection":
        if self.assessment_state == "no_material_risks_identified" and (
            self.risks or self.financial_flags
        ):
            raise ValueError(
                "no_material_risks_identified prohibits risk and financial-flag rows"
            )
        return self


class InitiationDossierReadinessGap(_ContractModel):
    gap_id: str = Field(min_length=1)
    description: str = Field(min_length=1)
    owner: str | None = None


class InitiationDossierPack(_ContractModel):
    contract_name: Literal["initiation-dossier"] = "initiation-dossier"
    schema_version: Literal["5.0"] = "5.0"
    ticker: str = Field(min_length=1)
    company_name: str = Field(min_length=1)
    industry_label: str | None = Field(default=None, min_length=1)
    verdict: InitiationDossierVerdict
    confidence: InitiationDossierConfidence
    as_of: date
    subtitle: str = Field(min_length=1)
    section_coverage: dict[InitiationSectionId, SectionCoverage] = Field(
        default_factory=dict
    )
    investment_conclusion: InitiationDossierInvestmentConclusion | None = None
    bottom_line: InitiationDossierBottomLine | None = None
    key_considerations: list[AttributedClaim] = Field(default_factory=list)
    company_diligence: CompanyDiligence | None = None
    industry_diligence: IndustryDiligencePack | None = None
    competitive_comparison: InitiationDossierComparison | None = None
    operating_history: InitiationDossierOperatingHistory | None = None
    report_model_forecast: ReportModelForecastPack | None = None
    thesis: InitiationDossierThesis | None = None
    risks: InitiationDossierRiskSection | None = None
    human_follow_up: list[AttributedClaim] = Field(default_factory=list)
    readiness_gaps: list[InitiationDossierReadinessGap] = Field(default_factory=list)
    data_gaps: list[DataGap] = Field(default_factory=list)
    source_manifest: list[InitiationDossierSourceArtifact] = Field(default_factory=list)
    capability_bindings: list[CapabilityBinding] = Field(default_factory=list)
    #: The ModelVersion this dossier was cut from and the Question it answers
    #: (W5d); absent on every dossier persisted before it. Reading a dossier
    #: whose research file's pointer moved past it reports
    #: ``deliverable_version_stale``.
    model_binding: DeliverableModelBinding | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    recommended_next_action: str = Field(min_length=1)

    @field_validator("ticker")
    @classmethod
    def _uppercase_ticker(cls, value: str) -> str:
        return value.upper()

    @model_validator(mode="after")
    def _validate_pack(self) -> "InitiationDossierPack":
        if self.verdict not in CIRCULATING_VERDICTS:
            if not self.data_gaps:
                raise ValueError("refusal dossier requires data_gaps")
            return self

        _validate_manifest_and_bindings(
            ticker=self.ticker,
            manifest=self.source_manifest,
            bindings=self.capability_bindings,
        )
        if set(self.section_coverage) != set(SECTION_IDS):
            raise ValueError(
                "circulating dossier requires the exact section_coverage map"
            )
        for section_id, coverage in self.section_coverage.items():
            expected_relevance = SECTION_DECISION_RELEVANCE[section_id]
            if coverage.decision_relevance != expected_relevance:
                raise ValueError(
                    f"{section_id} decision_relevance must be {expected_relevance}"
                )
        self._validate_coverage_bindings_and_refs()
        self._validate_payload_coverage()
        self._validate_derived_verdict()
        if self.industry_diligence and self.industry_diligence.ticker != self.ticker:
            raise ValueError("industry diligence ticker must match dossier")
        if (
            self.report_model_forecast
            and self.report_model_forecast.ticker != self.ticker
        ):
            raise ValueError("forecast ticker must match dossier")
        self._validate_forecast_sources_in_manifest()
        if self.competitive_comparison:
            focal = next(
                peer for peer in self.competitive_comparison.peers if peer.is_focal
            )
            if focal.ticker != self.ticker:
                raise ValueError(
                    "competitive comparison focal ticker must match dossier"
                )
        self._validate_all_qualified_refs()
        evaluate_registered_semantic_invariants(
            self.model_dump(mode="json"),
            contract_name=self.contract_name,
            schema_version=self.schema_version,
        )
        return self

    def _payload_state(self, section_id: InitiationSectionId) -> tuple[bool, bool]:
        forecast = self.report_model_forecast
        mapping: dict[InitiationSectionId, tuple[bool, bool]] = {
            "investment_conclusion": (
                self.investment_conclusion is not None,
                self.investment_conclusion is not None,
            ),
            "investment_considerations": (
                self.bottom_line is not None or bool(self.key_considerations),
                self.bottom_line is not None and bool(self.key_considerations),
            ),
            "company": (
                self.company_diligence is not None,
                self.company_diligence is not None,
            ),
            "industry": (
                self.industry_diligence is not None,
                self.industry_diligence is not None,
            ),
            "competitive_comparison": (
                self.competitive_comparison is not None,
                self.competitive_comparison is not None,
            ),
            "operating_history": (
                self.operating_history is not None,
                self.operating_history is not None,
            ),
            "forecast_guidance": (
                forecast is not None and forecast.guidance is not None,
                forecast is not None and forecast.guidance is not None,
            ),
            "forecast_consensus": (
                forecast is not None and forecast.consensus is not None,
                forecast is not None and forecast.consensus is not None,
            ),
            "forecast_scenarios": (
                forecast is not None and forecast.scenarios is not None,
                forecast is not None and forecast.scenarios is not None,
            ),
            "valuation": (
                forecast is not None and forecast.valuation is not None,
                forecast is not None and forecast.valuation is not None,
            ),
            "expected_value": (
                forecast is not None and forecast.expected_value is not None,
                forecast is not None and forecast.expected_value is not None,
            ),
            "thesis": (self.thesis is not None, self.thesis is not None),
            "risks": (
                self.risks is not None or bool(self.human_follow_up),
                self.risks is not None,
            ),
        }
        return mapping[section_id]

    def _validate_payload_coverage(self) -> None:
        for section_id, coverage in self.section_coverage.items():
            has_payload, complete_payload = self._payload_state(section_id)
            if coverage.evidence_status == "available" and not complete_payload:
                raise ValueError(
                    f"available {section_id} coverage requires its payload"
                )
            if (
                coverage.evidence_status in {"partial", "conflicting"}
                and not has_payload
            ):
                raise ValueError(
                    f"{coverage.evidence_status} {section_id} requires payload"
                )
            if (
                coverage.evidence_status
                in {
                    "not_disclosed",
                    "not_applicable",
                    "insufficient",
                }
                and has_payload
            ):
                raise ValueError(
                    f"{coverage.evidence_status} {section_id} prohibits substantive payload"
                )
        scenario_coverage = self.section_coverage["forecast_scenarios"]
        if scenario_coverage.evidence_status == "available":
            assert self.report_model_forecast is not None
            assert self.report_model_forecast.scenarios is not None
            scenarios = self.report_model_forecast.scenarios
            primary = next(
                row
                for row in scenarios.metrics
                if row.metric_id == scenarios.primary_ordering_metric_id
            )
            complete_primary = all(
                value is not None
                for value in (primary.bear, primary.base, primary.bull)
            )
            ordered = False
            if complete_primary:
                assert primary.bear is not None
                assert primary.base is not None
                assert primary.bull is not None
                ordered = (
                    primary.bear.value < primary.base.value < primary.bull.value
                    if scenarios.ordering_direction == "higher_is_better"
                    else primary.bear.value > primary.base.value > primary.bull.value
                )
            if (
                not scenarios.spread_passed
                or not ordered
                or any(
                    row.bear is None or row.base is None or row.bull is None
                    for row in scenarios.metrics
                )
            ):
                raise ValueError(
                    "available forecast scenarios require complete ordered cases and a passing spread"
                )
        expected_value_coverage = self.section_coverage["expected_value"]
        if expected_value_coverage.evidence_status == "available" and any(
            self.section_coverage[dependency].evidence_status != "available"
            for dependency in ("forecast_scenarios", "valuation")
        ):
            raise ValueError(
                "available expected value requires available scenarios and valuation"
            )

    def _validate_coverage_bindings_and_refs(self) -> None:
        binding_map = {
            binding.binding_id: binding for binding in self.capability_bindings
        }
        manifest_ids = {artifact.artifact_id for artifact in self.source_manifest}
        for section_id, coverage in self.section_coverage.items():
            unknown = set(coverage.binding_ids) - set(binding_map)
            if unknown:
                raise ValueError(
                    f"coverage references unknown binding ids: {sorted(unknown)}"
                )
            selected = [binding_map[binding_id] for binding_id in coverage.binding_ids]
            if any(binding.section_id != section_id for binding in selected):
                raise ValueError("coverage binding must authorize the same section")
            selected_artifacts = {
                artifact_id
                for binding in selected
                for artifact_id in binding.artifact_ids
            }
            if any(
                ref.artifact_ref.artifact_id not in selected_artifacts
                for ref in coverage.evidence_refs
            ):
                raise ValueError(
                    "coverage evidence ref must come from a selected binding"
                )
            if (
                coverage.evidence_status == "available"
                and not _section_expression_satisfied(section_id, selected)
            ):
                raise ValueError(
                    f"available {section_id} lacks its capability expression"
                )
            if not selected_artifacts <= manifest_ids:
                raise ValueError(
                    "coverage binding references artifact outside manifest"
                )

    def _validate_derived_verdict(self) -> None:
        binding_map = {
            binding.binding_id: binding for binding in self.capability_bindings
        }
        withheld = [
            section_id
            for section_id, coverage in self.section_coverage.items()
            if coverage.decision_relevance == "required"
            and not _section_expression_satisfied(
                section_id,
                [binding_map[binding_id] for binding_id in coverage.binding_ids],
            )
        ]
        if withheld:
            raise ValueError(
                "circulating dossier requires every required section bound: "
                f"{withheld}"
            )
        degraded = any(
            section_degrades_verdict(section_id, coverage.evidence_status)
            for section_id, coverage in self.section_coverage.items()
        )
        expected = (
            "INITIATION_DOSSIER_PARTIALLY_COMPILED"
            if degraded
            else "INITIATION_DOSSIER_COMPILED"
        )
        if self.verdict != expected:
            raise ValueError(f"dossier verdict must be derived as {expected}")
        if degraded and self.confidence == "HIGH":
            raise ValueError(
                "partial or conflicting dossier evidence caps confidence at MEDIUM"
            )

    def _validate_forecast_sources_in_manifest(self) -> None:
        forecast = self.report_model_forecast
        if forecast is None:
            return
        manifest_ids = {artifact.artifact_id for artifact in self.source_manifest}
        source_ids = {
            projection.source.artifact_id
            for projection in (
                forecast.guidance,
                forecast.consensus,
                forecast.scenarios,
                forecast.valuation,
                forecast.expected_value,
            )
            if projection is not None
        }
        source_ids.update(
            artifact_id
            for item in forecast.reconciliation
            for artifact_id in item.source_artifact_ids
        )
        outside = source_ids - manifest_ids
        if outside:
            raise ValueError(
                "forecast source artifacts must be present in dossier manifest: "
                f"{sorted(outside)}"
            )

    def _validate_all_qualified_refs(self) -> None:
        artifacts = {
            artifact.artifact_id: artifact for artifact in self.source_manifest
        }

        def visit(value: Any, *, parent_key: str | None = None) -> None:
            if isinstance(value, dict):
                if parent_key == "evidence_refs" and "source_id" in value:
                    if value.get("ref_kind") != "circulated":
                        raise ValueError(
                            "circulating dossier evidence refs must be artifact-qualified"
                        )
                    artifact_ref = VersionedArtifactRef.model_validate(
                        value.get("artifact_ref")
                    )
                    artifact_id = artifact_ref.artifact_id
                    source_id = value["source_id"]
                    claim_id = value.get("claim_id")
                    artifact = artifacts.get(artifact_id)
                    if artifact is None:
                        raise ValueError(
                            "qualified evidence ref artifact is not in manifest"
                        )
                    if artifact_ref != artifact.artifact_ref:
                        raise ValueError(
                            "qualified evidence ref does not match manifest artifact_ref"
                        )
                    sources = {source.source_id for source in artifact.source_registry}
                    if source_id not in sources:
                        raise ValueError(
                            "qualified evidence ref source does not resolve"
                        )
                    if claim_id is not None:
                        claims = {
                            claim.claim_id: claim for claim in artifact.claim_registry
                        }
                        claim = claims.get(claim_id)
                        if claim is None or source_id not in claim.evidence_source_ids:
                            raise ValueError(
                                "qualified evidence claim must resolve and include source"
                            )
                for key, nested in value.items():
                    visit(nested, parent_key=key)
            elif isinstance(value, list):
                for nested in value:
                    visit(nested, parent_key=parent_key)

        visit(self.model_dump(mode="python", exclude_none=True))


def _validate_manifest_and_bindings(
    *,
    ticker: str,
    manifest: list[InitiationDossierSourceArtifact],
    bindings: list[CapabilityBinding],
) -> None:
    artifact_ids = [artifact.artifact_id for artifact in manifest]
    if len(artifact_ids) != len(set(artifact_ids)):
        raise ValueError("source_manifest artifact ids must be unique")
    if any(artifact.ticker != ticker for artifact in manifest):
        raise ValueError("source_manifest ticker must match dossier ticker")
    binding_ids = [binding.binding_id for binding in bindings]
    if len(binding_ids) != len(set(binding_ids)):
        raise ValueError("capability binding ids must be unique")
    known = set(artifact_ids)
    referenced = {
        artifact_id for binding in bindings for artifact_id in binding.artifact_ids
    }
    if not referenced <= known:
        raise ValueError(
            "capability binding artifact ids must resolve in source_manifest"
        )
    if referenced != known:
        raise ValueError("every source_manifest artifact must participate in a binding")


def _section_expression_satisfied(
    section_id: InitiationSectionId, bindings: list[CapabilityBinding]
) -> bool:
    capabilities = {binding.capability for binding in bindings}
    return any(
        clause <= capabilities
        for clause in SECTION_REQUIREMENTS[section_id].any_of_clauses
    )


__all__ = [
    "CIRCULATING_VERDICTS",
    "CompiledScenariosDossierSourceRef",
    "InitiationClaimRecord",
    "InitiationDossierBottomLine",
    "InitiationDossierCatalyst",
    "InitiationDossierCompileRequest",
    "InitiationDossierComparison",
    "InitiationDossierConfidence",
    "InitiationDossierFinancialFlag",
    "InitiationDossierInvestmentConclusion",
    "InitiationDossierOperatingHistory",
    "InitiationDossierOperatingMetric",
    "InitiationDossierPack",
    "InitiationDossierPeer",
    "InitiationDossierPeerMetric",
    "InitiationDossierPositionPlan",
    "PositionSizingBasis",
    "InitiationDossierReadinessGap",
    "InitiationDossierRiskSection",
    "InitiationDossierSourceArtifact",
    "InitiationDossierThesis",
    "InitiationDossierVerdict",
    "InitiationSourceRecord",
]
