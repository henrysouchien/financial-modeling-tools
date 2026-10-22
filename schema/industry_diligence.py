"""Version 2 flexible, evidence-closed industry diligence contract."""

from __future__ import annotations

from collections.abc import Iterator, Mapping, Sequence
from decimal import Decimal, InvalidOperation
import re
from typing import Literal

from pydantic import (
    BaseModel,
    Field,
    TypeAdapter,
    ValidationError,
    field_validator,
    model_validator,
)
from value_semantics_core.period_ref import PeriodRef

from .initiation_evidence import (
    AttributedClaim,
    AttributedValue,
    EstimateAlternative,
    EstimateValue,
    CirculatedEvidenceRef,
    EvidenceAssessment,
    ProducerLocalEvidenceRef,
)
from .thesis_shared_slice import DataGap, _ContractModel
from industry_slice_core.thesis_shared_slice_helpers import _normalize_ticker
from industry_slice_core.thesis_shared_sources import SourceRecord


_DECIMAL_WIRE_RE = re.compile(r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$")
_LEGACY_ESTIMATE_FIELDS = frozenset({"currency", "unit"})
_PERIOD_REF_ADAPTER = TypeAdapter(PeriodRef)


class IndustryEstimateAlternative(EstimateAlternative[PeriodRef]):
    """Industry estimate alternative with one closed typed period wire."""


class IndustryEstimateValue(
    EstimateValue[PeriodRef, IndustryEstimateAlternative]
):
    """Industry estimate value with one closed typed period wire."""


def _iter_pack_evidence_source_ids(value: object) -> Iterator[str]:
    if isinstance(value, (ProducerLocalEvidenceRef, CirculatedEvidenceRef)):
        yield value.source_id
        return
    if isinstance(value, DataGap):
        yield from value.resolution_source_refs
    if isinstance(value, BaseModel):
        for field_name in type(value).model_fields:
            yield from _iter_pack_evidence_source_ids(getattr(value, field_name))
        return
    if isinstance(value, Mapping):
        for child in value.values():
            yield from _iter_pack_evidence_source_ids(child)
        return
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        for child in value:
            yield from _iter_pack_evidence_source_ids(child)


def _require_decimal_wire_string(value: object, *, path: str) -> None:
    if value is None:
        return
    if not isinstance(value, str) or not value or value != value.strip():
        raise ValueError(f"{path} must be a nonempty JSON string")
    if _DECIMAL_WIRE_RE.fullmatch(value) is None:
        raise ValueError(f"{path} must use canonical decimal notation")
    try:
        number = Decimal(value)
    except InvalidOperation as exc:
        raise ValueError(f"{path} must be a decimal") from exc
    if not number.is_finite() or str(number) != value:
        raise ValueError(f"{path} must use canonical decimal notation")


def _validate_raw_estimate(
    value: object,
    *,
    path: str,
    typed_model: type[IndustryEstimateValue]
    | type[IndustryEstimateAlternative] = IndustryEstimateValue,
) -> None:
    if value is None or isinstance(value, typed_model):
        return
    if not isinstance(value, Mapping):
        return

    legacy_fields = sorted(_LEGACY_ESTIMATE_FIELDS.intersection(value))
    if legacy_fields:
        joined = ", ".join(legacy_fields)
        raise ValueError(f"{path} contains forbidden legacy fields: {joined}")

    if "period" in value:
        period = value["period"]
        try:
            _PERIOD_REF_ADAPTER.validate_python(period)
        except ValidationError as exc:
            raise ValueError(f"{path}.period must be a valid PeriodRef object") from exc

    for field_name in ("point", "low", "high"):
        if field_name in value:
            _require_decimal_wire_string(
                value[field_name],
                path=f"{path}.{field_name}",
            )

    alternatives = value.get("alternatives")
    if isinstance(alternatives, Sequence) and not isinstance(
        alternatives, (str, bytes, bytearray)
    ):
        for index, alternative in enumerate(alternatives):
            _validate_raw_estimate(
                alternative,
                path=f"{path}.alternatives[{index}]",
                typed_model=IndustryEstimateAlternative,
            )


IndustryProducer = Literal[
    "competitive-position",
    "industry-landscape",
    "structural-trends",
    "industry-macro-overlay",
]
IndustryVerdict = Literal[
    "ATTRACTIVE",
    "MIXED",
    "WEAK",
    "INSUFFICIENT_DATA",
    "LANDSCAPE_WRITTEN",
    "LANDSCAPE_PARTIAL",
    "STRUCTURAL_TRENDS_WRITTEN",
    "STRUCTURAL_TRENDS_PARTIAL",
    "MACRO_OVERLAY_WRITTEN",
    "MACRO_OVERLAY_PARTIAL",
]
PillarName = Literal[
    "addressable_market",
    "industry_structure",
    "competitive_forces",
    "competitive_advantage",
]


class IndustryMarketDefinitionPayload(_ContractModel):
    market_name: AttributedValue[str]
    scope: AttributedValue[str]
    geography: AttributedValue[str]
    customer_segments: list[AttributedValue[str]] = Field(default_factory=list)
    measurement_basis: AttributedValue[str]


class IndustryMarketDefinition(_ContractModel):
    coverage: EvidenceAssessment
    payload: IndustryMarketDefinitionPayload | None = None

    @model_validator(mode="after")
    def _validate_payload(self) -> "IndustryMarketDefinition":
        allows_payload = self.coverage.evidence_status in {
            "available",
            "partial",
            "conflicting",
        }
        if allows_payload != (self.payload is not None):
            raise ValueError(
                "market definition payload must match available/partial/conflicting evidence"
            )
        return self


class _IndustryNumericObservation(_ContractModel):
    coverage: EvidenceAssessment
    estimate: IndustryEstimateValue | None = None

    @model_validator(mode="before")
    @classmethod
    def _validate_quantity_wire_values(cls, value: object) -> object:
        if isinstance(value, cls) or not isinstance(value, Mapping):
            return value
        _validate_raw_estimate(value.get("estimate"), path="estimate")
        return value

    @model_validator(mode="after")
    def _validate_estimate_coverage(self) -> "_IndustryNumericObservation":
        allows_estimate = self.coverage.evidence_status in {
            "available",
            "partial",
            "conflicting",
        }
        if allows_estimate != (self.estimate is not None):
            raise ValueError(
                "numeric industry estimate must match available/partial/conflicting evidence"
            )
        return self


def _validate_percentage_fraction_estimate(
    estimate: IndustryEstimateValue | None,
    *,
    observation: str,
) -> None:
    if estimate is None:
        return

    def _validate_numeric_leaf(
        *,
        display: object,
        values: tuple[Decimal | None, Decimal | None, Decimal | None],
        path: str,
    ) -> None:
        if (
            getattr(display, "numeric_kind", None) != "percentage"
            or getattr(display, "scale", None) != "unit"
        ):
            raise ValueError(
                f"{observation} {path} must use percentage display with unit scale"
            )
        for field_name, value in zip(("point", "low", "high"), values, strict=True):
            if value is not None and not Decimal("0") <= value <= Decimal("1"):
                raise ValueError(
                    f"{observation} {path}.{field_name} must be a decimal fraction "
                    "between 0 and 1"
                )

    if estimate.estimate_kind == "conflicting":
        for index, alternative in enumerate(estimate.alternatives):
            _validate_numeric_leaf(
                display=alternative.display,
                values=(alternative.point, alternative.low, alternative.high),
                path=f"estimate.alternatives[{index}]",
            )
        return

    if estimate.estimate_kind not in {"point", "range", "bounded", "proxy"}:
        raise ValueError(
            f"{observation} estimate must be numeric or conflicting when present"
        )
    _validate_numeric_leaf(
        display=estimate.display,
        values=(estimate.point, estimate.low, estimate.high),
        path="estimate",
    )


class IndustryMarketSizeObservation(_IndustryNumericObservation):
    observation_id: str = Field(min_length=1)
    kind: Literal["market_size", "tam", "sam"]
    geography: str = Field(min_length=1)
    population: str | None = Field(default=None, min_length=1)
    denominator: str | None = Field(default=None, min_length=1)


class IndustryPenetrationObservation(_IndustryNumericObservation):
    observation_id: str = Field(min_length=1)
    population: str = Field(min_length=1)
    denominator: str = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_percentage_fraction(self) -> "IndustryPenetrationObservation":
        _validate_percentage_fraction_estimate(
            self.estimate,
            observation="industry penetration",
        )
        return self


class IndustryMarketShareObservation(_IndustryNumericObservation):
    observation_id: str = Field(min_length=1)
    company: str = Field(min_length=1)
    ticker: str | None = None
    denominator: str = Field(min_length=1)
    geography: str = Field(min_length=1)

    @field_validator("ticker", mode="before")
    @classmethod
    def _validate_optional_ticker(cls, value: object | None) -> str | None:
        return None if value is None else _normalize_ticker(value)

    @model_validator(mode="after")
    def _validate_percentage_fraction(self) -> "IndustryMarketShareObservation":
        _validate_percentage_fraction_estimate(
            self.estimate,
            observation="industry market share",
        )
        return self


class IndustryRegulatoryFactorPayload(_ContractModel):
    name: AttributedValue[str]
    jurisdiction: AttributedValue[str]
    status: AttributedValue[str]
    impact: AttributedClaim


class IndustryRegulatoryFactor(_ContractModel):
    factor_id: str = Field(min_length=1)
    coverage: EvidenceAssessment
    payload: IndustryRegulatoryFactorPayload | None = None

    @model_validator(mode="after")
    def _validate_payload(self) -> "IndustryRegulatoryFactor":
        allows_payload = self.coverage.evidence_status in {
            "available",
            "partial",
            "conflicting",
        }
        if allows_payload != (self.payload is not None):
            raise ValueError(
                "regulatory payload must match available/partial/conflicting evidence"
            )
        return self


class IndustryFundingIndicator(_IndustryNumericObservation):
    observation_id: str = Field(min_length=1)
    metric: str = Field(min_length=1)
    denominator: str | None = Field(default=None, min_length=1)


class IndustryNarrativeObservation(_ContractModel):
    observation_id: str = Field(min_length=1)
    display_label: str = Field(min_length=1)
    time_horizon: Literal["near-term", "medium-term", "long-term"] | None = None
    coverage: EvidenceAssessment
    claims: list[AttributedClaim] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_claims(self) -> "IndustryNarrativeObservation":
        allows_claims = self.coverage.evidence_status in {
            "available",
            "partial",
            "conflicting",
        }
        if allows_claims and not self.claims:
            raise ValueError("supported narrative observation requires claims")
        if not allows_claims and self.claims:
            raise ValueError("unsupported narrative observation must omit claims")
        return self


class IndustryPillarAssessment(_ContractModel):
    pillar_id: PillarName
    coverage: EvidenceAssessment
    score: AttributedValue[int] | None = None
    assessment: AttributedClaim | None = None

    @model_validator(mode="after")
    def _validate_pillar(self) -> "IndustryPillarAssessment":
        if self.score is not None and not 1 <= self.score.value <= 5:
            raise ValueError("industry pillar score must be between 1 and 5")
        allows_payload = self.coverage.evidence_status in {
            "available",
            "partial",
            "conflicting",
        }
        has_payload = self.score is not None or self.assessment is not None
        if allows_payload and not has_payload:
            raise ValueError("supported industry pillar requires a score or assessment")
        if not allows_payload and has_payload:
            raise ValueError(
                "unsupported industry pillar must omit score and assessment"
            )
        return self


class CompetitiveAssessment(_ContractModel):
    pillars: list[IndustryPillarAssessment] = Field(min_length=4, max_length=4)
    composite: AttributedValue[float] | None = None
    dominant_pillar: PillarName | None = None
    ideal_market_pattern: AttributedValue[int] | None = None

    @model_validator(mode="after")
    def _validate_scorecard(self) -> "CompetitiveAssessment":
        pillar_ids = [pillar.pillar_id for pillar in self.pillars]
        expected = {
            "addressable_market",
            "industry_structure",
            "competitive_forces",
            "competitive_advantage",
        }
        if set(pillar_ids) != expected or len(pillar_ids) != len(set(pillar_ids)):
            raise ValueError("competitive assessment requires each stable pillar once")
        if self.composite is not None and not 1 <= self.composite.value <= 5:
            raise ValueError("competitive composite must be between 1 and 5")
        if self.ideal_market_pattern is not None and not (
            0 <= self.ideal_market_pattern.value <= 10
        ):
            raise ValueError("ideal market pattern must be between 0 and 10")
        return self


class IndustryFundamentals(_ContractModel):
    market_definition: IndustryMarketDefinition
    market_sizes: list[IndustryMarketSizeObservation] = Field(default_factory=list)
    penetration_history: list[IndustryPenetrationObservation] = Field(
        default_factory=list
    )
    market_share_history: list[IndustryMarketShareObservation] = Field(
        default_factory=list
    )
    regulatory_factors: list[IndustryRegulatoryFactor] = Field(default_factory=list)
    funding_indicators: list[IndustryFundingIndicator] = Field(default_factory=list)
    data_gaps: list[DataGap] = Field(default_factory=list)

    @model_validator(mode="after")
    def _unique_ids(self) -> "IndustryFundamentals":
        ids = [row.observation_id for row in self.market_sizes]
        ids.extend(row.observation_id for row in self.penetration_history)
        ids.extend(row.observation_id for row in self.market_share_history)
        ids.extend(row.factor_id for row in self.regulatory_factors)
        ids.extend(row.observation_id for row in self.funding_indicators)
        if len(ids) != len(set(ids)):
            raise ValueError("industry observation ids must be unique")
        return self


class IndustryDiligencePack(_ContractModel):
    schema_version: Literal["2.0"] = "2.0"
    ticker: str
    as_of: str = Field(min_length=1)
    producer: IndustryProducer
    verdict: IndustryVerdict
    confidence: float | None = Field(default=None, ge=0, le=1)
    competitive_assessment: CompetitiveAssessment | None = None
    industry_fundamentals: IndustryFundamentals | None = None
    structure_and_drivers: list[IndustryNarrativeObservation] = Field(
        default_factory=list
    )
    sources: list[SourceRecord] = Field(default_factory=list)
    data_gaps: list[DataGap] = Field(default_factory=list)
    recommended_next_action: str | None = Field(default=None, min_length=1)

    @field_validator("ticker", mode="before")
    @classmethod
    def _validate_ticker(cls, value: object) -> str:
        return _normalize_ticker(value)

    @model_validator(mode="after")
    def _validate_pack(self) -> "IndustryDiligencePack":
        if (
            not self.competitive_assessment
            and not self.industry_fundamentals
            and not self.structure_and_drivers
        ):
            if not self.data_gaps and not self.recommended_next_action:
                raise ValueError(
                    "empty industry diligence requires a data gap or next action"
                )
        source_ids = [str(source.id) for source in self.sources]
        if len(source_ids) != len(set(source_ids)):
            raise ValueError("industry source ids must be unique")
        observation_ids = [row.observation_id for row in self.structure_and_drivers]
        if len(observation_ids) != len(set(observation_ids)):
            raise ValueError("industry structure/driver ids must be unique")
        referenced_source_ids: set[str] = set()
        for branch in (
            self.competitive_assessment,
            self.industry_fundamentals,
            self.structure_and_drivers,
            self.data_gaps,
        ):
            referenced_source_ids.update(_iter_pack_evidence_source_ids(branch))
        unknown_source_ids = sorted(referenced_source_ids.difference(source_ids))
        if unknown_source_ids:
            raise ValueError(
                "industry evidence refs must resolve to pack sources: "
                + ", ".join(unknown_source_ids)
            )
        return self


__all__ = [
    "CompetitiveAssessment",
    "IndustryDiligencePack",
    "IndustryEstimateAlternative",
    "IndustryEstimateValue",
    "IndustryFundamentals",
    "IndustryFundingIndicator",
    "IndustryMarketDefinition",
    "IndustryMarketDefinitionPayload",
    "IndustryMarketShareObservation",
    "IndustryMarketSizeObservation",
    "IndustryNarrativeObservation",
    "IndustryPenetrationObservation",
    "IndustryPillarAssessment",
    "IndustryRegulatoryFactor",
    "IndustryRegulatoryFactorPayload",
]
