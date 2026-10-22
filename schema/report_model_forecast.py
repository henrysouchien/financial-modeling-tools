"""Version 2 flexible forecast, scenario, valuation, and expected-value contract."""

from __future__ import annotations

from decimal import Decimal
from math import isclose
from typing import ClassVar, Literal

from pydantic import Field, field_validator, model_validator

from .initiation_evidence import (
    AttributedClaim,
    AttributedValue,
    EstimateValue,
    EvidenceReference,
)
from .model_semantics import ScenarioName
from .numeric_display import NumericDisplaySpec, NumericKind
from industry_slice_core._contract import _ContractModel
from .value_semantics_periods import (
    CANONICAL_PERIOD_LABEL_RE,
    validate_canonical_period_label,
)


ComparisonStatus = Literal[
    "aligned", "diverged", "conflicting", "not_comparable", "unresolved"
]
ScenarioNumericKind = NumericKind
ValuationMethodFamily = Literal[
    "dcf",
    "earnings_multiple",
    "ebitda_multiple",
    "revenue_multiple",
    "book_value",
    "nav",
    "affo",
    "rnpv",
    "commodity_nav",
    "dividend_discount",
    "sum_of_parts",
    "other",
]


def _require_finite(value: Decimal, label: str) -> None:
    if not value.is_finite():
        raise ValueError(f"{label} must be finite")


class ReportSourceStamp(_ContractModel):
    artifact_id: str = Field(min_length=1)
    created_at: str = Field(min_length=1)
    verdict: str = Field(min_length=1)
    confidence: str | float | None = None


class GuidanceRow(_ContractModel):
    row_id: str = Field(min_length=1)
    metric: str = Field(min_length=1)
    guidance_value: EstimateValue
    confidence: str = Field(min_length=1)


class GuidanceThreeWayRow(_ContractModel):
    row_id: str = Field(min_length=1)
    metric: str = Field(min_length=1)
    management_guidance: AttributedValue[str]
    model_assumption: AttributedValue[str]
    consensus: AttributedValue[str]
    model_vs_guide: AttributedClaim
    flag: str = ""


class GuidanceProjection(_ContractModel):
    source: ReportSourceStamp
    summary: AttributedClaim
    guidance_rows: list[GuidanceRow] = Field(default_factory=list)
    three_way_rows: list[GuidanceThreeWayRow] = Field(default_factory=list)

    @model_validator(mode="after")
    def _unique_rows(self) -> "GuidanceProjection":
        ids = [row.row_id for row in self.guidance_rows]
        ids.extend(row.row_id for row in self.three_way_rows)
        if len(ids) != len(set(ids)):
            raise ValueError("guidance row ids must be unique")
        return self


class ConsensusComparisonRow(_ContractModel):
    row_id: str = Field(min_length=1)
    period: str = Field(
        min_length=1,
        pattern=CANONICAL_PERIOD_LABEL_RE.pattern,
    )
    metric: str = Field(min_length=1)
    display: NumericDisplaySpec
    model_estimate: AttributedValue[Decimal]
    consensus_average: AttributedValue[Decimal]
    delta_absolute: Decimal
    delta_pct: Decimal | None
    variant: bool

    @field_validator("period", mode="before")
    @classmethod
    def _canonical_period(cls, value: object) -> object:
        if not isinstance(value, str):
            raise ValueError("consensus comparison period must be a string")
        return validate_canonical_period_label(
            value,
            field_name="consensus comparison period",
        ) or value

    @model_validator(mode="after")
    def _validate_numbers(self) -> "ConsensusComparisonRow":
        for name in ("model_estimate", "consensus_average"):
            _require_finite(getattr(self, name).value, name)
        _require_finite(self.delta_absolute, "delta_absolute")
        if self.delta_pct is not None:
            _require_finite(self.delta_pct, "delta_pct")
        if not isclose(
            float(self.delta_absolute),
            float(self.model_estimate.value - self.consensus_average.value),
            abs_tol=1e-9,
        ):
            raise ValueError("consensus absolute delta does not reconcile")
        if self.consensus_average.value == 0:
            if self.delta_pct is not None:
                raise ValueError(
                    "zero consensus average requires an undefined percentage delta"
                )
        elif self.delta_pct is None or not isclose(
            float(self.delta_pct),
            float(self.delta_absolute / self.consensus_average.value),
            abs_tol=1e-9,
        ):
            raise ValueError("consensus percentage delta does not reconcile")
        return self


class ConsensusProjection(_ContractModel):
    source: ReportSourceStamp
    basis: str = Field(min_length=1)
    basis_matched: bool
    period_alignment: str = Field(min_length=1)
    status: ComparisonStatus
    summary: AttributedClaim
    rows: list[ConsensusComparisonRow] = Field(default_factory=list)
    narrative: AttributedClaim | None = None


class ScenarioMetricRow(_ContractModel):
    metric_id: str = Field(min_length=1)
    display_label: str = Field(min_length=1)
    display: NumericDisplaySpec
    bear: AttributedValue[Decimal] | None = None
    base: AttributedValue[Decimal] | None = None
    bull: AttributedValue[Decimal] | None = None

    @model_validator(mode="after")
    def _validate_values(self) -> "ScenarioMetricRow":
        for scenario in ("bear", "base", "bull"):
            value = getattr(self, scenario)
            if value is not None:
                _require_finite(value.value, scenario)
        return self


class ScenarioFactorCurve(_ContractModel):
    factor: str = Field(min_length=1)
    display: NumericDisplaySpec
    bear: dict[int, Decimal]
    base: dict[int, Decimal]
    bull: dict[int, Decimal]

    @model_validator(mode="after")
    def _aligned_years(self) -> "ScenarioFactorCurve":
        if not (set(self.bear) == set(self.base) == set(self.bull)):
            raise ValueError("scenario factor curves must use identical years")
        for curve in (self.bear, self.base, self.bull):
            for value in curve.values():
                _require_finite(value, "scenario factor")
        return self


class ScenarioProjection(_ContractModel):
    source: ReportSourceStamp
    headline: AttributedClaim
    terminal_year: int
    metrics: list[ScenarioMetricRow] = Field(min_length=1)
    primary_ordering_metric_id: str = Field(min_length=1)
    ordering_direction: Literal["higher_is_better", "lower_is_better"]
    spread_passed: bool
    spread_pct_of_base: Decimal | None = None
    spread_rationale: AttributedClaim
    reconciliation_warning: str | None = Field(default=None, min_length=1)
    factor_curves: list[ScenarioFactorCurve] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_scenarios(self) -> "ScenarioProjection":
        metric_ids = [row.metric_id for row in self.metrics]
        if len(metric_ids) != len(set(metric_ids)):
            raise ValueError("scenario metric ids must be unique")
        by_id = {row.metric_id: row for row in self.metrics}
        if self.primary_ordering_metric_id not in by_id:
            raise ValueError("primary ordering metric must be present")
        primary = by_id[self.primary_ordering_metric_id]
        values = (primary.bear, primary.base, primary.bull)
        complete = all(value is not None for value in values)
        ordered = False
        if complete:
            bear, base, bull = (value.value for value in values if value is not None)
            ordered = (
                bear < base < bull
                if self.ordering_direction == "higher_is_better"
                else bear > base > bull
            )
        if (
            not complete or not ordered or not self.spread_passed
        ) and not self.reconciliation_warning:
            raise ValueError(
                "incomplete, unordered, or failed-spread scenarios require a warning"
            )
        if self.spread_pct_of_base is not None:
            _require_finite(self.spread_pct_of_base, "spread_pct_of_base")
        return self


class ValuationAssumption(_ContractModel):
    quantity_bindings: ClassVar[tuple[tuple[str, str], ...]] = (
        ("value", "display"),
    )

    assumption_id: str = Field(min_length=1)
    name: str = Field(min_length=1)
    value: Decimal
    display: NumericDisplaySpec
    period: str = Field(min_length=1)
    rationale: str = Field(min_length=1)
    evidence_refs: list[EvidenceReference] = Field(min_length=1)

    @field_validator("value")
    @classmethod
    def _finite_value(cls, value: Decimal) -> Decimal:
        _require_finite(value, "valuation assumption")
        return value


class ValuationMethodTarget(_ContractModel):
    method_id: str = Field(min_length=1)
    method_family: ValuationMethodFamily
    display_label: str = Field(min_length=1)
    methodology_label: str | None = Field(default=None, min_length=1)
    methodology_rationale: AttributedClaim | None = None
    target_price: AttributedValue[Decimal]
    currency: str = Field(min_length=3, max_length=3)
    valuation_date: str = Field(min_length=1)
    assumptions: list[ValuationAssumption] = Field(default_factory=list)
    reality_check: Literal["pass", "watch", "fail", "not_applicable"]

    @field_validator("currency")
    @classmethod
    def _uppercase_currency(cls, value: str) -> str:
        return value.upper()

    @model_validator(mode="after")
    def _validate_method(self) -> "ValuationMethodTarget":
        _require_finite(self.target_price.value, "valuation target")
        if self.method_family == "other" and (
            not self.methodology_label or self.methodology_rationale is None
        ):
            raise ValueError("other valuation method requires label and rationale")
        return self


class ValuationProjection(_ContractModel):
    source: ReportSourceStamp
    as_of: str = Field(min_length=1)
    current_price: AttributedValue[Decimal] | None = None
    methods: list[ValuationMethodTarget] = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_methods(self) -> "ValuationProjection":
        ids = [method.method_id for method in self.methods]
        if len(ids) != len(set(ids)):
            raise ValueError("valuation method ids must be unique")
        if self.current_price is not None:
            _require_finite(self.current_price.value, "current price")
            if self.current_price.value <= 0:
                raise ValueError("current price must be positive")
        return self


class ScenarioProbabilityRow(_ContractModel):
    scenario: ScenarioName
    weight: AttributedValue[Decimal]
    rationale: AttributedClaim

    @model_validator(mode="after")
    def _validate_weight(self) -> "ScenarioProbabilityRow":
        _require_finite(self.weight.value, "scenario probability")
        if not 0 <= self.weight.value <= 1:
            raise ValueError("scenario probability must be between zero and one")
        return self


class ExpectedValueTargetRow(_ContractModel):
    scenario: ScenarioName
    target_price: AttributedValue[Decimal]
    return_pct: Decimal
    weight: Decimal
    target_period: str = Field(min_length=1)

    @model_validator(mode="after")
    def _finite_numbers(self) -> "ExpectedValueTargetRow":
        for label, value in (
            ("target_price", self.target_price.value),
            ("return_pct", self.return_pct),
            ("weight", self.weight),
        ):
            _require_finite(value, label)
        return self


class ExpectedValueProjection(_ContractModel):
    source: ReportSourceStamp
    selected_horizon: str = Field(min_length=1)
    horizon_months: int = Field(ge=1, le=120)
    current_price: AttributedValue[Decimal]
    probabilities: list[ScenarioProbabilityRow] = Field(min_length=3, max_length=3)
    targets: list[ExpectedValueTargetRow] = Field(min_length=3, max_length=3)
    expected_price: AttributedValue[Decimal]
    expected_return_pct: Decimal
    annualized_return_pct: Decimal
    return_to_risk: Decimal

    @model_validator(mode="after")
    def _validate_ev_math(self) -> "ExpectedValueProjection":
        probability_map = {row.scenario: row.weight.value for row in self.probabilities}
        target_map = {row.scenario: row for row in self.targets}
        required = {"bear", "base", "bull"}
        if set(probability_map) != required or set(target_map) != required:
            raise ValueError("expected value requires exactly bear/base/bull rows")
        for name, value in (
            ("current_price", self.current_price.value),
            ("expected_price", self.expected_price.value),
            ("expected_return_pct", self.expected_return_pct),
            ("annualized_return_pct", self.annualized_return_pct),
            ("return_to_risk", self.return_to_risk),
        ):
            _require_finite(value, name)
        if self.current_price.value <= 0 or self.expected_price.value <= 0:
            raise ValueError("expected value prices must be positive")
        if not isclose(float(sum(probability_map.values())), 1.0, abs_tol=1e-9):
            raise ValueError("scenario probabilities must sum to one")
        for scenario, target in target_map.items():
            if not isclose(
                float(target.weight), float(probability_map[scenario]), abs_tol=1e-9
            ):
                raise ValueError("target weights must match scenario probabilities")
            expected_return = target.target_price.value / self.current_price.value - 1
            if not isclose(
                float(target.return_pct), float(expected_return), abs_tol=1e-6
            ):
                raise ValueError("scenario return is inconsistent with target and spot")
            if target.target_period.lower() != self.selected_horizon.lower():
                raise ValueError("expected-value target horizons must match")
        expected_price = sum(
            probability_map[scenario] * target_map[scenario].target_price.value
            for scenario in required
        )
        if not isclose(
            float(self.expected_price.value), float(expected_price), abs_tol=1e-6
        ):
            raise ValueError("expected price is inconsistent with weighted targets")
        expected_return = expected_price / self.current_price.value - 1
        if not isclose(
            float(self.expected_return_pct), float(expected_return), abs_tol=1e-6
        ):
            raise ValueError("expected return is inconsistent with expected price")
        annualized = (expected_price / self.current_price.value) ** (
            Decimal(12) / Decimal(self.horizon_months)
        ) - 1
        if not isclose(
            float(self.annualized_return_pct), float(annualized), abs_tol=1e-6
        ):
            raise ValueError("annualized return is inconsistent with horizon")
        return self


class ForecastReconciliationItem(_ContractModel):
    topic: str = Field(min_length=1)
    status: ComparisonStatus
    detail: AttributedClaim
    source_artifact_ids: list[str] = Field(min_length=1)


class ReportModelForecastPack(_ContractModel):
    schema_version: Literal["2.0"] = "2.0"
    ticker: str = Field(min_length=1)
    guidance: GuidanceProjection | None = None
    consensus: ConsensusProjection | None = None
    scenarios: ScenarioProjection | None = None
    valuation: ValuationProjection | None = None
    expected_value: ExpectedValueProjection | None = None
    reconciliation: list[ForecastReconciliationItem] = Field(default_factory=list)

    @field_validator("ticker")
    @classmethod
    def _uppercase_ticker(cls, value: str) -> str:
        return value.upper()

    @model_validator(mode="after")
    def _require_payload(self) -> "ReportModelForecastPack":
        if not any(
            (
                self.guidance,
                self.consensus,
                self.scenarios,
                self.valuation,
                self.expected_value,
            )
        ):
            raise ValueError("forecast pack requires at least one subsection")
        if self.expected_value is not None and (
            self.scenarios is None or self.valuation is None
        ):
            raise ValueError("expected value requires scenario and valuation payloads")
        return self


__all__ = [
    "ComparisonStatus",
    "ConsensusComparisonRow",
    "ConsensusProjection",
    "ExpectedValueProjection",
    "ExpectedValueTargetRow",
    "ForecastReconciliationItem",
    "GuidanceProjection",
    "GuidanceRow",
    "GuidanceThreeWayRow",
    "ReportModelForecastPack",
    "ReportSourceStamp",
    "ScenarioFactorCurve",
    "ScenarioMetricRow",
    "ScenarioProbabilityRow",
    "ScenarioProjection",
    "ValuationAssumption",
    "ValuationMethodFamily",
    "ValuationMethodTarget",
    "ValuationProjection",
]
