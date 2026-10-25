"""Immutable precompile ScenarioSet judgment and deterministic projection."""

from __future__ import annotations

from datetime import datetime
import hashlib
import math
import secrets
from typing import Any, Literal, Mapping, Sequence

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from .business_model_revision_ref import BusinessModelRevisionRef
from .business_model_stage import BusinessModelSelectedEvidence
from .forecast_assumption_set import ForecastAssumptionSet, ForecastAssumptionSetRef
from .forecast_control_profile import ForecastControlProfileRef
from .historical_basis_snapshot import fiscal_axis_digest
from .historical_foundation import HistoricalFoundationRef
from .model_spec_authoring import ScenarioOverrideAuthoring, ScenarioReviewAuthoring
from .model_state_identity import validate_sha256_hex
from .stable_control_identity import FiscalAxis
from .ticker_contract import normalize_ticker
from .versioned_artifact import canonical_json_bytes


class _Contract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


def _digest(domain: str, value: object) -> str:
    return hashlib.sha256(
        domain.encode("utf-8") + b"\0" + canonical_json_bytes(value)
    ).hexdigest()


def _require_aware(value: datetime, *, field_name: str | None) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


def _sorted_unique(values: tuple[str, ...], *, field_name: str | None) -> tuple[str, ...]:
    if values != tuple(sorted(set(values))) or any(not value for value in values):
        raise ValueError(f"{field_name} must be sorted, unique, and nonblank")
    return values


class ScenarioSetRef(_Contract):
    schema_version: Literal["scenario-set-ref.v1"] = "scenario-set-ref.v1"
    set_id: str = Field(pattern=r"^scn_[0-9a-f]{32}$")
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _content_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")


def issue_scenario_set_id() -> str:
    return f"scn_{secrets.token_hex(16)}"


class CriticalFactorsArtifactRef(_Contract):
    """Exact immutable critical-factors prerequisite selected by the workflow."""

    schema_version: Literal["critical-factors-artifact-ref.v1"] = (
        "critical-factors-artifact-ref.v1"
    )
    skill: Literal["critical-factors"] = "critical-factors"
    artifact_id: str = Field(min_length=1)
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _content_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")


class ScenarioMethodologyPin(_Contract):
    schema_version: Literal["scenario-methodology-pin.v1"] = (
        "scenario-methodology-pin.v1"
    )
    name: Literal["earnings-scenarios"] = "earnings-scenarios"
    declared_version: str = Field(min_length=1)
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _content_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")


class ScenarioMateriality(_Contract):
    schema_version: Literal["scenario-materiality.v1"] = "scenario-materiality.v1"
    threshold_pct: float = Field(gt=0)
    metric: str = Field(min_length=1)
    horizon: str = Field(min_length=1)
    rationale: str = Field(min_length=1)
    evidence_ids: tuple[str, ...] = ()

    @field_validator("threshold_pct")
    @classmethod
    def _finite_threshold(cls, value: float) -> float:
        if not math.isfinite(value):
            raise ValueError("materiality threshold must be finite")
        return value

    @field_validator("evidence_ids")
    @classmethod
    def _evidence_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _sorted_unique(value, field_name="materiality evidence ids")


class ScenarioCriticalFactor(_Contract):
    schema_version: Literal["scenario-critical-factor.v1"] = (
        "scenario-critical-factor.v1"
    )
    factor_key: str = Field(min_length=1)
    driver_key: str = Field(min_length=1)
    # The earnings-scenarios floor of three is a judgment the ScenarioSet
    # context reports (``critical_factors_unqualified``), not a shape.
    forecastability_score: int = Field(ge=1, le=5)
    mechanism: str = Field(min_length=1)
    direction: Literal["higher_is_bull", "lower_is_bull"]
    evidence_ids: tuple[str, ...] = ()

    @field_validator("evidence_ids")
    @classmethod
    def _evidence_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _sorted_unique(value, field_name="critical-factor evidence ids")


class ScenarioCurveEvidence(_Contract):
    """Evidence closure for one bull/base/bear curve."""

    schema_version: Literal["scenario-curve-evidence.v1"] = (
        "scenario-curve-evidence.v1"
    )
    bull_by_period: dict[int, tuple[str, ...]]
    base_by_period: dict[int, tuple[str, ...]]
    bear_by_period: dict[int, tuple[str, ...]]

    @model_validator(mode="after")
    def _canonical_ids(self) -> "ScenarioCurveEvidence":
        for field_name in ("bull_by_period", "base_by_period", "bear_by_period"):
            rows = getattr(self, field_name)
            for year, evidence_ids in rows.items():
                if year <= 0:
                    raise ValueError(f"{field_name} fiscal years must be positive")
                if not evidence_ids:
                    raise ValueError(f"{field_name} requires evidence for every period")
                _sorted_unique(
                    evidence_ids,
                    field_name=f"{field_name}[{year}] evidence ids",
                )
        return self

    def all_ids(self) -> tuple[str, ...]:
        return tuple(
            sorted(
                {
                    evidence_id
                    for rows in (
                        self.bull_by_period,
                        self.base_by_period,
                        self.bear_by_period,
                    )
                    for evidence_ids in rows.values()
                    for evidence_id in evidence_ids
                }
            )
        )


def _curve_years(period_ids: tuple[str, ...]) -> set[int]:
    return {
        int(period_id.removeprefix("FY").removesuffix("E").split("Q", 1)[0])
        for period_id in period_ids
    }


class ScenarioDriverCurve(_Contract):
    """Agent-authored scenario economics for one natural forecast driver.

    The base case is not stored: it is the bound ForecastAssumptionSet's value
    curve, read through ``scenario_case_values``.  A flexed curve stores its
    authored bull and bear cases; a held-at-base curve stores no values, since
    every case of it is that base.
    """

    schema_version: Literal["scenario-driver-curve.v1"] = (
        "scenario-driver-curve.v1"
    )
    driver_key: str = Field(min_length=1)
    critical_factor_key: str | None = Field(default=None, min_length=1)
    role: Literal["selected_factor", "amplifier_channel", "held_at_base"]
    unit: str = Field(min_length=1)
    percentage_value_space: Literal["fraction", "percentage_points"] | None = None
    direction: Literal["higher_is_bull", "lower_is_bull", "unchanged"]
    period_ids: tuple[str, ...] = Field(min_length=1)
    bull_values: dict[int, float] | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    # Legacy: curves persisted before the base became a read of the FAS carry
    # a copy of it here (and held-at-base curves carry it as bull/bear too).
    # Never written and never read; kept decodable only because
    # ``scenario_set_digest`` covers those stored bytes, so dropping the key
    # on decode would break every such set's content digest.
    base_values: dict[int, float] | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    bear_values: dict[int, float] | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    rationale: str = Field(min_length=1)
    evidence: ScenarioCurveEvidence
    bull_extension_basis_by_period: dict[int, str] = Field(default_factory=dict)
    bear_extension_basis_by_period: dict[int, str] = Field(default_factory=dict)
    limitations: str = Field(min_length=1)

    @model_validator(mode="after")
    def _curve_shape(self) -> "ScenarioDriverCurve":
        if self.role == "held_at_base":
            if self.critical_factor_key is not None or self.direction != "unchanged":
                raise ValueError(
                    "held-at-base curves cannot bind a critical factor or direction"
                )
        elif self.critical_factor_key is None or self.direction == "unchanged":
            raise ValueError(
                "flexed scenario curves require a critical factor and bull direction"
            )
        elif self.bull_values is None or self.bear_values is None:
            raise ValueError("flexed scenario curves require bull and bear values")
        if self.unit != "percentage" and self.percentage_value_space is not None:
            raise ValueError("percentage_value_space requires percentage unit")
        years = _curve_years(self.period_ids)
        if len(years) != len(self.period_ids):
            raise ValueError("scenario period_ids must be unique fiscal-year selectors")
        for field_name in ("bull_values", "bear_values"):
            values = getattr(self, field_name)
            if values is None:
                continue
            if set(values) != years:
                raise ValueError(f"{field_name} must cover the exact scenario periods")
            if any(not math.isfinite(value) for value in values.values()):
                raise ValueError(f"{field_name} values must be finite")
        for field_name in (
            "bull_extension_basis_by_period",
            "bear_extension_basis_by_period",
        ):
            values = getattr(self, field_name)
            if not set(values) <= years or any(not value for value in values.values()):
                raise ValueError(f"{field_name} must name valued periods with nonblank text")
        for field_name in ("bull_by_period", "base_by_period", "bear_by_period"):
            if set(getattr(self.evidence, field_name)) != years:
                raise ValueError(f"evidence.{field_name} must cover the exact periods")
        return self


ScenarioCase = Literal["bull", "base", "bear"]


def scenario_base_values(
    forecast_set: ForecastAssumptionSet,
) -> dict[str, dict[int, float]]:
    """The base curve of every valued driver of one ForecastAssumptionSet.

    This is the one read of a ScenarioSet's base case: the set stores only its
    bull/bear judgment, and its base is the bound FAS's values.
    """

    years = tuple(
        period.fiscal_year for period in forecast_set.fiscal_axis.forecast_periods
    )
    result: dict[str, dict[int, float]] = {}
    for row in forecast_set.driver_assumptions:
        assumption = row.assumption
        if assumption.period_values is not None:
            result[assumption.driver] = {
                int(year): float(value)
                for year, value in assumption.period_values.items()
            }
        elif assumption.value is not None:
            result[assumption.driver] = {
                year: float(assumption.value) for year in years
            }
    return result


def scenario_case_values(
    curve: ScenarioDriverCurve,
    case: ScenarioCase,
    base_values_by_driver: Mapping[str, Mapping[int, float]],
) -> dict[int, float]:
    """One case of one curve: the FAS base for ``base`` and every held case."""

    if case == "base" or curve.role == "held_at_base":
        return dict(base_values_by_driver[curve.driver_key])
    values = curve.bull_values if case == "bull" else curve.bear_values
    # ``_curve_shape``: a flexed curve always stores both authored cases.
    assert values is not None
    return dict(values)


def scenario_curve_base_gap(
    curve: ScenarioDriverCurve,
    base_values: Mapping[int, float],
) -> str | None:
    """Why the FAS base cannot carry one curve, if it cannot.

    The base must cover exactly the curve's periods with finite values.
    """

    if set(base_values) != _curve_years(curve.period_ids):
        return f"base curve for {curve.driver_key} must cover the exact scenario periods"
    if any(not math.isfinite(value) for value in base_values.values()):
        return f"base curve for {curve.driver_key} must be finite"
    return None


def scenario_curve_order_violation(
    curve: ScenarioDriverCurve,
    base_values: Mapping[int, float],
) -> str | None:
    """Why one curve's cases are out of order around its FAS base, if they are.

    A flexed curve must run bull > base > bear (reversed for
    ``lower_is_bull``) in every period; a held-at-base curve is the base.
    A base that cannot carry the curve (:func:`scenario_curve_base_gap`) is
    reported first.
    """

    gap = scenario_curve_base_gap(curve, base_values)
    if gap is not None:
        return gap
    if curve.role == "held_at_base":
        return None
    bull_values = scenario_case_values(curve, "bull", {curve.driver_key: base_values})
    bear_values = scenario_case_values(curve, "bear", {curve.driver_key: base_values})
    for year in sorted(_curve_years(curve.period_ids)):
        bull = bull_values[year]
        base = base_values[year]
        bear = bear_values[year]
        ordered = bull > base > bear
        if curve.direction == "lower_is_bull":
            ordered = bull < base < bear
        if not ordered:
            return f"scenario ordering is invalid for {curve.driver_key} in FY{year}"
    return None


def roll_scenario_driver_curve(
    curve: ScenarioDriverCurve,
    *,
    prior_base_values: Mapping[int, float],
    new_base_values: Mapping[int, float],
    forecast_period_ids: tuple[str, ...],
    fallback_evidence_ids: tuple[str, ...] = (),
) -> ScenarioDriverCurve:
    """Roll one curve onto an updated forecast axis.

    Overlapping years keep their authored bull/bear offsets against the FAS
    base. A newly added outer year carries the prior terminal offset, and
    evidence keys follow the rolled years. A held-at-base curve stores no
    values.
    """

    target_years = tuple(sorted(_curve_years(forecast_period_ids)))

    def _roll_evidence_map(
        mapping: Mapping[int, tuple[str, ...]],
    ) -> dict[int, tuple[str, ...]]:
        rolled: dict[int, tuple[str, ...]] = {}
        for year in target_years:
            if year in mapping and mapping[year]:
                rolled[year] = tuple(mapping[year])
            elif mapping:
                rolled[year] = tuple(mapping[max(mapping)])
            else:
                rolled[year] = tuple(sorted(set(fallback_evidence_ids)))
        return rolled

    rolled_evidence = ScenarioCurveEvidence(
        bull_by_period=_roll_evidence_map(curve.evidence.bull_by_period),
        base_by_period=_roll_evidence_map(curve.evidence.base_by_period),
        bear_by_period=_roll_evidence_map(curve.evidence.bear_by_period),
    )
    if curve.role == "held_at_base":
        return ScenarioDriverCurve(
            driver_key=curve.driver_key,
            critical_factor_key=curve.critical_factor_key,
            role=curve.role,
            unit=curve.unit,
            percentage_value_space=curve.percentage_value_space,
            direction=curve.direction,
            period_ids=forecast_period_ids,
            bull_values=None,
            bear_values=None,
            rationale=curve.rationale,
            evidence=rolled_evidence,
            bull_extension_basis_by_period={
                year: text
                for year, text in curve.bull_extension_basis_by_period.items()
                if year in target_years
            },
            bear_extension_basis_by_period={
                year: text
                for year, text in curve.bear_extension_basis_by_period.items()
                if year in target_years
            },
            limitations=curve.limitations,
        )

    # ``_curve_shape``: a flexed curve always stores both authored cases.
    authored_bull = curve.bull_values
    authored_bear = curve.bear_values
    assert authored_bull is not None and authored_bear is not None
    prior_terminal_year = max(authored_bull)
    bull_values: dict[int, float] = {}
    bear_values: dict[int, float] = {}
    for year in target_years:
        if year in authored_bull:
            if year in prior_base_values and year in new_base_values:
                bull_val = float(new_base_values[year]) + (
                    float(authored_bull[year]) - float(prior_base_values[year])
                )
                bear_val = float(new_base_values[year]) + (
                    float(authored_bear[year]) - float(prior_base_values[year])
                )
            else:
                bull_val = float(authored_bull[year])
                bear_val = float(authored_bear[year])
        else:
            prior_base_anchor = float(
                prior_base_values.get(
                    prior_terminal_year,
                    new_base_values.get(year, 0.0),
                )
            )
            new_base_anchor = float(new_base_values.get(year, prior_base_anchor))
            bull_val = new_base_anchor + (
                float(authored_bull[prior_terminal_year]) - prior_base_anchor
            )
            bear_val = new_base_anchor + (
                float(authored_bear[prior_terminal_year]) - prior_base_anchor
            )
        bull_values[year] = bull_val
        bear_values[year] = bear_val
    bull_extension = {
        year: text
        for year, text in curve.bull_extension_basis_by_period.items()
        if year in target_years
    }
    bear_extension = {
        year: text
        for year, text in curve.bear_extension_basis_by_period.items()
        if year in target_years
    }
    for year in target_years:
        if year not in authored_bull:
            bull_extension[year] = curve.bull_extension_basis_by_period.get(
                prior_terminal_year,
                "terminal_carry",
            )
            bear_extension[year] = curve.bear_extension_basis_by_period.get(
                prior_terminal_year,
                "terminal_carry",
            )
    return ScenarioDriverCurve(
        driver_key=curve.driver_key,
        critical_factor_key=curve.critical_factor_key,
        role=curve.role,
        unit=curve.unit,
        percentage_value_space=curve.percentage_value_space,
        direction=curve.direction,
        period_ids=forecast_period_ids,
        bull_values=bull_values,
        bear_values=bear_values,
        rationale=curve.rationale,
        evidence=rolled_evidence,
        bull_extension_basis_by_period=bull_extension,
        bear_extension_basis_by_period=bear_extension,
        limitations=curve.limitations,
    )


class ScenarioCaseProbabilities(_Contract):
    """Agent-authored bull/base/bear weights for one ScenarioSet.

    Probabilities are per-name judgment owned by the ScenarioSet; there is no
    house default.  The compiler writes these weights, with the set's lineage,
    into the workbook's expected-value probability cells.
    """

    schema_version: Literal["scenario-case-probabilities.v1"] = (
        "scenario-case-probabilities.v1"
    )
    bull: float = Field(ge=0, le=1, allow_inf_nan=False)
    base: float = Field(ge=0, le=1, allow_inf_nan=False)
    bear: float = Field(ge=0, le=1, allow_inf_nan=False)
    rationale: str = Field(min_length=1)
    evidence_ids: tuple[str, ...] = ()

    @field_validator("evidence_ids")
    @classmethod
    def _evidence_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _sorted_unique(value, field_name="case-probability evidence ids")

    @model_validator(mode="after")
    def _sum_to_one(self) -> "ScenarioCaseProbabilities":
        if not math.isclose(self.bull + self.base + self.bear, 1.0, abs_tol=1e-9):
            raise ValueError("bull, base, and bear probabilities must sum to 1")
        return self


class ScenarioGap(_Contract):
    schema_version: Literal["scenario-gap.v1"] = "scenario-gap.v1"
    key: str = Field(min_length=1)
    description: str = Field(min_length=1)
    driver_key: str | None = Field(default=None, min_length=1)
    period_id: str | None = Field(default=None, min_length=1)
    blocking: bool
    repair_target: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _repairable_blocker(self) -> "ScenarioGap":
        if self.blocking and self.repair_target is None:
            raise ValueError("blocking scenario gaps require a repair target")
        return self


#: Key prefix of the blocking gap that records a selected critical factor the
#: set maps to no forecast driver. A set with no mapped factor stands only on it.
CRITICAL_FACTOR_UNMAPPED_GAP_PREFIX = "critical_factor_unmapped:"


class ScenarioTerminalIssue(_Contract):
    schema_version: Literal["scenario-terminal-issue.v1"] = (
        "scenario-terminal-issue.v1"
    )
    key: str = Field(min_length=1)
    text: str = Field(min_length=1)
    drivers: tuple[str, ...] = ()

    @field_validator("drivers")
    @classmethod
    def _drivers(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _sorted_unique(value, field_name="terminal issue drivers")


class ScenarioTerminalSummary(_Contract):
    schema_version: Literal["scenario-terminal-summary.v1"] = (
        "scenario-terminal-summary.v1"
    )
    authored_by: Literal["agent"] = "agent"
    summary: str = Field(min_length=1)
    key_decisions: tuple[str, ...] = ()
    confidence: Literal["low", "medium", "high"]
    limitations: tuple[ScenarioTerminalIssue, ...] = ()
    unresolved_questions: tuple[ScenarioTerminalIssue, ...] = ()
    recommended_next_stage: str = Field(min_length=1)

    @field_validator("key_decisions")
    @classmethod
    def _decisions(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)) or any(not item for item in value):
            raise ValueError("key decisions must be unique and nonblank")
        return value

    @model_validator(mode="after")
    def _issues(self) -> "ScenarioTerminalSummary":
        seen: set[str] = set()
        for field_name in ("limitations", "unresolved_questions"):
            rows = getattr(self, field_name)
            keys = tuple(row.key for row in rows)
            if keys != tuple(sorted(set(keys))):
                raise ValueError(f"{field_name} must be sorted and unique by key")
            overlap = seen & set(keys)
            if overlap:
                raise ValueError("terminal issue keys must be globally unique")
            seen.update(keys)
        return self


class ScenarioStageScope(_Contract):
    schema_version: Literal["scenario-stage-scope.v1"] = "scenario-stage-scope.v1"
    user_id: str = Field(min_length=1)
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1)

    @field_validator("ticker")
    @classmethod
    def _canonical_ticker(cls, value: str) -> str:
        normalized = normalize_ticker(value)
        if value != normalized:
            raise ValueError("ticker must be canonical")
        return normalized


class ScenarioStageReceipt(_Contract):
    schema_version: Literal["scenario-stage-receipt.v1"] = (
        "scenario-stage-receipt.v1"
    )
    stage: Literal["earnings-scenarios"] = "earnings-scenarios"
    status: Literal["accepted", "incomplete", "blocked"]
    scope: ScenarioStageScope
    scenario_set_ref: ScenarioSetRef
    forecast_assumption_set_ref: ForecastAssumptionSetRef
    forecast_assumption_stage_receipt_digest: str
    critical_factors_ref: CriticalFactorsArtifactRef
    started_at: datetime
    completed_at: datetime
    receipt_digest: str

    @field_validator("forecast_assumption_stage_receipt_digest", "receipt_digest")
    @classmethod
    def _digests(cls, value: str, info: ValidationInfo) -> str:
        return validate_sha256_hex(value, field_name=info.field_name)

    @field_validator("started_at", "completed_at")
    @classmethod
    def _timestamps(cls, value: datetime, info: ValidationInfo) -> datetime:
        return _require_aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _binding(self) -> "ScenarioStageReceipt":
        if self.completed_at < self.started_at:
            raise ValueError("completed_at must not precede started_at")
        if self.receipt_digest != scenario_stage_receipt_digest(self):
            raise ValueError("scenario stage receipt digest mismatch")
        return self


def scenario_stage_receipt_digest(
    value: ScenarioStageReceipt | Mapping[str, Any],
) -> str:
    payload = (
        value.model_dump(mode="json")
        if isinstance(value, ScenarioStageReceipt)
        else dict(value)
    )
    payload.pop("receipt_digest", None)
    return _digest("scenario-stage-receipt.v1", payload)


def build_scenario_stage_receipt(
    *,
    status: Literal["accepted", "incomplete", "blocked"],
    scope: ScenarioStageScope,
    scenario_set_ref: ScenarioSetRef,
    forecast_assumption_set_ref: ForecastAssumptionSetRef,
    forecast_assumption_stage_receipt_digest: str,
    critical_factors_ref: CriticalFactorsArtifactRef,
    started_at: datetime,
    completed_at: datetime,
) -> ScenarioStageReceipt:
    payload = {
        "schema_version": "scenario-stage-receipt.v1",
        "stage": "earnings-scenarios",
        "status": status,
        "scope": scope,
        "scenario_set_ref": scenario_set_ref,
        "forecast_assumption_set_ref": forecast_assumption_set_ref,
        "forecast_assumption_stage_receipt_digest": (
            forecast_assumption_stage_receipt_digest
        ),
        "critical_factors_ref": critical_factors_ref,
        "started_at": started_at,
        "completed_at": completed_at,
    }
    return ScenarioStageReceipt.model_validate(
        {**payload, "receipt_digest": scenario_stage_receipt_digest(payload)}
    )


class ScenarioSetArtifact(_Contract):
    schema_version: Literal["scenario-set.v1"] = "scenario-set.v1"
    user_id: str = Field(min_length=1)
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1)
    ref: ScenarioSetRef
    status: Literal["accepted", "incomplete", "blocked"]
    forecast_assumption_set_ref: ForecastAssumptionSetRef
    forecast_assumption_stage_receipt_digest: str
    business_model_revision_ref: BusinessModelRevisionRef
    business_model_selection_epoch: int = Field(gt=0)
    business_model_stage_receipt_digest: str
    historical_foundation_ref: HistoricalFoundationRef
    fiscal_axis: FiscalAxis
    fiscal_axis_sha256: str
    forecast_control_profile_ref: ForecastControlProfileRef
    forecast_control_denominator_sha256: str
    critical_factors_ref: CriticalFactorsArtifactRef
    critical_factors: tuple[ScenarioCriticalFactor, ...]
    materiality: ScenarioMateriality
    methodology: ScenarioMethodologyPin
    required_driver_keys: tuple[str, ...]
    driver_curves: tuple[ScenarioDriverCurve, ...] = ()
    inherited_forecast_evidence_ids: tuple[str, ...] = ()
    scenario_evidence: tuple[BusinessModelSelectedEvidence, ...] = ()
    case_probabilities: ScenarioCaseProbabilities | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    gaps: tuple[ScenarioGap, ...] = ()
    terminal_summary: ScenarioTerminalSummary
    created_by: str = Field(min_length=1)
    created_at: datetime
    # The Revision link (``schema/judgment_revision.py``): absent on a first
    # set and on every set persisted before it.
    predecessor_ref: ScenarioSetRef | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    stage_receipt: ScenarioStageReceipt

    @field_validator("ticker")
    @classmethod
    def _canonical_ticker(cls, value: str) -> str:
        normalized = normalize_ticker(value)
        if value != normalized:
            raise ValueError("ticker must be canonical")
        return normalized

    @field_validator(
        "forecast_assumption_stage_receipt_digest",
        "business_model_stage_receipt_digest",
        "fiscal_axis_sha256",
        "forecast_control_denominator_sha256",
    )
    @classmethod
    def _digests(cls, value: str, info: ValidationInfo) -> str:
        return validate_sha256_hex(value, field_name=info.field_name)

    @field_validator("required_driver_keys", "inherited_forecast_evidence_ids")
    @classmethod
    def _canonical_sets(
        cls,
        value: tuple[str, ...],
        info: ValidationInfo,
    ) -> tuple[str, ...]:
        return _sorted_unique(value, field_name=info.field_name)

    @field_validator("created_at")
    @classmethod
    def _created_at(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="created_at")

    @model_validator(mode="after")
    def _artifact(self) -> "ScenarioSetArtifact":
        if self.business_model_revision_ref.business_model_id != (
            f"{self.ticker}_business_model"
        ):
            raise ValueError("BusinessModel revision ref does not match set ticker")
        if self.fiscal_axis_sha256 != fiscal_axis_digest(self.fiscal_axis):
            raise ValueError("scenario fiscal axis digest mismatch")
        forecast_period_ids = tuple(
            period.period_id for period in self.fiscal_axis.forecast_periods
        )
        factor_keys = tuple(row.factor_key for row in self.critical_factors)
        factor_drivers = tuple(row.driver_key for row in self.critical_factors)
        if factor_keys != tuple(sorted(set(factor_keys))):
            raise ValueError("critical factors must be sorted and unique by factor_key")
        if len(factor_drivers) != len(set(factor_drivers)):
            raise ValueError("critical factors must bind unique forecast drivers")
        if len(self.critical_factors) > 2:
            raise ValueError("ScenarioSet requires one or two selected critical factors")
        if not self.critical_factors and (
            self.status == "accepted"
            or not any(
                row.blocking and row.key.startswith(CRITICAL_FACTOR_UNMAPPED_GAP_PREFIX)
                for row in self.gaps
            )
        ):
            raise ValueError(
                "a ScenarioSet with no mapped critical factor must be incomplete or "
                "blocked and carry a blocking critical_factor_unmapped gap"
            )

        curve_drivers = tuple(row.driver_key for row in self.driver_curves)
        if curve_drivers != tuple(sorted(set(curve_drivers))):
            raise ValueError("scenario curves must be sorted and unique by driver_key")
        factor_by_key: dict[str | None, ScenarioCriticalFactor] = {
            row.factor_key: row for row in self.critical_factors
        }
        selected_curve_drivers: set[str] = set()
        for curve in self.driver_curves:
            if curve.role == "held_at_base":
                continue
            factor = factor_by_key.get(curve.critical_factor_key)
            if factor is None:
                raise ValueError("scenario curve references an unknown critical factor")
            if curve.role == "selected_factor":
                if curve.driver_key != factor.driver_key:
                    raise ValueError(
                        "selected-factor curve must use its critical factor driver"
                    )
                if curve.direction != factor.direction:
                    raise ValueError(
                        "selected-factor curve direction must match critical factor"
                    )
                selected_curve_drivers.add(curve.driver_key)
            if curve.period_ids != forecast_period_ids:
                raise ValueError("scenario curve must cover the exact forecast axis")
        if not set(factor_drivers) <= set(self.required_driver_keys):
            raise ValueError("critical-factor drivers must belong to the denominator")

        evidence_ids = tuple(row.evidence_id for row in self.scenario_evidence)
        if evidence_ids != tuple(sorted(set(evidence_ids))):
            raise ValueError("scenario evidence must be sorted and unique by evidence_id")
        used_evidence = {
            evidence_id
            for curve in self.driver_curves
            for evidence_id in curve.evidence.all_ids()
        } | {
            evidence_id
            for factor in self.critical_factors
            for evidence_id in factor.evidence_ids
        } | set(self.materiality.evidence_ids) | set(
            ()
            if self.case_probabilities is None
            else self.case_probabilities.evidence_ids
        )
        retained = {*self.inherited_forecast_evidence_ids, *evidence_ids}
        if not used_evidence <= retained:
            raise ValueError("scenario judgment cites evidence outside retained authority")
        if retained - used_evidence:
            raise ValueError("retained scenario evidence must be used by the judgment")

        gap_keys = tuple(row.key for row in self.gaps)
        if gap_keys != tuple(sorted(set(gap_keys))):
            raise ValueError("scenario gaps must be sorted and unique by key")
        if self.status == "accepted":
            if curve_drivers != self.required_driver_keys:
                raise ValueError(
                    "accepted scenario curves must equal the required denominator"
                )
            if selected_curve_drivers != set(factor_drivers):
                raise ValueError(
                    "accepted ScenarioSet requires every selected factor curve"
                )
            if any(row.blocking for row in self.gaps):
                raise ValueError("accepted ScenarioSet cannot retain blocking gaps")
        elif not any(row.blocking for row in self.gaps):
            raise ValueError("incomplete or blocked ScenarioSet requires a blocking gap")

        receipt = self.stage_receipt
        expected_scope = ScenarioStageScope(
            user_id=self.user_id,
            research_file_id=self.research_file_id,
            ticker=self.ticker,
        )
        if (
            receipt.scope != expected_scope
            or receipt.status != self.status
            or receipt.scenario_set_ref != self.ref
            or receipt.forecast_assumption_set_ref
            != self.forecast_assumption_set_ref
            or receipt.forecast_assumption_stage_receipt_digest
            != self.forecast_assumption_stage_receipt_digest
            or receipt.critical_factors_ref != self.critical_factors_ref
        ):
            raise ValueError("scenario stage receipt does not bind the exact set")
        if self.ref.content_sha256 != scenario_set_digest(self):
            raise ValueError("ScenarioSet content digest mismatch")
        return self


def scenario_set_digest(
    value: ScenarioSetArtifact | Mapping[str, Any],
) -> str:
    payload = (
        value.model_dump(mode="json")
        if isinstance(value, ScenarioSetArtifact)
        else dict(value)
    )
    payload.pop("ref", None)
    payload.pop("stage_receipt", None)
    if payload.get("predecessor_ref") is None:
        payload.pop("predecessor_ref", None)
    return _digest("scenario-set.v1", payload)


def build_scenario_set(
    *,
    set_id: str | None,
    user_id: str,
    research_file_id: int,
    ticker: str,
    status: Literal["accepted", "incomplete", "blocked"],
    forecast_assumption_set_ref: ForecastAssumptionSetRef,
    forecast_assumption_stage_receipt_digest: str,
    business_model_revision_ref: BusinessModelRevisionRef,
    business_model_selection_epoch: int,
    business_model_stage_receipt_digest: str,
    historical_foundation_ref: HistoricalFoundationRef,
    fiscal_axis: FiscalAxis,
    forecast_control_profile_ref: ForecastControlProfileRef,
    forecast_control_denominator_sha256: str,
    critical_factors_ref: CriticalFactorsArtifactRef,
    critical_factors: Sequence[ScenarioCriticalFactor],
    materiality: ScenarioMateriality,
    methodology: ScenarioMethodologyPin,
    required_driver_keys: Sequence[str],
    driver_curves: Sequence[ScenarioDriverCurve],
    inherited_forecast_evidence_ids: Sequence[str],
    scenario_evidence: Sequence[BusinessModelSelectedEvidence],
    case_probabilities: ScenarioCaseProbabilities | None = None,
    gaps: Sequence[ScenarioGap],
    terminal_summary: ScenarioTerminalSummary,
    created_by: str,
    created_at: datetime,
    started_at: datetime,
    completed_at: datetime,
    predecessor_ref: ScenarioSetRef | None = None,
) -> ScenarioSetArtifact:
    canonical_factors = tuple(sorted(critical_factors, key=lambda row: row.factor_key))
    canonical_curves = tuple(sorted(driver_curves, key=lambda row: row.driver_key))
    canonical_evidence = tuple(
        sorted(scenario_evidence, key=lambda row: row.evidence_id)
    )
    canonical_gaps = tuple(sorted(gaps, key=lambda row: row.key))
    payload = {
        "schema_version": "scenario-set.v1",
        "user_id": user_id,
        "research_file_id": research_file_id,
        "ticker": ticker,
        "status": status,
        "forecast_assumption_set_ref": forecast_assumption_set_ref,
        "forecast_assumption_stage_receipt_digest": (
            forecast_assumption_stage_receipt_digest
        ),
        "business_model_revision_ref": business_model_revision_ref,
        "business_model_selection_epoch": business_model_selection_epoch,
        "business_model_stage_receipt_digest": business_model_stage_receipt_digest,
        "historical_foundation_ref": historical_foundation_ref,
        "fiscal_axis": fiscal_axis,
        "fiscal_axis_sha256": fiscal_axis_digest(fiscal_axis),
        "forecast_control_profile_ref": forecast_control_profile_ref,
        "forecast_control_denominator_sha256": (
            forecast_control_denominator_sha256
        ),
        "critical_factors_ref": critical_factors_ref,
        "critical_factors": canonical_factors,
        "materiality": materiality,
        "methodology": methodology,
        "required_driver_keys": tuple(sorted(set(required_driver_keys))),
        "driver_curves": canonical_curves,
        "inherited_forecast_evidence_ids": tuple(
            sorted(set(inherited_forecast_evidence_ids))
        ),
        "scenario_evidence": canonical_evidence,
        # Unauthored weights are absent from the content, exactly as the
        # field's ``exclude_if`` drops them from the artifact's dump, so sets
        # persisted before the field keep their digest.
        **(
            {}
            if case_probabilities is None
            else {"case_probabilities": case_probabilities}
        ),
        "gaps": canonical_gaps,
        "terminal_summary": terminal_summary,
        "created_by": created_by,
        "created_at": created_at,
        **({} if predecessor_ref is None else {"predecessor_ref": predecessor_ref}),
    }
    ref = ScenarioSetRef(
        set_id=set_id or issue_scenario_set_id(),
        content_sha256=scenario_set_digest(payload),
    )
    receipt = build_scenario_stage_receipt(
        status=status,
        scope=ScenarioStageScope(
            user_id=user_id,
            research_file_id=research_file_id,
            ticker=ticker,
        ),
        scenario_set_ref=ref,
        forecast_assumption_set_ref=forecast_assumption_set_ref,
        forecast_assumption_stage_receipt_digest=(
            forecast_assumption_stage_receipt_digest
        ),
        critical_factors_ref=critical_factors_ref,
        started_at=started_at,
        completed_at=completed_at,
    )
    return ScenarioSetArtifact.model_validate(
        {**payload, "ref": ref, "stage_receipt": receipt}
    )


def project_scenario_reviews(
    value: ScenarioSetArtifact,
    forecast_set: ForecastAssumptionSet,
) -> tuple[ScenarioReviewAuthoring, ...]:
    """Project one accepted ScenarioSet into server-owned ModelSpec rows.

    ``forecast_set`` is the set's bound ForecastAssumptionSet
    (``value.forecast_assumption_set_ref``); the base case of every curve, and
    every case of a held-at-base curve, is read from it.
    """

    record = ScenarioSetArtifact.model_validate_json(value.model_dump_json())
    if record.status != "accepted" or record.stage_receipt.status != "accepted":
        raise ValueError("only accepted ScenarioSets may be projected")
    base_values_by_driver = scenario_base_values(forecast_set)
    if tuple(row.driver_key for row in record.driver_curves) != (
        record.required_driver_keys
    ) or any(
        row.driver_key not in base_values_by_driver for row in record.driver_curves
    ):
        raise ValueError("scenario projection requires exact denominator closure")

    result: list[ScenarioReviewAuthoring] = []
    cases: tuple[ScenarioCase, ...] = ("base", "bull", "bear")
    for case in cases:
        overrides: list[ScenarioOverrideAuthoring] = []
        for curve in record.driver_curves:
            values = scenario_case_values(curve, case, base_values_by_driver)
            evidence_by_period = getattr(curve.evidence, f"{case}_by_period")
            evidence_ids = tuple(
                sorted(
                    {
                        evidence_id
                        for period_evidence in evidence_by_period.values()
                        for evidence_id in period_evidence
                    }
                )
            )
            if case == "base":
                direction: Literal["increase", "decrease", "base"] = "base"
            elif curve.role == "held_at_base":
                direction = "base"
            elif (case == "bull") == (curve.direction == "higher_is_bull"):
                direction = "increase"
            else:
                direction = "decrease"
            overrides.append(
                ScenarioOverrideAuthoring(
                    driver=curve.driver_key,
                    period_ids=curve.period_ids,
                    unit=curve.unit,
                    value=values,
                    direction=direction,
                    rationale=curve.rationale,
                    evidence_basis="source_backed",
                    evidence_ids=evidence_ids,
                    limitations=curve.limitations,
                )
            )
        result.append(
            ScenarioReviewAuthoring(
                scenario_name=case,
                description=(
                    f"{case.title()} case from accepted ScenarioSet "
                    f"{record.ref.set_id}."
                ),
                overrides=tuple(overrides),
            )
        )
    return tuple(result)


__all__ = [
    "CRITICAL_FACTOR_UNMAPPED_GAP_PREFIX",
    "CriticalFactorsArtifactRef",
    "ScenarioCase",
    "ScenarioCaseProbabilities",
    "ScenarioCriticalFactor",
    "ScenarioCurveEvidence",
    "ScenarioDriverCurve",
    "ScenarioGap",
    "ScenarioMateriality",
    "ScenarioMethodologyPin",
    "ScenarioSetArtifact",
    "ScenarioSetRef",
    "ScenarioStageReceipt",
    "ScenarioStageScope",
    "ScenarioTerminalIssue",
    "ScenarioTerminalSummary",
    "build_scenario_set",
    "build_scenario_stage_receipt",
    "issue_scenario_set_id",
    "project_scenario_reviews",
    "roll_scenario_driver_curve",
    "scenario_base_values",
    "scenario_case_values",
    "scenario_curve_base_gap",
    "scenario_curve_order_violation",
    "scenario_set_digest",
    "scenario_stage_receipt_digest",
]
