from __future__ import annotations

import re
import uuid
from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, Field, ValidationInfo, field_validator, model_validator

from ._insights_shared import _FROZEN_CONTRACT
from ._outcome_shared import (
    EarningsBasis,
    OutcomeComponent,
    RestatementPolicy,
    ReturnStatedKind,
    SentimentMeasure,
    ValuationBasisKind,
)
from ._uuid import _NS_OUTCOME
from .outcome_contract import (
    KeyValuePair,
    PredictionSnapshot,
    _canonical_utc_datetime,
)
from .thesis_shared_slice import SourceId


_ISO_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _reject_duplicate_keys(items: tuple[KeyValuePair, ...], *, field_name: str) -> None:
    seen: set[str] = set()
    duplicates: set[str] = set()
    for item in items:
        if item.key in seen:
            duplicates.add(item.key)
        seen.add(item.key)
    if duplicates:
        joined = ", ".join(sorted(duplicates))
        raise ValueError(f"{field_name} contains duplicate keys: {joined}")


def _canonical_iso_date(value: object, *, field_name: str) -> str:
    if isinstance(value, datetime):
        raise ValueError(f"{field_name} must be an ISO date")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        stripped = value.strip()
        if _ISO_DATE_RE.fullmatch(stripped):
            try:
                date.fromisoformat(stripped)
            except ValueError as exc:
                raise ValueError(f"{field_name} must be an ISO date") from exc
            return stripped
    raise ValueError(f"{field_name} must be an ISO date")


def _present_valuation_multiple_keys(snapshot: RealizedValuationSnapshot) -> set[str]:
    keys: set[str] = set()
    for key in ("pe", "ev_ebitda", "p_b", "p_fcf", "p_s"):
        if getattr(snapshot, key) is not None:
            keys.add(key)
    return keys


class ReturnDecomposition(BaseModel):
    absolute: float = Field(allow_inf_nan=False)
    benchmark_relative: float | None = Field(default=None, allow_inf_nan=False)
    factor_adjusted: float | None = Field(default=None, allow_inf_nan=False)

    model_config = _FROZEN_CONTRACT


class RealizedReturn(BaseModel):
    stated_kind: ReturnStatedKind
    stated_value: float = Field(allow_inf_nan=False)
    decomposition: ReturnDecomposition
    benchmark_id: str | None = Field(default=None, min_length=1)
    dividends_included: bool
    split_adjusted: bool
    buyback_adjusted: bool
    currency: str = Field(min_length=1)
    window_start: str = Field(min_length=1)
    window_end: str = Field(min_length=1)
    source: str = Field(min_length=1)

    @field_validator("window_start", "window_end", mode="before")
    @classmethod
    def _normalize_window_dates(cls, value: object, info: ValidationInfo) -> str:
        return _canonical_iso_date(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _stated_leg_matches_value(self) -> RealizedReturn:
        leg_value = getattr(self.decomposition, self.stated_kind)
        if leg_value is None:
            raise ValueError(f"decomposition.{self.stated_kind} is required")
        if leg_value != self.stated_value:
            raise ValueError("decomposition stated leg must equal stated_value")
        if self.stated_kind == "benchmark_relative" and self.benchmark_id is None:
            raise ValueError("benchmark_relative realized return requires benchmark_id")
        if self.stated_kind != "benchmark_relative" and self.benchmark_id is not None:
            raise ValueError("benchmark_id is only valid for benchmark_relative returns")
        return self

    model_config = _FROZEN_CONTRACT


class RealizedFundamentalMetric(BaseModel):
    concept_id: str = Field(min_length=1)
    value: float = Field(allow_inf_nan=False)
    unit: str = Field(min_length=1)

    model_config = _FROZEN_CONTRACT


class RealizedFundamentalsMetrics(BaseModel):
    ticker: str = Field(min_length=1)
    fiscal_period: str = Field(min_length=1)
    metrics: tuple[RealizedFundamentalMetric, ...] = Field(min_length=1)
    earnings_basis: EarningsBasis
    restatement_policy: RestatementPolicy
    source: str = Field(min_length=1)

    model_config = _FROZEN_CONTRACT


class RealizedValuationSnapshot(BaseModel):
    schema_version: Literal["1.0"] = "1.0"
    ticker: str = Field(min_length=1)
    as_of_date: str = Field(min_length=1)
    basis_kind: ValuationBasisKind
    price: float = Field(gt=0, allow_inf_nan=False)
    market_cap: float | None = Field(default=None, allow_inf_nan=False)
    net_debt: float | None = Field(default=None, allow_inf_nan=False)
    pe: float | None = Field(default=None, allow_inf_nan=False)
    ev_ebitda: float | None = Field(default=None, allow_inf_nan=False)
    p_b: float | None = Field(default=None, allow_inf_nan=False)
    p_fcf: float | None = Field(default=None, allow_inf_nan=False)
    p_s: float | None = Field(default=None, allow_inf_nan=False)
    earnings_basis: EarningsBasis
    price_source: str = Field(min_length=1)
    denominator_sources: tuple[KeyValuePair, ...] = Field(min_length=1)

    @field_validator("as_of_date", mode="before")
    @classmethod
    def _normalize_as_of_date(cls, value: object) -> str:
        return _canonical_iso_date(value, field_name="as_of_date")

    @model_validator(mode="after")
    def _multiples_and_denominators_match(self) -> RealizedValuationSnapshot:
        present_keys = _present_valuation_multiple_keys(self)
        if not present_keys:
            raise ValueError("RealizedValuationSnapshot requires at least one multiple")
        _reject_duplicate_keys(self.denominator_sources, field_name="denominator_sources")
        denominator_keys = {item.key for item in self.denominator_sources}
        if denominator_keys != present_keys:
            raise ValueError(
                "denominator_sources must have exactly one entry per present multiple"
            )
        if self.basis_kind in {"forward", "ntm"}:
            edgar_sources = [
                item.key
                for item in self.denominator_sources
                if item.value.startswith("edgar:")
            ]
            if edgar_sources:
                joined = ", ".join(sorted(edgar_sources))
                raise ValueError(
                    f"forward/ntm denominator_sources must use estimate sources: {joined}"
                )
        if self.basis_kind == "trailing":
            non_edgar_sources = [
                item.key
                for item in self.denominator_sources
                if not item.value.startswith("edgar:")
            ]
            if non_edgar_sources:
                joined = ", ".join(sorted(non_edgar_sources))
                raise ValueError(
                    f"trailing denominator_sources must use edgar sources: {joined}"
                )
        return self

    model_config = _FROZEN_CONTRACT


class RealizedSentimentSignal(BaseModel):
    signal_kind: str = Field(min_length=1)
    measure: SentimentMeasure
    value: float = Field(allow_inf_nan=False)
    direction: Literal["bullish", "bearish", "neutral"] | None = None

    @model_validator(mode="after")
    def _direction_required_for_direction_measure(self) -> RealizedSentimentSignal:
        if self.measure == "direction" and self.direction is None:
            raise ValueError("direction is required when measure='direction'")
        return self

    model_config = _FROZEN_CONTRACT


class RealizedSentiment(BaseModel):
    ticker: str = Field(min_length=1)
    as_of_date: str = Field(min_length=1)
    signals: tuple[RealizedSentimentSignal, ...] = Field(min_length=1)
    source: str = Field(min_length=1)

    @field_validator("as_of_date", mode="before")
    @classmethod
    def _normalize_as_of_date(cls, value: object) -> str:
        return _canonical_iso_date(value, field_name="as_of_date")

    model_config = _FROZEN_CONTRACT


class RealizedMarketRisk(BaseModel):
    ticker: str = Field(min_length=1)
    window_start: str = Field(min_length=1)
    window_end: str = Field(min_length=1)
    realized_vol: float | None = Field(default=None, allow_inf_nan=False)
    realized_downside_deviation: float | None = Field(default=None, allow_inf_nan=False)
    realized_max_drawdown: float | None = Field(default=None, allow_inf_nan=False)
    realized_beta: float | None = Field(default=None, allow_inf_nan=False)
    source: str = Field(min_length=1)

    @field_validator("window_start", "window_end", mode="before")
    @classmethod
    def _normalize_window_dates(cls, value: object, info: ValidationInfo) -> str:
        return _canonical_iso_date(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _at_least_one_risk_value(self) -> RealizedMarketRisk:
        if (
            self.realized_vol is None
            and self.realized_downside_deviation is None
            and self.realized_max_drawdown is None
            and self.realized_beta is None
        ):
            raise ValueError("RealizedMarketRisk requires at least one risk value")
        return self

    model_config = _FROZEN_CONTRACT


class RealizedThesisRiskOutcome(BaseModel):
    trigger_metric: str = Field(min_length=1)
    fired: bool
    observed_value: float | None = Field(default=None, allow_inf_nan=False)
    fired_at: str | None = Field(default=None, min_length=1)
    source: str = Field(min_length=1)

    @field_validator("fired_at", mode="before")
    @classmethod
    def _normalize_fired_at(cls, value: object | None) -> str | None:
        if value is None:
            return None
        return _canonical_iso_date(value, field_name="fired_at")

    @model_validator(mode="after")
    def _fired_at_matches_fired(self) -> RealizedThesisRiskOutcome:
        if self.fired and self.fired_at is None:
            raise ValueError("fired_at is required when fired is True")
        if not self.fired and self.fired_at is not None:
            raise ValueError("fired_at is only valid when fired is True")
        return self

    model_config = _FROZEN_CONTRACT


RealizedPayload = (
    RealizedReturn
    | RealizedFundamentalsMetrics
    | RealizedValuationSnapshot
    | RealizedSentiment
    | RealizedMarketRisk
    | RealizedThesisRiskOutcome
)


class ComponentObservation(BaseModel):
    prediction_target_id: str = Field(min_length=1)
    component: OutcomeComponent
    realized: RealizedPayload
    data_vintage: tuple[KeyValuePair, ...] = ()
    source_refs: tuple[SourceId, ...] = ()

    @model_validator(mode="after")
    def _component_matches_realized_payload(self) -> ComponentObservation:
        expected_type = _REALIZED_PAYLOAD_BY_COMPONENT[self.component]
        if not isinstance(self.realized, expected_type):
            raise ValueError(
                f"{self.component} observation requires {expected_type.__name__}"
            )
        _reject_duplicate_keys(self.data_vintage, field_name="data_vintage")
        return self

    model_config = _FROZEN_CONTRACT


_REALIZED_PAYLOAD_BY_COMPONENT: dict[OutcomeComponent, type[BaseModel]] = {
    "return": RealizedReturn,
    "fundamentals": RealizedFundamentalsMetrics,
    "valuation": RealizedValuationSnapshot,
    "sentiment": RealizedSentiment,
    "market_risk": RealizedMarketRisk,
    "thesis_risk": RealizedThesisRiskOutcome,
}


class Realization(BaseModel):
    schema_version: Literal["1.0"] = "1.0"
    realization_id: str = Field(min_length=1)
    prediction_snapshot_id: str = Field(min_length=1)
    as_of: str
    observations: tuple[ComponentObservation, ...] = Field(min_length=1)

    @field_validator("as_of", mode="before")
    @classmethod
    def _normalize_as_of(cls, value: object) -> str:
        return _canonical_utc_datetime(value, field_name="as_of")

    @model_validator(mode="after")
    def _unique_observation_targets(self) -> Realization:
        seen: set[str] = set()
        duplicates: set[str] = set()
        for observation in self.observations:
            if observation.prediction_target_id in seen:
                duplicates.add(observation.prediction_target_id)
            seen.add(observation.prediction_target_id)
        if duplicates:
            joined = ", ".join(sorted(duplicates))
            raise ValueError(
                f"observations contains duplicate prediction_target_id values: {joined}"
            )
        return self

    model_config = _FROZEN_CONTRACT


def compute_outcome_contract_id(
    prediction_snapshot_id: str,
    realization_id: str | None = None,
) -> str:
    return str(
        uuid.uuid5(
            _NS_OUTCOME,
            f"outcome_contract|{prediction_snapshot_id}|{realization_id or 'pending'}",
        )
    )


class OutcomeContract(BaseModel):
    schema_version: Literal["1.0"] = "1.0"
    outcome_contract_id: str = Field(min_length=1)
    prediction_snapshot: PredictionSnapshot
    realization: Realization | None = None
    scorecard_id: str | None = Field(default=None, min_length=1)
    scorer_version: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _links_and_scorecard_are_coherent(self) -> OutcomeContract:
        expected_contract_id = compute_outcome_contract_id(
            self.prediction_snapshot.prediction_snapshot_id,
            self.realization.realization_id if self.realization is not None else None,
        )
        if self.outcome_contract_id != expected_contract_id:
            raise ValueError("outcome_contract_id does not match prediction/realization")
        if (
            self.realization is not None
            and self.realization.prediction_snapshot_id
            != self.prediction_snapshot.prediction_snapshot_id
        ):
            raise ValueError("realization.prediction_snapshot_id must match snapshot")
        if self.realization is not None:
            target_components = {
                target.prediction_target_id: target.component
                for target in self.prediction_snapshot.targets
            }
            for observation in self.realization.observations:
                expected_component = target_components.get(observation.prediction_target_id)
                if expected_component is None:
                    raise ValueError(
                        "realization observations must reference snapshot targets"
                    )
                if observation.component != expected_component:
                    raise ValueError(
                        "observation component must match prediction target component"
                    )
        if (self.scorecard_id is None) != (self.scorer_version is None):
            raise ValueError("scorecard_id and scorer_version must be set together")
        return self

    model_config = _FROZEN_CONTRACT


__all__ = [
    "ComponentObservation",
    "OutcomeContract",
    "Realization",
    "RealizedFundamentalMetric",
    "RealizedFundamentalsMetrics",
    "RealizedMarketRisk",
    "RealizedPayload",
    "RealizedReturn",
    "RealizedSentiment",
    "RealizedSentimentSignal",
    "RealizedThesisRiskOutcome",
    "RealizedValuationSnapshot",
    "ReturnDecomposition",
    "compute_outcome_contract_id",
]
