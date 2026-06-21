from __future__ import annotations

import hashlib
import json
import re
import uuid
from collections.abc import Iterable, Mapping
from datetime import date, datetime, timezone
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from ._insights_shared import Confidence, _FROZEN_CONTRACT
from ._outcome_shared import (
    EarningsBasis,
    MarketRiskFrequency,
    MarketRiskMetric,
    OneTimeHandling,
    OutcomeComponent,
    RestatementPolicy,
    ReturnStatedKind,
    SentimentMeasure,
    ValuationBasisKind,
    ValuationMultipleKind,
)
from ._uuid import _NS_OUTCOME
from .model_ref import ModelVersionRef
from .thesis_shared_slice import SourceId


_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_JSON_SEPARATORS = (",", ":")


def _canonical_json(payload: Any) -> str:
    return json.dumps(
        payload,
        sort_keys=True,
        separators=_JSON_SEPARATORS,
        ensure_ascii=True,
        allow_nan=False,
    )


def _canonical_utc_datetime(value: object, *, field_name: str) -> str:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, date):
        raise ValueError(f"{field_name} must be a timezone-aware ISO datetime")
    elif isinstance(value, str):
        stripped = value.strip()
        if _DATE_RE.fullmatch(stripped):
            raise ValueError(f"{field_name} must be a timezone-aware ISO datetime")
        parse_value = stripped[:-1] + "+00:00" if stripped.endswith("Z") else stripped
        try:
            parsed = datetime.fromisoformat(parse_value)
        except ValueError as exc:
            raise ValueError(
                f"{field_name} must be a timezone-aware ISO datetime"
            ) from exc
    else:
        raise ValueError(f"{field_name} must be a timezone-aware ISO datetime")

    if parsed.tzinfo is None or parsed.utcoffset() is None:
        raise ValueError(f"{field_name} must include timezone information")
    if parsed.microsecond:
        raise ValueError(f"{field_name} must use whole-second precision")
    return parsed.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _canonical_resolution(value: object | None) -> str | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return _canonical_utc_datetime(value, field_name="resolves_at")
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, str):
        stripped = value.strip()
        if _DATE_RE.fullmatch(stripped):
            return stripped
        return _canonical_utc_datetime(stripped, field_name="resolves_at")
    raise ValueError("resolves_at must be an ISO date or timezone-aware ISO datetime")


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


class KeyValuePair(BaseModel):
    key: str = Field(min_length=1)
    value: str = Field(min_length=1)

    model_config = _FROZEN_CONTRACT


class ValueRange(BaseModel):
    low: float = Field(allow_inf_nan=False)
    mid: float = Field(allow_inf_nan=False)
    high: float = Field(allow_inf_nan=False)

    @model_validator(mode="after")
    def _ordered(self) -> ValueRange:
        if not (self.low <= self.mid <= self.high):
            raise ValueError("range must satisfy low<=mid<=high")
        return self

    model_config = _FROZEN_CONTRACT


class ValueThreshold(BaseModel):
    value: float = Field(allow_inf_nan=False)
    direction: Literal["above", "below"]

    model_config = _FROZEN_CONTRACT


class ValueEvent(BaseModel):
    trigger_when: Literal["occurs", "does_not_occur"] = "occurs"

    model_config = _FROZEN_CONTRACT


class ScoreableValue(BaseModel):
    kind: Literal["point", "range", "threshold", "event"]
    point: float | None = Field(default=None, allow_inf_nan=False)
    range: ValueRange | None = None
    threshold: ValueThreshold | None = None
    event: ValueEvent | None = None

    @model_validator(mode="after")
    def _exactly_one_shape(self) -> ScoreableValue:
        shapes = {
            "point": self.point,
            "range": self.range,
            "threshold": self.threshold,
            "event": self.event,
        }
        for name, value in shapes.items():
            if name == self.kind and value is None:
                raise ValueError(f"ScoreableValue.kind={self.kind!r} requires {name}")
            if name != self.kind and value is not None:
                raise ValueError(
                    f"ScoreableValue.kind={self.kind!r} forbids {name}"
                )
        return self

    model_config = _FROZEN_CONTRACT


class PredictionPeriod(BaseModel):
    kind: Literal["fiscal", "calendar"]
    label: str = Field(min_length=1)

    model_config = _FROZEN_CONTRACT


class PredictionHorizon(BaseModel):
    as_of: str
    resolves_at: str | None = Field(default=None, min_length=1)
    time_horizon_months: int | None = Field(default=None, ge=1, le=120)

    @field_validator("as_of", mode="before")
    @classmethod
    def _normalize_as_of(cls, value: object) -> str:
        return _canonical_utc_datetime(value, field_name="as_of")

    @field_validator("resolves_at", mode="before")
    @classmethod
    def _normalize_resolves_at(cls, value: object | None) -> str | None:
        return _canonical_resolution(value)

    @model_validator(mode="after")
    def _has_resolution_rule(self) -> PredictionHorizon:
        if self.resolves_at is None and self.time_horizon_months is None:
            raise ValueError("PredictionHorizon requires resolves_at or time_horizon_months")
        return self

    model_config = _FROZEN_CONTRACT


class ThesisDerivedRef(BaseModel):
    field_path: str = Field(min_length=1)
    ref_kind: Literal["object_id", "list_index", "singleton"]
    object_id: str | None = Field(default=None, min_length=1)
    item_index: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def _ref_kind_matches_payload(self) -> ThesisDerivedRef:
        if self.ref_kind == "object_id":
            if self.object_id is None or self.item_index is not None:
                raise ValueError("object_id ref requires object_id only")
        elif self.ref_kind == "list_index":
            if self.item_index is None or self.object_id is not None:
                raise ValueError("list_index ref requires item_index only")
        elif self.object_id is not None or self.item_index is not None:
            raise ValueError("singleton ref forbids object_id and item_index")
        return self

    model_config = _FROZEN_CONTRACT


class ConditioningRef(BaseModel):
    thesis_version: int = Field(ge=1)
    model_build_context_id: str | None = Field(default=None, min_length=1)
    model_build_context_version: int | None = Field(default=None, ge=1)

    @model_validator(mode="after")
    def _model_context_fields_move_together(self) -> ConditioningRef:
        if (self.model_build_context_id is None) != (
            self.model_build_context_version is None
        ):
            raise ValueError(
                "model_build_context_version is set iff model_build_context_id is set"
            )
        return self

    model_config = _FROZEN_CONTRACT


class ReturnBasis(BaseModel):
    stated_kind: ReturnStatedKind
    benchmark_id: str | None = Field(default=None, min_length=1)
    dividends_included: bool
    split_adjusted: bool
    buyback_adjusted: bool
    currency: str = Field(min_length=1)

    @model_validator(mode="after")
    def _benchmark_matches_kind(self) -> ReturnBasis:
        if self.stated_kind == "benchmark_relative" and self.benchmark_id is None:
            raise ValueError("benchmark_relative return basis requires benchmark_id")
        if self.stated_kind != "benchmark_relative" and self.benchmark_id is not None:
            raise ValueError("benchmark_id is only valid for benchmark_relative returns")
        return self

    model_config = _FROZEN_CONTRACT


class ValuationBasis(BaseModel):
    multiple_kind: ValuationMultipleKind
    forward_vs_trailing: ValuationBasisKind
    earnings_basis: EarningsBasis

    model_config = _FROZEN_CONTRACT


class FundamentalBasis(BaseModel):
    concept_id: str = Field(min_length=1)
    fiscal_period: str = Field(min_length=1)
    earnings_basis: EarningsBasis | None = None
    restatement_policy: RestatementPolicy
    one_time_handling: OneTimeHandling

    model_config = _FROZEN_CONTRACT


class SentimentBasis(BaseModel):
    signal_kind: str = Field(min_length=1)
    measure: SentimentMeasure

    model_config = _FROZEN_CONTRACT


class MarketRiskBasis(BaseModel):
    risk_metric: MarketRiskMetric
    window_months: int = Field(ge=1, le=120)
    frequency: MarketRiskFrequency
    annualized: bool
    benchmark_id: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _benchmark_matches_metric(self) -> MarketRiskBasis:
        if self.risk_metric == "beta" and self.benchmark_id is None:
            raise ValueError("beta market risk basis requires benchmark_id")
        if self.risk_metric != "beta" and self.benchmark_id is not None:
            raise ValueError("benchmark_id is only valid for beta market risk")
        return self

    model_config = _FROZEN_CONTRACT


MeasurementBasis = (
    ReturnBasis
    | ValuationBasis
    | FundamentalBasis
    | SentimentBasis
    | MarketRiskBasis
)


class ProducedBy(BaseModel):
    skill_name: str = Field(min_length=1)
    skill_version: str = Field(min_length=1)
    methodology_name: str | None = Field(default=None, min_length=1)
    methodology_version: str | None = Field(default=None, min_length=1)
    llm_model_id: str | None = Field(default=None, min_length=1)
    model_ref: ModelVersionRef | None = None
    tool_versions: tuple[KeyValuePair, ...] = ()
    run_id: str | None = Field(default=None, min_length=1)
    produced_at: str

    @field_validator("produced_at", mode="before")
    @classmethod
    def _normalize_produced_at(cls, value: object) -> str:
        return _canonical_utc_datetime(value, field_name="produced_at")

    @model_validator(mode="after")
    def _unique_tool_versions(self) -> ProducedBy:
        _reject_duplicate_keys(self.tool_versions, field_name="tool_versions")
        return self

    model_config = _FROZEN_CONTRACT


class PredictionTarget(BaseModel):
    prediction_target_id: str = Field(min_length=1)
    component: OutcomeComponent
    metric: str = Field(min_length=1)
    basis: MeasurementBasis | None = None
    value: ScoreableValue
    period: PredictionPeriod
    horizon: PredictionHorizon
    confidence: Confidence | None = None
    scoreability: Literal["scoreable", "needs_threshold", "prose_only"]
    derived_from: ThesisDerivedRef | None = None
    derivation_metadata: dict[str, Any] | None = None
    produced_by: ProducedBy | None = None
    source_refs: tuple[SourceId, ...] = ()

    @model_validator(mode="after")
    def _component_basis_and_value_rules(self) -> PredictionTarget:
        if self.component == "return":
            self._require_basis(ReturnBasis)
        elif self.component == "valuation":
            self._require_basis(ValuationBasis)
        elif self.component == "fundamentals":
            self._require_basis(FundamentalBasis)
        elif self.component == "sentiment":
            self._require_basis(SentimentBasis)
        elif self.component == "market_risk":
            self._require_basis(MarketRiskBasis)
        elif self.component == "thesis_risk" and self.basis is not None:
            if not isinstance(self.basis, _BASIS_TYPES):
                raise ValueError("thesis_risk basis must be a known measurement basis")

        expected_metric = (
            self.metric if self.component == "thesis_risk" and self.basis is None
            else _basis_metric(self.basis)
        )
        if expected_metric is not None and self.metric != expected_metric:
            raise ValueError(
                f"{self.component} metric must align with basis selector "
                f"{expected_metric!r}"
            )

        if self.component == "thesis_risk" and self.scoreability == "scoreable":
            expected_kind = "event" if self.basis is None else "threshold"
            if self.value.kind != expected_kind:
                raise ValueError(
                    "scoreable thesis_risk targets require "
                    f"value.kind={expected_kind!r}"
                )
        if self.value.kind == "event" and not (
            self.component == "thesis_risk" and self.basis is None
        ):
            raise ValueError("event values are only valid for thesis_risk event triggers")
        return self

    def _require_basis(self, basis_type: type[MeasurementBasis]) -> None:
        if not isinstance(self.basis, basis_type):
            raise ValueError(
                f"{self.component} target requires {basis_type.__name__}"
            )

    model_config = _FROZEN_CONTRACT


_BASIS_TYPES = (
    ReturnBasis,
    ValuationBasis,
    FundamentalBasis,
    SentimentBasis,
    MarketRiskBasis,
)


def _basis_metric(basis: MeasurementBasis | None) -> str | None:
    if basis is None:
        return None
    if isinstance(basis, ReturnBasis):
        return "return"
    if isinstance(basis, ValuationBasis):
        return basis.multiple_kind
    if isinstance(basis, FundamentalBasis):
        return basis.concept_id
    if isinstance(basis, SentimentBasis):
        return basis.signal_kind
    if isinstance(basis, MarketRiskBasis):
        return basis.risk_metric
    raise TypeError(f"Unsupported basis type: {type(basis).__name__}")


_TARGET_ID_EXCLUDE = {
    "prediction_target_id",
    "confidence",
    "produced_by",
    "source_refs",
}
_CONTENT_HASH_EXCLUDE = _TARGET_ID_EXCLUDE | {"derived_from"}


def _coerce_target(target: PredictionTarget | Mapping[str, Any]) -> PredictionTarget:
    if isinstance(target, PredictionTarget):
        return target
    if isinstance(target, Mapping):
        data = dict(target)
        data.setdefault("prediction_target_id", "__pending__")
        return PredictionTarget.model_validate(data)
    raise TypeError("target must be a PredictionTarget or mapping")


def _target_dump(
    target: PredictionTarget | Mapping[str, Any],
    *,
    exclude: set[str],
) -> dict[str, Any]:
    target_model = _coerce_target(target)
    return target_model.model_dump(
        mode="json",
        exclude_none=True,
        by_alias=False,
        exclude=exclude,
    )


def compute_prediction_target_id(
    research_file_id: int,
    target_without_id: PredictionTarget | Mapping[str, Any],
) -> str:
    identity = {
        "research_file_id": research_file_id,
        **_target_dump(target_without_id, exclude=_TARGET_ID_EXCLUDE),
    }
    serialized = _canonical_json(identity)
    return str(uuid.uuid5(_NS_OUTCOME, "prediction_target|" + serialized))


def compute_content_hash(
    targets: Iterable[PredictionTarget | Mapping[str, Any]],
) -> str:
    serialized_targets = [
        _canonical_json(_target_dump(target, exclude=_CONTENT_HASH_EXCLUDE))
        for target in targets
    ]
    payload = "[" + ",".join(sorted(serialized_targets)) + "]"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def compute_prediction_snapshot_id(
    research_file_id: int,
    as_of: str,
    content_hash: str,
) -> str:
    canonical_as_of = _canonical_utc_datetime(as_of, field_name="as_of")
    return str(
        uuid.uuid5(
            _NS_OUTCOME,
            f"prediction_snapshot|{research_file_id}|{canonical_as_of}|{content_hash}",
        )
    )


class PredictionSnapshot(BaseModel):
    schema_version: Literal["1.0"] = "1.0"
    prediction_snapshot_id: str = Field(min_length=1)
    research_file_id: int = Field(ge=1)
    ticker: str = Field(min_length=1)
    as_of: str
    targets: tuple[PredictionTarget, ...] = Field(min_length=1)
    produced_by: ProducedBy
    conditioning_ref: ConditioningRef | None = None
    content_hash: str = Field(min_length=1)
    supersedes: str | None = Field(default=None, min_length=1)

    @field_validator("as_of", mode="before")
    @classmethod
    def _normalize_as_of(cls, value: object) -> str:
        return _canonical_utc_datetime(value, field_name="as_of")

    @model_validator(mode="after")
    def _identity_surfaces_match(self) -> PredictionSnapshot:
        expected_content_hash = compute_content_hash(self.targets)
        if self.content_hash != expected_content_hash:
            raise ValueError("content_hash does not match targets")

        expected_snapshot_id = compute_prediction_snapshot_id(
            self.research_file_id,
            self.as_of,
            self.content_hash,
        )
        if self.prediction_snapshot_id != expected_snapshot_id:
            raise ValueError("prediction_snapshot_id does not match content hash")

        seen_target_ids: set[str] = set()
        for target in self.targets:
            if target.prediction_target_id in seen_target_ids:
                raise ValueError("duplicate prediction_target_id")
            seen_target_ids.add(target.prediction_target_id)

            expected_target_id = compute_prediction_target_id(
                self.research_file_id,
                target,
            )
            if target.prediction_target_id != expected_target_id:
                raise ValueError(
                    f"prediction_target_id does not match target: {target.metric}"
                )
            if target.horizon.as_of != self.as_of:
                raise ValueError("target.horizon.as_of must equal snapshot as_of")
        return self

    model_config = _FROZEN_CONTRACT


__all__ = [
    "ConditioningRef",
    "FundamentalBasis",
    "KeyValuePair",
    "MarketRiskBasis",
    "MeasurementBasis",
    "PredictionHorizon",
    "PredictionPeriod",
    "PredictionSnapshot",
    "PredictionTarget",
    "ProducedBy",
    "ReturnBasis",
    "ScoreableValue",
    "SentimentBasis",
    "ThesisDerivedRef",
    "ValueEvent",
    "ValueRange",
    "ValueThreshold",
    "ValuationBasis",
    "compute_content_hash",
    "compute_prediction_snapshot_id",
    "compute_prediction_target_id",
]
