from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator, model_validator

from ._insights_shared import _FROZEN_CONTRACT, Confidence
from .model_ref import ModelVersionRef


def _rank_is_dense_from_one(ranks: list[int], *, field_name: str) -> None:
    if ranks != list(range(1, len(ranks) + 1)):
        raise ValueError(f"{field_name}.rank must be dense [1..{len(ranks)}]; got {ranks}")


class PriceTargetRanges(BaseModel):
    low: float
    mid: float
    high: float

    @model_validator(mode="after")
    def _ordered(self) -> PriceTargetRanges:
        if not (self.low <= self.mid <= self.high):
            raise ValueError(
                f"ranges must satisfy low<=mid<=high; got ({self.low}, {self.mid}, {self.high})"
            )
        return self

    model_config = _FROZEN_CONTRACT


class PriceTargetDriverSensitivity(BaseModel):
    driver_key: str = Field(min_length=1)
    delta_per_pct: float
    rank: int = Field(ge=1)

    model_config = _FROZEN_CONTRACT


class CurrentPriceSnapshot(BaseModel):
    price: float = Field(gt=0)
    source: str = Field(min_length=1)
    provider: str | None = Field(default=None, min_length=1)
    as_of: str | None = Field(default=None, min_length=1)
    retrieved_at: str | None = Field(default=None, min_length=1)
    currency: str | None = Field(default=None, min_length=1)

    model_config = _FROZEN_CONTRACT


class ValuationHorizon(BaseModel):
    basis: Literal["fy1", "ntm", "calendar_12m", "explicit"]
    fiscal_period: str | None = Field(default=None, min_length=1)
    fiscal_year_end: str | None = Field(default=None, min_length=1)
    months_to_period_end: int | None = Field(default=None, ge=0, le=120)
    caveat: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _fy1_has_boundary_metadata(self) -> ValuationHorizon:
        if self.basis == "fy1" and (
            self.fiscal_period is None or self.months_to_period_end is None
            or self.fiscal_year_end is None
        ):
            raise ValueError(
                "valuation_horizon basis='fy1' requires fiscal_period, fiscal_year_end, "
                "and months_to_period_end"
            )
        return self

    model_config = _FROZEN_CONTRACT


class PriceTarget(BaseModel):
    price_target_id: str = Field(min_length=1)
    model_ref: ModelVersionRef
    as_of: str = Field(min_length=1)
    ranges: PriceTargetRanges
    confidence: Confidence
    method: str | None = Field(default=None, min_length=1)
    driver_sensitivities: list[PriceTargetDriverSensitivity] = Field(default_factory=list)
    time_horizon_months: int = Field(ge=1, le=120)
    current_price: float = Field(gt=0)
    current_price_snapshot: CurrentPriceSnapshot | None = None
    valuation_horizon: ValuationHorizon | None = None
    implied_return_pct: float
    schema_version: Literal["1.0"] = "1.0"

    @field_validator("driver_sensitivities")
    @classmethod
    def _rank_dense(
        cls,
        value: list[PriceTargetDriverSensitivity],
    ) -> list[PriceTargetDriverSensitivity]:
        if not value:
            return value
        _rank_is_dense_from_one(
            sorted(item.rank for item in value),
            field_name="driver_sensitivities",
        )
        return value

    @model_validator(mode="after")
    def _current_price_snapshot_matches_current_price(self) -> PriceTarget:
        if (
            self.current_price_snapshot is not None
            and abs(float(self.current_price_snapshot.price) - float(self.current_price)) > 1e-9
        ):
            raise ValueError("current_price_snapshot.price must equal current_price")
        return self

    model_config = _FROZEN_CONTRACT


__all__ = [
    "CurrentPriceSnapshot",
    "PriceTarget",
    "PriceTargetDriverSensitivity",
    "PriceTargetRanges",
    "ValuationHorizon",
]
