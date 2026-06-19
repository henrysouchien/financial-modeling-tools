from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, Field, field_validator

from ._insights_shared import _FROZEN_CONTRACT
from ._outcome_shared import OutcomeComponent
from .outcome_contract import _canonical_utc_datetime


class CalibrationBucket(BaseModel):
    confidence: str
    n: int = Field(ge=0)
    hit_rate: float | None = Field(default=None, allow_inf_nan=False)
    dispersion: float | None = Field(default=None, allow_inf_nan=False)

    model_config = _FROZEN_CONTRACT


class BuildingBlockContributor(BaseModel):
    skill: str = Field(min_length=1)
    co_occurrence_count: int = Field(ge=0)

    model_config = _FROZEN_CONTRACT


class MethodologyVerdict(BaseModel):
    component: OutcomeComponent
    metric: str
    n_total: int = Field(ge=0)
    n_scored: int = Field(ge=0)
    n_error: int = Field(ge=0)
    status: Literal["verdict", "insufficient_data"]
    n_min: int = Field(ge=1)
    bias: float | None = Field(default=None, allow_inf_nan=False)
    bias_median: float | None = Field(default=None, allow_inf_nan=False)
    hit_rate: float | None = Field(default=None, allow_inf_nan=False)
    dispersion: float | None = Field(default=None, allow_inf_nan=False)
    mean_score: float | None = Field(default=None, allow_inf_nan=False)
    calibration: tuple[CalibrationBucket, ...] = ()

    model_config = _FROZEN_CONTRACT


class MethodologyScorecard(BaseModel):
    schema_version: Literal["1.0"] = "1.0"
    methodology_name: str | None
    methodology_version: str
    as_of: str
    trade_filter: Literal["all", "traded"] = "all"
    verdicts: tuple[MethodologyVerdict, ...]
    prior_version: str | None = None

    @field_validator("as_of", mode="before")
    @classmethod
    def _normalize_as_of(cls, value: object) -> str:
        return _canonical_utc_datetime(value, field_name="as_of")

    model_config = _FROZEN_CONTRACT


__all__ = [
    "BuildingBlockContributor",
    "CalibrationBucket",
    "MethodologyScorecard",
    "MethodologyVerdict",
]
