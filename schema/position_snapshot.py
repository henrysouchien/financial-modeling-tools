from __future__ import annotations

from typing import Literal

from pydantic import ConfigDict, Field, field_validator

from .handoff import HandoffArtifactV1_1
from .thesis import PositionSize
from .thesis_shared_slice import ThresholdValue, _ContractModel


class InvalidationBreach(_ContractModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        populate_by_name=True,
    )

    source_kind: Literal[
        "watch_item",
        "invalidation_trigger",
        "price_target",
        "exit_signal",
    ]
    ref_id: str
    metric: str | None = None
    threshold: ThresholdValue | None = None
    threshold_direction: Literal["above", "below"] | None = None
    observed_value: float | str | None = None
    breached: bool
    tier: Literal["live", "reported", "qualitative"]
    evaluated_at: str
    note: str | None = None


class PositionRuntimeState(_ContractModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        populate_by_name=True,
    )

    current_price: float
    position_size: PositionSize | None = None
    holding_period_days: int | None = None
    recent_invalidation_breaches: list[InvalidationBreach] = Field(default_factory=list)

    @field_validator("holding_period_days", mode="before")
    @classmethod
    def _normalize_holding_period_days(cls, value: object | None) -> int | None:
        if value is None or value == "":
            return None
        if isinstance(value, bool):
            return None
        if isinstance(value, float) and not value.is_integer():
            return None
        try:
            normalized = int(value)
        except (TypeError, ValueError):
            return None
        if normalized < 0:
            return None
        return normalized


class PositionSnapshot(_ContractModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
        populate_by_name=True,
    )

    handoff_artifact: HandoffArtifactV1_1 | None = None
    runtime: PositionRuntimeState


__all__ = [
    "InvalidationBreach",
    "PositionRuntimeState",
    "PositionSnapshot",
]
