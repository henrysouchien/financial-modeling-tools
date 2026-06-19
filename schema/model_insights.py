from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import BaseModel, Field, field_validator

from ._insights_shared import DriverCategory, DriverUnit, Severity, _FROZEN_CONTRACT
from .handoff_patch import HandoffPatchOp
from .model_ref import ModelVersionRef
from .thesis_shared_slice import SourceId


class DriverSensitivity(BaseModel):
    driver_key: str = Field(min_length=1)
    target_metric: str = Field(min_length=1)
    impact_per_unit: float | None = None
    rank: int | None = Field(default=None, ge=1)
    periods: list[int] | None = None
    computation_status: Literal[
        "computed",
        "pinned_by_policy",
        "unresolved",
        "not_upstream",
        "unavailable",
        "legacy_diagnostic",
    ] = "computed"
    sensitivity_mode: Literal[
        "semantic_driver",
        "workbook_explicit",
        "workbook_global",
        "legacy_global",
    ] | None = None
    recompute_policy: Literal["projection_safe", "legacy_global"] | None = None
    shocked_item_id: str | None = None
    target_period: int | None = None
    bump_pct: float | None = None
    impact_basis: str | None = None
    candidate_scope: Literal[
        "semantic_driver_keys",
        "explicit_workbook_ids",
        "global_ranked",
        "mbc_drivers",
        "legacy_global",
    ] | None = None
    readout_item_ids: list[str] = Field(default_factory=list)
    display_label: str | None = None
    impact_direction: Literal["positive", "negative", "neutral", "mixed", "unknown"] | None = None
    expected_direction: Literal["positive", "negative", "neutral", "mixed", "unknown"] | None = None
    expected_moved_item_ids: list[str] = Field(default_factory=list)
    expected_pinned_item_ids: list[str] = Field(default_factory=list)
    temporal_pattern: Literal[
        "immediate",
        "lagged",
        "ramp",
        "fade",
        "capped",
        "cyclical",
        "terminal",
        "unknown",
    ] | None = None
    reconciliation_status: Literal[
        "matched_prior",
        "contradicts_prior",
        "requires_review",
        "not_evaluated",
        "legacy_unreviewed",
    ] | None = None
    judgment_notes: str | None = None
    warnings: list[dict[str, Any]] = Field(default_factory=list)

    model_config = _FROZEN_CONTRACT


class ImpliedAssumption(BaseModel):
    driver_key: str = Field(min_length=1)
    sia_category: DriverCategory | None = None
    value: float
    unit: DriverUnit
    rationale: str = Field(min_length=1)
    suggests_patch: bool = False

    @field_validator("sia_category", mode="before")
    @classmethod
    def _canonicalize_sia_category(cls, value: object | None) -> object | None:
        if value is None:
            return None
        return re.sub(r"[\s\-_]+", "_", str(value).strip()).lower()

    model_config = _FROZEN_CONTRACT


class RiskSurfaced(BaseModel):
    description: str = Field(min_length=1)
    severity: Severity
    type: str | None = None
    evidence: list[SourceId] = Field(default_factory=list)

    model_config = _FROZEN_CONTRACT


class ModelInsights(BaseModel):
    model_insights_id: str = Field(min_length=1)
    model_ref: ModelVersionRef
    model_build_context_id: str = Field(min_length=1)
    generated_at: str = Field(min_length=1)
    driver_sensitivities: list[DriverSensitivity] = Field(default_factory=list)
    implied_assumptions: list[ImpliedAssumption] = Field(default_factory=list)
    risks_surfaced: list[RiskSurfaced] = Field(default_factory=list)
    handoff_patch_suggestions: list[HandoffPatchOp] = Field(default_factory=list)
    schema_version: Literal["1.0", "1.1"] = "1.1"

    @field_validator("driver_sensitivities")
    @classmethod
    def _rank_is_dense_from_one(
        cls,
        value: list[DriverSensitivity],
    ) -> list[DriverSensitivity]:
        if not value:
            return value
        for item in value:
            if item.computation_status != "computed" and item.rank is not None:
                raise ValueError("non-computed driver_sensitivities rows must not be ranked")
            if item.computation_status == "computed" and (
                item.impact_per_unit is None or item.rank is None
            ):
                raise ValueError("computed driver_sensitivities rows require impact_per_unit and rank")
        ranks = sorted(
            item.rank
            for item in value
            if item.computation_status == "computed" and item.rank is not None
        )
        if not ranks:
            return value
        if ranks != list(range(1, len(ranks) + 1)):
            raise ValueError(
                f"driver_sensitivities.rank must be dense [1..{len(ranks)}]; got {ranks}"
            )
        return value

    model_config = _FROZEN_CONTRACT


__all__ = [
    "DriverSensitivity",
    "ImpliedAssumption",
    "ModelInsights",
    "ModelVersionRef",
    "RiskSurfaced",
]
