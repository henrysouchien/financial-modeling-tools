from __future__ import annotations

from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from industry_slice_core._contract import _ContractModel


PreMortemConfidence = Literal["HIGH", "MEDIUM", "LOW"]
PreMortemSourceVerdict = Literal["PRE_MORTEM_COMPLETED", "PRE_MORTEM_UNDER_SCOPED"]


class PreMortemSourceArtifact(_ContractModel):
    skill: Literal["thesis-pre-mortem"] = "thesis-pre-mortem"
    artifact_id: str = Field(..., min_length=1)
    ticker: str = Field(..., min_length=1)
    content_sha256: str = Field(..., min_length=64, max_length=64)
    created_at: str | None = None

    @field_validator("ticker")
    @classmethod
    def _uppercase_ticker(cls, value: str) -> str:
        return value.upper()


class PreMortemCompileRequest(_ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    ticker: str = Field(..., min_length=1)
    source_artifact: PreMortemSourceArtifact

    @field_validator("ticker")
    @classmethod
    def _uppercase_ticker(cls, value: str) -> str:
        return value.upper()

    @model_validator(mode="after")
    def _matching_ticker(self) -> "PreMortemCompileRequest":
        if self.source_artifact.ticker != self.ticker:
            raise ValueError("source_artifact ticker must match request ticker")
        return self


class PreMortemBottomLine(_ContractModel):
    action: str = Field(..., min_length=1)
    summary: str = Field(..., min_length=1)


class PreMortemFailureSurface(_ContractModel):
    surface: str = Field(..., min_length=1)
    failure_sentence: str = Field(..., min_length=1)
    trigger: str = Field(..., min_length=1)


class PreMortemTrigger(_ContractModel):
    trigger_id: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)
    metric: str = Field(..., min_length=1)
    threshold: str | int | float
    threshold_direction: Literal["above", "below"]
    source_refs: list[str] = Field(..., min_length=1)


class PreMortemRisk(_ContractModel):
    risk_id: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)
    severity: str = Field(..., min_length=1)
    type: str = Field(..., min_length=1)


class PreMortemMethodologyScreen(_ContractModel):
    applied: list[str] = Field(default_factory=list)
    candidate_count: int = Field(..., ge=0)
    rows: list[dict[str, Any]] = Field(default_factory=list)


class PreMortemModelInsightsMerge(_ContractModel):
    available: bool
    model_insights_id: str | None = None
    model_id: str | None = None
    model_version: int | None = Field(default=None, ge=1)
    risks_surfaced_merged: list[dict[str, Any]] = Field(default_factory=list)


class PreMortemDataGap(_ContractModel):
    gap_id: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)


class PreMortemPack(_ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    genre: Literal["pre_mortem"] = "pre_mortem"
    ticker: str = Field(..., min_length=1)
    verdict: Literal["PRE_MORTEM_COMPILED"] = "PRE_MORTEM_COMPILED"
    source_verdict: PreMortemSourceVerdict
    confidence: PreMortemConfidence
    as_of: str = Field(..., min_length=1)
    subtitle: str = Field(..., min_length=1)
    bottom_line: PreMortemBottomLine
    failure_surfaces: list[PreMortemFailureSurface] = Field(..., min_length=1)
    invalidation_triggers: list[PreMortemTrigger] = Field(..., min_length=1)
    risks: list[PreMortemRisk] = Field(..., min_length=1)
    methodology_evidence_screen: PreMortemMethodologyScreen
    model_insights_merge: PreMortemModelInsightsMerge
    data_gaps: list[PreMortemDataGap] = Field(default_factory=list)
    source_manifest: list[PreMortemSourceArtifact] = Field(..., min_length=1, max_length=1)
    recommended_next_action: str = Field(..., min_length=1)

    @field_validator("ticker")
    @classmethod
    def _uppercase_ticker(cls, value: str) -> str:
        return value.upper()

    @model_validator(mode="after")
    def _validate_compiled_pack(self) -> "PreMortemPack":
        if self.source_manifest[0].ticker != self.ticker:
            raise ValueError("source_manifest ticker must match pack ticker")
        if self.source_verdict == "PRE_MORTEM_COMPLETED" and (
            len(self.failure_surfaces) < 2
            or len(self.invalidation_triggers) < 2
            or len(self.risks) < 2
        ):
            raise ValueError("completed pre-mortem requires multiple failure surfaces, triggers, and risks")
        return self


__all__ = [
    "PreMortemBottomLine",
    "PreMortemCompileRequest",
    "PreMortemConfidence",
    "PreMortemDataGap",
    "PreMortemFailureSurface",
    "PreMortemMethodologyScreen",
    "PreMortemModelInsightsMerge",
    "PreMortemPack",
    "PreMortemRisk",
    "PreMortemSourceArtifact",
    "PreMortemSourceVerdict",
    "PreMortemTrigger",
]
