from __future__ import annotations

import logging
from copy import deepcopy
from typing import Literal, TypeAlias

from pydantic import Field, field_validator, model_validator

from .numeric_display import NumericDisplaySpec
from industry_slice_core.thesis_shared_industry import IndustryPeerComparison, OperatingComparison
from industry_slice_core._contract import _ContractModel


logger = logging.getLogger(__name__)

CompsScalar: TypeAlias = str | int | float
CompsNoteConfidence = Literal["HIGH", "MEDIUM", "LOW"]
ComparativeVerdict = Literal["STRATEGY_TRACKING", "STRATEGY_DRIFTING"]


class LegacyUnitsDetectedError(ValueError):
    """Raised when an obsolete ``units`` carrier reaches a guarded boundary."""


def strip_legacy_units(raw: object) -> object:
    """Deep-copy clean data and reject obsolete persisted ``units`` carriers."""

    copied = deepcopy(raw)
    stripped_count = 0

    def _strip(value: object) -> object:
        nonlocal stripped_count
        if isinstance(value, dict):
            if "units" in value:
                value.pop("units")
                stripped_count += 1
            for key, child in value.items():
                value[key] = _strip(child)
        elif isinstance(value, list):
            for index, child in enumerate(value):
                value[index] = _strip(child)
        elif isinstance(value, tuple):
            value = tuple(_strip(child) for child in value)
        return value

    stripped = _strip(copied)
    if stripped_count:
        payload_type = type(raw).__name__
        logger.error(
            "legacy units detected: stripped_count=%d payload_type=%s",
            stripped_count,
            payload_type,
            extra={
                "stripped_count": stripped_count,
                "payload_type": payload_type,
            },
        )
        raise LegacyUnitsDetectedError(
            "legacy units detected: "
            f"stripped_count={stripped_count} payload_type={payload_type}"
        )
    return stripped


class CompsNoteSourceArtifact(_ContractModel):
    skill: Literal["comparative-analysis"] = "comparative-analysis"
    artifact_id: str = Field(..., min_length=1)
    ticker: str = Field(..., min_length=1)
    content_sha256: str = Field(..., min_length=64, max_length=64)
    dashboard_artifact_id: str = Field(..., min_length=1)
    created_at: str | None = None
    #: The artifact_id the source sidecar's own bytes record, when it differs
    #: from ``artifact_id`` (the id the note selected and found it stored under):
    #: a sidecar copied to another id still verifies its content hash, so the
    #: manifest names both rather than one.
    embedded_artifact_id: str | None = None

    @field_validator("ticker")
    @classmethod
    def _uppercase_ticker(cls, value: str) -> str:
        return value.upper()


class CompsNoteBottomLine(_ContractModel):
    action: str = Field(..., min_length=1)
    status: str = Field(..., min_length=1)
    summary: str = Field(..., min_length=1)
    source_refs: list[str] = Field(default_factory=list)


class CompsNoteSnapshotItem(_ContractModel):
    label: str = Field(..., min_length=1)
    value: CompsScalar
    detail: str | None = None
    status: str | None = None
    source_refs: list[str] = Field(default_factory=list)


class CompsNoteMetricRow(_ContractModel):
    key: str = Field(..., min_length=1)
    label: str = Field(..., min_length=1)
    category: Literal["Operating", "Size", "Leverage"]
    display: NumericDisplaySpec | None = None
    values: dict[str, CompsScalar | None]
    median: float | int | None = None


class CompsNoteMetricExhibit(_ContractModel):
    as_of: str = Field(..., min_length=1)
    tickers: list[str] = Field(..., min_length=4)
    rows: list[CompsNoteMetricRow] = Field(..., min_length=1)


class CompsNoteTrendRow(_ContractModel):
    ticker: str = Field(..., min_length=1)
    values: dict[int, CompsScalar | None]

    @field_validator("ticker")
    @classmethod
    def _uppercase_ticker(cls, value: str) -> str:
        return value.upper()


class CompsNoteTrendExhibit(_ContractModel):
    key: str = Field(..., min_length=1)
    label: str = Field(..., min_length=1)
    display: NumericDisplaySpec | None = None
    years: list[int] = Field(..., min_length=3, max_length=5)
    rows: list[CompsNoteTrendRow] = Field(..., min_length=4)
    median_values: dict[int, float | int | None]


class CompsNoteGuardrail(_ContractModel):
    metric: str = Field(..., min_length=1)
    direction: str = Field(..., min_length=1)
    threshold: CompsScalar
    rationale: str = Field(..., min_length=1)
    source_refs: list[str] = Field(default_factory=list)


class CompsNoteInflectionAlert(_ContractModel):
    alert_id: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)
    severity: str = Field(..., min_length=1)
    expected_date: str | None = None


class CompsNoteDataGap(_ContractModel):
    gap_id: str = Field(..., min_length=1)
    severity: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)


class CompsNotePack(_ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    ticker: str = Field(..., min_length=1)
    company_name: str = Field(..., min_length=1)
    verdict: Literal["COMPS_NOTE_COMPILED"] = "COMPS_NOTE_COMPILED"
    comparative_verdict: ComparativeVerdict
    confidence: CompsNoteConfidence
    confidence_score: float = Field(..., ge=0, le=1)
    as_of: str = Field(..., min_length=1)
    industry_key: str = Field(..., min_length=1)
    subtitle: str = Field(..., min_length=1)
    bottom_line: CompsNoteBottomLine
    snapshot: list[CompsNoteSnapshotItem] = Field(default_factory=list)
    critical_kpis: list[str] = Field(..., min_length=1)
    peer_comparison: IndustryPeerComparison
    operating_comparison: OperatingComparison
    metric_exhibit: CompsNoteMetricExhibit
    trend_exhibit: CompsNoteTrendExhibit
    guardrails: list[CompsNoteGuardrail] = Field(default_factory=list)
    inflection_alerts: list[CompsNoteInflectionAlert] = Field(default_factory=list)
    decision_context: str = Field(..., min_length=1)
    data_gaps: list[CompsNoteDataGap] = Field(default_factory=list)
    source_manifest: list[CompsNoteSourceArtifact] = Field(
        ..., min_length=1, max_length=1
    )
    recommended_next_action: str = Field(..., min_length=1)

    @field_validator("ticker")
    @classmethod
    def _uppercase_ticker(cls, value: str) -> str:
        return value.upper()

    @model_validator(mode="after")
    def _validate_compiled_pack(self) -> "CompsNotePack":
        peers = self.peer_comparison.peers
        if len(peers) < 4:
            raise ValueError(
                "compiled comps note requires focal plus at least three external peers"
            )
        if peers[0].ticker != self.ticker:
            raise ValueError("peer_comparison must place the focal ticker first")
        if self.metric_exhibit.tickers != [peer.ticker for peer in peers]:
            raise ValueError("metric_exhibit tickers must match peer_comparison order")
        if [row.ticker for row in self.trend_exhibit.rows] != [
            peer.ticker for peer in peers
        ]:
            raise ValueError("trend_exhibit rows must match peer_comparison order")
        if self.source_manifest[0].ticker != self.ticker:
            raise ValueError("source_manifest ticker must match pack ticker")
        if self.peer_comparison.industry_key != self.industry_key:
            raise ValueError(
                "peer_comparison industry_key must match pack industry_key"
            )
        if self.operating_comparison.industry_key != self.industry_key:
            raise ValueError(
                "operating_comparison industry_key must match pack industry_key"
            )
        return self


__all__ = [
    "ComparativeVerdict",
    "CompsNoteBottomLine",
    "CompsNoteConfidence",
    "CompsNoteDataGap",
    "CompsNoteGuardrail",
    "CompsNoteInflectionAlert",
    "CompsNoteMetricExhibit",
    "CompsNoteMetricRow",
    "CompsNotePack",
    "CompsNoteSnapshotItem",
    "CompsNoteSourceArtifact",
    "CompsNoteTrendExhibit",
    "CompsNoteTrendRow",
    "LegacyUnitsDetectedError",
    "strip_legacy_units",
]
