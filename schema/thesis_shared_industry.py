from __future__ import annotations

from typing import Annotated, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, StringConstraints, field_validator

from .enum_canonicalizers import canonicalize_optional_time_horizon
from .thesis_shared_slice_helpers import _normalize_ticker


SourceId = Annotated[
    str, StringConstraints(strip_whitespace=True, pattern=r"^src_[1-9]\d*$")
]
ScalarValue: TypeAlias = str | int | float | bool


class _ContractModel(BaseModel):
    model_config = ConfigDict(
        extra="forbid", str_strip_whitespace=True, populate_by_name=True
    )


class IndustryLandscape(_ContractModel):
    narrative: str
    citations: list[SourceId] = Field(default_factory=list)


class CompsNarrative(_ContractModel):
    narrative: str
    citations: list[SourceId] = Field(default_factory=list)


class IndustryPeerComparisonPeer(_ContractModel):
    ticker: str
    name: str
    key_metrics: dict[str, ScalarValue] | None = None
    relative_position: str | None = None
    source_refs: list[SourceId] = Field(default_factory=list)

    @field_validator("ticker", mode="before")
    @classmethod
    def _validate_ticker(cls, value: object) -> str:
        return _normalize_ticker(value)


class EditorialPeer(_ContractModel):
    ticker: str
    name: str
    source: Literal["editorial"] = "editorial"
    added_by: str | None = None
    added_at: str | None = None
    rationale: str | None = None

    @field_validator("ticker", mode="before")
    @classmethod
    def _validate_ticker(cls, value: object) -> str:
        return _normalize_ticker(value)


class CompMetricCell(_ContractModel):
    value: ScalarValue | None = None
    source_refs: list[SourceId] = Field(default_factory=list)
    derived: bool = False


class SnapshotMetric(_ContractModel):
    key: str = Field(min_length=1)
    label: str
    units: str | None = None
    values: dict[str, CompMetricCell] = Field(default_factory=dict)
    median: CompMetricCell | None = None


class SnapshotSection(_ContractModel):
    name: str
    metrics: list[SnapshotMetric] = Field(default_factory=list)


class TimeseriesMetric(_ContractModel):
    key: str = Field(min_length=1)
    label: str
    units: str | None = None
    series: dict[str, dict[int, CompMetricCell]] = Field(default_factory=dict)
    median_series: dict[int, CompMetricCell] = Field(default_factory=dict)


class TimeseriesGroup(_ContractModel):
    name: str
    metrics: list[TimeseriesMetric] = Field(default_factory=list)


class OperatingComparison(_ContractModel):
    industry_key: str
    template_manifest_id: str
    years: list[int] = Field(default_factory=list)
    metric_groups: list[TimeseriesGroup] = Field(default_factory=list)


class IndustryPeerComparison(_ContractModel):
    peers: list[IndustryPeerComparisonPeer] = Field(default_factory=list)
    industry_key: str | None = None
    template_manifest_id: str | None = None
    as_of: str | None = None
    sections: list[SnapshotSection] = Field(default_factory=list)


class MacroOverlayDriver(_ContractModel):
    description: str
    sensitivity: str | None = None
    source_refs: list[SourceId] = Field(default_factory=list)


class MacroOverlay(_ContractModel):
    drivers: list[MacroOverlayDriver] = Field(default_factory=list)


class StructuralTrend(_ContractModel):
    description: str
    time_horizon: str | None = None
    source_refs: list[SourceId] = Field(default_factory=list)

    @field_validator("time_horizon", mode="before")
    @classmethod
    def _canonicalize_time_horizon(cls, value: object | None) -> str | None:
        return canonicalize_optional_time_horizon(value)


class IndustryAnalysis(_ContractModel):
    landscape: IndustryLandscape | None = None
    comps_narrative: CompsNarrative | None = None
    peer_comparison: IndustryPeerComparison | None = None
    macro_overlay: MacroOverlay | None = None
    structural_trends: list[StructuralTrend] = Field(default_factory=list)
    editorial_peer_set: list[EditorialPeer] = Field(default_factory=list)
    operating_comparison: OperatingComparison | None = None

    @field_validator("editorial_peer_set")
    @classmethod
    def _reject_duplicate_editorial_peer_tickers(
        cls, value: list[EditorialPeer]
    ) -> list[EditorialPeer]:
        seen: set[str] = set()
        duplicates: set[str] = set()
        for peer in value:
            if peer.ticker in seen:
                duplicates.add(peer.ticker)
            seen.add(peer.ticker)
        if duplicates:
            raise ValueError(
                "editorial_peer_set contains duplicate tickers: "
                f"{sorted(duplicates)}"
            )
        return value


__all__ = [
    "CompMetricCell",
    "CompsNarrative",
    "EditorialPeer",
    "IndustryAnalysis",
    "IndustryLandscape",
    "IndustryPeerComparison",
    "IndustryPeerComparisonPeer",
    "MacroOverlay",
    "MacroOverlayDriver",
    "OperatingComparison",
    "SnapshotMetric",
    "SnapshotSection",
    "StructuralTrend",
    "TimeseriesGroup",
    "TimeseriesMetric",
]
