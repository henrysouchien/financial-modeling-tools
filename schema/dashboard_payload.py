from __future__ import annotations

import re
from typing import Any, Literal

from pydantic import Field, field_validator, model_validator

from .thesis_shared_slice import SourceId, SourceRecord, _ContractModel


DASHBOARD_KIND = "hank_dashboard.v1"

ReadinessPosture = Literal[
    "draft",
    "working_draft",
    "screen_grade",
    "pm_ready",
    "decision_ready",
]
CitationPolicy = Literal["warn", "strict"]
DashboardStatus = Literal["good", "watch", "risk", "bull", "base", "bear", "gap"]

_SECTION_ID_RE = re.compile(r"^[a-z][a-z0-9-]*$")


class MetricTileItem(_ContractModel):
    label: str = Field(..., min_length=1)
    value: str | int | float = Field(...)
    detail: str | None = None
    status: DashboardStatus | None = None
    citations: list[SourceId] = Field(default_factory=list)


class DashboardHero(_ContractModel):
    title: str = Field(..., min_length=1)
    body: str = Field(..., min_length=1)
    status: DashboardStatus | None = None
    citations: list[SourceId] = Field(default_factory=list)


class DashboardMetadata(_ContractModel):
    readiness_posture: ReadinessPosture
    verdict_token: str | None = None
    may_circulate: bool = True
    freeze_time: str = Field(..., min_length=1)
    decision_context: str | None = None
    citation_policy: CitationPolicy


class DashboardModule(_ContractModel):
    type: str = Field(..., min_length=1)
    title: str | None = None
    data: dict[str, Any] = Field(default_factory=dict)


class DashboardSection(_ContractModel):
    id: str = Field(..., min_length=1)
    label: str = Field(..., min_length=1)
    modules: list[DashboardModule] = Field(default_factory=list)

    @field_validator("id")
    @classmethod
    def _validate_section_id(cls, value: str) -> str:
        if not _SECTION_ID_RE.fullmatch(value):
            raise ValueError("section id must be kebab-case and match ^[a-z][a-z0-9-]*$")
        return value


class DashboardPayload(_ContractModel):
    kind: Literal["hank_dashboard.v1"] = DASHBOARD_KIND
    title: str = Field(..., min_length=1)
    subtitle: str | None = None
    ticker: str | None = None
    scope_label: str | None = None
    metadata: DashboardMetadata
    hero: DashboardHero | None = None
    snapshot: list[MetricTileItem] = Field(default_factory=list)
    sections: list[DashboardSection]
    sources: list[SourceRecord] = Field(default_factory=list)

    @model_validator(mode="after")
    def _validate_section_ids_unique(self) -> "DashboardPayload":
        seen: set[str] = set()
        duplicates: list[str] = []
        for section in self.sections:
            if section.id in seen:
                duplicates.append(section.id)
            seen.add(section.id)
        if duplicates:
            raise ValueError("section ids must be unique: " + ", ".join(sorted(set(duplicates))))
        return self


__all__ = [
    "CitationPolicy",
    "DASHBOARD_KIND",
    "DashboardHero",
    "DashboardMetadata",
    "DashboardModule",
    "DashboardPayload",
    "DashboardSection",
    "DashboardStatus",
    "MetricTileItem",
    "ReadinessPosture",
]
