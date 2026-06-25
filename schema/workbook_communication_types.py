from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field


WorkbookCommunicationStatus = Literal["ready", "actionable", "blocked"]
WorkbookCommunicationMaterializationStatus = Literal["materialized", "blocked", "noop"]
WorkbookCommunicationFieldStatus = Literal[
    "populated",
    "materializable",
    "formula_only",
    "missing_source",
    "missing_sheet",
    "missing_cell",
]


class WorkbookCommunicationCandidateWrite(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    field: str
    label: str
    sheet: str
    cell: str
    value: Any
    source_kind: str
    source_ref: str | None = None
    current_formula: str | None = None
    current_display_value: Any = None
    reason: str


class WorkbookCommunicationFieldReport(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    field: str
    label: str
    sheet: str
    cell: str | None = None
    required: bool = True
    status: WorkbookCommunicationFieldStatus
    current_formula: str | None = None
    current_display_value: Any = None
    source_value: Any = None
    source_kind: str | None = None
    source_ref: str | None = None
    issue: str | None = None
    candidate_write: WorkbookCommunicationCandidateWrite | None = None


class WorkbookCommunicationReadiness(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    workbook_path: str
    workbook_exists: bool = True
    status: WorkbookCommunicationStatus
    fields: list[WorkbookCommunicationFieldReport] = Field(default_factory=list)
    candidate_writes: list[WorkbookCommunicationCandidateWrite] = Field(default_factory=list)
    issues: list[str] = Field(default_factory=list)
    summary: dict[str, int] = Field(default_factory=dict)


class WorkbookCommunicationMaterializationResult(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    workbook_path: str
    output_path: str | None = None
    status: WorkbookCommunicationMaterializationStatus
    pre_readiness: WorkbookCommunicationReadiness
    post_readiness: WorkbookCommunicationReadiness | None = None
    writes_applied: list[WorkbookCommunicationCandidateWrite] = Field(default_factory=list)
    issues: list[str] = Field(default_factory=list)


__all__ = [
    "WorkbookCommunicationCandidateWrite",
    "WorkbookCommunicationFieldReport",
    "WorkbookCommunicationFieldStatus",
    "WorkbookCommunicationMaterializationResult",
    "WorkbookCommunicationMaterializationStatus",
    "WorkbookCommunicationReadiness",
    "WorkbookCommunicationStatus",
]
