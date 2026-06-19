from __future__ import annotations

from typing import Any, Literal

from pydantic import Field

from .thesis_shared_slice import SourceRecord, _ContractModel


HtmlArtifactPurpose = Literal[
    "exploration",
    "comparison",
    "explainer",
    "session_log",
    "report",
    "other",
]


class StaticExports(_ContractModel):
    copy_as_prompt: str | None
    copy_as_markdown: str | None
    copy_as_json: dict[str, Any] | None


class HtmlArtifact(_ContractModel):
    artifact_id: str = Field(..., min_length=1)
    title: str = Field(..., min_length=1)
    purpose: HtmlArtifactPurpose
    content_ref: str = Field(..., min_length=1)
    summary: str = Field(..., min_length=1)
    ticker: str | None
    session_id: str | None
    source_skill: str = Field(..., min_length=1)
    sources: list[SourceRecord]
    exports: StaticExports
    ts: str = Field(..., min_length=1)
    research_file_id: int | None = None
    control_run_id: str | None = None
    origin_kind: Literal["product", "harness", "import"] | None = None
    visibility: Literal["default", "sandbox", "archived"] | None = None
    origin_ref: dict[str, Any] | None = None
    contract_name: Literal["HtmlArtifact"] = "HtmlArtifact"


__all__ = [
    "HtmlArtifact",
    "HtmlArtifactPurpose",
    "StaticExports",
]
