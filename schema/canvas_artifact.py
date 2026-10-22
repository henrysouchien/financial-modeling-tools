from __future__ import annotations

from typing import Any, Literal

from pydantic import Field

from .thesis_shared_slice import SourceRecord, _ContractModel


CanvasArtifactPurpose = Literal[
    "exploration",
    "comparison",
    "explainer",
    "session_log",
    "other",
]


class StaticExports(_ContractModel):
    copy_as_prompt: str | None = None
    copy_as_markdown: str = Field(..., min_length=1)
    copy_as_json: dict[str, Any] | None = None


class CanvasArtifact(_ContractModel):
    artifact_id: str = Field(..., min_length=1)
    title: str = Field(..., min_length=1)
    purpose: CanvasArtifactPurpose
    source_ref: str = Field(..., min_length=1)
    source_digest: str = Field(..., pattern=r"^[0-9a-f]{64}$")
    bundle_ref: str = Field(..., min_length=1)
    bundle_digest: str = Field(..., pattern=r"^[0-9a-f]{64}$")
    toolchain_version: str = Field(..., min_length=1)
    kit_contract_version: int = Field(..., ge=1)
    summary: str = Field(..., min_length=1)
    ticker: str | None
    session_id: str | None
    source_skill: str = Field(..., min_length=1)
    sources: list[SourceRecord]
    exports: StaticExports
    ts: str = Field(..., min_length=1)
    research_file_id: int | None = None
    control_run_id: str | None = None
    origin_kind: Literal["product", "harness", "import"] | None = "product"
    visibility: Literal["default", "sandbox", "archived"] | None = "default"
    origin_ref: dict[str, Any] | None = None
    contract_name: Literal["CanvasArtifact"] = "CanvasArtifact"


__all__ = [
    "CanvasArtifact",
    "CanvasArtifactPurpose",
    "StaticExports",
]
