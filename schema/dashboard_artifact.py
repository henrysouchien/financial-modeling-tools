from __future__ import annotations

from typing import Any, Literal

from pydantic import Field

from .dashboard_payload import ReadinessPosture
from .thesis_shared_slice import _ContractModel


class DashboardArtifact(_ContractModel):
    artifact_id: str = Field(..., min_length=1)
    title: str = Field(..., min_length=1)
    summary: str = Field(..., min_length=1)
    ticker: str | None
    scope_label: str | None
    source_skill: str = Field(..., min_length=1)
    readiness_posture: ReadinessPosture
    profile: Literal["draft", "production"]
    payload_ref: str = Field(..., min_length=1)
    ts: str = Field(..., min_length=1)
    research_file_id: int | None = None
    control_run_id: str | None = None
    origin_kind: Literal["product", "harness", "import"] | None = None
    visibility: Literal["default", "sandbox", "archived"] | None = None
    origin_ref: dict[str, Any] | None = None
    contract_name: Literal["DashboardArtifact"] = "DashboardArtifact"


__all__ = [
    "DashboardArtifact",
]
