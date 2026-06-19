from __future__ import annotations

from typing import Any, Literal

from pydantic import Field

from .handoff import CurrentModelRef
from .thesis_shared_slice import _ContractModel


ModelingWorkflowStage = Literal[
    "idea",
    "research",
    "diligence",
    "bm",
    "build",
    "forecast",
    "scenarios",
    "valuation",
    "review",
    "finalized",
]
ModelingWorkflowFinalizeGrade = Literal["build", "full"]
PendingActionSeverity = Literal["info", "warning", "blocking"]


class ModelingWorkflowGateSnapshot(_ContractModel):
    status: Literal["passed", "failed", "unknown"] = "unknown"
    purpose: Literal["handoff", "build"] = "handoff"
    template_id: str | None = None
    failed_gates: dict[str, Any] = Field(default_factory=dict)
    gates_hash: str


class ModelingWorkflowDiligenceCompletionRef(_ContractModel):
    handoff_id: int | None = Field(default=None, ge=1)
    handoff_version: int | None = Field(default=None, ge=1)
    diligence_completion_hash: str


class PendingAction(_ContractModel):
    code: str = Field(min_length=1)
    stage: ModelingWorkflowStage
    message: str = Field(min_length=1)
    severity: PendingActionSeverity = "blocking"
    target: str | None = None
    source: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class ModelingWorkflowStateCacheKey(_ContractModel):
    research_file_id: int = Field(ge=1)
    thesis_id: str | None = None
    thesis_version: int | None = Field(default=None, ge=1)
    handoff_id: int | None = Field(default=None, ge=1)
    handoff_version: int | None = Field(default=None, ge=1)
    model_id: str | None = None
    model_version: int | None = Field(default=None, ge=1)
    model_build_context_id: str | None = None
    model_build_context_version: int | None = Field(default=None, ge=1)
    diligence_completion_hash: str
    gates_hash: str
    model_semantics_hash: str | None = None


class ModelingWorkflowState(_ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    research_file_id: int = Field(ge=1)
    stage: ModelingWorkflowStage
    finalize_grade: ModelingWorkflowFinalizeGrade | None = None
    thesis_id: str | None = None
    thesis_version: int | None = Field(default=None, ge=1)
    handoff_id: int | None = Field(default=None, ge=1)
    handoff_version: int | None = Field(default=None, ge=1)
    handoff_status: str | None = None
    diligence_completion_ref: ModelingWorkflowDiligenceCompletionRef
    current_model_ref: CurrentModelRef | None = None
    gates_passed: ModelingWorkflowGateSnapshot
    pending_actions: list[PendingAction] = Field(default_factory=list)
    cache_key: ModelingWorkflowStateCacheKey


__all__ = [
    "ModelingWorkflowDiligenceCompletionRef",
    "ModelingWorkflowFinalizeGrade",
    "ModelingWorkflowGateSnapshot",
    "ModelingWorkflowStage",
    "ModelingWorkflowState",
    "ModelingWorkflowStateCacheKey",
    "PendingAction",
    "PendingActionSeverity",
]
