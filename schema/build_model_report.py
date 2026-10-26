"""The stored typed outputs of a run-bound build-model report sidecar.

The build-model report door (``api/fms/modules/build_model.py``
``run_bound_report_plan``) constructs one of these from the exact durable build
run arm plus the agent's admitted assessment, and persists its ``model_dump``;
the agent never submits it. A construction that does not validate is the door's
defect, raised with the field named, never a demand on the agent.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, JsonValue

from .build_model_product_contracts import ModelVersion


RUN_BOUND_REPORT_SCHEMA_VERSION = "run-bound/1"


class _StoredContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class BuildModelReportAssessment(_StoredContract):
    """The only build-model report fields the agent authors."""

    confidence: Literal["HIGH", "MEDIUM", "LOW"]
    summary: str = Field(min_length=1)
    recommended_next_action: str = Field(min_length=1)
    notes: tuple[str, ...] = ()


class BuildRunFailure(_StoredContract):
    stage: str = Field(min_length=1)
    typed_error_code: str = Field(min_length=1)
    occurred_at: str = Field(min_length=1)


class FailedBuildModelRunReport(_StoredContract):
    """A build run that recorded a typed failure before any ModelVersion commit."""

    schema_version: Literal["run-bound/1"] = RUN_BOUND_REPORT_SCHEMA_VERSION
    arm: Literal["failed_precommit"] = "failed_precommit"
    run_id: str = Field(min_length=1)
    ticker: str = Field(min_length=1)
    verdict: Literal["BUILD_BLOCKED", "BUILD_FAILED"]
    failure: BuildRunFailure
    agent: BuildModelReportAssessment


class PersistedBuildModelRunReport(_StoredContract):
    """A build run that committed one immutable ModelVersion.

    The server fields are the persisted build report projection
    (``model_build.report_projection``), already validated there.
    """

    schema_version: Literal["run-bound/1"] = RUN_BOUND_REPORT_SCHEMA_VERSION
    arm: Literal["persisted_pending_outputs", "persisted_final"]
    run_id: str = Field(min_length=1)
    ticker: str = Field(min_length=1)
    built_at: str = Field(min_length=1)
    verdict: Literal["BUILD_CLEAN", "BUILD_WITH_WARNINGS", "BUILD_BLOCKED"]
    model_version: ModelVersion
    annual_presentation: dict[str, JsonValue]
    control_snapshot_projection: dict[str, JsonValue]
    diagnostic_report: dict[str, JsonValue] | None
    findings: tuple[dict[str, JsonValue], ...]
    model_spec: dict[str, JsonValue]
    normalized: dict[str, JsonValue]
    readiness_cross_check: dict[str, JsonValue]
    semantic_rows: dict[str, JsonValue]
    source: dict[str, JsonValue]
    warning_codes: tuple[str, ...]
    model_insights_status: dict[str, JsonValue]
    price_target_status: dict[str, JsonValue]
    #: The CurrentModelPointer projection ``{epoch, selected}`` read after the commit.
    current_model: dict[str, JsonValue] | None
    #: Agent judgment recorded by ``fms_report_build_model``; ``None`` when written by ``build_model`` publish before ``fms_report_build_model`` runs.
    agent: BuildModelReportAssessment | None = None


__all__ = [
    "RUN_BOUND_REPORT_SCHEMA_VERSION",
    "BuildModelReportAssessment",
    "BuildRunFailure",
    "FailedBuildModelRunReport",
    "PersistedBuildModelRunReport",
]
