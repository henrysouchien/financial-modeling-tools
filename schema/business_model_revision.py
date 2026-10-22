"""Canonical immutable BusinessModel revision and selected-revision contracts."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import hashlib
from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .business_model import BusinessModel, DriverAssumptionPlan
from .business_model_revision_ref import BusinessModelRevisionRef
from .business_model_stage import (
    BusinessModelStageMetadata,
    BusinessModelStageReceipt,
    validate_business_model_stage_receipt,
)
from .judgment_revision import Supersession, validate_revision_fields
from .model_state_identity import validate_sha256_hex
from .ticker_contract import normalize_ticker
from .thesis import QuestionRef
from .versioned_artifact import canonical_json_bytes


class _Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class BusinessModelValidationWarning(_Contract):
    """One warning the BusinessModel door's validation returned at persist.

    A record of the run (S8): written on the revision at persist, read by the
    compile into ``business_model_validation_warning`` Findings. Never a
    refusal. ``code`` is the validator's (``schema/business_model_validate.py``);
    ``subject`` is the node, segment or discovered member it names.
    """

    code: str = Field(min_length=1)
    subject: str | None = Field(default=None, min_length=1)
    message: str = Field(min_length=1)


def _validation_warning_subject(row: Mapping[str, Any]) -> str | None:
    """The node, absorbs claim, discovered member or segment a row names.

    One subject per row the validator can emit more than once: an
    ``absorbed_claim_member_suggestion`` is per claim of a segment, so its
    ``claim_index`` joins the segment id.
    """

    if row.get("qualified_id"):
        return str(row["qualified_id"])
    if row.get("edgar_member"):
        return str(row["edgar_member"])
    segment_id = row.get("segment_id")
    if not segment_id:
        return None
    claim_index = row.get("claim_index")
    if claim_index is not None:
        return f"{segment_id}.absorbs[{claim_index}]"
    return str(segment_id)


def business_model_validation_warnings(
    validation: Mapping[str, Any],
) -> tuple[BusinessModelValidationWarning, ...]:
    """The typed rows of one ``business_model_validate.validate`` result."""

    return tuple(
        BusinessModelValidationWarning(
            code=str(row["code"]),
            subject=_validation_warning_subject(row),
            message=str(row["message"]),
        )
        for row in validation.get("validation_warnings") or ()
    )


class BusinessModelRevision(_Contract):
    schema_version: Literal["1.0"] = "1.0"
    user_id: str = Field(min_length=1)
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1)
    ref: BusinessModelRevisionRef
    predecessor_revision: str | None = Field(default=None, min_length=1)
    # The Revision fields (``schema/judgment_revision.py``): the typed link to
    # the revision ``predecessor_revision`` names, absent on a first revision
    # and on every revision persisted before it.
    predecessor_ref: BusinessModelRevisionRef | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    #: One per predecessor DriverAssumptionPlan entry (by ``driver_id``, the
    #: identity successors carry) this revision changes or drops.
    supersessions: tuple[Supersession, ...] = Field(
        default=(),
        exclude_if=lambda value: not value,
    )
    #: The Question this decomposition answers (R2, 2026-10-03: decompositions
    #: are question-keyed, one research file per Question). Absent on every
    #: revision persisted before it.
    question_ref: QuestionRef | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    request_digest: str
    driver_assumption_plan_sha256: str
    business_model: BusinessModel
    driver_assumption_plan: DriverAssumptionPlan
    created_by: str = Field(min_length=1)
    created_at: datetime
    stage_metadata: BusinessModelStageMetadata | None = None
    stage_receipt: BusinessModelStageReceipt | None = None
    #: The door's validation warnings at persist (record of the run); absent
    #: on every revision persisted before it. Outside ``ref.content_sha256``.
    validation_warnings: tuple[BusinessModelValidationWarning, ...] = Field(
        default=(),
        exclude_if=lambda value: not value,
    )

    @field_validator("ticker")
    @classmethod
    def _ticker(cls, value: str) -> str:
        normalized = normalize_ticker(value)
        if normalized != value:
            raise ValueError("ticker must be canonical")
        return normalized

    @field_validator("request_digest", "driver_assumption_plan_sha256")
    @classmethod
    def _digest(cls, value: str, info) -> str:
        return validate_sha256_hex(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _identity(self) -> "BusinessModelRevision":
        expected_id = f"{self.ticker}_business_model"
        revision = str(self.business_model.metadata.revision or "").strip()
        if (
            self.business_model.company.ticker != self.ticker
            or self.ref.business_model_id != expected_id
            or self.ref.revision != revision
            or self.driver_assumption_plan.business_model_id != expected_id
            or self.driver_assumption_plan.revision != revision
        ):
            raise ValueError("BusinessModel revision identity is inconsistent")
        business_model_digest = hashlib.sha256(
            canonical_json_bytes(self.business_model.model_dump(mode="json"))
        ).hexdigest()
        plan_digest = hashlib.sha256(
            canonical_json_bytes(
                self.driver_assumption_plan.model_dump(mode="json")
            )
        ).hexdigest()
        if business_model_digest != self.ref.content_sha256:
            raise ValueError("BusinessModel revision content digest is inconsistent")
        if plan_digest != self.driver_assumption_plan_sha256:
            raise ValueError(
                "BusinessModel revision DriverAssumptionPlan digest is inconsistent"
            )
        if (self.stage_metadata is None) != (self.stage_receipt is None):
            raise ValueError(
                "BusinessModel stage_metadata and stage_receipt must appear together"
            )
        validate_revision_fields(self.predecessor_ref, self.supersessions)
        if self.predecessor_ref is not None and (
            self.predecessor_ref.business_model_id != expected_id
            or self.predecessor_ref.revision != self.predecessor_revision
        ):
            raise ValueError(
                "BusinessModel predecessor_ref must name the predecessor_revision"
            )
        if (
            self.question_ref is not None
            and self.question_ref.research_file_id != self.research_file_id
        ):
            raise ValueError(
                "BusinessModel question_ref must name a Question of its research file"
            )
        if self.stage_metadata is not None and self.stage_receipt is not None:
            if self.stage_receipt.status != "accepted":
                raise ValueError("BusinessModel stage receipt must be accepted")
            if self.stage_receipt.stage_metadata != self.stage_metadata:
                raise ValueError(
                    "BusinessModel stage receipt metadata differs from revision metadata"
                )
            validate_business_model_stage_receipt(
                self.stage_receipt,
                user_id=self.user_id,
                research_file_id=self.research_file_id,
                ticker=self.ticker,
                business_model_revision_ref=self.ref,
            )
        return self


class SelectedBusinessModelRevision(_Contract):
    schema_version: Literal["1.0"] = "1.0"
    user_id: str = Field(min_length=1)
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1)
    selected: BusinessModelRevisionRef
    epoch: int = Field(ge=1)
    updated_by: str = Field(min_length=1)
    updated_at: datetime

    @field_validator("ticker")
    @classmethod
    def _ticker(cls, value: str) -> str:
        normalized = normalize_ticker(value)
        if normalized != value:
            raise ValueError("ticker must be canonical")
        return normalized


@dataclass(frozen=True)
class BusinessModelRevisionAbsent:
    """The exact revision a SourceBundle pins is not stored.

    Completeness, not corruption: nothing was written for that ref, so a reader
    records the gap. The producer is ``fms_persist_business_model``. A digest
    mismatch or an undecodable payload stays a hydration raise.
    """

    message: str
    research_file_id: int

    @property
    def next_call(self) -> dict[str, Any]:
        return {
            "tool_name": "fms_persist_business_model",
            "arguments": {"research_file_id": self.research_file_id},
        }


__all__ = [
    "BusinessModelRevision",
    "BusinessModelRevisionAbsent",
    "BusinessModelRevisionRef",
    "BusinessModelValidationWarning",
    "SelectedBusinessModelRevision",
    "business_model_validation_warnings",
]
