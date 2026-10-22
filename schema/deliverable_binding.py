"""The one ModelVersion a deliverable was computed from (plan principle S8; W5d).

Compression (E). The pitch, initiation dossier, earnings preview and earnings
review each carry one :class:`DeliverableModelBinding`: the research file whose
current pointer selects models, the exact ModelVersion the deliverable was cut
from, and the Question it answers (R1/R2, 2026-10-03). A binding may name an
older version than the pointer selects (a deliverable pinned to it); the door
that writes it then records ``deliverable_version_stale`` in its ``findings``.
The door stamps the research file's current Question ratified or not, and records
``question_unratified`` beside it while that Question is unratified (ruled (a),
2026-10-04). Reading a deliverable reports ``deliverable_version_stale`` again
when its research file's pointer has since moved, and ``question_unratified``
for as long as the bound Question stays unratified (``schema/build_findings.py``);
the binding never refuses a read.
"""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .finding_vocabulary import Finding
from .model_ref import ModelVersionRef
from .thesis import QuestionRef


class DeliverableModelBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    research_file_id: int = Field(gt=0, strict=True)
    model_version: ModelVersionRef
    question_ref: QuestionRef | None = None
    #: Stamped by the deliverable's door at write time, never proposed: the
    #: ``deliverable_version_stale`` finding when the pointer already selected
    #: another version, and ``question_unratified`` when the stamped Question was
    #: not yet ratified. Excluded from dumps while empty, so a binding to the
    #: current version under a ratified Question keeps its bytes.
    findings: tuple[Finding, ...] = Field(default=(), exclude_if=lambda value: not value)

    @model_validator(mode="after")
    def _one_research_file(self) -> "DeliverableModelBinding":
        if (
            self.question_ref is not None
            and self.question_ref.research_file_id != self.research_file_id
        ):
            raise ValueError(
                "question_ref must name a Question of the binding's research file"
            )
        return self


#: Where each deliverable's persisted typed outputs hold its binding: the
#: earnings review's typed outputs are its contract, the other three nest their
#: pack under one key.
_NESTED_DELIVERABLE_KEYS = ("stock_pitch", "earnings_preview", "initiation_dossier")


def deliverable_model_binding(
    typed_outputs: Mapping[str, Any] | None,
) -> DeliverableModelBinding | None:
    """The binding a persisted deliverable's typed outputs carry, if it is one of the four."""

    if not isinstance(typed_outputs, Mapping):
        return None
    if typed_outputs.get("contract_name") == "earnings-review-report":
        holder: Mapping[str, Any] | None = typed_outputs
    else:
        holder = next(
            (
                typed_outputs[key]
                for key in _NESTED_DELIVERABLE_KEYS
                if isinstance(typed_outputs.get(key), Mapping)
            ),
            None,
        )
    if holder is None or holder.get("model_binding") is None:
        return None
    return DeliverableModelBinding.model_validate(holder["model_binding"])


__all__ = ["DeliverableModelBinding", "deliverable_model_binding"]
