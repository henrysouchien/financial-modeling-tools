"""The stored model-review report: the review's verdict, tier subscores, findings and gate.

``fms_propose_model_review`` constructs :class:`ModelReviewOutputs` from the
admitted judgment; this model validates what the door built. Each finding keeps
the producer's own keys (evidence, item ids, remediation metadata) beside the
canonical ``severity`` / ``grounding`` / ``tier`` the door resolved, so a
finding is an open record here.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import Field

from .modeling_workflow_state import RemediationDirective
from .thesis_shared_slice import _ContractModel


ModelReviewVerdict = Literal[
    "MODEL_HEALTHY",
    "MODEL_STALE",
    "MODEL_HAS_ISSUES",
    "MODEL_BROKEN",
    "TARGETED_MODEL_REVIEW",
    "INSUFFICIENT_DATA",
]
ModelReviewMode = Literal["full", "targeted"]
ModelReviewGate = Literal[
    "proceed to scenario-multiple-pricing",
    "proceed to dcf-relative-valuation",
    "fix-then-review",
    "proceed-with-caveats",
]
ModelReviewConfidence = Literal["HIGH", "MEDIUM", "LOW"]
ModelReviewTier = Literal[
    "tier_1_mechanical",
    "tier_2_projection",
    "tier_3_scenario",
    "tier_4_valuation",
    "tier_5_methodology",
]
ModelReviewSubscore = Literal["pass", "concerns", "fail", "blocked", "not_run"]


class ModelReviewSummary(_ContractModel):
    mode: ModelReviewMode
    verdict: ModelReviewVerdict
    confidence: ModelReviewConfidence | None
    summary: str = Field(min_length=1)
    gate: ModelReviewGate


class ModelReviewFindingCounts(_ContractModel):
    blocking: int = Field(ge=0)
    material: int = Field(ge=0)
    advisory: int = Field(ge=0)
    staleness_blocking: int = Field(ge=0)


class ModelReviewOutputs(_ContractModel):
    review: ModelReviewSummary
    current_model: dict[str, Any]
    file_path: str | None
    build_source: Any
    tier_subscores: dict[ModelReviewTier, ModelReviewSubscore]
    findings: list[dict[str, Any]]
    finding_counts: ModelReviewFindingCounts
    targeted_scope: str | None
    methodology_applied: list[str]
    data_gaps: list[Any]
    remediation_directives: list[RemediationDirective]
    recommended_next_action: str | None


__all__ = [
    "ModelReviewConfidence",
    "ModelReviewFindingCounts",
    "ModelReviewGate",
    "ModelReviewMode",
    "ModelReviewOutputs",
    "ModelReviewSubscore",
    "ModelReviewSummary",
    "ModelReviewTier",
    "ModelReviewVerdict",
]
