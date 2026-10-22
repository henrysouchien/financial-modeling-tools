"""The post-compile valuation judgment artifact for one exact ModelVersion.

Store contract: the agent's method assessments and investment conclusion,
bound to the code-owned compiled snapshot (``schema.postcompile_valuation``).
The PriceTarget an accepted artifact publishes is not stored on it: it is
read through :func:`project_postcompile_price_target` from the snapshot and
the conclusion (``PostcompileValuationArtifact.price_target``).
"""

from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Any, Literal, Mapping, Sequence

from pydantic import Field, JsonValue, field_validator, model_validator

from .model_state_identity import validate_sha256_hex
from .postcompile_valuation import (
    _METHODS,
    CompiledValuationSnapshot,
    ExactModelVersionBinding,
    PostcompileMethodAssessment,
    PostcompilePriceTargetConclusion,
    PostcompileValuationGap,
    PostcompileValuationMethodologyPin,
    PostcompileValuationRef,
    PostcompileValuationStageReceipt,
    PostcompileValuationStageScope,
    PostcompileValuationTerminalSummary,
    _aware,
    _Contract,
    _digest,
    issue_postcompile_valuation_id,
    postcompile_valuation_stage_receipt_digest,
    project_postcompile_price_target,
)
from .scenario_set import ScenarioSetRef
from .valuation_policy import ValuationPolicyRef
from .price_target import PriceTarget
from .ticker_contract import normalize_ticker


class PostcompileValuationArtifact(_Contract):
    schema_version: Literal["postcompile-valuation.v1"] = "postcompile-valuation.v1"
    user_id: str = Field(min_length=1)
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1)
    ref: PostcompileValuationRef
    status: Literal["accepted", "incomplete", "blocked"]
    model_version: ExactModelVersionBinding
    valuation_policy_ref: ValuationPolicyRef
    valuation_policy_stage_receipt_digest: str
    scenario_set_ref: ScenarioSetRef
    snapshot: CompiledValuationSnapshot
    methodology: PostcompileValuationMethodologyPin
    method_assessments: tuple[PostcompileMethodAssessment, ...] = ()
    conclusion: PostcompilePriceTargetConclusion | None = None
    gaps: tuple[PostcompileValuationGap, ...] = ()
    terminal_summary: PostcompileValuationTerminalSummary
    created_by: str = Field(min_length=1)
    created_at: datetime
    stage_receipt: PostcompileValuationStageReceipt
    # Artifacts persisted before the PriceTarget became a read carry its
    # projection under ``price_target``. Decode-only: never written, never
    # read, excluded from every dump. The content digest re-derives the
    # projection (``postcompile_valuation_digest``), which the retired
    # equality check held equal to every stored copy, so persisted artifacts
    # keep their digest. A field, not a ``mode="before"`` validator: a before
    # validator re-validates JSON input in strict Python mode, which rejects
    # the stored ISO datetimes and decimals.
    legacy_price_target_copy: JsonValue | None = Field(
        default=None,
        validation_alias="price_target",
        exclude=True,
        repr=False,
    )

    @property
    def price_target(self) -> PriceTarget | None:
        """The PriceTarget this artifact's conclusion projects; None unless accepted."""

        if self.status != "accepted" or self.conclusion is None:
            return None
        return project_postcompile_price_target(
            research_file_id=self.research_file_id,
            snapshot=self.snapshot,
            conclusion=self.conclusion,
        )

    @field_validator("ticker")
    @classmethod
    def _ticker(cls, value: str) -> str:
        normalized = normalize_ticker(value)
        if normalized != value:
            raise ValueError("ticker must be canonical")
        return normalized

    @field_validator("valuation_policy_stage_receipt_digest")
    @classmethod
    def _policy_receipt(cls, value: str) -> str:
        return validate_sha256_hex(
            value, field_name="valuation_policy_stage_receipt_digest"
        )

    @field_validator("created_at")
    @classmethod
    def _created_at(cls, value: datetime) -> datetime:
        return _aware(value, field_name="created_at")

    @model_validator(mode="after")
    def _artifact(self) -> "PostcompileValuationArtifact":
        if (
            self.model_version != self.snapshot.model_version
            or self.valuation_policy_ref != self.snapshot.valuation_policy_ref
            or self.valuation_policy_stage_receipt_digest
            != self.snapshot.valuation_policy_stage_receipt_digest
            or self.scenario_set_ref != self.snapshot.scenario_set_ref
        ):
            raise ValueError("postcompile valuation inputs differ from exact snapshot")
        methods = tuple(row.method for row in self.method_assessments)
        if methods and methods != _METHODS:
            raise ValueError("method assessments require the exact method denominator")
        available_keys = {
            f"method:{row.method}.{case}"
            for row in self.snapshot.methods
            if row.status == "available"
            for case in ("bear", "base", "bull")
        }
        available_keys.update(f"wacc:{row.key}" for row in self.snapshot.wacc)
        available_keys.update(
            f"bridge:{row.key}" for row in self.snapshot.enterprise_to_equity_bridge
        )
        if any(
            not set(row.output_keys) <= available_keys
            for row in self.method_assessments
        ):
            raise ValueError("method assessment cites output outside compiled snapshot")
        gap_keys = tuple(row.key for row in self.gaps)
        if gap_keys != tuple(sorted(set(gap_keys))):
            raise ValueError("postcompile valuation gaps must be sorted and unique")

        if self.status == "accepted":
            if methods != _METHODS:
                raise ValueError(
                    "accepted postcompile valuation requires all method assessments"
                )
            if any(row.status != "available" for row in self.snapshot.methods):
                raise ValueError(
                    "accepted postcompile valuation requires all compiled methods"
                )
            if any(
                row.disposition not in {"included", "cross_check"}
                for row in self.method_assessments
            ):
                raise ValueError("accepted postcompile valuation must use every method")
            if self.conclusion is None:
                raise ValueError(
                    "accepted postcompile valuation requires a PriceTarget"
                )
            if any(row.blocking for row in self.gaps):
                raise ValueError(
                    "accepted postcompile valuation cannot retain blocking gaps"
                )
            envelope = [
                value
                for row in self.snapshot.methods
                if row.prices is not None
                for value in (row.prices.bear, row.prices.base, row.prices.bull)
            ]
            if not (
                min(envelope) <= Decimal(str(self.conclusion.ranges.low))
                and Decimal(str(self.conclusion.ranges.high)) <= max(envelope)
            ):
                raise ValueError(
                    "PriceTarget range falls outside compiled method envelope"
                )
        else:
            if self.conclusion is not None:
                raise ValueError(
                    "non-accepted postcompile valuation cannot publish PriceTarget"
                )
            if not any(row.blocking for row in self.gaps):
                raise ValueError(
                    "incomplete or blocked valuation requires a blocking gap"
                )

        expected_scope = PostcompileValuationStageScope(
            user_id=self.user_id,
            research_file_id=self.research_file_id,
            ticker=self.ticker,
        )
        receipt = self.stage_receipt
        price_target = self.price_target
        if (
            receipt.status != self.status
            or receipt.scope != expected_scope
            or receipt.postcompile_valuation_ref != self.ref
            or receipt.model_version != self.model_version
            or receipt.valuation_policy_ref != self.valuation_policy_ref
            or receipt.valuation_policy_stage_receipt_digest
            != self.valuation_policy_stage_receipt_digest
            or receipt.compiled_valuation_snapshot_sha256
            != self.snapshot.snapshot_sha256
            or receipt.price_target_id
            != (None if price_target is None else price_target.price_target_id)
        ):
            raise ValueError("postcompile stage receipt does not bind exact artifact")
        if self.ref.content_sha256 != postcompile_valuation_digest(self):
            raise ValueError("postcompile valuation content digest mismatch")
        return self


def postcompile_valuation_digest(
    value: PostcompileValuationArtifact | Mapping[str, Any],
) -> str:
    """Digest the artifact content, the projected PriceTarget included.

    The projection is part of the digested content exactly as the stored copy
    was before it became a read, so every persisted artifact keeps its digest.
    A mapping carries the projection under ``price_target``.
    """

    if isinstance(value, PostcompileValuationArtifact):
        payload = value.model_dump(mode="json")
        price_target = value.price_target
        payload["price_target"] = (
            None if price_target is None else price_target.model_dump(mode="json")
        )
    else:
        payload = dict(value)
    payload.pop("ref", None)
    payload.pop("stage_receipt", None)
    return _digest("postcompile-valuation.v1", payload)


def build_postcompile_valuation(
    *,
    artifact_id: str | None,
    user_id: str,
    research_file_id: int,
    ticker: str,
    status: Literal["accepted", "incomplete", "blocked"],
    snapshot: CompiledValuationSnapshot,
    methodology: PostcompileValuationMethodologyPin,
    method_assessments: Sequence[PostcompileMethodAssessment],
    conclusion: PostcompilePriceTargetConclusion | None,
    gaps: Sequence[PostcompileValuationGap],
    terminal_summary: PostcompileValuationTerminalSummary,
    created_by: str,
    created_at: datetime,
    started_at: datetime,
    completed_at: datetime,
) -> PostcompileValuationArtifact:
    price_target = (
        None
        if status != "accepted" or conclusion is None
        else project_postcompile_price_target(
            research_file_id=research_file_id,
            snapshot=snapshot,
            conclusion=conclusion,
        )
    )
    payload = {
        "schema_version": "postcompile-valuation.v1",
        "user_id": user_id,
        "research_file_id": research_file_id,
        "ticker": ticker,
        "status": status,
        "model_version": snapshot.model_version,
        "valuation_policy_ref": snapshot.valuation_policy_ref,
        "valuation_policy_stage_receipt_digest": (
            snapshot.valuation_policy_stage_receipt_digest
        ),
        "scenario_set_ref": snapshot.scenario_set_ref,
        "snapshot": snapshot,
        "methodology": methodology,
        "method_assessments": tuple(
            sorted(method_assessments, key=lambda row: row.method)
        ),
        "conclusion": conclusion,
        "gaps": tuple(sorted(gaps, key=lambda row: row.key)),
        "terminal_summary": terminal_summary,
        "created_by": created_by,
        "created_at": created_at,
    }
    ref = PostcompileValuationRef(
        artifact_id=artifact_id or issue_postcompile_valuation_id(),
        content_sha256=postcompile_valuation_digest(
            {**payload, "price_target": price_target}
        ),
    )
    receipt_payload = {
        "schema_version": "postcompile-valuation-stage-receipt.v1",
        "status": status,
        "scope": PostcompileValuationStageScope(
            user_id=user_id,
            research_file_id=research_file_id,
            ticker=ticker,
        ),
        "postcompile_valuation_ref": ref,
        "model_version": snapshot.model_version,
        "valuation_policy_ref": snapshot.valuation_policy_ref,
        "valuation_policy_stage_receipt_digest": (
            snapshot.valuation_policy_stage_receipt_digest
        ),
        "compiled_valuation_snapshot_sha256": snapshot.snapshot_sha256,
        "price_target_id": (
            None if price_target is None else price_target.price_target_id
        ),
        "started_at": started_at,
        "completed_at": completed_at,
    }
    receipt = PostcompileValuationStageReceipt.model_validate(
        {
            **receipt_payload,
            "receipt_digest": postcompile_valuation_stage_receipt_digest(
                receipt_payload
            ),
        }
    )
    return PostcompileValuationArtifact.model_validate(
        {**payload, "ref": ref, "stage_receipt": receipt}
    )


__all__ = [
    "PostcompileValuationArtifact",
    "build_postcompile_valuation",
    "postcompile_valuation_digest",
]
