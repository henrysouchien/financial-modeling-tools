"""Typed semantic-item to natural-claim bindings for BusinessModel handoff.

The agent authors claim keys on the compact BusinessModel judgment.  This
contract records the deterministic projection of those keys onto the bounded
semantic items exposed to downstream stages.  It intentionally carries no
evidence descriptors: durable claim-to-evidence bindings remain owned by
``BusinessModelStageMetadata``.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .versioned_artifact import canonical_json_bytes


BusinessModelSemanticItemKind = Literal[
    "summary",
    "segment",
    "driver",
    "cost",
    "capital",
    "limitation",
]


class BusinessModelSemanticClaimBinding(BaseModel):
    """One bounded downstream semantic item and its natural claim keys."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )

    schema_version: Literal["business-model-semantic-claim-binding.v1"] = (
        "business-model-semantic-claim-binding.v1"
    )
    item_kind: BusinessModelSemanticItemKind
    item_key: str = Field(min_length=1, max_length=256)
    claim_keys: tuple[str, ...] = Field(default=(), max_length=64)

    @field_validator("claim_keys")
    @classmethod
    def _canonical_claim_keys(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if any(not claim_key.strip() for claim_key in value):
            raise ValueError("claim_keys entries must be non-blank")
        canonical = tuple(sorted(set(value)))
        if value != canonical:
            raise ValueError("claim_keys must be unique and sorted")
        return value

    @model_validator(mode="after")
    def _summary_identity(self) -> "BusinessModelSemanticClaimBinding":
        if self.item_kind == "summary" and self.item_key != "business_model":
            raise ValueError("summary semantic binding key must be 'business_model'")
        return self


def canonical_business_model_semantic_claim_bindings(
    value: object,
) -> tuple[BusinessModelSemanticClaimBinding, ...]:
    """Decode and merge claim keys by semantic identity in canonical order."""

    if not isinstance(value, Sequence) or isinstance(
        value, (str, bytes, bytearray)
    ):
        raise TypeError("semantic_claim_bindings must be a sequence")
    by_identity: dict[
        tuple[str, str], BusinessModelSemanticClaimBinding
    ] = {}
    for raw in value:
        row = (
            raw
            if isinstance(raw, BusinessModelSemanticClaimBinding)
            else BusinessModelSemanticClaimBinding.model_validate_json(
                canonical_json_bytes(raw)
            )
        )
        identity = (row.item_kind, row.item_key)
        prior = by_identity.get(identity)
        if prior is not None and row.item_kind != "cost":
            raise ValueError("semantic_claim_bindings item identity must be unique")
        by_identity[identity] = (
            row
            if prior is None
            else build_business_model_semantic_claim_binding(
                item_kind=row.item_kind,
                item_key=row.item_key,
                claim_keys=(*prior.claim_keys, *row.claim_keys),
            )
        )
    return tuple(by_identity[key] for key in sorted(by_identity))


def build_business_model_semantic_claim_binding(
    *,
    item_kind: BusinessModelSemanticItemKind,
    item_key: str,
    claim_keys: Sequence[str],
) -> BusinessModelSemanticClaimBinding:
    """Build one canonical semantic claim binding from deterministic inputs."""

    return BusinessModelSemanticClaimBinding(
        item_kind=item_kind,
        item_key=item_key,
        claim_keys=tuple(sorted(set(claim_keys))),
    )


__all__ = [
    "BusinessModelSemanticClaimBinding",
    "BusinessModelSemanticItemKind",
    "build_business_model_semantic_claim_binding",
    "canonical_business_model_semantic_claim_bindings",
]
