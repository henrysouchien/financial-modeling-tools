"""Exact compiled-output authority for post-compile valuation judgment."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Callable

from schema.methodology_loader import parse_frontmatter
from schema.postcompile_valuation import (
    CompiledValuationSnapshot,
    PostcompileValuationMethodologyPin,
)
from schema.postcompile_valuation_artifact import PostcompileValuationArtifact


METHODOLOGY_NAME = "dcf-relative-valuation"
METHODOLOGY_PATH = (
    Path(__file__).resolve().parents[1]
    / "memory"
    / "workspace"
    / "notes"
    / "methodology"
    / "valuation-modeling"
    / f"{METHODOLOGY_NAME}.md"
)



class PostcompileValuationServiceError(ValueError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def postcompile_valuation_methodology_pin() -> PostcompileValuationMethodologyPin:
    raw = METHODOLOGY_PATH.read_bytes()
    frontmatter, _ = parse_frontmatter(raw.decode("utf-8"))
    if (
        str(frontmatter.get("name") or "").strip() != METHODOLOGY_NAME
        or not str(frontmatter.get("version") or "").strip()
    ):
        raise PostcompileValuationServiceError(
            "postcompile_valuation_methodology_invalid",
            "dcf-relative-valuation methodology frontmatter is invalid",
        )
    return PostcompileValuationMethodologyPin(
        declared_version=str(frontmatter["version"]).strip(),
        content_sha256=hashlib.sha256(raw).hexdigest(),
    )




def validate_postcompile_valuation_authority(
    record: PostcompileValuationArtifact,
    *,
    snapshot: CompiledValuationSnapshot,
) -> PostcompileValuationArtifact:
    value = PostcompileValuationArtifact.model_validate_json(record.model_dump_json())
    expected = CompiledValuationSnapshot.model_validate_json(snapshot.model_dump_json())
    if value.snapshot != expected:
        raise PostcompileValuationServiceError(
            "postcompile_valuation_snapshot_mismatch",
            "postcompile valuation differs from exact compiled outputs",
        )
    return value


class PostcompileValuationService:
    def __init__(self, repository: object) -> None:
        self._repository = repository

    def persist(
        self,
        record: PostcompileValuationArtifact,
        *,
        snapshot: CompiledValuationSnapshot,
    ) -> PostcompileValuationArtifact:
        value = validate_postcompile_valuation_authority(
            record,
            snapshot=snapshot,
        )
        creator: (
            Callable[[PostcompileValuationArtifact], PostcompileValuationArtifact]
            | None
        ) = getattr(self._repository, "create_postcompile_valuation", None)
        if not callable(creator):
            raise PostcompileValuationServiceError(
                "postcompile_valuation_store_unavailable",
                "repository cannot persist postcompile valuation artifacts",
            )
        stored = creator(value)
        # A readback that differs from the planned record is corrupt storage:
        # publishing those bytes would record a valuation the writer did not plan.
        # Operator corruption, not an agent retry.
        if stored != value:
            raise PostcompileValuationServiceError(
                "postcompile_valuation_readback_mismatch",
                "persisted postcompile valuation differs from planned bytes",
            )
        return stored


__all__ = [
    "METHODOLOGY_NAME",
    "METHODOLOGY_PATH",
    "PostcompileValuationService",
    "PostcompileValuationServiceError",
    "postcompile_valuation_methodology_pin",
    "validate_postcompile_valuation_authority",
]
