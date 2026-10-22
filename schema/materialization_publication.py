"""Canonical Thesis acceptance ancestry and journal effect identity contracts."""

from __future__ import annotations

from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .business_model_revision import BusinessModelRevisionRef
from .model_ref import ModelVersionRef
from .model_state_identity import validate_normalized_ticker, validate_sha256_hex
from .versioned_artifact import (
    CanonicalUserArtifactScope,
    GlobalArtifactScope,
    VersionedArtifactRef,
    WorkspacePreviewResearchFileArtifactScope,
)


class _StrictPublicationContract(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True, frozen=True)


def _validate_canonical_text(value: str, *, field_name: str) -> None:
    if not value or value.strip() != value:
        raise ValueError(f"{field_name} must be canonical non-empty text")


def _validate_ref_hash(
    ref: VersionedArtifactRef,
    companion_hash: str,
    *,
    field_name: str,
) -> None:
    validate_sha256_hex(companion_hash, field_name=field_name)
    if companion_hash != ref.content_sha256:
        raise ValueError(f"{field_name} must equal the exact ref content hash")


def _artifact_route(ref: VersionedArtifactRef) -> tuple[str, int, str]:
    scope = ref.scope
    try:
        if isinstance(scope, (GlobalArtifactScope, CanonicalUserArtifactScope)):
            raise AttributeError(
                f"{type(scope).__name__} has no research-file identity"
            )
        route = (scope.user_id, scope.research_file_id, scope.ticker)
    except AttributeError as exc:
        raise ValueError(
            f"{ref.artifact_kind} must use a research-file scope"
        ) from exc
    validate_normalized_ticker(route[2])
    return route


def _validate_registered_json_ref(
    ref: VersionedArtifactRef,
    *,
    field_name: str,
    artifact_kind: str,
    payload_schema_version: str,
    provider_id: str,
    thesis_id: str,
) -> None:
    if (
        ref.artifact_kind != artifact_kind
        or ref.provider_id != provider_id
        or ref.artifact_version != 1
        or ref.payload_schema_version != payload_schema_version
        or ref.media_type != "application/json"
        or ref.digest_codec != "canonical_json_v1"
    ):
        raise ValueError(f"{field_name} does not match its exact ref contract")
    _validate_canonical_text(ref.artifact_id, field_name=f"{field_name}.artifact_id")
    if getattr(ref.scope, "thesis_id", None) != thesis_id:
        raise ValueError(f"{field_name} crosses the Thesis identity")


class NonS4ThesisAcceptanceAncestry(_StrictPublicationContract):
    ancestry_kind: Literal["non_s4"]


class S4PromotedThesisAcceptanceAncestry(_StrictPublicationContract):
    ancestry_kind: Literal["s4_promoted_canonical"]
    parent_preview_manifest_ref: VersionedArtifactRef
    parent_preview_manifest_hash: str
    last_preview_stage_binding_ref: VersionedArtifactRef
    last_preview_stage_binding_hash: str
    diligence_pr_id: str
    merge_binding_digest: str
    selected_business_model_revision_ref: BusinessModelRevisionRef
    selected_business_model_revision_epoch: int = Field(gt=0)
    selected_model_version_ref: ModelVersionRef
    canonical_acceptance_continuation_ref: VersionedArtifactRef
    canonical_acceptance_continuation_hash: str

    @model_validator(mode="after")
    def _validate_ancestry(self) -> "S4PromotedThesisAcceptanceAncestry":
        pairs = (
            (
                "parent_preview_manifest_ref",
                "parent_preview_manifest_hash",
                "build_manifest",
                "build-manifest.v2",
                "artifact_store.workspace_research_file.v1",
            ),
            (
                "last_preview_stage_binding_ref",
                "last_preview_stage_binding_hash",
                "acceptance_stage_binding",
                "acceptance-stage-binding.v1",
                "artifact_store.workspace_research_file.v1",
            ),
            (
                "canonical_acceptance_continuation_ref",
                "canonical_acceptance_continuation_hash",
                "canonical_acceptance_continuation",
                "canonical-acceptance-continuation.v2",
                "artifact_store.canonical_research_file.v1",
            ),
        )
        _validate_canonical_text(self.diligence_pr_id, field_name="diligence_pr_id")
        validate_sha256_hex(
            self.merge_binding_digest.removeprefix("sha256:"),
            field_name="merge_binding_digest",
        )
        route: tuple[str, int, str] | None = None
        thesis_id: str | None = None
        preview_scope: WorkspacePreviewResearchFileArtifactScope | None = None
        for (
            ref_field,
            hash_field,
            artifact_kind,
            payload_schema_version,
            provider_id,
        ) in pairs:
            ref = getattr(self, ref_field)
            _validate_ref_hash(
                ref,
                getattr(self, hash_field),
                field_name=hash_field,
            )
            current_route = _artifact_route(ref)
            current_thesis_id = getattr(ref.scope, "thesis_id", None)
            if route is None:
                route = current_route
                thesis_id = current_thesis_id
            if current_route != route or current_thesis_id != thesis_id:
                raise ValueError(
                    "S4 ancestry artifacts must share one exact route and Thesis"
                )
            assert thesis_id is not None
            _validate_registered_json_ref(
                ref,
                field_name=ref_field,
                artifact_kind=artifact_kind,
                payload_schema_version=payload_schema_version,
                provider_id=provider_id,
                thesis_id=thesis_id,
            )
            if isinstance(ref.scope, WorkspacePreviewResearchFileArtifactScope):
                if preview_scope is None:
                    preview_scope = ref.scope
                elif ref.scope != preview_scope:
                    raise ValueError(
                        "S4 preview ancestry artifacts must share one exact scope"
                    )
        return self


ThesisAcceptanceAncestry = Annotated[
    NonS4ThesisAcceptanceAncestry | S4PromotedThesisAcceptanceAncestry,
    Field(discriminator="ancestry_kind"),
]


class MaterializationEffectIdentity(_StrictPublicationContract):
    plan_effect_index: int = Field(ge=0)
    effect_id: str
    effect_kind: str
    criticality: str
    dependency_ids: tuple[str, ...]
    payload_schema_version: str
    payload_digest: str

    @model_validator(mode="after")
    def _validate_effect(self) -> "MaterializationEffectIdentity":
        for field_name in (
            "effect_id",
            "effect_kind",
            "criticality",
            "payload_schema_version",
        ):
            _validate_canonical_text(getattr(self, field_name), field_name=field_name)
        validate_sha256_hex(self.payload_digest, field_name="payload_digest")
        if self.dependency_ids != tuple(sorted(set(self.dependency_ids))):
            raise ValueError("dependency_ids must be sorted and unique")
        return self


class EffectOutcome(_StrictPublicationContract):
    """Exact immutable-store outcome carried by the canonical journal receipt."""

    store_id: str
    outcome: Literal["written", "skipped"]
    final_hash: str

    @model_validator(mode="after")
    def _validate_outcome(self) -> "EffectOutcome":
        _validate_canonical_text(self.store_id, field_name="store_id")
        validate_sha256_hex(self.final_hash, field_name="final_hash")
        return self


class EffectDigest(_StrictPublicationContract):
    """Exact SQL-stage/projection digest carried by the journal receipt."""

    effect_id: str
    detail_digest: str

    @model_validator(mode="after")
    def _validate_effect_digest(self) -> "EffectDigest":
        _validate_canonical_text(self.effect_id, field_name="effect_id")
        validate_sha256_hex(self.detail_digest, field_name="detail_digest")
        return self



__all__ = [
    "EffectDigest",
    "EffectOutcome",
    "MaterializationEffectIdentity",
    "NonS4ThesisAcceptanceAncestry",
    "S4PromotedThesisAcceptanceAncestry",
    "ThesisAcceptanceAncestry",
]
