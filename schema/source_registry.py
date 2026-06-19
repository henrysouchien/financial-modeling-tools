from __future__ import annotations

import hashlib
import json
import re
from typing import Any

from .thesis_shared_slice import Excerpt, ScalarValue, SourceId, SourceRecord, SourceType


_SOURCE_ID_RE = re.compile(r"^src_([1-9]\d*)$")


def compute_identity_hash(
    type: SourceType,
    source_id: str,
    endpoint_or_filing_id: str | None,
    key_fields: dict[str, ScalarValue] | None,
    *,
    skill_name: str | None = None,
    artifact_path: str | None = None,
    artifact_id: str | None = None,
    skill_run_id: str | None = None,
    source_path: str | None = None,
) -> str:
    payload = {
        "type": type,
        "source_id": str(source_id or "").strip(),
        "endpoint_or_filing_id": _normalize_optional(endpoint_or_filing_id),
        "key_fields": _normalize_key_fields(key_fields),
    }
    skill_artifact = _normalize_skill_artifact_fields(
        skill_name=skill_name,
        artifact_path=artifact_path,
        artifact_id=artifact_id,
        skill_run_id=skill_run_id,
        source_path=source_path,
    )
    if skill_artifact:
        payload["skill_artifact"] = skill_artifact
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def compute_excerpt_hash(
    source_identity_hash: str,
    text: str,
    locator: dict[str, Any],
) -> str:
    payload = {
        "source_identity_hash": str(source_identity_hash or "").strip(),
        "text": str(text or "").strip(),
        "locator": locator,
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def next_source_id(existing: list[SourceRecord]) -> SourceId:
    max_id = 0
    for source in existing:
        match = _SOURCE_ID_RE.match(str(source.id or "").strip())
        if match is None:
            continue
        max_id = max(max_id, int(match.group(1)))
    return f"src_{max_id + 1}"


def register_source(
    sources: list[SourceRecord],
    candidate: SourceRecord,
) -> tuple[SourceId, list[SourceRecord]]:
    identity_hash = compute_identity_hash(
        candidate.type,
        candidate.source_id,
        candidate.endpoint_or_filing_id,
        candidate.key_fields,
        skill_name=candidate.skill_name,
        artifact_path=candidate.artifact_path,
        artifact_id=candidate.artifact_id,
        skill_run_id=candidate.skill_run_id,
        source_path=candidate.source_path,
    )
    candidate_identity = _identity_payload(candidate)
    candidate_excerpts = _normalize_excerpts(candidate.excerpts, identity_hash)

    updated_sources = list(sources)
    for index, source in enumerate(updated_sources):
        source_hash = source.identity_hash or compute_identity_hash(
            source.type,
            source.source_id,
            source.endpoint_or_filing_id,
            source.key_fields,
            skill_name=source.skill_name,
            artifact_path=source.artifact_path,
            artifact_id=source.artifact_id,
            skill_run_id=source.skill_run_id,
            source_path=source.source_path,
        )
        if source_hash != identity_hash:
            continue
        if _identity_payload(source) != candidate_identity:
            raise ValueError(
                "source identity hash collision for "
                f"{source.id}: {identity_hash}"
            )
        updates: dict[str, Any] = {}
        if source.identity_hash != identity_hash:
            updates["identity_hash"] = identity_hash
        merged_excerpts = _merge_excerpts(
            source.excerpts,
            candidate_excerpts,
            identity_hash,
            source_id=str(source.id),
        )
        if merged_excerpts != source.excerpts:
            updates["excerpts"] = merged_excerpts
        if updates:
            updated_sources[index] = source.model_copy(update=updates)
        return source.id, updated_sources

    minted_id = next_source_id(updated_sources)
    registered = candidate.model_copy(
        update={
            "id": minted_id,
            "identity_hash": identity_hash,
            "excerpts": candidate_excerpts,
        }
    )
    updated_sources.append(registered)
    return minted_id, updated_sources


def _identity_payload(source: SourceRecord) -> dict[str, Any]:
    payload = {
        "type": source.type,
        "source_id": str(source.source_id or "").strip(),
        "endpoint_or_filing_id": _normalize_optional(source.endpoint_or_filing_id),
        "key_fields": _normalize_key_fields(source.key_fields),
    }
    skill_artifact = _normalize_skill_artifact_fields(
        skill_name=source.skill_name,
        artifact_path=source.artifact_path,
        artifact_id=source.artifact_id,
        skill_run_id=source.skill_run_id,
        source_path=source.source_path,
    )
    if skill_artifact:
        payload["skill_artifact"] = skill_artifact
    return payload


def _normalize_optional(value: object | None) -> str | None:
    if value is None:
        return None
    normalized = str(value).strip()
    return normalized or None


def _normalize_skill_artifact_fields(
    *,
    skill_name: str | None,
    artifact_path: str | None,
    artifact_id: str | None,
    skill_run_id: str | None,
    source_path: str | None,
) -> dict[str, str]:
    values = {
        "skill_name": _normalize_optional(skill_name),
        "artifact_path": _normalize_optional(artifact_path),
        "artifact_id": _normalize_optional(artifact_id),
        "skill_run_id": _normalize_optional(skill_run_id),
        "source_path": _normalize_optional(source_path),
    }
    return {key: value for key, value in values.items() if value is not None}


def _locator_payload(excerpt: Excerpt) -> dict[str, Any]:
    return excerpt.locator.model_dump(mode="json", exclude_none=True)


def _excerpt_identity(excerpt: Excerpt) -> dict[str, Any]:
    return {
        "text": str(excerpt.text or "").strip(),
        "locator": _locator_payload(excerpt),
    }


def _normalize_excerpts(
    excerpts: list[Excerpt],
    source_identity_hash: str,
) -> list[Excerpt]:
    normalized: list[Excerpt] = []
    for excerpt in excerpts:
        excerpt_hash = excerpt.hash or compute_excerpt_hash(
            source_identity_hash,
            excerpt.text,
            _locator_payload(excerpt),
        )
        normalized.append(excerpt.model_copy(update={"hash": excerpt_hash}))
    return normalized


def _merge_excerpts(
    existing: list[Excerpt],
    candidates: list[Excerpt],
    source_identity_hash: str,
    *,
    source_id: str,
) -> list[Excerpt]:
    merged: list[Excerpt] = []
    by_hash: dict[str, int] = {}
    for excerpt in _normalize_excerpts(existing, source_identity_hash) + candidates:
        excerpt_hash = excerpt.hash or compute_excerpt_hash(
            source_identity_hash,
            excerpt.text,
            _locator_payload(excerpt),
        )
        normalized = excerpt.model_copy(update={"hash": excerpt_hash})
        existing_index = by_hash.get(excerpt_hash)
        if existing_index is None:
            by_hash[excerpt_hash] = len(merged)
            merged.append(normalized)
            continue
        current = merged[existing_index]
        if _excerpt_identity(current) != _excerpt_identity(normalized):
            raise ValueError(
                "excerpt hash collision for "
                f"{source_id}: {excerpt_hash}"
            )
        claim_ids = sorted(
            {
                *current.claim_ids,
                *normalized.claim_ids,
            }
        )
        if claim_ids != current.claim_ids:
            merged[existing_index] = current.model_copy(update={"claim_ids": claim_ids})
    return merged


def _normalize_key_fields(
    key_fields: dict[str, ScalarValue] | None,
) -> dict[str, ScalarValue] | None:
    if key_fields is None:
        return None
    return {str(key): value for key, value in sorted(key_fields.items())}


__all__ = [
    "compute_excerpt_hash",
    "compute_identity_hash",
    "next_source_id",
    "register_source",
]
