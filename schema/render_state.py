"""Helpers for the single atomic workbook render-state contract."""

from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any, Mapping, Sequence, TypedDict

from .handoff import ModelRenderState


_SHA256_REF_RE = re.compile(r"^sha256:([0-9a-f]{64})$")


def compiler_input_content_hash(value: Any) -> str:
    if hasattr(value, "model_dump"):
        value = value.model_dump(mode="json")
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        default=str,
    ).encode()
    return f"sha256:{hashlib.sha256(encoded).hexdigest()}"


class UnstoredSourceBundleRow(TypedDict):
    source_bundle_payload: dict[str, Any]
    source_bundle_content_sha256: str


def unstored_source_bundle_row(source_bundle: Any) -> UnstoredSourceBundleRow:
    """The ``source_bundle_payload`` / ``source_bundle_content_sha256`` pair a compile
    binds, for a SourceBundle that was never stored.

    Production reads the pair from the ``source_bundles`` row (its raw payload and
    the ``content_sha256`` the repository computed over it). A bundle built in
    memory — a golden capture, a test — has no row, so its own canonical dump
    sealed by the same hash is that row.
    """
    payload: dict[str, Any] = source_bundle.model_dump(mode="json")
    return UnstoredSourceBundleRow(
        source_bundle_payload=payload,
        source_bundle_content_sha256=compiler_input_content_hash(payload).removeprefix("sha256:"),
    )


def compiler_input_artifact_id(kind: str, content_hash: str) -> str:
    match = _SHA256_REF_RE.fullmatch(str(content_hash or "").strip())
    if match is None:
        raise ValueError("compiler-input content hash must be a sha256 ref")
    normalized_kind = str(kind or "").strip().replace("_", "-")
    if normalized_kind not in {"business-model", "segment-profile"}:
        raise ValueError(f"unsupported compiler-input artifact kind: {kind!r}")
    return f"compiler-input-{normalized_kind}-{match.group(1)}"


def render_state_payload(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    try:
        if isinstance(value, ModelRenderState):
            state = value
        else:
            state = ModelRenderState.model_validate(value)
    except (TypeError, ValueError) as exc:
        logging.warning(
            "Discarding invalid ModelRenderState payload as absent | %s: %s",
            type(exc).__name__,
            exc,
        )
        return None
    return state.model_dump(mode="json")


def render_state_hash(value: Any) -> str:
    payload = render_state_payload(value)
    if payload is None:
        return "absent"
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode()
    return hashlib.sha256(encoded).hexdigest()


def advance_workbook_semantics_hash(value: Any, new_hash: str) -> dict[str, Any]:
    payload = render_state_payload(value)
    if payload is None:
        raise ValueError("REBUILD_REQUIRED_RENDER_STATE_MISSING")
    payload["workbook_semantics_hash"] = str(new_hash)
    return ModelRenderState.model_validate(payload).model_dump(mode="json")


def construct_model_render_state(
    *,
    model_id: str,
    model_version: int,
    workbook_semantics_hash: str,
    compiler_inputs: Mapping[str, Any],
    seed_coverage_manifest: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Construct the sole canonical render-state shape for any producer."""

    normalized_inputs = dict(compiler_inputs)
    source_bundle = normalized_inputs.get("source_bundle")
    model_spec = normalized_inputs.get("model_spec")
    if not isinstance(source_bundle, Mapping):
        raise ValueError("render compiler inputs require SourceBundle")
    if not isinstance(model_spec, Mapping):
        raise ValueError("render compiler inputs require ModelSpec")
    source_bundle_digest = str(source_bundle.get("content_hash") or "")
    model_spec_digest = str(model_spec.get("content_hash") or "")
    build_id = (
        f"{source_bundle_digest}:{model_spec_digest}:"
        f"{int(model_version)}:{workbook_semantics_hash}"
    )
    seed_manifest = [
        {**dict(row), "build_identity": build_id}
        for row in seed_coverage_manifest
    ]
    return ModelRenderState.model_validate(
        {
            "schema_version": "1.0",
            "workbook_semantics_hash": str(workbook_semantics_hash),
            "compiler_inputs": normalized_inputs,
            "seed_coverage_manifest": seed_manifest,
            "identity": {
                "build_id": build_id,
                "model_id": str(model_id),
                "model_version": int(model_version),
                "source_bundle_digest": source_bundle_digest,
                "model_spec_digest": model_spec_digest,
            },
        }
    ).model_dump(mode="json")


__all__ = [
    "advance_workbook_semantics_hash",
    "compiler_input_artifact_id",
    "compiler_input_content_hash",
    "construct_model_render_state",
    "render_state_hash",
    "render_state_payload",
]
