"""Single verified decode boundary for persisted and reconstructed ModelSpecs."""

from __future__ import annotations

from typing import Any, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, ValidationError

from .build_model_product_contracts import ModelSpec
from .render_state import compiler_input_content_hash
from .versioned_artifact import canonical_json_bytes


VerificationMode = Literal[
    "persisted_model_version",
    "single_boundary",
    "writer",
]

VERIFICATION_MODE_SINGLE_BOUNDARY: VerificationMode = "single_boundary"


class ModelSpecRebuildRequired(ValueError):
    """A digest-intact stored ModelSpec cannot be read by this binary."""

    code = "model_spec_rebuild_required"
    fix = (
        "rebuild and republish this model on the current contract generation; "
        "stored ModelVersions are never migrated"
    )

class VerifiedModelSpecDecodeRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    payload_mapping: Mapping[str, Any]
    compiler_input_hash: str = Field(pattern=r"^sha256:[0-9a-f]{64}$")
    external_model_version_digest: str | None = Field(
        default=None,
        pattern=r"^(?:sha256:)?[0-9a-f]{64}$",
    )


class VerifiedModelSpecResult(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, arbitrary_types_allowed=True)

    model_spec: ModelSpec
    verification_mode: VerificationMode


def _canonical_digest(value: str) -> str:
    normalized = str(value).removeprefix("sha256:")
    return f"sha256:{normalized}"


def _decode_canonical(payload: Mapping[str, Any]) -> ModelSpec:
    # Writer validation intentionally does not use the stored rebuild-required
    # wrapper: uncommitted bytes can be repaired before publication and must
    # retain the ordinary Pydantic validation answer.
    # Compiler inputs are persisted canonical JSON. Re-enter through Pydantic's
    # JSON boundary so strict tuple, enum, and Decimal fields are decoded from
    # their canonical wire representations instead of being rejected as
    # untrusted Python coercions.
    return ModelSpec.model_validate_json(canonical_json_bytes(dict(payload)))


def _decode_persisted_canonical(payload: Mapping[str, Any]) -> ModelSpec:
    """Return the stored ModelSpec or its one typed rebuild-required refusal.

    ``ModelSpec`` owns version admission: its ``schema_version`` field states
    which labels this binary reads, so a digest-intact payload is decoded
    first and refused only when the contract classes reject it.
    """

    try:
        return _decode_canonical(payload)
    except ValidationError as exc:
        first = exc.errors()[0]
        location = ".".join(str(part) for part in first["loc"]) or "<root>"
        raise ModelSpecRebuildRequired(
            "stored ModelSpec does not satisfy this binary's contract "
            f"({exc.error_count()} rejections, first at {location}: "
            f"{first['msg']}); the stored ModelSpec is rebuild-required — "
            f"{ModelSpecRebuildRequired.fix}"
        ) from exc


def decode_verified_model_spec_compiler_input(
    request: VerifiedModelSpecDecodeRequest,
) -> VerifiedModelSpecResult:
    """Verify untouched compiler bytes, then decode exact ModelSpec 3.0."""

    request = VerifiedModelSpecDecodeRequest.model_validate(request)
    actual = compiler_input_content_hash(dict(request.payload_mapping))
    if actual != request.compiler_input_hash:
        raise ValueError("persisted ModelSpec differs from compiler input hash")
    if request.external_model_version_digest is not None:
        if _canonical_digest(request.external_model_version_digest) != actual:
            raise ValueError("persisted ModelSpec differs from ModelVersion digest")
        mode: VerificationMode = "persisted_model_version"
    else:
        mode = VERIFICATION_MODE_SINGLE_BOUNDARY
    return VerifiedModelSpecResult(
        model_spec=_decode_persisted_canonical(request.payload_mapping),
        verification_mode=mode,
    )


def decode_model_spec_for_writer(
    request: VerifiedModelSpecDecodeRequest,
) -> VerifiedModelSpecResult:
    """Verify an uncommitted successor input before its external digest exists."""

    request = VerifiedModelSpecDecodeRequest.model_validate(request)
    if request.external_model_version_digest is not None:
        raise ValueError("writer decode forbids a pre-existing ModelVersion digest")
    actual = compiler_input_content_hash(dict(request.payload_mapping))
    if actual != request.compiler_input_hash:
        raise ValueError("successor ModelSpec differs from compiler input hash")
    return VerifiedModelSpecResult(
        model_spec=_decode_canonical(request.payload_mapping),
        verification_mode="writer",
    )


def decode_verified_model_spec_result(value: Any) -> VerifiedModelSpecResult:
    """Decode a verified-result object or its canonical JSON wire mapping."""

    if isinstance(value, VerifiedModelSpecResult):
        return value
    if not isinstance(value, Mapping):
        raise TypeError("verified ModelSpec result must be an object or mapping")
    wire_value = dict(value)
    model_spec_payload = wire_value.get("model_spec")
    if isinstance(model_spec_payload, Mapping):
        wire_value["model_spec"] = _decode_persisted_canonical(model_spec_payload)
    return VerifiedModelSpecResult.model_validate(wire_value)


__all__ = [
    "ModelSpecRebuildRequired",
    "VERIFICATION_MODE_SINGLE_BOUNDARY",
    "VerificationMode",
    "VerifiedModelSpecDecodeRequest",
    "VerifiedModelSpecResult",
    "decode_model_spec_for_writer",
    "decode_verified_model_spec_result",
    "decode_verified_model_spec_compiler_input",
]
