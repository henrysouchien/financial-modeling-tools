"""Strict versioned-artifact identity, URI, and integrity contracts.

The module defines canonical scopes, refs, URI grammar, and hashing without
provider registries or storage interfaces.
"""

from __future__ import annotations

import hashlib
import json
import math
import threading
from collections import OrderedDict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Annotated, Any, Literal, Mapping, TypeVar
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .model_state_identity import (
    decode_canonical_uri_segment,
    encode_uri_segment,
    parse_positive_decimal,
    validate_normalized_ticker,
    validate_sha256_hex,
)


DigestCodec = Literal["canonical_json_v1", "raw_bytes_v1", "bundle_manifest_v1"]
ArtifactScopeKind = Literal[
    "global",
    "canonical_user",
    "canonical_research_file",
    "workspace_preview_research_file",
    "canonical_model",
    "workspace_preview_model",
    "canonical_materialization",
    "workspace_preview_materialization",
]
ArtifactUriFamily = Literal[
    "global.v1",
    "canonical_user.v1",
    "canonical_research_file.v1",
    "workspace_preview_research_file.v1",
    "canonical_model.v1",
    "workspace_preview_model.v1",
    "canonical_materialization.v1",
    "workspace_preview_materialization.v1",
]


def _validate_identifier(value: str, *, field_name: str) -> str:
    encode_uri_segment(value, field_name=field_name)
    return value


class StrictArtifactScope(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class GlobalArtifactScope(StrictArtifactScope):
    scope_kind: Literal["global"] = "global"


class CanonicalUserArtifactScope(StrictArtifactScope):
    scope_kind: Literal["canonical_user"] = "canonical_user"
    user_id: str

    @model_validator(mode="after")
    def _validate_scope(self) -> "CanonicalUserArtifactScope":
        _validate_identifier(self.user_id, field_name="user_id")
        return self


class CanonicalResearchFileArtifactScope(StrictArtifactScope):
    scope_kind: Literal["canonical_research_file"] = "canonical_research_file"
    user_id: str
    research_file_id: int = Field(gt=0)
    ticker: str
    thesis_id: str | None = None

    @model_validator(mode="after")
    def _validate_scope(self) -> "CanonicalResearchFileArtifactScope":
        _validate_common_research_scope(self)
        return self


class WorkspacePreviewResearchFileArtifactScope(StrictArtifactScope):
    scope_kind: Literal["workspace_preview_research_file"] = (
        "workspace_preview_research_file"
    )
    user_id: str
    research_file_id: int = Field(gt=0)
    ticker: str
    thesis_id: str | None = None
    workspace_id: str
    canonical_base_hash: str

    @model_validator(mode="after")
    def _validate_scope(self) -> "WorkspacePreviewResearchFileArtifactScope":
        _validate_common_research_scope(self)
        _validate_workspace_scope(self)
        return self


class CanonicalModelArtifactScope(StrictArtifactScope):
    scope_kind: Literal["canonical_model"] = "canonical_model"
    user_id: str
    research_file_id: int = Field(gt=0)
    ticker: str
    thesis_id: str
    model_id: str
    model_version: int = Field(gt=0)

    @model_validator(mode="after")
    def _validate_scope(self) -> "CanonicalModelArtifactScope":
        _validate_common_model_scope(self)
        return self


class WorkspacePreviewModelArtifactScope(StrictArtifactScope):
    scope_kind: Literal["workspace_preview_model"] = "workspace_preview_model"
    user_id: str
    research_file_id: int = Field(gt=0)
    ticker: str
    thesis_id: str
    workspace_id: str
    canonical_base_hash: str
    model_id: str
    model_version: int = Field(gt=0)

    @model_validator(mode="after")
    def _validate_scope(self) -> "WorkspacePreviewModelArtifactScope":
        _validate_common_model_scope(self)
        _validate_workspace_scope(self)
        return self


class CanonicalMaterializationArtifactScope(StrictArtifactScope):
    scope_kind: Literal["canonical_materialization"] = "canonical_materialization"
    user_id: str
    research_file_id: int = Field(gt=0)
    ticker: str
    thesis_id: str
    model_id: str
    model_version: int = Field(gt=0)
    materialization_revision_id: str

    @model_validator(mode="after")
    def _validate_scope(self) -> "CanonicalMaterializationArtifactScope":
        _validate_common_materialization_scope(self)
        return self


class WorkspacePreviewMaterializationArtifactScope(StrictArtifactScope):
    scope_kind: Literal["workspace_preview_materialization"] = (
        "workspace_preview_materialization"
    )
    user_id: str
    research_file_id: int = Field(gt=0)
    ticker: str
    thesis_id: str
    workspace_id: str
    canonical_base_hash: str
    model_id: str
    model_version: int = Field(gt=0)
    materialization_revision_id: str

    @model_validator(mode="after")
    def _validate_scope(self) -> "WorkspacePreviewMaterializationArtifactScope":
        _validate_common_materialization_scope(self)
        _validate_workspace_scope(self)
        return self


def _validate_common_research_scope(scope: Any) -> None:
    _validate_identifier(scope.user_id, field_name="user_id")
    validate_normalized_ticker(scope.ticker)
    if scope.thesis_id is not None:
        _validate_identifier(scope.thesis_id, field_name="thesis_id")


def _validate_common_model_scope(scope: Any) -> None:
    _validate_identifier(scope.user_id, field_name="user_id")
    validate_normalized_ticker(scope.ticker)
    _validate_identifier(scope.thesis_id, field_name="thesis_id")
    _validate_identifier(scope.model_id, field_name="model_id")


def _validate_common_materialization_scope(scope: Any) -> None:
    _validate_common_model_scope(scope)
    _validate_identifier(
        scope.materialization_revision_id, field_name="materialization_revision_id"
    )


def _validate_workspace_scope(scope: Any) -> None:
    _validate_identifier(scope.workspace_id, field_name="workspace_id")
    validate_sha256_hex(scope.canonical_base_hash, field_name="canonical_base_hash")


VersionedArtifactScope = Annotated[
    GlobalArtifactScope
    | CanonicalUserArtifactScope
    | CanonicalResearchFileArtifactScope
    | WorkspacePreviewResearchFileArtifactScope
    | CanonicalModelArtifactScope
    | WorkspacePreviewModelArtifactScope
    | CanonicalMaterializationArtifactScope
    | WorkspacePreviewMaterializationArtifactScope,
    Field(discriminator="scope_kind"),
]


@dataclass(frozen=True)
class _WorkspaceArtifactUriIdentity:
    workspace_id: str
    canonical_base_hash: str


@dataclass(frozen=True)
class ParsedVersionedArtifactUri:
    uri_family: str
    scope: VersionedArtifactScope
    artifact_kind: str
    artifact_id: str
    artifact_version: int


def _segment(value: str, field_name: str) -> str:
    return encode_uri_segment(value, field_name=field_name)


def format_versioned_artifact_uri(
    *,
    scope: VersionedArtifactScope,
    artifact_kind: str,
    artifact_id: str,
    artifact_version: int,
) -> str:
    """Format the sole canonical URI for an exact artifact identity."""

    if isinstance(artifact_version, bool) or artifact_version <= 0:
        raise ValueError("artifact_version must be positive")
    kind = _segment(artifact_kind, "artifact_kind")
    artifact = _segment(artifact_id, "artifact_id")
    suffix = f"kinds/{kind}/ids/{artifact}/versions/{artifact_version}"

    if isinstance(scope, GlobalArtifactScope):
        return f"artifact://global/{suffix}"
    if isinstance(scope, CanonicalUserArtifactScope):
        return f"artifact://user/users/{_segment(scope.user_id, 'user_id')}/{suffix}"

    user = _segment(scope.user_id, "user_id")
    ticker = _segment(validate_normalized_ticker(scope.ticker), "ticker")
    prefix = f"users/{user}/research-files/{scope.research_file_id}/tickers/{ticker}"
    thesis_id = scope.thesis_id
    thesis = (
        ""
        if thesis_id is None
        else f"/theses/{_segment(thesis_id, 'thesis_id')}"
    )
    workspace = ""
    if isinstance(
        scope,
        (
            WorkspacePreviewResearchFileArtifactScope,
            WorkspacePreviewModelArtifactScope,
            WorkspacePreviewMaterializationArtifactScope,
        ),
    ):
        workspace = (
            f"/workspaces/{_segment(scope.workspace_id, 'workspace_id')}"
            f"/bases/{validate_sha256_hex(scope.canonical_base_hash, field_name='canonical_base_hash')}"
        )

    if isinstance(
        scope,
        (
            CanonicalResearchFileArtifactScope,
            WorkspacePreviewResearchFileArtifactScope,
        ),
    ):
        authority = (
            "canonical"
            if isinstance(scope, CanonicalResearchFileArtifactScope)
            else "workspace-preview"
        )
        return f"artifact://{authority}/{prefix}{thesis}{workspace}/{suffix}"

    model = (
        f"/models/{_segment(scope.model_id, 'model_id')}/versions/{scope.model_version}"
    )
    if isinstance(
        scope,
        (CanonicalModelArtifactScope, WorkspacePreviewModelArtifactScope),
    ):
        authority = (
            "canonical"
            if isinstance(scope, CanonicalModelArtifactScope)
            else "workspace-preview"
        )
        return f"artifact://{authority}/{prefix}{thesis}{workspace}{model}/{suffix}"

    materialization = f"/materializations/{_segment(scope.materialization_revision_id, 'materialization_revision_id')}"
    member_suffix = f"kinds/{kind}/members/{artifact}/versions/{artifact_version}"
    authority = (
        "canonical"
        if isinstance(scope, CanonicalMaterializationArtifactScope)
        else "workspace-preview"
    )
    return (
        f"materialization://{authority}/{prefix}{thesis}{workspace}{model}"
        f"{materialization}/{member_suffix}"
    )


def _decode_path(uri: str) -> tuple[str, str, list[str]]:
    if not isinstance(uri, str) or not uri:
        raise ValueError("artifact URI must be non-empty")
    parsed = urlsplit(uri)
    if parsed.scheme not in {"artifact", "materialization"}:
        raise ValueError("artifact URI scheme is not registered")
    if parsed.query or parsed.fragment or parsed.username or parsed.password:
        raise ValueError("artifact URI forbids query, fragment, and userinfo")
    try:
        if parsed.port is not None:
            raise ValueError("artifact URI forbids a port")
    except ValueError as exc:
        raise ValueError("artifact URI has an invalid authority") from exc
    if parsed.netloc not in {"global", "user", "canonical", "workspace-preview"}:
        raise ValueError("artifact URI authority is not registered")
    raw_parts = parsed.path.split("/")[1:]
    if not raw_parts or any(part == "" for part in raw_parts):
        raise ValueError("artifact URI contains an empty path segment")
    return parsed.scheme, parsed.netloc, raw_parts


def _expect_label(parts: list[str], index: int, label: str) -> None:
    if index >= len(parts) or parts[index] != label:
        raise ValueError(f"artifact URI expected {label!r}")


def _take_identifier(parts: list[str], index: int, field_name: str) -> str:
    if index >= len(parts):
        raise ValueError(f"artifact URI is missing {field_name}")
    return decode_canonical_uri_segment(parts[index], field_name=field_name)


def parse_versioned_artifact_uri(uri: str) -> ParsedVersionedArtifactUri:
    """Parse only the ten canonical v1 URI forms (covering eight scopes)."""

    scheme, authority, parts = _decode_path(uri)
    cursor = 0

    if scheme == "artifact" and authority == "global":
        if len(parts) != 6:
            raise ValueError("invalid global artifact URI shape")
        scope: VersionedArtifactScope = GlobalArtifactScope()
        family = "global.v1"
    elif scheme == "artifact" and authority == "user":
        if len(parts) != 8:
            raise ValueError("invalid canonical-user artifact URI shape")
        _expect_label(parts, 0, "users")
        scope = CanonicalUserArtifactScope(
            user_id=_take_identifier(parts, 1, "user_id")
        )
        family = "canonical_user.v1"
        cursor = 2
    elif authority == "canonical" or authority == "workspace-preview":
        if scheme == "materialization" and authority == "global":
            raise ValueError("invalid materialization authority")
        _expect_label(parts, 0, "users")
        user_id = _take_identifier(parts, 1, "user_id")
        _expect_label(parts, 2, "research-files")
        research_file_id = parse_positive_decimal(
            parts[3], field_name="research_file_id"
        )
        _expect_label(parts, 4, "tickers")
        ticker = validate_normalized_ticker(_take_identifier(parts, 5, "ticker"))
        cursor = 6
        thesis_id: str | None = None
        if cursor < len(parts) and parts[cursor] == "theses":
            thesis_id = _take_identifier(parts, cursor + 1, "thesis_id")
            cursor += 2
        workspace_identity: _WorkspaceArtifactUriIdentity | None = None
        if authority == "workspace-preview":
            _expect_label(parts, cursor, "workspaces")
            workspace_id = _take_identifier(parts, cursor + 1, "workspace_id")
            _expect_label(parts, cursor + 2, "bases")
            canonical_base_hash = validate_sha256_hex(
                parts[cursor + 3], field_name="canonical_base_hash"
            )
            workspace_identity = _WorkspaceArtifactUriIdentity(
                workspace_id=workspace_id,
                canonical_base_hash=canonical_base_hash,
            )
            cursor += 4

        if cursor < len(parts) and parts[cursor] == "models":
            model_id = _take_identifier(parts, cursor + 1, "model_id")
            _expect_label(parts, cursor + 2, "versions")
            model_version = parse_positive_decimal(
                parts[cursor + 3], field_name="model_version"
            )
            cursor += 4

            if scheme == "materialization":
                if thesis_id is None:
                    raise ValueError(
                        "materialization URI requires thesis/model identity"
                    )
                _expect_label(parts, cursor, "materializations")
                materialization_revision_id = _take_identifier(
                    parts, cursor + 1, "materialization_revision_id"
                )
                cursor += 2
                if workspace_identity is not None:
                    scope = WorkspacePreviewMaterializationArtifactScope(
                        user_id=user_id,
                        research_file_id=research_file_id,
                        ticker=ticker,
                        thesis_id=thesis_id,
                        workspace_id=workspace_identity.workspace_id,
                        canonical_base_hash=workspace_identity.canonical_base_hash,
                        model_id=model_id,
                        model_version=model_version,
                        materialization_revision_id=materialization_revision_id,
                    )
                    family = "workspace_preview_materialization.v1"
                else:
                    scope = CanonicalMaterializationArtifactScope(
                        user_id=user_id,
                        research_file_id=research_file_id,
                        ticker=ticker,
                        thesis_id=thesis_id,
                        model_id=model_id,
                        model_version=model_version,
                        materialization_revision_id=materialization_revision_id,
                    )
                    family = "canonical_materialization.v1"
            else:
                if thesis_id is None:
                    raise ValueError("model URI requires thesis identity")
                if workspace_identity is not None:
                    scope = WorkspacePreviewModelArtifactScope(
                        user_id=user_id,
                        research_file_id=research_file_id,
                        ticker=ticker,
                        thesis_id=thesis_id,
                        workspace_id=workspace_identity.workspace_id,
                        canonical_base_hash=workspace_identity.canonical_base_hash,
                        model_id=model_id,
                        model_version=model_version,
                    )
                    family = "workspace_preview_model.v1"
                else:
                    scope = CanonicalModelArtifactScope(
                        user_id=user_id,
                        research_file_id=research_file_id,
                        ticker=ticker,
                        thesis_id=thesis_id,
                        model_id=model_id,
                        model_version=model_version,
                    )
                    family = "canonical_model.v1"
        else:
            if scheme == "materialization":
                raise ValueError(
                    "materialization URI requires thesis/model identity"
                )
            if workspace_identity is not None:
                scope = WorkspacePreviewResearchFileArtifactScope(
                    user_id=user_id,
                    research_file_id=research_file_id,
                    ticker=ticker,
                    thesis_id=thesis_id,
                    workspace_id=workspace_identity.workspace_id,
                    canonical_base_hash=workspace_identity.canonical_base_hash,
                )
                family = "workspace_preview_research_file.v1"
            else:
                scope = CanonicalResearchFileArtifactScope(
                    user_id=user_id,
                    research_file_id=research_file_id,
                    ticker=ticker,
                    thesis_id=thesis_id,
                )
                family = "canonical_research_file.v1"
    else:
        raise ValueError("artifact URI scheme/authority pair is not registered")

    _expect_label(parts, cursor, "kinds")
    artifact_kind = _take_identifier(parts, cursor + 1, "artifact_kind")
    expected_member_label = "members" if scheme == "materialization" else "ids"
    _expect_label(parts, cursor + 2, expected_member_label)
    artifact_id = _take_identifier(parts, cursor + 3, "artifact_id")
    _expect_label(parts, cursor + 4, "versions")
    artifact_version = parse_positive_decimal(
        parts[cursor + 5], field_name="artifact_version"
    )
    if cursor + 6 != len(parts):
        raise ValueError("artifact URI has trailing or unregistered segments")

    result = ParsedVersionedArtifactUri(
        uri_family=family,
        scope=scope,
        artifact_kind=artifact_kind,
        artifact_id=artifact_id,
        artifact_version=artifact_version,
    )
    if (
        format_versioned_artifact_uri(
            scope=result.scope,
            artifact_kind=result.artifact_kind,
            artifact_id=result.artifact_id,
            artifact_version=result.artifact_version,
        )
        != uri
    ):
        raise ValueError("artifact URI does not round-trip canonically")
    return result


class VersionedArtifactRef(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    ref_schema_version: Literal["versioned-artifact-ref.v1"]
    provider_id: str
    artifact_kind: str
    artifact_id: str
    artifact_version: int = Field(gt=0)
    scope: VersionedArtifactScope
    uri: str
    payload_schema_version: str
    media_type: str
    digest_codec: DigestCodec
    content_sha256: str

    @model_validator(mode="after")
    def _validate_ref(self) -> "VersionedArtifactRef":
        _validate_identifier(self.provider_id, field_name="provider_id")
        _validate_identifier(self.artifact_kind, field_name="artifact_kind")
        _validate_identifier(self.artifact_id, field_name="artifact_id")
        _validate_identifier(
            self.payload_schema_version, field_name="payload_schema_version"
        )
        if not self.media_type or self.media_type.strip() != self.media_type:
            raise ValueError("media_type must be a non-empty canonical value")
        validate_sha256_hex(self.content_sha256, field_name="content_sha256")
        parsed = parse_versioned_artifact_uri(self.uri)
        if (
            parsed.scope != self.scope
            or parsed.artifact_kind != self.artifact_kind
            or parsed.artifact_id != self.artifact_id
            or parsed.artifact_version != self.artifact_version
        ):
            raise ValueError("artifact URI identity does not match embedded fields")
        return self


def _json_object_no_duplicates(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate canonical JSON key: {key!r}")
        result[key] = value
    return result


def _reject_nonfinite_constant(value: str) -> None:
    raise ValueError(f"non-finite canonical JSON number: {value}")


def _canonical_json_value(value: Any) -> Any:
    """The JSON-compatible form of ``value`` in one pass: models dumped, datetimes
    ISO-8601 with ``Z``, Decimals as strings, mappings and sequences as dicts and
    lists. Rejects non-finite numbers and non-string object keys. Exact built-in
    types short-circuit the ABC checks: a canonicalized artifact is mostly leaves."""

    kind = type(value)
    if kind is str or kind is int or kind is bool or value is None:
        return value
    if kind is float:
        if not math.isfinite(value):
            raise ValueError("canonical JSON rejects non-finite floats")
        return value
    if kind is dict or isinstance(value, Mapping):
        converted = {}
        for key, child in value.items():
            if not isinstance(key, str):
                raise ValueError("canonical JSON object keys must be strings")
            converted[key] = _canonical_json_value(child)
        return converted
    if kind is list or kind is tuple or isinstance(value, (list, tuple)):
        return [_canonical_json_value(child) for child in value]
    if isinstance(value, BaseModel):
        return _canonical_json_value(value.model_dump(mode="json"))
    if isinstance(value, datetime):
        return value.isoformat().replace("+00:00", "Z")
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("canonical JSON rejects non-finite decimals")
        return str(value)
    if isinstance(value, float) and not math.isfinite(value):
        raise ValueError("canonical JSON rejects non-finite floats")
    return value


def canonical_json_bytes(value: Any) -> bytes:
    """Return deterministic JSON bytes, rejecting duplicates and non-finite data."""

    supplied_bytes: bytes | None = None
    if isinstance(value, (bytes, bytearray, memoryview)):
        raw = supplied_bytes = bytes(value)
        try:
            value = json.loads(
                raw.decode("utf-8"),
                object_pairs_hook=_json_object_no_duplicates,
                parse_constant=_reject_nonfinite_constant,
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError(
                "canonical_json_v1 payload is not valid UTF-8 JSON"
            ) from exc
    elif isinstance(value, str):
        supplied_bytes = value.encode("utf-8")
        try:
            value = json.loads(
                value,
                object_pairs_hook=_json_object_no_duplicates,
                parse_constant=_reject_nonfinite_constant,
            )
        except json.JSONDecodeError as exc:
            raise ValueError("canonical_json_v1 payload is not valid JSON") from exc
    value = _canonical_json_value(value)
    try:
        encoded = json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=False,
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError) as exc:
        raise ValueError("value is not canonical-JSON serializable") from exc
    if supplied_bytes is not None and supplied_bytes != encoded:
        raise ValueError("canonical_json_v1 bytes are not in canonical form")
    return encoded


def domain_sha256(domain: str, value: Any) -> str:
    _validate_identifier(domain, field_name="digest_domain")
    return hashlib.sha256(
        domain.encode("utf-8") + b"\x00" + canonical_json_bytes(value)
    ).hexdigest()


_ArtifactT = TypeVar("_ArtifactT", bound=BaseModel)
_VALIDATED_CAPACITY = 8
_validated: OrderedDict[tuple[type[BaseModel], str], BaseModel] = OrderedDict()
_validated_lock = threading.Lock()


def validated_from_json(model: type[_ArtifactT], payload: str | bytes) -> _ArtifactT:
    """``model.model_validate_json(payload)``, validating each distinct payload once.

    The validators of a versioned artifact are a pure function of its bytes (a
    frozen result, no validation context), and the large ones (digest recomputation
    over a multi-megabyte payload, source-binding checks) cost 50-150 ms. A caller
    that revalidates an artifact it already holds, or a second caller that reads the
    same payload, gets the instance the first validation produced. Failures are
    never cached. The last ``_VALIDATED_CAPACITY`` payloads are kept.
    """

    raw = payload.encode("utf-8") if isinstance(payload, str) else bytes(payload)
    key = (model, hashlib.sha256(raw).hexdigest())
    with _validated_lock:
        hit = _validated.get(key)
        if hit is not None:
            _validated.move_to_end(key)
            return hit  # type: ignore[return-value]
    value = model.model_validate_json(payload)
    with _validated_lock:
        _validated[key] = value
        _validated.move_to_end(key)
        while len(_validated) > _VALIDATED_CAPACITY:
            _validated.popitem(last=False)
    return value


def revalidated(value: _ArtifactT) -> _ArtifactT:
    """``value`` validated again from its own JSON (``validated_from_json``): the
    detached, fully checked copy, without repeating a validation already made."""

    return validated_from_json(type(value), value.model_dump_json())


def json_sha256(value: BaseModel) -> str:
    """SHA-256 of ``value``'s ``model_dump_json()``: a content key for a model
    (a memo key, not an artifact ref digest)."""

    return hashlib.sha256(value.model_dump_json().encode("utf-8")).hexdigest()


class BundleManifestMember(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    member_key: str
    content_sha256: str

    @model_validator(mode="after")
    def _validate_member(self) -> "BundleManifestMember":
        _validate_identifier(self.member_key, field_name="member_key")
        validate_sha256_hex(self.content_sha256, field_name="content_sha256")
        return self


class BundleManifest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["bundle-manifest.v1"] = "bundle-manifest.v1"
    members: tuple[BundleManifestMember, ...]

    @model_validator(mode="after")
    def _validate_manifest(self) -> "BundleManifest":
        keys = tuple(member.member_key for member in self.members)
        if keys != tuple(sorted(set(keys))):
            raise ValueError("bundle manifest members must be sorted and unique")
        return self


@dataclass(frozen=True)
class BundleArtifactPayload:
    manifest: BundleManifest
    member_bytes: Mapping[str, bytes]


def compute_artifact_content_sha256(payload: Any, digest_codec: DigestCodec) -> str:
    """Recompute a ref content digest under its selected exact codec."""

    if digest_codec == "canonical_json_v1":
        encoded = canonical_json_bytes(payload)
    elif digest_codec == "raw_bytes_v1":
        if not isinstance(payload, (bytes, bytearray, memoryview)):
            raise ValueError("raw_bytes_v1 requires exact bytes")
        encoded = bytes(payload)
    elif digest_codec == "bundle_manifest_v1":
        if not isinstance(payload, BundleArtifactPayload):
            raise ValueError("bundle_manifest_v1 requires BundleArtifactPayload")
        expected_keys = tuple(member.member_key for member in payload.manifest.members)
        if set(payload.member_bytes) != set(expected_keys):
            raise ValueError("bundle member bytes must exactly enumerate the manifest")
        for member in payload.manifest.members:
            raw = payload.member_bytes[member.member_key]
            if not isinstance(raw, (bytes, bytearray, memoryview)):
                raise ValueError("bundle members must be exact bytes")
            actual = hashlib.sha256(bytes(raw)).hexdigest()
            if actual != member.content_sha256:
                raise ValueError(f"bundle member digest mismatch: {member.member_key}")
        encoded = canonical_json_bytes(payload.manifest)
    else:  # pragma: no cover - guarded by the closed Literal type
        raise ValueError("unsupported artifact digest codec")
    return hashlib.sha256(encoded).hexdigest()


__all__ = [
    "ArtifactScopeKind",
    "ArtifactUriFamily",
    "BundleArtifactPayload",
    "BundleManifest",
    "BundleManifestMember",
    "CanonicalMaterializationArtifactScope",
    "CanonicalModelArtifactScope",
    "CanonicalResearchFileArtifactScope",
    "CanonicalUserArtifactScope",
    "DigestCodec",
    "GlobalArtifactScope",
    "ParsedVersionedArtifactUri",
    "StrictArtifactScope",
    "VersionedArtifactRef",
    "VersionedArtifactScope",
    "WorkspacePreviewMaterializationArtifactScope",
    "WorkspacePreviewModelArtifactScope",
    "WorkspacePreviewResearchFileArtifactScope",
    "canonical_json_bytes",
    "compute_artifact_content_sha256",
    "domain_sha256",
    "format_versioned_artifact_uri",
    "json_sha256",
    "parse_versioned_artifact_uri",
    "revalidated",
    "validated_from_json",
]
