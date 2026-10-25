"""Path-free model-state identity contracts for the annual-build foundation.

This module deliberately contains no filesystem resolver.  It validates and
formats logical identities only; the BMA-101 provider cutover owns physical
resolution.
"""

from __future__ import annotations

import hashlib
import json
import re
from enum import Enum
from typing import Any
from urllib.parse import quote, unquote_to_bytes, urlsplit

from agent_workflow_contracts.research_file_contract import is_research_file_id
from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from .model_ref import SqliteInt64Id
from .ticker_contract import require_canonical_contract_ticker


_SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
_POSITIVE_DECIMAL_RE = re.compile(r"^[1-9][0-9]*$")
_PERCENT_ESCAPE_RE = re.compile(r"%[0-9A-Fa-f]{2}")


def validate_sha256_hex(
    value: object, *, field_name: str | None = "digest"
) -> str:
    """Return a canonical lower-case SHA-256 hex digest or raise."""

    if not isinstance(value, str) or _SHA256_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be exactly 64 lowercase hex characters")
    return value


def validate_normalized_ticker(value: str) -> str:
    """Validate the canonical ticker spelling used by logical identities."""

    return require_canonical_contract_ticker(value, field_name="ticker")


def encode_uri_segment(value: str, *, field_name: str = "identifier") -> str:
    """Encode one canonical UTF-8 RFC 3986 identifier segment."""

    _validate_decoded_segment(value, field_name=field_name)
    return quote(value, safe="-._~", encoding="utf-8", errors="strict")


def decode_canonical_uri_segment(value: str, *, field_name: str = "identifier") -> str:
    """Decode exactly one canonical URI segment and reject aliases/encoding drift."""

    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} URI segment must be non-empty")
    for match in re.finditer("%", value):
        escape = value[match.start() : match.start() + 3]
        if re.fullmatch(r"%[0-9A-F]{2}", escape) is None:
            raise ValueError(f"{field_name} has a non-canonical percent escape")
    try:
        decoded = unquote_to_bytes(value).decode("utf-8", errors="strict")
    except (UnicodeDecodeError, ValueError) as exc:
        raise ValueError(f"{field_name} is not canonical UTF-8") from exc
    _validate_decoded_segment(decoded, field_name=field_name)
    if encode_uri_segment(decoded, field_name=field_name) != value:
        raise ValueError(f"{field_name} URI segment is not canonically encoded")
    return decoded


def parse_positive_decimal(value: str, *, field_name: str) -> int:
    """Parse a positive canonical decimal (no sign or leading zero)."""

    if not isinstance(value, str) or _POSITIVE_DECIMAL_RE.fullmatch(value) is None:
        raise ValueError(f"{field_name} must be a positive canonical decimal")
    parsed = int(value)
    if not is_research_file_id(parsed):
        raise ValueError(f"{field_name} must be a positive canonical integer")
    return parsed


def _validate_decoded_segment(value: str, *, field_name: str) -> None:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{field_name} must be non-empty")
    if value in {".", ".."} or value.casefold() in {"latest", "alias"}:
        raise ValueError(f"{field_name} uses a forbidden alias or dot segment")
    if "/" in value or "\\" in value or "\x00" in value:
        raise ValueError(f"{field_name} must be exactly one path-free URI segment")
    # A decoded escape-looking token could be decoded again by a lax consumer.
    # Reject it at the source so every formatter and typed identity shares the
    # parser's exactly-once grammar.
    if _PERCENT_ESCAPE_RE.search(value):
        raise ValueError(f"{field_name} contains an escape-looking token")


class ModelStateScope(str, Enum):
    CANONICAL = "canonical"
    WORKSPACE_PREVIEW = "workspace_preview"


class CurrentModelIdentity(BaseModel):
    """Path-free identity of the authenticated current ModelVersion selection."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
    )

    research_file_id: SqliteInt64Id
    ticker: str
    model_id: str = Field(min_length=1)
    version: SqliteInt64Id
    pointer_epoch: SqliteInt64Id
    workbook_xlsx_sha256: str
    model_spec_sha256: str

    @field_validator("ticker")
    @classmethod
    def _validate_ticker(cls, value: str) -> str:
        return validate_normalized_ticker(value)

    @field_validator("workbook_xlsx_sha256", "model_spec_sha256")
    @classmethod
    def _validate_digest(cls, value: str, info: Any) -> str:
        return validate_sha256_hex(value, field_name=info.field_name)


class ModelStateWriteContext(BaseModel):
    """Authenticated logical namespace supplied to every model-state write."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    user_id: str
    research_file_id: SqliteInt64Id
    ticker: str
    scope: ModelStateScope
    workspace_id: str | None = None
    canonical_base_hash: str | None = None
    annual_eligible: bool = True
    resolver_version: str

    @model_validator(mode="after")
    def _validate_context(self) -> "ModelStateWriteContext":
        encode_uri_segment(self.user_id, field_name="user_id")
        validate_normalized_ticker(self.ticker)
        encode_uri_segment(self.resolver_version, field_name="resolver_version")
        if self.scope is ModelStateScope.CANONICAL:
            if self.workspace_id is not None or self.canonical_base_hash is not None:
                raise ValueError("canonical context forbids workspace/base fields")
        else:
            if self.workspace_id is None or self.canonical_base_hash is None:
                raise ValueError(
                    "workspace_preview context requires workspace/base fields"
                )
            encode_uri_segment(self.workspace_id, field_name="workspace_id")
            validate_sha256_hex(
                self.canonical_base_hash, field_name="canonical_base_hash"
            )
        return self


class ModelStateRef(BaseModel):
    """An immutable, revision-addressed logical model-state identity."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    uri: str
    scope: ModelStateScope
    user_id: str
    research_file_id: SqliteInt64Id
    ticker: str
    revision: SqliteInt64Id
    content_hash: str
    workspace_id: str | None = None
    canonical_base_hash: str | None = None

    @model_validator(mode="after")
    def _validate_identity(self) -> "ModelStateRef":
        validate_sha256_hex(self.content_hash, field_name="content_hash")
        validate_normalized_ticker(self.ticker)
        encode_uri_segment(self.user_id, field_name="user_id")
        if self.scope is ModelStateScope.CANONICAL:
            if self.workspace_id is not None or self.canonical_base_hash is not None:
                raise ValueError("canonical ref forbids workspace/base fields")
        else:
            if self.workspace_id is None or self.canonical_base_hash is None:
                raise ValueError("workspace_preview ref requires workspace/base fields")
            encode_uri_segment(self.workspace_id, field_name="workspace_id")
            validate_sha256_hex(
                self.canonical_base_hash, field_name="canonical_base_hash"
            )
        parsed = parse_model_state_uri(self.uri)
        expected = (
            self.scope,
            self.user_id,
            self.research_file_id,
            self.ticker,
            self.revision,
            self.workspace_id,
            self.canonical_base_hash,
        )
        if parsed != expected:
            raise ValueError("model-state URI identity does not match embedded fields")
        if (
            format_model_state_uri(
                scope=self.scope,
                user_id=self.user_id,
                research_file_id=self.research_file_id,
                ticker=self.ticker,
                revision=self.revision,
                workspace_id=self.workspace_id,
                canonical_base_hash=self.canonical_base_hash,
            )
            != self.uri
        ):
            raise ValueError("model-state URI is not canonical")
        return self


def format_model_state_uri(
    *,
    scope: ModelStateScope,
    user_id: str,
    research_file_id: int,
    ticker: str,
    revision: int,
    workspace_id: str | None = None,
    canonical_base_hash: str | None = None,
) -> str:
    """Format one of the two complete path-free model-state URI forms."""

    try:
        scope = ModelStateScope(scope)
    except (TypeError, ValueError) as exc:
        raise ValueError("unsupported model-state scope") from exc
    if not is_research_file_id(research_file_id):
        raise ValueError("research_file_id must be positive")
    if not is_research_file_id(revision):
        raise ValueError("revision must be positive")
    user = encode_uri_segment(user_id, field_name="user_id")
    ticker_segment = encode_uri_segment(
        validate_normalized_ticker(ticker), field_name="ticker"
    )
    if scope is ModelStateScope.CANONICAL:
        if workspace_id is not None or canonical_base_hash is not None:
            raise ValueError("canonical URI forbids workspace/base fields")
        return (
            f"model-state://canonical/users/{user}/research-files/"
            f"{research_file_id}/tickers/{ticker_segment}/revisions/{revision}"
        )
    if workspace_id is None or canonical_base_hash is None:
        raise ValueError("workspace-preview URI requires workspace/base fields")
    workspace = encode_uri_segment(workspace_id, field_name="workspace_id")
    base = validate_sha256_hex(canonical_base_hash, field_name="canonical_base_hash")
    return (
        f"model-state://workspace-preview/users/{user}/research-files/"
        f"{research_file_id}/tickers/{ticker_segment}/workspaces/{workspace}/"
        f"bases/{base}/revisions/{revision}"
    )


def parse_model_state_uri(
    uri: str,
) -> tuple[ModelStateScope, str, int, str, int, str | None, str | None]:
    """Strictly parse a canonical model-state URI into its embedded identity."""

    if not isinstance(uri, str) or not uri:
        raise ValueError("model-state URI must be non-empty")
    parsed = urlsplit(uri)
    if parsed.scheme != "model-state" or parsed.query or parsed.fragment:
        raise ValueError("model-state URI has an invalid scheme/query/fragment")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("model-state URI forbids userinfo")
    try:
        if parsed.port is not None:
            raise ValueError("model-state URI forbids a port")
    except ValueError as exc:
        raise ValueError("model-state URI has an invalid authority") from exc
    authority = parsed.netloc
    parts = parsed.path.split("/")[1:]
    if not parts or any(part == "" for part in parts):
        raise ValueError("model-state URI has empty path segments")

    if authority == "canonical":
        if len(parts) != 8 or parts[0::2] != [
            "users",
            "research-files",
            "tickers",
            "revisions",
        ]:
            raise ValueError("invalid canonical model-state URI shape")
        user = decode_canonical_uri_segment(parts[1], field_name="user_id")
        research_file_id = parse_positive_decimal(
            parts[3], field_name="research_file_id"
        )
        ticker = validate_normalized_ticker(
            decode_canonical_uri_segment(parts[5], field_name="ticker")
        )
        revision = parse_positive_decimal(parts[7], field_name="revision")
        result = (
            ModelStateScope.CANONICAL,
            user,
            research_file_id,
            ticker,
            revision,
            None,
            None,
        )
    elif authority == "workspace-preview":
        if len(parts) != 12 or parts[0::2] != [
            "users",
            "research-files",
            "tickers",
            "workspaces",
            "bases",
            "revisions",
        ]:
            raise ValueError("invalid workspace-preview model-state URI shape")
        user = decode_canonical_uri_segment(parts[1], field_name="user_id")
        research_file_id = parse_positive_decimal(
            parts[3], field_name="research_file_id"
        )
        ticker = validate_normalized_ticker(
            decode_canonical_uri_segment(parts[5], field_name="ticker")
        )
        workspace = decode_canonical_uri_segment(parts[7], field_name="workspace_id")
        base = validate_sha256_hex(parts[9], field_name="canonical_base_hash")
        revision = parse_positive_decimal(parts[11], field_name="revision")
        result = (
            ModelStateScope.WORKSPACE_PREVIEW,
            user,
            research_file_id,
            ticker,
            revision,
            workspace,
            base,
        )
    else:
        raise ValueError("model-state URI authority is not registered")

    if (
        format_model_state_uri(
            scope=result[0],
            user_id=result[1],
            research_file_id=result[2],
            ticker=result[3],
            revision=result[4],
            workspace_id=result[5],
            canonical_base_hash=result[6],
        )
        != uri
    ):
        raise ValueError("model-state URI does not round-trip canonically")
    return result


def validate_model_state_ref_context(
    ref: ModelStateRef, context: ModelStateWriteContext
) -> ModelStateRef:
    """Fail closed unless a ref belongs to the exact supplied write context."""

    expected = (
        context.scope,
        context.user_id,
        context.research_file_id,
        context.ticker,
        context.workspace_id,
        context.canonical_base_hash,
    )
    actual = (
        ref.scope,
        ref.user_id,
        ref.research_file_id,
        ref.ticker,
        ref.workspace_id,
        ref.canonical_base_hash,
    )
    if actual != expected:
        raise ValueError("model-state ref does not match the exact write context")
    return ref


def model_state_ref_digest(ref: ModelStateRef) -> str:
    """Canonical domain-separated identity hash (never a physical-path hash)."""

    payload: Any = ref.model_dump(mode="json")
    encoded = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(b"model-state-ref.v1\x00" + encoded).hexdigest()


__all__ = [
    "CurrentModelIdentity",
    "ModelStateRef",
    "ModelStateScope",
    "ModelStateWriteContext",
    "decode_canonical_uri_segment",
    "encode_uri_segment",
    "format_model_state_uri",
    "model_state_ref_digest",
    "parse_model_state_uri",
    "parse_positive_decimal",
    "validate_model_state_ref_context",
    "validate_normalized_ticker",
    "validate_sha256_hex",
]
