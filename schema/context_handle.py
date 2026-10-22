"""Short model-facing handles for context bindings.

Wire contract: ``cxh1.<fam>.<32 lowercase hex>`` where the hex is the first 32
characters of the binding's ``context_content_sha256``. Server-owned identity
(canonical base64 refs, scope, lineage) never crosses the model; a handle
resolves only in the authenticated (user, research_file) scope at the owning
service, which re-derives the current binding and compares digests.
"""

from __future__ import annotations

import re

CONTEXT_HANDLE_PREFIX = "cxh1"
CONTEXT_HANDLE_FAMILIES = frozenset({"hf", "fa", "sc", "vp", "pv"})
_DIGEST_LEN = 32
_SHA256_RE = re.compile(r"[0-9a-f]{64}")
_DIGEST_RE = re.compile(r"[0-9a-f]{32}")


def context_handle(family: str, context_content_sha256: str) -> str:
    if family not in CONTEXT_HANDLE_FAMILIES:
        raise ValueError(f"unknown context handle family: {family!r}")
    if type(context_content_sha256) is not str or not _SHA256_RE.fullmatch(
        context_content_sha256
    ):
        raise ValueError("context_content_sha256 must be 64 lowercase hex characters")
    return f"{CONTEXT_HANDLE_PREFIX}.{family}.{context_content_sha256[:_DIGEST_LEN]}"


def _parts(value: object) -> tuple[str, str] | None:
    if type(value) is not str:
        return None
    parts = value.split(".")
    if len(parts) != 3 or parts[0] != CONTEXT_HANDLE_PREFIX:
        return None
    family, digest = parts[1], parts[2]
    if family not in CONTEXT_HANDLE_FAMILIES or not _DIGEST_RE.fullmatch(digest):
        return None
    return family, digest


def context_handle_digest(value: object, family: str) -> str | None:
    parts = _parts(value)
    if parts is None or parts[0] != family:
        return None
    return parts[1]


def is_context_handle(value: object) -> bool:
    return _parts(value) is not None


__all__ = [
    "CONTEXT_HANDLE_FAMILIES",
    "CONTEXT_HANDLE_PREFIX",
    "context_handle",
    "context_handle_digest",
    "is_context_handle",
]
