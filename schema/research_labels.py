from __future__ import annotations

import hashlib
import re
from urllib.parse import unquote


_PERCENT_OCTET_RE = re.compile(r"%[0-9A-Fa-f]{2}")


def canonicalize_research_label(value: object | None) -> str:
    """Normalize research label identity without broad URL-form decoding."""

    normalized = str(value or "").strip()
    if not normalized:
        return ""
    if _PERCENT_OCTET_RE.search(normalized):
        normalized = unquote(normalized).strip()
    return normalized


def canonicalize_optional_research_label(value: object | None) -> str | None:
    normalized = canonicalize_research_label(value)
    return normalized or None


def slugify_research_label(value: object | None, *, max_length: int | None = None) -> str:
    """Return the shared filesystem-safe slug for a canonical research label."""

    canonical = canonicalize_research_label(value)
    slug = re.sub(r"[^a-zA-Z0-9]+", "_", canonical.lower()).strip("_") or "label"
    return slug if max_length is None else slug[:max_length].rstrip("_") or "label"


def research_label_markdown_filename(
    ticker: str,
    label: object | None = None,
    *,
    digest_scheme: bool = False,
) -> str:
    """Derive a thesis filename under an explicitly selected path scheme."""

    canonical = canonicalize_research_label(label)
    if not canonical:
        return f"{ticker}.md"
    slug = slugify_research_label(canonical, max_length=40 if digest_scheme else None)
    if not digest_scheme:
        return f"{ticker}__{slug}.md"
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return f"{ticker}__{slug}_{digest}.md"


def format_label_for_display(
    label: object | None,
    *,
    label_user: object | None = None,
    label_direction: object | None = None,
    label_identity_ns: object | None = None,
    label_codec_version: object | None = None,
) -> str:
    """Render a persisted label using first-class axes when available."""

    stored = str(label or "")
    if str(label_codec_version or "") == "v1" and label_user is not None:
        user = str(label_user)
        direction = str(label_direction or "long")
        identity_ns = str(label_identity_ns or "base")
    else:
        user = stored
        direction = "long"
        identity_ns = "base"
        remaining = stored
        identity_marker = remaining.rfind("~i:")
        if identity_marker >= 0 and remaining[identity_marker + 3 :] == "nsec":
            identity_ns = "nsec"
            remaining = remaining[:identity_marker]
        direction_marker = remaining.rfind("~d:")
        if direction_marker >= 0 and remaining[direction_marker + 3 :] in {
            "short",
            "hedge",
            "pair",
        }:
            direction = remaining[direction_marker + 3 :]
            remaining = remaining[:direction_marker]
        if "~d:" not in remaining and "~i:" not in remaining:
            user = remaining.replace("%7E", "~").replace("%25", "%")
    rendered = user
    if direction != "long":
        rendered += f" ({direction})"
    if identity_ns != "base":
        rendered += f" [{identity_ns}]"
    return rendered


__all__ = [
    "canonicalize_optional_research_label",
    "canonicalize_research_label",
    "format_label_for_display",
    "research_label_markdown_filename",
    "slugify_research_label",
]
