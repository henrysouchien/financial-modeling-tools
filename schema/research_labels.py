from __future__ import annotations

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


__all__ = [
    "canonicalize_optional_research_label",
    "canonicalize_research_label",
]
