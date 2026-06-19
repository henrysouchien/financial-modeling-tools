from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import re
from typing import Any

import yaml


_ROOT = Path(__file__).resolve().parents[1]
_DEFAULT_TAXONOMY_PATH = _ROOT / "config" / "industry_taxonomy.yaml"
_UNKNOWN = "unknown"


def resolve_industry_key(fmp_profile: dict[str, Any]) -> str:
    """Return the configured editorial industry key for an FMP profile."""

    profile = dict(fmp_profile or {})
    industry = _normalize_industry(profile.get("industry"))
    if not industry:
        return _UNKNOWN

    searchable_text = " ".join(
        str(profile.get(field) or "")
        for field in (
            "companyName",
            "company_name",
            "description",
            "sector",
            "industry",
        )
    )
    taxonomy = _load_taxonomy(_DEFAULT_TAXONOMY_PATH)
    for industry_key, config in taxonomy.get("reference_industries", {}).items():
        configured = {
            _normalize_industry(value)
            for value in config.get("fmp_industries", [])
            if _normalize_industry(value)
        }
        if industry not in configured:
            continue
        pattern = str(config.get("fmp_industry_filter") or "").strip()
        if pattern and re.search(pattern, searchable_text, flags=re.IGNORECASE) is None:
            continue
        return str(industry_key)
    return _UNKNOWN


@lru_cache(maxsize=1)
def _load_taxonomy(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError("industry taxonomy must be a mapping")
    return payload


def _normalize_industry(value: object | None) -> str:
    normalized = str(value or "").strip().lower()
    if not normalized:
        return ""
    normalized = (
        normalized.replace("\u2013", "-")
        .replace("\u2014", "-")
        .replace("\u2015", "-")
    )
    normalized = re.sub(r"\s+", " ", normalized)
    normalized = re.sub(r"\s*-\s*", " - ", normalized)
    return normalized


__all__ = ["resolve_industry_key"]
