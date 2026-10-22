"""Environment-backed credential helpers used by the standalone schema package."""

from __future__ import annotations

import logging
import os


_EQUIVALENCE_FLAG_VALUES = {"false", "true"}


def get_equivalence_flag() -> str:
    raw = os.getenv("EDGAR_USE_UPSTREAM_EQUIVALENCE", "true").strip().lower()
    if raw not in _EQUIVALENCE_FLAG_VALUES:
        logging.warning(
            "Unknown EDGAR_USE_UPSTREAM_EQUIVALENCE=%r; defaulting to 'true'",
            raw,
        )
        return "true"
    return raw
