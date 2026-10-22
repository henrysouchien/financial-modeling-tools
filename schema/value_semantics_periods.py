"""Canonical serialized period-label grammar for value-semantics/v1."""

from __future__ import annotations

import re


CANONICAL_PERIOD_LABEL_RE = re.compile(
    r"^(?:FY[0-9]{4}|[0-9]{4}-Q[1-4]|[0-9]{4}-[0-9]{2}-[0-9]{2})$"
)


def validate_canonical_period_label(
    value: str | None,
    *,
    field_name: str = "period",
) -> str | None:
    if value is not None and not CANONICAL_PERIOD_LABEL_RE.fullmatch(value):
        raise ValueError(
            f"{field_name} must use canonical FYyyyy, yyyy-Qn, or yyyy-mm-dd grammar"
        )
    return value


__all__ = ["CANONICAL_PERIOD_LABEL_RE", "validate_canonical_period_label"]
