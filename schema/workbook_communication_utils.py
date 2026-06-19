from __future__ import annotations

import math
import re
import sys
from typing import Any, Mapping


def _compat(name: str, default: Any) -> Any:
    parent = sys.modules.get("schema.workbook_communication")
    if parent is None:
        parent = sys.modules.get("workbook_communication")
    return getattr(parent, name, default) if parent is not None else default


def _normalize_label(value: Any) -> str:
    if value is None:
        return ""
    return re.sub(r"[^a-z0-9]+", "", str(value).strip().lower())


def _is_formula(value: Any) -> bool:
    return isinstance(value, str) and value.startswith("=")


def _is_blankish(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, str):
        normalized = value.strip().lower()
        return normalized in {"", "n/a", "na", "not available", "#n/a", "#value!", "#div/0!"}
    if isinstance(value, float):
        return math.isnan(value)
    return False


def _values_equivalent(left: Any, right: Any) -> bool:
    number_or_none = _compat("_number_or_none", _number_or_none)
    left_number = number_or_none(left)
    right_number = number_or_none(right)
    if left_number is not None and right_number is not None:
        return math.isclose(left_number, right_number, rel_tol=1e-9, abs_tol=1e-9)
    return left == right


def _get_path(data: Mapping[str, Any], path: tuple[str, ...]) -> Any:
    current: Any = data
    for part in path:
        if not isinstance(current, Mapping):
            return None
        current = current.get(part)
    return current


def _first_mapping(*values: Any) -> Mapping[str, Any] | None:
    for value in values:
        if isinstance(value, Mapping):
            return value
    return None


def _first_number(*values: Any) -> float | None:
    number_or_none = _compat("_number_or_none", _number_or_none)
    for value in values:
        number = number_or_none(value)
        if number is not None:
            return number
    return None


def _number_or_none(value: Any) -> float | None:
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, float) and math.isnan(value):
            return None
        return float(value)
    if isinstance(value, str):
        cleaned = value.strip().replace(",", "")
        if cleaned.endswith("%"):
            number_or_none = _compat("_number_or_none", _number_or_none)
            percent_number = number_or_none(cleaned[:-1])
            return percent_number / 100.0 if percent_number is not None else None
        if cleaned.startswith("$"):
            cleaned = cleaned[1:]
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def _number_from_source(source: Any | None) -> float | None:
    if source is None:
        return None
    number_or_none = _compat("_number_or_none", _number_or_none)
    return number_or_none(source.value)


def _artifact_ref(payload: Mapping[str, Any]) -> str | None:
    string_or_none = _compat("_string_or_none", _string_or_none)
    return string_or_none(payload.get("artifact_id")) or string_or_none(payload.get("id"))


def _string_or_none(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


__all__ = [
    "_artifact_ref",
    "_first_mapping",
    "_first_number",
    "_get_path",
    "_is_blankish",
    "_is_formula",
    "_normalize_label",
    "_number_from_source",
    "_number_or_none",
    "_string_or_none",
    "_values_equivalent",
]
