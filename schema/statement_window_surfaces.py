"""Stable tool/report serialization for statement-window build results.

A build with compiled windows exposes the validated ``statement_windows``
result. A build with no compiled windows exposes no window block, represented
here as ``{}``.
"""

from __future__ import annotations

from dataclasses import asdict, is_dataclass
import json
from typing import Any



WINDOW_KEY = "statement_windows"


def _serialize(value: Any) -> dict[str, Any]:
    if hasattr(value, "to_dict"):
        payload = value.to_dict()
    elif hasattr(value, "model_dump"):
        payload = value.model_dump(mode="json")
    elif is_dataclass(value) and not isinstance(value, type):
        payload = asdict(value)
    elif isinstance(value, dict):
        payload = dict(value)
    else:
        raise TypeError(
            "statement-window surface values must be mappings or typed result objects"
        )
    if not isinstance(payload, dict):
        raise TypeError("statement-window surface serialization must produce an object")
    return json.loads(json.dumps(payload, sort_keys=True, allow_nan=False))


def _compiled_entries(payload: Any) -> list[Any] | None:
    if payload is None:
        return None
    if hasattr(payload, "to_dict"):
        payload = payload.to_dict()
    if not isinstance(payload, dict):
        return None
    compiled = payload.get("compiled")
    return compiled if isinstance(compiled, list) else None


def statement_window_build_surfaces(build_result: Any) -> dict[str, dict[str, Any]]:
    """Return the window key for a build that compiled windows."""

    return normalize_statement_window_surfaces(
        getattr(build_result, "statement_windows", None),
    )


def normalize_statement_window_surfaces(
    statement_windows: Any = None,
) -> dict[str, dict[str, Any]]:
    """Normalize a window surface, or return ``{}`` when no window compiled."""

    compiled = _compiled_entries(statement_windows)
    if not compiled:
        return {}
    return {WINDOW_KEY: _serialize(statement_windows)}




__all__ = [
    "WINDOW_KEY",
    "normalize_statement_window_surfaces",
    "statement_window_build_surfaces",
]
