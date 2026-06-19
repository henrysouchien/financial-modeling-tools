"""Shared helpers for schema reference values."""

from __future__ import annotations

from typing import Any, Optional

from .models import LineItemRef


_REF_KEYS = frozenset({"id", "t", "resolved", "period_anchor"})


def line_item_ref_from_obj(obj: Any) -> Optional[LineItemRef]:
    """Coerce a JSON-deserialized ref shape back to LineItemRef."""
    if isinstance(obj, LineItemRef):
        return obj
    if isinstance(obj, dict) and "id" in obj and obj.keys() <= _REF_KEYS:
        try:
            return LineItemRef(
                id=str(obj["id"]),
                t=int(obj.get("t", 0)),
                resolved=bool(obj.get("resolved", True)),
                period_anchor=str(obj.get("period_anchor", "first")),
            )
        except Exception:
            return None
    return None
