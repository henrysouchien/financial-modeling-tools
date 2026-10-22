"""Canonical workbook-presentation caption identity and failure contract."""

from __future__ import annotations

from typing import Callable, Optional

from .models import FormulaSpec, FormulaType
from .refs import line_item_ref_from_obj


WORKBOOK_PRESENTATION_CAPTION_ITEM_IDS = frozenset(
    {
        "tpl.fm.header.millions_except_per_share_data",
        "tpl.a.header.units_header",
        "tpl.fm.balance_sheet.balance_sheet_m",
        "tpl.fm.header.units_header",
    }
)


def item_renders_text(
    item_id: str,
    active_spec: Callable[[str], Optional[FormulaSpec]],
) -> bool:
    """Whether ``item_id`` renders text rather than a number.

    True when the item is a presentation caption, its active spec is a string
    constant, or its active spec is a ``t=0`` reference without adjustment or
    negation whose chain ends in either. ``active_spec`` returns the spec an
    item renders with.
    """

    seen: set[str] = set()
    while item_id not in seen:
        if item_id in WORKBOOK_PRESENTATION_CAPTION_ITEM_IDS:
            return True
        seen.add(item_id)
        spec = active_spec(item_id)
        if spec is None:
            return False
        params = spec.params or {}
        if spec.type is FormulaType.constant:
            return isinstance(params.get("value"), str)
        if spec.type is FormulaType.raw:
            formula = str(params.get("formula") or params.get("semantic_formula") or "")
            return bool(formula) and "NA()" not in formula.upper()
        if spec.type is not FormulaType.ref:
            return False
        source = line_item_ref_from_obj(params.get("source"))
        if (
            source is None
            or int(source.t) != 0
            or params.get("adjustment") is not None
            or params.get("negate")
        ):
            return False
        item_id = source.id
    return False


class PresentationCaptionSpecError(ValueError):
    """A caption identity was mutated or carried a non-presentation spec."""

    diagnostic_code = "presentation_caption_spec_violation"


__all__ = [
    "PresentationCaptionSpecError",
    "WORKBOOK_PRESENTATION_CAPTION_ITEM_IDS",
    "item_renders_text",
]
