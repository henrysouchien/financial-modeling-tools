"""Pure helper functions for dependency graph evaluation."""

from __future__ import annotations

from typing import Optional

from .models import ValueProvenance


def _is_input_provenance(provenance: ValueProvenance) -> bool:
    return provenance in (
        ValueProvenance.input,
        ValueProvenance.imported_other,
        ValueProvenance.imported_edgar,
        ValueProvenance.imported_fmp,
    )


def _numeric_label_value(label: str) -> Optional[float]:
    text = str(label or "").strip()
    if not text:
        return None
    is_percent = text.endswith("%")
    if is_percent:
        text = text[:-1].strip()
    text = text.replace(",", "").replace("$", "")
    if text.startswith("(") and text.endswith(")"):
        text = f"-{text[1:-1]}"
    try:
        value = float(text)
    except ValueError:
        return None
    return value / 100.0 if is_percent else value


def _col_to_index(col: str) -> int:
    index = 0
    for ch in col.upper():
        if not ch.isalpha():
            continue
        index = index * 26 + (ord(ch) - ord("A") + 1)
    return index


def _index_to_col(index: int) -> str:
    if index < 1:
        raise ValueError(f"Invalid column index: {index!r}")
    chars: list[str] = []
    while index:
        index, remainder = divmod(index - 1, 26)
        chars.append(chr(ord("A") + remainder))
    return "".join(reversed(chars))


def _offset_column(col: str, offset: int) -> str:
    return _index_to_col(_col_to_index(col) + int(offset))


__all__ = [
    "_col_to_index",
    "_index_to_col",
    "_is_input_provenance",
    "_numeric_label_value",
    "_offset_column",
]
