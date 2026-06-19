from __future__ import annotations

import re
import sys
from typing import Any, Optional, Tuple


_PARENT_MODULE = "schema.reader"


def _compat(name: str, fallback: Any) -> Any:
    parent = sys.modules.get(_PARENT_MODULE)
    if parent is not None and hasattr(parent, name):
        return getattr(parent, name)
    return fallback


def _get_cell_value(cells: dict[tuple[int, int], Any], row: int, col: int) -> Optional[str]:
    """Return the raw cell value if present."""
    cell = cells.get((row, col))
    if cell is None:
        return None
    return cell.value


def _normalize_year_token(token: str) -> Optional[int]:
    if not token:
        return None
    if len(token) == 2:
        year_suffix = int(token)
        year = 2000 + year_suffix if year_suffix < 80 else 1900 + year_suffix
    elif len(token) == 3:
        trimmed = token.lstrip("0") or "0"
        if len(trimmed) <= 2:
            year_suffix = int(trimmed)
            year = 2000 + year_suffix if year_suffix < 80 else 1900 + year_suffix
        else:
            year = int(trimmed)
    else:
        year = int(token)
    if 1900 <= year <= 2100:
        return year
    return None


def _parse_period_token(value: Optional[str]) -> Optional[Tuple[int, Optional[int], bool]]:
    """Parse a header token into (year, slot-or-none, is_quarterly-token)."""
    if value is None:
        return None
    text = str(value).strip()
    m = re.match(r"^([1-4])Q(\d{2,4})[AEae]?$", text)
    if m:
        slot = int(m.group(1))
        year_str = m.group(2)
        year = _compat("_normalize_year_token", _normalize_year_token)(year_str)
        if year is not None:
            return year, slot, True
    try:
        if text and text[-1] in "AEae":
            text = text[:-1]
        # For non-quarterly tokens, only accept explicit 4-digit years.
        # 2-digit normalization (e.g. 90->1990, 57->2057) is too aggressive
        # and misidentifies numeric data as years.
        annual = re.match(r"^(\d{4})(?:\.0+)?$", text)
        if annual is None:
            return None
        num = int(annual.group(1))
        if 1900 <= num <= 2100:
            return num, None, False
    except ValueError:
        return None
    return None


def _coerce_number(value: Optional[str]) -> Optional[float]:
    """Convert a string to float when possible."""
    if value is None:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _split_cell(cell_ref: str) -> Tuple[str, int]:
    col = ""
    row = ""
    for ch in cell_ref:
        if ch.isalpha():
            col += ch
        else:
            row += ch
    return col, int(row)


def _col_to_index(col: str) -> int:
    col = col.upper()
    index = 0
    for ch in col:
        if not ch.isalpha():
            continue
        index = index * 26 + (ord(ch) - ord("A") + 1)
    return index


def _index_to_col(idx: int) -> str:
    letters = []
    while idx > 0:
        idx, rem = divmod(idx - 1, 26)
        letters.append(chr(rem + ord("A")))
    return "".join(reversed(letters))


def _slugify(text: str) -> str:
    text = text.strip().lower()
    out = []
    prev_underscore = False
    for ch in text:
        if ch.isalnum():
            out.append(ch)
            prev_underscore = False
        else:
            if not prev_underscore:
                out.append("_")
                prev_underscore = True
    slug = "".join(out).strip("_")
    return slug or "line_item"


__all__ = [
    "_coerce_number",
    "_col_to_index",
    "_get_cell_value",
    "_index_to_col",
    "_normalize_year_token",
    "_parse_period_token",
    "_slugify",
    "_split_cell",
]
