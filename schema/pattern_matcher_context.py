from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional


@dataclass
class CellContext:
    """Context needed to interpret cell references semantically.

    sheet_row_to_item maps row numbers to line_item_ids per sheet.
    sheet_col_to_period maps column indices to period keys per sheet.
    time_order is the ordered list of period keys used to derive t offsets.
    """

    sheet: str
    row: int
    col: int
    sheet_row_to_item: Dict[str, Dict[int, str]]
    time_order: List[int]
    sheet_col_to_period: Optional[Dict[str, Dict[int, int]]] = None
    sheet_col_to_year: Optional[Dict[str, Dict[int, int]]] = None

    def __post_init__(self) -> None:
        if self.sheet_col_to_period is None and self.sheet_col_to_year is not None:
            self.sheet_col_to_period = self.sheet_col_to_year
        if self.sheet_col_to_period is None:
            self.sheet_col_to_period = {}
