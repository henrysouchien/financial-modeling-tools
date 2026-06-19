from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, DefaultDict, Dict, List, Optional


@dataclass(frozen=True)
class CellWrite:
    sheet: str
    cell: str
    value: Any


@dataclass(frozen=True)
class CellFormat:
    sheet: str
    range: str
    bold: Optional[bool] = None
    font_color: Optional[str] = None
    number_format: Optional[str] = None
    underline: Optional[bool] = None
    fill_color: Optional[str] = None
    top_border_color: Optional[str] = None
    bottom_border_color: Optional[str] = None


@dataclass(frozen=True)
class SheetSetup:
    sheet: str
    column_widths: Dict[str, float]
    freeze_panes: Optional[str] = None
    first_data_column: Optional[str] = None


@dataclass
class RenderPlan:
    sheet_setups: List[SheetSetup] = field(default_factory=list)
    writes: List[CellWrite] = field(default_factory=list)
    formats: List[CellFormat] = field(default_factory=list)

    def writes_by_sheet(self) -> Dict[str, List[CellWrite]]:
        grouped: DefaultDict[str, List[CellWrite]] = defaultdict(list)
        for write in self.writes:
            grouped[write.sheet].append(write)
        return dict(grouped)

    def formats_by_sheet(self) -> Dict[str, List[CellFormat]]:
        grouped: DefaultDict[str, List[CellFormat]] = defaultdict(list)
        for cell_format in self.formats:
            grouped[cell_format.sheet].append(cell_format)
        return dict(grouped)


__all__ = [
    "CellFormat",
    "CellWrite",
    "RenderPlan",
    "SheetSetup",
]
