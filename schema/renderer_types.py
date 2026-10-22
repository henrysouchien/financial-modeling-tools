from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, DefaultDict, Dict, List, Literal, Optional


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


RenderCellKind = Literal[
    "hardcode",
    "formula",
    "unavailable",
    "presentation",
]


@dataclass(frozen=True)
class SemanticCellRender:
    """Compact, derived contract for one rendered semantic cell."""

    sheet: str
    cell: str
    semantic_item_id: str
    semantic_label: str
    period: int
    cell_kind: RenderCellKind
    expected_state: str
    expected_value: Any
    origin: str
    dependencies: tuple[str, ...]
    unit: str | None
    scale: str
    sign: str
    style: str | None
    tolerance: float
    number_format: str | None
    observation_ids: tuple[str, ...] = ()
    assumption_ids: tuple[str, ...] = ()
    source_refs: tuple[str, ...] = ()
    evidence_ids: tuple[str, ...] = ()
    compiler_origin: str | None = None


@dataclass(frozen=True)
class AnnualRenderContext:
    """Build-owned annual render inputs: version identity and #N/A cells.

    The verdict and its findings are not here: they are appended to the
    Summary after validation (``renderer_summary.append_summary_findings``).
    """

    version_label: str
    version_number: int | None = None
    predecessor_version_number: int | None = None
    required_unavailable: frozenset[tuple[str, int]] = frozenset()


@dataclass
class RenderPlan:
    sheet_setups: List[SheetSetup] = field(default_factory=list)
    writes: List[CellWrite] = field(default_factory=list)
    formats: List[CellFormat] = field(default_factory=list)
    semantic_cells: List[SemanticCellRender] = field(default_factory=list)

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
    "AnnualRenderContext",
    "RenderPlan",
    "RenderCellKind",
    "SemanticCellRender",
    "SheetSetup",
]
