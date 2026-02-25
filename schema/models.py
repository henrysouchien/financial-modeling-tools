"""Schema data models for financial-model representation.

Defines the semantic objects used across the system: line items, formulas,
time structure, scenarios, and metadata. These models are serialized to JSON
and act as the source of truth for computation and rendering.

Example:
    Excel:   =H7-H9
    Schema:  FormulaSpec(type="arithmetic",
             params={"operands": ["-", LineItemRef("revenue"), LineItemRef("gross_profit")]})
"""

from __future__ import annotations

from enum import Enum
from typing import Dict, List, Optional, Tuple

from pydantic import AliasChoices, BaseModel, Field, PrivateAttr, field_validator


PERIOD_MODE_YEARLY = "yearly"
PERIOD_MODE_QUARTERLY5 = "quarterly5"


def encode_period(year: int, slot: int, mode: str) -> int:
    """Encode a period key for the configured time-axis mode."""
    if mode == PERIOD_MODE_YEARLY:
        return year
    if mode != PERIOD_MODE_QUARTERLY5:
        raise ValueError(f"Unknown period mode: {mode}")
    if slot < 1 or slot > 5:
        raise ValueError(f"Quarterly slot out of range: {slot}")
    return year * 10 + slot


def decode_period(period: int, mode: str) -> Tuple[int, int]:
    """Decode a period key into (year, slot)."""
    if mode == PERIOD_MODE_YEARLY:
        return period, 5
    if mode != PERIOD_MODE_QUARTERLY5:
        raise ValueError(f"Unknown period mode: {mode}")
    year = period // 10
    slot = period % 10
    if slot < 1 or slot > 5:
        raise ValueError(f"Invalid quarterly period key: {period}")
    return year, slot


def period_year(period: int, mode: str) -> int:
    return decode_period(period, mode)[0]


def period_slot_label(slot: int) -> str:
    labels = {1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4", 5: "A"}
    return labels.get(slot, f"S{slot}")


def shift_period(period: int, t: int, mode: str) -> Optional[int]:
    """Shift a period key by t periods."""
    if t == 0:
        return period
    if mode == PERIOD_MODE_YEARLY:
        return period + t
    if mode != PERIOD_MODE_QUARTERLY5:
        raise ValueError(f"Unknown period mode: {mode}")
    year, slot = decode_period(period, mode)
    index = year * 5 + (slot - 1)
    shifted = index + t
    if shifted < 0:
        return None
    shifted_year = shifted // 5
    shifted_slot = shifted % 5 + 1
    return encode_period(shifted_year, shifted_slot, mode)


class ItemType(str, Enum):
    """Classification of a line item row."""
    input = "input"
    derived = "derived"
    header = "header"
    spacer = "spacer"


class Unit(str, Enum):
    """Display/semantic unit for a line item."""
    dollars = "dollars"
    percentage = "percentage"
    ratio = "ratio"
    count = "count"
    per_share = "per_share"
    days = "days"
    multiple = "multiple"


class FormulaType(str, Enum):
    """Formula classification used by the schema."""
    ref = "ref"
    arithmetic = "arithmetic"
    driver = "driver"
    ratio = "ratio"
    growth = "growth"
    roll_forward = "roll_forward"
    valuation = "valuation"
    constant = "constant"
    raw = "raw"


class ValueProvenance(str, Enum):
    """Where a value came from (inputs, computed, or imported sources)."""
    input = "input"
    computed = "computed"
    imported_edgar = "imported_edgar"
    imported_fmp = "imported_fmp"
    imported_other = "imported_other"


class LineItemRef(BaseModel):
    """Reference to a semantic line item with a time offset.

    Offset is measured in model time-axis periods.
    t=0  -> same period
    t=-1 -> prior period
    t=+1 -> next period
    """
    model_config = {"frozen": True}
    id: str
    t: int = 0
    resolved: bool = True


class FormulaSpec(BaseModel):
    """How a line item is calculated.

    params shape depends on FormulaType. Common shapes:
    - ref: {"source": LineItemRef, "adjustment": float?, "negate": bool?}
    - arithmetic: {"operands": ["+", LineItemRef, LineItemRef, ...]} or {"function": "SUM"/"AVERAGE", "items": [...]}
    - driver: {"base": expr, "rate": expr, "scale": float?}
    - ratio: {"numerator": expr, "denominator": expr, "subtract_one": bool?}
    - growth: {"base": expr, "rate": expr}
    - roll_forward: {"beginning": expr, "additions": [...], "subtractions": [...]}
    - valuation: subtype-specific params (dcf_discount, terminal_value, capm, wacc, multiple, etc.)
    - constant: {"value": float}
    - raw: {"formula": "original Excel string"}
    """
    type: FormulaType
    subtype: Optional[str] = None
    params: Dict = Field(default_factory=dict)
    note: Optional[str] = None


class ValueCell(BaseModel):
    """Single value with provenance for a specific period key."""
    model_config = {"populate_by_name": True}
    period: int = Field(validation_alias=AliasChoices("period", "year"))
    value: Optional[float]
    provenance: ValueProvenance
    note: Optional[str] = None

    @property
    def year(self) -> int:
        """Backward-compatible alias during migration."""
        return self.period


class ValueSeries(BaseModel):
    """Time series of values for one line item."""
    values: Dict[int, ValueCell] = Field(default_factory=dict)
    last_updated: Optional[str] = None


class LineItem(BaseModel):
    """One row in the model (line item)."""
    id: str
    label: str
    row: int
    item_type: ItemType
    xbrl_tag: Optional[str] = None
    historical: Optional[FormulaSpec] = None
    projected: Optional[FormulaSpec] = None
    unit: Unit = Unit.dollars
    format: str = ""
    values: Optional[ValueSeries] = None
    overrides: Optional[Dict[int, FormulaSpec]] = None
    formula_periods: Optional[List[int]] = None


class Section(BaseModel):
    """Logical grouping of line items (e.g., income statement)."""
    id: str
    label: str
    line_items: List[LineItem] = Field(default_factory=list)


class Sheet(BaseModel):
    """One sheet in the model (assumptions, financial_model, valuation, scenarios)."""
    name: str
    sections: List[Section] = Field(default_factory=list)


class TimeStructure(BaseModel):
    """Defines the model's time axis and Excel column mapping."""
    fiscal_year_end: str
    period_mode: str = PERIOD_MODE_YEARLY
    historical_periods: List[int] = Field(default_factory=list)
    projection_periods: List[int] = Field(default_factory=list)
    period_column_map: Dict[int, str] = Field(default_factory=dict)
    historical_years: List[int] = Field(default_factory=list)
    projection_years: List[int] = Field(default_factory=list)
    column_map: Dict[int, str] = Field(default_factory=dict)
    # Example: {2024: "H", 2025: "I"} for rendering formulas back to Excel.


class ScenarioInputs(BaseModel):
    """Scenario assumption overrides."""
    name: str
    assumptions: Dict[str, float] = Field(default_factory=dict)
    description: str = ""


class CompanyInfo(BaseModel):
    """Basic company metadata."""
    ticker: str
    name: str
    fiscal_year_end: Optional[str] = None
    sector: Optional[str] = None
    industry: Optional[str] = None


class ModelMetadata(BaseModel):
    """Metadata about the model/template origin."""
    template_version: Optional[str] = None
    created_at: Optional[str] = None
    notes: Optional[str] = None


class FinancialModel(BaseModel):
    """Top-level model container for one company."""
    company: CompanyInfo
    time_structure: TimeStructure
    sheets: Dict[str, Sheet] = Field(default_factory=dict)
    scenarios: Dict[str, ScenarioInputs] = Field(default_factory=dict)
    metadata: ModelMetadata = Field(default_factory=ModelMetadata)

    _index: Dict[str, LineItem] = PrivateAttr(default_factory=dict)

    def build_index(self) -> None:
        """Build fast lookup index for line items by ID."""
        index: Dict[str, LineItem] = {}
        for sheet in self.sheets.values():
            for section in sheet.sections:
                for item in section.line_items:
                    if item.id in index:
                        raise ValueError(f"Duplicate line_item_id: {item.id}")
                    index[item.id] = item
        self._index = index

    def get_item(self, line_item_id: str) -> LineItem:
        """Get a line item by semantic ID (builds index if needed)."""
        if not self._index:
            self.build_index()
        return self._index[line_item_id]

    @field_validator("sheets", mode="before")
    @classmethod
    def _coerce_sheet_keys(cls, value):
        """Validate that sheets is a dict keyed by sheet name."""
        if isinstance(value, dict):
            return value
        raise ValueError("sheets must be a dict keyed by sheet name")
