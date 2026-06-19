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

import hashlib
import json
from datetime import date
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Tuple

from pydantic import BaseModel, Field, PrivateAttr, field_validator


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
    derived = "derived"


# ---------------------------------------------------------------------------
# Template metadata enums and models (F2a)
# ---------------------------------------------------------------------------


class DriverCategory(str, Enum):
    """SIA five-driver classification."""

    revenue = "revenue"
    unit_economics = "unit_economics"
    cost_structure = "cost_structure"
    reinvestment = "reinvestment"
    capital_sources = "capital_sources"
    valuation = "valuation"
    other = "other"


class CustomizationType(str, Enum):
    """How a template item adapts per company."""

    fixed = "fixed"
    rename = "rename"
    optional = "optional"
    repeatable = "repeatable"


class CellColor(str, Enum):
    """SIA color coding convention."""

    input_blue = "input_blue"
    formula_black = "formula_black"
    key_driver = "key_driver"
    header = "header"


class SheetType(str, Enum):
    """Sheet purpose classification."""

    assumptions = "assumptions"
    financial_model = "financial_model"
    valuation = "valuation"
    scenarios = "scenarios"


class BuildStatus(str, Enum):
    """Model build lifecycle stage."""

    template = "template"
    structure_set = "structure_set"
    historicals_populated = "historicals_populated"
    assumptions_set = "assumptions_set"
    complete = "complete"


class FinancialStatement(str, Enum):
    """Financial statement classification for data mapping."""

    income_statement = "income_statement"
    balance_sheet = "balance_sheet"
    cash_flow = "cash_flow"


class NonadmissibleReasonCode(str, Enum):
    """Why an EDGAR tag list is permanently excluded from registry migration."""

    broader_or_narrower_scope = "broader_or_narrower_scope"
    different_measurement_level = "different_measurement_level"
    computational_fallback = "computational_fallback"
    partial_or_combined_concept = "partial_or_combined_concept"
    disclosure_or_context_mismatch = "disclosure_or_context_mismatch"


class DataSourceMapping(BaseModel):
    """Multi-provider data mapping for a financial concept."""

    concept_id: str
    statement: Optional[FinancialStatement] = None
    registry_group_id: Optional[str] = None
    canonical_tag: Optional[str] = None
    edgar_tags: Optional[List[str]] = None
    non_equivalent_after: Optional[int] = None
    nonadmissible_reason_code: Optional[NonadmissibleReasonCode] = None
    fmp_field: Optional[str] = None
    fallback_fmp_field: Optional[str] = None
    fmp_endpoint: Optional[str] = None
    treat_zero_as_missing: Optional[bool] = None
    negate: Optional[bool] = None
    edgar_negate: Optional[bool] = None
    fmp_negate: Optional[bool] = None
    preferred_source: Optional[str] = None
    validation_tolerance_pct: Optional[float] = None
    missing_ok_when_covered_by: Optional[List[str]] = None
    optional_if_unreported: Optional[bool] = None
    notes: Optional[str] = None


class CellStyle(BaseModel):
    """Cell-level formatting for Excel rendering."""

    color: Optional[CellColor] = None
    bold: Optional[bool] = None
    fill_color: Optional[str] = None
    number_format: Optional[str] = None
    indent: Optional[int] = None


class SectionStyle(BaseModel):
    """Section-level formatting."""

    header_style: Optional[CellStyle] = None
    show_top_border: Optional[bool] = None
    show_bottom_border: Optional[bool] = None


class SheetLayout(BaseModel):
    """Column structure for one sheet."""

    label_column: Optional[str] = None
    first_data_column: Optional[str] = None
    column_width_label: Optional[float] = None
    column_width_data: Optional[float] = None
    header_rows: Optional[int] = None
    freeze_panes: Optional[str] = None
    period_scope: Literal["all", "projection", "historical"] = "all"


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
    period_anchor: Literal["first", "last"] = "first"


_LINE_ITEM_REF_KEYS = frozenset({"id", "t", "resolved", "period_anchor"})


def _coerce_formula_param_refs(value: Any) -> Any:
    """Rehydrate serialized LineItemRef shapes nested inside formula params."""
    if isinstance(value, LineItemRef):
        return value
    if isinstance(value, dict):
        if "id" in value and value.keys() <= _LINE_ITEM_REF_KEYS:
            return LineItemRef(
                id=str(value["id"]),
                t=int(value.get("t", 0)),
                resolved=bool(value.get("resolved", True)),
                period_anchor=value.get("period_anchor", "first"),
            )
        return {key: _coerce_formula_param_refs(nested) for key, nested in value.items()}
    if isinstance(value, list):
        return [_coerce_formula_param_refs(nested) for nested in value]
    if isinstance(value, tuple):
        return tuple(_coerce_formula_param_refs(nested) for nested in value)
    return value


class EdgarProvenance(BaseModel):
    """Per-period EDGAR source details for imported actuals."""

    registry_group_id: Optional[str] = None
    metric_tag: str
    registry_revision: Optional[str] = None


class FmpProvenance(BaseModel):
    """Per-period FMP source details for imported actuals."""

    endpoint: str
    field: str
    fallback_field_used: Optional[str] = None


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
    edgar_provenance: Optional[EdgarProvenance] = None
    fmp_provenance: Optional[FmpProvenance] = None

    @field_validator("params", mode="before")
    @classmethod
    def _rehydrate_refs(cls, value):
        return _coerce_formula_param_refs(value)


class ValueCell(BaseModel):
    """Single value with provenance for a specific period key."""
    period: int
    value: Optional[float]
    provenance: ValueProvenance
    edgar_provenance: Optional[EdgarProvenance] = None
    fmp_provenance: Optional[FmpProvenance] = None
    note: Optional[str] = None


class ValueSeries(BaseModel):
    """Time series of values for one line item."""
    values: Dict[int, ValueCell] = Field(default_factory=dict)
    last_updated: Optional[str] = None


class LineItem(BaseModel):
    """One row in the model (line item)."""
    id: str
    label: str
    row: int
    column: Optional[str] = None
    label_column: Optional[str] = None
    item_type: ItemType
    xbrl_tag: Optional[str] = None
    historical: Optional[FormulaSpec] = None
    projected: Optional[FormulaSpec] = None
    unit: Unit = Unit.dollars
    format: str = ""
    values: Optional[ValueSeries] = None
    overrides: Optional[Dict[int, FormulaSpec]] = None
    formula_periods: Optional[List[int]] = None
    # --- Template metadata (F2a) ---
    data_concept_id: Optional[str] = None
    style: Optional[CellStyle] = None
    driver_category: Optional[DriverCategory] = None
    customization: Optional[CustomizationType] = None
    template_token: Optional[str] = None
    build_notes: Optional[str] = None
    repeat_group_id: Optional[str] = None
    repeat_group_role: Optional[str] = None


class Section(BaseModel):
    """Logical grouping of line items (e.g., income statement)."""
    id: str
    label: str
    line_items: List[LineItem] = Field(default_factory=list)
    # --- Template metadata (F2a) ---
    driver_category: Optional[DriverCategory] = None
    build_phase: Optional[int] = None
    style: Optional[SectionStyle] = None
    description: Optional[str] = None
    is_repeatable: Optional[bool] = None


class Sheet(BaseModel):
    """One sheet in the model (assumptions, financial_model, valuation, scenarios)."""
    name: str
    sections: List[Section] = Field(default_factory=list)
    # --- Template metadata (F2a) ---
    layout: Optional[SheetLayout] = None
    description: Optional[str] = None
    sheet_type: Optional[SheetType] = None


class FiscalPeriodMetadata(BaseModel):
    """Calendar metadata for one fiscal-local period key."""
    period_key: int
    period_mode: str
    fiscal_year: int
    fiscal_slot: int
    period_type: Literal["quarter", "annual"]
    calendar_start: Optional[date] = None
    calendar_end: Optional[date] = None
    calendar_year: Optional[int] = None
    calendar_quarter: Optional[str] = None
    comparison_key: Optional[str] = None
    duration_months: Optional[int] = None
    period_end_source: Literal["reported_period_end", "fiscal_year_end_month"] = "fiscal_year_end_month"


class TimeStructure(BaseModel):
    """Defines the model's time axis and Excel column mapping."""
    fiscal_year_end: str
    period_mode: str = PERIOD_MODE_YEARLY
    historical_periods: List[int] = Field(default_factory=list)
    projection_periods: List[int] = Field(default_factory=list)
    period_column_map: Dict[int, str] = Field(default_factory=dict)
    period_metadata: Dict[int, FiscalPeriodMetadata] = Field(default_factory=dict)
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
    # --- Template metadata (F2a) ---
    is_template: Optional[bool] = None
    methodology: Optional[str] = None
    source_model: Optional[str] = None
    build_status: Optional[BuildStatus] = None


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


MODELS_SCHEMA_VERSION: str = hashlib.sha1(
    json.dumps(
        FinancialModel.model_json_schema(),
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
).hexdigest()[:16]
