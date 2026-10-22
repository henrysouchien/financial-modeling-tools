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
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Final,
    List,
    Literal,
    Optional,
    Tuple,
    TypeGuard,
    assert_never,
    get_args,
)

from pydantic import (
    BaseModel,
    Field,
    PrivateAttr,
    ValidationInfo,
    field_validator,
    model_serializer,
    model_validator,
)

from .period_value_series import PeriodValueSeries
from industry_slice_core.industry_types import DriverCategory, Unit

from .driver_formula import normalize_driver_formula_params
from .provider_port.contracts import VendorProvenance

if TYPE_CHECKING:
    from .refs import FormulaRef


PeriodAnchor = Literal["first", "last"]
_PERIOD_ANCHORS: Final[tuple[PeriodAnchor, ...]] = get_args(PeriodAnchor)


def _is_period_anchor(value: str) -> TypeGuard[PeriodAnchor]:
    return value in _PERIOD_ANCHORS


def parse_period_anchor(value: object) -> PeriodAnchor:
    """Parse the vocabulary owned by :class:`LineItemRef`."""

    parsed = str(value)
    if not _is_period_anchor(parsed):
        raise ValueError(f"period_anchor must be one of {list(_PERIOD_ANCHORS)!r}")
    return parsed


PeriodMode = Literal["yearly", "quarterly5"]
PERIOD_MODE_YEARLY: PeriodMode = "yearly"
PERIOD_MODE_QUARTERLY5: PeriodMode = "quarterly5"


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


ModelScale = Literal["millions", "units"]


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
    """Where a value came from (inputs, computed, or imported sources).

    ``default`` is a compiler-supplied mechanics value written where no
    authored row exists; ``input`` is reserved for authored values; ``claim``
    is a value written from a collected ``Claim`` (management guidance or
    street consensus, ``schema/collected_claims.py``), never a Fact.
    """
    input = "input"
    computed = "computed"
    imported_edgar = "imported_edgar"
    imported_fmp = "imported_fmp"
    imported_vendor = "imported_vendor"
    imported_other = "imported_other"
    historical_foundation = "historical_foundation"
    derived = "derived"
    default = "default"
    claim = "claim"


#: The operation that produced a compiled value (spec principle 2 plus
#: ``default``, the absence of a judgment where one is required; R4; and
#: ``claim``, a value someone stated about a number, W5a).
OperationKind = Literal["fact", "judgment", "derivation", "default", "claim"]


def operation_kind(provenance: ValueProvenance) -> OperationKind:
    """The one total mapping from a value's provenance to its operation kind."""
    match provenance:
        case (
            ValueProvenance.imported_edgar
            | ValueProvenance.imported_fmp
            | ValueProvenance.imported_vendor
            | ValueProvenance.imported_other
            | ValueProvenance.historical_foundation
        ):
            return "fact"
        case ValueProvenance.input:
            return "judgment"
        case ValueProvenance.computed | ValueProvenance.derived:
            return "derivation"
        case ValueProvenance.default:
            return "default"
        case ValueProvenance.claim:
            return "claim"
        case unreachable:
            assert_never(unreachable)


# ---------------------------------------------------------------------------
# Template metadata enums and models (F2a)
# ---------------------------------------------------------------------------


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


class EdgarCompositionTerm(BaseModel):
    """One signed filing tag inside a composed filing definition."""

    tag: str = Field(min_length=1)
    sign: Literal[1, -1] = 1


class EdgarComposition(BaseModel):
    """How a concept is composed from filing tags that report it separately.

    Some house concepts have no single us-gaap line: an issuer reports the
    gross legs and the vendor nets them. A flat tag list cannot state that —
    it would match one leg and read it as the whole — so the composition
    carries per-term orientation and is evaluated as a signed sum.
    """

    operation: Literal["signed_sum"] = "signed_sum"
    terms: List[EdgarCompositionTerm] = Field(min_length=1)


class DataSourceMapping(BaseModel):
    """Multi-provider data mapping for a required structure role."""

    required_role: str
    statement: Optional[FinancialStatement] = None
    registry_group_id: Optional[str] = None
    canonical_tag: Optional[str] = None
    edgar_tags: Optional[List[str]] = None
    edgar_composition: Optional[EdgarComposition] = None
    non_equivalent_after: Optional[int] = None
    nonadmissible_reason_code: Optional[NonadmissibleReasonCode] = None
    negate: Optional[bool] = None
    edgar_negate: Optional[bool] = None
    validation_tolerance_pct: Optional[float] = None
    missing_ok_when_covered_by: Optional[List[str]] = None
    optional_if_unreported: Optional[bool] = None
    notes: Optional[str] = None


StatementSectionRoleKind = Literal[
    "member",
    "section_total",
    "included_subtotal",
    "pre_subtotal",
    "subtotal",
    "presentation_only",
    "catch_all",
]


class StatementSectionRole(BaseModel):
    """Financial-statement section role metadata for a template row."""

    statement: FinancialStatement
    section: str
    role: StatementSectionRoleKind
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
    period_anchor: PeriodAnchor = "first"


def _coerce_formula_param_refs(value: Any) -> Any:
    """Rehydrate serialized LineItemRef shapes nested inside formula params."""
    from .refs import line_item_ref_from_obj

    ref = line_item_ref_from_obj(value, strict=True)
    if ref is not None:
        return ref
    if isinstance(value, dict):
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




FormulaDerivationStrategy = Literal[
    "revenue_ratio",
    "sbc_allocation",
    "acquired_intangible_roll_forward",
    "latest_historical_revenue_ratio",
    "reported_current_debt_bridge",
    "current_debt_classification_carry",
    "lease_current_revenue_ratio",
    "lease_current_portion_proxy",
    "lease_rou_balance_reconciliation",
    "lease_liability_balance_reconciliation",
    "net_dilution_share_curve",
    "latest_reported_net_dilution",
    "dilutive_share_increment",
    "end_share_proxy",
    "share_repurchase_revenue_ratio",
    "employee_stock_plan_revenue_ratio",
    "business_model_no_dividend_policy",
    "business_model_no_repurchase_policy",
    "business_model_no_acquisition_policy",
    "projected_non_gaap_other_items_exclusion",
    "declared_balance_roll",
    "historical_zero_balance_policy",
    "canonical_debt_lane_exclusion",
    "retained_earnings_roll_forward",
    "pass_through_gross_turnover_exclusion",
    "pass_through_balance_offset",
    "client_fund_asset_roll_forward_cash_bridge",
    "canonical_cash_flow_residual_exclusion",
    "terminal_zero_fx_policy",
    "treasury_stock_repurchase_roll_forward",
    "projected_equity_includes_treasury_stock",
    "balance_sheet_row_from_schedule",
    "corporate_cash_opening_bridge",
    "historical_physical_depreciation_residual_ratio",
    "latest_historical_nci_share",
    "historical_physical_depreciation_residual",
    "physical_depreciation_bridge",
    "reported_capitalized_software_balance",
    "capitalized_software_additions_bridge",
    "aggregate_sbc_eligible_expense_rate",
    "aggregate_sbc_uniform_historical_allocation",
    "aggregate_sbc_historical_allocation_sum",
    "canonical_retained_earnings_bridge",
    "historical_equity_sbc_offset",
    # Persisted pre-2026-09-25 sidecars carry this retired closure zero; it
    # stays readable. New builds write the four strategies below.
    "historical_non_gaap_tax_adjustment_exclusion",
    "historical_non_gaap_tax_effect_estimate",
    "historical_non_gaap_tax_rate_disclosed",
    "historical_non_gaap_tax_rate_disclosed_year_estimate",
    "historical_non_gaap_tax_rate_effective_rate_estimate",
    "historical_cash_flow_residual_exclusion",
    # Persisted pre-2026-09-30 sidecars carry this retired client-fund cash
    # bridge zero; it stays readable. New builds derive the bridge in
    # build_cash_historicals.
    "historical_client_fund_cash_bridge_exclusion",
    # Persisted model documents written before the strategies were named by
    # relationship (balance roll, pass-through exclusion/offset) carry these
    # spellings; they stay readable and no build emits them.
    "client_fund_balance_bridge",
    "client_fund_gross_turnover_exclusion",
    "client_fund_pass_through_offset",
    "composite_ppe_opening_normalization",
    "composite_intangible_opening_normalization",
    "composite_accrued_lease_opening_normalization",
    "filing_schedule_disclosure",
    "issuer_ebitda_reconciliation",
    "valuation_ticker_metadata",
    "scenario_case_probability",
    "scenario_control_label",
    "dcf_exit_multiple_derivation",
    "acquisition_goodwill_allocation_proposal",
    # A lowered override from which a cited (not_applicable / not_separately_presented)
    # operand was omitted; the omitted operands are in ``omitted_operands``.
    "cited_absence_omitted",
]


#: Retired strategy spelling -> the strategy naming the same relationship. A stored
#: model document written before the rename carries the old spelling; a reader that
#: branches on a strategy reads it through ``canonical_strategy``, never the raw name.
_RETIRED_STRATEGY_SPELLINGS: dict[FormulaDerivationStrategy, FormulaDerivationStrategy] = {
    "client_fund_balance_bridge": "declared_balance_roll",
    "client_fund_gross_turnover_exclusion": "pass_through_gross_turnover_exclusion",
    "client_fund_pass_through_offset": "pass_through_balance_offset",
}


def canonical_strategy(strategy: FormulaDerivationStrategy) -> FormulaDerivationStrategy:
    """The current spelling of ``strategy`` (itself when it was never renamed)."""

    return _RETIRED_STRATEGY_SPELLINGS.get(strategy, strategy)


class FormulaDerivation(BaseModel):
    """Typed ownership metadata for formulas synthesized by schema policy."""

    model_config = {"extra": "forbid", "frozen": True}

    owner: Literal[
        "build_non_gaap_addbacks",
        "build_company_grounded_schedules",
        "build_valuation_inputs",
        "build_scenarios",
    ]
    strategy: FormulaDerivationStrategy
    version: int = Field(default=1, ge=1)
    dependencies: Tuple[str, ...] = ()
    #: Operands a cited-operand lowering took out of this formula, as ``(item_id, offset)``;
    #: provenance for the continuity check, never something the formula reads.
    omitted_operands: Tuple[Tuple[str, int], ...] = ()


# ``column_offset_mode`` discriminator for ``valuation.offset_scenario`` params.
COLUMN_OFFSET_MODE_PERIOD_RELATIVE = "period_relative"


class FormulaSpec(BaseModel):
    """How a line item is calculated.

    params shape depends on FormulaType. Common shapes:
    - ref: {"source": LineItemRef, "adjustment": float?, "negate": bool?}
    - arithmetic: {"operands": ["+", LineItemRef, LineItemRef, ...]} or {"function": "SUM"/"AVERAGE", "items": [...]}
    - driver: {"base": expr, "rate": expr, "scale": positive float?, "scale_fn": "/N"|"*N"?}
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
    derivation: Optional[FormulaDerivation] = None
    edgar_provenance: Optional[EdgarProvenance] = None
    vendor_provenance: Optional[VendorProvenance] = None

    @field_validator("vendor_provenance", mode="before")
    @classmethod
    def _inject_vendor_provenance_registry(
        cls,
        value: object,
        info: ValidationInfo,
    ) -> object:
        from .provider_port.registry import inject_provider_registry_validation_context

        return inject_provider_registry_validation_context(cls, value, info)

    @model_serializer(mode="wrap")
    def _serialize_provenance(self, handler):
        payload = handler(self)
        if self.vendor_provenance is None:
            payload.pop("vendor_provenance", None)
        return payload

    @field_validator("params", mode="before")
    @classmethod
    def _rehydrate_refs(cls, value):
        return _coerce_formula_param_refs(value)

    @model_validator(mode="after")
    def _validate_typed_params(self):
        if self.type is FormulaType.driver:
            self.params = normalize_driver_formula_params(self.params)
        return self

    def references(
        self, *, evaluate_condition: Optional[Callable[[Any], Optional[float]]] = None
    ) -> Tuple["FormulaRef", ...]:
        """Every live ``LineItemRef`` in ``params`` with its role in the formula, in params order.

        The one walk over the documented encodings (``operands``, ``SUM``/``items``, ``op``/``args``/
        ``NEG``/``IF``, ``roll_forward``, ``ratio``, ``driver``, ``growth``, ``ref``,
        ``SUM_RANGE``, ``offset_scenario``); retained operand lineage is not walked.
        ``evaluate_condition`` narrows an ``IF`` to its condition and the selected branch.
        """
        from .refs import formula_references

        return formula_references(self, evaluate_condition=evaluate_condition)

    def without(self, predicate: Callable[["FormulaRef"], bool]) -> Optional["FormulaSpec"]:
        """This spec minus the refs ``predicate`` selects, or ``None`` if no formula remains.

        A selected sum term (``additive`` / ``subtractive``) leaves its list, a product that is a
        term of a sum leaves it whole when any ref inside it is selected, and a difference whose
        operands are both selected leaves its sum; the last term of a sum, a selected minuend whose
        subtrahend stands, and a selected ref in any other role (``numerator``, ``denominator``,
        ``base``, ``rate``, ``beginning``, ``source``, ``anchor``, ``target``, ``condition``,
        ``other``) leave no formula. The exceptions are the ``cost_debt`` and ``weight_debt`` slots
        of a ``valuation/wacc``: a selected ref there replaces that whole param with the literal
        ``0``, so the debt term is zero for every reader. ``self`` when the predicate selects nothing.
        """
        from .refs import formula_without

        return formula_without(self, predicate)


class ValueCell(BaseModel):
    """Single value with provenance for a specific period key."""
    period: int
    value: Optional[float]
    provenance: ValueProvenance
    edgar_provenance: Optional[EdgarProvenance] = None
    vendor_provenance: Optional[VendorProvenance] = None
    note: Optional[str] = None
    line_ref: Optional[Any] = None
    lineage_reference: Optional[Any] = None

    @field_validator("vendor_provenance", mode="before")
    @classmethod
    def _inject_vendor_provenance_registry(
        cls,
        value: object,
        info: ValidationInfo,
    ) -> object:
        from .provider_port.registry import inject_provider_registry_validation_context

        return inject_provider_registry_validation_context(cls, value, info)

    @model_serializer(mode="wrap")
    def _serialize_provenance(self, handler):
        payload = handler(self)
        if self.vendor_provenance is None:
            payload.pop("vendor_provenance", None)
        return payload


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
    unit: Unit | None = None
    model_scale: ModelScale = "units"
    format: str = ""
    values: Optional[ValueSeries] = None
    overrides: Optional[Dict[int, FormulaSpec]] = None
    formula_periods: Optional[List[int]] = None
    # --- Template metadata (F2a) ---
    required_role: Optional[str] = Field(
        default=None,
        pattern=r"^(statement\.(is|bs|cf)|operating|adjustment)\.[a-z0-9][a-z0-9_]*$",
    )
    style: Optional[CellStyle] = None
    driver_category: Optional[DriverCategory] = None
    customization: Optional[CustomizationType] = None
    template_token: Optional[str] = None
    build_notes: Optional[str] = None
    repeat_group_id: Optional[str] = None
    repeat_group_role: Optional[str] = None
    statement_section_roles: Optional[List[StatementSectionRole]] = Field(
        default=None,
        exclude_if=lambda value: not value,
    )


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
    assumptions: Dict[str, float | PeriodValueSeries] = Field(default_factory=dict)
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
