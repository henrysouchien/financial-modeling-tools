from __future__ import annotations

import re
from typing import Annotated, Literal

from pydantic import ConfigDict, Discriminator, Field, field_validator, model_validator

from .business_model_driver_plan import (
    _TEMPLATE_DRIVER_KEY_ALIASES,  # noqa: F401 - compatibility alias for schema.business_model imports
    _business_model_id,  # noqa: F401 - compatibility alias for schema.business_model imports
    _consolidated_driver_assumption_entries,  # noqa: F401 - compatibility alias for schema.business_model imports
    _driver_assumption_aliases,  # noqa: F401 - compatibility alias for schema.business_model imports
    _driver_assumption_entry,  # noqa: F401 - compatibility alias for schema.business_model imports
    _existing_driver_key_resolves_to_input,  # noqa: F401 - compatibility alias for schema.business_model imports
    _iter_driver_nodes,  # noqa: F401 - compatibility alias for schema.business_model imports
    _normal_existing_driver_key,  # noqa: F401 - compatibility alias for schema.business_model imports
    _plan_node_id,  # noqa: F401 - compatibility alias for schema.business_model imports
    _resolved_existing_driver_alias,  # noqa: F401 - compatibility alias for schema.business_model imports
    _template_driver_aliases,  # noqa: F401 - compatibility alias for schema.business_model imports
    _template_driver_assumption_entry,  # noqa: F401 - compatibility alias for schema.business_model imports
    _template_input_item_ids,  # noqa: F401 - compatibility alias for schema.business_model imports
    _unique_nonempty,
    derive_driver_assumption_plan,
    driver_assumption_key,
)
from .business_model_driver_specs import (
    _capital_source_driver_specs,  # noqa: F401 - compatibility alias for schema.business_model imports
    _working_capital_driver_specs,  # noqa: F401 - compatibility alias for schema.business_model imports
)
from .models import Unit
from .thesis_shared_slice import (
    _ContractModel,
    _normalize_ticker,
)


Factor = Literal["volume", "price", "unit_economics", "cost_structure", "reinvestment", "capital_sources"]
NodeBehavior = Literal["roughly_flat", "growing", "declining", "cyclical", "step_function"]
KpiFrequency = Literal["quarterly", "annual", "ad_hoc"]
ChildrenRole = Literal["decomposition", "tracking"]

RevenueModelType = Literal["volume_x_price", "market_based", "product_based", "blended"]

DriverExprType = Literal["growth", "product", "sum", "external", "derived", "roll_forward"]
SourceCategory = Literal["macro", "market_data", "manual"]

CompileTargetType = Literal["assumption_row", "derived_row", "existing_row", "commentary"]

GrossMarginBehavior = Literal["expanding", "stable", "compressing"]

CostModelAs = Literal["percent_of_revenue", "fixed_amount", "per_unit", "step_function"]

CapexModelAs = Literal["percent_of_revenue", "absolute_growth", "per_unit"]

WcModelAs = Literal["percent_of_revenue", "custom"]

SbcModelAs = Literal["percent_of_opex", "percent_of_revenue"]

Intensity = Literal["low", "moderate", "high"]
FundingModel = Literal["self_funded", "debt_funded", "equity_dependent", "blended"]
DebtLevel = Literal["none", "low", "moderate", "high"]
DilutionPattern = Literal["minimal", "moderate", "significant"]
ReturnPriority = Literal["reinvestment", "balanced", "return_focused"]
AcquisitionPattern = Literal["none", "opportunistic", "programmatic"]

RecommendedDepth = Literal["quick_and_dirty", "advanced"]

_NODE_ID_RE = re.compile(r"^[a-z][a-z0-9_]*$")
_AXIS_QNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*:[A-Za-z][A-Za-z0-9_-]*$")
_RESERVED_NODE_IDS = frozenset({"self"})


class NodeRef(_ContractModel):
    """Reference to a DriverNode within the same segment."""

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        str_strip_whitespace=True,
        populate_by_name=True,
    )

    node_id: str = Field(min_length=1)
    t: int = 0
    sign: int = 1

    @field_validator("sign", mode="before")
    @classmethod
    def _validate_sign(cls, value: object) -> int:
        if value not in (1, -1):
            raise ValueError("sign must be +1 or -1")
        return int(value)


class GrowthParams(_ContractModel):
    base: NodeRef
    rate_key: str = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_base_ref(self) -> GrowthParams:
        if self.base.node_id != "self" or self.base.t != -1:
            raise ValueError("growth base must be self[-1]")
        return self


class ProductParams(_ContractModel):
    operands: list[NodeRef] = Field(min_length=2)


class SumParams(_ContractModel):
    operands: list[NodeRef] = Field(min_length=1)


class ExternalParams(_ContractModel):
    source_category: SourceCategory
    description: str


class DerivedParams(_ContractModel):
    numerator: NodeRef
    denominator: NodeRef


class RollForwardParams(_ContractModel):
    beginning: NodeRef
    additions: list[NodeRef] = Field(default_factory=list)
    subtractions: list[NodeRef] = Field(default_factory=list)


class GrowthExpr(_ContractModel):
    type: Literal["growth"] = "growth"
    params: GrowthParams


class ProductExpr(_ContractModel):
    type: Literal["product"] = "product"
    params: ProductParams


class SumExpr(_ContractModel):
    type: Literal["sum"] = "sum"
    params: SumParams


class ExternalExpr(_ContractModel):
    type: Literal["external"] = "external"
    params: ExternalParams


class DerivedExpr(_ContractModel):
    type: Literal["derived"] = "derived"
    params: DerivedParams


class RollForwardExpr(_ContractModel):
    type: Literal["roll_forward"] = "roll_forward"
    params: RollForwardParams


DriverExpr = Annotated[
    GrowthExpr | ProductExpr | SumExpr | ExternalExpr | DerivedExpr | RollForwardExpr,
    Discriminator("type"),
]


class CompileTarget(_ContractModel):
    target_type: CompileTargetType
    sheet: str | None = "Assumptions"
    section_id: str | None = None
    existing_driver_key: str | None = None

    @model_validator(mode="after")
    def _validate_existing_row(self) -> CompileTarget:
        if self.target_type == "existing_row":
            if not self.existing_driver_key:
                raise ValueError("existing_driver_key required when target_type is 'existing_row'")
        elif self.existing_driver_key is not None:
            raise ValueError("existing_driver_key must be None when target_type is not 'existing_row'")
        return self


def _extract_node_refs(expr: DriverExpr) -> set[str]:
    """Extract all node_id references from a DriverExpr's params."""

    refs: set[str] = set()
    params = expr.params
    if hasattr(params, "operands"):
        for ref in params.operands:
            refs.add(ref.node_id)
    if hasattr(params, "base"):
        refs.add(params.base.node_id)
    if hasattr(params, "numerator"):
        refs.add(params.numerator.node_id)
        refs.add(params.denominator.node_id)
    if hasattr(params, "beginning"):
        refs.add(params.beginning.node_id)
        for ref in params.additions:
            refs.add(ref.node_id)
        for ref in params.subtractions:
            refs.add(ref.node_id)
    return refs


class DriverNode(_ContractModel):
    id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    factors: list[Factor] = Field(min_length=1)
    unit: Unit
    driver: DriverExpr | None = None
    compile_to: CompileTarget
    kpi: bool = False
    kpi_source: str | None = None
    kpi_frequency: KpiFrequency | None = None
    behavior: NodeBehavior | None = None
    management_target: str | None = None
    note: str | None = None
    children: list[DriverNode] | None = None
    children_role: ChildrenRole | None = None
    children_formula: DriverExpr | None = None

    @field_validator("id", mode="before")
    @classmethod
    def _normalize_id(cls, value: object) -> str:
        normalized = str(value or "").strip().lower()
        if not normalized:
            raise ValueError("id is required")
        if not _NODE_ID_RE.match(normalized):
            raise ValueError("id must match ^[a-z][a-z0-9_]*$")
        if normalized in _RESERVED_NODE_IDS:
            raise ValueError(f"id {normalized!r} is reserved (used as pseudo-ref in DriverExpr)")
        return normalized

    @model_validator(mode="after")
    def _validate_children_consistency(self) -> DriverNode:
        has_children = bool(self.children)

        if has_children:
            if self.children_role is None:
                raise ValueError("children_role required when children is non-empty")
            if self.children_role == "decomposition":
                if self.driver is not None:
                    raise ValueError("driver must be None when children_role is 'decomposition'")
                if self.children_formula is None:
                    raise ValueError("children_formula required when children_role is 'decomposition'")
                if self.compile_to.target_type != "derived_row":
                    raise ValueError("compile_to.target_type must be 'derived_row' for decomposition")
                if self.children_formula.type not in ("sum", "product"):
                    raise ValueError("children_formula.type must be 'sum' or 'product'")
            elif self.children_role == "tracking":
                if self.driver is None:
                    raise ValueError("driver required when children_role is 'tracking'")
                if self.children_formula is not None:
                    raise ValueError("children_formula must be None for tracking")

            if self.children_formula is not None and self.children:
                child_ids = {child.id for child in self.children}
                formula_refs = _extract_node_refs(self.children_formula)
                invalid = formula_refs - child_ids
                if invalid:
                    raise ValueError(f"children_formula references non-child nodes: {sorted(invalid)}")
        else:
            if self.compile_to.target_type != "commentary" and self.driver is None:
                raise ValueError("driver required for leaf nodes (no children)")
            if self.children_role is not None:
                raise ValueError("children_role must be None when no children")
            if self.children_formula is not None:
                raise ValueError("children_formula must be None when no children")

        return self


class CostItem(_ContractModel):
    id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    driver_key: str | None = None
    factors: list[Factor] = Field(default_factory=list)
    model_as: CostModelAs
    leverage: bool
    target_pct: float | None = None
    note: str | None = None


class GrossMargin(_ContractModel):
    behavior: GrossMarginBehavior
    current_pct: float | None = None
    target_pct: float | None = None
    drivers: list[str] = Field(default_factory=list)


class UnitEconomics(_ContractModel):
    gross_margin: GrossMargin
    unit_definition: str | None = None
    scale_dynamics: str | None = None


class RevenueModel(_ContractModel):
    type: RevenueModelType
    decomposition: list[DriverNode] = Field(min_length=1)
    consolidation_formula: DriverExpr


class Segment(_ContractModel):
    id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    match_name: str = Field(min_length=1)
    edgar_member: str | None = None
    edgar_axis: str | None = None
    is_default: bool
    revenue_share: float = Field(ge=0.0, le=1.0)
    revenue_model: RevenueModel
    unit_economics: UnitEconomics | None = None
    cost_overrides: list[CostItem] | None = None

    @field_validator("id", mode="before")
    @classmethod
    def _normalize_id(cls, value: object) -> str:
        normalized = str(value or "").strip().lower()
        if not normalized:
            raise ValueError("id is required")
        if not _NODE_ID_RE.match(normalized):
            raise ValueError("id must match ^[a-z][a-z0-9_]*$")
        return normalized

    @model_validator(mode="after")
    def _validate_axis_pair(self) -> "Segment":
        if self.edgar_axis is not None and self.edgar_member is None:
            raise ValueError(
                "edgar_axis requires edgar_member (XBRL axis without member is undefined)"
            )
        if self.edgar_axis is not None and not _AXIS_QNAME_RE.match(self.edgar_axis):
            raise ValueError(
                f"edgar_axis {self.edgar_axis!r} must be an XBRL QName "
                "(e.g., srt:ProductOrServiceAxis), not a family label"
            )
        return self


class CostStructure(_ContractModel):
    items: list[CostItem] = Field(default_factory=list)


class Capex(_ContractModel):
    model_as: CapexModelAs
    intensity: Intensity
    components: list[str] = Field(default_factory=list)
    depreciation_rate: float | None = None
    amortization_rate: float | None = None


class WorkingCapital(_ContractModel):
    model_as: WcModelAs
    intensity: Intensity
    driver_keys: dict[str, str] | None = None
    notes: list[str] | None = None


class Acquisitions(_ContractModel):
    pattern: AcquisitionPattern
    note: str | None = None


class Reinvestment(_ContractModel):
    capex: Capex
    working_capital: WorkingCapital | None = None
    acquisitions: Acquisitions | None = None


class Debt(_ContractModel):
    level: DebtLevel
    target_metric: str | None = None


class Sbc(_ContractModel):
    model_as: SbcModelAs | None = None
    offset: str | None = None


class Equity(_ContractModel):
    dilution_pattern: DilutionPattern
    sbc: Sbc | None = None


class CapitalReturn(_ContractModel):
    priority: ReturnPriority
    mechanisms: list[str] = Field(default_factory=list)


class CapitalSources(_ContractModel):
    funding_model: FundingModel
    debt: Debt | None = None
    equity: Equity | None = None
    capital_return: CapitalReturn | None = None


class Consolidated(_ContractModel):
    cost_structure: CostStructure
    reinvestment: Reinvestment | None = None
    capital_sources: CapitalSources | None = None


class ProfitabilityTargets(_ContractModel):
    adjusted_ebitda_margin: float | None = None
    free_cash_flow_margin: float | None = None
    operating_margin: float | None = None


class BmCompanyInfo(_ContractModel):
    ticker: str
    name: str = Field(min_length=1)
    sector: str | None = None
    industry: str | None = None
    business_type: str | None = None

    @field_validator("ticker", mode="before")
    @classmethod
    def _validate_ticker(cls, value: object) -> str:
        return _normalize_ticker(value)


class BmDecisionsLogEntry(_ContractModel):
    date: str = Field(min_length=1)
    change: str = Field(min_length=1)
    rationale: str | None = None


class BusinessModelMetadata(_ContractModel):
    revision: str | None = None
    created_by: str | None = None
    created_at: str | None = None
    last_updated: str | None = None
    last_reviewed_at: str | None = None
    review_basis_filing: str | None = None
    change_triggers: list[str] = Field(default_factory=list)
    source_materials: list[str] = Field(default_factory=list)


def _collect_node_ids(nodes: list[DriverNode], seen: set[str], segment_id: str) -> None:
    """Recursively validate node ID uniqueness within a segment tree."""

    for node in nodes:
        if node.id in seen:
            raise ValueError(f"duplicate node id {node.id!r} in segment {segment_id!r}")
        seen.add(node.id)
        if node.children:
            _collect_node_ids(node.children, seen, segment_id)


class BusinessModel(_ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    company: BmCompanyInfo
    segments: list[Segment] = Field(min_length=1)
    consolidated: Consolidated | None = None
    profitability_targets: ProfitabilityTargets | None = None
    recommended_depth: RecommendedDepth | None = None
    decisions_log: list[BmDecisionsLogEntry] = Field(default_factory=list)
    metadata: BusinessModelMetadata = Field(default_factory=BusinessModelMetadata)

    @model_validator(mode="after")
    def _validate_structure(self) -> BusinessModel:
        seg_ids: set[str] = set()
        for segment in self.segments:
            if segment.id in seg_ids:
                raise ValueError(f"duplicate segment id: {segment.id!r}")
            seg_ids.add(segment.id)

        for segment in self.segments:
            _collect_node_ids(segment.revenue_model.decomposition, set(), segment.id)

        return self


class DriverAssumptionPlanEntry(_ContractModel):
    driver_key: str = Field(min_length=1)
    business_model_id: str = Field(min_length=1)
    revision: str = Field(min_length=1)
    segment_id: str = Field(min_length=1)
    driver_node_id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    unit: Unit
    factors: list[Factor] = Field(default_factory=list)
    behavior: NodeBehavior | None = None
    management_target: str | None = None
    compile_target_type: CompileTargetType
    existing_driver_key: str | None = None
    base_case_required: bool = False
    aliases: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def _normalize_aliases(self) -> "DriverAssumptionPlanEntry":
        self.aliases = _unique_nonempty([*self.aliases, self.existing_driver_key])
        return self


class DriverAssumptionPlan(_ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    business_model_id: str = Field(min_length=1)
    revision: str = Field(min_length=1)
    entries: list[DriverAssumptionPlanEntry] = Field(default_factory=list)
    alias_map: dict[str, str] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _validate_keyspace(self) -> "DriverAssumptionPlan":
        entries_by_key: dict[str, DriverAssumptionPlanEntry] = {}
        alias_map: dict[str, str] = {}

        for entry in self.entries:
            if entry.driver_key in entries_by_key:
                raise ValueError(f"duplicate driver_key: {entry.driver_key!r}")
            entries_by_key[entry.driver_key] = entry

        for raw_alias, raw_target in self.alias_map.items():
            alias = str(raw_alias or "").strip()
            target = str(raw_target or "").strip()
            if not alias or not target:
                raise ValueError("alias_map entries must have non-empty alias and target")
            if target not in entries_by_key:
                raise ValueError(f"alias_map target does not exist: {target!r}")
            alias_map[alias] = target

        for entry in self.entries:
            for alias in entry.aliases:
                if alias == entry.driver_key:
                    continue
                existing = alias_map.get(alias)
                if existing is not None and existing != entry.driver_key:
                    raise ValueError(
                        f"alias {alias!r} maps to both {existing!r} and {entry.driver_key!r}"
                    )
                alias_map[alias] = entry.driver_key

        self.alias_map = alias_map
        return self

    @property
    def driver_keys(self) -> list[str]:
        return [entry.driver_key for entry in self.entries]

    def entry_for_driver_key(self, driver_key: str) -> DriverAssumptionPlanEntry | None:
        normalized = str(driver_key or "").strip()
        for entry in self.entries:
            if entry.driver_key == normalized:
                return entry
        canonical = self.alias_map.get(normalized)
        if canonical is None:
            return None
        return self.entry_for_driver_key(canonical)

    def resolve_driver_key(self, driver_key: str) -> str | None:
        normalized = str(driver_key or "").strip()
        if not normalized:
            return None
        if any(entry.driver_key == normalized for entry in self.entries):
            return normalized
        return self.alias_map.get(normalized)

    def accepted_driver_keys(self) -> list[str]:
        return sorted({*self.driver_keys, *self.alias_map})


DriverNode.model_rebuild()
BusinessModel.model_rebuild()


__all__ = [
    "AcquisitionPattern",
    "Acquisitions",
    "BmCompanyInfo",
    "BmDecisionsLogEntry",
    "BusinessModel",
    "BusinessModelMetadata",
    "Capex",
    "CapexModelAs",
    "CapitalReturn",
    "CapitalSources",
    "ChildrenRole",
    "CompileTarget",
    "CompileTargetType",
    "Consolidated",
    "CostItem",
    "CostModelAs",
    "CostStructure",
    "Debt",
    "DebtLevel",
    "DerivedExpr",
    "DerivedParams",
    "DilutionPattern",
    "DriverExpr",
    "DriverExprType",
    "DriverNode",
    "DriverAssumptionPlan",
    "DriverAssumptionPlanEntry",
    "Equity",
    "ExternalExpr",
    "ExternalParams",
    "Factor",
    "FundingModel",
    "GrossMargin",
    "GrossMarginBehavior",
    "GrowthExpr",
    "GrowthParams",
    "Intensity",
    "KpiFrequency",
    "NodeBehavior",
    "NodeRef",
    "ProductExpr",
    "ProductParams",
    "ProfitabilityTargets",
    "RecommendedDepth",
    "Reinvestment",
    "ReturnPriority",
    "RevenueModel",
    "RevenueModelType",
    "RollForwardExpr",
    "RollForwardParams",
    "Sbc",
    "SbcModelAs",
    "Segment",
    "SourceCategory",
    "SumExpr",
    "SumParams",
    "UnitEconomics",
    "WcModelAs",
    "WorkingCapital",
    "derive_driver_assumption_plan",
    "driver_assumption_key",
]
