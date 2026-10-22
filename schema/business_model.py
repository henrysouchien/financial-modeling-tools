from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Annotated, Any, Literal

from pydantic import (
    AfterValidator,
    BeforeValidator,
    ConfigDict,
    Discriminator,
    Field,
    WithJsonSchema,
    field_validator,
    PrivateAttr,
    model_validator,
)

from .business_model_driver_plan import derive_driver_assumption_plan, driver_assumption_key
from .business_model_sufficiency import (
    BusinessModelCapabilityProfile,
    BusinessModelSemanticSuccessorAction,
)
from .models import ModelScale
from .required_role_catalog import (
    balance_declaration_refusal,
    balance_roles,
    load_required_role_catalog,
)
from industry_slice_core.industry_types import Unit
from .stable_control_identity import BindingId, ControlId, DriverId
from .scale_conversion import SCALE_EXPONENT
from .thesis_shared_slice import (
    _ContractModel,
    _normalize_ticker,
)


Factor = Literal["volume", "price", "unit_economics", "cost_structure", "reinvestment", "capital_sources"]
NodeBehavior = Literal["roughly_flat", "growing", "declining", "cyclical", "step_function"]
KpiFrequency = Literal["quarterly", "annual", "ad_hoc"]
ChildrenRole = Literal["decomposition", "tracking"]
ObservationBasis = Literal["period_flow", "period_end", "period_average"]
Dimension = Literal["money", "entity", "money_per_entity", "share", "money_per_share", "dimensionless", "time", "money_per_time"]
UNIT_DIMENSION: Mapping[Unit, Dimension] = {
    Unit.dollars: "money",
    Unit.item_count: "entity",
    Unit.per_count: "money_per_entity",
    Unit.per_day: "money_per_time",
    Unit.share_count: "share",
    Unit.per_share: "money_per_share",
    Unit.percentage: "dimensionless",
    Unit.ratio: "dimensionless",
    Unit.multiple: "dimensionless",
    Unit.days: "time",
}
_DIMENSION_UNIT = {dimension: unit for unit, dimension in UNIT_DIMENSION.items()}
_DIMENSION_UNIT["dimensionless"] = Unit.ratio


def product_dimension(operands: Sequence[Dimension]) -> Dimension | None:
    """Return the expressible dimension of a product, ignoring scalar factors."""
    dimensions: list[Dimension] = [
        dimension for dimension in operands if dimension != "dimensionless"
    ]
    if not dimensions:
        return "dimensionless"
    if len(dimensions) == 1:
        return dimensions[0]
    if len(dimensions) == 2 and set(dimensions) in (
        {"entity", "money_per_entity"},
        {"share", "money_per_share"},
        {"time", "money_per_time"},
    ):
        return "money"
    return None


def quotient_dimension(numerator: Dimension, denominator: Dimension) -> Dimension | None:
    """Return an expressible quotient dimension without erasing intensity units."""
    if denominator == "dimensionless":
        return numerator
    if numerator == denominator:
        return "dimensionless"
    if numerator == "money":
        if denominator == "entity":
            return "money_per_entity"
        if denominator == "time":
            return "money_per_time"
        if denominator == "share":
            return "money_per_share"
    return None


def unit_for_dimension(dimension: Dimension) -> Unit:
    """Return the canonical model unit; scalar calculations use ratio, not percent."""
    return _DIMENSION_UNIT[dimension]


@dataclass(frozen=True)
class NodeMeasure:
    unit: Unit
    model_scale: ModelScale
    basis: Literal["flow", "balance", "average"] | None = None


def sum_operand_matches_output(operand: NodeMeasure, output: NodeMeasure) -> bool:
    """Unscaled sums and remainders require the node's exact unit and scale.

    Observation basis is independent of this numeric compatibility rule.
    """
    return (operand.unit, operand.model_scale) == (output.unit, output.model_scale)


#: Stored artifacts that predate the unit-relationship cutover named a per-count
#: monetization level by this `kpi_source` and declared it in dollars. The stored
#: BusinessModel decode marks such a node `DriverNode.legacy_per_count`; its stored
#: bytes, and so the revision's content digest, are never rewritten or refused. A
#: new judgment is validated at its own door before this decode.
LEGACY_PER_COUNT_KPI_SOURCES = frozenset({"revenue_per_client"})


def node_measure(node: DriverNode) -> NodeMeasure:
    """Read the measure without rewriting legacy declarations.

    Aggregate dollars default to millions; external price-only dollars and all
    other units default to units. A node the stored decode marked
    `legacy_per_count` and legacy unit-scale dollar prices read as per-count; a
    per-count series is declared by its own `unit`. Observation basis is never
    inferred.
    """
    scale = node.model_scale
    if scale is None:
        price_like = (
            node.driver is not None
            and node.driver.type == "external"
            and set(node.factors) == {"price"}
        )
        scale = "millions" if node.unit == Unit.dollars and not price_like else "units"
    basis: Literal["flow", "balance", "average"] | None = None
    if node.observation_basis == "period_flow":
        basis = "flow"
    elif node.observation_basis == "period_end":
        basis = "balance"
    elif node.observation_basis == "period_average":
        basis = "average"
    if node.legacy_per_count:
        return NodeMeasure(Unit.per_count, "units", basis)
    if node.unit == Unit.dollars and set(node.factors) == {"price"} and scale == "units":
        # This legacy interpretation is per-count only: a days counterpart does
        # not imply per-day dollars; that intensity must remain explicit.
        return NodeMeasure(Unit.per_count, "units", basis)
    return NodeMeasure(node.unit, scale, basis)


def assumption_unit_matches(declared: Unit, node: DriverNode) -> bool:
    """Accept the originating node's stored or resolved unit declaration."""
    return declared == node_measure(node).unit or declared == node.unit


def product_scale_exponent(
    operand_measures: Sequence[NodeMeasure], output: NodeMeasure
) -> int:
    return (
        sum(SCALE_EXPONENT[measure.model_scale] for measure in operand_measures)
        - SCALE_EXPONENT[output.model_scale]
    )



RevenueScope = Literal["own_revenue_stream_only", "consolidated_revenue"]
#: The balances a business-model node may be: every catalog balance a driver
#: may declare (`required_role_catalog.balance_roles`, derived from the
#: catalog, never listed here). A node declaring ``balance_of`` *is* that
#: balance, observed at its ``observation_basis`` (the period-end balance, or
#: its period average), and the company-grounded schedule moves the reported
#: balance with the node.
def _declarable_balance(value: str) -> str:
    refusal = balance_declaration_refusal(value)
    if refusal is not None:
        raise ValueError(
            f"{refusal.message}; a driver may be one of {sorted(balance_roles())}"
        )
    return value


def _stored_balance(value: str) -> str:
    if load_required_role_catalog().legacy_balance_roles.get(value, value) not in balance_roles():
        raise ValueError(
            f"{value!r} is neither a catalog balance nor a legacy spelling of one"
        )
    return value


BalanceRole = Annotated[
    str,
    AfterValidator(_declarable_balance),
    WithJsonSchema({"type": "string", "enum": sorted(balance_roles())}),
]
#: A stored revision's balance token: a catalog balance, or the legacy spelling
#: the catalog's ``legacy_roles`` declares for one. Never authored.
StoredBalanceRole = Annotated[
    str,
    AfterValidator(_stored_balance),
    WithJsonSchema({
        "type": "string",
        "enum": sorted(
            (
                *balance_roles(),
                *(
                    token
                    for token, role in load_required_role_catalog().legacy_balance_roles.items()
                    if role in balance_roles()
                ),
            )
        ),
    }),
]


def declared_balance_role(node: Any) -> str | None:
    """The balance a driver node is: its ``balance_of``, else the balance its ``kpi_source`` names.

    A node whose ``kpi_source`` is the balance role itself observes that balance, so
    both spellings declare it and every reader of the declaration resolves the same one.
    """
    declared = getattr(node, "balance_of", None)
    if declared is not None:
        return load_required_role_catalog().legacy_balance_roles.get(declared, declared)
    source = getattr(node, "kpi_source", None)
    return source if source in balance_roles() else None


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
# Revenue KPI-source local-names that name a single consolidated EDGAR revenue
# concept. Two absorb-segment nodes sharing one of these + no edgar_member would
# both denote the consolidated total → 2x; the schema partition guard below
# rejects that shape at authoring time (the retired inline-value seeder's
# mirror set died with the reverse mint, Stage A step A-5).
_SEGMENT_REVENUE_KPI_SOURCE_TAGS = frozenset({
    "revenuefromcontractwithcustomerexcludingassessedtax",
    "revenues",
    "salesrevenuegoodsnet",
    "salesrevenuenet",
})
_RESERVED_NODE_IDS = frozenset({"self"})


#: A statement role id (`statement.<statement>.<line>`): the one spelling of a
#: statement-line operand. `NodeRef.node_id` names a sibling node of the same
#: segment or, for a calculation's operand, a pinned statement role.
STATEMENT_ROLE_PATTERN = r"^statement\.[a-z]+\.[a-z0-9_]+$"
_STATEMENT_ROLE_RE = re.compile(STATEMENT_ROLE_PATTERN)


def is_statement_role(value: object) -> bool:
    return isinstance(value, str) and _STATEMENT_ROLE_RE.fullmatch(value) is not None


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

    @property
    def is_statement_role(self) -> bool:
        return is_statement_role(self.node_id)

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
        if self.target_type == "commentary":
            # Commentary has no workbook destination. Canonicalize the field
            # so an omitted/null sheet survives markdown render/parse exactly
            # instead of being reintroduced as the Assumptions default.
            self.sheet = None
        if self.target_type == "existing_row":
            if not self.existing_driver_key:
                raise ValueError("existing_driver_key required when target_type is 'existing_row'")
        elif self.existing_driver_key is not None:
            raise ValueError("existing_driver_key must be None when target_type is not 'existing_row'")
        return self


def _extract_node_refs(expr: DriverExpr) -> set[str]:
    """Extract all node_id references from a DriverExpr's params."""

    refs: set[str] = set()
    if expr.type == "product" or expr.type == "sum":
        refs.update(ref.node_id for ref in expr.params.operands)
    elif expr.type == "growth":
        refs.add(expr.params.base.node_id)
    elif expr.type == "derived":
        refs.add(expr.params.numerator.node_id)
        refs.add(expr.params.denominator.node_id)
    elif expr.type == "roll_forward":
        refs.add(expr.params.beginning.node_id)
        refs.update(ref.node_id for ref in expr.params.additions)
        refs.update(ref.node_id for ref in expr.params.subtractions)
    return refs


def expr_reads_statement_role(expr: DriverExpr | None) -> bool:
    """Whether a calculation has a statement role among its operands.

    Such a node's history is the calculation over the pinned statement series
    (`schema.build_bm_node_history`); it is no row-to-row formula, so the
    compiler materializes it as an input row, like an external one.
    """

    return expr is not None and any(is_statement_role(ref) for ref in _extract_node_refs(expr))


def expr_is_input_row(expr: DriverExpr | None, target_type: str) -> bool:
    """Whether a node's forecast is an authored control on an input row.

    ``external`` is one by construction. A ratio compiled to ``assumption_row``
    is another: its history is the calculation (`schema.build_bm_node_history`)
    and its forecast is authored, as for a calculation over a statement role,
    so a price that is itself a factor of its own revenue stays a driver. The
    compiler, the forecast control contracts and the forecast authority all
    read this one rule.
    """

    if expr is None:
        return False
    return (
        expr.type == "external"
        or expr_reads_statement_role(expr)
        or (expr.type == "derived" and target_type == "assumption_row")
    )


def _decimal_from_json(value: object) -> object:
    """A JSON number is a Decimal by its printed digits, not its binary float."""
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return Decimal(str(value))
    return value


_OBSERVATION_PERIOD_RE = re.compile(r"^FY[0-9]{4}$")


class KpiSourceCitation(_ContractModel):
    """One captured ``filing_text`` KPI row: ``{capture_id, row, span}`` or ``{accession, row, span}``."""

    model_config = ConfigDict(
        frozen=True, extra="forbid", str_strip_whitespace=True, populate_by_name=True
    )

    kind: Literal["kpi"] = "kpi"
    capture_id: str | None = Field(default=None, min_length=1)
    accession: str | None = Field(default=None, min_length=1)
    row: int = Field(ge=0)
    span: str = Field(min_length=1)

    @model_validator(mode="after")
    def _names_its_capture(self) -> "KpiSourceCitation":
        if self.capture_id is None and self.accession is None:
            raise ValueError("a KPI citation names a capture_id or an accession")
        return self

    def source_key(self) -> dict[str, Any]:
        """The key ``research.corpus_state.resolve_source`` resolves."""
        if self.capture_id is not None:
            return {"kind": "kpi", "capture_id": self.capture_id, "row": self.row, "span": self.span}
        return {"accession": self.accession, "row": self.row, "span": self.span}


class OccurrenceSourceCitation(_ContractModel):
    """The arc a statement row cites: ``{accession, preferred_label, tree_depth, cell_locator}``."""

    model_config = ConfigDict(
        frozen=True, extra="forbid", str_strip_whitespace=True, populate_by_name=True
    )

    accession: str = Field(min_length=1)
    preferred_label: str = Field(min_length=1)
    tree_depth: int = Field(ge=0)
    cell_locator: str = Field(min_length=1)

    def source_key(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class CaptureSourceCitation(_ContractModel):
    """A located span of a captured file: ``{capture_id, path?, start?, end?}``."""

    model_config = ConfigDict(
        frozen=True, extra="forbid", str_strip_whitespace=True, populate_by_name=True
    )

    capture_id: str = Field(min_length=1)
    path: str | None = Field(default=None, min_length=1)
    start: int | None = Field(default=None, ge=0)
    end: int | None = Field(default=None, gt=0)

    def source_key(self) -> dict[str, Any]:
        return self.model_dump(mode="json", exclude_none=True)


ObservationCitation = KpiSourceCitation | OccurrenceSourceCitation | CaptureSourceCitation


class CitedObservation(_ContractModel):
    """One value the author read for a driver node in one historical fiscal year.

    The citation names where it was read; ``business_model_observation_claim_key``
    is the claim key the stage binds its durable evidence to.
    """

    model_config = ConfigDict(
        frozen=True, extra="forbid", str_strip_whitespace=True, populate_by_name=True
    )

    period_id: str
    value: Annotated[Decimal, BeforeValidator(_decimal_from_json)]
    unit: Unit
    model_scale: ModelScale
    citation: ObservationCitation
    confidence: Literal["high", "medium", "low"] = "high"
    note: str | None = Field(default=None, min_length=1)

    @field_validator("period_id")
    @classmethod
    def _fiscal_year(cls, value: str) -> str:
        if _OBSERVATION_PERIOD_RE.fullmatch(value) is None:
            raise ValueError("observation period_id is an actual fiscal year such as 'FY2024'")
        return value

    @field_validator("value")
    @classmethod
    def _finite(cls, value: Decimal) -> Decimal:
        if not value.is_finite():
            raise ValueError("observation value must be finite")
        return value

    @model_validator(mode="after")
    def _scale_matches_unit(self) -> "CitedObservation":
        if self.unit is not Unit.dollars and self.model_scale != "units":
            raise ValueError("non-dollar observations use units scale")
        return self


def business_model_observation_claim_key(node_key: str, period_id: str) -> str:
    """The claim key one node's observation of one period is bound to evidence under."""
    return f"observation.{node_key}.{period_id}"


class DriverNode(_ContractModel):
    id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    factors: list[Factor] = Field(min_length=1)
    unit: Unit
    model_scale: ModelScale | None = None
    driver: DriverExpr | None = None
    compile_to: CompileTarget
    kpi: bool = False
    kpi_source: str | None = None
    kpi_frequency: KpiFrequency | None = None
    edgar_member: str | None = None
    edgar_axis: str | None = None
    behavior: NodeBehavior | None = None
    observation_basis: ObservationBasis | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    revenue_scope: RevenueScope | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    balance_of: StoredBalanceRole | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    management_target: str | None = None
    note: str | None = None
    children: list[DriverNode] | None = None
    children_role: ChildrenRole | None = None
    children_formula: DriverExpr | None = None
    historical_observations: tuple[CitedObservation, ...] = Field(
        default=(), exclude_if=lambda value: not value
    )
    _legacy_per_count: bool = PrivateAttr(default=False)

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
    def _decode_legacy_per_count(self) -> DriverNode:
        """The stored-shape decode: `LEGACY_PER_COUNT_KPI_SOURCES` names a per-count level.

        The legacy reading is the shape a pre-cutover artifact needs when its own data
        is per-count (or silent); a node whose recorded observations are not per-count
        units declares what it observes, and that declaration stands.
        """
        self._legacy_per_count = (
            self.unit == Unit.dollars
            and self.kpi_source in LEGACY_PER_COUNT_KPI_SOURCES
            and all(
                (row.unit, row.model_scale) == (Unit.per_count, "units")
                for row in self.historical_observations
            )
        )
        return self

    @property
    def legacy_per_count(self) -> bool:
        """True when the stored declaration (dollars, a legacy per-count `kpi_source`) means per-count."""
        return self._legacy_per_count

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

    @model_validator(mode="after")
    def _validate_observations(self) -> DriverNode:
        if not self.historical_observations:
            return self
        periods = [row.period_id for row in self.historical_observations]
        if len(periods) != len(set(periods)):
            raise ValueError(f"node {self.id!r} observes a period more than once")
        measure = node_measure(self)
        for row in self.historical_observations:
            if (row.unit, row.model_scale) != (measure.unit, measure.model_scale):
                raise ValueError(
                    f"node {self.id!r} is ({measure.unit.value}, {measure.model_scale}); its "
                    f"{row.period_id} observation is ({row.unit.value}, {row.model_scale})"
                )
        return self

    @model_validator(mode="after")
    def _validate_balance_declaration(self) -> DriverNode:
        balance_role = declared_balance_role(self)
        if balance_role is None:
            return self
        if self.observation_basis not in ("period_end", "period_average"):
            raise ValueError(
                f"node {self.id!r} declares balance_of {balance_role!r} and must "
                "observe that balance at period_end or period_average"
            )
        if self.unit is not Unit.dollars:
            raise ValueError(
                f"node {self.id!r} declares balance_of {balance_role!r} and must "
                "be denominated in dollars"
            )
        if self.compile_to.target_type not in ("assumption_row", "derived_row"):
            raise ValueError(
                f"node {self.id!r} declares balance_of {balance_role!r} and must "
                "compile to its own assumption_row or derived_row"
            )
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


class AbsorbedClaim(_ContractModel):
    name: str | None = None
    member: str | None = None

    @model_validator(mode="after")
    def _validate_claim(self) -> "AbsorbedClaim":
        if not str(self.name or "").strip() and not str(self.member or "").strip():
            raise ValueError("absorbed claim requires name and/or member")
        return self


class Segment(_ContractModel):
    id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    match_name: str = Field(min_length=1)
    edgar_member: str | None = None
    edgar_axis: str | None = None
    absorbs: list[AbsorbedClaim] | None = None
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
        # Asymmetry, stated deliberately: axis-without-member is rejected here,
        # but member-without-axis is TOLERATED at segment level (the node branch
        # below requires both-or-neither). The tolerance was a migration
        # concession for legacy bridge output (`3312ee236`); its beneficiary,
        # the kpi_bridge family, was deleted 2026-08-19 (`eba8f5c1d`) and no
        # Python reference to it remains. Until the tolerance is retired
        # -- breaking: it invalidates persisted BMR artifacts, the canonical
        # sample business models, and the FMS write-stack goldens, all of which
        # validate today -- build-time enforcement of the both-or-neither rule
        # lives downstream in
        # `schema/build_bm_node_history.py::_validate_axis_binding`. Do not
        # delete that guard on the assumption this validator has already ruled;
        # it has not, and nothing between here and the requirement plane
        # re-checks the pair.
        if self.edgar_axis is not None and self.edgar_member is None:
            raise ValueError(
                "edgar_axis requires edgar_member (XBRL axis without member is undefined)"
            )
        if self.edgar_axis is not None and not _AXIS_QNAME_RE.match(self.edgar_axis):
            raise ValueError(
                f"edgar_axis {self.edgar_axis!r} must be an XBRL QName "
                "(e.g., srt:ProductOrServiceAxis), not a family label"
            )
        if self.absorbs:
            if self.edgar_member is not None:
                raise ValueError("absorbs requires edgar_member to be None")
            if self.edgar_axis is not None:
                raise ValueError("absorbs requires edgar_axis to be None")
            seen_names: set[str] = set()
            seen_members: set[str] = set()
            for claim in self.absorbs:
                normalized_name = _normalize_absorbed_claim_name(claim.name)
                member = str(claim.member or "").strip()
                if normalized_name and normalized_name in seen_names:
                    raise ValueError("duplicate absorbed claim within segment")
                if member and member in seen_members:
                    raise ValueError("duplicate absorbed claim within segment")
                if normalized_name:
                    seen_names.add(normalized_name)
                if member:
                    seen_members.add(member)

        # Node-level EDGAR member identity: when a blended segment absorbs several
        # members, each member-backed revenue node declares its own member so the
        # seeder can source the per-member value instead of the consolidated total.
        absorbed_members = {
            str(claim.member).strip()
            for claim in (self.absorbs or [])
            if str(claim.member or "").strip()
        }
        for node in _iter_decomposition_nodes(self.revenue_model.decomposition):
            node_member = str(node.edgar_member or "").strip()
            if node.edgar_axis is not None and not node_member:
                raise ValueError(
                    f"node {node.id!r} edgar_axis requires edgar_member"
                )
            if not node_member:
                continue
            if node.edgar_axis is None:
                raise ValueError(f"node {node.id!r} edgar_member requires edgar_axis")
            if not _AXIS_QNAME_RE.match(str(node.edgar_axis)):
                raise ValueError(
                    f"node {node.id!r} edgar_axis {node.edgar_axis!r} must be an XBRL QName "
                    "(e.g., srt:ProductOrServiceAxis), not a family label"
                )
            if not absorbed_members:
                raise ValueError(
                    f"node {node.id!r} sets edgar_member but segment {self.id!r} "
                    "does not absorb any EDGAR members"
                )
            if node_member not in absorbed_members:
                raise ValueError(
                    f"node {node.id!r} edgar_member {node_member!r} is not among segment "
                    f"{self.id!r} absorbed members {sorted(absorbed_members)}"
                )

        # Partition guard: an absorb segment must PARTITION its contract revenue, not
        # duplicate it. If more than one contract-revenue KPI node exists, every one
        # must carry an edgar_member (so each seeds from its member's slice). Two
        # untagged contract-revenue nodes both resolve the consolidated total → the 2x
        # double-count. This fails loudly at BM validation instead of building silently.
        if self.absorbs:
            contract_revenue_nodes = [
                node
                for node in _iter_decomposition_nodes(self.revenue_model.decomposition)
                if bool(node.kpi)
                and str(node.kpi_source or "").rsplit(":", 1)[-1].lower()
                in _SEGMENT_REVENUE_KPI_SOURCE_TAGS
            ]
            if len(contract_revenue_nodes) > 1:
                untagged = [
                    node.id
                    for node in contract_revenue_nodes
                    if not str(node.edgar_member or "").strip()
                ]
                if untagged:
                    raise ValueError(
                        f"absorb segment {self.id!r} has {len(contract_revenue_nodes)} "
                        f"contract-revenue nodes but {untagged} lack edgar_member; each "
                        "must declare its EDGAR member so revenue is partitioned per-member "
                        "(a shared consolidated source across nodes double-counts revenue)"
                    )
        return self


def _iter_decomposition_nodes(nodes: list["DriverNode"] | None):
    for node in nodes or []:
        yield node
        if node.children:
            yield from _iter_decomposition_nodes(node.children)


def _normalize_absorbed_claim_name(value: str | None) -> str:
    normalized = re.sub(r"[^a-z0-9]+", " ", str(value or "").strip().casefold())
    return " ".join(normalized.split())


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


_PROFITABILITY_TARGET_MARGIN_FIELDS = (
    "adjusted_ebitda_margin",
    "free_cash_flow_margin",
    "operating_margin",
)
_PROFITABILITY_TARGET_PERCENT_SHAPED_MESSAGE = (
    "looks percent-shaped (e.g. 36.0); express profitability targets as "
    "fractions of revenue (e.g. 0.36 for 36%)"
)


def normalize_legacy_profitability_targets(payload: dict) -> dict:
    profitability_targets = payload.get("profitability_targets")
    if not isinstance(profitability_targets, dict):
        return payload

    for field in _PROFITABILITY_TARGET_MARGIN_FIELDS:
        value = profitability_targets.get(field)
        if isinstance(value, (int, float)) and abs(value) > 1.0:
            profitability_targets[field] = value / 100
    return payload


class ProfitabilityTargets(_ContractModel):
    adjusted_ebitda_margin: float | None = None
    free_cash_flow_margin: float | None = None
    operating_margin: float | None = None

    @field_validator(*_PROFITABILITY_TARGET_MARGIN_FIELDS)
    @classmethod
    def _reject_percent_shaped_margin(cls, value: float | None) -> float | None:
        if value is not None and abs(value) > 1.0:
            raise ValueError(_PROFITABILITY_TARGET_PERCENT_SHAPED_MESSAGE)
        return value


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


class DeclinedSeries(_ContractModel):
    """A pinned series the model deliberately does not carry.

    The build requires every pinned operating identity series and every
    registered formula over pinned inputs to be claimed by a node's
    ``kpi_source`` or declined here with the reason. One neither claims nor
    declines is the ``business_model_node_unresolved`` warning; each row here
    is projected into ``ModelSpec.declinations`` as the Declination that
    clears it, stamped with the revision's author and time
    (`schema/build_bm_node_history.py` `declined_series_rows`,
    `api/research/model_spec_assembly.py`). A decline is a declared judgment
    downstream reads, never a refusal (Henry 2026-10-08 20:15).
    """

    series: str = Field(min_length=1)
    reason: str = Field(min_length=1)


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
    annual_capability_profile: BusinessModelCapabilityProfile | None = None
    semantic_successor_actions: tuple[
        BusinessModelSemanticSuccessorAction, ...
    ] = ()
    decisions_log: list[BmDecisionsLogEntry] = Field(default_factory=list)
    declined_series: tuple[DeclinedSeries, ...] = Field(
        default=(), exclude_if=lambda value: not value
    )
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

        declared_balances: dict[str, str] = {}
        for segment in self.segments:
            for node in _iter_decomposition_nodes(segment.revenue_model.decomposition):
                balance_role = declared_balance_role(node)
                if balance_role is None:
                    continue
                declarer = f"{segment.id}.{node.id}"
                if balance_role in declared_balances:
                    raise ValueError(
                        f"balance_of {balance_role!r} is declared by both "
                        f"{declared_balances[balance_role]!r} and {declarer!r}; "
                        "one node is that balance"
                    )
                declared_balances[balance_role] = declarer

        return self


class DriverAssumptionPlanEntry(_ContractModel):
    driver_id: DriverId
    control_id: ControlId
    binding_id: BindingId
    template_item_id: str = Field(min_length=1)
    control_unit: Unit
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
        normalized = (
            str(value or "").strip()
            for value in (*self.aliases, self.existing_driver_key)
        )
        self.aliases = list(dict.fromkeys(value for value in normalized if value))
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
        for field_name in ("driver_id", "control_id", "binding_id"):
            values = [getattr(entry, field_name) for entry in self.entries]
            if len(values) != len(set(values)):
                raise ValueError(
                    f"DriverAssumptionPlan entries require unique {field_name}"
                )

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

    @property
    def driver_ids(self) -> list[DriverId]:
        return [entry.driver_id for entry in self.entries]

    def entry_for_driver_id(self, driver_id: str) -> DriverAssumptionPlanEntry | None:
        normalized = str(driver_id or "").strip()
        for entry in self.entries:
            if entry.driver_id == normalized:
                return entry
        return None

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
    "AbsorbedClaim",
    "BmCompanyInfo",
    "BmDecisionsLogEntry",
    "BalanceRole",
    "StoredBalanceRole",
    "declared_balance_role",
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
    "DeclinedSeries",
    "DilutionPattern",
    "Dimension",
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
    "NodeMeasure",
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
    "UNIT_DIMENSION",
    "WcModelAs",
    "WorkingCapital",
    "derive_driver_assumption_plan",
    "driver_assumption_key",
    "normalize_legacy_profitability_targets",
    "node_measure",
    "product_dimension",
    "product_scale_exponent",
    "quotient_dimension",
    "unit_for_dimension",
]
