"""Ergonomic BusinessModel judgment and deterministic legacy lowering.

The contracts in this module are the agent-facing side of the BusinessModel
boundary.  They deliberately describe economics with natural keys.  Stable
identities, compiler targets, expression implementation details, and revision
metadata remain server concerns and are introduced only by
``lower_business_model_judgment``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date, datetime
import math
import re
import unicodedata
from typing import Annotated, Any, Literal

from industry_slice_core.industry_types import Unit

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    field_validator,
    model_validator,
)

from .business_model import (
    STATEMENT_ROLE_PATTERN,
    UNIT_DIMENSION,
    BalanceRole,
    BusinessModel,
    CitedObservation,
    Dimension,
    ObservationBasis,
    RevenueScope,
    business_model_observation_claim_key,
    is_statement_role,
    product_dimension,
    quotient_dimension,
)
from .business_model_semantic_claims import (
    BusinessModelSemanticClaimBinding,
    build_business_model_semantic_claim_binding,
    canonical_business_model_semantic_claim_bindings,
)


NonBlank = Annotated[str, Field(min_length=1)]


def _issue_text(value: object) -> object:
    """A keyed ``{key, text}`` issue read where the field holds bare text: its text.

    The one normalizer of the two limitation spellings. ``BusinessModelJudgment``
    keeps limitations and unresolved questions as keyed issues and accepts bare
    text there (``_keyed_issues``); every other ``limitations`` field is bare text
    and accepts the keyed form here.
    """

    if isinstance(value, Mapping) and isinstance(value.get("text"), str):
        return value["text"]
    return value


LimitationText = Annotated[NonBlank, BeforeValidator(_issue_text)]
UnitName = Literal[
    "dollars",
    "percentage",
    "ratio",
    "count",
    "per_count",
    "per_day",
    "share_count",
    "per_share",
    "days",
    "date",
    "multiple",
]
FactorRole = Literal[
    "volume",
    "price",
    "unit_economics",
    "cost_structure",
    "reinvestment",
    "capital_sources",
]
Confidence = Literal["low", "medium", "high"]
Behavior = Literal[
    "roughly_flat",
    "growing",
    "declining",
    "cyclical",
    "step_function",
]
#: The one owner of the ``source_concept`` rule: an operating series (leaf or
#: ``operating.`` role id) or a statement role id. An XBRL caption, a taxonomy
#: QName or prose is no series name. Admits every statement and operating key
#: of the required-role grammar (``coverage_profile.ANNUAL_REQUIRED_ROLE_PATTERN``;
#: an adjustment role is no KPI source) plus a bare slug, so it cannot be that
#: constant; ``test_kpi_source_pattern_admits_every_required_role_key`` holds the
#: relation. The business-model tool schema advertises this string and the
#: node-history build reads it; neither restates it.
KPI_SOURCE_PATTERN = (
    r"^operating(?:\.[a-z0-9][a-z0-9_]*)+$|^[a-z][a-z0-9_]*$|" + STATEMENT_ROLE_PATTERN
)
_KPI_SOURCE_RE = re.compile(KPI_SOURCE_PATTERN)
_XBRL_QNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*:[A-Za-z][A-Za-z0-9_-]*$")


class _JudgmentDTO(BaseModel):
    """Strict, immutable base for agent-authored economic judgment."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )

    @model_validator(mode="before")
    @classmethod
    def _freeze_json_arrays(cls, value: object) -> object:
        # Tool arguments arrive as decoded JSON, so arrays are lists.  The
        # stored authoring object is nevertheless deeply tuple-shaped.
        if isinstance(value, dict):
            return {
                key: tuple(item) if isinstance(item, list) else item
                for key, item in value.items()
            }
        return value


def _nonempty_unique(values: tuple[str, ...], *, field_name: str) -> tuple[str, ...]:
    normalized = tuple(value.strip() for value in values)
    if any(not value for value in normalized):
        raise ValueError(f"{field_name} entries must be non-blank")
    if len(normalized) != len(set(normalized)):
        raise ValueError(f"{field_name} entries must be unique")
    return normalized


class CompanyJudgment(_JudgmentDTO):
    """Company economics; ticker identity is supplied by the server."""

    name: NonBlank
    sector: NonBlank | None = None
    industry: NonBlank | None = None
    business_type: NonBlank | None = None
    rationale: NonBlank | None = None
    limitations: tuple[LimitationText, ...] = ()
    claim_keys: tuple[NonBlank, ...] = ()


class AbsorbedReportingClaim(_JudgmentDTO):
    name: NonBlank | None = None
    member: NonBlank | None = None

    @model_validator(mode="after")
    def _has_identity(self) -> "AbsorbedReportingClaim":
        if self.name is None and self.member is None:
            raise ValueError("an absorbed reporting claim requires name or member")
        return self


class SegmentReportingJudgment(_JudgmentDTO):
    """Issuer reporting semantics, not persistence/source records."""

    member: NonBlank | None = None
    axis: NonBlank | None = None
    absorbs: tuple[AbsorbedReportingClaim, ...] = ()

    @field_validator("axis")
    @classmethod
    def _axis_qname(cls, value: str | None) -> str | None:
        if value is not None and _XBRL_QNAME_RE.fullmatch(value) is None:
            raise ValueError(
                "reporting axis must be an exact XBRL QName such as "
                "srt:ProductOrServiceAxis, not a bare family label"
            )
        return value

    @model_validator(mode="after")
    def _shape(self) -> "SegmentReportingJudgment":
        if self.member is None and not self.absorbs:
            raise ValueError(
                "reporting must choose exactly one shape: member with optional axis, "
                "or a non-empty absorbs list"
            )
        if self.axis is not None and self.member is None:
            raise ValueError(
                "mapped reporting axis requires member; omit axis/member entirely "
                "when using absorbs"
            )
        if self.absorbs and (self.member is not None or self.axis is not None):
            raise ValueError(
                "reporting is an exclusive union: use member with optional axis for "
                "one mapped member, or use absorbs alone and include every blended "
                "member (including the primary member)"
            )
        return self


class ReportedKpiJudgment(_JudgmentDTO):
    """Metadata needed to locate a reported KPI; no source record is authored."""

    source_concept: NonBlank
    frequency: Literal["quarterly", "annual", "ad_hoc"] | None = None
    member: NonBlank | None = None
    axis: NonBlank | None = None
    claim_keys: tuple[NonBlank, ...] = ()

    @field_validator("source_concept")
    @classmethod
    def _tag_like_source_concept(cls, value: str) -> str:
        if _KPI_SOURCE_RE.fullmatch(value) is None:
            raise ValueError(
                "source_concept must be an operating series or statement role id, "
                "not a caption, QName or prose"
            )
        return value

    @field_validator("axis")
    @classmethod
    def _axis_qname(cls, value: str | None) -> str | None:
        if value is not None and _XBRL_QNAME_RE.fullmatch(value) is None:
            raise ValueError(
                "reported KPI axis must be an exact XBRL QName such as "
                "srt:ProductOrServiceAxis, not a bare family label"
            )
        return value

    @model_validator(mode="after")
    def _dimension_pair(self) -> "ReportedKpiJudgment":
        if (self.member is None) != (self.axis is None):
            raise ValueError("reported KPI member and axis must be supplied together")
        return self


class GrowthCalculation(_JudgmentDTO):
    kind: Literal["growth"] = "growth"


class ExternalCalculation(_JudgmentDTO):
    kind: Literal["external"] = "external"
    source_category: Literal["macro", "market_data", "manual"]
    description: NonBlank


def split_signed_operand(raw: str) -> tuple[int, str]:
    """A sum input's sign and reference: a leading ``-`` subtracts the operand."""

    text = raw.strip()
    if text.startswith("-"):
        return -1, text[1:].strip()
    return 1, text


class SumCalculation(_JudgmentDTO):
    """A sum over sibling drivers and/or ``statement.*`` roles; ``-ref`` subtracts.

    Every driver operand shares the sum's unit. A statement role is read from
    the pinned statement series, so a node that is the remainder of a statement
    total over its siblings states it as ``[statement.<...>, -<sibling>, ...]``.
    """

    kind: Literal["sum"] = "sum"
    inputs: tuple[NonBlank, ...] = Field(min_length=1)

    @field_validator("inputs")
    @classmethod
    def _unique_inputs(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        inputs = _nonempty_unique(value, field_name="sum inputs")
        references = tuple(split_signed_operand(raw)[1] for raw in inputs)
        if any(not reference for reference in references):
            raise ValueError("sum inputs entries must name a driver or statement role after '-'")
        if len(references) != len(set(references)):
            raise ValueError("sum inputs entries must be unique, signed or not")
        return inputs


class ProductCalculation(_JudgmentDTO):
    kind: Literal["product"] = "product"
    inputs: tuple[NonBlank, ...] = Field(min_length=2)

    @field_validator("inputs")
    @classmethod
    def _unique_inputs(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _nonempty_unique(value, field_name="product inputs")


class RatioCalculation(_JudgmentDTO):
    kind: Literal["ratio"] = "ratio"
    numerator: NonBlank
    denominator: NonBlank

    @model_validator(mode="after")
    def _different_inputs(self) -> "RatioCalculation":
        if self.numerator == self.denominator:
            raise ValueError("ratio numerator and denominator must differ")
        return self


class RollForwardCalculation(_JudgmentDTO):
    kind: Literal["roll_forward"] = "roll_forward"
    beginning: Literal["prior_period"] = "prior_period"
    additions: tuple[NonBlank, ...] = ()
    subtractions: tuple[NonBlank, ...] = ()

    @model_validator(mode="after")
    def _movement(self) -> "RollForwardCalculation":
        additions = _nonempty_unique(self.additions, field_name="roll-forward additions")
        subtractions = _nonempty_unique(
            self.subtractions, field_name="roll-forward subtractions"
        )
        overlap = set(additions) & set(subtractions)
        if overlap:
            raise ValueError(
                f"roll-forward inputs cannot be both additions and subtractions: {sorted(overlap)}"
            )
        if not additions and not subtractions:
            raise ValueError("roll-forward requires at least one movement input")
        return self


class CommentaryCalculation(_JudgmentDTO):
    kind: Literal["commentary"] = "commentary"
    observation: NonBlank


CalculationJudgment = Annotated[
    GrowthCalculation
    | ExternalCalculation
    | SumCalculation
    | ProductCalculation
    | RatioCalculation
    | RollForwardCalculation
    | CommentaryCalculation,
    Field(discriminator="kind"),
]


class DriverJudgment(_JudgmentDTO):
    """One recursively nestable economic driver identified by a natural key."""

    key: NonBlank
    label: NonBlank | None = None
    factor_roles: tuple[FactorRole, ...] = Field(min_length=1)
    unit: UnitName
    calculation: CalculationJudgment
    behavior: Behavior | None = None
    observation_basis: ObservationBasis | None = None
    revenue_scope: RevenueScope | None = None
    balance_of: BalanceRole | None = None
    reported_kpi: ReportedKpiJudgment | None = None
    target_observation: NonBlank | None = None
    note: NonBlank | None = None
    rationale: NonBlank | None = None
    limitations: tuple[LimitationText, ...] = ()
    claim_keys: tuple[NonBlank, ...] = ()
    historical_observations: tuple[CitedObservation, ...] = ()
    children: tuple["DriverJudgment", ...] = ()
    children_role: Literal["decomposition", "tracking"] | None = None

    @model_validator(mode="after")
    def _local_shape(self) -> "DriverJudgment":
        if len(self.factor_roles) != len(set(self.factor_roles)):
            raise ValueError("factor_roles must be unique")
        if (
            isinstance(self.calculation, GrowthCalculation)
            and self.observation_basis is None
        ):
            raise ValueError(
                f"growth driver {self.key!r} requires observation_basis: use "
                "period_end for a stock, period_flow for a period flow, or "
                "period_average for an average; do not supply a numeric seed"
            )
        if self.children and self.children_role is None:
            raise ValueError("children_role is required when children are present")
        if not self.children and self.children_role is not None:
            raise ValueError("children_role is only valid when children are present")
        if self.children_role == "decomposition" and self.calculation.kind not in {
            "sum",
            "product",
        }:
            raise ValueError("decomposition children require a sum or product calculation")
        if self.children_role == "tracking" and self.calculation.kind == "commentary":
            raise ValueError("a tracking parent must have a materialized calculation")
        return self


class GrossMarginJudgment(_JudgmentDTO):
    behavior: Literal["expanding", "stable", "compressing"]
    observed_pct: float | None = Field(default=None, ge=-1.0, le=1.0, allow_inf_nan=False)
    target_observation: NonBlank | None = None
    drivers: tuple[NonBlank, ...] = ()


class UnitEconomicsJudgment(_JudgmentDTO):
    gross_margin: GrossMarginJudgment
    unit_definition: NonBlank | None = None
    scale_dynamics: NonBlank | None = None
    rationale: NonBlank | None = None
    limitations: tuple[LimitationText, ...] = ()
    claim_keys: tuple[NonBlank, ...] = ()


class CostJudgment(_JudgmentDTO):
    key: NonBlank
    label: NonBlank | None = None
    category: Literal[
        "sales_and_marketing",
        "research_and_development",
        "general_and_administrative",
        "cost_of_revenue",
        "other",
    ] = "other"
    model_as: Literal[
        "percent_of_revenue", "fixed_amount", "per_unit", "step_function"
    ]
    leverage: bool
    target_observation: NonBlank | None = None
    note: NonBlank | None = None
    rationale: NonBlank | None = None
    limitations: tuple[LimitationText, ...] = ()
    claim_keys: tuple[NonBlank, ...] = ()


class CapexJudgment(_JudgmentDTO):
    model_as: Literal["percent_of_revenue", "absolute_growth", "per_unit"]
    intensity: Literal["low", "moderate", "high"]
    components: tuple[NonBlank, ...] = ()
    observed_depreciation_rate: float | None = Field(
        default=None, ge=0.0, le=1.0, allow_inf_nan=False
    )
    observed_amortization_rate: float | None = Field(
        default=None, ge=0.0, le=1.0, allow_inf_nan=False
    )


class WorkingCapitalJudgment(_JudgmentDTO):
    model_as: Literal["percent_of_revenue", "custom"]
    intensity: Literal["low", "moderate", "high"]
    driver_metrics: tuple[
        Literal["days_sales_outstanding", "days_payable_outstanding"], ...
    ] = ()
    notes: tuple[NonBlank, ...] = ()

    @field_validator("driver_metrics")
    @classmethod
    def _unique_metrics(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)):
            raise ValueError("driver_metrics must be unique")
        return value


class AcquisitionsJudgment(_JudgmentDTO):
    pattern: Literal["none", "opportunistic", "programmatic"]
    note: NonBlank | None = None


class ReinvestmentJudgment(_JudgmentDTO):
    capex: CapexJudgment
    working_capital: WorkingCapitalJudgment | None = None
    acquisitions: AcquisitionsJudgment | None = None
    rationale: NonBlank | None = None
    limitations: tuple[LimitationText, ...] = ()
    claim_keys: tuple[NonBlank, ...] = ()


class DebtJudgment(_JudgmentDTO):
    level: Literal["none", "low", "moderate", "high"]
    target_metric: NonBlank | None = None


class StockCompensationJudgment(_JudgmentDTO):
    model_as: Literal["percent_of_opex", "percent_of_revenue"] | None = None
    offset: NonBlank | None = None


class EquityJudgment(_JudgmentDTO):
    dilution_pattern: Literal["minimal", "moderate", "significant"]
    stock_compensation: StockCompensationJudgment | None = None


class CapitalReturnJudgment(_JudgmentDTO):
    priority: Literal["reinvestment", "balanced", "return_focused"]
    mechanisms: tuple[NonBlank, ...] = ()


class CapitalSourcesJudgment(_JudgmentDTO):
    funding_model: Literal[
        "self_funded", "debt_funded", "equity_dependent", "blended"
    ]
    debt: DebtJudgment | None = None
    equity: EquityJudgment | None = None
    capital_return: CapitalReturnJudgment | None = None
    rationale: NonBlank | None = None
    limitations: tuple[LimitationText, ...] = ()
    claim_keys: tuple[NonBlank, ...] = ()


class TargetObservation(_JudgmentDTO):
    """A reported or guided target, not a forecast assumption."""

    key: NonBlank
    metric: NonBlank
    value: float | NonBlank
    unit: UnitName | None = None
    horizon: NonBlank | None = None
    rationale: NonBlank | None = None
    claim_keys: tuple[NonBlank, ...] = ()

    @field_validator("value")
    @classmethod
    def _finite_value(cls, value: float | str) -> float | str:
        if isinstance(value, float) and not math.isfinite(value):
            raise ValueError("target observation value must be finite")
        return value

    @model_validator(mode="after")
    def _percentage_shape(self) -> "TargetObservation":
        if self.unit == "percentage" and isinstance(self.value, float):
            if abs(self.value) > 1.0:
                raise ValueError(
                    "percentage observations use fractions (for example 0.36 for 36%)"
                )
        return self


class BusinessModelJudgmentIssue(_JudgmentDTO):
    """One natural-keyed limitation or unresolved question for stage handoff."""

    key: NonBlank
    text: NonBlank
    claim_keys: tuple[NonBlank, ...] = ()

    @field_validator("key", mode="before")
    @classmethod
    def _canonical_key(cls, value: object) -> str:
        if not isinstance(value, str):
            raise ValueError("issue key must be a string")
        return semantic_slug(value, kind="issue")


class DeclinedSeriesJudgment(_JudgmentDTO):
    """A pinned disclosed series this model deliberately does not model.

    ``series`` is a ``reported_kpi.source_concept`` name; the build requires
    every series it pins for the ticker to be claimed by a driver or declined
    here, and refuses a judgment that does neither.
    """

    series: NonBlank
    reason: NonBlank


class SegmentJudgment(_JudgmentDTO):
    key: NonBlank
    label: NonBlank | None = None
    issuer_match_name: NonBlank | None = None
    is_default: bool
    revenue_share: float = Field(ge=0.0, le=1.0, allow_inf_nan=False)
    revenue_model: Literal[
        "volume_x_price", "market_based", "product_based", "blended"
    ]
    drivers: tuple[DriverJudgment, ...] = Field(min_length=1)
    revenue_outputs: tuple[NonBlank, ...] = Field(min_length=1)
    reporting: SegmentReportingJudgment | None = None
    unit_economics: UnitEconomicsJudgment | None = None
    costs: tuple[CostJudgment, ...] = ()
    rationale: NonBlank | None = None
    limitations: tuple[LimitationText, ...] = ()
    claim_keys: tuple[NonBlank, ...] = ()

    @field_validator("revenue_outputs")
    @classmethod
    def _unique_outputs(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _nonempty_unique(value, field_name="revenue_outputs")

    @model_validator(mode="after")
    def _reporting_anchor_ownership(self) -> "SegmentJudgment":
        if self.reporting is None or self.reporting.member is None:
            return self

        def visit(nodes: tuple[DriverJudgment, ...]) -> None:
            for node in nodes:
                if node.reported_kpi is not None and (
                    node.reported_kpi.member is not None
                    or node.reported_kpi.axis is not None
                ):
                    raise ValueError(
                        "mapped segment reporting owns the XBRL member/axis anchor; "
                        "omit reported_kpi.member and reported_kpi.axis on drivers. "
                        "Node dimensions are only valid inside an absorbs segment"
                    )
                visit(node.children)

        visit(self.drivers)
        return self


class BusinessModelJudgment(_JudgmentDTO):
    """Compact agent-authored business-model judgment."""

    company: CompanyJudgment
    segments: tuple[SegmentJudgment, ...] = Field(min_length=1)
    costs: tuple[CostJudgment, ...] = ()
    reinvestment: ReinvestmentJudgment | None = None
    capital_sources: CapitalSourcesJudgment | None = None
    targets: tuple[TargetObservation, ...] = ()
    recommended_depth: Literal["quick_and_dirty", "advanced"]
    change_triggers: tuple[NonBlank, ...] = ()
    review_basis: NonBlank | None = None
    rationale: NonBlank
    limitations: tuple[BusinessModelJudgmentIssue, ...] = ()
    unresolved_questions: tuple[BusinessModelJudgmentIssue, ...] = ()
    declined_series: tuple[DeclinedSeriesJudgment, ...] = ()
    claim_keys: tuple[NonBlank, ...] = ()
    confidence: Confidence

    @model_validator(mode="before")
    @classmethod
    def _keyed_issues(cls, value: object) -> object:
        """Bare text in ``limitations`` / ``unresolved_questions`` is an issue keyed by its words.

        The reverse of ``_issue_text``: the other ``limitations`` fields are bare text,
        so an author writes bare text here too. A key is the slug of the text's first
        words, numbered past one already taken.
        """

        if not isinstance(value, Mapping):
            return value
        fields = ("limitations", "unresolved_questions")
        if not any(
            isinstance(item, str)
            for field_name in fields
            if isinstance(value.get(field_name), (list, tuple))
            for item in value[field_name]
        ):
            return value
        taken: set[str] = set()
        for field_name in fields:
            for item in value.get(field_name) or ():
                if isinstance(item, Mapping) and isinstance(item.get("key"), str):
                    try:
                        taken.add(semantic_slug(item["key"], kind="issue"))
                    except ValueError:
                        pass
        normalized = dict(value)
        for field_name in fields:
            items = value.get(field_name)
            if not isinstance(items, (list, tuple)):
                continue
            rows: list[object] = []
            for item in items:
                if isinstance(item, str) and item.strip():
                    base = semantic_slug(" ".join(item.split()[:6])[:60], kind="issue")
                    key, ordinal = base, 2
                    while key in taken:
                        key, ordinal = f"{base}_{ordinal}", ordinal + 1
                    taken.add(key)
                    rows.append({"key": key, "text": item})
                else:
                    rows.append(item)
            normalized[field_name] = tuple(rows)
        return normalized

    @model_validator(mode="after")
    def _unique_issue_keys(self) -> "BusinessModelJudgment":
        keys = [
            issue.key
            for issue in (*self.limitations, *self.unresolved_questions)
        ]
        if len(keys) != len(set(keys)):
            raise ValueError(
                "top-level limitation and unresolved-question keys must be unique "
                "after canonicalization"
            )
        _require_unique_control_cost_categories(self.costs, owner="consolidated")
        for segment in self.segments:
            _require_unique_control_cost_categories(
                segment.costs,
                owner=f"segment {segment.key!r}",
            )
        _validate_driver_semantic_intent(self)
        return self


def semantic_slug(value: str, *, kind: str = "key") -> str:
    """Return the deterministic canonical identifier for a natural semantic key."""

    text = unicodedata.normalize("NFKD", str(value or "").strip())
    text = "".join(character for character in text if not unicodedata.combining(character))
    text = text.casefold().replace("&", " and ")
    slug = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    if not slug:
        raise ValueError(f"{kind} must contain at least one letter or number")
    if slug[0].isdigit():
        slug = f"{kind}_{slug}"
    if slug == "self":
        raise ValueError(f"{kind} {value!r} resolves to reserved key 'self'")
    return slug


def _label(value: str, explicit: str | None) -> str:
    if explicit is not None:
        return explicit
    words = re.sub(r"[_-]+", " ", value).strip()
    return " ".join(word if word.isupper() else word.capitalize() for word in words.split())


def _normalized_server_time(value: date | datetime | str) -> tuple[str, str]:
    if isinstance(value, datetime):
        rendered = value.isoformat()
        return rendered, value.date().isoformat()
    if isinstance(value, date):
        rendered = value.isoformat()
        return rendered, rendered
    rendered = str(value or "").strip()
    if not rendered:
        raise ValueError("as_of is required")
    try:
        if "T" in rendered or " " in rendered:
            parsed = datetime.fromisoformat(rendered.replace("Z", "+00:00"))
            return parsed.isoformat(), parsed.date().isoformat()
        parsed_date = date.fromisoformat(rendered)
        return parsed_date.isoformat(), parsed_date.isoformat()
    except ValueError as exc:
        raise ValueError("as_of must be an ISO date or datetime") from exc


class _NodeIndex:
    def __init__(self, segment: SegmentJudgment, all_segments: dict[str, str]) -> None:
        self.segment = segment
        self.segment_key = semantic_slug(segment.key, kind="segment")
        self.all_segments = all_segments
        self.nodes: dict[str, DriverJudgment] = {}
        self.top_level: set[str] = set()
        self.aliases: dict[str, set[str]] = {}
        self.children: dict[str, set[str]] = {}
        for node in segment.drivers:
            node_key = self._register(node)
            self.top_level.add(node_key)

    def _register(self, node: DriverJudgment) -> str:
        key = semantic_slug(node.key, kind="driver")
        if key in self.nodes:
            raise ValueError(
                f"segment {self.segment.key!r} has duplicate/slug-colliding driver key {key!r}"
            )
        self.nodes[key] = node
        for alias in {key, semantic_slug(_label(node.key, node.label), kind="driver")}:
            self.aliases.setdefault(alias, set()).add(key)
        child_keys: set[str] = set()
        for child in node.children:
            child_key = self._register(child)
            child_keys.add(child_key)
        self.children[key] = child_keys
        return key

    def resolve_operand(self, reference: str) -> str:
        """A calculation operand: a sibling driver's key, or a statement role id unchanged."""

        if is_statement_role(str(reference or "").strip()):
            return str(reference).strip()
        return self.resolve(reference)

    def resolve(self, reference: str) -> str:
        raw = str(reference or "").strip()
        for separator in (".", "/", ":"):
            if separator not in raw:
                continue
            qualifier, candidate = raw.split(separator, 1)
            qualifier_key = semantic_slug(qualifier, kind="segment")
            if qualifier_key not in self.all_segments:
                continue
            if qualifier_key != self.segment_key:
                raise ValueError(
                    f"cross-segment driver reference {reference!r} is not allowed; "
                    "relationships must remain within one segment"
                )
            raw = candidate
            break
        candidate_key = semantic_slug(raw, kind="driver")
        if candidate_key in self.nodes:
            return candidate_key
        matches = self.aliases.get(candidate_key, set())
        if not matches:
            raise ValueError(
                f"segment {self.segment.key!r} references unknown driver {reference!r}"
            )
        if len(matches) != 1:
            raise ValueError(
                f"segment {self.segment.key!r} has ambiguous driver reference {reference!r}: "
                f"{sorted(matches)}"
            )
        return next(iter(matches))


def _calculation_refs(calculation: CalculationJudgment) -> tuple[str, ...]:
    if isinstance(calculation, SumCalculation):
        return tuple(split_signed_operand(raw)[1] for raw in calculation.inputs)
    if isinstance(calculation, ProductCalculation):
        return calculation.inputs
    if isinstance(calculation, RatioCalculation):
        return (calculation.numerator, calculation.denominator)
    if isinstance(calculation, RollForwardCalculation):
        return (*calculation.additions, *calculation.subtractions)
    return ()


def _reads_statement_role(calculation: CalculationJudgment) -> bool:
    return any(is_statement_role(ref) for ref in _calculation_refs(calculation))


def _node_dependencies(resolved: tuple[str, ...]) -> tuple[str, ...]:
    """The resolved operands that are sibling drivers (a statement role is no driver)."""

    return tuple(ref for ref in resolved if not is_statement_role(ref))


def _validate_unit_relationship(
    *,
    node_key: str,
    node: DriverJudgment,
    resolved_refs: tuple[str, ...],
    index: _NodeIndex,
) -> None:
    calculation = node.calculation
    output = node.unit
    input_units = tuple(index.nodes[ref].unit for ref in resolved_refs)
    if isinstance(calculation, SumCalculation):
        if any(unit != output for unit in input_units):
            raise ValueError(
                f"sum driver {node_key!r} unit {output!r} must match every input unit "
                f"{input_units!r}"
            )
        return
    if isinstance(calculation, ProductCalculation):
        dimensions: tuple[Dimension | None, ...] = tuple(
            UNIT_DIMENSION.get(Unit(unit)) for unit in input_units
        )
        known_dimensions: list[Dimension] = [
            dimension for dimension in dimensions if dimension is not None
        ]
        inferred = (
            product_dimension(known_dimensions)
            if all(dimension is not None for dimension in dimensions)
            else None
        )
        if inferred is None:
            raise ValueError(
                f"product driver {node_key!r} has incompatible input units {input_units!r}"
            )
        if UNIT_DIMENSION.get(Unit(output)) != inferred:
            raise ValueError(
                f"product driver {node_key!r} output unit {output!r} is incompatible "
                f"with input units {input_units!r}"
            )
        return
    if isinstance(calculation, RatioCalculation):
        numerator_unit, denominator_unit = input_units
        numerator_dimension = UNIT_DIMENSION.get(Unit(numerator_unit))
        denominator_dimension = UNIT_DIMENSION.get(Unit(denominator_unit))
        inferred = (
            quotient_dimension(numerator_dimension, denominator_dimension)
            if numerator_dimension is not None and denominator_dimension is not None
            else None
        )
        if inferred is None or UNIT_DIMENSION.get(Unit(output)) != inferred:
            raise ValueError(
                f"ratio driver {node_key!r} output unit {output!r} is incompatible "
                f"with {numerator_unit!r}/{denominator_unit!r}"
            )
        return
    if isinstance(calculation, RollForwardCalculation):
        if any(unit != output for unit in input_units):
            raise ValueError(
                f"roll-forward driver {node_key!r} unit {output!r} must match every "
                f"movement input unit {input_units!r}"
            )
    if isinstance(calculation, GrowthCalculation) and output == "date":
        raise ValueError("growth calculations cannot produce date units")


def _validate_segment_graph(index: _NodeIndex) -> dict[str, tuple[str, ...]]:
    edges: dict[str, tuple[str, ...]] = {}
    for node_key, node in index.nodes.items():
        if _reads_statement_role(node.calculation) and not isinstance(
            node.calculation, SumCalculation
        ):
            raise ValueError(
                f"{node.calculation.kind} driver {node_key!r} reads a statement role; "
                "only a sum calculation takes a statement role operand"
            )
        resolved = tuple(
            index.resolve_operand(ref) for ref in _calculation_refs(node.calculation)
        )
        if len(resolved) != len(set(resolved)):
            raise ValueError(f"driver {node_key!r} contains duplicate resolved inputs")
        if node.children_role == "decomposition":
            expected = index.children[node_key]
            if set(resolved) != expected:
                raise ValueError(
                    f"decomposition driver {node_key!r} must use every direct child exactly once; "
                    f"expected {sorted(expected)}, got {sorted(resolved)}"
                )
        dependencies = _node_dependencies(resolved)
        _validate_unit_relationship(
            node_key=node_key,
            node=node,
            resolved_refs=dependencies,
            index=index,
        )
        edges[node_key] = dependencies

    visiting: set[str] = set()
    visited: set[str] = set()

    def visit(key: str, path: tuple[str, ...]) -> None:
        if key in visiting:
            cycle_start = path.index(key) if key in path else 0
            cycle = (*path[cycle_start:], key)
            raise ValueError(
                f"cyclic driver relationship: {' -> '.join(cycle)}. Driver calculations "
                "must be acyclic; do not define a derived revenue-per-unit ratio from a "
                "revenue output that multiplies by that same driver. Use an external/manual "
                "historical residual instead"
            )
        if key in visited:
            return
        visiting.add(key)
        for dependency in edges[key]:
            visit(dependency, (*path, key))
        visiting.remove(key)
        visited.add(key)

    for key in edges:
        visit(key, ())
    return edges



def structural_lowering_errors(judgment: BusinessModelJudgment) -> tuple[str, ...]:
    """Collect all segment/driver structural lowering errors in a single pass."""

    errors: list[str] = []
    segment_keys: dict[str, str] = {}
    for segment in judgment.segments:
        key = semantic_slug(segment.key, kind="segment")
        if key in segment_keys:
            errors.append(
                f"duplicate/slug-colliding segment key {key!r}: "
                f"{segment_keys[key]!r} and {segment.key!r}"
            )
        else:
            segment_keys[key] = segment.key
    for segment in judgment.segments:
        try:
            index = _NodeIndex(segment, segment_keys)
        except ValueError as exc:
            errors.append(str(exc))
            continue
        edges: dict[str, tuple[str, ...]] = {}
        for node_key, node in index.nodes.items():
            if _reads_statement_role(node.calculation) and not isinstance(
                node.calculation, SumCalculation
            ):
                errors.append(
                    f"{node.calculation.kind} driver {node_key!r} reads a statement role; "
                    "only a sum calculation takes a statement role operand"
                )
            resolved_list: list[str] = []
            node_failed = False
            for ref in _calculation_refs(node.calculation):
                try:
                    resolved_list.append(index.resolve_operand(ref))
                except ValueError as exc:
                    errors.append(str(exc))
                    node_failed = True
            if node_failed:
                continue
            resolved = tuple(resolved_list)
            if len(resolved) != len(set(resolved)):
                errors.append(f"driver {node_key!r} contains duplicate resolved inputs")
                continue
            if node.children_role == "decomposition":
                expected = index.children[node_key]
                if set(resolved) != expected:
                    errors.append(
                        f"decomposition driver {node_key!r} must use every direct child exactly once; "
                        f"expected {sorted(expected)}, got {sorted(resolved)}"
                    )
                    continue
            dependencies = _node_dependencies(resolved)
            try:
                _validate_unit_relationship(
                    node_key=node_key,
                    node=node,
                    resolved_refs=dependencies,
                    index=index,
                )
            except ValueError as exc:
                errors.append(str(exc))
                continue
            edges[node_key] = dependencies
        visiting: set[str] = set()
        visited: set[str] = set()

        def visit(key: str, path: tuple[str, ...]) -> None:
            if key in visiting:
                cycle_start = path.index(key) if key in path else 0
                cycle = (*path[cycle_start:], key)
                errors.append(
                    f"cyclic driver relationship: {' -> '.join(cycle)}. Driver calculations "
                    "must be acyclic; do not define a derived revenue-per-unit ratio from a "
                    "revenue output that multiplies by that same driver. Use an external/manual "
                    "historical residual instead"
                )
                return
            if key in visited:
                return
            visiting.add(key)
            for dependency in edges.get(key, ()):
                visit(dependency, (*path, key))
            visiting.remove(key)
            visited.add(key)

        for key in edges:
            visit(key, ())
        output_keys: list[str] = []
        for output in segment.revenue_outputs:
            try:
                output_keys.append(index.resolve(output))
            except ValueError as exc:
                errors.append(str(exc))
        if len(output_keys) != len(set(output_keys)):
            errors.append(f"segment {segment.key!r} has duplicate revenue outputs")
        if any(output not in index.top_level for output in output_keys):
            errors.append(
                f"segment {segment.key!r} revenue_outputs must reference top-level drivers"
            )
        for output in output_keys:
            output_node = index.nodes[output]
            if output_node.unit != "dollars":
                errors.append(
                    f"revenue output {output!r} must use dollars, got {output_node.unit!r}"
                )
            if isinstance(output_node.calculation, CommentaryCalculation):
                errors.append(f"revenue output {output!r} cannot be commentary")
    return tuple(dict.fromkeys(errors))

def names_period_average(key: object, label: object, description: object = None) -> bool:
    """Whether a driver's own words (key, label, external description) name an average.

    The one reading of the period-average rule: the semantic validator requires
    such a driver to observe ``period_average``, and the BusinessModel door reads
    the basis from the same words when the judgment omits it.
    """

    semantic_name = " ".join(
        part for part in (key, label, description) if isinstance(part, str) and part
    )
    return re.search(r"\baverag(?:e|ed|ing)\b", semantic_name, flags=re.I) is not None


def _validate_driver_semantic_intent(judgment: BusinessModelJudgment) -> None:
    """Require explicit nonnumeric intent only where source semantics can drift."""

    segment_keys: dict[str, str] = {}
    for segment in judgment.segments:
        key = semantic_slug(segment.key, kind="segment")
        if key in segment_keys:
            return
        segment_keys[key] = segment.key
    indexes: list[
        tuple[SegmentJudgment, _NodeIndex, dict[str, tuple[str, ...]], tuple[str, ...]]
    ] = []
    stream_count = 0
    for segment in judgment.segments:
        try:
            index = _NodeIndex(segment, segment_keys)
            edges = {
                node_key: _node_dependencies(
                    tuple(
                        index.resolve_operand(ref)
                        for ref in _calculation_refs(node.calculation)
                    )
                )
                for node_key, node in index.nodes.items()
            }
            outputs = tuple(index.resolve(output) for output in segment.revenue_outputs)
        except ValueError:
            # Structural graph errors retain their existing lowering-time repair
            # paths; this validator owns only semantic intent.
            return
        indexes.append((segment, index, edges, outputs))
        stream_count += len(outputs)

        for node in index.nodes.values():
            if (
                names_period_average(
                    node.key,
                    node.label,
                    node.calculation.description
                    if isinstance(node.calculation, ExternalCalculation)
                    else None,
                )
                and node.observation_basis != "period_average"
            ):
                raise ValueError(
                    f"period-average driver {node.key!r} requires "
                    "observation_basis='period_average'; a period_end observation "
                    "is an ending stock, not an average"
                )

    if stream_count <= 1:
        return

    for segment, index, edges, outputs in indexes:
        reachable: set[str] = set()

        def visit(key: str) -> None:
            if key in reachable:
                return
            reachable.add(key)
            for dependency in edges[key]:
                visit(dependency)

        for output in outputs:
            visit(output)

        for parent_key in reachable:
            parent = index.nodes[parent_key]
            if not isinstance(parent.calculation, ProductCalculation):
                continue
            inputs = edges[parent_key]
            if not any(index.nodes[key].unit == "count" for key in inputs):
                continue
            for input_key in inputs:
                node = index.nodes[input_key]
                if (
                    node.unit not in ("dollars", "per_count", "per_day")
                    or "price" not in node.factor_roles
                ):
                    continue
                if node.revenue_scope != "own_revenue_stream_only":
                    raise ValueError(
                        f"revenue-per-unit driver {node.key!r} in segment "
                        f"{segment.key!r} requires "
                        "revenue_scope='own_revenue_stream_only' when other revenue "
                        "streams are modeled; consolidated revenue would double count "
                        "those separate streams"
                    )


def _calculation_expr(
    calculation: CalculationJudgment,
    *,
    node_key: str,
    index: _NodeIndex,
) -> dict[str, Any] | None:
    if isinstance(calculation, CommentaryCalculation):
        return None
    if isinstance(calculation, GrowthCalculation):
        return {
            "type": "growth",
            "params": {
                "base": {"node_id": "self", "t": -1, "sign": 1},
                "rate_key": f"{node_key}_growth",
            },
        }
    if isinstance(calculation, ExternalCalculation):
        return {
            "type": "external",
            "params": {
                "source_category": calculation.source_category,
                "description": calculation.description,
            },
        }
    if isinstance(calculation, SumCalculation):
        signed = [split_signed_operand(raw) for raw in calculation.inputs]
        return {
            "type": "sum",
            "params": {
                "operands": [
                    {"node_id": index.resolve_operand(ref), "t": 0, "sign": sign}
                    for sign, ref in signed
                ]
            },
        }
    if isinstance(calculation, ProductCalculation):
        return {
            "type": "product",
            "params": {
                "operands": [
                    {"node_id": index.resolve(ref), "t": 0, "sign": 1}
                    for ref in calculation.inputs
                ]
            },
        }
    if isinstance(calculation, RatioCalculation):
        return {
            "type": "derived",
            "params": {
                "numerator": {
                    "node_id": index.resolve(calculation.numerator),
                    "t": 0,
                    "sign": 1,
                },
                "denominator": {
                    "node_id": index.resolve(calculation.denominator),
                    "t": 0,
                    "sign": 1,
                },
            },
        }
    # CalculationJudgment is the closed union this dispatch covers; the branches
    # above narrow it to RollForwardCalculation, so a new union member is a type
    # error here, not a runtime refusal.
    return {
        "type": "roll_forward",
        "params": {
            "beginning": {"node_id": "self", "t": -1, "sign": 1},
            "additions": [
                {"node_id": index.resolve(ref), "t": 0, "sign": 1}
                for ref in calculation.additions
            ],
            "subtractions": [
                {"node_id": index.resolve(ref), "t": 0, "sign": 1}
                for ref in calculation.subtractions
            ],
        },
    }


def _render_note(
    *,
    observation: str | None = None,
    note: str | None = None,
    rationale: str | None = None,
    limitations: tuple[str, ...] = (),
    claim_keys: tuple[str, ...] = (),
) -> str | None:
    parts: list[str] = []
    if observation:
        parts.append(f"Observation: {observation}")
    if note:
        parts.append(note)
    if rationale:
        parts.append(f"Rationale: {rationale}")
    if limitations:
        parts.append(f"Limitations: {'; '.join(limitations)}")
    if claim_keys:
        parts.append(f"Claims: {', '.join(claim_keys)}")
    return " | ".join(parts) or None


def _target_text(target: TargetObservation) -> str:
    value = repr(target.value) if isinstance(target.value, float) else target.value
    parts = [f"{target.metric}: {value}"]
    if target.unit:
        parts.append(target.unit)
    if target.horizon:
        parts.append(f"horizon={target.horizon}")
    if target.rationale:
        parts.append(f"rationale={target.rationale}")
    if target.claim_keys:
        parts.append(f"claims={','.join(target.claim_keys)}")
    return "; ".join(parts)


def _target_fraction(target: TargetObservation, *, field: str) -> float:
    if target.unit != "percentage" or not isinstance(target.value, float):
        raise ValueError(
            f"{field} requires a numeric percentage target observation expressed as a fraction"
        )
    return target.value


def _target_map(judgment: BusinessModelJudgment) -> dict[str, TargetObservation]:
    targets: dict[str, TargetObservation] = {}
    for target in judgment.targets:
        key = semantic_slug(target.key, kind="target")
        if key in targets:
            raise ValueError(f"duplicate/slug-colliding target observation key {key!r}")
        targets[key] = target
    return targets


def _linked_target(
    value: str | None,
    targets: dict[str, TargetObservation],
) -> TargetObservation | None:
    """The declared target a ``target_observation`` link names, or ``None``.

    A link naming no declared target (a statement role such as
    ``statement.is.gross_profit``, qualitative text) stores no target: the
    judgment stands and ``unresolved_target_links`` reports the link.
    """

    if value is None:
        return None
    return targets.get(semantic_slug(value, kind="target"))


@dataclass(frozen=True)
class UnresolvedTargetLink:
    """One ``target_observation`` link that names no declared target."""

    field: str
    link: str
    declared: tuple[str, ...]

    @property
    def message(self) -> str:
        return (
            f"{self.field} links target observation {self.link!r}, which is not a "
            f"declared target ({list(self.declared)!r}); the link was not stored and "
            "the judgment stands. Declare the target in targets and link its key, or "
            "put the commentary in note or rationale"
        )


def unresolved_target_links(
    judgment: BusinessModelJudgment,
) -> tuple[UnresolvedTargetLink, ...]:
    """Every ``target_observation`` link of the judgment that resolves to no declared target."""

    targets = _target_map(judgment)
    declared = tuple(sorted(targets))
    rows: list[UnresolvedTargetLink] = []

    def check(field: str, link: str | None) -> None:
        if link is not None and _linked_target(link, targets) is None:
            rows.append(UnresolvedTargetLink(field=field, link=link, declared=declared))

    def visit(prefix: str, nodes: tuple[DriverJudgment, ...]) -> None:
        for node in nodes:
            check(f"{prefix}.drivers[{node.key}].target_observation", node.target_observation)
            visit(f"{prefix}.drivers[{node.key}]", node.children)

    for segment in judgment.segments:
        prefix = f"business_model_judgment.segments[{segment.key}]"
        visit(prefix, segment.drivers)
        if segment.unit_economics is not None:
            check(
                f"{prefix}.unit_economics.gross_margin.target_observation",
                segment.unit_economics.gross_margin.target_observation,
            )
        for cost in segment.costs:
            check(f"{prefix}.costs[{cost.key}].target_observation", cost.target_observation)
    for cost in _consolidated_cost_judgments(judgment):
        check(
            f"business_model_judgment.costs[{cost.key}].target_observation",
            cost.target_observation,
        )
    return tuple(rows)


def _dollar_scale(node: DriverJudgment) -> str | None:
    if node.unit in ("per_count", "per_day"):
        return "units"
    if node.unit != "dollars":
        return None
    if node.factor_roles == ("price",):
        return "units"
    return "millions"


def _compile_target(node: DriverJudgment, node_key: str) -> dict[str, Any]:
    kind = node.calculation.kind
    if kind == "commentary":
        return {"target_type": "commentary", "sheet": None, "section_id": node_key}
    if _reads_statement_role(node.calculation):
        # A calculation over a statement total is history only: the total
        # includes this node's own stream, so projection stays an assumption.
        return {"target_type": "assumption_row", "section_id": node_key}
    if kind in {"sum", "product", "ratio", "roll_forward"}:
        return {"target_type": "derived_row", "section_id": node_key}
    return {"target_type": "assumption_row", "section_id": node_key}


def _lower_driver(
    node: DriverJudgment,
    *,
    index: _NodeIndex,
    targets: dict[str, TargetObservation],
) -> dict[str, Any]:
    node_key = semantic_slug(node.key, kind="driver")
    expression = _calculation_expr(node.calculation, node_key=node_key, index=index)
    target = _linked_target(node.target_observation, targets)
    is_decomposition = node.children_role == "decomposition"
    payload: dict[str, Any] = {
        "id": node_key,
        "label": _label(node.key, node.label),
        "factors": list(node.factor_roles),
        "unit": node.unit,
        "model_scale": _dollar_scale(node),
        "driver": None if is_decomposition else expression,
        "compile_to": _compile_target(node, node_key),
        "kpi": node.reported_kpi is not None,
        "kpi_source": node.reported_kpi.source_concept if node.reported_kpi else None,
        "kpi_frequency": node.reported_kpi.frequency if node.reported_kpi else None,
        "edgar_member": node.reported_kpi.member if node.reported_kpi else None,
        "edgar_axis": node.reported_kpi.axis if node.reported_kpi else None,
        "behavior": node.behavior,
        "observation_basis": node.observation_basis,
        "revenue_scope": node.revenue_scope,
        "balance_of": node.balance_of,
        "management_target": _target_text(target) if target else None,
        "note": _render_note(
            observation=(
                node.calculation.observation
                if isinstance(node.calculation, CommentaryCalculation)
                else None
            ),
            note=node.note,
            rationale=node.rationale,
            limitations=node.limitations,
            claim_keys=tuple(
                dict.fromkeys(
                    (*node.claim_keys, *(node.reported_kpi.claim_keys if node.reported_kpi else ()))
                )
            ),
        ),
        "children": [
            _lower_driver(child, index=index, targets=targets) for child in node.children
        ]
        or None,
        "children_role": node.children_role,
        "children_formula": expression if is_decomposition else None,
        "historical_observations": [
            row.model_dump(mode="json") for row in node.historical_observations
        ],
    }
    return payload


_COST_DRIVER_KEYS = {
    "sales_and_marketing": "sales_marketing_pct",
    "research_and_development": "rd_pct",
    "general_and_administrative": "ga_pct",
}


def _require_unique_control_cost_categories(
    costs: tuple[CostJudgment, ...],
    *,
    owner: str,
) -> None:
    categories = [cost.category for cost in costs if cost.category in _COST_DRIVER_KEYS]
    duplicates = sorted(
        category for category in set(categories) if categories.count(category) > 1
    )
    if duplicates:
        raise ValueError(
            f"{owner} has duplicate compiler-facing cost categories {duplicates}; "
            "express one economic judgment per natural cost category"
        )


def _consolidated_cost_judgments(
    judgment: BusinessModelJudgment,
) -> tuple[CostJudgment, ...]:
    """Return unambiguous consolidated cost judgment without agent plumbing.

    A sole segment is the consolidated business, so its canonical natural cost
    categories can be promoted deterministically.  For a multi-segment model,
    consolidation weights are judgment rather than mechanics and only explicit
    top-level costs are eligible.
    """

    costs = list(judgment.costs)
    if len(judgment.segments) != 1:
        return tuple(costs)
    declared = {
        cost.category for cost in costs if cost.category in _COST_DRIVER_KEYS
    }
    for cost in judgment.segments[0].costs:
        if cost.category not in _COST_DRIVER_KEYS or cost.category in declared:
            continue
        costs.append(cost)
        declared.add(cost.category)
    return tuple(costs)


def _lower_cost(
    cost: CostJudgment, targets: dict[str, TargetObservation]
) -> dict[str, Any]:
    target = _linked_target(cost.target_observation, targets)
    return {
        "id": semantic_slug(cost.key, kind="cost"),
        "label": _label(cost.key, cost.label),
        "driver_key": _COST_DRIVER_KEYS.get(cost.category),
        "factors": ["cost_structure"],
        "model_as": cost.model_as,
        "leverage": cost.leverage,
        "target_pct": (
            _target_fraction(target, field=f"cost {cost.key!r} target")
            if target is not None
            else None
        ),
        "note": _render_note(
            note=cost.note,
            rationale=cost.rationale,
            limitations=cost.limitations,
            claim_keys=cost.claim_keys,
        ),
    }


def _lower_reinvestment(value: ReinvestmentJudgment) -> dict[str, Any]:
    working_capital: dict[str, Any] | None = None
    if value.working_capital is not None:
        driver_keys = {
            metric: metric
            for metric in value.working_capital.driver_metrics
        }
        working_capital = {
            "model_as": value.working_capital.model_as,
            "intensity": value.working_capital.intensity,
            "driver_keys": driver_keys or None,
            "notes": list(value.working_capital.notes) or None,
        }
    acquisitions = None
    if value.acquisitions is not None:
        acquisitions = {
            "pattern": value.acquisitions.pattern,
            "note": value.acquisitions.note,
        }
    return {
        "capex": {
            "model_as": value.capex.model_as,
            "intensity": value.capex.intensity,
            "components": list(value.capex.components),
            "depreciation_rate": value.capex.observed_depreciation_rate,
            "amortization_rate": value.capex.observed_amortization_rate,
        },
        "working_capital": working_capital,
        "acquisitions": acquisitions,
    }


def _lower_capital_sources(value: CapitalSourcesJudgment) -> dict[str, Any]:
    return {
        "funding_model": value.funding_model,
        "debt": (
            {"level": value.debt.level, "target_metric": value.debt.target_metric}
            if value.debt
            else None
        ),
        "equity": (
            {
                "dilution_pattern": value.equity.dilution_pattern,
                "sbc": (
                    {
                        "model_as": value.equity.stock_compensation.model_as,
                        "offset": value.equity.stock_compensation.offset,
                    }
                    if value.equity.stock_compensation
                    else None
                ),
            }
            if value.equity
            else None
        ),
        "capital_return": (
            {
                "priority": value.capital_return.priority,
                "mechanisms": list(value.capital_return.mechanisms),
            }
            if value.capital_return
            else None
        ),
    }


def _observation_claim_keys(segment_key: str, node: DriverJudgment) -> tuple[str, ...]:
    node_key = f"{segment_key}.{semantic_slug(node.key, kind='driver')}"
    return tuple(
        business_model_observation_claim_key(node_key, row.period_id)
        for row in node.historical_observations
    )


def business_model_observation_sources(
    judgment: BusinessModelJudgment,
) -> tuple[dict[str, Any], ...]:
    """The ``sources`` rows the judgment's cited observations select, one per observation.

    A citation an observation names is a durable reference: the stage mints its
    evidence from the research file's capture exactly as for an authored
    ``sources`` row, bound to the observation's claim key, so the author states
    each reference once, on the observation.
    """

    rows: list[dict[str, Any]] = []

    def visit(segment_key: str, nodes: tuple[DriverJudgment, ...]) -> None:
        for node in nodes:
            claim_keys = _observation_claim_keys(segment_key, node)
            for claim_key, row in zip(claim_keys, node.historical_observations, strict=True):
                rows.append(
                    {"source_ref": row.citation.source_key(), "claim_keys": [claim_key]}
                )
            visit(segment_key, node.children)

    for segment in judgment.segments:
        visit(semantic_slug(segment.key, kind="segment"), segment.drivers)
    return tuple(rows)


def business_model_claim_keys(judgment: BusinessModelJudgment) -> tuple[str, ...]:
    """Enumerate selected claim keys in stable first-appearance order for FMS."""

    if not isinstance(judgment, BusinessModelJudgment):
        raise TypeError("judgment must be a BusinessModelJudgment")
    values: list[str] = [*judgment.claim_keys, *judgment.company.claim_keys]
    for issue in (*judgment.limitations, *judgment.unresolved_questions):
        values.extend(issue.claim_keys)

    def add_driver(segment_key: str, node: DriverJudgment) -> None:
        values.extend(node.claim_keys)
        values.extend(_observation_claim_keys(segment_key, node))
        if node.reported_kpi:
            values.extend(node.reported_kpi.claim_keys)
        for child in node.children:
            add_driver(segment_key, child)

    for segment in judgment.segments:
        values.extend(segment.claim_keys)
        if segment.unit_economics:
            values.extend(segment.unit_economics.claim_keys)
        for driver in segment.drivers:
            add_driver(semantic_slug(segment.key, kind="segment"), driver)
        for cost in segment.costs:
            values.extend(cost.claim_keys)
    for cost in judgment.costs:
        values.extend(cost.claim_keys)
    if judgment.reinvestment:
        values.extend(judgment.reinvestment.claim_keys)
    if judgment.capital_sources:
        values.extend(judgment.capital_sources.claim_keys)
    for target in judgment.targets:
        values.extend(target.claim_keys)
    return tuple(dict.fromkeys(values))


def business_model_semantic_claim_bindings(
    judgment: BusinessModelJudgment,
) -> tuple[BusinessModelSemanticClaimBinding, ...]:
    """Project compact judgment claim keys onto bounded downstream items.

    The mapping is derived from the same natural-key DTO that is lowered into
    ``BusinessModel``.  It therefore preserves semantic ownership without
    parsing rendered notes or requiring claim keys to equal server-generated
    item keys.
    """

    if not isinstance(judgment, BusinessModelJudgment):
        raise TypeError("judgment must be a BusinessModelJudgment")

    targets = _target_map(judgment)
    owned_targets: set[str] = set()
    rows: list[BusinessModelSemanticClaimBinding] = []

    def target_claims(value: str | None) -> tuple[str, ...]:
        if value is None:
            return ()
        key = semantic_slug(value, kind="target")
        target = _linked_target(value, targets)
        if target is None:  # an unresolved link owns no claims
            return ()
        owned_targets.add(key)
        return target.claim_keys

    def add(
        item_kind: Literal[
            "summary", "segment", "driver", "cost", "capital", "limitation"
        ],
        item_key: str,
        *claim_groups: tuple[str, ...],
    ) -> None:
        rows.append(
            build_business_model_semantic_claim_binding(
                item_kind=item_kind,
                item_key=item_key,
                claim_keys=tuple(
                    claim_key
                    for claim_group in claim_groups
                    for claim_key in claim_group
                ),
            )
        )

    summary_claims = [*judgment.claim_keys, *judgment.company.claim_keys]

    def add_driver(*, segment_key: str, node: DriverJudgment) -> None:
        node_key = f"{segment_key}.{semantic_slug(node.key, kind='driver')}"
        add(
            "driver",
            node_key,
            node.claim_keys,
            _observation_claim_keys(segment_key, node),
            node.reported_kpi.claim_keys if node.reported_kpi else (),
            target_claims(node.target_observation),
        )
        for child in node.children:
            add_driver(segment_key=segment_key, node=child)

    for segment in judgment.segments:
        segment_key = semantic_slug(segment.key, kind="segment")
        unit_economics = segment.unit_economics
        add(
            "segment",
            segment_key,
            segment.claim_keys,
            unit_economics.claim_keys if unit_economics else (),
            (
                target_claims(unit_economics.gross_margin.target_observation)
                if unit_economics
                else ()
            ),
        )
        for node in segment.drivers:
            add_driver(segment_key=segment_key, node=node)
        for cost in segment.costs:
            add(
                "cost",
                f"{segment_key}.{semantic_slug(cost.key, kind='cost')}",
                cost.claim_keys,
                target_claims(cost.target_observation),
            )

    for cost in _consolidated_cost_judgments(judgment):
        add(
            "cost",
            f"consolidated.{semantic_slug(cost.key, kind='cost')}",
            cost.claim_keys,
            target_claims(cost.target_observation),
        )

    reinvestment = judgment.reinvestment
    if reinvestment is not None:
        add("capital", "capital.reinvestment.capex", reinvestment.claim_keys)
        if reinvestment.working_capital is not None:
            add(
                "capital",
                "capital.reinvestment.working_capital",
                reinvestment.claim_keys,
            )
        if reinvestment.acquisitions is not None:
            add(
                "capital",
                "capital.reinvestment.acquisitions",
                reinvestment.claim_keys,
            )

    capital_sources = judgment.capital_sources
    if capital_sources is not None:
        add(
            "capital",
            "capital.sources.funding_model",
            capital_sources.claim_keys,
        )
        if capital_sources.debt is not None:
            add("capital", "capital.sources.debt", capital_sources.claim_keys)
        if capital_sources.equity is not None:
            add("capital", "capital.sources.equity", capital_sources.claim_keys)
        if capital_sources.capital_return is not None:
            add(
                "capital",
                "capital.sources.capital_return",
                capital_sources.claim_keys,
            )

    for issue in judgment.limitations:
        add("limitation", issue.key, issue.claim_keys)
    for issue in judgment.unresolved_questions:
        add("limitation", issue.key, issue.claim_keys)

    for key, target in targets.items():
        if key not in owned_targets:
            summary_claims.extend(target.claim_keys)
    add("summary", "business_model", tuple(summary_claims))
    return canonical_business_model_semantic_claim_bindings(rows)


def _decision_entries(
    judgment: BusinessModelJudgment, *, decision_date: str
) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = [
        {
            "date": decision_date,
            "change": f"Business model judgment ({judgment.confidence} confidence)",
            "rationale": judgment.rationale,
        }
    ]
    if judgment.company.rationale:
        entries.append(
            {
                "date": decision_date,
                "change": "Company characterization",
                "rationale": judgment.company.rationale,
            }
        )
    for limitation in judgment.company.limitations:
        entries.append(
            {
                "date": decision_date,
                "change": "Company characterization limitation",
                "rationale": limitation,
            }
        )
    for limitation in judgment.limitations:
        entries.append(
            {
                "date": decision_date,
                "change": f"Business model limitation: {limitation.key}",
                "rationale": limitation.text,
            }
        )
    for question in judgment.unresolved_questions:
        entries.append(
            {
                "date": decision_date,
                "change": f"Unresolved business model question: {question.key}",
                "rationale": question.text,
            }
        )
    for segment in judgment.segments:
        if segment.rationale:
            entries.append(
                {
                    "date": decision_date,
                    "change": f"Segment judgment: {_label(segment.key, segment.label)}",
                    "rationale": segment.rationale,
                }
            )
        for limitation in segment.limitations:
            entries.append(
                {
                    "date": decision_date,
                    "change": f"Segment limitation: {_label(segment.key, segment.label)}",
                    "rationale": limitation,
                }
            )
        if segment.unit_economics and segment.unit_economics.rationale:
            entries.append(
                {
                    "date": decision_date,
                    "change": f"Unit economics judgment: {_label(segment.key, segment.label)}",
                    "rationale": segment.unit_economics.rationale,
                }
            )
        for limitation in (
            segment.unit_economics.limitations if segment.unit_economics else ()
        ):
            entries.append(
                {
                    "date": decision_date,
                    "change": f"Unit economics limitation: {_label(segment.key, segment.label)}",
                    "rationale": limitation,
                }
            )
    for section_name, section in (
        ("Reinvestment", judgment.reinvestment),
        ("Capital sources", judgment.capital_sources),
    ):
        if section and section.rationale:
            entries.append(
                {
                    "date": decision_date,
                    "change": f"{section_name} judgment",
                    "rationale": section.rationale,
                }
            )
        for limitation in section.limitations if section else ():
            entries.append(
                {
                    "date": decision_date,
                    "change": f"{section_name} limitation",
                    "rationale": limitation,
                }
            )
    for target in judgment.targets:
        entries.append(
            {
                "date": decision_date,
                "change": f"Observed target: {target.key}",
                "rationale": _target_text(target),
            }
        )
    return entries


def lower_business_model_judgment(
    judgment: BusinessModelJudgment,
    *,
    ticker: str,
    revision: str,
    created_by: str,
    as_of: date | datetime | str,
) -> BusinessModel:
    """Purely lower one economic judgment into the canonical BusinessModel.

    The caller owns authenticated identity and time metadata.  This function
    performs no I/O, evidence lookup, persistence, or forecast-value synthesis.
    """

    if not isinstance(judgment, BusinessModelJudgment):
        raise TypeError("judgment must be a BusinessModelJudgment")
    normalized_ticker = str(ticker or "").strip().upper()
    normalized_revision = str(revision or "").strip()
    normalized_creator = str(created_by or "").strip()
    if not normalized_ticker:
        raise ValueError("ticker is required")
    if not normalized_revision:
        raise ValueError("revision is required")
    if not normalized_creator:
        raise ValueError("created_by is required")
    rendered_as_of, decision_date = _normalized_server_time(as_of)

    segment_keys: dict[str, str] = {}
    for segment in judgment.segments:
        key = semantic_slug(segment.key, kind="segment")
        if key in segment_keys:
            raise ValueError(
                f"duplicate/slug-colliding segment key {key!r}: "
                f"{segment_keys[key]!r} and {segment.key!r}"
            )
        segment_keys[key] = segment.key

    targets = _target_map(judgment)
    segments: list[dict[str, Any]] = []
    for segment in judgment.segments:
        index = _NodeIndex(segment, segment_keys)
        _validate_segment_graph(index)
        output_keys = tuple(index.resolve(output) for output in segment.revenue_outputs)
        if len(output_keys) != len(set(output_keys)):
            raise ValueError(f"segment {segment.key!r} has duplicate revenue outputs")
        if any(output not in index.top_level for output in output_keys):
            raise ValueError(
                f"segment {segment.key!r} revenue_outputs must reference top-level drivers"
            )
        for output in output_keys:
            output_node = index.nodes[output]
            if output_node.unit != "dollars":
                raise ValueError(
                    f"revenue output {output!r} must use dollars, got {output_node.unit!r}"
                )
            if isinstance(output_node.calculation, CommentaryCalculation):
                raise ValueError(f"revenue output {output!r} cannot be commentary")

        unit_economics = None
        if segment.unit_economics:
            gm = segment.unit_economics.gross_margin
            gm_target = _linked_target(gm.target_observation, targets)
            unit_economics = {
                "gross_margin": {
                    "behavior": gm.behavior,
                    "current_pct": gm.observed_pct,
                    "target_pct": (
                        _target_fraction(
                            gm_target,
                            field=f"segment {segment.key!r} gross margin target",
                        )
                        if gm_target
                        else None
                    ),
                    "drivers": list(gm.drivers),
                },
                "unit_definition": segment.unit_economics.unit_definition,
                "scale_dynamics": segment.unit_economics.scale_dynamics,
            }

        reporting = segment.reporting
        segments.append(
            {
                "id": semantic_slug(segment.key, kind="segment"),
                "label": _label(segment.key, segment.label),
                "match_name": segment.issuer_match_name
                or _label(segment.key, segment.label),
                "edgar_member": reporting.member if reporting else None,
                "edgar_axis": reporting.axis if reporting else None,
                "absorbs": (
                    [claim.model_dump(mode="python") for claim in reporting.absorbs]
                    if reporting and reporting.absorbs
                    else None
                ),
                "is_default": segment.is_default,
                "revenue_share": segment.revenue_share,
                "revenue_model": {
                    "type": segment.revenue_model,
                    "decomposition": [
                        _lower_driver(driver, index=index, targets=targets)
                        for driver in segment.drivers
                    ],
                    "consolidation_formula": {
                        "type": "sum",
                        "params": {
                            "operands": [
                                {"node_id": output, "t": 0, "sign": 1}
                                for output in output_keys
                            ]
                        },
                    },
                },
                "unit_economics": unit_economics,
                "cost_overrides": [
                    _lower_cost(cost, targets) for cost in segment.costs
                ]
                or None,
            }
        )

    profitability_targets: dict[str, float] = {}
    for target in judgment.targets:
        metric = semantic_slug(target.metric, kind="target metric")
        if metric in {
            "adjusted_ebitda_margin",
            "free_cash_flow_margin",
            "operating_margin",
        }:
            if metric in profitability_targets:
                raise ValueError(f"ambiguous duplicate profitability target for {metric!r}")
            profitability_targets[metric] = _target_fraction(
                target, field=f"profitability target {target.key!r}"
            )

    consolidated_costs = _consolidated_cost_judgments(judgment)
    consolidated = None
    if consolidated_costs or judgment.reinvestment or judgment.capital_sources:
        consolidated = {
            "cost_structure": {
                "items": [_lower_cost(cost, targets) for cost in consolidated_costs]
            },
            "reinvestment": (
                _lower_reinvestment(judgment.reinvestment)
                if judgment.reinvestment
                else None
            ),
            "capital_sources": (
                _lower_capital_sources(judgment.capital_sources)
                if judgment.capital_sources
                else None
            ),
        }

    payload = {
        "company": {
            "ticker": normalized_ticker,
            "name": judgment.company.name,
            "sector": judgment.company.sector,
            "industry": judgment.company.industry,
            "business_type": judgment.company.business_type,
        },
        "segments": segments,
        "consolidated": consolidated,
        "profitability_targets": profitability_targets or None,
        "recommended_depth": judgment.recommended_depth,
        "decisions_log": _decision_entries(judgment, decision_date=decision_date),
        "declined_series": [
            declined.model_dump(mode="python") for declined in judgment.declined_series
        ],
        "metadata": {
            "revision": normalized_revision,
            "created_by": normalized_creator,
            "created_at": rendered_as_of,
            "last_updated": rendered_as_of,
            "last_reviewed_at": rendered_as_of,
            "review_basis_filing": judgment.review_basis,
            "change_triggers": list(judgment.change_triggers),
            "source_materials": list(business_model_claim_keys(judgment)),
        },
    }
    return BusinessModel.model_validate(payload)


DriverJudgment.model_rebuild()
BusinessModelJudgment.model_rebuild()


BUSINESS_MODEL_JUDGMENT_TYPES = (
    CompanyJudgment,
    AbsorbedReportingClaim,
    SegmentReportingJudgment,
    ReportedKpiJudgment,
    GrowthCalculation,
    ExternalCalculation,
    SumCalculation,
    ProductCalculation,
    RatioCalculation,
    RollForwardCalculation,
    CommentaryCalculation,
    DriverJudgment,
    GrossMarginJudgment,
    UnitEconomicsJudgment,
    CostJudgment,
    CapexJudgment,
    WorkingCapitalJudgment,
    AcquisitionsJudgment,
    ReinvestmentJudgment,
    DebtJudgment,
    StockCompensationJudgment,
    EquityJudgment,
    CapitalReturnJudgment,
    CapitalSourcesJudgment,
    TargetObservation,
    BusinessModelJudgmentIssue,
    DeclinedSeriesJudgment,
    SegmentJudgment,
    BusinessModelJudgment,
)


__all__ = [
    "BUSINESS_MODEL_JUDGMENT_TYPES",
    "AbsorbedReportingClaim",
    "AcquisitionsJudgment",
    "BusinessModelJudgment",
    "BusinessModelJudgmentIssue",
    "CalculationJudgment",
    "CapexJudgment",
    "CapitalReturnJudgment",
    "CapitalSourcesJudgment",
    "CommentaryCalculation",
    "CompanyJudgment",
    "CostJudgment",
    "DebtJudgment",
    "DeclinedSeriesJudgment",
    "DriverJudgment",
    "EquityJudgment",
    "ExternalCalculation",
    "GrossMarginJudgment",
    "GrowthCalculation",
    "ProductCalculation",
    "RatioCalculation",
    "ReinvestmentJudgment",
    "ReportedKpiJudgment",
    "RollForwardCalculation",
    "SegmentJudgment",
    "SegmentReportingJudgment",
    "StockCompensationJudgment",
    "SumCalculation",
    "TargetObservation",
    "UnitEconomicsJudgment",
    "UnresolvedTargetLink",
    "WorkingCapitalJudgment",
    "business_model_claim_keys",
    "business_model_observation_sources",
    "business_model_semantic_claim_bindings",
    "lower_business_model_judgment",
    "semantic_slug",
    "unresolved_target_links",
]
