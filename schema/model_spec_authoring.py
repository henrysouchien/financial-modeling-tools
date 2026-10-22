"""Plumbing-free authoring contracts for server-assembled ModelSpec 2.2.

These DTOs are the judgment projection of the canonical ModelSpec fragments.
They intentionally cannot express server bindings, generated identifiers,
review-event metadata, or the mechanically lowered ``scenarios`` field.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
import re
from types import MappingProxyType
from typing import Annotated, Final, Literal, Mapping

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    model_validator,
)

from .finding_vocabulary import DECLINABLE_FINDING_CODES, Declination, DeclinationAuthor
from .build_model_product_contracts import (
    DcfMethodControl,
    EvidenceBasis,
    HistoricalNormalizationDecision,
    ModelSensitivity,
    ModelSpec,
    ModelValuationPolicy,
    RelativeValuationControl,
    ScenarioOverride,
    ScenarioReview,
    SensitivityAxis,
    ValuationEstimateBasis,
    ValuationFamilyAuthorization,
    ValuationPeerPolicy,
    ValuationSummaryPolicy,
    ValuationTerminalCaseValues,
)
from .thesis_shared_slice import Assumption, FieldDisposition, Valuation


NonBlankString = Annotated[str, Field(min_length=1)]
Selector = Annotated[str, Field(min_length=1)]
FiniteNumber = Annotated[float, Field(allow_inf_nan=False)]
AuthoringPeriodValueSeries = dict[int, FiniteNumber]
ScalarValue = str | int | FiniteNumber | bool
Confidence = Literal["low", "medium", "high"]
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
DriverCategoryName = Literal[
    "revenue",
    "unit_economics",
    "cost_structure",
    "reinvestment",
    "capital_sources",
    "valuation",
    "other",
]


class _AuthoringDTO(BaseModel):
    """Common fail-closed shape for every authoring fragment."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


class HistoricalNormalizationDecisionAuthoring(_AuthoringDTO):
    """Normalization judgment named by an agent-visible natural key."""

    label: Annotated[NonBlankString, FieldDisposition.JUDGMENT]
    description: NonBlankString
    evidence_basis: EvidenceBasis
    evidence_ids: tuple[Selector, ...] = ()
    limitations: NonBlankString

    @model_validator(mode="after")
    def _source_backed(self) -> "HistoricalNormalizationDecisionAuthoring":
        if self.evidence_basis == "source_backed" and not self.evidence_ids:
            raise ValueError("source-backed decisions require evidence_ids")
        return self


class ForecastAssumptionAuthoring(_AuthoringDTO):
    """Forecast judgment keyed by the selected business-model driver."""

    driver: Selector
    value: ScalarValue | None = None
    period_values: AuthoringPeriodValueSeries | None = None
    unit: UnitName | None = None
    percentage_value_space: Literal["fraction", "percentage_points"] | None = None
    rationale: NonBlankString | None = None
    source_refs: tuple[Selector, ...] = ()
    source_refs_by_period: dict[int, tuple[Selector, ...]] = Field(default_factory=dict)
    extension_basis_by_period: dict[int, NonBlankString] = Field(default_factory=dict)
    confidence_by_period: dict[int, Confidence] = Field(default_factory=dict)
    source_notes_by_period: dict[int, NonBlankString] = Field(default_factory=dict)
    period_ids: tuple[Selector, ...] = ()
    evidence_basis: EvidenceBasis | None = None
    evidence_ids: tuple[Selector, ...] = ()
    #: HFF baseline evidence the control was seeded from (lineage
    #: ``historical_seed``); never a citation. Omitted when empty so rows and
    #: digests stored before the field existed read and verify unchanged.
    inherited_baseline_ids: tuple[Selector, ...] = Field(
        default=(), exclude_if=lambda value: not value
    )
    limitations: NonBlankString | None = None
    value_scale: Literal["model"] = "model"
    driver_category: DriverCategoryName | None = None
    confidence: Confidence | None = None
    held_at_base: bool = False

    @model_validator(mode="after")
    def _value_and_evidence_shape(self) -> "ForecastAssumptionAuthoring":
        if self.value is not None and self.period_values is not None:
            raise ValueError("value and period_values are mutually exclusive")
        if self.unit != "percentage" and self.percentage_value_space is not None:
            raise ValueError("percentage_value_space requires percentage unit")
        if (
            self.evidence_basis == "source_backed"
            and not self.evidence_ids
            and not self.source_refs
        ):
            raise ValueError("source-backed assumptions require evidence")
        valued_periods = set(self.period_values or {})
        metadata_periods = {
            *self.source_refs_by_period,
            *self.extension_basis_by_period,
            *self.confidence_by_period,
            *self.source_notes_by_period,
        }
        if not metadata_periods <= valued_periods:
            raise ValueError("period metadata must name a valued period")
        return self


class ScenarioOverrideAuthoring(_AuthoringDTO):
    """Scenario override referencing an assumption by its driver natural key."""

    driver: Selector
    period_ids: tuple[Selector, ...] = Field(min_length=1)
    unit: NonBlankString
    value: FiniteNumber | AuthoringPeriodValueSeries
    direction: Literal["increase", "decrease", "context_specific", "base"]
    rationale: NonBlankString
    evidence_basis: EvidenceBasis
    evidence_ids: tuple[Selector, ...] = ()
    limitations: NonBlankString

    @model_validator(mode="after")
    def _source_backed(self) -> "ScenarioOverrideAuthoring":
        if self.evidence_basis == "source_backed" and not self.evidence_ids:
            raise ValueError("source-backed scenario override requires evidence_ids")
        return self


class ScenarioReviewAuthoring(_AuthoringDTO):
    scenario_name: NonBlankString
    description: NonBlankString
    overrides: tuple[ScenarioOverrideAuthoring, ...] = ()


class ValuationControlAuthoring(_AuthoringDTO):
    method: Literal["dcf", "multiples", "sum_of_parts", "hybrid", "relative"]
    low: FiniteNumber | None = None
    mid: FiniteNumber | None = None
    high: FiniteNumber | None = None
    current_multiple: FiniteNumber | None = None
    wacc: FiniteNumber | None = None
    risk_free_rate: FiniteNumber | None = None
    sofr_rate: FiniteNumber | None = None
    credit_spread: FiniteNumber | None = None
    equity_risk_premium: FiniteNumber | None = None
    cost_of_equity: FiniteNumber | None = None
    raw_beta: FiniteNumber | None = None
    adjusted_beta: FiniteNumber | None = None
    beta_floor: FiniteNumber | None = None
    terminal_growth_rate: FiniteNumber | None = None
    terminal_multiple: FiniteNumber | None = None
    rationale: NonBlankString | None = None
    source_refs: tuple[Selector, ...] = ()

    @model_validator(mode="after")
    def _decimal_rates(self) -> "ValuationControlAuthoring":
        for name in (
            "wacc",
            "risk_free_rate",
            "sofr_rate",
            "credit_spread",
            "equity_risk_premium",
            "cost_of_equity",
            "terminal_growth_rate",
        ):
            value = getattr(self, name)
            if value is not None and not -1.0 <= value <= 1.0:
                raise ValueError(f"{name} must be a decimal rate between -1 and 1")
        return self


class DcfMethodControlAuthoring(_AuthoringDTO):
    method: Literal["terminal_growth", "exit_multiple"]
    display_role: Literal["selected_canonical", "labeled_cross_check"]
    wacc_control_field: Literal["wacc"]
    terminal_control_field: Literal["terminal_growth_rate", "terminal_multiple"]
    cash_flow_basis: Literal["unlevered_free_cash_flow"]
    forecast_period_ids: tuple[Selector, ...] = Field(min_length=1)
    enterprise_to_equity_bridge: Literal[
        "corporate_cash_less_corporate_debt_excluding_client_funds"
    ]
    share_basis: Literal["diluted_shares"]
    output_label: NonBlankString
    rationale: NonBlankString
    limitations: NonBlankString


class RelativeValuationControlAuthoring(_AuthoringDTO):
    method: Literal["forward_pe", "ev_adjusted_ebitda"]
    fiscal_basis_period_ids: tuple[Selector, ...] = Field(min_length=1)
    selection_basis: Literal[
        "none", "current_implied", "peer_derived", "thesis_judgment"
    ] = "none"
    rationale: NonBlankString | None = None
    bridge: Literal[
        "equity_value_per_diluted_share",
        "enterprise_value_to_equity_value_per_diluted_share",
    ]
    output_label: NonBlankString
    evidence_ids: tuple[Selector, ...] = ()
    limitations: NonBlankString


#: Relative selection bases whose value derives from observation evidence
#: through the peer roster; ``thesis_judgment`` and ``none`` need no roster.
PEER_ROSTER_SELECTION_BASES = frozenset({"current_implied", "peer_derived"})


class ValuationPeerPolicyAuthoring(_AuthoringDTO):
    """The peer roster; empty when no relative method derives from peers."""

    peer_tickers: tuple[Selector, ...]
    minimum_covered_peer_count: int = Field(ge=0)
    metric_ids: tuple[Literal["forward_pe", "ev_adjusted_ebitda"], ...]
    fiscal_basis_period_ids: tuple[Selector, ...] = Field(min_length=1)
    estimate_basis: ValuationEstimateBasis
    as_of_policy: Literal[
        "common_market_close_at_or_before_cutoff_max_five_business_days"
    ]
    aggregation: Literal["median"]
    replacement_policy: Literal["no_automatic_replacements"]

    @model_validator(mode="after")
    def _peer_policy(self) -> "ValuationPeerPolicyAuthoring":
        if self.peer_tickers != tuple(sorted(set(self.peer_tickers))):
            raise ValueError("peer_tickers must be sorted and unique")
        if self.minimum_covered_peer_count > len(self.peer_tickers):
            raise ValueError("minimum peer count cannot exceed named peers")
        if self.metric_ids != tuple(sorted(set(self.metric_ids))):
            raise ValueError("metric_ids must be sorted and unique")
        return self


class SensitivityAxisAuthoring(_AuthoringDTO):
    control_id: Selector
    unit: NonBlankString
    ordered_values: tuple[FiniteNumber, ...] = Field(min_length=3)

    @model_validator(mode="after")
    def _ordered(self) -> "SensitivityAxisAuthoring":
        if self.ordered_values != tuple(sorted(set(self.ordered_values))):
            raise ValueError("ordered_values must be sorted and unique")
        return self


class ModelSensitivityAuthoring(_AuthoringDTO):
    """Sensitivity judgment; ``sensitivity_id`` is server-minted."""

    row_axis: SensitivityAxisAuthoring
    column_axis: SensitivityAxisAuthoring
    output_ids: tuple[Selector, ...] = Field(min_length=1)
    pinned_input_policy: Literal["all_non_varied_model_spec_inputs"]
    nonzero_movement_required: Literal[True]


class ValuationSummaryPolicyAuthoring(_AuthoringDTO):
    displayed_method: Literal["dcf_terminal_growth"]
    displayed_output_label: Literal["Model valuation — equity value per diluted share"]
    target_fiscal_period_id: Selector
    requires_valid_base_model: Literal[True]


class ModelValuationPolicyAuthoring(_AuthoringDTO):
    dcf_methods: tuple[DcfMethodControlAuthoring, ...] = Field(
        min_length=2, max_length=2
    )
    relative_methods: tuple[RelativeValuationControlAuthoring, ...] = Field(
        min_length=2, max_length=2
    )
    peer_policy: ValuationPeerPolicyAuthoring
    sensitivities: tuple[ModelSensitivityAuthoring, ...] = Field(min_length=1)
    summary: ValuationSummaryPolicyAuthoring
    #: The observation ids the accepted ValuationPolicy cites for its terminal
    #: cases and its beta floor, copied by the server projection. Omitted when the
    #: policy cites none, so a policy without them keeps its digest.
    terminal_evidence_ids: tuple[str, ...] = Field(
        default=(), exclude_if=lambda value: not value
    )
    beta_floor_evidence_ids: tuple[str, ...] = Field(
        default=(), exclude_if=lambda value: not value
    )

    @model_validator(mode="after")
    def _roster_for_evidence_derived_selection(
        self,
    ) -> "ModelValuationPolicyAuthoring":
        if self.peer_policy.peer_tickers:
            return self
        selected = sorted(
            row.method
            for row in self.relative_methods
            if row.selection_basis in PEER_ROSTER_SELECTION_BASES
        )
        if selected:
            raise ValueError(
                f"relative methods {selected!r} select a peer-derived value and "
                "need at least one peer ticker"
            )
        return self


class ValuationFamilyAuthorizationAuthoring(_AuthoringDTO):
    method: Literal["forward_pe", "ev_adjusted_ebitda"]
    selection_basis: Literal["current_implied", "peer_derived", "thesis_judgment"]
    rationale: NonBlankString
    evidence_ids: tuple[Selector, ...] = ()
    case_order: Literal["bull_high", "bull_low"]
    bull: Decimal = Field(gt=0)
    base: Decimal = Field(gt=0)
    bear: Decimal = Field(gt=0)


def _fiscal_year(value: object) -> object:
    """Read a period id as a finding prints it (``FY2022``, ``FY2027E``)."""

    if isinstance(value, str):
        match = re.fullmatch(r"FY(\d{4})E?", value.strip())
        if match is not None:
            return int(match.group(1))
    return value


#: A fiscal year, given as the integer or as the printed period id.
FiscalYear = Annotated[int, BeforeValidator(_fiscal_year)]


class DeclinationAuthoring(_AuthoringDTO):
    """One answer to a declinable warning finding.

    ``code``, ``item_id`` and ``periods`` are copied from the warning the build
    or check response lists (a period as ``2022`` or as printed, ``FY2022``;
    empty ``periods`` answers every period of it);
    ``declination_id`` and ``author`` are server-stamped. Errors are never
    declinable.
    """

    code: NonBlankString
    item_id: Selector | None = None
    periods: tuple[FiscalYear, ...] = ()
    reason: NonBlankString
    citation: NonBlankString | None = None

    @model_validator(mode="after")
    def _declinable_code(self) -> "DeclinationAuthoring":
        if self.code not in DECLINABLE_FINDING_CODES:
            raise ValueError(
                f"finding code {self.code!r} is not declinable; declinable codes: "
                + ", ".join(DECLINABLE_FINDING_CODES)
            )
        if self.periods:
            object.__setattr__(self, "periods", tuple(sorted(set(self.periods))))
        return self


class DeclinationDraft(DeclinationAuthoring):
    """A declination as the author door stores it: with its stamped author."""

    author: DeclinationAuthor


class ModelSpecAuthoringSections(_AuthoringDTO):
    """All nine draftable ModelSpec 2.2 judgment sections.

    Empty tuple sections are intentional: an explicit empty judgment remains
    distinguishable from server ownership. Request-level subset/emptiness rules
    belong to the authoring door rather than this reusable value contract.
    """

    historical_normalization_decisions: tuple[
        HistoricalNormalizationDecisionAuthoring, ...
    ] = ()
    forecast_assumptions: tuple[ForecastAssumptionAuthoring, ...] = ()
    scenario_reviews: tuple[ScenarioReviewAuthoring, ...] = ()
    valuation_controls: tuple[ValuationControlAuthoring, ...] = ()
    valuation_policy: ModelValuationPolicyAuthoring | None = None
    valuation_family_authorizations: tuple[
        ValuationFamilyAuthorizationAuthoring, ...
    ] = ()
    declinations: tuple[DeclinationDraft, ...] = ()
    analyst_limitations: tuple[NonBlankString, ...] = ()


def _require_non_empty_non_null_subset(patch: BaseModel) -> None:
    if not patch.model_fields_set:
        raise ValueError("sections must contain a non-empty judgment-section subset")
    null_sections = tuple(
        sorted(
            field for field in patch.model_fields_set if getattr(patch, field) is None
        )
    )
    if null_sections:
        raise ValueError(f"authored sections cannot be null: {null_sections!r}")


class ModelSpecAuthoringSectionPatch(_AuthoringDTO):
    """A non-empty subset of judgment sections, server projections included.

    The server builds this from exact accepted artifacts.
    """

    historical_normalization_decisions: (
        tuple[HistoricalNormalizationDecisionAuthoring, ...] | None
    ) = None
    forecast_assumptions: tuple[ForecastAssumptionAuthoring, ...] | None = None
    scenario_reviews: tuple[ScenarioReviewAuthoring, ...] | None = None
    valuation_controls: tuple[ValuationControlAuthoring, ...] | None = None
    valuation_policy: ModelValuationPolicyAuthoring | None = None
    valuation_family_authorizations: (
        tuple[ValuationFamilyAuthorizationAuthoring, ...] | None
    ) = None
    declinations: tuple[DeclinationAuthoring, ...] | None = None
    analyst_limitations: tuple[NonBlankString, ...] | None = None

    @model_validator(mode="after")
    def _non_empty_subset(self) -> "ModelSpecAuthoringSectionPatch":
        _require_non_empty_non_null_subset(self)
        return self


AUTHORING_DTO_TYPES: Final[tuple[type[BaseModel], ...]] = tuple(
    _AuthoringDTO.__subclasses__()
)


MODEL_SPEC_2_2_JUDGMENT_FIELDS: Final[frozenset[str]] = frozenset(
    {
        "historical_normalization_decisions",
        "forecast_assumptions",
        "scenario_reviews",
        "valuation_controls",
        "valuation_policy",
        "valuation_family_authorizations",
        "declinations",
        "analyst_limitations",
    }
)


@dataclass(frozen=True)
class FieldDispositionRecord:
    fragment: str
    field: str
    disposition: FieldDisposition

    @property
    def path(self) -> str:
        return f"{self.fragment}.{self.field}"


MODEL_SPEC_2_2_FRAGMENT_TYPES: Final[Mapping[str, type[BaseModel]]] = MappingProxyType(
    {
        "ModelSpec": ModelSpec,
        "HistoricalNormalizationDecision": HistoricalNormalizationDecision,
        "Assumption": Assumption,
        "ScenarioReview": ScenarioReview,
        "ScenarioOverride": ScenarioOverride,
        "Valuation": Valuation,
        "ModelValuationPolicy": ModelValuationPolicy,
        "DcfMethodControl": DcfMethodControl,
        "RelativeValuationControl": RelativeValuationControl,
        "ValuationPeerPolicy": ValuationPeerPolicy,
        "ModelSensitivity": ModelSensitivity,
        "SensitivityAxis": SensitivityAxis,
        "ValuationSummaryPolicy": ValuationSummaryPolicy,
        "ValuationTerminalCaseValues": ValuationTerminalCaseValues,
        "ValuationFamilyAuthorization": ValuationFamilyAuthorization,
        "Declination": Declination,
    }
)


AUTHORING_SYNONYM_ALLOWLISTS: Final[Mapping[str, Mapping[str, str]]] = MappingProxyType(
    {
        section: MappingProxyType({})
        for section in sorted(MODEL_SPEC_2_2_JUDGMENT_FIELDS)
    }
)


def model_spec_2_2_field_dispositions() -> tuple[FieldDispositionRecord, ...]:
    """Read every disposition declared on a ModelSpec 2.2 fragment field.

    ``HistoricalNormalizationDecision.decision_id`` is minted from the authored
    ``label``, which exists only on the authoring DTO, so that DTO is read too.
    """

    owners: Mapping[str, type[BaseModel]] = {
        **MODEL_SPEC_2_2_FRAGMENT_TYPES,
        "HistoricalNormalizationDecisionAuthoring": (
            HistoricalNormalizationDecisionAuthoring
        ),
    }
    return tuple(
        FieldDispositionRecord(fragment, field, marker)
        for fragment, contract in owners.items()
        for field, info in contract.model_fields.items()
        for marker in info.metadata
        if isinstance(marker, FieldDisposition)
    )


__all__ = [
    "AUTHORING_DTO_TYPES",
    "AUTHORING_SYNONYM_ALLOWLISTS",
    "DeclinationAuthoring",
    "DeclinationDraft",
    "FieldDispositionRecord",
    "ForecastAssumptionAuthoring",
    "HistoricalNormalizationDecisionAuthoring",
    "MODEL_SPEC_2_2_FRAGMENT_TYPES",
    "MODEL_SPEC_2_2_JUDGMENT_FIELDS",
    "ModelSensitivityAuthoring",
    "ModelSpecAuthoringSections",
    "ModelSpecAuthoringSectionPatch",
    "ModelValuationPolicyAuthoring",
    "ScenarioOverrideAuthoring",
    "ScenarioReviewAuthoring",
    "ValuationControlAuthoring",
    "ValuationFamilyAuthorizationAuthoring",
    "model_spec_2_2_field_dispositions",
]
