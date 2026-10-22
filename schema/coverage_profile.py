"""Shared annual coverage-profile contracts and per-period source pins."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal
from typing import TYPE_CHECKING, Final, Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    field_validator,
    model_serializer,
    model_validator,
)

from .model_kind_capabilities import (
    ModelKindCapabilities,
    decode_model_kind_capabilities,
)
from .provider_port.contracts import FilingLineRef, SourceFactRef, mint_source_fact_ref
from .ticker_contract import normalize_contract_ticker

if TYPE_CHECKING:
    from .role_disposition import RoleDispositionTable


class _CoverageContract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


SourceRequirementFamily = Literal[
    "statement",
    "operating",
    "adjustment",
    "forecast_evidence",
    "valuation_comparable",
]


#: The one owner of the required-role key grammar: a statement role
#: (``statement.<is|bs|cf>.<leaf>``), an operating role
#: (``operating.<leaf>`` or the segment-scoped ``operating.<segment>.<metric>``)
#: or an adjustment role. Every consumer imports this string:
#: ``decode_statement_bindings`` holds each binding row key to it
#: (``role_key_invalid``) and ``business_model_judgment.KPI_SOURCE_PATTERN``
#: is a superset of it (tests/schema/test_business_model_judgment.py).
ANNUAL_REQUIRED_ROLE_PATTERN: Final = (
    r"^(?:statement\.(?:is|bs|cf)\.[a-z0-9][a-z0-9_]*"
    r"|(?:operating|adjustment)\.[a-z0-9][a-z0-9_]*(?:\.[a-z0-9][a-z0-9_]*)*)$"
)


#: The SRC-11 dispositions under which a coverage period carries a supported
#: numeric value. Cited inclusion is adjudicated but deliberately nonnumeric.
#: Single definition for every supported-denominator consumer.
SUPPORTED_COVERAGE_DISPOSITIONS: Final[frozenset[str]] = frozenset(
    {"observed", "derived", "aggregate_only_hidden"}
)


ValuationMetric = Literal[
    "fwd_pe",
    "fwd_ev_adj_ebitda",
    "quote",
    "beta",
    "risk_free_10y",
    "erp",
    "ev_component:price",
    "ev_component:shares",
    "ev_component:debt",
    "ev_component:cash",
    "ev_component:declared_adjustment",
]


ValuationFiscalBasis = str


class AbsenceCitation(_CoverageContract):
    """Filing citation for an authored nonnumeric issuer-binding judgment."""

    accession: str = Field(min_length=1)
    presentation_role_uri: str = Field(min_length=1)
    absence_statement: str = Field(min_length=1)


class RetainedVintageCandidate(_CoverageContract):
    """One numeric vintage of a period, in the requirement's units."""

    evidence_id: str = Field(min_length=1)
    line: FilingLineRef
    available_at: datetime
    value: Decimal
    resolution: Decimal
    restates_evidence_id: str | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    capture_id: str | None = Field(
        default=None, exclude_if=lambda value: value is None
    )


class PeriodPinCell(_CoverageContract):
    """One period of a role pin. The cell is the period's authority."""

    period_id: str = Field(min_length=1)
    # Inert wire field until the next HFF/snapshot epoch (ruling R6): the "formula" member.
    binding_state: Literal["bound", "formula", "not_applicable", "not_separately_presented", "unbound"]
    pinned_ref: FilingLineRef | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    absence_citation: AbsenceCitation | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    included_in: str | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    unbound_reason: str | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    # Inert wire field until the next HFF/snapshot epoch (ruling R6).
    registered_formula: str | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    vintage_candidates: tuple[RetainedVintageCandidate, ...] = Field(
        default=(), exclude_if=lambda value: not value
    )
    selected_evidence_id: str | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    cutoff_at: datetime | None = Field(
        default=None, exclude_if=lambda value: value is None
    )

    @property
    def source_ref(self) -> SourceFactRef | None:
        if self.pinned_ref is None:
            return None
        suffix = (
            self.selected_evidence_id.rsplit(".", 1)[-1]
            if self.selected_evidence_id is not None
            and self.selected_evidence_id.startswith("evidence.")
            else None
        )
        return mint_source_fact_ref(self.pinned_ref, "statement", suffix=suffix)

    @model_validator(mode="after")
    def _validate_cell(self) -> "PeriodPinCell":
        if self.binding_state == "bound":
            if self.pinned_ref is None:
                raise ValueError("bound period cell requires a pinned filing line")
            if self.pinned_ref.period.canonical_label() != self.period_id:
                raise ValueError("bound period cell line must name its period")
            if self.absence_citation is not None or self.registered_formula is not None:
                raise ValueError("bound period cell cannot carry an absence or formula")
        elif self.binding_state == "formula":
            if not self.registered_formula:
                raise ValueError("formula period cell requires its registered formula")
            if self.pinned_ref is not None or self.absence_citation is not None:
                raise ValueError("formula period cell pins nothing")
        elif self.binding_state in {"not_applicable", "not_separately_presented"}:
            if self.absence_citation is None:
                raise ValueError(
                    "cited period cell requires its filing citation"
                )
            if self.pinned_ref is not None or self.registered_formula is not None:
                raise ValueError("cited period cell pins nothing")
        elif (
            self.pinned_ref is not None
            or self.registered_formula is not None
            or self.absence_citation is not None
        ):
            raise ValueError("unbound period cell pins nothing")
        if self.vintage_candidates:
            if self.cutoff_at is None:
                raise ValueError(
                    "retained vintages require the cutoff they were selected at"
                )
            evidence_ids = tuple(item.evidence_id for item in self.vintage_candidates)
            if len(evidence_ids) != len(set(evidence_ids)):
                raise ValueError("retained vintage evidence ids must be unique")
            if any(
                item.line.period.canonical_label() != self.period_id
                for item in self.vintage_candidates
            ):
                raise ValueError("retained vintage line must name the cell period")
            if self.selected_evidence_id is None:
                if self.binding_state not in {"unbound", "not_separately_presented"} or self.pinned_ref is not None:
                    raise ValueError("a period with no selected vintage must be nonnumeric")
            else:
                chosen = next(
                    (
                        item
                        for item in self.vintage_candidates
                        if item.evidence_id == self.selected_evidence_id
                    ),
                    None,
                )
                if chosen is None:
                    raise ValueError(
                        "selected vintage must be one of the retained candidates"
                    )
                if self.pinned_ref != chosen.line:
                    raise ValueError(
                        "selected vintage line must be the pinned filing line"
                    )
        elif self.selected_evidence_id is not None or self.cutoff_at is not None:
            raise ValueError("vintage selection requires retained candidates")
        if self.binding_state == "not_separately_presented":
            if not self.included_in or self.selected_evidence_id is not None:
                raise ValueError("included period requires its parent and no numeric selection")
        elif self.included_in is not None:
            raise ValueError("only an included period carries included_in")
        return self


class KpiAssociation(_CoverageContract):
    """The typed KPI observations cited for one issuer series."""

    issuer_series_key: str = Field(min_length=1)
    observation_ids: tuple[str, ...] = Field(min_length=1)


class AnnualSourceRequirement(_CoverageContract):
    """One required role, optionally bound when carried on a completed pin."""

    requirement_id: str = Field(min_length=1)
    family: SourceRequirementFamily
    required_role: str = Field(
        min_length=1, pattern=ANNUAL_REQUIRED_ROLE_PATTERN
    )
    model_item: str = Field(min_length=1)
    period_ids: tuple[str, ...] = Field(min_length=1)
    #: No statement pin is implied by an empty pinned_columns tuple. KPI
    #: association is recorded independently, only when observations were cited.
    # Serialized only when it was set (explicitly, including an explicit null
    # read back from a stored row): coverage rows ride inside persisted
    # HistoricalFoundation / SourceBundle payloads whose content digests cover
    # the dump, so a row stored before this field existed re-serializes
    # without it and a row stored with ``null`` re-serializes with it.
    kpi_association: KpiAssociation | None = None
    pinned_columns: tuple[FilingLineRef, ...] = ()
    # Inert wire field until the next HFF/snapshot epoch (ruling R6).
    registered_formula: str | None = None
    unit: str = Field(min_length=1)
    scale: str = Field(min_length=1)
    sign: str = Field(min_length=1)
    downstream_capabilities: tuple[str, ...] = Field(min_length=1)
    disclosed_child: bool = False
    independently_material: bool = True
    aggregate_parent_requirement_id: str | None = None
    #: Issuer-binding state for this role (`statement_bindings.v2`): a
    #: `not_applicable` row carries its filing citation here and pins nothing;
    #: an `unbound` row carries its reason and pins nothing.
    # Inert wire field until the next HFF/snapshot epoch (ruling R6): the "formula" member.
    binding_state: Literal["bound", "formula", "not_applicable", "not_separately_presented", "unbound"] | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    absence_citation: "AbsenceCitation | None" = Field(
        default=None, exclude_if=lambda value: value is None
    )
    included_in: str | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    unbound_reason: str | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    #: Per-period pin. Empty on a stored epoch-10 row, which keeps the old
    #: one-state shape; excluded from the wire when empty so that row decodes
    #: and re-serializes unchanged. When present, these cells — not
    #: ``binding_state`` / ``absence_citation`` — are
    #: the period authority.
    period_cells: tuple["PeriodPinCell", ...] = Field(
        default=(), exclude_if=lambda value: not value
    )

    @model_validator(mode="after")
    def _validate_requirement(self) -> "AnnualSourceRequirement":
        if len(self.period_ids) != len(set(self.period_ids)):
            raise ValueError("requirement periods must be unique")
        if self.period_cells:
            if tuple(cell.period_id for cell in self.period_cells) != self.period_ids:
                raise ValueError("period cells must exactly cover requirement periods")
            bound_refs = tuple(
                cell.pinned_ref
                for cell in self.period_cells
                if cell.binding_state == "bound"
            )
            if self.pinned_columns != bound_refs:
                raise ValueError(
                    "pinned columns must be exactly the bound period cells"
                )
        else:
            if self.pinned_columns:
                bound_periods = tuple(
                    line.period.canonical_label() for line in self.pinned_columns
                )
                if bound_periods != self.period_ids:
                    raise ValueError(
                        "pinned columns must exactly cover requirement fiscal periods"
                    )
                if len(set(self.pinned_columns)) != len(self.pinned_columns):
                    raise ValueError("requirement pinned columns must be unique")
        if self.pinned_columns and len(set(self.pinned_columns)) != len(self.pinned_columns):
            raise ValueError("requirement pinned columns must be unique")
        if self.aggregate_parent_requirement_id == self.requirement_id:
            raise ValueError("requirement cannot be its own aggregate parent")
        return self

    def cell_for_period(self, period_id: str) -> "PeriodPinCell":
        """The period's pin cell.

        A persisted cell is returned as stored, except a stored ``formula`` cell
        (an inert wire member, ruling R6): it pins nothing, so it reads as an
        ``unbound`` cell here, once, for every reader. The stored cell, and so
        every content digest over its dump, is untouched. A stored row with no
        cells is decoded from that period's column, else its
        absence citation, else unbound with the reason the row actually stored.
        The role summary is not read to admit a period, and a period this row
        does not carry is not invented.
        """

        if period_id not in self.period_ids:
            raise ValueError(
                f"period {period_id} is not on requirement {self.requirement_id}"
            )
        if self.period_cells:
            cell = next(
                (item for item in self.period_cells if item.period_id == period_id),
                None,
            )
            if cell is None:
                raise ValueError(
                    f"period {period_id} has no persisted cell on {self.requirement_id}"
                )
            if cell.binding_state == "formula":
                return cell.model_copy(
                    update={"binding_state": "unbound", "registered_formula": None}
                )
            return cell
        line = next(
            (
                item
                for item in self.pinned_columns
                if item.period.canonical_label() == period_id
            ),
            None,
        )
        if line is not None:
            return PeriodPinCell(
                period_id=period_id, binding_state="bound", pinned_ref=line
            )
        if self.absence_citation is not None:
            return PeriodPinCell(
                period_id=period_id,
                binding_state=(
                    "not_separately_presented"
                    if self.binding_state == "not_separately_presented"
                    else "not_applicable"
                ),
                absence_citation=self.absence_citation,
                included_in=self.included_in,
            )
        return PeriodPinCell(
            period_id=period_id,
            binding_state="unbound",
            unbound_reason=self.unbound_reason,
        )

    @model_serializer(mode="wrap")
    def _omit_unset_kpi_association(self, handler, info):
        data = handler(self)
        if "kpi_association" not in self.model_fields_set and isinstance(data, dict):
            data.pop("kpi_association", None)
        return data


class AnnualEvidenceWindow(_CoverageContract):
    """DEC-07 parameter used to construct, never infer, the denominator."""

    family_id: str = Field(min_length=1)
    period_ids: tuple[str, ...] = Field(min_length=1)

    @model_validator(mode="after")
    def _unique_periods(self) -> "AnnualEvidenceWindow":
        if len(self.period_ids) != len(set(self.period_ids)):
            raise ValueError("evidence-window periods must be unique")
        return self


class ValuationPeriodLabel(_CoverageContract):
    """Resolved profile label for one valuation time-axis basis."""

    fiscal_basis: str = Field(min_length=1)
    fiscal_year: int = Field(ge=1900, le=9999)
    period_id: str = Field(min_length=1)
    display_label: str = Field(min_length=1)


class IssuerFiscalCalendarProfile(_CoverageContract):
    """Issuer fiscal-year end used to materialize profile valuation periods."""

    ticker: str
    fiscal_year_end_month: int = Field(ge=1, le=12)
    fiscal_year_end_day: int = Field(ge=1, le=31)

    @field_validator("ticker", mode="before")
    @classmethod
    def _ticker(cls, value: object) -> str:
        return normalize_contract_ticker(value)


class DerivedKpiInput(_CoverageContract):
    required_role: str = Field(min_length=1)
    period_offset: int = 0


class DerivedKpiStrategy(_CoverageContract):
    """Profile-owned registered-formula arithmetic over required roles."""

    strategy_id: str = Field(min_length=1)
    output_role: str = Field(min_length=1)
    formula: Literal[
        "arithmetic_mean",
        "multiply_then_divide",
        "subtract_remainder",
        "divide_by_average",
    ]
    inputs: tuple[DerivedKpiInput, ...] = Field(min_length=2)
    period_ids: tuple[str, ...] = Field(min_length=1)
    unit: str = Field(min_length=1)
    scale: str = Field(min_length=1)
    multiplier: Decimal = Decimal("1")
    expression: str = Field(min_length=1)
    note: str = Field(min_length=1)

    @model_validator(mode="after")
    def _formula_shape(self) -> "DerivedKpiStrategy":
        expected_inputs = {
            "arithmetic_mean": 2,
            "multiply_then_divide": 2,
            "subtract_remainder": 3,
            "divide_by_average": 3,
        }[self.formula]
        if len(self.inputs) != expected_inputs:
            raise ValueError(
                f"{self.formula} requires exactly {expected_inputs} inputs"
            )
        if len(self.period_ids) != len(set(self.period_ids)):
            raise ValueError("derived KPI periods must be unique")
        if not self.multiplier.is_finite():
            raise ValueError("derived KPI multiplier must be finite")
        return self


class ValuationRequirement(_CoverageContract):
    requirement_id: str = Field(min_length=1)
    metric: ValuationMetric
    fiscal_basis: ValuationFiscalBasis
    roster_rule: Literal["focal_and_peers", "single_observation"]
    scope: Literal["focal", "macro"] | None = None

    @model_validator(mode="after")
    def _scope_shape(self) -> "ValuationRequirement":
        if self.roster_rule == "focal_and_peers" and self.scope is not None:
            raise ValueError("peer requirements do not carry a single scope")
        if self.roster_rule == "single_observation" and self.scope is None:
            raise ValueError("single-observation requirement needs a scope")
        return self


class AnnualCoverageProfile(_CoverageContract):
    profile_id: str = Field(min_length=1)
    profile_version: str = Field(min_length=1)
    focal_ticker: str
    peer_tickers: tuple[str, ...]
    minimum_covered_peer_count: int = Field(ge=0)
    historical_windows: tuple[AnnualEvidenceWindow, ...]
    projection_period_ids: tuple[str, ...] = Field(min_length=1)
    rate_cycle_period_ids: tuple[str, ...] = Field(min_length=1)
    annual_requirements: tuple[AnnualSourceRequirement, ...] = ()
    derived_kpi_strategies: tuple[DerivedKpiStrategy, ...] = ()
    normalization_notes: tuple[str, ...] = ()
    valuation_requirements: tuple["ValuationRequirement", ...]
    valuation_fiscal_basis_ids: tuple[str, ...] = Field(min_length=1)
    selected_relative_fiscal_basis: str = Field(min_length=1)
    valuation_period_labels: tuple[ValuationPeriodLabel, ...] = Field(min_length=1)
    issuer_fiscal_calendars: tuple[IssuerFiscalCalendarProfile, ...] = Field(
        min_length=1
    )
    semantic_target_ids: tuple[str, ...] = ()
    target_control_ids: tuple[str, ...] = ()
    valuation_control_ids: tuple[str, ...] = ()
    output_binding_ids: tuple[str, ...] = ()
    required_roles: tuple[str, ...] = ()
    template_item_ids: tuple[str, ...] = ()
    registry_group_ids: tuple[str, ...] = ()
    registry_tags: tuple[str, ...] = ()
    registry_roles: tuple[str, ...] = ()
    filing_accessions: tuple[str, ...] = ()
    # IG-2 (issuer-generic plan §5.1/§5.5): the model kind's behavioral
    # declaration. Optional and default-excluded from serialization, so a
    # bundle stored before this field existed decodes unchanged and its
    # wire bytes are byte-identical to a profile that declares nothing.
    # Absence is feature-off: the generic compile arms run.
    capabilities: ModelKindCapabilities | None = None

    @field_validator("capabilities", mode="before")
    @classmethod
    def _capabilities(cls, value: object) -> object:
        # Route every payload shape through the one typed decoder so an
        # unknown key or a malformed row raises
        # `capability_declaration_invalid` with its repair target rather
        # than a generic ValidationError (issuer-generic plan §6).
        if value is None:
            return None
        return decode_model_kind_capabilities(value)

    @model_serializer(mode="wrap")
    def _exclude_absent_capabilities(self, handler):
        # Default-excluded (plan §5.6): an undeclared capability section is
        # omitted entirely, so pre-IG-2 and post-IG-2 bundles serialize
        # identically when nothing is declared and the SourceBundle epoch
        # derivation needs no arm.
        payload = handler(self)
        if self.capabilities is None:
            payload.pop("capabilities", None)
        return payload

    @field_validator("focal_ticker", mode="before")
    @classmethod
    def _focal_ticker(cls, value: object) -> str:
        return normalize_contract_ticker(value)

    @field_validator("peer_tickers", mode="before")
    @classmethod
    def _peer_tickers(cls, value: object) -> object:
        if isinstance(value, (list, tuple)):
            return tuple(normalize_contract_ticker(item) for item in value)
        return value

    @model_validator(mode="after")
    def _unique_windows(self) -> "AnnualCoverageProfile":
        for field_name in (
            "semantic_target_ids",
            "target_control_ids",
            "valuation_control_ids",
            "required_roles",
            "template_item_ids",
            "registry_group_ids",
            "registry_tags",
            "registry_roles",
            "filing_accessions",
        ):
            values = getattr(self, field_name)
            if values != tuple(sorted(set(values))) or any(
                not value.strip() for value in values
            ):
                raise ValueError(
                    f"profile {field_name} must be non-blank, sorted, and unique"
                )
        family_ids = tuple(item.family_id for item in self.historical_windows)
        if len(family_ids) != len(set(family_ids)):
            raise ValueError("historical-window family ids must be unique")
        if self.peer_tickers != tuple(sorted(set(self.peer_tickers))):
            raise ValueError("profile peer tickers must be sorted and unique")
        if self.focal_ticker in self.peer_tickers:
            raise ValueError("profile peer roster must exclude the focal ticker")
        if self.minimum_covered_peer_count > len(self.peer_tickers):
            raise ValueError("minimum covered peers exceeds the profile roster")
        if (
            self.selected_relative_fiscal_basis != "next_four_quarters"
            and self.selected_relative_fiscal_basis
            not in self.valuation_fiscal_basis_ids
        ):
            raise ValueError(
                "selected relative basis must belong to the valuation axis"
            )
        labels = {item.fiscal_basis: item for item in self.valuation_period_labels}
        if len(labels) != len(self.valuation_period_labels) or set(labels) != set(
            self.valuation_fiscal_basis_ids
        ):
            raise ValueError("valuation labels must exactly cover the profile axis")
        ordered_years = tuple(
            labels[basis].fiscal_year for basis in self.valuation_fiscal_basis_ids
        )
        if ordered_years != tuple(sorted(set(ordered_years))):
            raise ValueError("valuation fiscal years must be unique and increasing")
        calendars = {item.ticker: item for item in self.issuer_fiscal_calendars}
        expected_issuers = {self.focal_ticker, *self.peer_tickers}
        if (
            len(calendars) != len(self.issuer_fiscal_calendars)
            or set(calendars) != expected_issuers
        ):
            raise ValueError(
                "issuer fiscal calendars must exactly cover focal and peers"
            )
        for calendar in calendars.values():
            for fiscal_year in ordered_years:
                try:
                    date(
                        fiscal_year,
                        calendar.fiscal_year_end_month,
                        calendar.fiscal_year_end_day,
                    )
                except ValueError as exc:
                    raise ValueError(
                        "issuer fiscal calendar contains an invalid month/day"
                    ) from exc
        requirement_ids = tuple(
            item.requirement_id for item in self.valuation_requirements
        )
        if len(requirement_ids) != len(set(requirement_ids)):
            raise ValueError("profile valuation requirement IDs must be unique")
        if any(
            item.fiscal_basis not in {"point_in_time", "next_four_quarters"}
            and item.fiscal_basis not in self.valuation_fiscal_basis_ids
            for item in self.valuation_requirements
        ):
            raise ValueError("valuation requirement basis is outside the profile axis")
        annual_requirement_ids = tuple(
            item.requirement_id for item in self.annual_requirements
        )
        if len(annual_requirement_ids) != len(set(annual_requirement_ids)):
            raise ValueError("profile annual requirement IDs must be unique")
        strategy_ids = tuple(item.strategy_id for item in self.derived_kpi_strategies)
        output_ids = tuple(item.output_role for item in self.derived_kpi_strategies)
        if len(strategy_ids) != len(set(strategy_ids)):
            raise ValueError("profile derived KPI strategy IDs must be unique")
        if len(output_ids) != len(set(output_ids)):
            raise ValueError("profile derived KPI outputs must be unique")
        return self

    @property
    def role_dispositions(self) -> "RoleDispositionTable":
        """The complete per-(role, period) disposition table, derived on every read.

        Derived from ``annual_requirements`` (the pin) rather than stored, so a
        re-pinned profile and a profile stored before the table existed both
        answer from their own rows and no serialized payload changes.
        """

        from .role_disposition import derive_role_dispositions

        return derive_role_dispositions(self)


__all__ = [
    "AnnualCoverageProfile",
    "SUPPORTED_COVERAGE_DISPOSITIONS",
    "AnnualSourceRequirement",
    "KpiAssociation",
    "PeriodPinCell",
    "RetainedVintageCandidate",
    "AbsenceCitation",
    "ValuationRequirement",
    "AnnualEvidenceWindow",
    "ValuationPeriodLabel",
    "IssuerFiscalCalendarProfile",
    "DerivedKpiInput",
    "DerivedKpiStrategy",
    "SourceRequirementFamily",
    "ValuationMetric",
    "ValuationFiscalBasis",
]
