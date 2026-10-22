"""Immutable precompile ValuationPolicy judgment and deterministic projection."""

from __future__ import annotations

from datetime import date, datetime
from decimal import Decimal, ROUND_HALF_EVEN
import hashlib
import secrets
import weakref
from typing import Any, Literal, Mapping, Sequence

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_serializer,
    model_validator,
)

from .analysis import ExitEarningsMeasure
from .build_model_product_contracts import (
    EDGAR_API_EV_RESPONSE_ARTIFACT_KINDS,
    EDGAR_API_EV_RESPONSE_PAIR_FIELDS,
    FiscalPeriodFacts,
    HistoricalBasisFact,
    MarketCalendarFacts,
    ValuationCoverage,
    ValuationObservation,
    ValuationNetShareDilutionRates,
    validate_historical_reconciliation_source_bindings,
)
from .coverage_profile import ValuationRequirement
from .business_model_revision_ref import BusinessModelRevisionRef
from .forecast_assumption_set import ForecastAssumptionSetRef
from .forecast_control_profile import ForecastControlProfileRef
from .historical_basis_snapshot import fiscal_axis_digest
from .historical_foundation import HistoricalFoundationRef
from .model_spec_authoring import (
    ModelValuationPolicyAuthoring,
    ValuationControlAuthoring,
    ValuationFamilyAuthorizationAuthoring,
)
from .model_state_identity import validate_sha256_hex
from .scenario_set import ScenarioSetRef
from .stable_control_identity import FiscalAxis
from industry_slice_core.thesis_shared_sources import SourceId, SourceRecord
from .ticker_contract import normalize_ticker
# The snapshot's decode-time source-binding check is the one Engine import of
# this Store module (plan §2.7 known violations; removed by extending R10).
from .valuation_evidence_validation import validate_valuation_evidence_source_bindings
from .versioned_artifact import canonical_json_bytes


_RATE_QUANTUM = Decimal("0.000000000001")
_MULTIPLE_QUANTUM = Decimal("0.000001")
_BLOOMBERG_STYLE_BETA_WEIGHT = Decimal("0.67")
_MARKET_BETA_ANCHOR = Decimal("1")
_REQUIRED_OBSERVED_INPUTS = (
    "equity_risk_premium",
    "raw_beta",
    "risk_free_rate",
    "sofr_rate",
    "stock_price",
)
_REQUIRED_DERIVED_INPUTS = ("adjusted_beta", "cost_of_equity")


class _Contract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


def _digest(domain: str, value: object) -> str:
    return hashlib.sha256(
        domain.encode("utf-8") + b"\0" + canonical_json_bytes(value)
    ).hexdigest()


def _require_aware(value: datetime, *, field_name: str | None) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


def _canonical_strings(
    value: tuple[str, ...],
    *,
    field_name: str,
) -> tuple[str, ...]:
    if value != tuple(sorted(set(value))) or any(not item for item in value):
        raise ValueError(f"{field_name} must be sorted, unique, and nonblank")
    return value


def _canonical_decimal(value: Decimal, *, quantum: Decimal) -> Decimal:
    result = Decimal(value).quantize(quantum, rounding=ROUND_HALF_EVEN)
    if not result.is_finite():
        raise ValueError("valuation numeric values must be finite")
    return result


class ValuationPolicyRef(_Contract):
    schema_version: Literal["valuation-policy-ref.v1"] = "valuation-policy-ref.v1"
    policy_id: str = Field(pattern=r"^vpl_[0-9a-f]{32}$")
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _content_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")


def issue_valuation_policy_id() -> str:
    return f"vpl_{secrets.token_hex(16)}"


class ValuationMethodologyPin(_Contract):
    schema_version: Literal["valuation-methodology-pin.v1"] = (
        "valuation-methodology-pin.v1"
    )
    name: Literal["valuation-policy"] = "valuation-policy"
    declared_version: str = Field(min_length=1)
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _content_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")


def _valuation_referenced_source_ids(
    *,
    observations: Sequence[ValuationObservation],
    fiscal_calendar: Sequence[FiscalPeriodFacts],
    historical_basis_facts: Sequence[HistoricalBasisFact],
    source_by_id: Mapping[str, SourceRecord],
) -> set[str]:
    """Resolve the exact direct and source-owned parent closure for a snapshot."""

    referenced_source_ids = (
        {str(row.source_id) for row in observations}
        | {
            str(row.raw_close_source_id)
            for row in observations
            if row.raw_close_source_id is not None
        }
        | {
            str(row.definition_evidence.provider_doc_source_id)
            for row in observations
            if row.definition_evidence is not None
        }
        | {
            str(row.ntm_operands.reported_anchor.source_id)
            for row in observations
            if row.ntm_operands is not None
        }
        | {
            str(source_id)
            for row in observations
            if row.ntm_operands is not None
            for source_id in (
                row.ntm_operands.reported_anchor.selection_source_ids or ()
            )
        }
        | {
            str(row.forward_adjusted_ebitda_operands.reported_anchor.source_id)
            for row in observations
            if row.forward_adjusted_ebitda_operands is not None
        }
        | {
            str(source_id)
            for row in observations
            if row.forward_adjusted_ebitda_operands is not None
            for source_id in (
                row.forward_adjusted_ebitda_operands.reported_anchor.selection_source_ids
                or ()
            )
        }
        | {
            str(
                row.ntm_forward_adjusted_ebitda_operands.reported_anchor.source_id
            )
            for row in observations
            if row.ntm_forward_adjusted_ebitda_operands is not None
        }
        | {
            str(source_id)
            for row in observations
            if row.ntm_forward_adjusted_ebitda_operands is not None
            for source_id in (
                row.ntm_forward_adjusted_ebitda_operands.reported_anchor.selection_source_ids
                or ()
            )
        }
        | {
            str(quarter.fiscal_source_id)
            for row in observations
            if row.ntm_forward_adjusted_ebitda_operands is not None
            for quarter in (
                row.ntm_forward_adjusted_ebitda_operands.quarterly_adjusted_ebitda
            )
        }
        | {
            str(quarter.fiscal_source_id)
            for row in observations
            if row.ntm_operands is not None
            for quarter in row.ntm_operands.quarterly_eps
        }
        | {
            str(row.split_history_source_id)
            for row in observations
            if row.split_history_source_id is not None
        }
        | {
            str(row.currency_source_id)
            for row in observations
            if row.currency_source_id is not None
        }
        | {str(row.source_id) for row in fiscal_calendar if row.source_id is not None}
        | {str(row.source_id) for row in historical_basis_facts}
        | {
            str(source_id)
            for row in historical_basis_facts
            if row.reconciliation is not None
            for source_id in row.reconciliation.raw_parent_source_ids
        }
        | {
            str(ref.source_id)
            for row in historical_basis_facts
            if row.reconciliation is not None
            for ref in row.reconciliation.definition_refs
        }
    )
    pending = list(referenced_source_ids)
    while pending:
        source = source_by_id.get(pending.pop())
        if source is None:
            continue
        key_fields = source.key_fields or {}
        if (
            key_fields.get("artifact_kind")
            not in EDGAR_API_EV_RESPONSE_ARTIFACT_KINDS
        ):
            continue
        # Each EV response capture names both captures of its filing, so a
        # cover-facts-sourced shares row still reaches the financials response
        # the binder reads filing acceptance from.
        for field_name in EDGAR_API_EV_RESPONSE_PAIR_FIELDS:
            paired_id = key_fields.get(field_name)
            if not isinstance(paired_id, str) or not paired_id:
                raise ValueError(
                    "valuation EV response capture must name both response captures"
                )
            if paired_id not in referenced_source_ids:
                referenced_source_ids.add(paired_id)
                pending.append(paired_id)
    return referenced_source_ids


# A snapshot is frozen, so an instance that passed ``_canonical_snapshot`` stays
# valid. Pydantic runs the after-validator again whenever the instance is embedded
# in a ValuationPolicyArtifact (``ValuationPolicyArtifact.model_validate`` of a
# policy built on it): 0.13 s of source-binding checks over a 6 MB snapshot each
# time. Entries die with their instance.
_CANONICAL_SNAPSHOTS: "weakref.WeakValueDictionary[int, ValuationEvidenceSnapshot]" = (
    weakref.WeakValueDictionary()
)


class ValuationEvidenceSnapshot(_Contract):
    """Code-owned observed valuation inputs before analyst policy is applied."""

    schema_version: Literal[
        "valuation-evidence-snapshot.v2",
        "valuation-evidence-snapshot.v3",
        "valuation-evidence-snapshot.v4",
    ] = "valuation-evidence-snapshot.v2"
    cutoff_at: datetime
    retrieved_at: datetime
    requirements: tuple[ValuationRequirement, ...]
    observations: tuple[ValuationObservation, ...]
    coverage: tuple[ValuationCoverage, ...]
    market_calendar: MarketCalendarFacts
    fiscal_calendar: tuple[FiscalPeriodFacts, ...]
    historical_basis_facts: tuple[HistoricalBasisFact, ...] = Field(
        default=(),
        exclude_if=lambda value: not value,
    )
    sources: tuple[SourceRecord, ...]
    snapshot_sha256: str

    @field_validator("cutoff_at", "retrieved_at")
    @classmethod
    def _timestamp(cls, value: datetime, info: ValidationInfo) -> datetime:
        return _require_aware(value, field_name=info.field_name)

    @field_validator("snapshot_sha256")
    @classmethod
    def _snapshot_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="snapshot_sha256")

    @model_validator(mode="after")
    def _canonical_snapshot(self) -> "ValuationEvidenceSnapshot":
        if _CANONICAL_SNAPSHOTS.get(id(self)) is self:
            return self
        if tuple(row.requirement_id for row in self.requirements) != tuple(
            sorted({row.requirement_id for row in self.requirements})
        ):
            raise ValueError("valuation requirements must be sorted and unique")
        if tuple(row.observation_id for row in self.observations) != tuple(
            sorted({row.observation_id for row in self.observations})
        ):
            raise ValueError("valuation observations must be sorted and unique")
        if tuple(row.requirement_id for row in self.coverage) != tuple(
            sorted({row.requirement_id for row in self.coverage})
        ):
            raise ValueError("valuation coverage must be sorted and unique")
        if {row.requirement_id for row in self.coverage} != {
            row.requirement_id for row in self.requirements
        }:
            raise ValueError(
                "valuation coverage must equal the requirement denominator"
            )
        if self.cutoff_at > self.retrieved_at:
            raise ValueError("valuation evidence cutoff cannot postdate retrieval")
        source_by_id = {str(row.id): row for row in self.sources}
        referenced_source_ids = _valuation_referenced_source_ids(
            observations=self.observations,
            fiscal_calendar=self.fiscal_calendar,
            historical_basis_facts=self.historical_basis_facts,
            source_by_id=source_by_id,
        )
        expected_schema_version = (
            "valuation-evidence-snapshot.v4"
            if any(
                row.forward_adjusted_ebitda_operands is not None
                or row.ntm_forward_adjusted_ebitda_operands is not None
                for row in self.observations
            )
            else "valuation-evidence-snapshot.v3"
            if any(row.component_type is not None for row in self.observations)
            else "valuation-evidence-snapshot.v2"
        )
        if self.schema_version != expected_schema_version:
            raise ValueError(
                "valuation evidence snapshot version must match its dated-EV shape"
            )
        if tuple(row.id for row in self.sources) != tuple(
            sorted(referenced_source_ids)
        ):
            raise ValueError(
                "valuation evidence sources must exactly equal sorted referenced source ids"
            )
        validate_valuation_evidence_source_bindings(
            observations=self.observations,
            fiscal_calendar=self.fiscal_calendar,
            sources=self.sources,
            cutoff_at=self.cutoff_at,
            retrieved_at=self.retrieved_at,
            historical_basis_facts=self.historical_basis_facts,
        )
        validate_historical_reconciliation_source_bindings(
            self.historical_basis_facts,
            self.sources,
            allow_reconciliations=self.schema_version
            == "valuation-evidence-snapshot.v4",
        )
        if self.snapshot_sha256 != valuation_evidence_snapshot_digest(self):
            raise ValueError("valuation evidence snapshot digest mismatch")
        _CANONICAL_SNAPSHOTS[id(self)] = self
        return self


def valuation_evidence_snapshot_digest(
    value: ValuationEvidenceSnapshot | Mapping[str, Any],
) -> str:
    payload = (
        value.model_dump(mode="json")
        if isinstance(value, ValuationEvidenceSnapshot)
        else dict(value)
    )
    payload.pop("snapshot_sha256", None)
    schema_version = payload.get("schema_version")
    if schema_version not in {
        "valuation-evidence-snapshot.v2",
        "valuation-evidence-snapshot.v3",
        "valuation-evidence-snapshot.v4",
    }:
        raise ValueError("unsupported valuation evidence snapshot version")
    if not payload.get("historical_basis_facts"):
        # The digest preimage mirrors the wire form exactly. The field is
        # excluded when empty, which is every v2/v3 snapshot and every v4
        # snapshot whose forward denominator binds no issuer reconciliation.
        payload.pop("historical_basis_facts", None)
    calendar = payload["market_calendar"]
    market_calendar = (
        calendar
        if isinstance(calendar, MarketCalendarFacts)
        else MarketCalendarFacts.model_validate_json(canonical_json_bytes(calendar))
    )
    payload["market_calendar"] = market_calendar.model_dump(mode="json")
    return _digest(str(schema_version), payload)


def build_valuation_evidence_snapshot(
    *,
    cutoff_at: datetime,
    retrieved_at: datetime,
    requirements: Sequence[ValuationRequirement],
    observations: Sequence[ValuationObservation],
    coverage: Sequence[ValuationCoverage],
    market_calendar: MarketCalendarFacts,
    fiscal_calendar: Sequence[FiscalPeriodFacts],
    sources: Sequence[SourceRecord],
    historical_basis_facts: Sequence[HistoricalBasisFact] = (),
) -> ValuationEvidenceSnapshot:
    source_by_id = {source.id: source for source in sources}
    if len(source_by_id) != len(sources):
        raise ValueError("valuation evidence source ids must be unique")
    if len({row.fact_id for row in historical_basis_facts}) != len(
        historical_basis_facts
    ):
        raise ValueError("valuation historical-basis fact ids must be unique")
    # The forward EV denominator is a provider consensus estimate, so no issuer
    # reconciliation is bound and none is retained here. Issuer-reported
    # adjusted EBITDA keeps its own retention through the historical rails.
    referenced_source_ids = _valuation_referenced_source_ids(
        observations=observations,
        fiscal_calendar=fiscal_calendar,
        historical_basis_facts=(),
        source_by_id={str(key): value for key, value in source_by_id.items()},
    )
    missing_source_ids = referenced_source_ids - source_by_id.keys()
    if missing_source_ids:
        raise ValueError(
            "valuation evidence source_id must resolve: "
            + ", ".join(sorted(missing_source_ids))
        )
    referenced_sources = tuple(
        source_by_id[source_id] for source_id in sorted(referenced_source_ids)
    )
    schema_version = (
        "valuation-evidence-snapshot.v4"
        if any(
            row.forward_adjusted_ebitda_operands is not None
            or row.ntm_forward_adjusted_ebitda_operands is not None
            for row in observations
        )
        else "valuation-evidence-snapshot.v3"
        if any(row.component_type is not None for row in observations)
        else "valuation-evidence-snapshot.v2"
    )
    payload = {
        "schema_version": schema_version,
        "cutoff_at": cutoff_at,
        "retrieved_at": retrieved_at,
        "requirements": tuple(sorted(requirements, key=lambda row: row.requirement_id)),
        "observations": tuple(sorted(observations, key=lambda row: row.observation_id)),
        "coverage": tuple(sorted(coverage, key=lambda row: row.requirement_id)),
        "market_calendar": market_calendar,
        "fiscal_calendar": tuple(
            sorted(
                fiscal_calendar,
                key=lambda row: (row.issuer, row.basis, row.period_end),
            )
        ),
        "sources": referenced_sources,
    }
    return ValuationEvidenceSnapshot.model_validate(
        {**payload, "snapshot_sha256": valuation_evidence_snapshot_digest(payload)}
    )


ValuationObservedInputName = Literal[
    "stock_price",
    "raw_beta",
    "risk_free_rate",
    "equity_risk_premium",
    "sofr_rate",
]


class ValuationObservedInput(_Contract):
    """One observed input retained independently from session-issued handles."""

    schema_version: Literal["valuation-observed-input.v1"] = (
        "valuation-observed-input.v1"
    )
    input_name: ValuationObservedInputName
    value: Decimal
    unit: Literal["usd_per_share", "ratio"]
    observation_id: str = Field(min_length=1)
    source_id: SourceId
    provider: str = Field(min_length=1)
    locator: str = Field(min_length=1)
    as_of_date: date
    retrieved_at: datetime
    content_sha256: str

    @field_validator("value")
    @classmethod
    def _value(cls, value: Decimal) -> Decimal:
        return _canonical_decimal(value, quantum=_RATE_QUANTUM)

    @field_validator("retrieved_at")
    @classmethod
    def _retrieved_at(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="retrieved_at")

    @field_validator("content_sha256")
    @classmethod
    def _content_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")

    @model_validator(mode="after")
    def _semantic_unit(self) -> "ValuationObservedInput":
        expected = "usd_per_share" if self.input_name == "stock_price" else "ratio"
        if self.unit != expected:
            raise ValueError("observed valuation input unit does not match its name")
        if self.input_name == "stock_price":
            valid = self.value > 0
        elif self.input_name == "raw_beta":
            valid = Decimal("0") < self.value <= Decimal("4")
        else:
            valid = Decimal("0") < self.value <= Decimal("0.25")
        if not valid:
            raise ValueError("observed valuation input is outside its admissible range")
        return self


class ValuationPolicyInput(_Contract):
    schema_version: Literal["valuation-policy-input.v1"] = "valuation-policy-input.v1"
    input_name: Literal["beta_floor"] = "beta_floor"
    value: Decimal
    unit: Literal["ratio"] = "ratio"
    rationale: str = Field(min_length=1)
    evidence_ids: tuple[str, ...] = ()

    @field_validator("value")
    @classmethod
    def _value(cls, value: Decimal) -> Decimal:
        result = _canonical_decimal(value, quantum=_RATE_QUANTUM)
        if not Decimal("0") < result <= Decimal("4"):
            raise ValueError("beta floor must be a positive ratio no greater than four")
        return result

    @field_validator("evidence_ids")
    @classmethod
    def _evidence_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _canonical_strings(value, field_name="policy input evidence ids")


class ValuationDerivedInput(_Contract):
    schema_version: Literal["valuation-derived-input.v1"] = "valuation-derived-input.v1"
    input_name: Literal["adjusted_beta", "cost_of_equity"]
    value: Decimal
    unit: Literal["ratio"] = "ratio"
    formula: Literal[
        "max(0.67 * raw_beta + 0.33, beta_floor)",
        "risk_free_rate + adjusted_beta * equity_risk_premium",
    ]
    input_names: tuple[str, ...] = Field(min_length=2)

    @field_validator("value")
    @classmethod
    def _value(cls, value: Decimal) -> Decimal:
        return _canonical_decimal(value, quantum=_RATE_QUANTUM)

    @field_validator("input_names")
    @classmethod
    def _input_names(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _canonical_strings(value, field_name="derived input names")

    @model_validator(mode="after")
    def _formula_matches_name(self) -> "ValuationDerivedInput":
        expected = {
            "adjusted_beta": (
                "max(0.67 * raw_beta + 0.33, beta_floor)",
                ("beta_floor", "raw_beta"),
            ),
            "cost_of_equity": (
                "risk_free_rate + adjusted_beta * equity_risk_premium",
                ("adjusted_beta", "equity_risk_premium", "risk_free_rate"),
            ),
        }[self.input_name]
        if (self.formula, self.input_names) != expected:
            raise ValueError("derived valuation input formula does not match its name")
        return self


class ValuationCaseValues(_Contract):
    """One bull/base/bear case set. A set that does not run bull > base > bear
    is the build's declinable ``scenario_case_order_inverted`` warning, not a
    refusal (``schema/build_findings.py``)."""

    schema_version: Literal["valuation-case-values.v1"] = "valuation-case-values.v1"
    bull: Decimal
    base: Decimal
    bear: Decimal

    @field_validator("bull", "base", "bear")
    @classmethod
    def _values(cls, value: Decimal) -> Decimal:
        return _canonical_decimal(value, quantum=_MULTIPLE_QUANTUM)


class ValuationTerminalPolicy(_Contract):
    schema_version: Literal["valuation-terminal-policy.v1"] = (
        "valuation-terminal-policy.v1"
    )
    terminal_growth: ValuationCaseValues
    exit_multiple: ValuationCaseValues
    exit_earnings_measure: ExitEarningsMeasure = "terminal_fcf"
    rationale: str = Field(min_length=1)
    evidence_ids: tuple[str, ...] = ()
    limitations: str = Field(min_length=1)

    @field_validator("evidence_ids")
    @classmethod
    def _evidence_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _canonical_strings(value, field_name="terminal policy evidence ids")

    @model_validator(mode="after")
    def _terminal_ranges(self) -> "ValuationTerminalPolicy":
        if not all(
            Decimal("-0.05") <= value <= Decimal("0.10")
            for value in (
                self.terminal_growth.bull,
                self.terminal_growth.base,
                self.terminal_growth.bear,
            )
        ):
            raise ValueError("terminal growth cases must be decimal rates")
        if not all(
            Decimal("0") < value <= Decimal("100")
            for value in (
                self.exit_multiple.bull,
                self.exit_multiple.base,
                self.exit_multiple.bear,
            )
        ):
            raise ValueError("exit multiple cases must be positive and bounded")
        return self

    @model_serializer(mode="wrap")
    def _omit_default_exit_measure(self, handler):
        payload = handler(self)
        if isinstance(payload, dict) and payload.get("exit_earnings_measure") == "terminal_fcf":
            payload.pop("exit_earnings_measure", None)
        return payload


class ValuationNetShareDilutionRate(_Contract):
    """One forecast period's annual net share dilution, as a decimal rate."""

    schema_version: Literal["valuation-net-share-dilution-rate.v1"] = (
        "valuation-net-share-dilution-rate.v1"
    )
    period_id: str = Field(min_length=1)
    rate: Decimal

    @field_validator("rate")
    @classmethod
    def _rate(cls, value: Decimal) -> Decimal:
        result = _canonical_decimal(value, quantum=_RATE_QUANTUM)
        if not Decimal("-0.20") <= result <= Decimal("0.20"):
            raise ValueError("net share dilution rates must be decimal annual rates")
        return result


class ValuationNetShareDilution(_Contract):
    """The judge[parameter] net share dilution: the diluted-share y/y change.

    One knob, net of repurchases: grants and option exercise add shares, the
    buyback takes them back, and the rate is what remains.

    The one writer of the net-dilution rate row the share curves compound
    on; a forecast period it leaves without a rate compiles the house value
    (the latest reported diluted-share change) as ``default``.
    """

    schema_version: Literal["valuation-net-share-dilution.v1"] = (
        "valuation-net-share-dilution.v1"
    )
    rates: tuple[ValuationNetShareDilutionRate, ...] = Field(min_length=1)
    rationale: str = Field(min_length=1)
    evidence_ids: tuple[str, ...] = ()
    limitations: str = Field(min_length=1)

    @field_validator("evidence_ids")
    @classmethod
    def _evidence_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _canonical_strings(value, field_name="net share dilution evidence ids")

    @model_validator(mode="after")
    def _canonical_rates(self) -> "ValuationNetShareDilution":
        period_ids = tuple(row.period_id for row in self.rates)
        if period_ids != tuple(sorted(set(period_ids))):
            raise ValueError(
                "net share dilution rates must be sorted and unique by period_id"
            )
        return self


class ValuationPolicyGap(_Contract):
    schema_version: Literal["valuation-policy-gap.v1"] = "valuation-policy-gap.v1"
    key: str = Field(min_length=1)
    blocking: bool
    rationale: str = Field(min_length=1)
    repair_target: str = Field(min_length=1)


class ValuationPolicyTerminalIssue(_Contract):
    schema_version: Literal["valuation-policy-terminal-issue.v1"] = (
        "valuation-policy-terminal-issue.v1"
    )
    key: str = Field(min_length=1)
    blocking: bool
    rationale: str = Field(min_length=1)
    repair_target: str = Field(min_length=1)


class ValuationPolicyTerminalSummary(_Contract):
    schema_version: Literal["valuation-policy-terminal-summary.v1"] = (
        "valuation-policy-terminal-summary.v1"
    )
    summary: str = Field(min_length=1)
    decision: str = Field(min_length=1)
    confidence: Literal["high", "medium", "low"]
    limitations: tuple[str, ...]
    issues: tuple[ValuationPolicyTerminalIssue, ...] = ()
    recommended_next_stage: Literal["build-model", "valuation-policy"]

    @field_validator("limitations")
    @classmethod
    def _limitations(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _canonical_strings(value, field_name="terminal limitations")


class ValuationPolicyStageScope(_Contract):
    schema_version: Literal["valuation-policy-stage-scope.v1"] = (
        "valuation-policy-stage-scope.v1"
    )
    user_id: str = Field(min_length=1)
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1)

    @field_validator("ticker")
    @classmethod
    def _ticker(cls, value: str) -> str:
        normalized = normalize_ticker(value)
        if value != normalized:
            raise ValueError("ticker must be canonical")
        return normalized


class ValuationPolicyStageReceipt(_Contract):
    schema_version: Literal["valuation-policy-stage-receipt.v1"] = (
        "valuation-policy-stage-receipt.v1"
    )
    status: Literal["accepted", "incomplete", "blocked"]
    scope: ValuationPolicyStageScope
    valuation_policy_ref: ValuationPolicyRef
    forecast_assumption_set_ref: ForecastAssumptionSetRef
    forecast_assumption_stage_receipt_digest: str
    scenario_set_ref: ScenarioSetRef
    scenario_stage_receipt_digest: str
    valuation_evidence_snapshot_sha256: str
    started_at: datetime
    completed_at: datetime
    receipt_digest: str

    @field_validator(
        "forecast_assumption_stage_receipt_digest",
        "scenario_stage_receipt_digest",
        "valuation_evidence_snapshot_sha256",
        "receipt_digest",
    )
    @classmethod
    def _digests(cls, value: str, info: ValidationInfo) -> str:
        return validate_sha256_hex(value, field_name=info.field_name)

    @field_validator("started_at", "completed_at")
    @classmethod
    def _times(cls, value: datetime, info: ValidationInfo) -> datetime:
        return _require_aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _receipt(self) -> "ValuationPolicyStageReceipt":
        if self.completed_at < self.started_at:
            raise ValueError(
                "valuation policy receipt completed_at precedes started_at"
            )
        if self.receipt_digest != valuation_policy_stage_receipt_digest(self):
            raise ValueError("valuation policy stage receipt digest mismatch")
        return self


def valuation_policy_stage_receipt_digest(
    value: ValuationPolicyStageReceipt | Mapping[str, Any],
) -> str:
    payload = (
        value.model_dump(mode="json")
        if isinstance(value, ValuationPolicyStageReceipt)
        else dict(value)
    )
    payload.pop("receipt_digest", None)
    return _digest("valuation-policy-stage-receipt.v1", payload)


def build_valuation_policy_stage_receipt(
    *,
    status: Literal["accepted", "incomplete", "blocked"],
    scope: ValuationPolicyStageScope,
    valuation_policy_ref: ValuationPolicyRef,
    forecast_assumption_set_ref: ForecastAssumptionSetRef,
    forecast_assumption_stage_receipt_digest: str,
    scenario_set_ref: ScenarioSetRef,
    scenario_stage_receipt_digest: str,
    valuation_evidence_snapshot_sha256: str,
    started_at: datetime,
    completed_at: datetime,
) -> ValuationPolicyStageReceipt:
    payload = {
        "schema_version": "valuation-policy-stage-receipt.v1",
        "status": status,
        "scope": scope,
        "valuation_policy_ref": valuation_policy_ref,
        "forecast_assumption_set_ref": forecast_assumption_set_ref,
        "forecast_assumption_stage_receipt_digest": (
            forecast_assumption_stage_receipt_digest
        ),
        "scenario_set_ref": scenario_set_ref,
        "scenario_stage_receipt_digest": scenario_stage_receipt_digest,
        "valuation_evidence_snapshot_sha256": valuation_evidence_snapshot_sha256,
        "started_at": started_at,
        "completed_at": completed_at,
    }
    return ValuationPolicyStageReceipt.model_validate(
        {**payload, "receipt_digest": valuation_policy_stage_receipt_digest(payload)}
    )


class ValuationPolicyArtifact(_Contract):
    schema_version: Literal["valuation-policy.v1"] = "valuation-policy.v1"
    user_id: str = Field(min_length=1)
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1)
    ref: ValuationPolicyRef
    status: Literal["accepted", "incomplete", "blocked"]
    forecast_assumption_set_ref: ForecastAssumptionSetRef
    forecast_assumption_stage_receipt_digest: str
    scenario_set_ref: ScenarioSetRef
    scenario_stage_receipt_digest: str
    business_model_revision_ref: BusinessModelRevisionRef
    business_model_selection_epoch: int = Field(gt=0)
    business_model_stage_receipt_digest: str
    historical_foundation_ref: HistoricalFoundationRef
    fiscal_axis: FiscalAxis
    fiscal_axis_sha256: str
    forecast_control_profile_ref: ForecastControlProfileRef
    forecast_control_denominator_sha256: str
    evidence_snapshot: ValuationEvidenceSnapshot
    methodology: ValuationMethodologyPin
    observed_inputs: tuple[ValuationObservedInput, ...] = ()
    policy_inputs: tuple[ValuationPolicyInput, ...] = ()
    derived_inputs: tuple[ValuationDerivedInput, ...] = ()
    terminal_policy: ValuationTerminalPolicy | None = None
    # Absent on a policy that leaves the net dilution to the house value and
    # on every policy persisted before the parameter existed.
    net_share_dilution: ValuationNetShareDilution | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    model_policy: ModelValuationPolicyAuthoring | None = None
    family_authorizations: tuple[ValuationFamilyAuthorizationAuthoring, ...] = ()
    gaps: tuple[ValuationPolicyGap, ...] = ()
    terminal_summary: ValuationPolicyTerminalSummary
    created_by: str = Field(min_length=1)
    created_at: datetime
    # The Revision link (``schema/judgment_revision.py``): absent on a first
    # policy and on every policy persisted before it.
    predecessor_ref: ValuationPolicyRef | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    stage_receipt: ValuationPolicyStageReceipt

    @field_validator("ticker")
    @classmethod
    def _ticker(cls, value: str) -> str:
        normalized = normalize_ticker(value)
        if value != normalized:
            raise ValueError("ticker must be canonical")
        return normalized

    @field_validator(
        "forecast_assumption_stage_receipt_digest",
        "scenario_stage_receipt_digest",
        "business_model_stage_receipt_digest",
        "fiscal_axis_sha256",
        "forecast_control_denominator_sha256",
    )
    @classmethod
    def _digests(cls, value: str, info: ValidationInfo) -> str:
        return validate_sha256_hex(value, field_name=info.field_name)

    @field_validator("created_at")
    @classmethod
    def _created_at(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="created_at")

    @model_validator(mode="after")
    def _artifact(self) -> "ValuationPolicyArtifact":
        if self.business_model_revision_ref.business_model_id != (
            f"{self.ticker}_business_model"
        ):
            raise ValueError("BusinessModel revision ref does not match policy ticker")
        if self.fiscal_axis_sha256 != fiscal_axis_digest(self.fiscal_axis):
            raise ValueError("valuation policy fiscal axis digest mismatch")

        observed_names = tuple(row.input_name for row in self.observed_inputs)
        if observed_names != tuple(sorted(set(observed_names))):
            raise ValueError("observed valuation inputs must be sorted and unique")
        policy_names = tuple(row.input_name for row in self.policy_inputs)
        if policy_names != tuple(sorted(set(policy_names))):
            raise ValueError("valuation policy inputs must be sorted and unique")
        derived_names = tuple(row.input_name for row in self.derived_inputs)
        if derived_names != tuple(sorted(set(derived_names))):
            raise ValueError("derived valuation inputs must be sorted and unique")
        family_methods = tuple(row.method for row in self.family_authorizations)
        if family_methods != tuple(sorted(set(family_methods))):
            raise ValueError(
                "valuation family authorizations must be sorted and unique"
            )
        gap_keys = tuple(row.key for row in self.gaps)
        if gap_keys != tuple(sorted(set(gap_keys))):
            raise ValueError("valuation policy gaps must be sorted and unique")

        observed = {row.input_name: row for row in self.observed_inputs}
        policy = {row.input_name: row for row in self.policy_inputs}
        derived = {row.input_name: row for row in self.derived_inputs}
        if set(observed) == set(_REQUIRED_OBSERVED_INPUTS) and "beta_floor" in policy:
            expected_adjusted = derive_adjusted_beta(
                raw_beta=observed["raw_beta"].value,
                beta_floor=policy["beta_floor"].value,
            )
            expected_cost = derive_cost_of_equity(
                risk_free_rate=observed["risk_free_rate"].value,
                adjusted_beta=expected_adjusted,
                equity_risk_premium=observed["equity_risk_premium"].value,
            )
            expected = {
                "adjusted_beta": expected_adjusted,
                "cost_of_equity": expected_cost,
            }
            if set(derived) == set(expected):
                for name, value in expected.items():
                    if derived[name].value != value:
                        raise ValueError(f"derived valuation input {name} is stale")

        retained_evidence = (
            {row.observation_id for row in self.observed_inputs}
            | {row.content_sha256 for row in self.observed_inputs}
            | {row.observation_id for row in self.evidence_snapshot.observations}
        )
        used_evidence = {
            evidence_id
            for row in self.policy_inputs
            for evidence_id in row.evidence_ids
        }
        if self.terminal_policy is not None:
            used_evidence.update(self.terminal_policy.evidence_ids)
        if self.net_share_dilution is not None:
            used_evidence.update(self.net_share_dilution.evidence_ids)
        if self.model_policy is not None:
            used_evidence.update(
                evidence_id
                for row in self.model_policy.relative_methods
                for evidence_id in row.evidence_ids
            )
        used_evidence.update(
            evidence_id
            for row in self.family_authorizations
            for evidence_id in row.evidence_ids
        )
        if not used_evidence <= retained_evidence:
            raise ValueError(
                "valuation policy cites evidence outside retained authority"
            )

        if self.status == "accepted":
            if observed_names != _REQUIRED_OBSERVED_INPUTS:
                raise ValueError(
                    "accepted valuation policy requires all observed inputs"
                )
            if policy_names != ("beta_floor",):
                raise ValueError(
                    "accepted valuation policy requires exact policy inputs"
                )
            if derived_names != _REQUIRED_DERIVED_INPUTS:
                raise ValueError(
                    "accepted valuation policy requires derived input closure"
                )
            if self.terminal_policy is None or self.model_policy is None:
                raise ValueError(
                    "accepted valuation policy requires terminal and method policy"
                )
            if any(row.blocking for row in self.gaps):
                raise ValueError(
                    "accepted valuation policy cannot retain blocking gaps"
                )
        elif not any(row.blocking for row in self.gaps):
            raise ValueError(
                "incomplete or blocked valuation policy needs a blocking gap"
            )

        receipt = self.stage_receipt
        expected_scope = ValuationPolicyStageScope(
            user_id=self.user_id,
            research_file_id=self.research_file_id,
            ticker=self.ticker,
        )
        if (
            receipt.scope != expected_scope
            or receipt.status != self.status
            or receipt.valuation_policy_ref != self.ref
            or receipt.forecast_assumption_set_ref != self.forecast_assumption_set_ref
            or receipt.forecast_assumption_stage_receipt_digest
            != self.forecast_assumption_stage_receipt_digest
            or receipt.scenario_set_ref != self.scenario_set_ref
            or receipt.scenario_stage_receipt_digest
            != self.scenario_stage_receipt_digest
            or receipt.valuation_evidence_snapshot_sha256
            != self.evidence_snapshot.snapshot_sha256
        ):
            raise ValueError(
                "valuation policy stage receipt does not bind exact inputs"
            )
        if self.ref.content_sha256 != valuation_policy_digest(self):
            raise ValueError("ValuationPolicy content digest mismatch")
        return self


def derive_adjusted_beta(*, raw_beta: Decimal, beta_floor: Decimal) -> Decimal:
    adjusted = (
        _BLOOMBERG_STYLE_BETA_WEIGHT * Decimal(raw_beta)
        + (Decimal("1") - _BLOOMBERG_STYLE_BETA_WEIGHT) * _MARKET_BETA_ANCHOR
    )
    return _canonical_decimal(max(adjusted, Decimal(beta_floor)), quantum=_RATE_QUANTUM)


def derive_cost_of_equity(
    *,
    risk_free_rate: Decimal,
    adjusted_beta: Decimal,
    equity_risk_premium: Decimal,
) -> Decimal:
    return _canonical_decimal(
        Decimal(risk_free_rate) + Decimal(adjusted_beta) * Decimal(equity_risk_premium),
        quantum=_RATE_QUANTUM,
    )


def valuation_policy_digest(
    value: ValuationPolicyArtifact | Mapping[str, Any],
) -> str:
    payload = (
        value.model_dump(mode="json")
        if isinstance(value, ValuationPolicyArtifact)
        else dict(value)
    )
    payload.pop("ref", None)
    payload.pop("stage_receipt", None)
    if payload.get("predecessor_ref") is None:
        payload.pop("predecessor_ref", None)
    terminal = payload.get("terminal_policy")
    if terminal is not None:
        terminal_policy = (
            terminal
            if isinstance(terminal, ValuationTerminalPolicy)
            else ValuationTerminalPolicy.model_validate_json(canonical_json_bytes(terminal))
        )
        payload["terminal_policy"] = terminal_policy.model_dump(mode="json")
    evidence_snapshot = payload.get("evidence_snapshot")
    if isinstance(evidence_snapshot, Mapping):
        calendar = evidence_snapshot["market_calendar"]
        market_calendar = (
            calendar
            if isinstance(calendar, MarketCalendarFacts)
            else MarketCalendarFacts.model_validate_json(canonical_json_bytes(calendar))
        )
        payload["evidence_snapshot"] = {
            **evidence_snapshot,
            "market_calendar": market_calendar.model_dump(mode="json"),
        }
    return _digest("valuation-policy.v1", payload)


def build_valuation_policy(
    *,
    policy_id: str | None,
    user_id: str,
    research_file_id: int,
    ticker: str,
    status: Literal["accepted", "incomplete", "blocked"],
    forecast_assumption_set_ref: ForecastAssumptionSetRef,
    forecast_assumption_stage_receipt_digest: str,
    scenario_set_ref: ScenarioSetRef,
    scenario_stage_receipt_digest: str,
    business_model_revision_ref: BusinessModelRevisionRef,
    business_model_selection_epoch: int,
    business_model_stage_receipt_digest: str,
    historical_foundation_ref: HistoricalFoundationRef,
    fiscal_axis: FiscalAxis,
    forecast_control_profile_ref: ForecastControlProfileRef,
    forecast_control_denominator_sha256: str,
    evidence_snapshot: ValuationEvidenceSnapshot,
    methodology: ValuationMethodologyPin,
    observed_inputs: Sequence[ValuationObservedInput],
    policy_inputs: Sequence[ValuationPolicyInput],
    derived_inputs: Sequence[ValuationDerivedInput],
    terminal_policy: ValuationTerminalPolicy | None,
    model_policy: ModelValuationPolicyAuthoring | None,
    family_authorizations: Sequence[ValuationFamilyAuthorizationAuthoring],
    gaps: Sequence[ValuationPolicyGap],
    terminal_summary: ValuationPolicyTerminalSummary,
    created_by: str,
    created_at: datetime,
    started_at: datetime,
    completed_at: datetime,
    predecessor_ref: ValuationPolicyRef | None = None,
    net_share_dilution: ValuationNetShareDilution | None = None,
) -> ValuationPolicyArtifact:
    payload = {
        "schema_version": "valuation-policy.v1",
        "user_id": user_id,
        "research_file_id": research_file_id,
        "ticker": ticker,
        "status": status,
        "forecast_assumption_set_ref": forecast_assumption_set_ref,
        "forecast_assumption_stage_receipt_digest": (
            forecast_assumption_stage_receipt_digest
        ),
        "scenario_set_ref": scenario_set_ref,
        "scenario_stage_receipt_digest": scenario_stage_receipt_digest,
        "business_model_revision_ref": business_model_revision_ref,
        "business_model_selection_epoch": business_model_selection_epoch,
        "business_model_stage_receipt_digest": business_model_stage_receipt_digest,
        "historical_foundation_ref": historical_foundation_ref,
        "fiscal_axis": fiscal_axis,
        "fiscal_axis_sha256": fiscal_axis_digest(fiscal_axis),
        "forecast_control_profile_ref": forecast_control_profile_ref,
        "forecast_control_denominator_sha256": forecast_control_denominator_sha256,
        "evidence_snapshot": evidence_snapshot,
        "methodology": methodology,
        "observed_inputs": tuple(
            sorted(observed_inputs, key=lambda row: row.input_name)
        ),
        "policy_inputs": tuple(sorted(policy_inputs, key=lambda row: row.input_name)),
        "derived_inputs": tuple(sorted(derived_inputs, key=lambda row: row.input_name)),
        "terminal_policy": terminal_policy,
        **({} if net_share_dilution is None else {"net_share_dilution": net_share_dilution}),
        "model_policy": model_policy,
        "family_authorizations": tuple(
            sorted(family_authorizations, key=lambda row: row.method)
        ),
        "gaps": tuple(sorted(gaps, key=lambda row: row.key)),
        "terminal_summary": terminal_summary,
        "created_by": created_by,
        "created_at": created_at,
        **({} if predecessor_ref is None else {"predecessor_ref": predecessor_ref}),
    }
    ref = ValuationPolicyRef(
        policy_id=policy_id or issue_valuation_policy_id(),
        content_sha256=valuation_policy_digest(payload),
    )
    receipt = build_valuation_policy_stage_receipt(
        status=status,
        scope=ValuationPolicyStageScope(
            user_id=user_id,
            research_file_id=research_file_id,
            ticker=ticker,
        ),
        valuation_policy_ref=ref,
        forecast_assumption_set_ref=forecast_assumption_set_ref,
        forecast_assumption_stage_receipt_digest=(
            forecast_assumption_stage_receipt_digest
        ),
        scenario_set_ref=scenario_set_ref,
        scenario_stage_receipt_digest=scenario_stage_receipt_digest,
        valuation_evidence_snapshot_sha256=evidence_snapshot.snapshot_sha256,
        started_at=started_at,
        completed_at=completed_at,
    )
    return ValuationPolicyArtifact.model_validate(
        {**payload, "ref": ref, "stage_receipt": receipt}
    )


class ValuationPolicyProjection(_Contract):
    schema_version: Literal["valuation-policy-projection.v1"] = (
        "valuation-policy-projection.v1"
    )
    valuation_policy_ref: ValuationPolicyRef
    valuation_controls: tuple[ValuationControlAuthoring, ...]
    model_policy: ModelValuationPolicyAuthoring
    family_authorizations: tuple[ValuationFamilyAuthorizationAuthoring, ...]
    terminal_growth_cases: ValuationCaseValues
    exit_multiple_cases: ValuationCaseValues
    net_share_dilution: ValuationNetShareDilutionRates | None = None
    exit_earnings_measure: ExitEarningsMeasure = "terminal_fcf"


__all__ = [
    "ValuationCaseValues",
    "ValuationDerivedInput",
    "ValuationEvidenceSnapshot",
    "ValuationMethodologyPin",
    "ValuationObservedInput",
    "ValuationPolicyArtifact",
    "ValuationPolicyGap",
    "ValuationPolicyInput",
    "ValuationPolicyProjection",
    "ValuationPolicyRef",
    "ValuationPolicyStageReceipt",
    "ValuationPolicyStageScope",
    "ValuationPolicyTerminalIssue",
    "ValuationPolicyTerminalSummary",
    "ValuationNetShareDilution",
    "ValuationNetShareDilutionRate",
    "ValuationTerminalPolicy",
    "build_valuation_evidence_snapshot",
    "build_valuation_policy",
    "build_valuation_policy_stage_receipt",
    "derive_adjusted_beta",
    "derive_cost_of_equity",
    "issue_valuation_policy_id",
    "valuation_evidence_snapshot_digest",
    "valuation_policy_digest",
    "valuation_policy_stage_receipt_digest",
]
