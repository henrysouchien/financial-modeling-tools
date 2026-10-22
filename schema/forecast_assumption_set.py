"""Immutable pre-compile forecast judgment and its exact stage receipt."""

from __future__ import annotations

from datetime import datetime
import hashlib
import secrets
from typing import Any, Literal, Mapping, Sequence

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from .business_model_revision_ref import BusinessModelRevisionRef
from .business_model_stage import BusinessModelSelectedEvidence
from .forecast_control_profile import (
    ForecastControlProfilePin,
    ForecastControlProfileRef,
    forecast_control_profile_ref as canonical_forecast_control_profile_ref,
)
from .historical_basis_snapshot import (
    HistoricalBasisArtifactRef,
    HistoricalBasisFreshness,
    fiscal_axis_digest,
)
from .judgment_revision import (
    Supersession,
    canonical_supersessions,
    validate_revision_fields,
)
from .model_spec_authoring import ForecastAssumptionAuthoring
from .model_state_identity import validate_sha256_hex
from .stable_control_identity import FiscalAxis
from .ticker_contract import normalize_ticker
from .versioned_artifact import canonical_json_bytes


class _Contract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


def _require_aware(value: datetime, *, field_name: str | None) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


def _digest(domain: str, value: object) -> str:
    return hashlib.sha256(
        domain.encode("utf-8") + b"\0" + canonical_json_bytes(value)
    ).hexdigest()


class ForecastAssumptionSetRef(_Contract):
    schema_version: Literal["forecast-assumption-set-ref.v1"] = (
        "forecast-assumption-set-ref.v1"
    )
    set_id: str = Field(pattern=r"^fas_[0-9a-f]{32}$")
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _content_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")


def issue_forecast_assumption_set_id() -> str:
    return f"fas_{secrets.token_hex(16)}"


class ForecastMethodologyPin(_Contract):
    schema_version: Literal["forecast-methodology-pin.v1"] = (
        "forecast-methodology-pin.v1"
    )
    name: str = Field(min_length=1)
    declared_version: str = Field(min_length=1)
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _content_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")


class ForecastAssumptionGap(_Contract):
    schema_version: Literal["forecast-assumption-gap.v1"] = (
        "forecast-assumption-gap.v1"
    )
    key: str = Field(min_length=1)
    description: str = Field(min_length=1)
    driver: str | None = Field(default=None, min_length=1)
    period_id: str | None = Field(default=None, min_length=1)
    blocking: bool
    repair_target: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _repairable_blocker(self) -> "ForecastAssumptionGap":
        if self.blocking and self.repair_target is None:
            raise ValueError("blocking forecast gaps require a repair target")
        return self


class ForecastDriverAssumption(_Contract):
    """One quantitative path plus the five durable methodology pillars."""

    schema_version: Literal["forecast-driver-assumption.v1"] = (
        "forecast-driver-assumption.v1"
    )
    assumption: ForecastAssumptionAuthoring
    classification: Literal["critical", "supporting", "held_at_base"]
    mechanism: str = Field(min_length=1)
    base_rate_anchor: str = Field(min_length=1)
    evidence_quality: Literal["low", "medium", "high"]
    evidence_quality_assessment: str = Field(min_length=1)
    revision_trigger: str = Field(min_length=1)
    evidence_ids_by_period: dict[int, tuple[str, ...]] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _classification(self) -> "ForecastDriverAssumption":
        held = self.classification == "held_at_base"
        if self.assumption.held_at_base != held:
            raise ValueError(
                "held_at_base classification must equal assumption.held_at_base"
            )
        valued_years = set(self.assumption.period_values or {})
        if not set(self.evidence_ids_by_period) <= valued_years:
            raise ValueError("per-period evidence must name a valued forecast year")
        allowed_evidence = set(self.assumption.evidence_ids)
        for evidence_ids in self.evidence_ids_by_period.values():
            if evidence_ids != tuple(sorted(set(evidence_ids))) or any(
                not evidence_id for evidence_id in evidence_ids
            ):
                raise ValueError("per-period evidence ids must be sorted and unique")
            if not set(evidence_ids) <= allowed_evidence:
                raise ValueError(
                    "per-period evidence ids must belong to assumption.evidence_ids"
                )
        return self


class ForecastTerminalIssue(_Contract):
    schema_version: Literal["forecast-terminal-issue.v1"] = (
        "forecast-terminal-issue.v1"
    )
    key: str = Field(min_length=1)
    text: str = Field(min_length=1)
    drivers: tuple[str, ...] = ()

    @field_validator("drivers")
    @classmethod
    def _drivers(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != tuple(sorted(set(value))) or any(not item for item in value):
            raise ValueError("terminal issue drivers must be sorted, unique, and nonblank")
        return value


class ForecastTerminalSummary(_Contract):
    schema_version: Literal["forecast-terminal-summary.v1"] = (
        "forecast-terminal-summary.v1"
    )
    authored_by: Literal["agent"] = "agent"
    summary: str = Field(min_length=1)
    key_decisions: tuple[str, ...] = ()
    confidence: Literal["low", "medium", "high"]
    limitations: tuple[ForecastTerminalIssue, ...] = ()
    unresolved_questions: tuple[ForecastTerminalIssue, ...] = ()
    recommended_next_stage: str = Field(min_length=1)

    @field_validator("key_decisions")
    @classmethod
    def _decisions(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if len(value) != len(set(value)) or any(not item for item in value):
            raise ValueError("key decisions must be unique and nonblank")
        return value

    @model_validator(mode="after")
    def _issues(self) -> "ForecastTerminalSummary":
        for field_name in ("limitations", "unresolved_questions"):
            rows = getattr(self, field_name)
            seen_keys: set[str] = set()
            deduped_rows = []
            for row in sorted(rows, key=lambda r: r.key):
                if row.key not in seen_keys:
                    seen_keys.add(row.key)
                    deduped_rows.append(row)
            object.__setattr__(self, field_name, tuple(deduped_rows))
        overlap = {
            row.key for row in self.limitations
        } & {row.key for row in self.unresolved_questions}
        if overlap:
            raise ValueError("terminal issue keys must be globally unique")
        return self


class ForecastStageScope(_Contract):
    schema_version: Literal["forecast-stage-scope.v1"] = "forecast-stage-scope.v1"
    user_id: str = Field(min_length=1)
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1)

    @field_validator("ticker")
    @classmethod
    def _canonical_ticker(cls, value: str) -> str:
        normalized = normalize_ticker(value)
        if normalized != value:
            raise ValueError("ticker must be canonical")
        return normalized


class ForecastAssumptionStageReceipt(_Contract):
    schema_version: Literal["forecast-assumption-stage-receipt.v1"] = (
        "forecast-assumption-stage-receipt.v1"
    )
    stage: Literal["forecast-assumptions"] = "forecast-assumptions"
    status: Literal["accepted", "incomplete", "blocked"]
    scope: ForecastStageScope
    forecast_assumption_set_ref: ForecastAssumptionSetRef
    business_model_revision_ref: BusinessModelRevisionRef
    business_model_selection_epoch: int = Field(gt=0)
    business_model_stage_receipt_digest: str
    historical_basis_ref: HistoricalBasisArtifactRef
    historical_basis_freshness: HistoricalBasisFreshness
    started_at: datetime
    completed_at: datetime
    receipt_digest: str

    @field_validator(
        "business_model_stage_receipt_digest",
        "receipt_digest",
    )
    @classmethod
    def _digests(cls, value: str, info: ValidationInfo) -> str:
        return validate_sha256_hex(value, field_name=info.field_name)

    @field_validator("started_at", "completed_at")
    @classmethod
    def _timestamps(cls, value: datetime, info: ValidationInfo) -> datetime:
        return _require_aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _binding(self) -> "ForecastAssumptionStageReceipt":
        if self.completed_at < self.started_at:
            raise ValueError("completed_at must not precede started_at")
        if self.business_model_revision_ref.business_model_id != (
            f"{self.scope.ticker}_business_model"
        ):
            raise ValueError("BusinessModel revision ref does not match receipt scope")
        if self.receipt_digest != forecast_stage_receipt_digest(self):
            raise ValueError("forecast stage receipt digest mismatch")
        return self


def forecast_stage_receipt_digest(
    value: ForecastAssumptionStageReceipt | Mapping[str, Any],
) -> str:
    payload = (
        value.model_dump(mode="json")
        if isinstance(value, ForecastAssumptionStageReceipt)
        else dict(value)
    )
    payload.pop("receipt_digest", None)
    return _digest("forecast-assumption-stage-receipt.v1", payload)


def build_forecast_stage_receipt(
    *,
    status: Literal["accepted", "incomplete", "blocked"],
    scope: ForecastStageScope,
    forecast_assumption_set_ref: ForecastAssumptionSetRef,
    business_model_revision_ref: BusinessModelRevisionRef,
    business_model_selection_epoch: int,
    business_model_stage_receipt_digest: str,
    historical_basis_ref: HistoricalBasisArtifactRef,
    historical_basis_freshness: HistoricalBasisFreshness,
    started_at: datetime,
    completed_at: datetime,
) -> ForecastAssumptionStageReceipt:
    payload = {
        "schema_version": "forecast-assumption-stage-receipt.v1",
        "stage": "forecast-assumptions",
        "status": status,
        "scope": scope,
        "forecast_assumption_set_ref": forecast_assumption_set_ref,
        "business_model_revision_ref": business_model_revision_ref,
        "business_model_selection_epoch": business_model_selection_epoch,
        "business_model_stage_receipt_digest": business_model_stage_receipt_digest,
        "historical_basis_ref": historical_basis_ref,
        "historical_basis_freshness": historical_basis_freshness,
        "started_at": started_at,
        "completed_at": completed_at,
    }
    return ForecastAssumptionStageReceipt.model_validate(
        {**payload, "receipt_digest": forecast_stage_receipt_digest(payload)}
    )


class ForecastAssumptionSet(_Contract):
    schema_version: Literal["forecast-assumption-set.v1"] = (
        "forecast-assumption-set.v1"
    )
    user_id: str = Field(min_length=1)
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1)
    ref: ForecastAssumptionSetRef
    status: Literal["accepted", "incomplete", "blocked"]
    business_model_revision_ref: BusinessModelRevisionRef
    business_model_selection_epoch: int = Field(gt=0)
    business_model_stage_receipt_digest: str
    historical_basis_ref: HistoricalBasisArtifactRef
    historical_basis_freshness: HistoricalBasisFreshness
    fiscal_axis: FiscalAxis
    fiscal_axis_sha256: str
    forecast_control_profile_ref: ForecastControlProfileRef | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    forecast_control_denominator_sha256: str | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    methodology: ForecastMethodologyPin
    required_driver_keys: tuple[str, ...]
    # Policy controls the author may answer or leave to the compiler's house
    # default (which raises ``judgment_absent_default_used``); never required.
    optional_driver_keys: tuple[str, ...] = Field(
        default=(),
        exclude_if=lambda value: not value,
    )
    driver_assumptions: tuple[ForecastDriverAssumption, ...] = ()
    inherited_business_model_evidence_ids: tuple[str, ...] = ()
    forecast_evidence: tuple[BusinessModelSelectedEvidence, ...] = ()
    gaps: tuple[ForecastAssumptionGap, ...] = ()
    terminal_summary: ForecastTerminalSummary
    created_by: str = Field(min_length=1)
    created_at: datetime
    # The Revision fields (``schema/judgment_revision.py``): absent on a first
    # set and on every set persisted before them.
    predecessor_ref: ForecastAssumptionSetRef | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    #: One per predecessor row (driver key) this set changes or drops.
    supersessions: tuple[Supersession, ...] = Field(
        default=(),
        exclude_if=lambda value: not value,
    )
    stage_receipt: ForecastAssumptionStageReceipt

    @field_validator("ticker")
    @classmethod
    def _canonical_ticker(cls, value: str) -> str:
        normalized = normalize_ticker(value)
        if normalized != value:
            raise ValueError("ticker must be canonical")
        return normalized

    @field_validator(
        "business_model_stage_receipt_digest",
        "fiscal_axis_sha256",
    )
    @classmethod
    def _digests(cls, value: str, info: ValidationInfo) -> str:
        return validate_sha256_hex(value, field_name=info.field_name)

    @field_validator("forecast_control_denominator_sha256")
    @classmethod
    def _optional_forecast_digest(cls, value: str | None) -> str | None:
        if value is None:
            return None
        return validate_sha256_hex(
            value,
            field_name="forecast_control_denominator_sha256",
        )

    @field_validator("inherited_business_model_evidence_ids")
    @classmethod
    def _inherited_ids(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != tuple(sorted(set(value))):
            raise ValueError("inherited BusinessModel evidence ids must be sorted and unique")
        for evidence_id in value:
            validate_sha256_hex(
                evidence_id,
                field_name="inherited_business_model_evidence_ids",
            )
        return value

    @field_validator("required_driver_keys", "optional_driver_keys")
    @classmethod
    def _driver_key_sets(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        if value != tuple(sorted(set(value))) or any(not item for item in value):
            raise ValueError("driver keys must be sorted, unique, and nonblank")
        return value

    @field_validator("created_at")
    @classmethod
    def _created_at(cls, value: datetime) -> datetime:
        return _require_aware(value, field_name="created_at")

    @model_validator(mode="after")
    def _artifact(self) -> "ForecastAssumptionSet":
        if self.business_model_revision_ref.business_model_id != (
            f"{self.ticker}_business_model"
        ):
            raise ValueError("BusinessModel revision ref does not match set ticker")
        if self.fiscal_axis_sha256 != fiscal_axis_digest(self.fiscal_axis):
            raise ValueError("forecast fiscal axis digest mismatch")
        if (self.forecast_control_profile_ref is None) != (
            self.forecast_control_denominator_sha256 is None
        ):
            raise ValueError(
                "forecast-control profile and denominator digest must be paired"
            )
        drivers = tuple(row.assumption.driver for row in self.driver_assumptions)
        if drivers != tuple(sorted(set(drivers))):
            raise ValueError("forecast assumptions must be sorted and unique by driver")
        if set(self.required_driver_keys) & set(self.optional_driver_keys):
            raise ValueError("a driver key is either required or optional, never both")
        evidence_ids = tuple(row.evidence_id for row in self.forecast_evidence)
        if evidence_ids != tuple(sorted(set(evidence_ids))):
            raise ValueError("forecast evidence must be sorted and unique by evidence_id")
        gap_keys = tuple(row.key for row in self.gaps)
        if gap_keys != tuple(sorted(set(gap_keys))):
            raise ValueError("forecast gaps must be sorted and unique by key")
        validate_revision_fields(self.predecessor_ref, self.supersessions)

        used_evidence = {
            evidence_id
            for row in self.driver_assumptions
            for evidence_id in (
                *row.assumption.evidence_ids,
                *row.assumption.inherited_baseline_ids,
            )
        }
        required_direct = {
            *self.inherited_business_model_evidence_ids,
            *evidence_ids,
        }
        unused = tuple(sorted(required_direct - used_evidence))
        if unused:
            raise ValueError(f"retained forecast evidence is not used: {unused!r}")

        if self.status == "accepted":
            if self.historical_basis_ref.kind != "historical_foundation":
                raise ValueError(
                    "accepted forecast set requires an accepted HistoricalFoundation"
                )
            if not self.driver_assumptions:
                raise ValueError("accepted forecast set requires assumptions")
            if any(row.blocking for row in self.gaps):
                raise ValueError("accepted forecast set cannot retain blocking gaps")
            period_ids = tuple(
                period.period_id for period in self.fiscal_axis.forecast_periods
            )
            fiscal_years = {
                period.fiscal_year for period in self.fiscal_axis.forecast_periods
            }
            if not driver_coverage_closed(
                drivers,
                required=self.required_driver_keys,
                optional=self.optional_driver_keys,
            ):
                raise ValueError(
                    "accepted forecast drivers must cover the required denominator "
                    "and stay within required and optional policy keys"
                )
            for row in self.driver_assumptions:
                assumption = row.assumption
                if assumption.period_ids != period_ids:
                    raise ValueError(
                        "accepted forecast assumptions must cover the exact forecast axis"
                    )
                if assumption.period_values is not None and (
                    set(assumption.period_values) != fiscal_years
                ):
                    raise ValueError(
                        "accepted forecast period values must cover every forecast year"
                    )
                if (
                    assumption.value is None
                    and assumption.period_values is None
                    and not assumption.held_at_base
                ):
                    raise ValueError(
                        "accepted forecast assumption requires a value or held-at-base policy"
                    )

        receipt = self.stage_receipt
        expected_scope = ForecastStageScope(
            user_id=self.user_id,
            research_file_id=self.research_file_id,
            ticker=self.ticker,
        )
        if (
            receipt.scope != expected_scope
            or receipt.status != self.status
            or receipt.forecast_assumption_set_ref != self.ref
            or receipt.business_model_revision_ref != self.business_model_revision_ref
            or receipt.business_model_selection_epoch
            != self.business_model_selection_epoch
            or receipt.business_model_stage_receipt_digest
            != self.business_model_stage_receipt_digest
            or receipt.historical_basis_ref != self.historical_basis_ref
            or receipt.historical_basis_freshness
            != self.historical_basis_freshness
        ):
            raise ValueError("forecast stage receipt does not bind the exact set")
        if self.ref.content_sha256 != forecast_assumption_set_digest(self):
            raise ValueError("forecast assumption set content digest mismatch")
        return self


def forecast_assumption_set_digest(
    value: ForecastAssumptionSet | Mapping[str, Any],
) -> str:
    payload = (
        value.model_dump(mode="json")
        if isinstance(value, ForecastAssumptionSet)
        else dict(value)
    )
    payload.pop("ref", None)
    payload.pop("stage_receipt", None)
    if payload.get("forecast_control_profile_ref") is None:
        payload.pop("forecast_control_profile_ref", None)
    if payload.get("forecast_control_denominator_sha256") is None:
        payload.pop("forecast_control_denominator_sha256", None)
    if not payload.get("optional_driver_keys"):
        payload.pop("optional_driver_keys", None)
    if payload.get("predecessor_ref") is None:
        payload.pop("predecessor_ref", None)
    if not payload.get("supersessions"):
        payload.pop("supersessions", None)
    return _digest("forecast-assumption-set.v1", payload)


def driver_coverage_closed(
    drivers: Sequence[str],
    *,
    required: Sequence[str],
    optional: Sequence[str],
) -> bool:
    """Accepted coverage: every required key, plus any subset of optional keys."""

    present = set(drivers)
    return set(required) <= present <= {*required, *optional}


def build_forecast_assumption_set(
    *,
    set_id: str | None,
    user_id: str,
    research_file_id: int,
    ticker: str,
    status: Literal["accepted", "incomplete", "blocked"],
    business_model_revision_ref: BusinessModelRevisionRef,
    business_model_selection_epoch: int,
    business_model_stage_receipt_digest: str,
    historical_basis_ref: HistoricalBasisArtifactRef,
    historical_basis_freshness: HistoricalBasisFreshness,
    fiscal_axis: FiscalAxis,
    methodology: ForecastMethodologyPin,
    required_driver_keys: Sequence[str],
    driver_assumptions: Sequence[ForecastDriverAssumption],
    inherited_business_model_evidence_ids: Sequence[str],
    forecast_evidence: Sequence[BusinessModelSelectedEvidence],
    gaps: Sequence[ForecastAssumptionGap],
    terminal_summary: ForecastTerminalSummary,
    created_by: str,
    created_at: datetime,
    started_at: datetime,
    completed_at: datetime,
    forecast_control_profile_ref: (
        ForecastControlProfileRef | ForecastControlProfilePin | None
    ) = None,
    forecast_control_denominator_sha256: str | None = None,
    optional_driver_keys: Sequence[str] = (),
    predecessor_ref: ForecastAssumptionSetRef | None = None,
    supersessions: Sequence[Supersession] = (),
) -> ForecastAssumptionSet:
    canonical_historical_basis_ref = HistoricalBasisArtifactRef.model_validate(
        historical_basis_ref.model_dump(mode="python")
    )
    canonical_assumptions = tuple(
        sorted(driver_assumptions, key=lambda row: row.assumption.driver)
    )
    canonical_inherited = tuple(sorted(set(inherited_business_model_evidence_ids)))
    canonical_evidence = tuple(
        sorted(forecast_evidence, key=lambda row: row.evidence_id)
    )
    canonical_gaps = tuple(sorted(gaps, key=lambda row: row.key))
    payload = {
        "schema_version": "forecast-assumption-set.v1",
        "user_id": user_id,
        "research_file_id": research_file_id,
        "ticker": ticker,
        "status": status,
        "business_model_revision_ref": business_model_revision_ref,
        "business_model_selection_epoch": business_model_selection_epoch,
        "business_model_stage_receipt_digest": business_model_stage_receipt_digest,
        "historical_basis_ref": canonical_historical_basis_ref,
        "historical_basis_freshness": historical_basis_freshness,
        "fiscal_axis": fiscal_axis,
        "fiscal_axis_sha256": fiscal_axis_digest(fiscal_axis),
        "methodology": methodology,
        "required_driver_keys": tuple(sorted(set(required_driver_keys))),
        "optional_driver_keys": tuple(sorted(set(optional_driver_keys))),
        "driver_assumptions": canonical_assumptions,
        "inherited_business_model_evidence_ids": canonical_inherited,
        "forecast_evidence": canonical_evidence,
        "gaps": canonical_gaps,
        "terminal_summary": terminal_summary,
        "created_by": created_by,
        "created_at": created_at,
    }
    if forecast_control_profile_ref is not None:
        payload["forecast_control_profile_ref"] = canonical_forecast_control_profile_ref(
            forecast_control_profile_ref
        )
    if forecast_control_denominator_sha256 is not None:
        payload["forecast_control_denominator_sha256"] = (
            forecast_control_denominator_sha256
        )
    if predecessor_ref is not None:
        payload["predecessor_ref"] = predecessor_ref
    if supersessions:
        payload["supersessions"] = canonical_supersessions(supersessions)
    content_sha256 = forecast_assumption_set_digest(payload)
    ref = ForecastAssumptionSetRef(
        set_id=set_id or issue_forecast_assumption_set_id(),
        content_sha256=content_sha256,
    )
    scope = ForecastStageScope(
        user_id=user_id,
        research_file_id=research_file_id,
        ticker=ticker,
    )
    receipt = build_forecast_stage_receipt(
        status=status,
        scope=scope,
        forecast_assumption_set_ref=ref,
        business_model_revision_ref=business_model_revision_ref,
        business_model_selection_epoch=business_model_selection_epoch,
        business_model_stage_receipt_digest=business_model_stage_receipt_digest,
        historical_basis_ref=canonical_historical_basis_ref,
        historical_basis_freshness=historical_basis_freshness,
        started_at=started_at,
        completed_at=completed_at,
    )
    return ForecastAssumptionSet.model_validate(
        {**payload, "ref": ref, "stage_receipt": receipt}
    )


__all__ = [
    "ForecastAssumptionGap",
    "ForecastAssumptionSet",
    "ForecastAssumptionSetRef",
    "ForecastAssumptionStageReceipt",
    "ForecastDriverAssumption",
    "ForecastMethodologyPin",
    "ForecastStageScope",
    "ForecastTerminalIssue",
    "ForecastTerminalSummary",
    "build_forecast_assumption_set",
    "build_forecast_stage_receipt",
    "driver_coverage_closed",
    "forecast_assumption_set_digest",
    "forecast_stage_receipt_digest",
    "issue_forecast_assumption_set_id",
]
