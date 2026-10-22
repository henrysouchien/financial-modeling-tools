"""Exact-ModelVersion post-compile valuation contracts and the PriceTarget projection.

The compiler owns the numeric valuation snapshot.  The agent owns only the
method assessment and investment conclusion, recorded on the judgment
artifact (``schema.postcompile_valuation_artifact``).  An accepted artifact
projects one legacy-compatible :class:`PriceTarget` through
:func:`project_postcompile_price_target`; incomplete attempts remain useful
audit records but never manufacture a canonical target.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal, ROUND_HALF_EVEN
import hashlib
import secrets
from typing import Any, Literal, Mapping

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationInfo,
    field_validator,
    model_validator,
)

from .model_ref import ModelVersionRef
from .model_state_identity import validate_sha256_hex
from . import ids
from .scenario_set import ScenarioSetRef
from .valuation_policy import ValuationPolicyRef
from .price_target import CurrentPriceSnapshot, PriceTarget, PriceTargetRanges
from .ticker_contract import normalize_ticker
from .versioned_artifact import canonical_json_bytes


_VALUE_QUANTUM = Decimal("0.000000000001")
_PRICE_QUANTUM = Decimal("0.000001")
_METHODS = (
    "dcf_exit_multiple",
    "dcf_terminal_growth",
    "ev_adjusted_ebitda",
    "forward_pe",
)
CONSENSUS_NON_GAAP_EBITDA_DENOMINATOR_LIMITATION = (
    "Forward denominator is a provider consensus non-GAAP EBITDA estimate; its "
    "add-back components are not issuer-reconciled and its per-period "
    "construction is not attested."
)


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


def _aware(value: datetime, *, field_name: str | None) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return value


def _decimal(value: Decimal, *, quantum: Decimal = _VALUE_QUANTUM) -> Decimal:
    result = Decimal(value).quantize(quantum, rounding=ROUND_HALF_EVEN)
    if not result.is_finite():
        raise ValueError("valuation values must be finite")
    return result


def _canonical_strings(value: tuple[str, ...], *, field_name: str) -> tuple[str, ...]:
    if value != tuple(sorted(set(value))) or any(not item for item in value):
        raise ValueError(f"{field_name} must be sorted, unique, and nonblank")
    return value


class PostcompileValuationRef(_Contract):
    schema_version: Literal["postcompile-valuation-ref.v1"] = (
        "postcompile-valuation-ref.v1"
    )
    artifact_id: str = Field(pattern=r"^pcv_[0-9a-f]{32}$")
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _content_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")


def issue_postcompile_valuation_id() -> str:
    return f"pcv_{secrets.token_hex(16)}"


class PostcompileValuationMethodologyPin(_Contract):
    schema_version: Literal["postcompile-valuation-methodology-pin.v1"] = (
        "postcompile-valuation-methodology-pin.v1"
    )
    name: Literal["dcf-relative-valuation"] = "dcf-relative-valuation"
    declared_version: str = Field(min_length=1)
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _methodology_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")


class ExactModelVersionBinding(_Contract):
    """Human-resolvable immutable ModelVersion identity and content."""

    schema_version: Literal["exact-model-version-binding.v1"] = (
        "exact-model-version-binding.v1"
    )
    model_ref: ModelVersionRef
    source_bundle_id: str = Field(min_length=1)
    source_bundle_identity_digest: str
    model_version_content_sha256: str
    request_digest: str
    source_bundle_digest: str
    model_spec_digest: str
    workbook_content_sha256: str
    semantic_sidecar_content_sha256: str
    provenance_map_content_sha256: str
    validation_report_content_sha256: str
    committed_at: datetime

    @field_validator(
        "model_version_content_sha256",
        "source_bundle_identity_digest",
        "request_digest",
        "source_bundle_digest",
        "model_spec_digest",
        "workbook_content_sha256",
        "semantic_sidecar_content_sha256",
        "provenance_map_content_sha256",
        "validation_report_content_sha256",
    )
    @classmethod
    def _digests(cls, value: str, info: ValidationInfo) -> str:
        return validate_sha256_hex(value, field_name=info.field_name)

    @field_validator("committed_at")
    @classmethod
    def _committed_at(cls, value: datetime) -> datetime:
        return _aware(value, field_name="committed_at")


class CompiledCurrentPrice(_Contract):
    schema_version: Literal["compiled-current-price.v1"] = "compiled-current-price.v1"
    value: Decimal
    currency: Literal["USD"] = "USD"
    source_id: str = Field(min_length=1)
    provider: str = Field(min_length=1)
    as_of_date: date
    retrieved_at: datetime
    observation_id: str = Field(min_length=1)
    observation_content_sha256: str

    @field_validator("value")
    @classmethod
    def _value(cls, value: Decimal) -> Decimal:
        result = _decimal(value, quantum=_PRICE_QUANTUM)
        if result <= 0:
            raise ValueError("compiled current price must be positive")
        return result

    @field_validator("retrieved_at")
    @classmethod
    def _retrieved_at(cls, value: datetime) -> datetime:
        return _aware(value, field_name="retrieved_at")

    @field_validator("observation_content_sha256")
    @classmethod
    def _observation_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="observation_content_sha256")


class CompiledValuationComponent(_Contract):
    schema_version: Literal["compiled-valuation-component.v1"] = (
        "compiled-valuation-component.v1"
    )
    key: str = Field(min_length=1)
    value: Decimal
    unit: Literal["ratio", "multiple", "usd_millions", "usd_per_share", "count"]
    fiscal_year: int | None = None

    @field_validator("value")
    @classmethod
    def _value(cls, value: Decimal) -> Decimal:
        return _decimal(value)


class CompiledPriceCases(_Contract):
    """One method's compiled bull/base/bear prices.

    Cases that do not run bull > base > bear are reported beside the compiled
    valuation (``scenario_case_order_inverted``), not refused here.
    """

    schema_version: Literal["compiled-price-cases.v1"] = "compiled-price-cases.v1"
    bull: Decimal
    base: Decimal
    bear: Decimal

    @field_validator("bull", "base", "bear")
    @classmethod
    def _price(cls, value: Decimal) -> Decimal:
        result = _decimal(value, quantum=_PRICE_QUANTUM)
        if result <= 0:
            raise ValueError("compiled method prices must be positive")
        return result


class CompiledValuationMethodOutput(_Contract):
    schema_version: Literal["compiled-valuation-method-output.v1"] = (
        "compiled-valuation-method-output.v1"
    )
    method: Literal[
        "dcf_exit_multiple",
        "dcf_terminal_growth",
        "ev_adjusted_ebitda",
        "forward_pe",
    ]
    status: Literal["available", "unavailable", "invalid"]
    prices: CompiledPriceCases | None = None
    components: tuple[CompiledValuationComponent, ...] = ()
    limitations: tuple[str, ...] = ()

    @field_validator("limitations")
    @classmethod
    def _limitations(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _canonical_strings(value, field_name="method limitations")

    @model_validator(mode="after")
    def _shape(self) -> "CompiledValuationMethodOutput":
        keys = tuple(row.key for row in self.components)
        if keys != tuple(sorted(set(keys))):
            raise ValueError("compiled method components must be sorted and unique")
        if self.status == "available":
            if self.prices is None:
                raise ValueError("available valuation method requires price cases")
        elif self.prices is not None or not self.limitations:
            raise ValueError(
                "unavailable or invalid method forbids prices and requires limitations"
            )
        return self


class CompiledDriverSensitivity(_Contract):
    """One judgment row's ranked effect on the compiled DCF value per share.

    Computed by ``schema.analysis.sensitize``: the price change for a 1%
    relative change of the row's compiled values, every other row held.
    """

    schema_version: Literal["compiled-driver-sensitivity.v1"] = (
        "compiled-driver-sensitivity.v1"
    )
    driver_key: str = Field(min_length=1)
    subject: str = Field(min_length=1)
    item_ids: tuple[str, ...] = Field(min_length=1)
    delta_per_pct: Decimal
    rank: int = Field(ge=1)

    @field_validator("delta_per_pct")
    @classmethod
    def _delta(cls, value: Decimal) -> Decimal:
        return _decimal(value)


class CompiledValuationSnapshot(_Contract):
    """Code-owned bounded projection of exact compiled valuation outputs."""

    schema_version: Literal["compiled-valuation-snapshot.v1"] = (
        "compiled-valuation-snapshot.v1"
    )
    model_version: ExactModelVersionBinding
    valuation_policy_ref: ValuationPolicyRef
    valuation_policy_stage_receipt_digest: str
    scenario_set_ref: ScenarioSetRef
    current_price: CompiledCurrentPrice
    first_forecast_year: int
    terminal_forecast_year: int
    wacc: tuple[CompiledValuationComponent, ...]
    enterprise_to_equity_bridge: tuple[CompiledValuationComponent, ...]
    methods: tuple[CompiledValuationMethodOutput, ...]
    snapshot_sha256: str
    #: Ranked ``sensitize`` rows over the judgment rows the compiled cells cite;
    #: absent from snapshots derived before sensitize was called.
    driver_sensitivities: tuple[CompiledDriverSensitivity, ...] = Field(
        default=(), exclude_if=lambda value: not value
    )

    @field_validator("valuation_policy_stage_receipt_digest", "snapshot_sha256")
    @classmethod
    def _digests(cls, value: str, info: ValidationInfo) -> str:
        return validate_sha256_hex(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _canonical(self) -> "CompiledValuationSnapshot":
        if self.terminal_forecast_year < self.first_forecast_year:
            raise ValueError("terminal forecast year precedes first forecast year")
        for field_name, rows in (
            ("wacc", self.wacc),
            ("enterprise_to_equity_bridge", self.enterprise_to_equity_bridge),
        ):
            keys = tuple(row.key for row in rows)
            if keys != tuple(sorted(set(keys))):
                raise ValueError(
                    f"compiled snapshot {field_name} must be sorted and unique"
                )
        methods = tuple(row.method for row in self.methods)
        if methods != _METHODS:
            raise ValueError(
                "compiled valuation snapshot requires the exact method denominator"
            )
        ranks = [row.rank for row in self.driver_sensitivities]
        if ranks != list(range(1, len(ranks) + 1)):
            raise ValueError("compiled driver sensitivities must rank densely in order")
        if self.snapshot_sha256 != compiled_valuation_snapshot_digest(self):
            raise ValueError("compiled valuation snapshot digest mismatch")
        return self


def compiled_valuation_snapshot_digest(
    value: CompiledValuationSnapshot | Mapping[str, Any],
) -> str:
    payload = (
        value.model_dump(mode="json")
        if isinstance(value, CompiledValuationSnapshot)
        else dict(value)
    )
    payload.pop("snapshot_sha256", None)
    return _digest("compiled-valuation-snapshot.v1", payload)


class PostcompileMethodAssessment(_Contract):
    schema_version: Literal["postcompile-method-assessment.v1"] = (
        "postcompile-method-assessment.v1"
    )
    method: Literal[
        "dcf_exit_multiple",
        "dcf_terminal_growth",
        "ev_adjusted_ebitda",
        "forward_pe",
    ]
    disposition: Literal["included", "cross_check", "excluded", "unavailable"]
    output_keys: tuple[str, ...]
    rationale: str = Field(min_length=1)
    limitations: str = Field(min_length=1)

    @field_validator("output_keys")
    @classmethod
    def _output_keys(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _canonical_strings(value, field_name="method output keys")


class PostcompilePriceTargetConclusion(_Contract):
    schema_version: Literal["postcompile-price-target-conclusion.v1"] = (
        "postcompile-price-target-conclusion.v1"
    )
    ranges: PriceTargetRanges
    confidence: Literal["high", "medium", "low"]
    time_horizon_months: int = Field(ge=1, le=120)
    headline_method: str = Field(min_length=1)
    investment_conclusion: str = Field(min_length=1)
    limitations: tuple[str, ...]

    @field_validator("limitations")
    @classmethod
    def _limitations(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _canonical_strings(value, field_name="conclusion limitations")


class PostcompileValuationGap(_Contract):
    schema_version: Literal["postcompile-valuation-gap.v1"] = (
        "postcompile-valuation-gap.v1"
    )
    key: str = Field(min_length=1)
    blocking: bool
    rationale: str = Field(min_length=1)
    repair_target: str = Field(min_length=1)


class PostcompileValuationTerminalSummary(_Contract):
    schema_version: Literal["postcompile-valuation-terminal-summary.v1"] = (
        "postcompile-valuation-terminal-summary.v1"
    )
    summary: str = Field(min_length=1)
    decision: str = Field(min_length=1)
    confidence: Literal["high", "medium", "low"]
    limitations: tuple[str, ...]
    recommended_next_stage: Literal[
        "model-review", "postcompile-valuation", "risk-review", "decision"
    ]

    @field_validator("limitations")
    @classmethod
    def _limitations(cls, value: tuple[str, ...]) -> tuple[str, ...]:
        return _canonical_strings(value, field_name="terminal limitations")


class PostcompileValuationStageScope(_Contract):
    schema_version: Literal["postcompile-valuation-stage-scope.v1"] = (
        "postcompile-valuation-stage-scope.v1"
    )
    user_id: str = Field(min_length=1)
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1)

    @field_validator("ticker")
    @classmethod
    def _ticker(cls, value: str) -> str:
        normalized = normalize_ticker(value)
        if normalized != value:
            raise ValueError("ticker must be canonical")
        return normalized


class PostcompileValuationStageReceipt(_Contract):
    schema_version: Literal["postcompile-valuation-stage-receipt.v1"] = (
        "postcompile-valuation-stage-receipt.v1"
    )
    status: Literal["accepted", "incomplete", "blocked"]
    scope: PostcompileValuationStageScope
    postcompile_valuation_ref: PostcompileValuationRef
    model_version: ExactModelVersionBinding
    valuation_policy_ref: ValuationPolicyRef
    valuation_policy_stage_receipt_digest: str
    compiled_valuation_snapshot_sha256: str
    price_target_id: str | None = None
    started_at: datetime
    completed_at: datetime
    receipt_digest: str

    @field_validator(
        "valuation_policy_stage_receipt_digest",
        "compiled_valuation_snapshot_sha256",
        "receipt_digest",
    )
    @classmethod
    def _digests(cls, value: str, info: ValidationInfo) -> str:
        return validate_sha256_hex(value, field_name=info.field_name)

    @field_validator("started_at", "completed_at")
    @classmethod
    def _times(cls, value: datetime, info: ValidationInfo) -> datetime:
        return _aware(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _receipt(self) -> "PostcompileValuationStageReceipt":
        if self.completed_at < self.started_at:
            raise ValueError("postcompile receipt completed_at precedes started_at")
        if (self.status == "accepted") != (self.price_target_id is not None):
            raise ValueError("only accepted postcompile receipts bind a PriceTarget")
        if self.receipt_digest != postcompile_valuation_stage_receipt_digest(self):
            raise ValueError("postcompile valuation stage receipt digest mismatch")
        return self


def postcompile_valuation_stage_receipt_digest(
    value: PostcompileValuationStageReceipt | Mapping[str, Any],
) -> str:
    payload = (
        value.model_dump(mode="json")
        if isinstance(value, PostcompileValuationStageReceipt)
        else dict(value)
    )
    payload.pop("receipt_digest", None)
    return _digest("postcompile-valuation-stage-receipt.v1", payload)

def _wacc_roll(snapshot: CompiledValuationSnapshot, months: int) -> Decimal:
    """Compound present value forward ``months`` at the compiled WACC."""

    wacc = next((row.value for row in snapshot.wacc if row.key == "wacc"), None)
    if wacc is None or wacc <= 0 or months == 0:
        return Decimal("1")
    return (Decimal("1") + wacc) ** (Decimal(months) / Decimal("12"))


def _methods_at_horizon(
    snapshot: CompiledValuationSnapshot, months: int
) -> str:
    """Forward base prices of every available method, read on one date.

    Compiled DCF prices are present values, so they roll the full horizon.
    Forward P/E and EV/EBITDA are already one year forward; they roll only the
    residual between twelve months and the stated horizon.
    """

    dcf_factor = _wacc_roll(snapshot, months)
    relative_factor = _wacc_roll(snapshot, months - 12)
    parts: list[str] = []
    for row in snapshot.methods:
        if row.prices is None:
            continue
        factor = dcf_factor if row.method.startswith("dcf_") else relative_factor
        forward = _decimal(row.prices.base * factor, quantum=_PRICE_QUANTUM)
        parts.append(f"{row.method}={forward}")
    return ", ".join(parts)


def project_postcompile_price_target(
    *,
    research_file_id: int,
    snapshot: CompiledValuationSnapshot,
    conclusion: PostcompilePriceTargetConclusion,
) -> PriceTarget:
    """Project the public PriceTarget without re-reading model or market state.

    Compiled DCF values are present values. A forward horizon rolls those
    methods, and the residual of a forward multiple method, to that one date
    at the compiled WACC. The conclusion's ranges stay the analyst's target.
    """

    model = snapshot.model_version
    current = snapshot.current_price
    midpoint = Decimal(str(conclusion.ranges.mid))
    implied_return = ((midpoint / current.value) - Decimal("1")) * Decimal("100")
    months = conclusion.time_horizon_months
    dcf_factor = _wacc_roll(snapshot, months)
    horizon_year = snapshot.first_forecast_year + (months - 1) // 12
    aligned = _methods_at_horizon(snapshot, months)
    return PriceTarget.model_validate(
        {
            "price_target_id": ids.price_target_id(
                research_file_id,
                model.model_ref.model_id,
                model.model_ref.version,
            ),
            "model_id": model.model_ref.model_id,
            "model_version": model.model_ref.version,
            "as_of": model.committed_at.astimezone(timezone.utc)
            .isoformat(timespec="milliseconds")
            .replace("+00:00", "Z"),
            "ranges": conclusion.ranges.model_dump(mode="json"),
            "confidence": conclusion.confidence,
            "method": conclusion.headline_method,
            "driver_sensitivities": [
                {
                    "driver_key": row.driver_key,
                    "delta_per_pct": float(
                        _decimal(row.delta_per_pct * dcf_factor, quantum=_PRICE_QUANTUM)
                    ),
                    "rank": row.rank,
                }
                for row in snapshot.driver_sensitivities
            ],
            "time_horizon_months": months,
            "current_price": float(current.value),
            "current_price_snapshot": CurrentPriceSnapshot(
                price=float(current.value),
                source=current.source_id,
                provider=current.provider,
                as_of=current.as_of_date.isoformat(),
                retrieved_at=current.retrieved_at.isoformat().replace("+00:00", "Z"),
                currency=current.currency,
            ).model_dump(mode="json"),
            "valuation_horizon": {
                "basis": "explicit",
                "fiscal_period": f"FY{horizon_year}",
                "months_to_period_end": months,
                "caveat": (
                    "Methods aligned to the stated horizon by the WACC roll-forward"
                    + (f": {aligned}." if aligned else ".")
                ),
            },
            "implied_return_pct": float(_decimal(implied_return)),
            "schema_version": "3.0",
        }
    )


__all__ = [
    "CompiledCurrentPrice",
    "CompiledDriverSensitivity",
    "CompiledPriceCases",
    "CompiledValuationComponent",
    "CompiledValuationMethodOutput",
    "CompiledValuationSnapshot",
    "ExactModelVersionBinding",
    "PostcompileMethodAssessment",
    "PostcompilePriceTargetConclusion",
    "PostcompileValuationGap",
    "PostcompileValuationMethodologyPin",
    "PostcompileValuationRef",
    "PostcompileValuationStageReceipt",
    "PostcompileValuationStageScope",
    "PostcompileValuationTerminalSummary",
    "compiled_valuation_snapshot_digest",
    "issue_postcompile_valuation_id",
    "postcompile_valuation_stage_receipt_digest",
    "project_postcompile_price_target",
]
