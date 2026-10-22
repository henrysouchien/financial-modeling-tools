from __future__ import annotations

from datetime import date
from decimal import Decimal
import re
from typing import Annotated, ClassVar, Literal, TypeAlias

from pydantic import Field, field_validator, model_validator
from value_semantics_core.decimal_wire import CanonicalDecimalWire

from .numeric_display import NumericDisplaySpec
from .deliverable_binding import DeliverableModelBinding
from industry_slice_core._contract import _ContractModel
from .value_semantics_periods import (
    CANONICAL_PERIOD_LABEL_RE,
    validate_canonical_period_label,
)


PreviewVerdict = Literal[
    "EARNINGS_PREVIEW_COMPILED",
    "INSUFFICIENT_ARTIFACTS",
    "INSUFFICIENT_DATA",
]
PreviewConfidence = Literal["HIGH", "MEDIUM", "LOW"]
PreviewStatus = Literal["positive", "negative", "neutral", "warn"]

REQUIRED_SOURCE_SKILLS = frozenset(
    {"model-vs-consensus", "earnings-scenarios", "guidance-extraction"}
)
ISO_DATE_PATTERN = r"^\d{4}-\d{2}-\d{2}$"
def _validate_iso_date(value: str, *, field_name: str = "as_of") -> str:
    if len(value) != 10 or value[4] != "-" or value[7] != "-":
        raise ValueError(f"{field_name} must use ISO YYYY-MM-DD grammar")
    try:
        date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a valid ISO date") from exc
    return value


EarningsPreviewNumericKind = Literal[
    "currency",
    "percentage",
    "basis_points",
    "multiple",
    "count",
    "per_share",
    "ratio",
    "index",
    "duration",
    "weight",
]


class EarningsPreviewPercentageDisplaySpec(NumericDisplaySpec, frozen=True):
    """Percentage-only display contract visible in generated JSON Schema."""

    numeric_kind: Literal["percentage"]
    scale: Literal["unit"] = "unit"
    currency: None = None
    denominator_basis: None = None
    unit_label: None = None


class EarningsPreviewNumericDisplaySpec(NumericDisplaySpec, frozen=True):
    """Display semantics for Decimal quantities; date is a separate wire type."""

    numeric_kind: EarningsPreviewNumericKind


class EarningsPreviewNumericPoint(_ContractModel):
    quantity_bindings: ClassVar[tuple[tuple[str, str], ...]] = (("value", "display"),)

    kind: Literal["point"]
    value: CanonicalDecimalWire
    display: EarningsPreviewNumericDisplaySpec

    @model_validator(mode="after")
    def _validate_value_display_pair(self) -> "EarningsPreviewNumericPoint":
        _validate_bound_numeric(self.value, self.display, field_name="value")
        return self


class EarningsPreviewNumericRange(_ContractModel):
    quantity_bindings: ClassVar[tuple[tuple[str, str], ...]] = (
        ("low", "display"),
        ("high", "display"),
    )

    kind: Literal["range"]
    low: CanonicalDecimalWire
    high: CanonicalDecimalWire
    display: EarningsPreviewNumericDisplaySpec

    @model_validator(mode="after")
    def _ordered_range(self) -> "EarningsPreviewNumericRange":
        if self.low > self.high:
            raise ValueError("numeric range low cannot exceed high")
        _validate_bound_numeric(self.low, self.display, field_name="low")
        _validate_bound_numeric(self.high, self.display, field_name="high")
        return self


class EarningsPreviewTextCell(_ContractModel):
    # This union variant carries no numeric fields, so it has no display binding.

    kind: Literal["text"]
    text_kind: Literal["unavailable", "qualitative", "not_applicable"]
    text: str = Field(..., min_length=1)


EarningsPreviewCell: TypeAlias = Annotated[
    EarningsPreviewNumericPoint | EarningsPreviewNumericRange | EarningsPreviewTextCell,
    Field(discriminator="kind"),
]

_UNSIGNED_WEIGHT_BASES = frozenset(
    {"gross_exposure", "benchmark_total", "fund_total", "total_market_value"}
)


def _validate_bound_numeric(
    value: Decimal,
    display: EarningsPreviewNumericDisplaySpec,
    *,
    field_name: str,
) -> None:
    if (
        display.numeric_kind == "weight"
        and display.denominator_basis in _UNSIGNED_WEIGHT_BASES
        and not Decimal("0") <= value <= Decimal("1")
    ):
        raise ValueError(
            f"{field_name} weight for {display.denominator_basis} must be in [0, 1]"
        )


class EarningsPreviewSourceArtifact(_ContractModel):
    skill: str = Field(..., min_length=1)
    artifact_id: str = Field(..., min_length=1)
    ticker: str = Field(..., min_length=1)
    content_sha256: str = Field(
        ...,
        min_length=64,
        max_length=64,
        pattern=r"^[0-9a-f]{64}$",
    )
    schema_version: str = Field(..., min_length=1)

    @field_validator("ticker")
    @classmethod
    def _uppercase_ticker(cls, value: str) -> str:
        return value.upper()


class EarningsPreviewBottomLine(_ContractModel):
    action: str = Field(..., min_length=1)
    summary: str = Field(..., min_length=1)


class EarningsPreviewComparisonRow(_ContractModel):
    quantity_bindings: ClassVar[tuple[tuple[str, str], ...]] = (
        ("model_value", "display"),
        ("street_value", "display"),
        ("delta_pct", "delta_display"),
    )

    period: str = Field(
        ...,
        min_length=1,
        pattern=CANONICAL_PERIOD_LABEL_RE.pattern,
    )
    metric: str = Field(..., min_length=1)
    model_value: CanonicalDecimalWire
    street_value: CanonicalDecimalWire
    delta_pct: CanonicalDecimalWire | None = None
    display: EarningsPreviewNumericDisplaySpec
    delta_display: EarningsPreviewPercentageDisplaySpec
    status: PreviewStatus = "neutral"

    @field_validator("period")
    @classmethod
    def _canonical_period(cls, value: str) -> str:
        validated = validate_canonical_period_label(value, field_name="period") or value
        if re.fullmatch(ISO_DATE_PATTERN, validated):
            _validate_iso_date(validated, field_name="period")
        return validated

    @model_validator(mode="after")
    def _validate_value_display_pairs(self) -> "EarningsPreviewComparisonRow":
        _validate_bound_numeric(
            self.model_value, self.display, field_name="model_value"
        )
        _validate_bound_numeric(
            self.street_value,
            self.display,
            field_name="street_value",
        )
        return self


class EarningsPreviewModelVsStreet(_ContractModel):
    as_of: str = Field(
        ...,
        min_length=10,
        max_length=10,
        pattern=ISO_DATE_PATTERN,
        json_schema_extra={"format": "date"},
    )
    basis: str = Field(..., min_length=1)
    rows: list[EarningsPreviewComparisonRow] = Field(..., min_length=1)
    narrative: str = Field(..., min_length=1)

    @field_validator("as_of")
    @classmethod
    def _canonical_as_of(cls, value: str) -> str:
        return _validate_iso_date(value)


class EarningsPreviewScenarioRow(_ContractModel):
    quantity_bindings: ClassVar[tuple[tuple[str, str], ...]] = (
        ("bull", "display"),
        ("base", "display"),
        ("bear", "display"),
    )

    metric: str = Field(..., min_length=1)
    bull: CanonicalDecimalWire
    base: CanonicalDecimalWire
    bear: CanonicalDecimalWire
    display: EarningsPreviewNumericDisplaySpec
    bull_status: PreviewStatus
    base_status: PreviewStatus
    bear_status: PreviewStatus

    @model_validator(mode="after")
    def _validate_value_display_pairs(self) -> "EarningsPreviewScenarioRow":
        for field_name in ("bull", "base", "bear"):
            _validate_bound_numeric(
                getattr(self, field_name),
                self.display,
                field_name=field_name,
            )
        return self


class EarningsPreviewScenarioFrame(_ContractModel):
    quantity_bindings: ClassVar[tuple[tuple[str, str], ...]] = (
        ("spread_pct", "spread_display"),
    )
    non_quantity_fields: ClassVar[tuple[str, ...]] = ("terminal_year",)

    as_of: str = Field(
        ...,
        min_length=10,
        max_length=10,
        pattern=ISO_DATE_PATTERN,
        json_schema_extra={"format": "date"},
    )
    terminal_year: int = Field(..., ge=2000, le=2200)
    critical_factors: list[str] = Field(..., min_length=1)
    rows: list[EarningsPreviewScenarioRow] = Field(..., min_length=1)
    spread_pct: CanonicalDecimalWire = Field(..., ge=0)
    spread_display: EarningsPreviewPercentageDisplaySpec
    spread_passes: bool
    narrative: str = Field(..., min_length=1)

    @field_validator("as_of")
    @classmethod
    def _canonical_as_of(cls, value: str) -> str:
        return _validate_iso_date(value)


class EarningsPreviewWatchItem(_ContractModel):
    label: str = Field(..., min_length=1)
    conclusion: str = Field(..., min_length=1)
    evidence: str = Field(..., min_length=1)


class EarningsPreviewAssumptionDelta(_ContractModel):
    metric: str = Field(..., min_length=1)
    assessment: str = Field(..., min_length=1)
    suggested_action: str = Field(..., min_length=1)


class EarningsPreviewGuidanceRow(_ContractModel):
    metric: str = Field(..., min_length=1)
    period: str = Field(
        ...,
        min_length=1,
        pattern=CANONICAL_PERIOD_LABEL_RE.pattern,
    )
    guidance: EarningsPreviewCell
    confidence: str = Field(..., min_length=1)

    @field_validator("period")
    @classmethod
    def _canonical_period(cls, value: str) -> str:
        validated = validate_canonical_period_label(value, field_name="period") or value
        if re.fullmatch(ISO_DATE_PATTERN, validated):
            _validate_iso_date(validated, field_name="period")
        return validated


class EarningsPreviewQuote(_ContractModel):
    text: str = Field(..., min_length=1)
    speaker: str = Field(..., min_length=1)
    role: str = Field(..., min_length=1)
    source_ref: str = Field(..., min_length=1)


class EarningsPreviewThreeWayRow(_ContractModel):
    metric: str = Field(..., min_length=1)
    management_guide: EarningsPreviewCell
    model: EarningsPreviewCell
    street: EarningsPreviewCell
    gap: EarningsPreviewCell
    status: PreviewStatus = "neutral"


class EarningsPreviewGuidanceBacking(_ContractModel):
    as_of: str = Field(
        ...,
        min_length=10,
        max_length=10,
        pattern=ISO_DATE_PATTERN,
        json_schema_extra={"format": "date"},
    )
    context: str = Field(..., min_length=1)
    guidance_rows: list[EarningsPreviewGuidanceRow] = Field(..., min_length=1)
    quote: EarningsPreviewQuote | None = None
    three_way_rows: list[EarningsPreviewThreeWayRow] = Field(..., min_length=1)
    next_action: str = Field(..., min_length=1)

    @field_validator("as_of")
    @classmethod
    def _canonical_as_of(cls, value: str) -> str:
        return _validate_iso_date(value)


class EarningsPreviewPack(_ContractModel):
    contract_name: Literal["earnings-preview-report"]
    schema_version: Literal["2.0"]
    ticker: str = Field(..., min_length=1)
    verdict: PreviewVerdict
    confidence: PreviewConfidence
    as_of: str = Field(
        ...,
        min_length=10,
        max_length=10,
        pattern=ISO_DATE_PATTERN,
        json_schema_extra={"format": "date"},
    )
    subtitle: str = Field(..., min_length=1)
    bottom_line: EarningsPreviewBottomLine | None = None
    model_vs_street: EarningsPreviewModelVsStreet | None = None
    scenario_frame: EarningsPreviewScenarioFrame | None = None
    watch_items: list[EarningsPreviewWatchItem] = Field(default_factory=list)
    assumption_deltas: list[EarningsPreviewAssumptionDelta] = Field(
        default_factory=list
    )
    guidance_backing: EarningsPreviewGuidanceBacking | None = None
    data_gaps: list[str] = Field(default_factory=list)
    source_manifest: list[EarningsPreviewSourceArtifact] = Field(default_factory=list)
    #: The ModelVersion this preview was cut from and the Question it answers
    #: (W5d). The report door requires it on a compiled preview; previews
    #: persisted before it decode without one.
    model_binding: DeliverableModelBinding | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )
    recommended_next_action: str = Field(..., min_length=1)

    @field_validator("ticker")
    @classmethod
    def _uppercase_ticker(cls, value: str) -> str:
        return value.upper()

    @field_validator("as_of")
    @classmethod
    def _canonical_as_of(cls, value: str) -> str:
        return _validate_iso_date(value)

    @model_validator(mode="after")
    def _validate_pack(self) -> "EarningsPreviewPack":
        if self.verdict == "EARNINGS_PREVIEW_COMPILED":
            required_sections = {
                "bottom_line": self.bottom_line,
                "model_vs_street": self.model_vs_street,
                "scenario_frame": self.scenario_frame,
                "guidance_backing": self.guidance_backing,
            }
            missing = [
                name for name, value in required_sections.items() if value is None
            ]
            if missing:
                raise ValueError(
                    "compiled earnings preview missing sections: " + ", ".join(missing)
                )
            if not self.watch_items:
                raise ValueError("compiled earnings preview requires watch_items")

            skills = [source.skill for source in self.source_manifest]
            if len(skills) != len(set(skills)):
                raise ValueError("source_manifest skills must be unique")
            if set(skills) != REQUIRED_SOURCE_SKILLS:
                raise ValueError(
                    "source_manifest must contain exactly: "
                    + ", ".join(sorted(REQUIRED_SOURCE_SKILLS))
                )
            mixed = [
                source.ticker
                for source in self.source_manifest
                if source.ticker != self.ticker
            ]
            if mixed:
                raise ValueError("source_manifest ticker must match pack ticker")
        elif not self.data_gaps:
            raise ValueError("non-compiled earnings preview requires data_gaps")
        return self


__all__ = [
    "EarningsPreviewAssumptionDelta",
    "EarningsPreviewBottomLine",
    "EarningsPreviewComparisonRow",
    "EarningsPreviewCell",
    "EarningsPreviewGuidanceBacking",
    "EarningsPreviewGuidanceRow",
    "EarningsPreviewModelVsStreet",
    "EarningsPreviewNumericDisplaySpec",
    "EarningsPreviewNumericPoint",
    "EarningsPreviewNumericRange",
    "EarningsPreviewPack",
    "EarningsPreviewPercentageDisplaySpec",
    "EarningsPreviewQuote",
    "EarningsPreviewScenarioFrame",
    "EarningsPreviewScenarioRow",
    "EarningsPreviewSourceArtifact",
    "EarningsPreviewTextCell",
    "EarningsPreviewThreeWayRow",
    "EarningsPreviewWatchItem",
    "PreviewConfidence",
    "PreviewStatus",
    "PreviewVerdict",
    "REQUIRED_SOURCE_SKILLS",
]
