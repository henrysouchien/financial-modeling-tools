"""Strict public earnings-review report contract."""

from __future__ import annotations

from datetime import datetime
from typing import Annotated, Literal, TypeAlias

from pydantic import BaseModel, ConfigDict, Field, model_validator
from value_semantics_core.period_ref import PeriodRef

from .build_model_product_contracts import ModelVersion
from .numeric_display import NumericDisplaySpec
from .deliverable_binding import DeliverableModelBinding
from .semantic_invariants import evaluate_registered_semantic_invariants


class _StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class SourceAsOf(_StrictModel):
    method: Literal["producer_observed_at"]
    observed_at_utc: datetime


class DerivedAsOf(_StrictModel):
    method: Literal["latest_required_source_timestamp"]
    source_timestamps_utc: tuple[datetime, ...] = Field(min_length=1)
    derived_at_utc: datetime

    @model_validator(mode="after")
    def _derived_time(self) -> DerivedAsOf:
        if self.derived_at_utc < max(self.source_timestamps_utc):
            raise ValueError("derived_at_utc precedes a source timestamp")
        return self


AsOf: TypeAlias = Annotated[
    SourceAsOf | DerivedAsOf, Field(discriminator="method")
]


class DateRange(_StrictModel):
    start_utc: datetime
    end_utc: datetime

    @model_validator(mode="after")
    def _ordered(self) -> DateRange:
        if self.start_utc >= self.end_utc:
            raise ValueError("date range requires start before end")
        return self


class EarningsReviewNumericValue(_StrictModel):
    value: str = Field(
        pattern=r"^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?$"
    )
    display: NumericDisplaySpec
    rendered_text: str = Field(min_length=1)


class EarningsScorecardRow(_StrictModel):
    metric: str = Field(min_length=1)
    reported: EarningsReviewNumericValue
    yoy_delta: EarningsReviewNumericValue | None = None
    outcome: Literal["beat", "miss", "in_line", "not_comparable"]
    outcome_explanation: str = Field(min_length=1)
    source_refs: tuple[str, ...] = Field(min_length=1, max_length=4)


class ThesisReconciliationRow(_StrictModel):
    thesis_point: str = Field(min_length=1)
    evidence: str = Field(min_length=1)
    status: Literal[
        "confirmed",
        "improved",
        "unchanged",
        "weakened",
        "broken",
        "insufficient_evidence",
    ]
    direction: Literal["positive", "neutral", "negative", "unresolved"]
    source_refs: tuple[str, ...] = Field(min_length=1, max_length=4)

    @model_validator(mode="after")
    def _status_direction(self) -> ThesisReconciliationRow:
        expected = {
            "confirmed": "positive",
            "improved": "positive",
            "unchanged": "neutral",
            "weakened": "negative",
            "broken": "negative",
            "insufficient_evidence": "unresolved",
        }[self.status]
        if self.direction != expected:
            raise ValueError("thesis direction does not match status")
        return self


class EarningsReviewHeader(_StrictModel):
    ticker: str = Field(pattern=r"^[A-Z0-9.-]+$")
    title: str = Field(min_length=1)
    period: PeriodRef
    period_frame: Literal["fiscal"]
    frequency: Literal["quarterly", "annual"]
    actual_estimate_status: Literal["actual", "estimate"]
    as_of: AsOf
    observation_range: DateRange | None = None
    path: Literal["A", "B", "C", "D"]
    verdict: Literal[
        "FRESH_REVIEW_COMPLETE",
        "REFRESH_COMPLETE",
        "PRE_PRINT_BOOTSTRAP",
        "FAIL_CLOSED",
        "INSUFFICIENT_DATA",
    ]
    confidence: Literal["HIGH", "MEDIUM", "LOW"]
    summary: str = Field(min_length=1)
    thesis_status: Literal[
        "confirmed",
        "improved",
        "unchanged",
        "weakened",
        "broken",
        "insufficient_evidence",
    ]
    thesis_direction: Literal["positive", "neutral", "negative", "unresolved"]
    portfolio_action: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _period_semantics(self) -> EarningsReviewHeader:
        expected_frequency = (
            "quarterly"
            if self.period.kind == "fiscal_quarter"
            else "annual"
            if self.period.kind == "fiscal_year"
            else None
        )
        if expected_frequency is None or self.frequency != expected_frequency:
            raise ValueError(
                "earnings review requires matching fiscal period/frequency"
            )
        expected_direction = {
            "confirmed": "positive",
            "improved": "positive",
            "unchanged": "neutral",
            "weakened": "negative",
            "broken": "negative",
            "insufficient_evidence": "unresolved",
        }[self.thesis_status]
        if self.thesis_direction != expected_direction:
            raise ValueError("thesis direction does not match thesis status")
        return self


class EarningsReviewDownstreamRoute(_StrictModel):
    source: Literal["model_update_route"]
    status: Literal[
        "MODEL_UPDATE_REQUIRED",
        "MODEL_UPDATE_NOT_REQUIRED",
        "NO_CURRENT_MODEL",
        "UNKNOWN_NO_RESEARCH_FILE",
    ]
    research_file_id: int | None = Field(default=None, gt=0)
    current_model: ModelVersion | None = None
    pointer_epoch: int | None = Field(default=None, ge=0)
    reason: str | None = Field(default=None, min_length=1)
    next_skill: str | None = Field(default=None, min_length=1)
    sequence: tuple[str, ...]
    blocks_until_complete: tuple[str, ...]

    @model_validator(mode="after")
    def _declared_route(self) -> EarningsReviewDownstreamRoute:
        current_model_required = self.status in {
            "MODEL_UPDATE_REQUIRED",
            "MODEL_UPDATE_NOT_REQUIRED",
        }
        if current_model_required:
            if self.current_model is None:
                raise ValueError(
                    f"{self.status} requires an exact current ModelVersion"
                )
            if self.pointer_epoch is None:
                raise ValueError(
                    f"{self.status} requires the current pointer_epoch"
                )
            if self.current_model.research_file_id != self.research_file_id:
                raise ValueError(
                    "current_model research_file_id must match downstream route"
                )
        elif self.current_model is not None or self.pointer_epoch is not None:
            raise ValueError(
                f"{self.status} forbids current_model and pointer_epoch"
            )
        if self.status == "MODEL_UPDATE_NOT_REQUIRED":
            expected_sequence = (
                "scenario-multiple-pricing",
                "dcf-relative-valuation",
                "expected-value-decision",
                "thesis-review",
                "position-reassess",
            )
            if (
                self.sequence != expected_sequence
                or self.blocks_until_complete != expected_sequence[1:]
            ):
                raise ValueError("invalid declared MODEL_UPDATE_NOT_REQUIRED route")
            if self.next_skill != self.sequence[0]:
                raise ValueError("next_skill must be the first declared route step")
            return self
        if self.status == "UNKNOWN_NO_RESEARCH_FILE":
            if self.next_skill is not None or self.sequence:
                raise ValueError("unknown research file cannot declare a next skill")
            if not self.blocks_until_complete:
                raise ValueError("unknown research file route must declare blockers")
            return self
        if self.status == "MODEL_UPDATE_REQUIRED":
            required = (
                "model-update",
                "scenario-multiple-pricing",
                "dcf-relative-valuation",
                "expected-value-decision",
                "thesis-review",
                "position-reassess",
            )
            expected_blocks = required[1:]
        elif self.sequence and self.sequence[1:2] == ("forecast-assumptions",):
            # A report persisted while the route still named the retired
            # workbook-first forecast stage keeps reading; post_earnings_route
            # checks a declared sequence against its own and dispatches none
            # from it.
            # Persisted-data compatibility: this read stays for stored reports.
            required = (
                "build-model",
                "forecast-assumptions",
                "model-update",
                "scenario-multiple-pricing",
                "dcf-relative-valuation",
                "expected-value-decision",
                "thesis-review",
                "position-reassess",
            )
            expected_blocks = required[3:]
        else:
            required = (
                "build-model",
                "model-update",
                "scenario-multiple-pricing",
                "dcf-relative-valuation",
                "expected-value-decision",
                "thesis-review",
                "position-reassess",
            )
            expected_blocks = required[2:]
        if (
            self.sequence != required
            or self.next_skill != required[0]
            or self.blocks_until_complete != expected_blocks
        ):
            raise ValueError(f"invalid declared {self.status} route")
        return self


class EarningsReviewReportV1(_StrictModel):
    contract_name: Literal["earnings-review-report"]
    schema_version: Literal["1"]
    review: EarningsReviewHeader
    scorecard: tuple[EarningsScorecardRow, ...] = Field(max_length=7)
    thesis_reconciliation: tuple[ThesisReconciliationRow, ...] = Field(max_length=4)
    downstream_route: EarningsReviewDownstreamRoute
    recommended_next_action: str | None = Field(default=None, min_length=1)
    #: The ModelVersion this review was cut from and the Question it answers
    #: (W5d); it is the route's ``current_model`` when the route names one. The
    #: report door binds it from that route when the judgment omits it; reviews
    #: persisted before it decode without one.
    model_binding: DeliverableModelBinding | None = Field(
        default=None,
        exclude_if=lambda value: value is None,
    )

    @model_validator(mode="after")
    def _semantic_invariants(self) -> "EarningsReviewReportV1":
        current = self.downstream_route.current_model
        if self.model_binding is not None and current is not None and (
            self.model_binding.research_file_id != current.research_file_id
            or self.model_binding.model_version.model_id != current.model_id
            or self.model_binding.model_version.version != current.version
        ):
            raise ValueError(
                "model_binding must name the downstream route's current_model; "
                "a review names one ModelVersion"
            )
        evaluate_registered_semantic_invariants(
            self.model_dump(mode="json"),
            contract_name=self.contract_name,
            schema_version=self.schema_version,
        )
        return self


__all__ = [
    "AsOf",
    "DateRange",
    "EarningsReviewNumericValue",
    "EarningsReviewReportV1",
    "EarningsScorecardRow",
    "ThesisReconciliationRow",
]
