"""Strict consumer boundary for EDGAR metric-series semantic DTO v1."""

from __future__ import annotations

from collections.abc import Mapping
import copy
from datetime import date, datetime, timedelta
from typing import Any, Literal, cast, get_args

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator
from value_semantics_core import PeriodRef, Quantity

from schema.provider_port.contracts import ReportedPeriodType


EDGAR_METRIC_SERIES_CONTRACT_VERSION = "edgar-metric-series.v1"
EDGAR_METRIC_SERIES_ENTRY_CONTRACT_VERSION = "edgar-metric-series-entry.v1"
EDGAR_METRIC_SERIES_PRODUCER_ID = "edgar_updater"
NTM_ANCHOR_EDGAR_METRIC_NAME = (
    "RevenueFromContractWithCustomerExcludingAssessedTax"
)
#: Entry-status vocabulary for ``edgar-metric-series-entry.v1`` rows. This
#: Literal is the single owner of the vocabulary; consumers derive their
#: status sets from these constants instead of hand-copying the values.
EdgarMetricSeriesEntryStatus = Literal["ok", "missing", "locked", "error"]
METRIC_SERIES_ENTRY_STATUSES: frozenset[str] = frozenset(
    get_args(EdgarMetricSeriesEntryStatus)
)
#: The entry statuses counted as failed periods (the producer's own
#: ``periods_failed`` counter contract).
METRIC_SERIES_ENTRY_FAILED_STATUSES: frozenset[str] = frozenset(
    {"error", "locked"}
)
_VALIDATED_ENTRY_KEY = "__validated_edgar_metric_series_entry_v1__"
_METRIC_SERIES_REQUEST_PARAM_KEYS = frozenset(
    {
        "ticker",
        "metric_name",
        "end_year",
        "end_quarter",
        "periods",
        "full_year_mode",
        "include_equivalents",
    }
)


class EdgarMetricSeriesContractError(ValueError):
    """The EDGAR producer payload violates the pinned semantic DTO."""


def expected_ntm_anchor_metric_series_request_params(
    ticker: str,
    *,
    as_of_close: date,
    full_year_mode: bool,
) -> dict[str, object]:
    """Return the controlled raw-response window used for NTM anchor replay."""

    if not ticker.strip():
        raise ValueError("NTM anchor ticker must be non-empty")
    return {
        "ticker": ticker,
        "metric_name": NTM_ANCHOR_EDGAR_METRIC_NAME,
        "end_year": as_of_close.year + 1,
        "end_quarter": 4,
        "periods": 2 if full_year_mode else 8,
        "full_year_mode": "true" if full_year_mode else "false",
        "include_equivalents": "true",
    }


class _StrictWireModel(BaseModel):
    model_config = ConfigDict(extra="allow", frozen=True, strict=True)


def _canonical_iso_date(value: str | None, *, field_name: str) -> date | None:
    if value is None:
        return None
    try:
        parsed = date.fromisoformat(value)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a canonical ISO date") from exc
    if parsed.isoformat() != value:
        raise ValueError(f"{field_name} must be a canonical ISO date")
    return parsed


def _canonical_utc_datetime(
    value: str | None,
    *,
    field_name: str,
) -> datetime | None:
    if value is None:
        return None
    if not value.endswith("Z"):
        raise ValueError(f"{field_name} must be a canonical UTC datetime")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be a canonical UTC datetime") from exc
    if parsed.tzinfo is None or parsed.utcoffset() != timedelta(0):
        raise ValueError(f"{field_name} must be a canonical UTC datetime")
    return parsed


class EdgarMetricSeriesSourceIdentityV1(_StrictWireModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    producer_id: Literal["edgar_updater"]
    fact_source: str | None = None
    requested_source: str | None = None
    metric_tag: str = Field(min_length=1)
    equivalence_group_id: str | None = None
    accession: str | None = Field(default=None, max_length=128)
    filing_date: str | None = Field(default=None, max_length=10)
    report_date: str | None = Field(default=None, max_length=10)
    filing_url: str | None = Field(default=None, max_length=2048)
    accepted_at: str | None = Field(default=None, max_length=40)

    @model_validator(mode="after")
    def _validate_filing_dates(self) -> "EdgarMetricSeriesSourceIdentityV1":
        _canonical_iso_date(self.filing_date, field_name="filing_date")
        _canonical_iso_date(self.report_date, field_name="report_date")
        _canonical_utc_datetime(self.accepted_at, field_name="accepted_at")
        return self


class EdgarMetricSeriesEntryV1(_StrictWireModel):
    contract_version: Literal["edgar-metric-series-entry.v1"]
    period_ref: PeriodRef
    year: int
    quarter: int = Field(ge=1, le=4)
    status: EdgarMetricSeriesEntryStatus
    quantity: Quantity | None
    calculation_ready: bool
    semantic_error: Literal["unit_unavailable", "value_unavailable"] | None
    date_type: str | None = None
    filing_type: str | None = None
    period_start: str | None = None
    period_end: str | None
    metric_tag: str | None = None
    equivalence_group_id: str | None = None
    source_identity: EdgarMetricSeriesSourceIdentityV1 | None
    source_ref: dict[str, Any] | None = None

    @model_validator(mode="after")
    def _validate_semantics(self) -> "EdgarMetricSeriesEntryV1":
        fiscal_year = getattr(self.period_ref, "fiscal_year", None)
        if fiscal_year != self.year:
            raise ValueError("period_ref fiscal_year must match year")
        if self.period_ref.kind == "fiscal_quarter" and (
            getattr(self.period_ref, "quarter", None) != self.quarter
        ):
            raise ValueError("period_ref quarter must match quarter")
        if self.period_ref.kind not in {"fiscal_year", "fiscal_quarter"}:
            raise ValueError("metric-series period_ref must be fiscal")

        ready = self.status == "ok" and self.quantity is not None
        if self.calculation_ready != ready:
            raise ValueError(
                "calculation_ready must match status and quantity availability"
            )
        if self.quantity is None and self.semantic_error is None:
            raise ValueError("unavailable quantity requires semantic_error")
        if self.quantity is not None and self.semantic_error is not None:
            raise ValueError("available quantity cannot carry semantic_error")

        period_start = _canonical_iso_date(
            self.period_start,
            field_name="period_start",
        )
        period_end = _canonical_iso_date(self.period_end, field_name="period_end")
        if (
            period_start is not None
            and period_end is not None
            and period_start > period_end
        ):
            raise ValueError("period_start must not be after period_end")

        if self.status == "ok":
            if self.metric_tag is None or self.source_identity is None:
                raise ValueError(
                    "successful entry requires metric and source identity"
                )
            if self.source_identity.metric_tag != self.metric_tag:
                raise ValueError("source_identity metric_tag must match metric_tag")
            if (
                self.source_identity.equivalence_group_id
                != self.equivalence_group_id
            ):
                raise ValueError(
                    "source_identity equivalence_group_id must match "
                    "equivalence_group_id"
                )
        elif self.quantity is not None or self.source_identity is not None:
            raise ValueError(
                "unsuccessful entry cannot publish quantity or source identity"
            )
        return self

    @property
    def reported_period_start(self) -> date | None:
        return _canonical_iso_date(self.period_start, field_name="period_start")

    @property
    def source_accepted_at(self) -> datetime | None:
        identity = self.source_identity
        if identity is None:
            return None
        return _canonical_utc_datetime(
            identity.accepted_at,
            field_name="accepted_at",
        )

    def _has_bound_filing_identity(self, *, require_acceptance: bool) -> bool:
        identity = self.source_identity
        source_ref = self.source_ref
        if identity is None or not isinstance(source_ref, dict):
            return False
        current = source_ref.get("current")
        if not isinstance(current, dict):
            return False
        bound_identity = {
            "accession": identity.accession,
            "filing_date": identity.filing_date,
            "report_date": identity.report_date,
            "filing_url": identity.filing_url,
        }
        current_accepted_at = current.get("accepted_at")
        if current_accepted_at is not None and not isinstance(
            current_accepted_at,
            str,
        ):
            return False
        try:
            bound_accepted_at = _canonical_utc_datetime(
                current_accepted_at,
                field_name="source_ref.current.accepted_at",
            )
        except ValueError:
            return False
        identity_accepted_at = self.source_accepted_at
        if require_acceptance and identity_accepted_at is None:
            return False
        return bool(
            self.status == "ok"
            and self.calculation_ready
            and self.reported_period_start is not None
            and self.period_end is not None
            and self.reported_period_start < date.fromisoformat(self.period_end)
            and identity.fact_source == "xbrl"
            and identity.accession is not None
            and identity.accession.strip()
            and identity.filing_date is not None
            and identity.report_date == self.period_end
            and identity.filing_url is not None
            and identity.filing_url.strip()
            and source_ref.get("type") == "xbrl_context"
            and all(current.get(key) == value for key, value in bound_identity.items())
            and bound_accepted_at == identity_accepted_at
        )

    @property
    def reported_duration_period_type(self) -> ReportedPeriodType | None:
        """Exact reported duration represented by the selected source row."""

        if (
            self.period_ref.kind == "fiscal_year"
            and self.date_type == "FY"
            and self.filing_type in {"10-K", "10-K/A"}
            and self._has_bound_filing_identity(require_acceptance=False)
        ):
            return "FY"
        if (
            self.period_ref.kind == "fiscal_quarter"
            and self.date_type == "Q"
            and self.filing_type == "10-Q"
            and self._has_bound_filing_identity(require_acceptance=True)
        ):
            return cast(ReportedPeriodType, f"Q{self.quarter}")
        return None

    @property
    def has_reported_annual_duration_source(self) -> bool:
        """Compatibility view over the generalized duration provenance marker."""

        return self.reported_duration_period_type == "FY"

    @property
    def has_reported_quarter_duration_source(self) -> bool:
        return self.reported_duration_period_type in {"Q1", "Q2", "Q3", "Q4"}


class EdgarMetricSeriesResponseV1(_StrictWireModel):
    contract_version: Literal["edgar-metric-series.v1"]
    status: Literal["success", "partial", "error"]
    ticker: str
    metric_name: str
    full_year_mode: bool
    periods_requested: int = Field(ge=0)
    periods_returned: int = Field(ge=0)
    periods_failed: int = Field(ge=0)
    periods_missing: int = Field(ge=0)
    series: list[EdgarMetricSeriesEntryV1]

    @model_validator(mode="after")
    def _validate_period_mode(self) -> "EdgarMetricSeriesResponseV1":
        expected_kind = "fiscal_year" if self.full_year_mode else "fiscal_quarter"
        if any(entry.period_ref.kind != expected_kind for entry in self.series):
            raise ValueError("series period_ref kind must match full_year_mode")
        return self


def _ticker_identity(value: str) -> str:
    return value.upper().replace(".", "-").replace("/", "-")


def validate_edgar_metric_series_request_window(
    response: EdgarMetricSeriesResponseV1,
    request_params: Mapping[str, object],
) -> None:
    """Bind one producer response to its exact requested fiscal-period grid."""

    if set(request_params) != _METRIC_SERIES_REQUEST_PARAM_KEYS:
        raise EdgarMetricSeriesContractError(
            "EDGAR metric-series request parameters have invalid shape"
        )
    request_ticker = request_params.get("ticker")
    request_metric_name = request_params.get("metric_name")
    if not isinstance(request_ticker, str) or not request_ticker.strip():
        raise EdgarMetricSeriesContractError(
            "EDGAR metric-series request ticker must be non-empty"
        )
    if not isinstance(request_metric_name, str) or not request_metric_name.strip():
        raise EdgarMetricSeriesContractError(
            "EDGAR metric-series request metric_name must be non-empty"
        )
    end_year = cast(int, request_params.get("end_year"))
    end_quarter = cast(int, request_params.get("end_quarter"))
    periods = cast(int, request_params.get("periods"))
    raw_full_year_mode = request_params.get("full_year_mode")
    if end_quarter not in {1, 2, 3, 4}:
        raise EdgarMetricSeriesContractError(
            "EDGAR metric-series request end_quarter must be between 1 and 4"
        )
    if raw_full_year_mode not in {"true", "false"}:
        raise EdgarMetricSeriesContractError(
            "EDGAR metric-series request full_year_mode must be true or false"
        )
    full_year_mode = raw_full_year_mode == "true"
    if request_params.get("include_equivalents") not in {"true", "false"}:
        raise EdgarMetricSeriesContractError(
            "EDGAR metric-series request include_equivalents must be true or false"
        )

    if full_year_mode:
        expected_periods = tuple(
            (year, 4)
            for year in range(end_year - periods + 1, end_year + 1)
        )
    else:
        reverse_periods: list[tuple[int, int]] = []
        year = end_year
        quarter = end_quarter
        for _ in range(periods):
            reverse_periods.append((year, quarter))
            quarter -= 1
            if quarter < 1:
                year -= 1
                quarter = 4
        expected_periods = tuple(reversed(reverse_periods))

    actual_periods = tuple((entry.year, entry.quarter) for entry in response.series)
    if actual_periods != expected_periods:
        raise EdgarMetricSeriesContractError(
            "EDGAR metric-series rows must exactly match the requested fiscal "
            "periods in canonical order"
        )
    if response.full_year_mode is not full_year_mode:
        raise EdgarMetricSeriesContractError(
            "EDGAR metric-series response full_year_mode does not match request"
        )
    if response.periods_requested != periods:
        raise EdgarMetricSeriesContractError(
            "EDGAR metric-series response periods_requested does not match request"
        )
    if _ticker_identity(response.ticker) != _ticker_identity(request_ticker):
        raise EdgarMetricSeriesContractError(
            "EDGAR metric-series response ticker does not match request"
        )
    if response.metric_name != request_metric_name:
        raise EdgarMetricSeriesContractError(
            "EDGAR metric-series response metric_name does not match request"
        )

    periods_returned = sum(entry.status == "ok" for entry in response.series)
    periods_missing = sum(entry.status == "missing" for entry in response.series)
    periods_failed = sum(
        entry.status in METRIC_SERIES_ENTRY_FAILED_STATUSES
        for entry in response.series
    )
    if (
        response.periods_returned != periods_returned
        or response.periods_missing != periods_missing
        or response.periods_failed != periods_failed
    ):
        raise EdgarMetricSeriesContractError(
            "EDGAR metric-series response counters must equal row statuses"
        )
    expected_status = (
        "success"
        if periods_returned == periods
        else "error"
        if periods_returned == 0
        else "partial"
    )
    if response.status != expected_status:
        raise EdgarMetricSeriesContractError(
            "EDGAR metric-series response status must match returned rows"
        )


def validate_edgar_metric_series_payload(
    payload: dict[str, Any],
    *,
    expected_ticker: str | None = None,
    expected_metric_name: str | None = None,
    expected_request_params: Mapping[str, object] | None = None,
) -> dict[str, Any]:
    """Validate producer semantics and attach typed entries for internal parsing.

    Locally synthesized transport-error envelopes predate the producer DTO and
    carry no successful rows. They remain transport failures, not producer
    payloads, and therefore bypass semantic validation.
    """

    if payload.get("status") == "error" and "contract_version" not in payload:
        return payload

    try:
        response = EdgarMetricSeriesResponseV1.model_validate(payload)
    except ValidationError as exc:
        raise EdgarMetricSeriesContractError(str(exc)) from exc
    if (
        expected_ticker is not None
        and _ticker_identity(response.ticker) != _ticker_identity(expected_ticker)
    ):
        raise EdgarMetricSeriesContractError(
            "EDGAR metric-series response ticker does not match request"
        )
    if (
        expected_metric_name is not None
        and response.metric_name != expected_metric_name
    ):
        raise EdgarMetricSeriesContractError(
            "EDGAR metric-series response metric_name does not match request"
        )
    if expected_request_params is not None:
        validate_edgar_metric_series_request_window(
            response,
            expected_request_params,
        )
    validated = copy.deepcopy(payload)
    raw_series = validated.get("series")
    if not isinstance(raw_series, list) or len(raw_series) != len(response.series):
        raise ValueError("validated EDGAR metric series lost entry identity")
    for raw_entry, typed_entry in zip(raw_series, response.series, strict=True):
        if not isinstance(raw_entry, dict):
            raise ValueError("EDGAR metric-series entries must be objects")
        raw_entry[_VALIDATED_ENTRY_KEY] = typed_entry
    return validated


def validated_metric_series_entry(
    entry: dict[str, Any],
) -> EdgarMetricSeriesEntryV1 | None:
    candidate = entry.get(_VALIDATED_ENTRY_KEY)
    return candidate if isinstance(candidate, EdgarMetricSeriesEntryV1) else None


__all__ = [
    "EDGAR_METRIC_SERIES_CONTRACT_VERSION",
    "EDGAR_METRIC_SERIES_ENTRY_CONTRACT_VERSION",
    "EDGAR_METRIC_SERIES_PRODUCER_ID",
    "METRIC_SERIES_ENTRY_FAILED_STATUSES",
    "METRIC_SERIES_ENTRY_STATUSES",
    "NTM_ANCHOR_EDGAR_METRIC_NAME",
    "EdgarMetricSeriesContractError",
    "EdgarMetricSeriesEntryStatus",
    "EdgarMetricSeriesEntryV1",
    "EdgarMetricSeriesResponseV1",
    "EdgarMetricSeriesSourceIdentityV1",
    "expected_ntm_anchor_metric_series_request_params",
    "validate_edgar_metric_series_payload",
    "validate_edgar_metric_series_request_window",
    "validated_metric_series_entry",
]
