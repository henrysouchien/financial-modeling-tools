"""FMP adapter implementing Tier-1 provider-port capabilities."""

from __future__ import annotations

import csv
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from functools import wraps
import logging
from pathlib import Path
from typing import Any, Literal, Mapping, Protocol, TypedDict, Unpack

from schema.provider_port.adapters.fmp.mapping import (
    ConceptMapping,
    FmpFilingLineKey,
    mapping_by_filing_line,
)
from schema.provider_port.adapters.servability import (
    raise_adapter_unresolved_for_required_line,
)
from schema.provider_port.adapters.fmp.overlay import CustomBindingOverlay
from industry_slice_core.profile_normalization import (
    MALFORMED_SCALAR_ERRORS as _MALFORMED_SCALAR_ERRORS,
    company_profile_from_fmp_record,
    decimal_or_none as _decimal_or_none,
    malformed_payload_error as _malformed_payload_error,
    record_date as _record_date,
)
from schema.provider_port.adapters.fmp.quirks import (
    build_endpoint_lookup,
    _extract_numeric_value,
    scale_value,
    vendor_provenance,
)
from industry_slice_core.profile_normalization import ProviderCapability, ProviderError
from schema.provider_port.contracts import (
    EstimateFact,
    EstimateFactsResult,
    EstimatePeriod,
    FilingLineFact,
    FilingLineObservation,
    FilingLineRef,
    FiscalWindow,
    MetricSeries,
    PeerUniverse,
    PricePoint,
    PriceSeries,
    PriceSeriesBasis,
    ProviderResponseLineage,
    QualityFlag,
    Quote,
    ReportedPeriod,
    VendorProvenance,
)
from industry_slice_core.industry_types import CompanyProfile
from industry_slice_core.metric_registry import UnknownMetricIdError
from schema.provider_port.adapters.fmp.metrics import (
    build_annual_metric_series,
    build_ttm_metric_series,
    mapping_for_metric,
)
from schema.provider_port.adapters.fmp.native import (
    PRICE_FIELDS,
    extract_peer_symbols,
)
from schema.provider_port.adapters.fmp.transport import live_fetch_with_lineage
from schema.provider_port.registry import (
    AuthoritativeCapabilityRegistration,
    FMP_PROVIDER_ID,
    MissingValuePolicy,
)
from schema.provider_query_contract import (
    ArtifactRefHashBinding,
    ExplicitMissingProviderResult,
    FailedProviderResult,
    IssuerUniverseAvailableProviderResult,
    IssuerUniverseMember,
    IssuerUniverseProviderRequest,
    IssuerUniverseProviderResult,
    KnownBooleanIssuerFeature,
    KnownDecimalIssuerFeature,
    KnownTextIssuerFeature,
    UnavailableIssuerFeature,
    issuer_universe_member_digest,
    provider_request_digest,
    provider_result_digest,
)
from schema.ticker_contract import normalize_ticker
from schema.versioned_artifact import (
    VersionedArtifactRef,
    compute_artifact_content_sha256,
)
from value_semantics_core import NumericDisplaySpec, Quantity


_STATEMENT_ENDPOINTS = frozenset(
    {"income_statement", "balance_sheet", "cash_flow", "key_metrics"}
)


def _concept_capability(native_endpoint: str) -> ProviderCapability:
    if native_endpoint == "profile":
        return ProviderCapability.COMPANY_PROFILE
    if native_endpoint == "quote":
        return ProviderCapability.QUOTE
    return ProviderCapability.STATEMENT_FACTS


def _is_missing_or_zero(record: dict | None, field: str) -> bool:
    if record is None:
        return True
    value = record.get(field)
    if value is None:
        return True
    if isinstance(value, bool):
        return False
    try:
        return float(value) == 0.0
    except (TypeError, ValueError):
        return False


def _records_for_ticker(records: tuple[dict, ...], ticker: str) -> list[dict]:
    """Filter combined replay batches while preserving endpoint-local fixtures."""

    labeled = [
        record
        for record in records
        if str(record.get("symbol") or record.get("ticker") or "").strip()
    ]
    if not labeled:
        return list(records)
    normalized = normalize_ticker(ticker)
    return [
        record
        for record in labeled
        if normalize_ticker(str(record.get("symbol") or record.get("ticker")))
        == normalized
    ]


def _record_indices_for_ticker(
    records: tuple[dict, ...],
    ticker: str,
) -> list[tuple[int, dict]]:
    """Retain response row indices so scalar lineage coordinates stay exact."""

    labeled = [
        (index, record)
        for index, record in enumerate(records)
        if str(record.get("symbol") or record.get("ticker") or "").strip()
    ]
    if not labeled:
        return list(enumerate(records))
    normalized = normalize_ticker(ticker)
    return [
        (index, record)
        for index, record in labeled
        if normalize_ticker(str(record.get("symbol") or record.get("ticker")))
        == normalized
    ]


def _first_decimal_field(
    record: dict[str, Any],
    fields: tuple[str, ...],
) -> tuple[str, Decimal] | None:
    """Select the first finite numeric native field without a float round-trip."""

    for field_name in fields:
        raw_value = record.get(field_name)
        if raw_value is None or isinstance(raw_value, bool):
            continue
        try:
            value = Decimal(str(raw_value))
        except ArithmeticError:
            continue
        if value.is_finite():
            return field_name, value
    return None


def _optional_fiscal_year(record: dict[str, Any]) -> int | None:
    raw_value = record.get("fiscalYear")
    if raw_value in (None, ""):
        return None
    value = Decimal(str(raw_value))
    if not value.is_finite() or value != value.to_integral_value():
        raise ValueError("fiscalYear must be an integer when present")
    return int(value)


def _optional_reported_period(record: dict[str, Any]) -> Literal[
    "FY", "Q1", "Q2", "Q3", "Q4"
] | None:
    raw_value = record.get("period")
    if raw_value in (None, ""):
        return None
    value = str(raw_value).strip().upper()
    if value not in {"FY", "Q1", "Q2", "Q3", "Q4"}:
        raise ValueError("period must be FY or Q1-Q4 when present")
    return value  # type: ignore[return-value]


def _provider_response_lineage(
    batch: FetchedRecords,
    *,
    native_endpoint: str,
) -> ProviderResponseLineage | None:
    lineage_shape = (
        batch.observed_at,
        batch.validated_params,
        batch.fetch_source,
        batch.lineage_descriptor,
    )
    if all(item is None for item in lineage_shape):
        return None
    if any(item is None for item in lineage_shape):
        raise ValueError("provider response lineage fields must be all present or absent")
    observed_at = batch.observed_at
    if not isinstance(observed_at, datetime):
        raise TypeError("provider response observed_at must be datetime")
    fetch_source = batch.fetch_source
    if fetch_source not in ("live", "cache"):
        raise ValueError("provider response fetch_source must be live or cache")
    descriptor = dict(batch.lineage_descriptor or {})
    if descriptor.get("endpoint") != native_endpoint:
        raise ValueError("lineage descriptor endpoint must match requested endpoint")
    response_sha256 = descriptor.get("response_sha256")
    if not isinstance(response_sha256, str):
        raise ValueError("lineage descriptor must include response_sha256")
    raw_values = descriptor.get("values")
    if not isinstance(raw_values, list):
        raise ValueError("lineage descriptor must include scalar values")
    scalar_result_keys: list[str] = []
    for scalar in raw_values:
        if not isinstance(scalar, Mapping):
            raise TypeError("lineage scalar coordinate must be a mapping")
        result_key = scalar.get("result_key")
        if not isinstance(result_key, str) or not result_key.strip():
            raise ValueError("lineage scalar coordinate must include result_key")
        scalar_result_keys.append(result_key)
    params = tuple(sorted(dict(batch.validated_params or {}).items()))
    return ProviderResponseLineage(
        provider=FMP_PROVIDER_ID,
        native_endpoint=native_endpoint,
        response_observed_at=observed_at,
        normalized_response_sha256=response_sha256,
        validated_params=params,
        fetch_source=fetch_source,
        scalar_result_keys=tuple(scalar_result_keys),
    )


def _require_issued_result_keys(
    lineage: ProviderResponseLineage | None,
    result_keys: Sequence[str | None],
) -> None:
    if lineage is None:
        return
    issued = frozenset(lineage.scalar_result_keys)
    missing = sorted(
        result_key
        for result_key in result_keys
        if result_key is not None and result_key not in issued
    )
    if missing:
        raise ValueError(
            "normalized values reference scalar keys absent from provider lineage: "
            + ", ".join(missing)
        )


def _ttm_records_with_observed_at(
    records: list[dict],
    observed_at: datetime | None,
) -> list[dict]:
    """Date snapshot-style TTM rows from transport observation evidence.

    FMP's current ``key_metrics_ttm`` response is a point-in-time snapshot and
    may omit every fiscal/date field.  The transport observation time is the
    adapter-owned as-of evidence for those rows.  Explicit temporal fields,
    including malformed ones, remain untouched so malformed payloads still
    fail closed instead of being silently relabelled.
    """

    if observed_at is None:
        return records
    observed_date = observed_at.date().isoformat()
    temporal_fields = ("date", "fiscalDateEnding", "fiscalYear", "calendarYear")
    return [
        (
            {**record, "date": observed_date}
            if not any(record.get(field) is not None for field in temporal_fields)
            else record
        )
        for record in records
    ]


def _typed_payload_errors(capability: ProviderCapability):
    """Translate adapter normalization failures into the port's typed error.

    The translation itself lives in ``profile_normalization`` so a caller that
    imports that leaf directly gets the identical typed error; normalizers that
    already raise one -- ``company_profile_from_fmp_record`` among them -- pass
    through the ``ProviderError`` branch below untouched.
    """

    def decorate(method):
        @wraps(method)
        def wrapped(self, ticker: str, *args, **kwargs):
            try:
                return method(self, ticker, *args, **kwargs)
            except ProviderError:
                raise
            except UnknownMetricIdError:
                raise
            except _MALFORMED_SCALAR_ERRORS as exc:
                raise _malformed_payload_error(capability, ticker, exc) from exc

        return wrapped

    return decorate


@dataclass(frozen=True)
class FetchedRecords:
    """Normalized provider response plus the transport observation time."""

    records: tuple[dict, ...]
    observed_at: datetime | None
    validated_params: Mapping[str, Any] | None = None
    fetch_source: Literal["live", "cache"] | None = None
    lineage_descriptor: Mapping[str, Any] | None = None


class _QuoteRecordAbsent(ProviderError):
    """Internal discriminator for a requested ticker with no quote record."""


class FmpTransport(Protocol):
    """Injectable synchronous transport used by the provider adapter."""

    def fetch(
        self,
        endpoint: str,
        *,
        ticker: str,
        **params: object,
    ) -> FetchedRecords: ...


class IssuerUniverseArtifactPublisher(Protocol):
    """Durable artifact publisher required by the issuer-universe adapter."""

    def publish(
        self,
        *,
        artifact_kind: str,
        artifact_id: str,
        payload_schema_version: str,
        payload: Any,
        content_sha256: str,
    ) -> VersionedArtifactRef: ...


@dataclass(frozen=True)
class FmpIssuerUniverseConfiguration:
    """Complete authority and persistence seam for the FMP issuer port."""

    artifact_publisher: IssuerUniverseArtifactPublisher
    authoritative_registration: AuthoritativeCapabilityRegistration
    clock: Callable[[], datetime] = lambda: datetime.now(timezone.utc)


class FinancialsFetchTransport:
    """Live transport owned by the FMP adapter boundary."""

    def fetch(
        self,
        endpoint: str,
        *,
        ticker: str,
        **params: object,
    ) -> FetchedRecords:
        result = live_fetch_with_lineage(endpoint, ticker=ticker, **params)
        observed_at = result.observed_at
        if not isinstance(observed_at, datetime):
            raise TypeError("FMP provider result observed_at must be datetime")
        _require_aware_utc(
            observed_at,
            field_name="FMP provider result observed_at",
        )
        return FetchedRecords(
            records=tuple(dict(record) for record in result.records),
            observed_at=observed_at,
            validated_params=dict(result.validated_params),
            fetch_source=result.source,
            lineage_descriptor=dict(result.lineage_descriptor),
        )


class FmpAdapter:
    """FMP adapter backed by either replay data or an injectable live transport."""

    provider_name = FMP_PROVIDER_ID

    def __init__(
        self,
        *,
        fmp_data: dict[str, list[dict]] | None = None,
        transport: FmpTransport | None = None,
        overlay: CustomBindingOverlay | None = None,
        issuer_universe: FmpIssuerUniverseConfiguration | None = None,
    ) -> None:
        self._fmp_data = dict(fmp_data) if fmp_data is not None else None
        self._transport = transport
        self._transport_cache: dict[
            tuple[str, str, tuple[tuple[str, str], ...]], FetchedRecords
        ] = {}
        self._missing_value_policy_triggers: dict[
            tuple[str, int, int], frozenset[FilingLineRef]
        ] = {}
        self._quality_flags: dict[
            tuple[str, int, int], tuple[QualityFlag, ...]
        ] = {}
        self._overlay = overlay or CustomBindingOverlay()
        self._base_mappings = mapping_by_filing_line()
        self._issuer_universe = issuer_universe

    def with_fmp_data(self, fmp_data: dict[str, list[dict]]) -> FmpAdapter:
        return FmpAdapter(
            fmp_data=fmp_data,
            overlay=self._overlay,
            issuer_universe=self._issuer_universe,
        )

    def with_overlay(self, overlay: CustomBindingOverlay) -> FmpAdapter:
        """Return a request-scoped view with filing-line binding patches."""

        view = FmpAdapter(
            fmp_data=self._fmp_data,
            transport=self._transport,
            overlay=overlay,
            issuer_universe=self._issuer_universe,
        )
        view._transport_cache = self._transport_cache
        view._missing_value_policy_triggers = self._missing_value_policy_triggers
        view._quality_flags = self._quality_flags
        return view

    def advertised_capabilities(self) -> frozenset[ProviderCapability]:
        """Return only capabilities backed by a configured source."""

        if self._transport is not None:
            return frozenset(
                {
                    ProviderCapability.STATEMENT_FACTS,
                    ProviderCapability.COMPANY_PROFILE,
                    ProviderCapability.QUOTE,
                    ProviderCapability.ESTIMATES,
                    ProviderCapability.METRIC_SERIES,
                    ProviderCapability.PEER_UNIVERSE,
                    ProviderCapability.PRICE_SERIES,
                    *(
                        (ProviderCapability.ISSUER_UNIVERSE,)
                        if self._issuer_universe is not None
                        else ()
                    ),
                }
            )
        keys = frozenset(self._fmp_data or {})
        capabilities: set[ProviderCapability] = set()
        if keys & _STATEMENT_ENDPOINTS:
            capabilities.add(ProviderCapability.STATEMENT_FACTS)
        if "profile" in keys:
            capabilities.add(ProviderCapability.COMPANY_PROFILE)
        if "quote" in keys:
            capabilities.add(ProviderCapability.QUOTE)
        if "analyst_estimates" in keys:
            capabilities.add(ProviderCapability.ESTIMATES)
        if keys & {"key_metrics", "key_metrics_ttm", "ratios", "ratios_ttm"}:
            capabilities.add(ProviderCapability.METRIC_SERIES)
        if "stock_peers" in keys:
            capabilities.add(ProviderCapability.PEER_UNIVERSE)
        if keys & {"historical_price_eod", "historical_price_adjusted"}:
            capabilities.add(ProviderCapability.PRICE_SERIES)
        if "company_screener" in keys and self._issuer_universe is not None:
            capabilities.add(ProviderCapability.ISSUER_UNIVERSE)
        return frozenset(capabilities)

    def advertised_filing_line_bindings_by_capability(
        self,
    ) -> dict[ProviderCapability, frozenset[FmpFilingLineKey]]:
        """Return adapter-owned filing definitions without minting pin identity."""

        bindings: dict[ProviderCapability, set[FmpFilingLineKey]] = {
            capability: set() for capability in ProviderCapability
        }
        for key, mapping in self._resolved_mappings().items():
            bindings[_concept_capability(mapping.native_endpoint)].add(key)
        return {
            capability: frozenset(keys)
            for capability, keys in bindings.items()
            if keys
        }

    def missing_value_policies(
        self,
    ) -> dict[FmpFilingLineKey, MissingValuePolicy]:
        return {
            key: "zero_is_missing"
            for key, mapping in self._resolved_mappings().items()
            if mapping.treat_zero_as_missing
        }

    def missing_value_policy_triggered_lines(
        self,
        ticker: str,
        *,
        window: FiscalWindow,
    ) -> frozenset[FilingLineRef]:
        """Return policy triggers recorded by the matching canonical fetch."""

        return self._missing_value_policy_triggers.get(
            (ticker, window.end_fiscal_year, window.n_years),
            frozenset(),
        )

    def quality_flags(
        self,
        ticker: str,
        *,
        window: FiscalWindow,
    ) -> tuple[QualityFlag, ...]:
        return self._quality_flags.get(
            (ticker, window.end_fiscal_year, window.n_years),
            (),
        )

    def matches_filing_line_provenance(
        self,
        line: FilingLineRef,
        provenance: VendorProvenance,
        *,
        fallback_native_field_used: str | None = None,
    ) -> bool:
        mapping = self._mapping_for_line(line)
        if mapping is None:
            return False
        accepted_fields = {
            field
            for field in (mapping.native_field, mapping.fallback_native_field)
            if field is not None
        }
        expected_fallback = (
            provenance.native_field
            if mapping.fallback_native_field is not None
            and provenance.native_field == mapping.fallback_native_field
            else None
        )
        return (
            provenance.provider == FMP_PROVIDER_ID
            and provenance.native_endpoint == mapping.native_endpoint
            and provenance.native_field in accepted_fields
            and fallback_native_field_used == expected_fallback
        )

    def fetch_filing_line_facts(
        self,
        ticker: str,
        *,
        lines: Sequence[FilingLineRef],
        window: FiscalWindow,
    ) -> tuple[tuple[FilingLineObservation, ...], tuple[ReportedPeriod, ...]]:
        requested = tuple(
            sorted(
                set(lines),
                key=lambda line: (
                    line.tag,
                    -_filing_line_fiscal_year(line),
                    line.statement_role,
                    line.accession,
                ),
            )
        )
        mappings = {line: self._mapping_for_line(line) for line in requested}
        requested_endpoints = {
            mapping.native_endpoint
            for mapping in mappings.values()
            if mapping is not None
            and mapping.native_endpoint in _STATEMENT_ENDPOINTS
        }
        endpoints = set(requested_endpoints)
        if self._fmp_data is not None:
            endpoints &= set(self._fmp_data)
            endpoints.update(set(self._fmp_data) & _STATEMENT_ENDPOINTS)
        else:
            endpoints.update({"income_statement", "balance_sheet", "cash_flow"})
        fetch_order = sorted(
            endpoints,
            key=lambda endpoint: (endpoint not in requested_endpoints, endpoint),
        )
        source_data = {
            endpoint: self._records_for(
                endpoint,
                ticker,
                ProviderCapability.STATEMENT_FACTS,
                period="annual",
                limit=max(window.n_years, 5),
            ).records
            for endpoint in fetch_order
        }
        lookup = build_endpoint_lookup(source_data)
        triggered: set[FilingLineRef] = set()
        observations: list[FilingLineObservation] = []

        start_year = window.end_fiscal_year - window.n_years + 1
        for line in requested:
            fiscal_year = _filing_line_fiscal_year(line)
            if not start_year <= fiscal_year <= window.end_fiscal_year:
                raise ValueError(
                    f"requested filing line FY{fiscal_year} is outside "
                    f"FY{start_year}-FY{window.end_fiscal_year}"
                )
            mapping = mappings[line]
            if mapping is None:
                raise_adapter_unresolved_for_required_line(
                    producer=self.provider_name,
                    native_name=line.tag,
                    line=line,
                )
            record = lookup.get(mapping.native_endpoint, {}).get(fiscal_year)
            if mapping.treat_zero_as_missing and _is_missing_or_zero(
                record,
                mapping.native_field,
            ):
                triggered.add(line)
            try:
                raw_value, field_used, fallback_used = _extract_numeric_value(mapping, record)
                if raw_value is None:
                    observations.append(
                        FilingLineObservation(
                            line=line,
                            status="missing",
                            fact=None,
                            provider="fmp",
                            detail=None,
                        )
                    )
                    continue
                fact = FilingLineFact(
                    line=line,
                    value=_quantity_for_mapping(mapping, scale_value(mapping, raw_value)),
                    grade="adapter_asserted",
                    provenance=vendor_provenance(
                        mapping,
                        field_used,
                        fallback_used=fallback_used,
                    ),
                )
            except (ArithmeticError, TypeError, ValueError) as exc:
                observations.append(
                    FilingLineObservation(
                        line=line,
                        status="failed",
                        fact=None,
                        provider="fmp",
                        detail=f"{type(exc).__name__}: {exc}",
                    )
                )
                continue
            observations.append(
                FilingLineObservation(
                    line=line,
                    status="ok",
                    fact=fact,
                    detail=None,
                )
            )

        request_key = (ticker, window.end_fiscal_year, window.n_years)
        self._missing_value_policy_triggers[request_key] = frozenset(triggered)
        typed_observations = tuple(observations)
        self._quality_flags[request_key] = _quality_flags_for_observations(
            typed_observations
        )
        return typed_observations, _reported_periods_from_lookup(lookup, window=window)

    @_typed_payload_errors(ProviderCapability.COMPANY_PROFILE)
    def fetch_profile(self, ticker: str) -> CompanyProfile:
        batch = self._records_for("profile", ticker, ProviderCapability.COMPANY_PROFILE)
        if not batch.records:
            raise self._provider_error(
                ProviderCapability.COMPANY_PROFILE,
                ticker,
                f"profile unavailable for {ticker}",
            )
        return company_profile_from_fmp_record(batch.records[0], ticker=ticker)

    @_typed_payload_errors(ProviderCapability.QUOTE)
    def fetch_quote(self, ticker: str) -> Quote:
        batch = self._records_for("quote", ticker, ProviderCapability.QUOTE)
        records = batch.records
        record = next(
            (
                row
                for row in records
                if str(row.get("symbol") or "").upper() == ticker.upper()
            ),
            None,
        )
        if record is None:
            raise _QuoteRecordAbsent(
                provider="fmp",
                capability=ProviderCapability.QUOTE,
                ticker=ticker,
                retryable=False,
                detail=f"quote unavailable for {ticker}",
            )
        raw_price = next(
            (
                record.get(field)
                for field in PRICE_FIELDS
                if record.get(field) is not None
            ),
            None,
        )
        if raw_price is None:
            raise self._provider_error(
                ProviderCapability.QUOTE,
                ticker,
                f"quote price unavailable for {ticker}",
            )
        as_of = _quote_datetime(record)
        if as_of is None:
            raise self._provider_error(
                ProviderCapability.QUOTE,
                ticker,
                f"quote observation time unavailable for {ticker}",
            )
        return Quote(
            ticker=ticker,
            price=Decimal(str(raw_price)),
            currency=_string_or_none(record.get("currency")),
            as_of=as_of,
            market_cap=_decimal_or_none(record.get("marketCap")),
            shares_outstanding=_decimal_or_none(record.get("sharesOutstanding")),
        )

    def fetch_quotes(self, tickers: Sequence[str]) -> dict[str, Quote]:
        deduped = list(dict.fromkeys(tickers))
        if not deduped:
            return {}
        batch = self._records_for(
            "quote",
            ",".join(deduped),
            ProviderCapability.QUOTE,
        )
        records_by_symbol = {
            normalize_ticker(str(record.get("symbol") or "")): record
            for record in batch.records
            if str(record.get("symbol") or "").strip()
        }
        quotes: dict[str, Quote] = {}
        for ticker in deduped:
            record = records_by_symbol.get(normalize_ticker(ticker))
            if record is None:
                continue
            raw_price = next(
                (
                    record.get(field)
                    for field in PRICE_FIELDS
                    if record.get(field) is not None
                ),
                None,
            )
            if raw_price is None:
                raise self._provider_error(
                    ProviderCapability.QUOTE,
                    ticker,
                    f"quote price unavailable for {ticker}",
                )
            as_of = _quote_datetime(record)
            if as_of is None:
                raise self._provider_error(
                    ProviderCapability.QUOTE,
                    ticker,
                    f"quote observation time unavailable for {ticker}",
                )
            try:
                quotes[ticker] = Quote(
                    ticker=ticker,
                    price=Decimal(str(raw_price)),
                    currency=_string_or_none(record.get("currency")),
                    as_of=as_of,
                    market_cap=_decimal_or_none(record.get("marketCap")),
                    shares_outstanding=_decimal_or_none(
                        record.get("sharesOutstanding")
                    ),
                )
            except (ArithmeticError, TypeError, ValueError) as exc:
                # Per-ticker so one bad row cannot fail the batch; the detail
                # itself comes from the one owner in ``profile_normalization``.
                raise _malformed_payload_error(
                    ProviderCapability.QUOTE,
                    ticker,
                    exc,
                ) from exc
        return quotes

    @_typed_payload_errors(ProviderCapability.METRIC_SERIES)
    def fetch_metric_series_annual(
        self,
        ticker: str,
        *,
        metrics: Sequence[str],
        window: FiscalWindow,
    ) -> tuple[MetricSeries, ...]:
        if not metrics:
            return ()
        requested_mappings = tuple(
            (metric_id, mapping_for_metric(metric_id)) for metric_id in metrics
        )
        series: list[MetricSeries | None] = []
        for metric_id, mapping in requested_mappings:
            if mapping is None or mapping.capability != "metric_series":
                continue
            endpoint = mapping.annual_endpoint
            if endpoint is None:
                continue
            if self._transport is None and endpoint not in (self._fmp_data or {}):
                continue
            records = _records_for_ticker(
                self._records_for(
                    endpoint,
                    ticker,
                    ProviderCapability.METRIC_SERIES,
                    period="annual",
                    limit=window.n_years
                    + (1 if mapping.calculation == "growth" else 0),
                ).records,
                ticker,
            )
            denominator_records: list[dict] | None = None
            if mapping.denominator_endpoint is not None:
                denominator_records = _records_for_ticker(
                    self._records_for(
                        mapping.denominator_endpoint,
                        ticker,
                        ProviderCapability.METRIC_SERIES,
                        period="annual",
                        limit=window.n_years,
                    ).records,
                    ticker,
                )
            series.append(
                build_annual_metric_series(
                    ticker,
                    records,
                    metric_id,
                    window_end_year=window.end_fiscal_year,
                    window_n_years=window.n_years,
                    native_endpoint=endpoint,
                    denominator_records=denominator_records,
                )
            )
        return tuple(item for item in series if item is not None)

    @_typed_payload_errors(ProviderCapability.METRIC_SERIES)
    def fetch_metric_series_ttm(
        self,
        ticker: str,
        *,
        metrics: Sequence[str],
        as_of_cutoff: date,
        limit: int,
    ) -> tuple[MetricSeries, ...]:
        requested_mappings = tuple(
            (metric_id, mapping_for_metric(metric_id)) for metric_id in metrics
        )
        series: list[MetricSeries | None] = []
        for metric_id, mapping in requested_mappings:
            if mapping is None or mapping.capability != "metric_series":
                continue
            endpoint = mapping.ttm_endpoint
            if endpoint is None:
                continue
            if self._transport is None and endpoint not in (self._fmp_data or {}):
                continue
            fetched = self._records_for(
                endpoint,
                ticker,
                ProviderCapability.METRIC_SERIES,
            )
            records = _ttm_records_with_observed_at(
                _records_for_ticker(fetched.records, ticker),
                fetched.observed_at,
            )
            series.append(
                build_ttm_metric_series(
                    ticker,
                    records,
                    metric_id,
                    as_of_cutoff=as_of_cutoff,
                    limit=limit,
                    native_endpoint=endpoint,
                )
            )
        return tuple(item for item in series if item is not None)

    @_typed_payload_errors(ProviderCapability.ESTIMATES)
    def fetch_estimates(
        self,
        ticker: str,
        *,
        metrics: Sequence[str],
        period: EstimatePeriod,
        limit: int,
    ) -> EstimateFactsResult:
        if period not in ("annual", "quarter"):
            raise ValueError("period must be 'annual' or 'quarter'")
        if (
            isinstance(limit, bool)
            or not isinstance(limit, int)
            or not 1 <= limit <= 100
        ):
            raise ValueError("limit must be an integer from 1 through 100")
        requested_mappings = tuple(
            (metric_id, mapping_for_metric(metric_id)) for metric_id in metrics
        )
        batch = self._records_for(
            "analyst_estimates",
            ticker,
            ProviderCapability.ESTIMATES,
            period=period,
            limit=limit,
        )
        indexed_records = _record_indices_for_ticker(batch.records, ticker)[:limit]
        facts: list[EstimateFact] = []
        for row_index, record in indexed_records:
            raw_period_end = record.get("date")
            if raw_period_end in (None, ""):
                continue
            target_period_end = date.fromisoformat(str(raw_period_end)[:10])
            for metric_id, mapping in requested_mappings:
                if mapping is None or mapping.capability != "estimates":
                    continue
                selected_value = _first_decimal_field(record, mapping.native_fields)
                if selected_value is None:
                    continue
                native_field, value = selected_value
                analyst_count = None
                analyst_count_result_key = None
                if metric_id == "eps_avg":
                    raw_analyst_count = record.get("numAnalystsEps")
                    if raw_analyst_count not in (None, ""):
                        parsed_analyst_count = Decimal(str(raw_analyst_count))
                        if (
                            not parsed_analyst_count.is_finite()
                            or parsed_analyst_count < 0
                            or parsed_analyst_count
                            != parsed_analyst_count.to_integral_value()
                        ):
                            raise ValueError(
                                "numAnalystsEps must be a nonnegative integer"
                            )
                        analyst_count = int(parsed_analyst_count)
                        analyst_count_result_key = (
                            f"data[{row_index}].numAnalystsEps"
                        )
                facts.append(
                    EstimateFact(
                        ticker=ticker,
                        metric_id=metric_id,
                        period=period,
                        target_period_end=target_period_end,
                        statistic="mean",
                        value=value / mapping.scale_divisor,
                        analyst_count=analyst_count,
                        provider_vintage=_record_date(record, "asOf"),
                        target_period_end_result_key=f"data[{row_index}].date",
                        value_result_key=f"data[{row_index}].{native_field}",
                        analyst_count_result_key=analyst_count_result_key,
                        fiscal_year=_optional_fiscal_year(record),
                        period_type=_optional_reported_period(record),
                    )
                )
        source_lineage = _provider_response_lineage(
            batch,
            native_endpoint="analyst_estimates",
        )
        for fact in facts:
            _require_issued_result_keys(
                source_lineage,
                (
                    fact.target_period_end_result_key,
                    fact.value_result_key,
                    fact.analyst_count_result_key,
                ),
            )
        return EstimateFactsResult(
            facts=tuple(facts),
            source_lineage=source_lineage,
        )

    @_typed_payload_errors(ProviderCapability.PEER_UNIVERSE)
    def fetch_peers(self, ticker: str) -> PeerUniverse:
        batch = self._records_for(
            "stock_peers", ticker, ProviderCapability.PEER_UNIVERSE
        )
        records = batch.records
        peers = tuple(extract_peer_symbols(records, ticker, max_peers=50))
        as_of = _record_date(records[0], "asOf") if records else None
        if peers and as_of is None:
            raise self._provider_error(
                ProviderCapability.PEER_UNIVERSE,
                ticker,
                f"peer universe observation date unavailable for {ticker}",
            )
        return PeerUniverse(
            ticker=ticker,
            peers=peers,
            as_of=as_of,
            method="stock_peers",
        )

    def query_issuer_universe(
        self,
        request: IssuerUniverseProviderRequest,
    ) -> IssuerUniverseProviderResult:
        """Fetch, preserve, and normalize the active FMP public-equity universe."""

        configuration = self._issuer_universe
        if configuration is None:
            raise RuntimeError("FMP issuer-universe capability is not configured")
        request_hash = provider_request_digest(request)
        try:
            batch = self._records_for(
                "company_screener",
                request.normalized_focal_ticker,
                ProviderCapability.ISSUER_UNIVERSE,
                isActivelyTrading=True,
                limit=10000,
            )
        except ProviderError as exc:
            return _fmp_issuer_failed_result(
                configuration=configuration,
                request=request,
                request_hash=request_hash,
                failure_code="fmp_issuer_universe_transport_failed",
                detail=exc.detail,
            )

        provider_available_at = batch.observed_at or configuration.clock()
        _require_aware_utc(
            provider_available_at,
            field_name="provider_available_at_utc",
        )
        raw_payload = {
            "schema_version": "fmp-issuer-universe-response.v2",
            "endpoint": "company_screener",
            "request": request,
            "records": batch.records,
            "provider_available_at_utc": provider_available_at,
            "validated_params": (
                None
                if batch.validated_params is None
                else dict(batch.validated_params)
            ),
            "fetch_source": batch.fetch_source,
            "lineage_descriptor": (
                None
                if batch.lineage_descriptor is None
                else dict(batch.lineage_descriptor)
            ),
        }
        raw_ref = _publish_issuer_artifact(
            configuration.artifact_publisher,
            artifact_kind="issuer_universe_provider_response",
            artifact_id=f"fmp:{request_hash}:raw",
            payload_schema_version="fmp-issuer-universe-response.v2",
            payload=raw_payload,
        )
        if not batch.records:
            fields = {
                "result_kind": "explicit_missing",
                "schema_version": "evidence-provider-result.v1",
                "request_digest": request_hash,
                "attempt_artifact_ref": raw_ref,
                "attempt_artifact_hash": raw_ref.content_sha256,
                "missing_code": "fmp_active_public_equity_universe_empty",
                "provider_available_at_utc": provider_available_at,
            }
            return _provider_result(ExplicitMissingProviderResult, fields)

        evidence = ArtifactRefHashBinding(
            artifact_ref=raw_ref,
            artifact_hash=raw_ref.content_sha256,
        )
        try:
            members = tuple(
                sorted(
                    (
                        _issuer_member_from_fmp_record(record, evidence=evidence)
                        for record in batch.records
                    ),
                    key=lambda member: member.normalized_ticker,
                )
            )
            tickers = tuple(member.normalized_ticker for member in members)
            if tickers != tuple(sorted(set(tickers))):
                raise ValueError("FMP issuer universe contains duplicate tickers")
        except (ArithmeticError, TypeError, ValueError):
            fields = {
                "result_kind": "failed",
                "schema_version": "evidence-provider-result.v1",
                "request_digest": request_hash,
                "attempt_artifact_ref": raw_ref,
                "attempt_artifact_hash": raw_ref.content_sha256,
                "failure_code": "fmp_issuer_universe_normalization_failed",
                "provider_available_at_utc": provider_available_at,
            }
            return _provider_result(FailedProviderResult, fields)

        members_ref = _publish_issuer_artifact(
            configuration.artifact_publisher,
            artifact_kind="normalized_issuer_members",
            artifact_id=f"fmp:{request_hash}:members",
            payload_schema_version="issuer-universe-member-set.v1",
            payload=members,
        )
        universe_as_of = max(
            (
                observed
                for record in batch.records
                if (observed := _fmp_record_datetime(record)) is not None
            ),
            default=provider_available_at,
        )
        fields = {
            "result_kind": "issuer_universe_available",
            "schema_version": "issuer-universe-provider-result.v1",
            "request_digest": request_hash,
            "provider_response_ref": raw_ref,
            "provider_response_hash": raw_ref.content_sha256,
            "normalized_member_artifact_ref": members_ref,
            "normalized_member_artifact_hash": members_ref.content_sha256,
            "member_count": len(members),
            "universe_as_of_utc": universe_as_of,
            "provider_available_at_utc": provider_available_at,
        }
        return _provider_result(IssuerUniverseAvailableProviderResult, fields)

    @_typed_payload_errors(ProviderCapability.PRICE_SERIES)
    def fetch_prices(
        self,
        ticker: str,
        *,
        start: date,
        end: date,
        basis: PriceSeriesBasis,
    ) -> PriceSeries:
        if basis == "unadjusted_close":
            endpoint = "historical_price_eod"
            native_field = "close"
        elif basis == "dividend_adjusted_close":
            endpoint = "historical_price_adjusted"
            native_field = "adjClose"
        else:
            raise ValueError(
                "basis must be 'unadjusted_close' or 'dividend_adjusted_close'"
            )
        batch = self._records_for(
            endpoint,
            ticker,
            ProviderCapability.PRICE_SERIES,
            **{"from": start.isoformat(), "to": end.isoformat()},
        )
        records = batch.records
        points: list[PricePoint] = []
        for row_index, record in enumerate(records):
            raw_date = record.get("date")
            if not raw_date:
                continue
            trade_date = date.fromisoformat(str(raw_date)[:10])
            if trade_date < start or trade_date > end:
                continue
            raw_value = record.get(native_field)
            if raw_value is None:
                continue
            value = Decimal(str(raw_value))
            points.append(
                PricePoint(
                    trade_date=trade_date,
                    adjusted_close=(
                        value if basis == "dividend_adjusted_close" else None
                    ),
                    close=value if basis == "unadjusted_close" else None,
                    trade_date_result_key=f"data[{row_index}].date",
                    value_result_key=f"data[{row_index}].{native_field}",
                )
            )
        points.sort(key=lambda point: point.trade_date)
        currency = next(
            (
                parsed
                for record in records
                if (parsed := _string_or_none(record.get("currency"))) is not None
            ),
            None,
        )
        source_lineage = _provider_response_lineage(
            batch,
            native_endpoint=endpoint,
        )
        for point in points:
            _require_issued_result_keys(
                source_lineage,
                (point.trade_date_result_key, point.value_result_key),
            )
        return PriceSeries(
            ticker=ticker,
            basis=basis,
            currency=currency,
            points=tuple(points),
            source_lineage=source_lineage,
        )

    def _resolved_mappings(self) -> dict[FmpFilingLineKey, ConceptMapping]:
        resolved = dict(self._base_mappings)
        for mapping in self._overlay.entries:
            key = mapping.filing_line_key
            if key is None:
                continue
            resolved[key] = mapping
        return resolved

    def _mapping_for_line(self, line: FilingLineRef) -> ConceptMapping | None:
        matches = {
            mapping
            for mapping in self._resolved_mappings().values()
            if mapping.tag == line.tag
        }
        if not matches:
            return None
        if len(matches) != 1:
            raise ValueError(f"ambiguous FMP mapping for filing tag {line.tag!r}")
        return matches.pop()

    def _records_for(
        self,
        endpoint: str,
        ticker: str,
        capability: ProviderCapability,
        **params: object,
    ) -> FetchedRecords:
        if self._fmp_data is not None and endpoint in self._fmp_data:
            return FetchedRecords(
                records=tuple(self._fmp_data[endpoint]),
                observed_at=None,
            )
        if self._transport is None:
            raise self._provider_error(
                capability,
                ticker,
                f"no FMP source configured for endpoint {endpoint}",
            )
        key = (
            endpoint,
            ticker.upper(),
            tuple(sorted((name, str(value)) for name, value in params.items())),
        )
        if key not in self._transport_cache:
            try:
                self._transport_cache[key] = self._transport.fetch(
                    endpoint,
                    ticker=ticker,
                    **params,
                )
            except ProviderError:
                raise
            except Exception as exc:
                raise ProviderError(
                    provider="fmp",
                    capability=capability,
                    ticker=ticker,
                    retryable=True,
                    detail=f"FMP {endpoint} fetch failed: {exc}",
                ) from exc
        return self._transport_cache[key]

    @staticmethod
    def _provider_error(
        capability: ProviderCapability,
        ticker: str,
        detail: str,
    ) -> ProviderError:
        return ProviderError(
            provider="fmp",
            capability=capability,
            ticker=ticker,
            retryable=False,
            detail=detail,
        )


def _require_aware_utc(value: datetime, *, field_name: str) -> None:
    if value.tzinfo is None or value.utcoffset() != timezone.utc.utcoffset(value):
        raise ValueError(f"{field_name} must be aware UTC")


def _publish_issuer_artifact(
    publisher: IssuerUniverseArtifactPublisher,
    *,
    artifact_kind: str,
    artifact_id: str,
    payload_schema_version: str,
    payload: Any,
) -> VersionedArtifactRef:
    content_sha256 = compute_artifact_content_sha256(payload, "canonical_json_v1")
    ref = publisher.publish(
        artifact_kind=artifact_kind,
        artifact_id=artifact_id,
        payload_schema_version=payload_schema_version,
        payload=payload,
        content_sha256=content_sha256,
    )
    if ref.content_sha256 != content_sha256:
        raise ValueError("issuer artifact publisher substituted content hash")
    return ref


def _provider_result(model_type: type[Any], fields: dict[str, Any]) -> Any:
    draft = model_type.model_construct(**fields, result_digest="0" * 64)
    return model_type(
        **fields,
        result_digest=provider_result_digest(draft),
    )


def _fmp_issuer_failed_result(
    *,
    configuration: FmpIssuerUniverseConfiguration,
    request: IssuerUniverseProviderRequest,
    request_hash: str,
    failure_code: str,
    detail: str,
) -> FailedProviderResult:
    provider_available_at = configuration.clock()
    _require_aware_utc(
        provider_available_at,
        field_name="provider_available_at_utc",
    )
    attempt_payload = {
        "schema_version": "fmp-issuer-universe-attempt.v1",
        "endpoint": "company_screener",
        "request": request,
        "failure_code": failure_code,
        "detail": detail,
        "provider_available_at_utc": provider_available_at,
    }
    attempt_ref = _publish_issuer_artifact(
        configuration.artifact_publisher,
        artifact_kind="issuer_universe_attempt",
        artifact_id=f"fmp:{request_hash}:failed",
        payload_schema_version="fmp-issuer-universe-attempt.v1",
        payload=attempt_payload,
    )
    fields = {
        "result_kind": "failed",
        "schema_version": "evidence-provider-result.v1",
        "request_digest": request_hash,
        "attempt_artifact_ref": attempt_ref,
        "attempt_artifact_hash": attempt_ref.content_sha256,
        "failure_code": failure_code,
        "provider_available_at_utc": provider_available_at,
    }
    return _provider_result(FailedProviderResult, fields)


def _fmp_record_datetime(record: dict[str, Any]) -> datetime | None:
    for field_name in ("universeAsOf", "asOf", "lastUpdated", "date"):
        raw = record.get(field_name)
        if raw in (None, ""):
            continue
        try:
            if isinstance(raw, (int, float)):
                return datetime.fromtimestamp(float(raw), tz=timezone.utc)
            parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except (OverflowError, TypeError, ValueError):
            continue
    return None


def _first_record_value(
    record: dict[str, Any],
    field_names: tuple[str, ...],
) -> Any | None:
    return next(
        (
            value
            for field_name in field_names
            if (value := record.get(field_name)) not in (None, "")
        ),
        None,
    )


def _text_issuer_feature(
    record: dict[str, Any],
    field_names: tuple[str, ...],
    *,
    evidence: ArtifactRefHashBinding,
) -> KnownTextIssuerFeature | UnavailableIssuerFeature:
    raw = _first_record_value(record, field_names)
    if raw is None:
        return UnavailableIssuerFeature(
            feature_state="unavailable",
            reason_code="provider_missing",
            evidence=(evidence,),
        )
    normalized = str(raw).strip()
    if not normalized:
        return UnavailableIssuerFeature(
            feature_state="unavailable",
            reason_code="normalization_failed",
            evidence=(evidence,),
        )
    return KnownTextIssuerFeature(
        feature_state="known_text",
        value=normalized,
        evidence=evidence,
    )


def _decimal_issuer_feature(
    record: dict[str, Any],
    field_names: tuple[str, ...],
    *,
    evidence: ArtifactRefHashBinding,
) -> KnownDecimalIssuerFeature | UnavailableIssuerFeature:
    raw = _first_record_value(record, field_names)
    if raw is None:
        return UnavailableIssuerFeature(
            feature_state="unavailable",
            reason_code="provider_missing",
            evidence=(evidence,),
        )
    try:
        value = Decimal(str(raw))
    except (ArithmeticError, ValueError):
        return UnavailableIssuerFeature(
            feature_state="unavailable",
            reason_code="normalization_failed",
            evidence=(evidence,),
        )
    if not value.is_finite():
        return UnavailableIssuerFeature(
            feature_state="unavailable",
            reason_code="normalization_failed",
            evidence=(evidence,),
        )
    return KnownDecimalIssuerFeature(
        feature_state="known_decimal",
        value=value,
        evidence=evidence,
    )


def _active_issuer_feature(
    record: dict[str, Any],
    *,
    evidence: ArtifactRefHashBinding,
) -> KnownBooleanIssuerFeature | UnavailableIssuerFeature:
    raw = _first_record_value(
        record,
        ("isActivelyTrading", "is_actively_trading", "active"),
    )
    if raw is None:
        # The raw artifact also binds the exact screener request, whose closed
        # listing scope requires isActivelyTrading=True.
        value = True
    elif isinstance(raw, bool):
        value = raw
    elif isinstance(raw, (int, float)) and raw in (0, 1):
        value = bool(raw)
    elif isinstance(raw, str) and raw.strip().lower() in {
        "0",
        "1",
        "false",
        "true",
    }:
        value = raw.strip().lower() in {"1", "true"}
    else:
        return UnavailableIssuerFeature(
            feature_state="unavailable",
            reason_code="normalization_failed",
            evidence=(evidence,),
        )
    return KnownBooleanIssuerFeature(
        feature_state="known_boolean",
        value=value,
        evidence=evidence,
    )


def _issuer_member_from_fmp_record(
    record: dict[str, Any],
    *,
    evidence: ArtifactRefHashBinding,
) -> IssuerUniverseMember:
    raw_ticker = _first_record_value(record, ("symbol", "ticker"))
    if raw_ticker is None:
        raise ValueError("FMP issuer-universe row lacks symbol")
    ticker = normalize_ticker(str(raw_ticker))
    issuer_identity = _first_record_value(
        record,
        ("cik", "isin", "cusip"),
    )
    fields = {
        "issuer_id": (
            f"fmp:{str(issuer_identity).strip()}"
            if issuer_identity is not None
            else f"fmp-symbol:{ticker}"
        ),
        "normalized_ticker": ticker,
        "active_listing": _active_issuer_feature(record, evidence=evidence),
        "domicile_country": _text_issuer_feature(
            record,
            ("country", "domicileCountry"),
            evidence=evidence,
        ),
        "domicile_region": _text_issuer_feature(
            record,
            ("region", "domicileRegion"),
            evidence=evidence,
        ),
        "archetype_id": _text_issuer_feature(
            record,
            ("archetypeId", "archetype_id"),
            evidence=evidence,
        ),
        "industry_id": _text_issuer_feature(
            record,
            ("industry", "industryId", "industry_id"),
            evidence=evidence,
        ),
        "subindustry_id": _text_issuer_feature(
            record,
            ("subindustry", "subIndustry", "subindustryId", "subindustry_id"),
            evidence=evidence,
        ),
        "revenue_ttm": _decimal_issuer_feature(
            record,
            ("revenueTTM", "revenueTtm", "revenue_ttm", "revenue"),
            evidence=evidence,
        ),
        "market_cap": _decimal_issuer_feature(
            record,
            ("marketCap", "market_cap"),
            evidence=evidence,
        ),
    }
    draft = IssuerUniverseMember.model_construct(
        **fields,
        member_digest="0" * 64,
    )
    return IssuerUniverseMember(
        **fields,
        member_digest=issuer_universe_member_digest(draft),
    )


class _FmpAdapterKwargs(TypedDict, total=False):
    fmp_data: dict[str, list[dict]] | None
    transport: FmpTransport | None
    overlay: CustomBindingOverlay | None
    issuer_universe: FmpIssuerUniverseConfiguration | None


class PreviewFmpAdapter(FmpAdapter):
    """FMP capability adapter preserving the preview's tolerant fetch policy.

    The legacy preview treated a statement transport failure as an empty statement
    bundle, while still allowing independently fetched profile and quote inputs to
    populate valuation rows. Keep that consumer policy at the adapter boundary so
    the preview itself only sees canonical capabilities.
    """

    def __init__(self, **kwargs: Unpack[_FmpAdapterKwargs]) -> None:
        super().__init__(**kwargs)
        self._preview_state: dict[str, object] = {
            "statement_data_missing": False,
            "statement_failure": None,
            "missing_warning_emitted": False,
        }

    def with_overlay(self, overlay: CustomBindingOverlay) -> PreviewFmpAdapter:
        view = PreviewFmpAdapter(
            fmp_data=self._fmp_data,
            transport=self._transport,
            overlay=overlay,
            issuer_universe=self._issuer_universe,
        )
        view._transport_cache = self._transport_cache
        view._missing_value_policy_triggers = self._missing_value_policy_triggers
        view._quality_flags = self._quality_flags
        view._preview_state = self._preview_state
        return view

    def _empty_statement_result(
        self,
        ticker: str,
        *,
        lines: Sequence[FilingLineRef],
        window: FiscalWindow,
    ) -> tuple[tuple[FilingLineObservation, ...], tuple[ReportedPeriod, ...]]:
        return FmpAdapter(
            fmp_data={
                "income_statement": [],
                "balance_sheet": [],
                "cash_flow": [],
            },
            overlay=self._overlay,
        ).fetch_filing_line_facts(ticker, lines=lines, window=window)

    def fetch_filing_line_facts(
        self,
        ticker: str,
        *,
        lines: Sequence[FilingLineRef],
        window: FiscalWindow,
    ) -> tuple[tuple[FilingLineObservation, ...], tuple[ReportedPeriod, ...]]:
        if self._preview_state["statement_failure"] is not None:
            return self._empty_statement_result(
                ticker,
                lines=lines,
                window=window,
            )
        try:
            observations, periods = super().fetch_filing_line_facts(
                ticker,
                lines=lines,
                window=window,
            )
        except Exception as exc:
            self._preview_state["statement_failure"] = exc
            self._preview_state["statement_data_missing"] = True
            logging.warning(
                "preview FMP financials fetch failed for %s: %s",
                ticker,
                exc,
            )
            return self._empty_statement_result(
                ticker,
                lines=lines,
                window=window,
            )
        self._preview_state["statement_data_missing"] = not any(
            observation.fact is not None for observation in observations
        )
        return observations, periods

    def _note_market_input(self, ticker: str) -> None:
        if (
            self._preview_state["statement_data_missing"]
            and not self._preview_state["missing_warning_emitted"]
        ):
            logging.warning(
                "preview FMP data for %s has market inputs but no financial statements; "
                "the preview model historicals will be empty and forecast/valuation will "
                "fail on missing deps",
                ticker,
            )
            self._preview_state["missing_warning_emitted"] = True

    def fetch_profile(self, ticker: str) -> CompanyProfile:
        result = super().fetch_profile(ticker)
        self._note_market_input(ticker)
        return result

    def fetch_quote(self, ticker: str) -> Quote:
        result = super().fetch_quote(ticker)
        self._note_market_input(ticker)
        return result


def _string_or_none(value: object | None) -> str | None:
    if value in (None, ""):
        return None
    return str(value)


def _quote_datetime(record: dict) -> datetime | None:
    timestamp = record.get("timestamp")
    if timestamp not in (None, ""):
        return datetime.fromtimestamp(float(timestamp), tz=timezone.utc)
    raw = record.get("asOf")
    if raw:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
        return parsed.astimezone(timezone.utc)
    return None


def _quality_flags_for_observations(
    observations: Sequence[FilingLineObservation],
) -> tuple[QualityFlag, ...]:
    from schema.provider_port.adapters.fmp.quality import (
        QUALITY_BUCKET_FIELDS,
        quality_warnings,
    )

    values: dict[
        tuple[FilingLineRef, str, str],
        dict[int, float],
    ] = {}
    for observation in observations:
        fact = observation.fact
        if fact is None or fact.provenance is None:
            continue
        endpoint_and_field = (
            fact.provenance.native_endpoint,
            fact.provenance.native_field,
        )
        if endpoint_and_field not in QUALITY_BUCKET_FIELDS:
            continue
        key = (fact.line, *endpoint_and_field)
        values.setdefault(key, {})[_filing_line_fiscal_year(fact.line)] = float(
            fact.value.value
        )
    return quality_warnings(values)


def _filing_line_fiscal_year(line: FilingLineRef) -> int:
    if line.period.kind != "fiscal_year":
        raise ValueError("annual statement-facts fetch requires fiscal-year lines")
    return line.period.fiscal_year


def _quantity_for_mapping(mapping: ConceptMapping, value: float) -> Quantity:
    tag = mapping.tag or ""
    if "EarningsPerShare" in tag:
        display = NumericDisplaySpec(numeric_kind="currency", currency="USD")
    elif "Shares" in tag:
        display = NumericDisplaySpec(numeric_kind="count", scale="millions")
    else:
        display = NumericDisplaySpec(
            numeric_kind="currency",
            scale="millions",
            currency="USD",
        )
    return Quantity.model_validate({"value": str(value), "display": display})

def _period_end_from_record(record: dict | None) -> date | None:
    raw = record.get("date") if record else None
    if raw is None:
        return None
    if isinstance(raw, datetime):
        return raw.date()
    if isinstance(raw, date):
        return raw
    normalized = str(raw).strip()
    if not normalized or normalized.lower() in {"none", "nan", "nat"}:
        return None
    candidate = normalized[:10] if len(normalized) >= 10 else normalized
    try:
        return date.fromisoformat(candidate)
    except ValueError:
        return None


def _reported_periods_from_lookup(
    lookup: dict[str, dict[int, dict]],
    *,
    window: FiscalWindow,
) -> tuple[ReportedPeriod, ...]:
    start_year = window.end_fiscal_year - window.n_years + 1
    periods_by_year: dict[int, ReportedPeriod] = {}
    for endpoint in sorted(lookup):
        for fiscal_year, record in sorted(lookup[endpoint].items()):
            if not start_year <= fiscal_year <= window.end_fiscal_year:
                continue
            period_type = str(record.get("period") or "FY").upper()
            if period_type not in {"FY", "Q1", "Q2", "Q3", "Q4"}:
                period_type = "FY"
            period_end = _period_end_from_record(record)
            if period_end is None:
                continue
            candidate = ReportedPeriod(
                fiscal_year=fiscal_year,
                period_type=period_type,  # type: ignore[arg-type]
                period_end=period_end,
            )
            existing = periods_by_year.get(fiscal_year)
            if existing is None:
                periods_by_year[fiscal_year] = candidate
            elif existing.period_end != candidate.period_end:
                logging.warning(
                    "reported_period_end_conflict period=%s existing=%s incoming=%s "
                    "source=fmp:%s; keeping existing",
                    fiscal_year,
                    existing.period_end.isoformat(),
                    candidate.period_end.isoformat(),
                    endpoint,
                )
    periods = list(periods_by_year.values())
    periods.sort(key=lambda period: period.period_end, reverse=True)
    return tuple(periods)


def load_fixture_bundle(
    ticker: str,
    fixture_dir: Path,
    *,
    provider_port_dir: Path | None = None,
) -> dict[str, list[dict]]:
    """Load offline fixtures from explicitly selected directories.

    ``fixture_dir`` owns statement CSVs. Optional provider-port fixtures come
    from ``provider_port_dir`` when supplied, otherwise they are looked up in
    ``fixture_dir``. Directory names never alter lookup behavior.
    """

    symbol = ticker.upper()
    fmp_data: dict[str, list[dict]] = {}
    statement_dir = fixture_dir
    if (fixture_dir / f"{symbol}_income_statement.csv").exists():
        statement_mapping = {
            "income_statement": f"{symbol}_income_statement.csv",
            "balance_sheet": f"{symbol}_balance_sheet.csv",
            "cash_flow": f"{symbol}_cash_flow.csv",
        }
        for endpoint, filename in statement_mapping.items():
            path = statement_dir / filename
            if not path.exists():
                fmp_data[endpoint] = []
                continue
            with path.open(encoding="utf-8", newline="") as handle:
                fmp_data[endpoint] = list(csv.DictReader(handle))

    optional_dir = provider_port_dir if provider_port_dir is not None else fixture_dir
    optional_mapping = {
        "key_metrics": f"{symbol}_key_metrics.csv",
        "key_metrics_ttm": f"{symbol}_key_metrics_ttm.csv",
        "profile": f"{symbol}_profile.csv",
        "quote": f"{symbol}_quote.csv",
        "stock_peers": f"{symbol}_stock_peers.csv",
        "historical_price_eod": f"{symbol}_historical_price_eod.csv",
        "historical_price_adjusted": f"{symbol}_historical_price_adjusted.csv",
        "analyst_estimates": f"{symbol}_analyst_estimates.csv",
    }
    for endpoint, filename in optional_mapping.items():
        path = optional_dir / filename
        if not path.exists():
            continue
        with path.open(encoding="utf-8", newline="") as handle:
            fmp_data[endpoint] = list(csv.DictReader(handle))
    return fmp_data


def scaled_value_for_filing_line(
    line: FilingLineRef,
    fmp_data: dict[str, list[dict]],
) -> float | None:
    """Return the replay value for one fully identified filing line."""

    matches = {
        mapping
        for mapping in mapping_by_filing_line().values()
        if mapping.tag == line.tag
    }
    if len(matches) != 1:
        return None
    mapping = matches.pop()
    lookup = build_endpoint_lookup(fmp_data)
    record = lookup.get(mapping.native_endpoint, {}).get(
        _filing_line_fiscal_year(line)
    )
    raw_value, _field_used, _fallback = _extract_numeric_value(mapping, record)
    if raw_value is None:
        return None
    return scale_value(mapping, raw_value)
