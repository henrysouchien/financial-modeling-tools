"""Capability-scoped provider protocols."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date
from typing import TYPE_CHECKING, Protocol, runtime_checkable

from .contracts import (
    CanonicalNonGaapBridge,
    EstimateFactsResult,
    EstimatePeriod,
    FilingLineObservation,
    FilingLineRef,
    FiscalWindow,
    MetricSeries,
    PeerUniverse,
    PriceSeries,
    PriceSeriesBasis,
    Quote,
    ReportedPeriod,
    VendorProvenance,
)
from industry_slice_core.industry_types import CompanyProfile

if TYPE_CHECKING:
    from schema.provider_query_contract import (
        EvidenceProviderResult,
        IssuerUniverseProviderRequest,
        IssuerUniverseProviderResult,
        StatementEvidenceProviderRequest,
        ValuationCompsProviderRequest,
    )


@runtime_checkable
class StatementFactsPort(Protocol):
    provider_name: str

    def fetch_filing_line_facts(
        self,
        ticker: str,
        *,
        lines: Sequence[FilingLineRef],
        window: FiscalWindow,
    ) -> tuple[tuple[FilingLineObservation, ...], tuple[ReportedPeriod, ...]]: ...

@runtime_checkable
class KpiObservationsPort(Protocol):
    provider_name: str

    def fetch_kpi_observations(
        self,
        ticker: str,
        *,
        accessions: Sequence[str],
        window: FiscalWindow,
    ) -> tuple[FilingLineObservation, ...]: ...


@runtime_checkable
class NonGaapBridgesPort(Protocol):
    provider_name: str

    def fetch_bridges(
        self,
        ticker: str | None = None,
        *,
        accessions: Sequence[str] | None = None,
    ) -> tuple[CanonicalNonGaapBridge, ...]: ...

@runtime_checkable
class StatementEvidenceQueryPort(Protocol):
    """Typed BMA-199 statement-evidence query boundary."""

    provider_name: str

    def query_statement_evidence(
        self,
        request: StatementEvidenceProviderRequest,
    ) -> EvidenceProviderResult: ...


@runtime_checkable
class ValuationCompsEvidenceQueryPort(Protocol):
    """Typed BMA-199 valuation-comps query boundary."""

    provider_name: str

    def query_valuation_comps(
        self,
        request: ValuationCompsProviderRequest,
    ) -> EvidenceProviderResult: ...


@runtime_checkable
class ConceptProvenanceMatcher(Protocol):
    """Optional adapter metadata surface for persisted provenance validation."""

    def matches_concept_provenance(
        self,
        concept_id: str,
        provenance: VendorProvenance,
        *,
        fallback_native_field_used: str | None = None,
        canonical_vendor_provenance: bool = False,
    ) -> bool: ...


@runtime_checkable
class MissingValuePolicyInspector(Protocol):
    """Optional adapter surface for request-scoped missing-value decisions."""

    def missing_value_policy_triggered_concepts(
        self,
        ticker: str,
        *,
        window: FiscalWindow,
    ) -> frozenset[str]: ...


@runtime_checkable
class CompanyProfilePort(Protocol):
    provider_name: str

    def fetch_profile(self, ticker: str) -> CompanyProfile: ...


@runtime_checkable
class QuotePort(Protocol):
    provider_name: str

    def fetch_quote(self, ticker: str) -> Quote: ...

    def fetch_quotes(self, tickers: Sequence[str]) -> dict[str, Quote]: ...


@runtime_checkable
class EstimatesPort(Protocol):
    provider_name: str

    def fetch_estimates(
        self,
        ticker: str,
        *,
        metrics: Sequence[str],
        period: EstimatePeriod,
        limit: int,
    ) -> EstimateFactsResult: ...


@runtime_checkable
class MetricSeriesPort(Protocol):
    provider_name: str

    def fetch_metric_series_annual(
        self,
        ticker: str,
        *,
        metrics: Sequence[str],
        window: FiscalWindow,
    ) -> tuple[MetricSeries, ...]: ...

    def fetch_metric_series_ttm(
        self,
        ticker: str,
        *,
        metrics: Sequence[str],
        as_of_cutoff: date,
        limit: int,
    ) -> tuple[MetricSeries, ...]: ...


@runtime_checkable
class PeerUniversePort(Protocol):
    provider_name: str

    def fetch_peers(self, ticker: str) -> PeerUniverse: ...


@runtime_checkable
class IssuerUniversePort(Protocol):
    """Sole typed provider port for annual issuer-universe acquisition."""

    provider_name: str

    def query_issuer_universe(
        self,
        request: IssuerUniverseProviderRequest,
    ) -> IssuerUniverseProviderResult: ...


@runtime_checkable
class PriceSeriesPort(Protocol):
    provider_name: str

    def fetch_prices(
        self,
        ticker: str,
        *,
        start: date,
        end: date,
        basis: PriceSeriesBasis,
    ) -> PriceSeries: ...
