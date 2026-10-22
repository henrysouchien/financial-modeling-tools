"""Register default provider-port adapters for tests and future wiring."""

from __future__ import annotations

from schema.provider_port.adapters.fmp.adapter import (
    FinancialsFetchTransport,
    FmpAdapter,
    FmpIssuerUniverseConfiguration,
    FmpTransport,
    PreviewFmpAdapter,
)
from industry_slice_core.profile_normalization import ProviderCapability
from schema.provider_port.adapters.edgar.adapter import EdgarAdapter
from schema.provider_port.adapters.edgar.kpi_adapter import EdgarKpiAdapter
from schema.provider_port.adapters.edgar.non_gaap_bridge_adapter import (
    EdgarNonGaapBridgeAdapter,
)
from schema.provider_port.adapters.fmp.overlay import CustomBindingOverlay
from schema.provider_port.registry import (
    EDGAR_PROVIDER_ID,
    FMP_PROVIDER_ID,
    ProviderRegistry,
)

def build_default_registry(
    *,
    fmp_data: dict | None = None,
    fmp_transport: FmpTransport | None = None,
    overlay: CustomBindingOverlay | None = None,
    edgar_adapter: EdgarAdapter | None = None,
    edgar_kpi_adapter: EdgarKpiAdapter | None = None,
    edgar_non_gaap_bridge_adapter: EdgarNonGaapBridgeAdapter | None = None,
    fmp_issuer_universe: FmpIssuerUniverseConfiguration | None = None,
) -> ProviderRegistry:
    """Build the default registry without advertising unbacked FMP capabilities.

    Supplying replay data keeps tests offline.  With no replay data, the
    production default uses the same FMP client seam as ``financials_fetch``.
    Explicitly supplied EDGAR adapters advertise only the capabilities they
    implement; filing-facts and typed v2 KPI-envelope adapters remain separate.
    """

    transport = (
        fmp_transport
        if fmp_transport is not None
        else (FinancialsFetchTransport() if fmp_data is None else None)
    )
    adapter = FmpAdapter(
        fmp_data=fmp_data,
        transport=transport,
        overlay=overlay,
        issuer_universe=fmp_issuer_universe,
    )
    advertised = adapter.advertised_capabilities()
    registry = ProviderRegistry(
        default_providers={capability: FMP_PROVIDER_ID for capability in advertised}
    )
    for capability in advertised:
        registry.register(
            FMP_PROVIDER_ID,
            capability,
            adapter,
            authoritative_registration=(
                fmp_issuer_universe.authoritative_registration
                if capability is ProviderCapability.ISSUER_UNIVERSE
                and fmp_issuer_universe is not None
                else None
            ),
        )
    if edgar_adapter is not None:
        for capability in edgar_adapter.advertised_capabilities():
            registry.register(
                EDGAR_PROVIDER_ID,
                capability,
                edgar_adapter,
                authoritative_registration=None,
            )
    if edgar_kpi_adapter is not None:
        for capability in edgar_kpi_adapter.advertised_capabilities():
            registry.register(
                EDGAR_PROVIDER_ID,
                capability,
                edgar_kpi_adapter,
                authoritative_registration=None,
            )
    if edgar_non_gaap_bridge_adapter is not None:
        for capability in edgar_non_gaap_bridge_adapter.advertised_capabilities():
            registry.register(
                EDGAR_PROVIDER_ID,
                capability,
                edgar_non_gaap_bridge_adapter,
                authoritative_registration=None,
            )
    return registry


def build_preview_registry(
    *,
    fmp_data: dict | None = None,
    fmp_transport: FmpTransport | None = None,
    overlay: CustomBindingOverlay | None = None,
) -> ProviderRegistry:
    """Build the preview's canonical capability registry.

    PreviewFmpAdapter retains the preview-only tolerant statement failure policy;
    all payloads exposed to the build remain canonical provider-port contracts.
    """

    transport = (
        fmp_transport
        if fmp_transport is not None
        else (FinancialsFetchTransport() if fmp_data is None else None)
    )
    adapter = PreviewFmpAdapter(
        fmp_data=fmp_data,
        transport=transport,
        overlay=overlay,
    )
    advertised = adapter.advertised_capabilities()
    registry = ProviderRegistry(
        default_providers={capability: FMP_PROVIDER_ID for capability in advertised}
    )
    for capability in advertised:
        registry.register(
            FMP_PROVIDER_ID,
            capability,
            adapter,
            authoritative_registration=None,
        )
    return registry


__all__ = [
    "build_default_registry",
    "build_preview_registry",
]
