"""EDGAR provider adapters."""

from .adapter import (
    EdgarAdapter,
    EdgarOccurrenceArtifacts,
    latest_reported_period_accepted_by_cutoff,
    reported_period_candidates_from_validated_payload,
)
from .kpi_adapter import (
    EdgarKpiAdapter,
    EdgarKpiFetchResult,
    EdgarKpiIdentityCollision,
    kpi_quantity_display,
)
from .non_gaap_bridge_adapter import EdgarNonGaapBridgeAdapter

__all__ = [
    "EdgarAdapter",
    "EdgarOccurrenceArtifacts",
    "EdgarKpiAdapter",
    "EdgarKpiFetchResult",
    "EdgarKpiIdentityCollision",
    "EdgarNonGaapBridgeAdapter",
    "kpi_quantity_display",
    "latest_reported_period_accepted_by_cutoff",
    "reported_period_candidates_from_validated_payload",
]
