"""Adapter servability and filing-assertion construction."""

from __future__ import annotations

from decimal import Decimal
from typing import TYPE_CHECKING, NoReturn

from schema.provider_port.contracts import FilingLineRef
from value_semantics_core import PeriodRef

if TYPE_CHECKING:
    from schema.build_model_product_contracts import (
        AdapterAssertionKind,
        AdapterAssertionOutcome,
    )
    from schema.provider_port.adapters.edgar import EdgarOccurrenceArtifacts
    from schema.provider_port.registry import ProviderRegistry


def build_adapter_assertion_outcome(
    *,
    outcome: "AdapterAssertionKind",
    required_role: str,
    period: PeriodRef,
    asserted_tag: str | None,
    filing_line: FilingLineRef | None,
    vendor_value: Decimal,
    filing_value: Decimal | None,
    repair_target: str,
) -> "AdapterAssertionOutcome":
    """Construct the single product-contract representation for rung 1."""

    from schema.build_model_product_contracts import AdapterAssertionOutcome

    return AdapterAssertionOutcome(
        outcome=outcome,
        required_role=required_role,
        period=period,
        asserted_tag=asserted_tag,
        filing_line=filing_line,
        vendor_value=vendor_value,
        filing_value=filing_value,
        repair_target=repair_target,
    )


def raise_adapter_unresolved_for_required_line(
    *,
    producer: str,
    native_name: str,
    line: FilingLineRef,
) -> NoReturn:
    """Raise the product contract's distinct R9 adapter-resolution blocker."""

    from schema.build_model_product_contracts import (
        AdapterUnresolvedForRequiredLine,
    )

    raise AdapterUnresolvedForRequiredLine(
        producer=producer,
        native_name=native_name,
        line=line,
    )


def build_servability_registry(
    *,
    edgar_occurrence_artifacts: "EdgarOccurrenceArtifacts | None" = None,
) -> "ProviderRegistry":
    """Return provider registrations over optional sealed Edgar artifacts."""

    from schema.provider_port.adapters.edgar import EdgarAdapter
    from schema.provider_port.adapters.fmp.adapter import FmpAdapter
    from industry_slice_core.profile_normalization import ProviderCapability
    from schema.provider_port.registry import (
        EDGAR_PROVIDER_ID,
        FMP_PROVIDER_ID,
        ProviderRegistry,
    )

    registry = ProviderRegistry()
    registry.register(
        FMP_PROVIDER_ID,
        ProviderCapability.STATEMENT_FACTS,
        FmpAdapter(fmp_data={}),
    )
    registry.register(
        EDGAR_PROVIDER_ID,
        ProviderCapability.STATEMENT_FACTS,
        EdgarAdapter(occurrence_artifacts=edgar_occurrence_artifacts),
    )
    return registry


__all__ = [
    "build_adapter_assertion_outcome",
    "build_servability_registry",
    "raise_adapter_unresolved_for_required_line",
]
