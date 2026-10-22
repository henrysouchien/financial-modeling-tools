"""Issuer non-GAAP bridge v3 (transport design §1.3 as amended by C1, and
integrity design §2.9 ``BridgeClosureState``).

Chains are flattened at the producer: intermediate outputs travel as
``subtotals`` and there is no ``chained_from``.

The v3 wire types are owned by ``edgar_transport_contracts`` and imported
here directly; this module only adds the artifact codec, canonical bytes,
and the printed-value helper the historical rail consumes.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
import json
from typing import Any

from edgar_transport_contracts import (
    BridgeClosureState,
    BridgeFailureReason,
    BridgeGranularity,
    BridgeOperand,
    CellRef,
    EvidencedBlank,
    IssuerNonGaapBridgeResponseV3,
    IssuerNonGaapBridgeV3,
    MeasureType,
    MeasureUnit,
    NonGaapAcceptedAtEvidence,
    NonGaapAdjustmentRole,
    NonGaapBridgeComponent,
    NonGaapDocumentSource,
    NonGaapMetricFamily,
    NonGaapOutputMetricId,
    NonGaapTextEvidence,
    Scale,
    ScaleProvenance,
    ScaledQuantity,
    TypedPeriod,
)
from edgar_transport_contracts.enums import Basis
from edgar_transport_contracts.non_gaap_bridge import (
    BridgeClosureKind,
    BridgeOperandValue,
    NonGaapBridgeDiagnostic,
)

from .non_gaap_reconciliation_v1 import (
    VALUE_SCALE_FACTORS,
    _require_strict_json_object,
)


NonGaapBridgeComponentV3 = NonGaapBridgeComponent
BridgeClosureStateV3 = BridgeClosureState

ISSUER_NON_GAAP_BRIDGE_V3_ARTIFACT_CODEC = "issuer-non-gaap-bridge.v3+canonical-json"

def canonical_issuer_non_gaap_bridge_v3_bytes(
    value: IssuerNonGaapBridgeV3 | Mapping[str, Any],
) -> bytes:
    validated = (
        value
        if isinstance(value, IssuerNonGaapBridgeV3)
        else IssuerNonGaapBridgeV3.model_validate_json(
            json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            )
        )
    )
    return json.dumps(
        validated.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")


def decode_issuer_non_gaap_bridge_v3_bytes(payload: bytes) -> IssuerNonGaapBridgeV3:
    """Decode one ``issuer-non-gaap-bridge.v3`` bridge artifact."""

    _require_strict_json_object(payload)
    parsed = json.loads(payload)
    version = parsed.get("contract_version")
    if version != "issuer-non-gaap-bridge.v3":
        raise ValueError(
            f"version_refusal: unsupported non-GAAP bridge contract_version "
            f"(expected one of ['issuer-non-gaap-bridge.v3'], received {version!r})"
        )
    return IssuerNonGaapBridgeV3.model_validate_json(payload)


def v3_operand_printed_decimal(
    bridge: IssuerNonGaapBridgeV3,
    operand: BridgeOperand | NonGaapBridgeComponent,
) -> Decimal:
    """One operand's value in ``bridge.value_scale`` units, derived from ``ScaledQuantity``."""

    if isinstance(operand.value, EvidencedBlank):
        raise ValueError("an evidenced blank has no printed decimal value")
    canonical = operand.value.canonical_value
    if bridge.metric_family == "per_share" or operand.value.measure.unit not in {
        "dollars",
        "count",
        "share_count",
    }:
        return canonical
    return canonical / VALUE_SCALE_FACTORS[bridge.value_scale]



__all__ = [
    "Basis",
    "BridgeClosureKind",
    "BridgeClosureState",
    "BridgeClosureStateV3",
    "BridgeFailureReason",
    "BridgeGranularity",
    "BridgeOperand",
    "BridgeOperandValue",
    "CellRef",
    "EvidencedBlank",
    "ISSUER_NON_GAAP_BRIDGE_V3_ARTIFACT_CODEC",
    "IssuerNonGaapBridgeResponseV3",
    "IssuerNonGaapBridgeV3",
    "MeasureType",
    "MeasureUnit",
    "NonGaapAcceptedAtEvidence",
    "NonGaapAdjustmentRole",
    "NonGaapBridgeComponent",
    "NonGaapBridgeComponentV3",
    "NonGaapBridgeDiagnostic",
    "NonGaapDocumentSource",
    "NonGaapMetricFamily",
    "NonGaapOutputMetricId",
    "NonGaapTextEvidence",
    "Scale",
    "ScaleProvenance",
    "ScaledQuantity",
    "TypedPeriod",
    "canonical_issuer_non_gaap_bridge_v3_bytes",
    "decode_issuer_non_gaap_bridge_v3_bytes",
    "v3_operand_printed_decimal",
]
