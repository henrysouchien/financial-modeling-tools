"""Sealed KPI JSONL rows and their typed envelope metadata.

The sealed form adds only an artifact version to a driver observation. Both
forms enter the same wire-to-domain adapter; no numeric value is reinterpreted.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import json
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict

from .operational_kpi_driver_rows_v2 import (
    KpiDriverRowsV2,
    KpiLaneGap,
    OmittedTable,
    ReaderRecord,
    TableContext,
    TypedKpiObservation,
)


EDGAR_KPI_OBSERVATION_CONTRACT_VERSION = "edgar-kpi-observation.v3"


class EdgarKpiObservationInvalid(ValueError):
    """A sealed KPI row or its envelope metadata violates the v3 contract."""


class EdgarKpiObservationV3(TypedKpiObservation):
    contract_version: Literal["edgar-kpi-observation.v3"]

    @classmethod
    def from_driver_row(cls, row: TypedKpiObservation) -> EdgarKpiObservationV3:
        """Seal a typed driver observation without changing its fields."""
        return cls(
            **{name: getattr(row, name) for name in TypedKpiObservation.model_fields},
            contract_version=EDGAR_KPI_OBSERVATION_CONTRACT_VERSION,
        )

    def driver_row(self) -> TypedKpiObservation:
        """Return the shared typed observation without the artifact version."""
        return TypedKpiObservation(
            **{name: getattr(self, name) for name in TypedKpiObservation.model_fields},
        )

    def model_dump(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
        data = super().model_dump(*args, **kwargs)
        if data.get("xbrl_fact_ref") is None:
            data.pop("xbrl_fact_ref", None)
        return data

class KpiObservationCaptureMetadataV3(BaseModel):
    """Envelope-level data; capture-specific transport details stay additive."""

    model_config = ConfigDict(extra="allow", frozen=True, strict=True)
    contract_version: Literal["edgar-operational-kpi-driver-rows.v2"]
    omitted_tables: tuple[OmittedTable, ...]
    lane_gaps: tuple[KpiLaneGap, ...]
    readers: tuple[ReaderRecord, ...]
    table_context: tuple[TableContext, ...] = ()

def validate_edgar_kpi_observation_v3(
    payload: Mapping[str, Any] | EdgarKpiObservationV3,
) -> EdgarKpiObservationV3:
    """Decode exactly one v3 JSONL observation, keeping Decimal values typed."""
    if isinstance(payload, EdgarKpiObservationV3):
        return payload
    try:
        return EdgarKpiObservationV3.model_validate_json(
            json.dumps(payload, ensure_ascii=False, allow_nan=False), strict=True,
        )
    except (TypeError, ValueError) as exc:
        raise EdgarKpiObservationInvalid(
            f"invalid {EDGAR_KPI_OBSERVATION_CONTRACT_VERSION} row: {exc}"
        ) from exc


def assemble_edgar_kpi_observation_artifact(
    rows: Sequence[Mapping[str, Any] | EdgarKpiObservationV3],
    metadata: Mapping[str, Any] | KpiObservationCaptureMetadataV3,
) -> KpiDriverRowsV2:
    """Rejoin sealed observations with their sibling envelope metadata."""
    try:
        capture = (
            metadata if isinstance(metadata, KpiObservationCaptureMetadataV3)
            else KpiObservationCaptureMetadataV3.model_validate_json(
                json.dumps(metadata, ensure_ascii=False, allow_nan=False), strict=True,
            )
        )
        return KpiDriverRowsV2(
            contract_version=capture.contract_version,
            rows=tuple(validate_edgar_kpi_observation_v3(row).driver_row() for row in rows),
            omitted_tables=capture.omitted_tables,
            lane_gaps=capture.lane_gaps,
            readers=capture.readers,
            table_context=capture.table_context,
        )
    except (TypeError, ValueError) as exc:
        raise EdgarKpiObservationInvalid(f"invalid sealed KPI artifact: {exc}") from exc


def decode_edgar_kpi_observation_artifact(
    payload_bytes: bytes,
    metadata: Mapping[str, Any] | KpiObservationCaptureMetadataV3,
) -> KpiDriverRowsV2:
    """Decode v3 JSONL plus metadata, including an unread capture with no rows."""
    try:
        rows = [json.loads(line) for line in payload_bytes.splitlines()]
    except (UnicodeDecodeError, ValueError) as exc:
        raise EdgarKpiObservationInvalid(f"invalid KPI observation JSONL: {exc}") from exc
    return assemble_edgar_kpi_observation_artifact(rows, metadata)


__all__ = [
    "EDGAR_KPI_OBSERVATION_CONTRACT_VERSION", "EdgarKpiObservationInvalid",
    "EdgarKpiObservationV3", "KpiObservationCaptureMetadataV3",
    "validate_edgar_kpi_observation_v3", "assemble_edgar_kpi_observation_artifact",
    "decode_edgar_kpi_observation_artifact",
]
