"""Typed consumer boundary for EDGAR operational-KPI driver rows v2."""

from __future__ import annotations

from collections.abc import Mapping
from datetime import date
from decimal import Decimal
import json
from typing import Annotated, Any, Literal

from edgar_transport_contracts.dimensions import XbrlFactRef
from edgar_transport_contracts.enums import MeasureUnit, ScaleProvenance
from edgar_transport_contracts.kpi import KpiExplanation, TableContext
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    SerializerFunctionWrapHandler,
    ValidationError,
    model_serializer,
    model_validator,
)

from schema.business_model import ObservationBasis
from schema.scale_conversion import Scale


EDGAR_OPERATIONAL_KPI_DRIVER_ROWS_CONTRACT_VERSION = "edgar-operational-kpi-driver-rows.v2"


class EdgarOperationalKpiDriverRowsContractError(ValueError):
    """The producer payload violates the typed v2 wire contract."""


class _KpiType(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class MeasureType(_KpiType):
    unit: MeasureUnit
    observation_basis: ObservationBasis
    currency: str | None = Field(default=None, pattern=r"^[A-Z]{3}$")
    counted_entity: str | None = Field(default=None, min_length=1)

    @property
    def admissible_scales(self) -> tuple[Scale, ...]:
        if self.unit in {"dollars", "count", "share_count"}:
            return ("units", "thousands", "millions", "billions")
        return ("units",)

    @model_validator(mode="after")
    def _bind_measure(self) -> MeasureType:
        if (self.unit in {"dollars", "per_count", "per_share"}) != (self.currency is not None):
            raise ValueError("currency is required exactly for monetary measures")
        if (self.unit in {"count", "per_count"}) != (self.counted_entity is not None):
            raise ValueError("count and per-count measures require their entity")
        return self


class ScaledQuantity(_KpiType):
    kind: Literal["quantity"] = "quantity"
    measure: MeasureType
    canonical_value: Decimal = Field(allow_inf_nan=False)
    presentation_scale: Scale
    scale_provenance: ScaleProvenance
    scale_evidence: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _bind_scale(self) -> ScaledQuantity:
        # edgar-transport-contracts 0.1.3 (producer owner: kpi_schema / edgar_transport_contracts
        # ScaledQuantity.bind_scale): a per-entity price scales only by the scale printed in its own
        # figure (``value_text``); no table or preamble declaration ever scales one.
        if self.presentation_scale not in self.measure.admissible_scales and not (
            self.measure.unit in {"per_count", "per_share"} and self.scale_provenance == "value_text"
        ):
            raise ValueError("scale is incompatible with measure")
        if (self.scale_provenance == "none") != (self.scale_evidence is None):
            raise ValueError("scale provenance and evidence must agree")
        if self.scale_provenance == "none" and self.presentation_scale != "units":
            raise ValueError("scaled presentation requires a declaration")
        if self.scale_provenance == "monetary_preamble" and self.measure.unit != "dollars":
            raise ValueError("monetary preamble cannot scale this measure")
        return self


class TypedPeriod(_KpiType):
    fiscal_year: int = Field(ge=1, le=9999)
    period_type: Literal["annual", "quarterly", "ytd", "ttm"]
    temporal_kind: Literal["instant", "duration"]
    period_end: date
    period_start: date | None = None
    quarter: int | None = Field(default=None, ge=1, le=4)
    inferred: bool = False

    @model_validator(mode="after")
    def _bind_period(self) -> TypedPeriod:
        if self.temporal_kind == "instant" and self.period_start is not None:
            raise ValueError("instant has no start date")
        if self.period_start is not None and self.period_start > self.period_end:
            raise ValueError("period start follows end")
        if (self.period_type == "quarterly") != (self.quarter is not None):
            raise ValueError("quarter belongs exactly to quarterly periods")
        return self


class CanonicalSeriesMatch(_KpiType):
    issuer_series_key: str = Field(min_length=1)
    canonical_role: str | None = Field(default=None, pattern=r"^operating\..+$")
    match_basis: Literal["approved_binding", "issuer_specific"]
    binding_ref: str | None = Field(default=None, min_length=1)

    @model_validator(mode="after")
    def _bind_identity(self) -> CanonicalSeriesMatch:
        matched = self.match_basis == "approved_binding"
        if matched != (self.canonical_role is not None):
            raise ValueError("canonical role requires an approved match")
        if matched != (self.binding_ref is not None):
            raise ValueError("approved match requires its binding reference")
        return self


class KpiDate(_KpiType):
    kind: Literal["date"] = "date"
    value: date


class KpiGap(_KpiType):
    kind: Literal["gap"] = "gap"
    reason: str = Field(min_length=1)
    detail: str = Field(min_length=1)


class KpiLocator(_KpiType):
    accession: str = Field(min_length=1)
    section: str = Field(min_length=1)
    table_id: str | None = None
    row: int | None = Field(default=None, ge=0)
    column: int | None = Field(default=None, ge=0)
    span: str = Field(min_length=1)


class TypedKpiObservation(_KpiType):
    observation_id: str = Field(min_length=1)
    metric_name: str = Field(min_length=1)
    series: CanonicalSeriesMatch
    period: TypedPeriod | None
    value: Annotated[ScaledQuantity | KpiDate | KpiGap, Field(discriminator="kind")]
    value_raw: str
    locator: KpiLocator
    additional_locators: tuple[KpiLocator, ...] = ()
    dimensions: dict[str, str]
    value_semantic: Literal["level", "growth_rate", "change_amount"]
    grounded: bool
    calculation_ready: bool
    lane: Literal["table", "prose"]
    notes: tuple[str, ...] = ()
    xbrl_fact_ref: XbrlFactRef | None = None
    explanations: tuple[KpiExplanation, ...] = ()
    constituent_observation_ids: tuple[str, ...] = ()

    @model_serializer(mode="wrap")
    def _omit_empty_explanations(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data: dict[str, Any] = handler(self)
        if not data.get("explanations"):
            data.pop("explanations", None)
        if not data.get("constituent_observation_ids"):
            data.pop("constituent_observation_ids", None)
        return data
    @model_validator(mode="after")
    def _bind_value(self) -> TypedKpiObservation:
        if self.value_semantic == "change_amount" and self.calculation_ready:
            raise ValueError("change_amount observations are not calculation_ready")
        unresolved_period = isinstance(self.value, KpiGap) and self.value.reason == "unresolved_period"
        if (self.period is None) != unresolved_period:
            raise ValueError("period may be absent exactly for unresolved_period gaps")
        if not isinstance(self.value, ScaledQuantity):
            if self.calculation_ready:
                raise ValueError("dates and gaps are not calculation_ready")
        elif self.period is not None and (
            (self.value.measure.observation_basis == "period_end")
            != (self.period.temporal_kind == "instant")
        ):
            raise ValueError("period_end basis requires an instant and vice versa")
        # Parsing value_raw is the producer's canonicalizer's responsibility.
        return self


class OmittedTable(_KpiType):
    """Union of the producer vocabularies this consumer has persisted payloads for.

    The ``interpreter``/``reader`` stages and the first reason block are the v22
    producer (edgar_updater before ``27be078d``); the Turn-1 seed stores under
    ``api/research/corpus/*/capture/edgar_kpis/`` still carry them, so they stay
    for as long as such a store exists. The ``binder`` stage and the second block
    are the S4 envelope + binder lane. No reason is mapped onto another.
    """

    table_id: str
    section: str | None
    stage: Literal["parser", "interpreter", "reader", "canonicalizer", "binder"]
    reason: Literal[
        "outside_section", "layout_or_junk", "empty_after_headers",
        "structural_suppression", "class_suppression", "no_positive_signal",
        "statement_overlap", "missing_headers", "xbrl_match_required",
        "classifier_unavailable", "unresolved_measure", "unresolved_period",
        "junk", "xbrl_redundant", "binder_unavailable", "binding_invalid", "not_operating",
    ]
    detail: str


class KpiLaneGap(_KpiType):
    """``prose_lane_not_warmed`` is the pre-S4 producer's (persisted payloads carry it);
    ``not_disclosed`` is S4's: a series the prior filing bound has no row in this one."""

    lane: Literal["table", "prose"]
    reason: Literal[
        "value_unparseable", "period_unresolved", "unit_unresolved", "ambiguous",
        "prose_lane_not_warmed", "reader_unavailable", "not_disclosed",
    ]
    detail: str


class ReaderRecord(_KpiType):
    lane: Literal["table", "prose"]
    reader_version: str
    provider_name: str | None
    model_name: str | None


class KpiDriverRowsV2(_KpiType):
    contract_version: Literal["edgar-operational-kpi-driver-rows.v2"] = EDGAR_OPERATIONAL_KPI_DRIVER_ROWS_CONTRACT_VERSION
    rows: tuple[TypedKpiObservation, ...]
    omitted_tables: tuple[OmittedTable, ...]
    lane_gaps: tuple[KpiLaneGap, ...]
    readers: tuple[ReaderRecord, ...]
    table_context: tuple[TableContext, ...] = ()

    @model_serializer(mode="wrap")
    def _omit_empty_table_context(self, handler: SerializerFunctionWrapHandler) -> dict[str, Any]:
        data: dict[str, Any] = handler(self)
        if not data.get("table_context"):
            data.pop("table_context", None)
        return data

class _DriverRowsResponse(KpiDriverRowsV2):
    """The producer's additive HTTP envelope around the sealed artifact."""

    model_config = ConfigDict(extra="allow", frozen=True, strict=True)
    status: Literal["success"]
    row_count: int = Field(ge=0)

    @model_validator(mode="after")
    def _bind_row_count(self) -> _DriverRowsResponse:
        if self.row_count != len(self.rows):
            raise ValueError("row_count must match the number of rows")
        return self


def validate_edgar_operational_kpi_driver_rows_v2(
    payload: Mapping[str, Any] | KpiDriverRowsV2,
) -> KpiDriverRowsV2:
    """Decode the v2 artifact or producer HTTP response without reinterpreting it."""
    if isinstance(payload, KpiDriverRowsV2):
        return payload
    if not isinstance(payload, Mapping):
        raise EdgarOperationalKpiDriverRowsContractError("KPI driver rows must be an object")
    try:
        raw = json.dumps(payload, ensure_ascii=False, allow_nan=False)
        if "status" in payload or "row_count" in payload:
            response = _DriverRowsResponse.model_validate_json(raw, strict=True)
            return KpiDriverRowsV2(
                contract_version=response.contract_version,
                rows=response.rows,
                omitted_tables=response.omitted_tables,
                lane_gaps=response.lane_gaps,
                readers=response.readers,
                table_context=response.table_context,
            )
        return KpiDriverRowsV2.model_validate_json(raw, strict=True)
    except (TypeError, ValueError, ValidationError) as exc:
        raise EdgarOperationalKpiDriverRowsContractError(
            f"invalid {EDGAR_OPERATIONAL_KPI_DRIVER_ROWS_CONTRACT_VERSION}: {exc}"
        ) from exc


def adapt_edgar_operational_kpi_driver_rows_payload(
    payload: Mapping[str, Any] | KpiDriverRowsV2,
) -> KpiDriverRowsV2:
    """Return the typed producer envelope, never an untyped row projection."""
    return validate_edgar_operational_kpi_driver_rows_v2(payload)


__all__ = [
    "EDGAR_OPERATIONAL_KPI_DRIVER_ROWS_CONTRACT_VERSION",
    "EdgarOperationalKpiDriverRowsContractError", "MeasureUnit",
    "ScaleProvenance", "MeasureType", "ScaledQuantity", "TypedPeriod",
    "CanonicalSeriesMatch", "KpiDate", "KpiGap", "KpiLocator", "XbrlFactRef",
    "KpiExplanation", "TableContext", "TypedKpiObservation",
    "OmittedTable", "KpiLaneGap", "ReaderRecord", "KpiDriverRowsV2",
    "validate_edgar_operational_kpi_driver_rows_v2",
    "adapt_edgar_operational_kpi_driver_rows_payload",
]
