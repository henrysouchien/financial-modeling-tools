"""The EDGAR operational-KPI v2 wire-to-domain adapter."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
import json

from industry_slice_core.profile_normalization import ProviderCapability
from schema.provider_port.adapters.edgar.wire.kpi_observation_v3 import (
    EdgarKpiObservationV3,
    KpiObservationCaptureMetadataV3,
    assemble_edgar_kpi_observation_artifact,
)
from schema.provider_port.adapters.edgar.wire.operational_kpi_driver_rows_v2 import (
    KpiDriverRowsV2,
    KpiGap,
    KpiLaneGap,
    MeasureType,
    OmittedTable,
    ReaderRecord,
    ScaledQuantity,
    TypedKpiObservation,
    _four_quarter_series_key as _wire_four_quarter_series_key,
    derive_four_quarter_annual_kpi_observations as _wire_derive_four_quarter_annual_kpi_observations,
    validate_edgar_operational_kpi_driver_rows_v2,
)
from schema.provider_port.contracts import (
    FilingLineFact,
    FilingLineObservation,
    FilingLineRef,
    FiscalWindow,
    VendorProvenance,
    mint_source_fact_ref,
)
from schema.provider_port.registry import EDGAR_PROVIDER_ID
from value_semantics_core import (
    DatePeriod,
    FiscalYearPeriod,
    NumericDisplaySpec,
    NumericScale,
    Quantity,
    normalize_decimal_v1,
)


class EdgarKpiIdentityCollision(ValueError):
    """Two producer points repeat an observation ID within one envelope."""

    def __init__(self, line: FilingLineRef, first_locator: str, second_locator: str) -> None:
        self.line = line
        self.first_locator = first_locator
        self.second_locator = second_locator
        super().__init__(
            "EDGAR KPI identity collision for "
            f"{line.accession}/{line.period.canonical_label()}/{line.tag}: "
            f"{first_locator} <> {second_locator}"
        )


@dataclass(frozen=True, slots=True)
class EdgarKpiFetchResult:
    """Facts and nonnumeric diagnostics, with reader coverage kept as data."""

    observations: tuple[FilingLineObservation, ...]
    unperiodized: tuple[str, ...]
    unit_by_observation_id: Mapping[str, MeasureType]
    omitted_tables: tuple[OmittedTable, ...]
    lane_gaps: tuple[KpiLaneGap, ...]
    readers: tuple[ReaderRecord, ...]


def kpi_quantity_display(value: ScaledQuantity) -> NumericDisplaySpec:
    """Map the producer's typed measure and presentation scale exactly once."""
    measure = value.measure
    scale: NumericScale = "unit" if value.presentation_scale == "units" else value.presentation_scale
    if measure.unit == "dollars":
        return NumericDisplaySpec(
            numeric_kind="currency", scale=scale,
            currency=measure.currency, unit_label=measure.currency,
        )
    if measure.unit == "per_count":
        return NumericDisplaySpec(
            numeric_kind="currency", currency=measure.currency,
            unit_label=f"{measure.currency} per {measure.counted_entity}",
        )
    if measure.unit in ("count", "share_count"):
        return NumericDisplaySpec(
            numeric_kind="count", scale=scale,
            unit_label=measure.counted_entity if measure.unit == "count" else "shares",
        )
    if measure.unit == "per_share":
        return NumericDisplaySpec(numeric_kind="per_share", currency=measure.currency)
    if measure.unit == "days":
        return NumericDisplaySpec(numeric_kind="duration", unit_label="days")
    return NumericDisplaySpec(numeric_kind=measure.unit)


def _series_variant(row: TypedKpiObservation) -> str:
    return json.dumps(
        {
            "measure": row.value.measure.model_dump(mode="json")
            if isinstance(row.value, ScaledQuantity) else None,
            "value_semantic": row.value_semantic,
            "dimensions": row.dimensions,
        },
        sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    )


def _four_quarter_series_key(row: TypedKpiObservation) -> tuple[object, ...]:
    return _wire_four_quarter_series_key(row)


def derive_four_quarter_annual_kpi_observations(
    rows: Sequence[TypedKpiObservation],
) -> tuple[TypedKpiObservation, ...]:
    return _wire_derive_four_quarter_annual_kpi_observations(rows)


class EdgarKpiAdapter:
    """Expose one typed observation plane from live envelopes or sealed v3 rows."""

    provider_name = EDGAR_PROVIDER_ID

    def __init__(
        self,
        *,
        kpi_artifacts: Mapping[
            str,
            KpiDriverRowsV2 | Mapping[str, object]
            | Sequence[Mapping[str, object] | EdgarKpiObservationV3],
        ],
        kpi_capture_metadata: Mapping[
            str, Mapping[str, object] | KpiObservationCaptureMetadataV3
        ] | None = None,
    ) -> None:
        self._payloads: dict[str, KpiDriverRowsV2] = {}
        for accession, payload in kpi_artifacts.items():
            if isinstance(payload, (KpiDriverRowsV2, Mapping)):
                envelope = validate_edgar_operational_kpi_driver_rows_v2(payload)
            else:
                envelope = assemble_edgar_kpi_observation_artifact(
                    payload, (kpi_capture_metadata or {})[accession],
                )
            self._payloads[accession] = envelope
        variants: dict[tuple[str, str], set[str]] = {}
        for accession, payload in self._payloads.items():
            for row in payload.rows:
                if row.locator.accession != accession:
                    raise ValueError(
                        "KPI artifact accession key does not match row locator accession: "
                        f"{accession!r} != {row.locator.accession!r}"
                    )
                variants.setdefault((accession, row.series.issuer_series_key), set()).add(_series_variant(row))
        self._variants = variants

    def advertised_capabilities(self) -> frozenset[ProviderCapability]:
        return frozenset({ProviderCapability.OPERATING_OBSERVATIONS})

    def fetch_kpi_observations(
        self,
        ticker: str,
        *,
        accessions: Sequence[str],
        window: FiscalWindow,
    ) -> tuple[FilingLineObservation, ...]:
        return self.fetch_kpi_result(ticker, accessions=accessions, window=window).observations

    def fetch_kpi_result(
        self,
        ticker: str,
        *,
        accessions: Sequence[str],
        window: FiscalWindow,
    ) -> EdgarKpiFetchResult:
        del ticker
        start_year = window.end_fiscal_year - window.n_years + 1
        points: list[tuple[FilingLineRef, TypedKpiObservation]] = []
        unperiodized: list[str] = []
        units: dict[str, MeasureType] = {}
        omitted_tables: list[OmittedTable] = []
        lane_gaps: list[KpiLaneGap] = []
        readers: list[ReaderRecord] = []
        for accession in sorted(set(accessions)):
            payload = self._payloads.get(accession)
            if payload is None:
                continue
            omitted_tables.extend(payload.omitted_tables)
            lane_gaps.extend(payload.lane_gaps)
            readers.extend(payload.readers)
            for row in payload.rows:
                if isinstance(row.value, ScaledQuantity):
                    units[row.observation_id] = row.value.measure
                if row.period is None:
                    unperiodized.append(row.model_dump_json())
                    continue
                if row.period.temporal_kind != "instant" and row.period.period_type != "annual":
                    continue
                period = (
                    DatePeriod(kind="date", date=row.period.period_end.isoformat())
                    if row.period.temporal_kind == "instant"
                    else FiscalYearPeriod(kind="fiscal_year", fiscal_year=row.period.fiscal_year)
                )
                tag = row.series.issuer_series_key
                if len(self._variants[(accession, tag)]) > 1:
                    tag += ";" + _series_variant(row)
                points.append((FilingLineRef(
                    accession=accession, period=period, statement_role="kpi", tag=tag,
                ), row))
        # Fiscal year selects an instant identity, not the calendar year in its date.
        selected = {
            line for line, row in points
            if row.period is not None and start_year <= row.period.fiscal_year <= window.end_fiscal_year
        }
        by_observation_id: dict[
            tuple[str, str], tuple[FilingLineObservation, TypedKpiObservation]
        ] = {}
        for line, row in points:
            if line not in selected:
                continue
            identity = (line.accession, row.observation_id)
            prior = by_observation_id.get(identity)
            if prior is not None:
                raise EdgarKpiIdentityCollision(
                    line, prior[1].locator.model_dump_json(), row.locator.model_dump_json(),
                )
            detail = row.model_dump_json()
            if isinstance(row.value, ScaledQuantity):
                fact = FilingLineFact(
                    line=line,
                    value=Quantity.model_validate({
                        "value": normalize_decimal_v1(row.value.canonical_value),
                        "display": kpi_quantity_display(row.value),
                    }, strict=True),
                    grade="filing_native",
                    provenance=VendorProvenance(
                        provider=EDGAR_PROVIDER_ID, native_endpoint="operational_kpis",
                        native_field=row.observation_id,
                    ),
                    label=row.metric_name,
                    source_ref=mint_source_fact_ref(line, "kpi"),
                )
                observation = FilingLineObservation(
                    line=line, status="ok", fact=fact, provider=EDGAR_PROVIDER_ID, detail=detail,
                )
            else:
                # A date or extraction gap is retained, not invented as a numeric fact.
                prefix = (
                    f"explicit_gap:{row.value.reason}:{row.value.detail};"
                    if isinstance(row.value, KpiGap) else "nonnumeric_date;"
                )
                observation = FilingLineObservation(
                    line=line, status="missing", fact=None,
                    provider=EDGAR_PROVIDER_ID, detail=prefix + detail,
                )
            by_observation_id[identity] = (observation, row)
        return EdgarKpiFetchResult(
            observations=tuple(
                observation for observation, _row in sorted(
                    by_observation_id.values(), key=lambda item: (
                        item[0].line.accession, item[0].line.period.canonical_label(), item[0].line.tag,
                    ),
                )
            ),
            unperiodized=tuple(sorted(unperiodized)),
            unit_by_observation_id=units,
            omitted_tables=tuple(omitted_tables),
            lane_gaps=tuple(lane_gaps),
            readers=tuple(readers),
        )


__all__ = [
    "EdgarKpiAdapter", "EdgarKpiFetchResult", "EdgarKpiIdentityCollision",
    "derive_four_quarter_annual_kpi_observations", "kpi_quantity_display",
]
