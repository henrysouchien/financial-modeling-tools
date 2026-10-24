"""The EDGAR operational-KPI v2 wire-to-domain adapter."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from decimal import Decimal
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
    KpiLocator,
    MeasureType,
    OmittedTable,
    ReaderRecord,
    ScaledQuantity,
    TypedKpiObservation,
    TypedPeriod,
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


_KPI_SCALE_FACTORS: Mapping[str, Decimal] = {
    "units": Decimal(1),
    "thousands": Decimal("1e3"),
    "millions": Decimal("1e6"),
    "billions": Decimal("1e9"),
}
_FOUR_QUARTER_SUM_UNITS = frozenset({"dollars", "count", "share_count"})


def _four_quarter_series_key(row: TypedKpiObservation) -> tuple[object, ...]:
    assert isinstance(row.value, ScaledQuantity)
    measure = row.value.measure
    return (
        row.series.issuer_series_key,
        tuple(sorted(row.dimensions.items())),
        measure.unit,
        measure.observation_basis,
        measure.currency,
        measure.counted_entity,
        row.value_semantic,
    )


def derive_four_quarter_annual_kpi_observations(
    rows: Sequence[TypedKpiObservation],
) -> tuple[TypedKpiObservation, ...]:
    """Derive annual Q1+Q2+Q3+Q4 KPI observations carrying all four citations.

    Governing ruling ``DURATION-KPI-FOUR-QUARTER-ANNUAL-SUM``: a duration KPI in
    dollars/count with all four quarters disclosed and no annual column derives
    the annual as ``Q1+Q2+Q3+Q4`` carrying all four citations.
    """
    annual_present: set[tuple[tuple[object, ...], int]] = set()
    by_series_year: dict[
        tuple[tuple[object, ...], int], dict[int, list[TypedKpiObservation]]
    ] = {}
    for row in rows:
        if (
            row.period is None
            or not isinstance(row.value, ScaledQuantity)
            or row.value_semantic != "level"
            or row.period.temporal_kind != "duration"
            or row.value.measure.unit not in _FOUR_QUARTER_SUM_UNITS
            or row.value.measure.observation_basis != "period_flow"
        ):
            continue
        key = (_four_quarter_series_key(row), row.period.fiscal_year)
        if row.period.period_type == "annual":
            annual_present.add(key)
        elif (
            row.calculation_ready
            and row.period.period_type == "quarterly"
            and row.period.quarter in (1, 2, 3, 4)
        ):
            by_series_year.setdefault(key, {}).setdefault(row.period.quarter, []).append(row)

    derived: list[TypedKpiObservation] = []
    for (series_key, fiscal_year), by_quarter in by_series_year.items():
        if (series_key, fiscal_year) in annual_present:
            continue
        if set(by_quarter) != {1, 2, 3, 4}:
            continue
        quarters: list[TypedKpiObservation] = []
        disagreed = False
        for q_num in (1, 2, 3, 4):
            candidates = by_quarter[q_num]
            first_q = candidates[0]
            assert isinstance(first_q.value, ScaledQuantity)
            first_val = Decimal(normalize_decimal_v1(first_q.value.canonical_value))
            if any(
                Decimal(normalize_decimal_v1(other.value.canonical_value)) != first_val
                for other in candidates[1:]
                if isinstance(other.value, ScaledQuantity)
            ):
                disagreed = True
                break
            quarters.append(first_q)
        if disagreed:
            continue
        q1, q2, q3, q4 = quarters
        assert isinstance(q1.value, ScaledQuantity) and isinstance(q4.value, ScaledQuantity)
        scales = {
            q.value.presentation_scale
            for q in quarters
            if isinstance(q.value, ScaledQuantity)
        }
        if len(scales) != 1:
            continue
        canonical_sum = sum(
            (
                Decimal(normalize_decimal_v1(q.value.canonical_value))
                for q in quarters
                if isinstance(q.value, ScaledQuantity)
            ),
            Decimal(0),
        )
        scale_factor = _KPI_SCALE_FACTORS.get(q4.value.presentation_scale, Decimal(1))
        scaled_sum = canonical_sum / scale_factor
        assert q1.period is not None and q4.period is not None
        derived.append(
            TypedKpiObservation(
                observation_id=",".join(q.observation_id for q in quarters),
                metric_name=q4.metric_name,
                series=q4.series,
                period=TypedPeriod(
                    fiscal_year=fiscal_year,
                    period_type="annual",
                    temporal_kind="duration",
                    period_start=q1.period.period_start,
                    period_end=q4.period.period_end,
                    quarter=None,
                    inferred=True,
                ),
                value=ScaledQuantity(
                    measure=q4.value.measure,
                    canonical_value=canonical_sum,
                    presentation_scale=q4.value.presentation_scale,
                    scale_provenance=q4.value.scale_provenance,
                    scale_evidence=q4.value.scale_evidence,
                ),
                value_raw=normalize_decimal_v1(scaled_sum),
                locator=KpiLocator(
                    accession=q1.locator.accession,
                    section=q1.locator.section,
                    table_id=q1.locator.table_id,
                    row=q1.locator.row,
                    column=q1.locator.column,
                    span=";".join(q.locator.span for q in quarters),
                ),
                additional_locators=(
                    q1.locator,
                    q2.locator,
                    q3.locator,
                    q4.locator,
                    *(loc for q in quarters for loc in q.additional_locators),
                ),
                dimensions=dict(q4.dimensions),
                value_semantic=q4.value_semantic,
                grounded=all(q.grounded for q in quarters),
                calculation_ready=True,
                lane=q4.lane,
                notes=(
                    *dict.fromkeys(note for q in quarters for note in q.notes),
                    "derived_annual:four_quarter_sum",
                ),
            )
        )
    return tuple(derived)


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
            for row in (*payload.rows, *derive_four_quarter_annual_kpi_observations(payload.rows)):
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
            for row in (*payload.rows, *derive_four_quarter_annual_kpi_observations(payload.rows)):
                if isinstance(row.value, ScaledQuantity):
                    units[row.observation_id] = row.value.measure
                if row.period is None:
                    unperiodized.append(row.model_dump_json())
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
