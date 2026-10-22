"""The EDGAR issuer non-GAAP bridge wire-to-domain adapter (S-XREPO-4)."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
from typing import Literal

from industry_slice_core.profile_normalization import ProviderCapability
from .wire.non_gaap_bridge_v2 import (
    IssuerNonGaapBridgeV2,
    NonGaapReconciliationLine,
    bridge_from_v1,
    bridge_rows,
    decode_issuer_non_gaap_bridge_any_bytes,
    metric_identifier,
)
from .wire.non_gaap_bridge_v3 import (
    BridgeOperand,
    EvidencedBlank,
    IssuerNonGaapBridgeV3,
    NonGaapBridgeComponent,
    ScaledQuantity,
)
from .wire.non_gaap_reconciliation_v1 import (
    IssuerNonGaapNetIncomeReconciliationV1,
    IssuerNonGaapReconciliationV1,
    VALUE_SCALE_FACTORS,
    is_usd_currency_label,
)
from schema.provider_port.contracts import (
    CanonicalBridgeComponent,
    CanonicalNonGaapBridge,
    FilingLineFact,
    FilingLineRef,
    ReportedPeriod,
    ReportedPeriodType,
    VendorProvenance,
    mint_source_fact_ref,
)
from schema.provider_port.registry import EDGAR_PROVIDER_ID
from value_semantics_core import (
    DatePeriod,
    FiscalQuarterPeriod,
    FiscalYearPeriod,
    NumericDisplaySpec,
    PeriodRef,
    Quantity,
    normalize_decimal_v1,
)

_CONDENSED_ROLES = frozenset(
    {"other", "pre_tax_adjustments_total", "income_tax", "income_tax_effect"}
)
_CONDENSED_METRICS = frozenset(
    {
        "non_gaap_operating_income",
        "non_gaap_income_from_operations",
        "non_gaap_net_income",
    }
)


def _infer_bridge_granularity(
    bridge: IssuerNonGaapBridgeV2,
) -> Literal["itemized", "condensed_summary"]:
    if bridge.output_metric_id in _CONDENSED_METRICS and all(
        comp.role in _CONDENSED_ROLES for comp in bridge.components
    ):
        return "condensed_summary"
    return "itemized"


def _line_fact(
    *,
    bridge: IssuerNonGaapBridgeV2,
    series_role: str,
    line: NonGaapReconciliationLine,
) -> FilingLineFact:
    if bridge.period.period_type == "FY":
        period: PeriodRef = FiscalYearPeriod(
            kind="fiscal_year",
            fiscal_year=bridge.period.fiscal_year,
        )
    elif bridge.period.period_type == "Q":
        q_val: Literal[1, 2, 3, 4] = (
            bridge.period.fiscal_quarter
            if bridge.period.fiscal_quarter in (1, 2, 3, 4)
            else 4
        )
        period = FiscalQuarterPeriod(
            kind="fiscal_quarter",
            fiscal_year=bridge.period.fiscal_year,
            quarter=q_val,
        )
    else:
        period = DatePeriod(
            kind="date",
            date=bridge.period.period_end.isoformat(),
        )
    ref = FilingLineRef(
        accession=bridge.source.accession,
        period=period,
        statement_role="nongaapreconciliation",
        tag=f"issuer:{series_role}",
    )
    result_key = line.locator.result_key
    coordinate = result_key.replace("[", "_").replace("]", "").replace(".", "_")
    form = bridge.source.form.lower().replace("-", "")
    source_ref = mint_source_fact_ref(
        ref, "non_gaap_bridge", suffix=f"edgar{form}.{coordinate}"
    )
    factor = VALUE_SCALE_FACTORS[bridge.value_scale]
    raw_dec = Decimal(line.value)
    raw_curr = (bridge.currency_label or "USD").strip().upper()
    currency = (
        "USD"
        if is_usd_currency_label(bridge.currency_label) or len(raw_curr) < 3
        else raw_curr[:3]
    )
    if bridge.metric_family == "per_share":
        canonical_val = normalize_decimal_v1(raw_dec)
        display = NumericDisplaySpec(
            numeric_kind="per_share",
            currency=currency,
        )
    elif bridge.metric_family == "margin_pct":
        canonical_val = normalize_decimal_v1(raw_dec)
        display = NumericDisplaySpec(numeric_kind="percentage")
    else:
        canonical_val = normalize_decimal_v1(raw_dec * factor)
        display = NumericDisplaySpec(
            numeric_kind="currency",
            scale="unit" if bridge.value_scale == "units" else bridge.value_scale,
            currency=currency,
            unit_label=currency,
        )
    return FilingLineFact(
        line=ref,
        value=Quantity.model_validate(
            {
                "value": canonical_val,
                "display": display,
            },
            strict=True,
        ),
        grade="filing_native",
        provenance=VendorProvenance(
            provider=EDGAR_PROVIDER_ID,
            native_endpoint="non_gaap_reconciliations",
            native_field=result_key,
        ),
        label=line.issuer_label,
        source_ref=source_ref,
    )


def _v3_periods(
    bridge: IssuerNonGaapBridgeV3,
) -> tuple[PeriodRef, ReportedPeriod]:
    q_map: dict[int, ReportedPeriodType] = {1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"}
    quarter = bridge.period.quarter or 4
    if bridge.period.period_type == "annual":
        period: PeriodRef = FiscalYearPeriod(
            kind="fiscal_year",
            fiscal_year=bridge.period.fiscal_year,
        )
        reported = ReportedPeriod(
            fiscal_year=bridge.period.fiscal_year,
            period_type="FY",
            period_end=bridge.period.period_end,
        )
    elif bridge.period.period_type == "quarterly":
        q_val: Literal[1, 2, 3, 4] = quarter if quarter in (1, 2, 3, 4) else 4
        period = FiscalQuarterPeriod(
            kind="fiscal_quarter",
            fiscal_year=bridge.period.fiscal_year,
            quarter=q_val,
        )
        reported = ReportedPeriod(
            fiscal_year=bridge.period.fiscal_year,
            period_type=q_map.get(quarter, "Q4"),
            period_end=bridge.period.period_end,
        )
    else:
        period = DatePeriod(
            kind="date",
            date=bridge.period.period_end.isoformat(),
        )
        reported = ReportedPeriod(
            fiscal_year=bridge.period.fiscal_year,
            period_type=q_map.get(quarter, "Q4"),
            period_end=bridge.period.period_end,
        )
    return period, reported


def _v3_line_fact(
    *,
    bridge: IssuerNonGaapBridgeV3,
    series_role: str,
    operand: BridgeOperand | NonGaapBridgeComponent,
    period: PeriodRef,
) -> FilingLineFact:
    if isinstance(operand.value, EvidencedBlank) or getattr(
        operand.value, "kind", None
    ) == "evidenced_blank":
        raise ValueError("an evidenced blank has no filing-line fact value")
    if not isinstance(operand.value, ScaledQuantity):
        raise TypeError(f"Unsupported v3 operand value: {type(operand.value)!r}")
    quantity_value = operand.value
    result_key = operand.cell.result_key
    ref = FilingLineRef(
        accession=bridge.source.accession,
        period=period,
        statement_role="nongaapreconciliation",
        tag=f"issuer:{series_role}",
    )
    coordinate = result_key.replace("[", "_").replace("]", "").replace(".", "_")
    form = bridge.source.form.lower().replace("-", "")
    source_ref = mint_source_fact_ref(
        ref, "non_gaap_bridge", suffix=f"edgar{form}.{coordinate}"
    )
    currency = quantity_value.measure.currency
    if currency is None:
        raw_curr = (bridge.currency_label or "USD").strip().upper()
        currency = (
            "USD"
            if is_usd_currency_label(bridge.currency_label) or len(raw_curr) < 3
            else raw_curr[:3]
        )
    canonical_val = normalize_decimal_v1(quantity_value.canonical_value)
    scale_map: dict[str, Literal["unit", "thousands", "millions", "billions"]] = {
        "units": "unit",
        "unit": "unit",
        "thousands": "thousands",
        "millions": "millions",
        "billions": "billions",
    }
    display_scale = scale_map.get(str(quantity_value.presentation_scale), "unit")
    if bridge.metric_family == "per_share" or quantity_value.measure.unit == "per_share":
        display = NumericDisplaySpec(
            numeric_kind="per_share",
            currency=currency,
        )
    elif bridge.metric_family == "margin_pct" or quantity_value.measure.unit == "percentage":
        display = NumericDisplaySpec(numeric_kind="percentage")
    else:
        display = NumericDisplaySpec(
            numeric_kind="currency",
            scale=display_scale,
            currency=currency,
            unit_label=currency,
        )
    return FilingLineFact(
        line=ref,
        value=Quantity.model_validate(
            {
                "value": canonical_val,
                "display": display,
            },
            strict=True,
        ),
        grade="filing_native",
        provenance=VendorProvenance(
            provider=EDGAR_PROVIDER_ID,
            native_endpoint="non_gaap_reconciliations",
            native_field=result_key,
        ),
        label=operand.issuer_label,
        source_ref=source_ref,
    )


def _canonical_bridge_v3(
    bridge: IssuerNonGaapBridgeV3,
) -> CanonicalNonGaapBridge:
    period, reported_period = _v3_periods(bridge)
    gaap_metric_id = metric_identifier(bridge.gaap_metric_label)
    components: list[CanonicalBridgeComponent] = []
    seen: dict[str, int] = {}
    for comp in bridge.components:
        role = comp.role.value
        seen[role] = seen.get(role, 0) + 1
        series_role = f"{bridge.output_metric_slug}__{role}"
        if seen[role] > 1:
            series_role = f"{series_role}__{seen[role]}"
        if isinstance(comp.value, EvidencedBlank):
            components.append(
                CanonicalBridgeComponent(
                    role=role,
                    issuer_label=comp.issuer_label,
                    sign=comp.sign,
                    fact=None,
                    evidenced_blank=comp.value.reason,
                )
            )
        else:
            components.append(
                CanonicalBridgeComponent(
                    role=role,
                    issuer_label=comp.issuer_label,
                    sign=comp.sign,
                    fact=_v3_line_fact(
                        bridge=bridge,
                        series_role=series_role,
                        operand=comp,
                        period=period,
                    ),
                )
            )
    gaap_base_fact = (
        None
        if isinstance(bridge.gaap_base.value, EvidencedBlank)
        else _v3_line_fact(
            bridge=bridge,
            series_role=gaap_metric_id,
            operand=bridge.gaap_base,
            period=period,
        )
    )
    gaap_base_blank = (
        bridge.gaap_base.value.reason
        if isinstance(bridge.gaap_base.value, EvidencedBlank)
        else None
    )
    subtotal_facts = tuple(
        None
        if isinstance(sub.value, EvidencedBlank)
        else _v3_line_fact(
            bridge=bridge,
            series_role=(
                f"{bridge.output_metric_slug}__subtotal_"
                f"{metric_identifier(sub.issuer_label)}"
            ),
            operand=sub,
            period=period,
        )
        for sub in bridge.subtotals
    )
    subtotal_blanks: tuple[Literal["dash"] | None, ...] = tuple(
        "dash" if isinstance(sub.value, EvidencedBlank) else None
        for sub in bridge.subtotals
    )
    disclosed_output_fact = (
        None
        if isinstance(bridge.disclosed_output.value, EvidencedBlank)
        else _v3_line_fact(
            bridge=bridge,
            series_role=bridge.output_metric_slug,
            operand=bridge.disclosed_output,
            period=period,
        )
    )
    disclosed_output_blank = (
        bridge.disclosed_output.value.reason
        if isinstance(bridge.disclosed_output.value, EvidencedBlank)
        else None
    )
    return CanonicalNonGaapBridge(
        bridge_id=bridge.bridge_id,
        canonical_output_metric=bridge.canonical_output_metric.value,
        output_metric_slug=bridge.output_metric_slug,
        output_metric_label=bridge.output_metric_label,
        gaap_metric_id=gaap_metric_id,
        gaap_metric_label=bridge.gaap_metric_label,
        period=reported_period,
        granularity=bridge.granularity,
        closure_state=bridge.closure.state,
        half_ulp_budget=Decimal(bridge.closure.half_ulp_budget),
        subtotal_half_ulp_budgets=tuple(
            Decimal(v) for v in bridge.closure.subtotal_half_ulp_budgets
        ),
        gaap_base=gaap_base_fact,
        components=tuple(components),
        subtotals=subtotal_facts,
        disclosed_output=disclosed_output_fact,
        recomputed_output=Decimal(bridge.recomputed_output),
        output_residual=Decimal(bridge.output_residual),
        subtotal_residuals=tuple(Decimal(v) for v in bridge.subtotal_residuals),
        gaap_base_blank=gaap_base_blank,
        subtotal_blanks=subtotal_blanks,
        disclosed_output_blank=disclosed_output_blank,
    )


class EdgarNonGaapBridgeAdapter:
    """Single provider-port adapter for EDGAR issuer non-GAAP bridges (NON_GAAP_BRIDGES)."""

    provider_name = EDGAR_PROVIDER_ID

    def __init__(
        self,
        *,
        bridges: Sequence[
            IssuerNonGaapBridgeV2
            | IssuerNonGaapBridgeV3
            | IssuerNonGaapReconciliationV1
            | IssuerNonGaapNetIncomeReconciliationV1
            | bytes
            | Mapping[str, object]
        ] = (),
    ) -> None:
        normalized: list[IssuerNonGaapBridgeV2 | IssuerNonGaapBridgeV3] = []
        for item in bridges:
            if isinstance(item, (IssuerNonGaapBridgeV2, IssuerNonGaapBridgeV3)):
                normalized.append(item)
            elif isinstance(
                item,
                (IssuerNonGaapReconciliationV1, IssuerNonGaapNetIncomeReconciliationV1),
            ):
                normalized.append(bridge_from_v1(item))
            elif isinstance(item, bytes):
                normalized.append(decode_issuer_non_gaap_bridge_any_bytes(item))
            elif isinstance(item, Mapping):
                if item.get("contract_version") == "issuer-non-gaap-bridge.v3":
                    normalized.append(IssuerNonGaapBridgeV3.model_validate(item))
                else:
                    normalized.append(IssuerNonGaapBridgeV2.model_validate(item))
            else:
                raise TypeError(f"Unsupported bridge payload type: {type(item)!r}")
        self._bridges: tuple[IssuerNonGaapBridgeV2 | IssuerNonGaapBridgeV3, ...] = tuple(normalized)

    def advertised_capabilities(self) -> frozenset[ProviderCapability]:
        return frozenset({ProviderCapability.NON_GAAP_BRIDGES})

    def fetch_bridges(
        self,
        ticker: str | None = None,
        *,
        accessions: Sequence[str] | None = None,
    ) -> tuple[CanonicalNonGaapBridge, ...]:
        wanted_ticker = ticker.strip().upper() if ticker else None
        wanted_accessions = set(accessions) if accessions is not None else None
        results: list[CanonicalNonGaapBridge] = []
        for bridge in self._bridges:
            if wanted_ticker and bridge.source.ticker.upper() != wanted_ticker:
                continue
            if wanted_accessions is not None and bridge.source.accession not in wanted_accessions:
                continue
            if isinstance(bridge, IssuerNonGaapBridgeV3):
                results.append(_canonical_bridge_v3(bridge))
                continue
            rows = bridge_rows(bridge)
            fact_by_line_id = {
                id(line): _line_fact(bridge=bridge, series_role=series_role, line=line)
                for series_role, _role, line in rows
            }
            q_map: dict[int, ReportedPeriodType] = {1: "Q1", 2: "Q2", 3: "Q3", 4: "Q4"}
            period_type: ReportedPeriodType = (
                "FY"
                if bridge.period.period_type == "FY"
                else q_map.get(bridge.period.fiscal_quarter or 4, "Q4")
            )
            reported_period = ReportedPeriod(
                fiscal_year=bridge.period.fiscal_year,
                period_type=period_type,
                period_end=bridge.period.period_end,
            )
            bridge_id = (
                f"{bridge.source.accession}:{bridge.output_metric_id}:"
                f"{bridge.period.period_type}{bridge.period.fiscal_year}"
            )
            results.append(
                CanonicalNonGaapBridge(
                    bridge_id=bridge_id,
                    canonical_output_metric=bridge.output_metric_id,
                    output_metric_slug=bridge.output_metric_id,
                    output_metric_label=bridge.output_metric_label,
                    gaap_metric_id=bridge.gaap_metric_id,
                    gaap_metric_label=bridge.gaap_metric_label,
                    period=reported_period,
                    granularity=_infer_bridge_granularity(bridge),
                    closure_state=bridge.closure_state,
                    half_ulp_budget=Decimal(bridge.half_ulp_budget),
                    subtotal_half_ulp_budgets=tuple(
                        Decimal(v) for v in bridge.subtotal_half_ulp_budgets
                    ),
                    gaap_base=fact_by_line_id[id(bridge.gaap_base)],
                    components=tuple(
                        CanonicalBridgeComponent(
                            role=comp.role,
                            issuer_label=comp.issuer_label,
                            sign=comp.sign,
                            fact=fact_by_line_id[id(comp)],
                        )
                        for comp in bridge.components
                    ),
                    subtotals=tuple(
                        fact_by_line_id[id(sub)] for sub in bridge.subtotals
                    ),
                    disclosed_output=fact_by_line_id[id(bridge.disclosed_output)],
                    recomputed_output=Decimal(bridge.recomputed_output),
                    output_residual=Decimal(bridge.output_residual),
                    subtotal_residuals=tuple(
                        Decimal(v) for v in bridge.subtotal_residuals
                    ),
                )
            )
        return tuple(results)


__all__ = ["EdgarNonGaapBridgeAdapter"]
