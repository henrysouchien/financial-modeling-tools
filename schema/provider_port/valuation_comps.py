"""Provider-port valuation-comps snapshot construction.

This module is a pure provider read/transform boundary.  It has no model-state
write authority and is intentionally independent of the standalone model-engine
MCP surface.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import date
from decimal import Decimal
from typing import TYPE_CHECKING, Any

from value_semantics_core.forward_pe import (
    ForwardEpsEstimate,
    ForwardPeAvailable,
    derive_fy1_forward_pe,
)

from industry_slice_core.profile_normalization import ProviderCapability, ProviderError
from .contracts import (
    EstimateFact,
    FiscalWindow,
    MetricSeries,
    Quote,
)

if TYPE_CHECKING:
    from .registry import ProviderRegistry


_FORWARD_PE_QUANTUM = Decimal("0.000001")


def _default_registry(financials: dict | None) -> ProviderRegistry:
    from .adapters.fmp.adapter import FinancialsFetchTransport
    from .bootstrap import build_default_registry

    if financials is None:
        return build_default_registry()
    return build_default_registry(
        fmp_data=financials,
        fmp_transport=FinancialsFetchTransport(),
    )


def _resolve_best_effort(
    registry: ProviderRegistry,
    capability: ProviderCapability,
) -> Any | None:
    try:
        return registry.resolve(capability).implementation
    except (KeyError, ProviderError):
        return None


def _fetch_quotes(port: Any | None, symbols: Sequence[str]) -> dict[str, Quote]:
    if port is None:
        return {}
    try:
        return port.fetch_quotes(symbols)
    except ProviderError:
        return {}


def _fetch_estimates(
    port: Any | None,
    symbol: str,
) -> tuple[EstimateFact, ...]:
    if port is None:
        return ()
    try:
        return port.fetch_estimates(
            symbol,
            metrics=("eps_avg",),
            period="annual",
            limit=10,
        ).facts
    except ProviderError:
        return ()


def _fetch_metrics(
    port: Any | None,
    symbol: str,
    *,
    anchor_fiscal_year: int,
) -> tuple[MetricSeries, ...]:
    if port is None:
        return ()
    try:
        return port.fetch_metric_series_annual(
            symbol,
            metrics=("reported_pe_ratio", "peg_ratio", "ev_ebitda"),
            window=FiscalWindow(
                end_fiscal_year=anchor_fiscal_year,
                n_years=10,
            ),
        )
    except ProviderError:
        return ()


def _series_by_metric(series: Sequence[MetricSeries]) -> dict[str, MetricSeries]:
    return {item.metric_id: item for item in series}


def _latest_value(series: MetricSeries | None) -> float | None:
    if series is None or not series.points:
        return None
    return float(series.points[0].value)


def _positive_values(series: MetricSeries | None) -> list[float]:
    if series is None:
        return []
    return [float(point.value) for point in series.points if point.value > Decimal(0)]


def _median(values: Sequence[float]) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    midpoint = len(ordered) // 2
    if len(ordered) % 2:
        return ordered[midpoint]
    return (ordered[midpoint - 1] + ordered[midpoint]) / 2.0


def valuation_comp_entry(
    symbol: str,
    *,
    quote: Quote | None,
    estimates: Sequence[EstimateFact],
    metric_series: Sequence[MetricSeries],
    last_reported_period_end: date,
) -> dict[str, Any] | None:
    symbol_upper = symbol.upper()
    forward_result = derive_fy1_forward_pe(
        price=(
            quote.price
            if quote is not None and quote.ticker.upper() == symbol_upper
            else None
        ),
        estimates=tuple(
            ForwardEpsEstimate(
                period_end=fact.target_period_end,
                eps=fact.value,
                analyst_count=fact.analyst_count,
            )
            for fact in estimates
            if (
                fact.ticker.upper() == symbol_upper
                and fact.metric_id == "eps_avg"
                and fact.period == "annual"
                and fact.statistic == "mean"
            )
        ),
        last_reported_period_end=last_reported_period_end,
        quantum=_FORWARD_PE_QUANTUM,
    )
    forward_pe = (
        float(forward_result.multiple)
        if isinstance(forward_result, ForwardPeAvailable)
        else None
    )
    metrics = _series_by_metric(metric_series)
    trailing_values = _positive_values(metrics.get("reported_pe_ratio"))
    entry: dict[str, Any] = {
        "ticker": symbol_upper,
        "forward_pe": forward_pe,
        "peg": _latest_value(metrics.get("peg_ratio")),
        "ev_ebitda": _latest_value(metrics.get("ev_ebitda")),
        "trailing_low": min(trailing_values) if trailing_values else None,
        "trailing_median": _median(trailing_values),
        "trailing_high": max(trailing_values) if trailing_values else None,
        "source": "provider_port",
    }
    if any(
        entry.get(key) is not None
        for key in (
            "forward_pe",
            "peg",
            "ev_ebitda",
            "trailing_low",
            "trailing_median",
            "trailing_high",
        )
    ):
        return entry
    return None


def build_valuation_comps_snapshot(
    ticker: str,
    financials: dict | None = None,
    *,
    last_reported_period_end_by_symbol: Mapping[str, date],
    peers: list[str] | None = None,
    max_peers: int = 6,
    provider_registry: ProviderRegistry | None = None,
) -> dict[str, Any] | None:
    """Build a best-effort snapshot using canonical provider ports only."""

    ticker_upper = ticker.upper()
    registry = provider_registry or _default_registry(financials)
    peer_symbols: list[str]
    if peers is None:
        peer_port = _resolve_best_effort(
            registry,
            ProviderCapability.PEER_UNIVERSE,
        )
        try:
            peer_universe = (
                peer_port.fetch_peers(ticker_upper) if peer_port is not None else None
            )
        except ProviderError:
            peer_universe = None
        peer_symbols = (
            list(peer_universe.peers[:max_peers]) if peer_universe is not None else []
        )
    else:
        peer_symbols = []
        for raw_symbol in peers:
            if not isinstance(raw_symbol, str):
                continue
            symbol = raw_symbol.strip().upper()
            if not symbol or symbol == ticker_upper or symbol in peer_symbols:
                continue
            peer_symbols.append(symbol)
            if len(peer_symbols) >= max_peers:
                break

    symbols = [ticker_upper, *peer_symbols]
    fiscal_anchors: dict[str, date] = {}
    for raw_symbol, anchor in last_reported_period_end_by_symbol.items():
        symbol = raw_symbol.strip().upper()
        if not symbol:
            raise ValueError("fiscal-anchor symbol must be nonempty")
        existing = fiscal_anchors.get(symbol)
        if existing is not None and existing != anchor:
            raise ValueError(f"conflicting fiscal anchors for {symbol}")
        fiscal_anchors[symbol] = anchor
    missing_anchors = [symbol for symbol in symbols if symbol not in fiscal_anchors]
    if missing_anchors:
        raise ValueError(
            "missing last-reported period end for: " + ", ".join(missing_anchors)
        )

    quote_port = _resolve_best_effort(registry, ProviderCapability.QUOTE)
    estimates_port = _resolve_best_effort(registry, ProviderCapability.ESTIMATES)
    metrics_port = _resolve_best_effort(registry, ProviderCapability.METRIC_SERIES)
    quotes = _fetch_quotes(quote_port, symbols)

    entries: dict[str, dict[str, Any]] = {}
    for symbol in symbols:
        fiscal_anchor = fiscal_anchors[symbol]
        entry = valuation_comp_entry(
            symbol,
            quote=quotes.get(symbol),
            estimates=_fetch_estimates(
                estimates_port,
                symbol,
            ),
            metric_series=_fetch_metrics(
                metrics_port,
                symbol,
                anchor_fiscal_year=fiscal_anchor.year,
            ),
            last_reported_period_end=fiscal_anchor,
        )
        if entry is not None:
            entries[symbol] = entry

    target = entries.get(ticker_upper)
    if target is None:
        return None
    return {
        "source": "provider_port",
        "basis": "fy1",
        "target": target,
        "peers": [entries[symbol] for symbol in peer_symbols if symbol in entries],
    }


__all__ = [
    "build_valuation_comps_snapshot",
    "valuation_comp_entry",
]
