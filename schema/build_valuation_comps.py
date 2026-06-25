"""Valuation-comps fixed-cell population helpers for schema builds."""

from __future__ import annotations

import sys
from typing import Any, TypedDict

from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ValueCell,
    ValueProvenance,
    ValueSeries,
)
from .renderer import CellWrite, RenderPlan


class ValuationCompEntry(TypedDict, total=False):
    """One valuation-comps row consumed by build_model's fixed-cell populator.

    Required-by-convention field: ticker. Optional numeric fields:
    - forward_pe: FY1-forward P/E snapshot.
    - peg: current NTM PEG snapshot.
    - ev_ebitda: current EV/EBITDA snapshot, retained for downstream callers.
    - trailing_low/trailing_median/trailing_high: trailing-basis P/E range.
    """

    ticker: str
    forward_pe: float | int | str | None
    peg: float | int | str | None
    ev_ebitda: float | int | str | None
    trailing_low: float | int | str | None
    trailing_median: float | int | str | None
    trailing_high: float | int | str | None


class ValuationCompsPayload(TypedDict, total=False):
    """Payload contract for build-time valuation comps population.

    Shape:
      {
        "source": "peer_comparison" | "build_fallback" | str,
        "basis": "forward_ntm_fy1" | str,
        "target": ValuationCompEntry,
        "peers": [ValuationCompEntry, ...],
      }

    The primary production source is the caller's bridge from
    Thesis.industry_analysis.peer_comparison. schema/build.py only writes this
    payload into the template; it must not fetch from MCP or risk_module.
    """

    source: str
    basis: str
    target: ValuationCompEntry
    peers: list[ValuationCompEntry]


_VALUATION_COMP_PEER_ROLES = tuple(f"comp_{index}" for index in range(1, 7))
_VALUATION_COMP_ROLES = ("target", *_VALUATION_COMP_PEER_ROLES)
_VALUATION_COMP_PE_CLEAR_ROWS = {
    role: 9 + index
    for index, role in enumerate(_VALUATION_COMP_PEER_ROLES)
}
_VALUATION_COMP_PEG_CLEAR_ROWS = {
    role: 21 + index
    for index, role in enumerate(_VALUATION_COMP_PEER_ROLES)
}
_VALUATION_COMP_CLEAR_COLUMNS = ("B", "D", "E", "F")
_VALUATION_COMP_BLANK_FORMULA = '=""'
_VALUATION_COMP_RENDERED_VALUE_KEYS = (
    "trailing_low",
    "trailing_median",
    "trailing_high",
    "forward_pe",
    "peg",
    "peg_low",
    "peg_median",
    "peg_high",
)


def _parent_attr(name: str, fallback):
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    return getattr(parent, name, fallback) if parent is not None else fallback


def _coerce_optional_float_fallback(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _valuation_comp_periods(model: FinancialModel) -> list[int]:
    projection_periods = [int(period) for period in model.time_structure.projection_periods]
    if projection_periods:
        return projection_periods
    return [int(period) for period in model.time_structure.historical_periods[-1:]]


def _valuation_comps_provenance(source: str | None) -> ValueProvenance:
    normalized = str(source or "").strip().lower()
    if normalized in {"build_fallback", "fmp", "fmp_peer_comparison"}:
        return ValueProvenance.imported_fmp
    return ValueProvenance.imported_other


def _set_fixed_numeric_value(
    model: FinancialModel,
    item_id: str,
    value: float | None,
    *,
    periods: list[int],
    provenance: ValueProvenance,
    note: str | None,
) -> None:
    try:
        item_obj = model.get_item(item_id)
    except KeyError:
        return
    item_obj.values = None
    if value is None or not periods:
        return
    item_obj.values = ValueSeries()
    for period in periods:
        item_obj.values.values[int(period)] = ValueCell(
            period=int(period),
            value=float(value),
            provenance=provenance,
            note=note,
        )


def _set_fixed_text_formula(model: FinancialModel, item_id: str, value: str | None) -> None:
    try:
        item_obj = model.get_item(item_id)
    except KeyError:
        return
    item_obj.values = None
    item_obj.projected = FormulaSpec(
        type=FormulaType.constant,
        params={"value": str(value).upper() if value is not None else ""},
    )


def _clear_valuation_comp_peer_rows(model: FinancialModel) -> None:
    peer_roles = _parent_attr("_VALUATION_COMP_PEER_ROLES", _VALUATION_COMP_PEER_ROLES)
    set_fixed_text_formula = _parent_attr("_set_fixed_text_formula", _set_fixed_text_formula)
    for role in peer_roles:
        set_fixed_text_formula(model, f"tpl.s.comp_table_pe.{role}_ticker", None)
        set_fixed_text_formula(model, f"tpl.s.comp_table_peg.{role}_ticker", None)
        for table in ("comp_table_pe", "comp_table_peg"):
            for suffix in ("low", "high", "median"):
                try:
                    item_obj = model.get_item(f"tpl.s.{table}.{role}_{suffix}")
                except KeyError:
                    continue
                item_obj.label = ""
                item_obj.values = None


def _valuation_comp_entries(
    valuation_comps: dict[str, Any],
) -> list[tuple[str, dict[str, Any]]]:
    entries: list[tuple[str, dict[str, Any]]] = []
    target = valuation_comps.get("target")
    if isinstance(target, dict):
        entries.append(("target", target))
    peers = valuation_comps.get("peers")
    if isinstance(peers, list):
        peer_roles = _parent_attr("_VALUATION_COMP_PEER_ROLES", _VALUATION_COMP_PEER_ROLES)
        for role, peer in zip(peer_roles, peers):
            if isinstance(peer, dict):
                entries.append((role, peer))
    return entries


def _write_valuation_comp_row(
    model: FinancialModel,
    role: str,
    entry: dict[str, Any],
    *,
    periods: list[int],
    provenance: ValueProvenance,
    note: str | None,
) -> None:
    set_fixed_text_formula = _parent_attr("_set_fixed_text_formula", _set_fixed_text_formula)
    set_fixed_numeric_value = _parent_attr("_set_fixed_numeric_value", _set_fixed_numeric_value)
    coerce_optional_float = _parent_attr("_coerce_optional_float", _coerce_optional_float_fallback)

    ticker = entry.get("ticker")
    if role != "target" and isinstance(ticker, str) and ticker.strip():
        set_fixed_text_formula(model, f"tpl.s.comp_table_pe.{role}_ticker", ticker.strip())
        set_fixed_text_formula(model, f"tpl.s.comp_table_peg.{role}_ticker", ticker.strip())

    trailing_low = coerce_optional_float(entry.get("trailing_low"))
    trailing_median = coerce_optional_float(entry.get("trailing_median"))
    trailing_high = coerce_optional_float(entry.get("trailing_high"))
    forward_pe = coerce_optional_float(entry.get("forward_pe"))
    pe_values = {
        "low": trailing_low,
        "high": trailing_high,
        "median": trailing_median if trailing_median is not None else forward_pe,
    }
    for suffix, value in pe_values.items():
        set_fixed_numeric_value(
            model,
            f"tpl.s.comp_table_pe.{role}_{suffix}",
            value,
            periods=periods,
            provenance=provenance,
            note=note,
        )

    peg_value = coerce_optional_float(entry.get("peg"))
    explicit_peg_range = any(
        key in entry for key in ("peg_low", "peg_median", "peg_high")
    )
    if explicit_peg_range:
        peg_values = {
            "low": coerce_optional_float(entry.get("peg_low")),
            "high": coerce_optional_float(entry.get("peg_high")),
            "median": coerce_optional_float(entry.get("peg_median")) or peg_value,
        }
    else:
        # v1 payloads carry PEG as a current snapshot, not a constructed range.
        # Repeat the observed point across the evidence row so legacy PEG
        # cross-check formulas have an input while selected P/E stays blank.
        peg_values = {"low": peg_value, "high": peg_value, "median": peg_value}
    for suffix, value in peg_values.items():
        set_fixed_numeric_value(
            model,
            f"tpl.s.comp_table_peg.{role}_{suffix}",
            value,
            periods=periods,
            provenance=provenance,
            note=note,
        )


def populate_valuation_comps(
    model: FinancialModel,
    valuation_comps: ValuationCompsPayload | dict[str, Any] | None,
) -> None:
    """Populate Scenarios comp tables from a caller-supplied valuation_comps payload.

    The build boundary is intentionally narrow: callers pass peer-comparison
    data in, and this function only writes fixed cells. Primary production
    bridging from Thesis.industry_analysis.peer_comparison happens in the
    skill/agent layer, not inside schema/build.py.
    """

    if valuation_comps is None:
        return
    if not isinstance(valuation_comps, dict):
        raise ValueError("valuation_comps must be a dict payload when supplied")
    valuation_comp_entries = _parent_attr("_valuation_comp_entries", _valuation_comp_entries)
    entries = valuation_comp_entries(valuation_comps)
    if not entries:
        return
    if not model._index:
        model.build_index()
    valuation_comp_periods = _parent_attr("_valuation_comp_periods", _valuation_comp_periods)
    valuation_comps_provenance = _parent_attr(
        "_valuation_comps_provenance",
        _valuation_comps_provenance,
    )
    clear_valuation_comp_peer_rows = _parent_attr(
        "_clear_valuation_comp_peer_rows",
        _clear_valuation_comp_peer_rows,
    )
    write_valuation_comp_row = _parent_attr(
        "_write_valuation_comp_row",
        _write_valuation_comp_row,
    )
    periods = valuation_comp_periods(model)
    source = str(valuation_comps.get("source") or "").strip() or "valuation_comps"
    provenance = valuation_comps_provenance(source)
    note = f"valuation_comps source={source}"

    clear_valuation_comp_peer_rows(model)
    for role, entry in entries:
        write_valuation_comp_row(
            model,
            role,
            entry,
            periods=periods,
            provenance=provenance,
            note=note,
        )


def _valuation_comp_entry_has_rendered_payload(entry: dict[str, Any]) -> bool:
    ticker = str(entry.get("ticker") or "").strip()
    if ticker:
        return True
    coerce_optional_float = _parent_attr("_coerce_optional_float", _coerce_optional_float_fallback)
    rendered_value_keys = _parent_attr(
        "_VALUATION_COMP_RENDERED_VALUE_KEYS",
        _VALUATION_COMP_RENDERED_VALUE_KEYS,
    )
    return any(
        coerce_optional_float(entry.get(key)) is not None
        for key in rendered_value_keys
    )


def _append_valuation_comp_clear_writes(
    plan: RenderPlan,
    valuation_comps: ValuationCompsPayload | dict[str, Any] | None,
) -> None:
    if not isinstance(valuation_comps, dict):
        return
    valuation_comp_entries = _parent_attr("_valuation_comp_entries", _valuation_comp_entries)
    entries = valuation_comp_entries(valuation_comps)
    if not entries:
        return
    entry_has_rendered_payload = _parent_attr(
        "_valuation_comp_entry_has_rendered_payload",
        _valuation_comp_entry_has_rendered_payload,
    )
    peer_roles = _parent_attr("_VALUATION_COMP_PEER_ROLES", _VALUATION_COMP_PEER_ROLES)
    pe_clear_rows = _parent_attr("_VALUATION_COMP_PE_CLEAR_ROWS", _VALUATION_COMP_PE_CLEAR_ROWS)
    peg_clear_rows = _parent_attr("_VALUATION_COMP_PEG_CLEAR_ROWS", _VALUATION_COMP_PEG_CLEAR_ROWS)
    clear_columns = _parent_attr("_VALUATION_COMP_CLEAR_COLUMNS", _VALUATION_COMP_CLEAR_COLUMNS)
    blank_formula = _parent_attr("_VALUATION_COMP_BLANK_FORMULA", _VALUATION_COMP_BLANK_FORMULA)

    used_peer_roles = {
        role
        for role, entry in entries
        if role != "target" and entry_has_rendered_payload(entry)
    }
    for role in peer_roles:
        if role in used_peer_roles:
            continue
        for row in (
            pe_clear_rows[role],
            peg_clear_rows[role],
        ):
            for column in clear_columns:
                plan.writes.append(
                    CellWrite(
                        sheet="Scenarios",
                        cell=f"{column}{row}",
                        value=blank_formula,
                    )
                )


__all__ = [
    "ValuationCompEntry",
    "ValuationCompsPayload",
    "_VALUATION_COMP_BLANK_FORMULA",
    "_VALUATION_COMP_CLEAR_COLUMNS",
    "_VALUATION_COMP_PEER_ROLES",
    "_VALUATION_COMP_PE_CLEAR_ROWS",
    "_VALUATION_COMP_PEG_CLEAR_ROWS",
    "_VALUATION_COMP_RENDERED_VALUE_KEYS",
    "_VALUATION_COMP_ROLES",
    "_append_valuation_comp_clear_writes",
    "_clear_valuation_comp_peer_rows",
    "_set_fixed_numeric_value",
    "_set_fixed_text_formula",
    "_valuation_comp_entries",
    "_valuation_comp_entry_has_rendered_payload",
    "_valuation_comp_periods",
    "_valuation_comps_provenance",
    "_write_valuation_comp_row",
    "populate_valuation_comps",
]
