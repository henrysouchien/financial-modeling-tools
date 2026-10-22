"""Value and override writer helpers for schema build population."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any, Literal, cast

from .models import (
    EdgarProvenance,
    FormulaDerivation,
    FormulaDerivationStrategy,
    FormulaSpec,
    FormulaType,
    LineItem,
    ValueCell,
    ValueProvenance,
    ValueSeries,
    VendorProvenance,
)


class CellSourceStamp(dict[str, Any]):
    """Source identity recorded alongside a cell value or formula."""

    def copy(self) -> CellSourceStamp:
        return CellSourceStamp(super().copy())

    @property
    def cell_state(self) -> str:
        return str(self.get("cell") or "input")

    @property
    def origin(self) -> str | None:
        return self.get("origin")

    @property
    def source_dict(self) -> dict[str, Any]:
        source = self.get("source")
        return cast(dict[str, Any], source) if isinstance(source, Mapping) else {}

    @property
    def observation_ids(self) -> tuple[str, ...]:
        ids = list(self.get("observation_ids") or ())
        source = self.source_dict
        if source.get("observation_id"):
            ids.append(source["observation_id"])
        line = source.get("line")
        if isinstance(line, Mapping) and line.get("observation_id"):
            ids.append(line["observation_id"])
        for disclosure in source.get("schedule_disclosures") or ():
            if isinstance(disclosure, Mapping) and disclosure.get("fact_id"):
                ids.append(disclosure["fact_id"])
        return tuple(dict.fromkeys(str(value) for value in ids))

    @property
    def assumption_ids(self) -> tuple[str, ...]:
        ids = list(self.get("assumption_ids") or ())
        source = self.source_dict
        row = source.get("row")
        if source.get("kind") == "judgment_row" and isinstance(row, Mapping) and row.get("assumption_id"):
            ids.append(row["assumption_id"])
        return tuple(dict.fromkeys(str(value) for value in ids))

    @property
    def source_refs(self) -> tuple[str, ...]:
        source = self.source_dict
        refs = [
            *(self.get("source_refs") or ()),
            *(source.get("source_refs") or ()),
        ]
        if source.get("source_id"):
            refs.append(source["source_id"])
        for disclosure in source.get("schedule_disclosures") or ():
            if isinstance(disclosure, Mapping) and disclosure.get("source_id"):
                refs.append(disclosure["source_id"])
        return tuple(dict.fromkeys(str(value) for value in refs))

    @property
    def evidence_refs(self) -> tuple[Any, ...]:
        return tuple(self.get("evidence_refs") or self.source_dict.get("evidence_refs") or ())

    @property
    def assumption_refs(self) -> tuple[Any, ...]:
        return tuple(self.get("assumption_refs") or self.source_dict.get("assumption_refs") or ())

    @property
    def valuation_refs(self) -> tuple[Any, ...]:
        return tuple(self.get("valuation_refs") or self.source_dict.get("valuation_refs") or ())

    @property
    def historical_driver_seed_refs(self) -> tuple[Any, ...]:
        return tuple(
            self.get("historical_driver_seed_refs")
            or self.source_dict.get("historical_driver_seed_refs")
            or ()
        )

    @property
    def compiler_origin(
        self,
    ) -> Literal[
        "discount_convention",
        "scenario_control",
        "mechanical_constant",
        "presentation_scaffold_value",
    ] | None:
        kind = self.get("compiler_origin") or self.source_dict.get("compiler_origin")
        if kind in (
            "discount_convention",
            "scenario_control",
            "mechanical_constant",
            "presentation_scaffold_value",
        ):
            return kind
        return None

    @property
    def absence_citation(self) -> Mapping[str, Any] | None:
        citation = self.get("absence_citation") or self.source_dict.get("citation")
        return citation if isinstance(citation, Mapping) else None

    @property
    def included_in_role(self) -> str | None:
        return self.get("included_in") or self.source_dict.get("included_in")

    @property
    def missing_reason(self) -> str | None:
        return self.get("reason")

    @property
    def declination_ids(self) -> tuple[str, ...]:
        return tuple(str(value) for value in (self.get("declination_ids") or ()))


def stamp_cell_source(
    mapping: Mapping[str, Any] | None = None, /, **fields: Any
) -> CellSourceStamp:
    return CellSourceStamp({**dict(mapping or {}), **fields})


def _set_imported_value(
    item: LineItem,
    year: int,
    value: float,
    provenance: ValueProvenance = ValueProvenance.imported_fmp,
    *,
    edgar_provenance: EdgarProvenance | None = None,
    vendor_provenance: VendorProvenance | None = None,
    line_ref: Any = None,
) -> None:
    if item.values is None:
        item.values = ValueSeries()
    item.values.values[int(year)] = ValueCell(
        period=int(year),
        value=float(value),
        provenance=provenance,
        edgar_provenance=edgar_provenance,
        vendor_provenance=vendor_provenance,
        line_ref=line_ref,
    )

def _set_constant_override(
    item: LineItem,
    year: int,
    value: float,
    *,
    synthetic: bool = False,
    edgar_provenance: EdgarProvenance | None = None,
    vendor_provenance: VendorProvenance | None = None,
) -> None:
    if item.overrides is None:
        item.overrides = {}
    item.overrides[int(year)] = FormulaSpec(
        type=FormulaType.constant,
        params={"value": float(value) if isinstance(value, int) else value},
        note="synthetic" if synthetic else None,
        edgar_provenance=edgar_provenance,
        vendor_provenance=vendor_provenance,
    )

def _authored_periods(item: LineItem, periods: Iterable[int]) -> tuple[int, ...]:
    """The ``periods`` whose cell an authored row lowered (``input``)."""

    if item.values is None:
        return ()
    return tuple(
        int(period)
        for period in periods
        if (cell := item.values.values.get(int(period))) is not None
        and cell.provenance == ValueProvenance.input
    )


def _write_default_unless_authored(
    item: LineItem,
    periods: Iterable[int],
    value: float,
    *,
    owner: Literal[
        "build_non_gaap_addbacks",
        "build_company_grounded_schedules",
        "build_scenarios",
    ],
    strategy: FormulaDerivationStrategy,
    dependencies: Iterable[str] = (),
    lineage_by_period: Mapping[int, Mapping[str, Any]] | None = None,
) -> tuple[int, ...]:
    """Write a compiler house value as ``default`` where no authored row did.

    A period an authored FAS / ValuationPolicy row lowered keeps its
    ``input`` cell. Every other period gets ``ValueCell(provenance=default,
    note=strategy)`` - the cell ``judgment_absent_default_used`` reports - and
    the constant override an authored value lands with. The override carries
    its ``FormulaDerivation`` (``owner``, ``strategy``), the provenance entry
    that names the house convention, and the period's ``lineage`` (how the
    house value was computed) when one is given. Returns the periods written.
    """

    requested = tuple(int(period) for period in periods)
    authored = set(_authored_periods(item, requested))
    written = tuple(period for period in requested if period not in authored)
    if not written:
        return ()
    derivation = FormulaDerivation(
        owner=owner,
        strategy=strategy,
        dependencies=tuple(dict.fromkeys(dependencies)),
    )
    if item.values is None:
        item.values = ValueSeries()
    if item.overrides is None:
        item.overrides = {}
    for period in written:
        source: dict[str, Any] = {
            "kind": "construction_helper",
            "helper": owner,
            "owner": owner,
            "note": strategy,
            "dependencies": list(derivation.dependencies),
        }
        if lineage_by_period is not None and period in lineage_by_period:
            source["lineage"] = dict(lineage_by_period[period])
        stamp = stamp_cell_source({
            "cell": "input",
            "origin": "derived",
            "compiler_origin": "mechanical_constant",
            "source": source,
        })
        item.values.values[period] = ValueCell(
            period=period,
            value=float(value),
            provenance=ValueProvenance.default,
            note=strategy,
            lineage_reference=stamp,
        )
        params: dict[str, Any] = {"value": float(value)}
        if lineage_by_period is not None and period in lineage_by_period:
            params["lineage"] = dict(lineage_by_period[period])
        item.overrides[period] = FormulaSpec(
            type=FormulaType.constant,
            params=params,
            note=strategy,
            derivation=derivation,
        )
    return written


__all__ = [
    "CellSourceStamp",
    "stamp_cell_source",
    "_authored_periods",
    "_set_constant_override",
    "_set_imported_value",
    "_write_default_unless_authored",
]
