"""Human-readable workbook projection of the authoritative ProvenanceMap."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import replace
from typing import TYPE_CHECKING, Any

from .formatter import ModelFormatter
from .models import FormulaType, LineItem, operation_kind
from .renderer_columns import AbsoluteColumnMapper
from .renderer_payload import _index_to_col
from .renderer_summary import _unique_sheet_name
from .model_number_value import MissingReasonCode
from .renderer_types import CellFormat, CellWrite, RenderPlan, SheetSetup

if TYPE_CHECKING:
    from .finding_vocabulary import Declination
    from .build_model_product_contracts import CellLineage


def append_lineage_index(
    *,
    plan: RenderPlan,
    provenance: Any,
    model: Any,
    formatter: ModelFormatter,
    declinations: Mapping[str, Declination],
) -> None:
    """Append a presentation-only source and assumption index.

    Each row projects one ``CellLineage``: column D names its status (or, for a
    complete cell, the origin kinds it carries), columns E-K render every
    ref kind it holds and column L its ``OperationKind``. ``declinations`` is
    the ModelSpec's declinations by id, the same set the cells'
    ``declination_ids`` were drawn from.
    """

    sheet_name = _unique_sheet_name(
        "Sources & Assumptions",
        {setup.sheet for setup in plan.sheet_setups},
    )
    plan.sheet_setups.append(
        SheetSetup(
            sheet=sheet_name,
            column_widths={
                "A": 22.0,
                "B": 14.0,
                "C": 34.0,
                "D": 12.0,
                "E": 18.0,
                "F": 20.0,
                "G": 52.0,
                "H": 16.0,
                "I": 16.0,
                "J": 18.0,
                "K": 52.0,
                "L": 12.0,
            },
            freeze_panes="A3",
            first_data_column="A",
        )
    )
    items = {
        item.id: item
        for sheet in model.sheets.values()
        for section in sheet.sections
        for item in section.line_items
    }
    headers = (
        "Workbook location",
        "Fiscal period",
        "Model line",
        "Lineage type",
        "Issuer",
        "Provider",
        "Source",
        "As-of date",
        "Retrieved date",
        "Confidence",
        "Assumption rationale",
        "Kind",
    )
    plan.writes.append(
        CellWrite(
            sheet=sheet_name,
            cell="A1",
            value="Sources and Assumptions",
        )
    )
    for column, header in enumerate(headers, start=1):
        plan.writes.append(
            CellWrite(
                sheet=sheet_name,
                cell=f"{_index_to_col(column)}2",
                value=header,
            )
        )
    plan.formats.extend(
        (
            CellFormat(
                sheet=sheet_name,
                range="A1:L1",
                bold=True,
                bottom_border_color=formatter.section_header_border_color,
            ),
            CellFormat(
                sheet=sheet_name,
                range="A2:L2",
                bold=True,
                bottom_border_color=formatter.section_header_border_color,
            ),
        )
    )

    period_by_cell = {
        (entry.sheet, entry.cell): entry.period
        for entry in plan.semantic_cells
        if entry.period
    }
    # A provenance row for a cell the plan never wrote (an evidenced absence,
    # ``project_build_provenance``) has no semantic cell; its period is its
    # column on that sheet's period axis.
    planned_cells = {(entry.sheet, entry.cell) for entry in plan.semantic_cells}
    period_by_column: dict[str, dict[str, int]] = {}
    for lineage in provenance.cells:
        if (lineage.sheet, lineage.cell) in planned_cells:
            continue
        if lineage.sheet not in period_by_column:
            layout = model.sheets[lineage.sheet].layout
            mapper = AbsoluteColumnMapper(
                model.time_structure,
                layout.first_data_column if layout and layout.first_data_column else "B",
                period_scope=layout.period_scope if layout else "all",
            )
            period_by_column[lineage.sheet] = {
                mapper.col_for_period(period): int(period)
                for period in mapper.all_periods()
            }
        col_letter = lineage.cell.rstrip("0123456789")
        if col_letter in period_by_column[lineage.sheet]:
            period_by_cell[(lineage.sheet, lineage.cell)] = period_by_column[
                lineage.sheet
            ][col_letter]
    row = 3
    cells: Sequence[CellLineage] = provenance.cells
    for lineage in cells:
        location = f"{lineage.sheet}!{lineage.cell}"
        item = items.get(lineage.semantic_item_id)
        label = item.label if item is not None else "Model line"
        cell_period = period_by_cell.get((lineage.sheet, lineage.cell))
        period = f"FY{cell_period}" if cell_period else "Not applicable"
        evidence = lineage.evidence_refs
        assumptions = lineage.assumption_refs
        valuations = lineage.valuation_refs
        seeds = lineage.historical_driver_seed_refs
        covered_source_ids = {
            *(str(reference.source_id) for reference in evidence),
            *(str(reference.source_id) for reference in valuations),
            *(
                str(source_id)
                for reference in assumptions
                for source_id in reference.source_refs
            ),
        }
        cited_source_ids = tuple(
            str(source_id)
            for source_id in lineage.source_refs
            if str(source_id) not in covered_source_ids
        )
        match lineage.lineage_status:
            case "presentation":
                lineage_type = "Presentation"
            case "compiler_derived":
                assert lineage.compiler_origin is not None
                compiler_kind = lineage.compiler_origin.kind.replace("_", " ")
                lineage_type = f"Compiler derived — {compiler_kind}"
            case "source_absence_evidenced":
                lineage_type = "Source absence evidenced"
            case "business_model_node_unresolved":
                lineage_type = "Unresolved — business model node"
            case "missing_source":
                lineage_type = "Unresolved"
            case "complete":
                lineage_type = " + ".join(
                    kind
                    for kind, present in (
                        ("Source", evidence),
                        ("Market data", valuations),
                        ("Historical seed", seeds),
                        ("Assumption", assumptions),
                        ("Cited source", cited_source_ids),
                    )
                    if present
                )
                if (
                    lineage.formula is not None
                    and lineage.formula.type != FormulaType.constant
                ):
                    lineage_type = f"Formula over {lineage_type}"
            case missing_reason:
                reason_code: MissingReasonCode = missing_reason
                lineage_type = f"Unresolved — {reason_code.replace('_', ' ')}"
        rationale = _join(
            (
                *(reference.rationale for reference in assumptions),
                *(
                    line
                    for seed in seeds
                    for line in (
                        seed.derivation_rationale,
                        *(
                            f"Limitation: {limitation}"
                            for limitation in seed.limitations
                        ),
                    )
                ),
                *(
                    _declination_line(declinations[declination_id])
                    for declination_id in lineage.declination_ids
                ),
            )
        )
        values = (
            location,
            period,
            label,
            lineage_type,
            _join(reference.issuer for reference in evidence),
            _join(
                (
                    *(reference.provider for reference in evidence),
                    *(reference.provider for reference in valuations),
                )
            ),
            # Filing accession/section are not on EvidenceLineageReference today; they live in provenance.json / SourceEvidence.line.
            # A valuation ref's raw_ref locator and snapshot digest stay unrendered.
            _join(
                (
                    *(
                        f"{reference.source_id} · {reference.evidence_id}"
                        for reference in evidence
                    ),
                    *(
                        f"{reference.source_id} · {reference.observation_id}"
                        for reference in valuations
                    ),
                    *(
                        f"{seed.driver_key} {seed.period_id}: "
                        f"{seed.derivation_kind} of "
                        + ", ".join((*seed.fact_ids, *seed.evidence_ids))
                        for seed in seeds
                    ),
                    *cited_source_ids,
                )
            ),
            _join(
                (
                    *(reference.as_of_date.isoformat() for reference in evidence),
                    *(reference.as_of_close.isoformat() for reference in valuations),
                )
            ),
            _join(reference.retrieved_at.date().isoformat() for reference in evidence),
            _join(
                (
                    *(reference.confidence.title() for reference in evidence),
                    *(
                        (reference.confidence or "").title()
                        for reference in assumptions
                    ),
                    *(seed.confidence.title() for seed in seeds),
                )
            ),
            rationale
            or (
                "Add a source or document the analyst assumption and rebuild."
                if lineage.lineage_status == "missing_source"
                else ""
            ),
            _cell_kind(lineage, item, cell_period),
        )
        _write_row(plan, sheet_name, row, values)
        row += 1

    lineage_by_cell = {
        (cell.sheet, cell.cell): cell for cell in provenance.cells
    }
    plan.semantic_cells = [
        replace(
            entry,
            observation_ids=lineage_by_cell[(entry.sheet, entry.cell)].observation_ids,
            assumption_ids=lineage_by_cell[(entry.sheet, entry.cell)].assumption_ids,
            source_refs=tuple(lineage_by_cell[(entry.sheet, entry.cell)].source_refs),
            evidence_ids=tuple(
                reference.evidence_id
                for reference in lineage_by_cell[(entry.sheet, entry.cell)].evidence_refs
            ),
            compiler_origin=(
                lineage_by_cell[(entry.sheet, entry.cell)].compiler_origin.kind
                if lineage_by_cell[(entry.sheet, entry.cell)].compiler_origin
                is not None
                else None
            ),
        )
        if entry.cell_kind != "presentation"
        and (entry.sheet, entry.cell) in lineage_by_cell
        else entry
        for entry in plan.semantic_cells
    ]


def _write_row(
    plan: RenderPlan,
    sheet_name: str,
    row: int,
    values: tuple[Any, ...],
) -> None:
    for column, value in enumerate(values, start=1):
        if value is None or value == "":
            # An empty write reads back as an absent cell; writing it would
            # fabricate a readback mismatch without changing presentation.
            continue
        plan.writes.append(
            CellWrite(
                sheet=sheet_name,
                cell=f"{_index_to_col(column)}{row}",
                value=value,
            )
        )


def _cell_kind(
    lineage: CellLineage,
    item: LineItem | None,
    period: int | None,
) -> str:
    """The cell's operation kind: its value's provenance, else a formula's.

    A value written at the cell carries its kind on its ``ValueProvenance``;
    a formula cell is a derivation (a house parameter it reads is its own
    ``default`` cell). A presentation cell or one with neither is blank.
    """

    if lineage.lineage_status == "presentation":
        return ""
    if item is not None and item.values is not None and period is not None:
        cell = item.values.values.get(period)
        if cell is not None:
            return operation_kind(cell.provenance)
    if lineage.formula is not None:
        return "derivation"
    return ""


def _join(values: Any) -> str:
    return "\n".join(dict.fromkeys(str(value) for value in values if str(value)))


def _declination_line(declination: Declination) -> str:
    line = f"Declined {declination.code}: {declination.reason}"
    if declination.citation:
        return f"{line} ({declination.citation})"
    return line


__all__ = ["append_lineage_index"]
