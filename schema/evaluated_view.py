"""The in-turn evaluated view: the bounded, typed readback of the live sheet.

``build_evaluated_view`` projects one recalculated ``FinancialModel`` into at most ``limit`` rows
of the document (the root-cause items, the cells a refresh changed, the check rows, the DCF
anchor), a markdown table of them, a tie-out of the key statement rows against the pinned
reported values, the root causes (``schema.root_causes.collect_root_causes``) and the stage
coverage. It is pure: every input is a value the caller already holds, and nothing is read or
written. ``model_build.document`` is imported where a function needs it (``value_diff``), as
``schema/build.py`` does, because ``model_build`` imports ``schema``.

Cell states are the graph's typed states (``DependencyGraph.compute_typed``), read from the
document's recalc (``values``), which is what the sheet evaluates; a cell the recalc does not
carry falls back to ``typed``.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from decimal import Decimal
import re
from typing import TYPE_CHECKING, Any, Literal, TypeAlias, cast

from pydantic import BaseModel, ConfigDict, Field, model_validator

from .analysis import DCF_PRICE_ITEM_ID
from .build_check_projection import (
    CHECK_KEY_ITEMS,
    ReconciliationBasis,
    _bridge_stages,
    project_check_rows,
)
from .models import FinancialModel, LineItem, ModelScale, Unit
from .renderer_columns import item_owns_cell
from .stage_registry import Stage

if TYPE_CHECKING:
    from model_build import document

    from .build_model_product_contracts import HistoricalBasisFact, SourceBundle
    from .dependency_graph import DependencyGraph, DependencyGraphNumberValue
    from .finding_vocabulary import Finding
    from .role_disposition import RoleDispositionTable
    from .stable_control_identity import CellKey, FiscalAxis

CellState = Literal[
    "known",
    "missing",
    "unsupported",
    "invalid",
    "retired",
    "not_applicable",
    "not_separately_presented",
]

RootOwner: TypeAlias = Stage | Literal["model_document", "engine"]

#: What a root's ``repair_call`` is: a persist door of the stage that owns the root (``door``), an
#: edit of the working document through ``model_document`` (``model_document``), or the report of
#: a defect the engine owns (``report``). Derived from the call's tool (``RootCause`` validator),
#: so the door layer renders ``next_calls`` from the schema's routing and never re-derives it.
RepairKind = Literal["door", "model_document", "report"]

StageCoverage = Literal["persisted", "absent", "stale"]

EVALUATED_VIEW_ROW_LIMIT = 25
#: The cap on roots no blocking finding backs (``collect_root_causes``): a measurement of how many
#: the answer names, never a refusal. A root that explains a blocking finding is always carried.
ROOT_CAUSE_LIMIT = 5

#: ``abs(total_assets - total_liabilities_and_equity)`` within which the sheet balances (model scale).
BALANCE_TOLERANCE = 0.5
#: Half a unit of the filed table's reporting precision, by the row's unit: a per-share row
#: ties within half a cent, every other key row within half a model unit.
_TIE_TOLERANCE = 0.5
_TIE_TOLERANCE_PER_SHARE = 0.005
_TIE_OUT_ROW_LIMIT = 9

_TOTAL_ASSETS = "tpl.fm.balance_sheet.total_assets"
_TOTAL_LIABILITIES_AND_EQUITY = "tpl.fm.balance_sheet.total_liabilities_and_equity"
_PERIOD_ID_YEAR = re.compile(r"^FY(\d{4})")
_MISSING_TEXT = "—"


class _View(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class EvaluatedRow(_View):
    item_id: str
    label: str
    #: ``fm.income_statement`` | ``a.capital_sources`` | ``v.dcf`` | ``bm.<segment>`` ...
    section: str
    unit: Unit
    model_scale: ModelScale
    #: Keyed by period id (``FY2024A`` / ``FY2026E``).
    values: dict[str, float | None]
    states: dict[str, CellState]
    #: Touched by this refresh against the previous revision.
    changed: bool
    #: One line: the root cause of the first non-known cell, if any.
    why: str | None = None


class TieOutRow(_View):
    item_id: str
    period: str
    rendered: float | None
    reported: float | None
    delta: float | None
    basis: ReconciliationBasis
    status: Literal["ties", "off", "not_in_pin", "no_history"]


class TieOutSummary(_View):
    #: ``CHECK_KEY_ITEMS`` x historical periods, off rows first.
    rows: tuple[TieOutRow, ...]
    ties: int
    off: int
    not_in_pin: int
    #: Per period id; ``None`` until both sides are known.
    balance_sheet_balances: dict[str, bool | None]


_REPAIR_KIND_BY_TOOL: dict[Any, RepairKind] = {
    "model_document": "model_document",
    "fms_report_build_model": "report",
}


class ValidationSubReason(_View):
    loc: tuple[str | int, ...] = ()
    msg: str
    type: str


class RootCause(_View):
    root_item_id: str
    period: int
    #: A typed ``primary_reason`` or a finding code: ``source_value_unavailable``,
    #: ``stage_not_persisted:historical_foundation``, ``manual_input_uncited`` ...
    reason: str
    #: Downstream non-known cells attributed to this root.
    cascade_count: int
    owner: RootOwner
    #: ``{"tool": "...", "arguments": {...}}``.
    repair_call: dict[str, Any] | None
    #: What ``repair_call`` is (``RepairKind``). Never passed by a producer: it is read off the call's
    #: tool, so a root's routing is one fact. ``model_document`` edits the working document;
    #: ``fms_report_build_model`` reports a defect the engine owns; every other tool is a stage door.
    repair_kind: RepairKind = "door"
    field: str | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    got: tuple[ValidationSubReason, ...] | str | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    fix: str | None = Field(
        default=None, exclude_if=lambda value: value is None
    )
    @model_validator(mode="before")
    @classmethod
    def _repair_kind_from_call(cls, data: Any) -> Any:
        if not isinstance(data, dict) or "repair_kind" in data:
            return data
        call = data.get("repair_call")
        tool = call.get("tool") if isinstance(call, dict) else None
        return {**data, "repair_kind": _REPAIR_KIND_BY_TOOL.get(tool, "door")}


class EvaluatedView(_View):
    schema_version: Literal["evaluated_view.v1"] = "evaluated_view.v1"
    #: ``binding.revision`` after the refresh.
    revision: str
    #: Axis period ids, historical then forecast.
    columns: tuple[str, ...]
    rows: tuple[EvaluatedRow, ...]
    #: Rows x columns, values formatted by unit and scale, ``—`` for a value the cell lacks.
    markdown_table: str
    tie_out_summary: TieOutSummary
    root_causes: tuple[RootCause, ...]
    #: Roots the check found in all (``collect_root_causes_counted``): ``root_causes`` carries every
    #: root backed by a blocking finding and at most ``ROOT_CAUSE_LIMIT`` of the rest, so the cap is
    #: a measurement, ``root_causes_total - len(root_causes)`` roots left unnamed. ``None``: not measured.
    root_causes_total: int | None = None
    #: Line items of the document not shown in ``rows``.
    unchanged_row_count: int
    changed_cell_count: int
    stage_coverage: dict[str, StageCoverage]
    wall_seconds: float

    def changed_rows_only(self) -> EvaluatedView:
        """Project changed rows and their markdown without altering evaluation metadata."""
        rows = tuple(row for row in self.rows if row.changed)
        return self.model_copy(update={
            "rows": rows,
            "markdown_table": _markdown(self.columns, rows),
            "unchanged_row_count": self.unchanged_row_count + len(self.rows) - len(rows),
        })


# --------------------------------------------------------------------------
# axis and sheet order
# --------------------------------------------------------------------------


def _axis(model: FinancialModel) -> tuple[tuple[int, ...], tuple[int, ...]]:
    """``(historical, projection)`` years of the model's time axis."""
    time_structure = model.time_structure
    historical = time_structure.historical_periods or time_structure.historical_years
    projection = time_structure.projection_periods or time_structure.projection_years
    return tuple(int(year) for year in historical), tuple(int(year) for year in projection)


def period_ids(model: FinancialModel) -> tuple[dict[int, str], tuple[int, ...], tuple[int, ...]]:
    """``({year: "FY2024A" | "FY2026E"}, historical, projection)`` for the model's axis."""
    historical, projection = _axis(model)
    ids = {year: f"FY{year}A" for year in historical}
    ids.update({year: f"FY{year}E" for year in projection})
    return ids, historical, projection


def _indexed(model: FinancialModel) -> dict[str, LineItem]:
    """The model's line-item index, built on first use as ``FinancialModel.get_item`` does."""
    if not model._index:
        model.build_index()
    return model._index


def _sheet_items(model: FinancialModel) -> list[LineItem]:
    return [
        item
        for sheet in model.sheets.values()
        for section in sheet.sections
        for item in section.line_items
    ]


def _section_of(item_id: str) -> str:
    """``tpl.fm.income_statement.total_revenue`` -> ``fm.income_statement``; ``bm.<segment>.<node>`` -> ``bm.<segment>``."""
    parts = item_id.split(".")
    if parts[0] == "tpl":
        parts = parts[1:]
    return ".".join(parts[:-1]) if len(parts) > 1 else item_id


# --------------------------------------------------------------------------
# dispositions as cell states
# --------------------------------------------------------------------------


def disposition_cell_states(
    dispositions: "RoleDispositionTable | None", model: FinancialModel
) -> dict[CellKey, CellState]:
    """The cells a role disposition table declares empty on purpose, by view state.

    ``not_applicable`` (a cited absence, an ``optional_if_unreported`` role nothing declares, or a period
    the agent declared ``not_disclosed_in_vintage`` on a stored ``unbound`` row, never inferred) and
    ``unowed`` read ``not_applicable``; ``not_separately_presented`` reads itself. A role
    maps to its lines (``role_items``) and a period id (``FY2025``) to its fiscal year; lines the
    model does not carry are skipped.
    """
    if dispositions is None:
        return {}
    from .required_role_catalog import load_required_role_catalog
    from .role_disposition import items_by_role

    by_role = items_by_role(load_required_role_catalog(), ticker=model.company.ticker)
    states: dict[CellKey, CellState] = {}
    for row in dispositions.rows:
        state: CellState
        if row.state == "not_separately_presented":
            state = "not_separately_presented"
        elif row.state in ("not_applicable", "unowed") or row.optional_unreported or row.not_disclosed_in_vintage:
            state = "not_applicable"
        else:
            continue
        match = _PERIOD_ID_YEAR.match(row.period_id)
        role = by_role.get(row.required_role)
        if match is None or role is None:
            continue
        for item_id in role.all_items():
            if item_id in _indexed(model):
                states[(item_id, int(match.group(1)))] = state
    return states


# --------------------------------------------------------------------------
# cell state
# --------------------------------------------------------------------------


def _state_of(
    model: FinancialModel,
    item: LineItem,
    year: int,
    value: "DependencyGraphNumberValue | None",
    *,
    graph: "DependencyGraph",
    disposed: Mapping[CellKey, CellState],
) -> CellState:
    if item.column is not None and not item_owns_cell(model, item, year):
        # A fixed-cell row is one cell, at its anchor period: every other period is no cell of it.
        return "not_applicable"
    if value is None:
        # The graph carries no node-period for it: the cell was retired.
        return "retired"
    if value.state == "known":
        return "known"
    declared = disposed.get((item.id, year))
    if declared is not None:
        return declared
    # The cell's own declared absence (``mark_unavailable(reason="not_disclosed_in_vintage")``): the
    # typed cause names this very cell, so no role table is needed (an ``operating.*`` role has no
    # row in it); a dependent carries the origin's node id and keeps its own state.
    if value.state == "missing" and all(
        cause.node_id == item.id and cause.reason_code == "not_disclosed_in_vintage" for cause in value.causes
    ):
        return "not_applicable"
    if value.state == "evidenced_absent":
        if value.primary_reason == "not_separately_presented":
            return "not_separately_presented"
        # A vintage absence flows as the graph's typed state, but the view
        # makes the own-node distinction it made for the missing spelling:
        # only the cell the filing does not print reads ``not_applicable``;
        # a dependent that carries its cause stays ``missing``.
        if value.primary_reason == "not_disclosed_in_vintage" and not all(
            cause.node_id == item.id for cause in value.causes
        ):
            return "missing"
        return "not_applicable"
    return value.state


# --------------------------------------------------------------------------
# formatting
# --------------------------------------------------------------------------


def _format_number(value: float | None, unit: Unit | None, scale: ModelScale) -> str:
    """One cell of the markdown table.

    Percentages are fractions shown as percent; a ``units``-scale dollar or count at or above a
    million shows as ``M``/``B``; every other number shows with the fewest digits that keep three
    significant ones.
    """
    if value is None:
        return _MISSING_TEXT
    if unit is Unit.percentage:
        return f"{value * 100:.1f}%"
    if unit is Unit.multiple:
        return f"{value:.1f}x"
    if unit is Unit.ratio:
        return f"{value:.2f}"
    if unit in (Unit.per_share, Unit.per_count, Unit.per_day):
        return f"{value:.2f}"
    if unit is Unit.days:
        return f"{value:.0f}"
    magnitude = abs(value)
    if scale == "units" and unit in (Unit.dollars, Unit.share_count, Unit.item_count, None):
        if magnitude >= 1e9:
            return f"{value / 1e9:.1f}B"
        if magnitude >= 1e6:
            return f"{value / 1e6:.0f}M"
    if magnitude >= 100:
        return f"{value:.0f}"
    if magnitude >= 1:
        return f"{value:.1f}"
    return f"{value:.2f}"


def _label(label: str) -> str:
    return label.strip().replace("|", "\\|").replace("\n", " ")


def _markdown(columns: Sequence[str], rows: Sequence[EvaluatedRow]) -> str:
    lines = [
        "|item|" + "|".join(columns) + "|",
        "|---|" + "|".join("---" for _ in columns) + "|",
    ]
    for row in rows:
        cells = [
            _format_number(row.values.get(column), row.unit, row.model_scale)
            if row.states.get(column) == "known"
            else _MISSING_TEXT
            for column in columns
        ]
        lines.append(f"|{_label(row.label)}|" + "|".join(cells) + "|")
    return "\n".join(lines)


# --------------------------------------------------------------------------
# tie-out
# --------------------------------------------------------------------------


def _known(values: Mapping[CellKey, "DependencyGraphNumberValue"], item_id: str, year: int) -> float | None:
    value = values.get((item_id, year))
    return value.value if value is not None and value.state == "known" else None


def _balance_sheet_balances(
    values: Mapping[CellKey, "DependencyGraphNumberValue"], ids: Mapping[int, str]
) -> dict[str, bool | None]:
    balances: dict[str, bool | None] = {}
    for year, period_id in ids.items():
        assets = _known(values, _TOTAL_ASSETS, year)
        funding = _known(values, _TOTAL_LIABILITIES_AND_EQUITY, year)
        balances[period_id] = (
            None if assets is None or funding is None else abs(assets - funding) <= BALANCE_TOLERANCE
        )
    return balances


def _tie_status(
    rendered: float | None, reported: float | None, item: LineItem | None, basis: ReconciliationBasis
) -> Literal["ties", "off", "not_in_pin", "no_history"]:
    if reported is None:
        return "not_in_pin" if basis == "not_in_pin" else "no_history"
    if rendered is None:
        return "off"
    tolerance = _TIE_TOLERANCE_PER_SHARE if item is not None and item.unit is Unit.per_share else _TIE_TOLERANCE
    return "ties" if abs(rendered - reported) <= tolerance else "off"


def _fiscal_axis(model: FinancialModel) -> "FiscalAxis":
    from .stable_control_identity import FiscalAxis, FiscalPeriod

    historical, projection = _axis(model)
    return FiscalAxis(
        historical_periods=tuple(
            FiscalPeriod(period_id=f"FY{year}", fiscal_year=year, period_type="annual", is_estimate=False)
            for year in historical
        ),
        forecast_periods=tuple(
            FiscalPeriod(period_id=f"FY{year}", fiscal_year=year, period_type="annual", is_estimate=True)
            for year in projection
        ),
    )


def _hff_reported(
    facts: Sequence["HistoricalBasisFact"], model: FinancialModel
) -> dict[CellKey, Decimal]:
    """The filed value of each ``CHECK_KEY_ITEMS`` cell the facts state: by the fact's role
    correspondence for a statement line, by the bridge stage for an issuer bridge."""
    from .required_role_catalog import load_required_role_catalog
    from .role_disposition import items_by_role

    reported: dict[CellKey, Decimal] = {}
    by_role = items_by_role(load_required_role_catalog(), ticker=model.company.ticker)
    items_of_role = {
        role: {item for item in lines.all_items() if item in CHECK_KEY_ITEMS}
        for role, lines in by_role.items()
    }
    for fact in facts:
        match = _PERIOD_ID_YEAR.match(fact.period_id)
        role = fact.required_role_correspondence
        if match is None or role is None:
            continue
        for item_id in items_of_role.get(role, ()):
            reported.setdefault((item_id, int(match.group(1))), fact.value)
    for stage in _bridge_stages(facts):
        match = _PERIOD_ID_YEAR.match(stage.filed.period_id)
        if match is not None and stage.item_id in CHECK_KEY_ITEMS:
            reported[(stage.item_id, int(match.group(1)))] = stage.filed.value
    return reported


def _tie_out(
    model: FinancialModel,
    values: Mapping[CellKey, "DependencyGraphNumberValue"],
    ids: Mapping[int, str],
    historical: Sequence[int],
    source_bundle: "SourceBundle | None",
    hff_facts: "Sequence[HistoricalBasisFact] | None",
) -> TieOutSummary:
    items = _indexed(model)
    rows: list[TieOutRow] = []
    if source_bundle is not None:
        recalculated: dict[str, dict[int, float]] = {}
        for (item_id, year), value in values.items():
            if value.state == "known":
                recalculated.setdefault(item_id, {})[year] = value.value
        reconciliation = project_check_rows(source_bundle, recalculated, _fiscal_axis(model)).reconciliation
        for check in reconciliation:
            match = _PERIOD_ID_YEAR.match(check.period)
            if check.item_id not in CHECK_KEY_ITEMS or match is None or int(match.group(1)) not in ids:
                continue
            period_id = ids[int(match.group(1))]
            rows.append(TieOutRow(
                item_id=check.item_id,
                period=period_id,
                rendered=check.rendered,
                reported=check.reported,
                delta=check.delta,
                basis=check.basis,
                status=_tie_status(check.rendered, check.reported, items.get(check.item_id), check.basis),
            ))
    else:
        reported = None if hff_facts is None else _hff_reported(hff_facts, model)
        for item_id in CHECK_KEY_ITEMS:
            for year in historical:
                rendered = _known(values, item_id, year)
                filed = None if reported is None else reported.get((item_id, year))
                filed_value = None if filed is None else float(filed)
                basis: ReconciliationBasis = "statement_fact" if filed is not None else "not_in_pin"
                rows.append(TieOutRow(
                    item_id=item_id,
                    period=ids[year],
                    rendered=rendered,
                    reported=filed_value,
                    delta=None if rendered is None or filed_value is None else rendered - filed_value,
                    basis=basis,
                    # no pin and no HFF: nothing to compare against; an HFF that states no value for
                    # the cell: the cell is not in the pin
                    status="no_history" if reported is None else _tie_status(
                        rendered, filed_value, items.get(item_id), basis,
                    ),
                ))
    order = {item_id: index for index, item_id in enumerate(CHECK_KEY_ITEMS)}
    status_rank = {"off": 0, "ties": 1, "not_in_pin": 2, "no_history": 3}
    ranked = sorted(
        rows,
        key=lambda row: (
            status_rank[row.status],
            -abs(row.delta) if row.delta is not None else 0.0,
            order[row.item_id],
            row.period,
        ),
    )
    return TieOutSummary(
        rows=tuple(ranked[:_TIE_OUT_ROW_LIMIT]),
        ties=sum(row.status == "ties" for row in rows),
        off=sum(row.status == "off" for row in rows),
        not_in_pin=sum(row.status == "not_in_pin" for row in rows),
        balance_sheet_balances=_balance_sheet_balances(values, ids),
    )


# --------------------------------------------------------------------------
# the view
# --------------------------------------------------------------------------


def _selected_items(
    order: Mapping[str, int],
    root_causes: Sequence[RootCause],
    diff_cells: Mapping[str, int],
    limit: int,
) -> list[str]:
    """Row selection in priority order until ``limit``, returned in sheet order."""
    chosen: dict[str, None] = {}

    def take(item_ids: Sequence[str]) -> None:
        for item_id in item_ids:
            if len(chosen) >= limit:
                return
            if item_id in order:
                chosen.setdefault(item_id)

    take([root.root_item_id for root in root_causes])
    take(sorted(diff_cells, key=lambda item_id: (-diff_cells[item_id], order.get(item_id, 0), item_id)))
    take(CHECK_KEY_ITEMS)
    take((DCF_PRICE_ITEM_ID,))
    return sorted(chosen, key=lambda item_id: order[item_id])


def _why(
    item: LineItem,
    columns: Mapping[int, str],
    values: Mapping[CellKey, "DependencyGraphNumberValue"],
    states: Mapping[str, CellState],
    roots: Mapping[str, RootCause],
) -> str | None:
    for year, period_id in columns.items():
        if states[period_id] == "known":
            continue
        value = values.get((item.id, year))
        if value is None or value.state == "known" or not value.causes:
            return f"{states[period_id]} {period_id}"
        cause = value.causes[0]
        root = roots.get(cause.node_id)
        reason = value.primary_reason if root is None else root.reason
        if cause.node_id == item.id:
            return f"{reason} {period_id}"
        return f"{reason}: {cause.node_id} FY{cause.period.fiscal_year}"
    return None


def build_evaluated_view(
    doc: "document.ModelSheet",
    values: "document.Values",
    *,
    previous: "document.Values | None",
    graph: "DependencyGraph",
    typed: Mapping[str, Mapping[int, "DependencyGraphNumberValue"]],
    findings: Sequence["Finding"],
    root_causes: Sequence[RootCause],
    dispositions: "RoleDispositionTable | None",
    source_bundle: "SourceBundle | None",
    hff_facts: "Sequence[HistoricalBasisFact] | None",
    stage_coverage: Mapping[str, str],
    revision: str,
    wall_seconds: float,
    limit: int = EVALUATED_VIEW_ROW_LIMIT,
    root_causes_total: int | None = None,
) -> EvaluatedView:
    """The bounded readback of ``doc`` at its recalc ``values``.

    ``previous`` is the recalc of the revision the refresh replaced (``None`` on a first
    revision: no cell is ``changed``). Rows are the root-cause items, then the items with a
    changed cell, then ``CHECK_KEY_ITEMS``, then the DCF price, until ``limit``. ``findings`` are
    carried by the root causes (``collect_root_causes``) and are not read again here.
    ``root_causes_total`` is the number of roots the check found (``collect_root_causes_counted``);
    the view reports ``len(root_causes)`` when the caller holds no count.
    """
    from model_build import document

    model = doc.model
    ids, historical, projection = period_ids(model)
    columns = tuple(ids[year] for year in (*historical, *projection))
    cells = {
        (item_id, int(year)): value
        for item_id, by_period in typed.items()
        for year, value in by_period.items()
    }
    cells.update(values)

    diff = [] if previous is None else document.value_diff(previous, values, tolerance=1e-9)
    diff_cells: dict[str, int] = {}
    for entry in diff:
        diff_cells[entry["item"]] = diff_cells.get(entry["item"], 0) + 1

    sheet_items = _sheet_items(model)
    order = {item.id: index for index, item in enumerate(sheet_items)}
    disposed = disposition_cell_states(dispositions, model)
    roots_by_item = {root.root_item_id: root for root in root_causes}

    rows: list[EvaluatedRow] = []
    for item_id in _selected_items(order, root_causes, diff_cells, limit):
        item = _indexed(model)[item_id]
        states: dict[str, CellState] = {}
        cell_values: dict[str, float | None] = {}
        for year in (*historical, *projection):
            cell = cells.get((item_id, year))
            period_id = ids[year]
            states[period_id] = _state_of(model, item, year, cell, graph=graph, disposed=disposed)
            cell_values[period_id] = (
                cell.value if cell is not None and cell.state == "known" and states[period_id] == "known" else None
            )
        rows.append(EvaluatedRow(
            item_id=item_id,
            label=item.label,
            section=_section_of(item_id),
            unit=item.unit if item.unit is not None else Unit.item_count,
            model_scale=item.model_scale,
            values=cell_values,
            states=states,
            changed=item_id in diff_cells,
            why=_why(item, {year: ids[year] for year in (*historical, *projection)}, cells, states, roots_by_item),
        ))

    return EvaluatedView(
        revision=revision,
        columns=columns,
        rows=tuple(rows),
        markdown_table=_markdown(columns, rows),
        tie_out_summary=_tie_out(model, cells, ids, historical, source_bundle, hff_facts),
        root_causes=tuple(root_causes),
        root_causes_total=len(root_causes) if root_causes_total is None else root_causes_total,
        unchanged_row_count=len(sheet_items) - len(rows),
        changed_cell_count=len(diff),
        stage_coverage=cast(dict[str, StageCoverage], dict(stage_coverage)),
        wall_seconds=wall_seconds,
    )


__all__ = [
    "BALANCE_TOLERANCE",
    "CellState",
    "EVALUATED_VIEW_ROW_LIMIT",
    "EvaluatedRow",
    "EvaluatedView",
    "ROOT_CAUSE_LIMIT",
    "RepairKind",
    "RootCause",
    "RootOwner",
    "StageCoverage",
    "TieOutRow",
    "TieOutSummary",
    "build_evaluated_view",
    "disposition_cell_states",
    "period_ids",
]
