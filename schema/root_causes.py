"""Root-cause deduplication: the few cells whose repair clears every other non-known cell.

A sheet with one missing history row reads as hundreds of ``missing`` cells and as hundreds of
findings. ``collect_root_causes`` answers the roots by how many cells they explain, each with its
owner stage and the call that repairs it; ``check_model``, ``build_model`` and the evaluated view
carry the same tuple. Pure: nothing is read or written.

Root attribution has one owner per kind of cell. A cell that carries an error finding is
attributed by ``schema.build_findings.fold_semantic_value_findings`` (cascades folded onto the
root's own finding, ``root_item_id``/``root_period``/``root_reason_code`` stamped into
``Finding.observed``); this module reads that result and never re-walks those cells. Only the
non-known cells no finding names (the turn-1 sheet, before validation) are walked upstream
through ``DependencyGraph.cell_operands`` (the one cell-operand resolver, period offsets
included) to the cell that is the first reason they are not known.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
import json
from typing import TYPE_CHECKING, Any, NamedTuple, cast

from .build_findings import fold_semantic_value_findings
from .evaluated_view import (
    ROOT_CAUSE_LIMIT,
    RootCause,
    RootOwner,
    disposition_cell_states,
)
from .renderer_columns import _historical_periods, _projection_periods
from .stage_registry import STAGE_REGISTRY, STAGES

if TYPE_CHECKING:
    from .dependency_graph import DependencyGraph, DependencyGraphNumberValue
    from .finding_vocabulary import Finding
    from .model_number_value import InvalidNumberValue, MissingNumberValue, UnsupportedNumberValue
    from .models import FinancialModel, LineItem
    from .role_disposition import RoleDispositionTable
    from .stable_control_identity import CellKey


#: A reason no stage owns: the engine cannot evaluate the formula the template or document
#: carries, so no judgment or input repairs it. The one exit is the report, never a retry loop.
_ENGINE_REASONS = frozenset({
    "domain_error",
    "non_finite_result",
    "non_finite_input",
    "valuation_method_unsupported",
    "cycle_seed_missing",
    "required_dependency_missing",
})

#: A typed reason the validator mints for a cell whose formula the document holds but the graph cannot
#: evaluate (a raw Excel formula; a ratio whose denominator is zero): the root has no finding, so this
#: module classifies it. The row's formula is rewritten in ``model_document``, like a finding whose
#: ``repair_owner`` is ``document`` (``projected_financial_integrity.RepairOwner``, the one owner of
#: that classification). PAYC :8464 #292 cleared ``division_by_zero`` with ``set_formula``.
_DOCUMENT_REASONS = frozenset({"raw_excel_formula_semantics_unavailable", "division_by_zero"})

#: A scenario output cell the scenario-output step left unwritten because its named
#: source row held no finite value: its root is that source row's root, never the
#: scenario door, which cannot clear an unavailable source.
_SCENARIO_OUTPUT_SOURCE_UNAVAILABLE = "scenario_output_source_unavailable"

#: The finding code of a balance sheet whose projected total does not foot: its call also names the
#: ``add_line`` alternative (``_ADD_LINE_ALTERNATIVE``).
_FOOTING_CODE = "projected_balance_sheet_imbalance"

#: The choice an imbalance call leaves the agent: the total may omit a filed line the sheet has no row for.
_ADD_LINE_ALTERNATIVE = {
    "op": "add_line",
    "when": "the identity omits a filed line the sheet has no row for (add the row, then include it in the subtotal with set_formula)",
}


#: The typed reasons of a cell whose history was never written (no value, no evidence of absence).
_HISTORY_MISSING_REASONS = frozenset({"source_value_unavailable", "missing_source"})

MANUAL_INPUT_UNCITED = "manual_input_uncited"
STAGE_NOT_PERSISTED = "stage_not_persisted"

_STAGE_OWNERS: frozenset[str] = frozenset(STAGES)

#: The stages every other stage is built on (correspondence binds the rows the business model and
#: every later stage read): their gaps rank before every other root, in this order.
_UPSTREAM_STAGES = tuple(
    record.stage for record in STAGE_REGISTRY.values() if record.upstream
)
STAGE_UNREADABLE = "stage_unreadable"
_UPSTREAM_RANK = {
    f"{code}:{stage}": rank
    for rank, stage in enumerate(_UPSTREAM_STAGES)
    for code in (STAGE_NOT_PERSISTED, STAGE_UNREADABLE)
}





def _is_scenario_row(item_id: str) -> bool:
    return (
        item_id.startswith("tpl.s.")
        or ".scenario_tables." in item_id
        or ".header.scenario_" in item_id
    )


def _stage_rows(stage: str, item_id: str) -> bool:
    """Whether ``item_id`` is a row the persisted ``stage`` supplies to the sheet."""
    record = next(
        (candidate for candidate in STAGE_REGISTRY.values() if candidate.stage == stage),
        None,
    )
    if record is None:
        return False
    if record.scenario_rows == "only":
        return _is_scenario_row(item_id)
    matched = item_id.startswith(record.row_prefixes)
    if record.scenario_rows == "exclude":
        return matched and not _is_scenario_row(item_id)
    return matched


def _arguments(research_file_id: int | None, arguments: dict[str, Any]) -> dict[str, Any]:
    """``arguments`` led by ``research_file_id`` when the caller knows it (every door that takes one)."""
    return arguments if research_file_id is None else {"research_file_id": research_file_id, **arguments}


def _stage_door(
    stage: str,
    item_id: str | None,
    *,
    role: str | None = None,
    research_file_id: int | None = None,
    driver_key: str | None = None,
) -> dict[str, Any]:
    """The door that persists ``stage`` and its arguments, the judgment left as placeholders.

    ``item_id`` is the row the repair names; ``None`` is the stage as a whole, whose row the agent
    names (a ``"<...>"`` placeholder). ``driver_key`` is the control a ``forecast_assumption_set``
    row answers (the door's ``driver``, a key its drivers read returns, never the template item id):
    ``None`` leaves it for the agent to take from that read. The one owner of every per-stage call: a
    root item's repair (``_owner_and_call``) and a stage gap's repair (``_stage_gap_roots``) are built here."""
    subject = "<item_id>" if item_id is None else item_id
    if stage == "scenario_set":
        return {
            "tool": STAGE_REGISTRY["scenario_set"].persist_tool,
            "arguments": _arguments(research_file_id, {"judgment": {
                "verdict": "<verdict>",
                "summary": f"<summary: the scenario control {subject}>",
                "confidence": "<confidence>",
            }}),
        }
    if stage == "business_model":
        if item_id is not None and item_id.startswith("bm."):
            segment, _dot, node = item_id[len("bm."):].partition(".")
        else:
            segment, node = "<segment>", "<driver>"
        return {
            "tool": STAGE_REGISTRY["business_model"].persist_tool,
            "arguments": _arguments(research_file_id, {"judgment": {
                "draft_handle": "<latest draft handle>",
                "business_model_judgment": {"segments": [{"key": segment, "drivers": [{"key": node}]}]},
            }}),
        }
    if stage == "historical_foundation":
        # the seed key is the node's own ``<segment>.<node>`` (``CompiledDriverRegistry.node_items``), not its row id
        key = subject[len("bm."):] if subject.startswith("bm.") else subject
        return {
            "tool": STAGE_REGISTRY["historical_foundation"].persist_tool,
            "arguments": _arguments(research_file_id, {"judgment": {
                "verdict": "<verdict>",
                "summary": f"<summary: the historical seed of {subject}>",
                "recommended_next_action": "<recommended_next_action>",
                "historical_driver_seeds": [{"driver_key": key}],
            }}),
        }
    if stage == "correspondence":
        return {
            "tool": STAGE_REGISTRY["correspondence"].persist_tool,
            "arguments": {"judgment": {
                "artifact_kind": "statement_bindings",
                "rationale": f"<rationale: the filed line for {subject}>",
                "expected_revision": "<bindings_revision>",
                "changes": [{"role": role or subject, "state": "bound", "source_ref": "<line_identity>"}],
            }},
        }
    if stage == "forecast_assumption_set":
        # The row is the door's own spec (``forecast_assumption_set._judgment_spec``): ``driver`` names
        # the control the row answers, ``period_values`` is keyed by forecast year, and the pillars
        # below are the fields it requires; only the agent can author them.
        return {
            "tool": STAGE_REGISTRY["forecast_assumption_set"].persist_tool,
            "arguments": _arguments(research_file_id, {"judgment": {
                "verdict": "<verdict>",
                "summary": f"<summary: the forecast driver {subject}>",
                "assumptions": [{
                    "driver": "<driver_key>" if driver_key is None else driver_key,
                    "period_values": {"<forecast_year>": "<value>"},
                    "rationale": "<rationale>",
                    "mechanism": "<mechanism>",
                    "base_rate_anchor": "<base_rate_anchor>",
                    "evidence_quality": "<evidence_quality>",
                    "evidence_quality_assessment": "<evidence_quality_assessment>",
                    "revision_trigger": "<revision_trigger>",
                }],
            }}),
        }
    if stage == "valuation_policy":
        return {
            "tool": STAGE_REGISTRY["valuation_policy"].persist_tool,
            "arguments": _arguments(research_file_id, {"judgment": {
                "verdict": "<verdict>",
                "summary": f"<summary: the valuation input {subject}>",
                "decision": "<decision>",
                "confidence": "<confidence>",
            }}),
        }
    raise ValueError(f"no persist door for stage {stage!r}")


class _CellInput(NamedTuple):
    """What one input cell holds, as a ``model_document`` edit needs to read it (``_cell_input``).

    ``value`` the number the cell holds (a constant, an override or a known value), ``None`` when it
    holds none; ``line`` the filing line its lineage stamp names, ``None`` when the stamp names no
    filing line or there is no stamp; ``removable`` whether the cell stores an override or value
    snapshot a ``clear_input`` would remove."""

    value: float | None = None
    line: dict[str, Any] | None = None
    removable: bool = False


#: The choice a cite call leaves the agent: the held value may have no filed line to cite.
_CLEAR_ALTERNATIVE = {
    "op": "clear_input",
    "when": "the value is genuinely undefined (no filed line exists)",
}


def _filing_line_source(period: int | None) -> dict[str, Any]:
    """The ``set_input`` source of a cell whose stamp names no filing line: a ``filing_line`` key with the
    fields a ``corpus_state`` statement-view row returns (``accession``, ``tag``, ``dimensions`` and the
    ``capture_id`` of the filing-facts capture), left as placeholders for the agent to take from that row.
    Free text, ``{kind: agent}`` and evidence handles read as ``manual_input`` and leave ``missing_source``
    standing (``model_build.document.CITED_SOURCE_KINDS``)."""
    return {
        "kind": "filing_line",
        "capture_id": "<capture_id>",
        "ref": {
            "accession": "<accession>",
            "tag": "<tag>",
            "period": "<period>" if period is None else f"FY{period}",
            "dimensions": [],
        },
    }


def _model_document_call(
    item_id: str,
    periods: Sequence[int | None],
    research_file_id: int | None,
    *,
    cells: Mapping[int, _CellInput] | None = None,
    formula_side: str | None = None,
) -> dict[str, Any]:
    """The ``model_document`` edit that repairs ``item_id`` over ``periods``, the op named by the
    cell's value state and never by the absence of a lineage stamp.

    A cell that holds a value (``cells``, by period: a constant, an override or a known value) is
    cited: ``set_input`` echoing that value with a source, the filing line its stamp names or else the
    ``filing_line`` template (``_filing_line_source``). Where the stamp names no filing line the
    call carries ``alternative`` (``_CLEAR_ALTERNATIVE``): the agent may instead delete a value that
    no filed line carries, knowingly (a ``clear_input`` destroys the value). A cell that holds no value
    is ``clear_input`` only when it stores an override or snapshot to remove, otherwise
    ``set_input`` with a ``<value>`` placeholder and the ``filing_line`` template (nothing to clear). A derived
    row's forecast side (``formula_side``) is repaired with ``set_formula`` on that side:
    a forecast formula has no historical filing line to cite."""
    call: dict[str, Any] = {"tool": "model_document"}
    if formula_side is not None:
        ops: list[dict[str, Any]] = [{
            "op": "set_formula",
            "item_id": item_id,
            "formula_expr": "<formula_expr>",
            "apply_to": formula_side,
            "rationale": "<rationale: why this formula states the row with no filing line>",
        }]
    else:
        held = cells or {}
        ops = []
        uncited = False
        for period in periods:
            cell = _CellInput() if period is None else held.get(period, _CellInput())
            if cell.value is None and cell.removable:
                ops.append({
                    "op": "clear_input",
                    "item_id": item_id,
                    "period": period,
                    "rationale": "<rationale: why this cell holds nothing a filed line carries>",
                })
                continue
            uncited = uncited or (cell.value is not None and cell.line is None)
            ops.append({
                "op": "set_input",
                "item_id": item_id,
                "period": period,
                "value": "<value>" if cell.value is None else cell.value,
                "source": _filing_line_source(period) if cell.line is None else dict(cell.line),
            })
        if uncited:
            call["alternative"] = dict(_CLEAR_ALTERNATIVE)
    call["arguments"] = _arguments(research_file_id, {"operation": "edit", "ops": ops})
    return call


def _formula_repair_call(
    item_id: str, apply_to: str, code: str, research_file_id: int | None,
) -> dict[str, Any]:
    """The ``model_document`` ``set_formula`` that repairs ``item_id`` on the ``apply_to`` side of its
    cells; a footing failure also names the ``add_line`` alternative (``_ADD_LINE_ALTERNATIVE``)."""
    call: dict[str, Any] = {"tool": "model_document"}
    if code == _FOOTING_CODE:
        call["alternative"] = dict(_ADD_LINE_ALTERNATIVE)
    call["arguments"] = _arguments(research_file_id, {"operation": "edit", "ops": [{
        "op": "set_formula",
        "item_id": item_id,
        "formula_expr": "<formula_expr>",
        "apply_to": apply_to,
        "rationale": f"<rationale: how this formula repairs {code}>",
    }]})
    return call


_WORKING_CAPITAL_POLICY_DRIVER_BY_ITEM: dict[str, str] = {
    "tpl.fm.balance_sheet.deferred_revenue_current": "deferred_revenue_ratio",
    "tpl.a.balance_sheet_wc.deferred_revenues": "deferred_revenue_ratio",
    "tpl.fm.balance_sheet.accounts_receivable_net": "dso",
    "tpl.a.balance_sheet_wc.accounts_receivable_net": "dso",
    "tpl.fm.balance_sheet.accounts_payable": "dpo",
    "tpl.a.balance_sheet_wc.accounts_payable": "dpo",
    "tpl.fm.balance_sheet.current_asset_3": "prepaid_pct_revenue",
    "tpl.a.balance_sheet_wc.current_asset_3": "prepaid_pct_revenue",
    "tpl.fm.balance_sheet.current_operating_lease_liabilities": "current_operating_lease_liabilities_pct_revenue",
    "tpl.a.capital_sources.current_operating_lease_liabilities": "current_operating_lease_liabilities_pct_revenue",
}


def _working_capital_driver_call(
    driver_key: str,
    research_file_id: int | None,
) -> dict[str, Any]:
    """The ``fms_set_driver`` call that revises a working-capital-policy ratio driver."""
    judgment: dict[str, Any] = {
        **({"research_file_id": research_file_id} if research_file_id is not None else {}),
        "driver_key": driver_key,
        "values_by_year": {"<forecast_year>": "<value>"},
        "rationale": f"<rationale: the forecast driver {driver_key}>",
    }
    return {
        "tool": "fms_set_driver",
        "arguments": {"judgment": judgment},
    }


def _engine_owned(
    item_id: str,
    reason: str,
    repair_owner: str | None,
    *,
    finding_code: str | None = None,
) -> bool:
    """Whether only the engine's report repairs the root: a formula it cannot evaluate, or a finding that
    itself names the engine as its repair owner."""
    if (
        item_id in _WORKING_CAPITAL_POLICY_DRIVER_BY_ITEM
        and (finding_code or reason) == "statement_total_operand_offsheet"
    ):
        return False
    if reason in _ENGINE_REASONS and not (reason == "required_dependency_missing" and item_id.startswith("bm.")):
        return True
    return repair_owner == "engine"


def _owner_and_call(
    item_id: str,
    reason: str,
    *,
    period: int | None = None,
    periods: Sequence[int] | None = None,
    role: str | None = None,
    research_file_id: int | None = None,
    finding_code: str | None = None,
    repair_owner: str | None = None,
    cells: Mapping[int, _CellInput] | None = None,
    formula_side: str | None = None,
    projected: bool = False,
    historical: bool = False,
    role_root: bool = False,
    declared_absent: bool = False,
    driver_key: str | None = None,
    consumers: Sequence[str] = (),
    segment_node: str | None = None,
) -> tuple[RootOwner, dict[str, Any] | None]:
    """The stage that owns the root and the call that repairs it, routed by what the cell needs.

    A call is executable except its ``"<...>"`` placeholders: the judgment
    only the agent can author (summaries, verdicts, the ``source_ref`` and number of an input,
    the draft handle the door issued). ``research_file_id`` leads the arguments of every door
    that takes one when given; ``fms_persist_correspondence`` and ``fms_report_build_model``
    have no such argument.

    ``reason`` is the cell's typed reason, or the finding code when no typed state is in play;
    ``finding_code`` and ``repair_owner`` are the root's own error finding's code and
    ``observed.repair_owner`` when it has one. ``periods`` are the cells a ``model_document`` edit
    addresses (``period`` alone when omitted: a root with an error finding of its own addresses that
    finding's periods); ``cells`` what each period's cell holds (``_cell_input``) and
    ``formula_side`` the side a ``set_formula`` repair applies to (``_model_document_call``).
    Declared absences (a cited absence, or a cell the analyst cleared: ``declared_absent``) are
    reported at their downstream lowering owner, never rebound; ``consumers`` are the rows that read
    the absent cell (``_direct_readers``) and still fail. ``segment_node`` is the ``bm.`` revenue node a
    segment row reads (``_segment_revenue_node``): the seed that node lacks owns a face or ``__rev`` row
    that holds no value (``_HISTORY_MISSING_REASONS``), never a ``set_input`` typed over the face.
    An engine-owned defect is reported; a document-owned one (the finding's ``repair_owner`` ``document``,
    or a reason in ``_DOCUMENT_REASONS``) is a ``set_formula`` on the side its periods name (``projected`` /
    ``historical`` when every period is of that side, else ``both``); a projected formula is edited in
    ``model_document`` and a projected driver is repaired in the assumption set. A root whose id is no
    document row but a role (``role_root``: the role binding names a line the sheet does not hold) is
    bound in correspondence, the door that owns roles. Only historical face inputs with a
    ``required_role`` are sent to correspondence.
    """
    if declared_absent or reason in _cited_absence_states():
        downstream = ", ".join(consumers) or "the downstream consumer"
        summary = (
            f"{item_id}: {reason}; declared absent at FY{period}; "
            f"{downstream} does not lower the declared absence."
        )
        return "engine", {
            "tool": "fms_report_build_model",
            "arguments": {"judgment": {
                "summary": summary,
                "recommended_next_action": (
                    "Repair schedule absence lowering (R-2): lower_optional_absences / "
                    "the consumer template's optional_if_unreported contract; do not bind the absent cell."
                ),
            }},
        }
    addressed = tuple(periods) if periods else (period,)

    def edit() -> dict[str, Any]:
        return _model_document_call(
            item_id, addressed, research_file_id, cells=cells, formula_side=formula_side,
        )

    if reason == MANUAL_INPUT_UNCITED:
        return "model_document", edit()
    # A ``bm.`` row's formula is the BusinessModel's, never a document edit: ``_apply_set_formula``
    # refuses every ``set_formula`` on one (``bm_row_protected``), so its formula reasons fall through
    # to the ``bm.`` branch below.
    wc_driver_key = driver_key or _WORKING_CAPITAL_POLICY_DRIVER_BY_ITEM.get(item_id)
    if (
        item_id in _WORKING_CAPITAL_POLICY_DRIVER_BY_ITEM
        and wc_driver_key is not None
        and (
            (finding_code or reason) == "statement_total_operand_offsheet"
            or (projected and (finding_code or reason) in {"statement_total_operand_dropped", "missing_source"})
        )
    ):
        return "forecast_assumption_set", _working_capital_driver_call(wc_driver_key, research_file_id)
    if (repair_owner == "document" or reason in _DOCUMENT_REASONS) and not item_id.startswith("bm."):
        side = "projected" if projected else "historical" if historical else "both"
        return "model_document", _formula_repair_call(item_id, side, finding_code or reason, research_file_id)
    # A finding that names the engine as its repair owner is reported wherever its id sits: an id no
    # row carries is only a role (``role_root``) when nothing else owns it.
    if _engine_owned(item_id, reason, repair_owner, finding_code=finding_code):
        return "engine", {
            "tool": "fms_report_build_model",
            "arguments": {"judgment": {
                "summary": f"{item_id}: {finding_code or reason}; the engine cannot evaluate the formula.",
                "recommended_next_action": "Report the template or schedule control the engine cannot evaluate.",
            }},
        }
    if role_root:
        return "correspondence", _stage_door(
            "correspondence", item_id, role=item_id, research_file_id=research_file_id,
        )
    if repair_owner == "driver":
        return "forecast_assumption_set", _stage_door(
            "forecast_assumption_set", item_id, research_file_id=research_file_id, driver_key=wc_driver_key,
        )
    if _is_scenario_row(item_id):
        return "scenario_set", _stage_door("scenario_set", item_id, research_file_id=research_file_id)
    if item_id.startswith("bm."):
        # a node the compiler cannot evaluate (a missing dependency, a zero-denominator ratio on the
        # forecast side) is repaired where the node is authored; its history at the seed
        if reason == "required_dependency_missing" or (reason in _DOCUMENT_REASONS and projected):
            return "business_model", _stage_door("business_model", item_id, research_file_id=research_file_id)
        return "historical_foundation", _stage_door(
            "historical_foundation", segment_node or item_id, research_file_id=research_file_id,
        )
    if item_id.startswith("tpl.v."):
        return "valuation_policy", _stage_door("valuation_policy", item_id, research_file_id=research_file_id)
    if projected:
        if item_id in _WORKING_CAPITAL_POLICY_DRIVER_BY_ITEM and wc_driver_key is not None:
            return "forecast_assumption_set", _working_capital_driver_call(wc_driver_key, research_file_id)
        if formula_side is not None:
            return "model_document", edit()
        return "forecast_assumption_set", _stage_door(
            "forecast_assumption_set", item_id, research_file_id=research_file_id, driver_key=wc_driver_key,
        )
    if item_id.startswith("tpl.fm."):
        if segment_node is not None and reason in _HISTORY_MISSING_REASONS and not any(
            cell.value is not None for cell in (cells or {}).values()
        ):
            # no value and no evidence of absence: the history is the segment node's seed, never a hand-typed face
            return "historical_foundation", _stage_door(
                "historical_foundation", segment_node, research_file_id=research_file_id,
            )
        if role is None:
            return "model_document", edit()
        return "correspondence", _stage_door(
            "correspondence", item_id, role=role, research_file_id=research_file_id,
        )
    return "model_document", edit()


def owner_for(
    item_id: str,
    *,
    reason: str,
    research_file_id: int | None = None,
    role: str | None = None,
    projected: bool = False,
    finding_code: str | None = None,
    repair_owner: str | None = None,
) -> tuple[RootOwner, dict[str, Any] | None]:
    """The owner stage of a root item and its ``repair_call`` (``_owner_and_call``)."""
    return _owner_and_call(
        item_id, reason, research_file_id=research_file_id, role=role, projected=projected,
        finding_code=finding_code, repair_owner=repair_owner,
    )


def _cell_input(model: "FinancialModel", cell: CellKey) -> _CellInput:
    """What ``cell`` holds (``_CellInput``): the number of its value snapshot or, failing that, of a
    constant override; the filing line the value's lineage stamp names (its ``source`` object when
    the kind is ``filing_line``; an analyst's ``{kind: agent}``, a stage artifact or no stamp names
    none); and whether it stores an override or snapshot at all."""
    from .models import FormulaType

    item = model._index.get(cell[0])
    if item is None:
        return _CellInput()
    stored = None if item.values is None else item.values.values.get(cell[1])
    override = (item.overrides or {}).get(cell[1])
    value = None if stored is None else stored.value
    if value is None and override is not None and override.type is FormulaType.constant:
        constant = (override.params or {}).get("value")
        value = float(constant) if isinstance(constant, (int, float)) and not isinstance(constant, bool) else None
    stamp = None if stored is None else stored.lineage_reference
    source = stamp.get("source") if isinstance(stamp, Mapping) else None
    line = dict(source) if isinstance(source, Mapping) and source.get("kind") == "filing_line" else None
    return _CellInput(value=value, line=line, removable=stored is not None or override is not None)


def _formula_side(model: "FinancialModel", item: "LineItem | None", periods: Sequence[int]) -> str | None:
    """The projected formula side of a derived row, independent of its historical binding.

    The projection axis, not a row prefix or an absent historical year, determines the side.
    An absent spec on a derived row needs a formula too; an explicit driver spec is authored
    in the assumption set rather than as a document formula."""
    from .models import FormulaType, ItemType

    return "projected" if (
        item is not None
        and item.item_type is ItemType.derived
        and (item.projected is None or item.projected.type is not FormulaType.driver)
        and periods
        and all(period in _projection_periods(model.time_structure) for period in periods)
    ) else None


def _direct_readers(graph: "DependencyGraph", item_id: str, cascade: Collection[CellKey]) -> list[str]:
    """The rows of ``cascade`` that read ``item_id`` directly: the consumers of an absent cell, not
    every cell its failure reaches transitively. ``DependencyGraph.cell_operands`` is the one
    resolver of what a cell reads."""
    model = _model_of(graph)
    readers: set[str] = set()
    for row, period in cascade:
        item = model._index.get(row)
        if row != item_id and item is not None and any(
            operand[0] == item_id for operand in graph.cell_operands(item, period)
        ):
            readers.add(row)
    return sorted(readers)


def _segment_revenue_node(graph: "DependencyGraph", model: "FinancialModel", item_id: str, period: int) -> str | None:
    """The ``bm.`` revenue node whose history a segment row reads, ``None`` for any other row.

    A segment face (``tpl.fm.income_statement.business_segment_<i>_revenue``, a ref to its segment's
    ``bm.<segment>.__rev``), its growth, and the ``__rev`` row itself read that node through the
    one resolver of what a cell reads (``DependencyGraph.cell_operands``): each step follows the
    row's sole operand row until a ``bm.`` row that is not ``__rev``. A period whose cell is a typed
    constant or has no prior read (the analyst's literal over ``__rev``, a growth's first year)
    reads nothing there, so the row's reads are those of the first period of the axis that reads
    something — except at the row itself, whose read must be a historical one: a face whose history
    is an observed input (a segment with ``revenue_observations``: ``historical=None``, the
    ``__rev`` ref only in ``formula_periods`` = projection) reads the node in no historical year, and
    the node's seed cannot write its cell. A face bound to a filed line (``required_role``), or a
    row that reads several rows, names no node."""
    row = model._index.get(item_id)
    if row is None or not (item_id.startswith("tpl.fm.") or item_id.endswith(".__rev")) or row.required_role is not None:
        return None
    historical = _historical_periods(model.time_structure)
    current = item_id
    for depth in range(4):
        step = model._index.get(current)
        if step is None:
            return None
        if current.startswith("bm.") and not current.endswith(".__rev"):
            return current
        axis = (period, *historical) if depth == 0 else (period, *historical, *_projection_periods(model.time_structure))
        reads = next((
            found for year in axis
            if (found := {operand[0] for operand in graph.cell_operands(step, year)} - {current})
        ), set())
        if len(reads) != 1:
            return None
        (current,) = reads
    return None


def _model_of(graph: "DependencyGraph") -> "FinancialModel":
    if graph.model is None:
        raise ValueError("DependencyGraph.build() must be called before collect_root_causes().")
    return graph.model


def _projection_only_face_blank(model: "FinancialModel", item: "LineItem", period: int) -> bool:
    """Whether ``(item, period)`` is the historical blank of a projection-only face row, by design.

    A ``tpl.fm.`` row the template defines for the projection years alone (``historical`` is ``None``)
    and that carries no ``required_role`` has no filed line anyone could bind to its history, and
    nothing was written there: the blank is the row's shape (``unlevered_free_cash_flow``), not a
    missing input, and no repair exists for it."""
    from .models import ItemType

    return (
        item.id.startswith("tpl.fm.")
        and item.item_type is ItemType.derived
        and item.required_role is None
        and item.historical is None
        and item.projected is not None
        and period in _historical_periods(model.time_structure)
        and not (item.overrides and period in item.overrides)
        and not (item.values is not None and period in item.values.values)
    )


def _presentation_cell(graph: "DependencyGraph", cell: CellKey) -> bool:
    """Presentation identities and text are never financial roots, including finding roots.

    Read the model's row type, the caption owner's metadata set, and presentation lineage;
    do not infer presentation from a sheet/item prefix.
    """
    from .build_model_product_contracts import _WORKBOOK_METADATA_ITEM_IDS
    from .build_offline_validation import _formula_resolves_to_text_metadata
    from .models import ItemType

    model = _model_of(graph)
    item = model._index.get(cell[0])
    if item is None:
        return False
    if (
        item.item_type in {ItemType.header, ItemType.spacer}
        or item.id in _WORKBOOK_METADATA_ITEM_IDS
    ):
        return True
    stored = None if item.values is None else item.values.values.get(cell[1])
    stamp = None if stored is None else stored.lineage_reference
    if isinstance(stamp, Mapping) and stamp.get("origin") == "presentation":
        return True
    # A text override on a numeric input (for example an analyst's "n/a") is not a
    # presentation declaration: its missing-source finding still needs a real repair.
    if item.overrides and cell[1] in item.overrides:
        return False
    spec = graph._spec_for_period(item, cell[1])
    return spec is not None and _formula_resolves_to_text_metadata(model, item, cell[1], spec)


def _bad_cells(
    graph: "DependencyGraph",
    typed: Mapping[str, Mapping[int, "DependencyGraphNumberValue"]],
    dispositions: "RoleDispositionTable | None",
    gaps: Collection[CellKey],
) -> "dict[CellKey, MissingNumberValue | UnsupportedNumberValue | InvalidNumberValue]":
    """Every non-known cell except an intentional absence: exactly the bad-cell predicate.

    A cell is bad when its typed value is ``Missing``, ``Unsupported`` or ``Invalid``, it is
    not in the role disposition table's disposed states, its item exists and owns the
    coordinate (``item_owns_cell`` with the bundle's ``gaps``, the rule the renderer writes
    ``=NA()`` by; ``compute_typed`` still types every axis period), it is not a presentation
    cell, and it is not the historical blank of a projection-only face row. The graph walk
    and ``cascade_count`` read only this set.

    Each kept exclusion is derived from its owner, never an enumerated id list. A
    presentation cell (``_presentation_cell``) is one by the renderer's own facts: a
    header or spacer row type, the caption owner's workbook-metadata set, a presentation
    lineage stamp, or a formula that resolves to text metadata. A formula-less caption
    row types ``missing``, so without this exclusion the walk would attribute through it
    and the root-candidate filter would then discard it with its whole cascade. The
    face blank (``_projection_only_face_blank``) is blank by design: the template defines
    the row for the projection years alone and no filed line maps its history, and the
    role-keyed disposition table can never dispose a row that carries no role.

    Constant typing is three-way at ``compute_typed``, its owner: an authored text
    constant (a year label, a ticker) types ``evidenced_absent``, never
    ``non_finite_input``, so it is not in the typed set above; an override carrying text
    types ``missing`` (``source_value_unavailable``), so it stays a bad cell with a
    document repair; a non-string constant keeps ``non_finite_input``.
    """
    from .model_number_value import InvalidNumberValue, MissingNumberValue, UnsupportedNumberValue
    from .renderer_columns import item_owns_cell

    model = _model_of(graph)
    disposed = disposition_cell_states(dispositions, model)

    def kept(item_id: str, period: int) -> bool:
        item = model._index.get(item_id)
        if item is None or not item_owns_cell(model, item, period, gaps=gaps):
            return False
        if _presentation_cell(graph, (item_id, period)):
            return False
        if _projection_only_face_blank(model, item, period):
            return False
        return True

    return {
        (item_id, int(period)): value
        for item_id, by_period in typed.items()
        for period, value in by_period.items()
        if isinstance(
            value, (MissingNumberValue, UnsupportedNumberValue, InvalidNumberValue)
        )
        and (item_id, int(period)) not in disposed
        and kept(item_id, int(period))
    }


def _uncited_agent_input(model: "FinancialModel", cell: CellKey) -> bool:
    """Whether ``cell`` holds an input the analyst wrote without a cited source: its value's
    lineage stamp (``model_build.document.set_inputs``) is ``origin: manual_input`` over a
    ``{kind: agent}`` source. A cell a stage artifact wrote (``judgment_row``, ``presentation_choice``)
    is also ``manual_input`` and stays that stage's to repair; a cited input is ``cited_input``."""
    item = model._index.get(cell[0])
    value = None if item is None or item.values is None else item.values.values.get(cell[1])
    stamp = None if value is None else value.lineage_reference
    if not isinstance(stamp, Mapping) or stamp.get("origin") != "manual_input":
        return False
    source = stamp.get("source")
    return isinstance(source, Mapping) and source.get("kind") == "agent"


def _attribute(graph: "DependencyGraph", bad: Mapping[CellKey, Any]) -> dict[CellKey, CellKey]:
    """Each bad cell -> the root it is attributed to: the first root reached walking its
    operands in sorted order. A cell is a root when none of its operands is bad (an unseeded
    input, a ``prior_period_unavailable`` at the axis start, a ``source_value_unavailable``
    leaf), or when every bad operand lies on its own path (a cycle)."""
    model = _model_of(graph)

    def bad_operands(cell: CellKey) -> list[CellKey]:
        item = model._index.get(cell[0])
        if item is None:
            return []
        return sorted(operand for operand in graph.cell_operands(item, cell[1]) if operand in bad)

    root_of: dict[CellKey, CellKey] = {}
    for start in sorted(bad):
        if start in root_of:
            continue
        frames: list[list[Any]] = [[start, bad_operands(start), 0]]
        path = {start}
        while frames:
            frame = frames[-1]
            cell, operands, index = frame
            if index > 0 and operands[index - 1] in root_of:
                root_of[cell] = root_of[operands[index - 1]]
            elif index == len(operands):
                root_of[cell] = cell
            else:
                frame[2] = index + 1
                operand = operands[index]
                if operand in root_of:
                    root_of[cell] = root_of[operand]
                elif operand not in path:
                    frames.append([operand, bad_operands(operand), 0])
                    path.add(operand)
                    continue
                else:
                    continue
            path.discard(cell)
            frames.pop()
    return root_of


@dataclass
class _Attribution:
    """The cells one root item explains: its own root cells and the cells attributed to them.
    ``findings`` are the error findings on the root's own cells (empty for a root a finding explains
    only through a cascade, and for a walked root)."""

    item_id: str
    source: str  # "fold" | "walk"
    root_cells: set[CellKey] = field(default_factory=set)
    cells: set[CellKey] = field(default_factory=set)
    reasons: dict[CellKey, str] = field(default_factory=dict)
    findings: dict[CellKey, "Finding"] = field(default_factory=dict)

    def add_root(self, cell: CellKey, reason: str | None, finding: "Finding | None" = None) -> None:
        self.root_cells.add(cell)
        self.cells.add(cell)
        if reason is not None:
            self.reasons.setdefault(cell, reason)
        if finding is not None:
            self.findings.setdefault(cell, finding)

    def own_finding(self, cells: Collection[CellKey]) -> "Finding | None":
        """The finding on the earliest eligible root cell (never presentation)."""
        first = min(
            (cell for cell in self.findings if cell in cells),
            key=lambda cell: cell[1], default=None,
        )
        return None if first is None else self.findings[first]


def _folded_attribution(
    typed: Mapping[str, Mapping[int, "DependencyGraphNumberValue"]],
    findings: Sequence["Finding"],
    values: Mapping[str, Mapping[int, Any]],
) -> tuple[dict[str, _Attribution], set[CellKey]]:
    """The roots of the cells that carry an error finding, read from the folded findings.

    ``fold_semantic_value_findings`` is the owner: a finding that folded onto a root finding
    leaves its cells in that root's ``affected_cells``; a finding that stayed carries its root in
    ``observed``; any other error finding is its own root. Returns the attributions by root item
    and every cell a finding names (the cells the walker must not re-attribute).
    A finding that names no period names no cell and roots none; the door that reports it reads it from the findings.
    """
    errors = [
        finding for finding in findings
        if finding.severity == "error" and finding.item_id is not None and finding.periods
    ]
    root_cells = {
        (item_id, int(period))
        for item_id, by_period in typed.items()
        for period, value in by_period.items()
        if value.state in ("known", "missing")
    }
    named = {(cast(str, finding.item_id), int(period)) for finding in errors for period in finding.periods}
    attributions: dict[str, _Attribution] = {}

    def attribution(item_id: str) -> _Attribution:
        return attributions.setdefault(item_id, _Attribution(item_id, "fold"))

    for finding in fold_semantic_value_findings(errors, values, root_cells=root_cells):
        item_id = cast(str, finding.item_id)
        cells = {(item_id, int(period)) for period in finding.periods}
        observed = finding.observed
        if observed.get("root_item_id") is not None:
            root_item = str(observed["root_item_id"])
            reason = observed.get("root_reason_code")
            held = attribution(root_item)
            held.add_root((root_item, int(cast(int, observed["root_period"]))), None if reason is None else str(reason))
            held.cells.update(cells)
        else:
            held = attribution(item_id)
            for cell in cells:
                held.add_root(cell, finding.code, finding)
            affected = observed.get("affected_cells")
            if isinstance(affected, str):
                held.cells.update((str(row[0]), int(row[1])) for row in json.loads(affected))
    return attributions, named


def _walked_attribution(
    graph: "DependencyGraph",
    bad: Mapping[CellKey, Any],
    named: set[CellKey],
    folded: dict[str, _Attribution],
) -> dict[str, _Attribution]:
    """The non-known cells no finding names, attributed by the graph walk. A cell whose root is a
    cell a finding root already explains joins that root; any other root is a walked root."""
    root_of = _attribute(graph, bad)
    owner_of = {cell: item_id for item_id, held in folded.items() for cell in held.cells}
    walked: dict[str, _Attribution] = {}
    for cell in sorted(set(bad) - named - {c for held in folded.values() for c in held.cells}):
        root = root_of[cell]
        joined = owner_of.get(root)
        if joined is not None:
            folded[joined].cells.add(cell)
            continue
        held = walked.setdefault(root[0], _Attribution(root[0], "walk"))
        held.add_root(root, bad[root].primary_reason)
        held.cells.add(cell)
    return walked


def _read_first(next_call: Mapping[str, Any] | None, *, door: str) -> dict[str, Any] | None:
    """The producer's context read of a stage, as ``{"tool", "arguments"}``; ``None`` when the
    producer names no read (no ``next_call``, or the call is the persist door itself)."""
    if next_call is None or next_call["tool_name"] == door:
        return None
    return {"tool": next_call["tool_name"], "arguments": dict(next_call["arguments"])}


def _stage_gap_roots(
    stage_gaps: Sequence[Mapping[str, Any]],
    bad: Mapping[CellKey, Any],
    regular: dict[str, RootCause],
    attributions: Mapping[str, _Attribution],
    model: "FinancialModel",
    research_file_id: int | None,
    driver_keys_by_item: Mapping[str, str],
) -> list[RootCause]:
    """One root per stage gap, in rank order: the upstream stages (``_UPSTREAM_STAGES``), then by
    the cells explained.

    A gap explains every non-known cell of the rows its stage supplies, so ``cascade_count`` is
    that count. It sits on the most explanatory ``regular`` root among those rows, which it
    removes from ``regular`` (one root per item; the stage repair clears it), and its cascade
    adds the cells that root explained outside the stage rows; with no such root it sits on the
    synthetic ``stage:<stage>`` item. A stage with a persist door is repaired by that door
    (``_stage_door``), led by the producer's context read as ``read_first``; a stage with none
    (``source_bundle``) by the producer's own call."""
    gaps = {str(gap["stage"]): gap for gap in stage_gaps}
    stage_cells = {stage: {cell for cell in bad if _stage_rows(stage, cell[0])} for stage in gaps}
    order = sorted(gaps, key=lambda stage: (
        _UPSTREAM_STAGES.index(stage) if stage in _UPSTREAM_STAGES else len(_UPSTREAM_STAGES),
        -len(stage_cells[stage]),
        stage,
    ))
    roots: list[RootCause] = []
    for stage in order:
        cells = stage_cells[stage]
        top = next(
            (
                root for root in sorted(regular.values(), key=lambda r: (-r.cascade_count, r.root_item_id))
                if _stage_rows(stage, root.root_item_id)
            ),
            None,
        )
        if top is not None:
            del regular[top.root_item_id]
            cells = cells | attributions[top.root_item_id].cells
        item_id = None if top is None else top.root_item_id
        item = None if item_id is None else model._index.get(item_id)
        next_call = gaps[stage].get("next_call")
        repair_call: dict[str, Any] | None
        if stage in _STAGE_OWNERS:
            repair_call = _stage_door(
                stage, item_id, role=None if item is None else item.required_role,
                research_file_id=research_file_id, driver_key=driver_keys_by_item.get(item_id or ""),
            )
            read_first = _read_first(next_call, door=repair_call["tool"])
            if read_first is not None:
                repair_call["read_first"] = read_first
        else:
            repair_call = None if next_call is None else {
                "tool": next_call["tool_name"], "arguments": dict(next_call["arguments"]),
            }
        roots.append(RootCause(
            root_item_id=f"stage:{stage}" if top is None else top.root_item_id,
            period=0 if top is None else top.period,
            reason=f"{gaps[stage].get('code') or STAGE_NOT_PERSISTED}:{stage}",
            cascade_count=len(cells),
            owner=cast(RootOwner, stage) if stage in _STAGE_OWNERS else "model_document",
            repair_call=repair_call,
        ))
    return roots


def _findings_on_document_rows(
    findings: Sequence["Finding"], model: "FinancialModel", role_items: Mapping[str, Collection[str]],
) -> list["Finding"]:
    """``findings`` with each one that names a role re-pointed at the document rows that carry it.

    A pinned finding (an evidence audit of ``operating.<leaf>``) names the role it audits, and a
    ``model_document`` edit addresses rows of the document, never roles; ``role_items`` is the
    document's own role-to-rows map. A role no row of the model carries stays as it is: its root is
    repaired where roles are bound (``_owner_and_call``'s ``role_root``)."""
    rebound: list["Finding"] = []
    for finding in findings:
        rows = (
            sorted(row for row in role_items.get(finding.item_id, ()) if row in model._index)
            if finding.item_id is not None and finding.item_id not in model._index else []
        )
        if rows:
            rebound.extend(finding.model_copy(update={"item_id": row}) for row in rows)
        else:
            rebound.append(finding)
    return rebound


def _cited_absence_states() -> frozenset[str]:
    """The typed reasons of a cell the evidence already accounts for, so no repair is owed it.

    The dispositions of the cell algebra's owner
    (``schema.model_number_value.AbsenceDisposition``: every evidenced-absence
    disposition, including ``not_separately_presented``). They are the typed
    counterparts of the provenance status ``source_absence_evidenced`` and of
    the inclusion stamp (``model_build.target_build.project_provenance_map``)."""
    from typing import get_args

    from .model_number_value import AbsenceDisposition

    return frozenset(get_args(AbsenceDisposition))


def collect_root_causes(
    graph: "DependencyGraph",
    typed: Mapping[str, Mapping[int, "DependencyGraphNumberValue"]],
    *,
    findings: Sequence["Finding"],
    dispositions: "RoleDispositionTable | None",
    stage_gaps: Sequence[Mapping[str, Any]],
    role_items: Mapping[str, Collection[str]],
    manual_input_cells: Sequence[tuple[str, int]] = (),
    limit: int = ROOT_CAUSE_LIMIT,
    research_file_id: int | None = None,
    values: Mapping[str, Mapping[int, Any]] | None = None,
    gaps: Collection[CellKey] = (),
    cleared_cells: Collection[CellKey] = (),
    driver_keys_by_item: Mapping[str, str] = {},
) -> tuple[RootCause, ...]:
    """The roots of ``collect_root_causes_counted``, without the count."""
    return collect_root_causes_counted(
        graph, typed, findings=findings, dispositions=dispositions, stage_gaps=stage_gaps, role_items=role_items,
        manual_input_cells=manual_input_cells, limit=limit, research_file_id=research_file_id,
        values=values, gaps=gaps, cleared_cells=cleared_cells, driver_keys_by_item=driver_keys_by_item,
    )[0]


def collect_root_causes_counted(
    graph: "DependencyGraph",
    typed: Mapping[str, Mapping[int, "DependencyGraphNumberValue"]],
    *,
    findings: Sequence["Finding"],
    dispositions: "RoleDispositionTable | None",
    stage_gaps: Sequence[Mapping[str, Any]],
    role_items: Mapping[str, Collection[str]],
    manual_input_cells: Sequence[tuple[str, int]] = (),
    limit: int = ROOT_CAUSE_LIMIT,
    research_file_id: int | None = None,
    values: Mapping[str, Mapping[int, Any]] | None = None,
    gaps: Collection[CellKey] = (),
    cleared_cells: Collection[CellKey] = (),
    driver_keys_by_item: Mapping[str, str] = {},
) -> tuple[tuple[RootCause, ...], int]:
    """The root causes of every non-known cell ``_bad_cells`` keeps, most explanatory first, and
    the number of roots found before the cap.

    A cell that carries an error finding is attributed through the folded findings
    (``fold_semantic_value_findings`` over ``findings`` and ``values``, ``typed`` when ``values``
    is omitted, with the known/missing cells of ``typed`` as its root domain); a non-known cell
    no finding names is attributed by the graph walk. The roots of one item (the same missing
    line in every period) are one ``RootCause`` at the item's earliest root period;
    ``cascade_count`` is the cells attributed to it other than that one, so a root's explained
    cells are ``cascade_count + 1``. ``reason`` is the code of the root's own finding, else the
    typed reason the fold or the walk names, never a sentence. A declared-absence root names that
    absence and the rows that read it directly (``_direct_readers``) whose lowering still fails, and
    is reported rather than rebound.
    Presentation cells never become roots, even when a finding names them.
    A root with an error finding of its own is repaired at
    that finding's periods (``period`` is its earliest, the call addresses all of them): the walked
    cells merged onto the same item count in ``cascade_count`` but are not where the repair goes.

    A root a blocking finding backs (the item of an error finding's own cell, or the root a
    finding's cascade is attributed to) is always returned. A walked root every cell of which is
    already accounted for is not a root: a cell whose reading in ``values`` is a cited absence
    (``_cited_absence_states``), or one the document records the analyst cleared
    (``cleared_cells``, ``model_build.document._cleared_cells``: the provenance map's
    ``document_absence``, ``source_absence_evidenced``). Those cells are explained, not repaired,
    unless a cell of the walked root's cascade still fails with a reason no one accounts for: the
    absence is then a declared-absence root naming the rows that read it.
    ``values`` carries those readings (the document's recalc, whose ``unavailable_reasons`` replace
    ``source_value_unavailable`` with the declared reason); ``typed``, read when it is omitted,
    carries only ``not_separately_presented``.

    A ``stage_gaps`` entry (``{"stage", "next_call", ...}``, a stage no artifact supplies) is a
    root whose ``cascade_count`` is the non-known cells of the rows that stage supplies and whose
    repair is the stage's persist door with the producer's read as ``read_first``
    (``_stage_gap_roots``); it takes the place of the item root it sits on, so no item is two
    roots (and is finding-backed when that root was). Each item of ``manual_input_cells`` (an input
    written without a source) that is not already a root is a ``manual_input_uncited`` root with no
    cascade. A root whose own cell is an uncited analyst input (``_uncited_agent_input``, read from
    the model's lineage stamp) keeps its reason but is repaired by citing it in ``model_document``,
    not by its row's stage door; every other root is repaired by the door its cell needs
    (``_owner_and_call``, given the cell's period, its row's ``required_role`` and its own
    finding's code and ``repair_owner``).
    ``gaps`` are the owned cells beyond ``formula_periods`` the SourceBundle declares
    (``build_offline_validation.owned_cell_gaps``), the ``gaps`` of ``item_owns_cell``: a lag-input
    gap reader renders ``#N/A`` and is a cell the walk attributes. The default ``()`` is the true
    value before a bundle is bound (no bundle declares no gaps), not a fallback; a caller that
    holds a bundle passes its gaps.
    ``research_file_id`` fills each ``repair_call`` whose door takes one.
    ``driver_keys_by_item`` maps a template row to the forecast driver key that controls it (the key
    the assumption set's door resolves, derived by the caller from its owner,
    ``research.forecast_control_profile.forecast_driver_keys_by_item``); a row it does not name
    leaves ``driver`` a placeholder for the agent to take from the drivers read, never the item id.
    ``role_items`` maps a role (``operating.<leaf>``, ``statement.<...>``) to the document rows that carry
    it (``model_build.document._inclusion_role_items``, derived by the caller from its document): a
    finding that names a role is a finding of those rows, because a ``model_document`` edit addresses
    document rows only. A role no row carries is a root bound in correspondence (``role_root``).

    Sorted with the correspondence and business-model gaps first (every other stage reads them),
    then the finding-backed roots, then by ``cascade_count`` descending, then item id. ``limit``
    counts only the roots no finding backs, in that order; the second value is the number of roots
    before it cut any, so the cap is a measurement the caller reports, never a way to lose a
    finding.
    """
    model = _model_of(graph)
    findings = _findings_on_document_rows(findings, model, role_items)
    bad = _bad_cells(graph, typed, dispositions, gaps)
    folded, named = _folded_attribution(typed, findings, typed if values is None else values)
    attributions = _walked_attribution(graph, bad, named, folded)
    for item_id, held in folded.items():
        walked = attributions.get(item_id)
        if walked is not None:
            # the same item is a finding root in some periods and a walked root in others: one root
            held.root_cells |= walked.root_cells
            held.cells |= walked.cells
            if not held.reasons:
                held.reasons.update(walked.reasons)
        attributions[item_id] = held
    # A scenario output cell carrying ``scenario_output_source_unavailable`` folds
    # into its named source row's root: the typed cause names that row, and the
    # attribution holding the source cell (its own root, or the upstream root its
    # walk reached) explains the output cell too, so no ``scenario_set`` root stands.
    reading_for_fold = typed if values is None else values
    for item_id in list(attributions):
        held = attributions[item_id]
        named_source: CellKey | None = None
        for cell in sorted(held.root_cells):
            value = reading_for_fold.get(cell[0], {}).get(cell[1]) or bad.get(cell)
            if value is None or value.state == "known":
                continue
            named_source = next((
                (cause.node_id, int(cause.period.fiscal_year))
                for cause in value.causes
                if cause.reason_code == _SCENARIO_OUTPUT_SOURCE_UNAVAILABLE
                and cause.node_id != item_id
            ), None)
            if named_source is not None:
                break
        if named_source is None:
            continue
        target = attributions.get(named_source[0]) or next(
            (other for other in attributions.values() if named_source in other.cells), None,
        )
        if target is held:
            continue
        if target is None:
            target = _Attribution(named_source[0], "walk")
            source_value = bad.get(named_source)
            target.add_root(named_source, None if source_value is None else source_value.primary_reason)
            attributions[named_source[0]] = target
        target.cells.update(held.cells)
        del attributions[item_id]

    finding_items = {item_id for item_id, held in attributions.items() if held.source == "fold"}
    cited = _cited_absence_states()
    cleared = set(cleared_cells)
    reading = typed if values is None else values

    def cited_cell(cell: CellKey) -> bool:
        value = reading.get(cell[0], {}).get(cell[1])
        return value is not None and value.state != "known" and value.primary_reason in cited

    def accounted(cell: CellKey) -> bool:
        return cell in cleared or cited_cell(cell)

    roots: list[RootCause] = []
    for item_id, held in attributions.items():
        candidates = {cell for cell in held.root_cells if not _presentation_cell(graph, cell)}
        root_cells = {cell for cell in candidates if item_id in finding_items or not accounted(cell)}
        # a walked root every cell of which is accounted for is explained, unless a cell of its cascade
        # still fails with a reason nobody accounts for: the absence is then the root of that failure
        declared_root = not root_cells and bool(candidates) and any(
            cell not in held.root_cells and not accounted(cell) for cell in held.cells
        )
        if declared_root:
            root_cells = candidates
        if not root_cells:
            continue
        own = held.own_finding(root_cells)
        # an item with its own error finding is repaired where that finding sits: its periods, not the
        # earlier walked cells merged onto the item (those still count in ``cascade_count``)
        repair_periods = sorted({
            cell[1] for cell, finding in held.findings.items() if finding is own and cell in root_cells
        }) if own is not None else []
        repair_cells = {(item_id, period) for period in repair_periods} or root_cells
        first = min(repair_cells, key=lambda cell: cell[1])
        typed_value = bad.get(first)
        declared = reading.get(item_id, {}).get(first[1])
        absence_reason = (
            declared.primary_reason
            if declared is not None and declared.state != "known" and declared.primary_reason in cited
            else None
        )
        reason = next(
            (held.reasons[cell] for cell in sorted(held.reasons, key=lambda cell: cell[1]) if cell in repair_cells),
            None if typed_value is None else typed_value.primary_reason,
        ) or "semantic_value_not_known"
        absence_reason = absence_reason or (reason if reason in cited else None)
        item = model._index.get(item_id)
        repair_owner = None if own is None else own.observed.get("repair_owner")
        edited = repair_periods or [first[1]]
        # Only a cited absence is declared: the lowering owner (``document.lower_optional_absences``)
        # omits a cited reason from its sums and never the ``source_value_unavailable`` an agent
        # ``clear_input`` leaves, so a root of merely cleared cells whose readers still fail is
        # repaired by what the cell needs (a value or a citation), not reported at the reader.
        declared_absent = absence_reason is not None or (
            declared_root and all(cited_cell(cell) for cell in candidates)
        )
        owner, repair_call = _owner_and_call(
            item_id,
            # an input the analyst wrote without a source is repaired where it was written, whatever
            # stage the row belongs to
            absence_reason or (
                MANUAL_INPUT_UNCITED if _uncited_agent_input(model, first)
                else reason if typed_value is None else typed_value.primary_reason
            ),
            period=first[1],
            periods=edited,
            role=None if item is None else item.required_role,
            research_file_id=research_file_id,
            finding_code=None if own is None else own.code,
            repair_owner=None if repair_owner is None else str(repair_owner),
            cells={period: _cell_input(model, (item_id, period)) for period in edited},
            formula_side=_formula_side(model, item, edited),
            projected=all(period in _projection_periods(model.time_structure) for period in edited),
            historical=all(period in _historical_periods(model.time_structure) for period in edited),
            role_root=item is None,
            declared_absent=declared_absent,
            driver_key=driver_keys_by_item.get(item_id),
            consumers=_direct_readers(graph, item_id, held.cells) if declared_absent else (),
            segment_node=_segment_revenue_node(graph, model, item_id, first[1]),
        )
        roots.append(RootCause(
            root_item_id=item_id,
            period=first[1],
            reason=reason,
            cascade_count=len(held.cells) - 1,
            owner=owner,
            repair_call=repair_call,
        ))

    regular = {root.root_item_id: root for root in roots}
    gap_roots = _stage_gap_roots(stage_gaps, bad, regular, attributions, model, research_file_id, driver_keys_by_item)
    roots = [*regular.values(), *gap_roots]

    manual_items = sorted({item_id for item_id, _period in manual_input_cells} - {root.root_item_id for root in roots})
    for item_id in manual_items:
        period = min(period for item, period in manual_input_cells if item == item_id)
        if _presentation_cell(graph, (item_id, period)):
            continue
        owner, repair_call = _owner_and_call(
            item_id, MANUAL_INPUT_UNCITED, period=period, research_file_id=research_file_id,
            cells={period: _cell_input(model, (item_id, period))},
        )
        roots.append(RootCause(
            root_item_id=item_id,
            period=period,
            reason=MANUAL_INPUT_UNCITED,
            cascade_count=0,
            owner=owner,
            repair_call=repair_call,
        ))

    roots.sort(key=lambda root: (
        _UPSTREAM_RANK.get(root.reason, len(_UPSTREAM_STAGES)),
        root.root_item_id not in finding_items,
        -root.cascade_count,
        root.root_item_id,
    ))
    shown: list[RootCause] = []
    unbacked = 0
    for root in roots:
        if root.root_item_id not in finding_items:
            if unbacked == limit:
                continue
            unbacked += 1
        shown.append(root)
    return tuple(shown), len(roots)


__all__ = ["collect_root_causes", "collect_root_causes_counted", "owner_for"]
