"""The build-model checks: every producer of a Finding, and the declinations.

The vocabulary these checks report in — :class:`Finding`, the severity map,
:class:`Declination` and the one build verdict — is
``schema/finding_vocabulary.py``. :func:`make_finding` builds a Finding from
its map row. :func:`apply_declinations` attaches each matching declination to
its warning, which then counts as cleared; errors are never cleared, and a
declination that answers no finding is the ``info`` finding
``declination_unmatched``, never a refusal.

:func:`compile_stage_findings` evaluates the checks whose inputs exist before
the workbook is rendered (readiness, schedule-input audit, valuation-input
flags, compiler house values written ``default``, unit/format metadata,
required-unavailable cells, the historical diagnostic report, the findings
the compile itself reports instead of raising — business-model series
coverage and node history, required lines, statement windows, forecast
assumptions — and the judgment-revision findings the build lifecycle
computes on the bound FAS and HFF with :func:`judgment_revision_findings`).
``validate_offline_build_artifacts`` (``schema/build_offline_validation.py``)
calls it and applies the ModelSpec's
declinations, so every caller of the evaluator gets the same verdict. The
design and the severity oracle are ``docs/closed/one-finding-type-one-verdict.md``
and ``docs/closed/drivers-table-declination-successors.md``.
:func:`judgment_info_findings` reports the ``info`` judgment flags (uncited
driver, stale market input, unevidenced critical factor, scenario coverage);
info never enters the verdict (``docs/closed/info-findings-guidance-citation-staleness.md``).
:func:`deliverable_version_findings` reports ``deliverable_version_stale`` on a
deliverable read whose research file's pointer has moved past the ModelVersion
the deliverable names (``schema/deliverable_binding.py``); a notice, never a gate.
:func:`question_unratified_findings` reports ``question_unratified`` on a
deliverable whose bound Question is not yet ratified (ruled (a), 2026-10-04): a
warning, never a refusal. :func:`retired_plan_driver_findings` reports
``forecast_driver_plan_row_retired`` for each stored plan row the current
forecast denominator retired: a warning naming the door that drops it, never
a refusal.
"""

from __future__ import annotations

from collections.abc import Collection, Iterable, Mapping, Sequence
from dataclasses import dataclass
import json
import logging
import re
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Final, cast

from .corpus_citation import mask_corpus_version
from .presentation_captions import WORKBOOK_PRESENTATION_CAPTION_ITEM_IDS
from .finding_vocabulary import (
    FINDING_MESSAGE_MAX_LENGTH,
    FINDING_SEVERITY_MAP,
    Declination,
    Finding,
    FindingBasis,
    FindingScalar,
)

if TYPE_CHECKING:
    from datetime import datetime

    from .build_model_product_contracts import (
        BuildDiagnostic, ModelSpec, ProvenanceMap, SourceBundle, SourceEvidence,
    )
    from .business_model_revision import BusinessModelRevision
    from .deliverable_binding import DeliverableModelBinding
    from .dependency_graph import DependencyGraphNumberValue
    from .forecast_assumption_set import ForecastAssumptionSet
    from .historical_foundation import HistoricalFoundation
    from .model_ref import ModelVersionRef
    from .thesis import QuestionRef, ThesisQuestion
    from .judgment_history import JudgmentArtifact
    from .models import FinancialModel, LineItem


_LOGGER = logging.getLogger(__name__)


def bounded_finding_message(prefix: str, detail: object = "") -> str:
    """Fit a deterministic message inside the Finding message contract."""

    text = str(detail)
    length = len(prefix) + len(text)
    if length <= FINDING_MESSAGE_MAX_LENGTH:
        return prefix + text
    suffix = f" ... ({length - FINDING_MESSAGE_MAX_LENGTH} characters omitted)"
    head = FINDING_MESSAGE_MAX_LENGTH - len(suffix)
    if len(prefix) >= head:
        return prefix[:head] + suffix
    return prefix + text[: head - len(prefix)] + suffix


def make_finding(
    code: str,
    *,
    message: str,
    basis: FindingBasis | None = None,
    item_id: str | None = None,
    periods: Iterable[int] = (),
    operands: Iterable[str] = (),
    repair_periods: Iterable[int] = (),
    observed: Mapping[str, FindingScalar] | None = None,
    expected: Mapping[str, FindingScalar] | None = None,
) -> Finding:
    """Build a Finding whose severity and clearing route come from its map row."""

    rule = FINDING_SEVERITY_MAP[code]
    return Finding(
        code=code,
        severity=rule.severity,
        basis=rule.default_basis if basis is None else basis,
        item_id=item_id,
        periods=tuple(sorted({int(period) for period in periods})),
        operands=tuple(operands),
        repair_periods=tuple(sorted({int(period) for period in repair_periods})),
        observed=dict(observed or {}),
        expected=dict(expected or {}),
        message=bounded_finding_message(message),
        clearable_by=rule.clearable_by,
    )


def apply_declinations(
    findings: Iterable[Finding],
    declinations: Iterable[Declination],
) -> tuple[Finding, ...]:
    """Attach each warning's first answering declination; report the rest.

    The one place a declination clears a finding, applied where the finding
    list is assembled so a dry-run check and publish clear identically. An
    error is never answered; a declination that answers no finding becomes
    the ``info`` finding ``declination_unmatched``, which names the finding of
    the declination's code and item that no declination can clear, with the
    route that does (``clearable_by``), when there is one.
    """

    findings = tuple(findings)
    pending = tuple(declinations)
    answered: set[str] = set()
    result: list[Finding] = []
    for finding in findings:
        answering = [candidate for candidate in pending if candidate.answers(finding)]
        if not answering:
            result.append(finding)
            continue
        # Every answering declination is matched; the first is attached.
        answered.update(candidate.declination_id for candidate in answering)
        result.append(finding.model_copy(update={"declination": answering[0]}))
    for declination in pending:
        if declination.declination_id in answered:
            continue
        observed: dict[str, FindingScalar] = {
            "declination_id": declination.declination_id,
            "code": declination.code,
        }
        message = (
            f"declination {declination.declination_id} answers no "
            f"{declination.code} warning of this build"
        )
        # A finding of the declination's own code that no declination can clear
        # (an error, or a warning cleared by repair): the declination did not
        # clear it, and the finding says what does.
        held = next(
            (
                finding
                for finding in findings
                if finding.code == declination.code
                and finding.item_id == declination.item_id
                and not (finding.severity == "warning" and finding.clearable_by == "declination")
            ),
            None,
        )
        if held is not None:
            observed["severity"] = held.severity
            observed["clearable_by"] = held.clearable_by
            message += (
                f": the {held.code} finding on {held.item_id} is {held.severity} "
                f"(clearable_by {held.clearable_by}), which a declination never clears"
            )
            if held.clearable_by == "repair":
                message += (
                    "; repair it — the root_causes repair_call names the door"
                    + (
                        ", and in model_document that is clear_input, a cited set_input or a set_formula"
                        if declination.author.kind == "agent"
                        else ""
                    )
                )
        result.append(
            make_finding(
                "declination_unmatched",
                message=message,
                item_id=declination.item_id,
                periods=declination.periods,
                observed=observed,
            )
        )
    return tuple(result)


_SEVERITY_RANK: Final = {"error": 0, "warning": 1, "info": 2}
_BASIS_RANK: Final = {"rendered": 0, "pinned": 1, "authored": 2, "semantic": 3}


def collapse_findings(findings: Iterable[Finding]) -> tuple[Finding, ...]:
    """One finding per (code, item, periods, repair owner), in a deterministic order.

    When the same identity was evaluated on more than one basis the rendered
    one is kept (spec §4.3). The owner is part of the identity: two roots of the
    same missing cells can need a binding and an engine repair, and collapsing
    one into the other would drop a repair.
    """

    kept: dict[tuple[str, str | None, tuple[int, ...], str | None], Finding] = {}
    for finding in findings:
        identity = (
            finding.code,
            finding.item_id,
            finding.periods,
            str(finding.observed.get("repair_owner") or "") or None,
        )
        current = kept.get(identity)
        if current is None or _BASIS_RANK[finding.basis] < _BASIS_RANK[current.basis]:
            kept[identity] = finding
    return tuple(
        sorted(
            kept.values(),
            key=lambda finding: (
                _SEVERITY_RANK[finding.severity],
                finding.code,
                finding.item_id or "",
                finding.periods,
                finding.message,
            ),
        )
    )


def _semantic_value_roots(
    item_id: str,
    period: int,
    values: Mapping[str, Mapping[int, Any]],
) -> list[tuple[str, int, str | None]]:
    """Follow typed causes to their leaves, preserving the first cause's order."""

    roots: dict[tuple[str, int], str | None] = {}

    def visit(
        cell: tuple[str, int],
        reason: str | None,
        path: frozenset[tuple[str, int]],
        depth: int,
    ) -> None:
        value = values.get(cell[0], {}).get(cell[1])
        causes = getattr(value, "causes", ())
        if (
            cell in path
            or depth >= 32
            or getattr(value, "state", None) == "known"
            or not causes
        ):
            roots.setdefault(cell, reason)
            return
        path = path | {cell}
        for cause in causes:
            fiscal_year = getattr(getattr(cause, "period", None), "fiscal_year", None)
            if fiscal_year is None:
                roots.setdefault(cell, reason)
                continue
            visit(
                (cause.node_id, int(fiscal_year)),
                cause.reason_code,
                path,
                depth + 1,
            )

    value = values.get(item_id, {}).get(period)
    visit((item_id, period), getattr(value, "primary_reason", None), frozenset(), 0)
    return [(node_id, year, reason) for (node_id, year), reason in roots.items()]


def fold_semantic_value_findings(
    findings: Sequence[Finding],
    values: Mapping[str, Mapping[int, Any]],
    *,
    root_cells: Collection[tuple[str, int]],
) -> list[Finding]:
    """Attach missing-value cascades to missing-operand findings on their roots.

    Fold the canonical findings after all evaluation bases have been gathered.
    Metadata, identity and judgment findings do not report a missing operand.
    A root without its own missing-operand finding remains visible. Partial-period
    and mixed root coverage retain the unresolved semantic finding rather than
    hiding a missing operand. Typed causes carry selected-branch and period identity.
    ``root_cells`` is the shared compact render-input domain, not every owned
    blank slot: a root the published provenance omits cannot absorb on check.
    A ``business_model_node_unresolved`` finding carries no periods: it is the root of each
    root cell of the compiled line it names (``observed["compiled_item_id"]``) whose typed
    reason is ``business_model_node_unresolved`` (stamped on the node's unwritten historical
    cells by ``target_build.mark_missing_inputs``).
    """

    by_cell: dict[tuple[str, int], list[int]] = {}
    unresolved_node_lines: dict[str, list[int]] = {}
    for index, finding in enumerate(findings):
        if finding.item_id is not None and finding.code in (
            "missing_source",
            "semantic_value_not_known",
            "required_value_unavailable",
            "schedule_input_missing",
            "formula_input_missing",
            "projected_balance_sheet_inputs_missing",
            "projected_cash_roll_forward_inputs_missing",
            "projected_schedule_roll_forward_inputs_missing",
            "business_model_node_unresolved",
        ):
            for period in finding.periods:
                if (finding.item_id, period) in root_cells:
                    by_cell.setdefault((finding.item_id, period), []).append(index)
        compiled = (
            finding.observed.get("compiled_item_id")
            if finding.code == "business_model_node_unresolved"
            else None
        )
        if isinstance(compiled, str):
            unresolved_node_lines.setdefault(compiled, []).append(index)
    affected: dict[int, set[tuple[str, int]]] = {}
    retained: dict[int, tuple[int, ...]] = {}
    surviving_roots: dict[int, list[tuple[str, int, str | None]]] = {}
    first_unfolded_root: dict[int, tuple[str, int, str | None]] = {}
    for index, finding in enumerate(findings):
        if finding.code != "semantic_value_not_known" or finding.item_id is None:
            continue
        remaining: list[int] = []
        for period in finding.periods:
            value = values.get(finding.item_id, {}).get(period)
            roots = _semantic_value_roots(finding.item_id, period, values)
            targets: set[int] = set()
            uncovered: list[tuple[str, int, str | None]] = []
            foldable = (
                bool(getattr(value, "causes", ()))
                and getattr(value, "state", None) == "missing"
                and not finding.observed.get("operand_roles")
            )
            for root in roots:
                candidates = [
                    candidate
                    for candidate in (
                        *by_cell.get(root[:2], ()),
                        *(
                            unresolved_node_lines.get(root[0], ())
                            if root[2] == "business_model_node_unresolved" and root[:2] in root_cells
                            else ()
                        ),
                    )
                    if candidate != index
                ]
                if not candidates or not foldable:
                    uncovered.append(root)
                    continue
                targets.add(min(
                    candidates,
                    key=lambda candidate: (
                        findings[candidate].code == "semantic_value_not_known",
                        candidate,
                    ),
                ))
            if foldable and not uncovered:
                for target in targets:
                    affected.setdefault(target, set()).add((finding.item_id, period))
            else:
                remaining.append(period)
                surviving_roots.setdefault(index, []).extend(roots)
                first_unfolded_root.setdefault(index, (uncovered or roots)[0])
        retained[index] = tuple(remaining)
    result: list[Finding] = []
    for index, finding in enumerate(findings):
        periods = retained.get(index, finding.periods)
        if index in retained and not periods:
            continue
        observed = finding.observed
        message = finding.message
        if periods != finding.periods:
            value = values[cast(str, finding.item_id)][periods[0]]
            observed = {
                **observed,
                "state": value.state,
                "reason": value.primary_reason,
            }
            message = (
                f"{len(periods)} required semantic value(s) of {finding.item_id} are "
                f"non-known; first is {value.state} ({value.primary_reason})."
            )
        # An owed readout (``operand_roles``) already names its cause and next
        # call; root attribution describes a cascade, not a correspondence debt.
        if index in surviving_roots and not finding.observed.get("operand_roles"):
            roots = surviving_roots[index]
            root_item_id, root_period, root_reason = first_unfolded_root[index]
            cells = list(dict.fromkeys((item_id, year) for item_id, year, _reason in roots))
            observed = {
                **observed,
                "root_item_id": root_item_id,
                "root_period": root_period,
                "root_reason_code": root_reason,
                "root_cause_count": len(cells),
                "root_cells": json.dumps(cells[:20]),
            }
            message = bounded_finding_message(
                f"root {root_item_id} FY{root_period} ({root_reason}). ", message
            )
        if index in affected:
            cells = sorted(affected[index])
            rows = sorted({item_id for item_id, _period in cells})
            observed = {
                **observed,
                "affected_rows": json.dumps(rows),
                "affected_cells": json.dumps(cells),
            }
            message = bounded_finding_message(
                message, f" Affected rows: {', '.join(rows)}."
            )
        result.append(finding.model_copy(update={
            "periods": periods, "observed": observed, "message": message,
        }))
    return result


_FISCAL_PERIOD_RE: Final = re.compile(r"^FY(?P<year>\d{4})$")
#: Legacy fallback for bundles persisted before ``BuildDiagnostic.observed``: the
#: evidenced-absence form (``disposition=<x>``) and the evidence-audit form
#: (``… is <x>; downstream capability …``). Read only when no group row carries
#: ``disposition`` or ``disposition_by_period`` in ``observed``.
_DISPOSITION_RE: Final = re.compile(
    r"disposition=(?P<disposition>[a-z_]+)|\bis (?P<audit>[a-z_]+); downstream capability\b"
)
#: Total over ``BuildDiagnosticCode`` (``tests/schema/test_build_findings.py``
#: holds it to the Literal); every value is a ``FINDING_SEVERITY_MAP`` row.
_DIAGNOSTIC_CODE_FINDING: Final[Mapping[str, str]] = MappingProxyType(
    {
        "source_absence_evidenced": "source_absence_evidenced",
        "missing_source": "missing_source",
        "adapter_assertion": "adapter_assertion",
        # An analyst-declared model-spec gap: a section the author could not
        # write from the evidence.
        "incomplete_model_spec": "model_spec_section_unauthored",
        # The company-grounded schedule policy's balance-role refusal, the one
        # compile-stage row that rides the bundle diagnostics.
        "compile_failure": "balance_role_without_driver",
        "non_gaap_bridge_rounding_gap": "non_gaap_bridge_rounding_gap",
        "non_gaap_bridge_mismatch": "non_gaap_bridge_mismatch",
    }
)


def source_diagnostic_findings(
    diagnostics: Sequence["BuildDiagnostic"],
    *,
    basis: FindingBasis,
) -> tuple[Finding, ...]:
    """Map pinned-bundle and provenance diagnostics onto findings.

    Diagnostics that differ only by period collapse into one finding with all
    of their periods: one evidenced absence per role, its disposition kept in
    ``observed``.
    """

    grouped: dict[
        tuple[str, str | None, str],
        tuple[list[int], list[str], list["BuildDiagnostic"]],
    ] = {}
    for diagnostic in diagnostics:
        code = _DIAGNOSTIC_CODE_FINDING[diagnostic.code]
        period_id = diagnostic.period_id
        period_match = (
            _FISCAL_PERIOD_RE.match(period_id) if period_id is not None else None
        )
        message = (
            diagnostic.message.replace(period_id, "{period}")
            if period_id is not None and period_match is not None
            else diagnostic.message
        )
        key = (code, diagnostic.semantic_item_id, message)
        periods, unmatched, rows = grouped.setdefault(key, ([], [], []))
        rows.append(diagnostic)
        if period_match is not None:
            periods.append(int(period_match.group("year")))
        elif period_id is not None:
            unmatched.append(period_id)
    findings: list[Finding] = []
    for (code, item_id, message), (periods, unmatched, rows) in grouped.items():
        observed: dict[str, FindingScalar] = {}
        values_by_key: dict[str, dict[str, FindingScalar]] = {}
        for row in rows:
            for name, value in row.observed.items():
                observed.setdefault(name, value)
                values_by_key.setdefault(name, {}).setdefault(
                    row.period_id or "unbound", value
                )
        for name, values_by_period in values_by_key.items():
            if any(value != observed[name] for value in values_by_period.values()):
                del observed[name]
                observed[f"{name}_by_period"] = json.dumps(
                    dict(list(values_by_period.items())[:12]), sort_keys=True
                )
        # Legacy fallback for bundles persisted before BuildDiagnostic.observed: the
        # prose is read only when every row of the group carries no observed data at
        # all. A current producer's row (a disposition, or a pin-window gap's
        # gap_kind) is the data; its prose is never parsed.
        if not any(row.observed for row in rows):
            disposition = _DISPOSITION_RE.search(message)
            if disposition is not None:
                observed["disposition"] = (
                    disposition.group("disposition") or disposition.group("audit")
                )
        if unmatched:
            observed["period_ids"] = ", ".join(sorted(set(unmatched)))
        period_text = ", ".join(f"FY{year}" for year in sorted(set(periods)))
        findings.append(
            make_finding(
                code,
                basis=(
                    basis
                    if code
                    in {
                        "missing_source",
                        "non_gaap_bridge_rounding_gap",
                        "non_gaap_bridge_mismatch",
                    }
                    else None
                ),
                message=bounded_finding_message(
                    "", message.replace("{period}", period_text or "the period")
                ),
                item_id=item_id,
                periods=periods,
                observed=observed,
            )
        )
    return tuple(findings)


@dataclass(frozen=True, slots=True)
class CompileStageEvidence:
    """What compile computed that the evaluator reads before render.

    Every field is compile output for the same model being validated; the
    evaluator reads it, never recomputes it.
    """

    projection_readiness: Any
    model_quality_readiness: Any
    scenario_bridge_readiness: Any
    scenario_results: Mapping[str, object]
    valuation_input_readiness: Any
    schedule_input_audit: Any
    diagnostic_report: object | None
    required_unavailable: frozenset[tuple[str, int]]
    #: What the compile reported instead of raising: business-model series
    #: coverage and node history, required lines, statement windows and
    #: forecast assumptions (`CompiledModelIntermediate.compile_findings`).
    compile_findings: tuple[Finding, ...] = ()
    #: The bound judgment artifacts' revision findings
    #: (`judgment_revision_findings`), computed by the build lifecycle.
    judgment_findings: tuple[Finding, ...] = ()
    #: Transient typed document calculation; publication validates the same
    #: optional-absence semantics without recomputing the unlowered model.
    graph_values: Mapping[str, Mapping[int, DependencyGraphNumberValue]] | None = None
    #: Explicit document gaps the renderer writes even without a stored value.
    #: Shared by graph and publication when selecting compact cascade roots.
    render_unavailable: frozenset[tuple[str, int]] = frozenset()


def blocking_readiness_issues(
    *,
    projection_readiness: Any,
    model_quality_readiness: Any,
    scenario_bridge_readiness: Any,
    scenario_results: Mapping[str, object],
    projection_periods: Sequence[int],
) -> list[Any]:
    """The readiness issues a build reports: every ``blocking`` one.

    The same selection marks their missing cells required-unavailable in the
    render and makes them warning findings.
    """

    from .model_readiness_quality import _scenario_output_direction_issues

    issues: list[Any] = [*projection_readiness.issues]
    for domain_name in ("share_count", "working_capital", "valuation"):
        domain = model_quality_readiness.domains.get(domain_name)
        if domain is not None:
            issues.extend(domain.issues)
    if scenario_bridge_readiness.owners:
        issues.extend(scenario_bridge_readiness.issues)
        issues.extend(
            _scenario_output_direction_issues(
                # scenario_results is the {case: {"status", "outputs"}}
                # envelope; the direction checker reads the flat
                # {case: {output: {period: value}}} map it wraps.
                {
                    str(case): result["outputs"]
                    for case, result in scenario_results.items()
                    if isinstance(result, dict)
                    and isinstance(result.get("outputs"), dict)
                },
                list(projection_periods),
            )
        )
    segment_basis = model_quality_readiness.domains.get("segment_basis")
    if segment_basis is not None:
        issues.extend(segment_basis.issues)
    return [
        issue for issue in issues if getattr(issue, "severity", None) == "blocking"
    ]


def _label(model: "FinancialModel", item_id: str | None, fallback: str) -> str:
    item = model._index.get(str(item_id or ""))
    label = str(getattr(item, "label", "") or "").strip()
    return label or fallback


def _readiness_findings(
    evidence: CompileStageEvidence,
    model: "FinancialModel",
) -> list[Finding]:
    findings: list[Finding] = []
    for issue in blocking_readiness_issues(
        projection_readiness=evidence.projection_readiness,
        model_quality_readiness=evidence.model_quality_readiness,
        scenario_bridge_readiness=evidence.scenario_bridge_readiness,
        scenario_results=evidence.scenario_results,
        projection_periods=tuple(model.time_structure.projection_periods),
    ):
        item_id = getattr(issue, "item_id", None) or getattr(issue, "owner_id", None)
        fix = getattr(issue, "fix", None)
        findings.append(
            make_finding(
                str(issue.code),
                message=str(fix or issue.detail),
                item_id=item_id,
                periods=tuple(getattr(issue, "missing_periods", ()) or ()),
                operands=tuple(getattr(issue, "related_item_ids", ()) or ()),
                observed={"detail": bounded_finding_message(str(issue.detail))},
            )
        )
    return findings


def _unit_format_findings(model: "FinancialModel") -> list[Finding]:
    from .models import ItemType

    findings: list[Finding] = []
    for sheet_name, sheet in model.sheets.items():
        for section in sheet.sections:
            for item in section.line_items:
                style_format = (
                    item.style.number_format if item.style is not None else None
                )
                if (
                    (
                        item.historical is not None
                        or item.projected is not None
                        or item.overrides
                    )
                    and item.unit is None
                    and not str(item.format or "").strip()
                    and not str(style_format or "").strip()
                    and item.item_type not in {ItemType.header, ItemType.spacer}
                    and item.id not in WORKBOOK_PRESENTATION_CAPTION_ITEM_IDS
                ):
                    findings.append(
                        make_finding(
                            "unit_format_metadata_missing",
                            message=(
                                f"Add unit or explicit number-format metadata for "
                                f"{item.label} on {sheet_name}, then rebuild."
                            ),
                            item_id=item.id,
                        )
                    )
    return findings


def _interest_expense_not_separable_findings(model: "FinancialModel") -> list[Finding]:
    """One error per unlevered DCF whose debt years report no interest."""

    from .build_financial_schedules import (
        _OTHER_INCOME_ITEM_ID,
        _REPORTED_INTEREST_EXPENSE_ITEM_ID,
        _UNLEVERED_FREE_CASH_FLOW_ITEM_ID,
        unseparable_interest_periods,
    )

    unexplained = unseparable_interest_periods(model)
    if not unexplained:
        return []
    return [
        make_finding(
            "interest_expense_not_separable",
            message=(
                f"{_REPORTED_INTEREST_EXPENSE_ITEM_ID} reports no interest in a "
                "period this issuer carried debt, so its interest is inside "
                f"{_OTHER_INCOME_ITEM_ID} and {_UNLEVERED_FREE_CASH_FLOW_ITEM_ID} "
                "adds none back: the DCF would discount a levered strip."
            ),
            item_id=_REPORTED_INTEREST_EXPENSE_ITEM_ID,
            periods=tuple(int(row["period"]) for row in unexplained),
            operands=(_OTHER_INCOME_ITEM_ID, _UNLEVERED_FREE_CASH_FLOW_ITEM_ID),
            observed={
                f"average_debt_FY{int(row['period'])}": row["average_debt"]
                for row in unexplained
            },
        )
    ]


def _control_hint(
    item_id: str | None,
    controls_by_item: Mapping[str, tuple[str, ...]] | None = None,
) -> tuple[str, dict[str, FindingScalar]]:
    """The message suffix and ``observed`` entry naming the control that answers a row.

    The profile (``schema/forecast_control_profile.py``) owns which control
    lowers onto which template row; a row no control writes names none.
    """

    if controls_by_item is None:
        from .forecast_control_profile import forecast_control_ids_by_target_item

        controls_by_item = forecast_control_ids_by_target_item()
    controls = ", ".join(controls_by_item.get(str(item_id), ()))
    if not controls:
        return "", {}
    return f" Author forecast control {controls} to resolve it.", {"control": controls}


def _required_unavailable_findings(
    evidence: CompileStageEvidence,
    model: "FinancialModel",
    controls_by_item: Mapping[str, tuple[str, ...]] | None = None,
) -> list[Finding]:
    if controls_by_item is None:
        from .forecast_control_profile import forecast_control_ids_by_target_item

        controls_by_item = forecast_control_ids_by_target_item(template=model)
    # ``evidence.required_unavailable`` is the set the renderer writes ``=NA()`` for;
    # ``renderer_annual_context._annual_render_context`` filters it to owned cells
    # (``renderer_columns.item_owns_cell``), so a finding names exactly what the workbook shows.
    # A cell the typed graph reads as an evidenced absence is intentional, not
    # a required value that is unavailable, so it names no finding.
    graph_values = getattr(evidence, "graph_values", None)
    by_item: dict[str, list[int]] = {}
    for item_id, period in sorted(evidence.required_unavailable):
        if graph_values is not None:
            typed_value = graph_values.get(item_id, {}).get(int(period))
            if typed_value is not None and typed_value.state == "evidenced_absent":
                continue
        by_item.setdefault(item_id, []).append(int(period))
    return [
        make_finding(
            "required_value_unavailable",
            message=(
                f"{_label(model, item_id, 'Model output')}: the required compiled "
                f"value is unavailable for "
                f"{', '.join(f'FY{period}' for period in periods)}; the workbook "
                f"displays #N/A.{_control_hint(item_id, controls_by_item)[0]}"
            ),
            item_id=item_id,
            periods=periods,
            observed=_control_hint(item_id, controls_by_item)[1],
        )
        for item_id, periods in by_item.items()
    ]


def _scenario_result_findings(evidence: CompileStageEvidence) -> list[Finding]:
    return [
        make_finding(
            "scenario_result_blocked",
            message=(
                f"{str(case).title()} scenario outputs are blocked by missing "
                "required model inputs."
            ),
            observed={"scenario": str(case)},
        )
        for case, result in evidence.scenario_results.items()
        if isinstance(result, dict) and result.get("status") == "blocked"
    ]


def _schedule_input_findings(
    evidence: CompileStageEvidence,
    controls_by_item: Mapping[str, tuple[str, ...]] | None = None,
) -> list[Finding]:
    return [
        make_finding(
            "schedule_input_missing",
            message=(
                f"Required schedule input {row.public_label} is unresolved for "
                f"{', '.join(f'FY{period}' for period in row.missing_periods)}."
                f"{_control_hint(row.row_id, controls_by_item)[0]}"
            ),
            item_id=row.row_id,
            periods=tuple(row.missing_periods),
            observed={"family": str(row.family), **_control_hint(row.row_id, controls_by_item)[1]},
        )
        for row in evidence.schedule_input_audit.rows
        if row.state == "missing"
    ]


def _valuation_input_flag_findings(evidence: CompileStageEvidence) -> list[Finding]:
    return [
        make_finding(
            str(flag.code),
            message=str(flag.message or flag.code),
            observed={"flag_severity": str(flag.severity)},
        )
        for flag in evidence.valuation_input_readiness.flags
    ]


def _house_value_text(item: "LineItem", value: float) -> str:
    from industry_slice_core.industry_types import Unit

    if item.unit is Unit.percentage:
        return f"{value * 100:.4g}%"
    return f"{value:,.6g}"


def default_value_findings(model: "FinancialModel") -> tuple[Finding, ...]:
    """One ``judgment_absent_default_used`` per item holding a ``default`` value.

    A writer tags a compiler house value ``ValueProvenance.default`` with its
    strategy string as the cell note; the finding names the item, the
    defaulted periods, the house value written there and each strategy.
    """

    from .models import ValueProvenance

    findings: list[Finding] = []
    for sheet in model.sheets.values():
        for section in sheet.sections:
            for item in section.line_items:
                if item.values is None:
                    continue
                defaulted = {
                    int(period): cell
                    for period, cell in item.values.values.items()
                    if cell.provenance == ValueProvenance.default
                }
                if not defaulted:
                    continue
                strategies = tuple(
                    dict.fromkeys(
                        cell.note
                        for _period, cell in sorted(defaulted.items())
                        if cell.note
                    )
                )
                house_values = tuple(
                    dict.fromkeys(
                        float(cell.value)
                        for _period, cell in sorted(defaulted.items())
                        if cell.value is not None
                    )
                )
                if len(house_values) == 1:
                    house_value = _house_value_text(item, house_values[0])
                else:
                    house_value = ", ".join(
                        f"FY{period} {_house_value_text(item, float(cell.value))}"
                        for period, cell in sorted(defaulted.items())
                        if cell.value is not None
                    )
                findings.append(
                    make_finding(
                        "judgment_absent_default_used",
                        message=(
                            f"{item.label.strip() or item.id} uses the house "
                            f"value {house_value} "
                            f"({', '.join(strategies) or 'unnamed'}): no "
                            "authored forecast-assumption or valuation-policy row "
                            "exists. Author the row, or decline naming the "
                            "accepted house convention."
                        ),
                        item_id=item.id,
                        periods=tuple(defaulted),
                        operands=(item.id, *strategies),
                        observed=(
                            {"house_value": house_values[0]}
                            if len(house_values) == 1
                            else None
                        ),
                    )
                )
    return tuple(findings)


def _judgment_artifact_id(artifact: "JudgmentArtifact") -> str:
    from .business_model_revision import BusinessModelRevision
    from .historical_foundation import HistoricalFoundation
    from .valuation_policy import ValuationPolicyArtifact

    if isinstance(artifact, BusinessModelRevision):
        return artifact.ref.revision
    if isinstance(artifact, HistoricalFoundation):
        return artifact.ref.artifact_id
    if isinstance(artifact, ValuationPolicyArtifact):
        return artifact.ref.policy_id
    return artifact.ref.set_id


def _retained_citation_ids(
    artifact: "ForecastAssumptionSet | HistoricalFoundation | BusinessModelRevision",
) -> set[str]:
    """The ids an artifact's own rows may cite because it retains them."""

    from .forecast_assumption_set import ForecastAssumptionSet
    from .historical_foundation import HistoricalFoundation

    if isinstance(artifact, ForecastAssumptionSet):
        return {
            *(row.evidence_id for row in artifact.forecast_evidence),
            *artifact.inherited_business_model_evidence_ids,
        }
    if isinstance(artifact, HistoricalFoundation):
        return {
            *(row.fact_id for row in artifact.historical_basis_facts),
            *(row.evidence_id for row in artifact.evidence),
            *(row.evidence_id for row in artifact.judgment_evidence),
        }
    return set()


def judgment_revision_findings(
    successor: "JudgmentArtifact",
    predecessor: "JudgmentArtifact | None" = None,
    *,
    resolvable_ids: Iterable[str] = (),
    derived_view_ids: Iterable[str] = (),
) -> tuple[Finding, ...]:
    """The R7 judgment-revision findings on one judgment artifact (W4a).

    ``predecessor`` is the artifact ``successor.predecessor_ref`` names, loaded
    by the caller. On an artifact carrying supersessions (FAS, HFF, BMR) a
    predecessor row the successor changes or drops without a supersession is
    ``supersession_missing``. A supersession without its reason (or, for an
    Actual, its noise-or-signal reading), and a FAS row not held at base
    without its rationale, extension basis or per-period confidence, is
    ``judgment_reason_missing``. A cited id — a FAS row's evidence ids, a
    supersession's trigger ref other than re-thinking's, which names the
    Finding or view it answers — that is in ``derived_view_ids`` is
    ``judgment_cites_derived_view``; one neither the artifact retains nor
    ``resolvable_ids`` names, or an Actual, Evidence or Claim trigger with no
    ref, is ``judgment_citation_unresolved``. An HFF also reports its
    seed-source gaps (:func:`historical_seed_source_findings`) and its
    unresolved forecast controls
    (:func:`historical_forecast_control_unresolved_findings`). Reports only;
    never authors.
    """

    from .business_model_revision import BusinessModelRevision
    from .forecast_assumption_set import ForecastAssumptionSet
    from .historical_foundation import HistoricalFoundation
    from .judgment_history import artifact_supersessions, judgment_rows, same_judgment

    if not isinstance(
        successor, (ForecastAssumptionSet, HistoricalFoundation, BusinessModelRevision)
    ):
        return ()
    successor_id = _judgment_artifact_id(successor)
    supersessions = artifact_supersessions(successor)
    resolvable = {*resolvable_ids, *_retained_citation_ids(successor)}
    derived = set(derived_view_ids)
    missing_reason: dict[str, tuple[set[int], list[str]]] = {}
    unresolved: dict[str, tuple[set[int], set[str]]] = {}
    derived_cited: dict[str, tuple[set[int], set[str]]] = {}

    def cite(item: str, citation: str, periods: Iterable[int]) -> None:
        if citation in derived:
            target = derived_cited
        elif citation in resolvable:
            return
        else:
            target = unresolved
        years, ids = target.setdefault(item, (set(), set()))
        years.update(periods)
        ids.add(citation)

    findings: list[Finding] = []
    if predecessor is not None:
        predecessor_id = _judgment_artifact_id(predecessor)
        current = judgment_rows(successor)
        prior_rows = judgment_rows(predecessor)
        for key in sorted(prior_rows):
            prior = prior_rows[key]
            row = current.get(key)
            if key in supersessions or same_judgment(prior, row):
                continue
            change = "dropped" if row is None else "changed"
            findings.append(
                make_finding(
                    "supersession_missing",
                    message=(
                        f"{successor_id} {change} {key} from {predecessor_id} "
                        "without a supersession. Record the trigger and reason "
                        "on the successor, or decline."
                    ),
                    item_id=key,
                    observed={
                        "successor": successor_id,
                        "predecessor": predecessor_id,
                        "change": change,
                    },
                )
            )

    for prior_row, supersession in supersessions.items():
        parts = [
            part
            for part, absent in (
                ("supersession reason", supersession.reason is None),
                (
                    "noise-or-signal reading",
                    supersession.trigger == "actual" and supersession.reading is None,
                ),
            )
            if absent
        ]
        if parts:
            missing_reason.setdefault(prior_row, (set(), []))[1].extend(parts)
        if supersession.trigger == "re-thinking":
            continue
        if supersession.trigger_ref is None:
            unresolved.setdefault(prior_row, (set(), set()))[1].add(
                f"{supersession.trigger} trigger without trigger_ref"
            )
        else:
            cite(prior_row, supersession.trigger_ref, ())

    if isinstance(successor, ForecastAssumptionSet):
        for row in successor.driver_assumptions:
            assumption = row.assumption
            valued = tuple(assumption.period_values or {})
            if row.classification != "held_at_base":
                parts = [
                    part
                    for part, present in (
                        ("rationale", bool(assumption.rationale)),
                        (
                            "extension_basis_by_period",
                            bool(assumption.extension_basis_by_period),
                        ),
                        ("confidence_by_period", bool(assumption.confidence_by_period)),
                    )
                    if not present
                ]
                if parts:
                    years, missing = missing_reason.setdefault(
                        assumption.driver, (set(), [])
                    )
                    years.update(valued)
                    missing.extend(parts)
            for citation in assumption.evidence_ids:
                cite(assumption.driver, citation, ())
            for year, citations in row.evidence_ids_by_period.items():
                for citation in citations:
                    cite(assumption.driver, citation, (year,))

    for item, (years, parts) in sorted(missing_reason.items()):
        missing_text = ", ".join(parts)
        findings.append(
            make_finding(
                "judgment_reason_missing",
                message=(
                    f"{item} on {successor_id} has no {missing_text}. Author it on "
                    "a successor, or decline."
                ),
                item_id=item,
                periods=years,
                observed={"artifact": successor_id, "missing": missing_text},
            )
        )
    for code, cited, detail in (
        (
            "judgment_cites_derived_view",
            derived_cited,
            "a derived view: cite the evidence it derives from",
        ),
        (
            "judgment_citation_unresolved",
            unresolved,
            "which resolves to nothing the build retains: cite a resolvable id",
        ),
    ):
        for item, (years, ids) in sorted(cited.items()):
            citations = ", ".join(sorted(ids))
            findings.append(
                make_finding(
                    code,
                    message=(
                        f"{item} on {successor_id} cites {citations}, {detail}, "
                        "or decline."
                    ),
                    item_id=item,
                    periods=years,
                    operands=tuple(sorted(ids)),
                    observed={"artifact": successor_id, "citations": citations},
                )
            )
    if isinstance(successor, HistoricalFoundation):
        findings.extend(historical_seed_source_findings(successor))
        findings.extend(historical_forecast_control_unresolved_findings(successor))
    return tuple(findings)


def historical_seed_source_findings(
    foundation: "HistoricalFoundation",
) -> tuple[Finding, ...]:
    """``historical_seed_source_unresolved`` per seed-source gap the HFF carries.

    HFS records a historical opening seed whose filed source binds no one value
    as a non-blocking ``seed_source_unresolved`` gap on the snapshot's mapping
    plan, which the foundation seals; each is one declinable warning here.
    """

    from .historical_foundation import SEED_SOURCE_UNRESOLVED_GAP_PREFIX

    findings: list[Finding] = []
    for gap in foundation.mapping_plan.gaps:
        prefix, _, rest = gap.key.partition(":")
        if prefix != SEED_SOURCE_UNRESOLVED_GAP_PREFIX:
            continue
        period_id = rest.rpartition(":")[2]
        year = period_id[2:6]
        findings.append(
            make_finding(
                "historical_seed_source_unresolved",
                message=bounded_finding_message(
                    f"{foundation.ref.artifact_id}: ",
                    f"{gap.text} {gap.repair_target or ''} Or decline.".strip(),
                ),
                item_id=gap.key,
                periods=(int(year),) if year.isdigit() else (),
                observed={"artifact": foundation.ref.artifact_id, "gap": gap.key},
            )
        )
    return tuple(findings)


def historical_forecast_control_unresolved_findings(
    foundation: "HistoricalFoundation",
) -> tuple[Finding, ...]:
    """``historical_required_role_unobserved`` per unresolved required control.

    HFS records a required forecast control whose applicability the
    BusinessModel and historical facts leave unknown — the roles that would
    establish it are unobserved — as a non-blocking gap on the snapshot's
    mapping plan, which the foundation seals; the control stays in the
    denominator for the author to answer. Each is one declinable warning.
    """

    from .historical_foundation import FORECAST_CONTROL_UNRESOLVED_GAP_PREFIX

    findings: list[Finding] = []
    for gap in foundation.mapping_plan.gaps:
        prefix, _, driver_key = gap.key.partition(":")
        if prefix != FORECAST_CONTROL_UNRESOLVED_GAP_PREFIX:
            continue
        findings.append(
            make_finding(
                "historical_required_role_unobserved",
                message=bounded_finding_message(
                    f"{foundation.ref.artifact_id}: ",
                    f"{gap.text} {gap.repair_target or ''} Or decline.".strip(),
                ),
                item_id=gap.key,
                observed={
                    "artifact": foundation.ref.artifact_id,
                    "gap": gap.key,
                    "driver_key": driver_key,
                },
            )
        )
    return tuple(findings)


def business_model_validation_findings(
    revision: "BusinessModelRevision",
) -> tuple[Finding, ...]:
    """``business_model_validation_warning`` per warning the revision records.

    The BusinessModel door records its validation warnings on the revision at
    persist (``BusinessModelRevision.validation_warnings``); each is one
    declinable warning on the Summary, never a refusal. The validator's code
    rides ``observed``.
    """

    return tuple(
        make_finding(
            "business_model_validation_warning",
            message=bounded_finding_message(
                f"{revision.ref.revision}: {warning.code}: ",
                f"{warning.message} Repair it on a successor BusinessModel, or decline.",
            ),
            item_id=f"{warning.code}:{warning.subject or revision.ref.business_model_id}",
            observed={
                "artifact": revision.ref.revision,
                "validation_code": warning.code,
            },
        )
        for warning in revision.validation_warnings
    )


_DIAGNOSTIC_KIND_FINDING: Final[Mapping[str, str]] = MappingProxyType(
    {
        "insufficient_inputs": "historical_inputs_insufficient",
        "required_column_absent_from_pin": "historical_required_role_unobserved",
        "operating_cash_unexplained": "operating_cash_unexplained",
        "consolidation_mismatch": "consolidation_mismatch",
        "duplicate_rows": "duplicate_rows",
        "synthetic_zero_propagation": "synthetic_zero_propagation",
        "source_backed_projection_only": "source_backed_projection_only",
    }
)
_WRONG_TAG_FINDING: Final[Mapping[str, str]] = MappingProxyType(
    {
        "bs_balance": "historical_bs_balance_mismatch",
        "is_subtotal_integrity": "historical_is_subtotal_mismatch",
        "cf_reconciliation": "historical_cash_roll_mismatch",
    }
)
#: Report sections that carry no finding: the role-population aggregate.
_DIAGNOSTIC_NEUTRAL_CHECKS: Final = frozenset({"coverage_summary"})
_YEAR_KEY_RE: Final = re.compile(r"^\d{4}$")


def diagnostic_report_payload(report: object | None) -> dict[str, Any] | None:
    """The raw historical diagnostic report as the JSON object the report keeps."""

    from dataclasses import asdict, is_dataclass

    if report is None:
        return None
    if isinstance(report, Mapping):
        return dict(report)
    if is_dataclass(report) and not isinstance(report, type):
        return asdict(report)
    raise TypeError("diagnostic_report must be a mapping or dataclass")


def _diagnostic_finding_code(check: str, node: Mapping[str, Any]) -> str | None:
    kind = node.get("kind") or node.get("status")
    if kind == "wrong_tag_suspected":
        return _WRONG_TAG_FINDING.get(check)
    if isinstance(kind, str) and kind in _DIAGNOSTIC_KIND_FINDING:
        return _DIAGNOSTIC_KIND_FINDING[kind]
    if check == "bs_subline_reconciliation" and kind is None:
        return "historical_bs_subline_gap"
    if check == "cross_source_validation":
        return "cross_source_gap"
    return None


def diagnostic_report_findings(report: object | None) -> tuple[Finding, ...]:
    """Every non-ok historical diagnostic node, one finding per code and item."""

    payload = diagnostic_report_payload(report)
    if payload is None:
        return ()
    grouped: dict[tuple[str, str | None], dict[str, Any]] = {}

    def visit(check: str, node: object, years: tuple[int, ...], role: str | None) -> None:
        if isinstance(node, Mapping):
            severity = node.get("severity")
            if isinstance(severity, str) and severity != "ok":
                code = _diagnostic_finding_code(check, node)
                if code is None:
                    _LOGGER.warning(
                        "historical diagnostic %s node kind=%r severity=%r has no "
                        "finding code",
                        check,
                        node.get("kind"),
                        severity,
                    )
                else:
                    node_years = node.get("years")
                    periods = (
                        tuple(int(year) for year in node_years)
                        if isinstance(node_years, list)
                        and all(isinstance(year, int) for year in node_years)
                        else years
                    )
                    if isinstance(node.get("year"), int):
                        periods = (int(node["year"]),)
                    required_role = node.get("required_role")
                    item_id = node.get("template_item") or required_role or role
                    entry = grouped.setdefault(
                        (code, None if item_id is None else str(item_id)),
                        {
                            "periods": set(),
                            "operands": set(),
                            "severity": str(severity),
                            "check": check,
                        },
                    )
                    entry["periods"].update(periods)
                    if isinstance(required_role, str):
                        entry["operands"].add(required_role)
                    for ref in node.get("blocking_refs") or node.get("missing_inputs") or ():
                        entry["operands"].add(str(ref))
            for key, value in node.items():
                key_text = str(key)
                if _YEAR_KEY_RE.match(key_text):
                    visit(check, value, (int(key_text),), role)
                elif check == "cross_source_validation" and key_text == "by_role":
                    if isinstance(value, Mapping):
                        for role_id, role_value in value.items():
                            visit(check, role_value, years, str(role_id))
                else:
                    visit(check, value, years, role)
        elif isinstance(node, (list, tuple)):
            for value in node:
                visit(check, value, years, role)

    for check, section in payload.items():
        if str(check) in _DIAGNOSTIC_NEUTRAL_CHECKS:
            continue
        visit(str(check), section, (), None)

    findings: list[Finding] = []
    for (code, item_id), entry in grouped.items():
        periods = sorted(entry["periods"])
        period_text = (
            f" for {', '.join(f'FY{period}' for period in periods)}" if periods else ""
        )
        findings.append(
            make_finding(
                code,
                message=(
                    f"Historical diagnostic {entry['check']} reports "
                    f"{code.replace('_', ' ')} ({entry['severity']}) on "
                    f"{item_id or 'the model'}{period_text}."
                ),
                item_id=item_id,
                periods=periods,
                operands=sorted(entry["operands"]),
                observed={
                    "check": entry["check"],
                    "diagnostic_severity": entry["severity"],
                },
            )
        )
    return tuple(findings)


_FETCH_STAMP_FIELDS = {"retrieved_at", "available_at"}



def _evidence_identity(evidence: "SourceEvidence") -> dict[str, Any]:
    """The fields that identify a piece of evidence: fetch stamps excluded, corpus citations with the
    version they were read from masked (``schema.corpus_citation``)."""
    fields = evidence.model_dump(exclude=_FETCH_STAMP_FIELDS)
    fields["locator"] = mask_corpus_version(str(fields["locator"]))
    return fields


def historical_foundation_evidence_findings(
    source_bundle: "SourceBundle", model_spec: "ModelSpec",
) -> tuple[Finding, ...]:
    """Report divergent copies without replacing the accepted foundation's evidence.

    ``retrieved_at`` and ``available_at`` are provenance of the fetch (``available_at``
    falls back to the retrieval time when the filing carries no acceptance stamp), not
    source identity; every other evidence field remains compared. Keep collection stamps in the finding's audit trail, distinct
    from the current spread revision used by the repair.
    """
    from .historical_spread import HISTORICAL_SPREAD_REVISION

    fact_projection = model_spec.historical_foundation_fact_projection
    if fact_projection is None:
        return ()
    bundle_evidence = {evidence.evidence_id: evidence for evidence in source_bundle.evidence}
    findings: list[Finding] = []

    def stamp(evidence: "SourceEvidence") -> dict[str, Any]:
        payload = evidence.model_dump(mode="json")
        return {
            name: payload[name]
            for name in (
                "source_id", "provider", "retrieved_at", "available_at", "as_of_date", "locator",
            )
        } | {"accession": evidence.line.accession}

    for observation in fact_projection.observations:
        accepted = observation.evidence
        current = bundle_evidence.get(accepted.evidence_id)
        if current is None:
            continue
        accepted_fields = _evidence_identity(accepted)
        current_fields = _evidence_identity(current)
        differing_fields = sorted(
            name for name in accepted_fields.keys() | current_fields.keys()
            if accepted_fields.get(name) != current_fields.get(name)
        )
        if not differing_fields:
            continue
        accepted_stamp = {
            **stamp(accepted),
            "artifact_id": fact_projection.historical_foundation_artifact_id,
            "content_sha256": fact_projection.historical_foundation_content_sha256,
        }
        current_stamp = {
            **stamp(current),
            "collection_retrieved_at": source_bundle.retrieved_at.isoformat(),
        }
        findings.append(make_finding(
            "historical_foundation_evidence_diverges",
            item_id=accepted.evidence_id,
            message=(
                "Accepted historical foundation evidence diverges from the current SourceBundle. "
                "The accepted foundation remains authoritative until re-accepted; refresh its "
                "context, re-project on the current bundle and re-accept with the required judgment."
            ),
            observed={
                "evidence_id": accepted.evidence_id,
                "differing_fields": json.dumps(differing_fields),
                "historical_foundation_provenance": json.dumps(accepted_stamp, sort_keys=True),
                "source_bundle_provenance": json.dumps(current_stamp, sort_keys=True),
            },
            expected={
                "prerequisite_call": "get_historical_foundation_context",
                "prerequisite_arguments": "{}",
                "next_call": "fms_persist_historical_foundation",
                "next_call_arguments": json.dumps({
                    "context_ref": "<context_ref returned by get_historical_foundation_context>",
                    "judgment": {
                        "verdict": "HISTORICAL_FOUNDATION_RESOLVED",
                        "resolutions": [],
                        "historical_driver_seeds": [],
                        "data_gaps": [],
                        "summary": "<the foundation's summary in words>",
                        "recommended_next_action": "<the next action you recommend>",
                    },
                }),
                "historical_spread_revision": HISTORICAL_SPREAD_REVISION,
            },
        ))
    return tuple(findings)


def compile_stage_findings(
    evidence: CompileStageEvidence,
    *,
    model: "FinancialModel",
    template: "FinancialModel | None" = None,
    controls_by_item: Mapping[str, tuple[str, ...]] | None = None,
) -> tuple[Finding, ...]:
    """Findings whose inputs exist before the workbook is rendered."""
    from .forecast_control_profile import forecast_control_ids_by_target_item

    if controls_by_item is None:
        controls_by_item = forecast_control_ids_by_target_item(template=template)

    return (
        *_readiness_findings(evidence, model),
        *_unit_format_findings(model),
        *_interest_expense_not_separable_findings(model),
        *_required_unavailable_findings(evidence, model, controls_by_item),
        *_scenario_result_findings(evidence),
        *_schedule_input_findings(evidence, controls_by_item),
        *_valuation_input_flag_findings(evidence),
        *default_value_findings(model),
        *diagnostic_report_findings(evidence.diagnostic_report),
        *evidence.compile_findings,
        *evidence.judgment_findings,
    )


#: A market or macro valuation input whose pinned observation is older than
#: this many days at build time is reported ``market_input_stale``.
MARKET_INPUT_STALE_AFTER_DAYS: Final = 30
#: The rendered market and macro valuation inputs, by the template item their
#: pinned observation feeds (the provenance projection's market-input rows).
_MARKET_INPUT_LABELS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "tpl.v.current_valuation.stock_price": "Stock price",
        "tpl.v.cost_of_equity.raw_beta": "Raw beta",
        "tpl.v.cost_of_equity.risk_free_rate": "Risk-free rate",
        "tpl.v.cost_of_equity.equity_risk_premium": "Equity risk premium",
    }
)


def forecast_assumption_limitations_findings(model_spec: "ModelSpec") -> tuple[Finding, ...]:
    """Surface unstated forecast-driver limitations without inventing judgment."""

    return collapse_findings(
        make_finding(
            "forecast_assumption_limitations_unstated",
            message=f"Forecast driver {assumption.driver} has no stated limitations.",
            item_id=assumption.driver,
            periods=(
                assumption.period_values
                or (period.fiscal_year for period in model_spec.fiscal_axis.forecast_periods)
            ),
        )
        for assumption in model_spec.forecast_assumptions
        if not (assumption.limitations or "").strip()
    )


def _forecast_driver_uncited_findings(model_spec: "ModelSpec") -> list[Finding]:
    findings: list[Finding] = []
    for assumption in model_spec.forecast_assumptions:
        has_rationale = bool((assumption.rationale or "").strip())
        has_citation = bool(
            assumption.evidence_ids
            or assumption.source_refs
            or any(assumption.source_refs_by_period.values())
        )
        if has_rationale and has_citation:
            continue
        missing = " or ".join(
            part
            for part, present in (
                ("rationale", has_rationale),
                ("evidence id or source reference", has_citation),
            )
            if not present
        )
        label = (assumption.driver_label or "").strip() or assumption.driver
        findings.append(
            make_finding(
                "forecast_driver_uncited",
                message=(
                    f"Forecast driver {label} has no {missing}; its values stand "
                    "uncited."
                ),
                item_id=assumption.driver,
                periods=tuple(assumption.period_values or {}),
                observed={
                    "has_rationale": has_rationale,
                    "evidence_id_count": len(assumption.evidence_ids),
                    "inherited_baseline_id_count": len(
                        assumption.inherited_baseline_ids
                    ),
                    "source_ref_count": len(assumption.source_refs),
                    "evidence_basis": assumption.evidence_basis,
                },
            )
        )
    return findings


def _market_input_stale_findings(
    provenance: "ProvenanceMap",
    validated_at: "datetime",
) -> list[Finding]:
    build_date = validated_at.date()
    cited: dict[tuple[str, str], tuple[Any, str]] = {}
    for cell in provenance.cells:
        if cell.semantic_item_id not in _MARKET_INPUT_LABELS:
            continue
        for ref in cell.valuation_refs:
            cited[(cell.semantic_item_id, ref.observation_id)] = (
                ref.as_of_close,
                ref.provider,
            )
    findings: list[Finding] = []
    for (item_id, observation_id), (as_of, provider) in sorted(cited.items()):
        age_days = (build_date - as_of).days
        if age_days <= MARKET_INPUT_STALE_AFTER_DAYS:
            continue
        findings.append(
            make_finding(
                "market_input_stale",
                message=(
                    f"{_MARKET_INPUT_LABELS[item_id]} cites {observation_id} as of "
                    f"{as_of.isoformat()}, {age_days} days before this build on "
                    f"{build_date.isoformat()} (older than "
                    f"{MARKET_INPUT_STALE_AFTER_DAYS} days)."
                ),
                item_id=item_id,
                operands=(observation_id,),
                observed={
                    "as_of": as_of.isoformat(),
                    "age_days": age_days,
                    "provider": provider,
                },
                expected={"max_age_days": MARKET_INPUT_STALE_AFTER_DAYS},
            )
        )
    return findings


def _critical_factor_unevidenced_findings(model_spec: "ModelSpec") -> list[Finding]:
    binding = model_spec.scenario_set_binding
    if binding is None:
        return []
    return [
        make_finding(
            "critical_factor_unevidenced",
            message=(
                f"Critical factor {factor.factor_key} (driver {factor.driver_key}) "
                f"was selected for ScenarioSet {binding.scenario_set_id} with no "
                "evidence ids."
            ),
            item_id=factor.factor_key,
            operands=(factor.driver_key,),
            observed={"scenario_set_id": binding.scenario_set_id},
        )
        for factor in binding.critical_factors
        if not factor.evidence_ids
    ]


def _scenario_live_coverage_findings(model_spec: "ModelSpec") -> list[Finding]:
    drivers: set[str] = set()
    flexed: set[str] = set()
    binding = model_spec.scenario_set_binding
    if binding is not None and binding.driver_roles:
        # Count FAS drivers (the ScenarioSet's curves), not the compiler
        # controls one driver fans out to (sbc_pct lowers to four rows).
        for row in binding.driver_roles:
            drivers.add(row.driver_key)
            if row.role != "held_at_base":
                flexed.add(row.driver_key)
    else:
        # Scenario reviews authored without a ScenarioSet name their drivers
        # directly.
        for review in model_spec.scenario_reviews:
            for override in review.overrides:
                drivers.add(override.target_control_id)
                if override.direction != "base":
                    flexed.add(override.target_control_id)
    if not drivers:
        return []
    flexed_keys = tuple(sorted(flexed))
    return [
        make_finding(
            "scenario_live_coverage",
            message=(
                f"{len(flexed_keys)} of {len(drivers)} forecast drivers move "
                "between the bull and bear cases"
                + (f" ({', '.join(flexed_keys)})" if flexed_keys else "")
                + f"; issuer profile {model_spec.profile_id} names no swing "
                "factors to compare this count against."
            ),
            operands=flexed_keys,
            observed={
                "flexed_driver_count": len(flexed_keys),
                "driver_count": len(drivers),
                "profile_id": model_spec.profile_id,
            },
        )
    ]


def _valuation_case_order_findings(model_spec: "ModelSpec") -> list[Finding]:
    """``scenario_case_order_inverted`` per valuation case set.

    The bull and bear cases of terminal growth and of the exit multiple
    bracket the base (bull > base > bear), and each relative-method family
    runs its ``case_order`` (``bull_high``: bull > base > bear, ``bull_low``:
    bull < base < bear); a set that does not is a declinable warning, never a
    refusal (V54).
    """

    policy = model_spec.valuation_policy
    case_sets: list[tuple[str, str, Any, bool]] = []
    if policy is not None:
        case_sets.extend(
            (f"valuation_policy.{name}", f"ValuationPolicy {name}", cases, True)
            for name, cases in (
                ("terminal_growth", policy.terminal_growth_cases),
                ("exit_multiple", policy.exit_multiple_cases),
            )
            if cases is not None
        )
    case_sets.extend(
        (
            f"valuation_family_authorizations.{row.method}",
            f"The {row.method} family",
            row,
            row.case_order == "bull_high",
        )
        for row in model_spec.valuation_family_authorizations
    )
    findings: list[Finding] = []
    for item_id, label, cases, bull_high in case_sets:
        ordered = (
            cases.bull > cases.base > cases.bear
            if bull_high
            else cases.bull < cases.base < cases.bear
        )
        if ordered:
            continue
        order = "bull > base > bear" if bull_high else "bull < base < bear"
        findings.append(
            make_finding(
                "scenario_case_order_inverted",
                message=(
                    f"{label} cases are bull {cases.bull}, base {cases.base}, "
                    f"bear {cases.bear}; the cases do not run {order}."
                ),
                item_id=item_id,
                observed={
                    "bull": float(cases.bull),
                    "base": float(cases.base),
                    "bear": float(cases.bear),
                },
                expected={"order": order},
            )
        )
    return findings


def judgment_info_findings(
    *,
    model_spec: "ModelSpec",
    provenance: "ProvenanceMap",
    validated_at: "datetime",
) -> tuple[Finding, ...]:
    """Judgment info flags, case-order and unstated-limitations warnings.

    Reads the accepted FAS projection (``ModelSpec.forecast_assumptions``), the
    ScenarioSet projection (``scenario_set_binding.critical_factors`` and
    ``driver_roles``, else ``scenario_reviews``), the ValuationPolicy terminal
    cases and the rendered market-input lineage. The info flags are shown and
    never in the verdict; case-order and unstated-limitations warnings do not
    refuse publication. The guidance
    comparison waits on guidance reaching the build's inputs
    (``docs/closed/info-findings-guidance-citation-staleness.md``).
    """

    return (
        *_forecast_driver_uncited_findings(model_spec),
        *forecast_assumption_limitations_findings(model_spec),
        *_market_input_stale_findings(provenance, validated_at),
        *_critical_factor_unevidenced_findings(model_spec),
        *_scenario_live_coverage_findings(model_spec),
        *_valuation_case_order_findings(model_spec),
    )


def deliverable_version_findings(
    binding: "DeliverableModelBinding",
    current: "ModelVersionRef | None",
) -> tuple[Finding, ...]:
    """``deliverable_version_stale`` when the pointer has moved past the bound version.

    ``current`` is what the binding's research file's current pointer selects at
    read time (``None``: it selects nothing, so nothing has moved past). A
    warning that rides the read and never refuses it (D2, S8).
    """

    named = binding.model_version
    if current is None or (current.model_id, current.version) == (
        named.model_id,
        named.version,
    ):
        return ()
    return (
        make_finding(
            "deliverable_version_stale",
            message=(
                f"This deliverable was computed from {named.model_id} v{named.version}; "
                f"the current model is {current.model_id} v{current.version}."
            ),
            observed={
                "model_id": named.model_id,
                "version": named.version,
                "research_file_id": binding.research_file_id,
            },
            expected={"model_id": current.model_id, "version": current.version},
        ),
    )


def question_unratified_findings(
    question_ref: "QuestionRef",
    question: "ThesisQuestion | None",
) -> tuple[Finding, ...]:
    """``question_unratified`` while the Question a deliverable answers is unratified.

    ``question`` is the recorded Question ``question_ref`` names, as the Thesis
    holds it now (``None``: the Thesis no longer holds it, so there is nothing to
    report). The deliverable doors stamp the current Question ratified or not and
    record this warning; the artifact read reports it again until the Question is
    ratified. Never a refusal (ruled (a), 2026-10-04).
    """

    if question is None or question.ratified:
        return ()
    return (
        make_finding(
            "question_unratified",
            message=(
                f"This deliverable answers Question {question.question_id}, proposed "
                f"by {question.author} and not yet ratified."
            ),
            observed={
                "question_id": question.question_id,
                "author": question.author,
                "research_file_id": question_ref.research_file_id,
                "ratified": False,
            },
            expected={"ratified": True},
        ),
    )


def retired_plan_driver_findings(
    retired_node_keys: Sequence[str],
    *,
    business_model_revision: str,
) -> tuple[Finding, ...]:
    """``forecast_driver_plan_row_retired`` per stored plan row the denominator retired.

    ``retired_node_keys`` is ``retired_forecast_driver_plan_nodes`` of the
    revision (``api/research/forecast_driver_value_contracts.py``): base-case
    plan nodes the revision's BusinessModel no longer derives. The row derives
    no control and binds to no model row; the warning names the door that
    drops its judgment.
    """

    return tuple(
        make_finding(
            "forecast_driver_plan_row_retired",
            message=(
                f"BusinessModel revision {business_model_revision} still plans "
                f"{node_key}, which the current forecast denominator retired; it "
                "derives no control and binds to no model row. The row lives on "
                "the BusinessModel revision's plan, so a successor "
                "ForecastAssumptionSet cannot drop it: persist a successor "
                "BusinessModel revision without it (fms_persist_business_model), "
                "or decline this warning."
            ),
            item_id=node_key,
            observed={"business_model_revision": business_model_revision},
        )
        for node_key in retired_node_keys
    )


def bear_case_above_spot_findings(
    recalculated_values: Mapping[str, Mapping[int, float]],
    *,
    model_spec: Any | None = None,
) -> list[Finding]:
    """Emit ``bear_case_above_spot`` when the bear scenario valuation exceeds spot price."""

    from decimal import Decimal
    import math

    from .analysis import EXIT_EARNINGS_ITEMS, DcfMethod, ExitEarningsMeasure, dcf_case_prices

    spot_periods = recalculated_values.get("tpl.v.current_valuation.stock_price") or {}
    if not spot_periods:
        return []
    period = min(spot_periods)
    spot_price = float(spot_periods[period])
    if not math.isfinite(spot_price) or spot_price <= 0.0:
        return []

    bear_eps = (recalculated_values.get("tpl.s.valuation_scenarios.bear_eps") or {}).get(period)
    bear_multiple = (
        recalculated_values.get("tpl.s.valuation_scenarios.bear_selected_pe") or {}
    ).get(period)
    bear_periods = (
        recalculated_values.get("tpl.s.valuation_scenarios.bear_pe_valuation")
        or recalculated_values.get("tpl.s.valuation_scenarios.bear_valuation_avg")
        or {}
    )
    item_id = "tpl.s.valuation_scenarios.bear_pe_valuation"
    operands: tuple[str, ...] = (
        "tpl.s.valuation_scenarios.bear_eps",
        "tpl.s.valuation_scenarios.bear_selected_pe",
        "tpl.v.current_valuation.stock_price",
    )
    extra_observed: dict[str, FindingScalar] = {}
    decomp = ""
    bear_valuation: float | None = None
    if bear_periods:
        period = min(bear_periods)
        spot_price = float(spot_periods.get(period, spot_price))
        bear_valuation = float(bear_periods[period])
        if bear_eps is not None and bear_multiple is not None:
            extra_observed["bear_eps"] = round(float(bear_eps), 6)
            extra_observed["bear_multiple"] = round(float(bear_multiple), 6)
            decomp = f" (EPS {float(bear_eps):.2f} × {float(bear_multiple):.2f}x)"
    elif bear_eps is not None and bear_multiple is not None:
        bear_valuation = float(bear_eps) * float(bear_multiple)
        extra_observed["bear_eps"] = round(float(bear_eps), 6)
        extra_observed["bear_multiple"] = round(float(bear_multiple), 6)
        decomp = f" (EPS {float(bear_eps):.2f} × {float(bear_multiple):.2f}x)"
    else:
        policy = None if model_spec is None else getattr(model_spec, "valuation_policy", None)
        displayed = (
            None
            if policy is None or getattr(policy, "summary", None) is None
            else getattr(policy.summary, "displayed_method", None)
        )
        dcf_method: DcfMethod = (
            "dcf_exit_multiple" if displayed == "dcf_exit_multiple" else "dcf_terminal_growth"
        )
        exit_measure: ExitEarningsMeasure = (
            getattr(policy, "exit_earnings_measure", None) or "terminal_fcf"
        )
        pv_cf = (recalculated_values.get("tpl.v.dcf.pv_cash_flows") or {}).get(period)
        term_fcf = (recalculated_values.get("tpl.v.dcf.terminal_year_fcf") or {}).get(period)
        wacc = (recalculated_values.get("tpl.v.dcf.dcf_wacc") or {}).get(period)
        net_debt = (recalculated_values.get("tpl.v.dcf.less_net_debt") or {}).get(period)
        shares = (recalculated_values.get("tpl.v.dcf.shares_outstanding") or {}).get(period)
        disc_periods = recalculated_values.get("tpl.v.dcf.discount_period") or {}
        if dcf_method == "dcf_exit_multiple":
            control_val = (recalculated_values.get("tpl.v.dcf.exit_multiple_bear") or {}).get(period)
            control_item_id = "tpl.v.dcf.exit_multiple_bear"
        else:
            control_val = (recalculated_values.get("tpl.v.dcf.terminal_growth_bear") or {}).get(period)
            control_item_id = "tpl.v.dcf.terminal_growth_bear"
        exit_item_id = EXIT_EARNINGS_ITEMS.get(exit_measure, "tpl.v.dcf.terminal_year_fcf")
        exit_Periods = recalculated_values.get(exit_item_id) or {}
        exit_val = exit_Periods.get(max(exit_Periods)) if exit_Periods else term_fcf
        if (
            pv_cf is not None
            and term_fcf is not None
            and control_val is not None
            and wacc is not None
            and net_debt is not None
            and shares is not None
            and disc_periods
        ):
            disc_exp = float(disc_periods[max(disc_periods)])
            try:
                prices = dcf_case_prices(
                    method=dcf_method,
                    cases={"bear": Decimal(str(control_val))},
                    pv_cash_flows=Decimal(str(pv_cf)),
                    terminal_fcf=Decimal(str(term_fcf)),
                    terminal_discount_period=Decimal(str(disc_exp)),
                    wacc=Decimal(str(wacc)),
                    net_debt=Decimal(str(net_debt)),
                    shares=Decimal(str(shares)),
                    exit_earnings=None if exit_val is None else Decimal(str(exit_val)),
                    exit_earnings_measure=exit_measure,
                )
                bear_valuation = float(prices["bear"])
                item_id = control_item_id
                operands = (
                    control_item_id,
                    "tpl.v.dcf.pv_cash_flows",
                    exit_item_id if dcf_method == "dcf_exit_multiple" else "tpl.v.dcf.terminal_year_fcf",
                    "tpl.v.dcf.dcf_wacc",
                    "tpl.v.dcf.less_net_debt",
                    "tpl.v.dcf.shares_outstanding",
                    "tpl.v.current_valuation.stock_price",
                )
                extra_observed["dcf_method"] = dcf_method
                if dcf_method == "dcf_exit_multiple":
                    extra_observed["exit_multiple_bear"] = round(float(control_val), 6)
                    decomp = f" (DCF exit multiple {float(control_val):.2f}x)"
                else:
                    extra_observed["terminal_growth_bear"] = round(float(control_val), 6)
                    decomp = f" (DCF terminal growth {float(control_val):.2%})"
            except ValueError:
                bear_valuation = None

    if bear_valuation is None or not math.isfinite(bear_valuation) or bear_valuation <= spot_price:
        return []
    implied_discount = (bear_valuation - spot_price) / bear_valuation
    observed: dict[str, FindingScalar] = {
        "bear_valuation": round(bear_valuation, 6),
        "spot_price": round(spot_price, 6),
        "implied_discount": round(implied_discount, 6),
        **extra_observed,
    }
    return [
        make_finding(
            "bear_case_above_spot",
            basis="rendered",
            item_id=item_id,
            periods=[int(period)],
            operands=operands,
            observed=observed,
            message=(
                f"Bear case valuation ({bear_valuation:.2f}{decomp}) exceeds current "
                f"spot price ({spot_price:.2f}, implied discount {implied_discount:.2%}): "
                "the market discounts a worse case than the authored bear (deep "
                "undervaluation) or the bear case is not a genuine downside."
            ),
        )
    ]

def finding_subject_items(
    model_spec: "ModelSpec",
    seed_coverage_manifest: Sequence[Mapping[str, Any]],
) -> dict[str, str]:
    """The model item each non-item finding subject renders at.

    A finding's ``item_id`` (and the declination that selects it) is a model
    item id, a forecast driver key (``forecast_driver_uncited``) or a
    critical-factor key (``critical_factor_unevidenced``). A driver key
    renders at the item its ``model_spec_assumption`` seed-coverage row
    resolved; a factor key renders at its driver's item. A reader that shows
    findings to a user (the Summary) displays these subjects by that item.
    """

    subjects = {
        str(row["canonical_driver_key"]): str(row["resolved_item_id"])
        for row in seed_coverage_manifest
        if row.get("source_kind") == "model_spec_assumption"
        and row.get("canonical_driver_key")
        and row.get("resolved_item_id")
    }
    binding = model_spec.scenario_set_binding
    for factor in () if binding is None else binding.critical_factors:
        driver_item = subjects.get(factor.driver_key)
        if driver_item is not None:
            subjects.setdefault(factor.factor_key, driver_item)
    return subjects


__all__ = [
    "CompileStageEvidence",
    "MARKET_INPUT_STALE_AFTER_DAYS",
    "bounded_finding_message",
    "apply_declinations",
    "bear_case_above_spot_findings",
    "blocking_readiness_issues",
    "collapse_findings",
    "compile_stage_findings",
    "default_value_findings",
    "diagnostic_report_payload",
    "deliverable_version_findings",
    "diagnostic_report_findings",
    "finding_subject_items",
    "fold_semantic_value_findings",
    "historical_forecast_control_unresolved_findings",
    "historical_seed_source_findings",
    "judgment_info_findings",
    "judgment_revision_findings",
    "make_finding",
    "question_unratified_findings",
    "retired_plan_driver_findings",
    "source_diagnostic_findings",
]
