"""Pure historical statement identities and selected-vintage filed comparison."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass
from collections.abc import Collection, Mapping, Sequence
from decimal import Decimal
from typing import Any, Protocol
from value_semantics_core import CONVENTIONS_IR_V1

from .coverage_profile import AnnualSourceRequirement
from schema.provider_port.adapters.edgar.wire.filing_facts_v1 import FilingPresentationCellV1
from .filed_line_unclaimed import (
    CHECKED_STATEMENTS, _amount, _statement_of, filed_lines_unclaimed,
    unclaimed_line_record, unclaimed_line_message,
)
from .historical_foundation import (
    FILED_LINE_DOUBLE_CLAIMED_GAP_PREFIX, FILED_LINE_UNCLAIMED_GAP_PREFIX, FILED_VALUE_DISAGREES_GAP_PREFIX,
    HISTORICAL_BALANCE_SHEET_UNBALANCED_GAP_PREFIX, HISTORICAL_CASH_ROLL_BROKEN_GAP_PREFIX,
    HistoricalFoundationGap, HistoricalFoundationNextCall, HistoricalFiledTieOut,
    HistoricalFiledTieOutPeriod, HistoricalFiledVintageDifference,
)
from .provider_port.contracts import FilingLineRef
from .required_role_catalog import RequiredRoleCatalog, face_lines
from .statement_bindings_v2 import (
    BoundBindingRow, CompositeBindingRow, LineIdentity, StatementBindings,
    _canonical_binding_role, _face_line_identity, _line_identity_of, bound_lines, composition_leaf,
)
from .statement_topology import normalize_topology_role


#: The spread computation's revision. The fact-snapshot input basis keys on
#: it, so a snapshot materialized before the spread existed (or under an
#: older computation) is never reused as if it carried the current spread.
#: Bump when ``evaluate_historical_spread``'s computation, the snapshot's gap
#: derivation, or the shape or identity of a retained fact, evidence row or
#: source changes.
HISTORICAL_SPREAD_REVISION = "historical-spread.v10"

class SpreadVintageCandidate(Protocol):
    @property
    def evidence_id(self) -> str: ...
    @property
    def source_id(self) -> str: ...
    @property
    def line(self) -> FilingLineRef: ...
    @property
    def value(self) -> Decimal: ...
    @property
    def resolution(self) -> Decimal: ...
    @property
    def own(self) -> bool: ...


class PeriodVintagesLike(Protocol):
    @property
    def candidates(self) -> Sequence[SpreadVintageCandidate]: ...
    @property
    def selected(self) -> str | None: ...


_GAP_KEY_LIMIT = next(
    constraint.max_length
    for constraint in HistoricalFoundationGap.model_fields["key"].metadata
    if getattr(constraint, "max_length", None) is not None
)


def _bounded_gap_key(prefix: str, identity: str) -> str:
    """The line identity, cut to ``HistoricalFoundationGap.key``.

    The readable ``prefix:identity`` is the key when it fits. A long
    dimensional qname does not: the key is then the prefix plus a stable
    digest of the full identity, which ``details`` still carries.
    """

    key = f"{prefix}:{identity}"
    if len(key) <= _GAP_KEY_LIMIT:
        return key
    return f"{prefix}:{hashlib.sha256(identity.encode('utf-8')).hexdigest()}"


def _double_claim_gaps(
    bindings: StatementBindings,
    cells_by_accession: Mapping[str, Sequence[FilingPresentationCellV1]],
    catalog: RequiredRoleCatalog,
    modeled_fiscal_years: Collection[int],
    bindings_revision: str | None,
) -> tuple[HistoricalFoundationGap, ...]:
    """A filed line counted in two additive roles of the same statement is double-counting.

    This is the historical accounting face scope (balance sheet and cash flow), not
    non-additive reuse such as share counts or inputs to an aggregate formula. Negative
    composite terms net a line out. Catalog aliases name one position, not two claims.
    Claims belong to their binding accession even when a later filing supplies their
    comparative evidence. Findings never refuse or automatically edit correspondence.
    """
    def identity_of(identity: LineIdentity) -> LineIdentity:
        qname, role, dimensions = identity
        return qname, normalize_topology_role(role), tuple(sorted(dimensions))

    catalog_rows = {row.required_role: row for row in catalog.required_roles}

    def home_role(role: str) -> str:
        role = _canonical_binding_role(role)
        catalog_row = catalog_rows.get(role)
        while catalog_row is not None and catalog_row.alias_of is not None:
            role = catalog_row.alias_of
            catalog_row = catalog_rows.get(role)
        return role

    def additive_statement(role: str, row: BoundBindingRow | CompositeBindingRow) -> str | None:
        catalog_row = catalog_rows.get(home_role(role))
        if catalog_row is not None:
            return catalog_row.statement if (
                catalog_row.parent_total is not None
                and (catalog_row.template_items or catalog_row.population_items or catalog_row.hff_fact_items)
            ) else None
        leaf = composition_leaf(
            role, {} if row.model_item is None else {"model_item": row.model_item},
            ticker=bindings.ticker,
        )
        if leaf is not None:
            for statement in CHECKED_STATEMENTS:
                if leaf[1].startswith(f"tpl.fm.{statement}."):
                    return statement
        return None

    claims: dict[tuple[str, LineIdentity, str], dict[str, BoundBindingRow | CompositeBindingRow]] = {}
    sources: dict[tuple[str, LineIdentity, str], set[str]] = {}
    for window in bindings.vintage_windows:
        for role, row in window.rows.items():
            if not isinstance(row, (BoundBindingRow, CompositeBindingRow)):
                continue
            statement = additive_statement(role, row)
            if statement is None or statement not in CHECKED_STATEMENTS:
                continue
            lines = bound_lines(row)
            signs = row.signs if isinstance(row, CompositeBindingRow) else (1,) * len(lines)
            for line, sign in zip(lines, signs, strict=True):
                if sign < 0 or line.disclosure_plane:
                    continue
                for citation in line.reviewed_occurrences:
                    if citation.accession not in window.accessions:
                        continue
                    key = (citation.accession, identity_of(_line_identity_of(line)), statement)
                    claims.setdefault(key, {})[role] = row
                    sources.setdefault(key, set()).add(citation.vintage_of or citation.accession)

    def source_ref(line: BoundBindingRow) -> dict[str, Any]:
        return {
            "kind": "line_identity", "line_qname": line.line_qname,
            "presentation_role_name": line.presentation_role_name,
            "dimensions": [
                {"axis_qname": dim.axis_qname, "member_qname": dim.member_qname}
                for dim in line.dimensions
            ],
            **({} if line.absence_closure is None else {"absence_closure": {
                "parent_local_name": line.absence_closure.parent_local_name,
                "total_local_name": line.absence_closure.total_local_name,
            }}),
        }

    gaps: list[HistoricalFoundationGap] = []
    for key, rows in sorted(claims.items()):
        accession, identity, statement = key
        if len({home_role(role) for role in rows}) < 2:
            continue
        cells = [
            (source_accession, cell)
            for source_accession in sorted(sources[key])
            for cell in cells_by_accession.get(source_accession, ())
            if (line_identity := _face_line_identity(cell)) is not None
            and _statement_of(cell) == statement
            and identity_of(line_identity) == identity
            and cell.printed_column.period_ref.fiscal_year in modeled_fiscal_years
            and _amount(cell) not in (None, Decimal(0))
        ]
        if not cells:
            continue
        roles = sorted(rows)
        composite_roles = [role for role in roles if isinstance(rows[role], CompositeBindingRow)]
        repair_target = composite_roles[0] if composite_roles else roles[-1]
        row = rows[repair_target]
        qname, _, dimensions = identity
        presentation_role = cells[0][1].presentation.presentation_role_name
        change: dict[str, Any] = {
            "accessions": [accession], "role": repair_target, "state": "bound",
        }
        if row.model_item is not None:
            change["model_item"] = row.model_item
        if isinstance(row, CompositeBindingRow):
            remaining = [
                (line, sign) for line, sign in zip(row.lines, row.signs, strict=True)
                if not (sign > 0 and identity_of(_line_identity_of(line)) == identity)
            ]
            if len(remaining) == 1 and remaining[0][1] == 1:
                change["source_ref"] = source_ref(remaining[0][0])
            elif len(remaining) > 1:
                change["lines"] = [
                    {"source_ref": source_ref(line), "sign": sign} for line, sign in remaining
                ]
        if "source_ref" not in change and "lines" not in change:
            change["source_ref"] = {
                "kind": "line_identity", "line_qname": "<distinct filed line net of the shared claim>",
                "presentation_role_name": presentation_role,
                "dimensions": [{"axis_qname": axis, "member_qname": member} for axis, member in dimensions],
            }
        amounts = {
            cell.printed_column.period_ref.fiscal_year: str(_amount(cell)) for _, cell in cells
        }
        details = {
            "code": FILED_LINE_DOUBLE_CLAIMED_GAP_PREFIX,
            "line_qname": qname, "presentation_role_name": presentation_role,
            "dimensions": [{"axis_qname": axis, "member_qname": member} for axis, member in dimensions],
            "accessions": [accession], "roles": roles,
            "periods": [{"fiscal_year": year, "amount": amount} for year, amount in sorted(amounts.items())],
            "repair_owner": "binding", "change": change,
        }
        gaps.append(HistoricalFoundationGap(
            key=_bounded_gap_key(FILED_LINE_DOUBLE_CLAIMED_GAP_PREFIX, f"{accession}:{identity!r}"),
            text=(
                f"{accession}: filed line {qname} ({presentation_role}) is claimed additively by "
                f"{', '.join(roles)}. Re-bind {repair_target} net of this shared line so it is counted once."
            ),
            blocking=False, repair_target=repair_target, details=details,
            evidence_ids=tuple(sorted({f"filed:{source_accession}:{cell.source.cell_locator}" for source_accession, cell in cells})),
            next_call=HistoricalFoundationNextCall(tool_name="fms_persist_correspondence", arguments={
                "judgment": {
                    "artifact_kind": "statement_bindings",
                    "rationale": "<why the repaired role excludes the line claimed separately>",
                    "expected_revision": bindings_revision, "changes": [change],
                },
            }),
        ))
    return tuple(gaps)


#: The concepts a cash-flow statement closes on when the cash it reconciles includes restricted
#: cash and restricted cash equivalents (client funds, escrow). A balance-sheet cash line that
#: omits them cannot tie to a cash-flow statement that closes on one of these.
_RESTRICTED_INCLUSIVE_CASH_CONCEPTS = frozenset({"CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents"})


@dataclass(frozen=True)
class _RestrictedCashScope:
    """The restricted-cash-inclusive cash line a filing's cash-flow statement closes on."""

    qname: str
    beginning: Decimal
    ending: Decimal
    #: Every captured filing whose cash-flow statement prints the line.
    accessions: tuple[str, ...]
    evidence: tuple[str, ...]


def _restricted_cash_scope(
    cells_by_accession: Mapping[str, Sequence[FilingPresentationCellV1]],
    accession: str, fiscal_year: int, factor: Decimal, change: Decimal, tolerance: Decimal,
) -> _RestrictedCashScope | None:
    """The row of ``accession``'s cash-flow statement, on a restricted-cash-inclusive line, whose
    ``fiscal_year`` column exceeds its prior-year column by ``change`` (the period's net cash
    flows) within ``tolerance``, in the requirement's scale. The statement prints that line on
    both its beginning and its ending row, and only the ending row rolls by the net flows;
    ``None`` when no row does."""

    def inclusive_cells(cells: Sequence[FilingPresentationCellV1]) -> list[FilingPresentationCellV1]:
        return [
            cell for cell in cells
            if cell.line_qname.local_name in _RESTRICTED_INCLUSIVE_CASH_CONCEPTS
            and "cash_flow" in cell.presentation.statement_roles
            and (identity := _face_line_identity(cell)) is not None and not identity[2]
            and _amount(cell) is not None
        ]

    cells = inclusive_cells(cells_by_accession.get(accession, ()))
    for ending in cells:
        if ending.printed_column.period_ref.fiscal_year != fiscal_year:
            continue
        for beginning in cells:
            if (beginning.printed_column.period_ref.fiscal_year != fiscal_year - 1
                    or beginning.source.row_locator != ending.source.row_locator):
                continue
            ending_amount, beginning_amount = _amount(ending), _amount(beginning)
            assert ending_amount is not None and beginning_amount is not None
            if abs(beginning_amount / factor + change - ending_amount / factor) > tolerance:
                continue
            return _RestrictedCashScope(
                qname=ending.line_qname.lexical_qname, beginning=beginning_amount / factor, ending=ending_amount / factor,
                accessions=tuple(row for row, row_cells in cells_by_accession.items() if inclusive_cells(row_cells)),
                evidence=(f"filed:{accession}:{ending.source.cell_locator}", f"filed:{accession}:{beginning.source.cell_locator}"),
            )
    return None


def evaluate_historical_spread(
    *, bindings: StatementBindings | None,
    cells_by_accession: Mapping[str, Sequence[FilingPresentationCellV1]],
    catalog: RequiredRoleCatalog,
    spread_values: Mapping[tuple[str, str], Decimal],
    vintages: Mapping[tuple[str, str], PeriodVintagesLike],
    requirements: Sequence[AnnualSourceRequirement],
    modeled_fiscal_years: Collection[int], research_file_id: int, bindings_revision: str | None,
) -> tuple[tuple[HistoricalFoundationGap, ...], HistoricalFiledTieOut]:
    """Read vintage selection, never choose it; all failures remain non-blocking."""
    gaps: list[HistoricalFoundationGap] = []
    comparisons: list[dict[str, Any]] = []
    differences: list[HistoricalFiledVintageDifference] = []
    counts = {f"FY{year}": dict(agree=0, disagree=0, unclaimed=0) for year in sorted(modeled_fiscal_years)}
    selected = {
        key: candidate
        for key, period in vintages.items()
        for candidate in period.candidates
        if candidate.evidence_id == period.selected
    }
    requirement_by_role = {row.required_role: row for row in requirements}

    def mapping_gap(prefix: str, period: str, roles: Sequence[str], detail: str, suffix: str = "", details: dict[str, Any] | None = None, extra_evidence: tuple[str, ...] = (), next_call: HistoricalFoundationNextCall | None = None, *, repair_target: str | None) -> None:
        roles = tuple(dict.fromkeys(roles))
        gaps.append(HistoricalFoundationGap(
            key=f"{prefix}:{suffix}{period}", text=detail, blocking=False,
            repair_target=repair_target,
            details=details,
            evidence_ids=tuple(sorted({selected[(role, period)].evidence_id for role in roles if (role, period) in selected} | set(extra_evidence))),
            next_call=next_call or HistoricalFoundationNextCall(tool_name="corpus_state", arguments={
                "research_file_id": research_file_id, "view": "mapping_rows", "roles": list(roles),
            }),
        ))
    unclaimed_lines = () if bindings is None else filed_lines_unclaimed(cells_by_accession, bindings, modeled_fiscal_years=modeled_fiscal_years)
    if bindings is not None:
        gaps.extend(_double_claim_gaps(bindings, cells_by_accession, catalog, modeled_fiscal_years, bindings_revision))

    for (role, period), candidate in sorted(selected.items()):
        if period not in counts or (role, period) not in spread_values:
            continue
        value = spread_values[(role, period)]
        agree = 2 * abs(value - candidate.value) <= candidate.resolution
        counts[period]["agree" if agree else "disagree"] += 1
        requirement = requirement_by_role.get(role)
        comparisons.append(dict(
            role=role, fiscal_year=int(period[2:]), spread_value=str(value), filed_value=str(candidate.value),
            agree=agree, selected_accession=candidate.source_id, tag=candidate.line.tag,
            statement_role=candidate.line.statement_role,
            dimensions=[{"axis_qname": dim.axis_qname, "member_qname": dim.member_qname} for dim in candidate.line.dimensions],
            scale=None if requirement is None else requirement.scale,
        ))
        if not agree:
            mapping_gap(FILED_VALUE_DISAGREES_GAP_PREFIX, period, (role,),
                        f"{role} spread {value} disagrees with selected filed value {candidate.value} in {period}.", f"{role}:", comparisons[-1],
                        repair_target=role)
        for own in vintages[(role, period)].candidates:
            if own.own and own.value != candidate.value:
                differences.append(HistoricalFiledVintageDifference(
                    role=role, fiscal_year=int(period[2:]), selected_accession=candidate.source_id,
                    as_filed_accession=own.source_id, selected_value=str(candidate.value), as_filed_value=str(own.value),
                ))

    for period in counts:
        def owed(role: str) -> bool:
            requirement = requirement_by_role.get(role)
            return requirement is not None and period in requirement.period_ids

        def role_value(role: str) -> tuple[Decimal, Decimal, tuple[str, ...]] | None:
            requirement = requirement_by_role.get(role)
            if requirement is not None and period in requirement.period_ids:
                state = requirement.cell_for_period(period).binding_state
                if state in ("not_separately_presented", "not_applicable"):
                    return Decimal(0), Decimal(0), ()
            candidate = selected.get((role, period))
            if candidate is not None:
                return candidate.value, candidate.resolution / 2, (role,)
            # An unbound covering line is presented by the members it covers (cash and short-term
            # investments by cash and by short-term investments): their values are the line's. A member
            # that rolls into a total on its own (``parent_total``) already enters that total as itself,
            # so it never enters again through the line that covers it.
            covered = [
                value
                for member in catalog.roles_covered_by(role)
                if member.parent_total is None and (value := role_value(member.required_role)) is not None
            ]
            if covered:
                return (
                    sum((value[0] for value in covered), Decimal(0)),
                    sum((value[1] for value in covered), Decimal(0)),
                    tuple(member_role for value in covered for member_role in value[2]),
                )
            return None

        def total_value(line: str, unvalued: list[str] | None = None) -> tuple[Decimal, Decimal, tuple[str, ...]] | None:
            members = catalog.roles_rolling_into(line)
            if not members:
                row = catalog.presenting_role(line)
                return None if row is None else role_value(row.required_role)
            terms = []
            claimed: set[tuple[str, str, str, tuple[tuple[str, str], ...]]] = set()
            for member in members:
                candidate = selected.get((member.required_role, period))
                if candidate is not None:
                    identity = (candidate.source_id, candidate.line.tag, candidate.line.statement_role,
                                tuple(sorted((dim.axis_qname, dim.member_qname) for dim in candidate.line.dimensions)))
                    if identity in claimed:
                        continue
                    claimed.add(identity)
                subtotal = next((item for item in face_lines(member) if item in catalog.statement_totals()), None)
                term = role_value(member.required_role) if subtotal is None else total_value(subtotal, unvalued)
                if term is None and subtotal is None and unvalued is not None and owed(member.required_role):
                    unvalued.append(member.required_role)
                terms.append(term)
            present = [term for term in terms if term is not None]
            if not present:
                row = catalog.presenting_role(line)
                return None if row is None else role_value(row.required_role)
            return sum((term[0] for term in present), Decimal(0)), sum((term[1] for term in present), Decimal(0)), tuple(role for term in present for role in term[2])

        roots = [line for line in catalog.statement_totals() if line.rsplit(".", 1)[-1] in ("total_assets", "total_liabilities_and_equity")]
        results = {line: total_value(line) for line in catalog.statement_totals() if line.startswith("tpl.fm.balance_sheet.")}
        failures: list[str] = []
        involved: list[str] = []
        failing_lines: list[str] = []
        for line, result in results.items():
            row = catalog.presenting_role(line)
            filed = None if row is None else role_value(row.required_role)
            if result is not None and filed is not None and abs(result[0] - filed[0]) > result[1] + filed[1]:
                failures.append(f"{line} members {result[0]} differ from filed total {filed[0]}")
                involved.extend((*result[2], *filed[2]))
                failing_lines.append(line)
        if len(roots) == 2 and all(results[line] is not None for line in roots):
            left, right = (results[line] for line in roots)
            assert left is not None and right is not None
            if abs(left[0] - right[0]) > left[1] + right[1]:
                failures.append(f"assets and liabilities plus equity membership sums differ: {left[0]} versus {right[0]}")
                involved.extend((*left[2], *right[2]))
                failing_lines.extend(roots)
        if failures:
            # The roles to repair: those whose spread disagrees with their selected filed value, the
            # owed members of a failing total that carry no value, and the roles an unclaimed filed
            # balance-sheet line of the period may be bound to; the failing totals' own roles only
            # when no member is implicated.
            disagreeing = [
                role for role in dict.fromkeys(involved)
                if (role, period) in selected and (role, period) in spread_values
                and 2 * abs(spread_values[(role, period)] - selected[(role, period)].value) > selected[(role, period)].resolution
            ]
            unvalued: list[str] = []
            for line in dict.fromkeys(failing_lines):
                total_value(line, unvalued)
            unclaimed = [
                role for unclaimed_line in unclaimed_lines
                if unclaimed_line.statement == "balance_sheet" and any(f"FY{year}" == period for year, _amount_text in unclaimed_line.amounts)
                for role in unclaimed_line.candidate_roles
            ]
            culprits = tuple(dict.fromkeys((*disagreeing, *unvalued, *unclaimed)))
            if not culprits:
                culprits = tuple(dict.fromkeys(
                    total.required_role for total in (catalog.presenting_role(line) for line in failing_lines) if total is not None
                ))
            mapping_gap(HISTORICAL_BALANCE_SHEET_UNBALANCED_GAP_PREFIX, period, (*culprits, *involved), f"{period}: {'; '.join(failures)}.",
                        details={"disagreeing_roles": list(disagreeing), "unvalued_roles": list(dict.fromkeys(unvalued)),
                                 "unclaimed_roles": list(dict.fromkeys(unclaimed))},
                        repair_target=culprits[0] if culprits else None)
        ending_row = catalog.presenting_role("tpl.fm.cash_flow.cash_and_cash_equivalents_end_of_period")
        flow_rows = [catalog.presenting_role(f"tpl.fm.cash_flow.{item}") for item in (
            "operating_cash_flow", "investing_cash_flow", "financing_cash_flow",
        )]
        fx_row = catalog.presenting_role("tpl.fm.cash_flow.effect_of_exchange_rate_on_cash")
        if fx_row is not None and role_value(fx_row.required_role) is not None:
            flow_rows.append(fx_row)
        if ending_row is not None and all(row is not None for row in flow_rows):
            ending_role = ending_row.required_role
            ending = role_value(ending_role)
            flows = [role_value(row.required_role) for row in flow_rows if row is not None]
            operand_roles = [ending_role, *(row.required_role for row in flow_rows if row is not None)]
            valueless = [role for role, value in zip(operand_roles, (ending, *flows)) if value is None]
            if valueless:
                # The roll cannot be evaluated: an operand role has no value in this period.
                # That is an unmade binding, not a tie: name the role and the filings that
                # carry the period's other cash-flow operands.
                accessions = sorted({selected[(role, period)].source_id for role in operand_roles if (role, period) in selected})
                mapping_gap(HISTORICAL_CASH_ROLL_BROKEN_GAP_PREFIX, period, [*valueless, *operand_roles],
                            f"{period}: the cash roll cannot be evaluated; {', '.join(valueless)} has no value.",
                            "unevaluable:",
                            next_call=HistoricalFoundationNextCall(tool_name="fms_persist_correspondence", arguments={
                                "judgment": {
                                    "artifact_kind": "statement_bindings",
                                    "rationale": "<why the filed line belongs to the role>",
                                    "expected_revision": bindings_revision,
                                    "changes": [{"accessions": accessions, "role": role, "state": "bound",
                                                 "source_ref": {"kind": "line_identity", "line_qname": "<the filed line's qname>",
                                                                "statement": ending_row.statement}} for role in valueless],
                                },
                            }) if accessions else None,
                            repair_target=valueless[0])
            prior = selected.get((ending_role, f"FY{int(period[2:]) - 1}"))
            beginning: tuple[Decimal, Decimal] | None = None
            factor: Decimal | None = None
            opening_evidence: tuple[str, ...] = ()
            ending_candidate = selected.get((ending_role, period))
            if ending_candidate is not None:
                requirement = requirement_by_role[ending_role]
                scale = "unit" if requirement.scale == "units" else requirement.scale
                factor = next(Decimal(row.multiplier) for row in CONVENTIONS_IR_V1.scale_factors if row.numeric_scale == scale)
                matching = []
                for cell in cells_by_accession.get(ending_candidate.source_id, ()):
                    identity = _face_line_identity(cell)
                    if identity is None or identity[0] != ending_candidate.line.tag or identity[2] != tuple(sorted((dim.axis_qname, dim.member_qname) for dim in ending_candidate.line.dimensions)):
                        continue
                    if normalize_topology_role(identity[1]) != normalize_topology_role(ending_candidate.line.statement_role):
                        continue
                    matching.append(cell)
                current = next((cell for cell in matching
                                if cell.printed_column.period_ref.fiscal_year == int(period[2:])
                                and (amount := _amount(cell)) is not None
                                and 2 * abs(amount / factor - ending_candidate.value) <= ending_candidate.resolution), None)
                if current is not None:
                    for cell in matching:
                        if cell.printed_column.period_ref.fiscal_year != int(period[2:]) - 1:
                            continue
                        if cell.source.row_locator != current.source.row_locator or cell.preferred_label.role_uri != current.preferred_label.role_uri or cell.tree != current.tree:
                            continue
                        amount = _amount(cell)
                        if amount is not None:
                            beginning = (amount / factor, ending_candidate.resolution / 2)
                            opening_evidence = (f"filed:{ending_candidate.source_id}:{cell.source.cell_locator}",)
                            break
            if beginning is None and prior is not None:
                beginning = (prior.value, prior.resolution / 2)
                opening_evidence = (prior.evidence_id,)
            if beginning is not None and ending is not None and all(flow is not None for flow in flows):
                present_flows = [flow for flow in flows if flow is not None]
                change = sum((flow[0] for flow in present_flows), Decimal(0))
                tolerance = beginning[1] + ending[1] + sum((flow[1] for flow in present_flows), Decimal(0))
                if abs(beginning[0] + change - ending[0]) > tolerance:
                    scope = None if ending_candidate is None or factor is None else _restricted_cash_scope(
                        cells_by_accession, ending_candidate.source_id, int(period[2:]), factor, change, tolerance)
                    if scope is not None and ending_candidate is not None and ending_candidate.line.tag.rpartition(":")[2] not in _RESTRICTED_INCLUSIVE_CASH_CONCEPTS:
                        mapping_gap(HISTORICAL_CASH_ROLL_BROKEN_GAP_PREFIX, period,
                                    [ending_role, *(row.required_role for row in flow_rows if row is not None)],
                                    f"{period}: cash scope mismatch: the cash flow statement reconciles to cash including restricted cash "
                                    f"({scope.qname}: {scope.beginning} plus net cash flows {change} equals {scope.ending}), but {ending_role} "
                                    f"is bound to {ending_candidate.line.tag}, which excludes it. Bind {ending_role} to {scope.qname}; "
                                    f"the balance-sheet cash role stays on the corporate-cash line.",
                                    "scope:",
                                    details={"role_to_bind": ending_role, "bound_tag": ending_candidate.line.tag, "reconciling_tag": scope.qname,
                                             "beginning": str(scope.beginning), "net_change": str(change), "ending": str(scope.ending)},
                                    extra_evidence=(*opening_evidence, *scope.evidence),
                                    next_call=HistoricalFoundationNextCall(tool_name="fms_persist_correspondence", arguments={
                                        "judgment": {
                                            "artifact_kind": "statement_bindings",
                                            "rationale": "<why the filed line belongs to the role>",
                                            "expected_revision": bindings_revision,
                                            "changes": [{"accessions": list(scope.accessions), "role": ending_role, "state": "bound",
                                                         "source_ref": {"kind": "line_identity", "line_qname": scope.qname,
                                                                        "statement": ending_row.statement}}],
                                        },
                                    }),
                                    repair_target=ending_role)
                    else:
                        mapping_gap(HISTORICAL_CASH_ROLL_BROKEN_GAP_PREFIX, period,
                                    [ending_role, *(row.required_role for row in flow_rows if row is not None)],
                                    f"{period}: beginning cash {beginning[0]} plus net cash flows {change} differs from ending cash {ending[0]}.",
                                    extra_evidence=opening_evidence, repair_target=ending_role)

    for line in unclaimed_lines:
        record = unclaimed_line_record(line)
        evidence: set[str] = set()
        for accession in line.accessions:
            for cell in cells_by_accession.get(accession, ()):
                identity = _face_line_identity(cell)
                if identity is None:
                    continue
                qname, role, dimensions = identity
                amount = _amount(cell)
                if amount is None or amount == 0 or cell.printed_column.period_ref.fiscal_year not in modeled_fiscal_years:
                    continue
                if qname == line.line_qname and normalize_topology_role(role) == normalize_topology_role(line.presentation_role_name) and tuple(sorted(dimensions)) == tuple(sorted(line.dimensions)):
                    evidence.add(f"filed:{accession}:{cell.source.cell_locator}")
        for year, _filed_amount in line.amounts:
            if f"FY{year}" in counts:
                counts[f"FY{year}"]["unclaimed"] += 1
        gaps.append(HistoricalFoundationGap(
            key=_bounded_gap_key(FILED_LINE_UNCLAIMED_GAP_PREFIX, str(record["line_key"])),
            text=unclaimed_line_message(record), blocking=False,
            repair_target=str(record["line_key"]),
            evidence_ids=tuple(sorted(evidence)), details=record,
            # Carry the research-file scope and the bindings revision the spread read.
            next_call=HistoricalFoundationNextCall(tool_name="fms_persist_correspondence", arguments={
                "judgment": {
                    "artifact_kind": "statement_bindings",
                    "rationale": "<why each filed line belongs to its role>",
                    "expected_revision": bindings_revision,
                    "changes": [record["change"]],
                },
            }),
        ))
    return tuple(gaps), HistoricalFiledTieOut(
        by_period={period: HistoricalFiledTieOutPeriod(**count) for period, count in counts.items()},
        vintage_differences=tuple(differences), comparisons=tuple(comparisons),
    )
