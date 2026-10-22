"""Filed balance-sheet and cash-flow lines that carry a value and no row places.

A ``statement_bindings.v2`` row claims the filed line (or, for a composite row, the
N filed lines) its role reads. A face line the filing prints inside a total the template
foots, with a value in a modeled period, that no row claims and no residual absorbs, is
in the filed totals and in no template row: the statements stop footing and nothing names
the line. This check lists those lines, each with the face roles that feed its total, so
the build-model agent can bind it (alone, or as one more line of a composite row) or rule
it ``operating.*``.

Every clause is derived from an owner, never listed here:

- *inside a filed total* — the filing's own presentation tree: a line is inside the total
  that is a sibling of the line or of one of its section headers (the statement's root
  header excluded), and only a total a bound row claims for a role whose template item
  the template foots by arithmetic (``total_role_composition``). A supplemental or
  non-cash disclosure, or the components of a cash reconciliation whose ending balance
  the template references, hang under no such total.
- *claimed* — a bound row's line, where the row's role has a catalog home (a binding
  plane or a covering role); the same
  concept (qname and dimensions) bound on this statement, or on another statement only when
  a template row there reads the claimed row (the cash-flow net-income row refs the
  income-statement row; a concept bound on the income statement does not claim a cash-flow
  line that row does not read); a figure the filing repeats — the same presented line (same
  statement and concept, or the same fact on that statement) or the figure a template row on
  this statement reads, equal in every modeled period — not an equal amount on another
  statement or between a balance and a flow; or the figure of a period another accession's
  row already claims for the same concept, where that claim places this statement's line
  (a comparative column the later filing prints again).
- *absorbed* — a residual role (``residual_totals``) that the accession's windows neither bind
  nor cite absent: the residual takes the lines no row claims. A residual cited
  ``not_applicable`` or ``not_separately_presented`` says the filing has no such line, so it
  absorbs nothing.
- *printed once* — a dimensioned cell whose presentation role never places its axis in
  the tree is the line's own figure (the capture attached a fair-value level to it), not a
  breakdown the filer prints, so it is no line of its own.
- *misbound total* — a printed total no role claims whose calculation children are bound to
  roles that roll into a total role (the catalog's ``parent_total``) that is bound to one of
  those children: the total role reads a subtotal. The total is reported with that role as its
  candidate and ``candidate_bound_to`` naming the child line, even inside a claimed total. Every
  other aggregate the filing prints (a total, or a line that parents other lines) is not a line
  to claim: its parts are; an unbound anchor total is reported by ``_empty_pin_gaps``
  (``required_column_absent_from_pin:<role>``), not here.
- *role home* — the roles a binding row may target (``composite_target_roles``) that the
  template foots into the line's total; a line no such role feeds is not reported.

Blocking: an unclaimed line is an ``error`` finding (``filed_line_unclaimed``), computed once
by the build, which holds the bindings and the capture, and carried as
``unclaimed_line_record`` rows to every validation that reads it.
Pure: the occurrence cells and the decoded bindings in, the lines out. A
``not_applicable`` or ``not_separately_presented`` row cites a statement, not a line, so
it claims none: a line the filing prints with a value is a separate line by construction.
"""

from __future__ import annotations

import json
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from functools import lru_cache
from types import MappingProxyType
from typing import cast

from schema.provider_port.adapters.edgar.wire.filing_facts_v1 import FilingPresentationCellV1
from .required_role_catalog import (
    _role_has_home as catalog_role_has_home,
    load_required_role_catalog,
)
from .statement_bindings_v2 import (
    LineIdentity,
    StatementBindings,
    _face_line_identity,
    _referenced_item_ids,
    bound_lines,
    composite_target_roles,
    residual_totals,
    total_role_composition,
)
from .statement_topology import normalize_topology_role

#: The statements whose lines must foot to a filed total: the balance sheet and the
#: cash-flow statement. Their names are the catalog's ``statement`` values.
CHECKED_STATEMENTS = ("balance_sheet", "cash_flow")

_Identity = tuple[str, str, tuple[tuple[str, str], ...]]
_Concept = tuple[str, tuple[tuple[str, str], ...]]


@dataclass(frozen=True, slots=True)
class UnclaimedFiledLine:
    """One filed line with a modeled-period value and no claiming binding row."""

    line_qname: str
    presentation_role_name: str
    dimensions: tuple[tuple[str, str], ...]
    label: str | None
    statement: str
    #: Every captured filing in which the line is unclaimed, oldest first.
    accessions: tuple[str, ...]
    #: ``(fiscal_year, native amount)`` per modeled period, the newest filing's print.
    amounts: tuple[tuple[int, str], ...]
    #: The filing's calculation-arc weight for the line, the candidate sign of a
    #: composite row; ``None`` where the filing carries no calculation arc for it.
    calculation_weight: float | None
    negates_value: bool
    #: The face roles a binding row may target that the template foots into the line's total.
    candidate_roles: tuple[str, ...]
    #: The lines a candidate total role is bound to now, when the line is a printed total whose
    #: calculation children are bound to roles that roll into that total role and the role reads
    #: one of those children instead: the role is on the wrong line, not unplaced.
    candidate_bound_to: tuple[str, ...] = ()
    #: The filing's calculation-arc parent concept, or ``None`` without a calculation arc.
    calculation_parent_qname: str | None = None


def _normalized(identity: LineIdentity) -> _Identity:
    qname, role_name, pairs = identity
    return (qname, normalize_topology_role(role_name), tuple(sorted(pairs)))


def _concept(identity: _Identity) -> _Concept:
    return (identity[0], identity[2])


@dataclass(slots=True)
class _Claims:
    """What one accession's bindings claim: every role each claimed line is bound to (a line
    may sit in several), and the roles its windows cite as absent (``not_applicable`` /
    ``not_separately_presented``)."""

    roles_of: dict[_Identity, list[str]] = field(default_factory=dict)
    cited_absent: set[str] = field(default_factory=set)

    def bind(self, identity: _Identity, role: str) -> None:
        roles = self.roles_of.setdefault(identity, [])
        if role not in roles:
            roles.append(role)

    def bound_roles(self) -> frozenset[str]:
        return frozenset(role for roles in self.roles_of.values() for role in roles)


@lru_cache(maxsize=None)
def _role_has_home(role: str) -> bool:
    """Use the catalog's home rule; issuer-defined ``operating.*`` roles are not judged here."""

    catalog = load_required_role_catalog()
    by_id = {row.required_role: row for row in catalog.required_roles}
    if role not in by_id:
        return True
    return catalog_role_has_home(by_id[role], by_id=by_id)


def _claims_by_accession(bindings: StatementBindings) -> dict[str, _Claims]:
    """The lines each accession's window claims, including a later filing's comparative
    cited as this filing's period (``vintage_of``)."""

    claims: dict[str, _Claims] = {}
    for window in bindings.vintage_windows:
        for role, row in window.rows.items():
            if not _role_has_home(role):
                continue
            if row.state in ("not_applicable", "not_separately_presented"):
                for accession in window.accessions:
                    claims.setdefault(accession, _Claims()).cited_absent.add(role)
            for line in bound_lines(row):
                identity = _normalized(
                    (
                        line.line_qname,
                        line.presentation_role_name,
                        tuple((item.axis_qname, item.member_qname) for item in line.dimensions),
                    )
                )
                for accession in window.accessions:
                    claims.setdefault(accession, _Claims()).bind(identity, role)
                for citation in line.reviewed_occurrences:
                    if citation.vintage_of is not None:
                        claims.setdefault(citation.vintage_of, _Claims()).bind(identity, role)
    return claims


def _local_name(qname: str) -> str:
    if qname.startswith("{"):
        return qname.partition("}")[2]
    return qname.rpartition(":")[2]


def _calculation_parent_lines(cells: Sequence[FilingPresentationCellV1]) -> frozenset[tuple[str, str]]:
    """``(presentation role uri, local name)`` of each line a calculation arc makes a parent in
    the role the arc is printed in: a note's arcs make the face line no aggregate of the face."""

    return frozenset(
        (cell.presentation.presentation_role_uri, _local_name(cell.calculation.parent_qname))
        for cell in cells
        if cell.calculation is not None and cell.calculation.parent_qname
    )


def _aggregate_lines(cells: Sequence[FilingPresentationCellV1]) -> frozenset[tuple[str, str]]:
    """Lines the filing prints as an aggregate in their own presentation role: a calculation or
    presentation parent there."""

    return _calculation_parent_lines(cells) | frozenset(
        (cell.presentation.presentation_role_uri, cell.tree.parent_qname.local_name)
        for cell in cells
        if cell.tree.parent_qname is not None
    )


def _presented_axes(cells: Sequence[FilingPresentationCellV1]) -> frozenset[tuple[str, str]]:
    """``(presentation role uri, axis local name)`` for every axis the filing places in a
    presentation role: a breakdown the filer prints is a table with its axis in the tree."""

    return frozenset(
        (cell.presentation.presentation_role_uri, cell.line_qname.local_name)
        for cell in cells
        if cell.line_kind == "abstract"
    )


def _is_unprinted_breakdown(cell: FilingPresentationCellV1, presented: frozenset[tuple[str, str]]) -> bool:
    """Whether ``cell`` carries a dimension its presentation role never places: the capture
    attached a dimensioned fact (a fair-value level, say) to a line the filer prints once,
    so the cell is the same printed figure as the line's own, not a line of its own."""

    return cell.binding is not None and cell.binding.kind == "bound" and any(
        (cell.presentation.presentation_role_uri, dimension.axis.local_name) not in presented
        for dimension in cell.binding.fact.dimensions
    )


def _is_total(cell: FilingPresentationCellV1, calculation_parents: frozenset[tuple[str, str]]) -> bool:
    return (
        cell.line_kind == "numeric"
        and (
            cell.preferred_label.role_uri.rsplit("/", 1)[-1] == "totalLabel"
            or (cell.presentation.presentation_role_uri, cell.line_qname.local_name) in calculation_parents
        )
    )


def _statement_of(cell: FilingPresentationCellV1) -> str | None:
    roles = cell.presentation.statement_roles
    if "disclosure" in roles:
        return None
    if (
        cell.presentation.presentation_kind in ("parenthetical", "disclosure", "document")
        or "(parenthetical)" in cell.presentation.table_title.lower()
    ):
        return None
    return next((name for name in CHECKED_STATEMENTS if name in roles), None)


def _amount(cell: FilingPresentationCellV1) -> Decimal | None:
    """The numeric value of a bound cell, ``None`` where it is not a number."""

    if cell.binding is None or cell.binding.kind != "bound":
        return None
    quantity = cell.binding.fact.native_quantity
    if quantity.state != "numeric":
        return None
    number = quantity.native_number
    if number.kind == "decimal":
        return Decimal(number.value)
    return Decimal(number.numerator) / Decimal(number.denominator)


def _fact_keys(cell: FilingPresentationCellV1) -> frozenset[str]:
    """Anchors that name the fact ``cell`` prints. Empty when the capture named none."""

    if cell.binding is None or cell.binding.kind != "bound":
        return frozenset()
    return frozenset(
        locator.source_xpath
        for locator in cell.binding.fact.provenance.locators
        if locator.source_xpath
    )


class _FilingTree:
    """One filing's presentation tree: each line's parent header, each header's children."""

    def __init__(self, cells: Sequence[FilingPresentationCellV1]) -> None:
        self._calculation_parents = _calculation_parent_lines(cells)
        self._parent: dict[tuple[str, str], str | None] = {}
        self._children: dict[tuple[str, str], dict[str, FilingPresentationCellV1]] = {}
        #: Each calculation parent's child lines, by local name: a function of the
        #: accession's cells alone, built once with the tree.
        self.calculation_below: dict[str, list[str]] = {}
        for cell in cells:
            uri = cell.presentation.presentation_role_uri
            local = cell.line_qname.local_name
            key = (uri, local)
            parent = None if cell.tree.parent_qname is None else cell.tree.parent_qname.local_name
            self._parent.setdefault(key, parent)
            if cell.calculation is not None and cell.calculation.parent_qname:
                self.calculation_below.setdefault(_local_name(cell.calculation.parent_qname), []).append(local)
            if parent is None or cell.line_kind != "numeric":
                continue
            held = self._children.setdefault((uri, parent), {}).get(local)
            if held is None or (_face_line_identity(held) is None and _face_line_identity(cell) is not None):
                self._children[(uri, parent)][local] = cell

    def enclosing_totals(self, cell: FilingPresentationCellV1) -> list[LineIdentity]:
        """The identities of the totals the filing prints beside ``cell`` or beside the
        nearest of its section headers that has one; the statement's root header is no
        section and is never searched."""

        uri = cell.presentation.presentation_role_uri
        header = None if cell.tree.parent_qname is None else cell.tree.parent_qname.local_name
        while header is not None and self._parent.get((uri, header)) is not None:
            totals = [
                identity
                for sibling in self._children.get((uri, header), {}).values()
                if sibling.line_qname.local_name != cell.line_qname.local_name
                and _is_total(sibling, self._calculation_parents)
                and (identity := _face_line_identity(sibling)) is not None
            ]
            if totals:
                return totals
            header = self._parent[(uri, header)]
        return []


@dataclass(slots=True)
class _CalculationRoles:
    """One accession's bound roles by calculation line, built once from its cells and
    claims: for each calculation parent (by local name), the roles bound to its
    children and the child lines each of those roles reads, in cell order; and every
    line's bound roles, by local name."""

    children: dict[str, dict[str, set[str]]] = field(default_factory=dict)
    roles_of_line: dict[str, set[str]] = field(default_factory=dict)


@dataclass(slots=True)
class _Found:
    identity: LineIdentity
    accessions: list[str]
    amounts: dict[int, str]
    label: str | None
    negates_value: bool
    weight: float | None
    candidate_roles: tuple[str, ...]
    candidate_bound_to: tuple[str, ...] = ()
    parent_qname: str | None = None
    #: Source anchors of the fact this line prints, when the capture named one.
    fact_keys: set[str] = field(default_factory=set)


# The roles that close a statement. A pin that omits one of them, or a filed
# total none of them claims, is the gap an agent must bind before the lines
# inside that total can be named. Leaves, not a hand list of issuers.
_PIN_ANCHOR_LEAVES = frozenset(
    {
        "revenue",
        "net_income",
        "total_assets",
        "total_liabilities",
        "total_equity",
        "operating_cash_flow",
        "investing_cash_flow",
        "financing_cash_flow",
    }
)


def pin_anchor_roles() -> tuple[str, ...]:
    """Non-optional catalog roles a pin must carry before HFF can accept with no binding gap."""

    catalog = load_required_role_catalog()
    return tuple(
        row.required_role
        for row in catalog.required_roles
        if not row.optional_if_unreported
        and row.required_role.rsplit(".", 1)[-1] in _PIN_ANCHOR_LEAVES
    )


def _misbound_total_roles(
    cell: FilingPresentationCellV1,
    statement: str,
    tree: _FilingTree,
    calculation: _CalculationRoles,
) -> dict[str, tuple[str, ...]]:
    """The total roles a printed total ``cell`` that no role claims is the line of, with the
    lines each reads now: the roles bound to the lines ``cell`` sums (its calculation children)
    roll into a total role (the catalog's ``parent_total``), and that total role is bound to
    one of those children rather than to ``cell``. The filing's calculation parent decides:
    the printed total is the line, its child is the wrong one. A printed total is a total
    role's line only when all the statement roles its calculation tree sums roll into that
    role: a tree that also sums equity under liabilities and equity is not total liabilities."""

    catalog = load_required_role_catalog()
    name = cell.line_qname.local_name
    children = calculation.children.get(name, {})
    role_of_item = _role_of_template_item()
    summed_roles = _calculation_tree_roles(name, tree.calculation_below, calculation.roles_of_line)
    misbound: dict[str, tuple[str, ...]] = {}
    for role in children:
        if role not in catalog.role_ids():
            continue
        parent = catalog.role(role).parent_total
        total = None if parent is None else role_of_item.get(parent)
        if (
            total is not None
            and total in children
            and catalog.role(total).statement == statement
            and total_role_composition(total)
            and _sums_within(total, statement, summed_roles, role_of_item)
        ):
            misbound[total] = tuple(sorted(children[total]))
    return misbound


def _calculation_tree_roles(
    name: str, below: Mapping[str, Sequence[str]], roles_of_line: Mapping[str, Collection[str]]
) -> frozenset[str]:
    """The roles bound to every line in the calculation tree below the concept ``name``."""

    pending, reached = [name], {name}
    while pending:
        for child in below.get(pending.pop(), ()):
            if child not in reached:
                reached.add(child)
                pending.append(child)
    return frozenset(role for line in reached for role in roles_of_line.get(line, ()))


@lru_cache(maxsize=None)
def _role_template_homes(role: str) -> frozenset[str]:
    """The statement items carrying a role directly or through coverage."""

    catalog = load_required_role_catalog()
    statement = catalog.role(role).statement
    pending = [role]
    seen: set[str] = set()
    items: set[str] = set()
    while pending:
        current = pending.pop()
        if current in seen:
            continue
        seen.add(current)
        row = catalog.role(current)
        if row.statement != statement:
            continue
        items.update(row.template_items)
        pending.extend(row.covered_by_roles)
    return frozenset(items)


def _sums_within(
    total: str, statement: str, summed_roles: Collection[str], role_of_item: Mapping[str, str]
) -> bool:
    """Whether every modeled ``statement`` role below a printed total rolls into ``total``.

    A role rolls in when its direct or covered home is in the
    template composition, or its ``parent_total`` ancestry reaches ``total`` (the current
    lease liability, whose stamped item is an assumptions row).
    """

    catalog = load_required_role_catalog()
    composition = total_role_composition(total)
    for role in summed_roles:
        if role not in catalog.role_ids():
            continue
        row = catalog.role(role)
        homes = _role_template_homes(role)
        if row.statement != statement or not homes:
            continue
        if composition.intersection(homes):
            continue
        seen = {row.required_role}
        while row.required_role != total:
            parent_role = None if row.parent_total is None else role_of_item.get(row.parent_total)
            if parent_role is None or parent_role in seen:
                return False
            seen.add(parent_role)
            row = catalog.role(parent_role)
    return True


@lru_cache(maxsize=1)
def _role_of_template_item() -> Mapping[str, str]:
    """The role each stamped template item belongs to."""

    return MappingProxyType({
        item: row.required_role
        for row in load_required_role_catalog().required_roles
        if row.alias_of is None
        for item in row.template_items
    })


def _rolls_into(role: str, total_role: str) -> bool:
    """Whether the catalog's ``parent_total`` ancestry of ``role`` reaches ``total_role``: a role
    whose line the template builder inserts without arithmetic still sits under its total."""

    catalog = load_required_role_catalog()
    role_of_item = _role_of_template_item()
    seen = {role}
    current = role
    while current != total_role:
        parent = catalog.role(current).parent_total
        above = None if parent is None else role_of_item.get(parent)
        if above is None or above in seen:
            return False
        seen.add(above)
        current = above
    return True


def _candidate_roles(statement: str, total_role: str) -> tuple[str, ...]:
    """The roles a binding row may target in ``statement`` that foot into ``total_role``: the
    template's arithmetic reads their home, or their ``parent_total`` ancestry reaches it."""

    composition = total_role_composition(total_role)
    return tuple(
        role for role in composite_target_roles(statement)
        if composition.intersection(_role_template_homes(role)) or _rolls_into(role, total_role)
    )


def _role_statement(role: str) -> str | None:
    catalog = load_required_role_catalog()
    if role not in catalog.role_ids():
        return None
    return catalog.role(role).statement


@lru_cache(maxsize=1)
def _statements_reading_role() -> Mapping[str, frozenset[str]]:
    """Statements whose template rows read each role's stamped item.

    A historical ``ref`` is the row's value (cash-flow net income refs income-statement
    net income). A ratio or arithmetic total that merely uses the item does not: binding
    the item does not place that other row. Derived from the template and the catalog.
    """

    from .templates.template_builder_config import SIA_GENERIC_TEMPLATE_PATH

    catalog = load_required_role_catalog()
    statements = frozenset(row.statement for row in catalog.required_roles)
    item_roles: dict[str, set[str]] = {}
    for row in catalog.required_roles:
        for item_id in row.template_items:
            item_roles.setdefault(item_id, set()).add(row.required_role)
    reading: dict[str, set[str]] = {}
    document = json.loads(SIA_GENERIC_TEMPLATE_PATH.read_text(encoding="utf-8"))
    for sheet in document["sheets"].values():
        for section in sheet.get("sections") or []:
            statement = section.get("id")
            if statement not in statements:
                continue
            for item in section.get("line_items") or []:
                historical = item.get("historical")
                if not isinstance(historical, dict) or historical.get("type") != "ref":
                    continue
                for ref in _referenced_item_ids(historical):
                    for role in item_roles.get(ref, ()):
                        if _role_statement(role) != statement:
                            reading.setdefault(role, set()).add(statement)
    return {role: frozenset(found) for role, found in reading.items()}


def _role_places(role: str, statement: str) -> bool:
    """Whether a binding of ``role`` places its concept on ``statement``.

    The role's own statement always. Another statement only when a template row there
    reads the role's stamped item.
    """

    if _role_statement(role) == statement:
        return True
    return statement in _statements_reading_role().get(role, ())


def _concept_claimed_on(concept: _Concept, statement: str, claims: _Claims) -> bool:
    return any(
        _concept(identity) == concept and _role_places(role, statement)
        for identity, roles in claims.roles_of.items()
        for role in roles
    )


def _claimed_periods(
    cells_by_accession: Mapping[str, Sequence[FilingPresentationCellV1]],
    claims_by_accession: Mapping[str, _Claims],
) -> dict[tuple[_Concept, int], dict[str, set[Decimal]]]:
    """The figure each claimed role carries per fiscal year, across every accession.

    A comparative column a later filing prints again is the period an earlier window
    claims, and only where that claim places the later line's statement.
    """

    periods: dict[tuple[_Concept, int], dict[str, set[Decimal]]] = {}
    for accession, cells in cells_by_accession.items():
        claimed = claims_by_accession.get(accession)
        if claimed is None:
            continue
        for cell in cells:
            identity = _face_line_identity(cell)
            if identity is None or cell.line_kind != "numeric" or (amount := _amount(cell)) is None:
                continue
            normalized = _normalized(identity)
            for role in claimed.roles_of.get(normalized, ()):
                periods.setdefault(
                    (_concept(normalized), cell.printed_column.period_ref.fiscal_year), {},
                ).setdefault(role, set()).add(amount)
    return periods


def _period_claimed_on(
    concept: _Concept,
    fiscal_year: int,
    amount: Decimal,
    statement: str,
    claimed_periods: Mapping[tuple[_Concept, int], Mapping[str, set[Decimal]]],
) -> bool:
    return any(
        amount in amounts and _role_places(role, statement)
        for role, amounts in claimed_periods.get((concept, fiscal_year), {}).items()
    )


def filed_lines_unclaimed(
    cells_by_accession: Mapping[str, Sequence[FilingPresentationCellV1]],
    bindings: StatementBindings,
    *,
    modeled_fiscal_years: Collection[int],
) -> tuple[UnclaimedFiledLine, ...]:
    """Every balance-sheet and cash-flow line with a nonzero value in a modeled
    fiscal year that sits inside a filed total the template foots and that no binding
    row claims, no unbound residual role absorbs and no claimed line repeats, in the
    accession printing it.

    ``cells_by_accession`` is in filing order, oldest first. An aggregate the
    filing prints (a total, or a line that parents other lines) is not a line to
    claim: its parts are.
    """

    claims_by_accession = _claims_by_accession(bindings)
    claimed_periods = _claimed_periods(cells_by_accession, claims_by_accession)
    years = frozenset(modeled_fiscal_years)
    residual = residual_totals()
    found: dict[tuple[str, _Identity], _Found] = {}
    for accession, cells in cells_by_accession.items():
        claims = claims_by_accession.get(accession, _Claims())
        bound_roles = claims.bound_roles()
        absorbed_totals = {
            total for role, total in residual.items()
            if role not in bound_roles and role not in claims.cited_absent
        }
        aggregates = _aggregate_lines(cells)
        presented_axes = _presented_axes(cells)
        tree = _FilingTree(cells)
        claimed_amounts: dict[_Identity, dict[int, Decimal]] = {}
        claimed_facts: dict[_Identity, set[str]] = {}
        calculation = _CalculationRoles()
        for cell in cells:
            identity = _face_line_identity(cell)
            if identity is None:
                continue
            normalized = _normalized(identity)
            roles = claims.roles_of.get(normalized)
            if roles is not None:
                calculation.roles_of_line.setdefault(cell.line_qname.local_name, set()).update(roles)
                if cell.calculation is not None and cell.calculation.parent_qname:
                    children = calculation.children.setdefault(_local_name(cell.calculation.parent_qname), {})
                    for role in roles:
                        children.setdefault(role, set()).add(identity[0])
            if cell.line_kind != "numeric" or (amount := _amount(cell)) is None:
                continue
            if normalized in claims.roles_of:
                claimed_amounts.setdefault(normalized, {})[cell.printed_column.period_ref.fiscal_year] = amount
                claimed_facts.setdefault(normalized, set()).update(_fact_keys(cell))
        here: dict[tuple[str, _Identity], _Found] = {}
        for cell in cells:
            statement = _statement_of(cell)
            identity = None if statement is None else _face_line_identity(cell)
            if statement is None or identity is None or cell.line_kind != "numeric" or _is_unprinted_breakdown(cell, presented_axes):
                continue
            normalized = _normalized(identity)
            is_printed_total = (
                cell.preferred_label.role_uri.rsplit("/", 1)[-1] == "totalLabel"
            )
            unclaimed = (
                normalized not in claims.roles_of
                and not _concept_claimed_on(_concept(normalized), statement, claims)
            )
            if is_printed_total and unclaimed:
                fiscal_year = cell.printed_column.period_ref.fiscal_year
                amount = _amount(cell)
                if fiscal_year in years and amount is not None and amount != 0:
                    entry = here.get((statement, normalized))
                    if entry is None:
                        misbound = _misbound_total_roles(cell, statement, tree, calculation)
                        if misbound:
                            entry = here[(statement, normalized)] = _Found(
                                identity, [], {}, None, False, None, tuple(misbound),
                                tuple(line for role in misbound for line in misbound[role]),
                            )
                    if entry is not None:
                        entry.amounts[fiscal_year] = str(amount)
                        entry.fact_keys.update(_fact_keys(cell))
                        entry.label = cell.preferred_label.text
                        entry.negates_value = cell.preferred_label.negates_value
                        entry.weight = (
                            None if cell.calculation is None else cell.calculation.weight
                        )
                        entry.parent_qname = (
                            None if cell.calculation is None else cell.calculation.parent_qname
                        )
            if (
                (cell.presentation.presentation_role_uri, cell.line_qname.local_name) in aggregates
                or is_printed_total
                or not unclaimed
            ):
                continue
            fiscal_year = cell.printed_column.period_ref.fiscal_year
            amount = _amount(cell)
            if fiscal_year not in years or amount is None or amount == 0:
                continue
            if _period_claimed_on(_concept(normalized), fiscal_year, amount, statement, claimed_periods):
                continue
            entry = here.get((statement, normalized))
            if entry is None:
                total_roles = [
                    role
                    for total in tree.enclosing_totals(cell)
                    for role in claims.roles_of.get(_normalized(total), ())
                    if total_role_composition(role)
                ]
                if not total_roles or total_roles[0] in absorbed_totals:
                    continue
                candidates = _candidate_roles(statement, total_roles[0])
                if not candidates:
                    continue
                entry = here[(statement, normalized)] = _Found(identity, [], {}, None, False, None, candidates)
            entry.amounts[fiscal_year] = str(amount)
            entry.fact_keys.update(_fact_keys(cell))
            entry.label = cell.preferred_label.text
            entry.negates_value = cell.preferred_label.negates_value
            entry.weight = None if cell.calculation is None else cell.calculation.weight
            entry.parent_qname = None if cell.calculation is None else cell.calculation.parent_qname
        for key, entry in here.items():
            statement, _identity = key
            if _repeats_a_claimed_line(entry, statement, claimed_amounts, claims.roles_of, claimed_facts):
                continue
            merged = found.setdefault(
                key,
                _Found(entry.identity, [], {}, None, False, None, entry.candidate_roles, entry.candidate_bound_to),
            )
            merged.accessions.append(accession)
            # Oldest first, so a later filing's print of the same period replaces it.
            merged.amounts.update(entry.amounts)
            merged.label = entry.label
            merged.negates_value = entry.negates_value
            merged.weight = entry.weight
            merged.parent_qname = entry.parent_qname
    return tuple(
        UnclaimedFiledLine(
            line_qname=entry.identity[0],
            presentation_role_name=entry.identity[1],
            dimensions=entry.identity[2],
            label=entry.label,
            statement=statement,
            accessions=tuple(entry.accessions),
            amounts=tuple(sorted(entry.amounts.items())),
            calculation_weight=entry.weight,
            calculation_parent_qname=entry.parent_qname,
            negates_value=entry.negates_value,
            candidate_roles=entry.candidate_roles,
            candidate_bound_to=entry.candidate_bound_to,
        )
        for (statement, _key), entry in found.items()
    )


def _repeats_a_claimed_line(
    entry: _Found,
    statement: str,
    claimed_amounts: Mapping[_Identity, Mapping[int, Decimal]],
    roles_of: Mapping[_Identity, Sequence[str]],
    claimed_facts: Mapping[_Identity, set[str]],
) -> bool:
    """Whether ``entry`` is a claimed line printed again, not an equal amount elsewhere.

    A repeat is the same fact on this statement. It is also the figure a template row
    on ``statement`` reads, equal in every modeled period (net income printed again on
    the cash-flow statement, under whatever concept the filing uses). An equal amount
    on another statement, or between a balance and a flow, is a different line.
    """

    for identity, period in claimed_amounts.items():
        if not all(period.get(year) == Decimal(amount) for year, amount in entry.amounts.items()):
            continue
        for role in roles_of.get(identity, ()):
            on_this_statement = _role_statement(role) == statement
            same_fact = on_this_statement and bool(entry.fact_keys & claimed_facts.get(identity, set()))
            copied = statement in _statements_reading_role().get(role, ())
            if same_fact or copied:
                return True
    return False


def unclaimed_line_record(line: UnclaimedFiledLine) -> dict[str, object]:
    """One unclaimed line as the build carries it to its validations: the line, its statement,
    modeled-period amounts, ``candidate_roles``, ``repair_owner`` ``binding`` and the line's
    ``change`` skeleton, the ``fms_persist_correspondence`` ``changes[]`` entry that places it
    (``role`` is concrete for a single candidate; otherwise the agent chooses it). Filing
    calculation weight and parent accompany the skeleton for composite offers. JSON-plain."""

    dimensions = [{"axis_qname": axis, "member_qname": member} for axis, member in line.dimensions]
    breakdown = "".join(f"[{axis}={member}]" for axis, member in line.dimensions)
    return {
        "code": "filed_line_unclaimed",
        "line_key": f"{line.statement}:{line.presentation_role_name}:{line.line_qname}{breakdown}",
        "statement": line.statement,
        "line_qname": line.line_qname,
        "label": line.label,
        "presentation_role_name": line.presentation_role_name,
        "dimensions": dimensions,
        "accessions": list(line.accessions),
        "periods": [{"fiscal_year": year, "amount": amount} for year, amount in line.amounts],
        "calculation_weight": line.calculation_weight,
        "calculation_parent_qname": line.calculation_parent_qname,
        "negates_value": line.negates_value,
        "candidate_roles": list(line.candidate_roles),
        **({"candidate_bound_to": list(line.candidate_bound_to)} if line.candidate_bound_to else {}),
        "repair_owner": "binding",
        "change": {
            "accessions": list(line.accessions),
            "role": line.candidate_roles[0] if len(line.candidate_roles) == 1 else "<one of candidate_roles>",
            "state": "bound",
            "source_ref": {
                "kind": "line_identity",
                "line_qname": line.line_qname,
                "statement": line.statement,
                "dimensions": dimensions,
            },
        },
    }


def unclaimed_line_message(record: Mapping[str, object]) -> str:
    """The finding text of one ``unclaimed_line_record``: what is wrong and the repair."""

    periods = cast(Sequence[Mapping[str, object]], record["periods"])
    rendered = ", ".join(f"FY{item['fiscal_year']}={item['amount']}" for item in periods)
    label = record["label"] or record["line_qname"]
    statement = str(record["statement"]).replace("_", " ")
    if record.get("candidate_bound_to"):
        bound_to = ", ".join(cast(Sequence[str], record["candidate_bound_to"]))
        return (
            f"{label!r} is the total the filing prints on the {statement} ({rendered}) and no binding row "
            f"claims it. Its candidate role is bound to {bound_to}, a line this total sums in the "
            "filing's calculation tree, so the role reads a subtotal and the statements stop footing. "
            "Rebind the candidate role to this line (one source_ref, not a composite with the line it "
            "reads now). Then call check_model again."
        )
    return (
        f"{label!r} is printed on the {statement} with a value ({rendered}) and no binding row "
        "claims it, so it is in the filed totals and in no template row. Bind it to a "
        "candidate role; where the role already reads another filed line, submit the role's "
        "`lines` [{source_ref, sign: 1|-1}, ...] (the filing's calculation_weight is the "
        "candidate sign) so both sum into it; or bind it to an operating.* role. Then "
        "call check_model again."
    )


__all__ = [
    "CHECKED_STATEMENTS",
    "UnclaimedFiledLine",
    "filed_lines_unclaimed",
    "unclaimed_line_message",
    "unclaimed_line_record",
]
