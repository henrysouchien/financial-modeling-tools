"""Rank filing presentation lines as statement-binding candidates.

The deterministic ranks are: (i) an unchanged prior-vintage binding, (ii)
calculation-arc position, (iii) an abstract presentation-tree parent plus an
explicit total preferred-label role, supported for arithmetic totals by the
observed catalog member roles in its calculation or presentation subtree, or the
period-start or period-end preferred-label role a cash-flow balance role names (the
line printed on that label leads the rank),
(iv) taxonomy standard-label or extension anchor evidence, and (v) the global
Edgar binding row as a seed. Both continuity endpoints come from sealed
occurrences, including calculation membership. Missing arcs cannot establish
continuity. The binding artifact never stores either tree parent or a second
continuity fact. Seeds supply evidence only, never binding authority.

A candidate is a presentation line, not one of its printed-period cells.  Cells
sharing its presentation arc are collapsed before ranking; choosing a period is
the pin's concern.
"""

from __future__ import annotations

import re
from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass
from types import MappingProxyType
from typing import Any, Literal

from schema.correspondence import (
    CalculationMembership,
    IdentityDrifted,
    IdentityUnchanged,
    PresentationLineRef,
    presentation_identity_unchanged,
)
from schema.provider_port.adapters.edgar.wire.filing_facts_v1 import FilingPresentationCellV1
from schema.filed_line_unclaimed import _sums_within
from schema.provider_port.adapters.edgar.line_bindings import EdgarLineBinding
from schema.provider_port.contracts import FilingDimensionRef
from schema.required_role_catalog import load_required_role_catalog
from schema.statement_bindings_v2 import total_role_composition
from schema.statement_topology import (
    occurrence_namespace_prefixes,
    occurrence_qname,
)

_TOTAL_LABEL_SUFFIXES = ("/negatedTotalLabel", "/totalLabel")
#: A cash-flow statement prints its period balances on lines whose preferred-label role says
#: which balance they are; the role's own leaf names the balance it wants.
_PERIOD_BALANCE_LABELS: Mapping[str, str] = MappingProxyType({
    "cash_flow_cash_end_of_period": "/periodEndLabel",
    "cash_flow_cash_begin_of_period": "/periodStartLabel",
})
_STANDARD_TAXONOMY_PREFIXES = frozenset(
    {
        "cef",
        "country",
        "currency",
        "dei",
        "ecd",
        "exch",
        "invest",
        "naics",
        "oef",
        "rr",
        "rxp",
        "sic",
        "spac",
        "srt",
        "stpr",
        "us-gaap",
        "utr",
        "vip",
    }
)


@dataclass(frozen=True, slots=True)
class CandidateIdentity:
    """Canonical tag-in-role and explicit dimension-set identity."""

    line_qname: str
    presentation_role_name: str
    dimensions: tuple[tuple[str, str], ...] = ()

    def __post_init__(self) -> None:
        object.__setattr__(self, "dimensions", tuple(sorted(set(self.dimensions))))


@dataclass(frozen=True, slots=True)
class CandidateOccurrenceCitation:
    """Fields that resolve one presentation line inside an accession."""

    accession: str
    preferred_label: str | None
    tree_depth: int
    cell_locator: str


@dataclass(frozen=True, slots=True)
class BindingCandidate:
    """One ranked presentation-line candidate for a required role."""

    role: str
    identity: CandidateIdentity
    rank: int
    reason: str
    citation: CandidateOccurrenceCitation


@dataclass(frozen=True, slots=True)
class PriorBinding:
    """A prior reviewed identity and the accession containing its endpoint."""

    role: str
    identity: CandidateIdentity
    accession: str


@dataclass(frozen=True, slots=True)
class LocatedLine:
    """A line identity located in one accession: the presentation arc that prints it.

    ``identity`` carries the qname in the filing's own prefixes (the qname that
    matched); ``citation`` names the arc's representative cell, ``cell``.
    """

    role: str
    identity: CandidateIdentity
    citation: CandidateOccurrenceCitation
    cell: FilingPresentationCellV1


@dataclass(frozen=True, slots=True)
class BindingContinuity:
    """Derived carry or facts for an existing-door proposal; never persisted."""

    continuity: Mapping[str, str] | None
    comparison: IdentityUnchanged | IdentityDrifted | None
    reason: str | None = None


@dataclass(frozen=True, slots=True)
class CandidateReport:
    """Deterministically ordered candidate lists and input-availability facts."""

    candidates_by_role: Mapping[str, tuple[BindingCandidate, ...]]
    diagnostics: tuple[str, ...]

    def to_json_dict(self) -> dict[str, Any]:
        """Return a stable, JSON-serializable representation of this report."""

        return {
            "candidates_by_role": {
                role: [
                    {
                        "role": candidate.role,
                        "identity": {
                            "line_qname": candidate.identity.line_qname,
                            "presentation_role_name": (
                                candidate.identity.presentation_role_name
                            ),
                            "dimensions": [
                                {"axis_qname": axis, "member_qname": member}
                                for axis, member in candidate.identity.dimensions
                            ],
                        },
                        "rank": candidate.rank,
                        "reason": candidate.reason,
                        "citation": {
                            "accession": candidate.citation.accession,
                            "preferred_label": candidate.citation.preferred_label,
                            "tree_depth": candidate.citation.tree_depth,
                            "cell_locator": candidate.citation.cell_locator,
                        },
                    }
                    for candidate in self.candidates_by_role[role]
                ]
                for role in sorted(self.candidates_by_role)
            },
            "diagnostics": list(self.diagnostics),
        }


@dataclass(frozen=True, slots=True)
class _PresentationLine:
    identity: CandidateIdentity
    cell: FilingPresentationCellV1
    collapsed: tuple[CandidateIdentity, ...]


def _dimensions(
    cell: FilingPresentationCellV1,
) -> tuple[tuple[str, str], ...] | None:
    assert cell.binding is not None and cell.binding.kind == "bound"
    dimensions: list[tuple[str, str]] = []
    for dimension in cell.binding.fact.dimensions:
        if dimension.kind != "explicit":
            return None
        dimensions.append(
            (
                dimension.axis.lexical_qname,
                dimension.member.lexical_qname,
            )
        )
    return tuple(sorted(set(dimensions)))


def _identity(
    cell: FilingPresentationCellV1,
    prefixes: Mapping[str, str] = MappingProxyType({}),
) -> CandidateIdentity | None:
    dimensions = _dimensions(cell)
    if dimensions is None:
        return None
    return CandidateIdentity(
        line_qname=occurrence_qname(cell.line_qname.lexical_qname, prefixes),
        presentation_role_name=cell.presentation.presentation_role_name,
        dimensions=dimensions,
    )


def _line_order(cell: FilingPresentationCellV1) -> tuple[Any, ...]:
    return (
        cell.source.cell_locator,
        cell.tree.depth,
        cell.preferred_label.text or "",
        cell.printed_column.position,
        cell.printed_column.heading,
    )


def _presentation_arc(
    cell: FilingPresentationCellV1, prefixes: Mapping[str, str],
) -> tuple[Any, ...]:
    parent = cell.tree.parent_qname
    calculation = cell.calculation
    return (
        cell.source.source_filename,
        cell.source.table_locator,
        cell.source.row_locator,
        occurrence_qname(parent.lexical_qname, prefixes) if parent else None,
        cell.tree.order,
        cell.tree.depth,
        cell.tree.sibling_index,
        cell.preferred_label.role_uri,
        (
            occurrence_qname(calculation.parent_qname, prefixes)
            if calculation.parent_qname else None,
            calculation.role_uri,
            calculation.weight,
        ) if calculation is not None else None,
    )


def _is_face_for_plane(cell: FilingPresentationCellV1, plane: str) -> bool:
    buckets = cell.presentation.statement_roles
    return plane in buckets and "disclosure" not in buckets


def _collapse_for_plane(
    cells: Sequence[FilingPresentationCellV1],
    plane: str,
    prefixes: Mapping[str, str],
) -> tuple[_PresentationLine, ...]:
    cells_by_arc: dict[
        tuple[CandidateIdentity, tuple[Any, ...]], list[FilingPresentationCellV1]
    ] = {}
    for cell in cells:
        identity = _identity(cell, prefixes)
        if identity is not None:
            key = (identity, _presentation_arc(cell, prefixes))
            cells_by_arc.setdefault(key, []).append(cell)

    lines_by_qname: dict[str, list[tuple[CandidateIdentity, FilingPresentationCellV1]]] = {}
    for (identity, _arc), arc_cells in cells_by_arc.items():
        representative = min(arc_cells, key=_line_order)
        lines_by_qname.setdefault(identity.line_qname, []).append(
            (identity, representative)
        )

    lines: list[_PresentationLine] = []
    for qname in sorted(lines_by_qname):
        exhibits = lines_by_qname[qname]
        winning_score = min(
            (
                0 if _is_face_for_plane(cell, plane) else 1,
                0 if not identity.dimensions else 1,
            )
            for identity, cell in exhibits
        )
        winners = [
            (identity, cell)
            for identity, cell in exhibits
            if (
                0 if _is_face_for_plane(cell, plane) else 1,
                0 if not identity.dimensions else 1,
            )
            == winning_score
        ]
        winning_identities = {identity for identity, _cell in winners}
        losers = tuple(
            sorted(
                {identity for identity, _cell in exhibits if identity not in winning_identities},
                key=lambda identity: (
                    identity.line_qname,
                    identity.presentation_role_name,
                    identity.dimensions,
                ),
            )
        )
        for identity, cell in sorted(winners, key=lambda item: _line_order(item[1])):
            lines.append(
                _PresentationLine(
                    identity=identity,
                    cell=cell,
                    collapsed=losers,
                )
            )
    return tuple(lines)


def _current_line_ref(
    line: _PresentationLine | LocatedLine, prefixes: Mapping[str, str],
) -> PresentationLineRef | None:
    cell = line.cell
    assert cell.binding is not None and cell.binding.kind == "bound"
    parent = cell.tree.parent_qname
    if parent is None:
        return None
    arc = cell.calculation
    calculation = None
    if (
        arc is not None
        and arc.parent_qname
        and arc.role_uri
        and arc.weight is not None
    ):
        calculation = CalculationMembership(
            parent_tag=occurrence_qname(arc.parent_qname, prefixes),
            role=arc.role_uri,
            weight=arc.weight,
        )
    return PresentationLineRef(
        accession=cell.accession,
        fiscal_year=cell.printed_column.period_ref.fiscal_year,
        statement_role=cell.presentation.presentation_role_name,
        tag=occurrence_qname(cell.line_qname.lexical_qname, prefixes),
        dimensions=tuple(
            FilingDimensionRef(axis, member)
            for axis, member in line.identity.dimensions
        ),
        presentation_parent_tag=occurrence_qname(parent.lexical_qname, prefixes),
        calculation=calculation,
        period_type=cell.binding.fact.period.kind,
    )


@dataclass(frozen=True, slots=True)
class OccurrenceLineIndex:
    """One filing set's presentation arcs, derived from its occurrences once.

    ``prefixes`` are the occurrences' namespace prefixes (they qualify an identity's
    qname as the filing writes it) and ``accessions`` the accessions the occurrences
    name. ``exhibits`` groups each accession's non-abstract bound cells by presentation
    role name; ``arcs`` holds, per exhibit and once :meth:`arcs_of` has asked for it,
    every identity's presentation arcs (an arc's representative is its first cell in
    locator order, arcs in locator order). A lookup therefore reads one exhibit's
    cells, never the filing's, and a holder that keeps the index keeps what every
    later lookup would re-derive. Pure in the occurrences (and ``prefixes`` when
    supplied), so a holder keys it by their content.
    """

    accessions: frozenset[str]
    prefixes: Mapping[str, str]
    exhibits: Mapping[tuple[str, str], tuple[FilingPresentationCellV1, ...]]
    arcs: dict[tuple[str, str], dict[CandidateIdentity, tuple[FilingPresentationCellV1, ...]]]

    def arcs_of(
        self, accession: str, identity: CandidateIdentity,
    ) -> tuple[FilingPresentationCellV1, ...]:
        """The representative cell of every arc of ``accession`` printing ``identity``."""

        exhibit = (accession, identity.presentation_role_name)
        by_identity = self.arcs.get(exhibit)
        if by_identity is None:
            by_identity = self.arcs.setdefault(
                exhibit, _exhibit_arcs(self.exhibits.get(exhibit, ()), self.prefixes)
            )
        return by_identity.get(identity, ())


def _exhibit_arcs(
    cells: Sequence[FilingPresentationCellV1], prefixes: Mapping[str, str],
) -> dict[CandidateIdentity, tuple[FilingPresentationCellV1, ...]]:
    grouped: dict[CandidateIdentity, dict[tuple[Any, ...], list[FilingPresentationCellV1]]] = {}
    for cell in cells:
        identity = _identity(cell, prefixes)
        if identity is not None:
            grouped.setdefault(identity, {}).setdefault(
                _presentation_arc(cell, prefixes), []
            ).append(cell)
    return {
        identity: tuple(
            sorted((min(arc, key=_line_order) for arc in by_arc.values()), key=_line_order)
        )
        for identity, by_arc in grouped.items()
    }


def index_occurrence_lines(
    occurrences: Sequence[FilingPresentationCellV1],
    *,
    prefixes: Mapping[str, str] | None = None,
) -> OccurrenceLineIndex:
    """The :class:`OccurrenceLineIndex` of ``occurrences``, one pass over them.

    ``prefixes`` are the occurrences' own unless the caller already holds the
    prefixes of the filing these cells were narrowed from.
    """

    if prefixes is None:
        prefixes = _namespace_prefixes(occurrences)
    accessions: set[str] = set()
    exhibits: dict[tuple[str, str], list[FilingPresentationCellV1]] = {}
    for cell in occurrences:
        accessions.add(cell.accession)
        if cell.line_kind != "abstract" and cell.binding is not None and cell.binding.kind == "bound":
            exhibits.setdefault(
                (cell.accession, cell.presentation.presentation_role_name), []
            ).append(cell)
    return OccurrenceLineIndex(
        accessions=frozenset(accessions),
        prefixes=prefixes,
        exhibits={exhibit: tuple(cells) for exhibit, cells in exhibits.items()},
        arcs={},
    )


def _lines_of(
    occurrences: Sequence[FilingPresentationCellV1] | OccurrenceLineIndex,
    prefixes: Mapping[str, str] | None = None,
) -> OccurrenceLineIndex:
    if isinstance(occurrences, OccurrenceLineIndex):
        return occurrences
    return index_occurrence_lines(occurrences, prefixes=prefixes)


def line_arcs(
    role: str,
    identity: CandidateIdentity,
    occurrences: Sequence[FilingPresentationCellV1] | OccurrenceLineIndex,
    *,
    accession: str,
    prefixes: Mapping[str, str] | None = None,
) -> tuple[LocatedLine, ...]:
    """Every presentation arc of ``accession`` that prints ``identity`` with a bound fact.

    One :class:`LocatedLine` per arc, its representative the arc's first cell in
    locator order. ``occurrences`` is the cells or the :class:`OccurrenceLineIndex`
    a caller that locates many lines holds (the index carries its own prefixes;
    ``prefixes`` is read only when ``occurrences`` are cells). ``prefixes`` are the
    occurrences' namespace prefixes when the caller already holds them (they qualify
    ``identity``'s qname as the filing writes it); otherwise they are read from
    ``occurrences``.
    """

    lines = _lines_of(occurrences, prefixes)
    wanted = CandidateIdentity(
        occurrence_qname(identity.line_qname, lines.prefixes),
        identity.presentation_role_name,
        identity.dimensions,
    )
    return tuple(
        LocatedLine(
            role=role,
            identity=wanted,
            citation=CandidateOccurrenceCitation(
                accession=cell.accession,
                preferred_label=cell.preferred_label.text,
                tree_depth=cell.tree.depth,
                cell_locator=cell.source.cell_locator,
            ),
            cell=cell,
        )
        for cell in lines.arcs_of(accession, wanted)
    )


def locate_line(
    role: str,
    identity: CandidateIdentity,
    occurrences: Sequence[FilingPresentationCellV1] | OccurrenceLineIndex,
    *,
    accession: str,
    prefixes: Mapping[str, str] | None = None,
) -> LocatedLine | Literal["ambiguous"] | None:
    """The one presentation arc of ``accession`` printing ``identity``, with its cell.

    ``None`` when no arc prints it with a bound fact; ``"ambiguous"`` when several
    arcs do: locator order cannot adjudicate arcs (:func:`line_arcs` lists them).
    This is the step continuity and ranking locate both endpoints with.
    """

    arcs = line_arcs(role, identity, occurrences, accession=accession, prefixes=prefixes)
    if len(arcs) > 1:
        return "ambiguous"
    return next(iter(arcs), None)


def _namespace_prefixes(
    occurrences: Sequence[FilingPresentationCellV1],
) -> dict[str, str]:
    return occurrence_namespace_prefixes(
        (cell.line_qname.namespace_uri, cell.line_qname.lexical_qname)
        for cell in occurrences
    )


def derive_binding_continuity(
    prior: PriorBinding,
    occurrences: Sequence[FilingPresentationCellV1] | OccurrenceLineIndex,
    prior_occurrences: Sequence[FilingPresentationCellV1] | OccurrenceLineIndex,
    *,
    successor_identity: CandidateIdentity | None = None,
) -> BindingContinuity:
    """Derive a carry, or return drift facts for the existing correspondence door.

    An omitted successor selects the prior identity, never a label or seed.
    A supplied identity is the adjudicator's candidate, not an approved change.
    Competing arcs at either endpoint require a proposal, never a locator pick.
    Neither this result nor the three-part comparison writes product state.
    Either endpoint is the filing's cells or the :class:`OccurrenceLineIndex` a
    caller that derives many carries holds, so the prefixes and the arc scan are
    derived once per filing, never per carry.
    """

    lines = _lines_of(occurrences)
    prior_lines = _lines_of(prior_occurrences)
    accessions = lines.accessions
    if len(accessions) != 1:
        raise ValueError("successor occurrences must belong to one accession")
    prefixes = lines.prefixes
    prior_prefixes = prior_lines.prefixes
    prior_identity = CandidateIdentity(
        occurrence_qname(prior.identity.line_qname, prior_prefixes),
        prior.identity.presentation_role_name,
        prior.identity.dimensions,
    )
    previous = locate_line(
        prior.role, prior_identity, prior_lines, accession=prior.accession,
    )
    if previous is None:
        return BindingContinuity(None, None, "prior_line_unresolved")
    if previous == "ambiguous":
        return BindingContinuity(None, None, "prior_line_ambiguous")
    identity = successor_identity or prior_identity
    identity = CandidateIdentity(
        occurrence_qname(identity.line_qname, prefixes),
        identity.presentation_role_name,
        identity.dimensions,
    )
    current = locate_line(
        prior.role, identity, lines, accession=next(iter(accessions)),
    )
    if current is None:
        return BindingContinuity(None, None, "line_absent")
    if current == "ambiguous":
        return BindingContinuity(None, None, "line_ambiguous")
    if current.identity.dimensions != previous.identity.dimensions:
        return BindingContinuity(None, None, "dimensions_changed")
    before = _current_line_ref(previous, prior_prefixes)
    after = _current_line_ref(current, prefixes)
    if before is None or after is None:
        return BindingContinuity(None, None, "endpoint_unresolved")
    comparison = presentation_identity_unchanged(before, after)
    if isinstance(comparison, IdentityUnchanged):
        return BindingContinuity(
            MappingProxyType({"carried_from": prior.accession}), comparison,
        )
    return BindingContinuity(None, comparison)


def _has_abstract_parent(
    line: _PresentationLine,
    plane: str,
    abstract_parents: frozenset[tuple[str, str, str]],
) -> bool:
    parent = line.cell.tree.parent_qname
    if parent is None:
        return False
    return (
        plane,
        line.cell.presentation.presentation_role_name,
        parent.lexical_qname,
    ) in abstract_parents


def _collapse_reason(identities: tuple[CandidateIdentity, ...]) -> str | None:
    if not identities:
        return None
    rendered = ", ".join(
        f"{identity.presentation_role_name}"
        f"[{', '.join(f'{axis}={member}' for axis, member in identity.dimensions)}]"
        for identity in identities
    )
    return f"collapsed lower-priority exhibits: {rendered}"


_LABEL_STOPWORDS = frozenset({"and", "of", "the", "net", "total"})


def _label_tokens(value: str) -> tuple[str, ...]:
    return tuple(
        token
        for token in re.findall(r"[a-z0-9]+", value.lower())
        if token not in _LABEL_STOPWORDS
    )


def _role_label_tokens(role: str) -> tuple[str, ...]:
    return _label_tokens(role.rpartition(".")[2])


def _total_rank_iii_evidence(
    occurrences: Sequence[FilingPresentationCellV1],
    catalog_roles: Sequence[tuple[str, str]],
    seed_rows: Sequence[EdgarLineBinding],
    bound_lines: Sequence[tuple[str, LocatedLine]],
    prefixes: Mapping[str, str],
) -> set[tuple[str, str, CandidateIdentity, str]]:
    """Index role-aware total evidence once per face exhibit and dimension set.

    Only observed mapped members are required; optional catalog members absent
    from the filing are not evidence against a total. Without observed members,
    the preferred-label evidence is unchanged. Otherwise the calculation subtree
    or preceding presentation siblings and their subtrees must cover those
    members without covering a mapped role outside the target total.
    Preferred-label text is never a source of member-role identity.
    """

    totals = {
        role: plane for role, plane in catalog_roles if total_role_composition(role)
    }
    if not totals:
        return set()
    catalog = load_required_role_catalog()
    role_ids = catalog.role_ids()
    role_of_item = {
        item: row.required_role
        for row in catalog.required_roles
        if row.alias_of is None
        for item in row.template_items
    }
    seeds: dict[tuple[str, str], set[str]] = {}
    for seed in seed_rows:
        seeds.setdefault(
            (seed.statement_role, occurrence_qname(seed.tag, prefixes)), set(),
        ).add(seed.required_role)
    bound_roles: dict[CandidateIdentity, set[str]] = {}
    for role, line in bound_lines:
        bound_roles.setdefault(line.identity, set()).add(role)
    mapped_roles = {seed.required_role for seed in seed_rows}
    mapped_roles.update(role for role, _line in bound_lines)
    members = {
        total: frozenset(
            role for role in mapped_roles
            if role != total
            and role in role_ids
            and catalog.role(role).statement == plane
            and catalog.role(role).template_items
            and _sums_within(total, plane, (role,), role_of_item)
        )
        for total, plane in totals.items()
    }
    exhibits: dict[tuple[str, str], list[FilingPresentationCellV1]] = {}
    for cell in occurrences:
        for plane in set(cell.presentation.statement_roles).intersection(totals.values()):
            if _is_face_for_plane(cell, plane):
                exhibits.setdefault(
                    (plane, cell.presentation.presentation_role_name), [],
                ).append(cell)

    supported: set[tuple[str, str, CandidateIdentity, str]] = set()
    for (plane, _exhibit), cells in exhibits.items():
        abstract_children: dict[str, dict[str, float | None]] = {}
        dimensions: dict[
            tuple[tuple[str, str], ...], list[tuple[CandidateIdentity, FilingPresentationCellV1]]
        ] = {}
        for cell in cells:
            parent = cell.tree.parent_qname
            if cell.line_kind == "abstract":
                if parent is not None:
                    abstract_children.setdefault(
                        occurrence_qname(parent.lexical_qname, prefixes), {},
                    )[occurrence_qname(cell.line_qname.lexical_qname, prefixes)] = cell.tree.order
            elif cell.binding is not None and cell.binding.kind == "bound":
                identity = _identity(cell, prefixes)
                if identity is not None:
                    dimensions.setdefault(identity.dimensions, []).append((identity, cell))
        for lines in dimensions.values():
            presentation = {
                parent: dict(children) for parent, children in abstract_children.items()
            }
            calculation: dict[str, set[str]] = {}
            roles_of_line: dict[str, set[str]] = {}
            for identity, cell in lines:
                qname = identity.line_qname
                roles_of_line.setdefault(qname, set()).update(
                    seeds.get((plane, qname), ()),
                )
                roles_of_line[qname].update(bound_roles.get(identity, ()))
                parent = cell.tree.parent_qname
                if parent is not None:
                    presentation.setdefault(
                        occurrence_qname(parent.lexical_qname, prefixes), {},
                    )[qname] = cell.tree.order
                arc = cell.calculation
                if (
                    arc is not None
                    and arc.parent_qname is not None
                    and arc.weight is not None
                    and arc.role_uri == cell.presentation.presentation_role_uri
                ):
                    calculation.setdefault(
                        occurrence_qname(arc.parent_qname, prefixes), set(),
                    ).add(qname)
            observed = {role for roles in roles_of_line.values() for role in roles}

            def subtree_roles(
                root: str,
                children: Mapping[str, Collection[str]],
                cache: dict[str, frozenset[str]],
            ) -> frozenset[str]:
                if root not in cache:
                    pending, reached = [root], {root}
                    while pending:
                        for child in children.get(pending.pop(), ()):
                            if child not in reached:
                                reached.add(child)
                                pending.append(child)
                    cache[root] = frozenset(
                        role for qname in reached for role in roles_of_line.get(qname, ())
                    )
                return cache[root]

            calculation_cache: dict[str, frozenset[str]] = {}
            presentation_cache: dict[str, frozenset[str]] = {}
            presentation_prefixes: dict[tuple[str, float | None], frozenset[str]] = {}

            def preceding_roles(parent: str, order: float | None) -> frozenset[str]:
                key = (parent, order)
                if key not in presentation_prefixes:
                    presentation_prefixes[key] = frozenset(
                        role
                        for child, child_order in presentation.get(parent, {}).items()
                        if order is not None and child_order is not None and child_order <= order
                        for role in subtree_roles(child, presentation, presentation_cache)
                    )
                return presentation_prefixes[key]

            for total, total_plane in totals.items():
                expected = observed.intersection(members[total])
                if plane != total_plane:
                    continue
                allowed = expected | {total}
                for identity, cell in lines:
                    parent = cell.tree.parent_qname
                    if (
                        parent is None
                        or not cell.preferred_label.role_uri.endswith(_TOTAL_LABEL_SUFFIXES)
                    ):
                        continue
                    if (
                        not expected
                        or expected <= subtree_roles(
                            identity.line_qname, calculation, calculation_cache,
                        ) <= allowed
                        or expected <= preceding_roles(
                            occurrence_qname(parent.lexical_qname, prefixes),
                            cell.tree.order,
                        ) <= allowed
                    ):
                        supported.add((total, plane, identity, cell.source.cell_locator))
    return supported



def rank_candidates(
    occurrences: Sequence[FilingPresentationCellV1],
    catalog_roles: Sequence[tuple[str, str]],
    prior: Sequence[PriorBinding] = (),
    prior_occurrences: Sequence[FilingPresentationCellV1] = (),
    seed_rows: Sequence[EdgarLineBinding] = (),
    taxonomy_labels: Mapping[str, str] = MappingProxyType({}),
    extension_anchors: Mapping[str, str] = MappingProxyType({}),
    bound: Sequence[PriorBinding] = (),
) -> CandidateReport:
    """Rank in-memory presentation occurrences for each catalog role."""

    eligible = tuple(
        cell
        for cell in occurrences
        if cell.line_kind != "abstract"
        and cell.binding is not None
        and cell.binding.kind == "bound"
    )
    accessions = {cell.accession for cell in occurrences}
    if len(accessions) > 1:
        raise ValueError("rank_candidates occurrences must belong to one accession")
    abstract_parents = frozenset(
        (
            plane,
            cell.presentation.presentation_role_name,
            cell.line_qname.lexical_qname,
        )
        for cell in occurrences
        if cell.line_kind == "abstract"
        and "disclosure" not in cell.presentation.statement_roles
        for plane in cell.presentation.statement_roles
    )
    prior_by_role = {binding.role: binding for binding in prior}
    seeds_by_role: dict[str, tuple[EdgarLineBinding, ...]] = {}
    for seed in seed_rows:
        seeds_by_role.setdefault(seed.required_role, ())
        seeds_by_role[seed.required_role] += (seed,)

    lines_index = index_occurrence_lines(occurrences)
    prior_index = index_occurrence_lines(prior_occurrences)
    prefixes = lines_index.prefixes
    prior_prefixes = prior_index.prefixes
    diagnostics: set[str] = set()
    if not any(
        cell.calculation is not None
        and cell.calculation.parent_qname
        and cell.calculation.role_uri
        and cell.calculation.weight is not None
        for cell in occurrences
    ):
        diagnostics.add("calc_arcs_unavailable")
    if any(_dimensions(cell) is None for cell in eligible):
        diagnostics.add("typed_dimensions_unrepresentable")
    lines_by_plane = {
        plane: _collapse_for_plane(eligible, plane, prefixes)
        for _role, plane in sorted(set(catalog_roles))
    }
    bound_lines: list[tuple[str, LocatedLine]] = []
    for binding in bound:
        line = locate_line(
            binding.role, binding.identity, lines_index, accession=binding.accession,
        )
        if isinstance(line, LocatedLine):
            bound_lines.append((binding.role, line))
    for binding in prior:
        if derive_binding_continuity(
            binding, lines_index, prior_index,
        ).continuity is not None:
            line = locate_line(
                binding.role,
                CandidateIdentity(
                    occurrence_qname(binding.identity.line_qname, prior_prefixes),
                    binding.identity.presentation_role_name,
                    binding.identity.dimensions,
                ),
                lines_index,
                accession=next(iter(accessions)),
            )
            if isinstance(line, LocatedLine):
                bound_lines.append((binding.role, line))
    total_rank_iii = _total_rank_iii_evidence(
        occurrences, catalog_roles, seed_rows, bound_lines, prefixes,
    )

    candidates_by_role: dict[str, tuple[BindingCandidate, ...]] = {}
    for role, plane in sorted(set(catalog_roles)):
        lines = lines_by_plane[plane]
        role_candidates: list[BindingCandidate] = []
        #: Lines printed on the period balance label the role names; they lead their rank.
        on_period_balance: set[str] = set()
        role_prior = prior_by_role.get(role)
        role_seeds = seeds_by_role.get(role, ())
        prior_line_ref: PresentationLineRef | None = None
        if role_prior is not None:
            prior_line = locate_line(
                role_prior.role, role_prior.identity, prior_index,
                accession=role_prior.accession,
            )
            if prior_line is None:
                diagnostics.add(f"prior_line_unresolved:{role}")
            elif prior_line == "ambiguous":
                diagnostics.add(f"prior_line_ambiguous:{role}")
            else:
                prior_line_ref = _current_line_ref(prior_line, prior_prefixes)

        for line in lines:
            evidence: list[tuple[int, str]] = []
            if (
                role_prior is not None
                and prior_line_ref is not None
                and prior_line_ref.tag == line.identity.line_qname
                and role_prior.identity.presentation_role_name
                == line.identity.presentation_role_name
                and role_prior.identity.dimensions == line.identity.dimensions
            ):
                current = locate_line(
                    role, line.identity, lines_index, accession=line.cell.accession,
                )
                current_line_ref = (
                    _current_line_ref(current, prefixes)
                    if isinstance(current, LocatedLine) else None
                )
                if current_line_ref is not None:
                    comparison = presentation_identity_unchanged(
                        prior_line_ref,
                        current_line_ref,
                    )
                    if comparison.outcome == "unchanged":
                        evidence.append(
                            (
                                1,
                                "rank i: prior binding unchanged under "
                                "tag-in-role + tree-parent + calculation comparison",
                            )
                        )

            arc = line.cell.calculation
            for bound_role, bound_line in bound_lines:
                if (
                    bound_role == role
                    or bound_line.identity == line.identity
                    or bound_line.identity.dimensions != line.identity.dimensions
                    or bound_line.identity.presentation_role_name
                    != line.identity.presentation_role_name
                    or not _is_face_for_plane(bound_line.cell, plane)
                ):
                    continue
                bound_arc = bound_line.cell.calculation
                if (
                    arc is not None
                    and arc.parent_qname is not None
                    and arc.weight is not None
                    and occurrence_qname(arc.parent_qname, prefixes)
                    == bound_line.identity.line_qname
                    and arc.role_uri == bound_line.cell.presentation.presentation_role_uri
                ):
                    evidence.append((
                        2, f"rank ii: calculation child of bound {bound_role}",
                    ))
                if (
                    bound_arc is not None
                    and bound_arc.parent_qname is not None
                    and bound_arc.weight is not None
                    and occurrence_qname(bound_arc.parent_qname, prefixes)
                    == line.identity.line_qname
                    and bound_arc.role_uri == line.cell.presentation.presentation_role_uri
                ):
                    evidence.append((
                        2, f"rank ii: calculation parent of bound {bound_role}",
                    ))

            if (
                line.cell.preferred_label.role_uri.endswith(_TOTAL_LABEL_SUFFIXES)
                and _has_abstract_parent(line, plane, abstract_parents)
                and _is_face_for_plane(line.cell, plane)
                and (
                    not total_role_composition(role)
                    or (role, plane, line.identity, line.cell.source.cell_locator)
                    in total_rank_iii
                )
            ):
                evidence.append(
                    (3, "rank iii: total preferred-label role under abstract parent")
                )

            balance_label = _PERIOD_BALANCE_LABELS.get(role.rpartition(".")[2])
            if (
                balance_label is not None
                and line.cell.preferred_label.role_uri.endswith(balance_label)
                and _is_face_for_plane(line.cell, plane)
            ):
                evidence.append((
                    3, f"rank iii: period balance preferred-label role {balance_label[1:]}",
                ))
                on_period_balance.add(line.cell.source.cell_locator)

            qname = line.identity.line_qname
            prefix = qname.partition(":")[0]
            taxonomy_label = taxonomy_labels.get(qname)
            extension_anchor = extension_anchors.get(qname)
            role_tokens = _role_label_tokens(role)
            if (
                prefix == "us-gaap"
                and taxonomy_label is not None
                and role_tokens
                and _label_tokens(taxonomy_label) == role_tokens
            ):
                evidence.append(
                    (4, f"rank iv: taxonomy label evidence {taxonomy_label!r}")
                )
            elif (
                prefix not in _STANDARD_TAXONOMY_PREFIXES
                and extension_anchor is not None
            ):
                evidence.append(
                    (4, f"rank iv: extension anchor evidence {extension_anchor!r}")
                )

            for seed in role_seeds:
                if seed.tag == line.identity.line_qname and seed.statement_role == plane:
                    evidence.append(
                        (5, f"rank v: global seed row for {seed.required_role}")
                    )

            if (
                prefix not in _STANDARD_TAXONOMY_PREFIXES
                and extension_anchor is None
            ):
                diagnostics.add(f"extension_unanchored:{qname}")

            if not evidence:
                continue
            evidence.sort(key=lambda item: (item[0], item[1]))
            reason_parts = [reason for _rank, reason in evidence]
            collapsed_reason = _collapse_reason(line.collapsed)
            if collapsed_reason is not None:
                reason_parts.append(collapsed_reason)
            role_candidates.append(
                BindingCandidate(
                    role=role,
                    identity=line.identity,
                    rank=evidence[0][0],
                    reason="; ".join(reason_parts),
                    citation=CandidateOccurrenceCitation(
                        accession=line.cell.accession,
                        preferred_label=line.cell.preferred_label.text,
                        tree_depth=line.cell.tree.depth,
                        cell_locator=line.cell.source.cell_locator,
                    ),
                )
            )

        candidates_by_role[role] = tuple(
            sorted(
                role_candidates,
                key=lambda candidate: (
                    candidate.rank,
                    candidate.citation.cell_locator not in on_period_balance,
                    candidate.identity.line_qname,
                    candidate.identity.presentation_role_name,
                    candidate.identity.dimensions,
                    candidate.citation.cell_locator,
                ),
            )
        )

    return CandidateReport(
        candidates_by_role=MappingProxyType(candidates_by_role),
        diagnostics=tuple(sorted(diagnostics)),
    )


__all__ = [
    "BindingCandidate",
    "BindingContinuity",
    "CandidateIdentity",
    "CandidateOccurrenceCitation",
    "CandidateReport",
    "LocatedLine",
    "OccurrenceLineIndex",
    "PriorBinding",
    "derive_binding_continuity",
    "index_occurrence_lines",
    "line_arcs",
    "locate_line",
    "rank_candidates",
]
