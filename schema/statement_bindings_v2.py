"""Pure decoder for corpus-authored statement bindings and sealed citations."""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, field, replace
from functools import lru_cache
import json
from types import MappingProxyType
import re
from typing import Any, Literal, NoReturn

from schema.coverage_profile import ANNUAL_REQUIRED_ROLE_PATTERN
from schema.provider_port.adapters.edgar.wire.filing_facts_v1 import FilingPresentationCellV1
from schema.provider_port.adapters.edgar.wire.operational_kpi_driver_rows_v2 import ScaledQuantity, TypedKpiObservation
from schema.provider_port.contracts import FilingDimensionRef
from schema.required_role_catalog import (
    TICKER_PLACEHOLDER,
    CatalogBalance,
    load_required_role_catalog,
)


class StatementBindingsError(ValueError):
    """A binding artifact contradicts its shape, role states, or sealed occurrences.

    ``clause`` names the one condition that failed; ``window``, ``role`` and
    ``accession`` locate it (``role`` is ``"<artifact>"`` or ``"<window>"`` above
    row level; ``accession`` is ``None`` where the clause names none).
    """

    def __init__(
        self, message: str, *, clause: str, window: str, role: object,
        accession: str | None = None,
    ) -> None:
        super().__init__(message)
        self.clause = clause
        self.window = window
        self.role = role
        self.accession = accession


@dataclass(frozen=True, slots=True)
class OccurrenceCitation:
    """The arc a bound row cites for ``accession``.

    ``vintage_of`` names a later filing whose comparative column re-presents
    ``accession``'s period: the arc is that filing's, and its value is the period's
    live vintage. ``None`` (the stored key absent) is ``accession``'s own arc.
    """

    accession: str
    preferred_label: str
    tree_depth: int
    cell_locator: str
    vintage_of: str | None = None


@dataclass(frozen=True, slots=True)
class AbsenceClosure:
    parent_local_name: str
    total_local_name: str


@dataclass(frozen=True, slots=True)
class NotApplicableCitation:
    accession: str
    presentation_role_uri: str
    absence_statement: str


@dataclass(frozen=True, slots=True)
class BoundBindingRow:
    line_qname: str
    presentation_role_name: str
    dimensions: tuple[FilingDimensionRef, ...]
    reviewed_occurrences: tuple[OccurrenceCitation, ...]
    continuity: Literal["first_bind"] | Mapping[str, str]
    absence_closure: AbsenceClosure | None
    disclosure_plane: bool
    model_item: str | None = None
    state: Literal["bound"] = field(default="bound", init=False)


@dataclass(frozen=True, slots=True)
class CompositeBindingRow:
    """A role bound to N>=2 filed lines whose per-period signed sum is its value.

    The structural bridge's (B) composite. Each line is a complete single-line
    ``BoundBindingRow`` (its own citations, continuity and closure); ``signs``
    is parallel to ``lines`` and carries each line's authored ``+1``/``-1``.
    """

    lines: tuple[BoundBindingRow, ...]
    signs: tuple[Literal[1, -1], ...]
    model_item: str | None = None
    state: Literal["bound"] = field(default="bound", init=False)

    @property
    def disclosure_plane(self) -> bool:
        return self.lines[0].disclosure_plane


@dataclass(frozen=True, slots=True)
class FormulaBindingRow:
    formula_id: str
    state: Literal["formula"] = field(default="formula", init=False)


@dataclass(frozen=True, slots=True)
class NotApplicableBindingRow:
    citation: NotApplicableCitation
    model_item: str | None = None
    state: Literal["not_applicable"] = field(default="not_applicable", init=False)


@dataclass(frozen=True, slots=True)
class NotSeparatelyPresentedBindingRow:
    citation: NotApplicableCitation
    included_in: str
    model_item: str | None = None
    state: Literal["not_separately_presented"] = field(default="not_separately_presented", init=False)


@dataclass(frozen=True, slots=True)
class KpiCitation:
    """One captured ``filing_text`` KPI row: its index in the accession's capture and its span."""

    accession: str
    row: int
    span: str


#: The ``UnboundBindingRow.reason`` (and the pin cell's ``unbound_reason``) the agent declares on a stored
#: ``unbound`` row when a window's filings do not print a role another window binds: declared by the agent
#: after reading the matrix, never inferred from omission; a historical absence, never a missing source
#: (``AbsenceDisposition`` carries the same literal).
NOT_DISCLOSED_IN_VINTAGE = "not_disclosed_in_vintage"


@dataclass(frozen=True, slots=True)
class UnboundBindingRow:
    """A role the window's filings leave unmapped. ``reason`` is free text, or
    :data:`NOT_DISCLOSED_IN_VINTAGE` when the agent declares the filings do not print the role (an
    earlier vintage that predates the line); that reason builds a historical absence
    (``document.mark_unavailable``), never ``missing_source``. A window that merely omits a role is a gap
    (``statement_binding_window_gaps``), not this reason."""

    reason: str
    #: An ``operating.*`` role the filings print only as a KPI, one citation per accession.
    kpi_citations: tuple[KpiCitation, ...] = ()
    model_item: str | None = None
    state: Literal["unbound"] = field(default="unbound", init=False)


BindingRow = (
    BoundBindingRow | CompositeBindingRow | FormulaBindingRow | NotApplicableBindingRow
    | NotSeparatelyPresentedBindingRow | UnboundBindingRow
)


@dataclass(frozen=True, slots=True)
class VintageWindow:
    window_id: str
    accessions: tuple[str, ...]
    rows: Mapping[str, BindingRow]


@dataclass(frozen=True, slots=True)
class StatementBindings:
    ticker: str
    cik: str
    vintage_windows: tuple[VintageWindow, ...]


def _composition_item_template(balance: CatalogBalance) -> str:
    """The workbook slot a catalog balance's binding names when its row names none."""

    return balance.target_item or balance.semantic_item(TICKER_PLACEHOLDER)


#: Stored binding keys that spell a catalog balance by a retired name, to
#: ``(balance role, workbook slot)``. The catalog's ``legacy_roles`` own the
#: spellings and its balances own the slot, so this is a read of them. Delete
#: a spelling from the catalog when no stored binding carries it (measured).
LEGACY_COMPOSITION_ROLE_KEYS: Mapping[str, tuple[str, str]] = MappingProxyType({
    legacy: (
        balance.required_role,
        _composition_item_template(balance),
    )
    for balance in load_required_role_catalog().balances.values()
    for legacy in balance.legacy_roles
})


_CANONICAL_ROLE_KEY_BY_LEGACY: Mapping[str, str] = MappingProxyType({
    legacy: canonical for legacy, (canonical, _slot) in LEGACY_COMPOSITION_ROLE_KEYS.items()
})

#: The workbook slot each catalog operating balance populates when its binding names none;
#: derived from the catalog, so one table owns the slot. An explicit ``model_item`` wins.
_DEFAULT_COMPOSITION_ITEM_BY_ROLE: Mapping[str, str] = MappingProxyType({
    balance.required_role: _composition_item_template(balance)
    for balance in load_required_role_catalog().balances.values()
    if balance.node_driven
})


def _canonical_binding_role(role: str) -> str:
    return _CANONICAL_ROLE_KEY_BY_LEGACY.get(role, role)


_ROLE_KEY_RE = re.compile(ANNUAL_REQUIRED_ROLE_PATTERN)
RETIRED_SGA_FORMULA_ID = "statement.is.selling_general_and_administrative"
"""The formula id combined SG&A once carried (S&M + G&A); stored bindings still name it.

SG&A is a filed face line with its own optional template row, so no formula owns it. A stored
``formula`` row on this role keeps its bytes and still decodes (SGA-DESTINATION-AND-OPTIONAL-PARTICIPATION);
``retired_formula_nonparticipation`` reads it as cited non-participation. Delete this constant and that
branch of the reader when no stored binding carries the id (measured).
"""


def retired_formula_nonparticipation(
    role: str, row: BindingRow, *, window_id: str, accessions: Sequence[str],
) -> BindingRow:
    """The one reader of a stored ``formula`` row; any other row unchanged.

    A ``formula`` row decodes by shape alone (``formula_id == role``), so a stored artifact never
    refuses. What it *means* is read here and nothing is written back:

    - **SG&A** derived from its S&M and G&A rows (the retired SG&A formula) and bound no combined
      line of its own, so it reads as a cited ``not_applicable``: a filing-truth statement, "no
      combined SG&A line was bound". A filer whose G&A row carries the combined line (NVDA, ISRG)
      prints one, and the stored binding does not claim otherwise. The citation's basis is the
      stored row itself: its window and role.
    - **Every other role**: the derivation lives on the template formula or the business-model
      node, so the row reads as ``unbound`` ("no filed line is bound", all a ``formula`` row ever
      said). An ``optional_if_unreported`` role then owes nothing (``derive_role_dispositions``);
      an operating role leaves the pin.

    Delete the whole reader, ``FormulaBindingRow`` and its decode and encode branches when no stored
    binding carries a ``formula`` row (measured over the tracked corpora, the fixture and the
    evidence stores); SG&A's own gate is above and is not met.
    """
    if not isinstance(row, FormulaBindingRow):
        return row
    if role == RETIRED_SGA_FORMULA_ID:
        return NotApplicableBindingRow(
            NotApplicableCitation(
                accession=accessions[0],
                presentation_role_uri=f"statement_bindings.v2:{window_id}:{role}",
                absence_statement=(
                    "The stored binding derived SG&A from its sales and marketing and general and "
                    "administrative rows (the retired SG&A formula); no combined SG&A line was bound."
                ),
            )
        )
    return UnboundBindingRow(
        "The stored binding named a formula for this role; no filed line is bound. "
        "The derivation lives on the template formula or the business-model node."
    )


def composition_leaf(
    role: str, row: Mapping[str, Any], *, ticker: str,
) -> tuple[str, str] | None:
    """Resolve an issuer composition leaf to its operating role and workbook item.

    Delete the legacy entries when no stored binding carries those keys (measured).
    Catalog roles otherwise take their workbook items from the required-role catalog.
    A canonical operating balance names the same slot as its legacy key unless its row
    names another (``_DEFAULT_COMPOSITION_ITEM_BY_ROLE``).
    """
    legacy = LEGACY_COMPOSITION_ROLE_KEYS.get(role)
    if legacy is not None:
        canonical_role, model_item = legacy
    elif role.startswith("operating.") and "model_item" in row:
        canonical_role, model_item = role, row["model_item"]
    elif role in _DEFAULT_COMPOSITION_ITEM_BY_ROLE:
        canonical_role, model_item = role, _DEFAULT_COMPOSITION_ITEM_BY_ROLE[role]
    else:
        return None
    return canonical_role, model_item.format(ticker=ticker.lower())


_FACE_ROLES = frozenset(
    {"balance_sheet", "income_statement", "comprehensive_income", "equity", "cash_flow"}
)
_BOUND_FIELDS = frozenset(
    {
        "state", "line_qname", "presentation_role_name", "dimensions",
        "reviewed_occurrences", "continuity", "absence_closure",
    }
)
_COMPOSITE_FIELDS = frozenset({"state", "lines"})
_COMPOSITE_LINE_FIELDS = (_BOUND_FIELDS - {"state"}) | {"sign"}
_WINDOW_FIELDS = frozenset({"window_id", "accessions", "rows"})
_KPI_CITATION_FIELDS = frozenset({"accession", "row", "span"})
#: Operating KPI citations support counts, percentages, per-count rates and dollar amounts.
_KPI_CITABLE_UNITS = frozenset({"count", "percentage", "per_count", "dollars"})


def _invalid(
    window: str, role: object, reason: str, *, clause: str, accession: str | None = None,
) -> NoReturn:
    raise StatementBindingsError(
        f"statement_bindings.v2 window {window!r}, role {role!r}: {reason}",
        clause=clause, window=window, role=role, accession=accession,
    )


def _nonblank(value: object) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _unresolved(
    line_qname: str, presentation_role_name: str, detail: str, *, clause: str,
) -> UnboundBindingRow:
    """A bound row whose citation or absence closure does not resolve in the sealed cells.

    Resolution is a fact about the filings, not the artifact's shape: the row is
    decoded unbound, its reason leading with the clause, and the build carries it
    as that role's unknown-applicability coverage.
    """
    return UnboundBindingRow(
        f"{clause}: bound row {line_qname} in {presentation_role_name!r}: {detail}; recorded unbound"
    )


def _bound_row(
    raw: Mapping,
    *,
    window: str,
    role: str,
    accessions: tuple[str, ...],
    prior_accessions: Collection[str],
    cells_by_accession: Mapping[str, Sequence[FilingPresentationCellV1]],
    previous_plane: bool | None,
) -> BoundBindingRow | UnboundBindingRow:
    if set(raw) != _BOUND_FIELDS:
        _invalid(
            window, role,
            f"bound row keys must be exactly {sorted(_BOUND_FIELDS)!r}: "
            f"missing={sorted(_BOUND_FIELDS - set(raw))!r}, "
            f"extra={sorted(map(str, set(raw) - _BOUND_FIELDS))!r}",
            clause="bound_row_keys",
        )
    if not isinstance(raw["line_qname"], str) or ":" not in raw["line_qname"]:
        _invalid(window, role, "line_qname must be a prefixed qname (prefix:LocalName)", clause="line_qname_not_prefixed")
    if not isinstance(raw["presentation_role_name"], str) or not raw["presentation_role_name"]:
        _invalid(window, role, "presentation_role_name must be nonblank text", clause="presentation_role_name_blank")
    if not isinstance(raw["dimensions"], list):
        _invalid(window, role, "dimensions must be a list", clause="dimensions_not_list")
    if not isinstance(raw["reviewed_occurrences"], list):
        _invalid(window, role, "reviewed_occurrences must be a list", clause="reviewed_occurrences_not_list")
    line_qname = raw["line_qname"]
    presentation_role_name = raw["presentation_role_name"]
    continuity = raw["continuity"]
    if continuity != "first_bind":
        if (
            not isinstance(continuity, Mapping)
            or set(continuity) != {"carried_from"}
            or not _nonblank(continuity["carried_from"])
            or continuity["carried_from"] not in prior_accessions
        ):
            _invalid(window, role, "bound continuity is invalid", clause="bound_continuity")
        continuity = MappingProxyType(dict(continuity))
    raw_closure = raw["absence_closure"]
    closure = None
    if raw_closure is not None:
        if (
            not isinstance(raw_closure, Mapping)
            or set(raw_closure) != {"parent_local_name", "total_local_name"}
            or not all(_nonblank(value) for value in raw_closure.values())
        ):
            _invalid(window, role, "absence_closure is invalid", clause="absence_closure_shape")
        closure = AbsenceClosure(**raw_closure)
    dimensions: list[FilingDimensionRef] = []
    for dimension in raw["dimensions"]:
        if (
            not isinstance(dimension, Mapping)
            or set(dimension) != {"axis_qname", "member_qname"}
        ):
            _invalid(window, role, "bound dimensions are invalid", clause="bound_dimensions")
        try:
            dimensions.append(FilingDimensionRef(**dimension))
        except (TypeError, ValueError) as exc:
            _invalid(window, role, f"bound dimensions are invalid: {exc}", clause="bound_dimensions")
    dimension_pairs = tuple((item.axis_qname, item.member_qname) for item in dimensions)
    if (
        dimension_pairs != tuple(sorted(set(dimension_pairs)))
        or len({axis for axis, _member in dimension_pairs}) != len(dimension_pairs)
    ):
        _invalid(window, role, "bound dimensions are not canonical", clause="bound_dimensions_not_canonical")

    citations: list[OccurrenceCitation] = []
    cited_accessions: set[str] = set()
    disclosure_plane = previous_plane
    for citation in raw["reviewed_occurrences"]:
        if (
            not isinstance(citation, Mapping)
            or set(citation) - {"vintage_of"}
            != {"accession", "preferred_label", "tree_depth", "cell_locator"}
            or not isinstance(citation["accession"], str)
            or citation["accession"] not in accessions
            or citation["accession"] in cited_accessions
            or not isinstance(citation["preferred_label"], str)
            or not citation["preferred_label"]
            or type(citation["tree_depth"]) is not int
            or citation["tree_depth"] < 0
            or not isinstance(citation["cell_locator"], str)
            or not citation["cell_locator"]
            or (
                "vintage_of" in citation
                and (
                    not _nonblank(citation["vintage_of"])
                    or citation["vintage_of"] == citation["accession"]
                )
            )
        ):
            cited = citation.get("accession") if isinstance(citation, Mapping) else None
            _invalid(
                window, role, "bound citation is invalid or repeats an accession",
                clause="bound_citation_shape", accession=cited if isinstance(cited, str) else None,
            )
        accession = citation["accession"]
        # A vintage citation's arc is the later filing's that re-presents this period.
        source = citation.get("vintage_of", accession)
        unpinned = next((item for item in (accession, source) if item not in cells_by_accession), None)
        if unpinned is not None:
            return _unresolved(line_qname, presentation_role_name, f"citation accession {unpinned!r} is not pinned", clause="citation_accession_not_pinned")
        citation_planes: set[bool] = set()
        for cell in cells_by_accession[source]:
            if (
                cell.accession != source
                or cell.line_qname.lexical_qname != line_qname
                or cell.presentation.presentation_role_name != presentation_role_name
                or cell.preferred_label.text != citation["preferred_label"]
                or cell.tree.depth != citation["tree_depth"]
                or cell.source.cell_locator != citation["cell_locator"]
                or cell.binding is None
                or cell.binding.kind != "bound"
            ):
                continue
            cell_pairs: list[tuple[str, str]] = []
            for dimension in cell.binding.fact.dimensions:
                if dimension.kind != "explicit":
                    _invalid(window, role, f"citation {accession!r} dimension is not explicit", clause="citation_dimension_not_explicit", accession=accession)
                try:
                    ref = FilingDimensionRef(
                        dimension.axis.lexical_qname, dimension.member.lexical_qname
                    )
                except (TypeError, ValueError) as exc:
                    _invalid(window, role, f"citation {accession!r} dimension is invalid: {exc}", clause="citation_dimension_invalid", accession=accession)
                cell_pairs.append((ref.axis_qname, ref.member_qname))
            if tuple(sorted(cell_pairs)) == dimension_pairs:
                citation_planes.add(is_disclosure_plane(cell.presentation.statement_roles))
        if not citation_planes:
            return _unresolved(line_qname, presentation_role_name, f"citation {accession!r} does not resolve with exact dimensions", clause="citation_unresolved")
        if len(citation_planes) != 1:
            _invalid(window, role, f"citation {accession!r} crosses presentation planes", clause="citation_crosses_planes", accession=accession)
        citation_plane = next(iter(citation_planes))
        if disclosure_plane is not None and disclosure_plane != citation_plane:
            _invalid(window, role, "bound row crosses presentation planes", clause="row_crosses_planes", accession=accession)
        disclosure_plane = citation_plane
        citations.append(OccurrenceCitation(**citation))
        cited_accessions.add(accession)

    for accession in accessions:
        if accession in cited_accessions:
            continue
        if closure is None or accession not in cells_by_accession:
            return _unresolved(line_qname, presentation_role_name, f"accession {accession!r} has neither citation nor corroborated absence", clause="accession_without_citation_or_absence")
        local_names: set[str] = set()
        for cell in cells_by_accession[accession]:
            if (
                cell.accession == accession
                and cell.presentation.presentation_role_name == presentation_role_name
            ):
                if cell.line_qname.lexical_qname == line_qname:
                    _invalid(window, role, f"uncited accession {accession!r} does not prove line absence", clause="absence_closure_line_present", accession=accession)
                local_names.add(cell.line_qname.local_name)
        if not {closure.parent_local_name, closure.total_local_name}.issubset(local_names):
            return _unresolved(line_qname, presentation_role_name, f"absence_closure does not resolve for {accession!r}", clause="absence_closure_unresolved")
    return BoundBindingRow(
        line_qname=line_qname,
        presentation_role_name=presentation_role_name,
        dimensions=tuple(dimensions),
        reviewed_occurrences=tuple(citations),
        continuity=continuity,
        absence_closure=closure,
        disclosure_plane=bool(disclosure_plane),
    )


@lru_cache(maxsize=1)
def _template_items() -> Mapping[str, Mapping[str, object]]:
    """The generic template's items by id."""
    from schema.templates.template_builder_config import SIA_GENERIC_TEMPLATE_PATH

    document = json.loads(SIA_GENERIC_TEMPLATE_PATH.read_text(encoding="utf-8"))
    items: dict[str, Mapping[str, object]] = {}
    stack: list[object] = [document]
    while stack:
        node = stack.pop()
        if isinstance(node, Mapping):
            if "item_type" in node and isinstance(node.get("id"), str):
                items[node["id"]] = node
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)
    return MappingProxyType(items)


def _historical_type(item: Mapping[str, object]) -> object:
    historical = item.get("historical")
    return historical.get("type") if isinstance(historical, Mapping) else None


def _projected_statement_sum(item: Mapping[str, object]) -> bool:
    """A forecast sum of face rows remains a total when its history is filed input."""
    projected = item.get("projected")
    if not isinstance(projected, Mapping) or projected.get("type") != "arithmetic":
        return False
    params = projected.get("params")
    operands = params.get("operands") if isinstance(params, Mapping) else None
    return (
        isinstance(operands, list)
        and len(operands) > 2
        and operands[0] == "+"
        and all(
            isinstance(operand, Mapping)
            and isinstance(operand.get("id"), str)
            and operand["id"].rsplit(".", 1)[0] == str(item["id"]).rsplit(".", 1)[0]
            for operand in operands[1:]
        )
    )


@lru_cache(maxsize=1)
def _computed_template_items() -> frozenset[str]:
    """The generic template's computed rows, including forecast statement totals.

    A nonconstant historical formula is computed rather than a filed leaf. A forecast
    sum of statement face rows is still a total when history must bind the filer's
    printed total instead of borrowing a value from another statement.
    """
    return frozenset(
        item_id
        for item_id, item in _template_items().items()
        if (
            isinstance(item.get("historical"), Mapping) and _historical_type(item) != "constant"
        ) or _projected_statement_sum(item)
    )


def _referenced_item_ids(spec: object) -> set[str]:
    """Every template item id a spec (a ``historical`` expression) reads."""
    referenced: set[str] = set()
    stack: list[object] = [spec]
    while stack:
        node = stack.pop()
        if isinstance(node, Mapping):
            if isinstance(node.get("id"), str):
                referenced.add(node["id"])
            stack.extend(node.values())
        elif isinstance(node, list):
            stack.extend(node)
    return referenced


@lru_cache(maxsize=None)
def total_role_composition(role: str) -> frozenset[str]:
    """The template items the template foots ``role``'s value from: every item its stamped
    items' arithmetic ``historical`` specs read, through subtotals. Empty for a role whose
    stamped items the template does not compute by arithmetic (a leaf, or a reference such as
    ending cash that no component of the filing foots into)."""
    catalog = load_required_role_catalog()
    if role not in catalog.role_ids():
        return frozenset()
    items = _template_items()
    pending = [
        item_id for item_id in catalog.role(role).template_items
        if item_id in items and _historical_type(items[item_id]) == "arithmetic"
    ]
    composition: set[str] = set()
    while pending:
        item = items[pending.pop()]
        for item_id in _referenced_item_ids(item.get("historical")):
            if item_id in composition:
                continue
            composition.add(item_id)
            if item_id in items and _historical_type(items[item_id]) == "arithmetic":
                pending.append(item_id)
    return frozenset(composition)


@lru_cache(maxsize=1)
def residual_totals() -> Mapping[str, str]:
    """Each residual role by the role of the total it plugs (the catalog's ``residual_of``).

    A residual's total is footed by the template over the residual's own item: the filed lines
    no other row claims are absorbed into the residual, and the total still foots."""
    catalog = load_required_role_catalog()
    return MappingProxyType({
        row.required_role: row.residual_of
        for row in catalog.required_roles
        if row.residual_of is not None
    })


def composite_target_refusal(role: str) -> str | None:
    """Why ``role`` cannot be a multi-line bound row's target, or ``None`` when it can.

    The structural-integrity rule of the bridge's (B) composite: N filed lines sum into
    a catalog face role. A role whose stamped template item the template computes from other
    items (a total, a subtotal, a roll-forward or a ratio: ``_computed_template_items``) is an
    identity, never a value an issuer's lines are summed into.
    """
    catalog = load_required_role_catalog()
    if role not in catalog.role_ids():
        return f"{role!r} is not a catalog role"
    if not role.startswith("statement."):
        return f"{role!r} is not a statement face role"
    computed = sorted(set(catalog.role(role).template_items) & _computed_template_items())
    if computed:
        return f"{role!r} stamps {computed[0]}, a row the template computes from other rows"
    return None


def is_composite_target(role: str) -> bool:
    return composite_target_refusal(role) is None


def composite_target_roles(statement: str | None = None) -> tuple[str, ...]:
    """The catalog roles a multi-line bound row may target, in catalog order.

    ``statement`` is the catalog statement value (``balance_sheet``, ``income_statement``,
    ``cash_flow``); ``None`` lists every statement's.
    """
    return tuple(
        item.required_role
        for item in load_required_role_catalog().required_roles
        if (statement is None or item.statement == statement)
        and is_composite_target(item.required_role)
    )


def bound_lines(row: BindingRow) -> tuple[BoundBindingRow, ...]:
    """The filed lines a row claims: its one line, a composite's N, none for other states."""
    if isinstance(row, BoundBindingRow):
        return (row,)
    if isinstance(row, CompositeBindingRow):
        return row.lines
    return ()


def stored_bound_lines(row: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    """``bound_lines`` for a stored (undecoded) row: a bound row's one line (the row itself)
    or a composite's ``lines`` entries (each carrying its ``sign``); none for other states."""
    if row.get("state") != "bound":
        return ()
    lines = row.get("lines")
    if isinstance(lines, list):
        return tuple(line for line in lines if isinstance(line, Mapping))
    return (row,)


def _line_identity_of(line: BoundBindingRow) -> LineIdentity:
    return (
        line.line_qname,
        line.presentation_role_name,
        tuple((item.axis_qname, item.member_qname) for item in line.dimensions),
    )


def _composite_row(
    raw: Mapping,
    *,
    window: str,
    role: str,
    accessions: tuple[str, ...],
    prior_accessions: Collection[str],
    cells_by_accession: Mapping[str, Sequence[FilingPresentationCellV1]],
    previous_plane: bool | None,
) -> CompositeBindingRow | UnboundBindingRow:
    if set(raw) != _COMPOSITE_FIELDS:
        _invalid(
            window, role,
            f"composite bound row keys must be exactly {sorted(_COMPOSITE_FIELDS)!r}: "
            f"missing={sorted(_COMPOSITE_FIELDS - set(raw))!r}, "
            f"extra={sorted(map(str, set(raw) - _COMPOSITE_FIELDS))!r}",
            clause="composite_row_keys",
        )
    refusal = composite_target_refusal(role)
    if refusal is not None:
        _invalid(window, role, f"a bound row of several lines cannot target it: {refusal}", clause="composite_target_invalid")
    raw_lines = raw["lines"]
    if not isinstance(raw_lines, list) or len(raw_lines) < 2:
        _invalid(window, role, "composite lines must be a list of at least two lines", clause="composite_lines_count")
    lines: list[BoundBindingRow] = []
    signs: list[Literal[1, -1]] = []
    plane = previous_plane
    for raw_line in raw_lines:
        if not isinstance(raw_line, Mapping) or set(raw_line) != _COMPOSITE_LINE_FIELDS:
            _invalid(
                window, role,
                f"each composite line's keys must be exactly {sorted(_COMPOSITE_LINE_FIELDS)!r}",
                clause="composite_line_keys",
            )
        sign = raw_line["sign"]
        if type(sign) is not int or sign not in (1, -1):
            _invalid(window, role, "composite line sign must be the integer 1 or -1", clause="composite_line_sign")
        decoded = _bound_row(
            {"state": "bound", **{key: value for key, value in raw_line.items() if key != "sign"}},
            window=window, role=role, accessions=accessions,
            prior_accessions=prior_accessions, cells_by_accession=cells_by_accession,
            previous_plane=plane,
        )
        if isinstance(decoded, UnboundBindingRow):
            return decoded
        plane = decoded.disclosure_plane
        lines.append(decoded)
        signs.append(sign)
    identities = [_line_identity_of(line) for line in lines]
    if len(set(identities)) != len(identities):
        _invalid(window, role, "composite lines repeat a filed line", clause="composite_line_duplicate")
    return CompositeBindingRow(lines=tuple(lines), signs=tuple(signs))


def _comparative_kpi_citation_allowed(
    row: TypedKpiObservation,
    *,
    row_year: int | None,
    filing_year: int | None,
    kpi_rows_by_accession: Mapping[str, tuple[TypedKpiObservation, ...]] | None,
    filing_years_by_accession: Mapping[str, int | None] | None,
) -> bool:
    """Whether ``row`` (from a later filing's comparative column) is citable for ``row_year``."""
    if (
        row_year is None
        or filing_year is None
        or row_year >= filing_year
        or kpi_rows_by_accession is None
        or filing_years_by_accession is None
    ):
        return False
    earlier_accessions = [
        acc for acc, fy in filing_years_by_accession.items() if fy == row_year
    ]
    if not earlier_accessions:
        return False
    series_key = row.series.issuer_series_key
    metric_key = row.metric_name.strip().lower()
    for earlier_acc in earlier_accessions:
        for candidate in kpi_rows_by_accession.get(earlier_acc, ()):
            if (
                candidate.period is not None
                and candidate.period.fiscal_year == row_year
                and isinstance(candidate.value, ScaledQuantity)
                and candidate.calculation_ready
                and (
                    candidate.series.issuer_series_key == series_key
                    or candidate.metric_name.strip().lower() == metric_key
                )
            ):
                return False
    return True


def _kpi_row_unresolved(
    citation: KpiCitation,
    rows: tuple[TypedKpiObservation, ...],
    cells: Sequence[FilingPresentationCellV1],
    *,
    kpi_rows_by_accession: Mapping[str, tuple[TypedKpiObservation, ...]] | None = None,
    cells_by_accession: Mapping[str, Sequence[FilingPresentationCellV1]] | None = None,
    admitted_units: frozenset[str] = _KPI_CITABLE_UNITS,
) -> str | None:
    """Why a cited KPI row is not an admitted own-year or comparative KPI row, or ``None``."""
    if citation.row >= len(rows):
        return f"names no row of the {len(rows)} captured KPI rows"
    row = rows[citation.row]
    if row.locator.span != citation.span:
        return f"is at span {row.locator.span!r}, not {citation.span!r}"
    fiscal_year = max((cell.printed_column.period_ref.fiscal_year for cell in cells), default=None)
    row_year = row.period.fiscal_year if row.period is not None else None
    if row_year != fiscal_year:
        filing_years = (
            {
                acc: max((c.printed_column.period_ref.fiscal_year for c in acc_cells), default=None)
                for acc, acc_cells in cells_by_accession.items()
            }
            if cells_by_accession is not None
            else None
        )
        if not _comparative_kpi_citation_allowed(
            row,
            row_year=row_year,
            filing_year=fiscal_year,
            kpi_rows_by_accession=kpi_rows_by_accession,
            filing_years_by_accession=filing_years,
        ):
            return (
                f"reports fiscal year {row_year!r}, not the filing's own year "
                f"{fiscal_year!r} (a comparative row)"
            )
    if not isinstance(row.value, ScaledQuantity):
        return "carries no numeric value"
    if not row.calculation_ready:
        return "is not calculation_ready"
    if row.value.measure.unit not in admitted_units:
        return f"has unit {row.value.measure.unit!r}; only {sorted(admitted_units)!r} are citable"
    return None


def _kpi_citations(
    raw: object,
    *,
    window: str,
    role: str,
    accessions: tuple[str, ...],
    cells_by_accession: Mapping[str, Sequence[FilingPresentationCellV1]],
    kpi_rows_by_accession: Mapping[str, tuple[TypedKpiObservation, ...]] | None,
) -> tuple[KpiCitation, ...]:
    if not role.startswith("operating."):
        _invalid(window, role, "only an operating.* role may cite captured KPI rows", clause="kpi_citation_role")
    if not isinstance(raw, list) or not raw:
        _invalid(
            window, role, "kpi_citations must be a nonempty list of {accession, row, span}",
            clause="kpi_citation_shape",
        )
    window_years: set[int] = {
        fy
        for acc in accessions
        if (
            fy := max(
                (c.printed_column.period_ref.fiscal_year for c in cells_by_accession.get(acc, ())),
                default=None,
            )
        )
        is not None
    }
    citations: list[KpiCitation] = []
    for item in raw:
        if (
            not isinstance(item, Mapping)
            or set(item) != _KPI_CITATION_FIELDS
            or not isinstance(item["row"], int)
            or isinstance(item["row"], bool)
            or item["row"] < 0
            or not _nonblank(item["span"])
            or not isinstance(item.get("accession"), str)
        ):
            _invalid(
                window, role,
                "each kpi citation is exactly {accession, row, span}: an accession of this window, "
                "the row's index in that filing's captured KPI rows and its source span",
                clause="kpi_citation_shape",
            )
        cit_acc = item["accession"]
        cit_row = item["row"]
        if cit_acc not in accessions:
            comparative_in_window = False
            if cit_acc in cells_by_accession:
                cit_fy = max(
                    (c.printed_column.period_ref.fiscal_year for c in cells_by_accession.get(cit_acc, ())),
                    default=None,
                )
                if kpi_rows_by_accession is None:
                    comparative_in_window = (
                        cit_fy is not None and bool(window_years) and cit_fy > min(window_years)
                    )
                else:
                    cit_rows = kpi_rows_by_accession.get(cit_acc, ())
                    if cit_row < len(cit_rows) and (cit_period := cit_rows[cit_row].period) is not None and cit_fy is not None:
                        r_fy = cit_period.fiscal_year
                        comparative_in_window = r_fy < cit_fy and r_fy in window_years
            if not comparative_in_window:
                _invalid(
                    window, role,
                    "each kpi citation is exactly {accession, row, span}: an accession of this window, "
                    "the row's index in that filing's captured KPI rows and its source span",
                    clause="kpi_citation_shape",
                )
        citation = KpiCitation(cit_acc, cit_row, item["span"])
        if any((prior.accession, prior.row) == (citation.accession, citation.row) for prior in citations):
            _invalid(
                window, role, "at most one kpi citation per accession and row",
                clause="kpi_citation_shape", accession=citation.accession,
            )
        if kpi_rows_by_accession is not None:
            detail = _kpi_row_unresolved(
                citation,
                kpi_rows_by_accession.get(citation.accession, ()),
                cells_by_accession.get(citation.accession, ()),
                kpi_rows_by_accession=kpi_rows_by_accession,
                cells_by_accession=cells_by_accession,
            )
            if detail is not None:
                _invalid(
                    window, role,
                    f"kpi citation row {citation.row} at {citation.span!r} {detail}; cite the "
                    "filing's own-year row (as_filed) or an allowed comparative row of a citable KPI unit",
                    clause="kpi_citation_unresolved", accession=citation.accession,
                )
        citations.append(citation)
    return tuple(citations)


def statement_binding_required_roles(
    catalog_roles: Collection[str],
    profile_roles: Collection[str],
    *,
    presented_roles: Collection[str] = (),
) -> frozenset[str]:
    """Combine catalog roles with statement/operating profile roles, not adjustments.

    An ``optional_if_unreported`` catalog role is owed by a window only when the
    issuer's profile declares it or the window's captured filings present it (rule b).
    """
    from schema.required_role_catalog import load_required_role_catalog

    catalog = load_required_role_catalog()
    unreported = {
        r.required_role for r in catalog.required_roles if r.optional_if_unreported
    }
    catalog_set = frozenset(catalog_roles)
    base_catalog = catalog_set - {r for r in unreported if r in catalog_set}
    extra_profile = frozenset(
        _canonical_binding_role(role)
        for role in profile_roles
        if role.startswith(("statement.", "operating."))
    )
    extra_presented = frozenset(
        role for role in presented_roles if role in catalog_set
    )
    return frozenset(base_catalog | extra_profile | extra_presented)


def decode_statement_bindings(
    payload: Mapping,
    *,
    ticker: str,
    cik: str | None,
    occurrence_cells_by_accession: Mapping[str, Sequence[FilingPresentationCellV1]],
    kpi_rows_by_accession: Mapping[str, tuple[TypedKpiObservation, ...]] | None = None,
) -> StatementBindings:
    """Decode internally consistent windows against sealed filing cells.

    Each refusal is a :class:`StatementBindingsError` naming one ``clause``. A
    bound row whose citation or absence closure does not resolve in the sealed
    cells (``citation_accession_not_pinned``, ``citation_unresolved``,
    ``accession_without_citation_or_absence``, ``absence_closure_unresolved``)
    is not a refusal: it decodes as an :class:`UnboundBindingRow` whose reason
    leads with that clause. File integrity, the accession partition and role
    coverage (within and between windows) stay with callers, through
    :func:`statement_binding_window_gaps`; a role a window omits has no row for
    that window's accessions. Profile-required role coverage belongs to pin
    construction.

    An ``unbound`` ``operating.*`` row may cite captured KPI rows
    (``kpi_citations``). ``kpi_rows_by_accession`` (accession → its captured
    ``filing_text`` KPI rows in capture order) is the door's: given, each
    citation must name the filing's own-year row (or a comparative row whose
    vintage's own 10-K did not present the table) of an admitted KPI unit
    (``kpi_citation_unresolved``); ``None`` (a sealed artifact the door
    admitted) decodes the citation's shape only.
    """
    if (
        not isinstance(payload, Mapping)
        or set(payload) != {"schema_version", "issuer", "vintage_windows"}
        or payload["schema_version"] != "statement_bindings.v2"
    ):
        _invalid(
            "<artifact>", "<artifact>",
            "artifact must be exactly {schema_version: 'statement_bindings.v2', issuer, vintage_windows}",
            clause="artifact_shape",
        )
    issuer = payload["issuer"]
    if (
        not isinstance(issuer, Mapping)
        or set(issuer) != {"ticker", "cik"}
        or issuer["ticker"] != ticker
        or not _nonblank(issuer["cik"])
        or (cik is not None and issuer["cik"] != cik)
    ):
        _invalid("<artifact>", "<artifact>", "issuer identity is invalid", clause="issuer_identity")
    raw_windows = payload["vintage_windows"]
    if not isinstance(raw_windows, list) or not raw_windows:
        _invalid("<artifact>", "<artifact>", "vintage windows must be a nonempty list", clause="windows_not_nonempty_list")
    window_ids: set[str] = set()
    seen_accessions: set[str] = set()
    disclosure_planes: dict[str, bool] = {}
    windows: list[VintageWindow] = []
    for index, raw_window in enumerate(raw_windows):
        window = f"#{index}"
        if isinstance(raw_window, Mapping) and isinstance(raw_window.get("window_id"), str):
            window = raw_window["window_id"]
        if not isinstance(raw_window, Mapping):
            _invalid(window, "<window>", "a vintage window must be an object {window_id, accessions, rows}", clause="window_not_object")
        if set(raw_window) != _WINDOW_FIELDS:
            _invalid(
                window, "<window>",
                f"window keys must be exactly {sorted(_WINDOW_FIELDS)!r}: "
                f"missing={sorted(_WINDOW_FIELDS - set(raw_window))!r}, "
                f"extra={sorted(map(str, set(raw_window) - _WINDOW_FIELDS))!r}",
                clause="window_keys",
            )
        if not _nonblank(raw_window["window_id"]):
            _invalid(window, "<window>", "window_id must be nonblank text", clause="window_id_blank")
        if raw_window["window_id"] in window_ids:
            _invalid(window, "<window>", "window_id repeats an earlier window", clause="window_id_duplicate")
        if not isinstance(raw_window["accessions"], list):
            _invalid(window, "<window>", "accessions must be a list of filing accession strings", clause="accessions_not_list")
        if not raw_window["accessions"]:
            _invalid(window, "<window>", "accessions must name at least one filing accession", clause="accessions_empty")
        if not all(isinstance(value, str) and value for value in raw_window["accessions"]):
            _invalid(window, "<window>", "every accession must be nonblank text", clause="accession_not_text")
        if not isinstance(raw_window["rows"], Mapping):
            _invalid(
                window, "<window>",
                f"rows must be an object keyed by exact role name whose values are the rows, "
                f"not a {type(raw_window['rows']).__name__}",
                clause="rows_not_role_keyed",
            )
        accessions = tuple(raw_window["accessions"])
        accession_set = set(accessions)
        if len(accessions) != len(accession_set):
            _invalid(window, "<window>", "window accessions are not unique", clause="window_accessions_duplicate")
        repeated = sorted(accession_set & seen_accessions)
        if repeated:
            _invalid(window, "<window>", "accession repeats across windows", clause="accession_repeats_across_windows", accession=repeated[0])
        window_ids.add(window)
        raw_rows = raw_window["rows"]
        rows: dict[str, BindingRow] = {}
        for role, raw in raw_rows.items():
            if not isinstance(role, str) or not isinstance(raw, Mapping):
                _invalid(window, role, "row role must be text and its row an object", clause="row_shape")
            if not _ROLE_KEY_RE.fullmatch(role):
                _invalid(
                    window, role,
                    f"row key is not a required-role key (pattern {ANNUAL_REQUIRED_ROLE_PATTERN!r})",
                    clause="role_key_invalid",
                )
            state = raw.get("state")
            model_item = raw.get("model_item")
            if "model_item" in raw:
                if (
                    not role.startswith("operating.")
                    or state not in ("bound", "not_applicable", "unbound", "not_separately_presented")
                    or not _nonblank(model_item)
                ):
                    _invalid(
                        window, role,
                        "model_item must be nonblank text on an operating leaf row",
                        clause="model_item_invalid",
                    )
                raw = {key: value for key, value in raw.items() if key != "model_item"}
            if state == "bound":
                build = _composite_row if "lines" in raw else _bound_row
                row = build(
                    raw, window=window, role=role, accessions=accessions,
                    prior_accessions=seen_accessions,
                    cells_by_accession=occurrence_cells_by_accession,
                    previous_plane=disclosure_planes.get(role),
                )
                if isinstance(row, (BoundBindingRow, CompositeBindingRow)):
                    disclosure_planes[role] = row.disclosure_plane
                rows[role] = row
            elif state == "formula":
                if (
                    set(raw) != {"state", "formula_id"}
                    or not isinstance(raw["formula_id"], str)
                    or raw["formula_id"] != role
                ):
                    _invalid(window, role, "a formula row's formula_id must be its own role", clause="formula_id_not_role")
                rows[role] = FormulaBindingRow(raw["formula_id"])
            elif state in ("not_applicable", "not_separately_presented"):
                citation = raw.get("citation")
                fields = {"state", "citation"}
                if state == "not_separately_presented":
                    fields.add("included_in")
                if (
                    set(raw) != fields
                    or not isinstance(citation, Mapping)
                    or set(citation) != {"accession", "presentation_role_uri", "absence_statement"}
                    or not all(_nonblank(value) for value in citation.values())
                    or citation["accession"] not in accessions
                    or (
                        state == "not_separately_presented"
                        and (
                            not _nonblank(raw.get("included_in"))
                            or raw["included_in"] not in load_required_role_catalog().role_ids()
                        )
                    )
                ):
                    _invalid(window, role, f"{state} citation or parent is invalid", clause=f"{state}_citation_shape")
                if state == "not_separately_presented":
                    parent = load_required_role_catalog().role(raw["included_in"])
                    # Accounting integrity: an inclusion relates a component to a
                    # distinct presented aggregate, never the catalog role to itself.
                    if parent.required_role == role:
                        _invalid(
                            window, role,
                            f"included_in {parent.required_role!r} cannot contain itself",
                            clause="not_separately_presented_self_parent",
                            accession=citation["accession"],
                        )
                if not any(
                    cell.accession == citation["accession"]
                    and cell.presentation.presentation_role_uri == citation["presentation_role_uri"]
                    for cell in occurrence_cells_by_accession.get(citation["accession"], ())
                ):
                    _invalid(window, role, f"{state} citation does not resolve", clause=f"{state}_unresolved", accession=citation["accession"])
                cited = NotApplicableCitation(**citation)
                rows[role] = (
                    NotSeparatelyPresentedBindingRow(cited, raw["included_in"])
                    if state == "not_separately_presented" else NotApplicableBindingRow(cited)
                )
            elif state == "unbound":
                if (
                    set(raw) not in ({"state", "reason"}, {"state", "reason", "kpi_citations"})
                    or not _nonblank(raw["reason"])
                ):
                    _invalid(window, role, "unbound reason is invalid", clause="unbound_reason")
                rows[role] = UnboundBindingRow(
                    raw["reason"],
                    _kpi_citations(
                        raw["kpi_citations"], window=window, role=role, accessions=accessions,
                        cells_by_accession=occurrence_cells_by_accession,
                        kpi_rows_by_accession=kpi_rows_by_accession,
                    ) if "kpi_citations" in raw else (),
                )
            else:
                _invalid(
                    window, role,
                    f"row state is invalid: {state!r}; one of 'bound', 'formula', 'not_applicable', 'not_separately_presented', 'unbound'",
                    clause="row_state",
                )
            if model_item is not None:
                rows[role] = replace(rows[role], model_item=model_item)
        windows.append(VintageWindow(window, accessions, MappingProxyType(rows)))
        seen_accessions.update(accessions)
    return StatementBindings(issuer["ticker"], issuer["cik"], tuple(windows))


def is_disclosure_plane(statement_roles: Collection[str]) -> bool:
    """Whether a cell with these statement roles sits on the disclosure plane, not a face."""
    return "disclosure" in statement_roles and not _FACE_ROLES.intersection(statement_roles)


def _encode_line_fields(row: BoundBindingRow) -> dict[str, object]:
    return {
        "line_qname": row.line_qname,
        "presentation_role_name": row.presentation_role_name,
        "dimensions": [
            {"axis_qname": item.axis_qname, "member_qname": item.member_qname}
            for item in row.dimensions
        ],
        "reviewed_occurrences": [
            {
                "accession": item.accession,
                "preferred_label": item.preferred_label,
                "tree_depth": item.tree_depth,
                "cell_locator": item.cell_locator,
                **({} if item.vintage_of is None else {"vintage_of": item.vintage_of}),
            }
            for item in row.reviewed_occurrences
        ],
        "continuity": row.continuity if row.continuity == "first_bind" else dict(row.continuity),
        "absence_closure": None if row.absence_closure is None else {
            "parent_local_name": row.absence_closure.parent_local_name,
            "total_local_name": row.absence_closure.total_local_name,
        },
    }


def encode_binding_row(row: BindingRow) -> dict[str, object]:
    """One row as its ``statement_bindings.v2`` object; ``disclosure_plane`` is derived, not stored."""
    model_item = (
        {} if isinstance(row, FormulaBindingRow) or row.model_item is None
        else {"model_item": row.model_item}
    )
    if isinstance(row, BoundBindingRow):
        return {"state": "bound", **_encode_line_fields(row), **model_item}
    if isinstance(row, CompositeBindingRow):
        return {
            "state": "bound",
            **model_item,
            "lines": [
                {**_encode_line_fields(line), "sign": sign}
                for line, sign in zip(row.lines, row.signs, strict=True)
            ],
        }
    if isinstance(row, FormulaBindingRow):
        return {"state": "formula", "formula_id": row.formula_id}
    if isinstance(row, (NotApplicableBindingRow, NotSeparatelyPresentedBindingRow)):
        return {
            "state": row.state,
            **model_item,
            **({"included_in": row.included_in} if isinstance(row, NotSeparatelyPresentedBindingRow) else {}),
            "citation": {
                "accession": row.citation.accession,
                "presentation_role_uri": row.citation.presentation_role_uri,
                "absence_statement": row.citation.absence_statement,
            },
        }
    encoded: dict[str, object] = {"state": "unbound", "reason": row.reason, **model_item}
    if row.kpi_citations:
        encoded["kpi_citations"] = [
            {"accession": item.accession, "row": item.row, "span": item.span}
            for item in row.kpi_citations
        ]
    return encoded


def encode_statement_bindings(bindings: StatementBindings) -> dict[str, object]:
    """The document :func:`decode_statement_bindings` decodes back to ``bindings``.

    Its stored form is ``statement_binding_source_bytes(canonical_json_bytes(doc))``.
    A bound row the decoder recorded unbound (its citation or closure did not
    resolve) encodes as that unbound row, so a document whose rows all resolve
    is the one that round-trips byte for byte.
    """
    return {
        "schema_version": "statement_bindings.v2",
        "issuer": {"ticker": bindings.ticker, "cik": bindings.cik},
        "vintage_windows": [
            {
                "window_id": window.window_id,
                "accessions": list(window.accessions),
                "rows": {role: encode_binding_row(row) for role, row in window.rows.items()},
            }
            for window in bindings.vintage_windows
        ],
    }


@dataclass(frozen=True, slots=True)
class StatementBindingWindowGaps:
    """The partition and packaging-coverage verdict of one set of windows.

    ``missing_roles`` and ``extra_roles`` share keys: the windows whose role
    set differs from the required set, in window order.
    """

    missing_roles: Mapping[str, tuple[str, ...]]
    extra_roles: Mapping[str, tuple[str, ...]]
    uncovered_accessions: tuple[str, ...]
    unknown_accessions: tuple[str, ...]
    out_of_order: bool

    @property
    def partition_holds(self) -> bool:
        return not (self.uncovered_accessions or self.unknown_accessions or self.out_of_order)

    @property
    def coverage_holds(self) -> bool:
        return not self.missing_roles


def _is_formula_row(row: object) -> bool:
    """A decoded ``formula`` row, or the stored object a window built from an undecoded artifact holds."""
    return (row.get("state") if isinstance(row, Mapping) else getattr(row, "state", None)) == "formula"


def statement_binding_window_gaps(
    windows: Sequence[VintageWindow],
    required_roles: Collection[str],
    accessions: Sequence[str],
) -> StatementBindingWindowGaps:
    """Where windows fail to partition ``accessions`` in order or to cover the roles.

    The one rule behind the correspondence door's partition check, packaging's
    role coverage and ``corpus_state``'s ``self_check``: windows partition the
    source accessions exactly, in their order, and every window's role set
    equals ``required_roles``. Only role keys and accessions are read, so a
    window built from an undecoded persisted artifact gets the same verdict.
    """
    required = frozenset(_canonical_binding_role(role) for role in required_roles)
    missing: dict[str, tuple[str, ...]] = {}
    extra: dict[str, tuple[str, ...]] = {}
    for window in windows:
        # A stored ``formula`` row is a legacy derivation marker, never a role the window owes or
        # over-supplies: a role outside the required set it names is not an extra role.
        listed = {
            role: _canonical_binding_role(role)
            for role, row in window.rows.items()
            if _canonical_binding_role(role) in required or not _is_formula_row(row)
        }
        roles = frozenset(listed.values())
        if roles != required:
            missing[window.window_id] = tuple(sorted(required - roles))
            extra[window.window_id] = tuple(
                sorted(role for role, canonical in listed.items() if canonical not in required)
            )
    flat = tuple(accession for window in windows for accession in window.accessions)
    flat_set = frozenset(flat)
    source = frozenset(accessions)
    return StatementBindingWindowGaps(
        missing_roles=MappingProxyType(missing),
        extra_roles=MappingProxyType(extra),
        uncovered_accessions=tuple(a for a in accessions if a not in flat_set),
        unknown_accessions=tuple(dict.fromkeys(a for a in flat if a not in source)),
        out_of_order=tuple(a for a in flat if a in source)
        != tuple(a for a in accessions if a in flat_set),
    )


LineIdentity = tuple[str, str, tuple[tuple[str, str], ...]]


@dataclass(frozen=True, slots=True)
class LineAbsence:
    """One accession that lacks a line, with the closure pair that could prove it."""

    accession: str
    parent_local_name: str | None
    total_local_name: str | None
    closure_resolves: bool


@dataclass(frozen=True, slots=True)
class LinePresence:
    present_in: tuple[str, ...]
    absent_in: tuple[LineAbsence, ...]


@dataclass(frozen=True, slots=True)
class PresentationRoleNameDrift:
    """A face statement that changed presentation role name between two accessions."""

    from_name: str
    to_name: str
    accessions: tuple[str, str]


@dataclass(frozen=True, slots=True)
class AbsenceClosureCandidates:
    lines: Mapping[LineIdentity, LinePresence]
    presentation_role_name_drift: tuple[PresentationRoleNameDrift, ...]


def _local_name(qname: str) -> str:
    if qname.startswith("{"):
        return qname.partition("}")[2]
    return qname.rpartition(":")[2]


def _face_line_identity(cell: FilingPresentationCellV1) -> LineIdentity | None:
    if (
        cell.line_kind == "abstract"
        or cell.binding is None
        or cell.binding.kind != "bound"
        or not _FACE_ROLES.intersection(cell.presentation.statement_roles)
    ):
        return None
    pairs: list[tuple[str, str]] = []
    for dimension in cell.binding.fact.dimensions:
        if dimension.kind != "explicit":
            return None
        pairs.append((dimension.axis.lexical_qname, dimension.member.lexical_qname))
    return (
        cell.line_qname.lexical_qname,
        cell.presentation.presentation_role_name,
        tuple(sorted(pairs)),
    )


def absence_closure_candidates(
    occurrences_by_accession: Mapping[str, Sequence[FilingPresentationCellV1]],
) -> AbsenceClosureCandidates:
    """Each face line's presence across accessions, and its closure where absent.

    For an accession that lacks a line identity, the closure pair is the line's
    presentation parent and calculation total where it is present (newest
    first), and ``closure_resolves`` applies the decoder's closure rule
    (``_bound_row``'s uncited-accession branch): the line's qname is not in the
    presentation role there and both named lines are. Accession order is the
    mapping's; drift compares adjacent accessions. Pure; proposes nothing.
    """
    order = tuple(occurrences_by_accession)
    present: dict[LineIdentity, list[str]] = {}
    parents: dict[LineIdentity, str] = {}
    totals: dict[LineIdentity, str] = {}
    role_qnames: dict[tuple[str, str], set[str]] = {}
    role_locals: dict[tuple[str, str], set[str]] = {}
    face_roles: dict[str, dict[str, tuple[frozenset[str], set[str]]]] = {}
    for accession in reversed(order):
        buckets: dict[str, tuple[frozenset[str], set[str]]] = {}
        for cell in occurrences_by_accession[accession]:
            role_name = cell.presentation.presentation_role_name
            role_qnames.setdefault((accession, role_name), set()).add(
                cell.line_qname.lexical_qname
            )
            role_locals.setdefault((accession, role_name), set()).add(
                cell.line_qname.local_name
            )
            face = frozenset(_FACE_ROLES.intersection(cell.presentation.statement_roles))
            if face:
                buckets.setdefault(role_name, (face, set()))[1].add(
                    cell.line_qname.lexical_qname
                )
            identity = _face_line_identity(cell)
            if identity is None:
                continue
            accessions = present.setdefault(identity, [])
            if not accessions or accessions[-1] != accession:
                accessions.append(accession)
            if identity not in parents and cell.tree.parent_qname is not None:
                parents[identity] = cell.tree.parent_qname.local_name
            if (
                identity not in totals
                and cell.calculation is not None
                and cell.calculation.parent_qname
            ):
                totals[identity] = _local_name(cell.calculation.parent_qname)
        face_roles[accession] = buckets

    lines: dict[LineIdentity, LinePresence] = {}
    for identity, accessions in present.items():
        line_qname, role_name, _ = identity
        parent, total = parents.get(identity), totals.get(identity)
        absent: list[LineAbsence] = []
        for accession in order:
            if accession in accessions:
                continue
            resolves = (
                parent is not None
                and total is not None
                and line_qname not in role_qnames.get((accession, role_name), ())
                and {parent, total}.issubset(role_locals.get((accession, role_name), ()))
            )
            absent.append(LineAbsence(accession, parent, total, resolves))
        lines[identity] = LinePresence(
            tuple(a for a in order if a in accessions), tuple(absent)
        )

    drift: list[PresentationRoleNameDrift] = []
    for previous, current in zip(order, order[1:]):
        before, after = face_roles[previous], face_roles[current]
        added = [name for name in after if name not in before]
        for name in before:
            if name in after:
                continue
            face, qnames = before[name]
            best: tuple[int, str] | None = None
            for candidate in added:
                other_face, other_qnames = after[candidate]
                overlap = len(qnames & other_qnames)
                if (
                    face & other_face
                    and 2 * overlap >= min(len(qnames), len(other_qnames))
                    and (best is None or overlap > best[0])
                ):
                    best = (overlap, candidate)
            if best is not None:
                drift.append(PresentationRoleNameDrift(name, best[1], (previous, current)))
    return AbsenceClosureCandidates(MappingProxyType(lines), tuple(drift))
