"""The required-role catalog — build-model structure as authored data.

The model-structure/data boundary is described in
``docs/reference/filing-native-data-vs-build-model-structure-2026-08-17.md``;
the owner and checked-in configuration map is ``schema/README.md``.

One string once answered two questions: *what is this number* (data) and
*what does this model kind need* (structure). Only the second belongs here,
under names that cannot be confused with a data
column: a **required role** is a position a model kind needs filled — "the
revenue line". A role is never a series of numbers, never a second name
for a filing column, and never a key anything is stored under.

What a role binds to is the pinned filing column (accession + ``PeriodRef``
+ tag-in-role, `schema.provider_port.contracts.FilingLineRef`) resolved for
*that* pin. The catalog says which positions exist and which template items
occupy them; it never says what fills them.

**This module is a reader, not a second store.** The rows live in
``schema/templates/required_role_catalog.json`` — authored product data,
per template kind and never per ticker, so a second issuer is a data edit
and not a Python edit (the fourth amendment's rule, realized as
``tests/test_issuer_generic_grep_guard.py``). The artifact was seeded from
the two living structure sources and nothing else — the generic template's
stamp map and the structure fields of the data taxonomy — and step D-4.1
merged in the third, the other authored role→item store that had grown
beside them. Its own ``provenance`` string carries both derivations.

Where it fits the one required-set derivation: `derive_required_set`
(`schema/required_set.py`) takes a model kind's role-catalog rows and
window-scopes them. That input carries a *period* half — windows, measures,
downstream capabilities — which is model-kind coverage data, and a
*structure* half — the role, the statement plane it sits on, the template
items it binds, whether it is optional. This catalog owns the structure
half for the generic template kind; the model kind's coverage profile owns
the period half. Nothing here mints a requirement row: the mints re-key at
D-4.

A role binds on more than one plane, and the planes are named separately
because they are separately true. ``template_items`` / ``template_build_items``
are the stamped face plane — the compiled and build-pass ids the template's
own stamps carry. Step D-4.1 merged in the second authored role→item store
(the deleted ``schema/model_kind_role_catalog.py``) as two more namespaces:
``population_items``, the items statement/schedule population and the identity
fill bind for a role the stamps do not carry, and ``hff_fact_items``, the exact
items the accepted-HFF fact channel lands a role's disclosed filing-note
balance on. One role may bind different items on different planes and that is
not a conflict to resolve: ``statement.bs.current_lease_liability`` populates
the face-statement liability by identity while its HFF fact seeds the lease
schedule, so the presentation is not double counted. The namespaces are
therefore never pooled — the one-item-one-role check runs inside each.

A *balance* is a position a business-model driver may declare it is
(``balance_of``): a ``balance_sheet`` row authored ``balance: true`` (its face
line is the target) or an ``operating_balances`` row for a balance the template
owns no schedule for. A driver moves a balance only when its movement lands in
a cash-flow row: a working-capital or other-non-cash balance (the aggregates
the template builds from ``working_capital`` / ``other_noncash``) or a
pass-through balance with a cash-flow leg of its own. Every other balance row
names the ``projection_owner`` that already writes its projection (the cash-flow
ending cash, a roll-forward, a template schedule) and is no balance a driver
may be. ``balance_roles()`` is the declarable set, derived here and never
listed elsewhere; ``counterpart`` pairs a balance with the obligation it is
held against, ``pass_through`` marks one whose gross turnover is not the
company's own, and ``legacy_roles`` are the retired spellings stored revisions
and bindings still carry (``CatalogBalance``).

Decoding is hand-rolled over ``json`` in the form
:mod:`schema.model_kind_registry` established for authored model-kind data.
The catalog is checked-in configuration, so JSON types are not re-checked: a
wrong type fails where it is used. A semantic malformation (an unknown key, a
bad id, a duplicate or unclaimed binding) is a typed
refusal naming the first offending field and the lane that repairs it.
"""

from __future__ import annotations

from dataclasses import dataclass, field, replace
from functools import lru_cache
import json
from pathlib import Path
import re
from types import MappingProxyType
from typing import Any, ClassVar, Final, Literal, Mapping, NoReturn, NotRequired, TypedDict


#: The catalog artifact's schema identity. A payload that does not carry
#: exactly this string is refused: a future shape gets a new version and a
#: decoder that knows it, never a silent best-effort read.
CATALOG_SCHEMA_VERSION: Final = "required-role-catalog.v2"

#: The checked-in catalog artifact this package ships.
DEFAULT_CATALOG_PATH: Final = (
    Path(__file__).resolve().parent / "templates" / "required_role_catalog.json"
)

#: Typed failure codes.
REQUIRED_ROLE_CATALOG_INVALID: Final = "required_role_catalog_invalid"
REQUIRED_ROLE_UNKNOWN: Final = "required_role_unknown"
REQUIRED_ROLE_ITEM_UNRESOLVABLE: Final = "required_role_item_unresolvable"

#: Repair lanes named by the refusals.
CATALOG_REPAIR_LANE: Final = (
    "repair the required-role catalog artifact in the model-kind data lane"
)
CATALOG_AUTHOR_LANE: Final = (
    "author the role row in the required-role catalog artifact "
    "(model-kind data lane)"
)
ITEM_RESOLUTION_REPAIR_LANE: Final = (
    "pass the issuer symbol the model is being built for to the population "
    "lookup (case and surrounding space are normalized; an empty symbol is "
    "not one)"
)

#: The statement planes a role may sit on, and the segment each spells in a
#: role id. The role id restates its plane so a row cannot drift from the
#: plane it declares; the decoder cross-checks the two.
STATEMENT_PLANES: Final[Mapping[str, str]] = MappingProxyType(
    {
        "income_statement": "is",
        "balance_sheet": "bs",
        "cash_flow": "cf",
    }
)

_ROLE_RE: Final = re.compile(r"^statement\.(is|bs|cf)\.[a-z0-9][a-z0-9_]*$")
#: The balance-sheet total whose chain marks a balance an asset.
_ASSETS_TOTAL_ROLE: Final = "statement.bs.total_assets"
#: A balance's own role is role-shaped: dotted, lowercase, at least two
#: segments. The operating-balance plane reaches beyond the statement planes
#: (disclosed operating series are roles too), so its ids are not held to
#: ``_ROLE_RE``.
_DOTTED_ROLE_RE: Final = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z0-9][a-z0-9_]*)+$")
_ITEM_RE: Final = re.compile(r"^[a-z0-9][a-z0-9._]*$")
#: The one parameter a binding may carry. It names a *position* in an item
#: id, never an issuer: the fourth amendment's line is that a second issuer
#: is a value passed in, not a row authored or a branch taken.
TICKER_PLACEHOLDER: Final = "{ticker}"
#: One dotted piece of an item id, the unit every pattern below is built
#: from.
_ITEM_SEGMENT: Final = r"[a-z0-9][a-z0-9_]*"
#: The population plane binds one mid-compile semantic row whose id embeds
#: the issuer symbol, so that namespace — and only that one — admits the
#: placeholder, as a whole dotted segment and spelled exactly. A segment
#: like ``{issuer}``, ``{TICKER}`` or an unclosed ``{ticker`` refuses at
#: decode rather than reaching an item id unformatted. The fact plane has no
#: symbol to resolve with (its accessor takes no ticker), so it stays on
#: ``_ITEM_RE``: a templated fact binding is authored when the accessor that
#: could resolve it exists.
_POPULATION_SEGMENT: Final = rf"(?:\{{ticker\}}|{_ITEM_SEGMENT})"
_POPULATION_ITEM_RE: Final = re.compile(
    rf"^{_POPULATION_SEGMENT}(?:\.{_POPULATION_SEGMENT})*$"
)
#: What may be substituted for the placeholder. This is a lookup, so it may
#: not be stricter than the mint: the id it resolves to is produced by the
#: balance bridge's semantic row (`schema/build_semantic_rows_balance_bridge.py`),
#: which lowercases the *stripped* symbol into this same template and
#: validates no characters at all. The charset therefore covers every symbol
#: the canonical ticker contract can carry (`schema/ticker_contract.py`
#: ``EXTENDED_TICKER_RE`` — dotted class shares like ``BRK.B`` included),
#: with ``-`` admitted besides for a symbol that reaches the mint before
#: that normalization. What refuses is a symbol that would leave the id
#: *malformed* and silently miss every lookup — empty or blank, an empty
#: dotted segment, embedded whitespace.
_TICKER_SYMBOL: Final = rf"{_ITEM_SEGMENT}(?:[.-]{_ITEM_SEGMENT})*"
_TICKER_SYMBOL_RE: Final = re.compile(rf"^{_TICKER_SYMBOL}$")
_TEMPLATE_KIND_RE: Final = re.compile(r"^[a-z0-9][a-z0-9._-]*$")
#: An operating balance's ``semantic_role`` is a bare lowercase identifier.
_POSITION_RE: Final = re.compile(r"^[a-z][a-z0-9_]*$")

_CATALOG_KEYS: Final = frozenset(
    {
        "schema_version",
        "template_kind",
        "provenance",
        "required_roles",
    }
)
# Schedule relations do not mint required roles or add required root fields.
_CATALOG_OPTIONAL_KEYS: Final = frozenset(
    {
        "nsp_balance_runoff_rows",
        "disclosure_companions",
        "lease_reconciliation_shapes",
        "model_scale_relations",
        "statement_total_items",
        "operating_balances",
    }
)
_ROLE_REQUIRED_KEYS: Final = frozenset(
    {
        "required_role",
        "display_name",
        "statement",
        "template_items",
        "template_build_items",
        "population_items",
        "hff_fact_items",
    }
)
_ROLE_OPTIONAL_KEYS: Final = frozenset(
    {
        "optional_if_unreported",
        "presentation_optional",
        "covered_by_roles",
        "working_capital",
        "other_noncash",
        "change_of",
        "residual_of",
        "derived_from",
        "parent_total",
        "face_items",
        "counterpart",
        "alias_of",
        "polarity",
        "balance",
        "projection_owner",
        "pass_through",
    }
)
_OPERATING_BALANCE_KEYS: Final = frozenset(
    {
        "required_role",
        "display_name",
        "parent_total",
        "insert_after_item",
        "semantic_role",
    }
)
_OPERATING_BALANCE_OPTIONAL_KEYS: Final = frozenset(
    {"target_item", "counterpart", "pass_through", "legacy_roles"}
)
#: A balance-sheet role's operating working-capital side. Absent means the
#: balance is not an operating working-capital balance. The same two values
#: name the side of an ``other_noncash`` balance.
_WORKING_CAPITAL_SIDES: Final = frozenset({"asset", "liability"})
#: What a cash-flow role's ``derived_from`` may name: the balance attribute whose
#: leaves the role aggregates.
_DERIVED_FROM: Final = frozenset({"working_capital"})
#: Who already writes the projection of a balance a driver may not move, and
#: what that owner is: moving the balance with a node would leave the owner's
#: own statement identity or schedule (and the cash flow reading it) untied.
ProjectionOwner = Literal["cash_flow_link", "roll_forward", "schedule"]
PROJECTION_OWNERS: Final[Mapping[ProjectionOwner, str]] = MappingProxyType(
    {
        "cash_flow_link": "the cash-flow statement's ending cash, which is the balance sheet's cash",
        "roll_forward": "its statement roll-forward from net income and distributions",
        "schedule": "a template schedule whose own cash-flow rows carry its movement",
    }
)
#: The economic-positive direction of an income-statement role. The source
#: fact's XBRL balance determines whether its native sign matches this direction.
_INCOME_STATEMENT_POLARITIES: Final = frozenset(
    {"income_positive", "expense_positive"}
)
#: A statement face line lives on the Financial_model sheet's own statement
#: section: ``tpl.fm.<section>.<line>``. A role's ``parent_total`` and
#: ``face_items`` name lines of that plane only.
_FACE_LINE_PREFIX: Final = "tpl.fm."
#: The row's binding namespaces, as ``(field, what a refusal calls it)``.
#: One item is one role *inside* a namespace; across namespaces the same id
#: is two facts about two passes, never a collision.
_ITEM_NAMESPACES: Final[tuple[tuple[str, str], ...]] = (
    ("template_items", "template item"),
    ("template_build_items", "template build item"),
    ("population_items", "population item"),
    ("hff_fact_items", "HFF fact item"),
    ("face_items", "face item"),
)


class RequiredRoleCatalogError(ValueError):
    """Base typed catalog failure with a stable code and a repair lane."""

    code: ClassVar[str] = "required_role_catalog_error"

    def __init__(self, reason: str, *, repair: str) -> None:
        self.reason = reason
        self.repair = repair
        super().__init__(f"{self.code}: {reason} (repair: {repair})")


class RequiredRoleCatalogInvalid(RequiredRoleCatalogError):
    """The catalog artifact itself is malformed."""

    code: ClassVar[str] = REQUIRED_ROLE_CATALOG_INVALID

    def __init__(
        self,
        *,
        source: str,
        field: str,
        detail: str,
        repair: str = CATALOG_REPAIR_LANE,
    ) -> None:
        self.source = source
        self.field = field
        self.detail = detail
        super().__init__(f"{source}: {field}: {detail}", repair=repair)


class RequiredRoleUnknown(RequiredRoleCatalogError):
    """The catalog carries no such role, or no role for such a template item.

    Carries the starved identity and the authoring lane that repairs it. A
    lookup never falls back to a nearest match: an unbound structure
    position is a gap to author, not a value to guess.
    """

    code: ClassVar[str] = REQUIRED_ROLE_UNKNOWN

    def __init__(
        self,
        *,
        demanded: str,
        demanded_kind: str,
        source: str,
        repair: str = CATALOG_AUTHOR_LANE,
    ) -> None:
        self.demanded = demanded
        self.demanded_kind = demanded_kind
        self.source = source
        super().__init__(
            f"no required role is catalogued for {demanded_kind} {demanded!r} "
            f"in {source}",
            repair=repair,
        )


class RequiredRoleItemUnresolvable(RequiredRoleCatalogError):
    """A ``{ticker}``-templated binding cannot resolve to an item id.

    The placeholder itself is validated where the row is authored; this is
    the other half — the value substituted into it. A symbol that cannot
    spell the issuer's place in an item id would leave a malformed id to be
    looked up and silently miss, so it refuses here and names the caller
    that repairs it. It is deliberately not stricter than the producer of
    the id it resolves: a symbol that producer can mint with resolves.
    """

    code: ClassVar[str] = REQUIRED_ROLE_ITEM_UNRESOLVABLE

    def __init__(
        self,
        *,
        required_role: str,
        template: str,
        symbol: str,
        source: str,
        repair: str = ITEM_RESOLUTION_REPAIR_LANE,
    ) -> None:
        self.required_role = required_role
        self.template = template
        self.symbol = symbol
        self.source = source
        super().__init__(
            f"{template!r} of {required_role!r} in {source} cannot resolve "
            f"its issuer symbol {symbol!r}, which does not spell the issuer's "
            "place in a template item id",
            repair=repair,
        )


@dataclass(frozen=True, slots=True)
class RequiredRole:
    """One structure position of a template kind.

    ``display_name`` is presentation-only wording; identity and lookup always
    use the immutable ``required_role``.

    Five binding namespaces, each answering a different pass, none of them
    a restatement of another:

    ``template_items``
        the compiled template item ids the template's own stamps carry for
        this position (the stamped face plane).
    ``template_build_items``
        the ids the template build pass stamps by, same plane.
    ``population_items``
        the compiled ids statement/schedule population and the required-line
        identity fill bind for a role the stamps do not carry. The only
        namespace that may carry the ``{ticker}`` placeholder, resolved by
        :meth:`RequiredRoleCatalog.population_item_ids`.
    ``hff_fact_items``
        the exact compiled ids the accepted-HFF fact channel lands this
        role's disclosed filing-note balance on.
    ``face_items``
        the Financial_model statement lines that present this position
        without carrying its stamp: the line the template builder inserts
        for a role whose stamped item is a schedule row (a deferred-revenue
        balance is stamped on its Assumptions schedule and presented on a
        balance-sheet line). A face line is a ``template_items`` entry on
        the Financial_model plane or a ``face_items`` entry, so a role has
        one answer to "which statement line is this".

    Any of them is empty where that pass does not bind the role, and an
    empty stamped plane is a real structure row of the taxonomy plane that
    no item carries yet. A role holding different ids in two namespaces is
    the normal case, not a conflict: the planes are separate by design.

    ``parent_total`` is the statement total the role rolls into, including
    population-bound roles whose issuer-specific line exists only at build.
    It is the single owner of total membership. The template builder writes
    both lanes from the members with template face lines; the historical
    spread reads every member by role. A total is itself a role and rolls
    into its own ``parent_total`` until the chain closes at a total with none
    (``total_assets`` and ``total_liabilities_and_equity``).
    """

    required_role: str
    display_name: str
    statement: str
    template_items: tuple[str, ...]
    template_build_items: tuple[str, ...]
    population_items: tuple[str, ...]
    hff_fact_items: tuple[str, ...]
    optional_if_unreported: bool = False
    #: A role a filer may legitimately present inside another line (S&M and G&A inside a
    #: combined SG&A): a CITED absence leaves every additive total (rule (d)) but the role is
    #: never removed from the model and an UNCITED absence stays owed
    #: (FUNCTIONAL-EXPENSE-SPLIT-OPTIONAL). Distinct from ``optional_if_unreported``.
    presentation_optional: bool = False
    covered_by_roles: tuple[str, ...] = ()
    #: ``"asset"`` / ``"liability"`` for an operating working-capital
    #: balance; ``None`` for every other role.
    working_capital: str | None = None
    #: ``"asset"`` / ``"liability"`` for a non-working-capital balance whose
    #: period movement the other non-cash adjustments row carries (deferred
    #: taxes); ``None`` for every other role. A balance with a schedule and
    #: cash rows of its own (the operating-lease balances) is not a member:
    #: those rows are the balance's movement, each a line of operating cash flow.
    other_noncash: str | None = None
    #: The working-capital balance whose movement this cash-flow role
    #: carries as its own line; ``None`` for every other role.
    change_of: str | None = None
    #: The cash-flow statement total this role is the residual of: the line a
    #: filer need not print because its filed total, less the other members,
    #: closes to it (other non-cash, other investing, other financing).
    #: ``None`` for every other role.
    residual_of: str | None = None
    #: ``"working_capital"`` for the cash-flow role whose value the catalog derives
    #: from the working-capital balances no other cash-flow role carries
    #: (:func:`other_working_capital_leaves`); ``None`` for every other role.
    derived_from: str | None = None
    #: The statement total this role rolls into; ``None`` for a role that is
    #: no member of a statement total.
    parent_total: str | None = None
    face_items: tuple[str, ...] = ()
    #: The balance-sheet role that absorbs this cash-flow role's flow: the
    #: balance rolls forward by it, so a cash line with no schedule behind it
    #: still closes the balance sheet. The balance's own working-capital
    #: movement excludes it, so the cash moves once.
    counterpart: str | None = None
    #: The home role this role is another name for, when the catalog carries
    #: two names for one statement position (a legacy aggregate label and
    #: the face line that presents it). An alias row authors no items of its
    #: own: at decode it is materialized with its home's five binding
    #: planes and ``parent_total``, so a binding authored against the alias
    #: lands on the home's lines. Movement semantics (``working_capital``,
    #: ``other_noncash``, ``change_of``, ``counterpart``) stay the home's
    #: alone, so the position's movement is still carried once.
    alias_of: str | None = None
    #: Income/gains are positive for ``income_positive``; costs/charges are
    #: positive for ``expense_positive``. EPS follows income; share counts
    #: use the income-positive convention but a fact without an XBRL balance
    #: keeps its native sign. Other statement planes leave this unset.
    polarity: Literal["income_positive", "expense_positive"] | None = None
    #: A balance-sheet line a business-model driver may declare it *is*
    #: (``balance_of``): the node moves the reported balance. Totals, aliases
    #: and lines with no ``parent_total`` are never balances a driver moves.
    balance: bool = False
    #: A balance held for others whose movement is a pass-through, not the
    #: company's own investing or financing: its gross investment turnover
    #: leaves the cash flow while a net bridge exists. Only a balance carries it.
    pass_through: bool = False
    #: The writer of this balance's projection when that writer is not a
    #: business-model node: the balance is then known to the catalog but no
    #: driver may be it. Only a balance carries it, and a balance whose movement
    #: lands in a cash-flow aggregate (``working_capital`` / ``other_noncash`` /
    #: ``pass_through``) never does.
    projection_owner: ProjectionOwner | None = None

    @property
    def plane(self) -> str:
        """The role id's statement segment (``is`` / ``bs`` / ``cf``)."""

        return self.required_role.split(".")[1]


def other_working_capital_leaves(
    roles: tuple[RequiredRole, ...],
) -> tuple[RequiredRole, ...]:
    """The operating balance-sheet leaves no cash-flow role carries on its own line.

    Other working capital is the aggregate implied cash impact of these
    leaves: minus each asset's ``change``, plus each liability's. A balance
    with a ``working_capital`` side is an operating leaf; a cash-flow role's
    ``change_of`` takes that balance's movement onto its own line, so it
    leaves the set. Catalog order.
    """

    carried = frozenset(row.change_of for row in roles if row.change_of is not None)
    return tuple(
        row
        for row in roles
        if row.working_capital is not None and row.required_role not in carried
    )


class LeaseFormulaDerivation(TypedDict):
    owner: Literal["build_company_grounded_schedules"]
    strategy: Literal[
        "lease_liability_balance_reconciliation",
        "lease_rou_balance_reconciliation",
        "lease_current_portion_proxy",
    ]
    dependencies: list[str]


class LeaseReconciliationFormula(TypedDict):
    """Serialized FormulaSpec; one ratio leaf may be a balance_ratio parameter.

    Params retain the FormulaSpec contract's nested reference/expression shapes.
    The caller supplies the numeric ratio; this catalog never computes it.
    """

    type: Literal["ref", "arithmetic"]
    subtype: str
    params: dict[str, Any]
    note: str
    derivation: NotRequired[LeaseFormulaDerivation]


@dataclass(frozen=True, slots=True)
class LeaseReconciliationShape:
    target_item: str
    formula: LeaseReconciliationFormula


@dataclass(frozen=True, slots=True)
class ModelScaleRelation:
    source_item: str
    target_items: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class BalanceDeclarationRefusal:
    """Why a driver may not declare ``required_role`` as the balance it is.

    ``owner`` is the projection owner when ``reason`` is ``projection_owned``.
    """

    required_role: str
    reason: Literal[
        "not_a_balance", "projection_owned", "held_against_counterpart", "no_face_row"
    ]
    owner: ProjectionOwner | None = None
    counterpart_of: str | None = None

    @property
    def message(self) -> str:
        role = self.required_role
        if self.reason == "projection_owned" and self.owner is not None:
            return (
                f"{role!r} is a balance whose projection is owned by "
                f"{self.owner}: {PROJECTION_OWNERS[self.owner]}; a driver node "
                "cannot also move it"
            )
        if self.reason == "held_against_counterpart":
            return (
                f"{role!r} is held against {self.counterpart_of!r} and moves with "
                f"it; declare {self.counterpart_of!r} instead"
            )
        if self.reason == "no_face_row":
            return f"{role!r} has no existing balance-sheet row a driver could move"
        return f"{role!r} is not a balance the required-role catalog declares"


@dataclass(frozen=True, slots=True)
class CatalogBalance:
    """One balance a business-model driver may move, and how it bridges into the model.

    A balance is either a canonical ``balance_sheet`` role authored
    ``balance: true`` (its row and ``target_item`` are the role's own face
    line) or an ``operating_balances`` row: a balance the template owns no
    schedule for, so only a declaring node moves its reported history
    (``node_driven``). Both read alike to every consumer.

    ``parent_total`` is the statement total the balance rolls into and
    ``side`` the plane of the balance sheet it sits on, derived from the
    total chain. ``target_item`` is the existing face line the balance binds,
    or ``None`` when the bridge inserts its own row after ``insert_after_item``.
    ``counterpart`` is the balance this one is held against: it moves by the
    same ratio and its flow is the other side of this one's. ``pass_through``
    marks a balance whose gross investment turnover is not the company's own.
    ``semantic_role`` is the role name the bridge's semantic row carries;
    ``legacy_roles`` are the tokens stored revisions and bindings spelled the
    role by, read as this role. ``projection_owner`` names who already writes the
    balance's projection when that is not a node: such a balance is known to the
    catalog but is not one a driver may be.
    """

    required_role: str
    display_name: str
    parent_total: str
    side: Literal["asset", "liability"]
    target_item: str | None
    insert_after_item: str | None
    counterpart: str | None
    pass_through: bool
    semantic_role: str
    legacy_roles: tuple[str, ...]
    node_driven: bool
    projection_owner: ProjectionOwner | None

    @property
    def leaf(self) -> str:
        """The role id's final segment, the stem of the bridge's row ids."""

        return self.required_role.rsplit(".", 1)[1]

    @property
    def change_role(self) -> str:
        """The grounded-row role of this balance's period-change row."""

        return f"net_change_in_{self.leaf}"

    def semantic_item(self, ticker: str) -> str:
        """The semantic row id the bridge books this balance on for ``ticker``."""

        return f"fm.semantic.{str(ticker).strip().lower()}.{self.leaf}"

    def change_item(self, ticker: str) -> str:
        """The cash-flow row id carrying this balance's period change for ``ticker``."""

        return f"fm.semantic.{str(ticker).strip().lower()}.{self.change_role}"


@dataclass(frozen=True, slots=True)
class RequiredRoleCatalog:
    """The decoded catalog of one template kind."""

    source: Path
    template_kind: str
    provenance: str
    required_roles: tuple[RequiredRole, ...]
    #: Schedule balance -> ordered runoff rows, not statement-total membership.
    nsp_balance_runoff_rows: Mapping[str, tuple[str, ...]] = field(
        default_factory=lambda: MappingProxyType({})
    )
    #: Origin -> ordered (recipient, expected source) edges; order is significant.
    disclosure_companions: Mapping[str, tuple[tuple[str, str], ...]] = field(
        default_factory=lambda: MappingProxyType({})
    )
    lease_reconciliation_shapes: Mapping[str, LeaseReconciliationShape] = field(
        default_factory=lambda: MappingProxyType({})
    )
    model_scale_relations: tuple[ModelScaleRelation, ...] = ()
    #: Totals outside the balance-sheet parent_total membership plane.
    statement_total_items: Mapping[str, tuple[str, ...]] = field(
        default_factory=lambda: MappingProxyType({})
    )
    #: Every balance by role id, counterparts included.
    balances: Mapping[str, CatalogBalance] = field(
        default_factory=lambda: MappingProxyType({})
    )
    #: The balances a driver may declare: every balance with an existing face
    #: line that no other writer owns and that is not the counterpart another
    #: balance moves.
    declarable_balances: frozenset[str] = frozenset()
    #: Stored spelling -> the balance role it names.
    legacy_balance_roles: Mapping[str, str] = field(
        default_factory=lambda: MappingProxyType({})
    )

    def balance(self, required_role: str) -> CatalogBalance:
        """The balance one role is; typed refusal when the catalog declares none."""

        found = self.balances.get(required_role)
        if found is None:
            raise RequiredRoleUnknown(
                demanded=required_role,
                demanded_kind="balance role",
                source=self.source.name,
            )
        return found

    def declaration_refusal(self, required_role: str) -> BalanceDeclarationRefusal | None:
        """Why a driver may not be ``required_role``; ``None`` when it may."""

        if required_role in self.declarable_balances:
            return None
        balance = self.balances.get(required_role)
        if balance is None:
            return BalanceDeclarationRefusal(required_role, "not_a_balance")
        if balance.projection_owner is not None:
            return BalanceDeclarationRefusal(
                required_role, "projection_owned", owner=balance.projection_owner
            )
        holder = next(
            (other for other in self.balances.values() if other.counterpart == required_role),
            None,
        )
        if holder is not None:
            return BalanceDeclarationRefusal(
                required_role,
                "held_against_counterpart",
                counterpart_of=holder.required_role,
            )
        return BalanceDeclarationRefusal(required_role, "no_face_row")

    def pass_through_counterparts(self) -> tuple[CatalogBalance, ...]:
        """The obligations the pass-through balances are held against, once each."""

        return tuple(
            dict.fromkeys(
                self.balance(balance.counterpart)
                for balance in self.balances.values()
                if balance.pass_through and balance.counterpart is not None
            )
        )

    def role_ids(self) -> tuple[str, ...]:
        return tuple(row.required_role for row in self.required_roles)

    def role(self, required_role: str) -> RequiredRole:
        """The row for one role; typed refusal when the catalog has none."""

        row = self._row_or_none(required_role)
        if row is None:
            raise RequiredRoleUnknown(
                demanded=required_role,
                demanded_kind="role",
                source=self.source.name,
            )
        return row

    def _row_or_none(self, required_role: str) -> RequiredRole | None:
        """The row for one role, or ``None`` — the two item lookups' contract.

        They answer ``()`` for an uncatalogued role rather than refusing, so
        they need the absence as a value; :meth:`role` turns it into the
        typed refusal.
        """

        for row in self.required_roles:
            if row.required_role == required_role:
                return row
        return None

    def role_for_template_item(self, template_item: str) -> RequiredRole:
        """The role a compiled template item occupies; typed refusal if none.

        Answers for the ``template_items`` namespace alone — the stamped
        face plane — because that is the namespace the stamp pass keys on.
        The same id may occupy a different role's ``population_items`` (the
        face-statement row a disclosed liability is nested inside is exactly
        that case) and that is not an answer to this question.

        The decoder refuses a catalog in which one compiled item is claimed
        by two *canonical* roles inside this namespace; an alias row shares
        its home's items by declaration, and the home — never the alias —
        is the answer, so the answer is unique when it exists.
        """

        for row in self.required_roles:
            if row.alias_of is None and template_item in row.template_items:
                return row
        for row in self.required_roles:
            if template_item in row.template_items:
                return row
        raise RequiredRoleUnknown(
            demanded=template_item,
            demanded_kind="template item",
            source=self.source.name,
        )

    def population_item_ids(
        self, required_role: str, *, ticker: str
    ) -> tuple[str, ...]:
        """Compiled items population binds for a role, ``{ticker}`` resolved.

        ``()`` for a role the catalog does not carry and for a catalogued
        role whose population plane is empty. Both mean one thing to the
        caller — this role binds through the template stamps instead, or is
        not a line of this model kind at all (a formula-computed identity,
        say) — so the empty answer is the contract the population walks
        read, not a swallowed lookup failure.
        """

        row = self._row_or_none(required_role)
        if row is None or not row.population_items:
            return ()
        # Normalized exactly as the id's producer normalizes it, so the
        # lookup and the mint cannot disagree about the same issuer.
        symbol = str(ticker).strip().lower()
        return tuple(
            self._resolve_population_item(item, symbol, row.required_role)
            for item in row.population_items
        )

    def _resolve_population_item(
        self, item: str, symbol: str, required_role: str
    ) -> str:
        if TICKER_PLACEHOLDER not in item:
            return item
        if _TICKER_SYMBOL_RE.fullmatch(symbol) is None:
            raise RequiredRoleItemUnresolvable(
                required_role=required_role,
                template=item,
                symbol=symbol,
                source=self.source.name,
            )
        return item.replace(TICKER_PLACEHOLDER, symbol)

    def hff_fact_item_ids(self, required_role: str) -> tuple[str, ...]:
        """Exact items the accepted-HFF fact channel binds for a role.

        ``()`` for roles outside the fact plane — those roles' supported
        facts are owned by statement/schedule population from the pinned
        bundle, never by this channel. Same empty-answer contract as
        :meth:`population_item_ids`.
        """

        row = self._row_or_none(required_role)
        return () if row is None else row.hff_fact_items

    def roles_on_statement(self, statement: str) -> tuple[RequiredRole, ...]:
        return tuple(
            row for row in self.required_roles if row.statement == statement
        )

    def other_working_capital_leaves(self) -> tuple[RequiredRole, ...]:
        return other_working_capital_leaves(self.required_roles)

    def statement_totals(
        self, *, statements: tuple[str, ...] = ("balance_sheet",)
    ) -> tuple[str, ...]:
        """Template totals on the requested planes, in catalog order.

        Balance-sheet totals derive from role membership. Other statement
        planes declare their totals separately because their signed formulas
        are not balance-sheet membership chains. The default retains the
        balance-sheet-only contract used by template generation.
        """

        totals: dict[str, None] = {}
        for statement in statements:
            for row in self.required_roles:
                if row.statement == statement and row.parent_total is not None:
                    totals.setdefault(row.parent_total)
            for item_id in self.statement_total_items.get(statement, ()):
                totals.setdefault(item_id)
        return tuple(totals)

    def roles_rolling_into(self, total_item: str) -> tuple[RequiredRole, ...]:
        """All roles rolling into ``total_item``, including population-only members."""

        return tuple(row for row in self.required_roles if row.parent_total == total_item)

    def roles_covered_by(self, covering_role: str) -> tuple[RequiredRole, ...]:
        """The roles whose value ``covering_role``'s line presents (their ``covered_by_roles``)."""

        return tuple(row for row in self.required_roles if covering_role in row.covered_by_roles)

    def total_members(self, total_item: str) -> tuple[str, ...]:
        """The template face lines ``total_item`` sums, subtotals included.

        Population-only members have no template face line and are omitted.
        Consumers computing from filed roles use ``roles_rolling_into``.
        An alias member presents its home's face line, so the lines are
        deduplicated: a total sums a face line once however many role
        names present it.
        """

        return tuple(
            dict.fromkeys(
                line
                for row in self.roles_rolling_into(total_item)
                for line in face_lines(row)
            )
        )

    def role_for_face_line(self, line: str) -> RequiredRole:
        """The role a Financial_model statement line presents; typed refusal if none."""

        row = self.presenting_role(line)
        if row is None:
            raise RequiredRoleUnknown(
                demanded=line,
                demanded_kind="face line",
                source=self.source.name,
            )
        return row

    def presenting_role(self, line: str) -> RequiredRole | None:
        """The role a statement line presents, or ``None`` for a pure sum.

        The canonical (non-alias) role presenting the line, when one does:
        an alias presents its home's lines, and the home is the presenter.
        """

        return next(
            (
                row
                for row in self.required_roles
                if row.alias_of is None and line in face_lines(row)
            ),
            next(
                (row for row in self.required_roles if line in face_lines(row)),
                None,
            ),
        )


def face_lines(role: RequiredRole) -> tuple[str, ...]:
    """The Financial_model statement lines presenting ``role``.

    The role's stamped items on that sheet, then its ``face_items``. A role
    stamped only on a schedule row has no other answer, which is why
    ``face_items`` exists.
    """

    return tuple(
        item
        for item in (*role.template_items, *role.face_items)
        if item.startswith(_FACE_LINE_PREFIX)
    )


def _invalid(source: str, field: str, detail: str) -> NoReturn:
    raise RequiredRoleCatalogInvalid(source=source, field=field, detail=detail)


def _require_string(
    value: str,
    *,
    source: str,
    field: str,
    pattern: re.Pattern[str],
) -> str:
    if pattern.fullmatch(value) is None:
        _invalid(source, field, f"{value!r} does not match {pattern.pattern}")
    return value


def _require_id_list(
    value: list[str],
    *,
    source: str,
    field: str,
    pattern: re.Pattern[str],
) -> tuple[str, ...]:
    ids = tuple(
        _require_string(
            item, source=source, field=f"{field}[{index}]", pattern=pattern
        )
        for index, item in enumerate(value)
    )
    if len(set(ids)) != len(ids):
        _invalid(source, field, f"duplicate entries in {list(ids)}")
    return ids


def _decode_role(row: Mapping[str, Any], *, source: str, index: int) -> RequiredRole:
    field = f"required_roles[{index}]"
    unknown = sorted(set(row) - _ROLE_REQUIRED_KEYS - _ROLE_OPTIONAL_KEYS)
    if unknown:
        _invalid(source, field, f"unknown keys {unknown}")
    missing = sorted(_ROLE_REQUIRED_KEYS - set(row))
    if missing:
        _invalid(source, field, f"missing keys {missing}")
    required_role = _require_string(
        row["required_role"],
        source=source,
        field=f"{field}.required_role",
        pattern=_ROLE_RE,
    )
    statement = row["statement"]
    if statement not in STATEMENT_PLANES:
        _invalid(
            source,
            f"{field}.statement",
            f"{statement!r} is not one of {sorted(STATEMENT_PLANES)}",
        )
    plane = required_role.split(".")[1]
    if plane != STATEMENT_PLANES[statement]:
        _invalid(
            source,
            f"{field}.required_role",
            f"{required_role!r} declares plane {plane!r} but statement "
            f"{statement!r} is {STATEMENT_PLANES[statement]!r}",
        )
    optional = row.get("optional_if_unreported", False)
    presentation_optional = row.get("presentation_optional", False)
    if optional and presentation_optional:
        # Economic optionality (the row may leave the model) and presentation
        # optionality (the row stays, owed when uncited) are mutually exclusive
        # catalog semantics — docs/reference/rulings-index.md rows
        # DISCLOSURE-ADAPTS-ACCOUNTING-FIXED and FUNCTIONAL-EXPENSE-SPLIT-OPTIONAL;
        # a data-integrity refusal at the catalog owner (C1 decision 2026-10-18,
        # docs/closed/bcgs-consumes-role-disposition-single-pass.md).
        _invalid(
            source,
            f"{field}.presentation_optional",
            f"{required_role!r} declares both optional_if_unreported and presentation_optional",
        )
    covered_by = ()
    if "covered_by_roles" in row:
        covered_by = _require_id_list(
            row["covered_by_roles"],
            source=source,
            field=f"{field}.covered_by_roles",
            pattern=_ROLE_RE,
        )
        if required_role in covered_by:
            _invalid(
                source,
                f"{field}.covered_by_roles",
                f"{required_role!r} cannot cover itself",
            )
    polarity = row.get("polarity")
    if polarity is not None and (
        statement != "income_statement"
        or polarity not in _INCOME_STATEMENT_POLARITIES
    ):
        _invalid(
            source,
            f"{field}.polarity",
            f"{polarity!r} is not one of {sorted(_INCOME_STATEMENT_POLARITIES)} "
            "on an income_statement role",
        )
    working_capital = row.get("working_capital")
    if working_capital is not None and (
        statement != "balance_sheet" or working_capital not in _WORKING_CAPITAL_SIDES
    ):
        _invalid(
            source,
            f"{field}.working_capital",
            f"{working_capital!r} is not one of {sorted(_WORKING_CAPITAL_SIDES)} "
            "on a balance_sheet role",
        )
    other_noncash = row.get("other_noncash")
    if other_noncash is not None and (
        statement != "balance_sheet"
        or other_noncash not in _WORKING_CAPITAL_SIDES
        or working_capital is not None
    ):
        _invalid(
            source,
            f"{field}.other_noncash",
            f"{other_noncash!r} is not one of {sorted(_WORKING_CAPITAL_SIDES)} "
            "on a balance_sheet role that is not a working-capital balance",
        )
    change_of = None
    if "change_of" in row:
        change_of = _require_string(
            row["change_of"],
            source=source,
            field=f"{field}.change_of",
            pattern=_ROLE_RE,
        )
        if statement != "cash_flow":
            _invalid(source, f"{field}.change_of", "only a cash_flow role carries a change")
    residual_of = None
    if "residual_of" in row:
        residual_of = _require_string(
            row["residual_of"],
            source=source,
            field=f"{field}.residual_of",
            pattern=_ROLE_RE,
        )
        if statement != "cash_flow":
            _invalid(
                source, f"{field}.residual_of", "only a cash_flow role is a residual"
            )
    derived_from = None
    if "derived_from" in row:
        derived_from = row["derived_from"]
        if statement != "cash_flow" or derived_from not in _DERIVED_FROM:
            _invalid(
                source,
                f"{field}.derived_from",
                f"{derived_from!r} is not one of {sorted(_DERIVED_FROM)} on a cash_flow role",
            )
    counterpart = None
    if "counterpart" in row:
        counterpart = _require_string(
            row["counterpart"],
            source=source,
            field=f"{field}.counterpart",
            pattern=_ROLE_RE,
        )
        if statement != "cash_flow":
            _invalid(
                source, f"{field}.counterpart", "only a cash_flow role has a counterpart"
            )
    parent_total = None
    if "parent_total" in row:
        parent_total = _require_string(
            row["parent_total"],
            source=source,
            field=f"{field}.parent_total",
            pattern=_ITEM_RE,
        )
        if statement != "balance_sheet" or not parent_total.startswith(_FACE_LINE_PREFIX):
            _invalid(
                source,
                f"{field}.parent_total",
                f"{parent_total!r} must be a Financial_model total on a balance_sheet role",
            )
    face_items = _require_id_list(
        row.get("face_items", []),
        source=source,
        field=f"{field}.face_items",
        pattern=_ITEM_RE,
    )
    if face_items and parent_total is None:
        _invalid(source, f"{field}.face_items", "a face line needs a parent_total")
    for item in face_items:
        if not item.startswith(_FACE_LINE_PREFIX):
            _invalid(
                source,
                f"{field}.face_items",
                f"{item!r} is not a Financial_model statement line",
            )
    alias_of = None
    if "alias_of" in row:
        alias_of = _require_string(
            row["alias_of"],
            source=source,
            field=f"{field}.alias_of",
            pattern=_ROLE_RE,
        )
        if alias_of == required_role:
            _invalid(
                source, f"{field}.alias_of", f"{required_role!r} cannot alias itself"
            )
    balance = row.get("balance", False)
    pass_through = row.get("pass_through", False)
    if balance and (
        statement != "balance_sheet" or parent_total is None or alias_of is not None
    ):
        _invalid(
            source,
            f"{field}.balance",
            f"{required_role!r} is a balance a driver moves only as a canonical "
            "balance_sheet role that rolls into a total",
        )
    if pass_through and not balance:
        _invalid(
            source,
            f"{field}.pass_through",
            f"{required_role!r} is a pass-through only as a balance",
        )
    projection_owner = row.get("projection_owner")
    if projection_owner is not None and (
        not balance or projection_owner not in PROJECTION_OWNERS
    ):
        _invalid(
            source,
            f"{field}.projection_owner",
            f"{projection_owner!r} is not one of {sorted(PROJECTION_OWNERS)} "
            "on a balance",
        )
    decoded = RequiredRole(
        required_role=required_role,
        display_name=row["display_name"],
        statement=statement,
        template_items=_require_id_list(
            row["template_items"],
            source=source,
            field=f"{field}.template_items",
            pattern=_ITEM_RE,
        ),
        template_build_items=_require_id_list(
            row["template_build_items"],
            source=source,
            field=f"{field}.template_build_items",
            pattern=_ITEM_RE,
        ),
        population_items=_require_id_list(
            row["population_items"],
            source=source,
            field=f"{field}.population_items",
            pattern=_POPULATION_ITEM_RE,
        ),
        hff_fact_items=_require_id_list(
            row["hff_fact_items"],
            source=source,
            field=f"{field}.hff_fact_items",
            pattern=_ITEM_RE,
        ),
        optional_if_unreported=optional,
        presentation_optional=presentation_optional,
        covered_by_roles=covered_by,
        working_capital=working_capital,
        other_noncash=other_noncash,
        change_of=change_of,
        residual_of=residual_of,
        derived_from=derived_from,
        parent_total=parent_total,
        face_items=face_items,
        counterpart=counterpart,
        alias_of=alias_of,
        polarity=polarity,
        balance=balance,
        pass_through=pass_through,
        projection_owner=projection_owner,
    )
    if balance:
        if not face_lines(decoded):
            _invalid(
                source,
                f"{field}.balance",
                f"{required_role!r} is a balance only with a Financial_model face "
                "line a driver's movement lands on",
            )
        # A driver moves a balance whose change lands in a cash-flow row; every
        # other balance is written by an owner the row names, never by both.
        carried = (
            working_capital is not None or other_noncash is not None or pass_through
        )
        if carried and projection_owner is not None:
            _invalid(
                source,
                f"{field}.projection_owner",
                f"{required_role!r} moves in a cash-flow row, so no other owner "
                "writes its projection",
            )
        if not carried and projection_owner is None:
            _invalid(
                source,
                f"{field}.projection_owner",
                f"{required_role!r} moves in no cash-flow row and names no "
                "projection owner: a driver moving it would leave the balance "
                "sheet untied",
            )
    if alias_of is not None:
        # An alias authors nothing of its own: its binding planes (except
        # ``hff_fact_items``) and movement semantics are its home's,
        # materialized in ``_materialize_aliases``. A row carrying both would
        # name two different homes for one position.
        authored = (
            decoded.template_items
            or decoded.template_build_items
            or decoded.population_items
            or decoded.hff_fact_items
            or decoded.face_items
            or decoded.covered_by_roles
            or decoded.parent_total is not None
            or decoded.working_capital is not None
            or decoded.other_noncash is not None
            or decoded.change_of is not None
            or decoded.residual_of is not None
            or decoded.derived_from is not None
            or decoded.counterpart is not None
            or decoded.pass_through
        )
        if authored:
            _invalid(
                source,
                f"{field}.alias_of",
                f"{required_role!r} aliases {alias_of!r} but authors its own "
                "items or semantics; an alias carries its home's alone",
            )
    return decoded


def _population_resolutions_re(template: str) -> re.Pattern[str]:
    """Every compiled id one ``{ticker}`` binding can resolve to.

    The placeholder occupies whole segments, so the ids it reaches are the
    template with each placeholder replaced by any admissible issuer symbol
    — the same symbol class :func:`_resolve_population_item` substitutes.
    """

    body = _TICKER_SYMBOL.join(
        re.escape(part) for part in template.split(TICKER_PLACEHOLDER)
    )
    return re.compile(f"^{body}$")


def _refuse_population_resolution_collisions(
    roles: tuple[RequiredRole, ...], *, source: str
) -> None:
    """Refuse a literal population binding some issuer's template mints."""

    templates = [
        (row.required_role, item)
        for row in roles
        for item in row.population_items
        if TICKER_PLACEHOLDER in item
    ]
    if not templates:
        return
    literals = [
        (row.required_role, item)
        for row in roles
        for item in row.population_items
        if TICKER_PLACEHOLDER not in item
    ]
    for owner, template in templates:
        resolutions = _population_resolutions_re(template)
        for other, literal in literals:
            if resolutions.fullmatch(literal) is not None:
                _invalid(
                    source,
                    "required_roles",
                    f"population item {literal!r} of {other!r} is an issuer "
                    f"resolution of {template!r} of {owner!r}; on this plane "
                    "one compiled item is one role for every issuer",
                )


def _materialize_aliases(
    roles: tuple[RequiredRole, ...], *, source: str
) -> tuple[RequiredRole, ...]:
    """Give each alias row its home's binding planes and ``parent_total``.

    This is the alias resolution, and it happens here, at catalog load, so
    every consumer reading a row's items reads the home's lines for a
    binding authored against the alias. The home must be a catalogued,
    non-alias role on the same statement plane: an alias of an alias would
    make the position's one home a matter of chain length. Only
    ``template_items``, ``template_build_items``, ``population_items``,
    ``face_items`` and total membership (``parent_total``) are copied.
    ``hff_fact_items`` and the movement semantics stay at the home: when both
    roles of a pair are bound, copying the HFF plane would project one filed
    fact twice, and the position moves once.
    """

    by_id = {row.required_role: row for row in roles}
    materialized: list[RequiredRole] = []
    for row in roles:
        if row.alias_of is None:
            materialized.append(row)
            continue
        home = by_id.get(row.alias_of)
        if home is None:
            _invalid(
                source,
                "required_roles",
                f"{row.required_role!r} aliases {row.alias_of!r}, "
                "which the catalog does not carry",
            )
        if home.alias_of is not None:
            _invalid(
                source,
                "required_roles",
                f"{row.required_role!r} aliases {row.alias_of!r}, which is "
                "itself an alias; an alias names its home directly",
            )
        if home.statement != row.statement:
            _invalid(
                source,
                "required_roles",
                f"{row.required_role!r} aliases {row.alias_of!r} on a "
                "different statement plane",
            )
        materialized.append(
            replace(
                row,
                template_items=home.template_items,
                template_build_items=home.template_build_items,
                population_items=home.population_items,
                face_items=home.face_items,
                parent_total=home.parent_total,
            )
        )
    return tuple(materialized)


def _role_has_home(
    row: RequiredRole,
    *,
    by_id: Mapping[str, RequiredRole],
) -> bool:
    """Whether a value bound to ``row`` lands somewhere a consumer reads.

    The homes, all derived from the catalog's own fields: an item on any
    binding plane, or coverage by a role that itself binds items (the
    covering line presents this position inside it). An alias is homed by
    materialization before this runs.
    """

    if any(getattr(row, attribute) for attribute, _namespace in _ITEM_NAMESPACES):
        return True
    return any(
        any(
            getattr(by_id[covering], attribute)
            for attribute, _namespace in _ITEM_NAMESPACES
        )
        for covering in row.covered_by_roles
        if covering in by_id
    )


def _validate_role_homes(roles: tuple[RequiredRole, ...], *, source: str) -> None:
    """Every role has a home a bound filed line lands on.

    A role with no home can claim a filed line in a binding and put its
    value nowhere: no template line stamps it, no population or fact
    channel lands it, and no covering line presents it — the value
    silently vanishes from every total. That is a catalog malformation,
    refused here at load, never a silent drop at build.
    """

    by_id = {row.required_role: row for row in roles}
    for row in roles:
        if not _role_has_home(row, by_id=by_id):
            _invalid(
                source,
                "required_roles",
                f"{row.required_role!r} has no home: no template, build, "
                "population, HFF fact or face item, and no coverage by a role "
                "that binds items; a filed line bound to it would land nowhere",
            )


def _validate_statement_totals(
    roles: tuple[RequiredRole, ...], *, source: str
) -> None:
    """Every total membership chain closes on the balance-sheet plane.

    A member has either a template face line or a population binding whose
    issuer-specific line is materialized at build. Only template face lines
    participate in template generation; role traversal includes both.
    """

    role_by_line: dict[str, RequiredRole] = {}
    for row in roles:
        for line in face_lines(row):
            role_by_line.setdefault(line, row)
    for row in roles:
        if row.parent_total is None:
            continue
        if not face_lines(row) and not row.population_items:
            _invalid(
                source,
                "required_roles",
                f"{row.required_role!r} rolls into {row.parent_total!r} "
                "but has neither a Financial_model face line nor a population binding",
            )
        for line in face_lines(row):
            if not line.startswith("tpl.fm.balance_sheet."):
                _invalid(
                    source,
                    "required_roles",
                    f"{row.required_role!r} face line {line!r} is not a "
                    "balance-sheet line",
                )
        seen = {row.required_role}
        parent = row.parent_total
        while parent is not None:
            owner = role_by_line.get(parent)
            if owner is None:
                # A terminal total no filing line is pinned to (total liabilities
                # and equity): every role is a position the corpus packaging
                # must cover, so a pure sum carries none.
                break
            if owner.required_role in seen:
                _invalid(
                    source,
                    "required_roles",
                    f"{row.required_role!r} rolls into itself through "
                    f"{owner.required_role!r}",
                )
            seen.add(owner.required_role)
            parent = owner.parent_total


def _decode_balances(
    roles: tuple[RequiredRole, ...],
    rows: list[Mapping[str, Any]],
    *,
    source: str,
) -> tuple[CatalogBalance, ...]:
    """Every balance the catalog declares, from role rows and ``operating_balances``.

    The side of a balance is the plane of the balance sheet its ``parent_total``
    chain closes on; nothing authors it twice.
    """

    role_by_line: dict[str, RequiredRole] = {}
    for row in roles:
        for line in face_lines(row):
            role_by_line.setdefault(line, row)
    asset_roots = {
        line
        for row in roles
        if row.required_role == _ASSETS_TOTAL_ROLE
        for line in face_lines(row)
    }
    totals = {row.parent_total for row in roles if row.parent_total is not None}
    role_ids = {row.required_role for row in roles}

    def side_of(parent_total: str) -> Literal["asset", "liability"]:
        total, seen = parent_total, set()
        while total not in seen:
            seen.add(total)
            owner = role_by_line.get(total)
            if owner is None or owner.parent_total is None:
                break
            total = owner.parent_total
        return "asset" if total in asset_roots else "liability"

    balances: list[CatalogBalance] = []
    for row in roles:
        if not row.balance:
            continue
        assert row.parent_total is not None
        target = next(iter(face_lines(row)), None)
        if totals.intersection(face_lines(row)):
            _invalid(
                source,
                f"required_roles[{row.required_role}].balance",
                f"{row.required_role!r} is a statement total other rows roll into, "
                "never a balance a driver moves",
            )
        balances.append(
            CatalogBalance(
                required_role=row.required_role,
                display_name=row.display_name,
                parent_total=row.parent_total,
                side=side_of(row.parent_total),
                target_item=target,
                insert_after_item=target,
                counterpart=None,
                pass_through=row.pass_through,
                semantic_role=row.required_role.rsplit(".", 1)[1],
                legacy_roles=(),
                node_driven=False,
                projection_owner=row.projection_owner,
            )
        )
    for index, row in enumerate(rows):
        field_name = f"operating_balances[{index}]"
        unknown = sorted(
            set(row) - _OPERATING_BALANCE_KEYS - _OPERATING_BALANCE_OPTIONAL_KEYS
        )
        if unknown:
            _invalid(source, field_name, f"unknown keys {unknown}")
        missing = sorted(_OPERATING_BALANCE_KEYS - set(row))
        if missing:
            _invalid(source, field_name, f"missing keys {missing}")
        role = _require_string(
            row["required_role"],
            source=source,
            field=f"{field_name}.required_role",
            pattern=_DOTTED_ROLE_RE,
        )
        if _ROLE_RE.fullmatch(role) is not None or role in role_ids:
            _invalid(
                source,
                f"{field_name}.required_role",
                f"{role!r} is a statement role; an operating balance is a role "
                "no statement plane carries",
            )
        parent_total = _require_string(
            row["parent_total"],
            source=source,
            field=f"{field_name}.parent_total",
            pattern=_ITEM_RE,
        )
        if parent_total not in totals:
            _invalid(
                source,
                f"{field_name}.parent_total",
                f"{parent_total!r} is not a total any catalog role rolls into",
            )
        anchors = []
        for key in ("insert_after_item", "target_item"):
            if key not in row:
                anchors.append(None)
                continue
            anchors.append(
                _require_string(
                    row[key],
                    source=source,
                    field=f"{field_name}.{key}",
                    pattern=_ITEM_RE,
                )
            )
            if not anchors[-1].startswith("tpl.fm.balance_sheet."):
                _invalid(
                    source,
                    f"{field_name}.{key}",
                    f"{anchors[-1]!r} is not a balance-sheet line",
                )
        insert_after, target = anchors
        legacy = _require_id_list(
            row.get("legacy_roles", []),
            source=source,
            field=f"{field_name}.legacy_roles",
            pattern=_ROLE_RE,
        )
        balances.append(
            CatalogBalance(
                required_role=role,
                display_name=row["display_name"],
                parent_total=parent_total,
                side=side_of(parent_total),
                target_item=target,
                insert_after_item=insert_after,
                counterpart=row.get("counterpart"),
                pass_through=bool(row.get("pass_through", False)),
                semantic_role=_require_string(
                    row["semantic_role"],
                    source=source,
                    field=f"{field_name}.semantic_role",
                    pattern=_POSITION_RE,
                ),
                legacy_roles=legacy,
                node_driven=True,
                projection_owner=None,
            )
        )
    by_role: dict[str, CatalogBalance] = {}
    legacy_owner: dict[str, str] = {}
    for balance in balances:
        if balance.required_role in by_role:
            _invalid(
                source,
                "operating_balances",
                f"duplicate balance {balance.required_role!r}",
            )
        by_role[balance.required_role] = balance
        for token in balance.legacy_roles:
            if token in role_ids or token in legacy_owner:
                _invalid(
                    source,
                    "operating_balances",
                    f"legacy role {token!r} of {balance.required_role!r} is "
                    "already a catalog role or another balance's legacy spelling",
                )
            legacy_owner[token] = balance.required_role
    for balance in balances:
        counterpart = balance.counterpart
        if counterpart is None:
            continue
        other = by_role.get(counterpart)
        if other is None or counterpart == balance.required_role:
            _invalid(
                source,
                "operating_balances",
                f"{balance.required_role!r} is held against {counterpart!r}, "
                "which the catalog does not carry as another balance",
            )
        if other.side == balance.side:
            _invalid(
                source,
                "operating_balances",
                f"{balance.required_role!r} and its counterpart {counterpart!r} "
                "sit on the same side of the balance sheet",
            )
    return tuple(balances)


def decode_required_role_catalog(
    table: Mapping[str, Any], *, source: Path
) -> RequiredRoleCatalog:
    """Decode one checked-in catalog payload.

    The payload is this repository's own artifact, so JSON value types and
    emptiness are not checked here. Each catalog-semantics violation is a
    typed refusal naming the first offending field:
    an unknown or missing key, a value outside its vocabulary, a role id
    disagreeing with the statement plane it declares, a duplicate role, one
    item claimed by two canonical roles inside one of the five id
    namespaces, a ``{ticker}`` placeholder outside the population plane or
    misspelled inside it, a literal population binding that some issuer's
    ``{ticker}`` binding would mint, a coverage or alias reference to a
    role the catalog does not carry (or to another alias), or a role with no
    home a bound filed line could land on.

    The catalog is closed under its own coverage references, which is what
    makes ``covered_by_roles`` a decision the required-set derivation can
    make without a second lookup.
    """

    name = source.name
    unknown = sorted(set(table) - _CATALOG_KEYS - _CATALOG_OPTIONAL_KEYS)
    if unknown:
        _invalid(name, "<root>", f"unknown keys {unknown}")
    missing = sorted(_CATALOG_KEYS - set(table))
    if missing:
        _invalid(name, "<root>", f"missing keys {missing}")
    schema_version = table["schema_version"]
    if schema_version != CATALOG_SCHEMA_VERSION:
        _invalid(
            name,
            "schema_version",
            f"{schema_version!r} is not {CATALOG_SCHEMA_VERSION!r}",
        )
    template_kind = _require_string(
        table["template_kind"],
        source=name,
        field="template_kind",
        pattern=_TEMPLATE_KIND_RE,
    )
    provenance = table["provenance"]
    rows = table["required_roles"]
    roles = tuple(
        _decode_role(row, source=name, index=index) for index, row in enumerate(rows)
    )
    role_ids: set[str] = set()
    for row in roles:
        if row.required_role in role_ids:
            _invalid(name, "required_roles", f"duplicate role {row.required_role!r}")
        role_ids.add(row.required_role)
    # Five id namespaces, each checked on its own, over the rows as
    # authored (an alias authors no items; it shares its home's by
    # declaration when materialized below). They are different names
    # for different passes, and one role legitimately binds different items
    # on different planes — the face-statement row a disclosed liability is
    # nested inside is claimed by the row that presents it on the stamped
    # plane and by the lease role on the population plane, and neither
    # claim is a mistake. Pooling them would read that as a double claim and
    # delete a deliberate distinction. Within each namespace, one item is
    # one canonical role.
    claimed_by_namespace: dict[str, dict[str, str]] = {
        namespace: {} for _attribute, namespace in _ITEM_NAMESPACES
    }
    for row in roles:
        for attribute, namespace in _ITEM_NAMESPACES:
            claimed = claimed_by_namespace[namespace]
            for item in getattr(row, attribute):
                owner = claimed.get(item)
                if owner is not None:
                    _invalid(
                        name,
                        "required_roles",
                        f"{namespace} {item!r} is claimed by {owner!r} and "
                        f"{row.required_role!r}",
                    )
                claimed[item] = row.required_role
    # The population namespace is the only one that resolves at lookup, so
    # its uniqueness is a fact about resolved ids and not about the strings
    # the rows spell. A ``{ticker}`` binding and a literal one are different
    # strings that name the same compiled item for some issuer, and the
    # string-level check above cannot see it: the collision would surface as
    # a second silent claim on one row at build time, for one issuer,
    # instead of as a refusal here. The fourth amendment's grep guard does
    # not cover this — it refuses issuer *names*, and a literal need not
    # spell one to collide.
    _refuse_population_resolution_collisions(roles, source=name)
    for row in roles:
        for covering in row.covered_by_roles:
            if covering not in role_ids:
                _invalid(
                    name,
                    "required_roles",
                    f"{row.required_role!r} is covered by {covering!r}, "
                    "which the catalog does not carry",
                )
    working_capital_ids = {row.required_role for row in roles if row.working_capital}
    balance_sheet_ids = {row.required_role for row in roles if row.statement == "balance_sheet"}
    cash_flow_total_items = frozenset(
        table.get("statement_total_items", {}).get("cash_flow", ())
    )
    for row in roles:
        if row.change_of is not None and row.change_of not in working_capital_ids:
            _invalid(
                name,
                "required_roles",
                f"{row.required_role!r} carries the change of {row.change_of!r}, "
                "which the catalog does not carry as a working-capital balance",
            )
        # A residual closes a statement total the filing prints, so it names a
        # cash-flow total the catalog declares in ``statement_total_items``.
        if row.residual_of is not None and not any(
            total.required_role == row.residual_of
            and total.statement == "cash_flow"
            and cash_flow_total_items.intersection(total.template_items)
            for total in roles
        ):
            _invalid(
                name,
                "required_roles",
                f"{row.required_role!r} is the residual of {row.residual_of!r}, "
                "which the catalog does not carry as a cash-flow statement total",
            )
        # A counterpart is the balance a cash flow lands on. Any balance-sheet role
        # qualifies (investment securities, an equity contra such as treasury stock);
        # the flow-destination owner reads its ``working_capital`` side for the sign
        # and treats a balance without one as a debit balance.
        if row.counterpart is not None and row.counterpart not in balance_sheet_ids:
            _invalid(
                name,
                "required_roles",
                f"{row.required_role!r} has the counterpart {row.counterpart!r}, "
                "which the catalog does not carry as a balance-sheet role",
            )
    roles = _materialize_aliases(roles, source=name)
    _validate_statement_totals(roles, source=name)
    _validate_role_homes(roles, source=name)
    balances = _decode_balances(
        roles, list(table.get("operating_balances", ())), source=name
    )
    counterparts = frozenset(
        balance.counterpart for balance in balances if balance.counterpart is not None
    )
    return RequiredRoleCatalog(
        source=source,
        template_kind=template_kind,
        provenance=provenance,
        required_roles=roles,
        nsp_balance_runoff_rows=MappingProxyType({
            balance: tuple(flows)
            for balance, flows in table.get("nsp_balance_runoff_rows", {}).items()
        }),
        disclosure_companions=MappingProxyType({
            origin: tuple((recipient, expected_src) for recipient, expected_src in edges)
            for origin, edges in table.get("disclosure_companions", {}).items()
        }),
        lease_reconciliation_shapes=MappingProxyType({
            name: LeaseReconciliationShape(
                target_item=row["target_item"], formula=row["formula"]
            )
            for name, row in table.get("lease_reconciliation_shapes", {}).items()
        }),
        model_scale_relations=tuple(
            ModelScaleRelation(
                source_item=row["source_item"], target_items=tuple(row["target_items"])
            )
            for row in table.get("model_scale_relations", ())
        ),
        statement_total_items=MappingProxyType({
            statement: tuple(items)
            for statement, items in table.get("statement_total_items", {}).items()
        }),
        balances=MappingProxyType({balance.required_role: balance for balance in balances}),
        declarable_balances=frozenset(
            balance.required_role
            for balance in balances
            if balance.required_role not in counterparts
            and balance.projection_owner is None
            and balance.target_item is not None
        ),
        legacy_balance_roles=MappingProxyType({
            token: balance.required_role
            for balance in balances
            for token in balance.legacy_roles
        }),
    )


@lru_cache(maxsize=None)
def _load_catalog(resolved: str) -> RequiredRoleCatalog:
    source = Path(resolved)
    try:
        raw = source.read_bytes()
    except OSError as exc:
        _invalid(source.name, "<file>", f"catalog artifact is unreadable: {exc}")
    try:
        payload = json.loads(raw)
    except ValueError as exc:
        _invalid(source.name, "<file>", f"catalog artifact is not valid JSON: {exc}")
    return decode_required_role_catalog(payload, source=source)


def load_required_role_catalog(path: Path | None = None) -> RequiredRoleCatalog:
    """Load and decode the catalog artifact (the shipped one by default)."""

    return _load_catalog(str(path or DEFAULT_CATALOG_PATH))


def balance_roles() -> frozenset[str]:
    """The balances a driver may declare ``balance_of``, derived from the shipped catalog."""

    return load_required_role_catalog().declarable_balances


def balance_declaration_refusal(required_role: str) -> BalanceDeclarationRefusal | None:
    """Why the shipped catalog lets no driver be ``required_role``; ``None`` when one may."""

    return load_required_role_catalog().declaration_refusal(required_role)


def pass_through_counterparts() -> tuple[CatalogBalance, ...]:
    """The obligations the shipped catalog's pass-through balances are held against."""

    return load_required_role_catalog().pass_through_counterparts()


def node_driven_balance_roles() -> frozenset[str]:
    """The declarable balances whose reported history only a declaring node moves.

    The template owns no schedule for them, so a bound balance no node
    declares has no projection driver.
    """

    catalog = load_required_role_catalog()
    return frozenset(
        role for role in catalog.declarable_balances if catalog.balances[role].node_driven
    )


__all__ = [
    "CATALOG_AUTHOR_LANE",
    "CATALOG_REPAIR_LANE",
    "CATALOG_SCHEMA_VERSION",
    "DEFAULT_CATALOG_PATH",
    "ITEM_RESOLUTION_REPAIR_LANE",
    "REQUIRED_ROLE_CATALOG_INVALID",
    "REQUIRED_ROLE_ITEM_UNRESOLVABLE",
    "REQUIRED_ROLE_UNKNOWN",
    "BalanceDeclarationRefusal",
    "PROJECTION_OWNERS",
    "ProjectionOwner",
    "CatalogBalance",
    "LeaseReconciliationShape",
    "ModelScaleRelation",
    "RequiredRole",
    "RequiredRoleCatalog",
    "RequiredRoleCatalogError",
    "RequiredRoleCatalogInvalid",
    "RequiredRoleItemUnresolvable",
    "RequiredRoleUnknown",
    "STATEMENT_PLANES",
    "TICKER_PLACEHOLDER",
    "balance_roles",
    "balance_declaration_refusal",
    "decode_required_role_catalog",
    "face_lines",
    "load_required_role_catalog",
    "node_driven_balance_roles",
    "pass_through_counterparts",
]
