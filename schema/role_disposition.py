"""The one per-(role, period) disposition table a model build consumes.

The table is the coverage owner's statement of what it knows about every
required-role catalog position in every historical period: ``bound``
(a pinned source), ``not_applicable`` (a cited absence),
``not_separately_presented`` (a cited inclusion in another line), ``unbound``
(owed and missing) or ``unowed`` (an ``optional_if_unreported`` role nothing
declares and no authored forecast owns; produced only by
:func:`resolve_build_dispositions` at build time, never stored). Rulings: SB-4, CITED-NOT-APPLICABLE-PROFILE-ROLES, JD-a,
DISCLOSURE-ADAPTS-ACCOUNTING-FIXED, FUNCTIONAL-EXPENSE-SPLIT-OPTIONAL
(``docs/reference/rulings-index.md``).

Nothing here is stored beside the pin: :func:`derive_role_dispositions` reads a
profile's ``annual_requirements``, so the table follows every re-pinned profile
and a profile stored before the table existed answers from its own rows
(``AnnualCoverageProfile.role_dispositions``). The module also owns the one
role -> lines lookup (:func:`role_items`), derived from the required-role catalog.
"""

from __future__ import annotations

from collections.abc import Mapping
from functools import cached_property
from typing import Literal

from pydantic import Field, model_validator

from .coverage_profile import (
    AbsenceCitation,
    AnnualCoverageProfile,
    AnnualSourceRequirement,
    PeriodPinCell,
    _CoverageContract,
)
from .build_value_writers import _authored_periods
from .models import FinancialModel, FormulaType, LineItem
from .provider_port.contracts import FilingLineRef
from .required_role_catalog import (
    RequiredRoleCatalog,
    face_lines,
    load_required_role_catalog,
)
from .statement_bindings_v2 import NOT_DISCLOSED_IN_VINTAGE

RoleDispositionState = Literal[
    "bound",
    "not_applicable",
    "not_separately_presented",
    "unbound",
    "unowed",
]

#: A correspondence change-set's wire state (``api/fms/modules/correspondence.py``): the door
#: keeps ``"deleted"`` on the wire for a row the change-set removes.
CorrespondenceChangeState = Literal[
    "bound",
    "not_applicable",
    "not_separately_presented",
    "unbound",
    "deleted",
]


def correspondence_state_to_role_disposition(state: CorrespondenceChangeState) -> RoleDispositionState:
    """The G2.4 boundary mapping from a correspondence wire state to a role disposition.

    Henry Q3: the door name stays ``fms_persist_correspondence`` and its wire payload keeps
    ``"deleted"``; in the resolved role-disposition table that removal is ``"unowed"``.
    Every other wire state carries through unchanged.
    """
    return "unowed" if state == "deleted" else state


class RoleDisposition(_CoverageContract):
    """One catalog role in one historical period."""

    required_role: str = Field(min_length=1)
    #: ``PeriodPinCell.period_id`` vocabulary (``FY2025``).
    period_id: str = Field(min_length=1)
    state: RoleDispositionState
    absence_citation: AbsenceCitation | None = None
    #: The aggregate that holds the amount; only ``not_separately_presented``.
    included_in: str | None = None
    pinned_ref: FilingLineRef | None = None
    unbound_reason: str | None = None

    @model_validator(mode="after")
    def _state_fields(self) -> "RoleDisposition":
        state = self.state
        cited = state in ("not_applicable", "not_separately_presented")
        if cited != (self.absence_citation is not None):
            raise ValueError(f"{state} disposition carries a citation exactly when it is cited")
        if (state == "not_separately_presented") != (self.included_in is not None):
            raise ValueError("only not_separately_presented carries included_in, and it must")
        if (state == "bound") != (self.pinned_ref is not None):
            raise ValueError("only bound carries a pinned_ref, and it must")
        if (state == "unbound") != (self.unbound_reason is not None):
            raise ValueError("only unbound carries unbound_reason, and it must")
        return self

    @property
    def optional_unreported(self) -> bool:
        """An ``optional_if_unreported`` role nothing declares: owed nothing, history included."""

        return self.state == "unbound" and self.unbound_reason == "optional_unreported"

    @property
    def not_disclosed_in_vintage(self) -> bool:
        """A period the agent declared on a stored ``unbound`` row as not printed by its filing: a historical absence."""

        return self.state == "unbound" and self.unbound_reason == NOT_DISCLOSED_IN_VINTAGE


class RoleDispositionTable(_CoverageContract):
    """Exactly one row per (catalog role, period_id)."""

    rows: tuple[RoleDisposition, ...]

    @model_validator(mode="after")
    def _complete(self) -> "RoleDispositionTable":
        keys = [(row.required_role, row.period_id) for row in self.rows]
        if len(keys) != len(set(keys)):
            raise ValueError("role disposition table repeats a (role, period)")
        periods = {period_id for _role, period_id in keys}
        roles = {role for role, _period_id in keys}
        catalog_roles = set(load_required_role_catalog().role_ids())
        if roles != catalog_roles or set(keys) != {
            (role, period_id) for role in roles for period_id in periods
        }:
            raise ValueError("role disposition table must cover every catalog role in every period")
        return self

    @cached_property
    def _index(self) -> dict[tuple[str, str], RoleDisposition]:
        return {(row.required_role, row.period_id): row for row in self.rows}

    def row(self, role: str, period_id: str) -> RoleDisposition:
        """A (role, period) the table does not carry is a ``KeyError``, never a default."""

        try:
            return self._index[(role, period_id)]
        except KeyError:
            raise KeyError(f"no role disposition for {role}/{period_id}") from None

    def state_of(self, role: str, period_id: str) -> RoleDispositionState:
        return self.row(role, period_id).state

    def by_role(self, role: str) -> tuple[RoleDisposition, ...]:
        """The role's rows in period order."""

        return tuple(row for row in self.rows if row.required_role == role)

    def roles_in(self, *states: RoleDispositionState) -> frozenset[str]:
        """Roles whose every period is in ``states``."""

        outside = {row.required_role for row in self.rows if row.state not in states}
        return frozenset({row.required_role for row in self.rows} - outside)

    def unowed_roles(self) -> frozenset[str]:
        return self.roles_in("unowed")

    def cells_in(self, *states: RoleDispositionState) -> frozenset[tuple[str, str]]:
        return frozenset(
            (row.required_role, row.period_id) for row in self.rows if row.state in states
        )

    def optional_unreported_cells(self) -> frozenset[tuple[str, str]]:
        """Cells that are ``unbound(optional_unreported)``: the evidence says the issuer owes them nothing."""

        return frozenset(
            (row.required_role, row.period_id) for row in self.rows if row.optional_unreported
        )


class RoleItems(_CoverageContract):
    """The model lines one catalog role binds, by plane."""

    required_role: str
    template_items: tuple[str, ...]
    #: ``{ticker}`` already resolved.
    population_items: tuple[str, ...]
    hff_fact_items: tuple[str, ...]
    #: ``face_lines(role)``: the Financial_model lines of ``template_items`` + the role's ``face_items``.
    face_items: tuple[str, ...]

    def all_items(self) -> tuple[str, ...]:
        return tuple(
            dict.fromkeys(
                (*self.template_items, *self.population_items, *self.hff_fact_items, *self.face_items)
            )
        )


def role_items(catalog: RequiredRoleCatalog, role: str, *, ticker: str) -> RoleItems:
    """The one role -> lines lookup; a role the catalog does not carry is a typed refusal."""

    row = catalog.role(role)
    if row.alias_of is not None:
        # An alias names the same lines as its home role for binding vocabulary only; the
        # home role is the one governor of those lines, so an alias owns no items and no
        # consumer can retire or override a home line through the alias's disposition.
        return RoleItems(
            required_role=role, template_items=(), population_items=(), hff_fact_items=(), face_items=(),
        )
    return RoleItems(
        required_role=role,
        template_items=row.template_items,
        population_items=catalog.population_item_ids(role, ticker=ticker),
        hff_fact_items=catalog.hff_fact_item_ids(role),
        face_items=face_lines(row),
    )


def home_role(catalog: RequiredRoleCatalog, role: str) -> str:
    """The one alias -> home resolution: the role whose lines a row stored under ``role`` lands
    on. A catalog alias answers its ``alias_of``; every other role (an uncatalogued
    ``operating.*`` series included) answers itself."""

    row = next((row for row in catalog.required_roles if row.required_role == role), None)
    return role if row is None or row.alias_of is None else row.alias_of


def items_by_role(catalog: RequiredRoleCatalog, *, ticker: str) -> Mapping[str, RoleItems]:
    return {
        role: role_items(catalog, role, ticker=ticker) for role in catalog.role_ids()
    }


def _disposition_from_cell(role: str, cell: PeriodPinCell) -> RoleDisposition:
    state = cell.binding_state
    # ``cell_for_period`` reads a stored ``formula`` cell as ``unbound``.
    assert state != "formula"
    return RoleDisposition(
        required_role=role,
        period_id=cell.period_id,
        state=state,
        absence_citation=cell.absence_citation,
        included_in=cell.included_in,
        pinned_ref=cell.pinned_ref if state == "bound" else None,
        unbound_reason=(cell.unbound_reason or "stored_without_reason") if state == "unbound" else None,
    )


def _states_the_line(cell: PeriodPinCell) -> bool:
    """A cell that says something about its line: anything but ``unbound``, or the agent's declared
    ``not_disclosed_in_vintage`` (a historical absence the issuer owes nothing for)."""
    return cell.binding_state != "unbound" or cell.unbound_reason == NOT_DISCLOSED_IN_VINTAGE


def derive_role_dispositions(profile: AnnualCoverageProfile) -> RoleDispositionTable:
    """The evidence table: every catalog role x every historical window period.

    The coverage derivation has no model, so it states evidence only and never
    ``unowed`` (that participation decision is :func:`resolve_build_dispositions`).

    * A role with a requirement row takes the row's pin cell for each period it
      carries (``bound`` / cited ``not_applicable`` / cited
      ``not_separately_presented`` / ``unbound``); a period the row does not
      carry is ``unbound(requirement_omits_period)``, never invented: an omitted period is the window gap
      ``statement_binding_window_gaps`` reports, so it stays a missing source until the agent binds it
      (``not_disclosed_in_vintage`` is a stored ``unbound`` cell's declared reason, read through
      the stored cell, never inferred from another period's binding).
    * A row stored under an alias role (``alias_of``) is its home role's evidence for the
      periods the home's own rows do not carry: the home takes the alias's stated cell (anything
      but ``unbound``, bar the agent's declared ``not_disclosed_in_vintage``) through the same path (``required_role`` = home); where both carry a
      period the home's own cell wins. The alias row also stays under its own role.
    * A role nothing declares is ``unbound(optional_unreported)`` when it is
      ``optional_if_unreported`` (and not ``presentation_optional``): it states no cell
      (``bound`` / cited) in any period and the profile does not declare it with
      no row of its own -- whether it has no requirement row, or a row whose every stored
      cell is ``unbound``. A role the profile declares (``required_roles``) with no row is
      ``unbound(declared_but_unbound)``. Every other role with no requirement row is owed and
      missing -- ``unbound(no_binding_row)``. ``presentation_optional`` is always
      ``no_binding_row``, so it is never ``optional_unreported`` (hence never ``unowed``)
      whatever else the role is flagged.
    * A stored ``unbound`` cell with no reason is ``unbound(stored_without_reason)``.
    """

    catalog = load_required_role_catalog()
    periods = tuple(
        dict.fromkeys(
            period_id
            for window in profile.historical_windows
            for period_id in window.period_ids
        )
    )
    rows_by_role: dict[str, list[AnnualSourceRequirement]] = {}
    for requirement in profile.annual_requirements:
        rows_by_role.setdefault(requirement.required_role, []).append(requirement)
    # A row stored under an alias role is the home role's evidence for its periods (the alias
    # carries no items, so the home is the one governor of the line); the map is ``home_role``'s.
    alias_rows_by_home: dict[str, list[AnnualSourceRequirement]] = {}
    for requirement in profile.annual_requirements:
        home = home_role(catalog, requirement.required_role)
        if home != requirement.required_role:
            alias_rows_by_home.setdefault(home, []).append(requirement)

    def evidence_row(name: str, period_id: str) -> AnnualSourceRequirement | None:
        own = next((row for row in rows_by_role.get(name, ()) if period_id in row.period_ids), None)
        if own is not None:
            return own
        # An alias's ``unbound`` cell is no evidence about the home line (the empty pin owes the
        # owed alias a row; the home's own optional/owed reason stands), so only a stated cell
        # flows: a cited cell, a bound one, or the agent's declared ``not_disclosed_in_vintage``.
        return next(
            (
                row
                for row in alias_rows_by_home.get(name, ())
                if period_id in row.period_ids and _states_the_line(row.cell_for_period(period_id))
            ),
            None,
        )

    declared = set(profile.required_roles)
    out: list[RoleDisposition] = []
    for role in catalog.required_roles:
        name = role.required_role
        optional = role.optional_if_unreported and not role.presentation_optional
        evidence = {period_id: evidence_row(name, period_id) for period_id in periods}
        stated = any(
            requirement is not None and requirement.cell_for_period(period_id).binding_state != "unbound"
            for period_id, requirement in evidence.items()
        )
        if not optional:
            absent_reason = "no_binding_row"
        elif name in declared and name not in rows_by_role:
            absent_reason = "declared_but_unbound"
        else:
            absent_reason = "optional_unreported"
        unreported = not stated and absent_reason == "optional_unreported"
        for period_id, requirement in evidence.items():
            if requirement is not None and not unreported:
                out.append(_disposition_from_cell(name, requirement.cell_for_period(period_id)))
            else:
                out.append(
                    RoleDisposition(
                        required_role=name,
                        period_id=period_id,
                        state="unbound",
                        unbound_reason=(
                            "requirement_omits_period"
                            if (name in rows_by_role or any(row is not None for row in evidence.values()))
                            and not unreported
                            else absent_reason
                        ),
                    )
                )
    return RoleDispositionTable(rows=tuple(out))


_SOFTWARE_BALANCE_ROLE = "statement.bs.capitalized_software"
_SOFTWARE_AMORTIZATION_ROLE = "statement.cf.capitalized_software_amortization"


def _derived_line(item: LineItem) -> bool:
    """The item's history is a template formula over other lines of its own sheet.

    A filed-line stamp (no formula, or a constant) is not derived, and neither is a formula
    reading a schedule or control line of another sheet.
    """

    spec = item.historical
    if spec is None or spec.type is FormulaType.constant:
        return False
    sheet = item.id.split(".", 2)[:2]
    operands = [reference.ref.id for reference in spec.references()]
    return bool(operands) and all(
        operand != item.id and operand.split(".", 2)[:2] == sheet for operand in operands
    )


def resolve_build_dispositions(
    table: RoleDispositionTable,
    model: FinancialModel,
    role_items: Mapping[str, RoleItems],
    *,
    projected: tuple[int, ...],
) -> RoleDispositionTable:
    """The one build-time participation decision, made where the model is known.

    Maps only ``unbound(optional_unreported)``: ``unowed`` when no item of the
    role (``role_items[role].all_items()``), or one of their own controls (the direct
    same-period operands of the item's projected spec — its rate / of_revenue companion,
    never an upstream formula or a statement face row (``tpl.fm.*``: a statement line,
    never a role's own control)), holds an authored projected cell in
    ``projected``, else ``unbound(authored_forecast_without_history)``
    (the forecast participates; history stays owed and missing). A role the template
    computes (``computed_by_template``) is never ``unowed`` for want of authorship: it
    is returned unchanged, ``unbound(optional_unreported)``, and its lines stay. An authored
    amortization rate does not participate when ``statement.bs.capitalized_software``
    is unowed: the rate has no standalone balance to act on, so
    ``statement.cf.capitalized_software_amortization`` stays ``unowed``. Every other
    row is returned unchanged. ``model``'s line-item index is built here when the caller has not
    built it (``FinancialModel.build_index``).
    """

    catalog = load_required_role_catalog()
    if not model._index:
        model.build_index()

    def own_controls(item: LineItem) -> tuple[LineItem, ...]:
        """The item's direct same-period operands (one hop of its projected spec): its own
        rate / of_revenue companions, never upstream formulas or a statement face row."""
        if item.projected is None:
            return ()
        return tuple(
            control
            for ref in item.projected.references()
            if (ref.ref.t or 0) == 0
            and not ref.ref.id.startswith("tpl.fm.")
            and (control := model._index.get(ref.ref.id)) is not None
            and control.id != item.id
        )

    def role_lines(role: str) -> tuple[LineItem, ...]:
        return tuple(
            item for item_id in role_items[role].all_items()
            if (item := model._index.get(item_id)) is not None
        )

    def computed_by_template(role: str) -> bool:
        """The template, not a filed line or an author, computes the role's value.

        Either the catalog declares it the residual of a statement total (``residual_of``) or
        the aggregate of the balance leaves no other role carries (``derived_from``), or
        every line the role names is a derived line: its history is a template formula
        (never a constant stamp) whose operands are all other lines of the same template
        sheet (EBITDA, free cash flow, cash and short-term investments). Nothing an author
        could write replaces such a formula, so ``optional_if_unreported`` on the role only
        means no filed line is owed: the role is never ``unowed`` for want of an authored
        forecast.
        """

        declared = catalog.role(role)
        if declared.residual_of is not None or declared.derived_from is not None:
            return True
        lines = role_lines(role)
        return bool(lines) and all(_derived_line(item) for item in lines)

    def authored(role: str) -> bool:
        return any(
            _authored_periods(candidate, projected)
            for item in role_lines(role)
            for candidate in (item, *own_controls(item))
        )

    optional_roles = {row.required_role for row in table.rows if row.optional_unreported}
    resolved = {role: authored(role) for role in optional_roles}
    template_computed = {role: computed_by_template(role) for role in optional_roles}
    # The amortization rate prices a standalone software balance. When that
    # balance is unowed, an authored rate does not make the amortization role
    # participate.
    balance_rows = tuple(
        row for row in table.rows if row.required_role == _SOFTWARE_BALANCE_ROLE
    )
    balance_participates = resolved.get(_SOFTWARE_BALANCE_ROLE, False)
    if balance_rows and all(
        row.state == "unowed" or (row.optional_unreported and not balance_participates)
        for row in balance_rows
    ):
        resolved[_SOFTWARE_AMORTIZATION_ROLE] = False
    return RoleDispositionTable(
        rows=tuple(
            row
            if not row.optional_unreported
            else row.model_copy(update={"unbound_reason": "authored_forecast_without_history"})
            if resolved[row.required_role]
            else row
            if template_computed[row.required_role]
            else RoleDisposition(
                required_role=row.required_role,
                period_id=row.period_id,
                state=correspondence_state_to_role_disposition("deleted"),
            )
            for row in table.rows
        )
    )


__all__ = [
    "CorrespondenceChangeState",
    "RoleDisposition",
    "RoleDispositionState",
    "RoleDispositionTable",
    "RoleItems",
    "correspondence_state_to_role_disposition",
    "derive_role_dispositions",
    "home_role",
    "items_by_role",
    "resolve_build_dispositions",
    "role_items",
]
