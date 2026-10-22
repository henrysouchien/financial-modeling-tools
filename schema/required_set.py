"""One required-set derivation per model kind.

`derive_required_set` is the single point through which a model kind's role
bindings become the required set. Baseline mode reads the model-kind role
catalog. Full mode reads the basis-segmented filing columns pinned for this
build and joins a role across segments only when the pin carries approved
correspondence for that boundary.

The function is pure over its declared inputs. The required set is fixed at
pin time, before evidence or coverage is evaluated. A material role that was
present under the prior pin and disappears from the new pin raises a typed
basis event rather than silently shrinking the denominator.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Any, Sequence

from schema.coverage_profile import AnnualSourceRequirement

_FISCAL_PERIOD_RE = re.compile(r"^FY\d{4}$")


class RequiredSetError(ValueError):
    """Typed failure of the required-set derivation."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


class RequiredSetBasisEvent(RequiredSetError):
    """A pin-relative presentation change requires an explicit repair."""

    def __init__(
        self,
        *,
        missing_roles: Sequence[str],
        basis_ids: Sequence[str],
    ) -> None:
        self.missing_roles = tuple(sorted(set(missing_roles)))
        self.basis_ids = tuple(basis_ids)
        super().__init__(
            "required_set_basis_shrink",
            "material roles required under the prior pin are absent from the "
            f"new presentation tree: roles={self.missing_roles!r}, "
            f"bases={self.basis_ids!r}",
        )


@dataclass(frozen=True)
class PinnedPresentationBasis:
    """One presentation-basis segment after pin construction.

    ``rows`` are role-bound filing columns for only this segment.
    ``correspondence_roles_from_prior`` records the roles whose cross-segment
    continuity was approved by the correspondence lane. The first segment
    carries none. ``prior_material_roles`` is the prior pin's material
    denominator and activates the R8 shrink guard for the new pin.
    """

    basis_id: str
    rows: tuple[AnnualSourceRequirement, ...]
    correspondence_roles_from_prior: frozenset[str] = frozenset()
    prior_material_roles: frozenset[str] = frozenset()

    def __post_init__(self) -> None:
        if not self.basis_id.strip():
            raise RequiredSetError(
                "required_set_basis_invalid", "basis_id must be non-empty"
            )
        if not self.rows:
            raise RequiredSetError(
                "required_set_basis_invalid",
                f"presentation basis {self.basis_id!r} has no required rows",
            )
        if any(row.family != "statement" for row in self.rows):
            raise RequiredSetError(
                "required_set_basis_invalid",
                f"presentation basis {self.basis_id!r} contains a non-statement row",
            )


@dataclass(frozen=True)
class RequiredSet:
    """The derivation's total output: typed requirement rows, window-scoped.

    ``rows`` is total over the model kind's roles: each row is a typed
    `AnnualSourceRequirement` whose *historical* periods (``FYnnnn``) are
    intersected with ``fiscal_window``; projection-axis periods
    (``FYnnnnE``) are outside the window's domain and pass through unscoped
    (the forecast-evidence family lives there). A role is dropped only when
    every one of its periods is historical and outside the window. There is
    no third state: a role is present (bound to its periods) or wholly
    outside the window — never silently missing.
    """

    rows: tuple[AnnualSourceRequirement, ...]
    fiscal_window: tuple[str, ...]
    _by_family: dict[str, tuple[AnnualSourceRequirement, ...]] = field(
        init=False, repr=False, compare=False, default_factory=dict
    )

    def rows_for_family(self, family: str) -> tuple[AnnualSourceRequirement, ...]:
        cached = self._by_family.get(family)
        if cached is None:
            cached = tuple(row for row in self.rows if row.family == family)
            self._by_family[family] = cached
        return cached

    def statement_rows(self) -> tuple[AnnualSourceRequirement, ...]:
        return self.rows_for_family("statement")

    def requirement_ids(self) -> tuple[str, ...]:
        return tuple(row.requirement_id for row in self.rows)


def _scope_row(
    row: AnnualSourceRequirement,
    window: tuple[str, ...],
) -> AnnualSourceRequirement | None:
    window_set = frozenset(window)
    scoped = tuple(
        period_id
        for period_id in row.period_ids
        if period_id in window_set or not _FISCAL_PERIOD_RE.match(period_id)
    )
    if not scoped:
        return None
    if scoped == row.period_ids:
        return row
    return row.model_copy(
        update={
            "period_ids": scoped,
            "pinned_columns": tuple(
                line
                for line in row.pinned_columns
                if line.period.canonical_label() in scoped
            ),
            "period_cells": tuple(
                cell for cell in row.period_cells if cell.period_id in scoped
            ),
        }
    )


def _require_unique_ids(rows: Sequence[AnnualSourceRequirement]) -> None:
    seen: set[str] = set()
    duplicates: list[str] = []
    for row in rows:
        if row.requirement_id in seen:
            duplicates.append(row.requirement_id)
        seen.add(row.requirement_id)
    if duplicates:
        raise RequiredSetError(
            "required_set_duplicate_requirement",
            f"duplicate requirement ids in the role catalog: "
            f"{sorted(set(duplicates))!r}",
        )


def _join_pinned_bases(
    bases: tuple[PinnedPresentationBasis, ...],
) -> tuple[AnnualSourceRequirement, ...]:
    basis_ids = tuple(basis.basis_id for basis in bases)
    if len(basis_ids) != len(set(basis_ids)):
        raise RequiredSetError(
            "required_set_basis_invalid",
            f"presentation basis ids must be unique: {basis_ids!r}",
        )
    if bases[0].correspondence_roles_from_prior:
        raise RequiredSetError(
            "required_set_basis_invalid",
            "the first presentation basis cannot declare correspondence from a prior "
            "segment",
        )

    by_role: dict[str, AnnualSourceRequirement] = {}
    order: list[str] = []
    prior_roles: frozenset[str] = frozenset()
    for index, basis in enumerate(bases):
        rows_by_role = {row.required_role: row for row in basis.rows}
        if len(rows_by_role) != len(basis.rows):
            raise RequiredSetError(
                "required_set_basis_invalid",
                f"presentation basis {basis.basis_id!r} repeats a required role",
            )
        if index:
            continuing = prior_roles & rows_by_role.keys()
            unapproved = continuing - basis.correspondence_roles_from_prior
            if unapproved:
                raise RequiredSetError(
                    "required_set_correspondence_unapproved",
                    f"presentation basis {basis.basis_id!r} continues roles without "
                    f"approved correspondence: {sorted(unapproved)!r}",
                )
            unknown = basis.correspondence_roles_from_prior - continuing
            if unknown:
                raise RequiredSetError(
                    "required_set_basis_invalid",
                    f"presentation basis {basis.basis_id!r} declares correspondence "
                    f"for non-continuing roles: {sorted(unknown)!r}",
                )

        for required_role, row in rows_by_role.items():
            previous = by_role.get(required_role)
            if previous is None:
                by_role[required_role] = row
                order.append(required_role)
                continue
            comparable_fields = (
                "requirement_id",
                "family",
                "model_item",
                "unit",
                "scale",
                "sign",
                "downstream_capabilities",
                "disclosed_child",
                "independently_material",
                "aggregate_parent_requirement_id",
            )
            drifted = tuple(
                field_name
                for field_name in comparable_fields
                if getattr(previous, field_name) != getattr(row, field_name)
            )
            if drifted:
                raise RequiredSetError(
                    "required_set_basis_invalid",
                    f"role {required_role!r} changes requirement semantics across "
                    f"presentation bases: {drifted!r}",
                )
            periods = previous.period_ids + row.period_ids
            columns = previous.pinned_columns + row.pinned_columns
            if len(periods) != len(set(periods)):
                raise RequiredSetError(
                    "required_set_basis_invalid",
                    f"role {required_role!r} repeats a period across presentation "
                    "bases",
                )
            by_role[required_role] = previous.model_copy(
                update={
                    "period_ids": periods,
                    "pinned_columns": columns,
                    "binding_state": (
                        previous.binding_state
                        if previous.binding_state == row.binding_state
                        else None
                    ),
                    "absence_citation": (
                        previous.absence_citation
                        if previous.absence_citation == row.absence_citation
                        else None
                    ),
                    "included_in": (
                        previous.included_in
                        if previous.included_in == row.included_in
                        else None
                    ),
                    "unbound_reason": (
                        previous.unbound_reason
                        if previous.unbound_reason == row.unbound_reason
                        else None
                    ),
                    "period_cells": (
                        *(previous.cell_for_period(period) for period in previous.period_ids),
                        *(row.cell_for_period(period) for period in row.period_ids),
                    ),
                }
            )
        prior_roles = frozenset(rows_by_role)

    prior_material_roles = frozenset().union(
        *(basis.prior_material_roles for basis in bases)
    )
    current_roles = frozenset(by_role)
    missing_roles = prior_material_roles - current_roles
    if missing_roles:
        raise RequiredSetBasisEvent(
            missing_roles=tuple(missing_roles),
            basis_ids=basis_ids,
        )
    return tuple(by_role[role] for role in order)


def derive_required_set(
    model_kind_roles: Sequence[AnnualSourceRequirement],
    bmr: Any,
    fiscal_window: Sequence[str],
    pinned_presentation_bases: Sequence[PinnedPresentationBasis] = (),
) -> RequiredSet:
    """Derive the fixed required set for baseline or full mode.

    An empty ``pinned_presentation_bases`` selects baseline mode and scopes the
    model-kind catalog rows. A non-empty sequence selects full mode: statement
    rows come from the pin's basis segments, while non-statement rows continue
    to come from the model-kind catalog.
    """

    del bmr

    window = tuple(fiscal_window)
    if not window:
        raise RequiredSetError(
            "required_set_window_invalid", "fiscal window is empty"
        )
    if len(window) != len(set(window)):
        raise RequiredSetError(
            "required_set_window_invalid", "fiscal window periods must be unique"
        )
    malformed = tuple(p for p in window if not _FISCAL_PERIOD_RE.match(p))
    if malformed:
        raise RequiredSetError(
            "required_set_window_invalid",
            f"fiscal window periods must match FYnnnn: {malformed!r}",
        )

    roles = tuple(model_kind_roles)
    bases = tuple(pinned_presentation_bases)
    if not roles and not bases:
        raise RequiredSetError(
            "required_set_roles_empty", "model kind role catalog is empty"
        )
    _require_unique_ids(roles)

    source_rows = roles
    if bases:
        pinned_statement_rows = _join_pinned_bases(bases)
        source_rows = (
            pinned_statement_rows
            + tuple(row for row in roles if row.family != "statement")
        )
        _require_unique_ids(source_rows)

    emitted = tuple(
        scoped
        for row in source_rows
        if (scoped := _scope_row(row, window)) is not None
    )
    if not emitted:
        raise RequiredSetError(
            "required_set_window_invalid",
            "no role of the model kind has a period inside the fiscal window",
        )
    return RequiredSet(rows=emitted, fiscal_window=window)
