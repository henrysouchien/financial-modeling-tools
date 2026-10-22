"""Required-line population for statement/schedule lines, reported as Findings.

Stage A step A-2 (plan §4.1/§4.2/§4.4/§4.5: `docs/design/
build-model-stage-a-join-deletion-implementation-plan-2026-08-18.md`;
design §5.2/§5.5): statement/schedule population takes the required set
(`derive_required_set` output) as a declared input. A required line + period
that the pin declares supported (coverage disposition ``observed`` /
``derived``) and that population never fills is the ``error`` Finding
``required_column_absent_from_pin``; a required line + period the pin
covers with a typed SRC-11 gap disposition completes as value-algebra
Missing (never a synthetic zero). Issuer-cited non-applicability and
not-separately-presented inclusion are outside both classes; optional lines
are outside this pass.
A registered always-on template formula whose required input line is
unpopulated where the formula is the standing value is the ``error``
Finding ``formula_input_missing``; a pinned observation that contradicts a
standing value is ``required_line_value_conflict`` (the standing value is
kept).

Everything here is pure decision code over declared inputs (P2): the plan is
derived once from the required set and the bundle coverage; the passes read
the model they are handed and return Findings (``schema/finding_vocabulary.py``)
carrying the starved target's identity and repair target (design §5.5). The
compile carries them to the Summary (`CompiledModelIntermediate.compile_findings`);
an ``error`` blocks publish, never the compile.

Where "required" is decided: a line is required when the required set
contains a statement-family row whose ``required_role`` binds to the
period's pinned filing column. Model items bind to
that role through their ``required_role`` stamp or the required-role catalog's
``population_items`` plane.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Collection, Iterable, Mapping, Sequence

from .build_formula_refs import _all_refs_same_period, _extract_ref_ids
from .build_model_items import _iter_items
from .build_model_product_contracts import HistoricalBasisFact
from .build_real_data import _item_has_direct_real_data
from .build_findings import make_finding
from .finding_vocabulary import Finding
from .models import (
    FinancialModel,
    FormulaType,
    ValueCell,
    ValueProvenance,
    ValueSeries,
)
from .provider_port.contracts import FilingLineObservation, FilingLineRef

if TYPE_CHECKING:
    from .build_model_product_contracts import (
        AnnualRequirementCoverage,
    )
    from .coverage_profile import AnnualSourceRequirement
    from .build_population import SourceEvidenceByLine
    from .provider_port.contracts import FilingLineFact
    from .required_set import RequiredSet
    from .role_disposition import RoleDispositionTable, RoleItems

#: Coverage dispositions under which the pin owes population a value.
SUPPORTED_DISPOSITIONS = frozenset({"observed", "derived"})

#: Template historical-formula types that constitute registered same-period
#: identities on the living plane (the `rd_pct`-class closed set rides these).
_REGISTERED_FORMULA_TYPES = frozenset(
    {FormulaType.arithmetic, FormulaType.ref, FormulaType.ratio}
)

#: Excluded from the required-formula scope for the same reason it is
#: excluded from formula-first: a no-source fallback bridge, not a
#: registered statement identity (`schema/build_formula_first.py`).
_REQUIRED_FORMULA_EXCLUDED_ITEM_IDS = frozenset(
    {"tpl.fm.cash_flow.cash_and_cash_equivalents_end_of_period"}
)

_REPAIR_TARGET_PIN = "pin-construction lane (bind the required role to its filing column)"
_REPAIR_TARGET_FORMULA = "required-role catalog lane (registered formula input)"


@dataclass(frozen=True)
class RequiredLineRow:
    """One statement-family required row, coverage-classified per period."""

    requirement_id: str
    required_role: str
    model_item: str
    pinned_columns: tuple[FilingLineRef, ...]
    expected_years: frozenset[int]
    gap_years: frozenset[int]
    unit: str = "usd_millions"
    scale: str = "millions"
    absent_years: frozenset[int] = frozenset()

    @property
    def required_years(self) -> frozenset[int]:
        return self.expected_years | self.gap_years


@dataclass(frozen=True)
class RequiredLinePlan:
    """The population-facing view of the required set (declared input).

    ``expected`` years carry a supported SRC-11 disposition (``observed`` /
    ``derived``): the pin owes population a value and absence is the typed
    blocker. ``gap`` years carry a blocking SRC-11 disposition and complete
    as value-algebra Missing. Issuer-cited non-applicable years are excluded
    from both classes. A required year with no typed coverage answer is
    classified ``expected`` (fail-closed).
    """

    rows: tuple[RequiredLineRow, ...]
    _expected_by_role: dict[str, frozenset[int]] = field(
        init=False, repr=False, compare=False, default_factory=dict
    )
    _required_by_role: dict[str, frozenset[int]] = field(
        init=False, repr=False, compare=False, default_factory=dict
    )
    _row_by_role: dict[str, RequiredLineRow] = field(
        init=False, repr=False, compare=False, default_factory=dict
    )
    _line_by_role_year: dict[tuple[str, int], FilingLineRef] = field(
        init=False, repr=False, compare=False, default_factory=dict
    )

    def __post_init__(self) -> None:
        for row in self.rows:
            self._expected_by_role[row.required_role] = (
                self._expected_by_role.get(row.required_role, frozenset())
                | row.expected_years
            )
            self._required_by_role[row.required_role] = (
                self._required_by_role.get(row.required_role, frozenset())
                | row.required_years
            )
            self._row_by_role.setdefault(row.required_role, row)
            for line in row.pinned_columns:
                year = _historical_year(line.period.canonical_label())
                if year is not None:
                    self._line_by_role_year[(row.required_role, year)] = line

    def is_required(self, required_role: str, year: int) -> bool:
        return int(year) in self._required_by_role.get(required_role, frozenset())

    def expects_pin_value(self, required_role: str, year: int) -> bool:
        return int(year) in self._expected_by_role.get(required_role, frozenset())

    def row_for_role(self, required_role: str) -> RequiredLineRow | None:
        return self._row_by_role.get(required_role)

    def pinned_line(self, required_role: str, year: int) -> FilingLineRef | None:
        return self._line_by_role_year.get((required_role, int(year)))


def _historical_year(period_id: str) -> int | None:
    if (
        period_id.startswith("FY")
        and not period_id.endswith("E")
        and period_id[2:].isdigit()
    ):
        return int(period_id[2:])
    return None


def _classify_requirement_row(
    requirement: "AnnualSourceRequirement",
    coverage_by_id: Mapping[str, "AnnualRequirementCoverage"],
) -> RequiredLineRow:
    coverage_row = coverage_by_id.get(requirement.requirement_id)
    disposition_by_period = (
        {}
        if coverage_row is None
        else {item.period_id: item.disposition for item in coverage_row.periods}
    )
    expected: set[int] = set()
    gap: set[int] = set()
    absent: set[int] = set()
    for period_id in requirement.period_ids:
        year = _historical_year(period_id)
        if year is None:
            continue
        cell = requirement.cell_for_period(period_id)
        disposition = disposition_by_period.get(period_id)
        if disposition == "not_applicable_evidenced" or cell.binding_state == "not_applicable":
            absent.add(year)
        if (
            cell.binding_state == "not_separately_presented"
            or disposition in {"not_applicable_evidenced", "not_separately_presented"}
        ):
            continue
        if disposition is None or disposition in SUPPORTED_DISPOSITIONS:
            expected.add(year)
        else:
            gap.add(year)
    return RequiredLineRow(
        requirement_id=requirement.requirement_id,
        required_role=requirement.required_role,
        model_item=requirement.model_item,
        pinned_columns=requirement.pinned_columns,
        expected_years=frozenset(expected),
        gap_years=frozenset(gap),
        unit=requirement.unit,
        scale=requirement.scale,
        absent_years=frozenset(absent),
    )


def derive_required_line_plan(
    required_set: "RequiredSet",
    coverage: Sequence["AnnualRequirementCoverage"],
) -> RequiredLinePlan:
    """Classify the required set's statement family against pin coverage.

    Pure over declared inputs: the derivation's statement rows and the
    sealed bundle coverage. Every applicable historical period lands in
    exactly one class — ``expected`` (supported disposition, or no typed
    answer: fail-closed) or ``gap`` (typed SRC-11 blocking disposition).
    Issuer-cited non-applicable and included periods land in neither class.
    """

    coverage_by_id = {row.requirement_id: row for row in coverage}
    return RequiredLinePlan(
        rows=tuple(
            _classify_requirement_row(requirement, coverage_by_id)
            for requirement in required_set.statement_rows()
        )
    )


def evidenced_absence_cells(
    role_dispositions: "RoleDispositionTable", role_items: Mapping[str, "RoleItems"],
) -> frozenset[tuple[str, int]]:
    """The ``(item_id, year)`` cells the pin cites ``not_applicable``: the table's cited-absence
    rows, projected onto each role's lines.

    A cited absence is a ``RoleDisposition`` row; ``item_owns_cell`` learns of it through its
    ``gaps`` argument (``build_offline_validation.owned_cell_gaps``), the channel a typed lag-input
    gap uses, so a coordinate trimmed from ``formula_periods`` for its citation stays a cell.
    """
    return frozenset(
        (item_id, int(period_id.removeprefix("FY")))
        for role, period_id in role_dispositions.cells_in("not_applicable")
        for item_id in role_items[role].all_items()
    )


def _catalog_line_ids(row: RequiredLineRow, *, ticker: str) -> tuple[str, ...]:
    from .required_role_catalog import load_required_role_catalog

    catalog = load_required_role_catalog()
    ids = catalog.population_item_ids(row.required_role, ticker=ticker)
    if ids:
        return ids
    if row.model_item and row.model_item != row.required_role:
        return (row.model_item,)
    return ()


#: Provenances the identity fill never contests: the accepted HFF is the
#: normalization authority for its exact targets (its own application pass
#: carries the legacy-conflict fail-closed check).
_IDENTITY_YIELDING_PROVENANCE = frozenset({ValueProvenance.historical_foundation})
def _requirement_value(
    evidence_by_line: "SourceEvidenceByLine",
    fact: "FilingLineFact | HistoricalBasisFact",
    *,
    unit: str,
    scale: str,
):
    """The fact's economic value from its SourceBundle evidence row (no re-conversion)."""

    from .build_population import evidenced_requirement_value

    return evidenced_requirement_value(evidence_by_line, fact, unit=unit, scale=scale)



_IDENTITY_VALUE_TOLERANCE = 1e-6


def apply_required_line_identity(
    model: FinancialModel,
    *,
    observations: Iterable[object],
    plan: RequiredLinePlan,
    required_set: "RequiredSet | None" = None,
    coverage: Sequence["AnnualRequirementCoverage"] = (),
    ticker: str,
    historical_periods: Sequence[int],
    overwrite_notes: Collection[str] = frozenset(),
    evidence_by_line: "SourceEvidenceByLine",
) -> tuple[Finding, ...]:
    """Populate catalog-bound required lines by identity from the pin.

    Empty cells fill from the exact pinned observation. A standing cell
    whose ``note`` is in ``overwrite_notes`` (compiler-derived residuals,
    e.g. the client-funds subtotal bridge) is overwritten — a reported
    balance is never relabeled as a compiler-derived value (plan §4.1
    ordering note). A standing HFF-authority value is never contested here
    (the HFF pass owns that conflict check). Any other standing value must
    agree with the observation within tolerance; a genuine conflict keeps
    the standing value and is returned as the ``error`` Finding
    ``required_line_value_conflict`` (step A-3's value-conflict discipline).
    """
    years = {int(period) for period in historical_periods}
    stamped_by_role: dict[str, list[str]] = {}
    for item in _iter_items(model):
        if item.required_role:
            stamped_by_role.setdefault(item.required_role, []).append(item.id)
    targets_by_role: dict[str, tuple[str, ...]] = {}
    plan_rows = {row.required_role: row for row in plan.rows}
    if required_set is not None:
        coverage_by_id = {row.requirement_id: row for row in coverage}
        for requirement in required_set.rows_for_family("operating"):
            if requirement.model_item and requirement.model_item != requirement.required_role:
                plan_rows.setdefault(
                    requirement.required_role,
                    _classify_requirement_row(requirement, coverage_by_id),
                )
    for required_role, row in plan_rows.items():
        line_ids = tuple(
            dict.fromkeys(
                (*stamped_by_role.get(required_role, ()), *_catalog_line_ids(row, ticker=ticker))
            )
        )
        if line_ids:
            targets_by_role[required_role] = line_ids
    if not targets_by_role:
        return ()

    role_by_line = {
        line: row.required_role
        for row in plan_rows.values()
        for line in row.pinned_columns
    }
    findings: list[Finding] = []
    for observation in observations:
        if isinstance(observation, HistoricalBasisFact):
            required_role = observation.required_role_correspondence
            if required_role not in targets_by_role:
                continue
            fact = observation
            year = _historical_year(observation.period_id)
            row = plan_rows[required_role]
            if year is None or year not in years or year not in row.required_years:
                continue
        elif isinstance(observation, FilingLineObservation):
            required_role = role_by_line.get(observation.line)
            if required_role not in targets_by_role or observation.status != "ok":
                continue
            fact = observation.fact
            if fact is None:
                continue
            year = _historical_year(observation.line.period.canonical_label())
            row = plan_rows[required_role]
            if year is None or year not in years or year not in row.required_years:
                continue
        else:
            continue
        for line_id in targets_by_role[required_role]:
            if line_id not in model._index:
                continue
            item = model.get_item(line_id)
            if item.overrides is not None and year in item.overrides:
                continue
            if item.values is None:
                item.values = ValueSeries()
            existing = item.values.values.get(year)
            if existing is not None:
                if existing.provenance in _IDENTITY_YIELDING_PROVENANCE:
                    continue
                if (existing.note or "") not in overwrite_notes:
                    converted = _requirement_value(
                        evidence_by_line, fact, unit=row.unit, scale=row.scale,
                    )
                    if (
                        existing.value is not None
                        and abs(float(existing.value) - float(converted))
                        > _IDENTITY_VALUE_TOLERANCE
                    ):
                        findings.append(
                            make_finding(
                                "required_line_value_conflict",
                                message=(
                                    f"{line_id}/FY{year} holds {existing.value!r} "
                                    f"but the pinned observation for "
                                    f"{required_role!r} is {converted!r}; the "
                                    f"standing value is kept [repair: "
                                    f"{_REPAIR_TARGET_PIN}]"
                                ),
                                item_id=line_id,
                                periods=(year,),
                                observed={
                                    "required_role": required_role,
                                    "standing": float(existing.value),
                                    "pinned": float(converted),
                                },
                            )
                        )
                    continue
            item.values.values[year] = ValueCell(
                period=year,
                value=float(
                    _requirement_value(
                        evidence_by_line, fact, unit=row.unit, scale=row.scale,
                    )
                ),
                provenance=ValueProvenance.imported_other,
            )
    return tuple(findings)


def required_line_population_findings(
    model: FinancialModel,
    *,
    plan: RequiredLinePlan,
    ticker: str,
    historical_periods: Sequence[int],
    formula_satisfied: Mapping[str, Collection[int]] | None = None,
    population_eligible_years: Mapping[str, frozenset[int]] | None = None,
) -> tuple[Finding, ...]:
    """The required-line pass: totality over the bound statement family.

    For every statement-family required row that binds to at least one line
    of this model kind (template stamp or role-catalog row), every
    pin-supported period must carry a real value on some bound line or be
    satisfied by a registered formula (`apply_formula_first`'s validated
    set). Anything else is one ``required_column_absent_from_pin`` Finding
    per required role, naming its lines, periods and pinned columns.
    Rows with no bound line in this model kind are not population targets
    (their coverage is the pin's own fail-closed machinery); periods the
    v1 statement-mapping substrate excludes from population keep that
    substrate's authority (Stage C surface).
    """

    satisfied = formula_satisfied or {}
    years = [int(period) for period in historical_periods]
    lines_by_role: dict[str, list] = {}
    for item in _iter_items(model):
        if item.required_role:
            lines_by_role.setdefault(item.required_role, []).append(item)

    findings: list[Finding] = []
    for row in plan.rows:
        stamped = lines_by_role.get(row.required_role, [])
        catalog_items = [
            model.get_item(line_id)
            for line_id in _catalog_line_ids(row, ticker=ticker)
            if line_id in model._index
        ]
        targets = {item.id: item for item in (*stamped, *catalog_items)}
        if not targets:
            continue
        eligible = (
            None
            if population_eligible_years is None
            else population_eligible_years.get(row.required_role)
        )
        absent_years = [
            year
            for year in years
            if year in row.expected_years
            and (eligible is None or year in eligible)
            and not any(
                _item_has_direct_real_data(item, year)
                or year in satisfied.get(item_id, ())
                for item_id, item in targets.items()
            )
        ]
        if not absent_years:
            continue
        line_ids = sorted(targets)
        pinned_columns = tuple(
            line
            for year in absent_years
            if (line := plan.pinned_line(row.required_role, year)) is not None
        )
        findings.append(
            make_finding(
                "required_column_absent_from_pin",
                message=(
                    f"{row.required_role} ({row.requirement_id}) periods "
                    f"{', '.join(f'FY{year}' for year in absent_years)} lines "
                    f"{', '.join(line_ids)} pinned columns {len(pinned_columns)}: "
                    f"the pin declares a supported value no channel populated "
                    f"[repair: {_REPAIR_TARGET_PIN}]"
                ),
                item_id=line_ids[0],
                periods=absent_years,
                observed={
                    "required_role": row.required_role,
                    "requirement_id": row.requirement_id,
                    "line_ids": ", ".join(line_ids),
                    "pinned_columns": len(pinned_columns),
                },
            )
        )
    return tuple(findings)


def required_formula_input_findings(
    model: FinancialModel,
    *,
    plan: RequiredLinePlan,
    historical_periods: Sequence[int],
    formula_satisfied: Mapping[str, Collection[int]] | None = None,
    population_eligible_years: Mapping[str, frozenset[int]] | None = None,
) -> tuple[Finding, ...]:
    """Registered formulas may not silently ride unpopulated required inputs.

    Scoped to the living plane's registered same-period template identities
    (design §5.4e; the `rd_pct` precedent): where such a formula is the
    standing value for a period and one of its direct input lines is a
    required line the pin declares supported, that input must carry real
    data or be formula-satisfied itself — otherwise one ``formula_input_missing``
    Finding names the formula, the starved input, and the periods.
    Mirrors ``required_line_population_findings`` on the Stage C authority
    boundary: input concept-years the v1 statement-mapping substrate
    excludes from population keep that substrate's authority and are never
    reported here. Years outside ``item.formula_periods`` are skipped — the
    historical formula is not the standing value there
    (``apply_formula_first``'s active-period rule).
    """

    satisfied = formula_satisfied or {}
    years = [int(period) for period in historical_periods]
    items = {item.id: item for item in _iter_items(model)}
    starved: dict[tuple[str, str, str], set[int]] = {}
    for item in items.values():
        if item.historical is None:
            continue
        if item.id in _REQUIRED_FORMULA_EXCLUDED_ITEM_IDS:
            continue
        if item.historical.type not in _REGISTERED_FORMULA_TYPES:
            continue
        if not _all_refs_same_period(item.historical.params):
            continue
        ref_ids = _extract_ref_ids(item.historical.params)
        if not ref_ids:
            continue
        active_periods = (
            None
            if item.formula_periods is None
            else {int(period) for period in item.formula_periods}
        )
        for year in years:
            if active_periods is not None and year not in active_periods:
                continue  # the formula is not the standing value this period
            if _item_has_direct_real_data(item, year):
                continue  # an imported value stands; the formula is not the cell
            for ref_id in ref_ids:
                ref_item = items.get(ref_id)
                if ref_item is None:
                    continue
                required_role = ref_item.required_role
                if not required_role or not plan.expects_pin_value(required_role, year):
                    continue
                eligible = (
                    None
                    if population_eligible_years is None
                    else population_eligible_years.get(required_role)
                )
                if eligible is not None and year not in eligible:
                    continue
                if _item_has_direct_real_data(ref_item, year):
                    continue
                if year in satisfied.get(ref_id, ()):
                    continue
                starved.setdefault((item.id, ref_id, required_role), set()).add(year)
    return tuple(
        make_finding(
            "formula_input_missing",
            message=(
                f"{formula_id} misses {input_id} ({required_role}) periods "
                f"{', '.join(f'FY{year}' for year in sorted(missing_years))} "
                f"[repair: {_REPAIR_TARGET_FORMULA}]"
            ),
            item_id=formula_id,
            periods=missing_years,
            observed={"input_line_id": input_id, "input_role": required_role},
        )
        for (formula_id, input_id, required_role), missing_years in sorted(
            starved.items()
        )
    )


__all__ = [
    "RequiredLinePlan",
    "RequiredLineRow",
    "SUPPORTED_DISPOSITIONS",
    "apply_required_line_identity",
    "derive_required_line_plan",
    "required_formula_input_findings",
    "required_line_population_findings",
]
