"""BM-node history through the identity channel — the join class is deleted.

Stage A step A-3 (plan §4.1/§4.3/§4.4: `docs/design/
build-model-stage-a-join-deletion-implementation-plan-2026-08-18.md`;
design §5.3/§5.4b). A BusinessModel node that *is* a pinned disclosed series
binds its history by identity — node key → requirement → pinned evidence →
compiled template item — with the retired executor's exactness discipline
re-expressed at the pinned-series hop and its dual-target write preserved
(`CompiledDriverRegistry.node_items` plus the direct-segment income-statement
item via ``segment_mapping``). A node that declares a calculation of its own (a
ratio, a product, a sum, or a sum that subtracts operands) takes its history from that
calculation over its sibling nodes' claimed series (and, for a sum, a pinned statement
role), computed by the existing formula-first/graph machinery (CMP-07 one
representation); no series name keys a formula table. A claim the compile
cannot lower (partial axis binding, double claim, a ``kpi_source``
naming no series, a claim with no compiled line, a calculation operand that
resolves to no series) is the ``error`` Finding
``business_model_node_invalid`` on that node — repair in the BMR authoring
lane; the compile applies every other claim and does not apply that one.
There is no binding row to author and no fourth arm.

How a node claims its series (the computable pairing that replaces the
seven authored ``business_model_historical_bindings`` rows): the node's own
``kpi_source`` declaration, resolved by local name against the model kind's
required-role vocabulary, or -- for a node with a calculation of its own -- any
series name.
The claim is BMR-owned structure
(the design's living precedent is the member-backed segment-revenue seed
channel, `_operational_seed_targets`): partial XBRL axis/member bindings
fail closed, and two nodes claiming one series fail closed as ambiguous.

Dangling claims fail closed (step A-5, plan §10 item 16): a declared
``kpi_source`` that names no valid series (for example a raw EDGAR tag QName)
is the ``error`` Finding ``business_model_node_invalid`` — repair in the BMR
authoring lane. Any ``operating.<slug>`` (or bare ``<slug>``) missing from the statement
bindings is instead ``business_model_node_unresolved`` with
``repair_owner=binding``; the declaration stays, and correspondence supplies
its history. The business-model tool's ``source_concept`` is held to the same
shape (`schema.business_model_judgment.KPI_SOURCE_PATTERN`), not to an enum of one issuer's series. A KPI
node with no ``kpi_source``
normally makes no claim — the shape for approximation nodes such as
``retention``. Multiple segments' same-id assumptions do not select
an owner for the company-wide pinned series, so they make no implicit claim.

Omission is not a declaration: every series the build pins for the ticker
(`bm_required_series`) is covered — each pinned operating-family identity
series is claimed by a node or
declined. One neither claims nor declines is the ``warning`` finding
``business_model_node_unresolved`` (R2), which a ``Declination`` clears: the
BusinessModel's ``declined_series`` rows are projected into
``ModelSpec.declinations`` (`declined_series_rows`) with the revision's author
and time, so the coverage predicate here never reads them
(`bm_series_coverage_findings`). An uncleared warning rides the published
version as an incompleteness Finding, never a publish block (Henry
2026-10-08 20:15, superseding R10's claim-only arm and TC-R2's publish
predicate).

Admission is one predicate (`admit_business_model`): every refusal the build
can decide from the business model and the pin alone — malformed or
ambiguous claims, partial axis bindings, a calculation whose declared unit is
not what its operands yield, a reported balance no node declares ``balance_of`` —
returned together so one authoring pass repairs
them all. The business-model door calls it before acceptance; the compile
reports the malformed-claim arms as Findings at this channel
(`derive_bm_series_claims`), series coverage as findings
(`bm_series_coverage_findings`), and the balance where the balance-sheet
schedule wires it. A claim the channel cannot write — its target line at
another type, a calculation's operand series absent from both the model
and the pin — is likewise a Finding on the node, and that claim is not
applied (`apply_business_model_node_history` returns `BmNodeHistoryResult`).

Everything here is pure decision code over declared inputs (P2): the BMR
revision, the sealed pin (requirements / coverage / evidence), the required
set, and the compiled registry.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from decimal import Decimal
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Callable, Iterable, Literal, Mapping, NoReturn, Sequence

from .build_findings import make_finding
from .finding_vocabulary import Finding
from .business_model import (
    Dimension,
    NodeMeasure,
    UNIT_DIMENSION,
    declared_balance_role,
    is_statement_role,
    node_measure,
    product_scale_exponent,
    product_dimension,
    quotient_dimension,
    sum_operand_matches_output,
)
from .business_model_judgment import KPI_SOURCE_PATTERN
from .business_model_compiler_ratio import (
    period_mean,
    pinned_series_holds_average,
    ratio_averages_operand,
    ratio_scale_exponent,
    ratio_terms,
    row_holds_average_series,
    scaled,
)
from .business_model_revision import BusinessModelRevision
from .required_role_catalog import node_driven_balance_roles
from .dependency_graph import DependencyGraph
from .historical_driver_seeds import HistoricalDriverSeed
from .models import (
    FinancialModel,
    FormulaSpec,
    LineItem,
    FormulaType,
    ItemType,
    LineItemRef,
    ModelScale,
    ValueCell,
    ValueProvenance,
    ValueSeries,
)
from .renderer_columns import item_owns_cell
from .scale_conversion import SCALE_EXPONENT
from industry_slice_core.industry_types import Unit

if TYPE_CHECKING:
    from .business_model_compiler import CompiledDriverRegistry
    from .build_model_product_contracts import (
        AnnualRequirementCoverage,
        SourceBundle,
        SourceEvidence,
    )
    from .coverage_profile import AnnualSourceRequirement
    from .required_set import RequiredSet


#: Seed-coverage vocabulary of the identity channel (plan §7.3 set 2 — the
#: retired binding executor's observation rows and note strings migrate to
#: this vocabulary).
IDENTITY_SOURCE_KIND = "business_model_node_identity"
FORMULA_SOURCE_KIND = "business_model_node_formula"
#: A first-year cell's opening operand: the pin's opening-column evidence.
OPENING_BALANCE_SOURCE_KIND = "source_bundle_opening_balance"
_IDENTITY_NOTE_PREFIX = "bm_node_identity"
_FORMULA_NOTE_PREFIX = "registered_bm_formula"

_REPAIR_BMR = "BMR authoring lane (declare the node's disclosed-series identity)"
_REPAIR_PIN = "corpus/profile pin lane (SourceBundle evidence for the issuer concept)"

#: Requirement (unit, scale) -> compiled item (Unit, model_scale) — the
#: retired executor's exactness table, carried whole.
_SOURCE_TYPES: dict[tuple[str, str], tuple[Unit, ModelScale]] = {
    ("count", "units"): (Unit.item_count, "units"),
    ("count", "millions"): (Unit.item_count, "millions"),
    ("share_count", "millions"): (Unit.share_count, "millions"),
    ("ratio", "units"): (Unit.percentage, "units"),
    ("usd_millions", "millions"): (Unit.dollars, "millions"),
    ("usd_per_client", "units"): (Unit.per_count, "units"),
}

_KPI_SOURCE_RE = re.compile(
    r"^[A-Za-z_][A-Za-z0-9_.-]*(?::[A-Za-z_][A-Za-z0-9_.-]*)?$"
)
_DIMENSION_SUFFIXES = ("Member", "Domain", "Axis")
_KPI_SOURCE_SHAPE_RE = re.compile(KPI_SOURCE_PATTERN)

#: BM-series families of the required set on the living plane (plan §4.2:
#: the operating/BM portion is the operating family).
_BM_SERIES_FAMILIES = frozenset({"operating"})


class BmNodeHistoryCompileError(ValueError):
    """The identity channel cannot resolve or apply a BM node's history."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code


def _fail(code: str, message: str) -> NoReturn:
    raise BmNodeHistoryCompileError(code, message)


@dataclass(frozen=True)
class BmAdmissionRefusal:
    """One reason the build cannot compile a business model, with its repair.

    ``node_key`` and ``observed`` name the node and the declared input a
    per-node refusal concerns; the compile reports the refusal as that node's
    Finding (`finding`).
    """

    code: str
    message: str
    node_key: str | None = None
    observed: tuple[tuple[str, str], ...] = ()

    def finding(self) -> Finding:
        return make_finding(
            self.code,
            message=self.message,
            item_id=self.node_key,
            observed=dict(self.observed),
        )


@dataclass(frozen=True)
class BmNodeHistoryResult:
    """What the identity channel wrote, and the claims it could not apply.

    ``unresolved_items`` are the compiled item ids of the nodes whose finding is
    ``business_model_node_unresolved``: that finding is the one record of the
    node's missing history, and the document stamps the node's unwritten
    historical cells with the same typed reason
    (`model_build.target_build.mark_missing_inputs`)."""

    seed_coverage: tuple[dict[str, Any], ...]
    findings: tuple[Finding, ...]
    unresolved_items: tuple[str, ...] = ()


@dataclass(frozen=True)
class BmOperand:
    """One operand of a node's own calculation.

    ``ref`` is the sibling node's key (``segment.node``) or a statement role id.
    ``claim`` is that sibling's own claim (``None`` for a statement role and for
    a sibling that claims no series); ``measure`` is its declared measure
    (``None`` for a statement role, whose type is the pin's); ``average`` is
    whether its row holds period-end observations of a ``period_average``
    series, so a ratio over it takes their mean (`ratio_averages_operand`).
    """

    ref: str
    claim: "BmSeriesClaim | None" = None
    measure: NodeMeasure | None = None
    average: bool = False


@dataclass(frozen=True)
class BmCalculation:
    """A node's own calculation, in the shapes the history channel evaluates.

    ``ratio``: ``(numerator, denominator)``. ``product``: two or more factors.
    ``sum``: two or more positive addends. ``remainder``: one positive operand,
    then the operands it subtracts. ``measure`` is the node's declared
    measure, the type its history is written at.
    """

    kind: Literal["ratio", "product", "sum", "remainder"]
    operands: tuple[BmOperand, ...]
    measure: NodeMeasure


@dataclass(frozen=True)
class BmSeriesClaim:
    """One node's declared claim on a series (BMR structure), with the node's
    own calculation when it declares one the history channel evaluates."""

    node_key: str
    segment_id: str
    node_id: str
    series: str
    calculation: BmCalculation | None = None


@dataclass(frozen=True)
class BmNodeIdentity:
    """Identity arm: the node *is* the pinned disclosed series."""

    claim: BmSeriesClaim
    requirement_id: str
    required_role: str

@dataclass(frozen=True)
class RegisteredBmFormula:
    """One company-generic formula over a node's own calculation operands: a ratio, a
    product, a plain sum or a remainder."""

    formula_id: str
    numerator_series: str
    expected_type: tuple[Unit, ModelScale]
    expression: str
    operand_measures: Mapping[str, NodeMeasure]
    denominator_series: str | None = None
    #: Series subtracted from the numerator for a remainder formula.
    subtract_series: tuple[str, ...] = ()
    #: The further factors multiplied with the numerator series for a product formula.
    factor_series: tuple[str, ...] = ()
    #: The further series added to the numerator series for a plain sum formula.
    addend_series: tuple[str, ...] = ()
    #: Denominator is the mean of opening and ending balances of the series.
    average_denominator: bool = False

    def scale_multiplier(self, input_measures: Mapping[str, NodeMeasure]) -> Decimal:
        """Derive the numeric conversion from the operands to the output type.

        A product converts by every factor's scale; a ratio by its numerator and
        denominator (`ratio_scale_exponent`, the one conversion the projected lane
        applies too); a sum's operands share the output type, so it is one.
        """
        output = NodeMeasure(*self.expected_type, basis=None)
        if self.denominator_series is not None:
            return Decimal(10) ** ratio_scale_exponent(
                input_measures[self.numerator_series],
                input_measures[self.denominator_series],
                output,
            )
        return Decimal(10) ** product_scale_exponent(
            [
                input_measures[series]
                for series in (self.numerator_series, *self.factor_series)
            ],
            output,
        )

    def input_series(self) -> tuple[str, ...]:
        return (
            self.numerator_series,
            *((self.denominator_series,) if self.denominator_series is not None else ()),
            *self.factor_series,
            *self.addend_series,
            *self.subtract_series,
        )

    def historical_spec(
        self,
        *,
        input_operands: Mapping[str, Any],
        input_measures: Mapping[str, NodeMeasure] | None = None,
        prior_input_operands: Mapping[str, Any] | None = None,
    ) -> FormulaSpec:
        numerator: Any = input_operands[self.numerator_series]
        multiplier = float(
            self.scale_multiplier(
                self.operand_measures if input_measures is None else input_measures
            )
        )
        if self.factor_series:
            expression: Any = {
                "op": "*",
                "args": [
                    scaled(numerator, multiplier),
                    *(input_operands[series] for series in self.factor_series),
                ],
            }
        elif self.addend_series:
            expression = {
                "op": "+",
                "args": [
                    scaled(numerator, multiplier),
                    *(input_operands[series] for series in self.addend_series),
                ],
            }
        elif self.subtract_series:
            expression = {
                "op": "+",
                "args": [
                    scaled(numerator, multiplier),
                    *(
                        {"op": "NEG", "arg": input_operands[series]}
                        for series in self.subtract_series
                    ),
                ],
            }
        else:
            if self.denominator_series is None:
                raise ValueError("registered ratio formula requires a denominator")
            opening: Any | None = None
            if self.average_denominator:
                opening = (prior_input_operands or {}).get(self.denominator_series)
                if opening is None:
                    raise ValueError(
                        "average-denominator formula requires an opening operand"
                    )
            numerator, denominator = ratio_terms(
                numerator,
                input_operands[self.denominator_series],
                multiplier=multiplier,
                opening_denominator=opening,
            )
            expression = {"op": "/", "left": numerator, "right": denominator}
        return FormulaSpec(
            type=FormulaType.arithmetic,
            params={"expr": expression},
            note=f"{_FORMULA_NOTE_PREFIX}:{self.formula_id}",
        )

    def computed_value(
        self,
        *,
        input_values: Mapping[str, Decimal],
        input_measures: Mapping[str, NodeMeasure] | None = None,
        prior_input_values: Mapping[str, Decimal] | None = None,
    ) -> Decimal | None:
        """The formula's value, or ``None`` when its denominator is zero.

        A zero denominator is a genuine zero — an observed balance or a
        closure-corroborated ``Known(0)`` — not an evidenced absence. The
        ratio has no value that year. An evidenced absence is not an operand.
        """
        numerator = input_values[self.numerator_series] * self.scale_multiplier(
            self.operand_measures if input_measures is None else input_measures
        )
        if self.factor_series:
            value = numerator
            for series in self.factor_series:
                value *= input_values[series]
        elif self.addend_series:
            value = numerator + sum(
                (input_values[series] for series in self.addend_series),
                start=Decimal("0"),
            )
        elif self.subtract_series:
            value = numerator - sum(
                (input_values[series] for series in self.subtract_series),
                start=Decimal("0"),
            )
        else:
            if self.denominator_series is None:
                raise ValueError("registered ratio formula requires a denominator")
            denominator = input_values[self.denominator_series]
            if self.average_denominator:
                prior = (prior_input_values or {}).get(self.denominator_series)
                if prior is None:
                    raise ValueError(
                        "average-denominator formula requires an opening value"
                    )
                denominator = (prior + denominator) / Decimal("2")
            if denominator == 0:
                return None
            value = numerator / denominator
        return value


@dataclass(frozen=True)
class BmNodeFormula:
    """Formula arm: the node is a registered ratio over identity-bound lines."""

    claim: BmSeriesClaim
    formula: RegisteredBmFormula


@dataclass(frozen=True)
class BmNodeBlocker:
    """Blocker arm: ``business_model_node_invalid`` (design §5.5)."""

    claim: BmSeriesClaim
    attempted: str
    repair: str = _REPAIR_BMR


def _iter_nodes(nodes: Sequence[Any]):
    for node in nodes:
        yield node
        yield from _iter_nodes(tuple(getattr(node, "children", None) or ()))


def _series_name(raw_source: object) -> str | None:
    """A declared source's series local name, or None when unusable
    (invalid syntax or a dimension QName — never a series)."""

    raw = str(raw_source or "").strip()
    if not raw or _KPI_SOURCE_RE.fullmatch(raw) is None:
        return None
    local_name = raw.rsplit(":", 1)[-1]
    if local_name.endswith(_DIMENSION_SUFFIXES):
        return None
    return local_name.lower()


def _series_reference(node: Any) -> str | None:
    return _series_name(getattr(node, "kpi_source", None))


def _axis_binding_refusal(
    segment: Any, node: Any, node_key: str
) -> BmAdmissionRefusal | None:
    """Partial XBRL axis/member bindings fail closed (the seed-channel rule)."""

    node_axis = str(getattr(node, "edgar_axis", None) or "").strip()
    node_member = str(getattr(node, "edgar_member", None) or "").strip()
    segment_axis = str(getattr(segment, "edgar_axis", None) or "").strip()
    segment_member = str(getattr(segment, "edgar_member", None) or "").strip()
    axis = node_axis or segment_axis
    member = node_member or segment_member
    if bool(axis) == bool(member):
        return None
    return BmAdmissionRefusal(
        "business_model_node_invalid",
        f"node {node_key!r} carries a partial XBRL axis/member binding; "
        f"repair: {_REPAIR_BMR}",
        node_key=node_key,
        observed=(("edgar_axis", axis), ("edgar_member", member)),
    )


def _has_identity_series(requirement: "AnnualSourceRequirement") -> bool:
    return requirement.binding_state != "unbound"


def _identity_series(
    requirements: Iterable["AnnualSourceRequirement"],
) -> frozenset[str]:
    """The claim spellings that resolve by identity (`resolve_bm_node_history`)."""

    rows = tuple(row for row in requirements if _has_identity_series(row))
    return frozenset(
        row.required_role.removeprefix(f"{row.family}.")
        for row in rows
        if row.family in _BM_SERIES_FAMILIES
    ) | frozenset(row.required_role for row in rows)


def bm_series_vocabulary(
    requirements: Iterable["AnnualSourceRequirement"],
) -> frozenset[str]:
    """Every name a node's ``kpi_source`` may claim *by identity* over one model
    kind's roles.

    A claim names its series BMR-locally (the role leaf within a BM family) or
    as a full catalog role id verbatim (``statement.is.revenue``) -- the
    spellings `resolve_bm_node_history` resolves by identity. A node that
    declares a calculation of its own (`_calculation_shape`) may also claim any
    series name: its history is that calculation, not a pinned series. The
    compile passes the build's required set, the one vocabulary an identity
    claim is held to.
    """

    return _identity_series(requirements)


def _canonical_series(series: str) -> str:
    """One spelling per series: a BM-family role id and its leaf name one row."""

    for family in _BM_SERIES_FAMILIES:
        prefix = f"{family}."
        if series.startswith(prefix):
            return series.removeprefix(prefix)
    return series


@dataclass(frozen=True)
class BmRequiredSeries:
    """What one build's pin requires of a business model, read once.

    ``series``: every pinned BM-family identity series by its role leaf -- the
    model claims or declines each with a reason. ``carried_identity``:
    the pinned identity series whose type the identity channel carries
    (`_SOURCE_TYPES`), the rows a claim seeds reported history from; a claim
    of any other identity series cannot build. ``identity_series``: every
    claim spelling that resolves by identity rather than as a registered
    formula. ``roles_with_history``: every balance a node may declare
    (`node_driven_balance_roles`) whose statement row the pin binds — reported history whose
    projection only a ``balance_of`` node drives.
    """

    series: frozenset[str]
    carried_identity: frozenset[str]
    identity_series: frozenset[str]
    roles_with_history: frozenset[str]

    @property
    def vocabulary(self) -> frozenset[str]:
        """`bm_series_vocabulary` over the same rows."""

        return self.identity_series


def bm_required_series(
    requirements: Iterable["AnnualSourceRequirement"],
) -> BmRequiredSeries:
    """The series a business model must claim or decline for one build.

    The spellings `resolve_bm_node_history` resolves. The compile passes the
    build's required set, whose rows the SourceBundle pin bound; an authored
    profile's unpinned rows require nothing. The business-model door passes
    the same pin (`model_build.corpus_facts.required_business_model_series`).
    """

    rows = tuple(requirements)
    pinned = tuple(
        row
        for row in rows
        if row.pinned_columns
    )
    bound_roles = frozenset(row.required_role for row in pinned)

    # The schedule owns balance roles, including each bridge's held-against
    # liability. They are not driver series. A different workbook model_item
    # alone says nothing about whether a pinned operating series is a balance.
    from .build_company_grounded_schedules import opening_balance_roles

    balance_roles = opening_balance_roles()
    identity = tuple(
        row
        for row in pinned
        if row.family in _BM_SERIES_FAMILIES
        and row.required_role not in balance_roles
    )
    return BmRequiredSeries(
        series=frozenset(
            row.required_role.removeprefix(f"{row.family}.") for row in identity
        ),
        carried_identity=frozenset(
            row.required_role.removeprefix(f"{row.family}.")
            for row in identity
            if (row.unit, row.scale) in _SOURCE_TYPES
        ),
        identity_series=_identity_series(rows),
        roles_with_history=bound_roles & node_driven_balance_roles(),
    )


def declined_series_rows(business_model: Any) -> tuple[tuple[str, str], ...]:
    """Each ``declined_series`` row as (series, reason), by its canonical name.

    The name is the spelling `bm_series_coverage_findings` prints as the
    ``business_model_node_unresolved`` finding's ``item_id``, so the
    Declination the ModelSpec assembly projects from the row matches it.
    """

    rows: dict[str, str] = {}
    for declined in tuple(getattr(business_model, "declined_series", None) or ()):
        name = _series_name(getattr(declined, "series", None))
        if name is not None:
            rows.setdefault(_canonical_series(name), str(declined.reason))
    return tuple(sorted(rows.items()))


def bm_series_coverage_findings(
    business_model: Any,
    *,
    required: BmRequiredSeries,
) -> tuple[Finding, ...]:
    """Series coverage as findings: the compile's arm of the one predicate.

    A required series no node claims is the ``warning``
    ``business_model_node_unresolved`` (R2), cleared only by a Declination
    naming the series (``ModelSpec.declinations``, into which
    ``declined_series`` is projected) — this predicate never reads
    ``declined_series`` for it. Malformed claims are the identity channel's
    ``business_model_node_invalid`` findings (`derive_bm_series_claims`).
    """

    claimed, _refusals, _unresolved = _claims(business_model, required)
    unclaimed = sorted(
        required.series - {_canonical_series(claim.series) for claim, _node in claimed}
    )
    return tuple(
        make_finding(
            "business_model_node_unresolved",
            message=(
                f"pinned series {series!r} is claimed by no node; claim it "
                "(reported_kpi.source_concept on the driver that is that "
                "series) or decline it with the reason the model does not "
                "carry it"
            ),
            item_id=series,
        )
        for series in unclaimed
    )


def _calculation_shape(
    node: Any,
) -> tuple[Literal["ratio", "product", "sum", "remainder"], tuple[Any, ...]] | None:
    """The node's own calculation in a shape the history channel evaluates.

    ``ratio`` (a ``derived`` expression: numerator over denominator),
    ``product`` (two or more positive operands multiplied), ``remainder`` (a
    ``sum`` with exactly one positive operand and at least one subtracted one)
    or ``sum`` (two or more positive operands added), every operand at
    ``t = 0``. The operands come back as the expression's refs, the positive
    one first for a remainder. Any other calculation (roll-forward, growth,
    external, a mixed-sign sum) is computed by the node's compiled row or
    supplied by its owner: it has no history formula. A roll-forward node's
    history is its disclosed balance.
    """

    expression = (
        getattr(node, "children_formula", None)
        if getattr(node, "children_role", None) == "decomposition"
        else getattr(node, "driver", None)
    )
    if expression is None:
        return None
    kind: Literal["ratio", "product", "sum", "remainder"]
    if expression.type == "derived":
        refs: tuple[Any, ...] = (expression.params.numerator, expression.params.denominator)
        if any(ref.sign != 1 for ref in refs):
            return None
        kind = "ratio"
    elif expression.type == "product":
        refs = tuple(expression.params.operands)
        if any(ref.sign != 1 for ref in refs):
            return None
        kind = "product"
    elif expression.type == "sum":
        operands = tuple(expression.params.operands)
        positive = tuple(ref for ref in operands if ref.sign == 1)
        negative = tuple(ref for ref in operands if ref.sign == -1)
        if len(positive) + len(negative) != len(operands):
            return None
        if len(positive) == 1 and negative:
            refs = (*positive, *negative)
            kind = "remainder"
        elif len(positive) >= 2 and not negative:
            refs = positive
            kind = "sum"
        else:
            return None
    else:
        return None
    if any(ref.t != 0 or ref.node_id == "self" for ref in refs):
        return None
    return kind, refs


def _claims(
    business_model: Any,
    required: BmRequiredSeries,
) -> tuple[
    tuple[tuple[BmSeriesClaim, Any], ...],
    tuple[BmAdmissionRefusal, ...],
    tuple[BmAdmissionRefusal, ...],
]:
    """Every node's series claim, with the per-node refusals claiming
    meets: (claimed, axis, duplicate and cycle refusals, unresolved ``kpi_source``).

    A claim is the node's declared ``kpi_source``: a series the pin carries
    (identity), or any series name for a node that declares a calculation of its
    own (`_calculation_shape`) -- its history is that calculation over its
    sibling nodes' claimed series (and, for a remainder, a pinned statement
    role). A node with no ``kpi_source`` makes no claim: it is an assumption over
    evidence. Each claim carries the node's calculation, with the operand
    siblings' own claims (`BmOperand`), so `resolve_bm_node_history` resolves
    from the claim alone. A calculation cycle among claimed nodes refuses the
    node.

    Fail-closed ambiguity: two nodes claiming one series is the
    duplicate-source condition of the living seed-channel precedent
    (`_operational_seed_targets`), generalized to the identity channel. Every
    claimant of an ambiguous series is refused, so none of them is applied.
    """

    series_vocabulary = required.vocabulary
    refusals: list[BmAdmissionRefusal] = []
    unresolved: list[BmAdmissionRefusal] = []
    by_series: dict[str, str] = {}
    ambiguous_series: set[str] = set()
    registered: dict[str, tuple[str, Any, str]] = {}

    def register(
        segment: Any, segment_id: str, node: Any, node_key: str, series: str
    ) -> None:
        axis_refusal = _axis_binding_refusal(segment, node, node_key)
        if axis_refusal is not None:
            refusals.append(axis_refusal)
        prior = by_series.get(series)
        if prior is not None and prior != node_key:
            if series not in ambiguous_series:
                ambiguous_series.add(series)
                refusals.append(
                    BmAdmissionRefusal(
                        "business_model_node_invalid",
                        f"series {series!r} is claimed by both {prior!r} and "
                        f"{node_key!r}; repair: {_REPAIR_BMR}",
                        node_key=prior,
                        observed=(("series", series), ("claimed_by", node_key)),
                    )
                )
            refusals.append(
                BmAdmissionRefusal(
                    "business_model_node_invalid",
                    f"series {series!r} is claimed by both {prior!r} and "
                    f"{node_key!r}; repair: {_REPAIR_BMR}",
                    node_key=node_key,
                    observed=(("series", series), ("claimed_by", prior)),
                )
            )
            return
        by_series[series] = node_key
        registered[node_key] = (segment_id, node, series)

    segments = tuple(getattr(business_model, "segments", None) or ())

    def nodes_of(segment: Any):
        return _iter_nodes(
            tuple(
                getattr(getattr(segment, "revenue_model", None), "decomposition", None)
                or ()
            )
        )

    all_nodes: dict[str, Any] = {}
    for segment in segments:
        segment_id = str(getattr(segment, "id", "") or "")
        for node in nodes_of(segment):
            node_id = str(getattr(node, "id", "") or "")
            node_key = f"{segment_id}.{node_id}"
            all_nodes[node_key] = node
            if not bool(getattr(node, "kpi", False)):
                continue
            raw_source = str(getattr(node, "kpi_source", None) or "").strip()
            if not raw_source:
                # No declaration, no claim: the node is an assumption over
                # evidence (module docstring — the deliberate v13 shape).
                continue
            series = _series_reference(node)
            if series is None or (
                series not in series_vocabulary and _calculation_shape(node) is None
            ):
                # Any operating series is a valid declaration whether or not
                # this pin binds it: missing history belongs to binding, not
                # to the node's disclosed-series declaration.
                if (
                    _KPI_SOURCE_SHAPE_RE.fullmatch(raw_source) is not None
                    and not raw_source.startswith("statement.")
                ):
                    role = f"operating.{_canonical_series(raw_source)}"
                    unresolved.append(
                        BmAdmissionRefusal(
                            "business_model_node_unresolved",
                            f"node {node_key!r} declares valid kpi_source "
                            f"{raw_source!r}, but {role!r} has no statement "
                            "binding; repair through the correspondence door "
                            "without changing the node's kpi_source",
                            node_key=node_key,
                            observed=(
                                ("kpi_source", raw_source),
                                ("repair_owner", "binding"),
                                ("role", role),
                            ),
                        )
                    )
                    continue
                unresolved.append(
                    BmAdmissionRefusal(
                        "business_model_node_invalid",
                        f"node {node_key!r} declares kpi_source {raw_source!r}, "
                        "which names no pinned required series of this model "
                        "kind and no calculation of the node's own; repair: "
                        f"{_REPAIR_BMR}: name the series as operating.<slug> "
                        "or a statement role id, or drop the declaration "
                        "where the node is an assumption over evidence",
                        node_key=node_key,
                        observed=(("kpi_source", raw_source),),
                    )
                )
                continue
            register(segment, segment_id, node, node_key, series)

    claims: dict[str, BmSeriesClaim] = {}
    cyclic: set[str] = set()

    def claim_of(node_key: str, stack: tuple[str, ...] = ()) -> BmSeriesClaim:
        if node_key in claims:
            return claims[node_key]
        segment_id, node, series = registered[node_key]
        calculation: BmCalculation | None = None
        # A pinned series resolves by identity (`resolve_bm_node_history`): its
        # history is the disclosure, so a calculation the node also states is not
        # read, and cannot make a cycle.
        shape = None if series in required.identity_series else _calculation_shape(node)
        if shape is not None:
            kind, refs = shape
            operands: list[BmOperand] = []
            for ref in refs:
                if is_statement_role(ref.node_id):
                    operands.append(BmOperand(ref=ref.node_id))
                    continue
                operand_key = f"{segment_id}.{ref.node_id}"
                operand_node = all_nodes.get(operand_key)
                operand_claim: BmSeriesClaim | None = None
                if operand_key in registered:
                    if operand_key in (*stack, node_key):
                        if node_key not in cyclic:
                            cyclic.add(node_key)
                            refusals.append(
                                BmAdmissionRefusal(
                                    "business_model_node_invalid",
                                    f"node {node_key!r} calculates from "
                                    f"{operand_key!r}, which calculates from "
                                    f"it; a calculation cannot be cyclic; "
                                    f"repair: {_REPAIR_BMR}",
                                    node_key=node_key,
                                    observed=(("operand_node", operand_key),),
                                )
                            )
                    else:
                        operand_claim = claim_of(operand_key, (*stack, node_key))
                operands.append(
                    BmOperand(
                        ref=operand_key,
                        claim=operand_claim,
                        measure=None if operand_node is None else node_measure(operand_node),
                        average=operand_node is not None
                        and ratio_averages_operand(operand_node),
                    )
                )
            calculation = BmCalculation(kind, tuple(operands), node_measure(node))
        claims[node_key] = BmSeriesClaim(
            node_key=node_key,
            segment_id=segment_id,
            node_id=str(getattr(node, "id", "") or ""),
            series=series,
            calculation=calculation,
        )
        return claims[node_key]

    claimed = tuple((claim_of(key), registered[key][1]) for key in registered)
    return claimed, tuple(refusals), tuple(unresolved)


def _unresolved_refusal(
    unresolved: Sequence[BmAdmissionRefusal],
    series_vocabulary: frozenset[str],
) -> tuple[BmAdmissionRefusal, ...]:
    """The door's one refusal naming invalid claims, not unbound valid series."""

    unresolved = tuple(
        refusal for refusal in unresolved
        if refusal.code == "business_model_node_invalid"
    )
    if not unresolved:
        return ()
    named = "; ".join(
        f"{refusal.node_key!r} declares {dict(refusal.observed)['kpi_source']!r}"
        for refusal in unresolved
    )
    return (
        BmAdmissionRefusal(
            "business_model_node_invalid",
            f"kpi_source names no pinned required series of this model kind "
            f"and no calculation of the node's own: {named} (known series: "
            f"{', '.join(sorted(series_vocabulary))}); repair: {_REPAIR_BMR}",
        ),
    )


def derive_bm_series_claims(
    business_model: Any,
    *,
    required: BmRequiredSeries,
) -> tuple[tuple[BmSeriesClaim, ...], tuple[Finding, ...]]:
    """Every node's declared series claim the compile can lower, and one
    Finding per claim it cannot (malformed: ``business_model_node_invalid``;
    unbound operating series: ``business_model_node_unresolved``;
    a calculation whose declared unit is not what its operands yield:
    ``business_model_node_history_target_type_mismatch``). A refused claim is
    not returned; every other claim is."""

    claimed, refusals, unresolved = _claims(business_model, required)
    refused = (
        *refusals,
        *unresolved,
        *_formula_type_refusals(claimed, required),
    )
    refused_nodes = {refusal.node_key for refusal in refused}
    return (
        tuple(
            claim for claim, _node in claimed if claim.node_key not in refused_nodes
        ),
        tuple(refusal.finding() for refusal in refused),
    )


def _operand_measure_refusal(
    claim: BmSeriesClaim,
    formula: RegisteredBmFormula,
    series: str,
    measure: NodeMeasure,
    operand_key: str,
) -> BmAdmissionRefusal | None:
    expected = formula.operand_measures[series]
    if (
        (measure.unit, measure.model_scale) == (expected.unit, expected.model_scale)
        and (expected.basis is None or measure.basis == expected.basis)
    ):
        return None
    return BmAdmissionRefusal(
        "business_model_node_history_target_type_mismatch",
        f"node {claim.node_key!r} calculates {formula.formula_id!r}; operand "
        f"{series!r} ({operand_key}) declares "
        f"({measure.unit.value}, {measure.model_scale}, {measure.basis}), expected "
        f"({expected.unit.value}, {expected.model_scale}, {expected.basis}); "
        f"its history is not written; repair: {_REPAIR_BMR}",
        node_key=claim.node_key,
        observed=(
            ("formula", formula.formula_id),
            ("operand", series),
            ("operand_node", operand_key),
            ("operand_unit", measure.unit.value),
            ("operand_scale", measure.model_scale),
        ),
    )


def _formula_type_refusals(
    claimed: Sequence[tuple[BmSeriesClaim, Any]],
    required: BmRequiredSeries,
) -> tuple[BmAdmissionRefusal, ...]:
    """A node's declared unit must be what its own calculation yields.

    A ratio's dimension is its operands' quotient, a product's their product,
    and a sum's or remainder's is its operands' common one (`UNIT_DIMENSION`);
    a sum's operands share the node's scale, because the formula adds them
    unscaled (a product's scale is converted by the operand scales). Only node
    operands are checked here: a statement role's type is the pin's, checked
    where the formula is built (`_formula_from_calculation`).
    """

    refusals: list[BmAdmissionRefusal] = []
    for claim, _node in claimed:
        calculation = claim.calculation
        if calculation is None or claim.series in required.identity_series:
            continue
        output = calculation.measure
        node_operands = [
            (operand, operand.measure)
            for operand in calculation.operands
            if operand.measure is not None
        ]
        if calculation.kind == "ratio":
            if len(node_operands) != 2:
                continue
            (_n, numerator_measure), (_d, denominator_measure) = node_operands
            output_dimension = UNIT_DIMENSION.get(output.unit)
            numerator = UNIT_DIMENSION.get(numerator_measure.unit)
            denominator = UNIT_DIMENSION.get(denominator_measure.unit)
            if (
                numerator is None
                or denominator is None
                or output_dimension is None
                or quotient_dimension(numerator, denominator) == output_dimension
            ):
                continue
            offending = node_operands
        elif calculation.kind == "product":
            dimensions: list[Dimension | None] = [
                UNIT_DIMENSION.get(measure.unit) for _o, measure in node_operands
            ]
            known_dimensions: list[Dimension] = [d for d in dimensions if d is not None]
            output_dimension = UNIT_DIMENSION.get(output.unit)
            if (
                len(node_operands) != len(calculation.operands)
                or any(dimension is None for dimension in dimensions)
                or output_dimension is None
                or product_dimension(known_dimensions) == output_dimension
            ):
                continue
            offending = node_operands
        else:
            offending = [
                (operand, measure)
                for operand, measure in node_operands
                if not sum_operand_matches_output(measure, output)
            ]
            if not offending:
                continue
        operands = ", ".join(
            f"{operand.ref!r} ({measure.unit.value}, {measure.model_scale})"
            for operand, measure in offending
        )
        refusals.append(
            BmAdmissionRefusal(
                "business_model_node_history_target_type_mismatch",
                f"node {claim.node_key!r} calculates a {calculation.kind} over "
                f"{operands}, which does not yield its declared "
                f"({output.unit.value}, {output.model_scale}); repair: "
                f"{_REPAIR_BMR}",
                node_key=claim.node_key,
                observed=(
                    ("formula", claim.series),
                    ("node_unit", output.unit.value),
                    ("node_scale", output.model_scale),
                    ("operand_node", offending[0][0].ref),
                ),
            )
        )
    return tuple(refusals)


def bm_balance_refusals(
    business_model: Any,
    *,
    roles_with_history: Iterable[str],
) -> tuple[BmAdmissionRefusal, ...]:
    """A reported balance no node declares (`declared_balance_role`) has no driver.

    The balance-sheet schedule moves a reported balance only with the node
    that declares it (`build_company_grounded_schedules`
    `_apply_balance_sheet_policy`); nothing else projects it.
    """

    declared = {
        role
        for segment in tuple(getattr(business_model, "segments", None) or ())
        for node in _iter_nodes(
            tuple(
                getattr(getattr(segment, "revenue_model", None), "decomposition", None)
                or ()
            )
        )
        if (role := declared_balance_role(node)) is not None
    }
    return tuple(
        BmAdmissionRefusal(
            "balance_role_without_driver",
            f"the filings report {role!r} but no node declares balance_of "
            f"{role!r}, so its projected balance has no driver; repair: BMR "
            f"authoring lane (set balance_of {role!r} on the dollars driver that "
            "is that balance, observed at period_end or period_average)",
        )
        for role in sorted(set(roles_with_history) - declared)
    )


def admit_business_model(
    business_model: Any,
    *,
    required: BmRequiredSeries,
) -> tuple[BmAdmissionRefusal, ...]:
    """Every refusal the build decides from the business model and its pin.

    Empty when the build can compile the model's claims and balances. The
    business-model door calls it before acceptance, where the writer can
    resend. The compile reports the malformed-claim refusals as findings at
    the identity channel (`derive_bm_series_claims`), series coverage as
    findings (`bm_series_coverage_findings`), and the balance refusal as the
    blocked build's ``compile_failure`` where the balance-sheet schedule
    wires the balance. An unclaimed required series is a warning, never a
    refusal (R2). Facts the compiled model alone decides (evidence
    exactness, value conflicts, materialization) stay with the compile.
    """

    claimed, refusals, unresolved = _claims(business_model, required)
    return (
        *refusals,
        *_unresolved_refusal(unresolved, required.vocabulary),
        *_formula_type_refusals(claimed, required),
        *bm_balance_refusals(
            business_model, roles_with_history=required.roles_with_history
        ),
    )


def _pinned_identity_row(
    series: str, required_set: "RequiredSet"
) -> "AnnualSourceRequirement | None":
    """The one pinned requirement that carries ``series`` by identity."""

    rows = tuple(
        requirement
        for requirement in required_set.rows
        if _canonical_series(requirement.required_role) == _canonical_series(series)
        and _has_identity_series(requirement)
    )
    return rows[0] if len(rows) == 1 else None


def _formula_from_calculation(
    claim: BmSeriesClaim,
    calculation: BmCalculation,
    required_set: "RequiredSet",
) -> RegisteredBmFormula | str:
    """The formula a node's own calculation states, or the reason an operand
    does not resolve.

    An operand is a sibling node's claimed series (identity, or that sibling's
    own calculation, recursively) or a statement role the pin carries. The
    operand measures are the siblings' declared ones, or the pin's type for a
    statement role; the output type is the node's declared unit; an average
    denominator is a denominator node observed at ``period_average``.
    """

    series_names: list[str] = []
    measures: dict[str, NodeMeasure] = {}
    for operand in calculation.operands:
        measure: NodeMeasure | None
        if operand.claim is not None:
            inner = resolve_bm_node_history(operand.claim, required_set=required_set)
            if isinstance(inner, BmNodeBlocker):
                return (
                    f"operand node {operand.ref!r} resolves to no series: "
                    f"{inner.attempted}"
                )
            series = _canonical_series(operand.claim.series)
            measure = operand.measure
        elif is_statement_role(operand.ref):
            row = _pinned_identity_row(operand.ref, required_set)
            if row is None:
                return (
                    f"operand {operand.ref!r} is a statement role no single "
                    "pinned requirement carries"
                )
            source_type = _SOURCE_TYPES.get((row.unit, row.scale))
            if source_type is None:
                return (
                    f"operand {operand.ref!r} is pinned at ({row.unit}, "
                    f"{row.scale}), a type the history channel does not carry"
                )
            series = operand.ref
            measure = NodeMeasure(*source_type, basis=None)
        elif operand.measure is None:
            return f"operand node {operand.ref!r} is no node of the segment"
        else:
            return (
                f"operand node {operand.ref!r} claims no series: declare "
                "reported_kpi.source_concept on it, so its history is the "
                "series this calculation reads"
            )
        if measure is None:
            return f"operand node {operand.ref!r} has no declared measure"
        series_names.append(series)
        measures[series] = NodeMeasure(measure.unit, measure.model_scale, basis=None)
    if len(set(series_names)) != len(series_names):
        return "the calculation reads one series through two operands"
    expected_type = (calculation.measure.unit, calculation.measure.model_scale)
    formula_id = _canonical_series(claim.series)
    if calculation.kind == "ratio":
        numerator, denominator = series_names
        return RegisteredBmFormula(
            formula_id=formula_id,
            numerator_series=numerator,
            denominator_series=denominator,
            expected_type=expected_type,
            operand_measures=measures,
            expression=f"{numerator} / {denominator}",
            average_denominator=calculation.operands[1].average,
        )
    if calculation.kind == "product":
        dimensions: list[Dimension | None] = [
            UNIT_DIMENSION.get(measures[series].unit) for series in series_names
        ]
        known: list[Dimension] = [d for d in dimensions if d is not None]
        produced = product_dimension(known) if len(known) == len(dimensions) else None
        if produced is None or produced != UNIT_DIMENSION.get(expected_type[0]):
            return (
                f"a product over {', '.join(series_names)} does not yield the node's "
                f"declared ({expected_type[0].value}, {expected_type[1]})"
            )
        return RegisteredBmFormula(
            formula_id=formula_id,
            numerator_series=series_names[0],
            factor_series=tuple(series_names[1:]),
            expected_type=expected_type,
            operand_measures=measures,
            expression=" * ".join(series_names),
        )
    unscaled = [
        series
        for series, measure in measures.items()
        if not sum_operand_matches_output(measure, calculation.measure)
    ]
    if unscaled:
        return (
            f"sum operand {unscaled[0]!r} is not at the node's declared "
            f"({expected_type[0].value}, {expected_type[1]}); a sum's "
            "operands share its type"
        )
    if calculation.kind == "sum":
        return RegisteredBmFormula(
            formula_id=formula_id,
            numerator_series=series_names[0],
            addend_series=tuple(series_names[1:]),
            expected_type=expected_type,
            operand_measures=measures,
            expression=" + ".join(series_names),
        )
    return RegisteredBmFormula(
        formula_id=formula_id,
        numerator_series=series_names[0],
        subtract_series=tuple(series_names[1:]),
        expected_type=expected_type,
        operand_measures=measures,
        expression=" - ".join(series_names),
    )


def resolve_bm_node_history(
    claim: BmSeriesClaim,
    *,
    required_set: "RequiredSet",
) -> BmNodeIdentity | BmNodeFormula | BmNodeBlocker:
    """Identity | Formula | TypedBlocker for one claimed node (design §5.1).

    Identity when a pinned requirement carries the claimed series; else the
    node's own calculation (`BmSeriesClaim.calculation`) as a formula when its
    operands resolve; else a blocker naming the operand that did not.
    """

    # A BMR claim names its series in one of two exact spellings: the
    # BMR-local disclosed-series name (`<series>`), which the role
    # grammar prefixes into its BM family (`<family>.<series>`), or a
    # full catalog role id verbatim (`statement.is.revenue`) for a node that
    # IS a statement line. Both arms are exact-identity matches; there is no
    # fuzzy or leaf-only join across planes.
    claimed_roles = {claim.series} | {
        f"{family}.{claim.series}" for family in _BM_SERIES_FAMILIES
    }
    for requirement in required_set.rows:
        if (
            requirement.required_role in claimed_roles
            and _has_identity_series(requirement)
        ):
            return BmNodeIdentity(
                claim=claim,
                requirement_id=requirement.requirement_id,
                required_role=requirement.required_role,
            )
    if claim.calculation is None:
        return BmNodeBlocker(
            claim=claim,
            attempted=(
                f"series {claim.series!r} is neither a pinned required series "
                "of this model kind nor a calculation of the node's own over "
                "its sibling nodes (a ratio, a product, a sum, or a sum subtracting operands)"
            ),
        )
    formula = _formula_from_calculation(claim, claim.calculation, required_set)
    if isinstance(formula, str):
        return BmNodeBlocker(claim=claim, attempted=formula)
    return BmNodeFormula(claim=claim, formula=formula)


def _direct_segment_revenue_node_key(segment: Any) -> str | None:
    """Natural key for an exact one-node segment consolidation (dual write)."""

    formula = segment.revenue_model.consolidation_formula
    if formula.type != "sum" or len(formula.params.operands) != 1:
        return None
    operand = formula.params.operands[0]
    if operand.t != 0 or operand.sign != 1 or operand.node_id == "self":
        return None
    return f"{segment.id}.{operand.node_id}"


def direct_segment_face_items(
    business_model: Any, segment_mapping: Mapping[str, int]
) -> dict[str, str]:
    """Each exact one-node segment consolidation's node key -> its face income-statement line.

    The one place the two lines of a direct segment's node are named: the identity channel
    writes a claimed node's history to both (`_observation_targets`), and the historical writer
    reads this map so a role's declared absence reaches the face the same way."""

    return {
        node_key: f"tpl.fm.income_statement.business_segment_{segment_mapping[segment.id]}_revenue"
        for segment in business_model.segments
        if (node_key := _direct_segment_revenue_node_key(segment)) is not None
        and segment.id in segment_mapping
    }


def _historical_year(period_id: str, requirement_id: str) -> int:
    if (
        period_id.startswith("FY")
        and not period_id.endswith("E")
        and period_id[2:].isdigit()
    ):
        return int(period_id[2:])
    _fail(
        "business_model_node_history_period_invalid",
        f"{requirement_id!r} period {period_id!r} is not an actual fiscal year",
    )
    raise AssertionError("unreachable")


def _value_conflicts(existing: object, expected: Decimal) -> bool:
    if existing is None:
        return False
    try:
        return abs(Decimal(str(existing)) - expected) > Decimal("0.000001")
    except Exception:
        return True


def _unit_value(unit: Unit | None) -> str | None:
    return None if unit is None else unit.value


def _observation_targets(
    model: FinancialModel,
    *,
    node_key: str,
    natural_item_id: str,
    direct_segment_items: Mapping[str, str],
) -> tuple[tuple[str, Any], ...]:
    targets: list[tuple[str, Any]] = [
        (natural_item_id, model.get_item(natural_item_id))
    ]
    segment_item_id = direct_segment_items.get(node_key)
    if segment_item_id is not None:
        targets.append((segment_item_id, model.get_item(segment_item_id)))
    return tuple(targets)


_CONFIDENCE_RANK = {"low": 0, "medium": 1, "high": 2}


def _ratio_triples(business_model: Any) -> list[tuple[str, str, str]]:
    """Every (ratio, numerator, denominator) node-key triple the BusinessModel states.

    Two shapes state one: a ``derived`` node over a numerator and a denominator; a two-factor
    ``product`` node, whose unobserved factor is the product over the other factor (``Price =
    Revenue / Volume``).
    """

    triples: list[tuple[str, str, str]] = []
    for segment in business_model.segments:
        for node in _iter_nodes(segment.revenue_model.decomposition):
            key = f"{segment.id}.{node.id}"
            expression = node.driver
            if expression is None:
                continue
            if expression.type == "derived":
                params = expression.params
                refs = (params.numerator, params.denominator)
                if all(ref.t == 0 and ref.sign == 1 and ref.node_id != "self" for ref in refs):
                    triples.append((key, f"{segment.id}.{refs[0].node_id}", f"{segment.id}.{refs[1].node_id}"))
            elif expression.type == "product" and len(expression.params.operands) == 2:
                first, second = expression.params.operands
                if all(ref.t == 0 and ref.sign == 1 and ref.node_id != "self" for ref in (first, second)):
                    triples.append((f"{segment.id}.{first.node_id}", key, f"{segment.id}.{second.node_id}"))
                    triples.append((f"{segment.id}.{second.node_id}", key, f"{segment.id}.{first.node_id}"))
    return triples


def implied_ratio_seeds(
    revision: BusinessModelRevision,
    seeds: Sequence[HistoricalDriverSeed],
) -> tuple[HistoricalDriverSeed, ...]:
    """A ``derived`` ``ratio`` seed for every ratio node period its two inputs observe.

    A ratio node (``_ratio_triples``) with no seed of its own in period ``t`` whose numerator and
    denominator each carry an ``observed`` seed in ``t`` is implied: its value is the numerator over
    the denominator, the numerator's coefficient the unit conversion between the three measures,
    and the rationale ``implied: <numerator> / <denominator>``. The operands are the two observed
    seeds' own operands, so the implied value cites the same durable evidence; confidence is the
    lower of the two. A zero denominator implies nothing.
    """

    business_model = revision.business_model
    nodes = {
        f"{segment.id}.{node.id}": node
        for segment in business_model.segments
        for node in _iter_nodes(segment.revenue_model.decomposition)
    }
    entries = {
        f"{entry.segment_id}.{entry.driver_node_id}": entry
        for entry in revision.driver_assumption_plan.entries
    }
    observed = {
        (seed.driver_key, seed.period_id): seed
        for seed in seeds
        if seed.disposition == "observed" and seed.derivation_kind == "direct"
    }
    held = {(seed.driver_key, seed.period_id) for seed in seeds}
    implied: dict[tuple[str, str], HistoricalDriverSeed] = {}
    for ratio_key, numerator_key, denominator_key in _ratio_triples(business_model):
        ratio, entry = nodes.get(ratio_key), entries.get(ratio_key)
        if ratio is None or entry is None:
            continue
        measure = node_measure(ratio)
        for period_id in sorted({period for key, period in observed if key == numerator_key}):
            numerator = observed.get((numerator_key, period_id))
            denominator = observed.get((denominator_key, period_id))
            if (
                numerator is None
                or denominator is None
                or (ratio_key, period_id) in held
                or (ratio_key, period_id) in implied
                or denominator.value == 0
            ):
                continue
            exponent = (
                SCALE_EXPONENT[numerator.model_scale]
                - SCALE_EXPONENT[denominator.model_scale]
                - SCALE_EXPONENT[measure.model_scale]
            )
            conversion = None if exponent == 0 else Decimal(10) ** exponent
            numerator_operand = numerator.derivation_operands[0].model_copy(
                update={"coefficient": conversion}
            )
            operands = (numerator_operand, denominator.derivation_operands[0])
            implied[(ratio_key, period_id)] = HistoricalDriverSeed(
                driver_id=entry.driver_id,
                driver_key=ratio_key,
                period_id=period_id,
                value=numerator.value * (conversion or Decimal(1)) / denominator.value,
                unit=measure.unit,
                model_scale=measure.model_scale,
                disposition="derived",
                evidence_ids=tuple(sorted({o.evidence_id for o in operands if o.evidence_id})),
                evidence_value_ids=tuple(
                    sorted({o.evidence_value_id for o in operands if o.evidence_value_id})
                ),
                fact_ids=tuple(sorted({o.fact_id for o in operands if o.fact_id})),
                derivation_kind="ratio",
                derivation_operands=operands,
                derivation_rationale=f"implied: {numerator_key} / {denominator_key}",
                confidence=min(
                    numerator.confidence, denominator.confidence, key=_CONFIDENCE_RANK.__getitem__
                ),
            )
    return tuple(implied[key] for key in sorted(implied))


def _holds_value(item: Any, year: int) -> bool:
    values = getattr(getattr(item, "values", None), "values", None) or {}
    cell = values.get(year)
    return year in (item.overrides or {}) or (cell is not None and cell.value is not None)


def _ref_source_id(item: Any) -> str | None:
    spec = item.historical
    if spec is None or spec.type is not FormulaType.ref:
        return None
    return getattr(spec.params.get("source"), "id", None)


def release_unseeded_segment_revenue_cycles(
    model: FinancialModel,
    business_model: Any,
    segment_mapping: Mapping[str, int],
    years: Iterable[int],
) -> list[tuple[str, int]]:
    """A direct segment-revenue node with no value in a year owns no cell there.

    A segment whose consolidation is one node compiles to a three-row loop: the node's
    history reads the face income-statement line, the face line reads the segment's
    ``__rev`` row, and ``__rev`` sums the node. A year some row holds a value (a filed
    face override, a seed) is seeded and computes; a year none holds is the loop with
    nothing in it (``cycle_seed_missing``) rather than the absence the sources state.
    A segment introduced mid-history is that case for every year before it, so the year
    leaves the ``formula_periods`` of all three rows and the cell is an unowned
    ``source_value_unavailable``. Returns the (item id, year) pairs released.
    """

    if not model._index:
        model.build_index()
    released: list[tuple[str, int]] = []
    faces = direct_segment_face_items(business_model, segment_mapping)
    for segment in business_model.segments:
        node_key = _direct_segment_revenue_node_key(segment)
        face_id = None if node_key is None else faces.get(node_key)
        if node_key is None or face_id is None:
            continue
        revenue_id = f"bm.{segment.id}.__rev"
        node = model._index.get(f"bm.{node_key}")
        face = model._index.get(face_id)
        revenue = model._index.get(revenue_id)
        if node is None or face is None or revenue is None:
            continue
        if (_ref_source_id(node), _ref_source_id(face)) != (face_id, revenue_id):
            continue
        rows = (node, face, revenue)
        for year in sorted({int(year) for year in years}):
            if any(_holds_value(row, year) for row in rows):
                continue
            for row in rows:
                if row.formula_periods is not None and year in row.formula_periods:
                    row.formula_periods = [
                        int(period) for period in row.formula_periods if int(period) != year
                    ]
                    released.append((row.id, year))
    return released


def _apply_identity(
    model: FinancialModel,
    resolution: BmNodeIdentity,
    *,
    node: Any,
    requirement: "AnnualSourceRequirement",
    coverage: "AnnualRequirementCoverage | None",
    evidence_by_id: Mapping[str, "SourceEvidence"],
    historical_years: frozenset[int],
    natural_item_id: str,
    direct_segment_items: Mapping[str, str],
    without_mean: list[int],
    opening: Mapping[str, "OpeningBalance"],
    derived_strategies: Any = (),
) -> list[dict[str, Any]] | Finding:
    """The identity write: the retired executor's exactness checks, re-expressed
    at the pinned-series hop (single supported value, evidence tuple equality,
    type match, value-conflict fail-closed), plus the dual-target write.

    A node whose row is an average series (`row_holds_average_series`) is
    written as the mean of the opening and ending observations; a year the pin
    carries no opening observation for is appended to ``without_mean`` and
    written no value. When the pinned series itself already holds the period
    average (`pinned_series_holds_average`, owned by the profile's derived-KPI
    strategy), the pinned observation is the history as written: it is not
    averaged a second time, and no opening observation is required.

    A target line at another type than the pinned series is the node's
    ``business_model_node_history_target_type_mismatch`` Finding, returned
    before any write."""

    node_key = resolution.claim.node_key
    expected_type = _SOURCE_TYPES.get((requirement.unit, requirement.scale))
    if expected_type is None:
        _fail(
            "business_model_node_history_source_type_unsupported",
            f"requirement {requirement.requirement_id!r} has unsupported type "
            f"({requirement.unit!r}, {requirement.scale!r})",
        )
    targets = _observation_targets(
        model,
        node_key=node_key,
        natural_item_id=natural_item_id,
        direct_segment_items=direct_segment_items,
    )
    expected_unit, expected_scale = expected_type
    measure = node_measure(node)
    if (measure.unit, measure.model_scale) != expected_type:
        return make_finding(
            "business_model_node_history_target_type_mismatch",
            message=(
                f"operand node {node_key!r} claims pinned series "
                f"{resolution.required_role!r}, which is "
                f"({expected_unit.value}, {expected_scale}); the node declares "
                f"({measure.unit.value}, {measure.model_scale}), so its history "
                f"is not written; repair: {_REPAIR_BMR}"
            ),
            item_id=node_key,
            observed={
                "series": resolution.claim.series,
                "node_unit": measure.unit.value,
                "node_scale": measure.model_scale,
            },
        )
    for resolved_item_id, item in targets:
        if (item.unit, item.model_scale) != expected_type:
            item_unit = _unit_value(item.unit)
            return make_finding(
                "business_model_node_history_target_type_mismatch",
                message=(
                    f"node {node_key!r} claims pinned series "
                    f"{resolution.required_role!r}, which is "
                    f"({expected_unit.value}, {expected_scale}); its line "
                    f"{resolved_item_id!r} is ({item_unit}, {item.model_scale}), "
                    f"so its history is not written; repair: {_REPAIR_BMR}"
                ),
                item_id=node_key,
                observed={
                    "series": resolution.claim.series,
                    "requirement_id": requirement.requirement_id,
                    "resolved_item_id": resolved_item_id,
                    "item_unit": item_unit,
                    "item_scale": item.model_scale,
                },
            )
    outcomes_by_period = (
        {}
        if coverage is None
        else {row.period_id: row for row in coverage.periods}
    )
    result: list[dict[str, Any]] = []
    holds_average = row_holds_average_series(node)
    for period_id in requirement.period_ids:
        # A pinned series the pin already averages is taken as written; only
        # a pinned series of period-end observations is averaged here.
        averages = holds_average and not pinned_series_holds_average(
            resolution.required_role, derived_strategies, period_id=period_id
        )
        year = _historical_year(period_id, requirement.requirement_id)
        if year not in historical_years:
            # Requirement periods outside the compiled axis are not this
            # model's cells (the required set is window-scoped; the axis can
            # be narrower on partial-horizon compiles).
            continue
        period_outcome = outcomes_by_period.get(period_id)
        if period_outcome is None or period_outcome.disposition not in {
            "observed",
            "derived",
        }:
            # Any explicit non-supported disposition completes without a
            # value here: the typed SRC-11 gap dispositions genuinely carry
            # none, and the blocking dispositions (unknown_applicability /
            # unsupported) are already typed with repair targets and gate
            # the build through the bundle's own missing_source diagnostics
            # — the A-2 population rule (plan §10 item 12(b)), deferral to
            # bundle gating rather than a lost check (the retired executor
            # raised coverage_incomplete for them; plan §10 item 21 records
            # the channel difference). A missing typed answer stays with
            # the pin's fail-closed coverage machinery, not a silent zero.
            continue
        if len(period_outcome.evidence_ids) != 1:
            _fail(
                "business_model_node_history_coverage_incomplete",
                f"{requirement.requirement_id!r}/{period_id} lacks exactly one "
                f"supported value; repair: {_REPAIR_PIN}",
            )
        evidence_row = evidence_by_id.get(period_outcome.evidence_ids[0])
        pinned_line = requirement.cell_for_period(period_id).pinned_ref
        if evidence_row is None or (
            evidence_row.line,
            evidence_row.period_id,
            evidence_row.unit,
            evidence_row.scale,
            evidence_row.sign,
        ) != (
            pinned_line,
            period_id,
            requirement.unit,
            requirement.scale,
            requirement.sign,
        ):
            _fail(
                "business_model_node_history_evidence_mismatch",
                f"{requirement.requirement_id!r}/{period_id} evidence is not "
                f"exact; repair: {_REPAIR_PIN}",
            )
        observed = evidence_row.value
        lineage: dict[str, Any] = {}
        cited = [evidence_row]
        cited_opening: OpeningBalance | None = None
        if averages:
            # The row is the average series its declaration names: the mean
            # of the opening and ending period-end observations
            # (`row_holds_average_series`). The opening observation is the
            # pin's prior-year value on the axis, or the first year's opening
            # balance (`opening_balances`).
            prior_id = f"FY{year - 1}"
            prior: "SourceEvidence | None" = None
            if year - 1 in historical_years:
                if prior_id in requirement.period_ids:
                    prior = _exact_pinned_evidence(
                        requirement,
                        prior_id,
                        outcomes_by_period.get(prior_id),
                        evidence_by_id,
                    )
                if prior is not None:
                    cited.append(prior)
            else:
                cited_opening = opening.get(resolution.required_role)
                prior = None if cited_opening is None else cited_opening.evidence
            if prior is None:
                without_mean.append(year)
                continue
            observed = (prior.value + evidence_row.value) / Decimal("2")
            lineage = {"expr": period_mean(float(prior.value), float(evidence_row.value))}
        for resolved_item_id, item in targets:
            if item.values is None:
                item.values = ValueSeries()
            if item.overrides is None:
                item.overrides = {}
            existing = item.values.values.get(year)
            if existing is not None and _value_conflicts(
                existing.value,
                observed,
            ):
                _fail(
                    "business_model_node_history_value_conflict",
                    f"{node_key!r}/{period_id} conflicts with prior "
                    f"population of {resolved_item_id!r}",
                )
            note = f"{_IDENTITY_NOTE_PREFIX}:{evidence_row.evidence_id}"
            item.values.values[year] = ValueCell(
                period=year,
                value=float(observed),
                provenance=(
                    ValueProvenance.computed
                    if averages or evidence_row.derivation_input_ids
                    else ValueProvenance.imported_edgar
                ),
                note=note,
            )
            item.overrides[year] = FormulaSpec(
                type=FormulaType.constant,
                params={"value": float(observed), **lineage},
                note=note,
            )
            for row in cited:
                result.append(
                    {
                        "source_kind": IDENTITY_SOURCE_KIND,
                        "requirement_id": requirement.requirement_id,
                        "evidence_id": row.evidence_id,
                        "source_id": str(row.source_id),
                        "business_model_node_key": node_key,
                        "resolved_item_id": resolved_item_id,
                        "periods": [year],
                    }
                )
            if cited_opening is not None:
                result.append(
                    cited_opening.coverage(resolved_item_id=resolved_item_id, period=year)
                )
    return result


def _bound_years(
    item: Any, historical_years: frozenset[int], *, calculated: bool = False
) -> frozenset[int]:
    """The historical years a claimed line holds a value in.

    A line whose history `_apply_formula` installed as a formula (``calculated``)
    holds no stored value: the years it computes are its ``formula_periods``.
    """

    values = getattr(getattr(item, "values", None), "values", None) or {}
    years = {
        int(year)
        for year, cell in values.items()
        if int(year) in historical_years and cell.value is not None
    }
    if calculated and item.historical is not None:
        years |= {int(period) for period in item.formula_periods or ()} & historical_years
    return frozenset(years)


def _exact_pinned_evidence(
    requirement: "AnnualSourceRequirement",
    period_id: str,
    outcome: Any,
    evidence_by_id: Mapping[str, "SourceEvidence"],
) -> "SourceEvidence | None":
    """A pinned period's one supported value, or ``None`` when it has none.

    Cited inclusion has no numeric operand; unlike evidenced non-applicability,
    it never supplies absence-as-zero. Evidence that is not the period's pinned
    line at the requirement's type is a pin defect, never a missing value."""

    if (
        outcome is None
        or outcome.disposition not in {"observed", "derived"}
        or len(outcome.evidence_ids) != 1
    ):
        return None
    evidence = evidence_by_id.get(outcome.evidence_ids[0])
    pinned_line = requirement.cell_for_period(period_id).pinned_ref
    if evidence is None or (
        evidence.line,
        evidence.period_id,
        evidence.unit,
        evidence.scale,
        evidence.sign,
    ) != (
        pinned_line,
        period_id,
        requirement.unit,
        requirement.scale,
        requirement.sign,
    ):
        _fail(
            "business_model_node_history_evidence_mismatch",
            f"{requirement.requirement_id!r}/{period_id} formula input "
            f"is not exact; repair: {_REPAIR_PIN}",
        )
    return evidence


@dataclass(frozen=True)
class _PinnedRoleValues:
    """A pin-only formula input's numeric operand per historical year.

    ``evidenced_absences`` holds the years the pin carries as a cited
    ``not_applicable``: absent-with-citation, never a numeric operand. The
    citation stays on the record. A formula that has not declared the input
    optional — the catalog reading ``projection._formula_periods`` already
    applies; these formulas do not declare one — does not read the year.
    """

    values: dict[int, Decimal]
    evidenced_absences: dict[int, dict[str, Any]]
    measure: NodeMeasure


def _pinned_role_values(
    series: str,
    *,
    required_set: "RequiredSet",
    coverage_by_requirement: Mapping[str, "AnnualRequirementCoverage"],
    evidence_by_id: Mapping[str, "SourceEvidence"],
    historical_years: frozenset[int],
) -> _PinnedRoleValues | None:
    matches = tuple(
        requirement
        for requirement in required_set.rows
        if _canonical_series(requirement.required_role) == _canonical_series(series)
        and _has_identity_series(requirement)
    )
    if len(matches) != 1:
        return None
    requirement = matches[0]
    coverage = coverage_by_requirement.get(requirement.requirement_id)
    if coverage is None:
        return None
    outcomes = {row.period_id: row for row in coverage.periods}
    values: dict[int, Decimal] = {}
    evidenced_absences: dict[int, dict[str, Any]] = {}
    for period_id in requirement.period_ids:
        year = _historical_year(period_id, requirement.requirement_id)
        if year not in historical_years:
            continue
        outcome = outcomes.get(period_id)
        # Standing formula-coverage (a): a cited absence is not an operand.
        # The catalog reading is projection._formula_periods; this writer
        # does not add another. The citation stays on the record.
        if outcome is not None and outcome.disposition == "not_applicable_evidenced":
            evidenced_absences[year] = {
                "series": series,
                "requirement_id": requirement.requirement_id,
                "period": year,
                "reason": outcome.disposition,
                "rationale": outcome.rationale,
                "disclosure_search_accession": outcome.disclosure_search_accession,
                "disclosure_search_basis": outcome.disclosure_search_basis,
                "applicability_evidence_ids": list(outcome.applicability_evidence_ids),
            }
            continue
        evidence = _exact_pinned_evidence(
            requirement, period_id, outcome, evidence_by_id
        )
        if evidence is not None:
            values[year] = evidence.value
    if not values and not evidenced_absences:
        return None
    unit, scale = _SOURCE_TYPES[(requirement.unit, requirement.scale)]
    return _PinnedRoleValues(
        values=values,
        evidenced_absences=evidenced_absences,
        measure=NodeMeasure(unit, scale, basis=None),
    )


@dataclass(frozen=True)
class OpeningBalance:
    """A pinned role's value at the year before the first historical year.

    The pin's opening column
    (``build_model_source_bundle._opening_balance_observations``): the first
    year's opening balance, read where a formula needs the prior period the
    axis does not carry."""

    requirement_id: str
    evidence: "SourceEvidence"

    def coverage(self, *, resolved_item_id: str, period: int) -> dict[str, Any]:
        """The seed-coverage row naming this evidence as ``period``'s opening."""

        return {
            "source_kind": OPENING_BALANCE_SOURCE_KIND,
            "requirement_id": self.requirement_id,
            "evidence_id": self.evidence.evidence_id,
            "source_id": str(self.evidence.source_id),
            "resolved_item_id": resolved_item_id,
            "periods": [period],
        }


def opening_balances(
    source_bundle: "SourceBundle", *, first_historical_year: int
) -> dict[str, OpeningBalance]:
    """Every pinned role's opening balance for ``first_historical_year``, by role."""

    period_id = f"FY{first_historical_year - 1}"
    coverage = {row.requirement_id: row for row in source_bundle.coverage}
    evidence_by_id = {row.evidence_id: row for row in source_bundle.evidence}
    result: dict[str, OpeningBalance] = {}
    for requirement in source_bundle.requirements:
        requirement_coverage = coverage.get(requirement.requirement_id)
        if (
            period_id not in requirement.period_ids
            or requirement.cell_for_period(period_id).binding_state != "bound"
            or requirement_coverage is None
        ):
            continue
        evidence = _exact_pinned_evidence(
            requirement,
            period_id,
            next(
                (row for row in requirement_coverage.periods if row.period_id == period_id),
                None,
            ),
            evidence_by_id,
        )
        if evidence is not None:
            result[requirement.required_role] = OpeningBalance(
                requirement_id=requirement.requirement_id, evidence=evidence
            )
    return result


def _statement_series_item(
    model: FinancialModel,
    *,
    series: str,
    required_set: "RequiredSet",
    ticker: str,
) -> Any | None:
    """A pinned statement or operating series' bound workbook line, matched by
    `_canonical_series` so a flat and an ``operating.`` spelling are one role."""

    from .build_model_items import _iter_items
    from .required_role_catalog import load_required_role_catalog

    wanted = _canonical_series(series)
    for requirement in required_set.rows_for_family("operating"):
        if _canonical_series(requirement.required_role) != wanted:
            continue
        if requirement.model_item in model._index:
            return model.get_item(requirement.model_item)
        for item in _iter_items(model):
            if item.required_role == requirement.required_role:
                return item

    catalog = load_required_role_catalog()
    for requirement in required_set.statement_rows():
        if _canonical_series(requirement.required_role) != wanted:
            continue
        for line_id in catalog.population_item_ids(
            requirement.required_role, ticker=ticker
        ):
            if line_id in model._index:
                return model.get_item(line_id)
        for item in _iter_items(model):
            if item.required_role == requirement.required_role:
                return item
    return None


def _apply_formula(
    model: FinancialModel,
    resolution: BmNodeFormula,
    *,
    identity_items: Mapping[str, str],
    required_set: "RequiredSet",
    coverage_by_requirement: Mapping[str, "AnnualRequirementCoverage"],
    evidence_by_id: Mapping[str, "SourceEvidence"],
    historical_years: frozenset[int],
    opening: Mapping[str, OpeningBalance],
    natural_item_id: str,
    ticker: str,
    formula_items: frozenset[str] = frozenset(),
) -> list[dict[str, Any]] | Finding:
    """Install one node's calculation as its history over displayed or
    pin-only input lines.

    A target line at another type than the formula writes, or an input series
    neither the model nor the pin carries, is the node's Finding, returned
    before any write. A pinned input the pin carries as an evidenced absence
    is not an operand (`_pinned_role_values`): the citation stays on
    ``evidenced_absences``, and a year that needs that input is not bound.
    ``identity_items`` is keyed by `_canonical_series`: a role claimed as
    ``a`` or ``operating.a`` is one key, however the formula spells its
    input. It holds the lines of every claimed node already applied, a
    calculated one (``formula_items``) included.
    """

    formula = resolution.formula
    node_key = resolution.claim.node_key
    item = model.get_item(natural_item_id)
    if (item.unit, item.model_scale) != formula.expected_type:
        expected_unit, expected_scale = formula.expected_type
        item_unit = _unit_value(item.unit)
        return make_finding(
            "business_model_node_history_target_type_mismatch",
            message=(
                f"node {node_key!r} calculates "
                f"{formula.formula_id!r}, which writes ({expected_unit.value}, "
                f"{expected_scale}); its line {natural_item_id!r} is "
                f"({item_unit}, {item.model_scale}), so its history is not "
                f"written; repair: {_REPAIR_BMR}"
            ),
            item_id=node_key,
            observed={
                "formula": formula.formula_id,
                "resolved_item_id": natural_item_id,
                "item_unit": item_unit,
                "item_scale": item.model_scale,
            },
        )

    input_items: dict[str, Any] = {}
    pinned_values: dict[str, _PinnedRoleValues] = {}
    #: A node operand whose pinned series is a cited absence: not an operand
    #: (`_pinned_role_values`), and no gap either.
    operand_absences: dict[str, dict[int, dict[str, Any]]] = {}
    input_measures: dict[str, NodeMeasure] = {}
    for series in formula.input_series():
        identity_item_id = identity_items.get(_canonical_series(series))
        input_item = (
            model.get_item(identity_item_id)
            if identity_item_id is not None
            else _statement_series_item(
                model,
                series=series,
                required_set=required_set,
                ticker=ticker,
            )
        )
        if input_item is not None:
            input_unit = input_item.unit
            if input_unit is None:
                return make_finding(
                    "business_model_node_history_target_type_mismatch",
                    message=(
                        f"node {node_key!r} calculates "
                        f"{formula.formula_id!r}; operand {series!r} "
                        f"({input_item.id}) has no declared unit, so its history "
                        f"is not written; repair: {_REPAIR_BMR}"
                    ),
                    item_id=node_key,
                    observed={
                        "formula": formula.formula_id,
                        "operand": series,
                        "operand_node": input_item.id,
                        "operand_unit": None,
                        "operand_scale": input_item.model_scale,
                    },
                )
            measure = NodeMeasure(input_unit, input_item.model_scale, basis=None)
            refusal = _operand_measure_refusal(
                resolution.claim, formula, series, measure, input_item.id
            )
            if refusal is not None:
                return refusal.finding()
            input_measures[series] = measure
            input_items[series] = input_item
            cited = _pinned_role_values(
                series,
                required_set=required_set,
                coverage_by_requirement=coverage_by_requirement,
                evidence_by_id=evidence_by_id,
                historical_years=historical_years,
            )
            if cited is not None and cited.evidenced_absences:
                operand_absences[series] = cited.evidenced_absences
            continue
        pinned = _pinned_role_values(
            series,
            required_set=required_set,
            coverage_by_requirement=coverage_by_requirement,
            evidence_by_id=evidence_by_id,
            historical_years=historical_years,
        )
        if pinned is None:
            return make_finding(
                "formula_input_missing",
                message=(
                    f"the calculation of node {node_key!r} "
                    f"({formula.formula_id!r}) misses input series {series!r}: "
                    "no applied node claim and no single pinned requirement "
                    "carries it, so the node's history is not written; repair: "
                    "claim the series on the node that is it (BMR authoring "
                    "lane), or bind it (statement-binding lane)"
                ),
                item_id=node_key,
                observed={"formula": formula.formula_id, "series": series},
            )
        pinned_values[series] = pinned
        refusal = _operand_measure_refusal(
            resolution.claim, formula, series, pinned.measure, series
        )
        if refusal is not None:
            return refusal.finding()
        input_measures[series] = pinned.measure

    if pinned_values and any(
        input_item.id in formula_items for input_item in input_items.values()
    ):
        # Stored constants need numeric operands; a calculated operand line
        # holds a formula, not a value, so this node cannot write them.
        calculated = sorted(
            series for series, line in input_items.items() if line.id in formula_items
        )
        return make_finding(
            "business_model_node_unresolved",
            message=(
                f"node {node_key!r} calculates {formula.formula_id!r} over a "
                f"calculated sibling ({', '.join(calculated)}) and a pinned "
                "statement series, which the history channel cannot compute "
                "together; repair: express the pinned operand through a node "
                "that claims it (BMR authoring lane)"
            ),
            item_id=node_key,
            observed={"formula": formula.formula_id, "calculated": ", ".join(calculated)},
        )

    bound_year_sets = {
        series: (
            _bound_years(
                input_items[series],
                historical_years,
                calculated=input_items[series].id in formula_items,
            )
            if series in input_items
            else frozenset(pinned_values[series].values)
        )
        for series in formula.input_series()
    }
    # The first year's opening balance is off the axis; the pin's opening
    # column carries it (`opening_balances`), so the first year averages too.
    first_year = min(historical_years)
    opening_balance = (
        next(
            (
                balance
                for role, balance in opening.items()
                if _canonical_series(role)
                == _canonical_series(formula.denominator_series)
            ),
            None,
        )
        if formula.average_denominator and formula.denominator_series is not None
        else None
    )
    if formula.average_denominator:
        assert formula.denominator_series is not None
        denominator_years = bound_year_sets[formula.denominator_series]
        bound_year_sets[formula.denominator_series] = frozenset(
            year
            for year in denominator_years
            if (year - 1) in denominator_years
            or (year == first_year and opening_balance is not None)
        )
    bound_years = sorted(
        set.intersection(*(set(years) for years in bound_year_sets.values()))
    )
    if not bound_years:
        if operand_absences or any(
            pinned.evidenced_absences for pinned in pinned_values.values()
        ):
            # A cited absence is not an operand: the node's history holds no
            # value by the citation, which is the record, not a gap.
            return []
        unbound = sorted(series for series, years in bound_year_sets.items() if not years)
        return make_finding(
            "business_model_node_unresolved",
            message=(
                f"node {node_key!r} calculates "
                f"{formula.formula_id!r}, but no historical year has every "
                f"operand ({', '.join(unbound) or 'the operands share no year'}), "
                "so its history is not written; repair: bind the operand's "
                "filed line to its role (correspondence door)"
            ),
            item_id=node_key,
            observed={
                "formula": formula.formula_id,
                "unbound_series": ", ".join(unbound),
            },
        )
    opens_first_year = opening_balance is not None and first_year in bound_years

    def _operand(series: str, year: int, *, prior: bool = False) -> Any:
        if prior and year == first_year:
            assert opening_balance is not None
            return float(opening_balance.evidence.value)
        input_item = input_items.get(series)
        if input_item is not None:
            return {"id": input_item.id, "t": -1 if prior else 0}
        value_year = year - 1 if prior else year
        return float(pinned_values[series].values[value_year])

    def _spec(year: int) -> FormulaSpec:
        return formula.historical_spec(
            input_measures=input_measures,
            input_operands={
                series: _operand(series, year)
                for series in formula.input_series()
            },
            prior_input_operands={
                series: _operand(series, year, prior=True)
                for series in formula.input_series()
                if formula.average_denominator
                and series == formula.denominator_series
            },
        )

    undefined_years: list[int] = []
    if pinned_values:
        item.historical = None
        if item.values is None:
            item.values = ValueSeries()
        if item.overrides is None:
            item.overrides = {}

        def _input_value(series: str, year: int, *, prior: bool = False) -> Decimal:
            if prior and year == first_year:
                assert opening_balance is not None
                return opening_balance.evidence.value
            value_year = year - 1 if prior else year
            input_item = input_items.get(series)
            if input_item is None:
                return pinned_values[series].values[value_year]
            cell = input_item.values.values[value_year]
            return Decimal(str(cell.value))

        def _evidenced_absences(year: int) -> list[dict[str, Any]]:
            """Cited absences recorded for ``year``.

            An evidenced absence is not in ``values``, so a year that needs it
            is not bound and this does not read one as an operand. The record
            is the citation, for a year that was computed beside one.
            """

            read: list[dict[str, Any]] = []
            for series, pinned in pinned_values.items():
                read_years = [year]
                if (
                    formula.average_denominator
                    and series == formula.denominator_series
                    and year != first_year
                ):
                    read_years.append(year - 1)
                read.extend(
                    pinned.evidenced_absences[read_year]
                    for read_year in read_years
                    if read_year in pinned.evidenced_absences
                )
            return read

        for year in bound_years:
            value = formula.computed_value(
                input_measures=input_measures,
                input_values={
                    series: _input_value(series, year)
                    for series in formula.input_series()
                },
                prior_input_values={
                    series: _input_value(series, year, prior=True)
                    for series in formula.input_series()
                    if formula.average_denominator
                    and series == formula.denominator_series
                },
            )
            if value is None:
                # A genuine zero denominator: the ratio is undefined that year.
                undefined_years.append(year)
                continue
            note = f"{_FORMULA_NOTE_PREFIX}:{formula.formula_id}"
            item.values.values[year] = ValueCell(
                period=year,
                value=float(value),
                provenance=ValueProvenance.computed,
                note=note,
            )
            # Keep the unrounded input expression as lineage; the constant
            # remains the authority for the source-disclosed precision.
            input_spec = _spec(year)
            item.overrides[year] = FormulaSpec(
                type=FormulaType.constant,
                params={
                    "value": float(value),
                    **input_spec.params,
                    **(
                        {"evidenced_absences": absences}
                        if (absences := _evidenced_absences(year))
                        else {}
                    ),
                },
                note=note,
            )
    else:
        # Every later year reads model lines only; the first year's opening
        # operand is a pinned literal, so that year overrides the formula.
        item.historical = _spec(max(bound_years))
        if opens_first_year and max(bound_years) != first_year:
            if item.overrides is None:
                item.overrides = {}
            item.overrides[first_year] = _spec(first_year)
    if undefined_years:
        bound_years = sorted(set(bound_years) - set(undefined_years))
        if first_year in undefined_years:
            opens_first_year = False
    existing_periods = {int(period) for period in (item.formula_periods or ())}
    if item.formula_periods is None:
        existing_periods.update(
            int(period)
            for period in (
                getattr(getattr(model, "time_structure", None), "projection_periods", ())
                or ()
            )
        )
    non_historical_periods = existing_periods - historical_years
    item.formula_periods = sorted(non_historical_periods | set(bound_years))
    if not bound_years:
        # Every bound year's denominator is an evidenced absence: the node's history
        # holds no value and covers no period, so there is no coverage row to record.
        return []
    return [
        {
            "source_kind": FORMULA_SOURCE_KIND,
            "formula_id": formula.formula_id,
            "business_model_node_key": node_key,
            "resolved_item_id": natural_item_id,
            "input_item_ids": sorted(
                {input_item.id for input_item in input_items.values()}
            ),
            "periods": [int(year) for year in bound_years],
        },
        *(
            (
                opening_balance.coverage(
                    resolved_item_id=natural_item_id, period=first_year
                ),
            )
            if opening_balance is not None and opens_first_year
            else ()
        ),
    ]


def _formula_line_refs(params: Any) -> tuple[LineItemRef, ...]:
    """LineItemRef operands nested in a formula's params."""

    found: list[LineItemRef] = []

    def walk(value: Any) -> None:
        if isinstance(value, LineItemRef):
            found.append(value)
            return
        if isinstance(value, dict):
            item_id = value.get("id")
            if isinstance(item_id, str) and set(value).issubset(
                {"id", "t", "resolved", "period_anchor"}
            ):
                try:
                    found.append(LineItemRef(id=item_id, t=int(value.get("t") or 0)))
                except (TypeError, ValueError):
                    return
                return
            for nested in value.values():
                walk(nested)
            return
        if isinstance(value, (list, tuple)):
            for nested in value:
                walk(nested)

    walk(params)
    return tuple(found)


def _historical_support(
    model: FinancialModel,
    historical_years: frozenset[int],
    bound_item_ids: set[str],
) -> Callable[[str, int], bool]:
    """``supported(item_id, year)``: whether a line can stand as a historical operand in ``year``.

    A bound line supports the historical years its formula covers. Any line also
    supports a year it overrides or stores a value for. A derived line (a
    non-constant historical formula) supports a year it owns where every operand
    it reads is supported there, so a segment's ``__rev`` over claimed rows
    supports the years those rows do. A cycle supports nothing.
    """

    memo: dict[tuple[str, int], bool] = {}

    def supports(item: LineItem, year: int) -> bool:
        if year in {int(period) for period in (item.overrides or {})}:
            return True
        if item.values is not None:
            cell = item.values.values.get(year)
            if cell is not None and cell.value is not None:
                return True
        if item.id in bound_item_ids:
            return year in historical_years and year in {
                int(period) for period in (item.formula_periods or historical_years)
            }
        historical = item.historical
        if historical is None or historical.type is FormulaType.constant:
            return False
        if item.formula_periods is not None and year not in {
            int(period) for period in item.formula_periods
        }:
            return False
        refs = [ref for ref in _formula_line_refs(historical.params) if ref.id != item.id]
        return bool(refs) and all(
            supported(ref.id, year + int(ref.t or 0)) for ref in refs
        )

    def supported(item_id: str, year: int) -> bool:
        key = (item_id, year)
        if key not in memo:
            memo[key] = False
            item = model._index.get(item_id)
            memo[key] = item is not None and supports(item, year)
        return memo[key]

    return supported


def _rate_spec(item: LineItem) -> FormulaSpec | None:
    """The ratio spec that makes ``item`` a rate/ratio row, else ``None``.

    The compiler writes every y/y rate and derived ratio as ``FormulaType.ratio``;
    a product, sum, roll-forward or growth-driver row never is one. This is the
    one classification of the rows a period restriction may touch.
    """

    for spec in (item.projected, item.historical):
        if spec is not None and spec.type is FormulaType.ratio:
            return spec
    return None


def _lagged_denominator(spec: FormulaSpec) -> LineItemRef | None:
    denominator = spec.params.get("denominator")
    if isinstance(denominator, LineItemRef) and int(denominator.t or 0) < 0:
        return denominator
    return None


def _restrict_unbound_business_model_formula_periods(
    model: FinancialModel,
    historical_years: frozenset[int],
    bound_item_ids: set[str],
) -> None:
    """Historical formula periods are the years a rate row can actually read.

    ``_apply_formula`` already writes that for the lines it binds. A ``bm.*``
    rate/ratio row history did not bind — a y/y rate over a volume row, for
    example — keeps a historical year when every operand is supported that year
    (the rate's prior-year denominator included), or when the year itself carries
    an override or value. Years with neither are not required values. Derived
    product/sum/roll-forward rows are never restricted: their operands are
    inputs the agent supplies after this runs.
    """

    if not model._index:
        model.build_index()
    supported = _historical_support(model, historical_years, bound_item_ids)
    for item in model._index.values():
        if (
            not str(item.id).startswith("bm.")
            or item.id in bound_item_ids
            or item.formula_periods is None
            or _rate_spec(item) is None
        ):
            continue
        observed = {int(period) for period in (item.overrides or {})}
        if item.values is not None:
            observed.update(
                int(cell.period)
                for cell in item.values.values.values()
                if cell.value is not None
            )
        item.formula_periods = sorted(
            int(period)
            for period in item.formula_periods
            if int(period) not in historical_years
            or int(period) in observed
            or supported(item.id, int(period))
        )


def restrict_uncomputable_business_model_periods(
    model: FinancialModel, values: Mapping[tuple[str, int], Any],
) -> None:
    """A ``bm.*`` rate/ratio period that cannot compute is not a required cell.

    Two periods qualify: a historical year the row has no value for, and
    ``projection_periods[0]`` when the ratio's lagged (``t=-1``) denominator has
    no value — a projection growth rate with no prior revenue. An override is an
    authored value and stays. A product/sum/roll-forward row (``bm.<seg>.__rev``
    included) is never restricted by value — its inputs arrive after this runs — but
    every ``bm.*`` row, and every input row reading one, then drops the periods that read a cell no row owns
    (``_restrict_unowned_operand_periods``).
    """

    if not model._index:
        model.build_index()
    time_structure = model.time_structure
    historical_years = {int(period) for period in time_structure.historical_periods}
    projection_periods = time_structure.projection_periods
    first_projection = int(projection_periods[0]) if projection_periods else None

    def known(item_id: str, period: int) -> bool:
        return getattr(values.get((item_id, period)), "state", None) == "known"

    for item in model._index.values():
        if not str(item.id).startswith("bm.") or not item.formula_periods:
            continue
        spec = _rate_spec(item)
        if spec is None:
            continue
        lagged = _lagged_denominator(spec)
        authored = {int(period) for period in (item.overrides or {})}

        def computable(period: int) -> bool:
            if period in authored:
                return True
            if period in historical_years:
                return known(item.id, period)
            if period == first_projection and lagged is not None:
                return known(lagged.id, period + int(lagged.t or 0))
            return True

        item.formula_periods = sorted(
            int(period) for period in item.formula_periods if computable(int(period))
        )

    _restrict_unowned_operand_periods(model)


def _restrict_unowned_operand_periods(model: FinancialModel) -> None:
    """A ``bm.*`` row, an input row that reads one, and a row whose history is a ref to one (a segment's
    income-statement face) own a period only where every cell they read is owned.

    ``DependencyGraph.cell_operands`` is the read set of the spec the period selects (an
    override reads nothing, so an authored cell stays) and ``item_owns_cell`` the one
    ownership rule. A derived row over a growth-driver row that history never bound would
    otherwise keep its historical years while its operand owns none; so would a scenario
    control's history ref to a segment growth row at the first year, which has no prior year.
    A sum owns a year only when every term does, and a y/y rate only when its base owns that
    year and the one before. A projection cell is never dropped for reading a historical cell
    no row owns: the forecast of a row is not the history's to remove, and the cell that
    reads the absent year is the compiled structure's own finding. Dropping a period
    can strand that row's own readers, so it runs to a fixed point.
    """

    graph = DependencyGraph()
    graph.build(model)
    bm_ids = {item.id for item in model._index.values() if str(item.id).startswith("bm.")}

    def reads_bm_row(item: LineItem) -> bool:
        specs = (item.historical, item.projected, *(item.overrides or {}).values())
        return any(
            ref.id in bm_ids
            for spec in specs
            if spec is not None
            for ref in _formula_line_refs(spec.params)
        )

    def mirrors_bm_row(item: LineItem) -> bool:
        # A presentation row whose history is a ref to a bm row: a segment's income-statement face.
        spec = item.historical
        return (
            spec is not None
            and spec.type is FormulaType.ref
            and getattr(spec.params.get("source"), "id", None) in bm_ids
        )

    rows = [
        item for item in model._index.values()
        if item.formula_periods and (
            item.id in bm_ids
            or (item.item_type is ItemType.input and reads_bm_row(item))
            or mirrors_bm_row(item)
        )
        and not any(
            spec is not None and spec.type is FormulaType.roll_forward
            for spec in (item.projected, item.historical)
        )
    ]
    historical_years = {int(period) for period in model.time_structure.historical_periods}

    def reads_unowned_cell(item: LineItem, period: int) -> bool:
        return any(
            not item_owns_cell(model, model._index[target_id], target_period)
            and (period in historical_years or target_period not in historical_years)
            for target_id, target_period in graph.cell_operands(item, period)
        )

    def restricted(item: LineItem, period: int) -> bool:
        # An input reader's history only: its forecast cells are authored, never read-owned.
        return (item.id in bm_ids or period in historical_years) and reads_unowned_cell(item, period)

    changed = True
    while changed:
        changed = False
        for item in rows:
            owned = [
                int(period) for period in item.formula_periods or ()
                if not restricted(item, int(period))
            ]
            if len(owned) != len(item.formula_periods or ()):
                item.formula_periods = owned
                changed = True


def _retire_undemanded_history(
    model: FinancialModel, cells: Sequence[tuple[str, int]]
) -> None:
    """A claimed row owns no cell in a historical year its requirement does not demand.

    ``cells`` are ``(item_id, year)`` of claimed identity rows, any statement or ``operating.*``
    role: the requirement carries every historical year, and a year its window declares
    ``not_disclosed_in_vintage`` is typed coverage (``derive_coverage_profile``), so that year is
    an absence of the series, not a cell of
    the row. This is period ownership, never a rewrite of a formula: only ``formula_periods``
    change, only in historical years, and a cell something claims (an override or a stored
    value) stays. A row owns a historical year only when every cell it reads there is owned, so
    the absence follows to its readers: a segment's ``__rev`` owns the year only when every term
    does (never a partial sum), its y/y rate when the base owns that year and the one before,
    the segment's face rows and a scenario control's history ref with them. A forecast period
    is never touched; the cell that reads an absent year is the compiled structure's own finding.
    """

    if not cells:
        return
    historical_years = {int(period) for period in model.time_structure.historical_periods}
    axis = [
        *(int(period) for period in model.time_structure.historical_periods),
        *(int(period) for period in model.time_structure.projection_periods),
    ]

    def claimed(item: LineItem, year: int) -> bool:
        if year in {int(period) for period in (item.overrides or {})}:
            return True
        cell = None if item.values is None else item.values.values.get(year)
        return cell is not None and cell.value is not None

    def disown(item: LineItem, year: int) -> None:
        item.formula_periods = [
            int(period)
            for period in (axis if item.formula_periods is None else item.formula_periods)
            if int(period) != year
        ]

    absent: set[tuple[str, int]] = set()
    for item_id, year in cells:
        item = model._index[item_id]
        if not claimed(item, year):
            disown(item, year)
            absent.add((item_id, year))
    graph = DependencyGraph()
    graph.build(model)
    read_ids = {
        item.id: {
            ref.id
            for spec in (item.historical, item.projected)
            if spec is not None
            for ref in _formula_line_refs(spec.params)
        }
        for item in model._index.values()
    }
    changed = bool(absent)
    while changed:
        changed = False
        absent_ids = {item_id for item_id, _ in absent}
        for item in model._index.values():
            if not read_ids[item.id] & absent_ids:
                continue
            for year in sorted(historical_years):
                if (item.id, year) in absent or claimed(item, year):
                    continue
                if absent & graph.cell_operands(item, year):
                    disown(item, year)
                    absent.add((item.id, year))
                    changed = True


def _calculation_depth(claim: BmSeriesClaim) -> int:
    """How many calculations deep a claim reads: 0 for a claim with none."""

    if claim.calculation is None:
        return 0
    return 1 + max(
        (
            _calculation_depth(operand.claim)
            for operand in claim.calculation.operands
            if operand.claim is not None
        ),
        default=0,
    )



def _locate_unresolved_nodes(
    findings: Sequence[Finding], compiled_registry: "CompiledDriverRegistry"
) -> tuple[tuple[Finding, ...], tuple[str, ...]]:
    """Each ``business_model_node_unresolved`` finding names its node's compiled
    line in ``observed["compiled_item_id"]``, and those lines are returned.

    The node's history is not written and that finding is the one record of it:
    the line's cells carry its reason (`unresolved_items`) and the cascade rooted
    at them folds onto it (`build_findings.fold_semantic_value_findings`, which
    reads only findings, so the line rides the finding). The registry is the one
    place that knows a node key's line, so the id is attached here."""

    located: list[Finding] = []
    items: list[str] = []
    for finding in findings:
        item_id = (
            compiled_registry.node_items.get(finding.item_id)
            if finding.code == "business_model_node_unresolved" and finding.item_id is not None
            else None
        )
        if item_id is None:
            located.append(finding)
            continue
        located.append(
            finding.model_copy(
                update={"observed": {**finding.observed, "compiled_item_id": item_id}}
            )
        )
        items.append(item_id)
    return tuple(located), tuple(dict.fromkeys(items))


def apply_business_model_node_history(
    model: FinancialModel,
    source_bundle: "SourceBundle",
    *,
    business_model_revision: BusinessModelRevision,
    compiled_registry: "CompiledDriverRegistry",
    required_set: "RequiredSet",
    opening_balances: Mapping[str, OpeningBalance] = MappingProxyType({}),
) -> BmNodeHistoryResult:
    """Populate BM-node history through the identity channel (plan §4.3).

    Runs at the point the retired executor occupied. Returns the
    identity/formula seed-coverage rows and one Finding per claim the channel
    cannot apply (`derive_bm_series_claims`; a claim on a node with no
    compiled line; a target at another type; a formula input absent): that
    claim is not applied, every other claim is. Raises typed
    `BmNodeHistoryCompileError` on every pinned-evidence exactness failure.
    ``opening_balances`` (`opening_balances`) opens a first-year average
    denominator; empty when the pin carries none.
    """

    revision = (
        business_model_revision
        if isinstance(business_model_revision, BusinessModelRevision)
        else BusinessModelRevision.model_validate(business_model_revision)
    )
    if compiled_registry.validation_model is not model:
        _fail(
            "business_model_node_history_registry_mismatch",
            "compiled registry does not belong to the supplied model",
        )
    business_model = revision.business_model
    nodes = {
        f"{segment.id}.{node.id}": node
        for segment in business_model.segments
        for node in _iter_nodes(segment.revenue_model.decomposition)
    }

    # The malformed-claim arms of the one admission predicate the
    # business-model door also runs; series coverage is the compile's
    # findings (`bm_series_coverage_findings`), the balance arm the
    # balance-sheet schedule's (`admit_business_model`).
    claims, claim_findings = derive_bm_series_claims(
        business_model, required=bm_required_series(required_set.rows)
    )
    findings: list[Finding] = list(claim_findings)
    if not claims:
        _restrict_unbound_business_model_formula_periods(
            model,
            frozenset(int(period) for period in model.time_structure.historical_periods),
            set(),
        )
        located, unresolved_items = _locate_unresolved_nodes(findings, compiled_registry)
        return BmNodeHistoryResult(
            seed_coverage=(),
            findings=located,
            unresolved_items=unresolved_items,
        )

    requirements = {row.requirement_id: row for row in required_set.rows}
    coverage = {row.requirement_id: row for row in source_bundle.coverage}
    evidence_by_id = {row.evidence_id: row for row in source_bundle.evidence}
    # The pinned series' observation basis is owned by the profile's
    # derived-KPI strategies (`pinned_series_holds_average`).
    derived_strategies = (
        getattr(
            getattr(source_bundle, "coverage_profile", None),
            "derived_kpi_strategies",
            (),
        )
        or ()
    )
    historical_years = frozenset(
        int(period) for period in model.time_structure.historical_periods
    )
    direct_segment_items = direct_segment_face_items(
        business_model, compiled_registry.segment_mapping
    )
    ticker = str(getattr(getattr(business_model, "company", None), "ticker", "") or "")

    resolutions = [
        resolve_bm_node_history(claim, required_set=required_set)
        for claim in claims
    ]
    applicable: list[tuple[BmNodeIdentity | BmNodeFormula, str]] = []
    for resolution in resolutions:
        node_key = resolution.claim.node_key
        if isinstance(resolution, BmNodeBlocker):
            # A calculation operand that resolves to no series (an unclaimed
            # sibling, a statement role the pin lacks) or a claim that is
            # neither pinned nor a calculation: the node's own finding.
            findings.append(
                make_finding(
                    "business_model_node_invalid",
                    message=(
                        f"node {node_key!r}: {resolution.attempted}; "
                        f"repair: {resolution.repair}"
                    ),
                    item_id=node_key,
                    observed={"series": resolution.claim.series},
                )
            )
            continue
        item_id = compiled_registry.node_items.get(node_key)
        if item_id is None:
            # A claim that resolves but whose node compiles to no line
            # (commentary/existing-row targets carry no `node_items` entry)
            # would otherwise be silently inert — the retired executor's
            # target-missing behavior, re-expressed on the channel as the
            # node's finding (plan §4.5: no silent-drop state).
            findings.append(
                make_finding(
                    "business_model_node_invalid",
                    message=(
                        f"node {node_key!r} claims series "
                        f"{resolution.claim.series!r} but is not materialized "
                        "as a compiled line, so its history is not written; "
                        f"repair: {_REPAIR_BMR}"
                    ),
                    item_id=node_key,
                    observed={"series": resolution.claim.series},
                )
            )
            continue
        applicable.append((resolution, item_id))

    result: list[dict[str, Any]] = []
    identity_items: dict[str, str] = {}
    #: Historical cells of a claimed row its requirement does not carry.
    undemanded: list[tuple[str, int]] = []
    # Two-phase application: every identity resolution applies before any
    # formula resolution, so `_apply_formula`'s bound-years lineage always
    # sees the identity writes regardless of BMR document order.
    for resolution, item_id in applicable:
        if isinstance(resolution, BmNodeIdentity):
            without_mean: list[int] = []
            applied = _apply_identity(
                model,
                resolution,
                node=nodes[resolution.claim.node_key],
                requirement=requirements[resolution.requirement_id],
                coverage=coverage.get(resolution.requirement_id),
                evidence_by_id=evidence_by_id,
                historical_years=historical_years,
                natural_item_id=item_id,
                direct_segment_items=direct_segment_items,
                without_mean=without_mean,
                opening=opening_balances,
                derived_strategies=derived_strategies,
            )
            if isinstance(applied, Finding):
                findings.append(applied)
                continue
            result.extend(applied)
            identity_items[_canonical_series(resolution.claim.series)] = item_id
            if model.get_item(item_id).historical is None:
                # The requirement carries every historical year; a year its window declared
                # ``not_disclosed_in_vintage`` is typed coverage the row does not demand.
                requirement_coverage = coverage.get(resolution.requirement_id)
                declared_absent = {
                    row.period_id
                    for row in (() if requirement_coverage is None else requirement_coverage.periods)
                    if row.disposition == "not_disclosed_in_vintage"
                }
                demanded = {
                    _historical_year(period_id, resolution.requirement_id)
                    for period_id in requirements[resolution.requirement_id].period_ids
                    if period_id not in declared_absent
                }
                undemanded.extend(
                    (item_id, year) for year in sorted(historical_years - demanded)
                )
                # A year with no opening observation has no average: the
                # series is absent there, as before its first observation.
                undemanded.extend((item_id, year) for year in without_mean)
    bound_item_ids: set[str] = set()
    formulas = sorted(
        (
            (resolution, item_id)
            for resolution, item_id in applicable
            if isinstance(resolution, BmNodeFormula)
        ),
        # Operands first: a calculation over a calculated sibling reads its line.
        key=lambda row: _calculation_depth(row[0].claim),
    )
    for resolution, item_id in formulas:
        applied = _apply_formula(
            model,
            resolution,
            identity_items=identity_items,
            required_set=required_set,
            coverage_by_requirement=coverage,
            evidence_by_id=evidence_by_id,
            historical_years=historical_years,
            opening=opening_balances,
            natural_item_id=item_id,
            ticker=ticker,
            formula_items=frozenset(bound_item_ids),
        )
        if isinstance(applied, Finding):
            findings.append(applied)
            continue
        result.extend(applied)
        bound_item_ids.add(item_id)
        identity_items[_canonical_series(resolution.claim.series)] = item_id
    _retire_undemanded_history(model, undemanded)
    _restrict_unbound_business_model_formula_periods(
        model, historical_years, bound_item_ids,
    )
    located, unresolved_items = _locate_unresolved_nodes(findings, compiled_registry)
    return BmNodeHistoryResult(
        seed_coverage=tuple(result),
        findings=located,
        unresolved_items=unresolved_items,
    )


__all__ = [
    "BmAdmissionRefusal",
    "BmCalculation",
    "BmNodeBlocker",
    "BmNodeFormula",
    "BmNodeHistoryCompileError",
    "BmNodeHistoryResult",
    "BmNodeIdentity",
    "BmOperand",
    "BmRequiredSeries",
    "BmSeriesClaim",
    "FORMULA_SOURCE_KIND",
    "IDENTITY_SOURCE_KIND",
    "OPENING_BALANCE_SOURCE_KIND",
    "OpeningBalance",
    "RegisteredBmFormula",
    "admit_business_model",
    "apply_business_model_node_history",
    "implied_ratio_seeds",
    "bm_balance_refusals",
    "bm_required_series",
    "bm_series_coverage_findings",
    "bm_series_vocabulary",
    "derive_bm_series_claims",
    "declined_series_rows",
    "direct_segment_face_items",
    "opening_balances",
    "release_unseeded_segment_revenue_cycles",
    "resolve_bm_node_history",
]
