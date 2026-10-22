"""Own correspondence artifacts and derived presentation identity.

Design rule R6 — "identity over time is correspondence or an honest break" —
made representable. The plane has exactly three admitted artifact classes and
one pure derivation:

* **derived identity** — ``presentation_identity_unchanged``: one total,
  three-part comparison over two constructor-minted line refs. Continuity
  without drift is *identity*, computed at compile and **stored nowhere**
  (mechanism §3; storing it would be the second copy P1/P3 forbids).
* **``CorrespondenceLink``** — the admitted cross-basis continuity fact, for
  drift only (``drift_parts`` min 1).
* **``BasisDeclaration``** — the single-filing ambiguous-tree successor of the
  v1 ``authoritative_basis`` mode, admissible only where the calculation-linkbase
  derivation is genuinely ambiguous (``ambiguity`` min 1).
* **``DeclaredBasisBreak``** — the honest refusal: this row does not continue.

What is deliberately unwritable here (mechanism §2.7, the illegal-state audit —
enforced by *absence of representation*, not by validators):

* no label, caption, or preferred-label field exists anywhere in this module,
  so a label-derived link does not typecheck (R5);
* no ``registry_group_id`` / equivalence-group field exists, so tag-equivalence
  cannot widen correspondence (the governing §11 mutation family holds
  structurally, not by a check);
* a zero-drift link is unconstructible in both senses: ``drift_parts`` is min 1
  *and* ``CorrespondenceLink`` recomputes the three-part comparison over its own
  endpoints, refusing a link whose declared ``drift_parts`` disagree with what
  its endpoints actually say (identical endpoints say none) — the stored parts
  are a derived fact of the two refs on the artifact, so they may not be a
  second copy that can desynchronize (P1/P3). The closure never asks for one:
  identical endpoints validate with empty ``drift_parts`` and the door admits
  nothing;
* the **promotion** of a request into an admitted artifact, and the storage of
  one, happen only in the admission door's execute path, on an approved
  ChangeSet (governing design §15.1). A hand-written ``AdmissionRecord`` or
  artifact object remains constructible in memory, because the store has to
  hydrate its own rows back into these types and the closure takes current
  artifacts as declared inputs;
* link endpoints carry accessions but **no values**, so a restated value cannot
  enter through this plane.

This contract is issuer-generic by construction: no issuer identifier appears
in this module; issuer difference enters only as authored artifacts and test
fixtures. Current pipeline context lives in
``docs/reference/build-model-infrastructure-architecture-2026-08-04.md``;
the owner map is ``schema/README.md``.

Step boundaries, stated so nothing here is mistaken for a gap:

* the correspondence *repository* (``api/research/repository/correspondence_artifacts.py``),
  the admission *door* (``api/fms/modules/correspondence.py``) and the proposal
  *port* (``schema/correspondence_proposal_port.py``) landed at step C-3; this
  module supplies their contracts, their capability, and their closures;
* the declaration closure (``validate_basis_declaration``) and the symmetric
  half of ``break_conflict`` (``validate_declared_break``) landed with those
  doors at C-3;
* the closure's carried v1 post-compute codes (``ancestor_descendant_overlap``,
  ``unmapped_direct_child``, ``section_reconciliation_mismatch``) land when the
  validation engine is re-pointed at segments at C-4 (plan §4.7, "carried v1
  codes"); ``validate_correspondence_proposal`` names them in
  ``DEFERRED_CLOSURE_CODES`` so their absence is declared, not silent;
* the compile-side refusals (``multi_basis_window_incomplete``,
  ``correspondence_unlinked_required_year``, ``statement_source_material_missing``)
  land at C-4 with the compile integration.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime
from typing import Annotated, Any, ClassVar, Literal, Mapping, Sequence

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    PlainSerializer,
    field_validator,
    model_validator,
)

from .models import FinancialStatement
from .multi_basis_window import (
    BasisSegment,
    FactDurationType,
    MultiBasisWindow,
    SegmentRowBinding,
)
from .provider_port.contracts import FilingDimensionRef
from .statement_topology import (
    StatementTopology,
    TopologyEdge,
    normalize_topology_role,
)


# --------------------------------------------------------------------------
# contract idiom (the v1 substrate's shape, carried)
#
# Deliberately re-declared rather than imported: the module this idiom came
# from was `schema/statement_mappings.py`, the v1 substrate this plane
# succeeds, and step C-4 deleted it. Re-declaring is what let this plane land
# at C-1 without a load-bearing import of a module whose contract classes were
# already scheduled to die.
# --------------------------------------------------------------------------


class _CorrespondenceContract(BaseModel, frozen=True):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _require_non_blank(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must be non-empty")
    return normalized


#: Every ``frozenset`` field on these contracts is a set of ``Literal`` strings
#: whose JSON form feeds ``content_sha256`` and the artifact's content-derived
#: id. CPython set iteration order depends on ``PYTHONHASHSEED``, so the JSON
#: form is the one canonical order: the sorted list. Python mode keeps the
#: ``frozenset``.
_SortedInJson = PlainSerializer(
    lambda value: sorted(value), return_type=list[str], when_used="json"
)


IdentityPart = Literal["tag_in_role", "tree_parent", "calculation_membership"]

EvidenceKind = Literal[
    "tree_position",
    "calculation_membership",
    "value_continuity",
    "ordering",
    "human_rationale",
]

#: Closure codes this plane owns but does not evaluate until a later step.
#: Named so a reader can tell "not checked yet, by plan" from "forgotten".
DEFERRED_CLOSURE_CODES: tuple[str, ...] = (
    "ancestor_descendant_overlap",  # C-4, with the engine re-point at segments
    "unmapped_direct_child",  # C-4
    "section_reconciliation_mismatch",  # C-4
)


# --------------------------------------------------------------------------
# §2.1 the shared endpoint type
# --------------------------------------------------------------------------


class CalculationMembership(_CorrespondenceContract, frozen=True):
    """One line's calculation arc: additive parent, role, and arc weight."""

    parent_tag: str
    role: str
    weight: float

    @field_validator("parent_tag")
    @classmethod
    def _parent_tag_non_blank(cls, value: str) -> str:
        return _require_non_blank(value, field_name="parent_tag")

    @field_validator("role")
    @classmethod
    def _normalize_role(cls, value: str) -> str:
        normalized = normalize_topology_role(value)
        if not normalized:
            raise ValueError("role must normalize to a non-empty topology role")
        return normalized


def _canonical_dimensions(
    dimensions: tuple[FilingDimensionRef, ...],
) -> tuple[FilingDimensionRef, ...]:
    if len({dimension.axis_qname for dimension in dimensions}) != len(dimensions):
        raise ValueError("dimensions may contain each axis_qname only once")
    return tuple(
        sorted(
            dimensions,
            key=lambda dimension: (dimension.axis_qname, dimension.member_qname),
        )
    )


class PresentationLineRef(_CorrespondenceContract, frozen=True):
    """The full presentation identity of one line in one filing.

    Identity is the three parts R6 names — tag-in-role, tree parent, calculation
    membership — plus the vintage coordinates (accession, fiscal year) that
    compose with Stage B's vintage selection and the explicit dimension set
    selecting which line is compared. Empty dimensions mean consolidated.

    Deliberately **not** identity, and therefore absent as fields: presentation
    order (sibling insertions shift it), labels and preferred-label roles (R5:
    display only), values (evidence for proposals, never identity), and
    equivalence-group ids (no field exists anywhere in this plane).

    Only ``line_ref_from_topology`` mints one. A hand-written ref is a ref the
    pinned trees may not contain; the constructor is the parse step that makes
    "endpoint the filing does not present" unrepresentable rather than merely
    invalid.
    """

    accession: str
    fiscal_year: int = Field(ge=1900, le=2200)
    statement_role: str
    tag: str
    dimensions: tuple[FilingDimensionRef, ...] = ()
    presentation_parent_tag: str
    calculation: CalculationMembership | None = None
    period_type: FactDurationType

    @field_validator("accession", "tag", "presentation_parent_tag")
    @classmethod
    def _non_blank(cls, value: str, info) -> str:
        return _require_non_blank(value, field_name=info.field_name)

    @field_validator("statement_role")
    @classmethod
    def _normalize_statement_role(cls, value: str) -> str:
        normalized = normalize_topology_role(value)
        if not normalized:
            raise ValueError(
                "statement_role must normalize to a non-empty topology role"
            )
        return normalized

    @field_validator("dimensions")
    @classmethod
    def _normalize_dimensions(
        cls, value: tuple[FilingDimensionRef, ...]
    ) -> tuple[FilingDimensionRef, ...]:
        return _canonical_dimensions(value)


LineRefIssueCode = Literal[
    "endpoint_not_in_topology",
    "endpoint_ambiguous_in_topology",
]

#: Why the constructor refused. The *code* vocabulary is the accepted plan
#: §4.7 pair; the reason diagnoses which structural fact caused it, so a repair
#: is addressable without widening the code table.
LineRefIssueReason = Literal[
    "line_absent",  # no presentation edge for (year, role, tag)
    "presentation_parent_ambiguous",  # competing presentation parents
    "calculation_edge_ambiguous",  # competing calculation arcs
    "calculation_weight_unresolved",  # an arc exists but declares no weight
    "period_type_undeclared",  # the pin's payload declares no usable period type
    "accession_not_in_pin",  # the pin attributes this line to another filing
    "accession_missing_from_pin",  # the pin attributes this line to no filing
]


class LineRefIssue(_CorrespondenceContract, frozen=True):
    """Typed constructor refusal — errors-as-values on a total path.

    ``endpoint_not_in_topology`` recovers in the proposal lane or by pin repair;
    ``endpoint_ambiguous_in_topology`` recovers in the proposal lane (plan §4.7).
    """

    code: LineRefIssueCode
    reason: LineRefIssueReason
    fiscal_year: int
    statement_role: str
    tag: str
    competing_edges: tuple[str, ...] = ()
    detail: str

    @field_validator("detail", "tag")
    @classmethod
    def _issue_non_blank(cls, value: str, info) -> str:
        return _require_non_blank(value, field_name=info.field_name)

    @property
    def repair_target(self) -> str:
        if self.reason == "period_type_undeclared":
            return (
                "prepare lane: rebuild the pin from payloads declaring "
                "period_type for this line"
            )
        if self.reason == "accession_not_in_pin":
            return (
                "proposal lane: use the pin's accession for this line, or repair "
                "the pin"
            )
        if self.reason == "accession_missing_from_pin":
            return (
                "prepare lane: rebuild the pin from payloads carrying the filing "
                "accession for this line"
            )
        if self.code == "endpoint_not_in_topology":
            return "proposal lane: fix the endpoint selector, or repair the pin"
        return "proposal lane: the pinned trees carry competing edges for this line"


def _edges(
    topology: StatementTopology,
    *,
    year: int,
    role: str,
    tag: str,
    edge_kind: str,
) -> tuple[TopologyEdge, ...]:
    return tuple(
        edge
        for edge in topology.edges_by_year.get(int(year), ())
        if edge.edge_kind == edge_kind
        and edge.role == role
        and edge.child_tag == tag
    )


def _pinned_accession(
    topology: StatementTopology, *, tag: str, year: int
) -> str | None:
    """The accession the pin attributes this (year, tag) observation to, if any."""

    source_value = topology.source_value(tag, year)
    if source_value is None:
        return None
    source_ref = source_value.source_ref
    if not isinstance(source_ref, Mapping):
        return None
    current = source_ref.get("current")
    if not isinstance(current, Mapping):
        return None
    accession = current.get("accession")
    return str(accession).strip() if accession else None


def line_ref_from_topology(
    topology: StatementTopology,
    *,
    year: int,
    statement_role: str,
    tag: str,
    accession: str,
) -> PresentationLineRef | LineRefIssue:
    """The ONE mint for a ``PresentationLineRef`` (mechanism §2.1).

    Reads the presentation parent, the calculation membership, and the declared
    period type out of the fingerprint-bound per-year trees. Pure over declared
    inputs: nothing here reads a clock, a session, a label, or a value.
    ``StatementTopology`` currently exposes no node dimension set, so this
    mint emits ``()`` (consolidated); member coordinates are not inferred from
    labels, tags, or source-value metadata.

    Because ``StatementTopology.source_payloads_fingerprint`` is content-bound
    to the frozen payloads and the compile path refuses a topology whose
    fingerprint differs from the pin, a ref minted here is replayable against
    the pin that minted it — forever.
    """

    normalized_role = normalize_topology_role(statement_role)
    normalized_tag = str(tag or "").strip()
    if not normalized_role or not normalized_tag:
        return LineRefIssue(
            code="endpoint_not_in_topology",
            reason="line_absent",
            fiscal_year=int(year),
            statement_role=normalized_role,
            tag=normalized_tag or str(tag),
            detail="statement_role and tag are both required to mint an endpoint",
        )

    presentation_edges = _edges(
        topology,
        year=year,
        role=normalized_role,
        tag=normalized_tag,
        edge_kind="presentation",
    )
    if not presentation_edges:
        return LineRefIssue(
            code="endpoint_not_in_topology",
            reason="line_absent",
            fiscal_year=int(year),
            statement_role=normalized_role,
            tag=normalized_tag,
            detail=(
                f"no presentation edge for {normalized_tag!r} in role "
                f"{normalized_role!r} of FY{int(year)}"
            ),
        )
    parents = sorted({edge.parent_tag for edge in presentation_edges})
    if len(parents) > 1:
        return LineRefIssue(
            code="endpoint_ambiguous_in_topology",
            reason="presentation_parent_ambiguous",
            fiscal_year=int(year),
            statement_role=normalized_role,
            tag=normalized_tag,
            competing_edges=tuple(parents),
            detail=(
                f"{normalized_tag!r} has competing presentation parents in "
                f"FY{int(year)}"
            ),
        )

    calculation_edges = _edges(
        topology,
        year=year,
        role=normalized_role,
        tag=normalized_tag,
        edge_kind="calculation",
    )
    calculation: CalculationMembership | None = None
    if calculation_edges:
        arcs = sorted(
            {(edge.parent_tag, edge.weight) for edge in calculation_edges}
        )
        if len({parent for parent, _ in arcs}) > 1:
            return LineRefIssue(
                code="endpoint_ambiguous_in_topology",
                reason="calculation_edge_ambiguous",
                fiscal_year=int(year),
                statement_role=normalized_role,
                tag=normalized_tag,
                competing_edges=tuple(f"{parent}@{weight}" for parent, weight in arcs),
                detail=(
                    f"{normalized_tag!r} has competing calculation parents in "
                    f"FY{int(year)}"
                ),
            )
        weights = {weight for _, weight in arcs}
        parent_tag, weight = arcs[0]
        if weight is None or len(weights) > 1:
            # An arc whose weight the pin does not resolve cannot answer
            # identity part 3. Dropping it would let a weightless arc compare
            # equal to "no arc" and silently mask drift, so it refuses.
            return LineRefIssue(
                code="endpoint_ambiguous_in_topology",
                reason="calculation_weight_unresolved",
                fiscal_year=int(year),
                statement_role=normalized_role,
                tag=normalized_tag,
                competing_edges=tuple(f"{parent}@{weight}" for parent, weight in arcs),
                detail=(
                    f"calculation arc weight for {normalized_tag!r} is unresolved "
                    f"in FY{int(year)}"
                ),
            )
        calculation = CalculationMembership(
            parent_tag=parent_tag,
            role=normalized_role,
            weight=float(weight),
        )

    normalized_accession = str(accession or "").strip()
    pinned_accession = _pinned_accession(topology, tag=normalized_tag, year=int(year))
    if pinned_accession is None:
        # Unconditionally, not conditionally: the vintage coordinate is the
        # pin's. A pin that attributes this line to no filing cannot verify the
        # caller's accession, so the ref is not replayable against the pin that
        # would have minted it — and answering "fine" would let an unverified
        # accession travel onto an admitted artifact.
        return LineRefIssue(
            code="endpoint_not_in_topology",
            reason="accession_missing_from_pin",
            fiscal_year=int(year),
            statement_role=normalized_role,
            tag=normalized_tag,
            detail=(
                f"the pin carries no filing accession for {normalized_tag!r} in "
                f"FY{int(year)}; {normalized_accession!r} cannot be verified"
            ),
        )
    if pinned_accession != normalized_accession:
        # The vintage coordinate must be the pin's, not the caller's: a ref that
        # names another filing describes a line this pin does not contain.
        return LineRefIssue(
            code="endpoint_not_in_topology",
            reason="accession_not_in_pin",
            fiscal_year=int(year),
            statement_role=normalized_role,
            tag=normalized_tag,
            competing_edges=(pinned_accession,),
            detail=(
                f"the pin attributes {normalized_tag!r} in FY{int(year)} to "
                f"{pinned_accession!r}, not {normalized_accession!r}"
            ),
        )

    period_fact = topology.period_fact(normalized_tag, int(year))
    declared_period_type = None if period_fact is None else period_fact.period_type
    if declared_period_type not in ("instant", "duration"):
        return LineRefIssue(
            code="endpoint_not_in_topology",
            reason="period_type_undeclared",
            fiscal_year=int(year),
            statement_role=normalized_role,
            tag=normalized_tag,
            detail=(
                f"pinned payload declares period_type={declared_period_type!r} for "
                f"{normalized_tag!r} in FY{int(year)}; instant or duration required"
            ),
        )

    return PresentationLineRef(
        accession=accession,
        fiscal_year=int(year),
        statement_role=normalized_role,
        tag=normalized_tag,
        dimensions=(),
        presentation_parent_tag=parents[0],
        calculation=calculation,
        period_type=declared_period_type,
    )


# --------------------------------------------------------------------------
# §3 the derived-identity path
# --------------------------------------------------------------------------


class IdentityUnchanged(_CorrespondenceContract, frozen=True):
    """All three identity parts agree — continuity is identity, not correspondence."""

    outcome: Literal["unchanged"] = "unchanged"


class IdentityDrifted(_CorrespondenceContract, frozen=True):
    """At least one identity part moved. ``parts`` is the diagnosing fact."""

    outcome: Literal["drifted"] = "drifted"
    parts: Annotated[frozenset[IdentityPart], _SortedInJson] = Field(min_length=1)


IdentityComparison = Annotated[
    IdentityUnchanged | IdentityDrifted,
    Field(discriminator="outcome"),
]


def presentation_identity_unchanged(
    a: PresentationLineRef,
    b: PresentationLineRef,
) -> IdentityUnchanged | IdentityDrifted:
    """The three-part comparison (mechanism §3). Pure; no ambient input.

    Returns which parts moved, never a bare bool — the drift parts are the fact
    a reviewer and the Stage D topology trace both need.

    Both refs must select the same dimension set. Different sets name different
    lines, not presentation drift: comparing them is a caller bug and raises
    ``ValueError`` rather than inventing a fourth identity part.

    Values are deliberately **not** a fourth part: a value-equal line that moved
    in the tree is drift (the current-vs-noncurrent false-friend guard), and a
    value-changed line in an unchanged slot is a restatement question
    (``schema/comparative_discrimination.py``), not an identity question. A
    weight flip is drift part 3, never a value event.

    Missing calculation membership cannot establish continuity. Either absent
    side reports ``calculation_membership`` as unresolved drift, including two
    absent sides; only two present, matching memberships can carry.

    Boundary semantics: for a role bound in two bound spans of a window, the
    endpoints are the line in the **last presented year of the earlier span**
    and the line in the **first presented year of the later span**. The
    comparison is over two refs and is indifferent to any ``not_presented``
    years between them — the gap is already a typed segment fact.
    """

    if a.dimensions != b.dimensions:
        raise ValueError("presentation identity requires the same dimension set")

    parts: set[IdentityPart] = set()
    if a.tag != b.tag or a.statement_role != b.statement_role:
        parts.add("tag_in_role")
    if a.presentation_parent_tag != b.presentation_parent_tag:
        parts.add("tree_parent")
    if (
        a.calculation is None
        or b.calculation is None
        or _calculation_key(a) != _calculation_key(b)
    ):
        parts.add("calculation_membership")
    if not parts:
        return IdentityUnchanged()
    return IdentityDrifted(parts=frozenset(parts))


def _calculation_key(ref: PresentationLineRef) -> tuple[str, str, float] | None:
    if ref.calculation is None:
        return None
    return (
        ref.calculation.parent_tag,
        ref.calculation.role,
        ref.calculation.weight,
    )


# --------------------------------------------------------------------------
# §2.2 evidence, admission, and the link
# --------------------------------------------------------------------------


class BoundedText(_CorrespondenceContract, frozen=True):
    """Non-blank, length-bounded prose. Prose is never an endpoint."""

    text: str = Field(max_length=4000)

    @field_validator("text")
    @classmethod
    def _text_non_blank(cls, value: str) -> str:
        return _require_non_blank(value, field_name="text")


class TreePositionDetail(_CorrespondenceContract, frozen=True):
    kind: Literal["tree_position"] = "tree_position"
    from_parent_tag: str
    to_parent_tag: str

    @field_validator("from_parent_tag", "to_parent_tag")
    @classmethod
    def _non_blank(cls, value: str, info) -> str:
        return _require_non_blank(value, field_name=info.field_name)


class CalcMembershipDetail(_CorrespondenceContract, frozen=True):
    kind: Literal["calculation_membership"] = "calculation_membership"
    from_membership: CalculationMembership | None = None
    to_membership: CalculationMembership | None = None


class ValueContinuityDetail(_CorrespondenceContract, frozen=True):
    """Magnitude adjacency across a boundary. ADVISORY evidence, never identity."""

    kind: Literal["value_continuity"] = "value_continuity"
    from_fiscal_year: int
    to_fiscal_year: int
    from_value: float | None = None
    to_value: float | None = None


class OrderingDetail(_CorrespondenceContract, frozen=True):
    kind: Literal["ordering"] = "ordering"
    from_order: float
    to_order: float


class HumanRationaleDetail(_CorrespondenceContract, frozen=True):
    kind: Literal["human_rationale"] = "human_rationale"
    rationale: BoundedText


EvidenceDetail = Annotated[
    TreePositionDetail
    | CalcMembershipDetail
    | ValueContinuityDetail
    | OrderingDetail
    | HumanRationaleDetail,
    Field(discriminator="kind"),
]


class EvidenceProducer(_CorrespondenceContract, frozen=True):
    """Who produced a piece of evidence. Recorded so the §15.1 measured corpus exists."""

    kind: Literal["miner", "agent", "human"]
    id: str

    @field_validator("id")
    @classmethod
    def _id_non_blank(cls, value: str) -> str:
        return _require_non_blank(value, field_name="id")


class CorrespondenceEvidence(_CorrespondenceContract, frozen=True):
    """One advisory feature supporting a claim.

    ``confidence`` orders the human review queue and becomes the measured
    corpus; **no confidence threshold admits anything** — deterministic closure
    never reads it and the door never reads it. There is no ``label`` kind:
    R5 is enforced by absence of representation.
    """

    kind: EvidenceKind
    detail: EvidenceDetail
    produced_by: EvidenceProducer
    confidence: float = Field(ge=0.0, le=1.0)

    @model_validator(mode="after")
    def _detail_matches_kind(self) -> "CorrespondenceEvidence":
        if self.detail.kind != self.kind:
            raise ValueError(
                f"evidence detail {self.detail.kind!r} does not match kind {self.kind!r}"
            )
        return self


class AdmissionRecord(_CorrespondenceContract, frozen=True):
    """The promoted human judgment.

    Filled in by the admission door's execute path on an approved ChangeSet
    (mechanism §5): ``approval_id`` / ``approval_chain_id`` are the FMS
    approval lineage, and ``admitted_by`` is the authenticated human principal.

    The type fact that keeps the model from promoting is not this class's
    constructor — the store must be able to hydrate its own rows — but where
    promotion happens: only the admission door's execute path (an approved
    ChangeSet) turns a request into an admitted artifact and writes it.
    """

    admitted_by: str
    # The mechanism §2.2 field sketch types this `datetime`, and it is typed
    # `datetime` here: C-3 persists admission rows under immutability triggers,
    # so an untimestamped admission would be frozen into the store forever.
    admitted_at: datetime
    request_digest: str = Field(pattern=r"^[0-9a-f]{64}$")
    rationale: str
    approval_id: str | None = None
    approval_chain_id: str | None = None

    @field_validator("admitted_by", "rationale")
    @classmethod
    def _non_blank(cls, value: str, info) -> str:
        return _require_non_blank(value, field_name=info.field_name)


class _AdmittedArtifact(_CorrespondenceContract, frozen=True):
    """Shared shape of every admitted correspondence artifact.

    ``identity_field`` names the field holding the artifact's id and
    ``identity_prefix`` that id's prefix. The id is a name for the artifact's
    own digest, so it and the digest are excluded from the digested content.
    """

    identity_field: ClassVar[str]
    identity_prefix: ClassVar[str]

    ticker: str
    statement: FinancialStatement
    section: str
    evidence: tuple[CorrespondenceEvidence, ...] = Field(min_length=1)
    admission: AdmissionRecord
    revision: int = Field(ge=1)
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")

    @field_validator("ticker", "section")
    @classmethod
    def _non_blank(cls, value: str, info) -> str:
        return _require_non_blank(value, field_name=info.field_name)


class CorrespondenceLink(_AdmittedArtifact, frozen=True):
    """The admitted cross-basis continuity fact — for DRIFT only.

    ``drift_parts`` is nonempty and must describe the artifact's own endpoints.
    Historical admissions treated two absent calculation memberships as equal;
    B3b now reports that absence as unresolved drift for current carry and
    admission. Both admitted declarations remain readable without rewriting
    their parts, identities, or digests.

    This stored-row interpretation does not establish current carry eligibility.
    The door's closure still uses ``presentation_identity_unchanged`` against
    the pinned trees and mints the current comparison's parts.
    """

    identity_field: ClassVar[str] = "link_id"
    identity_prefix: ClassVar[str] = "link"
    link_id: str
    row_role: str
    from_line: PresentationLineRef
    to_line: PresentationLineRef
    drift_parts: Annotated[frozenset[IdentityPart], _SortedInJson] = Field(min_length=1)
    proposal_id: str
    supersedes_link_id: str | None = None

    @field_validator("link_id", "row_role", "proposal_id")
    @classmethod
    def _link_non_blank(cls, value: str, info) -> str:
        return _require_non_blank(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _validate_link(self) -> "CorrespondenceLink":
        if self.supersedes_link_id == self.link_id:
            raise ValueError("supersedes_link_id may not name the link itself")
        if self.to_line.fiscal_year <= self.from_line.fiscal_year:
            raise ValueError(
                "to_line.fiscal_year must be later than from_line.fiscal_year"
            )
        comparison = presentation_identity_unchanged(self.from_line, self.to_line)
        endpoint_parts = (
            comparison.parts
            if isinstance(comparison, IdentityDrifted)
            else frozenset()
        )
        declared_parts = self.drift_parts
        if self.from_line.calculation is None and self.to_line.calculation is None:
            # Pre-B3b admissions omitted this unresolved part. Interpret that
            # historical declaration without changing the immutable artifact.
            declared_parts = declared_parts | {"calculation_membership"}
        if endpoint_parts != declared_parts:
            raise ValueError(
                "drift_parts must equal the parts its own endpoints drift on (a "
                "zero-drift link is unconstructible): declared "
                f"{sorted(self.drift_parts)!r}, endpoints say "
                f"{sorted(endpoint_parts)!r}"
            )
        return self

    def validate_current_admission(self) -> "CorrespondenceLink":
        """New admissions use today's rule, not historical row interpretation."""
        comparison = presentation_identity_unchanged(self.from_line, self.to_line)
        if (
            not isinstance(comparison, IdentityDrifted)
            or comparison.parts != self.drift_parts
        ):
            raise ValueError(
                "new correspondence admission requires the current three-part drift"
            )
        return self


# --------------------------------------------------------------------------
# §2.3 the basis declaration
# --------------------------------------------------------------------------


class BasisMemberLine(_CorrespondenceContract, frozen=True):
    tag: str
    expected_weight: float
    coverage: Literal["self", "subtree"]

    @field_validator("tag")
    @classmethod
    def _tag_non_blank(cls, value: str) -> str:
        return _require_non_blank(value, field_name="tag")


BasisAmbiguityKind = Literal[
    "duplicate_edges",
    "competing_roles",
    "no_calculation_linkbase",
    "conflicting_weights",
    "presentation_calculation_disagreement",
]


class BasisAmbiguityReason(_CorrespondenceContract, frozen=True):
    kind: BasisAmbiguityKind
    detail: BoundedText


class BasisDeclaration(_AdmittedArtifact, frozen=True):
    """Which additive rows form a section's basis for ONE accession, and why code could not decide.

    ``ambiguity`` min 1 is load-bearing: an unambiguous filing may not receive a
    declaration. The door's closure additionally recomputes the derivation and
    refuses ``declaration_contradicts_pin`` when the pin exhibits none of the
    declared ambiguity (step C-3); admitting a declaration over a derivable
    basis would store a second copy of a derived fact.
    """

    identity_field: ClassVar[str] = "declaration_id"
    identity_prefix: ClassVar[str] = "declaration"
    declaration_id: str
    accession: str
    fiscal_year: int = Field(ge=1900, le=2200)
    section_total_tag: str
    basis_source: Literal["calculation", "presentation"]
    member_lines: tuple[BasisMemberLine, ...] = Field(min_length=1)
    ambiguity: tuple[BasisAmbiguityReason, ...] = Field(min_length=1)
    supersedes_declaration_id: str | None = None

    @field_validator("declaration_id", "accession", "section_total_tag")
    @classmethod
    def _declaration_non_blank(cls, value: str, info) -> str:
        return _require_non_blank(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _validate_declaration(self) -> "BasisDeclaration":
        if self.supersedes_declaration_id == self.declaration_id:
            raise ValueError(
                "supersedes_declaration_id may not name the declaration itself"
            )
        tags = [member.tag for member in self.member_lines]
        if len(tags) != len(set(tags)):
            raise ValueError("member_lines tags must be unique")
        return self


# --------------------------------------------------------------------------
# §2.4 the honest break
# --------------------------------------------------------------------------


class SegmentBoundaryRef(_CorrespondenceContract, frozen=True):
    """The junction between two bound spans of a window.

    For adjacent segments this is fiscal years Y and Y+1. Across declared
    ``not_presented`` years the junction is the last presented year of the
    earlier bound span and the first presented year of the later one — the
    endpoints mechanism §3 compares. ``after_fy > before_fy`` is the invariant.
    """

    before_fy: int = Field(ge=1900, le=2200)
    after_fy: int = Field(ge=1900, le=2200)

    @model_validator(mode="after")
    def _validate_boundary(self) -> "SegmentBoundaryRef":
        if self.after_fy <= self.before_fy:
            raise ValueError("boundary after_fy must be later than before_fy")
        return self


class RowBreak(_CorrespondenceContract, frozen=True):
    kind: Literal["row"] = "row"
    row_role: str

    @field_validator("row_role")
    @classmethod
    def _row_role_non_blank(cls, value: str) -> str:
        return _require_non_blank(value, field_name="row_role")


class SectionRebasis(_CorrespondenceContract, frozen=True):
    kind: Literal["section"] = "section"


BreakScope = Annotated[RowBreak | SectionRebasis, Field(discriminator="kind")]


class DeclaredBasisBreak(_AdmittedArtifact, frozen=True):
    """The honest refusal: this row does not continue across this boundary.

    Renders as two segments of history for the affected rows — a representable
    product fact, never a third numeric series and never a merged tag.

    **Window shortening is not a break.** Shortening an accepted profile's window
    is a DEC-07-class re-approval in the corpus/profile lane; the door
    cannot mint it, and the window type represents it as ``from_fy``.
    """

    identity_field: ClassVar[str] = "break_id"
    identity_prefix: ClassVar[str] = "break"
    break_id: str
    scope: BreakScope
    boundary: SegmentBoundaryRef
    reason: BoundedText
    supersedes_break_id: str | None = None

    @field_validator("break_id")
    @classmethod
    def _break_id_non_blank(cls, value: str) -> str:
        return _require_non_blank(value, field_name="break_id")

    @model_validator(mode="after")
    def _validate_break(self) -> "DeclaredBasisBreak":
        if self.supersedes_break_id == self.break_id:
            raise ValueError("supersedes_break_id may not name the break itself")
        return self


# --------------------------------------------------------------------------
# §2.6 the proposal record
# --------------------------------------------------------------------------


ProposalRejectionCode = Literal[
    "endpoint_not_in_topology",
    "endpoint_ambiguous_in_topology",
    "topology_basis_unavailable",
    "period_type_mismatch",
    "link_conflict",
    "supersession_conflict",
    "break_conflict",
    "declaration_contradicts_pin",
    # No closure emits these two any longer; they stay so a persisted rejected
    # proposal naming either still validates.
    "identity_no_drift",
    "basis_derivable",
]


class ProposalRejection(_CorrespondenceContract, frozen=True):
    """A recorded terminal. Required iff a proposal's status is ``rejected``."""

    outcome: Literal["rejected"] = "rejected"
    code: ProposalRejectionCode
    detail: BoundedText
    carried: Mapping[str, Any] = Field(default_factory=dict)

    @property
    def repair_target(self) -> str:
        return _REJECTION_REPAIR_TARGETS[self.code]


_REJECTION_REPAIR_TARGETS: Mapping[str, str] = {
    "endpoint_not_in_topology": "proposal lane: fix the endpoint selector, or repair the pin",
    "endpoint_ambiguous_in_topology": "proposal lane: the pinned trees carry competing edges",
    "topology_basis_unavailable": "corpus / segment-authoring lane: repair the pin or the reviewed roles",
    "identity_no_drift": "proposal lane: drop the candidate — identity already covers this row",
    "period_type_mismatch": "proposal lane: the endpoints are not the same kind of fact",
    "link_conflict": "admission lane: supersede the standing link, or withdraw",
    "supersession_conflict": "admission lane: re-propose against the current lineage head",
    "break_conflict": "admission lane: supersede the standing artifact — one resolution per boundary",
    "basis_derivable": "none — declarations are for genuine ambiguity only; the pinned filing already derives this basis",
    "declaration_contradicts_pin": "proposal lane: declare the basis the pinned filing carries and the ambiguity it exhibits — or repair the reviewed roles",
}


class ProposalValidated(_CorrespondenceContract, frozen=True):
    """Closure passed. NOT admitted — the judgment is still the human's.

    Empty ``drift_parts``: the endpoints are three-part identical, identity
    carries the row, and the door admits nothing (no ChangeSet).
    """

    outcome: Literal["validated"] = "validated"
    drift_parts: Annotated[frozenset[IdentityPart], _SortedInJson]
    boundary: SegmentBoundaryRef


ProposalClosure = Annotated[
    ProposalValidated | ProposalRejection,
    Field(discriminator="outcome"),
]


class CorrespondenceProposal(_CorrespondenceContract, frozen=True):
    """Port output: a candidate continuity claim awaiting a human.

    ``claim`` has exactly one arm. Endpoints are constructor-minted server-side
    from selectors; prose cannot smuggle an endpoint. Every proposal reaches a
    terminal status — a rejected or abandoned candidate is a recorded terminal,
    never a dangling open.
    """

    proposal_id: str
    ticker: str
    statement: FinancialStatement
    section: str
    row_role: str
    claim: Literal["continues"] = "continues"
    from_line: PresentationLineRef
    to_line: PresentationLineRef
    supersedes_link_id: str | None = None
    evidence: tuple[CorrespondenceEvidence, ...] = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)
    proposed_by: EvidenceProducer
    status: Literal[
        "proposed", "validated", "rejected", "withdrawn", "admitted", "superseded"
    ]
    rejection: ProposalRejection | None = None

    @field_validator("proposal_id", "ticker", "section", "row_role")
    @classmethod
    def _non_blank(cls, value: str, info) -> str:
        return _require_non_blank(value, field_name=info.field_name)

    @model_validator(mode="after")
    def _validate_proposal(self) -> "CorrespondenceProposal":
        if self.status == "rejected" and self.rejection is None:
            raise ValueError("a rejected proposal must carry its rejection")
        if self.status != "rejected" and self.rejection is not None:
            raise ValueError("only a rejected proposal may carry a rejection")
        if self.to_line.fiscal_year <= self.from_line.fiscal_year:
            raise ValueError(
                "to_line.fiscal_year must be later than from_line.fiscal_year"
            )
        return self


# --------------------------------------------------------------------------
# §4.3 deterministic closure
# --------------------------------------------------------------------------


def _rejection(
    code: ProposalRejectionCode,
    detail: str,
    **carried: Any,
) -> ProposalRejection:
    return ProposalRejection(
        code=code,
        detail=BoundedText(text=detail),
        carried=carried,
    )


def _bound_spans(window: MultiBasisWindow, row_role: str) -> tuple[BasisSegment, ...]:
    return tuple(
        segment
        for segment in window.segments
        if isinstance(segment.rows.get(row_role), SegmentRowBinding)
    )


def _span_junction(
    window: MultiBasisWindow,
    *,
    row_role: str,
    before_fy: int,
    after_fy: int,
) -> tuple[int, int] | None:
    """Which ADJACENT pair of bound spans two years straddle, as span indices.

    The junction — not the pair of years — is the identity of a boundary: it is
    a fact of the window's bound-span structure, which no proposal author
    controls. Two artifacts naming different years across the same junction are
    resolving the *same* boundary, and the "at most one standing resolution
    artifact per row/boundary" invariant (plan §4.3/§4.4) has to see that.

    ``None`` when the two years do not straddle one junction: the same span
    (no junction at all), a year outside every bound span, or two spans with a
    further bound span between them — the latter is two junctions, not one, and
    the claim would silently skip the span in the middle.
    """

    spans = _bound_spans(window, row_role)
    earlier = next(
        (i for i, s in enumerate(spans) if s.from_fy <= before_fy <= s.through_fy),
        None,
    )
    later = next(
        (i for i, s in enumerate(spans) if s.from_fy <= after_fy <= s.through_fy),
        None,
    )
    if earlier is None or later is None or later != earlier + 1:
        return None
    return (earlier, later)


def _row_binding_tag(window: MultiBasisWindow, *, row_role: str, year: int) -> str | None:
    """The exact tag the window binds ``row_role`` to in ``year``'s segment."""

    segment = window.segment_for(year)
    if segment is None:
        return None
    disposition = segment.rows.get(row_role)
    if not isinstance(disposition, SegmentRowBinding):
        return None
    return disposition.tag


def _has_reviewed_basis(
    topology: StatementTopology,
    segment: BasisSegment,
    year: int,
) -> bool:
    calculation = topology.additive_children_of(
        segment.section_total_tag,
        year,
        calculation_roles=segment.topology_roles.basis_calculation_roles,
    )
    if calculation:
        return True
    for parent_tag in segment.topology_roles.presentation_parent_tags:
        if topology.presentation_children_of(
            parent_tag,
            year,
            presentation_roles=segment.topology_roles.presentation_roles,
        ):
            return True
    return False


def validate_correspondence_proposal(
    proposal: CorrespondenceProposal,
    topologies: Mapping[int, StatementTopology],
    window: MultiBasisWindow,
    current_links: Sequence[CorrespondenceLink],
    current_breaks: Sequence[DeclaredBasisBreak],
) -> ProposalValidated | ProposalRejection:
    """Pure, total closure run BEFORE any human sees a candidate (mechanism §4.3).

    ``current_links`` / ``current_breaks`` are the lineage heads — the pure
    ``current_correspondence`` fold's output (step C-3), passed in as a declared
    input so this stays pure over its arguments (P2).

    What closure deliberately does **not** decide: whether the drifted line is
    *economically* the same row. That is the judgment. A proposal that passes is
    ``validated``, not admitted — false friends that pass every structural check
    still die at the human's hands, and that is the design, not a gap.

    Deferred by plan, not forgotten: ``DEFERRED_CLOSURE_CODES`` (the carried v1
    post-compute arms) join this closure when the validation engine is
    re-pointed at segments at step C-4.

    Refusal order — endpoint-local facts first, then window-relative facts, then
    standing-artifact facts, so the most specific diagnosis wins:

    1. endpoints re-minted against the pinned trees (``endpoint_not_in_topology``
       / ``endpoint_ambiguous_in_topology``);
    2. ``period_type_mismatch`` — period type is not one of the three identity
       parts, so it is checked before identity is compared;
    3. ``topology_basis_unavailable`` — the claim must straddle exactly one
       bound-span junction (adjacent spans: two junctions is a claim that skips
       the span between them), its endpoints must be the tags the window binds
       the row to on each side, both endpoint years must carry reviewed basis
       edges, and each endpoint's role must be in that reviewed basis's
       presentation-role allowlist;
    4. ``supersession_conflict`` — before ``link_conflict``, because the
       correction exemption is only meaningful once its target is valid;
    5. ``link_conflict``;
    6. ``break_conflict``.

    Endpoints that drift on no identity part validate with empty
    ``drift_parts``: identity carries the row and the door admits nothing.
    """

    if (
        window.ticker != proposal.ticker
        or window.statement is not proposal.statement
        or window.section != proposal.section
    ):
        raise ValueError(
            "closure requires the window for the proposal's ticker, statement "
            "and section"
        )

    # 1. Endpoints: re-mint server-side against the pinned trees. A proposal's
    #    refs are never trusted as authored — the trees are the authority.
    for label, ref in (("from_line", proposal.from_line), ("to_line", proposal.to_line)):
        topology = topologies.get(ref.fiscal_year)
        if topology is None:
            return _rejection(
                "endpoint_not_in_topology",
                f"{label}: no pinned topology for FY{ref.fiscal_year}",
                endpoint=label,
                fiscal_year=ref.fiscal_year,
            )
        minted = line_ref_from_topology(
            topology,
            year=ref.fiscal_year,
            statement_role=ref.statement_role,
            tag=ref.tag,
            accession=ref.accession,
        )
        if isinstance(minted, LineRefIssue):
            return _rejection(
                minted.code,
                f"{label}: {minted.detail}",
                endpoint=label,
                reason=minted.reason,
                fiscal_year=minted.fiscal_year,
                tag=minted.tag,
                competing_edges=list(minted.competing_edges),
                repair_target=minted.repair_target,
            )
        if minted != ref:
            return _rejection(
                "endpoint_not_in_topology",
                (
                    f"{label}: the pinned tree mints a different line for "
                    f"{ref.tag!r} in FY{ref.fiscal_year}"
                ),
                endpoint=label,
                fiscal_year=ref.fiscal_year,
                tag=ref.tag,
            )

    # 2. Period type: instant and duration lines are not the same kind of fact.
    #    Checked before identity because period type is not one of the three
    #    identity parts, so an otherwise-identical pair of different kinds must
    #    not validate as identity carrying the row.
    if proposal.from_line.period_type != proposal.to_line.period_type:
        return _rejection(
            "period_type_mismatch",
            (
                f"endpoints differ: {proposal.from_line.period_type} vs "
                f"{proposal.to_line.period_type}"
            ),
            from_period_type=proposal.from_line.period_type,
            to_period_type=proposal.to_line.period_type,
        )

    # 3. Identity: endpoints three-part identical drift on no part. That is not
    #    a refusal: the closure validates with empty ``drift_parts`` and the
    #    door plans nothing to admit, because identity already carries the row.
    comparison = presentation_identity_unchanged(proposal.from_line, proposal.to_line)
    drift_parts = (
        comparison.parts if isinstance(comparison, IdentityDrifted) else frozenset()
    )

    # 4. Reviewed basis: the claim must straddle exactly one bound-span junction
    #    of the window, its endpoints must be the tags the window binds the row
    #    to on each side, both endpoint years must carry a usable reviewed
    #    basis, and each endpoint's role must be reviewed by its segment.
    junction = _span_junction(
        window,
        row_role=proposal.row_role,
        before_fy=proposal.from_line.fiscal_year,
        after_fy=proposal.to_line.fiscal_year,
    )
    if junction is None:
        return _rejection(
            "topology_basis_unavailable",
            (
                f"row_role {proposal.row_role!r} is not bound in two adjacent "
                f"spans straddling FY{proposal.from_line.fiscal_year} and "
                f"FY{proposal.to_line.fiscal_year}"
            ),
            row_role=proposal.row_role,
            reasons=["no_bound_span_junction"],
        )
    boundary = SegmentBoundaryRef(
        before_fy=proposal.from_line.fiscal_year,
        after_fy=proposal.to_line.fiscal_year,
    )
    # The row_role scopes every conflict check below, so an endpoint that is not
    # the tag the window binds this row to would file the link under a row it
    # does not describe.
    mismatched = [
        f"FY{ref.fiscal_year}:row_not_bound_to_endpoint_tag"
        for ref in (proposal.from_line, proposal.to_line)
        if _row_binding_tag(window, row_role=proposal.row_role, year=ref.fiscal_year)
        != ref.tag
    ]
    if mismatched:
        return _rejection(
            "topology_basis_unavailable",
            (
                f"row_role {proposal.row_role!r} is not bound to the endpoint tags "
                "this proposal names: " + ", ".join(mismatched)
            ),
            row_role=proposal.row_role,
            reasons=mismatched,
        )
    unusable: list[str] = []
    for ref in (proposal.from_line, proposal.to_line):
        segment = window.segment_for(ref.fiscal_year)
        if segment is None or not _has_reviewed_basis(
            topologies[ref.fiscal_year], segment, ref.fiscal_year
        ):
            unusable.append(f"FY{ref.fiscal_year}:no_reviewed_edges")
    if unusable:
        return _rejection(
            "topology_basis_unavailable",
            "endpoint years carry no usable reviewed basis: " + ", ".join(unusable),
            row_role=proposal.row_role,
            reasons=unusable,
        )
    unreviewed_roles: list[str] = []
    for ref in (proposal.from_line, proposal.to_line):
        segment = window.segment_for(ref.fiscal_year)
        if segment is None:
            continue
        reviewed_roles = {
            normalize_topology_role(role)
            for role in segment.topology_roles.presentation_roles
        }
        if ref.statement_role not in reviewed_roles:
            unreviewed_roles.append(
                f"FY{ref.fiscal_year}:statement_role_not_in_reviewed_allowlist"
            )
    if unreviewed_roles:
        return _rejection(
            "topology_basis_unavailable",
            (
                "endpoint statement roles are outside their segments' reviewed "
                "presentation-role allowlists: " + ", ".join(unreviewed_roles)
            ),
            row_role=proposal.row_role,
            reasons=unreviewed_roles,
        )

    # 5. Supersession lineage: single-successor chains, recomputed at the door.
    scoped_links = tuple(
        link
        for link in current_links
        if link.ticker == proposal.ticker
        and link.statement is proposal.statement
        and link.section == proposal.section
    )
    if proposal.supersedes_link_id is not None:
        target = next(
            (link for link in scoped_links if link.link_id == proposal.supersedes_link_id),
            None,
        )
        if target is None:
            return _rejection(
                "supersession_conflict",
                (
                    f"supersedes_link_id {proposal.supersedes_link_id!r} is unknown "
                    "among the current lineage heads"
                ),
                target_link_id=proposal.supersedes_link_id,
                existing_successor=None,
            )
        successor = next(
            (
                link
                for link in scoped_links
                if link.supersedes_link_id == proposal.supersedes_link_id
            ),
            None,
        )
        if successor is not None:
            return _rejection(
                "supersession_conflict",
                (
                    f"link {proposal.supersedes_link_id!r} already has successor "
                    f"{successor.link_id!r}"
                ),
                target_link_id=proposal.supersedes_link_id,
                existing_successor=successor.link_id,
            )

    # 6. One continuity owner per endpoint, AND at most one standing link per
    #    row/boundary — the two-writers gate applied to the link-vs-link axis.
    #    A correction proposal is exempt only w.r.t. its named target.
    #    Matched on the JUNCTION for the same-row arm, for the same reason step
    #    7 matches breaks that way: the junction, not the pair of years, is the
    #    identity of a boundary. Endpoint equality alone would let two links
    #    whose endpoint pairs merely differ (FY2020→FY2022 and FY2021→FY2023
    #    across one junction) both stand, and the compile's boundary decision
    #    order (plan §4.3) would stop being exhaustive.
    endpoints = (proposal.from_line, proposal.to_line)
    for link in scoped_links:
        if link.link_id == proposal.supersedes_link_id:
            continue
        claims_endpoint = any(
            claimed in endpoints for claimed in (link.from_line, link.to_line)
        )
        resolves_junction = link.row_role == proposal.row_role and (
            _span_junction(
                window,
                row_role=proposal.row_role,
                before_fy=link.from_line.fiscal_year,
                after_fy=link.to_line.fiscal_year,
            )
            == junction
        )
        if claims_endpoint or resolves_junction:
            return _rejection(
                "link_conflict",
                (
                    f"link {link.link_id!r} already resolves this row/boundary"
                    if resolves_junction
                    else (
                        f"link {link.link_id!r} already claims an endpoint of this "
                        "section boundary"
                    )
                ),
                conflicting_link_id=link.link_id,
                row_role=proposal.row_role,
                boundary=boundary.model_dump(mode="json"),
                from_line=link.from_line.model_dump(mode="json"),
                to_line=link.to_line.model_dump(mode="json"),
            )

    # 7. One standing resolution artifact per boundary (link-vs-break axis).
    #    Matched on the JUNCTION, never on year equality: a break and a link may
    #    legitimately name different years across the same junction, and
    #    comparing years would let a proposal step one year past a standing
    #    break and validate anyway.
    #    The symmetric half — a standing link blocking a break admission —
    #    lands with the break door closure at step C-3.
    for declared_break in current_breaks:
        if (
            declared_break.ticker != proposal.ticker
            or declared_break.statement is not proposal.statement
            or declared_break.section != proposal.section
        ):
            continue
        if (
            _span_junction(
                window,
                row_role=proposal.row_role,
                before_fy=declared_break.boundary.before_fy,
                after_fy=declared_break.boundary.after_fy,
            )
            != junction
        ):
            continue
        scope = declared_break.scope
        if isinstance(scope, SectionRebasis) or scope.row_role == proposal.row_role:
            return _rejection(
                "break_conflict",
                (
                    f"break {declared_break.break_id!r} already resolves this "
                    "row/boundary"
                ),
                standing_artifact_id=declared_break.break_id,
                row_role=proposal.row_role,
                boundary=boundary.model_dump(mode="json"),
            )

    return ProposalValidated(drift_parts=drift_parts, boundary=boundary)


# --------------------------------------------------------------------------
# canonical digest (the door's `content_sha256`, mechanism §5)
# --------------------------------------------------------------------------


def correspondence_content_sha256(payload: Mapping[str, Any]) -> str:
    """Canonical-JSON digest of an artifact's content. One encoding, one digest."""

    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


# --------------------------------------------------------------------------
# admission requests — what a person approves, before an artifact exists
#
# A `CorrespondenceProposal` is the request for a link (mechanism §2.6). The
# other two artifact classes had no request type: these are theirs, carrying
# exactly the fields the artifact carries minus the ones only the door mints
# (id, admission, revision, content digest).
# --------------------------------------------------------------------------


class BasisDeclarationRequest(_CorrespondenceContract, frozen=True):
    """A request to admit one filing's basis, and the reasons code could not decide."""

    ticker: str
    statement: FinancialStatement
    section: str
    accession: str
    fiscal_year: int = Field(ge=1900, le=2200)
    section_total_tag: str
    basis_source: Literal["calculation", "presentation"]
    member_lines: tuple[BasisMemberLine, ...] = Field(min_length=1)
    ambiguity: tuple[BasisAmbiguityReason, ...] = Field(min_length=1)
    evidence: tuple[CorrespondenceEvidence, ...] = Field(min_length=1)
    supersedes_declaration_id: str | None = None

    @field_validator("ticker", "section", "accession", "section_total_tag")
    @classmethod
    def _non_blank(cls, value: str, info) -> str:
        return _require_non_blank(value, field_name=info.field_name)


class DeclaredBreakRequest(_CorrespondenceContract, frozen=True):
    """A request to declare that a row (or a whole section) does not continue."""

    ticker: str
    statement: FinancialStatement
    section: str
    scope: BreakScope
    boundary: SegmentBoundaryRef
    reason: BoundedText
    evidence: tuple[CorrespondenceEvidence, ...] = Field(min_length=1)
    supersedes_break_id: str | None = None

    @field_validator("ticker", "section")
    @classmethod
    def _non_blank(cls, value: str, info) -> str:
        return _require_non_blank(value, field_name=info.field_name)


class DeclarationValidated(_CorrespondenceContract, frozen=True):
    """Declaration closure passed. NOT admitted — the judgment is still the human's."""

    outcome: Literal["validated"] = "validated"
    #: The ambiguity kinds the pinned filing actually exhibits, recomputed here.
    #: Carried, not compared: the human's stated reasons are their explanation,
    #: and the door refuses only when the derivation is unambiguous.
    recomputed_ambiguity: Annotated[
        frozenset[BasisAmbiguityKind], _SortedInJson
    ] = Field(min_length=1)


class BreakValidated(_CorrespondenceContract, frozen=True):
    """Break closure passed. NOT admitted — the judgment is still the human's."""

    outcome: Literal["validated"] = "validated"
    boundary: SegmentBoundaryRef


# --------------------------------------------------------------------------
# §2.3 / §4.3 the declaration closure
# --------------------------------------------------------------------------


def _segment_junction(
    window: MultiBasisWindow, *, before_fy: int, after_fy: int
) -> tuple[int, int] | None:
    """Which ADJACENT pair of segments two years straddle, as segment indices."""

    earlier = next(
        (
            index
            for index, segment in enumerate(window.segments)
            if segment.from_fy <= before_fy <= segment.through_fy
        ),
        None,
    )
    later = next(
        (
            index
            for index, segment in enumerate(window.segments)
            if segment.from_fy <= after_fy <= segment.through_fy
        ),
        None,
    )
    if earlier is None or later is None or later != earlier + 1:
        return None
    return (earlier, later)


def _break_junction(
    window: MultiBasisWindow, request_scope: BreakScope, boundary: SegmentBoundaryRef
) -> tuple[str, tuple[int, int]] | None:
    """The junction a break resolves, tagged by the axis it is computed on.

    A row break joins the row's adjacent **bound spans** (the same identity the
    link closure uses, so a link and a break for one row/boundary are seen as
    resolving the same thing). A section rebasis joins adjacent **segments**.
    """

    if isinstance(request_scope, RowBreak):
        junction = _span_junction(
            window,
            row_role=request_scope.row_role,
            before_fy=boundary.before_fy,
            after_fy=boundary.after_fy,
        )
        return None if junction is None else ("row", junction)
    junction = _segment_junction(
        window, before_fy=boundary.before_fy, after_fy=boundary.after_fy
    )
    return None if junction is None else ("section", junction)


def derived_basis_ambiguity(
    topology: StatementTopology,
    *,
    section_total_tag: str,
    fiscal_year: int,
    topology_roles: Any,
) -> frozenset[BasisAmbiguityKind]:
    """Recompute why (or whether) the single-filing basis derivation is ambiguous.

    The derivation is the existing calculation-then-presentation selection. This
    returns the ambiguity kinds the pinned trees actually exhibit; an **empty**
    set means code owns the answer and a declaration would store a second copy
    of a derived fact.
    """

    year = int(fiscal_year)
    calculation = topology.additive_children_of(
        section_total_tag,
        year,
        calculation_roles=topology_roles.basis_calculation_roles,
    )
    kinds: set[BasisAmbiguityKind] = set()
    if calculation:
        by_tag: dict[str, list[TopologyEdge]] = {}
        for edge in calculation:
            by_tag.setdefault(edge.child_tag, []).append(edge)
        for edges in by_tag.values():
            if len(edges) > 1:
                kinds.add("duplicate_edges")
            if len({edge.weight for edge in edges}) > 1:
                kinds.add("conflicting_weights")
            if any(edge.weight is None for edge in edges):
                kinds.add("conflicting_weights")
        if len({edge.role for edge in calculation}) > 1:
            kinds.add("competing_roles")
        presented = {
            edge.child_tag
            for parent_tag in topology_roles.presentation_parent_tags
            for edge in topology.presentation_children_of(
                parent_tag,
                year,
                presentation_roles=topology_roles.presentation_roles,
            )
        } - {section_total_tag}
        # A usable calculation linkbase OWNS the answer — the derivation is
        # calculation-then-presentation — so presentation being *broader* than
        # the calculation is presentation convention, not a disagreement about
        # the basis: the section total presented beside its own members, memo
        # lines, abstract sub-headers, rows the issuer keeps off the rollup.
        # Reading any of those as ambiguity would manufacture a declaration over
        # a basis the pin already derives, which ``declaration_contradicts_pin``
        # exists to prevent. What genuinely contradicts the calculation basis is
        # the other direction: a calculation member the reviewed presentation
        # does not carry at all.
        if presented and set(by_tag) - presented:
            kinds.add("presentation_calculation_disagreement")
        return frozenset(kinds)

    kinds.add("no_calculation_linkbase")
    presentation_edges = [
        edge
        for parent_tag in topology_roles.presentation_parent_tags
        for edge in topology.presentation_children_of(
            parent_tag,
            year,
            presentation_roles=topology_roles.presentation_roles,
        )
        if edge.child_tag != section_total_tag
    ]
    if not presentation_edges:
        # Neither basis is usable at all. That is not "ambiguous" — there is
        # nothing to be ambiguous between — and the caller must refuse with
        # `topology_basis_unavailable` rather than admit a declaration.
        return frozenset()
    by_tag = {}
    for edge in presentation_edges:
        by_tag.setdefault(edge.child_tag, []).append(edge)
    if any(len(edges) > 1 for edges in by_tag.values()):
        kinds.add("duplicate_edges")
    if len({edge.role for edge in presentation_edges}) > 1:
        kinds.add("competing_roles")
    return frozenset(kinds)


def validate_basis_declaration(
    request: BasisDeclarationRequest,
    topologies: Mapping[int, StatementTopology],
    window: MultiBasisWindow,
    current_declarations: Sequence[BasisDeclaration],
) -> DeclarationValidated | ProposalRejection:
    """Pure, total closure for a declaration admission (mechanism §2.3, §4.3).

    Refusal order — the pin's own facts first, then what the request claims
    about the derivation, then standing-artifact facts:

    1. ``topology_basis_unavailable`` — no pinned topology for the year, the
       year sits outside the window, the request disagrees with the segment's
       reviewed total tag, or **neither** basis is usable (nothing to be
       ambiguous between: an unusable pin is a pin repair, never an admission);
    2. ``declaration_contradicts_pin`` — the request's own claims disagree with
       the derivation just recomputed: a ``basis_source`` the pin does not
       carry, or ambiguity kinds the pin does not exhibit. A basis the pin
       derives unambiguously exhibits no ambiguity, and ``ambiguity`` is min 1,
       so a declaration over it refuses here;
    3. ``topology_basis_unavailable`` again — a declared member line is not a
       child of the total **on the basis the request declares**;
    4. ``supersession_conflict`` — the lineage, and the "one standing
       declaration per (accession, section)" rule.
    """

    if (
        window.ticker != request.ticker
        or window.statement is not request.statement
        or window.section != request.section
    ):
        raise ValueError(
            "closure requires the window for the request's ticker, statement "
            "and section"
        )

    year = int(request.fiscal_year)
    topology = topologies.get(year)
    if topology is None:
        return _rejection(
            "topology_basis_unavailable",
            f"no pinned topology for FY{year}",
            fiscal_year=year,
            reasons=[f"FY{year}:no_pinned_topology"],
        )
    segment = window.segment_for(year)
    if segment is None:
        return _rejection(
            "topology_basis_unavailable",
            f"FY{year} is outside the window's segments",
            fiscal_year=year,
            reasons=[f"FY{year}:year_outside_window"],
        )
    if segment.section_total_tag != request.section_total_tag:
        return _rejection(
            "topology_basis_unavailable",
            (
                f"FY{year}'s segment reviews section total "
                f"{segment.section_total_tag!r}, not {request.section_total_tag!r}"
            ),
            fiscal_year=year,
            reasons=[f"FY{year}:section_total_tag_mismatch"],
        )
    pinned_accession = _pinned_accession(
        topology, tag=request.section_total_tag, year=year
    )
    if pinned_accession is None or pinned_accession != request.accession:
        return _rejection(
            "topology_basis_unavailable",
            (
                f"FY{year}'s pin attributes the section total to "
                f"{pinned_accession!r}, not {request.accession!r}"
            ),
            fiscal_year=year,
            reasons=[f"FY{year}:accession_not_in_pin"],
        )

    calculation_children = {
        edge.child_tag
        for edge in topology.additive_children_of(
            request.section_total_tag,
            year,
            calculation_roles=segment.topology_roles.basis_calculation_roles,
        )
    }
    presentation_children = {
        edge.child_tag
        for parent_tag in segment.topology_roles.presentation_parent_tags
        for edge in topology.presentation_children_of(
            parent_tag,
            year,
            presentation_roles=segment.topology_roles.presentation_roles,
        )
    }
    recomputed = derived_basis_ambiguity(
        topology,
        section_total_tag=request.section_total_tag,
        fiscal_year=year,
        topology_roles=segment.topology_roles,
    )
    if not recomputed and not calculation_children:
        return _rejection(
            "topology_basis_unavailable",
            (
                f"FY{year} carries neither a usable calculation basis nor a "
                "usable reviewed presentation basis"
            ),
            fiscal_year=year,
            reasons=[f"FY{year}:no_reviewed_edges"],
        )

    # The declared `basis_source` and the declared `ambiguity` are stored
    # verbatim on an immutable row, and the compile reads `basis_source` to pick
    # its basis engine. Neither is re-derivable from the row afterwards, so a
    # claim the closure has just recomputed and has NOT checked is a second copy
    # free to be false — the desynchronizing copy the link path forbids by
    # taking `drift_parts` from the closure instead of from the caller.
    carries_declared_basis = (
        bool(calculation_children)
        if request.basis_source == "calculation"
        else bool(presentation_children - {request.section_total_tag})
    )
    if not carries_declared_basis:
        return _rejection(
            "declaration_contradicts_pin",
            (
                f"the declaration claims a {request.basis_source} basis and "
                f"FY{year}'s pin carries no reviewed {request.basis_source} "
                "children of the section total"
            ),
            fiscal_year=year,
            reasons=[f"FY{year}:basis_source_not_in_pin"],
            basis_source=request.basis_source,
            recomputed_ambiguity=sorted(recomputed),
        )
    unsupported = sorted(
        {reason.kind for reason in request.ambiguity} - set(recomputed)
    )
    if unsupported:
        return _rejection(
            "declaration_contradicts_pin",
            (
                f"the declaration states ambiguity FY{year}'s pin does not "
                f"exhibit: {unsupported!r}"
            ),
            fiscal_year=year,
            reasons=[f"FY{year}:declared_ambiguity_not_in_pin"],
            unsupported_kinds=unsupported,
            recomputed_ambiguity=sorted(recomputed),
        )

    # Which children are legal members is a question only the declared basis
    # answers, so this is scoped to that basis rather than to the union: a
    # presentation-only row named as a member of a declared calculation basis is
    # a member of nothing the compile will read.
    declared_basis_children = (
        calculation_children
        if request.basis_source == "calculation"
        else presentation_children
    )
    unknown = sorted(
        member.tag
        for member in request.member_lines
        if member.tag not in declared_basis_children
    )
    if unknown:
        return _rejection(
            "topology_basis_unavailable",
            (
                "declared member lines are not children of the section total on "
                f"FY{year}'s pinned {request.basis_source} basis: {unknown!r}"
            ),
            fiscal_year=year,
            reasons=[f"FY{year}:member_line_not_in_pin"],
            member_lines=unknown,
        )

    scoped = tuple(
        declaration
        for declaration in current_declarations
        if declaration.ticker == request.ticker
        and declaration.statement is request.statement
        and declaration.section == request.section
    )
    if request.supersedes_declaration_id is not None:
        target = next(
            (
                declaration
                for declaration in scoped
                if declaration.declaration_id == request.supersedes_declaration_id
            ),
            None,
        )
        if target is None:
            return _rejection(
                "supersession_conflict",
                (
                    f"supersedes_declaration_id {request.supersedes_declaration_id!r} "
                    "is unknown among the current lineage heads"
                ),
                target_declaration_id=request.supersedes_declaration_id,
                existing_successor=None,
            )
        successor = next(
            (
                declaration
                for declaration in scoped
                if declaration.supersedes_declaration_id
                == request.supersedes_declaration_id
            ),
            None,
        )
        if successor is not None:
            return _rejection(
                "supersession_conflict",
                (
                    f"declaration {request.supersedes_declaration_id!r} already has "
                    f"successor {successor.declaration_id!r}"
                ),
                target_declaration_id=request.supersedes_declaration_id,
                existing_successor=successor.declaration_id,
            )
    standing = next(
        (
            declaration
            for declaration in scoped
            if declaration.accession == request.accession
            and declaration.declaration_id != request.supersedes_declaration_id
        ),
        None,
    )
    if standing is not None:
        return _rejection(
            "supersession_conflict",
            (
                f"declaration {standing.declaration_id!r} already declares the basis "
                f"for accession {request.accession!r}"
            ),
            target_declaration_id=standing.declaration_id,
            existing_successor=None,
        )
    return DeclarationValidated(recomputed_ambiguity=recomputed)


# --------------------------------------------------------------------------
# §2.4 / §4.3 the break closure — `break_conflict` symmetry
# --------------------------------------------------------------------------


def validate_declared_break(
    request: DeclaredBreakRequest,
    window: MultiBasisWindow,
    current_links: Sequence[CorrespondenceLink],
    current_breaks: Sequence[DeclaredBasisBreak],
) -> BreakValidated | ProposalRejection:
    """Pure, total closure for a break admission (mechanism §2.4, §4.3).

    The symmetric half of the link closure's ``break_conflict``: a standing
    link and a standing break can never both resolve one row/boundary, checked
    from **both** directions. Matching is on the junction — the pair of
    adjacent spans the boundary joins — never on year equality, so a break
    cannot step one year past a standing link and validate anyway.

    Refusal order: junction first (``topology_basis_unavailable``), then the
    supersession lineage, then the standing-artifact axis (``break_conflict``).
    """

    if (
        window.ticker != request.ticker
        or window.statement is not request.statement
        or window.section != request.section
    ):
        raise ValueError(
            "closure requires the window for the request's ticker, statement "
            "and section"
        )

    junction = _break_junction(window, request.scope, request.boundary)
    if junction is None:
        scope_detail = (
            f"row_role {request.scope.row_role!r} is not bound in two adjacent spans"
            if isinstance(request.scope, RowBreak)
            else "the window has no adjacent segment pair"
        )
        return _rejection(
            "topology_basis_unavailable",
            (
                f"{scope_detail} straddling FY{request.boundary.before_fy} and "
                f"FY{request.boundary.after_fy}"
            ),
            row_role=(
                request.scope.row_role if isinstance(request.scope, RowBreak) else None
            ),
            reasons=["no_bound_span_junction"],
        )

    scoped_breaks = tuple(
        declared_break
        for declared_break in current_breaks
        if declared_break.ticker == request.ticker
        and declared_break.statement is request.statement
        and declared_break.section == request.section
    )
    if request.supersedes_break_id is not None:
        target = next(
            (
                declared_break
                for declared_break in scoped_breaks
                if declared_break.break_id == request.supersedes_break_id
            ),
            None,
        )
        if target is None:
            return _rejection(
                "supersession_conflict",
                (
                    f"supersedes_break_id {request.supersedes_break_id!r} is unknown "
                    "among the current lineage heads"
                ),
                target_break_id=request.supersedes_break_id,
                existing_successor=None,
            )
        successor = next(
            (
                declared_break
                for declared_break in scoped_breaks
                if declared_break.supersedes_break_id == request.supersedes_break_id
            ),
            None,
        )
        if successor is not None:
            return _rejection(
                "supersession_conflict",
                (
                    f"break {request.supersedes_break_id!r} already has successor "
                    f"{successor.break_id!r}"
                ),
                target_break_id=request.supersedes_break_id,
                existing_successor=successor.break_id,
            )

    for declared_break in scoped_breaks:
        if declared_break.break_id == request.supersedes_break_id:
            continue
        if not _resolutions_collide(
            window,
            scope_a=request.scope,
            boundary_a=request.boundary,
            scope_b=declared_break.scope,
            boundary_b=declared_break.boundary,
        ):
            continue
        return _rejection(
            "break_conflict",
            f"break {declared_break.break_id!r} already resolves this row/boundary",
            standing_artifact_id=declared_break.break_id,
            row_role=(
                request.scope.row_role if isinstance(request.scope, RowBreak) else None
            ),
            boundary=request.boundary.model_dump(mode="json"),
        )

    for link in current_links:
        if (
            link.ticker != request.ticker
            or link.statement is not request.statement
            or link.section != request.section
        ):
            continue
        if not _resolutions_collide(
            window,
            scope_a=request.scope,
            boundary_a=request.boundary,
            scope_b=RowBreak(row_role=link.row_role),
            boundary_b=SegmentBoundaryRef(
                before_fy=link.from_line.fiscal_year,
                after_fy=link.to_line.fiscal_year,
            ),
        ):
            continue
        return _rejection(
            "break_conflict",
            (
                f"link {link.link_id!r} already resolves this row/boundary: a link "
                "and a break can never both stand"
            ),
            standing_artifact_id=link.link_id,
            row_role=link.row_role,
            boundary=request.boundary.model_dump(mode="json"),
        )

    return BreakValidated(boundary=request.boundary)


def _scopes_overlap(left: BreakScope, right: BreakScope) -> bool:
    if isinstance(left, SectionRebasis) or isinstance(right, SectionRebasis):
        return True
    return left.row_role == right.row_role


def _resolutions_collide(
    window: MultiBasisWindow,
    *,
    scope_a: BreakScope,
    boundary_a: SegmentBoundaryRef,
    scope_b: BreakScope,
    boundary_b: SegmentBoundaryRef,
) -> bool:
    """Do two standing resolutions resolve the same row/boundary?

    Compared on the JUNCTION, never on year equality, and normalized onto the
    row axis whenever either side names a row — the same normalization the link
    closure uses, so a section rebasis and a row break at one junction are seen
    as the one resolution they are, and a break cannot step a year past a
    standing artifact and validate anyway.
    """

    if not _scopes_overlap(scope_a, scope_b):
        return False
    roles = tuple(
        scope.row_role
        for scope in (scope_a, scope_b)
        if isinstance(scope, RowBreak)
    )
    if roles:
        for role in dict.fromkeys(roles):
            first = _span_junction(
                window,
                row_role=role,
                before_fy=boundary_a.before_fy,
                after_fy=boundary_a.after_fy,
            )
            second = _span_junction(
                window,
                row_role=role,
                before_fy=boundary_b.before_fy,
                after_fy=boundary_b.after_fy,
            )
            if first is not None and first == second:
                return True
        return False
    first_segment = _segment_junction(
        window, before_fy=boundary_a.before_fy, after_fy=boundary_a.after_fy
    )
    second_segment = _segment_junction(
        window, before_fy=boundary_b.before_fy, after_fy=boundary_b.after_fy
    )
    return first_segment is not None and first_segment == second_segment


# --------------------------------------------------------------------------
# §5 the mints — the ONLY promotion path from a request to an admitted artifact
# --------------------------------------------------------------------------


def admitted_artifact_content_sha256(record: _AdmittedArtifact) -> str:
    """The canonical digest of an admitted artifact's content.

    One encoding, one digest, computed the same way by the mint and by the
    store — so "the row's digest is the digest of the row's bytes" is a check
    the storage layer can actually make, not an article of faith.
    """

    payload = record.model_dump(mode="json")
    payload.pop(record.identity_field, None)
    payload.pop("content_sha256", None)
    return correspondence_content_sha256(payload)


def _mint_admitted(model_cls: Any, payload: Mapping[str, Any]) -> Any:
    """Build the artifact, digest its own content, then name it after that digest."""

    id_field, prefix = model_cls.identity_field, model_cls.identity_prefix
    draft = model_cls(
        **{**payload, id_field: f"{prefix}-pending", "content_sha256": "0" * 64}
    )
    digest = admitted_artifact_content_sha256(draft)
    return model_cls(
        **{**payload, id_field: f"{prefix}-{digest[:24]}", "content_sha256": digest}
    )


def admit_correspondence_link(
    *,
    proposal: CorrespondenceProposal,
    validated: ProposalValidated,
    admission: AdmissionRecord,
    revision: int,
) -> CorrespondenceLink:
    """Promote a closure-validated proposal into an admitted link. Door only.

    ``drift_parts`` is taken from the closure result, never from a caller: the
    parts are a derived consequence of the two endpoints, and the artifact
    validator recomputes them again on the way in.
    """

    return _mint_admitted(
        CorrespondenceLink,
        {
            "ticker": proposal.ticker,
            "statement": proposal.statement,
            "section": proposal.section,
            "row_role": proposal.row_role,
            "from_line": proposal.from_line,
            "to_line": proposal.to_line,
            "drift_parts": validated.drift_parts,
            "evidence": proposal.evidence,
            "proposal_id": proposal.proposal_id,
            "supersedes_link_id": proposal.supersedes_link_id,
            "admission": admission,
            "revision": int(revision),
        },
    ).validate_current_admission()


def admit_basis_declaration(
    *,
    request: BasisDeclarationRequest,
    validated: DeclarationValidated,
    admission: AdmissionRecord,
    revision: int,
) -> BasisDeclaration:
    """Promote a closure-validated declaration request. Door only.

    The closure's recomputed ambiguity is deliberately **not** stored: it is a
    fact of the pin, recomputable at any time, and a stored copy could
    desynchronize from the filing it describes (P1/P3). What IS stored is the
    author's own ``basis_source`` and ``ambiguity`` — safe only because the
    closure has already refused any request whose declared kinds are not among
    the recomputed ones and whose declared basis the pin does not carry
    (``declaration_contradicts_pin``). Without that check this would be the
    unverified second copy the link mint refuses by taking ``drift_parts`` from
    the closure rather than from its caller.
    """

    del validated
    return _mint_admitted(
        BasisDeclaration,
        {
            "ticker": request.ticker,
            "statement": request.statement,
            "section": request.section,
            "accession": request.accession,
            "fiscal_year": request.fiscal_year,
            "section_total_tag": request.section_total_tag,
            "basis_source": request.basis_source,
            "member_lines": request.member_lines,
            "ambiguity": request.ambiguity,
            "evidence": request.evidence,
            "supersedes_declaration_id": request.supersedes_declaration_id,
            "admission": admission,
            "revision": int(revision),
        },
    )


def admit_declared_basis_break(
    *,
    request: DeclaredBreakRequest,
    validated: BreakValidated,
    admission: AdmissionRecord,
    revision: int,
) -> DeclaredBasisBreak:
    """Promote a closure-validated break request. Door only."""

    del validated
    return _mint_admitted(
        DeclaredBasisBreak,
        {
            "ticker": request.ticker,
            "statement": request.statement,
            "section": request.section,
            "scope": request.scope,
            "boundary": request.boundary,
            "reason": request.reason,
            "evidence": request.evidence,
            "supersedes_break_id": request.supersedes_break_id,
            "admission": admission,
            "revision": int(revision),
        },
    )



__all__ = [
    "AdmissionRecord",
    "admit_basis_declaration",
    "admitted_artifact_content_sha256",
    "admit_correspondence_link",
    "admit_declared_basis_break",
    "BasisAmbiguityKind",
    "BasisAmbiguityReason",
    "BasisDeclaration",
    "BasisDeclarationRequest",
    "BasisMemberLine",
    "BoundedText",
    "BreakScope",
    "BreakValidated",
    "CalcMembershipDetail",
    "CalculationMembership",
    "correspondence_content_sha256",
    "CorrespondenceEvidence",
    "CorrespondenceLink",
    "CorrespondenceProposal",
    "DeclarationValidated",
    "DeclaredBasisBreak",
    "DeclaredBreakRequest",
    "DEFERRED_CLOSURE_CODES",
    "derived_basis_ambiguity",
    "EvidenceDetail",
    "EvidenceKind",
    "EvidenceProducer",
    "HumanRationaleDetail",
    "IdentityComparison",
    "IdentityDrifted",
    "IdentityPart",
    "IdentityUnchanged",
    "line_ref_from_topology",
    "LineRefIssue",
    "LineRefIssueCode",
    "LineRefIssueReason",
    "OrderingDetail",
    "presentation_identity_unchanged",
    "PresentationLineRef",
    "ProposalClosure",
    "ProposalRejection",
    "ProposalRejectionCode",
    "ProposalValidated",
    "RowBreak",
    "SectionRebasis",
    "SegmentBoundaryRef",
    "TreePositionDetail",
    "validate_basis_declaration",
    "validate_correspondence_proposal",
    "validate_declared_break",
    "ValueContinuityDetail",
]
