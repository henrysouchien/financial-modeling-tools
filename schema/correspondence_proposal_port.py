"""The correspondence proposal port (Stage C, step C-3).

The judgment boundary of design rule R6: *whether a drifted later line
continues an earlier line* is irreducible meaning. Code owns the three-part
comparison and everything structural; this module carries the residue across
the boundary and nothing else (mechanism §4).

Three properties this module exists to hold:

* **The authority rail is server-held.** Topologies, line refs, accessions, the
  window and the standing artifacts never leave the server. A proposer receives
  a materialized candidate context whose endpoints are *selectors*, and returns
  selector ids; ``compose_proposal`` re-mints both endpoints from the pinned
  trees via ``line_ref_from_topology``. Prose cannot smuggle an endpoint — an
  authored ``PresentationLineRef`` has no way in.
* **Features are advisory, never authority.** ``candidate_evidence`` computes
  tree position, calculation membership, ordering and value continuity over the
  pinned trees, each stamped with its producer and a confidence. Confidence
  orders the review queue; deterministic closure never reads it, and the
  admission door never reads it. No threshold admits anything.
* **Closure runs before any human sees a candidate.** ``compose_proposal``
  returns a proposal that already carries its closure terminal —
  ``validated`` or ``rejected(code)``. A candidate that cannot be structurally
  validated never reaches a person as if it could be.

What this module deliberately does **not** do: decide whether the drifted line
is *economically* the same row, and construct anything admitted. A proposal
that passes closure is ``validated``, not admitted; promotion needs the
admission door's authority (``schema/correspondence.py`` §5 mints).

Issuer-generic by construction (fourth amendment §6.1): no issuer identifier
appears here; issuer difference enters only as authored windows and fixtures.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Literal, Mapping, Sequence

from pydantic import BaseModel, ConfigDict, Field, field_validator
from .comparative_discrimination import (
    AsFiledLine,
    ComparativeCandidate,
    ComparativeConflict,
    ComparativeObservation,
    DeclaredPrecisionError,
    DriftAndRestatement,
    DriftCandidate,
    UnmatchedComparativeValue,
    comparative_lines,
    discriminate_comparative_observation,
    values_equal_at_declared_precision,
)

from .correspondence import (
    CalcMembershipDetail,
    CorrespondenceEvidence,
    CorrespondenceLink,
    CorrespondenceProposal,
    DeclaredBasisBreak,
    EvidenceProducer,
    IdentityUnchanged,
    LineRefIssue,
    OrderingDetail,
    PresentationLineRef,
    ProposalRejection,
    ProposalValidated,
    RowBreak,
    SectionRebasis,
    SegmentBoundaryRef,
    TreePositionDetail,
    ValueContinuityDetail,
    _canonical_dimensions,
    line_ref_from_topology,
    presentation_identity_unchanged,
    validate_correspondence_proposal,
)

# Deliberately imported rather than re-implemented: "which junction of the
# window is this boundary" has to have exactly one owner, or the queue and the
# closure it feeds disagree about what is already resolved.
from .correspondence import _bound_spans, _span_junction
from .multi_basis_window import (
    BasisSegment,
    MultiBasisWindow,
    SegmentRowBinding,
)
from .provider_port.contracts import FilingDimensionRef
from .statement_topology import StatementTopology, normalize_topology_role


class _PortContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _require_non_blank(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must be non-empty")
    return normalized


def _digest(payload: object) -> str:
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


# --------------------------------------------------------------------------
# the authority rail: server-minted selectors
# --------------------------------------------------------------------------


class LineSelector(_PortContract):
    """A server-minted handle for one line of one pinned filing.

    The proposer sees and returns ``selector_id`` only. The coordinates travel
    with it so a review surface can render the candidate, but they are not what
    the port trusts: ``compose_proposal`` re-mints the endpoint from the
    selector's own coordinates against the pinned trees, and a selector whose
    id does not match its coordinates is refused.
    """

    selector_id: str
    fiscal_year: int = Field(ge=1900, le=2200)
    statement_role: str
    tag: str
    dimensions: tuple[FilingDimensionRef, ...] = ()
    accession: str

    @field_validator("selector_id", "statement_role", "tag", "accession")
    @classmethod
    def _non_blank(cls, value: str, info) -> str:
        return _require_non_blank(value, field_name=info.field_name)

    @field_validator("dimensions")
    @classmethod
    def _normalize_dimensions(
        cls, value: tuple[FilingDimensionRef, ...]
    ) -> tuple[FilingDimensionRef, ...]:
        return _canonical_dimensions(value)

    @property
    def expected_selector_id(self) -> str:
        return mint_selector_id(
            fiscal_year=self.fiscal_year,
            statement_role=self.statement_role,
            tag=self.tag,
            dimensions=self.dimensions,
            accession=self.accession,
        )


def mint_selector_id(
    *,
    fiscal_year: int,
    statement_role: str,
    tag: str,
    accession: str,
    dimensions: tuple[FilingDimensionRef, ...] = (),
) -> str:
    """The one selector mint: a digest of the coordinates the server chose."""

    return "sel-" + _digest(
        {
            "fiscal_year": int(fiscal_year),
            "statement_role": normalize_topology_role(statement_role),
            "tag": str(tag),
            "dimensions": [
                {
                    "axis_qname": dimension.axis_qname,
                    "member_qname": dimension.member_qname,
                }
                for dimension in _canonical_dimensions(dimensions)
            ],
            "accession": str(accession),
        }
    )[:24]


def _selector_from_ref(ref: PresentationLineRef) -> LineSelector:
    return LineSelector(
        selector_id=mint_selector_id(
            fiscal_year=ref.fiscal_year,
            statement_role=ref.statement_role,
            tag=ref.tag,
            dimensions=ref.dimensions,
            accession=ref.accession,
        ),
        fiscal_year=ref.fiscal_year,
        statement_role=ref.statement_role,
        tag=ref.tag,
        dimensions=ref.dimensions,
        accession=ref.accession,
    )


# --------------------------------------------------------------------------
# surfacing the boundaries that actually need a judgment
# --------------------------------------------------------------------------


BoundaryIssueReason = Literal[
    "row_not_bound_in_two_adjacent_spans",
    "no_presented_year_in_span",
    "endpoint_not_mintable",
    "as_filed_value_unavailable",
]


class BoundaryIssue(_PortContract):
    """Typed refusal from candidate generation — errors-as-values on a total path.

    Candidate generation never guesses an endpoint. A span that presents the
    row in no year, or a pin that will not mint a ref, is reported with its
    repair target; it is not silently skipped and it is not defaulted to the
    span's first year.
    """

    row_role: str
    reason: BoundaryIssueReason
    fiscal_years: tuple[int, ...] = ()
    detail: str

    @property
    def repair_target(self) -> str:
        if self.reason == "row_not_bound_in_two_adjacent_spans":
            return "window-authoring lane: this row has no boundary to resolve"
        if self.reason == "no_presented_year_in_span":
            return (
                "corpus / prepare lane: the segment binds this row but no pinned "
                "year presents it"
            )
        if self.reason == "endpoint_not_mintable":
            return "prepare lane: repair the pin so the endpoint mints"
        return (
            "prepare lane: rebuild the frozen financials pin with the as-filed "
            "current-period value"
        )


class UnresolvedBoundary(_PortContract):
    """One row/boundary a person still has to decide, with its candidate context.

    ``from_selector`` is fixed — the last presented year of the earlier bound
    span is a fact of the window and the pin, not a choice. ``to_selectors``
    are the candidates in the first presented year of the later span: every
    reviewed presentation child of the section, so a proposer may claim any of
    them and is never limited to the one code would have guessed.
    """

    row_role: str
    boundary: SegmentBoundaryRef
    from_selector: LineSelector
    to_selectors: tuple[LineSelector, ...] = Field(min_length=1)


class ComparativeBoundaryObservation(_PortContract):
    """One closed discriminator outcome tied to the boundary that produced it."""

    row_role: str
    boundary: SegmentBoundaryRef
    observation: ComparativeObservation


class ComparativeBoundaryRefusal(_PortContract):
    """A typed comparative conflict; ambiguity never becomes a proposal."""

    row_role: str
    boundary: SegmentBoundaryRef
    conflict: ComparativeConflict


class ComparativeCandidateGeneration(_PortContract):
    """The routed result of comparative-aware proposal candidate generation."""

    proposal_candidates: tuple[UnresolvedBoundary, ...] = ()
    observations: tuple[ComparativeBoundaryObservation, ...] = ()
    refusals: tuple[ComparativeBoundaryRefusal, ...] = ()
    issues: tuple[BoundaryIssue, ...] = ()


def _bound_tag(segment: BasisSegment, row_role: str) -> str:
    binding = segment.rows[row_role]
    assert isinstance(binding, SegmentRowBinding)
    return binding.tag


def _mint(
    topologies: Mapping[int, StatementTopology],
    *,
    year: int,
    segment: BasisSegment,
    tag: str,
) -> PresentationLineRef | LineRefIssue | None:
    """Mint the ref for (year, tag) under the segment's reviewed presentation role.

    ``None`` means the pin has no such line at all in that year — the caller
    walks on to the next candidate year. A ``LineRefIssue`` means the line is
    there but the pin cannot yield an identity, which is a refusal, not a
    reason to keep walking.
    """

    topology = topologies.get(int(year))
    if topology is None:
        return None
    source_value = topology.source_value(tag, int(year))
    if source_value is None:
        # The pin does not carry this line in this year at all. That is the
        # honest "not presented here"; the caller walks on.
        return None
    accession = None
    if isinstance(source_value.source_ref, Mapping):
        current = source_value.source_ref.get("current")
        if isinstance(current, Mapping) and current.get("accession"):
            accession = str(current["accession"]).strip()
    if accession is None:
        # The line IS in the pin but the pin attributes it to no filing. That is
        # a pin defect, and it refuses with the constructor's own vocabulary —
        # walking on to another year would silently pick a different endpoint
        # than the one the caller asked about.
        return LineRefIssue(
            code="endpoint_not_in_topology",
            reason="accession_missing_from_pin",
            fiscal_year=int(year),
            statement_role=normalize_topology_role(
                segment.topology_roles.presentation_roles[0]
            ),
            tag=tag,
            detail=(
                f"the pin carries {tag!r} in FY{year} but attributes it to no "
                "filing accession"
            ),
        )
    for role in segment.topology_roles.presentation_roles:
        minted = line_ref_from_topology(
            topology,
            year=int(year),
            statement_role=role,
            tag=tag,
            accession=accession,
        )
        if isinstance(minted, PresentationLineRef):
            return minted
        if minted.reason != "line_absent":
            return minted
    return None

def _boundary_issue_from_line_ref(
    row_role: str, issue: LineRefIssue
) -> BoundaryIssue:
    return BoundaryIssue(
        row_role=row_role,
        reason="endpoint_not_mintable",
        fiscal_years=(issue.fiscal_year,),
        detail=f"{issue.code}/{issue.reason}: {issue.detail}",
    )


def _last_presented(
    topologies: Mapping[int, StatementTopology], segment: BasisSegment, row_role: str
) -> PresentationLineRef | LineRefIssue | None:
    tag = _bound_tag(segment, row_role)
    for year in reversed(segment.fiscal_years):
        minted = _mint(topologies, year=year, segment=segment, tag=tag)
        if minted is not None:
            return minted
    return None


def _first_presented(
    topologies: Mapping[int, StatementTopology], segment: BasisSegment, row_role: str
) -> PresentationLineRef | LineRefIssue | None:
    tag = _bound_tag(segment, row_role)
    for year in segment.fiscal_years:
        minted = _mint(topologies, year=year, segment=segment, tag=tag)
        if minted is not None:
            return minted
    return None


def _section_selectors(
    topologies: Mapping[int, StatementTopology], segment: BasisSegment, year: int
) -> tuple[LineSelector, ...]:
    topology = topologies.get(int(year))
    if topology is None:
        return ()
    selectors: dict[str, LineSelector] = {}
    for parent_tag in segment.topology_roles.presentation_parent_tags:
        for edge in topology.presentation_children_of(
            parent_tag,
            int(year),
            presentation_roles=segment.topology_roles.presentation_roles,
        ):
            minted = _mint(topologies, year=year, segment=segment, tag=edge.child_tag)
            if isinstance(minted, PresentationLineRef):
                selector = _selector_from_ref(minted)
                selectors[selector.selector_id] = selector
    return tuple(
        sorted(selectors.values(), key=lambda row: (row.tag, row.fiscal_year))
    )


def _standing_resolution(
    window: MultiBasisWindow,
    *,
    row_role: str,
    from_line: PresentationLineRef,
    to_line: PresentationLineRef,
    current_links: Sequence[CorrespondenceLink],
    current_breaks: Sequence[DeclaredBasisBreak],
) -> bool:
    """Is this row/boundary already resolved by a standing link or break?

    Decided on the JUNCTION, exactly as the closure decides it. A standing
    artifact may legitimately name different years across the same junction —
    the junction is a fact of the window's bound spans, the years are the
    author's — and comparing years would put an already-resolved boundary back
    in front of a person, who would then compose a proposal the door refuses.

    A *stale* link — one whose junction still matches but whose endpoint tags
    the window no longer binds — resolves nothing, so the boundary goes back in
    the queue for a person to supersede it.
    """

    standing = _standing_artifact_id(
        window,
        row_role=row_role,
        from_line=from_line,
        to_line=to_line,
        current_links=current_links,
        current_breaks=current_breaks,
    )
    return standing is not None and standing[0] != "stale_link"


def unresolved_boundaries(
    window: MultiBasisWindow,
    topologies: Mapping[int, StatementTopology],
    current_links: Sequence[CorrespondenceLink] = (),
    current_breaks: Sequence[DeclaredBasisBreak] = (),
) -> tuple[tuple[UnresolvedBoundary, ...], tuple[BoundaryIssue, ...]]:
    """Every row/boundary where continuity is neither derived nor already resolved.

    Returns the queue and the typed issues side by side. A boundary whose two
    endpoints are three-part identical is **not** in the queue: identity needs
    no artifact, so surfacing it would invite an artifact that closure would
    refuse with ``identity_no_drift``.
    """

    queue: list[UnresolvedBoundary] = []
    issues: list[BoundaryIssue] = []
    for row_role in window.required_roles:
        spans = _bound_spans(window, row_role)
        if len(spans) < 2:
            continue
        for earlier, later in zip(spans, spans[1:]):
            from_minted = _last_presented(topologies, earlier, row_role)
            to_minted = _first_presented(topologies, later, row_role)
            if from_minted is None or to_minted is None:
                empty = earlier if from_minted is None else later
                issues.append(
                    BoundaryIssue(
                        row_role=row_role,
                        reason="no_presented_year_in_span",
                        fiscal_years=empty.fiscal_years,
                        detail=(
                            f"row_role {row_role!r} is bound in FY{empty.from_fy}-"
                            f"FY{empty.through_fy} but the pin presents it in no year "
                            "of that span"
                        ),
                    )
                )
                continue
            if isinstance(from_minted, LineRefIssue):
                issues.append(_boundary_issue_from_line_ref(row_role, from_minted))
                continue
            if isinstance(to_minted, LineRefIssue):
                issues.append(_boundary_issue_from_line_ref(row_role, to_minted))
                continue
            if isinstance(
                presentation_identity_unchanged(from_minted, to_minted),
                IdentityUnchanged,
            ):
                continue
            if _standing_resolution(
                window,
                row_role=row_role,
                from_line=from_minted,
                to_line=to_minted,
                current_links=current_links,
                current_breaks=current_breaks,
            ):
                continue
            candidates = _section_selectors(topologies, later, to_minted.fiscal_year)
            if not candidates:
                issues.append(
                    BoundaryIssue(
                        row_role=row_role,
                        reason="endpoint_not_mintable",
                        fiscal_years=(to_minted.fiscal_year,),
                        detail=(
                            f"FY{to_minted.fiscal_year} presents no reviewed section "
                            "children to choose an endpoint from"
                        ),
                    )
                )
                continue
            queue.append(
                UnresolvedBoundary(
                    row_role=row_role,
                    boundary=SegmentBoundaryRef(
                        before_fy=from_minted.fiscal_year,
                        after_fy=to_minted.fiscal_year,
                    ),
                    from_selector=_selector_from_ref(from_minted),
                    to_selectors=candidates,
                )
            )
    return tuple(queue), tuple(issues)


def generate_comparative_candidates(
    window: MultiBasisWindow,
    topologies: Mapping[int, StatementTopology],
    financials_payloads_by_year: Mapping[int, Mapping[str, Any]],
    current_links: Sequence[CorrespondenceLink] = (),
    current_breaks: Sequence[DeclaredBasisBreak] = (),
) -> ComparativeCandidateGeneration:
    """Generate the proposal queue through the §6 comparative discriminator.

    The later filing's frozen payload supplies its comparative column. Every
    bound as-filed line is classified, including identity-stable lines: that is
    what lets an unequal value in the same slot become a recorded restatement
    terminal instead of disappearing behind identity continuation. Only drift
    arms enter the proposal queue; conflicts are typed refusals, and no arm
    constructs a ``CorrespondenceLink``.
    """

    queue: list[UnresolvedBoundary] = []
    observations: list[ComparativeBoundaryObservation] = []
    refusals: list[ComparativeBoundaryRefusal] = []
    issues: list[BoundaryIssue] = []

    folded_payloads = {
        int(year): comparative_lines(payload)
        for year, payload in financials_payloads_by_year.items()
    }
    for row_role in window.required_roles:
        spans = _bound_spans(window, row_role)
        for earlier, later in zip(spans, spans[1:]):
            from_minted = _last_presented(topologies, earlier, row_role)
            to_minted = _first_presented(topologies, later, row_role)
            if from_minted is None or to_minted is None:
                empty = earlier if from_minted is None else later
                issues.append(
                    BoundaryIssue(
                        row_role=row_role,
                        reason="no_presented_year_in_span",
                        fiscal_years=empty.fiscal_years,
                        detail=(
                            f"row_role {row_role!r} is bound in FY{empty.from_fy}-"
                            f"FY{empty.through_fy} but the pin presents it in no year "
                            "of that span"
                        ),
                    )
                )
                continue
            if isinstance(from_minted, LineRefIssue):
                issues.append(_boundary_issue_from_line_ref(row_role, from_minted))
                continue
            if isinstance(to_minted, LineRefIssue):
                issues.append(_boundary_issue_from_line_ref(row_role, to_minted))
                continue

            boundary_ref = SegmentBoundaryRef(
                before_fy=from_minted.fiscal_year,
                after_fy=to_minted.fiscal_year,
            )
            as_filed_cell = folded_payloads.get(from_minted.fiscal_year, {}).get(
                (normalize_topology_role(from_minted.statement_role), from_minted.tag)
            )
            if as_filed_cell is None or as_filed_cell.current_period_value is None:
                issues.append(
                    BoundaryIssue(
                        row_role=row_role,
                        reason="as_filed_value_unavailable",
                        fiscal_years=(from_minted.fiscal_year,),
                        detail=(
                            f"FY{from_minted.fiscal_year} {from_minted.tag!r} has no "
                            "current_period_value in the frozen financials payload"
                        ),
                    )
                )
                continue

            later_cells = folded_payloads.get(to_minted.fiscal_year, {})
            candidate_cell = later_cells.get(
                (normalize_topology_role(to_minted.statement_role), to_minted.tag)
            )
            if (
                candidate_cell is not None
                and to_minted.fiscal_year != from_minted.fiscal_year + 1
            ):
                # B's prior column is about B-1, not an older non-adjacent span.
                # Keep the slot observation and make the inapplicable value axis
                # explicitly silent rather than manufacturing a restatement.
                candidate_cell = candidate_cell.model_copy(
                    update={
                        "prior_period_value": None,
                        "prior_accession": None,
                    }
                )
            comparative_candidates = (
                (
                    ComparativeCandidate(
                        line=to_minted,
                        cell=candidate_cell,
                    ),
                )
                if candidate_cell is not None
                else ()
            )

            unmatched: list[UnmatchedComparativeValue] = []
            if candidate_cell is None:
                for selector in _section_selectors(
                    topologies, later, to_minted.fiscal_year
                ):
                    cell = later_cells.get(
                        (normalize_topology_role(selector.statement_role), selector.tag)
                    )
                    if (
                        cell is None
                        or cell.prior_period_value is None
                        or not cell.has_independent_prior_vintage
                    ):
                        continue
                    try:
                        reappears = values_equal_at_declared_precision(
                            as_filed_cell.current_period_value,
                            cell.prior_period_value,
                        )
                    except DeclaredPrecisionError:
                        reappears = False
                    if reappears:
                        unmatched.append(
                            UnmatchedComparativeValue(tag=selector.tag, cell=cell)
                        )

            observation = discriminate_comparative_observation(
                period=from_minted.fiscal_year,
                as_filed=AsFiledLine(
                    line=from_minted,
                    value=as_filed_cell.current_period_value,
                ),
                candidates=comparative_candidates,
                unmatched_comparative_values=tuple(unmatched),
            )
            if isinstance(observation, ComparativeConflict):
                refusals.append(
                    ComparativeBoundaryRefusal(
                        row_role=row_role,
                        boundary=boundary_ref,
                        conflict=observation,
                    )
                )
                continue

            observations.append(
                ComparativeBoundaryObservation(
                    row_role=row_role,
                    boundary=boundary_ref,
                    observation=observation,
                )
            )
            if not isinstance(observation, (DriftCandidate, DriftAndRestatement)):
                continue
            if _standing_resolution(
                window,
                row_role=row_role,
                from_line=from_minted,
                to_line=to_minted,
                current_links=current_links,
                current_breaks=current_breaks,
            ):
                continue
            selectors = _section_selectors(
                topologies, later, to_minted.fiscal_year
            )
            if not selectors:
                issues.append(
                    BoundaryIssue(
                        row_role=row_role,
                        reason="endpoint_not_mintable",
                        fiscal_years=(to_minted.fiscal_year,),
                        detail=(
                            f"FY{to_minted.fiscal_year} presents no reviewed section "
                            "children to choose an endpoint from"
                        ),
                    )
                )
                continue
            queue.append(
                UnresolvedBoundary(
                    row_role=row_role,
                    boundary=boundary_ref,
                    from_selector=_selector_from_ref(from_minted),
                    to_selectors=selectors,
                )
            )

    return ComparativeCandidateGeneration(
        proposal_candidates=tuple(queue),
        observations=tuple(observations),
        refusals=tuple(refusals),
        issues=tuple(issues),
    )


# --------------------------------------------------------------------------
# §4.2 candidate features — advisory evidence, never authority
# --------------------------------------------------------------------------


_PORT_PRODUCER = EvidenceProducer(kind="miner", id="correspondence-candidate-features")


def candidate_evidence(
    from_line: PresentationLineRef,
    to_line: PresentationLineRef,
    topologies: Mapping[int, StatementTopology],
) -> tuple[CorrespondenceEvidence, ...]:
    """The mechanism §4.2 feature set, computed over the pinned trees.

    Every feature is a fact of the two filings, stamped with its producer and a
    confidence. Confidence is a queue ordering, never a threshold: nothing here
    is read by closure or by the door.

    A feature whose inputs are absent is **omitted**, never emitted with a
    defaulted value — an absent value is not evidence of continuity.
    """

    evidence: list[CorrespondenceEvidence] = []
    same_parent = from_line.presentation_parent_tag == to_line.presentation_parent_tag
    evidence.append(
        CorrespondenceEvidence(
            kind="tree_position",
            detail=TreePositionDetail(
                from_parent_tag=from_line.presentation_parent_tag,
                to_parent_tag=to_line.presentation_parent_tag,
            ),
            produced_by=_PORT_PRODUCER,
            confidence=0.8 if same_parent else 0.2,
        )
    )

    from_calc = from_line.calculation
    to_calc = to_line.calculation
    if from_calc is not None and to_calc is not None:
        same_calc_parent = from_calc.parent_tag == to_calc.parent_tag
        same_weight = from_calc.weight == to_calc.weight
        evidence.append(
            CorrespondenceEvidence(
                kind="calculation_membership",
                detail=CalcMembershipDetail(
                    from_membership=from_calc,
                    to_membership=to_calc,
                ),
                produced_by=_PORT_PRODUCER,
                confidence=0.9 if same_calc_parent and same_weight else 0.2,
            )
        )

    from_order = _presentation_order(topologies, from_line)
    to_order = _presentation_order(topologies, to_line)
    if from_order is not None and to_order is not None:
        evidence.append(
            CorrespondenceEvidence(
                kind="ordering",
                detail=OrderingDetail(from_order=from_order, to_order=to_order),
                produced_by=_PORT_PRODUCER,
                confidence=0.5 if abs(from_order - to_order) <= 1.0 else 0.1,
            )
        )

    from_value = _value(topologies, from_line)
    to_value = _value(topologies, to_line)
    if from_value is not None and to_value is not None:
        evidence.append(
            CorrespondenceEvidence(
                kind="value_continuity",
                detail=ValueContinuityDetail(
                    from_fiscal_year=from_line.fiscal_year,
                    to_fiscal_year=to_line.fiscal_year,
                    from_value=from_value,
                    to_value=to_value,
                ),
                produced_by=_PORT_PRODUCER,
                confidence=0.3,
            )
        )
    return tuple(evidence)


def _presentation_order(
    topologies: Mapping[int, StatementTopology], ref: PresentationLineRef
) -> float | None:
    topology = topologies.get(ref.fiscal_year)
    if topology is None:
        return None
    edge = topology.relationship(
        ref.presentation_parent_tag,
        ref.tag,
        ref.fiscal_year,
        edge_kind="presentation",
        role=ref.statement_role,
    )
    return None if edge is None else float(edge.order)


def _value(
    topologies: Mapping[int, StatementTopology], ref: PresentationLineRef
) -> float | None:
    topology = topologies.get(ref.fiscal_year)
    if topology is None:
        return None
    source_value = topology.source_value(ref.tag, ref.fiscal_year)
    if source_value is None:
        return None
    return source_value.normalized_value


def candidate_confidence(evidence: Sequence[CorrespondenceEvidence]) -> float:
    """The queue ordering. Advisory: no threshold admits anything, ever."""

    if not evidence:
        raise ValueError("a candidate needs at least one feature to be ordered")
    return sum(row.confidence for row in evidence) / len(evidence)


# --------------------------------------------------------------------------
# §4.3 composing a proposal — closure runs before any human sees it
# --------------------------------------------------------------------------


class SelectorIssue(_PortContract):
    """The selector rail's typed refusal. A bad selector never becomes an endpoint."""

    code: Literal["selector_unknown", "selector_forged", "selector_not_mintable"]
    selector_id: str
    detail: str

    @property
    def repair_target(self) -> str:
        if self.code == "selector_forged":
            return (
                "proposal lane: return a selector id the server minted; the "
                "coordinates do not digest to it"
            )
        if self.code == "selector_unknown":
            return "proposal lane: choose a selector from this boundary's context"
        return "prepare lane: repair the pin so the selected line mints"


def compose_proposal(
    boundary: UnresolvedBoundary,
    *,
    window: MultiBasisWindow,
    topologies: Mapping[int, StatementTopology],
    current_links: Sequence[CorrespondenceLink],
    current_breaks: Sequence[DeclaredBasisBreak],
    to_selector_id: str,
    proposed_by: EvidenceProducer,
    rationale: CorrespondenceEvidence | None = None,
    supersedes_link_id: str | None = None,
    proposal_id: str | None = None,
) -> tuple[CorrespondenceProposal, ProposalValidated | ProposalRejection] | SelectorIssue:
    """Mint both endpoints server-side, compose the features, and run closure.

    The proposer supplies a **selector id** and a semantic composition (who
    proposed, an optional human rationale). It never supplies an endpoint: both
    refs are re-minted here from the pinned trees, so a proposal cannot name a
    line the filing does not present.

    Returns the proposal together with its closure outcome. The proposal's
    ``status`` is the terminal that outcome names — ``validated`` or
    ``rejected`` with the code and its carried data — so a candidate reaches a
    person already checked, and a rejected candidate is a recorded terminal
    rather than a dangling open.
    """

    selector = next(
        (row for row in boundary.to_selectors if row.selector_id == to_selector_id),
        None,
    )
    if selector is None:
        return SelectorIssue(
            code="selector_unknown",
            selector_id=str(to_selector_id),
            detail="the selector is not one of this boundary's candidates",
        )
    if selector.selector_id != selector.expected_selector_id:
        return SelectorIssue(
            code="selector_forged",
            selector_id=selector.selector_id,
            detail="selector coordinates do not digest to the selector id",
        )
    if boundary.from_selector.selector_id != boundary.from_selector.expected_selector_id:
        return SelectorIssue(
            code="selector_forged",
            selector_id=boundary.from_selector.selector_id,
            detail="from-selector coordinates do not digest to the selector id",
        )

    minted: dict[str, PresentationLineRef] = {}
    for label, candidate in (
        ("from_line", boundary.from_selector),
        ("to_line", selector),
    ):
        topology = topologies.get(candidate.fiscal_year)
        if topology is None:
            return SelectorIssue(
                code="selector_not_mintable",
                selector_id=candidate.selector_id,
                detail=f"{label}: no pinned topology for FY{candidate.fiscal_year}",
            )
        ref = line_ref_from_topology(
            topology,
            year=candidate.fiscal_year,
            statement_role=candidate.statement_role,
            tag=candidate.tag,
            accession=candidate.accession,
        )
        if isinstance(ref, LineRefIssue):
            return SelectorIssue(
                code="selector_not_mintable",
                selector_id=candidate.selector_id,
                detail=f"{label}: {ref.code}/{ref.reason}: {ref.detail}",
            )
        if ref.dimensions != candidate.dimensions:
            return SelectorIssue(
                code="selector_not_mintable",
                selector_id=candidate.selector_id,
                detail=f"{label}: the pinned tree mints a different dimension set",
            )
        minted[label] = ref

    evidence = list(
        candidate_evidence(minted["from_line"], minted["to_line"], topologies)
    )
    if rationale is not None:
        if rationale.kind != "human_rationale":
            raise ValueError("rationale evidence must be of kind human_rationale")
        evidence.append(rationale)
    features = tuple(evidence)
    confidence = candidate_confidence(features)

    identity = {
        "window_id": window.window_id,
        "row_role": boundary.row_role,
        "from_selector_id": boundary.from_selector.selector_id,
        "to_selector_id": selector.selector_id,
    }
    resolved_proposal_id = proposal_id or ("proposal-" + _digest(identity)[:24])

    draft = CorrespondenceProposal(
        proposal_id=resolved_proposal_id,
        ticker=window.ticker,
        statement=window.statement,
        section=window.section,
        row_role=boundary.row_role,
        from_line=minted["from_line"],
        to_line=minted["to_line"],
        supersedes_link_id=supersedes_link_id,
        evidence=features,
        confidence=confidence,
        proposed_by=proposed_by,
        status="proposed",
    )
    closure = validate_correspondence_proposal(
        draft, topologies, window, current_links, current_breaks
    )
    if isinstance(closure, ProposalValidated):
        return draft.model_copy(update={"status": "validated"}), closure
    return (
        draft.model_copy(update={"status": "rejected", "rejection": closure}),
        closure,
    )


def span_boundary_endpoints(
    window: MultiBasisWindow,
    topologies: Mapping[int, StatementTopology],
    row_role: str,
) -> tuple[
    tuple[
        PresentationLineRef | LineRefIssue | None,
        PresentationLineRef | LineRefIssue | None,
    ],
    ...,
]:
    """The endpoint pair for each adjacent bound-span junction of one row.

    The earlier endpoint is the last presented year of the earlier span, the
    later one the first presented year of the later span — mechanism §3's
    boundary semantics, computed from the pin rather than assumed from the span
    edges. ``None`` means the span presents the row in no year; a
    ``LineRefIssue`` means the pin will not yield an identity. Neither is
    silently turned into an endpoint.
    """

    spans = _bound_spans(window, row_role)
    return tuple(
    (
      _last_presented(topologies, earlier, row_role),
      _first_presented(topologies, later, row_role),
    )
    for earlier, later in zip(spans, spans[1:])
    )




class RowBoundaryResolution(_PortContract):
    """How ONE row/boundary is resolved today — the answer the compile acts on.

    Landed by step C-4 as the single resolver behind both the read-only preview
    surface and the compile boundary pass (plan §4.3, §6.2 "the same typed
    window artifacts drive both"). Before C-4 the preview carried its own copy
    that matched standing links on *year equality* while the closure and the
    queue matched on the bound-span junction; the two disagreed for any
    artifact whose declared years are not the span's presented edges, and the
    preview would then show "unlinked" for a boundary the door has already
    resolved. One resolver, one answer.
    """

    row_role: str
    before_fy: int
    after_fy: int
    #: ``identity`` needs no artifact; ``link``/``break`` name the admitted one;
    #: ``unlinked`` is the state that fails the compile with
    #: ``correspondence_unlinked_required_year``.
    resolution: Literal["identity", "link", "break", "unlinked"]
    artifact_id: str | None = None
    drift_parts: tuple[str, ...] = ()
    #: Set only on ``unlinked``: an admitted link stands over this junction but
    #: its endpoints are no longer the ones the compile mints, so it resolves
    #: nothing. Carried so the refusal can name the artifact to supersede
    #: instead of reading as "nobody ever looked at this boundary".
    stale_artifact_id: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "row_role": self.row_role,
            "before_fy": self.before_fy,
            "after_fy": self.after_fy,
            "resolution": self.resolution,
            "artifact_id": self.artifact_id,
            "drift_parts": list(self.drift_parts),
            "stale_artifact_id": self.stale_artifact_id,
        }


def _standing_artifact_id(
    window: MultiBasisWindow,
    *,
    row_role: str,
    from_line: PresentationLineRef,
    to_line: PresentationLineRef,
    current_links: Sequence[CorrespondenceLink],
    current_breaks: Sequence[DeclaredBasisBreak],
) -> tuple[Literal["link", "break", "stale_link"], str] | None:
    """``(kind, artifact_id)`` of the standing resolution over this junction.

    ``kind`` is ``"link"``, ``"break"``, or ``"stale_link"`` — the last meaning
    an admitted link stands over this junction but does **not** resolve it.

    Two conditions have to hold before an admitted artifact answers for a
    boundary, and only the first is a fact of the window's structure:

    1. the artifact straddles this bound-span *junction* (years are the
       author's, the junction is not — so the year tolerance here is
       deliberate, and is what lets one link resolve a junction it names by
       different years than the compile mints);
    2. for a link, its endpoints are still the ones being joined. Plan §4.3
       requires "a human-approved link **whose endpoints match**", and the
       admission door enforces exactly that at admission time
       (``correspondence.py``'s ``_row_binding_tag(...) != ref.tag`` arm). The
       window, though, is authored data that revises independently of the
       correspondence store: rebind a role's later segment to a different tag
       and every byte-identical standing link still straddles the same
       junction. Without (2) that link would admit a correspondence between two
       lines no person ever saw — the compile silently answering a question the
       approval never asked. The tag is the part the window binds and the part
       the door pinned, and it is constant across a span, so comparing it keeps
       (1)'s tolerance intact while closing that hole.
       Dimension sets must also agree: a member link cannot resolve a
       consolidated boundary sharing the same tag.

    A ``DeclaredBasisBreak`` carries no endpoints — it is the refusal of a
    correspondence, not a claim about two lines — so (2) has nothing to compare
    and breaks match on the junction alone.
    """

    junction = _span_junction(
        window,
        row_role=row_role,
        before_fy=from_line.fiscal_year,
        after_fy=to_line.fiscal_year,
    )
    if junction is None:
        return None
    for link in current_links:
        if (
            link.ticker == window.ticker
            and link.statement is window.statement
            and link.section == window.section
            and link.row_role == row_role
            and _span_junction(
                window,
                row_role=row_role,
                before_fy=link.from_line.fiscal_year,
                after_fy=link.to_line.fiscal_year,
            )
            == junction
        ):
            if (
                link.from_line.tag != from_line.tag
                or link.to_line.tag != to_line.tag
                or link.from_line.dimensions != from_line.dimensions
                or link.to_line.dimensions != to_line.dimensions
            ):
                return ("stale_link", link.link_id)
            return ("link", link.link_id)
    for declared_break in current_breaks:
        if (
            declared_break.ticker != window.ticker
            or declared_break.statement is not window.statement
            or declared_break.section != window.section
        ):
            continue
        scope = declared_break.scope
        if isinstance(scope, RowBreak) and scope.row_role != row_role:
            continue
        if not isinstance(scope, (RowBreak, SectionRebasis)):  # pragma: no cover
            continue
        if (
            _span_junction(
                window,
                row_role=row_role,
                before_fy=declared_break.boundary.before_fy,
                after_fy=declared_break.boundary.after_fy,
            )
            == junction
        ):
            return ("break", declared_break.break_id)
    return None


def resolve_row_boundaries(
    window: MultiBasisWindow,
    topologies: Mapping[int, StatementTopology],
    current_links: Sequence[CorrespondenceLink] = (),
    current_breaks: Sequence[DeclaredBasisBreak] = (),
) -> tuple[tuple[RowBoundaryResolution, ...], tuple[BoundaryIssue, ...]]:
    """Every adjacent bound-span junction of every required role, decided once.

    Decision order is exhaustive and is the mechanism's, not a preference:
    identity ⇒ continue; drift ⇒ a standing break over the *junction*, or a
    standing link over that junction **whose endpoints are the ones being
    joined** (plan §4.3); neither ⇒ ``unlinked``. A junction whose endpoints
    will not mint is an issue with its repair target — never resolved by
    guessing an endpoint, and never silently skipped.

    A link that straddles the junction but no longer names its endpoints leaves
    the boundary ``unlinked`` and is reported as ``stale_artifact_id``, so the
    refusal names the artifact to supersede.
    """

    resolutions: list[RowBoundaryResolution] = []
    issues: list[BoundaryIssue] = []
    for row_role in window.required_roles:
        spans = _bound_spans(window, row_role)
        if len(spans) < 2:
            continue
        for earlier, later in zip(spans, spans[1:]):
            from_minted = _last_presented(topologies, earlier, row_role)
            to_minted = _first_presented(topologies, later, row_role)
            if from_minted is None or to_minted is None:
                empty = earlier if from_minted is None else later
                issues.append(
                    BoundaryIssue(
                        row_role=row_role,
                        reason="no_presented_year_in_span",
                        fiscal_years=empty.fiscal_years,
                        detail=(
                            f"row_role {row_role!r} is bound in FY{empty.from_fy}-"
                            f"FY{empty.through_fy} but the pin presents it in no year "
                            "of that span"
                        ),
                    )
                )
                continue
            if isinstance(from_minted, LineRefIssue):
                issues.append(_boundary_issue_from_line_ref(row_role, from_minted))
                continue
            if isinstance(to_minted, LineRefIssue):
                issues.append(_boundary_issue_from_line_ref(row_role, to_minted))
                continue
            comparison = presentation_identity_unchanged(from_minted, to_minted)
            if isinstance(comparison, IdentityUnchanged):
                resolutions.append(
                    RowBoundaryResolution(
                        row_role=row_role,
                        before_fy=from_minted.fiscal_year,
                        after_fy=to_minted.fiscal_year,
                        resolution="identity",
                    )
                )
                continue
            standing = _standing_artifact_id(
                window,
                row_role=row_role,
                from_line=from_minted,
                to_line=to_minted,
                current_links=current_links,
                current_breaks=current_breaks,
            )
            resolution: Literal["link", "break", "unlinked"]
            artifact_id: str | None
            stale_artifact_id: str | None
            if standing is None:
                resolution = "unlinked"
                artifact_id = None
                stale_artifact_id = None
            elif standing[0] == "stale_link":
                resolution = "unlinked"
                artifact_id = None
                stale_artifact_id = standing[1]
            else:
                resolution = standing[0]
                artifact_id = standing[1]
                stale_artifact_id = None
            resolutions.append(
                RowBoundaryResolution(
                    row_role=row_role,
                    before_fy=from_minted.fiscal_year,
                    after_fy=to_minted.fiscal_year,
                    resolution=resolution,
                    artifact_id=artifact_id,
                    drift_parts=tuple(sorted(comparison.parts)),
                    stale_artifact_id=stale_artifact_id,
                )
            )
    return tuple(resolutions), tuple(issues)


def review_queue(
    proposals: Sequence[CorrespondenceProposal],
) -> tuple[CorrespondenceProposal, ...]:
    """Validated candidates, highest confidence first. Ordering only, never authority."""

    return tuple(
        sorted(
            (row for row in proposals if row.status == "validated"),
            key=lambda row: (-row.confidence, row.proposal_id),
        )
    )


__all__ = [
    "BoundaryIssue",
    "ComparativeBoundaryObservation",
    "ComparativeBoundaryRefusal",
    "ComparativeCandidateGeneration",
    "RowBoundaryResolution",
    "BoundaryIssueReason",
    "LineSelector",
    "SelectorIssue",
    "UnresolvedBoundary",
    "candidate_confidence",
    "candidate_evidence",
    "compose_proposal",
    "generate_comparative_candidates",
    "mint_selector_id",
    "resolve_row_boundaries",
    "review_queue",
    "span_boundary_endpoints",
    "unresolved_boundaries",
]
