"""Pure restatement-vintage selection over a declared evidence graph.

Current restatement and build-pipeline context lives in
``docs/reference/build-model-infrastructure-architecture-2026-08-04.md``;
the owner map is ``schema/README.md``.

Design rule R7: original and restated values for one period are two
essential facts, distinguishable by accession, linked by acyclic lineage;
**a build binds the latest restated vintage as of its cutoff through one
pure selection**. This module is that selection and nothing else — it
stores nothing, reads nothing ambient, and imports nothing from ``api/``
(schema-layer residence, matching `schema/build_required_lines.py`).

Three properties are the contract, and the owner tests
(`tests/schema/test_vintage_selection.py`) pin each one:

* **Selection is pure and total.** Every input lands in exactly one of
  ``SelectedVintage`` / ``VintageMissingForPeriod`` /
  ``VintageAmbiguousForPeriod`` / ``RestatementLineageInvalid`` — refusals
  are errors-as-values on total paths (design §5.5), never a silent empty
  and never a silently chosen candidate.
* **Cutoff is a declared input, never wall clock** (P2). Two cutoffs over
  one graph give two answers; a restatement that becomes available after
  the cutoff is invisible to that build by construction. New corpus
  content is a new corpus version and a new bundle identity on the next
  build — nothing mutates (design §7.3).
* **Both vintages always survive.** The bound vintage is a *selection over
  existing evidence rows*, never a new row: the superseded ancestors ride
  back on ``SelectedVintage.superseded``, and an unresolvable pair refuses
  with **both candidates retained** — SRC-07's conflict-retention branch,
  which this module concretizes only on its unambiguous side.

**Series identity is a function, not a key.** ``evidence_series_identity``
is the single authority for "these two rows are the same series and
period". It answers on the filing line (statement role + tag + dimension set)
and period, excluding accession so a later filing's restatement of the same line remains
the same series. The function is injected at every call site rather than
hard-called, which is the mechanical form of the design's identity-basis
coupling (§7.3): admitted correspondence can compose as same-series evidence
without changing selection.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import (
    Callable,
    ClassVar,
    Final,
    Iterable,
    Literal,
    Protocol,
    TYPE_CHECKING,
    TypeAlias,
)

if TYPE_CHECKING:
    from schema.provider_port.contracts import FilingDimensionRef


#: Repair targets (design §5.5 "recovery decision point" column). Each
#: refusal names the lane that can actually fix it; none is retryable.
REPAIR_TARGET_REPIN: Final[str] = (
    "corpus lane (re-pin: acquire the named period's filing bytes into the "
    "pinned corpus, then rebuild)"
)
REPAIR_TARGET_LINEAGE: Final[str] = (
    "corpus lane / producer contract (establish restatement lineage between "
    "the retained candidate accessions, or re-pin a single one)"
)
REPAIR_TARGET_PRODUCER: Final[str] = (
    "producer contract lane (repair the emitted restatement lineage; a "
    "malformed graph is never resolved by dropping an edge here)"
)
#: The one refusal whose repair is *caller*-side: selection was handed a
#: graph spanning more than one series for the period it was asked for.
#: No producer edit fixes it — the caller (B-5's per-(series, period) loop)
#: must scope the graph. Kept a distinct target because a refusal that names
#: a lane which cannot repair it is a mis-routed refusal.
REPAIR_TARGET_CALLER_SCOPE: Final[str] = (
    "calling consumer (scope the evidence graph to one series before "
    "selecting; selection is per (series, period) by contract)"
)


class FilingLineSeriesRef(Protocol):
    """The vintage-independent filing-line coordinates selection reads."""

    @property
    def statement_role(self) -> str: ...

    @property
    def tag(self) -> str: ...

    @property
    def dimensions(self) -> tuple[FilingDimensionRef, ...]: ...


class EvidenceSeriesIdentityRow(Protocol):
    """The minimal structural shape needed to identify a filing-line series."""

    @property
    def line(self) -> FilingLineSeriesRef: ...

    @property
    def period_id(self) -> str: ...


class VintageEvidenceRow(EvidenceSeriesIdentityRow, Protocol):
    """The structural shape selection reads off one evidence row."""

    @property
    def evidence_id(self) -> str: ...

    @property
    def source_id(self) -> str: ...

    @property
    def available_at(self) -> datetime: ...

    @property
    def restates_evidence_id(self) -> str | None: ...

@dataclass(frozen=True)
class SeriesIdentity:
    """The plane on which two evidence rows are the same series and period.

    ``series`` is the identity of the measured line; ``period`` is the
    period component, kept separate because selection is asked for one
    period of one series. Equality over the pair is the same-series test.
    """

    series: tuple[str, ...]
    period: str

    @property
    def key(self) -> tuple[str, ...]:
        """Flat filing-line key, including member coordinates when present."""

        return (*self.series, self.period)

    def __str__(self) -> str:  # pragma: no cover - display only
        return "/".join(self.key)


SeriesIdentityFn: TypeAlias = Callable[["VintageEvidenceRow"], SeriesIdentity]


def evidence_series_identity(row: EvidenceSeriesIdentityRow) -> SeriesIdentity:
    """The filing-line series identity of one evidence row.

    Accession is intentionally excluded: a restatement is a later filing
    vintage of the same role/tag/dimension series, not a new series. Consolidated
    keys keep their original spelling; only member series add a dimension component.
    """

    line = row.line
    if line.dimensions:
        series = (
            line.statement_role,
            line.tag,
            ";".join(
                f"{dimension.axis_qname}={dimension.member_qname}"
                for dimension in line.dimensions
            ),
        )
    else:
        series = (line.statement_role, line.tag)
    return SeriesIdentity(series=series, period=row.period_id)


@dataclass(frozen=True)
class VintageCandidate:
    """One vintage of one (series, period), as selection sees it.

    ``source_id`` is the pre-D handle for the vintage's accession: an
    evidence row names its source, and the accession itself lives on the
    resolved ``SourceRecord.endpoint_or_filing_id`` /
    ``HistoricalSourcePin.accession`` (plan §3.3). A pure function over
    evidence rows cannot resolve that table, so the refusals carry the
    handle and the bundle layer (B-5/B-6) renders the accession.
    """

    evidence_id: str
    source_id: str
    available_at: datetime
    restates_evidence_id: str | None

    def __str__(self) -> str:  # pragma: no cover - display only
        return f"{self.evidence_id} (source {self.source_id} @ {self.available_at.isoformat()})"


@dataclass(frozen=True)
class SelectedVintage:
    """The bound vintage: latest restated as of ``cutoff``.

    ``superseded`` carries every candidate the bound row restates, oldest
    first. Nothing is dropped — the ancestors stay addressable, which is
    what makes a restated arrival an *addition with ancestry* rather than a
    replacement (SAFE-03 composition, design §7.3).
    """

    code: ClassVar[str] = "vintage_selected"

    series: SeriesIdentity
    period: str
    cutoff: datetime
    bound: VintageCandidate
    superseded: tuple[VintageCandidate, ...]


class VintageSelectionRefusal:
    """Base of the three typed refusals — stable code, named repair target.

    Errors-as-values: refusals are *returned*, not raised, so selection is a
    total function; the bundle-production caller (B-5) is what turns one
    into a blocker.
    """

    code: ClassVar[str] = "vintage_selection_refused"
    _repair_target: ClassVar[str] = REPAIR_TARGET_PRODUCER

    @property
    def repair_target(self) -> str:
        return self._repair_target

    @property
    def message(self) -> str:  # pragma: no cover - overridden by each refusal
        raise NotImplementedError

    def __str__(self) -> str:
        return f"{self.code}: {self.message} [repair: {self.repair_target}]"


@dataclass(frozen=True)
class VintageMissingForPeriod(VintageSelectionRefusal):
    """No candidate vintage for a required period at or before the cutoff.

    ``known_after_cutoff`` is the honest other half: rows for the period
    that exist in the graph but only became available after the declared
    cutoff. Empty means the period is absent from the graph outright.

    ``series`` is ``None`` in exactly that outright-absent case, and only
    there. Series identity is derived by the **injected** identity function
    from an evidence row; when the requested period matches no row in the
    handed graph there is no row to derive it from, and a pure function may
    not invent one. The caller always knows which series it asked for — B-5
    must render that series when it turns this refusal into a blocker
    (plan §13 item 9(d)/9(f)); the refusal itself names the period, the
    declared cutoff, and the repair lane.
    """

    code: ClassVar[str] = "vintage_missing_for_period"
    _repair_target: ClassVar[str] = REPAIR_TARGET_REPIN

    series: SeriesIdentity | None
    period: str
    cutoff: datetime
    known_after_cutoff: tuple[VintageCandidate, ...]

    @property
    def message(self) -> str:
        series = "unknown series" if self.series is None else f"series {self.series}"
        if not self.known_after_cutoff:
            return (
                f"{series} period {self.period} has no evidence in the graph "
                f"at or before cutoff {self.cutoff.isoformat()}"
            )
        known = ", ".join(str(item) for item in self.known_after_cutoff)
        return (
            f"{series} period {self.period} has no evidence at or before "
            f"cutoff {self.cutoff.isoformat()}; {len(self.known_after_cutoff)} "
            f"post-cutoff row(s) known: {known}"
        )


@dataclass(frozen=True)
class VintageAmbiguousForPeriod(VintageSelectionRefusal):
    """Two or more candidates for one period with no lineage between them.

    Fails closed with **every candidate retained** (design §5.5, §7.3): the
    consumer never silently binds one accession, and never degrades to
    computing on the original. ``lineage_maximal`` is the subset nothing
    else restates — the pairs an operator or the producer must connect.
    """

    code: ClassVar[str] = "vintage_ambiguous_for_period"
    _repair_target: ClassVar[str] = REPAIR_TARGET_LINEAGE

    series: SeriesIdentity
    period: str
    cutoff: datetime
    candidates: tuple[VintageCandidate, ...]
    lineage_maximal: tuple[VintageCandidate, ...]

    @property
    def message(self) -> str:
        maximal = ", ".join(str(item) for item in self.lineage_maximal)
        return (
            f"series {self.series} period {self.period} has "
            f"{len(self.lineage_maximal)} unsuperseded candidates at or before "
            f"cutoff {self.cutoff.isoformat()} with no lineage between them: "
            f"{maximal}; all {len(self.candidates)} candidate(s) retained"
        )


#: The malformed-graph shapes selection refuses. Two names are not in the
#: plan's four-name enumeration (§4.3 "cycle / dangling / cross-series /
#: forward ancestry"), and both are recorded in the plan's §13 register:
#: ``duplicate_evidence_id``, because two rows under one evidence id make
#: every downstream answer arbitrary and a pure function may not pick one;
#: and ``graph_not_scoped_to_one_series``, which is the **caller**
#: precondition (§4.1's "per series" graph) rather than a producer-emitted
#: malformation — it is kept distinct from ``cross_series`` precisely so
#: the refusal can name the lane that can actually repair it.
LineageViolation: TypeAlias = Literal[
    "cycle",
    "dangling_ancestor",
    "cross_series",
    "forward_ancestry",
    "duplicate_evidence_id",
    "graph_not_scoped_to_one_series",
]

#: Recovery lane per violation (design §5.5's "recovery decision point").
#: Everything a producer emitted is repaired producer-side; the caller
#: precondition is repaired by the caller.
_REPAIR_TARGET_BY_VIOLATION: Final[dict[str, str]] = {
    "graph_not_scoped_to_one_series": REPAIR_TARGET_CALLER_SCOPE,
}


@dataclass(frozen=True)
class RestatementLineageInvalid(VintageSelectionRefusal):
    """A malformed lineage graph reached selection.

    Defense in depth behind the SourceBundle validator (plan §4.1): the
    validator seals whole bundles, while selection is a pure function usable
    on sub-graphs nothing has sealed. Repair of a *malformed graph* is
    always producer-side — this module never resolves one by dropping an
    edge. The single exception is ``graph_not_scoped_to_one_series``, whose
    repair is the caller's: the graph is not malformed, it is unscoped, and
    the refusal carries ``REPAIR_TARGET_CALLER_SCOPE`` so it never sends an
    operator to a lane with nothing to fix.
    """

    code: ClassVar[str] = "restatement_lineage_invalid"

    violation: LineageViolation
    evidence_ids: tuple[str, ...]
    detail: str

    @property
    def repair_target(self) -> str:
        return _REPAIR_TARGET_BY_VIOLATION.get(self.violation, REPAIR_TARGET_PRODUCER)

    @property
    def message(self) -> str:
        ids = ", ".join(self.evidence_ids)
        return f"{self.violation} on evidence [{ids}]: {self.detail}"


VintageSelectionOutcome: TypeAlias = (
    SelectedVintage
    | VintageMissingForPeriod
    | VintageAmbiguousForPeriod
    | RestatementLineageInvalid
)


def select_vintage(
    evidence_graph: Iterable[VintageEvidenceRow],
    period: str,
    cutoff: datetime,
    *,
    series_identity: SeriesIdentityFn,
) -> VintageSelectionOutcome:
    """Bind the latest restated vintage of ``period`` as of ``cutoff``.

    ``evidence_graph`` is the evidence rows of **one series** (all periods,
    all vintages); the restatement edges are the rows'
    ``restates_evidence_id`` fields. ``cutoff`` is a declared input — the
    pin's ``cutoff_at`` — never wall clock. ``series_identity`` is injected
    (Stage D re-keys that one function; §3.3).

    Two caller obligations, both fail-closed rather than assumed:

    * **The graph is scoped to one series.** A graph spanning two series
      *for the requested period* refuses
      ``restatement_lineage_invalid``/``graph_not_scoped_to_one_series``.
    * **Rivalry on the identity plane means rival vintages.** The plane is
      filing role + tag + period and deliberately does not distinguish
      provider lanes: two unlineaged rows for one filing line are two rival
      vintages and refuse ``vintage_ambiguous_for_period``. Callers scope
      selection to the requirement's bound evidence subset (or provide the
      lineage); selection never silently picks a provider.

    Total by construction; the four outcomes are exhaustive:

    * one lineage-maximal candidate → ``SelectedVintage`` (its ancestors
      retained on ``superseded``);
    * no candidate at or before ``cutoff`` → ``VintageMissingForPeriod``;
    * two or more unconnected candidates → ``VintageAmbiguousForPeriod``
      with all candidates retained;
    * a malformed graph → ``RestatementLineageInvalid``.
    """

    rows = tuple(evidence_graph)

    by_id: dict[str, VintageEvidenceRow] = {}
    duplicates: set[str] = set()
    for row in rows:
        if row.evidence_id in by_id:
            duplicates.add(row.evidence_id)
        by_id[row.evidence_id] = row
    if duplicates:
        return RestatementLineageInvalid(
            violation="duplicate_evidence_id",
            evidence_ids=tuple(sorted(duplicates)),
            detail="two or more evidence rows share one evidence id",
        )

    graph_refusal = _validate_restatement_edges(rows, by_id, series_identity)
    if graph_refusal is not None:
        return graph_refusal

    period_rows = tuple(
        row for row in rows if series_identity(row).period == period
    )
    series_keys = {series_identity(row).series for row in period_rows}
    if len(series_keys) > 1:
        return RestatementLineageInvalid(
            violation="graph_not_scoped_to_one_series",
            evidence_ids=_sorted_ids(period_rows),
            detail=(
                f"selection was asked for period {period} over a graph "
                f"spanning {len(series_keys)} series; scope the graph to one "
                "series before selecting"
            ),
        )

    if not period_rows:
        return VintageMissingForPeriod(
            series=None,
            period=period,
            cutoff=cutoff,
            known_after_cutoff=(),
        )

    series = series_identity(period_rows[0])
    # The cutoff is **inclusive** (§4.1: "with ``available_at <= cutoff``"),
    # and inclusivity is the dominant live case rather than a boundary
    # curiosity: the producer mints evidence with ``available_at =
    # retrieved_at`` and the bundle's ``cutoff_at`` *is* that retrieval
    # instant, so 403 of the 446 rows of the pinned v1 journey sit exactly
    # on the cutoff (plan §13 item 9(g)). A strict ``<`` here would refuse
    # `vintage_missing_for_period` on essentially every live period; the
    # owner tests pin both sides of the boundary.
    candidates = tuple(row for row in period_rows if row.available_at <= cutoff)
    if not candidates:
        return VintageMissingForPeriod(
            series=series,
            period=period,
            cutoff=cutoff,
            known_after_cutoff=_candidates(period_rows),
        )

    candidate_ids = {row.evidence_id for row in candidates}
    ancestors_by_id = {
        row.evidence_id: _ancestor_ids(row, by_id) & candidate_ids
        for row in candidates
    }

    for row in sorted(candidates, key=_ordering):
        others = candidate_ids - {row.evidence_id}
        if others <= ancestors_by_id[row.evidence_id]:
            return SelectedVintage(
                series=series,
                period=period,
                cutoff=cutoff,
                bound=_candidate(row),
                superseded=_candidates(
                    by_id[ancestor_id]
                    for ancestor_id in ancestors_by_id[row.evidence_id]
                ),
            )

    superseded_ids = {
        ancestor_id
        for ancestor_ids in ancestors_by_id.values()
        for ancestor_id in ancestor_ids
    }
    return VintageAmbiguousForPeriod(
        series=series,
        period=period,
        cutoff=cutoff,
        candidates=_candidates(candidates),
        lineage_maximal=_candidates(
            row for row in candidates if row.evidence_id not in superseded_ids
        ),
    )


def _validate_restatement_edges(
    rows: tuple[VintageEvidenceRow, ...],
    by_id: dict[str, VintageEvidenceRow],
    series_identity: SeriesIdentityFn,
) -> RestatementLineageInvalid | None:
    """Refuse dangling, cross-series, forward and cyclic restatement edges."""

    for row in sorted(rows, key=_ordering):
        ancestor_id = row.restates_evidence_id
        if ancestor_id is None:
            continue
        ancestor = by_id.get(ancestor_id)
        if ancestor is None:
            return RestatementLineageInvalid(
                violation="dangling_ancestor",
                evidence_ids=(row.evidence_id, ancestor_id),
                detail="restatement ancestor does not resolve in the graph",
            )
        if series_identity(ancestor) != series_identity(row):
            return RestatementLineageInvalid(
                violation="cross_series",
                evidence_ids=(row.evidence_id, ancestor_id),
                detail=(
                    "restatement ancestry requires identical series and "
                    f"period: {series_identity(row)} restates "
                    f"{series_identity(ancestor)}"
                ),
            )
        if ancestor.available_at > row.available_at:
            return RestatementLineageInvalid(
                violation="forward_ancestry",
                evidence_ids=(row.evidence_id, ancestor_id),
                detail=(
                    f"restatement ancestry cannot point forward: ancestor "
                    f"available {ancestor.available_at.isoformat()} > "
                    f"{row.available_at.isoformat()}"
                ),
            )

    visiting: list[str] = []
    visited: set[str] = set()
    for start in sorted(by_id):
        current: str | None = start
        while current is not None and current not in visited:
            if current in visiting:
                cycle = visiting[visiting.index(current) :]
                return RestatementLineageInvalid(
                    violation="cycle",
                    evidence_ids=tuple(cycle),
                    detail="restatement lineage must be acyclic",
                )
            visiting.append(current)
            current = by_id[current].restates_evidence_id
        visited.update(visiting)
        visiting.clear()
    return None


def _ancestor_ids(
    row: VintageEvidenceRow, by_id: dict[str, VintageEvidenceRow]
) -> set[str]:
    """Transitive restatement ancestry of one row (the graph is acyclic here)."""

    ancestors: set[str] = set()
    current = row.restates_evidence_id
    while current is not None and current not in ancestors:
        ancestors.add(current)
        current = by_id[current].restates_evidence_id
    return ancestors


def _candidate(row: VintageEvidenceRow) -> VintageCandidate:
    return VintageCandidate(
        evidence_id=row.evidence_id,
        source_id=str(row.source_id),
        available_at=row.available_at,
        restates_evidence_id=row.restates_evidence_id,
    )


def _candidates(rows: Iterable[VintageEvidenceRow]) -> tuple[VintageCandidate, ...]:
    """Deterministic candidate ordering: oldest first, then evidence id."""

    return tuple(_candidate(row) for row in sorted(rows, key=_ordering))


def _sorted_ids(rows: Iterable[VintageEvidenceRow]) -> tuple[str, ...]:
    return tuple(row.evidence_id for row in sorted(rows, key=_ordering))


def _ordering(row: VintageEvidenceRow) -> tuple[datetime, str]:
    return (row.available_at, row.evidence_id)


__all__ = [
    "LineageViolation",
    "REPAIR_TARGET_CALLER_SCOPE",
    "REPAIR_TARGET_LINEAGE",
    "REPAIR_TARGET_PRODUCER",
    "REPAIR_TARGET_REPIN",
    "RestatementLineageInvalid",
    "SelectedVintage",
    "SeriesIdentity",
    "SeriesIdentityFn",
    "VintageAmbiguousForPeriod",
    "EvidenceSeriesIdentityRow",
    "VintageCandidate",
    "VintageEvidenceRow",
    "VintageMissingForPeriod",
    "VintageSelectionOutcome",
    "VintageSelectionRefusal",
    "evidence_series_identity",
    "select_vintage",
]
