"""Read-only topology preview for candidate statement windows.

Two surfaces, one discipline — check a candidate against the pin, persist
nothing: the candidate-window pre-population preview (successor of the v1
authored-mapping preview, retired with its substrate at step C-4) and the
correspondence boundary preview.

Both read the SAME serialized artifacts the compile reads, and — since step
C-4 — the boundary view answers through the same resolver the compile uses
(``resolve_row_boundaries``). One serialization per contract, one resolver per
question: a boundary the preview shows as resolved is a boundary the compile
will not refuse.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping, Sequence

from .build_statement_window_validation import (
    validate_statement_window_pre_population,
)
from .build_statement_windows import compile_statement_window_build_plan
from .overrides import (
    TickerOverrides,
    TickerOverridesSnapshot,
    assert_ticker_overrides_snapshot_current,
    load_ticker_overrides_snapshot,
    replace_ticker_overrides,
    _runtime_provider_registry,
)
from .statement_topology import (
    build_statement_topology,
    freeze_payloads_by_year,
)
from .statement_windows import (
    StatementWindowsResult,
    compile_statement_windows,
    parse_statement_windows,
    serialize_statement_windows,
    window_requested_years,
)

if TYPE_CHECKING:
    from .correspondence import CorrespondenceLink, DeclaredBasisBreak
    from .correspondence_proposal_port import RowBoundaryResolution
    from .multi_basis_window import MultiBasisWindow
    from .provider_port.registry import ProviderRegistry


@dataclass(frozen=True)
class StatementWindowPreview:
    ticker: str
    store_hash: str
    base_payload_hash: str
    historical_years: tuple[int, ...]
    source_payloads_fingerprint: str
    submitted_window_ids: tuple[str, ...]
    candidate_statement_windows: dict[str, dict[str, Any]]
    result: StatementWindowsResult
    provider_registry: ProviderRegistry | None = field(
        default=None,
        repr=False,
        compare=False,
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "expected_store_hash": self.store_hash,
            "base_payload_hash": self.base_payload_hash,
            "historical_years": list(self.historical_years),
            "source_payloads_fingerprint": self.source_payloads_fingerprint,
            "submitted_window_ids": list(self.submitted_window_ids),
            "candidate_statement_windows": self.candidate_statement_windows,
            "statement_windows": self.result.to_dict(),
            "validation_status": (
                "fail" if self.result.enforcement_status == "fail" else "pass"
            ),
            "validation_stage": "pre_population",
        }




def preview_statement_windows(
    *,
    ticker: str,
    statement_windows: Mapping[str, Any],
    most_recent_fy: int,
    n_historical: int,
    n_projection: int,
    financials_payloads_by_year: Mapping[int, Mapping[str, Any]],
    overrides_dir: Path | None = None,
    snapshot: TickerOverridesSnapshot | None = None,
    provider_registry: ProviderRegistry | None = None,
) -> StatementWindowPreview:
    """Compile and topology-check a candidate without population or persistence."""

    ticker_upper = str(ticker or "").strip().upper()
    if not ticker_upper:
        raise ValueError("ticker is required")
    if int(n_historical) <= 0:
        raise ValueError("n_historical must be positive")
    if int(n_projection) <= 0:
        raise ValueError("n_projection must be positive")
    snapshot_registry = (
        _runtime_provider_registry(snapshot) if snapshot is not None else None
    )
    active_registry = (
        provider_registry if provider_registry is not None else snapshot_registry
    )
    snapshot_kwargs: dict[str, Any] = {}
    if active_registry is not None:
        snapshot_kwargs["provider_registry"] = active_registry
    active_snapshot = snapshot or load_ticker_overrides_snapshot(
        ticker_upper,
        overrides_dir,
        **snapshot_kwargs,
    )
    if active_snapshot.ticker != ticker_upper:
        raise ValueError(
            "Ticker override snapshot belongs to "
            f"{active_snapshot.ticker}, not {ticker_upper}"
        )
    if active_registry is None:
        active_registry = _runtime_provider_registry(active_snapshot)
    parsed = parse_statement_windows(
        statement_windows,
        provider_registry=active_registry,
    )
    if not parsed:
        raise ValueError("statement_windows must contain at least one window")

    existing = active_snapshot.overrides or TickerOverrides(
        ticker=active_snapshot.ticker,
        overrides={},
        file_meta={"ticker": active_snapshot.ticker, "schema_version": "1"},
        provider_registry=active_registry,
    )
    combined = {**existing.statement_windows, **parsed}
    candidate = replace_ticker_overrides(existing, statement_windows=combined)
    compiled = compile_statement_windows(
        ticker=active_snapshot.ticker,
        statement_windows=combined,
        existing_semantic_rows=candidate.semantic_rows,
        source_store_hash=active_snapshot.store_hash,
        provider_registry=active_registry,
    )

    historical_years = tuple(
        range(
            int(most_recent_fy) - int(n_historical) + 1,
            int(most_recent_fy) + 1,
        )
    )
    for section_plan in compiled.section_plans.values():
        window_requested_years(section_plan.window, historical_years)

    frozen_payloads = freeze_payloads_by_year(financials_payloads_by_year)
    missing_years = sorted(set(historical_years) - set(frozen_payloads))
    if missing_years:
        raise ValueError(
            "statement window preview is missing raw financial payload years "
            f"{missing_years}"
        )
    topology = build_statement_topology(frozen_payloads)

    from .build import load_template
    from .build_time_structure import remap_time_structure

    model = load_template()
    remap_time_structure(
        model,
        most_recent_fy=int(most_recent_fy),
        n_historical=int(n_historical),
        n_projection=int(n_projection),
    )
    build_plan = compile_statement_window_build_plan(
        ticker=active_snapshot.ticker,
        model=model,
        compiled_statement_windows=compiled,
    )
    result = validate_statement_window_pre_population(
        compiled_statement_windows=compiled,
        build_plan=build_plan,
        statement_topology=topology,
    )
    assert_ticker_overrides_snapshot_current(active_snapshot)
    return StatementWindowPreview(
        ticker=active_snapshot.ticker,
        store_hash=active_snapshot.store_hash,
        base_payload_hash=active_snapshot.payload_hash,
        historical_years=historical_years,
        source_payloads_fingerprint=topology.source_payloads_fingerprint,
        submitted_window_ids=tuple(sorted(parsed)),
        candidate_statement_windows=serialize_statement_windows(
            combined,
            provider_registry=active_registry,
        ),
        result=result,
        provider_registry=active_registry,
    )


# --------------------------------------------------------------------------
# Stage C (step C-3): the candidate window / correspondence preview
#
# The read-only half of the correspondence lane. The same typed artifacts drive
# this preview and (from step C-4) the compile — one serialization per contract,
# no hand-fabricated seam fixture between producer and consumer.
#
# Read-only and isolated by construction: it takes frozen payloads and a
# candidate window as arguments, touches no override store, and persists
# nothing. Fingerprint-bound: the result carries the fingerprint of the payloads
# it was computed from, so a caller can prove the answer belongs to the pin it
# asked about.
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class CorrespondenceCandidatePreview:
    ticker: str
    window_id: str
    source_payloads_fingerprint: str
    segments: tuple[dict[str, Any], ...]
    boundaries: tuple["RowBoundaryResolution", ...]
    unresolved: tuple[Any, ...]
    comparative_observations: tuple[Any, ...]
    comparative_refusals: tuple[Any, ...]
    issues: tuple[Any, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ticker": self.ticker,
            "window_id": self.window_id,
            "source_payloads_fingerprint": self.source_payloads_fingerprint,
            "segments": [dict(row) for row in self.segments],
            "boundary_resolutions": [row.to_dict() for row in self.boundaries],
            "unresolved_boundaries": [row.model_dump(mode="json") for row in self.unresolved],
            "comparative_observations": [
                row.model_dump(mode="json") for row in self.comparative_observations
            ],
            "comparative_refusals": [
                row.model_dump(mode="json") for row in self.comparative_refusals
            ],
            "issues": [row.model_dump(mode="json") for row in self.issues],
        }


def preview_correspondence_candidate(
    *,
    window: "MultiBasisWindow",
    financials_payloads_by_year: Mapping[int, Mapping[str, Any]],
    current_links: Sequence["CorrespondenceLink"] = (),
    current_breaks: Sequence["DeclaredBasisBreak"] = (),
) -> CorrespondenceCandidatePreview:
    """Check one candidate window against the pin, and say where the joins are.

    Every boundary of every required role gets one of four honest answers:
    continues by *identity* (derived, stored nowhere), resolved by an admitted
    *link*, refused by an admitted *break*, or *unlinked* — the state a person
    still has to decide. Nothing is inferred: a boundary whose endpoints will
    not mint is reported as an issue with its repair target, never resolved by
    guessing an endpoint.

    Step C-4 deleted this function's private resolver in favour of the one the
    compile calls. The private copy matched standing links on year equality
    while the closure and the queue matched on the bound-span junction, so a
    boundary the admission door had already resolved could still read
    "unlinked" here — a preview that disagrees with the build it previews.
    """

    from .correspondence_proposal_port import (
        generate_comparative_candidates,
        resolve_row_boundaries,
    )

    frozen_payloads = freeze_payloads_by_year(financials_payloads_by_year)
    topology = build_statement_topology(frozen_payloads)
    topologies = {year: topology for year in window.fiscal_years}

    comparative = generate_comparative_candidates(
        window,
        topologies,
        frozen_payloads,
        current_links,
        current_breaks,
    )
    queue = comparative.proposal_candidates
    queue_issues = comparative.issues
    resolutions, resolver_issues = resolve_row_boundaries(
        window, topologies, current_links, current_breaks
    )

    segments = tuple(
        {
            "from_fy": segment.from_fy,
            "through_fy": segment.through_fy,
            "section_total_tag": segment.section_total_tag,
            "rows": {
                role: disposition.model_dump(mode="json")
                for role, disposition in sorted(segment.rows.items())
            },
        }
        for segment in window.segments
    )
    # Both walks report the same structural refusals; the queue additionally
    # reports "the later span presents no reviewed section children to choose
    # an endpoint from", which only the proposal side needs. Union them rather
    # than picking one, so a caller never loses an issue because the other
    # walk happened to be silent.
    merged_issues: list[Any] = list(resolver_issues)
    seen = {row.model_dump_json() for row in merged_issues}
    for issue in queue_issues:
        key = issue.model_dump_json()
        if key not in seen:
            seen.add(key)
            merged_issues.append(issue)
    return CorrespondenceCandidatePreview(
        ticker=window.ticker,
        window_id=window.window_id,
        source_payloads_fingerprint=topology.source_payloads_fingerprint,
        segments=segments,
        boundaries=resolutions,
        unresolved=queue,
        comparative_observations=comparative.observations,
        comparative_refusals=comparative.refusals,
        issues=tuple(merged_issues),
    )


__all__ = [
    "CorrespondenceCandidatePreview",
    "StatementWindowPreview",
    "preview_correspondence_candidate",
    "preview_statement_windows",
]
