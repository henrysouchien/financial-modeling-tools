"""Compile reviewed multi-basis statement windows into role-bound build inputs.

Window rows are structure-plane roles. Their per-segment tags identify the
filing presentation lines bound to those roles; neither taxonomy overrides nor
provider-routing intents are minted here.
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import json
from typing import Any, Literal, Mapping

from .build_findings import make_finding
from .finding_vocabulary import Finding
from .models import FinancialStatement
from .multi_basis_window import (
    ExistingConceptTarget,
    InsertTarget,
    MultiBasisWindow,
    SegmentRowBinding,
)
from .provider_port.registry import ProviderRegistry


StatementWindowId = str


class StatementMappingSourceUnavailableError(RuntimeError):
    """The pinned trees and the pinned payloads are not the same content.

    Carried verbatim from the v1 substrate (plan §3.2 "payload/fingerprint
    checks — preserved, unchanged"): the compile refuses a topology whose
    fingerprint differs from the payloads the pin declares, because every
    stored ``PresentationLineRef`` replays against exactly that fingerprint.
    """

    error_code = "statement_mapping_source_unavailable"


class StatementSourceMaterialMissingError(RuntimeError):
    """The pin declares a window but does not carry the material to compile it.

    The severed-lane state (plan §2.2 fact 1), now typed with its repair
    target: rebuild the pin in the prepare lane so the bundle carries the
    frozen payloads for every requested year of every declared window.
    """

    error_code = "statement_source_material_missing"

    def __init__(
        self,
        *,
        ticker: str,
        window_id: str,
        missing_years: tuple[int, ...],
    ) -> None:
        self.ticker = ticker
        self.window_id = window_id
        self.missing_years = tuple(sorted(missing_years))
        self.repair_target = (
            "prepare lane: rebuild the pin so statement_source_material carries "
            "the frozen payloads for these years"
        )
        super().__init__(
            f"{ticker} window {window_id!r} requires pinned statement source "
            f"material for fiscal years {list(self.missing_years)}"
        )


class MultiBasisWindowIncompleteError(RuntimeError):
    """The build asks for years the window's segment union does not cover.

    Successor of ``mapping_window_incomplete``. The repair is the profile lane,
    not this path: extend the window with reviewed segments, or re-ratify a
    shorter model window (DEC-07-class). An undeclared shortening is not
    representable as success.
    """

    error_code = "multi_basis_window_incomplete"

    def __init__(
        self,
        *,
        window_id: str,
        uncovered_years: tuple[int, ...],
        from_fy: int,
        through_fy: int,
    ) -> None:
        self.window_id = window_id
        self.uncovered_years = tuple(sorted(uncovered_years))
        self.from_fy = int(from_fy)
        self.through_fy = int(through_fy)
        self.repair_target = (
            "profile lane: extend the window with reviewed segments, or "
            "re-ratify a shorter model window"
        )
        super().__init__(
            f"Statement window {window_id!r} does not cover requested fiscal "
            f"years {list(self.uncovered_years)}; declared window is "
            f"{self.from_fy}-{self.through_fy}"
        )


class CorrespondenceUnlinkedRequiredYearError(RuntimeError):
    """A required role drifts across a segment boundary with no human decision.

    The compile refusal of plan §4.3. It carries the row, the boundary, the
    drift parts, and which years are joined and which are not, so the repair is
    addressable without reading the build: admit a ``CorrespondenceLink`` or a
    ``DeclaredBasisBreak`` through the existing FMS approve-then-execute path,
    or shorten the window in the profile lane.

    Never inferred around: an unlinked drift is not a small gap to fill with
    the earlier tag, it is the absence of the one judgment R6 says a person
    owes.
    """

    error_code = "correspondence_unlinked_required_year"

    def __init__(
        self,
        *,
        window_id: str,
        row_role: str,
        before_fy: int,
        after_fy: int,
        drift_parts: tuple[str, ...],
        linked_years: tuple[int, ...],
        unlinked_years: tuple[int, ...],
        stale_artifact_id: str | None = None,
    ) -> None:
        self.window_id = window_id
        self.row_role = row_role
        self.before_fy = int(before_fy)
        self.after_fy = int(after_fy)
        self.drift_parts = tuple(sorted(drift_parts))
        self.linked_years = tuple(sorted(linked_years))
        self.unlinked_years = tuple(sorted(unlinked_years))
        #: An admitted link stands over this junction but its endpoints are not
        #: the ones the compile mints from the current window and pin. Naming
        #: it is the difference between "nobody has decided this" and "the
        #: decision that exists was made about two different lines".
        self.stale_artifact_id = stale_artifact_id
        self.repair_target = (
            (
                "FMS approve-then-execute (supersede the standing link, whose "
                "endpoints the window no longer binds), or the profile lane "
                "(shorten the window)"
            )
            if stale_artifact_id is not None
            else (
                "FMS approve-then-execute (admit a link or declare a break), or "
                "the profile lane (shorten the window)"
            )
        )
        stale_note = (
            ""
            if stale_artifact_id is None
            else (
                f"; link {stale_artifact_id!r} straddles this junction but its "
                "endpoints are not the ones the window now binds"
            )
        )
        super().__init__(
            f"Statement window {window_id!r} row {row_role!r} drifts across "
            f"FY{self.before_fy}->FY{self.after_fy} in "
            f"{list(self.drift_parts)} with no admitted link or declared break; "
            f"unlinked years {list(self.unlinked_years)}{stale_note}"
        )


@dataclass(frozen=True)
class WindowBoundaryOutcome:
    """The boundary pass over one window: its resolutions and what it records.

    ``findings`` are recorded on the published version, never refused: a
    declared break the pin does not carry (``statement_window_declared_break_absent``)
    and a required boundary the pin cannot decide
    (``statement_window_boundary_unresolvable``).
    """

    resolutions: tuple[dict[str, Any], ...]
    findings: tuple[Finding, ...]


@dataclass(frozen=True)
class CompiledWindowPlan:
    window_id: StatementWindowId
    window: MultiBasisWindow
    section_total_required_role: str
    selected_required_roles: tuple[str, ...]
    inserted_required_roles: tuple[str, ...]

    @property
    def statement(self) -> FinancialStatement:
        return self.window.statement

    @property
    def section(self) -> str:
        return self.window.section



@dataclass
class StatementWindowsResult:
    """The build-facing report of one window compile."""

    compiled: list[dict[str, Any]] = field(default_factory=list)
    population_dispositions: list[dict[str, Any]] = field(default_factory=list)
    materialized: list[dict[str, Any]] = field(default_factory=list)
    suppressed_formula_members: list[dict[str, Any]] = field(default_factory=list)
    discarded_duplicate_observations: list[dict[str, Any]] = field(
        default_factory=list
    )
    boundary_resolutions: list[dict[str, Any]] = field(default_factory=list)
    coverage: dict[str, dict[str, Any]] = field(default_factory=dict)
    collisions: list[dict[str, Any]] = field(default_factory=list)
    gaps: list[dict[str, Any]] = field(default_factory=list)
    conflicts: list[dict[str, Any]] = field(default_factory=list)
    enforcement_status: Literal["not_run", "pass", "fail"] = "not_run"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CompiledStatementWindows:
    semantic_rows: dict[str, dict[str, Any]]
    section_plans: dict[str, CompiledWindowPlan]
    report: StatementWindowsResult
    compilation_fingerprint: str
    source_ticker: str
    source_store_hash: str


def _require_non_blank(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must be non-empty")
    return normalized


def parse_statement_windows(
    value: Any,
    *,
    provider_registry: ProviderRegistry | None = None,
) -> dict[str, MultiBasisWindow]:
    """Validate a ``statement_windows`` section keyed by ``{statement}.{section}``."""

    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ValueError("statement_windows must be an object")

    parsed: dict[str, MultiBasisWindow] = {}
    for raw_window_id, raw_window in value.items():
        window_id = _require_non_blank(str(raw_window_id), field_name="window ID")
        if window_id != raw_window_id:
            raise ValueError("statement window IDs may not have surrounding whitespace")
        if provider_registry is None:
            window = MultiBasisWindow.model_validate(raw_window)
        else:
            window = MultiBasisWindow.model_validate(
                raw_window,
                context={"provider_registry": provider_registry},
            )
        if window_id != window.window_id:
            raise ValueError(
                f"statement window key {window_id!r} must equal {window.window_id!r}"
            )
        parsed[window_id] = window
    return parsed


def serialize_statement_windows(
    windows: Mapping[str, MultiBasisWindow] | Mapping[str, Any] | None,
    *,
    provider_registry: ProviderRegistry | None = None,
) -> dict[str, dict[str, Any]]:
    """Return the canonical JSON representation of a window section."""

    parsed = parse_statement_windows(
        {} if windows is None else windows,
        provider_registry=provider_registry,
    )
    return {
        window_id: window.model_dump(mode="json")
        for window_id, window in sorted(parsed.items())
    }


def statement_window_compilation_fingerprint(
    *,
    ticker: str,
    statement_windows: Mapping[str, MultiBasisWindow],
    existing_semantic_rows: Mapping[str, dict[str, Any]] | None = None,
) -> str:
    """Bind a compiled artifact to its ticker, windows, and semantic rows."""

    payload = {
        "version": 1,
        "ticker": _require_non_blank(ticker, field_name="ticker").upper(),
        "statement_windows": {
            window_id: window.model_dump(mode="json")
            for window_id, window in sorted(statement_windows.items())
        },
        "existing_semantic_rows": dict(existing_semantic_rows or {}),
    }
    encoded = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()




def window_declared_tags(
    compiled: "CompiledStatementWindows",
) -> dict[str, tuple[str, ...]]:
    """Every window concept and the exact tag set its window declares for it."""

    declared: dict[str, tuple[str, ...]] = {}
    for plan in compiled.section_plans.values():
        window = plan.window
        declared[plan.section_total_required_role] = tuple(
            dict.fromkeys(segment.section_total_tag for segment in window.segments)
        )
        for row_role in window.bound_roles:
            declared[row_role] = window.bound_tags_for(row_role)
    return declared




def _compiled_window_semantic_row(
    *,
    row_role: str,
    window: MultiBasisWindow,
    binding: SegmentRowBinding,
) -> dict[str, Any]:
    """The materialization row for one inserted role.

    Successor of the v1 ``_compiled_semantic_row``, deleted in this change. The
    source block carries the window's declared tags and states, structurally,
    that equivalents are not included: the field that could have said otherwise
    no longer exists on a binding.
    """

    target = binding.target
    if not isinstance(target, InsertTarget):  # pragma: no cover - caller-guarded
        raise TypeError("compiled semantic rows require insert targets")
    return {
        "statement": window.statement.value,
        "section": window.section,
        "semantic_role": row_role,
        "target_label": target.label,
        "unit": target.unit.value,
        "row_policy": {
            **target.row_policy.model_dump(mode="json"),
            "stable_item_id": target.stable_item_id,
        },
        "forecast_policy": target.forecast_policy.model_dump(mode="json"),
        "source": {
            "xbrl_tags": list(window.bound_tags_for(row_role)),
            "registry_group_id": None,
            "include_equivalents": False,
        },
    }


def compile_statement_windows(
    *,
    ticker: str,
    statement_windows: Mapping[str, MultiBasisWindow] | Mapping[str, Any] | None,
    existing_semantic_rows: Mapping[str, dict[str, Any]] | None = None,
    source_store_hash: str = "",
    provider_registry: ProviderRegistry | None = None,
) -> CompiledStatementWindows:
    """Compile windows without fetching data, mutating models, or writing stores."""

    _require_non_blank(ticker, field_name="ticker")
    windows = parse_statement_windows(
        {} if statement_windows is None else statement_windows,
        provider_registry=provider_registry,
    )
    semantic_row_ids = set(existing_semantic_rows or {})
    compiled_semantic_rows: dict[str, dict[str, Any]] = {}
    section_plans: dict[str, CompiledWindowPlan] = {}
    claimed_required_roles: set[str] = set()
    claimed_stable_item_ids: set[str] = set()
    claimed_tags: set[str] = set()
    report = StatementWindowsResult()

    def claim_tags(tags: tuple[str, ...], *, required_role: str) -> None:
        for tag in tags:
            if tag in claimed_tags:
                raise ValueError(
                    f"bound tag {tag!r} is reused by role {required_role!r}"
                )
            claimed_tags.add(tag)

    for window_id, window in sorted(windows.items()):
        if window.ticker.upper() != ticker.upper():
            raise ValueError(
                f"statement window {window_id!r} belongs to {window.ticker!r}, "
                f"not {ticker!r}"
            )
        total_role = window.section_total.target_concept_id
        if total_role in semantic_row_ids:
            raise ValueError(
                f"role {total_role!r} appears in both semantic_rows and "
                "statement_windows"
            )
        if total_role in claimed_required_roles:
            raise ValueError(
                f"statement window role {total_role!r} is selected more than once"
            )
        claimed_required_roles.add(total_role)
        total_tags = tuple(
            dict.fromkeys(segment.section_total_tag for segment in window.segments)
        )
        claim_tags(total_tags, required_role=total_role)

        inserted_roles: list[str] = []
        selected_roles: list[str] = []
        for row_role in sorted(window.bound_roles):
            binding = window.binding_for(row_role)
            assert binding is not None
            if row_role in semantic_row_ids:
                raise ValueError(
                    f"role {row_role!r} appears in both semantic_rows and "
                    "statement_windows"
                )
            if row_role in claimed_required_roles:
                raise ValueError(
                    f"statement window role {row_role!r} is selected more than once"
                )
            claimed_required_roles.add(row_role)
            claim_tags(
                window.bound_tags_for(row_role),
                required_role=row_role,
            )
            selected_roles.append(row_role)

            if isinstance(binding.target, InsertTarget):
                if binding.target.stable_item_id in claimed_stable_item_ids:
                    raise ValueError(
                        f"insert stable_item_id {binding.target.stable_item_id!r} "
                        "is reused"
                    )
                claimed_stable_item_ids.add(binding.target.stable_item_id)
                inserted_roles.append(row_role)
                compiled_semantic_rows[row_role] = _compiled_window_semantic_row(
                    row_role=row_role,
                    window=window,
                    binding=binding,
                )
            elif not isinstance(binding.target, ExistingConceptTarget):
                raise TypeError(binding.target)

        section_plans[window_id] = CompiledWindowPlan(
            window_id=window_id,
            window=window,
            section_total_required_role=total_role,
            selected_required_roles=tuple(sorted(selected_roles)),
            inserted_required_roles=tuple(sorted(inserted_roles)),
        )
        report.compiled.append(
            {
                "window_id": window_id,
                "from_fy": window.from_fy,
                "through_fy": window.through_fy,
                "segments": [
                    {
                        "from_fy": segment.from_fy,
                        "through_fy": segment.through_fy,
                        "basis_kind": segment.basis.kind,
                        "section_total_tag": segment.section_total_tag,
                        "rows": {
                            role: disposition.model_dump(mode="json")
                            for role, disposition in sorted(segment.rows.items())
                        },
                    }
                    for segment in window.segments
                ],
                "selected_roles": sorted(selected_roles),
                "tolerance_profile_id": window.tolerance_profile_id,
            }
        )
        report.coverage[window_id] = {"by_year": {}}

    return CompiledStatementWindows(
        semantic_rows=compiled_semantic_rows,
        section_plans=section_plans,
        report=report,
        compilation_fingerprint=statement_window_compilation_fingerprint(
            ticker=ticker,
            statement_windows=windows,
            existing_semantic_rows=existing_semantic_rows,
        ),
        source_ticker=_require_non_blank(ticker, field_name="ticker").upper(),
        source_store_hash=str(source_store_hash),
    )


def require_window_source_material(
    *,
    ticker: str,
    section_plans: Mapping[str, CompiledWindowPlan],
    payload_years: "frozenset[int] | set[int]",
    requested_years: tuple[int, ...],
) -> None:
    """Coverage first, then material — the compile's two window preconditions.

    Ordered deliberately and not interchangeable. A requested year outside the
    declared window is a **profile-lane** repair (extend the window, or
    re-ratify a shorter one); only once the years are inside a declared window
    is a missing payload the **prepare lane's** problem. Checking material
    first would send an author to rebuild a pin for a year the window never
    claimed.
    """

    for window_id, plan in sorted(section_plans.items()):
        window_requested_years(plan.window, requested_years)
        missing = tuple(
            year for year in requested_years if int(year) not in payload_years
        )
        if missing:
            raise StatementSourceMaterialMissingError(
                ticker=ticker,
                window_id=window_id,
                missing_years=missing,
            )


def enforce_window_boundaries(
    window: MultiBasisWindow,
    topologies: Mapping[int, Any],
    *,
    current_links: "tuple[Any, ...]" = (),
    current_breaks: "tuple[Any, ...]" = (),
    requested_years: tuple[int, ...] = (),
) -> WindowBoundaryOutcome:
    """The compile boundary pass (plan §4.3), exhaustive by construction.

    For every required role spanning two adjacent bound spans: recompute the
    three-part identity over refs minted from the pinned trees. ``Unchanged``
    continues. ``Drifted`` requires a declared break over that junction, or an
    admitted link over it whose endpoints are the ones the compile just minted
    (plan §4.3's "a human-approved link **whose endpoints match**"). Neither is
    ``correspondence_unlinked_required_year``.

    A window may be re-authored after a link is admitted, so an approved link
    can end up straddling a junction whose bound tags have moved underneath it.
    That link resolves nothing — the refusal fires and names it as the artifact
    to supersede.

    The declared inputs must also be complete: a ``break_id`` the window
    declares and the pin does not carry is recorded as
    ``statement_window_declared_break_absent``, and a required boundary the
    pin cannot decide at all as ``statement_window_boundary_unresolvable``.
    Both are completeness facts recorded on the published version (warning
    Findings on the outcome), never refusals: the rest of the pass still runs.
    Findings sharing a Finding identity (window, years) are one Finding naming
    every break or row, so no message is lost when findings collapse.

    Identity is recomputed here on every compile and stored nowhere (P1/P3):
    the window carries segments, not continuations, so there is no second copy
    of "this row continues" that could go stale against the pin it describes.

    ``requested_years`` narrows the refusal and the unresolvable findings to
    boundaries the build actually spans. A boundary entirely outside the
    requested historical window is reported in the returned resolutions but
    records nothing: the build never reads those years.
    """

    from .correspondence_proposal_port import resolve_row_boundaries

    pinned_break_ids = {
        str(getattr(declared_break, "break_id", "")) for declared_break in current_breaks
    }
    missing_breaks = tuple(
        sorted(
            break_id
            for break_id in window.declared_breaks
            if break_id not in pinned_break_ids
        )
    )
    findings: list[Finding] = []
    if missing_breaks:
        findings.append(
            make_finding(
                "statement_window_declared_break_absent",
                message=(
                    f"Statement window {window.window_id!r} declares break(s) "
                    f"{list(missing_breaks)} that the pinned correspondence set "
                    "does not carry; repair: the prepare lane rebuilds the pin "
                    "so it carries the declared break"
                ),
                item_id=window.window_id,
                observed={
                    "window_id": window.window_id,
                    "break_id": ", ".join(missing_breaks),
                },
            )
        )

    resolutions, issues = resolve_row_boundaries(
        window,
        topologies,
        current_links,
        current_breaks,
    )
    requested = set(int(year) for year in requested_years)
    issues_by_years: dict[tuple[int, ...], list[Any]] = {}
    for issue in issues:
        if not requested or requested & set(issue.fiscal_years):
            issues_by_years.setdefault(
                tuple(sorted(issue.fiscal_years)), []
            ).append(issue)
    for fiscal_years, year_issues in sorted(issues_by_years.items()):
        findings.append(
            make_finding(
                "statement_window_boundary_unresolvable",
                message=(
                    f"Statement window {window.window_id!r} has boundaries the "
                    "pin cannot decide: "
                    + "; ".join(
                        f"{issue.row_role}:{issue.reason}: {issue.detail}"
                        for issue in year_issues
                    )
                    + "; repair: the prepare lane (repair the pin) or the "
                    "profile lane"
                ),
                item_id=window.window_id,
                periods=fiscal_years,
                observed={
                    "window_id": window.window_id,
                    "row_role": ", ".join(issue.row_role for issue in year_issues),
                    "reason": ", ".join(
                        str(issue.reason) for issue in year_issues
                    ),
                },
            )
        )
    for resolution in resolutions:
        if resolution.resolution != "unlinked":
            continue
        # A junction blocks only when the build reads years on BOTH sides of
        # it. A five-year window feeding a two-year model must not be refused
        # for a join the build never crosses — refusing there would make an
        # honestly longer window unusable, which is the opposite of what the
        # window is for.
        if requested and not (
            min(requested) <= resolution.before_fy
            and max(requested) >= resolution.after_fy
        ):
            continue
        linked = tuple(
            sorted(
                year
                for year in (requested or set(window.fiscal_years))
                if window.bound_tag_for(resolution.row_role, year) is not None
                and year <= resolution.before_fy
            )
        )
        unlinked = tuple(
            sorted(
                year
                for year in (requested or set(window.fiscal_years))
                if window.bound_tag_for(resolution.row_role, year) is not None
                and year >= resolution.after_fy
            )
        )
        raise CorrespondenceUnlinkedRequiredYearError(
            window_id=window.window_id,
            row_role=resolution.row_role,
            before_fy=resolution.before_fy,
            after_fy=resolution.after_fy,
            drift_parts=resolution.drift_parts,
            linked_years=linked,
            unlinked_years=unlinked,
            stale_artifact_id=resolution.stale_artifact_id,
        )
    return WindowBoundaryOutcome(
        resolutions=tuple(resolution.to_dict() for resolution in resolutions),
        findings=tuple(findings),
    )


def window_requested_years(
    window: MultiBasisWindow,
    requested_years: tuple[int, ...],
) -> None:
    """Raise ``multi_basis_window_incomplete`` for any requested year outside it."""

    uncovered = window.uncovered_years(tuple(requested_years))
    if uncovered:
        raise MultiBasisWindowIncompleteError(
            window_id=window.window_id,
            uncovered_years=uncovered,
            from_fy=window.from_fy,
            through_fy=window.through_fy,
        )


__all__ = [
    "CompiledStatementWindows",
    "CompiledWindowPlan",
    "CorrespondenceUnlinkedRequiredYearError",
    "MultiBasisWindowIncompleteError",
    "StatementMappingSourceUnavailableError",
    "StatementSourceMaterialMissingError",
    "StatementWindowId",
    "StatementWindowsResult",
    "WindowBoundaryOutcome",
    "compile_statement_windows",
    "enforce_window_boundaries",
    "require_window_source_material",
    "parse_statement_windows",
    "serialize_statement_windows",
    "statement_window_compilation_fingerprint",
    "window_requested_years",
]
