"""Derive a model's filed history/forecast boundary from its pinned corpus.

The boundary is the latest fiscal year for which the verified corpus holds an
annual filing observation accepted at or before the manifest's own cutoff.
Selection is pure over fixed bytes: no wall clock, encounter-order tie break, or
profile literal may determine the answer.

Two evidence tiers can establish a filing observation:

``filing_dei``
    Cover-page DEI facts from a pinned inline-XBRL instance, paired with the
    filing header that supplies the acceptance timestamp.

``statement_occurrence``
    The strict accession-bound ``filing_facts.v1`` presentation-occurrence
    envelope. Its filing identity supplies fiscal period end, acceptance,
    accession, and form. FMP statement CSVs are cross-check inputs and are
    intentionally outside this derivation.

The manifest's ``period_accessions`` labels corroborate an observed boundary
but can never establish or reject it. The derived result is materialized at
bundle-build time; stored bundles are never re-derived against a newer corpus.
"""

from __future__ import annotations

import gzip
import json
import re
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Final, Iterable, Literal, Mapping, Protocol, Sequence
from xml.etree import ElementTree
from zoneinfo import ZoneInfo

from schema.provider_port.adapters.edgar.wire.filing_facts_v1 import (
    EdgarFilingFactsContractError,
    EdgarFilingFactsEnvelopeV1,
    validate_edgar_filing_facts_payload,
)





# ---------------------------------------------------------------------------
# Structural inputs. Nothing here imports `api/`; these describe the shapes
# `research.corpus.verify.VerifiedCorpus` and
# `schema.coverage_profile.AnnualCoverageProfile` already have.
# ---------------------------------------------------------------------------


class PinnedFile(Protocol):
    """One verified manifest entry, in the fields this derivation reads."""

    @property
    def relative_path(self) -> Any: ...

    @property
    def logical_id(self) -> str: ...

    @property
    def kind(self) -> str: ...

    @property
    def issuer(self) -> str | None: ...

    @property
    def accession(self) -> str | None: ...


class PinnedCorpus(Protocol):
    """A corpus whose byte domain and release identity were verified."""

    @property
    def root(self) -> Path: ...

    @property
    def ticker(self) -> str: ...

    @property
    def retrieved_at(self) -> str: ...

    @property
    def files(self) -> Sequence[PinnedFile]: ...

    def declared_period_labels(self) -> frozenset[str]:
        ...


class EvidenceWindow(Protocol):
    @property
    def family_id(self) -> str: ...

    @property
    def period_ids(self) -> Sequence[str]: ...


class CoverageProfile(Protocol):
    @property
    def profile_id(self) -> str: ...

    @property
    def historical_windows(self) -> Sequence[EvidenceWindow]: ...

    @property
    def projection_period_ids(self) -> Sequence[str]: ...


# ---------------------------------------------------------------------------
# Vocabulary
# ---------------------------------------------------------------------------

EVIDENCE_TIER_FILING_DEI: Final[str] = "filing_dei"
EVIDENCE_TIER_STATEMENT_OCCURRENCE: Final[str] = "statement_occurrence"
EVIDENCE_TIER_PIN_DECLARATION: Final[str] = "pin_declaration"

CODE_UNEVIDENCED: Final[str] = "filed_boundary_unevidenced"
CODE_AMBIGUOUS: Final[str] = "filed_boundary_ambiguous"
CODE_AFTER_CUTOFF: Final[str] = "filed_boundary_after_cutoff"
CODE_DISCONTIGUOUS: Final[str] = "filed_history_discontiguous"
CODE_PROFILE_DISAGREEMENT: Final[str] = "filed_boundary_profile_disagreement"

REPAIR_TARGET_RECAPTURE: Final[str] = (
    "corpus lane (re-capture the statement series with its filing provenance, "
    "or pin the issuer's capture/sec/ filing bytes, then rebuild)"
)
REPAIR_TARGET_SUPERSESSION: Final[str] = (
    "corpus lane (name the superseding accession in the pin's restatement "
    "lineage so schema/vintage_selection.py orders the rival filings, or "
    "re-release a single one)"
)
REPAIR_TARGET_RECAPTURE_OCCURRENCE: Final[str] = (
    "corpus lane (re-capture the accession-bound statement occurrence so its "
    "filing identity states one fiscal year, period end, and acceptance; rival "
    "accessions remain ordered only by schema/vintage_selection.py)"
)
REPAIR_TARGET_ADVANCE_CUTOFF: Final[str] = (
    "corpus lane (advance the release's retrieved_at/as_of_close past the "
    "filing's acceptance, or re-pin at the earlier cutoff)"
)
REPAIR_TARGET_PIN_MISSING_YEAR: Final[str] = (
    "corpus lane (pin the missing fiscal year's filing, then rebuild)"
)
REPAIR_TARGET_REDERIVE_PROFILE: Final[str] = (
    "model-kind lane (re-derive the coverage profile's authored calendar from "
    "the pin it is built against)"
)

#: Annual report forms whose cover page names a full fiscal year.
ANNUAL_FORM_TYPES: Final[frozenset[str]] = frozenset({"10-K", "20-F", "40-F"})
_PROJECTION_PERIOD_RE: Final[re.Pattern[str]] = re.compile(
    r"^FY(?P<year>[0-9]{4})E$"
)

_FILING_DEI_KIND: Final[str] = "deterministic_gzip_sec_inline_xbrl_bytes"
_FILING_HEADER_KIND: Final[str] = "deterministic_gzip_sec_filing_header_bytes"
_STATEMENT_OCCURRENCE_KIND: Final[str] = "edgar_statement_occurrences_json"

#: Every pinned manifest kind this derivation reads as evidence.
EVIDENCE_FILE_KINDS: Final[frozenset[str]] = frozenset(
    {_FILING_DEI_KIND, _FILING_HEADER_KIND, _STATEMENT_OCCURRENCE_KIND}
)

#: ``DocumentTransitionReport`` is an XBRL boolean, so both lexical forms of
#: false are the same fact. *Absent* is not one of them and is handled at the
#: reading site: absence means the filing did not declare itself a transition
#: report, which is the taxonomy's own default.
_TRANSITION_REPORT_FALSE: Final[frozenset[str]] = frozenset({"false", "0"})

_DEI_NAMESPACE_PREFIX: Final[str] = "http://xbrl.sec.gov/dei/"
_DEI_GATE_TAGS: Final[frozenset[str]] = frozenset(
    {
        "DocumentType",
        "DocumentFiscalPeriodFocus",
        "DocumentFiscalYearFocus",
        "DocumentPeriodEndDate",
        "DocumentTransitionReport",
    }
)
_ACCEPTANCE_RE = re.compile(r"<ACCEPTANCE-DATETIME>\s*([0-9]{14})")
_ANNUAL_PERIOD_RE = re.compile(r"^FY(?P<year>[0-9]{4})$")
_FILER_TIMEZONE: Final[ZoneInfo] = ZoneInfo("America/New_York")


# ---------------------------------------------------------------------------
# Contract types
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class FiledAnnualObservation:
    """One annual filing fact the pin holds, dated by its own acceptance."""

    fiscal_year: int
    period_end: date
    accepted_at: datetime
    evidence_tier: Literal[
        "filing_dei", "statement_occurrence", "pin_declaration"
    ]
    #: Accession-bound locator for filing/occurrence evidence.
    locator: str
    form_type: str | None

    def __str__(self) -> str:
        return (
            f"{self.locator} ({self.form_type or '-'}, FY{self.fiscal_year}, "
            f"{self.period_end.isoformat()}, {self.accepted_at.isoformat()}, "
            f"{self.evidence_tier})"
        )


@dataclass(frozen=True)
class FiledPeriodBoundary:
    """The derived history/forecast demarcation for one pinned corpus."""

    latest_filed_fiscal_year: int
    latest_filed_period_end: date
    #: Contiguous and ascending, ending at ``latest_filed_fiscal_year``.
    filed_fiscal_years: tuple[int, ...]
    cutoff_at: datetime
    corroborating_tiers: tuple[str, ...]
    anchor_evidence: FiledAnnualObservation


@dataclass(frozen=True)
class FiledBoundaryBlocker:
    """One reason the filed boundary could not be established or upheld.

    Carries the same shape as ``VintageBindingBlocker``: the stable code, the
    pin's declared cutoff, every candidate the refusal retained rendered in
    full, and the repair lane that can actually fix it.
    """

    code: str
    cutoff_at: datetime
    detail: str
    candidates: tuple[str, ...]
    repair_target: str

    def __str__(self) -> str:
        candidates = ", ".join(self.candidates) or "<none>"
        return (
            f"{self.code}: at cutoff {self.cutoff_at.isoformat()}: "
            f"{self.detail}; candidates retained: {candidates} "
            f"[repair: {self.repair_target}]"
        )


class FiledPeriodBoundaryRefused(ValueError):
    """The filed boundary is not derivable, or the authored axis contradicts it.

    Fail-closed by design and batch-raising, exactly like
    ``SourceVintageSelectionRefused``: an evidence gap never degrades to a
    guessed year and never falls through to wall clock. Every blocker is
    reported at once with its candidates retained and its repair lane named.
    """

    code: str = "filed_period_boundary_refused"

    def __init__(self, blockers: Sequence[FiledBoundaryBlocker]) -> None:
        self.blockers = tuple(blockers)
        self.codes = tuple(sorted({blocker.code for blocker in self.blockers}))
        detail = "; ".join(str(blocker) for blocker in self.blockers)
        super().__init__(
            f"{self.code}: {len(self.blockers)} filed-boundary refusal(s) "
            f"[{', '.join(self.codes)}]: {detail}"
        )


# ---------------------------------------------------------------------------
# Temporal helpers — the pin's clock only
# ---------------------------------------------------------------------------


def _require_aware_utc(value: datetime, *, label: str) -> datetime:
    if value.tzinfo is None or value.utcoffset() != timedelta(0):
        raise ValueError(f"{label} must be aware UTC")
    return value


def manifest_cutoff(corpus: PinnedCorpus) -> datetime:
    """The pin's own declared cutoff, never wall clock."""

    parsed = datetime.fromisoformat(corpus.retrieved_at.replace("Z", "+00:00"))
    return _require_aware_utc(parsed, label="corpus retrieved_at")




def _iso_date(value: str) -> date | None:
    try:
        return date.fromisoformat(value.strip())
    except ValueError:
        return None


def period_label_year(value: str) -> int | None:
    """The fiscal year an ``FY####`` history label names, or ``None``."""

    match = _ANNUAL_PERIOD_RE.fullmatch(value)
    return None if match is None else int(match.group("year"))


def projection_label_year(value: str) -> int | None:
    """The fiscal year an ``FY####E`` projection label names, or ``None``."""

    match = _PROJECTION_PERIOD_RE.fullmatch(value)
    return None if match is None else int(match.group("year"))


# ---------------------------------------------------------------------------
# Tier A — filing DEI (first-hand)
# ---------------------------------------------------------------------------


def _dei_gate_facts(path: Path) -> dict[str, str]:
    """Unambiguous cover-page DEI values in one pinned XBRL instance.

    A gate tag carrying two rival values is dropped rather than guessed; the
    caller then has no observation for that filing, which is tier A silence
    and therefore the safe direction (see the asymmetry rule).
    """

    seen: dict[str, set[str]] = {}
    with gzip.open(path, "rb") as handle:
        for _, element in ElementTree.iterparse(handle, events=("end",)):
            tag = element.tag
            if isinstance(tag, str) and tag.startswith("{"):
                namespace, _, local = tag[1:].partition("}")
                if local in _DEI_GATE_TAGS and namespace.startswith(
                    _DEI_NAMESPACE_PREFIX
                ):
                    text = (element.text or "").strip()
                    if text:
                        seen.setdefault(local, set()).add(text)
            element.clear()
    return {name: values.pop() for name, values in seen.items() if len(values) == 1}


def _filing_acceptance(path: Path) -> datetime | None:
    """``<ACCEPTANCE-DATETIME>`` off a pinned filing header, as aware UTC."""

    with gzip.open(path, "rb") as handle:
        text = handle.read().decode("utf-8", errors="replace")
    match = _ACCEPTANCE_RE.search(text)
    if match is None:
        return None
    stamp = datetime.strptime(match.group(1), "%Y%m%d%H%M%S").replace(
        tzinfo=_FILER_TIMEZONE
    )
    return stamp.astimezone(timezone.utc)


def _filing_dei_observations(
    corpus: PinnedCorpus,
) -> tuple[FiledAnnualObservation, ...]:
    headers: dict[str, Path] = {}
    ambiguous_headers: set[str] = set()
    for item in corpus.files:
        if item.kind != _FILING_HEADER_KIND or item.issuer != corpus.ticker:
            continue
        accession = item.accession
        if accession is None:
            continue
        if accession in headers:
            ambiguous_headers.add(accession)
            continue
        headers[accession] = corpus.root.joinpath(*item.relative_path.parts)

    observations: list[FiledAnnualObservation] = []
    for item in corpus.files:
        if item.kind != _FILING_DEI_KIND or item.issuer != corpus.ticker:
            continue
        accession = item.accession
        if accession is None or accession in ambiguous_headers:
            continue
        header_path = headers.get(accession)
        if header_path is None:
            continue
        facts = _dei_gate_facts(corpus.root.joinpath(*item.relative_path.parts))
        if facts.get("DocumentFiscalPeriodFocus") != "FY":
            continue
        form_type = facts.get("DocumentType")
        if form_type not in ANNUAL_FORM_TYPES:
            continue
        transition = facts.get("DocumentTransitionReport")
        if (
            transition is not None
            and transition.strip().lower() not in _TRANSITION_REPORT_FALSE
        ):
            # Declared a transition report (or declared something this does not
            # understand): excluded, per the fail-safe direction below. An
            # *absent* element is not a declaration — 20-F and 40-F cover-page
            # XBRL does not guarantee it, and reading absence as "transition"
            # would make the first-hand tier permanently silent for exactly the
            # foreign forms `ANNUAL_FORM_TYPES` admits for genericity.
            continue
        focus = facts.get("DocumentFiscalYearFocus", "")
        if not focus.isdigit():
            continue
        period_end = _iso_date(facts.get("DocumentPeriodEndDate", ""))
        if period_end is None:
            continue
        accepted_at = _filing_acceptance(header_path)
        if accepted_at is None:
            continue
        observations.append(
            FiledAnnualObservation(
                fiscal_year=int(focus),
                period_end=period_end,
                accepted_at=accepted_at,
                evidence_tier=EVIDENCE_TIER_FILING_DEI,
                locator=accession,
                form_type=form_type,
            )
        )
    return tuple(observations)


# ---------------------------------------------------------------------------
# Tier B — accession-bound statement occurrences
# ---------------------------------------------------------------------------


def _statement_occurrence_observations(
    corpus: PinnedCorpus,
    *,
    occurrence_envelopes: Mapping[str, EdgarFilingFactsEnvelopeV1] | None = None,
) -> tuple[FiledAnnualObservation, ...]:
    """Read annual filing identity from strict pinned occurrence envelopes."""

    ticker = corpus.ticker.strip().upper()
    observations: list[FiledAnnualObservation] = []
    for item in corpus.files:
        if (
            item.kind != _STATEMENT_OCCURRENCE_KIND
            or item.issuer != corpus.ticker
            or item.accession is None
        ):
            continue
        envelope = (
            occurrence_envelopes.get(item.accession)
            if occurrence_envelopes is not None
            else None
        )
        if envelope is None:
            path = corpus.root.joinpath(*item.relative_path.parts)
            payload = json.loads(path.read_bytes())
            if not isinstance(payload, Mapping):
                continue
            envelope = validate_edgar_filing_facts_payload(
                payload,
                expected_accession=item.accession,
            )
        elif envelope.filing.accession != item.accession:
            raise EdgarFilingFactsContractError(
                "filing_facts.v1 response accession does not match its artifact key"
            )
        filing = envelope.filing
        if filing.ticker is not None and filing.ticker.strip().upper() != ticker:
            continue
        if filing.form_type not in ANNUAL_FORM_TYPES or filing.accepted_at is None:
            continue
        period_end = _iso_date(filing.report_date)
        if period_end is None:
            continue
        accepted_at = datetime.fromisoformat(
            filing.accepted_at.removesuffix("Z") + "+00:00"
        )
        fiscal_years = {
            row.printed_column.period_ref.fiscal_year
            for row in envelope.occurrences
        }
        if not fiscal_years:
            continue
        observations.append(
            FiledAnnualObservation(
                fiscal_year=max(fiscal_years),
                period_end=period_end,
                accepted_at=accepted_at,
                evidence_tier=EVIDENCE_TIER_STATEMENT_OCCURRENCE,
                locator=item.accession,
                form_type=filing.form_type,
            )
        )
    return tuple(observations)


# ---------------------------------------------------------------------------
# Tier C — the pin's own declaration (corroboration only)
# ---------------------------------------------------------------------------


def _declared_fiscal_years(corpus: PinnedCorpus) -> tuple[int, ...]:
    years = {
        year
        for label in corpus.declared_period_labels()
        for year in (period_label_year(label),)
        if year is not None
    }
    return tuple(sorted(years))


# ---------------------------------------------------------------------------
# Selection
# ---------------------------------------------------------------------------


def _rendered(observations: Iterable[FiledAnnualObservation]) -> tuple[str, ...]:
    return tuple(
        str(item)
        for item in sorted(
            observations,
            key=lambda row: (row.period_end, row.fiscal_year, row.locator),
        )
    )


def _observation_identity(
    observation: FiledAnnualObservation,
) -> tuple[Any, ...]:
    """Distinguish rival accession filings without encounter-order tie breaks."""

    return (
        observation.fiscal_year,
        observation.period_end,
        observation.accepted_at,
        observation.form_type,
        observation.locator,
    )


def _tier_ambiguity_repair(tier: str) -> str:
    """Where a contradiction *inside* one tier can actually be repaired."""

    if tier == EVIDENCE_TIER_STATEMENT_OCCURRENCE:
        return REPAIR_TARGET_RECAPTURE_OCCURRENCE
    return REPAIR_TARGET_SUPERSESSION


def _tier_anchor(
    observations: Sequence[FiledAnnualObservation],
    *,
    tier: str,
    cutoff_at: datetime,
    blockers: list[FiledBoundaryBlocker],
) -> tuple[FiledAnnualObservation | None, tuple[int, ...]]:
    """The tier's anchor and its filed years, or ``None`` when the tier is silent."""

    eligible = tuple(item for item in observations if item.accepted_at <= cutoff_at)
    if not eligible:
        return None, ()

    by_year: dict[int, dict[tuple[Any, ...], FiledAnnualObservation]] = {}
    for item in eligible:
        bucket = by_year.setdefault(item.fiscal_year, {})
        identity = _observation_identity(item)
        held = bucket.get(identity)
        # Which row represents a folded identity is a pure max over a fixed
        # set, never the first one encountered: nothing may depend on manifest,
        # filesystem or locale order. The latest acceptance the pin's cutoff
        # admits is the one reported, and every candidate here already passed
        # that cutoff.
        if held is None or (item.accepted_at, item.locator) > (
            held.accepted_at,
            held.locator,
        ):
            bucket[identity] = item
    deduped: list[FiledAnnualObservation] = []
    for fiscal_year in sorted(by_year):
        rivals = by_year[fiscal_year]
        if len(rivals) != 1:
            # An amendment of year Y is evidence about Y, never about Y+1, so
            # this is never resolved by "latest acceptance wins". Two genuinely
            # rival readings of one year are somebody else's question: refuse,
            # and name the lane that can answer it for *this* tier — the pin's
            # restatement lineage where there are accessions to order, the
            # capture where there are not.
            blockers.append(
                FiledBoundaryBlocker(
                    code=CODE_AMBIGUOUS,
                    cutoff_at=cutoff_at,
                    detail=(
                        f"{tier} holds {len(rivals)} rival annual observations "
                        f"for FY{fiscal_year} with no supersession between them"
                    ),
                    candidates=_rendered(rivals.values()),
                    repair_target=_tier_ambiguity_repair(tier),
                )
            )
            return None, ()
        deduped.append(next(iter(rivals.values())))

    latest_period_end = max(item.period_end for item in deduped)
    at_latest = tuple(
        item for item in deduped if item.period_end == latest_period_end
    )
    if len(at_latest) != 1:
        blockers.append(
            FiledBoundaryBlocker(
                code=CODE_AMBIGUOUS,
                cutoff_at=cutoff_at,
                detail=(
                    f"{tier} holds {len(at_latest)} annual observations sharing "
                    f"the latest period end {latest_period_end.isoformat()}"
                ),
                candidates=_rendered(at_latest),
                repair_target=_tier_ambiguity_repair(tier),
            )
        )
        return None, ()

    anchor = at_latest[0]
    latest_year = max(item.fiscal_year for item in deduped)
    if anchor.fiscal_year != latest_year:
        # The year labels and the period ends order the same filings
        # differently: one of the two is wrong and neither may be preferred.
        blockers.append(
            FiledBoundaryBlocker(
                code=CODE_AMBIGUOUS,
                cutoff_at=cutoff_at,
                detail=(
                    f"{tier}'s latest period end is FY{anchor.fiscal_year}'s "
                    f"while its latest fiscal-year label is FY{latest_year}"
                ),
                candidates=_rendered(deduped),
                repair_target=_tier_ambiguity_repair(tier),
            )
        )
        return None, ()
    return anchor, tuple(item.fiscal_year for item in deduped)


def derive_filed_period_boundary(
    corpus: PinnedCorpus,
    *,
    occurrence_envelopes: Mapping[str, EdgarFilingFactsEnvelopeV1] | None = None,
) -> FiledPeriodBoundary:
    """Read the history/forecast boundary off one pinned, verified corpus."""

    cutoff_at = manifest_cutoff(corpus)
    blockers: list[FiledBoundaryBlocker] = []

    dei = _filing_dei_observations(corpus)
    statements = _statement_occurrence_observations(
        corpus, occurrence_envelopes=occurrence_envelopes,
    )
    dei_anchor, dei_years = _tier_anchor(
        dei, tier=EVIDENCE_TIER_FILING_DEI, cutoff_at=cutoff_at, blockers=blockers
    )
    statement_anchor, statement_years = _tier_anchor(
        statements,
        tier=EVIDENCE_TIER_STATEMENT_OCCURRENCE,
        cutoff_at=cutoff_at,
        blockers=blockers,
    )
    if blockers:
        raise FiledPeriodBoundaryRefused(blockers)

    if dei_anchor is None and statement_anchor is None:
        candidates = dei + statements
        if candidates:
            raise FiledPeriodBoundaryRefused(
                (
                    FiledBoundaryBlocker(
                        code=CODE_AFTER_CUTOFF,
                        cutoff_at=cutoff_at,
                        detail=(
                            f"the pin holds {len(candidates)} annual filing "
                            "observation(s), all accepted after its declared "
                            "cutoff"
                        ),
                        candidates=_rendered(candidates),
                        repair_target=REPAIR_TARGET_ADVANCE_CUTOFF,
                    ),
                )
            )
        raise FiledPeriodBoundaryRefused(
            (
                FiledBoundaryBlocker(
                    code=CODE_UNEVIDENCED,
                    cutoff_at=cutoff_at,
                    detail=(
                        "no annual filing observation in any evidence tier; the "
                        f"pin declares {len(_declared_fiscal_years(corpus))} "
                        "fiscal-period label(s), and labels alone are not evidence"
                    ),
                    candidates=(),
                    repair_target=REPAIR_TARGET_RECAPTURE,
                ),
            )
        )

    if dei_anchor is not None and statement_anchor is not None:
        # Asymmetric on purpose: a corpus need not pin `capture/sec/` bytes for
        # every year, so a *lower* first-hand answer is silence, not conflict.
        if dei_anchor.fiscal_year > statement_anchor.fiscal_year:
            raise FiledPeriodBoundaryRefused(
                (
                    FiledBoundaryBlocker(
                        code=CODE_AMBIGUOUS,
                        cutoff_at=cutoff_at,
                        detail=(
                            "the pinned filing DEI names FY"
                            f"{dei_anchor.fiscal_year} as filed while the "
                            "pinned statement occurrences reach only FY"
                            f"{statement_anchor.fiscal_year}"
                        ),
                        candidates=_rendered((dei_anchor, statement_anchor)),
                        repair_target=REPAIR_TARGET_RECAPTURE,
                    ),
                )
            )
        if (
            dei_anchor.fiscal_year == statement_anchor.fiscal_year
            and dei_anchor.period_end != statement_anchor.period_end
        ):
            raise FiledPeriodBoundaryRefused(
                (
                    FiledBoundaryBlocker(
                        code=CODE_AMBIGUOUS,
                        cutoff_at=cutoff_at,
                        detail=(
                            f"the two evidence tiers date FY"
                            f"{dei_anchor.fiscal_year} to different period ends"
                        ),
                        candidates=_rendered((dei_anchor, statement_anchor)),
                        repair_target=REPAIR_TARGET_RECAPTURE,
                    ),
                )
            )

    if dei_anchor is not None and (
        statement_anchor is None
        or dei_anchor.fiscal_year == statement_anchor.fiscal_year
    ):
        anchor = dei_anchor
    else:
        assert statement_anchor is not None
        anchor = statement_anchor

    filed_years = tuple(
        sorted(
            {
                year
                for year in dei_years + statement_years
                if year <= anchor.fiscal_year
            }
        )
    )
    expected = tuple(
        range(anchor.fiscal_year - len(filed_years) + 1, anchor.fiscal_year + 1)
    )
    if filed_years != expected:
        raise FiledPeriodBoundaryRefused(
            (
                FiledBoundaryBlocker(
                    code=CODE_DISCONTIGUOUS,
                    cutoff_at=cutoff_at,
                    detail=(
                        "the filed fiscal years are not contiguous: "
                        + ", ".join(f"FY{year}" for year in filed_years)
                    ),
                    candidates=_rendered(dei + statements),
                    repair_target=REPAIR_TARGET_PIN_MISSING_YEAR,
                ),
            )
        )

    declared_years = _declared_fiscal_years(corpus)
    corroborating = []
    if dei_anchor is not None and dei_anchor.fiscal_year == anchor.fiscal_year:
        corroborating.append(EVIDENCE_TIER_FILING_DEI)
    if (
        statement_anchor is not None
        and statement_anchor.fiscal_year == anchor.fiscal_year
    ):
        corroborating.append(EVIDENCE_TIER_STATEMENT_OCCURRENCE)
    if declared_years and max(declared_years) == anchor.fiscal_year:
        # Corroboration only. These labels are a zip of two authored literals in
        # the release builder, so they may neither raise the anchor nor refuse
        # one; a label the filings do not reach is a silent declaration, and
        # the profile assertion is what catches an authored calendar drifting.
        corroborating.append(EVIDENCE_TIER_PIN_DECLARATION)

    return FiledPeriodBoundary(
        latest_filed_fiscal_year=anchor.fiscal_year,
        latest_filed_period_end=anchor.period_end,
        filed_fiscal_years=filed_years,
        cutoff_at=cutoff_at,
        corroborating_tiers=tuple(corroborating),
        anchor_evidence=anchor,
    )


# ---------------------------------------------------------------------------
# Holding the authored calendar to the derived boundary
# ---------------------------------------------------------------------------


def assert_profile_matches_filed_boundary(
    boundary: FiledPeriodBoundary,
    profile: CoverageProfile,
) -> None:
    """Refuse when the profile's authored axis contradicts the pinned facts.

    The authored calendar is the profile's historical windows and its first
    ``FY####E`` period. A window that ends past the pin's latest accepted
    annual filing names a year the pin cannot ground; one that ends before it,
    or a forecast that does not open the year after it, forecasts a filed year.
    Both are identity. How many of the filed years an approved binding covers
    is not: an uncovered year is a gap the pin records (``unbound_periods``).
    """

    anchor = boundary.latest_filed_fiscal_year
    evidence = (str(boundary.anchor_evidence),)
    blockers: list[FiledBoundaryBlocker] = []

    for window in profile.historical_windows:
        years = tuple(
            year
            for period_id in window.period_ids
            for year in (period_label_year(period_id),)
            if year is not None
        )
        if not years:
            continue
        if max(years) != anchor:
            blockers.append(
                FiledBoundaryBlocker(
                    code=CODE_PROFILE_DISAGREEMENT,
                    cutoff_at=boundary.cutoff_at,
                    detail=(
                        f"profile {profile.profile_id} historical window "
                        f"{window.family_id} ends at FY{max(years)}, but the "
                        f"pin's filed history ends at FY{anchor}"
                    ),
                    candidates=evidence,
                    repair_target=REPAIR_TARGET_REDERIVE_PROFILE,
                )
            )

    first_projection = next(
        (
            year
            for period_id in profile.projection_period_ids
            for year in (projection_label_year(period_id),)
            if year is not None
        ),
        None,
    )
    if first_projection != anchor + 1:
        blockers.append(
            FiledBoundaryBlocker(
                code=CODE_PROFILE_DISAGREEMENT,
                cutoff_at=boundary.cutoff_at,
                detail=(
                    f"profile {profile.profile_id} opens its forecast at "
                    + (
                        f"FY{first_projection}E"
                        if first_projection is not None
                        else "no FY####E period"
                    )
                    + f", but the filed history ends at FY{anchor}"
                ),
                candidates=evidence,
                repair_target=REPAIR_TARGET_REDERIVE_PROFILE,
            )
        )

    if blockers:
        raise FiledPeriodBoundaryRefused(blockers)


__all__ = [
    "ANNUAL_FORM_TYPES",
    "CODE_AFTER_CUTOFF",
    "CODE_AMBIGUOUS",
    "CODE_DISCONTIGUOUS",
    "CODE_PROFILE_DISAGREEMENT",
    "CODE_UNEVIDENCED",
    "EVIDENCE_FILE_KINDS",
    "EVIDENCE_TIER_FILING_DEI",
    "EVIDENCE_TIER_PIN_DECLARATION",
    "EVIDENCE_TIER_STATEMENT_OCCURRENCE",
    "FiledAnnualObservation",
    "FiledBoundaryBlocker",
    "FiledPeriodBoundary",
    "FiledPeriodBoundaryRefused",
    "REPAIR_TARGET_RECAPTURE_OCCURRENCE",
    "REPAIR_TARGET_SUPERSESSION",
    "assert_profile_matches_filed_boundary",
    "derive_filed_period_boundary",
    "manifest_cutoff",
]
