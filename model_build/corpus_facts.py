"""Statement facts and business-model series over prepared sealed-corpus inputs.

Product adapters decode and adjudicate the corpus. This module owns projection
assembly; ``model_build.projection`` owns period state and vintage selection.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import date, datetime, timedelta
from decimal import Decimal, localcontext
from functools import cached_property
import json
import re
from types import MappingProxyType
from typing import Literal, Protocol

import model_build.projection as resolver
from model_build.projection import SOURCE_BUNDLE_DECIMAL_CONTEXT
from schema.build_bm_node_history import BmRequiredSeries, bm_required_series
from schema.build_model_product_contracts import (
  SourceEvidence,
  StatementSourceMaterial,
  historical_evidence_id,
)
from schema.corpus_citation import corpus_citation
from schema.coverage_profile import (
  AnnualCoverageProfile,
  AnnualEvidenceWindow,
  AnnualSourceRequirement,
  PeriodPinCell,
  RetainedVintageCandidate,
)
from schema.provider_port.adapters.edgar.wire.filing_facts_v1 import EdgarFilingFactsEnvelopeV1
from schema.model_kind_profile_loader import resolve_model_kind_coverage_profile
from schema.model_number_value import ModelNumberValue
from schema.provider_port.adapters.edgar import EdgarAdapter
from schema.provider_port.contracts import (
  FilingDimensionRef, FilingLineObservation, FilingLineRef, FiscalWindow, ReportedPeriod,
)
from schema.required_role_catalog import load_required_role_catalog
from schema.role_disposition import derive_role_dispositions
from schema.statement_topology import normalize_topology_role
from schema.vintage_selection import SelectedVintage, evidence_series_identity, select_vintage
from value_semantics_core import FiscalYearPeriod

COVERAGE_PROFILE_LOGICAL_ID = "issuer.coverage_profile"
#: The axis a filer adds to a segment member's fact to say the member is an operating
#: segment (not a breakdown of it); ``StatementFactProjection.member_observations``
#: admits it beside the wanted coordinates.
_CONTAINER_AXIS = "srt:ConsolidationItemsAxis"
_ANNUAL_PERIOD_RE = re.compile(r"^FY(?P<year>[0-9]{4})$")
_OCCURRENCE_BINDING_SOURCES = frozenset(
  {"statement_bindings", "statement_material", "pin", "opening_balance"}
)


class CorpusFactFileProtocol(resolver.ProjectedCorpusFileProtocol, Protocol):
  @property
  def issuer(self) -> str | None: ...

  @property
  def source_url(self) -> str | None: ...

  @property
  def period_accessions(self) -> Mapping[str, str | None] | None: ...

  @property
  def vintage_basis(self) -> str | None: ...


class CorpusView(resolver.VerifiedCorpusProtocol, Protocol):
  @property
  def capture_metadata(self) -> Mapping[str, object]: ...

  @property
  def retrieved_at(self) -> str: ...

  @property
  def files(self) -> Sequence[CorpusFactFileProtocol]: ...


class DisclosureStatementBindingProtocol(resolver.RoleStatementBindingProtocol, Protocol):
  @property
  def disclosure_plane(self) -> bool: ...


class OccurrenceAbsenceVerdictProtocol(Protocol):
  @property
  def required_role(self) -> str: ...

  @property
  def period_id(self) -> str: ...

  @property
  def accession(self) -> str: ...

  @property
  def verdict(self) -> Literal["corroborated", "genuinely_unreported"]: ...

  @property
  def search_basis(self) -> str: ...

  @property
  def citations(self) -> tuple[str, ...]: ...

  @property
  def value(self) -> Decimal | None: ...


@dataclass(frozen=True, slots=True)
class StatementPinInputs:
  """Decoded binding planes supplied to the one root period resolver."""

  corpus: CorpusView
  profile: AnnualCoverageProfile
  material: StatementSourceMaterial | None
  statement_bindings: Mapping[str, DisclosureStatementBindingProtocol]
  sources: resolver.PinSources
  pin: tuple[AnnualSourceRequirement, ...]
  issuer_columns: Mapping[str, Sequence[FilingLineRef]]
  reconciliation_columns: Mapping[str, Sequence[FilingLineRef]]
  schedule_columns: Mapping[str, Sequence[FilingLineRef]]


def statement_period_requirements(
  inputs: StatementPinInputs,
) -> list[resolver.PeriodRequirement]:
  """Resolve each authored period from its prepared binding planes."""
  return resolver.per_period_requirements(
    inputs.corpus,
    inputs.profile,
    inputs.material,
    sources=inputs.sources,
    pin=inputs.pin,
    issuer_columns=inputs.issuer_columns,
    reconciliation_columns=inputs.reconciliation_columns,
    schedule_columns=inputs.schedule_columns,
  )


@dataclass(frozen=True, slots=True)
class StatementFactInputs:
  """Decoded pin and fetched observations, before final vintage selection. ``roles`` is the set the
  reads were prepared for (``None``: every role's), ``fetched`` holding those roles' reads alone."""

  corpus: CorpusView
  profile: AnnualCoverageProfile
  pin_inputs: StatementPinInputs
  rows: Sequence[resolver.PeriodRequirement]
  statement_bindings: Mapping[str, DisclosureStatementBindingProtocol]
  sources: resolver.PinSources
  fetched: resolver.FetchedFacts
  reported_periods: tuple[ReportedPeriod, ...]
  occurrence_absence_verdicts: Mapping[tuple[str, str], OccurrenceAbsenceVerdictProtocol]
  occurrence_envelopes: Mapping[str, EdgarFilingFactsEnvelopeV1]
  filing_accepted_at_by_accession: Mapping[str, datetime | None]
  statement_artifacts: tuple[CorpusFactFileProtocol, ...]
  issuer_artifact: CorpusFactFileProtocol
  issuer_payload: Mapping[str, object]
  issuer_available_at_by_line: Mapping[FilingLineRef, datetime]
  schedule_artifact: CorpusFactFileProtocol | None
  schedule_payload: Mapping[str, object] | None
  schedule_lines: frozenset[FilingLineRef]
  unbound_periods: tuple[tuple[str, str], ...]
  window: FiscalWindow
  roles: frozenset[str] | None = None


@dataclass(frozen=True, slots=True)
class EmptyBusinessModelPin:
  """An authored pin with no requirements; preparation reads no facts."""

  corpus_id: str


def requirement_measure(required_role: str) -> tuple[str, str]:
  leaf = required_role.rsplit(".", 1)[-1]
  if leaf in {"eps_basic", "eps_diluted"}:
    return "usd_per_share", "units"
  if "shares" in leaf:
    return "shares_millions", "millions"
  return "usd_millions", "millions"


class PinBindsNoRequiredRoles(ValueError):
  """No approved statement binding gives the pin a required role yet.

  A runtime corpus derives its required roles from the analyst-approved
  bindings, so before the first binding the pin is empty; the correspondence
  door is the one call that resolves it.
  """

  code = "required_column_absent_from_pin"

  def __init__(self, corpus_id: str) -> None:
    super().__init__(
      "required_column_absent_from_pin: pin binds no required roles; propose "
      "the first statement bindings through fms_persist_correspondence for "
      "analyst approval"
    )
    self.next_call = {
      "tool_name": "fms_persist_correspondence",
      "arguments": {"context_ref": corpus_id},
    }


def _require_utc(value: datetime) -> None:
  if not isinstance(value, datetime):
    raise TypeError("retrieved_at must be a datetime")
  if value.tzinfo is None or value.utcoffset() != timedelta(0):
    raise ValueError("retrieved_at must be an aware UTC datetime")


def _pinned_profile(
  profile: AnnualCoverageProfile,
  requirements: tuple[AnnualSourceRequirement, ...],
) -> AnnualCoverageProfile:
  return profile.model_copy(
    update={
      "annual_requirements": requirements,
      # A role may span several pin rows (one per period state).
      "required_roles": tuple(
        sorted({row.required_role for row in requirements})
      ),
      "filing_accessions": tuple(
        sorted(
          {
            line.accession
            for requirement in requirements
            for line in requirement.pinned_columns
          }
        )
      ),
    }
  )


def _empty_pin(
  profile: AnnualCoverageProfile,
  corpus: CorpusView,
) -> tuple[AnnualSourceRequirement, ...]:
  """The empty pin, recorded at the first reader (R2-Q12).

  Before the first approved binding every owed catalog role is absent from the pin:
  one unbound gap row each over the profile's historical periods, carrying the
  correspondence door that binds it. An ``optional_if_unreported`` role is owed
  nothing, so it gets no row: the role disposition table reports it
  ``unbound(optional_unreported)`` for the build to resolve, never ``unbound`` by this pin.
  """

  empty_pin = PinBindsNoRequiredRoles(corpus.corpus_id)
  period_ids = tuple(
    dict.fromkeys(
      period_id
      for window in profile.historical_windows
      for period_id in window.period_ids
    )
  )
  unreported = {
    role
    for role, _period in derive_role_dispositions(
      profile.model_copy(update={"annual_requirements": (), "required_roles": ()})
    ).optional_unreported_cells()
  }
  rows: list[AnnualSourceRequirement] = []
  for catalog_role in load_required_role_catalog().required_roles:
    if catalog_role.required_role in unreported:
      continue
    role = catalog_role.required_role
    unit, scale = requirement_measure(role)
    rows.append(
      AnnualSourceRequirement(
        requirement_id=f"{profile.profile_id}.{role}",
        family="statement",
        required_role=role,
        model_item=role,
        period_ids=period_ids,
        unit=unit,
        scale=scale,
        sign="natural",
        downstream_capabilities=("linked_statements",),
        binding_state="unbound",
        unbound_reason=str(empty_pin),
      )
    )
  return tuple(sorted(rows, key=lambda row: row.requirement_id))


def _retained_vintage(candidate: resolver.VintageEvidence) -> RetainedVintageCandidate:
  return RetainedVintageCandidate(
    evidence_id=candidate.evidence_id,
    line=candidate.line,
    available_at=candidate.available_at,
    value=candidate.value,
    resolution=candidate.resolution,
    restates_evidence_id=candidate.restates_evidence_id,
    capture_id=candidate.capture_id,
  )


def _period_cell(
  row: resolver.PeriodRequirement,
  vintage: resolver.PeriodVintages | None,
) -> PeriodPinCell:
  """The persisted cell for one resolver period, including retained vintages."""

  candidates = (
    ()
    if vintage is None
    else tuple(_retained_vintage(item) for item in vintage.candidates)
  )
  selected = None if vintage is None else vintage.selected
  cutoff = None if vintage is None or not candidates else vintage.cutoff_at
  no_selected_vintage = bool(candidates) and selected is None and row.state == "bound"
  return PeriodPinCell(
    period_id=row.period_id,
    binding_state="unbound" if no_selected_vintage else row.state,
    pinned_ref=None if no_selected_vintage else row.pinned_ref,
    absence_citation=row.absence_citation,
    included_in=row.included_in,
    unbound_reason=(
      (
        row.unbound_reason
        or f"No vintage of {row.period_id} is available at the cutoff {cutoff.isoformat()}."
      )
      if no_selected_vintage and cutoff is not None
      else row.unbound_reason if row.binding_source == "statement_bindings" else None
    ),
    vintage_candidates=candidates,
    selected_evidence_id=selected if row.state == "bound" else None,
    cutoff_at=cutoff,
  )


def _pin_from_periods(
  profile: AnnualCoverageProfile,
  rows: Sequence[resolver.PeriodRequirement],
  statement_bindings: Mapping[str, DisclosureStatementBindingProtocol],
  *,
  vintages: Mapping[tuple[str, str], resolver.PeriodVintages] | None = None,
) -> tuple[AnnualSourceRequirement, ...]:
  """One requirement per role. Every period is a cell; pinned columns are the bound refs.

  The role summary fields are cleared: a mixed role has no single state, citation or
  included aggregate. ``period_cells`` is the authority, including a cutoff that admits
  no vintage (candidates retained, nothing selected, the period unbound).
  """

  authored = {row.required_role: row for row in profile.annual_requirements}
  by_role: dict[str, list[resolver.PeriodRequirement]] = {}
  for row in rows:
    by_role.setdefault(row.required_role, []).append(row)
  vintage_by_key = vintages or {}
  out: list[AnnualSourceRequirement] = []
  for required_role, members in by_role.items():
    base = authored[required_role]
    ordered = sorted(members, key=lambda member: _period_year(member.period_id))
    period_ids = tuple(member.period_id for member in ordered)
    if len(period_ids) != len(set(period_ids)):
      raise ValueError(f"pin repeats a period for {required_role}")
    cells = tuple(
      _period_cell(member, vintage_by_key.get(member.key)) for member in ordered
    )
    pinned = tuple(
      cell.pinned_ref
      for cell in cells
      if cell.binding_state == "bound" and cell.pinned_ref is not None
    )
    binding = statement_bindings.get(required_role)
    from_window = any(
      member.binding_source == "statement_bindings" and member.pinned_ref is not None
      for member in ordered
    )
    update: dict[str, object] = {
      "period_ids": period_ids,
      "pinned_columns": pinned,
      "period_cells": cells,
      "binding_state": None,
      "absence_citation": None,
      "included_in": None,
      "unbound_reason": None,
    }
    if from_window and binding is not None and binding.disclosure_plane:
      update["disclosed_child"] = True
    out.append(base.model_copy(update=update))
  return tuple(sorted(out, key=lambda row: row.requirement_id))


def _bundle_pin(
  profile: AnnualCoverageProfile,
  rows: Sequence[resolver.PeriodRequirement],
  statement_bindings: Mapping[str, DisclosureStatementBindingProtocol],
  *,
  vintages: Mapping[tuple[str, str], resolver.PeriodVintages] | None = None,
) -> tuple[AnnualSourceRequirement, ...]:
  """The source bundle's pin: ``_pin_from_periods`` less the roles the issuer owes no row.

  An ``optional_if_unreported`` role whose every period is unbound is owed nothing
  (``_empty_pin``'s rule): it gets no row, so the role disposition table reads it
  ``unbound(optional_unreported)`` and the bundle's requirements, coverage and evidence audit
  never carry a gap the issuer does not owe. The rule belongs to the bundle's pin, not to the
  conversion: ``target_build.lower_inclusions`` reads the same conversion and needs a
  role's unbound rows to restore an earlier ``not_separately_presented`` marker.
  """

  requirements = _pin_from_periods(profile, rows, statement_bindings, vintages=vintages)
  if not requirements:
    return ()
  # The pin's own period axis (``_pin_from_periods`` keeps the full axis on ``period_cells``)
  # is the table's axis: the dispositions are read over the pin, not the authored windows.
  pinned = _pinned_profile(profile, requirements).model_copy(
    update={
      "historical_windows": (
        AnnualEvidenceWindow(
          family_id="pin",
          period_ids=tuple(
            dict.fromkeys(period_id for row in requirements for period_id in row.period_ids)
          ),
        ),
      ),
    }
  )
  unreported = {
    role for role, _period in derive_role_dispositions(pinned).optional_unreported_cells()
  }
  return tuple(row for row in requirements if row.required_role not in unreported)


def _period_year(period_id: str) -> int:
  match = _ANNUAL_PERIOD_RE.fullmatch(period_id)
  if match is None:
    raise ValueError(f"historical annual period is not FY####: {period_id}")
  return int(match.group("year"))


def _historical_years(profile: AnnualCoverageProfile) -> tuple[int, ...]:
  # An opening column pins the year before the axis; it opens the first
  # year and is never a historical year of its own.
  opening_years = [
    _period_year(period_id)
    for window in profile.historical_windows
    for period_id in window.period_ids
    if _ANNUAL_PERIOD_RE.fullmatch(period_id)
  ]
  opening_period_id = f"FY{min(opening_years) - 1}" if opening_years else None
  years = tuple(
    sorted(
      {
        _period_year(period_id)
        for requirement in profile.annual_requirements
        for period_id in requirement.period_ids
        if _ANNUAL_PERIOD_RE.fullmatch(period_id)
        and period_id != opening_period_id
      }
    )
  )
  if not years:
    raise ValueError("profile must own at least one historical FY#### period")
  return years

def resolve_annual_coverage_profile(
  corpus: CorpusView,
) -> AnnualCoverageProfile:
  """Resolve the coverage profile one verified corpus builds against.

  A runtime corpus carries its own code-derived profile
  (``issuer.coverage_profile``, ``research.runtime_corpus``) when it has no
  registry row, or when a ``prices`` capture roots its valuation over a
  packaged base (the base's registry row on that capture's peer roster); a
  runtime corpus whose base's own root is current resolves the base's registry
  row; a packaged corpus resolves its own. The registry stays total (issuer-generic plan §5.2 /
  §7 IG-3): an unregistered corpus raises the typed ``model_kind_unregistered``
  refusal — a ``ValueError``, so the caller's ``profile_resolution_failed``
  rejection is unchanged.
  """

  carried = tuple(
    row for row in corpus.files if row.logical_id == COVERAGE_PROFILE_LOGICAL_ID
  )
  if carried:
    return AnnualCoverageProfile.model_validate_json(
      corpus.root.joinpath(*carried[0].relative_path.parts).read_bytes()
    )
  base = corpus.capture_metadata.get("base_corpus")
  return resolve_model_kind_coverage_profile(
    ticker=corpus.ticker,
    corpus_id=(
      str(base["corpus_id"]) if isinstance(base, Mapping) else corpus.corpus_id
    ),
  )

@dataclass(frozen=True, slots=True)
class StatementFactProjection:
  """One verified corpus version's statement facts under its pin.

  SourceBundle and the HistoricalFoundation fact snapshot (HFS) read the same
  observations (design §4.3): bound filing occurrences under the approved
  bindings, a closure-proven absence as corroborated zero, then the issuer
  evidence and schedule values of the pinned statement lines those artifacts
  carry. ``source_by_line`` names the sealed artifact every ``ok``
  observation was read from, so a fact's source is a ``logical_id`` of this
  corpus version; nothing here reads a live provider.
  """

  corpus: CorpusView
  #: The pinned denominator: one requirement per role, full period axis.
  profile: AnnualCoverageProfile
  #: The same rows as ``profile.annual_requirements``.
  requirements: tuple[AnnualSourceRequirement, ...]
  #: Every statement-fact observation, in SourceBundle order.
  observations: tuple[FilingLineObservation, ...]
  #: The filing-occurrence part of ``observations`` (closure-adjudicated).
  occurrence_observations: tuple[FilingLineObservation, ...]
  reported_periods: tuple[ReportedPeriod, ...]
  occurrence_absence_verdicts: Mapping[
    tuple[str, str], OccurrenceAbsenceVerdictProtocol
  ]
  occurrence_envelopes: Mapping[str, EdgarFilingFactsEnvelopeV1]
  filing_accepted_at_by_accession: Mapping[str, datetime | None]
  source_by_line: Mapping[FilingLineRef, CorpusFactFileProtocol | None]
  statement_artifacts: tuple[CorpusFactFileProtocol, ...]
  issuer_artifact: CorpusFactFileProtocol
  issuer_payload: Mapping[str, object]
  #: Issuer-evidence observations of pinned lines outside the statement family.
  issuer_operating_observations: tuple[FilingLineObservation, ...]
  issuer_available_at_by_line: Mapping[FilingLineRef, datetime]
  issuer_lines: frozenset[FilingLineRef]
  schedule_artifact: CorpusFactFileProtocol | None
  schedule_payload: Mapping[str, object] | None
  schedule_lines: frozenset[FilingLineRef]
  #: ``(period_id, accession)`` of each historical period whose as-filed
  #: filing no approved ``statement_bindings.v2`` window covers: its own
  #: filing binds nothing there, so each is a typed gap for the consumer to
  #: show (a later filing's comparative may still supply its vintage).
  unbound_periods: tuple[tuple[str, str], ...] = ()
  #: Each (required role, period) the resolver selected a vintage for, or
  #: found none admitted by the cutoff. The selected vintage and the earlier
  #: candidates are also on that period's ``period_cells`` entry.
  vintages: Mapping[tuple[str, str], resolver.PeriodVintages] = field(
    default_factory=lambda: MappingProxyType({})
  )
  #: Each (required role, period) source cell as a typed value
  #: (``model_build.projection.typed_values_from_projection``): an observed
  #: value carries its ``filing-line:`` observation id, an absence its reason.
  values: Mapping[tuple[str, str], "ModelNumberValue"] = field(
    default_factory=lambda: MappingProxyType({})
  )
  #: Ephemeral metadata from sealed filing concepts, never an observation-locator rewrite.
  concept_balances: Mapping[tuple[str, str], str | None] = field(
    default_factory=lambda: MappingProxyType({})
  )

  def observation_by_line(self) -> dict[FilingLineRef, FilingLineObservation]:
    """The ``ok`` statement observation of each pinned line."""

    return {
      observation.line: observation
      for observation in self.observations
      if observation.status == "ok" and observation.fact is not None
    }

  def member_observations(
    self,
    *,
    accession: str,
    period_id: str,
    local_names: frozenset[str],
    dimensions: tuple[FilingDimensionRef, ...],
  ) -> tuple[FilingLineObservation, ...]:
    """The sealed occurrences of one member-reported fact, in every role.

    A dimensioned fact (segment revenue on ``srt:ProductOrServiceAxis``,
    say) is presented in several roles; each presenting occurrence of
    ``accession`` for ``period_id`` whose line local name is in
    ``local_names`` (casefolded) and whose explicit coordinate set is
    ``dimensions`` plus, at most, members of the container axis
    (``srt:ConsolidationItemsAxis``) is returned as its own observation, so
    the caller sees every candidate value. A fact further broken down by
    another axis is a part of the member's value, not the member's own.
    """

    envelope = self.occurrence_envelopes.get(accession)
    if envelope is None:
      return ()
    wanted = tuple(
      sorted(
        dimensions,
        key=lambda row: (row.axis_qname, row.member_qname),
      )
    )
    wanted_set = set(wanted)
    lines: set[FilingLineRef] = set()
    for occurrence in envelope.occurrences:
      if (
        occurrence.line_kind == "abstract"
        or occurrence.line_qname.local_name.casefold() not in local_names
        or f"FY{occurrence.printed_column.period_ref.fiscal_year}" != period_id
      ):
        continue
      binding = occurrence.binding
      fact = getattr(binding, "fact", None)
      if fact is None:
        continue
      coordinates = tuple(
        sorted(
          (
            FilingDimensionRef(
              axis_qname=dimension.axis.lexical_qname,
              member_qname=dimension.member.lexical_qname,
            )
            for dimension in fact.dimensions
            if getattr(dimension, "member", None) is not None
          ),
          key=lambda row: (row.axis_qname, row.member_qname),
        )
      )
      if not wanted_set.issubset(coordinates) or any(
        row not in wanted_set and row.axis_qname != _CONTAINER_AXIS
        for row in coordinates
      ):
        continue
      lines.add(
        FilingLineRef(
          accession=accession,
          period=FiscalYearPeriod(
            kind="fiscal_year",
            fiscal_year=_period_year(period_id),
          ),
          statement_role=normalize_topology_role(
            occurrence.presentation.presentation_role_name
          ),
          tag=occurrence.line_qname.lexical_qname,
          dimensions=coordinates,
        )
      )
    if not lines:
      return ()
    fiscal_year = _period_year(period_id)
    observations, _ = EdgarAdapter(
      occurrence_artifacts={accession: envelope}
    ).fetch_filing_line_facts(
      self.corpus.ticker,
      lines=tuple(lines),
      window=FiscalWindow(end_fiscal_year=fiscal_year, n_years=1),
    )
    return tuple(
      observation
      for observation in observations
      if observation.status == "ok" and observation.fact is not None
    )



def project_statement_facts(inputs: StatementFactInputs) -> StatementFactProjection:
  """Assemble facts from prepared inputs using the one period/vintage resolver."""
  corpus, profile = inputs.corpus, inputs.profile
  statement_bindings = inputs.statement_bindings
  statement_artifacts = inputs.statement_artifacts
  occurrence_envelopes = inputs.occurrence_envelopes
  occurrence_absence_verdicts = inputs.occurrence_absence_verdicts
  issuer_artifact, issuer_payload = inputs.issuer_artifact, inputs.issuer_payload
  schedule_artifact, schedule_payload = inputs.schedule_artifact, inputs.schedule_payload
  issuer_observations = inputs.fetched.issuer_observations
  schedule_observations = inputs.fetched.schedule_observations
  issuer_available_at_by_line = inputs.issuer_available_at_by_line
  reported_periods = inputs.reported_periods
  with localcontext(SOURCE_BUNDLE_DECIMAL_CONTEXT):
    if not profile.annual_requirements:
      # The empty pin binds no line, so no fact is read.
      requirements = _empty_pin(profile, corpus)
      pinned_profile = _pinned_profile(profile, requirements)
      return StatementFactProjection(
        corpus=corpus,
        profile=pinned_profile,
        requirements=requirements,
        observations=(),
        occurrence_observations=(),
        reported_periods=reported_periods,
        occurrence_absence_verdicts=MappingProxyType({}),
        occurrence_envelopes=MappingProxyType(dict(occurrence_envelopes)),
        filing_accepted_at_by_accession=(
          inputs.filing_accepted_at_by_accession
        ),
        source_by_line=MappingProxyType({}),
        statement_artifacts=tuple(statement_artifacts),
        issuer_artifact=issuer_artifact,
        issuer_payload=issuer_payload,
        issuer_operating_observations=(),
        issuer_available_at_by_line=MappingProxyType({}),
        issuer_lines=frozenset(),
        schedule_artifact=schedule_artifact,
        schedule_payload=schedule_payload,
        schedule_lines=frozenset(),
        unbound_periods=inputs.unbound_periods,
      )
    edgar_adapter = EdgarAdapter(occurrence_artifacts=occurrence_envelopes)
    facts = resolver.project_facts_per_period(
      corpus,
      inputs.rows,
      sources=inputs.sources,
      fetched=inputs.fetched,
      adapter=edgar_adapter,
      window=inputs.window,
    )
    requirements = _bundle_pin(
      profile, facts.rows, statement_bindings, vintages=facts.vintages
    )
    pinned_profile = _pinned_profile(profile, requirements)
    # Each occurrence period's observation is its selected vintage's; two
    # roles may pin one line (cash within cash and short-term investments),
    # and the line is one observation either way.
    occurrence_observations = tuple(
      {
        observation.line: observation
        for row in facts.rows
        if row.binding_source in _OCCURRENCE_BINDING_SOURCES
        and (observation := facts.observations.get(row.key)) is not None
      }.values()
    )
    # A closure verdict describes the column it searched: it stays with the
    # period while that column is the period's selected vintage.
    selected_line = {row.key: row.pinned_ref for row in facts.rows}
    occurrence_absence_verdicts = MappingProxyType(
      {
        key: verdict
        for key, verdict in occurrence_absence_verdicts.items()
        if (line := selected_line.get(key)) is not None
        and line.accession == verdict.accession
      }
    )
    # The observation planes split by requirement FAMILY, not by producer.
    issuer_statement_pinned = frozenset(
      line
      for requirement in requirements
      if requirement.required_role.startswith("statement.")
      for line in requirement.pinned_columns
    )
    # A line a window-bound role and an issuer-filled role both pin is one
    # fact: the sealed occurrence carries it, so the issuer read of the same
    # line is not a second observation (two evidence rows would share its id).
    occurrence_lines = {observation.line for observation in occurrence_observations}
    issuer_statement_observations = tuple(
      observation
      for observation in issuer_observations
      if observation.line in issuer_statement_pinned
      and observation.line not in occurrence_lines
    )
    occurrence_artifact_by_accession = {
      str(item.accession): item
      for item in statement_artifacts
      if item.accession is not None
    }
    source_by_line: dict[FilingLineRef, CorpusFactFileProtocol | None] = {}
    for observation in occurrence_observations:
      if observation.status == "ok" and observation.fact is not None:
        source_by_line[observation.line] = occurrence_artifact_by_accession[
          observation.line.accession
        ]
    for observation in issuer_statement_observations:
      source_by_line.setdefault(observation.line, issuer_artifact)
    for observation in schedule_observations:
      source_by_line.setdefault(observation.line, schedule_artifact)
    return StatementFactProjection(
      corpus=corpus,
      profile=pinned_profile,
      requirements=requirements,
      observations=tuple(
        {
          observation.line: observation
          for observation in (
            *occurrence_observations,
            *issuer_statement_observations,
            *schedule_observations,
          )
        }.values()
      ),
      occurrence_observations=occurrence_observations,
      reported_periods=reported_periods,
      occurrence_absence_verdicts=occurrence_absence_verdicts,
      occurrence_envelopes=MappingProxyType(dict(occurrence_envelopes)),
      filing_accepted_at_by_accession=(
        inputs.filing_accepted_at_by_accession
      ),
      source_by_line=MappingProxyType(source_by_line),
      statement_artifacts=tuple(statement_artifacts),
      issuer_artifact=issuer_artifact,
      issuer_payload=issuer_payload,
      issuer_operating_observations=tuple(
        observation
        for observation in issuer_observations
        if observation.line not in issuer_statement_pinned
      ),
      issuer_available_at_by_line=MappingProxyType(
        dict(issuer_available_at_by_line)
      ),
      issuer_lines=frozenset(
        observation.line for observation in issuer_observations
      ),
      schedule_artifact=schedule_artifact,
      schedule_payload=schedule_payload,
      schedule_lines=inputs.schedule_lines,
      unbound_periods=inputs.unbound_periods,
      vintages=MappingProxyType(dict(facts.vintages)),
      concept_balances=facts.concept_balances,
      values=MappingProxyType(
        resolver.typed_values_from_projection(facts.rows, facts)
      ),
    )

# --------------------------------------------------------------------------
# statement-fact source identity: one owner for the SourceBundle and every
# snapshot (HFF) citing the same filed facts
# --------------------------------------------------------------------------
#: ``src_<9000+i>`` names the i-th (1-based) statement artifact's EDGAR filing facts.
EDGAR_STATEMENT_SOURCE_BASE = 9000
#: ``src_<9500+i>`` names the i-th (1-based) sealed KPI artifact.
KPI_SOURCE_BASE = 9500


def kpi_source_id(index: int) -> str:
  """The source id of the ``index``-th (1-based) sealed KPI artifact."""
  return f"src_{KPI_SOURCE_BASE + index}"


def edgar_statement_source_ids(
  statement_artifacts: Sequence[resolver.ProjectedCorpusFileProtocol],
) -> dict[str, str]:
  """Each EDGAR filing-facts artifact's source id by accession."""
  return {
    str(item.accession): f"src_{EDGAR_STATEMENT_SOURCE_BASE + index}"
    for index, item in enumerate(statement_artifacts, start=1)
    if item.kind == resolver.EDGAR_STATEMENT_OCCURRENCES_ARTIFACT_KIND
  }


@dataclass(frozen=True, slots=True)
class _NumberedSources:
  statement: tuple[str, ...]
  fmp_by_endpoint: Mapping[str, str]
  sec: tuple[str, ...]
  schedule: str | None
  issuer: str


class AnnualSourceIds:
  """The ids of one corpus version's annual sources, allocated once.

  EDGAR filing-facts artifacts number from ``EDGAR_STATEMENT_SOURCE_BASE`` over the
  statement artifacts. Every other annual source (FMP cross-checks, SEC filing bytes,
  the schedule and issuer artifacts) numbers past the valuation snapshot's reserved
  ids, which ``reserved_source_ids`` supplies on first use: a caller citing only
  filing facts never composes the valuation snapshot.
  """

  def __init__(
    self,
    statement_artifacts: Sequence[resolver.ProjectedCorpusFileProtocol],
    *,
    sec_artifact_count: int,
    has_schedule: bool,
    reserved_source_ids: Callable[[], frozenset[str]],
  ) -> None:
    self._statement_artifacts = tuple(statement_artifacts)
    self._sec_artifact_count = sec_artifact_count
    self._has_schedule = has_schedule
    self._reserved_source_ids = reserved_source_ids

  @cached_property
  def edgar_by_accession(self) -> dict[str, str]:
    return edgar_statement_source_ids(self._statement_artifacts)

  @cached_property
  def _numbered(self) -> _NumberedSources:
    reserved = self._reserved_source_ids()

    def unreserved(number: int) -> int:
      while f"src_{number}" in reserved:
        number += 1
      return number

    # Numbered ids of every source but the filing-facts ones, which sit at 9000+.
    numeric: list[int] = []

    def next_id() -> str:
      number = unreserved(max(numeric, default=0) + 1)
      numeric.append(number)
      return f"src_{number}"

    statement: list[str] = []
    fmp_by_endpoint: dict[str, str] = {}
    fmp_number = 0
    for index, artifact in enumerate(self._statement_artifacts, start=1):
      if artifact.kind == resolver.EDGAR_STATEMENT_OCCURRENCES_ARTIFACT_KIND:
        statement.append(f"src_{EDGAR_STATEMENT_SOURCE_BASE + index}")
        continue
      fmp_number = unreserved(fmp_number + 1)
      numeric.append(fmp_number)
      statement.append(f"src_{fmp_number}")
      fmp_by_endpoint[str(artifact.capture_metadata["endpoint"])] = f"src_{fmp_number}"
    sec = tuple(next_id() for _ in range(self._sec_artifact_count))
    schedule = next_id() if self._has_schedule else None
    return _NumberedSources(
      statement=tuple(statement),
      fmp_by_endpoint=fmp_by_endpoint,
      sec=sec,
      schedule=schedule,
      issuer=next_id(),
    )

  @property
  def statement(self) -> tuple[str, ...]:
    """Each statement artifact's id, aligned with the artifacts given."""
    return self._numbered.statement

  @property
  def fmp_by_endpoint(self) -> Mapping[str, str]:
    return self._numbered.fmp_by_endpoint

  @property
  def sec(self) -> tuple[str, ...]:
    """Each SEC filing artifact's id, in ``logical_id`` order."""
    return self._numbered.sec

  @property
  def schedule(self) -> str | None:
    return self._numbered.schedule

  @property
  def issuer(self) -> str:
    return self._numbered.issuer


def statement_fact_available_at(
  *,
  grade: object,
  line: FilingLineRef,
  filing_accepted_at_by_accession: Mapping[str, datetime | None],
  issuer_available_at_by_line: Mapping[FilingLineRef, datetime],
  retrieved_at: datetime,
) -> datetime:
  """When the filing made a value public: its acceptance, else the corpus retrieval."""
  if grade != "filing_native":
    return retrieved_at
  return (
    filing_accepted_at_by_accession.get(
      line.accession, issuer_available_at_by_line.get(line, retrieved_at),
    )
    or retrieved_at
  )


def evidence_disposition(row: SourceEvidence) -> Literal["observed", "derived"]:
  """The coverage disposition one selected evidence row earns: ``derived`` when the row carries
  its derivation (a closure-corroborated zero — with or without filed operands — a composite's
  sum), else ``observed``. The SourceBundle's coverage and the fact snapshot's read the same
  rule."""
  return "derived" if row.derivation_expression else "observed"


def closure_source_evidence(
  derived: SourceEvidence,
  *,
  citations: Sequence[str],
  envelope: EdgarFilingFactsEnvelopeV1,
  artifact_locator: str,
  evidence: Sequence[SourceEvidence],
  operand_cutoff_at: datetime | None = None,
  operand_available_at: datetime | None = None,
) -> tuple[SourceEvidence, ...]:
  """Land a closure-derived zero with its cited filed operands on their bound vintages.

  Both a correspondence's live cell and a presented-blank closure use this owner;
  the typed envelope carries the same native values as the captured occurrence payload.
  A live cell binds operands at its vintage, keeping the chronology of retained
  candidates while carrying the closure lineage on its existing evidence identity.
  """
  landed = {item.evidence_id: item for item in evidence}
  # A retained historical candidate keeps its own place in the restatement chain;
  # a later operand cannot move an ancestor past the filing that restates it.
  has_restatement_edge = any(
    item.restates_evidence_id == derived.evidence_id for item in evidence
  )
  vintage_cutoff_at = operand_cutoff_at
  if vintage_cutoff_at is None and (
    derived.restates_evidence_id is not None or has_restatement_edge
  ):
    vintage_cutoff_at = derived.available_at

  def bound_vintage_id(item: SourceEvidence) -> str:
    identity = evidence_series_identity(item)
    series_rows = {
      row.evidence_id: row
      for row in landed.values()
      if evidence_series_identity(row) == identity
    }
    family_ids = {item.evidence_id}
    frontier = [item.evidence_id]
    while frontier:
      current = series_rows[frontier.pop()]
      neighbours = {
        row.evidence_id
        for row in series_rows.values()
        if row.restates_evidence_id == current.evidence_id
        or row.evidence_id == current.restates_evidence_id
      }
      frontier.extend(sorted(neighbours - family_ids))
      family_ids |= neighbours
    family = tuple(
      row for row in landed.values() if row.evidence_id in family_ids
    )
    cutoff = (
      vintage_cutoff_at
      if vintage_cutoff_at is not None
      else max(row.available_at for row in family)
    )
    outcome = select_vintage(
      family,
      item.period_id,
      cutoff,
      series_identity=evidence_series_identity,
    )
    return (
      outcome.bound.evidence_id
      if isinstance(outcome, SelectedVintage)
      else item.evidence_id
    )

  operands: set[str] = set()
  for occurrence in envelope.occurrences:
    if f"FY{occurrence.printed_column.period_ref.fiscal_year}" != derived.period_id:
      continue
    binding = occurrence.binding
    if binding is None or binding.kind != "bound":
      continue
    quantity = binding.fact.native_quantity
    if quantity.state != "numeric" or quantity.native_number.kind != "decimal":
      continue
    try:
      value = Decimal(quantity.native_number.value)
    except ArithmeticError:
      continue
    if not value.is_finite():
      continue
    decimals = quantity.source.decimals
    precision = f";decimals={decimals}" if decimals is not None else ""
    citation = (
      f"{artifact_locator}#cell={occurrence.source.cell_locator or 'unlocated'};"
      f"qname={occurrence.line_qname.lexical_qname}{precision}"
    )
    if citation not in citations:
      continue
    if occurrence.presentation_state == "reported_fixed_zero_dash":
      value = Decimal(0)
    if occurrence.preferred_label.negates_value:
      value = -value
    operand_line = replace(
      derived.line,
      tag=occurrence.line_qname.lexical_qname,
      statement_role=normalize_topology_role(
        occurrence.presentation.presentation_role_name
      ),
    )
    existing = next(
      (
        item for item in landed.values()
        if item.line == operand_line
        and item.source_quality != "fallback"
        and item.evidence_id != derived.evidence_id
      ),
      None,
    )
    if existing is None:
      operand_id = historical_evidence_id(
        operand_line, derived.period_id, "closure_operand"
      )
      native_scale = "unit" if derived.scale == "units" else derived.scale
      existing = derived.model_copy(update={
        "evidence_id": operand_id,
        "line": operand_line,
        "grade": "filing_native",
        "available_at": (
          derived.available_at if operand_available_at is None else operand_available_at
        ),
        "value": value / Decimal(10) ** resolver._SCALE_EXPONENT[native_scale],
        "sign": "as_reported",
        "locator": citation,
        "accounting_definition": operand_line.tag,
        "restates_evidence_id": None,
      })
      landed[operand_id] = existing
    operands.add(bound_vintage_id(existing))
  # Corroborated by structure alone (an abstract parent, a blank total) the closure has no
  # filed operand: the expression alone is its lineage, the citations its evidence.
  landed[derived.evidence_id] = derived.model_copy(update={
    "derivation_expression": "closure_corroborated_zero",
    "derivation_input_ids": tuple(sorted(operands)),
    "available_at": max((
      derived.available_at,
      *(landed[operand].available_at for operand in operands),
    )),
  })
  return tuple(sorted(landed.values(), key=lambda item: item.evidence_id))


def statement_source_evidence(
  *,
  statement_facts: Sequence[FilingLineObservation],
  kpi_observations: Sequence[FilingLineObservation],
  requirements: Sequence[AnnualSourceRequirement],
  source_ids: AnnualSourceIds,
  issuer: str,
  retrieved_at: datetime,
  concept_balances: Mapping[tuple[str, str], str | None],
  vintages: Mapping[tuple[str, str], resolver.PeriodVintages],
  filing_accepted_at_by_accession: Mapping[str, datetime | None],
  issuer_available_at_by_line: Mapping[FilingLineRef, datetime],
  issuer_lines: frozenset[FilingLineRef],
  schedule_lines: frozenset[FilingLineRef],
  occurrence_envelopes: Mapping[str, EdgarFilingFactsEnvelopeV1] = MappingProxyType({}),
  occurrence_artifact_locators: Mapping[str, str] = MappingProxyType({}),
) -> tuple[SourceEvidence, ...]:
  """The SourceEvidence row of every pinned statement line, minted once.

  The SourceBundle emits these rows and every snapshot that cites a pinned line
  takes the same row by ``evidence_id``: issuer, sealed ``retrieved_at``, the
  allocated source id, the role id as the accounting definition and the filing's
  preferred-label metadata are this function's, not a caller's. Callers read it
  through ``projection_statement_evidence``.
  """
  requirement_by_line = {
    line: requirement
    for requirement in requirements
    for line in requirement.pinned_columns
  }
  retained: dict[FilingLineRef, resolver.VintageEvidence] = {}
  for requirement in requirements:
    for cell in requirement.period_cells:
      period = vintages.get((requirement.required_role, cell.period_id))
      if period is None:
        continue
      ancestor_id: str | None = None
      for candidate in period.candidates:
        if candidate.available_at > period.cutoff_at:
          continue
        if cell.binding_state == "not_separately_presented" or (
          cell.pinned_ref is not None
          and replace(candidate.line, accession=cell.pinned_ref.accession) == cell.pinned_ref
        ):
          retained[candidate.line] = replace(
            candidate, restates_evidence_id=ancestor_id
          )
          requirement_by_line[candidate.line] = requirement
          ancestor_id = candidate.evidence_id
        if candidate.evidence_id == cell.selected_evidence_id:
          break

  def native_source_id(line: FilingLineRef) -> str:
    """The producer that minted a native fact names its artifact; an accession names
    only the filing, which several artifacts share."""
    if line in schedule_lines and source_ids.schedule is not None:
      return source_ids.schedule
    if line in issuer_lines:
      return source_ids.issuer
    return source_ids.edgar_by_accession.get(line.accession, source_ids.issuer)

  rows: list[SourceEvidence] = []
  closures: list[tuple[SourceEvidence, tuple[str, ...]]] = []
  observations = {
    (
      observation.line,
      observation.fact.provenance.provider
      if observation.fact.provenance is not None else None,
    ): observation
    for observation in (
      *statement_facts,
      *kpi_observations,
      *(candidate.observation for candidate in retained.values()),
    )
    if observation.status == "ok"
    and observation.fact is not None
  }
  for observation in observations.values():
    fact = observation.fact
    if observation.status != "ok" or fact is None:
      continue
    requirement = requirement_by_line.get(fact.line)
    if requirement is None:
      continue
    provenance = fact.provenance
    if provenance is None:
      raise ValueError("producer evidence cannot be analyst_asserted")
    candidate = retained.get(fact.line)
    if candidate is not None and candidate.observation is not observation:
      candidate = None
    # A fact the consumer derived inside one filing's capture: a closure-corroborated
    # zero or a composite's signed sum of that filing's lines.
    closure_derived = (
      fact.grade == "adapter_asserted"
      and provenance.provider == "edgar"
      and provenance.native_endpoint in ("occurrence_closure", resolver.COMPOSITE_ENDPOINT)
    )
    source_id = (
      source_ids.edgar_by_accession[fact.line.accession]
      if closure_derived
      else (
        source_ids.fmp_by_endpoint[provenance.native_endpoint]
        if fact.grade == "adapter_asserted"
        else native_source_id(fact.line)
      )
    )
    period_id = fact.line.period.canonical_label()
    fiscal_year = _period_year(period_id)
    rows.append(
      SourceEvidence(
        evidence_id=(
          candidate.evidence_id if candidate is not None
          else fact.source_ref.evidence_id
          if fact.source_ref is not None
          and fact.source_ref.rail == "statement"
          and provenance.provider == "edgar"
          else historical_evidence_id(fact.line, period_id, provenance.provider)
        ),
        restates_evidence_id=(
          candidate.restates_evidence_id if candidate is not None else None
        ),
        issuer=issuer,
        line=fact.line,
        grade=fact.grade,
        period_id=period_id,
        value=resolver._fact_requirement_value(
          fact,
          required_role=requirement.required_role,
          unit=requirement.unit,
          scale=requirement.scale,
          concept_balances=concept_balances,
        ),
        unit=requirement.unit,
        scale=requirement.scale,
        sign=requirement.sign,
        source_id=source_id,
        provider=provenance.provider,
        retrieved_at=retrieved_at,
        available_at=(
          candidate.available_at if candidate is not None
          else statement_fact_available_at(
            grade=fact.grade,
            line=fact.line,
            filing_accepted_at_by_accession=filing_accepted_at_by_accession,
            issuer_available_at_by_line=issuer_available_at_by_line,
            retrieved_at=retrieved_at,
          )
        ),
        as_of_date=date(fiscal_year, 6, 30),
        locator=(
          f"{source_id}:{provenance.native_field};"
          f"accession={fact.line.accession}"
        ),
        fiscal_basis="issuer_fiscal_year",
        period_length_months=12,
        accounting_definition=requirement.required_role,
        confidence=(
          "high"
          if closure_derived or fact.grade != "adapter_asserted"
          else "medium"
        ),
        source_quality=(
          "primary"
          if closure_derived or fact.grade != "adapter_asserted"
          else "fallback"
        ),
        label=fact.label,
        preferred_label_role_uri=provenance.preferred_label_role_uri,
        preferred_label_negates_value=provenance.preferred_label_negates_value,
      )
    )
    if closure_derived and provenance.native_endpoint == "occurrence_closure":
      closures.append((rows[-1], tuple(json.loads(provenance.native_field)["citations"])))
  for derived, citations in closures:
    envelope = occurrence_envelopes[derived.line.accession]
    rows = list(closure_source_evidence(
      derived,
      citations=citations,
      envelope=envelope,
      artifact_locator=occurrence_artifact_locators[derived.line.accession],
      evidence=rows,
      operand_cutoff_at=derived.available_at,
      # a vintage's date has one owner: accepted_at, else filing_date
      operand_available_at=resolver.filing_moment(
        envelope.filing.accepted_at, envelope.filing.filing_date,
      ),
    ))
  return tuple(sorted(rows, key=lambda item: item.evidence_id))


def projection_statement_evidence(
  projection: StatementFactProjection,
  *,
  source_ids: AnnualSourceIds,
  issuer: str,
  retrieved_at: datetime,
  extra_statement_facts: Sequence[FilingLineObservation] = (),
  extra_kpi_observations: Sequence[FilingLineObservation] = (),
) -> tuple[SourceEvidence, ...]:
  """``statement_source_evidence`` over one corpus version's statement projection.

  ``extra_statement_facts`` and ``extra_kpi_observations`` add the observations only
  the SourceBundle reads (FMP cross-checks, producer KPI rows).
  """
  return statement_source_evidence(
    statement_facts=(*projection.observations, *extra_statement_facts),
    kpi_observations=(
      *projection.issuer_operating_observations, *extra_kpi_observations,
    ),
    requirements=projection.requirements,
    source_ids=source_ids,
    issuer=issuer,
    retrieved_at=retrieved_at,
    concept_balances=projection.concept_balances,
    vintages=projection.vintages,
    filing_accepted_at_by_accession=projection.filing_accepted_at_by_accession,
    issuer_available_at_by_line=projection.issuer_available_at_by_line,
    issuer_lines=projection.issuer_lines,
    schedule_lines=projection.schedule_lines,
    occurrence_envelopes=projection.occurrence_envelopes,
    occurrence_artifact_locators={
      str(artifact.accession): corpus_citation(
        projection.corpus.corpus_id, projection.corpus.corpus_version, artifact.logical_id,
      )
      for artifact in projection.statement_artifacts
      if artifact.accession is not None
    },
  )


def required_business_model_series(
  inputs: StatementFactInputs | EmptyBusinessModelPin,
) -> BmRequiredSeries:
  """Expose the prepared pin's claim-only and declinable series before authoring."""
  if isinstance(inputs, EmptyBusinessModelPin):
    raise PinBindsNoRequiredRoles(inputs.corpus_id)
  return bm_required_series(project_statement_facts(inputs).requirements)
