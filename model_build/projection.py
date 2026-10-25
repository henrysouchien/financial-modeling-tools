"""The per-period pin: each (required role, period) resolved from its own filing, at its selected vintage.

Owner: ``docs/closed/pin-resolves-each-period-and-vintage-from-its-own-filing.md`` (Invariant);
plan ``docs/design/build-model-package-boundary-plan-2026-10-05.md`` §2.4 (the corpus-facts lift,
landing here) and §2.6 (no product imports). Moved from ``scripts/research/bench/projection.py``,
which proved it (``docs/qa/financial-model-bench-acceptance-2026-10-05.md`` §2, §4b, §4e) and is
now an adapter over this module.

- ``per_period_requirements``: one ``PeriodRequirement`` per (authored role, authored period), each
  in **that period's own** accession state from the ``statement_bindings.v2`` windows — a bound
  period keeps its pinned ``FilingLineRef``, a cited nonnumeric period its ``AbsenceCitation``
  and ``not_separately_presented`` its ``included_in`` parent. Only an actually unbound period
  carries a reason. Unadjudicated roles read the product pin per period.
- ``fact_request`` / ``project_facts_per_period``: the facts are read in two halves. The first names
  what to read (the per-period pin in requirement shape, the occurrence and schedule lines, the
  fiscal window); the caller reads it (occurrence fetch, closure adjudication, issuer, schedule and
  opening observations) and hands the observations back as ``FetchedFacts``. The second resolves
  each period's disposition and then its restatement vintage: the own filing's column and every
  later captured filing's comparative are candidates, linked by the pin's restatement-edge rule, and
  ``schema.vintage_selection.select_vintage`` binds the latest restated vintage as of the cutoff
  (``default_cutoff`` when none is declared), every candidate retained on ``PeriodFacts.vintages``.
- ``typed_values_from_projection``: each source cell as a ``schema.model_number_value`` value with
  its origin (the ``filing-line:`` observation id) or nonnumeric reason.

Inputs are typed values and the ``VerifiedCorpusProtocol`` / ``RoleStatementBindingProtocol`` Protocols declared here;
the sealed corpus and its decoded bindings arrive from the caller.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, fields as dataclass_fields, is_dataclass, replace
from datetime import UTC, datetime
from decimal import ROUND_HALF_EVEN, Context, Decimal, localcontext
import hashlib
import json
from pathlib import Path, PurePath
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal, Protocol

from schema.build_model_product_contracts import (
  StatementSourceMaterial,
  historical_evidence_id,
)
from schema.coverage_profile import (
  AbsenceCitation,
  AnnualCoverageProfile,
  AnnualSourceRequirement,
  PeriodPinCell,
)
from schema.build_population import (
  QuantityConversionGap,
  quantity_native_value,
  quantity_to_requirement_value,
  requirement_value_polarity,
)
from schema.provider_port.adapters.edgar.wire.filing_facts_v1 import EdgarFilingFactsEnvelopeV1
from schema.provider_port.adapters.edgar.wire.non_gaap_reconciliation_v1 import ISSUER_NON_GAAP_RECONCILIATION_ARTIFACT_KIND
from schema.model_number_value import (
  AbsenceDisposition,
  AbsentCause,
  CellUnavailableReason,
  DerivedKnownNumberValue,
  EvidencedAbsentNumberValue,
  is_absence_disposition,
  FormulaOriginBinding,
  InvalidCause,
  InvalidNumberValue,
  MissingCause,
  MissingNumberValue,
  ModelNumberValue,
  ObservationOriginBinding,
  ObservedKnownNumberValue,
)
from schema.provider_port.registry import EDGAR_PROVIDER_ID
from schema.provider_port.contracts import (
  FilingDimensionRef,
  FilingLineFact,
  FilingLineObservation,
  FilingLineRef,
  FiscalWindow,
  VendorProvenance,
  mint_source_fact_ref,
)
from schema.required_role_catalog import (
  RequiredRole,
  RequiredRoleCatalog,
  RequiredRoleUnknown,
  load_required_role_catalog,
)
from schema.stable_control_identity import FiscalPeriod
from schema.statement_bindings_v2 import NOT_DISCLOSED_IN_VINTAGE, KpiCitation
from schema.statement_topology import normalize_topology_role
from schema.versioned_artifact import (
  GlobalArtifactScope,
  VersionedArtifactRef,
  VersionedArtifactScope,
  format_versioned_artifact_uri,
)
from schema.vintage_selection import (
  SelectedVintage,
  SeriesIdentity,
  SeriesIdentityFn,
  VintageEvidenceRow,
  VintageMissingForPeriod,
  select_vintage,
)
from value_semantics_core import CONVENTIONS_IR_V1, FiscalYearPeriod, Quantity, normalize_decimal_v1

if TYPE_CHECKING:
  from model_build.live_sheet import LiveAxis


PeriodState = Literal["bound", "not_applicable", "not_separately_presented", "unbound"]
BindingSource = Literal[
  "statement_bindings",
  "statement_material",
  "issuer_evidence",
  "schedule",
  "reconciliation",
  "opening_balance",
  "pin",
  "none",
]
PeriodKey = tuple[str, str]

#: The corpus verifier's kind for one filing's sealed statement-occurrence capture
#: (``research.corpus.verify.EDGAR_STATEMENT_OCCURRENCES_ARTIFACT_KIND``, same value).
EDGAR_STATEMENT_OCCURRENCES_ARTIFACT_KIND = "edgar_statement_occurrences_json"
#: The source bundle's decimal context (``research.build_model_source_bundle``, same values).
SOURCE_BUNDLE_DECIMAL_CONTEXT = Context(
  prec=34,
  rounding=ROUND_HALF_EVEN,
  Emin=-999999,
  Emax=999999,
  capitals=1,
  clamp=0,
)
#: The ``presentation_state`` the caller's closure adjudication writes on the ``detail`` of an
#: occurrence gap its closure corroborates as zero.
_CLOSURE_ZERO = "closure_corroborated_zero"
_SOURCE_EXPRESSION_PATH = "source"
#: Powers of ten of each ``NumericScale`` multiplier (``unit`` 0, ``thousands`` 3, ...).
_SCALE_EXPONENT = {row.numeric_scale: Decimal(row.multiplier).adjusted() for row in CONVENTIONS_IR_V1.scale_factors}
_MAX_DISPLAY_PRECISION = 8  # ``NumericDisplaySpec.precision`` ``le``
_OCCURRENCE_SOURCES: frozenset[BindingSource] = frozenset({"statement_bindings", "statement_material", "pin"})
#: The ``native_endpoint`` of a composite period's observation, whose ``native_field`` is the
#: JSON component list (``composite_observation``).
COMPOSITE_ENDPOINT = "occurrence_composite"
_COMPOSITE_STATE = "composite_signed_sum"
_SIGNED_SUM = "signed_sum"


# --------------------------------------------------------------------------
# typed inputs
# --------------------------------------------------------------------------


class ProjectedCorpusFileProtocol(Protocol):
  """One sealed corpus file, as the resolver reads it."""

  @property
  def kind(self) -> str: ...

  @property
  def logical_id(self) -> str: ...

  @property
  def sha256(self) -> str: ...

  @property
  def accession(self) -> str | None: ...

  @property
  def relative_path(self) -> PurePath: ...

  @property
  def capture_metadata(self) -> Mapping[str, object]: ...


class VerifiedCorpusProtocol(Protocol):
  """The members of a verified corpus the resolver calls; the product's corpus implements it."""

  @property
  def root(self) -> Path: ...

  @property
  def corpus_id(self) -> str: ...

  @property
  def corpus_version(self) -> int: ...

  @property
  def manifest_sha256(self) -> str: ...

  @property
  def ticker(self) -> str: ...

  @property
  def files(self) -> Sequence[ProjectedCorpusFileProtocol]: ...

  def as_filed_accessions_for_period(self, period_label: str) -> frozenset[str]: ...

  def declared_period_labels(self) -> frozenset[str]: ...


class BoundStatementIdentity(Protocol):
  """The filing line one ``statement_bindings.v2`` window binds a role to."""

  @property
  def presentation_role_name(self) -> str: ...

  @property
  def line_qname(self) -> str: ...

  @property
  def dimensions(self) -> tuple[FilingDimensionRef, ...]: ...


@dataclass(frozen=True)
class StatementTerm:
  """One filed line of a composite, with its authored sign and the arc its row cites for
  the accession this term is built for."""

  identity: BoundStatementIdentity
  sign: int
  cited_arc: str | None = None


@dataclass(frozen=True)
class CompositeStatementIdentity:
  """What one window binds a role to when the role is a multi-line bound row."""

  terms: tuple[StatementTerm, ...]


@dataclass(frozen=True)
class PeriodTerm:
  """One filed line of a composite period: its column, sign and cited arc."""

  line: FilingLineRef
  sign: int
  cited_arc: str | None = None


class RoleStatementBindingProtocol(Protocol):
  """One role's decoded ``statement_bindings.v2`` windows, per covered accession."""

  @property
  def state_by_accession(self) -> Mapping[str, PeriodState]: ...

  @property
  def identity_by_accession(self) -> Mapping[str, BoundStatementIdentity | CompositeStatementIdentity]: ...

  @property
  def absence_citation_by_accession(self) -> Mapping[str, AbsenceCitation]: ...

  @property
  def included_in_by_accession(self) -> Mapping[str, str]: ...

  @property
  def unbound_reason_by_accession(self) -> Mapping[str, str]: ...

  @property
  def cited_arc_by_accession(self) -> Mapping[str, str]:
    """Bound accession → the presentation arc (``cell_locator``) its row cites in that filing."""
    ...


class FilingLineFetcher(Protocol):
  """Reads filing-line columns out of the sealed occurrence captures (``EdgarAdapter``).

  ``cited_arcs`` is line → the arc its binding cites in that filing, beside the line identity.
  """

  def fetch_filing_line_facts(
    self,
    ticker: str,
    *,
    lines: Sequence[FilingLineRef],
    window: FiscalWindow,
    cited_arcs: Mapping[FilingLineRef, str],
  ) -> tuple[tuple[FilingLineObservation, ...], object]: ...


@dataclass(frozen=True)
class PinSources:
  """The decoded bindings of a corpus and the sealed artifacts the per-period pin cites.

  ``statement_bindings`` is role → its decoded windows; ``issuer_artifact``,
  ``schedule_artifact`` and ``bindings_artifact`` are the corpus's issuer-evidence,
  schedule-evidence and ``statement_bindings.v2`` files (``None`` where the corpus has none).
  """

  statement_bindings: Mapping[str, RoleStatementBindingProtocol]
  issuer_artifact: ProjectedCorpusFileProtocol | None = None
  schedule_artifact: ProjectedCorpusFileProtocol | None = None
  bindings_artifact: ProjectedCorpusFileProtocol | None = None


# --------------------------------------------------------------------------
# per-period requirement state
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class PeriodRequirement:
  """One (required role, period) of the pin, in that period's own state.

  ``disposition`` is the ``AnnualPeriodCoverage`` disposition the period carries:
  ``not_applicable_evidenced`` / ``not_separately_presented`` /
  ``unknown_applicability`` from the state alone,
  and for a bound period ``None`` until
  ``project_facts_per_period`` reads its fact (``observed``, ``derived`` for a
  closure-corroborated zero, ``unsupported`` when the sealed capture holds none).
  ``cited_arc`` is the presentation arc the ``statement_bindings.v2`` row cites for
  ``pinned_ref`` in its filing (``None`` for any other source).
  """

  required_role: str
  period_id: str
  accession: str | None
  state: PeriodState
  pinned_ref: FilingLineRef | None
  kpi_citation: KpiCitation | None
  absence_citation: AbsenceCitation | None
  unbound_reason: str | None
  disposition: str | None
  requirement_id: str
  family: str
  model_item: str
  unit: str
  scale: str
  binding_source: BindingSource
  capture_id: str | None = None
  included_in: str | None = None
  cited_arc: str | None = None
  #: A composite period's filed lines (the bridge's (B) composite): ``pinned_ref`` is then
  #: the synthetic :func:`composite_line` and ``terms`` the real lines it sums.
  terms: tuple[PeriodTerm, ...] = ()

  @property
  def key(self) -> PeriodKey:
    return (self.required_role, self.period_id)

  def to_row(self) -> dict[str, Any]:
    """The flat dump row; field names follow the bench's ``keyed_dump.dump_requirements``."""
    return {
      "required_role": self.required_role,
      "period_id": self.period_id,
      "accession": self.accession,
      "requirement_id": self.requirement_id,
      "family": self.family,
      "model_item": self.model_item,
      "binding_state": self.state,
      "binding_source": self.binding_source,
      "pinned_ref": None if self.pinned_ref is None else _filing_line_json(self.pinned_ref),
      "kpi_citation": (
        None
        if self.kpi_citation is None
        else {
          "accession": self.kpi_citation.accession,
          "row": self.kpi_citation.row,
          "span": self.kpi_citation.span,
        }
      ),
      "capture_id": self.capture_id,
      "unbound_reason": self.unbound_reason,
      "absence_citation": (
        None if self.absence_citation is None else self.absence_citation.model_dump(mode="json")
      ),
      "included_in": self.included_in,
      "disposition": self.disposition,
      **(
        {}
        if not self.terms
        else {
          "composite_terms": [
            {**_filing_line_json(term.line), "sign": term.sign, "cited_arc": term.cited_arc}
            for term in self.terms
          ]
        }
      ),
    }


def _filing_line_json(line: FilingLineRef) -> dict[str, Any]:
  return {
    "accession": line.accession,
    "period": line.period.canonical_label(),
    "statement_role": line.statement_role,
    "tag": line.tag,
    "dimensions": [[dimension.axis_qname, dimension.member_qname] for dimension in line.dimensions],
  }


def per_period_requirements(
  corpus: VerifiedCorpusProtocol,
  profile: AnnualCoverageProfile,
  material: StatementSourceMaterial | None = None,
  *,
  sources: PinSources,
  pin: Sequence[AnnualSourceRequirement],
  issuer_columns: Mapping[str, Sequence[FilingLineRef]],
  reconciliation_columns: Mapping[str, Sequence[FilingLineRef]],
  schedule_columns: Mapping[str, Sequence[FilingLineRef]],
) -> list[PeriodRequirement]:
  """One row per (authored role, authored period), in that period's own accession state.

  A role ``statement_bindings.v2`` adjudicates (an operating role, or any role when
  no reviewed statement ``material`` owns the statement family) takes each period's
  state from the window that covers the period's one as-filed accession: ``bound``
  pins that window's line identity; ``not_applicable`` carries the window's citation;
  ``unbound`` the authored reason and the row's KPI
  citation for the accession. A period whose accession no window covers, or whose
  window omits the role, is ``unbound`` with that fact as its reason. An unbound
  period an issuer-evidence or reconciliation column covers is bound by it; a
  schedule column always binds. Every other role reads ``pin`` (the product pin's
  rows) per period.

  ``issuer_columns`` / ``reconciliation_columns`` / ``schedule_columns`` are role →
  the columns the issuer evidence, the non-GAAP reconciliations and the schedule
  evidence pin.
  """
  bindings = sources.statement_bindings
  pin_by_role = {row.required_role: row for row in pin}
  window_accessions = _window_accessions(bindings)
  kpi_by_role_accession = _kpi_citations(corpus, sources.bindings_artifact)
  capture_by_accession = _captures_of_kind(corpus, EDGAR_STATEMENT_OCCURRENCES_ARTIFACT_KIND)
  reconciliation_capture_by_accession = _captures_of_kind(corpus, ISSUER_NON_GAAP_RECONCILIATION_ARTIFACT_KIND)
  issuer_capture = None if sources.issuer_artifact is None else sources.issuer_artifact.logical_id
  schedule_capture = None if sources.schedule_artifact is None else sources.schedule_artifact.logical_id
  schedule_lines = frozenset(line for columns in schedule_columns.values() for line in columns)
  issuer_lines = frozenset(line for columns in issuer_columns.values() for line in columns)
  reconciliation_lines = frozenset(
    line for columns in reconciliation_columns.values() for line in columns
  )

  def source_of(line: FilingLineRef) -> tuple[BindingSource, str | None]:
    if line in schedule_lines:
      return "schedule", schedule_capture
    if line in reconciliation_lines:
      return "reconciliation", reconciliation_capture_by_accession.get(line.accession)
    if line in issuer_lines or line.tag.startswith("issuer:"):
      return "issuer_evidence", issuer_capture
    if material is not None:
      return "statement_material", capture_by_accession.get(line.accession)
    return "pin", capture_by_accession.get(line.accession)

  rows: list[PeriodRequirement] = []
  for base in profile.annual_requirements:
    role = base.required_role
    binding = bindings.get(role)
    if material is not None and not role.startswith("operating."):
      binding = None
    fills = {
      line.period.canonical_label(): line
      for line in (*reconciliation_columns.get(role, ()), *issuer_columns.get(role, ()))
    }
    schedule_by_period = {line.period.canonical_label(): line for line in schedule_columns.get(role, ())}
    for period_id in base.period_ids:
      accession = _single_accession(corpus, period_id)
      seed = PeriodRequirement(
        required_role=role,
        period_id=period_id,
        accession=accession,
        state="unbound",
        pinned_ref=None,
        kpi_citation=None,
        absence_citation=None,
        unbound_reason=None,
        disposition="unknown_applicability",
        requirement_id=base.requirement_id,
        family=base.family,
        model_item=base.model_item,
        unit=base.unit,
        scale=base.scale,
        binding_source="none",
      )
      if binding is not None:
        row = _binding_period(
          seed,
          base=base,
          binding=binding,
          corpus=corpus,
          window_accessions=window_accessions,
          kpi_citation=kpi_by_role_accession.get((role, accession or "")),
          capture_id=capture_by_accession.get(accession or ""),
        )
      else:
        row = _pin_period(seed, pin_row=pin_by_role.get(role), source_of=source_of)
      if row.state == "unbound" and period_id in fills:
        line = fills[period_id]
        source, capture = source_of(line)
        row = _bound(row, line, source=source, capture_id=capture)
      if period_id in schedule_by_period:
        row = _bound(row, schedule_by_period[period_id], source="schedule", capture_id=schedule_capture)
      rows.append(row)
  return rows


def _captures_of_kind(corpus: VerifiedCorpusProtocol, kind: str) -> dict[str, str]:
  """Accession → the logical id of the corpus file of ``kind`` captured from that filing."""
  return {
    str(item.accession): item.logical_id
    for item in corpus.files
    if item.kind == kind and item.accession is not None
  }


def _occurrence_files(corpus: VerifiedCorpusProtocol) -> dict[str, ProjectedCorpusFileProtocol]:
  return {
    str(item.accession): item
    for item in corpus.files
    if item.kind == EDGAR_STATEMENT_OCCURRENCES_ARTIFACT_KIND and item.accession is not None
  }


def _window_accessions(bindings: Mapping[str, RoleStatementBindingProtocol]) -> frozenset[str]:
  """Every accession some approved ``statement_bindings.v2`` window covers."""
  return frozenset(accession for binding in bindings.values() for accession in binding.state_by_accession)


def _single_accession(corpus: VerifiedCorpusProtocol, period_id: str) -> str | None:
  accessions = corpus.as_filed_accessions_for_period(period_id)
  return next(iter(accessions)) if len(accessions) == 1 else None


def _window_line(identity: BoundStatementIdentity, accession: str, fiscal_year: int) -> FilingLineRef:
  """The column ``accession`` states for ``fiscal_year`` on the line its window binds."""
  return FilingLineRef(
    accession=accession,
    period=FiscalYearPeriod(kind="fiscal_year", fiscal_year=fiscal_year),
    statement_role=normalize_topology_role(identity.presentation_role_name),
    tag=identity.line_qname,
    dimensions=identity.dimensions,
  )


def composite_line(required_role: str, first: FilingLineRef) -> FilingLineRef:
  """The synthetic line a composite period sits on: the first term's accession, period and
  statement, under the tag ``composite:<required_role>`` (no filer's taxonomy prefix is
  ``composite``, and the role is the composite's own identity, so it names no real line)."""
  return FilingLineRef(
    accession=first.accession,
    period=first.period,
    statement_role=first.statement_role,
    tag=f"composite:{required_role}",
  )


def concept_balances_from_envelopes(
  envelopes: Mapping[str, EdgarFilingFactsEnvelopeV1],
) -> dict[tuple[str, str], str | None]:
  """Read economic-sign metadata without changing sealed observation provenance."""
  return {
    (accession, qname): concept.balance
    for accession, envelope in envelopes.items()
    for qname, concept in (envelope.concepts or {}).items()
  }


def _required_role_or_none(required_role: str) -> RequiredRole | None:
  try:
    return load_required_role_catalog().role(required_role)
  except RequiredRoleUnknown:
    return None


def _fact_requirement_value(
  fact: FilingLineFact,
  *,
  required_role: str,
  unit: str,
  scale: str,
  concept_balances: Mapping[tuple[str, str], str | None] | None = None,
) -> Decimal:
  """Route a role Quantity with its presentation directive and sealed concept balance.

  The balance orients a value read from the filing concept it names, so only an EDGAR
  fact looks it up. A vendor cross-check (FMP) is keyed to the same filing line but is
  the vendor's own number in the vendor's own convention: it keeps that sign.
  """
  role = _required_role_or_none(required_role)
  statement = None if role is None else role.statement
  provenance = fact.provenance
  # Composite terms are already economic and summed in the requirement's scale.
  # Their Quantity carries display precision, not another native observation.
  value = fact.value
  if provenance is not None and provenance.native_endpoint == COMPOSITE_ENDPOINT and isinstance(value, Quantity):
    value = Decimal(value.value)
  return quantity_to_requirement_value(
    value,
    unit=unit,
    scale=scale,
    statement=statement,
    preferred_label_negates_value=(
      provenance is not None and provenance.preferred_label_negates_value is True
    ),
    role_polarity=None if role is None else role.polarity,
    concept_balances=concept_balances,
    concept_key=(
      (fact.line.accession, fact.line.tag)
      if provenance is not None and provenance.provider == EDGAR_PROVIDER_ID
      else None
    ),
  )


def composite_observation(
  line: FilingLineRef,
  terms: Sequence[PeriodTerm],
  found: Mapping[FilingLineRef, FilingLineObservation],
  *,
  required_role: str,
  unit: str,
  scale: str,
  concept_balances: Mapping[tuple[str, str], str | None] | None = None,
) -> FilingLineObservation:
  """One composite period's observation: the signed sum of its filed lines' columns.

  Every line must have an ok observation, else the composite is not formed (``missing``,
  naming the lines): never a partial sum and never zero. The fact is ``adapter_asserted``
  on ``line`` with endpoint ``occurrence_composite``, and its ``native_field`` is the JSON
  component list :func:`composite_components` reads back.

  What ``term.sign`` multiplies depends on the role's statement. For an income-statement
  role each term is first oriented to the role (native value, times -1 when its concept's
  balance opposes the role's catalog polarity), so the sign is the term's economic
  coefficient, normally +1: a debit interest-expense line under an income-positive role
  already contributes negatively and takes +1. A calculation-arc weight is native-relative
  and must not be copied there. Balance-sheet and cash-flow terms keep the filed value, so
  their sign is the calculation weight.
  """
  with localcontext(SOURCE_BUNDLE_DECIMAL_CONTEXT):
    components: list[dict[str, Any]] = []
    gaps: list[dict[str, Any]] = []
    total = Decimal(0)
    decimals = 0
    target_scale = "unit" if scale == "units" else scale
    first: Quantity | None = None
    for term in terms:
      observation = found.get(term.line)
      fact = None if observation is None else observation.fact
      if observation is None or observation.status != "ok" or fact is None:
        gaps.append(_filing_line_json(term.line))
        continue
      try:
        value = _fact_requirement_value(
          fact, required_role=required_role, unit=unit, scale=scale,
          concept_balances=concept_balances,
        )
      except QuantityConversionGap as exc:
        return FilingLineObservation(
          line, "failed", None,
          json.dumps(
            {"presentation_state": "composite_unconvertible", "line": _filing_line_json(term.line), "reason": str(exc)},
            sort_keys=True, separators=(",", ":"), ensure_ascii=False,
          ),
        )
      total += term.sign * value
      if isinstance(fact.value, Quantity):
        if first is None:
          first = fact.value
        # The printed resolution of this line, in the requirement's scale: a line printed to
        # ``precision`` places in its own scale resolves ``source - target`` powers of ten finer.
        decimals = max(
          decimals,
          fact.value.display.precision + _SCALE_EXPONENT[target_scale] - _SCALE_EXPONENT[fact.value.display.scale],
        )
      component: dict[str, Any] = {
        **_filing_line_json(term.line),
        "sign": term.sign,
        "cited_arc": term.cited_arc,
        "value": format(value, "f"),
      }
      # A member the closure adjudication derived as zero records that derivation beside
      # its filed siblings, so the composite's readers see which terms were printed blank.
      if closure_corroborated_zero(observation):
        component["derivation"] = "closure_corroborated_zero"
      components.append(component)
    if gaps or first is None:
      return FilingLineObservation(
        line, "missing", None,
        json.dumps(
          {"presentation_state": "composite_incomplete", "missing": gaps},
          sort_keys=True, separators=(",", ":"), ensure_ascii=False,
        ),
      )
    quantity = Quantity.model_validate({
      "value": normalize_decimal_v1(total),
      "display": {
        "numeric_kind": first.display.numeric_kind,
        "scale": target_scale,
        "currency": first.display.currency,
        "denominator_basis": first.display.denominator_basis,
        "precision": min(max(decimals, 0), _MAX_DISPLAY_PRECISION),
        "unit_label": first.display.unit_label,
      },
    })
    native_field = json.dumps(
      {"derivation": _SIGNED_SUM, "components": components},
      sort_keys=True, separators=(",", ":"), ensure_ascii=False,
    )
    return FilingLineObservation(
      line=line,
      status="ok",
      fact=FilingLineFact(
        line=line,
        value=quantity,
        grade="adapter_asserted",
        provenance=VendorProvenance(
          provider="edgar", native_endpoint=COMPOSITE_ENDPOINT, native_field=native_field,
        ),
        label=required_role,
        source_ref=mint_source_fact_ref(line, "statement"),
      ),
      detail=json.dumps(
        {"presentation_state": _COMPOSITE_STATE, "terms": len(components)},
        sort_keys=True, separators=(",", ":"), ensure_ascii=False,
      ),
    )


def composite_components(observation: FilingLineObservation) -> tuple[Mapping[str, Any], ...] | None:
  """The filed lines a composite observation sums, each ``{accession, period, statement_role,
  tag, dimensions, sign, cited_arc, value}``; ``None`` for any other observation."""
  fact = observation.fact
  provenance = None if fact is None else fact.provenance
  if provenance is None or provenance.native_endpoint != COMPOSITE_ENDPOINT:
    return None
  return tuple(json.loads(provenance.native_field)["components"])


def _bound(
  row: PeriodRequirement,
  line: FilingLineRef,
  *,
  source: BindingSource,
  capture_id: str | None,
  cited_arc: str | None = None,
) -> PeriodRequirement:
  return replace(
    row,
    state="bound",
    accession=line.accession,
    pinned_ref=line,
    absence_citation=None,
    included_in=None,
    unbound_reason=None,
    disposition=None,
    binding_source=source,
    capture_id=capture_id,
    cited_arc=cited_arc,
    terms=(),
  )


def _binding_period(
  seed: PeriodRequirement,
  *,
  base: AnnualSourceRequirement,
  binding: RoleStatementBindingProtocol,
  corpus: VerifiedCorpusProtocol,
  window_accessions: frozenset[str],
  kpi_citation: KpiCitation | None,
  capture_id: str | None,
) -> PeriodRequirement:
  accession = seed.accession
  row = replace(seed, binding_source="statement_bindings", capture_id=capture_id)
  if accession is None:
    accessions = sorted(corpus.as_filed_accessions_for_period(seed.period_id))
    return replace(
      row,
      unbound_reason=f"{seed.period_id} has {len(accessions)} as-filed accessions {accessions}",
    )
  if accession not in window_accessions:
    return replace(
      row,
      unbound_reason=f"no approved statement_bindings.v2 window covers {accession} ({seed.period_id})",
    )
  state = binding.state_by_accession.get(accession)
  if state is None:
    # An omission is the window gap ``statement_binding_window_gaps`` reports, never a judgment that
    # the filing does not print the line: ``not_disclosed_in_vintage`` is read only from a stored
    # ``unbound`` row (the final return below), declared by the agent after reading the matrix.
    return replace(row, unbound_reason=f"the statement_bindings.v2 window covering {accession} omits {seed.required_role}")
  if state == "bound":
    identity = binding.identity_by_accession.get(accession)
    if identity is None:
      raise ValueError(
        "required_column_absent_from_pin: statement_bindings.v2 "
        f"{base.required_role!r} has no bound citation for {accession}"
      )
    year = int(seed.period_id.removeprefix("FY"))
    if isinstance(identity, CompositeStatementIdentity):
      terms = tuple(
        PeriodTerm(_window_line(term.identity, accession, year), term.sign, term.cited_arc)
        for term in identity.terms
      )
      return replace(
        _bound(
          row,
          composite_line(base.required_role, terms[0].line),
          source="statement_bindings",
          capture_id=capture_id,
        ),
        terms=terms,
      )
    line = _window_line(identity, accession, year)
    return _bound(
      row,
      line,
      source="statement_bindings",
      capture_id=capture_id,
      cited_arc=binding.cited_arc_by_accession.get(accession),
    )
  if state == "not_applicable":
    return replace(
      row,
      state="not_applicable",
      absence_citation=binding.absence_citation_by_accession[accession],
      disposition="not_applicable_evidenced",
    )
  if state == "not_separately_presented":
    return replace(
      row,
      state=state,
      absence_citation=binding.absence_citation_by_accession[accession],
      included_in=binding.included_in_by_accession[accession],
      disposition=state,
    )
  return replace(
    row,
    unbound_reason=binding.unbound_reason_by_accession.get(accession),
    kpi_citation=kpi_citation,
  )


def _pin_period(
  seed: PeriodRequirement,
  *,
  pin_row: AnnualSourceRequirement | None,
  source_of: Callable[[FilingLineRef], tuple[BindingSource, str | None]],
) -> PeriodRequirement:
  if pin_row is None or seed.period_id not in pin_row.period_ids:
    return replace(
      seed,
      unbound_reason=f"absent from the pin: nothing binds {seed.required_role}/{seed.period_id}",
    )
  cell = pin_row.cell_for_period(seed.period_id)
  line = cell.pinned_ref
  if line is not None:
    source, capture = source_of(line)
    return _bound(seed, line, source=source, capture_id=capture)
  if cell.binding_state == "not_applicable":
    return replace(
      seed,
      state="not_applicable",
      absence_citation=cell.absence_citation,
      disposition="not_applicable_evidenced",
      binding_source="pin",
    )
  if cell.binding_state == "not_separately_presented":
    return replace(
      seed,
      accession=None if cell.absence_citation is None else cell.absence_citation.accession,
      state=cell.binding_state,
      absence_citation=cell.absence_citation,
      included_in=cell.included_in,
      disposition=cell.binding_state,
      binding_source="pin",
    )
  return replace(
    seed,
    binding_source="pin",
    unbound_reason=cell.unbound_reason
    or f"absent from the pin: nothing binds {seed.required_role}/{seed.period_id}",
  )



def _kpi_citations(
  corpus: VerifiedCorpusProtocol,
  artifact: ProjectedCorpusFileProtocol | None,
) -> dict[tuple[str, str], KpiCitation]:
  """(role, accession) → the KPI row an ``unbound`` operating row cites for that accession.

  The bindings decode drops the citations; the sealed ``statement_bindings.v2`` artifact
  (whose digest that decode verified) is read for them here.
  """
  if artifact is None:
    return {}
  payload = json.loads(corpus.root.joinpath(*artifact.relative_path.parts).read_bytes())
  out: dict[tuple[str, str], KpiCitation] = {}
  for window in payload.get("vintage_windows", ()):
    for role, raw in (window.get("rows") or {}).items():
      for citation in (raw or {}).get("kpi_citations") or ():
        out[(role, str(citation["accession"]))] = KpiCitation(
          accession=str(citation["accession"]),
          row=int(citation["row"]),
          span=str(citation["span"]),
        )
  return out


# --------------------------------------------------------------------------
# facts
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class FactRequest:
  """What ``project_facts_per_period`` needs read, from the per-period rows.

  ``requirements`` is each role's authored requirement narrowed to its bound periods (the
  shape the caller's readers take); ``statement_lines`` the occurrence columns to fetch and
  adjudicate; ``cited_arcs`` each such column's cited arc, passed beside the lines to the
  fetch; ``schedule_lines`` the schedule-evidence columns; ``window`` the fiscal window
  the rows span.
  """

  requirements: tuple[AnnualSourceRequirement, ...]
  statement_lines: tuple[FilingLineRef, ...]
  cited_arcs: Mapping[FilingLineRef, str]
  schedule_lines: frozenset[FilingLineRef]
  window: FiscalWindow


@dataclass(frozen=True)
class FetchedFacts:
  """The caller's answer to a ``FactRequest``.

  ``occurrence_observations``: the fetched occurrence columns after closure adjudication (a
  closure-corroborated gap is an ok zero whose ``detail`` says so); ``closure_adjudications``:
  each adjudicated period's verdict; ``issuer_observations`` / ``schedule_observations``: the
  issuer- and schedule-evidence columns; ``opening_by_role``: each role's opening-balance column.
  """

  occurrence_observations: Sequence[FilingLineObservation] = ()
  closure_adjudications: Mapping[PeriodKey, object] = field(default_factory=dict)
  issuer_observations: Sequence[FilingLineObservation] = ()
  schedule_observations: Sequence[FilingLineObservation] = ()
  opening_by_role: Mapping[str, FilingLineObservation] = field(default_factory=dict)
  concept_balances: Mapping[tuple[str, str], str | None] = field(default_factory=dict)


@dataclass(frozen=True)
class PeriodFacts:
  """The observations of the per-period pin and the refs a value's origin cites.

  ``rows`` are the input rows with each bound period's disposition read from its
  fact, plus the opening-balance rows. ``source_refs`` names the sealed artifact
  each observation was read from; ``snapshot_ref`` is the corpus version that
  holds them; ``bindings_ref`` is the sealed ``statement_bindings.v2`` artifact.
  A statement-binding period's row, observation and source ref are its selected
  vintage's; ``vintages`` keeps every candidate of a period that has more than its
  own filing's column, or whose own column the cutoff does not admit;
  ``cutoff_at`` is the cutoff selection ran at and ``filed_at`` each captured
  filing's acceptance (``filing_available_at``).
  """

  rows: tuple[PeriodRequirement, ...]
  observations: Mapping[PeriodKey, FilingLineObservation]
  source_refs: Mapping[PeriodKey, VersionedArtifactRef]
  snapshot_ref: VersionedArtifactRef
  bindings_ref: VersionedArtifactRef | None = None
  closure_adjudications: Mapping[PeriodKey, object] = field(default_factory=dict)
  vintages: Mapping[PeriodKey, PeriodVintages] = field(default_factory=dict)
  cutoff_at: datetime | None = None
  filed_at: Mapping[str, datetime] = field(default_factory=dict)
  concept_balances: Mapping[tuple[str, str], str | None] = field(default_factory=dict)


def fact_request(
  profile: AnnualCoverageProfile, rows: Sequence[PeriodRequirement], *, axis: LiveAxis | None = None,
) -> FactRequest:
  """The reads ``rows`` need: occurrence lines are the statement-binding, material and pin
  columns (issuer-tagged lines excepted), each with the arc its binding row cites; schedule
  lines the schedule columns. ``axis`` is the window when ``rows`` is empty (a live sheet whose
  stages are not persisted yet)."""
  bound = [row for row in rows if row.pinned_ref is not None]
  occurrence_rows = [(line, arc) for _row, line, arc in _occurrence_cites(bound)]
  schedule_lines = frozenset(
    row.pinned_ref for row in bound if row.pinned_ref is not None and row.binding_source == "schedule"
  )
  return FactRequest(
    requirements=_synthetic_requirements(profile, bound),
    statement_lines=tuple(dict.fromkeys(line for line, _arc in occurrence_rows)),
    cited_arcs=_cited_arcs(occurrence_rows),
    schedule_lines=schedule_lines,
    window=_fiscal_window(rows, axis=axis),
  )


def _occurrence_cites(rows: Iterable[PeriodRequirement]) -> list[tuple[PeriodRequirement, FilingLineRef, str | None]]:
  """Every occurrence line the rows read, with the row that reads it and the arc that row cites: the
  statement-binding, material and pin rows' pinned line (a composite's term lines), issuer-tagged lines
  excepted. ``fact_request`` fetches these lines with ``_cited_arcs`` of their arcs, and
  ``moved_role_set`` groups the roles that read one line by the same list."""
  return [
    (row, line, arc)
    for row in rows
    if row.pinned_ref is not None
    and row.binding_source in _OCCURRENCE_SOURCES
    and not row.pinned_ref.tag.startswith("issuer:")
    for line, arc in (
      [(term.line, term.cited_arc) for term in row.terms]
      if row.terms else [(row.pinned_ref, row.cited_arc)]
    )
  ]


def _cited_arcs(cited: Iterable[tuple[FilingLineRef, str | None]]) -> Mapping[FilingLineRef, str]:
  """Line → the one arc its binding rows cite. A line two rows cite on different arcs is
  left uncited: one observation per line cannot carry both, so its rivals stay rivals."""
  arcs: dict[FilingLineRef, set[str]] = {}
  for line, arc in cited:
    if arc is not None:
      arcs.setdefault(line, set()).add(arc)
  return MappingProxyType({line: next(iter(found)) for line, found in arcs.items() if len(found) == 1})


def _fiscal_window(rows: Sequence[PeriodRequirement], *, axis: LiveAxis | None = None) -> FiscalWindow:
  if not rows:
    if axis is None:
      raise ValueError("fiscal window needs bound period requirements or a live axis")
    return FiscalWindow(end_fiscal_year=axis.historical_years[-1], n_years=len(axis.historical_years))
  years = sorted({int(row.period_id.removeprefix("FY")) for row in rows})
  return FiscalWindow(end_fiscal_year=years[-1], n_years=years[-1] - years[0] + 1)


def project_facts_per_period(
  corpus: VerifiedCorpusProtocol,
  rows: Sequence[PeriodRequirement],
  *,
  sources: PinSources,
  fetched: FetchedFacts,
  adapter: FilingLineFetcher,
  window: FiscalWindow,
  scope: VersionedArtifactScope | None = None,
  cutoff_at: datetime | None = None,
  last_actual: int | None = None,
) -> PeriodFacts:
  """Each bound period's fact and disposition, then each statement-binding period's vintage.

  ``fetched`` answers ``fact_request(profile, rows)``; ``window`` is that request's. An
  occurrence observation is sourced to its filing's capture, an issuer one to the issuer
  evidence, a schedule one to the schedule evidence; each role bound at the first axis year
  gets an opening-balance row from ``fetched.opening_by_role``. Bound years of a mixed role are
  read like any other. Then each statement-binding period takes its selected vintage as of
  ``cutoff_at`` (``_select_vintages``; default ``default_cutoff``), later filings' comparatives
  read through ``adapter``.
  """
  artifact_scope: VersionedArtifactScope = GlobalArtifactScope() if scope is None else scope
  occurrence_files = _occurrence_files(corpus)
  by_line: dict[FilingLineRef, tuple[FilingLineObservation, ProjectedCorpusFileProtocol | None]] = {}
  for observation in fetched.occurrence_observations:
    by_line[observation.line] = (observation, occurrence_files.get(observation.line.accession))
  for observation in fetched.issuer_observations:
    by_line.setdefault(observation.line, (observation, sources.issuer_artifact))
  for observation in fetched.schedule_observations:
    by_line[observation.line] = (observation, sources.schedule_artifact)

  base_by_role = {row.required_role: row for row in rows}
  opening_rows: list[PeriodRequirement] = []
  for role, observation in sorted(fetched.opening_by_role.items()):
    template = base_by_role[role]
    line = observation.line
    artifact = occurrence_files.get(line.accession)
    opening_rows.append(
      replace(
        template,
        period_id=line.period.canonical_label(),
        accession=line.accession,
        state="bound",
        pinned_ref=line,
        kpi_citation=None,
        absence_citation=None,
        included_in=None,
        unbound_reason=None,
        disposition=None,
        binding_source="opening_balance",
        capture_id=None if artifact is None else artifact.logical_id,
      )
    )
    by_line[line] = (observation, artifact)

  # A composite period's one observation is the signed sum of its filed lines' observations,
  # read by the lines it names (``composite_observation``); the synthetic line it sits on is
  # never fetched.
  for row in rows:
    if row.terms and row.pinned_ref is not None:
      observation = composite_observation(
        row.pinned_ref, row.terms,
        {line: found[0] for line, found in by_line.items()},
        required_role=row.required_role, unit=row.unit, scale=row.scale,
        concept_balances=fetched.concept_balances,
      )
      by_line[row.pinned_ref] = (observation, occurrence_files.get(row.pinned_ref.accession))

  observations: dict[PeriodKey, FilingLineObservation] = {}
  source_refs: dict[PeriodKey, VersionedArtifactRef] = {}
  resolved: list[PeriodRequirement] = []
  for row in (*opening_rows, *rows):
    if row.pinned_ref is None:
      resolved.append(row)
      continue
    found = by_line.get(row.pinned_ref)
    if found is None:
      # A reconciliation pin's fact reaches the SourceBundle as a HistoricalBasisFact,
      # outside the statement-fact projection: its disposition stays unread here, not
      # ``unsupported``.
      resolved.append(
        row if row.binding_source == "reconciliation" else replace(row, disposition="unsupported")
      )
      continue
    observation, artifact = found
    observations[row.key] = observation
    if artifact is not None:
      source_refs[row.key] = corpus_artifact_ref(corpus, artifact, scope=artifact_scope)
    if observation.status != "ok" or observation.fact is None:
      disposition = "unsupported"
    elif closure_corroborated_zero(observation) or composite_components(observation) is not None:
      disposition = "derived"
    else:
      disposition = "observed"
    resolved.append(replace(row, disposition=disposition))
  filed_at = filing_available_at(corpus)
  own_year = filing_years(corpus)
  cutoff = default_cutoff(filed_at, own_year=own_year, last_actual=last_actual) if cutoff_at is None else cutoff_at
  with localcontext(SOURCE_BUNDLE_DECIMAL_CONTEXT):
    resolved, vintages = _select_vintages(
      corpus,
      resolved,
      bindings=sources.statement_bindings,
      adapter=adapter,
      window=window,
      observations=observations,
      source_refs=source_refs,
      occurrence_files=occurrence_files,
      scope=artifact_scope,
      filed_at=filed_at,
      cutoff_at=cutoff,
      concept_balances=fetched.concept_balances,
    )
  resolved.sort(key=lambda row: (row.required_role, row.period_id))
  return PeriodFacts(
    rows=tuple(resolved),
    observations=observations,
    source_refs=source_refs,
    snapshot_ref=corpus_snapshot_ref(corpus, scope=artifact_scope),
    bindings_ref=(
      None
      if sources.bindings_artifact is None
      else corpus_artifact_ref(corpus, sources.bindings_artifact, scope=artifact_scope)
    ),
    closure_adjudications=dict(fetched.closure_adjudications),
    vintages=vintages,
    cutoff_at=cutoff,
    filed_at=filed_at,
    concept_balances=fetched.concept_balances,
  )


def _synthetic_requirements(
  profile: AnnualCoverageProfile,
  bound: Sequence[PeriodRequirement],
) -> tuple[AnnualSourceRequirement, ...]:
  """Each role's bound periods and cells, for the caller's capture readers."""
  authored = {row.required_role: row for row in profile.annual_requirements}
  lines_by_role: dict[str, list[FilingLineRef]] = {}
  for row in bound:
    if row.pinned_ref is not None:
      lines_by_role.setdefault(row.required_role, []).append(row.pinned_ref)
  out: list[AnnualSourceRequirement] = []
  for role, lines in lines_by_role.items():
    ordered = tuple(sorted(lines, key=lambda line: line.period.canonical_label()))
    out.append(
      authored[role].model_copy(
        update={
          "period_ids": tuple(line.period.canonical_label() for line in ordered),
          "pinned_columns": ordered,
          "period_cells": tuple(
            PeriodPinCell(
              period_id=line.period.canonical_label(),
              binding_state="bound",
              pinned_ref=line,
            )
            for line in ordered
          ),
          "binding_state": "bound",
          "absence_citation": None,
          "included_in": None,
          "unbound_reason": None,
        }
      )
    )
  return tuple(sorted(out, key=lambda row: row.requirement_id))


def closure_corroborated_zero(observation: FilingLineObservation) -> bool:
  """``True`` for the ok observation the closure adjudication derived as zero."""
  if observation.status != "ok" or not observation.detail:
    return False
  try:
    detail = json.loads(observation.detail)
  except ValueError:
    return False
  return isinstance(detail, Mapping) and detail.get("presentation_state") == _CLOSURE_ZERO


def corpus_snapshot_ref(
  corpus: VerifiedCorpusProtocol,
  *,
  scope: VersionedArtifactScope | None = None,
) -> VersionedArtifactRef:
  """The sealed corpus version as a ``VersionedArtifactRef`` (content hash: its manifest)."""
  return _ref(
    scope=GlobalArtifactScope() if scope is None else scope,
    artifact_kind="verified_corpus",
    artifact_id=corpus.corpus_id,
    artifact_version=int(corpus.corpus_version),
    payload_schema_version="verified-corpus-manifest",
    content_sha256=corpus.manifest_sha256,
  )


def corpus_artifact_ref(
  corpus: VerifiedCorpusProtocol,
  artifact: ProjectedCorpusFileProtocol,
  *,
  scope: VersionedArtifactScope | None = None,
) -> VersionedArtifactRef:
  """One sealed corpus file at the corpus version that holds it."""
  return _ref(
    scope=GlobalArtifactScope() if scope is None else scope,
    artifact_kind=str(artifact.kind),
    artifact_id=str(artifact.logical_id),
    artifact_version=int(corpus.corpus_version),
    payload_schema_version=str(artifact.kind),
    content_sha256=str(artifact.sha256),
  )


def _ref(
  *,
  scope: VersionedArtifactScope,
  artifact_kind: str,
  artifact_id: str,
  artifact_version: int,
  payload_schema_version: str,
  content_sha256: str,
) -> VersionedArtifactRef:
  return VersionedArtifactRef(
    ref_schema_version="versioned-artifact-ref.v1",
    provider_id="bench.verified_corpus",
    artifact_kind=artifact_kind,
    artifact_id=artifact_id,
    artifact_version=artifact_version,
    scope=scope,
    uri=format_versioned_artifact_uri(
      scope=scope,
      artifact_kind=artifact_kind,
      artifact_id=artifact_id,
      artifact_version=artifact_version,
    ),
    payload_schema_version=payload_schema_version,
    media_type="application/json",
    digest_codec="raw_bytes_v1",
    content_sha256=content_sha256,
  )


# --------------------------------------------------------------------------
# restatement vintages
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class VintageEvidence:
  """One vintage of one (role, period): that period's column in the filing it was read from.

  The ``schema.vintage_selection.VintageEvidenceRow`` shape: ``source_id`` is the accession,
  ``available_at`` that filing's acceptance, ``restates_evidence_id`` the vintage this one
  restates (``_lineage``). ``own`` marks the period's own filing's column. ``value`` and
  ``resolution`` (one unit of the column's last stated digit) are in the requirement's scale.
  """

  evidence_id: str
  source_id: str
  available_at: datetime
  restates_evidence_id: str | None
  line: FilingLineRef
  period_id: str
  required_role: str
  value: Decimal
  resolution: Decimal
  observation: FilingLineObservation
  source_ref: VersionedArtifactRef | None
  capture_id: str | None
  own: bool


@dataclass(frozen=True)
class PeriodVintages:
  """Every vintage candidate of one (role, period), oldest first, and what selection bound as
  of ``cutoff_at``: ``outcome`` is the selection's code (``vintage_selected`` /
  ``vintage_missing_for_period``), ``selected`` the bound candidate's evidence id."""

  cutoff_at: datetime
  candidates: tuple[VintageEvidence, ...]
  outcome: str
  selected: str | None

  def ordered(self) -> list[VintageEvidence]:
    """The selected candidate first, then the others newest first."""
    rest = sorted(
      (candidate for candidate in self.candidates if candidate.evidence_id != self.selected),
      key=lambda candidate: (candidate.available_at, candidate.source_id),
      reverse=True,
    )
    chosen = [candidate for candidate in self.candidates if candidate.evidence_id == self.selected]
    return [*chosen, *rest]


def filing_moment(accepted_at: object, filing_date: object) -> datetime | None:
  """The one vintage-date rule for a filing: its ``accepted_at``, else its ``filing_date`` at
  00:00 UTC; ``None`` when it carries neither (an undated filing)."""
  stamp = accepted_at or filing_date
  if not stamp:
    return None
  moment = datetime.fromisoformat(str(stamp).replace("Z", "+00:00"))
  return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


def filing_available_at(corpus: VerifiedCorpusProtocol) -> dict[str, datetime]:
  """Each captured filing's acceptance by accession: its sealed occurrence entry's
  ``accepted_at`` (the manifest's ``capture_metadata``), else its ``filing_date`` at 00:00 UTC
  (``filing_moment``). A filing with neither is undated and is no vintage source."""
  out: dict[str, datetime] = {}
  for item in corpus.files:
    if item.kind != EDGAR_STATEMENT_OCCURRENCES_ARTIFACT_KIND or item.accession is None:
      continue
    metadata = item.capture_metadata or {}
    moment = filing_moment(metadata.get("accepted_at"), metadata.get("filing_date"))
    if moment is not None:
      out[str(item.accession)] = moment
  return out


def filing_years(corpus: VerifiedCorpusProtocol) -> dict[str, int]:
  """Each captured filing's own fiscal year by accession (the period it is the as-filed filing of)."""
  return {
    accession: int(label.removeprefix("FY"))
    for label in corpus.declared_period_labels()
    for accession in corpus.as_filed_accessions_for_period(label)
  }


def default_cutoff(
  filed_at: Mapping[str, datetime], *, own_year: Mapping[str, int] | None = None, last_actual: int | None = None,
) -> datetime:
  """A build's cutoff when it declares none. With no declared ``last_actual``: the latest
  captured filing's acceptance (the model follows the latest reported filing). With one: the
  latest acceptance among the filings whose own fiscal year is at or before it — a build at
  ``last_actual: N`` is the model as of N's filing, so a captured N+1 filing is no vintage of
  it unless the build names a cutoff that admits it."""
  if last_actual is None or own_year is None:
    return max(filed_at.values(), default=datetime.max.replace(tzinfo=UTC))
  admitted = [at for accession, at in filed_at.items() if own_year.get(accession, last_actual) <= last_actual]
  return max(admitted, default=datetime.max.replace(tzinfo=UTC))


def _role_series(required_role: str) -> SeriesIdentityFn:
  """The series identity selection runs on: the required role. Each window binds its
  filing's line to the role, so a later filing's comparative under another tag is the same
  series (the admitted correspondence ``select_vintage``'s injected identity exists for)."""

  def identity(row: VintageEvidenceRow) -> SeriesIdentity:
    return SeriesIdentity(series=(required_role,), period=row.period_id)

  return identity


def _lineage(
  own: VintageEvidence | None,
  later: Sequence[VintageEvidence],
  *,
  included_filings: Sequence[tuple[datetime, str]] = (),
) -> tuple[VintageEvidence, ...]:
  """The pin's restatement-edge rule: a later filing's column is a vintage only where its
  value differs from the vintage before it, and then it restates that one. A column that
  repeats the value is the same fact re-reported; where the two filings state it at different
  precisions (thousands vs millions to one decimal) it repeats it when the two agree within
  half the coarser column's last stated digit. Each candidate resolves its ancestor over the
  ones before it, in filing order, so a chain composes. With no own column the first later
  one is the root."""
  chain: list[VintageEvidence] = [] if own is None else [own]
  for candidate in sorted(later, key=lambda item: (item.available_at, item.source_id)):
    current = chain[-1] if chain else None
    if (
      current is not None
      and 2 * abs(candidate.value - current.value) <= max(candidate.resolution, current.resolution)
      and not any(
        (current.available_at, current.source_id) < filing < (candidate.available_at, candidate.source_id)
        for filing in included_filings
      )
    ):
      continue
    chain.append(replace(candidate, restates_evidence_id=None if current is None else current.evidence_id))
  return tuple(chain)


def _select_vintages(
  corpus: VerifiedCorpusProtocol,
  rows: list[PeriodRequirement],
  *,
  bindings: Mapping[str, RoleStatementBindingProtocol],
  adapter: FilingLineFetcher,
  window: FiscalWindow,
  observations: dict[PeriodKey, FilingLineObservation],
  source_refs: dict[PeriodKey, VersionedArtifactRef],
  occurrence_files: Mapping[str, ProjectedCorpusFileProtocol],
  scope: VersionedArtifactScope,
  filed_at: Mapping[str, datetime],
  cutoff_at: datetime,
  concept_balances: Mapping[tuple[str, str], str | None] | None = None,
) -> tuple[list[PeriodRequirement], dict[PeriodKey, PeriodVintages]]:
  """Select the period's latest eligible numeric vintage or cited inclusion.

  Numeric candidates keep their original observations and restatement lineage.
  A later filing's cited inclusion applies to an earlier period only when that
  filing presents the declared parent's comparative column for the period.
  It supersedes older numeric candidates without creating a child fact; a later
  bound comparative restores the numeric state, even when its amount repeats.
  Own-period inclusion is the authored judgment and needs no inferred extension.
  """
  own_year = filing_years(corpus)
  window_accessions = _window_accessions(bindings)
  wanted: dict[PeriodKey, list[FilingLineRef]] = {}
  included_columns: dict[PeriodKey, dict[str, FilingLineRef]] = {}
  cited: list[tuple[FilingLineRef, str | None]] = []
  for row in rows:
    binding = bindings.get(row.required_role)
    if (
      row.binding_source != "statement_bindings"
      or row.state not in ("bound", "unbound", "not_separately_presented")
      or binding is None
    ):
      continue
    year = int(row.period_id.removeprefix("FY"))
    lines: list[FilingLineRef] = []
    parents: dict[str, FilingLineRef] = {}
    for accession in sorted(window_accessions):
      state = binding.state_by_accession.get(accession)
      if state == "not_separately_presented":
        cited_accession = binding.absence_citation_by_accession[accession].accession
        if cited_accession != row.accession and cited_accession in filed_at:
          parent = bindings.get(binding.included_in_by_accession[accession])
          parent_identity = None if parent is None else parent.identity_by_accession.get(cited_accession)
          if isinstance(parent_identity, CompositeStatementIdentity):
            parent_identity = None
          if (
            parent is not None
            and parent.state_by_accession.get(cited_accession) == "bound"
            and parent_identity is not None
          ):
            parents[cited_accession] = _window_line(parent_identity, cited_accession, year)
            cited.append((parents[cited_accession], parent.cited_arc_by_accession.get(cited_accession)))
        if (
          accession == row.accession
          and cited_accession != accession
          and cited_accession in filed_at
          and filed_at[cited_accession] <= cutoff_at
          and accession in filed_at
        ):
          parent = bindings.get(binding.included_in_by_accession[accession])
          parent_identity = None if parent is None else parent.identity_by_accession.get(accession)
          if isinstance(parent_identity, CompositeStatementIdentity):
            parent_identity = None
          if (
            parent is not None
            and parent.state_by_accession.get(accession) == "bound"
            and parent_identity is not None
          ):
            # The shared window authored this filing's own-period judgment.
            # Its own parent column is coverage, not an inferred comparative.
            parents[accession] = _window_line(parent_identity, accession, year)
            cited.append((parents[accession], parent.cited_arc_by_accession.get(accession)))
      elif (
        accession != row.accession
        and own_year.get(accession, year) > year
        and accession in filed_at
      ):
        identity = binding.identity_by_accession.get(accession)
        if isinstance(identity, CompositeStatementIdentity) or row.terms:
          # A composite's later-filing comparatives are not read: its own filing's
          # column is the period's one vintage.
          pass
        elif state == "bound" and identity is not None:
          lines.append(_window_line(identity, accession, year))
          cited.append((lines[-1], binding.cited_arc_by_accession.get(accession)))
        elif state is None and row.pinned_ref is not None:
          lines.append(replace(row.pinned_ref, accession=accession))
    wanted[row.key] = lines
    included_columns[row.key] = parents
  requested = tuple(dict.fromkeys(
    line
    for key, lines in wanted.items()
    for line in (*lines, *included_columns[key].values())
  ))
  fetched = (
    adapter.fetch_filing_line_facts(corpus.ticker, lines=requested, window=window, cited_arcs=_cited_arcs(cited))[0]
    if requested
    else ()
  )
  by_line = {observation.line: observation for observation in fetched}

  def candidate(row: PeriodRequirement, line: FilingLineRef, observation: FilingLineObservation | None, *, own: bool) -> VintageEvidence | None:
    if observation is None or observation.status != "ok" or observation.fact is None or line.accession not in filed_at:
      return None
    fact_value = observation.fact.value
    try:
      value = _fact_requirement_value(
        observation.fact, required_role=row.required_role, unit=row.unit, scale=row.scale,
        concept_balances=concept_balances,
      )
      resolution = (
        quantity_to_requirement_value(
          fact_value.model_copy(update={"value": Decimal(1).scaleb(-fact_value.display.precision)}),
          unit=row.unit,
          scale=row.scale,
        )
        if isinstance(fact_value, Quantity)
        else Decimal(0)
      )
    except QuantityConversionGap:
      return None
    artifact = occurrence_files.get(line.accession)
    return VintageEvidence(
      evidence_id=historical_evidence_id(line, row.period_id, "edgar" if own else "vintage"),
      source_id=line.accession,
      available_at=filed_at[line.accession],
      restates_evidence_id=None,
      line=line,
      period_id=row.period_id,
      required_role=row.required_role,
      value=value,
      resolution=resolution,
      observation=observation,
      source_ref=None if artifact is None else corpus_artifact_ref(corpus, artifact, scope=scope),
      capture_id=None if artifact is None else artifact.logical_id,
      own=own,
    )

  out: list[PeriodRequirement] = []
  vintages: dict[PeriodKey, PeriodVintages] = {}
  for row in rows:
    if row.key not in wanted or (
      row.state != "not_separately_presented"
      and row.accession is not None and row.accession not in filed_at
    ):
      # The existing numeric pin keeps an undated own column; cited inclusion
      # must instead establish when its authoritative filing was available.
      out.append(row)
      continue
    binding = bindings[row.required_role]
    included = [
      accession
      for accession, line in included_columns[row.key].items()
      if (observation := by_line.get(line)) is not None
      and observation.status == "ok"
      and observation.fact is not None
    ]
    if (
      row.state == "not_separately_presented"
      and row.absence_citation is not None
      and row.absence_citation.accession == row.accession
      and row.accession is not None
      and row.accession in filed_at
    ):
      included.append(row.accession)
    included_filings = [(filed_at[accession], accession) for accession in included]
    eligible_included = [filing for filing in included_filings if filing[0] <= cutoff_at]
    selected_included = max(eligible_included, default=None)
    own_late = row.accession is not None and row.accession in filed_at and filed_at[row.accession] > cutoff_at
    own = None if row.pinned_ref is None else candidate(row, row.pinned_ref, observations.get(row.key), own=True)
    later = [
      found for line in wanted[row.key]
      if (found := candidate(row, line, by_line.get(line), own=False)) is not None
    ]
    chain = _lineage(own, later, included_filings=included_filings)
    chosen: VintageEvidence | None = None
    outcome_code = "vintage_missing_for_period"
    if chain:
      outcome = select_vintage(chain, row.period_id, cutoff_at, series_identity=_role_series(row.required_role))
      outcome_code = outcome.code
      if isinstance(outcome, SelectedVintage):
        chosen = next(item for item in chain if item.evidence_id == outcome.bound.evidence_id)
      elif not isinstance(outcome, VintageMissingForPeriod):
        raise ValueError(f"select_vintage refused the lineage minted for {row.required_role}/{row.period_id}: {outcome}")
    if selected_included is not None and (
      chosen is None or selected_included > (chosen.available_at, chosen.source_id)
    ):
      accession = selected_included[1]
      artifact = occurrence_files.get(accession)
      observations.pop(row.key, None)
      source_refs.pop(row.key, None)
      row = replace(
        row,
        accession=accession,
        state="not_separately_presented",
        pinned_ref=None,
        cited_arc=None,
        kpi_citation=None,
        absence_citation=binding.absence_citation_by_accession[accession],
        included_in=binding.included_in_by_accession[accession],
        unbound_reason=None,
        disposition="not_separately_presented",
        capture_id=None if artifact is None else artifact.logical_id,
      )
      chosen = None
      outcome_code = "not_separately_presented"
    elif chosen is None and (own_late or row.state == "not_separately_presented"):
      if row.state == "not_separately_presented":
        citation = row.absence_citation
        reason = (
          f"no cited inclusion of {row.required_role}/{row.period_id} is available "
          f"at the cutoff {cutoff_at.isoformat()}: "
          f"{None if citation is None else citation.accession} must be dated, "
          "accepted by the cutoff, and present the included aggregate for this period"
        )
      else:
        late = {item.source_id for item in chain} | ({row.accession} if row.accession is not None else set())
        reason = (
          f"no vintage of {row.period_id} is available at the cutoff {cutoff_at.isoformat()}: "
          f"{', '.join(sorted(late))} accepted after it"
        )
      observations.pop(row.key, None)
      source_refs.pop(row.key, None)
      row = replace(
        row,
        state="unbound",
        pinned_ref=None,
        cited_arc=None,
        absence_citation=None,
        included_in=None,
        disposition="unknown_applicability",
        unbound_reason=reason,
      )
    elif chosen is not None and not chosen.own:
      observations[row.key] = chosen.observation
      if chosen.source_ref is None:
        source_refs.pop(row.key, None)
      else:
        source_refs[row.key] = chosen.source_ref
      row = replace(
        _bound(
          row,
          chosen.line,
          source="statement_bindings",
          capture_id=chosen.capture_id,
          cited_arc=binding.cited_arc_by_accession.get(chosen.line.accession),
        ),
        disposition="derived" if closure_corroborated_zero(chosen.observation) else "observed",
        kpi_citation=None,
      )
    if chain and (len(chain) > 1 or chosen is None or not chosen.own or included_filings):
      vintages[row.key] = PeriodVintages(
        cutoff_at=cutoff_at,
        candidates=chain,
        outcome=outcome_code,
        selected=None if chosen is None else chosen.evidence_id,
      )
    out.append(row)
  return out, vintages

# --------------------------------------------------------------------------
# per-role projection (docs/design/corpus-and-correspondence-kernel-design-2026-10-19.md §3)
# --------------------------------------------------------------------------
#: The corpus verifier's kind for the sealed ``statement_bindings.v2`` file
#: (``research.build_model_source_bundle._STATEMENT_BINDINGS_KIND``, same value).
STATEMENT_BINDINGS_ARTIFACT_KIND = "statement_bindings_json"
#: The logical id of the corpus's derived coverage profile
#: (``model_build.corpus_facts.COVERAGE_PROFILE_LOGICAL_ID``, same value).
_COVERAGE_PROFILE_LOGICAL_ID = "issuer.coverage_profile"
#: ``AnnualCoverageProfile`` fields a role's rows read: their members are folded into that role's
#: ``RoleBindingRefs`` sha. ``semantic_target_ids`` (the sorted role list) is its own ``RoleBindingRefs``
#: field and every other profile field is the axis (``axis_digest``); ``test_role_projection_parity``
#: pins that the three partition ``AnnualCoverageProfile.model_fields``.
_ROLE_KEYED_PROFILE_FIELDS: tuple[str, ...] = (
  "annual_requirements", "required_roles", "registry_roles", "registry_group_ids", "registry_tags", "template_item_ids",
)
#: The profile list fields whose entries are not role ids and that no catalog role claims: a change to
#: one cannot be attributed to a role, so it is part of the axis.
_UNATTRIBUTED_PROFILE_LISTS: tuple[str, ...] = ("registry_group_ids", "registry_tags")
_BINDING_PROPERTIES: tuple[str, ...] = (
  "state_by_accession", "identity_by_accession", "absence_citation_by_accession",
  "included_in_by_accession", "unbound_reason_by_accession", "cited_arc_by_accession",
)


@dataclass(frozen=True)
class RoleBindingRefs:
  """What one persisted state of a research file's statement bindings says about each role, and
  about everything outside a role that its projected rows read.

  ``roles`` is decoded ``required_role`` → sha256 of the canonical JSON of [its decoded binding (every
  window's state, identity, citation, arc, closure), its KPI citations, its ``AnnualSourceRequirement``
  rows, its membership in the profile's role-keyed lists]. ``window_accessions`` is the union of role-row
  membership (``_window_accessions``); ``period_ids`` every role's periods (the ``fact_request`` window's
  input); ``semantic_target_ids`` the profile's sorted role list; ``axis_digest`` every other profile
  field; ``company`` the issuer row's digest; ``corpus_version`` the ``artifact_version`` of every source
  ref; ``filings_digest`` the caller's digest of every input the rows read outside the bindings and the
  profile (``corpus_filings_digest``). Any of the last seven that differs between two states moves every
  role's rows (``moved_role_set``: whole stage)."""

  roles: Mapping[str, str]
  window_accessions: frozenset[str]
  period_ids: tuple[str, ...]
  semantic_target_ids: tuple[str, ...]
  axis_digest: str
  company: str
  corpus_version: int
  filings_digest: str

  def seeds(self, other: "RoleBindingRefs") -> frozenset[str]:
    """The roles whose sha differs between the two states, one that is only in one of them included."""
    return frozenset(
      role for role in {*self.roles, *other.roles} if self.roles.get(role) != other.roles.get(role)
    )


def _plain(value: Any) -> Any:
  """``value`` as JSON-ready data of one canonical shape: mappings and sets sorted by their serialized
  key, dataclasses and models by field."""
  if value is None or isinstance(value, (bool, int, str)):
    return value
  if isinstance(value, (Decimal, float)):
    return repr(value)
  if isinstance(value, datetime):
    return value.isoformat()
  model_dump = getattr(value, "model_dump", None)
  if callable(model_dump):
    return model_dump(mode="json")
  if is_dataclass(value) and not isinstance(value, type):
    return {item.name: _plain(getattr(value, item.name)) for item in dataclass_fields(value)}
  if isinstance(value, Mapping):
    if all(isinstance(key, str) for key in value):
      return [[key, _plain(value[key])] for key in sorted(value)]
    return sorted(
      ([_plain(key), _plain(item)] for key, item in value.items()),
      key=lambda pair: json.dumps(pair[0], sort_keys=True),
    )
  if isinstance(value, (set, frozenset)):
    return sorted((_plain(item) for item in value), key=lambda item: json.dumps(item, sort_keys=True))
  if isinstance(value, (list, tuple)):
    return [_plain(item) for item in value]
  return str(value)


def _digest(value: Any) -> str:
  return hashlib.sha256(
    json.dumps(_plain(value), sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8")
  ).hexdigest()


def corpus_filings_digest(corpus: VerifiedCorpusProtocol, *, material: StatementSourceMaterial | None = None) -> str:
  """sha256 of everything the rows read of the corpus outside the bindings and the profile derived from
  them: every other corpus file by ``(kind, logical_id, accession, sha256, capture_metadata)`` —
  filings and their acceptance, issuer evidence, schedule and reconciliation captures, the packaged
  base — the as-filed accessions of each declared period, and the reviewed statement ``material`` when
  one is given. The issuer-evidence file is derived from the bindings' KPI citations and stamps every
  issuer-sourced ``source_ref``, so it moves this digest, not a role's sha."""
  files = sorted(
    (
      (str(item.kind), str(item.logical_id), str(item.accession), str(item.sha256), _plain(item.capture_metadata))
      for item in corpus.files
      if item.kind != STATEMENT_BINDINGS_ARTIFACT_KIND and item.logical_id != _COVERAGE_PROFILE_LOGICAL_ID
    ),
    key=lambda entry: entry[:4],
  )
  periods = {label: sorted(corpus.as_filed_accessions_for_period(label)) for label in sorted(corpus.declared_period_labels())}
  return _digest([files, periods, None if material is None else material.model_dump(mode="json")])


def _bindings_kpi_citations(corpus: VerifiedCorpusProtocol) -> dict[str, list[tuple[str, int, str]]]:
  """Role → the ``(accession, row, span)`` KPI citations its stored rows carry: the decode drops them
  (``_kpi_citations``), and ``PeriodRequirement.kpi_citation`` reads them."""
  artifacts = [item for item in corpus.files if item.kind == STATEMENT_BINDINGS_ARTIFACT_KIND]
  if not artifacts:
    return {}
  payload = json.loads(corpus.root.joinpath(*artifacts[0].relative_path.parts).read_bytes())
  out: dict[str, list[tuple[str, int, str]]] = {}
  for window in payload.get("vintage_windows", ()):
    for role, raw in (window.get("rows") or {}).items():
      for citation in (raw or {}).get("kpi_citations") or ():
        out.setdefault(role, []).append((str(citation["accession"]), int(citation["row"]), str(citation["span"])))
  return {role: sorted(citations) for role, citations in out.items()}


def role_binding_refs(
  statement_bindings: Mapping[str, RoleStatementBindingProtocol], profile: AnnualCoverageProfile, *,
  company: Mapping[str, Any] | None, corpus: VerifiedCorpusProtocol, filings_digest: str,
) -> RoleBindingRefs:
  """``RoleBindingRefs`` of one state, from the decoded bindings the corpus layer already holds (never a
  second decode), the coverage profile, the issuer row, the corpus and ``corpus_filings_digest``.

  A role's sha reads the bindings through the ``RoleStatementBindingProtocol`` object itself (every field a
  dataclass binding carries, the closure specs and disclosure plane the decode keeps beside the seven
  protocol planes), so a field the projection or the closure adjudication reads cannot move without it.
  ``registry_group_ids`` / ``registry_tags`` name no catalog role: they are folded into the axis."""
  requirements: dict[str, list[Any]] = {}
  for requirement in profile.annual_requirements:
    requirements.setdefault(requirement.required_role, []).append(requirement)
  kpi = _bindings_kpi_citations(corpus)
  lists = {
    name: frozenset(getattr(profile, name, ()))
    for name in ("required_roles", "registry_roles", "template_item_ids")
  }
  roles: dict[str, str] = {}
  claimed_items: set[str] = set()
  for role in sorted({*statement_bindings, *requirements}):
    binding = statement_bindings.get(role)
    catalog_role = _required_role_or_none(role)
    items = frozenset(() if catalog_role is None else catalog_role.template_items)
    claimed_items |= items
    roles[role] = _digest([
      None if binding is None else (
        binding if is_dataclass(binding) and not isinstance(binding, type)
        else {name: getattr(binding, name) for name in _BINDING_PROPERTIES}
      ),
      kpi.get(role, ()),
      [requirement.model_dump(mode="json") for requirement in requirements.get(role, ())],
      [role in lists["required_roles"], role in lists["registry_roles"], sorted(items & lists["template_item_ids"])],
    ])
  axis_fields = [
    name for name in type(profile).model_fields
    if name not in _ROLE_KEYED_PROFILE_FIELDS and name != "semantic_target_ids" and name in profile.__dict__
  ]
  axis = {name: getattr(profile, name) for name in axis_fields}
  axis["<unattributed>"] = {
    **{name: getattr(profile, name, ()) for name in _UNATTRIBUTED_PROFILE_LISTS},
    "required_roles": sorted(lists["required_roles"] - set(roles)),
    "registry_roles": sorted(lists["registry_roles"] - set(roles)),
    "template_item_ids": sorted(lists["template_item_ids"] - claimed_items),
  }
  return RoleBindingRefs(
    roles=MappingProxyType(roles),
    window_accessions=_window_accessions(statement_bindings),
    period_ids=tuple(sorted({period for requirement in profile.annual_requirements for period in requirement.period_ids})),
    semantic_target_ids=tuple(getattr(profile, "semantic_target_ids", ())),
    axis_digest=_digest(axis),
    company=_digest(None if company is None else dict(company)),
    corpus_version=int(corpus.corpus_version),
    filings_digest=filings_digest,
  )


_LineKey = tuple[str, str, tuple[tuple[str, str], ...]]


def _line_key(statement_role: str, tag: str, dimensions: Iterable[Any]) -> _LineKey:
  """A filed line without its accession and period: a line one filing's row cites on an arc is read by
  every role that reads it, in that filing or in a later one (``_select_vintages`` asks a later filing
  for the same tag), so the key outlives the accession."""
  return (
    normalize_topology_role(statement_role), tag,
    tuple((dimension.axis_qname, dimension.member_qname) for dimension in dimensions),
  )


def _pinned_line_keys(binding: RoleStatementBindingProtocol) -> frozenset[_LineKey]:
  """Every filed line one role's windows pin (``_line_key``)."""
  keys: set[_LineKey] = set()
  for identity in binding.identity_by_accession.values():
    for member in (
      (term.identity for term in identity.terms) if isinstance(identity, CompositeStatementIdentity) else (identity,)
    ):
      keys.add(_line_key(member.presentation_role_name, member.line_qname, member.dimensions))
  return frozenset(keys)


def _read_line_keys(rows: Iterable[PeriodRequirement]) -> dict[_LineKey, set[str]]:
  """Line (``_line_key``) → the roles whose rows read it: every row ``fact_request`` fetches a line for
  (``_occurrence_cites``), whatever put the row there — a binding window, the reviewed material or the
  pin. A role with no binding reads a shared line through such a row alone."""
  out: dict[_LineKey, set[str]] = {}
  for row, line, _arc in _occurrence_cites(rows):
    out.setdefault(_line_key(line.statement_role, line.tag, line.dimensions), set()).add(row.required_role)
  return out


def moved_role_set(
  prior: RoleBindingRefs, now: RoleBindingRefs, *,
  prior_bindings: Mapping[str, RoleStatementBindingProtocol], bindings: Mapping[str, RoleStatementBindingProtocol],
  prior_rows: Sequence[PeriodRequirement], rows: Sequence[PeriodRequirement],
  catalog: RequiredRoleCatalog,
) -> frozenset[str] | None:
  """The roles whose projected rows may differ between the two states, or ``None``: re-project the
  whole stage.

  ``prior_rows`` / ``rows`` are every role's rows of each state (``per_period_requirements``; the
  earlier state's ``PeriodFacts.rows`` serve). ``None`` when the window accessions
  (``_window_accessions``), ``period_ids`` (the ``fact_request`` window), ``semantic_target_ids``,
  ``axis_digest``, the company row, ``corpus_version`` or ``filings_digest`` differ, or when the fixed
  point below covers every role. Otherwise the least fixed point of: (a) the roles whose sha moved;
  (b) the roles reading one line in either state, by a binding window that pins it or by any row the
  projection fetches it for (``_read_line_keys``: the arc applied to a line is the union over every row
  that reads it, ``_cited_arcs``; one closure adjudication survives per line,
  ``closure_target_by_line``); (c) a catalog alias and its home, an ``included_in`` parent and its
  child in either state (``_select_vintages`` reads the parent's binding).
  Every clause is a group of mutually dependent roles, so the fixed point is the union of the groups
  a seed belongs to, computed by union-find."""
  if (
    prior.window_accessions != now.window_accessions
    or prior.period_ids != now.period_ids
    or prior.semantic_target_ids != now.semantic_target_ids
    or prior.axis_digest != now.axis_digest
    or prior.company != now.company
    or prior.corpus_version != now.corpus_version
    or prior.filings_digest != now.filings_digest
  ):
    return None
  seeds = prior.seeds(now)
  parent: dict[str, str] = {}

  def find(role: str) -> str:
    root = role
    while parent.get(root, root) != root:
      root = parent[root]
    while parent.get(role, role) != root:
      parent[role], role = root, parent[role]
    return root

  def join(roles: Iterable[str]) -> None:
    first, *rest = roles
    for role in rest:
      parent[find(role)] = find(first)

  for row in catalog.required_roles:
    if row.alias_of is not None:
      join((row.required_role, row.alias_of))
  for state, state_rows in ((prior_bindings, prior_rows), (bindings, rows)):
    by_line = _read_line_keys(state_rows)
    for role, binding in state.items():
      for key in _pinned_line_keys(binding):
        by_line.setdefault(key, set()).add(role)
      for included in binding.included_in_by_accession.values():
        join((role, included))
    for reading in by_line.values():
      join(reading)
  seed_roots = {find(seed) for seed in seeds}
  moved = seeds | frozenset(role for role in [*prior.roles, *now.roles, *parent] if find(role) in seed_roots)
  if {*prior.roles, *now.roles} <= moved:
    return None
  return moved


def project_facts_for_roles(
  corpus: VerifiedCorpusProtocol, prior: PeriodFacts, rows: Sequence[PeriodRequirement], roles: frozenset[str], *,
  sources: PinSources, fetched: FetchedFacts, adapter: FilingLineFetcher, window: FiscalWindow,
  scope: VersionedArtifactScope | None = None, cutoff_at: datetime | None = None, last_actual: int | None = None,
) -> PeriodFacts:
  """``project_facts_per_period`` over every row, as the cold call returns it: ``roles``' rows,
  observations, source refs, vintages and closure adjudications from a call on the role-sliced ``rows``
  / ``fetched`` (with ``opening_by_role`` sliced the same way), every other role's from ``prior``;
  ``snapshot_ref`` / ``bindings_ref`` / ``filed_at`` / ``cutoff_at`` from ``corpus``; sorted
  ``(required_role, period_id)``.

  ``rows`` are every role's input rows (``per_period_requirements``), ``sources`` the full decoded
  bindings (the window accessions and the ``included_in`` parents are read from them), ``window`` the
  whole stage's. ``prior`` is the cold projection of the earlier state at the same ``scope``,
  ``cutoff_at`` and ``last_actual``; ``roles`` is closed under ``moved_role_set``. The observation,
  source-ref, vintage and closure mappings are sorted by key: the cold call's insertion order is not a
  contract (a vintage selected from a later filing inserts its observation last), and no reader of
  ``PeriodFacts`` takes it."""
  sliced = replace(
    fetched,
    opening_by_role={role: found for role, found in fetched.opening_by_role.items() if role in roles},
    closure_adjudications={key: found for key, found in fetched.closure_adjudications.items() if key[0] in roles},
  )
  part = project_facts_per_period(
    corpus, [row for row in rows if row.required_role in roles], sources=sources, fetched=sliced,
    adapter=adapter, window=window, scope=scope, cutoff_at=cutoff_at, last_actual=last_actual,
  )
  kept = [row for row in prior.rows if row.required_role not in roles]
  merged = sorted([*kept, *part.rows], key=lambda row: (row.required_role, row.period_id))

  def merge(held: Mapping[PeriodKey, Any], fresh: Mapping[PeriodKey, Any]) -> dict[PeriodKey, Any]:
    """The roles' entries from ``fresh``, every other role's from ``held``, sorted by key."""
    return dict(sorted({**{key: found for key, found in held.items() if key[0] not in roles}, **fresh}.items()))

  return PeriodFacts(
    rows=tuple(merged),
    observations=merge(prior.observations, part.observations),
    source_refs=merge(prior.source_refs, part.source_refs),
    snapshot_ref=part.snapshot_ref,
    bindings_ref=part.bindings_ref,
    closure_adjudications=merge(prior.closure_adjudications, part.closure_adjudications),
    vintages=merge(prior.vintages, part.vintages),
    cutoff_at=part.cutoff_at,
    filed_at=part.filed_at,
    concept_balances=part.concept_balances,
  )



# --------------------------------------------------------------------------
# value-algebra writers
# --------------------------------------------------------------------------
#: The record's ruled value when a filing citation evidences no such
#: debt balance or a bound debt blank is corroborated by filing totals.
EVIDENCED_ABSENCE_DERIVATION = "evidenced_absence"
_EVIDENCED_ABSENCE_DEBT_ROLES = frozenset({
  "statement.bs.short_term_debt",
  "statement.bs.long_term_debt",
})



def filing_line_observation_id(line: FilingLineRef) -> str:
  """The canonical dimension-complete identity carried by a sourced observation."""
  return mint_source_fact_ref(line, "statement").observation_id


def typed_values_from_projection(
  rows: Iterable[PeriodRequirement],
  facts: PeriodFacts,
) -> dict[PeriodKey, ModelNumberValue]:
  """Each (role, period) source cell as a ``ModelNumberValue``.

  - bound, ok fact → ``ObservedKnownNumberValue``: the corpus version is the
    observation snapshot, the sealed artifact the fact was read from its evidence,
    the ``filing-line:`` id its observation id;
  - bound, closure-corroborated zero → ``DerivedKnownNumberValue`` whose formula
    node is ``closure_corroborated_zero`` (the binding's fields are mapped onto
    the closure's sealed inputs; see the type-gap note in ``_closure_binding``);
  - bound, no ok fact in the capture → ``Missing(source_value_unavailable)``;
  - cited ``not_applicable`` debt → ``DerivedKnownNumberValue(0)`` with
    ``evidenced_absence`` and the cited binding; other absences are
    ``EvidencedAbsent`` values (``_missing`` with an ``AbsenceDisposition`` reason);
  - ``not_separately_presented`` → ``EvidencedAbsent(not_separately_presented)``,
    with its citation and included aggregate on the period requirement, never a number;
  - ``unbound`` → ``Missing(source_observation_absent)``, ``EvidencedAbsent(not_disclosed_in_vintage)`` when the
    agent declared that reason on the stored unbound row;
  - a quantity the requirement's unit/scale cannot hold → ``Invalid(unit_mismatch)``.

  A reconciliation pin this projection did not read gets no entry here either
  (its fact is a ``HistoricalBasisFact`` of the bundle's reconciliation path).
  """
  out: dict[PeriodKey, ModelNumberValue] = {}
  for row in rows:
    period = fiscal_period(row.period_id)
    if row.state == "not_applicable":
      absence = evidenced_debt_absence_value(row, facts)
      out[row.key] = absence if absence is not None else _missing(row, period, "not_applicable_evidenced")
      continue
    if row.state == "not_separately_presented":
      out[row.key] = EvidencedAbsentNumberValue(
        state="evidenced_absent",
        primary_reason="not_separately_presented",
        causes=(
          AbsentCause(
            state="evidenced_absent",
            node_id=row.required_role,
            period=period,
            expression_path=_SOURCE_EXPRESSION_PATH,
            reason_code="not_separately_presented",
          ),
        ),
        citation=row.absence_citation,
        included_in=row.included_in,
      )
      continue
    if row.state == "unbound":
      out[row.key] = _missing(
        row, period,
        "not_disclosed_in_vintage" if row.unbound_reason == NOT_DISCLOSED_IN_VINTAGE else "source_observation_absent",
      )
      continue
    observation = facts.observations.get(row.key)
    if observation is None and row.binding_source == "reconciliation":
      continue
    if observation is None or observation.status != "ok" or observation.fact is None:
      out[row.key] = _missing(row, period, "source_value_unavailable")
      continue
    components = composite_components(observation)
    try:
      value = float(_fact_requirement_value(
        observation.fact, required_role=row.required_role, unit=row.unit, scale=row.scale,
        concept_balances=facts.concept_balances,
      ))
    except QuantityConversionGap:
      out[row.key] = InvalidNumberValue(
        state="invalid",
        primary_reason="unit_mismatch",
        causes=(
          InvalidCause(
            state="invalid",
            node_id=row.required_role,
            period=period,
            expression_path=_SOURCE_EXPRESSION_PATH,
            reason_code="unit_mismatch",
          ),
        ),
      )
      continue
    if components is not None:
      out[row.key] = DerivedKnownNumberValue(
        state="known",
        value=value,
        origin="derived",
        origin_bindings=(_composite_binding(row, period, observation, facts),),
      )
      continue
    if closure_corroborated_zero(observation):
      out[row.key] = DerivedKnownNumberValue(
        state="known",
        value=value,
        origin="derived",
        origin_bindings=(_closure_binding(row, period, observation, facts),),
      )
      continue
    source_ref = facts.source_refs.get(row.key, facts.snapshot_ref)
    role = _required_role_or_none(row.required_role)
    is_income_statement = role is not None and role.statement == "income_statement"
    provenance = observation.fact.provenance
    native_value = (
      quantity_native_value(
        observation.fact.value,
        preferred_label_negates_value=(
          provenance is not None and provenance.preferred_label_negates_value is True
        ),
      )
      if is_income_statement and isinstance(observation.fact.value, Quantity)
      else None
    )
    applied_polarity = (
      requirement_value_polarity(
        role_polarity=role.polarity,
        concept_balances=facts.concept_balances,
        concept_key=(observation.line.accession, observation.line.tag),
      )
      if is_income_statement and role is not None and isinstance(observation.fact.value, Quantity)
      else None
    )
    out[row.key] = ObservedKnownNumberValue(
      state="known",
      value=value,
      origin="observed",
      origin_bindings=(
        ObservationOriginBinding(
          binding_kind="observed",
          observation_snapshot_ref=facts.snapshot_ref,
          observation_snapshot_hash=facts.snapshot_ref.content_sha256,
          observation_id=(
            observation.fact.source_ref.observation_id
            if observation.fact.source_ref is not None
            else filing_line_observation_id(observation.line)
          ),
          evidence_refs=(source_ref,),
          evidence_hashes=(source_ref.content_sha256,),
          semantic_identity=row.required_role,
          period=period,
          native_value=native_value,
          applied_polarity=applied_polarity,
        ),
      ),
    )
  return out


def fiscal_period(period_id: str) -> FiscalPeriod:
  return FiscalPeriod(
    period_id=period_id,
    fiscal_year=int(period_id.removeprefix("FY")),
    period_type="annual",
    is_estimate=False,
  )


def _missing(
  row: PeriodRequirement, period: FiscalPeriod, reason: CellUnavailableReason,
) -> MissingNumberValue | EvidencedAbsentNumberValue:
  """A source cell with neither a number nor (for an owed reason) a way to stand.

  A reason that is an ``AbsenceDisposition`` is an evidenced absence, never a missing
  source: the cell is not owed. Every other reason stays ``Missing``."""
  if is_absence_disposition(reason):
    return EvidencedAbsentNumberValue(
      state="evidenced_absent",
      primary_reason=reason,
      causes=(
        AbsentCause(
          state="evidenced_absent",
          node_id=row.required_role,
          period=period,
          expression_path=_SOURCE_EXPRESSION_PATH,
          reason_code=reason,
        ),
      ),
      citation=row.absence_citation,
      included_in=row.included_in,
    )
  return MissingNumberValue(
    state="missing",
    primary_reason=reason,
    causes=(
      MissingCause(
        state="missing",
        node_id=row.required_role,
        period=period,
        expression_path=_SOURCE_EXPRESSION_PATH,
        reason_code=reason,
      ),
    ),
  )


def _sha256_text(text: str) -> str:
  return hashlib.sha256(text.encode("utf-8")).hexdigest()


def evidenced_debt_absence_value(
  row: PeriodRequirement, facts: PeriodFacts, *, closure: Mapping[str, Any] | None = None,
) -> DerivedKnownNumberValue | None:
  """The ruled debt record zero, only when absence has authoritative evidence.

  A cited not-applicable debt line or a bound debt blank corroborated by the
  filing's totals supplies that evidence. Other roles and unproven blanks
  retain their ordinary nonnumeric source state.
  """
  if (
    row.state == "not_applicable"
    and row.required_role in _EVIDENCED_ABSENCE_DEBT_ROLES
    and row.absence_citation is not None
  ):
    evidence = row.absence_citation.model_dump(mode="json")
  elif (
    row.state == "bound"
    and row.required_role in _EVIDENCED_ABSENCE_DEBT_ROLES
    and closure is not None
    and closure.get("verdict") == "corroborated"
  ):
    evidence = dict(closure)
  else:
    return None
  return DerivedKnownNumberValue(
    state="known", value=0.0, origin="derived",
    origin_bindings=(_evidenced_absence_binding(row, fiscal_period(row.period_id), facts, evidence),),
  )


def _evidenced_absence_binding(
  row: PeriodRequirement, period: FiscalPeriod, facts: PeriodFacts, evidence: Mapping[str, Any],
) -> FormulaOriginBinding:
  """Bind the ruled debt zero to the record proving its absence.

  Like ``_closure_binding``, this source derivation uses the value algebra's
  formula binding carrier. The manifest owns the cited judgment; the inputs
  digest identifies its exact citation or corroborating blank closure.
  """
  manifest_ref = facts.bindings_ref or facts.snapshot_ref
  return FormulaOriginBinding(
    binding_kind="derived",
    formula_node_id=EVIDENCED_ABSENCE_DERIVATION,
    semantic_graph_ref=manifest_ref,
    semantic_graph_hash=manifest_ref.content_sha256,
    semantic_model_hash=facts.snapshot_ref.content_sha256,
    dependency_policy_manifest_ref=manifest_ref,
    dependency_policy_manifest_hash=manifest_ref.content_sha256,
    computation_request_digest=_sha256_text(f"{row.required_role}:{row.period_id}:{EVIDENCED_ABSENCE_DERIVATION}"),
    resolved_inputs_digest=_sha256_text(json.dumps(
      evidence, sort_keys=True,
    )),
    semantic_identity=row.required_role,
    period=period,
  )


def _composite_binding(
  row: PeriodRequirement,
  period: FiscalPeriod,
  observation: FilingLineObservation,
  facts: PeriodFacts,
) -> FormulaOriginBinding:
  """A multi-line bound row's signed sum, in the one binding ``derived`` admits.

  Like ``_closure_binding``, a source-level derivation has no binding kind of its own and
  rides ``FormulaOriginBinding``, whose only fields are refs and digests (no operand
  list: ``schema/model_number_value.py`` ``FormulaOriginBinding``). ``semantic_graph_ref`` =
  the occurrence artifact the lines were read from; ``dependency_policy_manifest_ref`` = the
  sealed ``statement_bindings.v2`` that authored the composite; ``resolved_inputs_digest`` =
  sha256 of the fact's ``native_field``, whose JSON is the structured component list
  (:func:`composite_components`: accession, line, arc, sign and value of every filed line).
  """
  graph_ref = facts.source_refs.get(row.key, facts.snapshot_ref)
  manifest_ref = facts.bindings_ref or facts.snapshot_ref
  fact = observation.fact
  native_field = "" if fact is None or fact.provenance is None else fact.provenance.native_field
  return FormulaOriginBinding(
    binding_kind="derived",
    formula_node_id=_SIGNED_SUM,
    semantic_graph_ref=graph_ref,
    semantic_graph_hash=graph_ref.content_sha256,
    semantic_model_hash=manifest_ref.content_sha256,
    dependency_policy_manifest_ref=manifest_ref,
    dependency_policy_manifest_hash=manifest_ref.content_sha256,
    computation_request_digest=_sha256_text(f"{row.required_role}:{row.period_id}:{_SIGNED_SUM}"),
    resolved_inputs_digest=_sha256_text(native_field),
    semantic_identity=row.required_role,
    period=period,
  )


def _closure_binding(
  row: PeriodRequirement,
  period: FiscalPeriod,
  observation: FilingLineObservation,
  facts: PeriodFacts,
) -> FormulaOriginBinding:
  """The closure-corroborated zero's derivation, in the one binding ``derived`` admits.

  Type gap: ``DerivedKnownNumberValue`` admits only ``FormulaOriginBinding``,
  whose fields name a compiled formula (semantic graph, dependency-policy
  manifest, computation and resolved-input digests). A source-level derivation
  — absence in the sealed presentation tree plus the authored closure — has no
  binding kind. It is mapped: ``semantic_graph_ref`` = the occurrence artifact
  the closure searched; ``dependency_policy_manifest_ref`` = the sealed
  ``statement_bindings.v2`` (where the closure was authored);
  ``semantic_model_hash`` = that artifact's hash; ``computation_request_digest``
  = sha256 of the fact's provenance ``native_field`` (accession, citations,
  derivation, search basis); ``resolved_inputs_digest`` = sha256 of the
  observation's ``detail``.
  """
  graph_ref = facts.source_refs.get(row.key, facts.snapshot_ref)
  manifest_ref = facts.bindings_ref or facts.snapshot_ref
  fact = observation.fact
  native_field = "" if fact is None or fact.provenance is None else fact.provenance.native_field
  return FormulaOriginBinding(
    binding_kind="derived",
    formula_node_id=_CLOSURE_ZERO,
    semantic_graph_ref=graph_ref,
    semantic_graph_hash=graph_ref.content_sha256,
    semantic_model_hash=manifest_ref.content_sha256,
    dependency_policy_manifest_ref=manifest_ref,
    dependency_policy_manifest_hash=manifest_ref.content_sha256,
    computation_request_digest=_sha256_text(native_field),
    resolved_inputs_digest=_sha256_text(observation.detail or ""),
    semantic_identity=row.required_role,
    period=period,
  )
