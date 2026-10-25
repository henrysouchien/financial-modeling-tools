"""Target build: the model as one authored document, built from C (per-period facts) and the
judgment artifacts, constructed by compile's own pieces, checked, snapshotted and published
by the bench (no ``compile_model_intermediate``, no pin/compile/publish service).

Integrated architecture §4–§5: the document is the authority for structure and formulas; a
rebuild is a recalc of the document with refreshed inputs, never a regeneration from template.

``target_build(source, refs=, workdir=)`` — ``source`` is the adapter (``BuildSource``) that
resolves C, the judgment artifacts and the reference version through the product and hands this
root typed ``StageBuildInputs``; the bench's is ``scripts/research/bench/context.py`` ``StoreBuildSource``:

1. **Structure.** ``schema.build.load_template()`` once, the selected BusinessModel lowered onto
   it by ``schema.business_model_compiler.compile_business_model`` + ``bind_driver_assumption_plan``
   (the typed lowering of the economic judgment into rows and formulas) and the time axis set by
   ``remap_time_structure`` to the profile's axis. That is one recorded structural edit in
   ``doc.edits`` whose source is the BusinessModel revision ref; after it every cell is written
   through the ``document`` writers with its semantic column.
2. **Historical cells (C → D by key).** Each ``(required role, period)`` of
   ``projection.per_period_requirements`` lands on the role's lines (the template's
   ``required_role`` stamp, the role catalog's ``population_item_ids``, and the BusinessModel
   node whose ``kpi_source`` claims the series): observed → input with
   ``{origin: observed, source: filing_line}``; closure-corroborated zero →
   ``{origin: derived, source: derivation}``; cited ``not_applicable`` debt → input
   zero with ``evidenced_absence`` derivation and citation from the projection's
   value writer; other ``not_applicable`` → unavailable ``not_applicable_evidenced``;
   a bound line printed blank in its own year whose absence the
   filing's totals corroborate (``presented_blank_closures``) → input zero with
   ``{origin: derived, source: derivation closure_corroborated_zero}``;
   uncorroborated → ``source_value_unavailable``; unbound →
   unavailable ``source_observation_absent`` (``not_disclosed_in_vintage`` when the agent declared it on a
   stored ``unbound`` row, never inferred from a window's omission); ``not_separately_presented`` → compile's shared
   ``=NA()`` marker with its citation and ``included_in`` (``document.mark_not_separately_presented``),
   then one ``apply_not_separately_presented_periods`` pass per write owns the period's operand
   sets. A mixed role is simply per-period cells. Each
   statement-binding period is read from its selected restatement vintage as of the build's
   ``axis.cutoff_at`` (``projection.project_facts_per_period``); a period with more than its own
   filing's column carries every candidate in ``source_history`` (selected first, each with
   ``vintage_accession``, ``available_at`` and whether the cutoff admits it).
3. **Judgment cells.** HFF historical driver seeds land on the BusinessModel node rows;
   ValuationPolicy controls (``project_valuation_policy``) on the rows
   ``schema.build_valuation_inputs`` maps them to; ScenarioSet case probabilities on the
   expected-value rows.
4. **Construction** (``construct_document``): the product's ModelSpec
   (``assemble_model_spec`` over the persisted SourceBundle and the artifacts) and compile's
   population, schedule, valuation and scenario pieces, each a recorded edit through
   ``construct`` (the FAS lowering first); authored cells stay authored. Every input nothing
   wrote is then unavailable (``required_dependency_missing`` / ``source_observation_absent``).
5. **Recalc** with DependencyGraph (``document.recalc``) — a cited evidenced absence stays
   evidenced absent and propagates, except a catalog-declared optional sum term, which is omitted.
   ``completeness.json`` compares the document with the latest committed product ModelVersion.
6. **Construction package** ``<workdir>/snapshot/``: the document package, ``model_spec.json``
   (when assembled), ``template.json``, ``scenario_controls.json``, ``input_baseline.json``, ``sources.jsonl``,
   ``values.jsonl``, ``completeness.json``, ``validation.json`` (``target_check``),
   ``findings.json`` and ``manifest.json``. Rendering and native LibreOffice verification belong
   to publication; the bench's ``target_snapshot`` adds the rendered and comparison artifacts.

``target_lookup(workdir, item_id, period)`` joins one cell across the snapshot;
``target_check(doc, validation=)`` is the check of a document: the one validation report
(``document_validation``) that publication persists, with its findings and blocking verdict;
``target_snapshot(package, out=)`` snapshots an edited document package (``doc_edit``'s output);
``target_publish(workdir, store)`` registers a ``published`` snapshot as a bench model version;
``target_refresh(workdir, source, out=)`` reopens a snapshot's document, writes only the C cells
the source's current bindings and captures change, re-runs the derivation helpers whose inputs
changed or that the parent lacked, recalcs and snapshots it; ``target_advance(workdir, source,
out=, new_actual_year=)`` does the same after moving the document's actual/forecast boundary one
year (``document.advance_period``) and the vintage cutoff to the latest captured filing, the FAS
re-lowered and every derivation helper re-run; in the year that becomes actual the actual wins and
the forecast it supersedes is kept as the cell's ``forecast_record`` (``forecast_vs_actual``); a
cell whose selected vintage changes keeps the old one in ``source_history`` and is listed in
``restated_cells`` / ``revintaged_cells``;
``target_explain(a, b, item_id, period)`` reads one cell across two snapshots with the changed
inputs that feed that period.
"""

from __future__ import annotations

from collections import Counter
from collections.abc import Callable, Collection, Iterator, Mapping, Sequence
import copy
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
import fcntl
import hashlib
import json
import math
import os
from pathlib import Path
import re
import shutil
import subprocess
from types import SimpleNamespace
from typing import TYPE_CHECKING, Any, Literal, Protocol, cast, get_args

from model_build import document, keyed_dump, live_sheet, projection
from model_build.refusals import refusal_row
from schema.build_value_writers import CellSourceStamp, stamp_cell_source
from schema.historical_driver_seeds import HistoricalDriverSeedDisposition
from schema.dependency_graph import DependencyGraphKnownNumberValue
from schema.finding_vocabulary import blocking_findings
from schema.model_queries import ANALYST_NOTE_ARTIFACT
from schema.model_number_value import MissingReasonCode
from schema.models import FinancialModel, FormulaSpec, FormulaType, ItemType, LineItem, ValueCell, ValueSeries
from schema.renderer_columns import item_owns_cell
from schema.statement_bindings_v2 import NOT_DISCLOSED_IN_VINTAGE, StatementBindings

if TYPE_CHECKING:
  from model_build.live_sheet import LiveAxis
  from schema.build_model_product_contracts import ValidationReport
  from schema.coverage_profile import AbsenceCitation
  from schema.finding_vocabulary import Finding
  from schema.model_number_value import CellUnavailableReason


#: The checkout this root sits in (``_bench_sha`` reads its HEAD).
REPO_ROOT = Path(__file__).resolve().parents[1]

#: Every typed reason a valueless cell can carry; ``project_provenance_map`` projects an uncited stamp
#: with one of these as its ``lineage_status``.
_MISSING_REASON_CODES: frozenset[str] = frozenset(get_args(MissingReasonCode))

SNAPSHOT_DIR = "snapshot"
DOCUMENT_NAME = "document.json"
WORKBOOK_STEM = "model"
#: The ScenarioSet case probability rows (``schema/build_scenarios.py``
#: ``_lower_scenario_case_probabilities``).
_CASE_PROBABILITY_ITEMS = {
  "bull": "tpl.s.expected_value.bull_probability",
  "base": "tpl.s.expected_value.base_probability",
  "bear": "tpl.s.expected_value.bear_probability",
}
#: What the snapshot records as not computed on this document (computed by nothing here).
_ANALYSES = {"scenarios": "stale", "sensitivity": "stale"}


# --------------------------------------------------------------------------
# refusals
# --------------------------------------------------------------------------


@dataclass
class _Refusals:
  """Library failures captured for the verdict; scenario admission stays typed."""

  rows: list[dict[str, Any]] = field(default_factory=list)

  def run(self, stage: str, call: Callable[[], Any]) -> Any:
    try:
      return call()
    except Exception as exc:
      self.rows.append(refusal_row(exc, stage=stage))
      return None


# --------------------------------------------------------------------------
# inputs
# --------------------------------------------------------------------------


@dataclass
class StageBuildInputs:
  """What a build reads. Every stage is optional: ``StageBuildInputs()`` is a research file with nothing
  persisted (a live sheet's first state); the construction helpers that read a stage skip it when
  it is ``None`` (``profile``/``revision`` ``None`` plan no historical targets, ``rows`` is ``()``)."""

  corpus: Any = None
  profile: Any = None
  rows: tuple[Any, ...] = ()
  facts: Any = None
  values: dict[tuple[str, str], Any] = field(default_factory=dict)
  revision: Any = None
  selection_epoch: int | None = None
  fas: Any = None
  hff: Any = None
  scenario_set: Any = None
  valuation_policy: Any = None
  handoff: dict[str, Any] | None = None
  #: The research file's persisted SourceBundle the selected BusinessModel and this corpus
  #: were prepared from (``None`` when none is stored): the ModelSpec assembler's argument and
  #: the compile-only inputs a construction helper reads (valuation observations, historical
  #: basis facts, opening balances). Never a historical statement cell's source.
  bundle: Any = None
  bundle_id: str | None = None
  #: ``content_sha256`` of the stored row ``bundle`` was decoded from (``None`` exactly when
  #: ``bundle`` is): what a ModelSpec and every digest naming the bundle bind, never a re-dump.
  bundle_content_sha256: str | None = None
  #: The ModelSpec over the judgment artifacts (the adapter's assembler).
  model_spec: Any = None
  #: The FAS projection's forecast-assumption rows (``project_forecast_and_historical_authoring``),
  #: resolved by the adapter when no ModelSpec was assembled: what ``write_forecast_cells`` mints
  #: and lowers in its place.
  fas_forecast_rows: Any = None
  #: Reconciliation-captured facts by ``FilingLineRef``: ``(fact, capture logical id, sha256)``.
  reconciliation: dict[Any, tuple[Any, str, str]] = field(default_factory=dict)
  #: What assembling the inputs met: ``{stage, code, owner_file, message}`` per failed step.
  findings: list[dict[str, Any]] = field(default_factory=list)
  #: The issuer the research file names: ``{ticker, name, cik}`` (``research_files``).
  company: dict[str, Any] | None = None
  #: Bound rows whose line the filing prints blank in its own year, by row key → whether the
  #: filing's totals corroborate the absence (``presented_blank_closures``).
  blank_closures: dict[Any, dict[str, Any]] = field(default_factory=dict)
  #: The cutoff C's vintage selection ran at (``projection.project_facts_per_period``), every
  #: vintage candidate of a period that has more than its own filing's column
  #: (``PeriodFacts.vintages``), and each captured filing's acceptance by accession.
  cutoff_at: datetime | None = None
  vintages: dict[Any, Any] = field(default_factory=dict)
  filed_at: dict[str, datetime] = field(default_factory=dict)
  #: The seeds the selected BusinessModel's own cited observations (and the ratios they imply)
  #: state, whether or not an HFF is stored: ``write_seed_cells`` places them under the HFF's own
  #: seeds. Never stored in an HFF (its seeds are the requirement denominator's).
  bm_observation_seeds: tuple[Any, ...] = ()
  #: The corpus's validated occurrence envelopes by accession, C's own read of them
  #: (``ResearchFileBuildSource.c_inputs_for``): what ``unclaimed_filed_lines`` reads in place of
  #: validating the artifacts' bytes again. ``None``: the source holds none, so the artifacts are read.
  occurrence_envelopes: Mapping[str, Any] | None = None
  #: The corpus's decoded statement bindings (``schema.statement_bindings_v2.StatementBindings``),
  #: decoded once by the C read that holds them (``ResearchFileBuildSource.c_inputs_for``, from its
  #: basis's one decode): what ``unclaimed_filed_lines`` reads in place of decoding the bindings
  #: artifact again. ``None``: no decode is held — the corpus holds no bindings artifact (so there
  #: are no unclaimed filed lines to compute), or the C read failed at its basis before decoding
  #: (an ``inputs`` finding holds that failure; a corpus C could not read has no unclaimed verdict).
  statement_bindings: StatementBindings | None = None
  #: The roles ``c_inputs_for`` projected again over its ``prior`` (``moved_role_set``): what a correspondence
  #: refresh restores and rewrites. ``None``: the whole stage was projected (or no prior was given).
  projected_roles: frozenset[str] | None = None


class StoreRef(Protocol):
  """A research file's store as a snapshot names it and a bench version is published under
  (``<root>/bench-versions/<ticker>/``). The bench's ``Store`` satisfies it."""

  @property
  def root(self) -> Path: ...

  @property
  def user_id(self) -> str: ...

  @property
  def research_file_id(self) -> int: ...

  @property
  def ticker(self) -> str: ...


@dataclass(frozen=True)
class CommittedModelReference:
  """A committed product ModelVersion of the research file: its rendered known values and its
  model (for what produced a cell there). ``error`` when it could not be read."""

  label: dict[str, Any]
  values: dict[tuple[str, int], float]
  model: Any = None
  error: str | None = None


class BuildSource(StoreRef, Protocol):
  """Where a build's typed inputs come from (plan §2.4): the adapter resolves C, the judgment
  artifacts and the reference version through the product and hands this root typed values.
  The bench's is ``scripts/research/bench/context.py`` ``StoreBuildSource``."""

  def load_c_inputs(
    self, refusals: _Refusals, *, cutoff_at: datetime | None = None, last_actual: int | None = None,
  ) -> StageBuildInputs | None:
    """C alone: per-period rows (each statement-binding period at its latest restated vintage as
    of ``cutoff_at``), their typed values, the reconciliation-captured facts, the presented-blank
    closures, the issuer and the selected BusinessModel revision; the judgment fields ``None``.
    ``None``, with its row in ``refusals``, when the research file has no selected revision."""
    ...

  def load_judgment_inputs(self, inputs: StageBuildInputs, refs: Mapping[str, Any] | None) -> StageBuildInputs:
    """The judgment artifacts (``refs`` given, else the newest accepted bound to the resolved
    upstream), the SourceBundle and the ModelSpec onto ``inputs``; a step that does not resolve
    is an input finding."""
    ...

  def reference_version(self, inputs: StageBuildInputs) -> CommittedModelReference:
    """The research file's latest committed ModelVersion, read-only."""
    ...


def store_label(store: StoreRef) -> dict[str, Any]:
  """The manifest's ``store`` entry."""
  return {"root": str(store.root), "user_id": store.user_id, "research_file_id": store.research_file_id}


def load_inputs(
  source: BuildSource, refs: Mapping[str, Any] | None, refusals: _Refusals, *, cutoff_at: datetime | None = None,
  last_actual: int | None = None,
) -> StageBuildInputs | None:
  """C's per-period rows and typed values, and the judgment artifacts, from ``source``."""
  inputs = source.load_c_inputs(refusals, cutoff_at=cutoff_at, last_actual=last_actual)
  if inputs is None:
    return None
  source.load_judgment_inputs(inputs, refs)
  return inputs


def inputs_unresolved(inputs: StageBuildInputs) -> list[dict[str, Any]]:
  """The input findings that leave construction without its SourceBundle or ModelSpec: the
  verdict's ``incomplete:inputs_unresolved``."""
  if inputs.bundle is not None and inputs.model_spec is not None:
    return []
  return [row for row in inputs.findings if row["stage"] in ("source_bundle", "model_spec_assembly")]
  return inputs


_STATEMENT_BINDINGS_KIND = "statement_bindings_json"


def _unclaimed_filed_lines_from_corpus(
  statement_bindings: Any, occurrence_envelopes: Mapping[str, Any], modeled_fiscal_years: Collection[int],
) -> list[dict[str, Any]]:
  """Unclaimed filed lines computed from the build's own corpus: the decoded
  bindings and occurrence cells the caller's C read already holds, through
  ``schema.filed_line_unclaimed``, the same pure computation the HFS spread
  evaluator (``schema.historical_spread.evaluate_historical_spread``) calls
  for its unclaimed gaps. The bindings artifact is decoded once, by that C
  read (``StageBuildInputs.statement_bindings``); the occurrence envelopes are the
  corpus's own validated envelopes (``StageBuildInputs.occurrence_envelopes``), read in
  filing order, oldest first."""
  from decimal import localcontext

  from schema.filed_line_unclaimed import filed_lines_unclaimed, unclaimed_line_record

  with localcontext(projection.SOURCE_BUNDLE_DECIMAL_CONTEXT):
    envelopes = sorted(
      occurrence_envelopes.values(), key=lambda row: (row.filing.report_date, row.filing.accession),
    )
    cells = {envelope.filing.accession: envelope.occurrences for envelope in envelopes}
    return [
      unclaimed_line_record(line)
      for line in filed_lines_unclaimed(cells, statement_bindings, modeled_fiscal_years=modeled_fiscal_years)
    ]


def _hff_spread_corpus_manifests(hff: Any) -> set[str]:
  """The corpus manifest digests the HFF's source pins record: the corpus its spread was
  computed on (each EDGAR pin's ``canonical_payload`` names the corpus artifact it read)."""
  return {
    str(payload["manifest_sha256"])
    for pin in getattr(hff, "source_manifest", ())
    if isinstance(payload := getattr(pin, "canonical_payload", None), dict) and payload.get("manifest_sha256")
  }


def unclaimed_filed_lines(
  hff: Any, modeled_fiscal_years: Collection[int], corpus: Any = None, *,
  occurrence_envelopes: Mapping[str, Any] | None = None, statement_bindings: Any = None,
) -> list[dict[str, Any]]:
  """Unclaimed filed lines for the document's historical periods.

  HFF acceptance remains non-blocking; the build verdict consumes these
  records as blocking binding findings. The build materializes no fact
  snapshot of its own (``StageBuildInputs.hff`` is the accepted HFF the FAS binds), so
  the HFF's ``filed_line_unclaimed:`` gaps are this build's spread only when
  the HFF carries a computed ``filed_tie_out`` AND its pins record the
  build's corpus manifest. An HFF with no tie-out predates the spread ("spread
  not computed", never "no unclaimed lines"); an HFF whose pins record another
  corpus manifest computed its spread on bindings the build no longer reads
  (correspondence persisted after the HFF). Both compute the lines from the
  build's own corpus (``_unclaimed_filed_lines_from_corpus``), the same pure
  computation the HFS spread uses, over the decoded statement bindings and
  occurrence envelopes the caller's C read holds on ``StageBuildInputs``
  (``statement_bindings`` / ``occurrence_envelopes``); a corpus whose C read
  holds no decoded bindings (no bindings artifact) has none. Without a corpus
  (unit callers) the HFF's gaps are read as given.
  """
  from schema.historical_foundation import FILED_LINE_UNCLAIMED_GAP_PREFIX

  tie_out = getattr(hff, "filed_tie_out", None)
  spread_computed = bool(getattr(tie_out, "by_period", None))
  if corpus is not None and (
    not spread_computed or _hff_spread_corpus_manifests(hff) != {corpus.manifest_sha256}
  ):
    if statement_bindings is None or occurrence_envelopes is None:
      return []
    return _unclaimed_filed_lines_from_corpus(
      statement_bindings, occurrence_envelopes, modeled_fiscal_years,
    )
  years = set(modeled_fiscal_years)
  records = []
  for gap in getattr(hff, "gaps", ()):
    if not gap.key.startswith(f"{FILED_LINE_UNCLAIMED_GAP_PREFIX}:"):
      continue
    record = dict(gap.details or {})
    periods = [period for period in record.get("periods", ()) if period["fiscal_year"] in years]
    if periods:
      record["periods"] = periods
      records.append(record)
  return records


def _captured(inputs: StageBuildInputs, stage: str, call: Callable[[], Any]) -> Any:
  """Record upstream absence as an input finding; scenario admission reaches the door."""
  try:
    return call()
  except Exception as exc:
    inputs.findings.append(refusal_row(exc, stage=stage, include_raise_site=False))
    return None


#: A printed blank whose zero value is corroborated by the filing's totals.
BLANK_CLOSURE_DERIVATION = "closure_corroborated_zero"


def _ref_json(value: Any) -> Any:
  ref = getattr(value, "ref", None)
  return None if ref is None else ref.model_dump(mode="json")


def filed_disagreement_findings(hff: Any) -> tuple[Finding, ...]:
  """Project accepted HFF disagreement gaps into the binding repair route."""
  from schema.build_findings import make_finding
  from schema.historical_foundation import FILED_VALUE_DISAGREES_GAP_PREFIX

  if hff is None:
    return ()
  return tuple(
    make_finding(
      "filed_value_disagrees",
      basis="pinned",
      message=gap.text[:1000],
      item_id=gap.details["role"],
      periods=(gap.details["fiscal_year"],),
      observed={
        "repair_owner": "binding",
        "selected_accession": gap.details["selected_accession"],
        "spread_value": gap.details["spread_value"],
      },
      expected={"filed_value": gap.details["filed_value"]},
    )
    for gap in hff.gaps
    if gap.key.split(":", 1)[0] == FILED_VALUE_DISAGREES_GAP_PREFIX
  )


def construction_input_baseline(
  inputs: StageBuildInputs, historical_years: Collection[int], unclaimed: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
  """Capture input-only check evidence once, while construction owns resolved inputs."""
  hff = inputs.hff
  tie_out = None if hff is None else hff.filed_tie_out
  return {
    "input_findings": list(inputs.findings),
    "historical_spread": None if hff is None else {
      "gaps": [gap.model_dump(mode="json") for gap in hff.gaps],
      "filed_tie_out": None if tie_out is None else tie_out.model_dump(mode="json"),
    },
    "filed_disagreements": [
      finding.model_dump(mode="json") for finding in filed_disagreement_findings(hff)
    ],
    "unclaimed_filed_lines": list(unclaimed),
    "resolved_refs": {
      key: _ref_json(artifact)
      for key, artifact in (
        ("forecast_assumption_set_ref", inputs.fas),
        ("scenario_set_ref", inputs.scenario_set),
        ("valuation_policy_ref", inputs.valuation_policy),
      )
      if artifact is not None
    },
    "forecast": None if inputs.fas is None else inputs.fas.model_dump(mode="json"),
    "accessions_by_year": {
      str(year): list(inputs.corpus.as_filed_accessions_for_period(f"FY{year}"))
      if inputs.corpus is not None else []
      for year in sorted(set(historical_years))
    },
  }


# --------------------------------------------------------------------------
# structure
# --------------------------------------------------------------------------


@dataclass
class SheetFrame:
  doc: document.ModelSheet
  registry: Any
  historical_years: tuple[int, ...]
  forecast_years: tuple[int, ...]
  template: FinancialModel | None = None


def _reported_fiscal_year_end(source_bundle: Any, most_recent_fy: int) -> str | None:
  """The month name of the bundle's reported period end for ``most_recent_fy``.

  None when the bundle has no period end: the structure records that and does
  not invent a month.
  """
  import calendar

  ends = [
    period.period_end
    for period in getattr(source_bundle, "reported_periods", ()) or ()
    if getattr(period, "fiscal_year", None) == most_recent_fy and period.period_end is not None
  ]
  if not ends:
    return None
  return calendar.month_name[max(ends).month]


def build_structure(
  inputs: StageBuildInputs, ticker: str, *, last_actual: int | None = None, axis: LiveAxis | None = None,
) -> SheetFrame:
  """Template + the BusinessModel's typed lowering + the profile's axis + the model kind's
  control binding (``unit_bound``: ``_bind_model_spec_control_units``, DSO/DPO in days and the
  acquisition and per-share bridges; ``registry_bound``: ``_bind_declared_model_spec_controls``
  over the kind's declared targets and semantic rows), as one recorded structural edit.

  ``last_actual`` is the axis as an input (``target_build``'s ``axis: {last_actual}``): the
  historical years run from the profile's first through ``last_actual`` and the projection keeps
  the profile's length from the next year, whatever the store has mapped.

  ``axis`` is the live sheet's axis (``live_sheet.live_axis``): the years when ``inputs.profile``
  is ``None``, and the fiscal-year-end month when ``inputs.bundle`` is ``None``. With no selected
  BusinessModel revision the template's own segment rows stay and the registry is empty.
  """
  from schema.build import load_template
  from schema.build_time_structure import remap_time_structure
  from schema.build_scenarios import _bind_declared_model_spec_controls, _bind_model_spec_control_units
  from schema.business_model_compiler import CompiledDriverRegistry, bind_driver_assumption_plan, compile_business_model
  from schema.model_kind_dispatch import (
    declared_capabilities,
    declared_control_binding,
    declared_grounded_semantic_rows,
    declared_scenario_controls,
    declared_semantic_row_materializations,
  )

  # The axis ``derive_fiscal_axis`` derives, read off the profile it reads.
  profile = inputs.profile
  if profile is not None:
    historical, forecast = profile_axis_years(profile)
  elif axis is not None:
    historical = axis.historical_years
    forecast = axis.forecast_years
  else:
    raise ValueError("build_structure needs a coverage profile or a live axis")
  if last_actual is not None:
    if last_actual < historical[0]:
      raise ValueError(f"axis.last_actual {last_actual} precedes the profile's first historical year {historical[0]}")
    historical = tuple(range(historical[0], last_actual + 1))
    forecast = tuple(range(last_actual + 1, last_actual + 1 + len(forecast)))
  model = load_template()
  template = model.model_copy(deep=True)
  if inputs.revision is None:
    registry = CompiledDriverRegistry(validation_model=model)
    steps = ["load_template", "remap_time_structure"]
  else:
    registry = bind_driver_assumption_plan(
      compile_business_model(model, inputs.revision.business_model),
      inputs.revision.driver_assumption_plan,
    )
    steps = ["load_template", "compile_business_model", "bind_driver_assumption_plan", "remap_time_structure"]
  remap_time_structure(
    model, most_recent_fy=historical[-1], n_historical=len(historical), n_projection=len(forecast),
  )
  model.build_index()
  capabilities = declared_capabilities(SimpleNamespace(coverage_profile=profile))
  binding = declared_control_binding(capabilities)
  if inputs.bundle is not None:
    fye = _reported_fiscal_year_end(inputs.bundle, historical[-1])
  else:
    fye = None if axis is None else axis.fiscal_year_end
  if fye is not None:
    from schema.build_company_info import update_company_info
    company = getattr(getattr(inputs.revision, "business_model", None), "company", None)
    update_company_info(
      model,
      ticker=ticker,
      name=str(getattr(company, "name", None) or ticker),
      fye=fye,
      sector=getattr(company, "sector", None),
      created_at=None,
    )
    steps.append("fiscal_year_end")
  else:
    steps.append("fiscal_year_end_unrecorded")
  model.build_index()
  if binding == "registry_bound":
    scenario_controls = declared_scenario_controls(capabilities)
    _bind_declared_model_spec_controls(
      model,
      registry,
      forecast_control_targets={} if scenario_controls is None else scenario_controls.forecast_control_target_map(),
      semantic_rows=declared_semantic_row_materializations(capabilities),
      grounded_semantic_rows=declared_grounded_semantic_rows(capabilities),
    )
    steps.append("bind_declared_model_spec_controls")
  else:
    _bind_model_spec_control_units(model)
    steps.append("bind_model_spec_control_units")
  model.build_index()
  doc = document.ModelSheet(model=model, model_spec=inputs.model_spec, semantic_values=(), source=f"bench target {ticker}")
  document.record_edit(
    doc,
    {"type": "structure", "steps": steps, "historical_periods": list(historical), "projection_periods": list(forecast)},
    source={"origin": "judgment", "artifact": "business_model_revision", "ref": _ref_json(inputs.revision)},
    result={
      "status": "ok",
      "driver_keys": dict(registry.driver_keys),
      "node_items": dict(registry.node_items),
      "segment_mapping": dict(registry.segment_mapping),
      "items": len(model._index),
      "control_binding": binding,
    },
  )
  return SheetFrame(
    doc=doc, registry=registry, historical_years=historical, forecast_years=forecast, template=template,
  )


def profile_axis_years(profile: Any) -> tuple[tuple[int, ...], tuple[int, ...]]:
  """The ``(historical, forecast)`` fiscal years a coverage profile names: its historical windows'
  periods and its projection periods. The one reading ``build_structure`` and
  ``live_sheet.live_axis`` share."""
  historical = tuple(sorted({
    _axis_year(period_id) for window in profile.historical_windows for period_id in window.period_ids
  }))
  forecast = tuple(sorted({_axis_year(period_id) for period_id in profile.projection_period_ids}))
  return historical, forecast


def _axis_year(period_id: str) -> int:
  """The fiscal year an annual period id names: its four-digit year (``FY2024``, ``FY2027E``)."""
  match = re.search(r"(?<!\d)(\d{4})(?!\d)", str(period_id))
  if match is None:
    raise ValueError(f"coverage profile period {period_id!r} carries no four-digit fiscal year")
  return int(match.group(1))


def _formula_owned(doc: document.ModelSheet, item_id: str, period: int) -> bool:
  item = doc.model.get_item(item_id)
  spec = document._render_spec_for_period(doc.model, item, period)
  # C's own inclusion marker is a source cell, never a document formula that owns the cell.
  return spec is not None and spec.type is not FormulaType.constant and spec.subtype != "not_separately_presented"


# --------------------------------------------------------------------------
# historical cells (C → D)
# --------------------------------------------------------------------------


def _role_targets(doc: document.ModelSheet, inputs: StageBuildInputs, registry: Any, ticker: str) -> dict[str, list[str]]:
  """Row role → its lines: the ``required_role`` stamp, the one role-items lookup's
  population and accepted-HFF fact items (schedule twins of a face role), operating
  requirements' declared model slots, and the BusinessModel node that claims the series.
  A row stored under a catalog alias lands on its home role's lines (``home_role``)."""
  from schema.build_bm_node_history import bm_required_series, derive_bm_series_claims, direct_segment_face_items
  from schema.required_role_catalog import load_required_role_catalog
  from schema.role_disposition import home_role, items_by_role

  catalog = load_required_role_catalog()
  home_of = {row.required_role: home_role(catalog, row.required_role) for row in inputs.rows}
  roles = set(home_of.values())
  by_home: dict[str, list[str]] = {}
  for item in doc.model._index.values():
    role = item.required_role
    if role is not None and role in roles:
      by_home.setdefault(role, []).append(item.id)
  for role, items in items_by_role(catalog, ticker=ticker).items():
    if role not in roles:
      continue
    for item_id in (*items.population_items, *items.hff_fact_items):
      if item_id in doc.model._index and item_id not in by_home.get(role, ()):
        by_home.setdefault(role, []).append(item_id)
  if inputs.bundle is not None:
    for requirement in inputs.bundle.requirements:
      role = requirement.required_role
      item_id = requirement.model_item
      if requirement.family == "operating" and role in roles and item_id in doc.model._index and item_id not in by_home.get(role, ()):
        by_home.setdefault(role, []).append(item_id)
  # No selected BusinessModel revision (or no profile) plans no BM-node targets.
  if inputs.revision is None or inputs.profile is None:
    claims, faces = (), {}
  else:
    claims, _findings = derive_bm_series_claims(
      inputs.revision.business_model, required=bm_required_series(inputs.profile.annual_requirements),
    )
    # A direct segment node's claim also lands on its segment's face line, as the node's history does.
    faces = direct_segment_face_items(inputs.revision.business_model, registry.segment_mapping) if claims else {}
  for claim in claims:
    role = claim.series if claim.series in roles else f"operating.{claim.series}"
    for item_id in (registry.node_items.get(claim.node_key), faces.get(claim.node_key)):
      if role in roles and item_id is not None and item_id in doc.model._index and item_id not in by_home.get(role, ()):
        by_home.setdefault(role, []).append(item_id)
  return {role: list(by_home[home]) for role, home in home_of.items() if home in by_home}


@dataclass(frozen=True)
class _Cell:
  """One C row as a document cell: ``kind`` ``input`` (``value`` set), ``unavailable``
  (``reason`` set), ``not_separately_presented`` (``included_in`` and ``absence_citation``
  set) or ``formula``; ``column`` is its semantic column."""

  kind: str
  column: dict[str, Any]
  value: float | None = None
  reason: CellUnavailableReason | None = None
  included_in: str | None = None
  absence_citation: dict[str, str] | None = None


def _filing_line_source(line: Any, capture_id: str | None, evidence_uri: str | None) -> dict[str, Any]:
  """A C cell's ``filing_line`` source: the ``FilingLineRef``, the capture it was read from, its
  ``filing-line:`` observation id and, when known, the sealed artifact's ref."""
  out: dict[str, Any] = {
    "kind": "filing_line",
    "ref": keyed_dump.filing_line_json(line),
    "capture_id": capture_id,
    "observation_id": projection.filing_line_observation_id(line),
  }
  if evidence_uri is not None:
    out["evidence_ref"] = evidence_uri
  return out


def vintage_history(vintages: Any) -> list[dict[str, Any]]:
  """A period's vintage candidates (``projection.PeriodVintages``) as a cell's
  ``source_history``: the selected vintage first, then the rest newest first, each
  ``{value, source, vintage_accession, available_at, available, selected, evidence_id,
  restates_evidence_id}``; ``available`` is false for a vintage accepted after the cutoff."""
  return [
    {
      "value": float(candidate.value),
      "source": _filing_line_source(
        candidate.line, candidate.capture_id, None if candidate.source_ref is None else candidate.source_ref.uri,
      ),
      "vintage_accession": candidate.source_id,
      "available_at": candidate.available_at.isoformat(),
      "available": candidate.available_at <= vintages.cutoff_at,
      "selected": candidate.evidence_id == vintages.selected,
      "evidence_id": candidate.evidence_id,
      "restates_evidence_id": candidate.restates_evidence_id,
    }
    for candidate in vintages.ordered()
  ]


def _historical_source(row: Any, value: Any, inputs: StageBuildInputs) -> _Cell:
  """The document cell of one C row. A bound reconciliation pin the statement projection does
  not read takes the value of the reconciliation-captured fact at the same ``FilingLineRef``."""
  facts = inputs.facts
  base: dict[str, Any] = {"required_role": row.required_role, "binding_state": row.state, "binding_source": row.binding_source}
  if row.state == "not_applicable":
    citation = None if row.absence_citation is None else row.absence_citation.model_dump(mode="json")
    if value is not None and value.state == "known":
      return _Cell("input", {
        **base, "origin": "derived",
        "source": {
          "kind": "absence_citation", "derivation": projection.EVIDENCED_ABSENCE_DERIVATION,
          "citation": citation,
        },
      }, value=float(value.value))
    return _Cell("unavailable", {**base, "source": {"kind": "absence_citation", "citation": citation}}, reason="not_applicable_evidenced")
  if row.state == "not_separately_presented":
    # A cited inclusion pins no line: it is the compiler's NA marker, never a numeric source.
    citation = cast("AbsenceCitation", row.absence_citation).model_dump(mode="json")
    included_in = cast(str, row.included_in)
    return _Cell(
      "not_separately_presented",
      {**base, "source": {"kind": "absence_citation", "citation": citation, "included_in": included_in}},
      included_in=included_in,
      absence_citation=citation,
    )
  if row.state == "unbound":
    kpi = None if row.kpi_citation is None else {
      "accession": row.kpi_citation.accession, "row": row.kpi_citation.row, "span": row.kpi_citation.span,
    }
    return _Cell("unavailable", {
      **base,
      "source": {"kind": "unbound", "reason": row.unbound_reason, "kpi_citation": kpi, "capture_id": row.capture_id},
    }, reason="not_disclosed_in_vintage" if row.unbound_reason == NOT_DISCLOSED_IN_VINTAGE else "source_observation_absent")
  line = row.pinned_ref
  evidence = facts.source_refs.get(row.key)
  filing_line = _filing_line_source(line, row.capture_id, None if evidence is None else evidence.uri)
  reconciled = inputs.reconciliation.get(line) if value is None and row.binding_source == "reconciliation" else None
  if reconciled is not None:
    fact, logical_id, sha256 = reconciled
    if (fact.unit, fact.scale) != (row.unit, row.scale):
      return _Cell("unavailable", {
        **base, "source": filing_line,
        "value": {"reconciliation_fact": fact.fact_id, "unit": fact.unit, "scale": fact.scale, "required": [row.unit, row.scale]},
      }, reason="source_value_unavailable")
    filing_line.update({"capture_id": logical_id, "capture_sha256": sha256, "fact_id": fact.fact_id})
    return _Cell("input", {**base, "origin": "observed", "source": filing_line}, value=float(fact.value))
  if value is None or value.state != "known":
    blank = inputs.blank_closures.get(row.key)
    if blank is not None and blank["verdict"] == "corroborated":
      debt_absence = projection.evidenced_debt_absence_value(row, facts, closure=blank)
      if debt_absence is not None:
        return _Cell("input", {
          **base, "origin": "derived",
          "source": {
            "kind": "absence_citation", "derivation": projection.EVIDENCED_ABSENCE_DERIVATION,
            "line": filing_line, "closure": blank,
          },
        }, value=float(debt_absence.value))
      # Filed closure proves a zero operand, not a nonnumeric absence.
      return _Cell("input", {
        **base,
        "origin": "derived",
        "source": {"kind": "derivation", "derivation": BLANK_CLOSURE_DERIVATION, "line": filing_line, "closure": blank},
      }, value=0.0)
    note = None if value is None else {"value_state": value.state, "value_reason": getattr(value, "primary_reason", None)}
    column = {**base, "source": filing_line, "value": note}
    if blank is not None:
      column["presented_blank"] = blank
    return _Cell("unavailable", column, reason="source_value_unavailable")
  if value.origin == "derived":
    observation = facts.observations.get(row.key)
    detail = None if observation is None or not observation.detail else json.loads(observation.detail)
    binding = value.origin_bindings[0]
    source: dict[str, Any] = {"kind": "derivation", "derivation": binding.formula_node_id, "line": filing_line, "detail": detail}
    components = None if observation is None else projection.composite_components(observation)
    if components is not None:
      # A composite's signed sum: each filed line it adds, with its sign and cited arc.
      source["components"] = [dict(component) for component in components]
    return _Cell("input", {**base, "origin": "derived", "source": source}, value=float(value.value))
  return _Cell("input", {**base, "origin": "observed", "source": filing_line}, value=float(value.value))


@dataclass
class HistoricalPlan:
  """What C asks of the document's historical cells: the inputs (value and semantic column per
  ``(item, year)``), the unavailable cells (reason and column), the cited inclusions, the cells
  the document's own formula owns (``formula``: C's column there), and every cell a C row
  reached (``covered``: role per cell)."""

  targets: dict[str, list[str]]
  inputs: dict[tuple[str, int], tuple[float, dict[str, Any]]]
  unavailable: dict[tuple[str, int], tuple[CellUnavailableReason, dict[str, Any]]]
  included: dict[tuple[str, int], tuple[str, dict[str, str], dict[str, Any]]]
  formula: dict[tuple[str, int], dict[str, Any]]
  covered: dict[tuple[str, int], str]
  counts: Counter[str]
  findings: list[dict[str, Any]]
  rows: tuple[Any, ...] = ()


def plan_historical_cells(
  structure: SheetFrame, inputs: StageBuildInputs, ticker: str, *, roles: Collection[str] | None = None,
) -> HistoricalPlan:
  """C's per-period rows as document cells on the role's lines, nothing written. A role line the
  document holds as a header or spacer still takes its cells (as compile's population writes
  them), and is a ``role_line_not_a_value_row`` finding: the renderer writes such a line's label
  only, until a construction helper makes it a value row (the treasury schedule, for one)."""
  doc = structure.doc
  targets = _role_targets(doc, inputs, structure.registry, ticker)
  findings: list[dict[str, Any]] = []
  for role, item_ids in targets.items():
    for item_id in item_ids:
      item_type = doc.model.get_item(item_id).item_type
      if item_type in (ItemType.header, ItemType.spacer):
        findings.append({"code": "role_line_not_a_value_row", "required_role": role, "item_id": item_id, "item_type": item_type.value})
  years = set(structure.historical_years)
  from schema.required_role_catalog import load_required_role_catalog
  from schema.role_disposition import home_role

  role_catalog = load_required_role_catalog()
  homes = {role: home_role(role_catalog, role) for role in targets}
  hff_only_by_role = {
    role: {
      item_id
      for item_id in role_catalog.hff_fact_item_ids(home)
      if item_id in doc.model._index
      and doc.model.get_item(item_id).required_role != home
      and item_id not in role_catalog.population_item_ids(home, ticker=ticker)
    }
    for role, home in homes.items()
  }
  planned: dict[tuple[str, int], tuple[float, dict[str, Any]]] = {}
  unavailable: dict[tuple[str, int], tuple[CellUnavailableReason, dict[str, Any]]] = {}
  included: dict[tuple[str, int], tuple[str, dict[str, str], dict[str, Any]]] = {}
  planned_rows: list[Any] = []
  formula: dict[tuple[str, int], dict[str, Any]] = {}
  written_by: dict[tuple[str, int], str] = {}
  counts: Counter[str] = Counter()
  for row in inputs.rows:
    if roles is not None and row.required_role not in roles:
      continue
    year = int(row.period_id.removeprefix("FY"))
    if year not in years:
      counts["c_rows_outside_axis"] += 1
      continue
    planned_rows.append(row)
    items = targets.get(row.required_role, [])
    if not items:
      counts["c_rows_no_document_line"] += 1
      findings.append({
        "code": "required_role_has_no_document_line",
        "required_role": row.required_role, "period": row.period_id, "binding_state": row.state,
      })
      continue
    cell = _historical_source(row, inputs.values.get(row.key), inputs)
    vintages = inputs.vintages.get(row.key)
    if vintages is not None and cell.kind != "formula":
      cell.column["source_history"] = vintage_history(vintages)
      counts["c_vintaged"] += 1
    counts[f"c_{cell.kind}"] += 1
    blank = inputs.blank_closures.get(row.key)
    if blank is not None and row.pinned_ref is not None:
      corroborated = blank["verdict"] == "corroborated"
      counts["c_presented_blank_" + ("corroborated" if corroborated else "uncorroborated")] += 1
      findings.append({
        "code": BLANK_CLOSURE_DERIVATION if corroborated else "presented_blank_uncorroborated",
        "required_role": row.required_role, "period": row.period_id, "items": items,
        "line": keyed_dump.filing_line_json(row.pinned_ref), "reason": cell.reason, **blank,
      })
    for item_id in items:
      key = (item_id, year)
      prior = written_by.get(key)
      if prior is not None:
        if key in planned:
          held = planned[key][0]
          # An observed non-zero value supersedes a zero derived from an absence/closure
          # on an alias or duplicate role for the same line item
          if cell.value is not None and abs(held) < 1e-9 and abs(cell.value) >= 1e-9:
            planned[key] = (cell.value, cell.column)
            written_by[key] = row.required_role
            continue
          if cell.value is not None and abs(cell.value) < 1e-9 and abs(held) >= 1e-9:
            continue
          if cell.value is not None and held != cell.value:
            findings.append({
              "code": "cell_role_conflict", "item_id": item_id, "period": year,
              "kept_role": prior, "other_role": row.required_role, "kept": held, "other": cell.value,
            })
          continue
        if cell.value is None:
          continue
        # A later observed value supersedes an earlier absence on the same
        # line: two role names for one position (a catalog alias and its
        # home) must not let the name's absence erase the filing fact,
        # whichever row is planned first.
        unavailable.pop(key, None)
        included.pop(key, None)
        formula.pop(key, None)
      formula_owned = _formula_owned(doc, item_id, year)
      if item_id in hff_only_by_role.get(row.required_role, ()):
        # A schedule twin listed only in hff_fact_items keeps its formula on a
        # bound year and receives C's cited absence on a not_applicable year.
        item_obj = doc.model.get_item(item_id)
        has_historical_formula = (
          item_obj.historical is not None
          and item_obj.historical.type is not FormulaType.constant
        )
        if cell.kind == "unavailable" and cell.reason == "not_applicable_evidenced":
          written_by[key] = row.required_role
          unavailable[key] = (cell.reason, cell.column)
        elif formula_owned or (row.state == "bound" and has_historical_formula):
          written_by[key] = row.required_role
          counts["formula_owned_cells"] += 1
          formula[key] = cell.column
        continue
      written_by[key] = row.required_role
      if formula_owned and row.state == "not_applicable":
        counts["formula_owned_cells"] += 1
        formula[key] = cell.column
      elif cell.value is not None:
        planned[key] = (cell.value, cell.column)
      elif cell.included_in is not None and cell.absence_citation is not None:
        # The cited inclusion owns its leaf in that period, as compile's lowering does.
        included[key] = (cell.included_in, cell.absence_citation, cell.column)
      elif cell.kind == "formula" or formula_owned:
        # The document's own formula owns the cell: nothing to write.
        counts["formula_owned_cells"] += 1
        formula[key] = cell.column
        if cell.reason is not None:
          findings.append({
            "code": "formula_owned_source_absent", "item_id": item_id, "period": year,
            "required_role": row.required_role, "reason": cell.reason,
          })
      elif cell.reason is not None:
        unavailable[key] = (cell.reason, cell.column)
  return HistoricalPlan(
    targets=targets, inputs=planned, unavailable=unavailable, included=included, formula=formula,
    covered=written_by, counts=counts, findings=findings, rows=tuple(planned_rows),
  )


def _write_planned(
  doc: document.ModelSheet,
  planned: Mapping[tuple[str, int], tuple[float, Mapping[str, Any]]],
  unavailable: Mapping[tuple[str, int], tuple[CellUnavailableReason, Mapping[str, Any]]],
  included: Mapping[tuple[str, int], tuple[str, Mapping[str, str], Mapping[str, Any]]],
) -> None:
  """Planned cells through the document writers: one ``set_inputs`` per line, then each
  unavailable cell ``mark_unavailable`` and each cited inclusion ``mark_not_separately_presented``."""
  by_item: dict[str, dict[int, tuple[float, Mapping[str, Any]]]] = {}
  for (item_id, year), cell in planned.items():
    by_item.setdefault(item_id, {})[year] = cell
  with document.transaction(doc):
    for item_id, by_year in by_item.items():
      document.set_inputs(
        doc, item_id, {year: value for year, (value, _) in by_year.items()},
        sources={year: column for year, (_, column) in by_year.items()},
      )
    for (item_id, year), (reason, column) in sorted(unavailable.items()):
      if reason == "not_applicable_evidenced" and _formula_owned(doc, item_id, year):
        item = doc.model.get_item(item_id)
        periods = item.formula_periods
        if periods is not None and year in periods:
          item.formula_periods = [int(period) for period in periods if int(period) != year]
      document.mark_unavailable(doc, item_id, year, reason=reason, source=column)
    for (item_id, year), (included_in, citation, column) in sorted(included.items()):
      document.mark_not_separately_presented(
        doc, item_id, year, included_in=included_in, absence_citation=citation, source=column,
      )


def lower_inclusions(
  doc: document.ModelSheet, inputs: StageBuildInputs, rows: Sequence[Any], *,
  kept: Collection[Any] = (), cleared_role_periods: Collection[tuple[str, int]] = (),
  previous_markers: Mapping[tuple[str, int], Any] | None = None,
) -> dict[str, Any]:
  """Compile's shared cited-inclusion lowering over the document, once per C write pass.

  ``rows`` are C's per-period requirements for the roles the pass wrote, in the product's
  annual-requirement form (``_pin_from_periods``); ``kept`` names the role-periods whose leaf an
  agent or judgment still holds, which enter as non-included so their authored value stays an
  operand. The document supplies every persisted role-to-line identity, including unmentioned
  C parents with no catalog template items. ``apply_not_separately_presented_periods`` owns
  markers and operand sets, including restoring them when a period is no longer included.
  ``cleared_role_periods`` names former C
  sources this refresh actually removed, including roles no longer in the profile. Transient
  ``previous_markers`` let the owner recover an authored formula a source clear replaced; a newly
  written override stays authoritative. The document's protected agent and judgment cells,
  including face aliases outside population targets, retain their operand membership.
  A changed additive or restored authored formula takes that formula's semantic column.
  Without a coverage profile there are no C rows and no inclusions to lower."""
  from model_build.corpus_facts import _pin_from_periods

  if inputs.profile is None:
    return {"cells_changed": []}
  authored = set(kept)
  held_inclusions = {
    (column["required_role"], f"FY{period}")
    for (item_id, period), column in doc.semantic.items()
    if column.get("cell") == "not_separately_presented" and column.get("required_role")
  }
  effective = [
    replace(row, state="unbound", absence_citation=None, included_in=None, disposition=None)
    if row.key in authored and row.state == "not_separately_presented" else row
    for row in rows if row.key not in authored or row.key not in held_inclusions
  ]

  lowered = document.reconcile_inclusions(
    doc, _pin_from_periods(inputs.profile, effective, {}),
    cleared_role_periods=cleared_role_periods, previous_markers=previous_markers,
  )
  cells = lowered["cells_changed"]
  if cells:
    document.record_edit(
      doc,
      {"type": "not_separately_presented_lowering"},
      source={
        "origin": "c_inclusion",
        "owner": "schema/build_company_grounded_schedules.py:apply_not_separately_presented_periods",
        "rows": len(effective), "kept": sorted([list(key) for key in authored]),
      },
      result={"status": "ok", "cells": cells},
    )
  return {"cells_changed": cells}


def _restore_disclosed_interest(doc: document.ModelSheet, historical_basis_facts: Any) -> None:
  """The EBITDA-bridge interest amount, after inclusion lowering has marked the face line.

  Company-grounded schedules and C refills first wire the filed net other-income projection
  from the inclusion. Then write the separate bridge amount before valuation reads reported
  interest to derive the credit spread, without changing the income-statement face marker.
  """

  from schema.build_financial_schedules import apply_disclosed_interest_bridge

  apply_disclosed_interest_bridge(doc.model, historical_basis_facts)
  item = doc.model._index.get("tpl.fm.adjusted_earnings.interest_expense")
  if item is None:
    return
  amounts = {
    int(period): float(spec.params["value"])
    for period, spec in (item.overrides or {}).items()
    if getattr(spec, "note", None) == "issuer_disclosed_adjusted_ebitda_bridge_interest"
    and isinstance(spec.params.get("value"), (int, float))
    and not isinstance(spec.params.get("value"), bool)
  }
  if not amounts:
    return
  column = {
    "origin": "derived",
    "source": {
      "kind": "construction_helper",
      "helper": "disclosed_interest_bridge",
      "owner": "schema/build_financial_schedules.py:apply_disclosed_interest_bridge",
      "note": "issuer_disclosed_adjusted_ebitda_bridge_interest",
    },
  }
  document.set_inputs(doc, item.id, amounts, sources={period: column for period in amounts})



def write_historical_cells(
  structure: SheetFrame, inputs: StageBuildInputs, ticker: str, *, roles: Collection[str] | None = None,
) -> dict[str, Any]:
  """C's per-period rows onto the role's lines (``plan_historical_cells``, then the writers and
  the shared inclusion lowering).

  ``roles`` restricts the pass to those roles: the identity refill after a construction helper
  created their lines (compile's second ``apply_required_line_identity``)."""
  plan = plan_historical_cells(structure, inputs, ticker, roles=roles)
  doc = structure.doc
  with document.transaction(doc):
    _write_planned(doc, plan.inputs, plan.unavailable, plan.included)
    lowered = lower_inclusions(doc, inputs, plan.rows)
  counts = plan.counts
  counts["cells_input"] = len(plan.inputs)
  counts["cells_unavailable"] = len(plan.unavailable)
  counts["cells_not_separately_presented"] = len(plan.included)
  counts["cells_lowered"] = len(lowered["cells_changed"])
  return {"counts": dict(counts), "findings": plan.findings, "targets": plan.targets}


# --------------------------------------------------------------------------
# judgment cells
# --------------------------------------------------------------------------


def _judgment(artifact: str, ref: Any, row: Mapping[str, Any]) -> dict[str, Any]:
  return {"origin": "manual_input", "source": {"kind": "judgment_row", "artifact": artifact, "ref": ref, "row": dict(row)}}


def write_seed_cells(
  structure: SheetFrame, inputs: StageBuildInputs, *, periods: Collection[int] | None = None, protect: Collection[tuple[str, int]] = (),
) -> dict[str, Any]:
  """The HFF's historical driver seeds, then the BusinessModel's observation seeds, onto the
  node rows they name; ``periods`` restricts the pass to those years (an advance's new actual
  year), and a ``protect``ed cell (the agent's) is never written (``historical_driver_seed_kept_agent``).
  An observation seed at a (driver key, period) the HFF seeds is not written: the HFF's wins.
  A direct segment-revenue node's loop (``release_unseeded_segment_revenue_cycles``) then
  releases the years nothing seeded."""
  from schema.build_bm_node_history import release_unseeded_segment_revenue_cycles
  from schema.build_historical_driver_seeds import (
    _direct_segment_revenue_item_id,
    hff_segment_revenue_series_values,
  )

  doc = structure.doc
  findings: list[dict[str, Any]] = []
  written = 0
  hff_seeds = () if inputs.hff is None else tuple(inputs.hff.historical_driver_seeds)
  segment_revenue_keys = {
    row.driver_key
    for row in (getattr(inputs.hff, "historical_driver_seed_requirements", ()) or ())
    if getattr(row, "history_scope", None) == "segment_revenue"
  }
  held_keys = {(seed.driver_key, seed.period_id) for seed in hff_seeds}
  obs_citations_by_key: dict[tuple[str, str], list[dict[str, Any]]] = {}
  bm_evidence_by_id: dict[str, Any] = {}
  if inputs.revision is not None and getattr(inputs.revision, "business_model", None) is not None:
    stage_meta = getattr(inputs.revision, "stage_metadata", None)
    ev_snap = getattr(stage_meta, "evidence_snapshot", None) if stage_meta is not None else None
    for bm_ev in getattr(ev_snap, "evidence", ()) or ():
      if getattr(bm_ev, "evidence_id", None):
        bm_evidence_by_id[str(bm_ev.evidence_id)] = bm_ev

    def _collect_observation_citations(segment_id: str, nodes: Any) -> None:
      for node in nodes:
        for obs in getattr(node, "historical_observations", ()) or ():
          obs_citations_by_key[(f"{segment_id}.{node.id}", str(obs.period_id))] = [
            citation.model_dump(mode="json")
            for citation in (getattr(obs, "citations", None) or (obs.citation,))
            if citation is not None
          ]
        _collect_observation_citations(segment_id, node.children or ())

    for segment in inputs.revision.business_model.segments:
      _collect_observation_citations(segment.id, segment.revenue_model.decomposition)
  sources = (
    ("historical_foundation", _ref_json(inputs.hff), hff_seeds),
    (
      "business_model", _ref_json(inputs.revision),
      tuple(seed for seed in inputs.bm_observation_seeds if (seed.driver_key, seed.period_id) not in held_keys),
    ),
  )

  def _write_one(artifact: str, ref: Any, seed: Any, target_id: str, wyear: int, wvalue: Any, wfact_ids: Any, wperiod_id: str, wdisposition: str | None = None) -> None:
    nonlocal written
    if periods is not None and wyear not in periods:
      return
    if wyear not in structure.historical_years:
      return
    if (target_id, wyear) in protect:
      findings.append({"code": "historical_driver_seed_kept_agent", "item_id": target_id, "period": wyear, "seed": float(wvalue)})
      return
    held = doc.semantic.get((target_id, wyear))
    if held is not None and held.get("cell") == "input" and document.PROVISIONAL not in held:
      stored = (doc.model.get_item(target_id).overrides or {}).get(wyear)
      stored_value = None if stored is None else stored.params.get("value")
      if stored_value is None or abs(float(stored_value) - float(wvalue)) > 1e-6:
        findings.append({
          "code": "historical_driver_seed_conflict", "item_id": target_id, "period": wyear,
          "document": stored_value, "seed": float(wvalue), "kept": "document",
        })
      return
    row = {
      "driver_key": seed.driver_key, "driver_id": seed.driver_id, "period_id": wperiod_id,
      "fact_ids": list(wfact_ids or ()), "disposition": wdisposition,
    }
    citations = obs_citations_by_key.get((seed.driver_key, wperiod_id))
    if (
      citations
      and artifact == "business_model"
      and getattr(seed, "disposition", None) in {"observed", "derived"}
      and bool(getattr(seed, "evidence_ids", None))
    ):
      resolved_citations: list[dict[str, Any]] = []
      seed_ev_ids = tuple(str(eid) for eid in (getattr(seed, "evidence_ids", ()) or ()))
      seed_val_ids = frozenset(str(vid) for vid in (getattr(seed, "evidence_value_ids", ()) or ()))
      for cit in citations:
        cit_copy = dict(cit)
        cit_path = str(cit_copy.get("path") or "")
        if cit_path.endswith("occurrences.jsonl") and "concept" not in cit_copy:
          for eid in seed_ev_ids:
            bm_ev = bm_evidence_by_id.get(eid)
            loc = getattr(bm_ev, "locator", None) if bm_ev is not None else None
            if (
              loc is not None
              and getattr(loc, "kind", None) == "corpus_excerpt"
              and getattr(loc, "section", None) == f"capture:{cit_path}"
              and getattr(loc, "char_start", None) == cit_copy.get("start")
            ):
              for val in getattr(bm_ev, "values", ()) or ():
                if (not seed_val_ids or getattr(val, "value_id", None) in seed_val_ids) and getattr(val, "concept", None):
                  cit_copy["concept"] = str(val.concept)
                  break
              break
        resolved_citations.append(cit_copy)
      row["citations"] = resolved_citations
    document.set_input(doc, target_id, wyear, float(wvalue), source=_judgment(artifact, ref, row))
    written += 1

  with document.transaction(doc):
    for artifact, ref, seeds in sources:
      for seed in seeds:
        item_id = structure.registry.node_items.get(seed.driver_key)
        year = int(str(seed.period_id).removeprefix("FY"))
        if item_id is None or year not in structure.historical_years:
          if periods is None or year in periods:
            findings.append({"code": "historical_driver_seed_unplaced", "driver_key": seed.driver_key, "period": seed.period_id})
          continue
        target_ids = [item_id]
        if (
          (artifact == "business_model" or (
            artifact == "historical_foundation" and seed.driver_key in segment_revenue_keys
          ))
          and inputs.revision is not None
          and getattr(inputs.revision, "business_model", None) is not None
        ):
          direct_id = _direct_segment_revenue_item_id(inputs.revision, structure.registry, seed.driver_key)
          if direct_id is not None and direct_id in doc.model._index and direct_id not in target_ids:
            target_ids.append(direct_id)
        for target_id in target_ids:
          _write_one(artifact, ref, seed, target_id, year, seed.value, getattr(seed, "fact_ids", ()) or (), seed.period_id)
        if artifact == "historical_foundation" and inputs.hff is not None:
          replayed = hff_segment_revenue_series_values(inputs.hff, seed, structure.historical_years)
          for ryear in sorted(replayed):
            rvalue, rfact_ids, rdisposition = replayed[ryear]
            for target_id in target_ids:
              _write_one(artifact, ref, seed, target_id, int(ryear), rvalue, rfact_ids, f"FY{int(ryear)}", rdisposition)
  if inputs.revision is not None:
    segment_mapping = structure.registry.segment_mapping
    release_unseeded_segment_revenue_cycles(
      doc.model, inputs.revision.business_model, segment_mapping,
      structure.historical_years if periods is None else periods,
    )
  return {"written": written, "findings": findings}


def write_forecast_cells(
  structure: SheetFrame, inputs: StageBuildInputs, *, reason: str | None = None, protect: document.Protected | None = None,
) -> dict[str, Any]:
  """FAS driver assumptions lowered by compile's own pass (``_apply_model_spec_assumptions``: the
  driver key's target row, the unit check, value normalization and disclosed-mix scaling, the
  model kind's equation-owned controls), run as one construction edit. Every input cell it
  writes carries the FAS judgment row (driver key, assumption id) whose coverage names it.
  The assumptions are the ModelSpec's when it was assembled, else the FAS projection's rows
  (``StageBuildInputs.fas_forecast_rows``, the adapter's) minted the same way (``mint_forecast_assumptions``).
  ``reason`` / ``protect`` as ``construct`` (an advance re-lowers the FAS onto the moved window)."""
  from schema.build import _model_spec_drivers
  from schema.build_mbc_entry import _apply_model_spec_assumptions
  from schema.model_kind_dispatch import declared_capabilities, declared_share_conversion
  from schema.model_spec_projections import mint_forecast_assumptions

  if inputs.fas is None or inputs.hff is None:
    return {"written": 0, "drivers": 0, "coverage": [], "findings": [{"code": "forecast_assumption_set_absent"}], "construction": None}
  if inputs.model_spec is not None:
    assumptions = inputs.model_spec.forecast_assumptions
  else:
    rows = inputs.fas_forecast_rows
    assumptions = None if rows is None else _captured(inputs, "fas_mint", lambda: mint_forecast_assumptions(rows))
    if assumptions is None:
      return {"written": 0, "drivers": 0, "coverage": [], "findings": inputs.findings[-1:], "construction": None}
  drivers, incomplete = _model_spec_drivers(assumptions, structure.forecast_years)
  share_conversion = declared_share_conversion(declared_capabilities(SimpleNamespace(coverage_profile=inputs.profile)))
  equation_owned = {} if share_conversion is None else dict(share_conversion.equation_owned_control_map())
  ref = _ref_json(inputs.fas)

  def source(coverage: Any, item_id: str, period: int) -> dict[str, Any] | None:
    for row in coverage or ():
      if row.get("resolved_item_id") == item_id and row.get("source_kind") == "model_spec_assumption":
        return _judgment("forecast_assumption_set", ref, {
          "driver": row.get("canonical_driver_key"), "assumption_id": row.get("assumption_id"), "period": period,
        })
    return None

  row = construct(
    structure,
    Helper("forecast_assumptions", "schema/build_mbc_entry.py:_apply_model_spec_assumptions"),
    lambda model: _apply_model_spec_assumptions(
      model, drivers, structure.registry,
      None if inputs.revision is None else inputs.revision.business_model,
      equation_owned_controls=equation_owned,
    ),
    inputs={"forecast_assumption_set": ref, "drivers": sorted(drivers)},
    cell_source=source,
    reason=reason,
    protect=protect,
  )
  return {
    "written": row["inputs_written"],
    "drivers": len(drivers),
    "coverage": row.pop("_returned", None) or [],
    "findings": [finding.model_dump(mode="json") for finding in incomplete],
    "construction": row,
  }


def write_valuation_cells(structure: SheetFrame, inputs: StageBuildInputs, refusals: _Refusals) -> dict[str, Any]:
  """ValuationPolicy controls onto the rows ``schema.build_valuation_inputs`` maps them to."""
  from schema.build import _merge_model_spec_valuation_controls
  from schema.build_valuation_inputs import (
    _VALUATION_ECONOMIC_INPUTS,
    _VALUATION_NET_SHARE_DILUTION_RATE,
    _VALUATION_SCENARIO_CONTROL_INPUTS,
  )
  from schema.model_spec_projections import project_valuation_policy

  if inputs.valuation_policy is None:
    return {"written": 0, "findings": [{"code": "valuation_policy_absent"}]}
  doc = structure.doc
  projected = refusals.run("valuation_policy_projection", lambda: project_valuation_policy(inputs.valuation_policy))
  if projected is None:
    return {"written": 0, "findings": []}
  merged = _merge_model_spec_valuation_controls(projected.valuation_controls)
  ref = _ref_json(inputs.valuation_policy)
  forecast = list(structure.forecast_years)
  written = 0
  findings: list[dict[str, Any]] = []

  def put(item_id: str, value: float, row: Mapping[str, Any], periods: list[int]) -> None:
    nonlocal written
    if item_id not in doc.model._index:
      findings.append({"code": "valuation_target_missing", "item_id": item_id, **row})
      return
    item = doc.model.get_item(item_id)
    targets = periods[:1] if item.column is not None else periods
    sources = {period: _judgment("valuation_policy", ref, {**row, "period": period}) for period in targets}
    document.set_inputs(doc, item_id, {period: float(value) for period in targets}, sources=sources)
    written += len(targets)

  with document.transaction(doc):
    for field_name, item_id in _VALUATION_ECONOMIC_INPUTS.items():
      value = None if merged is None else getattr(merged, field_name, None)
      if value is not None:
        put(item_id, float(value), {"field": f"valuation_controls.{field_name}"}, forecast)
    for family, attribute in (("dcf_terminal_growth", "terminal_growth_cases"), ("dcf_exit_multiple", "exit_multiple_cases")):
      cases = getattr(projected, attribute, None)
      if cases is None:
        continue
      for case, item_id in _VALUATION_SCENARIO_CONTROL_INPUTS[family].items():
        put(item_id, float(getattr(cases, case)), {"field": f"{attribute}.{case}"}, forecast)
    dilution = getattr(projected, "net_share_dilution", None)
    if dilution is not None:
      for rate in dilution.rates:
        if int(rate.fiscal_year) in forecast:
          put(_VALUATION_NET_SHARE_DILUTION_RATE, float(rate.rate), {"field": "net_share_dilution", "fiscal_year": rate.fiscal_year}, [int(rate.fiscal_year)])
  return {"written": written, "findings": findings}


def write_scenario_cells(structure: SheetFrame, inputs: StageBuildInputs) -> dict[str, Any]:
  """ScenarioSet case probabilities onto the expected-value rows. The bull/bear driver curves
  have no cell form in this document (the template carries cases as ``model.scenarios``, which
  the scenario analysis computes); they are counted, and the analysis recorded stale."""
  if inputs.scenario_set is None:
    return {"written": 0, "curves_without_cells": 0, "findings": [{"code": "scenario_set_absent"}]}
  doc = structure.doc
  ref = _ref_json(inputs.scenario_set)
  probabilities = inputs.scenario_set.case_probabilities
  written = 0
  with document.transaction(doc):
    for case, item_id in _CASE_PROBABILITY_ITEMS.items():
      if item_id not in doc.model._index:
        continue
      item = doc.model.get_item(item_id)
      periods = [structure.forecast_years[0]] if item.column is not None else list(structure.forecast_years)
      value = float(getattr(probabilities, case))
      document.set_inputs(doc, item_id, {period: value for period in periods}, sources={
        period: _judgment("scenario_set", ref, {"field": f"case_probabilities.{case}"}) for period in periods
      })
      written += len(periods)
  return {"written": written, "curves_without_cells": len(inputs.scenario_set.driver_curves), "findings": []}


#: The template's scenario case selector and the presented case (``schema/build_scenarios.py``
#: ``_populate_scenario_inputs``: bull 1, base 2, bear 3; the rendered model is pinned to Base).
_SCENARIO_SELECTOR = "tpl.a.header.scenario_value"
_PRESENTED_CASE = ("base", 2.0)


def write_presented_case(structure: SheetFrame) -> int:
  """The case selector set to Base: the presented case is the FAS path; the other cases are the
  scenario analysis's, which this document does not compute."""
  doc = structure.doc
  if _SCENARIO_SELECTOR not in doc.model._index:
    return 0
  item = doc.model.get_item(_SCENARIO_SELECTOR)
  periods = [structure.forecast_years[0]] if item.column is not None else list(structure.forecast_years)
  case, value = _PRESENTED_CASE
  column = {"origin": "manual_input", "source": {"kind": "presentation_choice", "case": case, "value": value}}
  document.set_inputs(doc, _SCENARIO_SELECTOR, {period: value for period in periods}, sources={period: column for period in periods})
  return len(periods)


# --------------------------------------------------------------------------
# construction helpers (compile's pieces, run once on the document)
# --------------------------------------------------------------------------


@dataclass(frozen=True)
class Helper:
  """One construction class: the compile piece it runs (``owner``, file:function) and the
  inputs it cannot run without (``model_spec``, ``bundle``). A cell a missing helper would
  have produced keeps its unavailable mark with ``gap: construction_helper_missing:<name>``."""

  name: str
  owner: str
  needs: tuple[str, ...] = ()
  #: The helper returns the ids of the rows it retired (``DependencyGraph.retire``); their authored
  #: inputs are not restored, the helper's own decision having removed the cells.
  retires: bool = False
  #: The helper's output depends on its inputs by removing or rewriting the lines they do not select
  #: (the valuation policy's suppression) or by authoring formula cells only where the line holds none (the
  #: spine's pinned-opening delta); its edits add and remove no lines: a re-run first puts back what its own
  #: recorded edits changed (``_recorded_structure_to_reset``: line fields and formula overrides), as a cold construction has it.
  resets_structure: bool = False


#: Authored absences a helper never overrides: the agent's evidenced not-applicable judgment.
_AUTHORED_ABSENCE = frozenset({"not_applicable_evidenced"})
#: C's nonnumeric actuals that supersede a provisional forecast: evidenced absence and cited inclusion.
_ACTUAL_NONNUMERIC = _AUTHORED_ABSENCE | {"not_separately_presented"}


def _cell_state(item: Any, period: int) -> tuple[Any, Any]:
  """What one period of a line stores as an input: its constant override and its value cell."""
  override = None if item is None else (item.overrides or {}).get(period)
  if override is not None and override.type is not FormulaType.constant:
    override = None
  value = None if item is None or item.values is None else item.values.values.get(period)
  return override, value


def _structure_of(item: Any) -> tuple[Any, ...]:
  formula_overrides = tuple(sorted(
    (int(period), spec)
    for period, spec in (item.overrides or {}).items()
    if spec.type is not FormulaType.constant
  ))
  periods = None if item.formula_periods is None else tuple(item.formula_periods)
  return (item.item_type, item.historical, item.projected, periods, formula_overrides)


def _restore_period(trial_item: Any, old_item: Any, period: int) -> None:
  override = None if old_item is None else (old_item.overrides or {}).get(period)
  _constant, value = _cell_state(old_item, period)
  if override is None:
    (trial_item.overrides or {}).pop(period, None)
  else:
    trial_item.overrides = {**(trial_item.overrides or {}), period: override.model_copy(deep=True)}
  if value is None:
    if trial_item.values is not None:
      trial_item.values.values.pop(period, None)
    return
  if trial_item.values is None and old_item is not None and old_item.values is not None:
    trial_item.values = old_item.values.model_copy(update={"values": {}})
  if trial_item.values is not None:
    trial_item.values.values[period] = value.model_copy(deep=True)


def _baseline_protected(
  doc: document.ModelSheet, trial: Any, protect: document.Protected,
) -> tuple[tuple[tuple[str, int], ...], dict[str, Any]]:
  """Hold every protected line and cell that has a recorded construction baseline at that baseline in
  ``trial`` while a helper runs: a line the agent set a formula on takes back the specs and
  ``formula_periods`` that ``set_formula`` displaced (``document._formula_baseline_before_agent``);
  the agent's cells (``protect.cells``) and every axis period of its lines (``protect.items``) take
  back the override and value their displacing edit recorded (``document._baseline_before_agent``,
  the one walk from a cell's current column to the state construction gave it). What has no recorded
  baseline (an added line, a judgment row, an edit that predates the records) is left as it is.
  Returns the cells held at baseline and, per line touched, the dump the helper was given — what
  ``_restore_protected`` compares against to tell a helper's change from the baseline swap."""
  axis = document.axis(trial)
  periods = sorted({*axis["historical"], *axis["projection"]})
  cells = set(protect.cells) | {(item_id, period) for item_id in protect.items for period in periods}
  held: list[tuple[str, int]] = []
  given: dict[str, Any] = {}
  for item_id in sorted(protect.items):
    item = trial._index.get(item_id)
    formula = None if item is None else document._formula_baseline_before_agent(doc, item_id)
    if item is None or formula is None:
      continue
    # a line the agent set a formula on: the helper reads the constructed specs, not the agent's
    item.historical = None if formula["historical"] is None else FormulaSpec.model_validate(formula["historical"])
    item.projected = None if formula["projected"] is None else FormulaSpec.model_validate(formula["projected"])
    item.formula_periods = None if formula["formula_periods"] is None else list(formula["formula_periods"])
    held.extend((item_id, period) for period in periods)
  for item_id, period in sorted(cells):
    item = trial._index.get(item_id)
    if item is None:
      continue
    baseline = document._baseline_before_agent(doc, (item_id, period))
    if baseline is None:
      continue
    if baseline["override"] is None:
      (item.overrides or {}).pop(period, None)
    else:
      item.overrides = {**(item.overrides or {}), period: FormulaSpec.model_validate(baseline["override"])}
    if baseline["value"] is None:
      if item.values is not None:
        item.values.values.pop(period, None)
    else:
      if item.values is None:
        item.values = ValueSeries()
      item.values.values[period] = ValueCell.model_validate(baseline["value"])
    if (item_id, period) not in held:
      held.append((item_id, period))
  for item_id in {item_id for item_id, _period in held}:
    given[item_id] = trial._index[item_id].model_dump()
  return tuple(held), given


def _restore_protected(
  trial: Any, before: Mapping[str, Any], protect: document.Protected, given: Mapping[str, Any] = {},
) -> list[dict[str, Any]]:
  """Put every protected line and cell of ``trial`` back as ``before`` held it; one ``kept`` row
  per line or cell the helper had changed — measured against what the helper was given
  (``given``, the baselined dump of a line held at baseline for the run), never against the
  agent's state the swap itself displaced."""
  kept: list[dict[str, Any]] = []

  def changed_by_helper(item_id: str, new: Any, period: int | None = None) -> bool:
    if item_id not in given:
      return True
    was = LineItem.model_validate(given[item_id])
    if period is None:
      return was.model_dump() != new.model_dump()
    return (
      _cell_state(was, period) != _cell_state(new, period)
      or (was.overrides or {}).get(period) != (new.overrides or {}).get(period)
      or _owns_formula(was, period) != _owns_formula(new, period)
    )

  for item_id in sorted(protect.items):
    old, new = before.get(item_id), trial._index.get(item_id)
    if old is None or new is None or old.model_dump() == new.model_dump():
      continue
    helper_changed = changed_by_helper(item_id, new)
    for name in type(old).model_fields:
      setattr(new, name, copy.deepcopy(getattr(old, name)))
    if helper_changed:
      kept.append({"item_id": item_id, "kept": "protected_line"})
  for item_id, period in sorted(protect.cells):
    old, new = before.get(item_id), trial._index.get(item_id)
    if old is None or new is None or (
      _cell_state(old, period) == _cell_state(new, period)
      and (old.overrides or {}).get(period) == (new.overrides or {}).get(period)
      and _owns_formula(old, period) == _owns_formula(new, period)
    ):
      continue
    helper_changed = changed_by_helper(item_id, new, period)
    _restore_period(new, old, period)
    # the cell's formula ownership is part of its state: a helper that drops the period from the line's
    # ``formula_periods`` (or adds it, including by widening the list to ``None``: every period) must
    # not change what the protected cell renders; the unprotected periods keep the helper's ownership
    if new.formula_periods is None and not _owns_formula(old, period):
      axis = document.axis(trial)
      new.formula_periods = sorted({*axis["historical"], *axis["projection"]})
    live_sheet._sync_formula_period(new, old, period)
    if helper_changed:
      kept.append({"item_id": item_id, "period": period, "kept": "protected_cell"})
  return kept


def _owns_formula(item: Any, period: int) -> bool:
  """Whether the line's formula owns ``period`` (a ``None`` list owns every period)."""
  return item.formula_periods is None or period in {int(year) for year in item.formula_periods}


def _returned_formula_owner(
  doc: document.ModelSheet, item_id: str, period: int, spec: FormulaSpec, template: FinancialModel | None,
) -> dict[str, str] | None | Literal[False]:
  """What a re-run helper returned a formula cell it had stamped to: the helper whose construction
  edit in ``doc.edits`` last recorded ``spec`` as the line's ``historical`` or ``projected`` formula
  (``field_changes``), as ``authored_by`` names it; ``None`` for the template's own formula (cold
  stamps nothing there); ``False`` for a formula of the helper's own (cold's stamp is the helper's)."""
  wanted = spec.model_dump(mode="json")
  for edit in reversed(doc.edits):
    op = edit.get("op") or {}
    if op.get("type") != "construction":
      continue
    changes = ((edit.get("result") or {}).get("field_changes") or {}).get(item_id) or {}
    if any((changes.get(side) or {}).get("after") == wanted for side in ("historical", "projected")):
      return {"helper": str(op["helper"]), "owner": str(op.get("owner"))}
  if template is None:
    return False
  pristine = template._index.get(item_id)
  if pristine is not None and document._render_spec_for_period(template, pristine, period) == spec:
    return None
  return False


def _field_json(value: Any) -> Any:
  """A line field as ``construct`` records it in ``field_changes``."""
  if isinstance(value, ItemType):
    return value.value
  if hasattr(value, "model_dump"):
    return value.model_dump(mode="json")
  return list(value) if isinstance(value, list) else value


def _formula_overrides(item: Any) -> list[list[Any]]:
  """A line's formula (non-constant) overrides as ``[period, spec]`` rows, by period: the cells a helper
  authors as formulas (the spine's first-year delta); a constant override is a cell's input, not structure."""
  return sorted(
    ([int(period), spec.model_dump(mode="json")] for period, spec in (item.overrides or {}).items() if spec.type is not FormulaType.constant),
    key=lambda row: row[0],
  )


def _line_field(item: Any, name: str) -> Any:
  """``name`` of ``item`` as ``field_changes`` compares and records it (``None`` for a line the sheet lacks)."""
  if item is None:
    return None
  return _formula_overrides(item) or None if name == "formula_overrides" else getattr(item, name, None)


def _field_from_json(name: str, value: Any) -> Any:
  """The inverse of :func:`_field_json` for the fields ``field_changes`` records."""
  if value is None:
    return [] if name == "formula_overrides" else None
  if name == "item_type":
    return ItemType(value)
  if name == "formula_periods":
    return list(value)
  if name == "formula_overrides":
    return [(int(period), FormulaSpec.model_validate(spec)) for period, spec in value]
  return FormulaSpec.model_validate(value)


def _recorded_structure_to_reset(
  model: FinancialModel, helper_name: str, edits: Sequence[Mapping[str, Any]], protected: document.Protected,
) -> dict[str, list[tuple[str, Any]]]:
  """The line fields ``helper_name``'s recorded construction edits changed and ``model`` still holds
  as that edit left them (``field_changes``: ``item_type`` / ``historical`` / ``projected`` /
  ``formula_periods``), newest edit first, with the recorded ``before`` to put back. A re-run helper
  runs over the constructed sheet, so a helper whose output depends on its inputs (the
  valuation-policy suppression) would otherwise read its own earlier decision as the template;
  putting these back gives it the sheet it first ran on, which is what a cold construction gives
  it. A field changed since (by a later helper or the agent) and a ``protected`` line stand. Only
  for a helper whose edits add or remove no lines (``Helper.resets_structure``): those cannot be put
  back. Reads only; ``construct`` applies it to its trial copy (``_reset_recorded_structure``), so
  a helper that raises leaves the document unchanged."""
  current: dict[tuple[str, str], Any] = {}
  final: dict[tuple[str, str], Any] = {}
  for edit in reversed(edits):
    op = edit.get("op") or {}
    if op.get("type") != "construction" or op.get("helper") != helper_name:
      continue
    result = edit.get("result") or {}
    for item_id, changes in (result.get("field_changes") or {}).items():
      item = model._index.get(item_id)
      if item is None or item_id in protected.items:
        continue
      for name, change in changes.items():
        key = (item_id, name)
        if key not in current:
          current[key] = _field_json(_line_field(item, name))
        if current[key] == change["after"]:
          current[key] = final[key] = change["before"]
  reset: dict[str, list[tuple[str, Any]]] = {}
  for (item_id, name), before in final.items():
    reset.setdefault(item_id, []).append((name, before))
  return reset


def _reset_recorded_structure(model: FinancialModel, reset: Mapping[str, Sequence[tuple[str, Any]]]) -> None:
  """Apply :func:`_recorded_structure_to_reset` to ``model`` (a trial copy)."""
  for item_id, fields in reset.items():
    item = model._index[item_id]
    for name, before in fields:
      if name == "formula_overrides":
        # The line's formula overrides go back to the recorded ones; its constant overrides (filed or authored cells) stand.
        item.overrides = {
          **{period: spec for period, spec in (item.overrides or {}).items() if spec.type is FormulaType.constant},
          **dict(_field_from_json(name, before)),
        } or None
      else:
        setattr(item, name, _field_from_json(name, before))


def construct(
  structure: SheetFrame,
  helper: Helper,
  call: Callable[[Any], Any],
  *,
  inputs: Mapping[str, Any],
  cell_source: Callable[[Any, str, int], Mapping[str, Any] | None] | None = None,
  reason: str | None = None,
  protect: document.Protected | None = None,
  over_baseline: bool = False,
  template: FinancialModel | None = None,
  reset_from: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
  """Run one compile piece on a copy of the document's model and keep it as a recorded
  structural edit (``doc.edits``: ``{type: construction, helper, owner}``, its source the helper
  and the inputs it read). ``call`` mutates the model it is given or returns its replacement.

  The document stays the authority over what was authored before the helper ran: a cell that
  holds an authored input (a C fact, a judgment row) keeps it, and a cell the agent judged not
  applicable keeps its absence; each kept cell is listed (``kept_authored``) with what the helper
  would have written. An input cell the helper writes gets the semantic column ``cell_source``
  names, else ``{origin: derived, source: {kind: construction_helper, helper, owner, note}}``; a
  re-run also transfers an explicitly changed ``cell_source`` for an existing input this helper
  owns, even when its stored value is unchanged; protected and authored inputs keep their source.
  A cell it now produces that was unavailable for want of a source drops that mark
  (``superseded_unavailable``). A forecast still held in an actual year (``document.PROVISIONAL``)
  is nobody's input: what the helper writes there, or the line's formula it leaves there, is the
  year's actual and carries the forecast as ``forecast_record`` (``document.supersede_forecast``).
  A raise leaves the document unchanged (``status: refused``).
  ``reason`` rides on the edit's op (``refresh`` for a re-run); a ``protect``ed line or cell keeps
  what it held before the helper ran (listed in ``kept_authored``). A re-run (``over_baseline``,
  ``rerun_derivations``' contract) runs the helper over the constructed sheet, never over the
  agent's edits: every protected cell with a recorded construction baseline
  (``document._baseline_before_agent``) holds that baseline in the trial while the helper runs
  (``_baseline_protected``, listed in ``kept_authored`` as ``baselined``), so a re-run equals a cold
  construction with the agent's ops replayed after it (HELPER-RERUN-OVER-AGENT-EDITS,
  ``docs/reference/rulings-index.md``): a disposition reads the FAS-authored value the agent later
  overrode, a schedule reads the constructed formula, not the agent's. The one helper that runs
  *because of* an agent edit — the ``document_edit`` scenario tail, which lowers the agent's input
  into the scenario tables — runs over the edited sheet.
  A re-run of a ``Helper.resets_structure`` helper (the valuation policy's suppression) likewise starts
  from the sheet it first ran on, not from the lines its own earlier decision removed or rewrote: the
  trial first gets back the line fields the helper's recorded edits in ``reset_from`` changed
  (``_recorded_structure_to_reset``), on the copy, so a raise still leaves the document unchanged and
  a run records the restoration in its own ``field_changes``.
  """
  from schema.models import FinancialModel

  doc = structure.doc
  before = {item.id: item for item in doc.model._index.values()}
  trial = document.copy_model(doc.model)
  trial.build_index()
  baselined, given = ((), {}) if protect is None or not over_baseline else _baseline_protected(doc, trial, protect)
  reset_lines: frozenset[str] = frozenset()
  if reset_from is not None and helper.resets_structure and protect is not None:
    # After the baseline swap: an agent cell holds the constructed cell it displaced (the helper's own formula
    # override among them), which is what the recorded edits' ``after`` names and the reset takes back.
    reset = _recorded_structure_to_reset(trial, helper.name, reset_from, protect)
    _reset_recorded_structure(trial, reset)
    reset_lines = frozenset(reset)
    given = {item_id: trial._index[item_id].model_dump() for item_id in given}
  op = {"type": "construction", "helper": helper.name, "owner": helper.owner, **({"reason": reason} if reason else {})}
  edit_source = {"origin": "construction_helper", "helper": helper.name, "owner": helper.owner, "inputs": dict(inputs)}
  try:
    returned = call(trial)
  except Exception as exc:  # a product raise is the helper's refusal, recorded; the document is unchanged
    result = refusal_row(exc, status="refused")
    document.record_edit(doc, op, source=edit_source, result=result)
    return {"helper": helper.name, "owner": helper.owner, **result, "inputs_written": 0}
  if isinstance(returned, FinancialModel):
    trial = returned
  trial.build_index()
  retired_rows = frozenset(returned) if helper.retires else frozenset()
  kept_protected = [] if protect is None else _restore_protected(trial, before, protect, given)
  added = sorted(set(trial._index) - set(before))
  removed = sorted(set(before) - set(trial._index))
  formulas: list[str] = []
  changed: list[tuple[str, int]] = []
  for item_id, item in trial._index.items():
    old = before.get(item_id)
    if old is not None and _structure_of(old) != _structure_of(item):
      formulas.append(item_id)
    periods = set((item.overrides or {})) | set(() if item.values is None else item.values.values)
    if old is not None:
      periods |= set(old.overrides or {}) | set(() if old.values is None else old.values.values)
    changed.extend((item_id, int(period)) for period in periods if _cell_state(old, period) != _cell_state(item, period))
  kept: list[dict[str, Any]] = []
  restored: set[tuple[str, int]] = set()
  superseded: list[dict[str, Any]] = []
  for (item_id, period), column in list(doc.semantic.items()):
    item = trial._index.get(item_id)
    old = before.get(item_id)
    if item is None or old is None:
      continue
    if column.get("cell") == "input" and _cell_state(old, period) != _cell_state(item, period):
      if "constructed_by" in column or document.PROVISIONAL in column or (column.get("source") or {}).get("kind") == "construction_helper":
        # An earlier helper's input (the FAS lowering, a schedule fallback, the structure's house
        # default such as the all-to-goodwill allocation proposal): compile's later passes re-lower
        # it, and so do they here. A provisional projection is nobody's input.
        continue
      if item_id in retired_rows:
        # The helper retired this row (``DependencyGraph.retire``): no cell is left to hold the input.
        continue
      override, value = _cell_state(item, period)
      wrote = override.params.get("value") if override is not None else (None if value is None else value.value)
      held_override, held_value = _cell_state(old, period)
      held = held_override.params.get("value") if held_override is not None else (None if held_value is None else held_value.value)
      _restore_period(item, old, period)
      restored.add((item_id, period))
      if wrote is None or held is None or not math.isclose(float(wrote), float(held), rel_tol=1e-9, abs_tol=1e-9):
        kept.append({"item_id": item_id, "period": period, "kept": "authored_input", "held": held, "helper_value": wrote})
      continue
    if column.get("cell") == "not_separately_presented":
      if _cell_state(old, period) != _cell_state(item, period):
        # C's cited inclusion owns its leaf: no helper constant or formula replaces the marker.
        wrote = _cell_state(item, period)[0]
        _restore_period(item, old, period)
        restored.add((item_id, period))
        kept.append({
          "item_id": item_id, "period": period, "kept": "not_separately_presented",
          "helper_value": None if wrote is None else wrote.params.get("value"),
        })
      continue
    if column.get("cell") != "unavailable":
      continue
    old_spec = document._render_spec_for_period(trial, old, period)
    new_spec = document._render_spec_for_period(trial, item, period)
    formula_now = new_spec is not None and new_spec.type is not FormulaType.constant and new_spec != old_spec
    if not formula_now and _cell_state(item, period) == (None, None):
      continue
    if column.get("reason") in _AUTHORED_ABSENCE:
      # The agent's evidenced absence is the role's: a constant the helper writes there is
      # dropped; a formula the document now owns takes the cell, as it would have had it
      # existed when C was written (``formula_owned_source_absent``).
      wrote = _cell_state(item, period)[0]
      _restore_period(item, old, period)
      if not formula_now:
        restored.add((item_id, period))
        kept.append({
          "item_id": item_id, "period": period, "kept": column.get("reason"),
          "helper_value": None if wrote is None else wrote.params.get("value"),
        })
        continue
    doc.semantic.pop((item_id, period))
    superseded.append({
      "item_id": item_id, "period": period, "reason": column.get("reason"),
      **({"formula": document.formula_text(new_spec), "finding": "formula_owned_source_absent", "column": column}
         if formula_now and new_spec is not None else {}),
    })
  written = 0
  for item_id, period in changed:
    if (item_id, period) in restored:
      continue
    override, value = _cell_state(trial._index.get(item_id), period)
    held = doc.semantic.get((item_id, period)) or {}
    if override is None and value is None:
      if document.PROVISIONAL in held:
        # The helper leaves an actual year's forecast to the line's own formula: the actual wins.
        line = trial._index.get(item_id)
        spec = None if line is None else document._render_spec_for_period(trial, line, period)
        if spec is not None and spec.type is not FormulaType.constant:
          doc.semantic[(item_id, period)] = stamp_cell_source(document.supersede_forecast(
            doc, (item_id, period), document.formula_column(spec),
          ))
        else:
          doc.semantic.pop((item_id, period))
      elif held.get("cell") == "input":
        doc.semantic.pop((item_id, period))
      continue
    note = override.note if override is not None else value.note
    column = None if cell_source is None else cell_source(returned, item_id, period)
    # Construction re-executes an owner; an analyst-edit tail deliberately lowers its snapshots.
    if (
      column is None and held.get("cell") == "input" and held.get("constructed_by") is not None
      and (held.get("constructed_by") != helper.name or reason == "document_edit")
    ):
      column = {
        **{key: entry for key, entry in held.items() if key not in (document.PROVISIONAL, document.FORECAST_RECORD)},
        "lowered_by": [*(name for name in held.get("lowered_by", ()) if name != helper.name), helper.name],
      }
    # A helper's input over an actual year's forecast supersedes it (``supersede_forecast``).
    doc.semantic[(item_id, period)] = stamp_cell_source(document.supersede_forecast(doc, (item_id, period), {
      "cell": "input", "constructed_by": helper.name, **dict(column or {
        "origin": "derived",
        "source": {"kind": "construction_helper", "helper": helper.name, "owner": helper.owner, "note": note},
      }),
    }))
    written += 1
  anchors = document.scalar_anchors(trial)
  for item_id in sorted(set(added) | set(formulas)):
    if item_id not in trial._index or (protect is not None and item_id in protect.items):
      continue
    item = trial._index[item_id]
    old = before.get(item_id)
    anchor = anchors.get(item_id)
    periods = (
      [] if anchor is None else [anchor]
    ) if item.column is not None else [*structure.historical_years, *structure.forecast_years]
    for period in periods:
      if (item_id, period) in restored or (protect is not None and (item_id, period) in protect.cells):
        continue
      spec = document._render_spec_for_period(trial, item, period)
      state = _cell_state(item, period)
      old_spec = None if old is None else document._render_spec_for_period(trial, old, period)
      if (
        spec is not None and spec.type is not FormulaType.constant and state == (None, None)
      ):
        if old is None or spec != old_spec:
          held_col = doc.semantic.get((item_id, period)) or {}
          # A re-run returning a formula cell it had stamped — or a line it reset from its own recorded
          # edits, whose suppressed cell held no column — to an earlier recorded formula or the template's.
          returned_to = (
            _returned_formula_owner(doc, item_id, period, spec, template)
            if reason is not None and (
              (held_col.get("cell") == "formula" and held_col.get("constructed_by") == helper.name) or item_id in reset_lines
            )
            else False
          )
          if returned_to is None:
            # back to the template's formula: cold stamped nothing there
            doc.semantic.pop((item_id, period), None)
          elif returned_to:
            # back to an earlier helper's recorded formula: cold's stamp is that helper's
            doc.semantic[(item_id, period)] = stamp_cell_source(document.supersede_forecast(doc, (item_id, period), {
              **document.formula_column(spec), "constructed_by": returned_to["helper"], "authored_by": returned_to,
            }))
          elif held_col.get("cell") not in document.STAMPED_CELL_KINDS:
            cb_col = None if cell_source is None else cell_source(returned, item_id, period)
            doc.semantic[(item_id, period)] = stamp_cell_source(document.supersede_forecast(doc, (item_id, period), {
              **document.formula_column(spec),
              "constructed_by": helper.name,
              "authored_by": {"helper": helper.name, "owner": helper.owner},
              **dict(cb_col or {}),
            }))
      elif spec is None and state == (None, None):
        if (doc.semantic.get((item_id, period)) or {}).get("cell") == "formula":
          doc.semantic.pop((item_id, period), None)
  if cell_source is not None:
    for (item_id, period), held in list(doc.semantic.items()):
      if held.get("cell") != "input" or held.get("constructed_by") != helper.name:
        continue
      if (item_id, period) in restored or (
        protect is not None and (item_id in protect.items or (item_id, period) in protect.cells)
      ):
        continue
      old, item = before.get(item_id), trial._index.get(item_id)
      if old is None or item is None:
        continue
      state = _cell_state(item, period)
      if state == (None, None) or _cell_state(old, period) != state:
        continue
      column = cell_source(returned, item_id, period)
      if column is None or not any(key not in held or held[key] != entry for key, entry in column.items()):
        continue
      # Only the callback's explicit fields change; value and ownership stay where they were.
      doc.semantic[(item_id, period)] = stamp_cell_source(held, **dict(column))
  doc.model = trial
  # A line the helper removed takes its cells' semantic columns with it (``document.remove_line``'s contract).
  gone = frozenset(removed)
  for key in [key for key in doc.semantic if key[0] in gone]:
    del doc.semantic[key]
  touched = {(item_id, int(period)) for item_id, period in changed if (item_id, period) not in restored}
  for item_id in formulas:
    item = trial._index.get(item_id)
    old = before.get(item_id)
    if item is None or (protect is not None and item_id in protect.items):
      continue
    anchor = anchors.get(item_id)
    periods = ([anchor] if anchor is not None else []) if item.column is not None else [
      *structure.historical_years, *structure.forecast_years,
    ]
    for period in periods:
      if protect is not None and (item_id, int(period)) in protect.cells:
        continue
      new_spec = document._render_spec_for_period(trial, item, int(period))
      old_spec = None if old is None else document._render_spec_for_period(trial, old, int(period))
      if new_spec != old_spec:
        touched.add((item_id, int(period)))
  field_changes: dict[str, dict[str, Any]] = {}

  for item_id in set(formulas) | set(added) | set(removed):
    old, item = before.get(item_id), trial._index.get(item_id)
    changes: dict[str, Any] = {}
    for name in ("item_type", "historical", "projected", "formula_periods", "formula_overrides"):
      previous, current = _line_field(old, name), _line_field(item, name)
      if previous == current:
        continue
      changes[name] = {"before": _field_json(previous), "after": _field_json(current)}
    if changes:
      field_changes[item_id] = changes
  result = {
    "status": "ok",
    "items_added": added,
    "items_removed": removed,
    "formulas": sorted(formulas),
    "inputs_written": written,
    "kept_authored": kept_protected + kept,
    **({"baselined": len(baselined)} if baselined else {}),
    "superseded_unavailable": superseded,
    "cells": [[item_id, period] for item_id, period in sorted(touched)],
    # A helper also depends on lines where the document kept the authored cell it tried to change.
    "kept_cells": [[item_id, period] for item_id, period in sorted(restored)],
    "field_changes": field_changes,
  }
  document.record_edit(doc, op, source=edit_source, result=result)
  return {"helper": helper.name, "owner": helper.owner, **result, "_returned": returned}


@dataclass(frozen=True)
class EditSegment:
  """One cold-order construction boundary, including boundaries that append no edits."""

  name: str
  edits: tuple[dict[str, Any], ...]
  #: Observed helper ownership survives an idempotent execution that changes no cells.
  items: frozenset[str] = frozenset()


class _EditSegments:
  def __init__(self, doc: document.ModelSheet, parent: Sequence[EditSegment] = ()) -> None:
    self.doc = doc
    self.parent = {segment.name: segment for segment in parent}
    self.segments: list[EditSegment] = []
    self.last_edits: tuple[dict[str, Any], ...] = ()

  def capture(self, name: str, start: int) -> tuple[dict[str, Any], ...]:
    from model_build import live_sheet

    edits = tuple(self.doc.edits[start:])
    self.last_edits = edits
    # Writer operations and construction edits have different result.cells schemas.
    items = {item_id for item_id, _period in live_sheet.writer_footprint(self.doc, edits)}
    for edit in edits:
      if (edit.get("op") or {}).get("type") != "construction":
        continue
      result = edit.get("result") or {}
      items.update(str(pair[0]) for pair in result.get("kept_cells") or ())
      items.update(str(item_id) for item_id in result.get("field_changes") or ())
      items.update(str(item_id) for item_id in result.get("formulas") or ())
      items.update(str(item_id) for item_id in result.get("items_added") or ())
      items.update(str(item_id) for item_id in result.get("items_removed") or ())
      items.update(str(row["item_id"]) for row in result.get("kept_authored") or () if "item_id" in row)
    previous = self.parent.get(name)
    if name.startswith("helper:") and previous is not None:
      items.update(previous.items)
    if self.segments and self.segments[-1].name == name:
      # a writer re-run after its kept segment: one segment, which the next refresh reads whole
      last = self.segments.pop()
      combined = (*last.edits, *edits)
      nsp = [edit for edit in combined if (edit.get("op") or {}).get("type") == "not_separately_presented_lowering"]
      if len(nsp) > 1:
        cells = sorted(
          {
            (str(row["item_id"]), int(row["period"])): dict(row)
            for edit in nsp
            for row in (edit.get("result") or {}).get("cells") or ()
          }.values(),
          key=lambda row: (str(row["item_id"]), int(row["period"])),
        )
        merged_nsp = {**nsp[0], "result": {"status": "ok", "cells": cells}}
        combined = (
          *(edit for edit in combined if (edit.get("op") or {}).get("type") != "not_separately_presented_lowering"),
          merged_nsp,
        )
        self.doc.edits[len(self.doc.edits) - len(last.edits) - len(edits) :] = combined
      edits, items = combined, items | last.items
    self.segments.append(EditSegment(name, edits, frozenset(items)))
    return self.last_edits

  def keep(self, name: str) -> None:
    segment = self.parent.get(name, EditSegment(name, ()))
    self.doc.edits.extend(segment.edits)
    self.segments.append(segment)

  def run(self, name: str, call: Callable[[], Any]) -> Any:
    start = len(self.doc.edits)
    result = call()
    self.capture(name, start)
    return result


@dataclass
class Construction:
  """What ``construct_document`` ran: one row per helper (``status`` ok / refused / missing), the
  recalc after each (``trace``, for the completeness classification; ``traced=False`` skips those
  recalcs, for a caller that reads no completeness report), historical
  refills (the C writer over the roles a helper gave a line), and input rows retired at their owner."""

  rows: list[dict[str, Any]] = field(default_factory=list)
  trace: list[tuple[str, document.Values]] = field(default_factory=list)
  refills: list[dict[str, Any]] = field(default_factory=list)
  retired: list[str] = field(default_factory=list)
  #: Compiled lines of the BusinessModel nodes whose history is unresolved (their finding is the record).
  unresolved_nodes: list[str] = field(default_factory=list)
  scenario_controls: dict[str, Any] | None = None
  #: The resolved disposition table and role lookup the schedule policies ran on (None: they did not run).
  role_dispositions: Any = None
  role_items: Any = None
  traced: bool = True
  edit_segments: tuple[EditSegment, ...] = ()
  helper_order: list[str] = field(default_factory=list)
  helper_findings: dict[str, list[dict[str, Any]]] = field(default_factory=dict)

  def record(self, name: str, doc: document.ModelSheet) -> None:
    """The recalc after helper ``name`` (``trace``), when this construction is traced."""
    if self.traced:
      self.trace.append((name, document.recalc(doc)))

  def missing(self) -> list[str]:
    return [row["helper"] for row in self.rows if row["status"] != "ok"]

  def lined_roles(self) -> set[str]:
    return {role for refill in self.refills for role, items in refill["targets"].items() if items}


def _market_inputs(bundle: Any) -> Any:
  from schema.valuation_evidence_derivations import derive_covered_market_inputs

  return derive_covered_market_inputs(
    bundle.valuation_observations, bundle.cutoff_at, bundle.market_calendar, bundle.fiscal_calendar,
    requirements=bundle.valuation_requirements, focal_ticker=bundle.coverage_profile.focal_ticker,
    sources=bundle.sources, retrieved_at=bundle.retrieved_at,
  )


def _write_scenario_outputs(
  model: Any, spec: Any, basis: str | None,
  *, clear_cells: Collection[tuple[str, int]] = (),
) -> None:
  from schema.build_scenarios import (
    _populate_scenario_eps, _populate_thesis_snapshot, compute_scenario_outputs,
  )
  from schema.scenario_bindings import scenario_case_gaps

  # A previously computed snapshot cannot survive a dependency becoming missing.
  # Authored cells are restored by the construction writer's protection policy.
  for item_id, period in clear_cells:
    item = model._index.get(item_id)
    if item is None:
      continue
    if item.values is not None:
      item.values.values.pop(period, None)
    override = (item.overrides or {}).get(period)
    if override is not None and override.type is FormulaType.constant:
      item.overrides.pop(period)
  values, item_ids = compute_scenario_outputs(
    model, unresolved_cases=frozenset(gap.scenario for gap in scenario_case_gaps(spec)),
  )
  if basis is not None:
    _populate_scenario_eps(
      model, {case: dict(outputs.get(basis, {})) for case, outputs in values.items()},
      {case: ids[basis] for case, ids in item_ids.items() if basis in ids},
      control_ids=values.control_ids,
    )
  _populate_thesis_snapshot(model, values, item_ids, control_ids=values.control_ids)


def release_refresh_record(doc: document.ModelSheet) -> tuple[int, dict[str, Any]] | None:
  """Take the previous batch's scenario-outputs refresh off the end of ``doc.edits``, as ``(position,
  record)`` (``None``: the last edit is not one, or ``doc`` has no ModelSpec to refresh with).

  The refresh re-derives the whole snapshot, so the batch that follows replaces its record rather than
  adding to it: the edit log a sheet that took its batches one by one holds is the one a single
  replay of the same ops leaves, and an op's ``edit_index`` is the position it has there."""
  if doc.model_spec is None or not doc.edits:
    return None
  op = doc.edits[-1].get("op") or {}
  if op.get("helper") != "scenario_outputs" or op.get("reason") != "document_edit":
    return None
  return len(doc.edits) - 1, doc.edits.pop()


def refresh_scenario_outputs(doc: document.ModelSheet) -> None:
  """Refresh construction-owned scenario snapshots after one authored edit batch.

  The original helper's inputs own the EPS publication basis. Legacy documents
  without that declaration refresh thesis fields only, never guess an EPS basis.
  """
  if doc.model_spec is None:
    return
  previous = next((
    edit for edit in reversed(doc.edits)
    if (edit.get("op") or {}).get("helper") == "scenario_outputs"
    and (edit.get("result") or {}).get("status") == "ok"
  ), None)
  if previous is None:
    return
  inputs = dict((previous.get("source") or {}).get("inputs") or {})
  clear_cells = {
    key for key, column in doc.semantic.items()
    if column.get("constructed_by") == "scenario_outputs"
  }
  structure = _document_structure(doc)
  construct(
    structure,
    Helper("scenario_outputs", "schema/build_scenarios.py:compute_scenario_outputs", ("model_spec",)),
    lambda model: _write_scenario_outputs(
      model, doc.model_spec, inputs.get("eps_publication_basis"), clear_cells=clear_cells,
    ),
    inputs=inputs, reason="document_edit", protect=document._protected(doc),
  )


def replay_tail(doc: document.ModelSheet, rerun: "Construction") -> None:
  """The end of a replay, after an offline re-run (``target_refresh`` / ``target_advance``): a
  re-run helper ran over the construction baselines (HELPER-RERUN-OVER-AGENT-EDITS), so where the
  document holds agent ops the scenario tail that every ``apply_ops`` batch ends with
  (``refresh_scenario_outputs``) runs once more, as cold-then-replay would have run it — the
  scenario outputs read the agent's inputs again. Nothing re-ran, or no agent op: nothing to do."""
  if not any("agent" in edit for edit in doc.edits):
    return
  if not any(row.get("status") == "ok" for row in rerun.rows):
    return
  refresh_scenario_outputs(doc)


#: The construction helpers that derive values from the document's facts and the judgment inputs
#: (schedules, valuation inputs, scenario outputs, control curves, BusinessModel node history,
#: HFF facts): ``target_refresh`` re-runs them, ``target_advance`` all of them (the axis moved). The
#: rest author structure (the roll-forward and spine normalizations, formula-first,
#: bridges, share conversion, provisional anchors, presentation), seed projection-year inputs from
#: the latest actual year (``PROJECTION_SEED_HELPERS``) or lower judgment (the FAS: run once,
#: re-lowered by an advance onto the moved projection window).
DERIVATION_HELPERS = frozenset({
  "statement_residual_histories", "business_model_node_history", "scenario_targets", "financial_schedules", "working_capital_schedule",
  "historical_foundation_facts", "three_statement_spine", "tax_schedule", "valuation_inputs", "company_grounded_schedules",
  "control_curves", "scenario_outputs",
})
#: The retirement that follows the valuation inputs (``retire_unused_cost_of_debt``): not a
#: derivation itself, it re-runs after any derivation that did.
VALUATION_RETIREMENT_HELPERS = frozenset({"retire_unused_cost_of_debt"})
#: The helpers that seed every projection year's inputs from the latest actual year (the non-GAAP
#: add-backs, the inventory policy): run once by a build, re-run by ``target_advance``, whose moved
#: boundary changes both the latest actual year and the projection years they seed.
PROJECTION_SEED_HELPERS = frozenset({"non_gaap_addbacks", "inventory_policy"})


@dataclass(frozen=True)
class Rerun:
  """A refresh's construction pass: ``select(helper, recorded_inputs)`` names why a runnable
  helper re-runs (``None``: it does not); each re-run edit carries ``reason``; ``protect`` is
  never changed. No historical refill runs (the refresh's C pass already wrote C), except a ``correspondence``
  refresh's, over the moved roles C has no pristine line for. ``template``
  is the structure as authored before construction (``scratch_structure``): the scenario-table
  ownership the scenario targets partition reads, which the control curves then replace in the
  document."""

  select: Callable[[Helper, Mapping[str, Any]], str | None]
  protect: document.Protected
  reason: str = "refresh"
  template: Any = None
  #: Resolved schedule dispositions survive a pass that does not re-run those schedules.
  role_dispositions: Any = None
  role_items: Any = None
  stages: Collection[str] = ()
  segments: _EditSegments | None = None
  helper_findings: Mapping[str, Sequence[dict[str, Any]]] = field(default_factory=dict)
  prepare: Callable[[Helper], None] | None = None
  changed: Callable[[Mapping[str, Any]], None] | None = None
  #: The recorded edits a ``Helper.resets_structure`` helper's trial is first reset from (``construct``).
  reset_from: Sequence[Mapping[str, Any]] | None = None


def run_step(
  out: Construction,
  structure: SheetFrame,
  helper: Helper,
  call: Callable[[Any], Any],
  *,
  recorded: Mapping[str, Any],
  available: Mapping[str, bool],
  cell_source: Callable[[Any, str, int], Mapping[str, Any] | None] | None = None,
  rerun: Rerun | None = None,
  segments: _EditSegments | None = None,
) -> Any:
  """One helper through ``construct``, its row in ``out``; ``missing`` (with what it needs) when
  an input it cannot run without is absent. Under ``rerun`` only a runnable helper ``select``
  names runs, its row carrying ``rerun: <why>``."""
  absent = [need for need in helper.needs if not available[need]]
  if rerun is not None and absent and (not rerun.stages or segments is None):
    return None
  out.helper_order.append(helper.name)
  why = None
  if rerun is not None and not absent:
    why = rerun.select(helper, recorded)
    if why is None:
      if segments is not None:
        segments.keep(f"helper:{helper.name}")
      return None
    if rerun.prepare is not None:
      rerun.prepare(helper)
  start = len(structure.doc.edits)
  if absent:
    out.rows.append({"helper": helper.name, "owner": helper.owner, "status": "missing", "needs": absent, "inputs_written": 0})
    if segments is not None:
      segments.capture(f"helper:{helper.name}", start)
    return None
  row = construct(
    structure, helper, call, inputs=recorded, cell_source=cell_source,
    reason=None if rerun is None else rerun.reason, protect=None if rerun is None else rerun.protect,
    over_baseline=rerun is not None, template=None if rerun is None else rerun.template,
    reset_from=None if rerun is None else rerun.reset_from,
  )
  returned = row.pop("_returned", None)
  if why is not None:
    row["rerun"] = why
  out.rows.append(row)
  if rerun is not None and rerun.changed is not None:
    rerun.changed(row)
  if segments is not None:
    segments.capture(f"helper:{helper.name}", start)
  out.record(helper.name, structure.doc)
  return returned


def scenario_publication_controls_payload(
  curves: Sequence[Any], bound: Sequence[str],
) -> dict[str, Any]:
  """The exact construction-owned controls persisted beside the document baseline."""
  return {
    "control_curves": [curve.to_dict() for curve in curves],
    "bound_scenario_control_ids": list(bound),
  }


def scenario_publication_controls_from_payload(
  payload: Mapping[str, Any],
) -> tuple[tuple[Any, ...], tuple[str, ...]]:
  from schema.control_snapshot_projection import CanonicalProjectionCurve

  curves = []
  for row in payload["control_curves"]:
    fields = dict(row)
    fields["values"] = tuple((cell["fiscal_year"], cell["value"]) for cell in row["values"])
    fields["extension_basis_by_period"] = tuple(
      (cell["fiscal_year"], cell["basis"]) for cell in row["extension_basis_by_period"]
    )
    curves.append(CanonicalProjectionCurve(**fields))
  return tuple(curves), tuple(payload["bound_scenario_control_ids"])


def opening_input_source(returned: Any, item_id: str, period: int) -> Mapping[str, Any] | None:
  """``cell_source`` of a helper whose formula cell reads the pin's opening balance as an operand:
  the column names that evidence as an input (``opening_inputs``, which ``publish._document_seed_rows``
  turns into a ``source_bundle_opening_balance`` manifest row), so the cell's lineage carries it as a
  derived input, never as the cell's own filing-line observation."""
  coverage = returned.get("source_fact_coverage", ()) if isinstance(returned, Mapping) else ()
  inputs = [
    {key: row[key] for key in ("requirement_id", "evidence_id", "source_id")}
    for row in coverage
    if row.get("source_kind") == "source_bundle_opening_balance"
    and row.get("resolved_item_id") == item_id
    and [int(candidate) for candidate in row.get("periods", ())] == [period]
  ]
  return {"opening_inputs": inputs} if inputs else None


def construct_document(
  structure: SheetFrame,
  inputs: StageBuildInputs,
  ticker: str,
  *,
  forecast: Mapping[str, Any],
  unlined_roles: Collection[str],
  rerun: Rerun | None = None,
  trace: bool = True,
  segments: _EditSegments | None = None,
) -> Construction:
  """Compile's population, schedule, valuation and scenario pieces applied through
  ``construct`` after ``write_forecast_cells``. Schedule inclusion rewiring and C refills
  precede the disclosed-interest bridge, which valuation population then reads.

  Not run here, because the document already owns them: historical statement cells
  (``populate_historicals`` / ``apply_required_line_identity`` — C writes them; after the
  client-funds subtotal bridge, and again after company-grounded schedules, the C writer
  refills the roles whose line a helper created or made a value row), historical
  driver seeds, valuation controls and case probabilities (the judgment writers). A helper whose
  inputs are absent is recorded ``missing`` with what it needs. With ``rerun`` (a refresh), the
  same sequence runs only the helpers it selects, and no refill (a ``correspondence`` refresh refills
  ``unlined_roles``, the moved roles C has no pristine line for).
  """
  from schema.build import _merge_model_spec_valuation_controls
  from schema.build_bm_node_history import apply_business_model_node_history, opening_balances
  from schema.build_cash_historicals import (
    _normalize_projected_balance_sheet_roll_forward,
    _normalize_projected_cash_roll_forward,
    _normalize_three_statement_spine,
  )
  from schema.build_company_grounded_schedules import (
    apply_company_grounded_schedule_policies,
  )
  from schema.build_financial_schedules import _complete_financial_schedules
  from schema.build_formula_first import apply_formula_first
  from schema.build_historical_foundation_facts import apply_model_spec_historical_foundation_facts
  from schema.build_non_gaap_addbacks import _seed_projected_non_gaap_addbacks
  from schema.build_presentation_residue import _resolve_presentation_template_residue
  from schema.build_projection_seeds import compile_candidate_model
  from schema.build_scenarios import (
    _finalize_authenticated_dependents,
    _materialize_declared_share_conversion,
    _populate_scenario_inputs,
  )
  from schema.build_semantic_rows_balance_bridge import _materialize_balance_bridges
  from schema.build_semantic_rows_tax import _materialize_tax_schedule
  from schema.build_types import SemanticRowsResult
  from schema.build_valuation_inputs import populate_valuation_inputs, suppress_unauthorized_relative_valuation_outputs
  from schema.build_working_capital_policy import _apply_projected_inventory_policy, _complete_working_capital_schedule
  from schema.model_kind_dispatch import (
    declared_capabilities,
    declared_eps_publication_basis,
    declared_grounded_semantic_rows,
    declared_provisional_valuation_defaults,
    declared_share_conversion,
  )
  from schema.models import FormulaSpec
  from schema.required_set import derive_required_set
  from schema.scenario_bindings import (
    lower_model_spec_direct_scenario_inputs,
    lower_model_spec_scenario_curves,
    partition_model_spec_scenario_workbook_targets,
  )
  from schema.templates.template_builder import _install_residual_histories
  from schema import helper_read_slices as read_slices

  out = Construction(traced=trace)
  segments = segments if rerun is None else rerun.segments
  segment_start = 0 if segments is None else len(segments.segments)
  # A helper whose ``needs`` names an absent input never runs (``step``), so inside a step these
  # hold the assembled ModelSpec and the persisted bundle.
  spec: Any = inputs.model_spec
  bundle: Any = inputs.bundle
  registry = structure.registry
  capabilities = declared_capabilities(SimpleNamespace(coverage_profile=inputs.profile))
  share_conversion = declared_share_conversion(capabilities)
  grounded_rows = declared_grounded_semantic_rows(capabilities)
  historical = [int(period) for period in structure.historical_years]
  available = {"model_spec": inputs.model_spec is not None, "bundle": inputs.bundle is not None}
  spec_ref = None if inputs.model_spec is None else {
    "ticker": inputs.model_spec.ticker, "source_bundle_sha256": inputs.model_spec.source_bundle_sha256,
    "content_sha256": hashlib.sha256(inputs.model_spec.model_dump_json().encode()).hexdigest(),
  }
  bundle_ref = None if inputs.bundle is None else {"bundle_id": inputs.bundle_id}
  context: dict[str, Any] = {}
  if forecast.get("construction") is not None and rerun is None:
    out.rows.append(forecast["construction"])
    out.record("forecast_assumptions", structure.doc)

  def step(
    helper: Helper,
    call: Callable[[Any], Any],
    cell_source: Callable[[Any, str, int], Mapping[str, Any] | None] | None = None,
    **read: Any,
  ) -> Any:
    # ``model_spec`` and ``source_bundle`` are provenance (the ModelSpec's sha, the bundle's id): they
    # move with every bundle prepared again and are never compared (``PROVENANCE_INPUTS``); what a
    # derivation helper read is its ``read_slice`` (``schema/helper_read_slices.py``).
    recorded = {
      **({"model_spec": spec_ref} if "model_spec" in helper.needs else {}),
      **({"source_bundle": bundle_ref} if "bundle" in helper.needs else {}),
      **read,
    }
    start = len(inputs.findings)
    rows_before = len(out.rows)
    result = run_step(
      out, structure, helper, call, recorded=recorded, available=available,
      cell_source=cell_source, rerun=rerun, segments=segments,
    )
    if rerun is not None and len(out.rows) == rows_before:
      inputs.findings.extend(rerun.helper_findings.get(helper.name, ()))
    out.helper_findings[helper.name] = list(inputs.findings[start:])
    return result

  def witness(model: Any) -> Any:
    # The registry's model witness follows the model it is applied to (compile re-witnesses
    # the same way after its window build plan replaces the model).
    registry.validation_model = model
    return registry

  opening = None if inputs.bundle is None else _captured(inputs, "opening_balances", lambda: opening_balances(
    bundle, first_historical_year=historical[0],
  ))
  step(
    Helper("statement_residual_histories", "schema/templates/template_builder.py:_install_residual_histories", ("bundle",)),
    _install_residual_histories, read_slice=read_slices.statement_residual_histories(),
  )
  node_history = step(
    Helper("business_model_node_history", "schema/build_bm_node_history.py:apply_business_model_node_history", ("bundle",)),
    lambda model: apply_business_model_node_history(
      model, bundle, business_model_revision=inputs.revision, compiled_registry=witness(model),
      required_set=derive_required_set(bundle.requirements, inputs.revision, tuple(f"FY{period}" for period in historical)),
      opening_balances=opening or {},
    ),
    read_slice=read_slices.business_model_node_history(bundle),
  )
  if node_history is not None:
    start = len(inputs.findings)
    inputs.findings.extend(
      {
        **finding.model_dump(mode="json"),
        "stage": "business_model_node_history",
        "owner_file": "schema/build_bm_node_history.py",
      }
      for finding in node_history.findings
    )
    out.helper_findings["business_model_node_history"].extend(inputs.findings[start:])
    out.unresolved_nodes.extend(node_history.unresolved_items)

  def scenario_targets(model: Any) -> None:
    # A re-run partitions against the pre-construction structure: in the document the control
    # curves already own the scenario tables (``Rerun.template``).
    owners = model if rerun is None or rerun.template is None else rerun.template
    canonical, direct = partition_model_spec_scenario_workbook_targets(spec, model=owners, compiled_registry=registry)
    context["curves"] = lower_model_spec_scenario_curves(spec, workbook_targets=canonical, allow_direct_targets=True)
    context["scenario_inputs"] = lower_model_spec_direct_scenario_inputs(spec, direct_targets=direct)
    curves = tuple(context["curves"])
    bound = tuple(sorted(set(direct) | {curve.control_id for curve in curves if curve.domain == "scenario"}))
    out.scenario_controls = scenario_publication_controls_payload(curves, bound)

  step(
    Helper("scenario_targets", "schema/scenario_bindings.py:partition_model_spec_scenario_workbook_targets", ("model_spec",)),
    scenario_targets, read_slice=read_slices.scenario_targets(spec),
  )

  def curves_of(model: Any) -> Any:
    # A refresh that re-runs control curves or scenario outputs without re-running the targets
    # (their inputs unchanged) lowers them again on the document, writing nothing.
    if rerun is not None and "curves" not in context:
      scenario_targets(model)
    return context.get("curves", ())

  step(Helper("cash_roll_forward", "schema/build_cash_historicals.py:_normalize_projected_cash_roll_forward"), _normalize_projected_cash_roll_forward)
  step(Helper("balance_sheet_roll_forward", "schema/build_cash_historicals.py:_normalize_projected_balance_sheet_roll_forward"), _normalize_projected_balance_sheet_roll_forward)
  step(
    Helper("three_statement_spine", "schema/build_cash_historicals.py:_normalize_three_statement_spine", resets_structure=True),
    lambda model: _normalize_three_statement_spine(model, opening_balances=opening or {}),
    opening_input_source,
    read_slice=read_slices.three_statement_spine(bundle, opening=opening),
  )
  step(Helper("financial_schedules", "schema/build_financial_schedules.py:_complete_financial_schedules"), _complete_financial_schedules)
  step(Helper("working_capital_schedule", "schema/build_working_capital_policy.py:_complete_working_capital_schedule"), _complete_working_capital_schedule)

  def hff_fact_source(_returned: Any, item_id: str, period: int) -> dict[str, Any] | None:
    """An HFF fact cell is the filing fact it projects: its ``FilingLineRef`` key, via the HFF."""
    from schema.required_role_catalog import load_required_role_catalog

    catalog = load_required_role_catalog()
    prefix = f"{spec.ticker.lower()}.observation."
    for observation in spec.historical_foundation_fact_projection.observations:
      role = observation.semantic_id.removeprefix(prefix)
      if int(observation.period_id.removeprefix("FY")) == period and item_id in catalog.hff_fact_item_ids(role):
        line = observation.fact.line
        return {"origin": "observed", "source": {
          "kind": "filing_line", "ref": keyed_dump.filing_line_json(line), "observation_id": projection.filing_line_observation_id(line),
          "fact_id": observation.fact.fact_id,
          "via": {"artifact": "historical_foundation", "ref": _ref_json(inputs.hff), "required_role": role},
        }}
    return None

  step(
    Helper("historical_foundation_facts", "schema/build_historical_foundation_facts.py:apply_model_spec_historical_foundation_facts", ("model_spec",)),
    lambda model: apply_model_spec_historical_foundation_facts(model, spec),
    hff_fact_source,
    historical_foundation=_ref_json(inputs.hff),
    read_slice=read_slices.historical_foundation_facts(spec),
  )
  step(Helper("tax_schedule", "schema/build_semantic_rows_tax.py:_materialize_tax_schedule"), _materialize_tax_schedule)
  step(
    Helper("formula_first", "schema/build_formula_first.py:apply_formula_first"),
    lambda model: apply_formula_first(model, historical, findings=[]),
  )
  step(
    Helper("balance_subtotal_bridges", "schema/build_semantic_rows_balance_bridge.py:_materialize_balance_bridges"),
    lambda model: _materialize_balance_bridges(
      model, ticker, historical, SemanticRowsResult(), business_model=inputs.revision.business_model,
    ),
  )
  # A correspondence refresh refills only the moved roles C has no pristine line for, over the refill
  # segment the parent kept (its other roles' cells stand).
  scoped = rerun is not None and "correspondence" in rerun.stages

  def refill(roles: Collection[str], name: str) -> None:
    from model_build import live_sheet

    if scoped and segments is not None:
      segments.keep(name)
    start = len(structure.doc.edits)
    out.refills.append(write_historical_cells(structure, inputs, ticker, roles=roles))
    out.record("historical_refill", structure.doc)
    appended = segments.capture(name, start) if segments is not None else tuple(structure.doc.edits[start:])
    if rerun is not None and rerun.changed is not None:
      rerun.changed({"cells": sorted(live_sheet.writer_footprint(structure.doc, appended))})

  if rerun is None or (scoped and unlined_roles):
    refill(unlined_roles, "glue:historical_refill:bridges")
  elif segments is not None:
    segments.keep("glue:historical_refill:bridges")
  def schedule_fact_source(_returned: Any, item_id: str, period: int) -> dict[str, Any] | None:
    """A company-grounded schedule cell cites the filing fact or opening evidence that landed it."""
    coverage = (_returned or {}).get("source_fact_coverage", ()) if isinstance(_returned, Mapping) else ()
    facts_by_id = {} if bundle is None else {fact.fact_id: fact for fact in bundle.historical_basis_facts}
    evidence_by_id = {} if bundle is None else {evidence.evidence_id: evidence for evidence in bundle.evidence}
    matched = [
      row for row in coverage
      if isinstance(row, Mapping)
      and row.get("resolved_item_id") == item_id
      and period in [int(candidate) for candidate in row.get("periods", ())]
    ]
    disclosures = [row for row in matched if row.get("source_kind") == "source_bundle_schedule_disclosure"]
    matched_facts = [
      fact for row in disclosures
      if (fact := facts_by_id.get(str(row.get("fact_id")))) is not None
    ]
    if matched_facts:
      first_fact = matched_facts[0]
      return {
        "origin": "observed",
        "source": {
          "kind": "filing_line",
          "ref": keyed_dump.filing_line_json(first_fact.line),
          "observation_id": first_fact.fact_id,
          "fact_id": first_fact.fact_id,
          "source_id": str(first_fact.source_id),
          "schedule_disclosures": [
            {"fact_id": fact.fact_id, "source_id": str(fact.source_id)}
            for fact in matched_facts
          ],
          "via": {
            "artifact": "source_bundle",
            "source_kind": "source_bundle_schedule_disclosure",
          },
        },
      }
    for row in matched:
      if row.get("source_kind") != "source_bundle_opening_balance":
        continue
      evidence = evidence_by_id.get(str(row.get("evidence_id")))
      if evidence is None:
        continue
      return {
        "origin": "observed",
        "source": {
          "kind": "filing_line",
          "ref": keyed_dump.filing_line_json(evidence.line),
          "observation_id": str(row.get("requirement_id")),
          "evidence_id": evidence.evidence_id,
          "source_id": str(evidence.source_id),
          "via": {
            "artifact": "source_bundle",
            "source_kind": "source_bundle_opening_balance",
          },
        },
      }
    return None

  def company_grounded_schedules(model: Any) -> Any:
    # The pinned profile's evidence table, resolved against this model: the one participation decision.
    from schema.required_role_catalog import load_required_role_catalog
    from schema.role_disposition import items_by_role, resolve_build_dispositions

    role_items = items_by_role(load_required_role_catalog(), ticker=str(model.company.ticker))
    model.build_index()
    return apply_company_grounded_schedule_policies(
      model, business_model_revision=inputs.revision, compiled_registry=registry,
      historical_basis_facts=bundle.historical_basis_facts, grounded_semantic_rows=grounded_rows,
      opening_balances=opening or {},
      role_dispositions=resolve_build_dispositions(
        bundle.coverage_profile.role_dispositions, model, role_items,
        projected=tuple(model.time_structure.projection_periods),
      ),
      role_items=role_items,
    )

  # Pass order from the schedule policies through the valuation retirements; identical to
  # schema.build.compile_model_intermediate (valuation_inputs holds populate + suppress, run after the policies):
  #   1. company_grounded_schedules           apply_company_grounded_schedule_policies
  #   2. valuation_inputs                     populate_valuation_inputs, suppress_unauthorized_relative_valuation_outputs
  #   3. retire_unused_cost_of_debt
  schedule_policies = step(
    Helper("company_grounded_schedules", "schema/build_company_grounded_schedules.py:apply_company_grounded_schedule_policies", ("bundle",)),
    company_grounded_schedules,
    schedule_fact_source,
    historical_basis_facts=None if inputs.bundle is None else len(inputs.bundle.historical_basis_facts),
    # Schedule policies read an authored dilution rate off the document.
    net_share_dilution=_dilution_json(inputs.valuation_policy),
    read_slice=read_slices.company_grounded_schedules(bundle, opening=opening, grounded_rows=grounded_rows),
  )
  if schedule_policies is not None:
    out.role_dispositions = schedule_policies["role_dispositions"]
    out.role_items = schedule_policies["role_items"]
  elif rerun is not None:
    out.role_dispositions, out.role_items = rerun.role_dispositions, rerun.role_items
  from schema.build_findings import source_diagnostic_findings

  start = len(inputs.findings)
  inputs.findings.extend(
    {
      **finding.model_dump(mode="json"),
      "stage": "company_grounded_schedules",
      "owner_file": "schema/build_company_grounded_schedules.py",
    }
    for finding in source_diagnostic_findings(
      (schedule_policies or {}).get("diagnostics", ()), basis="pinned",
    )
  )
  out.helper_findings["company_grounded_schedules"].extend(inputs.findings[start:])
  start = len(structure.doc.edits)
  # A cell the policies find undefined and the filing reports nil for takes its cited absence.
  for item_id, period, citation in (schedule_policies or {}).get("evidenced_absence_cells", ()):
    document.mark_unavailable(
      structure.doc, item_id, period, reason="not_applicable_evidenced",
      source={"source": {"kind": "absence_citation", "citation": citation}},
    )
  if segments is not None:
    if rerun is not None and not any(row["helper"] == "company_grounded_schedules" for row in out.rows):
      segments.keep("glue:schedule_absences")
    else:
      segments.capture("glue:schedule_absences", start)
  still = set(unlined_roles) - out.lined_roles()
  if still and (rerun is None or scoped):
    refill(still, "glue:historical_refill:schedules")
  elif segments is not None and "glue:historical_refill:schedules" in segments.parent:
    segments.keep("glue:historical_refill:schedules")
  if inputs.bundle is not None:
    start = len(structure.doc.edits)
    _restore_disclosed_interest(structure.doc, inputs.bundle.historical_basis_facts)
    if segments is not None:
      segments.capture("glue:disclosed_interest", start)
  market = None if inputs.bundle is None else _captured(inputs, "covered_market_inputs", lambda: _market_inputs(bundle))
  from schema.build_model_product_contracts import ValuationLineageReference
  from schema.build_valuation_inputs import _VALUATION_ECONOMIC_INPUTS

  valuation_sources: dict[str, Mapping[str, Any]] = {}

  def valuation_cell_source(readiness: Any, item_id: str, period: int) -> Mapping[str, Any] | None:
    return valuation_sources.get(item_id)

  declared_unselected: dict[str, dict[str, str]] = {}

  def valuation_inputs(model: Any) -> Any:
    beta_floor_row = next(
      (row for row in getattr(inputs.valuation_policy, "policy_inputs", ()) if row.input_name == "beta_floor"),
      None,
    )
    readiness = populate_valuation_inputs(
      model,
      beta_floor_rationale=None if beta_floor_row is None else beta_floor_row.rationale,
      valuation=_merge_model_spec_valuation_controls(spec.valuation_controls),
      pinned_market_inputs=market,
      valuation_policy=spec.valuation_policy,
      valuation_family_authorizations=spec.valuation_family_authorizations,
    )
    if spec.valuation_policy_binding is not None:
      declared_unselected.update(suppress_unauthorized_relative_valuation_outputs(
        model, valuation_policy=spec.valuation_policy, valuation_family_authorizations=spec.valuation_family_authorizations,
      ))
    selected_items: dict[str, str] = {}
    for field_name, item_id in _VALUATION_ECONOMIC_INPUTS.items():
      selected_source = readiness.sources.get(field_name)
      candidate = None if market is None else getattr(market, field_name, None)
      observation_id = getattr(candidate, "observation_id", None)
      if observation_id is not None and selected_source == f"pinned:{observation_id}":
        selected_items[observation_id] = item_id
      elif selected_source is not None and not selected_source.startswith("pinned:"):
        valuation_sources[item_id] = {
          "origin": "derived",
          "source": {
            "kind": "construction_helper",
            "helper": "valuation_inputs",
            "owner": "schema/build_valuation_inputs.py:populate_valuation_inputs",
            "field": field_name,
            "selected_source": selected_source,
          },
        }
    if selected_items:
      reference_fields = set(ValuationLineageReference.model_fields)
      for observation in bundle.valuation_observations:
        item_id = selected_items.get(observation.observation_id)
        if item_id is not None:
          reference = observation.model_dump(mode="json", include=reference_fields)
          valuation_sources[item_id] = {
            "origin": "derived",
            "source": {
              "kind": "valuation_observation",
              "observation_id": observation.observation_id,
              "source_refs": [reference["source_id"]],
              "valuation_refs": [reference],
            },
          }
    return readiness

  valued = step(
    Helper(
      "valuation_inputs", "schema/build_valuation_inputs.py:populate_valuation_inputs", ("model_spec", "bundle"),
      resets_structure=True,
    ),
    valuation_inputs,
    cell_source=valuation_cell_source,
    market_inputs={
      name: None if market is None else getattr(getattr(market, name), "observation_id", None)
      for name in ("stock_price", "raw_beta", "risk_free_rate", "equity_risk_premium")
    },
    valuation_policy=_ref_json(inputs.valuation_policy),
    read_slice=read_slices.valuation_inputs(spec, bundle, market=market),
  )
  from schema.build_valuation_inputs import retire_unused_cost_of_debt
  out.retired.extend(step(
    Helper("retire_unused_cost_of_debt", "schema/build_valuation_inputs.py:retire_unused_cost_of_debt", retires=True),
    lambda model: retire_unused_cost_of_debt(model, valued, role_dispositions=out.role_dispositions),
  ) or ())
  start = len(structure.doc.edits)
  if valued is not None and declared_unselected and spec.valuation_policy_binding is not None:
    mark_declared_unselected_relative_inputs(structure, declared_unselected, spec.valuation_policy_binding)
  if segments is not None:
    if rerun is not None and not any(row["helper"] == "valuation_inputs" for row in out.rows):
      segments.keep("glue:relative_absences")
    else:
      segments.capture("glue:relative_absences", start)
  if share_conversion is not None:
    step(
      Helper("share_conversion", "schema/build_scenarios.py:_materialize_declared_share_conversion", ("bundle",)),
      lambda model: _materialize_declared_share_conversion(model, market, share_conversion),
      read_slice=read_slices.share_conversion(bundle, market=market, declaration=share_conversion),
    )
  defaults = declared_provisional_valuation_defaults(capabilities)
  if defaults:

    def provisional(model: Any) -> None:
      # ``compile_model_intermediate``'s inline step: the kind's declared provisional anchors.
      first = int(model.time_structure.projection_periods[0])
      for declared in defaults:
        anchor = model.get_item(declared.anchor_item_id)
        anchor.unit = declared.unit
        anchor.projected = FormulaSpec(
          type=FormulaType.constant, params={"value": declared.value},
          note="provisional_valuation_default_requires_user_review",
        )
        anchor.formula_periods = [first]

    step(Helper("provisional_valuation_defaults", "schema/build.py:compile_model_intermediate (declared provisional anchors)"), provisional)
  step(
    Helper("control_curves", "schema/build_projection_seeds.py:compile_candidate_model", ("model_spec",)),
    lambda model: compile_candidate_model(model, None, curves_of(model), registry, historical[-1]).model,
    scenario_set=_ref_json(inputs.scenario_set),
    read_slice=read_slices.control_curves(spec),
  )
  dependents = None if share_conversion is None else share_conversion.authenticated_dependents
  if dependents is not None:
    step(
      Helper("authenticated_dependents", "schema/build_scenarios.py:_finalize_authenticated_dependents"),
      lambda model: _finalize_authenticated_dependents(model, grounded_semantic_rows=grounded_rows),
    )
  step(Helper("non_gaap_addbacks", "schema/build_non_gaap_addbacks.py:_seed_projected_non_gaap_addbacks"), _seed_projected_non_gaap_addbacks)
  step(Helper("inventory_policy", "schema/build_working_capital_policy.py:_apply_projected_inventory_policy"), _apply_projected_inventory_policy)

  def scenario_outputs(model: Any) -> None:
    curves = curves_of(model)
    _populate_scenario_inputs(
      model, context["scenario_inputs"],
      **({"authenticated_scenario_curves": True} if any(curve.domain == "scenario" for curve in curves) else {}),
    )
    _write_scenario_outputs(model, spec, declared_eps_publication_basis(capabilities))

  step(
    Helper("scenario_outputs", "schema/build_scenarios.py:compute_scenario_outputs", ("model_spec",)),
    scenario_outputs, eps_publication_basis=declared_eps_publication_basis(capabilities),
    read_slice=read_slices.scenario_outputs(spec, eps_publication_basis=declared_eps_publication_basis(capabilities)),
  )
  step(
    Helper("presentation_residue", "schema/build_presentation_residue.py:_resolve_presentation_template_residue"),
    lambda model: _resolve_presentation_template_residue(
      model,
      ticker=ticker,
      composition_slots={
        requirement.model_item: requirement.required_role
        for requirement in (bundle.requirements if bundle is not None else ())
        if requirement.family == "operating" and requirement.model_item != requirement.required_role
      },
    ),
  )
  # Only seeds and node-history construction change the BusinessModel's formula-period tail.
  history_reran = rerun is not None and any(
    row.get("helper") == "business_model_node_history" and row.get("status") == "ok" for row in out.rows
  )
  if rerun is None or {"historical_foundation", "correspondence"} & set(rerun.stages) or history_reran:
    start = len(structure.doc.edits)
    from schema.build_bm_node_history import restrict_uncomputable_business_model_periods
    prior_periods = {
      item_id: None if item.formula_periods is None else list(item.formula_periods)
      for item_id, item in structure.doc.model._index.items()
    }
    restrict_uncomputable_business_model_periods(structure.doc.model, document.recalc(structure.doc))
    changes = {
      item_id: {"formula_periods": {"before": prior_periods[item_id], "after": item.formula_periods}}
      for item_id, item in structure.doc.model._index.items()
      if item.formula_periods != prior_periods[item_id]
    }
    document.record_edit(
      structure.doc,
      {"type": "construction", "helper": "business_model_periods", "owner": "schema/build_bm_node_history.py:restrict_uncomputable_business_model_periods"},
      source={"origin": "construction_helper", "helper": "business_model_periods"},
      result={"status": "ok", "field_changes": changes},
    )
    if segments is not None:
      segments.capture("glue:business_model_periods", start)
  elif segments is not None:
    segments.keep("glue:business_model_periods")
  if segments is not None:
    out.edit_segments = tuple(segments.segments[segment_start:])
  return out


def mark_declared_unselected_relative_inputs(
  structure: SheetFrame, declared: Mapping[str, Mapping[str, str]], binding: Any,
) -> None:
  """A relative method the judgment declared unselected keeps its scenario multiple inputs as
  cited absences: each anchor cell is ``not_applicable_evidenced`` with the ValuationPolicy
  declaration (``declared_unselected_relative_methods``) and the policy's id as its citation.
  A persisted none row, authored or server-synthesized, is in ``declared`` and is a cited
  not-applicable absence. No valuation policy, or no binding, leaves the cells unwritten and
  ``mark_missing_inputs`` reports them missing. This declaration replaces its own earlier mark
  when the policy changes; other authored cells remain untouched."""
  doc = structure.doc
  anchors = document.scalar_anchors(doc.model)
  for item_id, citation in sorted(declared.items()):
    if item_id not in doc.model._index:
      continue
    if item_id in anchors:
      periods = [] if anchors[item_id] is None else [int(cast(int, anchors[item_id]))]
    else:
      periods = [*structure.forecast_years]
    for period in periods:
      held = doc.semantic.get((item_id, period))
      if held is not None and held.get("cell") != "formula" and (held.get("absence_citation") or {}).get("kind") != "valuation_policy":
        continue
      document.mark_unavailable(
        doc, item_id, period, reason="not_applicable_evidenced",
        source={"origin": "observed", "absence_citation": {
          **citation,
          "valuation_policy_id": binding.valuation_policy_id,
          "valuation_policy_content_sha256": binding.valuation_policy_content_sha256,
        }},
      )


def _unwritten_input_periods(
  structure: SheetFrame, item: LineItem, anchors: Mapping[str, Any], retired: Collection[str],
) -> Iterator[int]:
  """The periods of input line ``item`` that are cells nothing wrote (``mark_missing_inputs``'s
  cells): not retired, not a constant spec, not written or formula-owned, owned by the row, and
  holding no value or override. Lazy: a caller stamping one period sees the next one's state."""
  doc = structure.doc
  if item.id in retired:
    return
  if item.id in anchors:
    anchor = anchors[item.id]
    periods = [] if anchor is None else [int(anchor)]
  else:
    periods = [*structure.historical_years, *structure.forecast_years]
  for period in periods:
    spec = document._render_spec_for_period(doc.model, item, period)
    if spec is not None and spec.type is FormulaType.constant:
      continue
    if (item.id, period) in doc.semantic or _formula_owned(doc, item.id, period):
      continue
    if not item_owns_cell(doc.model, item, period):
      continue
    current = doc.model.get_item(item.id)
    if (current.overrides and period in current.overrides) or (
      current.values is not None and period in current.values.values
    ):
      continue
    yield period


def _mark_unresolved_node_cell(doc: document.ModelSheet, item_id: str, period: int, *, gaps: Sequence[str] = ()) -> None:
  """The one stamp of an unresolved BusinessModel node's unwritten historical cell:
  ``business_model_node_unresolved`` (``project_provenance_map`` projects every ``MissingReasonCode``
  stamp as its own typed ``lineage_status``, so this cell is never a ``missing_source`` and
  ``provenance_diagnostics_for_map`` emits no per-cell diagnostic for it)."""
  column: dict[str, Any] = {
    "source": {"kind": "none", "note": "the node's history is not written; its business_model_node_unresolved finding is the record"},
  }
  if gaps:
    column["gap"] = list(gaps)
  document.mark_unavailable(doc, item_id, period, reason="business_model_node_unresolved", source=column)


def mark_unresolved_node_history(structure: SheetFrame, unresolved_nodes: Collection[str]) -> int:
  """Stamp the unwritten historical cells of the lines ``unresolved_nodes`` names
  ``business_model_node_unresolved`` and nothing else: the stamp ``mark_missing_inputs`` gives them,
  for a document that does not stamp its other missing inputs (the compile's finalize document,
  whose other unwritten cells are projected from the model). Returns the cells stamped."""
  doc = structure.doc
  historical = set(structure.historical_years)
  anchors = document.scalar_anchors(doc.model)
  stamped = 0
  for item_id in sorted(set(unresolved_nodes)):
    item = doc.model._index.get(item_id)
    if item is None or item.item_type is not ItemType.input:
      continue
    for period in _unwritten_input_periods(structure, item, anchors, ()):
      if period in historical:
        _mark_unresolved_node_cell(doc, item_id, period)
        stamped += 1
  return stamped


def mark_missing_inputs(
  structure: SheetFrame, *, missing_helpers: Collection[str] = (), retired_items: Collection[str] = (),
  unresolved_nodes: Collection[str] = (),
) -> dict[str, int]:
  """Every input cell nothing wrote: projection periods ``required_dependency_missing``,
  historical periods ``source_observation_absent`` (no required role maps the line), or
  ``business_model_node_unresolved`` on a line ``unresolved_nodes`` names (a BusinessModel node
  whose history ``apply_business_model_node_history`` could not write: the node's own finding is
  the one record of it). ``project_provenance_map`` projects each of these ``MissingReasonCode``
  stamps as its own typed ``lineage_status`` and never as ``missing_source``, which means only that
  a write path admitted a value without an origin.
  When a construction helper did not run, each such cell's column names it
  (``gap: construction_helper_missing:<helper>``): the document cannot tell which of them that
  helper would have produced. Rows retired at their owner during construction
  (``Construction.retired``) and coordinates the row does not own (``item_owns_cell``) are not
  cells; every other unwritten input still stamps."""
  doc = structure.doc
  counts: Counter[str] = Counter()
  historical = set(structure.historical_years)
  anchors = document.scalar_anchors(doc.model)
  gaps = [f"construction_helper_missing:{name}" for name in missing_helpers]
  unresolved = set(unresolved_nodes)
  for item in list(doc.model._index.values()):
    if item.item_type is not ItemType.input:
      continue
    for period in _unwritten_input_periods(structure, item, anchors, retired_items):
      reason: CellUnavailableReason
      if period in historical and item.id in unresolved:
        _mark_unresolved_node_cell(doc, item.id, period, gaps=gaps)
        counts["business_model_node_unresolved"] += 1
        continue
      if period in historical:
        reason, note = "source_observation_absent", "no required role maps this line"
      else:
        reason, note = "required_dependency_missing", "no judgment row maps this input"
      column: dict[str, Any] = {"source": {"kind": "none", "note": note}}
      if gaps:
        column["gap"] = gaps
      document.mark_unavailable(doc, item.id, period, reason=reason, source=column)
      counts[reason] += 1
  return dict(counts)


# --------------------------------------------------------------------------
# check
# --------------------------------------------------------------------------


def target_check(
  doc: document.ModelSheet, *, validation: ValidationReport | None = None, deferred: bool = False,
) -> dict[str, Any]:
  """The check of ``doc``. ``validation`` is the one report publication persists
  (``document_validation``: ``publish.validate_document_graph`` over ``validate_offline_build_artifacts``),
  so its findings, their severities (``schema.finding_vocabulary.FINDING_SEVERITY_MAP``) and its
  ``status`` (``derive_build_verdict``) are the check's; ``blocking`` is the findings that make the
  status ``blocked`` (``blocking_findings``). ``None``: no SourceBundle / ModelSpec to stage the
  document against, so nothing was validated (``validated: False``); ``deferred`` (``validated:
  None``): the caller stages and validates the version itself (``target_build``'s ``stage_validation``).

  ``scale_mix`` is the one finding added here: an authoring warning on a formula's operand
  scales (``document.scale_warnings``), not in the vocabulary and never blocking."""
  findings: list[dict[str, Any]] = (
    [] if validation is None else [finding.model_dump(mode="json") for finding in validation.findings]
  )
  model = doc.model
  for item in model._index.values():
    specs = [spec for spec in (item.historical, item.projected) if spec is not None]
    if len(specs) == 2 and specs[0] == specs[1]:
      specs = specs[:1]
    for spec in specs:
      for warning in document.scale_warnings(spec, model, item.id):
        findings.append({"code": "scale_mix", "severity": "warning", "basis": "document", "item_id": item.id, "detail": warning})
  return {
    "validated": None if deferred else validation is not None,
    "status": None if validation is None else validation.status,
    "findings": findings,
    "blocking": [] if validation is None else [
      {key: row[key] for key in ("code", "severity", "item_id", "periods")}
      for row in (finding.model_dump(mode="json") for finding in blocking_findings(validation.findings))
    ],
    "counts": dict(Counter(row["code"] for row in findings)),
    "severity": dict(Counter(row["severity"] for row in findings)),
  }


def document_validation(
  doc: document.ModelSheet, inputs: StageBuildInputs, ticker: str, unclaimed: Sequence[Mapping[str, Any]], refusals: _Refusals,
  *, template: FinancialModel, scenario_controls: Mapping[str, Any] | None,
) -> ValidationReport | None:
  """The graph validator's report over ``doc`` (``publish.validate_document_graph``) with the
  build's own scenario controls and unclaimed filed lines: the one source of the build's check
  findings and blocking verdict. ``None`` when the SourceBundle or ModelSpec did not resolve
  (``inputs_unresolved``) or validation raised (a captured ``validation`` refusal)."""
  if inputs.bundle is None or inputs.model_spec is None:
    return None
  from model_build import publish
  if scenario_controls is None:
    from schema.build_model_product_contracts import ValidationReport
    from schema.finding_vocabulary import Finding

    return ValidationReport(
      status="blocked", validated_at=datetime.now(UTC),
      findings=(Finding(
        code="working_document_scenario_controls_unavailable", severity="error",
        basis="authored", clearable_by="repair",
        message="The saved document has no construction scenario-control baseline; rebuild it with target_build.",
        expected={"next_call": "target_build"},
      ),),
      recalculation_engine=None, recalculation_engine_version=None,
      recalculated_formula_cell_count=None, intentional_na_count=0,
      unexpected_excel_error_count=None, recalculation_divergence_count=None,
      readback_invariants_evaluated=False, readback_invariant_finding_count=0,
    )

  def validate() -> ValidationReport:
    curves, bound = scenario_publication_controls_from_payload(scenario_controls)
    return publish.validate_document_graph(
      doc,
      ticker=ticker,
      source_bundle=inputs.bundle,
      model_spec=inputs.model_spec,
      bundle_id=inputs.bundle_id,
      template=template,
      control_curves=curves,
      bound_scenario_control_ids=bound,
      unclaimed_filed_lines=tuple(unclaimed),
      input_findings=inputs.findings,
    )

  return refusals.run("validation", validate)


# --------------------------------------------------------------------------
# snapshot
# --------------------------------------------------------------------------


def _sha256(path: Path) -> str:
  return hashlib.sha256(path.read_bytes()).hexdigest()


def _bench_sha() -> str | None:
  completed = subprocess.run(["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, capture_output=True, text=True, check=False)
  return completed.stdout.strip() or None


def _operands(spec: Any) -> list[str]:
  from schema.build_formula_refs import _extract_ref_ids

  return sorted(_extract_ref_ids(spec.params or {})) if spec is not None and spec.params else []


def formula_authors(doc: document.ModelSheet) -> dict[str, dict[str, str]]:
  """Item → the construction helper that last authored its structure (``doc.edits``)."""
  out: dict[str, dict[str, str]] = {}
  for edit in doc.edits:
    op, result = edit.get("op") or {}, edit.get("result") or {}
    if op.get("type") != "construction" or result.get("status") != "ok":
      continue
    for item_id in (*result.get("items_added", ()), *result.get("formulas", ())):
      out[item_id] = {"helper": op["helper"], "owner": op["owner"]}
  return out


def semantic_rows(doc: document.ModelSheet, ours: document.Values) -> list[dict[str, Any]]:
  """The semantic column of every cell of ours: what the writers recorded, or for a cell no
  writer touched, the formula that produces it (text and operands, and the construction
  helper that authored it) or ``cell: empty``."""
  authors = formula_authors(doc)
  rows: list[dict[str, Any]] = []
  for (item_id, period), value in sorted(ours.items()):
    column = doc.semantic.get((item_id, period))
    item = doc.model.get_item(item_id)
    cell_obj = item.values.values.get(period) if item and item.values else None
    line_ref = getattr(cell_obj, "line_ref", None) if cell_obj else None
    spec = document._render_spec_for_period(doc.model, item, period)
    if column is None:
      if spec is not None and spec.type is not FormulaType.constant:
        column = document.formula_column(spec)
      elif spec is not None:
        column = stamp_cell_source(cell="template_constant", source={"kind": "template", "value": spec.params.get("value")})
      else:
        column = stamp_cell_source(cell="empty")
    if column.get("cell") == "formula":
      column = stamp_cell_source(column)
      if spec is not None and spec.type is not FormulaType.constant:
        column["source"] = {"kind": "formula", "formula": document.formula_text(spec), "operands": _operands(spec)}
      if item_id in authors and "authored_by" not in column:
        column["authored_by"] = authors[item_id]
    extra_line = {"line_ref": line_ref} if line_ref is not None else {}
    rows.append({"item_id": item_id, "period": period, **column, **extra_line, "value": document.value_row(value)})
  return rows

def _filing_line_ref(
  value: Any,
  *,
  statement_role_by_accession_tag: Mapping[Any, str] | None = None,
) -> Any | None:
  """A value cell's ``line_ref`` or ``filing_line`` source as a ``FilingLineRef``."""
  from pydantic import TypeAdapter, ValidationError
  from schema.provider_port.contracts import FilingLineRef
  from schema.statement_topology import normalize_topology_role

  if isinstance(value, FilingLineRef):
    return value
  raw: Any = value
  if isinstance(value, Mapping):
    nested = value.get("ref")
    if isinstance(nested, Mapping):
      raw = {
        **{
          key: item
          for key, item in value.items()
          if key in ("accession", "period", "statement_role", "tag", "dimensions")
        },
        **dict(nested),
      }
    else:
      raw = dict(value)
    if not raw.get("tag"):
      line_qname = raw.get("line_qname") or value.get("line_qname")
      if isinstance(line_qname, str) and line_qname:
        raw["tag"] = line_qname
    period = raw.get("period")
    if isinstance(period, bool):
      period = None
    if isinstance(period, int):
      raw["period"] = {"kind": "fiscal_year", "fiscal_year": period}
    elif isinstance(period, str):
      fy_match = re.fullmatch(r"FY(\d{4})", period.strip())
      if fy_match is not None:
        raw["period"] = {"kind": "fiscal_year", "fiscal_year": int(fy_match.group(1))}
    if raw.get("period") is None:
      fy_candidate = raw.get("fiscal_year") or value.get("fiscal_year")
      if isinstance(fy_candidate, int) and not isinstance(fy_candidate, bool):
        raw["period"] = {"kind": "fiscal_year", "fiscal_year": int(fy_candidate)}
    dimensions = raw.get("dimensions")
    if isinstance(dimensions, (list, tuple)):
      normalized_dims: list[Any] = []
      for entry in dimensions:
        if isinstance(entry, (list, tuple)) and len(entry) == 2:
          normalized_dims.append({"axis_qname": entry[0], "member_qname": entry[1]})
        elif (
          isinstance(entry, Mapping)
          and "axis_qname" not in entry
          and "axis" in entry
          and "member" in entry
        ):
          normalized_dims.append({"axis_qname": entry["axis"], "member_qname": entry["member"]})
        else:
          normalized_dims.append(entry)
      raw["dimensions"] = normalized_dims
    if not raw.get("statement_role"):
      presentation_role = raw.get("presentation_role_name") or value.get("presentation_role_name")
      if isinstance(presentation_role, str) and presentation_role.strip():
        normalized_role = normalize_topology_role(presentation_role)
        if normalized_role:
          raw["statement_role"] = normalized_role
      if not raw.get("statement_role") and statement_role_by_accession_tag:
        obs_key = _observation_role_lookup_key(
          raw.get("accession"),
          raw.get("period"),
          raw.get("tag"),
          raw.get("dimensions"),
        )
        if obs_key is not None and obs_key in statement_role_by_accession_tag:
          raw["statement_role"] = statement_role_by_accession_tag[obs_key]
  try:
    return TypeAdapter(FilingLineRef).validate_python(raw)
  except (ValidationError, ValueError):
    return None


def _observation_role_lookup_key(
  accession: Any,
  period: Any,
  tag: Any,
  dimensions: Any,
) -> tuple[str, str, str, tuple[tuple[str, str], ...]] | None:
  acc = str(accession or "").strip()
  tg = str(tag or "").strip()
  if not acc or not tg:
    return None
  per = ""
  if hasattr(period, "canonical_label"):
    per = str(period.canonical_label())
  elif isinstance(period, int) and not isinstance(period, bool):
    per = f"FY{period}"
  elif isinstance(period, str):
    per = period.strip()
  elif isinstance(period, Mapping):
    kind = period.get("kind")
    if kind == "fiscal_year" and period.get("fiscal_year") is not None:
      per = f"FY{int(period['fiscal_year'])}"
    elif kind == "fiscal_quarter" and period.get("fiscal_year") is not None and period.get("quarter") is not None:
      per = f"FY{int(period['fiscal_year'])}-Q{int(period['quarter'])}"
    elif kind == "date" and period.get("date") is not None:
      per = str(period["date"]).strip()
    elif period.get("end_date") is not None:
      per = str(period["end_date"]).strip()
  if not per:
    return None
  dim_pairs: list[tuple[str, str]] = []
  if isinstance(dimensions, Mapping):
    for k, v in dimensions.items():
      if k and v:
        dim_pairs.append((str(k), str(v)))
  elif isinstance(dimensions, (list, tuple)):
    for entry in dimensions:
      if isinstance(entry, (list, tuple)) and len(entry) == 2:
        dim_pairs.append((str(entry[0]), str(entry[1])))
      elif isinstance(entry, Mapping):
        ax = entry.get("axis_qname") or entry.get("axis")
        mem = entry.get("member_qname") or entry.get("member")
        if ax and mem:
          dim_pairs.append((str(ax), str(mem)))
      else:
        ax = getattr(entry, "axis_qname", None)
        mem = getattr(entry, "member_qname", None)
        if ax and mem:
          dim_pairs.append((str(ax), str(mem)))
  return (acc, per, tg, tuple(sorted(set(dim_pairs))))


def _statement_role_lookups(
  rows_or_doc: Any, source_bundle_obj: Any = None,
) -> dict[Any, str]:
  """Exact observation ``(accession, period, tag, dimensions)`` -> normalized ``statement_role``.

  Scans the SourceBundle's evidence / historical basis facts and any semantic
  row or document column whose source or ``line_ref`` already carries a
  ``statement_role``, so a ``filing_line`` source omitting the role resolves
  it only when the exact observation identity exists in the filing/bundle.
  """
  from schema.statement_topology import normalize_topology_role

  roles_by_identity: dict[tuple[str, str, str, tuple[tuple[str, str], ...]], set[str]] = {}

  def record(
    key: tuple[str, str, str, tuple[tuple[str, str], ...]] | None,
    role: Any,
  ) -> None:
    if key is None or not role:
      return
    normalized_role = normalize_topology_role(role)
    if not normalized_role:
      return
    roles_by_identity.setdefault(key, set()).add(normalized_role)

  def extract(
    candidate: Any,
    fallback_period: Any = None,
  ) -> tuple[tuple[str, str, str, tuple[tuple[str, str], ...]], str] | None:
    if candidate is None:
      return None
    if not isinstance(candidate, Mapping):
      accession = getattr(candidate, "accession", None)
      period = getattr(candidate, "period", None) or fallback_period
      tag = getattr(candidate, "tag", None)
      dims = getattr(candidate, "dimensions", None)
      role = getattr(candidate, "statement_role", None)
      key = _observation_role_lookup_key(accession, period, tag, dims)
      if key is not None and role:
        return key, str(role)
      return None
    nested = candidate.get("ref")
    merged: dict[str, Any] = {**dict(candidate), **dict(nested)} if isinstance(nested, Mapping) else dict(candidate)
    line_candidate = merged.get("line") or merged.get("line_ref")
    if line_candidate is not None and line_candidate is not candidate:
      extracted_line = extract(line_candidate, fallback_period=merged.get("period") or merged.get("fiscal_year") or fallback_period)
      if extracted_line is not None:
        return extracted_line
    accession = merged.get("accession")
    period = merged.get("period") or merged.get("fiscal_year") or fallback_period
    tag = merged.get("tag") or merged.get("line_qname")
    dims = merged.get("dimensions")
    role = merged.get("statement_role")
    if not role and merged.get("presentation_role_name"):
      role = normalize_topology_role(merged.get("presentation_role_name"))
    key = _observation_role_lookup_key(accession, period, tag, dims)
    if key is None or not role:
      return None
    return key, str(role)

  if source_bundle_obj is not None:
    for attribute in ("evidence", "historical_basis_facts"):
      for entry in getattr(source_bundle_obj, attribute, ()) or ():
        line = getattr(entry, "line", None)
        extracted = extract(line if line is not None else entry, fallback_period=getattr(entry, "period_id", None))
        if extracted is not None:
          record(*extracted)
  entries: list[tuple[Any, Any, Any]] = []
  semantic = getattr(rows_or_doc, "semantic", None)
  if isinstance(semantic, Mapping):
    for (item_id, period), column in semantic.items():
      entries.append((item_id, period, column))
  elif isinstance(rows_or_doc, (list, tuple)):
    for row in rows_or_doc:
      if isinstance(row, Mapping):
        entries.append((row.get("item_id"), row.get("period"), row))
  elif isinstance(rows_or_doc, Mapping):
    for key, column in rows_or_doc.items():
      if isinstance(key, tuple) and len(key) >= 2:
        entries.append((key[0], key[1], column))
      else:
        entries.append((key, None, column))
  for _item_id, col_period, column in entries:
    if not isinstance(column, Mapping):
      continue
    candidates: list[Any] = []
    if column.get("line_ref") is not None:
      candidates.append(column.get("line_ref"))
    if column.get("line") is not None:
      candidates.append(column.get("line"))
    source = column.get("source")
    if isinstance(source, Mapping):
      candidates.append(source)
    if column.get("accession") or column.get("statement_role") or column.get("ref"):
      candidates.append(column)
    for candidate in candidates:
      extracted = extract(candidate, fallback_period=col_period)
      if extracted is not None:
        record(*extracted)
  return {
    key: min(
      roles,
      key=lambda role: (
        not role.startswith("consolidated"),
        "comprehensive" in role,
        role,
      ),
    )
    for key, roles in roles_by_identity.items()
    if roles
  }


def _cell_filing_line(
  row: Mapping[str, Any], kind: Any, role_by_acc_tag: Mapping[Any, str],
) -> Any | None:
  """The ``FilingLineRef`` a semantic row cites: its ``line_ref``, else its ``filing_line`` source."""
  return _filing_line_ref(
    row.get("line_ref"),
    statement_role_by_accession_tag=role_by_acc_tag,
  ) or (
    _filing_line_ref(
      row.get("source"),
      statement_role_by_accession_tag=role_by_acc_tag,
    )
    if kind == "filing_line"
    else None
  )


def _cited_kpi_observation(source: Mapping[str, Any], source_bundle: Any, period: int) -> Any | None:
  """The sealed ``kpi_observations`` row a ``kpi`` source names, else ``None``.

  The bundle carries each KPI row's producer record in the observation ``detail``, so a citation
  names its row by the record's locator ``span`` (any of its locators) or its ``observation_id``,
  narrowed by a cited ``accession``. The capture ``row`` index and ``capture_id`` name the capture,
  which the bundle does not carry, so they identify nothing here. A citation that matches no ``ok``
  row in the cell's fiscal period, or more than one, names no observation and stays the agent's
  citation (RULE-I1).
  """
  if source_bundle is None:
    return None
  nested = source.get("ref")
  coordinates = {**source, **nested} if isinstance(nested, Mapping) else source
  span = coordinates.get("span")
  observation_id = coordinates.get("observation_id")
  accession = coordinates.get("accession")
  if not isinstance(span, str) and not isinstance(observation_id, str):
    return None
  period_labels = {
    f"FY{period}",
    *(
      annual.period_end.isoformat() for annual in source_bundle.reported_periods
      if annual.period_type == "FY" and annual.fiscal_year == period
    ),
  }
  matched = []
  for observation in source_bundle.kpi_observations:
    if (
      observation.status != "ok"
      or observation.line.period.canonical_label() not in period_labels
      or (isinstance(accession, str) and observation.line.accession != accession)
    ):
      continue
    try:
      record = json.loads(observation.detail or "")
    except ValueError:
      continue
    if not isinstance(record, Mapping):
      continue
    locators = (record.get("locator"), *(record.get("additional_locators") or ()))
    spans = {locator.get("span") for locator in locators if isinstance(locator, Mapping)}
    if (isinstance(span, str) and span not in spans) or (
      isinstance(observation_id, str) and observation_id != record.get("observation_id")
    ):
      continue
    matched.append(observation)
  return matched[0] if len(matched) == 1 else None


def _kpi_source_ids(line: Any | None, source_bundle: Any) -> tuple[str, ...]:
  """The sealed KPI ``SourceRecord`` ids that hold ``line``'s accession's KPI rows (the builder's ``kpi_observations:<accession>``)."""
  if line is None or source_bundle is None:
    return ()
  return tuple(
    str(source.id) for source in source_bundle.sources
    if source.endpoint_or_filing_id == f"kpi_observations:{line.accession}"
  )


def _cited_occurrence_evidence(
  source: Mapping[str, Any],
  source_bundle: Any,
  period: int,
  item: Any,
  authored: Any,
) -> Any | None:
  """The sealed ``SourceEvidence`` row an ``occurrences.jsonl`` capture citation names, else ``None``.

  Matches by the cited occurrence's resolved filing-line identity (``accession``, ``period_id``,
  ``tag``, ``dimensions``, and ``statement_role`` when present on ``concept``) and uses value
  agreement strictly as verification.
  """
  from decimal import Decimal
  from schema.build_model_product_contracts import VALUE_SCALE_FACTORS, source_measure

  if source_bundle is None or item is None or authored is None:
    return None
  path = str(source.get("path") or "")
  if not path.endswith("occurrences.jsonl") and not source.get("cell_locator"):
    return None
  acc_match = re.search(r"\d{10}-\d{2}-\d{6}", str(source.get("accession") or path))
  if acc_match is None:
    return None
  accession = acc_match.group(0)
  concept = str(source.get("concept") or "").strip()
  expected_role = ""
  expected_tag = ""
  expected_dims: tuple[tuple[str, str], ...] | None = None
  if concept and "|" in concept:
    parts = concept.split("|", 2)
    if len(parts) == 3:
      expected_role, expected_tag, dims_raw = parts
      parsed_dims: list[tuple[str, str]] = []
      for pair in dims_raw.split(","):
        if "=" in pair:
          ax, mem = pair.split("=", 1)
          if ax.strip() and mem.strip():
            parsed_dims.append((ax.strip(), mem.strip()))
      expected_dims = tuple(sorted(parsed_dims))
  elif concept:
    expected_tag = concept
    expected_dims = ()
  if not expected_tag:
    return None
  matched: list[Any] = []
  for ev in source_bundle.evidence:
    if (
      ev.line is None
      or ev.line.accession != accession
      or ev.period_id != f"FY{period}"
      or ev.line.tag != expected_tag
      or ev.value is None
    ):
      continue
    ev_dims = tuple(
      sorted((str(d.axis_qname), str(d.member_qname)) for d in (ev.line.dimensions or ()))
    )
    if expected_dims is not None and ev_dims != expected_dims:
      continue
    if expected_role and str(ev.line.statement_role or "") != expected_role:
      continue
    try:
      measure = source_measure(ev.unit)
      ev_scale = (measure.model_scale if measure is not None and measure.model_scale else None) or ev.scale
      scaled = float(
        Decimal(str(ev.value)) * VALUE_SCALE_FACTORS[ev_scale] / Decimal(10) ** document._line_exponent(item)
      )
    except Exception:
      continue
    if abs(scaled - float(authored)) <= CITED_FACT_AGREEMENT_ABS:
      matched.append(ev)
  if not matched:
    return None
  matched.sort(
    key=lambda ev: (
      not str(getattr(ev.line, "statement_role", "") or "").startswith("statement"),
      str(ev.evidence_id),
    )
  )
  return matched[0]


#: ``abs(authored - filed)`` on the model's scale at or below this is agreement (the journey
#: measure's ``IDENTITY_ABS``).
CITED_FACT_AGREEMENT_ABS = 1e-6


@dataclass(frozen=True)
class CitedFactMismatch:
  """One cited ``set_input`` whose authored value disagrees with the SourceBundle fact it cites.

  ``filed`` is the cited evidence's value on the cell's model scale (``model_scale``);
  ``evidence`` is the ``SourceEvidence`` row it was read from and ``line`` the cited filing line.
  """

  item_id: str
  period: int
  authored: float
  filed: Any
  model_scale: str
  evidence: Any | None
  line: Any

  @property
  def observation_id(self) -> str:
    return projection.filing_line_observation_id(self.line)


def cited_fact_mismatches(
  rows: Sequence[Mapping[str, Any]], source_bundle: Any, model: Any,
) -> dict[tuple[str, int], CitedFactMismatch]:
  """The cited-input cells of ``rows`` whose authored value disagrees with the fact they cite.

  The one owner of that comparison: ``project_provenance_map`` withholds the cited line as such a
  cell's own observation, and ``schema.build_findings.cited_fact_value_mismatch_findings`` emits the
  finding, both from this result. A cell is compared when its origin is ``cited_input``, its source
  is a ``filing_line`` or ``kpi`` that names a sealed bundle fact, and its value is known.
  A KPI uses the selected observation's canonical quantity, including when its line has a pin;
  ``quantity_native_value(kpi_fact.value)`` recovers that canonical quantity from the declared-scale
  ``Quantity``; the pin supplies evidence metadata, not a different observation's value. ``SourceEvidence.value`` is already
  the economic value of the model's scale convention: the bundle builder applied the presentation
  sign (``preferred_label_negates_value``) and the role polarity
  (``projection._fact_requirement_value``), so only its scale is reconciled with the line's
  ``model_scale`` — read as the accepted-HFF application reads it (``source_measure``): the scale
  the unit name fixes when it fixes one (``shares_millions`` folds the million into the name and
  lands on a ``units`` share row as ``56.55``), else ``SourceEvidence.scale``, converted by
  ``VALUE_SCALE_FACTORS``. The cell agrees when any evidence row of the cited line (a restated
  vintage, a vendor copy) holds the authored value within ``CITED_FACT_AGREEMENT_ABS``; otherwise
  the first such row, primary quality first, is named.
  """
  from decimal import Decimal
  from schema.build_model_product_contracts import VALUE_SCALE_FACTORS, source_measure
  from schema.build_population import quantity_native_value

  def evidence_scale(evidence: Any) -> str:
    measure = source_measure(evidence.unit)
    return (measure.model_scale if measure is not None and measure.model_scale else None) or evidence.scale

  if source_bundle is None:
    return {}
  evidence_by_line: dict[Any, list[Any]] = {}
  for evidence in sorted(source_bundle.evidence, key=lambda row: row.source_quality != "primary"):
    evidence_by_line.setdefault(evidence.line, []).append(evidence)
  role_by_acc_tag = _statement_role_lookups(rows, source_bundle)
  found: dict[tuple[str, int], CitedFactMismatch] = {}
  for row in rows:
    stamp: CellSourceStamp = row if isinstance(row, CellSourceStamp) else stamp_cell_source(row)
    kind = stamp.source_dict.get("kind")
    value = row["value"]
    if stamp.origin != "cited_input" or kind not in ("filing_line", "kpi") or value.get("state") != "known":
      continue
    kpi_observation = (
      _cited_kpi_observation(stamp.source_dict, source_bundle, row["period"]) if kind == "kpi" else None
    )
    line = (
      kpi_observation.line if kpi_observation is not None
      else _cell_filing_line(row, kind, role_by_acc_tag) if kind != "kpi" else None
    )
    cited = evidence_by_line.get(line, [])
    kpi_fact = kpi_observation.fact if kpi_observation is not None else None
    if kpi_fact is None and (kind == "kpi" or not cited):
      continue
    item = model.get_item(row["item_id"])
    authored = float(value["value"])
    on_model_scale = (
      [quantity_native_value(kpi_fact.value) / Decimal(10) ** document._line_exponent(item)]
      if kpi_fact is not None
      else [
        Decimal(evidence.value) * VALUE_SCALE_FACTORS[evidence_scale(evidence)] / Decimal(10) ** document._line_exponent(item)
        for evidence in cited
      ]
    )
    if any(abs(float(filed) - authored) <= CITED_FACT_AGREEMENT_ABS for filed in on_model_scale):
      continue
    found[(row["item_id"], row["period"])] = CitedFactMismatch(
      item_id=row["item_id"], period=row["period"], authored=authored,
      filed=on_model_scale[0], model_scale=item.model_scale, evidence=cited[0] if cited else None, line=line,
    )
  return found


def _provenance(rows: list[dict[str, Any]], rendered: Any) -> list[dict[str, Any]]:
  """Compatibility row view of the single provenance projector."""
  return project_provenance_map(cast(document.ModelSheet, rows), rendered)


def _cites_absence(column: Mapping[str, Any] | None) -> bool:
  """Whether a semantic column is a cited absence: ``not_applicable_evidenced`` or a cited inclusion."""
  if column is None:
    return False
  source = column.get("source") or {}
  if not (column.get("absence_citation") or source.get("citation")):
    return False
  return column.get("reason") == "not_applicable_evidenced" or column.get("cell") == "not_separately_presented"


def project_provenance_map(
  doc: document.ModelSheet,
  ours_or_rendered: Any = None,
  rendered: Any = None,
  *,
  source_bundle_digest: str = "",
  model_spec_digest: str = "",
  source_bundle: Any | None = None,
  model_spec: Any | None = None,
  seed_coverage_manifest: Sequence[Mapping[str, Any]] = (),
  evaluation_reads: Mapping[tuple[str, int], tuple[tuple[str, int], ...]] | None = None,
  case_evaluation_reads: Mapping[str, Mapping[tuple[str, int], tuple[tuple[str, int], ...]]] | None = None,
  values: document.Values | None = None,
) -> Any:
  """Project carried cell stamps, direct declarations and actual evaluation reads once.

  The row-list compatibility view serves snapshot JSON and older ``_provenance`` callers;
  document callers receive the typed ``ProvenanceMap``.
  """
  from decimal import Decimal
  from schema.build_model_product_contracts import (
    AssumptionLineageReference,
    CellLineage,
    CompilerOrigin,
    EvidenceLineageReference,
    HistoricalDriverSeedLineageReference,
    ModelSpec,
    ProvenanceMap,
    SourceBundle,
    ValuationLineageReference,
    VALUE_SCALE_FACTORS,
    _COMPILED_LITERAL_CONTROL_IDS,
    _WORKBOOK_METADATA_ITEM_IDS,
    decode_source_bundle_payload,
  )
  from schema.build_offline_validation import _semantic_cells, lag_input_gap_readers, owned_cell_gaps
  from schema.build_population import quantity_native_value
  from schema.coverage_profile import AnnualSourceRequirement, PeriodPinCell
  from schema.finding_vocabulary import Declination
  from schema.model_kind_dispatch import (
    declared_capabilities as _declared_capabilities,
    declared_compiler_origins as _declared_compiler_origins,
  )
  from schema.models import FormulaSpec
  from schema.model_queries import SCENARIO_ASSUMPTION_PREFIX
  from schema.provenance_classifier import REGISTRY_DISCOUNT, REGISTRY_SCAFFOLD, REGISTRY_SCENARIO
  from schema.renderer_columns import AbsoluteColumnMapper
  from schema.valuation_evidence_derivations import derive_covered_market_inputs

  legacy_rows = isinstance(doc, list)
  if rendered is None and (
    hasattr(ours_or_rendered, "semantic_cells") or hasattr(ours_or_rendered, "cells")
  ):
    rendered_obj: Any = ours_or_rendered
    ours: Any = None if legacy_rows else values if values is not None else document.recalc(doc)
  else:
    rendered_obj = rendered
    ours = values if values is not None else ours_or_rendered
  rows = cast(list[dict[str, Any]], doc) if legacy_rows else semantic_rows(doc, cast(document.Values, ours))
  if rendered_obj is None:
    from model_build import publish
    plan_less_gaps = owned_cell_gaps(
      doc.model,
      source_bundle
      if source_bundle is None or isinstance(source_bundle, SourceBundle)
      else decode_source_bundle_payload(source_bundle),
    )
    cells = {
      (item.id, int(period)): document.RenderedCell(
        sheet=sheet, cell=cell, kind="formula" if spec is not None else "hardcode",
        formula=None, tolerance=1e-9, family="document",
      )
      for sheet, cell, item, period, spec in publish.document_semantic_cells(doc, doc.model, plan_less_gaps)
    }
  elif hasattr(rendered_obj, "cells"):
    cells = rendered_obj.cells
  else:
    writes = {(write.sheet, write.cell): write.value for write in rendered_obj.writes}
    cells = {
      (entry.semantic_item_id, int(entry.period)): document.RenderedCell(
        sheet=entry.sheet,
        cell=entry.cell,
        kind=entry.cell_kind,
        formula=writes.get((entry.sheet, entry.cell)),
        tolerance=float(entry.tolerance),
        family="fixed_cell",
      )
      for entry in rendered_obj.semantic_cells
      if entry.cell_kind != "presentation"
    }
  raw_cells: list[dict[str, Any]] = []
  cell_lineages: list[CellLineage] = []
  _early_bundle_obj = (
    None
    if source_bundle is None
    else source_bundle
    if isinstance(source_bundle, SourceBundle)
    else decode_source_bundle_payload(source_bundle)
  )
  role_by_acc_tag = _statement_role_lookups(rows, _early_bundle_obj)
  # A legacy row list carries no model to give a cell its scale: it has nothing to compare.
  cited_mismatches = {} if legacy_rows else cited_fact_mismatches(rows, _early_bundle_obj, doc.model)
  for row in rows:
    cell = cells.get((row["item_id"], row["period"]))
    if cell is None:
      continue
    stamp: CellSourceStamp = row if isinstance(row, CellSourceStamp) else stamp_cell_source(row)
    source = stamp.source_dict
    kind = source.get("kind")
    # A cited line whose filed value disagrees with the authored one is not this cell's observation
    # (RULE-I1): the cell keeps what else it carries and the mismatch is the finding.
    mismatch = cited_mismatches.get((row["item_id"], row["period"]))
    observation_ids = [
      obs for obs in stamp.observation_ids if mismatch is None or obs != mismatch.observation_id
    ]
    via_kind = (
      (source.get("via") or {}).get("source_kind")
      if isinstance(source.get("via"), Mapping)
      else None
    )
    if mismatch is None and (not observation_ids or via_kind == "source_bundle_opening_balance"):
      kpi_observation = _cited_kpi_observation(source, _early_bundle_obj, row["period"]) if kind == "kpi" else None
      line = _cell_filing_line(row, kind, role_by_acc_tag) or (
        kpi_observation.line if kpi_observation is not None else None
      )
      if line is not None:
        line_obs_id = projection.filing_line_observation_id(line)
        if line_obs_id not in observation_ids:
          observation_ids.append(line_obs_id)
    # A cited input whose line this bundle carries no evidence for names no observation (RULE-I1):
    # the agent's citation stays in the cell's ``source`` and the ladder below reads what the cell
    # verifiably carries, so a cell with nothing else is ``missing_source`` until it cites a line the
    # bundle carries or declares the absence.
    is_historical_statement_row = (
      False
      if legacy_rows
      else document.is_historical_statement_cell(doc.model, str(row["item_id"]), int(row["period"]))
    )
    assumption_ids = [
      ass_id
      for ass_id in stamp.assumption_ids
      if not (is_historical_statement_row and str(ass_id).startswith(f"{ANALYST_NOTE_ARTIFACT}:"))
    ]
    lineage_refs = {
      field: list(getattr(stamp, field))
      for field in (
        "source_refs", "evidence_refs", "assumption_refs",
        "valuation_refs", "historical_driver_seed_refs",
      )
      if getattr(stamp, field)
    }
    absence_declared = (
      stamp.missing_reason == "not_applicable_evidenced"
      or source.get("derivation") in (projection.EVIDENCED_ABSENCE_DERIVATION, BLANK_CLOSURE_DERIVATION)
    )
    absence_cited = absence_declared and bool(stamp.absence_citation)
    blank_corroborated = (
      absence_declared and kind in ("absence_citation", "derivation")
      and (source.get("closure") or {}).get("verdict") == "corroborated"
    )
    if stamp.missing_reason == "not_separately_presented" or stamp.included_in_role:
      status = "compiler_derived"
    elif absence_cited or blank_corroborated or stamp.missing_reason == NOT_DISCLOSED_IN_VINTAGE:
      status = "source_absence_evidenced"
    elif observation_ids or assumption_ids or any(lineage_refs.values()):
      status = "complete"
    elif stamp.missing_reason in _MISSING_REASON_CODES:
      status = cast(Any, stamp.missing_reason)
    elif stamp.origin == "presentation":
      status = "presentation"
    elif kind in ("template", "formula", "construction_helper", "presentation_choice") or (
      (judgment_ref := document.judgment_row_ref(source)) is not None
      and judgment_ref["artifact"] != ANALYST_NOTE_ARTIFACT
    ):
      status = "compiler_derived"
    else:
      status = "missing_source"
    raw_cells.append({
      "sheet": cell.sheet,
      "cell": cell.cell,
      "semantic_item_id": row["item_id"],
      "period": row["period"],
      "origin": stamp.origin,
      "observation_ids": observation_ids,
      "assumption_ids": assumption_ids,
      **lineage_refs,
      "judgment_ref": document.judgment_row_ref(source),
      "formula": cell.formula if isinstance(cell.formula, str) and cell.formula.startswith("=") else None,
      "derivation": source if kind in ("derivation", "formula") else None,
      "lineage_status": status,
      "reason": stamp.missing_reason,
      "vintage_accession": _vintage(row),
      **({"source_history": row["source_history"]} if row.get("source_history") else {}),
    })
    if legacy_rows:
      continue
    item = doc.model.get_item(row["item_id"])
    spec = document._render_spec_for_period(doc.model, item, row["period"])
    # Explicit financial references stay financial even on an inclusion marker.
    if status == "compiler_derived" and (observation_ids or assumption_ids or any(lineage_refs.values())):
      status = "complete"
    cell_lineages.append(CellLineage(
      sheet=cell.sheet,
      cell=cell.cell,
      semantic_item_id=row["item_id"],
      formula=spec,
      observation_ids=tuple(observation_ids),
      assumption_ids=tuple(assumption_ids),
      **{field: tuple(refs) for field, refs in lineage_refs.items()},
      lineage_status=status,
      compiler_origin=CompilerOrigin(kind=stamp.compiler_origin or "mechanical_constant") if status == "compiler_derived" else None,
      declination_ids=stamp.declination_ids,
    ))
  if legacy_rows:
    return raw_cells

  model = doc.model
  source_bundle_obj = (
    None
    if source_bundle is None
    else source_bundle
    if isinstance(source_bundle, SourceBundle)
    else decode_source_bundle_payload(source_bundle)
  )
  raw_spec = model_spec if model_spec is not None else doc.model_spec
  model_spec_obj = (
    raw_spec
    if isinstance(raw_spec, ModelSpec)
    else ModelSpec.model_validate(raw_spec)
    if raw_spec is not None
    else None
  )

  items_by_role: dict[str, set[str]] = {}
  if source_bundle_obj is not None:
    for requirement in source_bundle_obj.coverage_profile.annual_requirements:
      items_by_role.setdefault(requirement.required_role, set()).add(requirement.model_item)
    # A requirement's ``model_item`` can be its role (PAYC's ``statement.*`` rows), so the sheet's lines
    # of a role come from the one role -> lines lookup, an alias landing on its home role's lines.
    from schema.required_role_catalog import load_required_role_catalog
    from schema.role_disposition import home_role, items_by_role as catalog_role_items

    catalog = load_required_role_catalog()
    role_lines = catalog_role_items(catalog, ticker=str(model.company.ticker))
    for role, lines in items_by_role.items():
      home = role_lines.get(home_role(catalog, role))
      if home is not None:
        lines.update(home.all_items())
  declinations_by_item: dict[str, list[Declination]] = {}
  for declination in (() if model_spec_obj is None else model_spec_obj.declinations):
    if declination.item_id is None:
      continue
    for named_item in {declination.item_id, *items_by_role.get(declination.item_id, ())}:
      declinations_by_item.setdefault(named_item, []).append(declination)

  def declination_ids_for(item_id: str, period: int) -> tuple[str, ...]:
    return tuple(
      sorted(
        {
          declination.declination_id
          for declination in declinations_by_item.get(item_id, ())
          if not declination.periods or period in declination.periods
        }
      )
    )

  from schema.required_role_catalog import load_required_role_catalog

  role_catalog = load_required_role_catalog()
  roles_by_face_or_pop_item: dict[str, list[str]] = {}
  for entry in role_catalog.required_roles:
    for catalog_item_id in (*entry.population_items, *entry.face_items, *entry.hff_fact_items):
      roles_by_face_or_pop_item.setdefault(catalog_item_id, []).append(entry.required_role)

  observations = (
    {}
    if source_bundle_obj is None
    else {
      observation.line: observation
      for observation in (
        *source_bundle_obj.statement_facts,
        *source_bundle_obj.kpi_observations,
      )
      if observation.status == "ok"
    }
  )
  assumptions = {
    assumption.driver: assumption
    for assumption in (() if model_spec_obj is None else model_spec_obj.forecast_assumptions)
    if assumption.assumption_id
  }
  assumption_driver_by_item_period = {
    (str(entry.get("resolved_item_id")), int(period)): str(entry.get("canonical_driver_key"))
    for entry in seed_coverage_manifest
    if entry.get("source_kind") == "model_spec_assumption"
    and entry.get("resolved_item_id")
    and entry.get("canonical_driver_key")
    for period in entry.get("periods", ())
  }
  historical_seed_refs_by_key: dict[str, HistoricalDriverSeedLineageReference] = {}
  historical_seed_keys_by_item_period: dict[tuple[str, int], tuple[str, ...]] = {}
  for entry in seed_coverage_manifest:
    if entry.get("source_kind") != "historical_driver_seed":
      continue
    item_id = str(entry.get("resolved_item_id") or "").strip()
    driver_id = str(entry.get("driver_id") or "").strip()
    driver_key = str(entry.get("driver_key") or "").strip()
    period_id = str(entry.get("period_id") or "").strip()
    if not item_id or not driver_id or not driver_key or not period_id:
      raise ValueError("historical driver seed coverage is incomplete")
    reference = HistoricalDriverSeedLineageReference.model_validate(
      {
        "historical_foundation_artifact_id": str(entry.get("historical_foundation_artifact_id") or ""),
        "historical_foundation_content_sha256": str(entry.get("historical_foundation_content_sha256") or ""),
        "historical_driver_seed_denominator_sha256": str(entry.get("historical_driver_seed_denominator_sha256") or ""),
        "driver_id": driver_id,
        "driver_key": driver_key,
        "period_id": period_id,
        "disposition": str(entry.get("disposition") or ""),
        "derivation_kind": str(entry.get("derivation_kind") or ""),
        "derivation_rationale": str(entry.get("derivation_rationale") or ""),
        "confidence": str(entry.get("confidence") or ""),
        "limitations": tuple(entry.get("limitations") or ()),
        "evidence_ids": tuple(entry.get("evidence_ids") or ()),
        "supporting_evidence_ids": tuple(entry.get("supporting_evidence_ids") or ()),
        "evidence_value_ids": tuple(entry.get("evidence_value_ids") or ()),
        "fact_ids": tuple(entry.get("fact_ids") or ()),
      }
    )
    ref_key = f"{reference.historical_foundation_artifact_id}:{reference.driver_id}:{reference.period_id}"
    existing = historical_seed_refs_by_key.get(ref_key)
    if existing is not None and existing != reference:
      raise ValueError("historical driver seed coverage identity conflicts")
    historical_seed_refs_by_key[ref_key] = reference
    periods = tuple(int(period) for period in entry.get("periods", ()))
    if len(periods) != 1:
      raise ValueError("historical driver seed coverage requires one period")
    identity = (item_id, periods[0])
    previous = historical_seed_keys_by_item_period.get(identity, ())
    historical_seed_keys_by_item_period[identity] = tuple(dict.fromkeys((*previous, ref_key)))

  historical_fact_refs_by_item_period: dict[tuple[str, int], tuple[tuple[str, str], ...]] = {}
  for entry in seed_coverage_manifest:
    if entry.get("source_kind") != "historical_foundation_fact":
      continue
    item_id = str(entry.get("resolved_item_id") or "").strip()
    fact_id = str(entry.get("fact_id") or "").strip()
    evidence_id = str(entry.get("evidence_id") or "").strip()
    periods = tuple(int(period) for period in entry.get("periods", ()))
    if not item_id or not fact_id or not evidence_id or len(periods) != 1:
      raise ValueError("historical foundation fact coverage is incomplete")
    identity = (item_id, periods[0])
    previous = historical_fact_refs_by_item_period.get(identity, ())
    historical_fact_refs_by_item_period[identity] = tuple(dict.fromkeys((*previous, (fact_id, evidence_id))))

  manifest_observation_refs_by_item_period: dict[tuple[str, int], tuple[tuple[str, str | None, str], ...]] = {}
  historical_basis_fact_by_id = (
    {}
    if source_bundle_obj is None
    else {fact.fact_id: fact for fact in source_bundle_obj.historical_basis_facts}
  )
  source_bundle_evidence_by_id = (
    {}
    if source_bundle_obj is None
    else {evidence.evidence_id: evidence for evidence in source_bundle_obj.evidence}
  )
  for entry in seed_coverage_manifest:
    source_kind = entry.get("source_kind")
    if source_kind not in {
      "business_model_node_identity",
      "source_bundle_adjusted_ebitda_bridge",
      "source_bundle_opening_balance",
      "source_bundle_schedule_disclosure",
    }:
      continue
    item_id = str(entry.get("resolved_item_id") or "").strip()
    source_id = str(entry.get("source_id") or "").strip()
    periods = tuple(int(period) for period in entry.get("periods", ()))
    if not item_id or not source_id or len(periods) != 1:
      raise ValueError("SourceBundle manifest coverage is incomplete")
    if source_kind in {"business_model_node_identity", "source_bundle_opening_balance"}:
      observation_id = str(entry.get("requirement_id") or "").strip()
      evidence_id = str(entry.get("evidence_id") or "").strip()
      evidence = source_bundle_evidence_by_id.get(evidence_id)
      if not observation_id or evidence is None or str(evidence.source_id) != source_id:
        raise ValueError(f"{source_kind} coverage differs from SourceBundle")
    else:
      observation_id = str(entry.get("fact_id") or "").strip()
      evidence_id = observation_id
      fact = historical_basis_fact_by_id.get(observation_id)
      if fact is None or str(fact.source_id) != source_id:
        raise ValueError("schedule disclosure coverage differs from SourceBundle")
    identity = (item_id, periods[0])
    previous = manifest_observation_refs_by_item_period.get(identity, ())
    manifest_observation_refs_by_item_period[identity] = tuple(
      dict.fromkeys((*previous, (observation_id, evidence_id, source_id)))
    )

  document_observations_by_item_period: dict[tuple[str, int], tuple[str, ...]] = {}
  document_assumption_by_item_period: dict[tuple[str, int], str] = {}
  document_mechanical: set[tuple[str, int]] = set()
  document_absence: set[tuple[str, int]] = set()
  for entry in seed_coverage_manifest:
    source_kind = entry.get("source_kind")
    if source_kind not in {
      "document_filing_line",
      "document_judgment",
      "document_construction",
      "document_absence",
    }:
      continue
    item_id = str(entry.get("resolved_item_id") or "").strip()
    periods = tuple(int(period) for period in entry.get("periods", ()))
    if not item_id or len(periods) != 1:
      continue
    identity = (item_id, periods[0])
    if source_kind == "document_filing_line":
      observation_id = str(entry.get("observation_id") or "").strip()
      if observation_id:
        prev_obs = document_observations_by_item_period.get(identity, ())
        document_observations_by_item_period[identity] = tuple(dict.fromkeys((*prev_obs, observation_id)))
    elif source_kind == "document_judgment":
      assumption_id = str(entry.get("assumption_id") or "").strip()
      if assumption_id:
        document_assumption_by_item_period[identity] = assumption_id
    elif source_kind == "document_construction":
      document_mechanical.add(identity)
    else:
      document_absence.add(identity)

  evidence_by_id = dict(source_bundle_evidence_by_id)
  sealed_evidence_ids_by_observation_id: dict[str, list[str]] = {}
  for sealed_evidence in source_bundle_evidence_by_id.values():
    sealed_evidence_ids_by_observation_id.setdefault(
      projection.filing_line_observation_id(sealed_evidence.line), [],
    ).append(sealed_evidence.evidence_id)
  fact_projection = None if model_spec_obj is None else model_spec_obj.historical_foundation_fact_projection
  if fact_projection is not None:
    for observation in fact_projection.observations:
      # The accepted artifact remains authoritative; the shared compile finding
      # blocks publication until re-acceptance reconciles the two copies.
      evidence_by_id[observation.evidence.evidence_id] = observation.evidence
  hff_observation_by_fact_id = (
    {}
    if fact_projection is None
    else {observation.fact.fact_id: observation for observation in fact_projection.observations}
  )
  seed_projection = None if model_spec_obj is None else model_spec_obj.historical_driver_seed_projection
  hff_seed_by_driver_id = (
    {}
    if seed_projection is None
    else {str(seed.driver_id): seed for seed in seed_projection.seeds}
  )
  hff_seed_by_driver_key = (
    {}
    if seed_projection is None
    else {str(seed.driver_key): seed for seed in seed_projection.seeds}
  )
  valuation_observations_by_id = (
    {}
    if source_bundle_obj is None
    else {item.observation_id: item for item in source_bundle_obj.valuation_observations}
  )
  valuation_ids_by_item: dict[str, tuple[str, ...]] = {}
  valuation_policy_source_ids_by_item: dict[str, tuple[str, ...]] = {}
  #: The observations a ValuationPolicy row cites, by the cell its judgment sets. Those
  #: cells are written by construction (they carry a ``doc.semantic`` stamp), so unlike
  #: ``valuation_ids_by_item`` this is not gated on the cell having none.
  valuation_policy_evidence_by_item: dict[str, tuple[str, ...]] = {}
  if source_bundle_obj is not None:
    market_requirements = {
      requirement.metric: requirement
      for requirement in source_bundle_obj.valuation_requirements
      if requirement.metric in {"quote", "beta", "risk_free_10y", "erp"}
    }
    if set(market_requirements) == {"quote", "beta", "risk_free_10y", "erp"}:
      market_inputs = derive_covered_market_inputs(
        source_bundle_obj.valuation_observations,
        source_bundle_obj.cutoff_at,
        source_bundle_obj.market_calendar,
        source_bundle_obj.fiscal_calendar,
        requirements=source_bundle_obj.valuation_requirements,
        focal_ticker=source_bundle_obj.coverage_profile.focal_ticker,
        sources=source_bundle_obj.sources,
        retrieved_at=source_bundle_obj.retrieved_at,
      )
      for field_name, item_id in {
        "stock_price": "tpl.v.current_valuation.stock_price",
        "raw_beta": "tpl.v.cost_of_equity.raw_beta",
        "risk_free_rate": "tpl.v.cost_of_equity.risk_free_rate",
        "equity_risk_premium": "tpl.v.cost_of_equity.equity_risk_premium",
      }.items():
        resolved = getattr(market_inputs, field_name)
        if resolved.status == "resolved" and resolved.observation_id is not None:
          valuation_ids_by_item[item_id] = (resolved.observation_id,)

  authored_controls: dict[str, AssumptionLineageReference] = {}
  authored_control_ids_by_item: dict[str, tuple[str, ...]] = {}
  for authorization in (() if model_spec_obj is None else model_spec_obj.valuation_family_authorizations):
    control_id = f"valuation_family_authorization.{authorization.method}"
    authored_controls[control_id] = AssumptionLineageReference(
      assumption_id=control_id,
      rationale=authorization.rationale,
    )
    if authorization.method == "forward_pe":
      item_ids = (
        "tpl.s.valuation_scenarios.bull_selected_pe",
        "tpl.s.valuation_scenarios.base_selected_pe",
        "tpl.s.valuation_scenarios.bear_selected_pe",
        "tpl.s.valuation_scenarios.bull_pe_multiple",
        "tpl.s.valuation_scenarios.base_pe_multiple",
        "tpl.s.valuation_scenarios.bear_pe_multiple",
        "tpl.v.forward_pe.method_price",
      )
    else:
      item_ids = (
        "tpl.s.valuation_scenarios.bull_selected_ev_ebitda",
        "tpl.s.valuation_scenarios.base_selected_ev_ebitda",
        "tpl.s.valuation_scenarios.bear_selected_ev_ebitda",
        "tpl.s.valuation_scenarios.bull_ev_ebitda_multiple",
        "tpl.s.valuation_scenarios.base_ev_ebitda_multiple",
        "tpl.s.valuation_scenarios.bear_ev_ebitda_multiple",
        "tpl.v.forward_ev_ebitda.method_multiple",
        "tpl.v.forward_ev_ebitda.method_implied_ev",
        "tpl.v.forward_ev_ebitda.method_implied_equity",
        "tpl.v.forward_ev_ebitda.method_price",
      )
    for item_id in item_ids:
      valuation_ids_by_item[item_id] = authorization.evidence_ids
      authored_control_ids_by_item[item_id] = (control_id,)

  for control in (() if model_spec_obj is None else model_spec_obj.valuation_controls):
    if model_spec_obj is not None and model_spec_obj.valuation_policy_binding is not None:
      policy_source_ids = tuple(dict.fromkeys(control.source_refs))
      for item_id in (
        "tpl.v.current_valuation.stock_price",
        "tpl.v.cost_of_equity.raw_beta",
        "tpl.v.cost_of_equity.risk_free_rate",
        "tpl.v.cost_of_equity.equity_risk_premium",
        "tpl.v.cost_of_equity.beta",
        "tpl.v.cost_of_equity.cost_of_equity",
        "tpl.v.wacc.sofr_rate",
      ):
        valuation_policy_source_ids_by_item[item_id] = tuple(
          dict.fromkeys((*valuation_policy_source_ids_by_item.get(item_id, ()), *policy_source_ids))
        )
      # The cells the policy's own judgment sets cite the observations the policy
      # row cites (its peer or macro evidence), not the market inputs it observed.
      policy_row = model_spec_obj.valuation_policy
      terminal_evidence = () if policy_row is None else policy_row.terminal_evidence_ids
      beta_floor_evidence = () if policy_row is None else policy_row.beta_floor_evidence_ids
      valuation_policy_evidence_by_item["tpl.v.cost_of_equity.beta_floor"] = beta_floor_evidence
      for item_id in (
        "tpl.v.dcf.terminal_growth_bull",
        "tpl.v.dcf.terminal_growth_base",
        "tpl.v.dcf.terminal_growth_bear",
        "tpl.v.dcf.terminal_growth_rate",
        "tpl.v.dcf.exit_multiple_bull",
        "tpl.v.dcf.exit_multiple_base",
        "tpl.v.dcf.exit_multiple_bear",
      ):
        valuation_policy_evidence_by_item[item_id] = terminal_evidence
      if control.rationale:
        control_id = f"valuation_control.{control.method}"
        authored_controls[control_id] = AssumptionLineageReference(
          assumption_id=control_id,
          rationale=control.rationale,
        )
        for item_id in (
          "tpl.v.cost_of_equity.raw_beta",
          "tpl.v.cost_of_equity.risk_free_rate",
          "tpl.v.cost_of_equity.equity_risk_premium",
          "tpl.v.cost_of_equity.beta_floor",
          "tpl.v.cost_of_equity.beta",
          "tpl.v.cost_of_equity.cost_of_equity",
          "tpl.v.wacc.sofr_rate",
          "tpl.v.dcf.terminal_growth_bull",
          "tpl.v.dcf.terminal_growth_base",
          "tpl.v.dcf.terminal_growth_bear",
          "tpl.v.dcf.terminal_growth_rate",
          "tpl.v.dcf.exit_multiple_bull",
          "tpl.v.dcf.exit_multiple_base",
          "tpl.v.dcf.exit_multiple_bear",
        ):
          authored_control_ids_by_item[item_id] = tuple(
            dict.fromkeys((*authored_control_ids_by_item.get(item_id, ()), control_id))
          )
    control_observation_ids = (
      ()
      if source_bundle_obj is None
      else tuple(
        observation.observation_id
        for observation in source_bundle_obj.valuation_observations
        if str(observation.source_id) in set(control.source_refs)
      )
    )
    if not control_observation_ids:
      continue
    for item_id in ("tpl.v.wacc.wacc",):
      valuation_ids_by_item[item_id] = tuple(
        dict.fromkeys((*valuation_ids_by_item.get(item_id, ()), *control_observation_ids))
      )

  authored_control_ids_by_identity: dict[tuple[str, int], tuple[str, ...]] = {}
  valuation_ids_by_identity: dict[tuple[str, int], tuple[str, ...]] = {}
  net_share_dilution = (
    None
    if model_spec_obj is None or model_spec_obj.valuation_policy is None
    else model_spec_obj.valuation_policy.net_share_dilution
  )
  if net_share_dilution is not None:
    control_id = "valuation_policy.net_share_dilution"
    authored_controls[control_id] = AssumptionLineageReference(
      assumption_id=control_id,
      rationale=net_share_dilution.rationale,
    )
    for row in net_share_dilution.rates:
      rate_identity = ("tpl.a.tax_net_income.y_y_chg", row.fiscal_year)
      authored_control_ids_by_identity[rate_identity] = (control_id,)
      valuation_ids_by_identity[rate_identity] = net_share_dilution.evidence_ids

  coverage_by_id = (
    {}
    if source_bundle_obj is None
    else {coverage.requirement_id: coverage for coverage in source_bundle_obj.coverage}
  )
  requirements_by_model_item: dict[str, list[AnnualSourceRequirement]] = {}
  requirements_by_role: dict[str, list[AnnualSourceRequirement]] = {}
  if source_bundle_obj is not None:
    for requirement in source_bundle_obj.requirements:
      requirements_by_model_item.setdefault(requirement.model_item, []).append(requirement)
      requirements_by_role.setdefault(requirement.required_role, []).append(requirement)

  def requirements_for_id(item_id: str) -> tuple[AnnualSourceRequirement, ...]:
    matched: list[AnnualSourceRequirement] = list(requirements_by_model_item.get(item_id, ()))
    item_obj = model._index.get(item_id)
    role = None if item_obj is None else getattr(item_obj, "required_role", None)
    if isinstance(role, str) and role:
      matched.extend(requirements_by_role.get(role, ()))
    for cat_role in roles_by_face_or_pop_item.get(item_id, ()):
      matched.extend(requirements_by_role.get(cat_role, ()))
    return tuple(dict.fromkeys(matched))

  def selected_bound_line(requirement: AnnualSourceRequirement, period_id: str) -> Any | None:
    if period_id not in requirement.period_ids:
      return None
    cell = requirement.cell_for_period(period_id)
    if cell.binding_state != "bound":
      return None
    return cell.pinned_ref

  def cited_inclusion_for(item_id: str, period: int) -> tuple[AnnualSourceRequirement, PeriodPinCell] | None:
    period_id = f"FY{period}"
    for requirement in requirements_for_id(item_id):
      if period_id not in requirement.period_ids:
        continue
      cell = requirement.cell_for_period(period_id)
      if cell.binding_state == "not_separately_presented":
        return requirement, cell
    return None

  def cited_inclusion_source_ids(item_id: str, period: int) -> tuple[str, ...]:
    inclusion = cited_inclusion_for(item_id, period)
    if inclusion is None:
      return ()
    requirement, _ = inclusion
    coverage = coverage_by_id.get(requirement.requirement_id)
    if coverage is None:
      return ()
    return tuple(
      str(outcome.disclosure_search_source_id)
      for outcome in coverage.periods
      if outcome.period_id == f"FY{period}"
      and outcome.disposition == "not_separately_presented"
      and outcome.disclosure_search_source_id is not None
    )

  def evidence_ids_for(
    item_id: str,
    period: int,
    observation: Any | None,
    extra_lines: Sequence[Any] = (),
  ) -> tuple[str, ...]:
    if source_bundle_obj is None:
      return ()
    period_ids = {f"FY{period}", f"FY{period}E", str(period)}
    bound_requirements = requirements_for_id(item_id)
    extra_set = {line for line in extra_lines if line is not None}
    bound_lines = {
      line
      for requirement in bound_requirements
      for period_id in period_ids
      if (line := selected_bound_line(requirement, period_id)) is not None
    } | extra_set
    if observation is not None:
      bound_lines.add(observation.line)
    resolved: list[str] = []
    for requirement in bound_requirements:
      coverage = coverage_by_id.get(requirement.requirement_id)
      if coverage is None:
        continue
      for outcome in coverage.periods:
        if outcome.period_id in period_ids:
          resolved.extend(outcome.evidence_ids)
    for evidence in source_bundle_obj.evidence:
      if evidence.line in bound_lines and (
        evidence.period_id in period_ids or evidence.line in extra_set
      ):
        resolved.append(evidence.evidence_id)
    return tuple(dict.fromkeys(resolved))

  def evidence_reference(evidence_id: str) -> EvidenceLineageReference:
    fact = historical_basis_fact_by_id.get(evidence_id)
    if fact is not None:
      return EvidenceLineageReference(
        evidence_id=fact.fact_id,
        source_id=fact.source_id,
        issuer="" if source_bundle_obj is None else source_bundle_obj.issuer,
        provider=fact.provider,
        locator=fact.locator,
        retrieved_at=fact.retrieved_at,
        as_of_date=fact.as_of_date,
        confidence=fact.confidence,
        accession=fact.line.accession,
        statement_role=fact.line.statement_role,
        tag=fact.line.tag,
      )
    evidence = evidence_by_id[evidence_id]
    return EvidenceLineageReference(
      evidence_id=evidence.evidence_id,
      source_id=evidence.source_id,
      issuer=evidence.issuer,
      provider=evidence.provider,
      locator=evidence.locator,
      retrieved_at=evidence.retrieved_at,
      as_of_date=evidence.as_of_date,
      confidence=evidence.confidence,
      derivation_input_ids=evidence.derivation_input_ids,
      accession=evidence.line.accession,
      statement_role=evidence.line.statement_role,
      tag=evidence.line.tag,
      preferred_label_text=evidence.label,
      preferred_label_role_uri=evidence.preferred_label_role_uri,
      preferred_label_negates_value=evidence.preferred_label_negates_value,
    )

  forecast_assumptions_by_id: dict[str, Any] = {}
  for forecast_assumption in assumptions.values():
    forecast_assumptions_by_id.setdefault(str(forecast_assumption.assumption_id), forecast_assumption)

  # An analyst note the document minted an assumption for (``document.analyst_note_judgment``): the
  # rationale is the note, held on the cell's own ``judgment_row``.
  analyst_notes: dict[str, str] = {}
  analyst_note_source_refs: dict[str, tuple[str, ...]] = {}
  for (sem_item_id, sem_period), column in doc.semantic.items():
    if document.is_historical_statement_cell(model, str(sem_item_id), int(sem_period)):
      continue
    judgment = document.judgment_row_ref(column.get("source"))
    row = None if judgment is None or judgment["artifact"] != ANALYST_NOTE_ARTIFACT else judgment["row"]
    if isinstance(row, Mapping) and row.get("assumption_id") and isinstance(row.get("rationale"), str):
      ass_id_str = str(row["assumption_id"])
      analyst_notes[ass_id_str] = row["rationale"]
      raw_row_refs = row.get("source_refs")
      analyst_note_source_refs[ass_id_str] = document.extract_analyst_note_handles(
        row["rationale"],
        raw_row_refs if isinstance(raw_row_refs, Sequence) and not isinstance(raw_row_refs, (str, bytes)) else None,
      )
  from schema.control_snapshot_projection import (
    AUTHENTICATED_SCENARIO_PROJECTION_NOTE,
    CANONICAL_SCENARIO_RENDER_TARGETS,
    CanonicalProjectionScenario,
    canonical_scenario_projection_owner_or_none,
  )
  from schema.driver_resolver import resolve_driver_key
  from schema.scenario_bindings import scenario_case_gaps

  projection_override_items: dict[tuple[str, str], set[str]] = {}
  assumption_items_by_driver: dict[str, set[str]] = {}
  for entry in seed_coverage_manifest:
    source_kind = entry.get("source_kind")
    resolved_id = entry.get("resolved_item_id")
    if not resolved_id:
      continue
    if source_kind == "projection_override" and entry.get("control_id") and entry.get("scenario"):
      projection_override_items.setdefault(
        (str(entry["scenario"]).strip().lower(), str(entry["control_id"])), set(),
      ).add(str(resolved_id))
    elif source_kind == "model_spec_assumption" and entry.get("canonical_driver_key"):
      assumption_items_by_driver.setdefault(str(entry["canonical_driver_key"]), set()).add(str(resolved_id))
  for (sem_item_id, _sem_period), column in doc.semantic.items():
    judgment = document.judgment_row_ref(column.get("source")) if isinstance(column, Mapping) else None
    row = None if judgment is None else judgment["row"]
    if isinstance(row, Mapping):
      if row.get("driver"):
        assumption_items_by_driver.setdefault(str(row["driver"]), set()).add(str(sem_item_id))
      if row.get("assumption_id"):
        assumption_items_by_driver.setdefault(str(row["assumption_id"]), set()).add(str(sem_item_id))

  from schema.build_company_grounded_schedules import scale_aggregate_sbc_scenario_values
  from schema.scenario_bridge import normalize_control_magnitude

  canonical_table_by_item: dict[str, Mapping[CanonicalProjectionScenario, str]] = {}
  for _anchor_id, case_targets in CANONICAL_SCENARIO_RENDER_TARGETS.items():
    for leaf_id in case_targets.values():
      canonical_table_by_item[leaf_id] = case_targets
    base_leaf = case_targets["base"]
    if base_leaf in model._index:
      try:
        owner_id = canonical_scenario_projection_owner_or_none(model, base_leaf)
      except ValueError:
        owner_id = None
      if owner_id is not None:
        canonical_table_by_item[owner_id] = case_targets

  projection_years = {int(p) for p in model.time_structure.projection_periods}
  unresolved_scenario_cases = (
    frozenset(gap.scenario for gap in scenario_case_gaps(model_spec_obj))
    if model_spec_obj is not None
    else frozenset()
  )
  base_review_overrides = {
    str(override.assumption_id): override
    for review in (() if model_spec_obj is None else model_spec_obj.scenario_reviews)
    if review.scenario_name.strip().lower() == "base"
    for override in review.overrides
  }

  def _override_period_values(raw_value: Any, period_ids: Sequence[str]) -> dict[int, float] | None:
    if isinstance(raw_value, Mapping):
      vals: dict[int, float] = {}
      for k, v in raw_value.items():
        try:
          yr = int(str(k).removeprefix("FY").removesuffix("E"))
          num = float(v)
        except (TypeError, ValueError):
          return None
        if math.isfinite(num):
          vals[yr] = num
      if projection_years and (set(vals) & projection_years) != projection_years:
        return None
      return vals
    try:
      scalar = float(raw_value)
    except (TypeError, ValueError):
      return None
    if not math.isfinite(scalar):
      return None
    return {
      int(str(pid).removeprefix("FY").removesuffix("E")): scalar
      for pid in period_ids
    }

  def _base_assumption_period_value(assumption_obj: Any, period: int) -> float | None:
    ass_id = str(assumption_obj.assumption_id)
    base_ov = base_review_overrides.get(ass_id)
    if base_ov is not None:
      base_ov_vals = _override_period_values(base_ov.value, base_ov.period_ids)
      if base_ov_vals is not None and period in base_ov_vals:
        return base_ov_vals[period]
    period_values = getattr(assumption_obj, "period_values", None)
    if isinstance(period_values, Mapping):
      raw = period_values.get(period, period_values.get(str(period)))
      if raw is not None:
        try:
          return float(raw)
        except (TypeError, ValueError):
          pass
    scalar = getattr(assumption_obj, "value", None)
    if scalar is not None:
      try:
        return float(scalar)
      except (TypeError, ValueError):
        pass
    return None

  scenario_overrides: dict[tuple[str, str, int], Any] = {}
  held_at_base_table_source: dict[tuple[str | None, str, int], str] = {}
  for review in (() if model_spec_obj is None else model_spec_obj.scenario_reviews):
    raw_case = review.scenario_name.strip().lower()
    if raw_case == "bull":
      case: CanonicalProjectionScenario = "bull"
    elif raw_case == "bear":
      case = "bear"
    else:
      continue
    if case in unresolved_scenario_cases:
      continue
    for override in review.overrides:
      ass_id = str(override.assumption_id)
      assumption_obj = forecast_assumptions_by_id.get(ass_id)
      if assumption_obj is None:
        continue
      ov_values = _override_period_values(override.value, override.period_ids)
      if ov_values is None:
        continue
      selector = str(override.target_control_id)
      raw_candidates: list[str] = [
        *projection_override_items.get((case, selector), ()),
        *assumption_items_by_driver.get(selector, ()),
        *assumption_items_by_driver.get(ass_id, ()),
      ]
      if selector in model._index:
        raw_candidates.append(selector)
      try:
        resolved_key = resolve_driver_key(selector)
      except Exception:
        resolved_key = None
      if resolved_key is not None and resolved_key in model._index:
        raw_candidates.append(resolved_key)
      candidate_items = list(dict.fromkeys(raw_candidates))
      resolved_case_items: list[str] = []
      table_pairs: list[tuple[str, str]] = []
      base_control_items: list[str] = []
      for cand_id in candidate_items:
        table_targets = canonical_table_by_item.get(cand_id)
        if cand_id not in resolved_case_items:
          resolved_case_items.append(cand_id)
        if table_targets is not None:
          base_leaf = table_targets["base"]
          case_leaf = table_targets[case]
          if case_leaf not in resolved_case_items:
            resolved_case_items.append(case_leaf)
          if (case_leaf, base_leaf) not in table_pairs:
            table_pairs.append((case_leaf, base_leaf))
          if base_leaf not in base_control_items:
            base_control_items.append(base_leaf)
          for owner_cand, targets_cand in canonical_table_by_item.items():
            if targets_cand == table_targets and owner_cand not in resolved_case_items:
              resolved_case_items.append(owner_cand)
        elif cand_id not in base_control_items:
          base_control_items.append(cand_id)
      normalized_ov_by_base: dict[str, dict[int, float]] = {}
      for base_id in base_control_items:
        norm_vals = {
          int(p): normalize_control_magnitude(base_id, float(v))
          for p, v in ov_values.items()
        }
        if base_id in model._index:
          norm_vals = scale_aggregate_sbc_scenario_values(model.get_item(base_id), norm_vals)
        normalized_ov_by_base[base_id] = norm_vals
      for period, ov_val in ov_values.items():
        base_val: float | None = None
        cmp_ov_val = ov_val
        if isinstance(ours, Mapping):
          for base_id in base_control_items:
            cell_val = ours.get((base_id, int(period)))
            if cell_val is None:
              continue
            raw_cell_val = getattr(cell_val, "value", cell_val)
            cell_state = getattr(cell_val, "state", "known")
            if (
              cell_state == "known"
              and isinstance(raw_cell_val, (int, float))
              and math.isfinite(float(raw_cell_val))
            ):
              base_val = float(raw_cell_val)
              cmp_ov_val = normalized_ov_by_base.get(base_id, {}).get(int(period), ov_val)
              break
        if base_val is None:
          base_val = _base_assumption_period_value(assumption_obj, period)
        is_held_at_base = (
          base_val is not None and math.isclose(cmp_ov_val, base_val, rel_tol=0.0, abs_tol=1e-12)
        )
        if is_held_at_base:
          for case_leaf, base_leaf in table_pairs:
            leaf_item = model._index.get(case_leaf)
            leaf_val = leaf_item.values.values.get(period) if leaf_item is not None and leaf_item.values is not None else None
            if leaf_val is not None and leaf_val.note == AUTHENTICATED_SCENARIO_PROJECTION_NOTE:
              held_at_base_table_source[(case, case_leaf, int(period))] = base_leaf
        else:
          for resolved_item_id in resolved_case_items:
            scenario_overrides[(case, resolved_item_id, int(period))] = override

  scenario_assumptions = {
    f"{SCENARIO_ASSUMPTION_PREFIX}{case}:{override.assumption_id}": override
    for (case, _, _), override in scenario_overrides.items()
  }

  def assumption_reference(assumption_id: str, period: int) -> AssumptionLineageReference | None:
    override = scenario_assumptions.get(assumption_id)
    if override is not None:
      base_assumption = forecast_assumptions_by_id.get(str(override.assumption_id))
      return AssumptionLineageReference(
        assumption_id=assumption_id,
        rationale=override.rationale,
        confidence=(
          str(base_assumption.confidence)
          if base_assumption is not None and base_assumption.confidence is not None
          else None
        ),
        evidence_ids=override.evidence_ids,
      )
    control = authored_controls.get(assumption_id)
    if control is not None:
      return control
    note = analyst_notes.get(assumption_id)
    if note is not None:
      return AssumptionLineageReference(
        assumption_id=assumption_id,
        rationale=note,
        source_refs=analyst_note_source_refs.get(assumption_id, ()),
      )
    assumption = forecast_assumptions_by_id.get(assumption_id)
    if assumption is None:
      return None
    return AssumptionLineageReference(
      assumption_id=assumption_id,
      rationale=assumption.rationale or "Analyst-authored model assumption.",
      confidence=(str(assumption.confidence) if assumption.confidence is not None else None),
      evidence_ids=assumption.evidence_ids,
      source_refs=tuple(assumption.source_refs_by_period.get(period, assumption.source_refs)),
    )

  declared_origin_kinds = (
    {}
    if source_bundle_obj is None
    else _declared_compiler_origins(_declared_capabilities(source_bundle_obj))
  )
  lag_gaps = lag_input_gap_readers(model, source_bundle_obj)
  presentation_metadata_ids = {
    item.id
    for sheet in model.sheets.values()
    for section in sheet.sections
    for item in section.line_items
    if section.id == "header" or item.id in _WORKBOOK_METADATA_ITEM_IDS
  }
  period_by_cell: dict[tuple[str, str], int] = {}
  state_by_cell: dict[tuple[str, str], str] = {}
  for entry in getattr(rendered_obj, "semantic_cells", ()):
    if (
      getattr(entry, "cell_kind", None) == "presentation"
      and getattr(entry, "semantic_item_id", None) == "presentation"
    ):
      continue
    coord = (str(entry.sheet), str(entry.cell).upper())
    period_by_cell[coord] = int(entry.period)
    state_by_cell[coord] = str(entry.expected_state)
  if rendered_obj is None or hasattr(rendered_obj, "cells"):
    for key, cell_entry in cells.items():
      period_key = key[1]
      coord = (str(cell_entry.sheet), str(cell_entry.cell).upper())
      period_by_cell.setdefault(coord, int(period_key))
  historical_periods = tuple(int(period) for period in model.time_structure.historical_periods)
  historical_periods_set = set(historical_periods)
  def direct_declarations_for(
    item_id: str,
    period: int,
  ) -> tuple[
    tuple[str, ...],
    tuple[str, ...],
    tuple[str, ...],
    tuple[str, ...],
    tuple[str, ...],
    tuple[str, ...],
  ]:
    if cited_inclusion_for(item_id, period) is not None:
      return (), (), (), (), (), cited_inclusion_source_ids(item_id, period)
    # A cell ``cited_fact_mismatches`` flags is not evidenced by the line it cites, on any path: its
    # observation id and that line's evidence are derived once here from the helper's result and
    # withheld from every declaration below (the statement binding, the semantic source, the seed
    # manifest). The cell keeps whatever else it carries; the mismatch is the finding.
    mismatch = cited_mismatches.get((item_id, period))
    withheld_line = None if mismatch is None else mismatch.line
    withheld_observation_ids = frozenset(() if mismatch is None else (mismatch.observation_id,))
    withheld_evidence_ids = frozenset(
      ()
      if mismatch is None or source_bundle_obj is None
      else (ev.evidence_id for ev in source_bundle_obj.evidence if ev.line == mismatch.line)
    )
    obs_list: list[str] = []
    ass_list: list[str] = []
    ev_list: list[str] = []
    val_list: list[str] = []
    seed_keys: list[str] = []
    src_list: list[str] = []
    for candidate_id in (item_id,):
      cand_identity = (candidate_id, period)
      sem_col = doc.semantic.get(cand_identity)
      sem_source = (sem_col or {}).get("source") if isinstance(sem_col, Mapping) else None
      # A ``filing_line`` source names the line it cites by its ``ref`` whether or not the door also
      # stamped the canonical ``observation_id`` on it, so that line joins the cell's evidence either
      # way; the stamped id is the same ``filing-line:`` identity the join then finds the sealed
      # ``SourceEvidence`` row for.
      sem_line = (
        _filing_line_ref(
          sem_source,
          statement_role_by_accession_tag=role_by_acc_tag,
        )
        if isinstance(sem_source, Mapping) and sem_source.get("kind") == "filing_line"
        else None
      )
      if isinstance(sem_source, Mapping) and sem_source.get("kind") == "kpi":
        kpi_observation = None if mismatch is not None else _cited_kpi_observation(sem_source, source_bundle_obj, period)
        sem_line = kpi_observation.line if kpi_observation is not None else None
        src_list.extend(_kpi_source_ids(sem_line, source_bundle_obj))
      bound_lines = [
        line
        for requirement in requirements_for_id(candidate_id)
        if (line := selected_bound_line(requirement, f"FY{period}")) is not None
      ]
      if sem_line is not None and sem_line not in bound_lines:
        bound_lines.append(sem_line)
      bound_lines = [line for line in bound_lines if line != withheld_line]
      observation = next((observations[line] for line in bound_lines if line in observations), None)
      if observation is not None:
        obs_list.append(projection.filing_line_observation_id(observation.line))
      elif sem_line is not None:
        obs_list.append(projection.filing_line_observation_id(sem_line))
      assumption_driver = assumption_driver_by_item_period.get(cand_identity, candidate_id)
      assumption = assumptions.get(assumption_driver)
      if assumption is not None:
        ass_list.append(str(assumption.assumption_id))
        ev_list.extend(assumption.evidence_ids)
      sem_jref = document.judgment_row_ref(sem_source)
      doc_ass = document_assumption_by_item_period.get(cand_identity) or (
        str(sem_jref["ref"])
        if sem_jref is not None and sem_jref["artifact"] == ANALYST_NOTE_ARTIFACT and sem_jref.get("ref")
        else None
      )
      is_analyst_note_override = bool(doc_ass and str(doc_ass).startswith(f"{ANALYST_NOTE_ARTIFACT}:"))
      if not is_analyst_note_override:
        ass_list.extend(authored_control_ids_by_item.get(candidate_id, ()))
      ass_list.extend(authored_control_ids_by_identity.get(cand_identity, ()))
      ev_list.extend(
        evidence_ids_for(
          candidate_id,
          period,
          observation,
          extra_lines=(sem_line,) if sem_line is not None else (),
        )
      )
      if isinstance(sem_source, Mapping):
        for ev_key in ("evidence_id", "evidence_ref"):
          ev_val = sem_source.get(ev_key)
          if isinstance(ev_val, str) and ev_val:
            ev_list.append(ev_val)
      for fact_id, evidence_id in historical_fact_refs_by_item_period.get(cand_identity, ()):
        if evidence_id in withheld_evidence_ids:
          continue
        obs_list.append(fact_id)
        ev_list.append(evidence_id)
      if cand_identity not in doc.semantic:
        val_list.extend(valuation_ids_by_item.get(candidate_id, ()))
      if not is_analyst_note_override:
        val_list.extend(valuation_policy_evidence_by_item.get(candidate_id, ()))
        src_list.extend(valuation_policy_source_ids_by_item.get(candidate_id, ()))
      val_list.extend(valuation_ids_by_identity.get(cand_identity, ()))
      seed_keys.extend(historical_seed_keys_by_item_period.get(cand_identity, ()))
      for (
        manifest_observation_id,
        manifest_evidence_id,
        manifest_source_id,
      ) in manifest_observation_refs_by_item_period.get(cand_identity, ()):
        if manifest_evidence_id in withheld_evidence_ids:
          continue
        obs_list.append(manifest_observation_id)
        if manifest_evidence_id is not None:
          ev_list.append(manifest_evidence_id)
        src_list.append(manifest_source_id)
      sem_judgment = (
        sem_source.get("row")
        if isinstance(sem_source, Mapping)
        and sem_source.get("kind") == "judgment_row"
        and isinstance(sem_source.get("row"), Mapping)
        else None
      )
      if (
        sem_judgment is not None
        and isinstance(sem_source, Mapping)
        and sem_source.get("artifact") == "business_model"
        and str(sem_judgment.get("disposition") or "") in {"", "observed", "derived"}
        and mismatch is None
      ):
        raw_citations = [c for c in (sem_judgment.get("citations") or ()) if isinstance(c, Mapping)]
        cited_observations = [
          obs
          for citation in raw_citations
          if (obs := _cited_kpi_observation(citation, source_bundle_obj, period)) is not None
        ]
        authored = document.operand_value(ours.get(cand_identity))
        if raw_citations and len(cited_observations) == len(raw_citations) and authored is not None:
          item = model.get_item(candidate_id)
          cited_total = sum(
            (quantity_native_value(observation.fact.value) / Decimal(10) ** document._line_exponent(item) for observation in cited_observations),
            Decimal(0),
          )
          if abs(float(cited_total) - float(authored)) <= CITED_FACT_AGREEMENT_ABS:
            for kpi_obs in cited_observations:
              obs_list.append(projection.filing_line_observation_id(kpi_obs.line))
              src_list.extend(_kpi_source_ids(kpi_obs.line, source_bundle_obj))
        elif len(raw_citations) == 1 and authored is not None:
          occ_ev = _cited_occurrence_evidence(
            raw_citations[0],
            source_bundle_obj,
            period,
            model.get_item(candidate_id),
            authored,
          )
          if occ_ev is not None:
            obs_list.append(projection.filing_line_observation_id(occ_ev.line))
            ev_list.append(occ_ev.evidence_id)
            src_list.append(str(occ_ev.source_id))
      sem_fact_ids = (
        tuple(str(fid).strip() for fid in (sem_judgment.get("fact_ids") or ()) if str(fid).strip())
        if sem_judgment is not None
        else ()
      )
      sem_disposition = (
        str(sem_judgment.get("disposition") or "").strip()
        if sem_judgment is not None
        else ""
      )
      own_fact_ids = () if sem_disposition == "derived" else sem_fact_ids
      if (
        sem_judgment is not None
        and seed_projection is not None
        and period in historical_periods_set
        and isinstance(sem_source, Mapping)
        and sem_source.get("artifact") == "historical_foundation"
      ):
        seed_driver_id = str(sem_judgment.get("driver_id") or "").strip()
        seed_driver_key = str(sem_judgment.get("driver_key") or "").strip()
        matched_seed = hff_seed_by_driver_id.get(seed_driver_id) or hff_seed_by_driver_key.get(seed_driver_key)
        if matched_seed is not None and bool(sem_fact_ids):
          seed_period_id = str(sem_judgment.get("period_id") or f"FY{period}").strip()
          ref_key = f"{seed_projection.historical_foundation_artifact_id}:{matched_seed.driver_id}:{seed_period_id}"
          if ref_key not in historical_seed_refs_by_key:
            historical_seed_refs_by_key[ref_key] = HistoricalDriverSeedLineageReference.model_validate(
              {
                "historical_foundation_artifact_id": seed_projection.historical_foundation_artifact_id,
                "historical_foundation_content_sha256": seed_projection.historical_foundation_content_sha256,
                "historical_driver_seed_denominator_sha256": seed_projection.denominator_sha256,
                "driver_id": str(matched_seed.driver_id),
                "driver_key": str(matched_seed.driver_key),
                "period_id": seed_period_id,
                "disposition": sem_disposition if sem_disposition in get_args(HistoricalDriverSeedDisposition) else str(matched_seed.disposition),
                "derivation_kind": str(matched_seed.derivation_kind),
                "derivation_rationale": str(matched_seed.derivation_rationale),
                "confidence": str(matched_seed.confidence),
                "limitations": tuple(matched_seed.limitations),
                "evidence_ids": tuple(matched_seed.evidence_ids) if seed_period_id == matched_seed.period_id else (),
                "supporting_evidence_ids": tuple(matched_seed.supporting_evidence_ids),
                "evidence_value_ids": tuple(matched_seed.evidence_value_ids) if seed_period_id == matched_seed.period_id else (),
                "fact_ids": sem_fact_ids,
              }
            )
          seed_keys.append(ref_key)
      for doc_obs_id in dict.fromkeys((*own_fact_ids, *document_observations_by_item_period.get(cand_identity, ()))):
        if doc_obs_id in hff_observation_by_fact_id:
          hff_obs = hff_observation_by_fact_id[doc_obs_id]
          obs_list.append(projection.filing_line_observation_id(hff_obs.fact.line))
          obs_list.append(doc_obs_id)
          ev_list.append(hff_obs.evidence.evidence_id)
          src_list.append(str(hff_obs.evidence.source_id))
        elif doc_obs_id in historical_basis_fact_by_id:
          basis_fact = historical_basis_fact_by_id[doc_obs_id]
          obs_list.append(projection.filing_line_observation_id(basis_fact.line))
          obs_list.append(doc_obs_id)
          matched_evs = evidence_ids_for(candidate_id, period, None, extra_lines=(basis_fact.line,))
          ev_list.extend(matched_evs or (basis_fact.fact_id,))
          src_list.append(str(basis_fact.source_id))
        elif doc_obs_id in sealed_evidence_ids_by_observation_id:
          obs_list.append(doc_obs_id)
          ev_list.extend(sealed_evidence_ids_by_observation_id[doc_obs_id])
        # A document observation id that is no sealed evidence line, historical foundation fact or
        # basis fact names nothing the bundle can verify: it is not admitted as this cell's observation.
      if doc_ass and not (
        document.is_historical_statement_cell(model, str(candidate_id), int(period))
        and str(doc_ass).startswith(f"{ANALYST_NOTE_ARTIFACT}:")
      ):
        ass_list.append(doc_ass)
        src_list.extend(analyst_note_source_refs.get(str(doc_ass), ()))
    return (
      tuple(obs for obs in dict.fromkeys(obs_list) if obs not in withheld_observation_ids),
      tuple(dict.fromkeys(ass_list)),
      tuple(
        ev_id
        for ev_id in dict.fromkeys(ev_list)
        if (ev_id in evidence_by_id or ev_id in historical_basis_fact_by_id)
        and ev_id not in withheld_evidence_ids
      ),
      tuple(
        val_id
        for val_id in dict.fromkeys(val_list)
        if val_id in valuation_observations_by_id
      ),
      tuple(
        ref_key
        for ref_key in dict.fromkeys(seed_keys)
        if ref_key in historical_seed_refs_by_key
      ),
      tuple(dict.fromkeys(src_list)),
    )

  def cited_absence_in_history(item_id: str) -> bool:
    """Whether every historical period of the line is a cited absence or a cited inclusion elsewhere.

    A written cell carries its citation on its semantic column; a cited inclusion the
    schedule rows take from the pin is the model's own marker with the citation in its params.
    """
    item = model._index.get(item_id)

    def cited(period: int) -> bool:
      marker = None if item is None else (item.overrides or {}).get(period)
      return _cites_absence(doc.semantic.get((item_id, period))) or (
        marker is not None
        and marker.subtype == "not_separately_presented"
        and bool(marker.params.get("absence_citation"))
      )

    return bool(historical_periods) and all(cited(period) for period in historical_periods)

  def cited_absence_source_ids(item_id: str, period: int | None = None) -> tuple[str, ...]:
    return tuple(dict.fromkeys(
      str(outcome.disclosure_search_source_id)
      for requirement in requirements_for_id(item_id)
      if (coverage := coverage_by_id.get(requirement.requirement_id)) is not None
      for outcome in coverage.periods
      if (period is None or outcome.period_id == f"FY{period}")
      and outcome.disposition in ("not_applicable_evidenced", "not_separately_presented")
      and outcome.disclosure_search_source_id is not None
    ))

  def evidenced_zero_dependencies(spec: Any) -> tuple[str, ...] | None:
    """The cited lines an explicit-zero forecast derives from, when each is cited absent in all its history.

    A zero the schedule policy writes names the lines it rests on in its derivation. It is
    sourced when every one of them is cited absent or included elsewhere in every historical
    period; a zero resting on no cited line is not sourced and keeps its compiler status.
    """
    derivation = getattr(spec, "derivation", None)
    if (
      spec is None
      or spec.type is not FormulaType.constant
      or derivation is None
      or derivation.owner != "build_company_grounded_schedules"
      or not derivation.dependencies
    ):
      return None
    if all(cited_absence_in_history(dependency) for dependency in derivation.dependencies):
      return tuple(derivation.dependencies)
    return None

  enriched: list[CellLineage] = []
  emitted_coords: set[tuple[str, str]] = {
    (str(lineage.sheet), str(lineage.cell).upper()) for lineage in cell_lineages
  }
  raw_writes = getattr(rendered_obj, "writes", ())
  writes_by_coord: dict[tuple[str, str], Any] = (
    {(str(sheet), str(cell).upper()): value for (sheet, cell), value in raw_writes.items()}
    if isinstance(raw_writes, Mapping)
    else {(str(write.sheet), str(write.cell).upper()): write.value for write in raw_writes}
  )
  all_lineages = list(cell_lineages)
  if hasattr(rendered_obj, "writes") and (not hasattr(rendered_obj, "cells") or source_bundle_obj is not None):
    for sheet_name, cell_coord, item_obj, period_int, synth_spec in _semantic_cells(model, rendered_obj):
      coord = (str(sheet_name), str(cell_coord).upper())
      if coord in emitted_coords:
        continue
      emitted_coords.add(coord)
      period_by_cell.setdefault(coord, int(period_int))
      all_lineages.append(
        CellLineage(
          sheet=sheet_name,
          cell=cell_coord,
          semantic_item_id=item_obj.id,
          formula=synth_spec,
          lineage_status="compiler_derived",
          compiler_origin=CompilerOrigin(kind="mechanical_constant"),
        )
      )
  for lineage in all_lineages:
    coord = (str(lineage.sheet), str(lineage.cell).upper())
    period = period_by_cell.get(coord)
    if period is None:
      enriched.append(lineage)
      continue
    item_id = str(lineage.semantic_item_id)
    (
      decl_obs,
      decl_ass,
      decl_ev,
      decl_val,
      decl_seeds,
      decl_srcs,
    ) = direct_declarations_for(item_id, period)
    obs_ids = tuple(dict.fromkeys((*lineage.observation_ids, *decl_obs)))
    ass_ids = tuple(dict.fromkeys((*lineage.assumption_ids, *decl_ass)))
    existing_ev_by_id = {ref.evidence_id: ref for ref in lineage.evidence_refs}
    declared_ev_refs: list[EvidenceLineageReference] = []
    for ev_id in decl_ev:
      if ev_id not in existing_ev_by_id:
        ev_ref = evidence_reference(ev_id)
        existing_ev_by_id[ev_id] = ev_ref
        declared_ev_refs.append(ev_ref)
    evidence_refs = tuple(existing_ev_by_id.values())
    existing_ass_by_id = {ref.assumption_id: ref for ref in lineage.assumption_refs}
    declared_ass_refs: list[AssumptionLineageReference] = []
    for ass_id in ass_ids:
      if ass_id not in existing_ass_by_id:
        resolved_ref = assumption_reference(ass_id, period)
        if resolved_ref is not None:
          existing_ass_by_id[ass_id] = resolved_ref
          declared_ass_refs.append(resolved_ref)
    assumption_refs = tuple(existing_ass_by_id.values())
    existing_val_by_id = {ref.observation_id: ref for ref in lineage.valuation_refs}
    declared_val_refs: list[ValuationLineageReference] = []
    for val_id in decl_val:
      if val_id not in existing_val_by_id:
        obs_obj = valuation_observations_by_id[val_id]
        val_ref = ValuationLineageReference(
          observation_id=obs_obj.observation_id,
          source_id=obs_obj.source_id,
          raw_ref=obs_obj.raw_ref,
          provider=obs_obj.provider,
          as_of_close=obs_obj.as_of_close,
          raw_market_date=obs_obj.raw_market_date,
          snapshot_manifest_sha256=obs_obj.snapshot_manifest_sha256,
        )
        existing_val_by_id[val_id] = val_ref
        declared_val_refs.append(val_ref)
    valuation_refs = tuple(existing_val_by_id.values())
    existing_seeds = list(lineage.historical_driver_seed_refs)
    existing_seed_keys = {
      f"{ref.historical_foundation_artifact_id}:{ref.driver_id}:{ref.period_id}"
      for ref in existing_seeds
    }
    for ref_key in decl_seeds:
      if ref_key not in existing_seed_keys:
        existing_seeds.append(historical_seed_refs_by_key[ref_key])
        existing_seed_keys.add(ref_key)
    historical_driver_seed_refs = tuple(existing_seeds)
    source_refs = tuple(
      dict.fromkeys(
        [
          *lineage.source_refs,
          *(ref.source_id for ref in declared_ev_refs),
          *(ref.source_id for ref in declared_val_refs),
          *decl_srcs,
          *(src_ref for ref in declared_ass_refs for src_ref in ref.source_refs),
        ]
      )
    )
    numerically_sourced_seed_refs = tuple(
      ref for ref in historical_driver_seed_refs if ref.evidence_ids or ref.fact_ids
    )
    has_financial_origins = bool(
      obs_ids
      or ass_ids
      or evidence_refs
      or assumption_refs
      or valuation_refs
      or numerically_sourced_seed_refs
      or source_refs
    )
    status = lineage.lineage_status
    comp_orig = lineage.compiler_origin
    spec = lineage.formula
    if (
      spec is None
      and item_id in _COMPILED_LITERAL_CONTROL_IDS
      and coord in writes_by_coord
    ):
      spec = FormulaSpec(
        type=FormulaType.constant,
        params={"value": writes_by_coord[coord]},
      )
    has_declarations = (
      source_bundle_obj is not None
      or model_spec_obj is not None
      or bool(seed_coverage_manifest)
    )
    nsp_marker = (
      has_declarations
      and spec is not None
      and getattr(spec, "type", None) is FormulaType.raw
      and getattr(spec, "subtype", None) == "not_separately_presented"
    )
    lag_gap = lag_gaps.get((item_id, period)) if not has_financial_origins else None
    evidenced_zero = evidenced_zero_dependencies(spec)
    if (
      cited_inclusion_for(item_id, period) is not None
      or nsp_marker
      or (item_id, period) in document_absence
      or status == "source_absence_evidenced"
      or evidenced_zero is not None
    ):
      status = "source_absence_evidenced"
      comp_orig = None
      if (item_id, period) in document_absence or status == "source_absence_evidenced" or evidenced_zero is not None:
        source_refs = tuple(dict.fromkeys((
          *source_refs,
          *cited_absence_source_ids(item_id, period),
          *(source for dependency in (evidenced_zero or ()) for source in cited_absence_source_ids(dependency)),
        )))
    elif lag_gap is not None and lag_gap.code == "source_absence_evidenced":
      status = "source_absence_evidenced"
      comp_orig = None
    elif lag_gap is not None and lag_gap.code == "missing_source":
      status = "missing_source"
      comp_orig = None
    elif historical_driver_seed_refs and not has_financial_origins:
      status = "missing_source"
      comp_orig = None
    elif has_financial_origins:
      status = "complete"
      comp_orig = None
    elif str(item_id).startswith("doc.") and (item_id, period) in document_mechanical:
      status = "compiler_derived"
      comp_orig = comp_orig or CompilerOrigin(kind="mechanical_constant")
    elif (item_id, period) not in doc.semantic and source_bundle_obj is not None:
      declared_kind = declared_origin_kinds.get(item_id)
      cell_state = state_by_cell.get(coord, "known")
      capabilities = source_bundle_obj.coverage_profile.capabilities
      feature_off = capabilities is not None and not capabilities.compiler_origin_classifications
      if declared_kind is not None and (
        cell_state == "known" or declared_kind == "presentation_scaffold_value"
      ):
        status = "compiler_derived"
        comp_orig = CompilerOrigin(kind=declared_kind)
      elif item_id in REGISTRY_SCAFFOLD and not feature_off:
        status = "compiler_derived"
        comp_orig = CompilerOrigin(kind="presentation_scaffold_value")
      elif item_id in REGISTRY_DISCOUNT and cell_state == "known" and not feature_off:
        status = "compiler_derived"
        comp_orig = CompilerOrigin(kind="discount_convention")
      elif item_id in REGISTRY_SCENARIO and cell_state == "known" and not feature_off:
        status = "compiler_derived"
        comp_orig = CompilerOrigin(kind="scenario_control")
      elif item_id in presentation_metadata_ids:
        status = "presentation"
        comp_orig = None
      elif declared_kind is not None:
        status = "missing_source"
        comp_orig = None
      elif item_id.startswith("tpl.v.") and feature_off:
        status = "missing_source"
        comp_orig = None
      else:
        item_obj = model._index.get(item_id)
        unstamped_valuation_input = (
          item_id.startswith("tpl.v.")
          and (item_id, period) not in document_mechanical
          and item_obj is not None and item_obj.item_type is ItemType.input
          and (spec is None or (spec.type is FormulaType.constant and spec.derivation is None))
        )
        if unstamped_valuation_input:
          status = "missing_source"
          comp_orig = None
        elif (
          status == "compiler_derived"
          or (item_id, period) in document_mechanical
          or (spec is not None and (spec.type is not FormulaType.constant or spec.derivation is not None))
          or (item_obj is not None and item_obj.item_type is ItemType.derived)
        ):
          status = "compiler_derived"
          comp_orig = comp_orig or CompilerOrigin(kind="mechanical_constant")
        else:
          status = "missing_source"
          comp_orig = None
    enriched.append(
      CellLineage(
        sheet=lineage.sheet,
        cell=lineage.cell,
        semantic_item_id=item_id,
        formula=spec,
        observation_ids=obs_ids,
        assumption_ids=ass_ids,
        source_refs=source_refs,
        evidence_refs=evidence_refs,
        assumption_refs=assumption_refs,
        valuation_refs=valuation_refs,
        historical_driver_seed_refs=historical_driver_seed_refs,
        lineage_status=status,
        compiler_origin=comp_orig,
        declination_ids=tuple(dict.fromkeys((*lineage.declination_ids, *declination_ids_for(item_id, period)))),
      )
    )

  # Append unwritten evidenced-absence cells from SourceBundle.diagnostics
  if source_bundle_obj is not None:
    historical_years = {int(p) for p in model.time_structure.historical_periods}
    projection_years = {int(p) for p in model.time_structure.projection_periods}
    rendered_periods = historical_years | projection_years
    absent_identities: set[tuple[str, int]] = set()
    for diagnostic in source_bundle_obj.diagnostics:
      absent_period = re.fullmatch(r"FY(\d{4})", str(diagnostic.period_id or ""))
      if (
        diagnostic.code != "source_absence_evidenced"
        or absent_period is None
        or int(absent_period.group(1)) not in rendered_periods
      ):
        continue
      for absent_item_id in items_by_role.get(str(diagnostic.semantic_item_id or ""), ()):
        absent_identities.add((absent_item_id, int(absent_period.group(1))))
    if absent_identities:
      for sheet_name, sheet in model.sheets.items():
        layout = sheet.layout
        mapper = AbsoluteColumnMapper(
          model.time_structure,
          layout.first_data_column if layout and layout.first_data_column else "B",
          period_scope=layout.period_scope if layout else "all",
        )
        for section in sheet.sections:
          for item in section.line_items:
            if item.column is not None:
              continue
            for period in mapper.all_periods():
              if (str(item.id), int(period)) not in absent_identities:
                continue
              cell_coord = f"{mapper.col_for_period(period)}{item.row}".upper()
              if (sheet_name, cell_coord) in emitted_coords:
                continue
              period_by_cell[(sheet_name, cell_coord)] = int(period)
              enriched.append(
                CellLineage(
                  sheet=sheet_name,
                  cell=cell_coord,
                  semantic_item_id=item.id,
                  lineage_status="source_absence_evidenced",
                  declination_ids=declination_ids_for(item.id, int(period)),
                )
              )

  if evaluation_reads is None:
    from schema.dependency_graph import DependencyGraph, formula_owned_recompute_ids

    eval_model = model.model_copy(deep=True)
    eval_model.build_index()
    document.lower_optional_absences(doc, eval_model)
    eval_graph = DependencyGraph()
    eval_graph.build(eval_model)
    eval_recompute = formula_owned_recompute_ids(eval_model)
    document._formula_owned_snapshots(eval_graph, eval_recompute)
    absences, evidenced_zeros = document.cell_dispositions(doc)
    eval_graph.compute_typed(
      {}, recompute=eval_recompute, propagate_roots=set(),
      absences=absences, evidenced_zeros=evidenced_zeros,
    )
    evaluation_reads = eval_graph.last_eval_reads

  if case_evaluation_reads is None and any(
    lineage.formula is not None
    and lineage.formula.subtype == "scenario_snapshot"
    and lineage.formula.params.get("case") in {"bull", "bear"}
    for lineage in enriched
  ):
    from schema.build_scenarios import compute_scenario_outputs

    scenario_outputs, _ = compute_scenario_outputs(
      model.model_copy(deep=True),
      unresolved_cases=unresolved_scenario_cases,
    )
    case_evaluation_reads = scenario_outputs.case_eval_reads

  if evaluation_reads or case_evaluation_reads:
    direct_payloads: dict[tuple[str, int], dict[str, Any]] = {}
    snapshot_cases: dict[tuple[str, int], str | None] = {}
    for lineage in enriched:
      coord = (str(lineage.sheet), str(lineage.cell).upper())
      period = period_by_cell.get(coord)
      if period is None:
        continue
      spec = lineage.formula
      if spec is not None and spec.subtype == "scenario_snapshot":
        snap_case = str(spec.params.get("case"))
        snapshot_cases[(str(lineage.semantic_item_id), period)] = (
          snap_case if snap_case in {"bull", "bear"} and snap_case in (case_evaluation_reads or {}) else None
        )
      direct_payloads[(str(lineage.semantic_item_id), period)] = {
        "observation_ids": set(str(value) for value in lineage.observation_ids),
        "source_refs": set(str(value) for value in lineage.source_refs),
        "evidence_refs": {ref.evidence_id: ref for ref in lineage.evidence_refs},
        "assumption_ids": set(str(value) for value in lineage.assumption_ids),
        "assumption_refs": {ref.assumption_id: ref for ref in lineage.assumption_refs},
        "valuation_refs": {ref.observation_id: ref for ref in lineage.valuation_refs},
        "seed_refs": {
          f"{ref.historical_foundation_artifact_id}:{ref.driver_id}:{ref.period_id}": ref
          for ref in lineage.historical_driver_seed_refs
        },
      }

    def _merge_payload(target: dict[str, Any], source: Mapping[str, Any] | None) -> None:
      if source is None:
        return
      target["observation_ids"].update(source["observation_ids"])
      target["source_refs"].update(source["source_refs"])
      target["evidence_refs"].update(source["evidence_refs"])
      target["assumption_ids"].update(source["assumption_ids"])
      target["assumption_refs"].update(source["assumption_refs"])
      target["valuation_refs"].update(source["valuation_refs"])
      target["seed_refs"].update(source["seed_refs"])

    payload_memo: dict[tuple[str | None, str, int], dict[str, Any]] = {}
    payload_active: set[tuple[str | None, str, int]] = set()

    def _read_payload(item_id: str, period: int, case: str | None = None) -> dict[str, Any]:
      cell_identity = (item_id, int(period))
      case = snapshot_cases.get(cell_identity, case)
      identity = (case, item_id, int(period))
      if identity in payload_memo:
        return payload_memo[identity]
      if identity in payload_active:
        active_payload = {
          "observation_ids": set(),
          "source_refs": set(),
          "evidence_refs": {},
          "assumption_ids": set(),
          "assumption_refs": {},
          "valuation_refs": {},
          "seed_refs": {},
        }
        if case is None or (case, item_id, int(period)) not in scenario_overrides:
          _merge_payload(active_payload, direct_payloads.get(cell_identity))
        return active_payload
      payload_active.add(identity)
      payload = {
        "observation_ids": set(),
        "source_refs": set(),
        "evidence_refs": {},
        "assumption_ids": set(),
        "assumption_refs": {},
        "valuation_refs": {},
        "seed_refs": {},
      }
      try:
        override = scenario_overrides.get((case, item_id, int(period))) if case is not None else None
        if override is not None:
          # A case input is a root: neither its base value's citations nor its
          # base formula's reads obtained the overridden value.
          assumption_id = f"{SCENARIO_ASSUMPTION_PREFIX}{case}:{override.assumption_id}"
          payload["assumption_ids"].add(assumption_id)
          ref = assumption_reference(assumption_id, period)
          if ref is not None:
            payload["assumption_refs"][assumption_id] = ref
          for ev_id in override.evidence_ids:
            if ev_id in evidence_by_id or ev_id in historical_basis_fact_by_id:
              ev_ref = evidence_reference(ev_id)
              payload["evidence_refs"][ev_id] = ev_ref
              payload["source_refs"].add(str(ev_ref.source_id))
        else:
          base_table_leaf = held_at_base_table_source.get(identity)
          if base_table_leaf is not None:
            _merge_payload(payload, _read_payload(base_table_leaf, int(period), case))
          else:
            _merge_payload(payload, direct_payloads.get(cell_identity))
          reads = evaluation_reads if case is None else (case_evaluation_reads or {}).get(case)
          cell_reads = (reads or {}).get(cell_identity)
          if cell_reads is None and cell_identity in snapshot_cases:
            cell_reads = (evaluation_reads or {}).get(cell_identity, ())
          for dependency_id, dependency_period in cell_reads or ():
            _merge_payload(payload, _read_payload(str(dependency_id), int(dependency_period), case))
      finally:
        payload_active.discard(identity)
      payload_memo[identity] = payload
      return payload

    propagated: list[CellLineage] = []
    for lineage in enriched:
      coord = (str(lineage.sheet), str(lineage.cell).upper())
      period = period_by_cell.get(coord)
      item_id = str(lineage.semantic_item_id)
      if period is None or lineage.lineage_status in {"source_absence_evidenced", "presentation"}:
        propagated.append(lineage)
        continue
      payload = _read_payload(item_id, period)
      obs_ids = tuple(dict.fromkeys((*lineage.observation_ids, *sorted(payload["observation_ids"]))))
      ass_ids = tuple(dict.fromkeys((*lineage.assumption_ids, *sorted(payload["assumption_ids"]))))
      source_refs = tuple(dict.fromkeys((*lineage.source_refs, *sorted(payload["source_refs"]))))
      evidence_refs = tuple(
        {ref.evidence_id: ref for ref in (*lineage.evidence_refs, *payload["evidence_refs"].values())}.values()
      )
      assumption_refs = tuple(
        {ref.assumption_id: ref for ref in (*lineage.assumption_refs, *payload["assumption_refs"].values())}.values()
      )
      valuation_refs = tuple(
        {ref.observation_id: ref for ref in (*lineage.valuation_refs, *payload["valuation_refs"].values())}.values()
      )
      seed_refs = tuple(
        {
          f"{ref.historical_foundation_artifact_id}:{ref.driver_id}:{ref.period_id}": ref
          for ref in (*lineage.historical_driver_seed_refs, *payload["seed_refs"].values())
        }.values()
      )
      numerically_sourced = any(ref.evidence_ids or ref.fact_ids for ref in seed_refs)
      if (
        obs_ids
        or ass_ids
        or source_refs
        or evidence_refs
        or assumption_refs
        or valuation_refs
        or numerically_sourced
      ):
        status = "complete"
        comp_orig = None
      elif seed_refs:
        status = "missing_source"
        comp_orig = None
      else:
        status = lineage.lineage_status
        comp_orig = lineage.compiler_origin
      if (
        status == lineage.lineage_status
        and comp_orig == lineage.compiler_origin
        and obs_ids == lineage.observation_ids
        and ass_ids == lineage.assumption_ids
        and source_refs == lineage.source_refs
        and evidence_refs == lineage.evidence_refs
        and assumption_refs == lineage.assumption_refs
        and valuation_refs == lineage.valuation_refs
        and seed_refs == lineage.historical_driver_seed_refs
      ):
        propagated.append(lineage)
        continue
      propagated.append(
        CellLineage(
          sheet=lineage.sheet,
          cell=lineage.cell,
          semantic_item_id=item_id,
          formula=lineage.formula,
          observation_ids=obs_ids,
          assumption_ids=ass_ids,
          source_refs=source_refs,
          evidence_refs=evidence_refs,
          assumption_refs=assumption_refs,
          valuation_refs=valuation_refs,
          historical_driver_seed_refs=seed_refs,
          lineage_status=status,
          compiler_origin=comp_orig,
          declination_ids=lineage.declination_ids,
        )
      )
    enriched = propagated
  return ProvenanceMap(
    source_bundle_digest=source_bundle_digest,
    model_spec_digest=model_spec_digest,
    cells=tuple(enriched),
  )


def snapshot_provenance_map(payload: Mapping[str, Any]) -> Any:
  from schema.build_model_product_contracts import CellLineage, ProvenanceMap

  cell_fields = set(CellLineage.model_fields)
  return ProvenanceMap.model_validate({
    "schema_version": payload.get("schema_version", "1.1"),
    "source_bundle_digest": payload["source_bundle_digest"],
    "model_spec_digest": payload["model_spec_digest"],
    "cells": [
      {key: value for key, value in cell.items() if key in cell_fields}
      for cell in payload.get("cells") or ()
      if cell.get("published_cell", True)
    ],
  })


def compare_into(snapshot: Path, doc: document.ModelSheet, refusals: _Refusals, soffice: str | None) -> dict[str, Any] | None:
  """Render + native compare into ``snapshot``: a run's first write there. The files an earlier
  run left in the directory are removed first, so everything in it is this run's — a run stopped
  at the native comparison leaves its workbook and no manifest, never a mix that publish or
  lookup would read as a snapshot."""
  for path in snapshot.iterdir():
    if path.is_file():
      path.unlink()
  return refusals.run("native_compare", lambda: document.ours_vs_native(doc, snapshot, WORKBOOK_STEM, soffice=soffice))


def write_snapshot(
  snapshot: Path,
  doc: document.ModelSheet,
  ours: document.Values,
  compared: Mapping[str, Any],
  check: Mapping[str, Any],
  findings: Mapping[str, Any],
  manifest: Mapping[str, Any],
  *,
  vintage_changes: Mapping[str, Any] | None = None,
  source_bundle: Any | None = None,
  source_bundle_content_sha256: str | None = None,
  model_spec: Any | None = None,
  template: FinancialModel | None = None,
  scenario_controls: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
  """The snapshot files of ``doc``. ``vintage_changes`` (a refresh or advance: its cutoff and
  ``restated_cells`` / ``revintaged_cells``) rides in ``provenance.json`` beside the cells."""
  rendered: document.Rendered = compared["_rendered"]
  document.save(doc, snapshot / DOCUMENT_NAME, values=ours)
  if template is not None:
    (snapshot / "template.json").write_text(template.model_dump_json(indent=1))
  if scenario_controls is not None:
    (snapshot / "scenario_controls.json").write_text(json.dumps(scenario_controls, indent=1))
  rows = semantic_rows(doc, ours)
  keyed_dump.write_jsonl(snapshot / "sources.jsonl", rows)
  matrix: dict[str, dict[int, Any]] = {}
  for (item_id, period), value in ours.items():
    matrix.setdefault(item_id, {})[period] = value
  keyed_dump.write_jsonl(snapshot / "values.jsonl", keyed_dump.dump_values(doc.model, matrix))
  (snapshot / "semantic.json").write_text(json.dumps([
    {
      "semantic_item_id": item_id, "period": period, "sheet": cell.sheet, "cell": cell.cell,
      "cell_kind": cell.kind, "value": document.value_row(ours[(item_id, period)]) if (item_id, period) in ours else None,
    }
    for (item_id, period), cell in sorted(rendered.cells.items())
  ], indent=1, default=str))
  from model_build import publish
  from schema.render_state import compiler_input_content_hash
  from schema.versioned_artifact import canonical_json_bytes

  eff_spec = model_spec if model_spec is not None else doc.model_spec
  if source_bundle is None:
    bundle_digest = "0" * 64
  else:
    assert source_bundle_content_sha256 is not None  # the row ``source_bundle`` was decoded from
    bundle_digest = source_bundle_content_sha256
  spec_digest = (
    "0" * 64
    if eff_spec is None
    else compiler_input_content_hash(eff_spec).removeprefix("sha256:")
  )
  seed_rows = (
    publish._document_seed_rows(doc, doc.model)
    if source_bundle is not None or eff_spec is not None
    else ()
  )
  projected_rendered: Any = rendered
  case_eval_reads: Mapping[str, Mapping[tuple[str, int], tuple[tuple[str, int], ...]]] | None = None
  if source_bundle is not None and eff_spec is not None:
    from schema.build_scenarios import compute_scenario_outputs, scenario_results_payload
    from schema.formatter import formatter_for_declared_style_guide
    from schema.model_kind_dispatch import declared_capabilities, declared_style_guide
    from schema.model_readiness import compute_model_projection_readiness
    from schema.model_readiness_quality import compute_model_quality_readiness
    from schema.model_readiness_scenario_bridge import compute_model_scenario_bridge_readiness
    from schema.model_readiness_valuation import compute_valuation_input_readiness
    from schema.renderer import reconcile_render_contract, render_model
    from schema.renderer_annual_context import (
      _annual_render_context,
      _append_valuation_evidence_status_block,
    )
    from schema.renderer_types import AnnualRenderContext
    from schema.scenario_bindings import scenario_case_gaps
    from schema.valuation_evidence_derivations import derive_valuation_evidence_status

    rendered_model = doc.model.model_copy(deep=True)
    rendered_model.build_index()
    computed_values: dict[str, dict[int, float]] = {}
    for (item_id, period), value in ours.items():
      if value.state == "known":
        computed_values.setdefault(item_id, {})[period] = float(value.value)
    unresolved_cases = frozenset(gap.scenario for gap in scenario_case_gaps(eff_spec))
    scenario_outputs, _ = compute_scenario_outputs(
      rendered_model.model_copy(deep=True), unresolved_cases=unresolved_cases,
    )
    case_eval_reads = scenario_outputs.case_eval_reads
    scenario_results = scenario_results_payload(scenario_outputs, unresolved_cases)
    projection_readiness = compute_model_projection_readiness(
      rendered_model, canonical_control_curves=(), computed_values=computed_values, guard_config=None,
    )
    scenario_bridge_readiness = compute_model_scenario_bridge_readiness(rendered_model)
    valuation_input_readiness = compute_valuation_input_readiness(rendered_model, computed_values=computed_values)
    model_quality_readiness = compute_model_quality_readiness(
      rendered_model, computed_values=computed_values,
      valuation_input_readiness=valuation_input_readiness, guard_config=None,
    )
    from schema.build_offline_validation import owned_cell_gaps

    cell_gaps = owned_cell_gaps(rendered_model, source_bundle)
    annual_context = _annual_render_context(
      model=rendered_model,
      source_bundle=source_bundle,
      computed_values=computed_values,
      projection_readiness=projection_readiness,
      model_quality_readiness=model_quality_readiness,
      scenario_bridge_readiness=scenario_bridge_readiness,
      scenario_results=scenario_results,
      version_identity=None,
      predecessor_semantic_document=None,
    )
    render_context = AnnualRenderContext(
      version_label=annual_context.version_label,
      version_number=annual_context.version_number,
      predecessor_version_number=annual_context.predecessor_version_number,
      required_unavailable=annual_context.required_unavailable
      | frozenset(
        key for key, value in ours.items()
        if value.state != "known" and publish._rendered_gap(doc, rendered_model, key, cell_gaps)
      ),
    )
    document.lower_optional_absences(doc, rendered_model)
    document._materialize_semantic_raw(rendered_model)
    formatter = formatter_for_declared_style_guide(declared_style_guide(declared_capabilities(source_bundle)))
    projected_rendered = render_model(rendered_model, formatter=formatter, annual_context=render_context)
    vp = getattr(eff_spec, "valuation_policy", None)
    _append_valuation_evidence_status_block(
      projected_rendered,
      derive_valuation_evidence_status(
        source_bundle, None if vp is None else vp.peer_policy, bundle_content_sha256=bundle_digest,
      ),
    )
    reconcile_render_contract(rendered_model, projected_rendered, formatter=formatter, annual_context=render_context)
  provenance_map = project_provenance_map(
    doc,
    ours,
    projected_rendered,
    source_bundle_digest=bundle_digest,
    model_spec_digest=spec_digest,
    source_bundle=source_bundle,
    model_spec=eff_spec,
    seed_coverage_manifest=seed_rows,
    case_evaluation_reads=case_eval_reads,
  )
  provenance_map_digest = hashlib.sha256(
    canonical_json_bytes(provenance_map.model_dump(mode="json"))
  ).hexdigest()
  snapshot_map = (
    provenance_map
    if projected_rendered is rendered
    else project_provenance_map(
      doc,
      ours,
      rendered,
      source_bundle_digest=bundle_digest,
      model_spec_digest=spec_digest,
      source_bundle=source_bundle,
      model_spec=eff_spec,
      seed_coverage_manifest=seed_rows,
      case_evaluation_reads=case_eval_reads,
    )
  )
  published_by_coord = {
    (str(cell.sheet), str(cell.cell).upper()): cell
    for cell in provenance_map.cells
  }
  snapshot_by_coord = {
    (str(cell.sheet), str(cell.cell).upper()): cell
    for cell in snapshot_map.cells
  }
  raw_by_coord = {
    (str(raw["sheet"]), str(raw["cell"]).upper()): raw
    for raw in _provenance(rows, rendered)
  }
  ordered_coords = list(snapshot_by_coord) + [
    coord for coord in published_by_coord if coord not in snapshot_by_coord
  ]
  snapshot_cells: list[dict[str, Any]] = []
  for coord in ordered_coords:
    in_published = coord in published_by_coord
    cell_lineage = published_by_coord[coord] if in_published else snapshot_by_coord[coord]
    raw = raw_by_coord.get(coord) or {}
    dumped = cell_lineage.model_dump(mode="json")
    cell_row: dict[str, Any] = {
      **dumped,
      "period": raw.get("period"),
      "origin": raw.get("origin"),
      "judgment_ref": raw.get("judgment_ref"),
      "derivation": raw.get("derivation"),
      "reason": raw.get("reason"),
      "vintage_accession": raw.get("vintage_accession"),
      **({"published_cell": False} if not in_published else {}),
      **({"source_history": raw["source_history"]} if raw.get("source_history") else {}),
    }
    snapshot_cells.append(cell_row)
  provenance = {
    "schema_version": provenance_map.schema_version,
    "source_bundle_digest": provenance_map.source_bundle_digest,
    "model_spec_digest": provenance_map.model_spec_digest,
    "provenance_map_digest": provenance_map_digest,
    "cells": snapshot_cells,
    **dict(vintage_changes or {}),
  }
  (snapshot / "provenance.json").write_text(json.dumps(provenance, indent=1, default=str))
  compare_rows = document.compare_rows(compared["_ours"], compared["_native"], rendered.cells)
  public = {key: value for key, value in compared.items() if not key.startswith("_")}
  (snapshot / "compare.json").write_text(json.dumps({**public, "rows": compare_rows}, indent=1, default=str))
  (snapshot / "validation.json").write_text(json.dumps(check, indent=1, default=str))
  (snapshot / "findings.json").write_text(json.dumps(findings, indent=1, default=str))
  files = sorted(path for path in snapshot.iterdir() if path.is_file() and path.name != "manifest.json")
  full = {
    **manifest, "engine": "DependencyGraph (document.recalc); native LibreOffice is the comparison",
    "files": {path.name: _sha256(path) for path in files},
  }
  (snapshot / "manifest.json").write_text(json.dumps(full, indent=1, default=str))
  return full


# --------------------------------------------------------------------------
# completeness (the document against a product version)
# --------------------------------------------------------------------------


_FAMILIES = (
  ("tpl.fm.income_statement.", "income_statement"),
  ("tpl.fm.balance_sheet.", "balance_sheet"),
  ("tpl.fm.cash_flow.", "cash_flow"),
  ("tpl.v.", "valuation"),
)
#: The valuation outputs reported by value (fixed cells: their one anchor period).
VALUATION_OUTPUTS = (
  "tpl.v.dcf.equity_value_per_share",
  "tpl.v.dcf.dcf_price",
  "tpl.v.forward_pe.method_price",
  "tpl.v.forward_ev_ebitda.method_price",
  "tpl.v.blended_target.blended_price",
)


def _family(item_id: str) -> str:
  return next((name for prefix, name in _FAMILIES if item_id.startswith(prefix)), "other")


def _agrees(value: document.Value | None, expected: float) -> bool:
  return value is not None and value.state == "known" and math.isclose(float(value.value), expected, rel_tol=1e-6, abs_tol=1e-6)


def _reference_note(model: Any, item_id: str, period: int) -> str | None:
  """What produced the reference's cell, as its own model records it: the override's or the
  period spec's note, else the value cell's provenance and note."""
  if model is None:
    return None
  item = model._index.get(item_id)
  if item is None:
    return "line_absent_in_reference"
  override = (item.overrides or {}).get(period)
  if override is not None:
    return f"override:{override.note or ''}"
  spec = document._render_spec_for_period(model, item, period)
  if spec is not None and spec.type is not FormulaType.constant:
    return f"{spec.type.value}:{spec.note or ''}"
  cell = None if item.values is None else item.values.values.get(period)
  if cell is not None:
    return f"value:{getattr(cell.provenance, 'value', cell.provenance)}:{cell.note or ''}"
  return None if spec is None else f"constant:{spec.note or ''}"


def _root_class(doc: document.ModelSheet, value: document.Value) -> tuple[str, dict[str, Any]]:
  """Why a cell is still unavailable: its first cause's semantic column, classified."""
  if isinstance(value, DependencyGraphKnownNumberValue) or not value.causes:
    return f"unattributed:{value.state}", {}
  cause = value.causes[0]
  key = (cause.node_id, int(cause.period.fiscal_year))
  column = doc.semantic.get(key) or {}
  root = {"item_id": key[0], "period": key[1], "reason": cause.reason_code, "cell": column.get("cell")}
  source = column.get("source") or {}
  if column.get("gap"):
    return str(column["gap"][0]) if len(column["gap"]) == 1 else "construction_helper_missing:" + ",".join(
      gap.removeprefix("construction_helper_missing:") for gap in column["gap"]
    ), root
  kind = source.get("kind")
  if column.get("cell") == "unavailable" and kind in ("absence_citation", "unbound"):
    return f"authored_absence:{column.get('reason')}", root
  if column.get("cell") == "unavailable" and kind == "filing_line":
    return "filing_value_unread", root
  if column.get("cell") == "unavailable" and kind == "derivation":
    return f"evidenced_absence:{source.get('derivation')}", root
  if column.get("cell") == "unavailable" and kind == "none":
    return f"no_source:{column.get('reason')}", root
  if value.state != "missing":
    return f"{value.state}:{value.primary_reason}", root
  return f"unattributed:{cause.reason_code}", root


def evidenced_absence_report(doc: document.ModelSheet, ours: document.Values) -> dict[str, Any]:
  """Cited absences after ``document.recalc``.

  A cited absence stays evidenced absent and propagates unless the document can match it to a
  balance-sheet sum term whose role is ``optional_if_unreported`` (``document._omit_optional_sum_terms``).
  ``optionality_unknown`` says a reader could not be matched, so that
  absence propagated and was not read as ``0``. ``cells`` keeps each absence's citation and,
  when a sum omitted it, which sum.
  """
  summary = document.evidenced_absence_summary(ours, doc)
  omitted_by: dict[tuple[str, int], list[list[Any]]] = {}
  for item_id, period, absence_id, absence_period in summary["omitted_from"]:
    omitted_by.setdefault((absence_id, absence_period), []).append([item_id, period])
  return {
    "optionality_unknown": summary["optionality_unknown"],
    "optionality": (
      "A cited absence is omitted only from a balance-sheet sum whose term's role is "
      "optional_if_unreported. Any other formula cannot see optionality, "
      "so the absence propagates."
    ),
    **{key: summary[key] for key in ("absences", "omitted", "dependents")},
    "cells": [
      {
        "item_id": key[0], "period": key[1], "reason": value.primary_reason,
        "omitted_by": omitted_by.get(key, []),
        "source": (doc.semantic.get(key) or {}).get("source"),
      }
      for key, value in sorted(ours.items())
      if value.state == "evidenced_absent"
      and value.causes
      and (value.causes[0].node_id, int(value.causes[0].period.fiscal_year)) == key
    ],
  }


def completeness_report(
  doc: document.ModelSheet, *, before: document.Values, ours: document.Values, construction: Construction, reference: CommittedModelReference,
) -> dict[str, Any]:
  """``completeness.json``: every cell the reference version knows that the document did not
  know before construction, by the construction helper whose edit first made it known
  (``produced_by``, the compile step that produced it), or — still unavailable — by the class of
  its root cause (``remaining``: ``construction_helper_missing:<helper>``, an authored absence,
  a filing value the projection does not read, an input no source writes). Plus agreement with
  the reference by statement family and basis, the valuation outputs side by side, and
  the cited absences (``evidenced_absence_report``), including which a registered sum omitted."""
  historical = set(doc.model.time_structure.historical_periods)
  cells: list[dict[str, Any]] = []
  produced: Counter[str] = Counter()
  remaining: Counter[str] = Counter()
  agreement: dict[str, dict[str, Counter[str]]] = {}
  for key, expected in sorted(reference.values.items()):
    mine = ours.get(key)
    basis = "historical" if key[1] in historical else "projected"
    family = _family(key[0])
    state = "agree" if _agrees(mine, expected) else (
      "absent" if mine is None else "disagree" if mine.state == "known" else "ours_unavailable"
    )
    agreement.setdefault(family, {}).setdefault(basis, Counter())[state] += 1
    earlier = before.get(key)
    if mine is None or (earlier is not None and earlier.state == "known"):
      continue
    row: dict[str, Any] = {
      "item_id": key[0], "period": key[1], "family": family, "basis": basis,
      "reference": expected, "reference_note": _reference_note(reference.model, *key),
      "ours": mine.value if mine.state == "known" else None, "agree": state == "agree",
    }
    if mine.state == "known":
      helper = next((name for name, values in construction.trace if (values.get(key) is not None and values[key].state == "known")), None)
      row["produced_by"] = helper
      produced[str(helper)] += 1
    else:
      row["class"], row["root"] = _root_class(doc, mine)
      remaining[row["class"]] += 1
    cells.append(row)
  helpers = {row["helper"]: row["owner"] for row in construction.rows}
  return {
    "reference": {"error": reference.error} if reference.error is not None else reference.label,
    "before": document.state_counts(before),
    "after": document.state_counts(ours),
    "evidenced_absence": evidenced_absence_report(doc, ours),
    "produced_by": {name: {"cells": count, "owner": helpers.get(name)} for name, count in produced.most_common()},
    "remaining": dict(remaining.most_common()),
    "agreement": {family: {basis: dict(counts) for basis, counts in by_basis.items()} for family, by_basis in agreement.items()},
    "valuation_outputs": {
      item_id: {
        str(period): {"ours": document.value_row(value), "reference": reference.values.get((item_id, period))}
        for (owner, period), value in sorted(ours.items()) if owner == item_id
      }
      for item_id in VALUATION_OUTPUTS
    },
    "construction": [
      {key: row.get(key) for key in ("helper", "owner", "status", "needs", "code", "inputs_written")}
      | ({"formulas": len(row["formulas"]), "kept_authored": row["kept_authored"], "superseded_unavailable": len(row["superseded_unavailable"])}
         if row["status"] == "ok" else {})
      for row in construction.rows
    ],
    "cells": cells,
  }



# --------------------------------------------------------------------------
# build
# --------------------------------------------------------------------------


def _is_statement_item(item_id: str) -> bool:
  return item_id.startswith(("tpl.fm.income_statement.", "tpl.fm.balance_sheet.", "tpl.fm.cash_flow."))


def key_resolution(rows: list[dict[str, Any]], historical: tuple[int, ...]) -> dict[str, Any]:
  """Classify historical statement inputs by their declared source.

  Filing observations and closure-derived filing values require filing keys.
  Construction inputs and citation-derived absence values claim no observed
  filing line and are counted in their own source categories.
  """
  hist = set(historical)
  statement = [row for row in rows if _is_statement_item(row["item_id"]) and row["period"] in hist]
  inputs = [row for row in statement if row.get("origin") in ("observed", "derived") and row.get("cell") == "input"]
  constructed = [row for row in inputs if (row.get("source") or {}).get("kind") == "construction_helper"]
  cited_absences = [row for row in inputs if (row.get("source") or {}).get("kind") == "absence_citation" and row.get("origin") == "derived"]
  observed = [row for row in inputs if row not in constructed and row not in cited_absences]
  keyed = [
    row for row in observed
    if (row.get("source") or {}).get("kind") == "filing_line"
    or ((row.get("source") or {}).get("kind") == "derivation" and (row["source"].get("line") or {}).get("ref"))
  ]
  return {
    "historical_statement_cells": len(statement),
    "observed_or_derived_inputs": len(observed),
    "resolve_to_filing_key": len(keyed),
    "construction_inputs": len(constructed),
    "evidenced_absence_inputs": len(cited_absences),
    "unkeyed_observed": [{"item_id": row["item_id"], "period": row["period"]} for row in observed if row not in keyed],
    "by_cell": dict(Counter(row.get("cell") for row in statement)),
  }


def parse_cutoff(value: Any) -> datetime | None:
  """A declared cutoff (an ISO datetime or date; naive reads as UTC), or ``None``."""
  if value is None:
    return None
  moment = value if isinstance(value, datetime) else datetime.fromisoformat(str(value).replace("Z", "+00:00"))
  return moment if moment.tzinfo is not None else moment.replace(tzinfo=UTC)


def _cutoff_field(cutoff_at: datetime | None) -> dict[str, Any]:
  return {} if cutoff_at is None else {"cutoff_at": cutoff_at.isoformat()}


def vintage_counts(inputs: StageBuildInputs) -> dict[str, Any]:
  """C's vintage selection at the build's cutoff: the periods with more than their own filing's
  column, those bound to a later filing's restated column, and those holding a vintage accepted
  after the cutoff (retained, not selected)."""
  rows = list(inputs.vintages.values())
  return {
    "cutoff_at": None if inputs.cutoff_at is None else inputs.cutoff_at.isoformat(),
    "periods": len(rows),
    "later_vintage_selected": sum(
      1 for row in rows if any(item.evidence_id == row.selected and not item.own for item in row.candidates)
    ),
    "vintage_after_cutoff": sum(1 for row in rows if any(item.available_at > row.cutoff_at for item in row.candidates)),
  }


@dataclass(frozen=True)
class Constructed:
  """What ``construct_from_inputs`` ran: the document and each writer's result, the recalc before
  the forecast and construction (``before``), the roles C has no document line for (``unlined``),
  the missing-input marks, the analyst ops replay's findings (``agent_op_findings``), and the
  cells each stage writer's edits named (``footprints``)."""

  doc: document.ModelSheet
  historical: dict[str, Any]
  seeds: Any
  valuation: dict[str, Any]
  scenarios: dict[str, Any]
  before: document.Values
  forecast: Mapping[str, Any]
  unlined: frozenset[str]
  construction: Construction
  missing: Any
  agent_op_findings: tuple[dict[str, Any], ...] = ()
  footprints: dict[str, frozenset[tuple[str, int]]] = field(default_factory=dict)
  edit_segments: tuple[EditSegment, ...] = ()


def construct_from_inputs(
  structure: SheetFrame, inputs: StageBuildInputs, ticker: str, refusals: _Refusals, *,
  agent_ops: Sequence[Mapping[str, Any]] = (), trace: bool = True,
) -> Constructed:
  """The one construction sequence over ``structure``'s document: company, C's historical cells,
  seeds, valuation and scenario cells, recalc, forecast, ``construct_document``, the presented case
  and the missing-input marks, in that order; then the analyst's ``agent_ops`` replayed onto the
  result (``live_sheet.replay_agent_ops``: an op the inputs no longer admit is an
  ``agent_op_superseded`` finding, never silently dropped). ``target_build`` and the live sheet's
  refresh both run it. ``trace=False`` skips the recalc after each helper (``Construction.trace``),
  which only ``completeness_report`` reads."""
  doc = structure.doc
  from model_build import live_sheet
  segments = _EditSegments(doc)
  segments.capture("structure", 0)

  def _wrote(n0: int) -> frozenset[tuple[str, int]]:
    return live_sheet.writer_footprint(doc, doc.edits[n0:])

  company = inputs.company or {}
  segments.run("company", lambda: document.set_company(doc, ticker, company.get("name"), company.get("cik")))
  with document.transaction(doc):
    historical = segments.run("historical", lambda: write_historical_cells(structure, inputs, ticker))
    n0 = len(doc.edits)
    seeds = segments.run("stage:historical_foundation", lambda: write_seed_cells(structure, inputs))
    seed_cells = _wrote(n0)
    n0 = len(doc.edits)
    valuation = segments.run("stage:valuation_policy", lambda: write_valuation_cells(structure, inputs, refusals))
    valuation_cells = _wrote(n0)
    n0 = len(doc.edits)
    scenarios = segments.run("stage:scenario_set", lambda: write_scenario_cells(structure, inputs))
    scenario_cells = _wrote(n0)
  before = document.recalc(doc)
  n0 = len(doc.edits)
  forecast = segments.run("stage:forecast_assumption_set", lambda: write_forecast_cells(structure, inputs))
  forecast_cells = _wrote(n0)
  unlined = frozenset(row.required_role for row in inputs.rows if not historical["targets"].get(row.required_role))
  construction = construct_document(
    structure, inputs, ticker, forecast=forecast, unlined_roles=unlined, trace=trace, segments=segments,
  )
  scenarios["presented_case_cells"] = segments.run("presented", lambda: write_presented_case(structure))
  missing = segments.run("missing", lambda: mark_missing_inputs(
    structure, missing_helpers=construction.missing(), retired_items=construction.retired,
    unresolved_nodes=construction.unresolved_nodes,
  ))
  agent_op_findings: tuple[dict[str, Any], ...] = ()
  if agent_ops:
    doc, _kept, agent_op_findings = live_sheet.replay_agent_ops(doc, agent_ops, model_spec=inputs.model_spec)
  return Constructed(
    doc=doc, historical=historical, seeds=seeds, valuation=valuation, scenarios=scenarios, before=before,
    forecast=forecast, unlined=unlined, construction=construction, missing=missing,
    agent_op_findings=agent_op_findings,
    edit_segments=tuple(segments.segments),
    footprints={
      "historical_foundation": seed_cells,
      "valuation_policy": valuation_cells,
      "scenario_set": scenario_cells,
      "forecast_assumption_set": forecast_cells,
    },
  )


def target_build(
  source: BuildSource,
  *,
  refs: Mapping[str, Any] | None = None,
  workdir: Path,
  accept_gaps: Collection[str] = (),
  axis: Mapping[str, Any] | None = None,
  defer_validation: bool = False,
) -> dict[str, Any]:
  """Construct the document from C and the judgment artifacts, check it, save its package.

  ``verdict`` (``_verdict``): ``stopped:<code>`` with the owner of the first product refusal met
  (or ``stopped:observed_cell_unkeyed``); else ``incomplete:<first reason>``
  (``incomplete_reasons``: inputs unresolved, a construction helper missing or refused and not
  named in ``accept_gaps``, a document never validated, a blocking finding of the graph validation
  report); else ``published``. The manifest records it. No workbook is rendered or natively
  compared here; publication verifies the authored document's rendered workbook.

  ``axis`` (``{"last_actual": <year>, "cutoff_at": <ISO datetime>}``) is the time axis as an
  input (``build_structure``); by default the profile's axis. ``cutoff_at`` is the instant C's
  restatement-vintage selection binds as of (``projection.project_facts_per_period``): a filing
  accepted after it is no vintage of the build; by default the latest captured filing's
  acceptance, or with a declared ``last_actual`` the latest among the filings whose own year is
  at or before it. The manifest's ``axis.last_actual`` records a declared one, ``axis.cutoff_at``
  the cutoff the build ran at.

  ``defer_validation`` (default ``False``) is for a caller that validates the working document
  or its published version itself: the check is then ``validated: None``, no validation reason
  enters the construction verdict, and the caller reads the validation report's verdict.
  By default the build validates the document graph itself (``document_validation``) and a
  blocking finding makes the verdict ``incomplete:check_error``.
  """
  unknown_axis = sorted(set(axis or {}) - {"last_actual", "cutoff_at"})
  if unknown_axis:
    raise ValueError(f"axis: unknown keys {unknown_axis}; expected last_actual, cutoff_at")
  last_actual = None if not axis or axis.get("last_actual") is None else int(axis["last_actual"])
  cutoff_at = parse_cutoff((axis or {}).get("cutoff_at"))
  ticker = source.ticker
  workdir = Path(workdir).expanduser()
  snapshot = workdir / SNAPSHOT_DIR
  snapshot.mkdir(parents=True, exist_ok=True)
  for path in snapshot.iterdir():
    if path.is_file():
      path.unlink()
  refusals = _Refusals()
  result: dict[str, Any] = {"ticker": ticker, "workdir": str(workdir), "snapshot": str(snapshot)}
  inputs = refusals.run("inputs", lambda: load_inputs(source, refs, refusals, cutoff_at=cutoff_at, last_actual=last_actual))
  structure = None if inputs is None else refusals.run("structure", lambda: build_structure(inputs, ticker, last_actual=last_actual))
  if inputs is None or structure is None:
    return {**result, "refusals": refusals.rows, "verdict": _verdict(refusals, None, None)}
  constructed = construct_from_inputs(structure, inputs, ticker, refusals)
  doc = constructed.doc
  historical, seeds, valuation, scenarios = constructed.historical, constructed.seeds, constructed.valuation, constructed.scenarios
  before, forecast, construction, missing = constructed.before, constructed.forecast, constructed.construction, constructed.missing
  c_states = Counter(row.state for row in inputs.rows)
  ours = document.recalc(doc)
  unclaimed = refusals.run("filed_line_unclaimed", lambda: unclaimed_filed_lines(
    inputs.hff, structure.historical_years, inputs.corpus,
    occurrence_envelopes=inputs.occurrence_envelopes, statement_bindings=inputs.statement_bindings,
  )) or []
  check = target_check(
    doc, validation=None if defer_validation else document_validation(
      doc, inputs, ticker, unclaimed, refusals, template=cast(FinancialModel, structure.template),
      scenario_controls=construction.scenario_controls,
    ),
    deferred=defer_validation,
  )
  rows = semantic_rows(doc, ours)
  resolution = key_resolution(rows, structure.historical_years)
  reference = source.reference_version(inputs)
  completeness = completeness_report(doc, before=before, ours=ours, construction=construction, reference=reference)
  findings = {
    "refusals": refusals.rows,
    "inputs": inputs.findings,
    "historical": historical["findings"],
    "historical_refill": construction.refills[-1]["findings"] if construction.refills else [],
    "seeds": seeds["findings"],
    "forecast": forecast["findings"],
    "valuation": valuation["findings"],
    "scenarios": scenarios["findings"],
    "construction": [
      {key: value for key, value in row.items() if key in ("helper", "owner", "status", "needs", "code", "owner_file", "message", "kept_authored")}
      for row in construction.rows if row["status"] != "ok" or row.get("kept_authored")
    ],
    "c_rows_not_written": [
      {"required_role": row.required_role, "period": row.period_id, "state": row.state, "code": "required_role_has_no_document_line"}
      for row in inputs.rows
      if not historical["targets"].get(row.required_role) and row.required_role not in construction.lined_roles()
    ],
  }
  scenario_row = next((row for row in construction.rows if row["helper"] == "scenario_outputs"), None)
  analyses = {**_ANALYSES, **({"scenarios": "constructed"} if scenario_row is not None and scenario_row["status"] == "ok" else {})}
  construction_rows = [{key: row.get(key) for key in ("helper", "owner", "status")} for row in construction.rows]
  incomplete = incomplete_reasons(
    construction_rows, check=check, unresolved=inputs_unresolved(inputs), accept_gaps=accept_gaps,
  )
  verdict = _verdict(refusals, resolution, True, incomplete)
  manifest = {
    "built_at": datetime.now(UTC).isoformat(),
    "bench_sha": _bench_sha(),
    "ticker": ticker,
    "company": inputs.company,
    "lineage": "fresh",
    "store": store_label(source),
    "corpus": {"corpus_id": inputs.corpus.corpus_id, "corpus_version": inputs.corpus.corpus_version, "manifest_sha256": inputs.corpus.manifest_sha256},
    "profile": {"profile_id": inputs.profile.profile_id, "profile_version": inputs.profile.profile_version},
    "inputs": {
      "business_model_revision": _ref_json(inputs.revision),
      "business_model_selection_epoch": inputs.selection_epoch,
      **judgment_refs(inputs),
      "source_bundle": inputs.bundle_id,
      "model_spec": None if inputs.model_spec is None else "research.model_spec_assembly.assemble_model_spec",
    },
    "axis": {
      "historical": list(structure.historical_years), "forecast": list(structure.forecast_years),
      **({"last_actual": last_actual} if last_actual is not None else {}),
      **_cutoff_field(inputs.cutoff_at),
    },
    "engine": "DependencyGraph (document.recalc)",
    "values_sha256": document.values_digest(ours),
    "construction": construction_rows,
    "analyses": analyses,
    "verdict": verdict,
    "incomplete": incomplete,
    "accepted_gaps": sorted(accept_gaps),
  }
  document.save(doc, snapshot / DOCUMENT_NAME, values=ours)
  if inputs.model_spec is not None:
    (snapshot / "model_spec.json").write_text(inputs.model_spec.model_dump_json(indent=1))
  if structure.template is not None:
    template_path = snapshot / "template.json"
    template_path.write_text(structure.template.model_dump_json(indent=1))
    result["template"] = str(template_path)
  if construction.scenario_controls is not None:
    controls_path = snapshot / "scenario_controls.json"
    controls_path.write_text(json.dumps(construction.scenario_controls, indent=1))
    result["scenario_controls"] = str(controls_path)
  baseline_path = snapshot / "input_baseline.json"
  baseline_path.write_text(json.dumps(
    construction_input_baseline(inputs, structure.historical_years, unclaimed), indent=1,
  ))
  result["input_baseline"] = str(baseline_path)
  keyed_dump.write_jsonl(snapshot / "sources.jsonl", rows)
  matrix: dict[str, dict[int, Any]] = {}
  for (item_id, period), value in ours.items():
    matrix.setdefault(item_id, {})[period] = value
  keyed_dump.write_jsonl(snapshot / "values.jsonl", keyed_dump.dump_values(doc.model, matrix))
  for name, payload in (
    ("completeness.json", completeness), ("validation.json", check), ("findings.json", findings),
  ):
    (snapshot / name).write_text(json.dumps(payload, indent=1, default=str))
  files = sorted(path for path in snapshot.iterdir() if path.is_file() and path.name != "manifest.json")
  manifest["files"] = {path.name: _sha256(path) for path in files}
  (snapshot / "manifest.json").write_text(json.dumps(manifest, indent=1, default=str))
  result.update({
    "c": {
      "rows": len(inputs.rows), "states": dict(c_states), "dispositions": dict(Counter(str(row.disposition) for row in inputs.rows)),
      "vintages": vintage_counts(inputs),
    },
    "historical": historical["counts"],
    "historical_refill": [refill["counts"] for refill in construction.refills],
    "seeds": {"written": seeds["written"], "findings": len(seeds["findings"])},
    "forecast": {key: forecast[key] for key in ("written", "drivers")} | {"findings": len(forecast["findings"])},
    "valuation": {"written": valuation["written"], "findings": len(valuation["findings"])},
    "scenarios": {key: scenarios[key] for key in ("written", "curves_without_cells", "presented_case_cells")},
    "construction": {
      row["helper"]: {
        "status": row["status"],
        **({"inputs_written": row["inputs_written"], "formulas": len(row["formulas"]), "items_added": len(row["items_added"]),
            "kept_authored": len(row["kept_authored"]), "superseded_unavailable": len(row["superseded_unavailable"])}
           if row["status"] == "ok" else {key: row.get(key) for key in ("needs", "code", "owner_file")}),
      }
      for row in construction.rows
    },
    "input_findings": [{key: row[key] for key in ("stage", "code")} for row in inputs.findings],
    "marked_unavailable": missing,
    "ours": document.state_counts(ours),
    "evidenced_absence": {
      key: completeness["evidenced_absence"][key]
      for key in ("absences", "omitted", "dependents", "optionality_unknown")
    },
    "completeness": {key: completeness[key] for key in ("reference", "before", "produced_by", "remaining", "agreement", "valuation_outputs")},
    "key_resolution": {key: value for key, value in resolution.items() if key != "unkeyed_observed"},
    "check": check["counts"],
    "filed_line_unclaimed": unclaimed,
    "refusals": refusals.rows,
    "edits": len(doc.edits),
    "incomplete": incomplete,
    "verdict": verdict,
  })
  return result


def incomplete_reasons(
  construction: Collection[Mapping[str, Any]] | None,
  *,
  compared: Mapping[str, Any] | None = None,
  check: Mapping[str, Any],
  unresolved: Collection[Mapping[str, Any]] = (),
  accept_gaps: Collection[str] = (),
) -> list[dict[str, Any]]:
  """Why a snapshot is not a publishable model, in order: its SourceBundle / ModelSpec did not
  resolve (``inputs_unresolved``, with the input findings), its construction helper statuses are
  unknown (``construction`` is ``None``: an edited package whose origin snapshot was not found,
  ``origin_unknown``), each construction helper ``missing`` or ``refused`` that ``accept_gaps``
  does not name (``construction_helper_<status>:<helper>``), native values disagreeing with ours
  (``native_value_mismatch``), the document never validated (``validation_not_run``: no SourceBundle
  / ModelSpec to stage it against), the validation report's blocking findings (``check`` as
  ``target_check`` returns it; ``check_error:<first code>``). Empty: nothing stands in the way."""
  accepted = set(accept_gaps)
  reasons: list[dict[str, Any]] = []
  if unresolved:
    reasons.append({"reason": "inputs_unresolved", "findings": [dict(row) for row in unresolved]})
  if construction is None:
    reasons.append({"reason": "origin_unknown"})
  for row in construction or ():
    if row.get("status") in ("missing", "refused") and row["helper"] not in accepted:
      reasons.append({"reason": f"construction_helper_{row['status']}:{row['helper']}", "helper": row["helper"], "status": row["status"]})
  mismatches = 0 if compared is None else int(compared["compare"]["by_kind"].get("value_mismatch", 0))
  if mismatches:
    reasons.append({"reason": "native_value_mismatch", "count": mismatches})
  if check.get("validated") is False:
    reasons.append({"reason": "validation_not_run"})
  blocking = check.get("blocking") or ()
  if blocking:
    reasons.append({
      "reason": f"check_error:{blocking[0]['code']}", "count": len(blocking), "codes": dict(Counter(row["code"] for row in blocking)),
    })
  return reasons


def _verdict(
  refusals: _Refusals,
  resolution: Mapping[str, Any] | None,
  complete: bool | None,
  incomplete: Collection[Mapping[str, Any]] = (),
) -> str:
  if refusals.rows:
    first = refusals.rows[0]
    return f"stopped:{first['code']}"
  if not complete:
    return "stopped:snapshot_incomplete"
  if resolution is not None and resolution["unkeyed_observed"]:
    return "stopped:observed_cell_unkeyed"
  if incomplete:
    return f"incomplete:{next(iter(incomplete))['reason']}"
  return "published"


# --------------------------------------------------------------------------
# lookup
# --------------------------------------------------------------------------


def _snapshot_dir(workdir: Path) -> Path:
  path = Path(workdir).expanduser()
  return path if (path / "manifest.json").is_file() else path / SNAPSHOT_DIR


def target_lookup(workdir: Path, item_id: str, period: int) -> dict[str, Any]:
  """One cell joined across the snapshot: value and state/reason (ours), its semantic column
  (origin, typed source ref), its source vintage (``vintage``: the accession the value is read
  from and the cell's ``source_history``), the forecast its actual superseded
  (``forecast_record``), the native value and whether the two engines agree."""
  snapshot = _snapshot_dir(workdir)
  period = int(period)

  def find(name: str, item_key: str, period_key: str) -> dict[str, Any] | None:
    path = snapshot / name
    if not path.is_file():
      return None
    for line in path.read_text().splitlines():
      if not line.strip():
        continue
      row = json.loads(line)
      if row.get(item_key) == item_id and int(row.get(period_key, -1)) == period:
        return row
    return None

  source = find("sources.jsonl", "item_id", "period")
  value = find("values.jsonl", "item_id", "period")
  compare_path = snapshot / "compare.json"
  compared = None
  if compare_path.is_file():
    compared = next(
      (row for row in json.loads(compare_path.read_text()).get("rows", ()) if row["item"] == item_id and int(row["period"]) == period),
      None,
    )
  return {
    "item_id": item_id,
    "period": period,
    "found": source is not None,
    "value": None if value is None else value.get("value"),
    "state": None if value is None else value.get("state"),
    "reason": None if value is None else value.get("reason"),
    "cell": None if source is None else source.get("cell"),
    "origin": None if source is None else source.get("origin"),
    "required_role": None if source is None else source.get("required_role"),
    "source": None if source is None else source.get("source"),
    "declared_reason": None if source is None else source.get("reason"),
    "operand": None if source is None else (source.get("value") or {}).get("operand"),
    "derivation": None if source is None else (source.get("value") or {}).get("derivation"),
    "vintage": _vintage_view(source),
    "forecast_record": None if source is None else source.get(document.FORECAST_RECORD),
    "native": None if compared is None else compared.get("native"),
    "rendered_cell": None if compared is None else compared.get("cell"),
    "agreement": None if compared is None else compared.get("kind"),
    "agree": None if compared is None else compared.get("agree"),
  }


def target_report(workdir: Path) -> dict[str, Any]:
  """A snapshot's report: its ``verdict``, the workbook it rendered (``model.xlsx``; ``None`` when
  the snapshot holds none) and its manifest (incomplete reasons, inputs, axis, construction,
  ``files`` with each file's sha256)."""
  snapshot = _snapshot_dir(workdir)
  manifest = json.loads((snapshot / "manifest.json").read_text())
  workbook = snapshot / f"{WORKBOOK_STEM}.xlsx"
  return {
    "snapshot": str(snapshot),
    "verdict": manifest.get("verdict"),
    "workbook": str(workbook) if workbook.is_file() else None,
    "manifest": manifest,
  }


# --------------------------------------------------------------------------
# refresh (C re-run onto the saved document) and explain
# --------------------------------------------------------------------------


REFRESH_NAME = "refresh.json"


def _dilution_json(policy: Any) -> Any:
  """The policy's authored net dilution, or ``None`` when no policy or dilution was authored."""
  dilution = getattr(policy, "net_share_dilution", None)
  return None if dilution is None else dilution.model_dump(mode="json")


def _plain(value: Any) -> Any:
  """``value`` as its JSON round trip: a column built in process compares equal to one reopened."""
  return json.loads(json.dumps(value, default=str))


def _stored_input(doc: document.ModelSheet, key: tuple[str, int]) -> float | None:
  override, cell = _cell_state(doc.model._index.get(key[0]), key[1])
  if override is not None:
    return override.params.get("value")
  return None if cell is None else cell.value


def _cell_view(doc: document.ModelSheet, key: tuple[str, int]) -> dict[str, Any]:
  """One cell as refresh reports it: the input it stores and its semantic column."""
  column = doc.semantic.get(key)
  return {"value": _stored_input(doc, key), "source": None if column is None else _plain(column)}


def _same_cell(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
  if left["source"] != right["source"]:
    return False
  if left["value"] is None or right["value"] is None:
    return left["value"] is right["value"]
  return math.isclose(float(left["value"]), float(right["value"]), rel_tol=1e-12, abs_tol=1e-12)


def _forecast_view(doc: document.ModelSheet, key: tuple[str, int]) -> dict[str, Any]:
  """``_cell_view`` read through a provisional mark (``document.advance_period``): a forecast an
  actual year's cell still holds compares equal to the column it held before the advance."""
  view = _cell_view(doc, key)
  column = view["source"]
  if column is not None and document.PROVISIONAL in column:
    view["source"] = (
      column.get("column_before") if column.get("origin") == document.PROVISIONAL
      else {name: entry for name, entry in column.items() if name != document.PROVISIONAL}
    )
  return view


def _authored_outcome(
  views: Mapping[tuple[str, int], Mapping[str, Any]], doc: document.ModelSheet,
  now: Callable[[tuple[str, int]], tuple[str, int]] = lambda key: key,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
  """What became of each authored cell (``_forecast_view`` before the run): ``superseded`` — the
  year's actual took the cell, the authored value now its ``forecast_record`` — and ``changed``;
  every other cell, a forecast still held in an actual year included, is untouched."""
  superseded: list[dict[str, Any]] = []
  changed: list[dict[str, Any]] = []
  for key, view in views.items():
    after = _forecast_view(doc, now(key))
    row = {"item": key[0], "period": key[1], "before": view, "after": after}
    if document.FORECAST_RECORD in (after["source"] or {}) and document.FORECAST_RECORD not in (view["source"] or {}):
      superseded.append(row)
    elif not _same_cell(view, after):
      changed.append(row)
  return superseded, changed


def _judgment_views(doc: document.ModelSheet) -> dict[tuple[str, int], dict[str, Any]]:
  return {
    key: _forecast_view(doc, key) for key, column in doc.semantic.items()
    if document.judgment_row_ref(column.get("source")) is not None
  }


def _edit_counts(prior_edits: Sequence[Mapping[str, Any]], doc: document.ModelSheet) -> dict[str, int]:
  """``edits_preserved``: the edits from before the run still live and unchanged;
  ``edits_superseded_by_actual``: those an actual superseded, wholly or in part
  (``superseded_by_actual``) — history, no longer live."""
  now = _plain(doc.edits[: len(prior_edits)])
  return {
    "edits_preserved": sum(1 for old, new in zip(prior_edits, now) if old == new and not new.get("superseded_by_actual")),
    "edits_superseded_by_actual": sum(1 for edit in now if edit.get("superseded_by_actual")),
  }


def forecast_records(doc: document.ModelSheet) -> set[tuple[str, int]]:
  """Every cell whose actual superseded the forecast it held (``forecast_record``)."""
  return {key for key, column in doc.semantic.items() if document.FORECAST_RECORD in column}


def forecast_vs_actual(doc: document.ModelSheet, values: document.Values, cells: Collection[tuple[str, int]]) -> list[dict[str, Any]]:
  """Each superseded cell of ``cells`` whose forecast and actual are both numbers: the forecast it
  held (``forecast_record``), the actual (``document.operand_value``; a cited absence is not a
  number, so it is not a delta), ``delta`` = actual − forecast, ``delta_pct`` against the forecast's magnitude, the
  forecast's origin and rationale."""
  rows: list[dict[str, Any]] = []
  for key in sorted(cells):
    record = doc.semantic[key][document.FORECAST_RECORD]
    forecast, actual = record.get("value"), document.operand_value(values.get(key))
    if forecast is None or actual is None:
      continue
    delta = float(actual) - float(forecast)
    row: dict[str, Any] = {
      "item": key[0], "period": key[1], "forecast": float(forecast), "actual": float(actual), "delta": delta,
      "delta_pct": None if float(forecast) == 0 else delta / abs(float(forecast)), "forecast_origin": record.get("origin"),
    }
    if record.get("rationale") is not None:
      row["rationale"] = record["rationale"]
    rows.append(row)
  return rows


def _refreshable(column: Mapping[str, Any] | None, *, planned_input: bool, planned_reason: str | None = None) -> str | None:
  """Why C may rewrite a cell holding ``column``; ``None`` when the cell is not C's to rewrite.

  C's own cells (``binding_state`` in the column), the unsourced gaps ``mark_missing_inputs``
  left, and cells nothing wrote (no column, or the column of the cell's own formula: the document's
  formula computes them, or a line the build held as a header) are, as a build writes C first. A
  construction helper's fallback yields to a C fact or cited inclusion (a helper keeps authored
  inputs). A forecast still held in an actual year (``advance_period``'s provisional mark: a
  projection, the agent's input, a judgment row) yields to a sourced input, evidenced absence or
  cited inclusion (``planned_reason``), never to an unknown: it keeps running until the filing gives the year. The
  agent's other cells are excluded before this; a judgment row's cell never is."""
  if column is None or (column.get("source") or {}).get("kind") == "formula":
    return "c_cell_unwritten"
  if document.PROVISIONAL in column:
    return "provisional_projection" if planned_input or planned_reason in _ACTUAL_NONNUMERIC else None
  if "constructed_by" in column:
    return "construction_fallback" if planned_input or planned_reason == "not_separately_presented" else None
  if "binding_state" in column:
    return "c_cell"
  if column.get("cell") == "unavailable" and (column.get("source") or {}).get("kind") == "none":
    return "unsourced_gap"
  return None


def _vintage(column: Mapping[str, Any] | None) -> str | None:
  """The accession of the filing a C column's value is read from (its ``FilingLineRef``)."""
  source = (column or {}).get("source") or {}
  if source.get("kind") == "filing_line":
    ref = source.get("ref")
  elif source.get("kind") == "derivation":
    ref = (source.get("line") or {}).get("ref")
  else:
    ref = None
  return ref.get("accession") if isinstance(ref, Mapping) else None


def _vintage_view(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
  """A cell's source vintage as lookup and explain report it: the accession its value is read
  from, that vintage's acceptance (from ``source_history``, when the cell carries one) and the
  ``source_history`` itself."""
  if row is None:
    return None
  history = row.get("source_history")
  accession = _vintage(row)
  selected = next((entry for entry in history or () if entry.get("vintage_accession") == accession), None)
  return {
    "accession": accession,
    "available_at": None if selected is None else selected.get("available_at"),
    "source_history": history,
  }


def _accepted(filed_at: Mapping[str, datetime], accession: str | None) -> str | None:
  """The acceptance of the captured filing ``accession`` (ISO), when the corpus dates it."""
  moment = None if accession is None else filed_at.get(accession)
  return None if moment is None else moment.isoformat()


def _vintage_entry(value: float | None, column: Mapping[str, Any], filed_at: Mapping[str, datetime]) -> dict[str, Any]:
  accession = _vintage(column)
  return {
    "value": value, "source": _plain(column.get("source")), "vintage_accession": accession,
    "available_at": _accepted(filed_at, accession),
  }


def _with_vintages(
  held: Mapping[str, Any] | None,
  held_value: float | None,
  column: Mapping[str, Any],
  value: float | None,
  filed_at: Mapping[str, datetime],
) -> dict[str, Any]:
  """C's planned column for one cell with every source vintage the cell has held. The planned
  ``source_history`` (C's vintage candidates, the selected one first; else the planned value's
  own vintage) comes first; every vintage of the held cell (its ``source_history``, else the
  vintage its value was read from) whose accession the planned history lacks follows, so a
  refresh or advance never drops an earlier vintage. ``source_history`` is written when C
  planned one or a held vintage is kept. A forecast the held cell's actual superseded
  (``forecast_record``) stays on it."""
  planned = dict(column)
  if held is not None and document.FORECAST_RECORD in held:
    planned[document.FORECAST_RECORD] = held[document.FORECAST_RECORD]
  history = [dict(entry) for entry in planned.get("source_history") or ()]
  if not history and value is not None and _vintage(planned) is not None:
    history = [_vintage_entry(value, planned, filed_at)]
  held_history = list((held or {}).get("source_history") or ())
  if not held_history and held_value is not None and _vintage(held) is not None:
    held_history = [_vintage_entry(held_value, held or {}, filed_at)]
  known = {entry.get("vintage_accession") for entry in history}
  kept = [
    {**_plain(entry), **({"selected": False} if "selected" in entry else {})}
    for entry in held_history if entry.get("vintage_accession") not in known
  ]
  if "source_history" in planned or kept:
    planned["source_history"] = [*history, *kept]
  return planned


def _document_structure(doc: document.ModelSheet) -> SheetFrame:
  """The saved document's structure as the historical writer reads it: its axis
  (``time_structure``) and the BusinessModel node lines, driver keys and segment mapping its
  structural edit recorded. A package recorded before the mapping was reads an empty one — a
  stored-artifact compatibility reading: an advance over such a package names no segment faces
  while a live ``build_structure`` does; that is the recorded contract's age, never a parity
  question."""
  edit = next((row for row in doc.edits if (row.get("op") or {}).get("type") == "structure"), None)
  recorded = (edit or {}).get("result") or {}
  time = doc.model.time_structure
  return SheetFrame(
    doc=doc,
    registry=SimpleNamespace(
      node_items=dict(recorded.get("node_items") or {}),
      driver_keys=dict(recorded.get("driver_keys") or {}),
      segment_mapping=dict(recorded.get("segment_mapping") or {}),
    ),
    historical_years=tuple(int(year) for year in time.historical_periods),
    forecast_years=tuple(int(year) for year in time.projection_periods),
  )


def refresh_cells(doc: document.ModelSheet, inputs: StageBuildInputs, ticker: str) -> dict[str, Any]:
  """Re-run C's historical plan (``plan_historical_cells``) against ``doc`` and write, through the
  document writers, only the C cells whose value or semantic column changed: a newly mapped
  period, a corrected mapping, a newly captured fact. A cell C now leaves to the document's
  formula has its C input cleared; a C cell no C row reaches any more is unavailable
  ``source_observation_absent``. Agent edits, judgment cells, formulas and structure are not
  written; a C cell the agent or a judgment holds is listed in ``kept``, and so is a provisional
  projection C has no value for. A forecast still held in an actual year — a projection, the
  agent's input, a judgment row (``advance_period``) — is not kept: the year's sourced input or
  evidenced absence supersedes it, the forecast riding on the actual as ``forecast_record``
  (``document.supersede_forecast``), its edit marked ``superseded_by_actual``. A cell keeps every
  source vintage it has held (``_with_vintages``); one whose selected vintage moved to another
  filing is in ``restated_cells`` where the value changed, ``revintaged_cells`` where only the
  filing did. Then ``document.release_provisional``."""
  structure = _document_structure(doc)
  plan = plan_historical_cells(structure, inputs, ticker)
  agent_items, agent_cells = document._agent_authored(doc)
  prior_edits = _plain(doc.edits)
  judgments = _judgment_views(doc)
  # key → (write kind, payload, the cell view after the write)
  wanted: dict[tuple[str, int], tuple[str, Any, dict[str, Any]]] = {}
  for key, (value, planned) in plan.inputs.items():
    column = _with_vintages(doc.semantic.get(key), _stored_input(doc, key), planned, value, inputs.filed_at)
    wanted[key] = ("input", (value, column), {"value": value, "source": _plain({"cell": "input", **column})})
  for key, (reason, planned) in plan.unavailable.items():
    column = _with_vintages(doc.semantic.get(key), _stored_input(doc, key), planned, None, inputs.filed_at)
    wanted[key] = ("unavailable", (reason, column), {"value": None, "source": _plain({"cell": "unavailable", **column, "reason": reason})})
  for key, (included_in, citation, planned) in plan.included.items():
    column = _with_vintages(doc.semantic.get(key), _stored_input(doc, key), planned, None, inputs.filed_at)
    wanted[key] = (
      "included", (included_in, citation, column),
      {"value": None, "source": _plain({"cell": "not_separately_presented", **column, "reason": "not_separately_presented"})},
    )
  for key, column in plan.formula.items():
    held = doc.semantic.get(key) or {}
    if "binding_state" in held and "constructed_by" not in held and held.get("cell") in ("input", "unavailable", "not_separately_presented"):
      wanted[key] = ("clear", column, {"value": None, "source": None})
  historical = set(structure.historical_years)
  stale_inclusions: dict[tuple[str, int], tuple[str, int]] = {}
  for key, column in doc.semantic.items():
    if key[1] in historical and key not in plan.covered and "binding_state" in column and "constructed_by" not in column:
      if column.get("cell") == "not_separately_presented":
        stale_inclusions[key] = (column["required_role"], key[1])
      stale = {"source": {"kind": "none", "note": "no C row maps this cell at refresh"}}
      reason: CellUnavailableReason = "source_observation_absent"
      after = {"value": None, "source": _plain({"cell": "unavailable", **stale, "reason": reason})}
      wanted[key] = ("unavailable", (reason, stale), after)
  inputs_out: dict[tuple[str, int], tuple[float, dict[str, Any]]] = {}
  unavailable_out: dict[tuple[str, int], tuple[CellUnavailableReason, dict[str, Any]]] = {}
  included_out: dict[tuple[str, int], tuple[str, dict[str, str], dict[str, Any]]] = {}
  clears: dict[tuple[str, int], dict[str, Any]] = {}
  changed: list[dict[str, Any]] = []
  kept: list[dict[str, Any]] = []
  restated: list[dict[str, Any]] = []
  revintaged: list[dict[str, Any]] = []
  previous_markers = {
    (item.id, int(period)): spec
    for item in doc.model._index.values()
    for period, spec in (item.overrides or {}).items()
    if spec.subtype == "not_separately_presented"
  }
  for key, (kind, payload, after) in sorted(wanted.items()):
    before = _cell_view(doc, key)
    if _same_cell(before, after):
      continue
    row = {"item": key[0], "period": key[1], "before": before, "after": after}
    held_column = doc.semantic.get(key)
    if (key[0] in agent_items or key in agent_cells) and document.PROVISIONAL not in (held_column or {}):
      kept.append({**row, "kept": "agent_edit"})
      continue
    planned_reason = payload[0] if kind == "unavailable" else "not_separately_presented" if kind == "included" else None
    why = _refreshable(held_column, planned_input=kind == "input", planned_reason=planned_reason)
    if why is None:
      if held_column is not None and document.PROVISIONAL in held_column:
        kept.append({**row, "kept": "provisional_projection"})
        continue
      held = (held_column or {}).get("source") or {}
      kept.append({**row, "kept": "authored", "held_kind": held.get("kind")})
      continue
    changed.append({**row, "owner": why})
    if kind == "input":
      inputs_out[key] = payload
      accession, held_accession = _vintage(payload[1]), _vintage(held_column)
      if accession is not None and held_accession is not None and accession != held_accession:
        vintage = {
          "item": key[0], "period": key[1], "required_role": payload[1].get("required_role"),
          "before": {"value": before["value"], "accession": held_accession, "available_at": _accepted(inputs.filed_at, held_accession)},
          "after": {"value": payload[0], "accession": accession, "available_at": _accepted(inputs.filed_at, accession)},
        }
        same = before["value"] is not None and math.isclose(float(before["value"]), float(payload[0]), rel_tol=1e-12, abs_tol=1e-12)
        (revintaged if same else restated).append(vintage)
    elif kind == "unavailable":
      unavailable_out[key] = payload
    elif kind == "included":
      included_out[key] = payload
    else:
      clears[key] = payload
  _write_planned(doc, inputs_out, unavailable_out, included_out)
  for (item_id, period), column in sorted(clears.items()):
    # C now leaves the cell to the document's formula: its input goes, and so does the column,
    # so the cell reads as the formula's (a superseded forecast's record stays on it).
    item_obj = doc.model.get_item(item_id)
    if (
      item_obj.historical is not None
      and item_obj.historical.type is not FormulaType.constant
      and item_obj.formula_periods is not None
      and period not in item_obj.formula_periods
    ):
      item_obj.formula_periods = sorted({*(int(p) for p in item_obj.formula_periods), int(period)})
    document.clear_input(doc, item_id, period, source=column)
    cleared = doc.semantic.pop((item_id, period), None) or {}
    spec = document._render_spec_for_period(doc.model, doc.model.get_item(item_id), period)
    if document.FORECAST_RECORD in cleared and spec is not None and spec.type is not FormulaType.constant:
      doc.semantic[(item_id, period)] = stamp_cell_source(
        document.formula_column(spec), **{document.FORECAST_RECORD: cleared[document.FORECAST_RECORD]},
      )
  # One lowering after the source writes; a role-period whose leaf an agent or judgment keeps is
  # not included, so its authored value stays an operand.
  held_keys = {(plan.covered[(row["item"], row["period"])], f"FY{row['period']}") for row in kept if (row["item"], row["period"]) in plan.covered}
  cleared_role_periods = [stale_inclusions[key] for key in unavailable_out if key in stale_inclusions]
  lowered = lower_inclusions(
    doc, inputs, plan.rows, kept=held_keys, cleared_role_periods=cleared_role_periods,
    previous_markers={key: spec for key, spec in previous_markers.items() if key in unavailable_out or key in clears},
  )
  # A C fact rewritten onto an aggregate the lowering re-derives ends where it started: not a change.
  changed = [
    {**row, "after": after}
    for row in changed
    if not _same_cell(row["before"], after := _cell_view(doc, (row["item"], row["period"])))
  ]
  added = [{"item": row["item"], "period": row["period"]} for row in changed if row["before"]["value"] is None and row["after"]["value"] is not None]
  removed = [{"item": row["item"], "period": row["period"]} for row in changed if row["before"]["value"] is not None and row["after"]["value"] is None]
  document.record_edit(
    doc,
    {"type": "refresh", "historical_periods": sorted(historical)},
    source={"origin": "c_refresh", "rows": len(inputs.rows)},
    result={
      "status": "ok", "cells_changed": len(changed), "cells_added": len(added), "cells_removed": len(removed), "kept": len(kept),
      "restated": len(restated), "revintaged": len(revintaged), "cells_lowered": len(lowered["cells_changed"]),
    },
  )
  released = document.release_provisional(doc)
  judgments_superseded, judgments_changed = _authored_outcome(judgments, doc)
  return {
    "cells_changed": changed,
    "cells_added": added,
    "cells_removed": removed,
    "kept": kept,
    "restated_cells": restated,
    "revintaged_cells": revintaged,
    "provisional_released": released,
    "judgments_untouched": len(judgments) - len(judgments_changed) - len(judgments_superseded),
    "judgments_changed": judgments_changed,
    "judgments_superseded": judgments_superseded,
    **_edit_counts(prior_edits, doc),
    "edits_before": len(prior_edits),
    "plan": {"counts": dict(plan.counts), "findings": plan.findings},
    "historical_years": structure.historical_years,
  }


def _recorded_helper_inputs(
  doc: document.ModelSheet, *, edits: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
  """Helper → the inputs its last construction edit recorded (``source.inputs``)."""
  out: dict[str, Any] = {}
  for edit in doc.edits if edits is None else edits:
    op = edit.get("op") or {}
    if op.get("type") == "construction":
      out[str(op["helper"])] = _plain((edit.get("source") or {}).get("inputs") or {})
  return out


#: Recorded inputs that name where a helper's inputs came from, not what it read: the bundle's id and
#: the ModelSpec's sha move whenever the bundle is prepared again. ``rerun_derivations`` never compares
#: them; a derivation helper's ``read_slice`` (``schema/helper_read_slices.py``) is what it read.
PROVENANCE_INPUTS = frozenset({"source_bundle", "model_spec"})


def _read_inputs(inputs: Mapping[str, Any]) -> dict[str, Any]:
  """``inputs`` as ``rerun_derivations`` compares them."""
  return {name: value for name, value in _plain(dict(inputs)).items() if name not in PROVENANCE_INPUTS}


#: ``correspondence`` is not a stage writer (it has no ``stage:`` segment): it names the C pass a
#: correspondence persist moved, scoped to ``moved_roles`` (``refresh_stage``).
DownstreamStage = Literal[
  "historical_foundation", "forecast_assumption_set", "scenario_set", "valuation_policy", "correspondence",
]


def _merge_construction_rows(
  parent: Collection[Mapping[str, Any]], rerun_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
  """Replace re-run helper rows by name, retaining the parent's construction order."""
  by_name = {str(row["helper"]): dict(row) for row in parent}
  order = [str(row["helper"]) for row in parent]
  for row in rerun_rows:
    name = str(row["helper"])
    if name not in by_name:
      order.append(name)
    by_name[name] = dict(row)
  return [by_name[name] for name in order]


def _correspondence_scope(
  doc: document.ModelSheet, scratch: SheetFrame, inputs: StageBuildInputs, ticker: str, moved_roles: Collection[str],
) -> tuple[frozenset[str], frozenset[str], frozenset[str]]:
  """What a correspondence move rewrites: the roles it names plus every role that shares a line with
  one of them (a line's cells are one role's last write, so they are rewritten together), the lines
  of those roles, and the roles of them C has no pristine line for (``unlined``: cold's
  ``construct_from_inputs`` rule, over the pristine structure). A role's lines are the live
  document's identities for it (``document._inclusion_role_items``, the C-stamped columns included)
  and ``_role_targets`` of the new inputs on the pristine structure."""
  held = document._inclusion_role_items(doc)
  targets = _role_targets(scratch.doc, inputs, scratch.registry, ticker)
  lines = {role: held.get(role, set()) | set(targets.get(role, ())) for role in held.keys() | targets.keys()}
  roles = set(moved_roles)
  while True:
    items = {item_id for role in roles for item_id in lines.get(role, ())}
    grown = {role for role, owned in lines.items() if role not in roles and owned & items}
    if not grown:
      break
    roles |= grown
  unlined = {row.required_role for row in inputs.rows if not targets.get(row.required_role)}
  return frozenset(roles), frozenset(items), frozenset(unlined & roles)


def _without_cell_edits(doc: document.ModelSheet, segment: EditSegment, items: Collection[str]) -> EditSegment:
  """``segment`` less the writer edits of cells on ``items`` (a role's rows are written again)."""
  from model_build import live_sheet

  kept: list[dict[str, Any]] = []
  for edit in segment.edits:
    kind = (edit.get("op") or {}).get("type")
    if kind in ("set_value", "clear_input", "not_separately_presented") and str(edit["op"]["item_id"]) in items:
      continue
    if kind == "not_separately_presented_lowering":
      cells = [row for row in (edit.get("result") or {}).get("cells") or () if str(row["item_id"]) not in items]
      if cells:
        kept.append({**edit, "result": {**(edit.get("result") or {}), "cells": cells}})
      continue
    kept.append(edit)
  edits = tuple(kept)
  return EditSegment(segment.name, edits, frozenset(item_id for item_id, _period in live_sheet.writer_footprint(doc, edits)))


def refresh_stage(
  structure: SheetFrame, inputs: StageBuildInputs, ticker: str, refusals: _Refusals, *,
  stages: Collection[DownstreamStage],
  parent_construction: Construction,
  parent_segments: Sequence[EditSegment],
  footprints: Mapping[str, frozenset[tuple[str, int]]],
  scratch: SheetFrame,
  moved_roles: Collection[str] = (),
) -> Constructed:
  """Refresh moved judgment stages on the pre-ops sheet, with no C or structural rebuild.

  Restore every moved writer's former cells before running any writer, in cold writer order.
  Derivations follow changed recorded inputs and dirty lines; agent ops belong to the caller,
  replayed only after construction. No cold comparison or runtime fallback runs here.

  ``correspondence`` (``moved_roles``: the roles ``c_inputs_for`` projected again) restores the cells
  the historical and refill segments wrote on the lines of those roles and of the roles that share
  them, and writes C again for them; the four stage writers' segments stand, and the seed writer is
  applied over them once more, as cold applies it after C: a seed lands where C leaves the cell.
  """
  from model_build import live_sheet

  doc = structure.doc
  order = live_sheet.period_order(doc)
  doc.model_spec = inputs.model_spec
  moved = [stage for stage in live_sheet.STAGE_WRITERS if stage in stages]
  restored = frozenset(cell for stage in moved for cell in footprints.get(stage, frozenset()))
  parent_by_name = {segment.name: segment for segment in parent_segments}
  missing_cells = live_sheet.writer_footprint(doc, parent_by_name.get("missing", EditSegment("missing", ())).edits)
  restored = restored | missing_cells
  correspondence = "correspondence" in stages
  closure_roles: frozenset[str] = frozenset()
  closure_items: frozenset[str] = frozenset()
  unlined_roles: frozenset[str] = frozenset()
  if correspondence:
    closure_roles, closure_items, unlined_roles = _correspondence_scope(doc, scratch, inputs, ticker, moved_roles)
    restored = restored | {
      cell
      for segment in parent_segments
      if segment.name == "historical" or segment.name.startswith("glue:historical_refill:")
      for cell in live_sheet.writer_footprint(doc, segment.edits)
      if cell[0] in closure_items
    }
  prior_findings = [finding for findings in parent_construction.helper_findings.values() for finding in findings]
  inputs.findings[:] = [finding for finding in inputs.findings if finding not in prior_findings]
  period_restored: set[tuple[str, int]] = set()
  retained_periods: dict[str, set[int]] = {}
  bm_periods = bool({"historical_foundation", "correspondence"} & set(stages))
  if bm_periods:
    for segment in parent_segments:
      retained_writer = (
        segment.name == "historical" or segment.name.startswith("glue:historical_refill:")
        or (
          segment.name.startswith("stage:")
          and segment.name.removeprefix("stage:") not in stages
        )
      )
      if not retained_writer:
        continue
      for item_id, period in live_sheet.writer_footprint(doc, segment.edits):
        if correspondence and (item_id, period) in restored:
          continue
        if (doc.semantic.get((item_id, period)) or {}).get("cell") == "input":
          retained_periods.setdefault(item_id, set()).add(period)
  with document.transaction(doc):
    live_sheet.restore_footprint(doc, restored, scratch.doc.model)
    if bm_periods:
      from schema.build_bm_node_history import _direct_segment_revenue_node_key, _formula_line_refs

      all_periods = set(structure.historical_years) | set(structure.forecast_years)

      def restore_periods(item_id: str) -> None:
        live, pristine = doc.model._index.get(item_id), scratch.doc.model._index.get(item_id)
        if live is None or pristine is None:
          return
        # Scratch precedes C. Its periods cannot revoke an unchanged writer's authored input.
        restored_periods = (
          None if pristine.formula_periods is None
          else sorted(set(pristine.formula_periods) | retained_periods.get(item_id, set()))
        )
        if live.formula_periods == restored_periods:
          return
        held = all_periods if live.formula_periods is None else set(live.formula_periods)
        original = all_periods if restored_periods is None else set(restored_periods)
        period_restored.update((item_id, int(period)) for period in held ^ original)
        live.formula_periods = restored_periods

      # The old tail pruned BM ownership transitively; it can only remove periods, not recover
      # those made computable by new seeds. Restart that ownership from the pristine structure.
      bm_ids = {item_id for item_id in scratch.doc.model._index if item_id.startswith("bm.")}
      for item_id, pristine in scratch.doc.model._index.items():
        specs = (pristine.historical, pristine.projected)
        if any(spec is not None and spec.type is FormulaType.roll_forward for spec in specs):
          continue
        if item_id in bm_ids or (
          pristine.item_type is ItemType.input and any(
            ref.id in bm_ids
            for spec in (*specs, *(pristine.overrides or {}).values())
            if spec is not None
            for ref in _formula_line_refs(spec.params)
          )
        ):
          restore_periods(item_id)
      if inputs.revision is not None:
        # Direct cycle release also disowns the statement face outside the writer's edits.
        for segment in inputs.revision.business_model.segments:
          index = scratch.registry.segment_mapping.get(segment.id)
          if index is not None and _direct_segment_revenue_node_key(segment) is not None:
            restore_periods(f"tpl.fm.income_statement.business_segment_{index}_revenue")
  doc.edits = []
  held_segments = parent_segments
  if correspondence:
    # The rewritten roles' lines are written again below: their earlier writes leave the log, as cold's has none.
    held_segments = [
      _without_cell_edits(doc, segment, closure_items)
      if segment.name == "historical" or segment.name.startswith("glue:historical_refill:") else segment
      for segment in parent_segments
    ]
  segments = _EditSegments(doc, held_segments)
  for name in ("structure", "company", "historical"):
    segments.keep(name)
  forecast: Mapping[str, Any] = {}
  written: dict[str, frozenset[tuple[str, int]]] = {}
  corr_written: frozenset[tuple[str, int]] = frozenset()
  if correspondence:
    with document.transaction(doc):
      # a role C has no pristine line for is written by the refills, after the helpers that make its line
      segments.run("historical", lambda: write_historical_cells(structure, inputs, ticker, roles=closure_roles - unlined_roles))
      corr_written = live_sheet.writer_footprint(doc, segments.last_edits)
  seeded: frozenset[tuple[str, int]] | None = None
  with document.transaction(doc):
    for stage, writer in live_sheet.STAGE_WRITERS.items():
      name = f"stage:{stage}"
      reapplied = correspondence and stage == "historical_foundation"
      if stage not in moved:
        segments.keep(name)
        if not reapplied:
          continue
      if stage == "valuation_policy":
        result = segments.run(name, lambda: writer(structure, inputs, refusals))
      else:
        result = segments.run(name, lambda: writer(structure, inputs))
      if stage == "forecast_assumption_set":
        forecast = result
      written[stage] = live_sheet.writer_footprint(doc, segments.last_edits)
      if reapplied:
        # the held seeds stand, less the cells C now holds or released; the seeds C leaves are new
        seeded = (footprints.get(stage, frozenset()) - restored - corr_written) | written[stage]
  construction = rerun_derivations(
    doc, inputs, ticker, parent_construction.rows, scratch=scratch, trace=False,
    protected=document.Protected(), dirty=restored.union(period_restored, corr_written, *written.values()),
    role_dispositions=parent_construction.role_dispositions, role_items=parent_construction.role_items,
    stages=stages, unlined_roles=unlined_roles,
    parent_segments=parent_segments, segments=segments, helper_findings=parent_construction.helper_findings,
  )
  reran = {row["helper"] for row in construction.rows}
  by_name = {row["helper"]: row for row in _merge_construction_rows(parent_construction.rows, construction.rows)}
  if "forecast_assumption_set" in stages:
    by_name.pop("forecast_assumptions", None)
    if forecast.get("construction") is not None:
      by_name["forecast_assumptions"] = forecast["construction"]
  row_order = (["forecast_assumptions"] if "forecast_assumptions" in by_name else []) + construction.helper_order
  merged = Construction(
    rows=[by_name[name] for name in row_order if name in by_name],
    retired=list(construction.retired if "retire_unused_cost_of_debt" in reran else parent_construction.retired),
    unresolved_nodes=list(construction.unresolved_nodes if "business_model_node_history" in reran else parent_construction.unresolved_nodes),
    refills=list(construction.refills), traced=False,
    scenario_controls=construction.scenario_controls if construction.scenario_controls is not None else parent_construction.scenario_controls,
    role_dispositions=construction.role_dispositions if construction.role_dispositions is not None else parent_construction.role_dispositions,
    role_items=construction.role_items if construction.role_items is not None else parent_construction.role_items,
    edit_segments=construction.edit_segments, helper_order=list(construction.helper_order),
    helper_findings=construction.helper_findings,
  )
  presented = segments.run("presented", lambda: write_presented_case(structure))
  missing = segments.run("missing", lambda: mark_missing_inputs(
    structure, missing_helpers=merged.missing(), retired_items=merged.retired,
    unresolved_nodes=merged.unresolved_nodes,
  ))
  c_cells = frozenset(
    cell for segment in segments.segments
    if segment.name == "historical" or segment.name.startswith("glue:historical_refill:")
    for cell in live_sheet.writer_footprint(doc, segment.edits)
  ) if correspondence else frozenset()
  live_sheet.keep_period_order(
    doc, order, pristine=live_sheet.period_order(scratch.doc) if correspondence else None, written=c_cells,
  )
  return Constructed(
    doc=doc, historical={}, seeds={}, valuation={}, scenarios={"presented_case_cells": presented},
    before={}, forecast={}, unlined=frozenset(), construction=merged, missing=missing,
    footprints={**footprints, **written, **({} if seeded is None else {"historical_foundation": seeded})},
    edit_segments=tuple(segments.segments),
  )


def _recorded_helper_items(
  doc: document.ModelSheet, *, edits: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, frozenset[str]]:
  """Helper → lines its last edit changed or tried to change over an authored cell."""
  out: dict[str, frozenset[str]] = {}
  for edit in doc.edits if edits is None else edits:
    op = edit.get("op") or {}
    if op.get("type") == "construction":
      result = edit.get("result") or {}
      out[str(op["helper"])] = (
        frozenset(str(pair[0]) for pair in result.get("cells") or ())
        | frozenset(str(pair[0]) for pair in result.get("kept_cells") or ())
        | frozenset(str(item_id) for item_id in result.get("field_changes") or ())
        | frozenset(str(item_id) for item_id in result.get("formulas") or ())
        | frozenset(str(row["item_id"]) for row in result.get("kept_authored") or () if isinstance(row, Mapping) and "item_id" in row)
      )
  return out


def rerun_derivations(
  doc: document.ModelSheet, inputs: StageBuildInputs, ticker: str, parent_construction: Collection[Mapping[str, Any]],
  *, axis_moved: bool = False, scratch: SheetFrame | None = None,
  protected: document.Protected | None = None, dirty: Collection[tuple[str, int]] = (),
  role_dispositions: Any = None, role_items: Any = None, trace: bool = True,
  stages: Collection[DownstreamStage] = (),
  parent_segments: Sequence[EditSegment] = (), segments: _EditSegments | None = None,
  helper_findings: Mapping[str, Sequence[dict[str, Any]]] | None = None,
  unlined_roles: Collection[str] = (),
) -> Construction:
  """Re-run the derivation helpers (``DERIVATION_HELPERS``) on a reopened document, in
  ``construct_document`` order: each that was ``missing`` or ``refused`` in the parent and whose
  inputs are now present (``rerun: parent_missing`` / ``parent_refused``), and each whose recorded
  inputs (its last construction edit's ``source.inputs``) differ from the current ones
  (``rerun: inputs_changed``); with ``axis_moved`` (a period advance: the axis is an input of every
  helper) each of them and each of the ``PROJECTION_SEED_HELPERS`` (``rerun: axis_moved``). Each
  re-run is one construction edit with
  ``reason: refresh`` (``advance``); the agent's lines and cells (``apply_ops``) and every
  judgment-row cell are protected. Template load and BusinessModel lowering never re-run on the
  document: ``scratch`` (default ``build_structure`` on a scratch template the caller discards)
  gives the registry the helpers read and the pre-construction structure (``Rerun.template``).

  ``protected`` replaces the usual agent/judgment protection; pre-ops stage refresh passes an
  empty set. ``dirty`` selects derivation helpers whose recorded lines intersect it and carries
  their lines forward to later helpers. Offline callers retain their default protection and trace.
  ``stages`` identifies an HFF refresh whose seed changes require the BusinessModel-period tail. A
  ``correspondence`` refresh also re-runs the projection seeds (they read the latest actual year's
  cells) and refills ``unlined_roles``, the moved roles C has no pristine line for (cold's refills)."""
  parent_status = {str(row["helper"]): row.get("status") for row in parent_construction}
  prior_edits = tuple(edit for segment in parent_segments for edit in segment.edits) if parent_segments else None
  recorded = _recorded_helper_inputs(doc, edits=prior_edits)

  rerunnable = DERIVATION_HELPERS | PROJECTION_SEED_HELPERS if axis_moved else DERIVATION_HELPERS
  if set(stages) & {"historical_foundation", "forecast_assumption_set", "correspondence"}:
    rerunnable |= PROJECTION_SEED_HELPERS
  touched = {item_id for item_id, _period in dirty}
  items_of = _recorded_helper_items(doc, edits=prior_edits) if touched or stages else {}
  for segment in parent_segments:
    if segment.name.startswith("helper:"):
      name = segment.name.removeprefix("helper:")
      items_of[name] = items_of.get(name, frozenset()) | segment.items

  selected: list[str] = []

  def select(helper: Helper, current: Mapping[str, Any]) -> str | None:
    why = _select_derivation(helper, current) or _select_cells(helper)
    if why is not None:
      selected.append(helper.name)
      touched.update(items_of.get(helper.name, ()))
    return why

  def _select_derivation(helper: Helper, current: Mapping[str, Any]) -> str | None:
    if helper.name in VALUATION_RETIREMENT_HELPERS:
      # A re-run derivation rebuilds the rows these retire: they follow it, in construction order.
      return "derivation_rerun" if selected else None
    if stages and helper.name not in parent_status:
      return "parent_missing"
    if stages and helper.name in recorded and _read_inputs(current) != _read_inputs(recorded[helper.name]):
      return "inputs_changed"
    if helper.name not in rerunnable:
      return None
    status = parent_status.get(helper.name)
    if status in ("missing", "refused"):
      return f"parent_{status}"
    if axis_moved:
      return "axis_moved"
    if helper.name in PROJECTION_SEED_HELPERS and stages:
      return "projection_inputs_changed"
    if helper.name in recorded and _read_inputs(current) != _read_inputs(recorded[helper.name]):
      return "inputs_changed"
    return None

  def _select_cells(helper: Helper) -> str | None:
    if not items_of.get(helper.name, frozenset()) & touched:
      return None
    return "items_touched"

  candidates = rerunnable if axis_moved else DERIVATION_HELPERS & (
    set(recorded) | {name for name, status in parent_status.items() if status in ("missing", "refused")}
  )
  if not candidates and not touched and not stages:
    return Construction()
  base = _document_structure(doc)
  if scratch is None:
    scratch = _captured(inputs, "construction_rerun_structure", lambda: build_structure(inputs, ticker))
  if scratch is None:
    return Construction()
  structure = SheetFrame(doc=doc, registry=scratch.registry, historical_years=base.historical_years, forecast_years=base.forecast_years)
  held = document._protected(doc) if protected is None else protected
  # The edits a resets_structure helper's trial is put back from: the parent's, under segments, else the document's.
  reset_from: Sequence[Mapping[str, Any]] = doc.edits if prior_edits is None else prior_edits

  def changed(row: Mapping[str, Any]) -> None:
    touched.update(str(pair[0]) for pair in row.get("cells") or ())
    touched.update(str(pair[0]) for pair in row.get("kept_cells") or ())
    touched.update(str(item_id) for item_id in row.get("items_added") or ())
    touched.update(str(item_id) for item_id in row.get("items_removed") or ())
    touched.update(str(item_id) for item_id in row.get("field_changes") or ())

  def prepare(helper: Helper) -> None:
    if helper.resets_structure:
      # The lines construct's trial will get back (and so this pass touched): planned here, applied on the copy there.
      touched.update(_recorded_structure_to_reset(doc.model, helper.name, reset_from, held))
    if segments is None:
      return
    name = {
      "valuation_inputs": "glue:relative_absences",
      "company_grounded_schedules": "glue:schedule_absences",
    }.get(helper.name)
    segment = None if name is None else segments.parent.get(name)
    if segment is None:
      return
    from model_build import live_sheet

    cells = frozenset(
      cell
      for edit in segment.edits
      for cell in live_sheet.writer_footprint(doc, (edit,))
      if (column := doc.semantic.get(cell) or {}).get("cell") == "unavailable"
      and all(column.get(key) == value for key, value in (edit.get("source") or {}).items())
    )
    if not cells:
      return
    start = len(doc.edits)
    live_sheet.restore_footprint(doc, cells, scratch.doc.model)
    del doc.edits[start:]
    touched.update(item_id for item_id, _period in cells)
  rerun = Rerun(
    select=select, protect=held, reason="advance" if axis_moved else "refresh", template=scratch.doc.model,
    role_dispositions=role_dispositions, role_items=role_items, stages=stages,
    segments=segments, helper_findings=helper_findings or {}, prepare=prepare, reset_from=reset_from,
    changed=changed if stages else None,
  )
  return construct_document(structure, inputs, ticker, forecast={}, unlined_roles=unlined_roles, rerun=rerun, trace=trace)


def _parent_inputs_refs(parent_inputs: Mapping[str, Any]) -> dict[str, Any]:
  """The judgment refs the parent snapshot was built from, as ``load_inputs`` takes them. A ref
  the parent recorded as ``None`` (it had none) resolves to the current default, which may name
  an artifact accepted since: the refresh manifest records what was loaded (``judgment_refs``)."""
  named = {"fas": "forecast_assumption_set", "scenario_set": "scenario_set", "valuation_policy": "valuation_policy"}
  return {key: parent_inputs[name] for key, name in named.items() if parent_inputs.get(name) is not None}


def judgment_refs(inputs: StageBuildInputs) -> dict[str, Any]:
  """The manifest ``inputs`` fields naming the judgment artifacts ``load_judgment_inputs`` loaded."""
  return {
    "historical_foundation": _ref_json(inputs.hff),
    "forecast_assumption_set": _ref_json(inputs.fas),
    "scenario_set": _ref_json(inputs.scenario_set),
    "valuation_policy": _ref_json(inputs.valuation_policy),
    "finalized_handoff": inputs.handoff,
  }


def target_refresh(
  workdir: Path, source: BuildSource, *, out: Path, soffice: str | None = None, accept_gaps: Collection[str] = (),
  cutoff_at: datetime | str | None = None,
) -> dict[str, Any]:
  """Refresh a saved snapshot's document from the source's current C and snapshot it under ``out``.

  The document package is reopened (edits, semantic columns, values), never reconstructed from
  template: ``refresh_cells`` writes the changed C cells, ``rerun_derivations`` re-runs the
  derivation helpers whose inputs changed or that the parent lacked, then recalc, render + native
  compare, ``target_check``. The judgment artifacts are the parent's (its manifest's FAS /
  ScenarioSet / ValuationPolicy refs; one it recorded as ``None`` resolves to the current default,
  and once a helper runs the manifest's ``inputs`` name the refs loaded). C's vintage cutoff is
  ``cutoff_at``, by default the
  parent's (``manifest.axis.cutoff_at``; a parent that records none: the latest captured filing's
  acceptance). ``<out>/snapshot/refresh.json``: every cell written (``before`` / ``after`` value
  and source), the cells added and removed, the vintage changes (``restated_cells`` /
  ``revintaged_cells``, also in ``provenance.json``), the cutoff before and after, ``helpers_rerun``,
  every other cell whose recalc changed (``downstream_changed``), the judgment cells and edits
  untouched and those an actual superseded (``forecast_vs_actual``, ``forecasts_superseded``,
  ``edits_superseded_by_actual``); ``manifest.parent_snapshot`` names the input snapshot. ``verdict`` as
  ``target_build``."""
  return _refresh_snapshot(
    workdir, source, out=out, soffice=soffice, accept_gaps=accept_gaps, new_actual_year=None,
    cutoff_at=parse_cutoff(cutoff_at), keep_parent_cutoff=cutoff_at is None,
  )


def target_advance(
  workdir: Path, source: BuildSource, *, out: Path, new_actual_year: int, soffice: str | None = None,
  accept_gaps: Collection[str] = (), cutoff_at: datetime | str | None = None,
) -> dict[str, Any]:
  """Advance a saved snapshot's document one period (J7) and snapshot it under ``out``.

  The package is reopened and ``document.advance_period`` moves the boundary: ``new_actual_year``
  becomes historical, the projection window gains the following year, every edit stays at its
  ``(item, year)``, and every forecast the new actual year holds (the projection, the agent's
  input, a judgment row, a construction seed) stays live, marked provisional, until the year's
  actual supersedes it. The cutoff moves too: ``cutoff_at``, by default the latest captured
  filing's acceptance. Then, as ``target_refresh``, C against the source's current mappings at that
  cutoff (``refresh_cells``: the new actual year's cells from their ``FilingLineRef``s, and every
  period whose selected vintage changed — a comparative a newer filing restates, retained beside
  the vintage the cell held in ``source_history``), the HFF's driver seeds of the new actual year,
  the parent's FAS lowered onto the moved projection window and every derivation helper re-run
  (``rerun: axis_moved``), all under protection of the agent's lines and cells and the judgment
  rows; provisional cells released to their historical formulas where their inputs are now
  sourced. The actual wins: a superseded forecast is the actual cell's ``forecast_record`` and its
  edit is marked ``superseded_by_actual``. The new actual year's unsourced gaps are relabelled
  historical and every input nothing wrote marked (``mark_missing_inputs``); recalc, render +
  native compare, ``target_check``. ``refresh.json`` adds ``boundary_moved``, ``axis_before`` /
  ``axis_after``, ``new_actual_cells``, ``provisional_projection_cells`` (forecasts still live: no
  actual yet), ``forecast_vs_actual``, ``forecasts_superseded``, ``edits_preserved`` /
  ``edits_superseded_by_actual``, ``judgments_untouched`` / ``judgments_superseded`` and
  ``agent_cells``; the manifest records ``lineage: advance`` with ``axis_before`` /
  ``axis_after`` and ``axis.cutoff_at``."""
  return _refresh_snapshot(
    workdir, source, out=out, soffice=soffice, accept_gaps=accept_gaps, new_actual_year=int(new_actual_year),
    cutoff_at=parse_cutoff(cutoff_at), keep_parent_cutoff=False,
  )


def _relabel_actual_gaps(doc: document.ModelSheet, year: int, *, unresolved_nodes: Collection[str] = ()) -> int:
  """An unsourced projection gap (``mark_missing_inputs``' ``required_dependency_missing``) in a
  year the advance made historical is a historical gap: ``source_observation_absent``, or
  ``business_model_node_unresolved`` on a line ``unresolved_nodes`` names, the reason
  ``mark_missing_inputs`` gives that line's historical cells (so a built and an advanced year agree)."""
  relabelled = 0
  for (item_id, period), column in sorted(doc.semantic.items()):
    if (
      period == year and column.get("cell") == "unavailable" and column.get("reason") == "required_dependency_missing"
      and (column.get("source") or {}).get("kind") == "none"
    ):
      unresolved = item_id in unresolved_nodes
      reason: MissingReasonCode = "business_model_node_unresolved" if unresolved else "source_observation_absent"
      note = (
        "the node's history is not written; its business_model_node_unresolved finding is the record"
        if unresolved else "no required role maps this line"
      )
      source: dict[str, Any] = {"source": {"kind": "none", "note": note}}
      if column.get("gap"):
        source["gap"] = column["gap"]
      document.mark_unavailable(doc, item_id, period, reason=reason, source=source)
      relabelled += 1
  return relabelled


def _refresh_snapshot(
  workdir: Path, source: BuildSource, *, out: Path, soffice: str | None, accept_gaps: Collection[str], new_actual_year: int | None,
  cutoff_at: datetime | None, keep_parent_cutoff: bool,
) -> dict[str, Any]:
  """``target_refresh`` (``new_actual_year`` ``None``) and ``target_advance``. C runs at
  ``cutoff_at``; with none given, at the parent's recorded cutoff when ``keep_parent_cutoff``,
  else at the latest captured filing's acceptance."""
  ticker = source.ticker
  parent = _snapshot_dir(Path(workdir))
  parent_document = parent / DOCUMENT_NAME
  parent_manifest = json.loads((parent / "manifest.json").read_text())
  parent_inputs = dict(parent_manifest.get("inputs") or {})
  parent_cutoff = parse_cutoff((parent_manifest.get("axis") or {}).get("cutoff_at"))
  if cutoff_at is None and keep_parent_cutoff:
    cutoff_at = parent_cutoff
  doc = document.reopen(parent_document)
  template = _snapshot_template(parent)
  before = document.recalc(doc)
  # Read before the run writes ``out``, which may be the parent's own directory.
  parent_ref = {
    "path": str(parent),
    "manifest_sha256": _sha256(parent / "manifest.json"),
    "values_sha256": parent_manifest.get("values_sha256"),
    "built_at": parent_manifest.get("built_at"),
  }
  parent_values_reproduced = document.values_digest(before) == document.saved_values_digest(parent_document)
  out = Path(out).expanduser()
  snapshot = out / SNAPSHOT_DIR
  snapshot.mkdir(parents=True, exist_ok=True)
  refusals = _Refusals()
  result: dict[str, Any] = {"ticker": ticker, "parent": str(parent), "workdir": str(out), "snapshot": str(snapshot)}
  inputs = refusals.run("inputs", lambda: source.load_c_inputs(refusals, cutoff_at=cutoff_at))
  if inputs is None:
    return {**result, "refusals": refusals.rows, "verdict": _verdict(refusals, None, None)}
  # The judgment artifacts feed only the derivation re-runs: one that does not load is an input
  # finding, and the helpers that need it stay as the parent left them.
  judgment = _captured(inputs, "judgment_inputs", lambda: source.load_judgment_inputs(inputs, _parent_inputs_refs(parent_inputs)))
  prior_edits = _plain(doc.edits)
  agent_items, agent_cells = document._agent_authored(doc)
  agent_views = {key: _forecast_view(doc, key) for key in sorted(agent_cells)}
  judgment_views = _judgment_views(doc)
  recorded_before = forecast_records(doc)
  advanced = None
  if new_actual_year is not None:
    advanced = document.advance_period(doc, new_actual_year, values=before, source={
      "origin": "target_advance",
      "corpus": {"corpus_id": inputs.corpus.corpus_id, "corpus_version": inputs.corpus.corpus_version, "manifest_sha256": inputs.corpus.manifest_sha256},
    })
  refreshed = refresh_cells(doc, inputs, ticker)
  revision = _ref_json(inputs.revision)
  rerun = Construction()
  lowered: list[dict[str, Any]] = []
  seeds: dict[str, Any] = {"written": 0, "findings": []}
  if revision != parent_inputs.get("business_model_revision"):
    inputs.findings.append({
      "stage": "refresh", "code": "business_model_revision_differs_from_document", "owner_file": None,
      "message": (
        f"selected {revision}; the document's structure was lowered from {parent_inputs.get('business_model_revision')}; "
        "no construction helper re-runs against another revision's registry"
      ),
    })
  elif advanced is None:
    rerun = rerun_derivations(doc, inputs, ticker, parent_manifest.get("construction") or ())
  else:
    scratch = _captured(inputs, "construction_rerun_structure", lambda: build_structure(inputs, ticker))
    if scratch is not None:
      base = _document_structure(doc)
      structure = SheetFrame(doc=doc, registry=scratch.registry, historical_years=base.historical_years, forecast_years=base.forecast_years)
      # The agent's forecasts the year's actual has not yet superseded yield to its driver seeds.
      live_agent = document._agent_authored(doc)[1] - set(document.provisional_cells(doc))
      seeds = write_seed_cells(structure, inputs, periods=(advanced["new_actual_year"],), protect=live_agent)
      forecast = write_forecast_cells(structure, inputs, reason="advance", protect=document._protected(doc))
      if forecast["construction"] is not None:
        lowered.append({**forecast["construction"], "rerun": "axis_moved", "findings": forecast["findings"]})
      rerun = rerun_derivations(
        doc, inputs, ticker, parent_manifest.get("construction") or (), axis_moved=True, scratch=scratch,
      )
  replay_tail(doc, rerun)
  released_after = document.release_provisional(doc) if advanced is not None else []
  relabelled = 0 if advanced is None else _relabel_actual_gaps(
    doc, advanced["new_actual_year"], unresolved_nodes=rerun.unresolved_nodes,
  )
  marked = {} if advanced is None else mark_missing_inputs(_document_structure(doc), retired_items=rerun.retired)
  helpers_rerun = [
    {key: row.get(key) for key in ("helper", "rerun", "status", "inputs_written", "code", "owner_file")}
    | {"kept_authored": len(row.get("kept_authored") or ()), "superseded_unavailable": len(row.get("superseded_unavailable") or ())}
    for row in [*lowered, *rerun.rows]
  ]
  rerun_status = {row["helper"]: row["status"] for row in [*lowered, *rerun.rows]}
  # A parent whose construction is unknown (an edit snapshot with no origin) stays unknown.
  parent_construction = _recorded_construction(parent_manifest)
  construction_rows = None if parent_construction is None else [
    {**row, "status": rerun_status.get(row["helper"], row.get("status"))} for row in parent_construction
  ]
  ours = document.recalc(doc)
  written = {(row["item"], row["period"]) for row in refreshed["cells_changed"]}
  downstream = [row for row in document.value_diff(before, ours) if (row["item"], row["period"]) not in written]
  compared = compare_into(snapshot, doc, refusals, soffice)
  unclaimed = refusals.run("filed_line_unclaimed", lambda: unclaimed_filed_lines(
    inputs.hff, _document_structure(doc).historical_years, inputs.corpus,
    occurrence_envelopes=inputs.occurrence_envelopes, statement_bindings=inputs.statement_bindings,
  )) or []
  scenario_controls = rerun.scenario_controls or _snapshot_scenario_controls(parent)
  check = target_check(doc, validation=document_validation(
    doc, inputs, ticker, unclaimed, refusals, template=template, scenario_controls=scenario_controls,
  ))
  resolution = key_resolution(semantic_rows(doc, ours), _document_structure(doc).historical_years)
  gaps = any(row.get("status") in ("missing", "refused") for row in construction_rows or ())
  incomplete = incomplete_reasons(
    construction_rows, compared=compared, check=check,
    unresolved=inputs_unresolved(inputs) if gaps else (), accept_gaps=accept_gaps,
  )
  verdict = _verdict(refusals, resolution, compared is not None, incomplete)
  # Over the whole run (the advance included): a fixed-cell line's keys moved with the boundary.
  moved = set(() if advanced is None else advanced["fixed_cells_shifted"])

  def now(key: tuple[str, int]) -> tuple[str, int]:
    return (key[0], key[1] + 1) if key[0] in moved else key

  edit_counts = _edit_counts(prior_edits, doc)
  judgments_superseded, judgments_changed = _authored_outcome(judgment_views, doc, now)
  agent_superseded, agent_changed = _authored_outcome(agent_views, doc, now)
  superseded = sorted(forecast_records(doc) - recorded_before)
  versus = forecast_vs_actual(doc, ours, superseded)
  refresh: dict[str, Any] = {
    "parent_snapshot": parent_ref,
    "construction_rerun": bool(rerun.rows or lowered),
    "helpers_rerun": helpers_rerun,
    "parent_values_reproduced": parent_values_reproduced,
    "cells_changed": refreshed["cells_changed"],
    "cells_added": refreshed["cells_added"],
    "cells_removed": refreshed["cells_removed"],
    "restated_cells": refreshed["restated_cells"],
    "revintaged_cells": refreshed["revintaged_cells"],
    "cutoff": {
      "before": None if parent_cutoff is None else parent_cutoff.isoformat(),
      "after": None if inputs.cutoff_at is None else inputs.cutoff_at.isoformat(),
    },
    "vintages": vintage_counts(inputs),
    "downstream_changed": downstream,
    "judgments_untouched": len(judgment_views) - len(judgments_changed) - len(judgments_superseded),
    "judgments_changed": judgments_changed,
    "judgments_superseded": len(judgments_superseded),
    "forecasts_superseded": len(superseded),
    "forecast_vs_actual": versus,
    **edit_counts,
    "edits_before": len(prior_edits),
    "agent_cells": {
      "before": len(agent_views), "preserved": len(agent_views) - len(agent_changed) - len(agent_superseded),
      "changed": agent_changed, "superseded": len(agent_superseded), "lines": sorted(agent_items),
    },
    "kept": refreshed["kept"],
  }
  counts = {
    "cells_changed": len(refreshed["cells_changed"]),
    "cells_added": len(refreshed["cells_added"]),
    "cells_removed": len(refreshed["cells_removed"]),
    "restated_cells": len(refreshed["restated_cells"]),
    "revintaged_cells": len(refreshed["revintaged_cells"]),
    "helpers_rerun": len(helpers_rerun),
    "downstream_changed": len(downstream),
    "kept": len(refreshed["kept"]),
    "judgments_untouched": refresh["judgments_untouched"],
    "judgments_superseded": refresh["judgments_superseded"],
    "forecasts_superseded": len(superseded),
    **edit_counts,
  }
  axis_now = document.axis(doc.model)
  if advanced is not None:
    year = advanced["new_actual_year"]
    new_actual = [
      {
        "item": row["item"], "value": row["after"]["value"], "owner": row["owner"],
        "accession": _vintage(row["after"]["source"]), "observation_id": ((row["after"]["source"] or {}).get("source") or {}).get("observation_id"),
      }
      for row in refreshed["cells_changed"] if row["period"] == year
    ]
    provisional = [{"item": key[0], "period": key[1], **mark} for key, mark in sorted(document.provisional_cells(doc).items())]
    added_year = advanced["new_projection_year"]
    new_year_states = Counter(value.state for (item_id, period), value in ours.items() if period == added_year)
    refresh.update({
      "boundary_moved": {"from": advanced["axis_before"]["historical"][-1], "to": year},
      "axis_before": advanced["axis_before"],
      "axis_after": axis_now,
      "new_actual_cells": new_actual,
      "seeds": seeds,
      "provisional_projection_cells": provisional,
      "provisional_pinned": len(advanced["provisional_projection"]),
      "provisional_released": [*refreshed["provisional_released"], *released_after],
      "actual_gaps_relabelled": relabelled,
      "marked_unavailable": marked,
      "new_projection_year": {"year": added_year, "states": dict(new_year_states), "formula_periods_extended": len(advanced["formula_periods_extended"])},
      "fixed_cells_shifted": len(moved),
    })
    counts.update({
      "new_actual_cells": len(new_actual),
      "new_actual_filing_cells": sum(1 for row in new_actual if row["accession"] is not None),
      "seeds_written": seeds["written"],
      "provisional_pinned": refresh["provisional_pinned"],
      "provisional_released": len(refresh["provisional_released"]),
      "provisional_projection_cells": len(provisional),
      "agent_cells_preserved": refresh["agent_cells"]["preserved"],
    })
  manifest = {
    **{key: value for key, value in parent_manifest.items() if key not in ("files", "values_sha256", "refresh", "advance", "axis_before", "axis_after")},
    "built_at": datetime.now(UTC).isoformat(),
    "bench_sha": _bench_sha(),
    "lineage": "refresh" if advanced is None else "advance",
    "store": store_label(source),
    "corpus": {"corpus_id": inputs.corpus.corpus_id, "corpus_version": inputs.corpus.corpus_version, "manifest_sha256": inputs.corpus.manifest_sha256},
    "profile": {"profile_id": inputs.profile.profile_id, "profile_version": inputs.profile.profile_version},
    # Lineage names what the helpers read: once one ran (a re-run, the lowered FAS, a driver
    # seed), the judgment refs are the loaded ones, which differ from the parent's only where
    # the parent had none (a ref it named is passed through ``_parent_inputs_refs``).
    "inputs": {
      **parent_inputs, "business_model_revision": revision, "business_model_selection_epoch": inputs.selection_epoch,
      **(judgment_refs(inputs) if judgment is not None and (rerun.rows or lowered or seeds["written"]) else {}),
      **({"source_bundle": inputs.bundle_id} if rerun.rows or lowered else {}),
    },
    "values_sha256": document.values_digest(ours),
    "parent_snapshot": parent_ref,
    "construction": construction_rows,
    "construction_rerun": bool(rerun.rows or lowered),
    "helpers_rerun": [row["helper"] for row in helpers_rerun],
    "verdict": verdict,
    "incomplete": incomplete,
    "accepted_gaps": sorted(accept_gaps),
  }
  if advanced is None:
    manifest["refresh"] = counts
    axis = {**(parent_manifest.get("axis") or {}), **_cutoff_field(inputs.cutoff_at)}
    if axis:
      manifest["axis"] = axis
  else:
    manifest.update({
      "axis": {"historical": axis_now["historical"], "forecast": axis_now["projection"], **_cutoff_field(inputs.cutoff_at)},
      "axis_before": advanced["axis_before"],
      "axis_after": axis_now,
      "advance": counts,
    })
  findings = {
    "refusals": refusals.rows,
    "inputs": inputs.findings,
    "historical": refreshed["plan"]["findings"],
    "refresh_kept": refreshed["kept"],
    "helpers_rerun": helpers_rerun,
    **({} if advanced is None else {"seeds": seeds["findings"], "forecast": [finding for row in lowered for finding in row["findings"]]}),
  }
  if compared is not None:
    (snapshot / REFRESH_NAME).write_text(json.dumps(refresh, indent=1, default=str))
    write_snapshot(snapshot, doc, ours, compared, check, findings, manifest, vintage_changes={
      "cutoff": refresh["cutoff"],
      "restated_cells": refreshed["restated_cells"],
      "revintaged_cells": refreshed["revintaged_cells"],
    }, template=template, scenario_controls=scenario_controls)
  result.update({
    "refresh" if advanced is None else "advance": counts,
    "cutoff": refresh["cutoff"],
    "restated_cells": refreshed["restated_cells"],
    "revintaged_cells": refreshed["revintaged_cells"],
    "forecast_vs_actual": versus,
    "construction_rerun": bool(rerun.rows or lowered),
    "helpers_rerun": helpers_rerun,
    "parent_values_reproduced": refresh["parent_values_reproduced"],
    "historical": refreshed["plan"]["counts"],
    "input_findings": [{key: row[key] for key in ("stage", "code")} for row in inputs.findings],
    "ours": document.state_counts(ours),
    "key_resolution": {key: value for key, value in resolution.items() if key != "unkeyed_observed"},
    "check": check["counts"],
    "compare": None if compared is None else {
      "agreement": compared["agreement"],
      "disagreements": compared["compare"]["disagreements"],
      "by_kind": compared["compare"]["by_kind"],
    },
    "refusals": refusals.rows,
    "edits": len(doc.edits),
    "incomplete": incomplete,
    "verdict": verdict,
  })
  if advanced is not None:
    result.update({key: refresh[key] for key in ("boundary_moved", "axis_before", "axis_after")})
  return result


def package_in(workdir_in: Path) -> Path:
  """The document package a directory holds: ``<dir>/document.json`` (a ``doc_edit`` run root, a
  snapshot directory) or ``<dir>/snapshot/document.json`` (a ``target_*`` workdir)."""
  directory = Path(workdir_in).expanduser()
  for candidate in (directory / DOCUMENT_NAME, directory / SNAPSHOT_DIR / DOCUMENT_NAME):
    if candidate.is_file():
      return candidate
  raise FileNotFoundError(f"{directory} holds no {DOCUMENT_NAME} (nor {SNAPSHOT_DIR}/{DOCUMENT_NAME})")


def origin_snapshot(package: Path) -> Path | None:
  """The snapshot an edited document package descends from: the package's own directory when it
  holds a snapshot ``manifest.json`` (``doc_edit`` wrote in place); else, when the directory is a
  bench run root (``run.json``), the package that run read (``arguments.path``), followed the same
  way. ``None`` when the chain reaches neither."""
  seen: set[Path] = set()
  current = Path(package).expanduser().resolve()
  while current not in seen:
    seen.add(current)
    directory = current.parent
    if (directory / "manifest.json").is_file():
      return directory
    run = directory / "run.json"
    if not run.is_file():
      return None
    source = (json.loads(run.read_text()).get("arguments") or {}).get("path")
    if source is None:
      return None
    current = Path(str(source)).expanduser().resolve()
  return None


#: Manifest keys a snapshot of an edited package does not inherit from its origin.
_NOT_INHERITED = frozenset({
  "files", "values_sha256", "parent_snapshot", "refresh", "helpers_rerun", "construction_rerun", "verdict", "incomplete",
  "accepted_gaps", "lineage", "package", "advance", "axis_before", "axis_after",
})


def _recorded_construction(manifest: Mapping[str, Any]) -> list[dict[str, Any]] | None:
  """A snapshot manifest's construction helper rows: ``None`` when it records them as unknown
  (``construction: null``, written by ``target_snapshot`` for a package with no origin); a
  manifest with no ``construction`` key, or an empty list, recorded no helper."""
  if "construction" in manifest and manifest["construction"] is None:
    return None
  return [dict(row) for row in manifest.get("construction") or ()]


def _snapshot_template(snapshot: Path | None) -> FinancialModel:
  path = None if snapshot is None else snapshot / "template.json"
  if path is not None and path.is_file():
    return FinancialModel.model_validate_json(path.read_text())
  from schema.templates import load_sia_generic_template

  return load_sia_generic_template()


def _snapshot_scenario_controls(snapshot: Path | None) -> dict[str, Any] | None:
  path = None if snapshot is None else snapshot / "scenario_controls.json"
  return json.loads(path.read_text()) if path is not None and path.is_file() else None


def saved_document_validation(
  doc: document.ModelSheet, source: BuildSource, manifest: Mapping[str, Any], refusals: _Refusals,
  *, template: FinancialModel, scenario_controls: Mapping[str, Any] | None,
) -> ValidationReport | None:
  """The validation report of a saved document. It carries no SourceBundle or ModelSpec, so
  ``source`` loads the ones its snapshot ``manifest`` names (the judgment refs, the cutoff and the
  last actual it was built at) and ``document_validation`` checks the graph against them."""
  axis = manifest.get("axis") or {}
  last_actual = axis.get("last_actual")
  inputs = refusals.run("inputs", lambda: load_inputs(
    source, _parent_inputs_refs(manifest.get("inputs") or {}), refusals,
    cutoff_at=parse_cutoff(axis.get("cutoff_at")), last_actual=None if last_actual is None else int(last_actual),
  ))
  if inputs is None:
    return None
  unclaimed = refusals.run(
    "filed_line_unclaimed", lambda: unclaimed_filed_lines(
      inputs.hff, _document_structure(doc).historical_years, inputs.corpus,
      occurrence_envelopes=inputs.occurrence_envelopes, statement_bindings=inputs.statement_bindings,
    ),
  ) or []
  return document_validation(
    doc, inputs, source.ticker, unclaimed, refusals, template=template, scenario_controls=scenario_controls,
  )


def target_snapshot(
  package: Path, *, out: Path, store: StoreRef | None = None, source: BuildSource | None = None,
  soffice: str | None = None, accept_gaps: Collection[str] = (),
) -> dict[str, Any]:
  """Snapshot an edited document package (``doc_edit``'s output) as a publishable snapshot under
  ``<out>/snapshot/``: reopen, recalc, render + native compare, ``target_check``, and every
  ``target_build`` snapshot file. ``manifest.parent_snapshot`` names the package's origin snapshot
  (``origin_snapshot``) when known, whose manifest the new one inherits (inputs, corpus, axis,
  ``construction``); ``lineage: edit``. ``verdict`` as ``target_build``, over the origin's
  construction rows. ``source`` (the origin manifest's inputs through ``saved_document_validation``)
  is what lets the document be validated; without it the snapshot is ``incomplete:validation_not_run``."""
  package = Path(package).expanduser()
  origin = origin_snapshot(package)
  origin_manifest = {} if origin is None else json.loads((origin / "manifest.json").read_text())
  parent_ref = None if origin is None else {
    "path": str(origin),
    "manifest_sha256": _sha256(origin / "manifest.json"),
    "values_sha256": origin_manifest.get("values_sha256"),
    "built_at": origin_manifest.get("built_at"),
  }
  doc = document.reopen(package)
  template = _snapshot_template(origin)
  scenario_controls = _snapshot_scenario_controls(origin)
  out = Path(out).expanduser()
  snapshot = out / SNAPSHOT_DIR
  snapshot.mkdir(parents=True, exist_ok=True)
  refusals = _Refusals()
  ours = document.recalc(doc)
  compared = compare_into(snapshot, doc, refusals, soffice)
  validation = None if source is None else saved_document_validation(
    doc, source, origin_manifest, refusals, template=template, scenario_controls=scenario_controls,
  )
  check = target_check(doc, validation=validation)
  historical = tuple(int(year) for year in doc.model.time_structure.historical_periods)
  resolution = key_resolution(semantic_rows(doc, ours), historical)
  # No origin snapshot found: the package's construction helper statuses are unknown, not absent.
  construction_rows = None if origin is None else _recorded_construction(origin_manifest)
  gaps = any(row.get("status") in ("missing", "refused") for row in construction_rows or ())
  unresolved = next((row["findings"] for row in origin_manifest.get("incomplete") or () if row.get("reason") == "inputs_unresolved"), ())
  incomplete = incomplete_reasons(
    construction_rows, compared=compared, check=check, unresolved=unresolved if gaps else (), accept_gaps=accept_gaps,
  )
  verdict = _verdict(refusals, resolution, compared is not None, incomplete)
  # The axis is the document's own (an ``advance_period`` op moves it); a declared ``last_actual``
  # rides along only while the axis is the one it declared.
  own_axis = document.axis(doc.model)
  axis = {"historical": own_axis["historical"], "forecast": own_axis["projection"]}
  origin_axis = origin_manifest.get("axis") or {}
  if {key: origin_axis.get(key) for key in axis} == axis:
    axis = {**origin_axis, **axis}
  manifest = {
    **{key: value for key, value in origin_manifest.items() if key not in _NOT_INHERITED},
    "axis": axis,
    "built_at": datetime.now(UTC).isoformat(),
    "bench_sha": _bench_sha(),
    "ticker": store.ticker if store is not None else origin_manifest.get("ticker", doc.model.company.ticker),
    **({"store": store_label(store)} if store is not None else {}),
    "lineage": "edit",
    "package": str(package),
    "values_sha256": document.values_digest(ours),
    "parent_snapshot": parent_ref,
    "construction": construction_rows,
    "edits": len(doc.edits),
    "verdict": verdict,
    "incomplete": incomplete,
    "accepted_gaps": sorted(accept_gaps),
  }
  findings = {"refusals": refusals.rows, "edit": {"package": str(package), "origin_snapshot": None if origin is None else str(origin)}}
  if compared is not None:
    write_snapshot(
      snapshot, doc, ours, compared, check, findings, manifest, template=template, scenario_controls=scenario_controls,
    )
  return {
    "package": str(package),
    "workdir": str(out),
    "snapshot": str(snapshot),
    "parent_snapshot": parent_ref,
    "edits": len(doc.edits),
    "ours": document.state_counts(ours),
    "key_resolution": {key: value for key, value in resolution.items() if key != "unkeyed_observed"},
    "check": check["counts"],
    "compare": None if compared is None else {
      "agreement": compared["agreement"],
      "disagreements": compared["compare"]["disagreements"],
      "by_kind": compared["compare"]["by_kind"],
    },
    "refusals": refusals.rows,
    "incomplete": incomplete,
    "verdict": verdict,
  }


def _snapshot_cells(snapshot: Path) -> dict[tuple[str, int], dict[str, Any]]:
  """``sources.jsonl`` by ``(item_id, period)``: each cell's semantic column and its value."""
  out: dict[tuple[str, int], dict[str, Any]] = {}
  for line in (snapshot / "sources.jsonl").read_text().splitlines():
    if line.strip():
      row = json.loads(line)
      out[(str(row["item_id"]), int(row["period"]))] = row
  return out


def _explain_view(row: Mapping[str, Any] | None) -> dict[str, Any] | None:
  if row is None:
    return None
  return {"value": row.get("value"), "source": {key: value for key, value in row.items() if key not in ("item_id", "period", "value")}}


def _supersession(before: Mapping[str, Any] | None, after: Mapping[str, Any] | None) -> dict[str, Any] | None:
  """``forecast → actual``: the forecast a cell held in ``before`` that its actual superseded by
  ``after`` (``sources.jsonl`` rows), ``None`` when no supersession lies between them."""
  record = (after or {}).get(document.FORECAST_RECORD)
  if after is None or record is None or (before or {}).get(document.FORECAST_RECORD) is not None:
    return None
  value = after.get("value") or {}
  actual = value.get("value") if value.get("value") is not None else value.get("operand")
  source = after.get("source") or {}
  return {
    "forecast": record,
    "actual": {"value": actual, "cell": after.get("cell"), "origin": after.get("origin"), "kind": source.get("kind"), "vintage_accession": _vintage(after)},
    "text": f"forecast {record.get('value')} ({record.get('origin')}) → actual {actual}",
  }


def target_explain(workdir_a: Path, workdir_b: Path, item_id: str, period: int) -> dict[str, Any]:
  """One cell across two snapshots (``a`` before, ``b`` after) and the changed inputs it reads.

  Reads files only (each snapshot's ``sources.jsonl`` and saved document), so the two may come
  from different processes. The walk is period-scoped: ``document.upstream_cells`` of
  ``(item_id, period)`` in each snapshot's document (offsets, ``[last]`` and fixed-cell anchors
  resolved), so only cells that feed this period in ``a`` or ``b`` count. A fixed-cell line is
  read at each snapshot's own anchor (a period advance moves it; ``periods`` names both). Every
  non-formula cell among them whose semantic column or value differs between the two snapshots is
  listed with its source change, its ``vintage`` (the accession before and after) when the filing
  it is read from changed, its ``supersession`` (``forecast → actual``) when the year's actual
  superseded the forecast it held, and a path (item hops through the formulas' operands, each
  hop's value at ``period``) — a formula cell too when its historical formula superseded a
  forecast. ``vintage`` names the explained cell's source vintage in each snapshot and its
  ``source_history`` (``_vintage_view``); ``supersession`` the explained cell's own."""
  snapshot_a, snapshot_b = _snapshot_dir(Path(workdir_a)), _snapshot_dir(Path(workdir_b))
  period = int(period)
  cells_a, cells_b = _snapshot_cells(snapshot_a), _snapshot_cells(snapshot_b)
  doc_a, doc_b = document.reopen(snapshot_a / DOCUMENT_NAME), document.reopen(snapshot_b / DOCUMENT_NAME)
  anchors_a, anchors_b = document.scalar_anchors(doc_a.model), document.scalar_anchors(doc_b.model)

  def at(anchors: Mapping[str, int | None], item: str, year: int) -> tuple[str, int]:
    anchor = anchors.get(item)
    return (item, int(anchor)) if anchor is not None else (item, year)

  upstream: set[tuple[str, int]] = set()
  operands: dict[str, set[str]] = {}
  for doc, anchors in ((doc_b, anchors_b), (doc_a, anchors_a)):
    if doc.model._index.get(item_id) is not None:
      upstream |= {(str(item), int(year)) for item, year in document.upstream_cells(doc, *at(anchors, item_id, period))}
    for operand, readers in document.dependents_map(doc.model).items():
      for reader in readers:
        operands.setdefault(reader, set()).add(operand)
  upstream_items = {item for item, _ in upstream}
  reached: dict[str, str | None] = {item_id: None}
  queue = [item_id]
  while queue:
    current = queue.pop(0)
    for operand in sorted(operands.get(current, ())):
      if operand not in reached and operand in upstream_items:
        reached[operand] = current
        queue.append(operand)

  def path_to(node: str) -> list[str]:
    if node not in reached:
      return [item_id, node]
    hops = [node]
    while (parent := reached[hops[-1]]) is not None:
      hops.append(parent)
    return hops[::-1]

  def at_period(node: str) -> dict[str, Any]:
    left, right = cells_a.get(at(anchors_a, node, period)), cells_b.get(at(anchors_b, node, period))
    return {"item": node, "before": None if left is None else left.get("value"), "after": None if right is None else right.get("value")}

  changed: list[dict[str, Any]] = []
  seen: set[tuple[str, int]] = set()
  for key in sorted(upstream):
    # A fixed-cell input is one cell, read at each snapshot's anchor.
    fixed = key[0] in anchors_a or key[0] in anchors_b
    logical = (key[0], 0) if fixed else key
    if logical in seen:
      continue
    seen.add(logical)
    row_a, row_b = cells_a.get(at(anchors_a, *key)), cells_b.get(at(anchors_b, *key))
    before, after = _explain_view(row_a), _explain_view(row_b)
    if before == after:
      continue
    superseded = _supersession(row_a, row_b)
    kinds = {(view["source"] or {}).get("cell") for view in (before, after) if view is not None}
    if kinds == {"formula"} and superseded is None:
      continue
    hops = path_to(key[0])
    left_vintage, right_vintage = _vintage(row_a), _vintage(row_b)
    changed.append({
      "item": key[0], "period": at(anchors_b, *key)[1], "before": before, "after": after,
      "source_changed": (before or {}).get("source") != (after or {}).get("source"),
      **({"vintage": {"before": left_vintage, "after": right_vintage}} if left_vintage != right_vintage else {}),
      **({"supersession": superseded} if superseded is not None else {}),
      "path": [at_period(node) for node in hops], "depth": len(hops) - 1,
    })
  changed.sort(key=lambda row: (row["depth"], row["item"], row["period"]))
  key_a, key_b = at(anchors_a, item_id, period), at(anchors_b, item_id, period)
  target_a, target_b = _explain_view(cells_a.get(key_a)), _explain_view(cells_b.get(key_b))
  return {
    "item_id": item_id,
    "period": period,
    **({"periods": {"a": key_a[1], "b": key_b[1]}} if key_a != key_b else {}),
    "a": str(snapshot_a),
    "b": str(snapshot_b),
    "before": target_a,
    "after": target_b,
    "changed": target_a != target_b,
    "vintage": {"before": _vintage_view(cells_a.get(key_a)), "after": _vintage_view(cells_b.get(key_b))},
    "supersession": _supersession(cells_a.get(key_a), cells_b.get(key_b)),
    "upstream_items": len(upstream_items),
    "upstream_cells": len(upstream),
    "changed_inputs": changed,
  }


# --------------------------------------------------------------------------
# publish (bench-only version index)
# --------------------------------------------------------------------------


class PublishConflict(RuntimeError):
  """The version index moved since the caller read it (``expected_version`` is stale)."""


class PublishRefused(RuntimeError):
  """The snapshot's verdict is not ``published`` and ``allow_incomplete`` was not given."""

  def __init__(self, verdict: str | None, incomplete: Any) -> None:
    self.verdict = verdict
    self.incomplete = incomplete
    super().__init__(f"snapshot verdict is {verdict!r}, not 'published'; pass allow_incomplete: true to publish it anyway")


def target_publish(
  workdir: Path, store: StoreRef, *, expected_version: int | None = None, allow_incomplete: bool = False,
) -> dict[str, Any]:
  """Register ``workdir``'s snapshot as bench model version ``v<N>`` of the store's ticker.

  A snapshot whose manifest ``verdict`` is not ``published`` (``incomplete:…``, ``stopped:…``)
  is refused (``PublishRefused``) unless ``allow_incomplete``; the index entry records the
  verdict and the flag. ``<store.root>/bench-versions/<TICKER>/index.json`` lists every version
  (``version``, ``sha256`` of the manifest, ``inputs``, ``parent``, ``lineage``,
  ``published_at``) and ``current``. ``parent`` is the published version whose manifest is the
  snapshot's ``manifest.parent_snapshot`` (by ``manifest_sha256``), else ``null``; ``lineage`` is
  the manifest's (``fresh`` from ``target_build``, ``refresh``, ``edit`` from
  ``target_snapshot``). The index is read and rewritten under an exclusive ``flock`` on
  ``index.lock``, so two publishes cannot both claim ``v<N>``; ``expected_version`` (the
  ``current`` the caller read) makes the publish a compare-and-swap. The files are copied to a
  temporary sibling and renamed to ``v<N>`` before the index is rewritten: a ``v<N>`` the index
  does not list is a publish that died before indexing, and the next publish replaces it
  (``replaced_unindexed: true`` in the result). Dev-only: it touches no product table.
  """
  snapshot = _snapshot_dir(workdir)
  manifest_path = snapshot / "manifest.json"
  if not manifest_path.is_file():
    raise FileNotFoundError(f"{snapshot} holds no snapshot manifest.json")
  manifest = json.loads(manifest_path.read_text())
  verdict = manifest.get("verdict")
  if verdict != "published" and not allow_incomplete:
    raise PublishRefused(verdict, manifest.get("incomplete"))
  parent_sha = (manifest.get("parent_snapshot") or {}).get("manifest_sha256")
  root = Path(store.root) / "bench-versions" / store.ticker
  root.mkdir(parents=True, exist_ok=True)
  index_path = root / "index.json"
  with open(root / "index.lock", "a+") as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    try:
      index = json.loads(index_path.read_text()) if index_path.is_file() else {"ticker": store.ticker, "versions": [], "current": None}
      current = index["current"]
      if expected_version is not None and current != expected_version:
        raise PublishConflict(f"index current is v{current}, not the expected v{expected_version}")
      parent = next((row["version"] for row in index["versions"] if parent_sha is not None and row["sha256"] == parent_sha), None)
      version = len(index["versions"]) + 1
      target = root / f"v{version}"
      staging = root / f".v{version}.tmp"
      if staging.exists():
        shutil.rmtree(staging)
      staging.mkdir()
      for path in snapshot.iterdir():
        if path.is_file():
          shutil.copy2(path, staging / path.name)
      replaced_unindexed = target.exists()
      if replaced_unindexed:
        shutil.rmtree(target)
      os.replace(staging, target)
      entry = {
        "version": version,
        "sha256": _sha256(target / "manifest.json"),
        "inputs": manifest.get("inputs"),
        "values_sha256": manifest.get("values_sha256"),
        "bench_sha": manifest.get("bench_sha"),
        "parent": parent,
        "lineage": manifest.get("lineage"),
        "verdict": verdict,
        "allow_incomplete": allow_incomplete,
        "source_workdir": str(Path(workdir).expanduser()),
        "published_at": datetime.now(UTC).isoformat(),
      }
      index["versions"].append(entry)
      index["current"] = version
      scratch = index_path.with_suffix(".json.tmp")
      scratch.write_text(json.dumps(index, indent=1, default=str))
      os.replace(scratch, index_path)
    finally:
      fcntl.flock(lock, fcntl.LOCK_UN)
  return {
    "version": version, "path": str(target), "index": str(index_path), "entry": entry, "replaced_unindexed": replaced_unindexed,
  }
