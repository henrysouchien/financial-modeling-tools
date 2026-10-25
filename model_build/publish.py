"""A model version as publishable bytes: render a document, compile the version's four artifacts.

Owner: ``docs/design/build-model-package-boundary-plan-2026-10-05.md`` §2.1–§2.2. This root renders
and compiles; the repository keeps the write, the lineage, the lock and the pointer CAS. The product
adapter (``commit_build_render_transaction``) checks the file row, the model id and the bundle path,
calls ``compile_version`` once, then compares a durable replay, writes the immutable files and
commits.

``render_document_version`` renders a ``document.ModelSheet`` the way ``document.render`` does (every
non-known cell of the document's recalc written ``=NA()``, a cited evidenced absence included)
through the compile render pipeline of
``schema.build.finalize_compiled_model`` up to its validation: annual render context, valuation
evidence block, render contract, lineage index, render state and annual presentation guard. Its
``CompileStageEvidence`` is computed from the document's model and recalc with the schema functions
compile uses. What the document path cannot derive from the document, its ModelSpec and its
SourceBundle is named where it is set.

``compile_version`` is the compile half of a publication: render state and annual presentation
binding, SourceBundle / ModelSpec decode, the rendered source snapshot, schedule input audit,
offline validation, the semantic sidecar, the four artifacts' bytes and locations, the request
digest and the ``ModelVersion``.
"""

from __future__ import annotations

from collections.abc import Collection, Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
import hashlib
from pathlib import Path
import subprocess
import tempfile
from typing import Any

from model_build import document
from model_build.document import STAMPED_CELL_KINDS
from schema import serialization
from schema.annual_presentation_guard import (
  evaluate_annual_presentation,
  read_annual_presentation_cells,
  require_bound_ready_annual_presentation,
)
from schema.build_findings import CompileStageEvidence
from schema.build_model_product_contracts import (
  ArtifactLocation,
  HandoffBuildModelVersionOrigin,
  ModelVersion,
  ValidationReport,
  ProvenanceMap,
  decode_source_bundle_payload,
)
from schema.build_offline_validation import (
  NativeRecalculation,
  _semantic_cells,
  owned_cell_gaps,
  validate_offline_build_artifacts,
)
from schema.dependency_graph import DependencyGraph
from schema.finding_vocabulary import Finding
from schema.handoff import ModelRenderState
from schema.model_ref import ModelStamp, ModelVersionRef
from schema.models import FinancialModel
from schema.model_semantic_document import ModelSemanticDocument, semantic_value_snapshots
from schema.model_spec_decoder import VerifiedModelSpecDecodeRequest, decode_model_spec_for_writer
from schema.render_state import (
  compiler_input_content_hash,
  construct_model_render_state,
  render_state_hash,
  render_state_payload,
)
from schema.renderer_columns import item_owns_cell
from schema.schedule_input_audit import build_schedule_input_audit
from schema.valuation_evidence_derivations import derive_valuation_evidence_status
from schema.valuation_evidence_validation import validate_source_bundle_binding_and_selection
from schema.versioned_artifact import canonical_json_bytes

#: The version's artifacts, in ``CompiledVersion.files`` order.
WORKBOOK_NAME = "model.xlsx"
SIDECAR_NAME = "semantic.json"
PROVENANCE_NAME = "provenance.json"
VALIDATION_NAME = "validation.json"
#: The working-capital row the schedule input audit requires when the model carries it.
_WORKING_CAPITAL_REQUIRED = ("tpl.fm.balance_sheet.current_asset_3",)


@dataclass(frozen=True)
class PublishedFile:
  """One immutable artifact of a version; ``location`` is relative to the repository root."""

  name: str
  location: str
  sha256: str
  content: bytes


@dataclass(frozen=True)
class StagedVersion:
  """A rendered, not yet compiled-for-publication version.

  ``target_model_ref`` carries ``model_id``, ``version``, ``render_state``,
  ``annual_presentation`` and ``provenance``: the render state and annual presentation
  bound to ``workbook_content``, and the source snapshot used by its Sources sheet.
  """

  model: Any
  render_plan: Any
  workbook_content: bytes
  target_model_ref: dict[str, Any]
  compile_stage: Any


@dataclass(frozen=True)
class CompiledVersion:
  """The ``ModelVersion`` and its files: workbook, semantic sidecar, provenance, validation."""

  model_version: Any
  files: tuple[PublishedFile, ...]


def version_location(model_ref: ModelVersionRef, name: str) -> str:
  """An artifact's location relative to the repository root."""
  return f"model-versions/{model_ref.model_id}/v{model_ref.version}/{name}"


def _published(model_ref: ModelVersionRef, name: str, content: bytes) -> PublishedFile:
  return PublishedFile(
    name=name, location=version_location(model_ref, name), sha256=hashlib.sha256(content).hexdigest(), content=content,
  )


@dataclass(frozen=True)
class _PublicationInputs:
  """What a staged version's ``target_model_ref`` carries, decoded and checked once."""

  annual_evidence: Any
  render_state: ModelRenderState
  source_bundle: Any
  model_spec: Any
  source_bundle_digest: str
  model_spec_digest: str
  provenance: ProvenanceMap


def _publication_inputs(target_model_ref: Mapping[str, Any], *, workbook_content: bytes) -> _PublicationInputs:
  """Raises ``AnnualPresentationBindingError`` when the annual presentation evidence is not bound
  to ``workbook_content`` and the render state, ``ValueError`` as ``compile_version`` documents."""
  annual_evidence = require_bound_ready_annual_presentation(target_model_ref, workbook_content=workbook_content)
  target_state = render_state_payload(target_model_ref.get("render_state"))
  if target_state is None:
    raise ValueError("build publication requires a complete render state")
  render_state = ModelRenderState.model_validate(target_state)
  compiler_inputs = render_state.compiler_inputs
  source_input = compiler_inputs.source_bundle
  model_spec_input = compiler_inputs.model_spec
  source_bundle = decode_source_bundle_payload(source_input.payload)
  verified_model_spec = decode_model_spec_for_writer(
    VerifiedModelSpecDecodeRequest(
      payload_mapping=model_spec_input.payload,
      compiler_input_hash=model_spec_input.content_hash,
      external_model_version_digest=None,
    )
  )
  if verified_model_spec.verification_mode != "writer":
    raise ValueError("build publication requires writer verification mode")
  model_spec = verified_model_spec.model_spec
  validate_source_bundle_binding_and_selection(
    source_bundle,
    model_spec,
    persisted_source_bundle_payload=source_input.payload,
    persisted_source_bundle_wire_digest=source_input.content_hash,
  )
  source_bundle_digest = source_input.content_hash.removeprefix("sha256:")
  model_spec_digest = model_spec_input.content_hash.removeprefix("sha256:")
  provenance_payload = target_model_ref.get("provenance")
  if provenance_payload is None:
    raise ValueError("build publication requires the rendered provenance snapshot")
  provenance = ProvenanceMap.model_validate(provenance_payload)
  if (
    provenance.source_bundle_digest != source_bundle_digest
    or provenance.model_spec_digest != model_spec_digest
  ):
    raise ValueError("build publication provenance snapshot digest mismatch")
  return _PublicationInputs(
    annual_evidence=annual_evidence,
    render_state=render_state,
    source_bundle=source_bundle,
    model_spec=model_spec,
    source_bundle_digest=source_bundle_digest,
    model_spec_digest=model_spec_digest,
    provenance=provenance,
  )


def native_recalculation(workbook_content: bytes) -> NativeRecalculation:
  """LibreOffice's recalculation of ``workbook_content``: the publish-time verifier of the rendered
  formulas (ENGINE-THREE-TIER). An engine that cannot run or fails comes back as a failed
  recalculation, which validation reports as the blocking ``recalculation_engine_failed`` finding;
  nothing then claims the formulas were evaluated."""
  version = "unknown"
  try:
    executable = document.soffice_path()
    version = subprocess.run(
      [executable, "--version"], capture_output=True, text=True, timeout=60, check=False,
    ).stdout.strip() or version
    with tempfile.TemporaryDirectory(prefix="publish-native-") as scratch:
      path = Path(scratch) / WORKBOOK_NAME
      path.write_bytes(workbook_content)
      values = document.native_recalc(path, soffice=executable)
  except Exception as exc:
    return NativeRecalculation("LibreOffice", version, {}, f"{type(exc).__name__}: {exc}")
  return NativeRecalculation(
    "LibreOffice", version, {(sheet.upper(), cell.upper()): value for (sheet, cell), value in values.items()},
  )


def _validate_staged(
  publication: _PublicationInputs,
  *,
  staged_model: Any,
  render_plan: Any,
  workbook_content: bytes,
  target_model_ref: Mapping[str, Any],
  compile_stage: CompileStageEvidence,
  validated_at: datetime,
  native: NativeRecalculation | None,
) -> ValidationReport:
  """The one evaluation of a staged version's findings and verdict
  (``validate_offline_build_artifacts``): ``compile_version`` persists its report and every
  non-persisting caller reads the same one. ``native`` is the external recalculation of
  ``workbook_content``; ``None`` leaves the report's recalculation evidence not evaluated."""
  from schema.build_offline_validation import provenance_diagnostics_for_map

  provenance_diagnostics = provenance_diagnostics_for_map(
    publication.provenance,
    render_plan=render_plan,
    model=staged_model,
  )
  return validate_offline_build_artifacts(
    model=staged_model,
    render_plan=render_plan,
    workbook_path=workbook_content,
    source_bundle=publication.source_bundle,
    model_spec=publication.model_spec,
    provenance=publication.provenance,
    annual_presentation=publication.annual_evidence,
    validated_at=validated_at,
    compile_stage=compile_stage,
    provenance_diagnostics=tuple(provenance_diagnostics),
    bound_scenario_control_ids=tuple(
      str(item) for item in (target_model_ref.get("bound_scenario_control_ids") or ())
    ),
    unclaimed_filed_lines=tuple(target_model_ref.get("unclaimed_filed_lines") or ()),
    document_decline_ops=tuple(target_model_ref.get("document_declinations") or ()),
    template=(
      None
      if target_model_ref.get("validation_template") is None
      else FinancialModel.model_validate(target_model_ref["validation_template"])
    ),
    native=native,
    values=compile_stage.graph_values,
  ).report


def validate_staged_version(staged: StagedVersion, *, validated_at: datetime) -> ValidationReport:
  """The validation report ``compile_version`` would persist for ``staged``, persisting nothing, minus
  the external recalculation: publication alone runs LibreOffice (RENDERED-RECHECK-AT-PUBLISH), so
  this report's recalculation evidence is not evaluated. Raises as ``compile_version`` does for a
  staged ref it cannot decode."""
  return _validate_staged(
    _publication_inputs(staged.target_model_ref, workbook_content=staged.workbook_content),
    staged_model=staged.model,
    render_plan=staged.render_plan,
    workbook_content=staged.workbook_content,
    target_model_ref=staged.target_model_ref,
    compile_stage=staged.compile_stage,
    validated_at=validated_at,
    native=None,
  )


def compile_version(
  *,
  user_id: str,
  research_file_id: int,
  ticker: str,
  handoff_id: int,
  handoff_version: int | None = None,
  model_ref: Any,
  predecessor: Any,
  staged_model: Any,
  render_plan: Any,
  workbook_content: bytes,
  target_model_ref: Mapping[str, Any],
  compile_stage: CompileStageEvidence,
  committed_at: datetime,
  native: NativeRecalculation | None = None,
) -> CompiledVersion:
  """The compile half of a publication: one staged version's four artifacts and its
  ``ModelVersion`` (status = the validation verdict, origin the handoff, ``predecessor`` as the
  repository read it). The carried provenance snapshot used by the workbook's Sources sheet
  supplies the stored provenance, semantic sidecar, schedule input audit and validation. Without
  ``native`` the validation reports the rendered formulas as not evaluated (the publish door passes
  ``native_recalculation(workbook_content)``: LibreOffice's recalculation at publish).
  Raises ``AnnualPresentationBindingError`` when the annual presentation evidence is not bound
  to ``workbook_content`` and the render state, ``ValueError`` when the render state is incomplete,
  the rendered provenance snapshot is missing or its digests do not match the render state,
  the ModelSpec does not decode in writer mode or the SourceBundle digest does not match its payload."""
  canonical_ticker = str(ticker).strip().upper()
  model_ref = ModelVersionRef.model_validate(model_ref)
  publication = _publication_inputs(target_model_ref, workbook_content=workbook_content)
  annual_evidence = publication.annual_evidence
  render_state = publication.render_state
  source_bundle = publication.source_bundle
  model_spec = publication.model_spec
  provenance = publication.provenance
  source_bundle_digest = publication.source_bundle_digest
  model_spec_digest = publication.model_spec_digest
  audit_graph = DependencyGraph()
  audit_graph.build(staged_model)
  schedule_input_audit = build_schedule_input_audit(
    model=staged_model,
    typed_values=audit_graph.compute_typed({}),
    projection_periods=tuple(int(value) for value in staged_model.time_structure.projection_periods),
    working_capital_required=tuple(item_id for item_id in _WORKING_CAPITAL_REQUIRED if item_id in staged_model._index),
    provenance=provenance,
    semantic_cells=tuple(render_plan.semantic_cells),
  )
  validation = _validate_staged(
    publication,
    staged_model=staged_model,
    render_plan=render_plan,
    workbook_content=workbook_content,
    target_model_ref=target_model_ref,
    compile_stage=compile_stage,
    validated_at=committed_at,
    native=native,
  )
  provenance_file = _published(model_ref, PROVENANCE_NAME, canonical_json_bytes(provenance.model_dump(mode="json")))
  validation_file = _published(model_ref, VALIDATION_NAME, canonical_json_bytes(validation.model_dump(mode="json")))
  sidecar_file = _published(model_ref, SIDECAR_NAME, canonical_json_bytes(
    ModelSemanticDocument(
      model_id=model_ref.model_id,
      model_version=model_ref.version,
      ticker=canonical_ticker,
      render_state=render_state,
      model=staged_model,
      provenance_map=ArtifactLocation(location=provenance_file.location, sha256=provenance_file.sha256),
      provenance=provenance,
      semantic_values=semantic_value_snapshots(render_plan),
      validation_status=validation.status,
      validation_blocker_count=validation.error_count,
      validation_warning_count=validation.uncleared_warning_count,
      valuation_evidence_status=derive_valuation_evidence_status(
        source_bundle, None if model_spec.valuation_policy is None else model_spec.valuation_policy.peer_policy,
        bundle_content_sha256=source_bundle_digest,
      ),
      schedule_input_audit=schedule_input_audit,
      scenario_controls=target_model_ref.get("scenario_controls"),
    ).model_dump(mode="json")
  ))
  files = (_published(model_ref, WORKBOOK_NAME, workbook_content), sidecar_file, provenance_file, validation_file)
  artifacts = tuple(ArtifactLocation(location=file.location, sha256=file.sha256) for file in files)
  request_digest = hashlib.sha256(
    canonical_json_bytes(
      {
        "user_id": user_id,
        "research_file_id": int(research_file_id),
        "ticker": canonical_ticker,
        "handoff_id": int(handoff_id),
        "model_ref": model_ref.model_dump(mode="json"),
        "source_bundle_digest": source_bundle_digest,
        "model_spec_digest": model_spec_digest,
        "artifacts": [artifact.model_dump(mode="json") for artifact in artifacts],
        "annual_presentation": annual_evidence.model_dump(mode="json"),
      }
    )
  ).hexdigest()
  metadata = getattr(staged_model, "metadata", None)
  model_version = ModelVersion(
    model_id=model_ref.model_id,
    version=model_ref.version,
    user_id=user_id,
    research_file_id=int(research_file_id),
    ticker=canonical_ticker,
    origin=HandoffBuildModelVersionOrigin(
      handoff_id=int(handoff_id),
      handoff_version=int(handoff_version) if handoff_version is not None else model_ref.version,
    ),
    predecessor=predecessor,
    model_spec_predecessor=model_spec.predecessor,
    model_spec_idempotency_key=model_spec.idempotency_key,
    request_digest=request_digest,
    source_bundle_digest=source_bundle_digest,
    model_spec_digest=model_spec_digest,
    compiler_version=serialization.COMPUTE_ENGINE_VERSION,
    template_version=str(
      getattr(metadata, "template_version", None) or getattr(metadata, "source_model", None) or "unversioned"
    ),
    workbook=artifacts[0],
    semantic_sidecar=artifacts[1],
    provenance_map=artifacts[2],
    validation_report=artifacts[3],
    status=validation.status,
    created_by=user_id,
    created_at=committed_at,
    committed_at=committed_at,
  )
  return CompiledVersion(model_version=model_version, files=files)


# --------------------------------------------------------------------------
# a document as a staged version
# --------------------------------------------------------------------------


def _canonical_control_curves(
  model_spec: Any, source_bundle: Any, control_curves: tuple[Any, ...] | None,
) -> tuple[Any, ...]:
  """Use the persisted construction curves when supplied; legacy direct callers may
  derive declared workbook targets from their ModelSpec and SourceBundle."""
  if control_curves is not None:
    return tuple(control_curves)
  from schema.model_kind_dispatch import declared_capabilities, declared_scenario_controls
  from schema.scenario_bindings import lower_model_spec_scenario_curves

  scenario_controls = declared_scenario_controls(declared_capabilities(source_bundle))
  if scenario_controls is not None and scenario_controls.workbook_targets:
    return lower_model_spec_scenario_curves(
      model_spec, workbook_targets=scenario_controls.workbook_target_map(), scenario_controls=scenario_controls,
    )
  if not any(review.overrides for review in model_spec.scenario_reviews):
    return ()
  raise ValueError(
    "render_document_version: the ModelSpec's scenario-review selectors resolve through the compiled "
    "driver registry; pass the persisted construction baseline's control_curves"
  )





def _document_seed_rows(
  doc: document.ModelSheet, model: Any, source_bundle: Any = None,
) -> tuple[dict[str, Any], ...]:
  """Seed-manifest rows for sources the document recorded and compile's lowering did not.

  Judgment rows map to ``model_spec_assumption`` (and ``document_judgment`` by assumption id).
  Filing lines and closure derivations carry their observation id. Construction helpers are a
  direct source. Cited absences are evidenced absences, except a bridge interest amount
  construction restored from the SourceBundle.
  """
  restored = {
    (item_id, int(period))
    for item_id, item in model._index.items()
    for period, spec in (item.overrides or {}).items()
    if getattr(spec, "note", None) == "issuer_disclosed_adjusted_ebitda_bridge_interest"
  }
  projection_years = {int(period) for period in model.time_structure.projection_periods}
  disposition_absences, _disposition_zeros = document.cell_dispositions(doc)
  rows: list[dict[str, Any]] = []
  assumptions: dict[tuple[str, str], list[int]] = {}
  from model_build import target_build

  role_by_acc_tag = target_build._statement_role_lookups(doc, source_bundle)
  for (item_id, period), column in doc.semantic.items():
    year = int(period)
    if (item_id, year) in restored:
      continue
    source = column.get("source") or {}
    # A formula over the pin's opening balance reads that evidence as an input (never as its own line).
    for opening in column.get("opening_inputs") or ():
      rows.append({
        "source_kind": "source_bundle_opening_balance",
        "resolved_item_id": item_id,
        "requirement_id": str(opening["requirement_id"]),
        "evidence_id": str(opening["evidence_id"]),
        "source_id": str(opening["source_id"]),
        "periods": [year],
      })
    kind = source.get("kind")
    if kind == "judgment_row":
      judgment = source.get("row") or {}
      driver = str(judgment.get("driver") or judgment.get("driver_key") or "")
      assumption_id = str(judgment.get("assumption_id") or "")
      # A derived row's fact_ids are the inputs of a derivation (the consolidated fact a
      # single-segment identity rests on), carried on the cell's seed ref; they are not the
      # cell's own filing lines.
      for fact_id in () if judgment.get("disposition") == "derived" else judgment.get("fact_ids") or ():
        rows.append({
          "source_kind": "document_filing_line",
          "resolved_item_id": item_id,
          "observation_id": str(fact_id),
          "periods": [year],
        })
      if driver:
        assumptions.setdefault((item_id, driver), []).append(year)
      if assumption_id:
        rows.append({
          "source_kind": "document_judgment",
          "resolved_item_id": item_id,
          "assumption_id": assumption_id,
          "periods": [year],
        })
    elif kind == "filing_line":
      via_kind = (source.get("via") or {}).get("source_kind")
      if via_kind == "source_bundle_schedule_disclosure":
        disclosures = source.get("schedule_disclosures") or (
          {"fact_id": source.get("fact_id") or source.get("observation_id"), "source_id": source.get("source_id")},
        )
        for disclosure in disclosures:
          if not isinstance(disclosure, Mapping):
            continue
          fact_id = disclosure.get("fact_id")
          source_id = disclosure.get("source_id")
          if fact_id and source_id:
            rows.append({
              "source_kind": "source_bundle_schedule_disclosure",
              "resolved_item_id": item_id,
              "fact_id": str(fact_id),
              "source_id": str(source_id),
              "periods": [year],
            })
      elif via_kind == "source_bundle_opening_balance":
        rows.append({
          "source_kind": "source_bundle_opening_balance",
          "resolved_item_id": item_id,
          "requirement_id": str(source.get("observation_id") or ""),
          "evidence_id": str(source.get("evidence_id") or ""),
          "source_id": str(source.get("source_id") or ""),
          "periods": [year],
        })
      elif source.get("observation_id"):
        rows.append({
          "source_kind": "document_filing_line",
          "resolved_item_id": item_id,
          "observation_id": str(source["observation_id"]),
          "periods": [year],
        })
      else:
        from model_build import projection, target_build
        line = target_build._filing_line_ref(
          source,
          statement_role_by_accession_tag=role_by_acc_tag,
        )
        if line is not None:
          rows.append({
            "source_kind": "document_filing_line",
            "resolved_item_id": item_id,
            "observation_id": projection.filing_line_observation_id(line),
            "periods": [year],
          })
    elif kind == "derivation":
      line = source.get("line") or {}
      observation_id = line.get("observation_id") if isinstance(line, dict) else None
      if observation_id:
        rows.append({
          "source_kind": "document_filing_line",
          "resolved_item_id": item_id,
          "observation_id": str(observation_id),
          "periods": [year],
        })
    elif kind == "construction_helper" or (
      kind == "agent"
      and column.get("cell") == "input"
      and str(item_id).startswith("doc.")
      and year in projection_years
      and bool(source.get("text") or column.get("rationale"))
    ):
      rows.append({
        "source_kind": "document_construction",
        "resolved_item_id": item_id,
        "source_id": str(source.get("helper") or source.get("text") or "agent_authored"),
        "periods": [year],
      })
    else:
      absence_declared = (
        (item_id, year) in disposition_absences
        or source.get("derivation") in ("evidenced_absence", "closure_corroborated_zero")
      )
      absence_cited = absence_declared and bool(column.get("absence_citation") or source.get("citation"))
      blank_corroborated = (
        absence_declared
        and kind in ("absence_citation", "derivation")
        and (source.get("closure") or {}).get("verdict") == "corroborated"
      )
      if column.get("cell") == "not_separately_presented" or source.get("included_in") or absence_cited or blank_corroborated:
        rows.append({
          "source_kind": "document_absence",
          "resolved_item_id": item_id,
          "periods": [year],
        })
  for (item_id, driver), periods in assumptions.items():
    rows.append({
      "source_kind": "model_spec_assumption",
      "resolved_item_id": item_id,
      "canonical_driver_key": driver,
      "periods": sorted(periods),
    })
  return tuple(rows)


def document_owns_cell(
  doc: document.ModelSheet, model: Any, item_id: str, period: int, gaps: Collection[tuple[str, int]] = (),
) -> bool:
  """Whether the document's row owns ``(item_id, period)`` under the renderer's rule.

  ``schema.renderer_columns.item_owns_cell`` owns the rule: ``formula_periods``, an override, a
  stored value, or a coordinate in ``gaps`` (``build_offline_validation.owned_cell_gaps``: a typed
  lag-input gap or a cell the pinned profile cites ``not_applicable``). A retired row
  (``Construction.retired`` or ``DependencyGraph.retire``) owns nothing. No semantic stamp owns a
  cell: a stamp describes a cell that exists, so a persisted missing stamp never resurrects a
  retired row or a coordinate outside the row's owned periods. ``_rendered_gap`` and the check's
  cell enumeration both read this rule, so a check and a build judge the same cells. It is the
  rule of ownership, not the render's write list: see ``item_owns_cell``."""
  item = model._index.get(item_id)
  if item is None:
    return False
  return item_owns_cell(model, item, int(period), gaps=gaps)


def document_semantic_cells(
  doc: document.ModelSheet, model: Any, gaps: Collection[tuple[str, int]],
) -> list[Any]:
  """The document's semantic cells without a render: every coordinate ``document_owns_cell``
  keeps. ``gaps`` is the source bundle's ``owned_cell_gaps``."""
  return _semantic_cells(model, None, owned=lambda item, period: document_owns_cell(doc, model, item.id, period, gaps))


def _rendered_gap(
  doc: document.ModelSheet, model: Any, key: tuple[str, int], gaps: Collection[tuple[str, int]] = (),
) -> bool:
  """A non-known recalc cell the line renders, or the document recorded."""

  from schema.models import FormulaType

  item_id, period = key
  if not document_owns_cell(doc, model, item_id, period, gaps):
    return False
  item = model._index[item_id]
  year = int(period)
  if (doc.semantic.get((item_id, year)) or {}).get("cell") in STAMPED_CELL_KINDS:
    return True
  spec = (item.overrides or {}).get(year)
  if spec is None:
    historical = {int(owned) for owned in model.time_structure.historical_periods}
    spec = item.historical if year in historical else item.projected
  if spec is None:
    return False
  if spec.type is FormulaType.constant and isinstance(spec.params.get("value"), str):
    return False
  if spec.type is FormulaType.raw:
    formula = str(spec.params.get("formula") or spec.params.get("semantic_formula") or "")
    return "NA()" in formula.upper()
  return True


@dataclass(frozen=True)
class _DocumentEvaluation:
  model: FinancialModel
  values: document.Values
  typed_values: dict[str, dict[int, Any]]
  evaluation_reads: Mapping[tuple[str, int], tuple[tuple[str, int], ...]] | None
  case_evaluation_reads: Mapping[str, Mapping[tuple[str, int], tuple[tuple[str, int], ...]]]
  compile_stage: CompileStageEvidence


def _document_evaluation(
  doc: document.ModelSheet, *, source_bundle: Any, model_spec: Any,
  control_curves: tuple[Any, ...] | None, finalized_at: datetime,
  values: document.Values | None = None,
  evaluation_reads: Mapping[tuple[str, int], tuple[tuple[str, int], ...]] | None = None,
  input_findings: Collection[Mapping[str, Any]] = (),
) -> _DocumentEvaluation:
  """The document-level compile evidence shared by graph checks and publication."""
  from schema.build_findings import cited_fact_value_mismatch_findings, historical_foundation_evidence_findings
  from schema.build_scenarios import compute_scenario_outputs, scenario_results_payload
  from schema.model_readiness import compute_model_projection_readiness
  from schema.model_readiness_quality import compute_model_quality_readiness
  from schema.model_readiness_scenario_bridge import compute_model_scenario_bridge_readiness
  from schema.model_readiness_valuation import compute_valuation_input_readiness
  from schema.renderer_annual_context import _annual_render_context
  from schema.scenario_bindings import scenario_case_gaps, scenario_gap_findings

  if finalized_at.tzinfo is None or finalized_at.utcoffset() is None:
    raise ValueError("finalized_at must be timezone-aware")
  model = doc.model.model_copy(deep=True)
  model.build_index()
  model.metadata.created_at = finalized_at.astimezone(timezone.utc).isoformat()
  if values is None:
    reads: dict[tuple[str, int], tuple[tuple[str, int], ...]] = {}
    values = document.recalc(document.ModelSheet(
      model=model, model_spec=doc.model_spec, semantic_values=doc.semantic_values, source=doc.source, semantic=doc.semantic,
    ), evaluation_reads=reads)
    evaluation_reads = reads
  typed_values: dict[str, dict[int, Any]] = {}
  computed_values: dict[str, dict[int, float]] = {}
  for (item_id, period), value in values.items():
    typed_values.setdefault(item_id, {})[period] = value
    if value.state == "known":
      computed_values.setdefault(item_id, {})[period] = float(value.value)
  unresolved_cases = frozenset(gap.scenario for gap in scenario_case_gaps(model_spec))
  scenario_outputs, _ = compute_scenario_outputs(
    model.model_copy(deep=True), unresolved_cases=unresolved_cases,
  )
  scenario_results = scenario_results_payload(scenario_outputs, unresolved_cases)
  projection_readiness = compute_model_projection_readiness(
    model,
    canonical_control_curves=tuple(
      curve for curve in _canonical_control_curves(model_spec, source_bundle, control_curves) if curve.domain == "forecast"
    ),
    computed_values=computed_values,
    guard_config=None,
  )
  scenario_bridge_readiness = compute_model_scenario_bridge_readiness(model)
  valuation_input_readiness = compute_valuation_input_readiness(model, computed_values=computed_values)
  model_quality_readiness = compute_model_quality_readiness(
    model, computed_values=computed_values, valuation_input_readiness=valuation_input_readiness, guard_config=None,
  )
  cell_gaps = owned_cell_gaps(model, source_bundle)
  annual_context = _annual_render_context(
    model=model, source_bundle=source_bundle, computed_values=computed_values,
    projection_readiness=projection_readiness, model_quality_readiness=model_quality_readiness,
    scenario_bridge_readiness=scenario_bridge_readiness, scenario_results=scenario_results,
    version_identity=None, predecessor_semantic_document=None,
  )
  return _DocumentEvaluation(
    model=model, values=values, typed_values=typed_values,
    evaluation_reads=evaluation_reads,
    case_evaluation_reads=scenario_outputs.case_eval_reads,
    compile_stage=CompileStageEvidence(
      projection_readiness=projection_readiness, model_quality_readiness=model_quality_readiness,
      scenario_bridge_readiness=scenario_bridge_readiness, scenario_results=scenario_results,
      valuation_input_readiness=valuation_input_readiness, schedule_input_audit=None,
      diagnostic_report=None, required_unavailable=annual_context.required_unavailable,
      graph_values=typed_values,
      render_unavailable=annual_context.required_unavailable | frozenset(
        key for key, value in values.items()
        if value.state != "known" and _rendered_gap(doc, model, key, cell_gaps)
      ),
      judgment_findings=scenario_gap_findings(model_spec),
      compile_findings=(
        *historical_foundation_evidence_findings(source_bundle, model_spec),
        *cited_fact_value_mismatch_findings(doc, values, source_bundle),
        *(
          Finding.model_validate({key: value for key, value in row.items() if key not in ("stage", "owner_file")})
          for row in input_findings if "severity" in row
        ),
      ),
    ),
  )


def render_document_version(
  doc: document.ModelSheet,
  *,
  ticker: str,
  source_bundle: Any,
  source_bundle_payload: Mapping[str, Any],
  source_bundle_content_sha256: str,
  model_spec: Any,
  bundle_id: str | None,
  bundle_identity_digest: str | None,
  model_ref: Any,
  finalized_at: datetime,
  control_curves: tuple[Any, ...] | None = None,
  bound_scenario_control_ids: tuple[str, ...] = (),
  unclaimed_filed_lines: tuple[Mapping[str, Any], ...] = (),
  template: FinancialModel | None = None,
  input_findings: Collection[Mapping[str, Any]] = (),
) -> StagedVersion:
  """``doc`` rendered as version ``model_ref``: the bytes, render plan, bound render state and
  annual presentation, source snapshot, and compile-stage evidence for ``compile_version``.
  The source snapshot used by the workbook's Sources sheet is carried in ``target_model_ref``
  for publication without reclassifying the authored document's sources.

  The staged model is the document's own (``metadata.created_at`` stamped); the render lowers a
  copy of it as ``document.render`` does (semantic raw formulas resolved to cells, every
  non-known recalc cell ``=NA()``, a cited evidenced absence included), so the published
  semantic document reopens as the authored document.

  Supplied from the document, its ModelSpec and its SourceBundle: ``seed_coverage_manifest``
  (the document's semantic-column sources), ``bound_scenario_control_ids`` (the build's
  scenario partition) and ``unclaimed_filed_lines`` (the build's
  ``schema.filed_line_unclaimed.unclaimed_line_record`` rows, which ``compile_version`` validates
  as blocking findings). Scenario gaps are carried as ``judgment_findings`` from the ModelSpec.
  Construction's typed ``input_findings`` travel through compile-stage evidence, so check and
  publication retain the same node-history and schedule diagnostics. Not supplied: the
  projection readiness's BusinessModel scope (``compiled_registry``,
  ``driver_assumption_plan``, artifact forecast coverage) and ``diagnostic_report`` (``None``).
  """
  from schema.build import _merge_model_spec_valuation_controls
  from schema.excel_writer import render_xlsx_bytes
  from schema.formatter import formatter_for_declared_style_guide
  from schema.model_kind_dispatch import declared_capabilities, declared_style_guide
  from schema.model_semantics import canonical_workbook_semantics_hash
  from schema.renderer import finalize_render_contract, reconcile_render_contract, render_model
  from schema.renderer_annual_context import (
    _append_valuation_evidence_status_block,
    _render_compiler_inputs,
  )
  from schema.renderer_lineage import append_lineage_index
  from schema.renderer_types import AnnualRenderContext

  if finalized_at.tzinfo is None or finalized_at.utcoffset() is None:
    raise ValueError("finalized_at must be timezone-aware")
  canonical_finalized_at = finalized_at.astimezone(timezone.utc)
  model_ref = ModelVersionRef.model_validate(model_ref)
  formatter = formatter_for_declared_style_guide(declared_style_guide(declared_capabilities(source_bundle)))
  render_compiler_inputs = _render_compiler_inputs(
    source_bundle=source_bundle,
    source_bundle_payload=source_bundle_payload,
    source_bundle_content_sha256=source_bundle_content_sha256,
    model_spec=model_spec,
    source_bundle_id=None if bundle_id is None else str(bundle_id),
    source_bundle_identity_digest=None if bundle_identity_digest is None else str(bundle_identity_digest),
  )
  canonical_control_curves = _canonical_control_curves(model_spec, source_bundle, control_curves)
  evaluation = _document_evaluation(
    doc, source_bundle=source_bundle, model_spec=model_spec,
    control_curves=canonical_control_curves, finalized_at=finalized_at,
    input_findings=input_findings,
  )
  model, ours, typed_values = evaluation.model, evaluation.values, evaluation.typed_values
  projection_periods = tuple(int(period) for period in model.time_structure.projection_periods)
  # A graph miss on a period the line does not render is not a cell. A cited
  # absence and a formula that failed stay, and render ``=NA()``.
  render_context = AnnualRenderContext(
    version_label=f"Version {model_ref.version}",
    version_number=model_ref.version,
    required_unavailable=evaluation.compile_stage.render_unavailable,
  )
  rendered_model = model.model_copy(deep=True)
  rendered_model.build_index()
  document.lower_optional_absences(doc, rendered_model)
  document._materialize_semantic_raw(rendered_model)
  plan = render_model(rendered_model, formatter=formatter, annual_context=render_context)
  valuation_evidence_status = derive_valuation_evidence_status(
    source_bundle, None if model_spec.valuation_policy is None else model_spec.valuation_policy.peer_policy,
    bundle_content_sha256=source_bundle_content_sha256,
  )
  _append_valuation_evidence_status_block(plan, valuation_evidence_status)
  reconcile_render_contract(rendered_model, plan, formatter=formatter, annual_context=render_context)

  seed_rows = _document_seed_rows(doc, model, source_bundle=source_bundle)
  seed_coverage_manifest: tuple[Mapping[str, Any], ...] = tuple(
    row for row in seed_rows if row["source_kind"] == "model_spec_assumption"
  )
  document_seed_rows = tuple(row for row in seed_rows if row["source_kind"] != "model_spec_assumption")
  from model_build import target_build
  ours = document.recalc(doc)
  preview_provenance = target_build.project_provenance_map(
    doc,
    ours,
    plan,
    source_bundle_digest=source_bundle_content_sha256,
    model_spec_digest=compiler_input_content_hash(model_spec).removeprefix("sha256:"),
    source_bundle=source_bundle,
    model_spec=model_spec,
    seed_coverage_manifest=seed_rows,
    evaluation_reads=evaluation.evaluation_reads,
    case_evaluation_reads=evaluation.case_evaluation_reads,
  )
  schedule_input_audit = build_schedule_input_audit(
    model=model,
    typed_values=typed_values,
    projection_periods=projection_periods,
    working_capital_required=tuple(item_id for item_id in _WORKING_CAPITAL_REQUIRED if item_id in model._index),
    provenance=preview_provenance,
    semantic_cells=tuple(plan.semantic_cells),
  )
  append_lineage_index(
    plan=plan,
    provenance=preview_provenance,
    model=model,
    formatter=formatter,
    declinations={declination.declination_id: declination for declination in model_spec.declinations},
  )
  finalize_render_contract(plan)

  render_state = construct_model_render_state(
    model_id=model_ref.model_id,
    model_version=model_ref.version,
    workbook_semantics_hash=canonical_workbook_semantics_hash(
      ticker=ticker,
      control_curves=canonical_control_curves,
      valuation=_merge_model_spec_valuation_controls(model_spec.valuation_controls),
    ),
    compiler_inputs=render_compiler_inputs,
    seed_coverage_manifest=seed_coverage_manifest,
  )
  workbook_content = render_xlsx_bytes(
    plan,
    finalized_at=canonical_finalized_at,
    stamp=ModelStamp(
      model_id=model_ref.model_id,
      version=model_ref.version,
      build_id=render_state["identity"]["build_id"],
    ),
  )
  annual_presentation = evaluate_annual_presentation(
    model,
    render_plan=plan,
    cell_readback=read_annual_presentation_cells(workbook_content),
    workbook_sha256=hashlib.sha256(workbook_content).hexdigest(),
    render_state_hash=render_state_hash(render_state),
    formatter=formatter,
  )
  return StagedVersion(
    model=model,
    render_plan=plan,
    workbook_content=workbook_content,
    target_model_ref={
      "model_id": model_ref.model_id,
      "version": model_ref.version,
      "render_state": render_state,
      "annual_presentation": annual_presentation.model_dump(mode="json"),
      "provenance": preview_provenance.model_dump(mode="json"),
      "bound_scenario_control_ids": list(bound_scenario_control_ids),
      "scenario_controls": target_build.scenario_publication_controls_payload(
        canonical_control_curves, bound_scenario_control_ids,
      ),
      "unclaimed_filed_lines": [dict(record) for record in unclaimed_filed_lines],
      "document_seed_rows": list(document_seed_rows),
      "document_declinations": [dict(row) for row in document.declinations(doc)],
      "validation_template": None if template is None else template.model_dump(mode="json"),
    },
    compile_stage=replace(evaluation.compile_stage, schedule_input_audit=schedule_input_audit),
  )


def document_provenance(
  doc: document.ModelSheet, *, model: FinancialModel, source_bundle: Any, model_spec: Any,
  values: document.Values,
  evaluation_reads: Mapping[tuple[str, int], tuple[tuple[str, int], ...]] | None = None,
  case_evaluation_reads: Mapping[str, Mapping[tuple[str, int], tuple[tuple[str, int], ...]]] | None = None,
) -> Any:
  """The ``ProvenanceMap`` of ``doc`` evaluated to ``values``: the one projection a graph check
  and a pre-publish read of the judgment rows (``check_model(section="sensitivity")``) share."""
  from model_build import target_build

  return target_build.project_provenance_map(
    doc, values,
    source_bundle_digest=model_spec.source_bundle_sha256,
    model_spec_digest=compiler_input_content_hash(model_spec).removeprefix("sha256:"),
    source_bundle=source_bundle, model_spec=model_spec,
    seed_coverage_manifest=_document_seed_rows(doc, model, source_bundle=source_bundle),
    values=values, evaluation_reads=evaluation_reads, case_evaluation_reads=case_evaluation_reads,
  )


def validate_document_graph(
  doc: document.ModelSheet,
  *,
  ticker: str,
  source_bundle: Any,
  model_spec: Any,
  bundle_id: str | None,
  template: FinancialModel | None,
  control_curves: tuple[Any, ...] | None = None,
  bound_scenario_control_ids: tuple[str, ...] = (),
  unclaimed_filed_lines: tuple[Mapping[str, Any], ...] = (),
  values: document.Values | None = None,
  evaluation_reads: Mapping[tuple[str, int], tuple[tuple[str, int], ...]] | None = None,
  input_findings: Collection[Mapping[str, Any]] = (),
  declinations: Sequence[Mapping[str, Any]] = (),
) -> ValidationReport:
  """Evaluate the authored document without rendering or reading a workbook.

  The construction ``template`` is supplied by the working-document package,
  never loaded during a check. Publication uses that same baseline.
  ``values`` reuses the caller's exact document recalculation in every evaluator.
  ``evaluation_reads`` carries the same recalculation's exact selected operands.
  ``declinations`` (``document.declinations(doc)``: the analyst's ``decline`` ops) answer the
  declinable warnings of the items they name.
  """
  validated_at = datetime.now(timezone.utc)
  evaluation = _document_evaluation(
    doc, source_bundle=source_bundle, model_spec=model_spec,
    control_curves=control_curves, finalized_at=validated_at, values=values,
    evaluation_reads=evaluation_reads,
    input_findings=input_findings,
  )
  model = evaluation.model
  provenance = document_provenance(
    doc, model=model, source_bundle=source_bundle, model_spec=model_spec,
    values=evaluation.values, evaluation_reads=evaluation.evaluation_reads,
    case_evaluation_reads=evaluation.case_evaluation_reads,
  )
  schedule_input_audit = build_schedule_input_audit(
    model=model, typed_values=evaluation.typed_values,
    projection_periods=tuple(int(period) for period in model.time_structure.projection_periods),
    working_capital_required=tuple(item_id for item_id in _WORKING_CAPITAL_REQUIRED if item_id in model._index),
    provenance=provenance, semantic_cells=tuple(document_semantic_cells(doc, model, owned_cell_gaps(model, source_bundle))),
  )
  return validate_offline_build_artifacts(
    model=model, source_bundle=source_bundle, model_spec=model_spec,
    provenance=provenance, template=template, validated_at=validated_at,
    compile_stage=replace(evaluation.compile_stage, schedule_input_audit=schedule_input_audit),
    bound_scenario_control_ids=bound_scenario_control_ids,
    unclaimed_filed_lines=unclaimed_filed_lines,
    values=evaluation.typed_values,
    controls_by_item={} if template is None else None,
    document_decline_ops=declinations,
  ).report
