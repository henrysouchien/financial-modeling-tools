"""``trace`` and ``enumerate``: the kind and lineage of a compiled number.

Engine (B), pure readers over one ModelVersion's stored artifacts; nothing is
read from a store or written. Every value they name is labelled with its
:data:`~schema.models.OperationKind`, read where the kind is typed: a
compiled value's ``ValueProvenance`` (``operation_kind``), and otherwise the
formula that computes the cell (a derivation).

- ``trace(document, model_spec, item_id=, period=)`` walks one rendered
  cell's formula references down the compiled model and returns each cell it
  reaches with its kind, plus the facts and judgments the cell rests on. The
  origins come from the cell's own ``CellLineage``, which the compiler closes
  over every upstream reference, so the origin set is the cell's whole
  dependency set. A forecast-assumption origin names its
  ForecastAssumptionSet row (the driver key the ModelSpec assumption was
  minted from); a historical driver seed names its HistoricalFoundation row;
  a fact bottoms out at the evidence id and the filing accession the producer
  recorded (plan §10: no producer fact-id mint).
- ``enumerate(document, artifacts)`` lists every judgment row of the bound
  stage artifacts (``judgment_history.judgment_rows``: the keys a
  Supersession names) with the supersession, if any, that revised it, plus
  every compiled value the compiler wrote as a house ``default`` and the count
  of compiled values by kind.

``history`` is ``schema.judgment_history.history``; ``diff`` is
``schema.model_version_compare.compare_model_versions``.
"""

from __future__ import annotations

from collections import Counter, deque
from collections.abc import Iterable, Mapping
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from .build_model_product_contracts import CellLineage, ModelSpec
from .business_model import DriverAssumptionPlanEntry
from .business_model_revision import BusinessModelRevision
from .business_model_revision_ref import BusinessModelRevisionRef
from .forecast_assumption_set import (
    ForecastAssumptionSet,
    ForecastAssumptionSetRef,
    ForecastDriverAssumption,
)
from .historical_driver_seeds import HistoricalDriverSeed
from .historical_foundation import HistoricalFoundation, HistoricalFoundationRef
from .judgment_history import (
    JudgmentArtifact,
    JudgmentArtifactRef,
    artifact_supersessions,
    judgment_rows,
    seed_row_key,
    valuation_policy_row_key,
)
from .judgment_revision import Supersession
from .model_semantic_document import ModelSemanticDocument, SemanticValueSnapshot
from .model_spec_authoring import HistoricalNormalizationDecisionAuthoring
from .models import (
    FormulaSpec,
    LineItem,
    LineItemRef,
    OperationKind,
    ValueCell,
    operation_kind,
    shift_period,
)
from .scenario_set import ScenarioSetArtifact, ScenarioSetRef


#: The stage artifact a judgment row lives on, with a persisted history loader.
JudgmentArtifactKind = Literal[
    "business_model_revision",
    "historical_foundation",
    "forecast_assumption_set",
    "scenario_set",
    "valuation_policy",
]
OriginArtifactKind = Literal[JudgmentArtifactKind, "analyst_note"]

#: The artifact and assumption-id prefix for an analyst's cell-local judgment.
ANALYST_NOTE_ARTIFACT = "analyst_note"

#: The lineage namespace for a case's override of a forecast assumption.
SCENARIO_ASSUMPTION_PREFIX = "scenario:"


class _QueryContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class TraceCellRef(_QueryContract):
    item_id: str = Field(min_length=1)
    period: int


class TraceCell(_QueryContract):
    """One cell the traced value depends on, the traced cell first."""

    item_id: str = Field(min_length=1)
    period: int
    label: str | None = None
    #: The rendered cell; null for a referenced period the workbook does not render.
    sheet: str | None = None
    cell: str | None = None
    value: Any = None
    #: ``ValueProvenance`` of the compiled value when the model stores one, else
    #: ``derivation`` for a cell its formula computes; null when the model holds
    #: neither for this period.
    operation_kind: OperationKind | None = None
    #: ``type`` or ``type/subtype`` of the formula that computes the cell.
    formula: str | None = None
    #: The compiler-owned origin of a deterministic cell (``CompilerOrigin.kind``).
    compiler_origin: str | None = None
    #: The cell's note on a ``default`` value: the house strategy that wrote it.
    note: str | None = None
    depends_on: tuple[TraceCellRef, ...] = ()


class TraceOrigin(_QueryContract):
    """One fact, judgment or house default the traced value rests on."""

    operation_kind: OperationKind
    #: The origin's own id: evidence, observation or fact id; ModelSpec
    #: assumption id or authored control id; ``driver_key@period_id`` of a
    #: seed; declination id; ``item_id@period`` of a default value.
    ref: str = Field(min_length=1)
    #: The stage artifact holding the row, and the row's key there.
    artifact: OriginArtifactKind | Literal["model_spec"] | None = None
    artifact_id: str | None = None
    row: str | None = None
    accession: str | None = None
    source_id: str | None = None
    fact_ids: tuple[str, ...] = ()
    rationale: str | None = None


class Trace(_QueryContract):
    item_id: str = Field(min_length=1)
    period: int
    lineage_status: str
    cells: tuple[TraceCell, ...]
    origins: tuple[TraceOrigin, ...]


class EnumeratedJudgment(_QueryContract):
    operation_kind: Literal["judgment"] = "judgment"
    artifact: JudgmentArtifactKind
    artifact_id: str = Field(min_length=1)
    #: The row's key: the one a Supersession and ``history`` take as subject.
    row: str = Field(min_length=1)
    label: str | None = None
    rationale: str | None = None
    #: This artifact's revision of the predecessor's row, if it recorded one.
    revision: Supersession | None = None


class EnumeratedDefault(_QueryContract):
    operation_kind: Literal["default"] = "default"
    item_id: str = Field(min_length=1)
    period: int
    label: str | None = None
    sheet: str | None = None
    cell: str | None = None
    value: float | None = None
    #: The house strategy the writer named in the value's note.
    note: str | None = None
    #: ModelSpec declinations naming this cell (``CellLineage.declination_ids``).
    declination_ids: tuple[str, ...] = ()


class Enumeration(_QueryContract):
    judgments: tuple[EnumeratedJudgment, ...]
    defaults: tuple[EnumeratedDefault, ...]
    #: Compiled values by operation kind, every sheet and period.
    value_kinds: dict[str, int]


def _line_items(document: ModelSemanticDocument) -> dict[str, LineItem]:
    return {
        item.id: item
        for sheet in document.model.sheets.values()
        for section in sheet.sections
        for item in section.line_items
    }


def _value_cell(item: LineItem | None, period: int) -> ValueCell | None:
    if item is None or item.values is None:
        return None
    return item.values.values.get(period)


def _formula_refs(value: Any, period: int, period_mode: str) -> tuple[TraceCellRef, ...]:
    """Every ``(item, period)`` a formula's params reference, in order."""

    refs: list[TraceCellRef] = []

    def visit(node: Any) -> None:
        if isinstance(node, LineItemRef):
            shifted = shift_period(period, int(node.t), period_mode)
            if shifted is not None:
                refs.append(TraceCellRef(item_id=node.id, period=int(shifted)))
        elif isinstance(node, Mapping):
            if isinstance(node.get("id"), str):
                shifted = shift_period(period, int(node.get("t", 0)), period_mode)
                if shifted is not None:
                    refs.append(TraceCellRef(item_id=node["id"], period=int(shifted)))
            else:
                for nested in node.values():
                    visit(nested)
        elif isinstance(node, (list, tuple)):
            for nested in node:
                visit(nested)

    visit(value)
    return tuple(dict.fromkeys(refs))


def _formula_name(formula: FormulaSpec | None) -> str | None:
    if formula is None:
        return None
    kind = formula.type.value
    return kind if formula.subtype is None else f"{kind}/{formula.subtype}"


def _cell_kind(value_cell: ValueCell | None, formula: FormulaSpec | None) -> OperationKind | None:
    if value_cell is not None:
        return operation_kind(value_cell.provenance)
    return "derivation" if formula is not None else None


def lineage_origins(
    lineage: CellLineage,
    model_spec: ModelSpec,
) -> list[TraceOrigin]:
    """The facts and judgment rows one cell's lineage names.

    A judgment origin's ``row`` is its ``judgment_rows`` key on ``artifact``.
    """

    origins: list[TraceOrigin] = []
    for evidence in lineage.evidence_refs:
        origins.append(
            TraceOrigin(
                operation_kind="fact",
                ref=evidence.evidence_id,
                accession=evidence.accession,
                source_id=str(evidence.source_id),
            )
        )
    for observation_id in lineage.observation_ids:
        origins.append(TraceOrigin(operation_kind="fact", ref=observation_id))
    for valuation in lineage.valuation_refs:
        origins.append(
            TraceOrigin(
                operation_kind="fact",
                ref=valuation.observation_id,
                source_id=str(valuation.source_id),
            )
        )
    for seed in lineage.historical_driver_seed_refs:
        # A seed read straight off a reported value is that fact; a derived,
        # proxy or normalized seed is the HistoricalFoundation's judgment row.
        key = seed_row_key(seed.driver_key, seed.period_id)
        observed = seed.disposition == "observed"
        origins.append(
            TraceOrigin(
                operation_kind="fact" if observed else "judgment",
                ref=key,
                artifact="historical_foundation",
                artifact_id=seed.historical_foundation_artifact_id,
                row=None if observed else key,
                fact_ids=seed.fact_ids,
                rationale=seed.derivation_rationale,
            )
        )
    forecast_drivers = {
        assumption.assumption_id: assumption.driver
        for assumption in model_spec.forecast_assumptions
        if assumption.assumption_id is not None
    }
    forecast_binding = model_spec.forecast_assumption_set_binding
    policy_binding = model_spec.valuation_policy_binding
    scenario_binding = model_spec.scenario_set_binding
    for assumption in lineage.assumption_refs:
        driver = forecast_drivers.get(assumption.assumption_id)
        if assumption.assumption_id.startswith(f"{ANALYST_NOTE_ARTIFACT}:"):
            artifact: OriginArtifactKind | None = ANALYST_NOTE_ARTIFACT
            artifact_id = None
            row: str | None = None
        elif assumption.assumption_id.startswith(SCENARIO_ASSUMPTION_PREFIX):
            forecast_id = assumption.assumption_id.removeprefix(
                SCENARIO_ASSUMPTION_PREFIX
            ).partition(":")[2]
            artifact = "scenario_set"
            artifact_id = (
                None if scenario_binding is None else scenario_binding.scenario_set_id
            )
            row = forecast_drivers.get(forecast_id)
        elif driver is not None:
            artifact = "forecast_assumption_set"
            artifact_id = (
                None
                if forecast_binding is None
                else forecast_binding.forecast_assumption_set_id
            )
            row = driver
        else:
            # Other authored controls the compiler writes into lineage are
            # projected from the ValuationPolicy (family authorizations, the
            # DCF control, net share dilution).
            artifact = "valuation_policy"
            artifact_id = (
                None if policy_binding is None else policy_binding.valuation_policy_id
            )
            row = valuation_policy_row_key(assumption.assumption_id)
        origins.append(
            TraceOrigin(
                operation_kind="judgment",
                ref=assumption.assumption_id,
                artifact=artifact,
                artifact_id=artifact_id,
                row=row,
                rationale=assumption.rationale,
            )
        )
    for declination_id in lineage.declination_ids:
        origins.append(
            TraceOrigin(
                operation_kind="judgment",
                ref=declination_id,
                artifact="model_spec",
                row=declination_id,
            )
        )
    return origins


def _parse_sheet_and_top_left_cell(
    *, sheet: str | None, cell: str
) -> tuple[str | None, str]:
    raw_cell = str(cell or "").strip()
    parsed_sheet = str(sheet).strip() if sheet is not None and str(sheet).strip() else None
    if "!" in raw_cell:
        sheet_part, cell_part = raw_cell.rsplit("!", 1)
        cleaned_sheet = sheet_part.strip()
        if len(cleaned_sheet) >= 2 and cleaned_sheet[0] == "'" and cleaned_sheet[-1] == "'":
            cleaned_sheet = cleaned_sheet[1:-1].replace("''", "'")
        if parsed_sheet is None and cleaned_sheet:
            parsed_sheet = cleaned_sheet
        raw_cell = cell_part.strip()
    top_left = raw_cell.split(":", 1)[0].strip().replace("$", "").upper()
    return parsed_sheet, top_left


def resolve_cell_snapshot(
    document: ModelSemanticDocument | Iterable[Any],
    *,
    sheet: str | None = None,
    cell: str,
) -> Any | None:
    """Resolve a workbook ``(sheet, cell)`` coordinate to its rendered ``SemanticValueSnapshot``."""

    values = (
        document.semantic_values
        if isinstance(document, ModelSemanticDocument)
        else tuple(document)
    )
    target_sheet, target_cell = _parse_sheet_and_top_left_cell(sheet=sheet, cell=cell)
    if not target_cell:
        return None
    if target_sheet is not None:
        target_sheet_lower = target_sheet.lower()
        for snapshot in values:
            if (
                str(snapshot.cell).upper() == target_cell
                and str(snapshot.sheet).lower() == target_sheet_lower
            ):
                return snapshot
        return None
    matches = [
        snapshot
        for snapshot in values
        if str(snapshot.cell).upper() == target_cell
    ]
    return matches[0] if len(matches) == 1 else None


def trace(
    document: ModelSemanticDocument,
    model_spec: ModelSpec,
    *,
    item_id: str,
    period: int,
) -> Trace | None:
    """The cells and origins one rendered value depends on; ``None`` if not rendered."""

    snapshots: dict[tuple[str, int], SemanticValueSnapshot] = {
        (snapshot.semantic_item_id, snapshot.period): snapshot
        for snapshot in document.semantic_values
    }
    root = snapshots.get((item_id, period))
    if root is None:
        return None
    lineages = {(cell.sheet, cell.cell): cell for cell in document.provenance.cells}
    root_lineage = lineages.get((root.sheet, root.cell))
    if root_lineage is None:
        return None
    items = _line_items(document)
    period_mode = document.model.time_structure.period_mode

    cells: list[TraceCell] = []
    seen: set[TraceCellRef] = set()
    queue = deque([TraceCellRef(item_id=item_id, period=period)])
    default_origins: list[TraceOrigin] = []
    reached_origins: list[TraceOrigin] = []
    while queue:
        ref = queue.popleft()
        if ref in seen:
            continue
        seen.add(ref)
        snapshot = snapshots.get((ref.item_id, ref.period))
        lineage = (
            None
            if snapshot is None
            else lineages.get((snapshot.sheet, snapshot.cell))
        )
        item = items.get(ref.item_id)
        value_cell = _value_cell(item, ref.period)
        formula = None if lineage is None else lineage.formula
        depends_on = (
            ()
            if formula is None
            or (
                formula.subtype == "scenario_snapshot"
                and (formula.params or {}).get("case") in {"bull", "bear"}
            )
            else _formula_refs(formula.params, ref.period, period_mode)
        )
        kind = _cell_kind(value_cell, formula)
        note = None if value_cell is None else value_cell.note
        cells.append(
            TraceCell(
                item_id=ref.item_id,
                period=ref.period,
                label=(
                    snapshot.semantic_label
                    if snapshot is not None
                    else None if item is None else item.label
                ),
                sheet=None if snapshot is None else snapshot.sheet,
                cell=None if snapshot is None else snapshot.cell,
                value=(
                    snapshot.value
                    if snapshot is not None
                    else None if value_cell is None else value_cell.value
                ),
                operation_kind=kind,
                formula=_formula_name(formula),
                compiler_origin=(
                    None
                    if lineage is None or lineage.compiler_origin is None
                    else lineage.compiler_origin.kind
                ),
                note=note,
                depends_on=depends_on,
            )
        )
        if lineage is not None:
            reached_origins.extend(lineage_origins(lineage, model_spec))
        if kind == "default":
            default_origins.append(
                TraceOrigin(
                    operation_kind="default",
                    ref=f"{ref.item_id}@{ref.period}",
                    row=ref.item_id,
                    rationale=note,
                )
            )
        queue.extend(dependency for dependency in depends_on if dependency not in seen)

    origins = reached_origins + default_origins
    return Trace(
        item_id=item_id,
        period=period,
        lineage_status=root_lineage.lineage_status,
        cells=tuple(cells),
        origins=tuple(dict.fromkeys(origins)),
    )


def artifact_ref_id(ref: JudgmentArtifactRef) -> str:
    """The id a stage artifact's own ref names it by, as its ModelSpec binding does."""

    if isinstance(ref, BusinessModelRevisionRef):
        return ref.revision
    if isinstance(ref, HistoricalFoundationRef):
        return ref.artifact_id
    if isinstance(ref, (ForecastAssumptionSetRef, ScenarioSetRef)):
        return ref.set_id
    return ref.policy_id


def artifact_identity(artifact: JudgmentArtifact) -> tuple[JudgmentArtifactKind, str]:
    """The artifact's kind and the id its ModelSpec binding names it by."""

    identity = artifact_ref_id(artifact.ref)
    if isinstance(artifact, BusinessModelRevision):
        return "business_model_revision", identity
    if isinstance(artifact, HistoricalFoundation):
        return "historical_foundation", identity
    if isinstance(artifact, ForecastAssumptionSet):
        return "forecast_assumption_set", identity
    if isinstance(artifact, ScenarioSetArtifact):
        return "scenario_set", identity
    return "valuation_policy", identity


def _row_label(row: BaseModel) -> str | None:
    if isinstance(row, ForecastDriverAssumption):
        return row.assumption.driver
    if isinstance(row, DriverAssumptionPlanEntry):
        return row.label
    if isinstance(row, HistoricalNormalizationDecisionAuthoring):
        return row.label
    if isinstance(row, HistoricalDriverSeed):
        return row.driver_key
    return None


def _row_rationale(row: BaseModel) -> str | None:
    if isinstance(row, ForecastDriverAssumption):
        return row.assumption.rationale
    if isinstance(row, HistoricalNormalizationDecisionAuthoring):
        return row.description
    if isinstance(row, HistoricalDriverSeed):
        return row.derivation_rationale
    rationale = getattr(row, "rationale", None)
    return rationale if isinstance(rationale, str) else None


def enumerate(
    document: ModelSemanticDocument,
    artifacts: Iterable[JudgmentArtifact],
) -> Enumeration:
    """Every judgment row of ``artifacts`` and every house default the version holds."""

    judgments: list[EnumeratedJudgment] = []
    for artifact in artifacts:
        kind, artifact_id = artifact_identity(artifact)
        revisions = artifact_supersessions(artifact)
        for key, row in judgment_rows(artifact).items():
            judgments.append(
                EnumeratedJudgment(
                    artifact=kind,
                    artifact_id=artifact_id,
                    row=key,
                    label=_row_label(row),
                    rationale=_row_rationale(row),
                    revision=revisions.get(key),
                )
            )

    snapshots = {
        (snapshot.semantic_item_id, snapshot.period): snapshot
        for snapshot in document.semantic_values
    }
    declinations = {
        (cell.sheet, cell.cell): cell.declination_ids
        for cell in document.provenance.cells
    }
    defaults: list[EnumeratedDefault] = []
    value_kinds: Counter[str] = Counter()
    for item in _line_items(document).values():
        if item.values is None:
            continue
        for period, value_cell in sorted(item.values.values.items()):
            kind = operation_kind(value_cell.provenance)
            value_kinds[kind] += 1
            if kind != "default":
                continue
            snapshot = snapshots.get((item.id, int(period)))
            defaults.append(
                EnumeratedDefault(
                    item_id=item.id,
                    period=int(period),
                    label=item.label,
                    sheet=None if snapshot is None else snapshot.sheet,
                    cell=None if snapshot is None else snapshot.cell,
                    value=value_cell.value,
                    note=value_cell.note,
                    declination_ids=(
                        ()
                        if snapshot is None
                        else declinations.get((snapshot.sheet, snapshot.cell), ())
                    ),
                )
            )
    return Enumeration(
        judgments=tuple(judgments),
        defaults=tuple(defaults),
        value_kinds=dict(sorted(value_kinds.items())),
    )


__all__ = [
    "ANALYST_NOTE_ARTIFACT",
    "EnumeratedDefault",
    "EnumeratedJudgment",
    "Enumeration",
    "JudgmentArtifactKind",
    "OriginArtifactKind",
    "SCENARIO_ASSUMPTION_PREFIX",
    "Trace",
    "TraceCell",
    "TraceCellRef",
    "TraceOrigin",
    "artifact_identity",
    "artifact_ref_id",
    "enumerate",
    "lineage_origins",
    "resolve_cell_snapshot",
    "trace",
]
