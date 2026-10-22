"""Model-kind capability declarations — what a model kind *does*, as data.

Issuer-specific capabilities belong in authored data artifacts, never in
issuer-named mechanism branches, constants, regexes, or special-cased driver
keys. This module owns the declaration record; ``model_kind_dispatch.py`` owns
its compile-time interpretation. See ``schema/README.md`` and
``docs/reference/build-model-infrastructure-architecture-2026-08-04.md``.

This module is the *record*, not the dispatcher. It carries one typed, frozen
declaration object — :class:`ModelKindCapabilities` — with exactly one field
per §3.1 branch family, so that a later step can replace each
``profile_id == "<issuer>_annual_v1"`` comparison with a read of the
declaration and delete the branch in the same change (plan §7 IG-4/IG-5, the
two-writers gate). Nothing here dispatches on anything yet: at IG-2 the
declarations are inert, and each branch dies in the step that lands its
dispatch.

Three rules govern every field:

* **Dispatch on the declaration, never on identity** (design principle P2).
  Generic code reads the declaration from its declared inputs
  (``source_bundle.coverage_profile.capabilities``); no mechanism module may
  consult a profile id, a ticker, or any issuer string for behavior.
* **Absence is feature-off** (P4). A model kind that declares nothing carries
  no capability record at all, and every scalar field's default *is* the
  generic arm the current ``else`` branches take — so "no record" and "a
  record holding only defaults" would describe the same system. The second
  shape is therefore refused at construction: declare absence by omitting the
  record, not by writing an empty one.
* **Fail closed on malformation** (plan §6). Every construction refusal is
  :class:`CapabilityDeclarationInvalid`, carrying the offending field, its
  value, and a named repair target. It is deliberately *not* a ``ValueError``,
  so it propagates out of pydantic validation as itself rather than being
  folded into a generic ``ValidationError`` that loses the stable code.

The module names no issuer: it is scanned by
``tests/test_issuer_generic_grep_guard.py`` from birth (plan §11, "every new
module this plan lands carries zero issuer names").
"""

from __future__ import annotations

from typing import Any, Final, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator

from .models import ModelScale
from industry_slice_core.industry_types import Unit


#: The stable code every construction refusal in this module carries
#: (plan §6, ``capability_declaration_invalid``).
CAPABILITY_DECLARATION_INVALID: Final = "capability_declaration_invalid"

#: The default repair lane for a malformed declaration (plan §6).
_MODEL_KIND_DATA_LANE: Final = "repair the declaration in the model-kind data lane"

#: Identifier namespaces the compile mechanism owns: template rows, business
#: model rows, and financial-model semantic rows. A model kind may not claim
#: one of these as its own legacy control namespace — that is the "alias
#: namespace colliding with a mechanism namespace" refusal of plan §6.
MECHANISM_OWNED_NAMESPACES: Final = ("tpl.", "bm.", "fm.")

#: A3: which control-binding pass the compile runs. ``registry_bound`` binds
#: this model kind's declared control keys onto the compiled driver registry;
#: ``unit_bound`` — the generic arm — binds only the control units.
ModelSpecControlBinding = Literal["registry_bound", "unit_bound"]

#: A3/A4: the schedule completions a model kind's compile runs. The empty
#: declaration means none run, which is the generic arm.
ScheduleCompletionId = Literal["financial_schedules", "working_capital"]

#: A7: which scenario output key publishes per-share earnings.
EpsPublicationBasis = Literal["publication_eps", "adj_eps"]

#: A13: where pinned market inputs come from.
MarketInputAuthority = Literal["bundle", "provider"]

#: A2: how forward evidence is discharged. ``oracle_backed`` reads a
#: checked-in expectation artifact; ``artifact_backed`` reads the accepted
#: forecast-assumption set. Absence means neither runs.
ForecastEvidenceMode = Literal["oracle_backed", "artifact_backed"]

#: S6: the origin classes provenance projection may declare for a workbook row
#: whose value the compiler itself owns. The vocabulary is the mechanism's —
#: only which rows carry which class is model-kind content.
CompilerOriginKind = Literal[
    "mechanical_constant",
    "scenario_control",
    "presentation_scaffold_value",
]

#: §14 IG-4(m): which half of an authored sensitivity grid a declared selector
#: names — a perturbed control axis or a reported output.
SensitivityBindingRole = Literal["control", "output"]

#: A10: what assumption lowering does with a control whose value an equation
#: owns. ``coverage_only`` keeps the typed assumption/driver chain in the
#: coverage manifest and writes no cell; ``carry_forward_basis`` writes the
#: authored flat curve once onto the period before the forecast axis, as the
#: equation's basis, and drops the requested periods.
EquationOwnedControlTreatment = Literal["coverage_only", "carry_forward_basis"]

#: The infix that marks a control selector as belonging to a *legacy control
#: namespace* rather than to the natural driver plane the compiled business
#: model owns. It is mechanism vocabulary, not an issuer's: a selector shaped
#: ``<namespace>.driver.<name>`` is a legacy control id, and the model kind
#: that still admits one must declare the namespace it lives in
#: (``ScenarioControlDeclaration.legacy_driver_namespace``). Natural selectors
#: — bare control keys and compiled business-model node paths — never carry it.
LEGACY_CONTROL_NAMESPACE_INFIX: Final = ".driver."


class CapabilityDeclarationInvalid(Exception):
    """Typed construction refusal for a malformed capability declaration.

    Deliberately not a ``ValueError``: pydantic converts ``ValueError`` and
    ``AssertionError`` raised inside validators into a generic
    ``ValidationError``, which would erase the stable code and the repair
    target. Every other exception type propagates unchanged, so this refusal
    survives nesting inside a coverage profile and inside a SourceBundle
    decode.
    """

    code: Final = CAPABILITY_DECLARATION_INVALID

    def __init__(
        self,
        *,
        field: str,
        value: object,
        repair: str = _MODEL_KIND_DATA_LANE,
    ) -> None:
        self.field = field
        self.value = value
        self.repair = repair
        super().__init__(f"{self.code}: {field}={value!r} — {repair}")


def _refuse(
    field: str, value: object, repair: str = _MODEL_KIND_DATA_LANE
) -> CapabilityDeclarationInvalid:
    return CapabilityDeclarationInvalid(field=field, value=value, repair=repair)


def _require_sorted_unique(
    field: str, values: tuple[str, ...], *, subject: str
) -> None:
    """Canonical ordering: a declaration has exactly one serialized form."""

    if any(not str(value).strip() for value in values):
        raise _refuse(field, values, f"{subject} must be non-blank")
    if len(values) != len(set(values)):
        raise _refuse(field, values, f"{subject} must be unique")
    if tuple(values) != tuple(sorted(values)):
        raise _refuse(
            field,
            values,
            f"{subject} must be sorted so the declaration has one canonical form",
        )


class _Declaration(BaseModel):
    """Frozen, closed, whitespace-normalized — the product-contract shape."""

    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        str_strip_whitespace=True,
    )


class ControlTargetDeclaration(_Declaration):
    """One declared control key bound to one workbook item (plan §3 row A1)."""

    control_key: str = Field(min_length=1)
    target_item_id: str = Field(min_length=1)


class ControlAliasDeclaration(_Declaration):
    """One legacy control selector aliased onto a mechanism control (row V7).

    The mechanism owns a small set of template/compiler forecast controls with
    stable ids. A model kind whose authored artifacts still name one of them
    through a legacy selector declares the alias here: the alias plane is the
    model kind's, so the mechanism's control rows carry no issuer vocabulary.
    """

    alias: str = Field(min_length=1)
    control_id: str = Field(min_length=1)


class EquationOwnedControlDeclaration(_Declaration):
    """One control whose value an equation owns (plan §3 row A10).

    Assumption lowering must keep the typed assumption/driver chain in the
    coverage manifest without replacing the computed row with a constant
    input. The two treatments differ in what they write, so the treatment is
    declared per control rather than inferred from the control's name.
    """

    control_key: str = Field(min_length=1)
    treatment: EquationOwnedControlTreatment


class ProvisionalValuationDefaultDeclaration(_Declaration):
    """One valuation scenario-table anchor seeded with a provisional default.

    Plan §3 row A5's second arm: a model kind whose reviewed valuation policy
    does not yet author its own scenario-table anchors declares the
    provisional constants the compile seeds, each stamped with the
    review-required note so the provisional state is visible in provenance.
    """

    anchor_item_id: str = Field(min_length=1)
    value: float
    unit: Unit


class ScenarioControlDeclaration(_Declaration):
    """The model kind's scenario control plane (plan §3 rows A1, A8, A9, V7).

    ``workbook_targets`` are the control-key-to-workbook-item bindings
    scenario-curve lowering uses. ``forecast_control_targets`` is the wider
    control-to-workbook binding the registry-bound control pass installs on
    the compiled driver registry — a superset of the scenario targets, since
    every scenario control is also a forecast control.
    ``legacy_driver_namespace`` is the one namespace in which this model kind
    still admits legacy control selectors — the declared replacement for an
    issuer-string gate, and the input the
    ``scenario_control_namespace_undeclared`` refusal reads;
    ``legacy_assumption_namespace`` is its sibling for authored assumption
    ids. ``legacy_control_aliases`` names the mechanism controls this model
    kind's legacy selectors alias.
    """

    workbook_targets: tuple[ControlTargetDeclaration, ...] = ()
    forecast_control_targets: tuple[ControlTargetDeclaration, ...] = ()
    legacy_driver_namespace: str | None = None
    legacy_assumption_namespace: str | None = None
    legacy_control_aliases: tuple[ControlAliasDeclaration, ...] = ()

    @model_validator(mode="after")
    def _declaration_shape(self) -> "ScenarioControlDeclaration":
        if not (
            self.workbook_targets
            or self.forecast_control_targets
            or self.legacy_driver_namespace
            or self.legacy_assumption_namespace
            or self.legacy_control_aliases
        ):
            raise _refuse(
                "scenario_control_curves",
                None,
                "an empty scenario-control declaration states nothing — omit "
                "the declaration to state that this model kind has no "
                "scenario control plane",
            )
        for field_name, namespace in (
            ("legacy_driver_namespace", self.legacy_driver_namespace),
            ("legacy_assumption_namespace", self.legacy_assumption_namespace),
        ):
            if namespace is None:
                continue
            field = f"scenario_control_curves.{field_name}"
            if not namespace.strip():
                raise _refuse(
                    field,
                    namespace,
                    "a declared legacy namespace must be non-blank",
                )
            if namespace.startswith(MECHANISM_OWNED_NAMESPACES):
                raise _refuse(
                    field,
                    namespace,
                    "the compile mechanism owns "
                    f"{MECHANISM_OWNED_NAMESPACES}; a model kind may not claim "
                    "one of those namespaces as its legacy control plane",
                )
            if not namespace.endswith("."):
                raise _refuse(
                    field,
                    namespace,
                    "a namespace is a dotted prefix, not an identifier — "
                    "declare it with its trailing separator",
                )
        driver_namespace = self.legacy_driver_namespace
        if driver_namespace is not None and (
            LEGACY_CONTROL_NAMESPACE_INFIX not in f".{driver_namespace}"
        ):
            raise _refuse(
                "scenario_control_curves.legacy_driver_namespace",
                driver_namespace,
                "a legacy control namespace is the one the mechanism "
                f"recognizes by its {LEGACY_CONTROL_NAMESPACE_INFIX!r} infix; "
                "a namespace without it would declare selectors the "
                "legacy-selector refusal can never see",
            )
        for field_name, rows in (
            ("workbook_targets", self.workbook_targets),
            ("forecast_control_targets", self.forecast_control_targets),
        ):
            field = f"scenario_control_curves.{field_name}"
            _require_sorted_unique(
                field,
                tuple(row.control_key for row in rows),
                subject="declared control keys",
            )
            item_ids = tuple(row.target_item_id for row in rows)
            if len(item_ids) != len(set(item_ids)):
                raise _refuse(
                    field,
                    item_ids,
                    "two declared controls may not drive one workbook row",
                )
        forecast_map = {
            row.control_key: row.target_item_id
            for row in self.forecast_control_targets
        }
        if forecast_map:
            for row in self.workbook_targets:
                if forecast_map.get(row.control_key) != row.target_item_id:
                    raise _refuse(
                        "scenario_control_curves.workbook_targets",
                        row.control_key,
                        "every scenario control is also a forecast control — "
                        "declare the same workbook target in "
                        "forecast_control_targets or omit that table",
                    )
        _require_sorted_unique(
            "scenario_control_curves.legacy_control_aliases",
            tuple(row.alias for row in self.legacy_control_aliases),
            subject="declared control aliases",
        )
        for row in self.legacy_control_aliases:
            if driver_namespace is not None and not row.alias.startswith(
                driver_namespace
            ):
                raise _refuse(
                    "scenario_control_curves.legacy_control_aliases",
                    row.alias,
                    "a declared alias must live inside this model kind's "
                    f"declared legacy namespace {driver_namespace!r}",
                )
            if row.control_id.startswith(MECHANISM_OWNED_NAMESPACES):
                raise _refuse(
                    "scenario_control_curves.legacy_control_aliases",
                    row.control_id,
                    "an alias names a mechanism *control id*, not a workbook "
                    f"row; {MECHANISM_OWNED_NAMESPACES} are workbook "
                    "namespaces",
                )
        return self

    def workbook_target_map(self) -> Mapping[str, str]:
        """Declared control key -> workbook item id (scenario lowering)."""

        return {row.control_key: row.target_item_id for row in self.workbook_targets}

    def forecast_control_target_map(self) -> Mapping[str, str]:
        """Declared control key -> workbook item id (registry-bound pass)."""

        return {
            row.control_key: row.target_item_id
            for row in self.forecast_control_targets
        }

    def legacy_control_alias_map(self) -> Mapping[str, str]:
        """Declared legacy selector -> mechanism control id."""

        return {row.alias: row.control_id for row in self.legacy_control_aliases}


class AuthenticatedDependentsDeclaration(_Declaration):
    """Enable post-compile software and historical presentation finalization.

    Client-fund obligation economics belong to the declared balance policy,
    not to a separately selected growth control.
    """


class ShareConversionDeclaration(_Declaration):
    """The repurchase-dollars-to-share-count basis (rows A5, A6, A10, A11).

    The four item ids are workbook rows the conversion names: the quote it reads, the price row it
    binds the quote to, the repurchase dollars and the share count. The equation itself is the
    template's; the conversion binds the price (``build_scenarios._materialize_declared_share_conversion``)
    and gives the price and the count their periods, and ``repurchase_amount_item_id`` is read only
    by the distinctness check below.
    ``equation_owned_controls`` names the controls whose values an equation
    owns, each with the lowering treatment it takes, so assumption lowering
    keeps their typed chain in the coverage manifest without replacing the
    computed row with a constant input — the declared form of the literal
    driver-key comparisons the compile carried before.
    """

    quote_source_item_id: str = Field(min_length=1)
    average_price_item_id: str = Field(min_length=1)
    repurchase_amount_item_id: str = Field(min_length=1)
    shares_repurchased_item_id: str = Field(min_length=1)
    equation_owned_controls: tuple[EquationOwnedControlDeclaration, ...] = ()
    authenticated_dependents: AuthenticatedDependentsDeclaration | None = None

    @model_validator(mode="after")
    def _declaration_shape(self) -> "ShareConversionDeclaration":
        _require_sorted_unique(
            "share_conversion.equation_owned_controls",
            tuple(row.control_key for row in self.equation_owned_controls),
            subject="equation-owned control keys",
        )
        operands = (
            self.quote_source_item_id,
            self.average_price_item_id,
            self.repurchase_amount_item_id,
            self.shares_repurchased_item_id,
        )
        if len(set(operands)) != len(operands):
            raise _refuse(
                "share_conversion",
                operands,
                "the conversion rows must be four distinct workbook rows",
            )
        return self

    def equation_owned_control_map(self) -> Mapping[str, EquationOwnedControlTreatment]:
        """Declared equation-owned control key -> lowering treatment."""

        return {
            row.control_key: row.treatment for row in self.equation_owned_controls
        }


class ForecastEvidenceDeclaration(_Declaration):
    """How forward evidence is discharged, under which namespace (A2, A14).

    ``requirement_namespace`` is the durable identity prefix this model kind's
    forecast-evidence requirement ids were first published under; a later
    profile release may deliberately retain a predecessor's namespace, so the
    namespace is declared rather than derived from the profile id.

    Unlike ``ScenarioControlDeclaration.legacy_driver_namespace`` — which *is*
    a dotted prefix and is matched against selector strings as written — this
    field is a bare identifier that :meth:`requirement_id_prefix` turns into a
    dotted prefix. The mechanism-collision refusal therefore has to test the
    prefix this declaration *emits*, not the identifier as authored: an
    undotted ``"tpl"`` mints ``tpl.forecast_evidence.…``, which lands squarely
    inside the mechanism-owned ``tpl.`` namespace the refusal exists to
    protect.
    """

    mode: ForecastEvidenceMode
    requirement_namespace: str = Field(min_length=1)

    @model_validator(mode="after")
    def _declaration_shape(self) -> "ForecastEvidenceDeclaration":
        namespace = self.requirement_namespace
        if namespace.endswith("."):
            raise _refuse(
                "forecast_evidence.requirement_namespace",
                namespace,
                "a requirement namespace is an identifier, not a dotted "
                "prefix — declare it without its trailing separator (the "
                "separator is supplied by the emitted requirement id prefix)",
            )
        # Test the id prefix this declaration MINTS, not the identifier as
        # authored: `tpl` mints `tpl.forecast_evidence.`, inside the
        # mechanism-owned `tpl.` namespace, while `tplx` mints
        # `tplx.forecast_evidence.` and is legal. Matching the raw identifier
        # against the dotted tuple would accept `tpl` — the obvious authoring
        # spelling — and refuse only the dotted one the shape rule above has
        # already rejected.
        if self.requirement_id_prefix().startswith(MECHANISM_OWNED_NAMESPACES):
            raise _refuse(
                "forecast_evidence.requirement_namespace",
                namespace,
                "the compile mechanism owns "
                f"{MECHANISM_OWNED_NAMESPACES}; a forecast-evidence "
                "requirement namespace may not mint ids inside one "
                f"(this one mints {self.requirement_id_prefix()!r})",
            )
        return self

    def requirement_id_prefix(self) -> str:
        """The declared prefix every forecast-evidence requirement id carries."""

        return f"{self.requirement_namespace}.forecast_evidence."


class SemanticUnitDeclaration(_Declaration):
    """One declared semantic row's unit and scale (plan §3 row S4)."""

    semantic_item_id: str = Field(min_length=1)
    unit: Unit
    scale: ModelScale


class GroundedSemanticRowDeclaration(_Declaration):
    """One grounded-schedule semantic role and its item id (row S5)."""

    semantic_role: str = Field(min_length=1)
    item_id: str = Field(min_length=1)


class SemanticRowMaterializationDeclaration(_Declaration):
    """One semantic control row the registry-bound pass mints (plan §14 IG-4(m)).

    The compile inserts a model kind's own percentage controls into the
    workbook after a named anchor row, and — for a growth control — projects
    the anchored row off the minted rate. Both the ids and the provenance note
    the minted formula carries are model-kind content, so they are declared
    rather than written as module constants beside the mechanism.

    ``growth_target_item_id`` and ``growth_note`` are declared together: a
    growth note with no target would name a formula nothing writes, and a
    target with no note would mint an unlabelled provenance stamp.

    Two rows may share an anchor. Insertion is *after* the anchor, so the
    later-declared row lands nearer it — which is why the minting table is the
    one declaration here that is ordered rather than sorted (see
    :class:`ModelKindCapabilities`): its order is workbook-observable content.
    """

    item_id: str = Field(min_length=1)
    label: str = Field(min_length=1)
    anchor_item_id: str = Field(min_length=1)
    unit: Unit
    scale: ModelScale
    growth_target_item_id: str | None = None
    growth_note: str | None = None

    @model_validator(mode="after")
    def _declaration_shape(self) -> "SemanticRowMaterializationDeclaration":
        declared = tuple(
            name
            for name, value in (
                ("growth_target_item_id", self.growth_target_item_id),
                ("growth_note", self.growth_note),
            )
            if value is not None and value.strip()
        )
        if len(declared) == 1:
            raise _refuse(
                "semantic_row_materializations",
                self.item_id,
                "declare growth_target_item_id and growth_note together, or "
                "neither — a growth row with only one of them either writes an "
                "unstamped formula or stamps a formula nothing writes",
            )
        if self.growth_target_item_id == self.item_id:
            raise _refuse(
                "semantic_row_materializations",
                self.item_id,
                "a minted rate row may not be its own growth target",
            )
        return self


class CompilerOriginDeclaration(_Declaration):
    """One workbook row's declared compiler origin class (plan §3 row S6).

    Provenance projection classifies an originless semantic cell — one with no
    observation, assumption, evidence, valuation or seed reference — and a row
    whose value the compiler itself owns has no such reference to find. Which
    rows those are is model-kind content: the classification was a hand table
    calibrated against the reference workbook, keyed by item id.
    """

    item_id: str = Field(min_length=1)
    origin_kind: CompilerOriginKind


class SensitivityBindingDeclaration(_Declaration):
    """One authored sensitivity selector bound to a compiled row (§14 IG-4(m)).

    A model kind's authored valuation policy names its sensitivity axes and
    outputs with its own selector ids. Each selector resolves through exactly
    one arm: a literal workbook row, an ordered list of compiled business-model
    node paths, or an ordered list of compiled driver-key paths. The ordered
    arms are first-match, which is how a selector spans two segment shapes
    without the mechanism knowing either.
    """

    selector_id: str = Field(min_length=1)
    role: SensitivityBindingRole
    item_id: str | None = None
    node_item_paths: tuple[str, ...] = ()
    driver_key_paths: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _exactly_one_arm(self) -> "SensitivityBindingDeclaration":
        declared = tuple(
            name
            for name, value in (
                ("item_id", (self.item_id or "").strip()),
                ("node_item_paths", self.node_item_paths),
                ("driver_key_paths", self.driver_key_paths),
            )
            if value
        )
        if len(declared) != 1:
            raise _refuse(
                "sensitivity_bindings",
                self.selector_id,
                "declare exactly one resolution arm — a literal item_id, "
                "node_item_paths, or driver_key_paths",
            )
        for field_name, paths in (
            ("node_item_paths", self.node_item_paths),
            ("driver_key_paths", self.driver_key_paths),
        ):
            if any(not path.strip() for path in paths):
                raise _refuse(
                    f"sensitivity_bindings.{field_name}",
                    self.selector_id,
                    "a declared resolution path must be non-blank",
                )
            if len(paths) != len(set(paths)):
                raise _refuse(
                    f"sensitivity_bindings.{field_name}",
                    self.selector_id,
                    "declared resolution paths must be unique",
                )
        return self


class ModelKindCapabilities(_Declaration):
    """Everything a model kind declares about how its compile behaves.

    One field per §3.1 branch family (plan §5.1). Every scalar default is the
    generic arm of the branch it replaces, so a model kind that declares
    nothing behaves exactly as the current ``else`` path does today.
    """

    scenario_control_curves: ScenarioControlDeclaration | None = None
    model_spec_control_binding: ModelSpecControlBinding = "unit_bound"
    schedule_completions: tuple[ScheduleCompletionId, ...] = ()
    share_conversion: ShareConversionDeclaration | None = None
    eps_publication_basis: EpsPublicationBasis = "adj_eps"
    market_input_authority: MarketInputAuthority = "provider"
    forecast_evidence: ForecastEvidenceDeclaration | None = None
    provisional_valuation_defaults: tuple[
        ProvisionalValuationDefaultDeclaration, ...
    ] = ()
    semantic_units: tuple[SemanticUnitDeclaration, ...] = ()
    grounded_semantic_rows: tuple[GroundedSemanticRowDeclaration, ...] = ()
    semantic_row_materializations: tuple[
        SemanticRowMaterializationDeclaration, ...
    ] = ()
    compiler_origin_classifications: tuple[CompilerOriginDeclaration, ...] = ()
    sensitivity_bindings: tuple[SensitivityBindingDeclaration, ...] = ()
    style_guide: str | None = None

    @model_validator(mode="after")
    def _declaration_shape(self) -> "ModelKindCapabilities":
        _require_sorted_unique(
            "schedule_completions",
            self.schedule_completions,
            subject="declared schedule completions",
        )
        _require_sorted_unique(
            "provisional_valuation_defaults",
            tuple(row.anchor_item_id for row in self.provisional_valuation_defaults),
            subject="declared provisional valuation anchors",
        )
        _require_sorted_unique(
            "semantic_units",
            tuple(row.semantic_item_id for row in self.semantic_units),
            subject="declared semantic unit ids",
        )
        _require_sorted_unique(
            "grounded_semantic_rows",
            tuple(row.semantic_role for row in self.grounded_semantic_rows),
            subject="declared grounded semantic roles",
        )
        grounded_ids = tuple(row.item_id for row in self.grounded_semantic_rows)
        if len(grounded_ids) != len(set(grounded_ids)):
            raise _refuse(
                "grounded_semantic_rows",
                grounded_ids,
                "two grounded semantic roles may not name one workbook row",
            )
        # Deliberately NOT sorted (the `_require_sorted_unique` convention every
        # other table here follows): these rows are inserted into the workbook in
        # declared order, so the order is content, not presentation. Uniqueness
        # and non-blankness still hold.
        minted_ids = tuple(row.item_id for row in self.semantic_row_materializations)
        if len(minted_ids) != len(set(minted_ids)):
            raise _refuse(
                "semantic_row_materializations",
                minted_ids,
                "a semantic row may be minted only once",
            )
        _require_sorted_unique(
            "compiler_origin_classifications",
            tuple(row.item_id for row in self.compiler_origin_classifications),
            subject="declared compiler-origin rows",
        )
        _require_sorted_unique(
            "sensitivity_bindings",
            tuple(row.selector_id for row in self.sensitivity_bindings),
            subject="declared sensitivity selectors",
        )
        declared_unit_ids = {row.semantic_item_id for row in self.semantic_units}
        for row in self.semantic_row_materializations:
            if row.item_id.startswith(MECHANISM_OWNED_NAMESPACES):
                continue
            if row.item_id not in declared_unit_ids:
                # A minted row outside the mechanism's own namespaces carries no
                # template unit contract, so the projected-integrity checker has
                # nothing to enforce it against unless the unit is declared.
                raise _refuse(
                    "semantic_units",
                    row.item_id,
                    "a minted semantic row outside the mechanism-owned "
                    f"namespaces {MECHANISM_OWNED_NAMESPACES} must declare its "
                    "unit row, or the projected-integrity checker cannot hold "
                    "it to any unit contract",
                )
        if self.style_guide is not None and not self.style_guide.strip():
            raise _refuse(
                "style_guide",
                self.style_guide,
                "a declared style guide id must be non-blank",
            )
        conversion = self.share_conversion
        controls = self.scenario_control_curves
        if conversion is not None and controls is not None:
            # A row the conversion equation computes may not also be lowered
            # from an authored assumption as a constant input: the control
            # that binds it has to be declared equation-owned, or the compile
            # would silently replace the equation with the authored value.
            equation_rows = {
                conversion.average_price_item_id,
                conversion.shares_repurchased_item_id,
            }
            owned = {row.control_key for row in conversion.equation_owned_controls}
            for row in controls.forecast_control_targets:
                if row.target_item_id in equation_rows and row.control_key not in owned:
                    raise _refuse(
                        "share_conversion.equation_owned_controls",
                        row.control_key,
                        f"control {row.control_key!r} binds {row.target_item_id!r}, "
                        "a row the declared share-conversion equation computes — "
                        "declare it equation-owned or the compile would lower an "
                        "authored constant over the equation",
                    )
        if self == _GENERIC_CAPABILITIES:
            raise _refuse(
                "<record>",
                None,
                "a capability record holding only the generic defaults states "
                "nothing that absence does not already state — omit the record "
                "instead (plan §5.1: absence is feature-off)",
            )
        return self

    def semantic_unit_map(self) -> Mapping[str, tuple[Unit, ModelScale]]:
        """Declared semantic item id -> (unit, scale)."""

        return {
            row.semantic_item_id: (row.unit, row.scale) for row in self.semantic_units
        }

    def grounded_semantic_row_map(self) -> Mapping[str, str]:
        """Declared grounded semantic role -> workbook item id."""

        return {row.semantic_role: row.item_id for row in self.grounded_semantic_rows}

    def compiler_origin_map(self) -> Mapping[str, CompilerOriginKind]:
        """Declared workbook item id -> compiler origin class."""

        return {
            row.item_id: row.origin_kind
            for row in self.compiler_origin_classifications
        }

    def sensitivity_binding_map(self) -> Mapping[str, SensitivityBindingDeclaration]:
        """Declared sensitivity selector id -> its resolution arm."""

        return {row.selector_id: row for row in self.sensitivity_bindings}


#: The all-defaults record. Constructing it is refused (see the validator
#: above); it exists so that refusal has one exact subject rather than a
#: hand-listed field comparison that would drift as fields are added.
_GENERIC_CAPABILITIES = ModelKindCapabilities.model_construct()


def decode_model_kind_capabilities(payload: Any) -> ModelKindCapabilities:
    """Decode a capability declaration, fail-closed on every malformation.

    Every refusal is :class:`CapabilityDeclarationInvalid` with a stable code,
    the first offending field, and a repair target — including the shapes
    pydantic would otherwise report as a generic ``ValidationError`` (an
    unknown key, a malformed unit row, a value outside a declared enum).
    """

    if isinstance(payload, ModelKindCapabilities):
        return payload
    if not isinstance(payload, Mapping):
        raise _refuse(
            "<record>",
            payload,
            "a capability declaration must be a mapping of declared fields",
        )
    try:
        return ModelKindCapabilities.model_validate(dict(payload))
    except ValidationError as exc:
        first = exc.errors()[0]
        location = ".".join(str(part) for part in first.get("loc", ())) or "<record>"
        raise _refuse(
            location,
            first.get("input"),
            f"{first.get('msg', 'invalid declaration')} — {_MODEL_KIND_DATA_LANE}",
        ) from exc


__all__ = [
    "CAPABILITY_DECLARATION_INVALID",
    "LEGACY_CONTROL_NAMESPACE_INFIX",
    "MECHANISM_OWNED_NAMESPACES",
    "AuthenticatedDependentsDeclaration",
    "CapabilityDeclarationInvalid",
    "CompilerOriginDeclaration",
    "CompilerOriginKind",
    "ControlAliasDeclaration",
    "ControlTargetDeclaration",
    "EpsPublicationBasis",
    "EquationOwnedControlDeclaration",
    "EquationOwnedControlTreatment",
    "ForecastEvidenceDeclaration",
    "ForecastEvidenceMode",
    "GroundedSemanticRowDeclaration",
    "MarketInputAuthority",
    "ModelKindCapabilities",
    "ModelSpecControlBinding",
    "ProvisionalValuationDefaultDeclaration",
    "ScenarioControlDeclaration",
    "ScheduleCompletionId",
    "SemanticRowMaterializationDeclaration",
    "SemanticUnitDeclaration",
    "SensitivityBindingDeclaration",
    "SensitivityBindingRole",
    "ShareConversionDeclaration",
    "decode_model_kind_capabilities",
]
