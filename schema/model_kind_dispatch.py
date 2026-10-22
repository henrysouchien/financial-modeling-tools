"""Capability dispatch — the one place the compile reads a model kind's record.

``model_kind_capabilities.py`` owns the declaration record; this module is its
reader. Compile phases dispatch on what that record declares instead of on who
the issuer is. See ``schema/README.md`` and
``docs/reference/build-model-infrastructure-architecture-2026-08-04.md``.

Three rules, taken verbatim from the plan and enforced here rather than
restated at each call site:

* **Dispatch on the declaration, never on identity** (design principle P2).
  Every accessor below takes the SourceBundle — a declared compile input —
  and returns a declaration or the generic default. No mechanism module may
  reach for a profile id, a ticker, or any issuer string to decide behavior.
* **Absence is feature-off** (P4). A bundle whose coverage profile carries no
  capability record is a legal, typed state: every accessor returns the
  generic arm, and nothing model-kind-shaped activates. That is why the
  accessors are total and why none of them raises on absence.
* **An artifact that demands an undeclared capability is refused** (plan §6).
  Absence is feature-off only for the *compile*; when a ModelSpec or FAS
  artifact carries content that only a declared capability can lower,
  :class:`CapabilityUndeclaredForArtifact` names the capability, the artifact
  and the repair lane. Fail-closed, never a silent fallback to the behavior
  the deleted issuer branch used to supply.

The module names no issuer: it is scanned by
``tests/test_issuer_generic_grep_guard.py`` from birth (plan §11, "every new
module this plan lands carries zero issuer names").
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Final, Mapping

from .model_kind_capabilities import (
    LEGACY_CONTROL_NAMESPACE_INFIX,
    MECHANISM_OWNED_NAMESPACES,
    CompilerOriginKind,
    EpsPublicationBasis,
    ForecastEvidenceDeclaration,
    ModelKindCapabilities,
    ModelSpecControlBinding,
    ProvisionalValuationDefaultDeclaration,
    ScenarioControlDeclaration,
    ScheduleCompletionId,
    SemanticRowMaterializationDeclaration,
    SensitivityBindingDeclaration,
    ShareConversionDeclaration,
    decode_model_kind_capabilities,
)
from .models import ModelScale
from industry_slice_core.industry_types import Unit

#: Plan §6 codes raised by this lane.
CAPABILITY_UNDECLARED_FOR_ARTIFACT: Final = "capability_undeclared_for_artifact"
SCENARIO_CONTROL_NAMESPACE_UNDECLARED: Final = "scenario_control_namespace_undeclared"


class ModelKindCapabilityError(ValueError):
    """Root of the capability-dispatch refusal vocabulary (plan §6).

    Derived from ``ValueError`` deliberately, for the reason IG-3 recorded for
    the registry refusals (plan §14 item 13(g)): every call site that already
    converts a compile refusal into a typed, fail-closed rejection catches
    ``ValueError``. A refusal outside that hierarchy would turn a rejection
    into an unhandled fault — a softening disguised as a stronger type. The
    stable code and the named repair target ride on the exception either way.
    """

    code: str = "model_kind_capability_refused"

    def __init__(self, message: str, *, repair: str) -> None:
        self.repair = repair
        super().__init__(f"{self.code}: {message} — {repair}")


class CapabilityUndeclaredForArtifact(ModelKindCapabilityError):
    """An artifact demands a capability this model kind does not declare."""

    code = CAPABILITY_UNDECLARED_FOR_ARTIFACT

    def __init__(self, *, capability: str, artifact: str, detail: str) -> None:
        self.capability = capability
        self.artifact = artifact
        self.detail = detail
        super().__init__(
            f"{artifact} demands the undeclared capability {capability!r}: {detail}",
            repair=(
                f"declare {capability!r} on this model kind's capability record, "
                "or re-author the artifact without it"
            ),
        )


def declared_capabilities(source_bundle: Any) -> ModelKindCapabilities | None:
    """The model kind's capability record, or ``None`` when it declares none.

    Reads the record off the compile's declared inputs — never off a profile
    id. ``None`` is the legal absent state, not an error (P4).
    """

    coverage_profile = getattr(source_bundle, "coverage_profile", None)
    if coverage_profile is None:
        return None
    capabilities = getattr(coverage_profile, "capabilities", None)
    if capabilities is None:
        return None
    return decode_model_kind_capabilities(capabilities)


def declared_scenario_controls(
    capabilities: ModelKindCapabilities | None,
) -> ScenarioControlDeclaration | None:
    """The declared scenario control plane, or ``None`` (feature-off)."""

    return None if capabilities is None else capabilities.scenario_control_curves


def declared_share_conversion(
    capabilities: ModelKindCapabilities | None,
) -> ShareConversionDeclaration | None:
    """The declared share-conversion basis, or ``None`` (feature-off)."""

    return None if capabilities is None else capabilities.share_conversion


def declared_forecast_evidence(
    capabilities: ModelKindCapabilities | None,
) -> ForecastEvidenceDeclaration | None:
    """The declared forecast-evidence mode, or ``None`` (neither arm runs)."""

    return None if capabilities is None else capabilities.forecast_evidence


def declared_schedule_completions(
    capabilities: ModelKindCapabilities | None,
) -> frozenset[ScheduleCompletionId]:
    """The schedule completions this model kind's compile runs (possibly none)."""

    if capabilities is None:
        return frozenset()
    return frozenset(capabilities.schedule_completions)


def declared_control_binding(
    capabilities: ModelKindCapabilities | None,
) -> ModelSpecControlBinding:
    """Which control-binding pass runs; the generic arm when undeclared."""

    if capabilities is None:
        return "unit_bound"
    return capabilities.model_spec_control_binding


def declared_eps_publication_basis(
    capabilities: ModelKindCapabilities | None,
) -> EpsPublicationBasis:
    """Which scenario output key publishes per-share earnings."""

    if capabilities is None:
        return "adj_eps"
    return capabilities.eps_publication_basis


def declared_provisional_valuation_defaults(
    capabilities: ModelKindCapabilities | None,
) -> tuple[ProvisionalValuationDefaultDeclaration, ...]:
    """The provisional valuation anchors this model kind seeds (possibly none)."""

    if capabilities is None:
        return ()
    return capabilities.provisional_valuation_defaults


@dataclass(frozen=True)
class DeclaredSemanticUnitPlane:
    """The model kind's semantic rows and the unit contract they must satisfy.

    ``units`` are the declared ``(unit, scale)`` pairs the projected-integrity
    checker enforces beside the template's own rows. ``required_ids`` are the
    semantic rows this model kind declares *somewhere* — a control target, a
    grounded schedule row, a minted control row — that live outside the
    mechanism-owned namespaces and therefore carry no template unit contract of
    their own. One of those present in a compiled model with no declared unit
    row is the ``semantic_unit_undeclared`` state of plan §6: the baked table's
    coverage would have caught it, so the declaration must too.
    """

    units: Mapping[str, tuple[Unit, ModelScale]] = field(default_factory=dict)
    required_ids: frozenset[str] = frozenset()
    model_kind: str = "<undeclared model kind>"


def declared_semantic_unit_plane(
    source_bundle: Any,
) -> DeclaredSemanticUnitPlane:
    """The declared semantic-unit plane read off the compile's declared inputs.

    Takes the SourceBundle rather than the record, because the refusal payload
    plan §6 requires names the model kind, and the profile is where that label
    lives. Absence is feature-off: an empty plane enforces the template rows
    alone, exactly as the checker did before any model kind declared anything.
    """

    capabilities = declared_capabilities(source_bundle)
    if capabilities is None:
        return DeclaredSemanticUnitPlane()
    coverage_profile = getattr(source_bundle, "coverage_profile", None)
    model_kind = str(getattr(coverage_profile, "profile_id", "") or "").strip()
    declared_ids: set[str] = set()
    controls = capabilities.scenario_control_curves
    if controls is not None:
        declared_ids.update(controls.workbook_target_map().values())
        declared_ids.update(controls.forecast_control_target_map().values())
    declared_ids.update(capabilities.grounded_semantic_row_map().values())
    declared_ids.update(
        row.item_id for row in capabilities.semantic_row_materializations
    )
    return DeclaredSemanticUnitPlane(
        units=dict(capabilities.semantic_unit_map()),
        required_ids=frozenset(
            item_id
            for item_id in declared_ids
            if not item_id.startswith(MECHANISM_OWNED_NAMESPACES)
        ),
        model_kind=model_kind or "<undeclared model kind>",
    )


def declared_grounded_semantic_rows(
    capabilities: ModelKindCapabilities | None,
) -> Mapping[str, str]:
    """Declared grounded-schedule semantic role -> workbook item id (row S5)."""

    if capabilities is None:
        return {}
    return capabilities.grounded_semantic_row_map()


def declared_semantic_row_materializations(
    capabilities: ModelKindCapabilities | None,
) -> tuple[SemanticRowMaterializationDeclaration, ...]:
    """The semantic control rows this model kind's compile mints (possibly none)."""

    if capabilities is None:
        return ()
    return capabilities.semantic_row_materializations


def declared_compiler_origins(
    capabilities: ModelKindCapabilities | None,
) -> Mapping[str, CompilerOriginKind]:
    """Declared workbook item id -> compiler origin class (row S6)."""

    if capabilities is None:
        return {}
    return capabilities.compiler_origin_map()


def declared_sensitivity_bindings(
    capabilities: ModelKindCapabilities | None,
) -> Mapping[str, SensitivityBindingDeclaration]:
    """Declared sensitivity selector id -> its resolution arm."""

    if capabilities is None:
        return {}
    return capabilities.sensitivity_binding_map()


def declared_style_guide(
    capabilities: ModelKindCapabilities | None,
) -> str | None:
    """The style guide this model kind's render resolves, or ``None`` (row S7)."""

    return None if capabilities is None else capabilities.style_guide


def resolve_sensitivity_binding(
    binding: SensitivityBindingDeclaration | None,
    *,
    node_items: Mapping[str, str],
    driver_keys: Mapping[str, str],
) -> str | None:
    """Resolve one declared sensitivity selector onto a compiled workbook row.

    First match wins on the ordered arms; an arm that resolves to nothing is a
    legal absent state (the authored grid simply has no row to perturb), not a
    refusal — which is what the deleted hand maps did.
    """

    if binding is None:
        return None
    if binding.item_id:
        return binding.item_id
    for path in binding.node_item_paths:
        resolved = node_items.get(path)
        if resolved is not None:
            return resolved
    for path in binding.driver_key_paths:
        resolved = driver_keys.get(path)
        if resolved is not None:
            return resolved
    return None


def is_legacy_control_selector(selector: str) -> bool:
    """Whether a selector belongs to a *legacy control namespace*.

    Mechanism vocabulary, not an issuer's: a legacy control id is shaped
    ``<namespace>.driver.<name>``. Natural selectors — bare mechanism control
    keys and compiled business-model node paths — never carry the infix, so
    the test discriminates the two planes without naming any model kind.
    """

    return LEGACY_CONTROL_NAMESPACE_INFIX in f".{selector}"


def undeclared_legacy_selectors(
    selectors: Iterable[str],
    scenario_controls: ScenarioControlDeclaration | None,
) -> tuple[str, ...]:
    """Legacy selectors that fall outside this model kind's declared namespace.

    Returns them sorted, so a refusal names an exact, stable set.
    """

    namespace = (
        None if scenario_controls is None else scenario_controls.legacy_driver_namespace
    )
    return tuple(
        sorted(
            selector
            for selector in selectors
            if is_legacy_control_selector(selector)
            and not (namespace is not None and selector.startswith(namespace))
        )
    )


__all__ = [
    "CAPABILITY_UNDECLARED_FOR_ARTIFACT",
    "SCENARIO_CONTROL_NAMESPACE_UNDECLARED",
    "CapabilityUndeclaredForArtifact",
    "DeclaredSemanticUnitPlane",
    "ModelKindCapabilityError",
    "declared_capabilities",
    "declared_compiler_origins",
    "declared_control_binding",
    "declared_eps_publication_basis",
    "declared_forecast_evidence",
    "declared_grounded_semantic_rows",
    "declared_provisional_valuation_defaults",
    "declared_scenario_controls",
    "declared_schedule_completions",
    "declared_semantic_row_materializations",
    "declared_semantic_unit_plane",
    "declared_sensitivity_bindings",
    "declared_share_conversion",
    "declared_style_guide",
    "is_legacy_control_selector",
    "resolve_sensitivity_binding",
    "undeclared_legacy_selectors",
]
