"""Scenario identity validation and whole-case judgment-to-compiler projection."""

from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Final, Mapping, cast

from .build_model_product_contracts import ModelSpec
from .build_findings import make_finding
from .finding_vocabulary import Finding
from .control_snapshot_projection import (
    CanonicalProjectionCurve,
    CanonicalProjectionScenario,
    canonical_scenario_projection_owner,
    canonical_scenario_projection_owner_or_none,
)
from .driver_resolver import resolve_driver_key
from .model_kind_capabilities import ScenarioControlDeclaration
from .model_kind_dispatch import (
    SCENARIO_CONTROL_NAMESPACE_UNDECLARED,
    ModelKindCapabilityError,
    undeclared_legacy_selectors,
)
from .models import ScenarioInputs

if TYPE_CHECKING:
    from .business_model_compiler import CompiledDriverRegistry
    from .models import FinancialModel


class ScenarioBindingValidationError(ValueError):
    """A scenario references stale or inconsistent authored model controls."""

    code = "scenario_binding_validation_failed"


class ScenarioControlNamespaceUndeclared(
    ScenarioBindingValidationError, ModelKindCapabilityError
):
    """An artifact's control selectors live in an undeclared legacy namespace.

    Plan §3 row A9 / §6: the declared replacement for the issuer-string gate
    this validator used to carry. It derives from both roots on purpose — from
    :class:`ScenarioBindingValidationError` so every caller that already maps a
    binding refusal onto a typed, fail-closed rejection keeps doing so, and
    from :class:`ModelKindCapabilityError` so the capability vocabulary has one
    root. The class is *defined* here, beside the parent whose boundary it must
    stay inside, rather than in ``model_kind_dispatch``; that module knows
    nothing about scenario binding and importing it back would be a cycle.
    """

    code = SCENARIO_CONTROL_NAMESPACE_UNDECLARED

    def __init__(
        self,
        *,
        selectors: tuple[str, ...],
        declared_namespace: str | None,
    ) -> None:
        self.selectors = selectors
        self.declared_namespace = declared_namespace
        declared = (
            "this model kind declares no legacy control namespace"
            if declared_namespace is None
            else f"this model kind declares only {declared_namespace!r}"
        )
        # Both roots are ValueError; call ValueError.__init__ directly so the
        # message is built once, whichever base would otherwise win the MRO.
        self.repair = (
            "declare the namespace on this model kind's capability record, or "
            "re-author the forecast assumptions on natural control selectors"
        )
        ValueError.__init__(
            self,
            f"{self.code}: legacy control selectors {list(selectors)!r} are "
            f"outside every declared legacy namespace ({declared}) — "
            f"{self.repair}",
        )


@dataclass(frozen=True)
class ScenarioCaseGap:
    scenario: str
    reason: str


def scenario_case_gaps(model_spec: ModelSpec) -> tuple[ScenarioCaseGap, ...]:
    """Identify optional bull/bear gaps; ModelSpec owns complete base admission."""

    if not model_spec.scenarios and not model_spec.scenario_reviews:
        return ()
    scenarios = {scenario.name: scenario for scenario in model_spec.scenarios}
    reviews = {review.scenario_name: review for review in model_spec.scenario_reviews}
    base = reviews.get("base")
    base_assumption_ids = {
        override.assumption_id for override in (() if base is None else base.overrides)
    }
    forecast_periods = tuple(
        period.period_id for period in model_spec.fiscal_axis.forecast_periods
    )
    forecast_years = {
        period.fiscal_year for period in model_spec.fiscal_axis.forecast_periods
    }
    gaps: list[ScenarioCaseGap] = []
    for name in ("bull", "bear"):
        scenario = scenarios.get(name)
        review = reviews.get(name)
        reasons: list[str] = []
        if scenario is None:
            reasons.append("scenario inputs are missing")
        elif not scenario.description:
            reasons.append("scenario description is missing")
        if review is None:
            reasons.append("scenario review is missing")
        else:
            if not review.description:
                reasons.append("scenario review description is missing")
            if not review.overrides:
                reasons.append("scenario review has no overrides")
            if {
                override.assumption_id for override in review.overrides
            } != base_assumption_ids:
                reasons.append("scenario overrides do not cover the base assumption set")
            for override in review.overrides:
                if override.period_ids != forecast_periods or (
                    isinstance(override.value, dict)
                    and set(override.value) != forecast_years
                ):
                    reasons.append(
                        f"forecast axis is incomplete for {override.target_control_id}"
                    )
        if reasons:
            gaps.append(ScenarioCaseGap(name, "; ".join(reasons)))
    return tuple(gaps)


_UNRESOLVED_STAGE_INPUT_META: Final[dict[str, tuple[str, str]]] = {
    "historical_foundation": (
        "get_historical_foundation_context",
        "No accepted HistoricalFoundation is bound to this build; "
        "call get_historical_foundation_context to author one.",
    ),
    "forecast_assumption_set": (
        "get_forecast_assumptions_context",
        "No accepted ForecastAssumptionSet is bound to this build; "
        "call get_forecast_assumptions_context to author one.",
    ),
    "scenario_set": (
        "get_scenario_set_context",
        "No accepted ScenarioSet is bound to this build; "
        "call get_scenario_set_context to author one.",
    ),
    "valuation_policy": (
        "get_valuation_policy_context",
        "No accepted ValuationPolicy is bound to this build; "
        "call get_valuation_policy_context to author one.",
    ),
}


def _profile_requires_valuation(profile_id: str) -> bool:
    """Whether a checked-in model-kind profile requires valuation sections."""
    import json
    from pathlib import Path

    profile_path = Path(__file__).resolve().parent / "model_kinds" / f"{profile_id}.json"
    if not profile_path.is_file():
        return True
    try:
        payload = json.loads(profile_path.read_text(encoding="utf-8"))
    except Exception:
        return True
    return bool(payload.get("valuation_requirements"))


def _unresolved_build_input_findings(model_spec: ModelSpec) -> tuple[Finding, ...]:
    """Emit ``build_input_unresolved`` findings when an assembled stage-chain
    ``ModelSpec`` is missing a profile-required upstream stage input."""
    stamped = getattr(model_spec, "_unresolved_stage_inputs", None)
    if stamped is not None:
        missing_names = tuple(stamped)
    else:
        stage_chain = (
            model_spec.forecast_assumption_set_binding is not None
            or model_spec.historical_foundation_fact_projection is not None
            or model_spec.scenario_set_binding is not None
            or model_spec.valuation_policy_binding is not None
            or (
                not model_spec.forecast_assumptions
                and not model_spec.scenarios
                and not model_spec.scenario_reviews
                and not model_spec.valuation_controls
                and model_spec.valuation_policy is None
            )
        )
        if not stage_chain:
            return ()
        names: list[str] = []
        if not model_spec.forecast_assumptions:
            if model_spec.historical_foundation_fact_projection is None:
                names.append("historical_foundation")
            names.append("forecast_assumption_set")
        if not model_spec.scenarios and not model_spec.scenario_reviews:
            names.append("scenario_set")
        if (
            not model_spec.valuation_controls
            and model_spec.valuation_policy is None
            and _profile_requires_valuation(model_spec.profile_id)
        ):
            names.append("valuation_policy")
        missing_names = tuple(names)
    return tuple(
        make_finding(
            "build_input_unresolved",
            item_id=name,
            observed={"missing": name, "next_call": _UNRESOLVED_STAGE_INPUT_META[name][0]},
            message=_UNRESOLVED_STAGE_INPUT_META[name][1],
        )
        for name in missing_names
        if name in _UNRESOLVED_STAGE_INPUT_META
    )


def scenario_gap_findings(model_spec: ModelSpec) -> tuple[Finding, ...]:
    """Retain unresolved authored cases and absent upstream stage inputs on both compile and publication paths."""

    reviews = {review.scenario_name: review for review in model_spec.scenario_reviews}
    return (
        *_unresolved_build_input_findings(model_spec),
        *(
            make_finding(
                "build_input_unresolved",
                item_id=f"scenario:{gap.scenario}",
                operands=tuple(
                    override.target_control_id
                    for override in (
                        reviews[gap.scenario].overrides
                        if gap.scenario in reviews else ()
                    )
                ),
                observed={
                    "scenario": gap.scenario,
                    "next_call": "get_scenario_set_context",
                },
                message=f"Scenario {gap.scenario} is unresolved: {gap.reason}",
            )
            for gap in scenario_case_gaps(model_spec)
        ),
    )

def validate_scenario_bindings(
    model_spec: ModelSpec,
    *,
    scenario_controls: ScenarioControlDeclaration | None = None,
) -> None:
    """Validate scenario bindings directly inside the assembled ModelSpec.

    The server-assembled ``ModelSpec`` already owns the scenario values.  This
    check deliberately authenticates no second envelope or snapshot: it proves
    that each review names an existing forecast assumption, that the review's
    target is that assumption's driver, and that the lowered scenario inputs
    still equal those reviews.  Compilation owns the later driver-to-workbook
    resolution; this validator owns the judgment-to-scenario binding.
    """

    spec = ModelSpec.model_validate(model_spec)
    assumptions = {
        assumption.assumption_id: assumption
        for assumption in spec.forecast_assumptions
    }
    if len(assumptions) != len(spec.forecast_assumptions):
        raise ScenarioBindingValidationError(
            "forecast assumption IDs must be unique"
        )

    for review in spec.scenario_reviews:
        for override in review.overrides:
            assumption = assumptions.get(override.assumption_id)
            if assumption is None:
                raise ScenarioBindingValidationError(
                    "scenario override references a stale forecast assumption: "
                    f"{override.assumption_id}"
                )
            if assumption.driver != override.target_control_id:
                raise ScenarioBindingValidationError(
                    "scenario override target differs from its forecast "
                    f"assumption driver: {override.assumption_id}"
                )
    expected_by_scenario = {
        review.scenario_name: {
            override.target_control_id: override.value
            for override in review.overrides
        }
        for review in spec.scenario_reviews
        if review.overrides
    }
    for scenario in spec.scenarios:
        expected = expected_by_scenario.get(scenario.name)
        if expected is not None and scenario.assumptions != expected:
            raise ScenarioBindingValidationError(
                "scenario inputs are not the exact lowering of scenario reviews: "
                f"{scenario.name}"
            )
    # Plan §3 row A9: a legacy control selector is admitted only inside the
    # namespace this model kind declares.  Recognizing "legacy" is mechanism
    # vocabulary (the `.driver.` infix), so the check names no model kind and
    # a kind that declares nothing admits no legacy selector at all.  It runs
    # last, after the structural checks above, so a malformed binding still
    # reports the refusal it reported before this gate stopped naming an
    # issuer — only the selectors of a structurally sound spec reach here.
    undeclared = undeclared_legacy_selectors(
        {assumption.driver for assumption in spec.forecast_assumptions}
        | {
            override.target_control_id
            for review in spec.scenario_reviews
            for override in review.overrides
        },
        scenario_controls,
    )
    if undeclared:
        raise ScenarioControlNamespaceUndeclared(
            selectors=undeclared,
            declared_namespace=(
                None
                if scenario_controls is None
                else scenario_controls.legacy_driver_namespace
            ),
        )


def lower_model_spec_scenario_curves(
    model_spec: ModelSpec,
    *,
    workbook_targets: Mapping[str, str],
    allow_direct_targets: bool = False,
    scenario_controls: ScenarioControlDeclaration | None = None,
) -> tuple[CanonicalProjectionCurve, ...]:
    """Lower validated ModelSpec reviews into deterministic renderer curves.

    Scenario judgment is already authored, reviewed, and bound inside the
    ``ModelSpec``. Rendering therefore derives its case curves directly from
    that contract instead of requiring a second scenario-state artifact.
    ``workbook_targets`` is the compiler-owned control-to-template binding.
    """

    spec = ModelSpec.model_validate(model_spec)
    validate_scenario_bindings(spec, scenario_controls=scenario_controls)
    assumptions = {
        assumption.assumption_id: assumption
        for assumption in spec.forecast_assumptions
    }
    unresolved_cases = {gap.scenario for gap in scenario_case_gaps(spec)}
    curves: list[CanonicalProjectionCurve] = []
    for review in spec.scenario_reviews:
        if review.scenario_name not in {"base", "bull", "bear"}:
            raise ScenarioBindingValidationError(
                "renderer scenario names must be base, bull, or bear: "
                f"{review.scenario_name}"
            )
        if review.scenario_name in unresolved_cases:
            continue
        scenario = cast(CanonicalProjectionScenario, review.scenario_name)
        for override in review.overrides:
            target_item_id = workbook_targets.get(override.target_control_id)
            if target_item_id is None:
                if allow_direct_targets:
                    continue
                raise ScenarioBindingValidationError(
                    "scenario control has no compiler workbook target: "
                    f"{override.target_control_id}"
                )
            assumption = assumptions[override.assumption_id]
            if isinstance(override.value, dict):
                values = tuple(
                    sorted(
                        (int(year), float(value))
                        for year, value in override.value.items()
                    )
                )
            else:
                values = tuple(
                    (
                        int(period_id.removeprefix("FY").removesuffix("E")),
                        float(override.value),
                    )
                    for period_id in override.period_ids
                )
            curves.append(
                CanonicalProjectionCurve(
                    domain="scenario",
                    driver_id=None,
                    control_id=override.target_control_id,
                    binding_id=f"model_spec:{override.assumption_id}",
                    template_item_id=target_item_id,
                    scenario=scenario,
                    values=values,
                    rationale=override.rationale,
                    confidence=assumption.confidence or "medium",
                    held_at_base=assumption.held_at_base,
                    disclosed_guidance_horizon=None,
                    extension_basis_by_period=tuple(
                        (
                            year,
                            assumption.extension_basis_by_period.get(year),
                        )
                        for year, _value in values
                    ),
                )
            )
    return tuple(
        sorted(
            curves,
            key=lambda curve: (
                curve.template_item_id.encode("utf-8"),
                curve.domain,
                curve.scenario.encode("utf-8"),
                curve.control_id,
            ),
        )
    )


def resolve_model_spec_scenario_workbook_targets(
    model_spec: ModelSpec,
    *,
    model: "FinancialModel",
    compiled_registry: "CompiledDriverRegistry | None",
) -> dict[str, str]:
    """Resolve only authored scenario selectors through compiler-owned bindings.

    New artifact-backed ModelSpecs carry natural forecast selectors rather than
    a declared legacy control namespace.  Resolve those selectors only after
    the exact BusinessModel has been compiled and its durable driver plan bound;
    profile-owned controls continue through the canonical driver mapping.
    """

    spec = ModelSpec.model_validate(model_spec)
    selectors = {
        override.target_control_id
        for review in spec.scenario_reviews
        for override in review.overrides
    }
    return {
        selector: canonical_scenario_projection_owner(
            model,
            resolve_driver_key(
                selector,
                compiled_registry=compiled_registry,
            ),
        )
        for selector in sorted(selectors)
    }


def partition_model_spec_scenario_workbook_targets(
    model_spec: ModelSpec,
    *,
    model: "FinancialModel",
    compiled_registry: "CompiledDriverRegistry | None",
) -> tuple[dict[str, str], dict[str, str]]:
    """Partition scenario controls into table-rendered and direct inputs."""

    spec = ModelSpec.model_validate(model_spec)
    selectors = {
        override.target_control_id
        for review in spec.scenario_reviews
        for override in review.overrides
    }
    canonical: dict[str, str] = {}
    direct: dict[str, str] = {}
    for selector in sorted(selectors):
        target = resolve_driver_key(
            selector,
            compiled_registry=compiled_registry,
        )
        owner = canonical_scenario_projection_owner_or_none(model, target)
        if owner is None:
            direct[selector] = target
        else:
            canonical[selector] = owner
    if set(canonical) & set(direct) or set(canonical) | set(direct) != selectors:
        raise ScenarioBindingValidationError(
            "scenario compiler target partition is not an exact selector bijection"
        )
    return canonical, direct


def lower_model_spec_direct_scenario_inputs(
    model_spec: ModelSpec,
    *,
    direct_targets: Mapping[str, str],
) -> tuple[ScenarioInputs, ...]:
    """Lower non-table scenario controls to exact compiler item identities."""

    spec = ModelSpec.model_validate(model_spec)
    unresolved_cases = {gap.scenario for gap in scenario_case_gaps(spec)}
    lowered: list[ScenarioInputs] = []
    for scenario in spec.scenarios:
        if scenario.name in unresolved_cases:
            continue
        assumptions: dict[str, object] = {}
        for selector, value in scenario.assumptions.items():
            target = direct_targets.get(selector)
            if target is None:
                continue
            previous = assumptions.get(target)
            if previous is not None and previous != value:
                raise ScenarioBindingValidationError(
                    "direct scenario controls collide on one compiler target: "
                    f"{target}"
                )
            assumptions[target] = value
        lowered.append(
            scenario.model_copy(update={"assumptions": assumptions})
        )
    return tuple(lowered)


__all__ = [
    "ScenarioBindingValidationError",
    "ScenarioCaseGap",
    "scenario_case_gaps",
    "scenario_gap_findings",
    "lower_model_spec_scenario_curves",
    "lower_model_spec_direct_scenario_inputs",
    "partition_model_spec_scenario_workbook_targets",
    "resolve_model_spec_scenario_workbook_targets",
    "ScenarioControlNamespaceUndeclared",
    "validate_scenario_bindings",
]
