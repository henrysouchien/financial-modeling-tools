"""Artifact → compiler-input projections: the accepted artifacts as ModelSpec rows.

Engine (B). Deterministic, schema-only projections that
``api/research/model_spec_assembly.py`` (F) binds and assembles into one
ModelSpec:

- artifact → authoring rows: ``project_forecast_assumptions`` from a
  ForecastAssumptionSet, ``project_valuation_policy`` from a ValuationPolicy;
- authoring rows → ModelSpec rows with natural-key IDs:
  ``mint_normalization_decisions``, ``mint_forecast_assumptions``,
  ``mint_scenario_reviews``, ``mint_valuation_policy`` (``mint_model_spec_id``
  is the one ID codec; ModelSpec content hashes are persisted, so its preimage
  never changes);
- artifact → ModelSpec binding rows: ``project_forecast_assumption_set_binding``,
  ``project_scenario_set_binding``, ``project_valuation_policy_binding``;
- SourceBundle → declared gaps: ``derive_declared_gaps``.

Every rejection here is a ``ModelSpecProjectionError`` carrying the code, field,
offending value and repair text; the assembly re-raises it as its own bounded
wire error at the call site.
"""

from __future__ import annotations

import hashlib
from typing import Any, Callable, Iterable, Mapping

from .analysis import ExitEarningsMeasure
from .build_model_product_contracts import (
    BuildDiagnostic,
    HistoricalNormalizationDecision,
    ModelSpecForecastAssumptionSetBinding,
    ModelSpecScenarioCaseProbabilities,
    ModelSpecScenarioCriticalFactor,
    ModelSpecScenarioDriverRole,
    ModelSpecScenarioSetBinding,
    ModelSpecValuationPolicyBinding,
    ModelValuationPolicy,
    ScenarioOverride,
    ScenarioReview,
    SourceBundle,
    ValuationNetShareDilutionRates,
    ValuationNetShareDilutionYearRate,
)
from .forecast_assumption_set import (
    ForecastAssumptionSet,
    ForecastAssumptionSetRef,
    driver_coverage_closed,
)
from .historical_foundation import HistoricalFoundationRef
from .model_kind_dispatch import declared_capabilities, declared_forecast_evidence
from .model_spec_authoring import (
    ForecastAssumptionAuthoring,
    HistoricalNormalizationDecisionAuthoring,
    ModelValuationPolicyAuthoring,
    ScenarioReviewAuthoring,
    ValuationControlAuthoring,
)
from .models import ScenarioInputs
from .scenario_set import ScenarioSetArtifact
from .valuation_policy import (
    ValuationCaseValues,
    ValuationPolicyArtifact,
    ValuationPolicyProjection,
)
from .thesis_shared_slice import Assumption
from .versioned_artifact import canonical_json_bytes, revalidated


class ModelSpecProjectionError(ValueError):
    """Typed projection rejection; the assembly re-raises it as its wire error."""

    def __init__(
        self,
        code: str,
        *,
        field: str,
        got: object,
        fix: str,
        example: str | None,
    ) -> None:
        self.code = code
        self.field = field
        self.got = got
        self.fix = fix
        self.example = example
        super().__init__(
            f"{code}: field={field}; got={got!r}; fix={fix}; example={example}"
        )


def project_forecast_assumptions(
    value: ForecastAssumptionSet,
) -> tuple[ForecastAssumptionAuthoring, ...]:
    """Project one accepted, receipt-bound set into ModelSpec authoring rows."""

    record = revalidated(value)
    if record.status != "accepted" or record.stage_receipt.status != "accepted":
        raise ValueError("only accepted ForecastAssumptionSets may be projected")
    if record.stage_receipt.forecast_assumption_set_ref != record.ref:
        raise ValueError("forecast projection requires an exact bound stage receipt")
    if (
        record.forecast_control_profile_ref is None
        or record.forecast_control_denominator_sha256 is None
    ):
        raise ValueError("forecast projection requires forecast-control identity")
    drivers = tuple(row.assumption.driver for row in record.driver_assumptions)
    if not driver_coverage_closed(
        drivers,
        required=record.required_driver_keys,
        optional=record.optional_driver_keys,
    ):
        raise ValueError("forecast projection requires the exact driver denominator")
    return tuple(row.assumption for row in record.driver_assumptions)


def project_valuation_policy(
    value: ValuationPolicyArtifact,
) -> ValuationPolicyProjection:
    """Project one accepted policy without manufacturing WACC or a PriceTarget."""

    record = revalidated(value)
    if record.status != "accepted" or record.stage_receipt.status != "accepted":
        raise ValueError("only accepted ValuationPolicies may be projected")
    assert record.model_policy is not None
    assert record.terminal_policy is not None
    observed = {row.input_name: row for row in record.observed_inputs}
    policy = {row.input_name: row for row in record.policy_inputs}
    source_refs = tuple(sorted({row.source_id for row in record.observed_inputs}))
    executable_relative_methods = tuple(
        method.model_copy(update={"rationale": None, "evidence_ids": ()})
        if method.selection_basis == "none"
        else method
        for method in record.model_policy.relative_methods
    )
    executable_model_policy = record.model_policy.model_copy(
        update={
            "relative_methods": executable_relative_methods,
            "terminal_evidence_ids": record.terminal_policy.evidence_ids,
            "beta_floor_evidence_ids": policy["beta_floor"].evidence_ids,
        }
    )
    control = ValuationControlAuthoring(
        method="dcf",
        sofr_rate=float(observed["sofr_rate"].value),
        beta_floor=float(policy["beta_floor"].value),
        terminal_growth_rate=float(record.terminal_policy.terminal_growth.base),
        terminal_multiple=float(record.terminal_policy.exit_multiple.base),
        rationale=record.terminal_policy.rationale,
        source_refs=source_refs,
    )
    return ValuationPolicyProjection(
        valuation_policy_ref=record.ref,
        valuation_controls=(control,),
        model_policy=executable_model_policy,
        family_authorizations=record.family_authorizations,
        terminal_growth_cases=record.terminal_policy.terminal_growth,
        exit_multiple_cases=record.terminal_policy.exit_multiple,
        net_share_dilution=_net_share_dilution_rates(record),
        exit_earnings_measure=record.terminal_policy.exit_earnings_measure,
    )


def _net_share_dilution_rates(
    record: ValuationPolicyArtifact,
) -> ValuationNetShareDilutionRates | None:
    """The policy's net-dilution rates keyed by the fiscal year they land on.

    A rate whose period the policy's fiscal axis no longer forecasts (a
    rebound policy on a shifted axis) lands nowhere; that year compiles the
    house value as ``default``.
    """

    if record.net_share_dilution is None:
        return None
    year_by_period = {
        period.period_id: period.fiscal_year
        for period in record.fiscal_axis.forecast_periods
    }
    rates = tuple(
        ValuationNetShareDilutionYearRate(
            fiscal_year=year_by_period[row.period_id],
            rate=row.rate,
        )
        for row in record.net_share_dilution.rates
        if row.period_id in year_by_period
    )
    if not rates:
        return None
    return ValuationNetShareDilutionRates(
        rates=tuple(sorted(rates, key=lambda row: row.fiscal_year)),
        rationale=record.net_share_dilution.rationale,
        evidence_ids=record.net_share_dilution.evidence_ids,
    )


def mint_model_spec_id(section: str, natural_key: object) -> str:
    """Mint the persisted ModelSpec row ID for one section natural key."""

    preimage = {
        "codec": "model_spec_2_2_natural_key_v1",
        "section": section,
        "natural_key": natural_key,
    }
    digest = hashlib.sha256(canonical_json_bytes(preimage)).hexdigest()[:32]
    prefix = {
        "statement_rows": "msrow",
        "historical_normalization_decisions": "msnorm",
        "forecast_assumptions": "msassumption",
        "sensitivities": "mssensitivity",
        "declinations": "msdecline",
    }.get(section, "msartifact")
    return f"{prefix}_{digest}"


def derived_natural_key(natural_key: object, occurrence: int) -> object:
    """The persisted key: the natural key, or that key plus its later occurrence."""

    return natural_key if occurrence == 0 else (natural_key, occurrence)


def natural_key_registry(
    rows: Iterable[Any],
    *,
    key: Callable[[Any], Any],
    field: str,
) -> dict[Any, Any]:
    """Index every row. A repeated natural key keeps a derived key, not a refusal."""

    del field
    result: dict[Any, Any] = {}
    counts: dict[Any, int] = {}
    for row in rows:
        natural_key = key(row)
        occurrence = counts.get(natural_key, 0)
        counts[natural_key] = occurrence + 1
        result[derived_natural_key(natural_key, occurrence)] = row
    return result


def mint_normalization_decisions(
    rows: tuple[HistoricalNormalizationDecisionAuthoring, ...],
) -> tuple[HistoricalNormalizationDecision, ...]:
    labels = natural_key_registry(
        rows,
        key=lambda row: row.label,
        field="historical_normalization_decisions.label",
    )
    return tuple(
        HistoricalNormalizationDecision.model_validate(
            {
                **row.model_dump(mode="python", exclude={"label"}),
                "decision_id": mint_model_spec_id(
                    "historical_normalization_decisions", label
                ),
            }
        )
        for label, row in labels.items()
    )


def mint_forecast_assumptions(
    rows: tuple[ForecastAssumptionAuthoring, ...],
) -> tuple[Assumption, ...]:
    by_driver = natural_key_registry(
        rows, key=lambda row: row.driver, field="forecast_assumptions.driver"
    )
    return tuple(
        Assumption.model_validate(
            {
                **row.model_dump(mode="python"),
                "assumption_id": mint_model_spec_id("forecast_assumptions", driver),
            }
        )
        for driver, row in by_driver.items()
    )


def mint_scenario_reviews(
    rows: tuple[ScenarioReviewAuthoring, ...],
    assumptions: tuple[Assumption, ...],
) -> tuple[tuple[ScenarioInputs, ...], tuple[ScenarioReview, ...]]:
    by_name = natural_key_registry(
        rows, key=lambda row: row.scenario_name, field="scenario_reviews.scenario_name"
    )
    assumption_by_driver = {row.driver: row for row in assumptions}
    reviews: list[ScenarioReview] = []
    scenarios: list[ScenarioInputs] = []
    for _derived_name, row in by_name.items():
        scenario_name = row.scenario_name
        overrides: list[ScenarioOverride] = []
        for override in row.overrides:
            assumption = assumption_by_driver.get(override.driver)
            if assumption is None:
                raise ModelSpecProjectionError(
                    "cross_section_reference_invalid",
                    field=f"scenario_reviews.{scenario_name}.overrides.driver",
                    got=override.driver,
                    fix="select a driver authored in forecast_assumptions in this draft snapshot",
                    example=next(
                        iter(sorted(assumption_by_driver)),
                        "no forecast assumption driver is available",
                    ),
                )
            target_control_id = assumption.driver
            overrides.append(
                ScenarioOverride.model_validate(
                    {
                        **override.model_dump(mode="python", exclude={"driver"}),
                        "assumption_id": assumption.assumption_id,
                        "target_control_id": target_control_id,
                    }
                )
            )
        typed_overrides = tuple(overrides)
        reviews.append(
            ScenarioReview(
                scenario_name=scenario_name,
                description=row.description,
                overrides=typed_overrides,
            )
        )
        scenarios.append(
            ScenarioInputs(
                name=scenario_name,
                description=row.description,
                assumptions={
                    override.target_control_id: override.value
                    for override in typed_overrides
                },
            )
        )
    return tuple(scenarios), tuple(reviews)


def mint_valuation_policy(
    dto: ModelValuationPolicyAuthoring | None,
    *,
    assumption_id_by_driver: Mapping[str, str],
    terminal_growth_cases: ValuationCaseValues | None = None,
    exit_multiple_cases: ValuationCaseValues | None = None,
    net_share_dilution: ValuationNetShareDilutionRates | None = None,
    exit_earnings_measure: ExitEarningsMeasure = "terminal_fcf",
) -> ModelValuationPolicy | None:
    if dto is None:
        return None
    payload = dto.model_dump(mode="python")
    sensitivities: list[dict[str, Any]] = []
    counts: dict[tuple[str, str], int] = {}
    for row in dto.sensitivities:
        natural_key = (row.row_axis.control_id, row.column_axis.control_id)
        occurrence = counts.get(natural_key, 0)
        counts[natural_key] = occurrence + 1
        derived = derived_natural_key(natural_key, occurrence)
        sensitivity = row.model_dump(mode="python")
        for axis_name in ("row_axis", "column_axis"):
            axis = sensitivity[axis_name]
            selector = axis["control_id"]
            axis["control_id"] = assumption_id_by_driver.get(selector, selector)
        sensitivity["sensitivity_id"] = mint_model_spec_id("sensitivities", derived)
        sensitivities.append(sensitivity)
    payload["sensitivities"] = tuple(sensitivities)
    if terminal_growth_cases is not None:
        payload["terminal_growth_cases"] = terminal_growth_cases.model_dump(
            mode="python",
            exclude={"schema_version"},
        )
    if exit_multiple_cases is not None:
        payload["exit_multiple_cases"] = exit_multiple_cases.model_dump(
            mode="python",
            exclude={"schema_version"},
        )
    if net_share_dilution is not None:
        payload["net_share_dilution"] = net_share_dilution
    if exit_earnings_measure != "terminal_fcf":
        payload["exit_earnings_measure"] = exit_earnings_measure
    return ModelValuationPolicy.model_validate(payload)


def project_forecast_assumption_set_binding(
    forecast_assumption_set_ref: ForecastAssumptionSetRef,
    historical_foundation_ref: HistoricalFoundationRef,
) -> ModelSpecForecastAssumptionSetBinding:
    """Bind the ModelSpec to one exact FAS and the HFF it pins."""

    return ModelSpecForecastAssumptionSetBinding(
        forecast_assumption_set_id=forecast_assumption_set_ref.set_id,
        forecast_assumption_set_content_sha256=(
            forecast_assumption_set_ref.content_sha256
        ),
        historical_foundation_artifact_id=historical_foundation_ref.artifact_id,
        historical_foundation_content_sha256=(
            historical_foundation_ref.content_sha256
        ),
    )


def project_scenario_set_binding(
    record: ScenarioSetArtifact,
) -> ModelSpecScenarioSetBinding:
    """Bind the ModelSpec to one accepted ScenarioSet and its upstream refs.

    ``case_probabilities`` is a projection of the bound ScenarioSet's case
    weights (pinned by its ref and content digest) into compiler input; it is
    written only here.
    """

    return ModelSpecScenarioSetBinding(
        scenario_set_id=record.ref.set_id,
        scenario_set_content_sha256=record.ref.content_sha256,
        scenario_stage_receipt_digest=record.stage_receipt.receipt_digest,
        forecast_assumption_set_id=record.forecast_assumption_set_ref.set_id,
        forecast_assumption_set_content_sha256=(
            record.forecast_assumption_set_ref.content_sha256
        ),
        critical_factors_artifact_id=record.critical_factors_ref.artifact_id,
        critical_factors_content_sha256=(
            record.critical_factors_ref.content_sha256
        ),
        case_probabilities=(
            None
            if record.case_probabilities is None
            else ModelSpecScenarioCaseProbabilities.model_validate(
                record.case_probabilities.model_dump(
                    mode="python", exclude={"schema_version"}
                )
            )
        ),
        critical_factors=tuple(
            ModelSpecScenarioCriticalFactor(
                factor_key=factor.factor_key,
                driver_key=factor.driver_key,
                evidence_ids=factor.evidence_ids,
            )
            for factor in record.critical_factors
        ),
        driver_roles=tuple(
            ModelSpecScenarioDriverRole(
                driver_key=curve.driver_key, role=curve.role
            )
            for curve in record.driver_curves
        ),
    )


def project_valuation_policy_binding(
    record: ValuationPolicyArtifact,
) -> ModelSpecValuationPolicyBinding:
    """Bind the ModelSpec to one accepted ValuationPolicy and its upstream refs."""

    return ModelSpecValuationPolicyBinding(
        valuation_policy_id=record.ref.policy_id,
        valuation_policy_content_sha256=record.ref.content_sha256,
        valuation_policy_stage_receipt_digest=(
            record.stage_receipt.receipt_digest
        ),
        forecast_assumption_set_id=record.forecast_assumption_set_ref.set_id,
        forecast_assumption_set_content_sha256=(
            record.forecast_assumption_set_ref.content_sha256
        ),
        scenario_set_id=record.scenario_set_ref.set_id,
        scenario_set_content_sha256=record.scenario_set_ref.content_sha256,
    )


def derive_declared_gaps(source_bundle: SourceBundle) -> tuple[BuildDiagnostic, ...]:
    # A14 (issuer-generic plan §3): a model kind that discharges its forward
    # evidence from the accepted assumption set carries its own requirement
    # namespace, declared rather than derived from a profile id — a later
    # profile release may deliberately retain a predecessor's namespace. The
    # raw per-requirement diagnostics that discharge replaces are dropped here.
    forecast_evidence = declared_forecast_evidence(
        declared_capabilities(source_bundle)
    )
    artifact_backed_prefix = (
        forecast_evidence.requirement_id_prefix()
        if forecast_evidence is not None
        and forecast_evidence.mode == "artifact_backed"
        else None
    )
    diagnostics = tuple(
        diagnostic
        for diagnostic in source_bundle.diagnostics
        if diagnostic.code != "source_absence_evidenced"
        and not (
            artifact_backed_prefix is not None
            and str(diagnostic.semantic_item_id or "").startswith(
                artifact_backed_prefix
            )
        )
    )
    if len(diagnostics) <= 32:
        return diagnostics
    return (
        BuildDiagnostic(
            code="missing_source",
            message=(
                f"{len(diagnostics)} profile coverage shortfalls remain; "
                "inspect the SourceBundle evidence audit for exact periods."
            ),
            semantic_item_id=f"{source_bundle.coverage_profile.profile_id}.coverage",
        ),
    )


__all__ = [
    "ModelSpecProjectionError",
    "derive_declared_gaps",
    "mint_forecast_assumptions",
    "mint_model_spec_id",
    "mint_normalization_decisions",
    "mint_scenario_reviews",
    "mint_valuation_policy",
    "natural_key_registry",
    "project_forecast_assumption_set_binding",
    "project_forecast_assumptions",
    "project_scenario_set_binding",
    "project_valuation_policy",
    "project_valuation_policy_binding",
]
