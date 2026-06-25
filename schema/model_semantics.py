from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from typing import Any, Literal, Mapping

from pydantic import Field, ValidationError, model_validator

from .overrides_projections import ProjectionEntry, ScenarioEntry
from .thesis_shared_slice import _ContractModel


ScenarioName = Literal["base", "bull", "bear"]
NonBaseScenarioName = Literal["bull", "bear"]
ValuationMethod = Literal["dcf", "relative", "multiple", "mixed", "manual"]


class ForecastArtifactEntry(_ContractModel):
    driver_key: str = Field(min_length=1)
    item_id: str = Field(min_length=1)
    base: ScenarioEntry


class ForecastArtifact(_ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    entries: list[ForecastArtifactEntry] = Field(default_factory=list)

    @property
    def by_driver_key(self) -> dict[str, ForecastArtifactEntry]:
        return {entry.driver_key: entry for entry in self.entries}


class ScenarioArtifactEntry(_ContractModel):
    driver_key: str = Field(min_length=1)
    item_id: str = Field(min_length=1)
    scenarios: dict[NonBaseScenarioName, ScenarioEntry] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _require_scenarios(self) -> "ScenarioArtifactEntry":
        if not self.scenarios:
            raise ValueError("scenario artifact entry must include at least one non-base scenario")
        return self


class ScenarioSet(_ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    entries: list[ScenarioArtifactEntry] = Field(default_factory=list)

    @property
    def by_driver_key(self) -> dict[str, ScenarioArtifactEntry]:
        return {entry.driver_key: entry for entry in self.entries}


class ModelSemanticsForecastPlanIssue(_ContractModel):
    field: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    driver_key: str | None = None
    item_id: str | None = None
    got: Any = None
    expected: dict[str, Any] = Field(default_factory=dict)
    fix: str = Field(min_length=1)
    example: dict[str, Any] = Field(default_factory=dict)


class ValuationInputValue(_ContractModel):
    value_decimal: float
    item_id: str | None = None
    source: str | None = None
    rationale: str | None = None
    as_of_date: str | None = None


class ValuationWaccInputs(_ContractModel):
    risk_free_rate: ValuationInputValue | None = None
    sofr_rate: ValuationInputValue | None = None
    credit_spread: ValuationInputValue | None = None
    equity_risk_premium: ValuationInputValue | None = None
    beta_floor: ValuationInputValue | None = None


class ValuationArtifact(_ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    method: ValuationMethod | None = None
    wacc: ValuationWaccInputs = Field(default_factory=ValuationWaccInputs)
    terminal_growth_rate: ValuationInputValue | None = None
    exit_multiple: ValuationInputValue | None = None
    sensitivity_setup: dict[str, Any] = Field(default_factory=dict)
    references: dict[str, str | None] = Field(default_factory=dict)

    @model_validator(mode="after")
    def _reject_computed_outputs(self) -> "ValuationArtifact":
        forbidden = {"price_target", "model_insights", "computed_outputs", "valuation_outputs"}
        payload = self.model_dump(mode="json")
        present = _forbidden_valuation_output_paths(payload, forbidden)
        if present:
            raise ValueError(
                "valuation artifact stores inputs/control only; computed outputs stay in PriceTarget/ModelInsights"
            )
        return self


class ModelSemantics(_ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    ticker: str
    forecast: ForecastArtifact = Field(default_factory=ForecastArtifact)
    scenarios: ScenarioSet = Field(default_factory=ScenarioSet)
    valuation: ValuationArtifact | None = None

    @classmethod
    def from_ticker_overrides(cls, overrides: Any) -> "ModelSemantics":
        forecast_entries: list[ForecastArtifactEntry] = []
        scenario_entries: list[ScenarioArtifactEntry] = []
        for item_id, value in sorted((getattr(overrides, "projections", {}) or {}).items()):
            if not isinstance(item_id, str) or not item_id.strip():
                continue
            try:
                projection = ProjectionEntry.model_validate(value)
            except ValidationError:
                continue
            scenarios = projection.scenarios
            if "base" in scenarios:
                forecast_entries.append(
                    ForecastArtifactEntry(
                        driver_key=item_id,
                        item_id=item_id,
                        base=scenarios["base"],
                    )
                )
            non_base = {
                name: scenario
                for name, scenario in scenarios.items()
                if name in {"bull", "bear"}
            }
            if non_base:
                scenario_entries.append(
                    ScenarioArtifactEntry(
                        driver_key=item_id,
                        item_id=item_id,
                        scenarios=non_base,
                    )
                )

        valuation_payload = getattr(overrides, "valuation", None) or None
        valuation = ValuationArtifact.model_validate(valuation_payload) if valuation_payload else None
        return cls(
            ticker=str(getattr(overrides, "ticker", "") or "").upper(),
            forecast=ForecastArtifact(entries=forecast_entries),
            scenarios=ScenarioSet(entries=scenario_entries),
            valuation=valuation,
        )

    def model_semantics_hash(self) -> str:
        payload = self.model_dump(mode="json")
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


def apply_model_semantics(overrides: Any, semantics: ModelSemantics) -> Any:
    projections = dict(getattr(overrides, "projections", {}) or {})
    for entry in semantics.forecast.entries:
        scenarios = _existing_projection_scenarios(projections.get(entry.item_id))
        scenarios["base"] = entry.base
        projections[entry.item_id] = ProjectionEntry(scenarios=scenarios).model_dump(mode="json")

    for entry in semantics.scenarios.entries:
        scenarios = _existing_projection_scenarios(projections.get(entry.item_id))
        for scenario_name, scenario in entry.scenarios.items():
            scenarios[scenario_name] = scenario
        projections[entry.item_id] = ProjectionEntry(scenarios=scenarios).model_dump(mode="json")

    valuation = semantics.valuation.model_dump(mode="json") if semantics.valuation is not None else getattr(overrides, "valuation", {})
    updated = replace(overrides, projections=projections, valuation=valuation)
    ModelSemantics.from_ticker_overrides(updated)
    return updated


def model_semantics_forecast_plan_issues(
    semantics: ModelSemantics,
    driver_assumption_plan: Any | None,
    *,
    units_by_driver_key: Mapping[str, str] | None = None,
    behaviors_by_driver_key: Mapping[str, str] | None = None,
) -> list[ModelSemanticsForecastPlanIssue]:
    """Return forecast entries that do not conform to the BusinessModel driver plan."""

    if driver_assumption_plan is None:
        return []

    unit_lookup = _normalized_string_map(units_by_driver_key or {})
    behavior_lookup = _normalized_string_map(behaviors_by_driver_key or {})
    issues: list[ModelSemanticsForecastPlanIssue] = []
    for index, entry in enumerate(semantics.forecast.entries):
        field = f"forecast.entries[{index}]"
        resolved: list[tuple[str, str, str]] = []
        unresolved: list[dict[str, str]] = []
        for suffix, value in (
            ("driver_key", entry.driver_key),
            ("item_id", entry.item_id),
        ):
            text = str(value or "").strip()
            if not text:
                continue
            canonical = _resolve_plan_driver_key(driver_assumption_plan, text)
            if canonical is None:
                unresolved.append({"field": f"{field}.{suffix}", "value": text})
            else:
                resolved.append((suffix, text, canonical))

        if not resolved:
            issues.append(
                ModelSemanticsForecastPlanIssue(
                    field=f"{field}.driver_key",
                    reason="forecast driver key must resolve through the accepted BusinessModel DriverAssumptionPlan",
                    driver_key=entry.driver_key,
                    item_id=entry.item_id,
                    got={"unresolved": unresolved},
                    expected={"accepted_driver_keys": _accepted_plan_driver_keys(driver_assumption_plan)[:20]},
                    fix="use a canonical DriverAssumptionPlan key or alias from the accepted BusinessModel revision",
                    example={"accepted_driver_keys": _accepted_plan_driver_keys(driver_assumption_plan)[:20]},
                )
            )
            continue

        canonical = resolved[0][2]
        plan_entry = _plan_entry_for_driver_key(driver_assumption_plan, canonical)
        if plan_entry is None:
            issues.append(
                ModelSemanticsForecastPlanIssue(
                    field=f"{field}.driver_key",
                    reason="DriverAssumptionPlan resolved to a missing entry",
                    driver_key=entry.driver_key,
                    item_id=entry.item_id,
                    got={"canonical_driver_key": canonical},
                    fix="repair the accepted BusinessModel DriverAssumptionPlan before writing forecast semantics",
                    example={"driver_key": canonical},
                )
            )
            continue

        mismatches = [
            {"field": f"{field}.{suffix}", "value": value, "resolved_driver_key": candidate}
            for suffix, value, candidate in resolved
            if candidate != canonical
        ]
        if mismatches or unresolved:
            issues.append(
                ModelSemanticsForecastPlanIssue(
                    field=f"{field}.driver_key",
                    reason="all forecast driver/item keys must resolve to the same DriverAssumptionPlan entry",
                    driver_key=entry.driver_key,
                    item_id=entry.item_id,
                    got={
                        "canonical_driver_key": canonical,
                        "mismatches": mismatches,
                        "unresolved": unresolved,
                    },
                    expected={"driver_key": canonical},
                    fix="do not mix arbitrary workbook item IDs with BusinessModel driver keys; use aliases for one canonical plan entry",
                    example={"driver_key": canonical, "accepted_aliases": list(getattr(plan_entry, "aliases", []) or [])},
                )
            )
            continue

        if not bool(getattr(plan_entry, "base_case_required", False)):
            issues.append(
                ModelSemanticsForecastPlanIssue(
                    field=f"{field}.driver_key",
                    reason="DriverAssumptionPlan entry is not eligible for base-case forecast semantics",
                    driver_key=entry.driver_key,
                    item_id=entry.item_id,
                    got={
                        "driver_key": canonical,
                        "compile_target_type": getattr(plan_entry, "compile_target_type", None),
                    },
                    expected={"base_case_required": True},
                    fix="write forecast semantics only for BusinessModel drivers compiled to assumption or existing rows",
                    example={"driver_key": canonical, "base_case_required": True},
                )
            )
            continue

        submitted_unit = _unit_for_forecast_entry(
            unit_lookup,
            entry=entry,
            canonical_driver_key=canonical,
        )
        if submitted_unit:
            accepted_units = _accepted_units_for_plan_entry(
                plan_entry,
                submitted_key=_unit_driver_key_candidate(entry, resolved, plan_entry=plan_entry),
            )
            if submitted_unit not in accepted_units:
                issues.append(
                    ModelSemanticsForecastPlanIssue(
                        field=f"{field}.unit",
                        reason="forecast unit must match the resolved BusinessModel DriverAssumptionPlan entry",
                        driver_key=entry.driver_key,
                        item_id=entry.item_id,
                        got={
                            "unit": submitted_unit,
                            "driver_key": canonical,
                            "accepted_units": sorted(accepted_units),
                            "plan_unit": _unit_value(getattr(plan_entry, "unit", None)),
                        },
                        expected={"unit": sorted(accepted_units)},
                        fix="use the unit implied by the accepted BusinessModel driver or its growth-rate alias",
                        example={"unit": sorted(accepted_units)[0] if accepted_units else _unit_value(getattr(plan_entry, "unit", None))},
                    )
                )

        submitted_behavior = _behavior_for_forecast_entry(
            behavior_lookup,
            entry=entry,
            canonical_driver_key=canonical,
        )
        expected_behavior = _behavior_value(getattr(plan_entry, "behavior", None))
        if submitted_behavior and expected_behavior and submitted_behavior != expected_behavior:
            issues.append(
                ModelSemanticsForecastPlanIssue(
                    field=f"{field}.behavior",
                    reason="forecast behavior must match the resolved BusinessModel DriverAssumptionPlan entry",
                    driver_key=entry.driver_key,
                    item_id=entry.item_id,
                    got={
                        "behavior": submitted_behavior,
                        "driver_key": canonical,
                        "plan_behavior": expected_behavior,
                    },
                    expected={"behavior": expected_behavior},
                    fix="use the behavior implied by the accepted BusinessModel driver, or omit behavior when the writer has no behavior metadata",
                    example={"behavior": expected_behavior},
                )
            )

    return issues


def _existing_projection_scenarios(value: Any) -> dict[ScenarioName, ScenarioEntry]:
    if not isinstance(value, dict) or not value.get("scenarios"):
        return {}
    try:
        return dict(ProjectionEntry.model_validate(value).scenarios)
    except ValidationError:
        return {}


def _forbidden_valuation_output_paths(value: Any, forbidden: set[str], *, path: str = "valuation") -> list[str]:
    paths: list[str] = []
    if isinstance(value, dict):
        for key, child in value.items():
            child_path = f"{path}.{key}"
            if str(key) in forbidden:
                paths.append(child_path)
            paths.extend(_forbidden_valuation_output_paths(child, forbidden, path=child_path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            paths.extend(_forbidden_valuation_output_paths(child, forbidden, path=f"{path}[{index}]"))
    return paths


def _resolve_plan_driver_key(driver_assumption_plan: Any, driver_key: str) -> str | None:
    resolver = getattr(driver_assumption_plan, "resolve_driver_key", None)
    if not callable(resolver):
        return None
    resolved = resolver(driver_key)
    text = str(resolved or "").strip()
    return text or None


def _plan_entry_for_driver_key(driver_assumption_plan: Any, driver_key: str) -> Any | None:
    getter = getattr(driver_assumption_plan, "entry_for_driver_key", None)
    if not callable(getter):
        return None
    return getter(driver_key)


def _accepted_plan_driver_keys(driver_assumption_plan: Any) -> list[str]:
    accepted = getattr(driver_assumption_plan, "accepted_driver_keys", None)
    if callable(accepted):
        return [str(value) for value in accepted()]
    return []


def _normalized_string_map(values: Mapping[str, str]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in values.items():
        key_text = str(key or "").strip()
        value_text = str(value or "").strip()
        if key_text and value_text:
            normalized[key_text] = value_text
    return normalized


def _unit_for_forecast_entry(
    units_by_driver_key: Mapping[str, str],
    *,
    entry: ForecastArtifactEntry,
    canonical_driver_key: str,
) -> str | None:
    for key in (entry.driver_key, entry.item_id, canonical_driver_key):
        unit = units_by_driver_key.get(str(key or "").strip())
        if unit:
            return unit
    return None


def _behavior_for_forecast_entry(
    behaviors_by_driver_key: Mapping[str, str],
    *,
    entry: ForecastArtifactEntry,
    canonical_driver_key: str,
) -> str | None:
    for key in (entry.driver_key, entry.item_id, canonical_driver_key):
        behavior = behaviors_by_driver_key.get(str(key or "").strip())
        if behavior:
            return behavior
    return None


def _unit_driver_key_candidate(
    entry: ForecastArtifactEntry,
    resolved: list[tuple[str, str, str]],
    *,
    plan_entry: Any | None = None,
) -> str:
    canonical_entry_key = str(getattr(plan_entry, "driver_key", "") or "").strip()
    for suffix, value, _canonical in resolved:
        if suffix == "item_id" and str(value or "").strip() != canonical_entry_key:
            return value
    if plan_entry is not None:
        for _suffix, value, _canonical in resolved:
            if str(value or "").strip() != canonical_entry_key and _is_growth_rate_alias(plan_entry, value):
                return value
    return entry.item_id or entry.driver_key


def _accepted_units_for_plan_entry(plan_entry: Any, submitted_key: str) -> set[str]:
    units = {_unit_value(getattr(plan_entry, "unit", None))}
    if _is_growth_rate_alias(plan_entry, submitted_key):
        units.add("percentage")
    return {unit for unit in units if unit}


def _is_growth_rate_alias(plan_entry: Any, submitted_key: str) -> bool:
    key = str(submitted_key or "").strip()
    if not key:
        return False
    aliases = {str(value or "").strip() for value in getattr(plan_entry, "aliases", []) or []}
    if key in aliases and _looks_like_growth_rate_key(key):
        return True
    segment_id = str(getattr(plan_entry, "segment_id", "") or "").strip()
    node_id = str(getattr(plan_entry, "driver_node_id", "") or "").strip()
    if not (segment_id and node_id):
        return False
    return key.startswith(f"{segment_id}.{node_id}.") or key.startswith(f"bm.{segment_id}.{node_id}__")


def _looks_like_growth_rate_key(key: str) -> bool:
    text = str(key or "").strip().lower()
    if not text:
        return False
    suffix = text.rsplit("__", 1)[-1].rsplit(".", 1)[-1]
    return "growth" in suffix or "rate" in suffix or suffix.endswith("_pct")


def _unit_value(value: Any) -> str:
    return str(getattr(value, "value", value) or "").strip()


def _behavior_value(value: Any) -> str:
    return str(getattr(value, "value", value) or "").strip()


__all__ = [
    "ForecastArtifact",
    "ForecastArtifactEntry",
    "ModelSemantics",
    "ModelSemanticsForecastPlanIssue",
    "ScenarioArtifactEntry",
    "ScenarioName",
    "ScenarioSet",
    "ValuationArtifact",
    "ValuationInputValue",
    "ValuationMethod",
    "ValuationWaccInputs",
    "apply_model_semantics",
    "model_semantics_forecast_plan_issues",
]
