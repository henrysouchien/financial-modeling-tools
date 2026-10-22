from __future__ import annotations

import hashlib
import json
import re
from dataclasses import replace as dataclass_replace
from typing import TYPE_CHECKING, Any, Literal, Mapping, Protocol, runtime_checkable

from pydantic import Field, field_validator, model_validator

if TYPE_CHECKING:
    from .business_model import DriverAssumptionPlan

from .overrides import TickerOverrides, replace_ticker_overrides
from .thesis_shared_slice import SourceRecord, _ContractModel
from .versioned_artifact import canonical_json_bytes


ScenarioName = Literal["base", "bull", "bear"]
NonBaseScenarioName = Literal["bull", "bear"]
ValuationMethod = Literal["dcf", "relative", "multiple", "mixed", "manual"]
ProjectionValueScale = Literal["display", "model"]
_YEAR_RE = re.compile(r"^\d{4}$")


class ScenarioProvenance(_ContractModel):
    source_skill: str
    skill_run_id: str
    writer_policy_id: str = Field(min_length=1)
    last_writer_id: str = Field(min_length=1)
    written_at: str
    model_build_id: str | None = None


class ScenarioEntry(_ContractModel):
    """Typed forecast/scenario intent independent of durable override storage."""

    values: dict[str, float]
    value_scale: ProjectionValueScale = "display"
    rationale: str
    confidence: Literal["low", "medium", "high"]
    held_at_base: bool = False
    disclosed_guidance_horizon: int | None = None
    extension_basis_by_period: dict[str, str | None] | None = None
    source_refs_by_period: dict[str, list[str]] | None = None
    sources: list[SourceRecord] = Field(default_factory=list)
    provenance: ScenarioProvenance

    @field_validator("values")
    @classmethod
    def _validate_year_keys(cls, value: dict[str, float]) -> dict[str, float]:
        for key in value:
            if _YEAR_RE.fullmatch(key) is None:
                raise ValueError(f"year key must be 4-digit FY, got {key!r}")
        return value

    @model_validator(mode="after")
    def _validate_period_consistency(self) -> "ScenarioEntry":
        value_years = set(self.values)
        if self.source_refs_by_period:
            extra = set(self.source_refs_by_period) - value_years
            if extra:
                raise ValueError(
                    f"source_refs_by_period has years not in values: {sorted(extra)}"
                )
        if self.extension_basis_by_period:
            extra = set(self.extension_basis_by_period) - value_years
            if extra:
                raise ValueError(
                    f"extension_basis_by_period has years not in values: {sorted(extra)}"
                )
        if self.source_refs_by_period:
            source_ids = {source.id for source in self.sources}
            for year, refs in self.source_refs_by_period.items():
                missing = set(refs) - source_ids
                if missing:
                    raise ValueError(
                        f"source_refs_by_period[{year}] references unknown source IDs: "
                        f"{sorted(missing)} (sources block has "
                        f"{sorted(source_ids) if source_ids else '[]'})"
                    )
        return self


@runtime_checkable
class _HasModelDump(Protocol):
    def model_dump(self, *, mode: str) -> object: ...


def canonical_workbook_semantics_hash(
    *,
    ticker: str,
    control_curves: tuple[Any, ...],
    valuation: Any | None,
) -> str:
    """Hash the one canonical control/valuation input projection for rendering."""

    curve_keys = tuple(
        (
            curve.template_item_id.encode("utf-8"),
            curve.domain,
            curve.scenario.encode("utf-8"),
            curve.control_id,
        )
        for curve in control_curves
    )
    if curve_keys != tuple(sorted(set(curve_keys))):
        raise ValueError("control_curves must be canonically sorted and unique")
    if valuation is None:
        valuation_payload = None
    elif isinstance(valuation, _HasModelDump):
        valuation_payload = valuation.model_dump(mode="json")
    else:
        valuation_payload = valuation
    payload = {
        "schema_version": "canonical-workbook-semantics.v2",
        "ticker": str(ticker).strip().upper(),
        "control_curves": [curve.to_dict() for curve in control_curves],
        "valuation": valuation_payload or None,
    }
    return hashlib.sha256(
        b"canonical-workbook-semantics.v2\0" + canonical_json_bytes(payload)
    ).hexdigest()


class ForecastArtifactEntry(_ContractModel):
    driver_id: str = Field(pattern=r"^drv_[0-9a-f]{32}$")
    control_id: str = Field(pattern=r"^ctl_[0-9a-f]{32}$")
    binding_id: str = Field(pattern=r"^bnd_[0-9a-f]{32}$")
    item_id: str = Field(min_length=1)
    base: ScenarioEntry


class ForecastArtifact(_ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    entries: list[ForecastArtifactEntry] = Field(default_factory=list)

    @property
    def by_driver_id(self) -> dict[str, ForecastArtifactEntry]:
        return {entry.driver_id: entry for entry in self.entries}

    @property
    def by_item_id(self) -> dict[str, ForecastArtifactEntry]:
        return {entry.item_id: entry for entry in self.entries}


class ScenarioArtifactEntry(_ContractModel):
    driver_id: str = Field(pattern=r"^drv_[0-9a-f]{32}$")
    control_id: str = Field(pattern=r"^ctl_[0-9a-f]{32}$")
    binding_id: str = Field(pattern=r"^bnd_[0-9a-f]{32}$")
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
    def by_driver_id(self) -> dict[str, ScenarioArtifactEntry]:
        return {entry.driver_id: entry for entry in self.entries}

    @property
    def by_item_id(self) -> dict[str, ScenarioArtifactEntry]:
        return {entry.item_id: entry for entry in self.entries}


class ModelSemanticsForecastPlanIssue(_ContractModel):
    field: str = Field(min_length=1)
    reason: str = Field(min_length=1)
    driver_id: str | None = None
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


class ScenarioControlFamily(_ContractModel):
    """One complete bull/base/bear family of durable valuation controls."""

    bull: float = Field(strict=True, allow_inf_nan=False)
    base: float = Field(strict=True, allow_inf_nan=False)
    bear: float = Field(strict=True, allow_inf_nan=False)


class ScenarioValuationControls(_ContractModel):
    """Durable scenario controls owned by the pricing and DCF writers."""

    selected_pe: ScenarioControlFamily | None = None
    dcf_terminal_growth: ScenarioControlFamily | None = None
    dcf_exit_multiple: ScenarioControlFamily | None = None

    @model_validator(mode="after")
    def _require_authored_family(self) -> "ScenarioValuationControls":
        if all(
            family is None
            for family in (
                self.selected_pe,
                self.dcf_terminal_growth,
                self.dcf_exit_multiple,
            )
        ):
            raise ValueError("scenario controls must include at least one complete family")
        return self


class ValuationArtifact(_ContractModel):
    schema_version: Literal["1.0"] = "1.0"
    method: ValuationMethod | None = None
    wacc: ValuationWaccInputs = Field(default_factory=ValuationWaccInputs)
    terminal_growth_rate: ValuationInputValue | None = None
    exit_multiple: ValuationInputValue | None = None
    scenario_controls: ScenarioValuationControls | None = None
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
        valuation_payload = getattr(overrides, "valuation", None) or None
        valuation = ValuationArtifact.model_validate(valuation_payload) if valuation_payload else None
        return cls(
            ticker=str(getattr(overrides, "ticker", "") or "").upper(),
            forecast=ForecastArtifact(),
            scenarios=ScenarioSet(),
            valuation=valuation,
        )

    def model_semantics_hash(self) -> str:
        payload = self.model_dump(mode="json")
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
        return hashlib.sha256(encoded).hexdigest()


def apply_model_semantics(overrides: Any, semantics: ModelSemantics) -> Any:
    if semantics.forecast.entries or semantics.scenarios.entries:
        raise ValueError(
            "forecast and scenario semantics must be written through canonical "
            "stable-ID control snapshots"
        )

    valuation = semantics.valuation.model_dump(mode="json") if semantics.valuation is not None else getattr(overrides, "valuation", {})
    if isinstance(overrides, TickerOverrides):
        updated = replace_ticker_overrides(
            overrides,
            valuation=valuation,
        )
    else:
        updated = dataclass_replace(
            overrides,
            valuation=valuation,
        )
    ModelSemantics.from_ticker_overrides(updated)
    return updated


def model_semantics_forecast_plan_issues(
    semantics: ModelSemantics,
    driver_assumption_plan: DriverAssumptionPlan | None,
    *,
    units_by_driver_id: Mapping[str, str] | None = None,
    behaviors_by_driver_id: Mapping[str, str] | None = None,
) -> list[ModelSemanticsForecastPlanIssue]:
    """Return forecast entries that do not conform to the BusinessModel driver plan."""

    if driver_assumption_plan is None:
        return []

    unit_lookup = _normalized_string_map(units_by_driver_id or {})
    behavior_lookup = _normalized_string_map(behaviors_by_driver_id or {})
    issues: list[ModelSemanticsForecastPlanIssue] = []
    for index, entry in enumerate(semantics.forecast.entries):
        field = f"forecast.entries[{index}]"
        plan_entry = driver_assumption_plan.entry_for_driver_id(entry.driver_id)
        if plan_entry is None:
            issues.append(
                ModelSemanticsForecastPlanIssue(
                    field=f"{field}.driver_id",
                    reason="forecast driver_id is absent from the accepted stable plan",
                    driver_id=entry.driver_id,
                    item_id=entry.item_id,
                    got={"driver_id": entry.driver_id},
                    fix="use an exact driver_id from the accepted stable plan",
                    example={"driver_id": entry.driver_id},
                )
            )
            continue

        expected_binding = {
            "driver_id": plan_entry.driver_id,
            "control_id": plan_entry.control_id,
            "binding_id": plan_entry.binding_id,
            "item_id": plan_entry.template_item_id,
        }
        actual_binding = {
            "driver_id": entry.driver_id,
            "control_id": entry.control_id,
            "binding_id": entry.binding_id,
            "item_id": entry.item_id,
        }
        if actual_binding != expected_binding:
            issues.append(
                ModelSemanticsForecastPlanIssue(
                    field=f"{field}.binding_id",
                    reason="forecast entry must equal one exact stable compile binding",
                    driver_id=entry.driver_id,
                    item_id=entry.item_id,
                    got=actual_binding,
                    expected=expected_binding,
                    fix="copy the complete stable identity triplet and item target",
                    example=expected_binding,
                )
            )
            continue

        if not bool(getattr(plan_entry, "base_case_required", False)):
            issues.append(
                ModelSemanticsForecastPlanIssue(
                    field=f"{field}.driver_id",
                    reason="DriverAssumptionPlan entry is not eligible for base-case forecast semantics",
                    driver_id=entry.driver_id,
                    item_id=entry.item_id,
                    got={
                        "driver_id": entry.driver_id,
                        "compile_target_type": getattr(plan_entry, "compile_target_type", None),
                    },
                    expected={"base_case_required": True},
                    fix="write forecast semantics only for BusinessModel drivers compiled to assumption or existing rows",
                    example={"driver_id": entry.driver_id, "base_case_required": True},
                )
            )
            continue

        submitted_unit = next(
            (
                unit_lookup[key]
                for key in (
                    entry.driver_id,
                    entry.control_id,
                    entry.binding_id,
                    entry.item_id,
                )
                if key in unit_lookup
            ),
            None,
        )
        if submitted_unit:
            accepted_units = _accepted_units_for_plan_entry(
                plan_entry,
            )
            if submitted_unit not in accepted_units:
                issues.append(
                    ModelSemanticsForecastPlanIssue(
                        field=f"{field}.unit",
                        reason="forecast unit must match the resolved BusinessModel DriverAssumptionPlan entry",
                        driver_id=entry.driver_id,
                        item_id=entry.item_id,
                        got={
                            "unit": submitted_unit,
                            "driver_id": entry.driver_id,
                            "accepted_units": sorted(accepted_units),
                            "plan_unit": _unit_value(getattr(plan_entry, "unit", None)),
                        },
                        expected={"unit": sorted(accepted_units)},
                        fix="use the control unit bound by the exact stable compile binding",
                        example={"unit": sorted(accepted_units)[0] if accepted_units else _unit_value(getattr(plan_entry, "unit", None))},
                    )
                )

        submitted_behavior = next(
            (
                behavior_lookup[key]
                for key in (
                    entry.driver_id,
                    entry.control_id,
                    entry.binding_id,
                    entry.item_id,
                )
                if key in behavior_lookup
            ),
            None,
        )
        expected_behavior = _behavior_value(getattr(plan_entry, "behavior", None))
        if submitted_behavior and expected_behavior and submitted_behavior != expected_behavior:
            issues.append(
                ModelSemanticsForecastPlanIssue(
                    field=f"{field}.behavior",
                    reason="forecast behavior must match the resolved BusinessModel DriverAssumptionPlan entry",
                    driver_id=entry.driver_id,
                    item_id=entry.item_id,
                    got={
                        "behavior": submitted_behavior,
                        "driver_id": entry.driver_id,
                        "plan_behavior": expected_behavior,
                    },
                    expected={"behavior": expected_behavior},
                    fix="use the behavior implied by the accepted BusinessModel driver, or omit behavior when the writer has no behavior metadata",
                    example={"behavior": expected_behavior},
                )
            )

    return issues


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


def _normalized_string_map(values: Mapping[str, str]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in values.items():
        key_text = str(key or "").strip()
        value_text = str(value or "").strip()
        if key_text and value_text:
            normalized[key_text] = value_text
    return normalized


def _accepted_units_for_plan_entry(plan_entry: Any) -> set[str]:
    unit = _unit_value(getattr(plan_entry, "control_unit", None))
    return {unit} if unit else set()


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
    "ScenarioControlFamily",
    "ScenarioEntry",
    "ScenarioName",
    "ScenarioProvenance",
    "ScenarioSet",
    "ScenarioValuationControls",
    "ValuationArtifact",
    "ValuationInputValue",
    "ValuationMethod",
    "ValuationWaccInputs",
    "apply_model_semantics",
    "canonical_workbook_semantics_hash",
    "model_semantics_forecast_plan_issues",
]
