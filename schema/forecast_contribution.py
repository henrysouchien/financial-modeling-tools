"""Fail-closed forecast contribution contracts.

R0 computes contribution status over the shipped driver-plan/horizon
denominator.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import hashlib
import json
import re
from typing import Annotated, Any, Iterable, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, model_validator

from schema.control_snapshot_projection import CanonicalProjectionCurve
from schema.stable_control_identity import DriverId, require_identity_kind


Sha256Hex = Annotated[str, Field(pattern=r"^[0-9a-f]{64}$")]


class ForecastContributionStatus(str, Enum):
    PATCH_APPLIED = "patch_applied"
    COMPLETE = "complete"
    NO_WRITE_INCOMPLETE = "no_write_incomplete"
    BLOCKED = "blocked"


class ForecastEvidenceRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    request_id: str = Field(min_length=1)
    tool: str = Field(min_length=1)
    args: dict[str, Any]
    purpose: str = Field(min_length=1)
    driver_ids: tuple[DriverId, ...] = ()
    data_gap_ids: tuple[str, ...] = ()

    @model_validator(mode="after")
    def _validate_sorted_identity_sets(self) -> "ForecastEvidenceRequest":
        for field_name in ("driver_ids", "data_gap_ids"):
            values = getattr(self, field_name)
            if tuple(sorted(set(values))) != values:
                raise ValueError(f"{field_name} must be sorted and unique")
        return self


class ForecastGapRoute(BaseModel):
    """Platform-owned continuation for one unresolved gap class."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    gap_kind: str
    destination: str
    driver_ids: tuple[DriverId, ...] = ()
    reason: str = Field(min_length=1)

    @model_validator(mode="after")
    def _validate_closed_route(self) -> "ForecastGapRoute":
        expected = {
            "evidence": "forecast_session",
            "binding": "model_engine",
            "structural": "business_model_construction",
        }
        if self.gap_kind not in expected:
            raise ValueError("unsupported forecast gap kind")
        if self.destination != expected[self.gap_kind]:
            raise ValueError("forecast gap destination does not match its kind")
        if tuple(sorted(set(self.driver_ids))) != self.driver_ids:
            raise ValueError("gap-route driver_ids must be sorted and unique")
        return self


class R0ForecastContribution(BaseModel):
    """Operational R0 contribution over the durable driver-plan denominator."""

    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["r0-forecast-contribution.v1"] = (
        "r0-forecast-contribution.v1"
    )
    denominator_kind: Literal["driver-plan-horizon.v1"] = "driver-plan-horizon.v1"
    status: ForecastContributionStatus
    render_state_hash: Sha256Hex | None = None
    driver_assumption_plan_hash: Sha256Hex | None = None
    required_driver_ids: tuple[DriverId, ...]
    required_periods: tuple[str, ...]
    covered_driver_ids: tuple[DriverId, ...]
    covered_periods_by_driver_id: dict[DriverId, tuple[str, ...]]
    unresolved_required_driver_ids: tuple[DriverId, ...]
    mutated_driver_ids: tuple[DriverId, ...] = ()
    pending_evidence_requests: tuple[ForecastEvidenceRequest, ...] = ()
    gap_routes: tuple[ForecastGapRoute, ...] = ()
    blocker_codes: tuple[str, ...] = ()
    mutation_receipt_id: Sha256Hex | None = None

    @model_validator(mode="after")
    def _validate_r0_contribution(self) -> "R0ForecastContribution":
        required = set(self.required_driver_ids)
        covered = set(self.covered_driver_ids)
        unresolved = set(self.unresolved_required_driver_ids)
        _require_sorted_unique(self.required_driver_ids, "required_driver_ids")
        _require_sorted_unique(self.required_periods, "required_periods")
        _require_sorted_unique(self.covered_driver_ids, "covered_driver_ids")
        _require_sorted_unique(
            self.unresolved_required_driver_ids, "unresolved_required_driver_ids"
        )
        _require_sorted_unique(self.mutated_driver_ids, "mutated_driver_ids")
        _require_sorted_unique(self.blocker_codes, "blocker_codes")
        if not set(self.mutated_driver_ids) <= required:
            raise ValueError(
                "mutated_driver_ids may contain only required denominator drivers"
            )
        if set(self.covered_periods_by_driver_id) != required:
            raise ValueError(
                "covered_periods_by_driver_id must exactly enumerate required drivers"
            )
        horizon = set(self.required_periods)
        for driver_id, periods in self.covered_periods_by_driver_id.items():
            _require_sorted_unique(periods, f"covered periods for {driver_id!r}")
            if not set(periods) <= horizon:
                raise ValueError(
                    "covered periods must be contained in the required horizon"
                )
        derived_covered = {
            driver_id
            for driver_id, periods in self.covered_periods_by_driver_id.items()
            if horizon and set(periods) == horizon
        }
        if covered != derived_covered:
            raise ValueError("covered_driver_ids is not derived from the full horizon")
        if covered | unresolved != required or covered & unresolved:
            raise ValueError(
                "covered and unresolved drivers must partition the required denominator"
            )
        request_ids = tuple(
            request.request_id for request in self.pending_evidence_requests
        )
        _require_sorted_unique(request_ids, "pending_evidence_requests")
        for request in self.pending_evidence_requests:
            if request.driver_ids and not set(request.driver_ids) <= unresolved:
                raise ValueError("evidence requests may name only unresolved drivers")
        route_keys = tuple(
            (route.gap_kind, route.destination, route.driver_ids)
            for route in self.gap_routes
        )
        if tuple(sorted(set(route_keys))) != route_keys:
            raise ValueError("gap_routes must be sorted and unique")
        for route in self.gap_routes:
            if route.driver_ids and not set(route.driver_ids) <= unresolved:
                raise ValueError("gap routes may name only unresolved drivers")

        if self.status is ForecastContributionStatus.BLOCKED:
            if not self.blocker_codes:
                raise ValueError("blocked contribution requires blocker_codes")
            if self.mutation_receipt_id is not None or self.mutated_driver_ids:
                raise ValueError("blocked contribution cannot claim a mutation")
            return self

        if not required or not horizon:
            raise ValueError(
                "non-blocked contribution requires a non-empty denominator"
            )
        if self.render_state_hash is None or self.driver_assumption_plan_hash is None:
            raise ValueError("non-blocked contribution requires exact R0 state hashes")
        if self.blocker_codes:
            raise ValueError("non-blocked contribution cannot retain blocker_codes")
        if self.status is ForecastContributionStatus.COMPLETE:
            if unresolved or self.pending_evidence_requests or self.gap_routes:
                raise ValueError("complete contribution cannot retain unresolved work")
            if self.mutation_receipt_id is None and self.mutated_driver_ids:
                raise ValueError("mutated complete contribution requires a receipt")
        elif self.status is ForecastContributionStatus.PATCH_APPLIED:
            if not unresolved or self.mutation_receipt_id is None:
                raise ValueError(
                    "patch_applied requires unresolved drivers and a receipt"
                )
            if not self.mutated_driver_ids:
                raise ValueError("patch_applied requires a non-empty useful mutation")
        elif self.status is ForecastContributionStatus.NO_WRITE_INCOMPLETE:
            if not unresolved or self.mutation_receipt_id is not None:
                raise ValueError(
                    "no_write_incomplete requires unresolved drivers and no receipt"
                )
            if self.mutated_driver_ids:
                raise ValueError("no_write_incomplete cannot claim mutated drivers")
        return self


@dataclass(frozen=True)
class DurableDriverPlanCoverage:
    """Pure durable coverage projection shared by readiness and FMS."""

    required_driver_ids: tuple[DriverId, ...]
    required_periods: tuple[str, ...]
    covered_periods_by_driver_id: Mapping[DriverId, tuple[str, ...]]
    accepted_driver_ids: frozenset[DriverId]


def durable_driver_plan_coverage(
    *,
    driver_assumption_plan: Any,
    canonical_control_curves: tuple[CanonicalProjectionCurve, ...],
    projection_periods: Iterable[int | str],
) -> DurableDriverPlanCoverage:
    """Derive R0 coverage from exact canonical forecast-control curves."""

    periods = tuple(sorted({_period_text(period) for period in projection_periods}))

    entries = list(_field(driver_assumption_plan, "entries", ()) or ())
    entries_by_driver_id: dict[str, tuple[str, str, str]] = {}
    required: set[str] = set()
    for entry in entries:
        driver_id = require_identity_kind(_field(entry, "driver_id"), "driver")
        control_id = require_identity_kind(_field(entry, "control_id"), "control")
        binding_id = require_identity_kind(_field(entry, "binding_id"), "binding")
        template_item_id = _field(entry, "template_item_id")
        if not template_item_id.strip():
            raise ValueError("driver plan entry requires a non-empty template_item_id")
        if driver_id in entries_by_driver_id:
            raise ValueError("driver plan contains duplicate stable driver_id")
        entries_by_driver_id[driver_id] = (
            control_id,
            binding_id,
            template_item_id,
        )
        if bool(_field(entry, "base_case_required", False)):
            required.add(driver_id)

    periods_by_driver_id: dict[str, set[str]] = {}
    for curve in canonical_control_curves:
        if curve.domain != "forecast" or curve.scenario != "base":
            raise ValueError(
                "forecast coverage accepts only canonical forecast/base curves"
            )
        driver_id = require_identity_kind(curve.driver_id, "driver")  # Validator owns input narrowing.
        control_id = require_identity_kind(curve.control_id, "control")
        binding_id = require_identity_kind(curve.binding_id, "binding")
        expected = entries_by_driver_id.get(driver_id)
        if expected is None:
            raise ValueError(
                "canonical forecast curve driver_id is absent from the accepted plan"
            )
        if (control_id, binding_id, curve.template_item_id) != expected:
            raise ValueError(
                "canonical forecast curve does not match the plan's exact "
                "control_id/binding_id/template_item_id"
            )
        if driver_id in periods_by_driver_id:
            raise ValueError(
                "canonical forecast curves contain duplicate stable driver_id"
            )
        periods_by_driver_id[driver_id] = {
            _period_text(fiscal_year) for fiscal_year, _value in curve.values
        }

    horizon = set(periods)
    required_ids = tuple(sorted(required))
    return DurableDriverPlanCoverage(
        required_driver_ids=required_ids,
        required_periods=periods,
        covered_periods_by_driver_id={
            driver_id: tuple(
                sorted(horizon & periods_by_driver_id.get(driver_id, set()))
            )
            for driver_id in required_ids
        },
        accepted_driver_ids=frozenset(entries_by_driver_id),
    )


def build_r0_forecast_contribution(
    *,
    render_state_hash: str | None,
    driver_assumption_plan_hash: str | None,
    coverage: DurableDriverPlanCoverage,
    mutated_driver_ids: Iterable[str] = (),
    pending_evidence_requests: Iterable[
        ForecastEvidenceRequest | Mapping[str, Any]
    ] = (),
    gap_routes: Iterable[ForecastGapRoute | Mapping[str, Any]] = (),
    blocker_codes: Iterable[str] = (),
    mutation_receipt_id: str | None = None,
) -> R0ForecastContribution:
    required = coverage.required_driver_ids
    horizon = set(coverage.required_periods)
    covered = tuple(
        key
        for key in required
        if horizon
        and set(coverage.covered_periods_by_driver_id.get(key, ())) == horizon
    )
    unresolved = tuple(key for key in required if key not in set(covered))
    blockers = tuple(sorted(set(blocker_codes)))
    requests = tuple(
        sorted(
            (
                value
                if isinstance(value, ForecastEvidenceRequest)
                else ForecastEvidenceRequest.model_validate(value)
                for value in pending_evidence_requests
            ),
            key=lambda value: value.request_id,
        )
    )
    routes = tuple(
        sorted(
            (
                value
                if isinstance(value, ForecastGapRoute)
                else ForecastGapRoute.model_validate(value)
                for value in gap_routes
            ),
            key=lambda value: (
                value.gap_kind,
                value.destination,
                value.driver_ids,
            ),
        )
    )
    mutated = tuple(sorted(set(mutated_driver_ids)))
    if blockers or not required or not coverage.required_periods:
        status = ForecastContributionStatus.BLOCKED
        mutation_receipt_id = None
        mutated = ()
        if not blockers:
            blockers = ("forecast_denominator_unavailable",)
    elif not unresolved:
        status = ForecastContributionStatus.COMPLETE
    elif mutation_receipt_id is not None and mutated:
        status = ForecastContributionStatus.PATCH_APPLIED
    else:
        status = ForecastContributionStatus.NO_WRITE_INCOMPLETE
    return R0ForecastContribution(
        status=status,
        render_state_hash=render_state_hash,
        driver_assumption_plan_hash=driver_assumption_plan_hash,
        required_driver_ids=required,
        required_periods=coverage.required_periods,
        covered_driver_ids=covered,
        covered_periods_by_driver_id=dict(
            coverage.covered_periods_by_driver_id
        ),
        unresolved_required_driver_ids=unresolved,
        mutated_driver_ids=mutated,
        pending_evidence_requests=requests,
        gap_routes=routes,
        blocker_codes=blockers,
        mutation_receipt_id=mutation_receipt_id,
    )


def canonical_sha256(value: Any) -> str:
    payload = json.dumps(
        _json_value(value),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def _require_sorted_unique(values: tuple[Any, ...], field_name: str) -> None:
    if tuple(sorted(set(values))) != values:
        raise ValueError(f"{field_name} must be sorted and unique")


def _field(value: Any, name: str, default: Any = None) -> Any:
    if isinstance(value, Mapping):
        return value.get(name, default)
    return getattr(value, name, default)


def _period_text(value: int | str) -> str:
    text = str(value).strip()
    if re.fullmatch(r"\d{4}", text) is None:
        raise ValueError(
            f"forecast period must be a four-digit fiscal year, got {value!r}"
        )
    return text


def _json_value(value: Any) -> Any:
    dumper = getattr(value, "model_dump", None)
    if callable(dumper):
        return dumper(mode="json")
    if isinstance(value, Mapping):
        return {str(key): _json_value(child) for key, child in value.items()}
    if isinstance(value, (list, tuple)):
        return [_json_value(child) for child in value]
    return value


__all__ = [
    "DurableDriverPlanCoverage",
    "ForecastContributionStatus",
    "ForecastEvidenceRequest",
    "ForecastGapRoute",
    "R0ForecastContribution",
    "build_r0_forecast_contribution",
    "canonical_sha256",
    "durable_driver_plan_coverage",
]
