"""``update``: an Actual against the judgments that held its period.

Engine (B), pure; spec ``update : (ModelState, Fact[Actual]) -> (Variance,
Set<Judgment> to revisit)``. The prior judgments are one accepted
ForecastAssumptionSet's driver rows. The Actual is the successor
HistoricalFoundation the historical-foundation door wrote when the reported
period arrived (``actual_fact_ref``). Every forecast period of the prior set
that the successor now holds as a historical period is a boundary period: a
judgment there now faces a fact. A comparative period both foundations hold
is a boundary period too when the successor restates a basis fact or driver
seed there. Each driver row valued at a forecast-to-actual boundary period
is a judgment to revisit; a comparative restatement is reported on
``boundary_periods``, not revisited, so a scalar row (``period_values``
empty) is not evaluated there.

Its Variance (actual minus judged) is computed when the successor states the
Actual in the driver's own terms: the historical driver seed of the driver's
BusinessModel node at that period, read as the value itself for a direct
control and as growth over the node's seed one fiscal year earlier (on the
successor or on the prior set's basis foundation) for a growth rate. Any other
row is still named, with no Variance: its Actual is not stated in driver terms.

The view is a derived reading, never a Finding and never stored. Whether the
miss is noise or signal is the analyst's judgment: the owning stage door
writes the successor row, whose Supersession (trigger ``actual``) cites
``actual_fact_ref`` and records the reading. The prior row survives on the
predecessor (``schema/judgment_history.py``).
"""

from __future__ import annotations

import math

from collections.abc import Iterable, Mapping
from decimal import Decimal
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

from .forecast_assumption_set import (
    ForecastAssumptionSet,
    ForecastAssumptionSetRef,
    ForecastDriverAssumption,
)
from .historical_driver_seeds import HistoricalDriverSeed
from .historical_foundation import HistoricalFoundation, HistoricalFoundationRef


class _View(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class DriverActualReading(_View):
    """How one forecast driver reads an Actual: its node and value semantics.

    The caller derives it from the BusinessModel revision the prior set binds
    (``research.forecast_driver_value_contracts``); a driver without one (a
    profile control) has no Actual in driver terms.
    """

    node_key: str = Field(min_length=1)
    value_semantics: Literal["direct_control_value", "growth_rate"]


class JudgmentRowRef(_View):
    """One judgment row: the artifact holding it and the key a Supersession names."""

    artifact_ref: ForecastAssumptionSetRef
    row: str = Field(min_length=1)


class ActualVariance(_View):
    """The prior judgment against the Actual, in the driver's own terms."""

    judged: float
    actual: float
    #: ``actual - judged``.
    difference: float
    #: ``difference / |judged|``; absent when the judgment was zero.
    relative: float | None
    #: The successor foundation's facts the Actual was read from.
    fact_ids: tuple[str, ...] = ()


class JudgmentToRevisit(_View):
    subject: str = Field(min_length=1)
    #: The Actual's period on the successor foundation (``FY2026``).
    period_id: str = Field(min_length=1)
    fiscal_year: int
    prior_row_ref: JudgmentRowRef
    #: The judged value at that period; absent for a non-numeric judgment.
    judged: float | None
    variance: ActualVariance | None


class BoundaryPeriod(_View):
    """A period the Actual moved from judgment to fact."""

    period_id: str = Field(min_length=1)
    fiscal_year: int
    #: The successor foundation's basis facts for the period: the ids a
    #: Supersession with trigger ``actual`` cites as ``trigger_ref``.
    fact_ids: tuple[str, ...] = ()


class RestatedHistoricalFact(_View):
    """A comparative basis fact whose value the successor restated."""

    period_id: str = Field(min_length=1)
    fiscal_year: int
    required_role: str = Field(min_length=1)
    prior_value: float
    restated_value: float
    #: ``restated_value - prior_value``.
    difference: float
    #: ``difference / |prior_value|``; absent when the prior value was zero.
    relative: float | None
    prior_fact_ids: tuple[str, ...] = ()
    restated_fact_ids: tuple[str, ...] = ()


class RestatedDriverSeed(_View):
    """A comparative driver seed whose value the successor restated."""

    driver_key: str = Field(min_length=1)
    period_id: str = Field(min_length=1)
    fiscal_year: int
    prior_value: float
    restated_value: float
    #: ``restated_value - prior_value``.
    difference: float
    #: ``difference / |prior_value|``; absent when the prior value was zero.
    relative: float | None
    prior_fact_ids: tuple[str, ...] = ()
    restated_fact_ids: tuple[str, ...] = ()


class ModelUpdateView(_View):
    schema_version: Literal["model-update-view.v1"] = "model-update-view.v1"
    actual_fact_ref: HistoricalFoundationRef
    prior_ref: ForecastAssumptionSetRef
    #: Successor historical periods the prior set held as forecast periods,
    #: plus comparative periods the successor restated against ``prior_basis``.
    boundary_periods: tuple[BoundaryPeriod, ...]
    judgments_to_revisit: tuple[JudgmentToRevisit, ...]
    restated_historical_facts: tuple[RestatedHistoricalFact, ...] = Field(
        default=(), exclude_if=lambda value: not value
    )
    restated_driver_seeds: tuple[RestatedDriverSeed, ...] = Field(
        default=(), exclude_if=lambda value: not value
    )


def _judged_value(row: ForecastDriverAssumption, fiscal_year: int) -> tuple[bool, float | None]:
    """Whether the row holds a judgment at ``fiscal_year``, and its number."""

    assumption = row.assumption
    if assumption.period_values is not None:
        if fiscal_year not in assumption.period_values:
            return False, None
        return True, float(assumption.period_values[fiscal_year])
    value = assumption.value
    if value is None:
        return False, None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return True, None
    return True, float(value)


def _seeds_by_node_year(
    foundations: Iterable[HistoricalFoundation],
) -> dict[tuple[str, int], HistoricalDriverSeed]:
    """Seeds keyed by node and fiscal year; an earlier foundation never overrides."""

    seeds: dict[tuple[str, int], HistoricalDriverSeed] = {}
    for foundation in foundations:
        years = {
            period.period_id: period.fiscal_year
            for period in foundation.fiscal_axis.historical_periods
        }
        for seed in foundation.historical_driver_seeds:
            year = years.get(seed.period_id)
            if year is not None:
                seeds.setdefault((seed.driver_key, year), seed)
    return seeds


def _actual_value(
    reading: DriverActualReading | None,
    fiscal_year: int,
    seeds: Mapping[tuple[str, int], HistoricalDriverSeed],
) -> tuple[float, tuple[str, ...]] | None:
    if reading is None:
        return None
    seed = seeds.get((reading.node_key, fiscal_year))
    if seed is None:
        return None
    if reading.value_semantics == "direct_control_value":
        return float(seed.value), seed.fact_ids
    prior = seeds.get((reading.node_key, fiscal_year - 1))
    if (
        prior is None
        or prior.value == 0
        or (prior.unit, prior.model_scale) != (seed.unit, seed.model_scale)
    ):
        return None
    growth = seed.value / prior.value - Decimal(1)
    return float(growth), tuple(sorted({*prior.fact_ids, *seed.fact_ids}))


def _variance(judged: float, actual: float, fact_ids: tuple[str, ...]) -> ActualVariance:
    difference = actual - judged
    return ActualVariance(
        judged=judged,
        actual=actual,
        difference=difference,
        relative=None if judged == 0 else difference / abs(judged),
        fact_ids=fact_ids,
    )


def _fact_role(fact: object) -> str | None:
    """The role a basis fact is compared under.

    A caller may name it ``required_role``. A stored ``HistoricalBasisFact``
    carries the same identity as ``required_role_correspondence``.
    """

    role = getattr(fact, "required_role", None)
    if not isinstance(role, str) or not role:
        role = getattr(fact, "required_role_correspondence", None)
    if isinstance(role, str) and role:
        return role
    return None


def _restated_facts(
    actual: HistoricalFoundation,
    prior_basis: HistoricalFoundation | None,
) -> tuple[RestatedHistoricalFact, ...]:
    """Basis facts the successor restated on a period both foundations hold."""

    if prior_basis is None:
        return ()
    prior_years = {
        period.period_id: period.fiscal_year
        for period in prior_basis.fiscal_axis.historical_periods
    }
    actual_years = {
        period.period_id: period.fiscal_year
        for period in actual.fiscal_axis.historical_periods
    }
    overlapping = [
        period.period_id
        for period in actual.fiscal_axis.historical_periods
        if period.period_id in prior_years
    ]
    prior_index: dict[tuple[str, str], object] = {}
    actual_index: dict[tuple[str, str], object] = {}
    for foundation, index in (
        (prior_basis, prior_index),
        (actual, actual_index),
    ):
        for fact in foundation.historical_basis_facts:
            if fact.period_id not in overlapping:
                continue
            role = _fact_role(fact)
            if role is None or getattr(fact, "value", None) is None:
                continue
            index.setdefault((fact.period_id, role), fact)
    rows: list[RestatedHistoricalFact] = []
    for period_id, required_role in sorted(set(prior_index) & set(actual_index)):
        prior_fact = prior_index[(period_id, required_role)]
        actual_fact = actual_index[(period_id, required_role)]
        if (
            getattr(prior_fact, "unit", None),
            getattr(prior_fact, "scale", None),
        ) != (
            getattr(actual_fact, "unit", None),
            getattr(actual_fact, "scale", None),
        ):
            continue
        prior_raw = getattr(prior_fact, "value")
        restated_raw = getattr(actual_fact, "value")
        if math.isclose(
            float(restated_raw),
            float(prior_raw),
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            continue
        prior_val = float(prior_raw)
        restated_val = float(restated_raw)
        difference = restated_val - prior_val
        prior_fact_id = getattr(prior_fact, "fact_id", None)
        restated_fact_id = getattr(actual_fact, "fact_id", None)
        rows.append(
            RestatedHistoricalFact(
                period_id=period_id,
                fiscal_year=actual_years[period_id],
                required_role=str(required_role),
                prior_value=prior_val,
                restated_value=restated_val,
                difference=difference,
                relative=None if prior_val == 0 else difference / abs(prior_val),
                prior_fact_ids=(str(prior_fact_id),) if prior_fact_id else (),
                restated_fact_ids=(str(restated_fact_id),) if restated_fact_id else (),
            )
        )
    return tuple(rows)


def _restated_seeds(
    actual: HistoricalFoundation,
    prior_basis: HistoricalFoundation | None,
) -> tuple[RestatedDriverSeed, ...]:
    """Driver seeds the successor restated on a period both foundations hold."""

    if prior_basis is None:
        return ()
    prior_years = {
        period.period_id: period.fiscal_year
        for period in prior_basis.fiscal_axis.historical_periods
    }
    actual_years = {
        period.period_id: period.fiscal_year
        for period in actual.fiscal_axis.historical_periods
    }
    overlapping = [
        period.period_id
        for period in actual.fiscal_axis.historical_periods
        if period.period_id in prior_years
    ]
    prior_index: dict[tuple[str, str], object] = {}
    actual_index: dict[tuple[str, str], object] = {}
    for foundation, index in (
        (prior_basis, prior_index),
        (actual, actual_index),
    ):
        for seed in foundation.historical_driver_seeds:
            if seed.period_id not in overlapping or getattr(seed, "value", None) is None:
                continue
            index.setdefault((seed.driver_key, seed.period_id), seed)
    rows: list[RestatedDriverSeed] = []
    for driver_key, period_id in sorted(set(prior_index) & set(actual_index)):
        prior_seed = prior_index[(driver_key, period_id)]
        actual_seed = actual_index[(driver_key, period_id)]
        if (
            getattr(prior_seed, "unit", None),
            getattr(prior_seed, "model_scale", None),
        ) != (
            getattr(actual_seed, "unit", None),
            getattr(actual_seed, "model_scale", None),
        ):
            continue
        prior_raw = getattr(prior_seed, "value")
        restated_raw = getattr(actual_seed, "value")
        if math.isclose(
            float(restated_raw),
            float(prior_raw),
            rel_tol=1e-12,
            abs_tol=1e-12,
        ):
            continue
        prior_val = float(prior_raw)
        restated_val = float(restated_raw)
        difference = restated_val - prior_val
        rows.append(
            RestatedDriverSeed(
                driver_key=driver_key,
                period_id=period_id,
                fiscal_year=actual_years[period_id],
                prior_value=prior_val,
                restated_value=restated_val,
                difference=difference,
                relative=None if prior_val == 0 else difference / abs(prior_val),
                prior_fact_ids=tuple(getattr(prior_seed, "fact_ids", ()) or ()),
                restated_fact_ids=tuple(getattr(actual_seed, "fact_ids", ()) or ()),
            )
        )
    return tuple(rows)


def update(
    prior: ForecastAssumptionSet,
    *,
    actual: HistoricalFoundation,
    prior_basis: HistoricalFoundation | None,
    readings: Mapping[str, DriverActualReading],
) -> ModelUpdateView:
    """The Variance of each prior judgment the Actual bears on, and those judgments.

    Comparative periods the successor restated against ``prior_basis`` are
    boundary periods too. A judgment is revisited only where a prior forecast
    period is now historical, never against a comparative restatement.
    """

    forecast_years = {period.fiscal_year for period in prior.fiscal_axis.forecast_periods}
    restated_facts = _restated_facts(actual, prior_basis)
    restated_seeds = _restated_seeds(actual, prior_basis)
    restated_period_ids = {row.period_id for row in restated_facts} | {
        row.period_id for row in restated_seeds
    }
    forecast_to_actual_periods = tuple(
        period
        for period in actual.fiscal_axis.historical_periods
        if period.fiscal_year in forecast_years
    )
    restated_periods = tuple(
        period
        for period in actual.fiscal_axis.historical_periods
        if period.period_id in restated_period_ids
        and period.fiscal_year not in forecast_years
    )
    boundary = tuple(
        period
        for period in actual.fiscal_axis.historical_periods
        if period in forecast_to_actual_periods or period in restated_periods
    )
    seeds = _seeds_by_node_year(
        (actual,) if prior_basis is None else (actual, prior_basis)
    )
    revisit: list[JudgmentToRevisit] = []
    for row in sorted(prior.driver_assumptions, key=lambda item: item.assumption.driver):
        subject = row.assumption.driver
        for period in forecast_to_actual_periods:
            held, judged = _judged_value(row, period.fiscal_year)
            if not held:
                continue
            observed = (
                None
                if judged is None
                else _actual_value(readings.get(subject), period.fiscal_year, seeds)
            )
            revisit.append(
                JudgmentToRevisit(
                    subject=subject,
                    period_id=period.period_id,
                    fiscal_year=period.fiscal_year,
                    prior_row_ref=JudgmentRowRef(artifact_ref=prior.ref, row=subject),
                    judged=judged,
                    variance=(
                        None
                        if observed is None or judged is None
                        else _variance(judged, *observed)
                    ),
                )
            )
    return ModelUpdateView(
        actual_fact_ref=actual.ref,
        prior_ref=prior.ref,
        boundary_periods=tuple(
            BoundaryPeriod(
                period_id=period.period_id,
                fiscal_year=period.fiscal_year,
                fact_ids=tuple(
                    sorted(
                        fact.fact_id
                        for fact in actual.historical_basis_facts
                        if fact.period_id == period.period_id
                    )
                ),
            )
            for period in boundary
        ),
        judgments_to_revisit=tuple(revisit),
        restated_historical_facts=restated_facts,
        restated_driver_seeds=restated_seeds,
    )


__all__ = [
    "ActualVariance",
    "BoundaryPeriod",
    "DriverActualReading",
    "JudgmentRowRef",
    "JudgmentToRevisit",
    "ModelUpdateView",
    "RestatedDriverSeed",
    "RestatedHistoricalFact",
    "update",
]
