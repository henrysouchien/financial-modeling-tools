"""``update``: an Actual against the judgments that held its period.

Engine (B), pure; spec ``update : (ModelState, Fact[Actual]) -> (Variance,
Set<Judgment> to revisit)``. The prior judgments are one accepted
ForecastAssumptionSet's driver rows. The Actual is the successor
HistoricalFoundation the historical-foundation door wrote when the reported
period arrived (``actual_fact_ref``). Every forecast period of the prior set
that the successor now holds as a historical period is a boundary period: a
judgment there now faces a fact. Each driver row valued at a boundary period
is a judgment to revisit.

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


class ModelUpdateView(_View):
    schema_version: Literal["model-update-view.v1"] = "model-update-view.v1"
    actual_fact_ref: HistoricalFoundationRef
    prior_ref: ForecastAssumptionSetRef
    #: Successor historical periods the prior set held as forecast periods.
    boundary_periods: tuple[BoundaryPeriod, ...]
    judgments_to_revisit: tuple[JudgmentToRevisit, ...]


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


def update(
    prior: ForecastAssumptionSet,
    *,
    actual: HistoricalFoundation,
    prior_basis: HistoricalFoundation | None,
    readings: Mapping[str, DriverActualReading],
) -> ModelUpdateView:
    """The Variance of each prior judgment the Actual bears on, and those judgments."""

    forecast_years = {period.fiscal_year for period in prior.fiscal_axis.forecast_periods}
    boundary = tuple(
        period
        for period in actual.fiscal_axis.historical_periods
        if period.fiscal_year in forecast_years
    )
    seeds = _seeds_by_node_year(
        (actual,) if prior_basis is None else (actual, prior_basis)
    )
    revisit: list[JudgmentToRevisit] = []
    for row in sorted(prior.driver_assumptions, key=lambda item: item.assumption.driver):
        subject = row.assumption.driver
        for period in boundary:
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
    )


__all__ = [
    "ActualVariance",
    "BoundaryPeriod",
    "DriverActualReading",
    "JudgmentRowRef",
    "JudgmentToRevisit",
    "ModelUpdateView",
    "update",
]
