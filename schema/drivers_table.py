"""One row per accepted forecast driver, joining the owners that judged it.

``drivers(research_file_id)`` reads the current accepted ForecastAssumptionSet,
the accepted ScenarioSet bound to its exact ref, and the forecast-assumptions
context driver descriptor. Every type a row carries is the owner's own type;
this module only names the join and the read's transparency fields.

The answer is bounded: ``drivers`` summarizes every driver in one line, and
``rows`` carries the full joined row only for the driver the ``driver_key``
selector names, so the table fits the runner's result bound however many
drivers and citations the forecast carries.
"""

from __future__ import annotations

from datetime import datetime

from industry_slice_core.industry_types import Unit
from pydantic import BaseModel, ConfigDict, Field

from .business_model_revision_ref import BusinessModelRevisionRef
from .forecast_assumption_set import (
    ForecastAssumptionSetRef,
    ForecastDriverAssumption,
)
from .forecast_assumptions_context import ForecastAssumptionsHistoricalDriverSeed
from .historical_foundation import HistoricalFoundationRef
from .models import ModelScale
from .model_spec_authoring import AuthoringPeriodValueSeries, ScalarValue
from .scenario_set import (
    ScenarioCriticalFactor,
    ScenarioDriverCurve,
    ScenarioSetRef,
)


class _Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class DriversTableRow(_Contract):
    """One FAS driver: descriptor, base judgment, scenario treatment, factor."""

    driver_key: str = Field(min_length=1)
    label: str = Field(min_length=1)
    unit: Unit
    model_scale: ModelScale
    value_semantics: str = Field(min_length=1)
    category: str = Field(min_length=1)
    historical_seeds: tuple[ForecastAssumptionsHistoricalDriverSeed, ...] = ()
    base: ForecastDriverAssumption
    scenario: ScenarioDriverCurve | None
    critical_factor: ScenarioCriticalFactor | None


class DriversTableSummaryRow(_Contract):
    """One driver's identity and base values; ``driver_key`` selects its row."""

    driver_key: str = Field(min_length=1)
    label: str = Field(min_length=1)
    unit: Unit
    model_scale: ModelScale
    category: str = Field(min_length=1)
    classification: str = Field(min_length=1)
    value: ScalarValue | None
    period_values: AuthoringPeriodValueSeries | None
    scenario_role: str | None
    critical_factor_key: str | None

    @classmethod
    def of(cls, row: DriversTableRow) -> "DriversTableSummaryRow":
        return cls(
            driver_key=row.driver_key,
            label=row.label,
            unit=row.unit,
            model_scale=row.model_scale,
            category=row.category,
            classification=row.base.classification,
            value=row.base.assumption.value,
            period_values=row.base.assumption.period_values,
            scenario_role=None if row.scenario is None else row.scenario.role,
            critical_factor_key=(
                None if row.critical_factor is None else row.critical_factor.factor_key
            ),
        )


class DriversTableRead(_Contract):
    research_file_id: int = Field(gt=0)
    business_model_revision_ref: BusinessModelRevisionRef
    historical_foundation_ref: HistoricalFoundationRef
    forecast_assumption_set_ref: ForecastAssumptionSetRef
    scenario_set_ref: ScenarioSetRef | None


class DriversTableProduced(_Contract):
    drivers: int = Field(ge=0)
    rows: int = Field(ge=0)


class DriversTableWallSeconds(_Contract):
    total: float = Field(ge=0)


class DriversTableAsOf(_Contract):
    forecast_assumption_set_created_at: datetime
    scenario_set_created_at: datetime | None


class DriversTableResponse(_Contract):
    schema_version: str = "drivers-table.v1"
    research_file_id: int = Field(gt=0)
    ticker: str = Field(min_length=1)
    forecast_assumption_set_ref: ForecastAssumptionSetRef
    scenario_set_ref: ScenarioSetRef | None
    drivers: tuple[DriversTableSummaryRow, ...]
    driver_key: str | None
    rows: tuple[DriversTableRow, ...]
    read: DriversTableRead
    produced: DriversTableProduced
    wall_seconds: DriversTableWallSeconds
    as_of: DriversTableAsOf


__all__ = [
    "DriversTableAsOf",
    "DriversTableProduced",
    "DriversTableRead",
    "DriversTableResponse",
    "DriversTableRow",
    "DriversTableSummaryRow",
    "DriversTableWallSeconds",
]
