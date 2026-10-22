"""Role-keyed validation input construction from pinned observations."""

from __future__ import annotations

from collections.abc import Mapping, Sequence

from .build_required_lines import RequiredLinePlan
from .provider_port.contracts import FilingLineObservation, FilingLineRef
from .provider_port.registry import EDGAR_PROVIDER_ID, FMP_PROVIDER_ID, ProviderId
from value_semantics_core import CONVENTIONS_IR_V1
from .required_role_catalog import load_required_role_catalog
from .statement_tolerance_profiles import CROSS_SOURCE_ADVISORY_TOLERANCES
from .validation_input import ValidationInput
_SCALE_FACTORS = {
    row.numeric_scale: row.multiplier
    for row in CONVENTIONS_IR_V1.scale_factors
}


def _observation_values_by_role(
    observations: Sequence[FilingLineObservation],
    *,
    role_by_line: Mapping[FilingLineRef, str],
    provider: ProviderId,
) -> dict[str, dict[int, float]]:
    values: dict[str, dict[int, float]] = {}
    for observation in observations:
        fact = observation.fact
        required_role = role_by_line.get(observation.line)
        if fact is None or required_role is None or fact.provenance is None:
            continue
        if fact.provenance.provider != provider:
            continue
        period_id = observation.line.period.canonical_label()
        if (
            not period_id.startswith("FY")
            or period_id.endswith("E")
            or not period_id[2:].isdigit()
        ):
            continue
        values.setdefault(required_role, {})[int(period_id[2:])] = float(
            fact.value.value * _SCALE_FACTORS[fact.value.display.scale]
        )
    return values


def _fetch_validation_fmp_line_buffer(
    observations: Sequence[FilingLineObservation],
    *,
    role_by_line: Mapping[FilingLineRef, str],
) -> dict[str, dict[int, float]]:
    return _observation_values_by_role(
        observations,
        role_by_line=role_by_line,
        provider=FMP_PROVIDER_ID,
    )


def _fetch_validation_edgar_line_buffer(
    observations: Sequence[FilingLineObservation],
    *,
    role_by_line: Mapping[FilingLineRef, str],
) -> dict[str, dict[int, float]]:
    return _observation_values_by_role(
        observations,
        role_by_line=role_by_line,
        provider=EDGAR_PROVIDER_ID,
    )


def _make_validation_input(
    *,
    statement_facts: Sequence[FilingLineObservation],
    required_line_plan: RequiredLinePlan,
    historical_periods: Sequence[int],
    served_source_by_role_year: Mapping[str, Mapping[int, ProviderId]],
) -> ValidationInput:
    """Project observed producer outcomes; never re-arbitrate the pin policy."""

    catalog = load_required_role_catalog()
    role_by_line = {
        line: row.required_role
        for row in required_line_plan.rows
        for line in row.pinned_columns
    }
    for required_role in frozenset(role_by_line.values()):
        catalog.role(required_role)

    tolerance_pct_by_role = {
        required_role: row.tolerance_pct
        for required_role, row in CROSS_SOURCE_ADVISORY_TOLERANCES.items()
        if required_role in frozenset(role_by_line.values())
    }
    return ValidationInput(
        fmp_buffer_by_role=_fetch_validation_fmp_line_buffer(
            statement_facts,
            role_by_line=role_by_line,
        ),
        edgar_buffer_by_role=_fetch_validation_edgar_line_buffer(
            statement_facts,
            role_by_line=role_by_line,
        ),
        opted_in_roles=sorted(tolerance_pct_by_role),
        tolerance_pct_by_role=tolerance_pct_by_role,
        historical_years=sorted({int(year) for year in historical_periods}),
        served_source_by_role_year={
            required_role: {int(year): provider for year, provider in by_year.items()}
            for required_role, by_year in served_source_by_role_year.items()
        },
    )


__all__ = [
    "_fetch_validation_edgar_line_buffer",
    "_fetch_validation_fmp_line_buffer",
    "_make_validation_input",
]
