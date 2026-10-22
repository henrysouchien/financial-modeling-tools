"""Cross-source validation checks for build diagnostics."""

from __future__ import annotations

from typing import Any

from .build_diagnostic_types import CrossSourceValidationCheck, DiagnosticTolerances
from .build_diagnostic_values import SEVERITY_ORDER
from .provider_port.registry import (
    ProviderId,
    ProviderRegistry,
    require_registered_provider_id,
)
from .validation_input import ValidationInput
from .required_role_catalog import RequiredRoleCatalog, load_required_role_catalog


def _check_cross_source_validation(
    validation_input: ValidationInput | None,
    *,
    role_catalog: RequiredRoleCatalog | None = None,
    tolerances: DiagnosticTolerances,
    provider_registry: ProviderRegistry | None = None,
) -> CrossSourceValidationCheck:
    if validation_input is None:
        return CrossSourceValidationCheck(enabled=False)

    result = CrossSourceValidationCheck(enabled=True)
    role_catalog = role_catalog or load_required_role_catalog()
    admitted_roles = frozenset(role_catalog.role_ids())
    opted_in = sorted(
        {
            str(role)
            for role in validation_input.opted_in_roles
            if str(role) in admitted_roles
        }
    )
    historical_years = sorted(int(year) for year in validation_input.historical_years)

    for required_role in opted_in:
        tolerance = validation_input.tolerance_pct_by_role.get(required_role)
        if tolerance is None:
            continue

        tolerance_pct = float(tolerance)
        role_payload: dict[str, Any] = {
            "tolerance_pct": tolerance_pct,
            "headline_severity": "ok",
            "by_year": {},
        }

        for year in historical_years:
            fmp_value = _validation_buffer_value(
                validation_input.fmp_buffer_by_role.get(required_role),
                year,
                ("values", "values_dict"),
            )
            edgar_value = _validation_buffer_value(
                validation_input.edgar_buffer_by_role.get(required_role),
                year,
                ("values_dict", "values"),
            )

            if fmp_value is None and edgar_value is None:
                continue

            served_source = _validation_served_source(
                validation_input,
                required_role,
                year,
                provider_registry=provider_registry,
            )
            if fmp_value is None or edgar_value is None:
                role_payload["by_year"][str(year)] = {
                    "comparison_status": "incomparable",
                    "fmp_value": fmp_value,
                    "edgar_value": edgar_value,
                    "delta": None,
                    "delta_pct": None,
                    "abs_delta_pct": None,
                    "served_source": served_source,
                    "severity": "ok",
                }
                result.summary["cells_incomparable"] += 1
                continue

            delta = float(edgar_value) - float(fmp_value)
            denominator = max(abs(float(edgar_value)), abs(float(fmp_value)))
            if denominator > 0:
                delta_pct = delta / denominator
                abs_delta_pct = abs(delta) / denominator
            else:
                delta_pct = 0.0
                abs_delta_pct = 0.0

            severity = "ok"
            if abs_delta_pct >= tolerances.cross_source_material_pct:
                severity = "material_gap"
            elif abs_delta_pct >= tolerance_pct:
                severity = "gap"

            role_payload["by_year"][str(year)] = {
                "comparison_status": "compared",
                "fmp_value": float(fmp_value),
                "edgar_value": float(edgar_value),
                "delta": delta,
                "delta_pct": delta_pct,
                "abs_delta_pct": abs_delta_pct,
                "served_source": served_source,
                "severity": severity,
            }
            result.summary["cells_compared"] += 1
            if (
                SEVERITY_ORDER[severity]
                > SEVERITY_ORDER[role_payload["headline_severity"]]
            ):
                role_payload["headline_severity"] = severity

        result.by_role[required_role] = role_payload

    result.summary["roles_checked"] = len(result.by_role)
    for payload in result.by_role.values():
        headline = payload.get("headline_severity")
        if headline == "gap":
            result.summary["roles_with_gap"] += 1
        elif headline == "material_gap":
            result.summary["roles_with_material_gap"] += 1

    return result


def _validation_buffer_value(
    fetch_result: Any,
    year: int,
    value_attrs: tuple[str, ...],
) -> float | None:
    if fetch_result is None:
        return None

    for attr in value_attrs:
        values = getattr(fetch_result, attr, None)
        if isinstance(values, dict):
            value = _year_lookup(values, year)
            if value is not None:
                return float(value)

    if isinstance(fetch_result, dict):
        for key in value_attrs:
            values = fetch_result.get(key)
            if isinstance(values, dict):
                value = _year_lookup(values, year)
                if value is not None:
                    return float(value)
        value = _year_lookup(fetch_result, year)
        if value is not None:
            return float(value)

    return None


def _year_lookup(values: dict[Any, Any], year: int) -> Any | None:
    if year in values:
        return values[year]
    return values.get(str(year))


def _validation_served_source(
    validation_input: ValidationInput,
    required_role: str,
    year: int,
    *,
    provider_registry: ProviderRegistry | None = None,
) -> ProviderId | None:
    source_by_year = validation_input.served_source_by_role_year.get(
        required_role, {}
    )
    value = source_by_year.get(year, source_by_year.get(str(year)))  # type: ignore[arg-type]
    if value is None:
        return None
    return require_registered_provider_id(
        value,
        provider_registry=provider_registry,
        entry_point=(
            f"cross-source validation served provider for role {required_role!r} "
            f"year {year}"
        ),
    )


__all__ = [
    "_check_cross_source_validation",
    "_validation_buffer_value",
    "_year_lookup",
    "_validation_served_source",
]
