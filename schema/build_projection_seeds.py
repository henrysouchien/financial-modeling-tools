"""Projection override seeding helpers for schema build orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
import logging
import sys
from typing import TYPE_CHECKING, Any, Callable, Literal

from .build_scenarios import _find_scenario_value_row as _find_scenario_value_row_fallback
from .models import FinancialModel, FormulaSpec, FormulaType, LineItem
from .overrides import TickerOverrides
from .refs import line_item_ref_from_obj as _line_item_ref_from_obj_fallback

if TYPE_CHECKING:
    from .business_model_compiler import CompiledDriverRegistry


@dataclass
class OrphanedProjection:
    rate_key: str
    reason: Literal[
        "bm_key_not_in_registry",
        "tpl_item_not_found",
        "item_not_found",
        "bull_flex_row_not_found",
        "bear_flex_row_not_found",
    ]
    detail: str
    last_provenance: dict[str, Any] | None

    def to_dict(self) -> dict[str, Any]:
        return {
            "rate_key": self.rate_key,
            "reason": self.reason,
            "detail": self.detail,
            "last_provenance": self.last_provenance,
        }


@dataclass
class SeedProjectionWarning:
    rate_key: str
    kind: Literal[
        "base_flex_row_not_found",
        "base_period_values_missing",
        "scenario_ordering_violation",
    ]
    detail: str
    last_provenance: dict[str, Any] | None
    scenario: Literal["bull", "bear"] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "rate_key": self.rate_key,
            "kind": self.kind,
            "scenario": self.scenario,
            "detail": self.detail,
            "last_provenance": self.last_provenance,
        }


@dataclass
class SeedProjectionsResult:
    seeded_count: int = 0
    orphans: list[OrphanedProjection] = field(default_factory=list)
    warnings: list[SeedProjectionWarning] = field(default_factory=list)
    total_rate_keys: int = 0
    schema_version_seen: str | None = None
    validation_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "seeded_count": self.seeded_count,
            "orphans": [orphan.to_dict() for orphan in self.orphans],
            "warnings": [warning.to_dict() for warning in self.warnings],
            "total_rate_keys": self.total_rate_keys,
            "schema_version_seen": self.schema_version_seen,
            "validation_error": self.validation_error,
        }


_SCENARIO_ORDERING_EPS = 1e-12


def _parent_attr(name: str, fallback: Any) -> Any:
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    if parent is None:
        from . import build as parent
    return getattr(parent, name, fallback)


def _schema_version_seen(ticker_overrides: TickerOverrides | None) -> str | None:
    if ticker_overrides is None:
        return None
    value = (ticker_overrides.file_meta or {}).get("schema_version")
    return str(value) if value is not None else None


def _projection_provenance_to_dict(provenance: Any) -> dict[str, Any] | None:
    if provenance is None:
        return None
    if hasattr(provenance, "model_dump"):
        return provenance.model_dump(mode="json")
    if isinstance(provenance, dict):
        return dict(provenance)
    return None


def _extract_last_provenance(projection_entry: Any) -> dict[str, Any] | None:
    projection_provenance_to_dict = _parent_attr(
        "_projection_provenance_to_dict",
        _projection_provenance_to_dict,
    )
    latest: tuple[str, dict[str, Any] | None] | None = None
    for scenario in getattr(projection_entry, "scenarios", {}).values():
        provenance = projection_provenance_to_dict(getattr(scenario, "provenance", None))
        if provenance is None:
            continue
        written_at = str(provenance.get("written_at") or "")
        if latest is None or written_at >= latest[0]:
            latest = (written_at, provenance)
    return latest[1] if latest is not None else None


def _resolve_scenario_flex_row(
    model: FinancialModel,
    item_id: str,
    scenario_name: Literal["bull", "bear"],
) -> LineItem | None:
    """Resolve a scenario-table flex row for an offset-scenario owner or anchor."""

    try:
        item = model.get_item(item_id)
    except KeyError:
        return None

    formula_type = _parent_attr("FormulaType", FormulaType)
    line_item_ref_from_obj = _parent_attr(
        "line_item_ref_from_obj",
        _line_item_ref_from_obj_fallback,
    )
    find_scenario_value_row = _parent_attr(
        "_find_scenario_value_row",
        _find_scenario_value_row_fallback,
    )

    anchor_ids: list[str] = []
    spec = item.projected
    if spec is not None and spec.type == formula_type.valuation and spec.subtype == "offset_scenario":
        anchor_ref = line_item_ref_from_obj((spec.params or {}).get("anchor"))
        if anchor_ref is not None:
            anchor_ids.append(anchor_ref.id)

    anchor_ids.append(item_id)

    seen: set[str] = set()
    for anchor_id in anchor_ids:
        if anchor_id in seen:
            continue
        seen.add(anchor_id)
        value_row_id = find_scenario_value_row(model, anchor_id, scenario_name)
        if value_row_id is None:
            continue
        try:
            return model.get_item(value_row_id)
        except KeyError:
            continue
    return None


def _resolve_scenario_case_row(
    model: FinancialModel,
    item_id: str,
    scenario_name: Literal["bull", "base", "bear"],
) -> LineItem | None:
    resolve_scenario_flex_row = _parent_attr(
        "_resolve_scenario_flex_row",
        _resolve_scenario_flex_row,
    )
    if scenario_name in {"bull", "bear"}:
        return resolve_scenario_flex_row(model, item_id, scenario_name)

    try:
        item = model.get_item(item_id)
    except KeyError:
        return None

    formula_type = _parent_attr("FormulaType", FormulaType)
    line_item_ref_from_obj = _parent_attr(
        "line_item_ref_from_obj",
        _line_item_ref_from_obj_fallback,
    )
    find_scenario_value_row = _parent_attr(
        "_find_scenario_value_row",
        _find_scenario_value_row_fallback,
    )

    anchor_ids: list[str] = []
    spec = item.projected
    if spec is not None and spec.type == formula_type.valuation and spec.subtype == "offset_scenario":
        anchor_ref = line_item_ref_from_obj((spec.params or {}).get("anchor"))
        if anchor_ref is not None:
            anchor_ids.append(anchor_ref.id)
    anchor_ids.append(item_id)

    seen: set[str] = set()
    for anchor_id in anchor_ids:
        if anchor_id in seen:
            continue
        seen.add(anchor_id)
        value_row_id = find_scenario_value_row(model, anchor_id, "base")
        if value_row_id is None:
            continue
        try:
            return model.get_item(value_row_id)
        except KeyError:
            continue
    return None


def _projection_entry_values_by_period(
    *,
    model: FinancialModel,
    item_id: str,
    scenario_entry: Any,
    most_recent_fy: int,
    percent_normalize: Callable[..., float],
) -> dict[int, float]:
    projection_entry_value_for_model = _parent_attr(
        "_projection_entry_value_for_model",
        _projection_entry_value_for_model,
    )
    return {
        int(year_str): projection_entry_value_for_model(
            model=model,
            item_id=item_id,
            scenario_entry=scenario_entry,
            raw_value=raw_value,
            percent_normalize=percent_normalize,
        )
        for year_str, raw_value in getattr(scenario_entry, "values", {}).items()
        if int(year_str) > most_recent_fy
    }


def _projection_entry_value_for_model(
    *,
    model: FinancialModel,
    item_id: str,
    scenario_entry: Any,
    raw_value: Any,
    percent_normalize: Callable[..., float],
) -> float:
    value_scale = getattr(scenario_entry, "value_scale", "display")
    if value_scale == "model":
        return float(raw_value)
    fields_set = (
        getattr(scenario_entry, "model_fields_set", None)
        or getattr(scenario_entry, "__fields_set__", set())
        or set()
    )
    explicit_display_scale = value_scale == "display" and "value_scale" in fields_set
    return percent_normalize(
        model,
        item_id,
        raw_value,
        decimal_passthrough=explicit_display_scale,
    )


def _item_seeded_values_by_period(item: LineItem, most_recent_fy: int) -> dict[int, float]:
    values: dict[int, float] = {}
    if item.values is not None:
        for period, cell in item.values.values.items():
            if int(period) > most_recent_fy and cell.value is not None:
                values[int(period)] = float(cell.value)
    if item.overrides is not None:
        formula_type = _parent_attr("FormulaType", FormulaType)
        for period, spec in item.overrides.items():
            if int(period) <= most_recent_fy:
                continue
            if spec.type != formula_type.constant:
                continue
            value = (spec.params or {}).get("value")
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                values[int(period)] = float(value)
    return values


def _projection_base_values_by_period(
    *,
    model: FinancialModel,
    projections: Any,
    projection_entry: Any,
    rate_key: str,
    base_item: LineItem | None,
    most_recent_fy: int,
    percent_normalize: Callable[..., float],
) -> dict[int, float]:
    if base_item is None:
        return {}

    projection_entry_values_by_period = _parent_attr(
        "_projection_entry_values_by_period",
        _projection_entry_values_by_period,
    )
    projection_owner_base_entry_for_case_row = _parent_attr(
        "_projection_owner_base_entry_for_case_row",
        _projection_owner_base_entry_for_case_row,
    )
    item_seeded_values_by_period = _parent_attr(
        "_item_seeded_values_by_period",
        _item_seeded_values_by_period,
    )

    base_entry = projection_entry.scenarios.get("base")
    if base_entry is None:
        base_projection_entry = projections.root.get(base_item.id)
        if base_projection_entry is None and base_item.id != rate_key:
            base_projection_entry = projections.root.get(rate_key)
        if base_projection_entry is not None:
            base_entry = base_projection_entry.scenarios.get("base")
    if base_entry is None:
        base_entry = projection_owner_base_entry_for_case_row(
            model=model,
            projections=projections,
            rate_key=rate_key,
            base_item=base_item,
        )
    if base_entry is not None:
        return projection_entry_values_by_period(
            model=model,
            item_id=base_item.id,
            scenario_entry=base_entry,
            most_recent_fy=most_recent_fy,
            percent_normalize=percent_normalize,
        )
    return item_seeded_values_by_period(base_item, most_recent_fy)


def _projection_owner_base_entry_for_case_row(
    *,
    model: FinancialModel,
    projections: Any,
    rate_key: str,
    base_item: LineItem,
) -> Any | None:
    """Find an owner-row base projection for an anchor-level scenario entry."""

    resolve_scenario_case_row = _parent_attr(
        "_resolve_scenario_case_row",
        _resolve_scenario_case_row,
    )
    for candidate_key, candidate_entry in getattr(projections, "root", {}).items():
        if candidate_key in {rate_key, base_item.id}:
            continue
        candidate_base_entry = candidate_entry.scenarios.get("base")
        if candidate_base_entry is None:
            continue
        try:
            model.get_item(candidate_key)
        except KeyError:
            continue
        candidate_base_item = resolve_scenario_case_row(model, candidate_key, "base")
        if candidate_base_item is None or candidate_base_item.id != base_item.id:
            continue
        return candidate_base_entry
    return None


def _scenario_ordering_violations_by_case(
    *,
    bull_values: dict[int, float],
    base_values: dict[int, float],
    bear_values: dict[int, float],
) -> dict[Literal["bull", "bear"], list[str]]:
    scenario_ordering_eps = _parent_attr("_SCENARIO_ORDERING_EPS", _SCENARIO_ORDERING_EPS)
    violations: dict[Literal["bull", "bear"], list[str]] = {"bull": [], "bear": []}
    for period in sorted(set(bull_values) & set(base_values) & set(bear_values)):
        bull = bull_values[period]
        base = base_values[period]
        bear = bear_values[period]
        detail = f"{period}:bull={bull:g},base={base:g},bear={bear:g}"
        if abs(bull - bear) <= scenario_ordering_eps:
            violations["bull"].append(f"{detail},expected=bull/base/bear_distinct")
            violations["bear"].append(f"{detail},expected=bull/base/bear_distinct")
        elif bull > bear:
            if bull <= base + scenario_ordering_eps:
                violations["bull"].append(f"{detail},expected=bull>base")
            if bear >= base - scenario_ordering_eps:
                violations["bear"].append(f"{detail},expected=bear<base")
        else:
            if bull >= base - scenario_ordering_eps:
                violations["bull"].append(f"{detail},expected=bull<base")
            if bear <= base + scenario_ordering_eps:
                violations["bear"].append(f"{detail},expected=bear>base")
    return {case: details for case, details in violations.items() if details}


def _seed_projections_from_overrides(
    model: FinancialModel,
    ticker_overrides: TickerOverrides | None,
    compiled_registry: "CompiledDriverRegistry | None",
    most_recent_fy: int,
) -> SeedProjectionsResult:
    """Seed durable projection override values into renderer-priority overrides."""

    schema_version_seen = _parent_attr("_schema_version_seen", _schema_version_seen)
    seed_projections_result = _parent_attr("SeedProjectionsResult", SeedProjectionsResult)
    schema_version_seen_value = schema_version_seen(ticker_overrides)
    if ticker_overrides is None or not ticker_overrides.projections:
        return seed_projections_result(
            total_rate_keys=0,
            schema_version_seen=schema_version_seen_value,
        )

    from pydantic import ValidationError

    from .overrides_projections import ProjectionsSection
    from .scenario_bridge import percent_normalize_via_formatter

    def _projection_percent_normalize(
        model: FinancialModel,
        target_item_id: str,
        value: Any,
        *,
        decimal_passthrough: bool = False,
    ) -> float:
        return percent_normalize_via_formatter(
            model,
            target_item_id,
            value,
            decimal_passthrough=decimal_passthrough,
        )

    try:
        projections = ProjectionsSection.model_validate(ticker_overrides.projections)
    except ValidationError as exc:
        logging.error("Projections section failed validation: %s", exc)
        return seed_projections_result(
            seeded_count=0,
            orphans=[],
            total_rate_keys=0,
            schema_version_seen=schema_version_seen_value,
            validation_error=str(exc),
        )

    orphaned_projection = _parent_attr("OrphanedProjection", OrphanedProjection)
    seed_projection_warning = _parent_attr("SeedProjectionWarning", SeedProjectionWarning)
    extract_last_provenance = _parent_attr("_extract_last_provenance", _extract_last_provenance)
    projection_provenance_to_dict = _parent_attr(
        "_projection_provenance_to_dict",
        _projection_provenance_to_dict,
    )
    resolve_scenario_case_row = _parent_attr(
        "_resolve_scenario_case_row",
        _resolve_scenario_case_row,
    )
    resolve_scenario_flex_row = _parent_attr(
        "_resolve_scenario_flex_row",
        _resolve_scenario_flex_row,
    )
    projection_entry_values_by_period = _parent_attr(
        "_projection_entry_values_by_period",
        _projection_entry_values_by_period,
    )
    projection_entry_value_for_model = _parent_attr(
        "_projection_entry_value_for_model",
        _projection_entry_value_for_model,
    )
    projection_base_values_by_period = _parent_attr(
        "_projection_base_values_by_period",
        _projection_base_values_by_period,
    )
    scenario_ordering_violations_by_case = _parent_attr(
        "_scenario_ordering_violations_by_case",
        _scenario_ordering_violations_by_case,
    )
    formula_spec = _parent_attr("FormulaSpec", FormulaSpec)
    formula_type = _parent_attr("FormulaType", FormulaType)

    seeded_count = 0
    orphans: list[OrphanedProjection] = []
    warnings: list[SeedProjectionWarning] = []
    total_rate_keys = len(projections.root)

    bm_registry_items: set[str] = set()
    if compiled_registry is not None:
        bm_registry_items = set(compiled_registry.node_items.values()) | set(
            compiled_registry.driver_keys.values()
        )

    for rate_key, projection_entry in projections.root.items():
        try:
            item = model.get_item(rate_key)
        except KeyError:
            item = None

        if item is None:
            if (
                rate_key.startswith("bm.")
                and compiled_registry is not None
                and rate_key not in bm_registry_items
            ):
                reason: Literal[
                    "bm_key_not_in_registry",
                    "tpl_item_not_found",
                    "item_not_found",
                    "bull_flex_row_not_found",
                    "bear_flex_row_not_found",
                ] = "bm_key_not_in_registry"
                detail = (
                    f"BM-driven key {rate_key!r} is not in this build's compiled registry; "
                    "the BM artifact may have changed since the projection was written"
                )
            elif rate_key.startswith("tpl."):
                reason = "tpl_item_not_found"
                detail = (
                    f"Template-driven key {rate_key!r} is not present in the model; "
                    "the SIA template may have been renamed or restructured"
                )
            else:
                reason = "item_not_found"
                detail = (
                    f"Projection key {rate_key!r} is not present in the current model; "
                    "it may be a manual edit or stale key"
                )
            orphans.append(
                orphaned_projection(
                    rate_key=rate_key,
                    reason=reason,
                    detail=detail,
                    last_provenance=extract_last_provenance(projection_entry),
                )
            )
            continue

        base = projection_entry.scenarios.get("base")
        if base is not None:
            if item.overrides is None:
                item.overrides = {}
            for year_str, raw_value in base.values.items():
                period = int(year_str)
                if period <= most_recent_fy:
                    continue
                normalized = projection_entry_value_for_model(
                    model=model,
                    item_id=item.id,
                    scenario_entry=base,
                    raw_value=raw_value,
                    percent_normalize=_projection_percent_normalize,
                )
                item.overrides[period] = formula_spec(
                    type=formula_type.constant,
                    params={"value": normalized},
                )
            seeded_count += 1
            base_scenario_item = resolve_scenario_case_row(model, item.id, "base")
            if (
                base_scenario_item is not None
                and base_scenario_item.id != item.id
                and projections.root.get(base_scenario_item.id) is None
            ):
                base_scenario_values = projection_entry_values_by_period(
                    model=model,
                    item_id=base_scenario_item.id,
                    scenario_entry=base,
                    most_recent_fy=most_recent_fy,
                    percent_normalize=_projection_percent_normalize,
                )
                if base_scenario_values:
                    if base_scenario_item.overrides is None:
                        base_scenario_item.overrides = {}
                    for period, normalized in base_scenario_values.items():
                        base_scenario_item.overrides[period] = formula_spec(
                            type=formula_type.constant,
                            params={"value": normalized},
                        )
                    seeded_count += 1

        skip_scenario_seed: set[str] = set()
        bull_entry = projection_entry.scenarios.get("bull")
        bear_entry = projection_entry.scenarios.get("bear")
        scenario_values: dict[str, dict[int, float]] = {}
        scenario_items: dict[str, LineItem] = {}
        if bull_entry is not None or bear_entry is not None:
            base_item = resolve_scenario_case_row(model, item.id, "base")
            if base_item is None:
                for scenario_name, scenario_entry in (
                    ("bull", bull_entry),
                    ("bear", bear_entry),
                ):
                    if scenario_entry is None:
                        continue
                    warnings.append(
                        seed_projection_warning(
                            rate_key=rate_key,
                            kind="base_flex_row_not_found",
                            scenario=scenario_name,
                            detail=f"No base scenario flex row found for {rate_key!r}; skipped {scenario_name} seed",
                            last_provenance=projection_provenance_to_dict(
                                scenario_entry.provenance
                            ),
                        )
                    )
                    skip_scenario_seed.add(scenario_name)
            else:
                base_values = projection_base_values_by_period(
                    model=model,
                    projections=projections,
                    projection_entry=projection_entry,
                    rate_key=rate_key,
                    base_item=base_item,
                    most_recent_fy=most_recent_fy,
                    percent_normalize=_projection_percent_normalize,
                )
                for scenario_name, scenario_entry in (
                    ("bull", bull_entry),
                    ("bear", bear_entry),
                ):
                    if scenario_entry is None:
                        continue
                    scenario_item = resolve_scenario_case_row(model, item.id, scenario_name)
                    if scenario_item is None:
                        continue
                    scenario_items[scenario_name] = scenario_item
                    values = projection_entry_values_by_period(
                        model=model,
                        item_id=scenario_item.id,
                        scenario_entry=scenario_entry,
                        most_recent_fy=most_recent_fy,
                        percent_normalize=_projection_percent_normalize,
                    )
                    scenario_values[scenario_name] = values
                    missing_base_periods = sorted(set(values) - set(base_values))
                    if missing_base_periods:
                        warnings.append(
                            seed_projection_warning(
                                rate_key=rate_key,
                                kind="base_period_values_missing",
                                scenario=scenario_name,
                                detail=(
                                    f"Base scenario row {base_item.id!r} lacks periods "
                                    f"{','.join(str(period) for period in missing_base_periods)}; "
                                    f"skipped {scenario_name} seed"
                                ),
                                last_provenance=projection_provenance_to_dict(
                                    scenario_entry.provenance
                                ),
                            )
                        )
                        skip_scenario_seed.add(scenario_name)
                if {"bull", "bear"} <= set(scenario_values):
                    ordering_issues = scenario_ordering_violations_by_case(
                        bull_values=scenario_values["bull"],
                        base_values=base_values,
                        bear_values=scenario_values["bear"],
                    )
                    for scenario_name, details in ordering_issues.items():
                        scenario_entry = projection_entry.scenarios[scenario_name]
                        warnings.append(
                            seed_projection_warning(
                                rate_key=rate_key,
                                kind="scenario_ordering_violation",
                                scenario=scenario_name,
                                detail=";".join(details[:5]),
                                last_provenance=projection_provenance_to_dict(
                                    scenario_entry.provenance
                                ),
                            )
                        )
                        skip_scenario_seed.add(scenario_name)

        for scenario_name in ("bull", "bear"):
            if scenario_name in skip_scenario_seed:
                continue
            scenario_entry = projection_entry.scenarios.get(scenario_name)
            if scenario_entry is None:
                continue
            scenario_item = scenario_items.get(scenario_name)
            if scenario_item is None:
                scenario_item = resolve_scenario_flex_row(model, item.id, scenario_name)
            if scenario_item is None:
                orphans.append(
                    orphaned_projection(
                        rate_key=rate_key,
                        reason=f"{scenario_name}_flex_row_not_found",
                        detail=f"No Scenarios sheet flex row found for {scenario_name} of {rate_key!r}",
                        last_provenance=projection_provenance_to_dict(
                            scenario_entry.provenance
                        ),
                    )
                )
                continue
            if scenario_item.overrides is None:
                scenario_item.overrides = {}
            values = scenario_values.get(scenario_name)
            if values is None:
                values = projection_entry_values_by_period(
                    model=model,
                    item_id=scenario_item.id,
                    scenario_entry=scenario_entry,
                    most_recent_fy=most_recent_fy,
                    percent_normalize=_projection_percent_normalize,
                )
            for period, normalized in values.items():
                scenario_item.overrides[period] = formula_spec(
                    type=formula_type.constant,
                    params={"value": normalized},
                )
            seeded_count += 1

    return seed_projections_result(
        seeded_count=seeded_count,
        orphans=orphans,
        warnings=warnings,
        total_rate_keys=total_rate_keys,
        schema_version_seen=schema_version_seen_value,
    )
