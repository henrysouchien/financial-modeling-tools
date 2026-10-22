from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from schema.overrides import (
    TickerOverridesSnapshot,
    load_ticker_overrides_snapshot,
)
from schema.segments import SEGMENT_AXES_PRIORITY
from schema.statement_windows import (
    CompiledStatementWindows,
    compile_statement_windows,
    window_requested_years,
)


@dataclass(frozen=True)
class ModelBuildPreflight:
    overrides_snapshot: TickerOverridesSnapshot
    compiled_statement_windows: CompiledStatementWindows
    required_topology_years: frozenset[int]

    @property
    def has_statement_windows(self) -> bool:
        return bool(self.compiled_statement_windows.section_plans)


def prepare_model_build_preflight(
    ticker: str,
    *,
    historical_years: Iterable[int] | None = None,
    overrides_dir: Path | None = None,
    provider_registry: Any | None = None,
) -> ModelBuildPreflight:
    snapshot_kwargs = (
        {"provider_registry": provider_registry}
        if provider_registry is not None
        else {}
    )
    snapshot = load_ticker_overrides_snapshot(
        ticker,
        overrides_dir,
        **snapshot_kwargs,
    )
    active_registry = provider_registry
    registry_kwargs = (
        {"provider_registry": active_registry} if active_registry is not None else {}
    )
    persisted = snapshot.overrides
    compiled = compile_statement_windows(
        ticker=ticker,
        statement_windows=(
            persisted.statement_windows if persisted is not None else {}
        ),
        existing_semantic_rows=(
            persisted.semantic_rows if persisted is not None else {}
        ),
        source_store_hash=snapshot.store_hash,
        **registry_kwargs,
    )
    configured_topology_years = frozenset(
        year
        for plan in compiled.section_plans.values()
        for year in plan.window.fiscal_years
    )
    requested_years = frozenset(int(year) for year in (historical_years or ()))
    if requested_years and compiled.section_plans:
        for plan in compiled.section_plans.values():
            window_requested_years(plan.window, tuple(sorted(requested_years)))
    required_topology_years = (
        requested_years
        if requested_years and compiled.section_plans
        else configured_topology_years
    )
    return ModelBuildPreflight(
        overrides_snapshot=snapshot,
        compiled_statement_windows=compiled,
        required_topology_years=required_topology_years,
    )


def serialize_year_values(
    values: Mapping[int, float | int | str] | None,
) -> dict[str, float]:
    return {
        str(int(year)): float(value) for year, value in sorted((values or {}).items())
    }


def axis_priority(
    axis: str | None,
    *,
    axes_priority: tuple[str, ...] | list[str] = SEGMENT_AXES_PRIORITY,
) -> int | None:
    if axis is None or axis not in axes_priority:
        return None
    return list(axes_priority).index(axis) + 1


__all__ = [
    "axis_priority",
    "prepare_model_build_preflight",
    "serialize_year_values",
]
