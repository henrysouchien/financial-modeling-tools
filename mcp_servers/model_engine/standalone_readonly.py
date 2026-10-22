"""Fail-closed schema readers for the standalone model-engine surface.

The authenticated gateway owns every durable model-state mutation.  These
wrappers deliberately bypass the process memo and forbid disk-cache or schema
sidecar refresh while retaining the schema tools' in-memory computations.
"""

from __future__ import annotations

from typing import Any

from schema import tools as schema_tools
from schema.tools import ModelToolError
from schema.handle import ModelHandle, load_handle as _load_handle
from schema.models import FinancialModel

from schema.published_model_readback import (
    overlay_published_model_values,
    verified_published_model,
)


_PUBLISHED_MODEL_READ_FUNCTIONS = {
    schema_tools.summarize,
    schema_tools.values,
    schema_tools.find,
    schema_tools.drivers,
    schema_tools.sensitivity,
    schema_tools.scenario,
}


def load_handle(
    file_path: str,
    *,
    model: FinancialModel | None = None,
    historical_cutoff_year: int | None = None,
    persist: bool = False,
    refresh_stale_sidecar: bool = False,
    use_cache: bool = False,
) -> ModelHandle:
    """Load one standalone handle without mutating cache or sidecar state.

    Callers may request only stricter read behavior; supplied loader flags can
    never relax the standalone server's read-only contract.
    """

    del persist, refresh_stale_sidecar, use_cache

    published_model = verified_published_model(file_path) if model is None else None
    return _load_handle(
        file_path,
        model=model if model is not None else published_model,
        historical_cutoff_year=historical_cutoff_year,
        persist=False,
        refresh_stale_sidecar=False,
        use_cache=False,
    )


def load_bundle(
    file_path: str,
    *,
    model: FinancialModel | None = None,
    historical_cutoff_year: int | None = None,
    persist: bool = False,
    refresh_stale_sidecar: bool = False,
    use_cache: bool = False,
) -> Any:
    """Load one standalone bundle without mutating cache or sidecar state."""

    return load_handle(
        file_path,
        model=model,
        historical_cutoff_year=historical_cutoff_year,
        persist=persist,
        refresh_stale_sidecar=refresh_stale_sidecar,
        use_cache=use_cache,
    ).to_bundle()


def _call(function: Any, /, *args: Any, **kwargs: Any) -> Any:
    kwargs["refresh_stale_sidecar"] = False
    kwargs["use_cache"] = False
    if (
        function in _PUBLISHED_MODEL_READ_FUNCTIONS
        and args
        and "model" not in kwargs
    ):
        published_model = verified_published_model(str(args[0]))
        if published_model is not None:
            kwargs["model"] = published_model
    return function(*args, **kwargs)


def summarize(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call(schema_tools.summarize, *args, **kwargs)


def values(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call(schema_tools.values, *args, **kwargs)


def published_values(
    file_path: str,
    item_ids: list[str],
    *,
    periods: str | list[int | str],
    historical_cutoff_year: int | None,
    fallback: dict[str, Any],
) -> dict[str, Any]:
    """Overlay verified canonical values for immutable ModelVersion bundles."""

    return overlay_published_model_values(
        file_path,
        item_ids,
        periods=periods,
        historical_cutoff_year=historical_cutoff_year,
        fallback=fallback,
    )




def find(*args: Any, **kwargs: Any) -> list[dict[str, Any]]:
    return _call(schema_tools.find, *args, **kwargs)


def drivers(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call(schema_tools.drivers, *args, **kwargs)


def sensitivity(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call(schema_tools.sensitivity, *args, **kwargs)


def scenario(*args: Any, **kwargs: Any) -> dict[str, Any]:
    return _call(schema_tools.scenario, *args, **kwargs)


def invalid_override_period_error(*args: Any, **kwargs: Any) -> ModelToolError:
    return _call(schema_tools.invalid_override_period_error, *args, **kwargs)


__all__ = [
    "drivers",
    "find",
    "invalid_override_period_error",
    "load_bundle",
    "load_handle",
    "published_values",
    "scenario",
    "sensitivity",
    "summarize",
    "values",
]
