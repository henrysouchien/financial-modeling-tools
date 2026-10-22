"""Core model bundle loading primitives for schema tools."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

from . import serialization
from .dependency_graph import DependencyGraph
from .model_bundle_paths import classify_platform_managed_model_bundle_path
from .models import FinancialModel, ItemType, LineItem
from .reader import read_model
from .templates.model_scale import assert_template_semantic_units
from .valuation_schema_invariant import (
    assert_valuation_template_schema,
    should_enforce_valuation_template_schema,
)


@dataclass
class _ModelBundle:
    model: FinancialModel
    graph: DependencyGraph
    base_results: serialization.BaseResults
    all_items: List[LineItem]
    derived_ids: Set[str]
    source: str
    file_signature: Tuple[int, int] | None


def _file_signature(file_path: str) -> Tuple[int, int] | None:
    try:
        stat = Path(file_path).stat()
    except OSError:
        return None
    return (stat.st_mtime_ns, stat.st_size)


def _build_bundle(
    file_path: str,
    *,
    model: Optional[FinancialModel] = None,
    cutoff: int,
    persist: bool = False,
    refresh_stale_sidecar: bool = True,
) -> _ModelBundle:
    cached_base_results: serialization.BaseResults | None = None
    sidecar_needs_refresh = False
    model_source = "explicit" if model is not None else "unknown"
    if model is None:
        sidecar_hit = serialization.try_load_sidecar(file_path)
        if sidecar_hit is not None:
            model, cached_base_results = sidecar_hit
            sidecar_needs_refresh = cached_base_results is None
            model_source = "sidecar_recomputed" if sidecar_needs_refresh else "sidecar"
        else:
            if classify_platform_managed_model_bundle_path(file_path) is not None:
                raise serialization.ModelBundleRebuildRequired(
                    "platform-managed model bundle is missing an exact current schema sidecar; "
                    "offline rebuild is required and lossy workbook fallback is forbidden"
                )
            disk_hit = serialization.try_load(file_path, cutoff)
            if disk_hit is not None:
                model, cached_base_results = disk_hit
                model_source = "disk_cache"

    parsed_fresh = False
    if model is None:
        loaded = read_model(file_path, mode="full", historical_cutoff_year=cutoff)
        if not isinstance(loaded, FinancialModel):
            raise TypeError("read_model(..., mode='full') did not return FinancialModel")
        model = loaded
        parsed_fresh = True
        model_source = "parsed_workbook"

    try:
        assert_template_semantic_units(
            model,
            origin=f"schema.tools.load.{model_source}",
        )
    except ValueError as exc:
        raise serialization.ModelBundleRebuildRequired(str(exc)) from exc

    _assert_loaded_valuation_template_schema(
        model,
        file_path=file_path,
        origin=f"schema.tools.load.{model_source}",
    )

    graph = DependencyGraph()
    graph.build(model)

    all_items = list(model._index.values())
    derived_ids = {item.id for item in all_items if item.item_type == ItemType.derived}
    if cached_base_results is not None:
        base_results = cached_base_results
    else:
        base_results = graph.compute({}, recompute=derived_ids)

    if (
        (parsed_fresh and refresh_stale_sidecar)
        or (sidecar_needs_refresh and refresh_stale_sidecar)
        or (persist and model is not None)
    ):
        serialization.save(file_path, cutoff, model, base_results)
    if (sidecar_needs_refresh and refresh_stale_sidecar) or (
        persist and model is not None
    ):
        if should_enforce_valuation_template_schema(model, workbook_path=file_path):
            assert_valuation_template_schema(
                model,
                origin="schema.tools.load.persist_sidecar",
                workbook_path=file_path,
                sidecar_path=serialization.sidecar_path(file_path),
                module_path=__file__,
            )
        serialization.save_sidecar(file_path, model, base_results)

    return _ModelBundle(
        model=model,
        graph=graph,
        base_results=base_results,
        all_items=all_items,
        derived_ids=derived_ids,
        source=model_source,
        file_signature=_file_signature(file_path),
    )


def _assert_loaded_valuation_template_schema(
    model: FinancialModel,
    *,
    file_path: str,
    origin: str,
) -> None:
    if not should_enforce_valuation_template_schema(model, workbook_path=file_path):
        return
    assert_valuation_template_schema(
        model,
        origin=origin,
        workbook_path=file_path,
        sidecar_path=serialization.sidecar_path(file_path),
        module_path=__file__,
    )
