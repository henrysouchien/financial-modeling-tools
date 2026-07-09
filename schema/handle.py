"""Value-oriented handle API for schema financial models."""

from __future__ import annotations

import copy
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Mapping

from schema.cas import (
    _strip_revision_exclusions,
    canonical_model_hash,
    workbook_bundle_hash,
)
from schema.dependency_graph import DependencyGraph
from schema.load_core import (
    _ModelBundle,
    _assert_loaded_valuation_template_schema,
    _build_bundle,
    _file_signature,
)
from schema.models import FinancialModel, ItemType

if TYPE_CHECKING:
    from schema.modify_core import Operation


_handle_memo: dict[tuple[str, int], "ModelHandle"] = {}


_canonical_model_hash = canonical_model_hash


def _revision_for(model: FinancialModel) -> str:
    return workbook_bundle_hash(model)


def _copy_computed(
    computed: Mapping[str, Mapping[int, float]],
) -> dict[str, dict[int, float]]:
    return {item_id: dict(values) for item_id, values in computed.items()}


@dataclass(frozen=True)
class ModelHandle:
    model: FinancialModel
    computed: Mapping[str, Mapping[int, float]]
    revision: str
    source: str = field(default="handle", compare=False)
    file_signature: tuple[int, int] | None = field(default=None, compare=False)
    _graph_cache: dict[str, DependencyGraph] = field(
        default_factory=dict,
        init=False,
        repr=False,
        compare=False,
    )

    @property
    def graph(self) -> DependencyGraph:
        graph = self._graph_cache.get("graph")
        if graph is None:
            graph = DependencyGraph()
            graph.build(self.model)
            self._graph_cache["graph"] = graph
        return graph

    @property
    def all_items(self) -> list:
        return list(self.model._index.values())

    @property
    def base_results(self) -> Mapping[str, Mapping[int, float]]:
        return self.computed

    @property
    def derived_ids(self) -> set[str]:
        return {item.id for item in self.all_items if item.item_type == ItemType.derived}

    @classmethod
    def from_bundle(cls, bundle: _ModelBundle) -> "ModelHandle":
        handle = cls(
            model=bundle.model,
            computed=bundle.base_results,
            revision=_revision_for(bundle.model),
            source=bundle.source,
            file_signature=bundle.file_signature,
        )
        handle._graph_cache["graph"] = bundle.graph
        return handle

    def to_bundle(self) -> _ModelBundle:
        return _ModelBundle(
            model=self.model,
            graph=self.graph,
            base_results=_copy_computed(self.computed),
            all_items=self.all_items,
            derived_ids=self.derived_ids,
            source=self.source,
            file_signature=self.file_signature,
        )


def evaluate(handle: ModelHandle, ops: list[Operation]) -> ModelHandle:
    """Apply in-memory operations to a handle and return a freshly computed handle."""

    from schema import modify_core
    from schema.modify_core import ModifyError, Operation
    from schema.modify_layout import _assert_layout_integrity

    parsed_ops = [
        op if isinstance(op, Operation) else Operation.model_validate(op)
        for op in ops
    ]
    snapshot = copy.deepcopy(handle.model)

    if not parsed_ops:
        return ModelHandle(
            model=snapshot,
            computed=_copy_computed(handle.computed),
            revision=handle.revision,
            source=handle.source,
            file_signature=handle.file_signature,
        )

    for op in parsed_ops:
        result = modify_core._dispatch_op(snapshot, op, file_path="")
        if result.status == "error":
            reason = result.reason or "operation_failed"
            raise ModifyError(f"{op.type.value} failed for {op.item_id!r}: {reason}")

    snapshot.build_index()
    _assert_layout_integrity(snapshot)

    graph = DependencyGraph()
    graph.build(snapshot)
    derived_ids = {
        item.id
        for item in snapshot._index.values()
        if item.item_type == ItemType.derived
    }
    base_results = graph.compute({}, recompute=derived_ids)

    return ModelHandle(
        model=snapshot,
        computed=base_results,
        revision=_revision_for(snapshot),
        source=handle.source,
        file_signature=handle.file_signature,
    )


def load_handle(
    file_path: str,
    *,
    model: FinancialModel | None = None,
    historical_cutoff_year: int | None = None,
    persist: bool = False,
) -> ModelHandle:
    cutoff = historical_cutoff_year if historical_cutoff_year is not None else datetime.now().year
    key = (file_path, cutoff)
    signature = _file_signature(file_path)

    if model is None and key in _handle_memo:
        cached = _handle_memo[key]
        if cached.file_signature == signature:
            _assert_loaded_valuation_template_schema(
                cached.model,
                file_path=file_path,
                origin=f"schema.tools.load.cache_hit.{cached.source}",
            )
            return cached
        del _handle_memo[key]

    bundle = _build_bundle(file_path, model=model, cutoff=cutoff, persist=persist)
    handle = ModelHandle.from_bundle(bundle)
    _handle_memo[key] = handle
    return handle


def peek_handle(
    file_path: str,
    historical_cutoff_year: int | None = None,
) -> ModelHandle | None:
    cutoff = historical_cutoff_year if historical_cutoff_year is not None else datetime.now().year
    return _handle_memo.get((file_path, cutoff))


def evict_handle(
    file_path: str,
    historical_cutoff_year: int | None = None,
) -> None:
    cutoff = historical_cutoff_year if historical_cutoff_year is not None else datetime.now().year
    _handle_memo.pop((file_path, cutoff), None)


__all__ = [
    "ModelHandle",
    "_handle_memo",
    "_canonical_model_hash",
    "evaluate",
    "evict_handle",
    "load_handle",
    "peek_handle",
]
