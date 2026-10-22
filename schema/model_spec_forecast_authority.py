"""Exact accepted-FAS coverage of BusinessModel forecast controls."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any, Literal, NoReturn, TypeGuard, TypedDict

from .build_model_product_contracts import ModelSpec
from .business_model import expr_is_input_row
from .business_model_revision import BusinessModelRevision
from .driver_resolver import resolve_driver_key


class _ModelSpecAssumptionCoverageRow(TypedDict):
    canonical_driver_key: str
    resolved_item_id: str
    periods: Sequence[int]
    source_kind: Literal["model_spec_assumption"]
    assumption_id: str | None


def _is_model_spec_assumption_coverage_row(
    row: Mapping[str, object],
) -> TypeGuard[_ModelSpecAssumptionCoverageRow]:
    return row.get("source_kind") == "model_spec_assumption"


class ModelSpecForecastAuthorityError(ValueError):
    """The exact ModelSpec forecast projection does not close its BMR plan."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _fail(code: str, message: str) -> NoReturn:
    raise ModelSpecForecastAuthorityError(code, message)


def _iter_nodes(nodes: Sequence[Any]):
    for node in nodes:
        yield node
        yield from _iter_nodes(node.children or ())


def _forecast_selector(
    entry: Any,
    *,
    nodes_by_key: Mapping[tuple[str, str], Any],
) -> str:
    existing = str(entry.existing_driver_key or "").strip()
    if existing:
        return existing
    node_key = (str(entry.segment_id), str(entry.driver_node_id))
    node = nodes_by_key.get(node_key)
    if node is None:
        _fail(
            "model_spec_forecast_authority_node_missing",
            f"required driver node {node_key!r} is absent from the exact BMR",
        )
    expression = node.driver
    if expression.type == "growth":
        return (
            f"{entry.segment_id}.{entry.driver_node_id}."
            f"{expression.params.rate_key}"
        )
    if expr_is_input_row(expression, node.compile_to.target_type):
        return f"{entry.segment_id}.{entry.driver_node_id}"
    _fail(
        "model_spec_forecast_authority_selector_unsupported",
        f"required driver node {node_key!r} has no forecast input selector",
    )


def project_model_spec_forecast_driver_coverage(
    model_spec: ModelSpec,
    business_model_revision: BusinessModelRevision,
    *,
    compiled_registry: Any,
    model_spec_seed_coverage: Sequence[Mapping[str, object]],
) -> dict[str, tuple[int, ...]]:
    """Bind accepted-FAS natural controls to exact BMR driver identities.

    This is readiness lineage only. It deliberately does not manufacture a
    canonical control-snapshot curve: growth-node stable identities describe
    the economic node, while the accepted FAS writes its separate rate input.
    """

    if model_spec.forecast_assumption_set_binding is None:
        return {}
    if model_spec.business_model_revision_ref != business_model_revision.ref:
        _fail(
            "model_spec_forecast_authority_revision_mismatch",
            "accepted FAS ModelSpec does not bind the exact BusinessModel revision",
        )

    nodes_by_key: dict[tuple[str, str], Any] = {}
    for segment in business_model_revision.business_model.segments:
        for node in _iter_nodes(segment.revenue_model.decomposition):
            key = (segment.id, node.id)
            if key in nodes_by_key:
                _fail(
                    "model_spec_forecast_authority_node_duplicate",
                    f"BusinessModel repeats forecast node {key!r}",
                )
            nodes_by_key[key] = node

    assumptions = {row.driver: row for row in model_spec.forecast_assumptions}
    if len(assumptions) != len(model_spec.forecast_assumptions):
        _fail(
            "model_spec_forecast_authority_assumption_duplicate",
            "ModelSpec repeats a forecast control selector",
        )
    coverage_by_selector: dict[str, list[_ModelSpecAssumptionCoverageRow]] = {}
    for row in model_spec_seed_coverage:
        if not _is_model_spec_assumption_coverage_row(row):
            continue
        selector = str(row.get("canonical_driver_key") or "").strip()
        if selector:
            coverage_by_selector.setdefault(selector, []).append(row)

    result: dict[str, tuple[int, ...]] = {}
    for entry in business_model_revision.driver_assumption_plan.entries:
        if not entry.base_case_required:
            continue
        selector = _forecast_selector(entry, nodes_by_key=nodes_by_key)
        assumption = assumptions.get(selector)
        rows = coverage_by_selector.get(selector, ())
        if assumption is None or len(rows) != 1:
            _fail(
                "model_spec_forecast_authority_coverage_missing",
                f"required forecast selector {selector!r} lacks one exact FAS compile row",
            )
        row = rows[0]
        expected_target = resolve_driver_key(
            selector,
            compiled_registry=compiled_registry,
        )
        periods = tuple(int(period) for period in row.get("periods", ()))
        expected_periods = tuple(
            period.fiscal_year
            for period in model_spec.fiscal_axis.forecast_periods
        )
        if (
            str(row.get("resolved_item_id") or "") != expected_target
            or str(row.get("assumption_id") or "")
            != str(assumption.assumption_id or "")
            or periods != expected_periods
        ):
            _fail(
                "model_spec_forecast_authority_coverage_mismatch",
                f"compiled forecast selector {selector!r} differs from exact FAS authority",
            )
        if entry.driver_id in result:
            _fail(
                "model_spec_forecast_authority_driver_duplicate",
                f"BusinessModel repeats stable driver {entry.driver_id!r}",
            )
        result[entry.driver_id] = periods
    return result


__all__ = [
    "ModelSpecForecastAuthorityError",
    "project_model_spec_forecast_driver_coverage",
]
