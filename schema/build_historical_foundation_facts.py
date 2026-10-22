"""Apply the accepted-HFF total fact projection to compiled historical inputs.

Stage A step A-4 (plan §5 A-4, §3.2: `docs/design/
build-model-stage-a-join-deletion-implementation-plan-2026-08-18.md`;
design §5.4a): the application boundary reads the required-role catalog's
HFF fact plane (`schema/templates/required_role_catalog.json`, whose
`hff_fact_items` namespace absorbed the retired `_TARGETS_BY_ROLE` hand map
at step A-4 and the second authored store at D-4.1) plus the total
bound-snapshot projection carried on the ModelSpec. Facts land by role
binding, never by authored mapping rows. An observation applies to its
exact catalog targets, or it belongs to a series a segment-revenue seed
draws on (`model_spec_segment_revenue_series`, the one owner
`segment_revenue_series` over this projection, applied onto the revenue
node by `apply_model_spec_historical_driver_seeds`). A role the catalog
aliases to another lands through its home's targets (materialized at
catalog decode). A projection row whose role the current catalog binds to
no HFF target at all is a spec assembled against an earlier catalog; the
filed value lands through that role's filing fact, so the row is logged
and skipped rather than refused.
"""

from __future__ import annotations

from decimal import Decimal
import logging
from typing import NoReturn

from .build_historical_driver_seeds import model_spec_segment_revenue_series
from .build_model_product_contracts import ModelSpec, source_measure
from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ValueCell,
    ValueProvenance,
    ValueSeries,
)
from industry_slice_core.industry_types import Unit
from .required_role_catalog import load_required_role_catalog


class HistoricalFoundationFactCompileError(ValueError):
    """A projected HFF fact cannot be safely applied to compiler state."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


def _fail(code: str, message: str) -> NoReturn:
    raise HistoricalFoundationFactCompileError(code, message)


def _semantic_role(ticker: str, semantic_id: str) -> str:
    prefix = f"{ticker.lower()}.observation."
    if not semantic_id.startswith(prefix):
        _fail(
            "historical_foundation_fact_semantic_id_invalid",
            f"projected semantic id {semantic_id!r} is outside {prefix!r}",
        )
    return semantic_id.removeprefix(prefix)


def apply_model_spec_historical_foundation_facts(
    model: FinancialModel,
    model_spec: ModelSpec,
) -> list[dict[str, object]]:
    """Apply the total HFF fact projection to its exact role-catalog targets."""

    projection = model_spec.historical_foundation_fact_projection
    if projection is None:
        if model_spec.forecast_assumption_set_binding is not None:
            # A FAS binding proves an accepted HFF entered assembly; after
            # step A-4 assembly always derives the total fact projection
            # from that foundation (no `None` escape). A bound spec without
            # the projection is a stale pre-cut artifact.
            _fail(
                "historical_foundation_fact_projection_rebuild_required",
                "accepted forecast/HFF lineage requires the total historical fact projection",
            )
        return []

    historical_periods = set(model.time_structure.historical_periods)
    # A requirement a segment-revenue seed's series draws on is consumed by
    # the driver-seed pass, which writes that series onto the revenue node.
    series_requirement_ids = {
        requirement_id
        for terms in model_spec_segment_revenue_series(model_spec).values()
        for _, requirement_id in terms
    }
    coverage: list[dict[str, object]] = []
    applied_cells: set[tuple[str, int]] = set()
    for observation in projection.observations:
        role = _semantic_role(model_spec.ticker, observation.semantic_id)
        item_ids = load_required_role_catalog().hff_fact_item_ids(role)
        if not item_ids:
            if observation.requirement_id not in series_requirement_ids:
                logging.info(
                    "HFF observation %s (%s) has no hff_fact_items target in the current catalog; skipped",
                    observation.semantic_id,
                    observation.requirement_id,
                )
            continue
        period = int(observation.period_id.removeprefix("FY"))
        if period not in historical_periods:
            _fail(
                "historical_foundation_fact_period_invalid",
                f"{observation.fact.fact_id!r} is outside the compiled historical axis",
            )
        measure = source_measure(observation.fact.unit)
        if measure is None:
            _fail(
                "historical_foundation_fact_unit_unsupported",
                f"{observation.fact.fact_id!r} has unsupported unit {observation.fact.unit!r}",
            )
        for item_id in item_ids:
            try:
                item = model.get_item(item_id)
            except KeyError as exc:
                raise HistoricalFoundationFactCompileError(
                    "historical_foundation_fact_target_missing",
                    f"exact target {item_id!r} is absent for {role!r}",
                ) from exc
            source_scale = measure.model_scale or observation.fact.scale
            if item.unit is not measure.model_unit or item.model_scale != source_scale:
                _fail(
                    "historical_foundation_fact_target_type_mismatch",
                    f"exact target {item_id!r} differs in unit or scale",
                )
            identity = (item.id, period)
            value = observation.fact.value
            if identity in applied_cells:
                existing = None if item.values is None else item.values.values.get(period)
                if existing is not None and existing.value is not None and Decimal(str(existing.value)) == value:
                    logging.info(
                        "Skipping duplicate HFF fact %s targeting %s/FY%d with identical value %s",
                        observation.fact.fact_id,
                        item.id,
                        period,
                        value,
                    )
                    continue
                _fail(
                    "historical_foundation_fact_target_duplicate",
                    f"multiple HFF facts target {item.id!r}/FY{period}",
                )
            applied_cells.add(identity)
            value = observation.fact.value
            existing = None if item.values is None else item.values.values.get(period)
            # The accepted/current HFF is the normalization authority for this
            # exact semantic target.  Legacy SourceBundle population may have
            # installed a lower-quality vendor fallback (including a reported
            # zero for an EDGAR-observed balance); replace that value.  A
            # different value already attributed to this same authority is a
            # genuine replay conflict and must remain fail-closed.
            if (
                existing is not None
                and existing.provenance is ValueProvenance.historical_foundation
                and (
                    existing.value is None
                    or Decimal(str(existing.value)) != value
                )
            ):
                _fail(
                    "historical_foundation_fact_legacy_conflict",
                    f"legacy value conflicts with {observation.fact.fact_id!r} at {item.id!r}/FY{period}",
                )
            if item.values is None:
                item.values = ValueSeries()
            note = (
                "accepted_historical_foundation_fact:"
                f"{projection.historical_foundation_artifact_id}:"
                f"{observation.fact.fact_id}"
            )
            item.values.values[period] = ValueCell(
                period=period,
                value=float(value),
                provenance=ValueProvenance.historical_foundation,
                note=note,
            )
            if item.overrides is None:
                item.overrides = {}
            item.overrides[period] = FormulaSpec(
                type=FormulaType.constant,
                params={"value": float(value)},
                note=note,
            )
            coverage.append(
                {
                    "source_kind": "historical_foundation_fact",
                    "historical_foundation_artifact_id": (
                        projection.historical_foundation_artifact_id
                    ),
                    "historical_foundation_content_sha256": (
                        projection.historical_foundation_content_sha256
                    ),
                    "requirement_id": observation.requirement_id,
                    "semantic_id": observation.semantic_id,
                    "fact_id": observation.fact.fact_id,
                    "evidence_id": observation.evidence.evidence_id,
                    "resolved_item_id": item.id,
                    "periods": [period],
                    "source_id": str(observation.evidence.source_id),
                }
            )
    return coverage


__all__ = [
    "HistoricalFoundationFactCompileError",
    "apply_model_spec_historical_foundation_facts",
]
