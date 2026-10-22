"""Truthful final-boundary audit for canonical schedule inputs."""

from __future__ import annotations

from typing import Any, Mapping

from .build_model_product_contracts import (
    ProvenanceMap,
    ScheduleInputAudit,
    ScheduleInputAuditRow,
    ScheduleInputFamily,
    ScheduleInputOrigin,
    ScheduleInputPeriodAudit,
)
from .dependency_graph import DependencyGraphNumberValue
from .model_number_value import UNRESOLVED_INPUT_STATES
from .renderer_columns import item_owns_cell


SCHEDULE_INPUT_REQUIREMENTS: Mapping[ScheduleInputFamily, tuple[str, ...]] = {
    "capitalized_software": (
        "tpl.a.capital_investments.of_revenue",
        "tpl.a.depreciation_amortization.amortization_of_beginning_capitalized_internal_use_software",
    ),
    "deferred_contract_cost": (
        "tpl.a.balance_sheet_wc.of_operating_line_adjust_link_as_neccesary",
        "tpl.a.other_assets.of_operating_line_adjust_link_as_neccesary",
    ),
    "lease": (
        "tpl.a.other_assets.operating_lease_rou_asset_additions",
        "tpl.a.other_assets.operating_lease_rou_asset_amortization",
        "tpl.a.capital_sources.current_operating_lease_liabilities",
        "tpl.a.capital_sources.operating_lease_liability_additions",
        "tpl.a.capital_sources.operating_lease_liability_payments",
    ),
    "debt": (
        "tpl.a.capital_sources.current_portion_of_long_term_debt",
        "tpl.a.capital_sources.change",
        "tpl.a.other_income_interest.interest_rate",
        "tpl.a.other_income_interest.other_income_expense",
    ),
    "share_count_capital_allocation": (
        "tpl.a.dividends_shares.basic_shares_outstanding",
        "tpl.a.dividends_shares.dilutive_shares_weighted_average",
        "tpl.a.dividends_shares.end_of_period_shares_outstanding",
        "tpl.a.dividends_shares.total_amount",
        "tpl.a.dividends_shares.dividend_paid",
        "tpl.a.dividends_shares.proceeds_from_employee_stock_purchase_plan",
    ),
}


def build_schedule_input_audit(
    *,
    model: Any,
    typed_values: Mapping[str, Mapping[int, DependencyGraphNumberValue]],
    projection_periods: tuple[int, ...],
    working_capital_required: tuple[str, ...] = (),
    provenance: ProvenanceMap | None = None,
    semantic_cells: tuple[Any, ...] = (),
) -> ScheduleInputAudit:
    """Extract every row from one immutable whole-graph typed matrix."""

    # Legacy persistence fixtures can carry a structurally valid model with no
    # projection horizon.  There is no period truth to audit in that case, so
    # preserve the typed artifact while emitting no (necessarily period-bound)
    # rows.
    if not projection_periods:
        return ScheduleInputAudit()

    requirements: dict[ScheduleInputFamily, tuple[str, ...]] = {
        **SCHEDULE_INPUT_REQUIREMENTS,
        "working_capital": tuple(sorted(set(working_capital_required))),
    }
    lineage_by_item_period: dict[tuple[str, int], list[Any]] = {}
    if provenance is not None:
        period_by_cell = {
            (cell.sheet, cell.cell): cell.period
            for cell in semantic_cells
        }
        for lineage in provenance.cells:
            period = period_by_cell.get((lineage.sheet, lineage.cell))
            if period is not None:
                lineage_by_item_period.setdefault(
                    (lineage.semantic_item_id, int(period)), []
                ).append(lineage)

    rows: list[ScheduleInputAuditRow] = []
    for family, row_ids in requirements.items():
        for row_id in row_ids:
            item = model._index.get(row_id)
            public_label = (
                str(getattr(item, "label", "") or "Schedule input").strip()
                if item is not None
                else "Schedule input"
            )
            # A coordinate the row does not own is no cell: retirement removed it, so it is
            # not an unresolved input (``renderer_columns.item_owns_cell``).
            periods = (
                tuple(projection_periods)
                if item is None
                else tuple(
                    period for period in projection_periods
                    if item_owns_cell(model, item, period)
                )
            )
            if not periods:
                continue
            period_rows: list[ScheduleInputPeriodAudit] = []
            origins: list[ScheduleInputOrigin] = []
            for period in periods:
                value = typed_values.get(row_id, {}).get(period)
                if value is None:
                    state = "missing"
                    reason = "source_value_unavailable"
                elif value.state == "known":
                    state = value.state
                    reason = None
                else:
                    state = value.state
                    reason = str(value.primary_reason)
                period_rows.append(
                    ScheduleInputPeriodAudit(
                        period=period,
                        state=state,
                        reason=reason,
                    )
                )
                for lineage in lineage_by_item_period.get((row_id, period), ()):
                    origins.extend(
                        ScheduleInputOrigin(kind="control", reference=value)
                        for value in lineage.assumption_ids
                    )
                    origins.extend(
                        ScheduleInputOrigin(kind="evidence", reference=ref.evidence_id)
                        for ref in lineage.evidence_refs
                    )
                    origins.extend(
                        ScheduleInputOrigin(kind="observation", reference=value)
                        for value in lineage.observation_ids
                    )
                    origins.extend(
                        ScheduleInputOrigin(kind="valuation", reference=ref.observation_id)
                        for ref in lineage.valuation_refs
                    )
            missing_periods = tuple(
                row.period for row in period_rows if row.state in UNRESOLVED_INPUT_STATES
            )
            unique_origins = tuple(
                {
                    (origin.kind, origin.reference): origin
                    for origin in origins
                }.values()
            )
            rows.append(
                ScheduleInputAuditRow(
                    family=family,
                    row_id=row_id,
                    public_label=public_label,
                    state="missing" if missing_periods else "resolvable",
                    periods=tuple(period_rows),
                    missing_periods=missing_periods,
                    origins=unique_origins,
                )
            )
    return ScheduleInputAudit(
        rows=tuple(sorted(rows, key=lambda row: (row.family, row.row_id)))
    )


__all__ = ["SCHEDULE_INPUT_REQUIREMENTS", "build_schedule_input_audit"]
