"""Project a checked candidate's recalculated rows against its pinned inputs.

``project_check_rows`` is the read side of the build agent's no-persist
``check_model`` door. It reads two things only: the SourceBundle the candidate
compiled from, and the recalculated semantic values the offline validation
computed from the rendered workbook (``OfflineValidation.recalculated_values``).
It is a projection, not a verdict: it adds no diagnostic and changes no
status. Rendered values come from the recalculated workbook only, never from
a model item's captured value, which is the reported number the formula row
may not reproduce.

This module also owns the pairing of each issuer bridge stage with its model
row. ``non_gaap_bridge_findings`` reads that same pairing for the one
evaluator (``validate_offline_build_artifacts``), so ``check_model`` and the
verdict compare the same rows against the same filed facts.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal
from typing import TYPE_CHECKING, Any, Iterable, Literal, Mapping

from pydantic import BaseModel, ConfigDict
from value_semantics_core import FiscalYearPeriod

from .build_findings import make_finding
from .finding_vocabulary import Finding
from .build_model_product_contracts import (
    HistoricalBasisFact,
    SourceBundle,
)
from .valuation_evidence_derivations import derive_covered_market_inputs
from schema.provider_port.adapters.edgar.wire.non_gaap_bridge_v2 import bridge_row_key
from .stable_control_identity import FiscalAxis

if TYPE_CHECKING:
    from .analysis import Implied, Sensitivity, Variance

#: The key statement rows ``check_model`` returns by period. One declaration.
CHECK_KEY_ITEMS: tuple[str, ...] = (
    "tpl.fm.income_statement.total_revenue",
    "tpl.fm.income_statement.eps",
    "tpl.fm.adjusted_earnings.adjusted_ebitda",
    "tpl.fm.adjusted_earnings.adjusted_net_income",
    "tpl.fm.adjusted_earnings.adjusted_eps",
    "tpl.fm.cash_flow.operating_cash_flow",
    "tpl.fm.cash_flow.free_cash_flow",
    "tpl.fm.balance_sheet.total_assets",
    "tpl.fm.balance_sheet.cash_and_marketable_securities",
)

#: The model rows each issuer bridge reconciles to, by its output metric: its
#: first subtotal's row (``None`` when the model has none) and its output's.
#: A bridge of any other metric stays on the rail as facts and is not
#: compared.
_BRIDGE_STAGE_ITEMS: Mapping[str, tuple[str | None, str]] = {
    "adjusted_ebitda": (
        "tpl.fm.adjusted_earnings.ebitda",
        "tpl.fm.adjusted_earnings.adjusted_ebitda",
    ),
    "non_gaap_operating_income": (
        None,
        "tpl.fm.adjusted_earnings.adjusted_operating_income",
    ),
    "non_gaap_net_income": (
        None,
        "tpl.fm.adjusted_earnings.adjusted_net_income",
    ),
    "non_gaap_income_from_operations": (
        None,
        "tpl.fm.adjusted_earnings.adjusted_operating_income",
    ),
}

#: The ``check_model`` sections the door's ``section`` selector returns whole: the diagnostic
#: report (held back from the default body), the two pre-publish reads over the sheet, and the
#: four tables the unfiltered answer holds back (``historical_spread``, ``rendered`` and
#: ``reconciliation`` -- every row, not the ``item_id``-scoped ones) or a finding drill-in omits
#: (``filed_line_unclaimed``).
CheckSection = Literal[
    "diagnostic_report",
    "sensitivity",
    "references",
    "historical_spread",
    "rendered",
    "reconciliation",
    "filed_line_unclaimed",
]
CHECK_SECTIONS: tuple[CheckSection, ...] = (
    "diagnostic_report",
    "sensitivity",
    "references",
    "historical_spread",
    "rendered",
    "reconciliation",
    "filed_line_unclaimed",
)
#: Rows of the ``sensitivity`` section: the judgment rows the DCF price responds to most.
CHECK_SENSITIVITY_ROWS = 10
#: Findings per ``check_model`` answer. A finding's message is at most 1,000
#: characters, so a page stays well inside the runner's 60,000-char bound;
#: ``finding_offset`` reads the next page.
CHECK_FINDINGS_PAGE_SIZE = 25


@dataclass(frozen=True, slots=True)
class _BridgeStage:
    """One filed bridge stage paired with the model row that must equal it."""

    item_id: str
    filed: HistoricalBasisFact
    #: Half a unit of the filed table per operand the stage sums.
    tolerance: Decimal
    stage_residual: Decimal = Decimal(0)

def _bridge_stages(facts: Iterable[HistoricalBasisFact]) -> tuple[_BridgeStage, ...]:
    """Each mapped bridge's subtotal and output stage, in fact order.

    The filed table's reporting unit is the finest decimal place its facts
    carry (a thousands table in model millions: 0.001). Each operand a stage
    sums can round by half that unit: the subtotal sums the starting fact and
    the components above it, the output the starting fact and every
    component. A bridge whose output metric has no model row is not staged.
    """

    facts = tuple(facts)
    facts_by_id = {fact.fact_id: fact for fact in facts}
    stages: list[_BridgeStage] = []
    for output in facts:
        binding = output.reconciliation
        if binding is None or binding.output_metric not in _BRIDGE_STAGE_ITEMS:
            continue
        subtotal_item, output_item = _BRIDGE_STAGE_ITEMS[binding.output_metric]
        table = (
            *(facts_by_id[fact_id] for fact_id in binding.dependency_fact_ids),
            output,
        )
        exponent = min(int(fact.value.as_tuple().exponent) for fact in table)
        half_unit = Decimal(1).scaleb(exponent) / 2
        if subtotal_item is not None and binding.subtotal_fact_ids:
            if any(
                bridge_row_key(
                    facts_by_id[fact_id].line.tag.removeprefix("issuer:")
                ) == ("adjusted_ebitda", "depreciation_and_amortization")
                for fact_id in binding.component_fact_ids[
                    : binding.subtotal_component_counts[0]
                ]
            ):
                stages.append(_BridgeStage(
                    subtotal_item,
                    facts_by_id[binding.subtotal_fact_ids[0]],
                    max(
                        half_unit * (1 + binding.subtotal_component_counts[0]),
                        binding.subtotal_half_ulp_budgets[0]
                        if binding.subtotal_half_ulp_budgets
                        else Decimal(0),
                    ),
                    binding.subtotal_residuals[0]
                    if binding.subtotal_residuals
                    else Decimal(0),
                ))
        stages.append(_BridgeStage(
            output_item,
            output,
            max(
                half_unit * (1 + len(binding.component_fact_ids)),
                binding.half_ulp_budget,
            ),
            binding.output_residual,
        ))
    return tuple(stages)


ReconciliationBasis = Literal[
    "statement_fact",
    "non_gaap_bridge",
    "not_in_pin",
]


class _CheckRow(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class RenderedCheckRow(_CheckRow):
    item_id: str
    status: Literal["rendered", "absent"]
    values: dict[str, float | None]


class ReconciliationCheckRow(_CheckRow):
    item_id: str
    period: str
    rendered: float | None
    reported: float | None
    delta: float | None
    reported_ref: str | None
    basis: ReconciliationBasis
    reason: str | None = None


class CheckRows(_CheckRow):
    rendered: tuple[RenderedCheckRow, ...]
    reconciliation: tuple[ReconciliationCheckRow, ...]

    def reconciliation_item_ids(self) -> tuple[str, ...]:
        """Every reconciled model item, in first-row order: the selector values."""

        return tuple(dict.fromkeys(row.item_id for row in self.reconciliation))

    def reconciliation_summary(self) -> dict[str, object]:
        """Row counts in total and by basis; the rows sit behind ``item_id``."""

        by_basis: dict[str, int] = {}
        for row in self.reconciliation:
            by_basis[row.basis] = by_basis.get(row.basis, 0) + 1
        return {"rows": len(self.reconciliation), "by_basis": by_basis}

    def reconciliation_for(self, item_id: str) -> tuple[ReconciliationCheckRow, ...]:
        """Every reconciliation row of one model item, all periods."""

        return tuple(row for row in self.reconciliation if row.item_id == item_id)


def _row(
    *,
    item_id: str,
    period: str,
    rendered: float | None,
    reported: Decimal | None,
    reported_ref: str | None,
    basis: ReconciliationBasis,
    reason: str | None = None,
) -> ReconciliationCheckRow:
    reported_value = None if reported is None else float(reported)
    return ReconciliationCheckRow(
        item_id=item_id,
        period=period,
        rendered=rendered,
        reported=reported_value,
        delta=(
            None
            if rendered is None or reported_value is None
            else rendered - reported_value
        ),
        reported_ref=reported_ref,
        basis=basis,
        reason=reason,
    )


def project_check_rows(
    source_bundle: SourceBundle,
    recalculated_values: Mapping[str, Mapping[int, float]],
    fiscal_axis: FiscalAxis,
) -> CheckRows:
    """Return the key rows by period and the historical reconciliation."""

    periods = (*fiscal_axis.historical_periods, *fiscal_axis.forecast_periods)
    rendered = tuple(
        RenderedCheckRow(
            item_id=item_id,
            status="rendered" if item_id in recalculated_values else "absent",
            values={
                period.period_id: recalculated_values.get(item_id, {}).get(
                    period.fiscal_year
                )
                for period in periods
            },
        )
        for item_id in CHECK_KEY_ITEMS
    )

    evidence_by_id = {
        evidence.evidence_id: evidence for evidence in source_bundle.evidence
    }
    coverage_by_requirement = {
        coverage.requirement_id: {period.period_id: period for period in coverage.periods}
        for coverage in source_bundle.coverage
    }
    rows: list[ReconciliationCheckRow] = []
    reconciled_items: set[str] = set()
    for requirement in source_bundle.coverage_profile.annual_requirements:
        reconciled_items.add(requirement.model_item)
        coverage = coverage_by_requirement.get(requirement.requirement_id, {})
        values = recalculated_values.get(requirement.model_item, {})
        for period in fiscal_axis.historical_periods:
            if period.period_id not in requirement.period_ids:
                continue
            value = values.get(period.fiscal_year)
            covered = coverage.get(period.period_id)
            if covered is None:
                rows.append(_row(
                    item_id=requirement.model_item,
                    period=period.period_id,
                    rendered=value,
                    reported=None,
                    reported_ref=None,
                    basis="not_in_pin",
                    reason="no coverage entry for this requirement period",
                ))
                continue
            evidence = next(
                (
                    evidence_by_id[evidence_id]
                    for evidence_id in covered.evidence_ids
                    if evidence_id in evidence_by_id
                    and evidence_by_id[evidence_id].period_id == period.period_id
                ),
                None,
            )
            if evidence is None:
                rows.append(_row(
                    item_id=requirement.model_item,
                    period=period.period_id,
                    rendered=value,
                    reported=None,
                    reported_ref=None,
                    basis="not_in_pin",
                    reason=f"coverage disposition {covered.disposition}",
                ))
                continue
            rows.append(_row(
                item_id=requirement.model_item,
                period=period.period_id,
                rendered=value,
                reported=evidence.value,
                reported_ref=evidence.evidence_id,
                basis="statement_fact",
            ))

    historical_by_id = {
        period.period_id: period for period in fiscal_axis.historical_periods
    }
    for stage in _bridge_stages(source_bundle.historical_basis_facts):
        period = historical_by_id.get(stage.filed.period_id)
        if period is None:
            continue
        reconciled_items.add(stage.item_id)
        rows.append(_row(
            item_id=stage.item_id,
            period=stage.filed.period_id,
            rendered=recalculated_values.get(stage.item_id, {}).get(period.fiscal_year),
            reported=stage.filed.value,
            reported_ref=stage.filed.fact_id,
            basis="non_gaap_bridge",
        ))

    for item_id in CHECK_KEY_ITEMS:
        if item_id in reconciled_items:
            continue
        values = recalculated_values.get(item_id, {})
        for period in fiscal_axis.historical_periods:
            rows.append(_row(
                item_id=item_id,
                period=period.period_id,
                rendered=values.get(period.fiscal_year),
                reported=None,
                reported_ref=None,
                basis="not_in_pin",
                reason=(
                    "the pinned SourceBundle carries no reported value or "
                    "issuer bridge output for this line"
                ),
            ))

    return CheckRows(rendered=rendered, reconciliation=tuple(rows))


def non_gaap_bridge_findings(
    historical_basis_facts: Iterable[HistoricalBasisFact],
    recalculated_values: Mapping[str, Mapping[int, float]],
) -> tuple[Finding, ...]:
    """One ``non_gaap_bridge_mismatch`` per bridge row-year the workbook misses.

    Each filed bridge stage (the subtotal and the output) is compared with its
    model row recalculated from the rendered workbook in the same fiscal year.
    A difference beyond the stage's rounding tolerance is an error naming the
    stage that broke. A year the workbook does not render is not compared, and
    a pin with no filed bridge yields no finding.
    """

    findings: list[Finding] = []
    for stage in _bridge_stages(historical_basis_facts):
        period = stage.filed.line.period
        if not isinstance(period, FiscalYearPeriod):
            continue
        rendered = recalculated_values.get(stage.item_id, {}).get(period.fiscal_year)
        if rendered is None:
            continue
        filed = float(stage.filed.value)
        tolerance = float(stage.tolerance)
        if abs(rendered - filed) <= tolerance:
            if stage.stage_residual != 0 and abs(rendered - filed) > 0:
                findings.append(
                    make_finding(
                        "non_gaap_bridge_rounding_gap",
                        message=(
                            f"{stage.item_id} renders {rendered:.3f} in "
                            f"{stage.filed.period_id}; the issuer's filed bridge "
                            f"reports {filed:.3f} ({stage.filed.fact_id}), within "
                            f"the half-ulp rounding budget {tolerance:g}."
                        ),
                        item_id=stage.item_id,
                        periods=(period.fiscal_year,),
                        observed={"rendered": rendered, "filed": filed},
                        expected={"bound": tolerance},
                    )
                )
            continue
        findings.append(
            make_finding(
                "non_gaap_bridge_mismatch",
                message=(
                    f"{stage.item_id} renders {rendered:.3f} in "
                    f"{stage.filed.period_id}; the issuer's filed bridge "
                    f"reports {filed:.3f} ({stage.filed.fact_id}), outside "
                    f"the rounding tolerance {tolerance:g}."
                ),
                item_id=stage.item_id,
                periods=(period.fiscal_year,),
                observed={"rendered": rendered, "filed": filed},
                expected={"bound": tolerance},
            )
        )
    return tuple(findings)


def project_market_input_closes(source_bundle: SourceBundle) -> dict[str, str | None]:
    """The close date of each market input a compile pins.

    Resolves the same covered market inputs ``compile_model_intermediate``
    pins and returns each input's
    observation ``as_of_close`` (``None`` for a blocked input).
    """

    covered = derive_covered_market_inputs(
        source_bundle.valuation_observations,
        source_bundle.cutoff_at,
        source_bundle.market_calendar,
        source_bundle.fiscal_calendar,
        requirements=source_bundle.valuation_requirements,
        focal_ticker=source_bundle.coverage_profile.focal_ticker,
        sources=source_bundle.sources,
        retrieved_at=source_bundle.retrieved_at,
    )
    observations = {
        observation.observation_id: observation
        for observation in source_bundle.valuation_observations
    }
    closes: dict[str, str | None] = {}
    for market_input in (
        covered.stock_price,
        covered.raw_beta,
        covered.risk_free_rate,
        covered.equity_risk_premium,
    ):
        observation = (
            None
            if market_input.observation_id is None
            else observations.get(market_input.observation_id)
        )
        closes[market_input.field_name] = (
            None if observation is None else observation.as_of_close.isoformat()
        )
    return closes


def project_sensitivity_section(
    sensitivities: Iterable["Sensitivity"],
    *,
    rationale_by_subject: Mapping[str, str | None],
    limit: int = CHECK_SENSITIVITY_ROWS,
) -> dict[str, Any]:
    """The ``sensitivity`` section: the judgment rows of ``schema.analysis.sensitize`` (computed
    rows first, ranked by the target's response to a relative bump), cut at ``limit``, each beside
    the reason its Forecast Assumption states for it (``None`` where the row has none)."""
    ranked = tuple(sensitivities)
    first = ranked[0] if ranked else None
    return {
        "target_id": None if first is None else first.target_id,
        "target_period": None if first is None else first.target_period,
        "bump_pct": None if first is None else first.bump_pct,
        "judgment_row_count": len(ranked),
        "computed_count": sum(1 for row in ranked if row.status == "computed"),
        "rows": [
            {
                "subject": row.subject,
                "driver_key": row.driver_key,
                "source": row.source,
                "base_value": row.base_value,
                "bumped_value": row.bumped_value,
                "delta_per_pct": row.delta_per_pct,
                "rank": row.rank,
                "status": row.status,
                "rationale": rationale_by_subject.get(row.subject),
            }
            for row in ranked[:limit]
        ],
    }


def project_reference_section(
    variances: Iterable["Variance"], implied: Iterable["Implied"],
) -> dict[str, Any]:
    """The ``references`` section: the model held against each Reference (``compare`` /
    ``compare_consensus``), one row per compared basis, and beside a Reference ``imply`` solved
    (the price) the judgment rows' implied scale and status."""
    implied_by_reference = {row.reference_id: row for row in implied}
    rows: list[dict[str, Any]] = []
    for variance in variances:
        solved = implied_by_reference.get(variance.reference_id)
        judgments = [] if solved is None else [
            {"driver_key": row.driver_key, "scale": row.scale, "status": row.status}
            for row in solved.judgments
        ]
        identity = {
            "reference_id": variance.reference_id,
            "reference_kind": variance.reference_kind,
            "subject": variance.subject,
            "fiscal_year": variance.fiscal_year,
        }
        if variance.status != "compared":
            rows.append({**identity, "status": variance.status, "reason": variance.reason, "implied": judgments})
            continue
        rows.extend(
            {
                **identity,
                "status": "compared",
                "basis": row.basis,
                "model_item_id": row.model_item_id,
                "model_value": float(row.model_value),
                "reference_value": float(row.reference_value),
                "delta_pct": None if row.delta_pct is None else float(row.delta_pct),
                "implied": judgments,
            }
            for row in variance.rows
        )
    return {"rows": rows}


__all__ = [
    "CHECK_FINDINGS_PAGE_SIZE",
    "CHECK_KEY_ITEMS",
    "CHECK_SECTIONS",
    "CHECK_SENSITIVITY_ROWS",
    "CheckRows",
    "CheckSection",
    "ReconciliationCheckRow",
    "RenderedCheckRow",
    "non_gaap_bridge_findings",
    "project_check_rows",
    "project_market_input_closes",
    "project_reference_section",
    "project_sensitivity_section",
]
