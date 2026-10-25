"""The Finding vocabulary: one finding type, one severity map, one verdict.

Store (A). Every build-model check reports a :class:`Finding`. Its severity
and ``clearable_by`` are taken from exactly one row of
:data:`FINDING_SEVERITY_MAP`, keyed by code; the map is the recorder, so a
caller's own values are replaced, never refused. :func:`derive_build_verdict`
is the only function that turns a finding list into ``blocked`` /
``ready_with_warnings`` / ``ready``; the
ValidationReport, the ModelVersion status, the semantic document, the report
projection and the Summary sheet all read that one verdict.

A :class:`Declination` (``ModelSpec.declinations``) answers one declinable
warning with a reason and citation; :data:`DECLINABLE_FINDING_CODES` is every
code one may answer. A declination that answers nothing is the ``info``
finding ``declination_unmatched``, never a refusal. The checks that produce
findings and attach declinations are ``schema/build_findings.py``.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Annotated, Final, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationInfo, field_validator, model_validator

from .business_model_revision_ref import BusinessModelRevisionRef
from .thesis_shared_slice import FieldDisposition


FindingSeverity = Literal["error", "warning", "info"]
FindingBasis = Literal["rendered", "semantic", "pinned", "authored"]
FindingClearableBy = Literal["declination", "repair", "none"]
BuildVerdict = Literal["blocked", "ready_with_warnings", "ready"]
FindingScalar = str | int | float | bool | None

FINDING_MESSAGE_MAX_LENGTH: Final = 1000


@dataclass(frozen=True, slots=True)
class FindingRule:
    """The one severity, basis set and clearing route a finding code has."""

    severity: FindingSeverity
    bases: tuple[FindingBasis, ...]
    clearable_by: FindingClearableBy
    #: R2: an uncleared warning with this flag blocks the verdict (publish
    #: refused) until a declination clears it.
    refuse_publish_uncleared: bool = False

    @property
    def default_basis(self) -> FindingBasis:
        return self.bases[0]


def _rows(
    codes: Iterable[str],
    severity: FindingSeverity,
    bases: tuple[FindingBasis, ...],
    clearable_by: FindingClearableBy,
    *,
    refuse_publish_uncleared: bool = False,
) -> dict[str, FindingRule]:
    rule = FindingRule(
        severity=severity,
        bases=bases,
        clearable_by=clearable_by,
        refuse_publish_uncleared=refuse_publish_uncleared,
    )
    return {code: rule for code in codes}


#: The severity oracle (case file ``one-finding-type-one-verdict``). One row
#: per code; a code that is not here cannot be a Finding.
FINDING_SEVERITY_MAP: Final[Mapping[str, FindingRule]] = MappingProxyType(
    {
        # B: source bundle, provenance and offline validation.
        **_rows(("source_absence_evidenced",), "warning", ("pinned",), "declination"),
        **_rows(("missing_source",), "error", ("pinned", "rendered"), "repair"),
        # RULE-I1: a ``set_input`` citing a filing line whose filed value differs from the
        # authored one. A declination of a wrong citation would be a rubber stamp: repair only.
        **_rows(("cited_fact_value_mismatch",), "error", ("pinned", "rendered"), "repair"),
        **_rows(
            ("historical_foundation_evidence_diverges",),
            "error",
            ("pinned",),
            "repair",
        ),
        **_rows(("hff_stale_for_roles",), "warning", ("pinned",), "repair"),
        **_rows(("adapter_assertion",), "warning", ("pinned",), "declination"),
        **_rows(
            ("semantic_value_not_known", "semantic_unit_missing"),
            "error",
            ("semantic",),
            "repair",
        ),
        # Spec §7 "Provenance on facts" under container plan §5.1: a cell in a
        # historical period the pin binds nothing in (a partial statement
        # binding) is a declared gap, not a missing disclosure; the same cell
        # on a bound period stays ``missing_source`` /
        # ``semantic_value_not_known``. A successor binding adding the window
        # repairs it; the analyst may decline the window instead.
        **_rows(
            ("period_unbound_in_pin",),
            "warning",
            ("rendered", "semantic"),
            "declination",
        ),
        # A pin that selects no statement fact yet: historical population
        # has no provider. A statement binding persisted through
        # ``fms_persist_correspondence`` selects the facts that supply one.
        **_rows(
            ("statement_population_source_absent",),
            "warning",
            ("pinned",),
            "repair",
        ),
        **_rows(
            ("scenario_control_unbound", "balance_role_without_driver"),
            "error",
            ("authored",),
            "repair",
        ),
        **_rows(
            (
                "readback_mismatch",
                "recalculation_engine_failed",
                "formula_link_broken",
                "recalculation_divergence",
            ),
            "error",
            ("rendered",),
            "none",
        ),
        **_rows(("excel_error_unexpected",), "error", ("rendered",), "repair"),
        **_rows(
            ("semantic_graph_failed", "semantic_parity_mismatch"),
            "error",
            ("semantic",),
            "none",
        ),
        # G: annual presentation guard (the guard reason rides ``observed``).
        **_rows(
            (
                "fiscal_axis_header_missing",
                "unit_format_mismatch",
                "template_residue_visible",
            ),
            "error",
            ("rendered",),
            "none",
        ),
        # D: projected integrity; rendered when recalculation evaluated it.
        **_rows(
            (
                "projected_balance_sheet_inputs_missing",
                "projected_balance_sheet_imbalance",
                "projected_cash_roll_forward_inputs_missing",
                "projected_cash_opening_balance_mismatch",
                "projected_cash_net_change_mismatch",
                "projected_cash_ending_balance_mismatch",
                "required_revenue_component_projection_missing",
                "required_revenue_component_reconciliation_failed",
                "projected_revenue_continuity_break",
                "semantic_unit_undeclared",
                "projected_unit_scale_invariant_failed",
                "projected_sign_invariant_failed",
                "projected_schedule_roll_forward_inputs_missing",
                "projected_schedule_roll_forward_mismatch",
            ),
            "error",
            ("rendered", "semantic"),
            "repair",
        ),
        # A filed balance-sheet or cash-flow line with a value that no
        # statement_bindings.v2 row claims (schema/filed_line_unclaimed.py).
        **_rows(("filed_line_unclaimed",), "error", ("pinned",), "repair"),
        # HFF's selected-vintage filed tie-out disagrees with its spread.
        **_rows(("filed_value_disagrees",), "error", ("pinned",), "repair"),
        # Wave 1: rendered historical statement identities; a statement
        # total's rendered operand set, on historical and projected columns.
        **_rows(
            (
                "historical_balance_sheet_imbalance",
                "historical_cash_net_change_mismatch",
                "historical_cash_ending_balance_mismatch",
                "historical_cash_opening_balance_mismatch",
                "statement_total_operand_dropped",
                "statement_total_operand_offsheet",
            ),
            "error",
            ("rendered",),
            "repair",
        ),
        # Henry 2026-10-13 (docs/reference/rulings-index.md, FILED-BALANCE-SHEET-FOOTING-GAP):
        # a filed balance sheet whose printed totals do not foot is kept at its face values
        # and the gap is a record citing the filer's rounding disclaimer, never a plug.
        **_rows(
            ("filed_balance_sheet_footing_gap",),
            "info",
            ("rendered", "semantic"),
            "none",
        ),
        # A rendered historical adjusted-EBITDA stage that departs from the
        # issuer's filed bridge (``build_check_projection``).
        **_rows(
            ("non_gaap_bridge_mismatch",), "error", ("rendered", "pinned"), "repair"
        ),
        # Henry 2026-10-22 (INTEGRITY-Q6 / design §2.9): a bridge whose printed
        # figures are rounding-consistent within the filer's half-ulp precision
        # budget projects as filed, and the residual is a declinable warning.
        **_rows(
            ("non_gaap_bridge_rounding_gap",),
            "warning",
            ("rendered", "pinned"),
            "declination",
        ),
        # A row whose definition changes between the last historical and the
        # first projected column (signed operand sets differ at the seam).
        **_rows(("row_definition_discontinuity",), "error", ("rendered",), "repair"),
        # C: compile raises; the severities wave 3's door reports.
        **_rows(("required_column_absent_from_pin",), "error", ("pinned",), "repair"),
        # R2: a malformed node reference the compile cannot lower (a compile
        # raise).
        **_rows(
            ("business_model_node_invalid",),
            "error",
            ("authored",),
            "repair",
        ),
        # Spec §7 "Provenance on facts" (every Fact has a source): a node bound
        # to a pinned series, or claiming a registered formula, lowers at a
        # unit/scale the series or formula does not write, so the sourced Fact
        # cannot land on it. The compile reports it and leaves the node unbound.
        **_rows(
            ("business_model_node_history_target_type_mismatch",),
            "error",
            ("authored",),
            "repair",
        ),
        # Spec §7 "Policy assumptions surfaced" (mechanics takes its parameters
        # as Judgments): a forecast assumption with no typed value, no unit, a
        # second row for one driver, or a curve short of the forecast axis is a
        # Judgment the mechanics cannot take; the compile leaves it unbound.
        **_rows(("forecast_assumption_incomplete",), "error", ("authored",), "repair"),
        **_rows(
            (
                "formula_input_missing",
                "required_line_population_error",
                "required_line_value_conflict",
            ),
            "error",
            ("semantic",),
            "repair",
        ),
        # R2: a required series no node claims; a declined_series row clears
        # it. Uncleared, it rides the published version as an incompleteness
        # Finding (Henry 2026-10-08 20:15: never a publish block).
        **_rows(
            ("business_model_node_unresolved",),
            "warning",
            ("authored",),
            "declination",
        ),
        # Spec §7 "Provenance on facts" (every Fact has a source): a warning the
        # BusinessModel door's validation recorded on the revision at persist —
        # a dollar node with no model_scale, a segment claim matched to a filed
        # member by name only, a discovered EDGAR member no segment claims,
        # segment validation that could not run. The validator's code rides
        # ``observed``; a record of the run, never a refusal.
        **_rows(
            ("business_model_validation_warning",),
            "warning",
            ("authored",),
            "declination",
        ),
        # R6 (W1): a compiler house value written ``ValueProvenance.default``
        # where no authored FAS / ValuationPolicy row exists; a computed,
        # balanced model carries the default as a nonblocking warning.
        **_rows(
            ("judgment_absent_default_used",),
            "warning",
            ("authored",),
            "declination",
            refuse_publish_uncleared=True,
        ),
        # SBC forecast method (2026-10-04): a per-line SBC row authored where
        # the issuer discloses no SBC split by line; the control does not
        # apply there. A notice the author answers or declines, never a refusal.
        **_rows(
            ("sbc_per_line_row_split_undisclosed",),
            "warning",
            ("authored",),
            "declination",
        ),
        # R7 (W4a): a judgment row without its reason, with an unresolved or
        # derived-view citation, or changed / dropped by a successor without a
        # supersession; refused at publish until repaired or declined.
        **_rows(
            (
                "judgment_reason_missing",
                "judgment_citation_unresolved",
                "judgment_cites_derived_view",
                "supersession_missing",
            ),
            "warning",
            ("authored",),
            "declination",
            refuse_publish_uncleared=True,
        ),
        # Spec §7 "Provenance on facts" (every Fact has a source): a historical
        # opening seed whose filed source binds no one value — an ambiguous
        # member or filing line, or a source binding the BusinessModel cannot
        # resolve. HFS records it as a non-blocking gap instead of refusing
        # acquisition; the seed is judged from cited evidence, or declined.
        **_rows(
            ("historical_seed_source_unresolved",),
            "warning",
            ("authored",),
            "declination",
        ),
        # Spec §7 "Provenance on facts": an accepted HistoricalFoundation
        # captured under another forecast-control profile pin or denominator
        # digest than current code derives for the same drivers. The
        # forecast-assumptions context answers on it and offers the current
        # profile with the predecessor's judgments carried, so a re-accept at
        # the ForecastAssumptionSet door records the current pin; the
        # foundation is re-persisted only when ``hff_stale_for_roles`` names a role.
        **_rows(
            ("historical_foundation_profile_drifted",), "warning", ("pinned",), "repair"
        ),
        # §7 "Provenance on facts": the HistoricalFoundation the forecast-
        # assumptions context reads is not accepted. The context answers on it
        # (the current foundation is the typed input) and names
        # fms_persist_historical_foundation, which re-judges it.
        **_rows(
            ("forecast_historical_foundation_not_accepted",),
            "info",
            ("authored",),
            "none",
        ),
        # §7 "Provenance on facts": the historical basis a forecast rests on
        # does not cover the review filing's period (its freshness is not
        # ``current``). A notice — the basis stays usable, accepted and
        # projected as-of its covered period; nothing waits on it (D2,
        # 2026-08-28).
        **_rows(("historical_basis_stale",), "info", ("pinned",), "none"),
        **_rows(
            ("statement_window_validation_failed",), "error", ("pinned",), "repair"
        ),
        # Spec §7 "Provenance on facts": a pinned filing subtotal whose
        # template children do not reproduce the filed value. The filed value
        # stays on the row; the subtotal, the computed and filed values and the
        # children it summed ride ``observed`` / ``operands``. A composition
        # fact the template or the binding repairs, or the author declines.
        **_rows(
            ("pinned_filing_subtotal_reconciliation_failed",),
            "warning",
            ("pinned",),
            "declination",
        ),
        # Spec §7 "Normalization choices recorded" (a normalization is a
        # Judgment with a reason; the original Fact survives): a reviewed
        # statement window the compile cannot plan onto the template (its
        # ``issue_code`` rides ``observed``) is not applied.
        **_rows(
            ("statement_mapping_build_plan_invalid",), "error", ("pinned",), "repair"
        ),
        # Spec §7 "Provenance on facts", recorded at publish: a window boundary
        # the pin cannot decide, or a declared basis break the pin does not
        # carry. The version records the gap; it does not block.
        **_rows(
            (
                "statement_window_boundary_unresolvable",
                "statement_window_declared_break_absent",
            ),
            "warning",
            ("pinned",),
            "repair",
        ),
        **_rows(
            ("section_total_target_ambiguous", "selected_row_target_ambiguous"),
            "error",
            ("semantic",),
            "none",
        ),
        **_rows(
            (
                "capital_structure_value_invalid",
                "capital_structure_long_term_debt_negative",
                "capital_structure_total_capital_invalid",
            ),
            "error",
            ("semantic",),
            "repair",
        ),
        # E: historical diagnostic-report kinds.
        **_rows(
            (
                "historical_bs_balance_mismatch",
                "historical_inputs_insufficient",
                "historical_bs_subline_gap",
                "historical_is_subtotal_mismatch",
                "historical_cash_roll_mismatch",
                "operating_cash_unexplained",
                "consolidation_mismatch",
                "duplicate_rows",
                "synthetic_zero_propagation",
                "source_backed_projection_only",
            ),
            "warning",
            ("semantic",),
            "declination",
        ),
        **_rows(
            ("historical_required_role_unobserved", "cross_source_gap"),
            "warning",
            ("pinned",),
            "declination",
        ),
        # Retired with the registered formulas (R-F5); no emitter. The row stays so a persisted
        # validation report naming it still decodes.
        **_rows(
            ("registered_formula_unevaluated",),
            "warning",
            ("pinned",),
            "declination",
        ),
        # H: model readiness (a Summary blocker today when ``blocking``).
        **_rows(
            (
                "primary_projection_item_missing",
                "primary_projection_missing",
                "primary_projection_incomplete",
                "required_driver_projection_missing",
                "driver_projection_period_gap",
                "bm_driver_projection_seed_missing",
                "bm_segment_projection_missing",
                "segment_history_basis_mixed",
                "segment_history_basis_unverified",
                "share_count_item_missing",
                "share_count_projection_missing",
                "share_count_dependency_missing",
                "working_capital_item_missing",
                "working_capital_projection_missing",
                "inventory_policy_missing",
                "valuation_item_missing",
                "valuation_projection_missing",
                "valuation_inputs_incomplete",
                "price_target_skip_reason",
                "capital_structure_item_missing",
                "capital_structure_projection_missing",
                "capital_structure_debt_magnitude_blocking",
                "capital_structure_debt_magnitude_warning",
                "capital_structure_wacc_out_of_band",
                "capital_structure_weight_out_of_bounds",
                "scenario_output_item_missing",
                "scenario_output_projection_missing",
                "scenario_output_projection_incomplete",
                "scenario_output_direction_mismatch",
                "valuation_inputs_missing",
                "legacy_valuation_placeholders_present",
            ),
            "warning",
            ("semantic",),
            "declination",
        ),
        **_rows(
            (
                "stale_render_state",
                "formula_rollup_missing_inputs",
                "scenario_bridge_target_missing",
                "scenario_bridge_owner_missing",
                "scenario_bridge_anchor_missing",
                "scenario_bridge_case_row_missing",
                "inert_scenario_anchor",
            ),
            "warning",
            ("semantic",),
            "repair",
        ),
        # H: valuation-input workbook flags.
        **_rows(
            (
                "valuation_projection_periods_missing",
                "market_valuation_inputs_missing",
                "macro_valuation_inputs_missing",
                "equity_risk_premium_missing",
            ),
            "warning",
            ("semantic",),
            "declination",
        ),
        # Folded into ``judgment_absent_default_used`` (W1); no emitter. The
        # rows stay so a persisted Declination naming either still validates.
        **_rows(
            (
                "valuation_terminal_assumptions_defaulted",
                "terminal_assumptions_defaulted",
            ),
            "warning",
            ("semantic",),
            "declination",
        ),
        **_rows(
            ("zero_debt_cost_of_debt_not_applicable",), "info", ("semantic",), "none"
        ),
        # §7 "Variance to what is priced in" (the price is a Reference stated
        # in the model's terms): an unlevered DCF whose issuer reports no
        # separable interest in a year it carried debt would discount a levered
        # strip, so no price publishes until the interest is bound (SC62).
        **_rows(("interest_expense_not_separable",), "error", ("semantic",), "repair"),
        # F: what the deleted Summary rollup reported.
        **_rows(("unit_format_metadata_missing",), "warning", ("semantic",), "repair"),
        **_rows(
            ("required_value_unavailable", "schedule_input_missing"),
            "warning",
            ("semantic",),
            "declination",
        ),
        **_rows(("scenario_result_blocked",), "warning", ("semantic",), "repair"),
        **_rows(
            ("forecast_assumption_limitations_unstated",),
            "warning",
            ("authored",),
            "repair",
        ),
        **_rows(("observed_input_lagged_print", "observed_input_cache_corrupt"), "warning", ("pinned",), "none"),
        # Info: judgment flags with no correct answer (spec §4.2). Shown on
        # every surface, never in the verdict, never cleared.
        **_rows(
            (
                "forecast_driver_uncited",
                "analyst_note_uncited",
                "critical_factor_unevidenced",
                "scenario_live_coverage",
            ),
            "info",
            ("authored",),
            "none",
        ),
        **_rows(("market_input_stale",), "info", ("pinned",), "none"),
        # E (W5d): a deliverable read after its research file's current pointer
        # moved past the ModelVersion it names. A notice, never a gate (D2): it
        # rides the read, never a build verdict; re-cutting the deliverable from
        # the current version repairs it.
        **_rows(("deliverable_version_stale",), "warning", ("pinned",), "repair"),
        # A report names a ModelVersion the research file does not hold. The
        # requested binding is kept; the read records the gap. Never invent a
        # current-model binding (Henry completeness rule).
        **_rows(("deliverable_version_unknown",), "warning", ("pinned",), "repair"),
        # A deliverable cut under a Question its author proposed and nobody has
        # ratified yet (ruled (a), 2026-10-04): the door stamps it anyway and
        # never refuses; ratifying the Question repairs it.
        **_rows(("question_unratified",), "warning", ("authored",), "repair"),
        # SC46/SC47: a semantic_rows override value (forecast_policy.type,
        # row_policy.mode, row_policy.formula_wiring_mode) outside the
        # vocabulary schema/build_semantic_rows.py owns; the build proceeds
        # with the stated default and names the value. Repair the override.
        **_rows(
            ("semantic_row_policy_unrecognized",), "warning", ("authored",), "repair"
        ),
        # A deliverable binding that named another Question than the research
        # file's current one: the door stamps the current Question and records
        # this notice instead of refusing (FM19). Nothing to clear.
        **_rows(("question_ref_restamped",), "info", ("authored",), "none"),
        # §7 "Estimate history" (Judgments are append-only; a supersession has
        # a Revision): a stored BusinessModel revision's plan still carries a
        # driver the current forecast denominator retired. The row derives no
        # control and binds to no model row; a successor ForecastAssumptionSet
        # drops its judgment, or the analyst declines it. Never a refusal.
        **_rows(
            ("forecast_driver_plan_row_retired",),
            "warning",
            ("authored",),
            "declination",
        ),
        # Valuation context and pricing (valuation-context-and-pricing-info-
        # findings): facts the context doors, the ScenarioSet check and the
        # output materializers report instead of refusing.
        # §7 "Policy assumptions surfaced" (Mechanics takes its parameters as
        # Judgments): a valuation context whose parameter judgments (accepted
        # FAS, ScenarioSet, ModelVersion, ValuationPolicy) do not resolve yet;
        # the door names the producer in ``next_call``.
        **_rows(
            ("valuation_context_upstream_unresolved",), "info", ("authored",), "none"
        ),
        # Historical-foundation context (research-runtime B43): no selected
        # BusinessModel is a prior-stage gap on the issued context. The door
        # names fms_persist_business_model in next_call. Not a capture stop.
        **_rows(
            ("historical_foundation_upstream_unresolved",),
            "info",
            ("authored",),
            "none",
        ),
        **_rows(
            ("handoff_not_finalized",),
            "info",
            ("authored",),
            "none",
        ),
        # Prepare (prepare-and-publish-receipts-deleted, census K10/K14/K20).
        # §7 "Policy assumptions surfaced" (Mechanics takes its parameters as
        # Judgments): a ScenarioSet or ValuationPolicy the build reads resolves
        # to no stored artifact for the carried chain; prepare proceeds and the
        # message names the context door that produces it. An error: the build
        # cannot assemble the sections that judgment parameterizes.
        **_rows(("build_input_unresolved",), "error", ("authored",), "repair"),
        # §7 "Summary in sync with the model" (derived from a Version, never
        # copied): a carried context_ref whose authority moved; the door
        # re-issues the context from the current state.
        **_rows(("valuation_context_reissued",), "info", ("pinned",), "none"),
        # §7 "Coherent scenarios" (a named set with a membership Judgment): a
        # scenario flexes a driver its ForecastAssumptionSet does not forecast.
        **_rows(("scenario_driver_not_forecast",), "error", ("authored",), "repair"),
        # §7 "Coherent scenarios": the bull and bear cases do not bracket the
        # base (ScenarioSet curves, ValuationPolicy terminal cases).
        **_rows(
            ("scenario_case_order_inverted",),
            "warning",
            ("authored",),
            "declination",
        ),
        # §7 "Coherent scenarios": the ForecastAssumptionSet a ScenarioSet
        # flexes is not accepted (R2-Q20 default: acceptance is completeness).
        **_rows(("scenario_forecast_not_accepted",), "info", ("authored",), "none"),
        # §7 "Provenance on facts" (every Fact has a source): the ScenarioSet's
        # base evidence or base value semantics differ from the FAS it reads.
        **_rows(("scenario_base_lineage_differs",), "info", ("authored",), "none"),
        # §7 "Which assumptions matter": the critical-factors judgment a
        # ScenarioSet rests on is not FACTORS_IDENTIFIED, selects other than
        # one or two factors, or a factor scores below forecastability three.
        **_rows(
            ("critical_factors_unqualified",),
            "warning",
            ("authored",),
            "declination",
        ),
        # §7 "Variance to what is priced in" (the price is a Reference): the
        # PriceTarget could not be derived from the published version.
        **_rows(("price_target_not_derived",), "warning", ("semantic",), "repair"),
        # §7 "Summary in sync with the model": ModelInsights could not be
        # derived from the published version.
        **_rows(("model_insights_not_derived",), "warning", ("semantic",), "repair"),
        # Postcompile valuation (prepare-and-publish-receipts-deleted's
        # emitter, registered here). §7 "The balance check" (a Projection
        # that fails an identity is invalid): the priced version is not ready.
        **_rows(
            ("postcompile_model_version_not_ready",),
            "warning",
            ("pinned",),
            "declination",
        ),
        # §7 "Policy assumptions surfaced": the policy priced is not accepted.
        **_rows(
            ("postcompile_valuation_policy_not_accepted",),
            "warning",
            ("authored",),
            "declination",
        ),
        # §7 "Variance to what is priced in": the compiled current price
        # differs from the price Reference.
        **_rows(
            ("postcompile_current_price_mismatch",),
            "warning",
            ("pinned",),
            "declination",
        ),
        # Build turn (build-turn-refusals-carry-a-usable-next-call): facts the
        # build lifecycle and the judgment doors report instead of refusing.
        # §7 "Provenance on facts" (every Fact has a source): a peer_derived
        # valuation selection the pinned SourceBundle no longer resolves
        # (census K50 = V48); ``observed`` carries ``selection_fix``.
        **_rows(
            ("valuation_selection_unresolved",),
            "warning",
            ("pinned",),
            "declination",
        ),
        # §7 "The reason on each judgment": an agent-authorable ModelSpec
        # section left unauthored (K39, R2-Q16 default).
        **_rows(
            ("model_spec_section_unauthored",),
            "warning",
            ("authored",),
            "declination",
        ),
        **_rows(
            (
                "working_document_unbound",
                "working_document_revision_conflict",
                "working_document_revision_mismatch",
                "working_document_scenario_controls_unavailable",
                "working_document_template_unavailable",
                "working_document_operation_invalid",
            ),
            "error",
            ("authored",),
            "repair",
        ),
        **_rows(
            ("working_document_inputs_changed",),
            "warning",
            ("authored",),
            "repair",
        ),
        **_rows(
            ("working_document_source_bundle_unavailable",),
            "error",
            ("pinned",),
            "repair",
        ),
        # §7 "Policy assumptions surfaced" (Mechanics takes its parameters as
        # Judgments): a persisted or tried ForecastAssumptionSet, ScenarioSet
        # or ValuationPolicy whose verdict is not accepted (R18, V29).
        **_rows(
            (
                "forecast_assumption_set_incomplete",
                "scenario_set_incomplete",
                "valuation_policy_incomplete",
            ),
            "warning",
            ("authored",),
            "declination",
        ),
        # §7 "Policy assumptions surfaced" (Mechanics takes its parameters as
        # Judgments): a ValuationPolicy that could not be rebound onto a
        # revised ForecastAssumptionSet or ScenarioSet (R20); the upstream
        # revisions persist and this error blocks publish until the policy is
        # re-persisted.
        **_rows(
            ("valuation_policy_rebind_failed",),
            "error",
            ("authored",),
            "repair",
        ),
        # §7 "Variance to what is priced in" (the price is a Reference stated
        # in the model's terms): a persisted PostcompileValuation that derived
        # no PriceTarget (V30).
        **_rows(
            ("postcompile_valuation_incomplete",),
            "warning",
            ("authored",),
            "declination",
        ),
        # §7 "The balance check" (a Projection that fails an identity is
        # invalid): the accepted ForecastAssumptionSet's fiscal axis differs
        # from the SourceBundle's (K13, handed over by prepare-and-publish).
        **_rows(
            ("forecast_authority_axis_mismatch",),
            "error",
            ("pinned",),
            "repair",
        ),
        # A declination that answers no finding of this build.
        **_rows(("declination_unmatched",), "info", ("authored",), "none"),
        # Henry 2026-09-30 (BEAR-ABOVE-SPOT): when the accepted bear case values
        # above current spot price, compile emits a non-blocking diagnostic naming
        # the implied discount and bear EPS × multiple.
        **_rows(("bear_case_above_spot",), "info", ("rendered",), "none"),
    }
)


#: Every code a Declination may answer: the warnings cleared by declination.
#: An error is never declinable.
DECLINABLE_FINDING_CODES: Final[tuple[str, ...]] = tuple(
    sorted(
        code
        for code, rule in FINDING_SEVERITY_MAP.items()
        if rule.severity == "warning" and rule.clearable_by == "declination"
    )
)


class DeclinationAuthor(BaseModel):
    """Who declined, stamped by the server, never authored.

    ``agent``: a ``decline`` operation of the working document
    (``model_document`` edit); a declination stored before the live sheet also
    carries the trusted user, the ``context_ref`` that issued the candidate and
    the authoring time.
    ``business_model``: a ``declined_series`` row the ModelSpec assembly
    projects, with the BusinessModel revision it came from.
    """

    model_config = ConfigDict(extra="forbid", frozen=True)

    kind: Literal["agent", "business_model"]
    user_id: str | None = Field(default=None, min_length=1)
    context_ref: str | None = Field(default=None, min_length=1)
    authored_at: str | None = Field(default=None, min_length=1)
    business_model_revision: BusinessModelRevisionRef | None = None


class Declination(BaseModel):
    """One answer to a declinable warning (spec §4.4).

    Matches a warning with the same ``code`` and ``item_id`` whose periods lie
    within ``periods`` (empty: every period).
    """

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    declination_id: Annotated[str, FieldDisposition.SERVER_MINTED] = Field(
        min_length=1
    )
    code: Annotated[str, FieldDisposition.CLOSED_VOCABULARY] = Field(min_length=1)
    item_id: Annotated[str | None, FieldDisposition.SELECTOR] = Field(
        default=None, min_length=1, max_length=200
    )
    periods: Annotated[tuple[int, ...], FieldDisposition.SELECTOR] = ()
    reason: Annotated[str, FieldDisposition.JUDGMENT] = Field(
        min_length=1, max_length=FINDING_MESSAGE_MAX_LENGTH
    )
    citation: Annotated[str | None, FieldDisposition.JUDGMENT] = Field(
        default=None, min_length=1, max_length=FINDING_MESSAGE_MAX_LENGTH
    )
    author: Annotated[DeclinationAuthor, FieldDisposition.SERVER_DERIVED]

    @field_validator("periods", mode="after")
    @classmethod
    def _canonical_periods(cls, periods: tuple[int, ...]) -> tuple[int, ...]:
        return tuple(sorted(set(periods)))

    def answers(self, finding: "Finding") -> bool:
        return (
            finding.severity == "warning"
            and finding.clearable_by == "declination"
            and finding.code == self.code
            and finding.item_id == self.item_id
            and (not self.periods or set(finding.periods) <= set(self.periods))
        )


class Finding(BaseModel):
    """One build-model check result (spec §4.1)."""

    model_config = ConfigDict(extra="forbid", frozen=True, str_strip_whitespace=True)

    code: str = Field(min_length=1, max_length=120)
    severity: FindingSeverity
    basis: FindingBasis
    item_id: str | None = Field(default=None, max_length=200)
    periods: tuple[int, ...] = ()
    operands: tuple[str, ...] = ()
    #: The years of ``operands`` the repair changes when they differ from
    #: ``periods`` (the finding's own cells): a missing projected total whose
    #: root is a historical seed. Empty means the repair is in ``periods``.
    repair_periods: tuple[int, ...] = Field(
        default=(), exclude_if=lambda value: not value
    )
    observed: dict[str, FindingScalar] = Field(default_factory=dict)
    expected: dict[str, FindingScalar] = Field(default_factory=dict)
    message: str = Field(min_length=1, max_length=FINDING_MESSAGE_MAX_LENGTH)
    clearable_by: FindingClearableBy
    #: The declination that cleared this warning (``apply_declinations``).
    declination: Declination | None = Field(
        default=None, exclude_if=lambda value: value is None
    )

    @field_validator("severity", "clearable_by", mode="before")
    @classmethod
    def _takes_its_map_row(cls, _value: object, info: ValidationInfo) -> str:
        """Default severity and clearing route belong to the vocabulary.

        Computed-model provenance is resolved below from the validator's
        persisted evidence; callers cannot choose a severity themselves.
        """

        code = info.data.get("code")
        rule = FINDING_SEVERITY_MAP.get(code or "")
        if rule is None:
            raise ValueError(f"finding code {code!r} has no severity map row")
        return rule.severity if info.field_name == "severity" else rule.clearable_by

    @model_validator(mode="after")
    def _takes_computed_model_severity(self) -> "Finding":
        if _computed_model_warning(self):
            object.__setattr__(self, "severity", "warning")
        return self

    @field_validator("periods", mode="after")
    @classmethod
    def _canonical_periods(cls, periods: tuple[int, ...]) -> tuple[int, ...]:
        return tuple(sorted(set(periods)))


def finding_is_cleared(finding: Finding) -> bool:
    """Whether a declination answers this warning (never an error)."""

    return (
        finding.severity == "warning"
        and finding.declination is not None
        and finding.declination.answers(finding)
    )


def _computed_model_provenance(finding: Finding) -> bool:
    return finding.code in (
        "judgment_absent_default_used",
        "forecast_assumption_limitations_unstated",
    ) or (
        finding.code == "missing_source"
        and finding.item_id is not None
        and finding.item_id.startswith(("bm.", "tpl.a.", "operating."))
    )


def _computed_model_warning(finding: Finding) -> bool:
    return (
        (
            finding.observed.get("computed_model") is True
            or finding.observed.get("graph_computed_model") is True
        )
        and _computed_model_provenance(finding)
    )


def computed_model_findings(
    findings: Iterable[Finding], *, computed_model: bool, basis: Literal["native", "graph"]
) -> tuple[Finding, ...]:
    """Record the validator's computed-model evidence without changing other findings.

    The vocabulary owns conditional warning severity for either basis, including
    on persisted readback. ``computed_model`` means native recalculation verified
    (``recalculation_divergence_count == 0``, ``unexpected_excel_error_count == 0``).
    ``graph_computed_model`` means the DependencyGraph criteria held with no
    native evidence (``recalculation_divergence_count is None``).
    """

    evidence_key = "computed_model" if basis == "native" else "graph_computed_model"
    return tuple(
        finding.model_copy(update={
            "severity": "warning",
            "observed": {
                **{
                    key: value for key, value in finding.observed.items()
                    if key not in ("computed_model", "graph_computed_model")
                },
                evidence_key: True,
            },
        })
        if computed_model and _computed_model_provenance(finding)
        else finding
        for finding in findings
    )


def uncleared_warnings(findings: Iterable[Finding]) -> tuple[Finding, ...]:
    return tuple(
        finding
        for finding in findings
        if finding.severity == "warning" and not finding_is_cleared(finding)
    )


def error_findings(findings: Iterable[Finding]) -> tuple[Finding, ...]:
    return tuple(finding for finding in findings if finding.severity == "error")


def blocking_findings(findings: Iterable[Finding]) -> tuple[Finding, ...]:
    """Every error and every uncleared refusing warning blocks, except the
    provenance warnings answered by the build's computed-model evidence."""

    materialized = tuple(findings)
    return (
        *error_findings(materialized),
        *(
            finding
            for finding in uncleared_warnings(materialized)
            if FINDING_SEVERITY_MAP[finding.code].refuse_publish_uncleared
            and not _computed_model_warning(finding)
        ),
    )


def derive_build_verdict(findings: Iterable[Finding]) -> BuildVerdict:
    """The one verdict.

    Any blocking finding blocks (``blocking_findings``); any other uncleared
    warning warns.
    """

    materialized = tuple(findings)
    if blocking_findings(materialized):
        return "blocked"
    if uncleared_warnings(materialized):
        return "ready_with_warnings"
    return "ready"


__all__ = [
    "BuildVerdict",
    "DECLINABLE_FINDING_CODES",
    "Declination",
    "DeclinationAuthor",
    "FINDING_MESSAGE_MAX_LENGTH",
    "FINDING_SEVERITY_MAP",
    "Finding",
    "FindingBasis",
    "FindingClearableBy",
    "FindingRule",
    "FindingScalar",
    "FindingSeverity",
    "blocking_findings",
    "computed_model_findings",
    "derive_build_verdict",
    "error_findings",
    "finding_is_cleared",
    "uncleared_warnings",
]
