"""Template mutation helpers used while building reference artifacts."""

from __future__ import annotations

from typing import Any, Dict, Iterable, List, NamedTuple, Optional, Sequence

from ..models import (
    CellColor,
    CellStyle,
    CustomizationType,
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
    Section,
)
from industry_slice_core.industry_types import Unit
from ..build_diagnostic_sections import balance_sheet_total_members
from ..required_role_catalog import face_lines, load_required_role_catalog
from .template_builder_config import (
    CASH_FLOW_ARTIFACT_HISTORICAL_IDS,
    CASH_FLOW_FULL_PROJECTION_IDS,
    CASH_FLOW_NET_INCOME_SOURCE_ID,
    CASH_FLOW_PER_SHARE_PROJECTION_IDS,
    IF_APPLICABLE_DEFAULT_ZERO_IDS,
    KEPT_SHEETS,
    _COLUMN_OFFSET_MODE_PERIOD_RELATIVE,
    _SCENARIO_LINKED_OFFSET_ANCHORS,
    _SCENARIO_SELECTOR_ID,
)

def _iter_items(model: FinancialModel) -> Iterable[tuple[str, LineItem]]:
    for sheet_name in KEPT_SHEETS:
        sheet = model.sheets[sheet_name]
        for section in sheet.sections:
            for item in section.line_items:
                yield sheet_name, item


def _normalize_generic_accounting_baseline(model: FinancialModel) -> None:
    """Keep the captured issuer schedule out of the accounting baseline.

    The captured statement totals sum rows this baseline removes and carry the
    issuer's own membership, so they leave blank here: the catalog owns every
    total's membership and `_generate_statement_totals` writes both lanes
    once every row exists.
    """
    removed_ids = {
        "assumptions.funds_held_for_clients",
        "assumptions.funds_held_for_client_s_cash_and_cash_equivalents",
        "assumptions.cash_equivalents",
        "financial_model.total_current_assets_before_funds_held_for_clients",
        "financial_model.total_current_liabilities_before_client_fund_obligations",
    }
    for sheet_name in KEPT_SHEETS:
        for section in model.sheets[sheet_name].sections:
            section.line_items = [
                item for item in section.line_items if item.id not in removed_ids
            ]
    catalog = load_required_role_catalog()
    for total_id in catalog.statement_totals():
        presenting = catalog.presenting_role(total_id)
        for build_id in () if presenting is None else presenting.template_build_items:
            total = _find_sheet_item(model, "Financial_model", build_id)
            total.historical = None
            total.projected = None
    # The statement reconciliation, unlike the removed assumptions schedule,
    # applies to every issuer and is supplied from reported ending and BS cash.
    reconciliation = _find_sheet_item(
        model, "Financial_model",
        "financial_model.funds_held_for_client_s_cash_and_cash_equivalents",
    )
    reconciliation.projected = FormulaSpec(
        type=FormulaType.ref,
        subtype="carry_forward",
        params={"source": LineItemRef(id=reconciliation.id, t=-1)},
    )
    # The workbook's first forecast column carries the prior AOCI balance as a
    # cell override. The compiler's cash-FX roll-forward owns the schedule in
    # every projection period, so a captured carry would shadow it in the first.
    _find_sheet_item(
        model, "Assumptions", "assumptions.accumulated_other_comprehensive_income_loss"
    ).overrides = None
    # The inventory assumption's ratio formula is available for every forecast
    # year the template carries; inventory materiality selects the projection
    # policy downstream, not the formula's end date.
    inventory = _find_sheet_item(model, "Assumptions", "assumptions.current_asset_2")
    projection_periods = (
        model.time_structure.projection_periods
        or model.time_structure.projection_years
        or []
    )
    inventory.formula_periods = sorted(
        {int(period) for period in (inventory.formula_periods or [])}
        | {int(period) for period in projection_periods}
    )

def _iter_formula_specs(item: LineItem) -> Iterable[FormulaSpec]:
    if item.historical:
        yield item.historical
    if item.projected:
        yield item.projected
    if item.overrides:
        yield from item.overrides.values()

def _extract_refs(obj) -> List[LineItemRef]:
    refs: List[LineItemRef] = []
    if obj is None:
        return refs
    if isinstance(obj, LineItemRef):
        return [obj]
    if isinstance(obj, dict):
        for value in obj.values():
            refs.extend(_extract_refs(value))
        return refs
    if isinstance(obj, (list, tuple, set)):
        for value in obj:
            refs.extend(_extract_refs(value))
    return refs

def _rename_refs(obj, renamed_ids: Dict[str, str]):
    """``obj`` with every reference to a renamed id repointed (references are frozen)."""
    if isinstance(obj, LineItemRef):
        new_id = renamed_ids.get(obj.id)
        return obj if new_id is None else obj.model_copy(update={"id": new_id})
    if isinstance(obj, dict):
        return {key: _rename_refs(value, renamed_ids) for key, value in obj.items()}
    if isinstance(obj, list):
        return [_rename_refs(value, renamed_ids) for value in obj]
    return obj

def _rename_spec_refs(spec: FormulaSpec, renamed_ids: Dict[str, str]) -> FormulaSpec:
    return spec.model_copy(update={"params": _rename_refs(spec.params, renamed_ids)})

def _apply_scenario_linked_offset_params(model: FinancialModel) -> None:
    for _, item in _iter_items(model):
        anchor_id = _SCENARIO_LINKED_OFFSET_ANCHORS.get(item.id)
        if anchor_id is None:
            continue
        item.projected = FormulaSpec(
            type=FormulaType.valuation,
            subtype="offset_scenario",
            params={
                "anchor": LineItemRef(id=anchor_id),
                "selector": LineItemRef(id=_SCENARIO_SELECTOR_ID),
                "column_offset_mode": _COLUMN_OFFSET_MODE_PERIOD_RELATIVE,
            }
        )

def _find_sheet_item(model: FinancialModel, sheet_name: str, item_id: str) -> LineItem:
    for section in model.sheets[sheet_name].sections:
        for item in section.line_items:
            if item.id == item_id:
                return item
    raise AssertionError(f"Could not find {sheet_name} item {item_id}")

def _find_section_with_item(sheet, item_id: str) -> Section:
    for section in sheet.sections:
        if any(item.id == item_id for item in section.line_items):
            return section
    raise AssertionError(f"Could not find section containing {item_id}")

def _apply_label_overrides(model: FinancialModel, overrides: Dict[str, str]) -> None:
    for source_id, new_label in overrides.items():
        if source_id.startswith("assumptions."):
            sheet_name = "Assumptions"
        elif source_id.startswith("financial_model."):
            sheet_name = "Financial_model"
        else:
            raise ValueError(f"Unknown source ID prefix in label override: {source_id!r}")
        item = _find_sheet_item(model, sheet_name, source_id)
        item.label = new_label

def _insert_sum_ref_after(items: object, *, after_id: str, new_id: str, context: str) -> List[LineItemRef]:
    if not isinstance(items, list):
        raise AssertionError(f"{context} is missing SUM items")

    updated_items: List[LineItemRef] = []
    inserted = False
    for ref in items:
        if not isinstance(ref, LineItemRef):
            raise AssertionError(f"{context} contains non-reference SUM items")
        updated_items.append(ref)
        if ref.id == after_id:
            updated_items.append(LineItemRef(id=new_id))
            inserted = True

    if not inserted:
        raise AssertionError(f"Could not insert {new_id} into {context}")
    return updated_items

def _insert_other_non_current_assets_row(model: FinancialModel) -> None:
    item_id = "financial_model.other_non_current_assets"
    insert_row = 85
    financial_sheet = model.sheets["Financial_model"]

    if any(item.id == item_id for _, item in _iter_items(model)):
        raise AssertionError("Synthetic other_non_current_assets row already exists")

    target_section = _find_section_with_item(financial_sheet, "financial_model.deferred_income_tax_assets")
    target_section.line_items.append(
        LineItem(
            id=item_id,
            label=" Other non-current assets",
            row=insert_row,
            item_type=ItemType.derived,
            unit=Unit.dollars,
        )
    )

    _insert_total_assets_sum_ref_after(
        model,
        total_assets_id="financial_model.total_assets",
        after_id="financial_model.deferred_income_tax_assets",
        new_id=item_id,
    )


def _insert_total_assets_sum_ref_after(
    model: FinancialModel, *, total_assets_id: str, after_id: str, new_id: str
) -> None:
    """Add a row to the long-term-asset SUM inside total assets, after ``after_id``."""

    total_assets = _find_sheet_item(model, "Financial_model", total_assets_id)
    for spec in (total_assets.historical, total_assets.projected):
        if spec is None:
            continue
        expr = spec.params.get("expr")
        if not isinstance(expr, dict) or expr.get("op") != "+":
            raise AssertionError("Total assets formula is missing the expected additive expression")
        args = expr.get("args")
        if not isinstance(args, list) or not args:
            raise AssertionError("Total assets formula is missing expression args")
        long_term_assets = args[0]
        if not isinstance(long_term_assets, dict) or long_term_assets.get("op") != "SUM":
            raise AssertionError("Total assets formula is missing the long-term asset SUM")
        long_term_assets["args"] = _insert_sum_ref_after(
            long_term_assets.get("args"),
            after_id=after_id,
            new_id=new_id,
            context="total assets",
        )


def _insert_other_current_liabilities_row(model: FinancialModel) -> None:
    item_id = "financial_model.current_liability_2"
    insert_row = 93
    financial_sheet = model.sheets["Financial_model"]

    if any(item.id == item_id for _, item in _iter_items(model)):
        raise AssertionError("Synthetic current_liability_2 row already exists")

    for section in financial_sheet.sections:
        for item in section.line_items:
            if item.row >= insert_row:
                item.row += 1

    target_section = _find_section_with_item(financial_sheet, "financial_model.current_liability_1")
    target_section.line_items.append(
        LineItem(
            id=item_id,
            label=" Other current liabilities",
            row=insert_row,
            item_type=ItemType.derived,
            unit=Unit.dollars,
        )
    )

def _insert_commercial_paper_row(model: FinancialModel) -> None:
    item_id = "financial_model.current_liability_3"
    insert_row = 94
    financial_sheet = model.sheets["Financial_model"]

    if any(item.id == item_id for _, item in _iter_items(model)):
        raise AssertionError("Synthetic current_liability_3 row already exists")

    for section in financial_sheet.sections:
        for item in section.line_items:
            if item.row >= insert_row:
                item.row += 1

    target_section = _find_section_with_item(financial_sheet, "financial_model.current_liability_2")
    target_section.line_items.append(
        LineItem(
            id=item_id,
            label=" Commercial paper",
            row=insert_row,
            item_type=ItemType.derived,
            unit=Unit.dollars,
        )
    )

def _insert_deferred_revenue_noncurrent_row(model: FinancialModel) -> None:
    item_id = "financial_model.deferred_revenue_noncurrent"
    insert_row = 103
    financial_sheet = model.sheets["Financial_model"]

    if any(item.id == item_id for _, item in _iter_items(model)):
        raise AssertionError("Synthetic deferred_revenue_noncurrent row already exists")
    if any(
        sheet_name == "Financial_model" and item.row == insert_row
        for sheet_name, item in _iter_items(model)
    ):
        raise AssertionError("Synthetic deferred_revenue_noncurrent row target is occupied")

    target_section = _find_section_with_item(financial_sheet, "financial_model.long_term_debt")
    target_section.line_items.append(
        LineItem(
            id=item_id,
            label=" Deferred revenue, noncurrent",
            row=insert_row,
            item_type=ItemType.derived,
            unit=Unit.dollars,
        )
    )


def _insert_investment_activity_rows(model: FinancialModel) -> None:
    purchase_id = "financial_model.purchases_of_investments"
    proceeds_id = "financial_model.sales_maturities_of_investments"
    purchase_row = 157
    proceeds_row = 158
    financial_sheet = model.sheets["Financial_model"]

    if any(item.id in {purchase_id, proceeds_id} for _, item in _iter_items(model)):
        raise AssertionError("Synthetic investment activity rows already exist")

    for section in financial_sheet.sections:
        for item in section.line_items:
            if item.row >= purchase_row:
                item.row += 2

    target_section = _find_section_with_item(financial_sheet, "financial_model.acquisitions_of_businesses")
    target_section.line_items.extend(
        [
            LineItem(
                id=purchase_id,
                label="Purchases of investments",
                row=purchase_row,
                item_type=ItemType.derived,
                unit=Unit.dollars,
            ),
            LineItem(
                id=proceeds_id,
                label="Sales and maturities of investments",
                row=proceeds_row,
                item_type=ItemType.derived,
                unit=Unit.dollars,
            ),
        ]
    )

    investing_cash_flow = _find_sheet_item(
        model,
        "Financial_model",
        "financial_model.net_cash_provided_by_investing_activities",
    )
    for spec in (investing_cash_flow.historical, investing_cash_flow.projected):
        if spec is None or spec.params.get("function") != "SUM":
            continue
        spec.params["items"] = _insert_sum_ref_after(
            spec.params.get("items"),
            after_id="financial_model.acquisitions_of_businesses",
            new_id=purchase_id,
            context="investing cash flow",
        )
        spec.params["items"] = _insert_sum_ref_after(
            spec.params.get("items"),
            after_id=purchase_id,
            new_id=proceeds_id,
            context="investing cash flow",
        )

    software_investment = _find_sheet_item(
        model,
        "Financial_model",
        "financial_model.capitalized_software_costs_if_applicable",
    )
    software_investment.historical = None

    free_cash_flow = _find_sheet_item(
        model,
        "Financial_model",
        "financial_model.free_cash_flow",
    )
    for spec in (free_cash_flow.historical, free_cash_flow.projected):
        if spec is None:
            continue
        operands = spec.params.get("operands")
        if not isinstance(operands, list) or operands[:1] != ["+"]:
            raise AssertionError("free cash flow is missing additive operands")
        refs = operands[1:]
        if any(not isinstance(ref, LineItemRef) for ref in refs):
            raise AssertionError("free cash flow contains non-reference operands")
        ref_ids = {ref.id for ref in refs}
        required_ids = {
            "financial_model.net_cash_provided_by_operating_activities",
            "financial_model.purchases_of_property_and_equipment",
        }
        if not required_ids <= ref_ids:
            raise AssertionError("free cash flow is missing CFO or PP&E")
        spec.params["operands"] = [
            "+",
            LineItemRef(id="financial_model.net_cash_provided_by_operating_activities"),
            LineItemRef(id="financial_model.purchases_of_property_and_equipment"),
            LineItemRef(id="financial_model.capitalized_software_costs_if_applicable"),
        ]

# The DCF discounts at WACC, so its numerator must be unlevered.  The model's
# free cash flow row is ``CFO - capex``: financing cash flows never enter it, so
# there is no net borrowing to strip out -- but the tax inside CFO is levied on a
# pretax base the debt schedule reduces by interest expense, which leaves the row
# short by exactly the after-tax interest.  That shortfall is structural and
# holds for every issuer, so the repair is one authored row rather than any
# issuer-specific arithmetic.
UNLEVERED_FREE_CASH_FLOW_ITEM_ID = "tpl.fm.cash_flow.unlevered_free_cash_flow"
FREE_CASH_FLOW_ITEM_ID = "tpl.fm.cash_flow.free_cash_flow"
# The Financial_model interest row the checked-in template already carries.  It
# holds the issuer's reported ``interest_expense`` concept, and the build's debt
# schedule seeds both its own interest row and the income statement's from that
# same observation.  An issuer that does not report the concept has its interest
# inside the other-income aggregate instead, and
# ``schema.build_financial_schedules`` refuses the unlevered basis there.
UNLEVERED_INTEREST_EXPENSE_ITEM_ID = "tpl.fm.adjusted_earnings.interest_expense"
# The model's own book effective rate: income tax expense / pretax income.  It
# is an average rate standing in for the marginal rate that actually shielded
# the interest, which is exact only while projected tax is proportional to
# projected pretax income -- true of every model this template produces today,
# and the assumption to revisit for the first issuer whose tax schedule carries
# a component that is not.
UNLEVERED_TAX_RATE_ITEM_ID = "tpl.fm.margins.tax_rate"
UNLEVERED_FREE_CASH_FLOW_LABEL = "Unlevered free cash flow"
UNLEVERED_FREE_CASH_FLOW_NOTE = "unlevered_free_cash_flow_after_tax_interest_addback"

_SOURCE_UNLEVERED_FREE_CASH_FLOW_ID = "financial_model.unlevered_free_cash_flow"
_SOURCE_FREE_CASH_FLOW_ID = "financial_model.free_cash_flow"
_SOURCE_INTEREST_EXPENSE_ID = "financial_model.interest_expense"
_SOURCE_TAX_RATE_ID = "financial_model.tax_rate"


def unlevered_free_cash_flow_formula(
    *,
    free_cash_flow_id: str = FREE_CASH_FLOW_ITEM_ID,
    interest_expense_id: str = UNLEVERED_INTEREST_EXPENSE_ITEM_ID,
    tax_rate_id: str = UNLEVERED_TAX_RATE_ITEM_ID,
) -> FormulaSpec:
    """``free cash flow + interest expense x (1 - tax rate)``.

    The ids are parameters because the template writer authors this before the
    ``tpl.*`` rename and every consumer afterwards reads it with the canonical
    ids; the arithmetic is the same object either way.
    """

    return FormulaSpec(
        type=FormulaType.arithmetic,
        subtype="multi_term",
        params={
            "operands": [
                "+",
                LineItemRef(id=free_cash_flow_id),
                {
                    "op": "*",
                    "args": [
                        LineItemRef(id=interest_expense_id),
                        {
                            "op": "-",
                            "left": 1.0,
                            "right": LineItemRef(id=tax_rate_id),
                        },
                    ],
                },
            ]
        },
        note=UNLEVERED_FREE_CASH_FLOW_NOTE,
    )


def _insert_unlevered_free_cash_flow_row(model: FinancialModel) -> None:
    """Add the DCF's unlevered numerator beside the free cash flow row.

    It lives in the cash flow statement rather than on the Valuation sheet
    because it is a cash-flow measure; the DCF only points at it, exactly as it
    pointed at the levered row before.

    Projected years only.  The conversion divides by the model's own book
    effective tax rate, and a reported year in which the issuer booked a tax
    benefit on positive pretax income carries a negative one -- which grosses
    the add-back *up*, above the interest itself.  The DCF reads the projection
    strip and nothing reads the reported years, so the row leaves them blank
    rather than displaying a figure that is not a comparable history.
    """

    financial_sheet = model.sheets["Financial_model"]
    if any(
        item.id == _SOURCE_UNLEVERED_FREE_CASH_FLOW_ID for _, item in _iter_items(model)
    ):
        raise AssertionError("Synthetic unlevered free cash flow row already exists")

    free_cash_flow = _find_sheet_item(
        model, "Financial_model", _SOURCE_FREE_CASH_FLOW_ID
    )
    unlevered_row = int(free_cash_flow.row) + 1
    # The row after free cash flow is the sheet's blank separator before the
    # financing header.  Take it and push the rest of the sheet down, so the
    # section spacing every other header on this sheet has is preserved.
    for section in financial_sheet.sections:
        for item in section.line_items:
            if item.row >= unlevered_row:
                item.row += 1

    target_section = _find_section_with_item(financial_sheet, _SOURCE_FREE_CASH_FLOW_ID)
    position = [item.id for item in target_section.line_items].index(
        _SOURCE_FREE_CASH_FLOW_ID
    )

    target_section.line_items.insert(
        position + 1,
        LineItem(
            id=_SOURCE_UNLEVERED_FREE_CASH_FLOW_ID,
            label=UNLEVERED_FREE_CASH_FLOW_LABEL,
            row=unlevered_row,
            item_type=ItemType.derived,
            unit=Unit.dollars,
            projected=unlevered_free_cash_flow_formula(
                free_cash_flow_id=_SOURCE_FREE_CASH_FLOW_ID,
                interest_expense_id=_SOURCE_INTEREST_EXPENSE_ID,
                tax_rate_id=_SOURCE_TAX_RATE_ID,
            ),
        ),
    )


def _clear_cash_flow_artifact_historicals(model: FinancialModel) -> None:
    for item_id in CASH_FLOW_ARTIFACT_HISTORICAL_IDS:
        _find_sheet_item(model, "Financial_model", item_id).historical = None

def _set_if_applicable_default_zero_historicals(model: FinancialModel) -> None:
    historical_periods = list(
        model.time_structure.historical_periods or model.time_structure.historical_years or []
    )
    projection_periods = list(
        model.time_structure.projection_periods or model.time_structure.projection_years or []
    )
    for item_id in IF_APPLICABLE_DEFAULT_ZERO_IDS:
        item = _find_sheet_item(model, "Financial_model", item_id)
        item.historical = FormulaSpec(
            type=FormulaType.constant,
            subtype=None,
            params={"value": 0},
            note=None,
        )
        existing = [int(p) for p in (item.formula_periods or [])]
        merged = sorted(
            set(existing)
            | {int(p) for p in historical_periods}
            | {int(p) for p in projection_periods}
        )
        item.formula_periods = merged

def _normalize_cash_flow_projection_links(model: FinancialModel) -> None:
    projection_periods = {
        int(period)
        for period in (
            model.time_structure.projection_periods
            or model.time_structure.projection_years
            or []
        )
    }
    if not projection_periods:
        return

    net_income_item = _find_sheet_item(model, "Financial_model", "tpl.fm.cash_flow.net_income")
    net_income_item.projected = FormulaSpec(
        type=FormulaType.ref,
        params={"source": LineItemRef(id=CASH_FLOW_NET_INCOME_SOURCE_ID)},
    )

    for item_id in sorted(CASH_FLOW_FULL_PROJECTION_IDS):
        item = _find_sheet_item(model, "Financial_model", item_id)
        item.formula_periods = sorted(
            {int(period) for period in (item.formula_periods or [])}
            | projection_periods
        )

    for item_id in sorted(CASH_FLOW_PER_SHARE_PROJECTION_IDS):
        item = _find_sheet_item(model, "Financial_model", item_id)
        if item.historical is not None:
            item.projected = item.historical.model_copy(deep=True)
        item.formula_periods = sorted(
            {int(period) for period in (item.formula_periods or [])}
            | projection_periods
        )

def _normalize_valuation_projection_links(model: FinancialModel) -> None:
    projection_periods = [
        int(period)
        for period in (
            model.time_structure.projection_periods
            or model.time_structure.projection_years
            or []
        )
    ]
    if not projection_periods:
        return

    stock_based_compensation = _find_sheet_item(
        model,
        "Financial_model",
        "tpl.fm.adjusted_earnings.stock_based_compensation_add_back",
    )
    stock_based_compensation_ref = FormulaSpec(
        type=FormulaType.ref,
        params={
            "source": LineItemRef(id="tpl.a.stock_based_compensation.stock_based_compensation"),
        },
    )
    stock_based_compensation.historical = stock_based_compensation_ref
    stock_based_compensation.projected = stock_based_compensation_ref

    adjusted_operating_income = _find_sheet_item(
        model,
        "Financial_model",
        "tpl.fm.adjusted_earnings.adjusted_operating_income",
    )
    adjusted_operating_income.projected = FormulaSpec(
        type=FormulaType.arithmetic,
        params={
            "operands": [
                "+",
                LineItemRef(id="tpl.fm.income_statement.operating_income"),
                LineItemRef(id="tpl.fm.adjusted_earnings.stock_based_compensation_add_back"),
                LineItemRef(id="tpl.fm.adjusted_earnings.other_items"),
                LineItemRef(id="tpl.fm.adjusted_earnings.amortization_of_acquired_intangibles"),
            ]
        },
    )
    # Adjusted operating income is operating income plus the operating
    # add-backs in every column. Derived from adjusted EBITDA instead, its
    # historical column would carry the bridge's non-operating income.
    adjusted_operating_income.historical = adjusted_operating_income.projected.model_copy(
        deep=True
    )
    adjusted_operating_income.formula_periods = sorted(
        {int(period) for period in (adjusted_operating_income.formula_periods or [])}
        | set(projection_periods[-2:])
    )

    assumptions_adjusted_operating_income = _find_sheet_item(
        model,
        "Assumptions",
        "tpl.a.adj_operating_income.adjusted_operating_income",
    )
    assumptions_adjusted_operating_income.projected = FormulaSpec(
        type=FormulaType.ref,
        params={
            "source": LineItemRef(id="tpl.fm.adjusted_earnings.adjusted_operating_income"),
        },
    )

    # One adjusted-EBITDA definition: the Assumptions block reads the
    # Financial_model bridge rows instead of recomputing them from partly
    # different upstream rows. ``other_items`` stays the forecast input the
    # Financial_model row reads forward.
    for assumptions_id, financial_model_id in (
        ("tpl.a.adj_ebitda.net_income", "tpl.fm.adjusted_earnings.net_income_base"),
        ("tpl.a.adj_ebitda.interest_expense", "tpl.fm.adjusted_earnings.interest_expense"),
        ("tpl.a.adj_ebitda.income_tax_expense", "tpl.fm.adjusted_earnings.income_tax_expense"),
        (
            "tpl.a.adj_ebitda.depreciation_and_amortization",
            "tpl.fm.adjusted_earnings.depreciation_and_amortization",
        ),
        ("tpl.a.adj_ebitda.ebtida", "tpl.fm.adjusted_earnings.ebitda"),
        ("tpl.a.adj_ebitda.adjusted_ebitda", "tpl.fm.adjusted_earnings.adjusted_ebitda"),
    ):
        assumptions_row = _find_sheet_item(model, "Assumptions", assumptions_id)
        financial_model_ref = FormulaSpec(
            type=FormulaType.ref,
            subtype="cross_sheet_ref",
            params={"source": LineItemRef(id=financial_model_id)},
        )
        assumptions_row.historical = financial_model_ref
        assumptions_row.projected = financial_model_ref.model_copy(deep=True)

    long_term_debt = _find_sheet_item(model, "Financial_model", "tpl.fm.balance_sheet.long_term_debt")
    long_term_debt.projected = FormulaSpec(
        type=FormulaType.ref,
        params={"source": LineItemRef(id="tpl.a.capital_sources.long_term_debt")},
    )
    long_term_debt.formula_periods = projection_periods

    net_cash = _find_sheet_item(model, "Financial_model", "tpl.fm.balance_sheet.net_cash")
    net_cash.projected = FormulaSpec(
        type=FormulaType.arithmetic,
        params={
            "operands": [
                "-",
                LineItemRef(id="tpl.fm.balance_sheet.cash_and_marketable_securities"),
                LineItemRef(id="tpl.fm.balance_sheet.long_term_debt"),
            ]
        },
    )
    net_cash.formula_periods = sorted(
        {int(period) for period in (net_cash.formula_periods or [])}
        | set(projection_periods)
    )

def _normalize_sbc_per_line_rate_historicals(model: FinancialModel) -> None:
    """Bind the R&D and G&A per-line SBC rate rows to a historical
    ratio(component[t=0] / opex_base[t=-1]), mirroring cost_of_revenues and
    sales_and_marketing.

    The generic source workbook carries the ratio for cost/S&M but leaves R&D/G&A
    blank, so those two rate rows have ``historical=None`` and a truncated
    ``formula_periods``. Each template rate row represents its per-line SBC component
    divided by the prior-year operating-expense base. Setting the ratio and widening
    ``formula_periods`` to the historical window completes those row definitions, and
    the projected carry-forward propagates the rate. Idempotent + self-limiting: only
    touches rate rows that currently lack a historical formula (cost/S&M keep their
    workbook-authored ratio untouched).
    """

    # Mirror the working sibling's formula_periods (cost_of_revenues carries the ratio
    # from the workbook) rather than recomputing from the model years — the two must be
    # remapped identically to the target ticker, and the workbook window is authoritative.
    try:
        sibling = _find_sheet_item(
            model,
            "Assumptions",
            "tpl.a.stock_based_compensation.cost_of_revenues_pct_line_item",
        )
    except (AssertionError, KeyError):
        return
    sibling_formula_periods = sorted({int(period) for period in (sibling.formula_periods or [])})
    if not sibling_formula_periods:
        return
    per_line = (
        (
            "tpl.a.stock_based_compensation.research_and_development_pct_line_item",
            "tpl.a.stock_based_compensation.research_and_development",
            "tpl.a.operating_leverage.research_and_development",
        ),
        (
            "tpl.a.stock_based_compensation.general_and_administrative_pct_line_item",
            "tpl.a.stock_based_compensation.general_and_administrative",
            "tpl.a.operating_leverage.general_and_administrative",
        ),
    )
    for rate_id, component_id, base_id in per_line:
        try:
            rate = _find_sheet_item(model, "Assumptions", rate_id)
        except (AssertionError, KeyError):
            continue
        if rate.historical is not None:
            continue
        rate.historical = FormulaSpec(
            type=FormulaType.ratio,
            subtype="divide",
            params={
                "numerator": LineItemRef(id=component_id, t=0),
                "denominator": LineItemRef(id=base_id, t=-1),
            },
        )
        rate.formula_periods = list(sibling_formula_periods)


def _normalize_generic_cash_reconciliation(model: FinancialModel) -> None:
    forex_item_id = "financial_model.effect_of_exchange_rate_on_cash"
    if any(item.id == forex_item_id for _, item in _iter_items(model)):
        raise AssertionError("Synthetic effect_of_exchange_rate_on_cash row already exists")

    net_change_item = _find_sheet_item(
        model,
        "Financial_model",
        "financial_model.net_change_in_cash_and_cash_equivalents",
    )
    forex_row = net_change_item.row - 1
    if any(
        sheet_name == "Financial_model" and item.row == forex_row
        for sheet_name, item in _iter_items(model)
    ):
        raise AssertionError("Synthetic effect_of_exchange_rate_on_cash row target is occupied")

    target_section = _find_section_with_item(
        model.sheets["Financial_model"],
        "financial_model.net_change_in_cash_and_cash_equivalents",
    )
    target_section.line_items.append(
        LineItem(
            id=forex_item_id,
            label="Effect of exchange rate on cash",
            row=forex_row,
            item_type=ItemType.derived,
            unit=Unit.dollars,
        )
    )

    for spec in (net_change_item.historical, net_change_item.projected):
        if spec is None:
            continue
        operands = spec.params.get("operands")
        if not isinstance(operands, list) or not operands or operands[0] != "+":
            raise AssertionError("net_change_in_cash_and_cash_equivalents formula has unexpected operands shape")
        if not any(isinstance(ref, LineItemRef) and ref.id == forex_item_id for ref in operands[1:]):
            operands.append(LineItemRef(id=forex_item_id))

    end_item = _find_sheet_item(
        model,
        "Financial_model",
        "financial_model.cash_and_cash_equivalents_end_of_period",
    )
    cash_item = _find_sheet_item(
        model,
        "Financial_model",
        "financial_model.cash_and_cash_equivalents",
    )

    end_item.historical = FormulaSpec(
        type=FormulaType.arithmetic,
        params={
            "operands": [
                "+",
                LineItemRef(id="financial_model.cash_and_cash_equivalents"),
                LineItemRef(id="financial_model.funds_held_for_client_s_cash_and_cash_equivalents"),
            ]
        },
    )
    cash_item.historical = None
    if cash_item.projected is None:
        cash_item.formula_periods = None


#: The house revenue ratios an authored forecast-assumption row answers: each
#: is an input rate row directly below the row it drives, so the compiler binds
#: the template input rather than inserting it only when a projection has a basis.
#: ``(anchor id, rate row id, label, driver category, numerator id)``, in the
#: generic artifact's tpl ids. The numerator is the balance or cash-flow row the
#: house policy divides by revenue; reported years use that same-period ratio,
#: independent of the latest observed ratio held as a forecast default. Missing
#: source operands stay missing. ``None`` is a ratio with no single-row numerator
#: (``depreciation_pct_revenue`` is a residual of several rows), which carries
#: no historical formula.
HOUSE_REVENUE_RATIO_ROWS: tuple[tuple[str, str, str, str, Optional[str]], ...] = (
    (
        "tpl.a.depreciation_amortization.depreciation",
        "tpl.a.depreciation_amortization.depreciation_pct_revenue",
        "  Depreciation as % of revenue",
        "reinvestment",
        None,
    ),
    (
        "tpl.a.balance_sheet_wc.of_rev",
        "tpl.a.balance_sheet_wc.prepaid_pct_revenue",
        "Prepaid expenses and other as % of revenue",
        "reinvestment",
        "tpl.fm.balance_sheet.current_asset_3",
    ),
    (
        "tpl.a.capital_sources.current_operating_lease_liabilities",
        "tpl.a.capital_sources.current_operating_lease_liabilities_pct_revenue",
        "  Current operating lease liabilities as % of revenue",
        "capital_sources",
        "tpl.a.capital_sources.current_operating_lease_liabilities",
    ),
    (
        "tpl.a.dividends_shares.proceeds_from_employee_stock_purchase_plan",
        "tpl.a.dividends_shares.employee_stock_plan_proceeds_pct_revenue",
        "  Employee stock plan proceeds as % of revenue",
        "capital_sources",
        "tpl.a.dividends_shares.proceeds_from_employee_stock_purchase_plan",
    ),
)

#: The revenue row every house revenue ratio divides by.
_HOUSE_REVENUE_RATIO_DENOMINATOR_ID = "tpl.fm.income_statement.total_revenue"


ACQUISITION_GOODWILL_ALLOCATION_ITEM_ID = (
    "tpl.a.intangibles_goodwill.acquisition_goodwill_allocation"
)
_ACQUISITION_GOODWILL_ADDITION_ANCHOR_ID = (
    "tpl.a.intangibles_goodwill.add_less_acquisitions_write_offs"
)


def _insert_acquisition_goodwill_allocation_row(model: FinancialModel) -> None:
    """Insert the authored goodwill-split fraction under the goodwill addition.

    The row is the forecast-control profile's ``acquisition_goodwill_allocation``
    target.  The bridge proposal writes one until a split is authored, so the
    goodwill addition equals the negated acquisition cash-flow row.
    """

    sheet = model.sheets["Assumptions"]
    section = _find_section_with_item(sheet, _ACQUISITION_GOODWILL_ADDITION_ANCHOR_ID)
    anchor_index, anchor = next(
        (index, item)
        for index, item in enumerate(section.line_items)
        if item.id == _ACQUISITION_GOODWILL_ADDITION_ANCHOR_ID
    )
    if any(
        item.id == ACQUISITION_GOODWILL_ALLOCATION_ITEM_ID
        for sheet_section in sheet.sections
        for item in sheet_section.line_items
    ):
        raise AssertionError(
            f"goodwill allocation row {ACQUISITION_GOODWILL_ALLOCATION_ITEM_ID} already exists"
        )
    insert_row = int(anchor.row) + 1
    for sheet_section in sheet.sections:
        for item in sheet_section.line_items:
            if int(item.row) >= insert_row:
                item.row = int(item.row) + 1
    section.line_items.insert(
        anchor_index + 1,
        LineItem.model_validate(
            {
                "id": ACQUISITION_GOODWILL_ALLOCATION_ITEM_ID,
                "label": "Acquisition cost allocated to goodwill",
                "row": insert_row,
                "item_type": "input",
                "unit": "percentage",
                "model_scale": "units",
                "style": {"color": "input_blue"},
                "driver_category": "reinvestment",
                "customization": "optional",
            }
        ),
    )


def _insert_house_revenue_ratio_rows(model: FinancialModel) -> None:
    """Insert each house-ratio rate row below its anchor, shifting later rows."""

    historical_periods = sorted(int(period) for period in model.time_structure.historical_periods)
    for anchor_id, item_id, label, driver_category, numerator_id in HOUSE_REVENUE_RATIO_ROWS:
        sheet = model.sheets["Assumptions"]
        section = _find_section_with_item(sheet, anchor_id)
        anchor_index, anchor = next(
            (index, item)
            for index, item in enumerate(section.line_items)
            if item.id == anchor_id
        )
        if any(
            item.id == item_id
            for sheet_section in sheet.sections
            for item in sheet_section.line_items
        ):
            raise AssertionError(f"house-ratio rate row {item_id} already exists")
        insert_row = int(anchor.row) + 1
        for sheet_section in sheet.sections:
            for item in sheet_section.line_items:
                if int(item.row) >= insert_row:
                    item.row = int(item.row) + 1
        row = LineItem.model_validate(
            {
                "id": item_id,
                "label": label,
                "row": insert_row,
                "item_type": "input",
                "unit": "percentage",
                "model_scale": "units",
                "style": {"color": "input_blue"},
                "driver_category": driver_category,
                "customization": "optional",
            }
        )
        if numerator_id is not None:
            row.historical = FormulaSpec(
                type=FormulaType.ratio,
                subtype="divide",
                params={
                    "numerator": LineItemRef(id=numerator_id),
                    "denominator": LineItemRef(id=_HOUSE_REVENUE_RATIO_DENOMINATOR_ID),
                },
            )
            row.formula_periods = list(historical_periods)
        section.line_items.insert(anchor_index + 1, row)


#: The face row that presents the treasury-stock contra-equity the Assumptions
#: share schedule carries. Shareholders' equity foots over the rows its own
#: sheet displays, so the template's ``total_equity`` sums this row: a filed
#: ``StockholdersEquity`` net of ``TreasuryStockValue`` is reproduced by the
#: rendered children wherever the pin binds ``statement.bs.treasury_stock``
#: (onto the schedule row, the role's population item).
TREASURY_STOCK_FACE_ITEM_ID = "tpl.fm.balance_sheet.treasury_stock"
TREASURY_STOCK_SCHEDULE_ITEM_ID = "tpl.a.dividends_shares.treasury_stock"
_TOTAL_EQUITY_ITEM_ID = "tpl.fm.balance_sheet.total_equity"
_TREASURY_STOCK_ANCHOR_ID = "tpl.fm.balance_sheet.accumulated_other_comprehensive_income_loss"


def _insert_treasury_stock_face_row(model: FinancialModel) -> None:
    """Show the schedule's treasury stock on the balance sheet, summed by equity."""

    sheet = model.sheets["Financial_model"]
    if any(item.id == TREASURY_STOCK_FACE_ITEM_ID for section in sheet.sections for item in section.line_items):
        raise AssertionError(f"treasury-stock face row {TREASURY_STOCK_FACE_ITEM_ID} already exists")
    section = _find_section_with_item(sheet, _TREASURY_STOCK_ANCHOR_ID)
    anchor_index, anchor = next(
        (index, item)
        for index, item in enumerate(section.line_items)
        if item.id == _TREASURY_STOCK_ANCHOR_ID
    )
    total_equity = _find_sheet_item(model, "Financial_model", _TOTAL_EQUITY_ITEM_ID)
    insert_row = int(anchor.row) + 1
    for sheet_section in sheet.sections:
        for item in sheet_section.line_items:
            if int(item.row) >= insert_row:
                item.row = int(item.row) + 1
    schedule_ref = FormulaSpec(
        type=FormulaType.ref,
        subtype="cross_sheet_ref",
        params={"source": LineItemRef(id=TREASURY_STOCK_SCHEDULE_ITEM_ID)},
    )
    section.line_items.insert(
        anchor_index + 1,
        LineItem(
            id=TREASURY_STOCK_FACE_ITEM_ID,
            label="Treasury stock",
            row=insert_row,
            item_type=ItemType.derived,
            unit=Unit.dollars,
            model_scale=total_equity.model_scale,
            driver_category=anchor.driver_category,
            customization=anchor.customization,
            style=anchor.style.model_copy(deep=True) if anchor.style is not None else None,
            historical=schedule_ref,
            projected=schedule_ref.model_copy(deep=True),
        ),
    )


#: The accounting baseline carries a face row for every balance an
#: issuer reports, so a filed balance always rolls into the projected balance
#: sheet its total sums. ``(item_id, label, anchor_id, schedule_id)``: a row
#: with a ``schedule_id`` presents that schedule's balance (the role's
#: population item) in every period; a row without one is the face row its
#: role stamps (`required_role_catalog.json`). The total a row rolls into is
#: its role's ``parent_total``, which `_generate_statement_totals` reads.
#: Order is insertion order: each row goes directly below its anchor.
BALANCE_SHEET_FACE_ROWS: tuple[tuple[str, str, str, str | None], ...] = (
    (
        "tpl.fm.balance_sheet.other_current_assets",
        " Other current assets",
        "tpl.fm.balance_sheet.current_asset_3",
        None,
    ),
    (
        "tpl.fm.balance_sheet.accrued_compensation",
        " Accrued compensation and benefits",
        "tpl.fm.balance_sheet.accrued_expenses",
        None,
    ),
    (
        "tpl.fm.balance_sheet.deferred_revenue_current",
        " Deferred revenue, current",
        "tpl.fm.balance_sheet.accrued_compensation",
        "tpl.a.balance_sheet_wc.deferred_revenues",
    ),
    (
        "tpl.fm.balance_sheet.current_operating_lease_liabilities",
        " Current operating lease liabilities",
        "tpl.fm.balance_sheet.deferred_revenue_current",
        "tpl.a.capital_sources.current_operating_lease_liabilities",
    ),
    (
        "tpl.fm.balance_sheet.noncontrolling_interest",
        "Noncontrolling interests",
        "tpl.fm.balance_sheet.treasury_stock",
        None,
    ),
)


def _insert_balance_sheet_face_rows(model: FinancialModel) -> None:
    """Give each baseline balance its face row beside its anchor."""

    sheet = model.sheets["Financial_model"]
    catalog = load_required_role_catalog()
    for item_id, label, anchor_id, schedule_id in BALANCE_SHEET_FACE_ROWS:
        if any(item.id == item_id for section in sheet.sections for item in section.line_items):
            raise AssertionError(f"balance-sheet face row {item_id} already exists")
        section = _find_section_with_item(sheet, anchor_id)
        anchor_index, anchor = next(
            (index, item)
            for index, item in enumerate(section.line_items)
            if item.id == anchor_id
        )
        total_id = catalog.role_for_face_line(item_id).parent_total
        if total_id is None:
            raise AssertionError(f"balance-sheet face row {item_id} rolls into no total")
        total = _find_sheet_item(model, "Financial_model", total_id)
        insert_row = int(anchor.row) + 1
        if any(
            int(item.row) == insert_row
            for sheet_section in sheet.sections
            for item in sheet_section.line_items
        ):
            for sheet_section in sheet.sections:
                for item in sheet_section.line_items:
                    if int(item.row) >= insert_row:
                        item.row = int(item.row) + 1
        schedule_ref = (
            None
            if schedule_id is None
            else FormulaSpec(
                type=FormulaType.ref,
                subtype="cross_sheet_ref",
                params={"source": LineItemRef(id=schedule_id)},
            )
        )
        section.line_items.insert(
            anchor_index + 1,
            LineItem(
                id=item_id,
                label=label,
                row=insert_row,
                item_type=ItemType.derived,
                unit=Unit.dollars,
                model_scale=total.model_scale,
                driver_category=anchor.driver_category,
                customization=CustomizationType.optional,
                style=total.style.model_copy(deep=True) if total.style is not None else None,
                historical=schedule_ref,
                projected=None if schedule_ref is None else schedule_ref.model_copy(deep=True),
            ),
        )


SHORT_TERM_INVESTMENTS_ITEM_ID = "tpl.fm.cash_flow.short_term_investments"
_CASH_AND_EQUIVALENTS_ITEM_ID = "tpl.fm.cash_flow.cash_and_cash_equivalents"


def _insert_short_term_investments_row(model: FinancialModel) -> None:
    """Hold reported short-term investments beside reported cash and equivalents.

    Both are inputs of the balance sheet's cash-and-short-term-investments
    line (the catalog's registered formula); this row is where the
    short-term-investments role lands, in the cash reconciliation block that
    already carries cash and cash equivalents.
    """

    sheet = model.sheets["Financial_model"]
    if any(
        item.id == SHORT_TERM_INVESTMENTS_ITEM_ID
        for section in sheet.sections
        for item in section.line_items
    ):
        raise AssertionError(f"{SHORT_TERM_INVESTMENTS_ITEM_ID} already exists")
    section = _find_section_with_item(sheet, _CASH_AND_EQUIVALENTS_ITEM_ID)
    anchor_index, anchor = next(
        (index, item)
        for index, item in enumerate(section.line_items)
        if item.id == _CASH_AND_EQUIVALENTS_ITEM_ID
    )
    insert_row = int(anchor.row) + 1
    if any(
        int(item.row) == insert_row
        for sheet_section in sheet.sections
        for item in sheet_section.line_items
    ):
        for sheet_section in sheet.sections:
            for item in sheet_section.line_items:
                if int(item.row) >= insert_row:
                    item.row = int(item.row) + 1
    section.line_items.insert(
        anchor_index + 1,
        LineItem(
            id=SHORT_TERM_INVESTMENTS_ITEM_ID,
            label=" Short-term investments",
            row=insert_row,
            item_type=anchor.item_type,
            unit=Unit.dollars,
            model_scale=anchor.model_scale,
            driver_category=anchor.driver_category,
            customization=CustomizationType.optional,
            style=anchor.style.model_copy(deep=True) if anchor.style is not None else None,
        ),
    )


#: The basic per-share face rows the catalog stamps, as
#: ``(item_id, label, anchor_id, unit)``: basic EPS directly below the
#: diluted EPS row, weighted-average basic shares directly below the
#: diluted shares row. Plain face rows — the required-line fill lands the
#: pinned filing line for each row's role; no template arithmetic reads
#: them, so no total changes.
_PER_SHARE_FACE_ROWS: tuple[tuple[str, str, str, Unit], ...] = (
    (
        "tpl.fm.income_statement.eps_basic",
        " EPS (basic)",
        "tpl.fm.income_statement.eps",
        Unit.per_share,
    ),
    (
        "tpl.fm.income_statement.basic_shares_outstanding_m",
        " Basic shares outstanding (m)",
        "tpl.fm.income_statement.diluted_shares_outstanding_m",
        Unit.share_count,
    ),
)


def _insert_per_share_face_rows(model: FinancialModel) -> None:
    """Give basic EPS and basic shares their income-statement face rows.

    Both are GAAP face disclosures the catalog carries roles for; without
    a template row each role's filed line lands nowhere. Each row takes
    its diluted sibling's scale, driver category and style.
    """

    sheet = model.sheets["Financial_model"]
    for item_id, label, anchor_id, unit in _PER_SHARE_FACE_ROWS:
        if any(
            item.id == item_id
            for section in sheet.sections
            for item in section.line_items
        ):
            raise AssertionError(f"per-share face row {item_id} already exists")
        section = _find_section_with_item(sheet, anchor_id)
        anchor_index, anchor = next(
            (index, item)
            for index, item in enumerate(section.line_items)
            if item.id == anchor_id
        )
        insert_row = int(anchor.row) + 1
        if any(
            int(item.row) == insert_row
            for sheet_section in sheet.sections
            for item in sheet_section.line_items
        ):
            for sheet_section in sheet.sections:
                for item in sheet_section.line_items:
                    if int(item.row) >= insert_row:
                        item.row = int(item.row) + 1
        section.line_items.insert(
            anchor_index + 1,
            LineItem(
                id=item_id,
                label=label,
                row=insert_row,
                item_type=anchor.item_type,
                unit=unit,
                model_scale=anchor.model_scale,
                driver_category=anchor.driver_category,
                customization=CustomizationType.optional,
                style=anchor.style.model_copy(deep=True) if anchor.style is not None else None,
            ),
        )


OPERATING_DEPRECIATION_AMORTIZATION_ITEM_ID = (
    "tpl.fm.income_statement.depreciation_and_amortization"
)
_GENERAL_AND_ADMINISTRATIVE_ITEM_ID = "tpl.fm.income_statement.general_and_administrative"
_TOTAL_OPERATING_EXPENSES_ITEM_ID = "tpl.fm.income_statement.total_operating_expenses"
_CASH_FLOW_DEPRECIATION_AMORTIZATION_ITEM_ID = "tpl.fm.cash_flow.depreciation_and_amortization"


def _insert_operating_depreciation_amortization_row(model: FinancialModel) -> None:
    """Give an operating-expense D&A line its face row inside total operating expenses.

    Some issuers print depreciation and amortization as its own line of the
    operating-expense block (PAYC's administrative expenses), so the filed
    total is the functional lines plus it. The row is optional: an issuer
    that reports no such line reads zero in history (the same constant the
    if-applicable rows carry), and a pinned filing value overrides it. In
    forecast it holds its last-historical share of total D&A
    (``cash_flow.depreciation_and_amortization``), so it rides the existing
    D&A schedule and an issuer without the line projects zero.
    """

    sheet = model.sheets["Financial_model"]
    if any(
        item.id == OPERATING_DEPRECIATION_AMORTIZATION_ITEM_ID
        for section in sheet.sections
        for item in section.line_items
    ):
        raise AssertionError(f"{OPERATING_DEPRECIATION_AMORTIZATION_ITEM_ID} already exists")
    section = _find_section_with_item(sheet, _GENERAL_AND_ADMINISTRATIVE_ITEM_ID)
    anchor_index, anchor = next(
        (index, item)
        for index, item in enumerate(section.line_items)
        if item.id == _GENERAL_AND_ADMINISTRATIVE_ITEM_ID
    )
    total = _find_sheet_item(model, "Financial_model", _TOTAL_OPERATING_EXPENSES_ITEM_ID)
    insert_row = int(anchor.row) + 1
    for sheet_section in sheet.sections:
        for item in sheet_section.line_items:
            if int(item.row) >= insert_row:
                item.row = int(item.row) + 1
    historical_periods = list(
        model.time_structure.historical_periods or model.time_structure.historical_years or []
    )
    projection_periods = list(
        model.time_structure.projection_periods or model.time_structure.projection_years or []
    )
    section.line_items.insert(
        anchor_index + 1,
        LineItem(
            id=OPERATING_DEPRECIATION_AMORTIZATION_ITEM_ID,
            label=" Depreciation and amortization",
            row=insert_row,
            item_type=ItemType.derived,
            unit=Unit.dollars,
            model_scale=anchor.model_scale,
            driver_category=anchor.driver_category,
            customization=CustomizationType.optional,
            style=anchor.style.model_copy(deep=True) if anchor.style is not None else None,
            historical=FormulaSpec(type=FormulaType.constant, params={"value": 0}),
            projected=FormulaSpec(
                type=FormulaType.driver,
                subtype="base_x_rate",
                params={
                    "base": LineItemRef(id=_CASH_FLOW_DEPRECIATION_AMORTIZATION_ITEM_ID),
                    "rate": {
                        "op": "/",
                        "left": LineItemRef(id=OPERATING_DEPRECIATION_AMORTIZATION_ITEM_ID, t=-1),
                        "right": LineItemRef(id=_CASH_FLOW_DEPRECIATION_AMORTIZATION_ITEM_ID, t=-1),
                    },
                },
                note="held_last_historical_share_of_total_depreciation_and_amortization",
            ),
            formula_periods=sorted(
                {int(period) for period in historical_periods}
                | {int(period) for period in projection_periods}
            ),
        ),
    )
    for spec in (total.historical, total.projected):
        if spec is None or spec.params.get("function") != "SUM":
            raise AssertionError(
                f"{_TOTAL_OPERATING_EXPENSES_ITEM_ID} formula is not the expected SUM over its rows"
            )
        spec.params["items"] = _insert_sum_ref_after(
            spec.params.get("items"),
            after_id=_GENERAL_AND_ADMINISTRATIVE_ITEM_ID,
            new_id=OPERATING_DEPRECIATION_AMORTIZATION_ITEM_ID,
            context=_TOTAL_OPERATING_EXPENSES_ITEM_ID,
        )


SELLING_GENERAL_ADMINISTRATIVE_ITEM_ID = "tpl.fm.income_statement.selling_general_and_administrative"
_SALES_AND_MARKETING_ITEM_ID = "tpl.fm.income_statement.sales_and_marketing"


def _insert_selling_general_administrative_row(model: FinancialModel) -> None:
    """Give a combined SG&A line its face row beside the functional lines.

    An issuer that prints one combined SG&A line (ETN, HD) has no separate
    selling or administrative figure, so that filed figure lands here and is a
    member of historical total operating expenses only. Forecast opex stays the
    functional lines; this row's projected SUM(S&M, G&A) is a readout of them,
    never a second operand. An issuer that prints the functional lines cites
    this row's absence, and the optional-participation rule drops it from the
    historical total, so a total counts the combined line or the functional
    pair, never both.
    """

    sheet = model.sheets["Financial_model"]
    if any(
        item.id == SELLING_GENERAL_ADMINISTRATIVE_ITEM_ID
        for section in sheet.sections
        for item in section.line_items
    ):
        raise AssertionError(f"{SELLING_GENERAL_ADMINISTRATIVE_ITEM_ID} already exists")
    section = _find_section_with_item(sheet, _GENERAL_AND_ADMINISTRATIVE_ITEM_ID)
    anchor_index, anchor = next(
        (index, item)
        for index, item in enumerate(section.line_items)
        if item.id == _GENERAL_AND_ADMINISTRATIVE_ITEM_ID
    )
    total = _find_sheet_item(model, "Financial_model", _TOTAL_OPERATING_EXPENSES_ITEM_ID)
    insert_row = int(anchor.row) + 1
    for sheet_section in sheet.sections:
        for item in sheet_section.line_items:
            if int(item.row) >= insert_row:
                item.row = int(item.row) + 1
    section.line_items.insert(
        anchor_index + 1,
        LineItem(
            id=SELLING_GENERAL_ADMINISTRATIVE_ITEM_ID,
            label=" Selling, general and administrative",
            row=insert_row,
            item_type=ItemType.derived,
            unit=Unit.dollars,
            model_scale=anchor.model_scale,
            driver_category=anchor.driver_category,
            customization=CustomizationType.optional,
            style=anchor.style.model_copy(deep=True) if anchor.style is not None else None,
            projected=FormulaSpec(
                type=FormulaType.arithmetic,
                subtype="sum_range",
                params={
                    "function": "SUM",
                    "items": [
                        LineItemRef(id=_SALES_AND_MARKETING_ITEM_ID),
                        LineItemRef(id=_GENERAL_AND_ADMINISTRATIVE_ITEM_ID),
                    ],
                },
            ),
            formula_periods=list(anchor.formula_periods or []),
        ),
    )
    historical = total.historical
    if historical is None or historical.params.get("function") != "SUM":
        raise AssertionError(
            f"{_TOTAL_OPERATING_EXPENSES_ITEM_ID} historical formula is not the expected SUM over its rows"
        )
    historical.params["items"] = _insert_sum_ref_after(
        historical.params.get("items"),
        after_id=_GENERAL_AND_ADMINISTRATIVE_ITEM_ID,
        new_id=SELLING_GENERAL_ADMINISTRATIVE_ITEM_ID,
        context=_TOTAL_OPERATING_EXPENSES_ITEM_ID,
    )


NET_INCOME_ATTRIBUTABLE_TO_NCI_ITEM_ID = (
    "tpl.fm.income_statement.net_income_attributable_to_noncontrolling_interest"
)
NCI_SHARE_RATE_ITEM_ID = "tpl.a.tax_net_income.nci_share_of_consolidated_net_income"
_NET_INCOME_ITEM_ID = "tpl.fm.income_statement.net_income"
_PRETAX_INCOME_ITEM_ID = "tpl.fm.income_statement.income_before_income_taxes"
_INCOME_TAX_ITEM_ID = "tpl.fm.income_statement.income_tax_expense_benefit"
_NCI_SHARE_ANCHOR_ID = "tpl.a.tax_net_income.tax_rate"


def _consolidated_net_income_expr() -> dict:
    """Consolidated net income inline: pretax income less the tax provision."""

    return {
        "op": "-",
        "left": LineItemRef(id=_PRETAX_INCOME_ITEM_ID),
        "right": LineItemRef(id=_INCOME_TAX_ITEM_ID),
    }


def _insert_net_income_attributable_to_nci_rows(model: FinancialModel) -> None:
    """Give net income attributable to noncontrolling interest its face row and held share.

    The face row follows net income and is the attributable line. Its forecast is
    consolidated net income (pretax less provision) times the share held at
    ``NCI_SHARE_RATE_ITEM_ID``, an authorable input row: the compile writes the
    terminal reported year's share ``default`` there unless an authored cell
    holds it (NCI-FORECAST-HELD-SHARE). Neither row carries a historical
    default: an issuer that reports no such line has no history, and the
    company-grounded policy takes the unowed rows out of its model.
    """

    fm_sheet = model.sheets["Financial_model"]
    assumptions = model.sheets["Assumptions"]
    for sheet in (fm_sheet, assumptions):
        if any(
            item.id in (NET_INCOME_ATTRIBUTABLE_TO_NCI_ITEM_ID, NCI_SHARE_RATE_ITEM_ID)
            for section in sheet.sections
            for item in section.line_items
        ):
            raise AssertionError("net income attributable to NCI rows already exist")
    historical_periods = sorted(
        int(period)
        for period in (
            model.time_structure.historical_periods or model.time_structure.historical_years or []
        )
    )
    projection_periods = sorted(
        int(period)
        for period in (
            model.time_structure.projection_periods or model.time_structure.projection_years or []
        )
    )

    section = _find_section_with_item(fm_sheet, _NET_INCOME_ITEM_ID)
    anchor_index, anchor = next(
        (index, item)
        for index, item in enumerate(section.line_items)
        if item.id == _NET_INCOME_ITEM_ID
    )
    insert_row = int(anchor.row) + 1
    for sheet_section in fm_sheet.sections:
        for item in sheet_section.line_items:
            if int(item.row) >= insert_row:
                item.row = int(item.row) + 1
    section.line_items.insert(
        anchor_index + 1,
        LineItem(
            id=NET_INCOME_ATTRIBUTABLE_TO_NCI_ITEM_ID,
            label=" Net income attributable to noncontrolling interest",
            row=insert_row,
            item_type=ItemType.derived,
            unit=Unit.dollars,
            model_scale=anchor.model_scale,
            driver_category=anchor.driver_category,
            customization=CustomizationType.optional,
            style=anchor.style.model_copy(deep=True) if anchor.style is not None else None,
            projected=FormulaSpec(
                type=FormulaType.arithmetic,
                params={
                    "expr": {
                        "op": "*",
                        "args": [
                            LineItemRef(id=NCI_SHARE_RATE_ITEM_ID),
                            _consolidated_net_income_expr(),
                        ],
                    }
                },
                note="held_share_of_consolidated_net_income",
            ),
            formula_periods=sorted({*historical_periods, *projection_periods}),
        ),
    )

    rate_section = _find_section_with_item(assumptions, _NCI_SHARE_ANCHOR_ID)
    rate_index, rate_anchor = next(
        (index, item)
        for index, item in enumerate(rate_section.line_items)
        if item.id == _NCI_SHARE_ANCHOR_ID
    )
    rate_row = int(rate_anchor.row) + 1
    for sheet_section in assumptions.sections:
        for item in sheet_section.line_items:
            if int(item.row) >= rate_row:
                item.row = int(item.row) + 1
    rate = LineItem.model_validate(
        {
            "id": NCI_SHARE_RATE_ITEM_ID,
            "label": "  Noncontrolling interest share of consolidated net income",
            "row": rate_row,
            "item_type": "input",
            "unit": "percentage",
            "model_scale": "units",
            "style": {"color": "input_blue"},
            "driver_category": rate_anchor.driver_category,
            "customization": "optional",
        }
    )
    rate.historical = FormulaSpec(
        type=FormulaType.arithmetic,
        params={
            "expr": {
                "op": "/",
                "left": LineItemRef(id=NET_INCOME_ATTRIBUTABLE_TO_NCI_ITEM_ID),
                "right": _consolidated_net_income_expr(),
            }
        },
    )
    rate.formula_periods = list(historical_periods)
    rate_section.line_items.insert(rate_index + 1, rate)


RESTRUCTURING_CHARGES_ITEM_ID = "tpl.fm.income_statement.restructuring_charges"


def _insert_restructuring_charges_row(model: FinancialModel) -> None:
    """Give an issuer's restructuring charges their own operating-expense face row.

    The row follows operating D&A inside total operating expenses (both lanes'
    ``SUM``). It is an optional input with no template history or projection:
    a filing that reports the line binds it, the forecast-control profile's
    ``restructuring_charges`` control authors its projection, and an issuer
    that does not report it leaves the total through the catalog's
    ``optional_if_unreported`` participation rule, never as a zero.
    """

    sheet = model.sheets["Financial_model"]
    if any(
        item.id == RESTRUCTURING_CHARGES_ITEM_ID
        for section in sheet.sections
        for item in section.line_items
    ):
        raise AssertionError(f"{RESTRUCTURING_CHARGES_ITEM_ID} already exists")
    section = _find_section_with_item(sheet, OPERATING_DEPRECIATION_AMORTIZATION_ITEM_ID)
    anchor_index, anchor = next(
        (index, item)
        for index, item in enumerate(section.line_items)
        if item.id == OPERATING_DEPRECIATION_AMORTIZATION_ITEM_ID
    )
    total = _find_sheet_item(model, "Financial_model", _TOTAL_OPERATING_EXPENSES_ITEM_ID)
    insert_row = int(anchor.row) + 1
    for sheet_section in sheet.sections:
        for item in sheet_section.line_items:
            if int(item.row) >= insert_row:
                item.row = int(item.row) + 1
    section.line_items.insert(
        anchor_index + 1,
        LineItem(
            id=RESTRUCTURING_CHARGES_ITEM_ID,
            label=" Restructuring charges",
            row=insert_row,
            item_type=ItemType.input,
            unit=Unit.dollars,
            model_scale=anchor.model_scale,
            driver_category=anchor.driver_category,
            customization=CustomizationType.optional,
            style=CellStyle(color=CellColor.input_blue),
        ),
    )
    for spec in (total.historical, total.projected):
        if spec is None or spec.params.get("function") != "SUM":
            raise AssertionError(
                f"{_TOTAL_OPERATING_EXPENSES_ITEM_ID} formula is not the expected SUM over its rows"
            )
        spec.params["items"] = _insert_sum_ref_after(
            spec.params.get("items"),
            after_id=OPERATING_DEPRECIATION_AMORTIZATION_ITEM_ID,
            new_id=RESTRUCTURING_CHARGES_ITEM_ID,
            context=_TOTAL_OPERATING_EXPENSES_ITEM_ID,
        )


#: The presentation-invariant SG&A base (DRIVER-BASE-PRESENTATION-INVARIANT). A driver that
#: reads "SG&A" reads this row, never the G&A (or S&M) row as a stand-in: its historical lane
#: sums every presentation of the line, so an included child leaves the sum (cited
#: not-separately-presented lowering) and a cited-absent SG&A line leaves it (rule (d)); its
#: forecast lane is the SG&A readout, S&M plus G&A. The one owner of the expression.
SGA_DRIVER_BASE_ITEM_ID = "tpl.a.operating_leverage.selling_general_and_administrative_base"
SGA_DRIVER_BASE_HISTORICAL_ITEM_IDS = (
    "tpl.fm.income_statement.sales_and_marketing",
    "tpl.fm.income_statement.general_and_administrative",
    SELLING_GENERAL_ADMINISTRATIVE_ITEM_ID,
)
SGA_DRIVER_BASE_FORECAST_ITEM_IDS = (
    "tpl.fm.income_statement.sales_and_marketing",
    "tpl.fm.income_statement.general_and_administrative",
)
#: The assumptions operating-expense total's historical lane (before D&A, which that total has
#: never carried): the functional lines and the combined SG&A line.
SGA_OPEX_BASE_HISTORICAL_ITEM_IDS = (
    "tpl.fm.income_statement.sales_and_marketing",
    "tpl.fm.income_statement.research_and_development",
    "tpl.fm.income_statement.general_and_administrative",
    SELLING_GENERAL_ADMINISTRATIVE_ITEM_ID,
)
SGA_OPEX_BASE_FORECAST_ITEM_IDS = (
    "tpl.fm.income_statement.sales_and_marketing",
    "tpl.fm.income_statement.research_and_development",
    "tpl.fm.income_statement.general_and_administrative",
)
#: The drivers that read the G&A assumption row as their "SG&A" base, by the row they drive.
SGA_DRIVER_BASE_READER_ITEM_IDS = (
    "tpl.a.balance_sheet_wc.of_cogs_and_sg_a",
    "tpl.a.balance_sheet_wc.current_asset_2",
    "tpl.a.balance_sheet_wc.accounts_payable",
    "tpl.a.balance_sheet_wc.accounts_payable_pct_cogs_and_sga",
    "tpl.a.balance_sheet_wc.days_payable_outstanding_dpo",
    "tpl.a.balance_sheet_wc.accrued_expenses",
    "tpl.a.balance_sheet_wc.accrued_expenses_pct_cogs_and_sga",
    "tpl.a.other_assets.of_general_and_administrative",
    "tpl.a.other_assets.long_term_asset_2",
    "tpl.a.other_assets.long_term_asset_2_pct_operating_line",
    "tpl.a.other_assets.long_term_asset_3",
    "tpl.a.other_assets.long_term_asset_3_pct_operating_line",
    "tpl.a.capital_sources.of_general_and_administrative",
    "tpl.a.capital_sources.other_long_term_liabilities",
    "tpl.a.capital_sources.other_long_term_liabilities_pct_general_and_administrative",
)
_GENERAL_AND_ADMINISTRATIVE_ASSUMPTION_ID = "tpl.a.operating_leverage.general_and_administrative"
_OPERATING_EXPENSES_ASSUMPTION_ID = "tpl.a.operating_leverage.total_operating_expenses"


def _install_sga_driver_base(model: FinancialModel) -> None:
    """Insert the SG&A driver base and point every stand-in reader at it."""

    assumptions = model.sheets["Assumptions"]
    if any(
        item.id == SGA_DRIVER_BASE_ITEM_ID
        for section in assumptions.sections
        for item in section.line_items
    ):
        raise AssertionError(f"{SGA_DRIVER_BASE_ITEM_ID} already exists")
    section = _find_section_with_item(assumptions, _OPERATING_EXPENSES_ASSUMPTION_ID)
    anchor_index, anchor = next(
        (index, item)
        for index, item in enumerate(section.line_items)
        if item.id == _OPERATING_EXPENSES_ASSUMPTION_ID
    )
    general = _find_sheet_item(model, "Assumptions", _GENERAL_AND_ADMINISTRATIVE_ASSUMPTION_ID)
    historical_periods = sorted(
        int(period)
        for period in (
            model.time_structure.historical_periods or model.time_structure.historical_years or []
        )
    )
    projection_periods = sorted(
        int(period)
        for period in (
            model.time_structure.projection_periods or model.time_structure.projection_years or []
        )
    )
    insert_row = int(anchor.row) + 1
    for sheet_section in assumptions.sections:
        for item in sheet_section.line_items:
            if int(item.row) >= insert_row:
                item.row = int(item.row) + 1

    def total(item_ids: Sequence[str]) -> FormulaSpec:
        return FormulaSpec(
            type=FormulaType.arithmetic,
            subtype="sum_range",
            params={"function": "SUM", "items": [LineItemRef(id=item_id) for item_id in item_ids]},
        )

    section.line_items.insert(
        anchor_index + 1,
        LineItem(
            id=SGA_DRIVER_BASE_ITEM_ID,
            label=" SG&A (driver base)",
            row=insert_row,
            item_type=ItemType.derived,
            unit=Unit.dollars,
            model_scale=general.model_scale,
            driver_category=general.driver_category,
            customization=CustomizationType.optional,
            style=general.style.model_copy(deep=True) if general.style is not None else None,
            historical=total(SGA_DRIVER_BASE_HISTORICAL_ITEM_IDS),
            projected=total(SGA_DRIVER_BASE_FORECAST_ITEM_IDS),
            formula_periods=sorted({*historical_periods, *projection_periods}),
        ),
    )

    def rewired(value: Any, counter: list[int]) -> Any:
        if isinstance(value, LineItemRef):
            if value.id == _GENERAL_AND_ADMINISTRATIVE_ASSUMPTION_ID:
                counter[0] += 1
                return value.model_copy(update={"id": SGA_DRIVER_BASE_ITEM_ID})
            return value
        if isinstance(value, dict):
            return {key: rewired(nested, counter) for key, nested in value.items()}
        if isinstance(value, list):
            return [rewired(nested, counter) for nested in value]
        return value

    for reader_id in SGA_DRIVER_BASE_READER_ITEM_IDS:
        reader = _find_sheet_item(model, "Assumptions", reader_id)
        counter = [0]
        for spec in _iter_formula_specs(reader):
            spec.params = rewired(spec.params, counter)
        if not counter[0]:
            raise AssertionError(f"{reader_id} does not read {_GENERAL_AND_ADMINISTRATIVE_ASSUMPTION_ID}")
    # The operating-expense total the opex-keyed drivers read sums the same presentations, over
    # the same rows in both lanes so a cited inclusion compares equal across the forecast seam.
    opex = _find_sheet_item(model, "Assumptions", _OPERATING_EXPENSES_ASSUMPTION_ID)
    opex.historical = total(SGA_OPEX_BASE_HISTORICAL_ITEM_IDS)
    opex.projected = total(SGA_OPEX_BASE_FORECAST_ITEM_IDS)


_LONG_TERM_ASSET_3_ROLE_ITEM_ID = "tpl.fm.balance_sheet.long_term_asset_3"
_LONG_TERM_ASSET_3_CASH_FLOW_ITEM_ID = "tpl.fm.cash_flow.long_term_asset_3"


class _SlotClone(NamedTuple):
    """One row of the second long-term asset slot and where its third-slot clone goes."""

    sheet: str
    source_id: str
    new_id: str
    follows_id: str
    #: Config key of the slot's placeholder text, which the clone's label carries.
    placeholder_key: Optional[str]
    #: The clone also carries the placeholder as its template token and build note.
    carries_token: bool


#: The third fixed noncurrent-asset slot is the second one's row set cloned.
LONG_TERM_ASSET_3_ROWS = (
    _SlotClone(
        "Assumptions",
        "tpl.a.other_assets.long_term_asset_2",
        "tpl.a.other_assets.long_term_asset_3",
        "tpl.a.other_assets.of_general_and_administrative",
        "assumptions.long_term_asset_3",
        True,
    ),
    _SlotClone(
        "Assumptions",
        "tpl.a.other_assets.long_term_asset_2_pct_operating_line",
        "tpl.a.other_assets.long_term_asset_3_pct_operating_line",
        "tpl.a.other_assets.long_term_asset_3",
        None,
        False,
    ),
    _SlotClone(
        "Financial_model",
        "tpl.fm.balance_sheet.long_term_asset_2",
        _LONG_TERM_ASSET_3_ROLE_ITEM_ID,
        "tpl.fm.balance_sheet.deferred_income_tax_assets",
        "financial_model.long_term_asset_3",
        True,
    ),
    _SlotClone(
        "Financial_model",
        "tpl.fm.cash_flow.long_term_asset_2",
        _LONG_TERM_ASSET_3_CASH_FLOW_ITEM_ID,
        "tpl.fm.cash_flow.deferred_income_tax_assets",
        "financial_model.long_term_asset_3",
        False,
    ),
)


def _insert_long_term_asset_3_rows(
    model: FinancialModel,
    *,
    template_tokens: Dict[str, str],
    build_notes: Dict[str, str],
) -> None:
    """Clone the second long-term asset slot into a third, beside the same neighbours.

    Each clone keeps its source row's formulas, periods and styling with ids and
    references moved to the third slot, carries the slot's placeholder label, and
    is inserted positionally (later rows shift when the row below its anchor is
    taken) so row order stays section order.
    The totals and cash-flow aggregates that include the slot are generated
    from the catalog (`_generate_statement_totals`,
    `_generate_cash_flow_movements`), never wired here.
    """

    renamed_ids = {clone.source_id: clone.new_id for clone in LONG_TERM_ASSET_3_ROWS}
    for slot in LONG_TERM_ASSET_3_ROWS:
        sheet = model.sheets[slot.sheet]
        new_id = slot.new_id
        anchor_id = slot.follows_id
        if any(item.id == new_id for section in sheet.sections for item in section.line_items):
            raise AssertionError(f"long-term asset 3 row {new_id} already exists")
        section = _find_section_with_item(sheet, anchor_id)
        anchor_index, anchor = next(
            (index, item)
            for index, item in enumerate(section.line_items)
            if item.id == anchor_id
        )
        insert_row = int(anchor.row) + 1
        if any(
            int(item.row) == insert_row
            for sheet_section in sheet.sections
            for item in sheet_section.line_items
        ):
            for sheet_section in sheet.sections:
                for item in sheet_section.line_items:
                    if int(item.row) >= insert_row:
                        item.row = int(item.row) + 1
        clone = _find_sheet_item(model, slot.sheet, slot.source_id).model_copy(deep=True)
        clone.id = new_id
        clone.row = insert_row
        if clone.historical is not None:
            clone.historical = _rename_spec_refs(clone.historical, renamed_ids)
        if clone.projected is not None:
            clone.projected = _rename_spec_refs(clone.projected, renamed_ids)
        if clone.overrides:
            clone.overrides = {
                period: _rename_spec_refs(spec, renamed_ids)
                for period, spec in clone.overrides.items()
            }
        if slot.placeholder_key is not None:
            placeholder = template_tokens[slot.placeholder_key]
            clone.label = " " + placeholder
            if slot.carries_token:
                clone.template_token = placeholder
                clone.build_notes = build_notes[slot.placeholder_key]
        section.line_items.insert(anchor_index + 1, clone)


_ADDITIONAL_PAID_IN_CAPITAL_ITEM_ID = "tpl.a.dividends_shares.additional_paid_in_capital"
_SHARE_BASED_COMPENSATION_ITEM_ID = "tpl.fm.cash_flow.share_based_compensation"


def _add_share_based_compensation_to_apic_roll_forward(model: FinancialModel) -> None:
    """Credit equity for the share-based compensation the cash flow adds back.

    Share-based compensation stays inside operating expense (so net income is
    already net of it) and the cash flow adds it back as non-cash; the matching
    credit is an addition to additional paid-in capital, which keeps the
    balance sheet in balance.
    """

    apic = _find_sheet_item(model, "Assumptions", _ADDITIONAL_PAID_IN_CAPITAL_ITEM_ID)
    spec = apic.projected
    if spec is None or spec.type != FormulaType.roll_forward:
        raise AssertionError(f"{_ADDITIONAL_PAID_IN_CAPITAL_ITEM_ID} is not a roll-forward")
    additions = spec.params.get("additions")
    if not isinstance(additions, list):
        raise AssertionError(f"{_ADDITIONAL_PAID_IN_CAPITAL_ITEM_ID} roll-forward has no additions")
    if any(
        isinstance(ref, LineItemRef) and ref.id == _SHARE_BASED_COMPENSATION_ITEM_ID
        for ref in additions
    ):
        raise AssertionError(
            f"{_ADDITIONAL_PAID_IN_CAPITAL_ITEM_ID} already adds share-based compensation"
        )
    additions.append(LineItemRef(id=_SHARE_BASED_COMPENSATION_ITEM_ID))


#: The note a generated optional-slot projection carries, so a consumer can
#: tell the template's held balance from a driver a build installed.
OPTIONAL_SLOT_CARRY_FORWARD_NOTE = "template_optional_slot_carry_forward"

#: Marks an unmodeled flow's template default rather than an authored forecast.
TEMPLATE_PROJECTED_ZERO_DEFAULT_NOTE = "unmodeled flow defaults to zero"

_CHANGES_IN_OPERATING_ASSETS_AND_LIABILITIES_ITEM_ID = (
    "tpl.fm.cash_flow.changes_in_operating_assets_and_liabilities"
)
_OTHER_NON_CASH_ADJUSTMENTS_ITEM_ID = "tpl.fm.cash_flow.other_non_cash_adjustments"


def _statement_periods(model: FinancialModel) -> tuple[list[int], list[int]]:
    """The template's historical and projection periods."""

    time_structure = model.time_structure
    historical = time_structure.historical_periods or time_structure.historical_years or []
    projection = time_structure.projection_periods or time_structure.projection_years or []
    return [int(period) for period in historical], [int(period) for period in projection]


def _statement_items(model: FinancialModel) -> Dict[str, LineItem]:
    return {item.id: item for _, item in _iter_items(model)}


def _generate_statement_totals(model: FinancialModel) -> None:
    """Write both lanes of every balance-sheet total from its shared membership.

    Required-role ``parent_total`` rows supply named face lines; the generic
    template configuration supplies optional slots with no role. Subtotals are
    included, in sheet-row order. Historical and projected carry the same
    formula over the same members, so a line a total sums in one lane it sums
    in the other. Diagnostic sections read the same membership.
    """

    catalog = load_required_role_catalog()
    historical, projection = _statement_periods(model)
    items = _statement_items(model)
    for total_id in catalog.statement_totals():
        members = balance_sheet_total_members(total_id)
        absent = [item_id for item_id in (total_id, *members) if item_id not in items]
        if absent:
            raise AssertionError(
                f"{total_id} sums lines the template does not carry: {absent}"
            )
        ordered = sorted(members, key=lambda item_id: int(items[item_id].row))
        formula = FormulaSpec(
            type=FormulaType.arithmetic,
            subtype="sum_range",
            params={"function": "SUM", "items": [LineItemRef(id=item_id) for item_id in ordered]},
        )
        total = items[total_id]
        total.historical = formula
        total.projected = formula.model_copy(deep=True)
        total.formula_periods = sorted({*historical, *projection})


def _install_current_debt_bridge(model: FinancialModel) -> None:
    """Read reported current debt historically and hold its forecast classification."""

    item = _find_sheet_item(
        model, "Assumptions", "tpl.a.capital_sources.current_portion_of_long_term_debt"
    )
    item.historical = FormulaSpec(
        type=FormulaType.ref,
        subtype="cross_sheet_ref",
        params={
            "source": LineItemRef(
                id="tpl.fm.balance_sheet.current_portion_of_long_term_debt"
            )
        },
        note="reported current debt statement-to-schedule bridge",
    )
    item.projected = FormulaSpec(
        type=FormulaType.ref,
        subtype="carry_forward",
        params={"source": LineItemRef(id=item.id, t=-1)},
        note="company-grounded current-debt classification carry-forward",
    )
    historical, projection = _statement_periods(model)
    item.formula_periods = sorted({*historical, *projection})


def _install_share_count_schedule(model: FinancialModel) -> None:
    """The share-count rows: the net change identity, the repurchase count, the price chain.

    Net issuance is the change in the end-share proxy in both lanes, so end-of-period shares
    declare no roll-forward of it (the change would read the count that reads the change); the
    proxy's projection is the grounded share policy's, over the basic-share row that compile
    inserts. ``number_of_shares_repurchased`` is ``total_amount`` converted at
    ``expected_average_share_price``, a cell only where a price owns one. The consensus growth
    rate is a valuation-policy input: no period is the row's own until one is authored.
    """

    _, projection = _statement_periods(model)
    end_id = "tpl.a.dividends_shares.end_of_period_shares_outstanding"
    issued = model.get_item("tpl.a.dividends_shares.shares_issued")
    issued.item_type = ItemType.derived
    issued.label = "Net change in shares"
    issued.projected = FormulaSpec(
        type=FormulaType.arithmetic,
        subtype="add_subtract",
        params={
            "operands": [
                "-",
                LineItemRef(id=end_id),
                LineItemRef(id=end_id, t=-1),
            ]
        },
        note="net change in the end-share proxy on the accepted net-dilution curve",
    )
    issued.formula_periods = sorted({*(issued.formula_periods or []), *projection})
    model.get_item(end_id).projected = None
    repurchased = model.get_item("tpl.a.dividends_shares.number_of_shares_repurchased")
    repurchased.item_type = ItemType.derived
    repurchased.unit = Unit.share_count
    repurchased.model_scale = "millions"
    repurchased.label = "Number of shares repurchased"
    repurchased.style = issued.style.model_copy(deep=True) if issued.style is not None else None
    repurchased.projected = FormulaSpec(
        type=FormulaType.ratio,
        subtype="divide",
        params={
            "numerator": LineItemRef(id="tpl.a.dividends_shares.total_amount"),
            "denominator": LineItemRef(id="tpl.a.dividends_shares.expected_average_share_price"),
        },
        note="one_repurchase_dollars_to_share_count_conversion",
    )
    repurchased.formula_periods = []
    model.get_item("tpl.a.dividends_shares.expected_average_share_price").formula_periods = []
    model.get_item("tpl.a.dividends_shares.consensus_eps_growth").formula_periods = []


def _install_historical_schedule_identities(model: FinancialModel) -> None:
    """Close structural schedule algebra; filing-dependent inclusions stay in population."""

    historical, projection = _statement_periods(model)
    items = _statement_items(model)
    physical_id = "tpl.a.depreciation_amortization.depreciation"
    software_amortization_id = (
        "tpl.a.depreciation_amortization.amortization_of_capitalized_internal_use_software"
    )
    acquired_amortization_id = (
        "tpl.a.depreciation_amortization.amortization_expense_for_acquired_intangible_assets"
    )
    physical = items[physical_id]
    physical.historical = FormulaSpec(
        type=FormulaType.arithmetic,
        subtype="historical_physical_depreciation_residual",
        params={
            "expr": {
                "op": "+",
                "args": [
                    LineItemRef(id="tpl.a.depreciation_amortization.depreciation_and_amortization_m"),
                    {"op": "NEG", "arg": LineItemRef(id=software_amortization_id)},
                    {"op": "NEG", "arg": LineItemRef(id=acquired_amortization_id)},
                ],
            }
        },
        note=(
            "exact reported total D&A less separately reported capitalized-software "
            "and acquired-intangible amortization"
        ),
    )
    rate = items["tpl.a.depreciation_amortization.depreciation_pct_revenue"]
    rate.historical = FormulaSpec(
        type=FormulaType.ratio,
        subtype="divide",
        params={
            "numerator": LineItemRef(id=physical_id),
            "denominator": LineItemRef(id="tpl.fm.income_statement.total_revenue"),
        },
        note="exact reported physical depreciation over revenue",
    )
    for item in (physical, rate):
        item.formula_periods = sorted({*(item.formula_periods or []), *historical})

    ending = items["tpl.a.depreciation_amortization.end_capitalized_internal_use_software"]
    ending.historical = FormulaSpec(
        type=FormulaType.ref,
        subtype="reported_capitalized_software_balance",
        params={"source": LineItemRef(id="tpl.a.capital_investments.software_investment_if_applicable")},
        note="exact reported capitalized-software balance",
    )
    ending.formula_periods = sorted({*(ending.formula_periods or []), *historical})

    beginning = items["tpl.a.depreciation_amortization.beg_capitalized_internal_use_software"]
    beginning.historical = FormulaSpec(
        type=FormulaType.ref,
        subtype="reported_capitalized_software_beginning",
        params={"source": LineItemRef(id="tpl.a.capital_investments.software_investment_if_applicable", t=-1)},
        note="prior exact reported capitalized-software balance",
    )
    beginning.projected = beginning.historical.model_copy(
        update={"note": "prior exact projected capitalized-software balance"}, deep=True
    )
    beginning.formula_periods = sorted({*historical[1:], *projection})

    # The canonical lease balance is populated from sources, not its face readout.
    items["tpl.a.other_assets.operating_lease_right_of_use_assets"].historical = None

    retained_balance = items["tpl.fm.balance_sheet.retained_earnings"]
    retained_balance.projected = FormulaSpec(
        type=FormulaType.arithmetic,
        subtype="retained_earnings_roll_forward",
        params={
            "expr": {
                "op": "+",
                "args": [
                    LineItemRef(id=retained_balance.id, t=-1),
                    LineItemRef(id="tpl.fm.income_statement.net_income"),
                    {
                        "op": "NEG",
                        "arg": LineItemRef(id="tpl.a.dividends_shares.dividend_paid"),
                    },
                ],
            }
        },
        note="canonical retained-earnings roll-forward from net income and dividends",
    )
    retained_balance.formula_periods = sorted(
        {*(retained_balance.formula_periods or []), *projection}
    )

    retained = items["tpl.a.dividends_shares.retained_earnings"]
    retained.projected = FormulaSpec(
        type=FormulaType.ref,
        subtype="canonical_retained_earnings_bridge",
        params={"source": LineItemRef(id="tpl.fm.balance_sheet.retained_earnings")},
        note="single canonical retained-earnings roll-forward authority",
    )
    if retained.overrides is not None:
        for period in projection:
            retained.overrides.pop(period, None)
    retained.formula_periods = sorted({*historical, *projection})

    deferred = items["tpl.fm.cash_flow.deferred_income_tax_liabilities"]
    if deferred.overrides is None:
        deferred.overrides = {}
    for period in historical:
        deferred.overrides[period] = FormulaSpec(
            type=FormulaType.constant,
            params={"value": 0.0},
            note="deferred-tax cash-flow activity remains in the exact reported residual lanes",
        )
    deferred.formula_periods = sorted({*(deferred.formula_periods or []), *historical})


_DEBT_INTEREST_EXPENSE_ID = "tpl.a.other_income_interest.interest_expense"
_DEBT_INTEREST_RATE_ID = "tpl.a.other_income_interest.interest_rate"
_DEBT_AVERAGE_ID = "tpl.a.other_income_interest.average"
_REPORTED_INTEREST_EXPENSE_ID = "tpl.fm.adjusted_earnings.interest_expense"


def _install_debt_interest_identities(model: FinancialModel) -> None:
    """Declare the debt schedule's interest rows, the one owner of their formulas.

    Reported interest expense reads the filing's line, the effective rate is that
    interest over the period's average debt and carries forward, and projected interest
    is average debt x rate (zero only when average debt is a known zero). A filer whose
    debt roles are cited absent has no rate, and no filer has a rate in a period whose average
    debt is a known zero: ``retire_unused_cost_of_debt`` retires the cells no debt defines
    rather than writing a zero.
    """

    items = _statement_items(model)
    expense = items[_DEBT_INTEREST_EXPENSE_ID]
    expense.historical = FormulaSpec(
        type=FormulaType.ref,
        subtype="cross_sheet_ref",
        params={"source": LineItemRef(id=_REPORTED_INTEREST_EXPENSE_ID)},
        note="reported_interest_expense_to_debt_schedule",
    )
    # No debt has no interest expense, not an assumed rate: the zero branch is read
    # only when average debt is a known numeric zero, so the undefined rate stays unread.
    expense.projected = FormulaSpec(
        type=FormulaType.arithmetic,
        subtype="expression",
        params={
            "expr": {
                "op": "IF",
                "args": [
                    LineItemRef(id=_DEBT_AVERAGE_ID),
                    {
                        "op": "*",
                        "args": [
                            LineItemRef(id=_DEBT_AVERAGE_ID),
                            LineItemRef(id=_DEBT_INTEREST_RATE_ID),
                        ],
                    },
                    0.0,
                ],
            },
        },
        note="interest_expense_from_average_debt_and_company_rate",
    )
    items[_DEBT_INTEREST_RATE_ID].historical = FormulaSpec(
        type=FormulaType.ratio,
        subtype="divide",
        params={
            "numerator": LineItemRef(id=_DEBT_INTEREST_EXPENSE_ID),
            "denominator": LineItemRef(id=_DEBT_AVERAGE_ID),
        },
        note="company_effective_debt_interest_rate_from_reported_interest_and_average_debt",
    )


def _install_financing_residual_defaults(model: FinancialModel) -> None:
    """Canonical financing lanes exhaust cash; do not add a second residual flow."""

    _, projection = _statement_periods(model)
    items = _statement_items(model)
    for item_id, note in (
        (
            "tpl.fm.cash_flow.other_financing_activities",
            "canonical debt, equity, repurchase, and client-fund lanes exhaust modeled financing activity",
        ),
        (
            "tpl.fm.cash_flow.other_cash_flows_from_financing",
            "canonical debt, equity, repurchase, and client-fund lanes exhaust modeled financing activity",
        ),
        (
            "tpl.fm.cash_flow.share_based_compensation_offset",
            "projected SBC financing offset is zero under post-SBC net income convention",
        ),
    ):
        item = items[item_id]
        item.projected = FormulaSpec(
            type=FormulaType.constant,
            params={"value": 0.0},
            note=note,
        )
        item.formula_periods = sorted({*(item.formula_periods or []), *projection})


def _install_non_gaap_other_items_default(model: FinancialModel) -> None:
    """Default future issuer-defined events; authored period inputs still take precedence."""

    _, projection = _statement_periods(model)
    item = model.get_item("tpl.a.adj_ebitda.other_items")
    item.projected = FormulaSpec(
        type=FormulaType.constant,
        params={"value": 0.0},
        note="projected_non_gaap_other_items_exclusion",
    )
    item.formula_periods = sorted({*(item.formula_periods or []), *projection})


def _install_non_gaap_tax_effect_identity(model: FinancialModel) -> None:
    """Apply the adjustment tax rate to add-backs in history as well as projection."""

    historical, projection = _statement_periods(model)
    effect = model.get_item("tpl.a.tax_net_income.income_tax_effect_on_adjustments")
    rate = model.get_item("tpl.a.tax_net_income.effective_tax_rate")
    effect.historical = FormulaSpec(
        type=FormulaType.driver,
        subtype="base_x_rate",
        params={
            "base": {
                "op": "SUM",
                "args": [
                    LineItemRef(id="tpl.a.tax_net_income.stock_based_compensation_add_back"),
                    LineItemRef(id="tpl.a.tax_net_income.amortization_of_acquired_intangibles"),
                    LineItemRef(id="tpl.a.tax_net_income.other_items"),
                ],
            },
            "rate": {"op": "NEG", "arg": LineItemRef(id=rate.id)},
        },
        note="add-backs at the year's grounded adjustment tax rate",
    )
    face = model.get_item("tpl.fm.adjusted_earnings.income_tax_effect_on_adjustments")
    face.historical = FormulaSpec(
        type=FormulaType.ref,
        subtype="cross_sheet_ref",
        params={"source": LineItemRef(id=effect.id)},
        note="estimated tax effect on adjustments; a disclosed year's override carries the reported amount",
    )
    # A disclosed face amount grounds the rate; the estimated schedule amount
    # already reads this rate and cannot also be its historical numerator.
    rate.historical = FormulaSpec(
        type=FormulaType.ratio,
        subtype="divide",
        params={
            "numerator": {"op": "NEG", "arg": LineItemRef(id=face.id)},
            "denominator": effect.historical.params["base"],
        },
    )
    for item in (effect, face):
        item.formula_periods = sorted({*historical, *projection})


def _install_optional_slot_carry_forward(model: FinancialModel) -> None:
    """Give every total member the projection a total can sum.

    A member with no projection rule (a slot no schedule drives: other current
    assets, optional current assets, noncurrent deferred revenue) holds its prior
    balance, so no total sums a blank projected operand. A build that has a
    driver for the slot replaces it; the note marks which is which. Every
    member's projection also covers every projected period, so a total never
    outruns a line it sums.
    """

    catalog = load_required_role_catalog()
    _, projection = _statement_periods(model)
    items = _statement_items(model)
    totals = set(catalog.statement_totals())
    for total_id in catalog.statement_totals():
        for member_id in balance_sheet_total_members(total_id):
            if member_id in totals:
                continue
            member = items[member_id]
            if member.projected is None:
                member.projected = FormulaSpec(
                    type=FormulaType.ref,
                    subtype="carry_forward",
                    params={"source": LineItemRef(id=member_id, t=-1)},
                    note=OPTIONAL_SLOT_CARRY_FORWARD_NOTE,
                )
                member.formula_periods = sorted({*(member.formula_periods or []), *projection})
            elif member.formula_periods is not None:
                member.formula_periods = sorted({*member.formula_periods, *projection})


def _install_projected_zero_defaults(
    template: FinancialModel, ids: Iterable[str]
) -> None:
    """Default unmodeled flows without replacing an existing projection."""

    _, projection = _statement_periods(template)
    items = _statement_items(template)
    for item_id in ids:
        item = items[item_id]
        if item.projected is None:
            item.projected = FormulaSpec(
                type=FormulaType.constant,
                params={"value": 0.0},
                note=TEMPLATE_PROJECTED_ZERO_DEFAULT_NOTE,
            )
            item.formula_periods = sorted(
                {*(item.formula_periods or []), *projection}
            )


def _balance_movement(line_id: str, side: str) -> tuple[LineItemRef, LineItemRef]:
    """``(minuend, subtrahend)`` of a balance's period change read as cash.

    An asset's increase uses cash and a liability's releases it.
    """

    opening = LineItemRef(id=line_id, t=-1)
    closing = LineItemRef(id=line_id, t=0)
    return (opening, closing) if side == "asset" else (closing, opening)


def _generate_cash_flow_movements(model: FinancialModel) -> None:
    """Carry every projected balance movement in exactly one cash-flow aggregate.

    The catalog says where a balance's movement lands: an operating
    working-capital balance (``working_capital``) in the changes in operating
    assets and liabilities, a noncurrent balance with no schedule of its own
    (``other_noncash``) in the other non-cash adjustments. Each aggregate's
    projection is the signed sum of those movements in every projected period,
    so the balance sheet's change and the cash flow's can only differ by a row
    the catalog does not name. The line-by-line cash rows beside the aggregates
    present the same movements in every projected period.
    """

    catalog = load_required_role_catalog()
    _, projection = _statement_periods(model)
    items = _statement_items(model)
    for aggregate_id, side_of in (
        (_CHANGES_IN_OPERATING_ASSETS_AND_LIABILITIES_ITEM_ID, lambda row: row.working_capital),
        (_OTHER_NON_CASH_ADJUSTMENTS_ITEM_ID, lambda row: row.other_noncash),
    ):
        movements = [
            (line, side_of(row))
            for row in catalog.required_roles
            if side_of(row) is not None
            for line in face_lines(row)
        ]
        absent = [line for line, _side in movements if line not in items]
        if absent:
            raise AssertionError(
                f"{aggregate_id} moves catalog lines the template does not carry: {absent}"
            )
        movements.sort(key=lambda movement: int(items[movement[0]].row))
        aggregate = items[aggregate_id]
        aggregate.projected = FormulaSpec(
            type=FormulaType.arithmetic,
            subtype="add_subtract",
            params={
                "expr": {
                    "op": "+",
                    "args": [
                        {"op": "-", "left": left, "right": right}
                        for left, right in (
                            _balance_movement(line, side) for line, side in movements
                        )
                    ],
                }
            },
        )
        aggregate.formula_periods = sorted({*(aggregate.formula_periods or []), *projection})
        # The aggregate is the same expression in every projected period; the
        # captured first-forecast override summed an older row set instead.
        if aggregate.overrides:
            aggregate.overrides = {
                period: spec
                for period, spec in aggregate.overrides.items()
                if int(period) not in projection
            } or None
        for line, side in movements:
            leg = items.get(line.replace(".balance_sheet.", ".cash_flow."))
            if leg is None:
                continue
            leg.projected = FormulaSpec(
                type=FormulaType.arithmetic,
                subtype="add_subtract",
                params={"operands": ["-", *_balance_movement(line, side)]},
            )
            leg.formula_periods = sorted({*(leg.formula_periods or []), *projection})


__all__ = [
    "LONG_TERM_ASSET_3_ROWS",
    "_insert_long_term_asset_3_rows",
    "_add_share_based_compensation_to_apic_roll_forward",
    "OPTIONAL_SLOT_CARRY_FORWARD_NOTE",
    "_generate_statement_totals",
    "_install_optional_slot_carry_forward",
    "_generate_cash_flow_movements",
    "BALANCE_SHEET_FACE_ROWS",
    "SHORT_TERM_INVESTMENTS_ITEM_ID",
    "_insert_short_term_investments_row",
    "_insert_per_share_face_rows",
    "OPERATING_DEPRECIATION_AMORTIZATION_ITEM_ID",
    "_insert_operating_depreciation_amortization_row",
    "SELLING_GENERAL_ADMINISTRATIVE_ITEM_ID",
    "_insert_selling_general_administrative_row",
    "NET_INCOME_ATTRIBUTABLE_TO_NCI_ITEM_ID",
    "NCI_SHARE_RATE_ITEM_ID",
    "_insert_net_income_attributable_to_nci_rows",
    "RESTRUCTURING_CHARGES_ITEM_ID",
    "_insert_restructuring_charges_row",
    "SGA_DRIVER_BASE_ITEM_ID",
    "SGA_DRIVER_BASE_HISTORICAL_ITEM_IDS",
    "SGA_DRIVER_BASE_FORECAST_ITEM_IDS",
    "SGA_DRIVER_BASE_READER_ITEM_IDS",
    "_install_sga_driver_base",
    "HOUSE_REVENUE_RATIO_ROWS",
    "_iter_items",
    "_iter_formula_specs",
    "_extract_refs",
    "_apply_scenario_linked_offset_params",
    "_find_sheet_item",
    "_find_section_with_item",
    "_apply_label_overrides",
    "_normalize_generic_accounting_baseline",
    "_insert_sum_ref_after",
    "_insert_other_non_current_assets_row",
    "_insert_other_current_liabilities_row",
    "_insert_commercial_paper_row",
    "_insert_deferred_revenue_noncurrent_row",
    "_insert_investment_activity_rows",
    "ACQUISITION_GOODWILL_ALLOCATION_ITEM_ID",
    "_insert_acquisition_goodwill_allocation_row",
    "_insert_house_revenue_ratio_rows",
    "_insert_treasury_stock_face_row",
    "_insert_balance_sheet_face_rows",
    "TREASURY_STOCK_FACE_ITEM_ID",
    "TREASURY_STOCK_SCHEDULE_ITEM_ID",
    "_clear_cash_flow_artifact_historicals",
    "_set_if_applicable_default_zero_historicals",
    "_normalize_cash_flow_projection_links",
    "_normalize_valuation_projection_links",
    "_normalize_sbc_per_line_rate_historicals",
    "_normalize_generic_cash_reconciliation",
]
