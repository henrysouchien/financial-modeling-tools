"""Template mutation helpers used while building reference artifacts."""

from __future__ import annotations

from typing import Dict, Iterable, List

from ..models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
    Section,
    Unit,
)
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

    total_assets = _find_sheet_item(model, "Financial_model", "financial_model.total_assets")
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
            after_id="financial_model.deferred_income_tax_assets",
            new_id=item_id,
            context="total assets",
        )

def _insert_deferred_revenue_row(model: FinancialModel) -> None:
    item_id = "financial_model.deferred_revenue_current"
    insert_row = 92
    financial_sheet = model.sheets["Financial_model"]

    if any(item.id == item_id for _, item in _iter_items(model)):
        raise AssertionError("Synthetic deferred_revenue row already exists")

    for section in financial_sheet.sections:
        for item in section.line_items:
            if item.row >= insert_row:
                item.row += 1

    target_section = _find_section_with_item(financial_sheet, "financial_model.accrued_expenses")
    target_section.line_items.append(
        LineItem(
            id=item_id,
            label=" Deferred revenue",
            row=insert_row,
            item_type=ItemType.derived,
            unit=Unit.dollars,
        )
    )

    current_liabilities = _find_sheet_item(
        model,
        "Financial_model",
        "financial_model.total_current_liabilities_before_client_fund_obligations",
    )
    for spec in (current_liabilities.historical, current_liabilities.projected):
        if spec is None or spec.params.get("function") != "SUM":
            continue
        spec.params["items"] = _insert_sum_ref_after(
            spec.params.get("items"),
            after_id="financial_model.accrued_expenses",
            new_id=item_id,
            context="current liabilities subtotal",
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

    total_current_liabilities = _find_sheet_item(
        model, "Financial_model", "financial_model.total_current_liabilities"
    )
    for spec in (total_current_liabilities.historical, total_current_liabilities.projected):
        if spec is None:
            continue
        operands = spec.params.get("operands")
        if not isinstance(operands, list) or not operands or operands[0] != "+":
            raise AssertionError("total_current_liabilities formula has unexpected operands shape")
        spec.params["operands"] = [
            operands[0],
            *_insert_sum_ref_after(
                operands[1:],
                after_id="financial_model.current_liability_1",
                new_id=item_id,
                context="total current liabilities",
            ),
        ]

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

    total_current_liabilities = _find_sheet_item(
        model, "Financial_model", "financial_model.total_current_liabilities"
    )
    for spec in (total_current_liabilities.historical, total_current_liabilities.projected):
        if spec is None:
            continue
        operands = spec.params.get("operands")
        if not isinstance(operands, list) or not operands or operands[0] != "+":
            raise AssertionError("total_current_liabilities formula has unexpected operands shape")
        spec.params["operands"] = [
            operands[0],
            *_insert_sum_ref_after(
                operands[1:],
                after_id="financial_model.current_liability_2",
                new_id=item_id,
                context="total current liabilities",
            ),
        ]

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

    total_liabilities = _find_sheet_item(
        model, "Financial_model", "financial_model.total_liabilities"
    )
    for spec in (total_liabilities.historical, total_liabilities.projected):
        if spec is None:
            continue
        expr = spec.params.get("expr")
        if not isinstance(expr, dict) or expr.get("op") != "+":
            raise AssertionError("total_liabilities formula is missing the expected additive expression")
        args = expr.get("args")
        if not isinstance(args, list) or not args:
            raise AssertionError("total_liabilities formula is missing expression args")
        noncurrent_liabilities = args[0]
        if not isinstance(noncurrent_liabilities, dict) or noncurrent_liabilities.get("op") != "SUM":
            raise AssertionError("total_liabilities formula is missing the noncurrent liability SUM")
        sum_args = noncurrent_liabilities.get("args")
        after_id = "financial_model.long_term_debt"
        if isinstance(sum_args, list) and not any(
            isinstance(ref, LineItemRef) and ref.id == after_id for ref in sum_args
        ):
            after_id = "financial_model.deferred_income_tax_liabilities"
        noncurrent_liabilities["args"] = _insert_sum_ref_after(
            sum_args,
            after_id=after_id,
            new_id=item_id,
            context="total liabilities",
        )

def _insert_inventory_row(model: FinancialModel) -> None:
    inventory_id = "financial_model.inventory"
    insert_row = 68
    financial_sheet = model.sheets["Financial_model"]

    if any(item.id == inventory_id for _, item in _iter_items(model)):
        raise AssertionError("Synthetic inventory row already exists")

    for section in financial_sheet.sections:
        for item in section.line_items:
            if item.row >= insert_row:
                item.row += 1

    target_section = next(
        (
            section
            for section in financial_sheet.sections
            if any(item.id == "financial_model.accounts_receivable_net" for item in section.line_items)
        ),
        None,
    )
    if target_section is None:
        raise AssertionError("Could not find Financial_model section for inventory insertion")

    target_section.line_items.append(
        LineItem(
            id=inventory_id,
            label=" Inventory",
            row=insert_row,
            item_type=ItemType.derived,
            unit=Unit.dollars,
        )
    )

    current_assets = next(
        (
            item
            for section in financial_sheet.sections
            for item in section.line_items
            if item.id == "financial_model.total_current_assets_before_funds_held_for_clients"
        ),
        None,
    )
    if current_assets is None:
        raise AssertionError("Could not find current assets subtotal for inventory insertion")

    for spec in (current_assets.historical, current_assets.projected):
        if spec is None or spec.params.get("function") != "SUM":
            continue
        items = spec.params.get("items")
        if not isinstance(items, list):
            raise AssertionError("Current assets subtotal is missing SUM items")

        updated_items = []
        inserted = False
        for ref in items:
            updated_items.append(ref)
            if isinstance(ref, LineItemRef) and ref.id == "financial_model.accounts_receivable_net":
                updated_items.append(LineItemRef(id=inventory_id))
                inserted = True

        if not inserted:
            raise AssertionError("Could not insert inventory into current assets subtotal")
        spec.params["items"] = updated_items

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
        "tpl.fm.adjusted_earnings.stock_based_compensation_and_related_payroll_taxes",
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
                LineItemRef(id="tpl.fm.adjusted_earnings.stock_based_compensation_and_related_payroll_taxes"),
                LineItemRef(id="tpl.fm.adjusted_earnings.other_items"),
                LineItemRef(id="tpl.fm.adjusted_earnings.amortization_of_acquired_intangibles"),
            ]
        },
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

    adjustment_effective_tax_rate = _find_sheet_item(
        model,
        "Assumptions",
        "tpl.a.tax_net_income.effective_tax_rate",
    )
    adjustment_effective_tax_rate.projected = FormulaSpec(
        type=FormulaType.ref,
        subtype="cell_ref",
        params={"source": LineItemRef(id="tpl.a.tax_net_income.tax_rate")},
    )
    adjustment_effective_tax_rate.formula_periods = sorted(
        {int(period) for period in (adjustment_effective_tax_rate.formula_periods or [])}
        | set(projection_periods)
    )

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


__all__ = [
    "_iter_items",
    "_iter_formula_specs",
    "_extract_refs",
    "_apply_scenario_linked_offset_params",
    "_find_sheet_item",
    "_find_section_with_item",
    "_apply_label_overrides",
    "_insert_sum_ref_after",
    "_insert_other_non_current_assets_row",
    "_insert_deferred_revenue_row",
    "_insert_other_current_liabilities_row",
    "_insert_commercial_paper_row",
    "_insert_deferred_revenue_noncurrent_row",
    "_insert_inventory_row",
    "_insert_investment_activity_rows",
    "_clear_cash_flow_artifact_historicals",
    "_set_if_applicable_default_zero_historicals",
    "_normalize_cash_flow_projection_links",
    "_normalize_valuation_projection_links",
    "_normalize_generic_cash_reconciliation",
]
