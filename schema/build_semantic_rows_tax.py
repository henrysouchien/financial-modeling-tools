"""Canonical tax-schedule materialization for annual model builds."""

from __future__ import annotations

from typing import Any

from .build_semantic_rows import _insert_line_item_after
from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
)
from .required_role_catalog import load_required_role_catalog
from industry_slice_core.industry_types import Unit


_PRETAX_INCOME_ITEM_ID = "tpl.fm.income_statement.income_before_income_taxes"
_INCOME_TAX_PROVISION_ITEM_ID = (
    "tpl.fm.income_statement.income_tax_expense_benefit"
)
_NET_INCOME_ITEM_ID = "tpl.fm.income_statement.net_income"
_TAX_SCHEDULE_PRETAX_ITEM_ID = (
    "tpl.a.tax_net_income.income_before_income_taxes"
)
_TAX_SCHEDULE_PROVISION_ITEM_ID = (
    "tpl.a.tax_net_income.income_tax_expense_benefit"
)
_TAX_RATE_ITEM_ID = "tpl.a.tax_net_income.tax_rate"
_CURRENT_TAX_EXPENSE_ITEM_ID = "tpl.a.tax_net_income.current_tax_expense"
_DEFERRED_TAX_EXPENSE_ITEM_ID = "tpl.a.tax_net_income.deferred_tax_expense"
_DEFERRED_TAX_ASSET_ITEM_ID = "tpl.fm.balance_sheet.deferred_income_tax_assets"
_DEFERRED_TAX_LIABILITY_ITEM_ID = (
    "tpl.fm.balance_sheet.deferred_income_tax_liabilities"
)


def _deferred_tax_inclusion_fact(
    model: FinancialModel,
    item_id: str,
    period: int | None = None,
) -> FormulaSpec | None:
    """The cited inclusion on a deferred-tax balance row, or ``None``.

    ``period`` is a historical column; the default is the last historical one,
    the fact the forecast reads.
    """

    historical = (
        model.time_structure.historical_periods
        or model.time_structure.historical_years
        or []
    )
    item = model._index.get(item_id)
    if item is None or (period is None and not historical):
        return None
    fact = (item.overrides or {}).get(period if period is not None else max(historical))
    if (
        fact is not None
        and fact.subtype == "not_separately_presented"
        and fact.params.get("included_in")
    ):
        return fact
    return None


def _liability_change() -> dict[str, Any]:
    return {
        "op": "-",
        "left": LineItemRef(id=_DEFERRED_TAX_LIABILITY_ITEM_ID),
        "right": LineItemRef(id=_DEFERRED_TAX_LIABILITY_ITEM_ID, t=-1),
    }


def _asset_change() -> dict[str, Any]:
    return {
        "op": "-",
        "left": LineItemRef(id=_DEFERRED_TAX_ASSET_ITEM_ID, t=-1),
        "right": LineItemRef(id=_DEFERRED_TAX_ASSET_ITEM_ID),
    }


def _deferred_tax_roll_forward_expr(
    model: FinancialModel,
    *,
    projected: bool,
    period: int | None = None,
) -> dict[str, Any]:
    """Keep each filed balance definition in the historical and forecast bridge.

    A cited inclusion is not zero: the included balance's change already lives in
    the change of the line it is included in (the net liability change, for an
    asset), so the cited balance is no term here. History reads that period's
    fact; the forecast reads the last historical fact. Separately presented
    balances retain the gross split.
    """

    cited = projected or period is not None
    terms = [
        change()
        for item_id, change in (
            (_DEFERRED_TAX_LIABILITY_ITEM_ID, _liability_change),
            (_DEFERRED_TAX_ASSET_ITEM_ID, _asset_change),
        )
        if not (
            cited
            and (
                _balance_cited_absent(model, item_id)
                or _deferred_tax_inclusion_fact(model, item_id, period) is not None
            )
        )
    ]
    if not terms:
        # Neither balance is a cell: `_refresh_deferred_tax_formulas` holds both tax rows
        # cited-NA in every period, so no cell evaluates this placeholder.
        return {"op": "+", "args": [_liability_change(), _asset_change()]}
    if len(terms) == 1:
        return terms[0]
    return {"op": "+", "args": terms}


def _balance_cited_absent(model: FinancialModel, item_id: str) -> bool:
    """Whether the filing cites the balance absent: no historical cell, a cited forecast marker.

    A cited-absent balance has no cell for its change to read: its movement is not a
    term of the tax bridge (a cell that is not there is no operand, never a zero).
    A balance with no history and no citation is owed, and its change stays a term.
    """

    item = model._index.get(item_id)
    if item is None or item.historical is not None:
        return False
    marker = item.projected
    if marker is None or marker.subtype != "not_applicable" or not marker.params.get("absence_citation"):
        return False
    years = model.time_structure.historical_periods or model.time_structure.historical_years or []
    return not any(
        (item.overrides or {}).get(int(year)) is not None
        or (item.values is not None and int(year) in item.values.values)
        for year in years
    )


def _deferred_tax_balance_absence(
    model: FinancialModel,
    item_id: str,
    period: int | None = None,
) -> FormulaSpec | None:
    """The cited evidence that a deferred-tax balance has no cell of its own, or ``None``.

    Two evidence kinds, one meaning: a cited inclusion in another line (``period``'s fact,
    the last historical one by default) or a cited absence in every window.
    """

    fact = _deferred_tax_inclusion_fact(model, item_id, period)
    if fact is not None:
        return fact
    if _balance_cited_absent(model, item_id):
        return model._index[item_id].projected
    return None


def _absent_balances_marker(asset: FormulaSpec, liability: FormulaSpec) -> FormulaSpec:
    """The cited ``=NA()`` the tax rows hold when neither deferred-tax balance is a cell.

    Both balances cited as included elsewhere or absent leave no movement to split the
    provision by: current and deferred tax are not applicable, never a roll over cells that
    are not there. The marker is the unified raw not-applicable spec the graph reads as
    ``EvidencedAbsent``: the balances' ``absence_citation`` when one was authored, and
    ``included_in`` when a balance is cited as included elsewhere (which makes the
    absence ``not_separately_presented`` rather than ``not_applicable_evidenced``).
    """

    included_spec = (
        asset if asset.params.get("included_in")
        else liability if liability.params.get("included_in")
        else None
    )
    included_in = str(included_spec.params.get("included_in")) if included_spec is not None else ""
    citation_spec = included_spec if included_spec is not None else asset
    absence_citation = (
        citation_spec.params.get("absence_citation") or liability.params.get("absence_citation")
    )
    params: dict[str, Any] = {
        "formula": "=NA()",
        "required_role": "statement.bs.deferred_tax_assets",
        "constituent_role": "statement.bs.deferred_tax_assets",
    }
    if absence_citation:
        params["absence_citation"] = (
            dict(absence_citation) if isinstance(absence_citation, dict) else absence_citation
        )
    if included_in:
        params["included_in"] = included_in
    return FormulaSpec(type=FormulaType.raw, subtype="not_applicable", params=params)


def _deferred_tax_cash_flow_terms(model: FinancialModel) -> list[dict[str, Any]]:
    """The deferred-tax balance changes the forecast cash bridge still owns.

    A balance the filing includes in another line has no dollar of its own: its
    change is that line's change, which the bridge already carries. A balance
    the filing cites absent has no change to carry. Each other
    separately presented balance keeps its own change term.
    """

    terms: list[dict[str, Any]] = []
    for item_id, change in (
        (_DEFERRED_TAX_LIABILITY_ITEM_ID, _liability_change),
        (_DEFERRED_TAX_ASSET_ITEM_ID, _asset_change),
    ):
        if (
            not _balance_cited_absent(model, item_id)
            and _deferred_tax_inclusion_fact(model, item_id) is None
        ):
            terms.append(change())
    return terms


def _refresh_deferred_tax_formulas(model: FinancialModel) -> None:
    """Re-read balance pins on engine-owned tax formulas, not authored cells."""
    deferred = model._index.get(_DEFERRED_TAX_EXPENSE_ITEM_ID)
    if deferred is None:
        return
    note = "deferred_tax_expense_from_balance_sheet_roll_forward"
    if deferred.projected is not None and deferred.projected.note == note:
        deferred.projected = deferred.projected.model_copy(
            update={"params": {"expr": _deferred_tax_roll_forward_expr(model, projected=True)}},
            deep=True,
        )
    historical = sorted(int(p) for p in (model.time_structure.historical_periods or model.time_structure.historical_years or []))
    current_tax = model._index.get(_CURRENT_TAX_EXPENSE_ITEM_ID)
    for period in historical:
        asset_fact = _deferred_tax_balance_absence(model, _DEFERRED_TAX_ASSET_ITEM_ID, period)
        liab_fact = _deferred_tax_balance_absence(model, _DEFERRED_TAX_LIABILITY_ITEM_ID, period)
        if asset_fact is not None and liab_fact is not None:
            for tax_item in (deferred, current_tax):
                if tax_item is not None:
                    if tax_item.overrides is None:
                        tax_item.overrides = {}
                    tax_item.overrides[period] = _absent_balances_marker(asset_fact, liab_fact)
                    if tax_item.formula_periods is not None and period not in tax_item.formula_periods:
                        tax_item.formula_periods = sorted({*tax_item.formula_periods, period})
            continue
        if period == historical[0]:
            continue
        original = (deferred.overrides or {}).get(period, deferred.historical)
        if original is not None and original.note == note:
            period_formula = original.model_copy(
                update={"params": {"expr": _deferred_tax_roll_forward_expr(
                    model, projected=False, period=period,
                )}},
                deep=True,
            )
            # Only a period's filed balance definition needs an override.
            # Repeating the general historical formula manufactures cells when
            # a later schedule pass changes the row's period ownership.
            if period_formula == deferred.historical:
                deferred.overrides = {
                    year: spec for year, spec in (deferred.overrides or {}).items()
                    if year != period
                } or None
            else:
                if deferred.overrides is None:
                    deferred.overrides = {}
                deferred.overrides[period] = period_formula
    if historical:
        last_hist = max(historical)
        asset_last = _deferred_tax_balance_absence(model, _DEFERRED_TAX_ASSET_ITEM_ID, last_hist)
        liab_last = _deferred_tax_balance_absence(model, _DEFERRED_TAX_LIABILITY_ITEM_ID, last_hist)
        if asset_last is not None and liab_last is not None:
            for tax_item in (deferred, current_tax):
                if tax_item is not None:
                    tax_item.projected = _absent_balances_marker(asset_last, liab_last)


def _bind_or_insert_tax_schedule_item(
    model: FinancialModel,
    *,
    after_item_id: str,
    item: LineItem,
) -> LineItem:
    """Bind an existing tax-schedule row, or insert it after the anchor.

    Row insertion is owned by the canonical `_insert_line_item_after` helper in
    `build_semantic_rows`, which gives an inserted row its anchor's ``model_scale``.
    """

    try:
        return model.get_item(item.id)
    except KeyError:
        pass

    return _insert_line_item_after(
        model,
        anchor_item_id=after_item_id,
        new_item=item,
    )


def _materialize_tax_schedule(model: FinancialModel) -> dict[str, Any]:
    """Compile provision, deferred, and current tax through canonical rows.

    The reported/projected provision continues to own the existing income
    statement identity. Parent net income also deducts NCI earnings when the
    model carries that catalog row and the row participates (a cited-absent
    optional row adds no term); a participating row with no forecast stays a
    required missing input, while an absent row adds no term. The two net-new
    schedule rows expose the current and deferred split. Deferred tax expense
    reads the filed balance definition: separate balances supply both changes;
    an asset included in the net tax liability supplies only that net liability
    change in the forecast.
    """

    if not model._index:
        model.build_index()
    projection_periods = sorted(
        int(period)
        for period in (
            model.time_structure.projection_periods
            or model.time_structure.projection_years
            or []
        )
    )
    historical_periods = sorted(
        int(period)
        for period in (
            model.time_structure.historical_periods
            or model.time_structure.historical_years
            or []
        )
    )
    if not projection_periods:
        return {"materialized": [], "linked": []}

    deferred_tax = _bind_or_insert_tax_schedule_item(
        model,
        after_item_id=_TAX_SCHEDULE_PROVISION_ITEM_ID,
        item=LineItem(
            id=_DEFERRED_TAX_EXPENSE_ITEM_ID,
            label="Deferred tax expense (benefit)",
            row=0,
            item_type=ItemType.derived,
            unit=Unit.dollars,
        ),
    )
    current_tax = _bind_or_insert_tax_schedule_item(
        model,
        after_item_id=_DEFERRED_TAX_EXPENSE_ITEM_ID,
        item=LineItem(
            id=_CURRENT_TAX_EXPENSE_ITEM_ID,
            label="Current tax expense (benefit)",
            row=0,
            item_type=ItemType.derived,
            unit=Unit.dollars,
        ),
    )

    split_periods = sorted(
        set(historical_periods[1:]) | set(projection_periods)
    )
    deferred_tax_formula = FormulaSpec(
        type=FormulaType.arithmetic,
        params={"expr": _deferred_tax_roll_forward_expr(model, projected=False)},
        note="deferred_tax_expense_from_balance_sheet_roll_forward",
    )
    deferred_tax.historical = deferred_tax_formula
    deferred_tax.projected = deferred_tax_formula.model_copy(
        update={"params": {"expr": _deferred_tax_roll_forward_expr(model, projected=True)}},
        deep=True,
    )
    deferred_tax.formula_periods = split_periods

    current_tax_formula = FormulaSpec(
        type=FormulaType.arithmetic,
        subtype="add_subtract",
        params={
            "operands": [
                "-",
                LineItemRef(id=_TAX_SCHEDULE_PROVISION_ITEM_ID),
                LineItemRef(id=_DEFERRED_TAX_EXPENSE_ITEM_ID),
            ]
        },
        note="current_tax_equals_provision_less_deferred_tax",
    )
    current_tax.historical = current_tax_formula
    current_tax.projected = current_tax_formula.model_copy(deep=True)
    current_tax.formula_periods = split_periods
    _refresh_deferred_tax_formulas(model)

    schedule_pretax = model.get_item(_TAX_SCHEDULE_PRETAX_ITEM_ID)
    schedule_pretax.projected = FormulaSpec(
        type=FormulaType.ref,
        params={"source": LineItemRef(id=_PRETAX_INCOME_ITEM_ID)},
        note="canonical_pretax_income_to_tax_schedule_link",
    )
    schedule_pretax.formula_periods = sorted(
        set(schedule_pretax.formula_periods or []) | set(projection_periods)
    )

    schedule_provision = model.get_item(_TAX_SCHEDULE_PROVISION_ITEM_ID)
    schedule_provision.projected = FormulaSpec(
        type=FormulaType.driver,
        subtype="base_x_rate",
        params={
            "base": LineItemRef(id=_TAX_SCHEDULE_PRETAX_ITEM_ID),
            "rate": LineItemRef(id=_TAX_RATE_ITEM_ID),
        },
        note="canonical_tax_provision",
    )
    schedule_provision.formula_periods = sorted(
        set(schedule_provision.formula_periods or []) | set(projection_periods)
    )

    income_statement_provision = model.get_item(_INCOME_TAX_PROVISION_ITEM_ID)
    income_statement_provision.projected = FormulaSpec(
        type=FormulaType.ref,
        params={
            "source": LineItemRef(id=_TAX_SCHEDULE_PROVISION_ITEM_ID)
        },
        note="canonical_tax_schedule_to_income_statement_link",
    )
    income_statement_provision.formula_periods = sorted(
        set(income_statement_provision.formula_periods or [])
        | set(projection_periods)
    )

    # Deferred: build_company_grounded_schedules imports this module's tax ids.
    from .build_company_grounded_schedules import optional_component_participates
    from .build_real_data import _item_has_real_data

    catalog = load_required_role_catalog()
    nci_role = catalog.role(
        "statement.is.net_income_attributable_to_noncontrolling_interest"
    )
    nci_item_ids = (
        *nci_role.template_items,
        *catalog.population_item_ids(nci_role.required_role, ticker=model.company.ticker),
    )
    net_income_operands: list[str | LineItemRef] = [
        "-",
        LineItemRef(id=_PRETAX_INCOME_ITEM_ID),
        LineItemRef(id=_INCOME_TAX_PROVISION_ITEM_ID),
    ]
    for item_id in nci_item_ids:
        if item_id in model._index and any(
            optional_component_participates(model, item_id, period)
            for period in projection_periods
        ):
            net_income_operands.append(LineItemRef(id=item_id))
            break
    historical_nci_item_id: str | None = None
    for item_id in nci_item_ids:
        if item_id in model._index and any(
            _item_has_real_data(model._index[item_id], period, model=model)
            and optional_component_participates(model, item_id, period)
            for period in historical_periods
        ):
            historical_nci_item_id = item_id
            break

    net_income = model.get_item(_NET_INCOME_ITEM_ID)
    net_income.projected = FormulaSpec(
        type=FormulaType.arithmetic,
        subtype="add_subtract",
        params={"operands": net_income_operands},
        note="canonical_net_income_after_tax",
    )
    if historical_nci_item_id is not None:
        net_income.historical = FormulaSpec(
            type=FormulaType.arithmetic,
            subtype="add_subtract",
            params={"operands": ["-", LineItemRef(id=_PRETAX_INCOME_ITEM_ID), LineItemRef(id=_INCOME_TAX_PROVISION_ITEM_ID), LineItemRef(id=historical_nci_item_id)]},
            note="canonical_net_income_after_tax",
        )
    net_income.formula_periods = sorted(
        set(net_income.formula_periods or [])
        | set(projection_periods)
        | (set(historical_periods) if historical_nci_item_id is not None else set())
    )
    model.build_index()
    return {
        "materialized": [
            _DEFERRED_TAX_EXPENSE_ITEM_ID,
            _CURRENT_TAX_EXPENSE_ITEM_ID,
        ],
        "linked": [
            _PRETAX_INCOME_ITEM_ID,
            _TAX_SCHEDULE_PROVISION_ITEM_ID,
            _INCOME_TAX_PROVISION_ITEM_ID,
            _NET_INCOME_ITEM_ID,
        ],
    }


__all__ = [
    "_CURRENT_TAX_EXPENSE_ITEM_ID",
    "_DEFERRED_TAX_EXPENSE_ITEM_ID",
    "_materialize_tax_schedule",
]
