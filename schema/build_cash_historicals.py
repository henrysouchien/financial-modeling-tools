"""Cash-history build helpers for schema build orchestration."""

from __future__ import annotations

import sys

from .build_formula_eval import (
    _evaluate_formula_simple as _formula_evaluate_formula_simple,
    _lookup_formula_value as _formula_lookup_formula_value,
)
from .build_model_items import _iter_items as _model_iter_items
from .build_value_writers import _set_constant_override as _writer_set_constant_override
from .models import FinancialModel, FormulaType, ValueProvenance


_CASH_BEGINNING_ITEM_ID = "tpl.fm.cash_flow.cash_and_cash_equivalents_beginning_of_period"
_CASH_END_ITEM_ID = "tpl.fm.cash_flow.cash_and_cash_equivalents_end_of_period"
_NET_CHANGE_IN_CASH_ITEM_ID = "tpl.fm.cash_flow.net_change_in_cash_and_cash_equivalents"


def _parent_attr(name: str, fallback):
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    return getattr(parent, name, fallback) if parent is not None else fallback


def _seed_cash_beginning_of_period(model: FinancialModel) -> bool:
    """Seed the oldest historical beginning cash from ending cash and net change."""

    historical_periods = [
        int(period)
        for period in (
            model.time_structure.historical_periods
            or model.time_structure.historical_years
            or []
        )
    ]
    if not historical_periods:
        return False
    first_year = historical_periods[0]

    cash_beginning_item_id = _parent_attr("_CASH_BEGINNING_ITEM_ID", _CASH_BEGINNING_ITEM_ID)
    cash_end_item_id = _parent_attr("_CASH_END_ITEM_ID", _CASH_END_ITEM_ID)
    net_change_in_cash_item_id = _parent_attr(
        "_NET_CHANGE_IN_CASH_ITEM_ID",
        _NET_CHANGE_IN_CASH_ITEM_ID,
    )
    lookup_formula_value = _parent_attr("_lookup_formula_value", _formula_lookup_formula_value)
    evaluate_formula_simple = _parent_attr(
        "_evaluate_formula_simple",
        _formula_evaluate_formula_simple,
    )
    set_constant_override = _parent_attr(
        "_set_constant_override",
        _writer_set_constant_override,
    )

    try:
        beginning_item = model.get_item(cash_beginning_item_id)
        end_item = model.get_item(cash_end_item_id)
        net_change_item = model.get_item(net_change_in_cash_item_id)
    except KeyError:
        return False

    existing_beginning = lookup_formula_value(
        model,
        cash_beginning_item_id,
        first_year,
        {},
    )
    if existing_beginning is not None:
        return False

    end_cash = lookup_formula_value(model, cash_end_item_id, first_year, {})
    if end_cash is None:
        end_cash = evaluate_formula_simple(model, end_item, first_year, {})
    if end_cash is None:
        return False

    net_change = lookup_formula_value(model, net_change_in_cash_item_id, first_year, {})
    if net_change is None:
        net_change = evaluate_formula_simple(model, net_change_item, first_year, {})
    if net_change is None:
        return False

    set_constant_override(beginning_item, first_year, float(end_cash) - float(net_change))
    return True


def _has_existing_imported_historicals(model: FinancialModel, historical_periods: list[int]) -> bool:
    historical_set = {int(period) for period in historical_periods}
    iter_items = _parent_attr("_iter_items", _model_iter_items)
    value_provenance = _parent_attr("ValueProvenance", ValueProvenance)
    formula_type = _parent_attr("FormulaType", FormulaType)
    for item in iter_items(model):
        if not item.data_concept_id:
            continue
        if item.values is not None:
            for year, cell in item.values.values.items():
                if (
                    int(year) in historical_set
                    and cell.provenance in {
                        value_provenance.imported_fmp,
                        value_provenance.imported_edgar,
                    }
                ):
                    return True
        if item.overrides:
            for year, spec in item.overrides.items():
                if int(year) in historical_set and spec.type is formula_type.constant:
                    return True
    return False


__all__ = [
    "_CASH_BEGINNING_ITEM_ID",
    "_CASH_END_ITEM_ID",
    "_NET_CHANGE_IN_CASH_ITEM_ID",
    "_has_existing_imported_historicals",
    "_seed_cash_beginning_of_period",
]
