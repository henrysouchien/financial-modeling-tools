"""Valuation input population helpers for schema build orchestration."""

from __future__ import annotations

import sys
from typing import Any

from .build_model_items import _iter_items as _model_iter_items
from .model_readiness import ValuationInputReadiness
from .model_semantics import ValuationArtifact, ValuationInputValue
from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    ValueCell,
    ValueProvenance,
    ValueSeries,
)


# Legacy template placeholders. These values are retained only for readback
# diagnostics and stale-workbook detection; build-time population must not use
# them as valuation inputs.
_VALUATION_DEFAULTS = {
    "tpl.v.cost_of_equity.risk_free_rate": 0.04,
    "tpl.v.cost_of_equity.equity_risk_premium": 0.045,
    "tpl.v.wacc.sofr_rate": 0.05,
    "tpl.v.wacc.credit_spread": 0.01,
}
_VALUATION_PLACEHOLDER_VALUES = {
    **_VALUATION_DEFAULTS,
    "tpl.v.current_valuation.stock_price": 100.0,
    "tpl.v.cost_of_equity.raw_beta": 1.0,
    "tpl.v.cost_of_equity.beta_floor": 1.0,
}
_VALUATION_ECONOMIC_INPUTS = {
    "stock_price": "tpl.v.current_valuation.stock_price",
    "risk_free_rate": "tpl.v.cost_of_equity.risk_free_rate",
    "equity_risk_premium": "tpl.v.cost_of_equity.equity_risk_premium",
    "raw_beta": "tpl.v.cost_of_equity.raw_beta",
    "beta_floor": "tpl.v.cost_of_equity.beta_floor",
    "adjusted_beta": "tpl.v.cost_of_equity.beta",
    "sofr_rate": "tpl.v.wacc.sofr_rate",
    "credit_spread": "tpl.v.wacc.credit_spread",
}
_VALUATION_TERMINAL_INPUTS = {
    "terminal_growth_rate": "tpl.v.dcf.terminal_growth_base",
    "exit_multiple": "tpl.v.dcf.exit_multiple_base",
}
_VALUATION_TERMINAL_INPUT_PREFIXES = (
    "tpl.v.dcf.terminal_growth_",
    "tpl.v.dcf.exit_multiple_",
)
_VALUATION_TERMINAL_DEFAULTS = {
    "tpl.v.dcf.terminal_growth_bull": 0.04,
    "tpl.v.dcf.terminal_growth_base": 0.03,
    "tpl.v.dcf.terminal_growth_bear": 0.01,
    "tpl.v.dcf.exit_multiple_bull": 25.0,
    "tpl.v.dcf.exit_multiple_base": 18.0,
    "tpl.v.dcf.exit_multiple_bear": 15.0,
}
_VALUATION_TERMINAL_DEFAULT_SOURCE = "template.valuation_terminal_default"
_VALUATION_TERMINAL_DEFAULT_FIELDS = {
    "tpl.v.dcf.terminal_growth_base": "terminal_growth_rate",
    "tpl.v.dcf.exit_multiple_base": "exit_multiple",
}
_VALUATION_REQUIRED_INPUTS = (
    "stock_price",
    "raw_beta",
    "adjusted_beta",
    "risk_free_rate",
    "equity_risk_premium",
    "sofr_rate",
    "credit_spread",
)
_BLOOMBERG_STYLE_BETA_WEIGHT = 0.67
_MARKET_BETA_ANCHOR = 1.0


def _parent_attr(name: str, fallback):
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    return getattr(parent, name, fallback) if parent is not None else fallback


def _read_value(model: FinancialModel, item_id: str, period: int) -> float | None:
    """Read a populated model value; return None when the cell is absent."""

    try:
        item = model.get_item(item_id)
    except KeyError:
        return None
    if item.values is None:
        return None
    cell = item.values.values.get(int(period))
    if cell is None or cell.value is None:
        return None
    return float(cell.value)


def _derive_credit_spread(model: FinancialModel, sofr: float | None) -> float | None:
    """Derive spread as effective yield minus SOFR for WACC consistency."""

    if sofr is None:
        return None
    try:
        hist_periods = sorted(int(period) for period in model.time_structure.historical_periods)
        if len(hist_periods) < 2:
            return None
        current_year, prior_year = hist_periods[-1], hist_periods[-2]

        read_value = _parent_attr("_read_value", _read_value)
        interest_expense = read_value(
            model,
            "tpl.fm.adjusted_earnings.interest_expense",
            current_year,
        )
        ltd_current = read_value(model, "tpl.fm.balance_sheet.long_term_debt", current_year)
        ltd_prior = read_value(model, "tpl.fm.balance_sheet.long_term_debt", prior_year)
        if interest_expense is None or ltd_current is None or ltd_prior is None:
            return None
        if ltd_current <= 0 or ltd_prior <= 0:
            return None

        avg_debt = (ltd_current + ltd_prior) / 2.0
        if avg_debt <= 0:
            return None

        effective_yield = abs(interest_expense) / avg_debt
        spread = effective_yield - sofr
        if spread <= 0 or spread > 0.20:
            return None

        return float(spread)
    except (KeyError, AttributeError, ZeroDivisionError):
        return None


def _adjust_raw_beta(raw_beta: float, beta_floor: float | None = None) -> float:
    bloomberg_weight = _parent_attr(
        "_BLOOMBERG_STYLE_BETA_WEIGHT",
        _BLOOMBERG_STYLE_BETA_WEIGHT,
    )
    market_anchor = _parent_attr("_MARKET_BETA_ANCHOR", _MARKET_BETA_ANCHOR)
    adjusted = (bloomberg_weight * raw_beta) + ((1.0 - bloomberg_weight) * market_anchor)
    if beta_floor is None:
        return float(adjusted)
    return max(float(adjusted), float(beta_floor))


def _clear_valuation_input_values(model: FinancialModel) -> None:
    valuation_economic_inputs = _parent_attr(
        "_VALUATION_ECONOMIC_INPUTS",
        _VALUATION_ECONOMIC_INPUTS,
    )
    valuation_terminal_inputs = _parent_attr(
        "_VALUATION_TERMINAL_INPUTS",
        _VALUATION_TERMINAL_INPUTS,
    )
    valuation_terminal_input_prefixes = _parent_attr(
        "_VALUATION_TERMINAL_INPUT_PREFIXES",
        _VALUATION_TERMINAL_INPUT_PREFIXES,
    )
    iter_items = _parent_attr("_iter_items", _model_iter_items)
    item_type = _parent_attr("ItemType", ItemType)

    item_ids = set(valuation_economic_inputs.values()) | set(valuation_terminal_inputs.values())
    for item in iter_items(model):
        if item.item_type is item_type.input and any(
            item.id.startswith(prefix)
            for prefix in valuation_terminal_input_prefixes
        ):
            item_ids.add(item.id)

    for item_id in item_ids:
        try:
            model.get_item(item_id).values = None
        except KeyError:
            continue


def _set_valuation_input_value(
    model: FinancialModel,
    item_id: str,
    value: float,
    *,
    projection_periods: list[int],
    provenance: ValueProvenance,
) -> bool:
    try:
        target = model.get_item(item_id)
    except KeyError:
        return False
    value_series_cls = _parent_attr("ValueSeries", ValueSeries)
    value_cell_cls = _parent_attr("ValueCell", ValueCell)
    target.values = value_series_cls()
    for period in projection_periods:
        target.values.values[int(period)] = value_cell_cls(
            period=int(period),
            value=float(value),
            provenance=provenance,
        )
    return True


def _valuation_input_has_value(model: FinancialModel, item_id: str) -> bool:
    try:
        item = model.get_item(item_id)
    except KeyError:
        return False
    if item.values is None:
        return False
    return any(cell.value is not None for cell in item.values.values.values())


def _extract_first_numeric_with_source(
    fmp_data: dict | None,
    endpoints: tuple[str, ...],
    fields: tuple[str, ...],
) -> tuple[float | None, str | None]:
    if not fmp_data:
        return None, None
    for endpoint in endpoints:
        records = fmp_data.get(endpoint)
        if isinstance(records, dict):
            iterable = [records]
        else:
            iterable = list(records or [])
        for record in iterable:
            if not isinstance(record, dict):
                continue
            for field_name in fields:
                raw_value = record.get(field_name)
                if raw_value is None:
                    continue
                try:
                    return float(raw_value), f"fmp.{endpoint}.{field_name}"
                except (TypeError, ValueError):
                    continue
    return None, None


def populate_valuation_inputs(
    model: FinancialModel,
    fmp_data: dict | None,
    equity_risk_premium: float | None = None,
    equity_risk_premium_source: str | None = None,
    equity_risk_premium_rationale: str | None = None,
    equity_risk_premium_as_of: str | None = None,
    valuation: dict[str, Any] | None = None,
) -> ValuationInputReadiness:
    """Populate only sourced/explicit fixed-cell valuation inputs."""

    if equity_risk_premium is not None and not (0 < equity_risk_premium < 1):
        raise ValueError(
            f"equity_risk_premium must be a decimal between 0 and 1; got {equity_risk_premium}"
        )
    if not model._index:
        model.build_index()
    projection_periods = [int(period) for period in model.time_structure.projection_periods]
    valuation_required_inputs = _parent_attr(
        "_VALUATION_REQUIRED_INPUTS",
        _VALUATION_REQUIRED_INPUTS,
    )
    readiness_cls = _parent_attr("ValuationInputReadiness", ValuationInputReadiness)
    if not projection_periods:
        return readiness_cls(
            status="incomplete",
            missing=list(valuation_required_inputs),
            flags=[
                {
                    "code": "valuation_projection_periods_missing",
                    "severity": "error",
                    "message": (
                        "Cannot populate valuation inputs because the model has "
                        "no projection periods."
                    ),
                }
            ],
        )

    clear_valuation_input_values = _parent_attr(
        "_clear_valuation_input_values",
        _clear_valuation_input_values,
    )
    stored_valuation_inputs = _parent_attr("_stored_valuation_inputs", _stored_valuation_inputs)
    extract_first_numeric_with_source = _parent_attr(
        "_extract_first_numeric_with_source",
        _extract_first_numeric_with_source,
    )
    set_valuation_input_value = _parent_attr(
        "_set_valuation_input_value",
        _set_valuation_input_value,
    )
    adjust_raw_beta = _parent_attr("_adjust_raw_beta", _adjust_raw_beta)
    valuation_economic_inputs = _parent_attr(
        "_VALUATION_ECONOMIC_INPUTS",
        _VALUATION_ECONOMIC_INPUTS,
    )
    valuation_terminal_inputs = _parent_attr(
        "_VALUATION_TERMINAL_INPUTS",
        _VALUATION_TERMINAL_INPUTS,
    )
    valuation_terminal_defaults = _parent_attr(
        "_VALUATION_TERMINAL_DEFAULTS",
        _VALUATION_TERMINAL_DEFAULTS,
    )
    valuation_terminal_default_fields = _parent_attr(
        "_VALUATION_TERMINAL_DEFAULT_FIELDS",
        _VALUATION_TERMINAL_DEFAULT_FIELDS,
    )
    value_provenance = _parent_attr("ValueProvenance", ValueProvenance)
    value_series_cls = _parent_attr("ValueSeries", ValueSeries)
    value_cell_cls = _parent_attr("ValueCell", ValueCell)
    formula_spec_cls = _parent_attr("FormulaSpec", FormulaSpec)
    formula_type = _parent_attr("FormulaType", FormulaType)
    valuation_input_has_value = _parent_attr(
        "_valuation_input_has_value",
        _valuation_input_has_value,
    )

    clear_valuation_input_values(model)
    sources: dict[str, str] = {}
    flags: list[dict[str, Any]] = []
    populated: set[str] = set()
    stored_inputs = stored_valuation_inputs(valuation)

    stock_price, stock_price_source = extract_first_numeric_with_source(
        fmp_data,
        ("quote", "quotes", "profile", "company_profile"),
        ("price", "currentPrice"),
    )
    raw_beta, raw_beta_source = extract_first_numeric_with_source(
        fmp_data,
        ("profile", "company_profile", "key_metrics", "company-key-metrics", "company_key_metrics"),
        ("beta", "Beta"),
    )

    if stock_price is not None:
        if set_valuation_input_value(
            model,
            valuation_economic_inputs["stock_price"],
            stock_price,
            projection_periods=projection_periods,
            provenance=value_provenance.imported_fmp,
        ):
            populated.add("stock_price")
            sources["stock_price"] = stock_price_source or "fmp"
    if raw_beta is not None:
        if set_valuation_input_value(
            model,
            valuation_economic_inputs["raw_beta"],
            raw_beta,
            projection_periods=projection_periods,
            provenance=value_provenance.imported_fmp,
        ):
            populated.add("raw_beta")
            sources["raw_beta"] = raw_beta_source or "fmp"

    for field_name in ("risk_free_rate", "sofr_rate", "credit_spread"):
        stored = stored_inputs.get(field_name)
        if stored is None:
            continue
        if set_valuation_input_value(
            model,
            valuation_economic_inputs[field_name],
            stored.value_decimal,
            projection_periods=projection_periods,
            provenance=value_provenance.input,
        ):
            populated.add(field_name)
            sources[field_name] = stored.source or f"ticker_overrides.valuation.{field_name}"

    stored_erp = stored_inputs.get("equity_risk_premium")
    if stored_erp is not None:
        if set_valuation_input_value(
            model,
            valuation_economic_inputs["equity_risk_premium"],
            stored_erp.value_decimal,
            projection_periods=projection_periods,
            provenance=value_provenance.input,
        ):
            populated.add("equity_risk_premium")
            sources["equity_risk_premium"] = (
                stored_erp.source
                or "ticker_overrides.valuation.equity_risk_premium"
            )
    elif equity_risk_premium is not None:
        if set_valuation_input_value(
            model,
            valuation_economic_inputs["equity_risk_premium"],
            equity_risk_premium,
            projection_periods=projection_periods,
            provenance=value_provenance.input,
        ):
            populated.add("equity_risk_premium")
            sources["equity_risk_premium"] = (
                equity_risk_premium_source
                or "explicit.model_build_context"
            )

    stored_beta_floor = stored_inputs.get("beta_floor")
    beta_floor_value = stored_beta_floor.value_decimal if stored_beta_floor is not None else None
    if stored_beta_floor is not None:
        if set_valuation_input_value(
            model,
            valuation_economic_inputs["beta_floor"],
            stored_beta_floor.value_decimal,
            projection_periods=projection_periods,
            provenance=value_provenance.input,
        ):
            populated.add("beta_floor")
            sources["beta_floor"] = (
                stored_beta_floor.source
                or "ticker_overrides.valuation.beta_floor"
            )

    if raw_beta is not None:
        if set_valuation_input_value(
            model,
            valuation_economic_inputs["adjusted_beta"],
            adjust_raw_beta(raw_beta, beta_floor_value),
            projection_periods=projection_periods,
            provenance=value_provenance.derived,
        ):
            populated.add("adjusted_beta")
            sources["adjusted_beta"] = (
                "derived.blume_adjusted_raw_beta_with_beta_floor"
                if beta_floor_value is not None
                else "derived.blume_adjusted_raw_beta"
            )

    for field_name, item_id in valuation_terminal_inputs.items():
        stored = stored_inputs.get(field_name)
        if stored is None:
            continue
        target_item_id = stored.item_id or item_id
        if set_valuation_input_value(
            model,
            target_item_id,
            stored.value_decimal,
            projection_periods=projection_periods,
            provenance=value_provenance.input,
        ):
            populated.add(field_name)
            sources[field_name] = stored.source or f"ticker_overrides.valuation.{field_name}"

    defaulted_terminal_item_ids: list[str] = []
    for item_id, default_value in valuation_terminal_defaults.items():
        if valuation_input_has_value(model, item_id):
            continue
        if set_valuation_input_value(
            model,
            item_id,
            float(default_value),
            projection_periods=projection_periods,
            provenance=value_provenance.input,
        ):
            defaulted_terminal_item_ids.append(item_id)
            field_name = valuation_terminal_default_fields.get(item_id)
            if field_name is not None:
                populated.add(field_name)
                sources.setdefault(field_name, _VALUATION_TERMINAL_DEFAULT_SOURCE)
    if defaulted_terminal_item_ids:
        flags.append(
            {
                "code": "terminal_assumptions_defaulted",
                "severity": "warning",
                "message": (
                    "Terminal growth / exit multiple inputs were not fully authored; "
                    "seeded template defaults (base g=3.0%, exit 18x). DCF terminal "
                    "value may use defaults, not company-specific inputs."
                ),
            }
        )

    missing = [field for field in valuation_required_inputs if field not in populated]
    if any(field in missing for field in ("stock_price", "raw_beta", "adjusted_beta")):
        flags.append(
            {
                "code": "market_valuation_inputs_missing",
                "severity": "warning",
                "message": (
                    "Quote/profile valuation inputs are missing; stock price and "
                    "beta rows were left blank."
                ),
            }
        )
    if any(field in missing for field in ("risk_free_rate", "sofr_rate", "credit_spread")):
        flags.append(
            {
                "code": "macro_valuation_inputs_missing",
                "severity": "warning",
                "message": (
                    "Macro valuation inputs are missing; run valuation-inputs or "
                    "supply explicit inputs before clean DCF acceptance."
                ),
            }
        )
    if "equity_risk_premium" in missing:
        flags.append(
            {
                "code": "equity_risk_premium_missing",
                "severity": "warning",
                "message": (
                    "Equity risk premium was not supplied; the build leaves ERP "
                    "blank instead of defaulting it."
                ),
            }
        )

    try:
        discount_period = model.get_item("tpl.v.dcf.discount_period")
    except KeyError:
        discount_period = None
    if discount_period is not None:
        if discount_period.values is None:
            discount_period.values = value_series_cls()
        for index, period in enumerate(projection_periods):
            discount_period.values.values[int(period)] = value_cell_cls(
                period=int(period),
                value=float(0.5 + index),
                provenance=value_provenance.input,
            )

    try:
        ticker_item = model.get_item("tpl.v.current_valuation.ticker")
    except KeyError:
        ticker_item = None
    if ticker_item is not None:
        ticker_item.projected = formula_spec_cls(
            type=formula_type.constant,
            params={"value": model.company.ticker},
        )

    return readiness_cls(
        status="complete" if not missing else "incomplete",
        populated=sorted(populated),
        missing=missing,
        sources=dict(sorted(sources.items())),
        flags=flags,
    )


def _stored_valuation_inputs(valuation: dict[str, Any] | None) -> dict[str, ValuationInputValue]:
    if not valuation:
        return {}
    valuation_artifact_cls = _parent_attr("ValuationArtifact", ValuationArtifact)
    artifact = valuation_artifact_cls.model_validate(valuation)
    result: dict[str, ValuationInputValue] = {}
    for field_name in (
        "risk_free_rate",
        "sofr_rate",
        "credit_spread",
        "equity_risk_premium",
        "beta_floor",
    ):
        value = getattr(artifact.wacc, field_name)
        if value is not None:
            result[field_name] = value
    if artifact.terminal_growth_rate is not None:
        result["terminal_growth_rate"] = artifact.terminal_growth_rate
    if artifact.exit_multiple is not None:
        result["exit_multiple"] = artifact.exit_multiple
    return result
