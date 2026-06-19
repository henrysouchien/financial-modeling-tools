from __future__ import annotations

import sys
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from .model_readiness_common import (
    _computed_values,
    _latest_numeric_value,
    _nearly_equal,
    _normalize_computed_values,
)
from .models import FinancialModel


ValuationInputReadinessStatus = Literal["complete", "incomplete"]
ValuationInputReadinessSeverity = Literal["info", "warning", "error"]

# Keep this small policy local to avoid a dependency cycle with schema.build,
# which consumes the readiness contracts defined in schema.model_readiness.
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
_VALUATION_REQUIRED_INPUTS = (
    "stock_price",
    "raw_beta",
    "adjusted_beta",
    "risk_free_rate",
    "equity_risk_premium",
    "sofr_rate",
    "credit_spread",
)
_VALUATION_PLACEHOLDER_VALUES = {
    "tpl.v.current_valuation.stock_price": 100.0,
    "tpl.v.cost_of_equity.risk_free_rate": 0.04,
    "tpl.v.cost_of_equity.equity_risk_premium": 0.045,
    "tpl.v.cost_of_equity.raw_beta": 1.0,
    "tpl.v.cost_of_equity.beta_floor": 1.0,
    "tpl.v.wacc.sofr_rate": 0.05,
    "tpl.v.wacc.credit_spread": 0.01,
}
_MARKET_BETA_ANCHOR = 1.0


class ValuationInputReadinessFlag(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    code: str
    severity: ValuationInputReadinessSeverity
    message: str | None = None


class ValuationInputReadiness(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    status: ValuationInputReadinessStatus = "incomplete"
    populated: list[str] = Field(default_factory=list)
    missing: list[str] = Field(default_factory=list)
    sources: dict[str, str] = Field(default_factory=dict)
    flags: list[ValuationInputReadinessFlag] = Field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


def _compat(name: str, default: Any) -> Any:
    parent = sys.modules.get("schema.model_readiness")
    if parent is None:
        parent = sys.modules.get("model_readiness")
    return getattr(parent, name, default) if parent is not None else default


def compute_valuation_input_readiness(
    model: FinancialModel,
    *,
    computed_values: dict[str, dict[int, float]] | None = None,
) -> ValuationInputReadiness:
    """Return deterministic valuation-input completeness from model row readback."""

    model.build_index()
    computed_values_fn = _compat("_computed_values", _computed_values)
    normalize_computed_values_fn = _compat("_normalize_computed_values", _normalize_computed_values)
    latest_numeric_value_fn = _compat("_latest_numeric_value", _latest_numeric_value)
    nearly_equal_fn = _compat("_nearly_equal", _nearly_equal)
    values = normalize_computed_values_fn(
        computed_values if computed_values is not None else computed_values_fn(model)
    )
    populated: list[str] = []
    missing: list[str] = []
    sources: dict[str, str] = {}
    has_legacy_placeholder = False
    economic_inputs = _compat("_VALUATION_ECONOMIC_INPUTS", _VALUATION_ECONOMIC_INPUTS)
    required_inputs = _compat("_VALUATION_REQUIRED_INPUTS", _VALUATION_REQUIRED_INPUTS)
    placeholder_values = _compat("_VALUATION_PLACEHOLDER_VALUES", _VALUATION_PLACEHOLDER_VALUES)
    market_beta_anchor = _compat("_MARKET_BETA_ANCHOR", _MARKET_BETA_ANCHOR)

    for field in required_inputs:
        item_id = economic_inputs[field]
        value = latest_numeric_value_fn(values.get(item_id, {}))
        if value is None:
            missing.append(field)
        else:
            populated.append(field)
            sources[field] = "workbook_readback"

    for item_id, placeholder_value in placeholder_values.items():
        value = latest_numeric_value_fn(values.get(item_id, {}))
        if nearly_equal_fn(value, placeholder_value, tolerance=1e-12):
            has_legacy_placeholder = True
            break

    raw_beta = latest_numeric_value_fn(values.get(economic_inputs["raw_beta"], {}))
    adjusted_beta = latest_numeric_value_fn(values.get(economic_inputs["adjusted_beta"], {}))
    if (
        nearly_equal_fn(raw_beta, placeholder_values["tpl.v.cost_of_equity.raw_beta"], tolerance=1e-12)
        and nearly_equal_fn(adjusted_beta, market_beta_anchor, tolerance=1e-12)
    ):
        has_legacy_placeholder = True

    flag_model = _compat("ValuationInputReadinessFlag", ValuationInputReadinessFlag)
    flags: list[ValuationInputReadinessFlag] = []
    if missing:
        flags.append(
            flag_model(
                code="valuation_inputs_missing",
                severity="warning",
                message=(
                    "One or more required valuation input rows are blank; do not "
                    "treat DCF/WACC outputs as clean until the inputs are sourced."
                ),
            )
        )
    if has_legacy_placeholder:
        flags.append(
            flag_model(
                code="legacy_valuation_placeholders_present",
                severity="warning",
                message=(
                    "One or more valuation input rows match legacy template "
                    "placeholder values; treat them as stale or ambiguous unless "
                    "independently sourced."
                ),
            )
        )

    readiness_model = _compat("ValuationInputReadiness", ValuationInputReadiness)
    return readiness_model(
        status="complete" if not missing and not has_legacy_placeholder else "incomplete",
        populated=populated,
        missing=missing,
        sources=sources,
        flags=flags,
    )


def _valuation_input_status(
    readiness: ValuationInputReadiness | dict[str, Any] | None,
) -> tuple[str | None, list[str]]:
    if readiness is None:
        return None, []
    if isinstance(readiness, dict):
        status = readiness.get("status")
        missing = readiness.get("missing") or []
    else:
        status = readiness.status
        missing = readiness.missing
    return str(status) if status is not None else None, [str(item) for item in missing]


__all__ = [
    "ValuationInputReadiness",
    "ValuationInputReadinessFlag",
    "ValuationInputReadinessSeverity",
    "ValuationInputReadinessStatus",
    "_MARKET_BETA_ANCHOR",
    "_VALUATION_ECONOMIC_INPUTS",
    "_VALUATION_PLACEHOLDER_VALUES",
    "_VALUATION_REQUIRED_INPUTS",
    "_valuation_input_status",
    "compute_valuation_input_readiness",
]
