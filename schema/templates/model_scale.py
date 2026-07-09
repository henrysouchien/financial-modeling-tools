"""Template helpers for explicit financial model scale metadata."""

from __future__ import annotations

from typing import Iterable

from ..formatter import SIA_FORMATTER
from ..models import FormulaSpec, FormulaType, ItemType, LineItem, ModelScale, Unit


PRICE_OR_UNIT_DOLLAR_ROW_IDS = {
    "tpl.a.revenue_drivers.price_driver_1",
    "tpl.a.revenue_drivers.business_segment_2_price_driver_1",
    "tpl.a.dividends_shares.expected_average_share_price",
}
FORCED_STATEMENT_DOLLAR_ROW_IDS = {
    "tpl.fm.cash_flow.effect_of_exchange_rate_on_cash",
}

_NON_VALUE_ID_TOKENS = (
    ".header.",
    "_header",
    "_ticker",
    ".ticker",
    "scenario_value",
    "scenario_selector",
)
_UNIT_SCALE_ID_TOKENS = (
    "volume_driver",
    "operating_metric",
    "days_sales_outstanding",
    "days_payable_outstanding",
    "revenue_per_",
    "per_unit",
    "average_share_price",
    "share_price",
    ".expected_value.",
)
_UNIT_SCALE_LABEL_TOKENS = (
    "price",
    "per share",
    "per client",
    "per unit",
    "days sales outstanding",
    "days payable outstanding",
)


def _iter_formula_specs(item: LineItem) -> Iterable[FormulaSpec]:
    if item.historical is not None:
        yield item.historical
    if item.projected is not None:
        yield item.projected
    if item.overrides:
        yield from item.overrides.values()


def _is_string_constant(spec: FormulaSpec) -> bool:
    return spec.type is FormulaType.constant and isinstance(spec.params.get("value"), str)


def _is_non_value_row(item: LineItem) -> bool:
    item_id = item.id.lower()
    if any(token in item_id for token in _NON_VALUE_ID_TOKENS):
        return True
    specs = list(_iter_formula_specs(item))
    return bool(specs) and all(
        _is_string_constant(spec) or spec.type is FormulaType.raw for spec in specs
    )


def requires_template_model_scale(item: LineItem) -> bool:
    """Return whether a template row must carry explicit scale metadata."""

    if item.item_type in {ItemType.header, ItemType.spacer}:
        return False
    if item.unit is not Unit.dollars:
        return False
    if (
        item.id not in FORCED_STATEMENT_DOLLAR_ROW_IDS
        and SIA_FORMATTER.number_format_for(item) != SIA_FORMATTER.dollar_format
    ):
        return False
    return not _is_non_value_row(item)


def infer_template_model_scale(item: LineItem) -> ModelScale | None:
    """Infer template-row scale for generated SIA artifacts."""

    if not requires_template_model_scale(item):
        return None

    item_id = item.id.lower()
    label = item.label.lower()
    if item.id in FORCED_STATEMENT_DOLLAR_ROW_IDS:
        return "millions"
    if item.id in PRICE_OR_UNIT_DOLLAR_ROW_IDS:
        return "units"
    if any(token in item_id for token in _UNIT_SCALE_ID_TOKENS):
        return "units"
    if any(token in label for token in _UNIT_SCALE_LABEL_TOKENS):
        return "units"
    return "millions"


def apply_template_model_scales(model) -> None:
    """Populate explicit scale metadata for generated template rows."""

    for sheet in model.sheets.values():
        for section in sheet.sections:
            for item in section.line_items:
                scale = infer_template_model_scale(item)
                if scale is not None:
                    item.model_scale = scale


def assert_template_model_scales_declared(model) -> None:
    """Fail if any value-bearing dollar template row lacks explicit scale metadata."""

    missing: list[str] = []
    for sheet in model.sheets.values():
        for section in sheet.sections:
            for item in section.line_items:
                if requires_template_model_scale(item) and "model_scale" not in item.model_fields_set:
                    missing.append(item.id)
    if missing:
        sample = ", ".join(sorted(missing)[:20])
        raise AssertionError(f"value-bearing dollar template rows missing model_scale: {sample}")


def require_declared_model_scale(item: LineItem) -> None:
    """Validate a builder-created item declares scale when the row requires it."""

    if requires_template_model_scale(item) and "model_scale" not in item.model_fields_set:
        raise ValueError(f"{item.id} is a value-bearing dollar row and must declare model_scale")
