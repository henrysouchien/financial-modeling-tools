"""Formatting rules for rendered financial models."""

from __future__ import annotations

from dataclasses import dataclass, fields as dataclass_fields
from typing import Optional

from .models import CellColor, ItemType, LineItem
from industry_slice_core.industry_types import Unit
from .style_guide_library import (
    StyleGuide,
    StyleGuideLibraryInvalid,
    library_default_style_guide,
    load_style_guide,
)


_PERCENTAGE_KEYWORDS = (
    "_growth",
    "_margin",
    "_pct_revenue",
    "_pct_",
    "_rate",
    "payout_ratio",
    "yield",
    "_conversion",
    "return_on_",
)
_SCENARIO_PERCENTAGE_SUFFIXES = (
    "volume_bull",
    "volume_base",
    "volume_bear",
    "margin_bull",
    "margin_base",
    "margin_bear",
)
_RATIO_KEYWORDS = (
    "debt_to_",
    "_to_ebitda",
    "_to_revenue",
    "_ratio",
    "_multiple",
    "_turns",
)
_SHARE_COUNT_KEYWORDS = (
    "shares_outstanding",
    "diluted_shares",
    "dilutive_shares",
    "shares_issued",
    "weighted_average_diluted",
)
_TOTAL_ROW_SUFFIXES = {
    "income_statement.total_revenue",
    "income_statement.gross_profit",
    "income_statement.total_operating_expenses",
    "income_statement.operating_income",
    "income_statement.net_income",
    "adjusted_earnings.ebitda",
    "adjusted_earnings.adjusted_ebitda",
    "adjusted_earnings.adjusted_operating_income",
    "adjusted_earnings.adjusted_net_income",
    "balance_sheet.total_current_assets",
    "balance_sheet.total_assets",
    "balance_sheet.total_current_liabilities",
    "balance_sheet.total_liabilities",
    "balance_sheet.total_equity",
    "balance_sheet.total_liabilities_and_equity",
    "cash_flow.operating_cash_flow",
    "cash_flow.investing_cash_flow",
    "cash_flow.financing_cash_flow",
    "cash_flow.free_cash_flow",
}


def _label_implies_percentage(label: str) -> bool:
    normalized = f" {str(label or '').strip().lower()} "
    stripped = normalized.strip()
    return (
        "(%" in stripped
        or stripped.startswith("%")
        or "% of " in stripped
        or "% change" in stripped
        or "% chg" in stripped
        or " as % " in normalized
    )


def _validated_number_format(value: str, *, source: str) -> str | None:
    """Return a normalized explicit format or reject malformed format text."""

    normalized = str(value or "").strip()
    if not normalized:
        return None
    if len(normalized) > 128 or any(ord(character) < 32 for character in normalized):
        raise ValueError(f"{source} contains an invalid Excel number format")
    return normalized


@dataclass
class ModelFormatter:
    """Configurable formatting rules for rendered financial models.

    Plan §3 row S7: the *identity* of a style guide and the benchmark it was
    calibrated against are authored data (``schema/style_guides/``), not
    mechanism defaults. A bare formatter therefore states no style guide; one
    built by :meth:`from_style_guide` carries the artifact's identity, its
    provenance, and whatever rule rows that artifact overrides.
    """

    style_guide_id: str | None = None
    style_guide_source_reference: str | None = None

    dollar_format: str = "#,##0"
    percentage_format: str = "0.0%"
    per_share_format: str = "#,##0.00"
    share_count_format: str = "#,##0.0"
    count_format: str = "#,##0"
    days_format: str = "0.0"
    date_format: str = "mmm d, yyyy"
    ratio_format: str = "0.0x"

    constant_color: str = "#0000FF"
    formula_color: str = "#000000"
    header_color: str = "#000000"
    key_driver_color: str = "#008000"
    input_color: str = "#0000FF"
    section_header_fill_color: str | None = "#FFFF00"
    section_header_fill_sheets: tuple[str, ...] | None = ("Assumptions",)
    section_header_border_color: str | None = "#000000"
    section_header_border_sheets: tuple[str, ...] | None = (
        "Assumptions",
        "Financial_model",
    )
    input_fill_color: str | None = "#FFFF00"
    input_fill_sheets: tuple[str, ...] | None = (
        "Assumptions",
        "Valuation",
        "Scenarios",
    )
    total_separator_border_color: str | None = "#000000"
    total_separator_border_sheets: tuple[str, ...] | None = (
        "Assumptions",
        "Financial_model",
    )
    underline_before_totals: bool = False
    summary_sheet_enabled: bool = True
    summary_sheet_name: str = "Summary"
    summary_max_key_drivers: int = 8
    summary_missing_value: str = "Not available"

    def number_format_for(self, item: LineItem) -> Optional[str]:
        if item.item_type in {ItemType.header, ItemType.spacer}:
            return None

        explicit_format = _validated_number_format(
            item.format,
            source=f"{item.id}.format",
        )
        if explicit_format is not None:
            return explicit_format

        # A declared semantic unit is authoritative. Only an actually absent
        # unit may continue to the style and identifier/label fallback tiers.
        if item.unit is not None:
            if item.unit is Unit.dollars:
                return self.dollar_format
            if item.unit is Unit.percentage:
                return self.percentage_format
            if item.unit in {Unit.ratio, Unit.multiple}:
                return self.ratio_format
            if item.unit in {Unit.per_share, Unit.per_count}:
                return self.per_share_format
            if item.unit is Unit.per_day:
                return f'{self.per_share_format}"/day"'
            if item.unit is Unit.days:
                return self.days_format
            if item.unit is Unit.date:
                return self.date_format
            if item.unit is Unit.share_count:
                return self.share_count_format
            if item.unit is Unit.item_count:
                return self.count_format
            raise ValueError(f"{item.id}.unit is unsupported: {item.unit!r}")
        if item.style is not None:
            style_format = _validated_number_format(
                item.style.number_format or "",
                source=f"{item.id}.style.number_format",
            )
            if style_format is not None:
                return style_format

        item_id = item.id.lower()
        label = item.label.lower()

        if any(keyword in item_id for keyword in _PERCENTAGE_KEYWORDS):
            return self.percentage_format
        if _label_implies_percentage(label):
            return self.percentage_format
        if any(item_id.endswith(suffix) for suffix in _SCENARIO_PERCENTAGE_SUFFIXES):
            return self.percentage_format
        if any(keyword in item_id for keyword in _RATIO_KEYWORDS):
            return self.ratio_format
        if (
            "eps" in item_id or "per_share" in item_id
        ) and "share_repurchase" not in item_id:
            return self.per_share_format
        if any(keyword in item_id for keyword in _SHARE_COUNT_KEYWORDS):
            return self.share_count_format
        return self.dollar_format

    def label_color_for(self, item: LineItem) -> Optional[str]:
        """Labels are always black — blue/black convention applies to data cells only."""
        return self.header_color

    def data_color_for(self, is_formula: bool, item: LineItem) -> str:
        if item.style is not None and item.style.color is CellColor.key_driver:
            return self.key_driver_color
        return self.formula_color if is_formula else self.constant_color

    def is_total_row(self, item_id: str) -> bool:
        lowered = item_id.lower()
        return any(lowered.endswith(suffix) for suffix in _TOTAL_ROW_SUFFIXES)

    @classmethod
    def from_style_guide(cls, guide: StyleGuide) -> "ModelFormatter":
        """Build the formatter one authored style guide describes.

        A rule the artifact does not override keeps the mechanism's own default,
        so an artifact states only what makes its style different. A rule name
        the formatter does not carry is refused rather than ignored: silently
        dropping an authored row would render in a style nobody authored.
        """

        known = {field.name for field in dataclass_fields(cls)}
        unknown = sorted(set(guide.rules) - known)
        if unknown:
            raise StyleGuideLibraryInvalid(
                artifact=guide.style_guide_id,
                field=f"rules.{unknown[0]}",
                detail="no such formatting rule on the model formatter",
            )
        for reserved in ("style_guide_id", "style_guide_source_reference"):
            if reserved in guide.rules:
                raise StyleGuideLibraryInvalid(
                    artifact=guide.style_guide_id,
                    field=f"rules.{reserved}",
                    detail=(
                        "identity and provenance are the artifact's own fields, "
                        "not rule overrides"
                    ),
                )
        return cls(
            style_guide_id=guide.style_guide_id,
            style_guide_source_reference=guide.source_reference,
            **dict(guide.rules),
        )


#: The house style the library marks default — the formatter every render
#: resolves when no model kind declares a style guide of its own (plan §5.1:
#: absence is feature-off, and this *is* the arm the mechanism took before the
#: style rows were authored data).
SIA_FORMATTER = ModelFormatter.from_style_guide(library_default_style_guide())


def formatter_for_declared_style_guide(
    style_guide_id: str | None,
) -> ModelFormatter:
    """The formatter a model kind's declared style guide resolves to (row S7).

    ``None`` is the legal absent state — the model kind declares no style of its
    own and renders in the library default. A declared id that names no authored
    artifact is ``style_guide_unresolved`` (plan §6), never a silent fall back:
    the declaration says the presentation differs.
    """

    if style_guide_id is None:
        return SIA_FORMATTER
    return ModelFormatter.from_style_guide(load_style_guide(style_guide_id))


__all__ = [
    "ModelFormatter",
    "SIA_FORMATTER",
    "formatter_for_declared_style_guide",
]
