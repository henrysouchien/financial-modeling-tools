from __future__ import annotations

from typing import Any, Protocol

from industry_slice_core.industry_types import Unit

DriverSpec = dict[str, Any]


class _WorkingCapitalLike(Protocol):
    @property
    def driver_keys(self) -> dict[str, str] | None: ...

    @property
    def model_as(self) -> str: ...


class _DebtLike(Protocol):
    @property
    def level(self) -> str: ...


class _CapitalSourcesLike(Protocol):
    @property
    def debt(self) -> _DebtLike | None: ...


def _working_capital_driver_specs(working_capital: _WorkingCapitalLike) -> list[DriverSpec]:
    requested = {
        _working_capital_driver_kind(key, value)
        for key, value in (working_capital.driver_keys or {}).items()
    }
    requested.discard(None)
    if not requested or working_capital.model_as == "percent_of_revenue":
        requested = {"dso", "dpo"}

    specs: list[DriverSpec] = []
    if "dso" in requested:
        specs.append(
            {
                "driver_node_id": "days_sales_outstanding_dso",
                "label": "Days Sales Outstanding",
                "unit": Unit.days,
                "factors": ["reinvestment"],
                "existing_driver_key": "dso",
                "aliases": [
                    "dso",
                    "dso_days",
                    "days_sales_outstanding",
                    "days_sales_outstanding_dso",
                    "tpl.a.balance_sheet_wc.days_sales_outstanding_dso",
                ],
            }
        )
    if "dpo" in requested:
        specs.append(
            {
                "driver_node_id": "days_payable_outstanding_dpo",
                "label": "Days Payable Outstanding",
                "unit": Unit.days,
                "factors": ["reinvestment"],
                "existing_driver_key": "dpo",
                "aliases": [
                    "dpo",
                    "dpo_days",
                    "days_payable_outstanding",
                    "days_payable_outstanding_dpo",
                    "tpl.a.balance_sheet_wc.days_payable_outstanding_dpo",
                ],
            }
        )
    return specs


def _capital_source_driver_specs(capital_sources: _CapitalSourcesLike) -> list[DriverSpec]:
    specs: list[DriverSpec] = []
    if capital_sources.debt is not None and capital_sources.debt.level != "none":
        specs.append(
            {
                "driver_node_id": "debt_change",
                "label": "Debt Change",
                "unit": Unit.dollars,
                "factors": ["capital_sources"],
                "existing_driver_key": "debt_change",
                "aliases": [
                    "debt_change",
                    "net_debt_change",
                    "capital_sources.debt_change",
                    "tpl.a.capital_sources.change",
                ],
            }
        )
    return specs


def _working_capital_driver_kind(key: str, value: str) -> str | None:
    text = f"{key} {value}".lower()
    if "dso" in text or "sales_outstanding" in text or "sales outstanding" in text:
        return "dso"
    if "dpo" in text or "payable" in text:
        return "dpo"
    return None
