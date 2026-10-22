"""Code-owned forecast-control profile keys and their exact identity pins.

The BusinessModel owns natural operating judgments. This module owns the
enumeration of the small template/compiler spine that cannot be inferred from
the BusinessModel driver plan: gross margin, stock compensation, tax, working
capital, capitalized-software roll-forward inputs, and the projection-year debt
and acquisition schedule inputs the projected balance-sheet, cash and debt
roll-forwards read when the BusinessModel supplies no judgment for them. It also
admits optional per-issuer policy controls (share repurchases, employee-stock-
plan proceeds, dividends paid, non-GAAP other items, the prepaid, current
operating lease liability and depreciation revenue ratios, stock-based
compensation and its four per-line rates) the author may leave to the
compiler's house default.

Stock-based compensation has two forecast methods, read from the authored rows
(the per-line precedent: disclosed SBC per line → % of each line): ``per_line``
— a per-line control's row is that line's rate — and ``aggregate_scale`` —
``sbc_pct`` lowers onto every line the per-line rows leave open, and where the
issuer discloses the line split the compile scales it by the disclosed mix
(``disclosed_mix_fanout``).

The profile enumerates controls and their bindings only; it writes no values.
Each control's compiler target row is resolved through ``driver_mapping.yaml``,
the registered key-to-row map, rather than restated here. The profile is
content-addressed: its pin (``ForecastControlProfilePin``/``ForecastControlProfileRef``)
is persisted on forecast artifacts. Applicability resolution, composition with
BusinessModel controls and compiler lowering live in
``api/research/forecast_control_profile.py``.
"""

from __future__ import annotations

import functools
import hashlib
from collections.abc import Mapping
from typing import TYPE_CHECKING, Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from industry_slice_core.industry_types import Unit

from .model_state_identity import validate_sha256_hex
from .versioned_artifact import canonical_json_bytes

if TYPE_CHECKING:
    from .models import FinancialModel


ForecastControlCategory = Literal[
    "revenue",
    "unit_economics",
    "cost_structure",
    "reinvestment",
    "capital_sources",
    "other",
]
# ``required`` controls must be answered by an accepted ForecastAssumptionSet;
# an ``optional_policy`` control may be left to the compiler's house default.
ForecastControlAuthoring = Literal["required", "optional_policy"]


class _Contract(BaseModel):
    model_config = ConfigDict(
        extra="forbid",
        frozen=True,
        strict=True,
        str_strip_whitespace=True,
    )


class ForecastControlProfileRef(_Contract):
    """Semantic identity of one immutable forecast-control policy profile."""

    schema_version: Literal["forecast-control-profile-ref.v1"] = (
        "forecast-control-profile-ref.v1"
    )
    profile_id: str = Field(min_length=1, max_length=256)
    declared_version: str = Field(min_length=1, max_length=128)
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _content_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")


class ForecastControlProfilePin(_Contract):
    """Code-owned policy pin before it is embedded as an artifact reference."""

    schema_version: Literal["forecast-control-profile-pin.v1"] = (
        "forecast-control-profile-pin.v1"
    )
    profile_id: str = Field(min_length=1, max_length=256)
    declared_version: str = Field(min_length=1, max_length=128)
    content_sha256: str

    @field_validator("content_sha256")
    @classmethod
    def _content_digest(cls, value: str) -> str:
        return validate_sha256_hex(value, field_name="content_sha256")

    @property
    def ref(self) -> ForecastControlProfileRef:
        return ForecastControlProfileRef(
            profile_id=self.profile_id,
            declared_version=self.declared_version,
            content_sha256=self.content_sha256,
        )


def forecast_control_profile_ref(
    value: ForecastControlProfileRef | ForecastControlProfilePin,
) -> ForecastControlProfileRef:
    """Canonicalize a policy pin to the artifact-chain reference shape."""

    if isinstance(value, ForecastControlProfilePin):
        return value.ref
    return ForecastControlProfileRef.model_validate(value)


class CompilerForecastTargetBinding(_Contract):
    """One hidden canonical compiler selector bound to one writable model row."""

    projection_driver_key: str = Field(min_length=1, max_length=256)
    target_item_id: str = Field(min_length=1, max_length=512)

    @field_validator("projection_driver_key", "target_item_id")
    @classmethod
    def _canonical(cls, value: str) -> str:
        if value != value.strip() or any(character.isspace() for character in value):
            raise ValueError("compiler target bindings must be canonical")
        return value


class CompilerForecastControlSpec(_Contract):
    """One immutable logical control and its hidden compiler lowering."""

    control_id: str = Field(min_length=1, max_length=256)
    driver_key: str = Field(min_length=1, max_length=256)
    label: str = Field(min_length=1, max_length=512)
    value_basis: str = Field(min_length=1, max_length=1000)
    category: ForecastControlCategory
    unit: Unit
    value_semantics: Literal["direct_control_value"] = "direct_control_value"
    percentage_value_space: Literal["fraction"] | None
    applicability_rule_id: Literal[
        "gross_margin_spine.v1",
        "stock_compensation.v1",
        "taxable_income.v1",
        "capitalized_software.v1",
        "capitalized_software_amortization.v1",
        "deferred_revenue.v1",
        "business_model_complement.v1",
        "acquisition_program.v1",
        "share_repurchase.v1",
        "employee_stock_plan.v1",
        "dividend_policy.v1",
        "non_gaap_bridge.v1",
        "prepaid_expenses.v1",
        "current_operating_lease.v1",
        "depreciation_expense.v1",
        "debt_schedule.v1",
        "deferred_revenue_noncurrent.v1",
        "investment_purchases.v1",
        "investment_sales.v1",
        "other_investing.v1",
        "exchange_rate_effect.v1",
        "restructuring_charges.v1",
        "noncontrolling_interest.v1",
    ]
    authoring: ForecastControlAuthoring = Field(
        default="required",
        exclude_if=lambda value: value == "required",
    )
    lowering_strategy: Literal["direct_target", "disclosed_mix_fanout"]
    target_bindings: tuple[CompilerForecastTargetBinding, ...] = Field(
        min_length=1,
        max_length=8,
    )
    # V7 (issuer-generic plan §3): this contract carried a `legacy_aliases`
    # tuple that named model-kind-specific legacy selectors. Those aliases are
    # now declared by the model kinds that still author them
    # (`ScenarioControlDeclaration.legacy_control_aliases`), and the field is
    # deleted in the same change so the alias plane has exactly one writer.

    @field_validator("control_id", "driver_key")
    @classmethod
    def _key(cls, value: str) -> str:
        if value != value.strip() or any(character.isspace() for character in value):
            raise ValueError("profile control keys must be canonical")
        return value

    @model_validator(mode="after")
    def _lowering_shape(self) -> "CompilerForecastControlSpec":
        expected_space = "fraction" if self.unit is Unit.percentage else None
        if self.percentage_value_space != expected_space:
            raise ValueError("percentage controls require canonical fraction space")
        if self.lowering_strategy == "disclosed_mix_fanout" and (
            self.unit is not Unit.percentage
        ):
            raise ValueError("disclosed-mix fanout applies only to percentage rates")
        projection_keys = tuple(row.projection_driver_key for row in self.target_bindings)
        if projection_keys != tuple(sorted(set(projection_keys))):
            raise ValueError("projection driver keys must be sorted and unique")
        target_item_ids = tuple(row.target_item_id for row in self.target_bindings)
        if len(target_item_ids) != len(set(target_item_ids)):
            raise ValueError("control target item IDs must be unique")
        if self.lowering_strategy == "direct_target" and len(self.target_bindings) != 1:
            raise ValueError("direct-target control requires exactly one target")
        if self.lowering_strategy == "disclosed_mix_fanout" and len(
            self.target_bindings
        ) < 2:
            raise ValueError("disclosed-mix fanout requires multiple targets")
        return self

    @property
    def target_item_ids(self) -> tuple[str, ...]:
        return tuple(row.target_item_id for row in self.target_bindings)


class CompilerForecastControlProfile(_Contract):
    """One code-owned, content-addressed compiler policy profile."""

    schema_version: Literal["compiler-forecast-control-profile.v1"] = (
        "compiler-forecast-control-profile.v1"
    )
    profile_id: str = Field(min_length=1, max_length=256)
    declared_version: str = Field(min_length=1, max_length=128)
    controls: tuple[CompilerForecastControlSpec, ...] = Field(min_length=1)
    content_sha256: str

    @model_validator(mode="after")
    def _closed(self) -> "CompilerForecastControlProfile":
        control_ids = tuple(row.control_id for row in self.controls)
        driver_keys = tuple(row.driver_key for row in self.controls)
        if control_ids != tuple(sorted(set(control_ids))):
            raise ValueError("profile control IDs must be sorted and unique")
        if driver_keys != tuple(sorted(set(driver_keys))):
            raise ValueError("profile driver keys must be sorted and unique")
        projection_keys = [
            binding.projection_driver_key
            for row in self.controls
            for binding in row.target_bindings
        ]
        if len(projection_keys) != len(set(projection_keys)):
            raise ValueError("profile projection driver keys must be globally unique")
        # A row has one writer per lowering: a direct control's row may also be
        # a disclosed-mix fanout target, which the direct row displaces.
        for strategy in ("direct_target", "disclosed_mix_fanout"):
            targets = [
                target
                for row in self.controls
                if row.lowering_strategy == strategy
                for target in row.target_item_ids
            ]
            if len(targets) != len(set(targets)):
                raise ValueError("profile target item IDs must be unique per lowering")
        if self.content_sha256 != forecast_control_profile_content_sha256(self):
            raise ValueError("forecast-control profile content digest mismatch")
        return self

    @property
    def pin(self) -> ForecastControlProfilePin:
        return ForecastControlProfilePin(
            profile_id=self.profile_id,
            declared_version=self.declared_version,
            content_sha256=self.content_sha256,
        )


def forecast_control_profile_content_sha256(
    value: CompilerForecastControlProfile | Mapping[str, Any],
) -> str:
    payload = (
        value.model_dump(mode="json") if isinstance(value, BaseModel) else dict(value)
    )
    payload.pop("content_sha256", None)
    return hashlib.sha256(
        b"forecast-control-profile.v1\0" + canonical_json_bytes(payload)
    ).hexdigest()


def _registered_binding(
    projection_driver_key: str, driver_mapping: Mapping[str, str] | None,
) -> CompilerForecastTargetBinding:
    """Bind a projection key to the row ``driver_mapping.yaml`` registers for it."""

    # Function-local: the resolver loads the template, which artifact stores
    # importing the pin types must not pay for.
    from .driver_resolver import resolve_driver_key

    return CompilerForecastTargetBinding(
        projection_driver_key=projection_driver_key,
        target_item_id=(
            resolve_driver_key(projection_driver_key)
            if driver_mapping is None else driver_mapping[projection_driver_key]
        ),
    )


def _control(
    driver_key: str,
    *,
    label: str,
    value_basis: str,
    category: ForecastControlCategory,
    unit: Unit,
    applicability_rule_id: str,
    fanout_keys: tuple[str, ...] = (),
    projection_key: str | None = None,
    authoring: ForecastControlAuthoring = "required",
    driver_mapping: Mapping[str, str] | None = None,
) -> CompilerForecastControlSpec:
    projection_keys = fanout_keys or (projection_key or driver_key,)
    return CompilerForecastControlSpec.model_validate(
        {
            "control_id": driver_key,
            "driver_key": driver_key,
            "label": label,
            "value_basis": value_basis,
            "category": category,
            "unit": unit,
            "percentage_value_space": (
                "fraction" if unit is Unit.percentage else None
            ),
            "applicability_rule_id": applicability_rule_id,
            "authoring": authoring,
            "lowering_strategy": (
                "disclosed_mix_fanout" if fanout_keys else "direct_target"
            ),
            "target_bindings": tuple(
                _registered_binding(key, driver_mapping) for key in sorted(projection_keys)
            ),
        }
    )


@functools.cache
def sia_generic_nonvaluation_forecast_profile() -> CompilerForecastControlProfile:
    """The SIA generic non-valuation profile, built once on first use."""
    return _build_nonvaluation_forecast_profile()


def _build_nonvaluation_forecast_profile(
    template: FinancialModel | None = None,
) -> CompilerForecastControlProfile:
    from .driver_resolver import registered_driver_mapping

    control = functools.partial(
        _control,
        driver_mapping=None if template is None else registered_driver_mapping(),
    )

    controls = (
        control("acquisition_goodwill_allocation",
        label="Acquisition Cost Allocated to Goodwill",
        value_basis=(
            "Fraction of negated cash paid for acquisitions of businesses "
            "allocated to goodwill. One is the standing proposal: goodwill "
            "receives the whole purchase price and acquired intangibles "
            "receive the remainder, which is zero until this split is "
            "authored. The judgment value belongs to the forecast "
            "assumption set."
        ),
        category="reinvestment",
        unit=Unit.percentage,
        applicability_rule_id="acquisition_program.v1",
        authoring="optional_policy",),
        control("acquisitions_of_businesses",
        label="Acquisitions of Businesses",
        value_basis=(
            "Cash paid for acquisitions of businesses, net of cash acquired, "
            "in the cash-flow statement's sign: cash paid is negative and "
            "zero means no acquisition in the period."
        ),
        category="reinvestment",
        unit=Unit.dollars,
        applicability_rule_id="acquisition_program.v1",),
        control("capitalized_software_amortization_rate",
        label="Capitalized Software Amortization Rate",
        value_basis=(
            "Amortization expense as a fraction of beginning capitalized "
            "internal-use software balance. Required unless the filer's "
            "latest historical year cites both the software balance and "
            "its amortization as included (not separately presented) in "
            "property and equipment or total D&A: the model then builds an "
            "inclusive depreciation row with no standalone software "
            "roll-forward. A presented balance whose amortization is cited "
            "inside total D&A keeps this rate; the model projects that "
            "amortization from the balance."
        ),
        category="reinvestment",
        unit=Unit.percentage,
        applicability_rule_id="capitalized_software_amortization.v1",),
        control("capitalized_software_pct",
        label="Capitalized Software Additions % of Revenue",
        value_basis=(
            "Capitalized internal-use software additions as a fraction of "
            "same-period revenue."
        ),
        category="reinvestment",
        unit=Unit.percentage,
        applicability_rule_id="capitalized_software.v1",),
        control("current_operating_lease_liabilities_pct_revenue",
        label="Current Operating Lease Liabilities % of Revenue",
        value_basis=(
            "Current operating lease liabilities at period end as a fraction "
            "of same-period revenue. An unanswered row compiles the house "
            "value, the latest reported ratio."
        ),
        category="capital_sources",
        unit=Unit.percentage,
        applicability_rule_id="current_operating_lease.v1",
        authoring="optional_policy",),
        control("debt_change",
        label="Net Change in Debt",
        value_basis=(
            "Net borrowings less repayments of debt in the period; positive "
            "adds debt, negative repays it, and zero holds the balance."
        ),
        category="capital_sources",
        unit=Unit.dollars,
        applicability_rule_id="business_model_complement.v1",),
        control("debt_interest_rate",
        label="Interest Rate on Funded Debt",
        value_basis=(
            "Annual interest rate on average funded debt as a fraction "
            "(0.05 is 5%). An unanswered row compiles the house value, the "
            "issuer's latest observed effective rate (interest expense over "
            "average debt) carried forward; an issuer with no funded-debt "
            "history has no observed rate, so the row stays an unresolved "
            "input until this control is authored."
        ),
        category="capital_sources",
        unit=Unit.percentage,
        applicability_rule_id="debt_schedule.v1",
        authoring="optional_policy",),
        control("deferred_revenue_noncurrent",
        label="Noncurrent Deferred Revenue",
        value_basis=(
            "Noncurrent deferred revenue balance at period end, in dollars, "
            "as a positive liability. An unanswered row compiles the house "
            "value, the latest reported balance carried forward."
        ),
        category="capital_sources",
        unit=Unit.dollars,
        applicability_rule_id="deferred_revenue_noncurrent.v1",
        authoring="optional_policy",),
        control("deferred_revenue_ratio",
        label="Deferred Revenue as a Percent of Revenue",
        value_basis=(
            "Filing-native deferred revenue from the disclosure schedule as "
            "a fraction of same-period revenue."
        ),
        category="capital_sources",
        unit=Unit.percentage,
        applicability_rule_id="deferred_revenue.v1",),
        control("depreciation_pct_revenue",
        label="Depreciation % of Revenue",
        value_basis=(
            "Depreciation as a fraction of same-period revenue: the "
            "depreciation row the model builds for the filer. It includes "
            "capitalized-software amortization only where the filer's "
            "latest historical year cites both the software balance and "
            "its amortization as included (not separately presented) in "
            "property and equipment or total D&A; otherwise it is physical "
            "depreciation only, excluding the software amortization the "
            "model projects from the software balance. "
            "Acquired-intangible amortization is always excluded. An "
            "unanswered row compiles the house value, the latest reported "
            "ratio on that same definition."
        ),
        category="reinvestment",
        unit=Unit.percentage,
        applicability_rule_id="depreciation_expense.v1",
        authoring="optional_policy",),
        control("dividends_paid",
        label="Dividends Paid",
        value_basis=(
            "Common dividends paid in the period as a positive amount; the "
            "cash-flow statement negates it. Stated dividend policy is the "
            "judgment; an unanswered row compiles the house value."
        ),
        category="capital_sources",
        unit=Unit.dollars,
        applicability_rule_id="dividend_policy.v1",
        authoring="optional_policy",),
        control("effect_of_exchange_rate_on_cash",
        label="Effect of Exchange Rate on Cash",
        value_basis=(
            "Effect of exchange-rate changes on cash in the period, in "
            "dollars, in the cash-flow statement's sign: a gain is positive "
            "and a loss negative. An unanswered row compiles zero with derived "
            "lineage wherever the latest reported effect is a known finite "
            "amount (terminal_zero_fx_policy)."
        ),
        category="other",
        unit=Unit.dollars,
        applicability_rule_id="exchange_rate_effect.v1",
        authoring="optional_policy",),
        control("employee_stock_plan_proceeds",
        label="Employee Stock Plan Proceeds",
        value_basis=(
            "Cash received from employee stock purchase and option plans in "
            "the period as a positive inflow. An unanswered row compiles the "
            "house value."
        ),
        category="capital_sources",
        unit=Unit.dollars,
        applicability_rule_id="employee_stock_plan.v1",
        authoring="optional_policy",),
        control("gross_margin",
        label="Gross Margin",
        value_basis=(
            "Gross profit as a fraction of same-period revenue for the base case."
        ),
        category="unit_economics",
        unit=Unit.percentage,
        applicability_rule_id="gross_margin_spine.v1",),
        control("nci_share_of_consolidated_net_income",
        label="Noncontrolling Interest Share of Consolidated Net Income",
        value_basis=(
            "Net income attributable to noncontrolling interests as a "
            "fraction of consolidated net income (pretax income less the "
            "income-tax provision; 0.01 is 1%). Positive when noncontrolling "
            "interests share in income. An unanswered row compiles the house "
            "value, the latest reported share; an issuer with no NCI line has "
            "no row."
        ),
        category="other",
        unit=Unit.percentage,
        applicability_rule_id="noncontrolling_interest.v1",
        authoring="optional_policy",),
        control("non_gaap_other_items",
        label="Non-GAAP Other Items",
        value_basis=(
            "Issuer-defined other items added back to Adjusted EBITDA in the "
            "period as a positive add-back. An unanswered row compiles the "
            "house value."
        ),
        category="other",
        unit=Unit.dollars,
        applicability_rule_id="non_gaap_bridge.v1",
        authoring="optional_policy",),
        control("other_changes_in_investing_activities",
        label="Other Investing Activities",
        value_basis=(
            "Other investing cash flows in the period beyond capital "
            "expenditures, acquisitions and investment purchases and "
            "maturities, in the cash-flow statement's sign: cash paid is "
            "negative and cash received positive. A client-fund issuer's "
            "funds-held asset change compiles into this row unless authored."
        ),
        category="reinvestment",
        unit=Unit.dollars,
        applicability_rule_id="other_investing.v1",
        authoring="optional_policy",),
        control("prepaid_pct_revenue",
        label="Prepaid Expenses and Other % of Revenue",
        value_basis=(
            "Prepaid expenses and other current assets at period end as a "
            "fraction of same-period revenue. An unanswered row compiles the "
            "house value, the latest reported ratio."
        ),
        category="reinvestment",
        unit=Unit.percentage,
        applicability_rule_id="prepaid_expenses.v1",
        authoring="optional_policy",),
        control("purchases_of_investments",
        label="Purchases of Investments",
        value_basis=(
            "Cash paid for purchases of investments in the period, in the "
            "cash-flow statement's sign: cash paid is negative and zero "
            "means no purchases. An issuer whose client-fund assets move "
            "through the model's net pass-through bridge compiles zero."
        ),
        category="reinvestment",
        unit=Unit.dollars,
        applicability_rule_id="investment_purchases.v1",
        authoring="optional_policy",),
        control("restructuring_charges",
        label="Restructuring Charges",
        value_basis=(
            "Restructuring charges in the period, in dollars, as a positive "
            "operating expense (the line's own sign on the income "
            "statement); zero means no restructuring in the period. There is "
            "no house value: an issuer that reports the line has no basis "
            "for extrapolating a one-time charge, so the row stays an "
            "unresolved input until this control is authored."
        ),
        category="cost_structure",
        unit=Unit.dollars,
        applicability_rule_id="restructuring_charges.v1",
        authoring="optional_policy",),
        control("sales_maturities_of_investments",
        label="Sales and Maturities of Investments",
        value_basis=(
            "Cash received from sales and maturities of investments in the "
            "period, in the cash-flow statement's sign: cash received is "
            "positive and zero means none. An issuer whose client-fund "
            "assets move through the model's net pass-through bridge "
            "compiles zero."
        ),
        category="reinvestment",
        unit=Unit.dollars,
        applicability_rule_id="investment_sales.v1",
        authoring="optional_policy",),
        control("sbc_pct",
        label="Stock-Based Compensation % of Eligible Operating Costs",
        value_basis=(
            "Total stock-based compensation as a fraction of aggregate eligible "
            "COGS, sales and marketing, research and development, and general "
            "and administrative expense (the aggregate_scale method). Where the "
            "issuer discloses SBC by line, it scales the disclosed mix: each "
            "line's rate is its latest disclosed rate times this value over the "
            "latest disclosed aggregate rate; undisclosed, it applies uniformly "
            "to the four line rates. A line whose per-line row is authored "
            "(the per_line method) takes that row instead. An unanswered row "
            "compiles the house value: the latest disclosed line rates, or the "
            "aggregate at its latest revenue rate where the split is undisclosed."
        ),
        category="capital_sources",
        unit=Unit.percentage,
        applicability_rule_id="stock_compensation.v1",
        fanout_keys=(
            "sbc_mix_cost_of_revenue_pct",
            "sbc_mix_ga_pct",
            "sbc_mix_rd_pct",
            "sbc_mix_sales_marketing_pct",
        ),
        authoring="optional_policy",),
        *(
            control(f"sbc_pct_{line}",
            label=f"Stock-Based Compensation % of {label}",
            value_basis=(
                f"Stock-based compensation in {label.lower()} as a fraction of "
                f"same-period {label.lower()} expense (the per_line method, "
                "applicable where the issuer discloses SBC by line). An "
                "authored row is this line's judgment and sbc_pct is unread "
                "for it. An unanswered row compiles the house value, the "
                "line's latest disclosed rate, or sbc_pct scaled by the "
                "disclosed mix when sbc_pct is authored."
            ),
            category="capital_sources",
            unit=Unit.percentage,
            applicability_rule_id="stock_compensation.v1",
            projection_key=projection_key,
            authoring="optional_policy",)
            for line, label, projection_key in (
                ("cost_of_revenues", "Cost of Revenues", "sbc_cost_of_revenue_pct"),
                (
                    "general_and_administrative",
                    "General and Administrative",
                    "sbc_ga_pct",
                ),
                (
                    "research_and_development",
                    "Research and Development",
                    "sbc_rd_pct",
                ),
                ("sales_and_marketing", "Sales and Marketing", "sbc_sales_marketing_pct"),
            )
        ),
        control("share_repurchases",
        label="Share Repurchases",
        value_basis=(
            "Share repurchase cash in the period beyond the buyback that offsets "
            "stock-based-compensation dilution (the model carries that offset "
            "separately), as a positive amount; the cash-flow statement negates "
            "it. Stated repurchase policy is the judgment; an unanswered row "
            "compiles the house value."
        ),
        category="capital_sources",
        unit=Unit.dollars,
        applicability_rule_id="share_repurchase.v1",
        authoring="optional_policy",),
        control("tax_rate",
        label="Effective Tax Rate",
        value_basis=(
            "Income-tax expense or benefit as a fraction of same-period "
            "income before income taxes."
        ),
        category="other",
        unit=Unit.percentage,
        applicability_rule_id="taxable_income.v1",),
    )
    payload = {
        "schema_version": "compiler-forecast-control-profile.v1",
        "profile_id": "sia-generic.nonvaluation-forecast-controls",
        # 1.12 corrects the exchange-rate-effect value basis: since the
        # terminal_zero_fx_policy landed, any known finite reported terminal
        # effect compiles to zero with derived lineage, not only a zero one.
        # 1.11 makes the latest-year inclusion predicate explicit in the
        # depreciation and software-amortization value bases: depreciation
        # includes software amortization only where the terminal year cites
        # both the software balance and its amortization as included. Only
        # then is the standalone software-amortization rate inapplicable.
        # Artifacts accepted under 1.11 or earlier retain their stored pins;
        # the typed re-accept door carries predecessor judgments onto this
        # profile. The optional acquisition-goodwill split admitted in 1.8
        # remains unchanged: unauthored, the bridge proposal allocates the
        # whole negated acquisition cash cost to goodwill.
        "declared_version": "1.12",
        "controls": controls,
    }
    return CompilerForecastControlProfile.model_validate(
        {**payload, "content_sha256": forecast_control_profile_content_sha256(payload)}
    )


SbcForecastMethod = Literal["per_line", "aggregate_scale"]


@functools.cache
def disclosed_mix_methods_by_projection_key() -> Mapping[str, tuple[str, SbcForecastMethod]]:
    """Each compiler key on a disclosed-mix row, its control and its method.

    A ``disclosed_mix_fanout`` control's keys are ``aggregate_scale``; the
    direct control bound to one of its rows is that line's ``per_line`` key.
    The compile reads the method from the key that wrote the row.
    """

    controls = sia_generic_nonvaluation_forecast_profile().controls
    mix_rows = {
        target
        for row in controls
        if row.lowering_strategy == "disclosed_mix_fanout"
        for target in row.target_item_ids
    }
    methods: dict[str, tuple[str, SbcForecastMethod]] = {}
    for row in controls:
        for binding in row.target_bindings:
            if row.lowering_strategy == "disclosed_mix_fanout":
                methods[binding.projection_driver_key] = (row.control_id, "aggregate_scale")
            elif binding.target_item_id in mix_rows:
                methods[binding.projection_driver_key] = (row.control_id, "per_line")
    return methods


def forecast_control_ids_by_target_item(
    *, template: FinancialModel | None = None,
) -> Mapping[str, tuple[str, ...]]:
    """Each template row a profile control lowers onto, and the controls that do.

    A Finding about an unresolved row names these so the author knows which
    control to answer; the rows come from the profile's registered bindings, not
    a second list. A supplied construction template scopes those bindings to its
    rows without loading or structurally validating a global template.
    """

    by_item: dict[str, list[str]] = {}
    profile = (
        sia_generic_nonvaluation_forecast_profile()
        if template is None else _build_nonvaluation_forecast_profile(template)
    )
    if template is not None and not template._index:
        template.build_index()
    for control in profile.controls:
        for item_id in control.target_item_ids:
            if template is not None and item_id not in template._index:
                continue
            by_item.setdefault(item_id, []).append(control.control_id)
    return {item_id: tuple(ids) for item_id, ids in by_item.items()}


__all__ = [
    "CompilerForecastControlProfile",
    "CompilerForecastControlSpec",
    "CompilerForecastTargetBinding",
    "ForecastControlAuthoring",
    "ForecastControlCategory",
    "ForecastControlProfilePin",
    "ForecastControlProfileRef",
    "SbcForecastMethod",
    "disclosed_mix_methods_by_projection_key",
    "forecast_control_ids_by_target_item",
    "forecast_control_profile_content_sha256",
    "forecast_control_profile_ref",
    "sia_generic_nonvaluation_forecast_profile",
]
