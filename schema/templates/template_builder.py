"""Build the checked-in generic SIA template artifact from checked-in inputs.

The build's source model is a checked-in capture of the parsed source
workbook (``template_source_inputs``), so regeneration runs on any machine
and the workbook path survives only as provenance inside that artifact.
The reference-artifact oracle has no production consumer and lives beside its
tests in ``tests/reference_template_oracle.py``. Source-input ownership is in
``schema/templates/source_inputs/README.md``; the engine map is
``schema/README.md``.
"""

from __future__ import annotations

import argparse
from pathlib import Path
import re
from typing import Callable, Dict, List, Mapping, Optional, overload, Sequence, Set

from ..models import (
    BuildStatus,
    CellColor,
    CellStyle,
    CompanyInfo,
    CustomizationType,
    FinancialStatement,
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
    ModelMetadata,
    Section,
    SheetLayout,
    SheetType,
    StatementSectionRole,
    StatementSectionRoleKind,
)
from ..refs import line_item_ref_from_obj
from industry_slice_core.industry_types import Unit
from ..build_diagnostic_sections import BS_SECTIONS
from ..required_role_catalog import load_required_role_catalog

from .template_builder_config import (
    SIA_GENERIC_TEMPLATE_PATH as SIA_GENERIC_TEMPLATE_PATH,
    KEPT_SHEETS as KEPT_SHEETS,
    EXPECTED_ITEM_COUNT_GENERIC as EXPECTED_ITEM_COUNT_GENERIC,
    EXPECTED_SECTION_COUNTS as EXPECTED_SECTION_COUNTS,
    EXPECTED_SHEET_ITEM_COUNTS_GENERIC as EXPECTED_SHEET_ITEM_COUNTS_GENERIC,
    TemplateRole as TemplateRole,
    SectionSpec as SectionSpec,
    TemplateMetadataSpec as TemplateMetadataSpec,
    PlaceholderInserter as PlaceholderInserter,
    TemplateBuildConfig as TemplateBuildConfig,
    ASSUMPTIONS_SECTIONS_GENERIC as ASSUMPTIONS_SECTIONS_GENERIC,
    FINANCIAL_MODEL_SECTIONS_GENERIC as FINANCIAL_MODEL_SECTIONS_GENERIC,
    SECTION_SPECS_GENERIC as SECTION_SPECS_GENERIC,
    SHEET_PREFIX as SHEET_PREFIX,
    CANONICAL_NAME_OVERRIDES_GENERIC as CANONICAL_NAME_OVERRIDES_GENERIC,
    CASH_FLOW_ARTIFACT_HISTORICAL_IDS as CASH_FLOW_ARTIFACT_HISTORICAL_IDS,
    IF_APPLICABLE_DEFAULT_ZERO_IDS as IF_APPLICABLE_DEFAULT_ZERO_IDS,
    CASH_FLOW_FULL_PROJECTION_IDS as CASH_FLOW_FULL_PROJECTION_IDS,
    CASH_FLOW_PER_SHARE_PROJECTION_IDS as CASH_FLOW_PER_SHARE_PROJECTION_IDS,
    CASH_FLOW_NET_INCOME_SOURCE_ID as CASH_FLOW_NET_INCOME_SOURCE_ID,
    EXPECTED_HEADER_DEPENDENCY_TARGETS_GENERIC as EXPECTED_HEADER_DEPENDENCY_TARGETS_GENERIC,
    SCENARIO_LINKED_IDS_GENERIC as SCENARIO_LINKED_IDS_GENERIC,
    _SCENARIO_SELECTOR_ID as _SCENARIO_SELECTOR_ID,
    _COLUMN_OFFSET_MODE_PERIOD_RELATIVE as _COLUMN_OFFSET_MODE_PERIOD_RELATIVE,
    _SCENARIO_LINKED_OFFSET_ANCHORS as _SCENARIO_LINKED_OFFSET_ANCHORS,
    SCENARIO_TABLE_IDS_GENERIC as SCENARIO_TABLE_IDS_GENERIC,
    INPUT_IDS_GENERIC as INPUT_IDS_GENERIC,
    PROJECTED_ZERO_DEFAULT_TEMPLATE_IDS_GENERIC as PROJECTED_ZERO_DEFAULT_TEMPLATE_IDS_GENERIC,
    KEY_DRIVER_IDS_GENERIC as KEY_DRIVER_IDS_GENERIC,
    OPTIONAL_IDS_GENERIC as OPTIONAL_IDS_GENERIC,
    TEMPLATE_TOKENS_GENERIC as TEMPLATE_TOKENS_GENERIC,
    BUILD_NOTES_GENERIC as BUILD_NOTES_GENERIC,
    LABEL_OVERRIDES_GENERIC as LABEL_OVERRIDES_GENERIC,
    HEADER_DEPENDENCY_NOTES_GENERIC as HEADER_DEPENDENCY_NOTES_GENERIC,
    REPEAT_GROUP_ROLES_GENERIC as REPEAT_GROUP_ROLES_GENERIC,
    REPEAT_GROUP_IDS_GENERIC as REPEAT_GROUP_IDS_GENERIC,
)

from .template_source_inputs import (
    TemplateRegenerationInputMissing as TemplateRegenerationInputMissing,
    load_template_source_model as load_template_source_model,
    source_input_path as source_input_path,
)

from .template_builder_insertions import (
    _iter_items as _iter_items,
    _iter_formula_specs as _iter_formula_specs,
    _extract_refs as _extract_refs,
    _apply_scenario_linked_offset_params as _apply_scenario_linked_offset_params,
    _find_sheet_item as _find_sheet_item,
    _find_section_with_item as _find_section_with_item,
    _apply_label_overrides as _apply_label_overrides,
    _normalize_generic_accounting_baseline as _normalize_generic_accounting_baseline,
    _insert_sum_ref_after as _insert_sum_ref_after,
    _insert_other_non_current_assets_row as _insert_other_non_current_assets_row,
    _insert_other_current_liabilities_row as _insert_other_current_liabilities_row,
    _insert_commercial_paper_row as _insert_commercial_paper_row,
    _insert_deferred_revenue_noncurrent_row as _insert_deferred_revenue_noncurrent_row,
    _insert_investment_activity_rows as _insert_investment_activity_rows,
    _insert_house_revenue_ratio_rows as _insert_house_revenue_ratio_rows,
    _insert_acquisition_goodwill_allocation_row as _insert_acquisition_goodwill_allocation_row,
    _insert_treasury_stock_face_row as _insert_treasury_stock_face_row,
    _insert_balance_sheet_face_rows as _insert_balance_sheet_face_rows,
    _insert_short_term_investments_row as _insert_short_term_investments_row,
    _insert_per_share_face_rows as _insert_per_share_face_rows,
    _insert_operating_depreciation_amortization_row as _insert_operating_depreciation_amortization_row,
    _insert_selling_general_administrative_row as _insert_selling_general_administrative_row,
    _insert_net_income_attributable_to_nci_rows as _insert_net_income_attributable_to_nci_rows,
    _insert_restructuring_charges_row as _insert_restructuring_charges_row,
    _install_sga_driver_base as _install_sga_driver_base,
    _insert_long_term_asset_3_rows as _insert_long_term_asset_3_rows,
    _add_share_based_compensation_to_apic_roll_forward as _add_share_based_compensation_to_apic_roll_forward,
    _generate_statement_totals as _generate_statement_totals,
    _install_optional_slot_carry_forward as _install_optional_slot_carry_forward,
    _install_current_debt_bridge as _install_current_debt_bridge,
    _install_share_count_schedule as _install_share_count_schedule,
    _install_debt_interest_identities as _install_debt_interest_identities,
    _install_historical_schedule_identities as _install_historical_schedule_identities,
    _install_financing_residual_defaults as _install_financing_residual_defaults,
    _install_non_gaap_other_items_default as _install_non_gaap_other_items_default,
    _install_non_gaap_tax_effect_identity as _install_non_gaap_tax_effect_identity,
    _install_projected_zero_defaults as _install_projected_zero_defaults,
    _generate_cash_flow_movements as _generate_cash_flow_movements,
    _insert_unlevered_free_cash_flow_row as _insert_unlevered_free_cash_flow_row,
    _clear_cash_flow_artifact_historicals as _clear_cash_flow_artifact_historicals,
    _set_if_applicable_default_zero_historicals as _set_if_applicable_default_zero_historicals,
    _normalize_cash_flow_projection_links as _normalize_cash_flow_projection_links,
    _normalize_valuation_projection_links as _normalize_valuation_projection_links,
    _normalize_sbc_per_line_rate_historicals as _normalize_sbc_per_line_rate_historicals,
    _normalize_generic_cash_reconciliation as _normalize_generic_cash_reconciliation,
)
from .template_builder_leases import (
    install_operating_lease_schedule as install_operating_lease_schedule,
)
from .template_builder_schedules import _install_canonical_schedules
from .model_scale import (
    apply_template_model_scales as apply_template_model_scales,
    apply_template_semantic_units as apply_template_semantic_units,
)
from .model_scale import assert_template_model_scales_declared as assert_template_model_scales_declared









def _section_for_row(
    sheet_name: str,
    row: int,
    *,
    section_specs: Dict[str, Sequence[SectionSpec]],
) -> SectionSpec:
    for spec in section_specs[sheet_name]:
        if spec.row_start <= row <= spec.row_end:
            return spec
    raise ValueError(f"No section for {sheet_name} row {row}")


def _clean_source_name(item_id: str) -> str:
    base = item_id.split(".", 1)[1]
    return re.sub(r"_r\d+$", "", base)


def _build_tpl_mapping(
    model: FinancialModel,
    *,
    section_specs: Dict[str, Sequence[SectionSpec]],
    name_overrides: Dict[str, str],
    expected_item_count: int,
) -> Dict[str, str]:
    id_map: Dict[str, str] = {}
    for sheet_name, item in _iter_items(model):
        section = _section_for_row(sheet_name, item.row, section_specs=section_specs)
        canonical_name = name_overrides.get(item.id, _clean_source_name(item.id))
        id_map[item.id] = f"{SHEET_PREFIX[sheet_name]}.{section.id}.{canonical_name}"

    if len(id_map) != expected_item_count:
        raise AssertionError(f"Expected {expected_item_count} mapped items, got {len(id_map)}")
    if len(set(id_map.values())) != len(id_map):
        raise AssertionError("Template ID mapping produced duplicates")
    return id_map


def _rewrite_refs(obj, id_map: Dict[str, str]):
    if obj is None:
        return None
    if isinstance(obj, LineItemRef):
        if obj.id in id_map:
            return LineItemRef(id=id_map[obj.id], t=obj.t, resolved=True)
        return LineItemRef(id=obj.id, t=obj.t, resolved=False)
    if isinstance(obj, dict):
        return {key: _rewrite_refs(value, id_map) for key, value in obj.items()}
    if isinstance(obj, list):
        return [_rewrite_refs(value, id_map) for value in obj]
    if isinstance(obj, tuple):
        return tuple(_rewrite_refs(value, id_map) for value in obj)
    if isinstance(obj, set):
        return {_rewrite_refs(value, id_map) for value in obj}
    return obj


@overload
def _rewrite_formula_spec(spec: None, id_map: Dict[str, str]) -> None: ...


@overload
def _rewrite_formula_spec(
    spec: FormulaSpec, id_map: Dict[str, str]
) -> FormulaSpec: ...


def _rewrite_formula_spec(spec: Optional[FormulaSpec], id_map: Dict[str, str]) -> Optional[FormulaSpec]:
    if spec is None:
        return None
    return spec.model_copy(update={"params": _rewrite_refs(spec.params, id_map)})


def _discover_header_dependency_targets(model: FinancialModel) -> Set[str]:
    item_by_id = {item.id: item for _, item in _iter_items(model)}
    result: Set[str] = set()
    for _, item in _iter_items(model):
        for spec in _iter_formula_specs(item):
            for ref in _extract_refs(spec.params):
                target = item_by_id.get(ref.id)
                if target and target.item_type == ItemType.header:
                    result.add(ref.id)
    return result


def _has_cross_sheet_ref(item: LineItem) -> bool:
    source_sheet = item.id.split(".", 1)[0]
    for spec in _iter_formula_specs(item):
        for ref in _extract_refs(spec.params):
            if ref.id.split(".", 1)[0] != source_sheet:
                return True
    return False


def _customization_for_item(
    item_id: str,
    *,
    optional_ids: Set[str],
    expected_header_dependency_targets: Set[str],
    template_tokens: Dict[str, str],
    repeat_group_ids: Dict[str, str],
) -> CustomizationType:
    customization = CustomizationType.fixed
    if item_id in optional_ids or item_id in expected_header_dependency_targets:
        customization = CustomizationType.optional
    if item_id in template_tokens and item_id not in repeat_group_ids:
        customization = CustomizationType.rename
    if item_id in repeat_group_ids:
        customization = CustomizationType.repeatable
    return customization


def _style_for_item(item_id: str, role: TemplateRole, *, key_driver_ids: Set[str]) -> CellStyle:
    if role == TemplateRole.header:
        return CellStyle(color=CellColor.header, bold=True)
    if item_id in key_driver_ids:
        return CellStyle(color=CellColor.key_driver)
    if role == TemplateRole.input:
        return CellStyle(color=CellColor.input_blue)
    return CellStyle(color=CellColor.formula_black)


def _merge_notes(*notes: Optional[str]) -> Optional[str]:
    parts = [note.strip() for note in notes if note and note.strip()]
    if not parts:
        return None
    return " ".join(parts)


def _standing_sum_items(spec: FormulaSpec | None) -> list[LineItemRef] | None:
    """Same-period members of a standing historical ``SUM`` range, or nothing.

    Only a ``sum_range`` whose function is ``SUM`` is a subtotal identity this
    installer can close. A period override is a filed value and is not read.
    """

    if (
        spec is None
        or spec.type != FormulaType.arithmetic
        or spec.subtype != "sum_range"
        or (spec.params or {}).get("function") != "SUM"
    ):
        return None
    raw_items = (spec.params or {}).get("items")
    if not isinstance(raw_items, list):
        return None
    items: list[LineItemRef] = []
    for raw in raw_items:
        ref = raw if isinstance(raw, LineItemRef) else line_item_ref_from_obj(raw)
        if ref is not None and ref.t == 0:
            items.append(ref)
    return items


def _formula_params_reference(value: object, target_id: str) -> bool:
    """Whether a formula param tree names ``target_id``.

    Walks ``LineItemRef`` objects and the dict and list forms a spec can
    still carry before rehydration. A ref-shaped dict is one id, not a walk
    into its ``id`` string.
    """

    ref = line_item_ref_from_obj(value)
    if ref is not None:
        return ref.id == target_id
    if isinstance(value, dict):
        return any(
            _formula_params_reference(nested, target_id) for nested in value.values()
        )
    if isinstance(value, (list, tuple)):
        return any(_formula_params_reference(nested, target_id) for nested in value)
    return False


def _period_spec_references(item: LineItem, period: int, target_id: str) -> bool:
    """Whether the effective historical spec at ``period`` names ``target_id``.

    The period override wins over the standing historical formula. A constant
    is a filed value and names nothing; a missing spec names nothing.
    """

    spec = (item.overrides or {}).get(period) or item.historical
    if spec is None or spec.type == FormulaType.constant:
        return False
    return _formula_params_reference(spec.params, target_id)


def _install_cash_and_short_term_investments_history(model: FinancialModel) -> None:
    """The cash and short-term investments line sums the positions it covers.

    The line is derived from the lines that present cash and short-term
    investments separately (the catalog's ``covered_by_roles``), so its history
    is the ``SUM`` over their template items in every historical period.
    """

    catalog = load_required_role_catalog()
    role = "statement.bs.cash_and_short_term_investments"
    items = catalog.role(role).template_items
    covered = [
        member
        for row in catalog.required_roles
        if role in row.covered_by_roles
        for member in row.template_items
    ]
    if len(items) != 1 or not covered:
        raise AssertionError(f"{role} has no single template line over covered positions")
    item = model.get_item(items[0])
    item.historical = FormulaSpec(
        type=FormulaType.arithmetic,
        subtype="sum_range",
        params={
            "function": "SUM",
            "items": [LineItemRef(id=member) for member in covered],
        },
    )
    historical = (
        model.time_structure.historical_periods
        or model.time_structure.historical_years
    )
    item.formula_periods = sorted(
        {*(int(period) for period in (item.formula_periods or ())), *(int(period) for period in historical)}
    )


def _install_residual_histories(model: FinancialModel) -> None:
    """Close each cash-flow residual line's history against its filed total.

    A catalog role carrying ``residual_of`` is the plug of a statement total
    that the filing prints: the residual's value is whatever the filed total
    leaves after its other members. Its history is ``total - (the other
    members of the total's standing historical SUM)``, read from the model as
    it stands, so operands a cash-flow linkage inserted into the sum are
    subtracted too. A static template formula cannot say this: the total's own
    ``SUM`` names the residual.

    Only a plug continues as a plug: a residual whose projection is a modeled
    formula keeps no history close, because its history would be a different
    quantity than its projection. The financing residual's total is a
    ``sum_list`` and sums an ``optional_if_unreported`` line the cash-flow plane
    does not lower, so its identity is not closed here either.

    A year whose effective total or member spec already names the residual is
    not a history year: the filed subtotal owns that value, and closing the
    identity there would cycle. A constant override or stored value remains an
    input; a per-year formula, including a lowered residual, remains a formula
    period. No supported year leaves the line unset. This installer owns the
    line's history years, so a year that lost its subtotal support leaves them.
    """

    catalog = load_required_role_catalog()
    historical = list(
        model.time_structure.historical_periods
        or model.time_structure.historical_years
    )
    model.build_index()
    for row in catalog.required_roles:
        if row.residual_of is None or len(row.template_items) != 1:
            continue
        total_items = catalog.role(row.residual_of).template_items
        if len(total_items) != 1:
            continue
        residual_id, total_id = row.template_items[0], total_items[0]
        if residual_id not in model._index or total_id not in model._index:
            continue
        item = model.get_item(residual_id)
        if item.projected is not None and item.projected.type != FormulaType.constant:
            # A modeled projection (the operating residual's balance-sheet deltas)
            # is a different quantity than the plug a history close would write.
            continue
        total = model.get_item(total_id)
        members = _standing_sum_items(total.historical)
        if members is None or not any(ref.id == residual_id for ref in members):
            continue
        others = [
            LineItemRef(id=ref.id) for ref in members if ref.id != residual_id
        ]
        if not others or any(ref.id not in model._index for ref in others):
            continue
        note = f"residual_of:{row.residual_of}"
        if item.historical is not None and item.historical.note != note:
            continue
        existing_periods = {
            int(period)
            for period, spec in (item.overrides or {}).items()
            if spec.type is FormulaType.constant
        } | {
            int(period)
            for period in (item.values.values if item.values is not None else {})
        }
        operands = [total, *(model.get_item(ref.id) for ref in others)]
        supported_historical = [
            int(period)
            for period in historical
            if int(period) not in existing_periods
            and not any(
                _period_spec_references(operand, int(period), residual_id)
                for operand in operands
            )
        ]
        if not supported_historical and item.historical is None:
            continue
        item.historical = FormulaSpec(
            type=FormulaType.arithmetic,
            params={
                "expr": {
                    "op": "-",
                    "left": LineItemRef(id=total_id),
                    "right": others[0]
                    if len(others) == 1
                    else {"op": "+", "args": others},
                }
            },
            note=note,
        )
        historical_set = {int(period) for period in historical}
        current = (
            {int(period) for period in item.formula_periods}
            if item.formula_periods is not None
            else historical_set
            | {
                int(period)
                for period in (
                    model.time_structure.projection_periods
                    or model.time_structure.projection_years
                    or ()
                )
            }
        )
        owned = (current - historical_set) | set(supported_historical)
        if item.formula_periods is not None or owned != current:
            item.formula_periods = sorted(owned)


def _stamp_scenario_offset_labels(model: FinancialModel) -> None:
    """Offset anchors are labels, not value cells.

    The renderer reads the rows below the anchor. A blank input on the anchor
    itself stamps ``missing_source`` in every period. They are not presentation
    captions and are not required to stay inputs: segment expansion only
    renames the volume label.
    """

    if not model._index:
        model.build_index()
    for item_id in (
        "tpl.a.scenario_tables.scenario_volume_growth_label",
        "tpl.a.scenario_tables.scenario_margin_label",
    ):
        item = model._index.get(item_id)
        if item is not None:
            item.item_type = ItemType.header


def _required_role_by_template_item() -> dict[str, str]:
    # An alias row shares its home's template items; the stamp names the
    # canonical home role, never the alias.
    return {
        item_id: row.required_role
        for row in load_required_role_catalog().required_roles
        if row.alias_of is None
        for item_id in row.template_items
    }


def _apply_required_role_stamps(model: FinancialModel) -> None:
    required_role_by_item = _required_role_by_template_item()
    for _sheet_name, item in _iter_items(model):
        item.required_role = required_role_by_item.get(item.id)


def _assign_metadata(
    model: FinancialModel,
    *,
    section_specs: Dict[str, Sequence[SectionSpec]],
    expected_header_dependency_targets: Set[str],
    input_ids: Set[str],
    scenario_linked_ids: Set[str],
    optional_ids: Set[str],
    template_tokens: Dict[str, str],
    build_notes: Dict[str, str],
    header_dependency_notes: Dict[str, str],
    repeat_group_roles: Dict[str, str],
    template_item_id_by_build_item: Mapping[str, str],
    key_driver_ids: Set[str],
) -> Set[str]:
    promoted_headers = _discover_header_dependency_targets(model)
    if promoted_headers != expected_header_dependency_targets:
        raise AssertionError(
            f"Header dependency targets mismatch: expected {sorted(expected_header_dependency_targets)}, "
            f"got {sorted(promoted_headers)}"
        )

    repeat_group_ids = {item_id: "revenue_segment" for item_id in repeat_group_roles}
    required_role_by_template_item = _required_role_by_template_item()
    for sheet_name, item in _iter_items(model):
        section = _section_for_row(sheet_name, item.row, section_specs=section_specs)
        if item.id in promoted_headers or item.id in input_ids:
            role = TemplateRole.input
        elif item.id in scenario_linked_ids:
            role = TemplateRole.scenario_linked
        elif item.item_type == ItemType.header:
            role = TemplateRole.header
        elif _has_cross_sheet_ref(item):
            role = TemplateRole.reference
        else:
            role = TemplateRole.derived

        if role == TemplateRole.input:
            item.item_type = ItemType.input
        elif role == TemplateRole.header:
            item.item_type = ItemType.header
        else:
            item.item_type = ItemType.derived

        item.driver_category = section.driver_category
        item.customization = _customization_for_item(
            item.id,
            optional_ids=optional_ids,
            expected_header_dependency_targets=expected_header_dependency_targets,
            template_tokens=template_tokens,
            repeat_group_ids=repeat_group_ids,
        )
        item.style = _style_for_item(item.id, role, key_driver_ids=key_driver_ids)
        item.required_role = required_role_by_template_item.get(
            template_item_id_by_build_item[item.id]
        )
        item.template_token = template_tokens.get(item.id)
        item.repeat_group_id = repeat_group_ids.get(item.id)
        item.repeat_group_role = repeat_group_roles.get(item.id)
        item.build_notes = _merge_notes(
            build_notes.get(item.id),
            header_dependency_notes.get(item.id) if item.id in promoted_headers else None,
        )

    return promoted_headers


def _apply_balance_sheet_section_roles(model: FinancialModel) -> None:
    item_by_id = {item.id: item for _, item in _iter_items(model)}
    roles_by_item_id: dict[str, list[StatementSectionRole]] = {}

    def add_role(
        item_id: str,
        *,
        section: str,
        role: StatementSectionRoleKind,
    ) -> None:
        if item_id not in item_by_id:
            raise AssertionError(f"BS section role references unknown item: {item_id}")
        roles_by_item_id.setdefault(item_id, []).append(
            StatementSectionRole(
                statement=FinancialStatement.balance_sheet,
                section=section,
                role=role,
            )
        )

    role_stamped_items = set(_required_role_by_template_item())
    for section, definition in BS_SECTIONS.items():
        if definition.get("presentation_only"):
            continue
        for member in definition["sub_lines"]:
            add_role(
                member.template_item_id,
                section=section,
                role="member"
                if member.template_item_id in role_stamped_items
                else "catch_all",
            )
        add_role(
            definition["total_item_id"],
            section=section,
            role="section_total",
        )
        included_subtotal_item_id = definition.get("also_includes_subtotal")
        if included_subtotal_item_id:
            add_role(
                included_subtotal_item_id,
                section=section,
                role="included_subtotal",
            )

    for item in item_by_id.values():
        item.statement_section_roles = roles_by_item_id.get(item.id)


def _filter_overrides(
    item_id: str,
    overrides: Optional[Dict[int, FormulaSpec]],
    historical_periods: Set[int],
    *,
    scenario_table_ids: Set[str],
    strip_scenario_table_constant_overrides: bool = False,
) -> Optional[Dict[int, FormulaSpec]]:
    if not overrides:
        return None
    if item_id in scenario_table_ids:
        result = {
            period: spec
            for period, spec in overrides.items()
            if not (
                strip_scenario_table_constant_overrides
                and spec.type == FormulaType.constant
            )
        }
        return result or None
    result: Dict[int, FormulaSpec] = {}
    for period, spec in overrides.items():
        if period in historical_periods:
            continue
        if spec.type == FormulaType.constant:
            continue
        result[period] = spec
    return result or None




def _extend_formula_periods_for_stripped_constants(
    item: LineItem,
    *,
    scenario_table_ids: Set[str],
    strip_scenario_table_constant_overrides: bool,
) -> None:
    if (
        not strip_scenario_table_constant_overrides
        or item.id not in scenario_table_ids
        or not item.overrides
    ):
        return

    stripped_periods = {
        int(period)
        for period, spec in item.overrides.items()
        if spec.type == FormulaType.constant
    }
    if not stripped_periods:
        return

    item.formula_periods = sorted(
        {int(period) for period in (item.formula_periods or [])} | stripped_periods
    )




































def _split_sections(
    sheet_name: str,
    items: Sequence[LineItem],
    *,
    section_specs: Dict[str, Sequence[SectionSpec]],
    expected_sheet_item_counts: Dict[str, int],
    expected_section_counts: Dict[str, int],
) -> List[Section]:
    sections: List[Section] = []
    assigned: Set[str] = set()
    for spec in section_specs[sheet_name]:
        line_items = [
            item
            for item in items
            if spec.row_start <= item.row <= spec.row_end
        ]
        for item in line_items:
            assigned.add(item.id)
        sections.append(
            Section(
                id=spec.id,
                label=spec.label,
                line_items=sorted(line_items, key=lambda item: item.row),
                driver_category=spec.driver_category,
            )
        )

    if len(assigned) != len(items):
        missing = sorted(item.id for item in items if item.id not in assigned)
        raise AssertionError(f"Unassigned {sheet_name} items: {missing}")
    if len(sections) != expected_section_counts[sheet_name]:
        raise AssertionError(f"Unexpected section count for {sheet_name}: {len(sections)}")
    if sum(len(section.line_items) for section in sections) != expected_sheet_item_counts[sheet_name]:
        raise AssertionError(f"Unexpected item count in split sections for {sheet_name}")
    return sections


def _validate_template(
    model: FinancialModel,
    *,
    expected_section_counts: Dict[str, int],
    expected_sheet_item_counts: Dict[str, int],
) -> None:
    model.build_index()
    FinancialModel.model_validate_json(model.model_dump_json())
    for sheet_name in KEPT_SHEETS:
        sheet = model.sheets[sheet_name]
        if len(sheet.sections) != expected_section_counts[sheet_name]:
            raise AssertionError(f"Unexpected section count for {sheet_name}")
        if any(section.id == "main" for section in sheet.sections):
            raise AssertionError(f"{sheet_name} still contains a main section")
        total = sum(len(section.line_items) for section in sheet.sections)
        if total != expected_sheet_item_counts[sheet_name]:
            raise AssertionError(f"Unexpected item count for {sheet_name}: {total}")


GENERIC_TEMPLATE_CONFIG = TemplateBuildConfig(
    name="generic",
    source_input=source_input_path("sia_generic"),
    source_input_ref="sia_generic",
    artifact_path=SIA_GENERIC_TEMPLATE_PATH,
    section_specs=SECTION_SPECS_GENERIC,
    name_overrides=CANONICAL_NAME_OVERRIDES_GENERIC,
    expected_item_count=EXPECTED_ITEM_COUNT_GENERIC,
    expected_sheet_item_counts=EXPECTED_SHEET_ITEM_COUNTS_GENERIC,
    metadata=TemplateMetadataSpec(
        source_model="sia_generic",
        notes="Generic SIA 2-sheet template derived from Model_template.xlsx",
        company_name="SIA Generic Template",
    ),
    expected_header_dependency_targets=EXPECTED_HEADER_DEPENDENCY_TARGETS_GENERIC,
    scenario_linked_ids=SCENARIO_LINKED_IDS_GENERIC,
    scenario_table_ids=SCENARIO_TABLE_IDS_GENERIC,
    input_ids=INPUT_IDS_GENERIC,
    projected_zero_default_ids=PROJECTED_ZERO_DEFAULT_TEMPLATE_IDS_GENERIC,
    key_driver_ids=KEY_DRIVER_IDS_GENERIC,
    optional_ids=OPTIONAL_IDS_GENERIC,
    template_tokens=TEMPLATE_TOKENS_GENERIC,
    build_notes=BUILD_NOTES_GENERIC,
    header_dependency_notes=HEADER_DEPENDENCY_NOTES_GENERIC,
    repeat_group_roles=REPEAT_GROUP_ROLES_GENERIC,
    label_overrides=LABEL_OVERRIDES_GENERIC,
    placeholder_inserters=(
        _normalize_generic_accounting_baseline,
        _insert_other_current_liabilities_row,
        _insert_commercial_paper_row,
        _insert_deferred_revenue_noncurrent_row,
        _insert_investment_activity_rows,
        _insert_unlevered_free_cash_flow_row,
        _normalize_generic_cash_reconciliation,
    ),
    clear_cash_flow_artifact_historicals=True,
    set_if_applicable_default_zero_historicals=True,
    # Section roles are stamped at the end of ``build_sia_generic_template``,
    # after the face rows it inserts exist; stamping here would miss them.
    apply_statement_section_roles=False,
    strip_scenario_table_constant_overrides=True,
    normalize_cash_flow_projection_links=True,
    normalize_valuation_projection_links=True,
)

TEMPLATE_CONFIGS: Dict[str, TemplateBuildConfig] = {
    "generic": GENERIC_TEMPLATE_CONFIG,
}


def _load_source_model(config: TemplateBuildConfig) -> FinancialModel:
    """The parsed source workbook, from the target's checked-in capture.

    An absent capture is ``template_regeneration_input_missing`` (plan §6),
    never a re-parse of whatever workbook happens to sit on this machine —
    that arm was deleted at IG-6 with the path constants that named it.
    """

    return load_template_source_model(
        config.source_input, input_ref=config.source_input_ref
    )


def _build_template_from_config(
    config: TemplateBuildConfig,
    *,
    model: Optional[FinancialModel] = None,
) -> FinancialModel:
    source_model = _load_source_model(config) if model is None else model

    template = source_model.model_copy(deep=True)
    template.sheets = {name: template.sheets[name] for name in KEPT_SHEETS}
    for inserter in config.placeholder_inserters:
        inserter(template)
    _apply_label_overrides(template, config.label_overrides)

    id_map = _build_tpl_mapping(
        template,
        section_specs=config.section_specs,
        name_overrides=config.name_overrides,
        expected_item_count=config.expected_item_count,
    )
    _assign_metadata(
        template,
        section_specs=config.section_specs,
        expected_header_dependency_targets=config.expected_header_dependency_targets,
        input_ids=config.input_ids,
        scenario_linked_ids=config.scenario_linked_ids,
        optional_ids=config.optional_ids,
        template_tokens=config.template_tokens,
        build_notes=config.build_notes,
        header_dependency_notes=config.header_dependency_notes,
        repeat_group_roles=config.repeat_group_roles,
        template_item_id_by_build_item=id_map,
        key_driver_ids=config.key_driver_ids,
    )
    if config.clear_cash_flow_artifact_historicals:
        _clear_cash_flow_artifact_historicals(template)
    if config.set_if_applicable_default_zero_historicals:
        _set_if_applicable_default_zero_historicals(template)

    historical_periods = set(
        template.time_structure.historical_periods or template.time_structure.historical_years
    )
    for _, item in _iter_items(template):
        _extend_formula_periods_for_stripped_constants(
            item,
            scenario_table_ids=config.scenario_table_ids,
            strip_scenario_table_constant_overrides=config.strip_scenario_table_constant_overrides,
        )
        item.overrides = _filter_overrides(
            item.id,
            item.overrides,
            historical_periods,
            scenario_table_ids=config.scenario_table_ids,
            strip_scenario_table_constant_overrides=config.strip_scenario_table_constant_overrides,
        )
        if item.id == "assumptions.interest_rate":
            item.historical = None
            if item.overrides:
                item.overrides = {
                    period: spec
                    for period, spec in item.overrides.items()
                    if spec.type != FormulaType.raw
                } or None

    for _, item in _iter_items(template):
        item.id = id_map[item.id]
        item.historical = _rewrite_formula_spec(item.historical, id_map)
        item.projected = _rewrite_formula_spec(item.projected, id_map)
        if item.overrides:
            item.overrides = {
                period: _rewrite_formula_spec(spec, id_map)
                for period, spec in item.overrides.items()
            }

    apply_template_semantic_units(template)
    _apply_scenario_linked_offset_params(template)
    if config.normalize_cash_flow_projection_links:
        _normalize_cash_flow_projection_links(template)
    if config.normalize_valuation_projection_links:
        _normalize_valuation_projection_links(template)
    _normalize_sbc_per_line_rate_historicals(template)
    if config.apply_statement_section_roles:
        _apply_balance_sheet_section_roles(template)
        apply_template_model_scales(template)
        assert_template_model_scales_declared(template)

    for sheet_name in KEPT_SHEETS:
        sheet = template.sheets[sheet_name]
        items = [item for section in sheet.sections for item in section.line_items]
        sheet.sections = _split_sections(
            sheet_name,
            items,
            section_specs=config.section_specs,
            expected_sheet_item_counts=config.expected_sheet_item_counts,
            expected_section_counts=EXPECTED_SECTION_COUNTS,
        )

    for _, item in _iter_items(template):
        item.values = None

    template.sheets["Assumptions"].sheet_type = SheetType.assumptions
    template.sheets["Assumptions"].description = (
        "Driver sheet containing all inputs and assumptions that feed Financial_model"
    )
    template.sheets["Assumptions"].layout = SheetLayout(
        label_column="A",
        first_data_column="D",
        column_width_label=40.0,
        column_width_data=14.0,
        header_rows=4,
        freeze_panes="D5",
    )

    template.sheets["Financial_model"].sheet_type = SheetType.financial_model
    template.sheets["Financial_model"].description = (
        "Three-statement model: income statement, balance sheet, cash flow, with margins, growth rates, and ratios"
    )
    template.sheets["Financial_model"].layout = SheetLayout(
        label_column="A",
        first_data_column="D",
        column_width_label=36.0,
        column_width_data=14.0,
        header_rows=3,
        freeze_panes="D4",
    )

    template.metadata = ModelMetadata(
        is_template=True,
        methodology="sia",
        source_model=config.metadata.source_model,
        build_status=BuildStatus.template,
        notes=config.metadata.notes,
    )
    template.company = CompanyInfo(
        ticker=config.metadata.company_ticker,
        name=config.metadata.company_name,
        fiscal_year_end=None,
    )

    _validate_template(
        template,
        expected_section_counts=EXPECTED_SECTION_COUNTS,
        expected_sheet_item_counts=config.expected_sheet_item_counts,
    )
    return template


def load_sia_generic_template() -> FinancialModel:
    """Load the checked-in generic SIA template artifact."""

    model = FinancialModel.model_validate_json(
        SIA_GENERIC_TEMPLATE_PATH.read_text(encoding="utf-8")
    )
    apply_template_semantic_units(model)
    _apply_required_role_stamps(model)
    _apply_balance_sheet_section_roles(model)
    _install_residual_histories(model)
    _stamp_scenario_offset_labels(model)
    return model


def build_sia_generic_template(
    config: TemplateBuildConfig = GENERIC_TEMPLATE_CONFIG,
    *,
    model: Optional[FinancialModel] = None,
) -> FinancialModel:
    """Transform the generic source workbook into the checked-in generic artifact.

    The config is a parameter rather than a closed-over constant so that the
    registry row and the builder are related by *the config the writer was
    handed*, not by convention: starving ``config.source_input`` refuses here
    (``template_regeneration_input_missing``) exactly as it does in the loader.
    ``model`` stays a keyword-only override for callers that already hold a
    parsed source model — it never reaches outside the repository either.
    """

    template = _build_template_from_config(config, model=model)
    template.build_index()

    other_income = template.get_item(
        "tpl.a.other_income_interest.other_income_expense"
    )
    other_income.item_type = ItemType.input
    deferred_revenue = template.get_item(
        "tpl.a.balance_sheet_wc.deferred_revenues"
    )
    deferred_revenue.model_scale = "millions"
    deferred_revenue.item_type = ItemType.input
    deferred_revenue_rate = template.get_item("tpl.a.balance_sheet_wc.of_rev")
    deferred_revenue_rate.item_type = ItemType.input
    deferred_revenue_rate.unit = Unit.percentage
    # Forecast-assumption policy targets (``employee_stock_plan_proceeds``,
    # ``non_gaap_other_items``): the driver resolver admits only input rows.
    for policy_target_id in (
        "tpl.a.dividends_shares.proceeds_from_employee_stock_purchase_plan",
        "tpl.a.adj_ebitda.other_items",
    ):
        template.get_item(policy_target_id).item_type = ItemType.input

    capital_sources = next(
        section
        for section in template.sheets["Assumptions"].sections
        if section.id == "capital_sources"
    )
    current_lease = LineItem.model_validate(
        {
            "id": "tpl.a.capital_sources.current_operating_lease_liabilities",
            "label": " Current operating lease liabilities",
            "row": 226,
            "item_type": "input",
            "unit": "dollars",
            "model_scale": "millions",
            "format": "",
            "required_role": load_required_role_catalog()
            .role_for_template_item(
                "tpl.a.capital_sources.current_operating_lease_liabilities"
            )
            .required_role,
            "style": {"color": "input_blue"},
            "driver_category": "capital_sources",
            "customization": "optional",
            "build_notes": (
                "Current operating lease liability input populated from the "
                "pinned filing line for its required role when reported."
            ),
        }
    )
    insert_at = next(
        index
        for index, item in enumerate(capital_sources.line_items)
        if item.id
        == "tpl.a.capital_sources.long_term_operating_lease_liabilities"
    )
    capital_sources.line_items.insert(insert_at, current_lease)
    _insert_house_revenue_ratio_rows(template)
    _insert_acquisition_goodwill_allocation_row(template)
    _insert_treasury_stock_face_row(template)
    _insert_balance_sheet_face_rows(template)
    _insert_short_term_investments_row(template)
    _insert_per_share_face_rows(template)
    _insert_operating_depreciation_amortization_row(template)
    _insert_selling_general_administrative_row(template)
    _insert_net_income_attributable_to_nci_rows(template)
    _insert_restructuring_charges_row(template)
    _insert_long_term_asset_3_rows(
        template,
        template_tokens=config.template_tokens,
        build_notes=config.build_notes,
    )
    _install_sga_driver_base(template)
    # Every row a total or cash-flow aggregate reads now exists: the catalog's
    # membership writes both lanes of each.
    _generate_statement_totals(template)
    _install_optional_slot_carry_forward(template)
    _install_current_debt_bridge(template)
    _install_share_count_schedule(template)
    _install_historical_schedule_identities(template)
    _install_debt_interest_identities(template)
    _install_canonical_schedules(template)
    _install_financing_residual_defaults(template)
    _install_non_gaap_other_items_default(template)
    _install_non_gaap_tax_effect_identity(template)
    _generate_cash_flow_movements(template)
    install_operating_lease_schedule(template)
    _install_projected_zero_defaults(template, config.projected_zero_default_ids)
    _add_share_based_compensation_to_apic_roll_forward(template)
    _install_cash_and_short_term_investments_history(template)
    _apply_required_role_stamps(template)
    _install_residual_histories(template)
    _apply_balance_sheet_section_roles(template)
    apply_template_model_scales(template)
    assert_template_model_scales_declared(template)

    cash_end = template.get_item(
        "tpl.fm.cash_flow.cash_and_cash_equivalents_end_of_period"
    )
    cash_end.historical = None
    template.build_index()
    return template


#: The build each registered target runs. Declared beside the target rather
#: than dispatched on its name, so a new target is a registry row and a
#: builder, never a branch (plan §5.1's dispatch rule applied to this lane).
#: Each builder takes the *config the writer was handed*, so a row paired with
#: the wrong builder cannot silently write another target's bytes to this
#: target's path.
TEMPLATE_BUILDERS: Dict[str, Callable[[TemplateBuildConfig], FinancialModel]] = {
    "generic": build_sia_generic_template,
}


def _write_template_artifact(config: TemplateBuildConfig) -> Path:
    builder = TEMPLATE_BUILDERS.get(config.name)
    if builder is None:
        raise ValueError(f"Unknown template target: {config.name}")
    template = builder(config)
    config.artifact_path.write_text(
        template.model_dump_json(indent=2) + "\n", encoding="utf-8"
    )
    return config.artifact_path


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Regenerate checked-in template artifacts.")
    registered = tuple(sorted(TEMPLATE_CONFIGS))
    parser.add_argument(
        "--target",
        choices=(*registered, "all"),
        default="all",
        help="Which artifact(s) to regenerate.",
    )
    args = parser.parse_args(argv)

    targets = registered if args.target == "all" else (args.target,)
    for target in targets:
        print(_write_template_artifact(TEMPLATE_CONFIGS[target]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
