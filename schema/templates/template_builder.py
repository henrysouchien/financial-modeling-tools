"""Build the checked-in PCTY reference and generic SIA template artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path
import re
from typing import Any, Dict, List, Optional, Sequence, Set

from ..models import (
    BuildStatus,
    CellColor,
    CellStyle,
    CompanyInfo,
    CustomizationType,
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
)
from ..reader import read_model

from .template_builder_config import (
    PCTY_PATH as PCTY_PATH,
    MODEL_TEMPLATE_PATH as MODEL_TEMPLATE_PATH,
    PCTY_REFERENCE_TEMPLATE_PATH as PCTY_REFERENCE_TEMPLATE_PATH,
    SIA_GENERIC_TEMPLATE_PATH as SIA_GENERIC_TEMPLATE_PATH,
    KEPT_SHEETS as KEPT_SHEETS,
    EXPECTED_ITEM_COUNT as EXPECTED_ITEM_COUNT,
    EXPECTED_ITEM_COUNT_GENERIC as EXPECTED_ITEM_COUNT_GENERIC,
    EXPECTED_SECTION_COUNTS as EXPECTED_SECTION_COUNTS,
    EXPECTED_SHEET_ITEM_COUNTS as EXPECTED_SHEET_ITEM_COUNTS,
    EXPECTED_SHEET_ITEM_COUNTS_GENERIC as EXPECTED_SHEET_ITEM_COUNTS_GENERIC,
    TemplateRole as TemplateRole,
    SectionSpec as SectionSpec,
    TemplateMetadataSpec as TemplateMetadataSpec,
    PlaceholderInserter as PlaceholderInserter,
    TemplateBuildConfig as TemplateBuildConfig,
    ASSUMPTIONS_SECTIONS as ASSUMPTIONS_SECTIONS,
    FINANCIAL_MODEL_SECTIONS as FINANCIAL_MODEL_SECTIONS,
    SECTION_SPECS as SECTION_SPECS,
    SECTION_SPECS_PCTY as SECTION_SPECS_PCTY,
    ASSUMPTIONS_SECTIONS_GENERIC as ASSUMPTIONS_SECTIONS_GENERIC,
    FINANCIAL_MODEL_SECTIONS_GENERIC as FINANCIAL_MODEL_SECTIONS_GENERIC,
    SECTION_SPECS_GENERIC as SECTION_SPECS_GENERIC,
    SHEET_PREFIX as SHEET_PREFIX,
    CANONICAL_NAME_OVERRIDES as CANONICAL_NAME_OVERRIDES,
    CANONICAL_NAME_OVERRIDES_PCTY as CANONICAL_NAME_OVERRIDES_PCTY,
    CANONICAL_NAME_OVERRIDES_GENERIC as CANONICAL_NAME_OVERRIDES_GENERIC,
    EXPECTED_HEADER_DEPENDENCY_TARGETS as EXPECTED_HEADER_DEPENDENCY_TARGETS,
    SCENARIO_LINKED_IDS as SCENARIO_LINKED_IDS,
    SCENARIO_TABLE_IDS as SCENARIO_TABLE_IDS,
    INPUT_IDS as INPUT_IDS,
    KEY_DRIVER_IDS as KEY_DRIVER_IDS,
    OPTIONAL_IDS as OPTIONAL_IDS,
    TEMPLATE_TOKENS as TEMPLATE_TOKENS,
    BUILD_NOTES as BUILD_NOTES,
    HEADER_DEPENDENCY_NOTES as HEADER_DEPENDENCY_NOTES,
    REPEAT_GROUP_ROLES as REPEAT_GROUP_ROLES,
    REPEAT_GROUP_IDS as REPEAT_GROUP_IDS,
    CASH_FLOW_ARTIFACT_HISTORICAL_IDS as CASH_FLOW_ARTIFACT_HISTORICAL_IDS,
    IF_APPLICABLE_DEFAULT_ZERO_IDS as IF_APPLICABLE_DEFAULT_ZERO_IDS,
    CASH_FLOW_FULL_PROJECTION_IDS as CASH_FLOW_FULL_PROJECTION_IDS,
    CASH_FLOW_PER_SHARE_PROJECTION_IDS as CASH_FLOW_PER_SHARE_PROJECTION_IDS,
    CASH_FLOW_NET_INCOME_SOURCE_ID as CASH_FLOW_NET_INCOME_SOURCE_ID,
    DATA_CONCEPT_MAP as DATA_CONCEPT_MAP,
    EXPECTED_HEADER_DEPENDENCY_TARGETS_PCTY as EXPECTED_HEADER_DEPENDENCY_TARGETS_PCTY,
    SCENARIO_LINKED_IDS_PCTY as SCENARIO_LINKED_IDS_PCTY,
    SCENARIO_TABLE_IDS_PCTY as SCENARIO_TABLE_IDS_PCTY,
    INPUT_IDS_PCTY as INPUT_IDS_PCTY,
    KEY_DRIVER_IDS_PCTY as KEY_DRIVER_IDS_PCTY,
    OPTIONAL_IDS_PCTY as OPTIONAL_IDS_PCTY,
    TEMPLATE_TOKENS_PCTY as TEMPLATE_TOKENS_PCTY,
    BUILD_NOTES_PCTY as BUILD_NOTES_PCTY,
    HEADER_DEPENDENCY_NOTES_PCTY as HEADER_DEPENDENCY_NOTES_PCTY,
    REPEAT_GROUP_ROLES_PCTY as REPEAT_GROUP_ROLES_PCTY,
    REPEAT_GROUP_IDS_PCTY as REPEAT_GROUP_IDS_PCTY,
    DATA_CONCEPT_MAP_PCTY as DATA_CONCEPT_MAP_PCTY,
    EXPECTED_HEADER_DEPENDENCY_TARGETS_GENERIC as EXPECTED_HEADER_DEPENDENCY_TARGETS_GENERIC,
    SCENARIO_LINKED_IDS_GENERIC as SCENARIO_LINKED_IDS_GENERIC,
    _SCENARIO_SELECTOR_ID as _SCENARIO_SELECTOR_ID,
    _COLUMN_OFFSET_MODE_PERIOD_RELATIVE as _COLUMN_OFFSET_MODE_PERIOD_RELATIVE,
    _SCENARIO_LINKED_OFFSET_ANCHORS as _SCENARIO_LINKED_OFFSET_ANCHORS,
    SCENARIO_TABLE_IDS_GENERIC as SCENARIO_TABLE_IDS_GENERIC,
    INPUT_IDS_GENERIC as INPUT_IDS_GENERIC,
    KEY_DRIVER_IDS_GENERIC as KEY_DRIVER_IDS_GENERIC,
    OPTIONAL_IDS_GENERIC as OPTIONAL_IDS_GENERIC,
    TEMPLATE_TOKENS_GENERIC as TEMPLATE_TOKENS_GENERIC,
    BUILD_NOTES_GENERIC as BUILD_NOTES_GENERIC,
    LABEL_OVERRIDES_GENERIC as LABEL_OVERRIDES_GENERIC,
    HEADER_DEPENDENCY_NOTES_GENERIC as HEADER_DEPENDENCY_NOTES_GENERIC,
    REPEAT_GROUP_ROLES_GENERIC as REPEAT_GROUP_ROLES_GENERIC,
    REPEAT_GROUP_IDS_GENERIC as REPEAT_GROUP_IDS_GENERIC,
    DATA_CONCEPT_MAP_GENERIC as DATA_CONCEPT_MAP_GENERIC,
)

from .template_builder_insertions import (
    _iter_items as _iter_items,
    _iter_formula_specs as _iter_formula_specs,
    _extract_refs as _extract_refs,
    _apply_scenario_linked_offset_params as _apply_scenario_linked_offset_params,
    _find_sheet_item as _find_sheet_item,
    _find_section_with_item as _find_section_with_item,
    _apply_label_overrides as _apply_label_overrides,
    _insert_sum_ref_after as _insert_sum_ref_after,
    _insert_other_non_current_assets_row as _insert_other_non_current_assets_row,
    _insert_deferred_revenue_row as _insert_deferred_revenue_row,
    _insert_other_current_liabilities_row as _insert_other_current_liabilities_row,
    _insert_commercial_paper_row as _insert_commercial_paper_row,
    _insert_deferred_revenue_noncurrent_row as _insert_deferred_revenue_noncurrent_row,
    _insert_inventory_row as _insert_inventory_row,
    _insert_investment_activity_rows as _insert_investment_activity_rows,
    _clear_cash_flow_artifact_historicals as _clear_cash_flow_artifact_historicals,
    _set_if_applicable_default_zero_historicals as _set_if_applicable_default_zero_historicals,
    _normalize_cash_flow_projection_links as _normalize_cash_flow_projection_links,
    _normalize_valuation_projection_links as _normalize_valuation_projection_links,
    _normalize_generic_cash_reconciliation as _normalize_generic_cash_reconciliation,
)









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


def _build_pcty_to_tpl_mapping(model: FinancialModel) -> Dict[str, str]:
    return _build_tpl_mapping(
        model,
        section_specs=SECTION_SPECS_PCTY,
        name_overrides=CANONICAL_NAME_OVERRIDES_PCTY,
        expected_item_count=EXPECTED_ITEM_COUNT,
    )


def _build_generic_to_tpl_mapping(model: FinancialModel) -> Dict[str, str]:
    return _build_tpl_mapping(
        model,
        section_specs=SECTION_SPECS_GENERIC,
        name_overrides=CANONICAL_NAME_OVERRIDES_GENERIC,
        expected_item_count=EXPECTED_ITEM_COUNT_GENERIC,
    )


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
    data_concept_map: Dict[str, str],
    key_driver_ids: Set[str],
) -> Set[str]:
    promoted_headers = _discover_header_dependency_targets(model)
    if promoted_headers != expected_header_dependency_targets:
        raise AssertionError(
            f"Header dependency targets mismatch: expected {sorted(expected_header_dependency_targets)}, "
            f"got {sorted(promoted_headers)}"
        )

    repeat_group_ids = {item_id: "revenue_segment" for item_id in repeat_group_roles}
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
        item.data_concept_id = data_concept_map.get(item.id)
        item.template_token = template_tokens.get(item.id)
        item.repeat_group_id = repeat_group_ids.get(item.id)
        item.repeat_group_role = repeat_group_roles.get(item.id)
        item.build_notes = _merge_notes(
            build_notes.get(item.id),
            header_dependency_notes.get(item.id) if item.id in promoted_headers else None,
        )

    return promoted_headers


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


def _include_treasury_stock_in_total_equity_historical(model: FinancialModel) -> None:
    total_equity = _find_sheet_item(
        model,
        "Financial_model",
        "tpl.fm.balance_sheet.total_equity",
    )
    spec = total_equity.historical
    if spec is None or spec.type is not FormulaType.arithmetic:
        raise AssertionError("total_equity historical formula must be arithmetic")

    items = spec.params.get("items")
    if not isinstance(items, list) or not all(isinstance(ref, LineItemRef) for ref in items):
        raise AssertionError("total_equity historical formula must be a SUM over refs")

    treasury_ref = "tpl.a.dividends_shares.treasury_stock"
    if any(ref.id == treasury_ref for ref in items):
        return

    spec.params["items"] = [*items, LineItemRef(id=treasury_ref)]


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


PCTY_TEMPLATE_CONFIG = TemplateBuildConfig(
    name="pcty",
    source_path=PCTY_PATH,
    artifact_path=PCTY_REFERENCE_TEMPLATE_PATH,
    section_specs=SECTION_SPECS_PCTY,
    name_overrides=CANONICAL_NAME_OVERRIDES_PCTY,
    expected_item_count=EXPECTED_ITEM_COUNT,
    expected_sheet_item_counts=EXPECTED_SHEET_ITEM_COUNTS,
    metadata=TemplateMetadataSpec(
        source_model="pcty_reference",
        notes="PCTY reference 2-sheet template derived from the PCTY reference model",
        company_name="PCTY Reference Template",
    ),
    expected_header_dependency_targets=EXPECTED_HEADER_DEPENDENCY_TARGETS_PCTY,
    scenario_linked_ids=SCENARIO_LINKED_IDS_PCTY,
    scenario_table_ids=SCENARIO_TABLE_IDS_PCTY,
    input_ids=INPUT_IDS_PCTY,
    key_driver_ids=KEY_DRIVER_IDS_PCTY,
    optional_ids=OPTIONAL_IDS_PCTY,
    template_tokens=TEMPLATE_TOKENS_PCTY,
    build_notes=BUILD_NOTES_PCTY,
    header_dependency_notes=HEADER_DEPENDENCY_NOTES_PCTY,
    repeat_group_roles=REPEAT_GROUP_ROLES_PCTY,
    data_concept_map=DATA_CONCEPT_MAP_PCTY,
    placeholder_inserters=(
        _insert_inventory_row,
        _insert_other_non_current_assets_row,
        _insert_deferred_revenue_row,
    ),
    load_kwargs={
        "expand_shared": True,
        "historical_cutoff_year": 2023,
    },
    clear_cash_flow_artifact_historicals=True,
)

GENERIC_TEMPLATE_CONFIG = TemplateBuildConfig(
    name="generic",
    source_path=MODEL_TEMPLATE_PATH,
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
    key_driver_ids=KEY_DRIVER_IDS_GENERIC,
    optional_ids=OPTIONAL_IDS_GENERIC,
    template_tokens=TEMPLATE_TOKENS_GENERIC,
    build_notes=BUILD_NOTES_GENERIC,
    header_dependency_notes=HEADER_DEPENDENCY_NOTES_GENERIC,
    repeat_group_roles=REPEAT_GROUP_ROLES_GENERIC,
    data_concept_map=DATA_CONCEPT_MAP_GENERIC,
    label_overrides=LABEL_OVERRIDES_GENERIC,
    placeholder_inserters=(
        _insert_other_current_liabilities_row,
        _insert_commercial_paper_row,
        _insert_deferred_revenue_noncurrent_row,
        _insert_investment_activity_rows,
        _normalize_generic_cash_reconciliation,
    ),
    load_kwargs={
        "historical_cutoff_year": 2023,
    },
    clear_cash_flow_artifact_historicals=True,
    set_if_applicable_default_zero_historicals=True,
    strip_scenario_table_constant_overrides=True,
    normalize_cash_flow_projection_links=True,
    normalize_valuation_projection_links=True,
)

TEMPLATE_CONFIGS = {
    "pcty": PCTY_TEMPLATE_CONFIG,
    "generic": GENERIC_TEMPLATE_CONFIG,
}


def _load_source_model(config: TemplateBuildConfig) -> FinancialModel:
    model = read_model(
        str(config.source_path),
        mode="full",
        **config.load_kwargs,
    )
    if not isinstance(model, FinancialModel):
        raise TypeError("read_model() did not return a FinancialModel")
    return model


def build_template(
    source_path: str | Path,
    section_specs: Dict[str, Sequence[SectionSpec]],
    name_overrides: Dict[str, str],
    metadata: TemplateMetadataSpec,
    placeholder_inserters: Sequence[PlaceholderInserter] = (),
    *,
    artifact_path: Path,
    expected_item_count: int,
    expected_sheet_item_counts: Dict[str, int],
    expected_header_dependency_targets: Set[str],
    scenario_linked_ids: Set[str],
    scenario_table_ids: Set[str],
    input_ids: Set[str],
    key_driver_ids: Set[str],
    optional_ids: Set[str],
    template_tokens: Dict[str, str],
    build_notes: Dict[str, str],
    header_dependency_notes: Dict[str, str],
    repeat_group_roles: Dict[str, str],
    data_concept_map: Dict[str, str],
    load_kwargs: Optional[Dict[str, Any]] = None,
    clear_cash_flow_artifact_historicals: bool = False,
    name: str = "custom",
    model: Optional[FinancialModel] = None,
) -> FinancialModel:
    config = TemplateBuildConfig(
        name=name,
        source_path=Path(source_path),
        artifact_path=artifact_path,
        section_specs=section_specs,
        name_overrides=name_overrides,
        expected_item_count=expected_item_count,
        expected_sheet_item_counts=expected_sheet_item_counts,
        metadata=metadata,
        expected_header_dependency_targets=expected_header_dependency_targets,
        scenario_linked_ids=scenario_linked_ids,
        scenario_table_ids=scenario_table_ids,
        input_ids=input_ids,
        key_driver_ids=key_driver_ids,
        optional_ids=optional_ids,
        template_tokens=template_tokens,
        build_notes=build_notes,
        header_dependency_notes=header_dependency_notes,
        repeat_group_roles=repeat_group_roles,
        data_concept_map=data_concept_map,
        placeholder_inserters=tuple(placeholder_inserters),
        load_kwargs=dict(load_kwargs or {}),
        clear_cash_flow_artifact_historicals=clear_cash_flow_artifact_historicals,
    )
    return _build_template_from_config(config, model=model)


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
        data_concept_map=config.data_concept_map,
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

    _apply_scenario_linked_offset_params(template)
    if config.normalize_cash_flow_projection_links:
        _normalize_cash_flow_projection_links(template)
    if config.normalize_valuation_projection_links:
        _normalize_valuation_projection_links(template)
    _include_treasury_stock_in_total_equity_historical(template)

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


def load_pcty_reference() -> FinancialModel:
    """Load the checked-in PCTY reference artifact."""

    return FinancialModel.model_validate_json(PCTY_REFERENCE_TEMPLATE_PATH.read_text(encoding="utf-8"))


def load_sia_generic_template() -> FinancialModel:
    """Load the checked-in generic SIA template artifact."""

    return FinancialModel.model_validate_json(SIA_GENERIC_TEMPLATE_PATH.read_text(encoding="utf-8"))


def build_pcty_reference_template(model: Optional[FinancialModel] = None) -> FinancialModel:
    """Transform the parsed PCTY workbook into the checked-in PCTY reference artifact."""

    return _build_template_from_config(PCTY_TEMPLATE_CONFIG, model=model)


def build_sia_generic_template(model: Optional[FinancialModel] = None) -> FinancialModel:
    """Transform the generic source workbook into the checked-in generic artifact."""

    return _build_template_from_config(GENERIC_TEMPLATE_CONFIG, model=model)


def _load_pcty_model() -> FinancialModel:
    return _load_source_model(PCTY_TEMPLATE_CONFIG)


def _load_generic_model() -> FinancialModel:
    return _load_source_model(GENERIC_TEMPLATE_CONFIG)


def _write_template_artifact(config: TemplateBuildConfig) -> Path:
    if config.name == "pcty":
        template = build_pcty_reference_template()
    elif config.name == "generic":
        template = build_sia_generic_template()
    else:  # pragma: no cover - guarded by CLI choices
        raise ValueError(f"Unknown template target: {config.name}")
    config.artifact_path.write_text(template.model_dump_json(indent=2), encoding="utf-8")
    return config.artifact_path


def main(argv: Optional[Sequence[str]] = None) -> int:
    parser = argparse.ArgumentParser(description="Regenerate checked-in template artifacts.")
    parser.add_argument(
        "--target",
        choices=("pcty", "generic", "all"),
        default="all",
        help="Which artifact(s) to regenerate.",
    )
    args = parser.parse_args(argv)

    targets = ("pcty", "generic") if args.target == "all" else (args.target,)
    for target in targets:
        print(_write_template_artifact(TEMPLATE_CONFIGS[target]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
