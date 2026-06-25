"""Semantic row materialization helpers for schema build orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
import re
import sys
from typing import Any

from .build_formula_refs import _extract_ref_ids as _formula_extract_ref_ids
from .build_model_items import _iter_items as _model_iter_items
from .build_real_data import _item_has_real_data as _real_data_item_has_real_data
from .build_semantic_rows_cash_flow import (
    _apply_semantic_cash_flow_linkages as _apply_semantic_cash_flow_linkages,
    _bind_or_insert_cf_linkage_row as _bind_or_insert_cf_linkage_row,
    _periods_for_delta_formula as _periods_for_delta_formula,
)
from .build_semantic_rows_client_funds import (
    _CLIENT_FUNDS_BRIDGE_SECTIONS as _CLIENT_FUNDS_BRIDGE_SECTIONS,
    _CLIENT_FUNDS_BUSINESS_MODEL_TERMS as _CLIENT_FUNDS_BUSINESS_MODEL_TERMS,
    _business_model_has_client_funds_topology as _business_model_has_client_funds_topology,
    _business_model_text_values as _business_model_text_values,
    _client_funds_bridge_entry as _client_funds_bridge_entry,
    _client_funds_bridge_residuals as _client_funds_bridge_residuals,
    _client_funds_bridge_target_is_available as _client_funds_bridge_target_is_available,
    _materialize_client_funds_subtotal_bridges as _materialize_client_funds_subtotal_bridges,
    _semantic_constant_value as _semantic_constant_value,
    _semantic_evaluate_expr as _semantic_evaluate_expr,
    _semantic_evaluate_formula as _semantic_evaluate_formula,
    _semantic_observed_value as _semantic_observed_value,
    _semantic_shift_period as _semantic_shift_period,
    _set_semantic_derived_values as _set_semantic_derived_values,
)
from .build_semantic_rows_valuation import (
    _apply_semantic_valuation_linkages as _apply_semantic_valuation_linkages,
    _semantic_role as _semantic_role,
)
from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    ItemType,
    LineItem,
    LineItemRef,
    Unit,
)
from .overrides import TickerOverrides


@dataclass
class SemanticRowsResult:
    materialized: list[dict[str, Any]] = field(default_factory=list)
    linkages: list[dict[str, Any]] = field(default_factory=list)
    collisions: list[dict[str, Any]] = field(default_factory=list)
    gaps: list[dict[str, Any]] = field(default_factory=list)
    source_custom_concepts: dict[str, dict[str, Any]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "materialized": list(self.materialized),
            "linkages": list(self.linkages),
            "collisions": list(self.collisions),
            "gaps": list(self.gaps),
        }


_CUSTOM_CONCEPT_CARRY_FORWARD_STRATEGIES = frozenset(
    {"carry_forward", "prior_period", "flat"}
)
_BS_TOTAL_FORMULA_BY_SECTION = {
    "current_assets": "tpl.fm.balance_sheet.total_current_assets",
    "current_liabilities": "tpl.fm.balance_sheet.total_current_liabilities",
}


def _parent_attr(name: str, fallback: Any) -> Any:
    parent_name = f"{__name__.rsplit('.', 1)[0]}.build"
    parent = sys.modules.get(parent_name)
    return getattr(parent, name, fallback) if parent is not None else fallback


def _semantic_slug(value: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", str(value).strip().lower())
    return slug.strip("_") or "row"


def _semantic_row_stable_id(
    ticker: str,
    concept_id: str,
    row_policy: dict[str, Any],
) -> str:
    configured = row_policy.get("stable_item_id")
    if isinstance(configured, str) and configured.strip():
        return configured.strip()
    semantic_slug = _parent_attr("_semantic_slug", _semantic_slug)
    return f"fm.semantic.{ticker.lower()}.{semantic_slug(concept_id)}"


def _locate_model_item(
    model: FinancialModel,
    item_id: str,
) -> tuple[str, Any, int, LineItem]:
    for sheet_name, sheet in model.sheets.items():
        for section in sheet.sections:
            for index, item in enumerate(section.line_items):
                if item.id == item_id:
                    return sheet_name, section, index, item
    raise KeyError(item_id)


def _item_has_any_real_historical_data(model: FinancialModel, item: LineItem) -> bool:
    item_has_real_data = _parent_attr("_item_has_real_data", _real_data_item_has_real_data)
    for period in (
        model.time_structure.historical_periods
        or model.time_structure.historical_years
    ):
        if item_has_real_data(item, int(period), model=model):
            return True
    return False


def _semantic_target_is_empty(model: FinancialModel, item: LineItem) -> bool:
    item_has_any_real_historical_data = _parent_attr(
        "_item_has_any_real_historical_data",
        _item_has_any_real_historical_data,
    )
    return not item.data_concept_id and not item_has_any_real_historical_data(model, item)


def _semantic_append_note(item: LineItem, note: str) -> None:
    note = note.strip()
    if not note:
        return
    if item.build_notes and note not in item.build_notes:
        item.build_notes = f"{item.build_notes}\n{note}"
    elif not item.build_notes:
        item.build_notes = note


def _semantic_forecast_type(entry: dict[str, Any]) -> str | None:
    policy = entry.get("forecast_policy")
    if isinstance(policy, dict):
        policy_type = policy.get("type")
        if isinstance(policy_type, str) and policy_type.strip():
            return policy_type.strip()
    strategy = entry.get("projection_strategy")
    if isinstance(strategy, str) and strategy.strip():
        return strategy.strip()
    return None


def _apply_semantic_row_metadata(
    item: LineItem,
    *,
    concept_id: str,
    entry: dict[str, Any],
) -> None:
    item.data_concept_id = concept_id
    label = entry.get("target_label") or entry.get("label")
    if isinstance(label, str) and label.strip():
        item.label = label.strip()
    unit = entry.get("unit")
    if unit is not None:
        unit_cls = _parent_attr("Unit", Unit)
        item.unit = unit_cls(str(unit))
    semantic_role = entry.get("semantic_role")
    forecast_policy = entry.get("forecast_policy")
    notes = entry.get("analyst_notes") or entry.get("notes")
    note_parts: list[str] = []
    if isinstance(notes, str) and notes.strip():
        note_parts.append(notes.strip())
    if isinstance(semantic_role, str) and semantic_role.strip():
        note_parts.append(f"Semantic role: {semantic_role.strip()}.")
    if isinstance(forecast_policy, dict):
        policy_type = forecast_policy.get("type")
        rationale = forecast_policy.get("rationale")
        if policy_type:
            note = f"Forecast policy: {policy_type}"
            if rationale:
                note += f" ({rationale})"
            note_parts.append(f"{note}.")
    semantic_append_note = _parent_attr("_semantic_append_note", _semantic_append_note)
    for note in note_parts:
        semantic_append_note(item, note)

    semantic_forecast_type = _parent_attr(
        "_semantic_forecast_type",
        _semantic_forecast_type,
    )
    forecast_type = semantic_forecast_type(entry)
    carry_forward_strategies = _parent_attr(
        "_CUSTOM_CONCEPT_CARRY_FORWARD_STRATEGIES",
        _CUSTOM_CONCEPT_CARRY_FORWARD_STRATEGIES,
    )
    formula_spec_cls = _parent_attr("FormulaSpec", FormulaSpec)
    formula_type = _parent_attr("FormulaType", FormulaType)
    line_item_ref_cls = _parent_attr("LineItemRef", LineItemRef)
    if forecast_type in carry_forward_strategies:
        item.projected = formula_spec_cls(
            type=formula_type.ref,
            params={"source": line_item_ref_cls(id=item.id, t=-1)},
        )
    elif forecast_type in {None, ""}:
        return
    elif forecast_type == "zero":
        item.projected = formula_spec_cls(type=formula_type.constant, params={"value": 0})
    else:
        raise ValueError(
            f"semantic_row {concept_id!r} forecast_policy type {forecast_type!r} "
            "is not supported"
        )


def _insert_line_item_after(
    model: FinancialModel,
    *,
    anchor_item_id: str,
    new_item: LineItem,
) -> LineItem:
    iter_items = _parent_attr("_iter_items", _model_iter_items)
    if any(item.id == new_item.id for item in iter_items(model)):
        raise ValueError(f"semantic row item_id {new_item.id!r} already exists")

    locate_model_item = _parent_attr("_locate_model_item", _locate_model_item)
    sheet_name, section, anchor_index, anchor = locate_model_item(model, anchor_item_id)
    insert_row = int(anchor.row) + 1
    for sheet_item in (
        item
        for section_obj in model.sheets[sheet_name].sections
        for item in section_obj.line_items
    ):
        if int(sheet_item.row) >= insert_row:
            sheet_item.row = int(sheet_item.row) + 1
    new_item.row = insert_row
    section.line_items.insert(anchor_index + 1, new_item)
    model.build_index()
    return new_item


def _formula_contains_ref(obj: Any, item_id: str) -> bool:
    extract_ref_ids = _parent_attr("_extract_ref_ids", _formula_extract_ref_ids)
    return item_id in extract_ref_ids(obj)


def _insert_ref_after_in_obj(obj: Any, *, after_id: str, new_id: str) -> bool:
    line_item_ref_cls = _parent_attr("LineItemRef", LineItemRef)
    insert_ref_after_in_obj = _parent_attr(
        "_insert_ref_after_in_obj",
        _insert_ref_after_in_obj,
    )
    if isinstance(obj, list):
        inserted = False
        index = 0
        while index < len(obj):
            value = obj[index]
            if insert_ref_after_in_obj(value, after_id=after_id, new_id=new_id):
                inserted = True
            if isinstance(value, line_item_ref_cls) and value.id == after_id:
                obj.insert(index + 1, line_item_ref_cls(id=new_id))
                inserted = True
                index += 1
            index += 1
        return inserted
    if isinstance(obj, dict):
        inserted = False
        for value in obj.values():
            if insert_ref_after_in_obj(value, after_id=after_id, new_id=new_id):
                inserted = True
        return inserted
    return False


def _add_formula_ref_after(
    model: FinancialModel,
    *,
    formula_item_id: str,
    after_id: str,
    new_id: str,
) -> bool:
    item = model.get_item(formula_item_id)
    formula_contains_ref = _parent_attr("_formula_contains_ref", _formula_contains_ref)
    insert_ref_after_in_obj = _parent_attr(
        "_insert_ref_after_in_obj",
        _insert_ref_after_in_obj,
    )
    inserted_any = False
    for spec in (item.historical, item.projected):
        if spec is None:
            continue
        if formula_contains_ref(spec.params, new_id):
            inserted_any = True
            continue
        inserted_any = (
            insert_ref_after_in_obj(spec.params, after_id=after_id, new_id=new_id)
            or inserted_any
        )
    return inserted_any


def _wire_semantic_bs_row(
    model: FinancialModel,
    *,
    section: str,
    row_policy: dict[str, Any],
    item_id: str,
) -> None:
    bs_total_formula_by_section = _parent_attr(
        "_BS_TOTAL_FORMULA_BY_SECTION",
        _BS_TOTAL_FORMULA_BY_SECTION,
    )
    formula_item_id = bs_total_formula_by_section.get(section)
    if formula_item_id is None:
        return
    after_id = row_policy.get("formula_insert_after_item_id") or row_policy.get(
        "insert_after_item_id"
    )
    if not isinstance(after_id, str) or not after_id.strip():
        return
    add_formula_ref_after = _parent_attr(
        "_add_formula_ref_after",
        _add_formula_ref_after,
    )
    if not add_formula_ref_after(
        model,
        formula_item_id=formula_item_id,
        after_id=after_id.strip(),
        new_id=item_id,
    ):
        raise ValueError(
            f"semantic row {item_id!r} could not be wired into {formula_item_id!r} "
            f"after {after_id!r}"
        )


def _create_semantic_line_item(
    ticker: str,
    concept_id: str,
    entry: dict[str, Any],
    row_policy: dict[str, Any],
) -> LineItem:
    semantic_row_stable_id = _parent_attr(
        "_semantic_row_stable_id",
        _semantic_row_stable_id,
    )
    line_item_cls = _parent_attr("LineItem", LineItem)
    item_type = _parent_attr("ItemType", ItemType)
    unit_cls = _parent_attr("Unit", Unit)
    return line_item_cls(
        id=semantic_row_stable_id(ticker, concept_id, row_policy),
        label=str(entry.get("target_label") or entry.get("label") or concept_id),
        row=0,
        item_type=item_type.derived,
        unit=unit_cls(str(entry.get("unit") or "dollars")),
        data_concept_id=concept_id,
    )


def _semantic_source_custom_entry(
    concept_id: str,
    entry: dict[str, Any],
    item_id: str,
) -> dict[str, Any]:
    source = entry.get("source") if isinstance(entry.get("source"), dict) else {}
    payload: dict[str, Any] = {
        "concept_id": concept_id,
        "target_item_id": item_id,
        "target_label": entry.get("target_label") or entry.get("label"),
        "statement": entry.get("statement"),
        "unit": entry.get("unit", "dollars"),
        "notes": entry.get("notes"),
        "analyst_notes": entry.get("analyst_notes"),
        "_meta": {"source": "semantic_rows"},
    }
    if isinstance(source, dict):
        tags = source.get("xbrl_tags") or source.get("edgar_tags")
        if tags is not None:
            payload["edgar_tags"] = tags
        for key in (
            "preferred_source",
            "fmp_endpoint",
            "fmp_field",
            "registry_group_id",
            "canonical_tag",
            "axis_key",
            "inline_values",
        ):
            if key in source:
                payload[key] = source[key]
    semantic_forecast_type = _parent_attr(
        "_semantic_forecast_type",
        _semantic_forecast_type,
    )
    forecast_type = semantic_forecast_type(entry)
    carry_forward_strategies = _parent_attr(
        "_CUSTOM_CONCEPT_CARRY_FORWARD_STRATEGIES",
        _CUSTOM_CONCEPT_CARRY_FORWARD_STRATEGIES,
    )
    if forecast_type in carry_forward_strategies:
        payload["projection_strategy"] = forecast_type
    return {key: value for key, value in payload.items() if value is not None}


def _bind_or_insert_semantic_row(
    model: FinancialModel,
    *,
    ticker: str,
    concept_id: str,
    entry: dict[str, Any],
    result: SemanticRowsResult,
) -> LineItem | None:
    row_policy = entry.get("row_policy")
    if not isinstance(row_policy, dict):
        raise ValueError(f"semantic_row {concept_id!r} missing row_policy object")
    mode = str(row_policy.get("mode") or "").strip()
    if not mode:
        raise ValueError(f"semantic_row {concept_id!r} missing row_policy.mode")

    semantic_row_stable_id = _parent_attr(
        "_semantic_row_stable_id",
        _semantic_row_stable_id,
    )
    apply_semantic_row_metadata = _parent_attr(
        "_apply_semantic_row_metadata",
        _apply_semantic_row_metadata,
    )
    stable_id = semantic_row_stable_id(ticker, concept_id, row_policy)
    try:
        item = model.get_item(stable_id)
        apply_semantic_row_metadata(item, concept_id=concept_id, entry=entry)
        result.materialized.append(
            {"concept_id": concept_id, "action": "bound_existing", "item_id": item.id}
        )
        return item
    except KeyError:
        pass

    iter_items = _parent_attr("_iter_items", _model_iter_items)
    for candidate in iter_items(model):
        if candidate.data_concept_id == concept_id:
            apply_semantic_row_metadata(candidate, concept_id=concept_id, entry=entry)
            result.materialized.append(
                {
                    "concept_id": concept_id,
                    "action": "bound_same_semantic",
                    "item_id": candidate.id,
                }
            )
            return candidate

    preferred_target_id = row_policy.get("preferred_target_item_id")
    preferred_item: LineItem | None = None
    if isinstance(preferred_target_id, str) and preferred_target_id.strip():
        try:
            preferred_item = model.get_item(preferred_target_id.strip())
        except KeyError:
            result.gaps.append(
                {
                    "concept_id": concept_id,
                    "kind": "preferred_target_missing",
                    "item_id": preferred_target_id,
                }
            )

    semantic_target_is_empty = _parent_attr(
        "_semantic_target_is_empty",
        _semantic_target_is_empty,
    )
    if preferred_item is not None:
        if mode in {
            "bind_if_empty",
            "bind_if_empty_or_insert",
        } and semantic_target_is_empty(model, preferred_item):
            apply_semantic_row_metadata(preferred_item, concept_id=concept_id, entry=entry)
            result.materialized.append(
                {
                    "concept_id": concept_id,
                    "action": "bound_empty",
                    "item_id": preferred_item.id,
                }
            )
            return preferred_item
        if (
            mode == "bind_if_same_semantic"
            and preferred_item.data_concept_id == concept_id
        ):
            apply_semantic_row_metadata(preferred_item, concept_id=concept_id, entry=entry)
            result.materialized.append(
                {
                    "concept_id": concept_id,
                    "action": "bound_same_semantic",
                    "item_id": preferred_item.id,
                }
            )
            return preferred_item
        if mode in {"bind_if_empty", "bind_if_same_semantic"}:
            result.collisions.append(
                {
                    "concept_id": concept_id,
                    "item_id": preferred_item.id,
                    "existing_concept_id": preferred_item.data_concept_id,
                    "mode": mode,
                }
            )
            raise ValueError(
                f"semantic_row {concept_id!r} cannot bind occupied target "
                f"{preferred_item.id!r} mapped to {preferred_item.data_concept_id!r}"
            )

    if mode not in {"bind_if_empty_or_insert", "insert", "insert_or_bind_same_semantic"}:
        result.gaps.append(
            {"concept_id": concept_id, "kind": "unsupported_row_policy", "mode": mode}
        )
        raise ValueError(f"semantic_row {concept_id!r} unsupported row_policy.mode {mode!r}")

    insert_after_id = row_policy.get("insert_after_item_id")
    if not isinstance(insert_after_id, str) or not insert_after_id.strip():
        raise ValueError(f"semantic_row {concept_id!r} insert mode missing insert_after_item_id")
    create_semantic_line_item = _parent_attr(
        "_create_semantic_line_item",
        _create_semantic_line_item,
    )
    insert_line_item_after = _parent_attr("_insert_line_item_after", _insert_line_item_after)
    wire_semantic_bs_row = _parent_attr("_wire_semantic_bs_row", _wire_semantic_bs_row)
    item = create_semantic_line_item(ticker, concept_id, entry, row_policy)
    apply_semantic_row_metadata(item, concept_id=concept_id, entry=entry)
    insert_line_item_after(model, anchor_item_id=insert_after_id.strip(), new_item=item)
    wire_semantic_bs_row(
        model,
        section=str(entry.get("section") or ""),
        row_policy=row_policy,
        item_id=item.id,
    )
    result.materialized.append(
        {"concept_id": concept_id, "action": "inserted", "item_id": item.id}
    )
    return item


def _materialize_semantic_rows(
    model: FinancialModel,
    ticker: str,
    overrides: TickerOverrides,
) -> SemanticRowsResult:
    semantic_rows_result_cls = _parent_attr("SemanticRowsResult", SemanticRowsResult)
    result = semantic_rows_result_cls()
    materialized_by_concept: dict[str, str] = {}
    bind_or_insert_semantic_row = _parent_attr(
        "_bind_or_insert_semantic_row",
        _bind_or_insert_semantic_row,
    )
    semantic_source_custom_entry = _parent_attr(
        "_semantic_source_custom_entry",
        _semantic_source_custom_entry,
    )
    for concept_id, entry in sorted((overrides.semantic_rows or {}).items()):
        if not isinstance(entry, dict):
            raise ValueError(f"semantic_row {concept_id!r} must be an object")
        if (entry.get("_meta") or {}).get("disabled") is True:
            continue
        statement = str(entry.get("statement") or "")
        if statement != "balance_sheet":
            result.gaps.append(
                {
                    "concept_id": concept_id,
                    "kind": "unsupported_statement",
                    "statement": statement,
                }
            )
            continue
        item = bind_or_insert_semantic_row(
            model,
            ticker=ticker,
            concept_id=concept_id,
            entry=entry,
            result=result,
        )
        if item is None:
            continue
        materialized_by_concept[concept_id] = item.id
        result.source_custom_concepts[concept_id] = semantic_source_custom_entry(
            concept_id,
            entry,
            item.id,
        )

    apply_semantic_cash_flow_linkages = _parent_attr(
        "_apply_semantic_cash_flow_linkages",
        _apply_semantic_cash_flow_linkages,
    )
    apply_semantic_valuation_linkages = _parent_attr(
        "_apply_semantic_valuation_linkages",
        _apply_semantic_valuation_linkages,
    )
    apply_semantic_cash_flow_linkages(
        model,
        ticker=ticker,
        overrides=overrides,
        materialized_by_concept=materialized_by_concept,
        result=result,
    )
    apply_semantic_valuation_linkages(
        model,
        ticker=ticker,
        overrides=overrides,
        materialized_by_concept=materialized_by_concept,
        result=result,
    )
    return result
