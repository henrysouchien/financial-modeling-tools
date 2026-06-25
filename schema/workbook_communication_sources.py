from __future__ import annotations

import json
from pathlib import Path
import sys
from typing import Any, Iterable, Mapping

from pydantic import BaseModel, ConfigDict

from .workbook_communication_specs import _SCENARIO_CASES
from .workbook_communication_utils import (
    _artifact_ref,
    _first_mapping,
    _first_number,
    _get_path,
    _normalize_label,
    _number_from_source,
    _number_or_none,
    _string_or_none,
)


class WorkbookCommunicationSourceValue(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    value: Any
    source_kind: str
    source_ref: str | None = None


def _parent_attr(name: str, fallback: Any) -> Any:
    parent = sys.modules.get("schema.workbook_communication")
    if parent is None:
        parent = sys.modules.get("workbook_communication")
    return getattr(parent, name, fallback) if parent is not None else fallback


def source_values_from_artifacts(
    *artifacts: str | Path | Mapping[str, Any],
) -> dict[str, WorkbookCommunicationSourceValue]:
    """Extract communication-ready values from FMS artifact payloads."""

    load_artifact_payload = _parent_attr("_load_artifact_payload", _load_artifact_payload)
    merge_current_model_values = _parent_attr("_merge_current_model_values", _merge_current_model_values)
    merge_price_target_values = _parent_attr("_merge_price_target_values", _merge_price_target_values)
    merge_valuation_method_values = _parent_attr(
        "_merge_valuation_method_values",
        _merge_valuation_method_values,
    )
    merge_scenario_values = _parent_attr("_merge_scenario_values", _merge_scenario_values)
    merge_expected_value_values = _parent_attr("_merge_expected_value_values", _merge_expected_value_values)
    derive_expected_return = _parent_attr("_derive_expected_return", _derive_expected_return)

    values: dict[str, WorkbookCommunicationSourceValue] = {}
    for artifact in artifacts:
        payload, source_ref = load_artifact_payload(artifact)
        merge_current_model_values(values, payload, source_ref)
        merge_price_target_values(values, payload, source_ref)
        merge_valuation_method_values(values, payload, source_ref)
        merge_scenario_values(values, payload, source_ref)
        merge_expected_value_values(values, payload, source_ref)
    derive_expected_return(values)
    return values


def _normalize_source_values(
    source_values: Mapping[str, Any],
) -> dict[str, WorkbookCommunicationSourceValue]:
    source_value_cls = _parent_attr("WorkbookCommunicationSourceValue", WorkbookCommunicationSourceValue)
    string_or_none = _parent_attr("_string_or_none", _string_or_none)

    normalized: dict[str, WorkbookCommunicationSourceValue] = {}
    for field, raw_value in source_values.items():
        if isinstance(raw_value, source_value_cls):
            normalized[field] = raw_value
        elif isinstance(raw_value, Mapping):
            normalized[field] = source_value_cls(
                value=raw_value.get("value"),
                source_kind=str(raw_value.get("source_kind") or "provided"),
                source_ref=string_or_none(raw_value.get("source_ref")),
            )
        else:
            normalized[field] = source_value_cls(
                value=raw_value,
                source_kind="provided",
            )
    return normalized


def _load_artifact_payload(artifact: str | Path | Mapping[str, Any]) -> tuple[Mapping[str, Any], str | None]:
    artifact_ref = _parent_attr("_artifact_ref", _artifact_ref)
    if isinstance(artifact, Mapping):
        return artifact, artifact_ref(artifact)
    path = Path(artifact)
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload, str(path)


def _merge_current_model_values(
    values: dict[str, WorkbookCommunicationSourceValue],
    payload: Mapping[str, Any],
    source_ref: str | None,
) -> None:
    first_mapping = _parent_attr("_first_mapping", _first_mapping)
    get_path = _parent_attr("_get_path", _get_path)
    string_or_none = _parent_attr("_string_or_none", _string_or_none)
    first_number = _parent_attr("_first_number", _first_number)
    set_number = _parent_attr("_set_number", _set_number)

    current_model = first_mapping(
        get_path(payload, ("verdict", "current_model")),
        get_path(payload, ("current_model",)),
    )
    if not current_model:
        return
    ref = string_or_none(current_model.get("price_target_id")) or string_or_none(
        current_model.get("model_id")
    ) or source_ref
    current_price = first_number(
        current_model.get("price_target_current_price"),
        current_model.get("current_price"),
        current_model.get("last_price"),
        current_model.get("stock_price"),
    )
    target_price = first_number(
        current_model.get("last_price_target"),
        current_model.get("price_target"),
        current_model.get("target_price"),
    )
    expected_return = first_number(
        current_model.get("price_target_expected_return"),
        current_model.get("expected_return"),
        current_model.get("return_pct"),
    )
    set_number(values, "summary.current_price", current_price, "current_model_artifact", ref)
    set_number(values, "valuation.current_price", current_price, "current_model_artifact", ref)
    set_number(values, "summary.blended_target_price", target_price, "current_model_artifact", ref)
    set_number(
        values,
        "valuation.blended_target_price",
        target_price,
        "current_model_artifact",
        ref,
    )
    set_number(values, "summary.expected_return", expected_return, "current_model_artifact", ref)
    set_number(values, "valuation.expected_return", expected_return, "current_model_artifact", ref)


def _merge_price_target_values(
    values: dict[str, WorkbookCommunicationSourceValue],
    payload: Mapping[str, Any],
    source_ref: str | None,
) -> None:
    first_mapping = _parent_attr("_first_mapping", _first_mapping)
    get_path = _parent_attr("_get_path", _get_path)
    string_or_none = _parent_attr("_string_or_none", _string_or_none)
    first_number = _parent_attr("_first_number", _first_number)
    set_number = _parent_attr("_set_number", _set_number)

    price_target = first_mapping(
        get_path(payload, ("verdict", "price_target")),
        get_path(payload, ("typed_outputs", "price_target")),
        get_path(payload, ("price_target",)),
    )
    if not price_target:
        return
    ref = string_or_none(price_target.get("id")) or source_ref
    ranges = price_target.get("ranges") if isinstance(price_target.get("ranges"), Mapping) else {}
    target_price = first_number(
        price_target.get("target_price"),
        price_target.get("price_target"),
        ranges.get("mid") if isinstance(ranges, Mapping) else None,
        ranges.get("base") if isinstance(ranges, Mapping) else None,
    )
    current_price = first_number(
        price_target.get("current_price"),
        price_target.get("price_target_current_price"),
    )
    expected_return = first_number(
        price_target.get("expected_return"),
        price_target.get("implied_return_pct"),
        price_target.get("return_pct"),
    )
    set_number(values, "summary.current_price", current_price, "price_target_artifact", ref)
    set_number(values, "valuation.current_price", current_price, "price_target_artifact", ref)
    set_number(values, "summary.blended_target_price", target_price, "price_target_artifact", ref)
    set_number(
        values,
        "valuation.blended_target_price",
        target_price,
        "price_target_artifact",
        ref,
    )
    set_number(values, "summary.expected_return", expected_return, "price_target_artifact", ref)
    set_number(values, "valuation.expected_return", expected_return, "price_target_artifact", ref)


def _merge_valuation_method_values(
    values: dict[str, WorkbookCommunicationSourceValue],
    payload: Mapping[str, Any],
    source_ref: str | None,
) -> None:
    first_mapping = _parent_attr("_first_mapping", _first_mapping)
    get_path = _parent_attr("_get_path", _get_path)
    first_number = _parent_attr("_first_number", _first_number)
    set_number = _parent_attr("_set_number", _set_number)

    targets_by_method = first_mapping(
        get_path(payload, ("typed_outputs", "targets_by_method")),
        get_path(payload, ("verdict", "targets_by_method")),
        get_path(payload, ("verdict", "typed_outputs", "targets_by_method")),
    )
    workbook_write = first_mapping(
        get_path(payload, ("typed_outputs", "workbook_write")),
        get_path(payload, ("verdict", "workbook_write")),
        get_path(payload, ("typed_outputs", "verification")),
        get_path(payload, ("verdict", "verification")),
    )
    if not targets_by_method and not workbook_write:
        return

    dcf = targets_by_method.get("dcf") if isinstance(targets_by_method, Mapping) else None
    forward_pe = targets_by_method.get("forward_pe") if isinstance(targets_by_method, Mapping) else None
    forward_ev_ebitda = (
        targets_by_method.get("forward_ev_ebitda") if isinstance(targets_by_method, Mapping) else None
    )
    dcf = dcf if isinstance(dcf, Mapping) else {}
    forward_pe = forward_pe if isinstance(forward_pe, Mapping) else {}
    forward_ev_ebitda = forward_ev_ebitda if isinstance(forward_ev_ebitda, Mapping) else {}
    effectiveness = (
        workbook_write.get("effectiveness_readback") if isinstance(workbook_write, Mapping) else None
    )
    fixed = workbook_write.get("fixed_cell_readback") if isinstance(workbook_write, Mapping) else None
    effectiveness = effectiveness if isinstance(effectiveness, Mapping) else {}
    fixed = fixed if isinstance(fixed, Mapping) else {}
    ref = source_ref

    set_number(
        values,
        "valuation.dcf_price",
        first_number(
            effectiveness.get("tpl.v.dcf.dcf_price"),
            effectiveness.get("tpl.v.dcf.dcf_price_summary"),
            dcf.get("target"),
        ),
        "valuation_artifact",
        ref,
    )
    set_number(
        values,
        "valuation.forward_pe_price",
        forward_pe.get("target"),
        "valuation_artifact",
        ref,
    )
    set_number(
        values,
        "valuation.forward_ev_ebitda_price",
        forward_ev_ebitda.get("target"),
        "valuation_artifact",
        ref,
    )
    set_number(
        values,
        "valuation.wacc",
        dcf.get("wacc_pct"),
        "valuation_artifact",
        ref,
    )
    set_number(
        values,
        "valuation.terminal_growth",
        first_number(
            fixed.get("tpl.v.dcf.terminal_growth_base"),
            dcf.get("terminal_growth"),
            dcf.get("terminal_growth_rate"),
        ),
        "valuation_artifact",
        ref,
    )


def _merge_scenario_values(
    values: dict[str, WorkbookCommunicationSourceValue],
    payload: Mapping[str, Any],
    source_ref: str | None,
) -> None:
    first_mapping = _parent_attr("_first_mapping", _first_mapping)
    get_path = _parent_attr("_get_path", _get_path)
    iter_scenarios = _parent_attr("_iter_scenarios", _iter_scenarios)
    scenario_cases = _parent_attr("_SCENARIO_CASES", _SCENARIO_CASES)
    first_number = _parent_attr("_first_number", _first_number)
    string_or_none = _parent_attr("_string_or_none", _string_or_none)
    set_number = _parent_attr("_set_number", _set_number)

    typed_outputs = first_mapping(
        get_path(payload, ("verdict", "typed_outputs")),
        get_path(payload, ("typed_outputs",)),
        get_path(payload, ("verdict", "fms_result", "typed_outputs")),
    )
    scenario_root = first_mapping(
        typed_outputs.get("scenarios") if typed_outputs else None,
        get_path(payload, ("verdict", "scenarios")),
        get_path(payload, ("scenarios",)),
    )
    if not scenario_root:
        return
    for case, scenario in iter_scenarios(scenario_root):
        if case not in scenario_cases:
            continue
        target_price = first_number(
            scenario.get("target_price"),
            scenario.get("price_target"),
            scenario.get("selected_scenario_valuation"),
            scenario.get("valuation"),
        )
        return_pct = first_number(
            scenario.get("return_pct"),
            scenario.get("implied_return_pct"),
            scenario.get("expected_return"),
        )
        probability = first_number(
            scenario.get("probability"),
            scenario.get("probability_weight"),
            scenario.get("weight"),
        )
        ref = string_or_none(scenario.get("id")) or source_ref
        set_number(
            values,
            f"summary.scenario.{case}.target_price",
            target_price,
            "scenario_pricing_artifact",
            ref,
        )
        set_number(
            values,
            f"summary.scenario.{case}.return_pct",
            return_pct,
            "scenario_pricing_artifact",
            ref,
        )
        set_number(
            values,
            f"summary.scenario.{case}.probability",
            probability,
            "scenario_pricing_artifact",
            ref,
        )


def _merge_expected_value_values(
    values: dict[str, WorkbookCommunicationSourceValue],
    payload: Mapping[str, Any],
    source_ref: str | None,
) -> None:
    first_mapping = _parent_attr("_first_mapping", _first_mapping)
    get_path = _parent_attr("_get_path", _get_path)
    set_number = _parent_attr("_set_number", _set_number)
    iter_scenarios = _parent_attr("_iter_scenarios", _iter_scenarios)
    scenario_cases = _parent_attr("_SCENARIO_CASES", _SCENARIO_CASES)
    string_or_none = _parent_attr("_string_or_none", _string_or_none)
    first_number = _parent_attr("_first_number", _first_number)

    expected_value = first_mapping(
        get_path(payload, ("typed_outputs", "expected_value")),
        get_path(payload, ("verdict", "expected_value")),
        get_path(payload, ("expected_value",)),
    )
    if expected_value:
        set_number(
            values,
            "summary.expected_value.price",
            expected_value.get("expected_price"),
            "expected_value_artifact",
            source_ref,
        )
        set_number(
            values,
            "summary.expected_value.return_pct",
            expected_value.get("expected_return_pct"),
            "expected_value_artifact",
            source_ref,
        )
        set_number(
            values,
            "summary.expected_value.return_to_risk",
            expected_value.get("return_to_risk"),
            "expected_value_artifact",
            source_ref,
        )

    probabilities = first_mapping(
        get_path(payload, ("typed_outputs", "scenario_probabilities")),
        get_path(payload, ("verdict", "scenario_probabilities")),
        get_path(payload, ("scenario_probabilities",)),
    )
    if not probabilities:
        return
    for case, scenario in iter_scenarios(probabilities):
        if case not in scenario_cases:
            continue
        ref = string_or_none(scenario.get("claim_id")) or source_ref
        set_number(
            values,
            f"summary.scenario.{case}.probability",
            first_number(
                scenario.get("probability"),
                scenario.get("probability_weight"),
                scenario.get("weight"),
            ),
            "expected_value_artifact",
            ref,
        )


def _derive_expected_return(values: dict[str, WorkbookCommunicationSourceValue]) -> None:
    number_from_source = _parent_attr("_number_from_source", _number_from_source)
    source_value_cls = _parent_attr("WorkbookCommunicationSourceValue", WorkbookCommunicationSourceValue)

    if "summary.expected_return" in values:
        return
    current = number_from_source(values.get("summary.current_price"))
    target = number_from_source(values.get("summary.blended_target_price"))
    if current is None or target is None or current == 0:
        return
    derived = source_value_cls(
        value=(target / current) - 1.0,
        source_kind="derived_from_typed_artifact_values",
        source_ref=values.get(
            "summary.blended_target_price",
            values.get("summary.current_price"),
        ).source_ref,
    )
    values["summary.expected_return"] = derived
    values["valuation.expected_return"] = derived


def _iter_scenarios(scenario_root: Mapping[str, Any]) -> Iterable[tuple[str, Mapping[str, Any]]]:
    scenario_case = _parent_attr("_scenario_case", _scenario_case)
    if all(isinstance(value, Mapping) for value in scenario_root.values()):
        for key, value in scenario_root.items():
            yield scenario_case(key, value), value
        return
    scenario_list = scenario_root.get("cases") or scenario_root.get("scenarios")
    if isinstance(scenario_list, list):
        for item in scenario_list:
            if isinstance(item, Mapping):
                yield scenario_case(item.get("case") or item.get("name"), item), item


def _field_from_mapping(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def _scenario_case(raw_case: Any, scenario: Mapping[str, Any]) -> str:
    field_from_mapping = _parent_attr("_field_from_mapping", _field_from_mapping)
    normalize_label = _parent_attr("_normalize_label", _normalize_label)
    candidate = raw_case or field_from_mapping(scenario, "case", "name", "scenario")
    text = normalize_label(candidate)
    if "bull" in text or "upside" in text:
        return "bull"
    if "bear" in text or "downside" in text:
        return "bear"
    if "base" in text or "central" in text:
        return "base"
    return text


def _set_number(
    values: dict[str, WorkbookCommunicationSourceValue],
    field: str,
    value: Any,
    source_kind: str,
    source_ref: str | None,
) -> None:
    number_or_none = _parent_attr("_number_or_none", _number_or_none)
    source_value_cls = _parent_attr("WorkbookCommunicationSourceValue", WorkbookCommunicationSourceValue)
    number = number_or_none(value)
    if number is None:
        return
    values[field] = source_value_cls(
        value=number,
        source_kind=source_kind,
        source_ref=source_ref,
    )


__all__ = [
    "WorkbookCommunicationSourceValue",
    "_derive_expected_return",
    "_field_from_mapping",
    "_iter_scenarios",
    "_load_artifact_payload",
    "_merge_current_model_values",
    "_merge_expected_value_values",
    "_merge_price_target_values",
    "_merge_scenario_values",
    "_merge_valuation_method_values",
    "_normalize_source_values",
    "_scenario_case",
    "_set_number",
    "source_values_from_artifacts",
]
