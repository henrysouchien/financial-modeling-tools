from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from pydantic import BaseModel, ConfigDict

from . import workbook_communication_utils as _utils
from .workbook_communication_specs import _SCENARIO_CASES


class WorkbookCommunicationSourceValue(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    value: Any
    source_kind: str
    source_ref: str | None = None


def source_values_from_artifacts(
    *artifacts: str | Path | Mapping[str, Any],
) -> dict[str, WorkbookCommunicationSourceValue]:
    """Extract communication-ready values from FMS artifact payloads."""

    values: dict[str, WorkbookCommunicationSourceValue] = {}
    for artifact in artifacts:
        payload, source_ref = _load_artifact_payload(artifact)
        _merge_price_target_values(values, payload, source_ref)
        _merge_valuation_method_values(values, payload, source_ref)
        _merge_scenario_values(values, payload, source_ref)
        _merge_expected_value_values(values, payload, source_ref)
    _derive_expected_return(values)
    return values


def _normalize_source_values(
    source_values: Mapping[str, Any],
) -> dict[str, WorkbookCommunicationSourceValue]:
    normalized: dict[str, WorkbookCommunicationSourceValue] = {}
    for field, raw_value in source_values.items():
        if isinstance(raw_value, WorkbookCommunicationSourceValue):
            normalized[field] = raw_value
        elif isinstance(raw_value, Mapping):
            normalized[field] = WorkbookCommunicationSourceValue(
                value=raw_value.get("value"),
                source_kind=str(raw_value.get("source_kind") or "provided"),
                source_ref=_utils._string_or_none(raw_value.get("source_ref")),
            )
        else:
            normalized[field] = WorkbookCommunicationSourceValue(
                value=raw_value,
                source_kind="provided",
            )
    return normalized


def _load_artifact_payload(artifact: str | Path | Mapping[str, Any]) -> tuple[Mapping[str, Any], str | None]:
    if isinstance(artifact, Mapping):
        return artifact, _utils._artifact_ref(artifact)
    path = Path(artifact)
    payload = json.loads(path.read_text(encoding="utf-8"))
    return payload, str(path)


def _merge_price_target_values(
    values: dict[str, WorkbookCommunicationSourceValue],
    payload: Mapping[str, Any],
    source_ref: str | None,
) -> None:
    # Current price, blended target and expected return are PriceTarget facts;
    # they are read from a PriceTarget payload and from no other copy.
    price_target = _utils._first_mapping(
        _utils._get_path(payload, ("verdict", "price_target")),
        _utils._get_path(payload, ("typed_outputs", "price_target")),
        _utils._get_path(payload, ("price_target",)),
    )
    if not price_target:
        return
    ref = _utils._string_or_none(price_target.get("price_target_id")) or source_ref
    ranges = price_target.get("ranges")
    target_price = ranges.get("mid") if isinstance(ranges, Mapping) else None
    current_price = price_target.get("current_price")
    expected_return = _percent_points_to_fraction(price_target.get("implied_return_pct"))
    _set_number(values, "summary.current_price", current_price, "price_target_artifact", ref)
    _set_number(values, "valuation.current_price", current_price, "price_target_artifact", ref)
    _set_number(values, "summary.blended_target_price", target_price, "price_target_artifact", ref)
    _set_number(
        values,
        "valuation.blended_target_price",
        target_price,
        "price_target_artifact",
        ref,
    )
    _set_number(values, "summary.expected_return", expected_return, "price_target_artifact", ref)
    _set_number(values, "valuation.expected_return", expected_return, "price_target_artifact", ref)


def _percent_points_to_fraction(value: Any) -> float | None:
    # PriceTarget.implied_return_pct is in percent points (``((mid / current) - 1) * 100``);
    # the workbook's expected-return cells hold a fraction. A "25%" string already
    # parses to its fraction.
    if isinstance(value, str) and value.strip().endswith("%"):
        return _utils._number_or_none(value)
    number = _utils._number_or_none(value)
    return None if number is None else number / 100.0


def _merge_valuation_method_values(
    values: dict[str, WorkbookCommunicationSourceValue],
    payload: Mapping[str, Any],
    source_ref: str | None,
) -> None:
    targets_by_method = _utils._first_mapping(
        _utils._get_path(payload, ("typed_outputs", "targets_by_method")),
        _utils._get_path(payload, ("verdict", "targets_by_method")),
        _utils._get_path(payload, ("verdict", "typed_outputs", "targets_by_method")),
    )
    workbook_write = _utils._first_mapping(
        _utils._get_path(payload, ("typed_outputs", "workbook_write")),
        _utils._get_path(payload, ("verdict", "workbook_write")),
        _utils._get_path(payload, ("typed_outputs", "verification")),
        _utils._get_path(payload, ("verdict", "verification")),
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

    _set_number(
        values,
        "valuation.dcf_price",
        _utils._first_number(
            effectiveness.get("tpl.v.dcf.dcf_price"),
            effectiveness.get("tpl.v.dcf.dcf_price_summary"),
            dcf.get("target"),
        ),
        "valuation_artifact",
        ref,
    )
    _set_number(
        values,
        "valuation.forward_pe_price",
        forward_pe.get("target"),
        "valuation_artifact",
        ref,
    )
    _set_number(
        values,
        "valuation.forward_ev_ebitda_price",
        forward_ev_ebitda.get("target"),
        "valuation_artifact",
        ref,
    )
    _set_number(
        values,
        "valuation.wacc",
        dcf.get("wacc_pct"),
        "valuation_artifact",
        ref,
    )
    _set_number(
        values,
        "valuation.terminal_growth",
        _utils._first_number(
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
    typed_outputs = _utils._first_mapping(
        _utils._get_path(payload, ("verdict", "typed_outputs")),
        _utils._get_path(payload, ("typed_outputs",)),
        _utils._get_path(payload, ("verdict", "fms_result", "typed_outputs")),
    )
    scenario_root = _utils._first_mapping(
        typed_outputs.get("scenarios") if typed_outputs else None,
        _utils._get_path(payload, ("verdict", "scenarios")),
        _utils._get_path(payload, ("scenarios",)),
    )
    if not scenario_root:
        return
    for case, scenario in _iter_scenarios(scenario_root):
        if case not in _SCENARIO_CASES:
            continue
        target_price = _utils._first_number(
            scenario.get("target_price"),
            scenario.get("price_target"),
            scenario.get("selected_scenario_valuation"),
            scenario.get("valuation"),
        )
        return_pct = _utils._first_number(
            scenario.get("return_pct"),
            scenario.get("implied_return_pct"),
            scenario.get("expected_return"),
        )
        probability = _utils._first_number(
            scenario.get("probability"),
            scenario.get("probability_weight"),
            scenario.get("weight"),
        )
        ref = _utils._string_or_none(scenario.get("id")) or source_ref
        _set_number(
            values,
            f"summary.scenario.{case}.target_price",
            target_price,
            "scenario_pricing_artifact",
            ref,
        )
        _set_number(
            values,
            f"summary.scenario.{case}.return_pct",
            return_pct,
            "scenario_pricing_artifact",
            ref,
        )
        _set_number(
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
    expected_value = _utils._first_mapping(
        _utils._get_path(payload, ("typed_outputs", "expected_value")),
        _utils._get_path(payload, ("verdict", "expected_value")),
        _utils._get_path(payload, ("expected_value",)),
    )
    if expected_value:
        _set_number(
            values,
            "summary.expected_value.price",
            expected_value.get("expected_price"),
            "expected_value_artifact",
            source_ref,
        )
        _set_number(
            values,
            "summary.expected_value.return_pct",
            expected_value.get("expected_return_pct"),
            "expected_value_artifact",
            source_ref,
        )
        _set_number(
            values,
            "summary.expected_value.return_to_risk",
            expected_value.get("return_to_risk"),
            "expected_value_artifact",
            source_ref,
        )

    probabilities = _utils._first_mapping(
        _utils._get_path(payload, ("typed_outputs", "scenario_probabilities")),
        _utils._get_path(payload, ("verdict", "scenario_probabilities")),
        _utils._get_path(payload, ("scenario_probabilities",)),
    )
    if not probabilities:
        return
    for case, scenario in _iter_scenarios(probabilities):
        if case not in _SCENARIO_CASES:
            continue
        ref = _utils._string_or_none(scenario.get("claim_id")) or source_ref
        _set_number(
            values,
            f"summary.scenario.{case}.probability",
            _utils._first_number(
                scenario.get("probability"),
                scenario.get("probability_weight"),
                scenario.get("weight"),
            ),
            "expected_value_artifact",
            ref,
        )


def _derive_expected_return(values: dict[str, WorkbookCommunicationSourceValue]) -> None:
    if "summary.expected_return" in values:
        return
    current = _utils._number_from_source(values.get("summary.current_price"))
    target = _utils._number_from_source(values.get("summary.blended_target_price"))
    if current is None or target is None or current == 0:
        return
    derived = WorkbookCommunicationSourceValue(
        value=(target / current) - 1.0,
        source_kind="derived_from_typed_artifact_values",
        source_ref=values["summary.blended_target_price"].source_ref,
    )
    values["summary.expected_return"] = derived
    values["valuation.expected_return"] = derived


def _iter_scenarios(scenario_root: Mapping[str, Any]) -> Iterable[tuple[str, Mapping[str, Any]]]:
    if all(isinstance(value, Mapping) for value in scenario_root.values()):
        for key, value in scenario_root.items():
            yield _scenario_case(key, value), value
        return
    scenario_list = scenario_root.get("cases") or scenario_root.get("scenarios")
    if isinstance(scenario_list, list):
        for item in scenario_list:
            if isinstance(item, Mapping):
                yield _scenario_case(item.get("case") or item.get("name"), item), item


def _field_from_mapping(mapping: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        if key in mapping:
            return mapping[key]
    return None


def _scenario_case(raw_case: Any, scenario: Mapping[str, Any]) -> str:
    candidate = raw_case or _field_from_mapping(scenario, "case", "name", "scenario")
    text = _utils._normalize_label(candidate)
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
    number = _utils._number_or_none(value)
    if number is None:
        return
    values[field] = WorkbookCommunicationSourceValue(
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
    "_merge_expected_value_values",
    "_merge_price_target_values",
    "_merge_scenario_values",
    "_merge_valuation_method_values",
    "_normalize_source_values",
    "_scenario_case",
    "_set_number",
    "source_values_from_artifacts",
]
