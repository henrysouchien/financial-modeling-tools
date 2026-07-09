"""Formula and reference helpers for segment expansion."""

from __future__ import annotations

from typing import Dict, List, Optional

from .models import FormulaSpec, FormulaType, LineItemRef

_SCENARIO_SELECTOR_ID = "tpl.a.header.scenario_value"
_SCENARIO_VOLUME_GROWTH_LABEL_ID = "tpl.a.scenario_tables.scenario_volume_growth_label"
_COLUMN_OFFSET_MODE_PERIOD_RELATIVE = "period_relative"


def _ref_target_id(spec: FormulaSpec) -> Optional[str]:
    if spec.type != FormulaType.ref:
        return None

    source = spec.params.get("source")
    if isinstance(source, LineItemRef):
        return source.id
    if isinstance(source, dict):
        return source.get("id")
    return None


def _rewrite_refs(obj, replacer):
    if isinstance(obj, LineItemRef):
        replacement = replacer({"id": obj.id, "t": obj.t, "resolved": obj.resolved})
        if replacement is not None:
            return replacement, True
        return obj, False

    if isinstance(obj, dict):
        if "id" in obj and set(obj.keys()) >= {"id"}:
            replacement = replacer(obj)
            if replacement is not None:
                return replacement, True
            return obj, False

        changed = False
        new_obj = {}
        for key, value in obj.items():
            new_value, updated = _rewrite_refs(value, replacer)
            new_obj[key] = new_value
            changed = changed or updated
        return new_obj, changed

    if isinstance(obj, list):
        changed = False
        new_list = []
        for value in obj:
            new_value, updated = _rewrite_refs(value, replacer)
            new_list.append(new_value)
            changed = changed or updated
        return new_list, changed

    if isinstance(obj, tuple):
        changed = False
        new_values = []
        for value in obj:
            new_value, updated = _rewrite_refs(value, replacer)
            new_values.append(new_value)
            changed = changed or updated
        return tuple(new_values), changed

    return obj, False


def _carry_forward_ref(item_id: str) -> Dict[str, object]:
    return _line_ref_dict(item_id, -1)


def _line_ref_dict(item_id: str, t: int = 0) -> Dict[str, object]:
    return {"id": item_id, "t": int(t), "resolved": True}


def _ref_source_id(spec: FormulaSpec | None) -> str | None:
    if spec is None or spec.type is not FormulaType.ref:
        return None
    source = (spec.params or {}).get("source")
    if isinstance(source, LineItemRef):
        return source.id
    if isinstance(source, dict) and isinstance(source.get("id"), str):
        return str(source["id"])
    return None


def _ref_source_t(spec: FormulaSpec | None) -> int | None:
    if spec is None or spec.type is not FormulaType.ref:
        return None
    source = (spec.params or {}).get("source")
    if isinstance(source, LineItemRef):
        return int(source.t)
    if isinstance(source, dict):
        try:
            return int(source.get("t", 0))
        except (TypeError, ValueError):
            return None
    return None


def _is_carry_forward_formula(spec: FormulaSpec | None, item_id: str) -> bool:
    return _ref_source_id(spec) == item_id and _ref_source_t(spec) == -1


def _ref_formula(item_id: str, t: int = 0) -> FormulaSpec:
    return FormulaSpec(type=FormulaType.ref, params={"source": LineItemRef(id=item_id, t=int(t))})


def _carry_forward_formula(item_id: str) -> FormulaSpec:
    return _ref_formula(item_id, t=-1)


def _growth_formula(base_id: str, rate_id: str) -> FormulaSpec:
    return FormulaSpec(
        type=FormulaType.growth,
        params={
            "base": LineItemRef(id=base_id, t=-1),
            "rate": LineItemRef(id=rate_id),
        },
    )


def _yoy_formula(item_id: str) -> FormulaSpec:
    return FormulaSpec(
        type=FormulaType.ratio,
        subtype="yoy_growth",
        params={
            "numerator": LineItemRef(id=item_id),
            "denominator": LineItemRef(id=item_id, t=-1),
            "subtract_one": True,
        },
    )


def _ratio_formula(numerator_id: str, denominator_id: str) -> FormulaSpec:
    return FormulaSpec(
        type=FormulaType.ratio,
        params={
            "numerator": LineItemRef(id=numerator_id),
            "denominator": LineItemRef(id=denominator_id),
        },
    )


def _sum_or_ref_formula(refs: List[LineItemRef]) -> FormulaSpec:
    if len(refs) == 1:
        ref = refs[0]
        return FormulaSpec(type=FormulaType.ref, params={"source": ref})
    return FormulaSpec(type=FormulaType.arithmetic, params={"operands": ["+", *refs]})


def _offset_scenario_formula(
    *,
    anchor_id: str = _SCENARIO_VOLUME_GROWTH_LABEL_ID,
    selector_id: str = _SCENARIO_SELECTOR_ID,
) -> FormulaSpec:
    return FormulaSpec(
        type=FormulaType.valuation,
        subtype="offset_scenario",
        params={
            "anchor": LineItemRef(id=anchor_id),
            "selector": LineItemRef(id=selector_id),
            "column_offset_mode": _COLUMN_OFFSET_MODE_PERIOD_RELATIVE,
        },
    )


__all__ = [
    "_COLUMN_OFFSET_MODE_PERIOD_RELATIVE",
    "_SCENARIO_SELECTOR_ID",
    "_SCENARIO_VOLUME_GROWTH_LABEL_ID",
    "_carry_forward_formula",
    "_carry_forward_ref",
    "_growth_formula",
    "_line_ref_dict",
    "_offset_scenario_formula",
    "_ratio_formula",
    "_ref_formula",
    "_ref_source_id",
    "_ref_source_t",
    "_ref_target_id",
    "_rewrite_refs",
    "_is_carry_forward_formula",
    "_sum_or_ref_formula",
    "_yoy_formula",
]
