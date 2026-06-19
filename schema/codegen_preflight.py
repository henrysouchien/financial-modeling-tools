"""Preflight checks for standalone code generation."""

from __future__ import annotations

import sys
from typing import Any, Iterator, Optional, Tuple

from .models import FinancialModel, FormulaSpec, FormulaType, LineItem, LineItemRef
from .refs import line_item_ref_from_obj


def codegen_preflight_scan(model: FinancialModel) -> None:
    """Reject formula features unsupported by the standalone codegen path."""
    all_specs_with_paths = _compat("_all_specs_with_paths", _all_specs_with_paths)
    scan_formula_spec = _compat("_scan_formula_spec", _scan_formula_spec)
    for _sheet_name, _section_label, item in _iter_items(model):
        for path, spec in all_specs_with_paths(item):
            scan_formula_spec(spec, item.id, path)


def _compat(name: str, default: Any) -> Any:
    original = _ORIGINALS.get(name, default)
    parent = sys.modules.get("schema.codegen")
    if parent is None:
        return default
    value = getattr(parent, name, original)
    return value if value is not original else default


def _iter_items(model: FinancialModel) -> Iterator[Tuple[str, str, LineItem]]:
    for sheet_name, sheet in model.sheets.items():
        for section in sheet.sections:
            for item in section.line_items:
                yield sheet_name, section.label, item


def _all_specs_with_paths(item: LineItem) -> Iterator[Tuple[str, FormulaSpec]]:
    if item.historical is not None:
        yield "historical", item.historical
    if item.projected is not None:
        yield "projected", item.projected
    if item.overrides:
        for period, spec in item.overrides.items():
            if spec is not None:
                yield f"overrides[{int(period)}]", spec


def _scan_formula_spec(spec: Optional[FormulaSpec], item_id: str, path: str = "") -> None:
    if spec is None:
        return
    params = spec.params or {}
    if spec.type == FormulaType.arithmetic and params.get("function") == "SUM_RANGE":
        target_obj = params.get("target")
        target_ref = line_item_ref_from_obj(target_obj)
        if target_ref is None:
            raise NotImplementedError(
                f"SUM_RANGE in item {item_id!r} (path: {path or '<root>'}) has "
                f"malformed 'target' (expected LineItemRef-coercible; got "
                f"{type(target_obj).__name__}: {target_obj!r})."
            )
        raise NotImplementedError(
            f"SUM_RANGE in item {item_id!r} (path: {path or '<root>'}) is not "
            "supported in codegen-compute path. Use dependency_graph evaluator. "
            "See docs/design/dcf-horizon-adaptive-task.md."
        )
    _compat("_walk_params_for_unsupported", _walk_params_for_unsupported)(params, item_id, path)


def _walk_params_for_unsupported(obj: Any, item_id: str, path: str) -> None:
    if isinstance(obj, LineItemRef):
        if obj.period_anchor != "first":
            raise NotImplementedError(
                f"LineItemRef in item {item_id!r} (path: {path or '<root>'}) "
                f"uses period_anchor={obj.period_anchor!r}; only 'first' is "
                "supported in codegen-compute path."
            )
        return
    if isinstance(obj, FormulaSpec):
        _compat("_scan_formula_spec", _scan_formula_spec)(obj, item_id, path)
        return
    if isinstance(obj, dict):
        coerced_ref = line_item_ref_from_obj(obj)
        if coerced_ref is not None:
            _compat("_walk_params_for_unsupported", _walk_params_for_unsupported)(coerced_ref, item_id, path)
            return
        if "type" in obj and "params" in obj:
            try:
                coerced_spec = FormulaSpec.model_validate(obj)
            except Exception as exc:
                raise NotImplementedError(
                    f"Invalid FormulaSpec-shaped dict in item {item_id!r} "
                    f"(path: {path or '<root>'}): {exc}. Codegen preflight refuses to "
                    "guess intent for partially-valid formula specs."
                ) from exc
            _compat("_scan_formula_spec", _scan_formula_spec)(coerced_spec, item_id, path)
            return
        for key, value in obj.items():
            child_path = f"{path}.{key}" if path else str(key)
            _compat("_walk_params_for_unsupported", _walk_params_for_unsupported)(value, item_id, child_path)
        return
    if isinstance(obj, list):
        for index, value in enumerate(obj):
            _compat("_walk_params_for_unsupported", _walk_params_for_unsupported)(value, item_id, f"{path}[{index}]")


_ORIGINALS = {
    "_all_specs_with_paths": _all_specs_with_paths,
    "_scan_formula_spec": _scan_formula_spec,
    "_walk_params_for_unsupported": _walk_params_for_unsupported,
}

__all__ = [
    "codegen_preflight_scan",
]
