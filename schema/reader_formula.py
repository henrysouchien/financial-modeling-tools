from __future__ import annotations

from typing import Any, Dict, List, Optional, Set, Tuple

from .models import FormulaSpec, FormulaType, LineItemRef


def _is_expanded_shared_raw(spec: FormulaSpec) -> bool:
    return spec.note == "expanded_shared_raw"


def _choose_formula(
    formulas_by_year: Dict[int, FormulaSpec],
    years: List[int],
) -> Tuple[Optional[FormulaSpec], Dict[int, FormulaSpec]]:
    """Pick a representative FormulaSpec for the given year set."""
    if not formulas_by_year:
        return None, {}

    candidate_years = [year for year in years if year in formulas_by_year] if years else list(formulas_by_year.keys())
    if not candidate_years:
        return None, {}

    vote_years = [year for year in candidate_years if not _is_expanded_shared_raw(formulas_by_year[year])]
    if not vote_years:
        return None, {year: formulas_by_year[year] for year in candidate_years}

    signature_counts: Dict[Tuple[FormulaType, Optional[str], object], List[int]] = {}
    for year in vote_years:
        spec = formulas_by_year[year]
        signature = (spec.type, spec.subtype, _param_shape(spec.params))
        signature_counts.setdefault(signature, []).append(year)

    best_signature = None
    best_count = -1
    best_latest_year = -1
    for signature, sig_years in signature_counts.items():
        count = len(sig_years)
        latest_year = max(sig_years)
        if count > best_count or (count == best_count and latest_year > best_latest_year):
            best_signature = signature
            best_count = count
            best_latest_year = latest_year

    if best_signature is None:
        return None, {}

    representative_year = max(signature_counts[best_signature])
    representative_spec = formulas_by_year[representative_year]
    overrides = {}
    for year in candidate_years:
        spec = formulas_by_year[year]
        if year not in vote_years:
            overrides[year] = spec
            continue
        signature = (spec.type, spec.subtype, _param_shape(spec.params))
        if signature != best_signature:
            overrides[year] = spec

    return representative_spec, overrides


def _param_shape(value: Any) -> Any:
    """Normalize formula params into a hashable shape."""
    if isinstance(value, LineItemRef):
        return ("ref", value.id, value.t, value.resolved)
    if isinstance(value, bool):
        return ("bool", value)
    if isinstance(value, (int, float)):
        return ("num", round(float(value), 6))
    if value is None:
        return ("none",)
    if isinstance(value, str):
        return ("str", value)
    if isinstance(value, dict):
        if "op" in value and isinstance(value.get("args"), list):
            op = value.get("op")
            args = list(value.get("args", []))
            if op in {"+", "*", "SUM", "AVG"}:
                args = sorted(args, key=lambda arg: _shape_sort_key(_param_shape(arg)))
            normalized = dict(value)
            normalized["args"] = args
            return ("dict", tuple((key, _param_shape(normalized[key])) for key in sorted(normalized)))
        if value.get("function") in {"SUM", "AVERAGE"} and isinstance(value.get("items"), list):
            items = list(value.get("items", []))
            items = sorted(items, key=lambda item: _shape_sort_key(_param_shape(item)))
            normalized = dict(value)
            normalized["items"] = items
            return ("dict", tuple((key, _param_shape(normalized[key])) for key in sorted(normalized)))
        return ("dict", tuple((key, _param_shape(value[key])) for key in sorted(value)))
    if isinstance(value, set):
        items = sorted(value, key=lambda item: _shape_sort_key(_param_shape(item)))
        return ("list", tuple(_param_shape(item) for item in items))
    if isinstance(value, (list, tuple)):
        return ("list", tuple(_param_shape(item) for item in value))
    return ("type", type(value).__name__)


def _shape_sort_key(value: Any) -> str:
    return repr(value)


def _collect_refs(value: Any, refs: List[LineItemRef]) -> None:
    if isinstance(value, LineItemRef):
        refs.append(value)
        return
    if isinstance(value, dict):
        for v in value.values():
            _collect_refs(v, refs)
        return
    if isinstance(value, (list, tuple, set)):
        for v in value:
            _collect_refs(v, refs)


def _is_self_referencing(spec: Optional[FormulaSpec], line_item_id: str) -> bool:
    """Check if a formula references its own line item at t=0."""
    if not spec:
        return False
    refs: List[LineItemRef] = []
    _collect_refs(spec.params, refs)
    if not refs:
        return False
    return any(ref.id == line_item_id and ref.t == 0 for ref in refs)


def _dedup_additive_refs(spec: FormulaSpec) -> FormulaSpec:
    if spec.type != FormulaType.arithmetic:
        return spec

    params = spec.params if isinstance(spec.params, dict) else {}
    changed = False

    def _dedup_expr(expr: Any) -> Any:
        if not isinstance(expr, dict):
            return expr

        op = expr.get("op")
        if op is None:
            return expr

        if op in {"+", "SUM", "AVG", "AVERAGE"}:
            args = expr.get("args", [])
            if not isinstance(args, list):
                return expr
            new_args = []
            for arg in args:
                new_args.append(_dedup_expr(arg))

            seen: Set[Tuple[str, int]] = set()
            deduped = []
            for arg in new_args:
                if isinstance(arg, LineItemRef):
                    key = (arg.id, arg.t)
                    if key in seen:
                        continue
                    seen.add(key)
                deduped.append(arg)

            if deduped != args or any(new is not old for new, old in zip(new_args, args)):
                return {"op": op, "args": deduped}
            return expr

        if op in {"-", "/", "^"}:
            left = expr.get("left")
            right = expr.get("right")
            new_left = _dedup_expr(left)
            new_right = _dedup_expr(right)
            if new_left is not left or new_right is not right:
                return {"op": op, "left": new_left, "right": new_right}
            return expr

        if op == "NEG":
            arg = expr.get("arg")
            new_arg = _dedup_expr(arg)
            if new_arg is not arg:
                return {"op": "NEG", "arg": new_arg}
            return expr

        if op == "*":
            args = expr.get("args", [])
            if not isinstance(args, list):
                return expr
            new_args = [_dedup_expr(arg) for arg in args]
            if any(new is not old for new, old in zip(new_args, args)):
                return {"op": "*", "args": new_args}
            return expr

        return expr

    def _dedup_items(items: object) -> object:
        nonlocal changed
        if not isinstance(items, list):
            return items

        seen: Set[Tuple[str, int]] = set()
        deduped = []
        for item in items:
            new_item = _dedup_expr(item) if isinstance(item, dict) else item
            if isinstance(new_item, LineItemRef):
                key = (new_item.id, new_item.t)
                if key in seen:
                    changed = True
                    continue
                seen.add(key)
            if new_item is not item:
                changed = True
            deduped.append(new_item)
        if len(deduped) != len(items):
            changed = True
        return deduped

    new_params = params

    function = params.get("function")
    if isinstance(function, str) and function.upper() in {"SUM", "AVERAGE", "AVG"} and "items" in params:
        new_items = _dedup_items(params.get("items"))
        if new_items is not params.get("items"):
            new_params = dict(params)
            new_params["items"] = new_items
    elif "items" in params and "function" not in params:
        new_items = _dedup_items(params.get("items"))
        if new_items is not params.get("items"):
            new_params = dict(params)
            new_params["items"] = new_items
    elif "operands" in params:
        operands = params.get("operands")
        if isinstance(operands, list) and operands:
            operator = operands[0]
            if operator == "+":
                seen: Set[Tuple[str, int]] = set()
                new_operands: List[object] = [operator]
                for operand in operands[1:]:
                    if isinstance(operand, LineItemRef):
                        key = (operand.id, operand.t)
                        if key in seen:
                            changed = True
                            continue
                        seen.add(key)
                    new_operands.append(operand)
                if len(new_operands) != len(operands):
                    changed = True
                if changed:
                    new_params = dict(params)
                    new_params["operands"] = new_operands
    elif "expr" in params:
        expr = params.get("expr")
        new_expr = _dedup_expr(expr)
        if new_expr is not expr:
            changed = True
            new_params = dict(params)
            new_params["expr"] = new_expr

    if not changed:
        return spec
    return FormulaSpec(
        type=spec.type,
        subtype=spec.subtype,
        params=new_params,
        note=spec.note,
    )


__all__ = [
    "_choose_formula",
    "_collect_refs",
    "_dedup_additive_refs",
    "_is_expanded_shared_raw",
    "_is_self_referencing",
    "_param_shape",
    "_shape_sort_key",
]
