"""Value, formula, and coverage helpers for build diagnostics."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Optional

from .models import (
    DataSourceMapping,
    FinancialModel,
    FormulaSpec,
    FormulaType,
    LineItem,
    LineItemRef,
    shift_period,
)

VALID_SEVERITIES = {"ok", "gap", "material_gap", "inconsistency"}
SEVERITY_ORDER = {
    "ok": 0,
    "gap": 1,
    "material_gap": 2,
    "inconsistency": 3,
}


def _is_synthetic_override(spec: FormulaSpec) -> bool:
    return spec.type is FormulaType.constant and spec.note == "synthetic"


def _iter_items(model: FinancialModel) -> Iterable[LineItem]:
    for sheet in model.sheets.values():
        for section in sheet.sections:
            yield from section.line_items


def _section_lookup(model: FinancialModel) -> dict[str, str]:
    lookup: dict[str, str] = {}
    for sheet in model.sheets.values():
        for section in sheet.sections:
            for item in section.line_items:
                lookup[item.id] = section.id
    return lookup


def _historical_years(model: FinancialModel) -> list[int]:
    periods = (
        model.time_structure.historical_periods or model.time_structure.historical_years
    )
    return sorted(int(period) for period in periods)


def _iso_utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _collect_severities(obj: Any) -> Iterable[str]:
    if isinstance(obj, dict):
        severity = obj.get("severity")
        if severity in VALID_SEVERITIES:
            yield severity
        for value in obj.values():
            yield from _collect_severities(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from _collect_severities(value)


def _max_tolerance(base: float, abs_floor: float, pct_floor: float) -> float:
    return max(float(abs_floor), float(pct_floor) * abs(float(base)))


def _percent(delta: float | None, base: float | None) -> float | None:
    if delta is None or base is None:
        return None
    return _ratio(delta, base) * 100.0


def _ratio(delta: float, base: float) -> float:
    denominator = abs(float(base))
    if denominator < 1e-9:
        return 0.0 if abs(float(delta)) < 1e-9 else 1.0
    return abs(float(delta)) / denominator


def _constant_override_value(spec: FormulaSpec | None) -> Optional[float]:
    if spec is None or spec.type is not FormulaType.constant:
        return None
    value = spec.params.get("value")
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _has_real_value(item: LineItem, period: int) -> bool:
    if item.values is not None and int(period) in item.values.values:
        cell = item.values.values[int(period)]
        if cell.value is not None:
            return True
    if item.overrides is not None and int(period) in item.overrides:
        spec = item.overrides[int(period)]
        if (
            not _is_synthetic_override(spec)
            and _constant_override_value(spec) is not None
        ):
            return True
    return False


def _observed_value(
    model: FinancialModel,
    item: LineItem,
    period: int,
    memo: dict[tuple[str, int], Optional[float]],
    stack: set[tuple[str, int]] | None = None,
) -> Optional[float]:
    key = (item.id, int(period))
    if key in memo:
        return memo[key]

    if item.overrides is not None and int(period) in item.overrides:
        override_value = _constant_override_value(item.overrides[int(period)])
        if override_value is not None:
            memo[key] = override_value
            return override_value
    if item.values is not None and int(period) in item.values.values:
        value_cell = item.values.values[int(period)]
        if value_cell.value is not None:
            memo[key] = float(value_cell.value)
            return memo[key]

    spec = _historical_spec(item, int(period))
    if spec is None:
        memo[key] = None
        return None

    active_stack = stack if stack is not None else set()
    if key in active_stack:
        return None
    active_stack.add(key)
    value = _evaluate_formula_spec(
        model,
        spec,
        int(period),
        memo,
        stack=active_stack,
        current_item_id=item.id,
    )
    active_stack.remove(key)
    memo[key] = value
    return value


def _historical_spec(item: LineItem, period: int) -> FormulaSpec | None:
    if item.historical is None:
        return None
    if item.formula_periods is None:
        return item.historical
    active_periods = {int(active_period) for active_period in item.formula_periods}
    if int(period) in active_periods:
        return item.historical
    return None


def _evaluate_formula_spec(
    model: FinancialModel,
    spec: FormulaSpec | None,
    period: int,
    memo: dict[tuple[str, int], Optional[float]],
    *,
    stack: set[tuple[str, int]] | None = None,
    current_item_id: str | None = None,
) -> Optional[float]:
    if spec is None:
        return None
    params = spec.params or {}

    if spec.type is FormulaType.constant:
        return _constant_override_value(spec)
    if spec.type is FormulaType.ref:
        value = _evaluate_expr(model, params.get("source"), period, memo, stack=stack)
        if value is None:
            return None
        adjustment = params.get("adjustment")
        if adjustment is not None:
            value += float(adjustment)
        if params.get("negate"):
            value = -value
        return value
    if spec.type is FormulaType.arithmetic:
        if "expr" in params:
            return _evaluate_expr(model, params.get("expr"), period, memo, stack=stack)
        function = params.get("function")
        if function in {"SUM", "AVERAGE"}:
            values = [
                _evaluate_expr(model, expr, period, memo, stack=stack)
                for expr in list(params.get("items", []) or [])
            ]
            if any(value is None for value in values):
                return None
            if not values:
                return 0.0
            return sum(values) / len(values) if function == "AVERAGE" else sum(values)
        operands = params.get("operands")
        if isinstance(operands, list) and operands:
            args = list(operands)
            operator = "+"
            if isinstance(args[0], str) and args[0] in {"+", "-", "*", "/"}:
                operator = args.pop(0)
            values = [
                _evaluate_expr(model, expr, period, memo, stack=stack) for expr in args
            ]
            if any(value is None for value in values):
                return None
            if not values:
                return 0.0
            if operator == "+":
                return sum(values)
            if operator == "-":
                result = values[0]
                for value in values[1:]:
                    result -= value
                return result
            if operator == "*":
                result = 1.0
                for value in values:
                    result *= value
                return result
            if operator == "/":
                result = values[0]
                for value in values[1:]:
                    if abs(value) < 1e-12:
                        return None
                    result /= value
                return result
    if spec.type is FormulaType.ratio:
        numerator = _evaluate_expr(
            model, params.get("numerator"), period, memo, stack=stack
        )
        denominator = _evaluate_expr(
            model, params.get("denominator"), period, memo, stack=stack
        )
        if numerator is None or denominator is None or abs(denominator) < 1e-12:
            return None
        result = numerator / denominator
        if params.get("subtract_one"):
            result -= 1
        return result
    if spec.type is FormulaType.growth:
        base = _evaluate_expr(model, params.get("base"), period, memo, stack=stack)
        rate = _evaluate_expr(model, params.get("rate"), period, memo, stack=stack)
        if base is None or rate is None:
            return None
        return base * (1.0 + rate)
    if spec.type is FormulaType.driver:
        base = _evaluate_expr(model, params.get("base"), period, memo, stack=stack)
        rate = _evaluate_expr(model, params.get("rate"), period, memo, stack=stack)
        if base is None or rate is None:
            return None
        scale = float(params.get("scale", 1.0))
        return base * rate * scale
    return None


def _evaluate_expr(
    model: FinancialModel,
    expr: Any,
    period: int,
    memo: dict[tuple[str, int], Optional[float]],
    *,
    stack: set[tuple[str, int]] | None = None,
) -> Optional[float]:
    if expr is None:
        return None
    if isinstance(expr, bool):
        return float(int(expr))
    if isinstance(expr, (int, float)):
        return float(expr)
    if isinstance(expr, LineItemRef):
        shifted = shift_period(
            int(period), int(expr.t), model.time_structure.period_mode
        )
        if shifted is None:
            return None
        try:
            return _observed_value(
                model, model.get_item(expr.id), int(shifted), memo, stack
            )
        except KeyError:
            return None
    if isinstance(expr, dict):
        if "id" in expr and isinstance(expr["id"], str):
            try:
                ref_t = int(expr.get("t", 0))
            except (TypeError, ValueError):
                ref_t = 0
            shifted = shift_period(int(period), ref_t, model.time_structure.period_mode)
            if shifted is None:
                return None
            try:
                return _observed_value(
                    model, model.get_item(expr["id"]), int(shifted), memo, stack
                )
            except KeyError:
                return None
        op = expr.get("op")
        args = list(expr.get("args", []) or [])
        if op in {"SUM", "AVERAGE"}:
            values = [
                _evaluate_expr(model, arg, period, memo, stack=stack) for arg in args
            ]
            if any(value is None for value in values):
                return None
            if not values:
                return 0.0
            return sum(values) / len(values) if op == "AVERAGE" else sum(values)
        if op == "+":
            values = [
                _evaluate_expr(model, arg, period, memo, stack=stack) for arg in args
            ]
            if any(value is None for value in values):
                return None
            return sum(values)
        if op == "*":
            values = [
                _evaluate_expr(model, arg, period, memo, stack=stack) for arg in args
            ]
            if any(value is None for value in values):
                return None
            result = 1.0
            for value in values:
                result *= value
            return result
        if op == "-":
            left = _evaluate_expr(model, expr.get("left"), period, memo, stack=stack)
            right = _evaluate_expr(model, expr.get("right"), period, memo, stack=stack)
            if left is None or right is None:
                return None
            return left - right
        if op == "/":
            left = _evaluate_expr(model, expr.get("left"), period, memo, stack=stack)
            right = _evaluate_expr(model, expr.get("right"), period, memo, stack=stack)
            if left is None or right is None or abs(right) < 1e-12:
                return None
            return left / right
        if op == "NEG":
            value = _evaluate_expr(model, expr.get("arg"), period, memo, stack=stack)
            if value is None:
                return None
            return -value
    return None


def _extract_ref_ids(obj: Any) -> set[str]:
    ids: set[str] = set()
    if obj is None:
        return ids
    if isinstance(obj, LineItemRef):
        return {obj.id}
    if isinstance(obj, dict):
        if "id" in obj and isinstance(obj["id"], str):
            return {obj["id"]}
        for value in obj.values():
            ids |= _extract_ref_ids(value)
        return ids
    if isinstance(obj, (list, tuple)):
        for value in obj:
            ids |= _extract_ref_ids(value)
    return ids


def _extract_ref_targets(
    obj: Any,
    *,
    period: int,
    mode: str,
) -> list[tuple[str, int | None]]:
    targets: list[tuple[str, int | None]] = []
    if obj is None:
        return targets
    if isinstance(obj, LineItemRef):
        shifted = shift_period(int(period), int(obj.t), mode)
        targets.append((obj.id, None if shifted is None else int(shifted)))
        return targets
    if isinstance(obj, dict):
        if "id" in obj and isinstance(obj["id"], str):
            try:
                ref_t = int(obj.get("t", 0))
            except (TypeError, ValueError):
                ref_t = 0
            shifted = shift_period(int(period), ref_t, mode)
            targets.append((obj["id"], None if shifted is None else int(shifted)))
            return targets
        for value in obj.values():
            targets.extend(_extract_ref_targets(value, period=period, mode=mode))
        return targets
    if isinstance(obj, (list, tuple)):
        for value in obj:
            targets.extend(_extract_ref_targets(value, period=period, mode=mode))
    return targets


def _missing_ref_ids(
    model: FinancialModel,
    item: LineItem,
    period: int,
    memo: dict[tuple[str, int], Optional[float]],
) -> list[str]:
    missing: list[str] = []
    for ref_id in sorted(
        _extract_ref_ids(item.historical.params if item.historical else None)
    ):
        try:
            ref_item = model.get_item(ref_id)
        except KeyError:
            missing.append(ref_id)
            continue
        if _observed_value(model, ref_item, period, memo) is None:
            missing.append(ref_id)
    return missing


def _build_reverse_dependency_graph(model: FinancialModel) -> dict[str, set[str]]:
    reverse_graph: dict[str, set[str]] = {}
    for item in _iter_items(model):
        refs = _extract_ref_ids(item.historical.params if item.historical else None)
        refs |= _extract_ref_ids(item.projected.params if item.projected else None)
        for ref_id in refs:
            reverse_graph.setdefault(ref_id, set()).add(item.id)
    return reverse_graph


def _transitive_downstream_ids(
    item_id: str,
    reverse_graph: dict[str, set[str]],
) -> set[str]:
    discovered: set[str] = set()
    frontier = sorted(reverse_graph.get(item_id, set()))
    while frontier:
        current = frontier.pop(0)
        if current in discovered:
            continue
        discovered.add(current)
        frontier.extend(sorted(reverse_graph.get(current, set()) - discovered))
    return discovered


def _concept_item_map(model: FinancialModel) -> dict[str, list[LineItem]]:
    concept_items: dict[str, list[LineItem]] = {}
    for item in _iter_items(model):
        if item.data_concept_id:
            concept_items.setdefault(item.data_concept_id, []).append(item)
    return concept_items


def _concept_has_coverage(
    model: FinancialModel,
    items: list[LineItem],
    historical_years: list[int],
    derivable_items: dict[str, set[int]],
    path_memo: dict[tuple[str, int], bool],
    value_memo: dict[tuple[str, int], Optional[float]],
) -> bool:
    for item in items:
        for year in historical_years:
            if _item_has_coverage_for_year(
                model,
                item,
                year,
                derivable_items,
                path_memo,
                value_memo,
            ):
                return True
    return False


def _concept_has_full_coverage(
    model: FinancialModel,
    items: list[LineItem],
    historical_years: list[int],
    derivable_items: dict[str, set[int]],
    path_memo: dict[tuple[str, int], bool],
    value_memo: dict[tuple[str, int], Optional[float]],
) -> bool:
    if not items or not historical_years:
        return False
    for year in historical_years:
        if not any(
            _item_has_coverage_for_year(
                model,
                item,
                year,
                derivable_items,
                path_memo,
                value_memo,
            )
            for item in items
        ):
            return False
    return True


def _item_has_coverage_for_year(
    model: FinancialModel,
    item: LineItem,
    year: int,
    derivable_items: dict[str, set[int]],
    path_memo: dict[tuple[str, int], bool],
    value_memo: dict[tuple[str, int], Optional[float]],
) -> bool:
    if _has_real_value(item, year):
        return True
    if year in derivable_items.get(item.id, set()):
        return True
    if _observed_value(model, item, year, value_memo) is not None and not (
        item.overrides is not None
        and year in item.overrides
        and _is_synthetic_override(item.overrides[year])
    ):
        return True
    return _item_has_path(model, item, year, derivable_items, path_memo, set())


def _intentional_blank_detail(
    model: FinancialModel,
    *,
    mapping: DataSourceMapping,
    concept_items: dict[str, list[LineItem]],
    years: list[int],
    derivable_items: dict[str, set[int]],
    path_memo: dict[tuple[str, int], bool],
    value_memo: dict[tuple[str, int], Optional[float]],
) -> dict[str, Any] | None:
    covered_by = [
        cover_concept_id
        for cover_concept_id in list(mapping.missing_ok_when_covered_by or [])
        if _concept_has_full_coverage(
            model,
            concept_items.get(cover_concept_id, []),
            years,
            derivable_items,
            path_memo,
            value_memo,
        )
    ]
    if covered_by:
        return {
            "status": "covered_by_concept",
            "severity": "ok",
            "kind": "covered_by_concept",
            "covered_by": covered_by,
        }
    if mapping.optional_if_unreported:
        return {
            "status": "optional_unreported",
            "severity": "ok",
            "kind": "optional_unreported",
        }
    return None


def _projection_only_historical_blank_detail(
    item: LineItem,
    historical_years: list[int],
) -> dict[str, Any] | None:
    if (
        item.data_concept_id is not None
        or item.historical is not None
        or item.projected is None
        or item.formula_periods is None
    ):
        return None

    historical_periods = {int(year) for year in historical_years}
    formula_periods = {int(period) for period in item.formula_periods}
    if not formula_periods or formula_periods & historical_periods:
        return None

    return {
        "status": "projection_only",
        "severity": "ok",
        "kind": "projection_only",
        "formula_periods": sorted(formula_periods),
    }


def _item_has_path(
    model: FinancialModel,
    item: LineItem,
    year: int,
    derivable_items: dict[str, set[int]],
    memo: dict[tuple[str, int], bool],
    stack: set[tuple[str, int]],
) -> bool:
    key = (item.id, int(year))
    if key in memo:
        return memo[key]
    if key in stack:
        return False
    if int(year) in derivable_items.get(item.id, set()):
        memo[key] = True
        return True
    if _has_real_value(item, int(year)):
        memo[key] = True
        return True
    if (
        item.overrides is not None
        and int(year) in item.overrides
        and _is_synthetic_override(item.overrides[int(year)])
    ):
        memo[key] = False
        return False
    spec = _historical_spec(item, int(year))
    if spec is None:
        memo[key] = False
        return False
    if spec.type is FormulaType.constant:
        memo[key] = _constant_override_value(spec) is not None
        return memo[key]

    targets = _extract_ref_targets(
        spec.params, period=int(year), mode=model.time_structure.period_mode
    )
    if not targets:
        memo[key] = True
        return True

    stack.add(key)
    result = True
    for ref_id, ref_year in targets:
        if ref_year is None:
            result = False
            break
        try:
            ref_item = model.get_item(ref_id)
        except KeyError:
            result = False
            break
        if not _item_has_path(
            model, ref_item, int(ref_year), derivable_items, memo, stack
        ):
            result = False
            break
    stack.remove(key)
    memo[key] = result
    return result


def _blocking_ref_ids(
    model: FinancialModel,
    item: LineItem,
    historical_years: list[int],
    value_memo: dict[tuple[str, int], Optional[float]],
) -> list[str]:
    blocked: set[str] = set()
    for year in historical_years:
        for ref_id in _extract_ref_ids(
            item.historical.params if item.historical else None
        ):
            try:
                ref_item = model.get_item(ref_id)
            except KeyError:
                blocked.add(ref_id)
                continue
            if _observed_value(model, ref_item, year, value_memo) is None:
                blocked.add(ref_id)
    return sorted(blocked)


def _is_edgar_sourced(mapping: DataSourceMapping) -> bool:
    return bool(
        mapping.edgar_tags or mapping.registry_group_id or mapping.canonical_tag
    )


__all__ = [
    "VALID_SEVERITIES",
    "SEVERITY_ORDER",
    "_is_synthetic_override",
    "_iter_items",
    "_section_lookup",
    "_historical_years",
    "_iso_utc_now",
    "_collect_severities",
    "_max_tolerance",
    "_percent",
    "_ratio",
    "_constant_override_value",
    "_has_real_value",
    "_observed_value",
    "_historical_spec",
    "_evaluate_formula_spec",
    "_evaluate_expr",
    "_extract_ref_ids",
    "_extract_ref_targets",
    "_missing_ref_ids",
    "_build_reverse_dependency_graph",
    "_transitive_downstream_ids",
    "_concept_item_map",
    "_concept_has_coverage",
    "_concept_has_full_coverage",
    "_item_has_coverage_for_year",
    "_intentional_blank_detail",
    "_projection_only_historical_blank_detail",
    "_item_has_path",
    "_blocking_ref_ids",
    "_is_edgar_sourced",
]
