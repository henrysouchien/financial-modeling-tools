"""Excel formula compilation helpers for schema rendering.

The compiler preserves authored formulas. LibreOffice evaluates the rendered
workbook at publication; function coverage is not a renderer registry.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import re
from typing import Any, Dict, Iterable, Literal, Optional, TypeGuard

from .dependency_graph_helpers import _offset_column
from .driver_formula import driver_formula_params
from .renderer_columns import (
    AbsoluteColumnMapper,
    _fixed_cell_anchor_period,
    _historical_periods,
    _projection_periods,
    _selector_column,
)
from .models import (
    COLUMN_OFFSET_MODE_PERIOD_RELATIVE as _COLUMN_OFFSET_MODE_PERIOD_RELATIVE,
    FinancialModel,
    FormulaSpec,
    FormulaType,
    LineItem,
    LineItemRef,
)
from .refs import line_item_ref_from_obj
from .presentation_captions import item_renders_text


_LOGGER = logging.getLogger(__name__)
_SCENARIO_TABLE_ROW_COUNT = 3


@dataclass(frozen=True)
class _CompiledExcelExpr:
    """Excel expression paired with the predicate that says it has a value.

    ``present=None`` means the expression is always present.  Keeping presence
    separate from the value prevents Excel's implicit blank-to-zero coercion
    from changing the dependency graph's missing-value semantics.
    """

    value: str
    present: Optional[str] = None

    def materialize(self, *, missing: str = '""') -> str:
        if self.present is None:
            return self.value
        return f"IF({self.present},{self.value},{missing})"


def _all_present(
    expressions: Iterable[_CompiledExcelExpr], *extra: str
) -> Optional[str]:
    predicates = [expr.present for expr in expressions if expr.present is not None]
    predicates.extend(predicate for predicate in extra if predicate)
    if not predicates:
        return None
    if len(predicates) == 1:
        return predicates[0]
    return f"AND({','.join(predicates)})"


def _scenario_refs_are_present(
    refs: dict[str, LineItemRef | None],
) -> bool:
    return refs.get("selector") is not None


def _formula_from_expr(
    expr: _CompiledExcelExpr, *, blank_if_error: bool = False
) -> str:
    # FormulaSpec does not carry authenticated optional-edge or fallback
    # authority.  A missing required operand must therefore remain visible in
    # Excel instead of being blank-suppressed into an apparently ordinary cell.
    formula = expr.materialize(missing="NA()")
    return f"={formula}"


class ExcelFormulaCompiler:
    """Compile schema FormulaSpecs into Excel formulas."""

    def __init__(
        self,
        model: FinancialModel,
        column_mapper: AbsoluteColumnMapper | Dict[str, AbsoluteColumnMapper],
        current_sheet: str,
    ) -> None:
        self._model = model
        self._current_sheet = current_sheet
        if isinstance(column_mapper, dict):
            self._mappers = column_mapper
            self._col = column_mapper[current_sheet]
        else:
            self._col = column_mapper
            self._mappers = {sheet_name: column_mapper for sheet_name in model.sheets}
        self._item_row_map: Dict[str, int] = {}
        self._item_sheet_map: Dict[str, str] = {}
        self._item_by_id: Dict[str, LineItem] = {}

        for sheet_name, sheet in model.sheets.items():
            for section in sheet.sections:
                for item in section.line_items:
                    self._item_row_map[item.id] = int(item.row)
                    self._item_sheet_map[item.id] = sheet_name
                    self._item_by_id[item.id] = item

    def compile_formula(
        self,
        spec: Optional[FormulaSpec],
        *,
        period: int,
        item_id: Optional[str] = None,
    ) -> Any:
        if spec is None:
            return None

        params = spec.params or {}

        if spec.type == FormulaType.constant:
            return _normalize_literal(params.get("value"))

        if spec.type == FormulaType.ref:
            source = self._compile_semantic_expr(params.get("source"), period=period)
            if source is None:
                return None
            value = source.value
            adjustment = params.get("adjustment")
            if adjustment is not None:
                value = _apply_adjustment(value, adjustment)
            if params.get("negate"):
                value = f"-({value})" if adjustment is not None else f"-{value}"
            return _formula_from_expr(_CompiledExcelExpr(value, source.present))

        if spec.type == FormulaType.arithmetic:
            if "expr" in params:
                expr = self._compile_semantic_expr(params.get("expr"), period=period)
                if expr is None:
                    return None
                return _formula_from_expr(
                    expr, blank_if_error=bool(params.get("blank_if_error"))
                )

            function = params.get("function")
            if function == "SUM_RANGE":
                target_obj = params.get("target")
                target_ref = line_item_ref_from_obj(target_obj)
                if target_ref is None:
                    _LOGGER.warning(
                        "SUM_RANGE in %r has a target that is not a line-item "
                        "ref (%s: %r); the cell is left unresolved",
                        item_id,
                        type(target_obj).__name__,
                        target_obj,
                    )
                    return None
                if target_ref.id not in self._item_row_map:
                    raise KeyError(
                        f"SUM_RANGE target {target_ref.id!r} not found in model index"
                    )

                target_item = self._item_by_id[target_ref.id]
                target_sheet = self._item_sheet_map[target_ref.id]
                target_mapper = self._mappers.get(target_sheet, self._col)
                target_row = self._item_row_map[target_ref.id]

                if target_item.column is not None:
                    cell = self._resolve_ref(
                        target_ref.id,
                        target_ref.t,
                        period,
                        period_anchor=target_ref.period_anchor,
                    )
                    return _formula_from_expr(
                        _CompiledExcelExpr(cell, f"ISNUMBER({cell})")
                    )

                periods = list(target_mapper.all_periods())
                if not periods:
                    return None
                first_col = target_mapper.col_for_period(periods[0])
                last_col = target_mapper.col_for_period(periods[-1])
                cell_range = f"{first_col}{target_row}:{last_col}{target_row}"
                if target_sheet != self._current_sheet:
                    cell_range = f"{_quote_sheet_name(target_sheet)}!{cell_range}"
                return _formula_from_expr(
                    _CompiledExcelExpr(
                        value=f"SUM({cell_range})",
                        present=f"COUNT({cell_range})={len(periods)}",
                    )
                )

            if function in {"SUM", "AVERAGE", "MEDIAN"}:
                items = list(params.get("items", []) or [])
                compiled = [
                    expr
                    for item in items
                    if (expr := self._compile_semantic_expr(item, period=period))
                    is not None
                ]
                if not compiled:
                    return None
                if function == "AVERAGE":
                    formula = f"AVERAGE({','.join(expr.value for expr in compiled)})"
                elif function == "MEDIAN":
                    formula = f"MEDIAN({','.join(expr.value for expr in compiled)})"
                else:
                    formula = f"SUM({','.join(expr.value for expr in compiled)})"
                return _formula_from_expr(
                    _CompiledExcelExpr(formula, _all_present(compiled)),
                    blank_if_error=bool(params.get("blank_if_error")),
                )

            operands = params.get("operands")
            if isinstance(operands, list) and operands:
                operator = "+"
                args = list(operands)
                if isinstance(args[0], str) and args[0] in {"+", "-", "*", "/"}:
                    operator = args.pop(0)
                arg_pairs = []
                for arg in args:
                    compiled_arg = self._compile_semantic_expr(arg, period=period)
                    if compiled_arg is not None:
                        arg_pairs.append((arg, compiled_arg))
                compiled = [compiled_arg for _arg, compiled_arg in arg_pairs]
                if not compiled:
                    return None
                if operator == "+":
                    value = f"SUM({','.join(expr.value for expr in compiled)})"
                    present = _all_present(compiled)
                else:
                    values = [
                        _wrap_if_operator_expr(arg, expr.value)
                        for arg, expr in arg_pairs
                    ]
                    value = operator.join(values)
                    extras = (
                        tuple(f"{value}<>0" for value in values[1:])
                        if operator == "/"
                        else ()
                    )
                    present = _all_present(compiled, *extras)
                return _formula_from_expr(_CompiledExcelExpr(value, present))

            items = list(params.get("items", []) or [])
            compiled = [
                expr
                for item in items
                if (expr := self._compile_semantic_expr(item, period=period))
                is not None
            ]
            if not compiled:
                return None
            value = f"SUM({','.join(expr.value for expr in compiled)})"
            return _formula_from_expr(_CompiledExcelExpr(value, _all_present(compiled)))

        if spec.type == FormulaType.driver:
            driver = driver_formula_params(params)
            base = self._compile_semantic_expr(driver.base, period=period)
            rate = self._compile_semantic_expr(driver.rate, period=period)
            if base is None or rate is None:
                return None
            base_expr = _wrap_if_operator_expr(driver.base, base.value)
            rate_expr = _wrap_if_operator_expr(driver.rate, rate.value)
            formula = f"{base_expr}*{rate_expr}"
            for operation in driver.scale_operations():
                operator = "/" if operation.operator == "divide" else "*"
                formula = f"{formula}{operator}{_excel_number_literal(operation.factor)}"
            return _formula_from_expr(
                _CompiledExcelExpr(formula, _all_present([base, rate]))
            )

        if spec.type == FormulaType.ratio:
            numerator = self._compile_semantic_expr(
                params.get("numerator"), period=period
            )
            denominator = self._compile_semantic_expr(
                params.get("denominator"), period=period
            )
            if numerator is None or denominator is None:
                return None
            numerator_expr = _wrap_if_operator_expr(
                params.get("numerator"), numerator.value
            )
            denominator_expr = _wrap_if_operator_expr(
                params.get("denominator"), denominator.value
            )
            formula = f"{numerator_expr}/{denominator_expr}"
            if params.get("subtract_one"):
                formula = f"{formula}-1"
            return _formula_from_expr(
                _CompiledExcelExpr(
                    formula,
                    _all_present([numerator, denominator], f"{denominator_expr}<>0"),
                )
            )

        if spec.type == FormulaType.growth:
            base = self._compile_semantic_expr(params.get("base"), period=period)
            rate = self._compile_semantic_expr(params.get("rate"), period=period)
            if base is None or rate is None:
                return None
            base_expr = _wrap_if_operator_expr(params.get("base"), base.value)
            return _formula_from_expr(
                _CompiledExcelExpr(
                    f"{base_expr}*(1+{rate.value})",
                    _all_present([base, rate]),
                )
            )

        if spec.type == FormulaType.roll_forward:
            beginning = self._compile_semantic_expr(
                params.get("beginning"), period=period
            )
            if beginning is None:
                return None
            additions = [
                expr
                for addition in list(params.get("additions", []) or [])
                if (expr := self._compile_semantic_expr(addition, period=period))
                is not None
            ]
            subtractions = [
                expr
                for subtraction in list(params.get("subtractions", []) or [])
                if (expr := self._compile_semantic_expr(subtraction, period=period))
                is not None
            ]
            value = beginning.value
            if additions:
                value = f"{value}+SUM({','.join(expr.value for expr in additions)})"
            if subtractions:
                value = f"{value}-SUM({','.join(expr.value for expr in subtractions)})"
            return _formula_from_expr(
                _CompiledExcelExpr(
                    value,
                    _all_present([beginning, *additions, *subtractions]),
                )
            )

        if spec.type == FormulaType.valuation:
            if spec.subtype == "scenario_select":
                return self._compile_scenario_select(item_id, spec, period)
            if spec.subtype == "offset_scenario":
                return self._compile_offset_scenario(item_id, spec, period)
            if spec.subtype == "dcf_discount":
                return self._compile_valuation_dcf_discount(spec, period)
            if spec.subtype == "terminal_value":
                return self._compile_valuation_terminal_value(spec, period)
            if spec.subtype == "capm":
                return self._compile_valuation_capm(spec, period)
            if spec.subtype == "wacc":
                return self._compile_valuation_wacc(spec, period)
            if spec.subtype == "multiple":
                return self._compile_valuation_multiple(spec, period)
            if spec.subtype == "probability_weighted":
                return self._compile_valuation_probability_weighted(spec, period)
            if spec.subtype == "kelly":
                return self._compile_valuation_kelly(spec, period)
            if spec.subtype == "break_even_probability":
                return self._compile_valuation_break_even_probability(spec, period)
            return None

        if spec.type == FormulaType.raw:
            formula = str(params.get("formula") or "").strip()
            if not formula:
                return None
            return formula if formula.startswith("=") else f"={formula}"

        return None

    def _compile_scenario_select(
        self,
        item_id: Optional[str],
        spec: FormulaSpec,
        period: int,
    ) -> Optional[str]:
        params = spec.params or {}
        refs = {
            name: line_item_ref_from_obj(params.get(name))
            for name in ("selector", "bull", "base", "bear")
        }
        if not _scenario_refs_are_present(refs):
            _LOGGER.warning(
                "scenario_select in %r lacks a selector line-item "
                "ref; the cell is left unresolved",
                item_id,
            )
            return None
        cells = {
            name: (
                "NA()" if value is None else self._resolve_ref(
                    value.id,
                    value.t,
                    period,
                    period_anchor=value.period_anchor,
                    absolute=True,
                )
            )
            for name, value in refs.items()
        }
        selector = cells["selector"]
        formula = (
            f"IF({selector}=1,{cells['bull']},"
            f"IF({selector}=2,{cells['base']},"
            f"IF({selector}=3,{cells['bear']},NA())))"
        )
        return _formula_from_expr(
            _CompiledExcelExpr(
                formula,
                f"ISNUMBER({selector})",
            )
        )

    def compile_expr(self, expr: Any, *, period: int) -> str:
        if expr is None:
            return ""

        ref = line_item_ref_from_obj(expr)
        if ref is not None:
            return self._resolve_ref(
                ref.id,
                ref.t,
                period,
                period_anchor=ref.period_anchor,
            )

        if isinstance(expr, bool):
            return "TRUE" if expr else "FALSE"

        if isinstance(expr, (int, float)):
            return _excel_number_literal(expr)

        if isinstance(expr, dict):
            op = str(expr.get("op") or "").upper()
            if op == "IF":
                args = list(expr.get("args", []) or [])
                if len(args) != 3:
                    return ""
                compiled = [self.compile_expr(arg, period=period) for arg in args]
                return f"IF({','.join(compiled)})" if all(compiled) else ""
            if op in {"+", "SUM"}:
                args = [
                    self.compile_expr(arg, period=period)
                    for arg in list(expr.get("args", []) or [])
                ]
                args = [arg for arg in args if arg]
                return "+".join(args)
            if op == "AVG":
                args = [
                    self.compile_expr(arg, period=period)
                    for arg in list(expr.get("args", []) or [])
                ]
                args = [arg for arg in args if arg]
                return f"AVERAGE({','.join(args)})" if args else ""
            if op == "MAX":
                args = [
                    self.compile_expr(arg, period=period)
                    for arg in list(expr.get("args", []) or [])
                ]
                args = [arg for arg in args if arg]
                return f"MAX({','.join(args)})" if args else ""
            if op == "MIN":
                args = [
                    self.compile_expr(arg, period=period)
                    for arg in list(expr.get("args", []) or [])
                ]
                args = [arg for arg in args if arg]
                return f"MIN({','.join(args)})" if args else ""
            if op == "IFERROR":
                inner = self.compile_expr(expr.get("expr"), period=period)
                return inner
            if op == "*":
                factors = [
                    _wrap_if_operator_expr(arg, self.compile_expr(arg, period=period))
                    for arg in list(expr.get("args", []) or [])
                ]
                factors = [factor for factor in factors if factor]
                return "*".join(factors)
            if op == "-":
                left_obj = expr.get("left")
                right_obj = expr.get("right")
                left = self.compile_expr(left_obj, period=period)
                right = self.compile_expr(right_obj, period=period)
                if not left or not right:
                    return ""
                return f"{_wrap_if_operator_expr(left_obj, left)}-{_wrap_if_operator_expr(right_obj, right)}"
            if op == "/":
                left_obj = expr.get("left")
                right_obj = expr.get("right")
                left = self.compile_expr(left_obj, period=period)
                right = self.compile_expr(right_obj, period=period)
                if not left or not right:
                    return ""
                return f"{_wrap_if_operator_expr(left_obj, left)}/{_wrap_if_operator_expr(right_obj, right)}"
            if op == "^":
                left_obj = expr.get("left")
                right_obj = expr.get("right")
                left = self.compile_expr(left_obj, period=period)
                right = self.compile_expr(right_obj, period=period)
                if not left or not right:
                    return ""
                return f"{_wrap_if_operator_expr(left_obj, left)}^{_wrap_if_operator_expr(right_obj, right)}"
            if op == "NEG":
                inner_obj = expr.get("arg")
                inner = self.compile_expr(inner_obj, period=period)
                if not inner:
                    return ""
                return (
                    f"-{inner}" if not _is_operator_expr(inner_obj) else f"-({inner})"
                )

        return ""

    def _compile_semantic_expr(
        self, expr: Any, *, period: int
    ) -> Optional[_CompiledExcelExpr]:
        """Compile an expression without losing whether its inputs are missing."""

        if expr is None:
            return None

        ref = line_item_ref_from_obj(expr)
        if ref is not None:
            value = self._resolve_ref(
                ref.id,
                ref.t,
                period,
                period_anchor=ref.period_anchor,
            )
            present = (
                None
                if item_renders_text(
                    ref.id,
                    lambda item_id: self._active_spec(item_id, period),
                )
                else f"ISNUMBER({value})"
            )
            return _CompiledExcelExpr(value=value, present=present)

        if isinstance(expr, bool):
            return _CompiledExcelExpr("TRUE" if expr else "FALSE")

        if isinstance(expr, (int, float)):
            return _CompiledExcelExpr(_excel_number_literal(expr))

        if not isinstance(expr, dict):
            return None

        op = str(expr.get("op") or "").upper()
        if op == "IF":
            args = list(expr.get("args", []) or [])
            if len(args) != 3:
                return None
            condition = self._compile_semantic_expr(args[0], period=period)
            when_true = self._compile_semantic_expr(args[1], period=period)
            when_false = self._compile_semantic_expr(args[2], period=period)
            if condition is None or when_true is None or when_false is None:
                return None
            selected_present = (
                f"IF({condition.value},{when_true.present or 'TRUE'},"
                f"{when_false.present or 'TRUE'})"
            )
            return _CompiledExcelExpr(
                value=f"IF({condition.value},{when_true.value},{when_false.value})",
                present=_all_present([condition], selected_present),
            )
        if op in {"+", "SUM", "AVG", "MAX", "MIN", "*"}:
            arg_pairs = []
            for arg in list(expr.get("args", []) or []):
                compiled = self._compile_semantic_expr(arg, period=period)
                if compiled is not None:
                    arg_pairs.append((arg, compiled))
            args = [compiled for _arg, compiled in arg_pairs]
            if op == "+":
                value = (
                    f"SUM({','.join(arg.value for arg in args)})"
                    if args
                    else ""
                )
                return _CompiledExcelExpr(value, _all_present(args)) if args else None
            if op == "SUM":
                value = (
                    f"SUM({','.join(arg.value for arg in args)})"
                    if args
                    else ""
                )
                return _CompiledExcelExpr(value, _all_present(args)) if args else None
            if op == "AVG":
                value = (
                    f"AVERAGE({','.join(arg.value for arg in args)})"
                    if args
                    else ""
                )
                return _CompiledExcelExpr(value, _all_present(args)) if args else None
            if op == "MAX":
                value = (
                    f"MAX({','.join(arg.value for arg in args)})"
                    if args
                    else ""
                )
                return _CompiledExcelExpr(value, _all_present(args)) if args else None
            if op == "MIN":
                value = (
                    f"MIN({','.join(arg.value for arg in args)})"
                    if args
                    else ""
                )
                return _CompiledExcelExpr(value, _all_present(args)) if args else None

            factors = [
                _wrap_if_operator_expr(arg_obj, arg.value)
                for arg_obj, arg in arg_pairs
            ]
            if not factors:
                return None
            value = "*".join(factors)
            return _CompiledExcelExpr(value, _all_present(args))

        if op == "IFERROR":
            inner = self._compile_semantic_expr(expr.get("expr"), period=period)
            return inner

        if op in {"-", "/", "^"}:
            left_obj = expr.get("left")
            right_obj = expr.get("right")
            left = self._compile_semantic_expr(left_obj, period=period)
            right = self._compile_semantic_expr(right_obj, period=period)
            if left is None or right is None:
                return None
            left_value = _wrap_if_operator_expr(left_obj, left.value)
            right_value = _wrap_if_operator_expr(right_obj, right.value)
            extras = (f"{right_value}<>0",) if op == "/" else ()
            return _CompiledExcelExpr(
                value=f"{left_value}{op}{right_value}",
                present=_all_present([left, right], *extras),
            )

        if op == "NEG":
            inner_obj = expr.get("arg")
            inner = self._compile_semantic_expr(inner_obj, period=period)
            if inner is None:
                return None
            value = (
                f"-{inner.value}"
                if not _is_operator_expr(inner_obj)
                else f"-({inner.value})"
            )
            return _CompiledExcelExpr(value, inner.present)

        return None

    def _compile_offset_scenario(
        self,
        item_id: Optional[str],
        spec: FormulaSpec,
        period: int,
    ) -> Optional[str]:
        params = spec.params or {}
        anchor_ref = line_item_ref_from_obj(params.get("anchor"))
        selector_ref = line_item_ref_from_obj(params.get("selector"))
        column_offset_mode = params.get("column_offset_mode")
        if (
            anchor_ref is None
            or selector_ref is None
            or column_offset_mode
            not in (None, _COLUMN_OFFSET_MODE_PERIOD_RELATIVE)
        ):
            _LOGGER.warning(
                "OFFSET scenario %r has anchor %r, selector %r, column_offset_mode "
                "%r; the cell is left unresolved",
                item_id,
                params.get("anchor"),
                params.get("selector"),
                column_offset_mode,
            )
            return None
        if column_offset_mode == _COLUMN_OFFSET_MODE_PERIOD_RELATIVE:
            return self._compile_offset_scenario_period_relative(
                item_id,
                anchor_id=anchor_ref.id,
                selector_ref=selector_ref,
                period=period,
            )

        anchor_cell = self._resolve_ref(
            anchor_ref.id,
            anchor_ref.t,
            period,
            period_anchor=anchor_ref.period_anchor,
            absolute=True,
        )
        selector_cell = self._resolve_ref(
            selector_ref.id,
            selector_ref.t,
            period,
            period_anchor=selector_ref.period_anchor,
            absolute=True,
        )
        column_offset = int(params.get("column_offset", 0))
        anchor_match = re.fullmatch(
            r"(?:(?P<sheet>(?:'[^']*(?:''[^']*)*'|[^!]+))!)?"
            r"\$(?P<column>[A-Z]+)\$(?P<row>[1-9][0-9]*)",
            anchor_cell,
        )
        if anchor_match is None:
            raise ValueError(
                f"INDEX scenario {item_id or '<unknown>'} has invalid anchor cell: "
                f"{anchor_cell!r}"
            )
        column = _offset_column(anchor_match.group("column"), column_offset)
        first_row = int(anchor_match.group("row")) + 1
        last_row = first_row + _SCENARIO_TABLE_ROW_COUNT - 1
        sheet_prefix = (
            f"{anchor_match.group('sheet')}!" if anchor_match.group("sheet") else ""
        )
        table_range = f"{sheet_prefix}${column}${first_row}:${column}${last_row}"
        formula = f"INDEX({table_range},{selector_cell},1)"
        return _formula_from_expr(
            _CompiledExcelExpr(
                formula,
                f"AND(ISNUMBER({selector_cell}),ISNUMBER({formula}))",
            )
        )

    def _compile_offset_scenario_period_relative(
        self,
        item_id: Optional[str],
        *,
        anchor_id: str,
        selector_ref: LineItemRef,
        period: int,
    ) -> str:
        try:
            selector_id = selector_ref.id
            anchor_sheet = self._item_sheet_map[anchor_id]
            selector_sheet = self._item_sheet_map[selector_id]
            header_row = self._item_row_map[anchor_id]
            selector_row = self._item_row_map[selector_id]
        except KeyError as exc:
            raise KeyError(
                f"Unknown OFFSET scenario ref for {item_id or '<unknown>'}: {exc.args[0]}"
            ) from exc
        if anchor_sheet != self._current_sheet or selector_sheet != self._current_sheet:
            raise ValueError(
                f"OFFSET scenario {item_id or '<unknown>'} with column_offset_mode="
                f"{_COLUMN_OFFSET_MODE_PERIOD_RELATIVE!r} requires same-sheet anchor and selector"
            )

        first_data_column = self._col.first_data_column
        selector_item = self._item_by_id[selector_ref.id]
        if selector_item.column is None:
            if int(selector_ref.t) != 0:
                raise ValueError(
                    f"OFFSET scenario {item_id or '<unknown>'} selector without fixed column requires t=0"
                )
            selector_col = _selector_column(first_data_column)
            selector_cell = _cell_ref(selector_col, selector_row, absolute=True)
        else:
            selector_cell = self._resolve_ref(
                selector_ref.id,
                selector_ref.t,
                period,
                period_anchor=selector_ref.period_anchor,
                absolute=True,
            )

        period_column = self._col.col_for_period(period)
        first_row = header_row + 1
        last_row = first_row + _SCENARIO_TABLE_ROW_COUNT - 1
        table_range = f"${period_column}${first_row}:${period_column}${last_row}"
        formula = f"INDEX({table_range},{selector_cell},1)"
        return _formula_from_expr(
            _CompiledExcelExpr(
                formula,
                f"AND(ISNUMBER({selector_cell}),ISNUMBER({formula}))",
            )
        )

    def _compile_valuation_dcf_discount(
        self, spec: FormulaSpec, period: int
    ) -> Optional[str]:
        params = spec.params or {}
        cash_flow = self._compile_semantic_expr(params.get("cash_flow"), period=period)
        discount_rate = self._compile_semantic_expr(
            params.get("discount_rate"), period=period
        )
        discount_period = self._compile_semantic_expr(
            params.get("period"), period=period
        )
        if cash_flow is None or discount_rate is None or discount_period is None:
            return None
        denominator = f"(1+{discount_rate.value})^{discount_period.value}"
        return _formula_from_expr(
            _CompiledExcelExpr(
                value=f"{cash_flow.value}/({denominator})",
                present=_all_present(
                    [cash_flow, discount_rate, discount_period],
                    f"{denominator}<>0",
                ),
            )
        )

    def _compile_valuation_terminal_value(
        self, spec: FormulaSpec, period: int
    ) -> Optional[str]:
        params = spec.params or {}
        final_cf = self._compile_semantic_expr(params.get("final_cf"), period=period)
        growth = self._compile_semantic_expr(params.get("growth"), period=period)
        discount = self._compile_semantic_expr(params.get("discount"), period=period)
        if final_cf is None or growth is None or discount is None:
            return None
        denominator = f"{discount.value}-{growth.value}"
        return _formula_from_expr(
            _CompiledExcelExpr(
                value=f"({final_cf.value}*(1+{growth.value}))/({denominator})",
                present=_all_present([final_cf, growth, discount], f"{denominator}<>0"),
            )
        )

    def _compile_valuation_capm(self, spec: FormulaSpec, period: int) -> Optional[str]:
        params = spec.params or {}
        risk_free = self._compile_semantic_expr(params.get("risk_free"), period=period)
        beta = self._compile_semantic_expr(params.get("beta"), period=period)
        erp = self._compile_semantic_expr(params.get("erp"), period=period)
        if risk_free is None or beta is None or erp is None:
            return None
        return _formula_from_expr(
            _CompiledExcelExpr(
                value=f"{risk_free.value}+({beta.value}*{erp.value})",
                present=_all_present([risk_free, beta, erp]),
            )
        )

    def _compile_valuation_wacc(self, spec: FormulaSpec, period: int) -> Optional[str]:
        params = spec.params or {}
        cost_equity = self._compile_semantic_expr(
            params.get("cost_equity"), period=period
        )
        weight_equity = self._compile_semantic_expr(
            params.get("weight_equity"), period=period
        )
        cost_debt = self._compile_semantic_expr(params.get("cost_debt"), period=period)
        weight_debt = self._compile_semantic_expr(
            params.get("weight_debt"), period=period
        )
        if weight_equity is None or weight_debt is None:
            return None
        if cost_equity is None or cost_debt is None:
            return None
        equity_present = cost_equity.present or "TRUE"
        presence_terms = [f"OR({weight_equity.value}=0,{equity_present})"]
        presence_terms.append(f"OR({weight_debt.value}=0,{cost_debt.present or 'TRUE'})")
        presence = _all_present([weight_equity, weight_debt], *presence_terms)
        return _formula_from_expr(
            _CompiledExcelExpr(
                value=(
                    f"IF({weight_equity.value}=0,0,{cost_equity.value}*{weight_equity.value})"
                    f"+IF({weight_debt.value}=0,0,{cost_debt.value}*{weight_debt.value})"
                ),
                present=presence,
            )
        )

    def _compile_valuation_multiple(
        self, spec: FormulaSpec, period: int
    ) -> Optional[str]:
        params = spec.params or {}
        multiple = self._compile_semantic_expr(params.get("multiple"), period=period)
        metric = self._compile_semantic_expr(params.get("metric"), period=period)
        if multiple is None or metric is None:
            return None
        compiled = _CompiledExcelExpr(
            value=f"{multiple.value}*{metric.value}",
            present=_all_present([multiple, metric]),
        )
        return _formula_from_expr(
            compiled, blank_if_error=bool(params.get("blank_if_missing"))
        )

    def _compile_valuation_probability_weighted(
        self, spec: FormulaSpec, period: int
    ) -> Optional[str]:
        params = spec.params or {}
        value = self._compile_semantic_expr(params.get("value"), period=period)
        current = self._compile_semantic_expr(params.get("current"), period=period)
        probability = self._compile_semantic_expr(
            params.get("probability"), period=period
        )
        if value is None or current is None or probability is None:
            return None
        return _formula_from_expr(
            _CompiledExcelExpr(
                value=f"({value.value}-{current.value})*{probability.value}",
                present=_all_present([value, current, probability]),
            )
        )

    def _compile_valuation_kelly(self, spec: FormulaSpec, period: int) -> Optional[str]:
        params = spec.params or {}
        expected_value = self._compile_semantic_expr(
            params.get("expected_value"), period=period
        )
        total_win = self._compile_semantic_expr(params.get("total_win"), period=period)
        if expected_value is None or total_win is None:
            return None
        return _formula_from_expr(
            _CompiledExcelExpr(
                value=f"{expected_value.value}/{total_win.value}",
                present=_all_present(
                    [expected_value, total_win], f"{total_win.value}<>0"
                ),
            )
        )

    def _compile_valuation_break_even_probability(
        self, spec: FormulaSpec, period: int
    ) -> Optional[str]:
        params = spec.params or {}
        up = self._compile_semantic_expr(params.get("upside_delta"), period=period)
        down = self._compile_semantic_expr(params.get("downside_delta"), period=period)
        if up is None or down is None:
            return None
        # D/(U+D) with D = -downside_delta; NA() unless the upside is a gain
        # and the downside a loss (graph semantics in _eval_valuation).
        return _formula_from_expr(
            _CompiledExcelExpr(
                value=f"({down.value})/(({down.value})-({up.value}))",
                present=_all_present(
                    [up, down], f"({up.value})>0", f"({down.value})<0"
                ),
            )
        )

    def _active_spec(self, item_id: str, period: int) -> Optional[FormulaSpec]:
        """Spec ``item_id`` renders with when referenced from ``period``."""

        item = self._item_by_id.get(item_id)
        if item is None:
            return None
        if item.column is not None:
            anchor = _fixed_cell_anchor_period(self._model, item)
            if anchor is None:
                return None
            period = anchor
        period = int(period)
        if item.overrides and period in item.overrides:
            return item.overrides[period]
        if item.formula_periods is not None and period not in {
            int(p) for p in item.formula_periods
        }:
            return None
        time_structure = self._model.time_structure
        if period in _historical_periods(time_structure):
            return item.historical
        if period in _projection_periods(time_structure):
            return item.projected
        return item.projected or item.historical

    def _resolve_ref(
        self,
        ref_id: str,
        ref_t: int,
        period: int,
        *,
        period_anchor: Literal["first", "last"] = "first",
        absolute: Optional[bool] = None,
    ) -> str:
        if ref_id not in self._item_row_map:
            raise KeyError(f"Unknown referenced item: {ref_id}")
        target_sheet = self._item_sheet_map[ref_id]
        target_item = self._item_by_id[ref_id]
        target_mapper = self._mappers.get(target_sheet, self._col)
        use_absolute = (
            bool(absolute) if absolute is not None else target_item.column is not None
        )

        if target_item.column is not None:
            if int(ref_t) != 0:
                raise ValueError(
                    f"Reference {ref_id} with offset {ref_t} targets fixed cell"
                )
            cell = _cell_ref(
                target_item.column, self._item_row_map[ref_id], absolute=use_absolute
            )
        else:
            if period_anchor == "last":
                projection_periods = list(target_mapper.projection_periods())
                if not projection_periods:
                    raise ValueError(
                        f"Reference {ref_id} with period_anchor='last' but target sheet "
                        f"{target_sheet!r} has no projection periods"
                    )
                t_int = int(ref_t)
                if t_int > 0:
                    raise ValueError(
                        f"Reference {ref_id} with period_anchor='last' requires t<=0 (got t={t_int})"
                    )
                target_idx = len(projection_periods) - 1 + t_int
                if target_idx < 0:
                    raise ValueError(
                        f"Reference {ref_id} with period_anchor='last' t={t_int} falls outside "
                        f"projection range (len={len(projection_periods)})"
                    )
                col = target_mapper.col_for_period(projection_periods[target_idx])
            else:
                col = target_mapper.col_for_offset(period, ref_t)
            if col is None:
                raise ValueError(
                    f"Reference {ref_id} with offset {ref_t} falls outside rendered periods for {period}"
                )
            cell = _cell_ref(col, self._item_row_map[ref_id], absolute=use_absolute)

        if target_sheet != self._current_sheet:
            return f"{_quote_sheet_name(target_sheet)}!{cell}"
        return cell


def _wrap_if_operator_expr(obj: Any, text: str) -> str:
    return f"({text})" if _is_operator_expr(obj) else text


def _is_operator_expr(obj: Any) -> bool:
    return isinstance(obj, dict) and "op" in obj


def _apply_adjustment(value: str, adjustment: Any) -> str:
    number = _excel_number_literal(adjustment)
    return f"{value}{number}" if str(number).startswith("-") else f"{value}+{number}"


def _normalize_literal(value: Any) -> Any:
    if isinstance(value, bool):
        return 1 if value else 0
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return value
    return value


def _excel_number_literal(value: Any) -> str:
    if isinstance(value, bool):
        return "1" if value else "0"
    number = float(value)
    if number.is_integer():
        return str(int(number))
    # ``g`` defaults to six significant digits and silently changes compiled
    # economics when a coefficient is embedded in an Excel formula.  Python's
    # float repr is the shortest decimal that round-trips to the exact binary
    # value, preserving the dependency-graph result without noisy padding.
    return repr(number)


def _quote_sheet_name(sheet_name: str) -> str:
    if re.match(r"^[A-Za-z0-9_]+$", sheet_name):
        return sheet_name
    escaped = sheet_name.replace("'", "''")
    return f"'{escaped}'"


def _cell_ref(column: str, row: int, *, absolute: bool = False) -> str:
    column = column.upper()
    if absolute:
        return f"${column}${row}"
    return f"{column}{row}"


__all__ = [
    "AbsoluteColumnMapper",
    "ExcelFormulaCompiler",
]
