"""Excel formula compilation helpers for schema rendering."""

from __future__ import annotations

import re
from typing import Any, Dict, Literal, Optional

from .renderer_columns import AbsoluteColumnMapper, _selector_column
from .models import (
    FinancialModel,
    FormulaSpec,
    FormulaType,
    LineItem,
    LineItemRef,
)
from .refs import line_item_ref_from_obj


_COLUMN_OFFSET_MODE_PERIOD_RELATIVE = "period_relative"


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
            value = self.compile_expr(params.get("source"), period=period)
            if not value:
                return None
            adjustment = params.get("adjustment")
            if adjustment is not None:
                value = _apply_adjustment(value, adjustment)
            if params.get("negate"):
                value = f"-({value})" if adjustment is not None else f"-{value}"
            return f"={value}"

        if spec.type == FormulaType.arithmetic:
            if "expr" in params:
                expr = self.compile_expr(params.get("expr"), period=period)
                if not expr:
                    return None
                if params.get("blank_if_error"):
                    return f'=IFERROR({expr},"")'
                return f"={expr}"

            function = params.get("function")
            if function == "SUM_RANGE":
                target_obj = params.get("target")
                target_ref = line_item_ref_from_obj(target_obj)
                if target_ref is None:
                    raise ValueError(
                        f"SUM_RANGE in {item_id!r} has malformed 'target' "
                        f"(expected LineItemRef-coercible; got {type(target_obj).__name__}: "
                        f"{target_obj!r})"
                    )
                if target_ref.id not in self._item_row_map:
                    raise KeyError(f"SUM_RANGE target {target_ref.id!r} not found in model index")

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
                    return f"={cell}"

                periods = list(target_mapper.all_periods())
                if not periods:
                    return None
                first_col = target_mapper.col_for_period(periods[0])
                last_col = target_mapper.col_for_period(periods[-1])
                cell_range = f"{first_col}{target_row}:{last_col}{target_row}"
                if target_sheet != self._current_sheet:
                    return f"=SUM({_quote_sheet_name(target_sheet)}!{cell_range})"
                return f"=SUM({cell_range})"

            if function in {"SUM", "AVERAGE", "MEDIAN"}:
                items = list(params.get("items", []) or [])
                compiled = [self.compile_expr(item, period=period) for item in items]
                compiled = [item for item in compiled if item]
                if not compiled:
                    return 0
                if function == "AVERAGE":
                    formula = f"AVERAGE({','.join(compiled)})"
                    return f'=IFERROR({formula},"")' if params.get("blank_if_error") else f"={formula}"
                if function == "MEDIAN":
                    formula = f"MEDIAN({','.join(compiled)})"
                    return f'=IFERROR({formula},"")' if params.get("blank_if_error") else f"={formula}"
                formula = "+".join(compiled)
                return f'=IFERROR({formula},"")' if params.get("blank_if_error") else f"={formula}"

            operands = params.get("operands")
            if isinstance(operands, list) and operands:
                operator = "+"
                args = list(operands)
                if isinstance(args[0], str) and args[0] in {"+", "-", "*", "/"}:
                    operator = args.pop(0)
                compiled_args = [self.compile_expr(arg, period=period) for arg in args]
                compiled_args = [value for value in compiled_args if value]
                if not compiled_args:
                    return 0
                if operator in {"*", "/"}:
                    compiled_args = [
                        _wrap_if_operator_expr(arg, text)
                        for arg, text in zip(args, compiled_args)
                    ]
                elif operator == "-":
                    compiled_args = [
                        compiled_args[0],
                        *[
                            _wrap_if_operator_expr(arg, text)
                            for arg, text in zip(args[1:], compiled_args[1:])
                        ],
                    ]
                return f"={operator.join(compiled_args)}"

            items = list(params.get("items", []) or [])
            compiled = [self.compile_expr(item, period=period) for item in items]
            compiled = [item for item in compiled if item]
            return f"={'+'.join(compiled)}" if compiled else 0

        if spec.type == FormulaType.driver:
            base_expr = self.compile_expr(params.get("base"), period=period)
            rate_expr = self.compile_expr(params.get("rate"), period=period)
            if not base_expr or not rate_expr:
                return None
            base_expr = _wrap_if_operator_expr(params.get("base"), base_expr)
            rate_expr = _wrap_if_operator_expr(params.get("rate"), rate_expr)
            formula = f"{base_expr}*{rate_expr}"
            if "scale" in params and params.get("scale") is not None:
                formula = f"{formula}/{_excel_number_literal(params.get('scale'))}"
            return f"={formula}"

        if spec.type == FormulaType.ratio:
            numerator_expr = self.compile_expr(params.get("numerator"), period=period)
            denominator_expr = self.compile_expr(params.get("denominator"), period=period)
            if not numerator_expr or not denominator_expr:
                return None
            numerator_expr = _wrap_if_operator_expr(params.get("numerator"), numerator_expr)
            denominator_expr = _wrap_if_operator_expr(params.get("denominator"), denominator_expr)
            formula = f"{numerator_expr}/{denominator_expr}"
            if params.get("subtract_one"):
                formula = f"{formula}-1"
            return f"={formula}"

        if spec.type == FormulaType.growth:
            base_expr = self.compile_expr(params.get("base"), period=period)
            rate_expr = self.compile_expr(params.get("rate"), period=period)
            if not base_expr or not rate_expr:
                return None
            base_expr = _wrap_if_operator_expr(params.get("base"), base_expr)
            return f"={base_expr}*(1+{rate_expr})"

        if spec.type == FormulaType.roll_forward:
            beginning = self.compile_expr(params.get("beginning"), period=period)
            if not beginning:
                return None
            parts = [beginning]
            for addition in list(params.get("additions", []) or []):
                compiled = self.compile_expr(addition, period=period)
                if compiled:
                    parts.append(f"+{compiled}")
            for subtraction in list(params.get("subtractions", []) or []):
                compiled = self.compile_expr(subtraction, period=period)
                if compiled:
                    compiled = _wrap_if_operator_expr(subtraction, compiled)
                    parts.append(f"-{compiled}")
            return f"={''.join(parts)}"

        if spec.type == FormulaType.valuation:
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
            return None

        if spec.type == FormulaType.raw:
            formula = str(params.get("formula") or "").strip()
            if not formula:
                return None
            return formula if formula.startswith("=") else f"={formula}"

        return None

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
            if op in {"+", "SUM"}:
                args = [self.compile_expr(arg, period=period) for arg in list(expr.get("args", []) or [])]
                args = [arg for arg in args if arg]
                return "+".join(args) if args else "0"
            if op == "AVG":
                args = [self.compile_expr(arg, period=period) for arg in list(expr.get("args", []) or [])]
                args = [arg for arg in args if arg]
                return f"AVERAGE({','.join(args)})" if args else "0"
            if op == "MAX":
                args = [self.compile_expr(arg, period=period) for arg in list(expr.get("args", []) or [])]
                args = [arg for arg in args if arg]
                return f"MAX({','.join(args)})" if args else "0"
            if op == "IFERROR":
                inner = self.compile_expr(expr.get("expr"), period=period)
                fallback = self.compile_expr(expr.get("fallback", 0), period=period)
                if not inner or not fallback:
                    return ""
                return f"IFERROR({inner},{fallback})"
            if op == "*":
                factors = [
                    _wrap_if_operator_expr(arg, self.compile_expr(arg, period=period))
                    for arg in list(expr.get("args", []) or [])
                ]
                factors = [factor for factor in factors if factor]
                return "*".join(factors) if factors else "0"
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
                return f"-{inner}" if not _is_operator_expr(inner_obj) else f"-({inner})"

        return ""

    def _compile_offset_scenario(
        self,
        item_id: Optional[str],
        spec: FormulaSpec,
        period: int,
    ) -> str:
        params = spec.params or {}
        anchor_ref = line_item_ref_from_obj(params.get("anchor"))
        selector_ref = line_item_ref_from_obj(params.get("selector"))
        if anchor_ref is None:
            raise ValueError(f"OFFSET scenario {item_id or '<unknown>'} is missing anchor ref")
        if selector_ref is None:
            raise ValueError(f"OFFSET scenario {item_id or '<unknown>'} is missing selector ref")
        column_offset_mode = params.get("column_offset_mode")
        if column_offset_mode == _COLUMN_OFFSET_MODE_PERIOD_RELATIVE:
            return self._compile_offset_scenario_period_relative(
                item_id,
                anchor_id=anchor_ref.id,
                selector_ref=selector_ref,
                period=period,
            )
        if column_offset_mode is not None:
            raise ValueError(
                f"OFFSET scenario {item_id or '<unknown>'} has unknown column_offset_mode: {column_offset_mode!r}"
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
        return f"=OFFSET({anchor_cell},{selector_cell},{column_offset})"

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
            raise KeyError(f"Unknown OFFSET scenario ref for {item_id or '<unknown>'}: {exc.args[0]}") from exc
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

        anchor_ref = f"${first_data_column}${header_row}"
        base_col_ref = f"${first_data_column}${selector_row}"
        return f"=OFFSET({anchor_ref},{selector_cell},COLUMN()-COLUMN({base_col_ref}))"

    def _compile_valuation_dcf_discount(self, spec: FormulaSpec, period: int) -> Optional[str]:
        params = spec.params or {}
        cash_flow = self.compile_expr(params.get("cash_flow"), period=period)
        discount_rate = self.compile_expr(params.get("discount_rate"), period=period)
        discount_period = self.compile_expr(params.get("period"), period=period)
        if not cash_flow or not discount_rate or not discount_period:
            return None
        return f"={cash_flow}/((1+{discount_rate})^{discount_period})"

    def _compile_valuation_terminal_value(self, spec: FormulaSpec, period: int) -> Optional[str]:
        params = spec.params or {}
        final_cf = self.compile_expr(params.get("final_cf"), period=period)
        growth = self.compile_expr(params.get("growth"), period=period)
        discount = self.compile_expr(params.get("discount"), period=period)
        if not final_cf or not growth or not discount:
            return None
        return f"=({final_cf}*(1+{growth}))/({discount}-{growth})"

    def _compile_valuation_capm(self, spec: FormulaSpec, period: int) -> Optional[str]:
        params = spec.params or {}
        risk_free = self.compile_expr(params.get("risk_free"), period=period)
        beta = self.compile_expr(params.get("beta"), period=period)
        erp = self.compile_expr(params.get("erp"), period=period)
        if not risk_free or not beta or not erp:
            return None
        return f"={risk_free}+({beta}*{erp})"

    def _compile_valuation_wacc(self, spec: FormulaSpec, period: int) -> Optional[str]:
        params = spec.params or {}
        cost_equity = self.compile_expr(params.get("cost_equity"), period=period)
        weight_equity = self.compile_expr(params.get("weight_equity"), period=period)
        cost_debt = self.compile_expr(params.get("cost_debt"), period=period)
        weight_debt = self.compile_expr(params.get("weight_debt"), period=period)
        if not cost_equity or not weight_equity or not cost_debt or not weight_debt:
            return None
        return f"=({cost_equity}*{weight_equity})+({cost_debt}*{weight_debt})"

    def _compile_valuation_multiple(self, spec: FormulaSpec, period: int) -> Optional[str]:
        params = spec.params or {}
        multiple = self.compile_expr(params.get("multiple"), period=period)
        metric = self.compile_expr(params.get("metric"), period=period)
        if not multiple or not metric:
            return None
        if params.get("blank_if_missing"):
            return f'=IFERROR(IF(OR({multiple}="",{metric}=""),"",{multiple}*{metric}),"")'
        return f"={multiple}*{metric}"

    def _compile_valuation_probability_weighted(self, spec: FormulaSpec, period: int) -> Optional[str]:
        params = spec.params or {}
        value = self.compile_expr(params.get("value"), period=period)
        current = self.compile_expr(params.get("current"), period=period)
        probability = self.compile_expr(params.get("probability"), period=period)
        if not value or not current or not probability:
            return None
        return f"=({value}-{current})*{probability}"

    def _compile_valuation_kelly(self, spec: FormulaSpec, period: int) -> Optional[str]:
        params = spec.params or {}
        expected_value = self.compile_expr(params.get("expected_value"), period=period)
        total_win = self.compile_expr(params.get("total_win"), period=period)
        if not expected_value or not total_win:
            return None
        return f"={expected_value}/{total_win}"

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
        use_absolute = bool(absolute) if absolute is not None else target_item.column is not None

        if target_item.column is not None:
            if int(ref_t) != 0:
                raise ValueError(f"Reference {ref_id} with offset {ref_t} targets fixed cell")
            cell = _cell_ref(target_item.column, self._item_row_map[ref_id], absolute=use_absolute)
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
    return format(number, "g")


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
