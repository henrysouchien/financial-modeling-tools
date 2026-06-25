"""Excel formula pattern matcher.

Purpose:
- Convert Excel formulas into schema FormulaSpec objects with semantic references.
- Recognize common financial-model patterns so downstream tools can reason
  about dependencies without cell addresses.

High-level flow:
1. Parse the formula into an AST.
2. If the AST is constant-only, emit a constant FormulaSpec.
3. Try pattern matchers in order (ref → growth → valuation → ratio → roll_forward
   → driver → arithmetic). The order encodes precedence for ambiguous patterns.
4. Fall back to a raw FormulaSpec when no pattern matches.

Key idea:
- Cell references are mapped to LineItemRef(id, t) using sheet/row/column context,
  where t is a time offset derived from the model period headers.

Examples:
Formula: =H7-H9
Schema:  FormulaSpec(type=arithmetic, params={"operands": ["-", LineItemRef("revenue"), LineItemRef("gross_profit")]})

Formula: =G14*(1+H15)
Schema:  FormulaSpec(type=growth, params={"base": LineItemRef("revenue", t=-1), "rate": LineItemRef("growth_rate")})

Formula: =E35/((1+$E$68)^E37)
Schema:  FormulaSpec(type=valuation, subtype="dcf_discount",
                    params={"cash_flow": LineItemRef("fcf"), "discount_rate": LineItemRef("wacc"), "period": LineItemRef("period")})
"""

from __future__ import annotations

from typing import List, Optional, Tuple

from .formula_ast import (
    BinaryOp,
    FormulaParseError,
    FormulaParser,
    FuncCall,
    Node,
    Range,
    Ref,
    UnaryOp,
)
from .models import FormulaSpec, FormulaType, LineItemRef
from .pattern_matcher_context import CellContext
from . import pattern_matcher_constant as _constant_patterns
from . import pattern_matcher_driver as _driver_patterns
from . import pattern_matcher_expr as _expr_patterns
from . import pattern_matcher_flatten as _flatten_patterns
from . import pattern_matcher_growth as _growth_patterns
from . import pattern_matcher_raw as _raw_patterns
from . import pattern_matcher_ref as _ref_patterns
from . import pattern_matcher_ratio as _ratio_patterns
from . import pattern_matcher_resolution as _resolution_patterns
from . import pattern_matcher_roll_forward as _roll_forward_patterns
from . import pattern_matcher_valuation as _valuation_patterns
from . import pattern_matcher_arithmetic as _arithmetic_patterns


class FormulaPatternMatcher:
    def classify(self, formula: str, context: CellContext) -> FormulaSpec:
        """Classify a raw Excel formula string into a FormulaSpec.

        The matcher tries fast-path patterns first (ref/growth/valuation/etc.)
        and falls back to a raw FormulaSpec if no pattern matches.
        """
        try:
            parser = FormulaParser(formula)
            ast = parser.parse()
        except FormulaParseError:
            return self._raw_spec(formula)

        # Strip unary plus — legacy Excel convention (=+Cell) common in institutional models
        while isinstance(ast, UnaryOp) and ast.op == "+":
            ast = ast.expr

        constant_value = self._constant_value(ast)
        if constant_value is not None:
            return FormulaSpec(
                type=FormulaType.constant,
                subtype="hardcoded_value",
                params={"value": constant_value},
            )

        ref_spec = self._match_ref(ast, context)
        if ref_spec:
            return ref_spec

        growth_spec = self._match_growth(ast, context)
        if growth_spec:
            return growth_spec

        valuation_spec = self._match_valuation(ast, context)
        if valuation_spec:
            return valuation_spec

        ratio_spec = self._match_ratio(ast, context)
        if ratio_spec:
            return ratio_spec

        roll_spec = self._match_roll_forward(ast, context)
        if roll_spec:
            return roll_spec

        driver_spec = self._match_driver(ast, context)
        if driver_spec:
            return driver_spec

        arithmetic_spec = self._match_arithmetic(ast, context)
        if arithmetic_spec:
            return arithmetic_spec

        # Note: Some models still surface raw formulas (e.g., IF/IFERROR wrappers or #REF! broken links).
        return self._raw_spec(formula)

    def _raw_spec(self, formula: str) -> FormulaSpec:
        return _raw_patterns.raw_spec(formula, raw_subtype_fn=self._raw_subtype)

    def _raw_subtype(self, formula: str) -> Optional[str]:
        return _raw_patterns.raw_subtype(formula)

    def _match_ref(self, ast: Node, context: CellContext) -> Optional[FormulaSpec]:
        return _ref_patterns.match_ref(
            ast,
            context,
            to_line_item_ref_fn=self._to_line_item_ref,
            ref_subtype_fn=self._ref_subtype,
        )

    def _ref_subtype(
        self,
        node: Ref,
        context: CellContext,
        *,
        adjusted: bool = False,
        negated: bool = False,
    ) -> str:
        return _ref_patterns.ref_subtype(
            node,
            context,
            col_to_index_fn=_col_to_index,
            adjusted=adjusted,
            negated=negated,
        )

    def _match_growth(self, ast: Node, context: CellContext) -> Optional[FormulaSpec]:
        return _growth_patterns.match_growth(
            ast,
            context,
            extract_growth_operands_fn=self._extract_growth_operands,
            to_line_item_ref_fn=self._to_line_item_ref,
        )

    def _extract_growth_operands(self, base_candidate: Node, rate_candidate: Node) -> Tuple[Optional[Ref], Optional[Ref]]:
        return _growth_patterns.extract_growth_operands(base_candidate, rate_candidate)

    def _match_ratio(self, ast: Node, context: CellContext) -> Optional[FormulaSpec]:
        return _ratio_patterns.match_ratio(
            ast,
            context,
            expr_from_node_fn=self._expr_from_node,
            is_delta_expr_fn=self._is_delta_expr,
            ratio_subtype_fn=self._ratio_subtype,
        )

    def _ratio_subtype(self, numerator_node: Node, denominator_node: Node, context: CellContext) -> str:
        return _ratio_patterns.ratio_subtype(
            numerator_node,
            denominator_node,
            context,
            is_share_denominator_fn=self._is_share_denominator,
        )

    def _is_share_denominator(self, node: Node, context: CellContext) -> bool:
        return _ratio_patterns.is_share_denominator(
            node,
            context,
            to_line_item_ref_fn=self._to_line_item_ref,
            is_share_count_id_fn=self._is_share_count_id,
        )

    def _is_share_count_id(self, line_item_id: str) -> bool:
        return _ratio_patterns.is_share_count_id(
            line_item_id,
            id_tokens_fn=self._id_tokens,
            has_token_sequence_fn=self._has_token_sequence,
        )

    def _id_tokens(self, line_item_id: str) -> List[str]:
        return _ratio_patterns.id_tokens(line_item_id)

    def _has_token_sequence(self, tokens: List[str], sequence: Tuple[str, ...]) -> bool:
        return _ratio_patterns.has_token_sequence(tokens, sequence)

    def _match_roll_forward(self, ast: Node, context: CellContext) -> Optional[FormulaSpec]:
        return _roll_forward_patterns.match_roll_forward(
            ast,
            context,
            flatten_add_sub_fn=self._flatten_add_sub,
            to_line_item_ref_fn=self._to_line_item_ref,
            has_same_row_ref_fn=self._has_same_row_ref,
        )

    def _match_driver(self, ast: Node, context: CellContext) -> Optional[FormulaSpec]:
        return _driver_patterns.match_driver(
            ast,
            context,
            to_line_item_ref_fn=self._to_line_item_ref,
            expr_from_node_fn=self._expr_from_node,
            contains_ref_fn=self._contains_ref,
            is_rate_x_base_fn=self._is_rate_x_base,
        )

    def _is_rate_x_base(self, left_node: Node, right_node: Node, context: CellContext) -> bool:
        return _driver_patterns.is_rate_x_base(
            left_node,
            right_node,
            context,
            to_line_item_ref_fn=self._to_line_item_ref,
            is_rate_like_id_fn=self._is_rate_like_id,
        )

    def _is_rate_like_id(self, line_item_id: str) -> bool:
        return _driver_patterns.is_rate_like_id(
            line_item_id,
            id_tokens_fn=self._id_tokens,
        )

    def _match_arithmetic(self, ast: Node, context: CellContext) -> Optional[FormulaSpec]:
        return _arithmetic_patterns.match_arithmetic(
            ast,
            context,
            range_to_refs_fn=self._range_to_refs,
            to_line_item_ref_fn=self._to_line_item_ref,
            expr_from_node_fn=self._expr_from_node,
            function_arithmetic_subtype_fn=self._function_arithmetic_subtype,
            binary_arithmetic_subtype_fn=self._binary_arithmetic_subtype,
            flatten_binary_fn=self._flatten_binary,
        )

    def _function_arithmetic_subtype(self, ast: FuncCall) -> Optional[str]:
        return _arithmetic_patterns.function_arithmetic_subtype(ast)

    def _binary_arithmetic_subtype(self, ast: BinaryOp, context: CellContext) -> Optional[str]:
        return _arithmetic_patterns.binary_arithmetic_subtype(
            ast,
            context,
            flatten_add_sub_fn=self._flatten_add_sub,
            to_line_item_ref_fn=self._to_line_item_ref,
        )

    def _match_valuation(self, ast: Node, context: CellContext) -> Optional[FormulaSpec]:
        return _valuation_patterns.match_valuation(
            ast,
            context,
            to_line_item_ref_fn=self._to_line_item_ref,
            extract_one_plus_ref_fn=self._extract_one_plus_ref,
            match_offset_scenario_fn=self._match_offset_scenario,
        )

    def _match_offset_scenario(self, ast: FuncCall, context: CellContext) -> Optional[FormulaSpec]:
        return _valuation_patterns.match_offset_scenario(
            ast,
            context,
            to_line_item_ref_fn=self._to_line_item_ref,
        )

    def _constant_value(self, ast: Node) -> Optional[float]:
        return _constant_patterns.constant_value(ast)

    def _to_line_item_ref(self, node: Node, context: CellContext) -> Optional[LineItemRef]:
        return _resolution_patterns.to_line_item_ref(
            node,
            context,
            col_to_index_fn=_col_to_index,
            period_offset_fn=self._period_offset,
        )

    def _range_to_refs(self, node: Range, context: CellContext) -> List[LineItemRef]:
        return _resolution_patterns.range_to_refs(
            node,
            context,
            col_to_index_fn=_col_to_index,
            period_offset_fn=self._period_offset,
        )

    def _period_offset(self, context: CellContext, target_sheet: str, target_col: int) -> Tuple[int, bool]:
        return _resolution_patterns.period_offset(context, target_sheet, target_col)

    def _expr_from_node(self, node: Node, context: CellContext):
        return _expr_patterns.expr_from_node(
            node,
            context,
            to_line_item_ref_fn=self._to_line_item_ref,
            range_to_refs_fn=self._range_to_refs,
            expr_from_node_fn=self._expr_from_node,
            append_expr_arg_fn=self._append_expr_arg,
        )

    def _append_expr_arg(self, args: List, expr, op: str) -> None:
        _expr_patterns.append_expr_arg(args, expr, op)

    def _contains_ref(self, expr) -> bool:
        return _expr_patterns.contains_ref(expr, contains_ref_fn=self._contains_ref)

    def _extract_one_plus_ref(self, node: Node, context: CellContext) -> Optional[LineItemRef]:
        return _valuation_patterns.extract_one_plus_ref(
            node,
            context,
            to_line_item_ref_fn=self._to_line_item_ref,
        )

    def _is_delta_expr(self, node: Node, context: CellContext) -> bool:
        return _ratio_patterns.is_delta_expr(
            node,
            context,
            to_line_item_ref_fn=self._to_line_item_ref,
        )

    def _flatten_add_sub(self, ast: Node) -> Optional[List[Tuple[str, Node]]]:
        return _flatten_patterns.flatten_add_sub(ast, flatten_add_sub_fn=self._flatten_add_sub)

    def _flatten_binary(self, ast: Node, op: str) -> Optional[List[Node]]:
        return _flatten_patterns.flatten_binary(ast, op, flatten_binary_fn=self._flatten_binary)

    def _has_same_row_ref(self, context: CellContext, refs: List[LineItemRef]) -> bool:
        return _roll_forward_patterns.has_same_row_ref(context, refs)


def _col_to_index(col: str) -> int:
    return _resolution_patterns.col_to_index(col)
