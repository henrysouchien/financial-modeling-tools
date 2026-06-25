"""Per-item compute function emission helpers for :mod:`schema.codegen`."""

from __future__ import annotations

from pprint import pformat
import sys
from typing import Any, Dict, Set

from .codegen_expr import CodeEmitter, ExprCompiler, _quote
from .codegen_model_helpers import _item_locations
from .models import FinancialModel, FormulaType, ItemType


def _parent_attr(name: str, default: Any) -> Any:
    parent = sys.modules.get("schema.codegen")
    if parent is None:
        parent = sys.modules.get("codegen")
    return getattr(parent, name, default) if parent is not None else default


def _emit_item_functions(
    emitter: CodeEmitter,
    model: FinancialModel,
    fn_by_item: Dict[str, str],
    compiler: ExprCompiler,
) -> None:
    emitter.comment("Per-item compute functions")

    item_locations = _parent_attr("_item_locations", _item_locations)
    emit_inline_assignment = _parent_attr("_emit_inline_assignment", _emit_inline_assignment)
    quote = _parent_attr("_quote", _quote)

    location = item_locations(model)
    for item_id, item in model._index.items():
        if item.item_type != ItemType.derived:
            continue

        fn_name = fn_by_item[item_id]
        sheet_name, section_label = location.get(item_id, ("", ""))
        emitter.line(f"def {fn_name}(r: Dict[str, Dict[int, Optional[float]]], p: int) -> None:")
        with emitter.indent():
            emitter.line(f'"""{item.label} (Sheet: {sheet_name} / {section_label}, Row {item.row})"""')

            if item.overrides:
                for period in sorted(item.overrides.keys()):
                    override = item.overrides[period]
                    needs_guard = (
                        override.type == FormulaType.constant
                        and item.projected is not None
                        and item.projected.type != FormulaType.constant
                    )
                    emitter.line(f"if p == {int(period)}:")
                    with emitter.indent():
                        if override.type == FormulaType.constant:
                            expr = compiler.compile_formula(override, item_id=item.id)
                            if needs_guard:
                                emitter.line(f"if {quote(item.id)} not in _PROPAGATE:")
                                with emitter.indent():
                                    emitter.line(f"r.setdefault({quote(item.id)}, {{}})[p] = {expr}")
                                    emitter.line("return")
                            else:
                                emitter.line(f"r.setdefault({quote(item.id)}, {{}})[p] = {expr}")
                                emitter.line("return")
                        else:
                            expr = compiler.compile_formula(override, item_id=item.id)
                            emitter.line(f"v = {expr}")
                            emitter.line("if v is not None:")
                            with emitter.indent():
                                emitter.line(f"r.setdefault({quote(item.id)}, {{}})[p] = v")
                            emitter.line("elif r.get(" + quote(item.id) + ", {}).get(p) is None:")
                            with emitter.indent():
                                emitter.line(f"r.setdefault({quote(item.id)}, {{}})[p] = None")
                            emitter.line("return")

            if item.formula_periods is not None:
                emit_inline_assignment(emitter, "_periods", set(item.formula_periods))
                skippable_override_periods: Set[int] = set()
                if item.overrides and item.projected and item.projected.type != FormulaType.constant:
                    for period, override in item.overrides.items():
                        if override.type == FormulaType.constant:
                            skippable_override_periods.add(int(period))
                if skippable_override_periods:
                    emit_inline_assignment(emitter, "_skippable", sorted(skippable_override_periods))
                    emitter.line("_skippable = set(_skippable)")
                emitter.line("if p not in _periods:")
                with emitter.indent():
                    if skippable_override_periods:
                        emitter.line(f"if p not in _skippable or {quote(item.id)} not in _PROPAGATE:")
                        with emitter.indent():
                            emitter.line(f"r.setdefault({quote(item.id)}, {{}})[p] = None")
                            emitter.line("return")
                    else:
                        emitter.line(f"r.setdefault({quote(item.id)}, {{}})[p] = None")
                        emitter.line("return")

            historical_expr = compiler.compile_formula(item.historical, item_id=item.id)
            projected_expr = compiler.compile_formula(item.projected, item_id=item.id)
            fallback_expr = projected_expr if item.projected is not None else historical_expr

            emitter.line("if p in _HISTORICAL_SET:")
            with emitter.indent():
                emitter.line(f"v = {historical_expr}")
            emitter.line("elif p in _PROJECTION_SET:")
            with emitter.indent():
                emitter.line(f"v = {projected_expr}")
            emitter.line("else:")
            with emitter.indent():
                emitter.line(f"v = {fallback_expr}")
            emitter.line(f"r.setdefault({quote(item.id)}, {{}})[p] = v")
        emitter.blank()


def _emit_inline_assignment(emitter: CodeEmitter, lhs: str, value) -> None:
    literal = pformat(value, sort_dicts=True, width=100)
    lines = literal.splitlines()
    if len(lines) == 1:
        emitter.line(f"{lhs} = {lines[0]}")
        return
    emitter.line(f"{lhs} = {lines[0]}")
    for line in lines[1:]:
        emitter.line(line)


__all__ = ["_emit_item_functions"]
