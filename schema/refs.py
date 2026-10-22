"""Shared helpers for schema reference values."""

from __future__ import annotations

import logging
from dataclasses import dataclass, replace
from typing import Any, Callable, Literal, Optional

from .models import FormulaSpec, FormulaType, LineItemRef, parse_period_anchor

logger = logging.getLogger(__name__)

# Derived from the owning model so the coercible key set cannot drift from
# LineItemRef's declared fields.
_REF_KEYS = frozenset(LineItemRef.model_fields)


def line_item_ref_from_obj(obj: Any, *, strict: bool = False) -> Optional[LineItemRef]:
    """Coerce a JSON-deserialized ref shape back to LineItemRef.

    A ref-shaped dict whose fields do not parse is logged and dropped (``None``); with ``strict``
    it raises, as ``LineItemRef`` itself does (the model validator's rehydration)."""
    if isinstance(obj, LineItemRef):
        return obj
    if isinstance(obj, dict) and "id" in obj and obj.keys() <= _REF_KEYS:
        try:
            return LineItemRef(
                id=str(obj["id"]),
                t=int(obj.get("t", 0)),
                resolved=bool(obj.get("resolved", True)),
                period_anchor=parse_period_anchor(
                    obj.get("period_anchor", "first")
                ),
            )
        except (TypeError, ValueError) as exc:
            if strict:
                raise
            logger.warning(
                "ref-shaped dict %r dropped by line_item_ref_from_obj: %s: %s",
                obj,
                type(exc).__name__,
                exc,
            )
            return None
    return None


# ---------------------------------------------------------------------------
# The formula reference owner
#
# ``FormulaSpec.references()`` / ``FormulaSpec.without()`` (schema/models.py) delegate here. This
# is the one walk over the documented ``params`` encodings (models.py ``FormulaSpec`` docstring and
# the expression tree the graph evaluates, dependency_graph_eval._eval / _eval_expr); the
# dependency graph's ``_extract_refs`` reads it, so the set of live refs and the set a removal can
# rewrite cannot drift apart.
# ---------------------------------------------------------------------------

FormulaRefRole = Literal[
    "additive",
    "subtractive",
    "minuend",
    "numerator",
    "denominator",
    "base",
    "rate",
    "beginning",
    "source",
    "anchor",
    "target",
    "condition",
    "cost_debt",
    "weight_debt",
    "other",
]

# The roles whose removal leaves a formula that still means something: a term of a sum.
_REMOVABLE_ROLES = frozenset({"additive", "subtractive"})

# The ``valuation/wacc`` slots whose whole param may leave: no debt is a zero debt cost and a zero
# debt weight, which every reader evaluates as a zero debt term. A ref nested in a composite
# slot value, or any other wacc slot, is not removable.
_WACC_ZERO_SLOTS: dict[str, FormulaRefRole] = {
    "cost_debt": "cost_debt",
    "weight_debt": "weight_debt",
}


@dataclass(frozen=True)
class FormulaRef:
    """One occurrence of a ``LineItemRef`` inside a spec, with its role in the formula.

    ``path`` addresses the occurrence inside ``spec.params`` (``("expr", "args", 1, "arg")``,
    ``("subtractions", 0)``). ``role`` is the position the ref holds: a term of a sum
    (``additive`` / ``subtractive``, the sign the term carries after any ``NEG`` / ``-`` right
    operand / roll-forward ``subtractions``), the first operand of a subtraction (``minuend``),
    or the named slot of the formula type it sits in. A factor of a product that is itself a term
    of a sum carries that term's sign; a ref inside any other multiplicative, ``IF`` branch or
    non-sum position is ``other``; inside a named slot every nested ref carries that slot's role.
    """

    ref: LineItemRef
    path: tuple[str | int, ...]
    role: FormulaRefRole


class _Unremovable(Exception):
    """A retired ref sits where removing it leaves no formula."""


class _Gone:
    """A term (or a sum of terms) removed from its additive parent."""


_GONE = _Gone()


@dataclass(frozen=True)
class _Ctx:
    """Where a node sits: a sum (``role is None``, terms carry ``sign``) or a named slot."""

    role: Optional[FormulaRefRole] = None
    sign: int = 1
    minuend: bool = False


_OTHER = _Ctx("other")


class _Walk:
    """One traversal of a spec's live params; ``predicate`` turns it into a removal."""

    def __init__(
        self,
        predicate: Optional[Callable[[FormulaRef], bool]],
        evaluate_condition: Optional[Callable[[Any], Optional[float]]] = None,
    ) -> None:
        self.predicate = predicate
        self.evaluate_condition = evaluate_condition
        self.found: list[FormulaRef] = []
        self.selected = 0

    def leaf(self, ref: LineItemRef, path: tuple[str | int, ...], ctx: _Ctx) -> bool:
        if ctx.role is not None:
            role: FormulaRefRole = ctx.role
        elif ctx.minuend:
            role = "minuend"
        else:
            role = "additive" if ctx.sign > 0 else "subtractive"
        found = FormulaRef(ref=ref, path=path, role=role)
        self.found.append(found)
        if self.predicate is None or not self.predicate(found):
            return True
        self.selected += 1
        if role in _REMOVABLE_ROLES or role == "minuend" or (role in _WACC_ZERO_SLOTS and len(path) == 1):
            return False
        raise _Unremovable

    def node(self, value: Any, path: tuple[str | int, ...], ctx: _Ctx) -> Any:
        ref = line_item_ref_from_obj(value)
        if ref is not None:
            return value if self.leaf(ref, path, ctx) else _GONE
        if isinstance(value, dict):
            return self.mapping(value, path, ctx)
        if isinstance(value, (list, tuple)):
            return self.sequence(value, path, ctx)
        if isinstance(value, (set, frozenset)):
            for member in value:
                self.node(member, path, _OTHER if ctx.role is None else ctx)
        return value

    def sequence(self, value: Any, path: tuple[str | int, ...], ctx: _Ctx) -> Any:
        kept: list[Any] = []
        changed = False
        for index, element in enumerate(value):
            result = self.node(element, (*path, index), ctx)
            if result is _GONE:
                changed = True
                continue
            changed = changed or result is not element
            kept.append(result)
        if value and not kept:
            return _GONE
        if not changed:
            return value
        return tuple(kept) if isinstance(value, tuple) else kept

    def operands(self, value: Any, path: tuple[str | int, ...]) -> Any:
        """``{"operands": [op?, a, b, ...]}``: ``+`` sums, ``-`` is ``a - (b + ...)``."""
        if not isinstance(value, (list, tuple)) or not value:
            return self.node(value, path, _OTHER)
        marked = isinstance(value[0], str) and value[0] in {"+", "-", "*", "/"}
        operator = value[0] if marked else "+"
        start = 1 if marked else 0
        kept: list[Any] = list(value[:start])
        changed = False
        minuend_gone = False
        for index in range(start, len(value)):
            if operator in {"*", "/"}:
                ctx = _OTHER
            elif operator == "-":
                ctx = _Ctx(sign=1, minuend=True) if index == start else _Ctx(sign=-1)
            else:
                ctx = _Ctx(sign=1)
            result = self.node(value[index], (*path, index), ctx)
            if result is _GONE:
                if operator == "-" and index == start:
                    minuend_gone = True
                changed = True
                continue
            changed = changed or result is not value[index]
            kept.append(result)
        if len(kept) == start:
            return _GONE
        if minuend_gone:
            raise _Unremovable
        if not changed:
            return value
        return tuple(kept) if isinstance(value, tuple) else kept

    def mapping(self, value: dict, path: tuple[str | int, ...], ctx: _Ctx) -> Any:
        raw_op = value.get("op")
        op = raw_op if isinstance(raw_op, str) else None
        upper = (op or "").upper()
        in_sum = ctx.role is None
        # A node nested in a sum is no longer the sum's bare minuend term.
        inner = replace(ctx, minuend=False) if in_sum else ctx
        other = _OTHER if in_sum else ctx
        if (
            op == "IF"
            and self.evaluate_condition is not None
            and isinstance(value.get("args"), (list, tuple))
            and len(value["args"]) == 3
        ):
            args = value["args"]
            self.node(args[0], (*path, "args", 0), _Ctx("condition"))
            condition = self.evaluate_condition(args[0])
            if condition is not None:
                branch = 1 if condition != 0 else 2
                self.node(args[branch], (*path, "args", branch), other)
            return value
        results: dict[Any, Any] = {}
        for key, child in value.items():
            child_path = (*path, key)
            if upper == "IFERROR":
                # FormulaSpec has no authenticated fallback policy: the primary expression is the
                # only live operand (dependency_graph_eval._eval_expr).
                results[key] = self.node(child, child_path, inner) if key == "expr" else child
            elif key == "op":
                results[key] = child
            elif op in {"+", "SUM"} and key == "args":
                # A sum's terms are additive wherever the sum sits: inside a non-removable
                # slot (a driver's ``base``, a ratio's ``numerator``) a term still leaves while
                # another remains; only an emptied sum makes the enclosing slot go.
                results[key] = self.node(child, child_path, inner if in_sum else _Ctx(sign=1))
            elif op == "NEG" and key == "arg":
                results[key] = self.node(
                    child, child_path, _Ctx(sign=-ctx.sign) if in_sum else ctx
                )
            elif op == "-" and key == "left":
                results[key] = self.node(
                    child, child_path, _Ctx(sign=ctx.sign, minuend=True) if in_sum else ctx
                )
            elif op == "-" and key == "right":
                results[key] = self.node(
                    child, child_path, _Ctx(sign=-ctx.sign) if in_sum else ctx
                )
            elif op == "IF" and key == "args" and isinstance(child, (list, tuple)):
                results[key] = self.if_args(child, child_path, other)
            elif (
                upper in {"*", "MUL"}
                and key == "args"
                and in_sum
                and not ctx.minuend
                and isinstance(child, (list, tuple))
            ):
                # A product that is a term of a sum: its factors are that term's, and a
                # selected one takes the whole term out of the sum (the term has no value
                # without it). Any other position keeps a selected factor unremovable.
                results[key] = self.factors(child, child_path, _Ctx(sign=ctx.sign))
            else:
                results[key] = self.node(child, child_path, other)
        if any(result is _GONE for result in results.values()):
            if op == "-" and "left" in results:
                if results["left"] is _GONE:
                    # A difference with no operand left leaves its sum; a subtrahend
                    # alone cannot stand without its minuend.
                    if results.get("right") is _GONE:
                        return _GONE
                    raise _Unremovable
                return results["left"]
            return _GONE
        if all(results[key] is value[key] for key in value):
            return value
        return results

    def factors(self, value: Any, path: tuple[str | int, ...], ctx: _Ctx) -> Any:
        """A product term: ``_GONE`` when any ref inside its factors is selected."""
        kept: list[Any] = []
        selected = self.selected
        for index, element in enumerate(value):
            try:
                result = self.node(element, (*path, index), ctx)
            except _Unremovable:
                # A selected ref nested where it cannot leave: the term goes with it.
                continue
            if result is not _GONE:
                kept.append(result)
        if self.selected != selected:
            return _GONE
        if all(new is old for new, old in zip(kept, value)):
            return value
        return tuple(kept) if isinstance(value, tuple) else kept

    def if_args(self, args: Any, path: tuple[str | int, ...], other: _Ctx) -> Any:
        kept = [
            self.node(arg, (*path, index), _Ctx("condition") if index == 0 else other)
            for index, arg in enumerate(args)
        ]
        if any(result is _GONE for result in kept):
            return _GONE
        if all(new is old for new, old in zip(kept, args)):
            return args
        return tuple(kept) if isinstance(args, tuple) else kept


def _key_ctx(spec: FormulaSpec, key: str, params: dict) -> _Ctx:
    """The role context of one top-level ``params`` key of ``spec``."""
    kind = spec.type
    if kind is FormulaType.arithmetic:
        function = params.get("function")
        if key == "expr":
            return _Ctx(sign=1)
        if key == "items" and function in (None, "SUM"):
            return _Ctx(sign=1)
        if key == "target" and function == "SUM_RANGE":
            return _Ctx("target")
    elif kind is FormulaType.ref:
        if key == "source":
            return _Ctx("source")
    elif kind is FormulaType.ratio:
        if key in ("numerator", "denominator"):
            return _Ctx(key)
    elif kind in (FormulaType.driver, FormulaType.growth):
        if key in ("base", "rate"):
            return _Ctx(key)
    elif kind is FormulaType.roll_forward:
        if key == "beginning":
            return _Ctx("beginning")
        if key == "additions":
            return _Ctx(sign=1)
        if key == "subtractions":
            return _Ctx(sign=-1)
    elif kind is FormulaType.valuation:
        if key == "anchor" and spec.subtype == "offset_scenario":
            return _Ctx("anchor")
        if key in _WACC_ZERO_SLOTS and spec.subtype == "wacc":
            return _Ctx(_WACC_ZERO_SLOTS[key])
    return _OTHER


def _walk_spec(spec: FormulaSpec, walk: _Walk) -> Optional[FormulaSpec]:
    """Run ``walk`` over ``spec``'s live params; the spec (identity if untouched) or ``None``."""
    params = spec.params or {}
    # Retained operand lineage records how a value was derived and is never a live ref: a
    # constant computes only its ``value`` and any formula's ``lineage`` slot is a literal's record.
    live = (
        tuple(key for key in params if key == "value")
        if spec.type is FormulaType.constant
        else tuple(key for key in params if key != "lineage")
    )
    rewritten = dict(params)
    changed = False
    try:
        for key in live:
            path: tuple[str | int, ...] = (key,)
            if spec.type is FormulaType.arithmetic and key == "operands":
                result = walk.operands(params[key], path)
            else:
                result = walk.node(params[key], path, _key_ctx(spec, key, params))
            if result is _GONE:
                if spec.type is FormulaType.roll_forward and key in ("additions", "subtractions"):
                    # A roll-forward with no additions or subtractions is still its beginning.
                    result = type(params[key])()
                elif (
                    spec.type is FormulaType.valuation
                    and spec.subtype == "wacc"
                    and key in _WACC_ZERO_SLOTS
                ):
                    # A wacc without a debt cost or weight is its equity term: the slot becomes
                    # the literal 0, which every reader already evaluates as a zero debt term.
                    rewritten[key] = 0
                    changed = True
                    continue
                else:
                    return None
            if result is not params[key]:
                rewritten[key] = result
                changed = True
    except _Unremovable:
        return None
    return spec.model_copy(update={"params": rewritten}) if changed else spec


def formula_references(
    spec: FormulaSpec,
    *,
    evaluate_condition: Optional[Callable[[Any], Optional[float]]] = None,
) -> tuple[FormulaRef, ...]:
    """Every live ref of ``spec`` with its role, in params order (``FormulaSpec.references``).

    With ``evaluate_condition`` an ``IF`` contributes its condition and the branch the condition
    selects, as the evaluator reads it.
    """
    walk = _Walk(None, evaluate_condition)
    _walk_spec(spec, walk)
    return tuple(walk.found)


def formula_without(
    spec: FormulaSpec, predicate: Callable[[FormulaRef], bool]
) -> Optional[FormulaSpec]:
    """``spec`` without the refs ``predicate`` selects (``FormulaSpec.without``).

    A selected ``additive`` / ``subtractive`` term leaves its sum, and a product term leaves it
    whole when any of its factors is selected; the last term of a sum leaves no formula
    (``None``), as does any selected ref in another role. ``spec`` itself, with its
    ``params`` untouched, when the predicate selects nothing.
    """
    return _walk_spec(spec, _Walk(predicate))
