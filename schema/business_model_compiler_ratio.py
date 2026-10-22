"""The one owner of a derived ratio's expression shape.

A ``ratio`` node's value is its numerator over its denominator, converted from the
operands' units/scales to the node's own (``convert_scale``) and, where the denominator
node is a ``period_average`` series observed at period ends, over the two-point mean of
the opening and ending observations. The historical lane
(`schema.build_bm_node_history.RegisteredBmFormula`) and the projected lane
(`schema.business_model_compiler_formula._compile_formula`) both build the operands here,
so the two cannot state the node differently.
"""

from __future__ import annotations

from typing import Any

from .business_model import NodeMeasure, product_scale_exponent


def ratio_scale_exponent(
    numerator: NodeMeasure, denominator: NodeMeasure, output: NodeMeasure
) -> int:
    """The power of ten that converts ``numerator / denominator`` to ``output``.

    The denominator is read at unit scale: a quotient's scale is the numerator's,
    less the denominator's, less the output's.
    """

    return product_scale_exponent([numerator], output) - product_scale_exponent(
        [denominator], NodeMeasure(denominator.unit, "units", basis=denominator.basis)
    )


def scaled(operand: Any, multiplier: float) -> Any:
    """``operand`` converted by ``multiplier``; the operand itself when it is one."""

    if multiplier == 1:
        return operand
    return {"op": "*", "args": [operand, float(multiplier)]}


def period_mean(opening: Any, ending: Any) -> Any:
    """The two-point mean of a series' opening and ending observations."""

    return {"op": "AVG", "args": [opening, ending]}


def ratio_terms(
    numerator: Any,
    denominator: Any,
    *,
    multiplier: float = 1.0,
    opening_denominator: Any | None = None,
) -> tuple[Any, Any]:
    """The numerator and denominator operands of a derived ratio.

    ``multiplier`` is the unit/scale conversion (`ratio_scale_exponent`);
    ``opening_denominator`` is the denominator's prior-period operand when the
    denominator is an average series (`ratio_averages_operand`), else ``None``.
    """

    return (
        scaled(numerator, multiplier),
        denominator
        if opening_denominator is None
        else period_mean(opening_denominator, denominator),
    )


def row_holds_average_series(node: Any) -> bool:
    """Whether a node's own row is the average series its declaration names.

    A ``growth`` node declared ``period_average`` is projected from its own
    prior value, so its history is the average the projection compounds: the
    two-point mean of the reported period-end observations
    (`schema.build_bm_node_history._apply_identity`), never the period-end stock.
    """

    expression = getattr(node, "driver", None)
    return (
        getattr(node, "observation_basis", None) == "period_average"
        and expression is not None
        and expression.type == "growth"
    )


def ratio_averages_operand(node: Any) -> bool:
    """Whether a ratio over ``node`` takes the mean of its opening and ending values.

    A ``period_average`` node whose row holds the period-end observations is averaged
    by its consumer; one whose row already is the average
    (`row_holds_average_series`) is divided by directly, never averaged twice.
    """

    return getattr(node, "observation_basis", None) == "period_average" and not (
        row_holds_average_series(node)
    )


def pinned_series_holds_average(
    series: str | None, strategies: Any, *, period_id: str | None = None
) -> bool:
    """Whether the pin already computes ``series`` as a period average.

    The coverage profile's derived-KPI strategy owns that basis: an
    ``arithmetic_mean`` strategy over one required role at period offsets
    -1 and 0 (the opening and ending period-end observations) publishes
    its output role as the average itself, so a node claiming that series
    holds the pinned observation, not a mean of pinned observations.
    """

    if series is None:
        return False
    for strategy in strategies or ():
        if getattr(strategy, "output_role", None) != series:
            continue
        if getattr(strategy, "formula", None) != "arithmetic_mean":
            continue
        if period_id is not None and period_id not in (
            getattr(strategy, "period_ids", ()) or ()
        ):
            continue
        inputs = getattr(strategy, "inputs", ()) or ()
        roles = {getattr(row, "required_role", None) for row in inputs}
        offsets = {getattr(row, "period_offset", None) for row in inputs}
        if len(inputs) == 2 and len(roles) == 1 and offsets == {-1, 0}:
            return True
    return False


__all__ = [
    "period_mean",
    "pinned_series_holds_average",
    "ratio_averages_operand",
    "ratio_scale_exponent",
    "ratio_terms",
    "row_holds_average_series",
    "scaled",
]
