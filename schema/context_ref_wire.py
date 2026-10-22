"""Wire context_ref values: canonical encode_*_context_ref locators and cxh1
exact-context handles (schema/context_handle.py), both preserved whole under
projection elision.

Consumers (result-projection elision) ask whether a string is one of these
values instead of listing prefixes. The admitted encoder set is the decode
functions imported below; tests/schema/test_context_ref_wire.py fails if a new
encode_*_context_ref is added without a decoder registered here.
"""

from __future__ import annotations

from .context_handle import is_context_handle
from .forecast_assumptions_context import decode_forecast_assumptions_context_ref
from .historical_foundation_context import decode_historical_foundation_context_ref
from .postcompile_valuation_context import decode_postcompile_valuation_context_ref
from .scenario_set_context import decode_scenario_set_context_ref
from .valuation_policy_context import decode_valuation_policy_context_ref

CONTEXT_REF_DECODERS = (
    decode_forecast_assumptions_context_ref,
    decode_historical_foundation_context_ref,
    decode_scenario_set_context_ref,
    decode_valuation_policy_context_ref,
    decode_postcompile_valuation_context_ref,
)

CONTEXT_REF_ENCODER_NAMES: tuple[str, ...] = tuple(
    decode.__name__.replace("decode_", "encode_", 1)
    for decode in CONTEXT_REF_DECODERS
)


def is_context_ref_wire_value(value: object) -> bool:
    if type(value) is not str or not value:
        return False
    if is_context_handle(value):
        return True
    for decode in CONTEXT_REF_DECODERS:
        try:
            decode(value)
        except ValueError:
            continue
        return True
    return False


__all__ = [
    "CONTEXT_REF_DECODERS",
    "CONTEXT_REF_ENCODER_NAMES",
    "is_context_ref_wire_value",
]
