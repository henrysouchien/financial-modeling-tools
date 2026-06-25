from __future__ import annotations

from collections.abc import Callable
from typing import Optional

from .models import FormulaSpec, FormulaType


RawSubtype = Callable[[str], Optional[str]]


def raw_spec(formula: str, *, raw_subtype_fn: RawSubtype) -> FormulaSpec:
    params = {"formula": formula}
    subtype = raw_subtype_fn(formula)
    if subtype == "data_table":
        params["feature"] = "data_table"
    return FormulaSpec(type=FormulaType.raw, subtype=subtype, params=params)


def raw_subtype(formula: str) -> Optional[str]:
    text = str(formula or "").strip()
    if "f_type=dataTable" in text:
        return "data_table"
    if "#REF!" in text.upper():
        return "broken_ref"
    if "&" in text:
        return "string_concat"
    return None
