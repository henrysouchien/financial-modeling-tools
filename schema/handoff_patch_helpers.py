from __future__ import annotations

_EMPTY_TARGET_NORMALIZED_OPS = frozenset({"register_sources", "add_assumption"})
_SOURCE_REF_LIST_KEYS = frozenset(
    {"citations", "evidence", "resolution_source_refs", "source_refs"}
)
_SOURCE_REF_SCALAR_KEYS = frozenset({"source_ref"})


def _normalize_empty_target(value: object) -> object:
    if not isinstance(value, dict):
        return value
    if value.get("op") in _EMPTY_TARGET_NORMALIZED_OPS and value.get("target") == {}:
        normalized = dict(value)
        normalized["target"] = None
        return normalized
    return value


def _rewrite_source_refs_recursive(value: object, source_ref_map: dict[str, str]) -> object:
    if not source_ref_map:
        return value
    if isinstance(value, dict):
        rewritten: dict[str, object] = {}
        for key, child in value.items():
            if key in _SOURCE_REF_LIST_KEYS and isinstance(child, list):
                rewritten[key] = [
                    _rewrite_source_refs_recursive(item, source_ref_map)
                    if isinstance(item, (dict, list))
                    else _rewrite_source_ref_leaf(item, source_ref_map)
                    for item in child
                ]
                continue
            if key in _SOURCE_REF_SCALAR_KEYS:
                rewritten[key] = (
                    _rewrite_source_refs_recursive(child, source_ref_map)
                    if isinstance(child, (dict, list))
                    else _rewrite_source_ref_leaf(child, source_ref_map)
                )
                continue
            rewritten[key] = _rewrite_source_refs_recursive(child, source_ref_map)
        return rewritten
    if isinstance(value, list):
        return [_rewrite_source_refs_recursive(item, source_ref_map) for item in value]
    return value


def _rewrite_source_ref_leaf(value: object, source_ref_map: dict[str, str]) -> object:
    normalized = str(value or "").strip()
    if not normalized:
        return value
    return source_ref_map.get(normalized, normalized)


def _next_split_op_id(base: str, existing_op_ids: set[str]) -> str:
    candidate = f"{base}_held_at_base"
    if candidate not in existing_op_ids:
        existing_op_ids.add(candidate)
        return candidate
    index = 2
    while f"{candidate}_{index}" in existing_op_ids:
        index += 1
    split_id = f"{candidate}_{index}"
    existing_op_ids.add(split_id)
    return split_id


def _split_inline_assumption_held_at_base(
    ops: list[object],
) -> list[object]:
    existing_op_ids = {
        str(op.get("op_id"))
        for op in ops
        if isinstance(op, dict) and op.get("op_id") is not None
    }
    normalized_ops: list[object] = []
    for op in ops:
        if not isinstance(op, dict) or op.get("op") != "add_assumption":
            normalized_ops.append(op)
            continue
        value = op.get("value")
        if not isinstance(value, dict) or not isinstance(value.get("held_at_base"), bool):
            normalized_ops.append(op)
            continue
        assumption_id = str(value.get("assumption_id") or "").strip()
        if not assumption_id:
            normalized_ops.append(op)
            continue
        held_at_base = bool(value["held_at_base"])
        normalized_value = dict(value)
        normalized_value.pop("held_at_base", None)
        normalized_op = dict(op)
        normalized_op["value"] = normalized_value
        normalized_ops.append(normalized_op)
        normalized_ops.append(
            {
                "op": "set_assumption_held_at_base",
                "op_id": _next_split_op_id(
                    str(op.get("op_id") or "add_assumption"),
                    existing_op_ids,
                ),
                "reason": str(op.get("reason") or "Set assumption held-at-base flag"),
                "target": {"assumption_id": assumption_id},
                "value": held_at_base,
            }
        )
    return normalized_ops


def _normalize_patch_batch_payload(value: object) -> object:
    if not isinstance(value, dict):
        return value
    raw_ops = value.get("ops")
    if not isinstance(raw_ops, list):
        return value
    source_ref_map: dict[str, str] = {}
    normalized_ops = [dict(op) if isinstance(op, dict) else op for op in raw_ops]
    normalized_ops = [
        _rewrite_source_refs_recursive(op, source_ref_map)
        for op in normalized_ops
    ]
    normalized_ops = _split_inline_assumption_held_at_base(normalized_ops)
    normalized = dict(value)
    normalized["ops"] = normalized_ops
    return normalized


_VALUATION_NUMERIC_FIELDS = frozenset(
    {
        "low",
        "mid",
        "high",
        "current_multiple",
        "wacc",
        "risk_free_rate",
        "equity_risk_premium",
        "cost_of_equity",
        "raw_beta",
        "adjusted_beta",
        "beta_floor",
        "terminal_growth_rate",
        "terminal_multiple",
    }
)
_VALUATION_RATE_FIELDS = frozenset(
    {
        "wacc",
        "risk_free_rate",
        "equity_risk_premium",
        "cost_of_equity",
        "terminal_growth_rate",
    }
)
_VALUATION_STRING_FIELDS = frozenset({"method", "rationale"})
