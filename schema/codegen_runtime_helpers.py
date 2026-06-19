"""Emit generated-code runtime helper sections."""

from __future__ import annotations

from typing import Any


def _emit_helpers(emitter: Any) -> None:
    emitter.comment("Helpers")

    emitter.line("def _bootstrap_period(period: int, t: int) -> Optional[int]:")
    with emitter.indent():
        emitter.line("if PERIOD_MODE == 'yearly':")
        with emitter.indent():
            emitter.line("return period + t")
        emitter.line("if PERIOD_MODE != 'quarterly5':")
        with emitter.indent():
            emitter.line("raise ValueError(f'Unknown period mode: {PERIOD_MODE}')")
        emitter.line("if t == 0:")
        with emitter.indent():
            emitter.line("return period")
        emitter.line("year = period // 10")
        emitter.line("slot = period % 10")
        emitter.line("if slot < 1 or slot > 5:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("index = year * 5 + (slot - 1)")
        emitter.line("shifted = index + t")
        emitter.line("if shifted < 0:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("shifted_year = shifted // 5")
        emitter.line("shifted_slot = shifted % 5 + 1")
        emitter.line("return shifted_year * 10 + shifted_slot")
    emitter.blank()

    emitter.line("def val(r: Dict[str, Dict[int, Optional[float]]], item_id: str, period: int, t: int = 0) -> Optional[float]:")
    with emitter.indent():
        emitter.line("idx = _PERIOD_INDEX.get(period)")
        emitter.line("if idx is None:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("if t == 0:")
        with emitter.indent():
            emitter.line("v = r.get(item_id, {}).get(period)")
            emitter.line("if v is not None:")
            with emitter.indent():
                emitter.line("return v")
            emitter.line("return INPUT_CACHED.get(item_id, {}).get(period)")
        emitter.line("target_idx = idx + t")
        emitter.line("if 0 <= target_idx < len(PERIODS):")
        with emitter.indent():
            emitter.line("target_period = PERIODS[target_idx]")
            emitter.line("v = r.get(item_id, {}).get(target_period)")
            emitter.line("if v is not None:")
            with emitter.indent():
                emitter.line("return v")
            emitter.line("return INPUT_CACHED.get(item_id, {}).get(target_period)")
        emitter.line("if target_idx < 0:")
        with emitter.indent():
            emitter.line("target_period = _bootstrap_period(period, t)")
            emitter.line("if target_period is None:")
            with emitter.indent():
                emitter.line("return None")
            emitter.line("return INPUT_CACHED.get(item_id, {}).get(target_period)")
        emitter.line("return None")
    emitter.blank()

    emitter.line("def safe_sum(*values: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("non_none = [v for v in values if v is not None]")
        emitter.line("if not non_none:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("return sum(non_none)")
    emitter.blank()

    emitter.line("def safe_avg(*values: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("non_none = [v for v in values if v is not None]")
        emitter.line("if not non_none:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("return sum(non_none) / len(non_none)")
    emitter.blank()

    emitter.line("def safe_median(*values: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("non_none = sorted(v for v in values if v is not None)")
        emitter.line("if not non_none:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("midpoint = len(non_none) // 2")
        emitter.line("if len(non_none) % 2:")
        with emitter.indent():
            emitter.line("return non_none[midpoint]")
        emitter.line("return (non_none[midpoint - 1] + non_none[midpoint]) / 2")
    emitter.blank()

    emitter.line("def safe_items(*values: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("if any(v is None for v in values):")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("return sum(values)")
    emitter.blank()

    emitter.line("def expr_add(*values: Optional[float]) -> float:")
    with emitter.indent():
        emitter.line("return sum(v or 0 for v in values)")
    emitter.blank()

    emitter.line("def expr_mul(*values: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("if any(v is None for v in values):")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("result = 1.0")
        emitter.line("for value in values:")
        with emitter.indent():
            emitter.line("result *= value")
        emitter.line("return result")
    emitter.blank()

    emitter.line("def expr_max(*values: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("non_none = [v for v in values if v is not None]")
        emitter.line("if not non_none:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("return max(non_none)")
    emitter.blank()

    emitter.line("def expr_iferror(value: Optional[float], fallback: Optional[float] = 0.0) -> Optional[float]:")
    with emitter.indent():
        emitter.line("return value if value is not None else fallback")
    emitter.blank()

    emitter.line("def safe_sub(left: Optional[float], right: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("if left is None or right is None:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("return left - right")
    emitter.blank()

    emitter.line("def safe_chain_sub(*values: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("if not values:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("if any(v is None for v in values):")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("return values[0] - sum(values[1:])")
    emitter.blank()

    emitter.line("def safe_mul(*values: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("if not values:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("if any(v is None for v in values):")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("result = 1.0")
        emitter.line("for value in values:")
        with emitter.indent():
            emitter.line("result *= value")
        emitter.line("return result")
    emitter.blank()

    emitter.line("def safe_div(left: Optional[float], right: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("if left is None or right is None:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("if right == 0:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("return left / right")
    emitter.blank()

    emitter.line("def safe_chain_div(*values: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("if not values:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("if any(v is None for v in values):")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("result = values[0]")
        emitter.line("for value in values[1:]:")
        with emitter.indent():
            emitter.line("if value == 0:")
            with emitter.indent():
                emitter.line("return None")
            emitter.line("result /= value")
        emitter.line("return result")
    emitter.blank()

    emitter.line("def _negate(value: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("return None if value is None else -value")
    emitter.blank()

    emitter.line("def _adjust(value: Optional[float], adjustment: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("if value is None:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("if adjustment is None:")
        with emitter.indent():
            emitter.line("return value")
        emitter.line("return value + adjustment")
    emitter.blank()

    emitter.line("def _apply_scale_fn(value: float, scale_fn: str) -> float:")
    with emitter.indent():
        emitter.line("scale_fn = scale_fn.strip()")
        emitter.line("if not scale_fn:")
        with emitter.indent():
            emitter.line("return value")
        emitter.line("if scale_fn.startswith('/'):")
        with emitter.indent():
            emitter.line("return value / float(scale_fn[1:])")
        emitter.line("if scale_fn.startswith('*'):")
        with emitter.indent():
            emitter.line("return value * float(scale_fn[1:])")
        emitter.line("return value")
    emitter.blank()

    emitter.line(
        "def _driver(base: Optional[float], rate: Optional[float], scale: Optional[float] = None, scale_fn: Optional[str] = None) -> Optional[float]:"
    )
    with emitter.indent():
        emitter.line("if base is None or rate is None:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("result = base * rate")
        emitter.line("if scale:")
        with emitter.indent():
            emitter.line("result /= float(scale)")
        emitter.line("if isinstance(scale_fn, str):")
        with emitter.indent():
            emitter.line("result = _apply_scale_fn(result, scale_fn)")
        emitter.line("return result")
    emitter.blank()

    emitter.line("def _ratio_sub1(numerator: Optional[float], denominator: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("value = safe_div(numerator, denominator)")
        emitter.line("if value is None:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("return value - 1")
    emitter.blank()

    emitter.line("def _growth(base: Optional[float], rate: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("if base is None or rate is None:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("return base * (1 + rate)")
    emitter.blank()

    emitter.line(
        "def _roll_fwd(beginning: Optional[float], additions: List[Optional[float]], subtractions: List[Optional[float]]) -> Optional[float]:"
    )
    with emitter.indent():
        emitter.line("if beginning is None:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("return beginning + sum(v or 0 for v in additions) - sum(v or 0 for v in subtractions)")
    emitter.blank()

    emitter.line("def _pow(left: Optional[float], right: Optional[float]) -> Optional[float]:")
    with emitter.indent():
        emitter.line("if left is None or right is None:")
        with emitter.indent():
            emitter.line("return None")
        emitter.line("try:")
        with emitter.indent():
            emitter.line("return left ** right")
        emitter.line("except (ValueError, OverflowError):")
        with emitter.indent():
            emitter.line("return None")
    emitter.blank()


def _emit_entry_point(emitter: Any) -> None:
    emitter.comment("Entry point")

    emitter.line("def _filter_items(results: Dict[str, Dict[int, Optional[float]]], item_ids: Optional[List[str]]) -> Dict[str, Dict[int, Optional[float]]]:")
    with emitter.indent():
        emitter.line("if not item_ids:")
        with emitter.indent():
            emitter.line("return results")
        emitter.line("return {item_id: results.get(item_id, {}) for item_id in item_ids}")
    emitter.blank()

    emitter.line("def _write_csv(path: str, results: Dict[str, Dict[int, Optional[float]]]) -> None:")
    with emitter.indent():
        emitter.line("with open(path, 'w', newline='', encoding='utf-8') as f:")
        with emitter.indent():
            emitter.line("writer = csv.writer(f)")
            emitter.line("writer.writerow(['item_id'] + [str(period) for period in PERIODS])")
            emitter.line("for item_id in sorted(results.keys()):")
            with emitter.indent():
                emitter.line("row = [item_id] + [results.get(item_id, {}).get(period) for period in PERIODS]")
                emitter.line("writer.writerow(row)")
    emitter.blank()

    emitter.line("def main() -> None:")
    with emitter.indent():
        emitter.line("parser = argparse.ArgumentParser(description='Run generated financial model.')")
        emitter.line("parser.add_argument('--json', action='store_true', help='Print JSON output.')")
        emitter.line("parser.add_argument('--csv', type=str, help='Write CSV output to the given path.')")
        emitter.line("parser.add_argument('--items', nargs='*', help='Limit output to listed item IDs.')")
        emitter.line("args = parser.parse_args()")
        emitter.line("results = compute(default_assumptions())")
        emitter.line("results = _filter_items(results, args.items)")
        emitter.line("if args.csv:")
        with emitter.indent():
            emitter.line("_write_csv(args.csv, results)")
        emitter.line("if args.json or not args.csv:")
        with emitter.indent():
            emitter.line("print(json.dumps(results, sort_keys=True, indent=2))")
    emitter.blank()

    emitter.line("if __name__ == '__main__':")
    with emitter.indent():
        emitter.line("main()")


__all__ = ["_emit_entry_point", "_emit_helpers"]
