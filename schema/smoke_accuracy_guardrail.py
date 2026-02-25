#!/usr/bin/env python3
"""Accuracy smoke tests with baseline + allowlist guardrails.

Usage:
  PYTHONHASHSEED=0 python3 schema/smoke_accuracy_guardrail.py --update-baseline
  PYTHONHASHSEED=0 python3 schema/smoke_accuracy_guardrail.py
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timezone
import json
import math
import os
from pathlib import Path
import sys
from typing import Dict, List, Optional, Set, Tuple

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from schema import DependencyGraph, read_model
from schema.models import ItemType
from schema.tools import _all_periods, _compute_scenario_results, _downstream_nodes, load


DEFAULT_MODELS: List[Tuple[str, int]] = [
    ("PCTY-model.xlsx", 2023),
    ("TW_simple-model.xlsx", 2024),
    ("EURN-model.xlsx", 2024),
    ("NVDA_simple-model.xlsx", 2024),
    ("SFM_model.xlsx", 2024),
    ("MSCI-model.xlsx", 2024),
]

DEFAULT_MODELS_DIR = (
    "/Users/henrychien/Library/CloudStorage/OneDrive-HenryChienLLC/Portfolio/Models"
)

DEFAULT_BASELINE_PATH = "schema/baselines/accuracy_smoke_baseline.json"
DEFAULT_ALLOWLIST_PATH = "schema/baselines/msci_known_deltas.json"

TOL = 0.01
SCENARIO_ABS_LIMIT = 1e20


@dataclass
class ModeResult:
    correct: int
    wrong: int
    missing: int
    total: int
    wrong_keys: Set[str]
    missing_keys: Set[str]

    @property
    def accuracy_pct(self) -> float:
        if self.total == 0:
            return 0.0
        return 100.0 * self.correct / self.total


def _pair_key(item_id: str, period: int) -> str:
    return f"{item_id}|{period}"


def _split_pair_key(key: str) -> Tuple[str, int]:
    item_id, period = key.rsplit("|", 1)
    return item_id, int(period)


def _iter_items(model) -> List:
    items = []
    for sheet in model.sheets.values():
        for section in sheet.sections:
            items.extend(section.line_items)
    return items


def _status(expected: float, got: Optional[float]) -> str:
    if got is None:
        return "missing"
    if abs(got - expected) < TOL:
        return "correct"
    return "wrong"


def _compute_mode_result(
    expected: Dict[Tuple[str, int], float],
    out: Dict[str, Dict[int, float]],
) -> ModeResult:
    correct = 0
    wrong = 0
    missing = 0
    wrong_keys: Set[str] = set()
    missing_keys: Set[str] = set()

    for (item_id, period), exp in expected.items():
        got = out.get(item_id, {}).get(period)
        st = _status(exp, got)
        if st == "correct":
            correct += 1
        elif st == "wrong":
            wrong += 1
            wrong_keys.add(_pair_key(item_id, period))
        else:
            missing += 1
            missing_keys.add(_pair_key(item_id, period))

    return ModeResult(
        correct=correct,
        wrong=wrong,
        missing=missing,
        total=len(expected),
        wrong_keys=wrong_keys,
        missing_keys=missing_keys,
    )


def _largest_block_activity(graph: DependencyGraph, periods: List[int]) -> Dict[str, int]:
    if not graph.cycle_blocks:
        return {
            "largest_cycle_active_periods": 0,
            "largest_cycle_total_periods": len(periods),
            "largest_active_cycle_size_max": 0,
        }

    largest = max(graph.cycle_blocks, key=lambda block: len(block.nodes))
    active_count = 0
    max_active_size = 0
    for period in periods:
        order_adj, cycle_adj = graph._active_adjs_for_period_subset(period, largest.nodes)  # noqa: SLF001
        components, _ = graph._components_from_adj(cycle_adj, order_adj=order_adj)  # noqa: SLF001
        sizes = [len(comp.nodes) for comp in components if comp.is_cycle]
        if not sizes:
            continue
        active_count += 1
        if max(sizes) > max_active_size:
            max_active_size = max(sizes)

    return {
        "largest_cycle_active_periods": active_count,
        "largest_cycle_total_periods": len(periods),
        "largest_active_cycle_size_max": max_active_size,
    }


def _scenario_guardrail(
    file_path: str,
    historical_cutoff_year: int,
) -> Optional[Dict[str, object]]:
    bundle = load(file_path, historical_cutoff_year=historical_cutoff_year)
    override_item = "assumptions.tax_rate"
    if override_item not in bundle.model._index:
        return None

    all_periods = _all_periods(bundle.model)
    overrides = {override_item: {period: 0.25 for period in all_periods}}

    recompute_ids: Set[str] = set()
    for item_id in overrides:
        recompute_ids |= _downstream_nodes(bundle.graph, item_id)

    scenario_results = _compute_scenario_results(bundle, overrides, recompute_ids)

    nonfinite_count = 0
    oversize_count = 0
    max_abs = 0.0
    max_pair: Optional[Tuple[str, int, float]] = None
    for item_id in recompute_ids:
        by_period = scenario_results.get(item_id, {})
        for period, value in by_period.items():
            if value is None:
                continue
            v = float(value)
            if not math.isfinite(v):
                nonfinite_count += 1
                continue
            abs_v = abs(v)
            if abs_v > SCENARIO_ABS_LIMIT:
                oversize_count += 1
            if abs_v > max_abs:
                max_abs = abs_v
                max_pair = (item_id, period, v)

    return {
        "override_item": override_item,
        "nonfinite_count": nonfinite_count,
        "oversize_count": oversize_count,
        "max_abs": max_abs,
        "max_pair": list(max_pair) if max_pair else None,
    }


def _run_smoke(models_dir: Path, models: List[Tuple[str, int]]) -> List[Dict[str, object]]:
    results: List[Dict[str, object]] = []
    for filename, cutoff in models:
        file_path = models_dir / filename
        if not file_path.exists():
            raise FileNotFoundError(f"Model file not found: {file_path}")

        model = read_model(str(file_path), mode="full", historical_cutoff_year=cutoff)
        graph = DependencyGraph()
        graph.build(model)

        periods = list(model.time_structure.historical_periods) + list(model.time_structure.projection_periods)
        if not periods:
            periods = list(model.time_structure.historical_years) + list(model.time_structure.projection_years)

        items = _iter_items(model)
        derived_ids = {item.id for item in items if item.item_type == ItemType.derived}

        expected: Dict[Tuple[str, int], float] = {}
        for item in items:
            if item.item_type != ItemType.derived or not item.values:
                continue
            for period in periods:
                cell = item.values.values.get(period)
                if cell is None or cell.value is None:
                    continue
                expected[(item.id, period)] = cell.value

        cached = graph.compute({})
        forced = graph.compute({}, recompute=derived_ids)

        cached_res = _compute_mode_result(expected, cached)
        forced_res = _compute_mode_result(expected, forced)

        cycle_blocks = len(graph.cycle_blocks)
        largest_cycle = max((len(block.nodes) for block in graph.cycle_blocks), default=0)
        activity = _largest_block_activity(graph, periods)

        scenario_guard = None
        if filename == "MSCI-model.xlsx":
            scenario_guard = _scenario_guardrail(str(file_path), cutoff)

        results.append(
            {
                "model": filename,
                "path": str(file_path),
                "historical_cutoff_year": cutoff,
                "period_mode": model.time_structure.period_mode,
                "cycle_blocks": cycle_blocks,
                "largest_cycle_block": largest_cycle,
                **activity,
                "modes": {
                    "cached": {
                        "correct": cached_res.correct,
                        "wrong": cached_res.wrong,
                        "missing": cached_res.missing,
                        "total": cached_res.total,
                        "accuracy_pct": round(cached_res.accuracy_pct, 4),
                        "wrong_keys": sorted(cached_res.wrong_keys),
                        "missing_keys": sorted(cached_res.missing_keys),
                    },
                    "forced": {
                        "correct": forced_res.correct,
                        "wrong": forced_res.wrong,
                        "missing": forced_res.missing,
                        "total": forced_res.total,
                        "accuracy_pct": round(forced_res.accuracy_pct, 4),
                        "wrong_keys": sorted(forced_res.wrong_keys),
                        "missing_keys": sorted(forced_res.missing_keys),
                    },
                },
                "scenario_guard": scenario_guard,
            }
        )

    return results


def _write_json(path: Path, payload: Dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _load_json(path: Path) -> Dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def _model_map(models: List[Dict[str, object]]) -> Dict[str, Dict[str, object]]:
    return {m["model"]: m for m in models}


def _validate_guardrails(
    current_models: List[Dict[str, object]],
    baseline: Dict[str, object],
    allowlist: Dict[str, object],
) -> Tuple[List[str], List[str]]:
    errors: List[str] = []
    notes: List[str] = []

    baseline_models = _model_map(baseline["models"])
    current_map = _model_map(current_models)

    for model_name, current in current_map.items():
        if model_name not in baseline_models:
            errors.append(f"{model_name}: missing from baseline")
            continue
        base = baseline_models[model_name]

        if current["cycle_blocks"] > base["cycle_blocks"]:
            errors.append(
                f"{model_name}: cycle_blocks worsened ({current['cycle_blocks']} > {base['cycle_blocks']})"
            )
        if current["largest_cycle_block"] > base["largest_cycle_block"]:
            errors.append(
                f"{model_name}: largest_cycle_block worsened "
                f"({current['largest_cycle_block']} > {base['largest_cycle_block']})"
            )
        if current["largest_cycle_active_periods"] > base["largest_cycle_active_periods"]:
            errors.append(
                f"{model_name}: largest_cycle_active_periods worsened "
                f"({current['largest_cycle_active_periods']} > {base['largest_cycle_active_periods']})"
            )

        for mode in ("cached", "forced"):
            cur_mode = current["modes"][mode]
            base_mode = base["modes"][mode]
            if cur_mode["missing"] > base_mode["missing"]:
                errors.append(
                    f"{model_name} {mode}: missing worsened ({cur_mode['missing']} > {base_mode['missing']})"
                )
            if cur_mode["wrong"] > base_mode["wrong"]:
                errors.append(
                    f"{model_name} {mode}: wrong worsened ({cur_mode['wrong']} > {base_mode['wrong']})"
                )

        if model_name == "MSCI-model.xlsx":
            for mode in ("cached", "forced"):
                cur_mode = current["modes"][mode]
                if cur_mode["missing"] != 0:
                    errors.append(f"{model_name} {mode}: expected missing=0, got {cur_mode['missing']}")

            scenario_guard = current.get("scenario_guard")
            if scenario_guard is not None:
                if scenario_guard["nonfinite_count"] > 0:
                    errors.append(
                        f"{model_name}: scenario non-finite values detected "
                        f"({scenario_guard['nonfinite_count']})"
                    )
                if scenario_guard["oversize_count"] > 0:
                    errors.append(
                        f"{model_name}: scenario oversize values > {SCENARIO_ABS_LIMIT:.0e} detected "
                        f"({scenario_guard['oversize_count']})"
                    )

            allowed = allowlist["allowed_wrong_pairs"]
            for mode in ("cached", "forced"):
                current_wrong = set(current["modes"][mode]["wrong_keys"])
                allowed_wrong = {
                    _pair_key(row["item_id"], int(row["period"]))
                    for row in allowed[mode]
                }
                unexpected = sorted(current_wrong - allowed_wrong)
                recovered = sorted(allowed_wrong - current_wrong)
                if unexpected:
                    preview = ", ".join(unexpected[:10])
                    errors.append(
                        f"{model_name} {mode}: {len(unexpected)} unexpected wrong pairs outside allowlist "
                        f"(first: {preview})"
                    )
                if recovered:
                    notes.append(f"{model_name} {mode}: {len(recovered)} allowlisted wrong pairs recovered")

    return errors, notes


def _print_summary(models: List[Dict[str, object]]) -> None:
    for model in models:
        name = model["model"]
        cached = model["modes"]["cached"]
        forced = model["modes"]["forced"]
        print(
            f"{name}: "
            f"cached={cached['correct']}/{cached['total']} ({cached['accuracy_pct']:.4f}%) "
            f"wrong={cached['wrong']} missing={cached['missing']} | "
            f"forced={forced['correct']}/{forced['total']} ({forced['accuracy_pct']:.4f}%) "
            f"wrong={forced['wrong']} missing={forced['missing']}"
        )
    print()


def main() -> int:
    parser = argparse.ArgumentParser(description="Run accuracy smoke tests with guardrails.")
    parser.add_argument("--models-dir", default=DEFAULT_MODELS_DIR)
    parser.add_argument("--baseline-path", default=DEFAULT_BASELINE_PATH)
    parser.add_argument("--allowlist-path", default=DEFAULT_ALLOWLIST_PATH)
    parser.add_argument(
        "--update-baseline",
        action="store_true",
        help="Rewrite baseline + allowlist from current smoke results.",
    )
    args = parser.parse_args()

    models_dir = Path(args.models_dir)
    baseline_path = Path(args.baseline_path)
    allowlist_path = Path(args.allowlist_path)

    if os.environ.get("PYTHONHASHSEED") != "0":
        print("warning: PYTHONHASHSEED is not 0; run may not be perfectly deterministic")

    smoke_models = _run_smoke(models_dir, DEFAULT_MODELS)
    _print_summary(smoke_models)

    now = datetime.now(timezone.utc).isoformat()

    if args.update_baseline:
        baseline_payload: Dict[str, object] = {
            "version": 1,
            "generated_at": now,
            "pythonhashseed": os.environ.get("PYTHONHASHSEED"),
            "tolerance": TOL,
            "models": [],
        }
        for model in smoke_models:
            baseline_payload["models"].append(
                {
                    "model": model["model"],
                    "path": model["path"],
                    "historical_cutoff_year": model["historical_cutoff_year"],
                    "period_mode": model["period_mode"],
                    "cycle_blocks": model["cycle_blocks"],
                    "largest_cycle_block": model["largest_cycle_block"],
                    "largest_cycle_active_periods": model["largest_cycle_active_periods"],
                    "largest_cycle_total_periods": model["largest_cycle_total_periods"],
                    "largest_active_cycle_size_max": model["largest_active_cycle_size_max"],
                    "modes": {
                        "cached": {
                            key: model["modes"]["cached"][key]
                            for key in ("correct", "wrong", "missing", "total", "accuracy_pct")
                        },
                        "forced": {
                            key: model["modes"]["forced"][key]
                            for key in ("correct", "wrong", "missing", "total", "accuracy_pct")
                        },
                    },
                    "scenario_guard": model.get("scenario_guard"),
                }
            )

        msci_model = next((m for m in smoke_models if m["model"] == "MSCI-model.xlsx"), None)
        if msci_model is None:
            raise RuntimeError("MSCI-model.xlsx not found in smoke run; cannot write allowlist")

        allowlist_payload: Dict[str, object] = {
            "version": 1,
            "generated_at": now,
            "model": "MSCI-model.xlsx",
            "tolerance": TOL,
            "allowed_wrong_pairs": {"cached": [], "forced": []},
        }
        for mode in ("cached", "forced"):
            keys = sorted(msci_model["modes"][mode]["wrong_keys"])
            for key in keys:
                item_id, period = _split_pair_key(key)
                allowlist_payload["allowed_wrong_pairs"][mode].append(
                    {
                        "item_id": item_id,
                        "period": period,
                        "category": "known_non_structural",
                    }
                )

        _write_json(baseline_path, baseline_payload)
        _write_json(allowlist_path, allowlist_payload)
        print(f"updated baseline: {baseline_path}")
        print(f"updated allowlist: {allowlist_path}")
        return 0

    if not baseline_path.exists():
        print(f"error: baseline not found: {baseline_path}")
        return 1
    if not allowlist_path.exists():
        print(f"error: allowlist not found: {allowlist_path}")
        return 1

    baseline = _load_json(baseline_path)
    allowlist = _load_json(allowlist_path)
    errors, notes = _validate_guardrails(smoke_models, baseline, allowlist)

    for note in notes:
        print(f"note: {note}")

    if errors:
        for err in errors:
            print(f"error: {err}")
        return 1

    print("guardrails: PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
