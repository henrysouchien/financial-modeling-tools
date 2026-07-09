"""CLI wrapper for schema.tools — run with ``python -m schema.tools_cli``."""

from __future__ import annotations

import argparse
import json
import sys
from typing import Dict


def _parse_override(raw: str) -> tuple[str, int, float]:
    """Parse ``item_id:period=value`` into (item_id, period, value)."""
    try:
        left, value_str = raw.split("=", 1)
        item_id, period_str = left.rsplit(":", 1)
        return item_id, int(period_str), float(value_str)
    except (ValueError, TypeError):
        raise argparse.ArgumentTypeError(
            f"Bad override format: {raw!r}  (expected ITEM_ID:PERIOD=VALUE)"
        )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="python -m schema.tools_cli",
        description="CLI for schema financial-model tools",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    # -- shared args helper --------------------------------------------------
    def add_common(p: argparse.ArgumentParser) -> None:
        p.add_argument("file_path", help="Path to the Excel model file")
        p.add_argument("--cutoff", type=int, default=None, dest="cutoff",
                       help="Historical cutoff year (default: current year)")

    # summarize
    p_sum = sub.add_parser("summarize", help="Summarize model structure")
    add_common(p_sum)

    # find
    p_find = sub.add_parser("find", help="Search for line items by name")
    add_common(p_find)
    p_find.add_argument("query", help="Search query string")
    p_find.add_argument("--limit", type=int, default=20)

    # drivers
    p_drv = sub.add_parser("drivers", help="Show upstream driver tree")
    add_common(p_drv)
    p_drv.add_argument("item_id", help="Fully-qualified item ID")
    p_drv.add_argument("--depth", type=int, default=3)

    # sensitivity
    p_sens = sub.add_parser("sensitivity", help="Rank inputs by impact on a target")
    add_common(p_sens)
    p_sens.add_argument("target_id", help="Target item ID")
    p_sens.add_argument("--n", type=int, default=15)
    p_sens.add_argument("--bump", type=float, default=0.10, dest="bump_pct")
    p_sens.add_argument("--max-candidates", type=int, default=None, dest="max_candidates")
    p_sens.add_argument(
        "--candidate-id",
        action="append",
        default=[],
        dest="candidate_ids",
        help="Evaluate a specific upstream candidate item id; repeatable",
    )
    p_sens.add_argument("--filter", default="drivers", dest="candidate_filter",
                        choices=["drivers", "inputs_only", "all"])
    p_sens.add_argument("--sensitivity-mode", default=None, dest="sensitivity_mode",
                        choices=["workbook_explicit", "workbook_global", "legacy_global"])
    p_sens.add_argument("--recompute-policy", default="projection_safe", dest="recompute_policy",
                        choices=["projection_safe", "legacy_global"])

    # scenario
    p_scen = sub.add_parser("scenario", help="Apply overrides and compare results")
    add_common(p_scen)
    p_scen.add_argument("--override", action="append", default=[], dest="overrides",
                        help="ITEM_ID:PERIOD=VALUE (repeatable)")
    p_scen.add_argument("--compare", action="append", default=[], dest="compare_items",
                        help="Item IDs to compare (repeatable)")
    p_scen.add_argument("--recompute-policy", default="projection_safe", dest="recompute_policy",
                        choices=["projection_safe", "legacy_global"])

    args = parser.parse_args(argv)

    # Lazy import so argparse --help stays fast
    from .tools import drivers, find, scenario, sensitivity, summarize

    cutoff_kw: dict = {}
    if args.cutoff is not None:
        cutoff_kw["historical_cutoff_year"] = args.cutoff

    if args.command == "summarize":
        result = summarize(args.file_path, **cutoff_kw)

    elif args.command == "find":
        result = find(args.file_path, args.query, limit=args.limit, **cutoff_kw)

    elif args.command == "drivers":
        result = drivers(args.file_path, args.item_id, depth=args.depth, **cutoff_kw)

    elif args.command == "sensitivity":
        result = sensitivity(
            args.file_path, args.target_id,
            n=args.n, bump_pct=args.bump_pct,
            max_candidates=args.max_candidates,
            candidate_ids=args.candidate_ids or None,
            candidate_filter=args.candidate_filter,
            sensitivity_mode=args.sensitivity_mode,
            recompute_policy=args.recompute_policy,
            **cutoff_kw,
        )

    elif args.command == "scenario":
        overrides: Dict[str, Dict[int, float]] = {}
        for raw in args.overrides:
            item_id, period, value = _parse_override(raw)
            overrides.setdefault(item_id, {})[period] = value
        result = scenario(
            args.file_path, overrides,
            compare_items=args.compare_items or None,
            recompute_policy=args.recompute_policy,
            **cutoff_kw,
        )

    json.dump(result, sys.stdout, indent=2, default=str)
    sys.stdout.write("\n")


if __name__ == "__main__":
    main()
