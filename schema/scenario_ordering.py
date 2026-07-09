"""Shared scenario ordering checks for bull/base/bear curves."""

from __future__ import annotations

from typing import Any


def _period_sort_key(period: Any) -> tuple[int, int | str]:
    try:
        return (0, int(period))
    except (TypeError, ValueError):
        return (1, str(period))


def _period_is_strict(period: Any, strict_through_period: int | None) -> bool:
    if strict_through_period is None:
        return True
    try:
        return int(period) <= int(strict_through_period)
    except (TypeError, ValueError):
        return True


def _value_for_period(values: dict[Any, float], period: Any) -> float | None:
    if period in values:
        return values[period]
    try:
        period_int = int(period)
    except (TypeError, ValueError):
        period_int = None
    if period_int is not None:
        if period_int in values:
            return values[period_int]
        period_str = str(period_int)
        if period_str in values:
            return values[period_str]
    return None


def scenario_ordering_issues(
    *,
    bull_values: dict[Any, float],
    base_values: dict[Any, float],
    bear_values: dict[Any, float],
    strict_through_period: int | None = None,
    eps: float = 1e-9,
) -> list[str]:
    """Return ordering violations for bull/base/bear curves.

    Distinct bull/base/bear separation is required through ``strict_through_period``.
    After that boundary, curves may converge but must remain monotonic in the same
    direction inferred from the non-flat periods. With no boundary, strict ordering is
    required for all shared periods to preserve legacy behavior.
    """

    shared_periods = sorted(set(bull_values) & set(bear_values), key=_period_sort_key)
    non_flat_periods = [
        period
        for period in shared_periods
        if abs(float(bull_values[period]) - float(bear_values[period])) > eps
    ]

    issues: list[str] = []
    if not non_flat_periods:
        for period in shared_periods:
            bull = float(bull_values[period])
            bear = float(bear_values[period])
            base = _value_for_period(base_values, period)
            if base is None:
                issues.append(f"{period}:bull={bull:g},base=missing,bear={bear:g},expected=base_value_present")
                continue
            detail = f"{period}:bull={bull:g},base={float(base):g},bear={bear:g}"
            issues.append(f"{detail},expected=bull/base/bear_distinct")
        return issues

    direction_period = max(
        non_flat_periods,
        key=lambda period: abs(float(bull_values[period]) - float(bear_values[period])),
    )
    bull_above_bear = float(bull_values[direction_period]) > float(bear_values[direction_period])

    for period in shared_periods:
        bull = float(bull_values[period])
        bear = float(bear_values[period])
        base = _value_for_period(base_values, period)
        if base is None:
            issues.append(f"{period}:bull={bull:g},base=missing,bear={bear:g},expected=base_value_present")
            continue
        base = float(base)
        detail = f"{period}:bull={bull:g},base={base:g},bear={bear:g}"
        strict = _period_is_strict(period, strict_through_period)

        if abs(bull - bear) <= eps:
            if strict:
                issues.append(f"{detail},expected=bull/base/bear_distinct")
            elif bull_above_bear:
                if bull < base - eps:
                    issues.append(f"{detail},expected=bull>=base")
                if bear > base + eps:
                    issues.append(f"{detail},expected=bear<=base")
            else:
                if bull > base + eps:
                    issues.append(f"{detail},expected=bull<=base")
                if bear < base - eps:
                    issues.append(f"{detail},expected=bear>=base")
            continue

        if bull_above_bear and bull < bear - eps:
            issues.append(f"{detail},expected=consistent_bull_above_bear")
            continue
        if not bull_above_bear and bull > bear + eps:
            issues.append(f"{detail},expected=consistent_bull_below_bear")
            continue

        if bull_above_bear:
            if strict:
                if bull <= base + eps:
                    issues.append(f"{detail},expected=bull>base")
                if bear >= base - eps:
                    issues.append(f"{detail},expected=bear<base")
            else:
                if bull < base - eps:
                    issues.append(f"{detail},expected=bull>=base")
                if bear > base + eps:
                    issues.append(f"{detail},expected=bear<=base")
        elif strict:
            if bull >= base - eps:
                issues.append(f"{detail},expected=bull<base")
            if bear <= base + eps:
                issues.append(f"{detail},expected=bear>base")
        else:
            if bull > base + eps:
                issues.append(f"{detail},expected=bull<=base")
            if bear < base - eps:
                issues.append(f"{detail},expected=bear>=base")
    return issues


__all__ = ["scenario_ordering_issues"]
