"""Filing-line buffer helpers for build population."""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from .provider_port.contracts import (
    FilingLineObservation,
    FilingLineRef,
)




def _historical_year(line: FilingLineRef) -> int | None:
    period_id = line.period.canonical_label()
    if (
        period_id.startswith("FY")
        and not period_id.endswith("E")
        and period_id[2:].isdigit()
    ):
        return int(period_id[2:])
    return None


def _build_statement_fact_buffer(
    statement_facts: Sequence[FilingLineObservation],
    lines: Iterable[FilingLineRef],
    historical_periods: Iterable[int],
) -> dict[FilingLineRef, FilingLineObservation]:
    """Index canonical observations by their exact pinned filing-line identity."""

    requested_lines = frozenset(lines)
    requested_years = {int(year) for year in historical_periods}
    buffer: dict[FilingLineRef, FilingLineObservation] = {}
    for observation in statement_facts:
        if observation.line not in requested_lines:
            continue
        year = _historical_year(observation.line)
        if year is None or year not in requested_years:
            continue
        buffer[observation.line] = observation
    return buffer




__all__ = ["_build_statement_fact_buffer"]
