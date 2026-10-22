"""The one definition of "capitalized software is included" in the model.

Software is inside inclusive PP&E and total D&A only where the filer cites BOTH
its balance (``statement.bs.capitalized_software``) and its amortization
(``statement.cf.capitalized_software_amortization``) as not separately
presented. A presented balance with an included amortization still rolls
forward on a numeric amortization that total D&A contains and its history
cannot isolate, so the model builds physical depreciation only.

The model decides once, on the terminal historical year: the projected software
lines and the depreciation row it builds follow that year's citations.
:func:`capitalized_software_included_at_terminal` owns that period selection
and :func:`capitalized_software_included` the per-period test it applies. The
schedule builder (``build_financial_schedules``) and the applicability rule
``capitalized_software_amortization.v1`` both call the terminal function over
the roles' per-period dispositions, so the depreciation row the model builds
and the control it answers cannot drift apart.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping

INCLUDED_DISPOSITION = "not_separately_presented"


def capitalized_software_included(
    balance_dispositions: Iterable[str | None],
    amortization_dispositions: Iterable[str | None],
) -> bool:
    """Whether one period's software balance and amortization are both included.

    ``balance_dispositions`` are the period dispositions of the roles the
    software balance is read from (any one cited as included is enough);
    ``amortization_dispositions`` are those of the amortization role (all must be
    cited as included, and one must exist).
    """

    amortization = tuple(amortization_dispositions)
    return (
        bool(amortization)
        and all(value == INCLUDED_DISPOSITION for value in amortization)
        and any(value == INCLUDED_DISPOSITION for value in balance_dispositions)
    )


def capitalized_software_included_at_terminal(
    balance_dispositions_by_year: Mapping[int, Iterable[str | None]],
    amortization_dispositions_by_year: Mapping[int, Iterable[str | None]],
) -> bool:
    """Whether the model builds the inclusive row: software included in the
    terminal historical year, the latest year either role has a disposition for.
    Earlier years never decide it."""

    years = {*balance_dispositions_by_year, *amortization_dispositions_by_year}
    if not years:
        return False
    terminal = max(years)
    return capitalized_software_included(
        balance_dispositions_by_year.get(terminal, ()),
        amortization_dispositions_by_year.get(terminal, ()),
    )
