"""Represent per-statement tolerance profiles as reviewed data rows.

One owner for the question "what tolerance does this statement section
reconcile and grade materiality under". Retired mechanism constants answered
that only for balance sheets; this owner instead uses **data rows**: profile id
→ {reconciliation tolerance,
materiality thresholds for ``unmapped_direct_child``}, so IS/CF sections
generalize by adding a reviewed row rather than by editing code.

Two rules this module exists to enforce:

1. ``balance_sheet_exact`` carries today's constants **verbatim** — the row is
   the same numbers, not a re-derivation. ``tests/schema/test_statement_tolerance_profiles.py``
   pins the reviewed values.
2. **No IS/CF section activates without its reviewed profile row.** The table
   ships one row. Asking for a profile that has not been reviewed is a typed
   refusal naming the profile lane, never a fallback to the balance-sheet
   numbers.

Tolerance profiles never gate value *equality*. The restatement producer's
declared-precision predicate owns equality; profiles stamp advisory severity
on facts already emitted. ``advisory_severity`` below is that stamp and
nothing more. Current pipeline context lives in
``docs/reference/build-model-infrastructure-architecture-2026-08-04.md``;
the owner map is ``schema/README.md``.
"""

from __future__ import annotations

from typing import Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .models import FinancialStatement


ToleranceProfileId = str

AdvisorySeverity = Literal["within_profile", "exceeds_profile"]


class ToleranceProfileError(ValueError):
    """Typed refusal of a tolerance-profile lookup.

    Carries a stable code and the named repair target; there is no fallback
    profile and no default numbers (fail-closed — a section whose profile has
    not been reviewed does not activate).
    """

    def __init__(self, code: str, message: str, *, repair_target: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.repair_target = repair_target


class _ToleranceContract(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


def _require_non_blank(value: str, *, field_name: str) -> str:
    normalized = str(value or "").strip()
    if not normalized:
        raise ValueError(f"{field_name} must be non-empty")
    return normalized


class StatementToleranceProfile(_ToleranceContract):
    """One reviewed tolerance row for one statement.

    ``*_absolute_m`` values are in the model's normalized unit (millions —
    ``schema/source_values.py`` normalizes EDGAR facts to millions); the
    relative fractions are of the reported section total.
    """

    profile_id: ToleranceProfileId
    statement: FinancialStatement
    reconciliation_absolute_tolerance_m: float = Field(ge=0.0)
    reconciliation_relative_tolerance: float = Field(ge=0.0, le=1.0)
    materiality_absolute_m: float = Field(ge=0.0)
    materiality_relative: float = Field(ge=0.0, le=1.0)
    reviewed_rationale: str

    @field_validator("profile_id", "reviewed_rationale")
    @classmethod
    def _non_blank(cls, value: str, info) -> str:
        return _require_non_blank(value, field_name=info.field_name)

class CrossSourceAdvisoryTolerance(_ToleranceContract):
    """Reviewed advisory threshold for one required role across producers."""

    required_role: str = Field(
        min_length=1,
        pattern=r"^statement\.(is|bs|cf)\.[a-z0-9][a-z0-9_]*$",
    )
    tolerance_pct: float = Field(ge=0.0, le=1.0)


# The reviewed table. One row per (statement, profile) that has been reviewed
# against real filings. `balance_sheet_exact` carries the v1 constants
# verbatim: 1.0 (millions) absolute and 0.001 relative, used by v1 for BOTH the
# post-compute reconciliation arm and the `unmapped_direct_child` materiality
# gate through the single `_is_exact_mismatch` predicate.
#
# Deliberately absent: income-statement and cash-flow rows. Those sections
# activate only when their reviewed rows are added in the profile lane.
_PROFILE_ROWS: tuple[StatementToleranceProfile, ...] = (
    StatementToleranceProfile(
        profile_id="balance_sheet_exact",
        statement=FinancialStatement.balance_sheet,
        reconciliation_absolute_tolerance_m=1.0,
        reconciliation_relative_tolerance=0.001,
        materiality_absolute_m=1.0,
        materiality_relative=0.001,
        reviewed_rationale=(
            "Balance-sheet face sections reconcile exactly to the reported "
            "section total; the 1.0M absolute / 0.1% relative pair is the "
            "reviewed v1 tolerance carried verbatim from the retiring "
            "BALANCE_SHEET_EXACT_* constants."
        ),
    ),
)

# Verbatim from the nine non-null ``validation_tolerance_pct`` rows in the
# retired taxonomy at the Stage D cut. Role membership is owned by the
# required-role catalog; this table owns only the reviewed advisory numbers.
_CROSS_SOURCE_ROWS: tuple[CrossSourceAdvisoryTolerance, ...] = (
    CrossSourceAdvisoryTolerance(
        required_role="statement.is.revenue", tolerance_pct=0.05
    ),
    CrossSourceAdvisoryTolerance(
        required_role="statement.is.operating_income", tolerance_pct=0.005
    ),
    CrossSourceAdvisoryTolerance(
        required_role="statement.bs.goodwill", tolerance_pct=0.005
    ),
    CrossSourceAdvisoryTolerance(
        required_role="statement.bs.total_assets", tolerance_pct=0.005
    ),
    CrossSourceAdvisoryTolerance(
        required_role="statement.bs.total_liabilities", tolerance_pct=0.005
    ),
    CrossSourceAdvisoryTolerance(
        required_role="statement.bs.total_equity", tolerance_pct=0.005
    ),
    CrossSourceAdvisoryTolerance(
        required_role="statement.cf.operating_cash_flow", tolerance_pct=0.005
    ),
    CrossSourceAdvisoryTolerance(
        required_role="statement.cf.investing_cash_flow", tolerance_pct=0.005
    ),
    CrossSourceAdvisoryTolerance(
        required_role="statement.cf.financing_cash_flow", tolerance_pct=0.005
    ),
)

CROSS_SOURCE_ADVISORY_TOLERANCES: Mapping[
    str, CrossSourceAdvisoryTolerance
] = {row.required_role: row for row in _CROSS_SOURCE_ROWS}

TOLERANCE_PROFILES: Mapping[ToleranceProfileId, StatementToleranceProfile] = {
    row.profile_id: row for row in _PROFILE_ROWS
}


def require_tolerance_profile(
    profile_id: str,
    *,
    statement: FinancialStatement,
) -> StatementToleranceProfile:
    """Return the reviewed profile row, or refuse typed.

    ``tolerance_profile_unreviewed`` — no reviewed row carries this id (the
    IS/CF activation gate: the mechanism ships, the numbers are reviewed data).
    ``tolerance_profile_statement_mismatch`` — the row exists but was reviewed
    for a different statement; a balance-sheet row may not silently serve a
    cash-flow section.
    """

    normalized_id = str(profile_id or "").strip()
    row = TOLERANCE_PROFILES.get(normalized_id)
    if row is None:
        raise ToleranceProfileError(
            "tolerance_profile_unreviewed",
            (
                f"no reviewed tolerance profile {normalized_id!r} for "
                f"{statement.value}; reviewed profiles: "
                f"{sorted(TOLERANCE_PROFILES)!r}"
            ),
            repair_target=(
                "profile lane: ratify a reviewed tolerance-profile row for this "
                "statement section before activating it"
            ),
        )
    if row.statement is not statement:
        raise ToleranceProfileError(
            "tolerance_profile_statement_mismatch",
            (
                f"tolerance profile {normalized_id!r} is reviewed for "
                f"{row.statement.value}, not {statement.value}"
            ),
            repair_target=(
                "profile lane: ratify a reviewed tolerance-profile row for this "
                "statement, or bind the section to its own statement's profile"
            ),
        )
    return row

def require_cross_source_advisory_tolerance(
    required_role: str,
) -> CrossSourceAdvisoryTolerance:
    """Return the reviewed role threshold or refuse without a default."""

    from .required_role_catalog import load_required_role_catalog

    load_required_role_catalog().role(required_role)
    row = CROSS_SOURCE_ADVISORY_TOLERANCES.get(required_role)
    if row is None:
        raise ToleranceProfileError(
            "cross_source_tolerance_unreviewed",
            f"no reviewed cross-source advisory tolerance for {required_role!r}",
            repair_target=(
                "profile lane: ratify a reviewed cross-source tolerance row "
                "before opting this required role into comparison"
            ),
        )
    return row


def _exceeds(amount: float, reported_total: float, *, absolute_m: float, relative: float) -> bool:
    """The v1 `_is_exact_mismatch` predicate, carried verbatim onto profile rows.

    Retired v1 module (git history: `build_statement_mapping_validation.py:318-323`): a gap counts only
    when it exceeds BOTH the absolute floor and the relative fraction of the
    reported total.
    """

    magnitude = abs(float(amount))
    return magnitude > absolute_m and magnitude > abs(float(reported_total)) * relative


def exceeds_reconciliation_tolerance(
    gap: float,
    reported_total: float,
    profile: StatementToleranceProfile,
) -> bool:
    """True when a section's selected-row sum fails to reconcile under `profile`."""

    return _exceeds(
        gap,
        reported_total,
        absolute_m=profile.reconciliation_absolute_tolerance_m,
        relative=profile.reconciliation_relative_tolerance,
    )


def is_material_unmapped_child(
    value: float,
    reported_total: float,
    profile: StatementToleranceProfile,
) -> bool:
    """True when an unclaimed direct calculation child is material under `profile`."""

    return _exceeds(
        value,
        reported_total,
        absolute_m=profile.materiality_absolute_m,
        relative=profile.materiality_relative,
    )


def advisory_severity(
    delta: float,
    reported_total: float,
    profile: StatementToleranceProfile,
) -> AdvisorySeverity:
    """Grade an ALREADY-EMITTED delta. Never an equality gate.

    Mechanism §6.2 / producer contract §3.3: a pair unequal at declared
    precision is emitted regardless of what this returns. ``within_profile``
    means "small relative to the section" — it never means "equal".
    """

    return (
        "exceeds_profile"
        if _exceeds(
            delta,
            reported_total,
            absolute_m=profile.materiality_absolute_m,
            relative=profile.materiality_relative,
        )
        else "within_profile"
    )


__all__ = [
    "AdvisorySeverity",
    "CROSS_SOURCE_ADVISORY_TOLERANCES",
    "CrossSourceAdvisoryTolerance",
    "StatementToleranceProfile",
    "TOLERANCE_PROFILES",
    "ToleranceProfileError",
    "ToleranceProfileId",
    "advisory_severity",
    "exceeds_reconciliation_tolerance",
    "is_material_unmapped_child",
    "require_cross_source_advisory_tolerance",
    "require_tolerance_profile",
]
