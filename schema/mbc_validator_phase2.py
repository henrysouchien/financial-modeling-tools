from __future__ import annotations

from collections.abc import Iterator, Mapping
import re
from typing import TYPE_CHECKING, Literal

from pydantic import Field

from .driver_resolver import resolve_driver_key
from .model_build_context import ModelBuildContext
from .model_build_context_errors import (
    InvalidDriverKey,
    MissingSegmentSnapshot,
    SegmentExpansionAmbiguity,
    SegmentProfileMismatch,
    UnsupportedInSegmentMode,
)
from .thesis_shared_slice import _ContractModel

if TYPE_CHECKING:
    from .business_model_compiler import CompiledDriverRegistry


VerdictOrigin = Literal["drivers", "scenarios"]
VerdictCategory = Literal["A", "B1", "B2", "C"]
VerdictErrorType = Literal[
    "InvalidDriverKey",
    "SegmentExpansionAmbiguity",
    "UnsupportedInSegmentMode",
    "MissingSegmentSnapshot",
    "SegmentProfileMismatch",
]

_SUMMARY_KEYS = ("A", "B1", "B2", "C", "errors")
_SEGMENT_CONFIG_SENTINEL = "__segment_config__"
_SEGMENT_DRIVER_KEY_RE = re.compile(r"^revenue\.segment_(\d+)\.(\w+)$")
_CATEGORY_A_ROLES = frozenset({"volume_growth", "price_growth"})


class DriverVerdict(_ContractModel):
    origin: VerdictOrigin
    scenario_name: str | None = None
    driver_key: str
    category: VerdictCategory | None = None
    resolved_item_id: str | None = None
    error_type: VerdictErrorType | None = None
    error_reason: str | None = None


class ValidationReport(_ContractModel):
    phase1_passed: bool
    phase2_passed: bool
    driver_verdicts: list[DriverVerdict] = Field(default_factory=list)
    summary: dict[str, int] = Field(default_factory=dict)


def _empty_summary() -> dict[str, int]:
    return {key: 0 for key in _SUMMARY_KEYS}


def _iter_driver_entries(mbc: ModelBuildContext) -> Iterator[tuple[VerdictOrigin, str | None, str]]:
    for driver_key in mbc.drivers.keys():
        yield "drivers", None, driver_key
    for scenario_name, scenario in mbc.scenarios.items():
        for driver_key in scenario.overrides.keys():
            yield "scenarios", scenario_name, driver_key


def _record_verdict(summary: dict[str, int], verdicts: list[DriverVerdict], verdict: DriverVerdict) -> None:
    if verdict.category is not None:
        summary[verdict.category] += 1
    if verdict.error_type is not None:
        summary["errors"] += 1
    verdicts.append(verdict)


def _finalize_report(verdicts: list[DriverVerdict], summary: dict[str, int]) -> ValidationReport:
    phase1_passed = all(verdict.error_type != "InvalidDriverKey" for verdict in verdicts)
    phase2_passed = all(verdict.error_type is None for verdict in verdicts)
    return ValidationReport(
        phase1_passed=phase1_passed,
        phase2_passed=phase2_passed,
        driver_verdicts=verdicts,
        summary=summary,
    )


def _segment_error_verdict(error_type: VerdictErrorType, reason: str) -> DriverVerdict:
    return DriverVerdict(
        origin="drivers",
        scenario_name=None,
        driver_key=_SEGMENT_CONFIG_SENTINEL,
        category=None,
        resolved_item_id=None,
        error_type=error_type,
        error_reason=reason,
    )


def _category_a_segment_index(driver_key: str) -> int | None:
    match = _SEGMENT_DRIVER_KEY_RE.match(driver_key)
    if match is None:
        return None
    if match.group(2) not in _CATEGORY_A_ROLES:
        return None
    return int(match.group(1))


def _snapshot_segment_index(segment: object) -> int:
    if isinstance(segment, Mapping):
        return int(segment["segment_index"])
    return int(getattr(segment, "segment_index"))


def validate_phase2(
    mbc: ModelBuildContext,
    *,
    compiled_registry: "CompiledDriverRegistry | None" = None,
    raise_on_first_error: bool = False,
) -> ValidationReport:
    if mbc.segment_config is None:
        return ValidationReport(
            phase1_passed=True,
            phase2_passed=True,
            driver_verdicts=[],
            summary=_empty_summary(),
        )

    summary = _empty_summary()
    verdicts: list[DriverVerdict] = []
    snapshot = getattr(mbc.segment_config, "segment_profile_snapshot", None)
    if snapshot is None:
        error = MissingSegmentSnapshot("segment_config populated without segment_profile_snapshot")
        if raise_on_first_error:
            raise error
        _record_verdict(summary, verdicts, _segment_error_verdict("MissingSegmentSnapshot", error.reason))
        return _finalize_report(verdicts, summary)

    indices = sorted(_snapshot_segment_index(segment) for segment in snapshot.segments)
    expected = list(range(1, len(snapshot.segments) + 1))
    if indices != expected:
        error = SegmentProfileMismatch(reason=f"segment_index sequence must be 1..N complete; got {indices}")
        if raise_on_first_error:
            raise error
        _record_verdict(summary, verdicts, _segment_error_verdict("SegmentProfileMismatch", error.reason))
        return _finalize_report(verdicts, summary)

    max_index = len(snapshot.segments)
    for origin, scenario_name, driver_key in _iter_driver_entries(mbc):
        segment_index = _category_a_segment_index(driver_key)
        if segment_index is not None and not (1 <= segment_index <= max_index):
            error = SegmentExpansionAmbiguity(
                driver_key=driver_key,
                segment_index=segment_index,
                reason=f"segment_index {segment_index} out of range for snapshot with {max_index} segments",
            )
            verdict = DriverVerdict(
                origin=origin,
                scenario_name=scenario_name,
                driver_key=driver_key,
                category="A",
                resolved_item_id=None,
                error_type="SegmentExpansionAmbiguity",
                error_reason=error.reason,
            )
            if raise_on_first_error:
                raise error
            _record_verdict(summary, verdicts, verdict)
            continue

        try:
            if compiled_registry is None:
                resolved_item_id = resolve_driver_key(driver_key)
            else:
                resolved_item_id = resolve_driver_key(driver_key, compiled_registry=compiled_registry)
        except UnsupportedInSegmentMode as exc:
            verdict = DriverVerdict(
                origin=origin,
                scenario_name=scenario_name,
                driver_key=driver_key,
                category=exc.category,
                resolved_item_id=None,
                error_type="UnsupportedInSegmentMode",
                error_reason=exc.reason,
            )
            if raise_on_first_error:
                raise exc
            _record_verdict(summary, verdicts, verdict)
            continue
        except InvalidDriverKey as exc:
            verdict = DriverVerdict(
                origin=origin,
                scenario_name=scenario_name,
                driver_key=driver_key,
                category=None,
                resolved_item_id=None,
                error_type="InvalidDriverKey",
                error_reason=exc.reason,
            )
            if raise_on_first_error:
                raise exc
            _record_verdict(summary, verdicts, verdict)
            continue

        if segment_index is None:
            _record_verdict(
                summary,
                verdicts,
                DriverVerdict(
                    origin=origin,
                    scenario_name=scenario_name,
                    driver_key=driver_key,
                    category="C",
                    resolved_item_id=resolved_item_id,
                    error_type=None,
                    error_reason=None,
                ),
            )
            continue

        _record_verdict(
            summary,
            verdicts,
            DriverVerdict(
                origin=origin,
                scenario_name=scenario_name,
                driver_key=driver_key,
                category="A",
                resolved_item_id=resolved_item_id,
                error_type=None,
                error_reason=None,
            ),
        )

    return _finalize_report(verdicts, summary)


__all__ = ["DriverVerdict", "ValidationReport", "validate_phase2"]
