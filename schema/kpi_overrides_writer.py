"""BusinessModel-to-per-ticker KPI override writer."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import logging
from pathlib import Path
import re
from collections.abc import Mapping
from typing import Iterator, Literal, TYPE_CHECKING

from .business_model import BusinessModel, DriverNode, Segment
from .kpi_bridge import _decode_period_key
from .overrides import (
    TickerOverrides,
    load_ticker_overrides,
    resolve_overrides_dir,
    save_ticker_overrides,
)
from .templates import load_data_taxonomy

if TYPE_CHECKING:
    from .kpi_bridge_report import BridgeReport


_KPI_SOURCE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_.-]*(?::[A-Za-z_][A-Za-z0-9_.-]*)?$")
_AXIS_QNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_-]*:[A-Za-z][A-Za-z0-9_-]*$")
_EDGAR_AXIS_FAMILY_LABELS = frozenset({"business_segment", "product", "geography"})
_DIMENSION_SUFFIXES = ("Member", "Domain", "Axis")
_HASH_INPUT_KEYS = (
    "concept_id",
    "axis_key",
    "edgar_tags",
    "fmp_field",
    "fmp_endpoint",
    "inline_values",
    "non_equivalent_after",
    "negate",
    "preferred_source",
    "target_item_id",
    "unit",
)


@dataclass
class OverridesWriteReport:
    ticker: str
    nodes_walked: int = 0
    nodes_kpi: int = 0
    nodes_emitted: int = 0
    nodes_skipped_derived_row: int = 0
    nodes_skipped_commentary: int = 0
    nodes_skipped_existing_row: int = 0
    nodes_skipped_no_source: int = 0
    nodes_skipped_invalid_source: list[tuple[str, str]] = field(default_factory=list)
    preserved_manual: list[str] = field(default_factory=list)
    preserved_locked: list[str] = field(default_factory=list)
    regenerated: list[str] = field(default_factory=list)
    conflicts: list[str] = field(default_factory=list)
    dimensional_intent_backfilled: list[str] = field(default_factory=list)
    tombstoned: list[str] = field(default_factory=list)
    output_path: Path | None = None
    exit_status: int = 0


def business_model_to_overrides(
    business_model: BusinessModel,
    ticker: str,
    overrides_dir: Path | None = None,
    dry_run: bool = False,
    prune: bool = False,
    bridge_report: "BridgeReport | None" = None,
    inline_values_by_node: Mapping[str, Mapping[str | int, float]] | None = None,
    generated_conflict_strategy: Literal["preserve", "prefer_new_unlocked"] = "preserve",
) -> OverridesWriteReport:
    """Walk BM KPI nodes, merge generated custom concepts, and write overrides."""

    ticker_upper = _normalize_ticker(ticker)
    directory = _override_dir(overrides_dir)
    existing = load_ticker_overrides(ticker_upper, directory) or TickerOverrides(
        ticker=ticker_upper,
        overrides={},
        custom_concepts={},
        file_meta={},
    )
    merged, report = business_model_to_ticker_overrides(
        business_model,
        ticker_upper,
        existing=existing,
        prune=prune,
        bridge_report=bridge_report,
        inline_values_by_node=inline_values_by_node,
        generated_conflict_strategy=generated_conflict_strategy,
    )

    if not dry_run:
        report.output_path = save_ticker_overrides(merged, directory)

    return report


def business_model_to_ticker_overrides(
    business_model: BusinessModel,
    ticker: str,
    *,
    existing: TickerOverrides | None = None,
    prune: bool = False,
    bridge_report: "BridgeReport | None" = None,
    inline_values_by_node: Mapping[str, Mapping[str | int, float]] | None = None,
    generated_conflict_strategy: Literal["preserve", "prefer_new_unlocked"] = "preserve",
) -> tuple[TickerOverrides, OverridesWriteReport]:
    """Return a merged per-ticker override object for BM KPI custom concepts."""

    ticker_upper = _normalize_ticker(ticker)
    report = OverridesWriteReport(ticker=ticker_upper)
    bm_ticker = getattr(getattr(business_model, "company", None), "ticker", None)
    if bm_ticker and str(bm_ticker).strip().upper() != ticker_upper:
        logging.warning(
            "BusinessModel ticker %s differs from explicit override ticker %s; using explicit ticker",
            bm_ticker,
            ticker_upper,
        )

    existing = existing or TickerOverrides(
        ticker=ticker_upper,
        overrides={},
        custom_concepts={},
        file_meta={},
    )
    base_taxonomy_ids = set(load_data_taxonomy().keys())

    new_entries: dict[str, dict] = {}
    for segment, node in _walk_all_nodes(business_model):
        report.nodes_walked += 1
        if not node.kpi:
            continue
        report.nodes_kpi += 1

        skip_reason = _classify_for_emission(node)
        if skip_reason == "derived_row":
            report.nodes_skipped_derived_row += 1
            continue
        if skip_reason == "commentary":
            report.nodes_skipped_commentary += 1
            continue
        if skip_reason == "existing_row":
            report.nodes_skipped_existing_row += 1
            continue

        source_ok, reason = _validate_kpi_source(node.kpi_source)
        if not source_ok:
            qualified_id = f"{segment.id}.{node.id}"
            if reason == "missing":
                report.nodes_skipped_no_source += 1
            else:
                report.nodes_skipped_invalid_source.append((qualified_id, str(node.kpi_source)))
            continue

        concept_id = f"bm__{segment.id}__{node.id}"
        if concept_id in base_taxonomy_ids or concept_id in existing.overrides:
            raise ValueError(
                f"concept_id {concept_id!r} collides with base taxonomy or overrides section"
            )

        new_entries[concept_id] = _build_custom_concept_entry(
            segment,
            node,
            concept_id,
            bridge_report,
            inline_values_by_node=inline_values_by_node,
        )
        report.nodes_emitted += 1

    _validate_duplicate_axis_coordinates(business_model, bridge_report, report)

    merged_custom = _merge_with_existing(
        existing.custom_concepts,
        new_entries,
        report,
        prune=prune,
        generated_conflict_strategy=generated_conflict_strategy,
    )
    custom_changed = merged_custom != existing.custom_concepts

    file_meta = dict(existing.file_meta or {})
    if custom_changed or report.conflicts:
        file_meta["last_updated"] = _utc_now_iso()

    merged = TickerOverrides(
        ticker=ticker_upper,
        overrides=existing.overrides,
        custom_concepts=merged_custom,
        file_meta=file_meta,
        projections=dict(existing.projections or {}),
        semantic_rows=dict(existing.semantic_rows or {}),
        valuation=dict(existing.valuation or {}),
    )

    report.exit_status = 1 if report.conflicts else 0
    return merged, report


def _walk_all_nodes(bm: BusinessModel) -> Iterator[tuple[Segment, DriverNode]]:
    for segment in bm.segments:
        for node in segment.revenue_model.decomposition:
            yield from _walk_node(segment, node)


def _walk_node(segment: Segment, node: DriverNode) -> Iterator[tuple[Segment, DriverNode]]:
    yield segment, node
    for child in node.children or []:
        yield from _walk_node(segment, child)


def _classify_for_emission(node: DriverNode) -> str | None:
    target_type = node.compile_to.target_type
    if target_type == "assumption_row":
        return None
    if target_type in {"derived_row", "commentary", "existing_row"}:
        return target_type
    raise ValueError(f"Unsupported compile target type: {target_type!r}")


def _validate_kpi_source(value: object) -> tuple[bool, str | None]:
    if value is None:
        return False, "missing"

    raw = str(value).strip()
    if not raw:
        return False, "missing"
    if _KPI_SOURCE_RE.fullmatch(raw) is None:
        return False, "invalid"

    local_name = raw.rsplit(":", 1)[-1]
    if local_name.endswith(_DIMENSION_SUFFIXES):
        return False, "dimension"

    return True, None


def validate_kpi_source_reference(value: object) -> tuple[bool, str | None]:
    """Validate the syntax of a BusinessModel KPI source reference."""

    return _validate_kpi_source(value)


def _build_custom_concept_entry(
    segment: Segment,
    node: DriverNode,
    concept_id: str,
    bridge_report: "BridgeReport | None" = None,
    *,
    inline_values_by_node: Mapping[str, Mapping[str | int, float]] | None = None,
) -> dict:
    unit = getattr(node.unit, "value", node.unit)
    entry: dict = {
        "concept_id": concept_id,
        "edgar_tags": [str(node.kpi_source).strip()],
        "target_item_id": f"bm.{segment.id}.{node.id}",
        "unit": str(unit),
        "notes": f"F2h: KPI node from {segment.label} segment; kpi_frequency={node.kpi_frequency}",
        "_meta": {
            "source": "f2h",
            "locked": False,
            "disabled": False,
            "generated_at": _utc_now_iso(),
        },
    }
    if segment.edgar_member:
        entry.setdefault("_meta", {})["dimensional_intent"] = True

    if segment.edgar_axis and segment.edgar_member:
        axis_key = f"{segment.edgar_axis}={segment.edgar_member}"
        _validate_axis_key(axis_key, concept_id)
        entry["axis_key"] = axis_key

    if bridge_report is not None:
        inline_values = _inline_values_for_node(bridge_report, segment, node)
        if inline_values:
            entry["inline_values"] = inline_values
    if "inline_values" not in entry and inline_values_by_node:
        inline_values = _inline_values_from_mapping(inline_values_by_node, segment, node)
        if inline_values:
            entry["inline_values"] = inline_values

    entry["_meta"]["content_hash"] = _compute_content_hash(entry)
    return entry


def _merge_with_existing(
    existing_custom_concepts: dict[str, dict],
    new_entries: dict[str, dict],
    report: OverridesWriteReport,
    prune: bool,
    generated_conflict_strategy: Literal["preserve", "prefer_new_unlocked"] = "preserve",
) -> dict[str, dict]:
    merged: dict[str, dict] = {}

    for concept_id in sorted(existing_custom_concepts):
        existing_entry = existing_custom_concepts[concept_id]
        new_entry = new_entries.get(concept_id)

        if new_entry is None:
            source = _entry_source(existing_entry)
            if source is None:
                report.preserved_manual.append(concept_id)
                merged[concept_id] = existing_entry
                continue
            if _is_locked(existing_entry):
                report.preserved_locked.append(concept_id)
                merged[concept_id] = existing_entry
                continue
            if source == "f2h":
                report.tombstoned.append(concept_id)
                if prune:
                    continue
                merged[concept_id] = _tombstone_entry(existing_entry)
                continue
            report.preserved_manual.append(concept_id)
            merged[concept_id] = existing_entry
            continue

        source = _entry_source(existing_entry)
        if source is None:
            report.preserved_manual.append(concept_id)
            merged[concept_id] = existing_entry
            continue
        if source == "f2h":
            if _is_locked(existing_entry):
                stored_hash = _entry_meta(existing_entry).get("content_hash")
                existing_hash = _compute_content_hash(existing_entry)
                new_hash = _entry_meta(new_entry).get("content_hash")
                preserved = _backfill_dimensional_intent(
                    concept_id,
                    existing_entry,
                    new_entry,
                    report,
                )
                if stored_hash == existing_hash == new_hash:
                    report.preserved_locked.append(concept_id)
                    merged[concept_id] = preserved
                    continue

                report.conflicts.append(concept_id)
                merged[concept_id] = preserved
                continue

            if _entry_meta(existing_entry).get("disabled") is True:
                report.regenerated.append(concept_id)
                merged[concept_id] = new_entry
                continue

            stored_hash = _entry_meta(existing_entry).get("content_hash")
            existing_hash = _compute_content_hash(existing_entry)
            new_hash = _entry_meta(new_entry).get("content_hash")
            if stored_hash == existing_hash == new_hash:
                report.regenerated.append(concept_id)
                # R6 #1: _meta is excluded from content_hash, so verbatim-preserved
                # F2h entries still need the dimensional safety marker backfilled.
                merged[concept_id] = _backfill_dimensional_intent(
                    concept_id,
                    existing_entry,
                    new_entry,
                    report,
                )
                continue

            if generated_conflict_strategy == "prefer_new_unlocked":
                if "inline_values" in existing_entry and "inline_values" not in new_entry:
                    merged[concept_id] = existing_entry
                    continue
                report.regenerated.append(concept_id)
                merged[concept_id] = new_entry
                continue
            report.conflicts.append(concept_id)
            merged[concept_id] = existing_entry
            continue

        report.preserved_manual.append(concept_id)
        merged[concept_id] = existing_entry

    for concept_id in sorted(new_entries):
        if concept_id not in existing_custom_concepts:
            merged[concept_id] = new_entries[concept_id]

    return merged


def _compute_content_hash(entry: dict) -> str:
    payload = {
        key: _canonical_hash_value(key, entry[key])
        for key in _HASH_INPUT_KEYS
        if key in entry
    }
    canonical = json.dumps(
        payload,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )
    return f"sha256:{hashlib.sha256(canonical.encode('utf-8')).hexdigest()}"


def _canonical_hash_value(key: str, value: object) -> object:
    if key != "inline_values":
        return value
    if not isinstance(value, dict):
        return value
    normalized = {str(year): float(raw_value) for year, raw_value in value.items()}
    return json.dumps(
        normalized,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _validate_axis_key(axis_key: str, concept_id: str) -> None:
    if not isinstance(axis_key, str):
        raise ValueError(f"custom_concept {concept_id!r} axis_key must be a string")
    if "|" in axis_key:
        raise ValueError(
            f"custom_concept {concept_id!r} axis_key must contain exactly one axis/member "
            "pair; multi-axis '|' keys are not supported"
        )
    if axis_key.count("=") != 1:
        raise ValueError(
            f"custom_concept {concept_id!r} axis_key must have shape "
            "'axis_qname=member_qname'"
        )

    axis_qname, member_qname = axis_key.split("=", 1)
    if not axis_qname or not member_qname:
        raise ValueError(
            f"custom_concept {concept_id!r} axis_key must have non-empty axis and member"
        )
    if axis_qname in _EDGAR_AXIS_FAMILY_LABELS:
        raise ValueError(
            f"custom_concept {concept_id!r} axis_key axis {axis_qname!r} is a family "
            "label; expected an XBRL axis QName"
        )
    if not _AXIS_QNAME_RE.match(axis_qname):
        raise ValueError(
            f"custom_concept {concept_id!r} axis_key axis {axis_qname!r} must be an "
            "XBRL QName"
        )
    if not _AXIS_QNAME_RE.match(member_qname):
        raise ValueError(
            f"custom_concept {concept_id!r} axis_key member {member_qname!r} must be an "
            "XBRL QName"
        )


def _inline_values_for_node(
    bridge_report: "BridgeReport",
    segment: Segment,
    node: DriverNode,
) -> dict[str, float]:
    observed = bridge_report.observed_values_per_node.get(f"{segment.id}:{node.id}", {})
    annual_values: dict[str, float] = {}
    for period_key, value in observed.items():
        frame, period_int, _period_end, range_bound = _decode_period_key(period_key)
        if frame != "annual" or range_bound is not None or period_int is None:
            continue
        annual_values[str(period_int)] = float(value)
    return dict(sorted(annual_values.items()))


def _inline_values_from_mapping(
    inline_values_by_node: Mapping[str, Mapping[str | int, float]],
    segment: Segment,
    node: DriverNode,
) -> dict[str, float]:
    observed = (
        inline_values_by_node.get(f"{segment.id}:{node.id}")
        or inline_values_by_node.get(f"{segment.id}.{node.id}")
        or {}
    )
    annual_values: dict[str, float] = {}
    for raw_period, value in observed.items():
        try:
            period = int(raw_period)
            annual_values[str(period)] = float(value)
        except (TypeError, ValueError):
            continue
    return dict(sorted(annual_values.items()))


def _backfill_dimensional_intent(
    concept_id: str,
    existing_entry: dict,
    new_entry: dict,
    report: OverridesWriteReport,
) -> dict:
    if _entry_meta(new_entry).get("dimensional_intent") is not True:
        return existing_entry
    if _entry_meta(existing_entry).get("dimensional_intent") is True:
        return existing_entry

    preserved = dict(existing_entry)
    preserved["_meta"] = dict(_entry_meta(existing_entry))
    preserved["_meta"]["dimensional_intent"] = True
    if concept_id not in report.dimensional_intent_backfilled:
        report.dimensional_intent_backfilled.append(concept_id)
    return preserved


def _validate_duplicate_axis_coordinates(
    business_model: BusinessModel,
    bridge_report: "BridgeReport | None",
    report: OverridesWriteReport,
) -> None:
    seen: dict[tuple[str, str], str] = {}

    for segment in business_model.segments:
        if not (segment.edgar_axis and segment.edgar_member):
            continue
        coord = (segment.edgar_axis, segment.edgar_member)
        location = f"segment:{segment.id}"
        previous = seen.get(coord)
        if previous is not None:
            report.conflicts.append(
                f"Duplicate (edgar_axis, edgar_member) pair {coord!r} "
                f"between {previous} and {location}"
            )
        else:
            seen[coord] = location

    if bridge_report is None:
        return

    for family, alt_set in bridge_report.alternative_segment_sets.items():
        for axis_qname, members in alt_set.edgar_members_by_axis.items():
            for member in members:
                coord = (axis_qname, member)
                location = f"alt:{family}:{axis_qname}:{member}"
                previous = seen.get(coord)
                if previous is not None:
                    report.conflicts.append(
                        f"Duplicate (edgar_axis, edgar_member) pair {coord!r} "
                        f"between {previous} and {location}"
                    )
                else:
                    seen[coord] = location


def _normalize_ticker(ticker: str) -> str:
    normalized = str(ticker or "").strip().upper()
    if not normalized:
        raise ValueError("ticker is required")
    return normalized


def _override_dir(overrides_dir: Path | None) -> Path:
    return resolve_overrides_dir(overrides_dir)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _entry_meta(entry: dict) -> dict:
    meta = entry.get("_meta") or {}
    return meta if isinstance(meta, dict) else {}


def _entry_source(entry: dict) -> object:
    return _entry_meta(entry).get("source")


def _is_locked(entry: dict) -> bool:
    return _entry_meta(entry).get("locked") is True


def _tombstone_entry(entry: dict) -> dict:
    tombstoned = dict(entry)
    tombstoned["_meta"] = dict(_entry_meta(entry))
    tombstoned["_meta"]["disabled"] = True
    return tombstoned


__all__ = [
    "OverridesWriteReport",
    "business_model_to_overrides",
    "business_model_to_ticker_overrides",
    "validate_kpi_source_reference",
    "_walk_all_nodes",
    "_classify_for_emission",
    "_validate_kpi_source",
    "_build_custom_concept_entry",
    "_merge_with_existing",
    "_compute_content_hash",
]
