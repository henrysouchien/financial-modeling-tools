from __future__ import annotations

import re
from typing import Iterable

from .business_model import BusinessModel, DriverExpr, DriverNode, _extract_node_refs
from .kpi_bridge_report import CompletionFinding
from .kpi_bridge_utils import TOLERANCE_ERROR


def validate_business_model_draft_completion(bm: BusinessModel) -> list[CompletionFinding]:
    """Validate completion invariants not enforced by BusinessModel Pydantic fields."""

    findings: list[CompletionFinding] = []
    share_sum = sum(segment.revenue_share for segment in bm.segments)
    deviation = abs(share_sum - 1.0)
    if deviation > TOLERANCE_ERROR:
        findings.append(
            CompletionFinding(
                kind="revenue_share_sum_off_one",
                severity="error",
                detail=f"revenue_share sum={share_sum:.4f}, deviation={deviation:.4f}, tolerance={TOLERANCE_ERROR}",
            )
        )

    default_segments = [segment for segment in bm.segments if segment.is_default]
    if not default_segments:
        findings.append(
            CompletionFinding(
                kind="no_default_segment",
                severity="error",
                detail="Exactly one segment must set is_default=True",
            )
        )
    elif len(default_segments) > 1:
        findings.append(
            CompletionFinding(
                kind="multiple_default_segments",
                severity="error",
                detail=f"{len(default_segments)} segments set is_default=True",
            )
        )
    elif not bm.segments[0].is_default:
        findings.append(
            CompletionFinding(
                kind="default_not_first_segment",
                severity="error",
                segment_id=default_segments[0].id,
                detail="Compiler uses segments[0] as primary; reorder the default segment to index 0",
            )
        )

    findings.extend(_duplicate_field_findings(bm))
    findings.extend(_leaf_driver_completion_findings(bm))
    findings.extend(_commentary_reference_findings(bm))
    return findings


def _duplicate_field_findings(bm: BusinessModel) -> list[CompletionFinding]:
    findings: list[CompletionFinding] = []
    edgar_members: dict[str, str] = {}
    match_names: dict[str, str] = {}
    for segment in bm.segments:
        if segment.edgar_member:
            key = segment.edgar_member.strip().lower()
            if key in edgar_members:
                findings.append(
                    CompletionFinding(
                        kind="duplicate_edgar_member",
                        severity="error",
                        segment_id=segment.id,
                        detail=(
                            f"Segment {segment.id!r} duplicates edgar_member from "
                            f"segment {edgar_members[key]!r}"
                        ),
                    )
                )
            else:
                edgar_members[key] = segment.id
        match_key = re.sub(r"\s+", " ", segment.match_name.strip().lower())
        if match_key in match_names:
            findings.append(
                CompletionFinding(
                    kind="duplicate_match_name",
                    severity="error",
                    segment_id=segment.id,
                    detail=f"Segment {segment.id!r} duplicates match_name from segment {match_names[match_key]!r}",
                )
            )
        else:
            match_names[match_key] = segment.id
    return findings


def _leaf_driver_completion_findings(bm: BusinessModel) -> list[CompletionFinding]:
    findings: list[CompletionFinding] = []
    for segment in bm.segments:
        for node in _walk_nodes(segment.revenue_model.decomposition):
            if not node.children and node.driver is None and node.compile_to.target_type != "commentary":
                findings.append(
                    CompletionFinding(
                        kind="leaf_driver_missing",
                        severity="error",
                        segment_id=segment.id,
                        node_id=node.id,
                        detail="Leaf DriverNode must have driver or compile_to.target_type='commentary'",
                    )
                )
    return findings


def _commentary_reference_findings(bm: BusinessModel) -> list[CompletionFinding]:
    findings: list[CompletionFinding] = []
    for segment in bm.segments:
        node_by_id = {node.id: node for node in _walk_nodes(segment.revenue_model.decomposition)}
        expressions: list[tuple[str, str | None, DriverExpr]] = [
            ("consolidation_formula", None, segment.revenue_model.consolidation_formula)
        ]
        for node in node_by_id.values():
            if node.driver is not None:
                expressions.append(("driver_expr_references_commentary", node.id, node.driver))
            if node.children_formula is not None:
                expressions.append(("children_formula_references_commentary", node.id, node.children_formula))

        for source_kind, node_id, expr in expressions:
            for ref_id in _extract_node_refs(expr):
                if ref_id == "self":
                    continue
                referenced = node_by_id.get(ref_id)
                if referenced is not None and referenced.compile_to.target_type == "commentary":
                    kind = (
                        "consolidation_formula_references_commentary"
                        if source_kind == "consolidation_formula"
                        else source_kind
                    )
                    findings.append(
                        CompletionFinding(
                            kind=kind,
                            severity="error",
                            segment_id=segment.id,
                            node_id=node_id,
                            detail=f"{source_kind} references commentary-only node {ref_id!r}",
                        )
                    )
    return findings


def _walk_nodes(nodes: list[DriverNode]) -> Iterable[DriverNode]:
    for node in nodes:
        yield node
        if node.children:
            yield from _walk_nodes(node.children)


__all__ = [
    "validate_business_model_draft_completion",
    "_commentary_reference_findings",
    "_duplicate_field_findings",
    "_leaf_driver_completion_findings",
    "_walk_nodes",
]
