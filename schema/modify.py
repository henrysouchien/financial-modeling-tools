"""Persistent in-memory modifications for built financial models."""

from __future__ import annotations

import copy
from datetime import datetime
import os
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

from api.addin_dispatch import _dispatch_to_addin
from schema.build import _set_constant_override, write_xlsx  # noqa: F401
from schema.models import (
    FinancialModel,
    FormulaSpec,  # noqa: F401 - compatibility alias for schema.modify imports
    FormulaType,  # noqa: F401 - compatibility alias for schema.modify imports
    ItemType,  # noqa: F401 - compatibility alias for schema.modify imports
    LineItem,  # noqa: F401 - compatibility alias for schema.modify imports
    LineItemRef,  # noqa: F401 - compatibility alias for schema.modify imports
    SheetType,  # noqa: F401 - compatibility alias for schema.modify imports
    ValueProvenance,  # noqa: F401
)
from schema.modify_core import (
    LayoutError,  # noqa: F401 - compatibility alias for schema.modify imports
    ModifyError,
    Operation,
    OperationResult,
    OperationType,
    _apply_add_item,  # noqa: F401 - compatibility alias for schema.modify imports
    _apply_remove_item,  # noqa: F401 - compatibility alias for schema.modify imports
    _apply_rename_item,  # noqa: F401 - compatibility alias for schema.modify imports
    _apply_reorder_items,  # noqa: F401 - compatibility alias for schema.modify imports
    _apply_set_formula,  # noqa: F401 - compatibility alias for schema.modify imports
    _apply_set_value,  # noqa: F401 - compatibility alias for schema.modify imports
    _counts,
    _dispatch_op,
)
from schema.modify_layout import (
    _assert_layout_integrity,
    _collect_sheet_items,  # noqa: F401 - compatibility alias for schema.modify imports
    _extract_formula_refs,  # noqa: F401 - compatibility alias for schema.modify imports
    _find_item_location,  # noqa: F401 - compatibility alias for schema.modify imports
    _iter_item_formula_specs,  # noqa: F401 - compatibility alias for schema.modify imports
    _iter_model_items,  # noqa: F401 - compatibility alias for schema.modify imports
    _matches_custom_concept_target,  # noqa: F401 - compatibility alias for schema.modify imports
    _replace_refs,  # noqa: F401 - compatibility alias for schema.modify imports
    _replace_refs_in_item,  # noqa: F401 - compatibility alias for schema.modify imports
)
from schema.modify_persistence import (
    _MissingFile,
    _read_optional_bytes,
    _restore_optional_bytes,
    _sha256,
)
from schema.model_writer_lock import model_writer_lock
from schema.refs import line_item_ref_from_obj  # noqa: F401 - compatibility alias for schema.modify imports
from schema.renderer import (
    _fixed_cell_anchor_period,  # noqa: F401 - compatibility alias for schema.modify imports
    _spec_for_period,  # noqa: F401
    render_model,
    render_plan_to_addin_payload,
)
from schema.segments import _shift_rows  # noqa: F401 - compatibility alias for schema.modify imports
from schema import serialization
from schema.handle import evict_handle, load_handle, peek_handle


def _load_on_cache_miss_enabled() -> bool:
    value = os.getenv("SCHEMA_MODIFY_LOAD_ON_CACHE_MISS", "true").strip().lower()
    return value not in {"0", "false", "no", "off"}


class ModifyResult(BaseModel):
    operations_applied: int
    operations_failed: int
    rolled_back: bool
    output_path: Optional[str] = None
    workbook_status: Optional[Dict[str, Any]] = None
    results: List[OperationResult] = Field(default_factory=list)
    stale_file_detected: bool = False


def _schema_sidecar_matches(file_path: str, expected_model: FinancialModel) -> tuple[bool, str]:
    loaded = serialization.try_load_sidecar(file_path)
    if loaded is None:
        return False, "schema_sidecar_unavailable"
    loaded_model, _base_results = loaded
    if loaded_model.model_dump(mode="json") != expected_model.model_dump(mode="json"):
        return False, "schema_sidecar_model_mismatch"
    return True, "ok"


def apply_modify_request(
    file_path: str,
    operations: List[Operation],
    *,
    target: Literal["file", "workbook", "both"] = "file",
    conflict_strategy: Literal["fail_on_collision", "overwrite"] = "fail_on_collision",
    force_overwrite: bool = False,
    best_effort: bool = False,
    historical_cutoff_year: Optional[int] = None,
    post_process_model: Optional[Callable[[FinancialModel], Any]] = None,
) -> ModifyResult:
    cutoff = historical_cutoff_year if historical_cutoff_year is not None else datetime.now().year
    handle = peek_handle(file_path, cutoff)
    if handle is None and _load_on_cache_miss_enabled():
        try:
            handle = load_handle(file_path, historical_cutoff_year=cutoff)
        except Exception as exc:
            raise ModifyError(
                f"Unable to load model for {file_path!r} (cutoff={cutoff}) on cache miss: {exc}"
            ) from exc
    if handle is None:
        raise ModifyError(
            f"Model not in cache for {file_path!r} (cutoff={cutoff}). "
            "Call model_build first to warm canonical state, then retry modify."
        )

    parsed_ops = [
        op if isinstance(op, Operation) else Operation.model_validate(op)
        for op in operations
    ]

    if not parsed_ops:
        applied, failed = _counts([])
        return ModifyResult(
            operations_applied=applied,
            operations_failed=failed,
            rolled_back=False,
            results=[],
        )

    expected_sha = _sha256(file_path)
    snapshot = copy.deepcopy(handle.model)

    results: List[OperationResult] = []
    for op in parsed_ops:
        try:
            result = _dispatch_op(snapshot, op, file_path=file_path)
            results.append(result)
            if result.status == "error" and not best_effort:
                applied, failed = _counts(results)
                return ModifyResult(
                    operations_applied=applied,
                    operations_failed=failed,
                    rolled_back=True,
                    results=results,
                )
        except Exception as exc:
            results.append(
                OperationResult(
                    op_type=op.type,
                    item_id=op.item_id,
                    status="error",
                    reason=str(exc),
                )
            )
            if not best_effort:
                applied, failed = _counts(results)
                return ModifyResult(
                    operations_applied=applied,
                    operations_failed=failed,
                    rolled_back=True,
                    results=results,
                )

    snapshot.build_index()
    if post_process_model is not None:
        try:
            post_process_model(snapshot)
            snapshot.build_index()
        except Exception as exc:
            results.append(
                OperationResult(
                    op_type=OperationType.set_value,
                    item_id=None,
                    status="error",
                    reason=f"post_process_failed: {exc}",
                )
            )
            applied, failed = _counts(results)
            return ModifyResult(
                operations_applied=applied,
                operations_failed=failed,
                rolled_back=True,
                results=results,
            )
    try:
        _assert_layout_integrity(snapshot)
    except LayoutError as exc:
        results.append(
            OperationResult(
                op_type=OperationType.set_value,
                item_id=None,
                status="error",
                reason=f"validation_failed: {exc}",
            )
        )
        applied, failed = _counts(results)
        return ModifyResult(
            operations_applied=applied,
            operations_failed=failed,
            rolled_back=True,
            results=results,
        )

    new_plan = render_model(snapshot)

    with model_writer_lock(file_path, ticker=snapshot.company.ticker):
        output_path: Optional[str] = None
        file_write_succeeded = False
        original_file_bytes: bytes | object = _MissingFile
        original_sidecar_bytes: bytes | object = _MissingFile
        sidecar_path = serialization.sidecar_path(file_path)
        if target in {"file", "both"}:
            current_sha = _sha256(file_path)
            if current_sha != expected_sha and not force_overwrite:
                applied, failed = _counts(results)
                return ModifyResult(
                    operations_applied=applied,
                    operations_failed=failed,
                    rolled_back=True,
                    stale_file_detected=True,
                    results=results,
                )

            original_file_bytes = _read_optional_bytes(Path(file_path))
            original_sidecar_bytes = _read_optional_bytes(sidecar_path)
            tmp_path = file_path + ".tmp"
            try:
                write_xlsx(new_plan, tmp_path)
                os.replace(tmp_path, file_path)
                output_path = file_path
                file_write_succeeded = True
            finally:
                Path(tmp_path).unlink(missing_ok=True)

        workbook_status: Optional[Dict[str, Any]] = None
        workbook_dispatch_failed = False
        if target in {"workbook", "both"}:
            payload = render_plan_to_addin_payload(
                new_plan,
                conflict_strategy=conflict_strategy,
            )
            try:
                workbook_status = _dispatch_to_addin("apply_render_plan", payload)
            except Exception as exc:
                workbook_dispatch_failed = True
                workbook_status = {
                    "status": "error",
                    "error": str(exc),
                    "error_type": type(exc).__name__,
                    "kind": "dispatch_failed",
                }

        if file_write_succeeded or (target == "workbook" and not workbook_dispatch_failed):
            load_handle(
                file_path,
                model=snapshot,
                historical_cutoff_year=cutoff,
                persist=file_write_succeeded,
            )
            if file_write_succeeded:
                sidecar_ok, sidecar_reason = _schema_sidecar_matches(file_path, snapshot)
                if not sidecar_ok:
                    _restore_optional_bytes(Path(file_path), original_file_bytes)
                    _restore_optional_bytes(sidecar_path, original_sidecar_bytes)
                    evict_handle(file_path, cutoff)
                    results.append(
                        OperationResult(
                            op_type=OperationType.set_value,
                            item_id=None,
                            status="error",
                            reason=sidecar_reason,
                            details={
                                "hint": (
                                    "Workbook writes must persist a matching .schema.json sidecar; "
                                    "without it, future model tools lose canonical template item ids."
                                ),
                                "sidecar_path": str(sidecar_path),
                            },
                        )
                    )
                    applied, failed = _counts(results)
                    return ModifyResult(
                        operations_applied=applied,
                        operations_failed=failed,
                        rolled_back=True,
                        results=results,
                    )

        applied, failed = _counts(results)
        return ModifyResult(
            operations_applied=applied,
            operations_failed=failed,
            rolled_back=(target == "workbook" and workbook_dispatch_failed),
            output_path=output_path,
            workbook_status=workbook_status,
            results=results,
        )
