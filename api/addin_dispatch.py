"""Excel add-in dispatch compatibility for the standalone mirror."""

from __future__ import annotations

from typing import Any


class StandaloneAddinDispatchUnavailable(RuntimeError):
    """Raised when workbook live-dispatch is requested outside AI-excel-addin."""


def classify_addin_dispatch_error(error: BaseException | str) -> str:
    message = str(error).strip().lower()
    if "standalone" in message or "not available" in message:
        return "standalone_unavailable"
    if "timeout" in message or "timed out" in message:
        return "dispatch_timeout"
    return "dispatch_failed"


def addin_dispatch_error_status(
    error: BaseException | str,
    *,
    status: str = "error",
    kind: str = "live_workbook_unavailable",
) -> dict[str, Any]:
    return {
        "status": status,
        "error": str(error),
        "error_type": type(error).__name__ if isinstance(error, BaseException) else "DispatchError",
        "kind": kind,
        "reason": classify_addin_dispatch_error(error),
        "live_workbook_unavailable": True,
    }


def _dispatch_to_addin(tool_name: str, tool_input: dict, timeout: int = 180) -> dict:
    _ = tool_name, tool_input, timeout
    raise StandaloneAddinDispatchUnavailable(
        "Excel add-in dispatch is not available in the standalone financial-modeling-tools mirror"
    )

