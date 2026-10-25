"""A library raise read as a refusal: its code, clause, next call, raise site and owner.

The build side records library raises as refusal rows (``target_build``'s ``_Refusals``,
a construction helper's ``refused`` row), except marked scenario ModelSpec admission
errors, which reach the door unchanged. The bench's run record
(``scripts/research/bench/record.py``) and harness walk read the same fields from here.
"""

from __future__ import annotations

from pathlib import Path
import traceback
from typing import Any


#: The checkout a raise site is reported relative to: the directory holding this root.
_CHECKOUT = Path(__file__).resolve().parents[1]


def walk_site(tb: Any) -> str | None:
  """The innermost checkout frame of a traceback (this module and installed packages excluded)."""
  site = None
  for frame in traceback.extract_tb(tb):
    path = Path(frame.filename).resolve()
    if path == Path(__file__).resolve() or ".venv" in path.parts or "site-packages" in path.parts:
      continue
    try:
      relative = path.relative_to(_CHECKOUT)
    except ValueError:
      continue
    site = f"{relative}:{frame.lineno} ({frame.name})"
  return site


def walk_refusal(exc: BaseException) -> dict[str, Any]:
  """A refusal's code, clause, next_call, raise site and owner.

  The owner is the innermost raise site below the build lifecycle's re-coding
  wrapper: a wrapped refusal is constructed, never raised, so the original
  sits on the ``__cause__``/``__context__`` side.
  """
  data = getattr(exc, "data", None)
  data = data if isinstance(data, dict) else {}
  code = getattr(exc, "code", None) or data.get("code") or data.get("reason")
  seen: list[BaseException] = []
  frontier: list[BaseException] = [exc]
  while frontier:
    current = frontier.pop(0)
    for following in (current.__cause__, current.__context__, getattr(current, "cause", None)):
      if isinstance(following, BaseException) and following is not exc and all(following is not item for item in seen):
        seen.append(following)
        frontier.append(following)
  sited = [item for item in seen if walk_site(item.__traceback__) is not None]
  inner = [item for item in sited if "normal_handoff_build_lifecycle.py" not in str(walk_site(item.__traceback__))]
  origin = (inner or sited or [exc])[-1]
  next_call = getattr(exc, "next_call", None)
  return {
    "exception": type(exc).__name__,
    "code": None if code is None else str(code),
    "clause": getattr(exc, "clause", None) or data.get("clause"),
    "next_call": next_call if next_call is not None else data.get("next_call"),
    "stage": getattr(exc, "stage", None),
    "message": str(exc)[:3000],
    "raised_at": walk_site(exc.__traceback__),
    "owner": walk_site(origin.__traceback__) or walk_site(exc.__traceback__),
    "cause_chain": [f"{type(item).__name__}({getattr(item, 'code', '')})@{walk_site(item.__traceback__)}" for item in seen],
  }


def site_file(site: str | None) -> str | None:
  """`path:line (function)` → `path`."""
  if not site:
    return None
  return site.split(" (", 1)[0].rsplit(":", 1)[0]


def refusal_row(
  exc: BaseException, *, stage: str | None = None, status: str | None = None,
  include_raise_site: bool = True,
) -> dict[str, Any]:
  """Build refusal fields, with a message for a stage or status row.

  Scenario admission errors reach the door unchanged. ``include_raise_site`` omits
  the traceback location for input findings, which carry only the owner's file.
  """
  if (
    stage is not None
    and (getattr(exc, "data", None) or {}).get("reason") == "scenario_model_spec_admission"
  ):
    raise exc
  walked = walk_refusal(exc)
  row = {
    "code": walked["code"] or walked["exception"],
    "owner_file": site_file(walked["owner"]),
  }
  if include_raise_site:
    row["raise_site"] = walked["raised_at"]
  if stage is not None:
    row["stage"] = stage
  if status is not None:
    row["status"] = status
  if stage is not None or status is not None:
    row["message"] = str(exc)[:2000]
    if hasattr(exc, "field") and hasattr(exc, "fix"):
      row["field"] = getattr(exc, "field", None)
      row["got"] = getattr(exc, "raw_got", getattr(exc, "got", None))
      row["fix"] = getattr(exc, "fix", None)
      row["example"] = getattr(exc, "example", None)
  return row
