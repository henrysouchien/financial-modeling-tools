from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Optional, get_type_hints


# The registered tool keeps its agent-facing name `statement_mapping_validate`.
# Step C-4 retargets what it validates (a `statement_windows` candidate) and
# what it returns, but renaming a registered MCP tool would move a product
# surface pinned in several other lanes' catalogs and policies, which this step
# does not own. The name is the tool's identity; the payload is the contract.


@dataclass(frozen=True)
class StatementWindowToolDeps:
  coerce_json_dict_arg: Callable[..., dict[str, Any]]
  make_edgar_financials_fetcher: Callable[[], Any]
  load_snapshot: Callable[[str], Any]
  preview_statement_windows: Callable[..., Any]
  parse_statement_windows: Callable[[Any], dict[str, Any]]
  serialize_statement_windows: Callable[[Any], dict[str, Any]]
  model_tool_error_payload: Callable[[Exception], dict[str, Any]]
  provider_registry: Any | None = None


def _candidate_payloads(
  deps: StatementWindowToolDeps,
  *,
  ticker: str,
  historical_years: list[int],
  financials_payloads_by_year: dict | None,
) -> dict[int, dict[str, Any]]:
  if financials_payloads_by_year is not None:
    raw = deps.coerce_json_dict_arg(
      financials_payloads_by_year,
      name="financials_payloads_by_year",
    )
    payloads: dict[int, dict[str, Any]] = {}
    for raw_year, payload in raw.items():
      try:
        year = int(raw_year)
      except (TypeError, ValueError) as exc:
        raise ValueError(
          "financials_payloads_by_year keys must be fiscal years"
        ) from exc
      if not isinstance(payload, dict):
        raise ValueError(
          f"financials_payloads_by_year[{raw_year!r}] must be an object"
        )
      payloads[year] = payload
    return payloads

  fetcher = deps.make_edgar_financials_fetcher()
  payloads: dict[int, dict[str, Any]] = {}
  for year in historical_years:
    payload = fetcher(ticker, year, 4, True)
    if isinstance(payload, dict):
      payloads[int(year)] = payload
  return payloads


def _preview_candidate(
  deps: StatementWindowToolDeps,
  *,
  ticker: str,
  statement_windows: dict,
  most_recent_fy: int,
  n_historical: int,
  n_projection: int,
  financials_payloads_by_year: dict | None,
) -> Any:
  ticker_upper = str(ticker or "").strip().upper()
  if not ticker_upper:
    raise ValueError("ticker is required")
  candidate = deps.coerce_json_dict_arg(
    statement_windows,
    name="statement_windows",
  )
  historical_count = int(n_historical)
  projection_count = int(n_projection)
  if historical_count <= 0:
    raise ValueError("n_historical must be positive")
  if projection_count <= 0:
    raise ValueError("n_projection must be positive")
  provider_registry = getattr(deps, "provider_registry", None)
  snapshot_kwargs = (
    {"provider_registry": provider_registry}
    if provider_registry is not None
    else {}
  )
  snapshot = deps.load_snapshot(ticker_upper, **snapshot_kwargs)
  snapshot_registry = getattr(snapshot, "provider_registry", None)
  active_registry = (
    provider_registry
    if provider_registry is not None
    else snapshot_registry
  )
  registry_kwargs = (
    {"provider_registry": active_registry}
    if active_registry is not None
    else {}
  )
  parsed = deps.parse_statement_windows(candidate, **registry_kwargs)
  if not parsed:
    raise ValueError("statement_windows must contain at least one window")
  canonical_candidate = deps.serialize_statement_windows(
    parsed,
    **registry_kwargs,
  )
  historical_years = list(
    range(
      int(most_recent_fy) - historical_count + 1,
      int(most_recent_fy) + 1,
    )
  )
  payloads = _candidate_payloads(
    deps,
    ticker=ticker_upper,
    historical_years=historical_years,
    financials_payloads_by_year=financials_payloads_by_year,
  )
  return deps.preview_statement_windows(
    ticker=ticker_upper,
    statement_windows=canonical_candidate,
    most_recent_fy=int(most_recent_fy),
    n_historical=historical_count,
    n_projection=projection_count,
    financials_payloads_by_year=payloads,
    snapshot=snapshot,
    **registry_kwargs,
  )


def statement_mapping_validate_handler(
  *,
  deps: StatementWindowToolDeps,
  ticker: str,
  statement_windows: dict,
  most_recent_fy: int,
  n_historical: int = 5,
  n_projection: int = 2,
  financials_payloads_by_year: dict | None = None,
) -> dict[str, Any]:
  try:
    preview = _preview_candidate(
      deps,
      ticker=ticker,
      statement_windows=statement_windows,
      most_recent_fy=most_recent_fy,
      n_historical=n_historical,
      n_projection=n_projection,
      financials_payloads_by_year=financials_payloads_by_year,
    )
    return {"status": "success", **preview.to_dict()}
  except Exception as exc:
    payload = deps.model_tool_error_payload(exc)
    payload.setdefault("ticker", str(ticker or "").strip().upper())
    return payload


ParentNamespaceProvider = Callable[[], dict[str, Any]]


@dataclass(frozen=True)
class StatementWindowToolFunctions:
  statement_mapping_validate: Callable[..., dict[str, Any]]


def _bind_parent_module(
  function: Callable[..., dict[str, Any]],
  parent_namespace: ParentNamespaceProvider,
) -> Callable[..., dict[str, Any]]:
  parent_module = parent_namespace().get("__name__")
  if isinstance(parent_module, str):
    function.__module__ = parent_module
    function.__qualname__ = function.__name__
    function.__annotations__ = {**get_type_hints(function), "return": dict}
  return function


def _register_tool(
  mcp: Any,
  function: Callable[..., dict[str, Any]],
) -> Callable[..., dict[str, Any]]:
  registered = mcp.tool(meta={
    "hank/code-disposition": "read_composable",
    "hank/code-reason": "Validates a candidate statement mapping from a cell against historical EDGAR financials; a cold fetch that persists artifacts is a side effect of the read.",
  })(function)
  return registered or function


def build_statement_mapping_tool_functions(
  *,
  parent_namespace: ParentNamespaceProvider,
) -> StatementWindowToolFunctions:
  def statement_mapping_validate(
    ticker: str,
    statement_windows: dict,
    *,
    most_recent_fy: int,
    n_historical: int = 5,
    n_projection: int = 2,
    financials_payloads_by_year: Optional[dict] = None,
  ) -> dict:
    """Read-only compile and topology preview for a candidate window."""

    return statement_mapping_validate_handler(
      deps=parent_namespace()["_statement_mapping_tool_deps"](),
      ticker=ticker,
      statement_windows=statement_windows,
      most_recent_fy=most_recent_fy,
      n_historical=n_historical,
      n_projection=n_projection,
      financials_payloads_by_year=financials_payloads_by_year,
    )

  return StatementWindowToolFunctions(
    statement_mapping_validate=statement_mapping_validate,
  )


def register_statement_mapping_tools(
  mcp: Any,
  *,
  parent_namespace: ParentNamespaceProvider,
  functions: StatementWindowToolFunctions | None = None,
) -> StatementWindowToolFunctions:
  functions = functions or build_statement_mapping_tool_functions(
    parent_namespace=parent_namespace,
  )
  return StatementWindowToolFunctions(
    statement_mapping_validate=_register_tool(
      mcp,
      _bind_parent_module(functions.statement_mapping_validate, parent_namespace),
    )
  )


__all__ = [
  "StatementWindowToolDeps",
  "StatementWindowToolFunctions",
  "build_statement_mapping_tool_functions",
  "register_statement_mapping_tools",
  "statement_mapping_validate_handler",
]
