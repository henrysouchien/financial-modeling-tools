#!/usr/bin/env python3
"""Model Engine MCP Server — exposes schema financial model tools via MCP."""

import sys

_real_stdout = sys.stdout
sys.stdout = sys.stderr

import os
from pathlib import Path
from typing import Dict, List, Optional

from fastmcp import FastMCP

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from schema.tools import (  # noqa: E402
  clear_cache as _clear_cache,
  drivers as _drivers,
  find as _find,
  scenario as _scenario,
  sensitivity as _sensitivity,
  summarize as _summarize,
  values as _values,
)

sys.stdout = _real_stdout

mcp = FastMCP(
  "model-engine",
  instructions="""Financial model schema engine tools.

Use these tools to analyze Excel-based financial models:
- model_summarize: Get model structure, sheets, sections, and key metrics
- model_find: Search for line items by name/ID
- model_values: Get full time series for specific line items
- model_drivers: Trace upstream driver tree for any line item
- model_sensitivity: Rank inputs by impact on a target metric (can be slow ~30-300s)
- model_scenario: Apply overrides to inputs and compare resulting metrics
- model_clear_cache: Clear the in-memory model cache

Most tools require file_path pointing to an Excel model file (.xlsx).
First call for a given file_path is slow (parses Excel); subsequent calls use cache.""",
)


def _validate_file_path(file_path: str) -> str:
  """Expand ~ and verify the file exists. Returns resolved path."""
  resolved = os.path.expanduser(file_path)
  if not os.path.isfile(resolved):
    raise FileNotFoundError(f"Model file not found: {resolved}")
  return resolved


@mcp.tool()
def model_summarize(
  file_path: str,
  historical_cutoff_year: Optional[int] = None,
) -> dict:
  """Summarize model structure, sheets, sections, item counts, and key metrics.

  Discovery: pass file_path as an existing .xlsx model path. If the caller only
  has a workspace or ticker, locate the workbook first with file search or the
  upstream model-building tool, then use this summary to discover sheet names,
  sections, item IDs, and period coverage.

  Sibling tools: call model_find after this when you need candidate item IDs by
  label, model_values when you already know item IDs, and model_clear_cache
  after editing the workbook on disk.

  Common mistake: historical_cutoff_year changes period classification only; it
  does not filter the workbook file itself.
  """
  try:
    file_path = _validate_file_path(file_path)
    result = _summarize(file_path, historical_cutoff_year=historical_cutoff_year)
    return {"status": "ok", **result}
  except Exception as exc:
    return {"status": "error", "error": str(exc)}


@mcp.tool()
def model_find(
  file_path: str,
  query: str,
  limit: int = 20,
  historical_cutoff_year: Optional[int] = None,
) -> dict:
  """Search for line items by name or ID substring match.

  Discovery: call model_summarize first to confirm file_path parses and to
  understand available sheets/sections. Use a short query such as revenue,
  ebitda, working capital, or part of a known item_id; returned items include
  IDs that can feed model_values, model_drivers, sensitivity, and scenario.

  Sibling tools: use model_values for exact item IDs, model_drivers for one
  item's upstream tree, and model_summarize when the caller has not inspected
  the workbook structure yet.

  Common mistake: query is a substring search, not a formula evaluator. Use the
  returned item IDs in downstream tools.
  """
  try:
    file_path = _validate_file_path(file_path)
    results = _find(
      file_path,
      query,
      limit=limit,
      historical_cutoff_year=historical_cutoff_year,
    )
    return {"status": "ok", "items": results, "count": len(results)}
  except Exception as exc:
    return {"status": "error", "error": str(exc)}


@mcp.tool()
def model_values(
  file_path: str,
  item_ids: List[str],
  periods: str = "all",
  historical_cutoff_year: Optional[int] = None,
) -> dict:
  """Get time-series values for one or more model line items.

  Discovery: run model_find or model_summarize first to obtain exact item_ids.
  Pass item_ids as a list of IDs from those results; periods can be all or the
  period selector supported by the schema tools.

  Sibling tools: use model_drivers when you need to explain where a value comes
  from, model_sensitivity to rank upstream inputs for one target, and
  model_scenario to compare values after overrides.

  Common mistake: item_ids are schema/model line-item IDs, not display labels.
  Search with model_find when you only know the label.
  """
  try:
    file_path = _validate_file_path(file_path)
    result = _values(
      file_path,
      item_ids,
      periods=periods,
      historical_cutoff_year=historical_cutoff_year,
    )
    return {"status": "ok", **result}
  except Exception as exc:
    return {"status": "error", "error": str(exc)}


@mcp.tool()
def model_drivers(
  file_path: str,
  item_id: str,
  depth: int = 3,
  historical_cutoff_year: Optional[int] = None,
) -> dict:
  """Trace the upstream driver tree for one model line item.

  Discovery: run model_find or model_values first to choose the exact item_id.
  Increase depth only when the first driver tree is too shallow; larger depths
  can make the response much larger.

  Sibling tools: use model_values for the target's time series,
  model_sensitivity to quantify which drivers matter most, and model_scenario
  to test explicit input overrides.

  Common mistake: item_id must be one line-item ID. Do not pass a display label
  or a list; use model_find and then call this tool once per target.
  """
  try:
    file_path = _validate_file_path(file_path)
    result = _drivers(
      file_path,
      item_id,
      depth=depth,
      historical_cutoff_year=historical_cutoff_year,
    )
    return {"status": "ok", **result}
  except Exception as exc:
    return {"status": "error", "error": str(exc)}


@mcp.tool()
def model_sensitivity(
  file_path: str,
  target_id: str,
  n: int = 15,
  bump_pct: float = 0.10,
  candidate_filter: str = "drivers",
  max_candidates: Optional[int] = None,
  historical_cutoff_year: Optional[int] = None,
) -> dict:
  """Rank upstream inputs by impact on one target metric.

  Discovery: run model_find/model_values first to choose target_id, and
  model_drivers to sanity-check the upstream driver graph. candidate_filter
  controls which inputs are considered; max_candidates limits expensive scans.

  Sibling tools: use model_drivers for explainability, model_scenario when you
  already know the overrides to test, and model_values to inspect the target
  before and after sensitivity work.

  Common mistake: this can be slow on large workbooks. Start with a modest n,
  candidate_filter='drivers', and max_candidates when an agent needs a quick
  read.
  """
  try:
    file_path = _validate_file_path(file_path)
    result = _sensitivity(
      file_path,
      target_id,
      n=n,
      bump_pct=bump_pct,
      candidate_filter=candidate_filter,
      max_candidates=max_candidates,
      historical_cutoff_year=historical_cutoff_year,
    )
    return {"status": "ok", **result}
  except Exception as exc:
    return {"status": "error", "error": str(exc)}


@mcp.tool()
def model_scenario(
  file_path: str,
  overrides: Dict[str, Dict[str, float]],
  compare_items: Optional[List[str]] = None,
  historical_cutoff_year: Optional[int] = None,
) -> dict:
  """Apply input overrides and compare resulting metrics against the base case.

  Discovery: run model_find/model_values first to obtain exact input item IDs
  and compare item IDs. overrides must map item_id to {period: value}; periods
  are converted to integer period keys before evaluation.

  Sibling tools: use model_sensitivity to discover candidate inputs,
  model_drivers to understand upstream dependencies, and model_values to fetch
  current values before choosing override magnitudes.

  Common mistake: overrides use item IDs and period keys, not display labels or
  Excel cell addresses. Use model_find and model_values to build the override
  payload.
  """
  try:
    file_path = _validate_file_path(file_path)
    normalized: Dict[str, Dict[int, float]] = {}
    if not isinstance(overrides, dict):
      return {"status": "error", "error": "overrides must be a dict mapping item_id -> {period -> value}"}
    for item_id, period_values in overrides.items():
      if not isinstance(period_values, dict):
        return {"status": "error", "error": f"overrides['{item_id}'] must be a dict mapping period -> value"}
      normalized[item_id] = {int(k): float(v) for k, v in period_values.items()}

    result = _scenario(
      file_path,
      normalized,
      compare_items=compare_items,
      historical_cutoff_year=historical_cutoff_year,
    )
    return {"status": "ok", **result}
  except Exception as exc:
    return {"status": "error", "error": str(exc)}


@mcp.tool()
def model_clear_cache() -> dict:
  """Clear the in-memory parsed-model cache.

  Discovery: call this after editing a workbook on disk or after a model build
  rewrites a file_path that has already been parsed by this server.

  Sibling tools: use model_summarize immediately after clearing to confirm the
  server reloads the current workbook. Value, driver, sensitivity, and scenario
  tools will also reload on their next call.

  Common mistake: this does not delete model files or generated artifacts; it
  only clears the process-local cache.
  """
  try:
    _clear_cache()
    return {"status": "ok", "message": "Model cache cleared"}
  except Exception as exc:
    return {"status": "error", "error": str(exc)}


if __name__ == "__main__":
  mcp.run()
