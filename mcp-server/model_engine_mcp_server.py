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
  """Summarize model structure: sheets, sections, item counts, key metrics."""
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
  """Search for line items by name or ID substring match."""
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
  """Get full time series values for one or more line items."""
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
  """Trace the upstream driver tree for a line item."""
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
  """Rank upstream inputs by impact on a target metric. Can be slow."""
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
  """Apply input overrides and compare resulting metrics against the base case."""
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
  """Clear the in-memory model cache. Useful after editing model files."""
  try:
    _clear_cache()
    return {"status": "ok", "message": "Model cache cleared"}
  except Exception as exc:
    return {"status": "error", "error": str(exc)}


if __name__ == "__main__":
  mcp.run()
