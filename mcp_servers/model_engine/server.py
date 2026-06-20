from __future__ import annotations

from fastmcp import FastMCP


MODEL_ENGINE_INSTRUCTIONS = """Financial model schema engine tools.

Use these tools to analyze Excel-based financial models:
- model_summarize: Get model structure, sheets, sections, and key metrics
- model_valuation_summary: Read canonical valuation/DCF/WACC outputs without row probing
- model_semantics: Read typed forecast/scenario/valuation intent for a current model or ticker
- model_find: Search for line items by name/ID
- model_values: Get full time series for specific line items
- model_drivers: Trace upstream driver tree for any line item
- model_sensitivity: Rank inputs by impact on a target metric (can be slow ~30-300s)
- model_scenario: Apply overrides to inputs and compare resulting metrics
- model_presentation_fingerprint: Inventory workbook presentation/style primitives
- model_presentation_compare: Compare observed workbook presentation primitives against a reference
- model_scenario_topology: List scenario owner, anchor, and bull/base/bear case rows
- model_find_scenario_anchor: Resolve thesis factors to workbook scenario anchors
- model_bridge_scenarios: Write thesis scenarios into workbook scenario rows
- model_discover_segments: Discover all validated EDGAR segment axes before building
- business_model_validate: Validate a BusinessModel JSON/markdown artifact before build
- model_build: Build a populated Excel model from the SIA template plus financial data
  (optional historical_sources enables per-concept FMP/EDGAR routing and fallback;
   optional valuation_comps populates scenario comp tables)
- comps_build: Render an industry_peer_comparison payload into a deterministic
  comp-sheet artifact, workbook tab, or grid rows
- annotate_model_with_research: Post-build research handoff annotation for a workbook
- model_clear_cache: Clear the in-memory model cache

Most tools require file_path pointing to an Excel model file (.xlsx).
First call for a given file_path is slow (parses Excel); subsequent calls use cache."""


def create_mcp() -> FastMCP:
  return FastMCP(
    "model-engine",
    instructions=MODEL_ENGINE_INSTRUCTIONS,
  )


def main(mcp: FastMCP) -> None:
  mcp.run()


__all__ = [
  "FastMCP",
  "MODEL_ENGINE_INSTRUCTIONS",
  "create_mcp",
  "main",
]
