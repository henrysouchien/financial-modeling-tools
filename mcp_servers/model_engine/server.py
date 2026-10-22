"""Own model-engine server metadata, FastMCP construction, and CLI dispatch.

The executable shim in ``mcp_servers/model_engine_mcp_server.py`` registers
tool handlers on the ``FastMCP`` returned here. This module owns the single
declarative tool catalogue and capabilities projection, but not handler or
schema behavior. See ``mcp_servers/README.md`` for the owner boundary.
"""

from __future__ import annotations

import json
import sys
from collections.abc import Sequence
from typing import TextIO

from fastmcp import FastMCP


MODEL_ENGINE_CAPABILITIES_URI = "resource://model-engine/capabilities"
MODEL_ENGINE_CAPABILITIES_VERSION = "2.0"

_MODEL_ENGINE_CLUSTERS = (
  {
    "name": "inspect",
    "description": "Inspect model structure, values, dependencies, scenarios, and presentation.",
    "tools": (
      ("model_summarize", "Summarize sheets, sections, items, and key metrics."),
      ("model_valuation_summary", "Read canonical valuation, DCF, WACC, and multiple outputs."),
      ("model_semantics", "Read typed forecast, scenario, valuation, and source intent."),
      ("model_presentation_fingerprint", "Inventory workbook presentation primitives."),
      ("model_presentation_compare", "Compare presentation primitives with a reference."),
      ("model_find", "Search for model line items by name or ID."),
      ("model_values", "Read bounded time series for selected line items."),
      ("model_drivers", "Trace the upstream driver tree for a line item."),
      (
        "model_sensitivity",
        "Rank inputs by impact on a target metric; on the current model, view=judgments|compare|imply ranks judgment rows, computes the Variance against the price and consensus, or implies the pinned price.",
      ),
      ("model_scenario", "Apply in-memory overrides and compare resulting metrics."),
    ),
  },
  {
    "name": "discover-and-validate",
    "description": "Discover build inputs and validate model artifacts without durable writes.",
    "tools": (
      ("model_discover_segments", "Discover validated EDGAR segment axes before building."),
      ("business_model_validate", "Validate a BusinessModel JSON or markdown artifact."),
      ("statement_mapping_validate", "Preview and validate statement mappings without persisting them."),
      ("model_scenario_topology", "List scenario owners, anchors, and case rows."),
      ("model_find_scenario_anchor", "Resolve thesis factors to workbook scenario anchors."),
    ),
  },
)

MODEL_ENGINE_TOOL_NAMES = tuple(
  tool_name
  for cluster in _MODEL_ENGINE_CLUSTERS
  for tool_name, _description in cluster["tools"]
)


def _instruction_text() -> str:
  lines = [
    "Read-only financial model schema engine (MCP-first stdio server).",
    "",
    "Use the MCP tools below for the complete supported surface. The packaged",
    "financial-model-engine command is a partial developer CLI: it starts this server",
    "or prints discovery metadata; it does not mirror the model tools as subcommands.",
  ]
  for cluster in _MODEL_ENGINE_CLUSTERS:
    lines.extend(("", f"{cluster['name']} — {cluster['description']}"))
    lines.extend(
      f"- {tool_name}: {description}"
      for tool_name, description in cluster["tools"]
    )
  lines.extend((
    "",
    "Workbook inspection tools require file_path pointing to an .xlsx model. The first",
    "parse is slower; subsequent reads may use cache. A model_values response carries",
    "at most 600 cells: a larger request returns the first item batch with",
    "requested_cells and remaining_item_id_batches to call next. For model_semantics,",
    "narrow with sections/item_ids/driver_keys/scenario_names/years and paginate with",
    "cursor; a page over 40,000 characters halves until it fits, and",
    "view.response_chars records the returned size.",
    f"Machine-readable capabilities: {MODEL_ENGINE_CAPABILITIES_URI}",
  ))
  return "\n".join(lines)


MODEL_ENGINE_INSTRUCTIONS = _instruction_text()


def model_engine_capabilities() -> dict:
  clusters = [
    {
      "name": cluster["name"],
      "description": cluster["description"],
      "tools": [
        {"name": tool_name, "description": description}
        for tool_name, description in cluster["tools"]
      ],
    }
    for cluster in _MODEL_ENGINE_CLUSTERS
  ]
  return {
    "schema_version": MODEL_ENGINE_CAPABILITIES_VERSION,
    "server": {
      "name": "model-engine",
      "positioning": "mcp-first",
      "transport": "stdio",
      "capabilities_resource": MODEL_ENGINE_CAPABILITIES_URI,
    },
    "contract": {
      "tool_count": len(MODEL_ENGINE_TOOL_NAMES),
      "tool_names": list(MODEL_ENGINE_TOOL_NAMES),
      "clusters": clusters,
    },
    "developer_cli": {
      "command": "financial-model-engine",
      "inspection": [
        "financial-model-engine --help",
        "financial-model-engine --capabilities",
        "financial-model-engine capabilities --json",
      ],
      "mcp_contract_parity": False,
      "tool_cli": "python -m schema.tools_cli --help",
    },
    "limits": {
      "model_values": {
        "max_response_cells": 600,
        "oversize": "first_item_batch_plus_remaining_item_id_batches",
      },
      "model_semantics": {
        "filters": [
          "sections",
          "item_ids",
          "driver_keys",
          "scenario_names",
          "years",
        ],
        "pagination": True,
        "source_detail": {
          "allowed": ["none", "metadata", "full"],
          "default": "metadata",
        },
        "min_limit": 1,
        "default_limit": 25,
        "max_limit": 100,
        "limit_out_of_range": "clamped",
        "max_response_chars": 40_000,
        "oversize": {
          "behavior": "halve_page_until_fits_or_one_entry",
          "result_fields": [
            "pagination.budget_limited",
            "pagination.next_cursor",
            "view.response_chars",
          ],
          "silent_truncation": False,
        },
      },
    },
  }


def model_engine_capabilities_json() -> str:
  return json.dumps(
    model_engine_capabilities(),
    indent=2,
    sort_keys=True,
  ) + "\n"


MODEL_ENGINE_HELP = f"""financial-model-engine help

Usage:
  financial-model-engine
      Start the MCP stdio server.
  financial-model-engine --help
  financial-model-engine help
      Show this help without starting MCP or validating runtime startup.
  financial-model-engine --capabilities
  financial-model-engine capabilities [--json]
      Print deterministic JSON capabilities without starting MCP or validating runtime startup.

This is a read-only MCP-first server, not a full tool-by-tool CLI.
For developer tool commands, run: python -m schema.tools_cli --help
Capabilities resource: {MODEL_ENGINE_CAPABILITIES_URI}
"""


_HELP_ARGUMENTS = frozenset({"--help", "help"})
_CAPABILITIES_ARGUMENTS = frozenset({"--capabilities", "capabilities"})
_CAPABILITIES_MODIFIERS = frozenset({"--json"})


def create_mcp() -> FastMCP:
  """Construct the unregistered server and its capabilities resource.

  The executable shim is the supported extension point for tool registration;
  callers receive a fresh server and this function performs no runtime startup.
  """
  mcp = FastMCP(
    "model-engine",
    instructions=MODEL_ENGINE_INSTRUCTIONS,
  )

  @mcp.resource(
    MODEL_ENGINE_CAPABILITIES_URI,
    name="model-engine-capabilities",
    version=MODEL_ENGINE_CAPABILITIES_VERSION,
    description="Deterministic model-engine tool contract and response limits.",
    mime_type="application/json",
  )
  def capabilities_resource() -> str:
    return model_engine_capabilities_json()

  return mcp


def main(
  mcp: FastMCP,
  *,
  argv: Sequence[str] | None = None,
  stdout: TextIO | None = None,
  stderr: TextIO | None = None,
) -> None:
  """Dispatch discovery-only CLI modes or run the supplied stdio server.

  Help and capabilities write deterministic output without starting FastMCP.
  Unsupported serving arguments exit with status 2; with no arguments, control
  delegates to ``FastMCP.run``.
  """
  args = list(sys.argv[1:] if argv is None else argv)
  out = sys.stdout if stdout is None else stdout
  err = sys.stderr if stderr is None else stderr

  def _warn_ignored(extras: list[str]) -> None:
    if extras:
      err.write(
        "warning[ignored_arguments]: ignoring unsupported financial-model-engine "
        f"arguments: {' '.join(extras)}\n"
      )

  if any(arg in _HELP_ARGUMENTS for arg in args):
    _warn_ignored([arg for arg in args if arg not in _HELP_ARGUMENTS])
    out.write(MODEL_ENGINE_HELP)
    return
  if any(arg in _CAPABILITIES_ARGUMENTS for arg in args):
    _warn_ignored(
      [
        arg
        for arg in args
        if arg not in _CAPABILITIES_ARGUMENTS and arg not in _CAPABILITIES_MODIFIERS
      ]
    )
    out.write(model_engine_capabilities_json())
    return
  if args:
    err.write(
      "error[invalid_arguments]: unsupported financial-model-engine arguments: "
      f"{' '.join(args)}\n"
      "The serving invocation is owned by the deploy MCP templates "
      "(deploy/mcp*.json), which pass no arguments.\n"
      "Run `financial-model-engine --help` for server discovery, or "
      "`python -m schema.tools_cli --help` for developer tool commands.\n"
    )
    raise SystemExit(2)

  mcp.run()


__all__ = [
  "FastMCP",
  "MODEL_ENGINE_CAPABILITIES_URI",
  "MODEL_ENGINE_CAPABILITIES_VERSION",
  "MODEL_ENGINE_HELP",
  "MODEL_ENGINE_INSTRUCTIONS",
  "MODEL_ENGINE_TOOL_NAMES",
  "create_mcp",
  "main",
  "model_engine_capabilities",
  "model_engine_capabilities_json",
]
