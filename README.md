# financial-modeling-tools

Toolkit for programmatically understanding, populating, and analyzing Excel-based financial models.

**Turn your financial model into code, and populate it with data.**

## What's in the box

| Component | Language | Description |
|-----------|----------|-------------|
| **Schema Engine** (`schema/`) | Python | Parse Excel models → inspect structure, trace formula dependencies, run sensitivity analysis, generate Python code |
| **Model Matcher** (`model-matcher/`) | TypeScript | Match structured financial data into model cells via constant/formula matching, conversion factor detection, collision flagging |
| **MCP Server** (`mcp-server/`) | Python | Expose the schema engine as tools for Claude and other AI agents |

## Schema Engine

Parse any Excel financial model and work with it programmatically.

### Capabilities

- **`model_summarize`** — Get model structure: sheets, sections, line items, key metrics
- **`model_find`** — Search for line items by name or ID (fuzzy matching, suggestions on typos)
- **`model_values`** — Get full time series for specific line items
- **`model_drivers`** — Trace the upstream driver tree for any line item
- **`model_sensitivity`** — Rank inputs by impact on a target metric
- **`model_scenario`** — Apply overrides to inputs and compare resulting metrics

### Usage

```python
from schema.tools import summarize, find_items, get_values, trace_drivers

# Parse and summarize a model
summary = summarize("path/to/model.xlsx")

# Find line items
results = find_items("path/to/model.xlsx", query="revenue")

# Get time series
values = get_values("path/to/model.xlsx", item_ids=["revenue", "net_income"])

# Trace what drives a metric
drivers = trace_drivers("path/to/model.xlsx", item_id="free_cash_flow", depth=3)
```

### Requirements

```
pip install openpyxl
```

## Model Matcher

Pure TypeScript library for matching financial data into an existing model column. Given a column of values in your model and a dataset of financial facts, it figures out which facts go where.

### How it works

1. **Constant matching** — finds cells where the model value equals the data value (within rounding tolerance)
2. **Formula matching** — tokenizes Excel formulas, identifies which data values appear as formula inputs, rebuilds with updated values
3. **Conversion factor detection** — auto-detects if the model is in thousands, millions, etc.
4. **Collision flagging** — marks cells where the data source indicates potential conflicts

### Usage

```typescript
import { match } from "model-matcher";

const result = match({
  data: [
    { prior_value: 100000, current_value: 120000, collision_flag: false },
    { prior_value: 200000, current_value: 240000, collision_flag: true },
  ],
  source_values: [100, 200],
  source_formulas: [null, "=200/100-1"],
  conversion_factor: 1000,
});

// result.changes — every row with change_type: constant | formula | cleared | unchanged | skipped
// result.stats — { processed, updated, cleared, collisions, low_match_warning }
// result.conversion_factor — the factor used (provided or auto-detected)
```

### Install

```bash
npm install  # from model-matcher/
```

## MCP Server

The MCP server exposes the schema engine tools for use with Claude Code, Claude Desktop, or any MCP-compatible client.

### Configuration

Add to your MCP client config (e.g., `~/.claude.json`):

```json
{
  "mcpServers": {
    "model-engine": {
      "command": "python",
      "args": ["/path/to/financial-modeling-tools/mcp-server/model_engine_mcp_server.py"]
    }
  }
}
```

Then ask Claude to analyze your model:

> "Summarize the structure of my DCF model"
> "What drives free cash flow in this model?"
> "Run a sensitivity analysis on revenue growth"

## Who this is for

- **Quants and finance engineers** building automated model pipelines
- **AI tool builders** adding financial model understanding to agents
- **Analysts** who want programmatic access to their Excel models

## License

MIT
