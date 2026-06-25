"""Shadow-mode diff logging for upstream registry comparisons."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict

from .models import EdgarProvenance


SHADOW_LOG_PATH = Path(__file__).resolve().parents[1] / "api" / "logs" / "equivalence_shadow.jsonl"


def log_shadow_diffs(
    *,
    concept_id: str,
    ticker: str,
    legacy_values: Dict[int, float],
    legacy_provenance: Dict[int, EdgarProvenance],
    upstream_values: Dict[int, float],
    upstream_provenance: Dict[int, EdgarProvenance],
    log_path: Path | None = None,
) -> int:
    """Append one JSONL record per year where legacy and upstream values differ."""

    path = log_path or SHADOW_LOG_PATH
    years = sorted(set(legacy_values) | set(upstream_values))
    rows: list[str] = []
    timestamp = datetime.now(timezone.utc).isoformat()

    for year in years:
        legacy_value = legacy_values.get(year)
        upstream_value = upstream_values.get(year)
        if legacy_value == upstream_value:
            continue

        legacy_source = None
        if year in legacy_provenance:
            legacy_source = legacy_provenance[year].metric_tag

        upstream_metric_tag = None
        registry_revision = None
        if year in upstream_provenance:
            upstream_metric_tag = upstream_provenance[year].metric_tag
            registry_revision = upstream_provenance[year].registry_revision

        rows.append(
            json.dumps(
                {
                    "concept_id": concept_id,
                    "ticker": ticker,
                    "year": int(year),
                    "legacy_value": legacy_value,
                    "upstream_value": upstream_value,
                    "legacy_source": legacy_source,
                    "upstream_metric_tag": upstream_metric_tag,
                    "registry_revision": registry_revision,
                    "timestamp": timestamp,
                },
                sort_keys=True,
            )
        )

    if not rows:
        return 0

    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(rows))
        handle.write("\n")
    return len(rows)
