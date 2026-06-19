from __future__ import annotations

from collections import defaultdict
from typing import Any

from schema.source_values import normalize_edgar_source_value


RECONCILE_CANDIDATE_CONCEPTS = {
  ("is_subtotal_integrity", "gross_profit"): [
    "revenue",
    "cost_of_revenue",
  ],
  ("is_subtotal_integrity", "operating_income"): [
    "revenue",
    "cost_of_revenue",
    "operating_expenses",
    "selling_general_and_administrative",
    "sales_and_marketing",
    "general_and_administrative",
    "research_and_development",
  ],
  ("bs_balance", "balance_sheet"): [
    "total_assets",
    "total_liabilities",
    "total_equity",
  ],
  ("cf_reconciliation", "net_change"): [
    "operating_cash_flow",
    "investing_cash_flow",
    "financing_cash_flow",
  ],
}


def normalize_metric_entries(entries: list[dict], *, year: int) -> list[dict]:
  normalized = []
  for entry in entries:
    tag = entry.get("tag")
    if not isinstance(tag, str) or not tag.strip() or "current_value" not in entry:
      raise ValueError(
        f"list_metrics entry for {year} must include tag and current_value"
      )
    normalized.append(dict(entry))
  return normalized


def metric_value_for_concept(entry: dict, concept_id: str, year: int) -> float | None:
  value = entry.get("current_value")
  if value is None:
    return None
  if "scale" in entry:
    source_value = normalize_edgar_source_value(
      value,
      entry.get("scale"),
      concept_id,
      source="edgar",
      tag=entry.get("tag"),
      year=year,
      source_ref=entry.get("source_ref"),
    )
    return source_value.normalized_value
  return float(value)


def metrics_by_tag_for_concept(
  entries: list[dict],
  concept_id: str,
  year: int,
  metric_value_for_concept_fn=None,
) -> dict[str, list[float]]:
  metric_value_for_concept_fn = (
    metric_value_for_concept
    if metric_value_for_concept_fn is None
    else metric_value_for_concept_fn
  )
  by_tag: dict[str, list[float]] = defaultdict(list)
  for entry in entries:
    tag = str(entry.get("tag") or "").strip()
    value = metric_value_for_concept_fn(entry, concept_id, year)
    if tag and value is not None:
      by_tag[tag].append(float(value))
  return dict(by_tag)


def relative_distance(value: float, target: float, floor_abs_m: float) -> float:
  return abs(float(value) - float(target)) / max(abs(float(target)), float(floor_abs_m))


def best_value_for_target(
  values: list[float],
  target: float,
  floor_abs_m: float,
  relative_distance_fn=None,
) -> tuple[float, float]:
  relative_distance_fn = relative_distance if relative_distance_fn is None else relative_distance_fn
  best_value = min(
    [float(value) for value in values],
    key=lambda value: relative_distance_fn(value, target, floor_abs_m),
  )
  return best_value, relative_distance_fn(best_value, target, floor_abs_m)


def current_values_from_diagnostic_inputs(
  entries: list[dict[str, Any]],
  concept_id: str,
  years: list[int],
) -> tuple[dict[int, float], list[int]]:
  entry_by_year = {int(entry["year"]): entry["entry"] for entry in entries}
  values: dict[int, float] = {}
  missing_years: list[int] = []
  for year in years:
    inputs = entry_by_year[int(year)].get("inputs") or {}
    if concept_id not in inputs:
      missing_years.append(int(year))
    else:
      values[int(year)] = float(inputs[concept_id])
  return values, missing_years


def delta_by_year(entries: list[dict[str, Any]]) -> dict[str, float]:
  result: dict[str, float] = {}
  for entry in sorted(entries, key=lambda item: int(item["year"])):
    delta = entry["entry"].get("delta")
    if delta is not None:
      result[str(int(entry["year"]))] = float(delta)
  return result


def target_value_for_candidate(
  diagnostic_type: str,
  candidate_concept: str,
  current_value: float,
  delta: float,
) -> float:
  if diagnostic_type == "is_subtotal_integrity":
    if candidate_concept == "revenue":
      return float(current_value) + float(delta)
    return float(current_value) - float(delta)
  if diagnostic_type == "bs_balance":
    if candidate_concept == "total_assets":
      return float(current_value) - float(delta)
    return float(current_value) + float(delta)
  if diagnostic_type == "cf_reconciliation":
    return float(current_value) - float(delta)
  raise ValueError(f"unsupported diagnostic_type: {diagnostic_type}")


def evaluate_reconcile_candidates(
  *,
  diagnostic_type: str,
  subtotal_name: str,
  entries: list[dict[str, Any]],
  tags_by_year: dict[int, list[dict]],
  match_tolerance_pct: float,
  match_floor_abs_m: float,
  candidate_concepts_by_key=None,
  delta_by_year_fn=None,
  current_values_from_diagnostic_inputs_fn=None,
  target_value_for_candidate_fn=None,
  metrics_by_tag_for_concept_fn=None,
  best_value_for_target_fn=None,
) -> dict:
  candidate_concepts_by_key = (
    RECONCILE_CANDIDATE_CONCEPTS
    if candidate_concepts_by_key is None
    else candidate_concepts_by_key
  )
  delta_by_year_fn = delta_by_year if delta_by_year_fn is None else delta_by_year_fn
  current_values_from_diagnostic_inputs_fn = (
    current_values_from_diagnostic_inputs
    if current_values_from_diagnostic_inputs_fn is None
    else current_values_from_diagnostic_inputs_fn
  )
  target_value_for_candidate_fn = (
    target_value_for_candidate
    if target_value_for_candidate_fn is None
    else target_value_for_candidate_fn
  )
  metrics_by_tag_for_concept_fn = (
    metrics_by_tag_for_concept
    if metrics_by_tag_for_concept_fn is None
    else metrics_by_tag_for_concept_fn
  )
  best_value_for_target_fn = (
    best_value_for_target
    if best_value_for_target_fn is None
    else best_value_for_target_fn
  )

  candidate_concepts = candidate_concepts_by_key[(diagnostic_type, subtotal_name)]
  years = sorted(int(entry["year"]) for entry in entries)
  delta_values = {
    int(year): delta
    for year, delta in (
      (int(key), value) for key, value in delta_by_year_fn(entries).items()
    )
  }
  searched_tags = {
    str(metric.get("tag"))
    for metrics in tags_by_year.values()
    for metric in metrics
    if isinstance(metric.get("tag"), str)
  }

  matches: list[dict[str, Any]] = []
  partials: list[dict[str, Any]] = []
  skip_reasons: list[dict[str, Any]] = []
  nearest_by_year: dict[str, list[dict[str, Any]]] = {str(year): [] for year in years}

  for concept_id in candidate_concepts:
    current_by_year, missing_years = current_values_from_diagnostic_inputs_fn(
      entries,
      concept_id,
      years,
    )
    if missing_years:
      skip_reasons.append(
        {
          "candidate_concept": concept_id,
          "reason": "concept_not_in_diagnostic_inputs",
          "years": [str(year) for year in missing_years],
        }
      )
      continue

    target_by_year = {
      year: target_value_for_candidate_fn(
        diagnostic_type,
        concept_id,
        current_by_year[year],
        float(delta_values[year]),
      )
      for year in years
    }
    zero_target_years = [
      year for year, target in target_by_year.items()
      if abs(float(target)) < float(match_floor_abs_m)
    ]
    if zero_target_years:
      skip_reasons.append(
        {
          "candidate_concept": concept_id,
          "reason": "zero_target_guard",
          "years": [str(year) for year in zero_target_years],
        }
      )
      continue

    normalized_by_year = {
      year: metrics_by_tag_for_concept_fn(tags_by_year[year], concept_id, year)
      for year in years
    }
    concept_tags = sorted(
      {
        tag
        for metrics_by_tag in normalized_by_year.values()
        for tag in metrics_by_tag
      }
    )

    for year in years:
      target = target_by_year[year]
      nearest = []
      for tag, values in normalized_by_year[year].items():
        value, distance = best_value_for_target_fn(values, target, match_floor_abs_m)
        nearest.append(
          {
            "candidate_concept": concept_id,
            "tag": tag,
            "value": value,
            "target": target,
            "distance": distance,
          }
        )
      nearest_by_year[str(year)].extend(nearest)

    for tag in concept_tags:
      value_by_year: dict[int, float] = {}
      distance_by_year: dict[int, float] = {}
      matched_years: list[int] = []
      for year in years:
        values = normalized_by_year[year].get(tag)
        if not values:
          continue
        value, distance = best_value_for_target_fn(
          values,
          target_by_year[year],
          match_floor_abs_m,
        )
        value_by_year[year] = value
        distance_by_year[year] = distance
        if distance <= float(match_tolerance_pct):
          matched_years.append(year)

      if len(matched_years) == len(years):
        matches.append(
          {
            "candidate_concept": concept_id,
            "tag": tag,
            "value_by_year": value_by_year,
            "distance_max_across_years": max(distance_by_year.values()),
          }
        )
      elif matched_years:
        partials.append(
          {
            "candidate_concept": concept_id,
            "tag": tag,
            "candidate_per_year": {
              str(year): tag if year in matched_years else None
              for year in years
            },
            "matched_year_count": len(matched_years),
          }
        )

  top_5_nearest_by_year = {
    year: sorted(rows, key=lambda row: row["distance"])[:5]
    for year, rows in nearest_by_year.items()
  }
  partials.sort(
    key=lambda item: (
      -int(item["matched_year_count"]),
      str(item["candidate_concept"]),
      str(item["tag"]),
    )
  )
  matches.sort(
    key=lambda item: (
      str(item["candidate_concept"]),
      str(item["tag"]),
    )
  )
  return {
    "matches": matches,
    "partials": partials,
    "searched_tag_count": len(searched_tags),
    "top_5_nearest_by_year": top_5_nearest_by_year,
    "skip_reasons": skip_reasons,
  }


__all__ = [
  "RECONCILE_CANDIDATE_CONCEPTS",
  "best_value_for_target",
  "current_values_from_diagnostic_inputs",
  "delta_by_year",
  "evaluate_reconcile_candidates",
  "metric_value_for_concept",
  "metrics_by_tag_for_concept",
  "normalize_metric_entries",
  "relative_distance",
  "target_value_for_candidate",
]
