"""Financial-model bench keyed dumps: requirement state, value state, source lookup.

Lane A/C output of the bench (plan §3, ``keyed_dump``). Each dump is a list of
flat JSON rows keyed so two runs diff row-by-row:

- ``dump_requirements(bundle)``: one row per (required_role, period_id,
  accession) of a ``SourceBundle``; a role whose ``pinned_columns`` do not
  cover a period still gets that period's row, with ``pinned_ref=None``.
- ``dump_values(model, typed)``: one row per (item_id, period) of the typed
  ``DependencyGraph`` matrix the compile pass computes.
- ``lookup(document, model_spec, item_id, period)``: ``schema.model_queries.trace``
  of one published value, its ``filing-line:`` observation ids parsed into the
  same fields ``pinned_ref`` carries.

The SourceBundle carries neither a KPI citation nor a capture id on an
``AnnualSourceRequirement`` (both live on ``statement_bindings.v2`` rows), so
those two row fields are ``None`` here. Contract:
docs/design/financial-model-bench-implementation-plan-2026-10-05.md.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
import json
from pathlib import Path
import re
from typing import TYPE_CHECKING, Any

from schema.dependency_graph import DependencyGraph
from schema.model_queries import trace

if TYPE_CHECKING:
  from schema.build_model_product_contracts import ModelSpec, SourceBundle
  from schema.model_semantic_document import ModelSemanticDocument
  from schema.models import FinancialModel


#: ``schema/build_provenance.py`` writes a bound statement observation as
#: ``filing-line:{accession}:{period}:{statement_role}:{tag}:{axis=member;...}``.
_FILING_LINE_PREFIX = "filing-line:"
_FISCAL_YEAR_LABEL = re.compile(r"^FY(\d{4})$")


def compact_json(payload: Any) -> bytes:
  """``payload`` as one compact UTF-8 JSON line body (no spaces, non-ASCII kept)."""
  return json.dumps(payload, separators=(",", ":"), ensure_ascii=False, default=str).encode()


def filing_line_json(line: Any) -> dict[str, Any]:
  """A ``FilingLineRef`` as the flat fields a keyed row and a ``filing_line`` source carry."""
  return {
    "accession": line.accession,
    "period": line.period.canonical_label(),
    "statement_role": line.statement_role,
    "tag": line.tag,
    "dimensions": [[d.axis_qname, d.member_qname] for d in line.dimensions],
  }


def dump_requirements(bundle: SourceBundle) -> list[dict[str, Any]]:
  """One row per (required_role, period_id, pinned accession or None)."""

  coverage = {
    (row.requirement_id, period.period_id): period
    for row in bundle.coverage
    for period in row.periods
  }
  unbound = {row.period_id: row.accession for row in bundle.unbound_periods}
  rows: list[dict[str, Any]] = []
  for requirement in bundle.requirements:
    for period_id in requirement.period_ids:
      cell = requirement.cell_for_period(period_id)
      line = cell.pinned_ref
      period_coverage = coverage.get((requirement.requirement_id, period_id))
      rows.append({
        "required_role": requirement.required_role,
        "period_id": period_id,
        "accession": None if line is None else line.accession,
        "requirement_id": requirement.requirement_id,
        "family": requirement.family,
        "model_item": requirement.model_item,
        "binding_state": cell.binding_state,
        "pinned_ref": None if line is None else filing_line_json(line),
        "kpi_citation": None,
        "capture_id": None,
        "unbound_reason": cell.unbound_reason,
        "absence_citation": (
          None
          if cell.absence_citation is None
          else cell.absence_citation.model_dump(mode="json")
        ),
        "included_in": cell.included_in,
        "disposition": None if period_coverage is None else period_coverage.disposition,
        "evidence_ids": [] if period_coverage is None else list(period_coverage.evidence_ids),
        "in_unbound_periods": period_id in unbound,
        "unbound_accession": unbound.get(period_id),
      })
  return rows


def dump_values(
  model: FinancialModel,
  typed: Mapping[Any, Any] | None = None,
) -> list[dict[str, Any]]:
  """One row per (item_id, period) of the typed matrix; computed when ``typed`` is None."""

  if not model._index:
    model.build_index()
  graph = DependencyGraph()
  graph.build(model)
  matrix = graph.compute_typed({}) if typed is None else typed
  rows: list[dict[str, Any]] = []
  for item_id in sorted(matrix, key=str):
    item = model._index.get(str(item_id))
    for period in sorted(matrix[item_id], key=int):
      value = matrix[item_id][period]
      state = _field(value, "state")
      spec = None if item is None else graph._static_spec_for_period(item, int(period))
      cell = None
      if item is not None and item.values is not None:
        cell = item.values.values.get(int(period))
      provenance = _field(value, "provenance")
      rows.append({
        "item_id": str(item_id),
        "period": int(period),
        "state": state,
        "value": _field(value, "value") if state == "known" else None,
        "reason": None if state == "known" else _field(value, "primary_reason"),
        "origin": _field(value, "origin"),
        "provenance": getattr(provenance, "value", provenance),
        "source_key": _observation_ids(value),
        "formula_type": None if spec is None else spec.type.value,
        "formula_subtype": None if spec is None else spec.subtype,
        "cell_provenance": None if cell is None else cell.provenance.value,
        "edgar_tag": (
          None
          if cell is None or cell.edgar_provenance is None
          else cell.edgar_provenance.metric_tag
        ),
      })
  return rows


def lookup(
  document: ModelSemanticDocument,
  model_spec: ModelSpec,
  item_id: str,
  period: int,
) -> dict[str, Any]:
  """The trace of one published value; filing-line origins parsed into ``pinned_ref`` fields."""

  traced = trace(document, model_spec, item_id=item_id, period=period)
  if traced is None:
    return {
      "item_id": item_id,
      "period": period,
      "rendered": False,
      "lineage_status": None,
      "cells": [],
      "origins": [],
      "assumption_ids": [],
      "assumption_refs": [],
    }
  root = _root_lineage(document, item_id, period)
  return {
    "item_id": traced.item_id,
    "period": traced.period,
    "rendered": True,
    "lineage_status": traced.lineage_status,
    "cells": [cell.model_dump(mode="json") for cell in traced.cells],
    "origins": [
      {**origin.model_dump(mode="json"), "filing_line": parse_filing_line_id(origin.ref)}
      for origin in traced.origins
    ],
    "assumption_ids": [] if root is None else list(root.assumption_ids),
    "assumption_refs": (
      [] if root is None else [ref.model_dump(mode="json") for ref in root.assumption_refs]
    ),
  }


def parse_filing_line_id(observation_id: str) -> dict[str, Any] | None:
  """``filing-line:`` observation id -> the ``pinned_ref`` fields; ``None`` for any other id.

  Accession, period label and normalized role carry no ``:``; the tag and
  each dimension axis are prefixed QNames, so the dimension list starts at
  the ``:`` before the first ``axis=`` QName.
  """

  if not observation_id.startswith(_FILING_LINE_PREFIX):
    return None
  parts = observation_id[len(_FILING_LINE_PREFIX):].split(":", 3)
  if len(parts) != 4:
    return None
  accession, period, statement_role, rest = parts
  if ":" not in rest:
    return None
  equals = rest.find("=")
  if equals < 0:
    tag, _, dimension_text = rest.rpartition(":")
  else:
    axis_colon = rest.rfind(":", 0, equals)
    if axis_colon < 0:
      return None
    split = rest.rfind(":", 0, axis_colon)
    if split < 0:
      return None
    tag, dimension_text = rest[:split], rest[split + 1:]
  fiscal_year = _FISCAL_YEAR_LABEL.match(period)
  tag_prefix, _, tag_local = tag.partition(":")
  if not accession or fiscal_year is None or not statement_role or not tag_prefix or not tag_local:
    return None
  dimensions: list[list[str]] = []
  for entry in dimension_text.split(";"):
    if not entry:
      continue
    if "=" not in entry:
      return None
    axis_qname, member_qname = entry.split("=", 1)
    if ":" not in axis_qname or ":" not in member_qname:
      return None
    dimensions.append([axis_qname, member_qname])
  return {
    "accession": accession,
    "period": period,
    "fiscal_year": int(fiscal_year.group(1)),
    "statement_role": statement_role,
    "tag": tag,
    "dimensions": dimensions,
  }


def write_jsonl(path: Path, rows: Iterable[Mapping[str, Any]]) -> int:
  """Write ``rows`` one compact JSON object per line; returns the row count."""

  path.parent.mkdir(parents=True, exist_ok=True)
  count = 0
  with path.open("wb") as handle:
    for row in rows:
      handle.write(compact_json(dict(row)) + b"\n")
      count += 1
  return count


def diff(
  a: Iterable[Mapping[str, Any]],
  b: Iterable[Mapping[str, Any]],
  *,
  key: Sequence[str],
) -> dict[str, list[dict[str, Any]]]:
  """Rows only in ``a``, only in ``b``, and changed, joined on the ``key`` columns."""

  left = _index_rows(a, key)
  right = _index_rows(b, key)
  changed: list[dict[str, Any]] = []
  for identity, row_a in left.items():
    row_b = right.get(identity)
    if row_b is None or _canonical(row_a) == _canonical(row_b):
      continue
    fields = sorted(
      name
      for name in set(row_a) | set(row_b)
      if _canonical(row_a.get(name)) != _canonical(row_b.get(name))
    )
    changed.append({
      "key": {name: row_a.get(name) for name in key},
      "fields": fields,
      "a": dict(row_a),
      "b": dict(row_b),
    })
  return {
    "only_a": [dict(row) for identity, row in left.items() if identity not in right],
    "only_b": [dict(row) for identity, row in right.items() if identity not in left],
    "changed": changed,
  }


def _field(value: Any, name: str) -> Any:
  if isinstance(value, Mapping):
    return value.get(name)
  return getattr(value, name, None)


def _observation_ids(value: Any) -> list[str] | None:
  bindings = _field(value, "origin_bindings") or ()
  ids = [
    _field(binding, "observation_id")
    for binding in bindings
    if _field(binding, "binding_kind") == "observed"
  ]
  return ids or None


def _root_lineage(document: ModelSemanticDocument, item_id: str, period: int) -> Any:
  snapshot = next(
    (
      row
      for row in document.semantic_values
      if row.semantic_item_id == item_id and row.period == period
    ),
    None,
  )
  if snapshot is None:
    return None
  return next(
    (
      cell
      for cell in document.provenance.cells
      if (cell.sheet, cell.cell) == (snapshot.sheet, snapshot.cell)
    ),
    None,
  )


def _canonical(value: Any) -> str:
  return json.dumps(value, sort_keys=True, default=str)


def _index_rows(
  rows: Iterable[Mapping[str, Any]],
  key: Sequence[str],
) -> dict[str, Mapping[str, Any]]:
  indexed: dict[str, Mapping[str, Any]] = {}
  for row in rows:
    identity = _canonical([row.get(name) for name in key])
    if identity in indexed:
      raise ValueError(f"diff key {list(key)} is not unique: {identity}")
    indexed[identity] = row
  return indexed
