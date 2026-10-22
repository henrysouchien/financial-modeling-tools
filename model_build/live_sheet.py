"""The live semantic sheet's pure core: its axis value and the analyst-op replay.

A research file has a working document from its first turn: ``api/research/live_document.py``
binds the template on the issuer's own fiscal calendar (``live_axis``) when nothing was
constructed yet, and a later refresh (``api/research/live_sheet_service.py``) constructs over
whatever stages are persisted (``target_build.construct_from_inputs``) and replays the analyst's
ops (``replay_agent_ops``). This root imports nothing from the product (§2.6 rule 2); what reads
the capture store and the working-document store lives beside them.
``docs/design/turn-1-live-semantic-sheet-and-ergonomic-doors-design-2026-10-19.md`` §3.1, §3.2, §3.5.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING, Any, Callable, Collection, Literal, Mapping, Sequence

from model_build import document

if TYPE_CHECKING:
  from schema.models import FinancialModel


@dataclass(frozen=True)
class LiveAxis:
  historical_years: tuple[int, ...]
  forecast_years: tuple[int, ...]
  #: The month name ``target_build._reported_fiscal_year_end`` returns; ``None`` when unknown.
  fiscal_year_end: str | None
  basis: Literal["coverage_profile", "captured_filings", "template_default"]


def axis_from_fiscal_year_ends(
  ends: Sequence[tuple[int, date]], *, template: FinancialModel, forecast_horizon: int = 5,
) -> LiveAxis:
  """The axis the annual filings' ``(fiscal year, report date)`` ends give (``CapturedIndex.fiscal_year_ends``,
  oldest report first): the most recent years the template's own historical capacity holds as
  ``historical``, the next ``forecast_horizon`` years as ``forecast``, the latest report date's month
  as the fiscal year end."""
  capacity = len(_template_years(template)[0])
  years = tuple(dict.fromkeys(year for year, _report_date in ends))[-capacity:]
  return LiveAxis(
    years, tuple(range(years[-1] + 1, years[-1] + 1 + forecast_horizon)), ends[-1][1].strftime("%B"),
    "captured_filings",
  )


def axis_from_template(template: FinancialModel) -> LiveAxis:
  """The template's own ``time_structure`` years: the fiscal calendar is unknown."""
  historical, projection = _template_years(template)
  return LiveAxis(historical, projection, None, "template_default")


def _template_years(template: FinancialModel) -> tuple[tuple[int, ...], tuple[int, ...]]:
  time_structure = template.time_structure
  return (
    tuple(int(year) for year in (time_structure.historical_periods or time_structure.historical_years)),
    tuple(int(year) for year in (time_structure.projection_periods or time_structure.projection_years)),
  )


def _ref_index(value: Any) -> int | None:
  if (
    isinstance(value, Mapping) and set(value) == {"$ref"}
    and isinstance(value["$ref"], int) and not isinstance(value["$ref"], bool)
  ):
    return value["$ref"]
  return None


def _minted_add_line_ids(doc: document.ModelSheet) -> list[str]:
  """Ids ``add_line`` minted on ``doc``, in order. The op log stores the raw op, not the result;
  the agent ``add_item`` edit is the record of the id."""
  return [
    str(edit["op"]["item_id"])
    for edit in doc.edits
    if "agent" in edit and (edit.get("op") or {}).get("type") == "add_item"
    and isinstance((edit.get("op") or {}).get("item_id"), str)
  ]


def _prefix_item_ids(
  doc: document.ModelSheet, applied_ops: Sequence[Mapping[str, Any]] | None, base: int,
) -> list[str | None]:
  """The minted id of each already-applied op, ``None`` when that op was not an ``add_line``.

  A count of agent ``add_item`` edits that does not match the prefix's ``add_line`` ops is not
  guessed from: no id is substituted, and a ``$ref`` into the prefix then fails rather than
  naming another line."""
  if not applied_ops:
    return [None] * base
  minted = _minted_add_line_ids(doc)
  ids: list[str | None] = []
  cursor = 0
  for op in applied_ops:
    if op.get("op") == "add_line":
      ids.append(minted[cursor] if cursor < len(minted) else None)
      cursor += 1
    else:
      ids.append(None)
  if cursor != len(minted):
    return [None] * len(applied_ops)
  return ids


def _rebase_refs(value: Any, base: int, prefix_ids: Sequence[str | None]) -> Any:
  """``{"$ref": n}`` in the log's index space, as ``apply_ops`` reads a suffix: ``n - base`` when
  the add_line is in the suffix, else the id that prefix op minted."""
  target = _ref_index(value)
  if target is not None:
    if target >= base:
      return {"$ref": target - base}
    item_id = prefix_ids[target] if 0 <= target < len(prefix_ids) else None
    return item_id if isinstance(item_id, str) else {"$ref": target}
  if isinstance(value, Mapping):
    return {key: _rebase_refs(item, base, prefix_ids) for key, item in value.items()}
  if isinstance(value, list):
    return [_rebase_refs(item, base, prefix_ids) for item in value]
  return value


def _move_refs(value: Any, frm: int, to: int) -> Any:
  """``value`` with each ``{"$ref": n}`` at or past ``frm`` (a new op's add_line) moved to the log
  index space whose first new op is at ``to``."""
  target = _ref_index(value)
  if target is not None:
    return {"$ref": target - frm + to} if target >= frm else value
  if isinstance(value, Mapping):
    return {key: _move_refs(item, frm, to) for key, item in value.items()}
  if isinstance(value, list):
    return [_move_refs(item, frm, to) for item in value]
  return value

def offset_op_refs(value: Any, base: int) -> Any:
  """``value`` with each batch-local ``{"$ref": n}`` moved by ``base`` into the whole-log index space."""
  target = _ref_index(value)
  if target is not None:
    return {"$ref": target + base}
  if isinstance(value, Mapping):
    return {key: offset_op_refs(item, base) for key, item in value.items()}
  if isinstance(value, list):
    return [offset_op_refs(item, base) for item in value]
  return value



def apply_new_ops(
  doc: document.ModelSheet, new_ops: Sequence[Mapping[str, Any]], *, model_spec: Any,
  ref_base: int, applied_ops: Sequence[Mapping[str, Any]],
) -> tuple[document.ModelSheet, list[dict[str, Any]], list[dict[str, Any]]]:
  """The analyst's new batch applied onto ``doc`` as one ``apply_ops`` batch: the sheet, the ops as
  the log keeps them, and ``apply_ops``' typed result per op.

  ``new_ops`` carry batch-local ``{"$ref": k}`` (``k`` names the k-th op of ``new_ops``; an
  already-existing line is named by its ``item_id`` string). Each batch-local ref is first offset
  by ``ref_base`` (the stored log's length) into the whole-log index space, so the stored log
  keeps whole-log ``$ref`` indices and ``replay_agent_ops`` is unchanged. ``applied_ops`` are the
  ops already on ``doc``. A new op the sheet refuses raises ``OpUsage`` / ``EditRefused`` with
  its index in ``new_ops``, never a superseded finding: the analyst is told, and ``doc`` is
  unchanged. The ops are returned at the log index ``len(applied_ops)`` they now follow, which is
  ``ref_base`` unless a refresh dropped a stored op."""
  original = [offset_op_refs(dict(op), ref_base) for op in new_ops]
  prefix_ids = _prefix_item_ids(doc, applied_ops, ref_base)
  trial = document.fork(doc)
  trial.model_spec = model_spec
  rebased = [_rebase_refs(op, ref_base, prefix_ids) for op in original]
  results = document.apply_ops(trial, rebased)
  return trial, [_move_refs(op, ref_base, len(applied_ops)) for op in original], results


def replay_agent_ops(
  doc: document.ModelSheet, prior_ops: Sequence[Mapping[str, Any]], *, model_spec: Any,
) -> tuple[document.ModelSheet, list[dict[str, Any]], tuple[dict[str, Any], ...]]:
  """The analyst's stored op log applied onto ``doc`` (a reconstruction replays the whole log).

  Reopened construction packages do not carry their ModelSpec. Replay binds the selected
  spec before applying edits so dependent scenario snapshots refresh. An op the refreshed inputs
  no longer admit ends the replay: the ops before it stay, the rest are dropped (their later ops
  may name its result), and each dropped op is an ``agent_op_superseded`` finding (the third
  element), never dropped silently. A batch the analyst just sent is ``apply_new_ops``: refused,
  never superseded."""
  if not prior_ops:
    return doc, [], ()
  original = [dict(op) for op in prior_ops]
  trial = document.fork(doc)
  trial.model_spec = model_spec
  try:
    document.apply_ops(trial, original, stored=True)
    return trial, original, ()
  except (document.OpUsage, document.EditRefused) as exc:
    failed = int(exc.op_index or 0)
    kept = original[:failed]
    findings = tuple(
      {
        "code": "agent_op_superseded",
        "op_index": index,
        "item_id": op.get("item_id") if isinstance(op.get("item_id"), str) else None,
        "message": (
          str(exc) if index == failed
          else f"dropped with op {failed}, which the refreshed inputs no longer admit: {exc}"
        ),
      }
      for index, op in enumerate(original) if index >= failed
    )
  trial = document.fork(doc)
  trial.model_spec = model_spec
  if failed:
    document.apply_ops(trial, kept, stored=True)
  return trial, kept, findings


class _StageWriters(Mapping[str, Callable[..., Any]]):
  """The four stage writers, iterated in cold construction order. Resolved on access so
  this module and ``target_build`` do not import one another at load."""

  _names = (
    "historical_foundation", "valuation_policy", "scenario_set", "forecast_assumption_set",
  )

  def __getitem__(self, stage: str) -> Callable[..., Any]:
    from model_build import target_build
    return {
      "historical_foundation": target_build.write_seed_cells,
      "forecast_assumption_set": target_build.write_forecast_cells,
      "scenario_set": target_build.write_scenario_cells,
      "valuation_policy": target_build.write_valuation_cells,
    }[stage]

  def __iter__(self) -> Any:
    return iter(self._names)

  def __len__(self) -> int:
    return len(self._names)


#: Stage → the writer that owns its cells, in cold construction order.
STAGE_WRITERS: Mapping[str, Callable[..., Any]] = _StageWriters()


def writer_footprint(doc: document.ModelSheet, edits: Sequence[Mapping[str, Any]]) -> frozenset[tuple[str, int]]:
  """The cells named by a writer's appended edits, including ``construction.result.cells``.
  A fixed-cell line's cell is its anchor."""
  cells: set[tuple[str, int]] = set()
  indexed = doc.model._index
  for edit in edits:
    op = edit.get("op") or {}
    kind = op.get("type")
    if kind in ("set_value", "clear_input"):
      item_id = str(op["item_id"])
      if kind == "set_value" and "values" in op:
        periods = [int(period) for period in op["values"]]
      elif kind == "set_value":
        periods = [int(period) for period in (edit.get("source") or ())] or [0]
      else:
        periods = [int(op["period"])]
      for period in periods:
        cells.add(document._input_cell(doc, item_id, period) if item_id in indexed else (item_id, period))
    elif kind == "construction":
      for pair in (edit.get("result") or {}).get("cells") or ():
        cells.add((str(pair[0]), int(pair[1])))
  return frozenset(cells)


def restore_footprint(
  doc: document.ModelSheet, footprint: frozenset[tuple[str, int]], scratch_model: FinancialModel,
) -> None:
  """Restore writer cells to the pristine structure, including template formulas/defaults,
  and remove their semantic columns. Nested calls join the caller's restore transaction."""
  if not footprint:
    return
  if not scratch_model._index:
    scratch_model.build_index()
  with document.transaction(doc):
    for item_id, period in sorted(footprint):
      _restore_cell(doc, scratch_model, item_id, int(period))
      doc.semantic.pop((item_id, int(period)), None)


def _restore_cell(doc: document.ModelSheet, scratch_model: FinancialModel, item_id: str, period: int) -> None:
  from schema.models import FormulaType

  if item_id not in doc.model._index:
    return
  live = doc.model.get_item(item_id)
  scratch = scratch_model._index.get(item_id)
  spec = None if scratch is None else document._render_spec_for_period(scratch_model, scratch, period)
  formula = (
    spec is not None and spec.type is not FormulaType.constant and spec.subtype != "not_separately_presented"
  )
  if not formula:
    if live.column is None:
      document.clear_input(doc, live.id, period)
    _clear_writer_value(live, period, scratch)
    _restore_scratch_input(live, scratch, period)
    _sync_formula_period(live, scratch, period)
    return
  _clear_writer_value(live, period, scratch)
  _sync_formula_period(live, scratch, period)
  if scratch is not None:
    historical, projected = _template_years(scratch_model)
    side = "historical" if period in historical or (period not in projected and scratch.projected is None) else "projected"
    pristine_spec = getattr(scratch, side)
    if getattr(live, side) != pristine_spec:
      setattr(live, side, None if pristine_spec is None else pristine_spec.model_copy(deep=True))
  scratch_override = None if scratch is None or not scratch.overrides else scratch.overrides.get(period)
  if scratch_override is not None and scratch_override.type is not FormulaType.constant:
    live.overrides = {**(live.overrides or {}), period: scratch_override.model_copy(deep=True)}
  elif (
    live.overrides
    and period in live.overrides
    and live.overrides[period].type is not FormulaType.constant
    and live.overrides[period].subtype != "not_separately_presented"
  ):
    live.overrides.pop(period)
    if not live.overrides:
      live.overrides = None if scratch is None or scratch.overrides is None else {}

def _clear_writer_value(item: Any, period: int, scratch: Any = None) -> None:
  """Restore the writer's constant and stored value in place; drop keys scratch never held."""
  from schema.models import FormulaType, ValueSeries

  scratch_override = None if scratch is None or not getattr(scratch, "overrides", None) else scratch.overrides.get(int(period))
  if scratch_override is not None and scratch_override.type is FormulaType.constant:
    item.overrides = dict(item.overrides or {})
    item.overrides[int(period)] = scratch_override.model_copy(deep=True)
  elif item.overrides and int(period) in item.overrides and item.overrides[int(period)].type is FormulaType.constant:
    item.overrides.pop(int(period))
  if item.overrides is not None and not item.overrides:
    item.overrides = None if scratch is None or scratch.overrides is None else {}
  scratch_value = None if scratch is None or getattr(scratch, "values", None) is None else scratch.values.values.get(int(period))
  if scratch_value is not None:
    if item.values is None:
      item.values = ValueSeries()
    item.values.values[int(period)] = scratch_value.model_copy(deep=True)
  elif item.values is not None and int(period) in item.values.values:
    item.values.values.pop(int(period))
  if item.values is not None and not item.values.values:
    item.values = (
      None if scratch is None or scratch.values is None
      else scratch.values.model_copy(update={"values": {}})
    )


def _restore_scratch_input(live: Any, scratch: Any, period: int) -> None:
  """A removed writer key returns to the template default, not an artificial blank."""
  if scratch is None:
    return
  override = None if not scratch.overrides else scratch.overrides.get(period)
  if override is not None:
    live.overrides = {**(live.overrides or {}), period: override.model_copy(deep=True)}
  value = None if scratch.values is None else scratch.values.values.get(period)
  if value is None:
    return
  if live.values is None:
    from schema.models import ValueSeries
    live.values = ValueSeries()
  live.values.values[period] = value.model_copy(deep=True)


def _sync_formula_period(live: Any, scratch: Any, period: int) -> None:
  """Restore this cell's formula ownership; a ``None`` period list owns every period."""
  if scratch is None or live.formula_periods is None:
    return
  scratch_owns = scratch.formula_periods is None or period in {int(year) for year in scratch.formula_periods}
  owned = {int(year) for year in live.formula_periods}
  if scratch_owns and period not in owned:
    live.formula_periods = sorted(owned | {period})
  elif not scratch_owns and period in owned:
    live.formula_periods = sorted(year for year in owned if year != period)


def period_order(doc: document.ModelSheet) -> dict[str, tuple[tuple[int, ...], tuple[int, ...]]]:
  """The insertion order in which each line's overrides and stored values serialize."""
  return {
    item.id: (tuple(item.overrides or ()), tuple(item.values.values if item.values is not None else ()))
    for item in doc.model._index.values()
  }


def keep_period_order(
  doc: document.ModelSheet, before: Mapping[str, tuple[tuple[int, ...], tuple[int, ...]]],
  pristine: Mapping[str, tuple[tuple[int, ...], tuple[int, ...]]] | None = None,
  written: Collection[tuple[str, int]] = (),
) -> None:
  """Keep surviving period keys in cold insertion order; new keys follow them. With ``pristine`` (the
  structure's ``period_order``) and ``written`` (the cells C wrote) a new key takes the place cold gives it:
  C writes over the structure's own slot for the period, else before every key a later writer added, in
  period order. A cell a refresh restored and wrote again returns to its place."""
  by_item: dict[str, set[int]] = {}
  for item_id, period in written:
    by_item.setdefault(item_id, set()).add(period)
  for item in doc.model._index.values():
    was = before.get(item.id)
    if was is None:
      continue
    slots = (None, None) if pristine is None or item.id not in pristine else pristine[item.id]
    mine = by_item.get(item.id, set())
    if item.overrides:
      item.overrides = _in_order(item.overrides, was[0], slots[0], mine)
    if item.values is not None and item.values.values:
      item.values.values = _in_order(item.values.values, was[1], slots[1], mine)


def _in_order(
  held: Mapping[int, Any], was: tuple[int, ...], slots: tuple[int, ...] | None = None, written: Collection[int] = (),
) -> Any:
  stood = set(was)
  order = [period for period in was if period in held]
  rank = {} if slots is None else {period: index for index, period in enumerate(slots)}

  def place(period: int) -> tuple[int, int]:
    # the structure's slot, then C's writes by period, then what a later writer added
    return (0, rank[period]) if period in rank else (1, period) if period in written else (2, 0)

  for period in (period for period in held if period not in stood):
    if place(period)[0] == 2:
      order.append(period)
      continue
    after = next((index for index, kept in enumerate(order) if place(kept) > place(period)), len(order))
    order.insert(after, period)
  return held if order == list(held) else {period: held[period] for period in order}
