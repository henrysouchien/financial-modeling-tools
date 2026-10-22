"""``python -m model_build <door> --json '{...}'``: argument parsing over this root's door functions.

Plan §2.6 rule 3. Each door reads one JSON object, calls the root's own function and prints its
typed result as one JSON object on stdout. Exit 0 on success; 1 when a build's snapshot verdict is
not ``published``; 2 on a usage error, or for a door whose typed input only an adapter can resolve
today (one line on stderr naming the root function that does not exist).

| door | args | root function |
|---|---|---|
| ``prepare`` | — | none: C and the judgment artifacts (``target_build.StageBuildInputs``) are resolved by an adapter's ``BuildSource`` |
| ``author`` | ``{"path", "ops", "out"}`` | ``document.reopen`` → ``apply_ops`` → ``save`` (all-or-nothing) |
| ``check`` | ``{"path"}`` or ``{"workdir"}`` | ``target_build.target_check``: the document-level findings only; the validation report needs a ``BuildSource`` |
| ``build`` | ``{"path"}`` or ``{"workdir_in"}``, ``"out"``, ``"soffice"``?, ``"accept_gaps"``? | ``target_build.target_snapshot``: with no ``source`` the snapshot is ``incomplete:validation_not_run`` |
| ``report`` | ``{"workdir", "item_id"?, "period"?}`` | ``target_build.target_report`` (and ``target_lookup`` for one cell) |
| ``refresh`` | — | none: a refresh re-runs C, which only an adapter's ``BuildSource`` resolves |
| ``explain`` | ``{"a", "b", "item_id", "period"}`` | ``target_build.target_explain`` |

A build from C (``target_build.target_build``) and a refresh take a ``BuildSource``; the bench's is
``scripts/research/bench/context.py`` ``StoreBuildSource`` (``scripts/research/bench.py target_build``,
``target_refresh``).
"""

from __future__ import annotations

import argparse
from collections.abc import Callable, Mapping, Sequence
import json
from pathlib import Path
import sys
from typing import Any


class _Usage(ValueError):
  """A missing or malformed argument: exit 2."""


class _NoRootFunction(RuntimeError):
  """A door this root cannot answer from primitives today: exit 2, one line on stderr."""


def _require(args: Mapping[str, Any], key: str) -> Any:
  if args.get(key) is None:
    raise _Usage(f"{key} is required")
  return args[key]


def _path(args: Mapping[str, Any], key: str) -> Path:
  return Path(str(_require(args, key))).expanduser()


def _prepare(args: Mapping[str, Any]) -> tuple[int, Any]:
  raise _NoRootFunction(
    "prepare: no root function reads model_build.target_build.StageBuildInputs (C rows, corpus, coverage profile, "
    "BusinessModel revision, judgment artifacts) from a path; an adapter's BuildSource.load_c_inputs resolves "
    "them (scripts/research/bench/context.py StoreBuildSource)"
  )


def _author(args: Mapping[str, Any]) -> tuple[int, Any]:
  from model_build import document

  ops = _require(args, "ops")
  if not isinstance(ops, list):
    raise _Usage("ops must be a list of {op: add_line|set_formula|set_input|clear_input|remove_line|advance_period, ...}")
  doc = document.reopen(_path(args, "path"))
  before = document.recalc(doc)
  trial = document.fork(doc)
  # A refusal raises before anything is saved: the batch is all-or-nothing.
  results = document.apply_ops(trial, ops)
  after = document.recalc(trial)
  saved = document.save(trial, _path(args, "out"))
  diff = document.value_diff(before, after)
  return 0, {
    "source": doc.source,
    "path": str(saved),
    "results": results,
    "edits": trial.edits[len(doc.edits):],
    "changed_cells": len(diff),
    "changes": diff,
  }


def _check(args: Mapping[str, Any]) -> tuple[int, Any]:
  from model_build import document, target_build

  if args.get("path") is not None:
    path = _path(args, "path")
  else:
    path = target_build._snapshot_dir(_path(args, "workdir")) / target_build.DOCUMENT_NAME
  return 0, {"path": str(path), **target_build.target_check(document.reopen(path))}


def _build(args: Mapping[str, Any]) -> tuple[int, Any]:
  from model_build import target_build

  if args.get("path") is not None:
    package = _path(args, "path")
  elif args.get("workdir_in") is not None:
    package = target_build.package_in(_path(args, "workdir_in"))
  else:
    raise _Usage(
      "path (a document package) or workdir_in is required; a build from C takes an adapter's BuildSource "
      "(model_build.target_build.target_build), not a path"
    )
  gaps = args.get("accept_gaps") or []
  if not isinstance(gaps, list) or not all(isinstance(name, str) for name in gaps):
    raise _Usage("accept_gaps must be a list of construction helper names")
  soffice = args.get("soffice")
  result = target_build.target_snapshot(
    package, out=_path(args, "out"), soffice=None if soffice is None else str(soffice), accept_gaps=gaps,
  )
  return (0 if result["verdict"] == "published" else 1), result


def _report(args: Mapping[str, Any]) -> tuple[int, Any]:
  from model_build import target_build

  workdir = _path(args, "workdir")
  out = target_build.target_report(workdir)
  if args.get("item_id") is not None or args.get("period") is not None:
    out["lookup"] = target_build.target_lookup(workdir, str(_require(args, "item_id")), int(_require(args, "period")))
  return 0, out


def _refresh(args: Mapping[str, Any]) -> tuple[int, Any]:
  raise _NoRootFunction(
    "refresh: no root function reads the C re-run (model_build.target_build.StageBuildInputs) from a path; "
    "target_build.target_refresh takes an adapter's BuildSource (scripts/research/bench/context.py StoreBuildSource)"
  )


def _explain(args: Mapping[str, Any]) -> tuple[int, Any]:
  from model_build import target_build

  return 0, target_build.target_explain(
    _path(args, "a"), _path(args, "b"), str(_require(args, "item_id")), int(_require(args, "period")),
  )


DOORS: dict[str, tuple[Callable[[Mapping[str, Any]], tuple[int, Any]], str]] = {
  "prepare": (_prepare, "C and the judgment artifacts as typed inputs (not answered by the root: an adapter's BuildSource)"),
  "author": (_author, "the agent's edit batch on a document package, saved to out (all-or-nothing)"),
  "check": (_check, "the document-level validators over a document package or snapshot"),
  "build": (_build, "an authored document package snapshotted: recalc, render + native compare, check, verdict"),
  "report": (_report, "a snapshot's verdict, workbook and manifest; one cell's lookup with item_id and period"),
  "refresh": (_refresh, "a snapshot re-run against current C (not answered by the root: an adapter's BuildSource)"),
  "explain": (_explain, "one cell across two snapshots and the changed inputs it reads"),
}


def _parser() -> argparse.ArgumentParser:
  parser = argparse.ArgumentParser(prog="python -m model_build", description=(__doc__ or "").splitlines()[0])
  doors = parser.add_subparsers(dest="door", required=True, metavar="DOOR")
  for name, (_handler, summary) in DOORS.items():
    door = doors.add_parser(name, help=summary, description=summary)
    door.add_argument("--json", dest="json_args", default="{}", help="the door's arguments as one JSON object")
  return parser


def main(argv: Sequence[str] | None = None) -> int:
  options = _parser().parse_args(argv)
  try:
    args = json.loads(options.json_args)
    if not isinstance(args, dict):
      raise _Usage("--json must be one JSON object")
    code, result = DOORS[options.door][0](args)
  except _NoRootFunction as exc:
    print(str(exc), file=sys.stderr)
    return 2
  except (_Usage, json.JSONDecodeError) as exc:
    print(json.dumps({"error": {"code": "usage", "message": f"{options.door}: {exc}"}}))
    return 2
  print(json.dumps(result, default=str))
  return code


if __name__ == "__main__":
  sys.exit(main())
