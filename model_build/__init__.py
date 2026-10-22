"""The build-model root: where the bench's build side lands, and what may not be imported here.

Owner: ``docs/design/build-model-package-boundary-plan-2026-10-05.md`` (§2.5 membership, §2.6 rules,
§4 queries). This package is *prepared* as a distribution — one import root, no product imports, a
CLI over plain door functions — and is **not** one: no ``pyproject.toml``, no requirements line, no
console-script. It folds into ``financial-model-engine`` after cutover (plan R1).

Rules (plan §2.6, verbatim):

1. One root. ``model_build/`` at the checkout root beside ``schema/``, importable without an
   ``api/`` path entry. Bench doors land here, never in ``api/research/`` (plan R9).
2. No product imports. Nothing from ``api.*``, ``research.*``, ``fms.*``, ``agent.*``,
   ``agent_gateway.*``, ``mcp_servers.*``, ``fastmcp``, ``scripts.*``. What a door needs arrives as
   parameters or Protocols this root declares: a ``sqlite3.Connection`` plus write-transaction
   callable for its own tables; a ``ModelRecordWriter`` (commit record + lineage, CAS pointer) the
   repository implements (plan W1); the typed inputs of plan §2.4 (``VerifiedCorpus``,
   ``StatementFactProjection``, coverage profile, the five judgment artifacts, correspondence rows);
   a user root path; a clock; ``run_id: str``. Never a ``ResearchRepository``.
3. CLI-shaped entry points. Every door is a plain function over primitives and typed values
   returning a typed result; ``python -m model_build <door> …`` is argument parsing over them.
   The same callables serve the gateway-local tool definitions and ``code_execute``.
4. Its own tests under ``tests/model_build/``, runnable without the gateway.

Verification (plan §4), recorded in each wave's dated ``docs/qa/`` note:

- query 4, static half, enforced by ``tests/model_build/test_import_boundary.py``::

    git grep -n -E "(^|\\s)(from|import) (api|research|fms|agent|agent_gateway|mcp_servers|fastmcp|scripts)(\\.|\\b)" -- model_build/

  → empty.
- query 5: ``schema/`` and ``packages/financial-modeling-tools-compat/`` never import this root.
- the isolation run (plan §4 query 4, second half) once ``__main__`` exists.
"""
