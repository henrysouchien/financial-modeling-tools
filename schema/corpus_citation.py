"""The ``corpus://`` citation: one owner of its shape.

A corpus citation names the filing artifact a line was read from, in the staged corpus it was read
from: ``corpus://<corpus_id>/<corpus_version>/<logical_id>``. The version is where the line was read
-- a fetch stamp like ``retrieved_at`` -- so evidence identity compares citations with the version
masked: a corpus re-staged for a changed profile carries every unchanged filing line under a new
version, and that is not a change in the evidence.
"""
from __future__ import annotations

import re

_SCHEME = "corpus://"
_VERSION_SEGMENT = re.compile(r"(corpus://[^/\s\"]+/)\d+/")


def corpus_citation(corpus_id: str, corpus_version: int | str, logical_id: str) -> str:
    """The citation of ``logical_id`` in version ``corpus_version`` of ``corpus_id``."""
    return f"{_SCHEME}{corpus_id}/{corpus_version}/{logical_id}"


def mask_corpus_version(text: str) -> str:
    """``text`` with the version segment of every ``corpus://`` citation it contains removed."""
    return _VERSION_SEGMENT.sub(r"\1", text)


__all__ = ["corpus_citation", "mask_corpus_version"]
