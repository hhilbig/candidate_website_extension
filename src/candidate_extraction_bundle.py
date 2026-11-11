"""Corpus-specific bound values of the candidate extraction pipeline.

The pipeline scripts compare their inputs with literal values (hashes and
counts) of the 2002-2024 run of September 2026. Those literals stay in the
scripts as the default. A second corpus (the 2026 collection) has its own
values. The environment variable CANDIDATE_EXTRACTION_BUNDLE names a JSON file
with the values of that corpus:

  {"bundle_id": "2026", "values": {"source_corpus_sha256": "...", ...}}

`bound(name, default)` returns the bundle value, or the default if no bundle is
active. An active bundle must hold each name that a script asks for. A missing
name stops the script; the September value never fills a gap of another corpus.
A value that is not known yet is `null`; each comparison with it then fails.
Only names in BOUND_NAMES are legal, so a typing error in a bundle file fails.
"""
from __future__ import annotations

import json
import os
from functools import lru_cache
from pathlib import Path
from typing import Any

ENVIRONMENT_VARIABLE = "CANDIDATE_EXTRACTION_BUNDLE"
DEFAULT_BUNDLE_ID = "2002_2024"

BOUND_NAMES = frozenset({
    # export
    "source_export_dir",
    "source_manifest_sha256",
    "source_corpus_sha256",
    "source_document_ids_sha256",
    "complete_line_census",
    "endorsement_parents",
    "endorsement_segments",
    # round four
    "round4_stage_a_input_manifest_sha256",
    "round4_stage_b_input_manifest_sha256",
    # The generation config of a stage includes its input manifest hash.
    "round4_stage_a_generation_config_sha256",
    "round4_stage_b_generation_config_sha256",
    "array_item_cap_withheld_windows",
    "core_route_column",
    "core_url_fallback_column",
    # release build
    "excluded_parent_documents",
    "wrong_candidate_parent_documents",
    "parent_counts",
    "segment_counts",
    "universe_rows",
    "transformed_parent_documents",
    # The 2002-2024 audit found 11 omissions of the earlier pipeline. A corpus with
    # no earlier pipeline has zero; its omission file is an empty list.
    "known_omission_count",
})


@lru_cache(maxsize=None)
def _load(path: str) -> dict[str, Any]:
    payload = json.loads(Path(path).read_text())
    if set(payload) != {"bundle_id", "values"} or not isinstance(payload["values"], dict):
        raise ValueError(f"bundle file must hold exactly bundle_id and values: {path}")
    if payload["bundle_id"] == DEFAULT_BUNDLE_ID:
        raise ValueError("a bundle file cannot use the identifier of the default bundle")
    unknown = set(payload["values"]) - BOUND_NAMES
    if unknown:
        raise ValueError(f"unknown bound names in {path}: {sorted(unknown)}")
    return payload


def active_bundle() -> dict[str, Any] | None:
    path = os.environ.get(ENVIRONMENT_VARIABLE)
    return _load(path) if path else None


def bundle_id() -> str:
    bundle = active_bundle()
    return bundle["bundle_id"] if bundle else DEFAULT_BUNDLE_ID


def bound(name: str, default: Any) -> Any:
    """Return the value of `name` for the active bundle, or `default`."""
    if name not in BOUND_NAMES:
        raise KeyError(f"not a bound name: {name}")
    bundle = active_bundle()
    if bundle is None:
        return default
    if name not in bundle["values"]:
        raise KeyError(f"bundle {bundle['bundle_id']} has no value for {name}")
    return bundle["values"][name]
