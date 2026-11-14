#!/usr/bin/env python3
"""Re-align the stored one-stage outputs under schema version 2.2.

Version 2.2 adds one search fallback to the aligner and changes nothing about
generation. The prompts, the JSON schemas and the user-message template are the
2.1 objects, so a stored 2.1 output is a valid 2.2 input and this pass needs no
GPU and no new inference.

Why the pass exists. The 2.1 aligner withholds a record when the model's
evidence is not an exact substring of the page. On the issue route it withholds
13.2 percent of model items, and 68.9 percent of a 4,000-item sample differ only
in punctuation form, for example a straight apostrophe where the page carries a
curly one. The content is on the page. Version 2.2 finds it and stores the
page's own slice.

What the pass preserves. Every generation field is copied without change:
`raw_output`, every hash of it, the token counts, the prompt and schema hashes,
the user-message hash and the model identity. Only the alignment outputs and
the contract identity change.

Usage:
  python scripts/realign_candidate_extraction_v2_2.py \
    --output-dir build/hive_candidate_extraction/v2_1_full_hive_output_20260916p \
    --export-dir build/hive_candidate_extraction/v2_1_full_export_20260824p \
    --destination build/hive_candidate_extraction/v2_2_full_hive_output_20260923 \
    --report quality_reports/candidate_extraction_realign_v2_2_2026-09-23.json
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import re
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterator

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from src.candidate_extraction import (  # noqa: E402
    align_extraction_exact_spans,
    get_extraction_contract,
    validate_extraction,
)

# The alignment spec sits inside the frozen generation config, so re-aligning a
# corpus necessarily gives it a new generation identity. This pass therefore
# re-stamps `generation_config_sha256` on every row and rewrites the run
# metadata. Generation itself does not change: the prompts, the schemas, the
# user-message template, the model, the seed and the token budget are the 2.1
# objects, and `raw_output` is copied untouched. Both mappings are computed, not
# typed in, so they cannot drift from the contract.
def _generation_identity_map() -> dict[str, str]:
    """Return {2.1 generation hash: 2.2 generation hash} for every seed used."""
    import importlib

    mapping: dict[str, str] = {}
    os.environ["CANDIDATE_EXTRACTION_SCHEMA_VERSION"] = "2.1"
    old_module = importlib.reload(
        importlib.import_module("scripts.import_candidate_extraction_v2")
    )
    seeds = (
        old_module.EXPECTED_GENERATION_SEED,
        20260917,
        20260918,
    )
    old_hashes = {
        seed: old_module.generation_config_for_seed(seed)[1] for seed in seeds
    }
    os.environ["CANDIDATE_EXTRACTION_SCHEMA_VERSION"] = "2.2"
    new_module = importlib.reload(
        importlib.import_module("scripts.import_candidate_extraction_v2")
    )
    for seed in seeds:
        mapping[old_hashes[seed]] = new_module.generation_config_for_seed(seed)[1]
    return mapping

TARGET_VERSION = "2.2"
GENERATION_IDENTITY: dict[str, str] = {}
# Every route is re-aligned. Scoping this to issues and biography on
# 2026-09-23 was wrong: a row carries its own alignment fields and generation
# hash, so a skipped route leaves two identities inside one bundle, which the
# importer correctly refuses. Endorsement records still reach the release
# through the two-stage ledger; that ledger is a separate artifact.
REALIGNED_ROUTES = ("issues", "biography", "endorsements")


def read_jsonl_gz(path: Path) -> Iterator[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def chunk_index(path: Path) -> str:
    match = re.search(r"chunk_(\d+)", path.name)
    if match is None:
        raise SystemExit(f"cannot read a chunk index from {path.name}")
    return match.group(1)


def realign_row(
    row: dict[str, Any], source: dict[str, Any], contract: dict[str, Any]
) -> tuple[dict[str, Any], str]:
    """Return the 2.2 row and what happened to it."""
    new_row = dict(row)
    new_row["schema_version"] = TARGET_VERSION
    new_row["alignment_id"] = contract["alignment_id"]
    new_row["alignment_spec_sha256"] = contract["alignment_spec_sha256"]
    new_row["extraction_contract_sha256"] = contract["extraction_contract_sha256"]
    old_generation = str(row.get("generation_config_sha256") or "")
    if old_generation:
        if old_generation not in GENERATION_IDENTITY:
            raise SystemExit(
                f"{row.get('document_id')}: unknown generation identity "
                f"{old_generation}, refusing to re-stamp it"
            )
        new_row["generation_config_sha256"] = GENERATION_IDENTITY[old_generation]

    if row.get("parse_status") != "parsed":
        return new_row, "not_parsed"
    model_extraction = row.get("model_extraction")
    if not isinstance(model_extraction, dict):
        return new_row, "no_model_extraction"

    route = str(row["route"])
    source_text = str(source["source_text"])
    aligned, withheld, alignment = align_extraction_exact_spans(
        route,
        model_extraction,
        source_text,
        endorsement_context_provenance=source.get("endorsement_context_provenance"),
        segment_start_char=int(row.get("segment_start_char") or 0),
        schema_version=TARGET_VERSION,
    )
    if alignment["original_model_extraction_sha256"] != row.get(
        "model_extraction_sha256"
    ):
        raise SystemExit(
            f"{row['document_id']}: the model extraction hash changed, refusing"
        )
    new_row["extraction"] = aligned
    new_row["withheld_model_items"] = withheld
    new_row["exact_alignment"] = alignment
    new_row["aligned_extraction_sha256"] = alignment["aligned_extraction_sha256"]
    new_row["validation"] = validate_extraction(
        route,
        aligned,
        source_text,
        TARGET_VERSION,
        endorsement_context_provenance=source.get("endorsement_context_provenance"),
        segment_start_char=int(row.get("segment_start_char") or 0),
    )
    gained = int(alignment["aligned_item_count"]) - len(
        (row.get("extraction") or {}).get(
            next(iter(aligned)) if aligned else "", []
        )
        if isinstance(row.get("extraction"), dict)
        else []
    )
    return new_row, "gained" if gained > 0 else "unchanged"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--export-dir", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--limit-chunks", type=int, default=0)
    args = parser.parse_args()

    contract = get_extraction_contract(TARGET_VERSION)
    global GENERATION_IDENTITY
    GENERATION_IDENTITY = _generation_identity_map()
    out_chunks = sorted(args.output_dir.glob("chunk_*.out.jsonl.gz"))
    if not out_chunks:
        raise SystemExit(f"no output chunks under {args.output_dir}")
    export_by_index = {
        chunk_index(path): path
        for path in sorted((args.export_dir / "chunks").glob("chunk_*.jsonl.gz"))
    }
    args.destination.mkdir(parents=True, exist_ok=True)

    rows_seen = Counter()
    aligned_before = Counter()
    aligned_after = Counter()
    withheld_before = Counter()
    withheld_after = Counter()
    dispositions = Counter()
    method_counts = Counter()

    if args.limit_chunks:
        out_chunks = out_chunks[: args.limit_chunks]
    for position, out_path in enumerate(out_chunks, start=1):
        index = chunk_index(out_path)
        export_path = export_by_index.get(index)
        if export_path is None:
            raise SystemExit(f"no export chunk for {out_path.name}")
        sources = {str(row["document_id"]): row for row in read_jsonl_gz(export_path)}
        destination = args.destination / out_path.name
        temporary = destination.with_suffix(destination.suffix + ".partial")
        with gzip.open(temporary, "wt", encoding="utf-8") as handle:
            for row in read_jsonl_gz(out_path):
                route = str(row.get("route") or "")
                rows_seen[route] += 1
                before = row.get("exact_alignment") or {}
                aligned_before[route] += int(before.get("aligned_item_count") or 0)
                withheld_before[route] += int(before.get("withheld_item_count") or 0)
                if route not in REALIGNED_ROUTES:
                    dispositions["route_not_realigned"] += 1
                    handle.write(json.dumps(row, ensure_ascii=False) + "\n")
                    aligned_after[route] += int(before.get("aligned_item_count") or 0)
                    withheld_after[route] += int(before.get("withheld_item_count") or 0)
                    continue
                source = sources.get(str(row["document_id"]))
                if source is None:
                    raise SystemExit(f"{row['document_id']} is absent from the export")
                new_row, disposition = realign_row(row, source, contract)
                dispositions[disposition] += 1
                after = new_row.get("exact_alignment") or {}
                aligned_after[route] += int(after.get("aligned_item_count") or 0)
                withheld_after[route] += int(after.get("withheld_item_count") or 0)
                list_key = {
                    "issues": "issue_blocks",
                    "biography": "facts",
                    "endorsements": "endorsements",
                }[route]
                for item in (new_row.get("extraction") or {}).get(list_key, []) or []:
                    for key, value in item.items():
                        if key.endswith("_alignment_method"):
                            method_counts[str(value)] += 1
                handle.write(json.dumps(new_row, ensure_ascii=False) + "\n")
        os.replace(temporary, destination)
        if position % 50 == 0 or position == len(out_chunks):
            print(f"{position}/{len(out_chunks)} chunks", flush=True)

    report = {
        "format": "candidate-extraction-realign-v2-2-v1",
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "source_output_dir": str(args.output_dir),
        "export_dir": str(args.export_dir),
        "destination": str(args.destination),
        "schema_version": TARGET_VERSION,
        "extraction_contract_sha256": contract["extraction_contract_sha256"],
        "alignment_spec_sha256": contract["alignment_spec_sha256"],
        "realigned_routes": list(REALIGNED_ROUTES),
        "chunks": len(out_chunks),
        "rows_by_route": dict(rows_seen),
        "aligned_before": dict(aligned_before),
        "aligned_after": dict(aligned_after),
        "withheld_before": dict(withheld_before),
        "withheld_after": dict(withheld_after),
        "recovered_records": {
            route: aligned_after[route] - aligned_before[route]
            for route in sorted(set(aligned_before) | set(aligned_after))
        },
        "dispositions": dict(dispositions),
        "alignment_method_counts": dict(method_counts),
    }
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(report, indent=1, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps({
        "report": str(args.report),
        "recovered_records": report["recovered_records"],
        "withheld_before": report["withheld_before"],
        "withheld_after": report["withheld_after"],
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
