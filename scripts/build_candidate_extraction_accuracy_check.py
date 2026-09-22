#!/usr/bin/env python3
"""Build a small record-accuracy hand-coding task from the production records.

This is an interim accuracy check, not the approved release validation. The
approved design is 240 items in four batches of 60, drawn from a frozen bundle
by `build_candidate_extraction_handcode.py`. Hanno asked on 2026-09-18 for a
smaller and simpler pass, so this script draws at most 100 records and asks two
yes/no questions about each.

The records are the same objects the release package will carry. The packager
reshapes and checks them; it does not change their content. A record judged
wrong here is wrong in the package too, so the answers carry over.

Sampling is stratified by route and reproducible from the seed. Only records
that survived alignment are eligible, because a withheld record carries no
content to check.

Usage:
  python scripts/build_candidate_extraction_accuracy_check.py \
    --export-dir build/hive_candidate_extraction/v2_1_full_export_20260824p \
    --one-stage-output-dir build/hive_candidate_extraction/v2_1_full_hive_output_20260916p \
    --round4-ledger-dir build/hive_candidate_extraction/round4_record_ledger_20260918 \
    --output-dir validation/accuracy_check_2026-09-18
"""
from __future__ import annotations

import argparse
import gzip
import json
import random
import sys
from pathlib import Path
from typing import Any, Iterator

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

TASK_ID = "candidate_extraction_accuracy_2026_09_18"
DEFAULT_SEED = 20260918
ROUTE_QUOTA = {"issues": 34, "biography": 33, "endorsements": 33}

INSTRUCTIONS = """\
### What you are checking

Each item is one record the model pulled out of one archived campaign page.
You see the record, then the page text it came from, with the claimed evidence
marked.

Answer two questions. Both are strict: answer **no** if you are unsure.

### Question 1, evidence

Does the marked text actually appear on the page and is it the source of this
record? Answer **no** if nothing is marked, if the marked text does not support
the record, or if the record rests on something else on the page.

### Question 2, content

Given that evidence, is every field of the record correct as written? Answer
**no** if any field is invented, altered, incomplete, or attached to the wrong
person or organisation.

A record counts as accurate only when both answers are yes.

Use the notes box when the reason matters, for example a near miss or a page
that is hard to read.
"""

QUESTIONS = [
    {
        "id": "evidence_supports",
        "label": "Does the marked page text support this record?",
        "type": "yesno",
        "help": (
            "No if nothing is marked, if the marked text does not support the "
            "record, or if the record rests on other text."
        ),
    },
    {
        "id": "content_correct",
        "label": "Is every field of the record correct as written?",
        "type": "yesno",
        "help": (
            "No if any field is invented, altered, incomplete, or attached to "
            "the wrong person or organisation."
        ),
    },
]


def read_jsonl_gz(path: Path) -> Iterator[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def read_chunks(directory: Path, pattern: str) -> Iterator[dict[str, Any]]:
    paths = sorted(directory.glob(pattern))
    if not paths:
        raise SystemExit(f"no {pattern} under {directory}")
    for path in paths:
        yield from read_jsonl_gz(path)


def evidence_field(route: str, item: dict[str, Any]) -> str:
    """Return the item's own evidence text, which the page must contain."""
    for key in ("record_evidence", "evidence", "issue_evidence", "biography_evidence"):
        value = item.get(key)
        if isinstance(value, str) and value.strip():
            return value
    for key, value in item.items():
        if key.endswith("_evidence") and isinstance(value, str) and value.strip():
            return value
    return ""


# The released record carries bookkeeping the coder does not need: character
# offsets, hashes, and the links between pipeline stages. Hiding them keeps the
# judgement on the content.
_HIDDEN_SUFFIXES = (
    "_start_char", "_end_char", "_sha256", "_alignment_method",
    "_exact_match_count", "_segment_id", "_batch_id", "_slot", "_index",
    "_item_id",
)
_HIDDEN_KEYS = {
    "line_inventory_id",
    "source_line_id",
    "target_block_alignment_method",
}
# The endorsement route also carries where on the site the context came from.
# That is provenance for the dataset, not something to judge against this page.
_HIDDEN_PREFIXES = ("endorsement_context_",)


def display_record(item: dict[str, Any]) -> dict[str, Any]:
    """Return the record without its bookkeeping fields."""
    return {
        key: value
        for key, value in sorted(item.items())
        if key not in _HIDDEN_KEYS
        and not key.endswith(_HIDDEN_SUFFIXES)
        and not key.startswith(_HIDDEN_PREFIXES)
    }


def badge_block(source: dict[str, Any], route: str) -> dict[str, Any]:
    urls = source.get("source_urls") or []
    return {
        "label": "Candidate",
        "format": "badges",
        "content": {
            "Candidate ID": source.get("candidate_cycle_id"),
            "State": source.get("state"),
            "Office": source.get("office"),
            "Year": source.get("year"),
            "Party": source.get("party"),
            "Route": route,
            "Page": str(urls[0]) if urls else "",
        },
    }


def build_items(
    *,
    export_dir: Path,
    one_stage_output_dir: Path,
    round4_ledger_dir: Path,
    seed: int,
    total: int,
) -> list[dict[str, Any]]:
    sources: dict[str, dict[str, Any]] = {}
    for row in read_chunks(export_dir / "chunks", "chunk_*.jsonl.gz"):
        sources[str(row["document_id"])] = row

    pools: dict[str, list[dict[str, Any]]] = {route: [] for route in ROUTE_QUOTA}
    for row in read_chunks(one_stage_output_dir, "chunk_*.out.jsonl.gz"):
        route = str(row.get("route") or "")
        if route not in {"issues", "biography"}:
            continue
        extraction = row.get("extraction")
        if not isinstance(extraction, dict):
            continue
        for key, value in extraction.items():
            if not isinstance(value, list):
                continue
            for index, item in enumerate(value):
                if isinstance(item, dict):
                    pools[route].append(
                        {
                            "id": f"{row['document_id']}:{key}:{index}",
                            "segment_id": str(row["document_id"]),
                            "route": route,
                            "item": item,
                        }
                    )
    ledger_private = round4_ledger_dir / "endorsement_round4_record_ledger_private.jsonl.gz"
    for record in read_jsonl_gz(ledger_private):
        pools["endorsements"].append(
            {
                "id": str(record["record_id"]),
                "segment_id": str(record["canonical_segment_id"]),
                "route": "endorsements",
                "item": record["item"],
            }
        )

    rng = random.Random(seed)
    chosen: list[dict[str, Any]] = []
    for route, quota in ROUTE_QUOTA.items():
        pool = sorted(pools[route], key=lambda entry: entry["id"])
        if not pool:
            raise SystemExit(f"route {route} has no eligible record")
        take = min(quota, len(pool))
        chosen.extend(rng.sample(pool, take))
    chosen = chosen[:total]
    chosen.sort(key=lambda entry: (entry["route"], entry["id"]))

    items = []
    for entry in chosen:
        source = sources.get(entry["segment_id"])
        if source is None:
            raise SystemExit(f"segment {entry['segment_id']} is absent from the export")
        evidence = evidence_field(entry["route"], entry["item"])
        label_name = str(source.get("candidate_cycle_id") or "")
        items.append(
            {
                "id": entry["id"],
                "label": f"{entry['route']}: {label_name}"[:60],
                "display": [
                    badge_block(source, entry["route"]),
                    {
                        "label": "Model record",
                        "format": "json",
                        "content": display_record(entry["item"]),
                    },
                    {
                        "label": "Page text, with the claimed evidence marked",
                        "format": "text",
                        "content": str(source.get("source_text") or ""),
                        "highlight": [evidence] if evidence else [],
                    },
                ],
            }
        )
    return items


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export-dir", type=Path, required=True)
    parser.add_argument("--one-stage-output-dir", type=Path, required=True)
    parser.add_argument("--round4-ledger-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--total", type=int, default=100)
    args = parser.parse_args()

    if args.total > 100:
        raise SystemExit("this interim check is capped at 100 items")
    items = build_items(
        export_dir=args.export_dir,
        one_stage_output_dir=args.one_stage_output_dir,
        round4_ledger_dir=args.round4_ledger_dir,
        seed=args.seed,
        total=args.total,
    )
    task = {
        "task_id": TASK_ID,
        "title": "Candidate extraction: record accuracy check",
        "instructions": INSTRUCTIONS,
        "items": items,
        "questions": QUESTIONS,
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    task_path = args.output_dir / f"{TASK_ID}.task.json"
    task_path.write_text(
        json.dumps(task, indent=1, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    counts: dict[str, int] = {}
    for item in items:
        route = str(item["label"].split(":")[0])
        counts[route] = counts.get(route, 0) + 1
    print(
        json.dumps(
            {
                "task_file": str(task_path),
                "items": len(items),
                "by_route": counts,
                "seed": args.seed,
                "items_without_marked_evidence": sum(
                    1
                    for item in items
                    if not item["display"][2]["highlight"]
                ),
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
