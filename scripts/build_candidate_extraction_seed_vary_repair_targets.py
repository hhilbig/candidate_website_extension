#!/usr/bin/env python3
"""Build the seed-vary repair target list from an audit report or a prior repair.

The audit-report mode names output files, so the builder removes the ".out" marker
from every chunk name: the repair runner reads input chunks. The repair-manifest
mode takes the windows a previous repair attempt left at the cap, which is how a
later attempt is scoped.

Usage:
  python scripts/build_candidate_extraction_seed_vary_repair_targets.py \
    --audit-report quality_reports/candidate_extraction_one_stage_audit_2026-09-17.json \
    --primary-seed 20260824 \
    --repair-seed 20260917 \
    --output quality_reports/candidate_extraction_repair_targets_2026-09-17.json

  python scripts/build_candidate_extraction_seed_vary_repair_targets.py \
    --previous-repair-manifest repair_manifest.json \
    --primary-seed 20260824 \
    --repair-seed 20260918 \
    --attempt-index 3 \
    --output quality_reports/candidate_extraction_repair_targets_attempt3.json
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

REPAIR_TARGETS_VERSION = "candidate_extraction_seed_vary_repair_v1"
REPAIR_REASON = (
    "output_truncated: greedy decoding repeated one item and reached the output cap, "
    "so an identical rerun returns identical bytes"
)
TRUNCATED_DEFECT_KEY = "output_truncated_finish_reason_length"


def input_chunk_name(output_name: str) -> str:
    """Map an output file name to the input chunk name the runner reads."""
    return output_name.replace(".out.jsonl.gz", ".jsonl.gz")


def clean_chunk_name(chunk: object) -> str:
    """Check one chunk name and return it as text."""
    name = str(chunk)
    if (
        not name.startswith("chunk_")
        or not name.endswith(".jsonl.gz")
        or "/" in name
        or ".out." in name
    ):
        raise ValueError(f"unsafe chunk name: {name!r}")
    return name


def build_target_payload(
    audit: dict,
    *,
    primary_seed: int,
    repair_seed: int,
    audit_source: str,
    attempt_index: int = 2,
) -> dict:
    """Build the repair target payload from an audit report."""
    defects = audit.get("defects")
    if not isinstance(defects, dict):
        raise ValueError("audit report holds no defects block")
    entries = defects.get(TRUNCATED_DEFECT_KEY)
    if not isinstance(entries, list) or not entries:
        raise ValueError("audit report holds no cap-limited windows")
    targets = []
    seen: set[tuple[str, str]] = set()
    for entry in entries:
        chunk = input_chunk_name(str(entry["chunk"]))
        document_id = str(entry["document_id"])
        if not chunk.startswith("chunk_") or not chunk.endswith(".jsonl.gz"):
            raise ValueError(f"unsafe chunk name derived from the audit: {chunk!r}")
        if ".out." in chunk:
            raise ValueError(f"chunk name still names an output file: {chunk!r}")
        if (chunk, document_id) in seen:
            raise ValueError(f"repeated cap-limited window: {chunk} {document_id}")
        seen.add((chunk, document_id))
        targets.append({"chunk": chunk, "document_id": document_id})
    return {
        "repair_manifest_version": REPAIR_TARGETS_VERSION,
        "created_from": audit_source,
        "primary_seed": int(primary_seed),
        "repair_seed": int(repair_seed),
        "attempt_index": int(attempt_index),
        "reason": REPAIR_REASON,
        "target_count": len(targets),
        "targets": sorted(targets, key=lambda item: (item["chunk"], item["document_id"])),
    }


def build_target_payload_from_repair(
    previous: dict,
    *,
    primary_seed: int,
    repair_seed: int,
    manifest_source: str,
    attempt_index: int,
) -> dict:
    """Build the next attempt's target list from a previous repair attestation.

    Only the windows the previous attempt left at the cap are carried forward.
    A window the previous attempt recovered is finished, and re-inferring it would
    put a second valid row against the same window.
    """
    if previous.get("repair_manifest_version") != REPAIR_TARGETS_VERSION:
        raise ValueError("previous repair attestation version differs")
    previous_attempt = previous.get("attempt_index")
    if not isinstance(previous_attempt, int) or isinstance(previous_attempt, bool):
        raise ValueError("previous repair attestation has no attempt index")
    if int(attempt_index) <= previous_attempt:
        raise ValueError(
            f"attempt index {attempt_index} does not follow {previous_attempt}"
        )
    if int(repair_seed) in {
        int(primary_seed),
        int(previous.get("repair_seed") or 0),
    }:
        raise ValueError("the repair seed repeats a seed already used")
    entries = previous.get("targets")
    if not isinstance(entries, list) or not entries:
        raise ValueError("previous repair attestation names no targets")
    targets = []
    unrepaired = 0
    for entry in entries:
        if bool(entry.get("repaired")):
            continue
        unrepaired += 1
        targets.append(
            {
                "chunk": clean_chunk_name(entry["chunk"]),
                "document_id": str(entry["document_id"]),
            }
        )
    if not targets:
        raise ValueError("the previous repair attestation has no unrepaired window")
    if unrepaired != int(previous.get("unrepaired_count", -1)):
        raise ValueError("previous repair attestation unrepaired count differs")
    return {
        "repair_manifest_version": REPAIR_TARGETS_VERSION,
        "created_from": manifest_source,
        "created_from_attempt_index": previous_attempt,
        "primary_seed": int(primary_seed),
        "repair_seed": int(repair_seed),
        "attempt_index": int(attempt_index),
        "reason": REPAIR_REASON,
        "target_count": len(targets),
        "targets": sorted(targets, key=lambda item: (item["chunk"], item["document_id"])),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    source = parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--audit-report", type=Path)
    source.add_argument("--previous-repair-manifest", type=Path)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--primary-seed", type=int, default=20260824)
    parser.add_argument("--repair-seed", type=int, default=20260917)
    parser.add_argument("--attempt-index", type=int, default=2)
    args = parser.parse_args()

    if args.audit_report is not None:
        audit = json.loads(args.audit_report.read_text(encoding="utf-8"))
        payload = build_target_payload(
            audit,
            primary_seed=args.primary_seed,
            repair_seed=args.repair_seed,
            audit_source=str(args.audit_report),
            attempt_index=args.attempt_index,
        )
    else:
        previous = json.loads(
            args.previous_repair_manifest.read_text(encoding="utf-8")
        )
        payload = build_target_payload_from_repair(
            previous,
            primary_seed=args.primary_seed,
            repair_seed=args.repair_seed,
            manifest_source=str(args.previous_repair_manifest),
            attempt_index=args.attempt_index,
        )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps({
        "output": str(args.output),
        "target_count": payload["target_count"],
        "chunk_count": len({item["chunk"] for item in payload["targets"]}),
        "attempt_index": payload["attempt_index"],
        "repair_seed": payload["repair_seed"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
