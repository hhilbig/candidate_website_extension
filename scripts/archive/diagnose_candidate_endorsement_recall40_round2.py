#!/usr/bin/env python3
"""Diagnose the authenticated round-two recall40 result without changing it."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts.audit_candidate_endorsement_40_parent_feasibility import (  # noqa: E402
    score_recall_matching,
)
from scripts.score_candidate_endorsement_recall40_round2_human_truth import (  # noqa: E402
    OUTPUT_DIR,
    TASK_DIR,
    score,
    strict,
)
from src.candidate_endorsement_line_ledger import sha256_json  # noqa: E402

INPUT_DIR = REPO / (
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round2_stage_b_preflight_20260902a"
)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_jsonl_gz(paths: list[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in paths:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            rows.extend(json.loads(line) for line in handle)
    return rows


def build_diagnostic(
    task_path: Path,
    results_path: Path,
    score_path: Path,
) -> dict[str, Any]:
    observed_score = strict(score_path)
    if observed_score != score(task_path, results_path):
        raise ValueError("stored human-truth score differs from exact replay")

    task = strict(task_path)
    results = strict(results_path)
    responses = {row["parent_document_id"]: row for row in results["responses"]}
    input_rows = read_jsonl_gz(sorted((INPUT_DIR / "chunks").glob("*.jsonl.gz")))
    output_rows = read_jsonl_gz(sorted(OUTPUT_DIR.glob("chunk_*.out.jsonl.gz")))
    inputs = {row["batch_id"]: row for row in input_rows}
    if len(inputs) != len(input_rows) or len(output_rows) != len(input_rows):
        raise ValueError("Stage-B input/output batch closure differs")

    raw_models: dict[str, list[dict[str, Any]]] = {}
    target_spans: dict[str, list[tuple[int, int]]] = {}
    for output in output_rows:
        batch = inputs[output["batch_id"]]
        targets = {row["slot"]: row for row in batch["targets"]}
        context = batch["endorsement_line_context"]
        context_start = context.get("context_parent_start_char")
        context_end = context.get("context_parent_end_char")
        context_span = (
            [context_start, context_end]
            if type(context_start) is int and type(context_end) is int
            else [-1, -1]
        )
        for target in batch["targets"]:
            block = target["record_block_lines"]
            span = (
                min(row["parent_start_char"] for row in block),
                max(row["parent_end_char"] for row in block),
            )
            target_spans.setdefault(batch["parent_document_id"], []).append(span)
        for slot, decision in output["model_output"].items():
            if slot not in targets:
                if decision["records"]:
                    raise ValueError("model emitted records for a padding slot")
                continue
            block = targets[slot]["record_block_lines"]
            evidence_span = [
                min(row["parent_start_char"] for row in block),
                max(row["parent_end_char"] for row in block),
            ]
            for record_index, record in enumerate(decision["records"]):
                raw_models.setdefault(batch["parent_document_id"], []).append({
                    "endorser_name": record["endorser_name"],
                    "record_evidence_parent_span": evidence_span,
                    "endorsement_context_parent_span": context_span,
                    "batch_id": output["batch_id"],
                    "slot": slot,
                    "record_index": record_index,
                })

    totals = {
        "human_occurrences": 0,
        "raw_model_records": 0,
        "matched_occurrences": 0,
        "support_covered_occurrences": 0,
        "complete_pages": 0,
        "detected_anchor_blocks": 0,
        "anchor_blocks_with_human_occurrence": 0,
        "human_occurrences_in_anchor_block": 0,
    }
    causes = {"detected_block_but_name_differs": 0, "no_detected_anchor_block": 0}
    page_rows: list[dict[str, Any]] = []
    for ordinal, parent in enumerate(task["parents"], start=1):
        parent_id = parent["parent_document_id"]
        source = parent["parent_source_text"]
        response = responses[parent_id]
        humans = [
            {
                "endorser_name": source[row["name_start"] : row["name_end"]],
                **row,
            }
            for row in response["occurrences"]
        ]
        models = raw_models.get(parent_id, [])
        matching = score_recall_matching(humans, models) if humans else {
            "matched_pairs": [],
            "matched_count": 0,
            "combined_support_covered_count": 0,
        }
        matched_humans = {pair[0] for pair in matching["matched_pairs"]}
        spans = target_spans.get(parent_id, [])
        covered_humans = {
            index
            for index, human in enumerate(humans)
            if any(
                start <= human["name_start"] and end >= human["name_end"]
                for start, end in spans
            )
        }
        blocks_with_human = sum(
            any(
                start <= human["name_start"] and end >= human["name_end"]
                for human in humans
            )
            for start, end in spans
        )
        for index in range(len(humans)):
            if index not in matched_humans:
                causes[
                    "detected_block_but_name_differs"
                    if index in covered_humans
                    else "no_detected_anchor_block"
                ] += 1
        complete = (
            matching["matched_count"] == len(humans)
            and len(models) == len(humans)
        )
        page_rows.append({
            "task_parent_ordinal": ordinal,
            "human_occurrences": len(humans),
            "raw_model_records": len(models),
            "matched_occurrences": matching["matched_count"],
            "support_covered_occurrences": matching[
                "combined_support_covered_count"
            ],
            "detected_anchor_blocks": len(spans),
            "anchor_blocks_with_human_occurrence": blocks_with_human,
            "human_occurrences_in_anchor_block": len(covered_humans),
            "complete_page": complete,
        })
        totals["human_occurrences"] += len(humans)
        totals["raw_model_records"] += len(models)
        totals["matched_occurrences"] += matching["matched_count"]
        totals["support_covered_occurrences"] += matching[
            "combined_support_covered_count"
        ]
        totals["complete_pages"] += int(complete)
        totals["detected_anchor_blocks"] += len(spans)
        totals["anchor_blocks_with_human_occurrence"] += blocks_with_human
        totals["human_occurrences_in_anchor_block"] += len(covered_humans)

    denominator = totals["human_occurrences"]
    matched = totals["matched_occurrences"]
    blocks = totals["detected_anchor_blocks"]
    worst = max(
        page_rows,
        key=lambda row: row["detected_anchor_blocks"]
        - row["anchor_blocks_with_human_occurrence"],
    )
    return {
        "version": "candidate_endorsement_recall40_round2_diagnostic_v1",
        "scope": "private_prediction_blind_pilot_diagnostic_only",
        "task_sha256": sha(task_path),
        "results_sha256": sha(results_path),
        "score_sha256": sha(score_path),
        "input_manifest_sha256": sha(INPUT_DIR / "manifest.json"),
        "failed_output_inventory_sha256": sha256_json(sorted(
            {path.name: sha(path) for path in OUTPUT_DIR.iterdir()}.items()
        )),
        "strict_score_gate_decision": observed_score["gate_decision"],
        "strict_score_checks": observed_score["checks"],
        "raw_output_latent_census": totals,
        "raw_output_latent_rates_percent": {
            "name_and_anchor_presence": 100 * matched / denominator,
            "combined_support_among_matches": (
                100 * totals["support_covered_occurrences"] / matched
            ),
            "human_occurrence_anchor_recall": (
                100 * totals["human_occurrences_in_anchor_block"] / denominator
            ),
            "detected_anchor_block_precision": (
                100 * totals["anchor_blocks_with_human_occurrence"] / blocks
            ),
        },
        "unmatched_human_occurrence_causes": causes,
        "largest_false_anchor_concentration": {
            "task_parent_ordinal": worst["task_parent_ordinal"],
            "anchor_blocks_without_human_occurrence": (
                worst["detected_anchor_blocks"]
                - worst["anchor_blocks_with_human_occurrence"]
            ),
        },
        "page_census_sha256": sha256_json(page_rows),
        "full_private_inference_authorized": False,
        "release_authorized": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--task", type=Path, default=TASK_DIR / "task.json")
    parser.add_argument("--results", type=Path, default=TASK_DIR / "results.json")
    parser.add_argument("--score", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("diagnostic output must not already exist")
    report = build_diagnostic(args.task, args.results, args.score)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
