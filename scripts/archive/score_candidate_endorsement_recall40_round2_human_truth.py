#!/usr/bin/env python3
"""Authenticate and score prediction-blind round-two recall40 human truth."""
from __future__ import annotations

import argparse
import datetime as dt
import gzip
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any, Mapping

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts.audit_candidate_endorsement_40_parent_feasibility import (  # noqa: E402
    score_recall_matching,
)
from scripts.audit_candidate_endorsement_recall40_round2_stage_b import (  # noqa: E402
    build_audit,
)
from scripts.build_candidate_endorsement_recall40_round2_human_task import (  # noqa: E402
    build_task,
)
from src.candidate_endorsement_line_ledger import sha256_json  # noqa: E402

TASK_DIR = REPO / "build/private_candidate_endorsement_recall40_round2_human_truth_20260905"
TASK_SHA256 = "d7a5b3ca8c94c6ffda185fdc9e62b5f2f6af844a20c3ef70e28508ac2a121f03"
HTML_SHA256 = "a884fedf229af7fb07789045d9fe5fee93c564266221ba0c6c9c018071f94b02"
AUDIT = REPO / "quality_reports/candidate_endorsement_recall40_round2_stage_b_failed_2026-09-03.json"
AUDIT_SHA256 = "f62034c81561b3bd8f5cf845b098c20f0c0c2509e8ad65edfa5d3043709f4063"
OUTPUT_DIR = REPO / (
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round2_stage_b_outputs_20260902a"
)
MATCHER = REPO / "scripts/audit_candidate_endorsement_40_parent_feasibility.py"
MATCHER_SHA256 = "c5fc6edad2cd32ffec3d1941f877fbf25ef8d1d8487f62b017bd6c597b24f2ca"
STAMP = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def strict(path: Path) -> Any:
    def hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        value: dict[str, Any] = {}
        for key, item in pairs:
            if key in value:
                raise ValueError(f"duplicate JSON key: {key}")
            value[key] = item
        return value
    return json.loads(
        path.read_text(encoding="utf-8"), object_pairs_hook=hook,
        parse_constant=lambda value: (_ for _ in ()).throw(
            ValueError(f"nonfinite JSON value: {value}")
        ),
    )


def _validated_models() -> tuple[dict[str, list[dict[str, Any]]], dict[str, int]]:
    audit = strict(AUDIT)
    if sha(AUDIT) != AUDIT_SHA256 or sha256_json(audit) != sha256_json(build_audit(OUTPUT_DIR)):
        raise ValueError("failed Stage-B audit differs from exact replay")
    by_parent: dict[str, list[dict[str, Any]]] = {}
    identities: set[tuple[str, str, int]] = set()
    duplicates = 0
    rows: list[dict[str, Any]] = []
    for path in sorted(OUTPUT_DIR.glob("chunk_*.out.jsonl.gz")):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            rows.extend(json.loads(line) for line in handle)
    for row in rows:
        if row["technical_pass"] is not True:
            continue
        for record in row["stage_alignment"]["aligned_records"]:
            identity = (
                record["stage_b_batch_id"], record["stage_b_slot"],
                record["model_record_index"],
            )
            duplicates += int(identity in identities)
            identities.add(identity)
            by_parent.setdefault(row["parent_document_id"], []).append(record)
    return by_parent, {
        "output_batches": audit["authenticated_census"]["output_rows"],
        "technical_pass_batches": audit["authenticated_census"]["technical_pass"],
        "technical_fail_batches": audit["authenticated_census"]["technical_fail"],
        "failed_alignment_withheld_records": sum(
            audit["authenticated_census"]["alignment_withheld_reason_counts"].values()
        ),
        "duplicate_model_identity_count": duplicates,
    }


def score(task_path: Path, results_path: Path) -> dict[str, Any]:
    html_path = task_path.parent / "index.html"
    if sha(task_path) != TASK_SHA256 or sha(html_path) != HTML_SHA256:
        raise ValueError("frozen round-two task/app bytes differ")
    if sha(AUDIT) != AUDIT_SHA256 or sha(MATCHER) != MATCHER_SHA256:
        raise ValueError("frozen audit or matcher bytes differ")
    task = strict(task_path)
    if task != build_task(OUTPUT_DIR, AUDIT):
        raise ValueError("task differs from authenticated source")
    results = strict(results_path)
    if not isinstance(results, Mapping) or set(results) != {
        "task_id", "format", "exported_at", "parent_count", "responses",
    }:
        raise ValueError("results field set differs")
    if (
        results["task_id"] != task["task_id"]
        or results["format"] != "recall40-human-truth-v1"
        or type(results["parent_count"]) is not int
        or results["parent_count"] != 40
    ):
        raise ValueError("results identity/count differs")
    stamp = results["exported_at"]
    if not isinstance(stamp, str) or not STAMP.fullmatch(stamp):
        raise ValueError("results timestamp differs")
    parsed = dt.datetime.strptime(stamp, "%Y-%m-%dT%H:%M:%S.%fZ")
    if parsed.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" != stamp:
        raise ValueError("results timestamp is not canonical")

    parents = {parent["parent_document_id"]: parent for parent in task["parents"]}
    responses = results["responses"]
    if (
        not isinstance(responses, list)
        or [response.get("parent_document_id") for response in responses]
        != list(parents)
    ):
        raise ValueError("results response order differs")
    models_by_parent, technical = _validated_models()
    totals = {"human": 0, "matched": 0, "support": 0, "model": 0, "true_zero": 0}
    pages: list[dict[str, Any]] = []
    for response in responses:
        if not isinstance(response, Mapping) or set(response) != {
            "parent_document_id", "true_zero", "occurrences",
        }:
            raise ValueError("response field set differs")
        parent_id = response["parent_document_id"]
        source = parents[parent_id]["parent_source_text"]
        zero = response["true_zero"]
        occurrences = response["occurrences"]
        if type(zero) is not bool or not isinstance(occurrences, list) or zero == bool(occurrences):
            raise ValueError("true-zero/occurrence closure differs")
        human: list[dict[str, Any]] = []
        seen: set[tuple[int, int]] = set()
        for occurrence in occurrences:
            if not isinstance(occurrence, Mapping) or set(occurrence) != {
                "name_start", "name_end", "support_start", "support_end",
            } or any(type(occurrence[key]) is not int for key in occurrence):
                raise ValueError("occurrence schema differs")
            ns, ne, ss, se = (occurrence[key] for key in (
                "name_start", "name_end", "support_start", "support_end",
            ))
            if not (0 <= ns < ne <= len(source) and 0 <= ss < se <= len(source)):
                raise ValueError("occurrence offsets differ")
            if (ns, ne) in seen:
                raise ValueError("duplicate human occurrence")
            seen.add((ns, ne))
            human.append({
                "endorser_name": source[ns:ne], "name_start": ns, "name_end": ne,
                "support_start": ss, "support_end": se,
            })
        model = []
        for record in models_by_parent.get(parent_id, []):
            context_start = record.get("endorsement_context_evidence_parent_start_char")
            context_end = record.get("endorsement_context_evidence_parent_end_char")
            model.append({
                "endorser_name": record["endorser_name"],
                "record_evidence_parent_span": [
                    record["record_evidence_parent_start_char"],
                    record["record_evidence_parent_end_char"],
                ],
                "endorsement_context_parent_span": (
                    [context_start, context_end]
                    if type(context_start) is int and type(context_end) is int
                    else [-1, -1]
                ),
                "batch_id": record["stage_b_batch_id"],
                "slot": record["stage_b_slot"],
                "record_index": record["model_record_index"],
            })
        matched = score_recall_matching(human, model) if human else {
            "matched_count": 0, "combined_support_covered_count": 0,
        }
        unmatched_model = len(model) - matched["matched_count"]
        zero_pass = zero and not model
        page_pass = zero_pass if zero else (
            matched["matched_count"] == len(human) and unmatched_model == 0
        )
        totals["human"] += len(human)
        totals["matched"] += matched["matched_count"]
        totals["support"] += matched["combined_support_covered_count"]
        totals["model"] += len(model)
        totals["true_zero"] += int(zero)
        pages.append({
            "parent_document_id": parent_id, "human_occurrences": len(human),
            "model_records": len(model), "matched": matched["matched_count"],
            "support_covered": matched["combined_support_covered_count"],
            "unmatched_model_records": unmatched_model, "true_zero": zero,
            "page_pass": page_pass,
        })
    complete = sum(page["page_pass"] for page in pages)
    presence = 100 * totals["matched"] / totals["human"] if totals["human"] else 100.0
    support = 100 * totals["support"] / totals["matched"] if totals["matched"] else 100.0
    unresolved = (
        totals["human"] - totals["matched"]
        + technical["failed_alignment_withheld_records"]
        + technical["duplicate_model_identity_count"]
    )
    checks = [
        {"id": "technical_batch_validity_percent", "observed": 100 * technical["technical_pass_batches"] / technical["output_batches"], "expected": 100, "passed": technical["technical_pass_batches"] == technical["output_batches"]},
        {"id": "complete_pages_min", "observed": complete, "expected": 36, "passed": complete >= 36},
        {"id": "human_truth_presence_percent", "observed": presence, "expected": 100, "passed": presence == 100},
        {"id": "combined_record_context_support_percent", "observed": support, "expected": 100, "passed": support == 100},
        {"id": "unresolved_withheld_duplicate_matches", "observed": unresolved, "expected": 0, "passed": unresolved == 0},
    ]
    return {
        "version": "candidate_endorsement_recall40_round2_human_truth_score_v1",
        "task_sha256": sha(task_path), "html_sha256": sha(html_path),
        "results_sha256": sha(results_path), "failed_audit_sha256": sha(AUDIT),
        "matcher_implementation_sha256": MATCHER_SHA256,
        "gate_decision": "GO_CONSIDER_FULL_PRIVATE_ENDORSEMENT_INFERENCE"
        if all(check["passed"] for check in checks) else "NO_GO",
        "checks": checks, "technical_census": technical, "totals": totals,
        "page_scores_sha256": sha256_json(pages),
        "full_private_inference_authorized": False, "release_authorized": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", type=Path, required=True)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise ValueError("score output must not already exist")
    report = score(args.task, args.results)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    return 0 if report["gate_decision"].startswith("GO_") else 2


if __name__ == "__main__":
    raise SystemExit(main())
