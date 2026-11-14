#!/usr/bin/env python3
"""Replay the independent sample through the round-four release adapter.

This script calls no model.  It reads the completed Stage-A and Stage-B
development artifacts, rebuilds every parent from the frozen source export,
and projects each Stage-B batch into public endorsement records.

The replay proves the adapter on real model output instead of a synthetic
fixture.  Its report is development evidence.  It is never a release input,
and it never authorizes a release.
"""
from __future__ import annotations

import argparse
import gzip
import json
from collections import Counter, defaultdict
from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import scripts.audit_candidate_endorsement_40_parent_feasibility as base
from src.candidate_endorsement_round4_production import (
    ROUND4_DEVELOPMENT_ARTIFACT_SCOPE,
    ROUND4_PRODUCTION_ARTIFACT_SCOPE,
    get_round4_production_contract,
)
from src.candidate_endorsement_round4_release import (
    classify_stage_b_response,
    parent_inventory_from_export_rows,
    project_round4_stage_b_batch,
    round4_parent_coverage,
)

STAGE_A_INPUT = (
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_independent40_stage_a_20260914b"
)
STAGE_A_OUTPUT = (
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_independent40_stage_a_outputs_20260914b"
)
STAGE_B_INPUT = (
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_independent40_stage_b_20260914a"
)
STAGE_B_OUTPUT = (
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_independent40_stage_b_outputs_20260914a"
)
STAGE_A_INPUT_MANIFEST_SHA256 = (
    "0a962b41e022a68eebbffe01c3f406cf41bbc55b4710251102ae6ba92f79f1d2"
)
STAGE_B_INPUT_MANIFEST_SHA256 = (
    "4f6113518037916c014c51a26ff7927b151915e2d0eaa4a5af5b493e13e61148"
)

SCOPES = {
    "development": ROUND4_DEVELOPMENT_ARTIFACT_SCOPE,
    "production": ROUND4_PRODUCTION_ARTIFACT_SCOPE,
}


def read_jsonl_gz(directory: Path, pattern: str) -> list[dict]:
    rows: list[dict] = []
    for path in sorted(directory.glob(pattern)):
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            rows.extend(json.loads(line) for line in handle if line.strip())
    return rows


def _authenticated_manifest(path: Path, expected: str) -> dict:
    digest = base.sha(path)
    if digest != expected:
        raise ValueError(f"{path.name} bytes differ from the expected digest")
    return json.loads(path.read_text())


def replay(
    repo: Path,
    *,
    scope: str,
    stage_a_input: Path,
    stage_a_output: Path,
    stage_b_input: Path,
    stage_b_output: Path,
    stage_a_manifest_sha256: str,
    stage_b_manifest_sha256: str,
) -> dict:
    artifact_scope = SCOPES[scope]
    _authenticated_manifest(
        stage_a_input / "manifest.json", stage_a_manifest_sha256
    )
    _authenticated_manifest(
        stage_b_input / "manifest.json", stage_b_manifest_sha256
    )
    stage_a_batches = read_jsonl_gz(stage_a_input / "chunks", "*.jsonl.gz")
    stage_a_rows = read_jsonl_gz(stage_a_output, "*.out.jsonl.gz")
    stage_b_batches = read_jsonl_gz(stage_b_input / "chunks", "*.jsonl.gz")
    stage_b_rows = read_jsonl_gz(stage_b_output, "*.out.jsonl.gz")

    full_rows, _ = base.authenticated_full_rows()
    parent_ids = {str(batch["parent_document_id"]) for batch in stage_b_batches}
    grouped: dict[str, list[dict]] = defaultdict(list)
    for row in full_rows:
        if (
            row.get("route") == "endorsements"
            and str(row.get("parent_document_id")) in parent_ids
        ):
            grouped[str(row["parent_document_id"])].append(row)
    parents = {
        parent_id: parent_inventory_from_export_rows(rows)
        for parent_id, rows in grouped.items()
    }

    stage_b_by_id = {str(row["batch_id"]): row for row in stage_b_rows}
    stage_a_by_parent: dict[str, list[dict]] = defaultdict(list)
    for row in stage_a_rows:
        stage_a_by_parent[str(row["parent_document_id"])].append(row)

    classes: Counter = Counter()
    projections_by_parent: dict[str, list[dict]] = defaultdict(list)
    withheld_reasons: Counter = Counter()
    lineage_complete = 0
    evidence_exact = 0
    accounted_outputs = 0
    model_record_total = 0

    lineage_fields = (
        "line_inventory_id", "source_line_id", "stage_a_batch_id",
        "stage_a_slot", "stage_b_batch_id", "stage_b_slot",
        "model_record_index", "carrier_alignment_source_model_item_id",
        "inventory_owner_segment_id", "record_evidence_carrier_segment_id",
        "target_block_alignment_method", "target_block_exact_match_count",
        "record_evidence_parent_start_char", "record_evidence_parent_end_char",
    )

    for batch in stage_b_batches:
        batch_id = str(batch["batch_id"])
        output_row = stage_b_by_id.get(batch_id)
        if output_row is None:
            classes["missing_output_row"] += 1
            continue
        accounted_outputs += 1
        failure_class, _ = classify_stage_b_response(output_row)
        classes[failure_class] += 1
        parent_id = str(batch["parent_document_id"])
        projection = project_round4_stage_b_batch(
            batch, output_row, parents[parent_id],
            artifact_scope=artifact_scope,
        )
        projections_by_parent[parent_id].append(projection)
        model_output = output_row.get("model_output") or {}
        for decision in model_output.values():
            if isinstance(decision, dict):
                model_record_total += len(decision.get("records") or [])
        parent_text = str(parents[parent_id]["parent_source_text"])
        for record in projection["aligned_records"]:
            if all(field in record for field in lineage_fields):
                lineage_complete += 1
            start = int(record["record_evidence_parent_start_char"])
            end = int(record["record_evidence_parent_end_char"])
            if parent_text[start:end] == record["record_evidence"]:
                evidence_exact += 1
        for entry in projection["withheld_records"]:
            withheld_reasons[str(entry["reason"])] += 1

    parent_summaries = [
        round4_parent_coverage(
            parent_id, stage_a_by_parent.get(parent_id, []), projections
        )
        for parent_id, projections in sorted(projections_by_parent.items())
    ]
    aligned_total = sum(
        item["aligned_record_count"] for item in parent_summaries
    )
    withheld_total = sum(
        item["withheld_record_count"] for item in parent_summaries
    )
    unresolved_total = sum(
        len(item["unresolved_target_blocks"]) for item in parent_summaries
    )
    anchors = sum(
        int(projection["model_anchor_count"])
        for projections in projections_by_parent.values()
        for projection in projections
    )
    stage_a_parsed = sum(
        1 for row in stage_a_rows if row.get("parse_status") == "parsed"
    )
    return {
        "version": "candidate_endorsement_round4_release_replay_v1",
        "scope": scope,
        "artifact_scope": artifact_scope,
        "development_scope": scope == "development",
        "release_authorized": False,
        "model_calls": 0,
        "round4_production_contract_sha256": get_round4_production_contract()[
            "round4_production_contract_sha256"
        ],
        "stage_a_input_manifest_sha256": stage_a_manifest_sha256,
        "stage_b_input_manifest_sha256": stage_b_manifest_sha256,
        # Stage A covers every sampled parent.  Stage B covers only the
        # parents that hold at least one Stage-A anchor.
        "stage_a_parents": len({
            str(batch["parent_document_id"]) for batch in stage_a_batches
        }),
        "stage_b_parents": len(parents),
        "stage_a_batches": len(stage_a_batches),
        "stage_a_responses": len(stage_a_rows),
        "stage_a_parsed": stage_a_parsed,
        "stage_b_batches": len(stage_b_batches),
        "stage_b_responses": len(stage_b_rows),
        "stage_b_anchors": anchors,
        "failure_classes": dict(sorted(classes.items())),
        "aligned_records": aligned_total,
        "withheld_records": withheld_total,
        "unresolved_target_blocks": unresolved_total,
        "optional_field_repairs": sum(
            len(item["optional_field_repairs"]) for item in parent_summaries
        ),
        "withheld_reasons": dict(sorted(withheld_reasons.items())),
        "records_with_complete_lineage": lineage_complete,
        "records_with_exact_parent_evidence": evidence_exact,
        "model_records_returned": model_record_total,
        "model_records_accounted": aligned_total + withheld_total,
        "silently_dropped_model_records": model_record_total
        - (aligned_total + withheld_total),
        "parents_complete": sum(
            1 for item in parent_summaries if item["public_coverage_complete"]
        ),
        "accounted_outputs": accounted_outputs,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=REPO)
    parser.add_argument(
        "--scope", choices=sorted(SCOPES), required=True,
        help="the artifact scope of the replayed outputs",
    )
    parser.add_argument("--stage-a-input-dir", type=Path)
    parser.add_argument("--stage-a-output-dir", type=Path)
    parser.add_argument("--stage-b-input-dir", type=Path)
    parser.add_argument("--stage-b-output-dir", type=Path)
    parser.add_argument(
        "--expected-stage-a-input-manifest-sha256",
        default=STAGE_A_INPUT_MANIFEST_SHA256,
    )
    parser.add_argument(
        "--expected-stage-b-input-manifest-sha256",
        default=STAGE_B_INPUT_MANIFEST_SHA256,
    )
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()

    repo = args.repo
    report = replay(
        repo,
        scope=args.scope,
        stage_a_input=args.stage_a_input_dir or repo / STAGE_A_INPUT,
        stage_a_output=args.stage_a_output_dir or repo / STAGE_A_OUTPUT,
        stage_b_input=args.stage_b_input_dir or repo / STAGE_B_INPUT,
        stage_b_output=args.stage_b_output_dir or repo / STAGE_B_OUTPUT,
        stage_a_manifest_sha256=args.expected_stage_a_input_manifest_sha256,
        stage_b_manifest_sha256=args.expected_stage_b_input_manifest_sha256,
    )
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
