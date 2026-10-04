#!/usr/bin/env python3
"""Authenticate the bounded round-two Stage-B run before human scoring."""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Mapping

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts.hive.extract_candidate_endorsement_line_ledger_vllm import (  # noqa: E402
    file_sha256,
    load_validated_stage_run,
)
from src.candidate_endorsement_line_ledger import (  # noqa: E402
    RECORD_EXTRACTION_STAGE,
    RECALL40_ROUND2_STAGE_B_SCOPE_SPEC,
    get_frozen_recall40_round2_stage_b_contract,
    sha256_json,
)


INPUT_EXPORT = REPO / (
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round2_stage_b_preflight_20260902a"
)
INPUT_MANIFEST_SHA256 = (
    "66aabcca08a1d54a644d2399827b0dba1b56b829e225dc79f23958a6a80788e9"
)
EXPECTED_GENERATION_CONFIG_SHA256 = (
    "dd20bca47b5a15b79864fcccdaa290a59c9463dead2ce257501f414f9b50b12f"
)


def _canonical_report(
    manifest: Mapping[str, Any],
    rows: list[dict[str, Any]],
    artifacts: Mapping[str, Any],
) -> dict[str, Any]:
    if len(rows) != RECALL40_ROUND2_STAGE_B_SCOPE_SPEC["stage_b_batch_count"]:
        raise ValueError("round-two Stage-B output row count differs")
    batch_ids = [str(row.get("batch_id")) for row in rows]
    if len(batch_ids) != len(set(batch_ids)):
        raise ValueError("round-two Stage-B output batch IDs are duplicated")
    aligned_records: list[dict[str, Any]] = []
    parent_counts: dict[str, int] = {}
    for row in rows:
        validation = row.get("stage_validation")
        alignment = row.get("stage_alignment")
        if (
            row.get("technical_pass") is not True
            or not isinstance(validation, Mapping)
            or validation.get("schema_valid") is not True
            or validation.get("coverage_complete") is not True
            or validation.get("errors") != []
            or not isinstance(alignment, Mapping)
            or alignment.get("coverage_complete") is not True
            or alignment.get("withheld_records") != []
            or alignment.get("withheld_record_count") != 0
            or not isinstance(alignment.get("aligned_records"), list)
        ):
            raise ValueError("round-two Stage-B row is not technically releasable")
        parent_id = str(row.get("parent_document_id"))
        records = alignment["aligned_records"]
        parent_counts[parent_id] = parent_counts.get(parent_id, 0) + len(records)
        aligned_records.extend(dict(record) for record in records)
    selected_parent_ids = manifest.get("selected_parent_ids")
    if (
        not isinstance(selected_parent_ids, list)
        or len(selected_parent_ids) != 40
        or set(parent_counts) - set(selected_parent_ids)
    ):
        raise ValueError("round-two Stage-B parent coverage differs")
    return {
        "version": "candidate_endorsement_recall40_round2_stage_b_gate_v1",
        "gate_decision": "GO_BUILD_PREDICTION_BLIND_HUMAN_TRUTH_TASK",
        "input_manifest_sha256": INPUT_MANIFEST_SHA256,
        "generation_config_sha256": EXPECTED_GENERATION_CONFIG_SHA256,
        "line_ledger_contract_sha256": manifest[
            "line_ledger_contract_sha256"
        ],
        "validated_batch_count": len(rows),
        "selected_parent_count": len(selected_parent_ids),
        "parents_with_aligned_records": len(parent_counts),
        "aligned_record_count": len(aligned_records),
        "withheld_record_count": 0,
        "ordered_output_batch_ids_sha256": sha256_json(batch_ids),
        "aligned_records_sha256": sha256_json(aligned_records),
        "aligned_record_counts_by_parent_sha256": sha256_json(
            dict(sorted(parent_counts.items()))
        ),
        "run_artifacts_sha256": sha256_json(dict(artifacts)),
        "prediction_blind_human_task_authorized": True,
        "full_private_inference_authorized": False,
        "release_authorized": False,
    }


def evaluate(output_dir: Path) -> dict[str, Any]:
    manifest_path = INPUT_EXPORT / "manifest.json"
    if file_sha256(manifest_path) != INPUT_MANIFEST_SHA256:
        raise ValueError("frozen round-two Stage-B manifest bytes differ")
    manifest, _batches, rows, artifacts = load_validated_stage_run(
        INPUT_EXPORT,
        output_dir,
        stage=RECORD_EXTRACTION_STAGE,
        contract_override=get_frozen_recall40_round2_stage_b_contract(),
    )
    if artifacts.get("generation_config_sha256") != (
        EXPECTED_GENERATION_CONFIG_SHA256
    ):
        raise ValueError("round-two Stage-B generation identity differs")
    return _canonical_report(manifest, rows, artifacts)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args()
    if args.report.exists():
        raise ValueError("report path must not already exist")
    report = evaluate(args.output_dir)
    args.report.parent.mkdir(parents=True, exist_ok=True)
    args.report.write_text(
        json.dumps(report, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n",
        encoding="utf-8",
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
