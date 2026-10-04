#!/usr/bin/env python3
"""Reproduce and preserve the failed recall40 round-two Stage-B pilot."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path
from types import SimpleNamespace
from typing import Any

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts.evaluate_candidate_endorsement_recall40_round2_stage_b import (  # noqa: E402
    EXPECTED_GENERATION_CONFIG_SHA256,
    INPUT_EXPORT,
    INPUT_MANIFEST_SHA256,
)
from scripts.hive.extract_candidate_endorsement_line_ledger_vllm import (  # noqa: E402
    _derive_output_row,
    file_sha256,
    json_value_equal_typed,
    read_gzip_jsonl,
    validate_input_manifest,
)
from src.candidate_endorsement_line_ledger import (  # noqa: E402
    RECORD_EXTRACTION_STAGE,
    get_frozen_recall40_round2_stage_b_contract,
    sha256_json,
)


def _inventory(path: Path) -> list[dict[str, Any]]:
    return [
        {"file": item.name, "bytes": item.stat().st_size, "sha256": file_sha256(item)}
        for item in sorted(path.iterdir())
        if item.is_file()
    ]


def build_audit(output_dir: Path) -> dict[str, Any]:
    contract = get_frozen_recall40_round2_stage_b_contract()
    if file_sha256(INPUT_EXPORT / "manifest.json") != INPUT_MANIFEST_SHA256:
        raise ValueError("frozen Stage-B input manifest differs")
    manifest, _payload, input_paths = validate_input_manifest(
        INPUT_EXPORT / "manifest.json",
        INPUT_EXPORT / "chunks",
        stage=RECORD_EXTRACTION_STAGE,
        contract=contract,
    )
    expected_names = {
        *(path.name.replace(".jsonl.gz", ".out.jsonl.gz") for path in input_paths),
        *(f"run_metadata_shard_{index:05d}.json" for index in range(len(input_paths))),
    }
    actual_names = {path.name for path in output_dir.iterdir() if path.is_file()}
    if actual_names != expected_names or any(path.is_dir() for path in output_dir.iterdir()):
        raise ValueError("failed-run output inventory differs")

    args = SimpleNamespace(max_model_len=32768, max_output_tokens=16384)
    output_rows: list[dict[str, Any]] = []
    input_rows: list[dict[str, Any]] = []
    rows_by_chunk: list[list[dict[str, Any]]] = []
    for input_path in input_paths:
        batches = read_gzip_jsonl(input_path)
        output_path = output_dir / input_path.name.replace(".jsonl.gz", ".out.jsonl.gz")
        rows = read_gzip_jsonl(output_path)
        if len(rows) != len(batches):
            raise ValueError("failed-run chunk row count differs")
        for batch, row in zip(batches, rows):
            expected = _derive_output_row(
                batch=batch,
                raw_output=row["raw_output"],
                finish_reason=row["finish_reason"],
                stop_reason=row["stop_reason"],
                output_tokens=row["output_tokens"],
                prompt_tokens=row["prompt_tokens_used"],
                args=args,
                contract=contract,
                input_manifest_sha256=INPUT_MANIFEST_SHA256,
                generation_hash=EXPECTED_GENERATION_CONFIG_SHA256,
            )
            if not json_value_equal_typed(row, expected):
                raise ValueError(f"failed-run row does not rederive: {row.get('batch_id')}")
        rows_by_chunk.append(rows)
        input_rows.extend(batches)
        output_rows.extend(rows)
    if [row["batch_id"] for row in output_rows] != [row["batch_id"] for row in input_rows]:
        raise ValueError("failed-run output order differs")

    metadata_jobs: list[dict[str, Any]] = []
    for index, rows in enumerate(rows_by_chunk):
        path = output_dir / f"run_metadata_shard_{index:05d}.json"
        metadata = json.loads(path.read_text(encoding="utf-8"))
        statuses = dict(sorted(Counter(row["parse_status"] for row in rows).items()))
        finishes = dict(sorted(Counter(row["finish_reason"] for row in rows).items()))
        if (
            metadata.get("shard_index") != index
            or metadata.get("shard_count") != len(input_paths)
            or metadata.get("selected_batches") != len(rows)
            or metadata.get("processed_outputs_this_run") != len(rows)
            or metadata.get("technical_pass_outputs_this_run")
            != sum(row["technical_pass"] is True for row in rows)
            or metadata.get("parse_status_counts_this_run") != statuses
            or metadata.get("finish_reason_counts_this_run") != finishes
            or metadata.get("input_manifest_sha256") != INPUT_MANIFEST_SHA256
            or metadata.get("generation_config_sha256")
            != EXPECTED_GENERATION_CONFIG_SHA256
        ):
            raise ValueError(f"failed-run metadata does not reconcile: {path.name}")
        metadata_jobs.append({
            "shard_index": index,
            "slurm_job_id": metadata["slurm_job_id"],
            "technical_pass": metadata["technical_pass_outputs_this_run"],
            "parse_status_counts": statuses,
        })

    failed = [row for row in output_rows if row["technical_pass"] is False]
    valid = [row for row in output_rows if row["technical_pass"] is True]
    error_counts: Counter[str] = Counter()
    alignment_counts: Counter[str] = Counter()
    exact_failures: list[dict[str, Any]] = []
    for row in failed:
        errors = list((row.get("stage_validation") or {}).get("errors", []))
        withheld = list((row.get("stage_alignment") or {}).get("withheld_records", []))
        for error in errors:
            error_counts[str(error).rsplit(":", 1)[-1]] += 1
        for item in withheld:
            details = item.get("carrier_alignment_report", {}).get(
                "withheld_item_counts_by_reason", {}
            )
            alignment_counts.update({str(key): int(value) for key, value in details.items()})
        exact_failures.append({
            "batch_id": row["batch_id"],
            "parent_document_id": row["parent_document_id"],
            "parse_status": row["parse_status"],
            "errors": errors,
            "alignment_failure_reasons": [item["reason"] for item in withheld],
        })

    return {
        "artifact_type": "immutable_recall40_round2_stage_b_technical_failure_audit",
        "input_manifest_sha256": INPUT_MANIFEST_SHA256,
        "line_ledger_contract_sha256": manifest["line_ledger_contract_sha256"],
        "generation_config_sha256": EXPECTED_GENERATION_CONFIG_SHA256,
        "output_inventory": _inventory(output_dir),
        "metadata_jobs": metadata_jobs,
        "authenticated_census": {
            "output_rows": len(output_rows),
            "finish_reason_stop": sum(row["finish_reason"] == "stop" for row in output_rows),
            "technical_pass": len(valid),
            "technical_fail": len(failed),
            "invalid_stage_output": sum(row["parse_status"] == "invalid_stage_output" for row in failed),
            "invalid_stage_alignment": sum(row["parse_status"] == "invalid_stage_alignment" for row in failed),
            "valid_row_aligned_records": sum(row["stage_alignment"]["aligned_record_count"] for row in valid),
            "valid_row_withheld_records": sum(row["stage_alignment"]["withheld_record_count"] for row in valid),
            "failed_parent_count": len({row["parent_document_id"] for row in failed}),
            "validation_error_counts": dict(sorted(error_counts.items())),
            "alignment_withheld_reason_counts": dict(sorted(alignment_counts.items())),
        },
        "ordered_output_batch_ids_sha256": sha256_json([row["batch_id"] for row in output_rows]),
        "exact_failures": exact_failures,
        "strict_evaluator_status": "NO_GO_before_prediction_blind_human_scoring",
        "authorization": {
            "blind_retry_authorized": False,
            "prediction_blind_human_truth_task_authorized": True,
            "technical_failure_scoring_only": True,
            "full_private_inference_authorized": False,
            "release_authorized": False,
        },
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--audit", type=Path, required=True)
    args = parser.parse_args()
    if args.audit.exists():
        raise ValueError("audit path must not already exist")
    audit = build_audit(args.output_dir)
    args.audit.parent.mkdir(parents=True, exist_ok=True)
    args.audit.write_text(json.dumps(audit, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(audit["authenticated_census"], indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
