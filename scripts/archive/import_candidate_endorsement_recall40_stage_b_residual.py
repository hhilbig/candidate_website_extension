"""Authenticate confirmations and replay the exact job-21576064 residual merge."""
from __future__ import annotations

import argparse
import copy
import datetime as dt
import hashlib
import json
import re
import sys
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts.build_candidate_endorsement_recall40_stage_b_residual_handcode import (
    FAILED_BATCH_IDS, build_task, authenticated_inputs, proposed_output,
)
from src.candidate_endorsement_line_ledger import (
    RECALL40_STAGE_B_RESIDUAL_PROJECTION_SPEC_SHA256,
    align_record_extraction_output,
    computed_recall40_stage_b_residual_contract,
    sha256_json,
    validate_record_extraction_output,
)

FROZEN_TASK_SHA256 = "b091190a11b373f51a3fccff478f2c88975a05886ac23ffa1e059487ddd7f126"
FROZEN_HTML_SHA256 = "8541bceae1a42aef42e4ad080dcc3b099bfcd4150130d76793be3e67e8f463eb"
CANONICAL_TIMESTAMP = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$")


def sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def strict_load(path: Path) -> Any:
    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise ValueError(f"duplicate JSON key: {key}")
            result[key] = value
        return result
    return json.loads(
        path.read_text(encoding="utf-8"), object_pairs_hook=pairs,
        parse_constant=lambda value: (_ for _ in ()).throw(
            ValueError(f"nonfinite JSON constant: {value}")
        ),
    )


def validate_results(results: Any, task: dict[str, Any]) -> list[dict[str, Any]]:
    if not isinstance(results, dict) or set(results) != {
        "task_id", "format", "exported_at", "n_items", "n_complete", "responses"
    }:
        raise ValueError("results top-level fields differ")
    if results["task_id"] != task["task_id"] or results["format"] != "handcode-results-v1":
        raise ValueError("results task identity differs")
    if type(results["n_items"]) is not int or type(results["n_complete"]) is not int:
        raise TypeError("results counts must be exact integers")
    if results["n_items"] != 7 or results["n_complete"] != 7:
        raise ValueError("all seven residual items must be complete")
    value = results["exported_at"]
    if not isinstance(value, str) or CANONICAL_TIMESTAMP.fullmatch(value) is None:
        raise ValueError("exported_at must be canonical UTC milliseconds")
    try:
        timestamp = dt.datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ").replace(
            tzinfo=dt.timezone.utc
        )
    except (AttributeError, ValueError) as error:
        raise ValueError("exported_at must be canonical UTC milliseconds") from error
    if timestamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" != value:
        raise ValueError("exported_at must round-trip canonically")
    responses = results["responses"]
    if not isinstance(responses, list) or [row.get("item_id") for row in responses] != list(
        FAILED_BATCH_IDS
    ):
        raise ValueError("response order/IDs differ")
    for row in responses:
        if not isinstance(row, dict) or set(row) != {"item_id", "status", "answers"}:
            raise ValueError("response fields differ")
        answers = row["answers"]
        if not isinstance(answers, dict) or not set(answers) <= {
            "approve_exact_tuple", "notes"
        } or answers.get("approve_exact_tuple") != "yes":
            raise ValueError("every exact tuple requires explicit yes confirmation")
        if "notes" in answers and not isinstance(answers["notes"], str):
            raise ValueError("response notes must be strings")
        if row["status"] != "complete":
            raise ValueError("every exact tuple requires explicit yes confirmation")
    return responses


def import_results(task_path: Path, html_path: Path, results_path: Path) -> dict[str, Any]:
    if sha256_file(task_path) != FROZEN_TASK_SHA256 or sha256_file(html_path) != FROZEN_HTML_SHA256:
        raise ValueError("frozen task/app bytes differ")
    task = strict_load(task_path)
    if task != build_task():
        raise ValueError("task does not reproduce from authenticated inputs")
    results = strict_load(results_path)
    validate_results(results, task)
    batches, rows = authenticated_inputs()
    contract = computed_recall40_stage_b_residual_contract()
    merged_rows = []
    repaired_model_output_sha256: dict[str, str] = {}
    aligned_count = withheld_count = 0
    for batch_id, row in rows.items():
        merged = copy.deepcopy(row)
        if row.get("model_output_sha256") != sha256_json(row.get("model_output")):
            raise ValueError(f"stored model-output hash differs: {batch_id}")
        if batch_id not in FAILED_BATCH_IDS:
            if row.get("technical_pass") is not True:
                raise ValueError(f"unexpected non-residual technical failure: {batch_id}")
            merged_rows.append(merged)
            aligned_count += row["stage_alignment"]["aligned_record_count"]
            withheld_count += row["stage_alignment"]["withheld_record_count"]
            continue
        model_output = proposed_output(batch_id, row["model_output"])
        validation = validate_record_extraction_output(
            batches[batch_id], model_output,
            residual_projection_spec_sha256=RECALL40_STAGE_B_RESIDUAL_PROJECTION_SPEC_SHA256,
        )
        alignment = align_record_extraction_output(
            batches[batch_id], model_output,
            line_ledger_contract_sha256=contract["line_ledger_contract_sha256"],
            residual_projection_spec_sha256=RECALL40_STAGE_B_RESIDUAL_PROJECTION_SPEC_SHA256,
        )
        if not validation["schema_valid"] or not alignment["public_coverage_complete"]:
            raise ValueError(f"merged residual replay failed: {batch_id}")
        merged.update({"model_output": model_output, "stage_validation": validation,
                       "stage_alignment": alignment, "technical_pass": True,
                       "model_output_sha256": sha256_json(model_output),
                       "parse_status": "parsed", "parse_error": None})
        repaired_model_output_sha256[batch_id] = merged["model_output_sha256"]
        merged_rows.append(merged)
        aligned_count += alignment["aligned_record_count"]
        withheld_count += alignment["withheld_record_count"]
    return {
        "version": "candidate_endorsement_recall40_stage_b_residual_import_v1",
        "task_sha256": FROZEN_TASK_SHA256,
        "html_sha256": FROZEN_HTML_SHA256,
        "results_sha256": sha256_file(results_path),
        "residual_contract_sha256": contract["line_ledger_contract_sha256"],
        "projection_spec_sha256": RECALL40_STAGE_B_RESIDUAL_PROJECTION_SPEC_SHA256,
        "confirmed_batch_ids": list(FAILED_BATCH_IDS),
        "repaired_model_output_sha256": repaired_model_output_sha256,
        "merged_row_count": len(merged_rows),
        "technical_pass_count": sum(row["technical_pass"] is True for row in merged_rows),
        "aligned_record_count": aligned_count,
        "withheld_record_count": withheld_count,
        "merged_rows_sha256": sha256_json(merged_rows),
        "content_gates_evaluated": False,
        "larger_run_or_release_authorized": False,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task", type=Path, required=True)
    parser.add_argument("--html", type=Path, required=True)
    parser.add_argument("--results", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    artifact = import_results(args.task, args.html, args.results)
    args.output.write_text(json.dumps(artifact, sort_keys=True, separators=(",", ":")) + "\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
