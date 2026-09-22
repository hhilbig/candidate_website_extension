"""Authenticate and merge human exact-source repairs for Stage-B job 21470382."""
from __future__ import annotations

import argparse
import copy
import datetime as dt
import json
import re
import sys
from pathlib import Path
from typing import Any, Mapping

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts.build_candidate_endorsement_stage_b_residual_handcode import (
    SPEC_SHA256, build_task, file_sha256, load_frozen_failed_run,
)
from src.candidate_endorsement_line_ledger import (
    align_record_extraction_output,
    get_line_ledger_contract,
    sha256_json,
    validate_record_extraction_output,
)

FROZEN_TASK_SHA256 = "3ce65abdfd56bb8783bb9349df1eb6c6201934cedd7605dee3f3a3ecc92f8fef"
FROZEN_HTML_SHA256 = "f7f816a879bc5f19c0845c3d4345401e141b8bd0444999f87f3653a49dbf629c"


def validate_task(
    task_path: Path, html_path: Path, export_dir: Path, output_dir: Path,
) -> dict[str, Any]:
    if file_sha256(task_path) != FROZEN_TASK_SHA256:
        raise ValueError("frozen Stage-B residual task bytes differ")
    if file_sha256(html_path) != FROZEN_HTML_SHA256:
        raise ValueError("frozen Stage-B residual HTML bytes differ")
    task = json.loads(task_path.read_text())
    expected = build_task(export_dir, output_dir)
    if task != expected:
        raise ValueError("Stage-B residual task does not replay")
    if task["provenance"]["spec_sha256"] != SPEC_SHA256:
        raise ValueError("Stage-B residual task spec differs")
    return task


def validate_results(task: Mapping[str, Any], results: Mapping[str, Any]) -> dict[str, str | None]:
    exported_at = results.get("exported_at")
    timestamp_is_canonical = False
    if isinstance(exported_at, str) and re.fullmatch(
        r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z", exported_at
    ):
        try:
            parsed = dt.datetime.strptime(exported_at, "%Y-%m-%dT%H:%M:%S.%fZ")
            timestamp_is_canonical = (
                parsed.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z" == exported_at
            )
        except ValueError:
            timestamp_is_canonical = False
    if set(results) != {
        "task_id", "title", "format", "exported_at", "n_items", "n_complete",
        "responses",
    }:
        raise ValueError("Stage-B residual results field set differs")
    if (
        results["task_id"] != task["task_id"]
        or results["title"] != task["title"]
        or results["format"] != "handcode-results-v1"
        or type(results["n_items"]) is not int
        or results["n_items"] != len(task["items"])
        or type(results["n_complete"]) is not int
        or results["n_complete"] != len(task["items"])
        or not timestamp_is_canonical
        or not isinstance(results["responses"], list)
        or len(results["responses"]) != len(task["items"])
    ):
        raise ValueError("Stage-B residual results coverage differs")
    items = {item["id"]: item for item in task["items"]}
    if [response.get("item_id") if isinstance(response, Mapping) else None
        for response in results["responses"]] != [item["id"] for item in task["items"]]:
        raise ValueError("Stage-B residual response order differs")
    decisions: dict[str, str | None] = {}
    for response in results["responses"]:
        if not isinstance(response, Mapping) or set(response) != {
            "item_id", "status", "answers"
        } or response["status"] != "complete":
            raise ValueError("Stage-B residual response shape differs")
        residual_id = response["item_id"]
        if residual_id not in items or residual_id in decisions:
            raise ValueError("Stage-B residual response identity differs")
        answers = response["answers"]
        if (
            not isinstance(answers, Mapping)
            or set(answers) != {"exact_source_value", "notes"}
            or not isinstance(answers["notes"], str)
        ):
            raise ValueError("Stage-B residual answer shape differs")
        value = answers["exact_source_value"]
        residual = items[residual_id]["residual"]
        allowed_values = items[residual_id].get("allowed_values")
        if not isinstance(allowed_values, list) or value not in allowed_values:
            raise ValueError("Stage-B residual value is not an enumerated semantic option")
        if value is None and residual["field"] == "endorser_name":
            raise ValueError("endorser_name residual cannot be NULL")
        if value is not None and (not isinstance(value, str) or not value):
            raise ValueError("Stage-B residual exact source value has wrong type")
        decisions[residual_id] = value
    if set(decisions) != set(items):
        raise ValueError("Stage-B residual results omit task items")
    for group in task.get("compatibility_groups", []):
        if not isinstance(group, Mapping) or set(group) != {
            "item_ids", "allowed_combinations"
        }:
            raise ValueError("Stage-B residual compatibility group shape differs")
        item_ids = group["item_ids"]
        combinations = group["allowed_combinations"]
        if (
            not isinstance(item_ids, list)
            or not isinstance(combinations, list)
            or not combinations
            or not any(
                isinstance(combo, Mapping)
                and set(combo) == set(item_ids)
                and all(decisions[item_id] == combo[item_id] for item_id in item_ids)
                for combo in combinations
            )
        ):
            raise ValueError("Stage-B residual paired semantic choices differ")
    return decisions


def merge_repairs(
    task: Mapping[str, Any], decisions: Mapping[str, str | None],
    batches: list[dict[str, Any]], rows: list[dict[str, Any]],
) -> dict[str, Any]:
    repaired_outputs = {
        row["batch_id"]: copy.deepcopy(row["model_output"]) for row in rows
    }
    for item in task["items"]:
        residual = item["residual"]
        repaired_outputs[residual["batch_id"]][residual["slot"]]["records"][
            residual["record_index"]
        ][residual["field"]] = decisions[item["id"]]
    validations: dict[str, Any] = {}
    alignments: dict[str, Any] = {}
    for batch in batches:
        batch_id = batch["batch_id"]
        output = repaired_outputs[batch_id]
        validation = validate_record_extraction_output(batch, output)
        if validation["schema_valid"] is not True or validation["coverage_complete"] is not True:
            raise ValueError(f"repaired Stage-B batch remains invalid: {batch_id}: {validation['errors']}")
        alignment = align_record_extraction_output(batch, output)
        if (
            alignment["public_coverage_complete"] is not True
            or alignment["withheld_record_count"] != 0
        ):
            raise ValueError(f"repaired Stage-B batch alignment is incomplete: {batch_id}")
        validations[batch_id] = validation
        alignments[batch_id] = alignment
    ordered_outputs = [repaired_outputs[batch["batch_id"]] for batch in batches]
    return {
        "repaired_outputs": repaired_outputs,
        "validations": validations,
        "alignments": alignments,
        "repaired_model_outputs_sha256": sha256_json(ordered_outputs),
        "repaired_alignments_sha256": sha256_json([
            alignments[batch["batch_id"]] for batch in batches
        ]),
        "technical_pass_batch_count": len(batches),
        "withheld_record_count": 0,
        "public_coverage_complete": True,
    }


def import_artifact(
    *, export_dir: Path, output_dir: Path, task_path: Path, html_path: Path,
    results_path: Path,
) -> dict[str, Any]:
    task = validate_task(task_path, html_path, export_dir, output_dir)
    results = json.loads(results_path.read_text())
    decisions = validate_results(task, results)
    _, batches, rows = load_frozen_failed_run(export_dir, output_dir)
    merged = merge_repairs(task, decisions, batches, rows)
    return {
        "version": "candidate_endorsement_stage_b_residual_import_v1",
        "task_sha256": file_sha256(task_path),
        "html_sha256": file_sha256(html_path),
        "results_sha256": file_sha256(results_path),
        "results_exported_at": results["exported_at"],
        "source_line_ledger_contract_sha256": task["provenance"][
            "line_ledger_contract_sha256"
        ],
        "repair_validation_contract_sha256": get_line_ledger_contract()[
            "line_ledger_contract_sha256"
        ],
        "decision_count": len(decisions),
        "decisions_sha256": sha256_json(decisions),
        "decisions": decisions,
        "repaired_model_outputs_sha256": merged["repaired_model_outputs_sha256"],
        "repaired_alignments_sha256": merged["repaired_alignments_sha256"],
        "technical_pass_batch_count": merged["technical_pass_batch_count"],
        "withheld_record_count": 0,
        "public_coverage_complete": True,
        "pilot_gate_evaluation_authorized": True,
        "full_expansion_or_release_authorized": False,
    }


def replay_import_artifact(
    *, export_dir: Path, output_dir: Path, task_path: Path, html_path: Path,
    results_path: Path, artifact_path: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    expected = import_artifact(
        export_dir=export_dir, output_dir=output_dir, task_path=task_path,
        html_path=html_path, results_path=results_path,
    )
    if json.loads(artifact_path.read_text()) != expected:
        raise ValueError("stored Stage-B residual import artifact differs")
    task = validate_task(task_path, html_path, export_dir, output_dir)
    decisions = validate_results(task, json.loads(results_path.read_text()))
    _, batches, rows = load_frozen_failed_run(export_dir, output_dir)
    merged = merge_repairs(task, decisions, batches, rows)
    repaired_rows = []
    for batch, original in zip(batches, rows):
        row = copy.deepcopy(original)
        batch_id = batch["batch_id"]
        row["model_output"] = merged["repaired_outputs"][batch_id]
        row["model_output_sha256"] = sha256_json(row["model_output"])
        row["stage_validation"] = merged["validations"][batch_id]
        row["stage_alignment"] = merged["alignments"][batch_id]
        row["parse_status"] = "parsed"
        row["parse_error"] = None
        row["technical_pass"] = True
        repaired_rows.append(row)
    return batches, repaired_rows, expected


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage-b-export-dir", type=Path, required=True)
    parser.add_argument("--stage-b-output-dir", type=Path, required=True)
    parser.add_argument("--task-json", type=Path, required=True)
    parser.add_argument("--html", type=Path, required=True)
    parser.add_argument("--results-json", type=Path, required=True)
    parser.add_argument("--output-artifact", type=Path, required=True)
    args = parser.parse_args()
    artifact = import_artifact(
        export_dir=args.stage_b_export_dir, output_dir=args.stage_b_output_dir,
        task_path=args.task_json, html_path=args.html,
        results_path=args.results_json,
    )
    args.output_artifact.write_text(json.dumps(artifact, indent=2, sort_keys=True) + "\n")
    print(json.dumps({"artifact": str(args.output_artifact), "decisions": 23}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
