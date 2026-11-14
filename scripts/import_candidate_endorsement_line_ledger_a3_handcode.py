#!/usr/bin/env python3
"""Import exact human A3 decisions and replay the unchanged Stage-A2 gates."""

from __future__ import annotations

import argparse
import copy
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_candidate_endorsement_line_ledger_a3_handcode import (
    A3_SPEC_SHA256,
    FROZEN_A3_TASK_SHA256,
    FROZEN_A3_RESULTS_SHA256,
    build_a3_provenance,
    derive_a3_residuals,
    sha256_file,
)
from scripts.evaluate_candidate_endorsement_line_ledger_pilot import (
    evaluate_stage_a2_only,
    evaluate_stage_a2_only_observations,
    replay_stage_a2_merge,
)
from src.candidate_endorsement_line_ledger import (
    ANCHOR_ROLES,
    continuation_ownership_report,
    sha256_json,
)


ALLOWED_FINAL_ROLES = {"navigation_form_footer", "nonrecord_content"}


def validate_task_provenance(
    task: Mapping[str, Any], expected_provenance: Mapping[str, Any]
) -> None:
    if task.get("provenance") != expected_provenance:
        raise ValueError("Stage-A3 task provenance replay differs")


def validate_results(
    task: Mapping[str, Any], results: Mapping[str, Any]
) -> dict[str, dict[str, Any]]:
    if set(results) != {
        "task_id", "format", "exported_at", "n_items", "n_complete", "responses"
    }:
        raise ValueError("Stage-A3 results field set differs")
    items = task.get("items")
    if not isinstance(items, list):
        raise ValueError("Stage-A3 task items are invalid")
    expected_ids = [str(item["id"]) for item in items]
    if (
        results.get("task_id") != task.get("task_id")
        or results.get("format") != "handcode-results-v1"
        or type(results.get("n_items")) is not int
        or results.get("n_items") != len(expected_ids)
        or type(results.get("n_complete")) is not int
        or results.get("n_complete") != len(expected_ids)
        or not isinstance(results.get("exported_at"), str)
        or not results["exported_at"]
    ):
        raise ValueError("Stage-A3 results identity or completion differs")
    responses = results.get("responses")
    if not isinstance(responses, list) or len(responses) != len(expected_ids):
        raise ValueError("Stage-A3 result coverage differs")
    residual_by_id = {
        str(item["id"]): item["residual"] for item in items
    }
    decisions: dict[str, dict[str, Any]] = {}
    for expected_id, response in zip(expected_ids, responses, strict=True):
        if not isinstance(response, Mapping) or set(response) != {
            "item_id", "status", "answers"
        }:
            raise ValueError("Stage-A3 response shape differs")
        answers = response["answers"]
        if (
            response["item_id"] != expected_id
            or response["status"] != "complete"
            or not isinstance(answers, Mapping)
            or set(answers) - {
                "resolution", "final_role", "anchor_source_line_id", "notes"
            }
            or ("notes" in answers and not isinstance(answers["notes"], str))
        ):
            raise ValueError("Stage-A3 typed response differs")
        resolution = answers.get("resolution")
        if resolution == "terminal":
            if (
                answers.get("final_role") not in ALLOWED_FINAL_ROLES
                or "anchor_source_line_id" in answers
            ):
                raise ValueError("Stage-A3 terminal response differs")
            decisions[expected_id] = {
                "resolution": "terminal",
                "final_role": str(answers["final_role"]),
                "anchor_source_line_id": None,
            }
        elif resolution == "attach":
            anchor_id = answers.get("anchor_source_line_id")
            if (
                "final_role" in answers
                or anchor_id not in residual_by_id[expected_id][
                    "eligible_final_anchor_source_line_ids"
                ]
            ):
                raise ValueError("Stage-A3 attachment response differs")
            decisions[expected_id] = {
                "resolution": "attach",
                "final_role": None,
                "anchor_source_line_id": str(anchor_id),
            }
        else:
            raise ValueError("Stage-A3 resolution differs")
    return decisions


def merge_human_a3(
    parents: Sequence[Mapping[str, Any]],
    stage_a2_merge: Mapping[str, Any],
    residuals: Sequence[Mapping[str, Any]],
    decisions: Mapping[str, Mapping[str, Any]],
    *,
    task_sha256: str,
    results_sha256: str,
) -> dict[str, Any]:
    expected = {str(row["source_line_id"]): row for row in residuals}
    if set(decisions) != set(expected):
        raise ValueError("Stage-A3 decision coverage differs")
    final = copy.deepcopy(stage_a2_merge["final_decisions"])
    provenance = copy.deepcopy(stage_a2_merge["per_line_provenance"])
    line_by_id = {
        str(line["source_line_id"]): line
        for parent in parents for line in parent["lines"]
    }
    for line_id, decision in decisions.items():
        if decision.get("resolution") == "terminal":
            final_role = decision.get("final_role")
            owner = None
            if final_role not in ALLOWED_FINAL_ROLES:
                raise ValueError("Stage-A3 final role is invalid")
        elif decision.get("resolution") == "attach":
            owner = decision.get("anchor_source_line_id")
            if owner not in expected[line_id][
                "eligible_final_anchor_source_line_ids"
            ]:
                raise ValueError("Stage-A3 attachment owner is invalid")
            source_index = int(line_by_id[line_id]["parent_line_index"])
            owner_index = int(line_by_id[str(owner)]["parent_line_index"])
            final_role = (
                "record_continuation_next"
                if source_index < owner_index else "record_continuation_previous"
            )
        else:
            raise ValueError("Stage-A3 decision is invalid")
        final[line_id].update({
            "role": final_role,
            "continuation_owner_source_line_id": owner,
            "stage_a3_human_adjudication_applied": True,
            "stage_a3_final_role": final_role,
        })
        provenance[line_id]["stage_a3_human_adjudication"] = {
            "a3_spec_sha256": A3_SPEC_SHA256,
            "task_sha256": task_sha256,
            "results_sha256": results_sha256,
            "residual_reason": expected[line_id]["residual_reason"],
            "prior_final_role": expected[line_id]["stage_a2_final_role"],
            "final_role": final_role,
            "continuation_owner_source_line_id": owner,
        }
        provenance[line_id]["final_role"] = final_role
        provenance[line_id]["continuation_owner_source_line_id"] = owner

    ownership: dict[str, str] = {}
    reports = []
    unowned = duplicate = invalid = 0
    for parent in parents:
        report = continuation_ownership_report(parent, final)
        reports.append(report)
        for line_id, owner in report["ownership_by_continuation"].items():
            if line_id in ownership and ownership[line_id] != owner:
                duplicate += 1
            ownership[line_id] = owner
        unowned += int(report["unowned_continuation_count"])
        duplicate += int(report["duplicate_continuation_owner_count"])
        invalid += int(report["invalid_continuation_owner_count"])
    base_validation = stage_a2_merge["validation"]
    validation = {
        **dict(base_validation),
        "stage_a3_expected_residual_count": len(expected),
        "stage_a3_adjudicated_residual_count": len(decisions),
        "stage_a3_coverage_complete": len(decisions) == len(expected),
        "unowned_continuation_count": unowned,
        "duplicate_continuation_owner_count": duplicate,
        "ownership_conflict_count": unowned + duplicate + invalid,
    }
    validation["validation_passed"] = bool(
        validation["coverage_complete"]
        and validation["unresolved_line_count"] == 0
        and validation["withheld_line_count"] == 0
        and validation["stage_a3_coverage_complete"]
        and validation["ownership_conflict_count"] == 0
    )
    return {
        "final_decisions": final,
        "ownership_by_continuation": dict(sorted(ownership.items())),
        "ownership_reports": reports,
        "validation": validation,
        "per_line_provenance": provenance,
        "final_decisions_sha256": sha256_json(final),
        "per_line_provenance_sha256": sha256_json(provenance),
        "stage_a3_decisions_sha256": sha256_json(dict(sorted(decisions.items()))),
    }


def unchanged_gate_replay(
    replayed: Mapping[str, Any], merged: Mapping[str, Any]
) -> dict[str, Any]:
    final_roles = {
        str(line_id): str(value["role"])
        for line_id, value in merged["final_decisions"].items()
    }
    anchor_counts = {}
    for parent in replayed["parents"]:
        parent_id = str(parent["parent_document_id"])
        anchor_counts[parent_id] = sum(
            final_roles.get(str(line["source_line_id"])) in ANCHOR_ROLES
            for line in parent["lines"]
        )
    observations = {
        "stage_a2_merge_validation": dict(merged["validation"]),
        "candidate_self_exception_line_ids": sorted(
            line_id for line_id, decision in replayed["stage_a_decisions"].items()
            if decision.get("role") == "candidate_self"
        ),
        "final_roles": final_roles,
        "ownership_by_continuation": dict(merged["ownership_by_continuation"]),
        "anchor_counts_by_parent": anchor_counts,
        "final_role_counts": dict(sorted(Counter(final_roles.values()).items())),
    }
    return {
        "evaluation": evaluate_stage_a2_only_observations(observations),
        "observations": observations,
    }


def human_a3_import_artifact_payload(
    *,
    task_sha256: str,
    results_sha256: str,
    results_exported_at: str,
    residual_count: int,
    decisions: Mapping[str, Mapping[str, Any]],
    stage_a2_merge: Mapping[str, Any],
    stage_a3_merge: Mapping[str, Any],
    gate_replay: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "artifact_version": "candidate_endorsement_line_ledger_human_a3_v1",
        "a3_spec_sha256": A3_SPEC_SHA256,
        "task_sha256": task_sha256,
        "results_sha256": results_sha256,
        "results_exported_at": results_exported_at,
        "residual_count": residual_count,
        "decisions": dict(sorted(decisions.items())),
        "stage_a2_final_decisions_sha256": stage_a2_merge[
            "final_decisions_sha256"
        ],
        "stage_a3_final_decisions_sha256": stage_a3_merge[
            "final_decisions_sha256"
        ],
        "stage_a3_per_line_provenance_sha256": stage_a3_merge[
            "per_line_provenance_sha256"
        ],
        "stage_a3_decisions_sha256": stage_a3_merge[
            "stage_a3_decisions_sha256"
        ],
        "merge_validation": stage_a3_merge["validation"],
        "unchanged_stage_a2_only_gate_replay": gate_replay,
        "stage_b_authorized": gate_replay["gate_decision"] == "GO",
    }


def import_results(args: argparse.Namespace) -> dict[str, Any]:
    actual_task_sha256 = sha256_file(args.task_json)
    if actual_task_sha256 != FROZEN_A3_TASK_SHA256:
        raise ValueError("Stage-A3 task differs from frozen reviewed bytes")
    if actual_task_sha256 != args.expected_task_sha256:
        raise ValueError("Stage-A3 task bytes differ from expected SHA-256")
    task = json.loads(args.task_json.read_text())
    if task.get("provenance", {}).get("a3_spec_sha256") != A3_SPEC_SHA256:
        raise ValueError("Stage-A3 task spec differs")
    if sha256_file(args.results_json) != FROZEN_A3_RESULTS_SHA256:
        raise ValueError("Stage-A3 results differ from frozen reviewed bytes")
    results = json.loads(args.results_json.read_text())
    decisions = validate_results(task, results)
    replayed = replay_stage_a2_merge(
        args.stage_a_export_dir, args.stage_a_output_dir,
        args.stage_a2_export_dir, args.stage_a2_output_dir, args.full_export_dir,
    )
    evaluation = evaluate_stage_a2_only(
        args.stage_a_export_dir, args.stage_a_output_dir,
        args.stage_a2_export_dir, args.stage_a2_output_dir, args.full_export_dir,
    )
    stored_evaluation = json.loads(args.stage_a2_evaluation_report.read_text())
    if stored_evaluation != evaluation:
        raise ValueError("Stage-A3 Stage-A2 evaluation report replay differs")
    residuals = derive_a3_residuals(replayed, evaluation)
    expected_provenance = build_a3_provenance(
        replayed,
        residuals,
        stage_a2_export_dir=args.stage_a2_export_dir,
        stage_a2_evaluation_report=args.stage_a2_evaluation_report,
    )
    validate_task_provenance(task, expected_provenance)
    task_residuals = [item.get("residual") for item in task["items"]]
    if task_residuals != residuals:
        raise ValueError("Stage-A3 task residual replay differs")
    merged = merge_human_a3(
        replayed["parents"], replayed["stage_a2_merge"], residuals, decisions,
        task_sha256=actual_task_sha256,
        results_sha256=sha256_file(args.results_json),
    )
    gates = unchanged_gate_replay(replayed, merged)
    output = human_a3_import_artifact_payload(
        task_sha256=actual_task_sha256,
        results_sha256=sha256_file(args.results_json),
        results_exported_at=results["exported_at"],
        residual_count=len(residuals),
        decisions=decisions,
        stage_a2_merge=replayed["stage_a2_merge"],
        stage_a3_merge=merged,
        gate_replay=gates["evaluation"],
    )
    args.output_json.parent.mkdir(parents=True, exist_ok=True)
    if args.output_json.exists():
        raise FileExistsError(args.output_json)
    args.output_json.write_text(json.dumps(output, indent=2, sort_keys=True) + "\n")
    return output


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-json", type=Path, required=True)
    parser.add_argument("--expected-task-sha256", required=True)
    parser.add_argument("--results-json", type=Path, required=True)
    parser.add_argument("--stage-a-export-dir", type=Path, required=True)
    parser.add_argument("--stage-a-output-dir", type=Path, required=True)
    parser.add_argument("--stage-a2-export-dir", type=Path, required=True)
    parser.add_argument("--stage-a2-output-dir", type=Path, required=True)
    parser.add_argument("--stage-a2-evaluation-report", type=Path, required=True)
    parser.add_argument("--full-export-dir", type=Path, required=True)
    parser.add_argument("--output-json", type=Path, required=True)
    args = parser.parse_args()
    output = import_results(args)
    print(json.dumps(output, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
