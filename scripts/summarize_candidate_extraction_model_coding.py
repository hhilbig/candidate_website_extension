#!/usr/bin/env python3
"""Summarise MODEL-coded validation answers for the extraction release.

This is an interim quality estimate, not the release gate. The release gate in
`scripts/score_candidate_extraction_handcode.py` accepts human answers only and
writes the bundle's `human_validation.json`. This script never touches the
bundle. It reports the same metrics so the model estimate and the later human
result are comparable.

Usage:
  python scripts/summarize_candidate_extraction_model_coding.py \
    --validation-dir validation/release_validation_2026-09-22 \
    --output quality_reports/candidate_extraction_model_coded_validation_2026-09-22.json
"""
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PRECISION_QUESTIONS = {
    "biography": (
        "fact_present",
        "value_faithful",
        "field_correct",
        "timing_correct",
        "relationship_correct",
        "evidence_supports_fact",
    ),
    "issues": (
        "statement_present",
        "claim_faithful",
        "kind_correct",
        "heading_correct",
        "evidence_supports_statement",
    ),
    "endorsements": (
        "endorser_present",
        "name_correct",
        "entity_category_correct",
        "optional_fields_correct",
        "record_evidence_supports_identity",
        "combined_record_context_support",
    ),
}
ROUTES = ("issues", "biography", "endorsements")
ALL_DIMENSION_GATE = 0.90
SUBTYPE_GATE = 0.80
RECALL_GATE = 0.90
EXPECTED_OMISSIONS = 11


def load_answers(validation_dir: Path, task_id: str) -> dict[str, dict[str, Any]]:
    path = validation_dir / f"{task_id}_results.json"
    if not path.is_file():
        raise SystemExit(f"results are absent: {path}")
    payload = json.loads(path.read_text(encoding="utf-8"))
    if payload.get("format") != "handcode-results-v1":
        raise SystemExit(f"{path} is not a handcode-results-v1 file")
    if payload.get("task_id") != task_id:
        raise SystemExit(f"{path} names task {payload.get('task_id')}")
    answers: dict[str, dict[str, Any]] = {}
    for response in payload.get("responses") or []:
        item_id = str(response.get("item_id"))
        if item_id in answers:
            raise SystemExit(f"{path} answers {item_id} twice")
        answers[item_id] = dict(response.get("answers") or {})
    return answers


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--validation-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--coder", default="claude-opus-5")
    args = parser.parse_args()

    manifest = json.loads(
        (args.validation_dir / "handcode_manifest.json").read_text(encoding="utf-8")
    )
    samples: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for task in manifest["tasks"]:
        answers = load_answers(args.validation_dir, task["task_id"])
        for item in task["sample_items"]:
            row = dict(item)
            row["task_id"] = task["task_id"]
            row["answers"] = answers.get(str(item["item_id"]))
            samples[(task["kind"], str(task.get("route")))].append(row)

    missing = [
        row["item_id"]
        for rows in samples.values()
        for row in rows
        if row["answers"] is None
    ]
    if missing:
        raise SystemExit(f"{len(missing)} items have no answer, first {missing[0]}")

    route_reports: dict[str, Any] = {}
    for route in ROUTES:
        dimensions = PRECISION_QUESTIONS[route]
        correct: list[bool] = []
        per_question: dict[str, list[bool]] = {q: [] for q in dimensions}
        subtype_results: dict[str, list[bool]] = defaultdict(list)
        for row in samples[("precision", route)]:
            answers = row["answers"]
            for question in dimensions:
                per_question[question].append(answers.get(question) == "yes")
            row_correct = all(answers.get(q) == "yes" for q in dimensions)
            correct.append(row_correct)
            subtype_results[str(row["subtype"])].append(row_correct)

        recall_rows = samples[("recall", route)]
        recall_correct = [
            row["answers"].get("all_requested_records_captured") == "yes"
            for row in recall_rows
        ]
        missed_total = 0
        for row in recall_rows:
            missed = row["answers"].get("missed_records") or []
            missed_total += len(missed) if isinstance(missed, list) else 0

        subtype_accuracy = {
            subtype: {"n": len(values), "all_dimension_accuracy": sum(values) / len(values)}
            for subtype, values in sorted(subtype_results.items())
        }
        minimum_subtype = min(
            value["all_dimension_accuracy"] for value in subtype_accuracy.values()
        )
        all_dimension_accuracy = sum(correct) / len(correct)
        complete_page_recall = sum(recall_correct) / len(recall_correct)
        route_reports[route] = {
            "precision_n": len(correct),
            "all_dimension_accurate_n": sum(correct),
            "all_dimension_accuracy": all_dimension_accuracy,
            "per_question_accuracy": {
                question: sum(values) / len(values)
                for question, values in per_question.items()
            },
            "subtype_accuracy": subtype_accuracy,
            "minimum_subtype_accuracy": minimum_subtype,
            "recall_n": len(recall_correct),
            "complete_pages_n": sum(recall_correct),
            "complete_page_recall": complete_page_recall,
            "missed_records_total": missed_total,
            "meets_thresholds": {
                "all_dimension_accuracy": all_dimension_accuracy >= ALL_DIMENSION_GATE,
                "minimum_subtype_accuracy": minimum_subtype >= SUBTYPE_GATE,
                "complete_page_recall": complete_page_recall >= RECALL_GATE,
            },
        }

    omission_confirmations: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in samples[("known_omission", "all")]:
        answers = row["answers"]
        omission_confirmations[str(row["omission_id"])].append(
            {
                "record_id": row["record_id"],
                "semantic_recovery_confirmed": answers.get("semantic_recovery_confirmed") == "yes",
                "recovery_evidence_confirmed": answers.get("recovery_evidence_confirmed") == "yes",
                "pair_confirmed": (
                    answers.get("semantic_recovery_confirmed") == "yes"
                    and answers.get("recovery_evidence_confirmed") == "yes"
                ),
            }
        )
    omission_items = [
        {
            "omission_id": omission_id,
            "confirmed_record_ids": sorted(
                entry["record_id"] for entry in entries if entry["pair_confirmed"]
            ),
            "semantic_recovery_confirmed": any(entry["pair_confirmed"] for entry in entries),
            "record_link_confirmations": entries,
        }
        for omission_id, entries in sorted(omission_confirmations.items())
    ]
    confirmed = sum(item["semantic_recovery_confirmed"] for item in omission_items)

    report = {
        "format": "candidate-extraction-model-coded-validation-v1",
        "coding_source": "model",
        "coder": args.coder,
        "independent_human_only": False,
        "release_gate_satisfied": False,
        "release_gate_note": (
            "The release gate accepts human coding only. This report is an "
            "interim estimate and does not replace human_validation.json."
        ),
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "validation_dir": str(args.validation_dir),
        "bundle_manifest_sha256": manifest.get("bundle_manifest_sha256"),
        "endorsement_architecture": manifest.get("endorsement_architecture"),
        "seed": manifest.get("seed"),
        "thresholds": {
            "all_dimension_accuracy_minimum": ALL_DIMENSION_GATE,
            "subtype_accuracy_minimum": SUBTYPE_GATE,
            "complete_page_recall_minimum": RECALL_GATE,
            "known_omissions_expected": EXPECTED_OMISSIONS,
        },
        "routes": route_reports,
        "known_omissions": {
            "omission_n": len(omission_items),
            "confirmed_n": confirmed,
            "meets_threshold": len(omission_items) == EXPECTED_OMISSIONS
            and confirmed == EXPECTED_OMISSIONS,
            "items": omission_items,
        },
    }
    report["all_thresholds_met"] = all(
        all(value["meets_thresholds"].values()) for value in route_reports.values()
    ) and report["known_omissions"]["meets_threshold"]

    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(
        json.dumps(report, indent=1, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps({
        "output": str(args.output),
        "all_thresholds_met": report["all_thresholds_met"],
        "routes": {
            route: {
                "all_dimension_accuracy": round(value["all_dimension_accuracy"], 4),
                "minimum_subtype_accuracy": round(value["minimum_subtype_accuracy"], 4),
                "complete_page_recall": round(value["complete_page_recall"], 4),
                "missed_records_total": value["missed_records_total"],
            }
            for route, value in route_reports.items()
        },
        "known_omissions_confirmed": f"{confirmed}/{len(omission_items)}",
    }, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
