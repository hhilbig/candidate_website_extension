#!/usr/bin/env python3
"""Merge chat-coded candidate-extraction validation results into coding tables."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any

import pandas as pd


PRECISION_FIELDS = (
    "record_present_in_page",
    "normalized_value_faithful",
    "record_type_correct",
    "evidence_alone_supports_record",
)


def _response_map(results: dict[str, Any]) -> dict[str, dict[str, Any]]:
    responses = results.get("responses", [])
    mapped = {str(row["item_id"]): row for row in responses}
    if len(mapped) != len(responses):
        raise ValueError("duplicate item_id in coding results")
    if int(results.get("n_complete", -1)) != len(responses):
        raise ValueError("results n_complete does not match response count")
    if any(row.get("status") != "complete" for row in responses):
        raise ValueError("coding results contain incomplete responses")
    return mapped


def merge_precision_rows(
    base_rows: list[dict[str, Any]], results: dict[str, Any]
) -> list[dict[str, Any]]:
    mapped = _response_map(results)
    base_ids = [str(row["validation_id"]) for row in base_rows]
    if set(base_ids) != set(mapped):
        raise ValueError("precision result IDs do not exactly match the coding sample")
    merged: list[dict[str, Any]] = []
    for row in base_rows:
        output = dict(row)
        response = mapped[str(row["validation_id"])]
        answers = response.get("answers", {})
        for field in PRECISION_FIELDS:
            value = answers.get(field)
            if value not in {"yes", "no"}:
                raise ValueError(f"invalid precision answer for {field}: {value!r}")
            output[field] = value
        output["coding_status"] = "complete"
        output["coder_notes"] = str(answers.get("notes", ""))
        output["coding_mode"] = str(
            response.get("coding_mode")
            or ("human_confirmed_chat" if base_ids.index(str(row["validation_id"])) < 50 else "ai_assisted_audit")
        )
        output["release_scope"] = str(response.get("release_scope", "included"))
        merged.append(output)
    return merged


def merge_recall_rows(
    base_rows: list[dict[str, Any]], results: dict[str, Any]
) -> list[dict[str, Any]]:
    mapped = _response_map(results)
    base_ids = [str(row["parent_document_id"]) for row in base_rows]
    if set(base_ids) != set(mapped):
        raise ValueError("recall result IDs do not exactly match the coding sample")
    merged: list[dict[str, Any]] = []
    for row in base_rows:
        output = dict(row)
        response = mapped[str(row["parent_document_id"])]
        answers = response.get("answers", {})
        answer = answers.get("all_requested_records_captured")
        if answer not in {"yes", "no"}:
            raise ValueError(f"invalid recall answer: {answer!r}")
        missed = answers.get("missed_records", [])
        if not isinstance(missed, list) or (answer == "yes" and missed):
            raise ValueError("recall omissions must be an empty list when answer is yes")
        if answer == "no" and not missed:
            raise ValueError("a no recall answer requires at least one documented omission")
        output["all_requested_records_captured"] = answer
        output["missed_records_json"] = json.dumps(
            missed, ensure_ascii=False, sort_keys=True
        )
        output["coding_status"] = "complete"
        output["coder_notes"] = str(answers.get("notes", ""))
        output["coding_mode"] = str(results.get("coding_mode", "ai_assisted_audit"))
        merged.append(output)
    return merged


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--validation-dir", type=Path, required=True)
    parser.add_argument("--precision-results", type=Path, required=True)
    parser.add_argument("--recall-results", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.output_dir.exists() and any(args.output_dir.iterdir()):
        raise FileExistsError(f"refusing nonempty output directory: {args.output_dir}")
    args.output_dir.mkdir(parents=True, exist_ok=True)

    precision_base = json.loads(
        (args.validation_dir / "record_precision_coding.json").read_text()
    )
    recall_base = json.loads(
        (args.validation_dir / "page_recall_coding.json").read_text()
    )
    precision_results = json.loads(args.precision_results.read_text())
    recall_results = json.loads(args.recall_results.read_text())
    precision = merge_precision_rows(precision_base, precision_results)
    recall = merge_recall_rows(recall_base, recall_results)

    outputs = {
        "record_precision_coding.json": precision,
        "page_recall_coding.json": recall,
    }
    for filename, rows in outputs.items():
        (args.output_dir / filename).write_text(
            json.dumps(rows, indent=2, ensure_ascii=False) + "\n"
        )
        pd.DataFrame(rows).to_csv(
            args.output_dir / filename.replace(".json", ".csv"), index=False
        )

    files = sorted(path.name for path in args.output_dir.iterdir())
    manifest = {
        "schema_version": "1.0",
        "coding_status": "complete_ai_assisted_audit",
        "precision_rows": len(precision),
        "recall_pages": len(recall),
        "human_confirmed_precision_rows": 50,
        "ai_assisted_precision_rows": 25,
        "ai_assisted_recall_pages": 15,
        "source_validation_dir": str(args.validation_dir),
        "result_provenance": {
            str(args.precision_results): _sha256(args.precision_results),
            str(args.recall_results): _sha256(args.recall_results),
        },
        "coding_file_sha256": {
            filename: _sha256(args.output_dir / filename) for filename in files
        },
    }
    (args.output_dir / "completion_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )
    print(json.dumps(manifest, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
