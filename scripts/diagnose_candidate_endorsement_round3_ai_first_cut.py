#!/usr/bin/env python3
"""Compare frozen round-three predictions with provisional AI-coded labels.

This is a development diagnostic.  The supplied labels are useful for finding
failure patterns, but they are not human truth and cannot authorize expansion
or release.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import unicodedata
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


REPO = Path(__file__).resolve().parents[1]
TASK_PATH = Path(
    "build/private_candidate_endorsement_recall40_name_truth_amendment_"
    "20260906a/artifact/task.json"
)
DRAFT_RESULTS_PATH = Path(
    "build/private_candidate_endorsement_recall40_name_truth_amendment_"
    "20260906a/results_ai_coded_draft.json"
)
STAGE_B_INPUT_DIR = Path(
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round3_stage_b_preflight_20260906a"
)
STAGE_B_OUTPUT_DIR = Path(
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round3_stage_b_outputs_20260906a"
)
STAGE_B_REPORT_PATH = Path(
    "quality_reports/"
    "candidate_endorsement_recall40_round3_stage_b_22685041_2026-09-06.json"
)

EXPECTED_TASK_SHA256 = (
    "dc1662798e7a676a9e8ec1281b5e0b6976286435c1564ac4e092cefcb550a83e"
)
EXPECTED_DRAFT_RESULTS_SHA256 = (
    "1b461c191c1a8394c54f0f66f6bed56e9da5ced84fb8777a3aa0e2d4169f4c6d"
)
EXPECTED_STAGE_B_REPORT_SHA256 = (
    "f775b6e3d7d27111f926516b449b85203d0bbdb263c1f1eeb8de9d7a58b082fa"
)
REPORT_VERSION = "candidate_endorsement_round3_ai_first_cut_diagnostic_v2"
LEDGER_VERSION = "candidate_endorsement_round3_ai_first_cut_disagreement_ledger_v1"


def canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def strict_load(path: Path) -> Any:
    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise ValueError(f"duplicate JSON key in {path}: {key}")
            result[key] = value
        return result

    return json.loads(
        path.read_text(encoding="utf-8"),
        object_pairs_hook=pairs,
        parse_constant=lambda value: (_ for _ in ()).throw(
            ValueError(f"nonfinite JSON constant in {path}: {value}")
        ),
    )


def _normalized_name(value: str) -> str:
    return " ".join(unicodedata.normalize("NFKC", value).split()).casefold()


def _canonical_child_name(source: str, child: Mapping[str, Any]) -> str:
    spans = child.get("name_spans")
    if type(spans) is not list or not spans:
        raise ValueError("draft child name_spans must be a nonempty list")
    parts: list[str] = []
    previous_end = -1
    for pair in spans:
        if (
            type(pair) is not list
            or len(pair) != 2
            or any(type(value) is not int for value in pair)
        ):
            raise ValueError("draft child name span must contain two integers")
        start, end = pair
        if not (0 <= start < end <= len(source)) or start < previous_end:
            raise ValueError("draft child name span is outside or unordered")
        if not source[start:end].strip():
            raise ValueError("draft child name span selects only whitespace")
        parts.append(source[start:end])
        previous_end = end
    name = " ".join(" ".join(parts).split())
    if not name:
        raise ValueError("draft child has an empty canonical name")
    return name


def _block_ranges(target: Mapping[str, Any]) -> tuple[tuple[int, int], ...]:
    lines = target.get("record_block_lines")
    if type(lines) is not list or not lines:
        raise ValueError("Stage-B target has no record block lines")
    result: list[tuple[int, int]] = []
    for line in lines:
        if type(line) is not dict:
            raise ValueError("Stage-B record block line is not an object")
        start = line.get("parent_start_char")
        end = line.get("parent_end_char")
        if type(start) is not int or type(end) is not int or not 0 <= start < end:
            raise ValueError("Stage-B record block line coordinates are invalid")
        result.append((start, end))
    return tuple(result)


def child_is_in_target(child: Mapping[str, Any], target: Mapping[str, Any]) -> bool:
    """Return whether all draft name coordinates fall in the target block."""
    ranges = _block_ranges(target)
    spans = child.get("name_spans")
    if type(spans) is not list or not spans:
        raise ValueError("draft child name_spans must be a nonempty list")
    return all(
        any(start >= block_start and end <= block_end for block_start, block_end in ranges)
        for start, end in spans
    )


def _read_gzip_rows(paths: Iterable[Path]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for path in paths:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line_index, line in enumerate(handle):
                if not line.strip():
                    raise ValueError(f"blank gzip row: {path}:{line_index}")
                value = json.loads(line)
                if type(value) is not dict:
                    raise ValueError(f"gzip row is not an object: {path}:{line_index}")
                rows.append(value)
    return rows


def _validate_file_inventory(
    root: Path,
    entries: Sequence[Mapping[str, Any]],
    directory: Path,
) -> list[Path]:
    paths: list[Path] = []
    for entry in entries:
        name = entry.get("file")
        expected_sha = entry.get("sha256")
        expected_bytes = entry.get("bytes")
        if type(name) is not str or type(expected_sha) is not str:
            raise ValueError("Stage-B report file inventory is invalid")
        path = root / directory / name
        if (
            not path.is_file()
            or sha256_file(path) != expected_sha
            or (type(expected_bytes) is int and path.stat().st_size != expected_bytes)
        ):
            raise ValueError(f"Stage-B file inventory differs: {path}")
        paths.append(path)
    return paths


def _exact_record_counter(records: Sequence[Mapping[str, Any]]) -> Counter[tuple[str, str]]:
    result: Counter[tuple[str, str]] = Counter()
    for record in records:
        if type(record) is not dict:
            raise ValueError("model record is not an object")
        name = record.get("endorser_name")
        entity_type = record.get("entity_type")
        if type(name) is not str or not name.strip() or type(entity_type) is not str:
            raise ValueError("model record name/type is invalid")
        result[(_normalized_name(name), entity_type)] += 1
    return result


def _exact_child_counter(
    source: str, children: Sequence[Mapping[str, Any]]
) -> Counter[tuple[str, str]]:
    result: Counter[tuple[str, str]] = Counter()
    for child in children:
        if type(child) is not dict:
            raise ValueError("draft child is not an object")
        entity_type = child.get("entity_type")
        if entity_type not in {"individual", "organization"}:
            raise ValueError("draft child entity_type is invalid")
        result[(_normalized_name(_canonical_child_name(source, child)), entity_type)] += 1
    return result


def build_diagnostic(repo: Path = REPO) -> tuple[dict[str, Any], dict[str, Any]]:
    repo = repo.resolve()
    task_path = repo / TASK_PATH
    draft_path = repo / DRAFT_RESULTS_PATH
    report_path = repo / STAGE_B_REPORT_PATH
    if sha256_file(task_path) != EXPECTED_TASK_SHA256:
        raise ValueError("frozen name-amendment task bytes differ")
    if sha256_file(draft_path) != EXPECTED_DRAFT_RESULTS_SHA256:
        raise ValueError("provisional AI-coded result bytes differ")
    if sha256_file(report_path) != EXPECTED_STAGE_B_REPORT_SHA256:
        raise ValueError("frozen Stage-B report bytes differ")
    task = strict_load(task_path)
    draft = strict_load(draft_path)
    stage_b_report = strict_load(report_path)
    if (
        type(task) is not dict
        or type(draft) is not dict
        or type(stage_b_report) is not dict
        or draft.get("task_sha256") != EXPECTED_TASK_SHA256
        or draft.get("parent_count") != 40
        or stage_b_report.get("gate_decision") != "NO_GO"
    ):
        raise ValueError("provisional diagnostic input identity differs")

    parents = task.get("parents")
    responses = draft.get("responses")
    if (
        type(parents) is not list
        or type(responses) is not list
        or len(parents) != 40
        or len(responses) != 40
    ):
        raise ValueError("provisional diagnostic parent closure differs")
    parent_by_id = {parent["parent_document_id"]: parent for parent in parents}
    response_by_id = {response["parent_document_id"]: response for response in responses}
    if (
        len(parent_by_id) != 40
        or len(response_by_id) != 40
        or list(parent_by_id) != list(response_by_id)
    ):
        raise ValueError("provisional task/result parent order differs")

    input_manifest = strict_load(repo / STAGE_B_INPUT_DIR / "manifest.json")
    input_chunk_entries = input_manifest.get("files", {}).get("chunks")
    if type(input_chunk_entries) is not list or not input_chunk_entries:
        raise ValueError("Stage-B input manifest chunk inventory is invalid")
    input_paths = [
        repo / STAGE_B_INPUT_DIR / "chunks" / entry["file"]
        for entry in input_chunk_entries
    ]
    for path, entry in zip(input_paths, input_chunk_entries, strict=True):
        if sha256_file(path) != entry["sha256"]:
            raise ValueError(f"Stage-B input chunk bytes differ: {path}")
    output_paths = _validate_file_inventory(
        repo,
        stage_b_report["output_file_inventory"],
        STAGE_B_OUTPUT_DIR,
    )
    batches = _read_gzip_rows(input_paths)
    output_rows = _read_gzip_rows(output_paths)
    expected_ids = input_manifest.get("expected_batch_ids")
    if (
        [batch.get("batch_id") for batch in batches] != expected_ids
        or [row.get("batch_id") for row in output_rows] != expected_ids
    ):
        raise ValueError("Stage-B input/output order differs")

    output_by_id = {row["batch_id"]: row for row in output_rows}
    assigned_children: set[tuple[str, int]] = set()
    anchor_topology_counts: Counter[str] = Counter()
    exact_anchor_match_count = 0
    block_local_exact_record_match_count = 0
    disagreements: list[dict[str, Any]] = []
    predictions_by_parent: dict[str, list[dict[str, Any]]] = {
        parent_id: [] for parent_id in parent_by_id
    }

    for batch in batches:
        parent_id = batch["parent_document_id"]
        source = parent_by_id[parent_id]["parent_source_text"]
        children = response_by_id[parent_id]["children"]
        output_row = output_by_id[batch["batch_id"]]
        model_output = output_row.get("model_output")
        if type(model_output) is not dict:
            raise ValueError("parsed Stage-B row lacks model output")
        for target in batch["targets"]:
            slot = target["slot"]
            records = model_output[slot]["records"]
            predictions_by_parent[parent_id].extend(records)
            covered_children = [
                (index, child)
                for index, child in enumerate(children)
                if child_is_in_target(child, target)
            ]
            assigned_children.update(
                (parent_id, index) for index, _child in covered_children
            )
            truth_count = len(covered_children)
            prediction_count = len(records)
            anchor_topology_counts[
                f"truth_{truth_count}_prediction_{prediction_count}"
            ] += 1
            truth_counter = _exact_child_counter(
                source, [child for _index, child in covered_children]
            )
            prediction_counter = _exact_record_counter(records)
            block_local_exact_record_match_count += sum(
                (truth_counter & prediction_counter).values()
            )
            exact = truth_counter == prediction_counter
            exact_anchor_match_count += int(exact)
            if not exact:
                disagreements.append({
                    "parent_document_id": parent_id,
                    "stage_b_batch_id": batch["batch_id"],
                    "stage_b_slot": slot,
                    "source_line_id": target["source_line_id"],
                    "stage_a_role": target["stage_a_role"],
                    "technical_pass": output_row["technical_pass"],
                    "validation_errors": list(
                        output_row["stage_validation"]["errors"]
                    ),
                    "provisional_children": [
                        {
                            "canonical_name": _canonical_child_name(source, child),
                            "entity_type": child["entity_type"],
                            "name_spans": child["name_spans"],
                        }
                        for _index, child in covered_children
                    ],
                    "model_records": records,
                    "record_block_lines": target["record_block_lines"],
                })

    all_children = {
        (parent_id, index)
        for parent_id, response in response_by_id.items()
        for index, _child in enumerate(response["children"])
    }
    unanchored_children = all_children - assigned_children
    exact_record_match_count = 0
    provisional_child_count = 0
    model_record_count = 0
    parent_level_disagreement_count = 0
    for parent_id, parent in parent_by_id.items():
        source = parent["parent_source_text"]
        truth_counter = _exact_child_counter(
            source, response_by_id[parent_id]["children"]
        )
        prediction_counter = _exact_record_counter(
            predictions_by_parent[parent_id]
        )
        exact_record_match_count += sum((truth_counter & prediction_counter).values())
        provisional_child_count += sum(truth_counter.values())
        model_record_count += sum(prediction_counter.values())
        parent_level_disagreement_count += int(truth_counter != prediction_counter)

    validation_errors = stage_b_report["validation_error_reason_counts"]
    technical = stage_b_report["technical_counts"]
    anchor_count = sum(len(batch["targets"]) for batch in batches)
    if (
        anchor_count != 2_221
        or provisional_child_count != 2_225
        or model_record_count != 2_225
        or technical.get("technical_fail_rows") != 53
    ):
        raise ValueError("provisional diagnostic expected census differs")

    ledger = {
        "version": LEDGER_VERSION,
        "status": "PRIVATE_PROVISIONAL_DEVELOPMENT_ONLY",
        "task_sha256": EXPECTED_TASK_SHA256,
        "ai_draft_results_sha256": EXPECTED_DRAFT_RESULTS_SHA256,
        "stage_b_report_sha256": EXPECTED_STAGE_B_REPORT_SHA256,
        "disagreements": disagreements,
        "unanchored_provisional_children": [
            {
                "parent_document_id": parent_id,
                "child_index": child_index,
                "canonical_name": _canonical_child_name(
                    parent_by_id[parent_id]["parent_source_text"],
                    response_by_id[parent_id]["children"][child_index],
                ),
                "entity_type": response_by_id[parent_id]["children"][child_index][
                    "entity_type"
                ],
                "name_spans": response_by_id[parent_id]["children"][child_index][
                    "name_spans"
                ],
            }
            for parent_id, child_index in sorted(unanchored_children)
        ],
        "human_truth_established": False,
        "accuracy_established": False,
        "full_expansion_authorized": False,
        "release_authorized": False,
    }
    ledger_sha256 = sha256_json(ledger)
    report = {
        "version": REPORT_VERSION,
        "status": "PROVISIONAL_DEVELOPMENT_DIAGNOSTIC_COMPLETE",
        "scope": "same_sample_prompt_and_rule_development_only",
        "inputs": {
            "task_sha256": EXPECTED_TASK_SHA256,
            "ai_draft_results_sha256": EXPECTED_DRAFT_RESULTS_SHA256,
            "stage_b_report_sha256": EXPECTED_STAGE_B_REPORT_SHA256,
        },
        "census": {
            "parent_count": len(parents),
            "stage_b_batch_count": len(batches),
            "stage_b_anchor_count": anchor_count,
            "provisional_child_count": provisional_child_count,
            "model_record_count": model_record_count,
            "technical_pass_batch_count": technical["technical_pass_rows"],
            "technical_fail_batch_count": technical["technical_fail_rows"],
            "technical_failure_parent_count": technical[
                "technical_failure_parent_count"
            ],
            "exact_name_and_type_match_count": exact_record_match_count,
            "exact_name_and_type_mismatch_count": provisional_child_count
            - exact_record_match_count,
            "exact_name_and_type_match_percent": round(
                100 * exact_record_match_count / provisional_child_count, 6
            ),
            "block_local_exact_name_and_type_match_count": (
                block_local_exact_record_match_count
            ),
            "block_local_exact_name_and_type_mismatch_count": (
                provisional_child_count - block_local_exact_record_match_count
            ),
            "block_local_exact_name_and_type_match_percent": round(
                100
                * block_local_exact_record_match_count
                / provisional_child_count,
                6,
            ),
            "exact_anchor_multiset_match_count": exact_anchor_match_count,
            "exact_anchor_multiset_mismatch_count": anchor_count
            - exact_anchor_match_count,
            "parent_with_any_exact_multiset_disagreement_count": (
                parent_level_disagreement_count
            ),
            "false_anchor_against_provisional_labels_count": sum(
                count
                for key, count in anchor_topology_counts.items()
                if key.startswith("truth_0_")
            ),
            "multi_child_anchor_against_provisional_labels_count": sum(
                count
                for key, count in anchor_topology_counts.items()
                if not key.startswith("truth_0_")
                and not key.startswith("truth_1_")
            ),
            "unanchored_provisional_child_count": len(unanchored_children),
        },
        "anchor_topology_counts": dict(sorted(anchor_topology_counts.items())),
        "validation_error_reason_counts": validation_errors,
        "development_findings": [
            "preserve_identity_suffixes_nicknames_parenthetical_aliases_digits_and_source_abbreviations",
            "return_one_record_per_coordinated_identity_including_shared_surnames",
            "prevent_cross_slot_identity_copying_and_repetition",
            "revisit_false_and_missing_stage_a_anchors_against_source_context",
            "return_null_for_optional_fields_that_are_not_exact_source_substrings",
        ],
        "private_disagreement_ledger_sha256": ledger_sha256,
        "interpretation": (
            "The match rate is descriptive agreement with an AI-coded first cut, "
            "not an accuracy estimate. The same 40 parents may be used for repair "
            "development only; a fresh sample and later human review are required."
        ),
        "authorizations": {
            "prompt_and_rule_iteration": True,
            "bounded_fresh_validation_sample": True,
            "human_truth": False,
            "accuracy_claim": False,
            "production_integration": False,
            "full_expansion": False,
            "release": False,
        },
    }
    return report, ledger


def _write_new(path: Path, value: Any) -> None:
    if path.exists():
        raise FileExistsError(f"refusing to overwrite: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(value) + "\n", encoding="utf-8")
    if path.stat().st_size <= 1:
        raise ValueError(f"written artifact is empty: {path}")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=REPO)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--private-ledger", type=Path, required=True)
    args = parser.parse_args(argv)
    report, ledger = build_diagnostic(args.repo)
    if report["private_disagreement_ledger_sha256"] != sha256_json(ledger):
        raise ValueError("aggregate report and private ledger binding differs")
    _write_new(args.private_ledger, ledger)
    _write_new(args.report, report)
    print(canonical_json({
        "report": str(args.report),
        "report_sha256": sha256_file(args.report),
        "private_ledger": str(args.private_ledger),
        "private_ledger_sha256": sha256_file(args.private_ledger),
        "census": report["census"],
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
