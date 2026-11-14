#!/usr/bin/env python3
"""Compare a round-four development run with the provisional AI first cut.

The comparison is diagnostic only.  The supplied draft is not human truth and
the resulting report cannot authorize expansion, integration, or release.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import re
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence


REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))
DEFAULT_DRAFT = REPO / (
    "build/private_candidate_endorsement_recall40_name_truth_amendment_"
    "20260906a/results_ai_coded_draft.json"
)
DEFAULT_TASK = REPO / (
    "build/private_candidate_endorsement_recall40_name_truth_amendment_"
    "20260906a/artifact/task.json"
)
DEFAULT_DRAFT_SHA256 = (
    "1b461c191c1a8394c54f0f66f6bed56e9da5ced84fb8777a3aa0e2d4169f4c6d"
)
DEFAULT_TASK_SHA256 = (
    "dc1662798e7a676a9e8ec1281b5e0b6976286435c1564ac4e092cefcb550a83e"
)
REPORT_VERSION = "candidate_endorsement_round4_regression_diagnostic_v3"
LEDGER_VERSION = "candidate_endorsement_round4_regression_private_ledger_v3"
BASELINE_MATCH_COUNT = 2_183
BASELINE_REFERENCE_COUNT = 2_225
OPTIONAL_FIELD_ERROR_RE = re.compile(
    r"^\$\.slot_\d{2}\.records\[\d+\]\."
    r"(?:title|affiliation|jurisdiction): "
    r"not an exact contiguous visible substring$"
)


def canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        allow_nan=False,
    )


def sha256_json(value: Any) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def strict_json(path: Path) -> Any:
    def hook(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise ValueError(f"duplicate JSON key in {path}: {key}")
            result[key] = value
        return result

    return json.loads(
        path.read_text(encoding="utf-8"),
        object_pairs_hook=hook,
        parse_constant=lambda value: (_ for _ in ()).throw(
            ValueError(f"nonfinite JSON constant in {path}: {value}")
        ),
    )


def read_gzip_jsonl(path: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                raise ValueError(f"blank output row: {path}:{line_number}")
            value = json.loads(line)
            if type(value) is not dict:
                raise ValueError(f"non-object output row: {path}:{line_number}")
            rows.append(value)
    return rows


def _name_key(name: str, entity_type: str) -> tuple[str, str]:
    from src.candidate_endorsement_round4 import normalize_name_for_match

    if entity_type not in {"individual", "organization", "unclear"}:
        raise ValueError(f"invalid entity type: {entity_type}")
    normalized = normalize_name_for_match(name)
    if not normalized:
        raise ValueError("empty normalized name")
    return normalized, entity_type


def provisional_parent_counters(
    draft: Mapping[str, Any], parent_source: Mapping[str, str]
) -> dict[str, Counter[tuple[str, str]]]:
    responses = draft.get("responses")
    if type(responses) is not list or len(responses) != len(parent_source):
        raise ValueError("provisional draft response count differs")
    result: dict[str, Counter[tuple[str, str]]] = {}
    for response in responses:
        if type(response) is not dict:
            raise ValueError("provisional response is not an object")
        parent_id = response.get("parent_document_id")
        if parent_id not in parent_source or parent_id in result:
            raise ValueError("provisional parent closure differs")
        source = parent_source[parent_id]
        children = response.get("children")
        if type(children) is not list:
            raise ValueError("provisional children must be a list")
        counter: Counter[tuple[str, str]] = Counter()
        for child in children:
            spans = child.get("name_spans")
            if type(spans) is not list or not spans:
                raise ValueError("provisional child has no name spans")
            previous_end = -1
            parts: list[str] = []
            for span in spans:
                if (
                    type(span) is not list
                    or len(span) != 2
                    or any(type(value) is not int for value in span)
                ):
                    raise ValueError("provisional name span schema differs")
                start, end = span
                if not 0 <= start < end <= len(source) or start < previous_end:
                    raise ValueError("provisional name span is out of bounds")
                parts.append(source[start:end])
                previous_end = end
            counter[_name_key(" ".join(parts), str(child.get("entity_type")))] += 1
        if bool(response.get("true_zero")) != (sum(counter.values()) == 0):
            raise ValueError("provisional true-zero/child equivalence differs")
        result[str(parent_id)] = counter
    if set(result) != set(parent_source):
        raise ValueError("provisional parent inventory differs")
    return result


def task_parent_sources(task: Mapping[str, Any]) -> dict[str, str]:
    parents = task.get("parents")
    if type(parents) is not list or not parents:
        raise ValueError("task parents must be a nonempty list")
    result: dict[str, str] = {}
    for parent in parents:
        if type(parent) is not dict:
            raise ValueError("task parent is not an object")
        parent_id = parent.get("parent_document_id")
        source = parent.get("parent_source_text")
        if type(parent_id) is not str or not parent_id or parent_id in result:
            raise ValueError("task parent identity is invalid or duplicated")
        if type(source) is not str:
            raise ValueError("task parent source is not text")
        if parent.get("source_n_chars") != len(source):
            raise ValueError("task parent source length differs")
        if parent.get("source_text_sha256") != hashlib.sha256(
            source.encode("utf-8")
        ).hexdigest():
            raise ValueError("task parent source hash differs")
        result[parent_id] = source
    return result


def predicted_parent_counters(
    batches: Sequence[Mapping[str, Any]],
    outputs: Sequence[Mapping[str, Any]],
    parent_ids: Iterable[str] | None = None,
    *,
    slot_salvage_ledger: list[dict[str, Any]] | None = None,
) -> tuple[
    dict[str, Counter[tuple[str, str]]],
    Counter[str],
    list[str],
    list[dict[str, Any]],
]:
    from src.candidate_endorsement_round4 import canonical_name_from_parts

    if len(batches) != len(outputs):
        raise ValueError("input/output batch counts differ")
    batch_parent_ids = {str(batch["parent_document_id"]) for batch in batches}
    complete_parent_ids = (
        batch_parent_ids if parent_ids is None else {str(value) for value in parent_ids}
    )
    if not batch_parent_ids <= complete_parent_ids:
        raise ValueError("batch parent inventory is outside the supplied closure")
    result = {parent_id: Counter() for parent_id in complete_parent_ids}
    dispositions: Counter[str] = Counter()
    failures: list[str] = []
    optional_repairs: list[dict[str, Any]] = []
    for batch, output in zip(batches, outputs, strict=True):
        if output.get("batch_id") != batch.get("batch_id"):
            raise ValueError("output order or batch identity differs")
        if output.get("technical_pass") is not True:
            if batch.get("stage") == "record_extraction" and isinstance(batch.get("targets"), list):
                from scripts.build_candidate_endorsement_round4_human_review_app import (
                    NonrepairableStageBOutput, _sanitized_stage_b_slot_decision,
                )
                from scripts.hive import extract_candidate_endorsement_round4_development_vllm as runner

                targets = {target["slot"]: target for target in batch["targets"]}
                accepted_slots, unresolved_slots, changes = [], [], []
                retained_record_count = 0
                for slot in batch["target_slots"]:
                    try:
                        decision, slot_changes, _validation = _sanitized_stage_b_slot_decision(
                            runner=runner, batch=batch, output_row=output, slot=slot, target=targets[slot],
                        )
                    except NonrepairableStageBOutput:
                        unresolved_slots.append(slot)
                        continue
                    accepted_slots.append(slot)
                    dispositions[str(decision["disposition"])] += 1
                    for record in decision["records"]:
                        name = canonical_name_from_parts(record["name_parts"])
                        result[str(batch["parent_document_id"])][_name_key(name, record["entity_type"])] += 1
                        retained_record_count += 1
                    changes.extend(slot_changes)
                if unresolved_slots:
                    failures.append(str(batch["batch_id"]))
                if changes:
                    optional_repairs.append({"batch_id": str(batch["batch_id"]),
                                             "parent_document_id": str(batch["parent_document_id"]),
                                             "nulling_operations": changes})
                if slot_salvage_ledger is not None:
                    slot_salvage_ledger.append({"batch_id": str(batch["batch_id"]),
                                               "parent_document_id": str(batch["parent_document_id"]),
                                               "accepted_target_slots": accepted_slots,
                                               "unresolved_target_slots": unresolved_slots,
                                               "retained_record_count": retained_record_count})
                continue
            validation = output.get("stage_validation")
            errors = (
                validation.get("errors")
                if type(validation) is dict
                else None
            )
            projections = (
                validation.get("source_projection")
                if type(validation) is dict
                else None
            )
            repairable = bool(
                type(validation) is dict
                and validation.get("schema_valid") is True
                and validation.get("coverage_complete") is True
                and type(errors) is list
                and errors
                and all(
                    type(error) is str
                    and OPTIONAL_FIELD_ERROR_RE.fullmatch(error) is not None
                    for error in errors
                )
                and type(projections) is dict
            )
            if not repairable:
                failures.append(str(batch["batch_id"]))
                continue
            changes = [
                {
                    "slot": slot,
                    "record_index": projection["record_index"],
                    "changes": projection["optional_field_changes"],
                }
                for slot, slot_projections in sorted(projections.items())
                for projection in slot_projections
                if projection.get("optional_field_changes")
            ]
            if sum(len(entry["changes"]) for entry in changes) != len(errors):
                failures.append(str(batch["batch_id"]))
                continue
            optional_repairs.append({
                "batch_id": str(batch["batch_id"]),
                "parent_document_id": str(batch["parent_document_id"]),
                "nulling_operations": changes,
            })
        model_output = output.get("model_output")
        if type(model_output) is not dict:
            raise ValueError("technical-pass output lacks model output")
        parent_id = str(batch["parent_document_id"])
        for slot in batch["target_slots"]:
            decision = model_output[slot]
            disposition = str(decision["disposition"])
            dispositions[disposition] += 1
            for record in decision["records"]:
                name = canonical_name_from_parts(record["name_parts"])
                result[parent_id][_name_key(name, record["entity_type"])] += 1
    return result, dispositions, failures, optional_repairs


def compare_parent_counters(
    reference: Mapping[str, Counter[tuple[str, str]]],
    predicted: Mapping[str, Counter[tuple[str, str]]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    if set(reference) != set(predicted):
        raise ValueError("reference/prediction parent closure differs")
    matched = reference_only = predicted_only = exact_parents = 0
    disagreements: list[dict[str, Any]] = []
    for parent_id in sorted(reference):
        expected = reference[parent_id]
        observed = predicted[parent_id]
        overlap = expected & observed
        matched += sum(overlap.values())
        missing = expected - observed
        extra = observed - expected
        reference_only += sum(missing.values())
        predicted_only += sum(extra.values())
        if not missing and not extra:
            exact_parents += 1
            continue
        disagreements.append({
            "parent_document_id": parent_id,
            "reference_only": [
                {"normalized_name": name, "entity_type": entity, "count": count}
                for (name, entity), count in sorted(missing.items())
            ],
            "predicted_only": [
                {"normalized_name": name, "entity_type": entity, "count": count}
                for (name, entity), count in sorted(extra.items())
            ],
        })
    reference_count = matched + reference_only
    predicted_count = matched + predicted_only
    return ({
        "parent_count": len(reference),
        "parent_exact_multiset_count": exact_parents,
        "parent_exact_multiset_rate": exact_parents / len(reference),
        "reference_record_count": reference_count,
        "predicted_record_count": predicted_count,
        "matched_name_type_count": matched,
        "reference_only_count": reference_only,
        "predicted_only_count": predicted_only,
        "reference_recall": matched / reference_count if reference_count else 1.0,
        "predicted_precision": matched / predicted_count if predicted_count else 1.0,
        "disagreement_parent_count": len(disagreements),
    }, disagreements)


def authenticated_run(
    input_dir: Path,
    output_dir: Path,
    expected_input_manifest_sha256: str,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    from scripts.hive.extract_candidate_endorsement_round4_development_vllm import (
        RECORD_EXTRACTION_STAGE,
        read_gzip_jsonl as read_input_rows,
        sha256_json as runner_sha256_json,
        validate_input_manifest,
    )

    manifest, manifest_hash, chunk_paths = validate_input_manifest(
        input_dir / "manifest.json",
        input_dir / "chunks",
        stage=RECORD_EXTRACTION_STAGE,
        expected_manifest_sha256=expected_input_manifest_sha256,
    )
    if manifest_hash != expected_input_manifest_sha256:
        raise ValueError("validated input manifest hash differs")
    batches = [row for path in chunk_paths for row in read_input_rows(path)]
    expected_output_names = [
        path.name.replace(".jsonl.gz", ".out.jsonl.gz") for path in chunk_paths
    ]
    actual_output_names = sorted(path.name for path in output_dir.glob("*.out.jsonl.gz"))
    if actual_output_names != expected_output_names:
        raise ValueError("output chunk inventory differs from input chunks")
    outputs = [
        row
        for name in expected_output_names
        for row in read_gzip_jsonl(output_dir / name)
    ]
    if len(outputs) != len(batches):
        raise ValueError("output row count differs from input batch count")
    generation_hashes: set[str] = set()
    for batch, output in zip(batches, outputs, strict=True):
        if output.get("batch_id") != batch["batch_id"]:
            raise ValueError("output batch order differs")
        if output.get("input_batch_sha256") != runner_sha256_json(batch):
            raise ValueError("output input-batch binding differs")
        if output.get("input_manifest_sha256") != manifest_hash:
            raise ValueError("output input-manifest binding differs")
        generation_hash = output.get("generation_config_sha256")
        if type(generation_hash) is not str or len(generation_hash) != 64:
            raise ValueError("output generation binding is invalid")
        generation_hashes.add(generation_hash)
        if output.get("development_only") is not True:
            raise ValueError("output is not marked development-only")
    if len(generation_hashes) != 1:
        raise ValueError("output generation configuration differs across rows")
    metadata = strict_json(output_dir / "run_metadata.json")
    generation_hash = next(iter(generation_hashes))
    if (
        metadata.get("input_manifest_sha256") != manifest_hash
        or metadata.get("generation_config_sha256") != generation_hash
        or metadata.get("processed_output_count") != len(outputs)
    ):
        raise ValueError("run metadata binding or count differs")
    return manifest, batches, outputs, metadata


def build_evaluation(
    *,
    manifest: Mapping[str, Any],
    batches: Sequence[Mapping[str, Any]],
    outputs: Sequence[Mapping[str, Any]],
    metadata: Mapping[str, Any],
    task: Mapping[str, Any],
    task_sha256: str,
    draft: Mapping[str, Any],
    draft_sha256: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    parent_source = task_parent_sources(task)
    batch_parent_source: dict[str, str] = {}
    for batch in batches:
        parent_id = str(batch["parent_document_id"])
        source = str(batch["parent_source_text"])
        if parent_id not in parent_source or parent_source[parent_id] != source:
            raise ValueError("batch parent source differs from authenticated task")
        if parent_id in batch_parent_source and batch_parent_source[parent_id] != source:
            raise ValueError("parent source differs across input batches")
        batch_parent_source[parent_id] = source
    reference = provisional_parent_counters(draft, parent_source)
    slot_salvage: list[dict[str, Any]] = []
    predicted, dispositions, unrepaired_failures, optional_repairs = (
        predicted_parent_counters(batches, outputs, parent_source, slot_salvage_ledger=slot_salvage)
    )
    metrics, disagreements = compare_parent_counters(reference, predicted)
    baseline_rate = BASELINE_MATCH_COUNT / BASELINE_REFERENCE_COUNT
    report = {
        "version": REPORT_VERSION,
        "development_only": True,
        "reference_is_provisional_ai_first_cut_not_human_truth": True,
        "input_manifest_sha256": metadata["input_manifest_sha256"],
        "generation_config_sha256": metadata["generation_config_sha256"],
        "authenticated_task_sha256": task_sha256,
        "provisional_draft_sha256": draft_sha256,
        "batch_count": len(batches),
        "raw_technical_pass_batch_count": sum(
            output.get("technical_pass") is True for output in outputs
        ),
        "optional_field_repaired_batch_count": len(optional_repairs),
        "usable_after_optional_nulling_batch_count": (
            len(batches) - len(unrepaired_failures)
        ),
        "unrepaired_technical_fail_batch_count": len(unrepaired_failures),
        "partial_failed_batch_with_usable_slots_count": sum(bool(row["accepted_target_slots"]) and bool(row["unresolved_target_slots"]) for row in slot_salvage),
        "records_retained_from_partial_failed_batches": sum(row["retained_record_count"] for row in slot_salvage if row["unresolved_target_slots"]),
        "disposition_counts": dict(sorted(dispositions.items())),
        "comparison": metrics,
        "round3_baseline": {
            "matched_name_type_count": BASELINE_MATCH_COUNT,
            "reference_record_count": BASELINE_REFERENCE_COUNT,
            "reference_recall": baseline_rate,
            "difference_in_matched_count": (
                metrics["matched_name_type_count"] - BASELINE_MATCH_COUNT
            ),
            "difference_in_reference_recall": metrics["reference_recall"] - baseline_rate,
        },
        "authorizations": {
            "human_truth": False,
            "production_integration": False,
            "full_expansion": False,
            "release": False,
        },
    }
    ledger = {
        "version": LEDGER_VERSION,
        "development_only": True,
        "reference_is_provisional_ai_first_cut_not_human_truth": True,
        "input_manifest_sha256": metadata["input_manifest_sha256"],
        "generation_config_sha256": metadata["generation_config_sha256"],
        "authenticated_task_sha256": task_sha256,
        "provisional_draft_sha256": draft_sha256,
        "unrepaired_technical_failure_batch_ids": unrepaired_failures,
        "optional_field_repairs": optional_repairs,
        "failed_batch_slot_replays": slot_salvage,
        "parent_disagreements": disagreements,
    }
    report["private_disagreement_ledger_payload_sha256"] = sha256_json(ledger)
    return report, ledger


def write_new(path: Path, value: Mapping[str, Any]) -> None:
    if path.exists() or path.is_symlink():
        raise FileExistsError(f"refusing to overwrite {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(value) + "\n", encoding="utf-8")


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--expected-input-manifest-sha256", required=True)
    parser.add_argument("--task", type=Path, default=DEFAULT_TASK)
    parser.add_argument("--expected-task-sha256", default=DEFAULT_TASK_SHA256)
    parser.add_argument("--draft", type=Path, default=DEFAULT_DRAFT)
    parser.add_argument("--expected-draft-sha256", default=DEFAULT_DRAFT_SHA256)
    parser.add_argument("--report", type=Path, required=True)
    parser.add_argument("--ledger", type=Path, required=True)
    args = parser.parse_args(argv)
    if sha256_file(args.task) != args.expected_task_sha256:
        raise ValueError("authenticated task bytes differ")
    task = strict_json(args.task)
    if sha256_file(args.draft) != args.expected_draft_sha256:
        raise ValueError("provisional draft bytes differ")
    draft = strict_json(args.draft)
    manifest, batches, outputs, metadata = authenticated_run(
        args.input_dir.resolve(),
        args.output_dir.resolve(),
        args.expected_input_manifest_sha256,
    )
    report, ledger = build_evaluation(
        manifest=manifest,
        batches=batches,
        outputs=outputs,
        metadata=metadata,
        task=task,
        task_sha256=args.expected_task_sha256,
        draft=draft,
        draft_sha256=args.expected_draft_sha256,
    )
    write_new(args.ledger.resolve(), ledger)
    write_new(args.report.resolve(), report)
    print(canonical_json({
        "report": str(args.report.resolve()),
        "report_sha256": sha256_file(args.report.resolve()),
        "ledger": str(args.ledger.resolve()),
        "ledger_sha256": sha256_file(args.ledger.resolve()),
        "comparison": report["comparison"],
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
