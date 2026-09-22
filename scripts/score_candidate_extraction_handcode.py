#!/usr/bin/env python3
"""Score complete human-only extraction review results and finalize the gate.

The scorer reloads the frozen bundle, replays deterministic sample selection,
and byte-rebuilds every coding app before it verifies results. It refuses
incomplete, duplicate, substituted, or malformed inputs. A passing report
replaces the bundle's pending
``human_validation.json``. The scorer then updates only its manifest hash and
the release status. It never changes the frozen segment file or its metadata.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import os
import sys
import tempfile
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT))

from scripts.build_candidate_extraction_handcode import (
    BIOGRAPHY_FIELDS_V2,
    BIOGRAPHY_PER_FIELD,
    ENDORSEMENT_PRECISION_N,
    ISSUE_PRECISION_N,
    RECALL_MULTI_TARGET,
    RECALL_PER_ROUTE,
    RECALL_ZERO_TARGET,
    ROUTES,
    build_task_html,
    build_assignment_batches,
    canonical_json,
    handcode_renderer_provenance,
    prepare_handcode_selection,
    sha256_file,
    sha256_text,
)
from src.candidate_endorsement_line_ledger import get_line_ledger_contract
from scripts.hive.extract_candidate_pages_vllm import (
    RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID,
    RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256,
    STRUCTURED_DECODING_BACKEND,
    STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
)
from scripts.import_candidate_extraction_v2 import (
    CONTRACT as V2_CONTRACT,
    EXPECTED_GENERATION_CONFIG_SHA256,
)


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
PRECISION_MINIMUMS = {
    "biography": BIOGRAPHY_PER_FIELD * 6,
    "issues": ISSUE_PRECISION_N,
    "endorsements": ENDORSEMENT_PRECISION_N,
}
ALL_DIMENSION_GATE = 0.90
SUBTYPE_GATE = 0.80
RECALL_GATE = 0.90


class HumanValidationError(ValueError):
    """The human validation inputs are incomplete or internally inconsistent."""


def _require(condition: Any, message: str) -> None:
    if not bool(condition):
        raise HumanValidationError(message)


def _typed_equal(left: Any, right: Any) -> bool:
    """Compare JSON provenance without treating booleans as integers."""
    if type(left) is not type(right):
        return False
    if isinstance(left, Mapping):
        return left.keys() == right.keys() and all(
            _typed_equal(left[key], right[key]) for key in left
        )
    if isinstance(left, (list, tuple)):
        return len(left) == len(right) and all(
            _typed_equal(left_item, right_item)
            for left_item, right_item in zip(left, right, strict=True)
        )
    return left == right


def _require_raw_structural_whitespace_gate(
    manifest: Mapping[str, Any],
) -> None:
    _require(
        manifest.get("raw_structural_whitespace_validator_id")
        == RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID
        and manifest.get("raw_structural_whitespace_validator_spec_sha256")
        == RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256
        and manifest.get("raw_structural_whitespace_gate_passed") is True,
        "handcode raw structural-whitespace gate differs",
    )


def _require_technical_raw_structural_whitespace_gate(
    checks: Any,
) -> None:
    _require(
        isinstance(checks, Mapping)
        and checks.get("raw_structural_whitespace_validator_id")
        == RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID
        and checks.get("raw_structural_whitespace_validator_spec_sha256")
        == RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256
        and checks.get("raw_structural_whitespace_gate_passed") is True,
        "technical raw structural-whitespace gate differs",
    )


def _require_model_item_guard_gate(
    provenance: Mapping[str, Any], label: str
) -> None:
    _require(
        provenance.get("array_item_cap_spec_id")
        == V2_CONTRACT["array_item_cap_spec_id"]
        and provenance.get("array_item_cap_spec_sha256")
        == V2_CONTRACT["array_item_cap_spec_sha256"]
        and _typed_equal(
            provenance.get("route_array_item_caps"),
            V2_CONTRACT["route_array_item_caps"],
        )
        and provenance.get("model_item_repetition_spec_id")
        == V2_CONTRACT["model_item_repetition_spec_id"]
        and provenance.get("model_item_repetition_spec_sha256")
        == V2_CONTRACT["model_item_repetition_spec_sha256"]
        and type(provenance.get("array_item_cap_reached_outputs")) is int
        and provenance["array_item_cap_reached_outputs"] == 0
        and type(provenance.get("model_item_repetition_error_outputs")) is int
        and provenance["model_item_repetition_error_outputs"] == 0
        and provenance.get("pre_alignment_model_item_guard_gate_passed")
        is True,
        f"{label} pre-alignment model-item guard gate differs",
    )


def _require_suffix_p_contract_identity(
    provenance: Mapping[str, Any], label: str
) -> None:
    """Bind handcoding and scoring to the frozen suffix-p contract."""
    _require(
        provenance.get("extraction_contract_sha256")
        == V2_CONTRACT["extraction_contract_sha256"]
        and provenance.get("endorsement_context_provenance_spec_sha256")
        == V2_CONTRACT["endorsement_context_provenance_spec_sha256"]
        and provenance.get("user_message_template_id")
        == V2_CONTRACT["user_message_template_id"]
        and provenance.get("user_message_template_sha256")
        == V2_CONTRACT["user_message_template_sha256"],
        f"{label} suffix-p contract identity differs",
    )


def _require_line_ledger_identity(
    provenance: Mapping[str, Any], label: str, *, technical: bool = False
) -> None:
    expected = get_line_ledger_contract()["line_ledger_contract_sha256"]
    _require(
        provenance.get("endorsement_line_ledger_contract_sha256") == expected
        and (
            not technical
            or provenance.get(
                "endorsement_line_ledger_public_coverage_complete"
            ) is True
        ),
        f"{label} endorsement line-ledger identity differs",
    )


def _require_user_message_replay_gate(
    provenance: Mapping[str, Any], label: str
) -> None:
    """Require exact replay of every per-window user-message hash."""
    _require(
        _typed_equal(provenance.get("user_message_hash_replay_rate"), 1.0),
        f"{label} user-message hash replay is below 100%",
    )


def _safe_file(root: Path, spec: Any, label: str) -> Path:
    _require(isinstance(spec, dict), f"{label} file specification is invalid")
    relative = Path(str(spec.get("file") or ""))
    _require(str(relative) not in {"", "."}, f"{label} file is not named")
    _require(not relative.is_absolute() and ".." not in relative.parts, f"unsafe {label} path")
    path = (root / relative).resolve()
    _require(root.resolve() in path.parents, f"{label} path leaves its directory")
    _require(path.is_file(), f"{label} file is absent: {relative}")
    _require(sha256_file(path) == spec.get("sha256"), f"{label} file hash differs")
    return path


def _task_payloads_sha256(
    task_payloads: Mapping[str, Mapping[str, Any]],
) -> str:
    return sha256_text(
        canonical_json({
            str(task_id): task_payloads[task_id]
            for task_id in sorted(task_payloads)
        })
    )


def _sample_inventory_sha256(manifest: Mapping[str, Any]) -> str:
    tasks = manifest.get("tasks")
    if not isinstance(tasks, list):
        return sha256_text(canonical_json(None))
    fields = (
        "task_id",
        "kind",
        "route",
        "batch_index",
        "item_count",
        "sample_items",
    )
    return sha256_text(canonical_json([
        {field: entry.get(field) for field in fields}
        if isinstance(entry, Mapping)
        else None
        for entry in tasks
    ]))


def _verify_rendered_handcode_app(
    handcode_dir: Path,
    entry: Mapping[str, Any],
    task_id: str,
) -> None:
    task_path = _safe_file(
        handcode_dir, entry.get("task_json"), f"{task_id} task"
    )
    html_spec = entry.get("html")
    _require(
        isinstance(html_spec, Mapping)
        and set(html_spec) == {"file", "bytes", "sha256"}
        and type(html_spec.get("bytes")) is int
        and html_spec["bytes"] > 0,
        f"{task_id} rendered app provenance differs",
    )
    html_path = _safe_file(handcode_dir, html_spec, f"{task_id} HTML")
    _require(
        html_path.stat().st_size == html_spec["bytes"],
        f"{task_id} rendered app byte count differs",
    )
    with tempfile.TemporaryDirectory(prefix="candidate-handcode-replay-") as temp:
        replay_path = Path(temp) / f"{task_id}.html"
        try:
            build_task_html(task_path, replay_path)
        except (OSError, ValueError) as error:
            raise HumanValidationError(
                f"{task_id} rendered app replay failed: {error}"
            ) from error
        _require(
            replay_path.read_bytes() == html_path.read_bytes(),
            f"{task_id} rendered app differs from deterministic replay",
        )


def verify_handcode_selection_against_bundle(
    bundle_dir: Path,
    handcode_dir: Path,
    manifest: Mapping[str, Any],
    manifest_sha256: str,
    task_payloads: Mapping[str, dict[str, Any]],
) -> dict[str, Any]:
    """Replay the deterministic holdout and bind every displayed task to it."""
    seed = manifest.get("seed")
    batch_size = manifest.get("batch_size")
    _require(
        type(seed) is int and seed >= 0,
        "handcode seed is not a nonnegative integer",
    )
    _require(
        type(batch_size) is int and batch_size > 0,
        "handcode batch size is not a positive integer",
    )
    exclusion = manifest.get("excluded_parent_ids")
    _require(
        isinstance(exclusion, Mapping)
        and set(exclusion) == {"count", "sha256", "values"},
        "handcode tuning-exclusion provenance differs",
    )
    excluded_values = exclusion.get("values")
    _require(
        isinstance(excluded_values, list)
        and bool(excluded_values)
        and all(isinstance(value, str) and bool(value) for value in excluded_values)
        and excluded_values == sorted(set(excluded_values)),
        "handcode tuning-exclusion IDs are invalid",
    )
    _require(
        type(exclusion.get("count")) is int
        and exclusion["count"] == len(excluded_values)
        and exclusion.get("sha256")
        == sha256_text(canonical_json(excluded_values)),
        "handcode tuning-exclusion count or hash differs",
    )
    try:
        prepared = prepare_handcode_selection(
            bundle_dir,
            set(excluded_values),
            seed=seed,
            batch_size=batch_size,
        )
    except (OSError, ValueError) as error:
        raise HumanValidationError(
            f"deterministic handcode selection replay failed: {error}"
        ) from error
    bundle_info = prepared["bundle_info"]
    try:
        renderer = handcode_renderer_provenance()
    except (OSError, ValueError) as error:
        raise HumanValidationError(
            f"handcode renderer provenance cannot be verified: {error}"
        ) from error
    _require(
        _typed_equal(manifest.get("handcode_renderer"), renderer),
        "handcode renderer or template identity differs",
    )
    expected_bundle_fields = {
        "bundle_manifest_sha256": bundle_info["bundle_manifest_sha256"],
        "segments_file": bundle_info["segments_file"],
        "segments_file_sha256": bundle_info["segments_file_sha256"],
        "source_corpus_sha256": bundle_info["bundle_manifest"].get(
            "source_corpus_sha256"
        ),
        "endorsement_line_ledger_contract_sha256": (
            get_line_ledger_contract()["line_ledger_contract_sha256"]
        ),
        "known_omission_recovery_file": bundle_info[
            "known_omission_recovery_file"
        ],
    }
    for field, expected in expected_bundle_fields.items():
        _require(
            _typed_equal(manifest.get(field), expected),
            f"handcode {field} differs from the frozen bundle",
        )
    _require(
        _typed_equal(manifest.get("selection"), prepared["selection"]),
        "handcode selection summary differs from deterministic replay",
    )
    expected_tasks = prepared["tasks"]
    expected_ids = [str(task["task_id"]) for task in expected_tasks]
    actual_entries = manifest.get("tasks")
    _require(
        isinstance(actual_entries, list)
        and [str(entry.get("task_id") or "") for entry in actual_entries]
        == expected_ids,
        "handcode task inventory differs from deterministic replay",
    )
    _require(
        _typed_equal(
            manifest.get("assignment_batches"),
            build_assignment_batches(actual_entries),
        )
        and manifest.get("known_omission_tasks_separate_from_holdout") is True,
        "handcode assignment batches differ from deterministic replay",
    )
    _require(
        list(task_payloads) == expected_ids,
        "loaded task order differs from deterministic replay",
    )
    for expected_task, actual_entry in zip(
        expected_tasks, actual_entries, strict=True
    ):
        task_id = str(expected_task["task_id"])
        expected_entry = {
            "task_id": task_id,
            "kind": expected_task["kind"],
            "route": expected_task["route"],
            "batch_index": expected_task["batch_index"],
            "item_count": len(expected_task["payload"]["items"]),
            "sample_items": expected_task["sample_items"],
        }
        actual_subset = {
            field: actual_entry.get(field) for field in expected_entry
        }
        _require(
            _typed_equal(actual_subset, expected_entry),
            f"{task_id} manifest sample differs from deterministic replay",
        )
        _require(
            _typed_equal(task_payloads.get(task_id), expected_task["payload"]),
            f"{task_id} displayed task differs from deterministic replay",
        )
        _verify_rendered_handcode_app(handcode_dir, actual_entry, task_id)
    return {
        "version": "candidate_extraction_handcode_selection_replay_v1",
        "passed": True,
        "bundle_manifest_sha256": bundle_info["bundle_manifest_sha256"],
        "handcode_manifest_sha256": manifest_sha256,
        "task_payloads_sha256": _task_payloads_sha256(task_payloads),
        "sample_inventory_sha256": _sample_inventory_sha256(manifest),
        "handcode_renderer_sha256": sha256_text(canonical_json(renderer)),
        "selection_sha256": sha256_text(canonical_json(prepared["selection"])),
        "seed": seed,
        "batch_size": batch_size,
        "excluded_parent_ids_sha256": exclusion["sha256"],
    }


def _require_handcode_selection_replay(
    replay: Any,
    manifest: Mapping[str, Any],
    manifest_sha256: str,
    task_payloads: Mapping[str, dict[str, Any]],
) -> None:
    exclusion = manifest.get("excluded_parent_ids")
    expected = {
        "version": "candidate_extraction_handcode_selection_replay_v1",
        "passed": True,
        "bundle_manifest_sha256": manifest.get("bundle_manifest_sha256"),
        "handcode_manifest_sha256": manifest_sha256,
        "task_payloads_sha256": _task_payloads_sha256(task_payloads),
        "sample_inventory_sha256": _sample_inventory_sha256(manifest),
        "handcode_renderer_sha256": sha256_text(
            canonical_json(manifest.get("handcode_renderer"))
        ),
        "selection_sha256": sha256_text(canonical_json(manifest.get("selection"))),
        "seed": manifest.get("seed"),
        "batch_size": manifest.get("batch_size"),
        "excluded_parent_ids_sha256": (
            exclusion.get("sha256") if isinstance(exclusion, Mapping) else None
        ),
    }
    _require(
        isinstance(replay, Mapping) and _typed_equal(dict(replay), expected),
        "deterministic handcode selection replay is absent or differs",
    )


def load_handcode_manifest(handcode_dir: Path) -> tuple[dict[str, Any], str]:
    path = handcode_dir / "handcode_manifest.json"
    _require(path.is_file(), "handcode_manifest.json is absent")
    manifest = json.loads(path.read_text(encoding="utf-8"))
    _require(isinstance(manifest, dict), "handcode manifest is not an object")
    _require(
        manifest.get("format") == "candidate-extraction-handcode-v2",
        "handcode manifest format differs",
    )
    _require(
        str(manifest.get("schema_version")) == "2.1",
        "handcode schema is not v2.1",
    )
    _require(
        manifest.get("generation_config_sha256")
        == EXPECTED_GENERATION_CONFIG_SHA256,
        "handcode generation config is not the frozen v2.1 identity",
    )
    _require(
        manifest.get("structured_decoding_backend")
        == STRUCTURED_DECODING_BACKEND,
        "handcode structured-decoding backend differs",
    )
    _require(
        manifest.get("structured_decoding_disable_any_whitespace")
        is STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
        "handcode structured-decoding whitespace policy differs",
    )
    _require_raw_structural_whitespace_gate(manifest)
    _require_suffix_p_contract_identity(manifest, "handcode")
    _require_line_ledger_identity(manifest, "handcode")
    _require_line_ledger_identity(manifest, "handcode")
    _require_model_item_guard_gate(manifest, "handcode")
    checks = manifest.get("technical_checks")
    _require(isinstance(checks, dict) and checks.get("passed") is True, "technical gate did not pass")
    _require(
        _typed_equal(checks.get("schema_valid_rate"), 1.0),
        "schema validity is below 100%",
    )
    _require(
        _typed_equal(checks.get("exact_span_rate"), 1.0),
        "exact-span validity is below 100%",
    )
    _require(
        _typed_equal(checks.get("silently_selected_overlap_conflicts"), 0),
        "an overlap conflict was selected silently",
    )
    _require(
        _typed_equal(checks.get("truncated_segments"), 0),
        "truncated segments remain",
    )
    _require_technical_raw_structural_whitespace_gate(checks)
    _require_suffix_p_contract_identity(checks, "technical")
    _require_line_ledger_identity(checks, "technical", technical=True)
    _require_user_message_replay_gate(checks, "technical")
    _require_model_item_guard_gate(checks, "technical")
    tasks = manifest.get("tasks")
    _require(isinstance(tasks, list) and bool(tasks), "handcode manifest has no tasks")
    task_ids = [str(task.get("task_id") or "") for task in tasks]
    _require(all(task_ids) and len(task_ids) == len(set(task_ids)), "task IDs are blank or duplicate")
    try:
        expected_assignments = build_assignment_batches(tasks)
    except ValueError as error:
        raise HumanValidationError(str(error)) from error
    _require(
        _typed_equal(
            manifest.get("assignment_batches"), expected_assignments
        )
        and manifest.get("known_omission_tasks_separate_from_holdout") is True,
        "handcode assignment batches differ",
    )
    return manifest, sha256_file(path)


def load_task_payloads(
    handcode_dir: Path, manifest: Mapping[str, Any]
) -> dict[str, dict[str, Any]]:
    payloads: dict[str, dict[str, Any]] = {}
    for entry in manifest["tasks"]:
        task_id = str(entry["task_id"])
        path = _safe_file(handcode_dir, entry.get("task_json"), f"{task_id} task")
        payload = json.loads(path.read_text(encoding="utf-8"))
        _require(payload.get("task_id") == task_id, f"{task_id} task payload ID differs")
        _require(
            type(entry.get("item_count")) is int
            and len(payload.get("items") or []) == entry["item_count"],
            f"{task_id} task item count differs",
        )
        html_spec = entry.get("html")
        if html_spec is not None:
            _safe_file(handcode_dir, html_spec, f"{task_id} HTML")
        payloads[task_id] = payload
    return payloads


def load_result_payloads(
    expected_task_ids: set[str],
    result_paths: Iterable[Path] = (),
    results_dir: Path | None = None,
) -> tuple[dict[str, dict[str, Any]], dict[str, str]]:
    paths = list(result_paths)
    if results_dir is not None:
        _require(results_dir.is_dir(), f"results directory is absent: {results_dir}")
        paths.extend(sorted(results_dir.rglob("*_results.json")))
    unique_paths = list(dict.fromkeys(path.resolve() for path in paths))
    results: dict[str, dict[str, Any]] = {}
    hashes: dict[str, str] = {}
    for path in unique_paths:
        _require(path.is_file(), f"result file is absent: {path}")
        value = json.loads(path.read_text(encoding="utf-8"))
        task_id = str(value.get("task_id") or "")
        _require(task_id in expected_task_ids, f"unexpected result task ID: {task_id}")
        _require(task_id not in results, f"duplicate results for task: {task_id}")
        results[task_id] = value
        hashes[task_id] = sha256_file(path)
    missing = expected_task_ids - set(results)
    _require(not missing, f"results are missing for {len(missing)} tasks: {sorted(missing)[:3]}")
    return results, hashes


def _question_required(question: Mapping[str, Any]) -> bool:
    if isinstance(question.get("required"), bool):
        return bool(question["required"])
    return question.get("type") not in {"text", "structured", "multiselect"}


def _question_visible(question: Mapping[str, Any], answers: Mapping[str, Any]) -> bool:
    condition = question.get("show_if")
    if not condition:
        return True
    value = answers.get(condition.get("question"))
    if "in" in condition:
        return value in condition["in"]
    return value == condition.get("equals")


def _answer_is_valid(question: Mapping[str, Any], value: Any) -> bool:
    kind = question.get("type")
    if kind == "yesno":
        return value in {"yes", "no"}
    if kind == "select":
        return value in question.get("options", [])
    if kind == "multiselect":
        return isinstance(value, list) and all(item in question.get("options", []) for item in value)
    if kind in {"scale", "numeric"}:
        return isinstance(value, (int, float)) and not isinstance(value, bool)
    if kind == "text":
        return isinstance(value, str)
    if kind == "structured":
        if not isinstance(value, list):
            return False
        fields = {field["id"] for field in question.get("fields", [])}
        return all(
            isinstance(row, dict)
            and set(row) == fields
            and all(str(row[field] or "").strip() for field in fields)
            for row in value
        )
    return False


def validate_result_payload(
    task: Mapping[str, Any], result: Mapping[str, Any]
) -> dict[str, dict[str, Any]]:
    task_id = str(task["task_id"])
    _require(result.get("format") == "handcode-results-v1", f"{task_id} result format differs")
    _require(result.get("task_id") == task_id, f"{task_id} result ID differs")
    _require(bool(str(result.get("exported_at") or "")), f"{task_id} lacks export time")
    items = task.get("items")
    questions = task.get("questions")
    _require(isinstance(items, list) and isinstance(questions, list), f"{task_id} task is invalid")
    item_ids = [str(item["id"]) for item in items]
    _require(len(item_ids) == len(set(item_ids)), f"{task_id} has duplicate item IDs")
    responses = result.get("responses")
    _require(isinstance(responses, list), f"{task_id} responses are not a list")
    response_ids = [str(response.get("item_id") or "") for response in responses]
    _require(len(response_ids) == len(set(response_ids)), f"{task_id} has duplicate responses")
    _require(set(response_ids) == set(item_ids), f"{task_id} response coverage differs")
    _require(result.get("n_items") == len(item_ids), f"{task_id} n_items differs")
    _require(result.get("n_complete") == len(item_ids), f"{task_id} n_complete differs")

    question_map = {str(question["id"]): question for question in questions}
    question_map["notes"] = {"id": "notes", "type": "text", "required": False}
    by_item: dict[str, dict[str, Any]] = {}
    task_items = {str(item["id"]): item for item in items}
    for response in responses:
        item_id = str(response["item_id"])
        _require(response.get("status") == "complete", f"{task_id}/{item_id} is incomplete")
        answers = response.get("answers")
        _require(isinstance(answers, dict), f"{task_id}/{item_id} answers are invalid")
        unknown = set(answers) - set(question_map)
        _require(not unknown, f"{task_id}/{item_id} has unknown answers: {sorted(unknown)}")
        for question_id, value in answers.items():
            _require(
                _answer_is_valid(question_map[question_id], value),
                f"{task_id}/{item_id}/{question_id} has an invalid value",
            )
        for question in questions:
            if not _question_visible(question, answers):
                continue
            if _question_required(question):
                question_id = str(question["id"])
                _require(question_id in answers, f"{task_id}/{item_id} lacks {question_id}")
                if question.get("type") == "structured":
                    _require(bool(answers[question_id]), f"{task_id}/{item_id}/{question_id} is empty")
                    if question_id == "missed_records":
                        source_displays = [
                            display.get("content")
                            for display in task_items[item_id].get("display", [])
                            if display.get("label") == "Complete archived page text"
                        ]
                        _require(
                            len(source_displays) == 1 and isinstance(source_displays[0], str),
                            f"{task_id}/{item_id} lacks complete source text",
                        )
                        for missed in answers[question_id]:
                            _require(
                                missed["evidence"] in source_displays[0],
                                f"{task_id}/{item_id} missed-record evidence is not source-exact",
                            )
        by_item[item_id] = dict(answers)
    return by_item


def _task_and_sample_checks(
    manifest: Mapping[str, Any],
    task_payloads: Mapping[str, dict[str, Any]],
) -> dict[tuple[str, str], list[dict[str, Any]]]:
    samples: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    excluded = set(manifest.get("excluded_parent_ids", {}).get("values") or [])
    seen_items: set[str] = set()
    for entry in manifest["tasks"]:
        task_id = str(entry["task_id"])
        route = str(entry.get("route") or "")
        kind = str(entry.get("kind") or "")
        regular = kind in {"precision", "recall"} and route in ROUTES
        omission_check = kind == "known_omission" and route == "all"
        _require(regular or omission_check, f"{task_id} has invalid metadata")
        item_ids = {str(item["id"]) for item in task_payloads[task_id]["items"]}
        metadata = entry.get("sample_items")
        _require(isinstance(metadata, list), f"{task_id} lacks sample metadata")
        metadata_ids = {str(item.get("item_id") or "") for item in metadata}
        _require(metadata_ids == item_ids, f"{task_id} sample metadata coverage differs")
        _require(not (seen_items & item_ids), f"{task_id} repeats an item from another task")
        seen_items.update(item_ids)
        for item in metadata:
            _require(item.get("kind") == kind, f"{task_id} sample metadata differs")
            if omission_check:
                _require(item.get("route") in ROUTES, f"{task_id} omission route differs")
                _require(
                    item.get("parent_document_id") in excluded,
                    f"{task_id} omission parent is not in the tuning exclusions",
                )
                _require(
                    item.get("record_id") in (item.get("matched_record_ids") or []),
                    f"{task_id} omission record linkage differs",
                )
                samples[("known_omission", "all")].append(
                    dict(item, task_id=task_id)
                )
            else:
                _require(item.get("route") == route, f"{task_id} sample route differs")
                _require(item.get("parent_document_id") not in excluded, f"{task_id} includes a tuning parent")
                samples[(kind, route)].append(dict(item, task_id=task_id))

    expected = {
        ("precision", "biography"): PRECISION_MINIMUMS["biography"],
        ("precision", "issues"): PRECISION_MINIMUMS["issues"],
        ("precision", "endorsements"): PRECISION_MINIMUMS["endorsements"],
        ("recall", "biography"): RECALL_PER_ROUTE,
        ("recall", "issues"): RECALL_PER_ROUTE,
        ("recall", "endorsements"): RECALL_PER_ROUTE,
    }
    for key, count in expected.items():
        _require(len(samples[key]) == count, f"{key} sample has {len(samples[key])}; needs {count}")
    biography_counts = Counter(
        item["subtype"] for item in samples[("precision", "biography")]
    )
    _require(
        biography_counts == Counter({field: BIOGRAPHY_PER_FIELD for field in BIOGRAPHY_FIELDS_V2}),
        f"biography precision is not {BIOGRAPHY_PER_FIELD} records per released field",
    )
    selection = manifest.get("selection")
    _require(isinstance(selection, Mapping), "handcode manifest lacks selection provenance")
    pool_levels = selection.get("pool_observed_levels")
    _require(
        isinstance(pool_levels, Mapping),
        "handcode manifest lacks precision-pool levels",
    )
    required_strata = {
        "issues": {
            "year", "party", "page_length", "segmentation", "heading",
            "statement_kind",
        },
        "endorsements": {
            "entity_type", "endorser_category", "support_mode",
            "context_scope",
        },
    }
    for route, fields in required_strata.items():
        declared = pool_levels.get(route)
        _require(
            isinstance(declared, Mapping) and set(declared) == fields,
            f"{route} precision-pool strata differ",
        )
        for field in fields:
            values = declared[field]
            _require(
                isinstance(values, list)
                and bool(values)
                and len(values) == len(set(str(value) for value in values)),
                f"{route} precision-pool {field} levels are invalid",
            )
            observed = {str(value) for value in values}
            sampled = {
                str(item.get(field))
                for item in samples[("precision", route)]
            }
            _require(
                sampled == observed,
                f"{route} precision does not cover every observed {field}",
            )
    for route in ROUTES:
        recall_rows = samples[("recall", route)]
        _require(
            sum(bool(item.get("zero_result")) for item in recall_rows)
            >= RECALL_ZERO_TARGET,
            f"{route} recall has fewer than {RECALL_ZERO_TARGET} zero-result pages",
        )
        _require(
            sum(bool(item.get("multi_window")) for item in recall_rows)
            >= RECALL_MULTI_TARGET,
            f"{route} recall has fewer than {RECALL_MULTI_TARGET} multi-window pages",
        )
        precision_parents = {
            item["parent_document_id"] for item in samples[("precision", route)]
        }
        recall_parents = {item["parent_document_id"] for item in recall_rows}
        _require(
            not (precision_parents & recall_parents),
            f"{route} precision and recall samples share parent pages",
        )
    for field in ("title", "affiliation", "jurisdiction"):
        count = sum(
            bool(item.get(f"{field}_nonnull"))
            for item in samples[("precision", "endorsements")]
        )
        _require(count >= 20, f"endorsement precision has fewer than 20 non-null {field} examples")
    omission_rows = samples[("known_omission", "all")]
    omission_ids = {str(item.get("omission_id") or "") for item in omission_rows}
    _require(len(omission_ids) == 11 and "" not in omission_ids, "known-omission task does not cover 11 IDs")
    for omission_id in omission_ids:
        rows = [item for item in omission_rows if item["omission_id"] == omission_id]
        declared = {tuple(item.get("matched_record_ids") or []) for item in rows}
        _require(len(declared) == 1, f"{omission_id} matched-record inventory differs")
        _require(
            {item["record_id"] for item in rows} == set(next(iter(declared))),
            f"{omission_id} linked-record task coverage differs",
        )
    return samples


def score_handcode_results(
    manifest: Mapping[str, Any],
    manifest_sha256: str,
    task_payloads: Mapping[str, dict[str, Any]],
    results: Mapping[str, dict[str, Any]],
    result_hashes: Mapping[str, str],
    *,
    confirm_human_only: bool,
    selection_replay: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    _require(confirm_human_only, "human-only coding attestation is required")
    _require_handcode_selection_replay(
        selection_replay,
        manifest,
        manifest_sha256,
        task_payloads,
    )
    _require(
        manifest.get("generation_config_sha256")
        == EXPECTED_GENERATION_CONFIG_SHA256,
        "handcode generation config is not the frozen v2.1 identity",
    )
    _require(
        manifest.get("structured_decoding_backend")
        == STRUCTURED_DECODING_BACKEND,
        "handcode structured-decoding backend differs",
    )
    _require(
        manifest.get("structured_decoding_disable_any_whitespace")
        is STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
        "handcode structured-decoding whitespace policy differs",
    )
    _require_raw_structural_whitespace_gate(manifest)
    _require_suffix_p_contract_identity(manifest, "handcode")
    _require_technical_raw_structural_whitespace_gate(
        manifest.get("technical_checks")
    )
    _require_suffix_p_contract_identity(
        manifest.get("technical_checks"), "technical"
    )
    _require_line_ledger_identity(
        manifest.get("technical_checks"), "technical", technical=True
    )
    _require_user_message_replay_gate(
        manifest.get("technical_checks"), "technical"
    )
    _require(
        _typed_equal(
            manifest["technical_checks"].get(
                "silently_selected_overlap_conflicts"
            ),
            0,
        ),
        "an overlap conflict was selected silently",
    )
    _require(
        _typed_equal(
            manifest["technical_checks"].get("truncated_segments"), 0
        ),
        "truncated segments remain",
    )
    _require_model_item_guard_gate(manifest, "handcode")
    _require_model_item_guard_gate(
        manifest.get("technical_checks"), "technical"
    )
    samples = _task_and_sample_checks(manifest, task_payloads)
    answers_by_task: dict[str, dict[str, dict[str, Any]]] = {}
    for task_id, task in task_payloads.items():
        _require(task_id in results, f"results are absent for {task_id}")
        answers_by_task[task_id] = validate_result_payload(task, results[task_id])

    route_reports: dict[str, dict[str, Any]] = {}
    for route in ROUTES:
        precision_rows = samples[("precision", route)]
        dimensions = PRECISION_QUESTIONS[route]
        correct: list[bool] = []
        subtype_results: dict[str, list[bool]] = defaultdict(list)
        presence: list[bool] = []
        combined_support: list[bool] = []
        for item in precision_rows:
            answers = answers_by_task[item["task_id"]][item["item_id"]]
            row_correct = all(answers.get(question) == "yes" for question in dimensions)
            correct.append(row_correct)
            subtype_results[str(item["subtype"])].append(row_correct)
            if route == "endorsements":
                presence.append(answers.get("endorser_present") == "yes")
                combined_support.append(
                    answers.get("combined_record_context_support") == "yes"
                )
        subtype_accuracy = {
            subtype: {
                "n": len(values),
                "all_dimension_accuracy": sum(values) / len(values),
            }
            for subtype, values in sorted(subtype_results.items())
        }
        minimum_subtype = min(
            value["all_dimension_accuracy"] for value in subtype_accuracy.values()
        )

        recall_rows = samples[("recall", route)]
        recall_correct = [
            answers_by_task[item["task_id"]][item["item_id"]].get(
                "all_requested_records_captured"
            ) == "yes"
            for item in recall_rows
        ]
        all_dimension_accuracy = sum(correct) / len(correct)
        complete_page_recall = sum(recall_correct) / len(recall_correct)
        checks = {
            "precision_sample_size": len(correct) >= PRECISION_MINIMUMS[route],
            "all_dimension_accuracy": all_dimension_accuracy >= ALL_DIMENSION_GATE,
            "minimum_subtype_accuracy": minimum_subtype >= SUBTYPE_GATE,
            "recall_sample_size": len(recall_correct) >= RECALL_PER_ROUTE,
            "complete_page_recall": complete_page_recall >= RECALL_GATE,
        }
        report: dict[str, Any] = {
            "precision_n": len(correct),
            "all_dimension_accurate_n": sum(correct),
            "all_dimension_accuracy": all_dimension_accuracy,
            "subtype_accuracy": subtype_accuracy,
            "minimum_subtype_accuracy": minimum_subtype,
            "recall_n": len(recall_correct),
            "complete_pages_n": sum(recall_correct),
            "complete_page_recall": complete_page_recall,
            "checks": checks,
        }
        if route == "endorsements":
            presence_rate = sum(presence) / len(presence)
            support_rate = sum(combined_support) / len(combined_support)
            checks.update({
                "sampled_presence_rate": presence_rate == 1.0,
                "sampled_combined_support_rate": support_rate == 1.0,
            })
            report.update({
                "sampled_presence_rate": presence_rate,
                "sampled_combined_support_rate": support_rate,
                "nonnull_optional_fields": {
                    field: sum(bool(item.get(f"{field}_nonnull")) for item in precision_rows)
                    for field in ("title", "affiliation", "jurisdiction")
                },
            })
        report["passed"] = all(checks.values())
        route_reports[route] = report

    omission_rows = samples[("known_omission", "all")]
    omission_confirmations: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for item in omission_rows:
        answers = answers_by_task[item["task_id"]][item["item_id"]]
        confirmed = (
            answers.get("semantic_recovery_confirmed") == "yes"
            and answers.get("recovery_evidence_confirmed") == "yes"
        )
        omission_confirmations[item["omission_id"]].append({
            "record_id": item["record_id"],
            "semantic_recovery_confirmed": (
                answers.get("semantic_recovery_confirmed") == "yes"
            ),
            "recovery_evidence_confirmed": (
                answers.get("recovery_evidence_confirmed") == "yes"
            ),
            "pair_confirmed": confirmed,
        })
    omission_items: list[dict[str, Any]] = []
    for omission_id, confirmations in sorted(omission_confirmations.items()):
        metadata = next(
            item for item in omission_rows if item["omission_id"] == omission_id
        )
        confirmed_record_ids = sorted(
            item["record_id"] for item in confirmations if item["pair_confirmed"]
        )
        omission_items.append({
            "omission_id": omission_id,
            "parent_document_id": metadata["parent_document_id"],
            "route": metadata["route"],
            "matched_record_ids": metadata["matched_record_ids"],
            "confirmed_record_ids": confirmed_record_ids,
            "semantic_recovery_confirmed": bool(confirmed_record_ids),
            "record_link_confirmations": confirmations,
        })
    confirmed_omissions = sum(
        item["semantic_recovery_confirmed"] for item in omission_items
    )
    omission_gate_passed = len(omission_items) == 11 and confirmed_omissions == 11

    technical_passed = (
        manifest["technical_checks"].get("passed") is True
        and _typed_equal(
            manifest["technical_checks"].get("schema_valid_rate"), 1.0
        )
        and _typed_equal(
            manifest["technical_checks"].get("exact_span_rate"), 1.0
        )
        and _typed_equal(
            manifest["technical_checks"].get("user_message_hash_replay_rate"),
            1.0,
        )
        and _typed_equal(
            manifest["technical_checks"].get(
                "silently_selected_overlap_conflicts"
            ),
            0,
        )
        and _typed_equal(
            manifest["technical_checks"].get("truncated_segments"), 0
        )
        and manifest["technical_checks"].get(
            "pre_alignment_model_item_guard_gate_passed"
        )
        is True
        and manifest["technical_checks"].get(
            "array_item_cap_reached_outputs"
        )
        == 0
        and manifest["technical_checks"].get(
            "model_item_repetition_error_outputs"
        )
        == 0
    )
    overall_passed = technical_passed and omission_gate_passed and all(
        report["passed"] for report in route_reports.values()
    )
    exported_times = sorted(str(result.get("exported_at")) for result in results.values())
    return {
        "format": "candidate-extraction-human-validation-v2",
        "schema_version": "2.1",
        "status": "passed" if overall_passed else "failed",
        "independent_human_only": True,
        "coding_source": "human",
        "human_only_attestation": "confirmed_at_scoring",
        "prior_ai_assisted_audit_role": "tuning_only",
        "tuning_audit_excluded": True,
        "scored_at_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "first_result_exported_at": exported_times[0],
        "last_result_exported_at": exported_times[-1],
        "bundle_manifest_sha256_at_sampling": manifest["bundle_manifest_sha256"],
        "handcode_manifest_sha256": manifest_sha256,
        "handcode_selection_replay": dict(selection_replay or {}),
        "source_corpus_sha256": manifest.get("source_corpus_sha256"),
        "extraction_contract_sha256": manifest.get(
            "extraction_contract_sha256"
        ),
        "endorsement_context_provenance_spec_sha256": manifest.get(
            "endorsement_context_provenance_spec_sha256"
        ),
        "endorsement_line_ledger_contract_sha256": manifest.get(
            "endorsement_line_ledger_contract_sha256"
        ),
        "assignment_batches": manifest.get("assignment_batches"),
        "user_message_template_id": manifest.get(
            "user_message_template_id"
        ),
        "user_message_template_sha256": manifest.get(
            "user_message_template_sha256"
        ),
        "array_item_cap_spec_id": manifest.get("array_item_cap_spec_id"),
        "array_item_cap_spec_sha256": manifest.get(
            "array_item_cap_spec_sha256"
        ),
        "route_array_item_caps": manifest.get("route_array_item_caps"),
        "model_item_repetition_spec_id": manifest.get(
            "model_item_repetition_spec_id"
        ),
        "model_item_repetition_spec_sha256": manifest.get(
            "model_item_repetition_spec_sha256"
        ),
        "technical_checks": manifest["technical_checks"],
        "technical_gate_passed": technical_passed,
        "known_omission_human_confirmation": {
            "expected_n": 11,
            "confirmed_n": confirmed_omissions,
            "all_confirmed": omission_gate_passed,
            "items": omission_items,
        },
        "results": [
            {"task_id": task_id, "sha256": result_hashes[task_id]}
            for task_id in sorted(result_hashes)
        ],
        "gates": {
            "all_dimension_accuracy_minimum": ALL_DIMENSION_GATE,
            "subtype_accuracy_minimum": SUBTYPE_GATE,
            "complete_page_recall_minimum": RECALL_GATE,
            "endorsement_presence_required": 1.0,
            "endorsement_combined_support_required": 1.0,
        },
        "routes": route_reports,
        "gates_passed": overall_passed,
    }


def _atomic_write(path: Path, content: str) -> None:
    temporary = path.with_suffix(path.suffix + f".tmp-{os.getpid()}")
    temporary.write_text(content, encoding="utf-8")
    os.replace(temporary, path)


def finalize_passing_bundle(
    bundle_dir: Path,
    handcode_manifest: Mapping[str, Any],
    report: Mapping[str, Any],
) -> str:
    _require(report.get("gates_passed") is True, "refusing to finalize a failed validation")
    replay = report.get("handcode_selection_replay")
    _require(
        isinstance(replay, Mapping)
        and replay.get("version")
        == "candidate_extraction_handcode_selection_replay_v1"
        and replay.get("passed") is True
        and replay.get("bundle_manifest_sha256")
        == handcode_manifest.get("bundle_manifest_sha256"),
        "refusing to finalize without deterministic handcode artifact replay",
    )
    manifest_path = bundle_dir / "bundle_manifest.json"
    _require(manifest_path.is_file(), "bundle manifest is absent")
    old_manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(old_manifest_bytes)
    _require(
        hashlib.sha256(old_manifest_bytes).hexdigest()
        == handcode_manifest.get("bundle_manifest_sha256"),
        "bundle manifest changed after sampling",
    )
    segment_spec = copy.deepcopy(manifest.get("segments_file"))
    _require(isinstance(segment_spec, dict), "bundle segment specification is invalid")
    _require(
        str(segment_spec.get("file")) == str(handcode_manifest.get("segments_file")),
        "frozen segment file name changed after sampling",
    )
    segment_path = _safe_file(bundle_dir, segment_spec, "frozen segment")
    _require(
        sha256_file(segment_path) == handcode_manifest.get("segments_file_sha256"),
        "frozen segment bytes changed after sampling",
    )
    ledger = manifest.get("endorsement_line_ledger")
    ledger_spec = (
        ledger.get("private_file") if isinstance(ledger, Mapping) else None
    )
    _require(
        isinstance(ledger_spec, dict),
        "bundle endorsement line-ledger specification is invalid",
    )
    ledger_path = _safe_file(
        bundle_dir, ledger_spec, "frozen endorsement line ledger"
    )
    omission_spec = handcode_manifest.get("known_omission_recovery_file")
    _require(isinstance(omission_spec, dict), "known-omission file specification is invalid")
    _safe_file(bundle_dir, omission_spec, "known-omission recovery")
    human_spec = manifest.get("human_validation_file")
    _require(isinstance(human_spec, dict), "bundle human-validation specification is invalid")
    _safe_file(bundle_dir, human_spec, "pending human validation")
    human_relative = Path(str(human_spec.get("file") or ""))
    _require(
        str(human_relative) == "human_validation.json",
        "bundle human-validation file name differs",
    )
    human_path = bundle_dir / human_relative
    old_human_bytes = human_path.read_bytes() if human_path.exists() else None
    report_content = json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True) + "\n"
    try:
        _atomic_write(human_path, report_content)
        report_hash = sha256_file(human_path)
        manifest["human_validation_file"]["sha256"] = report_hash
        manifest["release_status"] = "human_validation_passed"
        manifest["human_validation_gate_passed"] = True
        _require(manifest.get("segments_file") == segment_spec, "segment metadata changed")
        _require(
            manifest.get("endorsement_line_ledger") == ledger,
            "endorsement line-ledger metadata changed",
        )
        _atomic_write(
            manifest_path,
            json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        )
        _require(manifest["segments_file"] == segment_spec, "segment metadata changed")
        _require(sha256_file(segment_path) == segment_spec["sha256"], "segment bytes changed")
        _require(
            sha256_file(ledger_path) == ledger_spec["sha256"],
            "endorsement line-ledger bytes changed",
        )
        _atomic_write(
            bundle_dir / "human_validation.sha256",
            f"{report_hash}  human_validation.json\n",
        )
        return report_hash
    except Exception:
        _atomic_write(manifest_path, old_manifest_bytes.decode("utf-8"))
        if old_human_bytes is not None:
            _atomic_write(human_path, old_human_bytes.decode("utf-8"))
        raise


def write_failed_report(path: Path, report: Mapping[str, Any]) -> str:
    _require(not path.exists(), f"failed report already exists: {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    content = json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True) + "\n"
    _atomic_write(path, content)
    digest = sha256_file(path)
    _atomic_write(path.with_suffix(".sha256"), f"{digest}  {path.name}\n")
    return digest


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--bundle-dir", type=Path, required=True)
    parser.add_argument("--handcode-dir", type=Path, required=True)
    parser.add_argument("--results-dir", type=Path)
    parser.add_argument("--result", type=Path, action="append", default=[])
    parser.add_argument(
        "--confirm-human-only",
        action="store_true",
        help="Attest that a human completed every supplied result without AI coding.",
    )
    parser.add_argument(
        "--failed-report",
        type=Path,
        help="Path for a complete but failed report. The frozen bundle stays pending.",
    )
    args = parser.parse_args(argv)

    manifest, manifest_hash = load_handcode_manifest(args.handcode_dir)
    tasks = load_task_payloads(args.handcode_dir, manifest)
    selection_replay = verify_handcode_selection_against_bundle(
        args.bundle_dir,
        args.handcode_dir,
        manifest,
        manifest_hash,
        tasks,
    )
    results, result_hashes = load_result_payloads(
        set(tasks), args.result, args.results_dir
    )
    report = score_handcode_results(
        manifest,
        manifest_hash,
        tasks,
        results,
        result_hashes,
        confirm_human_only=args.confirm_human_only,
        selection_replay=selection_replay,
    )
    if report["gates_passed"]:
        digest = finalize_passing_bundle(args.bundle_dir, manifest, report)
        print(canonical_json({
            "status": "passed",
            "human_validation_sha256": digest,
            "bundle_dir": str(args.bundle_dir),
        }))
        return 0
    failed_path = args.failed_report or args.handcode_dir / "human_validation_failed.json"
    digest = write_failed_report(failed_path, report)
    print(canonical_json({
        "status": "failed",
        "failed_report": str(failed_path),
        "sha256": digest,
        "bundle_unchanged": True,
    }))
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
