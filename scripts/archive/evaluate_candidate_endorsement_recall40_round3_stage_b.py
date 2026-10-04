#!/usr/bin/env python3
"""Authenticate frozen round-three Stage B before source-only human scoring."""
from __future__ import annotations

import argparse
import copy
import json
import re
import sys
from collections import Counter
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Mapping, Sequence


REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts.build_candidate_endorsement_line_ledger import (  # noqa: E402
    load_json_no_duplicates,
    sha256_file,
)
from scripts.hive.extract_candidate_endorsement_line_ledger_vllm import (  # noqa: E402
    FROZEN_GENERATION_SEED,
    _derive_output_row,
    _run_metadata,
    generation_config_payload,
    file_sha256,
    json_value_equal_typed,
    line_ledger_chunk_is_complete,
    read_gzip_jsonl,
    schema_variant_inventory,
    validate_input_manifest,
)
from src.candidate_endorsement_line_ledger import (  # noqa: E402
    FROZEN_RECALL40_ROUND3_STAGE_B_CONTRACT_SHA256,
    RECORD_EXTRACTION_STAGE,
    RECALL40_ROUND3_STAGE_B_SCOPE_SPEC,
    align_record_extraction_output,
    get_frozen_recall40_round3_stage_b_contract,
    sha256_json,
    validate_record_extraction_output,
)


INPUT_EXPORT = REPO / (
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round3_stage_b_preflight_20260906a"
)
INPUT_MANIFEST_SHA256 = (
    "261ea3b87af846dec354428b373c2bdb85a5f3e1d9745f916e501791b75d6f11"
)
EXPECTED_GENERATION_CONFIG_SHA256 = (
    "e485c3462c5f8c4b093508b44de8d09e05763df5d9984459d52fef04459429db"
)
EXPECTED_BATCH_COUNT = 298
EXPECTED_ANCHOR_COUNT = 2_221
EXPECTED_SELECTED_PARENT_COUNT = 40
EXPECTED_PARENT_WITH_ANCHORS_COUNT = 33
EXPECTED_CHUNK_COUNT = 5
EXPECTED_MAX_MODEL_LEN = 32_768
EXPECTED_MAX_OUTPUT_TOKENS = 16_384
SLURM_ARRAY_JOB_ID = "22685041"
LOCAL_LOG_DIR = REPO / (
    "build/hive_candidate_extraction/"
    "endorsement-recall40-round3-stage-b_22685041_logs"
)
SLURM_TERMINAL_SNAPSHOT = REPO / (
    "quality_reports/"
    "candidate_endorsement_recall40_round3_stage_b_slurm_22685041_2026-09-06.txt"
)
SLURM_TERMINAL_SNAPSHOT_SHA256 = (
    "ee8c5e76ed3edbe1053daee442f508760bf4c825e81ec51b610b126b49ae4334"
)
EXPECTED_OUTPUT_SHA256 = {
    "chunk_00000.out.jsonl.gz": (
        "905a29efecc9651c6a1a3fa45d32d063a1a891ad1998df0b84428ea70ca6a5a3"
    ),
    "chunk_00001.out.jsonl.gz": (
        "6656030c65e30139dd5a0739d3fd77844e2b8cc9432bcc3001fe63b688053eec"
    ),
    "chunk_00002.out.jsonl.gz": (
        "9d998337803fa046c15cd893f4ce801fc95446482831cacc2a37cb12882bb0b3"
    ),
    "chunk_00003.out.jsonl.gz": (
        "0f8ff3b2d4e7c072556997d36791f36e50add4df5594d07be10ef285e84aeb02"
    ),
    "chunk_00004.out.jsonl.gz": (
        "dff62146debb9880ead06cc4050c517120c510ba95731edc0efd07ff20fb1ae6"
    ),
}
EXPECTED_METADATA_SHA256 = {
    "run_metadata_shard_00000.json": (
        "ea9e588adbca6a896e2bb8f254ac30efb83891b42e11c1e522f2cb300c789aa3"
    ),
    "run_metadata_shard_00001.json": (
        "74b7517988fa03ba2ee34b8aa82eabad19627f80d6b1f020b978875ea4ba8a41"
    ),
    "run_metadata_shard_00002.json": (
        "83f64e01587204f4bf274c473f214c508cbc4651744edc9071a69ce29d188f59"
    ),
    "run_metadata_shard_00003.json": (
        "617b67cba3317a12985e8c1f93afb0988ea0d389e00174ab34ffd9bfb70cc731"
    ),
    "run_metadata_shard_00004.json": (
        "6a33d571a101f33f12c92ecf6c1afbe84e7163ab0af4fb6bf8433c7b09b4af5a"
    ),
}
EXPECTED_LOG_SHA256 = {
    "endorsement-line-ledger_22685041_0.log": (
        "3647061524dc84f56cea4136c2c148a88ec75dab7aacb48b04a1284f4ed2cdd8"
    ),
    "endorsement-line-ledger_22685041_1.log": (
        "c60618837296084269bbb20d99700f3ddc3bfcad394bf89a3e4437d8dea00e80"
    ),
    "endorsement-line-ledger_22685041_2.log": (
        "ea29af24fe8bdf180129f3f4668ee556f6be0379c43a773b0bf45c3a99022e23"
    ),
    "endorsement-line-ledger_22685041_3.log": (
        "735339cab1a5397f407198ceac5cfde2ccef29132d2e37efcffc136468031d18"
    ),
    "endorsement-line-ledger_22685041_4.log": (
        "26833c286d332cb50b02f82e127a8ef3fdfff8105201c8fe923f41ad429726fd"
    ),
}
EXPECTED_SLURM_TASK_JOB_IDS = (
    "22685042", "22685043", "22685048", "22685050", "22685041",
)

REPORT_VERSION = (
    "candidate_endorsement_recall40_round3_stage_b_suffix_a_technical_gate_v1"
)
EVALUATION_SCOPE = "frozen_private_recall40_round3_stage_b_suffix_a"
GO_DECISION = "GO_PREPARE_SOURCE_ONLY_HUMAN_TRUTH_SCORING"
NO_GO_DECISION = "NO_GO"
AUTHORIZED_NEXT_ACTION = "prepare_source_only_human_truth_scoring"

FIELD_OBSERVABILITY_FIELDS = (
    "endorser_name",
    "entity_type",
    "endorser_category",
    "title",
    "affiliation",
    "jurisdiction",
)
FIELD_OBSERVABILITY_COUNT_FIELDS = {
    "present_count",
    "nonnull_count",
    "null_count",
    "nonempty_string_count",
    "other_nonnull_count",
}
TECHNICAL_COUNT_FIELDS = {
    "input_batches",
    "output_rows",
    "parsed_rows",
    "nonparsed_rows",
    "finish_stop_rows",
    "technical_pass_rows",
    "technical_fail_rows",
    "technical_failure_parent_count",
    "schema_valid_rows",
    "schema_not_valid_rows",
    "validation_coverage_complete_rows",
    "alignment_present_rows",
    "alignment_public_coverage_complete_rows",
    "alignment_incomplete_rows",
    "model_anchor_count",
    "model_record_count",
    "aligned_record_count",
    "withheld_record_count",
    "unresolved_anchor_count",
    "duplicate_aligned_record_key_count",
    "duplicate_source_model_item_id_count",
}
CHECK_IDS = (
    "exact_batch_and_anchor_closure",
    "authenticated_output_inventory_and_metadata",
    "authenticated_local_logs_and_terminal_scheduler_census",
    "strict_restart_reuse_eligibility",
    "parsed_stop_and_technical_rows",
    "deterministic_schema_coverage",
    "deterministic_alignment_coverage",
    "aligned_record_key_uniqueness",
)
REPORT_FIELDS = {
    "version",
    "evaluation_scope",
    "gate_decision",
    "authorized_next_action",
    "input_manifest_sha256",
    "input_manifest_payload_sha256",
    "line_ledger_contract_sha256",
    "generation_config_sha256",
    "selected_parent_count",
    "parents_with_authenticated_anchors_count",
    "authenticated_anchor_count",
    "input_chunk_count",
    "output_chunk_count",
    "run_metadata_file_count",
    "output_file_inventory",
    "run_metadata_file_inventory",
    "local_log_file_count",
    "local_log_file_inventory",
    "slurm_array_job_id",
    "slurm_terminal_snapshot",
    "slurm_task_census",
    "slurm_task_census_sha256",
    "technical_counts",
    "parse_status_counts",
    "finish_reason_counts",
    "parse_error_reason_counts",
    "validation_error_reason_counts",
    "withheld_reason_counts",
    "field_observability_census",
    "aligned_record_counts_by_parent",
    "aligned_record_counts_by_parent_sha256",
    "ordered_output_batch_ids_sha256",
    "aligned_records_sha256",
    "withheld_records_sha256",
    "technical_failure_ledger_sha256",
    "run_artifacts_sha256",
    "typed_row_rederivation_complete",
    "strict_restart_reuse_eligible_chunk_count",
    "strict_restart_reuse_ineligible_chunk_count",
    "checks",
    "passed_check_count",
    "failed_check_count",
    "source_only_human_truth_scoring_preparation_authorized",
    "source_only_human_truth_scoring_authorized",
    "prediction_bearing_human_task_authorized",
    "recall_or_content_quality_established",
    "additional_hive_inference_authorized",
    "full_expansion_authorized",
    "release_authorized",
    "report_payload_sha256",
}


def _is_sha256(value: Any) -> bool:
    return (
        isinstance(value, str)
        and len(value) == 64
        and all(character in "0123456789abcdef" for character in value)
    )


def _exact_int(value: Any, label: str, *, minimum: int = 0) -> int:
    if type(value) is not int or value < minimum:
        raise ValueError(f"{label} must be an exact integer >= {minimum}")
    return value


def _exact_bool(value: Any, label: str) -> bool:
    if type(value) is not bool:
        raise ValueError(f"{label} must be an exact boolean")
    return value


def _string_count_map(values: Any, label: str) -> dict[str, int]:
    if not isinstance(values, Mapping):
        raise ValueError(f"{label} must be a mapping")
    result: dict[str, int] = {}
    for key, value in values.items():
        if not isinstance(key, str) or not key:
            raise ValueError(f"{label} has an invalid key")
        result[key] = _exact_int(value, f"{label}.{key}")
    return dict(sorted(result.items()))


def _counter(values: Sequence[str]) -> dict[str, int]:
    return dict(sorted(Counter(values).items()))


def _require_unique_strings(values: Any, label: str) -> list[str]:
    if (
        not isinstance(values, list)
        or any(not isinstance(value, str) or not value for value in values)
        or len(values) != len(set(values))
    ):
        raise ValueError(f"{label} must be unique nonempty strings")
    return list(values)


def _manifest_chunk_entries(manifest: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    files = manifest.get("files")
    chunks = files.get("chunks") if isinstance(files, Mapping) else None
    if not isinstance(chunks, list) or not chunks:
        raise ValueError("Stage-B manifest chunk inventory is invalid")
    entries: list[Mapping[str, Any]] = []
    names: list[str] = []
    for index, entry in enumerate(chunks):
        if not isinstance(entry, Mapping):
            raise ValueError("Stage-B manifest chunk entry is invalid")
        name = entry.get("file")
        if not isinstance(name, str) or name != f"chunk_{index:05d}.jsonl.gz":
            raise ValueError("Stage-B manifest chunk order differs")
        _exact_int(entry.get("records"), f"manifest chunk {name} records", minimum=1)
        if not _is_sha256(entry.get("sha256")):
            raise ValueError(f"manifest chunk {name} hash is invalid")
        names.append(name)
        entries.append(entry)
    if len(names) != len(set(names)):
        raise ValueError("Stage-B manifest chunk names are duplicated")
    return entries


def _artifact_file_entries(
    artifacts: Mapping[str, Any], field: str, *, records: bool,
) -> list[Mapping[str, Any]]:
    values = artifacts.get(field)
    if not isinstance(values, list):
        raise ValueError(f"run artifacts {field} is not a list")
    result: list[Mapping[str, Any]] = []
    names: list[str] = []
    for entry in values:
        if not isinstance(entry, Mapping) or set(entry) != (
            {"file", "sha256", "bytes", "records"}
            if records else {"file", "sha256"}
        ):
            raise ValueError(f"run artifacts {field} entry shape differs")
        name = entry.get("file")
        if not isinstance(name, str) or not name or not _is_sha256(entry.get("sha256")):
            raise ValueError(f"run artifacts {field} entry identity is invalid")
        if records:
            _exact_int(entry.get("bytes"), f"run artifacts {field} bytes", minimum=1)
            _exact_int(entry.get("records"), f"run artifacts {field} records", minimum=1)
        names.append(name)
        result.append(entry)
    if len(names) != len(set(names)):
        raise ValueError(f"run artifacts {field} filenames are duplicated")
    return result


def _metadata_file_entries(artifacts: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    values = artifacts.get("run_metadata_files")
    if not isinstance(values, list):
        raise ValueError("run artifacts metadata inventory is not a list")
    result: list[Mapping[str, Any]] = []
    names: list[str] = []
    for entry in values:
        if (
            not isinstance(entry, Mapping)
            or set(entry) != {"file", "sha256", "bytes"}
            or not isinstance(entry.get("file"), str)
            or not entry["file"]
            or not _is_sha256(entry.get("sha256"))
        ):
            raise ValueError("run artifacts metadata entry is invalid")
        _exact_int(entry.get("bytes"), "run artifacts metadata bytes", minimum=1)
        names.append(str(entry["file"]))
        result.append(entry)
    if len(names) != len(set(names)):
        raise ValueError("run artifacts metadata filenames are duplicated")
    return result


def _log_file_entries(artifacts: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    values = artifacts.get("local_log_files")
    if not isinstance(values, list):
        raise ValueError("run artifacts local-log inventory is not a list")
    result: list[Mapping[str, Any]] = []
    names: list[str] = []
    for entry in values:
        if (
            not isinstance(entry, Mapping)
            or set(entry) != {"file", "sha256", "bytes"}
            or not isinstance(entry.get("file"), str)
            or not entry["file"]
            or not _is_sha256(entry.get("sha256"))
        ):
            raise ValueError("run artifacts local-log entry is invalid")
        _exact_int(entry.get("bytes"), "run artifacts local-log bytes", minimum=1)
        names.append(str(entry["file"]))
        result.append(entry)
    if len(names) != len(set(names)):
        raise ValueError("run artifacts local-log filenames are duplicated")
    return result


SLURM_SNAPSHOT_FIELDS = {
    "file", "sha256", "bytes", "captured_at_utc", "source", "command",
}
SLURM_TASK_FIELDS = {
    "array_job_id",
    "array_task_id",
    "slurm_job_id",
    "metadata_started_at_utc",
    "metadata_completed_at_utc",
    "metadata_host",
    "output_row_count",
    "technical_pass_row_count",
    "technical_fail_row_count",
    "log_file",
    "log_sha256",
    "log_bytes",
    "log_processed_batch_count",
    "log_technical_failure_count",
    "strict_restart_reuse_eligible",
    "terminal_state",
    "terminal_exit_code",
    "terminal_elapsed",
    "terminal_start",
    "terminal_end",
    "terminal_node",
    "terminal_restarts",
}


def _parse_iso_datetime(value: Any, label: str, *, timezone_required: bool) -> datetime:
    if not isinstance(value, str) or not value:
        raise ValueError(f"{label} must be a nonempty ISO timestamp")
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as error:
        raise ValueError(f"{label} must be an ISO timestamp") from error
    if timezone_required and parsed.tzinfo is None:
        raise ValueError(f"{label} must include a timezone")
    return parsed


def _slurm_snapshot_entry(artifacts: Mapping[str, Any]) -> Mapping[str, Any]:
    entry = artifacts.get("slurm_terminal_snapshot")
    if (
        not isinstance(entry, Mapping)
        or set(entry) != SLURM_SNAPSHOT_FIELDS
        or not isinstance(entry.get("file"), str)
        or not entry["file"]
        or not _is_sha256(entry.get("sha256"))
        or not isinstance(entry.get("source"), str)
        or not entry["source"]
        or not isinstance(entry.get("command"), str)
        or not entry["command"]
    ):
        raise ValueError("run artifacts Slurm terminal snapshot is invalid")
    _exact_int(entry.get("bytes"), "Slurm terminal snapshot bytes", minimum=1)
    _parse_iso_datetime(
        entry.get("captured_at_utc"),
        "Slurm terminal snapshot capture time",
        timezone_required=True,
    )
    return entry


def _slurm_task_entries(artifacts: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    values = artifacts.get("slurm_task_census")
    if not isinstance(values, list) or not values:
        raise ValueError("run artifacts Slurm task census is invalid")
    result: list[Mapping[str, Any]] = []
    task_ids: list[int] = []
    for entry in values:
        if not isinstance(entry, Mapping) or set(entry) != SLURM_TASK_FIELDS:
            raise ValueError("run artifacts Slurm task entry shape differs")
        task_id = _exact_int(entry.get("array_task_id"), "Slurm array task ID")
        for field in (
            "output_row_count",
            "technical_pass_row_count",
            "technical_fail_row_count",
            "log_bytes",
            "log_processed_batch_count",
            "log_technical_failure_count",
            "terminal_restarts",
        ):
            _exact_int(entry.get(field), f"Slurm task {task_id} {field}")
        _exact_bool(
            entry.get("strict_restart_reuse_eligible"),
            f"Slurm task {task_id} restart eligibility",
        )
        for field in (
            "array_job_id", "slurm_job_id", "metadata_host", "log_file",
            "terminal_state", "terminal_exit_code", "terminal_elapsed",
            "terminal_start", "terminal_end", "terminal_node",
        ):
            if not isinstance(entry.get(field), str) or not entry[field]:
                raise ValueError(
                    f"run artifacts Slurm task {task_id} {field} is invalid"
                )
        if not _is_sha256(entry.get("log_sha256")):
            raise ValueError(f"run artifacts Slurm task {task_id} log hash is invalid")
        started = _parse_iso_datetime(
            entry.get("metadata_started_at_utc"),
            f"Slurm task {task_id} metadata start",
            timezone_required=True,
        )
        completed = _parse_iso_datetime(
            entry.get("metadata_completed_at_utc"),
            f"Slurm task {task_id} metadata completion",
            timezone_required=True,
        )
        terminal_start = _parse_iso_datetime(
            entry.get("terminal_start"),
            f"Slurm task {task_id} terminal start",
            timezone_required=False,
        )
        terminal_end = _parse_iso_datetime(
            entry.get("terminal_end"),
            f"Slurm task {task_id} terminal end",
            timezone_required=False,
        )
        if completed < started or terminal_end < terminal_start:
            raise ValueError(f"run artifacts Slurm task {task_id} time ordering differs")
        if (
            entry["technical_pass_row_count"]
            + entry["technical_fail_row_count"]
            != entry["output_row_count"]
            or entry["log_processed_batch_count"] != entry["output_row_count"]
            or entry["log_technical_failure_count"]
            != entry["technical_fail_row_count"]
            or entry["metadata_host"] != entry["terminal_node"]
        ):
            raise ValueError(f"run artifacts Slurm task {task_id} counts differ")
        if entry["technical_fail_row_count"]:
            if entry["terminal_state"] != "FAILED" or entry[
                "terminal_exit_code"
            ] != "2:0":
                raise ValueError(
                    f"run artifacts Slurm task {task_id} failure state differs"
                )
        elif (
            entry["terminal_state"] != "COMPLETED"
            or entry["terminal_exit_code"] != "0:0"
        ):
            raise ValueError(
                f"run artifacts Slurm task {task_id} success state differs"
            )
        task_ids.append(task_id)
        result.append(entry)
    if task_ids != list(range(len(values))):
        raise ValueError("run artifacts Slurm task order differs")
    return result


def _load_local_log_evidence() -> tuple[
    list[dict[str, Any]], dict[int, dict[str, int]],
]:
    """Authenticate the five downloaded logs and parse their final counters."""
    if not LOCAL_LOG_DIR.is_dir():
        raise FileNotFoundError(f"round-three Stage-B log directory is absent: {LOCAL_LOG_DIR}")
    actual_files = {
        path.name for path in LOCAL_LOG_DIR.iterdir() if path.is_file()
    }
    if actual_files != set(EXPECTED_LOG_SHA256):
        raise ValueError("round-three Stage-B local-log inventory differs")
    if any(path.is_dir() for path in LOCAL_LOG_DIR.iterdir()):
        raise ValueError("round-three Stage-B local-log directory has nested content")

    files: list[dict[str, Any]] = []
    counters: dict[int, dict[str, int]] = {}
    pattern = re.compile(
        r"^processed=(\d+) technical_failures=(\d+) generation=([0-9a-f]{64})$",
        flags=re.MULTILINE,
    )
    for task_id in range(EXPECTED_CHUNK_COUNT):
        name = f"endorsement-line-ledger_{SLURM_ARRAY_JOB_ID}_{task_id}.log"
        path = LOCAL_LOG_DIR / name
        observed_hash = file_sha256(path)
        if observed_hash != EXPECTED_LOG_SHA256[name]:
            raise ValueError(f"round-three Stage-B local-log bytes differ: {name}")
        matches = pattern.findall(path.read_text(encoding="utf-8"))
        if len(matches) != 1:
            raise ValueError(f"round-three Stage-B local-log counter differs: {name}")
        processed_raw, failures_raw, generation_hash = matches[0]
        if generation_hash != EXPECTED_GENERATION_CONFIG_SHA256:
            raise ValueError(f"round-three Stage-B local-log generation differs: {name}")
        counters[task_id] = {
            "processed": int(processed_raw),
            "technical_failures": int(failures_raw),
        }
        files.append({
            "file": name,
            "sha256": observed_hash,
            "bytes": path.stat().st_size,
        })
    return files, counters


def _load_terminal_slurm_snapshot() -> tuple[
    dict[str, Any], dict[int, dict[str, Any]],
]:
    """Authenticate and parse the raw terminal sacct observation."""
    path = SLURM_TERMINAL_SNAPSHOT
    if not path.is_file():
        raise FileNotFoundError(f"round-three Stage-B Slurm snapshot is absent: {path}")
    observed_hash = file_sha256(path)
    if observed_hash != SLURM_TERMINAL_SNAPSHOT_SHA256:
        raise ValueError("round-three Stage-B Slurm snapshot bytes differ")
    lines = path.read_text(encoding="utf-8").splitlines()
    if len(lines) != 3 + EXPECTED_CHUNK_COUNT * 3:
        raise ValueError("round-three Stage-B Slurm snapshot row count differs")
    expected_source = "ssh hhilbig@hive.hpc.ucdavis.edu sacct"
    expected_command = (
        "sacct -j 22685041 --format=JobID,JobIDRaw,State,ExitCode,Elapsed,"
        "Start,End,NodeList,Restarts -n -P"
    )
    if not lines[0].startswith("captured_at_utc="):
        raise ValueError("round-three Stage-B Slurm capture time is absent")
    captured_at = lines[0].split("=", 1)[1]
    _parse_iso_datetime(captured_at, "Slurm capture time", timezone_required=True)
    if lines[1] != f"source={expected_source}" or lines[2] != f"command={expected_command}":
        raise ValueError("round-three Stage-B Slurm observation identity differs")

    parsed_rows: dict[str, dict[str, Any]] = {}
    for line in lines[3:]:
        fields = line.split("|")
        if len(fields) != 9:
            raise ValueError("round-three Stage-B Slurm row shape differs")
        (
            job_id,
            job_id_raw,
            state,
            exit_code,
            elapsed,
            start,
            end,
            node,
            restarts,
        ) = fields
        if not all((job_id, job_id_raw, state, exit_code, elapsed, start, end, node)):
            raise ValueError("round-three Stage-B Slurm row has an empty identity field")
        if job_id in parsed_rows:
            raise ValueError("round-three Stage-B Slurm job IDs are duplicated")
        parsed_rows[job_id] = {
            "job_id_raw": job_id_raw,
            "state": state,
            "exit_code": exit_code,
            "elapsed": elapsed,
            "start": start,
            "end": end,
            "node": node,
            "restarts": restarts,
        }

    tasks: dict[int, dict[str, Any]] = {}
    for task_id in range(EXPECTED_CHUNK_COUNT):
        base_id = f"{SLURM_ARRAY_JOB_ID}_{task_id}"
        expected_job_ids = {base_id, f"{base_id}.batch", f"{base_id}.extern"}
        if not expected_job_ids.issubset(parsed_rows):
            raise ValueError(f"round-three Stage-B Slurm task {task_id} rows are incomplete")
        task = parsed_rows[base_id]
        if task["job_id_raw"] != EXPECTED_SLURM_TASK_JOB_IDS[task_id]:
            raise ValueError(f"round-three Stage-B Slurm task {task_id} job mapping differs")
        if not task["restarts"].isdigit():
            raise ValueError(f"round-three Stage-B Slurm task {task_id} restarts differ")
        for suffix in ("batch", "extern"):
            child = parsed_rows[f"{base_id}.{suffix}"]
            if child["job_id_raw"] != f"{task['job_id_raw']}.{suffix}":
                raise ValueError(
                    f"round-three Stage-B Slurm task {task_id} child mapping differs"
                )
            for field in ("elapsed", "start", "end", "node"):
                if child[field] != task[field]:
                    raise ValueError(
                        f"round-three Stage-B Slurm task {task_id} child timing differs"
                    )
        tasks[task_id] = {
            **task,
            "restarts": int(task["restarts"]),
        }
    if set(parsed_rows) != {
        name
        for task_id in range(EXPECTED_CHUNK_COUNT)
        for name in (
            f"{SLURM_ARRAY_JOB_ID}_{task_id}",
            f"{SLURM_ARRAY_JOB_ID}_{task_id}.batch",
            f"{SLURM_ARRAY_JOB_ID}_{task_id}.extern",
        )
    }:
        raise ValueError("round-three Stage-B Slurm snapshot has unexpected jobs")
    snapshot = {
        "file": str(path.relative_to(REPO)),
        "sha256": observed_hash,
        "bytes": path.stat().st_size,
        "captured_at_utc": captured_at,
        "source": expected_source,
        "command": expected_command,
    }
    return snapshot, tasks


def _build_execution_provenance(
    shard_evidence: Sequence[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], dict[str, Any], list[dict[str, Any]]]:
    """Join metadata, outputs, logs, restart results, and terminal Slurm rows."""
    log_files, log_counters = _load_local_log_evidence()
    snapshot, terminal_tasks = _load_terminal_slurm_snapshot()
    if len(shard_evidence) != EXPECTED_CHUNK_COUNT:
        raise ValueError("round-three Stage-B shard evidence count differs")
    census: list[dict[str, Any]] = []
    for task_id, evidence in enumerate(shard_evidence):
        if evidence.get("array_task_id") != task_id:
            raise ValueError("round-three Stage-B shard evidence order differs")
        log_entry = log_files[task_id]
        log_counts = log_counters[task_id]
        task = terminal_tasks[task_id]
        row_count = _exact_int(evidence.get("output_row_count"), "shard output rows")
        pass_count = _exact_int(
            evidence.get("technical_pass_row_count"), "shard technical-pass rows"
        )
        fail_count = row_count - pass_count
        if (
            log_counts["processed"] != row_count
            or log_counts["technical_failures"] != fail_count
            or evidence.get("slurm_job_id") != task["job_id_raw"]
            or evidence.get("metadata_host") != task["node"]
        ):
            raise ValueError(f"round-three Stage-B task {task_id} provenance differs")
        if fail_count:
            expected_state, expected_exit = "FAILED", "2:0"
        else:
            expected_state, expected_exit = "COMPLETED", "0:0"
        if task["state"] != expected_state or task["exit_code"] != expected_exit:
            raise ValueError(f"round-three Stage-B task {task_id} terminal state differs")
        census.append({
            "array_job_id": SLURM_ARRAY_JOB_ID,
            "array_task_id": task_id,
            "slurm_job_id": str(evidence["slurm_job_id"]),
            "metadata_started_at_utc": str(evidence["metadata_started_at_utc"]),
            "metadata_completed_at_utc": str(evidence["metadata_completed_at_utc"]),
            "metadata_host": str(evidence["metadata_host"]),
            "output_row_count": row_count,
            "technical_pass_row_count": pass_count,
            "technical_fail_row_count": fail_count,
            "log_file": str(log_entry["file"]),
            "log_sha256": str(log_entry["sha256"]),
            "log_bytes": int(log_entry["bytes"]),
            "log_processed_batch_count": log_counts["processed"],
            "log_technical_failure_count": log_counts["technical_failures"],
            "strict_restart_reuse_eligible": bool(
                evidence["strict_restart_reuse_eligible"]
            ),
            "terminal_state": str(task["state"]),
            "terminal_exit_code": str(task["exit_code"]),
            "terminal_elapsed": str(task["elapsed"]),
            "terminal_start": str(task["start"]),
            "terminal_end": str(task["end"]),
            "terminal_node": str(task["node"]),
            "terminal_restarts": int(task["restarts"]),
        })
    wrapped = {
        "slurm_task_census": census,
        "slurm_terminal_snapshot": snapshot,
        "local_log_files": log_files,
    }
    _log_file_entries(wrapped)
    _slurm_snapshot_entry(wrapped)
    _slurm_task_entries(wrapped)
    return log_files, snapshot, census


def _technical_pass(
    validation: Mapping[str, Any] | None,
    alignment: Mapping[str, Any] | None,
) -> bool:
    if (
        not isinstance(validation, Mapping)
        or validation.get("schema_valid") is not True
        or validation.get("coverage_complete") is not True
        or not isinstance(alignment, Mapping)
    ):
        return False
    aligned = alignment.get("aligned_records")
    withheld = alignment.get("withheld_records")
    aligned_count = alignment.get("aligned_record_count")
    withheld_count = alignment.get("withheld_record_count")
    return bool(
        isinstance(aligned, list)
        and isinstance(withheld, list)
        and type(aligned_count) is int
        and type(withheld_count) is int
        and aligned_count == len(aligned)
        and withheld_count == len(withheld)
        and alignment.get("public_coverage_complete") is True
        and withheld_count == 0
        and not withheld
    )


def _duplicate_count(values: Sequence[Any]) -> int:
    return len(values) - len(set(values))


def _field_observability(
    records: Sequence[Mapping[str, Any]],
) -> dict[str, dict[str, int]]:
    census: dict[str, dict[str, int]] = {}
    for field in FIELD_OBSERVABILITY_FIELDS:
        present = sum(field in record for record in records)
        if present != len(records):
            raise ValueError(f"aligned record omits model field {field}")
        values = [record[field] for record in records]
        nonnull = sum(value is not None for value in values)
        nonempty_strings = sum(
            isinstance(value, str) and bool(value.strip()) for value in values
        )
        census[field] = {
            "present_count": present,
            "nonnull_count": nonnull,
            "null_count": len(values) - nonnull,
            "nonempty_string_count": nonempty_strings,
            "other_nonnull_count": nonnull - nonempty_strings,
        }
    return census


def _check(
    check_id: str, expected: Mapping[str, Any], observed: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "check_id": check_id,
        "expected": copy.deepcopy(dict(expected)),
        "observed": copy.deepcopy(dict(observed)),
        "passed": json_value_equal_typed(expected, observed),
    }


def _canonical_checks_from_report(
    report: Mapping[str, Any],
    technical: Mapping[str, int],
    slurm_tasks: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Reconstruct the check ledger solely from authenticated report fields."""
    output_rows = technical["output_rows"]
    technical_failures = technical["technical_fail_rows"]
    task_output_rows = sum(int(task["output_row_count"]) for task in slurm_tasks)
    task_failures = sum(
        int(task["technical_fail_row_count"]) for task in slurm_tasks
    )
    log_output_rows = sum(
        int(task["log_processed_batch_count"]) for task in slurm_tasks
    )
    log_failures = sum(
        int(task["log_technical_failure_count"]) for task in slurm_tasks
    )
    return [
        _check(
            CHECK_IDS[0],
            {
                "batch_count": technical["input_batches"],
                "anchor_count": report["authenticated_anchor_count"],
                "parent_with_anchors_count": report[
                    "parents_with_authenticated_anchors_count"
                ],
                "ordered_batch_ids_sha256": report[
                    "ordered_output_batch_ids_sha256"
                ],
            },
            {
                "batch_count": output_rows,
                "anchor_count": report["authenticated_anchor_count"],
                "parent_with_anchors_count": report[
                    "parents_with_authenticated_anchors_count"
                ],
                "ordered_batch_ids_sha256": report[
                    "ordered_output_batch_ids_sha256"
                ],
            },
        ),
        _check(
            CHECK_IDS[1],
            {
                "input_chunk_count": report["input_chunk_count"],
                "output_chunk_count": report["input_chunk_count"],
                "run_metadata_file_count": report["input_chunk_count"],
                "validated_output_batches": technical["input_batches"],
                "typed_row_rederivation_complete": True,
                "generation_config_sha256": EXPECTED_GENERATION_CONFIG_SHA256,
            },
            {
                "input_chunk_count": report["input_chunk_count"],
                "output_chunk_count": report["output_chunk_count"],
                "run_metadata_file_count": report["run_metadata_file_count"],
                "validated_output_batches": output_rows,
                "typed_row_rederivation_complete": report[
                    "typed_row_rederivation_complete"
                ],
                "generation_config_sha256": report["generation_config_sha256"],
            },
        ),
        _check(
            CHECK_IDS[2],
            {
                "local_log_file_count": report["input_chunk_count"],
                "terminal_scheduler_task_count": report["input_chunk_count"],
                "task_output_row_count": output_rows,
                "task_technical_fail_row_count": technical_failures,
                "log_processed_batch_count": output_rows,
                "log_technical_failure_count": technical_failures,
                "slurm_array_job_id": SLURM_ARRAY_JOB_ID,
            },
            {
                "local_log_file_count": report["local_log_file_count"],
                "terminal_scheduler_task_count": len(slurm_tasks),
                "task_output_row_count": task_output_rows,
                "task_technical_fail_row_count": task_failures,
                "log_processed_batch_count": log_output_rows,
                "log_technical_failure_count": log_failures,
                "slurm_array_job_id": report["slurm_array_job_id"],
            },
        ),
        _check(
            CHECK_IDS[3],
            {
                "strict_restart_reuse_eligible_chunk_count": report[
                    "input_chunk_count"
                ],
                "strict_restart_reuse_ineligible_chunk_count": 0,
            },
            {
                "strict_restart_reuse_eligible_chunk_count": report[
                    "strict_restart_reuse_eligible_chunk_count"
                ],
                "strict_restart_reuse_ineligible_chunk_count": report[
                    "strict_restart_reuse_ineligible_chunk_count"
                ],
            },
        ),
        _check(
            CHECK_IDS[4],
            {
                "parsed_rows": output_rows,
                "finish_stop_rows": output_rows,
                "technical_pass_rows": output_rows,
                "technical_fail_rows": 0,
                "parse_error_count": 0,
            },
            {
                "parsed_rows": technical["parsed_rows"],
                "finish_stop_rows": technical["finish_stop_rows"],
                "technical_pass_rows": technical["technical_pass_rows"],
                "technical_fail_rows": technical_failures,
                "parse_error_count": sum(
                    int(value)
                    for value in report["parse_error_reason_counts"].values()
                ),
            },
        ),
        _check(
            CHECK_IDS[5],
            {
                "schema_valid_rows": output_rows,
                "validation_coverage_complete_rows": output_rows,
                "model_anchor_count": report["authenticated_anchor_count"],
                "unresolved_anchor_count": 0,
                "validation_error_count": 0,
            },
            {
                "schema_valid_rows": technical["schema_valid_rows"],
                "validation_coverage_complete_rows": technical[
                    "validation_coverage_complete_rows"
                ],
                "model_anchor_count": technical["model_anchor_count"],
                "unresolved_anchor_count": technical["unresolved_anchor_count"],
                "validation_error_count": sum(
                    int(value)
                    for value in report["validation_error_reason_counts"].values()
                ),
            },
        ),
        _check(
            CHECK_IDS[6],
            {
                "alignment_present_rows": output_rows,
                "alignment_public_coverage_complete_rows": output_rows,
                "withheld_record_count": 0,
            },
            {
                "alignment_present_rows": technical["alignment_present_rows"],
                "alignment_public_coverage_complete_rows": technical[
                    "alignment_public_coverage_complete_rows"
                ],
                "withheld_record_count": technical["withheld_record_count"],
            },
        ),
        _check(
            CHECK_IDS[7],
            {
                "duplicate_aligned_record_key_count": 0,
                "duplicate_source_model_item_id_count": 0,
            },
            {
                "duplicate_aligned_record_key_count": technical[
                    "duplicate_aligned_record_key_count"
                ],
                "duplicate_source_model_item_id_count": technical[
                    "duplicate_source_model_item_id_count"
                ],
            },
        ),
    ]


def _validate_production_scope(
    manifest: Mapping[str, Any],
    batches: Sequence[Mapping[str, Any]],
    rows: Sequence[Mapping[str, Any]],
    artifacts: Mapping[str, Any],
) -> None:
    """Enforce the exact suffix-a run identity after the shared replay."""
    if manifest.get("mode") != "record_extraction_recall40_round3":
        raise ValueError("round-three Stage-B manifest mode differs")
    if manifest.get("stage") != RECORD_EXTRACTION_STAGE:
        raise ValueError("round-three Stage-B manifest stage differs")
    if manifest.get("line_ledger_contract_sha256") != (
        FROZEN_RECALL40_ROUND3_STAGE_B_CONTRACT_SHA256
    ):
        raise ValueError("round-three Stage-B contract identity differs")
    for field, expected in (
        ("expected_stage_batch_count", EXPECTED_BATCH_COUNT),
        ("exported_records", EXPECTED_BATCH_COUNT),
    ):
        if _exact_int(manifest.get(field), f"manifest {field}") != expected:
            raise ValueError(f"manifest {field} differs")
    expected_ids = _require_unique_strings(
        manifest.get("expected_batch_ids"), "manifest expected batch IDs"
    )
    if (
        len(expected_ids) != EXPECTED_BATCH_COUNT
        or manifest.get("expected_batch_ids_sha256") != sha256_json(expected_ids)
    ):
        raise ValueError("round-three Stage-B expected batch ledger differs")
    selected_parent_ids = _require_unique_strings(
        manifest.get("selected_parent_ids"), "manifest selected parent IDs"
    )
    if len(selected_parent_ids) != EXPECTED_SELECTED_PARENT_COUNT:
        raise ValueError("round-three Stage-B selected-parent count differs")
    closure = manifest.get("stage_b_closure")
    if not isinstance(closure, Mapping):
        raise ValueError("round-three Stage-B closure is absent")
    for field, expected in (
        ("stage_b_batch_count", EXPECTED_BATCH_COUNT),
        ("stage_b_anchor_count", EXPECTED_ANCHOR_COUNT),
        (
            "stage_b_parent_with_anchors_count",
            EXPECTED_PARENT_WITH_ANCHORS_COUNT,
        ),
    ):
        if _exact_int(closure.get(field), f"stage_b_closure.{field}") != expected:
            raise ValueError(f"round-three Stage-B closure {field} differs")
    chunk_entries = _manifest_chunk_entries(manifest)
    if len(chunk_entries) != EXPECTED_CHUNK_COUNT:
        raise ValueError("round-three Stage-B input chunk count differs")

    if len(batches) != EXPECTED_BATCH_COUNT or len(rows) != EXPECTED_BATCH_COUNT:
        raise ValueError("round-three Stage-B replay row count differs")
    input_ids = [batch.get("batch_id") for batch in batches]
    output_ids = [row.get("batch_id") for row in rows]
    if input_ids != expected_ids or output_ids != expected_ids:
        raise ValueError("round-three Stage-B replay batch order differs")
    if len(input_ids) != len(set(input_ids)) or len(output_ids) != len(set(output_ids)):
        raise ValueError("round-three Stage-B replay batch IDs are duplicated")
    anchor_count = 0
    parent_ids: list[str] = []
    for batch in batches:
        targets = batch.get("targets")
        parent_id = batch.get("parent_document_id")
        if not isinstance(targets, list) or not isinstance(parent_id, str):
            raise ValueError("round-three Stage-B replay batch shape differs")
        anchor_count += len(targets)
        parent_ids.append(parent_id)
    if anchor_count != EXPECTED_ANCHOR_COUNT:
        raise ValueError("round-three Stage-B replay anchor count differs")
    if len(set(parent_ids)) != EXPECTED_PARENT_WITH_ANCHORS_COUNT:
        raise ValueError("round-three Stage-B replay parent coverage differs")

    expected_artifact_fields = {
        "input_manifest_sha256",
        "generation_config_sha256",
        "max_model_len",
        "max_output_tokens",
        "input_files",
        "output_files",
        "output_batch_ids_sha256",
        "validated_output_batches",
        "run_metadata_files",
        "local_log_files",
        "slurm_terminal_snapshot",
        "slurm_task_census",
        "typed_row_rederivation_complete",
        "strict_restart_reuse_eligible_chunk_count",
        "strict_restart_reuse_ineligible_chunk_count",
    }
    if set(artifacts) != expected_artifact_fields:
        raise ValueError("round-three Stage-B run-artifact field set differs")
    if artifacts.get("input_manifest_sha256") != INPUT_MANIFEST_SHA256:
        raise ValueError("round-three Stage-B run manifest identity differs")
    if artifacts.get("generation_config_sha256") != (
        EXPECTED_GENERATION_CONFIG_SHA256
    ):
        raise ValueError("round-three Stage-B generation identity differs")
    if _exact_int(artifacts.get("max_model_len"), "max_model_len") != (
        EXPECTED_MAX_MODEL_LEN
    ):
        raise ValueError("round-three Stage-B model context differs")
    if _exact_int(artifacts.get("max_output_tokens"), "max_output_tokens") != (
        EXPECTED_MAX_OUTPUT_TOKENS
    ):
        raise ValueError("round-three Stage-B output budget differs")
    if _exact_int(
        artifacts.get("validated_output_batches"), "validated_output_batches"
    ) != EXPECTED_BATCH_COUNT:
        raise ValueError("round-three Stage-B validated output count differs")
    if artifacts.get("typed_row_rederivation_complete") is not True:
        raise ValueError("round-three Stage-B typed row replay is incomplete")
    eligible = _exact_int(
        artifacts.get("strict_restart_reuse_eligible_chunk_count"),
        "strict_restart_reuse_eligible_chunk_count",
    )
    ineligible = _exact_int(
        artifacts.get("strict_restart_reuse_ineligible_chunk_count"),
        "strict_restart_reuse_ineligible_chunk_count",
    )
    if eligible + ineligible != EXPECTED_CHUNK_COUNT:
        raise ValueError("round-three Stage-B restart census differs")
    if artifacts.get("output_batch_ids_sha256") != sha256_json(expected_ids):
        raise ValueError("round-three Stage-B output batch ledger differs")

    input_files = _artifact_file_entries(
        artifacts, "input_files", records=False
    )
    output_files = _artifact_file_entries(
        artifacts, "output_files", records=True
    )
    metadata_files = _metadata_file_entries(artifacts)
    log_files = _log_file_entries(artifacts)
    slurm_snapshot = _slurm_snapshot_entry(artifacts)
    slurm_tasks = _slurm_task_entries(artifacts)
    expected_inputs = [
        {"file": entry["file"], "sha256": entry["sha256"]}
        for entry in chunk_entries
    ]
    if not json_value_equal_typed(input_files, expected_inputs):
        raise ValueError("round-three Stage-B run input-file ledger differs")
    if [entry["file"] for entry in output_files] != [
        str(entry["file"]).replace(".jsonl.gz", ".out.jsonl.gz")
        for entry in chunk_entries
    ]:
        raise ValueError("round-three Stage-B run output-file ledger differs")
    if sum(int(entry["records"]) for entry in output_files) != (
        EXPECTED_BATCH_COUNT
    ):
        raise ValueError("round-three Stage-B output-file counts differ")
    if {
        str(entry["file"]): str(entry["sha256"])
        for entry in output_files
    } != EXPECTED_OUTPUT_SHA256:
        raise ValueError("round-three Stage-B output-file hashes differ")
    expected_metadata_names = [
        f"run_metadata_shard_{index:05d}.json"
        for index in range(EXPECTED_CHUNK_COUNT)
    ]
    if [entry["file"] for entry in metadata_files] != expected_metadata_names:
        raise ValueError("round-three Stage-B metadata-file ledger differs")
    if {
        str(entry["file"]): str(entry["sha256"])
        for entry in metadata_files
    } != EXPECTED_METADATA_SHA256:
        raise ValueError("round-three Stage-B metadata-file hashes differ")
    if {
        str(entry["file"]): str(entry["sha256"])
        for entry in log_files
    } != EXPECTED_LOG_SHA256:
        raise ValueError("round-three Stage-B local-log ledger differs")
    if (
        slurm_snapshot["file"] != str(SLURM_TERMINAL_SNAPSHOT.relative_to(REPO))
        or slurm_snapshot["sha256"] != SLURM_TERMINAL_SNAPSHOT_SHA256
    ):
        raise ValueError("round-three Stage-B terminal Slurm snapshot differs")
    if len(slurm_tasks) != EXPECTED_CHUNK_COUNT:
        raise ValueError("round-three Stage-B terminal Slurm task count differs")
    for task_id, task in enumerate(slurm_tasks):
        if (
            task["array_job_id"] != SLURM_ARRAY_JOB_ID
            or task["array_task_id"] != task_id
            or task["slurm_job_id"] != EXPECTED_SLURM_TASK_JOB_IDS[task_id]
        ):
            raise ValueError("round-three Stage-B terminal Slurm mapping differs")


def _build_canonical_report(
    manifest: Mapping[str, Any],
    batches: Sequence[Mapping[str, Any]],
    rows: Sequence[Mapping[str, Any]],
    artifacts: Mapping[str, Any],
) -> dict[str, Any]:
    """Rederive every row and summarize a complete authenticated run."""
    expected_count = _exact_int(
        manifest.get("expected_stage_batch_count"),
        "manifest expected_stage_batch_count",
        minimum=1,
    )
    expected_ids = _require_unique_strings(
        manifest.get("expected_batch_ids"), "manifest expected batch IDs"
    )
    if len(expected_ids) != expected_count:
        raise ValueError("manifest expected batch count and ledger differ")
    if manifest.get("expected_batch_ids_sha256") != sha256_json(expected_ids):
        raise ValueError("manifest expected batch ledger hash differs")
    if len(batches) != expected_count or len(rows) != expected_count:
        raise ValueError("Stage-B input/output row count differs")
    input_ids = [batch.get("batch_id") for batch in batches]
    output_ids = [row.get("batch_id") for row in rows]
    if input_ids != expected_ids or output_ids != expected_ids:
        raise ValueError("Stage-B input/output batch alignment differs")
    if len(input_ids) != len(set(input_ids)) or len(output_ids) != len(set(output_ids)):
        raise ValueError("Stage-B input/output batch IDs are duplicated")

    chunk_entries = _manifest_chunk_entries(manifest)
    input_files = _artifact_file_entries(
        artifacts, "input_files", records=False
    )
    output_files = _artifact_file_entries(
        artifacts, "output_files", records=True
    )
    metadata_files = _metadata_file_entries(artifacts)
    log_files = _log_file_entries(artifacts)
    slurm_snapshot = _slurm_snapshot_entry(artifacts)
    slurm_tasks = _slurm_task_entries(artifacts)
    if artifacts.get("input_manifest_sha256") != INPUT_MANIFEST_SHA256:
        raise ValueError("Stage-B artifact manifest identity differs")
    if artifacts.get("generation_config_sha256") != (
        EXPECTED_GENERATION_CONFIG_SHA256
    ):
        raise ValueError("Stage-B artifact generation identity differs")
    if _exact_int(
        artifacts.get("validated_output_batches"), "validated_output_batches"
    ) != expected_count:
        raise ValueError("Stage-B artifact validated count differs")
    _exact_bool(
        artifacts.get("typed_row_rederivation_complete"),
        "typed_row_rederivation_complete",
    )
    if artifacts["typed_row_rederivation_complete"] is not True:
        raise ValueError("Stage-B typed row replay is incomplete")
    eligible_restart_chunks = _exact_int(
        artifacts.get("strict_restart_reuse_eligible_chunk_count"),
        "strict_restart_reuse_eligible_chunk_count",
    )
    ineligible_restart_chunks = _exact_int(
        artifacts.get("strict_restart_reuse_ineligible_chunk_count"),
        "strict_restart_reuse_ineligible_chunk_count",
    )
    if eligible_restart_chunks + ineligible_restart_chunks != len(chunk_entries):
        raise ValueError("Stage-B strict restart census does not reconcile")
    if artifacts.get("output_batch_ids_sha256") != sha256_json(output_ids):
        raise ValueError("Stage-B artifact output ledger differs")
    if len(input_files) != len(chunk_entries) or len(output_files) != len(chunk_entries):
        raise ValueError("Stage-B artifact chunk inventory differs")
    if sum(int(entry["records"]) for entry in output_files) != len(rows):
        raise ValueError("Stage-B artifact output-file counts do not reconcile")
    if not metadata_files:
        raise ValueError("Stage-B run metadata inventory is empty")
    if (
        len(log_files) != len(chunk_entries)
        or len(slurm_tasks) != len(chunk_entries)
    ):
        raise ValueError("Stage-B execution-provenance inventory differs")

    selected_parent_ids = _require_unique_strings(
        manifest.get("selected_parent_ids"), "manifest selected parent IDs"
    )
    closure = manifest.get("stage_b_closure")
    if not isinstance(closure, Mapping):
        raise ValueError("manifest Stage-B closure is absent")
    authenticated_anchor_count = _exact_int(
        closure.get("stage_b_anchor_count"),
        "stage_b_closure.stage_b_anchor_count",
        minimum=1,
    )
    expected_parent_with_anchors = _exact_int(
        closure.get("stage_b_parent_with_anchors_count"),
        "stage_b_closure.stage_b_parent_with_anchors_count",
        minimum=1,
    )
    observed_anchor_count = 0
    observed_parent_ids: list[str] = []
    for batch in batches:
        targets = batch.get("targets")
        parent_id = batch.get("parent_document_id")
        if not isinstance(targets, list) or not isinstance(parent_id, str) or not parent_id:
            raise ValueError("Stage-B batch target or parent shape differs")
        observed_anchor_count += len(targets)
        observed_parent_ids.append(parent_id)

    parse_statuses: list[str] = []
    finish_reasons: list[str] = []
    parse_errors: list[str] = []
    validation_errors: list[str] = []
    withheld_reasons: list[str] = []
    aligned_records: list[dict[str, Any]] = []
    withheld_records: list[dict[str, Any]] = []
    aligned_counts_by_parent: Counter[str] = Counter()
    parsed_rows = 0
    stop_rows = 0
    technical_pass_rows = 0
    schema_valid_rows = 0
    coverage_complete_rows = 0
    alignment_present_rows = 0
    alignment_complete_rows = 0
    model_anchor_count = 0
    model_record_count = 0
    unresolved_anchor_count = 0

    for batch, row in zip(batches, rows, strict=True):
        if not isinstance(batch, Mapping) or not isinstance(row, Mapping):
            raise ValueError("Stage-B replay row is not an object")
        parent_id = batch.get("parent_document_id")
        if row.get("parent_document_id") != parent_id:
            raise ValueError("Stage-B output parent identity differs")
        parse_status = row.get("parse_status")
        finish_reason = row.get("finish_reason")
        if not isinstance(parse_status, str) or not isinstance(finish_reason, str):
            raise ValueError("Stage-B parse or finish status is invalid")
        parse_statuses.append(parse_status)
        finish_reasons.append(finish_reason)
        parsed_rows += int(parse_status == "parsed")
        stop_rows += int(finish_reason == "stop")
        parse_error = row.get("parse_error")
        if parse_error is not None:
            if not isinstance(parse_error, str) or not parse_error:
                raise ValueError("Stage-B parse error is invalid")
            parse_errors.append(parse_error)

        if parse_status in {
            "parsed", "invalid_stage_output", "invalid_stage_alignment",
        }:
            model_output = row.get("model_output")
            validation = validate_record_extraction_output(batch, model_output)
            alignment = (
                align_record_extraction_output(
                    batch,
                    model_output,
                    line_ledger_contract_sha256=(
                        FROZEN_RECALL40_ROUND3_STAGE_B_CONTRACT_SHA256
                    ),
                )
                if validation["schema_valid"]
                else None
            )
        else:
            validation = None
            alignment = None
            if row.get("model_output") is not None:
                raise ValueError("nonparsed Stage-B row carries model output")
        if not json_value_equal_typed(row.get("stage_validation"), validation):
            raise ValueError("Stage-B cached validation differs from deterministic replay")
        if not json_value_equal_typed(row.get("stage_alignment"), alignment):
            raise ValueError("Stage-B cached alignment differs from deterministic replay")
        expected_technical = _technical_pass(validation, alignment)
        actual_technical = _exact_bool(
            row.get("technical_pass"), "Stage-B technical_pass"
        )
        if actual_technical is not expected_technical:
            raise ValueError("Stage-B cached technical-pass flag differs")
        technical_pass_rows += int(expected_technical)

        if isinstance(validation, Mapping):
            schema_valid = _exact_bool(
                validation.get("schema_valid"), "stage_validation.schema_valid"
            )
            coverage_complete = _exact_bool(
                validation.get("coverage_complete"),
                "stage_validation.coverage_complete",
            )
            schema_valid_rows += int(schema_valid)
            coverage_complete_rows += int(coverage_complete)
            errors = validation.get("errors")
            if not isinstance(errors, list) or any(
                not isinstance(error, str) or not error for error in errors
            ):
                raise ValueError("Stage-B validation error ledger is invalid")
            validation_errors.extend(
                error.rsplit(":", 1)[-1] for error in errors
            )
            model_anchor_count += _exact_int(
                validation.get("anchor_decision_count"),
                "stage_validation.anchor_decision_count",
            )
            model_record_count += _exact_int(
                validation.get("model_record_count"),
                "stage_validation.model_record_count",
            )
            unresolved_anchor_count += _exact_int(
                validation.get("unresolved_anchor_count"),
                "stage_validation.unresolved_anchor_count",
            )
        if isinstance(alignment, Mapping):
            alignment_present_rows += 1
            public_complete = _exact_bool(
                alignment.get("public_coverage_complete"),
                "stage_alignment.public_coverage_complete",
            )
            alignment_complete_rows += int(public_complete)
            aligned = alignment.get("aligned_records")
            withheld = alignment.get("withheld_records")
            if not isinstance(aligned, list) or not isinstance(withheld, list):
                raise ValueError("Stage-B alignment record ledgers are invalid")
            if _exact_int(
                alignment.get("aligned_record_count"),
                "stage_alignment.aligned_record_count",
            ) != len(aligned):
                raise ValueError("Stage-B aligned-record count differs")
            if _exact_int(
                alignment.get("withheld_record_count"),
                "stage_alignment.withheld_record_count",
            ) != len(withheld):
                raise ValueError("Stage-B withheld-record count differs")
            for record in aligned:
                if not isinstance(record, Mapping):
                    raise ValueError("Stage-B aligned record is not an object")
                aligned_records.append(copy.deepcopy(dict(record)))
                aligned_counts_by_parent[str(parent_id)] += 1
            for record in withheld:
                if not isinstance(record, Mapping):
                    raise ValueError("Stage-B withheld record is not an object")
                reason = record.get("reason")
                if not isinstance(reason, str) or not reason:
                    raise ValueError("Stage-B withheld record lacks a reason")
                withheld_reasons.append(reason)
                withheld_records.append(copy.deepcopy(dict(record)))

    record_keys: list[tuple[str, str, int]] = []
    source_model_item_ids: list[str] = []
    for record in aligned_records:
        batch_id = record.get("stage_b_batch_id")
        slot = record.get("stage_b_slot")
        record_index = record.get("model_record_index")
        source_model_item_id = record.get("source_model_item_id")
        if (
            not isinstance(batch_id, str)
            or not batch_id
            or not isinstance(slot, str)
            or not slot
            or type(record_index) is not int
            or record_index < 0
            or not isinstance(source_model_item_id, str)
            or not source_model_item_id
        ):
            raise ValueError("Stage-B aligned record key is invalid")
        record_keys.append((batch_id, slot, record_index))
        source_model_item_ids.append(source_model_item_id)

    technical_counts = {
        "input_batches": len(batches),
        "output_rows": len(rows),
        "parsed_rows": parsed_rows,
        "nonparsed_rows": len(rows) - parsed_rows,
        "finish_stop_rows": stop_rows,
        "technical_pass_rows": technical_pass_rows,
        "technical_fail_rows": len(rows) - technical_pass_rows,
        "technical_failure_parent_count": len({
            str(row["parent_document_id"])
            for row in rows if row["technical_pass"] is False
        }),
        "schema_valid_rows": schema_valid_rows,
        "schema_not_valid_rows": len(rows) - schema_valid_rows,
        "validation_coverage_complete_rows": coverage_complete_rows,
        "alignment_present_rows": alignment_present_rows,
        "alignment_public_coverage_complete_rows": alignment_complete_rows,
        "alignment_incomplete_rows": len(rows) - alignment_complete_rows,
        "model_anchor_count": model_anchor_count,
        "model_record_count": model_record_count,
        "aligned_record_count": len(aligned_records),
        "withheld_record_count": len(withheld_records),
        "unresolved_anchor_count": unresolved_anchor_count,
        "duplicate_aligned_record_key_count": _duplicate_count(record_keys),
        "duplicate_source_model_item_id_count": _duplicate_count(
            source_model_item_ids
        ),
    }
    parse_status_counts = _counter(parse_statuses)
    finish_reason_counts = _counter(finish_reasons)
    parse_error_reason_counts = _counter(parse_errors)
    validation_error_reason_counts = _counter(validation_errors)
    withheld_reason_counts = _counter(withheld_reasons)
    task_output_rows = sum(int(task["output_row_count"]) for task in slurm_tasks)
    task_technical_failures = sum(
        int(task["technical_fail_row_count"]) for task in slurm_tasks
    )
    log_processed_rows = sum(
        int(task["log_processed_batch_count"]) for task in slurm_tasks
    )
    log_technical_failures = sum(
        int(task["log_technical_failure_count"]) for task in slurm_tasks
    )
    task_restart_eligible = sum(
        task["strict_restart_reuse_eligible"] is True for task in slurm_tasks
    )
    if (
        task_output_rows != len(rows)
        or task_technical_failures != technical_counts["technical_fail_rows"]
        or log_processed_rows != len(rows)
        or log_technical_failures != technical_counts["technical_fail_rows"]
        or task_restart_eligible != eligible_restart_chunks
    ):
        raise ValueError("Stage-B execution-provenance census does not reconcile")

    closure_expected = {
        "batch_count": expected_count,
        "anchor_count": authenticated_anchor_count,
        "parent_with_anchors_count": expected_parent_with_anchors,
        "ordered_batch_ids_sha256": sha256_json(expected_ids),
    }
    closure_observed = {
        "batch_count": len(rows),
        "anchor_count": observed_anchor_count,
        "parent_with_anchors_count": len(set(observed_parent_ids)),
        "ordered_batch_ids_sha256": sha256_json(output_ids),
    }
    inventory_expected = {
        "input_chunk_count": len(chunk_entries),
        "output_chunk_count": len(chunk_entries),
        "run_metadata_file_count": len(chunk_entries),
        "validated_output_batches": expected_count,
        "typed_row_rederivation_complete": True,
        "generation_config_sha256": EXPECTED_GENERATION_CONFIG_SHA256,
    }
    inventory_observed = {
        "input_chunk_count": len(input_files),
        "output_chunk_count": len(output_files),
        "run_metadata_file_count": len(metadata_files),
        "validated_output_batches": artifacts["validated_output_batches"],
        "typed_row_rederivation_complete": artifacts[
            "typed_row_rederivation_complete"
        ],
        "generation_config_sha256": artifacts["generation_config_sha256"],
    }
    execution_expected = {
        "local_log_file_count": len(chunk_entries),
        "terminal_scheduler_task_count": len(chunk_entries),
        "task_output_row_count": expected_count,
        "task_technical_fail_row_count": len(rows) - technical_pass_rows,
        "log_processed_batch_count": expected_count,
        "log_technical_failure_count": len(rows) - technical_pass_rows,
        "slurm_array_job_id": SLURM_ARRAY_JOB_ID,
    }
    execution_observed = {
        "local_log_file_count": len(log_files),
        "terminal_scheduler_task_count": len(slurm_tasks),
        "task_output_row_count": task_output_rows,
        "task_technical_fail_row_count": task_technical_failures,
        "log_processed_batch_count": log_processed_rows,
        "log_technical_failure_count": log_technical_failures,
        "slurm_array_job_id": (
            slurm_tasks[0]["array_job_id"] if slurm_tasks else None
        ),
    }
    checks = [
        _check(
            CHECK_IDS[0],
            closure_expected,
            closure_observed,
        ),
        _check(
            CHECK_IDS[1],
            inventory_expected,
            inventory_observed,
        ),
        _check(
            CHECK_IDS[2],
            execution_expected,
            execution_observed,
        ),
        _check(
            CHECK_IDS[3],
            {
                "strict_restart_reuse_eligible_chunk_count": len(
                    chunk_entries
                ),
                "strict_restart_reuse_ineligible_chunk_count": 0,
            },
            {
                "strict_restart_reuse_eligible_chunk_count": (
                    eligible_restart_chunks
                ),
                "strict_restart_reuse_ineligible_chunk_count": (
                    ineligible_restart_chunks
                ),
            },
        ),
        _check(
            CHECK_IDS[4],
            {
                "parsed_rows": expected_count,
                "finish_stop_rows": expected_count,
                "technical_pass_rows": expected_count,
                "technical_fail_rows": 0,
                "parse_error_count": 0,
            },
            {
                "parsed_rows": parsed_rows,
                "finish_stop_rows": stop_rows,
                "technical_pass_rows": technical_pass_rows,
                "technical_fail_rows": len(rows) - technical_pass_rows,
                "parse_error_count": len(parse_errors),
            },
        ),
        _check(
            CHECK_IDS[5],
            {
                "schema_valid_rows": expected_count,
                "validation_coverage_complete_rows": expected_count,
                "model_anchor_count": authenticated_anchor_count,
                "unresolved_anchor_count": 0,
                "validation_error_count": 0,
            },
            {
                "schema_valid_rows": schema_valid_rows,
                "validation_coverage_complete_rows": coverage_complete_rows,
                "model_anchor_count": model_anchor_count,
                "unresolved_anchor_count": unresolved_anchor_count,
                "validation_error_count": len(validation_errors),
            },
        ),
        _check(
            CHECK_IDS[6],
            {
                "alignment_present_rows": expected_count,
                "alignment_public_coverage_complete_rows": expected_count,
                "withheld_record_count": 0,
            },
            {
                "alignment_present_rows": alignment_present_rows,
                "alignment_public_coverage_complete_rows": (
                    alignment_complete_rows
                ),
                "withheld_record_count": len(withheld_records),
            },
        ),
        _check(
            CHECK_IDS[7],
            {
                "duplicate_aligned_record_key_count": 0,
                "duplicate_source_model_item_id_count": 0,
            },
            {
                "duplicate_aligned_record_key_count": _duplicate_count(
                    record_keys
                ),
                "duplicate_source_model_item_id_count": _duplicate_count(
                    source_model_item_ids
                ),
            },
        ),
    ]
    passed = sum(check["passed"] is True for check in checks)
    failed = len(checks) - passed
    go = failed == 0
    aligned_parent_counts = dict(sorted(aligned_counts_by_parent.items()))
    technical_failure_ledger = [
        {
            "batch_id": row["batch_id"],
            "parent_document_id": row["parent_document_id"],
            "parse_status": row["parse_status"],
            "validation_errors": list(
                (row.get("stage_validation") or {}).get("errors", [])
            ),
            "withheld_reasons": [
                record["reason"]
                for record in (
                    (row.get("stage_alignment") or {}).get(
                        "withheld_records", []
                    )
                )
            ],
        }
        for row in rows
        if row["technical_pass"] is False
    ]
    report: dict[str, Any] = {
        "version": REPORT_VERSION,
        "evaluation_scope": EVALUATION_SCOPE,
        "gate_decision": GO_DECISION if go else NO_GO_DECISION,
        "authorized_next_action": AUTHORIZED_NEXT_ACTION if go else None,
        "input_manifest_sha256": artifacts["input_manifest_sha256"],
        "input_manifest_payload_sha256": manifest.get(
            "manifest_payload_sha256"
        ),
        "line_ledger_contract_sha256": manifest.get(
            "line_ledger_contract_sha256"
        ),
        "generation_config_sha256": artifacts[
            "generation_config_sha256"
        ],
        "selected_parent_count": len(selected_parent_ids),
        "parents_with_authenticated_anchors_count": len(
            set(observed_parent_ids)
        ),
        "authenticated_anchor_count": observed_anchor_count,
        "input_chunk_count": len(input_files),
        "output_chunk_count": len(output_files),
        "run_metadata_file_count": len(metadata_files),
        "output_file_inventory": copy.deepcopy(output_files),
        "run_metadata_file_inventory": copy.deepcopy(metadata_files),
        "local_log_file_count": len(log_files),
        "local_log_file_inventory": copy.deepcopy(log_files),
        "slurm_array_job_id": SLURM_ARRAY_JOB_ID,
        "slurm_terminal_snapshot": copy.deepcopy(dict(slurm_snapshot)),
        "slurm_task_census": copy.deepcopy(slurm_tasks),
        "slurm_task_census_sha256": sha256_json(slurm_tasks),
        "technical_counts": technical_counts,
        "parse_status_counts": parse_status_counts,
        "finish_reason_counts": finish_reason_counts,
        "parse_error_reason_counts": parse_error_reason_counts,
        "validation_error_reason_counts": validation_error_reason_counts,
        "withheld_reason_counts": withheld_reason_counts,
        "field_observability_census": _field_observability(aligned_records),
        "aligned_record_counts_by_parent": aligned_parent_counts,
        "aligned_record_counts_by_parent_sha256": sha256_json(
            aligned_parent_counts
        ),
        "ordered_output_batch_ids_sha256": sha256_json(output_ids),
        "aligned_records_sha256": sha256_json(aligned_records),
        "withheld_records_sha256": sha256_json(withheld_records),
        "technical_failure_ledger_sha256": sha256_json(
            technical_failure_ledger
        ),
        "run_artifacts_sha256": sha256_json(dict(artifacts)),
        "typed_row_rederivation_complete": artifacts[
            "typed_row_rederivation_complete"
        ],
        "strict_restart_reuse_eligible_chunk_count": (
            eligible_restart_chunks
        ),
        "strict_restart_reuse_ineligible_chunk_count": (
            ineligible_restart_chunks
        ),
        "checks": checks,
        "passed_check_count": passed,
        "failed_check_count": failed,
        "source_only_human_truth_scoring_preparation_authorized": go,
        "source_only_human_truth_scoring_authorized": go,
        "prediction_bearing_human_task_authorized": False,
        "recall_or_content_quality_established": False,
        "additional_hive_inference_authorized": False,
        "full_expansion_authorized": False,
        "release_authorized": False,
    }
    report["report_payload_sha256"] = sha256_json(report)
    return report


def _validate_report_structure(report: Mapping[str, Any]) -> None:
    """Reject malformed, internally inconsistent, or broadened reports."""
    if set(report) != REPORT_FIELDS:
        raise ValueError("round-three Stage-B report field set differs")
    payload = copy.deepcopy(dict(report))
    recorded = payload.pop("report_payload_sha256", None)
    if not _is_sha256(recorded) or sha256_json(payload) != recorded:
        raise ValueError("round-three Stage-B report payload hash differs")
    if report.get("version") != REPORT_VERSION or report.get(
        "evaluation_scope"
    ) != EVALUATION_SCOPE:
        raise ValueError("round-three Stage-B report identity differs")
    if report.get("input_manifest_sha256") != INPUT_MANIFEST_SHA256:
        raise ValueError("round-three Stage-B report manifest differs")
    if report.get("line_ledger_contract_sha256") != (
        FROZEN_RECALL40_ROUND3_STAGE_B_CONTRACT_SHA256
    ):
        raise ValueError("round-three Stage-B report contract differs")
    if report.get("generation_config_sha256") != (
        EXPECTED_GENERATION_CONFIG_SHA256
    ):
        raise ValueError("round-three Stage-B report generation differs")
    for field in (
        "input_manifest_payload_sha256",
        "aligned_record_counts_by_parent_sha256",
        "ordered_output_batch_ids_sha256",
        "aligned_records_sha256",
        "withheld_records_sha256",
        "technical_failure_ledger_sha256",
        "run_artifacts_sha256",
        "slurm_task_census_sha256",
    ):
        if not _is_sha256(report.get(field)):
            raise ValueError(f"round-three Stage-B report {field} is invalid")

    for field in (
        "selected_parent_count",
        "parents_with_authenticated_anchors_count",
        "authenticated_anchor_count",
        "input_chunk_count",
        "output_chunk_count",
        "run_metadata_file_count",
        "local_log_file_count",
        "strict_restart_reuse_eligible_chunk_count",
        "strict_restart_reuse_ineligible_chunk_count",
        "passed_check_count",
        "failed_check_count",
    ):
        _exact_int(report.get(field), f"report {field}")
    technical = report.get("technical_counts")
    if not isinstance(technical, Mapping) or set(technical) != TECHNICAL_COUNT_FIELDS:
        raise ValueError("round-three Stage-B technical-count field set differs")
    for field in TECHNICAL_COUNT_FIELDS:
        _exact_int(technical[field], f"technical_counts.{field}")
    if technical["input_batches"] != technical["output_rows"]:
        raise ValueError("round-three Stage-B report batch counts do not reconcile")
    if technical["parsed_rows"] + technical["nonparsed_rows"] != technical[
        "output_rows"
    ]:
        raise ValueError("round-three Stage-B report parse counts do not reconcile")
    if technical["technical_pass_rows"] + technical[
        "technical_fail_rows"
    ] != technical["output_rows"]:
        raise ValueError("round-three Stage-B technical counts do not reconcile")
    if technical["schema_valid_rows"] + technical[
        "schema_not_valid_rows"
    ] != technical["output_rows"]:
        raise ValueError("round-three Stage-B schema counts do not reconcile")
    if technical["alignment_public_coverage_complete_rows"] + technical[
        "alignment_incomplete_rows"
    ] != technical["output_rows"]:
        raise ValueError("round-three Stage-B alignment counts do not reconcile")

    output_files = _artifact_file_entries(
        {"output_files": report.get("output_file_inventory")},
        "output_files",
        records=True,
    )
    metadata_files = _metadata_file_entries({
        "run_metadata_files": report.get("run_metadata_file_inventory")
    })
    log_files = _log_file_entries({
        "local_log_files": report.get("local_log_file_inventory")
    })
    slurm_snapshot = _slurm_snapshot_entry({
        "slurm_terminal_snapshot": report.get("slurm_terminal_snapshot")
    })
    slurm_tasks = _slurm_task_entries({
        "slurm_task_census": report.get("slurm_task_census")
    })
    if (
        len(output_files) != report["output_chunk_count"]
        or len(metadata_files) != report["run_metadata_file_count"]
        or len(log_files) != report["local_log_file_count"]
        or len(slurm_tasks) != report["input_chunk_count"]
        or sum(int(entry["records"]) for entry in output_files)
        != technical["output_rows"]
    ):
        raise ValueError("round-three Stage-B report artifact inventory differs")
    if report.get("slurm_array_job_id") != SLURM_ARRAY_JOB_ID:
        raise ValueError("round-three Stage-B report Slurm array identity differs")
    if sha256_json(slurm_tasks) != report.get("slurm_task_census_sha256"):
        raise ValueError("round-three Stage-B report Slurm task hash differs")
    if (
        sum(int(task["output_row_count"]) for task in slurm_tasks)
        != technical["output_rows"]
        or sum(int(task["technical_pass_row_count"]) for task in slurm_tasks)
        != technical["technical_pass_rows"]
        or sum(int(task["technical_fail_row_count"]) for task in slurm_tasks)
        != technical["technical_fail_rows"]
        or sum(task["strict_restart_reuse_eligible"] is True for task in slurm_tasks)
        != report["strict_restart_reuse_eligible_chunk_count"]
    ):
        raise ValueError("round-three Stage-B report Slurm task census differs")
    expected_task_logs = [
        {
            "file": task["log_file"],
            "sha256": task["log_sha256"],
            "bytes": task["log_bytes"],
        }
        for task in slurm_tasks
    ]
    if not json_value_equal_typed(log_files, expected_task_logs):
        raise ValueError("round-three Stage-B report task/log binding differs")
    if not isinstance(slurm_snapshot, Mapping):
        raise ValueError("round-three Stage-B report Slurm snapshot differs")

    for field in (
        "parse_status_counts",
        "finish_reason_counts",
        "parse_error_reason_counts",
        "validation_error_reason_counts",
        "withheld_reason_counts",
        "aligned_record_counts_by_parent",
    ):
        _string_count_map(report.get(field), f"report {field}")
    if sum(report["parse_status_counts"].values()) != technical["output_rows"]:
        raise ValueError("round-three Stage-B parse-status census differs")
    if sum(report["finish_reason_counts"].values()) != technical["output_rows"]:
        raise ValueError("round-three Stage-B finish-reason census differs")
    if report["parse_status_counts"].get("parsed", 0) != technical["parsed_rows"]:
        raise ValueError("round-three Stage-B parsed-row census differs")
    if report["finish_reason_counts"].get("stop", 0) != technical[
        "finish_stop_rows"
    ]:
        raise ValueError("round-three Stage-B stop-row census differs")
    if (
        technical["technical_pass_rows"] != technical["parsed_rows"]
        or technical["technical_fail_rows"] != technical["nonparsed_rows"]
        or technical["alignment_present_rows"] != technical["schema_valid_rows"]
        or technical["validation_coverage_complete_rows"]
        > technical["schema_valid_rows"]
        or technical["alignment_public_coverage_complete_rows"]
        > technical["alignment_present_rows"]
        or technical["model_record_count"]
        < technical["aligned_record_count"] + technical["withheld_record_count"]
    ):
        raise ValueError("round-three Stage-B technical cross-field census differs")
    failure_parent_count = technical["technical_failure_parent_count"]
    if (
        (technical["technical_fail_rows"] == 0) != (failure_parent_count == 0)
        or failure_parent_count > report["parents_with_authenticated_anchors_count"]
        or report["parents_with_authenticated_anchors_count"]
        > report["selected_parent_count"]
    ):
        raise ValueError("round-three Stage-B failure-parent census differs")
    if sum(report["aligned_record_counts_by_parent"].values()) != technical[
        "aligned_record_count"
    ]:
        raise ValueError("round-three Stage-B parent record counts differ")
    if sha256_json(dict(report["aligned_record_counts_by_parent"])) != report[
        "aligned_record_counts_by_parent_sha256"
    ]:
        raise ValueError("round-three Stage-B parent record ledger hash differs")
    if sum(report["withheld_reason_counts"].values()) != technical[
        "withheld_record_count"
    ]:
        raise ValueError("round-three Stage-B withheld-reason counts differ")

    observability = report.get("field_observability_census")
    if not isinstance(observability, Mapping) or set(observability) != set(
        FIELD_OBSERVABILITY_FIELDS
    ):
        raise ValueError("round-three Stage-B field-observability census differs")
    for field in FIELD_OBSERVABILITY_FIELDS:
        counts = observability[field]
        if not isinstance(counts, Mapping) or set(counts) != (
            FIELD_OBSERVABILITY_COUNT_FIELDS
        ):
            raise ValueError(
                f"round-three Stage-B field census shape differs: {field}"
            )
        for count_field in FIELD_OBSERVABILITY_COUNT_FIELDS:
            _exact_int(counts[count_field], f"{field}.{count_field}")
        if counts["present_count"] != technical["aligned_record_count"]:
            raise ValueError(f"round-three Stage-B field presence differs: {field}")
        if counts["nonnull_count"] + counts["null_count"] != counts[
            "present_count"
        ]:
            raise ValueError(f"round-three Stage-B null census differs: {field}")
        if counts["nonempty_string_count"] + counts[
            "other_nonnull_count"
        ] != counts["nonnull_count"]:
            raise ValueError(f"round-three Stage-B value census differs: {field}")

    checks = report.get("checks")
    if not isinstance(checks, list) or len(checks) != len(CHECK_IDS):
        raise ValueError("round-three Stage-B report checks differ")
    for expected_id, check in zip(CHECK_IDS, checks, strict=True):
        if (
            not isinstance(check, Mapping)
            or set(check) != {"check_id", "expected", "observed", "passed"}
            or check.get("check_id") != expected_id
            or not isinstance(check.get("expected"), Mapping)
            or not isinstance(check.get("observed"), Mapping)
            or type(check.get("passed")) is not bool
            or check["passed"] is not json_value_equal_typed(
                check["expected"], check["observed"]
            )
        ):
            raise ValueError(f"round-three Stage-B report check differs: {expected_id}")
    canonical_checks = _canonical_checks_from_report(
        report, technical, slurm_tasks
    )
    if not json_value_equal_typed(checks, canonical_checks):
        raise ValueError("round-three Stage-B report check ledger is not canonical")
    passed = sum(check["passed"] is True for check in checks)
    if (
        report["passed_check_count"] != passed
        or report["failed_check_count"] != len(checks) - passed
    ):
        raise ValueError("round-three Stage-B report check counts differ")
    authority_ready = bool(
        report["failed_check_count"] == 0
        and technical["technical_fail_rows"] == 0
        and technical["technical_failure_parent_count"] == 0
        and technical["schema_not_valid_rows"] == 0
        and technical["alignment_incomplete_rows"] == 0
        and technical["withheld_record_count"] == 0
        and technical["unresolved_anchor_count"] == 0
        and not report["parse_error_reason_counts"]
        and not report["validation_error_reason_counts"]
        and not report["withheld_reason_counts"]
        and report["strict_restart_reuse_ineligible_chunk_count"] == 0
        and all(
            task["terminal_state"] == "COMPLETED"
            and task["terminal_exit_code"] == "0:0"
            for task in slurm_tasks
        )
    )
    go = report["failed_check_count"] == 0
    if go is not authority_ready:
        raise ValueError("round-three Stage-B report authority gate is contradictory")
    if report.get("gate_decision") != (GO_DECISION if go else NO_GO_DECISION):
        raise ValueError("round-three Stage-B report gate decision differs")
    if report.get("authorized_next_action") != (
        AUTHORIZED_NEXT_ACTION if go else None
    ):
        raise ValueError("round-three Stage-B authorized next action differs")
    if report.get(
        "source_only_human_truth_scoring_preparation_authorized"
    ) is not go:
        raise ValueError("round-three Stage-B source-only authorization differs")
    if report.get("source_only_human_truth_scoring_authorized") is not go:
        raise ValueError("round-three Stage-B source-only scoring boundary differs")
    for field in (
        "prediction_bearing_human_task_authorized",
        "recall_or_content_quality_established",
        "additional_hive_inference_authorized",
        "full_expansion_authorized",
        "release_authorized",
    ):
        if report.get(field) is not False:
            raise ValueError(f"round-three Stage-B boundary differs: {field}")
    if report.get("typed_row_rederivation_complete") is not True:
        raise ValueError("round-three Stage-B report row-replay boundary differs")
    if report[
        "strict_restart_reuse_eligible_chunk_count"
    ] + report["strict_restart_reuse_ineligible_chunk_count"] != report[
        "input_chunk_count"
    ]:
        raise ValueError("round-three Stage-B report restart census differs")


def validate_report_against_run(
    report: Mapping[str, Any],
    manifest: Mapping[str, Any],
    batches: Sequence[Mapping[str, Any]],
    rows: Sequence[Mapping[str, Any]],
    artifacts: Mapping[str, Any],
) -> dict[str, Any]:
    """Require exact typed equality with a fresh deterministic reconstruction."""
    _validate_report_structure(report)
    expected = _build_canonical_report(manifest, batches, rows, artifacts)
    if not json_value_equal_typed(report, expected):
        raise ValueError("round-three Stage-B report differs from fresh replay")
    return dict(report)


def _load_authenticated_run_allowing_technical_failures(
    output_dir: Path,
) -> tuple[
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, Any],
]:
    """Authenticate exact persisted rows and metadata without hiding failures.

    The shared successful-run loader correctly rejects any chunk containing a
    technical failure.  A post-run evaluator still has to preserve and count
    those exact failures.  This function uses the same row derivation,
    generation payload, metadata constructor, prompt-token digests, and strict
    restart predicate, while treating restart ineligibility as a reported
    outcome rather than as an unreadable run.
    """
    contract = get_frozen_recall40_round3_stage_b_contract()
    manifest, manifest_sha256, input_paths = validate_input_manifest(
        INPUT_EXPORT / "manifest.json",
        INPUT_EXPORT / "chunks",
        stage=RECORD_EXTRACTION_STAGE,
        contract=contract,
    )
    if manifest_sha256 != INPUT_MANIFEST_SHA256:
        raise ValueError("frozen round-three Stage-B manifest identity differs")
    if len(input_paths) != EXPECTED_CHUNK_COUNT:
        raise ValueError("round-three Stage-B authenticated chunk count differs")
    if not output_dir.is_dir():
        raise FileNotFoundError(f"round-three Stage-B output directory is absent: {output_dir}")
    expected_output_names = [
        path.name.replace(".jsonl.gz", ".out.jsonl.gz")
        for path in input_paths
    ]
    expected_metadata_names = [
        f"run_metadata_shard_{index:05d}.json"
        for index in range(len(input_paths))
    ]
    expected_names = set(expected_output_names + expected_metadata_names)
    actual_files = {
        str(path.relative_to(output_dir))
        for path in output_dir.rglob("*")
        if path.is_file()
    }
    if actual_files != expected_names:
        raise ValueError("round-three Stage-B output inventory differs")
    if any(path.is_dir() for path in output_dir.rglob("*")):
        raise ValueError("round-three Stage-B output contains nested directories")

    base_args = {
        "stage": RECORD_EXTRACTION_STAGE,
        "seed": FROZEN_GENERATION_SEED,
        "max_model_len": EXPECTED_MAX_MODEL_LEN,
        "max_output_tokens": EXPECTED_MAX_OUTPUT_TOKENS,
        "expected_input_manifest_sha256": INPUT_MANIFEST_SHA256,
        "shard_count": EXPECTED_CHUNK_COUNT,
    }
    generation_args = SimpleNamespace(**base_args, shard_index=0)
    generation_payload = generation_config_payload(
        generation_args,
        contract,
        input_manifest_sha256=INPUT_MANIFEST_SHA256,
    )
    if sha256_json(generation_payload) != EXPECTED_GENERATION_CONFIG_SHA256:
        raise ValueError("round-three Stage-B expected generation payload differs")

    all_batches: list[dict[str, Any]] = []
    all_rows: list[dict[str, Any]] = []
    output_files: list[dict[str, Any]] = []
    metadata_files: list[dict[str, Any]] = []
    shard_evidence: list[dict[str, Any]] = []
    strict_restart_eligible = 0
    for chunk_index, input_path in enumerate(input_paths):
        output_path = output_dir / expected_output_names[chunk_index]
        batches = read_gzip_jsonl(input_path)
        rows = read_gzip_jsonl(output_path)
        if len(batches) != len(rows):
            raise ValueError(
                f"round-three Stage-B chunk row count differs: {output_path.name}"
            )
        if [batch.get("batch_id") for batch in batches] != [
            row.get("batch_id") for row in rows
        ]:
            raise ValueError(
                f"round-three Stage-B chunk batch order differs: {output_path.name}"
            )
        prompt_digest = RECALL40_ROUND3_STAGE_B_SCOPE_SPEC[
            "chunk_ordered_prompt_token_counts_sha256"
        ][chunk_index]
        prompt_counts: list[int] = []
        row_args = SimpleNamespace(**base_args, shard_index=chunk_index)
        for batch, row in zip(batches, rows, strict=True):
            if not isinstance(batch, Mapping) or not isinstance(row, Mapping):
                raise ValueError("round-three Stage-B chunk row is not an object")
            raw_output = row.get("raw_output")
            finish_reason = row.get("finish_reason")
            output_tokens = _exact_int(
                row.get("output_tokens"), "output row output_tokens", minimum=1
            )
            prompt_tokens = _exact_int(
                row.get("prompt_tokens_used"),
                "output row prompt_tokens_used",
                minimum=1,
            )
            if not isinstance(raw_output, str) or not isinstance(finish_reason, str):
                raise ValueError("round-three Stage-B raw output identity is invalid")
            expected_row = _derive_output_row(
                batch=batch,
                raw_output=raw_output,
                finish_reason=finish_reason,
                stop_reason=row.get("stop_reason"),
                output_tokens=output_tokens,
                prompt_tokens=prompt_tokens,
                args=row_args,
                contract=contract,
                input_manifest_sha256=INPUT_MANIFEST_SHA256,
                generation_hash=EXPECTED_GENERATION_CONFIG_SHA256,
            )
            if not json_value_equal_typed(row, expected_row):
                raise ValueError(
                    "round-three Stage-B output row does not rederive: "
                    f"{row.get('batch_id')}"
                )
            prompt_counts.append(prompt_tokens)
        if sha256_json(prompt_counts) != prompt_digest:
            raise ValueError(
                f"round-three Stage-B prompt-token ledger differs: {output_path.name}"
            )

        metadata_path = output_dir / expected_metadata_names[chunk_index]
        metadata = load_json_no_duplicates(metadata_path)
        if not isinstance(metadata, dict):
            raise ValueError(
                f"round-three Stage-B metadata is not an object: {metadata_path.name}"
            )
        if metadata.get("recovery_attestation") is not False:
            raise ValueError(
                f"round-three Stage-B metadata is not the full attempt: {metadata_path.name}"
            )
        started = metadata.get("started_at")
        completed = metadata.get("completed_at")
        host = metadata.get("host")
        if not all(isinstance(value, str) and value for value in (started, completed, host)):
            raise ValueError(
                f"round-three Stage-B metadata dynamic identity is invalid: {metadata_path.name}"
            )
        try:
            started_at = datetime.fromisoformat(str(started))
            completed_at = datetime.fromisoformat(str(completed))
        except ValueError as error:
            raise ValueError(
                f"round-three Stage-B metadata timestamp is invalid: {metadata_path.name}"
            ) from error
        if (
            started_at.tzinfo is None
            or completed_at.tzinfo is None
            or completed_at < started_at
        ):
            raise ValueError(
                f"round-three Stage-B metadata time ordering differs: {metadata_path.name}"
            )
        if (
            metadata.get("shard_index") != chunk_index
            or metadata.get("shard_count") != EXPECTED_CHUNK_COUNT
            or metadata.get("slurm_array_task_id") != str(chunk_index)
            or not isinstance(metadata.get("slurm_job_id"), str)
            or not str(metadata["slurm_job_id"]).isdigit()
        ):
            raise ValueError(
                f"round-three Stage-B metadata shard identity differs: {metadata_path.name}"
            )
        expected_metadata = _run_metadata(
            args=row_args,
            contract=contract,
            generation_hash=EXPECTED_GENERATION_CONFIG_SHA256,
            input_manifest_sha256=INPUT_MANIFEST_SHA256,
            selected=len(batches),
            selected_variant_inventory=schema_variant_inventory(batches),
            selected_stage_a_provenance_sha256=[],
            rows=rows,
            started=str(started),
            recovery=False,
        )
        for dynamic_field in (
            "completed_at", "host", "slurm_job_id", "slurm_array_task_id",
        ):
            expected_metadata[dynamic_field] = metadata[dynamic_field]
        if not json_value_equal_typed(metadata, expected_metadata):
            raise ValueError(
                f"round-three Stage-B metadata does not rederive: {metadata_path.name}"
            )

        restart_eligible = line_ledger_chunk_is_complete(
            input_path,
            output_path,
            stage=RECORD_EXTRACTION_STAGE,
            input_manifest_sha256=INPUT_MANIFEST_SHA256,
            expected_generation_config_sha256=(
                EXPECTED_GENERATION_CONFIG_SHA256
            ),
            expected_max_model_len=EXPECTED_MAX_MODEL_LEN,
            expected_max_output_tokens=EXPECTED_MAX_OUTPUT_TOKENS,
            expected_prompt_token_counts_sha256=prompt_digest,
            contract_override=contract,
        )
        strict_restart_eligible += int(restart_eligible)
        output_hash = file_sha256(output_path)
        if output_hash != EXPECTED_OUTPUT_SHA256[output_path.name]:
            raise ValueError(
                f"round-three Stage-B output bytes differ: {output_path.name}"
            )
        metadata_hash = file_sha256(metadata_path)
        if metadata_hash != EXPECTED_METADATA_SHA256[metadata_path.name]:
            raise ValueError(
                f"round-three Stage-B metadata bytes differ: {metadata_path.name}"
            )
        output_files.append({
            "file": output_path.name,
            "sha256": output_hash,
            "bytes": output_path.stat().st_size,
            "records": len(rows),
        })
        metadata_files.append({
            "file": metadata_path.name,
            "sha256": metadata_hash,
            "bytes": metadata_path.stat().st_size,
        })
        shard_evidence.append({
            "array_task_id": chunk_index,
            "slurm_job_id": metadata["slurm_job_id"],
            "metadata_started_at_utc": metadata["started_at"],
            "metadata_completed_at_utc": metadata["completed_at"],
            "metadata_host": metadata["host"],
            "output_row_count": len(rows),
            "technical_pass_row_count": sum(
                row["technical_pass"] is True for row in rows
            ),
            "strict_restart_reuse_eligible": restart_eligible,
        })
        all_batches.extend(batches)
        all_rows.extend(rows)

    local_log_files, slurm_terminal_snapshot, slurm_task_census = (
        _build_execution_provenance(shard_evidence)
    )

    artifacts = {
        "input_manifest_sha256": INPUT_MANIFEST_SHA256,
        "generation_config_sha256": EXPECTED_GENERATION_CONFIG_SHA256,
        "max_model_len": EXPECTED_MAX_MODEL_LEN,
        "max_output_tokens": EXPECTED_MAX_OUTPUT_TOKENS,
        "input_files": [
            {"file": path.name, "sha256": file_sha256(path)}
            for path in input_paths
        ],
        "output_files": output_files,
        "output_batch_ids_sha256": sha256_json([
            str(row["batch_id"]) for row in all_rows
        ]),
        "validated_output_batches": len(all_rows),
        "run_metadata_files": metadata_files,
        "local_log_files": local_log_files,
        "slurm_terminal_snapshot": slurm_terminal_snapshot,
        "slurm_task_census": slurm_task_census,
        "typed_row_rederivation_complete": True,
        "strict_restart_reuse_eligible_chunk_count": (
            strict_restart_eligible
        ),
        "strict_restart_reuse_ineligible_chunk_count": (
            len(input_paths) - strict_restart_eligible
        ),
    }
    return manifest, all_batches, all_rows, artifacts


def evaluate(output_dir: Path) -> dict[str, Any]:
    """Authenticate exact suffix-a inputs and outputs and return one report."""
    manifest_path = INPUT_EXPORT / "manifest.json"
    if file_sha256(manifest_path) != INPUT_MANIFEST_SHA256:
        raise ValueError("frozen round-three Stage-B manifest bytes differ")
    contract = get_frozen_recall40_round3_stage_b_contract()
    if contract["line_ledger_contract_sha256"] != (
        FROZEN_RECALL40_ROUND3_STAGE_B_CONTRACT_SHA256
    ):
        raise ValueError("frozen round-three Stage-B contract differs")
    manifest, batches, rows, artifacts = (
        _load_authenticated_run_allowing_technical_failures(output_dir)
    )
    _validate_production_scope(manifest, batches, rows, artifacts)
    report = _build_canonical_report(manifest, batches, rows, artifacts)
    return validate_report_against_run(
        report, manifest, batches, rows, artifacts
    )


def _report_is_authority_bearing(report: Mapping[str, Any]) -> bool:
    return bool(
        report.get("gate_decision") == GO_DECISION
        or report.get("authorized_next_action") is not None
        or report.get("source_only_human_truth_scoring_authorized") is True
        or report.get(
            "source_only_human_truth_scoring_preparation_authorized"
        ) is True
    )


def _validate_report_for_write(
    report: Mapping[str, Any],
    *,
    output_dir: Path | None = None,
    operation: str = "round-three Stage-B report validation",
) -> None:
    """Validate structure and bind every GO to a fresh immutable run replay."""
    _validate_report_structure(report)
    if output_dir is None:
        if _report_is_authority_bearing(report):
            raise ValueError(
                f"{operation} requires output_dir and fresh immutable run evidence for GO"
            )
        return
    if not isinstance(output_dir, Path):
        raise ValueError(f"{operation} output_dir must be a pathlib.Path")
    expected_report = evaluate(output_dir)
    if not json_value_equal_typed(report, expected_report):
        raise ValueError(f"{operation} report differs from fresh immutable run evidence")


def load_report_strict(
    path: Path,
    *,
    output_dir: Path | None = None,
) -> dict[str, Any]:
    """Load strict JSON; a GO requires replay of the immutable output directory."""
    value = load_json_no_duplicates(path)
    if not isinstance(value, dict):
        raise ValueError("round-three Stage-B report is not an object")
    _validate_report_for_write(
        value,
        output_dir=output_dir,
        operation="round-three Stage-B report load",
    )
    return value


def write_report(
    report: Mapping[str, Any],
    report_path: Path,
    *,
    output_dir: Path | None = None,
) -> None:
    """Create one canonical report without overwriting an earlier result."""
    _validate_report_for_write(
        report,
        output_dir=output_dir,
        operation="round-three Stage-B report write",
    )
    report_path.parent.mkdir(parents=True, exist_ok=True)
    serialized = json.dumps(
        report,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ) + "\n"
    try:
        with report_path.open("x", encoding="utf-8", newline="") as handle:
            handle.write(serialized)
    except FileExistsError as error:
        raise FileExistsError(
            f"refusing to overwrite round-three Stage-B report: {report_path}"
        ) from error
    reloaded = load_report_strict(report_path, output_dir=output_dir)
    if not json_value_equal_typed(reloaded, report):
        raise ValueError("written round-three Stage-B report differs")
    if report_path.stat().st_size <= 1 or not sha256_file(report_path):
        raise ValueError("written round-three Stage-B report is empty")


def cli_exit_code(
    report: Mapping[str, Any],
    *,
    output_dir: Path | None = None,
) -> int:
    _validate_report_for_write(
        report,
        output_dir=output_dir,
        operation="round-three Stage-B CLI exit",
    )
    return 0 if report["gate_decision"] == GO_DECISION else 2


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(argv)
    report = evaluate(args.output_dir)
    write_report(report, args.report, output_dir=args.output_dir)
    print(json.dumps({
        "gate_decision": report["gate_decision"],
        "passed_check_count": report["passed_check_count"],
        "failed_check_count": report["failed_check_count"],
        "aligned_record_count": report["technical_counts"][
            "aligned_record_count"
        ],
        "report": str(args.report),
        "report_sha256": sha256_file(args.report),
    }, indent=2, sort_keys=True))
    return 0 if report["gate_decision"] == GO_DECISION else 2


if __name__ == "__main__":
    raise SystemExit(main())
