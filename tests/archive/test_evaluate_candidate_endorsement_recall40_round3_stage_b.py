from __future__ import annotations

import copy
import json
import shutil
from pathlib import Path
from typing import Any

import pytest

import scripts.evaluate_candidate_endorsement_recall40_round3_stage_b as evaluator
from scripts.hive.extract_candidate_endorsement_line_ledger_vllm import (
    read_gzip_jsonl,
)
from src.candidate_endorsement_line_ledger import (
    FROZEN_RECALL40_ROUND3_STAGE_B_CONTRACT_SHA256,
    align_record_extraction_output,
    schema_binding_for_batch,
    sha256_json,
    validate_record_extraction_output,
)


REAL_OUTPUT = Path(
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round3_stage_b_outputs_20260906a"
)
REAL_REPORT = Path(
    "quality_reports/"
    "candidate_endorsement_recall40_round3_stage_b_22685041_2026-09-06.json"
)


@pytest.fixture(scope="module")
def single_batch() -> dict[str, Any]:
    for path in sorted((evaluator.INPUT_EXPORT / "chunks").glob("*.jsonl.gz")):
        for source_batch in read_gzip_jsonl(path):
            for source_target in source_batch["targets"]:
                if source_target["line_text"] != "Michael Rohr, CPA":
                    continue
                batch = copy.deepcopy(source_batch)
                target = copy.deepcopy(source_target)
                target["slot"] = "slot_00"
                batch["targets"] = [target]
                batch["target_slots"] = ["slot_00"]
                batch["padding_slots"] = [
                    f"slot_{index:02d}" for index in range(1, 8)
                ]
                binding = schema_binding_for_batch(batch)
                for field in (
                    "json_schema_variant_id",
                    "json_schema_sha256",
                    "json_schema_catalog_sha256",
                    "target_slot_count",
                ):
                    batch[field] = binding[field]
                return batch
    raise AssertionError("known synthetic target is absent")


def _model_output() -> dict[str, Any]:
    record = {
        "endorser_name": "Michael Rohr",
        "entity_type": "individual",
        "endorser_category": "private_individual",
        "title": "CPA",
        "affiliation": None,
        "jurisdiction": None,
    }
    return {
        f"slot_{index:02d}": {
            "records": [copy.deepcopy(record)] if index == 0 else []
        }
        for index in range(8)
    }


def _row(
    batch: dict[str, Any], model_output: dict[str, Any],
) -> dict[str, Any]:
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
    technical = evaluator._technical_pass(validation, alignment)
    parse_status = (
        "parsed"
        if technical
        else (
            "invalid_stage_output"
            if not validation["schema_valid"]
            else "invalid_stage_alignment"
        )
    )
    return {
        "batch_id": batch["batch_id"],
        "parent_document_id": batch["parent_document_id"],
        "parse_status": parse_status,
        "parse_error": None,
        "finish_reason": "stop",
        "model_output": copy.deepcopy(model_output),
        "stage_validation": validation,
        "stage_alignment": alignment,
        "technical_pass": technical,
    }


def _synthetic_evidence(
    batch: dict[str, Any], row: dict[str, Any],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    batch_id = str(batch["batch_id"])
    parent_id = str(batch["parent_document_id"])
    technical_pass = row["technical_pass"] is True
    terminal_state = "COMPLETED" if technical_pass else "FAILED"
    terminal_exit_code = "0:0" if technical_pass else "2:0"
    manifest = {
        "expected_stage_batch_count": 1,
        "expected_batch_ids": [batch_id],
        "expected_batch_ids_sha256": sha256_json([batch_id]),
        "selected_parent_ids": [parent_id],
        "stage_b_closure": {
            "stage_b_anchor_count": 1,
            "stage_b_parent_with_anchors_count": 1,
        },
        "manifest_payload_sha256": "1" * 64,
        "line_ledger_contract_sha256": (
            FROZEN_RECALL40_ROUND3_STAGE_B_CONTRACT_SHA256
        ),
        "files": {
            "chunks": [{
                "file": "chunk_00000.jsonl.gz",
                "records": 1,
                "sha256": "2" * 64,
            }],
        },
    }
    artifacts = {
        "input_manifest_sha256": evaluator.INPUT_MANIFEST_SHA256,
        "generation_config_sha256": (
            evaluator.EXPECTED_GENERATION_CONFIG_SHA256
        ),
        "max_model_len": evaluator.EXPECTED_MAX_MODEL_LEN,
        "max_output_tokens": evaluator.EXPECTED_MAX_OUTPUT_TOKENS,
        "input_files": [{
            "file": "chunk_00000.jsonl.gz",
            "sha256": "2" * 64,
        }],
        "output_files": [{
            "file": "chunk_00000.out.jsonl.gz",
            "sha256": "3" * 64,
            "bytes": 100,
            "records": 1,
        }],
        "output_batch_ids_sha256": sha256_json([batch_id]),
        "validated_output_batches": 1,
        "run_metadata_files": [{
            "file": "run_metadata_shard_00000.json",
            "sha256": "4" * 64,
            "bytes": 200,
        }],
        "local_log_files": [{
            "file": "endorsement-line-ledger_22685041_0.log",
            "sha256": "5" * 64,
            "bytes": 300,
        }],
        "slurm_terminal_snapshot": {
            "file": "synthetic-sacct.txt",
            "sha256": "6" * 64,
            "bytes": 400,
            "captured_at_utc": "2026-09-06T23:52:19+00:00",
            "source": "synthetic sacct",
            "command": "synthetic command",
        },
        "slurm_task_census": [{
            "array_job_id": evaluator.SLURM_ARRAY_JOB_ID,
            "array_task_id": 0,
            "slurm_job_id": "22685042",
            "metadata_started_at_utc": "2026-09-06T21:50:23+00:00",
            "metadata_completed_at_utc": "2026-09-06T21:54:02+00:00",
            "metadata_host": "synthetic-node",
            "output_row_count": 1,
            "technical_pass_row_count": int(technical_pass),
            "technical_fail_row_count": int(not technical_pass),
            "log_file": "endorsement-line-ledger_22685041_0.log",
            "log_sha256": "5" * 64,
            "log_bytes": 300,
            "log_processed_batch_count": 1,
            "log_technical_failure_count": int(not technical_pass),
            "strict_restart_reuse_eligible": technical_pass,
            "terminal_state": terminal_state,
            "terminal_exit_code": terminal_exit_code,
            "terminal_elapsed": "00:03:39",
            "terminal_start": "2026-09-06T14:50:23",
            "terminal_end": "2026-09-06T14:54:02",
            "terminal_node": "synthetic-node",
            "terminal_restarts": 0,
        }],
        "typed_row_rederivation_complete": True,
        "strict_restart_reuse_eligible_chunk_count": int(
            technical_pass
        ),
        "strict_restart_reuse_ineligible_chunk_count": int(
            not technical_pass
        ),
    }
    return manifest, [batch], [row], artifacts


def _report(
    batch: dict[str, Any], row: dict[str, Any],
) -> dict[str, Any]:
    return evaluator._build_canonical_report(
        *_synthetic_evidence(batch, row)
    )


def _rehash_report(report: dict[str, Any]) -> None:
    report.pop("report_payload_sha256", None)
    report["report_payload_sha256"] = sha256_json(report)


def test_synthetic_complete_run_is_canonical_and_narrowly_authorized(
    single_batch: dict[str, Any],
) -> None:
    report = _report(single_batch, _row(single_batch, _model_output()))
    assert report["gate_decision"] == evaluator.GO_DECISION
    assert report["passed_check_count"] == len(evaluator.CHECK_IDS)
    assert report["failed_check_count"] == 0
    assert report["technical_counts"]["aligned_record_count"] == 1
    assert report["technical_counts"]["withheld_record_count"] == 0
    assert report["field_observability_census"]["title"] == {
        "present_count": 1,
        "nonnull_count": 1,
        "null_count": 0,
        "nonempty_string_count": 1,
        "other_nonnull_count": 0,
    }
    assert report["field_observability_census"]["affiliation"][
        "null_count"
    ] == 1
    assert report[
        "source_only_human_truth_scoring_preparation_authorized"
    ] is True
    assert report["prediction_bearing_human_task_authorized"] is False
    assert report["recall_or_content_quality_established"] is False
    assert report["additional_hive_inference_authorized"] is False
    assert report["full_expansion_authorized"] is False
    assert report["release_authorized"] is False
    evaluator._validate_report_structure(report)


def test_synthetic_incomplete_and_duplicate_ledgers_are_rejected(
    single_batch: dict[str, Any],
) -> None:
    row = _row(single_batch, _model_output())
    manifest, batches, rows, artifacts = _synthetic_evidence(
        single_batch, row
    )
    with pytest.raises(ValueError, match="row count differs"):
        evaluator._build_canonical_report(
            manifest, batches, [], artifacts
        )

    duplicated_manifest = copy.deepcopy(manifest)
    duplicated_manifest["expected_stage_batch_count"] = 2
    duplicated_manifest["expected_batch_ids"] = [
        single_batch["batch_id"], single_batch["batch_id"]
    ]
    duplicated_manifest["expected_batch_ids_sha256"] = sha256_json(
        duplicated_manifest["expected_batch_ids"]
    )
    with pytest.raises(ValueError, match="unique nonempty strings"):
        evaluator._build_canonical_report(
            duplicated_manifest,
            [single_batch, single_batch],
            [row, row],
            artifacts,
        )


def test_synthetic_model_output_and_cached_alignment_tamper_are_rejected(
    single_batch: dict[str, Any],
) -> None:
    row = _row(single_batch, _model_output())
    attacked_output = copy.deepcopy(row)
    attacked_output["model_output"]["slot_00"]["records"][0][
        "endorser_name"
    ] = "Not In Source"
    with pytest.raises(ValueError, match="cached validation differs"):
        _report(single_batch, attacked_output)

    attacked_alignment = copy.deepcopy(row)
    attacked_alignment["stage_alignment"]["aligned_record_count"] = 0
    with pytest.raises(ValueError, match="cached alignment differs"):
        _report(single_batch, attacked_alignment)


def test_synthetic_schema_failure_is_counted_and_blocks_gate(
    single_batch: dict[str, Any],
) -> None:
    output = _model_output()
    output["slot_00"]["records"] = []
    report = _report(single_batch, _row(single_batch, output))
    assert report["gate_decision"] == evaluator.NO_GO_DECISION
    assert report["technical_counts"]["schema_not_valid_rows"] == 1
    assert report["technical_counts"]["technical_fail_rows"] == 1
    assert report["validation_error_reason_counts"] == {
        "single_anchor_record_count_mismatch": 1,
    }
    assert report[
        "source_only_human_truth_scoring_preparation_authorized"
    ] is False


def test_synthetic_alignment_failure_is_counted_and_blocks_gate(
    single_batch: dict[str, Any],
) -> None:
    attacked_batch = copy.deepcopy(single_batch)
    attacked_batch["segment_descriptors"] = []
    report = _report(
        attacked_batch, _row(attacked_batch, _model_output())
    )
    assert report["gate_decision"] == evaluator.NO_GO_DECISION
    assert report["technical_counts"]["schema_valid_rows"] == 1
    assert report["technical_counts"]["alignment_incomplete_rows"] == 1
    assert report["technical_counts"]["withheld_record_count"] == 1
    assert report["withheld_reason_counts"] == {
        "record_evidence_has_no_single_source_segment_carrier": 1,
    }


@pytest.mark.parametrize("attack", ["manifest_float", "artifact_bool", "row_float"])
def test_synthetic_typed_count_attacks_are_rejected(
    single_batch: dict[str, Any], attack: str,
) -> None:
    row = _row(single_batch, _model_output())
    manifest, batches, rows, artifacts = _synthetic_evidence(
        single_batch, row
    )
    if attack == "manifest_float":
        manifest["expected_stage_batch_count"] = 1.0
    elif attack == "artifact_bool":
        artifacts["validated_output_batches"] = True
    else:
        rows[0]["stage_validation"]["model_record_count"] = 1.0
    with pytest.raises(ValueError):
        evaluator._build_canonical_report(
            manifest, batches, rows, artifacts
        )


def test_report_forgery_and_duplicate_json_keys_are_rejected(
    single_batch: dict[str, Any], tmp_path: Path,
) -> None:
    report = _report(single_batch, _row(single_batch, _model_output()))
    broadened = copy.deepcopy(report)
    broadened["full_expansion_authorized"] = True
    _rehash_report(broadened)
    with pytest.raises(ValueError, match="boundary differs"):
        evaluator._validate_report_for_write(broadened)

    typed = copy.deepcopy(report)
    typed["technical_counts"]["aligned_record_count"] = 1.0
    _rehash_report(typed)
    with pytest.raises(ValueError, match="exact integer"):
        evaluator._validate_report_for_write(typed)

    slurm_forgery = copy.deepcopy(report)
    slurm_forgery["slurm_task_census"][0]["terminal_restarts"] = 0.0
    slurm_forgery["slurm_task_census_sha256"] = sha256_json(
        slurm_forgery["slurm_task_census"]
    )
    _rehash_report(slurm_forgery)
    with pytest.raises(ValueError, match="exact integer"):
        evaluator._validate_report_for_write(slurm_forgery)

    duplicate = tmp_path / "duplicate.json"
    duplicate.write_text(
        '{"version":"first","version":"second"}\n',
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="duplicate JSON key"):
        evaluator.load_report_strict(duplicate)


def test_cli_writes_canonical_report_and_preserves_existing_file(
    single_batch: dict[str, Any], tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    report = _report(single_batch, _row(single_batch, _model_output()))
    monkeypatch.setattr(evaluator, "evaluate", lambda _output_dir: report)
    destination = tmp_path / "report.json"
    assert evaluator.main([
        "--output-dir", str(tmp_path / "synthetic-output"),
        "--report", str(destination),
    ]) == 0
    assert evaluator.load_report_strict(
        destination, output_dir=tmp_path / "synthetic-output"
    ) == report
    raw = destination.read_text(encoding="utf-8")
    assert raw.endswith("\n")
    assert ": " not in raw
    with pytest.raises(FileExistsError, match="refusing to overwrite"):
        evaluator.main([
            "--output-dir", str(tmp_path / "synthetic-output"),
            "--report", str(destination),
        ])


def test_authority_bearing_consumers_require_authenticated_expectation(
    single_batch: dict[str, Any], tmp_path: Path,
) -> None:
    report = _report(single_batch, _row(single_batch, _model_output()))
    source = tmp_path / "go-report.json"
    source.write_text(
        json.dumps(report, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="requires output_dir"):
        evaluator.load_report_strict(source)
    with pytest.raises(ValueError, match="requires output_dir"):
        evaluator.write_report(report, tmp_path / "unauthenticated-write.json")
    with pytest.raises(ValueError, match="requires output_dir"):
        evaluator.cli_exit_code(report)

    with pytest.raises(ValueError, match="output_dir must be a pathlib.Path"):
        evaluator.load_report_strict(source, output_dir=[])  # type: ignore[arg-type]
    with pytest.raises(FileNotFoundError, match="output directory is absent"):
        evaluator.load_report_strict(source, output_dir=tmp_path / "stub-output")

    stub = tmp_path / "stub.json"
    stub.write_text("{}\n", encoding="utf-8")
    with pytest.raises(ValueError, match="field set differs"):
        evaluator.load_report_strict(stub)

    extra_report = copy.deepcopy(report)
    extra_report["unexpected"] = False
    _rehash_report(extra_report)
    extra = tmp_path / "extra.json"
    extra.write_text(
        json.dumps(extra_report, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="field set differs"):
        evaluator.load_report_strict(extra)

    nonobject = tmp_path / "nonobject.json"
    nonobject.write_text("[]\n", encoding="utf-8")
    with pytest.raises(ValueError, match="report is not an object"):
        evaluator.load_report_strict(nonobject)


def test_exact_rehashed_authorization_forgery_is_rejected_everywhere(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    if not REAL_REPORT.is_file():
        pytest.skip("authentic round-three Stage-B report is unavailable")
    authentic = evaluator.load_report_strict(REAL_REPORT)
    assert authentic["gate_decision"] == evaluator.NO_GO_DECISION
    forged = copy.deepcopy(authentic)
    technical = forged["technical_counts"]
    technical.update({
        "parsed_rows": technical["output_rows"],
        "nonparsed_rows": 0,
        "technical_pass_rows": technical["output_rows"],
        "technical_fail_rows": 0,
        "technical_failure_parent_count": 0,
        "schema_valid_rows": technical["output_rows"],
        "schema_not_valid_rows": 0,
        "validation_coverage_complete_rows": technical["output_rows"],
        "alignment_present_rows": technical["output_rows"],
        "alignment_public_coverage_complete_rows": technical["output_rows"],
        "alignment_incomplete_rows": 0,
        "withheld_record_count": 0,
        "unresolved_anchor_count": 0,
    })
    forged["parse_status_counts"] = {"parsed": technical["output_rows"]}
    forged["parse_error_reason_counts"] = {}
    forged["validation_error_reason_counts"] = {}
    forged["withheld_reason_counts"] = {}
    forged["withheld_records_sha256"] = sha256_json([])
    forged["technical_failure_ledger_sha256"] = sha256_json([])
    for task in forged["slurm_task_census"]:
        task["technical_pass_row_count"] = task["output_row_count"]
        task["technical_fail_row_count"] = 0
        task["log_technical_failure_count"] = 0
        task["strict_restart_reuse_eligible"] = True
        task["terminal_state"] = "COMPLETED"
        task["terminal_exit_code"] = "0:0"
    forged["slurm_task_census_sha256"] = sha256_json(
        forged["slurm_task_census"]
    )
    forged["strict_restart_reuse_eligible_chunk_count"] = 5
    forged["strict_restart_reuse_ineligible_chunk_count"] = 0
    forged["checks"] = evaluator._canonical_checks_from_report(
        forged, technical, forged["slurm_task_census"]
    )
    forged["passed_check_count"] = len(evaluator.CHECK_IDS)
    forged["failed_check_count"] = 0
    forged["gate_decision"] = evaluator.GO_DECISION
    forged["authorized_next_action"] = evaluator.AUTHORIZED_NEXT_ACTION
    forged["source_only_human_truth_scoring_preparation_authorized"] = True
    forged["source_only_human_truth_scoring_authorized"] = True
    _rehash_report(forged)

    evaluator._validate_report_structure(forged)
    with pytest.raises(ValueError, match="requires output_dir"):
        evaluator._validate_report_for_write(forged)
    calls: list[Path] = []

    def fresh_evaluate(output_dir: Path) -> dict[str, Any]:
        calls.append(output_dir)
        return copy.deepcopy(authentic)

    monkeypatch.setattr(evaluator, "evaluate", fresh_evaluate)
    with pytest.raises(ValueError, match="fresh immutable run evidence"):
        evaluator._validate_report_for_write(
            forged, output_dir=REAL_OUTPUT
        )
    forged_path = tmp_path / "forged.json"
    forged_path.write_text(
        json.dumps(forged, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="fresh immutable run evidence"):
        evaluator.load_report_strict(
            forged_path, output_dir=REAL_OUTPUT
        )
    destination = tmp_path / "forged-write.json"
    with pytest.raises(ValueError, match="fresh immutable run evidence"):
        evaluator.write_report(
            forged, destination, output_dir=REAL_OUTPUT
        )
    assert not destination.exists()
    with pytest.raises(ValueError, match="fresh immutable run evidence"):
        evaluator.cli_exit_code(forged, output_dir=REAL_OUTPUT)
    assert calls == [REAL_OUTPUT, REAL_OUTPUT, REAL_OUTPUT, REAL_OUTPUT]


def _production_scope_evidence() -> tuple[
    dict[str, Any], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]
]:
    manifest = json.loads(
        (evaluator.INPUT_EXPORT / "manifest.json").read_text(encoding="utf-8")
    )
    chunk_entries = manifest["files"]["chunks"]
    batches = [
        batch
        for path in sorted((evaluator.INPUT_EXPORT / "chunks").glob("*.jsonl.gz"))
        for batch in read_gzip_jsonl(path)
    ]
    rows = [{"batch_id": batch["batch_id"]} for batch in batches]
    artifacts = {
        "input_manifest_sha256": evaluator.INPUT_MANIFEST_SHA256,
        "generation_config_sha256": (
            evaluator.EXPECTED_GENERATION_CONFIG_SHA256
        ),
        "max_model_len": evaluator.EXPECTED_MAX_MODEL_LEN,
        "max_output_tokens": evaluator.EXPECTED_MAX_OUTPUT_TOKENS,
        "input_files": [
            {"file": entry["file"], "sha256": entry["sha256"]}
            for entry in chunk_entries
        ],
        "output_files": [
            {
                "file": entry["file"].replace(
                    ".jsonl.gz", ".out.jsonl.gz"
                ),
                "sha256": evaluator.EXPECTED_OUTPUT_SHA256[
                    entry["file"].replace(".jsonl.gz", ".out.jsonl.gz")
                ],
                "bytes": 100 + index,
                "records": entry["records"],
            }
            for index, entry in enumerate(chunk_entries)
        ],
        "output_batch_ids_sha256": manifest["expected_batch_ids_sha256"],
        "validated_output_batches": 298,
        "run_metadata_files": [
            {
                "file": f"run_metadata_shard_{index:05d}.json",
                "sha256": evaluator.EXPECTED_METADATA_SHA256[
                    f"run_metadata_shard_{index:05d}.json"
                ],
                "bytes": 200 + index,
            }
            for index in range(5)
        ],
        "local_log_files": [
            {
                "file": name,
                "sha256": sha256,
                "bytes": 300 + index,
            }
            for index, (name, sha256) in enumerate(
                evaluator.EXPECTED_LOG_SHA256.items()
            )
        ],
        "slurm_terminal_snapshot": {
            "file": str(evaluator.SLURM_TERMINAL_SNAPSHOT.relative_to(evaluator.REPO)),
            "sha256": evaluator.SLURM_TERMINAL_SNAPSHOT_SHA256,
            "bytes": 1_786,
            "captured_at_utc": "2026-09-06T23:52:19+00:00",
            "source": "ssh hhilbig@hive.hpc.ucdavis.edu sacct",
            "command": (
                "sacct -j 22685041 --format=JobID,JobIDRaw,State,ExitCode,"
                "Elapsed,Start,End,NodeList,Restarts -n -P"
            ),
        },
        "slurm_task_census": [
            {
                "array_job_id": evaluator.SLURM_ARRAY_JOB_ID,
                "array_task_id": index,
                "slurm_job_id": evaluator.EXPECTED_SLURM_TASK_JOB_IDS[index],
                "metadata_started_at_utc": "2026-09-06T21:50:23+00:00",
                "metadata_completed_at_utc": "2026-09-06T21:54:02+00:00",
                "metadata_host": "hive-dc-7-5-58",
                "output_row_count": [60, 60, 60, 60, 58][index],
                "technical_pass_row_count": [50, 52, 33, 52, 58][index],
                "technical_fail_row_count": [10, 8, 27, 8, 0][index],
                "log_file": list(evaluator.EXPECTED_LOG_SHA256)[index],
                "log_sha256": list(evaluator.EXPECTED_LOG_SHA256.values())[index],
                "log_bytes": 300 + index,
                "log_processed_batch_count": [60, 60, 60, 60, 58][index],
                "log_technical_failure_count": [10, 8, 27, 8, 0][index],
                "strict_restart_reuse_eligible": index == 4,
                "terminal_state": "COMPLETED" if index == 4 else "FAILED",
                "terminal_exit_code": "0:0" if index == 4 else "2:0",
                "terminal_elapsed": "00:04:15",
                "terminal_start": "2026-09-06T14:49:55",
                "terminal_end": "2026-09-06T14:54:10",
                "terminal_node": "hive-dc-7-5-58",
                "terminal_restarts": 0,
            }
            for index in range(5)
        ],
        "typed_row_rederivation_complete": True,
        "strict_restart_reuse_eligible_chunk_count": 1,
        "strict_restart_reuse_ineligible_chunk_count": 4,
    }
    return manifest, batches, rows, artifacts


def test_production_scope_rejects_inventory_duplicate_and_typed_tamper() -> None:
    evidence = _production_scope_evidence()
    evaluator._validate_production_scope(*evidence)

    incomplete = copy.deepcopy(evidence)
    incomplete[2].pop()
    with pytest.raises(ValueError, match="row count differs"):
        evaluator._validate_production_scope(*incomplete)

    duplicated = copy.deepcopy(evidence)
    duplicated[2][-1]["batch_id"] = duplicated[2][0]["batch_id"]
    with pytest.raises(ValueError, match="batch order differs"):
        evaluator._validate_production_scope(*duplicated)

    tampered = copy.deepcopy(evidence)
    tampered[3]["output_files"][0]["sha256"] = "not-a-hash"
    with pytest.raises(ValueError, match="entry identity is invalid"):
        evaluator._validate_production_scope(*tampered)

    typed = copy.deepcopy(evidence)
    typed[3]["validated_output_batches"] = 298.0
    with pytest.raises(ValueError, match="exact integer"):
        evaluator._validate_production_scope(*typed)


def test_exact_loader_rejects_inventory_and_typed_metadata_tamper(
    tmp_path: Path,
) -> None:
    if not REAL_OUTPUT.is_dir():
        pytest.skip("round-three Stage-B suffix-a outputs are unavailable")
    inventory_attack = tmp_path / "inventory-attack"
    shutil.copytree(REAL_OUTPUT, inventory_attack)
    (inventory_attack / "unexpected.txt").write_text("x", encoding="utf-8")
    with pytest.raises(ValueError, match="output inventory differs"):
        evaluator._load_authenticated_run_allowing_technical_failures(
            inventory_attack
        )

    metadata_attack = tmp_path / "metadata-attack"
    shutil.copytree(REAL_OUTPUT, metadata_attack)
    metadata_path = metadata_attack / "run_metadata_shard_00000.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    metadata["processed_outputs_this_run"] = 60.0
    metadata_path.write_text(
        json.dumps(metadata, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="metadata does not rederive"):
        evaluator._load_authenticated_run_allowing_technical_failures(
            metadata_attack
        )


def test_frozen_execution_provenance_rejects_log_and_snapshot_tamper(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    log_attack = tmp_path / "logs"
    shutil.copytree(evaluator.LOCAL_LOG_DIR, log_attack)
    attacked_log = log_attack / "endorsement-line-ledger_22685041_0.log"
    attacked_log.write_bytes(attacked_log.read_bytes() + b"tamper\n")
    monkeypatch.setattr(evaluator, "LOCAL_LOG_DIR", log_attack)
    with pytest.raises(ValueError, match="local-log bytes differ"):
        evaluator._load_local_log_evidence()

    snapshot_attack = tmp_path / "sacct.txt"
    shutil.copyfile(evaluator.SLURM_TERMINAL_SNAPSHOT, snapshot_attack)
    snapshot_attack.write_bytes(snapshot_attack.read_bytes() + b"tamper\n")
    monkeypatch.setattr(evaluator, "SLURM_TERMINAL_SNAPSHOT", snapshot_attack)
    with pytest.raises(ValueError, match="Slurm snapshot bytes differ"):
        evaluator._load_terminal_slurm_snapshot()


def test_real_suffix_a_report_replays_when_outputs_are_available() -> None:
    if not REAL_OUTPUT.is_dir():
        pytest.skip("round-three Stage-B suffix-a outputs are unavailable")
    report = evaluator.evaluate(REAL_OUTPUT)
    assert report["gate_decision"] == evaluator.NO_GO_DECISION
    assert report["technical_counts"]["output_rows"] == 298
    assert report["technical_counts"]["technical_pass_rows"] == 245
    assert report["technical_counts"]["technical_fail_rows"] == 53
    assert report["technical_counts"]["aligned_record_count"] == 1_832
    assert report["technical_counts"]["withheld_record_count"] == 5
    assert report["strict_restart_reuse_eligible_chunk_count"] == 1
    assert report["strict_restart_reuse_ineligible_chunk_count"] == 4
    assert len(report["output_file_inventory"]) == 5
    assert len(report["run_metadata_file_inventory"]) == 5
    assert len(report["local_log_file_inventory"]) == 5
    assert report["slurm_terminal_snapshot"]["sha256"] == (
        evaluator.SLURM_TERMINAL_SNAPSHOT_SHA256
    )
    assert [task["terminal_state"] for task in report["slurm_task_census"]] == [
        "FAILED", "FAILED", "FAILED", "FAILED", "COMPLETED",
    ]
    assert [task["terminal_restarts"] for task in report["slurm_task_census"]] == [
        0, 0, 0, 0, 0,
    ]
    assert report["validation_error_reason_counts"] == {
        "affiliation_not_exact_in_block": 43,
        "jurisdiction_not_exact_in_block": 5,
        "model_record_exact_repetition_threshold_reached": 1,
        "name_not_exact_in_block": 66,
        "split_line_name_not_fully_accounted": 1,
        "title_not_exact_in_block": 27,
    }
    assert report["withheld_reason_counts"] == {
        "carrier_exact_alignment_failed": 5,
    }
