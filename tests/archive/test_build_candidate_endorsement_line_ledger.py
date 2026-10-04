"""Focused integration tests for the two-stage line-ledger export builder."""
from __future__ import annotations

import json
import shutil
from argparse import Namespace
from pathlib import Path

import pytest

from scripts.build_candidate_endorsement_line_ledger import (
    CORRECTED_ISSUE_AUDIT_GATE,
    DEFAULT_FULL_EXPORT_DIR,
    FROZEN_PILOT_BATCH_COUNTS,
    FROZEN_PILOT_INVENTORY,
    PILOT_PARENT_IDS,
    build_line_decision_export,
    build_recall40_round2_record_extraction_export,
    build_record_extraction_export,
    _load_stage_a_input_export,
    read_jsonl_gz,
    sha256_file,
    validate_manifest_payload_hash,
    write_jsonl_gz,
)
from scripts.hive.extract_candidate_endorsement_line_ledger_vllm import (
    STRUCTURED_DECODING_BACKEND,
    STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
    PROMPT_TOKEN_COUNT_SPEC_SHA256,
    _derive_output_row,
    _run_metadata,
    generation_config_payload,
    generation_config_sha256,
    schema_variant_inventory,
    validate_input_manifest,
)
from scripts.evaluate_candidate_endorsement_line_ledger_pilot import (
    load_validated_run,
)
from src.candidate_endorsement_line_ledger import (
    LINE_BATCH_SPEC_SHA256,
    LINE_DECISION_STAGE,
    RECORD_EXTRACTION_STAGE,
    LINE_INVENTORY_SPEC_SHA256,
    get_frozen_recall40_round2_stage_b_contract,
    get_line_ledger_contract,
    sha256_json,
)


ROUND2_STAGE_A_INPUT = Path(
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round2_stage_a_preflight_20260901a"
)
ROUND2_STAGE_A_OUTPUT = Path(
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round2_stage_a_outputs_20260901a"
)
ROUND2_STAGE_A2_INPUT = Path(
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round2_stage_a2_preflight_20260901a"
)
ROUND2_STAGE_A2_OUTPUT = Path(
    "build/hive_candidate_extraction/"
    "v2_1_endorsement_recall40_round2_stage_a2_outputs_20260901a"
)
ROUND2_A3 = Path(
    "build/private_candidate_endorsement_recall40_round2_a3_20260901"
)


@pytest.mark.skipif(
    not all((path / "manifest.json").is_file() for path in (
        DEFAULT_FULL_EXPORT_DIR, ROUND2_STAGE_A_INPUT, ROUND2_STAGE_A2_INPUT,
    ))
    or not (ROUND2_STAGE_A_OUTPUT / "run_metadata_shard_00000.json").is_file()
    or not (ROUND2_STAGE_A2_OUTPUT / "run_metadata_shard_00000.json").is_file()
    or not (ROUND2_A3 / "results.json").is_file(),
    reason="authoritative round-two A1/A2/A3 artifacts are not present",
)
def test_round2_a3_builds_frozen_stage_b_and_rejects_scope_tamper(
    tmp_path: Path,
) -> None:
    export_dir = tmp_path / "round2_stage_b"
    manifest = build_recall40_round2_record_extraction_export(
        DEFAULT_FULL_EXPORT_DIR,
        ROUND2_STAGE_A_INPUT,
        ROUND2_STAGE_A_OUTPUT,
        ROUND2_STAGE_A2_INPUT,
        ROUND2_STAGE_A2_OUTPUT,
        ROUND2_A3 / "task.json",
        ROUND2_A3 / "index.html",
        ROUND2_A3 / "results.json",
        export_dir,
    )
    contract = get_frozen_recall40_round2_stage_b_contract()
    validated, _manifest_hash, chunks = validate_input_manifest(
        export_dir / "manifest.json",
        export_dir / "chunks",
        stage=RECORD_EXTRACTION_STAGE,
        contract=contract,
    )
    assert sum(entry["records"] for entry in validated["files"]["chunks"]) == 304
    assert len(chunks) == 6
    assert validated["private_recall40_round2_stage_b"]["final_anchor_count"] == 2276

    attacked = json.loads(json.dumps(manifest))
    attacked["private_recall40_round2_stage_b"]["stage_a3_residual_count"] = 13
    payload = dict(attacked)
    payload.pop("manifest_payload_sha256", None)
    attacked["manifest_payload_sha256"] = sha256_json(payload)
    (export_dir / "manifest.json").write_text(
        json.dumps(attacked, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="round-two Stage-B scope"):
        validate_input_manifest(
            export_dir / "manifest.json",
            export_dir / "chunks",
            stage=RECORD_EXTRACTION_STAGE,
            contract=contract,
        )


def test_corrected_issue_gate_keeps_public_resolution_and_private_conflict_separate():
    assert CORRECTED_ISSUE_AUDIT_GATE == {
        "route": "issues",
        "parent_document_id": (
            "71c197e1b81d48216f8c2cb757f1da7e2b769819dd64e0e7865645d96b62bec8"
        ),
        "route_status": "unchanged_v2_1_issue_route_control",
        "minimum_quote_bearing_resolved_issue_records": 2,
        "minimum_fully_accounted_private_conflict_groups": 1,
        "required_conflict_statement_kinds": ["proposal", "oppose"],
        "conflict_disposition": "conflict_withheld_private",
        "legacy_minimum_three_resolved_records_gate_disallowed": True,
    }


@pytest.mark.skipif(
    not (DEFAULT_FULL_EXPORT_DIR / "manifest.json").is_file(),
    reason="authoritative suffix-p full source export is not present",
)
def test_runner_manifest_authentication_rejects_unlisted_and_global_duplicates(
    tmp_path: Path,
) -> None:
    export_dir = tmp_path / "authenticated_export"
    manifest = build_line_decision_export(
        DEFAULT_FULL_EXPORT_DIR, export_dir, preflight=True, chunk_size=60
    )
    manifest["contract_is_frozen"] = True
    manifest["frozen_line_ledger_contract_sha256"] = manifest[
        "line_ledger_contract_sha256"
    ]
    payload = dict(manifest)
    payload.pop("manifest_payload_sha256", None)
    manifest["manifest_payload_sha256"] = sha256_json(payload)
    manifest_path = export_dir / "manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    contract = get_line_ledger_contract()
    validate_input_manifest(
        manifest_path, export_dir / "chunks",
        stage=LINE_DECISION_STAGE, contract=contract,
    )

    for field, tampered_value in (
        ("json_schema_catalog_sha256", "0" * 64),
        ("json_schema_variant_sha256", {}),
        ("schema_variant_counts", {}),
        ("ordered_schema_variant_ids_sha256", "1" * 64),
        ("line_ledger_contract", {"fabricated": True}),
        ("frozen_line_ledger_contract_sha256", "2" * 64),
        ("unchanged_route_hashes", {"fabricated": True}),
    ):
        attacked_manifest = json.loads(json.dumps(manifest))
        attacked_manifest[field] = tampered_value
        payload = dict(attacked_manifest)
        payload.pop("manifest_payload_sha256", None)
        attacked_manifest["manifest_payload_sha256"] = sha256_json(payload)
        manifest_path.write_text(
            json.dumps(attacked_manifest, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        with pytest.raises(ValueError, match="manifest|schema variant"):
            _load_stage_a_input_export(export_dir)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    first_chunk = export_dir / "chunks/chunk_00000.jsonl.gz"
    original_first_rows = list(read_jsonl_gz(first_chunk))
    schema_attacked_rows = json.loads(json.dumps(original_first_rows))
    schema_attacked_rows[0]["json_schema_variant_id"] = (
        "line_decision_targets_01_v1"
    )
    write_jsonl_gz(first_chunk, schema_attacked_rows)
    schema_attacked_manifest = json.loads(json.dumps(manifest))
    first_entry = schema_attacked_manifest["files"]["chunks"][0]
    first_entry.update({
        "bytes": first_chunk.stat().st_size,
        "sha256": sha256_file(first_chunk),
        "content_sha256": sha256_json(schema_attacked_rows),
    })
    payload = dict(schema_attacked_manifest)
    payload.pop("manifest_payload_sha256", None)
    schema_attacked_manifest["manifest_payload_sha256"] = sha256_json(payload)
    manifest_path.write_text(
        json.dumps(schema_attacked_manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="schema variant"):
        validate_input_manifest(
            manifest_path, export_dir / "chunks",
            stage=LINE_DECISION_STAGE, contract=contract,
        )
    write_jsonl_gz(first_chunk, original_first_rows)
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    extra_chunk = export_dir / "chunks/chunk_99999.jsonl.gz"
    shutil.copyfile(first_chunk, extra_chunk)
    with pytest.raises(ValueError, match="unlisted or missing input chunks"):
        validate_input_manifest(
            manifest_path, export_dir / "chunks",
            stage=LINE_DECISION_STAGE, contract=contract,
        )
    extra_chunk.unlink()

    second_chunk = export_dir / "chunks/chunk_00001.jsonl.gz"
    second_rows = list(read_jsonl_gz(second_chunk))
    original_second_rows = json.loads(json.dumps(second_rows))
    first_row = next(read_jsonl_gz(first_chunk))
    second_rows[0] = first_row
    write_jsonl_gz(second_chunk, second_rows)
    attacked = json.loads(json.dumps(manifest))
    second_entry = attacked["files"]["chunks"][1]
    second_entry.update({
        "records": len(second_rows),
        "bytes": second_chunk.stat().st_size,
        "sha256": sha256_file(second_chunk),
        "content_sha256": sha256_json(second_rows),
        "first_document_id": second_rows[0]["batch_id"],
        "last_document_id": second_rows[-1]["batch_id"],
    })
    expected_ids = list(attacked["expected_batch_ids"])
    expected_ids[len(list(read_jsonl_gz(first_chunk)))] = first_row["batch_id"]
    attacked["expected_batch_ids"] = expected_ids
    attacked["expected_batch_ids_sha256"] = sha256_json(expected_ids)
    payload = dict(attacked)
    payload.pop("manifest_payload_sha256", None)
    attacked["manifest_payload_sha256"] = sha256_json(payload)
    manifest_path.write_text(
        json.dumps(attacked, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="batch ID order/coverage"):
        validate_input_manifest(
            manifest_path, export_dir / "chunks",
            stage=LINE_DECISION_STAGE, contract=contract,
        )
    write_jsonl_gz(second_chunk, original_second_rows)


@pytest.mark.skipif(
    not (DEFAULT_FULL_EXPORT_DIR / "manifest.json").is_file(),
    reason="authoritative suffix-p full source export is not present",
)
def test_preflight_replay_and_stage_b_require_complete_bound_outputs(tmp_path: Path):
    stage_a_export = tmp_path / "stage_a_export"
    manifest = build_line_decision_export(
        DEFAULT_FULL_EXPORT_DIR,
        stage_a_export,
        preflight=True,
        chunk_size=60,
    )
    validate_manifest_payload_hash(manifest)
    assert manifest["selected_parent_ids"] == list(PILOT_PARENT_IDS)
    assert manifest["inventory_census"] == FROZEN_PILOT_INVENTORY
    assert manifest["expected_batch_counts"] == FROZEN_PILOT_BATCH_COUNTS
    assert manifest["exported_records"] == 113
    assert manifest["complete_parent_closure"] is True
    assert manifest["bounded_preflight"]["builder_performed_inference"] is False
    assert manifest["bounded_preflight"][
        "requires_separate_submission_approval"
    ] is True
    assert manifest["schema_variant_counts"] == {
        "line_decision_targets_04_v1": 1,
        "line_decision_targets_05_v1": 1,
        "line_decision_targets_08_v1": 2,
        "line_decision_targets_10_v1": 1,
        "line_decision_targets_11_v1": 1,
        "line_decision_targets_16_v1": 107,
    }
    assert manifest["json_schema_catalog_sha256"] == get_line_ledger_contract()[
        "json_schema_catalog_sha256"
    ][LINE_DECISION_STAGE]
    assert manifest["ordered_schema_variant_ids_sha256"]
    assert manifest["ordered_schema_variant_hashes_sha256"]
    manifest["contract_is_frozen"] = True
    manifest["frozen_line_ledger_contract_sha256"] = manifest[
        "line_ledger_contract_sha256"
    ]
    manifest_payload = dict(manifest)
    manifest_payload.pop("manifest_payload_sha256", None)
    manifest["manifest_payload_sha256"] = sha256_json(manifest_payload)
    (stage_a_export / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    input_chunks = sorted((stage_a_export / "chunks").glob("*.jsonl.gz"))
    input_manifest_sha256 = sha256_file(stage_a_export / "manifest.json")
    contract = get_line_ledger_contract()
    contract_sha256 = contract["line_ledger_contract_sha256"]
    runner_args = Namespace(
        stage=LINE_DECISION_STAGE,
        seed=20260825,
        max_model_len=32_768,
        max_output_tokens=2_048,
        shard_index=None,
        shard_count=None,
    )
    generation_hash = generation_config_sha256(
        runner_args,
        contract,
        input_manifest_sha256=input_manifest_sha256,
    )

    stage_a_output = tmp_path / "stage_a_output"
    stage_a_output.mkdir()
    for shard_index, input_chunk in enumerate(input_chunks):
        input_chunk_rows = list(read_jsonl_gz(input_chunk))
        shard_rows = []
        for batch in input_chunk_rows:
            target_slots = set(batch["target_slots"])
            model_output = {
                f"slot_{index:02d}": {
                    "role": (
                        "nonrecord_content"
                        if f"slot_{index:02d}" in target_slots
                        else "padding"
                    )
                }
                for index in range(16)
            }
            raw_output = json.dumps(model_output, ensure_ascii=False)
            shard_rows.append(_derive_output_row(
                batch=batch,
                raw_output=raw_output,
                finish_reason="stop",
                stop_reason=None,
                output_tokens=100,
                prompt_tokens=1_000,
                args=runner_args,
                contract=contract,
                input_manifest_sha256=input_manifest_sha256,
                generation_hash=generation_hash,
            ))
        write_jsonl_gz(
            stage_a_output / input_chunk.name.replace(
                ".jsonl.gz", ".out.jsonl.gz"
            ),
            shard_rows,
        )
        shard_args = Namespace(
            **{
                **vars(runner_args),
                "shard_index": shard_index,
                "shard_count": len(input_chunks),
            }
        )
        metadata = _run_metadata(
            args=shard_args,
            contract=contract,
            generation_hash=generation_hash,
            input_manifest_sha256=input_manifest_sha256,
            selected=len(shard_rows),
            selected_variant_inventory=schema_variant_inventory(
                input_chunk_rows
            ),
            selected_stage_a_provenance_sha256=[],
            rows=shard_rows,
            started="2026-08-25T00:00:00+00:00",
            recovery=False,
        )
        (stage_a_output / f"run_metadata_shard_{shard_index:05d}.json").write_text(
            json.dumps(metadata, sort_keys=True) + "\n",
            encoding="utf-8",
        )

    loaded_manifest, loaded_batches, loaded_rows, loaded_artifacts = (
        load_validated_run(
            stage_a_export, stage_a_output, stage=LINE_DECISION_STAGE
        )
    )
    assert loaded_manifest["exported_records"] == 113
    assert len(loaded_batches) == len(loaded_rows) == 113
    assert loaded_artifacts["validated_output_batches"] == 113

    for attack_name in ("filename", "boolean_index", "empty_shard"):
        attacked_output = tmp_path / f"stage_a_output_{attack_name}"
        shutil.copytree(stage_a_output, attacked_output)
        first_metadata_path = (
            attacked_output / "run_metadata_shard_00000.json"
        )
        if attack_name == "filename":
            first_metadata_path.rename(
                attacked_output / "run_metadata_shard_00002.json"
            )
        elif attack_name == "boolean_index":
            attacked_metadata = json.loads(
                first_metadata_path.read_text(encoding="utf-8")
            )
            attacked_metadata["shard_index"] = False
            first_metadata_path.write_text(
                json.dumps(attacked_metadata, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        else:
            for metadata_path in sorted(
                attacked_output.glob("run_metadata_shard_*.json")
            ):
                attacked_metadata = json.loads(
                    metadata_path.read_text(encoding="utf-8")
                )
                attacked_metadata["shard_count"] = len(input_chunks) + 1
                metadata_path.write_text(
                    json.dumps(attacked_metadata, sort_keys=True) + "\n",
                    encoding="utf-8",
                )
            empty_metadata = dict(metadata)
            empty_metadata.update({
                "shard_index": len(input_chunks),
                "shard_count": len(input_chunks) + 1,
                "selected_batches": 0,
                "selected_schema_variant_counts": {},
                "selected_schema_variant_hashes_used": {},
                "selected_schema_variant_ids_sha256": sha256_json([]),
                "selected_schema_variant_hashes_sha256": sha256_json([]),
                "recovery_attestation": True,
                "processed_outputs_this_run": 0,
                "processed_schema_variant_counts_this_run": {},
                "technical_pass_outputs_this_run": 0,
                "parse_status_counts_this_run": {},
                "finish_reason_counts_this_run": {},
                "output_tokens_total_this_run": 0,
                "output_tokens_max_this_run": 0,
                "prompt_tokens_max_this_run": 0,
                "minimum_context_margin_tokens_this_run": 0,
            })
            (attacked_output / "run_metadata_shard_00002.json").write_text(
                json.dumps(empty_metadata, sort_keys=True) + "\n",
                encoding="utf-8",
            )
        with pytest.raises(ValueError, match="shard|metadata"):
            load_validated_run(
                stage_a_export,
                attacked_output,
                stage=LINE_DECISION_STAGE,
            )

    stage_b_export = tmp_path / "stage_b_export"
    stage_b = build_record_extraction_export(
        DEFAULT_FULL_EXPORT_DIR,
        stage_a_export,
        stage_a_output,
        stage_b_export,
    )
    validate_manifest_payload_hash(stage_b)
    assert stage_b["stage"] == "record_extraction"
    assert stage_b["stage_a_provenance"]["source_line_decision_count"] == 1_758
    assert stage_b["stage_a_provenance"]["anchor_line_count"] == 0
    assert stage_b["exported_records"] == 0
    assert stage_b["stage_a_provenance"][
        "raw_to_canonical_typed_replay_complete"
    ] is True

    tampered_path = stage_a_output / "chunk_00000.out.jsonl.gz"
    tampered_rows = list(read_jsonl_gz(tampered_path))
    tampered_rows[0]["raw_output"] = "{}"
    write_jsonl_gz(tampered_path, tampered_rows)
    with pytest.raises(ValueError, match="raw-to-canonical typed replay"):
        build_record_extraction_export(
            DEFAULT_FULL_EXPORT_DIR,
            stage_a_export,
            stage_a_output,
            tmp_path / "tampered_stage_b_export",
        )

    with pytest.raises(FileExistsError, match="refusing nonempty output directory"):
        build_line_decision_export(
            DEFAULT_FULL_EXPORT_DIR,
            stage_a_export,
            preflight=True,
        )
