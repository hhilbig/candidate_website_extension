"""Focused tests for the v2 Hive importer and overlap policy."""
from __future__ import annotations

import base64
import gzip
import hashlib
import csv
import io
import json
import runpy
import sys
from collections import Counter
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace

import pytest

import scripts.import_candidate_extraction_v2 as importer

from scripts.import_candidate_extraction_v2 import (
    EXPECTED_GENERATION_CONFIG,
    EXPECTED_GENERATION_CONFIG_SHA256,
    EXPECTED_MAX_OUTPUT_TOKENS,
    EXPECTED_REPAIR_GENERATION_CONFIG,
    EXPECTED_REPAIR_GENERATION_CONFIG_SHA256,
    EXPECTED_REPAIR_GENERATION_CONFIG_SHA256S,
    EXPECTED_REPAIR_GENERATION_CONFIGS,
    EXPECTED_REPAIR_GENERATION_SEED,
    EXPECTED_REPAIR_GENERATION_SEEDS,
    FROZEN_STAGE_A2_CHUNK_SIZE,
    FROZEN_STAGE_A2_GENERATION_CONFIG_SHA256,
    FROZEN_STAGE_A2_INPUT_MANIFEST_SHA256,
    FROZEN_SUFFIX_P_COMPLETE_LINE_CENSUS,
    FROZEN_SUFFIX_P_FULL_SOURCE_MANIFEST_SHA256,
    FROZEN_SUFFIX_P_SOURCE_CORPUS_SHA256,
    FROZEN_SUFFIX_P_SOURCE_DOCUMENT_IDS_SHA256,
    KNOWN_OMISSION_MATCHING_SPEC_SHA256,
    OUTPUT_INFERENCE_DISPLAY_FIELDS,
    OUTPUT_RAW_STRUCTURAL_WHITESPACE_FIELDS,
    REPAIR_ATTEMPT_INDEX,
    REPAIR_MANIFEST_VERSION,
    REPAIR_REASON,
    RUN_METADATA_MAPPING_FIELDS,
    RUN_METADATA_MAX_MAPPING_FIELDS,
    RUN_METADATA_SCALAR_FIELDS,
    all_output_aggregate,
    apply_repair_attempts,
    apply_repair_windows,
    build_record_variants,
    build_bundle,
    canonical_json,
    endorsement_segment_complete_line_census,
    enrich_segments,
    load_inputs,
    load_repair_windows,
    match_known_omissions,
    output_provenance_summary,
    resolve_parent_variants,
    run_metadata_aggregate,
    technical_segment_gate_passes,
    validate_generation_config_identity,
    validate_endorsement_context_manifest,
    validate_output_row,
    validate_run_metadata_accounting,
    validate_run_metadata_against_all_output,
    validate_run_metadata_source_coverage,
    validate_source_export_identity,
    validate_endorsement_line_ledger_private_rows,
    load_endorsement_line_ledger_runs,
    _attest_line_ledger_transport_files,
    _stage_a3_private_row,
    _validate_stage_a3_private_row,
    _validate_line_ledger_output_row,
)
from scripts.import_candidate_endorsement_line_ledger_a3_handcode import (
    import_results as import_a3_results,
)
from scripts.evaluate_candidate_endorsement_line_ledger_pilot import (
    replay_stage_a2_merge,
)
from scripts.hive.extract_candidate_endorsement_line_ledger_vllm import (
    DEFAULT_MAX_OUTPUT_TOKENS as LINE_LEDGER_MAX_OUTPUT_TOKENS,
    FROZEN_GENERATION_SEED as LINE_LEDGER_GENERATION_SEED,
    PROMPT_TOKEN_COUNT_SPEC_SHA256,
    RUN_METADATA_VERSION as LINE_LEDGER_RUN_METADATA_VERSION,
    _derive_output_row as derive_line_ledger_output_row,
    generation_config_payload as line_ledger_generation_config_payload,
    generation_config_sha256 as line_ledger_generation_config_sha256,
    schema_variant_inventory as line_ledger_schema_variant_inventory,
)
from scripts.build_candidate_endorsement_line_ledger import (
    CORRECTED_ISSUE_AUDIT_GATE,
    DEFAULT_CHUNK_SIZE as LINE_LEDGER_CHUNK_SIZE,
    FROZEN_SOURCE_PARENTS_BY_ROUTE,
    FROZEN_SOURCE_SCHEMA_VERSION,
    FROZEN_SOURCE_SEGMENTS_BY_ROUTE,
    _selection_rows as line_ledger_selection_rows,
)
from scripts.hive.extract_candidate_pages_vllm import (
    RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID,
    RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256,
    STRUCTURED_DECODING_BACKEND,
    STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
    build_user_message,
    canonicalize_json_string_controls,
    validate_raw_structural_whitespace,
)
from src.candidate_extraction import (
    ARRAY_ITEM_CAP_REACHED_STATUS,
    MODEL_ID,
    MODEL_ITEM_REPETITION_ERROR_STATUS,
    MODEL_REVISION,
    RUNTIME_LOCK_SHA256,
    ENDORSEMENT_CONTEXT_CLASSIFIER_ID,
    ENDORSEMENT_CONTEXT_CLASSIFIER_SPEC_SHA256,
    ENDORSEMENT_CONTEXT_PROVENANCE_SPEC_SHA256,
    ENDORSEMENT_CONTEXT_PROVENANCE_VERSION,
    ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC_SHA256,
    align_extraction_exact_spans,
    build_endorsement_context_provenance,
    get_extraction_contract,
    inference_display_provenance,
    pre_alignment_model_item_guard_report,
    validate_extraction,
)
from src.candidate_extraction_provenance import (
    OFFSET_COORDINATE_SYSTEM,
    SOURCE_TEXT_TRANSFORM_RULE,
    SOURCE_TEXT_TRANSFORM_VERSION,
)
from src.candidate_endorsement_line_ledger import (
    FROZEN_STAGE_A_V2_CONTRACT_SHA256,
    LINE_DECISION_SLOT_COUNT,
    _compound_anchor_expected_names,
    _required_split_line_name,
    LINE_DECISION_STAGE,
    RECORD_EXTRACTION_SLOT_COUNT,
    RECORD_EXTRACTION_STAGE,
    build_line_decision_batches,
    build_record_extraction_batches,
    get_line_ledger_contract,
    expected_batch_counts,
    inventory_census,
    line_decision_ledger,
    reconstruct_parent_inventory,
    sha256_json as line_ledger_sha256_json,
    sha256_text as line_ledger_sha256_text,
)


CONTRACT = get_extraction_contract("2.1")


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _line_ledger_test_segment() -> dict[str, object]:
    source = "Endorsements\nAlice Smith\n"
    parent_id = line_ledger_sha256_text("test-parent")
    segment_id = line_ledger_sha256_text("test-segment")
    source_urls = [
        "https://web.archive.org/web/20240101000000/"
        "https://example.com/endorsements/"
    ]
    return {
        "candidate_cycle_id": "H4TX00000-2024",
        "candidate_icpsr": "999999",
        "document_id": segment_id,
        "parent_document_id": parent_id,
        "route": "endorsements",
        "state": "TX",
        "office": "house",
        "year": 2024,
        "party": "democrat",
        "raw_text_sha256": line_ledger_sha256_text(source),
        "extraction_source_text_sha256": line_ledger_sha256_text(source),
        "extraction_source_n_chars": len(source),
        "source_urls": source_urls,
        "segment_index": 0,
        "segment_count": 1,
        "segment_start_char": 0,
        "segment_end_char": len(source),
        "segment_text_sha256": line_ledger_sha256_text(source),
        "source_text": source,
        "endorsement_context_provenance": (
            build_endorsement_context_provenance(
                parent_document_id=parent_id,
                parent_source_text=source,
                source_urls=source_urls,
                segment_id=segment_id,
                segment_start_char=0,
                segment_end_char=len(source),
            )
        ),
    }


def _line_ledger_args(stage: str) -> SimpleNamespace:
    return SimpleNamespace(
        stage=stage,
        seed=LINE_LEDGER_GENERATION_SEED,
        max_model_len=32_768,
        max_output_tokens=LINE_LEDGER_MAX_OUTPUT_TOKENS[stage],
    )


def _selection_csv_text(rows: list[dict[str, object]]) -> str:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(
        buffer, fieldnames=list(rows[0]), lineterminator="\n"
    )
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def _chunk_attestation(
    file_name: str,
    rows: list[dict[str, object]],
    *,
    transport_sha256: str,
    transport_bytes: int,
    compressed_bytes: bytes,
) -> dict[str, object]:
    content = "".join(
        json.dumps(
            row,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ) + "\n"
        for row in rows
    ).encode("utf-8")
    return {
        "file": file_name,
        "records": len(rows),
        "transport_sha256": transport_sha256,
        "transport_bytes": transport_bytes,
        "compressed_bytes_base64": base64.b64encode(
            compressed_bytes
        ).decode("ascii"),
        "content_attestation_version": "canonical_sorted_jsonl_utf8_v1",
        "canonical_jsonl_sha256": hashlib.sha256(content).hexdigest(),
        "canonical_jsonl_bytes": len(content),
    }


def _private_line_ledger_stage(
    stage: str,
    batches: list[dict[str, object]],
    model_outputs: list[dict[str, object]],
    *,
    parents: list[dict[str, object]],
    stage_a_manifest_provenance: dict[str, object] | None = None,
    stage_a_decisions: dict[str, dict[str, object]] | None = None,
) -> tuple[
    dict[str, object], list[dict[str, object]], dict[str, object],
    dict[str, object],
]:
    contract = get_line_ledger_contract()
    parent_ids = [str(parent["parent_document_id"]) for parent in parents]
    batch_ids = [str(batch["batch_id"]) for batch in batches]
    variant_inventory = line_ledger_schema_variant_inventory(batches)
    mode = (
        "line_decision_full"
        if stage == LINE_DECISION_STAGE
        else "record_extraction_full_from_stage_a"
    )
    selection_rows = line_ledger_selection_rows(
        parents, mode=mode, stage_a_decisions=stage_a_decisions
    )
    selection_text = _selection_csv_text(selection_rows)
    selection_bytes = selection_text.encode("utf-8")
    chunks: list[dict[str, object]] = []
    input_files: list[dict[str, object]] = []
    output_files: list[dict[str, object]] = []
    input_compressed_chunks: list[bytes] = []
    for chunk_index, offset in enumerate(
        range(0, len(batches), LINE_LEDGER_CHUNK_SIZE)
    ):
        part = batches[offset:offset + LINE_LEDGER_CHUNK_SIZE]
        name = f"chunk_{chunk_index:05d}.jsonl.gz"
        input_raw = "".join(
            json.dumps(row, ensure_ascii=False) + "\n" for row in part
        ).encode("utf-8")
        input_compressed = gzip.compress(input_raw, mtime=0)
        input_compressed_chunks.append(input_compressed)
        chunk_sha256 = hashlib.sha256(input_compressed).hexdigest()
        chunks.append({
            "file": name,
            "records": len(part),
            "first_document_id": part[0]["document_id"],
            "last_document_id": part[-1]["document_id"],
            "content_sha256": line_ledger_sha256_json(part),
            "bytes": len(input_compressed),
            "sha256": chunk_sha256,
        })
        input_files.append({"file": name, "sha256": chunk_sha256})
        output_files.append({
            "file": name.replace(".jsonl.gz", ".out.jsonl.gz"),
            "sha256": "0" * 64,
            "bytes": 1,
            "records": len(part),
        })
    manifest: dict[str, object] = {
        "builder": "scripts/build_candidate_endorsement_line_ledger.py",
        "builder_version": "candidate_endorsement_line_ledger_export_v1",
        "mode": mode,
        "stage": stage,
        "route": "endorsements",
        "source_export": {
            "schema_version": FROZEN_SOURCE_SCHEMA_VERSION,
            "source_corpus_sha256": FROZEN_SUFFIX_P_SOURCE_CORPUS_SHA256,
            "source_export_manifest_sha256": (
                FROZEN_SUFFIX_P_FULL_SOURCE_MANIFEST_SHA256
            ),
            "source_document_ids_sha256": (
                FROZEN_SUFFIX_P_SOURCE_DOCUMENT_IDS_SHA256
            ),
            "replayed_segments_by_route": FROZEN_SOURCE_SEGMENTS_BY_ROUTE,
            "replayed_parents_by_route": FROZEN_SOURCE_PARENTS_BY_ROUTE,
            "corrected_issue_control_source_segments": 3,
        },
        "line_ledger_contract": contract,
        "line_ledger_contract_sha256": contract[
            "line_ledger_contract_sha256"
        ],
        "frozen_line_ledger_contract_sha256": contract[
            "line_ledger_contract_sha256"
        ],
        "contract_is_frozen": True,
        "line_inventory_spec_sha256": contract[
            "line_inventory_spec_sha256"
        ],
        "line_batch_spec_sha256": contract["line_batch_spec_sha256"],
        "json_schema_catalog_sha256": contract[
            "json_schema_catalog_sha256"
        ][stage],
        "json_schema_variant_sha256": contract[
            "json_schema_variant_sha256"
        ][stage],
        "unchanged_route_hashes": {
            "issue_prompt_sha256": contract["unchanged_issue_prompt_sha256"],
            "issue_schema_sha256": contract["unchanged_issue_schema_sha256"],
            "biography_prompt_sha256": contract[
                "unchanged_biography_prompt_sha256"
            ],
            "biography_schema_sha256": contract[
                "unchanged_biography_schema_sha256"
            ],
        },
        **variant_inventory,
        "corrected_71c_issue_audit_gate": CORRECTED_ISSUE_AUDIT_GATE,
        "complete_parent_closure": True,
        "selected_parent_ids": parent_ids,
        "selected_parent_ids_sha256": line_ledger_sha256_json(parent_ids),
        "inventory_census": inventory_census(parents),
        "expected_batch_counts": expected_batch_counts(parents),
        "expected_batch_ids": batch_ids,
        "expected_batch_ids_sha256": line_ledger_sha256_json(batch_ids),
        "expected_stage_batch_count": len(batches),
        "exported_records": len(batches),
        "chunk_size": LINE_LEDGER_CHUNK_SIZE,
        "files": {
            "selection": {
                "file": "selection.csv",
                "rows": len(selection_rows),
                "columns": list(selection_rows[0]),
                "bytes": len(selection_bytes),
                "sha256": hashlib.sha256(selection_bytes).hexdigest(),
            },
            "chunks": chunks,
        },
        "bounded_preflight": {
            "enabled": False,
            "pilot_controls": [],
            "predeclared_content_gates": {},
            "builder_performed_inference": False,
            "requires_separate_submission_approval": True,
        },
    }
    if stage == RECORD_EXTRACTION_STAGE:
        assert stage_a_manifest_provenance is not None
        manifest["stage_a_provenance"] = stage_a_manifest_provenance
    manifest["manifest_payload_sha256"] = line_ledger_sha256_json(manifest)
    manifest_text = json.dumps(manifest, sort_keys=True)
    manifest_sha256 = line_ledger_sha256_text(manifest_text)
    args = _line_ledger_args(stage)
    generation = line_ledger_generation_config_payload(
        args, contract, input_manifest_sha256=manifest_sha256
    )
    generation_sha256 = line_ledger_sha256_json(generation)
    outputs = [
        derive_line_ledger_output_row(
            batch=batch,
            raw_output=json.dumps(model_output),
            finish_reason="stop",
            stop_reason=None,
            output_tokens=256,
            prompt_tokens=512,
            args=args,
            contract=contract,
            input_manifest_sha256=manifest_sha256,
            generation_hash=generation_sha256,
        )
        for batch, model_output in zip(batches, model_outputs, strict=True)
    ]
    output_compressed_chunks: list[bytes] = []
    for chunk_index, output_file in enumerate(output_files):
        part = outputs[
            chunk_index * LINE_LEDGER_CHUNK_SIZE:
            (chunk_index + 1) * LINE_LEDGER_CHUNK_SIZE
        ]
        output_raw = "".join(
            json.dumps(row, ensure_ascii=False) + "\n" for row in part
        ).encode("utf-8")
        output_compressed = gzip.compress(output_raw, mtime=0)
        output_compressed_chunks.append(output_compressed)
        output_file["sha256"] = hashlib.sha256(output_compressed).hexdigest()
        output_file["bytes"] = len(output_compressed)
    metadata = {
        "run_metadata_version": LINE_LEDGER_RUN_METADATA_VERSION,
        "accounting_scope": "outputs_inferred_during_this_process_attempt",
        "recovery_attestation": False,
        "stage": stage,
        "input_manifest_sha256": manifest_sha256,
        "line_ledger_contract_sha256": contract[
            "line_ledger_contract_sha256"
        ],
        "prompt_sha256": contract["prompt_sha256"][stage],
        "json_schema_catalog_sha256": contract[
            "json_schema_catalog_sha256"
        ][stage],
        "json_schema_variant_sha256": contract[
            "json_schema_variant_sha256"
        ][stage],
        "line_inventory_spec_sha256": contract[
            "line_inventory_spec_sha256"
        ],
        "line_batch_spec_sha256": contract["line_batch_spec_sha256"],
        "model_id": contract["model_id"],
        "model_revision": contract["model_revision"],
        "runtime_lock_sha256": contract["runtime_lock_sha256"],
        "structured_decoding_backend": STRUCTURED_DECODING_BACKEND,
        "structured_decoding_disable_any_whitespace": (
            STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
        ),
        "prompt_token_count_spec_sha256": PROMPT_TOKEN_COUNT_SPEC_SHA256,
        "generation_config": generation,
        "generation_config_sha256": generation_sha256,
        "max_model_len": args.max_model_len,
        "max_output_tokens": args.max_output_tokens,
        "shard_index": None,
        "shard_count": None,
        "selected_batches": len(batches),
        "selected_schema_variant_counts": variant_inventory[
            "schema_variant_counts"
        ],
        "selected_schema_variant_hashes_used": variant_inventory[
            "schema_variant_hashes_used"
        ],
        "selected_schema_variant_ids_sha256": variant_inventory[
            "ordered_schema_variant_ids_sha256"
        ],
        "selected_schema_variant_hashes_sha256": variant_inventory[
            "ordered_schema_variant_hashes_sha256"
        ],
        "processed_outputs_this_run": len(outputs),
        "processed_schema_variant_counts_this_run": dict(sorted(Counter(
            str(output["json_schema_variant_id"]) for output in outputs
        ).items())),
        "technical_pass_outputs_this_run": sum(
            output["technical_pass"] is True for output in outputs
        ),
        "parse_status_counts_this_run": dict(sorted(Counter(
            str(output["parse_status"]) for output in outputs
        ).items())),
        "finish_reason_counts_this_run": dict(sorted(Counter(
            str(output["finish_reason"]) for output in outputs
        ).items())),
        "output_tokens_total_this_run": sum(
            int(output["output_tokens"]) for output in outputs
        ),
        "output_tokens_max_this_run": max(
            int(output["output_tokens"]) for output in outputs
        ),
        "prompt_tokens_max_this_run": max(
            int(output["prompt_tokens_used"]) for output in outputs
        ),
        "minimum_context_margin_tokens_this_run": min(
            int(output["context_margin_tokens"]) for output in outputs
        ),
        "started_at": "2026-08-25T00:00:00+00:00",
        "completed_at": "2026-08-25T00:01:00+00:00",
        "host": "test-host",
        "slurm_job_id": None,
        "slurm_array_task_id": None,
    }
    metadata_text = json.dumps(metadata, sort_keys=True)
    artifacts: dict[str, object] = {
        "input_manifest_sha256": manifest_sha256,
        "generation_config_sha256": generation_sha256,
        "max_model_len": args.max_model_len,
        "max_output_tokens": args.max_output_tokens,
        "input_files": input_files,
        "output_batch_ids_sha256": line_ledger_sha256_json(batch_ids),
        "validated_output_batches": len(outputs),
        "output_files": output_files,
        "run_metadata_files": [{
            "file": "run_metadata.json",
            "sha256": line_ledger_sha256_text(metadata_text),
        }],
        "typed_restart_replay_complete": True,
    }
    provenance = {
        "kind": "stage_provenance",
        "stage": stage,
        "input_manifest_text": manifest_text,
        "selection_text": selection_text,
        "input_chunk_attestations": [
            _chunk_attestation(
                str(chunk["file"]),
                batches[
                    index * LINE_LEDGER_CHUNK_SIZE:
                    (index + 1) * LINE_LEDGER_CHUNK_SIZE
                ],
                transport_sha256=str(chunk["sha256"]),
                transport_bytes=int(chunk["bytes"]),
                compressed_bytes=input_compressed_chunks[index],
            )
            for index, chunk in enumerate(chunks)
        ],
        "output_chunk_attestations": [
            _chunk_attestation(
                str(output_file["file"]),
                outputs[
                    index * LINE_LEDGER_CHUNK_SIZE:
                    (index + 1) * LINE_LEDGER_CHUNK_SIZE
                ],
                transport_sha256=str(output_file["sha256"]),
                transport_bytes=int(output_file["bytes"]),
                compressed_bytes=output_compressed_chunks[index],
            )
            for index, output_file in enumerate(output_files)
        ],
        "artifacts": artifacts,
        "run_metadata_files": [{
            "file": "run_metadata.json",
            "sha256": line_ledger_sha256_text(metadata_text),
            "text": metadata_text,
        }],
    }
    return provenance, outputs, artifacts, manifest


def _stage_a_manifest_provenance_fixture(
    provenance: dict[str, object],
    artifacts: dict[str, object],
    manifest: dict[str, object],
    batches: list[dict[str, object]],
    decisions: dict[str, dict[str, object]],
) -> dict[str, object]:
    private_metadata = provenance["run_metadata_files"]
    metadata = json.loads(str(private_metadata[0]["text"]))
    roles = Counter(str(decision["role"]) for decision in decisions.values())
    contract = get_line_ledger_contract()
    return {
        "run_metadata_files": [{
            "file": "run_metadata.json",
            "sha256": private_metadata[0]["sha256"],
            "shard_index": None,
            "shard_count": None,
            "selected_batches": len(batches),
            "processed_outputs_this_run": metadata[
                "processed_outputs_this_run"
            ],
        }],
        "input_manifest_sha256": artifacts["input_manifest_sha256"],
        "generation_config_sha256": artifacts["generation_config_sha256"],
        "json_schema_catalog_sha256": contract[
            "json_schema_catalog_sha256"
        ][LINE_DECISION_STAGE],
        "json_schema_variant_sha256": contract[
            "json_schema_variant_sha256"
        ][LINE_DECISION_STAGE],
        "schema_variant_inventory": line_ledger_schema_variant_inventory(
            batches
        ),
        "prompt_token_count_spec_sha256": PROMPT_TOKEN_COUNT_SPEC_SHA256,
        "max_model_len": 32_768,
        "max_output_tokens": LINE_LEDGER_MAX_OUTPUT_TOKENS[
            LINE_DECISION_STAGE
        ],
        "raw_to_canonical_typed_replay_complete": True,
        "exact_input_output_chunk_mapping": True,
        "output_files": artifacts["output_files"],
        "output_batch_ids_sha256": artifacts["output_batch_ids_sha256"],
        "validated_output_batches": artifacts["validated_output_batches"],
        "input_export_mode": manifest["mode"],
        "input_export_manifest_sha256": artifacts["input_manifest_sha256"],
        "input_batch_count": len(batches),
        "source_line_decision_count": len(decisions),
        "role_counts": dict(sorted(roles.items())),
        "anchor_line_count": (
            roles["record_anchor"] + roles["compound_record_anchor"]
        ),
        "unclear_line_count": roles["unclear"],
    }


def test_importer_replays_rotated_line_ledger_output_identity_exactly() -> None:
    parent = reconstruct_parent_inventory([_line_ledger_test_segment()])
    stage_a_batch = build_line_decision_batches([parent])[0]
    stage_a_model = {
        f"slot_{index:02d}": {
            "role": (
                "record_anchor"
                if f"slot_{index:02d}" in stage_a_batch["target_slots"]
                else "padding"
            )
        }
        for index in range(LINE_DECISION_SLOT_COUNT)
    }
    contract = get_line_ledger_contract()
    manifest_hash = line_ledger_sha256_text("stage-a-manifest")
    args = _line_ledger_args(LINE_DECISION_STAGE)
    generation_hash = line_ledger_generation_config_sha256(
        args, contract, input_manifest_sha256=manifest_hash
    )
    stage_a_row = derive_line_ledger_output_row(
        batch=stage_a_batch,
        raw_output=json.dumps(stage_a_model),
        finish_reason="stop",
        stop_reason=None,
        output_tokens=128,
        prompt_tokens=256,
        args=args,
        contract=contract,
        input_manifest_sha256=manifest_hash,
        generation_hash=generation_hash,
    )
    generation = {
        "input_manifest_sha256": manifest_hash,
        "generation_config_sha256": generation_hash,
        "max_model_len": args.max_model_len,
        "max_output_tokens": args.max_output_tokens,
    }
    validation, alignment = _validate_line_ledger_output_row(
        stage_a_batch,
        stage_a_row,
        LINE_DECISION_STAGE,
        generation,
        contract=contract,
    )
    assert validation["coverage_complete"] is True
    assert alignment is None
    assert stage_a_row["prompt_token_count_spec_sha256"] == (
        PROMPT_TOKEN_COUNT_SPEC_SHA256
    )

    attacks = {
        "json_schema_catalog_sha256": "0" * 64,
        "json_schema_variant_id": "line_decision_targets_16_v1",
        "json_schema_sha256": "1" * 64,
        "target_slot_count": True,
        "prompt_token_count_spec_sha256": "2" * 64,
    }
    for field, value in attacks.items():
        tampered = deepcopy(stage_a_row)
        tampered[field] = value
        with pytest.raises(ValueError, match="differs"):
            _validate_line_ledger_output_row(
                stage_a_batch,
                tampered,
                LINE_DECISION_STAGE,
                generation,
                contract=contract,
            )

    decisions = line_decision_ledger(
        [stage_a_batch],
        [{"batch_id": stage_a_batch["batch_id"], "model_output": stage_a_model}],
    )
    # Stage B must carry its adjudication provenance. The current contract
    # carries only the three-pass specification, because the Stage-A3 human
    # adjudication is part of it, so a two-pass batch is no longer a valid
    # input. Each line therefore records its Stage-A3 disposition.
    per_line_provenance = {
        line_id: {"stage_a3_human_adjudication": {"resolution": "unchanged"}}
        for line_id in decisions
    }
    stage_b_batch = build_record_extraction_batches(
        [parent], decisions,
        per_line_provenance=per_line_provenance,
        adjudication_pass_count=3,
    )[0]
    stage_b_model: dict[str, object] = {}
    for index in range(RECORD_EXTRACTION_SLOT_COUNT):
        slot = f"slot_{index:02d}"
        # The line-ledger Stage-B slot carries only "records", and each model
        # record carries only the six identity fields.  Evidence spans and the
        # support mode are deterministic projections, so the model never
        # returns them.
        stage_b_model[slot] = (
            {
                "records": [{
                    "endorser_name": "Alice Smith",
                    "entity_type": "individual",
                    "endorser_category": "private_individual",
                    "title": None,
                    "affiliation": None,
                    "jurisdiction": None,
                }],
            }
            if slot in stage_b_batch["target_slots"]
            else {"records": []}
        )
    manifest_hash = line_ledger_sha256_text("stage-b-manifest")
    args = _line_ledger_args(RECORD_EXTRACTION_STAGE)
    generation_hash = line_ledger_generation_config_sha256(
        args, contract, input_manifest_sha256=manifest_hash
    )
    stage_b_row = derive_line_ledger_output_row(
        batch=stage_b_batch,
        raw_output=json.dumps(stage_b_model),
        finish_reason="stop",
        stop_reason=None,
        output_tokens=256,
        prompt_tokens=512,
        args=args,
        contract=contract,
        input_manifest_sha256=manifest_hash,
        generation_hash=generation_hash,
    )
    generation = {
        "input_manifest_sha256": manifest_hash,
        "generation_config_sha256": generation_hash,
        "max_model_len": args.max_model_len,
        "max_output_tokens": args.max_output_tokens,
    }
    _, alignment = _validate_line_ledger_output_row(
        stage_b_batch,
        stage_b_row,
        RECORD_EXTRACTION_STAGE,
        generation,
        contract=contract,
    )
    assert alignment is not None
    assert alignment["public_coverage_complete"] is True
    assert alignment["aligned_record_count"] == 1
    private_tamper = deepcopy(stage_b_row)
    private_tamper["stage_alignment"]["aligned_records"][0][
        "inventory_owner_segment_id"
    ] = "f" * 64
    with pytest.raises(ValueError, match="differ"):
        _validate_line_ledger_output_row(
            stage_b_batch,
            private_tamper,
            RECORD_EXTRACTION_STAGE,
            generation,
            contract=contract,
        )


def test_private_line_ledger_rejects_legacy_a1_b_bundle_without_a2() -> None:
    legacy_rows = [
        {"kind": "stage_provenance", "stage": LINE_DECISION_STAGE},
        {"kind": "stage_provenance", "stage": RECORD_EXTRACTION_STAGE},
    ]
    with pytest.raises(ValueError, match="Stage-A3 extraction dependency"):
        validate_endorsement_line_ledger_private_rows({}, legacy_rows)


def test_bundle_fails_before_creating_output_when_a3_results_are_absent(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "bundle"
    absent = tmp_path / "absent.json"
    with pytest.raises(ValueError, match="human results.*absent"):
        build_bundle(
            tmp_path / "unused_export",
            tmp_path / "unused_hive_output",
            output_dir,
            endorsement_stage_a_export_dir=tmp_path / "unused_a_export",
            endorsement_stage_a_output_dir=tmp_path / "unused_a_output",
            endorsement_stage_a2_export_dir=tmp_path / "unused_a2_export",
            endorsement_stage_a2_output_dir=tmp_path / "unused_a2_output",
            endorsement_stage_a3_task_json=tmp_path / "unused_task.json",
            endorsement_stage_a3_task_html=tmp_path / "unused_task.html",
            endorsement_stage_a2_evaluation_report=tmp_path / "unused_eval.json",
            endorsement_stage_a3_results_json=absent,
            endorsement_stage_a3_imported_artifact_json=(
                tmp_path / "unused_import.json"
            ),
            endorsement_stage_b_export_dir=tmp_path / "unused_b_export",
            endorsement_stage_b_output_dir=tmp_path / "unused_b_output",
        )
    assert not output_dir.exists()


def test_three_stage_private_bundle_replays_real_a1_and_rejects_a2_tampering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    plumbing = runpy.run_path(str(
        Path(__file__).with_name(
            "test_candidate_endorsement_line_adjudication_plumbing.py"
        )
    ))
    if not plumbing["HAS_FROZEN_INPUTS"]:
        pytest.skip("exact suffix-b A1 and suffix-p source are unavailable")

    materialize_stage_a2 = plumbing["_materialize_valid_stage_a2_run"]
    original_stage_a2_builder = materialize_stage_a2.__globals__[
        "build_line_adjudication_export"
    ]
    def build_frozen_stage_a2(*args, **kwargs):
        kwargs["chunk_size"] = FROZEN_STAGE_A2_CHUNK_SIZE
        return original_stage_a2_builder(*args, **kwargs)

    materialize_stage_a2.__globals__["build_line_adjudication_export"] = (
        build_frozen_stage_a2
    )
    stage_a2_export, stage_a2_output = materialize_stage_a2(tmp_path)
    # The line-ledger contract embeds the Stage-A3 authentication, so
    # completing that human adjudication advanced the contract.  The Stage-A2
    # artifacts were built before it and embed the earlier contract, and
    # Stage A3 derives from Stage A2, so regeneration cannot reproduce the
    # pre-adjudication bytes.  The reviewed artifact on disk is therefore the
    # authority, and this test authenticates it rather than rebuilding it.
    frozen_stage_a2_manifest = (
        Path(__file__).resolve().parents[1]
        / "build/hive_candidate_extraction"
        / "v2_1_endorsement_line_ledger_stage_a2_preflight_20260825f"
        / "manifest.json"
    )
    if not frozen_stage_a2_manifest.is_file():
        pytest.skip("the reviewed Stage-A2 artifact is unavailable")
    assert _sha256_file(frozen_stage_a2_manifest) == (
        FROZEN_STAGE_A2_INPUT_MANIFEST_SHA256
    )
    regenerated = _sha256_file(stage_a2_export / "manifest.json")
    assert regenerated != FROZEN_STAGE_A2_INPUT_MANIFEST_SHA256, (
        "regeneration now matches the pre-adjudication bytes; if Stage A3 was "
        "reverted, restore the original regeneration assertion"
    )
    stage_b_export = tmp_path / "stage_b_export"
    stage_b_manifest = plumbing[
        "build_adjudicated_record_extraction_export"
    ](
        plumbing["DEFAULT_FULL_EXPORT_DIR"],
        plumbing["STAGE_A_EXPORT"],
        plumbing["STAGE_A_OUTPUT"],
        stage_a2_export,
        stage_a2_output,
        stage_b_export,
    )
    stage_b_manifest = plumbing["_mark_test_manifest_frozen"](
        stage_b_export / "manifest.json", stage_b_manifest
    )
    contract = get_line_ledger_contract()
    manifest_sha256 = _sha256_file(stage_b_export / "manifest.json")
    args = SimpleNamespace(
        stage=RECORD_EXTRACTION_STAGE,
        seed=LINE_LEDGER_GENERATION_SEED,
        max_model_len=32_768,
        max_output_tokens=LINE_LEDGER_MAX_OUTPUT_TOKENS[
            RECORD_EXTRACTION_STAGE
        ],
        shard_index=None,
        shard_count=None,
    )
    generation_hash = line_ledger_generation_config_sha256(
        args, contract, input_manifest_sha256=manifest_sha256
    )
    input_paths = sorted((stage_b_export / "chunks").glob("*.jsonl.gz"))
    batches = [
        row for path in input_paths
        for row in plumbing["read_jsonl_gz"](path)
    ]
    output_rows: list[dict[str, object]] = []
    stage_b_output = tmp_path / "stage_b_output"
    stage_b_output.mkdir()
    for input_path in input_paths:
        chunk_rows = []
        for batch in plumbing["read_jsonl_gz"](input_path):
            # The line-ledger Stage-B slot carries only "records", and the
            # contract requires exactly one record for each single anchor.
            # The anchor line text is the exact source mention, so it is the
            # only name that satisfies the deterministic exactness check.
            target_by_slot = {
                str(t["slot"]): t for t in batch["targets"]
            }
            model_output = {}
            for index in range(RECORD_EXTRACTION_SLOT_COUNT):
                slot = f"slot_{index:02d}"
                target = target_by_slot.get(slot)
                records = []
                if target is not None:
                    # A compound anchor needs one record for each coordinated
                    # identity. An anchor whose proper name continues onto the
                    # next owned line must be accounted for in full.
                    compound = (
                        _compound_anchor_expected_names(
                            str(target["line_text"])
                        )
                        if target.get("stage_a_role")
                        == "compound_record_anchor"
                        else None
                    )
                    if compound:
                        names = list(compound)
                    else:
                        split_name = _required_split_line_name(target)
                        names = [
                            split_name if split_name is not None
                            else str(target["line_text"]).strip()
                        ]
                    records = [{
                        "endorser_name": name,
                        "entity_type": "organization",
                        "endorser_category": "other_organization",
                        "title": None,
                        "affiliation": None,
                        "jurisdiction": None,
                    } for name in names]
                model_output[slot] = {"records": records}
            row = derive_line_ledger_output_row(
                batch=batch,
                raw_output=json.dumps(model_output),
                finish_reason="stop",
                stop_reason=None,
                output_tokens=100,
                prompt_tokens=1_000,
                args=args,
                contract=contract,
                input_manifest_sha256=manifest_sha256,
                generation_hash=generation_hash,
            )
            assert row["technical_pass"] is True
            output_rows.append(row)
            chunk_rows.append(row)
        plumbing["write_jsonl_gz"](
            stage_b_output / input_path.name.replace(
                ".jsonl.gz", ".out.jsonl.gz"
            ),
            chunk_rows,
        )
    metadata = plumbing["_run_metadata"](
        args=args,
        contract=contract,
        generation_hash=generation_hash,
        input_manifest_sha256=manifest_sha256,
        selected=len(batches),
        selected_variant_inventory=line_ledger_schema_variant_inventory(
            batches
        ),
        selected_stage_a_provenance_sha256=[],
        rows=output_rows,
        started="2026-08-25T00:00:00+00:00",
        recovery=False,
    )
    (stage_b_output / "run_metadata.json").write_text(
        json.dumps(metadata, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    _, all_inputs = load_inputs(plumbing["DEFAULT_FULL_EXPORT_DIR"])
    pilot_parent_ids = set(plumbing["PILOT_PARENT_IDS"])
    inputs = {
        key: value for key, value in all_inputs.items()
        if value["route"] == "endorsements"
        and value["parent_document_id"] in pilot_parent_ids
    }
    monkeypatch.setattr(
        importer,
        "FROZEN_LINE_LEDGER_CONTRACT_SHA256",
        contract["line_ledger_contract_sha256"],
    )
    absent = tmp_path / "human_results_absent.json"
    with pytest.raises(
        ValueError, match="human results.*absent.*Stage B is forbidden"
    ):
        load_endorsement_line_ledger_runs(
            inputs,
            plumbing["STAGE_A_EXPORT"],
            plumbing["STAGE_A_OUTPUT"],
            stage_a2_export,
            stage_a2_output,
            Path(
                "build/candidate_endorsement_line_ledger_a3_handcode_21447139/"
                "task.json"
            ),
            Path(
                "build/candidate_endorsement_line_ledger_a3_handcode_21447139/"
                "candidate_endorsement_line_ledger_human_a3_21447139.html"
            ),
            Path(
                "quality_reports/"
                "candidate_endorsement_line_ledger_stage_a2_preflight_"
                "21447139_2026-08-25.json"
            ),
            absent,
            tmp_path / "imported_a3_absent.json",
            stage_b_export,
            stage_b_output,
        )


def test_line_ledger_import_authenticates_actual_compressed_transport(
    tmp_path: Path,
) -> None:
    export_dir = tmp_path / "export"
    chunks_dir = export_dir / "chunks"
    output_dir = tmp_path / "output"
    chunks_dir.mkdir(parents=True)
    output_dir.mkdir()
    input_path = chunks_dir / "chunk_00000.jsonl.gz"
    output_path = output_dir / "chunk_00000.out.jsonl.gz"
    with gzip.open(input_path, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps({"batch_id": "a"}) + "\n")
    with gzip.open(output_path, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps({"batch_id": "a", "technical_pass": True}) + "\n")
    manifest = {
        "files": {
            "chunks": [{
                "file": input_path.name,
                "records": 1,
                "bytes": input_path.stat().st_size,
                "sha256": _sha256_file(input_path),
            }],
        },
    }
    artifacts = {
        "output_files": [{
            "file": output_path.name,
            "records": 1,
            "bytes": output_path.stat().st_size,
            "sha256": _sha256_file(output_path),
        }],
    }
    inputs, outputs = _attest_line_ledger_transport_files(
        manifest, artifacts, export_dir, output_dir
    )
    assert inputs[0]["canonical_jsonl_sha256"]
    assert outputs[0]["canonical_jsonl_sha256"]

    bad_input = deepcopy(manifest)
    bad_input["files"]["chunks"][0]["bytes"] += 1
    with pytest.raises(ValueError, match="input compressed bytes differ"):
        _attest_line_ledger_transport_files(
            bad_input, artifacts, export_dir, output_dir
        )

    bad_output = deepcopy(artifacts)
    bad_output["output_files"][0]["sha256"] = "f" * 64
    with pytest.raises(ValueError, match="output compressed bytes differ"):
        _attest_line_ledger_transport_files(
            manifest, bad_output, export_dir, output_dir
        )


def test_stage_a3_dependency_replays_synthetic_results_and_rejects_tampering(
    tmp_path: Path,
) -> None:
    repo = Path(__file__).resolve().parents[1]
    stage_a_export = repo / (
        "build/hive_candidate_extraction/"
        "v2_1_endorsement_line_ledger_stage_a_preflight_20260825b"
    )
    stage_a_output = repo / (
        "build/hive_candidate_extraction/"
        "v2_1_endorsement_line_ledger_stage_a_preflight_"
        "hive_output_20260825b_21437036"
    )
    stage_a2_export = repo / (
        "build/hive_candidate_extraction/"
        "v2_1_endorsement_line_ledger_stage_a2_preflight_20260825f"
    )
    stage_a2_output = repo / (
        "build/hive_candidate_extraction/"
        "v2_1_endorsement_line_ledger_stage_a2_preflight_"
        "hive_output_20260825f_21447139"
    )
    full_export = repo / (
        "build/hive_candidate_extraction/v2_1_full_export_20260824p"
    )
    task_dir = repo / (
        "build/candidate_endorsement_line_ledger_a3_handcode_21447139"
    )
    task_json = task_dir / "task.json"
    task_html = task_dir / (
        "candidate_endorsement_line_ledger_human_a3_21447139.html"
    )
    evaluation_report = repo / (
        "quality_reports/candidate_endorsement_line_ledger_stage_a2_"
        "preflight_21447139_2026-08-25.json"
    )
    required = (
        stage_a_export, stage_a_output, stage_a2_export, stage_a2_output,
        full_export, task_json, task_html, evaluation_report,
    )
    if not all(path.exists() for path in required):
        pytest.skip("exact A1/A2/A3 dependency fixtures are unavailable")
    # The Stage-A3 human adjudication is complete, and the importer pins its
    # exact reviewed bytes.  Synthetic results can no longer import, so this
    # test replays the real frozen results file.
    results_json = task_dir / (
        "candidate_endorsement_line_ledger_human_a3_21447139_results.json"
    )
    if not results_json.exists():
        pytest.skip("the reviewed Stage-A3 results file is unavailable")
    assert _sha256_file(results_json) == (
        "f0a13ba66a617f46621d48109865d2ff88c6ba068a2e329d05093152958b74db"
    )
    imported_artifact = tmp_path / "synthetic_a3_imported.json"
    import_a3_results(SimpleNamespace(
        task_json=task_json,
        expected_task_sha256=importer.FROZEN_STAGE_A3_TASK_SHA256,
        results_json=results_json,
        stage_a_export_dir=stage_a_export,
        stage_a_output_dir=stage_a_output,
        stage_a2_export_dir=stage_a2_export,
        stage_a2_output_dir=stage_a2_output,
        stage_a2_evaluation_report=evaluation_report,
        full_export_dir=full_export,
        output_json=imported_artifact,
    ))
    row = _stage_a3_private_row(
        task_json=task_json,
        task_html=task_html,
        stage_a2_evaluation_report=evaluation_report,
        results_json=results_json,
        imported_artifact_json=imported_artifact,
    )
    replayed = replay_stage_a2_merge(
        stage_a_export,
        stage_a_output,
        stage_a2_export,
        stage_a2_output,
        full_export,
    )
    merged, summary = _validate_stage_a3_private_row(
        row,
        parents=replayed["parents"],
        stage_a_manifest=replayed["stage_a_manifest"],
        stage_a_provenance=replayed["stage_a_artifacts"],
        stage_a_decisions=replayed["stage_a_decisions"],
        stage_a2_manifest_text=(stage_a2_export / "manifest.json").read_text(
            encoding="utf-8"
        ),
        stage_a2_artifacts=replayed["stage_a2_artifacts"],
        stage_a2_batches=replayed["stage_a2_batches"],
        stage_a2_decisions=replayed["stage_a2_decisions"],
        stage_a2_merge=replayed["stage_a2_merge"],
    )
    assert summary["residual_count"] == 14
    assert summary["stage_b_authorized"] is True
    assert merged["validation"]["validation_passed"] is True

    def replace_json_bytes(
        source: dict[str, object], field: str, mutate: object
    ) -> dict[str, object]:
        attacked = deepcopy(source)
        value = json.loads(base64.b64decode(attacked[field]).decode("utf-8"))
        mutate(value)
        attacked[field] = base64.b64encode(
            json.dumps(value).encode("utf-8")
        ).decode("ascii")
        return attacked

    attacks = []
    html_attack = deepcopy(row)
    html_attack["html_bytes_base64"] = base64.b64encode(
        base64.b64decode(html_attack["html_bytes_base64"]) + b"\n"
    ).decode("ascii")
    attacks.append((html_attack, "HTML bytes"))
    evaluation_attack = replace_json_bytes(
        row,
        "evaluation_report_bytes_base64",
        lambda value: value.__setitem__("passed_checks", 999),
    )
    attacks.append((evaluation_attack, "evaluation report replay"))
    results_attack = replace_json_bytes(
        row,
        "results_bytes_base64",
        lambda value: value.__setitem__("n_complete", 0),
    )
    attacks.append((results_attack, "completion"))
    artifact_attack = replace_json_bytes(
        row,
        "imported_artifact_bytes_base64",
        lambda value: value.__setitem__("stage_b_authorized", False),
    )
    attacks.append((artifact_attack, "imported artifact replay"))
    for attacked, message in attacks:
        with pytest.raises(ValueError, match=message):
            _validate_stage_a3_private_row(
                attacked,
                parents=replayed["parents"],
                stage_a_manifest=replayed["stage_a_manifest"],
                stage_a_provenance=replayed["stage_a_artifacts"],
                stage_a_decisions=replayed["stage_a_decisions"],
                stage_a2_manifest_text=(
                    stage_a2_export / "manifest.json"
                ).read_text(encoding="utf-8"),
                stage_a2_artifacts=replayed["stage_a2_artifacts"],
                stage_a2_batches=replayed["stage_a2_batches"],
                stage_a2_decisions=replayed["stage_a2_decisions"],
                stage_a2_merge=replayed["stage_a2_merge"],
            )

    for source, label, message in (
        (task_json, "task.json", "task bytes"),
        (task_html, "task.html", "HTML bytes"),
        (evaluation_report, "evaluation.json", "provenance replay"),
    ):
        attacked_path = tmp_path / label
        attacked_path.write_bytes(source.read_bytes().replace(b"\n", b"\r\n"))
        paths = {
            "task_json": task_json,
            "task_html": task_html,
            "stage_a2_evaluation_report": evaluation_report,
            "results_json": results_json,
            "imported_artifact_json": imported_artifact,
        }
        if label == "task.json":
            paths["task_json"] = attacked_path
        elif label == "task.html":
            paths["task_html"] = attacked_path
        else:
            paths["stage_a2_evaluation_report"] = attacked_path
        if label == "evaluation.json":
            attacked_row = _stage_a3_private_row(**paths)
            with pytest.raises(ValueError, match=message):
                _validate_stage_a3_private_row(
                    attacked_row,
                    parents=replayed["parents"],
                    stage_a_manifest=replayed["stage_a_manifest"],
                    stage_a_provenance=replayed["stage_a_artifacts"],
                    stage_a_decisions=replayed["stage_a_decisions"],
                    stage_a2_manifest_text=(
                        stage_a2_export / "manifest.json"
                    ).read_text(encoding="utf-8"),
                    stage_a2_artifacts=replayed["stage_a2_artifacts"],
                    stage_a2_batches=replayed["stage_a2_batches"],
                    stage_a2_decisions=replayed["stage_a2_decisions"],
                    stage_a2_merge=replayed["stage_a2_merge"],
                )
        else:
            with pytest.raises(ValueError, match=message):
                _stage_a3_private_row(**paths)


def test_run_metadata_aggregate_uses_max_across_shards() -> None:
    def shard(max_tokens: int, total_tokens: int) -> dict:
        return {
            **{field: 0 for field in RUN_METADATA_SCALAR_FIELDS},
            **{field: {} for field in RUN_METADATA_MAPPING_FIELDS},
            **{
                field: {route: 0 for route in CONTRACT["route_array_item_caps"]}
                for field in RUN_METADATA_MAX_MAPPING_FIELDS
            },
            "recovery_attestation_without_inference": False,
            "documents_selected_for_shard": 1,
            "documents_processed_this_run": 1,
            "documents_parsed_this_run": 1,
            "documents_schema_valid_this_run": 1,
            "documents_exactly_supported_this_run": 1,
            "output_tokens_total_this_run": total_tokens,
            "output_tokens_max_this_run": max_tokens,
        }

    aggregate = run_metadata_aggregate([
        shard(12_800, 651_213),
        shard(18_867, 779_278),
        shard(12_739, 681_383),
        shard(8_819, 487_057),
    ])

    assert aggregate["output_tokens_total_this_run"] == 2_598_931
    assert aggregate["output_tokens_max_this_run"] == 18_867


def _zero_run_metadata(*, recovery: bool, selected: int = 0) -> dict:
    return {
        **{field: 0 for field in RUN_METADATA_SCALAR_FIELDS},
        **{field: {} for field in RUN_METADATA_MAPPING_FIELDS},
        **{
            field: {route: 0 for route in CONTRACT["route_array_item_caps"]}
            for field in RUN_METADATA_MAX_MAPPING_FIELDS
        },
        "documents_selected_for_shard": selected,
        "recovery_attestation_without_inference": recovery,
    }


def test_zero_attempt_counters_require_recovery_attestation() -> None:
    with pytest.raises(ValueError, match="require a recovery attestation"):
        validate_run_metadata_accounting(
            _zero_run_metadata(recovery=False, selected=4), "metadata"
        )
    validate_run_metadata_accounting(
        _zero_run_metadata(recovery=True, selected=4), "metadata"
    )


def test_attempt_metadata_cannot_exceed_replayed_output_ledger() -> None:
    all_rows = all_output_aggregate([])
    aggregate = run_metadata_aggregate([
        _zero_run_metadata(recovery=True, selected=0)
    ])
    validate_run_metadata_against_all_output(aggregate, all_rows)

    scalar = deepcopy(aggregate)
    scalar["documents_processed_this_run"] = 1
    with pytest.raises(ValueError, match="processed.*exceeds"):
        validate_run_metadata_against_all_output(scalar, all_rows)

    mapping = deepcopy(aggregate)
    mapping["route_model_item_count_max_this_run"]["issues"] = 1
    with pytest.raises(ValueError, match="route_model_item_count_max.*exceeds"):
        validate_run_metadata_against_all_output(mapping, all_rows)


def test_import_accepts_complete_schema_1_4_context_manifest(tmp_path: Path) -> None:
    source = _source("segment-1", "She is a teacher.")
    chunks = tmp_path / "chunks"
    chunks.mkdir()
    chunk = chunks / "chunk_00000.jsonl.gz"
    with gzip.open(chunk, "wt", encoding="utf-8") as handle:
        handle.write(json.dumps(source) + "\n")
    context_provenance = {
        "version": ENDORSEMENT_CONTEXT_PROVENANCE_VERSION,
        "provenance_spec_sha256": ENDORSEMENT_CONTEXT_PROVENANCE_SPEC_SHA256,
        "url_classifier_id": ENDORSEMENT_CONTEXT_CLASSIFIER_ID,
        "url_classifier_spec_sha256": ENDORSEMENT_CONTEXT_CLASSIFIER_SPEC_SHA256,
        "structural_heading_spec_sha256": (
            ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC_SHA256
        ),
        "inference_display_escape_id": CONTRACT[
            "inference_display_escape_id"
        ],
        "inference_display_escape_spec_sha256": CONTRACT[
            "inference_display_escape_spec_sha256"
        ],
        "scope_preference": [
            "segment", "parent_document", "page_or_section_route_context",
        ],
        "segment_complete_line_census": dict(
            FROZEN_SUFFIX_P_COMPLETE_LINE_CENSUS
        ),
    }
    manifest = {
        "schema_version": "1.4",
        "source_corpus_sha256": FROZEN_SUFFIX_P_SOURCE_CORPUS_SHA256,
        "source_export_manifest_sha256": (
            FROZEN_SUFFIX_P_FULL_SOURCE_MANIFEST_SHA256
        ),
        "source_text_transform": {
            "version": SOURCE_TEXT_TRANSFORM_VERSION,
            "rule": SOURCE_TEXT_TRANSFORM_RULE,
            "offset_coordinate_system": OFFSET_COORDINATE_SYSTEM,
            "transformed_exported_parent_documents": 0,
        },
        "endorsement_context_provenance": context_provenance,
        "exported_records": 1,
        "chunks": [{
            "file": chunk.name,
            "records": 1,
            "sha256": _sha256_file(chunk),
        }],
    }
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    loaded_manifest, loaded_rows = load_inputs(tmp_path)
    assert loaded_manifest["endorsement_context_provenance"] == context_provenance
    assert set(loaded_rows) == {"segment-1"}

    assert loaded_manifest["source_export_manifest_sha256"] == (
        FROZEN_SUFFIX_P_FULL_SOURCE_MANIFEST_SHA256
    )

    bad_inherited = deepcopy(manifest)
    bad_inherited["source_export_manifest_sha256"] = "f" * 64
    manifest_path.write_text(json.dumps(bad_inherited), encoding="utf-8")
    with pytest.raises(ValueError, match="does not attest"):
        load_inputs(tmp_path)

    del manifest["endorsement_context_provenance"]["provenance_spec_sha256"]
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(ValueError, match="endorsement-context provenance"):
        load_inputs(tmp_path)


def test_import_rejects_tampered_complete_line_census() -> None:
    source = _source("segment-1", "She is a teacher.")
    context = {
        "version": ENDORSEMENT_CONTEXT_PROVENANCE_VERSION,
        "provenance_spec_sha256": ENDORSEMENT_CONTEXT_PROVENANCE_SPEC_SHA256,
        "url_classifier_id": ENDORSEMENT_CONTEXT_CLASSIFIER_ID,
        "url_classifier_spec_sha256": ENDORSEMENT_CONTEXT_CLASSIFIER_SPEC_SHA256,
        "structural_heading_spec_sha256": (
            ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC_SHA256
        ),
        "inference_display_escape_id": CONTRACT[
            "inference_display_escape_id"
        ],
        "inference_display_escape_spec_sha256": CONTRACT[
            "inference_display_escape_spec_sha256"
        ],
        "scope_preference": [
            "segment", "parent_document", "page_or_section_route_context",
        ],
        "segment_complete_line_census": {
            "endorsement_segments": 1,
            "segments_with_complete_line": 1,
            "segments_without_complete_line": 0,
            "first_source_display_boundary_differs": 0,
            "last_source_display_boundary_differs": 0,
        },
    }
    with pytest.raises(ValueError, match="census differs from rows"):
        validate_endorsement_context_manifest(context, [source])


def test_source_export_identity_is_frozen() -> None:
    direct = {"source_corpus_sha256": FROZEN_SUFFIX_P_SOURCE_CORPUS_SHA256}
    validate_source_export_identity(
        direct, FROZEN_SUFFIX_P_FULL_SOURCE_MANIFEST_SHA256
    )

    bad_corpus = deepcopy(direct)
    bad_corpus["source_corpus_sha256"] = "f" * 64
    with pytest.raises(ValueError, match="corpus SHA-256"):
        validate_source_export_identity(
            bad_corpus, FROZEN_SUFFIX_P_FULL_SOURCE_MANIFEST_SHA256
        )

    with pytest.raises(ValueError, match="frozen suffix-p full export"):
        validate_source_export_identity(direct, "f" * 64)

    derived = {
        **direct,
        "source_export_manifest_sha256": (
            FROZEN_SUFFIX_P_FULL_SOURCE_MANIFEST_SHA256
        ),
    }
    validate_source_export_identity(derived, "f" * 64)
    derived["source_export_manifest_sha256"] = "f" * 64
    with pytest.raises(ValueError, match="does not attest"):
        validate_source_export_identity(derived, "f" * 64)


REAL_SUFFIX_P_PREFLIGHT_EXPORT = (
    Path(__file__).resolve().parents[1]
    / "build/hive_candidate_extraction/v2_1_quote_cap_preflight_export_20260824p"
)


@pytest.mark.skipif(
    not (REAL_SUFFIX_P_PREFLIGHT_EXPORT / "manifest.json").is_file(),
    reason="immutable local suffix-p artifact is absent",
)
def test_import_accepts_real_suffix_p_preflight_manifest() -> None:
    manifest, rows = load_inputs(REAL_SUFFIX_P_PREFLIGHT_EXPORT)
    assert len(rows) == 44
    assert manifest["source_export_manifest_sha256"] == (
        "f5e6545a2d2ec35a83f4184e0d328635e5fba9d3c80cd4c8a7466178b7d9172c"
    )
    assert manifest["endorsement_context_provenance"][
        "segment_complete_line_census"
    ] == {
        "endorsement_segments": 6_096,
        "segments_with_complete_line": 6_055,
        "segments_without_complete_line": 41,
        "first_source_display_boundary_differs": 169,
        "last_source_display_boundary_differs": 988,
    }
    assert endorsement_segment_complete_line_census(rows.values()) == {
        "endorsement_segments": 15,
        "segments_with_complete_line": 15,
        "segments_without_complete_line": 0,
        "first_source_display_boundary_differs": 1,
        "last_source_display_boundary_differs": 6,
    }


def test_run_metadata_selected_counts_bind_to_exact_source_shards() -> None:
    source_manifest = {
        "chunks": [
            {"file": "chunk_00000.jsonl.gz", "records": 3},
            {"file": "chunk_00001.jsonl.gz", "records": 5},
            {"file": "chunk_00002.jsonl.gz", "records": 7},
        ]
    }
    metadata = [
        {
            "metadata_file": "run_metadata_00000.json",
            "shard_index": 0,
            "shard_count": 2,
            "documents_selected_for_shard": 10,
        },
        {
            "metadata_file": "run_metadata_00001.json",
            "shard_index": 1,
            "shard_count": 2,
            "documents_selected_for_shard": 5,
        },
    ]
    validate_run_metadata_source_coverage(source_manifest, metadata)
    metadata[0]["documents_selected_for_shard"] = 9
    with pytest.raises(ValueError, match="source chunks"):
        validate_run_metadata_source_coverage(source_manifest, metadata)


def _source(document_id: str, text: str, *, start: int = 0, count: int = 1) -> dict:
    import hashlib

    raw_hash = hashlib.sha256(text.encode()).hexdigest()
    return {
        "document_id": document_id,
        "parent_document_id": "parent-1",
        "candidate_cycle_id": "H0AA00001-2020",
        "candidate_icpsr": "Example, Candidate",
        "state": "AA",
        "office": "house",
        "year": 2020,
        "party": "other",
        "route": "biography",
        "raw_text_sha256": raw_hash,
        "extraction_source_text_sha256": raw_hash,
        "extraction_source_n_chars": len(text),
        "source_text_transform_version": "candidate_extraction_source_text_v1",
        "source_text_transform_changed": False,
        "segment_index": 0,
        "segment_count": count,
        "segment_start_char": start,
        "segment_end_char": start + len(text),
        "segment_text_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "source_text": text,
        "n_raw_occurrences": 1,
        "first_capture_timestamp": "20200101000000",
        "last_capture_timestamp": "20200101000000",
        "source_urls": ["https://web.archive.org/example"],
    }


def _bio_item(text: str, value: str = "teacher", timing: str = "current") -> dict:
    start = text.index("teacher")
    return {
        "field": "occupation",
        "value": value,
        "timing": timing,
        "relationship": None,
        "evidence": "teacher",
        "evidence_start_char": start,
        "evidence_end_char": start + len("teacher"),
    }


def _model_output(source: dict, model_extraction: dict) -> dict:
    route = str(source["route"])
    context = source.get("endorsement_context_provenance")
    extraction, withheld, alignment = align_extraction_exact_spans(
        route,
        model_extraction,
        source["source_text"],
        endorsement_context_provenance=context,
        segment_start_char=int(source["segment_start_char"]),
    )
    raw_output = json.dumps(model_extraction, ensure_ascii=False)
    _, output_report = canonicalize_json_string_controls(raw_output)
    display_provenance = inference_display_provenance(
        source["source_text"]
    )
    structural_whitespace = validate_raw_structural_whitespace(raw_output)
    guard = pre_alignment_model_item_guard_report(route, model_extraction)
    assert guard["pre_alignment_model_item_guard_validation_passed"] is True
    output = {
        **{key: source[key] for key in (
            "document_id", "parent_document_id", "candidate_cycle_id", "route",
            "raw_text_sha256", "segment_index", "segment_count",
            "segment_start_char", "segment_end_char", "segment_text_sha256",
        )},
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
        "schema_version": "2.1",
        "prompt_sha256": CONTRACT["prompt_sha256"][route],
        "json_schema_sha256": CONTRACT["json_schema_sha256"][route],
        "extraction_contract_sha256": CONTRACT["extraction_contract_sha256"],
        "alignment_id": CONTRACT["alignment_id"],
        "alignment_spec_sha256": CONTRACT["alignment_spec_sha256"],
        "semantic_validation_spec_sha256": CONTRACT["semantic_validation_spec_sha256"],
        "endorsement_context_provenance_spec_sha256": CONTRACT[
            "endorsement_context_provenance_spec_sha256"
        ],
        "user_message_template_id": CONTRACT["user_message_template_id"],
        "user_message_template_sha256": CONTRACT["user_message_template_sha256"],
        "user_message_sha256": hashlib.sha256(
            build_user_message(
                source, source["source_text"], "2.1"
            ).encode("utf-8")
        ).hexdigest(),
        **display_provenance,
        "raw_output": raw_output,
        **output_report,
        **structural_whitespace,
        "array_item_cap_spec_id": CONTRACT["array_item_cap_spec_id"],
        "array_item_cap_spec_sha256": CONTRACT["array_item_cap_spec_sha256"],
        "route_array_item_cap": CONTRACT["route_array_item_caps"][route],
        "model_item_repetition_spec_id": CONTRACT[
            "model_item_repetition_spec_id"
        ],
        "model_item_repetition_spec_sha256": CONTRACT[
            "model_item_repetition_spec_sha256"
        ],
        "generation_config_sha256": EXPECTED_GENERATION_CONFIG_SHA256,
        "structured_decoding_backend": STRUCTURED_DECODING_BACKEND,
        "structured_decoding_disable_any_whitespace": (
            STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
        ),
        "temperature": 0.0,
        "thinking_enabled": False,
        "compact_repair": False,
        "input_truncated": False,
        "input_chars_used": len(source["source_text"]),
        "input_tokens_used": 4,
        "max_output_tokens": EXPECTED_MAX_OUTPUT_TOKENS,
        "finish_reason": "stop",
        "stop_reason": None,
        "output_tokens": 100,
        "parse_status": "parsed",
        "model_extraction": model_extraction,
        "model_extraction_sha256": alignment["original_model_extraction_sha256"],
        "pre_alignment_model_item_guard": guard,
        "array_item_cap_report": guard["array_item_cap_report"],
        "model_item_repetition_report": guard["model_item_repetition_report"],
        "extraction": extraction,
        "aligned_extraction_sha256": alignment["aligned_extraction_sha256"],
        "withheld_model_items": withheld,
        "exact_alignment": alignment,
        "validation": validate_extraction(
            route,
            extraction,
            source["source_text"],
            "2.1",
            endorsement_context_provenance=context,
            segment_start_char=int(source["segment_start_char"]),
            parent_source_text=source["source_text"],
        ),
    }
    if context is not None:
        output["endorsement_context_provenance"] = context
    return output


def _replace_guard_payload(output: dict, route: str, parsed: dict) -> None:
    guard = pre_alignment_model_item_guard_report(route, parsed)
    output["pre_alignment_model_item_guard"] = guard
    output["array_item_cap_report"] = guard["array_item_cap_report"]
    output["model_item_repetition_report"] = guard[
        "model_item_repetition_report"
    ]


def _guard_failure_output(
    source: dict, model_extraction: dict, expected_status: str
) -> dict:
    output = _model_output(
        source,
        {"facts": [_bio_item(source["source_text"])]},
    )
    raw_output = json.dumps(model_extraction, ensure_ascii=False)
    _, output_report = canonicalize_json_string_controls(raw_output)
    output.update(output_report)
    output.update(validate_raw_structural_whitespace(raw_output))
    output["raw_output"] = raw_output
    output["model_extraction"] = model_extraction
    output["model_extraction_sha256"] = hashlib.sha256(
        canonical_json(model_extraction).encode("utf-8")
    ).hexdigest()
    _replace_guard_payload(output, "biography", model_extraction)
    assert output["pre_alignment_model_item_guard"]["failure_status"] == (
        expected_status
    )
    output["parse_status"] = expected_status
    output["extraction"] = None
    for field in (
        "aligned_extraction_sha256",
        "withheld_model_items",
        "exact_alignment",
        "validation",
    ):
        output.pop(field)
    return output


def test_guard_failures_are_private_technical_failures_and_never_aligned() -> None:
    source = _source("segment-1", "She is a teacher.")
    distinct_items = [
        {
            "field": "occupation",
            "value": f"teacher-{index}",
            "timing": "current",
            "relationship": None,
            "evidence": "teacher",
        }
        for index in range(CONTRACT["route_array_item_caps"]["biography"])
    ]
    cap_output = _guard_failure_output(
        source,
        {"facts": distinct_items},
        ARRAY_ITEM_CAP_REACHED_STATUS,
    )
    validation, status = validate_output_row(source, cap_output)
    assert validation["status"] == ARRAY_ITEM_CAP_REACHED_STATUS
    assert status == "failed_array_item_cap_reached"
    segments, _ = enrich_segments(
        {source["document_id"]: source},
        {source["document_id"]: cap_output},
    )
    segment = segments[0]
    assert segment["model_extraction"] == {"facts": distinct_items}
    assert segment["extraction"] is None
    assert segment["exact_alignment"] is None
    assert segment["resolved_records"] == []
    aggregate = all_output_aggregate(segments)
    assert aggregate["strict_json_parsed_outputs"] == 1
    assert aggregate["model_items_strictly_parsed"] == 96
    assert aggregate["array_item_cap_reached_outputs"] == 1
    assert aggregate["model_item_repetition_error_outputs"] == 0
    assert not technical_segment_gate_passes({status: 1}, 1)

    repeated = {
        "field": "occupation",
        "value": "teacher",
        "timing": "current",
        "relationship": None,
        "evidence": "teacher",
    }
    intervening = {**repeated, "value": "educator"}
    repetition_output = _guard_failure_output(
        source,
        {"facts": [repeated, intervening, repeated, repeated]},
        MODEL_ITEM_REPETITION_ERROR_STATUS,
    )
    _, repetition_status = validate_output_row(source, repetition_output)
    assert repetition_status == "failed_model_item_repetition_error"
    assert repetition_output["model_item_repetition_report"][
        "repeated_model_item_groups"
    ][0]["item_indices"] == [0, 2, 3]


def test_endorsement_schema_omission_retains_256_item_post_generation_failure() -> None:
    text = "Organizations\nAlice Example"
    source = _source("segment-endorsement-cap", text)
    source["route"] = "endorsements"
    source["source_urls"] = [
        "https://web.archive.org/web/20200101000000/"
        "https://example.org/supporters"
    ]
    source["endorsement_context_provenance"] = (
        build_endorsement_context_provenance(
            parent_document_id=source["parent_document_id"],
            parent_source_text=text,
            source_urls=source["source_urls"],
            segment_id=source["document_id"],
            segment_start_char=0,
            segment_end_char=len(text),
        )
    )
    valid_item = {
        "endorser_name": "Alice Example",
        "entity_type": "individual",
        "endorser_category": "private_individual",
        "title": None,
        "affiliation": None,
        "jurisdiction": None,
        "record_evidence": "Alice Example",
        "endorsement_context_evidence": None,
        "support_mode": "page_or_section_context",
    }
    output = _model_output(source, {"endorsements": [valid_item]})
    capped_extraction = {
        "endorsements": [
            {**valid_item, "endorser_name": f"Endorser {index}"}
            for index in range(CONTRACT["route_array_item_caps"]["endorsements"])
        ]
    }
    raw_output = json.dumps(capped_extraction, ensure_ascii=False)
    _, output_report = canonicalize_json_string_controls(raw_output)
    output.update(output_report)
    output.update(validate_raw_structural_whitespace(raw_output))
    output["raw_output"] = raw_output
    output["model_extraction"] = capped_extraction
    output["model_extraction_sha256"] = hashlib.sha256(
        canonical_json(capped_extraction).encode("utf-8")
    ).hexdigest()
    _replace_guard_payload(output, "endorsements", capped_extraction)
    assert output["array_item_cap_report"]["model_item_count"] == 256
    assert output["pre_alignment_model_item_guard"]["failure_status"] == (
        ARRAY_ITEM_CAP_REACHED_STATUS
    )
    output["parse_status"] = ARRAY_ITEM_CAP_REACHED_STATUS
    output["extraction"] = None
    for field in (
        "aligned_extraction_sha256",
        "withheld_model_items",
        "exact_alignment",
        "validation",
    ):
        output.pop(field)

    validation, status = validate_output_row(source, output)
    assert validation["status"] == ARRAY_ITEM_CAP_REACHED_STATUS
    assert status == "failed_array_item_cap_reached"
    segments, _ = enrich_segments(
        {source["document_id"]: source},
        {source["document_id"]: output},
    )
    assert segments[0]["extraction"] is None
    assert segments[0]["exact_alignment"] is None
    assert segments[0]["resolved_records"] == []


def test_cap_precedence_and_guard_tampering_fail_closed() -> None:
    source = _source("segment-1", "She is a teacher.")
    repeated = {
        "field": "occupation",
        "value": "teacher",
        "timing": "current",
        "relationship": None,
        "evidence": "teacher",
    }
    cap_output = _guard_failure_output(
        source,
        {
            "facts": [
                deepcopy(repeated)
                for _ in range(CONTRACT["route_array_item_caps"]["biography"])
            ]
        },
        ARRAY_ITEM_CAP_REACHED_STATUS,
    )
    assert cap_output["model_item_repetition_report"][
        "model_item_repetition_validation_passed"
    ] is False
    assert cap_output["pre_alignment_model_item_guard"]["failure_status"] == (
        ARRAY_ITEM_CAP_REACHED_STATUS
    )

    tampered_report = deepcopy(cap_output)
    tampered_report["array_item_cap_report"] = deepcopy(
        tampered_report["array_item_cap_report"]
    )
    tampered_report["array_item_cap_report"]["model_item_count"] -= 1
    with pytest.raises(ValueError, match="array_item_cap_report differs"):
        validate_output_row(source, tampered_report)

    tampered_hash = deepcopy(cap_output)
    tampered_hash["model_extraction_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="model extraction hash"):
        validate_output_row(source, tampered_hash)

    tampered_status = deepcopy(cap_output)
    tampered_status["parse_status"] = MODEL_ITEM_REPETITION_ERROR_STATUS
    with pytest.raises(ValueError, match="guard-failure status differs"):
        validate_output_row(source, tampered_status)

    leaked_alignment = deepcopy(cap_output)
    leaked_alignment["exact_alignment"] = {}
    with pytest.raises(ValueError, match="output fields differ"):
        validate_output_row(source, leaked_alignment)

    successful = _output(source, _bio_item(source["source_text"]))
    successful["pre_alignment_model_item_guard"][
        "pre_alignment_model_item_guard_validation_passed"
    ] = False
    with pytest.raises(ValueError, match="pre_alignment_model_item_guard differs"):
        validate_output_row(source, successful)


def test_importer_rejects_missing_or_weakened_structured_whitespace_policy() -> None:
    source = _source("segment-1", "She is a teacher.")
    output = _output(source, _bio_item(source["source_text"]))
    validate_output_row(source, output)

    missing = deepcopy(output)
    del missing["structured_decoding_backend"]
    with pytest.raises(ValueError, match="output fields differ"):
        validate_output_row(source, missing)

    weakened = deepcopy(output)
    weakened["structured_decoding_disable_any_whitespace"] = False
    with pytest.raises(ValueError, match="unexpected structured_decoding"):
        validate_output_row(source, weakened)

    integer_attestation = deepcopy(output)
    integer_attestation["structured_decoding_disable_any_whitespace"] = 1
    with pytest.raises(ValueError, match="unexpected structured_decoding"):
        validate_output_row(source, integer_attestation)


def test_importer_recomputes_fixed_separator_gate_from_raw_bytes() -> None:
    source = _source("segment-1", "She is a teacher.")
    output = _output(source, _bio_item(source["source_text"]))
    pretty = json.dumps(output["model_extraction"], ensure_ascii=False, indent=2)
    _, report = canonicalize_json_string_controls(pretty)
    output.update(report)
    output.update(validate_raw_structural_whitespace(pretty))
    output["raw_output"] = pretty
    with pytest.raises(ValueError, match="structural-whitespace failure status"):
        validate_output_row(source, output)

    failed = deepcopy(output)
    failed["parse_status"] = "raw_structural_whitespace_error"
    failed["extraction"] = None
    for field in (
        "model_extraction",
        "model_extraction_sha256",
        "pre_alignment_model_item_guard",
        "array_item_cap_report",
        "model_item_repetition_report",
        "aligned_extraction_sha256",
        "withheld_model_items",
        "exact_alignment",
        "validation",
    ):
        failed.pop(field)
    validation, status = validate_output_row(source, failed)
    assert validation["status"] == "raw_structural_whitespace_error"
    assert status == "failed_parse"
    summary = output_provenance_summary([
        {**source, **failed, "processing_status": status}
    ])
    assert summary["raw_structural_whitespace_validator_id"] == (
        RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID
    )
    assert summary["raw_structural_whitespace_validator_spec_sha256"] == (
        RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256
    )
    assert summary["raw_structural_whitespace_gate_passed"] is False
    assert summary["raw_structural_whitespace_invalid_outputs"] == 1
    assert summary["raw_structural_whitespace_violation_count"] > 0


def _different_typed_provenance_value(value):
    if isinstance(value, bool):
        return not value
    if isinstance(value, int):
        return value + 1
    if isinstance(value, str):
        return value + "-tampered"
    if isinstance(value, dict):
        return {**value, "tampered": 1}
    raise TypeError(f"unsupported provenance fixture type: {type(value)}")


def _display_provenance_fixture() -> tuple[dict, dict]:
    source = _source(
        "segment-display-provenance",
        'She is a "teacher".\tPath\\name',
    )
    return source, _output(source, _bio_item(source["source_text"]))


def test_importer_accepts_and_preserves_complete_runner_provenance() -> None:
    source, output = _display_provenance_fixture()
    validate_output_row(source, output)
    segments, _ = enrich_segments(
        {source["document_id"]: source},
        {source["document_id"]: output},
    )
    segment = segments[0]
    for field in (
        *OUTPUT_INFERENCE_DISPLAY_FIELDS,
        *OUTPUT_RAW_STRUCTURAL_WHITESPACE_FIELDS,
    ):
        assert segment[field] == output[field]
    assert segment["user_message_sha256"] == output["user_message_sha256"]


def test_importer_requires_exact_replayed_user_message_hash() -> None:
    source, output = _display_provenance_fixture()
    validate_output_row(source, output)

    missing = deepcopy(output)
    del missing["user_message_sha256"]
    with pytest.raises(ValueError, match="output fields differ"):
        validate_output_row(source, missing)

    tampered = deepcopy(output)
    tampered["user_message_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="user_message_sha256.*message replay"):
        validate_output_row(source, tampered)

    source_tampered = deepcopy(source)
    source_tampered["party"] = "tampered-party"
    with pytest.raises(ValueError, match="user_message_sha256.*message replay"):
        validate_output_row(source_tampered, output)


@pytest.mark.parametrize("field", OUTPUT_INFERENCE_DISPLAY_FIELDS)
def test_importer_replays_every_inference_display_field(field: str) -> None:
    source, output = _display_provenance_fixture()
    tampered = deepcopy(output)
    tampered[field] = _different_typed_provenance_value(tampered[field])
    with pytest.raises(ValueError, match=field):
        validate_output_row(source, tampered)


@pytest.mark.parametrize("field", OUTPUT_RAW_STRUCTURAL_WHITESPACE_FIELDS)
def test_importer_replays_every_raw_structural_field(field: str) -> None:
    source, output = _display_provenance_fixture()
    tampered = deepcopy(output)
    tampered[field] = _different_typed_provenance_value(tampered[field])
    with pytest.raises(ValueError, match=field):
        validate_output_row(source, tampered)


@pytest.mark.parametrize(
    "field",
    [
        "inference_display_roundtrip_valid",
        "raw_structural_whitespace_validation_passed",
    ],
)
def test_importer_provenance_comparison_is_type_strict(field: str) -> None:
    source, output = _display_provenance_fixture()
    assert output[field] is True
    tampered = deepcopy(output)
    tampered[field] = 1
    with pytest.raises(ValueError, match=field):
        validate_output_row(source, tampered)


def test_importer_runner_provenance_whitelist_rejects_missing_or_extra() -> None:
    source, output = _display_provenance_fixture()
    for field in (
        "inference_display_source_text_sha256",
        "raw_structural_whitespace_validator_id",
    ):
        missing = deepcopy(output)
        del missing[field]
        with pytest.raises(ValueError, match="output fields differ"):
            validate_output_row(source, missing)

    extra = deepcopy(output)
    extra["unexpected_runner_provenance"] = True
    with pytest.raises(ValueError, match="output fields differ"):
        validate_output_row(source, extra)


def test_complete_generation_config_validator_rejects_partial_or_mutated() -> None:
    valid = deepcopy(EXPECTED_GENERATION_CONFIG)
    assert validate_generation_config_identity(
        valid, EXPECTED_GENERATION_CONFIG_SHA256, "test"
    ) == valid

    partial = {"structured_decoding": deepcopy(valid["structured_decoding"])}
    with pytest.raises(ValueError, match="config hash differs"):
        validate_generation_config_identity(
            partial, EXPECTED_GENERATION_CONFIG_SHA256, "partial"
        )

    mutated = deepcopy(valid)
    mutated["seed"] += 1
    with pytest.raises(ValueError, match="config hash differs"):
        validate_generation_config_identity(
            mutated, EXPECTED_GENERATION_CONFIG_SHA256, "mutated"
        )


def _output(source: dict, item: dict) -> dict:
    model_item = {
        key: value for key, value in item.items()
        if key in {
            "field", "value", "timing", "relationship", "evidence"
        }
    }
    return _model_output(source, {"facts": [model_item]})


def test_exact_output_becomes_resolved_private_bundle_record() -> None:
    source = _source("segment-1", "She is a teacher.")
    output = _output(source, _bio_item(source["source_text"]))
    segments, parents = enrich_segments({"segment-1": source}, {"segment-1": output})
    assert segments[0]["processing_status"] == "processed_success"
    assert len(segments[0]["record_variants"]) == 1
    assert len(segments[0]["resolved_records"]) == 1
    record = segments[0]["resolved_records"][0]
    assert record["resolution_status"] == "single_variant"
    assert record["parent_offsets"]["evidence_parent_start_char"] == 9
    assert parents["parent-1"] == source["source_text"]


def test_url_route_endorsement_context_survives_import_without_fake_offsets() -> None:
    text = "Organizations\nAlice Example"
    source = _source("segment-endorsement", text)
    source["route"] = "endorsements"
    source["source_urls"] = [
        "https://web.archive.org/web/20200101000000/"
        "https://example.org/supporters"
    ]
    source["endorsement_context_provenance"] = (
        build_endorsement_context_provenance(
            parent_document_id=source["parent_document_id"],
            parent_source_text=text,
            source_urls=source["source_urls"],
            segment_id=source["document_id"],
            segment_start_char=0,
            segment_end_char=len(text),
        )
    )
    assert source["endorsement_context_provenance"]["source_scope"] == (
        "page_or_section_route_context"
    )
    output = _model_output(source, {"endorsements": [{
        "endorser_name": "Alice Example",
        "entity_type": "individual",
        "endorser_category": "private_individual",
        "title": None,
        "affiliation": None,
        "jurisdiction": None,
        "record_evidence": "Alice Example",
        "endorsement_context_evidence": None,
        "support_mode": "page_or_section_context",
    }]})
    segments, _ = enrich_segments(
        {source["document_id"]: source},
        {source["document_id"]: output},
    )
    assert segments[0]["processing_status"] == "processed_success"
    record = segments[0]["resolved_records"][0]
    assert record["item"]["endorsement_context_evidence"] is None
    assert record["item"]["endorsement_context_evidence_source_scope"] == (
        "page_or_section_route_context"
    )
    assert record["parent_offsets"][
        "endorsement_context_evidence_parent_start_char"
    ] is None
    assert record["item"]["endorsement_context_qualifying_original_path"] == (
        "/supporters"
    )


def test_nonexact_evidence_is_accounted_withheld_and_not_resolved() -> None:
    source = _source("segment-1", "She is a teacher.")
    item = _bio_item(source["source_text"])
    item["evidence"] = "Teacher"
    output = _output(source, item)
    segments, _ = enrich_segments({"segment-1": source}, {"segment-1": output})
    assert segments[0]["processing_status"] == "processed_all_withheld"
    assert segments[0]["withheld_model_items"][0]["reason"] == "missing_exact_span"
    assert segments[0]["resolved_records"] == []


def _blank_anchor_case(
    route: str,
    anchor_field: str,
    blank: str,
) -> tuple[dict, dict]:
    if route == "issues":
        text = "Jobs\nWe support good jobs."
        source = _source(f"segment-{route}-{anchor_field}", text)
        source["route"] = route
        item = {
            "source_heading": "Jobs",
            "claim": "The candidate supports good jobs.",
            "statement_kind": "support",
            "evidence": "We support good jobs.",
        }
        item[anchor_field] = blank
        return source, {"issue_blocks": [item]}
    if route == "biography":
        text = "She is a teacher."
        source = _source(f"segment-{route}-{anchor_field}", text)
        item = {
            "field": "occupation",
            "value": "teacher",
            "timing": "current",
            "relationship": None,
            "evidence": "teacher",
        }
        item[anchor_field] = blank
        return source, {"facts": [item]}

    text = "We are proud to endorse Jane Roe."
    source = _source(f"segment-{route}-{anchor_field}", text)
    source["route"] = route
    source["source_urls"] = [
        "https://web.archive.org/web/20200101000000/"
        "https://example.org/endorsements"
    ]
    source["endorsement_context_provenance"] = (
        build_endorsement_context_provenance(
            parent_document_id=source["parent_document_id"],
            parent_source_text=text,
            source_urls=source["source_urls"],
            segment_id=source["document_id"],
            segment_start_char=0,
            segment_end_char=len(text),
        )
    )
    item = {
        "endorser_name": "Jane Roe",
        "entity_type": "individual",
        "endorser_category": "private_individual",
        "title": None,
        "affiliation": None,
        "jurisdiction": None,
        "record_evidence": text,
        "endorsement_context_evidence": "endorse Jane Roe",
        "support_mode": "explicit_text",
    }
    item[anchor_field] = blank
    return source, {"endorsements": [item]}


@pytest.mark.parametrize(
    ("route", "anchor_field"),
    [
        ("issues", "source_heading"),
        ("issues", "evidence"),
        ("biography", "evidence"),
        ("endorsements", "record_evidence"),
        ("endorsements", "endorsement_context_evidence"),
    ],
)
@pytest.mark.parametrize("blank", ["", " ", "\t\n", "\u00a0", "\u2003"])
def test_blank_exact_anchor_cannot_enter_aligned_records_or_status(
    route: str,
    anchor_field: str,
    blank: str,
) -> None:
    source, model_extraction = _blank_anchor_case(
        route, anchor_field, blank
    )
    output = _model_output(source, model_extraction)
    list_key = {
        "issues": "issue_blocks",
        "biography": "facts",
        "endorsements": "endorsements",
    }[route]

    alignment = output["exact_alignment"]
    assert alignment["status"] == "invalid_schema"
    assert alignment["model_schema_valid"] is False
    assert alignment["aligned_item_count"] == 0
    assert alignment["withheld_item_count"] == 1
    assert output["withheld_model_items"][0]["reason"] == (
        "invalid_item_schema"
    )
    assert output["extraction"] == {list_key: []}

    _, status = validate_output_row(source, output)
    assert status == "failed_schema"
    segments, _ = enrich_segments(
        {source["document_id"]: source},
        {source["document_id"]: output},
    )
    assert segments[0]["processing_status"] == "failed_schema"
    assert segments[0]["record_variants"] == []
    assert segments[0]["resolved_records"] == []
    assert not technical_segment_gate_passes({"failed_schema": 1}, 1)


@pytest.mark.parametrize("blank", ["", " \t\n", "\u00a0", "\u2003"])
def test_blank_nullable_anchor_is_not_treated_as_null(blank: str) -> None:
    source, model_extraction = _blank_anchor_case(
        "endorsements", "endorsement_context_evidence", blank
    )
    model_extraction["endorsements"][0]["support_mode"] = (
        "page_or_section_context"
    )
    output = _model_output(source, model_extraction)
    assert source["endorsement_context_provenance"]["source_scope"] == (
        "page_or_section_route_context"
    )
    assert output["exact_alignment"]["status"] == "invalid_schema"
    _, status = validate_output_row(source, output)
    assert status == "failed_schema"


def test_one_blank_anchor_fails_the_whole_mixed_window() -> None:
    source, model_extraction = _blank_anchor_case(
        "issues", "evidence", ""
    )
    valid = deepcopy(model_extraction["issue_blocks"][0])
    valid["evidence"] = "We support good jobs."
    model_extraction["issue_blocks"].insert(0, valid)
    output = _model_output(source, model_extraction)

    alignment = output["exact_alignment"]
    assert alignment["status"] == "invalid_schema"
    assert alignment["model_item_count"] == 2
    assert alignment["aligned_item_count"] == 1
    assert alignment["withheld_item_count"] == 1
    _, status = validate_output_row(source, output)
    assert status == "failed_schema"

    segments, _ = enrich_segments(
        {source["document_id"]: source},
        {source["document_id"]: output},
    )
    assert segments[0]["processing_status"] == "failed_schema"
    assert segments[0]["record_variants"] == []
    assert segments[0]["resolved_records"] == []


def test_nonstop_valid_json_is_attested_as_failed_not_tampering() -> None:
    source = _source("segment-1", "She is a teacher.")
    output = _output(source, _bio_item(source["source_text"]))
    output["finish_reason"] = "length"
    output["parse_status"] = "output_truncated"
    output["extraction"] = None
    for field in (
        "model_extraction", "model_extraction_sha256",
        "pre_alignment_model_item_guard", "array_item_cap_report",
        "model_item_repetition_report",
        "aligned_extraction_sha256", "withheld_model_items",
        "exact_alignment", "validation",
    ):
        output.pop(field)
    segments, _ = enrich_segments(
        {"segment-1": source}, {"segment-1": output}
    )
    assert segments[0]["parse_status"] == "output_truncated"
    assert segments[0]["processing_status"] == "failed_output_cap_unrepaired"
    # The cap-limited class is not accounted on its own. It becomes accounted
    # only when the caller shows a repair attestation that names the window.
    assert not technical_segment_gate_passes(
        {"failed_output_cap_unrepaired": 1}, len(segments)
    )
    assert technical_segment_gate_passes(
        {"failed_output_cap_unrepaired": 1},
        len(segments),
        allow_withheld_output_cap=True,
    )
    assert output_provenance_summary(segments)["segments"] == 1
    assert all_output_aggregate(segments)["processing_status_counts"] == {
        "failed_output_cap_unrepaired": 1
    }
    tampered = deepcopy(output)
    tampered["model_extraction"] = {"facts": []}
    with pytest.raises(ValueError, match="output fields differ"):
        enrich_segments({"segment-1": source}, {"segment-1": tampered})


def test_nonstop_invalid_json_is_attested_as_failed_not_tampering() -> None:
    source = _source("segment-1", "She is a teacher.")
    output = _output(source, _bio_item(source["source_text"]))
    # Truncation may end after a delimiter before xgrammar can emit its fixed
    # separator. The finish reason remains the primary failure classification.
    raw_output = '{"facts":'
    _, report = canonicalize_json_string_controls(raw_output)
    output.update(report)
    output.update(validate_raw_structural_whitespace(raw_output))
    output.update({
        "raw_output": raw_output,
        "finish_reason": "length",
        "parse_status": "output_truncated",
    })
    output["extraction"] = None
    for field in (
        "model_extraction", "model_extraction_sha256",
        "pre_alignment_model_item_guard", "array_item_cap_report",
        "model_item_repetition_report",
        "aligned_extraction_sha256", "withheld_model_items",
        "exact_alignment", "validation",
    ):
        output.pop(field)
    segments, _ = enrich_segments(
        {"segment-1": source}, {"segment-1": output}
    )
    assert segments[0]["processing_status"] == "failed_output_cap_unrepaired"
    raw_summary = output_provenance_summary(segments)
    assert raw_summary["raw_structural_whitespace_gate_passed"] is False
    assert raw_summary["raw_structural_whitespace_invalid_outputs"] == 1


def test_invalid_output_token_accounting_is_rejected() -> None:
    source = _source("segment-1", "She is a teacher.")
    output = _output(source, _bio_item(source["source_text"]))
    output["output_tokens"] = output["max_output_tokens"] + 1
    with pytest.raises(ValueError, match="token accounting"):
        enrich_segments({"segment-1": source}, {"segment-1": output})

    output = _output(source, _bio_item(source["source_text"]))
    output["input_tokens_used"] = True
    with pytest.raises(ValueError, match="input token or character"):
        enrich_segments({"segment-1": source}, {"segment-1": output})

    output = _output(source, _bio_item(source["source_text"]))
    output["stop_reason"] = 7
    with pytest.raises(ValueError, match="stop reason"):
        enrich_segments({"segment-1": source}, {"segment-1": output})


@pytest.mark.parametrize(
    ("field", "tampered_value"),
    [
        ("json_control_canonicalization_applied", 0),
        ("json_control_canonicalization_count", False),
    ],
)
def test_canonicalization_replay_rejects_bool_integer_coercion(
    field: str,
    tampered_value,
) -> None:
    source = _source("segment-1", "She is a teacher.")
    output = _output(source, _bio_item(source["source_text"]))
    assert output["json_control_canonicalization_applied"] is False
    assert output["json_control_canonicalization_count"] == 0
    assert output[field] == tampered_value
    assert type(output[field]) is not type(tampered_value)
    output[field] = tampered_value
    with pytest.raises(ValueError, match=field):
        validate_output_row(source, output)


def test_canonicalization_replay_rejects_nested_map_type_coercion() -> None:
    source = _source("segment-1", "She is a teacher.")
    output = _output(source, _bio_item(source["source_text"]))
    raw_output = output["raw_output"].replace(
        '"evidence": "teacher"',
        '"evidence": "teach\u000ber"',
    )
    _, report = canonicalize_json_string_controls(raw_output)
    assert report["json_control_canonicalization_codepoint_counts"] == {
        "U+000B": 1
    }
    parsed = json.loads(raw_output.replace("\u000b", "\\u000b"))
    output.update(report)
    output.update(validate_raw_structural_whitespace(raw_output))
    output.update({
        "raw_output": raw_output,
        "model_extraction": parsed,
    })
    _replace_guard_payload(output, "biography", parsed)
    extraction, withheld, alignment = align_extraction_exact_spans(
        "biography", parsed, source["source_text"]
    )
    output.update({
        "model_extraction_sha256": alignment[
            "original_model_extraction_sha256"
        ],
        "extraction": extraction,
        "aligned_extraction_sha256": alignment[
            "aligned_extraction_sha256"
        ],
        "withheld_model_items": withheld,
        "exact_alignment": alignment,
        "validation": validate_extraction(
            "biography",
            extraction,
            source["source_text"],
            "2.1",
            parent_source_text=source["source_text"],
        ),
    })
    validate_output_row(source, output)

    tampered = deepcopy(output)
    tampered["json_control_canonicalization_codepoint_counts"] = {
        "U+000B": True
    }
    tampered_counts = tampered[
        "json_control_canonicalization_codepoint_counts"
    ]
    original_counts = output[
        "json_control_canonicalization_codepoint_counts"
    ]
    assert tampered_counts == original_counts
    assert type(tampered_counts["U+000B"]) is not type(
        original_counts["U+000B"]
    )
    with pytest.raises(
        ValueError,
        match="json_control_canonicalization_codepoint_counts",
    ):
        validate_output_row(source, tampered)


def test_raw_json_control_canonicalization_is_recomputed() -> None:
    source = _source("segment-1", "She is a teacher.")
    output = _output(source, _bio_item(source["source_text"]))
    raw_output = output["raw_output"].replace("teacher", "teach\u000ber")
    _, report = canonicalize_json_string_controls(raw_output)
    parsed = json.loads(raw_output.replace("\u000b", "\\u000b"))
    output.update(report)
    output.update(validate_raw_structural_whitespace(raw_output))
    output.update({
        "raw_output": raw_output,
        "model_extraction": parsed,
    })
    _replace_guard_payload(output, "biography", parsed)
    extraction, withheld, alignment = align_extraction_exact_spans(
        "biography", parsed, source["source_text"]
    )
    output.update({
        "model_extraction_sha256": alignment[
            "original_model_extraction_sha256"
        ],
        "extraction": extraction,
        "aligned_extraction_sha256": alignment["aligned_extraction_sha256"],
        "withheld_model_items": withheld,
        "exact_alignment": alignment,
        "validation": validate_extraction(
            "biography", extraction, source["source_text"], "2.1",
            parent_source_text=source["source_text"],
        ),
    })
    segments, _ = enrich_segments(
        {"segment-1": source}, {"segment-1": output}
    )
    assert segments[0]["json_control_canonicalization_count"] == 2
    assert segments[0]["processing_status"] == "processed_all_withheld"

    tampered = deepcopy(output)
    tampered["canonical_output_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="canonical_output_sha256"):
        enrich_segments({"segment-1": source}, {"segment-1": tampered})


def test_semantic_overlap_conflict_is_explicitly_withheld() -> None:
    text = "She is a teacher."
    first = {
        **_source("segment-1", text),
        "processing_status": "processed_success",
        "extraction": {"facts": [_bio_item(text, timing="current")]},
    }
    second = deepcopy(first)
    second["document_id"] = "segment-2"
    second["segment_index"] = 1
    second["extraction"] = {"facts": [_bio_item(text, timing="former")]}
    variants = build_record_variants([first, second])
    annotated, resolved = resolve_parent_variants("parent-1", "biography", variants)
    assert resolved == []
    assert {row["overlap_resolution_status"] for row in annotated} == {"conflict_withheld"}
    assert not any(row["group_release_eligible"] for row in annotated)
    assert technical_segment_gate_passes(
        {"processed_success": 1, "processed_partial_withheld": 1}, 2
    )
    assert not technical_segment_gate_passes(
        {"processed_success": 1, "failed_parse": 1}, 2
    )


def test_issue_heading_is_provenance_and_exact_heading_wins_null_variant() -> None:
    text = "Jobs\nWe support good jobs."
    evidence = "We support good jobs."
    evidence_start = text.index(evidence)
    exact_heading_item = {
        "source_heading": "Jobs",
        "source_heading_start_char": 0,
        "source_heading_end_char": 4,
        "source_heading_exact_match_count": 1,
        "source_heading_alignment_method": "unique_exact_unicode_substring_v1",
        "claim": "Support good jobs",
        "statement_kind": "support",
        "evidence": evidence,
        "evidence_start_char": evidence_start,
        "evidence_end_char": evidence_start + len(evidence),
    }
    null_heading_item = {
        **exact_heading_item,
        "source_heading": None,
        "source_heading_start_char": None,
        "source_heading_end_char": None,
        "source_heading_exact_match_count": 0,
        "source_heading_alignment_method": "not_applicable",
    }
    first = {
        **_source("segment-heading", text),
        "route": "issues",
        "processing_status": "processed_success",
        "extraction": {"issue_blocks": [exact_heading_item]},
    }
    second = {
        **_source("segment-null", text),
        "route": "issues",
        "segment_index": 1,
        "processing_status": "processed_success",
        "extraction": {"issue_blocks": [null_heading_item]},
    }
    annotated, resolved = resolve_parent_variants(
        "parent-1", "issues", build_record_variants([first, second])
    )
    assert len(resolved) == 1
    assert resolved[0]["item"]["source_heading"] == "Jobs"
    assert resolved[0]["resolution_method"].startswith(
        "prefer_exact_source_heading"
    )
    assert not any(
        row["overlap_resolution_status"] == "conflict_withheld"
        for row in annotated
    )

    second["extraction"]["issue_blocks"][0]["claim"] = "Oppose good jobs"
    annotated, resolved = resolve_parent_variants(
        "parent-1", "issues", build_record_variants([first, second])
    )
    assert resolved == []
    assert {row["overlap_resolution_status"] for row in annotated} == {
        "conflict_withheld"
    }


def test_distinct_exact_evidence_uses_documented_shortest_rule() -> None:
    long_text = "She works as a teacher."
    short_text = "teacher"
    first_item = _bio_item(long_text)
    first_item.update(
        evidence=long_text,
        evidence_start_char=0,
        evidence_end_char=len(long_text),
    )
    second_item = _bio_item(short_text)
    first = {
        **_source("segment-1", long_text),
        "processing_status": "processed_success",
        "extraction": {"facts": [first_item]},
    }
    second = {
        **_source("segment-2", short_text),
        "processing_status": "processed_success",
        "extraction": {"facts": [second_item]},
    }
    variants = build_record_variants([first, second])
    _, resolved = resolve_parent_variants("parent-1", "biography", variants)
    assert len(resolved) == 1
    assert resolved[0]["resolution_status"] == "equivalent_semantics_distinct_exact_evidence"
    assert resolved[0]["item"]["evidence"] == "teacher"
    assert resolved[0]["resolution_method"] == "shortest_exact_evidence_then_stable_segment_key"


def test_repeated_semantics_at_nonoverlapping_parent_spans_remain_distinct() -> None:
    first = {
        **_source("segment-1", "teacher", start=0, count=2),
        "processing_status": "processed_success",
        "extraction": {"facts": [_bio_item("teacher")]},
    }
    second = {
        **_source("segment-2", "teacher", start=20, count=2),
        "processing_status": "processed_success",
        "segment_index": 1,
        "extraction": {"facts": [_bio_item("teacher")]},
    }
    variants = build_record_variants([first, second])
    annotated, resolved = resolve_parent_variants("parent-1", "biography", variants)
    assert len({row["overlap_group_id"] for row in annotated}) == 2
    assert len(resolved) == 2


def test_known_omission_match_requires_route_field_and_exact_span_overlap() -> None:
    source = _source("segment-1", "She organized a free vaccination clinic.")
    item = {
        "field": "public_service_experience",
        "value": "organized a free vaccination clinic",
        "timing": "former",
        "relationship": None,
        "evidence": "organized a free vaccination clinic",
        "evidence_start_char": 4,
        "evidence_end_char": 4 + len("organized a free vaccination clinic"),
    }
    output = _output(source, item)
    segments, parents = enrich_segments({"segment-1": source}, {"segment-1": output})
    report = match_known_omissions(
        [{
            "omission_id": "o1",
            "parent_document_id": "parent-1",
            "candidate_cycle_id": source["candidate_cycle_id"],
            "route": "biography",
            "expected_type": "public_service_experience",
            "expected_value": "organized clinic",
            "evidence": "organized a free vaccination clinic",
        }],
        segments,
        parents,
    )
    assert report["all_recovered"] is True
    assert report["items"][0]["matched_record_ids"]
    assert report["matching_spec_sha256"] == KNOWN_OMISSION_MATCHING_SPEC_SHA256

    containing_target = match_known_omissions(
        [{
            "omission_id": "o1b",
            "parent_document_id": "parent-1",
            "candidate_cycle_id": source["candidate_cycle_id"],
            "route": "biography",
            "expected_type": "public_service_experience",
            "expected_value": "a differently worded fact for human review",
            "evidence": "She organized a free vaccination clinic.",
        }],
        segments,
        parents,
    )
    assert containing_target["items"][0]["recovered"] is True

    missing_source = match_known_omissions(
        [{
            "omission_id": "o2",
            "parent_document_id": "parent-1",
            "candidate_cycle_id": source["candidate_cycle_id"],
            "route": "biography",
            "expected_type": "public_service_experience",
            "expected_value": "organized clinic",
            "evidence": "Organized a free vaccination clinic",
        }],
        segments,
        parents,
    )
    assert missing_source["items"][0]["source_evidence_exact_occurrences"] == 0
    assert missing_source["items"][0]["recovered"] is False

    different_expected_value = match_known_omissions(
        [{
            "omission_id": "o3",
            "parent_document_id": "parent-1",
            "candidate_cycle_id": source["candidate_cycle_id"],
            "route": "biography",
            "expected_type": "public_service_experience",
            "expected_value": "built a bridge",
            "evidence": "organized a free vaccination clinic",
        }],
        segments,
        parents,
    )
    assert different_expected_value["items"][0]["recovered"] is True
    assert different_expected_value["items"][0][
        "expected_value_confirmation_required"
    ] is True

    wrong_type = match_known_omissions(
        [{
            "omission_id": "o4",
            "parent_document_id": "parent-1",
            "candidate_cycle_id": source["candidate_cycle_id"],
            "route": "biography",
            "expected_type": "military_service",
            "expected_value": "organized clinic",
            "evidence": "organized a free vaccination clinic",
        }],
        segments,
        parents,
    )
    assert wrong_type["items"][0]["recovered"] is False


# --- seed-vary repair identity -------------------------------------------------


def _repair_output_row(source: dict, item: dict, *, attempt_index: int = 2) -> dict:
    """Return one valid output row stamped with a repair attempt's identity."""
    row = _output(source, item)
    row["generation_config_sha256"] = EXPECTED_REPAIR_GENERATION_CONFIG_SHA256S[
        attempt_index
    ]
    return row


def _repair_window(
    chunk: str, row: dict, *, repaired: bool, attempt_index: int = 2
) -> dict:
    return {
        "chunk": chunk,
        "repair_output_file": chunk.replace(".jsonl.gz", ".out.jsonl.gz"),
        "repair_output_sha256": "a" * 64,
        "route": str(row["route"]),
        "repaired": repaired,
        "row": row,
        "attempt_index": attempt_index,
        "repair_seed": EXPECTED_REPAIR_GENERATION_SEEDS[attempt_index],
        "expected_generation_config_sha256": (
            EXPECTED_REPAIR_GENERATION_CONFIG_SHA256S[attempt_index]
        ),
        "expected_generation_config": EXPECTED_REPAIR_GENERATION_CONFIGS[
            attempt_index
        ],
        "manifest_sha256": "b" * 64,
    }


def _write_gzip_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with gzip.open(path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True) + "\n")


def _read_gzip_rows(path: Path) -> list[dict]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def _repair_attestation(
    *,
    chunk: str,
    chunk_sha256: str,
    output_sha256: str,
    row: dict,
    attempt_index: int = 2,
) -> dict:
    payload = {
        "repair_manifest_version": REPAIR_MANIFEST_VERSION,
        "schema_version": importer.SCHEMA_VERSION,
        "model_id": importer.MODEL_ID,
        "model_revision": importer.MODEL_REVISION,
        "runtime_lock_sha256": importer.RUNTIME_LOCK_SHA256,
        "prompt_sha256": importer.CONTRACT["prompt_sha256"],
        "json_schema_sha256": importer.CONTRACT["json_schema_sha256"],
        "extraction_contract_sha256": importer.CONTRACT[
            "extraction_contract_sha256"
        ],
        "max_model_len": importer.EXPECTED_MAX_MODEL_LEN,
        "max_output_tokens": EXPECTED_MAX_OUTPUT_TOKENS,
        "primary_seed": importer.EXPECTED_GENERATION_SEED,
        "repair_seed": EXPECTED_REPAIR_GENERATION_SEEDS[attempt_index],
        "primary_generation_config_sha256": EXPECTED_GENERATION_CONFIG_SHA256,
        "repair_generation_config_sha256": (
            EXPECTED_REPAIR_GENERATION_CONFIG_SHA256S[attempt_index]
        ),
        "attempt_index": attempt_index,
        "reason": REPAIR_REASON,
        "target_count": 1,
        "repaired_count": 1,
        "unrepaired_count": 0,
        "all_targets_repaired": True,
        "targets": [
            {
                "chunk": chunk,
                "document_id": str(row["document_id"]),
                "route": str(row["route"]),
                "input_chunk_sha256": chunk_sha256,
                "repair_output_file": chunk.replace(
                    ".jsonl.gz", ".out.jsonl.gz"
                ),
                "repair_output_sha256": output_sha256,
                "finish_reason": str(row["finish_reason"]),
                "parse_status": str(row["parse_status"]),
                "output_tokens": int(row["output_tokens"]),
                "raw_output_sha256": hashlib.sha256(
                    str(row["raw_output"]).encode("utf-8")
                ).hexdigest(),
                "generation_config_sha256": (
                    EXPECTED_REPAIR_GENERATION_CONFIG_SHA256S[attempt_index]
                ),
                "repaired": True,
            }
        ],
    }
    return payload


def _repair_fixture(
    tmp_path: Path, *, attempt_index: int = 2
) -> tuple[Path, Path, Path, dict]:
    source = _source("segment-repair", "She is a teacher.")
    # The attestation names document ids as sha256 digests, as the runner writes
    # them, so the fixture uses a digest rather than the readable test id.
    source["document_id"] = "a" * 64
    row = _repair_output_row(
        source, _bio_item(source["source_text"]), attempt_index=attempt_index
    )
    chunk = "chunk_00001.jsonl.gz"
    chunks_dir = tmp_path / "export" / "chunks"
    output_dir = tmp_path / f"repair_attempt_{attempt_index}"
    _write_gzip_jsonl(chunks_dir / chunk, [source])
    output_path = output_dir / chunk.replace(".jsonl.gz", ".out.jsonl.gz")
    _write_gzip_jsonl(output_path, [row])
    manifest = _repair_attestation(
        chunk=chunk,
        chunk_sha256=hashlib.sha256(
            (chunks_dir / chunk).read_bytes()
        ).hexdigest(),
        output_sha256=hashlib.sha256(output_path.read_bytes()).hexdigest(),
        row=row,
        attempt_index=attempt_index,
    )
    manifest_path = tmp_path / f"repair_manifest_{attempt_index}.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True))
    return manifest_path, output_dir, chunks_dir, manifest


def test_repair_identity_is_the_frozen_config_with_the_repair_seed() -> None:
    """Only the seed may differ, so the second identity is fully determined."""
    from scripts.hive.extract_candidate_pages_vllm import (
        generation_config_payload,
        get_extraction_contract,
    )

    args = SimpleNamespace(
        schema_version="2.1",
        seed=EXPECTED_REPAIR_GENERATION_SEED,
        max_model_len=importer.EXPECTED_MAX_MODEL_LEN,
        max_output_tokens=EXPECTED_MAX_OUTPUT_TOKENS,
        compact_repair=False,
    )
    observed = generation_config_payload(args, get_extraction_contract("2.1"))
    assert observed == EXPECTED_REPAIR_GENERATION_CONFIG
    assert EXPECTED_REPAIR_GENERATION_CONFIG["seed"] == (
        EXPECTED_REPAIR_GENERATION_SEED
    )
    assert importer.sha256_text(
        importer.canonical_json(EXPECTED_REPAIR_GENERATION_CONFIG)
    ) == EXPECTED_REPAIR_GENERATION_CONFIG_SHA256
    assert EXPECTED_REPAIR_GENERATION_CONFIG_SHA256 != (
        EXPECTED_GENERATION_CONFIG_SHA256
    )
    assert {
        key
        for key in EXPECTED_REPAIR_GENERATION_CONFIG
        if EXPECTED_REPAIR_GENERATION_CONFIG[key]
        != EXPECTED_GENERATION_CONFIG.get(key)
    } == {"seed"}


def test_repair_row_is_accepted_only_under_the_declared_window() -> None:
    """A repair row is refused unless the caller names its window as repaired."""
    source = _source("segment-repair", "She is a teacher.")
    primary = _output(source, _bio_item(source["source_text"]))
    repair = _repair_output_row(source, _bio_item(source["source_text"]))
    validate_output_row(source, primary)
    with pytest.raises(ValueError, match="not the frozen v2.1 identity"):
        validate_output_row(source, repair)
    validate_output_row(
        source,
        repair,
        EXPECTED_REPAIR_GENERATION_CONFIG_SHA256,
        EXPECTED_REPAIR_GENERATION_CONFIG,
        None,
        repair_identity=True,
    )
    # The repair identity does not admit any third identity.
    forged = dict(repair, generation_config_sha256="d" * 64)
    with pytest.raises(ValueError, match="is absent|not the frozen"):
        validate_output_row(
            source,
            forged,
            EXPECTED_REPAIR_GENERATION_CONFIG_SHA256,
            EXPECTED_REPAIR_GENERATION_CONFIG,
            None,
            repair_identity=True,
        )


def test_enrich_segments_records_the_attempt_on_every_named_window() -> None:
    source = _source("segment-repair", "She is a teacher.")
    repair = _repair_output_row(source, _bio_item(source["source_text"]))
    window = _repair_window("chunk_00001.jsonl.gz", repair, repaired=True)
    segments, _ = enrich_segments(
        {source["document_id"]: source},
        {source["document_id"]: repair},
        EXPECTED_GENERATION_CONFIG_SHA256,
        EXPECTED_GENERATION_CONFIG,
        repair_windows={source["document_id"]: window},
    )
    attempt = segments[0]["repair_attempt"]
    assert attempt["repaired"] is True
    assert attempt["attempt_index"] == REPAIR_ATTEMPT_INDEX
    assert attempt["repair_seed"] == EXPECTED_REPAIR_GENERATION_SEED
    assert attempt["repair_manifest_sha256"] == "b" * 64
    assert attempt["repair_generation_config_sha256"] == (
        EXPECTED_REPAIR_GENERATION_CONFIG_SHA256
    )
    assert segments[0]["processing_status"] == "processed_success"

    # An attestation that reports the window as unrepaired leaves the row bound to
    # the primary identity, so the repair row is refused.
    unrepaired = _repair_window(
        "chunk_00001.jsonl.gz", repair, repaired=False
    )
    with pytest.raises(ValueError, match="not the frozen v2.1 identity"):
        enrich_segments(
            {source["document_id"]: source},
            {source["document_id"]: repair},
            EXPECTED_GENERATION_CONFIG_SHA256,
            EXPECTED_GENERATION_CONFIG,
            repair_windows={source["document_id"]: unrepaired},
        )

    incomplete = dict(window)
    incomplete.pop("manifest_sha256")
    with pytest.raises(ValueError, match="lacks its attempt identity"):
        enrich_segments(
            {source["document_id"]: source},
            {source["document_id"]: repair},
            EXPECTED_GENERATION_CONFIG_SHA256,
            EXPECTED_GENERATION_CONFIG,
            repair_windows={source["document_id"]: incomplete},
        )


def test_apply_repair_windows_swaps_only_recovered_windows() -> None:
    recovered_source = _source("segment-a", "She is a teacher.")
    kept_source = _source("segment-b", "He is a teacher.")
    recovered_row = _repair_output_row(
        recovered_source, _bio_item(recovered_source["source_text"])
    )
    kept_primary = _output(kept_source, _bio_item(kept_source["source_text"]))
    kept_repair = _repair_output_row(
        kept_source, _bio_item(kept_source["source_text"])
    )
    outputs = {
        recovered_source["document_id"]: _output(
            recovered_source, _bio_item(recovered_source["source_text"])
        ),
        kept_source["document_id"]: kept_primary,
    }
    windows = {
        recovered_source["document_id"]: _repair_window(
            "chunk_00001.jsonl.gz", recovered_row, repaired=True
        ),
        kept_source["document_id"]: _repair_window(
            "chunk_00002.jsonl.gz", kept_repair, repaired=False
        ),
    }
    applied = apply_repair_windows(outputs, windows)
    assert set(applied) == {recovered_source["document_id"]}
    assert outputs[recovered_source["document_id"]] == recovered_row
    assert outputs[kept_source["document_id"]] == kept_primary

    with pytest.raises(ValueError, match="absent from the Hive output"):
        apply_repair_windows(
            {},
            {
                recovered_source["document_id"]: windows[
                    recovered_source["document_id"]
                ]
            },
        )


def test_load_repair_windows_accepts_a_consistent_attestation(
    tmp_path: Path,
) -> None:
    manifest_path, output_dir, chunks_dir, manifest = _repair_fixture(tmp_path)
    loaded = load_repair_windows(
        manifest_path,
        repair_output_dir=output_dir,
        export_chunks_dir=chunks_dir,
    )
    assert loaded["target_count"] == 1
    assert loaded["repaired_count"] == 1
    assert loaded["unrepaired_count"] == 0
    assert loaded["attempt_index"] == REPAIR_ATTEMPT_INDEX
    assert loaded["manifest_sha256"] == hashlib.sha256(
        manifest_path.read_bytes()
    ).hexdigest()
    window = loaded["windows"][manifest["targets"][0]["document_id"]]
    assert window["repaired"] is True
    assert window["repair_output_file"] == "chunk_00001.out.jsonl.gz"


@pytest.mark.parametrize(
    "corruption, message",
    [
        ("version", "version differs"),
        ("seed", "has wrong repair_seed"),
        ("repair_hash", "has wrong repair_generation_config_sha256"),
        ("attempt", "undeclared attempt index"),
        ("count", "repaired count differs"),
        ("row_generation", "repair generation hash differs"),
        ("foreign_identity", "generation config is not the repair identity"),
        ("row_finish_reason", "repair finish_reason differs"),
        ("output_hash", "repair output hash differs"),
        ("flag", "repair acceptance flag differs"),
    ],
)
def test_load_repair_windows_refuses_a_corrupted_attestation(
    tmp_path: Path, corruption: str, message: str
) -> None:
    manifest_path, output_dir, chunks_dir, _ = _repair_fixture(tmp_path)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    output_path = output_dir / payload["targets"][0]["repair_output_file"]
    if corruption == "version":
        payload["repair_manifest_version"] = "something_else"
    elif corruption == "seed":
        payload["repair_seed"] = EXPECTED_REPAIR_GENERATION_SEED + 1
    elif corruption == "repair_hash":
        payload["repair_generation_config_sha256"] = "e" * 64
    elif corruption == "attempt":
        payload["attempt_index"] = 4
    elif corruption == "count":
        payload["repaired_count"] = 0
        payload["unrepaired_count"] = 1
    elif corruption == "row_generation":
        payload["targets"][0]["generation_config_sha256"] = "e" * 64
    elif corruption == "foreign_identity":
        # The row and the attestation agree on a hash that is neither the primary
        # nor the declared repair identity, so the identity check must refuse it.
        rows = [
            dict(row, generation_config_sha256="e" * 64)
            for row in _read_gzip_rows(output_path)
        ]
        _write_gzip_jsonl(output_path, rows)
        payload["targets"][0]["generation_config_sha256"] = "e" * 64
        payload["targets"][0]["repair_output_sha256"] = hashlib.sha256(
            output_path.read_bytes()
        ).hexdigest()
    elif corruption == "row_finish_reason":
        payload["targets"][0]["finish_reason"] = "length"
    elif corruption == "output_hash":
        payload["targets"][0]["repair_output_sha256"] = "e" * 64
    elif corruption == "flag":
        payload["targets"][0]["repaired"] = False
        payload["repaired_count"] = 0
        payload["unrepaired_count"] = 1
        payload["all_targets_repaired"] = False
    manifest_path.write_text(json.dumps(payload, sort_keys=True))
    with pytest.raises(ValueError, match=message):
        load_repair_windows(
            manifest_path,
            repair_output_dir=output_dir,
            export_chunks_dir=chunks_dir,
        )


def test_load_repair_windows_refuses_a_wrong_field_set(tmp_path: Path) -> None:
    manifest_path, output_dir, chunks_dir, _ = _repair_fixture(tmp_path)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["targets"][0]["extra_field"] = "x"
    manifest_path.write_text(json.dumps(payload, sort_keys=True))
    with pytest.raises(ValueError, match="entry fields differ"):
        load_repair_windows(
            manifest_path,
            repair_output_dir=output_dir,
            export_chunks_dir=chunks_dir,
        )


def test_load_repair_windows_refuses_a_repeated_target(tmp_path: Path) -> None:
    manifest_path, output_dir, chunks_dir, _ = _repair_fixture(tmp_path)
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    payload["targets"].append(dict(payload["targets"][0]))
    payload["target_count"] = 2
    payload["repaired_count"] = 2
    manifest_path.write_text(json.dumps(payload, sort_keys=True))
    with pytest.raises(ValueError, match="repeated repair target"):
        load_repair_windows(
            manifest_path,
            repair_output_dir=output_dir,
            export_chunks_dir=chunks_dir,
        )


def test_load_repair_windows_accepts_the_third_attempt(tmp_path: Path) -> None:
    """A third seed is a declared attempt with its own derived identity."""
    manifest_path, output_dir, chunks_dir, manifest = _repair_fixture(
        tmp_path, attempt_index=3
    )
    loaded = load_repair_windows(
        manifest_path,
        repair_output_dir=output_dir,
        export_chunks_dir=chunks_dir,
    )
    assert loaded["attempt_index"] == 3
    assert loaded["repair_seed"] == 20260918
    assert loaded["generation_config_sha256"] == (
        EXPECTED_REPAIR_GENERATION_CONFIG_SHA256S[3]
    )
    assert loaded["generation_config_sha256"] != (
        EXPECTED_REPAIR_GENERATION_CONFIG_SHA256S[2]
    )
    window = loaded["windows"][manifest["targets"][0]["document_id"]]
    assert window["attempt_index"] == 3
    assert window["repair_seed"] == 20260918


def test_apply_repair_attempts_prefers_the_later_accepted_row(
    tmp_path: Path,
) -> None:
    """A window the second attempt fixed takes that row, whatever order it arrives."""
    second_manifest, second_dir, chunks_dir, second = _repair_fixture(
        tmp_path, attempt_index=2
    )
    third_manifest, third_dir, _, third = _repair_fixture(
        tmp_path, attempt_index=3
    )
    document_id = third["targets"][0]["document_id"]
    loaded_second = load_repair_windows(
        second_manifest,
        repair_output_dir=second_dir,
        export_chunks_dir=chunks_dir,
    )
    loaded_third = load_repair_windows(
        third_manifest,
        repair_output_dir=third_dir,
        export_chunks_dir=chunks_dir,
    )
    # The second attempt reached the cap again on this window.
    loaded_second["windows"][document_id]["repaired"] = False
    outputs = {document_id: _output(
        _source("segment-repair", "She is a teacher."),
        _bio_item("She is a teacher."),
    )}
    applied = apply_repair_attempts(outputs, [loaded_third, loaded_second])
    assert set(applied) == {document_id}
    assert applied[document_id]["attempt_index"] == 3
    assert outputs[document_id]["generation_config_sha256"] == (
        EXPECTED_REPAIR_GENERATION_CONFIG_SHA256S[3]
    )

    with pytest.raises(ValueError, match="repeated repair attempt index"):
        apply_repair_attempts(outputs, [loaded_third, loaded_third])


def test_main_requires_paired_repair_arguments(tmp_path: Path, monkeypatch) -> None:
    """The repair manifest and its output directory are given in pairs."""
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "import_candidate_extraction_v2.py",
            "--export-dir",
            str(tmp_path / "export"),
            "--hive-output-dir",
            str(tmp_path / "hive"),
            "--endorsement-stage-a-export-dir",
            str(tmp_path / "a"),
            "--endorsement-stage-a-output-dir",
            str(tmp_path / "a"),
            "--endorsement-stage-a2-export-dir",
            str(tmp_path / "a2"),
            "--endorsement-stage-a2-output-dir",
            str(tmp_path / "a2"),
            "--endorsement-stage-a3-task-json",
            str(tmp_path / "task.json"),
            "--endorsement-stage-a3-task-html",
            str(tmp_path / "task.html"),
            "--endorsement-stage-a2-evaluation-report",
            str(tmp_path / "report.json"),
            "--endorsement-stage-a3-results-json",
            str(tmp_path / "results.json"),
            "--endorsement-stage-a3-imported-artifact-json",
            str(tmp_path / "imported.json"),
            "--endorsement-stage-b-export-dir",
            str(tmp_path / "b"),
            "--endorsement-stage-b-output-dir",
            str(tmp_path / "b"),
            "--output-dir",
            str(tmp_path / "bundle"),
            "--repair-manifest",
            str(tmp_path / "repair_manifest_2.json"),
        ],
    )
    with pytest.raises(SystemExit):
        importer.main()


def _repair_run_metadata_payload(
    tmp_path: Path, *, attempt_index: int = 2, selected: int = 1, seed: int | None = None
) -> dict:
    """Build a repair run's metadata with the runner that writes it in production."""
    from argparse import Namespace

    from scripts.hive.extract_candidate_pages_vllm import (
        get_extraction_contract,
        run_metadata_payload,
    )

    args = Namespace(
        schema_version="2.1",
        model_path=tmp_path / MODEL_REVISION,
        max_output_tokens=EXPECTED_MAX_OUTPUT_TOKENS,
        max_model_len=importer.EXPECTED_MAX_MODEL_LEN,
        seed=(
            seed
            if seed is not None
            else importer.EXPECTED_REPAIR_GENERATION_SEEDS[attempt_index]
        ),
        compact_repair=False,
        shard_index=None,
        shard_count=None,
    )
    return run_metadata_payload(
        args,
        get_extraction_contract("2.1"),
        started="2026-09-18T04:22:39+00:00",
        completed="2026-09-18T06:12:51+00:00",
        processed=1,
        parsed=1,
        schema_valid=1,
        exactly_supported=1,
        selected_documents=selected,
        recovery_attestation=False,
        finish_reason_counts={"stop": 1},
        total_output_tokens=100,
        max_output_tokens_observed=100,
        model_items=1,
        aligned_items=1,
        withheld_items=0,
        alignment_status_counts={"complete": 1},
        strict_json_parsed_outputs=1,
        model_items_strictly_parsed=1,
        max_exact_model_item_multiplicity=1,
        route_model_item_count_max={
            "issues": 1,
            "biography": 0,
            "endorsements": 0,
        },
    )


def _write_repair_run_metadata(directory: Path, payload: dict) -> Path:
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / "run_metadata.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True))
    return path


def test_repair_run_metadata_binds_the_attempt_identity(tmp_path: Path) -> None:
    """The attempt's own metadata must carry that attempt's seed and hash."""
    output_dir = tmp_path / "repair_attempt_2"
    path = _write_repair_run_metadata(
        output_dir, _repair_run_metadata_payload(tmp_path, selected=36)
    )
    loaded = importer.load_repair_run_metadata(
        output_dir, attempt_index=2, target_count=36
    )
    assert loaded["metadata_file"] == "run_metadata.json"
    assert loaded["metadata_sha256"] == hashlib.sha256(
        path.read_bytes()
    ).hexdigest()
    assert loaded["attempt_index"] == 2
    assert loaded["generation_config_sha256"] == (
        importer.EXPECTED_REPAIR_GENERATION_CONFIG_SHA256S[2]
    )
    entry = importer._repair_run_metadata_entry(loaded)
    assert entry["seed"] == importer.EXPECTED_REPAIR_GENERATION_SEEDS[2]
    assert entry["documents_selected_for_shard"] == 36
    assert entry["outputs_complete_for_selected_shard"] is True


def test_repair_run_metadata_refuses_the_primary_identity(tmp_path: Path) -> None:
    """A repair that kept the frozen seed is not a repair, so it is refused."""
    output_dir = tmp_path / "repair_attempt_2"
    _write_repair_run_metadata(
        output_dir,
        _repair_run_metadata_payload(
            tmp_path, seed=importer.EXPECTED_GENERATION_SEED
        ),
    )
    with pytest.raises(ValueError, match="generation block differs"):
        importer.load_repair_run_metadata(
            output_dir, attempt_index=2, target_count=1
        )


def test_repair_run_metadata_refuses_another_attempt_seed(tmp_path: Path) -> None:
    """Attempt 3's metadata must not pass as attempt 2's."""
    output_dir = tmp_path / "repair_attempt_3"
    _write_repair_run_metadata(
        output_dir, _repair_run_metadata_payload(tmp_path, attempt_index=3)
    )
    with pytest.raises(ValueError, match="generation block differs"):
        importer.load_repair_run_metadata(
            output_dir, attempt_index=2, target_count=1
        )
    loaded = importer.load_repair_run_metadata(
        output_dir, attempt_index=3, target_count=1
    )
    assert loaded["generation"]["seed"] == (
        importer.EXPECTED_REPAIR_GENERATION_SEEDS[3]
    )


def test_repair_run_metadata_refuses_a_scope_that_differs(tmp_path: Path) -> None:
    """The selected count covers the whole attempt, so it must equal the targets."""
    output_dir = tmp_path / "repair_attempt_2"
    _write_repair_run_metadata(
        output_dir, _repair_run_metadata_payload(tmp_path, selected=36)
    )
    with pytest.raises(ValueError, match="differ from the target count"):
        importer.load_repair_run_metadata(
            output_dir, attempt_index=2, target_count=24
        )


def test_repair_run_metadata_refuses_an_incomplete_attestation(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "repair_attempt_2"
    payload = _repair_run_metadata_payload(tmp_path)
    payload["outputs_complete_for_selected_shard"] = False
    _write_repair_run_metadata(output_dir, payload)
    with pytest.raises(ValueError, match="not attested complete"):
        importer.load_repair_run_metadata(
            output_dir, attempt_index=2, target_count=1
        )


def test_repair_run_metadata_refuses_a_sharded_run(tmp_path: Path) -> None:
    """The repair selects chunks by name, so a shard index means a wrong file."""
    output_dir = tmp_path / "repair_attempt_2"
    payload = _repair_run_metadata_payload(tmp_path)
    payload["shard_index"] = 0
    payload["shard_count"] = 20
    _write_repair_run_metadata(output_dir, payload)
    with pytest.raises(ValueError, match="does not shard"):
        importer.load_repair_run_metadata(
            output_dir, attempt_index=2, target_count=1
        )


def test_repair_run_metadata_requires_the_file(tmp_path: Path) -> None:
    (tmp_path / "repair_attempt_2").mkdir()
    with pytest.raises(ValueError, match="lacks run_metadata.json"):
        importer.load_repair_run_metadata(
            tmp_path / "repair_attempt_2", attempt_index=2, target_count=1
        )


def test_round4_production_stage_identities_are_frozen() -> None:
    """The two production stages are bound to one identity each.

    These were read from the completed runs on 2026-09-18. Stage A and Stage B
    each carried exactly one generation identity across their shards. The record
    ledger builder writes them into the ledger summary, so a blank or repeated
    value here would let the release claim an identity it never ran under.
    """
    frozen = {
        "stage_a_input_manifest": (
            importer.FROZEN_ROUND4_PRODUCTION_STAGE_A_INPUT_MANIFEST_SHA256
        ),
        "stage_a_generation_config": (
            importer.FROZEN_ROUND4_PRODUCTION_STAGE_A_GENERATION_CONFIG_SHA256
        ),
        "stage_b_input_manifest": (
            importer.FROZEN_ROUND4_PRODUCTION_STAGE_B_INPUT_MANIFEST_SHA256
        ),
        "stage_b_generation_config": (
            importer.FROZEN_ROUND4_PRODUCTION_STAGE_B_GENERATION_CONFIG_SHA256
        ),
    }
    for name, value in frozen.items():
        assert isinstance(value, str), f"{name} is not frozen"
        assert importer.HEX64.fullmatch(value), f"{name} is not a sha256 digest"
    assert len(set(frozen.values())) == len(frozen), "a stage identity repeats"


def test_round4_and_line_ledger_records_are_labelled_by_architecture(
    monkeypatch,
) -> None:
    """A released record names the architecture that produced it.

    The two architectures build a record the same way, so the same aligned item
    must still yield a different record identity and different labels. Reusing
    the line-ledger vocabulary would make a round-four record read as a
    line-ledger record in the released table, where the architecture is not
    otherwise visible. The parent offsets are stubbed because they are the same
    computation for both and are not what this test is about.
    """
    from src.candidate_endorsement_round4_release import (
        ROUND4_RECORD_RESOLUTION_METHOD,
        ROUND4_RECORD_RESOLUTION_STATUS,
    )

    offsets = {
        "record_evidence_parent_start_char": 0,
        "record_evidence_parent_end_char": 17,
    }
    monkeypatch.setattr(importer, "parent_offsets", lambda *a, **k: dict(offsets))
    aligned = {
        "source_model_item_id": "9" * 64,
        "record_evidence_carrier_segment_id": "c" * 64,
        "inventory_owner_segment_id": "c" * 64,
        **offsets,
    }
    segment_by_id = {
        "c" * 64: {"parent_document_id": "p" * 64, "segment_start_char": 0}
    }
    parent_id = "p" * 64

    line = importer._line_ledger_resolved_record(aligned, segment_by_id, parent_id)
    round4 = importer.round4_resolved_record(aligned, segment_by_id, parent_id)

    assert line["resolution_status"] == "line_ledger_single_record"
    assert line["resolution_method"] == "two_stage_parent_line_ledger_v1"
    assert round4["resolution_status"] == ROUND4_RECORD_RESOLUTION_STATUS
    assert round4["resolution_method"] == ROUND4_RECORD_RESOLUTION_METHOD
    assert round4["record_id"] != line["record_id"]
    # Everything else about the record is the same construction.
    ignored = {"record_id", "resolution_status", "resolution_method"}
    assert {k: v for k, v in round4.items() if k not in ignored} == {
        k: v for k, v in line.items() if k not in ignored
    }
