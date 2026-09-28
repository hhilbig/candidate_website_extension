"""Tests for the fail-closed candidate extraction release packager."""
from __future__ import annotations

import gzip
import json
from copy import deepcopy
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.build_candidate_extraction_release import (
    EXTENSION_MANIFEST_FILENAME,
    ENDORSEMENT_COLUMNS,
    EXPECTED_EXTRACTION_CONTRACT_SHA256,
    LINE_LEDGER_ENDORSEMENT_ARCHITECTURE,
    PUBLIC_FILES,
    ROUND4_ENDORSEMENT_ARCHITECTURE,
    ROUND4_LEDGER_MANIFEST_KEY,
    ReleaseInputError,
    _endorsement_architecture,
    _endorsement_route_contract,
    _load_human_validation,
    _parent_payload,
    _public_metadata,
    _public_segment_output_provenance,
    _validate_item,
    _validate_run_metadata_entries,
    _validate_seed_vary_repair,
    build_release,
    extension_manifest_entries,
    load_bundle,
    sha256_file,
    sha256_text,
)
from scripts.build_candidate_extraction_handcode import (
    HandcodeInputError,
    load_frozen_bundle,
)
from scripts.import_candidate_extraction_v2 import (
    EXPECTED_GENERATION_CONFIG,
    EXPECTED_GENERATION_CONFIG_SHA256,
    EXPECTED_GENERATION_SEED,
    EXPECTED_MAX_MODEL_LEN,
    EXPECTED_MAX_OUTPUT_TOKENS,
    FROZEN_SUFFIX_P_FULL_SOURCE_MANIFEST_SHA256,
    FROZEN_SUFFIX_P_SOURCE_CORPUS_SHA256,
    KNOWN_OMISSION_MATCHING_SPEC,
    KNOWN_OMISSION_MATCHING_SPEC_SHA256,
    all_output_aggregate,
    build_record_variants,
    canonical_json,
    endorsement_segment_complete_line_census,
    output_provenance_summary,
    resolve_parent_variants,
    run_metadata_aggregate,
)
from scripts.hive.extract_candidate_pages_vllm import (
    GENERATION_CONFIG_VERSION,
    STRUCTURED_DECODING_BACKEND,
    STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
    STRUCTURED_DECODING_MODE,
    build_user_message,
    canonicalize_json_string_controls,
    validate_raw_structural_whitespace,
)
from scripts.validate_candidate_extraction_release import (
    _validate_arrow_table,
    _validate_public_route_provenance,
    validate_candidate_extraction_release,
)
from src.candidate_extraction import (
    ENDORSEMENT_CONTEXT_CLASSIFIER_ID,
    ENDORSEMENT_CONTEXT_CLASSIFIER_SPEC_SHA256,
    ENDORSEMENT_CONTEXT_PROVENANCE_VERSION,
    ENDORSEMENT_CONTEXT_PROVENANCE_SPEC_SHA256,
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
from src.candidate_endorsement_line_ledger import get_line_ledger_contract


MODEL = "Qwen/Qwen3.6-27B-FP8"
REVISION = "e89b16ebf1988b3d6befa7de50abc2d76f26eb09"
RUNTIME = "4f46101fa493e38b312af034b8d740c612bd0001bab4cb0f624f4a9e1c49f5b3"
CONTRACT = get_extraction_contract("2.1")
GENERATION_CONFIG = deepcopy(EXPECTED_GENERATION_CONFIG)
GENERATION_HASH = sha256_text(canonical_json(GENERATION_CONFIG))
assert GENERATION_HASH == EXPECTED_GENERATION_CONFIG_SHA256


def test_release_binds_the_live_frozen_v2_1_contract() -> None:
    assert EXPECTED_EXTRACTION_CONTRACT_SHA256 == CONTRACT[
        "extraction_contract_sha256"
    ]


def test_run_metadata_accounting_is_fail_closed(tmp_path: Path) -> None:
    _, bundle, rows = _write_fixture(tmp_path)
    manifest = json.loads((bundle / "bundle_manifest.json").read_text())
    _validate_run_metadata_entries(manifest)

    tampered = deepcopy(manifest)
    tampered["run_metadata"][0]["documents_selected_for_shard"] = "4"
    with pytest.raises(ReleaseInputError, match="selected_for_shard"):
        _validate_run_metadata_entries(tampered)

    tampered = deepcopy(manifest)
    entry = tampered["run_metadata"][0]
    entry["json_control_canonicalized_outputs_this_run"] = 1
    entry["json_control_canonicalized_characters_this_run"] = 1
    entry[
        "json_control_canonicalization_codepoint_counts_this_run"
    ] = {"NOT_A_CODEPOINT": 1}
    tampered["run_metadata_aggregate"] = run_metadata_aggregate(
        tampered["run_metadata"]
    )
    with pytest.raises(ReleaseInputError, match="code-point accounting"):
        _validate_run_metadata_entries(tampered)

    weakened = deepcopy(manifest)
    weakened["run_metadata"][0][
        "structured_decoding_disable_any_whitespace"
    ] = False
    with pytest.raises(ReleaseInputError, match="whitespace policy"):
        _validate_run_metadata_entries(weakened)

    guard_identity = deepcopy(manifest)
    guard_identity["run_metadata"][0]["array_item_cap_spec_sha256"] = "0" * 64
    with pytest.raises(ReleaseInputError, match="array_item_cap_spec_sha256"):
        _validate_run_metadata_entries(guard_identity)

    guard_count = deepcopy(manifest)
    guard_count["run_metadata"][0][
        "array_item_cap_reached_outputs_this_run"
    ] = 1
    guard_count["run_metadata_aggregate"] = run_metadata_aggregate(
        guard_count["run_metadata"]
    )
    with pytest.raises(ReleaseInputError, match="strict-JSON output counts"):
        _validate_run_metadata_entries(guard_count)

    route_max = deepcopy(manifest)
    route_max["run_metadata"][0]["route_model_item_count_max_this_run"][
        "biography"
    ] = CONTRACT["route_array_item_caps"]["biography"] + 1
    route_max["run_metadata_aggregate"] = run_metadata_aggregate(
        route_max["run_metadata"]
    )
    with pytest.raises(ReleaseInputError, match="exceeds its cap"):
        _validate_run_metadata_entries(route_max)

    bogus_zero = deepcopy(manifest)
    entry = bogus_zero["run_metadata"][0]
    for field in entry:
        if field.endswith("_this_run"):
            if field == "documents_selected_for_shard":
                continue
            entry[field] = (
                {route: 0 for route in CONTRACT["route_array_item_caps"]}
                if field == "route_model_item_count_max_this_run"
                else {}
                if field in {
                    "finish_reason_counts_this_run",
                    "alignment_status_counts_this_run",
                    "json_control_canonicalization_codepoint_counts_this_run",
                }
                else 0
            )
    entry["recovery_attestation_without_inference"] = False
    bogus_zero["run_metadata_aggregate"] = run_metadata_aggregate(
        bogus_zero["run_metadata"]
    )
    with pytest.raises(ReleaseInputError, match="require a recovery attestation"):
        _validate_run_metadata_entries(bogus_zero)

    recovery_only = deepcopy(bogus_zero)
    recovery_only["run_metadata"][0][
        "recovery_attestation_without_inference"
    ] = True
    recovery_only["run_metadata_aggregate"] = run_metadata_aggregate(
        recovery_only["run_metadata"]
    )
    _validate_run_metadata_entries(recovery_only)

    partial_restart = deepcopy(manifest)
    partial_entry = _run_metadata_entry(rows[:1])
    partial_entry["documents_selected_for_shard"] = len(rows)
    partial_restart["run_metadata"] = [partial_entry]
    partial_restart["run_metadata_aggregate"] = run_metadata_aggregate(
        partial_restart["run_metadata"]
    )
    _validate_run_metadata_entries(partial_restart)

    over_ledger = deepcopy(manifest)
    over_ledger["run_metadata"][0]["output_tokens_total_this_run"] += 1
    over_ledger["run_metadata_aggregate"] = run_metadata_aggregate(
        over_ledger["run_metadata"]
    )
    with pytest.raises(ReleaseInputError, match="output_tokens_total.*exceeds"):
        _validate_run_metadata_entries(over_ledger)

    for mapping_field, category in (
        ("finish_reason_counts_this_run", "invented_zero_category"),
        ("alignment_status_counts_this_run", "partial_withheld"),
    ):
        zero_category = deepcopy(manifest)
        zero_category["run_metadata"][0][mapping_field][category] = 0
        zero_category["run_metadata_aggregate"] = run_metadata_aggregate(
            zero_category["run_metadata"]
        )
        with pytest.raises(ReleaseInputError, match="zero category"):
            _validate_run_metadata_entries(zero_category)


def test_release_rejects_boolean_guard_counts_in_private_aggregate(
    tmp_path: Path,
) -> None:
    core, bundle, _ = _write_fixture(tmp_path)
    manifest_path = bundle / "bundle_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["all_output_aggregate"]["array_item_cap_reached_outputs"] = False
    manifest["all_output_aggregate"][
        "model_item_repetition_error_outputs"
    ] = False
    manifest_path.write_text(json.dumps(manifest))

    with pytest.raises(ReleaseInputError, match="all-output aggregate differs"):
        build_release(core, bundle, tmp_path / "release", production=False)


def _url(route: str, timestamp: str = "20200102010101") -> str:
    return f"https://web.archive.org/web/{timestamp}/https://example.org/{route}"


def _raw_row(candidate_id: str, route: str, text: str, *, timestamp: str = "20200102010101") -> dict:
    return {
        "candidate_cycle_id": candidate_id,
        "candidate_icpsr": f"Candidate {candidate_id}",
        "state": "AA",
        "office": "house",
        "year": 2020,
        "party": "democrat",
        "date": timestamp,
        "snap_url": _url(route, timestamp),
        "page_type": route,
        "text_snap_content": text,
    }


def _parent_id(candidate_id: str, route: str, text: str) -> tuple[str, str]:
    raw_hash = sha256_text(text)
    return sha256_text(f"{candidate_id}\n{route}\n{raw_hash}"), raw_hash


def _segment_row(candidate_id: str, route: str, text: str, extraction: dict) -> dict:
    parent_id, raw_hash = _parent_id(candidate_id, route, text)
    segment_hash = sha256_text(text)
    segment_id = sha256_text(f"{parent_id}\n0\n0\n{len(text)}\n{segment_hash}")
    context_provenance = (
        build_endorsement_context_provenance(
            parent_document_id=parent_id,
            parent_source_text=text,
            source_urls=[_url(route)],
            segment_id=segment_id,
            segment_start_char=0,
            segment_end_char=len(text),
        )
        if route == "endorsements"
        else None
    )
    list_key = {"issues": "issue_blocks", "biography": "facts", "endorsements": "endorsements"}[route]
    model_extraction = {
        list_key: [
            {
                key: value for key, value in item.items()
                if not key.endswith((
                    "_start_char", "_end_char", "_exact_match_count",
                    "_alignment_method",
                )) and key != "source_model_item_id"
            }
            for item in extraction[list_key]
        ]
    }
    aligned, withheld, alignment = align_extraction_exact_spans(
        route,
        model_extraction,
        text,
        endorsement_context_provenance=context_provenance,
        segment_start_char=0,
    )
    assert alignment["status"] == "complete"
    assert withheld == []
    validation = validate_extraction(
        route,
        aligned,
        text,
        "2.1",
        endorsement_context_provenance=context_provenance,
        segment_start_char=0,
        parent_source_text=text,
    )
    raw_output = json.dumps(model_extraction, ensure_ascii=False)
    _, output_report = canonicalize_json_string_controls(raw_output)
    display_provenance = inference_display_provenance(text)
    structural_whitespace = validate_raw_structural_whitespace(raw_output)
    guard = pre_alignment_model_item_guard_report(route, model_extraction)
    assert guard["pre_alignment_model_item_guard_validation_passed"] is True
    row = {
        "schema_version": "2.1",
        "document_id": segment_id,
        "parent_document_id": parent_id,
        "candidate_cycle_id": candidate_id,
        "candidate_icpsr": f"Candidate {candidate_id}",
        "state": "AA",
        "office": "house",
        "year": 2020,
        "party": "democrat",
        "route": route,
        "raw_text_sha256": raw_hash,
        "extraction_source_text_sha256": sha256_text(text),
        "extraction_source_n_chars": len(text),
        "source_text_transform_version": SOURCE_TEXT_TRANSFORM_VERSION,
        "source_text_transform_changed": False,
        "source_text": text,
        "n_raw_occurrences": 1,
        "first_capture_timestamp": "20200102010101",
        "last_capture_timestamp": "20200102010101",
        "source_urls": [_url(route)],
        "segment_index": 0,
        "segment_count": 1,
        "segment_start_char": 0,
        "segment_end_char": len(text),
        "segment_text_sha256": segment_hash,
        "parse_status": "parsed",
        "processing_status": "processed_success",
        "schema_valid": True,
        "validation": validation,
        "input_truncated": False,
        "input_chars_used": len(text),
        "input_tokens_used": 10,
        "finish_reason": "stop",
        "stop_reason": None,
        "output_tokens": 100,
        "max_output_tokens": EXPECTED_MAX_OUTPUT_TOKENS,
        "temperature": 0.0,
        "thinking_enabled": False,
        "compact_repair": False,
        "model_id": MODEL,
        "model_revision": REVISION,
        "runtime_lock_sha256": RUNTIME,
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
        "generation_config_sha256": GENERATION_HASH,
        "structured_decoding_backend": STRUCTURED_DECODING_BACKEND,
        "structured_decoding_disable_any_whitespace": (
            STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
        ),
        "model_extraction": model_extraction,
        "model_extraction_sha256": alignment["original_model_extraction_sha256"],
        "pre_alignment_model_item_guard": guard,
        "array_item_cap_report": guard["array_item_cap_report"],
        "model_item_repetition_report": guard["model_item_repetition_report"],
        "extraction": aligned,
        "aligned_extraction_sha256": alignment["aligned_extraction_sha256"],
        "withheld_model_items": withheld,
        "exact_alignment": alignment,
        "record_variants": [],
        "resolved_records": [],
    }
    if context_provenance is not None:
        row["endorsement_context_provenance"] = context_provenance
    row["user_message_sha256"] = sha256_text(
        build_user_message(row, text, "2.1")
    )
    return row


def _set_model_extraction(row: dict, model_extraction: dict) -> None:
    aligned, withheld, alignment = align_extraction_exact_spans(
        row["route"],
        model_extraction,
        row["source_text"],
        endorsement_context_provenance=row.get(
            "endorsement_context_provenance"
        ),
        segment_start_char=int(row["segment_start_char"]),
    )
    row["model_extraction"] = model_extraction
    guard = pre_alignment_model_item_guard_report(row["route"], model_extraction)
    row["pre_alignment_model_item_guard"] = guard
    row["array_item_cap_report"] = guard["array_item_cap_report"]
    row["model_item_repetition_report"] = guard[
        "model_item_repetition_report"
    ]
    row["model_extraction_sha256"] = alignment["original_model_extraction_sha256"]
    row["extraction"] = aligned
    row["aligned_extraction_sha256"] = alignment["aligned_extraction_sha256"]
    row["withheld_model_items"] = withheld
    row["exact_alignment"] = alignment
    row["validation"] = validate_extraction(
        row["route"],
        aligned,
        row["source_text"],
        "2.1",
        endorsement_context_provenance=row.get(
            "endorsement_context_provenance"
        ),
        segment_start_char=int(row["segment_start_char"]),
        parent_source_text=row["source_text"],
    )
    row["processing_status"] = {
        "complete": "processed_success",
        "partial_withheld": "processed_partial_withheld",
        "all_withheld": "processed_all_withheld",
        "invalid_schema": "failed_schema",
    }[alignment["status"]]
    raw_output = json.dumps(model_extraction, ensure_ascii=False)
    _, output_report = canonicalize_json_string_controls(raw_output)
    row["raw_output"] = raw_output
    row.update(output_report)
    row.update(validate_raw_structural_whitespace(raw_output))
    row.update(inference_display_provenance(row["source_text"]))
    row["user_message_sha256"] = sha256_text(
        build_user_message(row, row["source_text"], "2.1")
    )


def _run_metadata_entry(rows: list[dict]) -> dict:
    status_counts: dict[str, int] = {}
    finish_counts: dict[str, int] = {}
    canonicalization_counts: dict[str, int] = {}
    canonicalized_outputs = 0
    canonicalized_characters = 0
    model_items = aligned_items = withheld_items = 0
    strict_model_items = repetition_groups = max_multiplicity = 0
    parsed = schema_valid = strict_parsed = 0
    cap_failures = repetition_failures = 0
    route_model_item_max = {
        route: 0 for route in CONTRACT["route_array_item_caps"]
    }
    exactly_supported = 0
    for row in rows:
        finish = str(row["finish_reason"])
        finish_counts[finish] = finish_counts.get(finish, 0) + 1
        parse_status = str(row["parse_status"])
        parsed += int(parse_status == "parsed")
        validation = row.get("validation")
        if isinstance(validation, dict):
            schema_valid += int(validation.get("schema_valid") is True)
            exactly_supported += int(
                validation.get("schema_valid") is True
                and validation.get("n_items") == validation.get("n_supported")
            )
        alignment = row.get("exact_alignment")
        if isinstance(alignment, dict):
            status = str(alignment["status"])
            status_counts[status] = status_counts.get(status, 0) + 1
            model_items += int(alignment["model_item_count"])
            aligned_items += int(alignment["aligned_item_count"])
            withheld_items += int(alignment["withheld_item_count"])
        if parse_status in {
            "parsed",
            "array_item_cap_reached",
            "model_item_repetition_error",
        }:
            strict_parsed += 1
            guard_count = int(
                row["array_item_cap_report"]["model_item_count"]
            )
            strict_model_items += guard_count
            route_model_item_max[row["route"]] = max(
                route_model_item_max[row["route"]], guard_count
            )
            repetition_groups += int(
                row["model_item_repetition_report"][
                    "repeated_model_item_group_count"
                ]
            )
            max_multiplicity = max(
                max_multiplicity,
                int(
                    row["model_item_repetition_report"][
                        "max_exact_model_item_multiplicity"
                    ]
                ),
            )
        cap_failures += int(parse_status == "array_item_cap_reached")
        repetition_failures += int(
            parse_status == "model_item_repetition_error"
        )
        repaired = int(row["json_control_canonicalization_count"])
        canonicalized_outputs += int(repaired > 0)
        canonicalized_characters += repaired
        for key, count in row[
            "json_control_canonicalization_codepoint_counts"
        ].items():
            canonicalization_counts[key] = (
                canonicalization_counts.get(key, 0) + int(count)
            )
    return {
        "file": "run_metadata.json",
        "sha256": "5" * 64,
        "shard_index": None,
        "shard_count": None,
        "generation_config_sha256": GENERATION_HASH,
        "structured_decoding_backend": STRUCTURED_DECODING_BACKEND,
        "structured_decoding_disable_any_whitespace": (
            STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
        ),
        "array_item_cap_spec_id": CONTRACT["array_item_cap_spec_id"],
        "array_item_cap_spec_sha256": CONTRACT["array_item_cap_spec_sha256"],
        "route_array_item_caps": CONTRACT["route_array_item_caps"],
        "model_item_repetition_spec_id": CONTRACT[
            "model_item_repetition_spec_id"
        ],
        "model_item_repetition_spec_sha256": CONTRACT[
            "model_item_repetition_spec_sha256"
        ],
        "recovery_attestation_without_inference": False,
        "documents_selected_for_shard": len(rows),
        "documents_processed_this_run": len(rows),
        "documents_parsed_this_run": parsed,
        "documents_schema_valid_this_run": schema_valid,
        "documents_exactly_supported_this_run": exactly_supported,
        "finish_reason_counts_this_run": dict(sorted(finish_counts.items())),
        "output_tokens_total_this_run": sum(
            int(row["output_tokens"]) for row in rows
        ),
        "output_tokens_max_this_run": max(
            (int(row["output_tokens"]) for row in rows), default=0
        ),
        "model_items_parsed_this_run": model_items,
        "aligned_items_this_run": aligned_items,
        "withheld_items_this_run": withheld_items,
        "strict_json_parsed_outputs_this_run": strict_parsed,
        "model_items_strictly_parsed_this_run": strict_model_items,
        "array_item_cap_reached_outputs_this_run": cap_failures,
        "model_item_repetition_error_outputs_this_run": repetition_failures,
        "model_item_repetition_groups_this_run": repetition_groups,
        "max_exact_model_item_multiplicity_this_run": max_multiplicity,
        "route_model_item_count_max_this_run": dict(
            sorted(route_model_item_max.items())
        ),
        "alignment_status_counts_this_run": dict(sorted(status_counts.items())),
        "json_control_canonicalized_outputs_this_run": canonicalized_outputs,
        "json_control_canonicalized_characters_this_run": (
            canonicalized_characters
        ),
        "json_control_canonicalization_codepoint_counts_this_run": dict(
            sorted(canonicalization_counts.items())
        ),
    }


def _refresh_private_provenance(manifest: dict, rows: list[dict]) -> None:
    manifest["private_output_provenance_summary"] = (
        output_provenance_summary(rows)
    )
    manifest["all_output_aggregate"] = all_output_aggregate(rows)
    entry = _run_metadata_entry(rows)
    manifest["run_metadata"] = [entry]
    manifest["run_metadata_aggregate"] = run_metadata_aggregate([entry])


def _resolve_rows(rows: list[dict]) -> tuple[int, int, int, dict[str, int]]:
    by_parent: dict[str, list[dict]] = {}
    for row in rows:
        row["record_variants"] = []
        row["resolved_records"] = []
        by_parent.setdefault(row["parent_document_id"], []).append(row)
    conflict_groups: set[str] = set()
    resolved_count = 0
    variant_count = 0
    route_counts: dict[str, int] = {}
    for parent_id, parent_rows in by_parent.items():
        route = parent_rows[0]["route"]
        variants = build_record_variants(parent_rows)
        annotated, resolved = resolve_parent_variants(parent_id, route, variants)
        by_id = {row["document_id"]: row for row in parent_rows}
        for variant in annotated:
            by_id[variant["segment_id"]]["record_variants"].append(variant)
            if variant["overlap_resolution_status"] == "conflict_withheld":
                conflict_groups.add(variant["overlap_group_id"])
        for record in resolved:
            by_id[record["canonical_segment_id"]]["resolved_records"].append(record)
            route_counts[route] = route_counts.get(route, 0) + 1
        variant_count += len(annotated)
        resolved_count += len(resolved)
    return variant_count, resolved_count, len(conflict_groups), route_counts


def _human_validation(omission_items: list[dict]) -> dict:
    return {
        "schema_version": "2.1",
        "status": "passed",
        "independent_human_only": True,
        "coding_source": "human",
        "human_only_attestation": "confirmed_at_scoring",
        "tuning_audit_excluded": True,
        "technical_gate_passed": True,
        "gates_passed": True,
        "known_omission_human_confirmation": {
            "expected_n": 11,
            "confirmed_n": 11,
            "all_confirmed": True,
            "items": [
                {
                    "omission_id": item["omission_id"],
                    "parent_document_id": item["parent_document_id"],
                    "route": item["route"],
                    "matched_record_ids": item["matched_record_ids"],
                    "confirmed_record_ids": item["matched_record_ids"],
                    "semantic_recovery_confirmed": True,
                    "record_link_confirmations": [
                        {
                            "record_id": record_id,
                            "semantic_recovery_confirmed": True,
                            "recovery_evidence_confirmed": True,
                            "pair_confirmed": True,
                        }
                        for record_id in item["matched_record_ids"]
                    ],
                }
                for item in omission_items
            ],
        },
        "routes": {
            "issues": {
                "precision_n": 150,
                "all_dimension_accuracy": 0.91,
                "minimum_subtype_accuracy": 0.80,
                "recall_n": 40,
                "complete_page_recall": 0.90,
                "passed": True,
            },
            "biography": {
                "precision_n": 180,
                "all_dimension_accuracy": 0.91,
                "minimum_subtype_accuracy": 0.80,
                "recall_n": 40,
                "complete_page_recall": 0.90,
                "passed": True,
            },
            "endorsements": {
                "precision_n": 180,
                "all_dimension_accuracy": 0.91,
                "minimum_subtype_accuracy": 0.80,
                "recall_n": 40,
                "complete_page_recall": 0.90,
                "sampled_presence_rate": 1.0,
                "sampled_combined_support_rate": 1.0,
                "nonnull_optional_fields": {
                    "title": 20,
                    "affiliation": 20,
                    "jurisdiction": 20,
                },
                "passed": True,
            },
        },
    }


def test_human_omission_gate_allows_rejecting_false_span_candidates(
    tmp_path: Path,
) -> None:
    omission_items = [
        {
            "omission_id": f"omission-{index}",
            "parent_document_id": f"parent-{index}",
            "route": "biography" if index < 2 else "issues",
            "matched_record_ids": [f"record-{index}"],
        }
        for index in range(11)
    ]
    omission_items[0]["matched_record_ids"] = ["record-0", "false-candidate"]
    validation = _human_validation(omission_items)
    first = validation["known_omission_human_confirmation"]["items"][0]
    first["confirmed_record_ids"] = ["record-0"]
    for link in first["record_link_confirmations"]:
        if link["record_id"] == "false-candidate":
            link["semantic_recovery_confirmed"] = False
            link["recovery_evidence_confirmed"] = True
            link["pair_confirmed"] = False
    validation_path = tmp_path / "human_validation.json"
    validation_path.write_text(json.dumps(validation))
    report = {"items": omission_items}
    _, route_passed = _load_human_validation(validation_path, report)
    assert all(route_passed.values())


def test_parent_and_url_route_endorsement_contexts_are_publicly_explicit() -> None:
    parent_text = "ENDORSEMENTS\nAlice Example"
    segment_text = "Alice Example"
    segment_start = parent_text.index(segment_text)
    context = build_endorsement_context_provenance(
        parent_document_id="parent-context",
        parent_source_text=parent_text,
        source_urls=[_url("endorsements")],
        segment_id="segment-context",
        segment_start_char=segment_start,
        segment_end_char=len(parent_text),
    )
    assert context["source_scope"] == "parent_document"
    model = {"endorsements": [{
        "endorser_name": "Alice Example",
        "entity_type": "individual",
        "endorser_category": "private_individual",
        "title": None,
        "affiliation": None,
        "jurisdiction": None,
        "record_evidence": "Alice Example",
        "endorsement_context_evidence": "ENDORSEMENTS",
        "support_mode": "page_or_section_context",
    }]}
    aligned, withheld, _ = align_extraction_exact_spans(
        "endorsements",
        model,
        segment_text,
        endorsement_context_provenance=context,
        segment_start_char=segment_start,
    )
    assert withheld == []
    item = aligned["endorsements"][0]
    valid = _validate_item(
        "endorsements",
        item,
        segment_text,
        "parent-context-item",
        endorsement_context_provenance=context,
        segment_start_char=segment_start,
        parent_source_text=parent_text,
    )
    public = _parent_payload("endorsements", valid, segment_start)
    assert public["endorsement_context_evidence_start_char"] is None
    assert public["endorsement_context_evidence_parent_start_char"] == 0
    assert public["endorsement_context_evidence_alignment_method"] == (
        "provided_exact_parent_span"
    )

    fallback_text = "Organizations\nAlice Example"
    fallback = build_endorsement_context_provenance(
        parent_document_id="parent-fallback",
        parent_source_text=fallback_text,
        source_urls=[_url("endorsements")],
        segment_id="segment-fallback",
        segment_start_char=0,
        segment_end_char=len(fallback_text),
    )
    assert fallback["source_scope"] == "page_or_section_route_context"
    fallback_model = deepcopy(model)
    fallback_model["endorsements"][0]["endorsement_context_evidence"] = None
    aligned, withheld, _ = align_extraction_exact_spans(
        "endorsements",
        fallback_model,
        fallback_text,
        endorsement_context_provenance=fallback,
        segment_start_char=0,
    )
    assert withheld == []
    fallback_item = aligned["endorsements"][0]
    valid = _validate_item(
        "endorsements",
        fallback_item,
        fallback_text,
        "route-context-item",
        endorsement_context_provenance=fallback,
        segment_start_char=0,
        parent_source_text=fallback_text,
    )
    public = _parent_payload("endorsements", valid, 0)
    assert public["endorsement_context_evidence"] is None
    assert public["endorsement_context_evidence_parent_start_char"] is None
    assert public["endorsement_context_evidence_alignment_method"] == (
        "page_or_section_route_context"
    )
    assert public["endorsement_context_qualifying_wayback_url"] == _url(
        "endorsements"
    )
    assert public["endorsement_context_qualifying_original_path"] == (
        "/endorsements"
    )


def test_line_ledger_record_fields_are_exact_and_private_payloads_are_rejected() -> None:
    source = "ENDORSEMENTS\nAlice Example"
    context = build_endorsement_context_provenance(
        parent_document_id="parent-ledger",
        parent_source_text=source,
        source_urls=[_url("endorsements")],
        segment_id="a" * 64,
        segment_start_char=0,
        segment_end_char=len(source),
    )
    model = {"endorsements": [{
        "endorser_name": "Alice Example",
        "entity_type": "individual",
        "endorser_category": "private_individual",
        "title": None,
        "affiliation": None,
        "jurisdiction": None,
        "record_evidence": "Alice Example",
        "endorsement_context_evidence": "ENDORSEMENTS",
        "support_mode": "page_or_section_context",
    }]}
    aligned, withheld, _ = align_extraction_exact_spans(
        "endorsements",
        model,
        source,
        endorsement_context_provenance=context,
        segment_start_char=0,
    )
    assert withheld == []
    item = deepcopy(aligned["endorsements"][0])
    record_start = int(item["record_evidence_start_char"])
    record_end = int(item["record_evidence_end_char"])
    item.update({
        "line_inventory_id": "1" * 64,
        "source_line_id": "2" * 64,
        "stage_a_batch_id": "3" * 64,
        "stage_a_slot": "slot_00",
        "stage_b_batch_id": "4" * 64,
        "stage_b_slot": "slot_00",
        "model_record_index": 0,
        "carrier_alignment_source_model_item_id": "5" * 64,
        "inventory_owner_segment_id": "6" * 64,
        "record_evidence_carrier_segment_id": "a" * 64,
        "target_block_alignment_method": (
            "unique_exact_target_block_substring_v1"
        ),
        "target_block_exact_match_count": 1,
        "record_evidence_parent_start_char": record_start,
        "record_evidence_parent_end_char": record_end,
    })
    valid = _validate_item(
        "endorsements",
        item,
        source,
        "ledger-item",
        endorsement_context_provenance=context,
        segment_start_char=0,
        parent_source_text=source,
        line_ledger_item=True,
    )
    assert valid["record_evidence_parent_start_char"] == record_start
    assert valid["target_block_exact_match_count"] == 1
    assert set(ENDORSEMENT_COLUMNS).isdisjoint({
        "model_output", "stage_alignment", "withheld_records",
        "target_blocks", "line_decisions", "raw_output",
    })

    for field, value, message in (
        ("target_block_exact_match_count", True, "integer|exact-match count"),
        ("record_evidence_parent_start_char", record_start + 1, "parent offsets"),
        ("stage_b_slot", "slot_08", "stage_b_slot"),
    ):
        tampered = deepcopy(item)
        tampered[field] = value
        with pytest.raises(ReleaseInputError, match=message):
            _validate_item(
                "endorsements",
                tampered,
                source,
                "ledger-item",
                endorsement_context_provenance=context,
                segment_start_char=0,
                parent_source_text=source,
                line_ledger_item=True,
            )
    private = deepcopy(item)
    private["model_output"] = {"slot_00": {"records": []}}
    with pytest.raises(ReleaseInputError, match="fields differ"):
        _validate_item(
            "endorsements",
            private,
            source,
            "ledger-item",
            endorsement_context_provenance=context,
            segment_start_char=0,
            parent_source_text=source,
            line_ledger_item=True,
        )


def test_two_stage_singular_provenance_is_null_only_for_endorsements() -> None:
    ledger_contract = get_line_ledger_contract()
    metadata = {
        "schema_version": CONTRACT["schema_version"],
        "model_id": CONTRACT["model_id"],
        "model_revision": CONTRACT["model_revision"],
        "runtime_lock_sha256": CONTRACT["runtime_lock_sha256"],
        "prompt_sha256": CONTRACT["prompt_sha256"],
        "json_schema_sha256": CONTRACT["json_schema_sha256"],
        "extraction_contract_sha256": CONTRACT[
            "extraction_contract_sha256"
        ],
        "generation_config_sha256": GENERATION_HASH,
        "alignment_id": CONTRACT["alignment_id"],
        "alignment_spec_sha256": CONTRACT["alignment_spec_sha256"],
        "semantic_validation_spec_sha256": CONTRACT[
            "semantic_validation_spec_sha256"
        ],
        "endorsement_context_provenance_spec_sha256": CONTRACT[
            "endorsement_context_provenance_spec_sha256"
        ],
        "user_message_template_id": CONTRACT["user_message_template_id"],
        "user_message_template_sha256": CONTRACT[
            "user_message_template_sha256"
        ],
        "endorsement_route_contract_sha256": ledger_contract[
            "line_ledger_contract_sha256"
        ],
    }
    singular_fields = {
        "prompt_sha256", "json_schema_sha256", "generation_config_sha256",
        "alignment_id", "alignment_spec_sha256", "user_message_template_id",
        "user_message_template_sha256",
    }
    for route in ("issues", "biography"):
        public = _public_metadata(metadata, route)
        assert public["prompt_sha256"] == CONTRACT["prompt_sha256"][route]
        assert public["json_schema_sha256"] == CONTRACT[
            "json_schema_sha256"
        ][route]
        assert public["generation_config_sha256"] == GENERATION_HASH
        assert public["alignment_id"] == CONTRACT["alignment_id"]
        assert public["alignment_spec_sha256"] == CONTRACT[
            "alignment_spec_sha256"
        ]
        assert public["user_message_template_id"] == CONTRACT[
            "user_message_template_id"
        ]
        assert public["user_message_template_sha256"] == CONTRACT[
            "user_message_template_sha256"
        ]
        assert public["extraction_contract_sha256"] == CONTRACT[
            "extraction_contract_sha256"
        ]
    endorsement = _public_metadata(metadata, "endorsements")
    assert all(endorsement[field] is None for field in singular_fields)
    assert endorsement["extraction_contract_sha256"] == ledger_contract[
        "line_ledger_contract_sha256"
    ]
    assert endorsement["model_id"] == CONTRACT["model_id"]
    assert endorsement["model_revision"] == CONTRACT["model_revision"]
    assert endorsement["runtime_lock_sha256"] == CONTRACT[
        "runtime_lock_sha256"
    ]
    assert endorsement[
        "endorsement_context_provenance_spec_sha256"
    ] == CONTRACT["endorsement_context_provenance_spec_sha256"]

    canonicalization = {
        "json_control_canonicalization_applied": False,
        "json_control_canonicalization_count": 0,
    }
    for route in ("issues", "biography"):
        output = _public_segment_output_provenance(
            route,
            line_ledger_enabled=True,
            output_tokens=123,
            max_output_tokens=456,
            canonicalization_report=canonicalization,
        )
        assert output == {
            "output_tokens": 123,
            "max_output_tokens": 456,
            "json_control_canonicalization_applied": False,
            "json_control_canonicalization_count": 0,
        }
    assert _public_segment_output_provenance(
        "endorsements",
        line_ledger_enabled=True,
        output_tokens=123,
        max_output_tokens=456,
        canonicalization_report=canonicalization,
    ) == {
        "output_tokens": None,
        "max_output_tokens": None,
        "json_control_canonicalization_applied": None,
        "json_control_canonicalization_count": None,
    }

    issue_segment = {
        "segment_id": "issue-segment",
        "route": "issues",
        **_public_metadata(metadata, "issues"),
        **_public_segment_output_provenance(
            "issues",
            line_ledger_enabled=True,
            output_tokens=123,
            max_output_tokens=456,
            canonicalization_report=canonicalization,
        ),
    }
    endorsement_segment = {
        "segment_id": "endorsement-segment",
        "route": "endorsements",
        **endorsement,
        **_public_segment_output_provenance(
            "endorsements",
            line_ledger_enabled=True,
            output_tokens=123,
            max_output_tokens=456,
            canonicalization_report=canonicalization,
        ),
    }
    _validate_public_route_provenance(
        [issue_segment, endorsement_segment],
        table_name="candidate_extraction_segments.parquet",
        two_stage_endorsements=True,
    )
    bad_issue = deepcopy(issue_segment)
    bad_issue["prompt_sha256"] = None
    with pytest.raises(AssertionError, match="one-stage route provenance"):
        _validate_public_route_provenance(
            [bad_issue],
            table_name="candidate_extraction_segments.parquet",
            two_stage_endorsements=True,
        )
    bad_endorsement = deepcopy(endorsement_segment)
    bad_endorsement["prompt_sha256"] = CONTRACT["prompt_sha256"][
        "endorsements"
    ]
    with pytest.raises(AssertionError, match="superseded singular"):
        _validate_public_route_provenance(
            [bad_endorsement],
            table_name="candidate_extraction_segments.parquet",
            two_stage_endorsements=True,
        )


def _write_fixture(tmp_path: Path) -> tuple[Path, Path, list[dict]]:
    core = tmp_path / "core"
    bundle = tmp_path / "bundle"
    core.mkdir(parents=True)
    bundle.mkdir(parents=True)
    candidates = ["H0AA00001-2020", "H0AA00002-2020"]
    pd.DataFrame({"candidate_cycle_id": candidates}).to_csv(
        core / "candidate_year_universe.csv", index=False
    )

    issue_text = "Jobs\nWe support good jobs."
    issue_evidence = "We support good jobs."
    issue_start = issue_text.index(issue_evidence)
    biography_text = "Alex formerly served as mayor."
    biography_evidence = "formerly served as mayor"
    biography_start = biography_text.index(biography_evidence)
    endorsement_text = "We are proud to endorse Jane Roe."
    record_evidence = endorsement_text
    record_start = endorsement_text.index(record_evidence)
    context_evidence = "endorse Jane Roe"
    context_start = endorsement_text.index(context_evidence)
    zero_text = "Issues"

    raw_rows = [
        _raw_row(candidates[0], "issues", issue_text),
        _raw_row(candidates[0], "biography", biography_text),
        _raw_row(candidates[0], "endorsements", endorsement_text),
        _raw_row(candidates[1], "issues", zero_text),
        _raw_row(candidates[1], "biography", "   "),
    ]
    pq.write_table(pa.Table.from_pylist(raw_rows), core / "raw_corpus.parquet")

    rows = [
        _segment_row(candidates[0], "issues", issue_text, {
            "issue_blocks": [{
                "source_heading": "Jobs",
                "source_heading_start_char": 0,
                "source_heading_end_char": 4,
                "claim": "The candidate supports good jobs.",
                "statement_kind": "support",
                "evidence": issue_evidence,
                "evidence_start_char": issue_start,
                "evidence_end_char": issue_start + len(issue_evidence),
            }],
        }),
        _segment_row(candidates[0], "biography", biography_text, {
            "facts": [{
                "field": "elected_office_experience",
                "value": "Mayor",
                "timing": "former",
                "relationship": None,
                "evidence": biography_evidence,
                "evidence_start_char": biography_start,
                "evidence_end_char": biography_start + len(biography_evidence),
            }],
        }),
        _segment_row(candidates[0], "endorsements", endorsement_text, {
            "endorsements": [{
                "endorser_name": "Jane Roe",
                "entity_type": "individual",
                "endorser_category": "private_individual",
                "title": None,
                "affiliation": None,
                "jurisdiction": None,
                "record_evidence": record_evidence,
                "record_evidence_start_char": record_start,
                "record_evidence_end_char": record_start + len(record_evidence),
                "endorsement_context_evidence": context_evidence,
                "endorsement_context_evidence_start_char": context_start,
                "endorsement_context_evidence_end_char": context_start + len(context_evidence),
                "support_mode": "explicit_text",
            }],
        }),
        _segment_row(candidates[1], "issues", zero_text, {"issue_blocks": []}),
    ]
    raw_variants, resolved_records, conflicts, resolved_by_route = _resolve_rows(rows)
    segments_path = bundle / "segments.jsonl.gz"
    with gzip.open(segments_path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    omission_path = bundle / "known_omission_recovery.json"
    biography_record = rows[1]["resolved_records"][0]
    first_record_id = biography_record["record_id"]
    omission_items = [
        {
            "omission_id": f"o{index}",
            "parent_document_id": rows[1]["parent_document_id"],
            "candidate_cycle_id": candidates[0],
            "route": "biography",
            "expected_type": "elected_office_experience",
            "expected_value": "Mayor",
            "evidence": biography_evidence,
            "source_evidence_exact_occurrences": 1,
            "recovered": True,
            "matched_record_ids": [first_record_id],
            "human_confirmation_required": True,
            "expected_value_confirmation_required": True,
            "human_confirmed_semantic_recovery": None,
        }
        for index in range(11)
    ]
    omission_report = {
        "known_omissions": 11,
        "recovered": 11,
        "all_recovered": True,
        "human_confirmation_required": True,
        "human_confirmation_status": "pending",
        "matching_spec": KNOWN_OMISSION_MATCHING_SPEC,
        "matching_spec_sha256": KNOWN_OMISSION_MATCHING_SPEC_SHA256,
        "matching_rule": "fixture",
        "items": omission_items,
    }
    omission_path.write_text(json.dumps(omission_report, sort_keys=True), encoding="utf-8")
    validation_path = bundle / "human_validation.json"
    validation_payload = _human_validation(omission_items)
    validation_payload["source_corpus_sha256"] = sha256_file(
        core / "raw_corpus.parquet"
    )
    validation_path.write_text(
        json.dumps(validation_payload, sort_keys=True), encoding="utf-8"
    )
    route_counts = pd.Series([row["route"] for row in rows]).value_counts().to_dict()
    model_items = sum(row["exact_alignment"]["model_item_count"] for row in rows)
    aligned_items = sum(row["exact_alignment"]["aligned_item_count"] for row in rows)
    withheld_items = sum(row["exact_alignment"]["withheld_item_count"] for row in rows)
    manifest = {
        "schema_version": "2.1",
        "release_status": "human_validation_passed",
        "human_validation_gate_passed": True,
        "source_corpus_sha256": sha256_file(core / "raw_corpus.parquet"),
        "source_export_manifest_sha256": "4" * 64,
        "source_export_upstream_full_manifest_sha256": None,
        "source_text_transform": {
            "version": SOURCE_TEXT_TRANSFORM_VERSION,
            "rule": SOURCE_TEXT_TRANSFORM_RULE,
            "offset_coordinate_system": OFFSET_COORDINATE_SYSTEM,
            "transformed_universe_parent_documents": 0,
            "transformed_exported_parent_documents": 0,
        },
        "endorsement_context_provenance": {
            "version": ENDORSEMENT_CONTEXT_PROVENANCE_VERSION,
            "provenance_spec_sha256": (
                ENDORSEMENT_CONTEXT_PROVENANCE_SPEC_SHA256
            ),
            "url_classifier_id": ENDORSEMENT_CONTEXT_CLASSIFIER_ID,
            "url_classifier_spec_sha256": (
                ENDORSEMENT_CONTEXT_CLASSIFIER_SPEC_SHA256
            ),
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
                "segment", "parent_document",
                "page_or_section_route_context",
            ],
            "segment_complete_line_census": (
                endorsement_segment_complete_line_census(rows)
            ),
        },
        "endorsement_context_selected_segment_complete_line_census": (
            endorsement_segment_complete_line_census(rows)
        ),
        "model_id": MODEL,
        "model_revision": REVISION,
        "runtime_lock_sha256": RUNTIME,
        "prompt_sha256": CONTRACT["prompt_sha256"],
        "json_schema_sha256": CONTRACT["json_schema_sha256"],
        "extraction_contract_sha256": CONTRACT["extraction_contract_sha256"],
        "alignment_id": CONTRACT["alignment_id"],
        "alignment_spec_sha256": CONTRACT["alignment_spec_sha256"],
        "semantic_validation_spec_sha256": CONTRACT["semantic_validation_spec_sha256"],
        "endorsement_context_provenance_spec_sha256": CONTRACT[
            "endorsement_context_provenance_spec_sha256"
        ],
        "user_message_template_id": CONTRACT["user_message_template_id"],
        "user_message_template_sha256": CONTRACT["user_message_template_sha256"],
        "inference_display_escape_id": CONTRACT[
            "inference_display_escape_id"
        ],
        "inference_display_escape_spec_sha256": CONTRACT[
            "inference_display_escape_spec_sha256"
        ],
        "json_string_control_canonicalizer_id": CONTRACT[
            "json_string_control_canonicalizer_id"
        ],
        "json_string_control_canonicalizer_spec_sha256": CONTRACT[
            "json_string_control_canonicalizer_spec_sha256"
        ],
        "array_item_cap_spec_id": CONTRACT["array_item_cap_spec_id"],
        "array_item_cap_spec_sha256": CONTRACT["array_item_cap_spec_sha256"],
        "route_array_item_caps": CONTRACT["route_array_item_caps"],
        "model_item_repetition_spec_id": CONTRACT[
            "model_item_repetition_spec_id"
        ],
        "model_item_repetition_spec_sha256": CONTRACT[
            "model_item_repetition_spec_sha256"
        ],
        "generation_config": GENERATION_CONFIG,
        "generation_config_sha256": GENERATION_HASH,
        "structured_decoding_backend": STRUCTURED_DECODING_BACKEND,
        "structured_decoding_disable_any_whitespace": (
            STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
        ),
        "parent_documents": len(rows),
        "segments": len(rows),
        "parent_documents_by_route": route_counts,
        "segments_by_route": route_counts,
        "raw_record_variants": raw_variants,
        "model_items": model_items,
        "aligned_items": aligned_items,
        "withheld_model_items": withheld_items,
        "resolved_records": resolved_records,
        "resolved_records_by_route": resolved_by_route,
        "withheld_overlap_conflict_groups": conflicts,
        "segment_processing_status": {"processed_success": len(rows)},
        "technical_segment_gate_passed": True,
        "known_omission_gate": True,
        "known_omission_matching_spec_sha256": (
            KNOWN_OMISSION_MATCHING_SPEC_SHA256
        ),
        "run_metadata": [],
        "segments_file": {
            "file": segments_path.name,
            "rows": len(rows),
            "sha256": sha256_file(segments_path),
        },
        "human_validation_file": {
            "file": validation_path.name,
            "sha256": sha256_file(validation_path),
        },
        "known_omission_file": {
            "file": omission_path.name,
            "rows": 11,
            "sha256": sha256_file(omission_path),
        },
    }
    _refresh_private_provenance(manifest, rows)
    (bundle / "bundle_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    return core, bundle, rows


def test_builds_and_validates_all_six_public_outputs(tmp_path: Path) -> None:
    core, bundle, _ = _write_fixture(tmp_path)
    release = tmp_path / "release"
    summary = build_release(core, bundle, release, production=False)
    # The packager writes the six public files plus its own inventory, which the
    # validator reads through --manifest.
    assert set(path.name for path in release.iterdir()) == set(PUBLIC_FILES) | {
        EXTENSION_MANIFEST_FILENAME
    }
    assert summary["manifest_file"] == EXTENSION_MANIFEST_FILENAME
    assert json.loads(
        (release / EXTENSION_MANIFEST_FILENAME).read_text(encoding="utf-8")
    ) == summary["manifest_entries"]
    assert summary["release_ready"] is True
    assert summary["generation_config_sha256"] == GENERATION_HASH

    status = pd.read_csv(release / "candidate_extraction_status.csv")
    assert len(status) == 6
    assert status.extraction_status.value_counts().to_dict() == {
        "not_extracted_no_source": 2,
        "observed_positive": 3,
        "observed_zero": 1,
    }
    documents = pq.read_table(release / "candidate_extraction_documents.parquet")
    segments = pq.read_table(release / "candidate_extraction_segments.parquet")
    assert len(documents) == len(segments) == 4
    assert "source_text" not in documents.column_names
    assert "source_text" not in segments.column_names
    assert segments.column(
        "inference_display_escape_id"
    ).to_pylist() == [CONTRACT["inference_display_escape_id"]] * 4
    assert segments.column(
        "json_string_control_canonicalizer_id"
    ).to_pylist() == [
        CONTRACT["json_string_control_canonicalizer_id"]
    ] * 4
    assert segments.column(
        "json_control_canonicalization_count"
    ).to_pylist() == [0] * 4
    for private_field in (
        "raw_output", "raw_output_sha256", "canonical_output_sha256",
        "inference_display_text_sha256", "user_message_sha256",
        "display_anchor_adjustments", "endorsement_context_extensions",
        "json_control_canonicalization_codepoint_counts",
    ):
        assert private_field not in segments.column_names
    segment_rows = segments.to_pylist()
    endorsement_segment = next(
        row for row in segment_rows if row["route"] == "endorsements"
    )
    assert endorsement_segment["segment_starts_mid_line"] is False
    assert endorsement_segment["segment_has_complete_line"] is True
    assert endorsement_segment["segment_ends_mid_line"] is False
    assert endorsement_segment[
        "segment_first_complete_line_start_char"
    ] == 0
    assert endorsement_segment[
        "segment_first_complete_line_parent_start_char"
    ] == 0
    assert endorsement_segment[
        "segment_first_complete_line_inference_display_start_char"
    ] == 0
    endorsement_length = (
        endorsement_segment["segment_end_char"]
        - endorsement_segment["segment_start_char"]
    )
    assert endorsement_segment[
        "segment_last_complete_line_end_char"
    ] == endorsement_length
    assert endorsement_segment[
        "segment_last_complete_line_parent_end_char"
    ] == endorsement_segment["segment_end_char"]
    assert endorsement_segment[
        "segment_last_complete_line_inference_display_end_char"
    ] == endorsement_length
    assert all(
        row["segment_has_complete_line"] is None
        for row in segment_rows
        if row["route"] != "endorsements"
    )
    assert "wayback_urls" in documents.column_names
    assert "candidate_icpsr" in documents.column_names
    assert "candidate_name" not in documents.column_names
    assert "source_urls" not in documents.column_names
    assert all(
        url.startswith("https://web.archive.org/web/")
        for values in documents.column("wayback_urls").to_pylist()
        for url in values
    )
    assert all(
        url.startswith("https://example.org/")
        for values in documents.column("original_urls").to_pylist()
        for url in values
    )
    endorsements = pq.read_table(release / "candidate_endorsements.parquet")
    assert "organization" not in endorsements.column_names
    assert endorsements.column("record_evidence_parent_start_char").to_pylist() == [0]
    assert endorsements.column("record_evidence_exact_match_count").to_pylist() == [1]

    manifest_path = release / EXTENSION_MANIFEST_FILENAME
    validated = validate_candidate_extraction_release(
        release, core, bundle, manifest_path, production=False
    )
    assert validated["release_ready"] is True
    assert validated["generation_config_sha256"] == GENERATION_HASH
    assert validated["rows"]["candidate_extraction_status.csv"] == 6


@pytest.mark.parametrize("field,mistype", [("rows", True), ("columns", "text"), ("bytes", "text")])
def test_release_manifest_requires_typed_integer_counts(
    tmp_path: Path, field: str, mistype: object
) -> None:
    core, bundle, _ = _write_fixture(tmp_path)
    release = tmp_path / "release"
    build_release(core, bundle, release, production=False)
    entries = extension_manifest_entries(release)
    target = next(
        row for row in entries if row["file"] == "candidate_issue_statements.parquet"
    )
    target[field] = (
        mistype if mistype is True else str(target[field])
    )
    manifest_path = release / EXTENSION_MANIFEST_FILENAME
    manifest_path.write_text(json.dumps(entries), encoding="utf-8")
    with pytest.raises(AssertionError, match=f"manifest {field} differs"):
        validate_candidate_extraction_release(
            release,
            core,
            bundle,
            manifest_path,
            production=False,
        )


def test_release_record_gate_rejects_blank_exact_anchors(
    tmp_path: Path,
) -> None:
    _, _, rows = _write_fixture(tmp_path)
    list_keys = {
        "issues": "issue_blocks",
        "biography": "facts",
        "endorsements": "endorsements",
    }
    cases = (
        ("issues", "evidence"),
        ("biography", "evidence"),
        ("endorsements", "record_evidence"),
        ("endorsements", "endorsement_context_evidence"),
    )
    for route, anchor_field in cases:
        row = next(value for value in rows if value["route"] == route)
        for blank in ("", " \t\n"):
            item = deepcopy(row["extraction"][list_keys[route]][0])
            item[anchor_field] = blank
            with pytest.raises(
                ReleaseInputError,
                match="fails v2.1 exact-alignment",
            ):
                _validate_item(
                    route,
                    item,
                    row["source_text"],
                    f"{route}-{anchor_field}",
                    endorsement_context_provenance=row.get(
                        "endorsement_context_provenance"
                    ),
                    segment_start_char=int(row["segment_start_char"]),
                    parent_source_text=row["source_text"],
                )


def test_release_validator_rejects_blank_public_anchor_bytes(
    tmp_path: Path,
) -> None:
    core, bundle, _ = _write_fixture(tmp_path)
    release = tmp_path / "release"
    build_release(core, bundle, release, production=False)
    issue_path = release / "candidate_issue_statements.parquet"
    original = pq.read_table(issue_path)
    evidence_index = original.schema.get_field_index("evidence")
    evidence_field = original.schema.field(evidence_index)

    for blank in ("", " \t\n"):
        tampered = original.set_column(
            evidence_index,
            evidence_field,
            pa.array([blank], type=evidence_field.type),
        )
        pq.write_table(tampered, issue_path)
        manifest_path = release / EXTENSION_MANIFEST_FILENAME
        manifest_path.write_text(
            json.dumps(extension_manifest_entries(release)),
            encoding="utf-8",
        )
        with pytest.raises(
            AssertionError,
            match="values differ from the frozen bundle",
        ):
            validate_candidate_extraction_release(
                release,
                core,
                bundle,
                manifest_path,
                production=False,
            )


@pytest.mark.parametrize(
    "private_field",
    [
        "inference_display_source_text_sha256",
        "inference_display_source_n_chars",
        "inference_display_escape_count",
        "inference_display_escape_codepoint_counts",
        "inference_display_interior_boundary_count",
        "inference_display_boundary_map_sha256",
        "inference_display_roundtrip_valid",
        "user_message_sha256",
        "display_anchor_adjustments",
        "endorsement_context_extensions",
    ],
)
def test_release_validator_forbids_new_private_display_fields(
    tmp_path: Path,
    private_field: str,
) -> None:
    path = tmp_path / "tampered.parquet"
    tampered = pa.table({private_field: ["private"]})
    pq.write_table(tampered, path)
    with pytest.raises(AssertionError, match="exposes a forbidden field"):
        _validate_arrow_table(path, tampered)


def test_refuses_a_bundle_raw_corpus_hash_mismatch(tmp_path: Path) -> None:
    core, bundle, _ = _write_fixture(tmp_path)
    manifest_path = bundle / "bundle_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["source_corpus_sha256"] = "f" * 64
    validation_path = bundle / "human_validation.json"
    validation = json.loads(validation_path.read_text())
    validation["source_corpus_sha256"] = "f" * 64
    validation_path.write_text(json.dumps(validation, sort_keys=True))
    manifest["human_validation_file"]["sha256"] = sha256_file(validation_path)
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ReleaseInputError, match="raw-corpus hash differs"):
        build_release(core, bundle, tmp_path / "release", production=False)


def test_production_consumers_require_frozen_full_source_identity(
    tmp_path: Path,
) -> None:
    _, bundle, _ = _write_fixture(tmp_path)
    manifest_path = bundle / "bundle_manifest.json"
    validation_path = bundle / "human_validation.json"
    manifest = json.loads(manifest_path.read_text())
    validation = json.loads(validation_path.read_text())
    manifest["source_corpus_sha256"] = FROZEN_SUFFIX_P_SOURCE_CORPUS_SHA256
    manifest["source_export_manifest_sha256"] = (
        FROZEN_SUFFIX_P_FULL_SOURCE_MANIFEST_SHA256
    )
    manifest["source_export_upstream_full_manifest_sha256"] = None
    validation["source_corpus_sha256"] = FROZEN_SUFFIX_P_SOURCE_CORPUS_SHA256
    validation_path.write_text(json.dumps(validation, sort_keys=True))
    manifest["human_validation_file"]["sha256"] = sha256_file(validation_path)
    manifest_path.write_text(json.dumps(manifest, sort_keys=True))

    with pytest.raises(
        ReleaseInputError, match="lacks an endorsement record ledger"
    ):
        load_bundle(bundle)

    cases = (
        (
            "source_corpus_sha256",
            "f" * 64,
            "frozen suffix-p corpus",
        ),
        (
            "source_export_manifest_sha256",
            "f" * 64,
            "frozen suffix-p full export",
        ),
        (
            "source_export_upstream_full_manifest_sha256",
            "f" * 64,
            "directly from the frozen full export",
        ),
    )
    for field, value, message in cases:
        tampered = deepcopy(manifest)
        tampered[field] = value
        manifest_path.write_text(json.dumps(tampered, sort_keys=True))
        with pytest.raises(ReleaseInputError, match=message):
            load_bundle(bundle)
        with pytest.raises(HandcodeInputError, match=message):
            load_frozen_bundle(bundle)


def test_refuses_nonexact_unicode_offsets(tmp_path: Path) -> None:
    core, bundle, rows = _write_fixture(tmp_path)
    rows[0]["extraction"]["issue_blocks"][0]["evidence_start_char"] += 1
    segments_path = bundle / "segments.jsonl.gz"
    with gzip.open(segments_path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    manifest_path = bundle / "bundle_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["segments_file"]["sha256"] = sha256_file(segments_path)
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(
        ReleaseInputError,
        match="known omission|differs from deterministic alignment",
    ):
        build_release(core, bundle, tmp_path / "release", production=False)


def test_refuses_excluded_biography_fields_and_unpassed_validation(tmp_path: Path) -> None:
    core, bundle, rows = _write_fixture(tmp_path)
    fact = rows[1]["extraction"]["facts"][0]
    fact["field"] = "family"
    segments_path = bundle / "segments.jsonl.gz"
    with gzip.open(segments_path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    manifest_path = bundle / "bundle_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["segments_file"]["sha256"] = sha256_file(segments_path)
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(
        ReleaseInputError,
        match="known omission|differs from deterministic alignment",
    ):
        build_release(core, bundle, tmp_path / "release", production=False)

    core, bundle, _ = _write_fixture(tmp_path / "second")
    validation_path = bundle / "human_validation.json"
    validation = {"schema_version": "2.1", "status": "pending"}
    validation_path.write_text(json.dumps(validation, sort_keys=True))
    manifest_path = bundle / "bundle_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["human_validation_file"]["sha256"] = sha256_file(validation_path)
    manifest_path.write_text(json.dumps(manifest))
    second_release = tmp_path / "second-release"
    summary = build_release(core, bundle, second_release, production=False)
    assert summary["release_ready"] is False
    pending_status = pd.read_csv(second_release / "candidate_extraction_status.csv")
    assert pending_status.loc[
        pending_status.source_observability.eq("observed_extractable"),
        "extraction_status",
    ].eq("withheld_validation").all()
    manifest_output = second_release / EXTENSION_MANIFEST_FILENAME
    manifest_output.write_text(
        json.dumps(extension_manifest_entries(second_release)), encoding="utf-8"
    )
    with pytest.raises(AssertionError, match="not release-ready"):
        validate_candidate_extraction_release(
            second_release, core, bundle, manifest_output, production=False
        )


def test_withholds_distinct_payloads_from_overlapping_segments(tmp_path: Path) -> None:
    core, bundle, rows = _write_fixture(tmp_path)
    first = rows[0]
    text = first["source_text"]
    first["segment_count"] = 2
    first_model = deepcopy(first["model_extraction"])
    first_item = first_model["issue_blocks"][0]
    first_item["source_heading"] = None
    _set_model_extraction(first, first_model)

    second = deepcopy(first)
    second["segment_index"] = 1
    second["segment_start_char"] = 5
    second["source_text"] = text[5:]
    second["segment_text_sha256"] = sha256_text(second["source_text"])
    second["document_id"] = sha256_text(
        f"{second['parent_document_id']}\n1\n5\n{len(text)}\n{second['segment_text_sha256']}"
    )
    second_model = deepcopy(second["model_extraction"])
    second_item = second_model["issue_blocks"][0]
    second_item["statement_kind"] = "proposal"
    _set_model_extraction(second, second_model)
    rows.insert(1, second)
    for row in (first, second):
        row["input_chars_used"] = len(row["source_text"])
        row["processing_status"] = "processed_success"
    raw_variants, resolved_records, conflicts, resolved_by_route = _resolve_rows(rows)

    segments_path = bundle / "segments.jsonl.gz"
    with gzip.open(segments_path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    manifest_path = bundle / "bundle_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["segments"] = len(rows)
    manifest["segments_by_route"]["issues"] = 3
    manifest["raw_record_variants"] = raw_variants
    manifest["resolved_records"] = resolved_records
    manifest["resolved_records_by_route"] = resolved_by_route
    manifest["withheld_overlap_conflict_groups"] = conflicts
    manifest["segment_processing_status"] = {"processed_success": len(rows)}
    manifest["technical_segment_gate_passed"] = True
    manifest["model_items"] = sum(
        row["exact_alignment"]["model_item_count"] for row in rows
    )
    manifest["aligned_items"] = sum(
        row["exact_alignment"]["aligned_item_count"] for row in rows
    )
    manifest["withheld_model_items"] = sum(
        row["exact_alignment"]["withheld_item_count"] for row in rows
    )
    _refresh_private_provenance(manifest, rows)
    manifest["segments_file"]["rows"] = len(rows)
    manifest["segments_file"]["sha256"] = sha256_file(segments_path)
    manifest_path.write_text(json.dumps(manifest))

    release = tmp_path / "release"
    summary = build_release(core, bundle, release, production=False)
    assert summary["release_ready"] is True
    assert len(pq.read_table(release / "candidate_issue_statements.parquet")) == 0
    status = pd.read_csv(release / "candidate_extraction_status.csv")
    assert status.loc[
        status.candidate_cycle_id.eq("H0AA00001-2020")
        & status.route.eq("issues"),
        "extraction_status",
    ].item() == "withheld_validation"
    assert status.loc[
        status.candidate_cycle_id.eq("H0AA00001-2020")
        & status.route.eq("issues"),
        "n_withheld_overlap_conflict_groups",
    ].item() == 1
    documents = pq.read_table(
        release / "candidate_extraction_documents.parquet"
    ).to_pandas()
    assert documents.loc[
        documents.parent_document_id.eq(first["parent_document_id"]),
        "withheld_overlap_conflict_group_count",
    ].item() == 1
    segments = pq.read_table(
        release / "candidate_extraction_segments.parquet"
    ).to_pandas()
    assert segments.loc[
        segments.parent_document_id.eq(first["parent_document_id"]),
        "withheld_overlap_conflict_group_count",
    ].tolist() == [1, 1]
    output_manifest = release / EXTENSION_MANIFEST_FILENAME
    output_manifest.write_text(json.dumps(extension_manifest_entries(release)))
    validated = validate_candidate_extraction_release(
        release, core, bundle, output_manifest, production=False
    )
    assert validated["release_ready"] is True

    conflict_variant = rows[0]["record_variants"][0]
    rows[0]["resolved_records"] = [{
        "record_id": "a" * 64,
        "parent_document_id": rows[0]["parent_document_id"],
        "route": rows[0]["route"],
        "canonical_segment_id": rows[0]["document_id"],
        "canonical_variant_id": conflict_variant["variant_id"],
        "all_source_segment_ids": [rows[0]["document_id"]],
        "all_variant_ids": [conflict_variant["variant_id"]],
        "variant_count": 1,
        "resolution_status": "single_variant",
        "resolution_method": "tampered_first_wins",
        "item": conflict_variant["item"],
        "parent_offsets": conflict_variant["parent_offsets"],
    }]
    with gzip.open(segments_path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    manifest["segments_file"]["sha256"] = sha256_file(segments_path)
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ReleaseInputError, match="resolved records differ"):
        build_release(core, bundle, tmp_path / "tampered-release", production=False)


def test_partial_alignment_keeps_exact_records_but_prevents_measured_zero(
    tmp_path: Path,
) -> None:
    core, bundle, rows = _write_fixture(tmp_path)
    issue_row = rows[0]
    model = deepcopy(issue_row["model_extraction"])
    missing = deepcopy(model["issue_blocks"][0])
    missing["claim"] = "A missing second claim"
    missing["evidence"] = "This text is not on the page."
    model["issue_blocks"].append(missing)
    _set_model_extraction(issue_row, model)
    assert issue_row["processing_status"] == "processed_partial_withheld"
    raw_variants, resolved_records, conflicts, resolved_by_route = _resolve_rows(rows)

    segments_path = bundle / "segments.jsonl.gz"
    with gzip.open(segments_path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    manifest_path = bundle / "bundle_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["raw_record_variants"] = raw_variants
    manifest["resolved_records"] = resolved_records
    manifest["resolved_records_by_route"] = resolved_by_route
    manifest["withheld_overlap_conflict_groups"] = conflicts
    manifest["segment_processing_status"] = {
        "processed_partial_withheld": 1,
        "processed_success": len(rows) - 1,
    }
    manifest["technical_segment_gate_passed"] = True
    manifest["model_items"] = sum(
        row["exact_alignment"]["model_item_count"] for row in rows
    )
    manifest["aligned_items"] = sum(
        row["exact_alignment"]["aligned_item_count"] for row in rows
    )
    manifest["withheld_model_items"] = sum(
        row["exact_alignment"]["withheld_item_count"] for row in rows
    )
    _refresh_private_provenance(manifest, rows)
    manifest["segments_file"]["sha256"] = sha256_file(segments_path)
    manifest_path.write_text(json.dumps(manifest))

    release = tmp_path / "release"
    summary = build_release(core, bundle, release, production=False)
    assert summary["release_ready"] is True
    issues = pq.read_table(release / "candidate_issue_statements.parquet")
    assert len(issues) == 1
    segments = pq.read_table(
        release / "candidate_extraction_segments.parquet"
    ).to_pandas()
    issue_segment = segments.loc[
        (segments["route"] == "issues")
        & (segments["candidate_cycle_id"] == "H0AA00001-2020")
    ]
    assert issue_segment["aligned_item_count"].tolist() == [1]
    assert issue_segment["withheld_item_count"].tolist() == [1]
    assert issue_segment["processing_status"].tolist() == ["processed"]
    status = pd.read_csv(release / "candidate_extraction_status.csv")
    assert status.loc[
        status.candidate_cycle_id.eq("H0AA00001-2020")
        & status.route.eq("issues"),
        "extraction_status",
    ].item() == "observed_positive"


def test_all_withheld_items_are_accounted_but_never_reported_as_zero(
    tmp_path: Path,
) -> None:
    core, bundle, rows = _write_fixture(tmp_path)
    issue_row = rows[0]
    model = deepcopy(issue_row["model_extraction"])
    model["issue_blocks"][0]["evidence"] = "This text is not on the page."
    _set_model_extraction(issue_row, model)
    assert issue_row["processing_status"] == "processed_all_withheld"
    raw_variants, resolved_records, conflicts, resolved_by_route = _resolve_rows(rows)

    segments_path = bundle / "segments.jsonl.gz"
    with gzip.open(segments_path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    manifest_path = bundle / "bundle_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["raw_record_variants"] = raw_variants
    manifest["resolved_records"] = resolved_records
    manifest["resolved_records_by_route"] = resolved_by_route
    manifest["withheld_overlap_conflict_groups"] = conflicts
    manifest["segment_processing_status"] = {
        "processed_all_withheld": 1,
        "processed_success": len(rows) - 1,
    }
    manifest["technical_segment_gate_passed"] = True
    manifest["model_items"] = sum(
        row["exact_alignment"]["model_item_count"] for row in rows
    )
    manifest["aligned_items"] = sum(
        row["exact_alignment"]["aligned_item_count"] for row in rows
    )
    manifest["withheld_model_items"] = sum(
        row["exact_alignment"]["withheld_item_count"] for row in rows
    )
    _refresh_private_provenance(manifest, rows)
    manifest["segments_file"]["sha256"] = sha256_file(segments_path)
    manifest_path.write_text(json.dumps(manifest))

    release = tmp_path / "release"
    summary = build_release(core, bundle, release, production=False)
    assert summary["release_ready"] is True
    issues = pq.read_table(release / "candidate_issue_statements.parquet")
    assert len(issues) == 0
    status = pd.read_csv(release / "candidate_extraction_status.csv")
    assert status.loc[
        status.candidate_cycle_id.eq("H0AA00001-2020")
        & status.route.eq("issues"),
        "extraction_status",
    ].item() == "withheld_validation"
    output_manifest = release / EXTENSION_MANIFEST_FILENAME
    output_manifest.write_text(json.dumps(extension_manifest_entries(release)))
    validated = validate_candidate_extraction_release(
        release, core, bundle, output_manifest, production=False
    )
    assert validated["release_ready"] is True


def test_rejects_nonstop_generation_and_transform_provenance_changes(
    tmp_path: Path,
) -> None:
    core, bundle, rows = _write_fixture(tmp_path)
    rows[0]["finish_reason"] = "length"
    rows[0]["parse_status"] = "output_truncated"
    for field in (
        "model_extraction", "model_extraction_sha256", "extraction",
        "pre_alignment_model_item_guard", "array_item_cap_report",
        "model_item_repetition_report",
        "aligned_extraction_sha256", "withheld_model_items",
        "exact_alignment", "validation",
    ):
        rows[0][field] = None
    segments_path = bundle / "segments.jsonl.gz"
    with gzip.open(segments_path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    manifest_path = bundle / "bundle_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["model_items"] = sum(
        int((row.get("exact_alignment") or {}).get("model_item_count", 0))
        for row in rows
    )
    manifest["aligned_items"] = sum(
        int((row.get("exact_alignment") or {}).get("aligned_item_count", 0))
        for row in rows
    )
    manifest["withheld_model_items"] = sum(
        int((row.get("exact_alignment") or {}).get("withheld_item_count", 0))
        for row in rows
    )
    manifest["segment_processing_status"] = {
        "failed_output_cap_unrepaired": 1,
        "processed_success": len(rows) - 1,
    }
    manifest["technical_segment_gate_passed"] = False
    _refresh_private_provenance(manifest, rows)
    manifest["segments_file"]["sha256"] = sha256_file(segments_path)
    manifest_path.write_text(json.dumps(manifest))
    # A cap-limited row is refused unless a repair attestation reports that
    # window as unrepaired. This bundle carries no attestation at all.
    with pytest.raises(ReleaseInputError, match="no repair attestation"):
        build_release(core, bundle, tmp_path / "release", production=False)

    core, bundle, rows = _write_fixture(tmp_path / "transform")
    rows[0]["source_text_transform_version"] = "unknown_transform"
    segments_path = bundle / "segments.jsonl.gz"
    with gzip.open(segments_path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    manifest_path = bundle / "bundle_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["segments_file"]["sha256"] = sha256_file(segments_path)
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(ReleaseInputError, match="source-transform provenance differs"):
        build_release(core, bundle, tmp_path / "transform-release", production=False)


def test_release_packager_rejects_private_repetition_guard_failure(
    tmp_path: Path,
) -> None:
    core, bundle, rows = _write_fixture(tmp_path)
    row = next(value for value in rows if value["route"] == "biography")
    original = deepcopy(row["model_extraction"]["facts"][0])
    _set_model_extraction(
        row,
        {"facts": [deepcopy(original), {**original, "value": "Mayor"}, deepcopy(original), deepcopy(original)]},
    )
    assert row["pre_alignment_model_item_guard"]["failure_status"] == (
        "model_item_repetition_error"
    )
    row["parse_status"] = "model_item_repetition_error"
    row["processing_status"] = "failed_model_item_repetition_error"
    row["extraction"] = None
    row["aligned_extraction_sha256"] = None
    row["withheld_model_items"] = []
    row["exact_alignment"] = None
    row["validation"] = {
        "schema_valid": False,
        "n_items": 0,
        "n_supported": 0,
        "evidence_support_rate": None,
        "n_required_anchors": 0,
        "n_exact_anchors": 0,
        "exact_anchor_rate": None,
        "status": "model_item_repetition_error",
    }
    row["record_variants"] = []
    row["resolved_records"] = []

    segments_path = bundle / "segments.jsonl.gz"
    with gzip.open(segments_path, "wt", encoding="utf-8") as handle:
        for value in rows:
            handle.write(json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n")
    manifest_path = bundle / "bundle_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    _refresh_private_provenance(manifest, rows)
    manifest["technical_segment_gate_passed"] = False
    manifest["segments_file"]["sha256"] = sha256_file(segments_path)
    manifest_path.write_text(json.dumps(manifest))

    with pytest.raises(
        ReleaseInputError,
        match="pre-alignment model-item guard failure",
    ):
        build_release(core, bundle, tmp_path / "guard-release", production=False)


def test_validator_refuses_a_manifest_hash_change(tmp_path: Path) -> None:
    core, bundle, _ = _write_fixture(tmp_path)
    release = tmp_path / "release"
    build_release(core, bundle, release, production=False)
    entries = extension_manifest_entries(release)
    entries[0]["sha256"] = "0" * 64
    manifest_path = release / EXTENSION_MANIFEST_FILENAME
    manifest_path.write_text(json.dumps(entries), encoding="utf-8")
    with pytest.raises(AssertionError, match="manifest hash differs"):
        validate_candidate_extraction_release(
            release, core, bundle, manifest_path, production=False
        )


def test_validator_refuses_extra_manifest_inventory(tmp_path: Path) -> None:
    core, bundle, _ = _write_fixture(tmp_path)
    release = tmp_path / "release"
    build_release(core, bundle, release, production=False)
    entries = extension_manifest_entries(release)
    entries.append({
        "file": "private_output.jsonl",
        "rows": 1,
        "columns": 1,
        "bytes": 1,
        "sha256": "0" * 64,
    })
    manifest_path = release / EXTENSION_MANIFEST_FILENAME
    manifest_path.write_text(json.dumps(entries), encoding="utf-8")
    with pytest.raises(AssertionError, match="exactly the six extension files"):
        validate_candidate_extraction_release(
            release, core, bundle, manifest_path, production=False
        )


def test_builder_and_validator_refuse_extra_disk_files(tmp_path: Path) -> None:
    core, bundle, _ = _write_fixture(tmp_path)
    nonempty = tmp_path / "nonempty"
    nonempty.mkdir()
    (nonempty / "segments_enriched.jsonl.gz").write_bytes(b"private")
    with pytest.raises(FileExistsError, match="nonempty extraction release"):
        build_release(core, bundle, nonempty, production=False)

    release = tmp_path / "release"
    build_release(core, bundle, release, production=False)
    manifest_path = release / EXTENSION_MANIFEST_FILENAME
    manifest_path.write_text(
        json.dumps(extension_manifest_entries(release)), encoding="utf-8"
    )
    (release / "raw_output.jsonl").write_text("private", encoding="utf-8")
    with pytest.raises(AssertionError, match="directory inventory differs"):
        validate_candidate_extraction_release(
            release, core, bundle, manifest_path, production=False
        )


def _round4_ledger_block(parents=(), records_file_rows=0):
    """Build a minimal declared round-four ledger manifest block."""
    from src.candidate_endorsement_round4_release import (
        build_round4_record_ledger,
    )

    ledger = build_round4_record_ledger(
        list(parents),
        stage_a_input_manifest_sha256="stage-a-manifest",
        stage_a_generation_config_sha256="stage-a-config",
        stage_b_input_manifest_sha256="stage-b-manifest",
        stage_b_generation_config_sha256="stage-b-config",
        stage_b_response_count=records_file_rows,
        accounted_output_count=records_file_rows,
    )
    return ledger


def _round4_parent(parent_id="parent-1"):
    return {
        "parent_document_id": parent_id,
        "aligned_record_count": 0,
        "withheld_record_count": 0,
        "stage_a_unclear_lines": [],
        "unresolved_target_blocks": [],
        "optional_field_repairs": [],
        "retry_required": False,
        "public_coverage_complete": True,
        "processing_status": "processed_success",
    }


def test_endorsement_architecture_selects_the_round4_contract():
    from src.candidate_endorsement_round4_production import (
        get_round4_production_contract,
    )
    from src.candidate_endorsement_line_ledger import get_line_ledger_contract

    round4 = {"_validated_endorsement_round4_ledger": {"summary": {}}}
    assert _endorsement_architecture(round4) == ROUND4_ENDORSEMENT_ARCHITECTURE
    assert _endorsement_route_contract(round4) == get_round4_production_contract()[
        "round4_production_contract_sha256"
    ]

    line_ledger = {"_validated_endorsement_line_ledger": {"summary": {}}}
    assert _endorsement_architecture(line_ledger) == (
        LINE_LEDGER_ENDORSEMENT_ARCHITECTURE
    )
    assert _endorsement_route_contract(line_ledger) == get_line_ledger_contract()[
        "line_ledger_contract_sha256"
    ]

    assert _endorsement_architecture({}) is None
    assert _endorsement_route_contract({}) is None


def test_public_metadata_binds_the_round4_contract_on_endorsement_rows():
    from src.candidate_endorsement_round4_production import (
        get_round4_production_contract,
    )

    contract_sha256 = get_round4_production_contract()[
        "round4_production_contract_sha256"
    ]
    metadata = {
        "schema_version": "2.1",
        "model_id": CONTRACT["model_id"],
        "model_revision": CONTRACT["model_revision"],
        "runtime_lock_sha256": CONTRACT["runtime_lock_sha256"],
        "prompt_sha256": CONTRACT["prompt_sha256"],
        "json_schema_sha256": CONTRACT["json_schema_sha256"],
        "extraction_contract_sha256": CONTRACT["extraction_contract_sha256"],
        "generation_config_sha256": GENERATION_HASH,
        "alignment_id": CONTRACT["alignment_id"],
        "alignment_spec_sha256": CONTRACT["alignment_spec_sha256"],
        "semantic_validation_spec_sha256": CONTRACT[
            "semantic_validation_spec_sha256"
        ],
        "endorsement_context_provenance_spec_sha256": CONTRACT[
            "endorsement_context_provenance_spec_sha256"
        ],
        "user_message_template_id": CONTRACT["user_message_template_id"],
        "user_message_template_sha256": CONTRACT[
            "user_message_template_sha256"
        ],
        "endorsement_route_contract_sha256": contract_sha256,
    }
    public = _public_metadata(metadata, "endorsements")
    assert public["extraction_contract_sha256"] == contract_sha256
    for field in (
        "prompt_sha256", "json_schema_sha256", "generation_config_sha256",
        "alignment_id", "alignment_spec_sha256", "user_message_template_id",
        "user_message_template_sha256",
    ):
        assert public[field] is None
    # The one-stage routes keep their own frozen identity.
    issues = _public_metadata(metadata, "issues")
    assert issues["extraction_contract_sha256"] == (
        CONTRACT["extraction_contract_sha256"]
    )
    assert issues["prompt_sha256"] == CONTRACT["prompt_sha256"]["issues"]


def test_a_bundle_cannot_declare_two_endorsement_architectures(tmp_path):
    core, bundle, _ = _write_fixture(tmp_path)
    manifest_path = bundle / "bundle_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["endorsement_line_ledger"] = {"summary": {}, "private_file": {}}
    manifest[ROUND4_LEDGER_MANIFEST_KEY] = {
        "summary": {}, "parents": [], "private_file": {},
    }
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(
        ReleaseInputError, match="declares two endorsement architectures"
    ):
        load_bundle(bundle, production=False)


def test_a_bundle_architecture_must_match_its_ledger(tmp_path):
    core, bundle, _ = _write_fixture(tmp_path)
    manifest_path = bundle / "bundle_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest[ROUND4_LEDGER_MANIFEST_KEY] = {
        "summary": {}, "parents": [], "private_file": {},
    }
    manifest["endorsement_architecture"] = (
        LINE_LEDGER_ENDORSEMENT_ARCHITECTURE
    )
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(
        ReleaseInputError, match="architecture differs from its ledger"
    ):
        load_bundle(bundle, production=False)


def test_a_named_architecture_without_a_ledger_is_rejected(tmp_path):
    core, bundle, _ = _write_fixture(tmp_path)
    manifest_path = bundle / "bundle_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["endorsement_architecture"] = ROUND4_ENDORSEMENT_ARCHITECTURE
    manifest_path.write_text(json.dumps(manifest))
    with pytest.raises(
        ReleaseInputError, match="architecture without a ledger"
    ):
        load_bundle(bundle, production=False)


def test_a_round4_ledger_with_an_open_retry_is_refused():
    """The ledger build is the gate that keeps a retryable failure out."""
    from src.candidate_endorsement_round4_release import Round4ProjectionError

    parent = _round4_parent()
    parent["retry_required"] = True
    parent["processing_status"] = None
    with pytest.raises(Round4ProjectionError, match="retryable failure"):
        _round4_ledger_block([parent])


def test_a_round4_ledger_reports_the_accounting_gate_fields():
    ledger = _round4_ledger_block([_round4_parent()], records_file_rows=1)
    summary = ledger["summary"]
    assert summary["all_model_records_accounted"] is True
    assert summary["unaccounted_outputs"] == 0
    assert summary["unresolved_infrastructure_failures"] == 0
    assert summary["endorsement_architecture"] == (
        ROUND4_ENDORSEMENT_ARCHITECTURE
    )


def _seed_vary_attempt(
    tmp_path: Path,
    *,
    attempt_index: int,
    target_count: int,
    repaired_count: int,
) -> dict:
    """Build one bundle block for a repair attempt, with its own run metadata."""
    import scripts.import_candidate_extraction_v2 as importer
    from tests.test_import_candidate_extraction_v2 import (
        _repair_run_metadata_payload,
        _write_repair_run_metadata,
    )

    output_dir = tmp_path / f"repair_attempt_{attempt_index}"
    _write_repair_run_metadata(
        output_dir,
        _repair_run_metadata_payload(
            tmp_path, attempt_index=attempt_index, selected=target_count
        ),
    )
    loaded = importer.load_repair_run_metadata(
        output_dir, attempt_index=attempt_index, target_count=target_count
    )
    return {
        "attempt_index": attempt_index,
        "repair_seed": importer.EXPECTED_REPAIR_GENERATION_SEEDS[attempt_index],
        "repair_manifest_file": "repair_manifest.json",
        "repair_manifest_sha256": "c" * 64,
        "repair_output_dir": str(output_dir),
        "reason": "output_truncated: greedy decoding reached the output cap",
        "primary_generation_config_sha256": EXPECTED_GENERATION_CONFIG_SHA256,
        "repair_generation_config_sha256": (
            importer.EXPECTED_REPAIR_GENERATION_CONFIG_SHA256S[attempt_index]
        ),
        "target_count": target_count,
        "repaired_count": repaired_count,
        "unrepaired_count": target_count - repaired_count,
        "final_job_run_metadata": importer._repair_run_metadata_entry(loaded),
        "windows": [
            {
                "document_id": f"{index:064x}",
                "chunk": f"chunk_{index:05d}.jsonl.gz",
                "repaired": index < repaired_count,
            }
            for index in range(target_count)
        ],
    }


def test_seed_vary_repair_block_is_fail_closed(tmp_path: Path) -> None:
    """The repair block carries the repair identity the primary list cannot."""
    _, bundle, _ = _write_fixture(tmp_path)
    manifest = json.loads((bundle / "bundle_manifest.json").read_text())
    assert manifest.get("seed_vary_repair") is None
    _validate_seed_vary_repair(manifest)

    repaired = deepcopy(manifest)
    repaired["seed_vary_repair"] = [
        _seed_vary_attempt(
            tmp_path, attempt_index=2, target_count=36, repaired_count=12
        ),
        _seed_vary_attempt(
            tmp_path, attempt_index=3, target_count=24, repaired_count=8
        ),
    ]
    _validate_seed_vary_repair(repaired)

    # A later attempt covers exactly what the previous one left at the cap.
    scoped = deepcopy(repaired)
    scoped["seed_vary_repair"][1]["target_count"] = 20
    scoped["seed_vary_repair"][1]["unrepaired_count"] = 12
    scoped["seed_vary_repair"][1]["windows"] = scoped["seed_vary_repair"][1][
        "windows"
    ][:20]
    scoped["seed_vary_repair"][1]["final_job_run_metadata"][
        "documents_selected_for_shard"
    ] = 20
    with pytest.raises(ReleaseInputError, match="left at the cap"):
        _validate_seed_vary_repair(scoped)

    # The attempt's metadata must not carry the frozen primary identity.
    primary_identity = deepcopy(repaired)
    primary_identity["seed_vary_repair"][0]["final_job_run_metadata"][
        "generation_config_sha256"
    ] = EXPECTED_GENERATION_CONFIG_SHA256
    with pytest.raises(ReleaseInputError, match="generation identity differs"):
        _validate_seed_vary_repair(primary_identity)

    # The seed and the attempt index are declared, not taken from the artifact.
    wrong_seed = deepcopy(repaired)
    wrong_seed["seed_vary_repair"][0]["repair_seed"] = 20_260_919
    with pytest.raises(ReleaseInputError, match="not the declared seed"):
        _validate_seed_vary_repair(wrong_seed)

    # The window outcomes and the counts must agree.
    counts = deepcopy(repaired)
    counts["seed_vary_repair"][0]["repaired_count"] = 13
    counts["seed_vary_repair"][0]["unrepaired_count"] = 23
    with pytest.raises(ReleaseInputError, match="repaired windows differ"):
        _validate_seed_vary_repair(counts)

    missing = deepcopy(repaired)
    missing["seed_vary_repair"][0].pop("final_job_run_metadata")
    with pytest.raises(ReleaseInputError, match="run metadata is absent"):
        _validate_seed_vary_repair(missing)

    unattested = deepcopy(repaired)
    unattested["seed_vary_repair"][0]["final_job_run_metadata"][
        "outputs_complete_for_selected_shard"
    ] = False
    with pytest.raises(ReleaseInputError, match="attest complete outputs"):
        _validate_seed_vary_repair(unattested)

    out_of_order = deepcopy(repaired)
    out_of_order["seed_vary_repair"] = list(
        reversed(out_of_order["seed_vary_repair"])
    )
    with pytest.raises(ReleaseInputError, match="does not follow"):
        _validate_seed_vary_repair(out_of_order)


def test_cap_withheld_row_is_accepted_only_with_its_attestation(
    tmp_path: Path,
) -> None:
    """A window every repair attempt left at the cap is released as withheld.

    Hanno approved this on 2026-09-18. The row carries no extraction, finishes
    with `length`, and is admitted only because the bundle's repair attestation
    reports that exact window as unrepaired.
    """
    core, bundle, rows = _write_fixture(tmp_path)
    document_id = str(rows[-1]["document_id"])
    rows[-1]["finish_reason"] = "length"
    rows[-1]["parse_status"] = "output_truncated"
    rows[-1]["processing_status"] = "failed_output_cap_unrepaired"
    for field in (
        "model_extraction", "model_extraction_sha256", "extraction",
        "pre_alignment_model_item_guard", "array_item_cap_report",
        "model_item_repetition_report",
        "aligned_extraction_sha256", "withheld_model_items",
        "exact_alignment",
    ):
        rows[-1][field] = None
    # The importer still records a validation block for a truncated row, with the
    # parse status as its status, so the release row carries one too.
    rows[-1]["validation"] = {
        "schema_valid": False,
        "n_items": 0,
        "n_supported": 0,
        "evidence_support_rate": None,
        "n_required_anchors": 0,
        "n_exact_anchors": 0,
        "exact_anchor_rate": None,
        "status": "output_truncated",
    }
    segments_path = bundle / "segments.jsonl.gz"
    with gzip.open(segments_path, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    manifest_path = bundle / "bundle_manifest.json"
    manifest = json.loads(manifest_path.read_text())
    for key, field in (
        ("model_items", "model_item_count"),
        ("aligned_items", "aligned_item_count"),
        ("withheld_model_items", "withheld_item_count"),
    ):
        manifest[key] = sum(
            int((row.get("exact_alignment") or {}).get(field, 0)) for row in rows
        )
    manifest["segment_processing_status"] = {
        "failed_output_cap_unrepaired": 1,
        "processed_success": len(rows) - 1,
    }
    attempt = _seed_vary_attempt(
        tmp_path, attempt_index=2, target_count=1, repaired_count=0
    )
    attempt["windows"] = [
        {
            "document_id": document_id,
            "chunk": "chunk_00001.jsonl.gz",
            "repaired": False,
        }
    ]
    manifest["seed_vary_repair"] = [attempt]
    manifest["technical_segment_gate_passed"] = True
    _refresh_private_provenance(manifest, rows)
    # No row in this fixture was actually replaced by a repair, so what the
    # primary run produced equals the corpus. This has to follow the refresh,
    # which rebuilds the corpus aggregate from the rows.
    manifest["primary_equivalent_output_aggregate"] = manifest["all_output_aggregate"]
    manifest["segments_file"]["sha256"] = sha256_file(segments_path)
    manifest_path.write_text(json.dumps(manifest))
    _validate_seed_vary_repair(manifest)
    build_release(core, bundle, tmp_path / "release", production=False)

    # The same row is refused once the attestation names another window.
    foreign = json.loads(manifest_path.read_text())
    foreign["seed_vary_repair"][0]["windows"] = [
        {"document_id": "f" * 64, "chunk": "chunk_00001.jsonl.gz", "repaired": False}
    ]
    manifest_path.write_text(json.dumps(foreign))
    with pytest.raises(ReleaseInputError, match="no repair attestation"):
        build_release(core, bundle, tmp_path / "release-foreign", production=False)

    # And refused when the attestation says the window was repaired, because
    # then the bundle should be carrying the repaired row instead. A repaired
    # window must carry its attempt's repair identity, so this row, which still
    # carries the primary identity, is refused on that before anything else.
    repaired = json.loads(manifest_path.read_text())
    repaired["seed_vary_repair"][0]["windows"] = [
        {"document_id": document_id, "chunk": "chunk_00001.jsonl.gz", "repaired": True}
    ]
    repaired["seed_vary_repair"][0]["repaired_count"] = 1
    repaired["seed_vary_repair"][0]["unrepaired_count"] = 0
    manifest_path.write_text(json.dumps(repaired))
    with pytest.raises(
        ReleaseInputError,
        match="generation_config_sha256 differs|no repair attestation",
    ):
        build_release(core, bundle, tmp_path / "release-repaired", production=False)


def test_official_government_hosts_leave_the_release_population() -> None:
    """Hanno approved dropping .gov pages on 2026-09-24.

    The population is campaign websites. Four of these hosts also carried the
    wrong candidate, for example billnelson.senate.gov under a Nebraska
    candidate named Nelson.
    """
    import pandas as pd

    from scripts.build_candidate_extraction_release import (
        _drop_excluded_hosts,
        _url_host,
    )

    assert _url_host(
        "https://web.archive.org/web/20180101000000/https://chu.house.gov/issues"
    ) == "chu.house.gov"
    assert _url_host(
        "https://web.archive.org/web/20180101000000/https://www.smithforcongress.com/"
    ) == "www.smithforcongress.com"

    raw = pd.DataFrame(
        {
            "candidate_cycle_id": ["A", "B", "C"],
            "snap_url": [
                "https://web.archive.org/web/20180101000000/https://chu.house.gov/issues",
                "https://web.archive.org/web/20180101000000/https://billnelson.senate.gov/x",
                "https://web.archive.org/web/20180101000000/https://smithforcongress.com/",
            ],
        }
    )
    kept, report = _drop_excluded_hosts(raw)
    assert list(kept["candidate_cycle_id"]) == ["C"]
    assert report["excluded_raw_rows"] == 2
    assert report["excluded_hosts"] == ["billnelson.senate.gov", "chu.house.gov"]
    assert report["excluded_candidate_years"] == ["A", "B"]


def test_approved_contract_hashes_match_the_computed_contracts() -> None:
    """The packager's approved hashes must be the real contract hashes.

    Both are literals so an accidental edit to a contract input fails closed.
    This test is what keeps the literals honest.
    """
    from src.candidate_extraction import get_extraction_contract
    from scripts.build_candidate_extraction_release import (
        APPROVED_EXTRACTION_CONTRACT_SHA256,
    )

    for version, approved in APPROVED_EXTRACTION_CONTRACT_SHA256.items():
        assert (
            get_extraction_contract(version)["extraction_contract_sha256"] == approved
        ), f"the approved {version} contract hash is stale"


def test_the_packager_writes_its_manifest_inventory() -> None:
    """The validator reads this file, so the packager must write it.

    Before 2026-09-27 the summary carried the entries but no file was written,
    and the 2026-09-25 inventory had to be rebuilt by hand.
    """
    import inspect

    from scripts.build_candidate_extraction_release import (
        EXTENSION_MANIFEST_FILENAME,
        build_release,
    )

    source = inspect.getsource(build_release)
    assert "EXTENSION_MANIFEST_FILENAME" in source
    assert "write_text" in source
    assert EXTENSION_MANIFEST_FILENAME == "candidate_extraction_manifest.json"
