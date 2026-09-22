#!/usr/bin/env python3
"""Run the fixed-slot endorsement line ledger with pinned Qwen on Hive.

This runner is intentionally separate from the frozen candidate-page runner.
It accepts only one homogeneous line-ledger stage per invocation, never alters
source text or batches, and replays the complete raw-output-to-alignment chain
before an atomic chunk can be reused after preemption.
"""
from __future__ import annotations

import argparse
import copy
import csv
import gzip
import hashlib
import json
import os
import socket
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from numbers import Integral
from pathlib import Path
from typing import Any, Mapping

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from scripts.hive.extract_candidate_pages_vllm import (  # noqa: E402
    JSON_STRING_CONTROL_CANONICALIZER_ID,
    JSON_STRING_CONTROL_CANONICALIZER_SPEC_SHA256,
    RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID,
    RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256,
    STRUCTURED_DECODING_BACKEND,
    STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
    StrictJSONError,
    canonicalize_json_string_controls,
    json_value_equal_typed,
    select_shard,
    strict_json_loads,
    structured_parameters,
    validate_raw_structural_whitespace,
)
from src.candidate_endorsement_line_ledger import (  # noqa: E402
    FROZEN_STAGE_A1_DECISION_IDENTITY_COUNT,
    FROZEN_STAGE_A1_DECISION_IDENTITY_LEDGER_SHA256,
    FROZEN_STAGE_A1_ORDERED_DECISION_SOURCE_LINE_IDS_SHA256,
    FROZEN_STAGE_A2_BATCH_COUNT,
    FROZEN_STAGE_A2_EXCEPTION_COUNT,
    FROZEN_STAGE_A2_EXCEPTION_IDENTITY_LEDGER_SHA256,
    FROZEN_STAGE_A2_EXCEPTION_REASON_COUNTS,
    FROZEN_STAGE_A2_ORDERED_SOURCE_LINE_IDS_SHA256,
    FROZEN_STAGE_A3_AUTHENTICATION,
    FROZEN_STAGE_A_V2_CONTRACT_SHA256,
    LINE_ADJUDICATION_BATCH_SCHEMA_RULE_SHA256,
    LINE_ADJUDICATION_SLOT_COUNT,
    LINE_ADJUDICATION_SPEC_SHA256,
    LINE_ADJUDICATION_STAGE,
    LINE_DECISION_SLOT_COUNT,
    LINE_DECISION_STAGE,
    LINE_INVENTORY_SPEC_SHA256,
    MODEL_ID,
    MODEL_REVISION,
    RECORD_EXTRACTION_STAGE,
    RECORD_EXTRACTION_BATCH_SCHEMA_RULE_SHA256,
    RUNTIME_LOCK_SHA256,
    SCHEMA_CATALOG_SHA256,
    SCHEMA_VARIANT_SHA256,
    STAGE_B_THREE_PASS_PROVENANCE_SPEC_SHA256,
    STAGE_B_TWO_PASS_PROVENANCE_SPEC_SHA256,
    STAGES,
    align_record_extraction_output,
    build_stage_user_message,
    canonical_line_adjudication_relations,
    computed_recall40_stage_a2_contract,
    computed_recall40_stage_b_contract,
    computed_recall40_round3_stage_a_contract,
    computed_recall40_round3_stage_a2_contract,
    computed_recall40_round3_stage_b_contract,
    get_frozen_recall40_round3_stage_a_contract,
    get_frozen_recall40_round3_stage_a2_contract,
    get_frozen_recall40_round3_stage_b_contract,
    get_frozen_recall40_round2_stage_a2_contract,
    get_frozen_recall40_round2_stage_a_contract,
    RECALL40_ROUND2_STAGE_A2_SCOPE_SPEC,
    RECALL40_ROUND2_STAGE_B_SCOPE_SPEC,
    RECALL40_ROUND3_STAGE_A_SCOPE_SPEC,
    FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256,
    FROZEN_RECALL40_ROUND3_STAGE_A2_CONTRACT_SHA256,
    FROZEN_RECALL40_ROUND3_STAGE_B_CONTRACT_SHA256,
    FROZEN_RECALL40_ROUND3_STAGE_A_INPUT_MANIFEST_SHA256,
    FROZEN_RECALL40_ROUND3_STAGE_A_GENERATION_CONFIG_SHA256,
    RECALL40_ROUND3_STAGE_A2_BATCH_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_A2_SCHEMA_RULE_SHA256,
    RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC,
    RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_A2_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_B_BATCH_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC,
    RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_B_RECORD_PROJECTION_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_B_RENDERING_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_B_SCHEMA_CATALOG_SHA256,
    RECALL40_ROUND3_STAGE_B_SCHEMA_RULE_SHA256,
    RECALL40_ROUND3_STAGE_B_SCHEMA_VARIANT_SHA256,
    RECALL40_ROUND3_STAGE_B_SCOPE_SPEC,
    RECALL40_ROUND3_STAGE_B_SCOPE_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_B_SEMANTIC_SPEC_SHA256,
    RECALL40_ROUND3_STAGE_B_SOURCE_CATALOG_SPEC_SHA256,
    get_frozen_recall40_round2_stage_b_contract,
    RECALL40_STAGE_A2_SCOPE_SPEC,
    RECALL40_STAGE_B_SCOPE_SPEC,
    get_line_ledger_contract,
    get_frozen_stage_a2_v4_contract,
    line_adjudication_exception_identity_ledger,
    round3_line_adjudication_exception_ledger,
    prompt_for_contract_stage,
    round3_stage_b_closure_ledgers,
    schema_binding_for_batch,
    sha256_json,
    sha256_text,
    validate_line_decision_output,
    validate_line_adjudication_output,
    validate_record_extraction_output,
    validate_round3_stage_a_context_batch,
)


GENERATION_CONFIG_VERSION = "candidate_endorsement_line_ledger_generation_v4"
RUN_METADATA_VERSION = "candidate_endorsement_line_ledger_run_metadata_v5"
PROMPT_TOKEN_COUNT_SPEC = {
    "version": "candidate_endorsement_prompt_token_count_v2",
    "chat_template": "pinned_tokenizer.apply_chat_template",
    "arguments": {
        "tokenize": True,
        "add_generation_prompt": True,
        "enable_thinking": False,
    },
    "accepted_shape": (
        "one_unbatched_integer_token_id_sequence;_or_mapping_input_ids_with_"
        "one_unbatched_or_singleton_batched_integer_sequence"
    ),
    "failure_rule": "reject_mapping_length_or_ambiguous_or_noninteger_shapes",
}
PROMPT_TOKEN_COUNT_SPEC_SHA256 = sha256_json(PROMPT_TOKEN_COUNT_SPEC)
DEFAULT_MAX_OUTPUT_TOKENS = {
    LINE_DECISION_STAGE: 2_048,
    LINE_ADJUDICATION_STAGE: 2_048,
    RECORD_EXTRACTION_STAGE: 16_384,
}
FROZEN_MAX_MODEL_LEN = 32_768
CONTEXT_SAFETY_MARGIN_TOKENS = 256
FROZEN_HISTORICAL_STAGE_B_SUFFIX_E_MANIFEST_SHA256 = (
    "aef6d6263d51873854cfa7fb1e4a1d2be6a7c3bf5f1244d0be93d57931dec178"
)
FROZEN_RECALL40_SELECTED_PARENT_IDS_SHA256 = (
    "955ec2242fbba4385a7a1dffbb7dec0eba9dd44545f20aae94984720a0e0d6e8"
)
FROZEN_RECALL40_BATCH_PARENT_ORDER_SHA256 = (
    "71ecaad0d98561f4b1b4962d417a10c841bd2219a7d997cfe3b50512af9cfc30"
)
FROZEN_RECALL40_PRIVATE_SELECTION_SHA256 = (
    "5e6b0908f12cd59560699079fe19ea190303b0a8a14b9cebc1792761ed0567e6"
)
FROZEN_RECALL40_ASSIGNMENT_SHA256 = (
    "68fe62747c357239e87e785b3a444ec946c9ee1d96bc521bcaeece73f06f6877"
)
FROZEN_RECALL40_INVENTORY = {
    "complete_line_overlap_duplicate_occurrences": 28,
    "context_anchor_boundary_lines": 31, "empty_physical_lines": 5,
    "model_decision_line_characters": 43836, "model_decision_lines": 1115,
    "nonempty_physical_lines": 1524, "outside_scope_lines": 378,
    "parent_documents": 40, "physical_lines": 1529,
    "reconstructed_long_lines": 0, "source_segments": 43,
    "unique_model_decision_line_ids": 1115, "unique_source_line_ids": 1529,
}
FROZEN_RECALL40_SOURCE_EXPORT = {
    "corrected_issue_control_source_segments": 3,
    "replayed_parents_by_route": {"biography": 6962, "endorsements": 2583, "issues": 14326},
    "replayed_segments_by_route": {"biography": 8444, "endorsements": 6096, "issues": 20673},
    "schema_version": "1.4", "source_corpus_sha256": "c53d36b24b293ac5d47706aa67731f1c107d6a679cb948c9ad71a70b5c8ce7c3",
    "source_document_ids_sha256": "826f3b4d6622c06e03448beff2d20b59437d51bce04630ec520c36b4d461ba21",
    "source_export_manifest_sha256": "f5e6545a2d2ec35a83f4184e0d328635e5fba9d3c80cd4c8a7466178b7d9172c",
}
FROZEN_RECALL40_SELECTION_SHA256 = (
    "906554da0a9df1e593fdfa40fa62bf9ba54e83e3d3667e488aae28103c9b9204"
)
FROZEN_RECALL40_STAGE_A_SELECTION_BASE_ROWS_SHA256 = (
    "162768261a6a219dbbe32853a9711cb42086129be354ce7974084121213590d4"
)
FROZEN_GENERATION_SEED = 20260825
FROZEN_SOURCE_CORPUS_SHA256 = (
    "c53d36b24b293ac5d47706aa67731f1c107d6a679cb948c9ad71a70b5c8ce7c3"
)


def canonical_json(value: Any) -> str:
    return json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def read_gzip_jsonl(path: Path) -> list[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [strict_json_loads(line) for line in handle if line.strip()]


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _is_sha256(value: Any) -> bool:
    if not isinstance(value, str) or len(value) != 64:
        return False
    try:
        int(value, 16)
    except ValueError:
        return False
    return True


def _is_round3_stage_a2_contract(contract: Mapping[str, Any]) -> bool:
    return (
        contract.get("contract_version")
        == "candidate_endorsement_recall40_round3_stage_a2_v2"
        and contract.get("active_stage") == LINE_ADJUDICATION_STAGE
        and contract.get("line_adjudication_spec_sha256")
        == RECALL40_ROUND3_STAGE_A2_SPEC_SHA256
    )


def _is_round3_stage_b_contract(contract: Mapping[str, Any]) -> bool:
    return (
        contract.get("contract_version")
        == "candidate_endorsement_recall40_round3_stage_b_v1"
        and contract.get("active_stage") == RECORD_EXTRACTION_STAGE
        and contract.get("recall40_round3_stage_b_semantic_spec_sha256")
        == RECALL40_ROUND3_STAGE_B_SEMANTIC_SPEC_SHA256
        and contract.get("recall40_round3_stage_b_scope_spec_sha256")
        == RECALL40_ROUND3_STAGE_B_SCOPE_SPEC_SHA256
        and contract.get("stage_b_two_pass_provenance_spec_sha256")
        == RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC_SHA256
        and contract.get("record_extraction_batch_schema_rule_sha256")
        == RECALL40_ROUND3_STAGE_B_SCHEMA_RULE_SHA256
    )


def _is_two_pass_stage_b_contract(contract: Mapping[str, Any]) -> bool:
    return (
        contract.get("contract_version")
        == "candidate_endorsement_recall40_stage_b_v1"
        or _is_round3_stage_b_contract(contract)
    )


def _adjudication_contract_bindings(
    contract: Mapping[str, Any],
) -> dict[str, str]:
    if _is_round3_stage_a2_contract(contract):
        return {
            "line_adjudication_batch_schema_rule_sha256": (
                RECALL40_ROUND3_STAGE_A2_SCHEMA_RULE_SHA256
            ),
            "line_adjudication_spec_sha256": (
                RECALL40_ROUND3_STAGE_A2_SPEC_SHA256
            ),
            "upstream_stage_a_contract_sha256": (
                FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256
            ),
        }
    return {
        "line_adjudication_batch_schema_rule_sha256": (
            LINE_ADJUDICATION_BATCH_SCHEMA_RULE_SHA256
        ),
        "line_adjudication_spec_sha256": LINE_ADJUDICATION_SPEC_SHA256,
        "upstream_stage_a_contract_sha256": str(
            contract.get(
                "upstream_stage_a_v2_contract_sha256",
                FROZEN_STAGE_A_V2_CONTRACT_SHA256,
            )
        ),
    }


def _round3_stage_a2_upstream_identities(
    value: Mapping[str, Any],
) -> dict[str, str]:
    fields = (
        "upstream_stage_a_input_manifest_sha256",
        "upstream_stage_a_generation_config_sha256",
        "upstream_stage_a_output_batch_ids_sha256",
        "upstream_stage_a_decision_ledger_sha256",
        "stage_a_provenance_sha256",
    )
    output: dict[str, str] = {}
    for field in fields:
        observed = value.get(field)
        if not _is_sha256(observed):
            raise ValueError(f"round-three Stage-A2 {field} is invalid")
        output[field] = str(observed)
    if output["upstream_stage_a_input_manifest_sha256"] != (
        FROZEN_RECALL40_ROUND3_STAGE_A_INPUT_MANIFEST_SHA256
    ) or output["upstream_stage_a_generation_config_sha256"] != (
        FROZEN_RECALL40_ROUND3_STAGE_A_GENERATION_CONFIG_SHA256
    ):
        raise ValueError("round-three Stage-A2 frozen upstream identity differs")
    return output


def validate_expected_input_manifest_sha256(
    manifest_path: Path, expected_sha256: Any,
) -> str:
    """Require exact externally reviewed manifest bytes before JSON parsing."""
    if not _is_sha256(expected_sha256):
        raise ValueError("expected input manifest SHA-256 is invalid")
    actual_sha256 = file_sha256(manifest_path)
    if actual_sha256 != expected_sha256:
        raise ValueError(
            "input manifest bytes differ from externally frozen SHA-256"
        )
    return actual_sha256


def schema_variant_inventory(
    batches: list[Mapping[str, Any]],
) -> dict[str, Any]:
    variant_ids: list[str] = []
    schema_hashes: list[str] = []
    counts: Counter[str] = Counter()
    used_hashes: dict[str, str] = {}
    for batch in batches:
        binding = schema_binding_for_batch(batch)
        for field in (
            "json_schema_variant_id", "json_schema_sha256",
            "json_schema_catalog_sha256", "target_slot_count",
        ):
            if not json_value_equal_typed(batch.get(field), binding[field]):
                raise ValueError(f"input batch {field} differs from schema variant")
        variant_id = str(binding["json_schema_variant_id"])
        schema_hash = str(binding["json_schema_sha256"])
        variant_ids.append(variant_id)
        schema_hashes.append(schema_hash)
        counts[variant_id] += 1
        previous = used_hashes.setdefault(variant_id, schema_hash)
        if previous != schema_hash:
            raise ValueError("schema variant ID maps to multiple hashes")
    return {
        "schema_variant_counts": dict(sorted(counts.items())),
        "schema_variant_hashes_used": dict(sorted(used_hashes.items())),
        "ordered_schema_variant_ids_sha256": sha256_json(variant_ids),
        "ordered_schema_variant_hashes_sha256": sha256_json(schema_hashes),
    }


def _validate_round3_stage_a2_manifest(
    manifest: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    files: Mapping[str, Any],
    selection: Mapping[str, Any],
    parsed_selection_rows: list[dict[str, str]],
    parsed_selection_columns: list[str],
    entries: list[Mapping[str, Any]],
    all_batches: list[dict[str, Any]],
    manifest_path: Path,
    chunks_dir: Path,
) -> None:
    """Validate the open/frozen round-three A2 artifact as a closed world."""
    if contract != computed_recall40_round3_stage_a2_contract():
        raise ValueError("round-three Stage-A2 contract differs")
    expected_manifest_fields = {
        "builder", "builder_version", "chunk_size", "complete_parent_closure",
        "contract_is_frozen", "corrected_71c_issue_audit_gate",
        "exception_ledger", "expected_batch_counts", "expected_batch_ids",
        "expected_batch_ids_sha256", "expected_stage_a2_batch_count",
        "expected_stage_batch_count", "exported_records", "files",
        "frozen_line_ledger_contract_sha256", "inventory_census",
        "json_schema_catalog_sha256", "json_schema_variant_sha256",
        "line_adjudication_batch_schema_rule_sha256",
        "line_adjudication_spec_sha256", "line_batch_spec_sha256",
        "line_inventory_spec_sha256", "line_ledger_contract",
        "line_ledger_contract_sha256", "manifest_payload_sha256", "mode",
        "ordered_schema_variant_hashes_sha256",
        "ordered_schema_variant_ids_sha256",
        "private_recall40_round3_stage_a2",
        "recall40_round3_stage_a2_scope_spec_sha256", "route",
        "schema_variant_counts", "schema_variant_hashes_used",
        "selected_parent_ids", "selected_parent_ids_sha256", "source_export",
        "stage", "stage_a_decision_identity_ledger",
        "stage_a_decision_identity_ledger_sha256", "stage_a_exception_count",
        "stage_a_exception_identity_ledger",
        "stage_a_exception_identity_ledger_sha256",
        "stage_a_exception_ledger_sha256",
        "stage_a_exception_reason_counts",
        "stage_a_exception_source_line_ids_sha256", "stage_a_provenance",
        "unchanged_route_hashes", "upstream_stage_a_contract_sha256",
        "upstream_stage_a_decision_ledger_sha256",
        "upstream_stage_a_generation_config_sha256",
        "upstream_stage_a_input_manifest_sha256",
        "upstream_stage_a_output_batch_ids_sha256",
    }
    if set(manifest) != expected_manifest_fields:
        raise ValueError("round-three Stage-A2 manifest field set differs")
    if (
        any(
            type(manifest.get(field)) is not int
            for field in (
                "chunk_size", "expected_stage_a2_batch_count",
                "expected_stage_batch_count", "exported_records",
            )
        )
        or type(selection.get("bytes")) is not int
        or type(selection.get("rows")) is not int
        or any(
            type(entry.get(field)) is not int
            for entry in entries for field in ("bytes", "records")
        )
    ):
        raise ValueError("round-three Stage-A2 count type differs")
    if (
        manifest.get("builder")
        != "scripts/build_candidate_endorsement_line_ledger.py"
        or manifest.get("builder_version")
        != "candidate_endorsement_line_ledger_export_v1"
        or manifest.get("mode") != "line_adjudication_recall40_round3"
        or manifest.get("recall40_round3_stage_a2_scope_spec_sha256")
        != RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC_SHA256
        or manifest.get("line_adjudication_spec_sha256")
        != RECALL40_ROUND3_STAGE_A2_SPEC_SHA256
        or manifest.get("line_adjudication_batch_schema_rule_sha256")
        != RECALL40_ROUND3_STAGE_A2_SCHEMA_RULE_SHA256
        or manifest.get("upstream_stage_a_contract_sha256")
        != FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256
        or manifest.get("upstream_stage_a_input_manifest_sha256")
        != FROZEN_RECALL40_ROUND3_STAGE_A_INPUT_MANIFEST_SHA256
        or manifest.get("upstream_stage_a_generation_config_sha256")
        != FROZEN_RECALL40_ROUND3_STAGE_A_GENERATION_CONFIG_SHA256
        or sha256_json(manifest.get("corrected_71c_issue_audit_gate"))
        != RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
            "corrected_71c_issue_audit_gate_sha256"
        ]
        or manifest.get("chunk_size") != 60
        or manifest.get("expected_stage_a2_batch_count") != len(all_batches)
    ):
        raise ValueError("round-three Stage-A2 fixed scope differs")
    for field, scope_field in (
        (
            "upstream_stage_a_output_batch_ids_sha256",
            "upstream_stage_a_output_batch_ids_sha256",
        ),
        (
            "upstream_stage_a_decision_ledger_sha256",
            "upstream_stage_a_decision_ledger_sha256",
        ),
    ):
        if manifest.get(field) != RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
            scope_field
        ]:
            raise ValueError(f"round-three Stage-A2 {field} differs")
    if (
        set(files) != {"selection", "chunks"}
        or set(selection) != {"bytes", "columns", "file", "rows", "sha256"}
        or any(set(entry) != {
            "bytes", "content_sha256", "file", "first_document_id",
            "last_document_id", "records", "sha256",
        } for entry in entries)
        or [entry.get("file") for entry in entries] != [
            f"chunk_{index:05d}.jsonl.gz" for index in range(len(entries))
        ]
        or [entry.get("records") for entry in entries] != [
            min(60, len(all_batches) - offset)
            for offset in range(0, len(all_batches), 60)
        ]
        or sorted(path.name for path in manifest_path.parent.iterdir())
        != ["chunks", "manifest.json", "selection.csv"]
        or sorted(path.name for path in chunks_dir.iterdir())
        != [entry["file"] for entry in entries]
    ):
        raise ValueError("round-three Stage-A2 artifact inventory differs")
    expected_selection_columns = [
        "selection_group", "pilot_control_kind", "parent_document_id",
        "candidate_cycle_id", "candidate_icpsr", "state", "office", "year",
        "party", "segment_count", "physical_line_count",
        "model_decision_line_count", "line_decision_batch_count",
        "record_extraction_batch_count_maximum",
        "parent_line_inventory_sha256", "context_scope",
        "stage_a_anchor_line_count", "stage_a_unclear_line_count",
        "stage_a_role_counts_json", "record_extraction_batch_count",
    ]
    selected_ids = manifest.get("selected_parent_ids")
    if (
        parsed_selection_columns != expected_selection_columns
        or selection.get("columns") != expected_selection_columns
        or selection.get("rows") != 40
        or selection.get("sha256")
        != RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC["stage_a2_selection_sha256"]
        or not isinstance(selected_ids, list)
        or len(selected_ids) != 40
        or len(set(selected_ids)) != 40
        or sha256_json(selected_ids)
        != RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC["selected_parent_ids_sha256"]
        or manifest.get("selected_parent_ids_sha256")
        != RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC["selected_parent_ids_sha256"]
        or [row.get("parent_document_id") for row in parsed_selection_rows]
        != selected_ids
        or any(
            row.get("selection_group") != "private_recall_pilot40"
            for row in parsed_selection_rows
        )
    ):
        raise ValueError("round-three Stage-A2 selection closure differs")
    if (
        not json_value_equal_typed(
            manifest.get("source_export"), FROZEN_RECALL40_SOURCE_EXPORT
        )
        or sha256_json(manifest.get("inventory_census"))
        != RECALL40_ROUND3_STAGE_A_SCOPE_SPEC["inventory_census_sha256"]
        or sha256_json(manifest.get("expected_batch_counts"))
        != RECALL40_ROUND3_STAGE_A_SCOPE_SPEC["expected_batch_counts_sha256"]
    ):
        raise ValueError("round-three Stage-A2 frozen source scope differs")

    provenance = manifest.get("stage_a_provenance")
    if not isinstance(provenance, Mapping):
        raise ValueError("round-three Stage-A2 Stage-A provenance is absent")
    provenance_payload = dict(provenance)
    provenance_sha256 = provenance_payload.pop("stage_a_provenance_sha256", None)
    if (
        provenance_sha256 != sha256_json(provenance_payload)
        or provenance_sha256
        != RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
            "upstream_stage_a_provenance_sha256"
        ]
        or provenance.get("stage_a_line_ledger_contract_sha256")
        != FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256
        or provenance.get("stage_a_input_manifest_sha256")
        != FROZEN_RECALL40_ROUND3_STAGE_A_INPUT_MANIFEST_SHA256
        or provenance.get("stage_a_generation_config_sha256")
        != FROZEN_RECALL40_ROUND3_STAGE_A_GENERATION_CONFIG_SHA256
        or provenance.get("stage_a_ordered_output_batch_ids_sha256")
        != RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
            "upstream_stage_a_output_batch_ids_sha256"
        ]
        or provenance.get("source_line_decision_ledger_sha256")
        != RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
            "upstream_stage_a_decision_ledger_sha256"
        ]
        or provenance.get("validated_output_batches") != 270
        or provenance.get("typed_restart_replay_complete") is not True
        or provenance.get("selected_parent_ids_sha256")
        != RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC["selected_parent_ids_sha256"]
    ):
        raise ValueError("round-three Stage-A2 Stage-A provenance differs")
    expected_input_files = [
        {"file": row["file"], "sha256": row["sha256"]}
        for row in RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
            "upstream_stage_a_input_chunks"
        ]
    ]
    if (
        not json_value_equal_typed(
            provenance.get("input_files"), expected_input_files
        )
        or not json_value_equal_typed(
            provenance.get("output_files"),
            RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
                "upstream_stage_a_output_files"
            ],
        )
        or not json_value_equal_typed(
            provenance.get("run_metadata_files"),
            RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
                "upstream_stage_a_metadata_files"
            ],
        )
    ):
        raise ValueError("round-three Stage-A2 five-shard provenance differs")
    by_batch_id = provenance.get("by_batch_id")
    if not isinstance(by_batch_id, Mapping) or len(by_batch_id) != 270:
        raise ValueError("round-three Stage-A2 per-batch provenance differs")

    identity_ledger = manifest.get("stage_a_decision_identity_ledger")
    if (
        not isinstance(identity_ledger, list)
        or len(identity_ledger) != 4019
        or any(
            not isinstance(row, Mapping)
            or set(row) != {
                "parent_document_id", "source_line_id", "stage_a_role",
                "stage_a_batch_id", "stage_a_slot",
            }
            for row in identity_ledger
        )
        or manifest.get("stage_a_decision_identity_ledger_sha256")
        != sha256_json(identity_ledger)
        or manifest.get("stage_a_decision_identity_ledger_sha256")
        != RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
            "upstream_stage_a_decision_identity_ledger_sha256"
        ]
    ):
        raise ValueError("round-three Stage-A2 Stage-A decision ledger differs")
    identity_by_line = {
        str(row["source_line_id"]): dict(row) for row in identity_ledger
    }
    if len(identity_by_line) != len(identity_ledger):
        raise ValueError("round-three Stage-A2 Stage-A decisions duplicate")

    batches_by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    catalog_by_parent: dict[str, list[dict[str, Any]]] = {}
    prompt_metadata_by_parent: dict[str, dict[str, Any]] = {}
    selection_by_parent = {
        str(row["parent_document_id"]): row for row in parsed_selection_rows
    }
    prompt_metadata_fields = (
        "candidate_cycle_id", "candidate_icpsr", "state", "office", "year",
        "party", "raw_text_sha256", "parent_document_id",
        "extraction_source_text_sha256", "extraction_source_n_chars",
        "source_urls", "parent_line_inventory_sha256",
        "endorsement_line_context",
    )
    exact_batch_upstream = {
        "upstream_stage_a_input_manifest_sha256": (
            RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
                "upstream_stage_a_input_manifest_sha256"
            ]
        ),
        "upstream_stage_a_generation_config_sha256": (
            RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
                "upstream_stage_a_generation_config_sha256"
            ]
        ),
        "upstream_stage_a_output_batch_ids_sha256": (
            RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
                "upstream_stage_a_output_batch_ids_sha256"
            ]
        ),
        "upstream_stage_a_decision_ledger_sha256": (
            RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
                "upstream_stage_a_decision_ledger_sha256"
            ]
        ),
        "stage_a_provenance_sha256": (
            RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
                "upstream_stage_a_provenance_sha256"
            ]
        ),
    }
    for batch in all_batches:
        if any(
            batch.get(field) != expected
            for field, expected in exact_batch_upstream.items()
        ):
            raise ValueError("round-three Stage-A2 batch provenance differs")
        parent_id = str(batch["parent_document_id"])
        batches_by_parent[parent_id].append(batch)
        catalog = batch["source_line_catalog"]
        prior_catalog = catalog_by_parent.setdefault(parent_id, catalog)
        if not json_value_equal_typed(prior_catalog, catalog):
            raise ValueError("round-three Stage-A2 parent catalogs differ")
        metadata = {
            key: copy.deepcopy(batch[key]) for key in prompt_metadata_fields
        }
        prior_metadata = prompt_metadata_by_parent.setdefault(parent_id, metadata)
        if not json_value_equal_typed(prior_metadata, metadata):
            raise ValueError("round-three Stage-A2 parent metadata differs")
        selection_row = selection_by_parent.get(parent_id)
        if (
            selection_row is None
            or batch["parent_line_inventory_sha256"]
            != selection_row["parent_line_inventory_sha256"]
            or batch["parent_physical_line_count"]
            != int(selection_row["physical_line_count"])
        ):
            raise ValueError("round-three Stage-A2 selection/source differs")
        for line in catalog:
            line_id = str(line["source_line_id"])
            expected_identity = identity_by_line.get(line_id)
            if line["model_decision_required"]:
                observed_identity = {
                    "parent_document_id": parent_id,
                    "source_line_id": line_id,
                    "stage_a_role": line["stage_a_role"],
                    "stage_a_batch_id": line["stage_a_batch_id"],
                    "stage_a_slot": line["stage_a_slot"],
                }
                if not json_value_equal_typed(
                    observed_identity, expected_identity
                ):
                    raise ValueError(
                        "round-three Stage-A2 catalog Stage-A decision differs"
                    )
            elif expected_identity is not None:
                raise ValueError(
                    "round-three Stage-A2 catalog hides a Stage-A decision"
                )
    prompt_metadata_ledger = [
        prompt_metadata_by_parent[parent_id]
        for parent_id in sorted(prompt_metadata_by_parent)
    ]
    if (
        len(prompt_metadata_ledger)
        != RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
            "stage_a2_exception_parent_count"
        ]
        or sha256_json(prompt_metadata_ledger)
        != RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
            "stage_a2_parent_prompt_metadata_ledger_sha256"
        ]
    ):
        raise ValueError("round-three Stage-A2 prompt metadata differs")

    expected_all: list[dict[str, Any]] = []
    observed_all: list[dict[str, Any]] = []
    expected_batch_order: list[tuple[str, int]] = []
    for parent_id in sorted(batches_by_parent):
        parent_batches = batches_by_parent[parent_id]
        indices = [batch["batch_index_within_parent"] for batch in parent_batches]
        catalog = catalog_by_parent[parent_id]
        raw_lines = [{
            key: copy.deepcopy(line[key]) for key in (
                "source_line_id", "parent_line_index", "parent_start_char",
                "parent_end_char", "parent_full_end_char", "line_text",
                "line_text_sha256", "line_terminator", "model_decision_required",
            )
        } for line in catalog]
        decisions = {
            str(line["source_line_id"]): {
                "source_line_id": str(line["source_line_id"]),
                "role": line["stage_a_role"],
                "stage_a_batch_id": line["stage_a_batch_id"],
                "stage_a_slot": line["stage_a_slot"],
            }
            for line in catalog if line["model_decision_required"]
        }
        expected = round3_line_adjudication_exception_ledger(
            [{
                "parent_document_id": parent_id,
                "candidate_icpsr": parent_batches[0].get("candidate_icpsr"),
                "lines": raw_lines,
            }],
            decisions,
            upstream_stage_a_contract_sha256=(
                FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256
            ),
        )
        expected_batch_count = (
            len(expected) + LINE_ADJUDICATION_SLOT_COUNT - 1
        ) // LINE_ADJUDICATION_SLOT_COUNT
        if indices != list(range(expected_batch_count)):
            raise ValueError("round-three Stage-A2 parent batch closure differs")
        expected_batch_order.extend(
            (parent_id, index) for index in range(expected_batch_count)
        )
        expected_all.extend(expected)
        for batch in parent_batches:
            for target in batch["targets"]:
                stage_a = target["stage_a_decision"]
                if not json_value_equal_typed(
                    target["stage_a_provenance_locator"],
                    by_batch_id.get(str(stage_a["stage_a_batch_id"])),
                ):
                    raise ValueError(
                        "round-three Stage-A2 Stage-A locator differs"
                    )
                observed_all.append({
                    key: copy.deepcopy(value)
                    for key, value in target.items()
                    if key not in {"slot", "stage_a_provenance_locator"}
                })
    observed_batch_order = [
        (
            str(batch["parent_document_id"]),
            int(batch["batch_index_within_parent"]),
        )
        for batch in all_batches
    ]
    if observed_batch_order != expected_batch_order:
        raise ValueError("round-three Stage-A2 canonical batch order differs")
    if not json_value_equal_typed(observed_all, expected_all):
        raise ValueError("round-three Stage-A2 global exception closure differs")
    source_ids = [str(row["source_line_id"]) for row in expected_all]
    exception_ids = [str(row["exception_id"]) for row in expected_all]
    reasons = dict(sorted(Counter(
        str(row["exception_reason"]) for row in expected_all
    ).items()))
    exception_identity = line_adjudication_exception_identity_ledger(
        all_batches
    )
    expected_exception_fields = {
        "stage_a_exception_count": len(expected_all),
        "stage_a_exception_ledger_sha256": sha256_json(expected_all),
        "stage_a_exception_source_line_ids_sha256": sha256_json(source_ids),
        "stage_a_exception_reason_counts": reasons,
        "stage_a_exception_identity_ledger": exception_identity,
        "stage_a_exception_identity_ledger_sha256": sha256_json(
            exception_identity
        ),
    }
    if any(
        not json_value_equal_typed(manifest.get(field), expected)
        for field, expected in expected_exception_fields.items()
    ) or not json_value_equal_typed(manifest.get("exception_ledger"), {
        "count": len(expected_all),
        "ordered_exception_ids_sha256": sha256_json(exception_ids),
        "ordered_source_line_ids_sha256": sha256_json(source_ids),
        "reason_counts": reasons,
    }):
        raise ValueError("round-three Stage-A2 exception ledger differs")
    if (
        len(expected_all)
        != RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC["stage_a2_exception_count"]
        or reasons
        != RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
            "stage_a2_exception_reason_counts"
        ]
        or sha256_json(expected_all)
        != RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
            "stage_a2_exception_ledger_sha256"
        ]
        or sha256_json(exception_ids)
        != RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
            "stage_a2_ordered_exception_ids_sha256"
        ]
        or sha256_json(source_ids)
        != RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
            "stage_a2_ordered_source_line_ids_sha256"
        ]
    ):
        raise ValueError("round-three Stage-A2 frozen exception closure differs")
    private = manifest.get("private_recall40_round3_stage_a2")
    expected_private = {
        "enabled": True,
        "round": 3,
        "source_stage_a_contract_sha256": (
            FROZEN_RECALL40_ROUND3_STAGE_A_CONTRACT_SHA256
        ),
        "source_stage_a_input_manifest_sha256": (
            FROZEN_RECALL40_ROUND3_STAGE_A_INPUT_MANIFEST_SHA256
        ),
        "source_stage_a_generation_config_sha256": (
            FROZEN_RECALL40_ROUND3_STAGE_A_GENERATION_CONFIG_SHA256
        ),
        "selected_parent_count": 40,
        "stage_a_decision_count": 4019,
        "exception_count": len(expected_all),
        "exception_reason_counts": reasons,
        "builder_performed_inference": False,
        "hive_submission_authorized": False,
        "stage_b_or_release_authorized": False,
    }
    if not json_value_equal_typed(private, expected_private):
        raise ValueError("round-three Stage-A2 private authorization differs")


_ROUND3_POST_A2_REPORT_FIELDS = {
    "version", "evaluation_scope", "gate_decision", "passed_checks",
    "failed_checks", "checks", "selected_parent_count",
    "selected_parent_ids_sha256", "stage_a_contract_sha256",
    "stage_a_input_manifest_sha256", "stage_a_generation_config_sha256",
    "stage_a_output_batch_ids_sha256", "stage_a_batch_count",
    "stage_a_decision_count", "stage_a_decisions_sha256",
    "stage_a2_contract_sha256", "stage_a2_input_manifest_sha256",
    "stage_a2_generation_config_sha256",
    "stage_a2_output_batch_ids_sha256", "stage_a2_batch_count",
    "stage_a2_exception_count", "stage_a2_decisions_sha256",
    "stage_a2_run_artifacts_sha256", "final_decision_count",
    "final_decisions_sha256", "per_line_provenance_sha256",
    "merge_validation", "authorized_next_action",
    "bounded_stage_b_input_build_authorized",
    "stage_b_hive_submission_authorized",
    "full_expansion_or_release_authorized", "report_payload_sha256",
}


def _validate_round3_stage_b_manifest(
    manifest: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
    files: Mapping[str, Any],
    selection: Mapping[str, Any],
    parsed_selection_rows: list[dict[str, str]],
    parsed_selection_columns: list[str],
    entries: list[Mapping[str, Any]],
    all_batches: list[dict[str, Any]],
    manifest_path: Path,
    chunks_dir: Path,
) -> None:
    """Authenticate the complete frozen round-three Stage-B artifact."""
    scope = RECALL40_ROUND3_STAGE_B_SCOPE_SPEC
    expected_contract = computed_recall40_round3_stage_b_contract()
    expected_manifest_fields = {
        "builder", "builder_version", "chunk_size", "complete_parent_closure",
        "contract_is_frozen", "corrected_71c_issue_audit_gate",
        "expected_batch_counts", "expected_batch_ids",
        "expected_batch_ids_sha256", "expected_stage_batch_count",
        "exported_records", "files", "frozen_line_ledger_contract_sha256",
        "inventory_census", "json_schema_catalog_sha256",
        "json_schema_variant_sha256", "line_batch_spec_sha256",
        "line_inventory_spec_sha256", "line_ledger_contract",
        "line_ledger_contract_sha256", "manifest_payload_sha256", "mode",
        "ordered_schema_variant_hashes_sha256",
        "ordered_schema_variant_ids_sha256",
        "private_recall40_round3_stage_b",
        "recall40_round3_stage_b_record_projection_spec_sha256",
        "recall40_round3_stage_b_rendering_spec_sha256",
        "recall40_round3_stage_b_scope_spec_sha256",
        "recall40_round3_stage_b_semantic_spec_sha256",
        "recall40_round3_stage_b_source_catalog_spec_sha256",
        "record_extraction_batch_schema_rule_sha256", "round3_stage_b_gate",
        "route", "schema_variant_counts", "schema_variant_hashes_used",
        "selected_parent_ids", "selected_parent_ids_sha256", "source_export",
        "stage", "stage_b_closure", "stage_b_two_pass_provenance_spec_sha256",
        "stage_b_upstream_pass_count", "two_pass_merge", "unchanged_route_hashes",
        "upstream_artifact_hashes", "upstream_stage_a", "upstream_stage_a2",
    }
    if set(manifest) != expected_manifest_fields:
        raise ValueError("round-three Stage-B manifest field set differs")
    top_level_counts = (
        "chunk_size", "stage_b_upstream_pass_count",
        "expected_stage_batch_count", "exported_records",
    )
    expected_batch_counts = manifest.get("expected_batch_counts")
    inventory_census = manifest.get("inventory_census")
    if (
        any(type(manifest.get(field)) is not int for field in top_level_counts)
        or not isinstance(expected_batch_counts, Mapping)
        or any(type(value) is not int for value in expected_batch_counts.values())
        or not isinstance(inventory_census, Mapping)
        or any(type(value) is not int for value in inventory_census.values())
    ):
        raise ValueError("round-three Stage-B top-level count type differs")
    if (
        contract != expected_contract
        or manifest.get("line_ledger_contract") != expected_contract
        or manifest.get("mode") != "record_extraction_recall40_round3"
        or manifest.get("stage") != RECORD_EXTRACTION_STAGE
        or manifest.get("route") != "endorsements"
        or manifest.get("builder")
        != "scripts/build_candidate_endorsement_line_ledger.py"
        or manifest.get("builder_version")
        != "candidate_endorsement_line_ledger_export_v1"
        or type(manifest.get("chunk_size")) is not int
        or manifest["chunk_size"] != scope["chunk_size"]
        or type(manifest.get("stage_b_upstream_pass_count")) is not int
        or manifest["stage_b_upstream_pass_count"] != 2
        or manifest.get("contract_is_frozen") is not True
        or manifest.get("frozen_line_ledger_contract_sha256")
        != FROZEN_RECALL40_ROUND3_STAGE_B_CONTRACT_SHA256
        or manifest.get("complete_parent_closure") is not True
    ):
        raise ValueError("round-three Stage-B fixed contract fields differ")
    expected_top_bindings = {
        "line_batch_spec_sha256": RECALL40_ROUND3_STAGE_B_BATCH_SPEC_SHA256,
        "json_schema_catalog_sha256": (
            RECALL40_ROUND3_STAGE_B_SCHEMA_CATALOG_SHA256
        ),
        "json_schema_variant_sha256": dict(
            RECALL40_ROUND3_STAGE_B_SCHEMA_VARIANT_SHA256
        ),
        "stage_b_two_pass_provenance_spec_sha256": (
            RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC_SHA256
        ),
        "record_extraction_batch_schema_rule_sha256": (
            RECALL40_ROUND3_STAGE_B_SCHEMA_RULE_SHA256
        ),
        "recall40_round3_stage_b_semantic_spec_sha256": (
            RECALL40_ROUND3_STAGE_B_SEMANTIC_SPEC_SHA256
        ),
        "recall40_round3_stage_b_source_catalog_spec_sha256": (
            RECALL40_ROUND3_STAGE_B_SOURCE_CATALOG_SPEC_SHA256
        ),
        "recall40_round3_stage_b_record_projection_spec_sha256": (
            RECALL40_ROUND3_STAGE_B_RECORD_PROJECTION_SPEC_SHA256
        ),
        "recall40_round3_stage_b_rendering_spec_sha256": (
            RECALL40_ROUND3_STAGE_B_RENDERING_SPEC_SHA256
        ),
        "recall40_round3_stage_b_scope_spec_sha256": (
            RECALL40_ROUND3_STAGE_B_SCOPE_SPEC_SHA256
        ),
    }
    if any(
        not json_value_equal_typed(manifest.get(field), expected)
        for field, expected in expected_top_bindings.items()
    ):
        raise ValueError("round-three Stage-B semantic/schema binding differs")

    expected_selection_columns = [
        "selection_group", "pilot_control_kind", "parent_document_id",
        "candidate_cycle_id", "candidate_icpsr", "state", "office", "year",
        "party", "segment_count", "physical_line_count",
        "model_decision_line_count", "line_decision_batch_count",
        "record_extraction_batch_count_maximum", "parent_line_inventory_sha256",
        "context_scope", "stage_a_anchor_line_count",
        "stage_a_unclear_line_count", "stage_a_role_counts_json",
        "record_extraction_batch_count",
    ]
    expected_chunk_names = [
        f"chunk_{index:05d}.jsonl.gz"
        for index in range(len(scope["chunk_record_counts"]))
    ]
    if (
        set(files) != {"selection", "chunks"}
        or type(selection.get("bytes")) is not int
        or type(selection.get("rows")) is not int
        or not json_value_equal_typed(selection, {
            "bytes": scope["selection_csv_bytes"],
            "columns": expected_selection_columns,
            "file": "selection.csv",
            "rows": scope["selected_parent_count"],
            "sha256": scope["selection_csv_sha256"],
        })
        or parsed_selection_columns != expected_selection_columns
        or len(parsed_selection_rows) != scope["selected_parent_count"]
        or any(set(entry) != {
            "bytes", "content_sha256", "file", "first_document_id",
            "last_document_id", "records", "sha256",
        } for entry in entries)
        or any(
            type(entry.get(field)) is not int
            for entry in entries for field in ("bytes", "records")
        )
        or [entry.get("file") for entry in entries] != expected_chunk_names
        or [entry.get("records") for entry in entries]
        != scope["chunk_record_counts"]
        or sorted(path.name for path in manifest_path.parent.iterdir())
        != ["chunks", "manifest.json", "selection.csv"]
        or sorted(path.name for path in chunks_dir.iterdir())
        != expected_chunk_names
    ):
        raise ValueError("round-three Stage-B file inventory differs")
    selected_ids = manifest.get("selected_parent_ids")
    selection_ids = [row.get("parent_document_id") for row in parsed_selection_rows]
    if (
        not isinstance(selected_ids, list)
        or len(selected_ids) != scope["selected_parent_count"]
        or len(selected_ids) != len(set(selected_ids))
        or sha256_json(selected_ids) != scope["selected_parent_ids_sha256"]
        or manifest.get("selected_parent_ids_sha256")
        != scope["selected_parent_ids_sha256"]
        or selection_ids != selected_ids
        or any(
            row.get("selection_group") != "private_recall_pilot40"
            for row in parsed_selection_rows
        )
        or not json_value_equal_typed(
            manifest.get("source_export"), FROZEN_RECALL40_SOURCE_EXPORT
        )
        or sha256_json(manifest.get("inventory_census"))
        != scope["inventory_census_sha256"]
        or sha256_json(manifest.get("expected_batch_counts"))
        != scope["expected_batch_counts_sha256"]
        or sha256_json(manifest.get("corrected_71c_issue_audit_gate"))
        != "bdd1601458bbacf160ed0fc639b64b5febc5903ad91b11c01fc1c2cb4d443bc0"
    ):
        raise ValueError("round-three Stage-B source/selection closure differs")
    if any(
        not value.isdigit()
        for row in parsed_selection_rows
        for value in (
            row["segment_count"], row["physical_line_count"],
            row["model_decision_line_count"], row["line_decision_batch_count"],
            row["record_extraction_batch_count_maximum"],
            row["stage_a_anchor_line_count"], row["stage_a_unclear_line_count"],
            row["record_extraction_batch_count"],
        )
    ):
        raise ValueError("round-three Stage-B selection count is invalid")

    gate = manifest.get("round3_stage_b_gate")
    if not isinstance(gate, Mapping) or set(gate) != {
        "report_file_sha256", "report_object_sha256", "report",
    }:
        raise ValueError("round-three Stage-B gate object differs")
    report = gate.get("report")
    if not isinstance(report, Mapping) or set(report) != (
        _ROUND3_POST_A2_REPORT_FIELDS
    ):
        raise ValueError("round-three Stage-B report field set differs")
    report_payload = dict(report)
    recorded_report_payload = report_payload.pop("report_payload_sha256", None)
    if (
        any(
            type(report.get(field)) is not int
            for field in (
                "passed_checks", "failed_checks", "selected_parent_count",
                "stage_a_batch_count", "stage_a_decision_count",
                "stage_a2_batch_count", "stage_a2_exception_count",
                "final_decision_count",
            )
        )
        or
        gate.get("report_file_sha256")
        != RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
            "post_a2_report_file_sha256"
        ]
        or gate.get("report_object_sha256")
        != RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
            "post_a2_report_object_sha256"
        ]
        or sha256_json(report) != gate.get("report_object_sha256")
        or recorded_report_payload
        != RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
            "post_a2_report_payload_sha256"
        ]
        or sha256_json(report_payload) != recorded_report_payload
        or report.get("version")
        != "candidate_endorsement_recall40_round3_post_a2_gate_v1"
        or report.get("gate_decision") != "GO_BUILD_BOUNDED_STAGE_B_INPUTS"
        or type(report.get("passed_checks")) is not int
        or report["passed_checks"] != 6
        or type(report.get("failed_checks")) is not int
        or report["failed_checks"] != 0
        or not isinstance(report.get("checks"), list)
        or len(report["checks"]) != 6
        or any(
            not isinstance(check, Mapping) or check.get("passed") is not True
            for check in report["checks"]
        )
        or report.get("authorized_next_action")
        != "build_private_recall40_round3_bounded_stage_b_inputs"
        or report.get("bounded_stage_b_input_build_authorized") is not True
        or report.get("stage_b_hive_submission_authorized") is not False
        or report.get("full_expansion_or_release_authorized") is not False
    ):
        raise ValueError("round-three Stage-B canonical report differs")

    upstream_a = manifest.get("upstream_stage_a")
    upstream_a2 = manifest.get("upstream_stage_a2")
    merge = manifest.get("two_pass_merge")
    expected_upstream_a = {
        "contract_sha256": (
            RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
                "upstream_stage_a_contract_sha256"
            ]
        ),
        "input_manifest_sha256": (
            RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
                "upstream_stage_a_input_manifest_sha256"
            ]
        ),
        "generation_config_sha256": (
            RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
                "upstream_stage_a_generation_config_sha256"
            ]
        ),
        "ordered_output_batch_ids_sha256": (
            RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
                "upstream_stage_a_output_batch_ids_sha256"
            ]
        ),
        "decision_ledger_sha256": (
            RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
                "upstream_stage_a_decision_ledger_sha256"
            ]
        ),
        "provenance_sha256": (
            RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
                "upstream_stage_a_provenance_sha256"
            ]
        ),
    }
    expected_merge = {
        "validation": {
            "expected_exception_count": 11,
            "adjudicated_exception_count": 11,
            "unresolved_line_count": 0,
            "withheld_line_count": 0,
            "unowned_continuation_count": 0,
            "duplicate_continuation_owner_count": 0,
            "ownership_conflict_count": 0,
            "coverage_complete": True,
            "validation_passed": True,
        },
        "final_decisions_sha256": scope["final_decisions_sha256"],
        "per_line_provenance_sha256": scope["per_line_provenance_sha256"],
        "ownership_by_continuation_sha256": (
            scope["ownership_by_continuation_sha256"]
        ),
    }
    if (
        not json_value_equal_typed(upstream_a, expected_upstream_a)
        or sha256_json(upstream_a) != scope["upstream_stage_a_object_sha256"]
        or not isinstance(upstream_a2, Mapping)
        or set(upstream_a2) != {
            "input_manifest_sha256", "generation_config_sha256",
            "max_model_len", "max_output_tokens", "input_files",
            "output_files", "output_batch_ids_sha256",
            "validated_output_batches", "run_metadata_files",
            "typed_restart_replay_complete", "decision_ledger_sha256",
        }
        or upstream_a2.get("input_manifest_sha256")
        != RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
            "upstream_stage_a2_input_manifest_sha256"
        ]
        or upstream_a2.get("generation_config_sha256")
        != RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
            "upstream_stage_a2_generation_config_sha256"
        ]
        or upstream_a2.get("output_batch_ids_sha256")
        != RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
            "upstream_stage_a2_output_batch_ids_sha256"
        ]
        or upstream_a2.get("decision_ledger_sha256")
        != RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
            "upstream_stage_a2_decision_ledger_sha256"
        ]
        or type(upstream_a2.get("max_model_len")) is not int
        or upstream_a2.get("max_model_len") != FROZEN_MAX_MODEL_LEN
        or type(upstream_a2.get("max_output_tokens")) is not int
        or upstream_a2.get("max_output_tokens")
        != DEFAULT_MAX_OUTPUT_TOKENS[LINE_ADJUDICATION_STAGE]
        or type(upstream_a2.get("validated_output_batches")) is not int
        or upstream_a2.get("validated_output_batches") != 3
        or upstream_a2.get("typed_restart_replay_complete") is not True
        or sha256_json(upstream_a2) != scope["upstream_stage_a2_object_sha256"]
        or not json_value_equal_typed(merge, expected_merge)
        or sha256_json(merge) != scope["two_pass_merge_object_sha256"]
        or sha256_json(merge["validation"])
        != scope["merge_validation_sha256"]
    ):
        raise ValueError("round-three Stage-B upstream/merge binding differs")

    artifact_hashes = manifest.get("upstream_artifact_hashes")
    expected_artifact_hashes = {
        "stage_a_contract_sha256": expected_upstream_a["contract_sha256"],
        "stage_a_input_manifest_sha256": expected_upstream_a[
            "input_manifest_sha256"
        ],
        "stage_a_generation_config_sha256": expected_upstream_a[
            "generation_config_sha256"
        ],
        "stage_a_output_batch_ids_sha256": expected_upstream_a[
            "ordered_output_batch_ids_sha256"
        ],
        "stage_a_decision_ledger_sha256": expected_upstream_a[
            "decision_ledger_sha256"
        ],
        "stage_a_provenance_sha256": expected_upstream_a[
            "provenance_sha256"
        ],
        "stage_a_provenance_object_sha256": scope[
            "upstream_stage_a_provenance_object_sha256"
        ],
        "stage_a_input_files_sha256": scope[
            "upstream_stage_a_input_files_sha256"
        ],
        "stage_a_output_files_sha256": scope[
            "upstream_stage_a_output_files_sha256"
        ],
        "stage_a_metadata_files_sha256": scope[
            "upstream_stage_a_metadata_files_sha256"
        ],
        "stage_a2_contract_sha256": (
            RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC[
                "upstream_stage_a2_contract_sha256"
            ]
        ),
        "stage_a2_input_manifest_sha256": upstream_a2[
            "input_manifest_sha256"
        ],
        "stage_a2_generation_config_sha256": upstream_a2[
            "generation_config_sha256"
        ],
        "stage_a2_output_batch_ids_sha256": upstream_a2[
            "output_batch_ids_sha256"
        ],
        "stage_a2_decision_ledger_sha256": upstream_a2[
            "decision_ledger_sha256"
        ],
        "stage_a2_artifacts_object_sha256": scope[
            "upstream_stage_a2_artifacts_object_sha256"
        ],
        "stage_a2_run_artifacts_sha256": scope[
            "upstream_stage_a2_run_artifacts_sha256"
        ],
        "stage_a2_input_files_sha256": scope[
            "upstream_stage_a2_input_files_sha256"
        ],
        "stage_a2_output_files_sha256": scope[
            "upstream_stage_a2_output_files_sha256"
        ],
        "stage_a2_metadata_files_sha256": scope[
            "upstream_stage_a2_metadata_files_sha256"
        ],
    }
    if not json_value_equal_typed(artifact_hashes, expected_artifact_hashes):
        raise ValueError("round-three Stage-B upstream artifact hashes differ")
    if (
        sha256_json(upstream_a2["input_files"])
        != scope["upstream_stage_a2_input_files_sha256"]
        or sha256_json(upstream_a2["output_files"])
        != scope["upstream_stage_a2_output_files_sha256"]
        or sha256_json(upstream_a2["run_metadata_files"])
        != scope["upstream_stage_a2_metadata_files_sha256"]
        or report.get("stage_a2_run_artifacts_sha256")
        != scope["upstream_stage_a2_run_artifacts_sha256"]
    ):
        raise ValueError("round-three Stage-B upstream file inventory differs")

    expected_batch_fields = {
        "batch_id", "batch_index_within_parent", "candidate_cycle_id",
        "candidate_icpsr", "document_id", "endorsement_line_context",
        "extraction_source_n_chars", "extraction_source_text_sha256",
        "final_line_decisions_sha256", "json_schema_catalog_sha256",
        "json_schema_sha256", "json_schema_variant_id",
        "line_batch_spec_sha256", "line_inventory_spec_sha256", "office",
        "padding_slots", "parent_document_id", "parent_line_inventory_sha256",
        "parent_source_text", "party",
        "recall40_round3_stage_b_record_projection_spec_sha256",
        "recall40_round3_stage_b_rendering_spec_sha256",
        "recall40_round3_stage_b_scope_spec_sha256",
        "recall40_round3_stage_b_semantic_spec_sha256",
        "recall40_round3_stage_b_source_catalog_spec_sha256",
        "record_extraction_batch_schema_rule_sha256", "route",
        "segment_descriptors", "source_urls", "stage", "state",
        "stage_b_two_pass_provenance_spec_sha256", "target_slot_count",
        "target_slots", "targets", "two_pass_line_provenance_sha256", "year",
        "raw_text_sha256",
    }
    expected_target_fields = {
        "anchor_line_decision_provenance", "complete_carrier_segment_ids",
        "deterministic_disposition", "inventory_unit_type",
        "line_adjudication_applied", "line_terminator", "line_text",
        "line_text_sha256", "model_decision_required", "owner_segment_id",
        "owner_segment_index", "parent_end_char", "parent_full_end_char",
        "parent_line_index", "parent_start_char",
        "record_block_line_decision_provenance", "record_block_lines",
        "segment_fragments", "slot", "source_line_id", "stage_a2_batch_id",
        "stage_a2_decision", "stage_a2_slot", "stage_a_batch_id",
        "stage_a_proposed_role", "stage_a_role", "stage_a_slot",
    }
    for batch in all_batches:
        if set(batch) != expected_batch_fields:
            raise ValueError("round-three Stage-B batch field set differs")
        if (
            batch.get("final_line_decisions_sha256")
            != scope["final_decisions_sha256"]
            or batch.get("two_pass_line_provenance_sha256")
            != scope["per_line_provenance_sha256"]
            or any(
                batch.get(field) != expected
                for field, expected in expected_top_bindings.items()
                if field not in {"json_schema_variant_sha256"}
            )
            or batch.get("json_schema_variant_id", "").startswith(
                "recall40_round3_record_extraction_anchor_cardinality_"
            ) is not True
            or type(batch.get("batch_index_within_parent")) is not int
            or type(batch.get("extraction_source_n_chars")) is not int
            or type(batch.get("target_slot_count")) is not int
            or type(batch.get("year")) is not int
            or batch["target_slot_count"] != len(batch.get("targets", []))
            or any(
                not isinstance(target, Mapping)
                or set(target) != expected_target_fields
                for target in batch.get("targets", [])
            )
        ):
            raise ValueError("round-three Stage-B batch contract differs")
        binding = schema_binding_for_batch(batch)
        if any(
            not json_value_equal_typed(batch.get(field), binding[field])
            for field in (
                "json_schema_variant_id", "json_schema_sha256",
                "json_schema_catalog_sha256", "target_slot_count",
            )
        ):
            raise ValueError("round-three Stage-B batch schema binding differs")
        schema_properties = binding["schema"].get("properties", {})
        for target in batch["targets"]:
            record_schema = schema_properties[target["slot"]]["properties"][
                "records"
            ]
            expected_minimum = (
                2 if target["stage_a_role"] == "compound_record_anchor" else 1
            )
            expected_maximum = (
                32 if target["stage_a_role"] == "compound_record_anchor" else 1
            )
            if (
                record_schema.get("minItems") != expected_minimum
                or record_schema.get("maxItems") != expected_maximum
            ):
                raise ValueError("round-three Stage-B output cardinality schema differs")
        for slot in batch["padding_slots"]:
            if schema_properties[slot]["properties"]["records"].get(
                "maxItems"
            ) != 0:
                raise ValueError("round-three Stage-B padding schema differs")
        identity = {
            "stage": RECORD_EXTRACTION_STAGE,
            "contract": contract["line_ledger_contract_sha256"],
            "parent_document_id": batch["parent_document_id"],
            "batch_index": batch["batch_index_within_parent"],
            "anchor_line_ids": [
                target["source_line_id"] for target in batch["targets"]
            ],
            "final_decisions_sha256": batch["final_line_decisions_sha256"],
            "two_pass_provenance_sha256": batch[
                "two_pass_line_provenance_sha256"
            ],
            "stage_b_two_pass_provenance_spec_sha256": batch[
                "stage_b_two_pass_provenance_spec_sha256"
            ],
            "record_extraction_batch_schema_rule_sha256": batch[
                "record_extraction_batch_schema_rule_sha256"
            ],
            "recall40_round3_stage_b_semantic_spec_sha256": batch[
                "recall40_round3_stage_b_semantic_spec_sha256"
            ],
            "recall40_round3_stage_b_source_catalog_spec_sha256": batch[
                "recall40_round3_stage_b_source_catalog_spec_sha256"
            ],
            "recall40_round3_stage_b_record_projection_spec_sha256": batch[
                "recall40_round3_stage_b_record_projection_spec_sha256"
            ],
            "recall40_round3_stage_b_rendering_spec_sha256": batch[
                "recall40_round3_stage_b_rendering_spec_sha256"
            ],
            "recall40_round3_stage_b_scope_spec_sha256": batch[
                "recall40_round3_stage_b_scope_spec_sha256"
            ],
        }
        expected_batch_id = sha256_json(identity)
        if (
            batch.get("batch_id") != expected_batch_id
            or batch.get("document_id") != expected_batch_id
        ):
            raise ValueError("round-three Stage-B batch identity differs")

    closure = round3_stage_b_closure_ledgers(all_batches)
    expected_closure_fields = {
        "stage_b_batch_count", "stage_b_anchor_count", "ordinary_anchor_count",
        "compound_anchor_count", "stage_b_parent_with_anchors_count",
        "stage_b_used_line_provenance_count",
        "ordered_anchor_source_line_ids_sha256",
        "ordered_anchor_parent_ids_sha256", "ordered_batch_parent_ids_sha256",
        "batch_partition_sha256", "source_projection_ledger_sha256",
        "parent_source_catalog_ledger_sha256",
        "stage_b_used_line_provenance_sha256",
        "semantic_source_batch_ledger_sha256",
    }
    if (
        set(closure) != expected_closure_fields
        or not json_value_equal_typed(manifest.get("stage_b_closure"), closure)
        or any(closure[field] != scope[field] for field in closure)
        or any(
            type(closure[field]) is not int
            for field in (
                "stage_b_batch_count", "stage_b_anchor_count",
                "ordinary_anchor_count", "compound_anchor_count",
                "stage_b_parent_with_anchors_count",
                "stage_b_used_line_provenance_count",
            )
        )
        or len(all_batches) != scope["stage_b_batch_count"]
        or manifest.get("expected_stage_batch_count")
        != scope["stage_b_batch_count"]
        or type(manifest.get("expected_stage_batch_count")) is not int
        or manifest.get("exported_records") != scope["stage_b_batch_count"]
        or type(manifest.get("exported_records")) is not int
    ):
        raise ValueError("round-three Stage-B batch/anchor closure differs")

    private = manifest.get("private_recall40_round3_stage_b")
    expected_private = {
        "enabled": True,
        "round": 3,
        "authorization_scope": (
            "private_recall40_round3_bounded_stage_b_inputs_only"
        ),
        "selected_parent_count": 40,
        "stage_a_line_count": 4019,
        "stage_a2_exception_count": 11,
        "final_anchor_count": scope["stage_b_anchor_count"],
        "ordinary_anchor_count": scope["ordinary_anchor_count"],
        "compound_anchor_count": scope["compound_anchor_count"],
        "stage_b_batch_count": scope["stage_b_batch_count"],
        "stage_b_input_build_authorized": True,
        "builder_performed_inference": False,
        "contract_is_frozen": True,
        "human_name_adjudication_imported": False,
        "independent_pre_freeze_review_required": False,
        "hive_submission_authorized": False,
        "full_expansion_or_release_authorized": False,
    }
    if not json_value_equal_typed(private, expected_private):
        raise ValueError("round-three Stage-B private authorization differs")


def validate_input_manifest(
    manifest_path: Path,
    chunks_dir: Path,
    *,
    stage: str,
    contract: Mapping[str, Any],
    allow_unfrozen_review: bool = False,
) -> tuple[dict[str, Any], str, list[Path]]:
    """Authenticate the exact manifest, selection, chunks, rows, and IDs."""
    if manifest_path.parent.resolve() != chunks_dir.parent.resolve():
        raise ValueError("input manifest and chunks must share one export root")
    if chunks_dir.resolve() != (manifest_path.parent / "chunks").resolve():
        raise ValueError("chunks directory must be the manifest's exact chunks path")
    manifest_hash = file_sha256(manifest_path)
    manifest = strict_json_loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(manifest, dict):
        raise TypeError("input manifest is not an object")
    if _is_round3_stage_a2_contract(contract) and not allow_unfrozen_review:
        if not FROZEN_RECALL40_ROUND3_STAGE_A2_CONTRACT_SHA256:
            raise RuntimeError(
                "recall40 round-three Stage-A2 production contract is not frozen"
            )
        if contract.get("line_ledger_contract_sha256") != (
            FROZEN_RECALL40_ROUND3_STAGE_A2_CONTRACT_SHA256
        ):
            raise ValueError(
                "recall40 round-three Stage-A2 production contract differs"
            )
    if _is_round3_stage_b_contract(contract) and not allow_unfrozen_review:
        if not FROZEN_RECALL40_ROUND3_STAGE_B_CONTRACT_SHA256:
            raise RuntimeError(
                "recall40 round-three Stage-B production contract is not frozen"
            )
        if contract.get("line_ledger_contract_sha256") != (
            FROZEN_RECALL40_ROUND3_STAGE_B_CONTRACT_SHA256
        ):
            raise ValueError(
                "recall40 round-three Stage-B production contract differs"
            )
    if allow_unfrozen_review:
        exact_round3_a = (
            stage == LINE_DECISION_STAGE
            and manifest.get("mode") == "line_decision_recall40_round3"
            and contract == computed_recall40_round3_stage_a_contract()
        )
        exact_round3_a2 = (
            stage == LINE_ADJUDICATION_STAGE
            and manifest.get("mode")
            == "line_adjudication_recall40_round3"
            and contract == computed_recall40_round3_stage_a2_contract()
        )
        exact_recall_a2 = (
            stage == LINE_ADJUDICATION_STAGE
            and manifest.get("mode") == "line_adjudication_recall_pilot40"
            and contract.get("contract_version")
            == "candidate_endorsement_recall40_stage_a2_v1"
            and contract.get("upstream_stage_a_v2_contract_sha256")
            == get_line_ledger_contract()["line_ledger_contract_sha256"]
        )
        exact_recall_b = (
            stage == RECORD_EXTRACTION_STAGE
            and manifest.get("mode") == "record_extraction_recall_pilot40"
            and contract.get("contract_version")
            == "candidate_endorsement_recall40_stage_b_v1"
        )
        exact_round3_b = (
            stage == RECORD_EXTRACTION_STAGE
            and manifest.get("mode") == "record_extraction_recall40_round3"
            and contract == computed_recall40_round3_stage_b_contract()
        )
        if not (
            exact_round3_a or exact_round3_a2
            or exact_round3_b or exact_recall_a2 or exact_recall_b
        ):
            raise ValueError("unfrozen review is restricted to exact recall40 stages")
    payload = dict(manifest)
    recorded_payload_hash = payload.pop("manifest_payload_sha256", None)
    if recorded_payload_hash != sha256_json(payload):
        raise ValueError("input manifest payload SHA-256 mismatch")
    fixed = {
        "stage": stage,
        "route": "endorsements",
        "line_ledger_contract": dict(contract),
        "line_ledger_contract_sha256": contract[
            "line_ledger_contract_sha256"
        ],
        "frozen_line_ledger_contract_sha256": (
            None if allow_unfrozen_review else contract[
                "line_ledger_contract_sha256"
            ]
        ),
        "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
        "line_batch_spec_sha256": contract["line_batch_spec_sha256"],
        "json_schema_catalog_sha256": contract[
            "json_schema_catalog_sha256"
        ][stage],
        "json_schema_variant_sha256": contract[
            "json_schema_variant_sha256"
        ][stage],
        "complete_parent_closure": True,
        "contract_is_frozen": not allow_unfrozen_review,
        "unchanged_route_hashes": {
            "issue_prompt_sha256": contract[
                "unchanged_issue_prompt_sha256"
            ],
            "issue_schema_sha256": contract[
                "unchanged_issue_schema_sha256"
            ],
            "biography_prompt_sha256": contract[
                "unchanged_biography_prompt_sha256"
            ],
            "biography_schema_sha256": contract[
                "unchanged_biography_schema_sha256"
            ],
        },
    }
    if stage == LINE_ADJUDICATION_STAGE:
        fixed.update(_adjudication_contract_bindings(contract))
    if (
        stage == RECORD_EXTRACTION_STAGE
        and manifest.get("mode") == "record_extraction_recall40_round3"
    ):
        fixed.update({
            "stage_b_two_pass_provenance_spec_sha256": (
                RECALL40_ROUND3_STAGE_B_PROVENANCE_SPEC_SHA256
            ),
            "record_extraction_batch_schema_rule_sha256": (
                RECALL40_ROUND3_STAGE_B_SCHEMA_RULE_SHA256
            ),
            "recall40_round3_stage_b_semantic_spec_sha256": (
                RECALL40_ROUND3_STAGE_B_SEMANTIC_SPEC_SHA256
            ),
            "recall40_round3_stage_b_source_catalog_spec_sha256": (
                RECALL40_ROUND3_STAGE_B_SOURCE_CATALOG_SPEC_SHA256
            ),
            "recall40_round3_stage_b_record_projection_spec_sha256": (
                RECALL40_ROUND3_STAGE_B_RECORD_PROJECTION_SPEC_SHA256
            ),
            "recall40_round3_stage_b_rendering_spec_sha256": (
                RECALL40_ROUND3_STAGE_B_RENDERING_SPEC_SHA256
            ),
            "recall40_round3_stage_b_scope_spec_sha256": (
                RECALL40_ROUND3_STAGE_B_SCOPE_SPEC_SHA256
            ),
        })
    if (
        stage == RECORD_EXTRACTION_STAGE
        and manifest.get("mode") == "record_extraction_recall_pilot40"
    ):
        expected_manifest_fields = {
            "builder", "builder_version", "chunk_size", "complete_parent_closure",
            "contract_is_frozen", "corrected_71c_issue_audit_gate",
            "expected_batch_counts", "expected_batch_ids",
            "expected_batch_ids_sha256", "expected_stage_batch_count",
            "exported_records", "files", "frozen_line_ledger_contract_sha256",
            "inventory_census", "json_schema_catalog_sha256",
            "json_schema_variant_sha256", "line_batch_spec_sha256",
            "line_inventory_spec_sha256", "line_ledger_contract",
            "line_ledger_contract_sha256", "manifest_payload_sha256", "mode",
            "ordered_schema_variant_hashes_sha256",
            "ordered_schema_variant_ids_sha256", "private_recall40_stage_b",
            "record_extraction_batch_schema_rule_sha256", "route",
            "schema_variant_counts", "schema_variant_hashes_used",
            "selected_parent_ids", "selected_parent_ids_sha256", "source_export",
            "stage", "stage_b_two_pass_provenance_spec_sha256",
            "stage_b_upstream_pass_count", "unchanged_route_hashes",
        }
        if set(manifest) != expected_manifest_fields:
            raise ValueError("private recall40 Stage-B manifest field set differs")
        if (
            manifest.get("builder") != "scripts/build_candidate_endorsement_line_ledger.py"
            or manifest.get("builder_version")
            != "candidate_endorsement_line_ledger_export_v1"
            or type(manifest.get("chunk_size")) is not int
            or manifest["chunk_size"] != 60
            or not json_value_equal_typed(manifest.get("expected_batch_counts"), {
                "line_decision_batches": 85,
                "record_extraction_batches_maximum": 158,
            })
            or not json_value_equal_typed(
                manifest.get("inventory_census"), FROZEN_RECALL40_INVENTORY
            )
            or sha256_json(manifest.get("corrected_71c_issue_audit_gate"))
            != "bdd1601458bbacf160ed0fc639b64b5febc5903ad91b11c01fc1c2cb4d443bc0"
        ):
            raise ValueError("private recall40 Stage-B fixed census differs")
        if not json_value_equal_typed(
            manifest.get("source_export"), FROZEN_RECALL40_SOURCE_EXPORT
        ):
            raise ValueError("private recall40 Stage-B source export differs")
        if sorted(path.name for path in manifest_path.parent.iterdir()) != [
            "chunks", "manifest.json", "selection.csv"
        ]:
            raise ValueError("private recall40 Stage-B export inventory differs")
        fixed["record_extraction_batch_schema_rule_sha256"] = (
            RECORD_EXTRACTION_BATCH_SCHEMA_RULE_SHA256
        )
        fixed["stage_b_two_pass_provenance_spec_sha256"] = (
            contract["stage_b_two_pass_provenance_spec_sha256"]
        )
        private_b = manifest.get("private_recall40_stage_b")
        if (
            type(manifest.get("stage_b_upstream_pass_count")) is not int
            or manifest["stage_b_upstream_pass_count"] != 2
            or not isinstance(private_b, Mapping)
            or private_b.get("evaluation_report_sha256")
            != RECALL40_STAGE_B_SCOPE_SPEC["stage_a2_evaluation_report_sha256"]
            or private_b.get("stage_a_input_manifest_sha256")
            != RECALL40_STAGE_B_SCOPE_SPEC["stage_a_input_manifest_sha256"]
            or private_b.get("stage_a2_input_manifest_sha256")
            != RECALL40_STAGE_B_SCOPE_SPEC["stage_a2_input_manifest_sha256"]
            or private_b.get("stage_a2_generation_config_sha256")
            != RECALL40_STAGE_B_SCOPE_SPEC["stage_a2_generation_config_sha256"]
            or private_b.get("stage_a2_output_batch_ids_sha256")
            != RECALL40_STAGE_B_SCOPE_SPEC["stage_a2_output_batch_ids_sha256"]
            or private_b.get("final_decisions_sha256")
            != RECALL40_STAGE_B_SCOPE_SPEC["final_decisions_sha256"]
            or private_b.get("per_line_provenance_sha256")
            != RECALL40_STAGE_B_SCOPE_SPEC["per_line_provenance_sha256"]
            or private_b.get("gate_decision")
            != "GO_BUILD_BOUNDED_STAGE_B_INPUTS"
            or private_b.get("bounded_stage_b_input_build_authorized") is not True
            or private_b.get("builder_performed_inference") is not False
            or private_b.get("hive_submission_authorized") is not False
            or private_b.get("stage_b_hive_submission_authorized") is not False
            or private_b.get("full_expansion_or_release_authorized") is not False
        ):
            raise ValueError("private recall40 Stage-B authorization differs")
        private_semantic = dict(private_b)
        private_semantic.pop("stage_b_ordered_batch_sha256", None)
        if sha256_json(private_semantic) != RECALL40_STAGE_B_SCOPE_SPEC[
            "private_authorization_semantic_sha256"
        ]:
            raise ValueError("private recall40 Stage-B authorization ledger differs")
        if len(manifest.get("selected_parent_ids", [])) != 40:
            raise ValueError("private recall40 Stage-B parent closure differs")
    if (
        stage == RECORD_EXTRACTION_STAGE
        and manifest.get("mode") == "record_extraction_recall40_round2"
    ):
        expected_manifest_fields = {
            "builder", "builder_version", "chunk_size", "complete_parent_closure",
            "contract_is_frozen", "corrected_71c_issue_audit_gate",
            "expected_batch_counts", "expected_batch_ids",
            "expected_batch_ids_sha256", "expected_stage_batch_count",
            "exported_records", "files", "frozen_line_ledger_contract_sha256",
            "inventory_census", "json_schema_catalog_sha256",
            "json_schema_variant_sha256", "line_batch_spec_sha256",
            "line_inventory_spec_sha256", "line_ledger_contract",
            "line_ledger_contract_sha256", "manifest_payload_sha256", "mode",
            "ordered_schema_variant_hashes_sha256",
            "ordered_schema_variant_ids_sha256",
            "private_recall40_round2_stage_b",
            "record_extraction_batch_schema_rule_sha256", "route",
            "schema_variant_counts", "schema_variant_hashes_used",
            "selected_parent_ids", "selected_parent_ids_sha256", "source_export",
            "stage", "stage_b_three_pass_provenance_spec_sha256",
            "stage_b_upstream_pass_count", "unchanged_route_hashes",
        }
        private_b = manifest.get("private_recall40_round2_stage_b")
        expected_private = {
            key: value
            for key, value in RECALL40_ROUND2_STAGE_B_SCOPE_SPEC.items()
            if key not in {
                "version", "bounded_private_inference_only",
                "selected_parent_ids_sha256", "stage_b_batch_count",
                "stage_b_parent_with_anchors_count",
                "stage_b_chunk_record_counts",
                "ordered_anchor_source_line_ids_sha256",
                "stage_b_used_line_provenance_sha256",
                "semantic_batch_ledger_sha256",
            }
        }
        expected_private.update({
            "builder_performed_inference": False,
            "hive_submission_authorized": False,
        })
        if (
            set(manifest) != expected_manifest_fields
            or type(manifest.get("stage_b_upstream_pass_count")) is not int
            or manifest["stage_b_upstream_pass_count"] != 3
            or type(manifest.get("chunk_size")) is not int
            or manifest["chunk_size"] != 60
            or not json_value_equal_typed(private_b, expected_private)
            or manifest.get("selected_parent_ids_sha256")
            != RECALL40_ROUND2_STAGE_B_SCOPE_SPEC[
                "selected_parent_ids_sha256"
            ]
            or not isinstance(manifest.get("selected_parent_ids"), list)
            or len(manifest["selected_parent_ids"]) != 40
            or len(set(manifest["selected_parent_ids"])) != 40
            or sha256_json(manifest["selected_parent_ids"])
            != RECALL40_ROUND2_STAGE_B_SCOPE_SPEC[
                "selected_parent_ids_sha256"
            ]
        ):
            raise ValueError("private recall40 round-two Stage-B scope differs")
        fixed["stage_b_three_pass_provenance_spec_sha256"] = contract[
            "stage_b_three_pass_provenance_spec_sha256"
        ]
        fixed["record_extraction_batch_schema_rule_sha256"] = contract[
            "record_extraction_batch_schema_rule_sha256"
        ]
    if (
        stage == RECORD_EXTRACTION_STAGE
        and manifest.get("mode") not in {
            "record_extraction_recall_pilot40",
            "record_extraction_recall40_round2",
            "record_extraction_recall40_round3",
        }
    ):
        if manifest.get("mode") != "record_extraction_preflight_from_stage_a3":
            raise ValueError("Stage-B manifest mode is not authenticated A3")
        fixed["stage_b_three_pass_provenance_spec_sha256"] = (
            contract["stage_b_three_pass_provenance_spec_sha256"]
        )
        fixed["record_extraction_batch_schema_rule_sha256"] = (
            contract["record_extraction_batch_schema_rule_sha256"]
        )
    for field, expected in fixed.items():
        if not json_value_equal_typed(manifest.get(field), expected):
            raise ValueError(f"input manifest {field} mismatch")
    if manifest.get("source_export", {}).get(
        "source_corpus_sha256"
    ) != FROZEN_SOURCE_CORPUS_SHA256:
        raise ValueError("input manifest source corpus mismatch")
    files = manifest.get("files")
    if not isinstance(files, Mapping):
        raise ValueError("input manifest lacks file inventory")
    selection = files.get("selection")
    if not isinstance(selection, Mapping):
        raise ValueError("input manifest lacks selection inventory")
    selection_name = selection.get("file")
    if not isinstance(selection_name, str) or Path(selection_name).name != selection_name:
        raise ValueError("unsafe selection filename")
    selection_path = manifest_path.parent / selection_name
    if (
        not selection_path.is_file()
        or file_sha256(selection_path) != selection.get("sha256")
        or selection_path.stat().st_size != selection.get("bytes")
    ):
        raise ValueError("input selection bytes differ from manifest")
    with selection_path.open(newline="", encoding="utf-8") as handle:
        selection_reader = csv.DictReader(handle)
        parsed_selection_rows = list(selection_reader)
        parsed_selection_columns = list(selection_reader.fieldnames or [])
    if (
        selection.get("columns") != parsed_selection_columns
        or selection.get("rows") != len(parsed_selection_rows)
        or any(set(row) != set(parsed_selection_columns)
               for row in parsed_selection_rows)
    ):
        raise ValueError("selection declared schema/count differs from CSV")
    entries = files.get("chunks")
    if not isinstance(entries, list) or not entries:
        raise ValueError("input manifest has no chunks")
    paths: list[Path] = []
    batch_ids: list[str] = []
    all_batches: list[dict[str, Any]] = []
    for entry in entries:
        if not isinstance(entry, Mapping):
            raise TypeError("input chunk entry is not an object")
        name = entry.get("file")
        if not isinstance(name, str) or Path(name).name != name:
            raise ValueError("unsafe input chunk filename")
        path = chunks_dir / name
        if (
            not path.is_file()
            or file_sha256(path) != entry.get("sha256")
            or path.stat().st_size != entry.get("bytes")
        ):
            raise ValueError(f"input chunk bytes differ from manifest: {name}")
        rows = read_gzip_jsonl(path)
        if len(rows) != entry.get("records"):
            raise ValueError(f"input chunk row count differs: {name}")
        if sha256_json(rows) != entry.get("content_sha256"):
            raise ValueError(f"input chunk content digest differs: {name}")
        ids = [str(row.get("batch_id")) for row in rows]
        if not ids or len(ids) != len(set(ids)):
            raise ValueError(f"input chunk has missing/duplicate batch IDs: {name}")
        if (
            entry.get("first_document_id") != ids[0]
            or entry.get("last_document_id") != ids[-1]
        ):
            raise ValueError(f"input chunk endpoint IDs differ: {name}")
        if any(
            row.get("document_id") != row.get("batch_id")
            or row.get("stage") != stage
            or row.get("line_inventory_spec_sha256")
            != LINE_INVENTORY_SPEC_SHA256
            or row.get("line_batch_spec_sha256")
            != contract["line_batch_spec_sha256"]
            or (
                stage == LINE_ADJUDICATION_STAGE
                and (
                    row.get("line_adjudication_batch_schema_rule_sha256")
                    != _adjudication_contract_bindings(contract)[
                        "line_adjudication_batch_schema_rule_sha256"
                    ]
                    or
                    row.get("line_adjudication_spec_sha256")
                    != _adjudication_contract_bindings(contract)[
                        "line_adjudication_spec_sha256"
                    ]
                    or row.get("upstream_stage_a_contract_sha256")
                    != _adjudication_contract_bindings(contract)[
                        "upstream_stage_a_contract_sha256"
                    ]
                )
            )
            or (
                stage == RECORD_EXTRACTION_STAGE
                and (
                    (
                        row.get("stage_b_two_pass_provenance_spec_sha256")
                        != contract["stage_b_two_pass_provenance_spec_sha256"]
                        if _is_two_pass_stage_b_contract(contract)
                        else row.get("stage_b_three_pass_provenance_spec_sha256")
                        != contract["stage_b_three_pass_provenance_spec_sha256"]
                    )
                    or row.get("record_extraction_batch_schema_rule_sha256")
                    != contract["record_extraction_batch_schema_rule_sha256"]
                )
            )
            for row in rows
        ):
            raise ValueError(f"input chunk row contract differs: {name}")
        paths.append(path)
        batch_ids.extend(ids)
        all_batches.extend(rows)
    actual_names = sorted(path.name for path in chunks_dir.glob("*.jsonl.gz"))
    listed_names = sorted(path.name for path in paths)
    if actual_names != listed_names:
        raise ValueError("unlisted or missing input chunks")
    if any(chunks_dir.glob("*.tmp-*")):
        raise ValueError("temporary input chunk artifact is present")
    expected_ids = manifest.get("expected_batch_ids")
    if (
        len(batch_ids) != len(set(batch_ids))
        or not isinstance(expected_ids, list)
        or len(expected_ids) != len(set(str(value) for value in expected_ids))
        or batch_ids != [
        str(value) for value in expected_ids
        ]
    ):
        raise ValueError("input batch ID order/coverage differs from manifest")
    if (
        len(batch_ids) != manifest.get("expected_stage_batch_count")
        or len(batch_ids) != manifest.get("exported_records")
        or sha256_json(batch_ids) != manifest.get("expected_batch_ids_sha256")
    ):
        raise ValueError("input batch count/digest differs from manifest")
    variant_inventory = schema_variant_inventory(all_batches)
    if (
        stage == RECORD_EXTRACTION_STAGE
        and manifest.get("mode") == "record_extraction_recall40_round2"
    ):
        expected_chunk_counts = RECALL40_ROUND2_STAGE_B_SCOPE_SPEC[
            "stage_b_chunk_record_counts"
        ]
        if (
            set(files) != {"selection", "chunks"}
            or set(selection) != {"bytes", "columns", "file", "rows", "sha256"}
            or any(set(entry) != {
                "bytes", "content_sha256", "file", "first_document_id",
                "last_document_id", "records", "sha256",
            } for entry in entries)
            or [entry.get("file") for entry in entries] != [
                f"chunk_{index:05d}.jsonl.gz"
                for index in range(len(expected_chunk_counts))
            ]
            or [entry.get("records") for entry in entries]
            != expected_chunk_counts
            or len(all_batches)
            != RECALL40_ROUND2_STAGE_B_SCOPE_SPEC["stage_b_batch_count"]
        ):
            raise ValueError("private recall40 round-two Stage-B inventory differs")
        with selection_path.open(newline="", encoding="utf-8") as handle:
            selection_rows = list(csv.DictReader(handle))
        if (
            [row.get("parent_document_id") for row in selection_rows]
            != manifest["selected_parent_ids"]
            or any(
                row.get("selection_group") != "private_recall_pilot40"
                for row in selection_rows
            )
        ):
            raise ValueError("private recall40 round-two Stage-B selection differs")
        anchor_ids = [
            str(target["source_line_id"])
            for batch in all_batches for target in batch["targets"]
        ]
        batch_parents = {
            str(batch["parent_document_id"]) for batch in all_batches
        }
        if (
            len(anchor_ids) != RECALL40_ROUND2_STAGE_B_SCOPE_SPEC[
                "final_anchor_count"
            ]
            or len(anchor_ids) != len(set(anchor_ids))
            or sha256_json(anchor_ids)
            != RECALL40_ROUND2_STAGE_B_SCOPE_SPEC[
                "ordered_anchor_source_line_ids_sha256"
            ]
            or len(batch_parents)
            != RECALL40_ROUND2_STAGE_B_SCOPE_SPEC[
                "stage_b_parent_with_anchors_count"
            ]
            or not batch_parents.issubset(set(manifest["selected_parent_ids"]))
        ):
            raise ValueError("private recall40 round-two Stage-B anchor closure differs")
        used_line_provenance: dict[str, dict[str, Any]] = {}
        for batch in all_batches:
            for target in batch["targets"]:
                provenance_rows = [
                    target.get("anchor_line_decision_provenance"),
                    *target.get("record_block_line_decision_provenance", []),
                ]
                for provenance_row in provenance_rows:
                    if not isinstance(provenance_row, Mapping):
                        raise ValueError("round-two Stage-B inline provenance is invalid")
                    line_id = provenance_row.get("source_line_id")
                    if not isinstance(line_id, str) or not line_id:
                        raise ValueError("round-two Stage-B provenance identity is invalid")
                    prior = used_line_provenance.setdefault(
                        line_id, dict(provenance_row)
                    )
                    if not json_value_equal_typed(prior, provenance_row):
                        raise ValueError("round-two Stage-B provenance copies differ")
            identity = {
                "stage": RECORD_EXTRACTION_STAGE,
                "contract": contract["line_ledger_contract_sha256"],
                "parent_document_id": batch["parent_document_id"],
                "batch_index": batch["batch_index_within_parent"],
                "anchor_line_ids": [
                    target["source_line_id"] for target in batch["targets"]
                ],
                "final_decisions_sha256": batch["final_line_decisions_sha256"],
                "three_pass_provenance_sha256": batch[
                    "three_pass_line_provenance_sha256"
                ],
                "stage_b_three_pass_provenance_spec_sha256": batch[
                    "stage_b_three_pass_provenance_spec_sha256"
                ],
                "record_extraction_batch_schema_rule_sha256": batch[
                    "record_extraction_batch_schema_rule_sha256"
                ],
            }
            expected_batch_id = sha256_json(identity)
            if (
                batch.get("batch_id") != expected_batch_id
                or batch.get("document_id") != expected_batch_id
            ):
                raise ValueError("round-two Stage-B batch identity differs")
        if sha256_json(dict(sorted(used_line_provenance.items()))) != (
            RECALL40_ROUND2_STAGE_B_SCOPE_SPEC[
                "stage_b_used_line_provenance_sha256"
            ]
        ):
            raise ValueError("round-two Stage-B used-line provenance differs")
        semantic_drop = {
            "batch_id", "document_id", "json_schema_catalog_sha256",
            "json_schema_sha256", "json_schema_variant_id", "target_slot_count",
        }
        semantic_batches = [
            {key: value for key, value in batch.items() if key not in semantic_drop}
            for batch in all_batches
        ]
        if sha256_json(semantic_batches) != RECALL40_ROUND2_STAGE_B_SCOPE_SPEC[
            "semantic_batch_ledger_sha256"
        ]:
            raise ValueError("round-two Stage-B semantic ledger differs")
    if (
        stage == RECORD_EXTRACTION_STAGE
        and manifest.get("mode") == "record_extraction_recall_pilot40"
    ):
        if (
            set(files) != {"selection", "chunks"}
            or set(selection) != {"bytes", "columns", "file", "rows", "sha256"}
            or any(set(entry) != {
                "bytes", "content_sha256", "file", "first_document_id",
                "last_document_id", "records", "sha256",
            } for entry in entries)
            or [entry.get("file") for entry in entries]
            != ["chunk_00000.jsonl.gz", "chunk_00001.jsonl.gz"]
            or [entry.get("records") for entry in entries] != [60, 2]
        ):
            raise ValueError("private recall40 Stage-B nested inventory differs")
        private_b = manifest["private_recall40_stage_b"]
        if (
            private_b.get("stage_b_batch_count") != len(all_batches)
            or private_b.get("stage_b_ordered_batch_sha256")
            != sha256_json([sha256_json(batch) for batch in all_batches])
            or len(all_batches) != 62
            or sum(len(batch["targets"]) for batch in all_batches) != 360
        ):
            raise ValueError("private recall40 Stage-B batch/target closure differs")
        batch_parents = {
            str(batch["parent_document_id"]) for batch in all_batches
        }
        if not batch_parents.issubset(set(manifest["selected_parent_ids"])):
            raise ValueError("private recall40 Stage-B batch parent is out of scope")
    for field, actual in variant_inventory.items():
        if not json_value_equal_typed(manifest.get(field), actual):
            raise ValueError(f"input manifest {field} differs from batches")
    if (
        stage == LINE_ADJUDICATION_STAGE
        and manifest.get("mode") == "line_adjudication_recall40_round3"
    ):
        _validate_round3_stage_a2_manifest(
            manifest,
            contract=contract,
            files=files,
            selection=selection,
            parsed_selection_rows=parsed_selection_rows,
            parsed_selection_columns=parsed_selection_columns,
            entries=entries,
            all_batches=all_batches,
            manifest_path=manifest_path,
            chunks_dir=chunks_dir,
        )
    if (
        stage == RECORD_EXTRACTION_STAGE
        and manifest.get("mode") == "record_extraction_recall40_round3"
    ):
        _validate_round3_stage_b_manifest(
            manifest,
            contract=contract,
            files=files,
            selection=selection,
            parsed_selection_rows=parsed_selection_rows,
            parsed_selection_columns=parsed_selection_columns,
            entries=entries,
            all_batches=all_batches,
            manifest_path=manifest_path,
            chunks_dir=chunks_dir,
        )
    if (
        stage == RECORD_EXTRACTION_STAGE
        and manifest.get("mode") == "record_extraction_recall_pilot40"
    ):
        selected_ids = manifest.get("selected_parent_ids")
        if (
            not isinstance(selected_ids, list)
            or len(selected_ids) != 40
            or len(set(selected_ids)) != 40
            or sha256_json(selected_ids) != FROZEN_RECALL40_SELECTED_PARENT_IDS_SHA256
            or manifest.get("selected_parent_ids_sha256")
            != FROZEN_RECALL40_SELECTED_PARENT_IDS_SHA256
        ):
            raise ValueError("private recall40 Stage-B selected parents differ")
        with selection_path.open(newline="", encoding="utf-8") as handle:
            selection_rows = list(csv.DictReader(handle))
        selection_ids = [row.get("parent_document_id") for row in selection_rows]
        if (
            selection_ids != selected_ids
            or selection.get("sha256")
            != RECALL40_STAGE_B_SCOPE_SPEC["selection_csv_sha256"]
            or any(row.get("selection_group") != "private_recall_pilot40"
                   for row in selection_rows)
            or sha256_json([
                {key: row[key] for key in list(row)[:16]}
                for row in selection_rows
            ]) != FROZEN_RECALL40_STAGE_A_SELECTION_BASE_ROWS_SHA256
        ):
            raise ValueError("private recall40 Stage-B selection differs")
        anchor_ids = [
            str(target["source_line_id"])
            for batch in all_batches for target in batch["targets"]
        ]
        anchor_parents = []
        for batch in all_batches:
            parent_id = str(batch["parent_document_id"])
            if not anchor_parents or anchor_parents[-1] != parent_id:
                anchor_parents.append(parent_id)
        if (
            len(anchor_ids) != len(set(anchor_ids))
            or sha256_json(anchor_ids) != RECALL40_STAGE_B_SCOPE_SPEC[
                "ordered_anchor_source_line_ids_sha256"
            ]
            or sha256_json(anchor_parents) != RECALL40_STAGE_B_SCOPE_SPEC[
                "ordered_anchor_parent_ids_sha256"
            ]
        ):
            raise ValueError("private recall40 Stage-B anchor closure differs")
        semantic_drop = {
            "batch_id", "document_id", "json_schema_catalog_sha256",
            "json_schema_sha256", "json_schema_variant_id", "target_slot_count",
        }
        semantic_batches = [
            {key: value for key, value in batch.items() if key not in semantic_drop}
            for batch in all_batches
        ]
        if sha256_json(semantic_batches) != RECALL40_STAGE_B_SCOPE_SPEC[
            "semantic_batch_ledger_sha256"
        ]:
            raise ValueError("private recall40 Stage-B semantic ledger differs")
        for batch in all_batches:
            identity = {
                "stage": RECORD_EXTRACTION_STAGE,
                "contract": contract["line_ledger_contract_sha256"],
                "parent_document_id": batch["parent_document_id"],
                "batch_index": batch["batch_index_within_parent"],
                "anchor_line_ids": [
                    target["source_line_id"] for target in batch["targets"]
                ],
                "final_decisions_sha256": batch["final_line_decisions_sha256"],
                "two_pass_provenance_sha256": batch[
                    "two_pass_line_provenance_sha256"
                ],
                "stage_b_two_pass_provenance_spec_sha256": batch[
                    "stage_b_two_pass_provenance_spec_sha256"
                ],
                "record_extraction_batch_schema_rule_sha256": batch[
                    "record_extraction_batch_schema_rule_sha256"
                ],
            }
            expected_batch_id = sha256_json(identity)
            if (
                batch.get("batch_id") != expected_batch_id
                or batch.get("document_id") != expected_batch_id
            ):
                raise ValueError("private recall40 Stage-B batch identity differs")
    if stage == LINE_DECISION_STAGE:
        pilot_mode = manifest.get("mode") == "line_decision_recall_pilot40"
        round2_mode = manifest.get("mode") == "line_decision_recall40_round2"
        round3_mode = manifest.get("mode") == "line_decision_recall40_round3"
        private = manifest.get("private_recall40")
        if not pilot_mode and private is not None:
            raise ValueError("nonpilot Stage-A manifest carries private recall40 block")
        if pilot_mode:
            expected_private = {
                "enabled": True,
                "private_selection_file_sha256": FROZEN_RECALL40_PRIVATE_SELECTION_SHA256,
                "canonical_assignment_sha256": FROZEN_RECALL40_ASSIGNMENT_SHA256,
                "selected_parent_count": 40,
                "selected_parent_ids_sha256": FROZEN_RECALL40_SELECTED_PARENT_IDS_SHA256,
                "inventory_census": FROZEN_RECALL40_INVENTORY,
                "batch_census": {"line_decision_batches": 85,
                                 "record_extraction_batches_maximum": 158},
                "source_preview_or_human_task_rendered": False,
                "hive_submission_authorized": False,
                "reserve80_loaded": False,
                "downstream_derivation_shell": {
                    "stage_a2": "derive only after authenticated complete Stage-A outputs and separate review",
                    "stage_b": "derive only after authenticated Stage-A/A2 residual handling and separate review",
                    "full_expansion_or_release_authorized": False,
                },
            }
            if not json_value_equal_typed(private, expected_private):
                raise ValueError("private recall40 manifest contract differs")
            expected_counts = {"line_decision_batches": 85,
                               "record_extraction_batches_maximum": 158}
            if (not json_value_equal_typed(manifest.get("expected_batch_counts"), expected_counts)
                    or manifest.get("chunk_size") != 60
                    or [entry.get("records") for entry in entries] != [60, 25]
                    or not json_value_equal_typed(manifest.get("source_export"),
                                                  FROZEN_RECALL40_SOURCE_EXPORT)
                    or not json_value_equal_typed(selection, {
                        "file": "selection.csv", "rows": 40, "bytes": 10051,
                        "sha256": FROZEN_RECALL40_SELECTION_SHA256,
                        "columns": ["selection_group", "pilot_control_kind",
                            "parent_document_id", "candidate_cycle_id", "candidate_icpsr",
                            "state", "office", "year", "party", "segment_count",
                            "physical_line_count", "model_decision_line_count",
                            "line_decision_batch_count", "record_extraction_batch_count_maximum",
                            "parent_line_inventory_sha256", "context_scope"],
                    })):
                raise ValueError("private recall40 source/selection/chunk census differs")
            selected_ids = manifest.get("selected_parent_ids")
            if (not isinstance(selected_ids, list) or len(selected_ids) != 40
                    or len(set(selected_ids)) != 40
                    or sha256_json(selected_ids) != FROZEN_RECALL40_SELECTED_PARENT_IDS_SHA256
                    or manifest.get("selected_parent_ids_sha256")
                    != FROZEN_RECALL40_SELECTED_PARENT_IDS_SHA256
                    or not json_value_equal_typed(manifest.get("inventory_census"),
                                                  FROZEN_RECALL40_INVENTORY)):
                raise ValueError("private recall40 frozen parent coverage differs")
            with selection_path.open(newline="", encoding="utf-8") as handle:
                selection_rows = list(csv.DictReader(handle))
            if any(row.get("selection_group") != "private_recall_pilot40"
                   for row in selection_rows):
                raise ValueError("private recall40 selection group differs")
            selection_ids = [row["parent_document_id"] for row in selection_rows]
            parent_sequence = [str(batch.get("parent_document_id")) for batch in all_batches]
            batch_parent_order = [value for index, value in enumerate(parent_sequence)
                                  if index == 0 or value != parent_sequence[index - 1]]
            if (selection_ids != selected_ids or len(batch_parent_order) != 40
                    or set(batch_parent_order) != set(selected_ids)
                    or sha256_json(batch_parent_order)
                    != FROZEN_RECALL40_BATCH_PARENT_ORDER_SHA256):
                raise ValueError("private recall40 parent order differs across artifacts")
        if round2_mode:
            expected_private_round2 = {
                "enabled": True,
                "private_selection_file_sha256": "cda7d58561c439aee65dbb4be391c6c9edcbc86a7209625512bf12c619cebdac",
                "round": 2, "seed": 20261827, "selected_parent_count": 40,
                "selected_parent_ids_sha256": "25962ac169c3f88bccbf84ef4767e62dd15f82267017ac126efa50e3b43ec191",
                "route_revision_spec_sha256": "85fc9a714f20175297c4bbf923ee0cb1535b53a2f39a7d4b6036d420a4382365",
                "source_preview_or_human_task_rendered": False,
                "hive_submission_authorized": False,
                "stage_a2_stage_b_full_release_authorized": False,
            }
            if not json_value_equal_typed(
                manifest.get("private_recall40_round2"), expected_private_round2
            ):
                raise ValueError("private recall40 round-two scope differs")
            if (
                manifest.get("selected_parent_ids_sha256")
                != expected_private_round2["selected_parent_ids_sha256"]
                or manifest.get("expected_stage_batch_count") != 270
                or manifest.get("exported_records") != 270
                or manifest.get("chunk_size") != 60
                or [entry.get("records") for entry in entries] != [60, 60, 60, 60, 30]
                or selection.get("sha256") != "4ac306cf4b59d11c4a05fc466db2b95d8a3abc99195a19f8edda54e995ecf44a"
            ):
                raise ValueError("private recall40 round-two census differs")
        if round3_mode:
            expected_contract = computed_recall40_round3_stage_a_contract()
            if contract != expected_contract:
                raise ValueError("private recall40 round-three contract differs")
            scope = RECALL40_ROUND3_STAGE_A_SCOPE_SPEC
            expected_manifest_fields = {
                "builder", "builder_version", "chunk_size",
                "complete_parent_closure", "contract_is_frozen",
                "corrected_71c_issue_audit_gate", "expected_batch_counts",
                "expected_batch_ids", "expected_batch_ids_sha256",
                "expected_stage_batch_count", "exported_records", "files",
                "frozen_line_ledger_contract_sha256", "inventory_census",
                "json_schema_catalog_sha256", "json_schema_variant_sha256",
                "line_batch_spec_sha256", "line_inventory_spec_sha256",
                "line_ledger_contract", "line_ledger_contract_sha256",
                "manifest_payload_sha256", "mode",
                "ordered_schema_variant_hashes_sha256",
                "ordered_schema_variant_ids_sha256",
                "private_recall40_round3", "route", "schema_variant_counts",
                "schema_variant_hashes_used", "selected_parent_ids",
                "selected_parent_ids_sha256", "source_export", "stage",
                "unchanged_route_hashes",
            }
            expected_selection_columns = [
                "selection_group", "pilot_control_kind", "parent_document_id",
                "candidate_cycle_id", "candidate_icpsr", "state", "office",
                "year", "party", "segment_count", "physical_line_count",
                "model_decision_line_count", "line_decision_batch_count",
                "record_extraction_batch_count_maximum",
                "parent_line_inventory_sha256", "context_scope",
            ]
            expected_chunk_counts = scope["chunk_record_counts"]
            expected_chunk_names = [
                f"chunk_{index:05d}.jsonl.gz"
                for index in range(len(expected_chunk_counts))
            ]
            if (
                set(manifest) != expected_manifest_fields
                or manifest.get("builder")
                != "scripts/build_candidate_endorsement_line_ledger.py"
                or manifest.get("builder_version")
                != "candidate_endorsement_line_ledger_export_v1"
                or set(files) != {"selection", "chunks"}
                or not json_value_equal_typed(selection, {
                    "bytes": scope["selection_csv_bytes"],
                    "columns": expected_selection_columns,
                    "file": "selection.csv",
                    "rows": scope["parent_count"],
                    "sha256": scope["selection_csv_sha256"],
                })
                or any(set(entry) != {
                    "bytes", "content_sha256", "file", "first_document_id",
                    "last_document_id", "records", "sha256",
                } for entry in entries)
                or [entry.get("file") for entry in entries]
                != expected_chunk_names
                or [entry.get("records") for entry in entries]
                != expected_chunk_counts
                or sorted(path.name for path in manifest_path.parent.iterdir())
                != ["chunks", "manifest.json", "selection.csv"]
                or sorted(path.name for path in chunks_dir.iterdir())
                != expected_chunk_names
                or not json_value_equal_typed(
                    manifest.get("source_export"), FROZEN_RECALL40_SOURCE_EXPORT
                )
                or sha256_json(manifest.get("inventory_census"))
                != scope["inventory_census_sha256"]
                or sha256_json(manifest.get("expected_batch_counts"))
                != scope["expected_batch_counts_sha256"]
                or manifest.get("expected_batch_counts", {}).get(
                    "line_decision_batches"
                ) != scope["stage_a_batch_count"]
                or manifest.get("expected_batch_counts", {}).get(
                    "record_extraction_batches_maximum"
                ) != scope["record_extraction_batch_count_maximum"]
                or sha256_json(manifest.get("corrected_71c_issue_audit_gate"))
                != "bdd1601458bbacf160ed0fc639b64b5febc5903ad91b11c01fc1c2cb4d443bc0"
            ):
                raise ValueError("private recall40 round-three closed inventory differs")
            expected_private_round3 = {
                "enabled": True,
                "round": 3,
                "source_round": 2,
                "private_selection_file_sha256": (
                    "cda7d58561c439aee65dbb4be391c6c9edcbc86a7209625512bf12c619cebdac"
                ),
                "selected_parent_count": 40,
                "selected_parent_ids_sha256": (
                    "25962ac169c3f88bccbf84ef4767e62dd15f82267017ac126efa50e3b43ec191"
                ),
                "stage_a_semantic_revision_spec_sha256": contract[
                    "recall40_round3_stage_a_semantic_revision_spec_sha256"
                ],
                "stage_a_context_profile_sha256": contract[
                    "stage_a_context_profile_sha256"
                ],
                "stage_a_line_batch_spec_sha256": contract[
                    "line_batch_spec_sha256"
                ],
                "stage_a_scope_spec_sha256": contract[
                    "recall40_round3_stage_a_scope_spec_sha256"
                ],
                "source_preview_or_human_task_rendered": False,
                "hive_submission_authorized": False,
                "stage_a2_stage_b_full_release_authorized": False,
            }
            selected_ids = manifest.get("selected_parent_ids")
            selection_ids = [
                row.get("parent_document_id") for row in parsed_selection_rows
            ]
            parent_sequence = [
                str(batch.get("parent_document_id")) for batch in all_batches
            ]
            batch_parent_order = [
                value for index, value in enumerate(parent_sequence)
                if index == 0 or value != parent_sequence[index - 1]
            ]
            if (
                not json_value_equal_typed(
                    manifest.get("private_recall40_round3"),
                    expected_private_round3,
                )
                or not isinstance(selected_ids, list)
                or len(selected_ids) != 40
                or len(set(selected_ids)) != 40
                or sha256_json(selected_ids)
                != expected_private_round3["selected_parent_ids_sha256"]
                or manifest.get("selected_parent_ids_sha256")
                != expected_private_round3["selected_parent_ids_sha256"]
                or selection_ids != selected_ids
                or any(
                    row.get("selection_group") != "private_recall_pilot40"
                    for row in parsed_selection_rows
                )
                or batch_parent_order != sorted(selected_ids)
                or manifest.get("expected_stage_batch_count") != 270
                or manifest.get("exported_records") != 270
                or type(manifest.get("chunk_size")) is not int
                or manifest["chunk_size"] != 60
                or [entry.get("records") for entry in entries]
                != [60, 60, 60, 60, 30]
                or selection.get("sha256")
                != "44c415d8224512629d701097bb15b5a69ee4dcf725ba4c830b3e3743bb34ee62"
                or manifest.get("source_export", {}).get(
                    "source_export_manifest_sha256"
                ) != FROZEN_RECALL40_SOURCE_EXPORT[
                    "source_export_manifest_sha256"
                ]
            ):
                raise ValueError("private recall40 round-three scope differs")
            expected_batch_count_by_parent = {
                row["parent_document_id"]: int(row["line_decision_batch_count"])
                for row in parsed_selection_rows
            }
            actual_batch_count_by_parent = Counter(parent_sequence)
            if dict(actual_batch_count_by_parent) != (
                expected_batch_count_by_parent
            ):
                raise ValueError("private recall40 round-three parent batches differ")
            selection_by_parent = {
                str(row["parent_document_id"]): row
                for row in parsed_selection_rows
            }
            catalog_digest_by_parent: dict[str, str] = {}
            expected_target_ids_by_parent: dict[str, list[str]] = {}
            actual_target_ids_by_parent: dict[str, list[str]] = defaultdict(list)
            batch_index_by_parent: Counter[str] = Counter()
            parent_inventory_ledger: list[dict[str, str]] = []
            parent_catalog_ledger: list[dict[str, str]] = []
            parent_prompt_metadata_ledger: list[dict[str, Any]] = []
            prompt_metadata_by_parent: dict[str, dict[str, Any]] = {}
            prompt_metadata_keys = (
                "candidate_cycle_id", "candidate_icpsr", "state", "office",
                "year", "party", "raw_text_sha256", "parent_document_id",
                "extraction_source_text_sha256", "extraction_source_n_chars",
                "source_urls", "parent_line_inventory_sha256",
                "endorsement_line_context",
            )
            for batch in all_batches:
                validate_round3_stage_a_context_batch(batch)
                parent_id = str(batch["parent_document_id"])
                expected_index = batch_index_by_parent[parent_id]
                if batch.get("batch_index_within_parent") != expected_index:
                    raise ValueError(
                        "private recall40 round-three parent batch order differs"
                    )
                batch_index_by_parent[parent_id] += 1
                catalog_digest = str(
                    batch["full_parent_physical_lines_sha256"]
                )
                first_parent_batch = parent_id not in catalog_digest_by_parent
                prior = catalog_digest_by_parent.setdefault(parent_id, catalog_digest)
                if prior != catalog_digest:
                    raise ValueError(
                        "private recall40 round-three parent catalogs differ"
                    )
                target_ids = [
                    str(target["source_line_id"])
                    for target in batch["targets"]
                ]
                prompt_metadata = {
                    key: copy.deepcopy(batch[key]) for key in prompt_metadata_keys
                }
                prior_prompt_metadata = prompt_metadata_by_parent.setdefault(
                    parent_id, prompt_metadata
                )
                if not json_value_equal_typed(
                    prior_prompt_metadata, prompt_metadata
                ):
                    raise ValueError(
                        "private recall40 round-three parent metadata differs"
                    )
                if first_parent_batch:
                    expected_target_ids_by_parent[parent_id] = [
                        str(line["source_line_id"])
                        for line in batch["visible_lines"]
                        if line["model_decision_required"] is True
                    ]
                    selection_row = selection_by_parent[parent_id]
                    if (
                        batch["parent_line_inventory_sha256"]
                        != selection_row["parent_line_inventory_sha256"]
                        or batch["full_parent_line_count"]
                        != int(selection_row["physical_line_count"])
                        or len(expected_target_ids_by_parent[parent_id])
                        != int(selection_row["model_decision_line_count"])
                        or actual_batch_count_by_parent[parent_id]
                        != int(selection_row["line_decision_batch_count"])
                    ):
                        raise ValueError(
                            "private recall40 round-three selection/source differs"
                        )
                    parent_inventory_ledger.append({
                        "parent_document_id": parent_id,
                        "parent_line_inventory_sha256": str(
                            batch["parent_line_inventory_sha256"]
                        ),
                    })
                    parent_catalog_ledger.append({
                        "parent_document_id": parent_id,
                        "full_parent_physical_lines_sha256": catalog_digest,
                        "full_parent_source_line_ids_sha256": str(
                            batch["full_parent_source_line_ids_sha256"]
                        ),
                    })
                    parent_prompt_metadata_ledger.append(prompt_metadata)
                canonical_target_ids = expected_target_ids_by_parent[parent_id][
                    expected_index * LINE_DECISION_SLOT_COUNT:
                    (expected_index + 1) * LINE_DECISION_SLOT_COUNT
                ]
                if target_ids != canonical_target_ids:
                    raise ValueError(
                        "private recall40 round-three target partition differs"
                    )
                actual_target_ids_by_parent[parent_id].extend(target_ids)
                identity = {
                    "stage": LINE_DECISION_STAGE,
                    "contract": contract["line_ledger_contract_sha256"],
                    "parent_document_id": parent_id,
                    "batch_index": batch["batch_index_within_parent"],
                    "source_line_ids": [
                        target["source_line_id"] for target in batch["targets"]
                    ],
                    "stage_a_context_profile_sha256": contract[
                        "stage_a_context_profile_sha256"
                    ],
                    "full_parent_physical_lines_sha256": catalog_digest,
                }
                expected_batch_id = sha256_json(identity)
                if (
                    batch.get("batch_id") != expected_batch_id
                    or batch.get("document_id") != expected_batch_id
                ):
                    raise ValueError(
                        "private recall40 round-three batch identity differs"
                    )
            if actual_target_ids_by_parent != expected_target_ids_by_parent:
                raise ValueError(
                    "private recall40 round-three parent target coverage differs"
                )
            ordered_target_ids = [
                str(target["source_line_id"])
                for batch in all_batches for target in batch["targets"]
            ]
            if (
                len(ordered_target_ids) != scope["model_decision_line_count"]
                or len(ordered_target_ids) != len(set(ordered_target_ids))
                or sha256_json(ordered_target_ids)
                != scope["ordered_target_source_line_ids_sha256"]
                or sha256_json(parent_inventory_ledger)
                != scope["parent_line_inventory_ledger_sha256"]
                or sha256_json(parent_catalog_ledger)
                != scope["parent_catalog_ledger_sha256"]
                or sha256_json(parent_prompt_metadata_ledger)
                != scope["parent_prompt_metadata_ledger_sha256"]
            ):
                raise ValueError(
                    "private recall40 round-three frozen source closure differs"
                )
        elif manifest.get("private_recall40_round3") is not None:
            raise ValueError(
                "non-round-three Stage-A manifest carries round-three scope"
            )
    if (
        stage == LINE_ADJUDICATION_STAGE
        and manifest.get("mode") != "line_adjudication_recall40_round3"
    ):
        round2_a2 = manifest.get("mode") == "line_adjudication_recall40_round2"
        recall40_a2 = (
            manifest.get("mode") == "line_adjudication_recall_pilot40"
            or round2_a2
        )
        expected_upstream_contract = (
            contract["upstream_stage_a_v2_contract_sha256"]
            if recall40_a2 else FROZEN_STAGE_A_V2_CONTRACT_SHA256
        )
        provenance = manifest.get("stage_a_provenance")
        if not isinstance(provenance, Mapping):
            raise ValueError("Stage-A2 manifest lacks authenticated Stage-A provenance")
        provenance_payload = dict(provenance)
        recorded_provenance_hash = provenance_payload.pop(
            "stage_a_provenance_sha256", None
        )
        if recorded_provenance_hash != sha256_json(provenance_payload):
            raise ValueError("Stage-A2 Stage-A provenance digest mismatch")
        if provenance.get("stage_a_line_ledger_contract_sha256") != expected_upstream_contract:
            raise ValueError("Stage-A2 upstream Stage-A contract mismatch")
        if recall40_a2 and not round2_a2 and (
            provenance.get("stage_a_input_manifest_sha256")
            != "d4672273f9c2eab6e13e09b9219200d4008e33d75efc63cc9e213beba28f1fd9"
            or provenance.get("stage_a_generation_config_sha256")
            != "7c89e227eb81f372125be51660d7d2adec406d3250c2f4df4e7c3a923e7c6ede"
            or manifest.get("selected_parent_ids_sha256")
            != FROZEN_RECALL40_SELECTED_PARENT_IDS_SHA256
            or provenance.get("stage_a_selection_base_rows_sha256")
            != FROZEN_RECALL40_STAGE_A_SELECTION_BASE_ROWS_SHA256
        ):
            raise ValueError("Stage-A2 recall40 upstream run differs")
        by_batch_id = provenance.get("by_batch_id")
        if not isinstance(by_batch_id, Mapping):
            raise ValueError("Stage-A2 provenance lacks Stage-A batch locators")
        stage_a_identity_ledger = manifest.get(
            "stage_a_decision_identity_ledger"
        )
        if (
            not isinstance(stage_a_identity_ledger, list)
            or (not recall40_a2 and len(stage_a_identity_ledger)
                != FROZEN_STAGE_A1_DECISION_IDENTITY_COUNT)
            or any(
                not isinstance(row, Mapping)
                or set(row) != {
                    "parent_document_id", "source_line_id", "stage_a_role",
                    "stage_a_batch_id", "stage_a_slot",
                }
                for row in stage_a_identity_ledger
            )
            or manifest.get("stage_a_decision_identity_ledger_sha256")
            != sha256_json(stage_a_identity_ledger)
            or (not recall40_a2 and sha256_json(stage_a_identity_ledger)
                != FROZEN_STAGE_A1_DECISION_IDENTITY_LEDGER_SHA256)
            or (not recall40_a2 and sha256_json([
                row.get("source_line_id") for row in stage_a_identity_ledger
            ]) != FROZEN_STAGE_A1_ORDERED_DECISION_SOURCE_LINE_IDS_SHA256)
        ):
            raise ValueError("Stage-A2 frozen Stage-A decision ledger differs")
        stage_a_identity_by_line = {
            str(row["source_line_id"]): dict(row)
            for row in stage_a_identity_ledger
        }
        if len(stage_a_identity_by_line) != len(stage_a_identity_ledger):
            raise ValueError("Stage-A2 Stage-A decision identities duplicate")
        exceptions: list[dict[str, Any]] = []
        reason_counts: Counter[str] = Counter()
        parent_batch_indices: dict[str, list[int]] = {}
        for batch in all_batches:
            if batch.get("stage_a_provenance_sha256") != recorded_provenance_hash:
                raise ValueError("Stage-A2 batch provenance digest mismatch")
            relations = canonical_line_adjudication_relations(batch)
            for line in relations["catalog"]:
                expected_stage_a = stage_a_identity_by_line.get(
                    str(line["source_line_id"])
                )
                if line["model_decision_required"]:
                    observed_stage_a = {
                        "parent_document_id": batch["parent_document_id"],
                        "source_line_id": line["source_line_id"],
                        "stage_a_role": line["stage_a_role"],
                        "stage_a_batch_id": line["stage_a_batch_id"],
                        "stage_a_slot": line["stage_a_slot"],
                    }
                    if not json_value_equal_typed(
                        observed_stage_a, expected_stage_a
                    ):
                        raise ValueError(
                            "Stage-A2 catalog differs from frozen Stage-A decision"
                        )
                elif expected_stage_a is not None:
                    raise ValueError(
                        "Stage-A2 catalog hides a frozen Stage-A decision"
                    )
            parent_id = batch.get("parent_document_id")
            batch_index = batch.get("batch_index_within_parent")
            if (
                not isinstance(parent_id, str)
                or not parent_id
                or not isinstance(batch_index, int)
                or isinstance(batch_index, bool)
                or batch_index < 0
            ):
                raise ValueError("Stage-A2 parent-batch identity is invalid")
            parent_batch_indices.setdefault(parent_id, []).append(batch_index)
            for target in batch.get("targets", []):
                if not isinstance(target, Mapping):
                    raise ValueError("Stage-A2 target is not an object")
                stage_a = target.get("stage_a_decision")
                if not isinstance(stage_a, Mapping):
                    raise ValueError("Stage-A2 target lacks Stage-A proposal")
                upstream_batch_id = str(stage_a.get("stage_a_batch_id"))
                if not json_value_equal_typed(
                    target.get("stage_a_provenance_locator"),
                    by_batch_id.get(upstream_batch_id),
                ):
                    raise ValueError("Stage-A2 target Stage-A locator mismatch")
                exception = {
                    key: value for key, value in target.items()
                    if key not in {"slot", "stage_a_provenance_locator"}
                }
                exceptions.append(exception)
                reason_counts[str(target.get("exception_reason"))] += 1
        for parent_id, indices in parent_batch_indices.items():
            if sorted(indices) != list(range(len(indices))):
                raise ValueError(
                    f"Stage-A2 parent batch-index closure differs: {parent_id}"
                )
        source_line_ids = [
            str(row.get("source_line_id")) for row in exceptions
        ]
        exception_ids = [str(row.get("exception_id")) for row in exceptions]
        exception_identity_ledger = (
            line_adjudication_exception_identity_ledger(all_batches)
        )
        if (
            (not recall40_a2 and len(all_batches) != FROZEN_STAGE_A2_BATCH_COUNT)
            or (not recall40_a2 and len(exceptions) != FROZEN_STAGE_A2_EXCEPTION_COUNT)
            or len(source_line_ids) != len(set(source_line_ids))
            or len(exception_ids) != len(set(exception_ids))
            or (not recall40_a2 and dict(sorted(reason_counts.items()))
                != FROZEN_STAGE_A2_EXCEPTION_REASON_COUNTS)
            or (not recall40_a2 and sha256_json(source_line_ids)
                != FROZEN_STAGE_A2_ORDERED_SOURCE_LINE_IDS_SHA256)
            or (not recall40_a2 and sha256_json(exception_identity_ledger)
                != FROZEN_STAGE_A2_EXCEPTION_IDENTITY_LEDGER_SHA256)
        ):
            raise ValueError("Stage-A2 frozen exception coverage differs")
        fixed_exception_fields = {
            "stage_a_exception_count": len(exceptions),
            "stage_a_exception_ledger_sha256": sha256_json(exceptions),
            "stage_a_exception_source_line_ids_sha256": sha256_json(
                source_line_ids
            ),
            "stage_a_exception_reason_counts": dict(sorted(reason_counts.items())),
            "stage_a_exception_identity_ledger": exception_identity_ledger,
            "stage_a_exception_identity_ledger_sha256": sha256_json(
                exception_identity_ledger
            ),
            "expected_stage_a2_batch_count": len(all_batches),
        }
        for field, expected in fixed_exception_fields.items():
            if not json_value_equal_typed(manifest.get(field), expected):
                raise ValueError(f"Stage-A2 manifest {field} mismatch")
        expected_exception_ledger = {
            "count": len(exceptions),
            "ordered_exception_ids_sha256": sha256_json(exception_ids),
            "ordered_source_line_ids_sha256": sha256_json(source_line_ids),
            "reason_counts": dict(sorted(reason_counts.items())),
        }
        if not json_value_equal_typed(
            manifest.get("exception_ledger"), expected_exception_ledger
        ):
            raise ValueError("Stage-A2 manifest exception ledger mismatch")
        if recall40_a2:
            scope_spec = (
                RECALL40_ROUND2_STAGE_A2_SCOPE_SPEC
                if round2_a2
                else RECALL40_STAGE_A2_SCOPE_SPEC
            )
            scope_decision_count = scope_spec[
                "stage_a_decision_count"
                if round2_a2 else "stage_a_decision_identity_count"
            ]
            scope_batch_count = (
                len(all_batches)
                if round2_a2 else scope_spec["stage_a2_batch_count"]
            )
            scope_exception_count = scope_spec[
                "exception_count" if round2_a2 else "stage_a2_exception_count"
            ]
            scope_reason_counts = scope_spec[
                "exception_reason_counts"
                if round2_a2 else "stage_a2_exception_reason_counts"
            ]
            scope_exception_ledger_sha256 = scope_spec[
                "batched_exception_ledger_sha256"
                if round2_a2 else "stage_a2_exception_ledger_sha256"
            ]
            scope_source_ids_sha256 = scope_spec[
                "ordered_exception_source_line_ids_sha256"
                if round2_a2
                else "stage_a2_ordered_exception_source_line_ids_sha256"
            ]
            if (
                len(stage_a_identity_ledger)
                != scope_decision_count
                or sha256_json(stage_a_identity_ledger)
                != scope_spec["stage_a_decision_identity_ledger_sha256"]
                or len(all_batches) != scope_batch_count
                or len(exceptions) != scope_exception_count
                or dict(sorted(reason_counts.items()))
                != scope_reason_counts
                or sha256_json(exceptions)
                != scope_exception_ledger_sha256
                or sha256_json(source_line_ids)
                != scope_source_ids_sha256
                or (
                    not round2_a2
                    and sha256_json(exception_identity_ledger)
                    != scope_spec["stage_a2_exception_identity_ledger_sha256"]
                )
            ):
                raise ValueError("Stage-A2 recall40 frozen exception closure differs")
            expected_selection_columns = [
                "selection_group", "pilot_control_kind", "parent_document_id",
                "candidate_cycle_id", "candidate_icpsr", "state", "office",
                "year", "party", "segment_count", "physical_line_count",
                "model_decision_line_count", "line_decision_batch_count",
                "record_extraction_batch_count_maximum",
                "parent_line_inventory_sha256", "context_scope",
                "stage_a_anchor_line_count", "stage_a_unclear_line_count",
                "stage_a_role_counts_json", "record_extraction_batch_count",
            ]
            if parsed_selection_columns != expected_selection_columns:
                raise ValueError("Stage-A2 recall40 selection schema differs")
            expected_private_a2 = {
                "enabled": True,
                "source_stage_a_input_manifest_sha256": (
                    "d4672273f9c2eab6e13e09b9219200d4008e33d75efc63cc9e213beba28f1fd9"
                ),
                "source_stage_a_generation_config_sha256": (
                    "7c89e227eb81f372125be51660d7d2adec406d3250c2f4df4e7c3a923e7c6ede"
                ),
                "selected_parent_count": 40,
                "exception_count": len(exceptions),
                "exception_reason_counts": dict(sorted(reason_counts.items())),
                "builder_performed_inference": False,
                "hive_submission_authorized": False,
                "stage_b_authorized": False,
            }
            if round2_a2:
                expected_private_round2 = {
                    "enabled": True,
                    "source_stage_a_input_manifest_sha256": scope_spec[
                        "stage_a_input_manifest_sha256"
                    ],
                    "source_stage_a_generation_config_sha256": scope_spec[
                        "stage_a_generation_config_sha256"
                    ],
                    "selected_parent_count": 40,
                    "stage_a_decision_count": scope_decision_count,
                    "exception_count": len(exceptions),
                    "exception_reason_counts": dict(sorted(reason_counts.items())),
                    "builder_performed_inference": False,
                    "hive_submission_authorized": False,
                    "stage_b_authorized": False,
                    "full_expansion_or_release_authorized": False,
                }
                if not json_value_equal_typed(
                    manifest.get("private_recall40_round2_stage_a2"),
                    expected_private_round2,
                ):
                    raise ValueError(
                        "Stage-A2 recall40 round-two private scope differs"
                    )
            elif not json_value_equal_typed(
                manifest.get("private_recall40_stage_a2"), expected_private_a2
            ):
                raise ValueError("Stage-A2 recall40 private scope differs")
            expected_inventory = (
                {
                    "complete_line_overlap_duplicate_occurrences": 285,
                    "context_anchor_boundary_lines": 0,
                    "empty_physical_lines": 0,
                    "model_decision_line_characters": 118258,
                    "model_decision_lines": 4019,
                    "nonempty_physical_lines": 4228,
                    "outside_scope_lines": 209,
                    "parent_documents": 40,
                    "physical_lines": 4228,
                    "reconstructed_long_lines": 0,
                    "source_segments": 72,
                    "unique_model_decision_line_ids": 4019,
                    "unique_source_line_ids": 4228,
                }
                if round2_a2 else FROZEN_RECALL40_INVENTORY
            )
            expected_batch_counts_value = (
                {
                    "line_decision_batches": 270,
                    "record_extraction_batches_maximum": 518,
                }
                if round2_a2 else {
                    "line_decision_batches": 85,
                    "record_extraction_batches_maximum": 158,
                }
            )
            if (
                not json_value_equal_typed(
                    manifest.get("inventory_census"), expected_inventory
                )
                or not json_value_equal_typed(
                    manifest.get("expected_batch_counts"),
                    expected_batch_counts_value,
                )
                or manifest.get("chunk_size") != 60
                or [entry.get("records") for entry in entries] != [
                    min(60, len(all_batches) - offset)
                    for offset in range(0, len(all_batches), 60)
                ]
            ):
                raise ValueError("Stage-A2 recall40 source/batch census differs")
            selected_ids = manifest.get("selected_parent_ids")
            expected_selected_ids_sha256 = (
                "25962ac169c3f88bccbf84ef4767e62dd15f82267017ac126efa50e3b43ec191"
                if round2_a2 else FROZEN_RECALL40_SELECTED_PARENT_IDS_SHA256
            )
            if (
                not isinstance(selected_ids, list)
                or len(selected_ids) != 40
                or len(set(selected_ids)) != 40
                or sha256_json(selected_ids)
                != expected_selected_ids_sha256
                or manifest.get("selected_parent_ids_sha256")
                != expected_selected_ids_sha256
            ):
                raise ValueError("Stage-A2 recall40 parent scope differs")
            a2_selection_rows = parsed_selection_rows
            base_rows = [
                {key: row[key] for key in expected_selection_columns[:16]}
                for row in a2_selection_rows
            ]
            if (
                [row.get("parent_document_id") for row in a2_selection_rows]
                != selected_ids
                or any(row.get("selection_group") != "private_recall_pilot40"
                       for row in a2_selection_rows)
                or not set(parent_batch_indices).issubset(set(selected_ids))
                or sha256_json(base_rows)
                != (
                    scope_spec["a2_selection_base_rows_sha256"]
                    if round2_a2
                    else FROZEN_RECALL40_STAGE_A_SELECTION_BASE_ROWS_SHA256
                )
            ):
                raise ValueError("Stage-A2 recall40 selection differs")
            roles_by_parent: dict[str, Counter[str]] = {}
            for identity_row in stage_a_identity_ledger:
                parent_id = str(identity_row["parent_document_id"])
                roles_by_parent.setdefault(parent_id, Counter())[
                    str(identity_row["stage_a_role"])
                ] += 1
            for row in a2_selection_rows:
                roles = roles_by_parent.get(str(row["parent_document_id"]), Counter())
                anchors = roles["record_anchor"] + roles["compound_record_anchor"]
                expected_derived = {
                    "stage_a_anchor_line_count": str(anchors),
                    "stage_a_unclear_line_count": str(roles["unclear"]),
                    "stage_a_role_counts_json": json.dumps(
                        dict(sorted(roles.items())), sort_keys=True,
                        separators=(",", ":"), ensure_ascii=False,
                    ),
                    "record_extraction_batch_count": str((anchors + 7) // 8),
                }
                if any(row.get(field) != expected
                       for field, expected in expected_derived.items()):
                    raise ValueError("Stage-A2 recall40 derived selection differs")
    if (
        stage == RECORD_EXTRACTION_STAGE
        and manifest.get("mode") == "record_extraction_preflight_from_stage_a3"
    ):
        if (
            type(manifest.get("stage_b_upstream_pass_count")) is not int
            or manifest["stage_b_upstream_pass_count"] != 3
        ):
            raise ValueError("Stage-B requires authenticated A1+A2+A3 provenance")
        upstream_a = manifest.get("upstream_stage_a")
        upstream_a2 = manifest.get("upstream_stage_a2")
        merge = manifest.get("two_pass_merge")
        if not isinstance(upstream_a, Mapping) or set(upstream_a) != {
            "contract_sha256", "input_manifest_sha256",
            "generation_config_sha256", "ordered_output_batch_ids_sha256",
            "decision_ledger_sha256", "provenance_sha256",
        }:
            raise ValueError("Stage-B upstream Stage-A provenance is invalid")
        if upstream_a.get("contract_sha256") != (
            FROZEN_STAGE_A_V2_CONTRACT_SHA256
        ) or any(
            not _is_sha256(upstream_a.get(field))
            for field in set(upstream_a) - {"contract_sha256"}
        ):
            raise ValueError("Stage-B upstream Stage-A hashes are invalid")
        expected_a2_fields = {
            "input_manifest_sha256", "generation_config_sha256",
            "max_model_len", "max_output_tokens", "input_files",
            "output_files", "output_batch_ids_sha256",
            "validated_output_batches", "run_metadata_files",
            "typed_restart_replay_complete", "decision_ledger_sha256",
        }
        if not isinstance(upstream_a2, Mapping) or set(upstream_a2) != (
            expected_a2_fields
        ):
            raise ValueError("Stage-B upstream Stage-A2 provenance is invalid")
        if (
            upstream_a2.get("max_model_len") != FROZEN_MAX_MODEL_LEN
            or upstream_a2.get("max_output_tokens")
            != DEFAULT_MAX_OUTPUT_TOKENS[LINE_ADJUDICATION_STAGE]
            or upstream_a2.get("typed_restart_replay_complete") is not True
            or not isinstance(upstream_a2.get("validated_output_batches"), int)
            or isinstance(upstream_a2.get("validated_output_batches"), bool)
            or upstream_a2["validated_output_batches"] < 1
            or any(
                not _is_sha256(upstream_a2.get(field))
                for field in (
                    "input_manifest_sha256", "generation_config_sha256",
                    "output_batch_ids_sha256", "decision_ledger_sha256",
                )
            )
        ):
            raise ValueError("Stage-B upstream Stage-A2 fields are invalid")
        for field, exact_item_fields in (
            ("input_files", {"file", "sha256"}),
            ("output_files", {"file", "sha256", "bytes", "records"}),
            ("run_metadata_files", {"file", "sha256"}),
        ):
            items = upstream_a2.get(field)
            if (
                not isinstance(items, list)
                or not items
                or any(
                    not isinstance(item, Mapping)
                    or set(item) != exact_item_fields
                    or not isinstance(item.get("file"), str)
                    or not _is_sha256(item.get("sha256"))
                    for item in items
                )
            ):
                raise ValueError(f"Stage-B upstream Stage-A2 {field} is invalid")
        if not isinstance(merge, Mapping) or set(merge) != {
            "validation", "final_decisions_sha256",
            "per_line_provenance_sha256",
            "ownership_by_continuation_sha256",
        }:
            raise ValueError("Stage-B two-pass merge provenance is invalid")
        three_pass = True
        effective_merge = manifest.get("three_pass_merge")
        if three_pass:
            upstream_a3 = manifest.get("upstream_stage_a3")
            if not isinstance(upstream_a3, Mapping) or set(upstream_a3) != {
                "a3_spec_sha256", "task_sha256", "html_sha256", "results_sha256",
                "import_artifact_sha256", "stage_a2_evaluation_report_sha256",
                "stage_a3_decisions_sha256", "stage_a3_final_decisions_sha256",
                "stage_a3_per_line_provenance_sha256",
                "stage_a3_ownership_by_continuation_sha256",
                "stage_a3_unchanged_gate_replay_sha256",
                "stage_b_used_line_provenance_sha256",
                "stage_b_ordered_anchor_source_line_ids_sha256",
            } or any(not _is_sha256(value) for value in upstream_a3.values()) or any(
                upstream_a3.get(field) != expected
                for field, expected in FROZEN_STAGE_A3_AUTHENTICATION.items()
            ):
                raise ValueError("Stage-B upstream Stage-A3 provenance is invalid")
            if not isinstance(effective_merge, Mapping) or set(effective_merge) != {
                "validation", "final_decisions_sha256",
                "per_line_provenance_sha256",
                "ownership_by_continuation_sha256", "unchanged_gate_replay",
            }:
                raise ValueError("Stage-B three-pass merge provenance is invalid")
            gate_replay = effective_merge.get("unchanged_gate_replay")
            if (
                not isinstance(gate_replay, Mapping)
                or gate_replay.get("gate_decision") != "GO"
                or not isinstance(gate_replay.get("checks"), list)
                or len(gate_replay["checks"]) != 11
                or any(
                    not isinstance(check, Mapping)
                    or check.get("passed") is not True
                    for check in gate_replay["checks"]
                )
            ):
                raise ValueError("Stage-B three-pass unchanged gates are invalid")
            if sha256_json(gate_replay) != FROZEN_STAGE_A3_AUTHENTICATION[
                "stage_a3_unchanged_gate_replay_sha256"
            ]:
                raise ValueError("Stage-B three-pass unchanged gates differ")
            if (
                upstream_a3["stage_a3_final_decisions_sha256"]
                != effective_merge.get("final_decisions_sha256")
                or upstream_a3["stage_a3_per_line_provenance_sha256"]
                != effective_merge.get("per_line_provenance_sha256")
                or upstream_a3["stage_a3_ownership_by_continuation_sha256"]
                != effective_merge.get("ownership_by_continuation_sha256")
            ):
                raise ValueError("Stage-B Stage-A3 merge hashes differ")
        merge_validation = effective_merge.get("validation")
        required_validation_fields = {
            "expected_exception_count", "adjudicated_exception_count",
            "unresolved_line_count", "withheld_line_count",
            "unowned_continuation_count",
            "duplicate_continuation_owner_count", "ownership_conflict_count",
            "coverage_complete", "validation_passed",
        }
        if three_pass:
            required_validation_fields |= {
                "stage_a3_expected_residual_count",
                "stage_a3_adjudicated_residual_count",
                "stage_a3_coverage_complete",
            }
        if not isinstance(merge_validation, Mapping) or set(merge_validation) != required_validation_fields:
            raise ValueError("Stage-B two-pass merge validation is invalid")
        expected_count = merge_validation.get("expected_exception_count")
        if (
            not isinstance(expected_count, int)
            or isinstance(expected_count, bool)
            or expected_count < 1
            or merge_validation.get("adjudicated_exception_count")
            != expected_count
            or any(
                merge_validation.get(field) != 0
                for field in (
                    "unresolved_line_count", "withheld_line_count",
                    "unowned_continuation_count",
                    "duplicate_continuation_owner_count",
                    "ownership_conflict_count",
                )
            )
            or merge_validation.get("coverage_complete") is not True
            or merge_validation.get("validation_passed") is not True
            or any(
                not _is_sha256(effective_merge.get(field))
                for field in (
                    "final_decisions_sha256",
                    "per_line_provenance_sha256",
                    "ownership_by_continuation_sha256",
                )
            )
        ):
            raise ValueError("Stage-B two-pass merge gate is invalid")
        if three_pass and (
            merge_validation.get("stage_a3_expected_residual_count") != 14
            or merge_validation.get("stage_a3_adjudicated_residual_count") != 14
            or merge_validation.get("stage_a3_coverage_complete") is not True
        ):
            raise ValueError("Stage-B Stage-A3 coverage gate is invalid")
        final_hashes: set[str] = set()
        provenance_hashes: set[str] = set()
        used_line_provenance: dict[str, dict[str, Any]] = {}
        ordered_anchor_source_line_ids: list[str] = []
        expected_batch_fields = {
            "batch_id", "batch_index_within_parent", "candidate_cycle_id",
            "candidate_icpsr", "document_id", "endorsement_line_context",
            "extraction_source_n_chars", "extraction_source_text_sha256",
            "final_line_decisions_sha256", "json_schema_catalog_sha256",
            "json_schema_sha256", "json_schema_variant_id",
            "line_batch_spec_sha256", "line_inventory_spec_sha256", "office",
            "padding_slots", "parent_document_id", "parent_line_inventory_sha256",
            "parent_source_text", "party", "raw_text_sha256", "route",
            "segment_descriptors", "source_urls", "stage", "state",
            "target_slot_count", "target_slots", "targets",
                "stage_b_three_pass_provenance_spec_sha256",
                "record_extraction_batch_schema_rule_sha256",
                "three_pass_line_provenance_sha256", "year",
        }
        expected_target_fields = {
            "anchor_line_decision_provenance", "complete_carrier_segment_ids",
            "deterministic_disposition", "inventory_unit_type",
            "line_adjudication_applied", "line_terminator", "line_text",
            "line_text_sha256", "model_decision_required", "owner_segment_id",
            "owner_segment_index", "parent_end_char", "parent_full_end_char",
            "parent_line_index", "parent_start_char",
            "record_block_line_decision_provenance", "record_block_lines", "segment_fragments",
            "slot", "source_line_id", "stage_a2_batch_id", "stage_a2_decision",
            "stage_a2_slot", "stage_a_batch_id", "stage_a_proposed_role",
            "stage_a_role", "stage_a_slot",
        }
        for batch in all_batches:
            if set(batch) != expected_batch_fields:
                raise ValueError("Stage-B batch field set differs")
            final_hash = batch.get("final_line_decisions_sha256")
            provenance_hash = batch.get("three_pass_line_provenance_sha256")
            if not _is_sha256(final_hash) or not _is_sha256(provenance_hash):
                raise ValueError("Stage-B batch lacks two-pass ledger hashes")
            final_hashes.add(str(final_hash))
            provenance_hashes.add(str(provenance_hash))
            targets = batch.get("targets")
            if not isinstance(targets, list):
                raise ValueError("Stage-B batch targets are invalid")
            for target in targets:
                if not isinstance(target, Mapping):
                    raise ValueError("Stage-B target is not an object")
                if set(target) != expected_target_fields:
                    raise ValueError("Stage-B target field set differs")
                source_line_id = str(target.get("source_line_id"))
                ordered_anchor_source_line_ids.append(source_line_id)
                anchor_provenance = target.get(
                    "anchor_line_decision_provenance"
                )
                block_provenance = target.get(
                    "record_block_line_decision_provenance"
                )
                if (
                    not isinstance(anchor_provenance, Mapping)
                    or not isinstance(block_provenance, list)
                    or not isinstance(target.get("line_adjudication_applied"), bool)
                ):
                    raise ValueError("Stage-B target lacks two-pass provenance")
                if (
                    anchor_provenance.get("source_line_id") != source_line_id
                    or anchor_provenance.get("final_role")
                    != target.get("stage_a_role")
                    or anchor_provenance.get("stage_a_proposal", {}).get("role")
                    != target.get("stage_a_proposed_role")
                    or anchor_provenance.get("stage_a_proposal", {}).get(
                        "stage_a_batch_id"
                    ) != target.get("stage_a_batch_id")
                    or anchor_provenance.get("stage_a_proposal", {}).get(
                        "stage_a_slot"
                    ) != target.get("stage_a_slot")
                ):
                    raise ValueError("Stage-B anchor provenance differs")
                for provenance_row in [anchor_provenance, *block_provenance]:
                    line_id = provenance_row.get("source_line_id")
                    if not isinstance(line_id, str) or not line_id:
                        raise ValueError("Stage-B inline provenance identity is invalid")
                    prior = used_line_provenance.setdefault(
                        line_id, dict(provenance_row)
                    )
                    if not json_value_equal_typed(prior, provenance_row):
                        raise ValueError("Stage-B inline provenance copies differ")
                block_lines = target.get("record_block_lines")
                if not isinstance(block_lines, list) or any(
                    not isinstance(row, Mapping) for row in block_lines
                ):
                    raise ValueError("Stage-B record block is invalid")
                if [
                    row.get("source_line_id") for row in block_provenance
                    if isinstance(row, Mapping)
                ] != [row.get("source_line_id") for row in block_lines]:
                    raise ValueError("Stage-B block provenance coverage differs")
                applied = target["line_adjudication_applied"]
                adjudication = anchor_provenance.get("stage_a2_adjudication")
                if applied:
                    if (
                        not isinstance(adjudication, Mapping)
                        or target.get("stage_a2_batch_id")
                        != adjudication.get("stage_a2_batch_id")
                        or target.get("stage_a2_slot")
                        != adjudication.get("stage_a2_slot")
                        or not json_value_equal_typed(
                            target.get("stage_a2_decision"),
                            adjudication.get("stage_a2_decision"),
                        )
                    ):
                        raise ValueError("Stage-B Stage-A2 provenance differs")
                elif any(
                    value is not None
                    for value in (
                        target.get("stage_a2_batch_id"),
                        target.get("stage_a2_slot"),
                        target.get("stage_a2_decision"),
                        adjudication,
                    )
                ):
                    raise ValueError("Stage-B nonadjudicated anchor has A2 data")
        if len(final_hashes) != 1 or len(provenance_hashes) != 1:
            raise ValueError("Stage-B batches disagree on two-pass ledger hashes")
        _validate_stage_b_anchor_coverage(ordered_anchor_source_line_ids)
        if sha256_json(dict(sorted(used_line_provenance.items()))) != (
            FROZEN_STAGE_A3_AUTHENTICATION[
                "stage_b_used_line_provenance_sha256"
            ]
        ):
            raise ValueError("Stage-B used-line provenance ledger differs")
        if (
            effective_merge.get("final_decisions_sha256") != next(iter(final_hashes))
            or effective_merge.get("per_line_provenance_sha256")
            != next(iter(provenance_hashes))
            or effective_merge.get("validation", {}).get("validation_passed") is not True
        ):
            raise ValueError("Stage-B manifest two-pass merge binding differs")
    return manifest, manifest_hash, paths


def _validate_stage_b_anchor_coverage(
    ordered_anchor_source_line_ids: list[str],
) -> None:
    if (
        len(ordered_anchor_source_line_ids) != 1194
        or len(set(ordered_anchor_source_line_ids)) != 1194
        or sha256_json(ordered_anchor_source_line_ids)
        != FROZEN_STAGE_A3_AUTHENTICATION[
            "stage_b_ordered_anchor_source_line_ids_sha256"
        ]
    ):
        raise ValueError("Stage-B frozen authenticated-anchor coverage differs")


def generation_config_payload(
    args: argparse.Namespace,
    contract: Mapping[str, Any],
    *,
    input_manifest_sha256: str,
) -> dict[str, Any]:
    stage = str(args.stage)
    expected_manifest_sha256 = getattr(
        args, "expected_input_manifest_sha256", input_manifest_sha256
    )
    if expected_manifest_sha256 != input_manifest_sha256:
        raise ValueError("expected input manifest SHA-256 differs from actual")
    payload = {
        "generation_config_version": GENERATION_CONFIG_VERSION,
        "stage": stage,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
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
        "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
        "line_batch_spec_sha256": contract["line_batch_spec_sha256"],
        "input_manifest_sha256": input_manifest_sha256,
        "expected_input_manifest_sha256": expected_manifest_sha256,
        "seed": int(args.seed),
        "max_model_len": int(args.max_model_len),
        "max_output_tokens": int(args.max_output_tokens),
        "temperature": 0.0,
        "thinking_enabled": False,
        "chat_template_add_generation_prompt": True,
        "context_safety_margin_tokens": CONTEXT_SAFETY_MARGIN_TOKENS,
        "prompt_token_count_spec_sha256": PROMPT_TOKEN_COUNT_SPEC_SHA256,
        "structured_decoding": {
            "mode": "json_schema",
            "backend": STRUCTURED_DECODING_BACKEND,
            "disable_any_whitespace": (
                STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
            ),
        },
        "json_string_control_canonicalizer_id": (
            JSON_STRING_CONTROL_CANONICALIZER_ID
        ),
        "json_string_control_canonicalizer_spec_sha256": (
            JSON_STRING_CONTROL_CANONICALIZER_SPEC_SHA256
        ),
        "raw_structural_whitespace_validator_id": (
            RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID
        ),
        "raw_structural_whitespace_validator_spec_sha256": (
            RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256
        ),
    }
    if stage == LINE_ADJUDICATION_STAGE:
        payload.update(_adjudication_contract_bindings(contract))
        if _is_round3_stage_a2_contract(contract):
            payload.update(_round3_stage_a2_upstream_identities({
                field: getattr(args, field, None)
                for field in (
                    "upstream_stage_a_input_manifest_sha256",
                    "upstream_stage_a_generation_config_sha256",
                    "upstream_stage_a_output_batch_ids_sha256",
                    "upstream_stage_a_decision_ledger_sha256",
                    "stage_a_provenance_sha256",
                )
            }))
    if (
        stage == LINE_DECISION_STAGE
        and contract.get(
            "recall40_round3_stage_a_scope_spec_sha256"
        ) is not None
    ):
        payload.update({
            "ordered_prompt_token_counts_sha256": (
                RECALL40_ROUND3_STAGE_A_SCOPE_SPEC[
                    "ordered_prompt_token_counts_sha256"
                ]
            ),
            "tokenizer_transformers_version": (
                RECALL40_ROUND3_STAGE_A_SCOPE_SPEC[
                    "tokenizer_transformers_version"
                ]
            ),
        })
    if stage == LINE_ADJUDICATION_STAGE and _is_round3_stage_a2_contract(
        contract
    ):
        payload.update({
            "ordered_prompt_token_counts_sha256": (
                RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
                    "ordered_prompt_token_counts_sha256"
                ]
            ),
            "tokenizer_transformers_version": (
                RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC[
                    "tokenizer_transformers_version"
                ]
            ),
        })
    if stage == RECORD_EXTRACTION_STAGE and _is_round3_stage_b_contract(
        contract
    ):
        payload.update({
            "ordered_prompt_token_counts_sha256": (
                RECALL40_ROUND3_STAGE_B_SCOPE_SPEC[
                    "ordered_prompt_token_counts_sha256"
                ]
            ),
            "tokenizer_transformers_version": (
                RECALL40_ROUND3_STAGE_B_SCOPE_SPEC[
                    "tokenizer_transformers_version"
                ]
            ),
        })
    if stage == RECORD_EXTRACTION_STAGE:
        if _is_two_pass_stage_b_contract(contract):
            payload["stage_b_two_pass_provenance_spec_sha256"] = (
                contract["stage_b_two_pass_provenance_spec_sha256"]
            )
        else:
            payload["stage_b_three_pass_provenance_spec_sha256"] = (
                contract["stage_b_three_pass_provenance_spec_sha256"]
            )
        payload["record_extraction_batch_schema_rule_sha256"] = (
            contract["record_extraction_batch_schema_rule_sha256"]
        )
    return payload


def generation_config_sha256(
    args: argparse.Namespace,
    contract: Mapping[str, Any],
    *,
    input_manifest_sha256: str,
) -> str:
    return sha256_json(
        generation_config_payload(
            args, contract, input_manifest_sha256=input_manifest_sha256
        )
    )


def validate_expected_generation_config_sha256(observed: str, expected: str) -> None:
    if not isinstance(expected, str) or expected != observed:
        raise ValueError("generation configuration differs from external expectation")


def _frozen_round3_prompt_token_counts_sha256(
    chunk_name: str,
    *,
    scope_spec: Mapping[str, Any] = RECALL40_ROUND3_STAGE_A_SCOPE_SPEC,
) -> str:
    digests = scope_spec[
        "chunk_ordered_prompt_token_counts_sha256"
    ]
    names = [
        f"chunk_{index:05d}.jsonl.gz" for index in range(len(digests))
    ]
    if chunk_name not in names:
        raise ValueError("round-three prompt-token chunk is out of scope")
    digest = digests[names.index(chunk_name)]
    if not _is_sha256(digest):
        raise ValueError("round-three prompt-token digest is invalid")
    return str(digest)


def _expected_prompt_token_counts_sha256(
    manifest: Mapping[str, Any], input_path: Path,
) -> str | None:
    mode = manifest.get("mode")
    if mode == "line_decision_recall40_round3":
        scope_spec = RECALL40_ROUND3_STAGE_A_SCOPE_SPEC
    elif mode == "line_adjudication_recall40_round3":
        scope_spec = RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC
    elif mode == "record_extraction_recall40_round3":
        scope_spec = RECALL40_ROUND3_STAGE_B_SCOPE_SPEC
    else:
        return None
    entries = manifest.get("files", {}).get("chunks")
    if not isinstance(entries, list):
        raise ValueError("round-three prompt-token chunk inventory is invalid")
    names = [entry.get("file") for entry in entries]
    expected_names = [
        f"chunk_{index:05d}.jsonl.gz"
        for index in range(len(
            scope_spec[
                "chunk_ordered_prompt_token_counts_sha256"
            ]
        ))
    ]
    if names != expected_names:
        raise ValueError("round-three prompt-token digest inventory differs")
    return _frozen_round3_prompt_token_counts_sha256(
        input_path.name, scope_spec=scope_spec
    )


def reject_stale_output_temporaries(output_dir: Path) -> None:
    if sorted(output_dir.glob("*.tmp-*")):
        raise ValueError("stale temporary output artifact is present; preserve and audit before retry")


def llm_engine_kwargs(args: argparse.Namespace) -> dict[str, Any]:
    return {
        "model": str(args.model_path),
        "tokenizer": str(args.model_path),
        "max_model_len": int(args.max_model_len),
        "gpu_memory_utilization": float(args.gpu_memory_utilization),
        "seed": int(args.seed),
        "trust_remote_code": False,
        "language_model_only": True,
        "structured_outputs_config": {
            "backend": STRUCTURED_DECODING_BACKEND,
            "disable_any_whitespace": (
                STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
            ),
        },
    }


def _stage_derivation(
    batch: Mapping[str, Any], model_output: Any,
    contract: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    stage = str(batch["stage"])
    if stage == LINE_DECISION_STAGE:
        return validate_line_decision_output(batch, model_output), None
    if stage == LINE_ADJUDICATION_STAGE:
        return validate_line_adjudication_output(batch, model_output), None
    if stage == RECORD_EXTRACTION_STAGE:
        validation = validate_record_extraction_output(batch, model_output)
        alignment = (
            align_record_extraction_output(
                batch, model_output,
                line_ledger_contract_sha256=(
                    contract["line_ledger_contract_sha256"]
                    if contract is not None else None
                ),
            )
            if validation["schema_valid"] else None
        )
        return validation, alignment
    raise ValueError(f"unknown line-ledger stage: {stage}")


def _expected_parsed_row_fields(
    canonicalization: Mapping[str, Any],
    structural: Mapping[str, Any],
    *,
    stage: str,
    stage_b_two_pass: bool = False,
    round3_stage_a2: bool = False,
) -> set[str]:
    fields = {
        "document_id", "batch_id", "stage", "route",
        "parent_document_id", "candidate_cycle_id", "input_batch_sha256",
        "input_manifest_sha256", "model_id", "model_revision",
        "runtime_lock_sha256", "line_ledger_contract_sha256",
        "prompt_sha256", "json_schema_catalog_sha256",
        "json_schema_variant_id", "json_schema_sha256", "target_slot_count",
        "line_inventory_spec_sha256", "line_batch_spec_sha256",
        "generation_config_sha256", "temperature", "thinking_enabled",
        "structured_decoding_backend",
        "structured_decoding_disable_any_whitespace", "max_model_len",
        "max_output_tokens", "finish_reason", "stop_reason",
        "prompt_token_count_spec_sha256",
        "output_tokens", "prompt_tokens_used", "context_margin_tokens",
        "input_truncated", "user_message_sha256", "raw_output",
        "model_output", "model_output_sha256", "parse_status",
        "parse_error", "stage_validation", "stage_alignment",
        "technical_pass",
        *canonicalization.keys(), *structural.keys(),
    }
    if stage == LINE_ADJUDICATION_STAGE:
        fields.update({
            "line_adjudication_batch_schema_rule_sha256",
            "line_adjudication_spec_sha256",
            "upstream_stage_a_contract_sha256",
            "stage_a_provenance_sha256",
        })
        if round3_stage_a2:
            fields.update({
                "upstream_stage_a_input_manifest_sha256",
                "upstream_stage_a_generation_config_sha256",
                "upstream_stage_a_output_batch_ids_sha256",
                "upstream_stage_a_decision_ledger_sha256",
            })
    if stage == RECORD_EXTRACTION_STAGE:
        fields.add(
            "stage_b_two_pass_provenance_spec_sha256"
            if stage_b_two_pass else "stage_b_three_pass_provenance_spec_sha256"
        )
        fields.add("record_extraction_batch_schema_rule_sha256")
    return fields


def line_ledger_chunk_is_complete(
    input_path: Path,
    output_path: Path,
    *,
    stage: str,
    input_manifest_sha256: str,
    expected_generation_config_sha256: str,
    expected_max_model_len: int,
    expected_max_output_tokens: int,
    expected_prompt_token_counts_sha256: str | None = None,
    contract_override: Mapping[str, Any] | None = None,
) -> bool:
    """Replay every content and identity field before reusing an atomic chunk."""
    if not output_path.exists():
        return False
    try:
        inputs = read_gzip_jsonl(input_path)
        outputs = read_gzip_jsonl(output_path)
        contract = (
            dict(contract_override)
            if contract_override is not None else get_line_ledger_contract()
        )
    except (OSError, EOFError, json.JSONDecodeError, RuntimeError, ValueError):
        return False
    if len(inputs) != len(outputs):
        return False
    round3_stage_a = (
        stage == LINE_DECISION_STAGE
        and contract.get("recall40_round3_stage_a_scope_spec_sha256")
        is not None
    )
    round3_stage_a2 = (
        stage == LINE_ADJUDICATION_STAGE
        and _is_round3_stage_a2_contract(contract)
    )
    round3_stage_b = (
        stage == RECORD_EXTRACTION_STAGE
        and _is_round3_stage_b_contract(contract)
    )
    round3_prompt_ledger = (
        round3_stage_a or round3_stage_a2 or round3_stage_b
    )
    if round3_prompt_ledger and not _is_sha256(
        expected_prompt_token_counts_sha256
    ):
        return False
    if round3_prompt_ledger:
        scope_spec = (
            RECALL40_ROUND3_STAGE_B_SCOPE_SPEC
            if round3_stage_b
            else (
                RECALL40_ROUND3_STAGE_A2_SCOPE_SPEC
                if round3_stage_a2 else RECALL40_ROUND3_STAGE_A_SCOPE_SPEC
            )
        )
        try:
            frozen_prompt_digest = _frozen_round3_prompt_token_counts_sha256(
                input_path.name, scope_spec=scope_spec
            )
        except ValueError:
            return False
        if expected_prompt_token_counts_sha256 != frozen_prompt_digest:
            return False
    if (
        expected_prompt_token_counts_sha256 is not None
        and not _is_sha256(expected_prompt_token_counts_sha256)
    ):
        return False
    input_ids = [row.get("batch_id") for row in inputs]
    output_ids = [row.get("batch_id") for row in outputs]
    if input_ids != output_ids or len(input_ids) != len(set(input_ids)):
        return False
    for batch, row in zip(inputs, outputs):
        if batch.get("stage") != stage or row.get("stage") != stage:
            return False
        if not json_value_equal_typed(row.get("input_batch_sha256"), sha256_json(batch)):
            return False
        try:
            schema_binding = schema_binding_for_batch(batch)
        except (KeyError, TypeError, ValueError):
            return False
        identity = {
            "document_id": batch.get("document_id"),
            "batch_id": batch.get("batch_id"),
            "stage": stage,
            "route": batch.get("route"),
            "parent_document_id": batch.get("parent_document_id"),
            "candidate_cycle_id": batch.get("candidate_cycle_id"),
            "input_manifest_sha256": input_manifest_sha256,
            "model_id": MODEL_ID,
            "model_revision": MODEL_REVISION,
            "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
            "line_ledger_contract_sha256": contract[
                "line_ledger_contract_sha256"
            ],
            "prompt_sha256": contract["prompt_sha256"][stage],
            "json_schema_catalog_sha256": schema_binding[
                "json_schema_catalog_sha256"
            ],
            "json_schema_variant_id": schema_binding["json_schema_variant_id"],
            "json_schema_sha256": schema_binding["json_schema_sha256"],
            "target_slot_count": schema_binding["target_slot_count"],
            "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
            "line_batch_spec_sha256": contract["line_batch_spec_sha256"],
            "generation_config_sha256": expected_generation_config_sha256,
            "temperature": 0.0,
            "thinking_enabled": False,
            "structured_decoding_backend": STRUCTURED_DECODING_BACKEND,
            "structured_decoding_disable_any_whitespace": (
                STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
            ),
            "max_model_len": expected_max_model_len,
            "max_output_tokens": expected_max_output_tokens,
            "prompt_token_count_spec_sha256": PROMPT_TOKEN_COUNT_SPEC_SHA256,
            "input_truncated": False,
        }
        if stage == LINE_ADJUDICATION_STAGE:
            identity.update(_adjudication_contract_bindings(contract))
            identity["stage_a_provenance_sha256"] = batch.get(
                "stage_a_provenance_sha256"
            )
            if _is_round3_stage_a2_contract(contract):
                identity.update(_round3_stage_a2_upstream_identities(batch))
        if stage == RECORD_EXTRACTION_STAGE:
            if _is_two_pass_stage_b_contract(contract):
                identity["stage_b_two_pass_provenance_spec_sha256"] = (
                    contract["stage_b_two_pass_provenance_spec_sha256"]
                )
            else:
                identity["stage_b_three_pass_provenance_spec_sha256"] = (
                    contract["stage_b_three_pass_provenance_spec_sha256"]
                )
            identity["record_extraction_batch_schema_rule_sha256"] = (
                contract["record_extraction_batch_schema_rule_sha256"]
            )
        if any(
            not json_value_equal_typed(row.get(field), expected)
            for field, expected in identity.items()
        ):
            return False
        try:
            user_message = build_stage_user_message(batch)
        except (KeyError, TypeError, ValueError):
            return False
        if row.get("user_message_sha256") != sha256_text(user_message):
            return False
        if (
            row.get("finish_reason") != "stop"
            or row.get("parse_status") != "parsed"
            or row.get("technical_pass") is not True
            or row.get("parse_error") is not None
        ):
            return False
        for field in ("output_tokens", "prompt_tokens_used", "context_margin_tokens"):
            if (
                not isinstance(row.get(field), int)
                or isinstance(row.get(field), bool)
                or row[field] < 0
            ):
                return False
        if not (0 < row["output_tokens"] <= expected_max_output_tokens):
            return False
        if row["context_margin_tokens"] != (
            expected_max_model_len
            - row["prompt_tokens_used"]
            - expected_max_output_tokens
        ):
            return False
        raw_output = row.get("raw_output")
        if not isinstance(raw_output, str):
            return False
        canonical, canonicalization = canonicalize_json_string_controls(raw_output)
        structural = validate_raw_structural_whitespace(raw_output)
        if not structural["raw_structural_whitespace_validation_passed"]:
            return False
        if any(
            not json_value_equal_typed(row.get(field), value)
            for field, value in {**canonicalization, **structural}.items()
        ):
            return False
        if set(row) != _expected_parsed_row_fields(
            canonicalization, structural, stage=stage,
            stage_b_two_pass=_is_two_pass_stage_b_contract(contract),
            round3_stage_a2=_is_round3_stage_a2_contract(contract),
        ):
            return False
        if (
            canonicalization["json_control_unrepaired_after_escape_count"] != 0
            or canonicalization["json_control_outside_string_invalid_count"] != 0
        ):
            return False
        try:
            parsed = strict_json_loads(canonical)
            validation, alignment = _stage_derivation(batch, parsed, contract)
        except (KeyError, TypeError, ValueError, StrictJSONError):
            return False
        if not json_value_equal_typed(row.get("model_output"), parsed):
            return False
        if row.get("model_output_sha256") != sha256_json(parsed):
            return False
        if not json_value_equal_typed(row.get("stage_validation"), validation):
            return False
        if not json_value_equal_typed(row.get("stage_alignment"), alignment):
            return False
        if validation.get("schema_valid") is not True or validation.get(
            "coverage_complete"
        ) is not True:
            return False
        if not _stage_derivation_is_technical_pass(stage, validation, alignment):
            return False
    if (
        expected_prompt_token_counts_sha256 is not None
        and sha256_json([row["prompt_tokens_used"] for row in outputs])
        != expected_prompt_token_counts_sha256
    ):
        return False
    return True


def load_validated_stage_run(
    input_export_dir: Path,
    output_dir: Path,
    *,
    stage: str,
    contract_override: Mapping[str, Any] | None = None,
    allow_unfrozen_review: bool = False,
) -> tuple[
    dict[str, Any],
    list[dict[str, Any]],
    list[dict[str, Any]],
    dict[str, Any],
]:
    """Authenticate one complete current-contract stage run for replay.

    This is the shared builder/evaluator boundary.  It binds the immutable
    input manifest and chunks, canonical generation payload, exact shard
    accounting, target-count schema variants, every atomic output row, and (for
    Stage A2) the selected frozen Stage-A provenance digest.
    """
    if stage not in STAGES:
        raise ValueError(f"unknown line-ledger stage: {stage}")
    if contract_override is not None:
        contract = dict(contract_override)
    else:
        candidate_manifest = json.loads(
            (input_export_dir / "manifest.json").read_text(encoding="utf-8")
        )
        if candidate_manifest.get("mode") == "line_decision_recall40_round3":
            contract = (
                computed_recall40_round3_stage_a_contract()
                if allow_unfrozen_review
                else get_frozen_recall40_round3_stage_a_contract()
            )
        elif candidate_manifest.get("mode") == (
            "line_adjudication_recall40_round3"
        ):
            contract = (
                computed_recall40_round3_stage_a2_contract()
                if allow_unfrozen_review
                else get_frozen_recall40_round3_stage_a2_contract()
            )
        elif candidate_manifest.get("mode") == "line_adjudication_recall40_round2":
            contract = get_frozen_recall40_round2_stage_a2_contract()
        elif candidate_manifest.get("mode") == "line_adjudication_recall_pilot40":
            contract = computed_recall40_stage_a2_contract(
                get_line_ledger_contract()["line_ledger_contract_sha256"]
            )
        elif candidate_manifest.get("mode") == "record_extraction_recall_pilot40":
            contract = computed_recall40_stage_b_contract()
        elif candidate_manifest.get("mode") == "record_extraction_recall40_round2":
            contract = get_frozen_recall40_round2_stage_b_contract()
        elif candidate_manifest.get("mode") == "record_extraction_recall40_round3":
            contract = (
                computed_recall40_round3_stage_b_contract()
                if allow_unfrozen_review
                else get_frozen_recall40_round3_stage_b_contract()
            )
        elif file_sha256(input_export_dir / "manifest.json") == (
            FROZEN_HISTORICAL_STAGE_B_SUFFIX_E_MANIFEST_SHA256
        ):
            contract = dict(candidate_manifest["line_ledger_contract"])
        elif candidate_manifest.get("line_ledger_contract_sha256") == (
            "d8072b6269438b1742c47f1a25ff271edab32add331a18db9898d439f9b25bb0"
        ):
            contract = get_frozen_stage_a2_v4_contract()
        else:
            contract = get_line_ledger_contract()
    manifest, manifest_sha256, input_paths = validate_input_manifest(
        input_export_dir / "manifest.json",
        input_export_dir / "chunks",
        stage=stage,
        contract=contract,
        allow_unfrozen_review=allow_unfrozen_review,
    )
    input_rows = [
        row for path in input_paths for row in read_gzip_jsonl(path)
    ]

    unsharded = output_dir / "run_metadata.json"
    sharded = sorted(output_dir.glob("run_metadata_shard_*.json"))
    if unsharded.is_file() and sharded:
        raise ValueError("run output mixes sharded and unsharded metadata")
    metadata_paths = [unsharded] if unsharded.is_file() else sharded
    if not metadata_paths:
        raise FileNotFoundError(f"run metadata is absent: {output_dir}")
    metadata_rows: list[dict[str, Any]] = []
    for path in metadata_paths:
        value = strict_json_loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, dict):
            raise TypeError(f"run metadata is not an object: {path}")
        metadata_rows.append(value)

    fixed_metadata: dict[str, Any] = {
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
        "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
        "line_batch_spec_sha256": contract["line_batch_spec_sha256"],
        "model_id": contract["model_id"],
        "model_revision": contract["model_revision"],
        "runtime_lock_sha256": contract["runtime_lock_sha256"],
        "max_model_len": FROZEN_MAX_MODEL_LEN,
        "max_output_tokens": DEFAULT_MAX_OUTPUT_TOKENS[stage],
        "structured_decoding_backend": STRUCTURED_DECODING_BACKEND,
        "structured_decoding_disable_any_whitespace": (
            STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
        ),
        "prompt_token_count_spec_sha256": PROMPT_TOKEN_COUNT_SPEC_SHA256,
    }
    if stage == LINE_ADJUDICATION_STAGE:
        fixed_metadata.update(_adjudication_contract_bindings(contract))
        if _is_round3_stage_a2_contract(contract):
            fixed_metadata.update(_round3_stage_a2_upstream_identities({
                **manifest,
                "stage_a_provenance_sha256": manifest.get(
                    "stage_a_provenance", {}
                ).get("stage_a_provenance_sha256"),
            }))
    if stage == RECORD_EXTRACTION_STAGE:
        stage_b_spec_field = (
            "stage_b_two_pass_provenance_spec_sha256"
            if _is_two_pass_stage_b_contract(contract)
            else "stage_b_three_pass_provenance_spec_sha256"
        )
        fixed_metadata[stage_b_spec_field] = (
            contract["stage_b_two_pass_provenance_spec_sha256"]
            if stage_b_spec_field.startswith("stage_b_two")
            else contract["stage_b_three_pass_provenance_spec_sha256"]
        )
        fixed_metadata["record_extraction_batch_schema_rule_sha256"] = (
            contract["record_extraction_batch_schema_rule_sha256"]
        )

    expected_metadata_fields = {
        "run_metadata_version", "accounting_scope", "recovery_attestation",
        "stage", "input_manifest_sha256", "line_ledger_contract_sha256",
        "prompt_sha256", "json_schema_catalog_sha256",
        "json_schema_variant_sha256", "line_inventory_spec_sha256",
        "line_batch_spec_sha256", "generation_config_sha256",
        "generation_config", "model_id", "model_revision",
        "runtime_lock_sha256", "max_model_len", "max_output_tokens",
        "prompt_token_count_spec_sha256", "structured_decoding_backend",
        "structured_decoding_disable_any_whitespace", "shard_index",
        "shard_count", "selected_batches", "selected_schema_variant_counts",
        "selected_schema_variant_hashes_used",
        "selected_schema_variant_ids_sha256",
        "selected_schema_variant_hashes_sha256",
        "processed_outputs_this_run",
        "processed_schema_variant_counts_this_run",
        "technical_pass_outputs_this_run", "parse_status_counts_this_run",
        "finish_reason_counts_this_run", "output_tokens_total_this_run",
        "output_tokens_max_this_run", "prompt_tokens_max_this_run",
        "minimum_context_margin_tokens_this_run", "started_at",
        "completed_at", "host", "slurm_job_id", "slurm_array_task_id",
    }
    if stage == LINE_ADJUDICATION_STAGE:
        expected_metadata_fields.update({
            "line_adjudication_batch_schema_rule_sha256",
            "line_adjudication_spec_sha256",
            "upstream_stage_a_contract_sha256",
            "selected_stage_a_provenance_sha256",
        })
        if _is_round3_stage_a2_contract(contract):
            expected_metadata_fields.update({
                "upstream_stage_a_input_manifest_sha256",
                "upstream_stage_a_generation_config_sha256",
                "upstream_stage_a_output_batch_ids_sha256",
                "upstream_stage_a_decision_ledger_sha256",
                "stage_a_provenance_sha256",
            })
    if stage == RECORD_EXTRACTION_STAGE:
        expected_metadata_fields.update({
            stage_b_spec_field,
            "record_extraction_batch_schema_rule_sha256",
        })

    generation_hashes: set[str] = set()
    max_model_lengths: set[int] = set()
    max_output_tokens: set[int] = set()
    for path, metadata in zip(metadata_paths, metadata_rows):
        if set(metadata) != expected_metadata_fields:
            raise ValueError(f"{path.name} metadata field set differs")
        for field, expected in fixed_metadata.items():
            if not json_value_equal_typed(metadata.get(field), expected):
                raise ValueError(f"{path.name} metadata {field} differs")
        payload = metadata.get("generation_config")
        if not isinstance(payload, Mapping):
            raise ValueError(f"{path.name} lacks canonical generation payload")
        generation_hash = metadata.get("generation_config_sha256")
        if not isinstance(generation_hash, str):
            raise ValueError(f"{path.name} generation hash is invalid")
        if sha256_json(payload) != generation_hash:
            raise ValueError(f"{path.name} generation payload hash differs")
        for field, target in (
            ("max_model_len", max_model_lengths),
            ("max_output_tokens", max_output_tokens),
        ):
            value = metadata.get(field)
            if not isinstance(value, int) or isinstance(value, bool):
                raise ValueError(f"{path.name} {field} is not an integer")
            target.add(value)
        expected_args: dict[str, Any] = {
            "stage": stage,
            "seed": FROZEN_GENERATION_SEED,
            "max_model_len": metadata["max_model_len"],
            "max_output_tokens": metadata["max_output_tokens"],
            "upstream_stage_a_contract_sha256": (
                _adjudication_contract_bindings(contract)[
                    "upstream_stage_a_contract_sha256"
                ] if stage == LINE_ADJUDICATION_STAGE
                else FROZEN_STAGE_A_V2_CONTRACT_SHA256
            ),
        }
        if _is_round3_stage_a2_contract(contract):
            expected_args.update(_round3_stage_a2_upstream_identities(metadata))
        expected_payload = generation_config_payload(
            argparse.Namespace(**expected_args),
            contract,
            input_manifest_sha256=manifest_sha256,
        )
        if not json_value_equal_typed(payload, expected_payload):
            raise ValueError(f"{path.name} generation payload differs")
        generation_hashes.add(generation_hash)
        if metadata.get("run_metadata_version") != RUN_METADATA_VERSION:
            raise ValueError(f"{path.name} metadata version differs")
        if metadata.get("accounting_scope") != (
            "outputs_inferred_during_this_process_attempt"
        ):
            raise ValueError(f"{path.name} accounting scope differs")
        if not isinstance(metadata.get("recovery_attestation"), bool):
            raise ValueError(f"{path.name} recovery attestation is not boolean")
        for field in (
            "selected_batches", "processed_outputs_this_run",
            "technical_pass_outputs_this_run", "output_tokens_total_this_run",
            "output_tokens_max_this_run", "prompt_tokens_max_this_run",
            "minimum_context_margin_tokens_this_run",
        ):
            value = metadata.get(field)
            if (
                not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
            ):
                raise ValueError(f"{path.name} {field} is invalid")
        for field in (
            "selected_schema_variant_counts",
            "selected_schema_variant_hashes_used",
            "processed_schema_variant_counts_this_run",
            "parse_status_counts_this_run",
            "finish_reason_counts_this_run",
        ):
            if not isinstance(metadata.get(field), Mapping):
                raise ValueError(f"{path.name} {field} is not a mapping")
        for field in ("started_at", "completed_at", "host"):
            if not isinstance(metadata.get(field), str) or not metadata[field]:
                raise ValueError(f"{path.name} {field} is invalid")
        try:
            started_at = datetime.fromisoformat(metadata["started_at"])
            completed_at = datetime.fromisoformat(metadata["completed_at"])
        except ValueError as exc:
            raise ValueError(f"{path.name} timestamps are invalid") from exc
        if (
            started_at.tzinfo is None
            or completed_at.tzinfo is None
            or completed_at < started_at
        ):
            raise ValueError(f"{path.name} timestamp ordering is invalid")
        for field in ("slurm_job_id", "slurm_array_task_id"):
            if metadata.get(field) is not None and not isinstance(
                metadata[field], str
            ):
                raise ValueError(f"{path.name} {field} is invalid")
    if not (
        len(generation_hashes)
        == len(max_model_lengths)
        == len(max_output_tokens)
        == 1
    ):
        raise ValueError("run metadata disagrees on generation identity")
    if sum(
        int(metadata.get("selected_batches", -1))
        for metadata in metadata_rows
    ) != len(input_rows):
        raise ValueError("run metadata selected-batch accounting is incomplete")

    if len(metadata_rows) == 1 and metadata_rows[0].get("shard_count") is None:
        if metadata_rows[0].get("shard_index") is not None:
            raise ValueError("unsharded metadata has a shard index")
        if metadata_paths[0].name != "run_metadata.json":
            raise ValueError("unsharded metadata filename differs")
    else:
        shard_counts = {row.get("shard_count") for row in metadata_rows}
        if len(shard_counts) != 1:
            raise ValueError("run metadata shard counts disagree")
        shard_count = next(iter(shard_counts))
        if (
            not isinstance(shard_count, int)
            or isinstance(shard_count, bool)
            or shard_count < 1
            or shard_count > len(input_paths)
        ):
            raise ValueError("run metadata shard count is invalid")
        if len(metadata_rows) != shard_count:
            raise ValueError("run metadata shard closure differs")
        shard_indices: list[int] = []
        for path, metadata in zip(metadata_paths, metadata_rows):
            shard_index = metadata.get("shard_index")
            if (
                not isinstance(shard_index, int)
                or isinstance(shard_index, bool)
                or shard_index < 0
                or shard_index >= shard_count
            ):
                raise ValueError("run metadata shard index is invalid")
            if path.name != f"run_metadata_shard_{shard_index:05d}.json":
                raise ValueError("run metadata shard filename differs")
            shard_indices.append(shard_index)
        if sorted(shard_indices) != list(range(shard_count)):
            raise ValueError("run metadata shard closure differs")

    for path, metadata in zip(metadata_paths, metadata_rows):
        if metadata.get("shard_count") is None:
            selected_input_paths = input_paths
        else:
            shard_index = int(metadata["shard_index"])
            shard_count = int(metadata["shard_count"])
            selected_input_paths = [
                input_path
                for index, input_path in enumerate(input_paths)
                if index % shard_count == shard_index
            ]
        selected_batches = [
            row
            for input_path in selected_input_paths
            for row in read_gzip_jsonl(input_path)
        ]
        if not selected_batches:
            raise ValueError(f"{path.name} selects no input batches")
        if metadata.get("selected_batches") != len(selected_batches):
            raise ValueError(f"{path.name} selected batch count differs")
        expected_variant_inventory = schema_variant_inventory(
            selected_batches
        )
        for metadata_field, inventory_field in {
            "selected_schema_variant_counts": "schema_variant_counts",
            "selected_schema_variant_hashes_used": (
                "schema_variant_hashes_used"
            ),
            "selected_schema_variant_ids_sha256": (
                "ordered_schema_variant_ids_sha256"
            ),
            "selected_schema_variant_hashes_sha256": (
                "ordered_schema_variant_hashes_sha256"
            ),
        }.items():
            if not json_value_equal_typed(
                metadata.get(metadata_field),
                expected_variant_inventory[inventory_field],
            ):
                raise ValueError(
                    f"{path.name} selected schema variants differ"
                )
        if stage == LINE_ADJUDICATION_STAGE:
            expected_provenance = sorted({
                str(batch.get("stage_a_provenance_sha256"))
                for batch in selected_batches
            })
            if not json_value_equal_typed(
                metadata.get("selected_stage_a_provenance_sha256"),
                expected_provenance,
            ):
                raise ValueError(
                    f"{path.name} selected Stage-A provenance differs"
                )

    expected_output_paths = [
        output_dir / path.name.replace(".jsonl.gz", ".out.jsonl.gz")
        for path in input_paths
    ]
    actual_output_names = sorted(
        path.name for path in output_dir.glob("*.out.jsonl.gz")
    )
    if actual_output_names != sorted(
        path.name for path in expected_output_paths
    ):
        raise ValueError("input/output chunk mapping differs")
    allowed = {
        *(path.name for path in expected_output_paths),
        *(path.name for path in metadata_paths),
    }
    actual = {
        str(path.relative_to(output_dir))
        for path in output_dir.rglob("*")
        if path.is_file()
    }
    if actual != allowed:
        raise ValueError("run output inventory differs")
    if any(path.is_dir() for path in output_dir.rglob("*")):
        raise ValueError("run output contains nested directories")

    generation_hash = next(iter(generation_hashes))
    max_model_len = next(iter(max_model_lengths))
    max_tokens = next(iter(max_output_tokens))
    output_rows: list[dict[str, Any]] = []
    output_rows_by_input_name: dict[str, list[dict[str, Any]]] = {}
    output_files: list[dict[str, Any]] = []
    for input_path, output_path in zip(input_paths, expected_output_paths):
        if not line_ledger_chunk_is_complete(
            input_path,
            output_path,
            stage=stage,
            input_manifest_sha256=manifest_sha256,
            expected_generation_config_sha256=generation_hash,
            expected_max_model_len=max_model_len,
            expected_max_output_tokens=max_tokens,
            expected_prompt_token_counts_sha256=(
                _expected_prompt_token_counts_sha256(manifest, input_path)
            ),
            contract_override=contract,
        ):
            raise ValueError(
                f"run chunk fails typed restart replay: {output_path.name}"
            )
        rows = read_gzip_jsonl(output_path)
        output_rows_by_input_name[input_path.name] = rows
        output_rows.extend(rows)
        output_files.append({
            "file": output_path.name,
            "sha256": file_sha256(output_path),
            "bytes": output_path.stat().st_size,
            "records": len(rows),
        })
    if [row.get("batch_id") for row in output_rows] != [
        row.get("batch_id") for row in input_rows
    ]:
        raise ValueError("run output batch order differs")

    count_fields = (
        "processed_schema_variant_counts_this_run",
        "parse_status_counts_this_run",
        "finish_reason_counts_this_run",
    )
    for path, metadata in zip(metadata_paths, metadata_rows):
        if metadata.get("shard_count") is None:
            selected_input_paths = input_paths
        else:
            shard_index = int(metadata["shard_index"])
            shard_count = int(metadata["shard_count"])
            selected_input_paths = [
                input_path
                for index, input_path in enumerate(input_paths)
                if index % shard_count == shard_index
            ]
        selected_rows = [
            row
            for input_path in selected_input_paths
            for row in output_rows_by_input_name[input_path.name]
        ]
        full_ledgers: dict[str, Counter[str]] = {
            "processed_schema_variant_counts_this_run": Counter(
                str(row["json_schema_variant_id"]) for row in selected_rows
            ),
            "parse_status_counts_this_run": Counter(
                str(row["parse_status"]) for row in selected_rows
            ),
            "finish_reason_counts_this_run": Counter(
                str(row["finish_reason"]) for row in selected_rows
            ),
        }
        processed = metadata["processed_outputs_this_run"]
        recovery = metadata["recovery_attestation"]
        for field in count_fields:
            observed = metadata[field]
            if any(
                not isinstance(key, str)
                or not isinstance(value, int)
                or isinstance(value, bool)
                or value < 0
                or value > full_ledgers[field].get(key, 0)
                for key, value in observed.items()
            ):
                raise ValueError(f"{path.name} {field} exceeds row ledger")
            if sum(observed.values()) != processed:
                raise ValueError(f"{path.name} {field} does not reconcile")
        technical = metadata["technical_pass_outputs_this_run"]
        full_technical = sum(
            row.get("technical_pass") is True for row in selected_rows
        )
        if technical != processed or technical > full_technical:
            raise ValueError(f"{path.name} technical-pass count differs")
        numeric_attempt = {
            "output_tokens_total_this_run": sum(
                int(row["output_tokens"]) for row in selected_rows
            ),
            "output_tokens_max_this_run": max(
                (int(row["output_tokens"]) for row in selected_rows),
                default=0,
            ),
            "prompt_tokens_max_this_run": max(
                (int(row["prompt_tokens_used"]) for row in selected_rows),
                default=0,
            ),
        }
        if any(
            metadata[field] > maximum
            for field, maximum in numeric_attempt.items()
        ):
            raise ValueError(f"{path.name} token accounting exceeds row ledger")
        full_margins = [
            int(row["context_margin_tokens"]) for row in selected_rows
        ]
        minimum_margin = metadata["minimum_context_margin_tokens_this_run"]
        if recovery:
            zero_fields = (
                "processed_outputs_this_run",
                "technical_pass_outputs_this_run",
                "output_tokens_total_this_run",
                "output_tokens_max_this_run",
                "prompt_tokens_max_this_run",
                "minimum_context_margin_tokens_this_run",
            )
            if any(metadata[field] != 0 for field in zero_fields) or any(
                metadata[field] != {} for field in count_fields
            ):
                raise ValueError(f"{path.name} recovery counters are nonzero")
        else:
            if processed != len(selected_rows):
                raise ValueError(
                    f"{path.name} non-recovery attempt is not complete"
                )
            if (
                metadata["output_tokens_total_this_run"] < processed
                or metadata["output_tokens_max_this_run"] < 1
                or metadata["prompt_tokens_max_this_run"] < 1
                or metadata["minimum_context_margin_tokens_this_run"]
                < CONTEXT_SAFETY_MARGIN_TOKENS
            ):
                raise ValueError(f"{path.name} attempt token accounting is invalid")
            if full_margins and not (
                min(full_margins) <= minimum_margin <= max(full_margins)
            ):
                raise ValueError(
                    f"{path.name} minimum context margin differs"
                )
            exact_full_attempt = {
                **{
                    field: dict(sorted(ledger.items()))
                    for field, ledger in full_ledgers.items()
                },
                "technical_pass_outputs_this_run": full_technical,
                **numeric_attempt,
                "minimum_context_margin_tokens_this_run": min(
                    full_margins, default=0
                ),
            }
            if any(
                not json_value_equal_typed(metadata.get(field), expected)
                for field, expected in exact_full_attempt.items()
            ):
                raise ValueError(
                    f"{path.name} full-attempt accounting differs"
                )
    return manifest, input_rows, output_rows, {
        "input_manifest_sha256": manifest_sha256,
        "generation_config_sha256": generation_hash,
        "max_model_len": max_model_len,
        "max_output_tokens": max_tokens,
        "input_files": [
            {"file": path.name, "sha256": file_sha256(path)}
            for path in input_paths
        ],
        "output_files": output_files,
        "output_batch_ids_sha256": sha256_json([
            str(row["batch_id"]) for row in output_rows
        ]),
        "validated_output_batches": len(output_rows),
        "run_metadata_files": [
            {"file": path.name, "sha256": file_sha256(path)}
            for path in metadata_paths
        ],
        "typed_restart_replay_complete": True,
    }


def _single_prompt_token_ids(encoded: Any) -> list[int]:
    if isinstance(encoded, Mapping):
        if "input_ids" not in encoded:
            raise TypeError("chat-template mapping has no input_ids")
        encoded = encoded["input_ids"]
    if hasattr(encoded, "tolist"):
        encoded = encoded.tolist()
    if isinstance(encoded, tuple):
        encoded = list(encoded)
    if (
        isinstance(encoded, list)
        and len(encoded) == 1
        and isinstance(encoded[0], (list, tuple))
    ):
        encoded = list(encoded[0])
    if not isinstance(encoded, list) or not encoded:
        raise TypeError("chat-template input_ids are not one nonempty sequence")
    if any(
        isinstance(value, bool) or not isinstance(value, Integral)
        for value in encoded
    ):
        raise TypeError("chat-template input_ids contain nested or noninteger values")
    return [int(value) for value in encoded]


def _prompt_token_count(tokenizer: Any, conversation: list[dict[str, str]]) -> int:
    try:
        encoded = tokenizer.apply_chat_template(
            conversation,
            tokenize=True,
            add_generation_prompt=True,
            enable_thinking=False,
        )
        return len(_single_prompt_token_ids(encoded))
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        raise RuntimeError(
            "the pinned tokenizer could not apply the exact chat template; "
            "context-budget attestation fails closed"
        ) from error


def _stage_derivation_is_technical_pass(
    stage: str,
    validation: Mapping[str, Any],
    alignment: Mapping[str, Any] | None,
) -> bool:
    if validation.get("schema_valid") is not True or validation.get(
        "coverage_complete"
    ) is not True:
        return False
    if stage != RECORD_EXTRACTION_STAGE:
        return True
    if not isinstance(alignment, Mapping):
        return False
    aligned = alignment.get("aligned_records")
    withheld = alignment.get("withheld_records")
    aligned_count = alignment.get("aligned_record_count")
    withheld_count = alignment.get("withheld_record_count")
    if not isinstance(aligned, list) or not isinstance(withheld, list):
        return False
    if (
        type(aligned_count) is not int
        or type(withheld_count) is not int
        or aligned_count != len(aligned)
        or withheld_count != len(withheld)
    ):
        return False
    return (
        alignment.get("public_coverage_complete") is True
        and withheld_count == 0
        and not withheld
    )


def _derive_output_row(
    *,
    batch: Mapping[str, Any],
    raw_output: str,
    finish_reason: str,
    stop_reason: Any,
    output_tokens: int,
    prompt_tokens: int,
    args: argparse.Namespace,
    contract: Mapping[str, Any],
    input_manifest_sha256: str,
    generation_hash: str,
) -> dict[str, Any]:
    canonical, canonicalization = canonicalize_json_string_controls(raw_output)
    structural = validate_raw_structural_whitespace(raw_output)
    parse_status = "parsed"
    parse_error: str | None = None
    model_output: Any = None
    model_hash: str | None = None
    validation: dict[str, Any] | None = None
    alignment: dict[str, Any] | None = None
    if finish_reason != "stop":
        parse_status = "generation_incomplete"
    elif not structural["raw_structural_whitespace_validation_passed"]:
        parse_status = "raw_structural_whitespace_error"
    elif (
        canonicalization["json_control_unrepaired_after_escape_count"] != 0
        or canonicalization["json_control_outside_string_invalid_count"] != 0
    ):
        parse_status = "json_control_error"
    else:
        try:
            model_output = strict_json_loads(canonical)
            model_hash = sha256_json(model_output)
            validation, alignment = _stage_derivation(batch, model_output, contract)
            if not validation.get("schema_valid") or not validation.get(
                "coverage_complete"
            ):
                parse_status = "invalid_stage_output"
            elif not _stage_derivation_is_technical_pass(
                str(batch["stage"]), validation, alignment
            ):
                parse_status = "invalid_stage_alignment"
        except (StrictJSONError, KeyError, TypeError, ValueError) as error:
            parse_status = "strict_json_parse_error"
            parse_error = f"{type(error).__name__}: {error}"
    technical_pass = bool(
        parse_status == "parsed"
        and finish_reason == "stop"
        and validation is not None
        and validation.get("schema_valid") is True
        and validation.get("coverage_complete") is True
        and _stage_derivation_is_technical_pass(
            str(batch["stage"]), validation, alignment
        )
    )
    stage = str(batch["stage"])
    schema_binding = schema_binding_for_batch(batch)
    row = {
        "document_id": batch["document_id"],
        "batch_id": batch["batch_id"],
        "stage": stage,
        "route": batch["route"],
        "parent_document_id": batch["parent_document_id"],
        "candidate_cycle_id": batch["candidate_cycle_id"],
        "input_batch_sha256": sha256_json(batch),
        "input_manifest_sha256": input_manifest_sha256,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
        "line_ledger_contract_sha256": contract[
            "line_ledger_contract_sha256"
        ],
        "prompt_sha256": contract["prompt_sha256"][stage],
        "json_schema_catalog_sha256": schema_binding[
            "json_schema_catalog_sha256"
        ],
        "json_schema_variant_id": schema_binding["json_schema_variant_id"],
        "json_schema_sha256": schema_binding["json_schema_sha256"],
        "target_slot_count": schema_binding["target_slot_count"],
        "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
        "line_batch_spec_sha256": contract["line_batch_spec_sha256"],
        "generation_config_sha256": generation_hash,
        "temperature": 0.0,
        "thinking_enabled": False,
        "structured_decoding_backend": STRUCTURED_DECODING_BACKEND,
        "structured_decoding_disable_any_whitespace": (
            STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
        ),
        "max_model_len": int(args.max_model_len),
        "max_output_tokens": int(args.max_output_tokens),
        "prompt_token_count_spec_sha256": PROMPT_TOKEN_COUNT_SPEC_SHA256,
        "finish_reason": finish_reason,
        "stop_reason": stop_reason,
        "output_tokens": int(output_tokens),
        "prompt_tokens_used": int(prompt_tokens),
        "context_margin_tokens": (
            int(args.max_model_len) - prompt_tokens - int(args.max_output_tokens)
        ),
        "input_truncated": False,
        "user_message_sha256": sha256_text(build_stage_user_message(batch)),
        "raw_output": raw_output,
        **canonicalization,
        **structural,
        "model_output": model_output,
        "model_output_sha256": model_hash,
        "parse_status": parse_status,
        "parse_error": parse_error,
        "stage_validation": validation,
        "stage_alignment": alignment,
        "technical_pass": technical_pass,
    }
    if stage == LINE_ADJUDICATION_STAGE:
        row.update(_adjudication_contract_bindings(contract))
        row["stage_a_provenance_sha256"] = batch[
            "stage_a_provenance_sha256"
        ]
        if _is_round3_stage_a2_contract(contract):
            row.update(_round3_stage_a2_upstream_identities(batch))
    if stage == RECORD_EXTRACTION_STAGE:
        if _is_two_pass_stage_b_contract(contract):
            row["stage_b_two_pass_provenance_spec_sha256"] = (
                contract["stage_b_two_pass_provenance_spec_sha256"]
            )
        else:
            row["stage_b_three_pass_provenance_spec_sha256"] = (
                contract["stage_b_three_pass_provenance_spec_sha256"]
            )
        row["record_extraction_batch_schema_rule_sha256"] = (
            contract["record_extraction_batch_schema_rule_sha256"]
        )
    expected = _expected_parsed_row_fields(
        canonicalization, structural, stage=stage,
        stage_b_two_pass=_is_two_pass_stage_b_contract(contract),
        round3_stage_a2=_is_round3_stage_a2_contract(contract),
    )
    if set(row) != expected:
        raise AssertionError("line-ledger output row fields drifted")
    return row


def _metadata_name(shard_index: int | None) -> str:
    return (
        "run_metadata.json" if shard_index is None
        else f"run_metadata_shard_{shard_index:05d}.json"
    )


def _write_json_atomic(path: Path, value: Mapping[str, Any]) -> None:
    temporary = path.with_suffix(path.suffix + f".tmp-{os.getpid()}")
    temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
    os.replace(temporary, path)


def _run_metadata(
    *,
    args: argparse.Namespace,
    contract: Mapping[str, Any],
    generation_hash: str,
    input_manifest_sha256: str,
    selected: int,
    selected_variant_inventory: Mapping[str, Any],
    selected_stage_a_provenance_sha256: list[str],
    rows: list[Mapping[str, Any]],
    started: str,
    recovery: bool,
) -> dict[str, Any]:
    status_counts = Counter(str(row["parse_status"]) for row in rows)
    finish_counts = Counter(str(row["finish_reason"]) for row in rows)
    generation_config = generation_config_payload(
        args, contract, input_manifest_sha256=input_manifest_sha256
    )
    if sha256_json(generation_config) != generation_hash:
        raise AssertionError("generation configuration hash differs from payload")
    metadata = {
        "run_metadata_version": RUN_METADATA_VERSION,
        "accounting_scope": "outputs_inferred_during_this_process_attempt",
        "recovery_attestation": recovery,
        "stage": args.stage,
        "input_manifest_sha256": input_manifest_sha256,
        "line_ledger_contract_sha256": contract[
            "line_ledger_contract_sha256"
        ],
        "prompt_sha256": contract["prompt_sha256"][args.stage],
        "json_schema_catalog_sha256": contract[
            "json_schema_catalog_sha256"
        ][args.stage],
        "json_schema_variant_sha256": contract[
            "json_schema_variant_sha256"
        ][args.stage],
        "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
        "line_batch_spec_sha256": contract["line_batch_spec_sha256"],
        "generation_config_sha256": generation_hash,
        "generation_config": generation_config,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
        "max_model_len": int(args.max_model_len),
        "max_output_tokens": int(args.max_output_tokens),
        "prompt_token_count_spec_sha256": PROMPT_TOKEN_COUNT_SPEC_SHA256,
        "structured_decoding_backend": STRUCTURED_DECODING_BACKEND,
        "structured_decoding_disable_any_whitespace": (
            STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
        ),
        "shard_index": args.shard_index,
        "shard_count": args.shard_count,
        "selected_batches": selected,
        "selected_schema_variant_counts": selected_variant_inventory[
            "schema_variant_counts"
        ],
        "selected_schema_variant_hashes_used": selected_variant_inventory[
            "schema_variant_hashes_used"
        ],
        "selected_schema_variant_ids_sha256": selected_variant_inventory[
            "ordered_schema_variant_ids_sha256"
        ],
        "selected_schema_variant_hashes_sha256": selected_variant_inventory[
            "ordered_schema_variant_hashes_sha256"
        ],
        "processed_outputs_this_run": len(rows),
        "processed_schema_variant_counts_this_run": dict(sorted(Counter(
            str(row.get("json_schema_variant_id")) for row in rows
        ).items())),
        "technical_pass_outputs_this_run": sum(
            row.get("technical_pass") is True for row in rows
        ),
        "parse_status_counts_this_run": dict(sorted(status_counts.items())),
        "finish_reason_counts_this_run": dict(sorted(finish_counts.items())),
        "output_tokens_total_this_run": sum(
            int(row.get("output_tokens", 0)) for row in rows
        ),
        "output_tokens_max_this_run": max(
            (int(row.get("output_tokens", 0)) for row in rows), default=0
        ),
        "prompt_tokens_max_this_run": max(
            (int(row.get("prompt_tokens_used", 0)) for row in rows), default=0
        ),
        "minimum_context_margin_tokens_this_run": min(
            (int(row.get("context_margin_tokens", 0)) for row in rows), default=0
        ),
        "started_at": started,
        "completed_at": utc_now(),
        "host": socket.gethostname(),
        "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
        "slurm_array_task_id": os.environ.get("SLURM_ARRAY_TASK_ID"),
    }
    if args.stage == LINE_ADJUDICATION_STAGE:
        metadata.update(_adjudication_contract_bindings(contract))
        metadata["selected_stage_a_provenance_sha256"] = sorted({
            str(value) for value in selected_stage_a_provenance_sha256
        })
        if _is_round3_stage_a2_contract(contract):
            metadata.update(_round3_stage_a2_upstream_identities({
                field: getattr(args, field, None)
                for field in (
                    "upstream_stage_a_input_manifest_sha256",
                    "upstream_stage_a_generation_config_sha256",
                    "upstream_stage_a_output_batch_ids_sha256",
                    "upstream_stage_a_decision_ledger_sha256",
                    "stage_a_provenance_sha256",
                )
            }))
    if args.stage == RECORD_EXTRACTION_STAGE:
        if _is_two_pass_stage_b_contract(contract):
            metadata["stage_b_two_pass_provenance_spec_sha256"] = (
                contract["stage_b_two_pass_provenance_spec_sha256"]
            )
        else:
            metadata["stage_b_three_pass_provenance_spec_sha256"] = (
                contract["stage_b_three_pass_provenance_spec_sha256"]
            )
        metadata["record_extraction_batch_schema_rule_sha256"] = (
            contract["record_extraction_batch_schema_rule_sha256"]
        )
    return metadata


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chunks-dir", type=Path, required=True)
    parser.add_argument("--input-manifest", type=Path, required=True)
    parser.add_argument(
        "--expected-input-manifest-sha256", required=True
    )
    parser.add_argument("--expected-generation-config-sha256", required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--model-path", type=Path, required=True)
    parser.add_argument("--stage", choices=STAGES, required=True)
    parser.add_argument(
        "--max-model-len", type=int, default=FROZEN_MAX_MODEL_LEN
    )
    parser.add_argument("--max-output-tokens", type=int)
    parser.add_argument("--gpu-memory-utilization", type=float, default=0.90)
    parser.add_argument("--seed", type=int, default=FROZEN_GENERATION_SEED)
    parser.add_argument("--shard-index", type=int)
    parser.add_argument("--shard-count", type=int)
    args = parser.parse_args()
    if args.max_output_tokens is None:
        args.max_output_tokens = DEFAULT_MAX_OUTPUT_TOKENS[args.stage]
    if args.max_model_len != FROZEN_MAX_MODEL_LEN:
        raise ValueError(
            f"max-model-len must remain frozen at {FROZEN_MAX_MODEL_LEN}; "
            f"got {args.max_model_len}"
        )
    if args.max_output_tokens != DEFAULT_MAX_OUTPUT_TOKENS[args.stage]:
        raise ValueError(
            "max-output-tokens must remain frozen at "
            f"{DEFAULT_MAX_OUTPUT_TOKENS[args.stage]} for {args.stage}; "
            f"got {args.max_output_tokens}"
        )
    if args.seed != FROZEN_GENERATION_SEED:
        raise ValueError(
            f"seed must remain frozen at {FROZEN_GENERATION_SEED}; got {args.seed}"
        )
    if args.model_path.name.lower() != MODEL_REVISION.lower():
        raise ValueError(
            f"model snapshot must end in {MODEL_REVISION}; got {args.model_path}"
        )
    observed_manifest_sha256 = validate_expected_input_manifest_sha256(
        args.input_manifest, args.expected_input_manifest_sha256
    )
    candidate_manifest = json.loads(args.input_manifest.read_text(encoding="utf-8"))
    if candidate_manifest.get("mode") == "record_extraction_recall40_round3":
        contract = get_frozen_recall40_round3_stage_b_contract()
    else:
        contract = (
        get_frozen_recall40_round3_stage_a_contract()
        if candidate_manifest.get("mode") == "line_decision_recall40_round3"
        else (
        get_frozen_recall40_round3_stage_a2_contract()
        if candidate_manifest.get("mode") == "line_adjudication_recall40_round3"
        else (
        get_frozen_recall40_round2_stage_a_contract()
        if candidate_manifest.get("mode") == "line_decision_recall40_round2"
        else (
        get_frozen_recall40_round2_stage_a2_contract()
        if candidate_manifest.get("mode") == "line_adjudication_recall40_round2"
        else (
        computed_recall40_stage_a2_contract(
            get_line_ledger_contract()["line_ledger_contract_sha256"]
        ))
        if candidate_manifest.get("mode") == "line_adjudication_recall_pilot40"
        else (
            get_frozen_recall40_round2_stage_b_contract()
            if candidate_manifest.get("mode") == "record_extraction_recall40_round2"
            else (
            computed_recall40_stage_b_contract()
            if candidate_manifest.get("mode") == "record_extraction_recall_pilot40"
            else (
                dict(candidate_manifest["line_ledger_contract"])
                if file_sha256(args.input_manifest)
                == FROZEN_HISTORICAL_STAGE_B_SUFFIX_E_MANIFEST_SHA256
                else get_line_ledger_contract()
            )
        )))))
        )
    manifest, input_manifest_sha256, chunks = validate_input_manifest(
        args.input_manifest,
        args.chunks_dir,
        stage=args.stage,
        contract=contract,
    )
    if input_manifest_sha256 != args.expected_input_manifest_sha256:
        raise ValueError("parsed input manifest SHA-256 differs from expectation")
    if (
        args.stage == LINE_ADJUDICATION_STAGE
        and manifest.get("mode") in {
            "line_adjudication_recall_pilot40",
            "line_adjudication_recall40_round2",
            "line_adjudication_recall40_round3",
        }
    ):
        args.upstream_stage_a_contract_sha256 = (
            _adjudication_contract_bindings(contract)[
                "upstream_stage_a_contract_sha256"
            ]
        )
        if _is_round3_stage_a2_contract(contract):
            identities = _round3_stage_a2_upstream_identities({
                **manifest,
                "stage_a_provenance_sha256": manifest[
                    "stage_a_provenance"
                ]["stage_a_provenance_sha256"],
            })
            for field, value in identities.items():
                setattr(args, field, value)
    generation_hash = generation_config_sha256(
        args, contract, input_manifest_sha256=input_manifest_sha256
    )
    validate_expected_generation_config_sha256(
        generation_hash, args.expected_generation_config_sha256
    )
    if manifest.get("mode") in {
        "line_decision_recall_pilot40", "line_adjudication_recall_pilot40",
        "record_extraction_recall_pilot40", "line_decision_recall40_round2",
        "line_adjudication_recall40_round2", "line_decision_recall40_round3",
        "line_adjudication_recall40_round3", "record_extraction_recall40_round3",
    } and (
        args.shard_count != len(chunks) or args.shard_index is None
    ):
        raise ValueError("private recall40 requires one exact input chunk per shard")
    chunks = select_shard(chunks, args.shard_index, args.shard_count)
    if not chunks:
        raise ValueError("selected shard has no chunks")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    reject_stale_output_temporaries(args.output_dir)
    todo: list[tuple[Path, Path]] = []
    selected = 0
    selected_batches_all: list[dict[str, Any]] = []
    for input_path in chunks:
        rows = read_gzip_jsonl(input_path)
        selected += len(rows)
        selected_batches_all.extend(rows)
        if any(row.get("stage") != args.stage for row in rows):
            raise ValueError(f"{input_path}: mixed or incorrect stages")
        output_path = args.output_dir / input_path.name.replace(
            ".jsonl.gz", ".out.jsonl.gz"
        )
        if not line_ledger_chunk_is_complete(
            input_path,
            output_path,
            stage=args.stage,
            input_manifest_sha256=input_manifest_sha256,
            expected_generation_config_sha256=generation_hash,
            expected_max_model_len=args.max_model_len,
            expected_max_output_tokens=args.max_output_tokens,
            expected_prompt_token_counts_sha256=(
                _expected_prompt_token_counts_sha256(manifest, input_path)
            ),
            contract_override=contract,
        ):
            if output_path.exists():
                raise ValueError(
                    "existing output is incomplete or from another contract; "
                    f"preserved without overwrite: {output_path}"
                )
            todo.append((input_path, output_path))
    selected_variant_inventory = schema_variant_inventory(selected_batches_all)
    selected_stage_a_provenance_sha256 = sorted({
        str(batch.get("stage_a_provenance_sha256"))
        for batch in selected_batches_all
        if batch.get("stage_a_provenance_sha256") is not None
    })
    if args.stage == LINE_ADJUDICATION_STAGE and len(
        selected_stage_a_provenance_sha256
    ) != 1:
        raise ValueError(
            "Stage-A2 inputs must bind exactly one authenticated Stage-A provenance"
        )
    metadata_path = args.output_dir / _metadata_name(args.shard_index)
    if not todo:
        timestamp = utc_now()
        _write_json_atomic(
            metadata_path,
            _run_metadata(
                args=args,
                contract=contract,
                generation_hash=generation_hash,
                input_manifest_sha256=input_manifest_sha256,
                selected=selected,
                selected_variant_inventory=selected_variant_inventory,
                selected_stage_a_provenance_sha256=(
                    selected_stage_a_provenance_sha256
                ),
                rows=[],
                started=timestamp,
                recovery=True,
            ),
        )
        print(f"selected={selected} todo=0 recovery=true", flush=True)
        return 0

    from vllm import LLM, SamplingParams

    started = utc_now()
    llm = LLM(**llm_engine_kwargs(args))
    tokenizer = llm.get_tokenizer()
    sampling_by_variant: dict[str, Any] = {}
    inferred_rows: list[dict[str, Any]] = []
    for input_path, output_path in todo:
        batches = read_gzip_jsonl(input_path)
        ids = [batch.get("batch_id") for batch in batches]
        if len(ids) != len(set(ids)):
            raise ValueError(f"{input_path}: duplicate batch IDs")
        conversations: list[list[dict[str, str]]] = []
        prompt_tokens: list[int] = []
        sampling_params: list[Any] = []
        for batch in batches:
            user_message = build_stage_user_message(batch)
            conversation = [
                {
                    "role": "system",
                    "content": prompt_for_contract_stage(contract, args.stage),
                },
                {"role": "user", "content": user_message},
            ]
            tokens = _prompt_token_count(tokenizer, conversation)
            margin = args.max_model_len - tokens - args.max_output_tokens
            if margin < CONTEXT_SAFETY_MARGIN_TOKENS:
                raise ValueError(
                    f"{batch['batch_id']}: context margin {margin} is below "
                    f"{CONTEXT_SAFETY_MARGIN_TOKENS}; never truncate a ledger batch"
                )
            conversations.append(conversation)
            prompt_tokens.append(tokens)
            schema_binding = schema_binding_for_batch(batch)
            variant_id = str(schema_binding["json_schema_variant_id"])
            if variant_id not in sampling_by_variant:
                sampling_by_variant[variant_id] = SamplingParams(
                    temperature=0.0,
                    max_tokens=args.max_output_tokens,
                    seed=args.seed,
                    **structured_parameters(schema_binding["schema"]),
                )
            sampling_params.append(sampling_by_variant[variant_id])
        expected_prompt_digest = _expected_prompt_token_counts_sha256(
            manifest, input_path
        )
        if (
            expected_prompt_digest is not None
            and sha256_json(prompt_tokens) != expected_prompt_digest
        ):
            raise ValueError(
                f"{input_path.name}: prompt-token counts differ from frozen census"
            )
        print(f"{input_path.name}: batches={len(batches)}", flush=True)
        outputs = llm.chat(
            conversations,
            sampling_params,
            use_tqdm=True,
            chat_template_kwargs={"enable_thinking": False},
        )
        if len(outputs) != len(batches):
            raise RuntimeError("vLLM returned the wrong number of outputs")
        temporary = output_path.with_suffix(
            output_path.suffix + f".tmp-{os.getpid()}"
        )
        chunk_rows: list[dict[str, Any]] = []
        with gzip.open(temporary, "wt", encoding="utf-8") as handle:
            for batch, output, tokens in zip(batches, outputs, prompt_tokens):
                completion = output.outputs[0]
                row = _derive_output_row(
                    batch=batch,
                    raw_output=completion.text,
                    finish_reason=str(completion.finish_reason or ""),
                    stop_reason=completion.stop_reason,
                    output_tokens=len(completion.token_ids or []),
                    prompt_tokens=tokens,
                    args=args,
                    contract=contract,
                    input_manifest_sha256=input_manifest_sha256,
                    generation_hash=generation_hash,
                )
                handle.write(json.dumps(row, ensure_ascii=False) + "\n")
                chunk_rows.append(row)
        os.replace(temporary, output_path)
        inferred_rows.extend(chunk_rows)
    _write_json_atomic(
        metadata_path,
        _run_metadata(
            args=args,
            contract=contract,
            generation_hash=generation_hash,
            input_manifest_sha256=input_manifest_sha256,
            selected=selected,
            selected_variant_inventory=selected_variant_inventory,
            selected_stage_a_provenance_sha256=(
                selected_stage_a_provenance_sha256
            ),
            rows=inferred_rows,
            started=started,
            recovery=False,
        ),
    )
    failures = sum(row["technical_pass"] is not True for row in inferred_rows)
    print(
        f"processed={len(inferred_rows)} technical_failures={failures} "
        f"generation={generation_hash}",
        flush=True,
    )
    return 0 if failures == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
