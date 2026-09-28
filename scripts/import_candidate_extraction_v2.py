#!/usr/bin/env python3
"""Import frozen v2 Hive outputs into a conflict-safe private bundle."""
from __future__ import annotations

import argparse
import base64
import binascii
import copy
import csv
import gzip
import hashlib
import io
import json
import os
import re
import sys
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Iterator, Mapping, Sequence

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from src.candidate_extraction import (  # noqa: E402
    ALIGNMENT_METHOD_UNIQUE,
    ARRAY_ITEM_CAP_REACHED_STATUS,
    ENDORSEMENT_CONTEXT_CLASSIFIER_ID,
    ENDORSEMENT_CONTEXT_CLASSIFIER_SPEC_SHA256,
    ENDORSEMENT_CONTEXT_PROVENANCE_VERSION,
    ENDORSEMENT_CONTEXT_PROVENANCE_SPEC_SHA256,
    ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC_SHA256,
    EXACT_ALIGNMENT_ID,
    LIST_KEY_BY_VERSION,
    MODEL_ID,
    MODEL_ITEM_REPETITION_ERROR_STATUS,
    MODEL_ITEM_REPETITION_THRESHOLD,
    MODEL_REVISION,
    RUNTIME_LOCK_SHA256,
    align_extraction_exact_spans,
    endorsement_context_provenance_errors,
    get_extraction_contract,
    inference_display_escape,
    inference_display_provenance,
    pre_alignment_model_item_guard_report,
    parent_char_span,
    validate_extraction,
)
from src.candidate_extraction_provenance import (  # noqa: E402
    OFFSET_COORDINATE_SYSTEM,
    SOURCE_TEXT_TRANSFORM_RULE,
    SOURCE_TEXT_TRANSFORM_VERSION,
)
from src.candidate_endorsement_line_ledger import (  # noqa: E402
    FROZEN_LINE_LEDGER_CONTRACT_SHA256,
    FROZEN_STAGE_A1_DECISION_IDENTITY_COUNT,
    FROZEN_STAGE_A1_DECISION_IDENTITY_LEDGER_SHA256,
    FROZEN_STAGE_A1_ORDERED_DECISION_SOURCE_LINE_IDS_SHA256,
    FROZEN_STAGE_A2_EXCEPTION_IDENTITY_LEDGER_SHA256,
    FROZEN_STAGE_A2_ORDERED_SOURCE_LINE_IDS_SHA256,
    FROZEN_STAGE_A_V2_CONTRACT_SHA256,
    LINE_ADJUDICATION_BATCH_SCHEMA_RULE_SHA256,
    LINE_ADJUDICATION_SPEC_SHA256,
    LINE_ADJUDICATION_STAGE,
    LINE_BATCH_SPEC_SHA256,
    LINE_DECISION_STAGE,
    LINE_INVENTORY_SPEC_SHA256,
    RECORD_EXTRACTION_STAGE,
    STAGE_B_TWO_PASS_PROVENANCE_SPEC_SHA256,
    align_record_extraction_output,
    build_line_adjudication_batches,
    build_line_decision_batches,
    build_record_extraction_batches_from_adjudicated_merge,
    build_stage_user_message,
    get_frozen_stage_a_v2_contract,
    get_line_ledger_contract,
    expected_batch_counts,
    inventory_census,
    line_adjudication_exception_identity_ledger,
    line_adjudication_exception_ledger,
    line_adjudication_ledger,
    line_decision_ledger,
    merge_line_adjudication_ledger,
    reconstruct_parent_inventory,
    schema_binding_for_batch,
    sha256_json as line_ledger_sha256_json,
    stage_a_decision_identity_ledger,
    validate_line_decision_output,
    validate_line_adjudication_output,
    validate_record_extraction_output,
)
from scripts.build_candidate_endorsement_line_ledger import (  # noqa: E402
    CORRECTED_ISSUE_AUDIT_GATE,
    DEFAULT_CHUNK_SIZE as LINE_LEDGER_CHUNK_SIZE,
    FROZEN_SOURCE_PARENTS_BY_ROUTE,
    FROZEN_SOURCE_SCHEMA_VERSION,
    FROZEN_SOURCE_SEGMENTS_BY_ROUTE,
    FROZEN_STAGE_A2_EXCEPTION_COUNT,
    FROZEN_STAGE_A2_EXCEPTION_COUNTS,
    FROZEN_STAGE_A_SUFFIX_B_DECISION_LEDGER_SHA256,
    FROZEN_STAGE_A_SUFFIX_B_GENERATION_CONFIG_SHA256,
    FROZEN_STAGE_A_SUFFIX_B_INPUT_FILES,
    FROZEN_STAGE_A_SUFFIX_B_INPUT_MANIFEST_SHA256,
    FROZEN_STAGE_A_SUFFIX_B_OUTPUT_BATCH_IDS_SHA256,
    FROZEN_STAGE_A_SUFFIX_B_OUTPUT_FILES,
    PILOT_CONTENT_GATES,
    PILOT_CONTROLS,
    STAGE_A2_CANDIDATE_CONTROL_FINAL_ROLES,
    STAGE_A2_UNOWNED_ATTACHMENT_CONTROLS,
    STAGE_A2_UNOWNED_FINAL_ROLE_CONTROLS,
    _selection_rows as line_ledger_selection_rows,
    load_frozen_stage_a_suffix_b,
)
from scripts.hive.extract_candidate_endorsement_line_ledger_vllm import (  # noqa: E402
    DEFAULT_MAX_OUTPUT_TOKENS as LINE_LEDGER_MAX_OUTPUT_TOKENS,
    FROZEN_GENERATION_SEED as LINE_LEDGER_GENERATION_SEED,
    PROMPT_TOKEN_COUNT_SPEC_SHA256,
    RUN_METADATA_VERSION as LINE_LEDGER_RUN_METADATA_VERSION,
    generation_config_payload as line_ledger_generation_config_payload,
    load_validated_stage_run,
    schema_variant_inventory as line_ledger_schema_variant_inventory,
)
from scripts.hive.extract_candidate_pages_vllm import (  # noqa: E402
    GENERATION_CONFIG_VERSION,
    RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID,
    RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256,
    STRUCTURED_DECODING_BACKEND,
    STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
    STRUCTURED_DECODING_MODE,
    build_user_message,
    canonicalize_json_string_controls,
    strict_json_loads,
    validate_raw_structural_whitespace,
)
from scripts.build_candidate_endorsement_line_ledger_a3_handcode import (  # noqa: E402
    A3_SPEC,
    A3_SPEC_SHA256,
    derive_a3_residuals,
)
from scripts.import_candidate_endorsement_line_ledger_a3_handcode import (  # noqa: E402
    merge_human_a3,
    unchanged_gate_replay,
    validate_results as validate_a3_results,
    validate_task_provenance as validate_a3_task_provenance,
)


# The importer binds its contract at module load. Version 2.2 differs from 2.1
# only in the alignment spec, so a 2.2 import reads the same generation fields
# and the same stored model outputs. The default stays 2.1 so every existing
# call keeps its behaviour; the 2.2 re-alignment sets this variable.
SCHEMA_VERSION = os.environ.get("CANDIDATE_EXTRACTION_SCHEMA_VERSION", "2.1")
if SCHEMA_VERSION not in ("2.1", "2.2"):
    raise RuntimeError(f"this importer reads 2.1 and 2.2, not {SCHEMA_VERSION}")
LIST_KEYS = LIST_KEY_BY_VERSION[SCHEMA_VERSION]
SEGMENTS_FILENAME = "segments_enriched.jsonl.gz"
ENDORSEMENT_LINE_LEDGER_FILENAME = "endorsement_line_ledger_private.jsonl.gz"
ENDORSEMENT_ROUND4_LEDGER_FILENAME = (
    "endorsement_round4_record_ledger_private.jsonl.gz"
)

# The line-ledger contract hash as it stood before the Stage-A3 human
# adjudication was imported. The contract payload includes that adjudication,
# so completing it advanced the hash. Artifacts written earlier carry this
# value and cannot be regenerated to match the current one.
PRE_STAGE_A3_LINE_LEDGER_CONTRACT_SHA256 = (
    "d8072b6269438b1742c47f1a25ff271edab32add331a18db9898d439f9b25bb0"
)

# Read from the completed production runs on 2026-09-18, after both stages
# finished: Stage A wrote 320 of 320 output chunks and Stage B wrote 671 of 671.
# Each stage carries exactly one generation identity across its shards, verified
# over all four Stage-A and all eight Stage-B run-metadata files. The Stage-A
# input manifest hash also matches the value the Stage-B build command asserted.
FROZEN_ROUND4_PRODUCTION_STAGE_A_INPUT_MANIFEST_SHA256 = (
    "032a6694a27457ea93ea944e2f71a2eb1dade564c32e2749dd0ed46fce8fa477"
)
FROZEN_ROUND4_PRODUCTION_STAGE_A_GENERATION_CONFIG_SHA256 = (
    "159517b984d31e8cc5deee394d978556d83191fda03ffde5662c6e5753fa7840"
)
FROZEN_ROUND4_PRODUCTION_STAGE_B_INPUT_MANIFEST_SHA256 = (
    "13bba4688d63fc665b42911519a8c414dd9c766e7628ed2734234536c4daff98"
)
FROZEN_ROUND4_PRODUCTION_STAGE_B_GENERATION_CONFIG_SHA256 = (
    "cd309252f5231a929594924e29020584e69d3d36b89e37c8a1ddb022cab2cfac"
)
HUMAN_VALIDATION_FILENAME = "human_validation.json"
CONTRACT = get_extraction_contract(SCHEMA_VERSION)
HEX64 = re.compile(r"^[0-9a-f]{64}$")
LINE_LEDGER_MAX_MODEL_LEN = 32_768
FROZEN_STAGE_A2_CHUNK_SIZE = 60
FROZEN_STAGE_A2_INPUT_MANIFEST_SHA256 = (
    "caa26dbd4b978ba71859c58f78a8b35c689daa2c873cd44e9fd9bed63b3f6191"
)
FROZEN_STAGE_A2_GENERATION_CONFIG_SHA256 = (
    "446aa533312fffacea3ebaaa4642e0c353dc1cccb8b49fe6ee23e7216d101ba6"
)
FROZEN_STAGE_A3_TASK_SHA256 = (
    "e834f23d55804bc138d6dac1441dfbd96ab0449ddd34e1eeb0edf2585d96bfe5"
)
FROZEN_STAGE_A3_HTML_SHA256 = (
    "ba9649cb0538ba6422fa6469f5455b38b0567ed988f47e5e060826a8659b73f3"
)
STAGE_A3_PRIVATE_KIND = "stage_a3_human_dependency"
LINE_LEDGER_ARTIFACT_FIELDS = {
    "input_manifest_sha256",
    "generation_config_sha256",
    "max_model_len",
    "max_output_tokens",
    "input_files",
    "output_files",
    "output_batch_ids_sha256",
    "validated_output_batches",
    "run_metadata_files",
    "typed_restart_replay_complete",
}
LINE_LEDGER_MANIFEST_BASE_FIELDS = {
    "builder",
    "builder_version",
    "mode",
    "stage",
    "route",
    "source_export",
    "line_ledger_contract",
    "line_ledger_contract_sha256",
    "frozen_line_ledger_contract_sha256",
    "contract_is_frozen",
    "line_inventory_spec_sha256",
    "line_batch_spec_sha256",
    "json_schema_catalog_sha256",
    "json_schema_variant_sha256",
    "schema_variant_counts",
    "schema_variant_hashes_used",
    "ordered_schema_variant_ids_sha256",
    "ordered_schema_variant_hashes_sha256",
    "unchanged_route_hashes",
    "corrected_71c_issue_audit_gate",
    "selected_parent_ids",
    "selected_parent_ids_sha256",
    "complete_parent_closure",
    "inventory_census",
    "expected_batch_counts",
    "expected_batch_ids",
    "expected_batch_ids_sha256",
    "expected_stage_batch_count",
    "exported_records",
    "chunk_size",
    "files",
    "bounded_preflight",
    "manifest_payload_sha256",
}
LINE_LEDGER_RUN_METADATA_FIELDS = {
    "run_metadata_version",
    "accounting_scope",
    "recovery_attestation",
    "stage",
    "input_manifest_sha256",
    "line_ledger_contract_sha256",
    "prompt_sha256",
    "json_schema_catalog_sha256",
    "json_schema_variant_sha256",
    "line_inventory_spec_sha256",
    "line_batch_spec_sha256",
    "generation_config_sha256",
    "generation_config",
    "model_id",
    "model_revision",
    "runtime_lock_sha256",
    "max_model_len",
    "max_output_tokens",
    "prompt_token_count_spec_sha256",
    "structured_decoding_backend",
    "structured_decoding_disable_any_whitespace",
    "shard_index",
    "shard_count",
    "selected_batches",
    "selected_schema_variant_counts",
    "selected_schema_variant_hashes_used",
    "selected_schema_variant_ids_sha256",
    "selected_schema_variant_hashes_sha256",
    "processed_outputs_this_run",
    "processed_schema_variant_counts_this_run",
    "technical_pass_outputs_this_run",
    "parse_status_counts_this_run",
    "finish_reason_counts_this_run",
    "output_tokens_total_this_run",
    "output_tokens_max_this_run",
    "prompt_tokens_max_this_run",
    "minimum_context_margin_tokens_this_run",
    "started_at",
    "completed_at",
    "host",
    "slurm_job_id",
    "slurm_array_task_id",
}
LINE_LEDGER_CHUNK_CONTENT_ATTESTATION_VERSION = (
    "canonical_sorted_jsonl_utf8_v1"
)
HISTORICAL_STAGE_A_RUN_METADATA_VERSION = (
    "candidate_endorsement_line_ledger_run_metadata_v3"
)
HISTORICAL_STAGE_A_GENERATION_CONFIG_VERSION = (
    "candidate_endorsement_line_ledger_generation_v2"
)
# The frozen v2.1 generation identity. The alignment spec sits inside the
# generation config, so version 2.2 necessarily has its own hash. The 2.1
# literal stays as a fail-closed assertion: an accidental edit to any contract
# input still breaks the build instead of changing this value silently.
FROZEN_V2_1_GENERATION_CONFIG_SHA256 = (
    "edfe53dd421acb30f1da4bc17404be4b5415a3d6060b77650c448d76edafc13f"
)
EXPECTED_GENERATION_SEED = 20260824
EXPECTED_MAX_MODEL_LEN = 32_768
EXPECTED_MAX_OUTPUT_TOKENS = 24_576
FROZEN_SUFFIX_P_FULL_SOURCE_MANIFEST_SHA256 = (
    "f5e6545a2d2ec35a83f4184e0d328635e5fba9d3c80cd4c8a7466178b7d9172c"
)
FROZEN_SUFFIX_P_SOURCE_CORPUS_SHA256 = (
    "c53d36b24b293ac5d47706aa67731f1c107d6a679cb948c9ad71a70b5c8ce7c3"
)
FROZEN_SUFFIX_P_SOURCE_DOCUMENT_IDS_SHA256 = (
    "826f3b4d6622c06e03448beff2d20b59437d51bce04630ec520c36b4d461ba21"
)
FROZEN_SUFFIX_P_COMPLETE_LINE_CENSUS = {
    "endorsement_segments": 6_096,
    "segments_with_complete_line": 6_055,
    "segments_without_complete_line": 41,
    "first_source_display_boundary_differs": 169,
    "last_source_display_boundary_differs": 988,
}
EXPECTED_STRUCTURED_DECODING = {
    "mode": STRUCTURED_DECODING_MODE,
    "backend": STRUCTURED_DECODING_BACKEND,
    "disable_any_whitespace": STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
    "route_json_schema_sha256": CONTRACT["json_schema_sha256"],
}
EXPECTED_GENERATION_CONFIG = {
    "generation_config_version": GENERATION_CONFIG_VERSION,
    "schema_version": SCHEMA_VERSION,
    "model_id": MODEL_ID,
    "model_revision": MODEL_REVISION,
    "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
    "prompt_sha256": CONTRACT["prompt_sha256"],
    "json_schema_sha256": CONTRACT["json_schema_sha256"],
    "extraction_contract_sha256": CONTRACT["extraction_contract_sha256"],
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
    "seed": EXPECTED_GENERATION_SEED,
    "max_model_len": EXPECTED_MAX_MODEL_LEN,
    "max_output_tokens": EXPECTED_MAX_OUTPUT_TOKENS,
    "temperature": 0.0,
    "thinking_enabled": False,
    "compact_repair": False,
    "structured_decoding": EXPECTED_STRUCTURED_DECODING,
    "inference_display_escape_id": CONTRACT["inference_display_escape_id"],
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
}

OUTPUT_CANONICALIZATION_FIELDS = (
    "json_string_control_canonicalizer_id",
    "json_string_control_canonicalizer_spec_sha256",
    "raw_output_sha256",
    "raw_output_n_chars",
    "raw_output_utf8_n_bytes",
    "canonical_output_sha256",
    "canonical_output_n_chars",
    "canonical_output_utf8_n_bytes",
    "json_control_canonicalization_applied",
    "json_control_canonicalization_count",
    "json_control_canonicalization_codepoint_counts",
    "json_control_unrepaired_after_escape_count",
    "json_control_unrepaired_after_escape_codepoint_counts",
    "json_control_outside_string_permitted_whitespace_count",
    "json_control_outside_string_permitted_whitespace_codepoint_counts",
    "json_control_outside_string_invalid_count",
    "json_control_outside_string_invalid_codepoint_counts",
)
OUTPUT_COUNT_FIELDS = (
    "raw_output_n_chars",
    "raw_output_utf8_n_bytes",
    "canonical_output_n_chars",
    "canonical_output_utf8_n_bytes",
    "json_control_canonicalization_count",
    "json_control_unrepaired_after_escape_count",
    "json_control_outside_string_permitted_whitespace_count",
    "json_control_outside_string_invalid_count",
)
OUTPUT_CODEPOINT_COUNT_FIELDS = (
    "json_control_canonicalization_codepoint_counts",
    "json_control_unrepaired_after_escape_codepoint_counts",
    "json_control_outside_string_permitted_whitespace_codepoint_counts",
    "json_control_outside_string_invalid_codepoint_counts",
)
OUTPUT_INFERENCE_DISPLAY_FIELDS = (
    "inference_display_escape_id",
    "inference_display_escape_spec_sha256",
    "inference_display_source_text_sha256",
    "inference_display_source_n_chars",
    "inference_display_text_sha256",
    "inference_display_chars_used",
    "inference_display_escape_count",
    "inference_display_escape_codepoint_counts",
    "inference_display_interior_boundary_count",
    "inference_display_boundary_map_sha256",
    "inference_display_roundtrip_valid",
)
OUTPUT_RAW_STRUCTURAL_WHITESPACE_FIELDS = (
    "raw_structural_whitespace_validator_id",
    "raw_structural_whitespace_validator_spec_sha256",
    "raw_structural_whitespace_validation_passed",
    "raw_structural_separator_count",
    "raw_structural_separator_space_count",
    "raw_structural_whitespace_violation_count",
    "raw_structural_whitespace_violation_counts",
)
OUTPUT_COMMON_FIELDS = {
    "document_id", "parent_document_id", "candidate_cycle_id", "route",
    "raw_text_sha256", "segment_index", "segment_count",
    "segment_start_char", "segment_end_char", "segment_text_sha256",
    "model_id", "model_revision", "runtime_lock_sha256", "schema_version",
    "prompt_sha256", "json_schema_sha256", "extraction_contract_sha256",
    "alignment_id", "alignment_spec_sha256",
    "semantic_validation_spec_sha256",
    "endorsement_context_provenance_spec_sha256",
    "user_message_template_id", "user_message_template_sha256",
    "user_message_sha256",
    "generation_config_sha256", "temperature", "thinking_enabled",
    "structured_decoding_backend",
    "structured_decoding_disable_any_whitespace",
    "compact_repair", "input_truncated", "input_chars_used",
    "input_tokens_used", "max_output_tokens", "finish_reason",
    "stop_reason", "output_tokens", "parse_status",
    "array_item_cap_spec_id", "array_item_cap_spec_sha256",
    "route_array_item_cap", "model_item_repetition_spec_id",
    "model_item_repetition_spec_sha256",
    "raw_output", *OUTPUT_CANONICALIZATION_FIELDS,
    *OUTPUT_INFERENCE_DISPLAY_FIELDS,
    *OUTPUT_RAW_STRUCTURAL_WHITESPACE_FIELDS,
}
OUTPUT_PARSED_GUARD_FIELDS = {
    "model_extraction", "model_extraction_sha256",
    "pre_alignment_model_item_guard", "array_item_cap_report",
    "model_item_repetition_report",
}
OUTPUT_ALIGNED_SUCCESS_FIELDS = {
    "extraction",
    "aligned_extraction_sha256", "withheld_model_items", "exact_alignment",
    "validation",
}
OUTPUT_SUCCESS_FIELDS = OUTPUT_PARSED_GUARD_FIELDS | OUTPUT_ALIGNED_SUCCESS_FIELDS
GUARD_FAILURE_PARSE_STATUSES = {
    ARRAY_ITEM_CAP_REACHED_STATUS,
    MODEL_ITEM_REPETITION_ERROR_STATUS,
}

ANCHORS = {
    "issues": (
        ("source_heading", "source_heading_start_char", "source_heading_end_char"),
        ("evidence", "evidence_start_char", "evidence_end_char"),
    ),
    "biography": (("evidence", "evidence_start_char", "evidence_end_char"),),
    "endorsements": (
        ("record_evidence", "record_evidence_start_char", "record_evidence_end_char"),
        (
            "endorsement_context_evidence",
            "endorsement_context_evidence_start_char",
            "endorsement_context_evidence_end_char",
        ),
    ),
}
SEMANTIC_FIELDS = {
    # The heading is exact source provenance, not a substantive model claim.
    # Null-vs-exact heading variants may therefore resolve deterministically,
    # while claim/kind differences remain fail-closed semantic conflicts.
    "issues": ("claim", "statement_kind"),
    "biography": ("field", "value", "timing", "relationship"),
    "endorsements": (
        "endorser_name",
        "entity_type",
        "endorser_category",
        "title",
        "affiliation",
        "jurisdiction",
        "support_mode",
    ),
}
PRIMARY_EVIDENCE_FIELD = {
    "issues": "evidence",
    "biography": "evidence",
    "endorsements": "record_evidence",
}
ACCOUNTED_PROCESSING_STATUSES = {
    "processed_success",
    "processed_partial_withheld",
    "processed_all_withheld",
}
# A window that greedy decoding drove into the output cap, and that every
# approved repair attempt left there. Hanno approved admitting these as withheld
# on 2026-09-18. It is an accounted outcome only when a repair attestation names
# the window as unrepaired, so the caller must ask for it.
OUTPUT_CAP_WITHHELD_STATUS = "failed_output_cap_unrepaired"
KNOWN_OMISSION_MATCHING_SPEC = {
    "version": "candidate_extraction_known_omission_matching_v2",
    "source_text": "transformed_extraction_text_exact_unicode",
    "record_scope": "resolved_public_eligible_records_only",
    "route_and_type": "exact_handcoded_route_and_record_type",
    "span_relation": "positive_overlap_of_zero_based_half_open_parent_spans",
    "primary_evidence": {
        "issues": "evidence",
        "biography": "evidence",
        "endorsements": "record_evidence",
    },
    "expected_value": "retained_for_independent_human_confirmation_only",
}


def canonical_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _typed_equal(left: Any, right: Any) -> bool:
    """Compare provenance values without Python's bool/integer coercion."""
    if type(left) is not type(right):
        return False
    if isinstance(left, dict):
        return left.keys() == right.keys() and all(
            _typed_equal(left[key], right[key]) for key in left
        )
    if isinstance(left, (list, tuple)):
        return len(left) == len(right) and all(
            _typed_equal(left_item, right_item)
            for left_item, right_item in zip(left, right, strict=True)
        )
    return left == right


EXPECTED_GENERATION_CONFIG_SHA256 = sha256_text(
    canonical_json(EXPECTED_GENERATION_CONFIG)
)
if SCHEMA_VERSION == "2.1" and (
    EXPECTED_GENERATION_CONFIG_SHA256 != FROZEN_V2_1_GENERATION_CONFIG_SHA256
):
    raise RuntimeError(
        "the v2.1 generation config differs from its frozen SHA-256"
    )


def generation_config_for_seed(seed: int) -> tuple[dict[str, Any], str]:
    """Return the generation config for one seed and its hash.

    A seed-vary repair keeps every field except the seed, so its identity is
    this block with the seed replaced. The caller uses it to map a repair run's
    hash across schema versions.
    """
    config = {**EXPECTED_GENERATION_CONFIG, "seed": int(seed)}
    return config, sha256_text(canonical_json(config))


def validate_generation_config_identity(
    generation_config: Any,
    generation_config_sha256: Any,
    label: str,
) -> dict[str, Any]:
    """Require the exact complete frozen v2.1 generation identity."""
    generation_hash = str(generation_config_sha256 or "")
    if not isinstance(generation_config, dict) or not HEX64.fullmatch(
        generation_hash
    ):
        raise ValueError(f"{label}: generation config provenance is absent")
    if sha256_text(canonical_json(generation_config)) != generation_hash:
        raise ValueError(f"{label}: generation config hash differs")
    if generation_hash != EXPECTED_GENERATION_CONFIG_SHA256:
        raise ValueError(
            f"{label}: generation config is not the frozen v2.1 identity"
        )
    if not _typed_equal(generation_config, EXPECTED_GENERATION_CONFIG):
        differing = sorted(
            set(generation_config) ^ set(EXPECTED_GENERATION_CONFIG)
            | {
                key
                for key in set(generation_config) & set(EXPECTED_GENERATION_CONFIG)
                if not _typed_equal(
                    generation_config[key], EXPECTED_GENERATION_CONFIG[key]
                )
            }
        )
        raise ValueError(
            f"{label}: complete frozen generation config differs: {differing}"
        )
    return generation_config


def technical_segment_gate_passes(
    status_counts: Mapping[str, int],
    segment_count: int,
    *,
    allow_withheld_output_cap: bool = False,
) -> bool:
    """Return whether every inference window has a fully accounted outcome.

    `allow_withheld_output_cap` admits the cap-limited windows no repair attempt
    recovered. The default refuses them, so a truncated row cannot pass this gate
    on its own. The caller sets the flag only when a repair attestation names
    every such window as unrepaired, which is the evidence that the outcome is
    final rather than unexamined.
    """
    accounted = set(ACCOUNTED_PROCESSING_STATUSES)
    if allow_withheld_output_cap:
        accounted.add(OUTPUT_CAP_WITHHELD_STATUS)
    return (
        sum(int(value) for value in status_counts.values()) == segment_count
        and set(status_counts) <= accounted
    )


KNOWN_OMISSION_MATCHING_SPEC_SHA256 = sha256_text(
    canonical_json(KNOWN_OMISSION_MATCHING_SPEC)
)

# Seed-vary repair of cap-limited windows. Hanno approved a retry with a
# different seed under a byte-identical prompt, schema, model, and contract. He
# approved a third seed on September 17, 2026, after the first repair recovered 10
# of 21 windows. Greedy decoding at temperature 0 returns identical bytes for a
# repeated window, so each retry needs its own seed and its own generation
# identity. The importer accepts a declared repair identity for exactly the
# windows an attestation names, and records the attempt.

REPAIR_MANIFEST_VERSION = "candidate_extraction_seed_vary_repair_v1"
REPAIR_MANIFEST_NAME = "repair_manifest.json"
REPAIR_ATTEMPT_INDEX = 2
TRUNCATED_PARSE_STATUS = "output_truncated"
REPAIR_REASON = (
    "output_truncated: greedy decoding repeated one item and reached the output cap, "
    "so an identical rerun returns identical bytes"
)
# Attempt index to seed. The primary run is attempt 1.
EXPECTED_REPAIR_GENERATION_SEEDS = {2: 20_260_917, 3: 20_260_918}
EXPECTED_REPAIR_GENERATION_CONFIGS = {
    attempt: {**EXPECTED_GENERATION_CONFIG, "seed": seed}
    for attempt, seed in EXPECTED_REPAIR_GENERATION_SEEDS.items()
}
EXPECTED_REPAIR_GENERATION_CONFIG_SHA256S = {
    attempt: sha256_text(canonical_json(config))
    for attempt, config in EXPECTED_REPAIR_GENERATION_CONFIGS.items()
}
# The first repair attempt keeps its singular names for the existing callers.
EXPECTED_REPAIR_GENERATION_SEED = EXPECTED_REPAIR_GENERATION_SEEDS[
    REPAIR_ATTEMPT_INDEX
]
EXPECTED_REPAIR_GENERATION_CONFIG = EXPECTED_REPAIR_GENERATION_CONFIGS[
    REPAIR_ATTEMPT_INDEX
]
EXPECTED_REPAIR_GENERATION_CONFIG_SHA256 = (
    EXPECTED_REPAIR_GENERATION_CONFIG_SHA256S[REPAIR_ATTEMPT_INDEX]
)
REPAIR_TARGET_ENTRY_FIELDS = {
    "chunk",
    "document_id",
    "route",
    "input_chunk_sha256",
    "repair_output_file",
    "repair_output_sha256",
    "finish_reason",
    "parse_status",
    "output_tokens",
    "raw_output_sha256",
    "generation_config_sha256",
    "repaired",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _nonnegative_integer(value: Any, label: str) -> int:
    if (
        not isinstance(value, int)
        or isinstance(value, bool)
        or value < 0
    ):
        raise ValueError(f"{label} is not a nonnegative integer")
    return int(value)


def _count_mapping(value: Any, label: str) -> dict[str, int]:
    if not isinstance(value, dict):
        raise ValueError(f"{label} is not an object")
    result: dict[str, int] = {}
    for key, count in value.items():
        text = str(key)
        if not text:
            raise ValueError(f"{label} has a blank key")
        result[text] = _nonnegative_integer(count, f"{label}.{text}")
    return dict(sorted(result.items()))


def validate_private_output_provenance(
    row: Mapping[str, Any],
    source_text: str,
    label: str,
    *,
    source_record: Mapping[str, Any] | None = None,
) -> tuple[Any | None, dict[str, Any]]:
    """Replay display, JSON parsing, and both pre-alignment model-item guards.

    The raw generation remains private. The parsed object returned here is the
    only object that may feed exact alignment and public-record validation. A
    valid-JSON guard failure retains its parsed model payload privately, but
    this function returns ``None`` so downstream code cannot align it.
    """
    if not isinstance(source_text, str):
        raise ValueError(f"{label}: source text is not a string")
    message_source = source_record if source_record is not None else row
    try:
        user_message = build_user_message(
            dict(message_source), source_text, SCHEMA_VERSION
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(
            f"{label}: exact user message cannot be reconstructed"
        ) from error
    expected_user_message_sha256 = sha256_text(user_message)
    if not _typed_equal(
        row.get("user_message_sha256"), expected_user_message_sha256
    ):
        raise ValueError(
            f"{label}: user_message_sha256 differs from exact message replay"
        )
    display_provenance = inference_display_provenance(source_text)
    for field in OUTPUT_INFERENCE_DISPLAY_FIELDS:
        if not _typed_equal(row.get(field), display_provenance[field]):
            raise ValueError(
                f"{label}: {field} differs from source-text display replay"
            )

    raw_output = row.get("raw_output")
    if not isinstance(raw_output, str):
        raise ValueError(f"{label}: raw model output is absent")
    structural_whitespace = validate_raw_structural_whitespace(raw_output)
    for field in OUTPUT_RAW_STRUCTURAL_WHITESPACE_FIELDS:
        if not _typed_equal(row.get(field), structural_whitespace[field]):
            raise ValueError(
                f"{label}: {field} differs from raw-output structural replay"
            )
    canonical_output, report = canonicalize_json_string_controls(raw_output)
    report = {**report, **structural_whitespace}
    for field in OUTPUT_CANONICALIZATION_FIELDS:
        if not _typed_equal(row.get(field), report[field]):
            raise ValueError(f"{label}: {field} differs from raw output")
    if bool(report["json_control_canonicalization_applied"]) != (
        int(report["json_control_canonicalization_count"]) > 0
    ):
        raise ValueError(f"{label}: canonicalization applied flag differs")
    for count_field, mapping_field in (
        (
            "json_control_canonicalization_count",
            "json_control_canonicalization_codepoint_counts",
        ),
        (
            "json_control_unrepaired_after_escape_count",
            "json_control_unrepaired_after_escape_codepoint_counts",
        ),
        (
            "json_control_outside_string_permitted_whitespace_count",
            "json_control_outside_string_permitted_whitespace_codepoint_counts",
        ),
        (
            "json_control_outside_string_invalid_count",
            "json_control_outside_string_invalid_codepoint_counts",
        ),
    ):
        if int(report[count_field]) != sum(int(value) for value in report[mapping_field].values()):
            raise ValueError(f"{label}: {count_field} does not reconcile")

    def require_no_parsed_or_aligned_payload() -> None:
        for field in (
            "model_extraction",
            "model_extraction_sha256",
            "pre_alignment_model_item_guard",
            "array_item_cap_report",
            "model_item_repetition_report",
            "extraction",
            "aligned_extraction_sha256",
            "exact_alignment",
        ):
            if row.get(field) is not None:
                raise ValueError(
                    f"{label}: failed generation retains derived field {field}"
                )
        if row.get("withheld_model_items") not in (None, []):
            raise ValueError(
                f"{label}: failed generation retains withheld model items"
            )

    def require_no_aligned_payload() -> None:
        for field in (
            "extraction",
            "aligned_extraction_sha256",
            "exact_alignment",
        ):
            if row.get(field) is not None:
                raise ValueError(
                    f"{label}: guard failure retains aligned field {field}"
                )
        if row.get("withheld_model_items") not in (None, []):
            raise ValueError(
                f"{label}: guard failure retains withheld model items"
            )

    finish_reason = str(row.get("finish_reason") or "")
    if finish_reason != "stop":
        expected_status = (
            "output_truncated"
            if finish_reason == "length"
            else "generation_incomplete"
        )
        if row.get("parse_status") != expected_status:
            raise ValueError(f"{label}: non-stop parse status differs")
        require_no_parsed_or_aligned_payload()
        return None, report

    if not structural_whitespace[
        "raw_structural_whitespace_validation_passed"
    ]:
        if row.get("parse_status") != "raw_structural_whitespace_error":
            raise ValueError(
                f"{label}: raw structural-whitespace failure status differs"
            )
        require_no_parsed_or_aligned_payload()
        return None, report

    parsed: Any | None = None
    parse_error: ValueError | None = None
    try:
        parsed = strict_json_loads(canonical_output)
    except ValueError as error:
        parse_error = error
    parse_status = str(row.get("parse_status") or "")
    strict_parse_statuses = {"parsed", *GUARD_FAILURE_PARSE_STATUSES}
    if parse_status not in strict_parse_statuses:
        if parse_error is None:
            raise ValueError(f"{label}: strict JSON parsed despite stored parse failure")
        if parse_status != "strict_json_parse_error":
            raise ValueError(f"{label}: stored parse status differs from strict JSON")
        require_no_parsed_or_aligned_payload()
        return None, report

    if parse_error is not None:
        raise ValueError(f"{label}: stored strict-JSON output does not parse") from parse_error
    if report["json_control_unrepaired_after_escape_count"] != 0:
        raise ValueError(f"{label}: parsed output has an unrepaired escaped control")
    if report["json_control_outside_string_invalid_count"] != 0:
        raise ValueError(f"{label}: parsed output has an invalid outside-string control")
    if row.get("model_extraction") != parsed:
        raise ValueError(f"{label}: model extraction differs from canonical output")
    parsed_sha256 = sha256_text(canonical_json(parsed))
    if row.get("model_extraction_sha256") != parsed_sha256:
        raise ValueError(f"{label}: model extraction hash differs")

    route = str(row.get("route") or "")
    if route not in LIST_KEYS:
        raise ValueError(f"{label}: route is invalid for model-item guards")
    guard = pre_alignment_model_item_guard_report(route, parsed)
    expected_reports = {
        "pre_alignment_model_item_guard": guard,
        "array_item_cap_report": guard["array_item_cap_report"],
        "model_item_repetition_report": guard["model_item_repetition_report"],
    }
    for field, expected in expected_reports.items():
        if not _typed_equal(row.get(field), expected):
            raise ValueError(f"{label}: {field} differs from recomputation")
    failure_status = guard["failure_status"]
    guard_passed = guard[
        "pre_alignment_model_item_guard_validation_passed"
    ]
    if parse_status == "parsed":
        if guard_passed is not True or failure_status is not None:
            raise ValueError(f"{label}: parsed output failed a pre-alignment guard")
        return parsed, report
    if failure_status != parse_status or guard_passed is not False:
        raise ValueError(f"{label}: stored guard-failure status differs")
    require_no_aligned_payload()
    return None, report


def output_provenance_summary(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Aggregate private output accounting after every row has been replayed."""
    rows = list(rows)
    totals = {field: 0 for field in OUTPUT_COUNT_FIELDS}
    mappings = {field: Counter() for field in OUTPUT_CODEPOINT_COUNT_FIELDS}
    applied = 0
    display_changed = 0
    structural_valid = 0
    structural_separator_count = 0
    structural_separator_space_count = 0
    structural_violation_count = 0
    structural_violation_counts: Counter[str] = Counter()
    for row in rows:
        source_text = str(row.get("source_text") or "")
        _, report = validate_private_output_provenance(
            row,
            source_text,
            str(row.get("document_id") or "segment"),
            source_record=row,
        )
        applied += int(bool(report["json_control_canonicalization_applied"]))
        display_changed += int(inference_display_escape(source_text) != source_text)
        for field in OUTPUT_COUNT_FIELDS:
            totals[field] += int(report[field])
        for field in OUTPUT_CODEPOINT_COUNT_FIELDS:
            mappings[field].update(
                {str(key): int(value) for key, value in report[field].items()}
            )
        structural_valid += int(
            report["raw_structural_whitespace_validation_passed"] is True
        )
        structural_separator_count += int(
            report["raw_structural_separator_count"]
        )
        structural_separator_space_count += int(
            report["raw_structural_separator_space_count"]
        )
        structural_violation_count += int(
            report["raw_structural_whitespace_violation_count"]
        )
        structural_violation_counts.update(
            {
                str(key): int(value)
                for key, value in report[
                    "raw_structural_whitespace_violation_counts"
                ].items()
            }
        )
    structural_invalid = len(rows) - structural_valid
    if structural_violation_count != sum(structural_violation_counts.values()):
        raise ValueError("raw structural-whitespace counts do not reconcile")
    return {
        "version": "candidate_extraction_private_output_accounting_v2",
        "segments": len(rows),
        "inference_display_changed_segments": display_changed,
        "json_control_canonicalized_outputs": applied,
        "raw_structural_whitespace_validator_id": (
            RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID
        ),
        "raw_structural_whitespace_validator_spec_sha256": (
            RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256
        ),
        "raw_structural_whitespace_gate_passed": structural_invalid == 0,
        "raw_structural_whitespace_checked_outputs": len(rows),
        "raw_structural_whitespace_valid_outputs": structural_valid,
        "raw_structural_whitespace_invalid_outputs": structural_invalid,
        "raw_structural_separator_count": structural_separator_count,
        "raw_structural_separator_space_count": (
            structural_separator_space_count
        ),
        "raw_structural_whitespace_violation_count": (
            structural_violation_count
        ),
        "raw_structural_whitespace_violation_counts": dict(
            sorted(structural_violation_counts.items())
        ),
        **{f"{field}_total": value for field, value in totals.items()},
        **{
            f"{field}_total": dict(sorted(counter.items()))
            for field, counter in mappings.items()
        },
    }


def all_output_aggregate(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    """Recompute corpus-wide output counts independently of restart ledgers."""
    rows = list(rows)
    parse_statuses: Counter[str] = Counter()
    processing_statuses: Counter[str] = Counter()
    finish_reasons: Counter[str] = Counter()
    alignment_statuses: Counter[str] = Counter()
    canonicalized_codepoints: Counter[str] = Counter()
    input_truncated = schema_valid = exactly_supported = 0
    output_tokens_total = output_tokens_max = 0
    model_items = aligned_items = withheld_items = 0
    strict_json_parsed_outputs = model_items_strictly_parsed = 0
    array_item_cap_reached_outputs = model_item_repetition_error_outputs = 0
    model_item_repetition_groups = max_exact_model_item_multiplicity = 0
    route_model_item_count_max = {
        route: 0 for route in CONTRACT["route_array_item_caps"]
    }
    canonicalized_outputs = canonicalized_characters = 0
    for row in rows:
        parse_statuses[str(row.get("parse_status") or "missing")] += 1
        processing_statuses[str(row.get("processing_status") or "missing")] += 1
        finish_reasons[str(row.get("finish_reason") or "missing")] += 1
        input_truncated += int(row.get("input_truncated") is True)
        tokens = _nonnegative_integer(
            row.get("output_tokens"), "all_output_aggregate.output_tokens"
        )
        output_tokens_total += tokens
        output_tokens_max = max(output_tokens_max, tokens)
        validation = row.get("validation")
        if isinstance(validation, Mapping):
            schema_valid += int(validation.get("schema_valid") is True)
            exactly_supported += int(
                validation.get("schema_valid") is True
                and validation.get("n_items") == validation.get("n_supported")
            )
        alignment = row.get("exact_alignment")
        if isinstance(alignment, Mapping):
            alignment_statuses[str(alignment.get("status") or "missing")] += 1
            model_items += _nonnegative_integer(
                alignment.get("model_item_count"),
                "all_output_aggregate.model_item_count",
            )
            aligned_items += _nonnegative_integer(
                alignment.get("aligned_item_count"),
                "all_output_aggregate.aligned_item_count",
            )
            withheld_items += _nonnegative_integer(
                alignment.get("withheld_item_count"),
                "all_output_aggregate.withheld_item_count",
            )
        parse_status = str(row.get("parse_status") or "")
        if parse_status in {"parsed", *GUARD_FAILURE_PARSE_STATUSES}:
            strict_json_parsed_outputs += 1
            cap_report = row.get("array_item_cap_report")
            repetition_report = row.get("model_item_repetition_report")
            if not isinstance(cap_report, Mapping) or not isinstance(
                repetition_report, Mapping
            ):
                raise ValueError("all-output guard reports are absent")
            route = str(row.get("route") or "")
            if route not in route_model_item_count_max:
                raise ValueError("all-output guard route differs")
            item_count = _nonnegative_integer(
                cap_report.get("model_item_count"),
                "all_output_aggregate.strict_model_item_count",
            )
            model_items_strictly_parsed += item_count
            route_model_item_count_max[route] = max(
                route_model_item_count_max[route], item_count
            )
            repeated_groups = _nonnegative_integer(
                repetition_report.get("repeated_model_item_group_count"),
                "all_output_aggregate.repeated_model_item_group_count",
            )
            multiplicity = _nonnegative_integer(
                repetition_report.get("max_exact_model_item_multiplicity"),
                "all_output_aggregate.max_exact_model_item_multiplicity",
            )
            model_item_repetition_groups += repeated_groups
            max_exact_model_item_multiplicity = max(
                max_exact_model_item_multiplicity, multiplicity
            )
        array_item_cap_reached_outputs += int(
            parse_status == ARRAY_ITEM_CAP_REACHED_STATUS
        )
        model_item_repetition_error_outputs += int(
            parse_status == MODEL_ITEM_REPETITION_ERROR_STATUS
        )
        repaired = _nonnegative_integer(
            row.get("json_control_canonicalization_count"),
            "all_output_aggregate.json_control_canonicalization_count",
        )
        canonicalized_outputs += int(repaired > 0)
        canonicalized_characters += repaired
        canonicalized_codepoints.update(
            _count_mapping(
                row.get("json_control_canonicalization_codepoint_counts"),
                "all_output_aggregate.json_control_codepoints",
            )
        )
    if model_items != aligned_items + withheld_items:
        raise ValueError("all-output model-item counts do not reconcile")
    if model_items_strictly_parsed < model_items:
        raise ValueError("all-output strict model-item counts do not reconcile")
    if (
        array_item_cap_reached_outputs + model_item_repetition_error_outputs
        > strict_json_parsed_outputs
    ):
        raise ValueError("all-output guard-failure counts do not reconcile")
    if canonicalized_characters != sum(canonicalized_codepoints.values()):
        raise ValueError("all-output JSON-control counts do not reconcile")
    return {
        "version": "candidate_extraction_all_output_aggregate_v2",
        "segments": len(rows),
        "parse_status_counts": dict(sorted(parse_statuses.items())),
        "processing_status_counts": dict(sorted(processing_statuses.items())),
        "finish_reason_counts": dict(sorted(finish_reasons.items())),
        "input_truncated_segments": input_truncated,
        "schema_valid_segments": schema_valid,
        "exactly_supported_segments": exactly_supported,
        "output_tokens_total": output_tokens_total,
        "output_tokens_max": output_tokens_max,
        "model_items": model_items,
        "aligned_items": aligned_items,
        "withheld_items": withheld_items,
        "strict_json_parsed_outputs": strict_json_parsed_outputs,
        "model_items_strictly_parsed": model_items_strictly_parsed,
        "array_item_cap_reached_outputs": array_item_cap_reached_outputs,
        "model_item_repetition_error_outputs": (
            model_item_repetition_error_outputs
        ),
        "model_item_repetition_groups": model_item_repetition_groups,
        "max_exact_model_item_multiplicity": (
            max_exact_model_item_multiplicity
        ),
        "route_model_item_count_max": dict(
            sorted(route_model_item_count_max.items())
        ),
        "alignment_status_counts": dict(sorted(alignment_statuses.items())),
        "json_control_canonicalized_outputs": canonicalized_outputs,
        "json_control_canonicalized_characters": canonicalized_characters,
        "json_control_canonicalization_codepoint_counts": dict(
            sorted(canonicalized_codepoints.items())
        ),
    }


RUN_METADATA_SCALAR_FIELDS = (
    "documents_selected_for_shard",
    "documents_processed_this_run",
    "documents_parsed_this_run",
    "documents_schema_valid_this_run",
    "documents_exactly_supported_this_run",
    "output_tokens_total_this_run",
    "output_tokens_max_this_run",
    "model_items_parsed_this_run",
    "aligned_items_this_run",
    "withheld_items_this_run",
    "strict_json_parsed_outputs_this_run",
    "model_items_strictly_parsed_this_run",
    "array_item_cap_reached_outputs_this_run",
    "model_item_repetition_error_outputs_this_run",
    "model_item_repetition_groups_this_run",
    "max_exact_model_item_multiplicity_this_run",
    "json_control_canonicalized_outputs_this_run",
    "json_control_canonicalized_characters_this_run",
)
RUN_METADATA_MAX_FIELDS = (
    "output_tokens_max_this_run",
    "max_exact_model_item_multiplicity_this_run",
)
RUN_METADATA_ADDITIVE_FIELDS = tuple(
    field for field in RUN_METADATA_SCALAR_FIELDS
    if field not in RUN_METADATA_MAX_FIELDS
)
RUN_METADATA_MAPPING_FIELDS = (
    "finish_reason_counts_this_run",
    "alignment_status_counts_this_run",
    "json_control_canonicalization_codepoint_counts_this_run",
)
RUN_METADATA_MAX_MAPPING_FIELDS = ("route_model_item_count_max_this_run",)


def run_metadata_aggregate(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = list(rows)
    totals = {field: 0 for field in RUN_METADATA_SCALAR_FIELDS}
    mappings = {field: Counter() for field in RUN_METADATA_MAPPING_FIELDS}
    max_mappings = {
        field: {route: 0 for route in CONTRACT["route_array_item_caps"]}
        for field in RUN_METADATA_MAX_MAPPING_FIELDS
    }
    recovery = 0
    for row in rows:
        recovery += int(row.get("recovery_attestation_without_inference") is True)
        for field in RUN_METADATA_ADDITIVE_FIELDS:
            totals[field] += _nonnegative_integer(row.get(field), field)
        for field in RUN_METADATA_MAX_FIELDS:
            totals[field] = max(
                totals[field], _nonnegative_integer(row.get(field), field)
            )
        for field in RUN_METADATA_MAPPING_FIELDS:
            mappings[field].update(_count_mapping(row.get(field), field))
        for field in RUN_METADATA_MAX_MAPPING_FIELDS:
            observed = _count_mapping(row.get(field), field)
            if set(observed) != set(CONTRACT["route_array_item_caps"]):
                raise ValueError(f"{field} route keys differ")
            for route, value in observed.items():
                max_mappings[field][route] = max(
                    max_mappings[field][route], value
                )
    return {
        "version": "candidate_extraction_run_metadata_aggregate_v2",
        "metadata_files": len(rows),
        "recovery_attestation_files": recovery,
        **totals,
        **{
            field: dict(sorted(counter.items()))
            for field, counter in mappings.items()
        },
        **{
            field: dict(sorted(values.items()))
            for field, values in max_mappings.items()
        },
    }


def validate_run_metadata_accounting(
    row: Mapping[str, Any], label: str
) -> None:
    """Validate one shard's internally reconciling this-run counters."""
    selected = _nonnegative_integer(
        row.get("documents_selected_for_shard"),
        f"{label}.documents_selected_for_shard",
    )
    processed = _nonnegative_integer(
        row.get("documents_processed_this_run"),
        f"{label}.documents_processed_this_run",
    )
    parsed = _nonnegative_integer(
        row.get("documents_parsed_this_run"),
        f"{label}.documents_parsed_this_run",
    )
    schema_valid = _nonnegative_integer(
        row.get("documents_schema_valid_this_run"),
        f"{label}.documents_schema_valid_this_run",
    )
    exact = _nonnegative_integer(
        row.get("documents_exactly_supported_this_run"),
        f"{label}.documents_exactly_supported_this_run",
    )
    if not (0 <= exact <= schema_valid <= parsed <= processed <= selected):
        raise ValueError(f"{label}: document aggregate counts do not nest")

    finish_counts = _count_mapping(
        row.get("finish_reason_counts_this_run"),
        f"{label}.finish_reason_counts_this_run",
    )
    if any(count <= 0 for count in finish_counts.values()):
        raise ValueError(f"{label}: finish-reason counts contain a zero category")
    if sum(finish_counts.values()) != processed:
        raise ValueError(f"{label}: finish-reason counts do not reconcile")
    total_tokens = _nonnegative_integer(
        row.get("output_tokens_total_this_run"),
        f"{label}.output_tokens_total_this_run",
    )
    max_tokens = _nonnegative_integer(
        row.get("output_tokens_max_this_run"),
        f"{label}.output_tokens_max_this_run",
    )
    if (
        max_tokens > EXPECTED_MAX_OUTPUT_TOKENS
        or (processed == 0 and (total_tokens != 0 or max_tokens != 0))
        or (
            processed > 0
            and not (
                0 < max_tokens <= total_tokens
                <= processed * EXPECTED_MAX_OUTPUT_TOKENS
            )
        )
    ):
        raise ValueError(f"{label}: output-token aggregates differ")

    model_items = _nonnegative_integer(
        row.get("model_items_parsed_this_run"),
        f"{label}.model_items_parsed_this_run",
    )
    aligned_items = _nonnegative_integer(
        row.get("aligned_items_this_run"),
        f"{label}.aligned_items_this_run",
    )
    withheld_items = _nonnegative_integer(
        row.get("withheld_items_this_run"),
        f"{label}.withheld_items_this_run",
    )
    if model_items != aligned_items + withheld_items:
        raise ValueError(f"{label}: model-item aggregates do not reconcile")
    alignment_counts = _count_mapping(
        row.get("alignment_status_counts_this_run"),
        f"{label}.alignment_status_counts_this_run",
    )
    if not set(alignment_counts) <= {
        "complete", "partial_withheld", "all_withheld", "invalid_schema",
    }:
        raise ValueError(f"{label}: alignment-status values differ")
    if any(count <= 0 for count in alignment_counts.values()):
        raise ValueError(f"{label}: alignment-status counts contain a zero category")
    if sum(alignment_counts.values()) != parsed:
        raise ValueError(f"{label}: alignment-status counts do not reconcile")

    strict_parsed = _nonnegative_integer(
        row.get("strict_json_parsed_outputs_this_run"),
        f"{label}.strict_json_parsed_outputs_this_run",
    )
    strict_model_items = _nonnegative_integer(
        row.get("model_items_strictly_parsed_this_run"),
        f"{label}.model_items_strictly_parsed_this_run",
    )
    cap_failures = _nonnegative_integer(
        row.get("array_item_cap_reached_outputs_this_run"),
        f"{label}.array_item_cap_reached_outputs_this_run",
    )
    repetition_failures = _nonnegative_integer(
        row.get("model_item_repetition_error_outputs_this_run"),
        f"{label}.model_item_repetition_error_outputs_this_run",
    )
    repetition_groups = _nonnegative_integer(
        row.get("model_item_repetition_groups_this_run"),
        f"{label}.model_item_repetition_groups_this_run",
    )
    max_multiplicity = _nonnegative_integer(
        row.get("max_exact_model_item_multiplicity_this_run"),
        f"{label}.max_exact_model_item_multiplicity_this_run",
    )
    route_max = _count_mapping(
        row.get("route_model_item_count_max_this_run"),
        f"{label}.route_model_item_count_max_this_run",
    )
    if set(route_max) != set(CONTRACT["route_array_item_caps"]):
        raise ValueError(f"{label}: route model-item maximum keys differ")
    if any(
        value > int(CONTRACT["route_array_item_caps"][route])
        for route, value in route_max.items()
    ):
        raise ValueError(f"{label}: route model-item maximum exceeds its cap")
    if strict_parsed != parsed + cap_failures + repetition_failures:
        raise ValueError(f"{label}: strict-JSON output counts do not reconcile")
    if strict_parsed > processed or strict_model_items < model_items:
        raise ValueError(f"{label}: strict model-item counts do not reconcile")
    if (cap_failures or repetition_failures) and strict_model_items <= model_items:
        raise ValueError(f"{label}: guard failures lack private model items")
    if not (cap_failures or repetition_failures) and strict_model_items != model_items:
        raise ValueError(f"{label}: successful strict model-item counts differ")
    if cap_failures > 0 and not any(
        route_max[route] == int(cap)
        for route, cap in CONTRACT["route_array_item_caps"].items()
    ):
        raise ValueError(f"{label}: cap failure lacks a route at its cap")
    if cap_failures == 0 and any(
        route_max[route] == int(cap)
        for route, cap in CONTRACT["route_array_item_caps"].items()
    ):
        raise ValueError(f"{label}: route reached its cap without a cap failure")
    if repetition_groups < repetition_failures:
        raise ValueError(f"{label}: repetition-group counts do not reconcile")
    if (repetition_groups > 0) != (
        max_multiplicity >= MODEL_ITEM_REPETITION_THRESHOLD
    ):
        raise ValueError(f"{label}: repetition multiplicity does not reconcile")
    if strict_model_items == 0:
        if any(route_max.values()) or max_multiplicity:
            raise ValueError(f"{label}: empty parsed items have nonzero maxima")
    else:
        if max(route_max.values()) > strict_model_items:
            raise ValueError(f"{label}: route model-item maximum does not reconcile")

    canonicalized_outputs = _nonnegative_integer(
        row.get("json_control_canonicalized_outputs_this_run"),
        f"{label}.json_control_canonicalized_outputs_this_run",
    )
    canonicalized_characters = _nonnegative_integer(
        row.get("json_control_canonicalized_characters_this_run"),
        f"{label}.json_control_canonicalized_characters_this_run",
    )
    canonicalized_codepoints = _count_mapping(
        row.get("json_control_canonicalization_codepoint_counts_this_run"),
        f"{label}.json_control_canonicalization_codepoint_counts_this_run",
    )
    for codepoint, count in canonicalized_codepoints.items():
        if (
            not re.fullmatch(r"U\+[0-9A-F]{4}", codepoint)
            or int(codepoint[2:], 16) >= 0x20
            or count <= 0
        ):
            raise ValueError(
                f"{label}: JSON-control code-point accounting differs"
            )
    if (
        canonicalized_outputs > processed
        or canonicalized_characters != sum(canonicalized_codepoints.values())
        or (canonicalized_outputs == 0) != (canonicalized_characters == 0)
        or canonicalized_characters < canonicalized_outputs
    ):
        raise ValueError(f"{label}: JSON-control aggregates do not reconcile")

    recovery = row.get("recovery_attestation_without_inference")
    if not isinstance(recovery, bool):
        raise ValueError(f"{label}: recovery attestation flag is absent")
    if recovery and any(
        value
        for value in (
            processed, parsed, schema_valid, exact, total_tokens, max_tokens,
            model_items, aligned_items, withheld_items,
            strict_parsed, strict_model_items, cap_failures,
            repetition_failures, repetition_groups, max_multiplicity,
            canonicalized_outputs, canonicalized_characters,
            *finish_counts.values(), *alignment_counts.values(),
            *route_max.values(),
            *canonicalized_codepoints.values(),
        )
    ):
        raise ValueError(f"{label}: recovery attestation reports new inference")
    if not recovery and processed == 0:
        raise ValueError(
            f"{label}: zero processed outputs require a recovery attestation"
        )


PRIMARY_EQUIVALENT_AGGREGATE_VERSION = (
    "candidate_extraction_primary_equivalent_output_aggregate_v1"
)
_AGGREGATE_COUNTER_FIELDS = (
    "parse_status_counts",
    "processing_status_counts",
    "finish_reason_counts",
    "json_control_canonicalization_codepoint_counts",
)
_AGGREGATE_SUM_FIELDS = (
    "input_truncated_segments",
    "schema_valid_segments",
    "exactly_supported_segments",
    "output_tokens_total",
    "model_items",
    "aligned_items",
    "withheld_items",
    "strict_json_parsed_outputs",
    "model_items_strictly_parsed",
    "array_item_cap_reached_outputs",
    "model_item_repetition_error_outputs",
    "model_item_repetition_groups",
    "json_control_canonicalized_outputs",
    "json_control_canonicalized_characters",
)
_AGGREGATE_MAX_FIELDS = ("output_tokens_max", "max_exact_model_item_multiplicity")


def primary_equivalent_output_aggregate(
    corpus: Mapping[str, Any],
    superseded_rows: Sequence[Mapping[str, Any]],
    replacement_rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Return the corpus aggregate as the primary run alone would have produced it.

    A repaired window contributes its repair row to the bundle and its primary
    row to the primary run's metadata. The primary metadata therefore counts an
    outcome the bundle no longer shows, and the bound check reads that as an
    unaccounted claim. Swapping each repair row back for the primary row it
    replaced restores the comparison the check was written for. It does not
    loosen the check: the row count is unchanged, every other window is
    untouched, and a maximum is taken over both sides, which is exact because
    the rows removed are exactly the rows added back.
    """
    if len(superseded_rows) != len(replacement_rows):
        raise ValueError(
            "superseded and replacement row counts differ, so the primary "
            "equivalent aggregate cannot be exact"
        )
    if not superseded_rows:
        return dict(corpus)
    removed = all_output_aggregate(replacement_rows)
    added = all_output_aggregate(superseded_rows)
    result = dict(corpus)
    # The shape is the all-output shape, and the version names the shape, so it
    # stays. A separate key records how this copy was derived.
    result["derivation"] = PRIMARY_EQUIVALENT_AGGREGATE_VERSION
    result["superseded_repair_rows"] = len(superseded_rows)
    for field in _AGGREGATE_COUNTER_FIELDS:
        counts = Counter(_count_mapping(corpus.get(field), field))
        counts.update(_count_mapping(added.get(field), field))
        counts.subtract(_count_mapping(removed.get(field), field))
        if any(value < 0 for value in counts.values()):
            raise ValueError(f"primary equivalent aggregate went negative in {field}")
        result[field] = dict(sorted(
            (key, value) for key, value in counts.items() if value
        ))
    for field in _AGGREGATE_SUM_FIELDS:
        value = (
            int(corpus.get(field, 0)) + int(added.get(field, 0))
            - int(removed.get(field, 0))
        )
        if value < 0:
            raise ValueError(f"primary equivalent aggregate went negative in {field}")
        result[field] = value
    for field in _AGGREGATE_MAX_FIELDS:
        result[field] = max(int(corpus.get(field, 0)), int(added.get(field, 0)))
    routes = dict(corpus.get("route_model_item_count_max") or {})
    for route, value in (added.get("route_model_item_count_max") or {}).items():
        routes[route] = max(int(routes.get(route, 0)), int(value))
    result["route_model_item_count_max"] = dict(sorted(routes.items()))
    return result


def validate_run_metadata_against_all_output(
    metadata: Mapping[str, Any],
    all_output: Mapping[str, Any],
    label: str = "run metadata aggregate",
) -> None:
    """Bound attempt-local counters by the independently replayed output ledger.

    A restarted shard may report only the work performed in its latest attempt,
    while its selected output rows also include earlier atomic checkpoints. The
    selected-document total must therefore equal row coverage, but inference
    counters and maxima may be smaller and can never exceed the all-row ledger.
    """
    if metadata.get("version") != "candidate_extraction_run_metadata_aggregate_v2":
        raise ValueError(f"{label}: run-metadata aggregate version differs")
    if all_output.get("version") != "candidate_extraction_all_output_aggregate_v2":
        raise ValueError(f"{label}: all-output aggregate version differs")
    metadata_files = _nonnegative_integer(
        metadata.get("metadata_files"), f"{label}.metadata_files"
    )
    recovery_files = _nonnegative_integer(
        metadata.get("recovery_attestation_files"),
        f"{label}.recovery_attestation_files",
    )
    if metadata_files < 1 or recovery_files > metadata_files:
        raise ValueError(f"{label}: metadata-file accounting differs")

    selected = _nonnegative_integer(
        metadata.get("documents_selected_for_shard"),
        f"{label}.documents_selected_for_shard",
    )
    segments = _nonnegative_integer(
        all_output.get("segments"), f"{label}.all_output.segments"
    )
    if selected != segments:
        raise ValueError(
            f"{label}: selected-document coverage differs from all output rows"
        )

    parse_statuses = _count_mapping(
        all_output.get("parse_status_counts"),
        f"{label}.all_output.parse_status_counts",
    )
    scalar_bounds = {
        "documents_processed_this_run": segments,
        "documents_parsed_this_run": parse_statuses.get("parsed", 0),
        "documents_schema_valid_this_run": _nonnegative_integer(
            all_output.get("schema_valid_segments"),
            f"{label}.all_output.schema_valid_segments",
        ),
        "documents_exactly_supported_this_run": _nonnegative_integer(
            all_output.get("exactly_supported_segments"),
            f"{label}.all_output.exactly_supported_segments",
        ),
        "output_tokens_total_this_run": _nonnegative_integer(
            all_output.get("output_tokens_total"),
            f"{label}.all_output.output_tokens_total",
        ),
        "output_tokens_max_this_run": _nonnegative_integer(
            all_output.get("output_tokens_max"),
            f"{label}.all_output.output_tokens_max",
        ),
        "model_items_parsed_this_run": _nonnegative_integer(
            all_output.get("model_items"), f"{label}.all_output.model_items"
        ),
        "aligned_items_this_run": _nonnegative_integer(
            all_output.get("aligned_items"), f"{label}.all_output.aligned_items"
        ),
        "withheld_items_this_run": _nonnegative_integer(
            all_output.get("withheld_items"),
            f"{label}.all_output.withheld_items",
        ),
        "strict_json_parsed_outputs_this_run": _nonnegative_integer(
            all_output.get("strict_json_parsed_outputs"),
            f"{label}.all_output.strict_json_parsed_outputs",
        ),
        "model_items_strictly_parsed_this_run": _nonnegative_integer(
            all_output.get("model_items_strictly_parsed"),
            f"{label}.all_output.model_items_strictly_parsed",
        ),
        "array_item_cap_reached_outputs_this_run": _nonnegative_integer(
            all_output.get("array_item_cap_reached_outputs"),
            f"{label}.all_output.array_item_cap_reached_outputs",
        ),
        "model_item_repetition_error_outputs_this_run": _nonnegative_integer(
            all_output.get("model_item_repetition_error_outputs"),
            f"{label}.all_output.model_item_repetition_error_outputs",
        ),
        "model_item_repetition_groups_this_run": _nonnegative_integer(
            all_output.get("model_item_repetition_groups"),
            f"{label}.all_output.model_item_repetition_groups",
        ),
        "max_exact_model_item_multiplicity_this_run": _nonnegative_integer(
            all_output.get("max_exact_model_item_multiplicity"),
            f"{label}.all_output.max_exact_model_item_multiplicity",
        ),
        "json_control_canonicalized_outputs_this_run": _nonnegative_integer(
            all_output.get("json_control_canonicalized_outputs"),
            f"{label}.all_output.json_control_canonicalized_outputs",
        ),
        "json_control_canonicalized_characters_this_run": _nonnegative_integer(
            all_output.get("json_control_canonicalized_characters"),
            f"{label}.all_output.json_control_canonicalized_characters",
        ),
    }
    for field, maximum in scalar_bounds.items():
        observed = _nonnegative_integer(metadata.get(field), f"{label}.{field}")
        if observed > maximum:
            raise ValueError(f"{label}: {field} exceeds the all-output ledger")

    mapping_bounds = {
        "finish_reason_counts_this_run": "finish_reason_counts",
        "alignment_status_counts_this_run": "alignment_status_counts",
        "json_control_canonicalization_codepoint_counts_this_run": (
            "json_control_canonicalization_codepoint_counts"
        ),
        "route_model_item_count_max_this_run": "route_model_item_count_max",
    }
    for metadata_field, output_field in mapping_bounds.items():
        observed = _count_mapping(metadata.get(metadata_field), f"{label}.{metadata_field}")
        available = _count_mapping(
            all_output.get(output_field), f"{label}.all_output.{output_field}"
        )
        if metadata_field == "route_model_item_count_max_this_run" and (
            set(observed) != set(CONTRACT["route_array_item_caps"])
            or set(available) != set(CONTRACT["route_array_item_caps"])
        ):
            raise ValueError(f"{label}: route maximum keys differ")
        if (
            metadata_field != "route_model_item_count_max_this_run"
            and not set(observed) <= set(available)
        ):
            raise ValueError(
                f"{label}: {metadata_field} contains a category absent from "
                "the all-output ledger"
            )
        if any(count > available.get(key, 0) for key, count in observed.items()):
            raise ValueError(
                f"{label}: {metadata_field} exceeds the all-output ledger"
            )


def read_jsonl_gz(path: Path) -> Iterator[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def write_jsonl_gz(path: Path, rows: Iterable[dict[str, Any]]) -> int:
    temp = path.with_suffix(path.suffix + f".tmp-{os.getpid()}")
    count = 0
    with temp.open("wb") as raw:
        with gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) as compressed:
            for row in rows:
                compressed.write(
                    (json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n").encode("utf-8")
                )
                count += 1
    os.replace(temp, path)
    return count


def _line_ledger_run_metadata_files(
    output_dir: Path, artifacts: Mapping[str, Any]
) -> list[dict[str, Any]]:
    expected = artifacts.get("run_metadata_files")
    if not isinstance(expected, list) or not expected:
        raise ValueError("line-ledger artifacts lack run metadata files")
    result: list[dict[str, Any]] = []
    for entry in expected:
        if not isinstance(entry, Mapping) or set(entry) != {"file", "sha256"}:
            raise ValueError("line-ledger run metadata inventory differs")
        name = entry.get("file")
        if not isinstance(name, str) or Path(name).name != name:
            raise ValueError("line-ledger run metadata filename is invalid")
        path = output_dir / name
        text = path.read_text(encoding="utf-8")
        if sha256_text(text) != entry.get("sha256"):
            raise ValueError(f"line-ledger run metadata hash differs: {name}")
        result.append({"file": name, "sha256": entry["sha256"], "text": text})
    return result


def _line_ledger_selection_csv_text(
    rows: list[Mapping[str, Any]],
) -> str:
    if not rows:
        raise ValueError("line-ledger selection cannot be empty")
    fieldnames = list(rows[0])
    if any(list(row) != fieldnames for row in rows):
        raise ValueError("line-ledger selection field order differs")
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(
        buffer, fieldnames=fieldnames, lineterminator="\n"
    )
    writer.writeheader()
    writer.writerows(rows)
    return buffer.getvalue()


def _line_ledger_selection_text(
    export_dir: Path, manifest: Mapping[str, Any]
) -> str:
    files = manifest.get("files")
    selection = files.get("selection") if isinstance(files, Mapping) else None
    name = selection.get("file") if isinstance(selection, Mapping) else None
    if not isinstance(name, str) or Path(name).name != name:
        raise ValueError("line-ledger selection filename is invalid")
    path = export_dir / name
    text = path.read_text(encoding="utf-8")
    encoded = text.encode("utf-8")
    if (
        type(selection.get("bytes")) is not int
        or selection["bytes"] != len(encoded)
        or selection.get("sha256") != hashlib.sha256(encoded).hexdigest()
    ):
        raise ValueError("line-ledger selection bytes differ from manifest")
    return text


def _canonical_jsonl_attestation(
    file_name: str,
    rows: list[Mapping[str, Any]],
    *,
    transport_sha256: str,
    transport_bytes: int,
    compressed_bytes: bytes,
) -> dict[str, Any]:
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
        "content_attestation_version": (
            LINE_LEDGER_CHUNK_CONTENT_ATTESTATION_VERSION
        ),
        "canonical_jsonl_sha256": hashlib.sha256(content).hexdigest(),
        "canonical_jsonl_bytes": len(content),
    }


def _replay_compressed_chunk_attestation(
    observed: Any,
    expected_rows: list[Mapping[str, Any]],
    *,
    file_name: str,
    transport_sha256: str,
    transport_bytes: int,
) -> dict[str, Any]:
    if not isinstance(observed, Mapping) or set(observed) != {
        "file", "records", "transport_sha256", "transport_bytes",
        "compressed_bytes_base64", "content_attestation_version",
        "canonical_jsonl_sha256", "canonical_jsonl_bytes",
    }:
        raise ValueError("line-ledger compressed chunk attestation fields differ")
    encoded = observed.get("compressed_bytes_base64")
    if not isinstance(encoded, str):
        raise ValueError("line-ledger compressed chunk bytes are absent")
    try:
        compressed = base64.b64decode(encoded, validate=True)
        raw_text = gzip.decompress(compressed).decode("utf-8")
        decoded_rows = [
            json.loads(line) for line in raw_text.splitlines() if line.strip()
        ]
    except (ValueError, OSError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise ValueError(
            "line-ledger compressed chunk replay failed"
        ) from exc
    if (
        len(compressed) != transport_bytes
        or hashlib.sha256(compressed).hexdigest() != transport_sha256
    ):
        raise ValueError("line-ledger compressed chunk attestation differs")
    if not _typed_equal(decoded_rows, expected_rows):
        raise ValueError("line-ledger compressed chunk rows differ")
    expected = _canonical_jsonl_attestation(
        file_name,
        expected_rows,
        transport_sha256=transport_sha256,
        transport_bytes=transport_bytes,
        compressed_bytes=compressed,
    )
    if not _typed_equal(observed, expected):
        raise ValueError("line-ledger compressed chunk attestation differs")
    return expected


def _attest_line_ledger_transport_files(
    manifest: Mapping[str, Any],
    artifacts: Mapping[str, Any],
    export_dir: Path,
    output_dir: Path,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    files = manifest.get("files")
    chunks = files.get("chunks") if isinstance(files, Mapping) else None
    output_files = artifacts.get("output_files")
    if not isinstance(chunks, list) or not isinstance(output_files, list):
        raise ValueError("line-ledger transport inventory is absent")
    input_attestations: list[dict[str, Any]] = []
    output_attestations: list[dict[str, Any]] = []
    for chunk, output_entry in zip(chunks, output_files, strict=True):
        if not isinstance(chunk, Mapping) or not isinstance(
            output_entry, Mapping
        ):
            raise ValueError("line-ledger transport entry differs")
        input_name = chunk.get("file")
        output_name = output_entry.get("file")
        if (
            not isinstance(input_name, str)
            or Path(input_name).name != input_name
            or not isinstance(output_name, str)
            or Path(output_name).name != output_name
        ):
            raise ValueError("line-ledger transport filename differs")
        input_path = export_dir / "chunks" / input_name
        output_path = output_dir / output_name
        for path, entry, label in (
            (input_path, chunk, "input"),
            (output_path, output_entry, "output"),
        ):
            actual_bytes = path.stat().st_size
            actual_sha256 = sha256_file(path)
            if (
                type(entry.get("bytes")) is not int
                or entry["bytes"] != actual_bytes
                or entry.get("sha256") != actual_sha256
            ):
                raise ValueError(
                    f"line-ledger {label} compressed bytes differ"
                )
        input_compressed = input_path.read_bytes()
        output_compressed = output_path.read_bytes()
        input_rows = list(read_jsonl_gz(input_path))
        output_rows = list(read_jsonl_gz(output_path))
        input_attestations.append(_canonical_jsonl_attestation(
            input_name,
            input_rows,
            transport_sha256=str(chunk["sha256"]),
            transport_bytes=int(chunk["bytes"]),
            compressed_bytes=input_compressed,
        ))
        output_attestations.append(_canonical_jsonl_attestation(
            output_name,
            output_rows,
            transport_sha256=str(output_entry["sha256"]),
            transport_bytes=int(output_entry["bytes"]),
            compressed_bytes=output_compressed,
        ))
    return input_attestations, output_attestations


def _historical_stage_a_artifacts(
    manifest: Mapping[str, Any],
    outputs: list[Mapping[str, Any]],
    stage_a_provenance: Mapping[str, Any],
    output_dir: Path,
) -> dict[str, Any]:
    metadata_path = output_dir / "run_metadata.json"
    metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    if not isinstance(metadata, dict):
        raise ValueError("historical Stage-A metadata is not an object")
    files = manifest.get("files")
    chunks = files.get("chunks") if isinstance(files, Mapping) else None
    output_files = stage_a_provenance.get("stage_a_output_files")
    if not isinstance(chunks, list) or not isinstance(output_files, list):
        raise ValueError("historical Stage-A file inventory is incomplete")
    return {
        "input_manifest_sha256": stage_a_provenance[
            "stage_a_input_manifest_sha256"
        ],
        "generation_config_sha256": stage_a_provenance[
            "stage_a_generation_config_sha256"
        ],
        "max_model_len": metadata["max_model_len"],
        "max_output_tokens": metadata["max_output_tokens"],
        "input_files": [
            {"file": str(chunk["file"]), "sha256": str(chunk["sha256"])}
            for chunk in chunks
        ],
        "output_files": [
            {
                "file": str(entry["file"]),
                "sha256": str(entry["sha256"]),
                "bytes": int(entry["bytes"]),
                "records": int(entry["records"]),
            }
            for entry in output_files
        ],
        "output_batch_ids_sha256": line_ledger_sha256_json([
            str(output["batch_id"]) for output in outputs
        ]),
        "validated_output_batches": len(outputs),
        "run_metadata_files": [{
            "file": metadata_path.name,
            "sha256": sha256_file(metadata_path),
        }],
        "typed_restart_replay_complete": True,
    }


def _line_ledger_stage_provenance_row(
    stage: str,
    manifest: Mapping[str, Any],
    artifacts: Mapping[str, Any],
    export_dir: Path,
    output_dir: Path,
    *,
    upstream_stage_a_provenance: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    manifest_text = (export_dir / "manifest.json").read_text(encoding="utf-8")
    if sha256_text(manifest_text) != artifacts.get("input_manifest_sha256"):
        raise ValueError(f"{stage} input-manifest bytes differ from replay")
    if not _typed_equal(json.loads(manifest_text), manifest):
        raise ValueError(f"{stage} input-manifest object differs from replay")
    input_attestations, output_attestations = (
        _attest_line_ledger_transport_files(
            manifest, artifacts, export_dir, output_dir
        )
    )
    return {
        "kind": "stage_provenance",
        "stage": stage,
        "input_manifest_text": manifest_text,
        "selection_text": _line_ledger_selection_text(export_dir, manifest),
        "upstream_stage_a_provenance": (
            None
            if upstream_stage_a_provenance is None
            else dict(upstream_stage_a_provenance)
        ),
        "input_chunk_attestations": input_attestations,
        "output_chunk_attestations": output_attestations,
        "artifacts": dict(artifacts),
        "run_metadata_files": _line_ledger_run_metadata_files(
            output_dir, artifacts
        ),
    }


def _validate_line_ledger_stage_provenance(
    row: Mapping[str, Any],
    expected_stage: str,
    expected_parents: list[Mapping[str, Any]],
    expected_batches: list[Mapping[str, Any]],
    expected_outputs: list[Mapping[str, Any]],
    *,
    contract: Mapping[str, Any],
    expected_mode: str,
    expected_bounded_preflight: Mapping[str, Any],
    expected_chunk_size: int,
    stage_a_decisions: Mapping[str, Mapping[str, Any]] | None = None,
    expected_manifest_extras: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    if set(row) != {
        "kind", "stage", "input_manifest_text", "selection_text",
        "upstream_stage_a_provenance",
        "input_chunk_attestations", "output_chunk_attestations",
        "artifacts", "run_metadata_files",
    } or row.get("kind") != "stage_provenance" or row.get("stage") != expected_stage:
        raise ValueError(f"{expected_stage} private provenance fields differ")
    manifest_text = row.get("input_manifest_text")
    if not isinstance(manifest_text, str):
        raise ValueError(f"{expected_stage} input-manifest bytes are absent")
    manifest = json.loads(manifest_text)
    if not isinstance(manifest, dict):
        raise ValueError(f"{expected_stage} input manifest is not an object")
    expected_manifest_fields = set(LINE_LEDGER_MANIFEST_BASE_FIELDS)
    expected_manifest_fields.update((expected_manifest_extras or {}).keys())
    if set(manifest) != expected_manifest_fields:
        raise ValueError(f"{expected_stage} exporter manifest fields differ")
    payload = dict(manifest)
    recorded_payload_hash = payload.pop("manifest_payload_sha256", None)
    if line_ledger_sha256_json(payload) != recorded_payload_hash:
        raise ValueError(f"{expected_stage} manifest payload hash differs")
    artifacts = row.get("artifacts")
    if not isinstance(artifacts, Mapping) or set(artifacts) != (
        LINE_LEDGER_ARTIFACT_FIELDS
    ):
        raise ValueError(f"{expected_stage} artifact fields differ")
    input_manifest_sha256 = sha256_text(manifest_text)
    if artifacts.get("input_manifest_sha256") != input_manifest_sha256:
        raise ValueError(f"{expected_stage} input-manifest attestation differs")
    expected_parent_ids = [
        str(parent["parent_document_id"]) for parent in expected_parents
    ]
    expected_source_export = {
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
    }
    historical_stage_a = expected_stage == LINE_DECISION_STAGE
    expected_frozen_contract = (
        FROZEN_STAGE_A_V2_CONTRACT_SHA256
        if historical_stage_a
        else (FROZEN_LINE_LEDGER_CONTRACT_SHA256 or None)
    )
    expected_manifest_values = {
        "builder": "scripts/build_candidate_endorsement_line_ledger.py",
        "builder_version": "candidate_endorsement_line_ledger_export_v1",
        "mode": expected_mode,
        "stage": expected_stage,
        "route": "endorsements",
        "source_export": expected_source_export,
        "line_ledger_contract": contract,
        "line_ledger_contract_sha256": contract[
            "line_ledger_contract_sha256"
        ],
        "frozen_line_ledger_contract_sha256": expected_frozen_contract,
        "contract_is_frozen": expected_frozen_contract is not None,
        "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
        "line_batch_spec_sha256": LINE_BATCH_SPEC_SHA256,
        "json_schema_catalog_sha256": contract[
            "json_schema_catalog_sha256"
        ][expected_stage],
        "json_schema_variant_sha256": contract[
            "json_schema_variant_sha256"
        ][expected_stage],
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
        "corrected_71c_issue_audit_gate": CORRECTED_ISSUE_AUDIT_GATE,
        "complete_parent_closure": True,
        "selected_parent_ids": expected_parent_ids,
        "inventory_census": inventory_census(expected_parents),
        "expected_batch_counts": expected_batch_counts(expected_parents),
        "chunk_size": expected_chunk_size,
        "bounded_preflight": expected_bounded_preflight,
    }
    expected_manifest_values.update(expected_manifest_extras or {})
    for field, expected in expected_manifest_values.items():
        if not _typed_equal(manifest.get(field), expected):
            raise ValueError(f"{expected_stage} manifest {field} differs")
    if manifest.get("selected_parent_ids_sha256") != line_ledger_sha256_json(
        expected_parent_ids
    ):
        raise ValueError(f"{expected_stage} selected-parent hash differs")
    expected_batch_ids = [str(batch["batch_id"]) for batch in expected_batches]
    if (
        not _typed_equal(manifest.get("expected_batch_ids"), expected_batch_ids)
        or
        manifest.get("expected_batch_ids_sha256")
        != line_ledger_sha256_json(expected_batch_ids)
        or type(manifest.get("expected_stage_batch_count")) is not int
        or manifest["expected_stage_batch_count"] != len(expected_batch_ids)
        or type(manifest.get("exported_records")) is not int
        or manifest["exported_records"] != len(expected_batch_ids)
    ):
        raise ValueError(f"{expected_stage} batch census differs")
    expected_variant_inventory = line_ledger_schema_variant_inventory(
        expected_batches
    )
    for field, expected in expected_variant_inventory.items():
        if not _typed_equal(manifest.get(field), expected):
            raise ValueError(
                f"{expected_stage} manifest schema-variant inventory differs"
            )
    files = manifest.get("files")
    chunks = files.get("chunks") if isinstance(files, Mapping) else None
    selection = files.get("selection") if isinstance(files, Mapping) else None
    if (
        not isinstance(files, Mapping)
        or set(files) != {"selection", "chunks"}
        or not isinstance(chunks, list)
        or not chunks
    ):
        raise ValueError(f"{expected_stage} input chunk inventory is absent")
    expected_selection_rows = line_ledger_selection_rows(
        expected_parents,
        mode=expected_mode,
        stage_a_decisions=stage_a_decisions,
    )
    expected_selection_text = _line_ledger_selection_csv_text(
        expected_selection_rows
    )
    expected_selection_bytes = expected_selection_text.encode("utf-8")
    expected_selection_spec = {
        "file": "selection.csv",
        "rows": len(expected_selection_rows),
        "columns": list(expected_selection_rows[0]),
        "bytes": len(expected_selection_bytes),
        "sha256": hashlib.sha256(expected_selection_bytes).hexdigest(),
    }
    if (
        not _typed_equal(selection, expected_selection_spec)
        or row.get("selection_text") != expected_selection_text
    ):
        raise ValueError(f"{expected_stage} selection replay differs")
    expected_chunk_parts = [
        expected_batches[offset:offset + expected_chunk_size]
        for offset in range(0, len(expected_batches), expected_chunk_size)
    ]
    if len(chunks) != len(expected_chunk_parts):
        raise ValueError(f"{expected_stage} input chunk partition differs")
    chunk_batches: list[list[Mapping[str, Any]]] = []
    batch_offset = 0
    for chunk_index, (chunk, expected_part) in enumerate(
        zip(chunks, expected_chunk_parts, strict=True)
    ):
        if (
            not isinstance(chunk, Mapping)
            or set(chunk) != {
                "file", "records", "first_document_id", "last_document_id",
                "content_sha256", "bytes", "sha256",
            }
            or type(chunk.get("records")) is not int
            or type(chunk.get("bytes")) is not int
            or chunk["bytes"] <= 0
            or not isinstance(chunk.get("file"), str)
            or chunk["file"] != f"chunk_{chunk_index:05d}.jsonl.gz"
            or not isinstance(chunk.get("sha256"), str)
            or not HEX64.fullmatch(chunk["sha256"])
        ):
            raise ValueError(f"{expected_stage} input chunk census differs")
        record_count = chunk["records"]
        part = expected_part
        if record_count != len(part) or any(
            not _typed_equal(chunk.get(field), expected)
            for field, expected in {
                "first_document_id": part[0]["document_id"],
                "last_document_id": part[-1]["document_id"],
                "content_sha256": line_ledger_sha256_json(part),
            }.items()
        ):
            raise ValueError(f"{expected_stage} input chunk content differs")
        chunk_batches.append(part)
        batch_offset += record_count
    if batch_offset != len(expected_batches):
        raise ValueError(f"{expected_stage} input chunk coverage differs")
    expected_input_files = [
        {"file": str(chunk["file"]), "sha256": str(chunk["sha256"])}
        for chunk in chunks
    ]
    if not _typed_equal(artifacts.get("input_files"), expected_input_files):
        raise ValueError(f"{expected_stage} input artifact inventory differs")
    expected_output_parts = [
        expected_outputs[offset:offset + expected_chunk_size]
        for offset in range(0, len(expected_outputs), expected_chunk_size)
    ]
    output_files = artifacts.get("output_files")
    if (
        len(expected_output_parts) != len(chunks)
        or not isinstance(output_files, list)
        or len(output_files) != len(chunks)
    ):
        raise ValueError(f"{expected_stage} output artifact inventory differs")
    input_attestations = row.get("input_chunk_attestations")
    output_attestations = row.get("output_chunk_attestations")
    if (
        not isinstance(input_attestations, list)
        or len(input_attestations) != len(chunks)
        or not isinstance(output_attestations, list)
        or len(output_attestations) != len(output_files)
    ):
        raise ValueError(f"{expected_stage} chunk attestation inventory differs")
    for observed, chunk, part in zip(
        input_attestations, chunks, expected_chunk_parts, strict=True
    ):
        _replay_compressed_chunk_attestation(
            observed,
            part,
            file_name=str(chunk["file"]),
            transport_sha256=str(chunk["sha256"]),
            transport_bytes=int(chunk["bytes"]),
        )
    for observed, output_entry, part in zip(
        output_attestations, output_files, expected_output_parts, strict=True
    ):
        _replay_compressed_chunk_attestation(
            observed,
            part,
            file_name=str(output_entry["file"]),
            transport_sha256=str(output_entry["sha256"]),
            transport_bytes=int(output_entry["bytes"]),
        )
    metadata_files = row.get("run_metadata_files")
    artifact_metadata = artifacts.get("run_metadata_files")
    if not isinstance(metadata_files, list) or not isinstance(
        artifact_metadata, list
    ) or len(metadata_files) != len(artifact_metadata):
        raise ValueError(f"{expected_stage} run metadata inventory differs")
    metadata_rows: list[dict[str, Any]] = []
    for private_entry, artifact_entry in zip(
        metadata_files, artifact_metadata, strict=True
    ):
        if not isinstance(private_entry, Mapping) or set(private_entry) != {
            "file", "sha256", "text",
        } or not isinstance(artifact_entry, Mapping) or set(artifact_entry) != {
            "file", "sha256",
        }:
            raise ValueError(f"{expected_stage} run metadata entry differs")
        if any(
            private_entry.get(field) != artifact_entry.get(field)
            for field in ("file", "sha256")
        ) or not isinstance(private_entry.get("text"), str) or sha256_text(
            private_entry["text"]
        ) != private_entry["sha256"]:
            raise ValueError(f"{expected_stage} run metadata bytes differ")
        metadata = json.loads(private_entry["text"])
        expected_metadata_fields = set(LINE_LEDGER_RUN_METADATA_FIELDS)
        if expected_stage == LINE_ADJUDICATION_STAGE:
            expected_metadata_fields.update({
                "line_adjudication_batch_schema_rule_sha256",
                "line_adjudication_spec_sha256",
                "upstream_stage_a_contract_sha256",
                "selected_stage_a_provenance_sha256",
            })
        if expected_stage == RECORD_EXTRACTION_STAGE:
            expected_metadata_fields.add(
                "stage_b_two_pass_provenance_spec_sha256"
            )
        if not isinstance(metadata, dict) or set(metadata) != (
            expected_metadata_fields
        ):
            raise ValueError(f"{expected_stage} run metadata fields differ")
        generation = metadata.get("generation_config")
        generation_hash = metadata.get("generation_config_sha256")
        if not isinstance(generation, Mapping) or (
            line_ledger_sha256_json(generation) != generation_hash
        ):
            raise ValueError(f"{expected_stage} generation payload hash differs")
        if (
            not historical_stage_a
            and generation.get("expected_input_manifest_sha256")
            != input_manifest_sha256
        ):
            raise ValueError(
                f"{expected_stage} generation input-manifest binding differs"
            )
        expected_metadata = {
            "run_metadata_version": (
                HISTORICAL_STAGE_A_RUN_METADATA_VERSION
                if historical_stage_a
                else LINE_LEDGER_RUN_METADATA_VERSION
            ),
            "accounting_scope": (
                "outputs_inferred_during_this_process_attempt"
            ),
            "stage": expected_stage,
            "input_manifest_sha256": input_manifest_sha256,
            "line_ledger_contract_sha256": contract[
                "line_ledger_contract_sha256"
            ],
            "prompt_sha256": contract["prompt_sha256"][expected_stage],
            "json_schema_catalog_sha256": contract[
                "json_schema_catalog_sha256"
            ][expected_stage],
            "json_schema_variant_sha256": contract[
                "json_schema_variant_sha256"
            ][expected_stage],
            "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
            "line_batch_spec_sha256": LINE_BATCH_SPEC_SHA256,
            "model_id": contract["model_id"],
            "model_revision": contract["model_revision"],
            "runtime_lock_sha256": contract["runtime_lock_sha256"],
            "structured_decoding_backend": STRUCTURED_DECODING_BACKEND,
            "structured_decoding_disable_any_whitespace": (
                STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
            ),
            "prompt_token_count_spec_sha256": PROMPT_TOKEN_COUNT_SPEC_SHA256,
        }
        if expected_stage == LINE_ADJUDICATION_STAGE:
            expected_metadata.update({
                "line_adjudication_batch_schema_rule_sha256": (
                    LINE_ADJUDICATION_BATCH_SCHEMA_RULE_SHA256
                ),
                "line_adjudication_spec_sha256": (
                    LINE_ADJUDICATION_SPEC_SHA256
                ),
                "upstream_stage_a_contract_sha256": (
                    FROZEN_STAGE_A_V2_CONTRACT_SHA256
                ),
            })
        if expected_stage == RECORD_EXTRACTION_STAGE:
            expected_metadata[
                "stage_b_two_pass_provenance_spec_sha256"
            ] = STAGE_B_TWO_PASS_PROVENANCE_SPEC_SHA256
        for field, expected in expected_metadata.items():
            if not _typed_equal(metadata.get(field), expected):
                raise ValueError(
                    f"{expected_stage} run metadata {field} differs"
                )
        expected_generation = line_ledger_generation_config_payload(
            argparse.Namespace(
                stage=expected_stage,
                seed=LINE_LEDGER_GENERATION_SEED,
                max_model_len=metadata.get("max_model_len"),
                max_output_tokens=metadata.get("max_output_tokens"),
            ),
            contract,
            input_manifest_sha256=input_manifest_sha256,
        )
        if historical_stage_a:
            expected_generation["generation_config_version"] = (
                HISTORICAL_STAGE_A_GENERATION_CONFIG_VERSION
            )
            expected_generation.pop("expected_input_manifest_sha256", None)
        if (
            type(metadata.get("max_model_len")) is not int
            or metadata["max_model_len"] != LINE_LEDGER_MAX_MODEL_LEN
            or type(metadata.get("max_output_tokens")) is not int
            or metadata["max_output_tokens"]
            != LINE_LEDGER_MAX_OUTPUT_TOKENS[expected_stage]
        ):
            raise ValueError(
                f"{expected_stage} run metadata token budgets differ"
            )
        if not _typed_equal(generation, expected_generation):
            raise ValueError(f"{expected_stage} generation payload differs")
        shard_index = metadata.get("shard_index")
        shard_count = metadata.get("shard_count")
        if shard_index is None and shard_count is None:
            selected_batches = expected_batches
        elif (
            type(shard_index) is int
            and type(shard_count) is int
            and 0 <= shard_index < shard_count
        ):
            selected_batches = [
                batch
                for index, part in enumerate(chunk_batches)
                if index % shard_count == shard_index
                for batch in part
            ]
        else:
            raise ValueError(f"{expected_stage} run shard identity differs")
        expected_metadata_name = (
            "run_metadata.json" if shard_index is None
            else f"run_metadata_shard_{shard_index:05d}.json"
        )
        if private_entry.get("file") != expected_metadata_name:
            raise ValueError(f"{expected_stage} run metadata filename differs")
        if (
            type(metadata.get("selected_batches")) is not int
            or metadata["selected_batches"] != len(selected_batches)
        ):
            raise ValueError(
                f"{expected_stage} run metadata selected-batch count differs"
            )
        selected_inventory = line_ledger_schema_variant_inventory(
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
            if not _typed_equal(
                metadata.get(metadata_field),
                selected_inventory[inventory_field],
            ):
                raise ValueError(
                    f"{expected_stage} run metadata schema variants differ"
                )
        if expected_stage == LINE_ADJUDICATION_STAGE:
            expected_stage_a_digests = sorted({
                str(batch.get("stage_a_provenance_sha256"))
                for batch in selected_batches
            })
            if not _typed_equal(
                metadata.get("selected_stage_a_provenance_sha256"),
                expected_stage_a_digests,
            ):
                raise ValueError(
                    f"{expected_stage} selected Stage-A provenance differs"
                )
        recovery = metadata.get("recovery_attestation")
        if type(recovery) is not bool:
            raise ValueError(
                f"{expected_stage} recovery attestation type differs"
            )
        selected_ids = {str(batch["batch_id"]) for batch in selected_batches}
        selected_outputs = [
            output for output in expected_outputs
            if str(output.get("batch_id")) in selected_ids
        ]
        if len(selected_outputs) != len(selected_batches):
            raise ValueError(
                f"{expected_stage} output rows do not cover metadata shard"
            )
        if recovery:
            attempt_outputs: list[Mapping[str, Any]] = []
        else:
            if metadata.get("processed_outputs_this_run") != len(
                selected_batches
            ):
                raise ValueError(
                    f"{expected_stage} partial restart accounting cannot be "
                    "authenticated"
                )
            attempt_outputs = selected_outputs
        for output in attempt_outputs:
            for field in (
                "output_tokens", "prompt_tokens_used", "context_margin_tokens"
            ):
                if type(output.get(field)) is not int:
                    raise ValueError(
                        f"{expected_stage} output accounting type differs"
                    )
        attempt_variant_counts = dict(sorted(Counter(
            str(output.get("json_schema_variant_id"))
            for output in attempt_outputs
        ).items()))
        attempt_parse_counts = dict(sorted(Counter(
            str(output.get("parse_status")) for output in attempt_outputs
        ).items()))
        attempt_finish_counts = dict(sorted(Counter(
            str(output.get("finish_reason")) for output in attempt_outputs
        ).items()))
        expected_attempt_accounting = {
            "processed_outputs_this_run": len(attempt_outputs),
            "processed_schema_variant_counts_this_run": (
                attempt_variant_counts
            ),
            "technical_pass_outputs_this_run": sum(
                output.get("technical_pass") is True
                for output in attempt_outputs
            ),
            "parse_status_counts_this_run": attempt_parse_counts,
            "finish_reason_counts_this_run": attempt_finish_counts,
            "output_tokens_total_this_run": sum(
                output["output_tokens"] for output in attempt_outputs
            ),
            "output_tokens_max_this_run": max(
                (output["output_tokens"] for output in attempt_outputs),
                default=0,
            ),
            "prompt_tokens_max_this_run": max(
                (output["prompt_tokens_used"] for output in attempt_outputs),
                default=0,
            ),
            "minimum_context_margin_tokens_this_run": min(
                (
                    output["context_margin_tokens"]
                    for output in attempt_outputs
                ),
                default=0,
            ),
        }
        for field, expected in expected_attempt_accounting.items():
            if not _typed_equal(metadata.get(field), expected):
                raise ValueError(
                    f"{expected_stage} run metadata {field} differs"
                )
        for field in ("started_at", "completed_at", "host"):
            if not isinstance(metadata.get(field), str) or not metadata[field]:
                raise ValueError(
                    f"{expected_stage} run metadata {field} is invalid"
                )
        for field in ("slurm_job_id", "slurm_array_task_id"):
            if metadata.get(field) is not None and not isinstance(
                metadata.get(field), str
            ):
                raise ValueError(
                    f"{expected_stage} run metadata {field} is invalid"
                )
        metadata_rows.append(metadata)
    shard_pairs = [
        (metadata.get("shard_index"), metadata.get("shard_count"))
        for metadata in metadata_rows
    ]
    if shard_pairs != [(None, None)]:
        shard_counts = {pair[1] for pair in shard_pairs}
        if len(shard_counts) != 1:
            raise ValueError(f"{expected_stage} run shard counts differ")
        shard_count = next(iter(shard_counts))
        if type(shard_count) is not int or sorted(
            pair[0] for pair in shard_pairs
        ) != list(range(shard_count)):
            raise ValueError(f"{expected_stage} run shard closure differs")
    generation_hashes = {
        str(value["generation_config_sha256"]) for value in metadata_rows
    }
    max_model_lengths = {value.get("max_model_len") for value in metadata_rows}
    max_output_tokens = {value.get("max_output_tokens") for value in metadata_rows}
    if not (
        len(generation_hashes) == len(max_model_lengths)
        == len(max_output_tokens) == 1
    ):
        raise ValueError(f"{expected_stage} generation identity differs by shard")
    generation_identity = {
        "generation_config_sha256": next(iter(generation_hashes)),
        "max_model_len": next(iter(max_model_lengths)),
        "max_output_tokens": next(iter(max_output_tokens)),
        "input_manifest_sha256": input_manifest_sha256,
    }
    if any(
        type(generation_identity[field]) is not int
        for field in ("max_model_len", "max_output_tokens")
    ) or generation_identity["max_model_len"] != LINE_LEDGER_MAX_MODEL_LEN or (
        generation_identity["max_output_tokens"]
        != LINE_LEDGER_MAX_OUTPUT_TOKENS[expected_stage]
    ):
        raise ValueError(f"{expected_stage} token budgets are invalid")
    expected_output_ids = [str(batch["batch_id"]) for batch in expected_batches]
    for output_entry, input_chunk in zip(output_files, chunks, strict=True):
        expected_name = str(input_chunk["file"]).replace(
            ".jsonl.gz", ".out.jsonl.gz"
        )
        if (
            not isinstance(output_entry, Mapping)
            or set(output_entry) != {"file", "sha256", "bytes", "records"}
            or output_entry.get("file") != expected_name
            or not isinstance(output_entry.get("sha256"), str)
            or not HEX64.fullmatch(output_entry["sha256"])
            or type(output_entry.get("bytes")) is not int
            or output_entry["bytes"] <= 0
            or type(output_entry.get("records")) is not int
            or output_entry["records"] != input_chunk["records"]
        ):
            raise ValueError(
                f"{expected_stage} output artifact entry differs"
            )
    if (
        artifacts.get("generation_config_sha256")
        != generation_identity["generation_config_sha256"]
        or type(artifacts.get("max_model_len")) is not int
        or artifacts["max_model_len"] != LINE_LEDGER_MAX_MODEL_LEN
        or type(artifacts.get("max_output_tokens")) is not int
        or artifacts["max_output_tokens"]
        != LINE_LEDGER_MAX_OUTPUT_TOKENS[expected_stage]
        or artifacts.get("output_batch_ids_sha256")
        != line_ledger_sha256_json(expected_output_ids)
        or type(artifacts.get("validated_output_batches")) is not int
        or artifacts["validated_output_batches"] != len(expected_batches)
        or artifacts.get("typed_restart_replay_complete") is not True
    ):
        raise ValueError(f"{expected_stage} output artifact census differs")
    return manifest, generation_identity, {
        "metadata_rows": metadata_rows,
        "artifacts": dict(artifacts),
    }


def _validate_line_ledger_output_row(
    batch: Mapping[str, Any],
    output: Mapping[str, Any],
    stage: str,
    generation: Mapping[str, Any],
    *,
    contract: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    schema_binding = schema_binding_for_batch(batch)
    for field in (
        "json_schema_catalog_sha256",
        "json_schema_variant_id",
        "json_schema_sha256",
        "target_slot_count",
    ):
        if not _typed_equal(batch.get(field), schema_binding[field]):
            raise ValueError(f"{stage} input batch {field} differs")
    expected_identity = {
        "document_id": batch.get("document_id"),
        "batch_id": batch.get("batch_id"),
        "stage": stage,
        "route": "endorsements",
        "parent_document_id": batch.get("parent_document_id"),
        "candidate_cycle_id": batch.get("candidate_cycle_id"),
        "input_batch_sha256": line_ledger_sha256_json(batch),
        "input_manifest_sha256": generation["input_manifest_sha256"],
        "model_id": contract["model_id"],
        "model_revision": contract["model_revision"],
        "runtime_lock_sha256": contract["runtime_lock_sha256"],
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
        "line_batch_spec_sha256": LINE_BATCH_SPEC_SHA256,
        "prompt_token_count_spec_sha256": PROMPT_TOKEN_COUNT_SPEC_SHA256,
        "generation_config_sha256": generation["generation_config_sha256"],
        "temperature": 0.0,
        "thinking_enabled": False,
        "structured_decoding_backend": STRUCTURED_DECODING_BACKEND,
        "structured_decoding_disable_any_whitespace": (
            STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
        ),
        "max_model_len": generation["max_model_len"],
        "max_output_tokens": generation["max_output_tokens"],
        "input_truncated": False,
        "finish_reason": "stop",
        "parse_status": "parsed",
        "parse_error": None,
        "technical_pass": True,
    }
    for field, expected in expected_identity.items():
        if not _typed_equal(output.get(field), expected):
            raise ValueError(f"{stage} output {field} differs")
    if output.get("user_message_sha256") != sha256_text(
        build_stage_user_message(batch)
    ):
        raise ValueError(f"{stage} output user-message hash differs")
    raw_output = output.get("raw_output")
    if not isinstance(raw_output, str):
        raise ValueError(f"{stage} raw output is absent")
    canonical, canonicalization = canonicalize_json_string_controls(raw_output)
    structural = validate_raw_structural_whitespace(raw_output)
    if structural.get("raw_structural_whitespace_validation_passed") is not True:
        raise ValueError(f"{stage} raw structural whitespace differs")
    for field, expected in {**canonicalization, **structural}.items():
        if not _typed_equal(output.get(field), expected):
            raise ValueError(f"{stage} output {field} differs from raw bytes")
    model_output = strict_json_loads(canonical)
    if not _typed_equal(output.get("model_output"), model_output) or (
        output.get("model_output_sha256")
        != line_ledger_sha256_json(model_output)
    ):
        raise ValueError(f"{stage} parsed model output differs")
    if stage == LINE_DECISION_STAGE:
        validation = validate_line_decision_output(batch, model_output)
        alignment = None
    elif stage == LINE_ADJUDICATION_STAGE:
        validation = validate_line_adjudication_output(batch, model_output)
        alignment = None
    elif stage == RECORD_EXTRACTION_STAGE:
        validation = validate_record_extraction_output(batch, model_output)
        alignment = align_record_extraction_output(batch, model_output)
    else:
        raise ValueError(f"unknown line-ledger output stage: {stage}")
    if (
        validation.get("schema_valid") is not True
        or validation.get("coverage_complete") is not True
        or not _typed_equal(output.get("stage_validation"), validation)
        or not _typed_equal(output.get("stage_alignment"), alignment)
    ):
        raise ValueError(f"{stage} validation or alignment differs")
    expected_fields = {
        "document_id", "batch_id", "stage", "route",
        "parent_document_id", "candidate_cycle_id", "input_batch_sha256",
        "input_manifest_sha256", "model_id", "model_revision",
        "runtime_lock_sha256", "line_ledger_contract_sha256",
        "prompt_sha256", "json_schema_catalog_sha256",
        "json_schema_variant_id", "json_schema_sha256", "target_slot_count",
        "line_inventory_spec_sha256", "line_batch_spec_sha256",
        "prompt_token_count_spec_sha256", "generation_config_sha256", "temperature",
        "thinking_enabled", "structured_decoding_backend",
        "structured_decoding_disable_any_whitespace", "max_model_len",
        "max_output_tokens", "finish_reason", "stop_reason", "output_tokens",
        "prompt_tokens_used", "context_margin_tokens", "input_truncated",
        "user_message_sha256", "raw_output", "model_output",
        "model_output_sha256", "parse_status", "parse_error",
        "stage_validation", "stage_alignment", "technical_pass",
        *canonicalization.keys(), *structural.keys(),
    }
    if stage == LINE_ADJUDICATION_STAGE:
        expected_fields.update({
            "line_adjudication_batch_schema_rule_sha256",
            "line_adjudication_spec_sha256",
            "upstream_stage_a_contract_sha256",
            "stage_a_provenance_sha256",
        })
        expected_stage_fields = {
            "line_adjudication_batch_schema_rule_sha256": (
                LINE_ADJUDICATION_BATCH_SCHEMA_RULE_SHA256
            ),
            "line_adjudication_spec_sha256": LINE_ADJUDICATION_SPEC_SHA256,
            "upstream_stage_a_contract_sha256": (
                FROZEN_STAGE_A_V2_CONTRACT_SHA256
            ),
            "stage_a_provenance_sha256": batch.get(
                "stage_a_provenance_sha256"
            ),
        }
        for field, expected in expected_stage_fields.items():
            if not _typed_equal(output.get(field), expected):
                raise ValueError(f"{stage} output {field} differs")
    if stage == RECORD_EXTRACTION_STAGE:
        # A Stage-B row states which adjudication path produced it. The
        # three-pass path includes the Stage-A3 human adjudication and is the
        # one the current contract carries; the two-pass path is historical.
        # Exactly one specification must be present and exact.
        three_pass = contract.get("stage_b_three_pass_provenance_spec_sha256")
        if "stage_b_three_pass_provenance_spec_sha256" in output:
            expected_fields.add("stage_b_three_pass_provenance_spec_sha256")
            # The three-pass batch also binds its record-extraction schema
            # rule, which the two-pass batch never carried.
            expected_fields.add("record_extraction_batch_schema_rule_sha256")
            expected_rule = contract.get(
                "record_extraction_batch_schema_rule_sha256"
            )
            if (
                three_pass is None
                or output["stage_b_three_pass_provenance_spec_sha256"]
                != three_pass
            ):
                raise ValueError(
                    f"{stage} output three-pass provenance differs"
                )
            if (
                expected_rule is None
                or output.get("record_extraction_batch_schema_rule_sha256")
                != expected_rule
            ):
                raise ValueError(
                    f"{stage} output record-extraction schema rule differs"
                )
        else:
            expected_fields.add("stage_b_two_pass_provenance_spec_sha256")
            if output.get("stage_b_two_pass_provenance_spec_sha256") != (
                STAGE_B_TWO_PASS_PROVENANCE_SPEC_SHA256
            ):
                raise ValueError(f"{stage} output two-pass provenance differs")
    if set(output) != expected_fields:
        raise ValueError(f"{stage} output fields differ")
    for field in ("output_tokens", "prompt_tokens_used", "context_margin_tokens"):
        if type(output.get(field)) is not int or output[field] < 0:
            raise ValueError(f"{stage} output {field} is invalid")
    if not 0 < output["output_tokens"] <= generation["max_output_tokens"] or (
        output["context_margin_tokens"]
        != generation["max_model_len"]
        - output["prompt_tokens_used"]
        - generation["max_output_tokens"]
    ):
        raise ValueError(f"{stage} output token accounting differs")
    return validation, alignment


def _expected_historical_stage_a_provenance(
    manifest: Mapping[str, Any],
    batches: list[Mapping[str, Any]],
    outputs: list[Mapping[str, Any]],
    decisions: Mapping[str, Mapping[str, Any]],
    replay: Mapping[str, Any],
) -> dict[str, Any]:
    metadata_rows = replay.get("metadata_rows")
    if not isinstance(metadata_rows, list) or len(metadata_rows) != 1:
        raise ValueError("historical Stage-A metadata replay differs")
    metadata = metadata_rows[0]
    files = manifest.get("files")
    chunks = files.get("chunks") if isinstance(files, Mapping) else None
    if not isinstance(chunks, list):
        raise ValueError("historical Stage-A chunks are absent")
    batch_file_provenance: dict[str, dict[str, Any]] = {}
    offset = 0
    for chunk in chunks:
        count = int(chunk["records"])
        input_rows = batches[offset:offset + count]
        output_rows = outputs[offset:offset + count]
        output_name = str(chunk["file"]).replace(
            ".jsonl.gz", ".out.jsonl.gz"
        )
        input_key = f"chunks/{chunk['file']}"
        input_entry = FROZEN_STAGE_A_SUFFIX_B_INPUT_FILES[input_key]
        output_entry = FROZEN_STAGE_A_SUFFIX_B_OUTPUT_FILES[output_name]
        for batch, output in zip(input_rows, output_rows, strict=True):
            batch_id = str(batch["batch_id"])
            batch_file_provenance[batch_id] = {
                "input_chunk_file": input_key,
                "input_chunk_sha256": input_entry["sha256"],
                "output_chunk_file": output_name,
                "output_chunk_sha256": output_entry["sha256"],
                "input_batch_sha256": line_ledger_sha256_json(batch),
                "raw_output_sha256": sha256_text(str(output["raw_output"])),
                "model_output_sha256": output["model_output_sha256"],
                "generation_config_sha256": (
                    FROZEN_STAGE_A_SUFFIX_B_GENERATION_CONFIG_SHA256
                ),
            }
        offset += count
    if offset != len(batches):
        raise ValueError("historical Stage-A chunk coverage differs")
    required = {
        "stage_a_input_manifest_sha256": (
            FROZEN_STAGE_A_SUFFIX_B_INPUT_MANIFEST_SHA256
        ),
        "stage_a_line_ledger_contract_sha256": (
            FROZEN_STAGE_A_V2_CONTRACT_SHA256
        ),
        "stage_a_generation_config_sha256": (
            FROZEN_STAGE_A_SUFFIX_B_GENERATION_CONFIG_SHA256
        ),
        "stage_a_ordered_output_batch_ids_sha256": (
            FROZEN_STAGE_A_SUFFIX_B_OUTPUT_BATCH_IDS_SHA256
        ),
        "stage_a_output_files": [
            {"file": name, **dict(entry)}
            for name, entry in FROZEN_STAGE_A_SUFFIX_B_OUTPUT_FILES.items()
            if name.endswith(".out.jsonl.gz")
        ],
        "by_batch_id": batch_file_provenance,
    }
    payload = {
        **required,
        "upstream_stage": LINE_DECISION_STAGE,
        "upstream_stage_a_contract_sha256": (
            FROZEN_STAGE_A_V2_CONTRACT_SHA256
        ),
        "input_export_manifest_sha256": (
            FROZEN_STAGE_A_SUFFIX_B_INPUT_MANIFEST_SHA256
        ),
        "input_files": [
            {"file": name, **dict(entry)}
            for name, entry in sorted(
                FROZEN_STAGE_A_SUFFIX_B_INPUT_FILES.items()
            )
        ],
        "run_metadata_files": [{
            "file": "run_metadata.json",
            **dict(FROZEN_STAGE_A_SUFFIX_B_OUTPUT_FILES[
                "run_metadata.json"
            ]),
        }],
        "generation_config": metadata["generation_config"],
        "generation_config_sha256": (
            FROZEN_STAGE_A_SUFFIX_B_GENERATION_CONFIG_SHA256
        ),
        "output_files": [
            {"file": name, **dict(entry)}
            for name, entry in sorted(
                FROZEN_STAGE_A_SUFFIX_B_OUTPUT_FILES.items()
            )
            if name.endswith(".out.jsonl.gz")
        ],
        "output_batch_ids_sha256": (
            FROZEN_STAGE_A_SUFFIX_B_OUTPUT_BATCH_IDS_SHA256
        ),
        "validated_output_batches": len(outputs),
        "source_line_decision_count": len(decisions),
        "source_line_decision_ledger_sha256": (
            FROZEN_STAGE_A_SUFFIX_B_DECISION_LEDGER_SHA256
        ),
        "role_counts": dict(sorted(Counter(
            str(decision["role"]) for decision in decisions.values()
        ).items())),
        "exact_frozen_byte_inventory_validated": True,
    }
    return {
        **payload,
        "stage_a_provenance_sha256": line_ledger_sha256_json(payload),
    }


def _pilot_control_rows() -> list[dict[str, str]]:
    return [
        {"control_kind": kind, "parent_document_id": parent_id}
        for kind, parent_id in PILOT_CONTROLS
    ]


def _build_historical_stage_a_batches(
    parents: Iterable[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    """Rebind unchanged Stage-A batch bodies to the frozen v2 contract."""
    historical_contract = get_frozen_stage_a_v2_contract()
    batches = build_line_decision_batches(parents)
    rebound: list[dict[str, Any]] = []
    for batch in batches:
        value = dict(batch)
        identity = {
            "stage": LINE_DECISION_STAGE,
            "contract": historical_contract["line_ledger_contract_sha256"],
            "parent_document_id": value["parent_document_id"],
            "batch_index": value["batch_index_within_parent"],
            "source_line_ids": [
                row["source_line_id"] for row in value["targets"]
            ],
        }
        batch_id = line_ledger_sha256_json(identity)
        value["document_id"] = batch_id
        value["batch_id"] = batch_id
        rebound.append(value)
    if len({batch["batch_id"] for batch in rebound}) != len(rebound):
        raise ValueError("duplicate historical Stage-A batch IDs")
    return rebound


def _historical_stage_a_bounded_preflight() -> dict[str, Any]:
    return {
        "enabled": True,
        "pilot_controls": _pilot_control_rows(),
        "predeclared_content_gates": PILOT_CONTENT_GATES,
        "builder_performed_inference": False,
        "requires_separate_submission_approval": True,
    }


def _stage_a2_bounded_preflight() -> dict[str, Any]:
    return {
        "enabled": True,
        "pilot_controls": _pilot_control_rows(),
        "predeclared_content_gates": PILOT_CONTENT_GATES,
        "exact_stage_a_suffix_b_only": True,
        "expected_exception_count": FROZEN_STAGE_A2_EXCEPTION_COUNT,
        "expected_exception_counts": dict(FROZEN_STAGE_A2_EXCEPTION_COUNTS),
        "candidate_control_final_roles": dict(
            STAGE_A2_CANDIDATE_CONTROL_FINAL_ROLES
        ),
        "unowned_attachment_controls": dict(
            STAGE_A2_UNOWNED_ATTACHMENT_CONTROLS
        ),
        "unowned_final_role_controls": dict(
            STAGE_A2_UNOWNED_FINAL_ROLE_CONTROLS
        ),
        "builder_performed_inference": False,
        "requires_separate_submission_approval": True,
    }


def _stage_b_bounded_preflight() -> dict[str, Any]:
    return {
        "enabled": True,
        "pilot_controls": _pilot_control_rows(),
        "predeclared_content_gates": PILOT_CONTENT_GATES,
        "exact_stage_a_suffix_b_and_stage_a2_only": True,
        "stage_a2_merge_validation_passed": True,
        "builder_performed_inference": False,
        "requires_separate_submission_approval": True,
    }


def _stage_a2_exception_reason_counts(
    exceptions: Iterable[Mapping[str, Any]],
) -> dict[str, int]:
    observed = dict(sorted(Counter(
        str(row.get("exception_reason")) for row in exceptions
    ).items()))
    expected = {
        "stage_a_candidate_self_review": (
            FROZEN_STAGE_A2_EXCEPTION_COUNTS["candidate_self"]
        ),
        "stage_a_unclear": FROZEN_STAGE_A2_EXCEPTION_COUNTS["unclear"],
        "stage_a_unowned_continuation": (
            FROZEN_STAGE_A2_EXCEPTION_COUNTS["unowned_continuation"]
        ),
    }
    if not _typed_equal(observed, expected):
        raise ValueError("Stage-A2 exception-reason census differs")
    return observed


def _stage_a2_manifest_extras(
    parents: list[Mapping[str, Any]],
    stage_a_decisions: Mapping[str, Mapping[str, Any]],
    exceptions: list[Mapping[str, Any]],
    stage_a_provenance: Mapping[str, Any],
    stage_a2_batches: list[Mapping[str, Any]],
) -> dict[str, Any]:
    if len(exceptions) != FROZEN_STAGE_A2_EXCEPTION_COUNT:
        raise ValueError("Stage-A2 exception count differs")
    stage_a_identity_ledger = stage_a_decision_identity_ledger(
        parents, stage_a_decisions
    )
    if (
        len(stage_a_identity_ledger)
        != FROZEN_STAGE_A1_DECISION_IDENTITY_COUNT
        or line_ledger_sha256_json([
            row["source_line_id"] for row in stage_a_identity_ledger
        ]) != FROZEN_STAGE_A1_ORDERED_DECISION_SOURCE_LINE_IDS_SHA256
        or line_ledger_sha256_json(stage_a_identity_ledger)
        != FROZEN_STAGE_A1_DECISION_IDENTITY_LEDGER_SHA256
    ):
        raise ValueError("Stage-A frozen decision identity ledger differs")
    batched_exceptions = [
        {
            key: value for key, value in target.items()
            if key not in {"slot", "stage_a_provenance_locator"}
        }
        for batch in stage_a2_batches
        for target in batch["targets"]
    ]
    if [row["source_line_id"] for row in batched_exceptions] != [
        row["source_line_id"] for row in exceptions
    ]:
        raise ValueError("Stage-A2 compact exception order differs")
    exception_identity_ledger = line_adjudication_exception_identity_ledger(
        stage_a2_batches
    )
    if (
        line_ledger_sha256_json([
            row["source_line_id"] for row in exception_identity_ledger
        ]) != FROZEN_STAGE_A2_ORDERED_SOURCE_LINE_IDS_SHA256
        or line_ledger_sha256_json(exception_identity_ledger)
        != FROZEN_STAGE_A2_EXCEPTION_IDENTITY_LEDGER_SHA256
    ):
        raise ValueError("Stage-A2 frozen exception identity ledger differs")
    reason_counts = _stage_a2_exception_reason_counts(batched_exceptions)
    return {
        "line_adjudication_batch_schema_rule_sha256": (
            LINE_ADJUDICATION_BATCH_SCHEMA_RULE_SHA256
        ),
        "line_adjudication_spec_sha256": LINE_ADJUDICATION_SPEC_SHA256,
        "upstream_stage_a_contract_sha256": (
            FROZEN_STAGE_A_V2_CONTRACT_SHA256
        ),
        "stage_a_provenance": dict(stage_a_provenance),
        "stage_a_decision_identity_ledger": stage_a_identity_ledger,
        "stage_a_decision_identity_ledger_sha256": (
            line_ledger_sha256_json(stage_a_identity_ledger)
        ),
        "stage_a_exception_ledger_sha256": (
            line_ledger_sha256_json(batched_exceptions)
        ),
        "stage_a_exception_source_line_ids_sha256": line_ledger_sha256_json([
            str(row["source_line_id"]) for row in batched_exceptions
        ]),
        "stage_a_exception_count": len(batched_exceptions),
        "stage_a_exception_reason_counts": reason_counts,
        "stage_a_exception_identity_ledger": exception_identity_ledger,
        "stage_a_exception_identity_ledger_sha256": (
            line_ledger_sha256_json(exception_identity_ledger)
        ),
        "expected_stage_a2_batch_count": len(stage_a2_batches),
        "exception_ledger": {
            "count": len(batched_exceptions),
            "ordered_exception_ids_sha256": line_ledger_sha256_json([
                str(row["exception_id"]) for row in batched_exceptions
            ]),
            "ordered_source_line_ids_sha256": line_ledger_sha256_json([
                str(row["source_line_id"]) for row in batched_exceptions
            ]),
            "reason_counts": reason_counts,
        },
    }


def _expected_pre_a3_merge_gate() -> dict[str, Any]:
    return {
        "expected_exception_count": FROZEN_STAGE_A2_EXCEPTION_COUNT,
        "adjudicated_exception_count": FROZEN_STAGE_A2_EXCEPTION_COUNT,
        "unresolved_line_count": 0,
        "withheld_line_count": 0,
        "unowned_continuation_count": 0,
        "duplicate_continuation_owner_count": 0,
        "ownership_conflict_count": 8,
        "coverage_complete": True,
        "validation_passed": False,
    }


def _stage_a2_evaluation_report(
    *,
    parents: list[Mapping[str, Any]],
    stage_a_decisions: Mapping[str, Mapping[str, Any]],
    stage_a_provenance: Mapping[str, Any],
    stage_a2_artifacts: Mapping[str, Any],
    stage_a2_decisions: Mapping[str, Mapping[str, Any]],
    merged: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    replayed = {
        "parents": parents,
        "stage_a_decisions": stage_a_decisions,
        "stage_a2_merge": merged,
    }
    gate_replay = unchanged_gate_replay(replayed, merged)
    evaluation = gate_replay["evaluation"]
    observations = gate_replay["observations"]
    authenticated_a2 = {
        **dict(stage_a2_artifacts),
        "stage_a2_decision_ledger_sha256": line_ledger_sha256_json(
            stage_a2_decisions
        ),
    }
    report = {
        **evaluation,
        "line_ledger_contract_sha256": get_line_ledger_contract()[
            "line_ledger_contract_sha256"
        ],
        "stage_a_artifacts": dict(stage_a_provenance),
        "stage_a2_artifacts": authenticated_a2,
        "stage_a2_merge": {
            "validation": dict(merged["validation"]),
            "final_decisions_sha256": merged["final_decisions_sha256"],
            "per_line_provenance_sha256": merged[
                "per_line_provenance_sha256"
            ],
            "ownership_by_continuation_sha256": line_ledger_sha256_json(
                merged["ownership_by_continuation"]
            ),
        },
        "observations": observations,
        "stage_b_required": False,
    }
    return report, authenticated_a2


def _stage_a3_private_row(
    *,
    task_json: Path,
    task_html: Path,
    stage_a2_evaluation_report: Path,
    results_json: Path,
    imported_artifact_json: Path,
) -> dict[str, Any]:
    required = {
        "task": task_json,
        "HTML": task_html,
        "Stage-A2 evaluation": stage_a2_evaluation_report,
        "human results": results_json,
        "imported artifact": imported_artifact_json,
    }
    missing = [label for label, path in required.items() if not path.is_file()]
    if missing:
        raise ValueError(
            "Stage-A3 extraction-dependency artifacts are incomplete "
            f"({', '.join(missing)} absent); Stage B is forbidden"
        )
    task_bytes = task_json.read_bytes()
    html_bytes = task_html.read_bytes()
    if hashlib.sha256(task_bytes).hexdigest() != FROZEN_STAGE_A3_TASK_SHA256:
        raise ValueError("Stage-A3 task bytes differ from the reviewed task")
    if hashlib.sha256(html_bytes).hexdigest() != FROZEN_STAGE_A3_HTML_SHA256:
        raise ValueError("Stage-A3 HTML bytes differ from the reviewed app")
    return {
        "kind": STAGE_A3_PRIVATE_KIND,
        "task_bytes_base64": base64.b64encode(task_bytes).decode("ascii"),
        "html_bytes_base64": base64.b64encode(html_bytes).decode("ascii"),
        "evaluation_report_bytes_base64": base64.b64encode(
            stage_a2_evaluation_report.read_bytes()
        ).decode("ascii"),
        "results_bytes_base64": base64.b64encode(
            results_json.read_bytes()
        ).decode("ascii"),
        "imported_artifact_bytes_base64": base64.b64encode(
            imported_artifact_json.read_bytes()
        ).decode("ascii"),
    }


def _validate_stage_a3_private_row(
    row: Mapping[str, Any],
    *,
    parents: list[Mapping[str, Any]],
    stage_a_manifest: Mapping[str, Any],
    stage_a_provenance: Mapping[str, Any],
    stage_a_decisions: Mapping[str, Mapping[str, Any]],
    stage_a2_manifest_text: str,
    stage_a2_artifacts: Mapping[str, Any],
    stage_a2_batches: list[Mapping[str, Any]],
    stage_a2_decisions: Mapping[str, Mapping[str, Any]],
    stage_a2_merge: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    fields = {
        "kind", "task_bytes_base64", "html_bytes_base64",
        "evaluation_report_bytes_base64", "results_bytes_base64",
        "imported_artifact_bytes_base64",
    }
    if set(row) != fields or row.get("kind") != STAGE_A3_PRIVATE_KIND:
        raise ValueError("Stage-A3 private provenance fields differ")
    encoded_fields = fields - {"kind"}
    if any(not isinstance(row.get(field), str) for field in encoded_fields):
        raise ValueError("Stage-A3 private provenance bytes are absent")
    decoded: dict[str, bytes] = {}
    for field in encoded_fields:
        try:
            decoded[field] = base64.b64decode(
                str(row[field]), validate=True
            )
        except (ValueError, binascii.Error) as exc:
            raise ValueError(
                f"Stage-A3 private provenance {field} is not exact base64"
            ) from exc
    task_bytes = decoded["task_bytes_base64"]
    html_bytes = decoded["html_bytes_base64"]
    evaluation_bytes = decoded["evaluation_report_bytes_base64"]
    results_bytes = decoded["results_bytes_base64"]
    artifact_bytes = decoded["imported_artifact_bytes_base64"]
    try:
        task_text = task_bytes.decode("utf-8")
        html_text = html_bytes.decode("utf-8")
        evaluation_text = evaluation_bytes.decode("utf-8")
        results_text = results_bytes.decode("utf-8")
        artifact_text = artifact_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise ValueError("Stage-A3 private provenance is not UTF-8") from exc
    task_sha256 = hashlib.sha256(task_bytes).hexdigest()
    html_sha256 = hashlib.sha256(html_bytes).hexdigest()
    results_sha256 = hashlib.sha256(results_bytes).hexdigest()
    if task_sha256 != FROZEN_STAGE_A3_TASK_SHA256:
        raise ValueError("Stage-A3 task bytes differ from the reviewed task")
    if html_sha256 != FROZEN_STAGE_A3_HTML_SHA256:
        raise ValueError("Stage-A3 HTML bytes differ from the reviewed app")
    task = json.loads(task_text)
    evaluation = json.loads(evaluation_text)
    results = json.loads(results_text)
    artifact = json.loads(artifact_text)
    expected_evaluation, authenticated_a2 = _stage_a2_evaluation_report(
        parents=parents,
        stage_a_decisions=stage_a_decisions,
        stage_a_provenance=stage_a_provenance,
        stage_a2_artifacts=stage_a2_artifacts,
        stage_a2_decisions=stage_a2_decisions,
        merged=stage_a2_merge,
    )
    # The line-ledger contract embeds the Stage-A3 authentication, so
    # completing that adjudication advanced the contract hash. This evaluation
    # report was written before the adjudication and therefore carries the
    # pre-adjudication hash. Every other field must still match byte for byte.
    # The allowance is pinned to that one historical value and to that one
    # field, so any other drift still fails closed.
    comparable_evaluation = dict(evaluation)
    if (
        comparable_evaluation.get("line_ledger_contract_sha256")
        == PRE_STAGE_A3_LINE_LEDGER_CONTRACT_SHA256
        and expected_evaluation.get("line_ledger_contract_sha256")
        == get_line_ledger_contract()["line_ledger_contract_sha256"]
    ):
        comparable_evaluation["line_ledger_contract_sha256"] = (
            expected_evaluation["line_ledger_contract_sha256"]
        )
    if not _typed_equal(comparable_evaluation, expected_evaluation):
        raise ValueError("Stage-A3 Stage-A2 evaluation report replay differs")
    replayed = {
        "parents": parents,
        "stage_a_manifest": dict(stage_a_manifest),
        "stage_a_decisions": stage_a_decisions,
        "stage_a2_manifest": json.loads(stage_a2_manifest_text),
        "stage_a2_batches": stage_a2_batches,
        "stage_a2_artifacts": authenticated_a2,
        "stage_a2_merge": stage_a2_merge,
    }
    residuals = derive_a3_residuals(replayed, evaluation)
    expected_task_provenance = {
        "a3_spec": A3_SPEC,
        "a3_spec_sha256": A3_SPEC_SHA256,
        "stage_a_contract_sha256": stage_a_manifest[
            "line_ledger_contract_sha256"
        ],
        "stage_a2_contract_sha256": replayed["stage_a2_manifest"][
            "line_ledger_contract_sha256"
        ],
        "stage_a2_input_manifest_sha256": sha256_text(
            stage_a2_manifest_text
        ),
        "stage_a2_output_artifacts": authenticated_a2,
        "stage_a2_evaluation_report_sha256": hashlib.sha256(
            evaluation_bytes
        ).hexdigest(),
        "stage_a2_final_decisions_sha256": stage_a2_merge[
            "final_decisions_sha256"
        ],
        "stage_a2_per_line_provenance_sha256": stage_a2_merge[
            "per_line_provenance_sha256"
        ],
        "ordered_residual_core_sha256": line_ledger_sha256_json([{
            key: residual[key] for key in (
                "source_line_id", "parent_document_id", "parent_line_index",
                "line_text_sha256", "residual_reason", "stage_a2_final_role",
                "stage_a2_continuation_owner_source_line_id",
                "eligible_final_anchor_source_line_ids",
            )
        } for residual in residuals]),
        "residual_count": len(residuals),
    }
    validate_a3_task_provenance(task, expected_task_provenance)
    if [item.get("residual") for item in task.get("items", [])] != residuals:
        raise ValueError("Stage-A3 task residual replay differs")
    decisions = validate_a3_results(task, results)
    merged = merge_human_a3(
        parents,
        stage_a2_merge,
        residuals,
        decisions,
        task_sha256=task_sha256,
        results_sha256=results_sha256,
    )
    gates = unchanged_gate_replay(replayed, merged)["evaluation"]
    if (
        gates.get("gate_decision") != "GO"
        or type(gates.get("failed_checks")) is not int
        or gates["failed_checks"] != 0
        or not all(check.get("passed") is True for check in gates.get("checks", []))
    ):
        raise ValueError("Stage-A3 unchanged gate replay did not pass")
    expected_artifact = {
        "artifact_version": "candidate_endorsement_line_ledger_human_a3_v1",
        "a3_spec_sha256": A3_SPEC_SHA256,
        "task_sha256": task_sha256,
        "results_sha256": results_sha256,
        "results_exported_at": results["exported_at"],
        "residual_count": len(residuals),
        "decisions": dict(sorted(decisions.items())),
        "stage_a2_final_decisions_sha256": stage_a2_merge[
            "final_decisions_sha256"
        ],
        "stage_a3_final_decisions_sha256": merged[
            "final_decisions_sha256"
        ],
        "stage_a3_per_line_provenance_sha256": merged[
            "per_line_provenance_sha256"
        ],
        "stage_a3_decisions_sha256": merged["stage_a3_decisions_sha256"],
        "merge_validation": merged["validation"],
        "unchanged_stage_a2_only_gate_replay": gates,
        "stage_b_authorized": True,
    }
    if not _typed_equal(artifact, expected_artifact):
        raise ValueError("Stage-A3 imported artifact replay differs")
    summary = {
        "a3_spec_sha256": A3_SPEC_SHA256,
        "task_sha256": task_sha256,
        "html_sha256": html_sha256,
        "evaluation_report_sha256": hashlib.sha256(
            evaluation_bytes
        ).hexdigest(),
        "results_sha256": results_sha256,
        "imported_artifact_sha256": hashlib.sha256(
            artifact_bytes
        ).hexdigest(),
        "residual_count": len(residuals),
        "stage_a3_decisions_sha256": merged["stage_a3_decisions_sha256"],
        "stage_a3_final_decisions_sha256": merged[
            "final_decisions_sha256"
        ],
        "stage_a3_per_line_provenance_sha256": merged[
            "per_line_provenance_sha256"
        ],
        "stage_b_authorized": True,
    }
    return merged, summary


def _stage_b_manifest_extras(
    stage_a_provenance: Mapping[str, Any],
    stage_a2_artifacts: Mapping[str, Any],
    stage_a2_decisions: Mapping[str, Mapping[str, Any]],
    merged: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "stage_b_two_pass_provenance_spec_sha256": (
            STAGE_B_TWO_PASS_PROVENANCE_SPEC_SHA256
        ),
        "upstream_stage_a": {
            "contract_sha256": stage_a_provenance[
                "stage_a_line_ledger_contract_sha256"
            ],
            "input_manifest_sha256": stage_a_provenance[
                "stage_a_input_manifest_sha256"
            ],
            "generation_config_sha256": stage_a_provenance[
                "stage_a_generation_config_sha256"
            ],
            "ordered_output_batch_ids_sha256": stage_a_provenance[
                "stage_a_ordered_output_batch_ids_sha256"
            ],
            "decision_ledger_sha256": stage_a_provenance[
                "source_line_decision_ledger_sha256"
            ],
            "provenance_sha256": stage_a_provenance[
                "stage_a_provenance_sha256"
            ],
        },
        "upstream_stage_a2": {
            **dict(stage_a2_artifacts),
            "decision_ledger_sha256": line_ledger_sha256_json(
                stage_a2_decisions
            ),
        },
        "two_pass_merge": {
            "validation": dict(merged["validation"]),
            "final_decisions_sha256": merged["final_decisions_sha256"],
            "per_line_provenance_sha256": merged[
                "per_line_provenance_sha256"
            ],
            "ownership_by_continuation_sha256": line_ledger_sha256_json(
                merged["ownership_by_continuation"]
            ),
        },
    }


def _ledger_resolved_record(
    aligned: Mapping[str, Any],
    segment_by_id: Mapping[str, Mapping[str, Any]],
    parent_id: str,
    *,
    record_identity: str,
    resolution_status: str,
    resolution_method: str,
) -> dict[str, Any]:
    """Build one resolved record from an aligned two-stage ledger item.

    The two endorsement architectures construct a record the same way, from a
    carrier segment, an inventory owner segment, and one model item. They differ
    only in the labels the released record carries, which name the architecture
    that produced it.
    """
    item = dict(aligned)
    carrier_id = str(item["record_evidence_carrier_segment_id"])
    owner_id = str(item["inventory_owner_segment_id"])
    if carrier_id not in segment_by_id or owner_id not in segment_by_id:
        raise ValueError("line-ledger record links to an unknown source segment")
    carrier = segment_by_id[carrier_id]
    if str(carrier.get("parent_document_id")) != parent_id:
        raise ValueError("line-ledger record carrier crosses parent documents")
    computed_parent_offsets = parent_offsets(
        "endorsements", item, int(carrier["segment_start_char"])
    )
    if (
        computed_parent_offsets["record_evidence_parent_start_char"]
        != item.get("record_evidence_parent_start_char")
        or computed_parent_offsets["record_evidence_parent_end_char"]
        != item.get("record_evidence_parent_end_char")
    ):
        raise ValueError("line-ledger record parent offsets differ")
    source_model_item_id = str(item["source_model_item_id"])
    record_id = sha256_text(canonical_json({
        "record_identity": record_identity,
        "parent_document_id": parent_id,
        "source_model_item_id": source_model_item_id,
    }))
    source_segment_ids = sorted({carrier_id, owner_id})
    return {
        "record_id": record_id,
        "parent_document_id": parent_id,
        "route": "endorsements",
        "canonical_segment_id": carrier_id,
        "canonical_variant_id": source_model_item_id,
        "all_source_segment_ids": source_segment_ids,
        "all_variant_ids": [source_model_item_id],
        "variant_count": 1,
        "resolution_status": resolution_status,
        "resolution_method": resolution_method,
        "item": item,
        "parent_offsets": computed_parent_offsets,
    }


def _line_ledger_resolved_record(
    aligned: Mapping[str, Any],
    segment_by_id: Mapping[str, Mapping[str, Any]],
    parent_id: str,
) -> dict[str, Any]:
    return _ledger_resolved_record(
        aligned,
        segment_by_id,
        parent_id,
        record_identity="candidate_endorsement_parent_line_ledger_v1",
        resolution_status="line_ledger_single_record",
        resolution_method="two_stage_parent_line_ledger_v1",
    )


def round4_resolved_record(
    aligned: Mapping[str, Any],
    segment_by_id: Mapping[str, Mapping[str, Any]],
    parent_id: str,
) -> dict[str, Any]:
    """Build one resolved record from a round-four aligned record."""
    from src.candidate_endorsement_round4_release import (
        ROUND4_RECORD_IDENTITY,
        ROUND4_RECORD_RESOLUTION_METHOD,
        ROUND4_RECORD_RESOLUTION_STATUS,
    )

    return _ledger_resolved_record(
        aligned,
        segment_by_id,
        parent_id,
        record_identity=ROUND4_RECORD_IDENTITY,
        resolution_status=ROUND4_RECORD_RESOLUTION_STATUS,
        resolution_method=ROUND4_RECORD_RESOLUTION_METHOD,
    )


def validate_endorsement_line_ledger_private_rows(
    inputs: Mapping[str, Mapping[str, Any]],
    private_rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Replay frozen A1, mandatory A2/A3, and Stage B exactly."""
    rows = [dict(row) for row in private_rows]
    stage_a3_rows = [
        row for row in rows if row.get("kind") == STAGE_A3_PRIVATE_KIND
    ]
    if len(stage_a3_rows) != 1:
        raise ValueError(
            "private line-ledger Stage-A3 extraction dependency is incomplete"
        )
    provenance_by_stage: dict[str, dict[str, Any]] = {}
    batches_by_stage: dict[str, list[dict[str, Any]]] = defaultdict(list)
    outputs_by_stage: dict[str, list[dict[str, Any]]] = defaultdict(list)
    stages = {
        LINE_DECISION_STAGE,
        LINE_ADJUDICATION_STAGE,
        RECORD_EXTRACTION_STAGE,
    }
    for row in rows:
        kind = row.get("kind")
        if kind == STAGE_A3_PRIVATE_KIND:
            continue
        stage = str(row.get("stage") or "")
        if stage not in stages:
            raise ValueError("private line-ledger row has an invalid stage")
        if kind == "stage_provenance":
            if stage in provenance_by_stage:
                raise ValueError(f"duplicate {stage} private provenance")
            provenance_by_stage[stage] = row
        elif kind == "stage_batch_output" and set(row) == {
            "kind", "stage", "input_batch", "output_row",
        }:
            if not isinstance(row.get("input_batch"), Mapping) or not isinstance(
                row.get("output_row"), Mapping
            ):
                raise ValueError(f"{stage} private batch row differs")
            batches_by_stage[stage].append(dict(row["input_batch"]))
            outputs_by_stage[stage].append(dict(row["output_row"]))
        else:
            raise ValueError("private line-ledger row fields differ")
    if set(provenance_by_stage) != stages:
        raise ValueError("private line-ledger stage provenance is incomplete")
    endorsement_groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for source in inputs.values():
        if source.get("route") == "endorsements":
            endorsement_groups[str(source["parent_document_id"])].append(source)
    all_parents = [
        reconstruct_parent_inventory(endorsement_groups[parent_id])
        for parent_id in sorted(endorsement_groups)
    ]
    all_parent_by_id = {
        str(parent["parent_document_id"]): parent for parent in all_parents
    }
    stage_a_preview = json.loads(str(
        provenance_by_stage[LINE_DECISION_STAGE].get("input_manifest_text")
    ))
    preview_mode = stage_a_preview.get("mode")
    preview_parent_ids = stage_a_preview.get("selected_parent_ids")
    expected_parent_ids = [parent_id for _, parent_id in PILOT_CONTROLS]
    if preview_mode != "line_decision_preflight" or not _typed_equal(
        preview_parent_ids, expected_parent_ids
    ):
        raise ValueError("private Stage-A exporter mode differs")
    missing = [
        parent_id for parent_id in expected_parent_ids
        if parent_id not in all_parent_by_id
    ]
    if missing:
        raise ValueError("private Stage-A preflight parents are absent")
    parents = [all_parent_by_id[parent_id] for parent_id in expected_parent_ids]

    historical_contract = get_frozen_stage_a_v2_contract()
    current_contract = get_line_ledger_contract()
    stage_a_batches = _build_historical_stage_a_batches(parents)
    a_manifest, a_generation, a_replay = _validate_line_ledger_stage_provenance(
        provenance_by_stage[LINE_DECISION_STAGE],
        LINE_DECISION_STAGE,
        parents,
        stage_a_batches,
        outputs_by_stage[LINE_DECISION_STAGE],
        contract=historical_contract,
        expected_mode=preview_mode,
        expected_bounded_preflight=_historical_stage_a_bounded_preflight(),
        expected_chunk_size=LINE_LEDGER_CHUNK_SIZE,
    )
    expected_a_input_files = [
        {
            "file": Path(name).name,
            "sha256": entry["sha256"],
        }
        for name, entry in sorted(FROZEN_STAGE_A_SUFFIX_B_INPUT_FILES.items())
        if name.startswith("chunks/")
    ]
    expected_a_output_files = [
        {
            "file": name,
            "sha256": entry["sha256"],
            "bytes": entry["bytes"],
            "records": entry["records"],
        }
        for name, entry in sorted(FROZEN_STAGE_A_SUFFIX_B_OUTPUT_FILES.items())
        if name.endswith(".out.jsonl.gz")
    ]
    expected_a_metadata_files = [{
        "file": "run_metadata.json",
        "sha256": FROZEN_STAGE_A_SUFFIX_B_OUTPUT_FILES[
            "run_metadata.json"
        ]["sha256"],
    }]
    if (
        a_generation["input_manifest_sha256"]
        != FROZEN_STAGE_A_SUFFIX_B_INPUT_MANIFEST_SHA256
        or a_generation["generation_config_sha256"]
        != FROZEN_STAGE_A_SUFFIX_B_GENERATION_CONFIG_SHA256
        or not _typed_equal(
            a_replay["artifacts"].get("input_files"),
            expected_a_input_files,
        )
        or not _typed_equal(
            a_replay["artifacts"].get("output_files"),
            expected_a_output_files,
        )
        or not _typed_equal(
            a_replay["artifacts"].get("run_metadata_files"),
            expected_a_metadata_files,
        )
        or a_replay["artifacts"].get("output_batch_ids_sha256")
        != FROZEN_STAGE_A_SUFFIX_B_OUTPUT_BATCH_IDS_SHA256
    ):
        raise ValueError("historical Stage-A frozen byte identity differs")
    if not _typed_equal(batches_by_stage[LINE_DECISION_STAGE], stage_a_batches):
        raise ValueError("private Stage-A batches differ from source replay")
    if [batch["batch_id"] for batch in stage_a_batches] != a_manifest.get(
        "expected_batch_ids"
    ):
        raise ValueError("private Stage-A batch order differs from manifest")
    stage_a_validations: list[dict[str, Any]] = []
    for batch, output in zip(
        stage_a_batches,
        outputs_by_stage[LINE_DECISION_STAGE],
        strict=True,
    ):
        validation, _ = _validate_line_ledger_output_row(
            batch,
            output,
            LINE_DECISION_STAGE,
            a_generation,
            contract=historical_contract,
        )
        stage_a_validations.append(validation)
    stage_a_decisions = line_decision_ledger(
        stage_a_batches, outputs_by_stage[LINE_DECISION_STAGE]
    )
    if line_ledger_sha256_json(stage_a_decisions) != (
        FROZEN_STAGE_A_SUFFIX_B_DECISION_LEDGER_SHA256
    ):
        raise ValueError("historical Stage-A decision ledger differs")
    expected_stage_a_provenance = _expected_historical_stage_a_provenance(
        a_manifest,
        stage_a_batches,
        outputs_by_stage[LINE_DECISION_STAGE],
        stage_a_decisions,
        a_replay,
    )
    if not _typed_equal(
        provenance_by_stage[LINE_DECISION_STAGE].get(
            "upstream_stage_a_provenance"
        ),
        expected_stage_a_provenance,
    ):
        raise ValueError("historical Stage-A provenance replay differs")

    exceptions = line_adjudication_exception_ledger(
        parents, stage_a_decisions
    )
    stage_a2_batches = build_line_adjudication_batches(
        parents,
        stage_a_decisions,
        stage_a_provenance=expected_stage_a_provenance,
    )
    if (
        len(exceptions) != FROZEN_STAGE_A2_EXCEPTION_COUNT
        or sum(len(batch["targets"]) for batch in stage_a2_batches)
        != FROZEN_STAGE_A2_EXCEPTION_COUNT
        or len(stage_a2_batches) != 29
    ):
        raise ValueError("Stage-A2 deterministic batch census differs")
    stage_a2_extras = _stage_a2_manifest_extras(
        parents,
        stage_a_decisions,
        exceptions,
        expected_stage_a_provenance,
        stage_a2_batches,
    )
    a2_manifest, a2_generation, a2_replay = (
        _validate_line_ledger_stage_provenance(
            provenance_by_stage[LINE_ADJUDICATION_STAGE],
            LINE_ADJUDICATION_STAGE,
            parents,
            stage_a2_batches,
            outputs_by_stage[LINE_ADJUDICATION_STAGE],
            contract=current_contract,
            expected_mode=(
                "line_adjudication_preflight_from_frozen_stage_a_suffix_b"
            ),
            expected_bounded_preflight=_stage_a2_bounded_preflight(),
            expected_chunk_size=FROZEN_STAGE_A2_CHUNK_SIZE,
            stage_a_decisions=stage_a_decisions,
            expected_manifest_extras=stage_a2_extras,
        )
    )
    if (
        a2_generation["input_manifest_sha256"]
        != FROZEN_STAGE_A2_INPUT_MANIFEST_SHA256
        or a2_generation["generation_config_sha256"]
        != FROZEN_STAGE_A2_GENERATION_CONFIG_SHA256
    ):
        raise ValueError("frozen Stage-A2 byte identity differs")
    if provenance_by_stage[LINE_ADJUDICATION_STAGE].get(
        "upstream_stage_a_provenance"
    ) is not None:
        raise ValueError("Stage-A2 private provenance has an extra A1 copy")
    if not _typed_equal(
        batches_by_stage[LINE_ADJUDICATION_STAGE], stage_a2_batches
    ):
        raise ValueError("private Stage-A2 batches differ from A1 replay")
    if [batch["batch_id"] for batch in stage_a2_batches] != a2_manifest.get(
        "expected_batch_ids"
    ):
        raise ValueError("private Stage-A2 batch order differs from manifest")
    stage_a2_validations: list[dict[str, Any]] = []
    for batch, output in zip(
        stage_a2_batches,
        outputs_by_stage[LINE_ADJUDICATION_STAGE],
        strict=True,
    ):
        validation, _ = _validate_line_ledger_output_row(
            batch,
            output,
            LINE_ADJUDICATION_STAGE,
            a2_generation,
            contract=current_contract,
        )
        stage_a2_validations.append(validation)
    stage_a2_decisions = line_adjudication_ledger(
        stage_a2_batches, outputs_by_stage[LINE_ADJUDICATION_STAGE]
    )
    pre_a3_merged = merge_line_adjudication_ledger(
        parents, stage_a_decisions, stage_a2_decisions
    )
    if not _typed_equal(
        pre_a3_merged.get("validation"), _expected_pre_a3_merge_gate()
    ):
        raise ValueError("Stage-A2 pre-A3 merge gate differs")
    merged, stage_a3_summary = _validate_stage_a3_private_row(
        stage_a3_rows[0],
        parents=parents,
        stage_a_manifest=a_manifest,
        stage_a_provenance=expected_stage_a_provenance,
        stage_a_decisions=stage_a_decisions,
        stage_a2_manifest_text=str(
            provenance_by_stage[LINE_ADJUDICATION_STAGE][
                "input_manifest_text"
            ]
        ),
        stage_a2_artifacts=a2_replay["artifacts"],
        stage_a2_batches=stage_a2_batches,
        stage_a2_decisions=stage_a2_decisions,
        stage_a2_merge=pre_a3_merged,
    )

    stage_b_batches = build_record_extraction_batches_from_adjudicated_merge(
        parents, merged
    )
    stage_b_extras = _stage_b_manifest_extras(
        expected_stage_a_provenance,
        a2_replay["artifacts"],
        stage_a2_decisions,
        merged,
    )
    b_manifest, b_generation, _ = _validate_line_ledger_stage_provenance(
        provenance_by_stage[RECORD_EXTRACTION_STAGE],
        RECORD_EXTRACTION_STAGE,
        parents,
        stage_b_batches,
        outputs_by_stage[RECORD_EXTRACTION_STAGE],
        contract=current_contract,
        expected_mode="record_extraction_preflight_from_stage_a2",
        expected_bounded_preflight=_stage_b_bounded_preflight(),
        expected_chunk_size=LINE_LEDGER_CHUNK_SIZE,
        stage_a_decisions=merged["final_decisions"],
        expected_manifest_extras=stage_b_extras,
    )
    if provenance_by_stage[RECORD_EXTRACTION_STAGE].get(
        "upstream_stage_a_provenance"
    ) is not None:
        raise ValueError("Stage-B private provenance has an extra A1 copy")
    if not _typed_equal(batches_by_stage[RECORD_EXTRACTION_STAGE], stage_b_batches):
        raise ValueError("private Stage-B batches differ from Stage-A replay")
    if [batch["batch_id"] for batch in stage_b_batches] != b_manifest.get(
        "expected_batch_ids"
    ):
        raise ValueError("private Stage-B batch order differs from manifest")
    segment_by_id = {
        str(row["document_id"]): row
        for values in endorsement_groups.values() for row in values
    }
    parent_by_id = {str(parent["parent_document_id"]): parent for parent in parents}
    parent_summaries: dict[str, dict[str, Any]] = {}
    resolved_by_segment: dict[str, list[dict[str, Any]]] = defaultdict(list)
    aligned_record_ids: set[str] = set()
    stage_b_validation_rows: list[dict[str, Any]] = []
    for batch, output in zip(
        stage_b_batches,
        outputs_by_stage[RECORD_EXTRACTION_STAGE],
        strict=True,
    ):
        validation, alignment = _validate_line_ledger_output_row(
            batch,
            output,
            RECORD_EXTRACTION_STAGE,
            b_generation,
            contract=current_contract,
        )
        if not isinstance(alignment, dict):
            raise ValueError("Stage-B alignment is absent")
        stage_b_validation_rows.append({
            "batch": batch,
            "validation": validation,
            "alignment": alignment,
        })
        parent_id = str(batch["parent_document_id"])
        for aligned in alignment["aligned_records"]:
            resolved = _line_ledger_resolved_record(
                aligned, segment_by_id, parent_id
            )
            if resolved["record_id"] in aligned_record_ids:
                raise ValueError("duplicate line-ledger public record ID")
            aligned_record_ids.add(resolved["record_id"])
            resolved_by_segment[resolved["canonical_segment_id"]].append(resolved)
    source_line_parent = {
        str(line["source_line_id"]): parent_id
        for parent_id, parent in parent_by_id.items()
        for line in parent["lines"]
    }
    stage_a_by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    final_by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for decision in stage_a_decisions.values():
        source_line_id = str(decision["source_line_id"])
        parent_id = source_line_parent.get(source_line_id)
        if parent_id is None:
            raise ValueError("Stage-A decision links to an unknown parent line")
        stage_a_by_parent[parent_id].append(decision)
    for decision in merged["final_decisions"].values():
        source_line_id = str(decision["source_line_id"])
        parent_id = source_line_parent.get(source_line_id)
        if parent_id is None:
            raise ValueError("final decision links to an unknown parent line")
        final_by_parent[parent_id].append(decision)
    stage_b_by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for value in stage_b_validation_rows:
        stage_b_by_parent[str(value["batch"]["parent_document_id"])].append(value)
    carrier_counts_by_parent: dict[str, Counter[str]] = defaultdict(Counter)
    for records in resolved_by_segment.values():
        for resolved in records:
            carrier_counts_by_parent[str(resolved["parent_document_id"])][
                str(resolved["canonical_segment_id"])
            ] += 1
    for parent_id, parent in parent_by_id.items():
        parent_stage_a = stage_a_by_parent[parent_id]
        parent_final = final_by_parent[parent_id]
        parent_stage_b = stage_b_by_parent[parent_id]
        stage_a_role_counts = Counter(
            str(value["role"]) for value in parent_stage_a
        )
        final_role_counts = Counter(
            str(value["role"]) for value in parent_final
        )
        aligned_count = sum(
            int(value["alignment"]["aligned_record_count"])
            for value in parent_stage_b
        )
        withheld_count = sum(
            int(value["alignment"]["withheld_record_count"])
            for value in parent_stage_b
        )
        public_coverage_complete = (
            final_role_counts["unclear"] == 0
            and all(
                value["alignment"]["public_coverage_complete"] is True
                for value in parent_stage_b
            )
        )
        owner_counts = Counter(
            str(line["owner_segment_id"]) for line in parent["lines"]
            if line["model_decision_required"]
        )
        carrier_counts = carrier_counts_by_parent[parent_id]
        parent_summaries[parent_id] = {
            "line_inventory_id": parent["parent_line_inventory_sha256"],
            "stage_a_batch_count": sum(
                batch["parent_document_id"] == parent_id for batch in stage_a_batches
            ),
            "stage_a_decision_count": len(parent_stage_a),
            "stage_a_unresolved_line_count": stage_a_role_counts["unclear"],
            "stage_b_batch_count": len(parent_stage_b),
            "stage_b_anchor_count": sum(
                int(value["validation"]["anchor_decision_count"])
                for value in parent_stage_b
            ),
            "aligned_record_count": aligned_count,
            "withheld_record_count": withheld_count,
            "public_coverage_complete": public_coverage_complete,
            "owner_segment_model_line_counts": dict(sorted(owner_counts.items())),
            "carrier_segment_aligned_record_counts": dict(
                sorted(carrier_counts.items())
            ),
        }
    total_withheld = sum(
        int(value["alignment"]["withheld_record_count"])
        for value in stage_b_validation_rows
    )
    summary = {
        "version": "candidate_endorsement_line_ledger_bundle_v3",
        "line_ledger_contract": current_contract,
        "line_ledger_contract_sha256": current_contract[
            "line_ledger_contract_sha256"
        ],
        "historical_stage_a_contract_sha256": historical_contract[
            "line_ledger_contract_sha256"
        ],
        "line_inventory_spec_sha256": LINE_INVENTORY_SPEC_SHA256,
        "line_batch_spec_sha256": LINE_BATCH_SPEC_SHA256,
        "stage_a_input_manifest_sha256": a_generation["input_manifest_sha256"],
        "stage_a_generation_config_sha256": a_generation[
            "generation_config_sha256"
        ],
        "stage_a_provenance_sha256": expected_stage_a_provenance[
            "stage_a_provenance_sha256"
        ],
        "stage_a2_input_manifest_sha256": a2_generation[
            "input_manifest_sha256"
        ],
        "stage_a2_generation_config_sha256": a2_generation[
            "generation_config_sha256"
        ],
        "stage_a2_decision_ledger_sha256": line_ledger_sha256_json(
            stage_a2_decisions
        ),
        "stage_b_input_manifest_sha256": b_generation["input_manifest_sha256"],
        "stage_b_generation_config_sha256": b_generation[
            "generation_config_sha256"
        ],
        "parent_documents": len(parents),
        "stage_a_batches": len(stage_a_batches),
        "stage_a_model_decision_lines": len(stage_a_decisions),
        "stage_a_unresolved_lines": sum(
            value["unresolved_line_count"] for value in stage_a_validations
        ),
        "stage_a2_batches": len(stage_a2_batches),
        "stage_a2_exception_lines": len(stage_a2_decisions),
        "stage_a2_unresolved_lines": merged["validation"][
            "unresolved_line_count"
        ],
        "stage_a2_withheld_lines": sum(
            value["withheld_line_count"] for value in stage_a2_validations
        ),
        "pre_a3_merge_validation": dict(pre_a3_merged["validation"]),
        "stage_a3": stage_a3_summary,
        "three_pass_merge_validation": dict(merged["validation"]),
        "final_line_decisions_sha256": merged["final_decisions_sha256"],
        "three_pass_line_provenance_sha256": merged[
            "per_line_provenance_sha256"
        ],
        "stage_b_batches": len(stage_b_batches),
        "stage_b_anchors": sum(
            int(value["validation"]["anchor_decision_count"])
            for value in stage_b_validation_rows
        ),
        "aligned_records": len(aligned_record_ids),
        "withheld_records": total_withheld,
        "public_coverage_complete": all(
            value["public_coverage_complete"] is True
            for value in parent_summaries.values()
        ) and merged["validation"]["validation_passed"] is True,
    }
    return {
        "summary": summary,
        "parent_summaries": parent_summaries,
        "resolved_records_by_segment": {
            segment_id: sorted(records, key=lambda value: value["record_id"])
            for segment_id, records in sorted(resolved_by_segment.items())
        },
    }


def load_endorsement_line_ledger_runs(
    inputs: Mapping[str, Mapping[str, Any]],
    stage_a_export_dir: Path,
    stage_a_output_dir: Path,
    stage_a2_export_dir: Path,
    stage_a2_output_dir: Path,
    stage_a3_task_json: Path,
    stage_a3_task_html: Path,
    stage_a2_evaluation_report: Path,
    stage_a3_results_json: Path,
    stage_a3_imported_artifact_json: Path,
    stage_b_export_dir: Path,
    stage_b_output_dir: Path,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    (
        a_manifest,
        a_batches,
        a_outputs,
        stage_a_decisions,
        a_provenance,
    ) = load_frozen_stage_a_suffix_b(
        stage_a_export_dir, stage_a_output_dir
    )
    a_artifacts = _historical_stage_a_artifacts(
        a_manifest, a_outputs, a_provenance, stage_a_output_dir
    )
    a2_manifest, a2_batches, a2_outputs, a2_artifacts = (
        load_validated_stage_run(
            stage_a2_export_dir,
            stage_a2_output_dir,
            stage=LINE_ADJUDICATION_STAGE,
        )
    )
    stage_a3_row = _stage_a3_private_row(
        task_json=stage_a3_task_json,
        task_html=stage_a3_task_html,
        stage_a2_evaluation_report=stage_a2_evaluation_report,
        results_json=stage_a3_results_json,
        imported_artifact_json=stage_a3_imported_artifact_json,
    )
    endorsement_groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for source in inputs.values():
        if source.get("route") == "endorsements":
            endorsement_groups[str(source["parent_document_id"])].append(source)
    parent_by_id = {
        parent_id: reconstruct_parent_inventory(endorsement_groups[parent_id])
        for parent_id in sorted(endorsement_groups)
    }
    parents = [parent_by_id[parent_id] for parent_id in PILOT_PARENT_IDS]
    stage_a2_decisions = line_adjudication_ledger(a2_batches, a2_outputs)
    pre_a3_merge = merge_line_adjudication_ledger(
        parents, stage_a_decisions, stage_a2_decisions
    )
    _validate_stage_a3_private_row(
        stage_a3_row,
        parents=parents,
        stage_a_manifest=a_manifest,
        stage_a_provenance=a_provenance,
        stage_a_decisions=stage_a_decisions,
        stage_a2_manifest_text=(stage_a2_export_dir / "manifest.json").read_text(
            encoding="utf-8"
        ),
        stage_a2_artifacts=a2_artifacts,
        stage_a2_batches=a2_batches,
        stage_a2_decisions=stage_a2_decisions,
        stage_a2_merge=pre_a3_merge,
    )
    b_manifest, b_batches, b_outputs, b_artifacts = (
        load_validated_stage_run(
            stage_b_export_dir,
            stage_b_output_dir,
            stage=RECORD_EXTRACTION_STAGE,
        )
    )
    private_rows = [
        _line_ledger_stage_provenance_row(
            LINE_DECISION_STAGE,
            a_manifest,
            a_artifacts,
            stage_a_export_dir,
            stage_a_output_dir,
            upstream_stage_a_provenance=a_provenance,
        ),
        *(
            {
                "kind": "stage_batch_output",
                "stage": LINE_DECISION_STAGE,
                "input_batch": batch,
                "output_row": output,
            }
            for batch, output in zip(a_batches, a_outputs, strict=True)
        ),
        _line_ledger_stage_provenance_row(
            LINE_ADJUDICATION_STAGE,
            a2_manifest,
            a2_artifacts,
            stage_a2_export_dir,
            stage_a2_output_dir,
        ),
        *(
            {
                "kind": "stage_batch_output",
                "stage": LINE_ADJUDICATION_STAGE,
                "input_batch": batch,
                "output_row": output,
            }
            for batch, output in zip(a2_batches, a2_outputs, strict=True)
        ),
        stage_a3_row,
        _line_ledger_stage_provenance_row(
            RECORD_EXTRACTION_STAGE,
            b_manifest,
            b_artifacts,
            stage_b_export_dir,
            stage_b_output_dir,
        ),
        *(
            {
                "kind": "stage_batch_output",
                "stage": RECORD_EXTRACTION_STAGE,
                "input_batch": batch,
                "output_row": output,
            }
            for batch, output in zip(b_batches, b_outputs, strict=True)
        ),
    ]
    validated = validate_endorsement_line_ledger_private_rows(
        inputs, private_rows
    )
    return private_rows, validated


def load_endorsement_line_ledger_bundle(
    bundle_dir: Path,
    manifest: Mapping[str, Any],
    segment_rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    declared = manifest.get("endorsement_line_ledger")
    if not isinstance(declared, Mapping) or set(declared) != {
        "summary", "private_file",
    }:
        raise ValueError("bundle endorsement line-ledger provenance is absent")
    spec = declared.get("private_file")
    if not isinstance(spec, Mapping) or set(spec) != {"file", "rows", "sha256"}:
        raise ValueError("bundle endorsement line-ledger file spec differs")
    name = spec.get("file")
    if name != ENDORSEMENT_LINE_LEDGER_FILENAME:
        raise ValueError("bundle endorsement line-ledger filename differs")
    path = bundle_dir / name
    if not path.is_file() or sha256_file(path) != spec.get("sha256"):
        raise ValueError("bundle endorsement line-ledger bytes differ")
    rows = list(read_jsonl_gz(path))
    if type(spec.get("rows")) is not int or spec["rows"] != len(rows):
        raise ValueError("bundle endorsement line-ledger row count differs")
    inputs = {str(row["document_id"]): row for row in segment_rows}
    validated = validate_endorsement_line_ledger_private_rows(inputs, rows)
    if not _typed_equal(declared.get("summary"), validated["summary"]):
        raise ValueError("bundle endorsement line-ledger summary differs")
    expected_resolved = validated["resolved_records_by_segment"]
    for row in segment_rows:
        if row.get("route") != "endorsements":
            continue
        segment_id = str(row["document_id"])
        if not _typed_equal(
            row.get("resolved_records"), expected_resolved.get(segment_id, [])
        ) or row.get("record_variants") != []:
            raise ValueError(
                f"{segment_id}: endorsement ledger record projection differs"
            )
    return validated


def load_endorsement_round4_ledger_bundle(
    bundle_dir: Path,
    manifest: Mapping[str, Any],
    segment_rows: Iterable[Mapping[str, Any]],
) -> dict[str, Any]:
    """Load and replay the round-four endorsement record ledger.

    The check mirrors the line-ledger loader.  It authenticates the private
    file by name, byte digest, and row count, revalidates the ledger against
    itself, and requires every endorsement segment row to carry exactly the
    records the ledger resolved.
    """
    from src.candidate_endorsement_round4_release import (
        Round4ProjectionError,
        validate_endorsement_round4_record_ledger_private_rows,
    )

    declared = manifest.get("endorsement_round4_record_ledger")
    if not isinstance(declared, Mapping) or set(declared) != {
        "summary", "parents", "private_file",
    }:
        raise ValueError("bundle endorsement round-four provenance is absent")
    spec = declared.get("private_file")
    if not isinstance(spec, Mapping) or set(spec) != {"file", "rows", "sha256"}:
        raise ValueError("bundle endorsement round-four file spec differs")
    name = spec.get("file")
    if name != ENDORSEMENT_ROUND4_LEDGER_FILENAME:
        raise ValueError("bundle endorsement round-four filename differs")
    path = bundle_dir / name
    if not path.is_file() or sha256_file(path) != spec.get("sha256"):
        raise ValueError("bundle endorsement round-four bytes differ")
    rows = list(read_jsonl_gz(path))
    if type(spec.get("rows")) is not int or spec["rows"] != len(rows):
        raise ValueError("bundle endorsement round-four row count differs")
    ledger = {
        "summary": declared["summary"], "parents": declared["parents"],
    }
    try:
        validate_endorsement_round4_record_ledger_private_rows(ledger)
    except Round4ProjectionError as error:
        raise ValueError(f"round-four ledger replay differs: {error}") from error

    resolved_by_segment: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise ValueError("round-four private row is not an object")
        segment_id = str(row["canonical_segment_id"])
        resolved_by_segment.setdefault(segment_id, []).append(dict(row))
    for segment_id, records in resolved_by_segment.items():
        records.sort(key=lambda value: str(value["record_id"]))
    for row in segment_rows:
        if row.get("route") != "endorsements":
            continue
        segment_id = str(row["document_id"])
        if not _typed_equal(
            row.get("resolved_records"), resolved_by_segment.get(segment_id, [])
        ) or row.get("record_variants") != []:
            raise ValueError(
                f"{segment_id}: round-four record projection differs"
            )
    return {
        "summary": declared["summary"],
        "parent_summaries": {
            str(item["parent_document_id"]): dict(item)
            for item in declared["parents"]
        },
        "resolved_records_by_segment": resolved_by_segment,
    }


def endorsement_segment_complete_line_census(
    rows: Iterable[Mapping[str, Any]],
) -> dict[str, int]:
    """Recompute complete-line counts from exact endorsement segment rows."""
    endorsement_segments = 0
    with_complete_line = 0
    without_complete_line = 0
    first_display_differs = 0
    last_display_differs = 0
    for row in rows:
        if row.get("route") != "endorsements":
            continue
        endorsement_segments += 1
        context = row.get("endorsement_context_provenance")
        if not isinstance(context, Mapping):
            raise ValueError("endorsement segment lacks context provenance")
        has_complete_line = context.get("segment_has_complete_line")
        if type(has_complete_line) is not bool:
            raise ValueError(
                "endorsement segment has invalid complete-line status"
            )
        if not has_complete_line:
            without_complete_line += 1
            continue
        with_complete_line += 1
        boundary_fields = (
            "segment_first_complete_line_start_char",
            "segment_first_complete_line_inference_display_start_char",
            "segment_last_complete_line_end_char",
            "segment_last_complete_line_inference_display_end_char",
        )
        if any(
            type(context.get(field)) is not int or context[field] < 0
            for field in boundary_fields
        ):
            raise ValueError(
                "endorsement segment has invalid complete-line boundaries"
            )
        first_display_differs += int(
            context["segment_first_complete_line_start_char"]
            != context[
                "segment_first_complete_line_inference_display_start_char"
            ]
        )
        last_display_differs += int(
            context["segment_last_complete_line_end_char"]
            != context[
                "segment_last_complete_line_inference_display_end_char"
            ]
        )
    return {
        "endorsement_segments": endorsement_segments,
        "segments_with_complete_line": with_complete_line,
        "segments_without_complete_line": without_complete_line,
        "first_source_display_boundary_differs": first_display_differs,
        "last_source_display_boundary_differs": last_display_differs,
    }


def validate_endorsement_context_manifest(
    context_provenance: Any,
    rows: Iterable[Mapping[str, Any]],
    *,
    inherited_full_source_manifest_sha256: Any = None,
    source_corpus_sha256: Any = None,
) -> dict[str, int]:
    """Validate static context identity and independently check its census.

    Direct exports report a census over their own rows. Derived preflight and
    stress subsets intentionally inherit the frozen full-export census; those
    manifests are authenticated by the frozen full-manifest SHA-256, while the
    selected-row census is recomputed and bounded by the inherited totals.
    """
    expected_static = {
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
            "segment",
            "parent_document",
            "page_or_section_route_context",
        ],
    }
    expected_fields = set(expected_static) | {"segment_complete_line_census"}
    if (
        not isinstance(context_provenance, Mapping)
        or set(context_provenance) != expected_fields
    ):
        raise ValueError("source export endorsement-context provenance differs")
    for field, expected in expected_static.items():
        if not _typed_equal(context_provenance.get(field), expected):
            raise ValueError(
                f"source export endorsement-context provenance {field} differs"
            )
    census_value = context_provenance.get("segment_complete_line_census")
    if (
        not isinstance(census_value, Mapping)
        or set(census_value) != set(FROZEN_SUFFIX_P_COMPLETE_LINE_CENSUS)
    ):
        raise ValueError(
            "source export endorsement complete-line census fields differ"
        )
    declared_census = {
        field: _nonnegative_integer(
            census_value.get(field),
            f"endorsement complete-line census.{field}",
        )
        for field in FROZEN_SUFFIX_P_COMPLETE_LINE_CENSUS
    }
    if (
        declared_census["segments_with_complete_line"]
        + declared_census["segments_without_complete_line"]
        != declared_census["endorsement_segments"]
        or declared_census["first_source_display_boundary_differs"]
        > declared_census["segments_with_complete_line"]
        or declared_census["last_source_display_boundary_differs"]
        > declared_census["segments_with_complete_line"]
    ):
        raise ValueError(
            "source export endorsement complete-line census does not reconcile"
        )
    selected_census = endorsement_segment_complete_line_census(rows)
    if inherited_full_source_manifest_sha256 is None:
        if not _typed_equal(declared_census, selected_census):
            raise ValueError(
                "source export endorsement complete-line census differs from rows"
            )
    else:
        if (
            inherited_full_source_manifest_sha256
            != FROZEN_SUFFIX_P_FULL_SOURCE_MANIFEST_SHA256
            or source_corpus_sha256
            != FROZEN_SUFFIX_P_SOURCE_CORPUS_SHA256
            or not _typed_equal(
                declared_census, FROZEN_SUFFIX_P_COMPLETE_LINE_CENSUS
            )
        ):
            raise ValueError(
                "derived source export full-universe context attestation differs"
            )
        if any(
            selected_census[field] > declared_census[field]
            for field in declared_census
        ):
            raise ValueError(
                "derived source export selected census exceeds full universe"
            )
    return selected_census


def validate_source_export_identity(
    manifest: Mapping[str, Any], actual_manifest_sha256: Any
) -> None:
    """Require the frozen suffix-p full export or a subset derived from it."""
    if manifest.get("source_corpus_sha256") != FROZEN_SUFFIX_P_SOURCE_CORPUS_SHA256:
        raise ValueError("source export corpus SHA-256 is not the frozen suffix-p corpus")
    upstream_manifest_sha256 = manifest.get("source_export_manifest_sha256")
    if upstream_manifest_sha256 is None:
        if actual_manifest_sha256 != FROZEN_SUFFIX_P_FULL_SOURCE_MANIFEST_SHA256:
            raise ValueError(
                "direct source export manifest is not the frozen suffix-p full export"
            )
    elif upstream_manifest_sha256 != FROZEN_SUFFIX_P_FULL_SOURCE_MANIFEST_SHA256:
        raise ValueError(
            "derived source export does not attest the frozen suffix-p full export"
        )


def load_inputs(export_dir: Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    manifest_path = export_dir / "manifest.json"
    manifest = json.loads(manifest_path.read_text())
    if manifest.get("schema_version") != "1.4":
        raise ValueError("source export manifest is not schema 1.4")
    validate_source_export_identity(manifest, sha256_file(manifest_path))
    transform = manifest.get("source_text_transform")
    expected_transform = {
        "version": SOURCE_TEXT_TRANSFORM_VERSION,
        "rule": SOURCE_TEXT_TRANSFORM_RULE,
        "offset_coordinate_system": OFFSET_COORDINATE_SYSTEM,
    }
    if not isinstance(transform, dict):
        raise ValueError("source export manifest lacks structured transform provenance")
    for field, expected in expected_transform.items():
        if transform.get(field) != expected:
            raise ValueError(f"source export manifest has wrong transform {field}")
    context_provenance = manifest.get("endorsement_context_provenance")
    rows: dict[str, dict[str, Any]] = {}
    for item in manifest["chunks"]:
        path = export_dir / "chunks" / item["file"]
        if sha256_file(path) != item["sha256"]:
            raise ValueError(f"source chunk hash mismatch: {path}")
        chunk_rows = list(read_jsonl_gz(path))
        if int(item["records"]) != len(chunk_rows):
            raise ValueError(f"source chunk row count mismatch: {path}")
        for row in chunk_rows:
            document_id = str(row["document_id"])
            if document_id in rows:
                raise ValueError(f"duplicate source segment: {document_id}")
            if row.get("source_text_transform_version") != SOURCE_TEXT_TRANSFORM_VERSION:
                raise ValueError(f"{document_id}: source transform version differs")
            if not isinstance(row.get("source_text_transform_changed"), bool):
                raise ValueError(f"{document_id}: transform changed flag is absent")
            if not isinstance(row.get("candidate_icpsr"), str) or not row[
                "candidate_icpsr"
            ].strip():
                raise ValueError(f"{document_id}: candidate_icpsr is absent")
            if "candidate_name" in row:
                raise ValueError(f"{document_id}: ambiguous candidate_name field is prohibited")
            if row.get("route") == "endorsements":
                errors = endorsement_context_provenance_errors(
                    row.get("endorsement_context_provenance"),
                    segment_source_text=str(row["source_text"]),
                    segment_id=document_id,
                    segment_start_char=int(row["segment_start_char"]),
                    parent_document_id=str(row["parent_document_id"]),
                    source_urls=row.get("source_urls") or [],
                )
                if errors:
                    raise ValueError(
                        f"{document_id}: invalid endorsement context: "
                        + ", ".join(errors)
                    )
            elif "endorsement_context_provenance" in row:
                raise ValueError(
                    f"{document_id}: non-endorsement row has context provenance"
                )
            rows[document_id] = row
    if len(rows) != int(manifest["exported_records"]):
        raise ValueError("source export manifest count differs from its chunks")
    validate_endorsement_context_manifest(
        context_provenance,
        rows.values(),
        inherited_full_source_manifest_sha256=manifest.get(
            "source_export_manifest_sha256"
        ),
        source_corpus_sha256=manifest.get("source_corpus_sha256"),
    )
    transformed_parents = {
        str(row.get("parent_document_id") or row["document_id"])
        for row in rows.values()
        if row["source_text_transform_changed"]
    }
    if len(transformed_parents) != int(
        transform.get("transformed_exported_parent_documents", -1)
    ):
        raise ValueError("source export transformed-parent count differs")
    return manifest, rows


def load_outputs(output_dir: Path) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    paths = sorted(output_dir.glob("chunk_*.out.jsonl.gz"))
    if not paths:
        raise FileNotFoundError(f"no Hive output chunks under {output_dir}")
    for path in paths:
        for row in read_jsonl_gz(path):
            document_id = str(row["document_id"])
            if document_id in rows:
                raise ValueError(f"duplicate Hive output segment: {document_id}")
            rows[document_id] = row
    return rows


def frozen_run_metadata_identity() -> dict[str, Any]:
    """Return the run-metadata fields every run of this corpus must carry.

    The seed and the generation hash are not here, because a seed-vary repair
    changes exactly those two fields and keeps every other field identical.
    """
    return {
        "schema_version": SCHEMA_VERSION,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
        "prompt_sha256": CONTRACT["prompt_sha256"],
        "json_schema_sha256": CONTRACT["json_schema_sha256"],
        "extraction_contract_sha256": CONTRACT["extraction_contract_sha256"],
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
        "array_item_cap_spec_sha256": CONTRACT[
            "array_item_cap_spec_sha256"
        ],
        "route_array_item_caps": CONTRACT["route_array_item_caps"],
        "model_item_repetition_spec_id": CONTRACT[
            "model_item_repetition_spec_id"
        ],
        "model_item_repetition_spec_sha256": CONTRACT[
            "model_item_repetition_spec_sha256"
        ],
        "structured_decoding_backend": STRUCTURED_DECODING_BACKEND,
        "structured_decoding_disable_any_whitespace": (
            STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
        ),
    }


def validate_run_metadata(output_dir: Path) -> list[dict[str, Any]]:
    paths = sorted(output_dir.glob("run_metadata*.json"))
    if not paths:
        raise ValueError("Hive output lacks run metadata")
    metadata: list[dict[str, Any]] = []
    for path in paths:
        row = json.loads(path.read_text())
        expected = frozen_run_metadata_identity()
        # A 2.2 bundle is built by re-aligning a 2.1 inference run. Generation
        # is identical between the versions, so the run metadata legitimately
        # still declares 2.1 for the four version-bearing fields. Accepting the
        # 2.1 values keeps the provenance honest: no 2.2 inference run happened.
        realigned_from_2_1 = False
        if SCHEMA_VERSION == "2.2":
            source_identity = get_extraction_contract("2.1")
            inherited = {
                "schema_version": "2.1",
                "extraction_contract_sha256": source_identity[
                    "extraction_contract_sha256"
                ],
                "alignment_id": source_identity["alignment_id"],
                "alignment_spec_sha256": source_identity["alignment_spec_sha256"],
            }
            if all(
                _typed_equal(row.get(key), value)
                for key, value in inherited.items()
            ):
                realigned_from_2_1 = True
                expected = {
                    key: value
                    for key, value in expected.items()
                    if key not in inherited
                }
        for key, value in expected.items():
            if not _typed_equal(row.get(key), value):
                raise ValueError(f"{path.name}: wrong {key}")
        if realigned_from_2_1:
            row["realigned_from_schema_version"] = "2.1"
            row["realigned_to_schema_version"] = SCHEMA_VERSION
        if row.get("outputs_complete_for_selected_shard") is not True:
            raise ValueError(f"{path.name}: shard outputs are not attested complete")
        generation = row.get("generation") or {}
        expected_generation_block = {
            "temperature": 0.0,
            "thinking_enabled": False,
            "compact_repair": False,
            "seed": EXPECTED_GENERATION_SEED,
            "max_model_len": EXPECTED_MAX_MODEL_LEN,
            "max_output_tokens": EXPECTED_MAX_OUTPUT_TOKENS,
        }
        if any(
            not _typed_equal(generation.get(field), value)
            for field, value in expected_generation_block.items()
        ):
            raise ValueError(f"{path.name}: generation block differs from the frozen identity")
        generation_config = row.get("generation_config")
        generation_hash = str(row.get("generation_config_sha256") or "")
        validate_generation_config_identity(
            generation_config, generation_hash, path.name
        )

        validate_run_metadata_accounting(row, path.name)
        metadata.append({**row, "metadata_file": path.name, "metadata_sha256": sha256_file(path)})

    shard_counts = {row.get("shard_count") for row in metadata}
    if shard_counts == {None}:
        if (
            len(metadata) != 1
            or metadata[0].get("shard_index") is not None
            or metadata[0]["metadata_file"] != "run_metadata.json"
        ):
            raise ValueError("unsharded run metadata is inconsistent")
    else:
        if (
            len(shard_counts) != 1
            or None in shard_counts
            or any(
                not isinstance(value, int) or isinstance(value, bool)
                for value in shard_counts
            )
        ):
            raise ValueError("sharded run metadata disagrees on shard count")
        count = next(iter(shard_counts))
        indexes = [row.get("shard_index") for row in metadata]
        if (
            count <= 0
            or any(
                not isinstance(value, int) or isinstance(value, bool)
                for value in indexes
            )
            or set(indexes) != set(range(count))
            or any(
                row["metadata_file"]
                != f"run_metadata_{row['shard_index']:05d}.json"
                for row in metadata
            )
        ):
            raise ValueError("sharded run metadata does not cover every shard")
    generation_hashes = {str(row["generation_config_sha256"]) for row in metadata}
    generation_configs = {canonical_json(row["generation_config"]) for row in metadata}
    if len(generation_hashes) != 1 or len(generation_configs) != 1:
        raise ValueError("run metadata shards disagree on generation identity")
    return metadata


def load_repair_windows(
    manifest_path: Path,
    *,
    repair_output_dir: Path,
    export_chunks_dir: Path,
) -> dict[str, Any]:
    """Read a seed-vary repair attestation and check it against the artifacts.

    The return value carries the attestation metadata and one entry per affected
    window. Every declared value is checked against the frozen repair identity and
    against the files on disk, so an attestation cannot name a window the repair
    did not produce, or claim an outcome the published row does not hold.
    """
    if not manifest_path.is_file():
        raise FileNotFoundError(f"repair attestation is absent: {manifest_path}")
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("repair attestation must be an object")
    if payload.get("repair_manifest_version") != REPAIR_MANIFEST_VERSION:
        raise ValueError("repair attestation version differs")
    attempt_index = payload.get("attempt_index")
    if (
        not isinstance(attempt_index, int)
        or isinstance(attempt_index, bool)
        or attempt_index not in EXPECTED_REPAIR_GENERATION_SEEDS
    ):
        raise ValueError("repair attestation has an undeclared attempt index")
    repair_seed = EXPECTED_REPAIR_GENERATION_SEEDS[attempt_index]
    repair_generation_config = EXPECTED_REPAIR_GENERATION_CONFIGS[attempt_index]
    repair_generation_sha256 = EXPECTED_REPAIR_GENERATION_CONFIG_SHA256S[
        attempt_index
    ]
    expected_identity = {
        "schema_version": SCHEMA_VERSION,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
        "prompt_sha256": CONTRACT["prompt_sha256"],
        "json_schema_sha256": CONTRACT["json_schema_sha256"],
        "extraction_contract_sha256": CONTRACT["extraction_contract_sha256"],
        "max_model_len": EXPECTED_MAX_MODEL_LEN,
        "max_output_tokens": EXPECTED_MAX_OUTPUT_TOKENS,
        "primary_seed": EXPECTED_GENERATION_SEED,
        "repair_seed": repair_seed,
        "primary_generation_config_sha256": EXPECTED_GENERATION_CONFIG_SHA256,
        "repair_generation_config_sha256": repair_generation_sha256,
        "reason": REPAIR_REASON,
    }
    for key, value in expected_identity.items():
        if not _typed_equal(payload.get(key), value):
            raise ValueError(f"repair attestation has wrong {key}")
    entries = payload.get("targets")
    if not isinstance(entries, list) or not entries:
        raise ValueError("repair attestation names no targets")
    manifest_sha256 = sha256_file(manifest_path)
    windows: dict[str, dict[str, Any]] = {}
    repaired_count = 0
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("repair target entry is not an object")
        if set(entry) != REPAIR_TARGET_ENTRY_FIELDS:
            raise ValueError(
                "repair target entry fields differ: "
                f"{sorted(set(entry) ^ REPAIR_TARGET_ENTRY_FIELDS)}"
            )
        chunk = str(entry["chunk"])
        document_id = str(entry["document_id"])
        if (
            not chunk.startswith("chunk_")
            or not chunk.endswith(".jsonl.gz")
            or "/" in chunk
            or ".out." in chunk
        ):
            raise ValueError(f"unsafe repair target chunk name: {chunk!r}")
        if not HEX64.fullmatch(document_id):
            raise ValueError(
                f"repair target document id is not a digest: {document_id!r}"
            )
        if document_id in windows:
            raise ValueError(f"repeated repair target: {document_id}")
        output_file = chunk.replace(".jsonl.gz", ".out.jsonl.gz")
        if entry["repair_output_file"] != output_file:
            raise ValueError(f"{document_id}: repair output file name differs")
        input_path = export_chunks_dir / chunk
        if not input_path.is_file():
            raise ValueError(f"repair target chunk is absent from the export: {chunk}")
        if entry["input_chunk_sha256"] != sha256_file(input_path):
            raise ValueError(f"{document_id}: repair input chunk hash differs")
        output_path = repair_output_dir / output_file
        if not output_path.is_file():
            raise ValueError(f"repair output is absent: {output_path}")
        if entry["repair_output_sha256"] != sha256_file(output_path):
            raise ValueError(f"{document_id}: repair output hash differs")
        rows = {
            str(row.get("document_id")): row for row in read_jsonl_gz(output_path)
        }
        row = rows.get(document_id)
        if row is None:
            raise ValueError(f"{document_id}: repair output holds no such row")
        if str(row.get("route")) != str(entry["route"]):
            raise ValueError(f"{document_id}: repair route differs")
        for field in ("finish_reason", "parse_status", "output_tokens"):
            if not _typed_equal(row.get(field), entry[field]):
                raise ValueError(f"{document_id}: repair {field} differs")
        declared_hash = str(entry["generation_config_sha256"] or "")
        if not HEX64.fullmatch(declared_hash):
            raise ValueError(f"{document_id}: repair generation hash is absent")
        if declared_hash != str(row.get("generation_config_sha256") or ""):
            raise ValueError(f"{document_id}: repair generation hash differs")
        if declared_hash != repair_generation_sha256:
            raise ValueError(
                f"{document_id}: generation config is not the repair identity"
            )
        raw_output = row.get("raw_output")
        raw_hash = (
            hashlib.sha256(raw_output.encode("utf-8")).hexdigest()
            if isinstance(raw_output, str)
            else None
        )
        if entry["raw_output_sha256"] != raw_hash:
            raise ValueError(f"{document_id}: repair raw output hash differs")
        repaired = (
            str(entry["finish_reason"]) == "stop"
            and str(entry["parse_status"]) == "parsed"
        )
        if bool(entry["repaired"]) is not repaired:
            raise ValueError(f"{document_id}: repair acceptance flag differs")
        if repaired:
            repaired_count += 1
        elif (
            str(entry["finish_reason"]) != "length"
            or str(entry["parse_status"]) != TRUNCATED_PARSE_STATUS
        ):
            raise ValueError(
                f"{document_id}: unrepaired repair row is not cap-limited"
            )
        windows[document_id] = {
            "chunk": chunk,
            "repair_output_file": output_file,
            "repair_output_sha256": str(entry["repair_output_sha256"]),
            "route": str(entry["route"]),
            "repaired": repaired,
            "row": row,
            "attempt_index": attempt_index,
            "repair_seed": repair_seed,
            "expected_generation_config_sha256": repair_generation_sha256,
            "expected_generation_config": repair_generation_config,
            "manifest_sha256": manifest_sha256,
        }
    if int(payload.get("target_count", -1)) != len(entries):
        raise ValueError("repair attestation target count differs")
    if int(payload.get("repaired_count", -1)) != repaired_count:
        raise ValueError("repair attestation repaired count differs")
    if int(payload.get("unrepaired_count", -1)) != len(entries) - repaired_count:
        raise ValueError("repair attestation unrepaired count differs")
    if payload.get("all_targets_repaired") is not (repaired_count == len(entries)):
        raise ValueError("repair attestation completion flag differs")
    return {
        "manifest_path": str(manifest_path),
        "manifest_sha256": manifest_sha256,
        "target_count": len(entries),
        "repaired_count": repaired_count,
        "unrepaired_count": len(entries) - repaired_count,
        "attempt_index": attempt_index,
        "repair_seed": repair_seed,
        "generation_config_sha256": repair_generation_sha256,
        "generation_config": repair_generation_config,
        "reason": REPAIR_REASON,
        "windows": windows,
    }


def load_repair_run_metadata(
    repair_output_dir: Path,
    *,
    attempt_index: int,
    target_count: int,
) -> dict[str, Any]:
    """Read and check one repair attempt's own run metadata.

    A repair attempt can span more than one Slurm job, because a job can reach its
    wall limit and a later job resumes the rest. The runner writes
    `run_metadata.json` when a job completes its selection, so the file records the
    job that finished the attempt: the `*_this_run` counters cover that final job.
    Two fields cover the whole attempt. `documents_selected_for_shard` is the size
    of the target list, and `outputs_complete_for_selected_shard` attests that every
    selected window has an output. The window-level record for the attempt is the
    repair attestation, which `load_repair_windows` checks against the artifacts.

    The identity check is the point of this function. The metadata must carry the
    frozen identity in every field except the seed, and it must carry the derived
    repair identity for this attempt in the seed and the generation hash.
    """
    if (
        not isinstance(attempt_index, int)
        or isinstance(attempt_index, bool)
        or attempt_index not in EXPECTED_REPAIR_GENERATION_SEEDS
    ):
        raise ValueError("repair run metadata has an undeclared attempt index")
    path = Path(repair_output_dir) / "run_metadata.json"
    if not path.is_file():
        raise ValueError(
            f"repair attempt {attempt_index} output lacks {path.name}"
        )
    label = f"repair attempt {attempt_index} {path.name}"
    row = json.loads(path.read_text())
    if not isinstance(row, dict):
        raise ValueError(f"{label}: run metadata must be a JSON object")
    for key, value in frozen_run_metadata_identity().items():
        if not _typed_equal(row.get(key), value):
            raise ValueError(f"{label}: wrong {key}")

    repair_seed = EXPECTED_REPAIR_GENERATION_SEEDS[attempt_index]
    repair_generation_sha256 = EXPECTED_REPAIR_GENERATION_CONFIG_SHA256S[
        attempt_index
    ]
    expected_generation_block = {
        "temperature": 0.0,
        "thinking_enabled": False,
        "compact_repair": False,
        "seed": repair_seed,
        "max_model_len": EXPECTED_MAX_MODEL_LEN,
        "max_output_tokens": EXPECTED_MAX_OUTPUT_TOKENS,
    }
    generation = row.get("generation") or {}
    if any(
        not _typed_equal(generation.get(field), value)
        for field, value in expected_generation_block.items()
    ):
        raise ValueError(f"{label}: generation block differs from the repair identity")
    generation_hash = str(row.get("generation_config_sha256") or "")
    generation_config = row.get("generation_config")
    if not isinstance(generation_config, dict) or not HEX64.fullmatch(generation_hash):
        raise ValueError(f"{label}: generation config provenance is absent")
    if sha256_text(canonical_json(generation_config)) != generation_hash:
        raise ValueError(f"{label}: generation config hash differs")
    if generation_hash != repair_generation_sha256:
        raise ValueError(f"{label}: generation identity is not attempt {attempt_index}")
    expected_config = EXPECTED_REPAIR_GENERATION_CONFIGS[attempt_index]
    if not _typed_equal(generation_config, expected_config):
        differing = sorted(
            set(generation_config) ^ set(expected_config)
            | {
                key
                for key in set(generation_config) & set(expected_config)
                if not _typed_equal(generation_config[key], expected_config[key])
            }
        )
        raise ValueError(
            f"{label}: complete repair generation config differs: {differing}"
        )
    if row.get("shard_index") is not None or row.get("shard_count") is not None:
        raise ValueError(f"{label}: a repair run does not shard")
    if row.get("recovery_attestation_without_inference") is not False:
        raise ValueError(f"{label}: the repair ran inference, so the flag must be false")
    if row.get("outputs_complete_for_selected_shard") is not True:
        raise ValueError(f"{label}: repair outputs are not attested complete")
    selected = row.get("documents_selected_for_shard")
    if (
        not isinstance(selected, int)
        or isinstance(selected, bool)
        or selected != int(target_count)
    ):
        raise ValueError(
            f"{label}: selected windows {selected!r} differ from the "
            f"target count {int(target_count)}"
        )
    validate_run_metadata_accounting(row, label)
    return {
        **row,
        "metadata_file": path.name,
        "metadata_sha256": sha256_file(path),
        "attempt_index": int(attempt_index),
    }


def last_attempt_unrepaired_documents(
    attempts: Iterable[Mapping[str, Any]]
) -> set[str]:
    """Return the windows the last repair attempt left at the output cap.

    A later attempt is scoped to what the previous one left, so the last
    attempt's unrepaired set is the surviving set. With no attempt the set is
    empty, and every cap-limited row is then unattested.
    """
    ordered = sorted(attempts, key=lambda attempt: int(attempt["attempt_index"]))
    if not ordered:
        return set()
    return {
        document_id
        for document_id, window in ordered[-1]["windows"].items()
        if not window["repaired"]
    }


def _repair_run_metadata_entry(row: Mapping[str, Any]) -> dict[str, Any]:
    """Return the bundle entry for one repair attempt's final-job run metadata."""
    return {
        "file": row["metadata_file"],
        "sha256": row["metadata_sha256"],
        "attempt_index": int(row["attempt_index"]),
        "generation_config_sha256": row["generation_config_sha256"],
        "seed": row["generation"]["seed"],
        "documents_selected_for_shard": row["documents_selected_for_shard"],
        "outputs_complete_for_selected_shard": row[
            "outputs_complete_for_selected_shard"
        ],
        "structured_decoding_backend": row["structured_decoding_backend"],
        "structured_decoding_disable_any_whitespace": row[
            "structured_decoding_disable_any_whitespace"
        ],
        "array_item_cap_spec_id": row["array_item_cap_spec_id"],
        "array_item_cap_spec_sha256": row["array_item_cap_spec_sha256"],
        "route_array_item_caps": row["route_array_item_caps"],
        "model_item_repetition_spec_id": row["model_item_repetition_spec_id"],
        "model_item_repetition_spec_sha256": row[
            "model_item_repetition_spec_sha256"
        ],
        "recovery_attestation_without_inference": row[
            "recovery_attestation_without_inference"
        ],
        **{field: row[field] for field in RUN_METADATA_SCALAR_FIELDS},
        **{field: row[field] for field in RUN_METADATA_MAPPING_FIELDS},
        **{field: row[field] for field in RUN_METADATA_MAX_MAPPING_FIELDS},
    }


def apply_repair_attempts(
    outputs: dict[str, dict[str, Any]],
    attempts: Iterable[Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    """Apply every repair attempt in order and return the windows they filled.

    A later attempt covers the windows an earlier one left at the cap, so applying
    them in ascending attempt order gives each window its latest recovered row. A
    window no attempt recovered keeps its primary row.
    """
    ordered = sorted(attempts, key=lambda attempt: int(attempt["attempt_index"]))
    seen: set[int] = set()
    applied: dict[str, Mapping[str, Any]] = {}
    for attempt in ordered:
        index = int(attempt["attempt_index"])
        if index in seen:
            raise ValueError(f"repeated repair attempt index: {index}")
        seen.add(index)
        filled = apply_repair_windows(outputs, attempt["windows"])
        applied.update(filled)
    return applied


def apply_repair_windows(
    outputs: dict[str, dict[str, Any]],
    windows: Mapping[str, Mapping[str, Any]],
) -> dict[str, Mapping[str, Any]]:
    """Swap in the repair row for every window the repair recovered.

    The primary row stays on disk untouched and an attempted window that reached
    the cap again keeps its primary row. The attestation records both outcomes.
    """
    applied: dict[str, Mapping[str, Any]] = {}
    for document_id, window in windows.items():
        if document_id not in outputs:
            raise ValueError(
                f"repair target is absent from the Hive output: {document_id}"
            )
        if not window["repaired"]:
            continue
        outputs[document_id] = dict(window["row"])
        applied[document_id] = window
    return applied


def validate_run_metadata_source_coverage(
    source_manifest: Mapping[str, Any],
    metadata: Iterable[Mapping[str, Any]],
) -> None:
    """Bind each shard's selected-document count to exact export chunks."""
    source_chunks = sorted(
        source_manifest["chunks"], key=lambda item: str(item["file"])
    )
    for row in metadata:
        shard_index = row.get("shard_index")
        shard_count = row.get("shard_count")
        selected_chunks = (
            source_chunks
            if shard_index is None
            else [
                item for index, item in enumerate(source_chunks)
                if index % int(shard_count) == int(shard_index)
            ]
        )
        expected_selected = sum(int(item["records"]) for item in selected_chunks)
        if int(row["documents_selected_for_shard"]) != expected_selected:
            raise ValueError(
                f"{row['metadata_file']}: selected-document count differs "
                "from its source chunks"
            )


def reconstruct_parent_text(rows: list[dict[str, Any]]) -> str:
    ordered = sorted(rows, key=lambda row: int(row["segment_index"]))
    count = int(ordered[0]["segment_count"])
    if len(ordered) != count or {int(row["segment_index"]) for row in ordered} != set(range(count)):
        raise ValueError(f"incomplete segment coverage for {ordered[0]['parent_document_id']}")
    size = max(int(row["segment_end_char"]) for row in ordered)
    chars: list[str | None] = [None] * size
    for row in ordered:
        start = int(row["segment_start_char"])
        end = int(row["segment_end_char"])
        source = str(row["source_text"])
        if end - start != len(source):
            raise ValueError(f"segment bounds differ from text length: {row['document_id']}")
        if sha256_text(source) != row["segment_text_sha256"]:
            raise ValueError(f"segment text hash mismatch: {row['document_id']}")
        for offset, char in enumerate(source):
            position = start + offset
            if chars[position] is not None and chars[position] != char:
                raise ValueError(f"segment overlap text mismatch: {row['parent_document_id']}")
            chars[position] = char
    if any(char is None for char in chars):
        raise ValueError(f"segment gap for {ordered[0]['parent_document_id']}")
    parent = "".join(char for char in chars if char is not None)
    if "extraction_source_text_sha256" not in ordered[0]:
        raise ValueError(
            f"source export lacks extraction text hash: {ordered[0]['parent_document_id']}"
        )
    if sha256_text(parent) != ordered[0]["extraction_source_text_sha256"]:
        raise ValueError(
            f"reconstructed extraction-source hash mismatch: {ordered[0]['parent_document_id']}"
        )
    if len(parent) != int(ordered[0].get("extraction_source_n_chars", -1)):
        raise ValueError(
            f"reconstructed extraction-source length mismatch: {ordered[0]['parent_document_id']}"
        )
    return parent


def validate_parent_sources(inputs: Mapping[str, dict[str, Any]]) -> dict[str, str]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in inputs.values():
        grouped[str(row.get("parent_document_id") or row["document_id"])].append(row)
    parents: dict[str, str] = {}
    for parent_id, rows in grouped.items():
        routes = {str(row["route"]) for row in rows}
        hashes = {str(row["raw_text_sha256"]) for row in rows}
        if len(routes) != 1 or len(hashes) != 1:
            raise ValueError(f"parent source metadata conflict: {parent_id}")
        parent_text = reconstruct_parent_text(rows)
        if next(iter(routes)) == "endorsements":
            for row in rows:
                errors = endorsement_context_provenance_errors(
                    row.get("endorsement_context_provenance"),
                    parent_source_text=parent_text,
                    segment_source_text=str(row["source_text"]),
                    segment_id=str(row["document_id"]),
                    segment_start_char=int(row["segment_start_char"]),
                    parent_document_id=parent_id,
                    source_urls=row.get("source_urls") or [],
                )
                if errors:
                    raise ValueError(
                        f"{row['document_id']}: endorsement parent context differs: "
                        + ", ".join(errors)
                    )
        parents[parent_id] = parent_text
    return parents


def validate_output_row(
    source: dict[str, Any],
    output: dict[str, Any],
    expected_generation_config_sha256: str | None = None,
    expected_generation_config: Mapping[str, Any] | None = None,
    parent_source_text: str | None = None,
    *,
    repair_identity: bool = False,
) -> tuple[dict[str, Any], str]:
    """Validate one output row against its source row.

    `repair_identity` admits the declared seed-vary repair identity as an
    alternative to the frozen primary identity. The caller sets it only for the
    windows a checked repair attestation names, and it passes the repair identity
    as the expected value, so the row is still bound to one exact hash.
    """
    document_id = str(source["document_id"])
    copied_fields = (
        "document_id",
        "candidate_cycle_id",
        "route",
        "raw_text_sha256",
        "parent_document_id",
        "segment_index",
        "segment_count",
        "segment_start_char",
        "segment_end_char",
        "segment_text_sha256",
    )
    for field in copied_fields:
        if not _typed_equal(output.get(field), source.get(field)):
            raise ValueError(f"{document_id}: output {field} differs from source")
    route = str(source["route"])
    context_provenance = (
        source.get("endorsement_context_provenance")
        if route == "endorsements"
        else None
    )
    if not _typed_equal(
        output.get("endorsement_context_provenance"), context_provenance
    ):
        raise ValueError(
            f"{document_id}: output endorsement context differs from source"
        )
    expected_output_fields = set(OUTPUT_COMMON_FIELDS)
    if route == "endorsements":
        expected_output_fields.add("endorsement_context_provenance")
    parse_status = str(output.get("parse_status") or "")
    if parse_status == "parsed":
        expected_output_fields.update(OUTPUT_SUCCESS_FIELDS)
    elif parse_status in GUARD_FAILURE_PARSE_STATUSES:
        expected_output_fields.update(OUTPUT_PARSED_GUARD_FIELDS)
        expected_output_fields.add("extraction")
    else:
        expected_output_fields.add("extraction")
    if set(output) != expected_output_fields:
        raise ValueError(
            f"{document_id}: output fields differ: "
            f"{sorted(set(output) ^ expected_output_fields)}"
        )
    expected = {
        "schema_version": SCHEMA_VERSION,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
        "prompt_sha256": CONTRACT["prompt_sha256"][str(source["route"])],
        "json_schema_sha256": CONTRACT["json_schema_sha256"][str(source["route"])],
        "extraction_contract_sha256": CONTRACT["extraction_contract_sha256"],
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
        "route_array_item_cap": CONTRACT["route_array_item_caps"][route],
        "model_item_repetition_spec_id": CONTRACT[
            "model_item_repetition_spec_id"
        ],
        "model_item_repetition_spec_sha256": CONTRACT[
            "model_item_repetition_spec_sha256"
        ],
        "structured_decoding_backend": STRUCTURED_DECODING_BACKEND,
        "structured_decoding_disable_any_whitespace": (
            STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
        ),
        "temperature": 0.0,
        "thinking_enabled": False,
        "compact_repair": False,
    }
    for field, value in expected.items():
        if not _typed_equal(output.get(field), value):
            raise ValueError(f"{document_id}: unexpected {field}")
    generation_hash = str(output.get("generation_config_sha256") or "")
    if not HEX64.fullmatch(generation_hash):
        raise ValueError(f"{document_id}: generation config hash is absent")
    allowed_generation_hashes = {EXPECTED_GENERATION_CONFIG_SHA256}
    if repair_identity:
        allowed_generation_hashes.update(
            EXPECTED_REPAIR_GENERATION_CONFIG_SHA256S.values()
        )
    if generation_hash not in allowed_generation_hashes:
        raise ValueError(f"{document_id}: generation config is not the frozen v2.1 identity")
    if (
        expected_generation_config_sha256 is not None
        and generation_hash != expected_generation_config_sha256
    ):
        raise ValueError(f"{document_id}: generation config differs from run metadata")
    finish_reason = str(output.get("finish_reason") or "")
    if not finish_reason:
        raise ValueError(f"{document_id}: generation finish reason is absent")
    stop_reason = output.get("stop_reason")
    if stop_reason is not None and (
        not isinstance(stop_reason, str) or not stop_reason
    ):
        raise ValueError(f"{document_id}: stop reason is not null or text")
    input_truncated = output.get("input_truncated")
    input_chars_used = output.get("input_chars_used")
    input_tokens_used = output.get("input_tokens_used")
    if (
        not isinstance(input_truncated, bool)
        or not isinstance(input_chars_used, int)
        or isinstance(input_chars_used, bool)
        or not 0 <= input_chars_used <= len(str(source["source_text"]))
        or not isinstance(input_tokens_used, int)
        or isinstance(input_tokens_used, bool)
        or input_tokens_used <= 0
    ):
        raise ValueError(f"{document_id}: input token or character accounting is invalid")
    output_tokens = output.get("output_tokens")
    max_output_tokens = output.get("max_output_tokens")
    if (
        not isinstance(output_tokens, int)
        or isinstance(output_tokens, bool)
        or output_tokens < 1
        or not isinstance(max_output_tokens, int)
        or isinstance(max_output_tokens, bool)
        or max_output_tokens != EXPECTED_MAX_OUTPUT_TOKENS
        or output_tokens > max_output_tokens
    ):
        raise ValueError(f"{document_id}: output token accounting is invalid")
    if (
        expected_generation_config is not None
        and max_output_tokens
        != int(expected_generation_config.get("max_output_tokens", -1))
    ):
        raise ValueError(f"{document_id}: output token budget differs from run metadata")

    parsed_model_extraction, _ = validate_private_output_provenance(
        output,
        str(source["source_text"]),
        document_id,
        source_record=source,
    )
    alignment_status = None
    if parse_status == "parsed":
        aligned, withheld, alignment = align_extraction_exact_spans(
            route,
            parsed_model_extraction,
            str(source["source_text"]),
            endorsement_context_provenance=context_provenance,
            segment_start_char=int(source["segment_start_char"]),
            schema_version=SCHEMA_VERSION,
        )
        alignment_status = str(alignment["status"])
        expected_alignment_values = {
            "extraction": aligned,
            "withheld_model_items": withheld,
            "exact_alignment": alignment,
            "model_extraction_sha256": alignment[
                "original_model_extraction_sha256"
            ],
            "aligned_extraction_sha256": alignment[
                "aligned_extraction_sha256"
            ],
        }
        for field, value in expected_alignment_values.items():
            if not _typed_equal(output.get(field), value):
                raise ValueError(
                    f"{document_id}: stored {field} differs from deterministic alignment"
                )
        validation = validate_extraction(
            route,
            output.get("extraction"),
            str(source["source_text"]),
            SCHEMA_VERSION,
            endorsement_context_provenance=context_provenance,
            segment_start_char=int(source["segment_start_char"]),
            parent_source_text=parent_source_text,
        )
    else:
        validation = {
            "schema_valid": False,
            "n_items": 0,
            "n_supported": 0,
            "evidence_support_rate": None,
            "n_required_anchors": 0,
            "n_exact_anchors": 0,
            "exact_anchor_rate": None,
            "status": str(output.get("parse_status") or "missing_parse_status"),
        }
    if output.get("validation") is not None and not _typed_equal(
        output["validation"], validation
    ):
        raise ValueError(f"{document_id}: stored validation differs from recomputation")

    full_input = (
        output.get("input_truncated") is False
        and int(output.get("input_chars_used", -1)) == len(str(source["source_text"]))
    )
    exact = validation["n_items"] == validation["n_supported"]
    if (
        parse_status == "parsed"
        and validation["schema_valid"]
        and exact
        and full_input
        and alignment_status == "complete"
    ):
        status = "processed_success"
    elif output.get("input_truncated") is True:
        status = "failed_truncated_input"
    elif parse_status == ARRAY_ITEM_CAP_REACHED_STATUS:
        status = "failed_array_item_cap_reached"
    elif parse_status == MODEL_ITEM_REPETITION_ERROR_STATUS:
        status = "failed_model_item_repetition_error"
    elif (
        parse_status == TRUNCATED_PARSE_STATUS
        and str(output.get("finish_reason") or "") == "length"
    ):
        # Greedy decoding repeated one item until it reached the output cap, so
        # the answer is cut off mid-JSON. Decoding is deterministic, so an
        # identical rerun returns identical bytes; only a seed change can break
        # the loop. A window that every approved repair attempt left at the cap
        # is withheld under its own status rather than counted as a parse
        # failure, so the release can state and count the class.
        status = OUTPUT_CAP_WITHHELD_STATUS
    elif parse_status != "parsed":
        status = "failed_parse"
    elif alignment_status == "partial_withheld":
        status = "processed_partial_withheld"
    elif alignment_status == "all_withheld":
        status = "processed_all_withheld"
    elif alignment_status == "invalid_schema":
        status = "failed_schema"
    elif not validation["schema_valid"]:
        status = "failed_schema"
    else:
        status = "failed_exact_span"
    return validation, status


def normalized_identity(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip().casefold()


def semantic_payload(route: str, item: Mapping[str, Any]) -> dict[str, Any]:
    return {field: item.get(field) for field in SEMANTIC_FIELDS[route]}


def parent_offsets(route: str, item: Mapping[str, Any], segment_start: int) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for text_field, start_field, end_field in ANCHORS[route]:
        value = item.get(text_field)
        if route == "endorsements" and text_field == (
            "endorsement_context_evidence"
        ):
            scope = item.get("endorsement_context_evidence_source_scope")
            parent_start = item.get(
                "endorsement_context_evidence_parent_start_char"
            )
            parent_end = item.get(
                "endorsement_context_evidence_parent_end_char"
            )
            if scope == "segment":
                computed_start, computed_end = parent_char_span(
                    segment_start, int(item[start_field]), int(item[end_field])
                )
                if (parent_start, parent_end) != (
                    computed_start, computed_end
                ):
                    raise ValueError(
                        "segment endorsement context parent offsets differ"
                    )
            elif scope == "parent_document":
                if (
                    value is None
                    or item.get(start_field) is not None
                    or item.get(end_field) is not None
                    or not isinstance(parent_start, int)
                    or isinstance(parent_start, bool)
                    or not isinstance(parent_end, int)
                    or isinstance(parent_end, bool)
                    or parent_start < 0
                    or parent_end <= parent_start
                ):
                    raise ValueError(
                        "parent endorsement context offsets are invalid"
                    )
            elif scope == "page_or_section_route_context":
                if any(
                    candidate is not None
                    for candidate in (
                        value, item.get(start_field), item.get(end_field),
                        parent_start, parent_end,
                    )
                ):
                    raise ValueError(
                        "URL-route endorsement context has text offsets"
                    )
            else:
                raise ValueError("endorsement context source scope is invalid")
            result[f"{text_field}_parent_start_char"] = parent_start
            result[f"{text_field}_parent_end_char"] = parent_end
            continue
        if value is None:
            result[f"{text_field}_parent_start_char"] = None
            result[f"{text_field}_parent_end_char"] = None
            continue
        start, end = parent_char_span(
            segment_start, int(item[start_field]), int(item[end_field])
        )
        result[f"{text_field}_parent_start_char"] = start
        result[f"{text_field}_parent_end_char"] = end
    return result


def evidence_length(route: str, item: Mapping[str, Any]) -> int:
    return sum(len(str(item.get(text_field) or "")) for text_field, _, _ in ANCHORS[route])


def _resolution_preference(
    route: str, variant: Mapping[str, Any]
) -> tuple[Any, ...]:
    """Return the documented deterministic preference within one semantic group."""
    item = variant["item"]
    if route == "issues":
        heading = item.get("source_heading")
        valid_exact_heading = (
            isinstance(heading, str)
            and bool(heading.strip())
            and isinstance(item.get("source_heading_start_char"), int)
            and not isinstance(item.get("source_heading_start_char"), bool)
            and isinstance(item.get("source_heading_end_char"), int)
            and not isinstance(item.get("source_heading_end_char"), bool)
            and item.get("source_heading_exact_match_count") == 1
            and item.get("source_heading_alignment_method")
            == ALIGNMENT_METHOD_UNIQUE
        )
        # A validated exact heading is more informative than null. An invalid
        # non-null heading is never preferred over the safe null provenance.
        heading_rank = 0 if valid_exact_heading else 1 if heading is None else 2
    else:
        heading_rank = 0
    return (
        heading_rank,
        evidence_length(route, item),
        int(variant["segment_index"]),
        str(variant["segment_id"]),
        int(variant["item_index"]),
    )


def _positive_interval_overlap(
    first_start: int, first_end: int, second_start: int, second_end: int
) -> bool:
    return max(first_start, second_start) < min(first_end, second_end)


def _cross_window_evidence_overlap(
    first: Mapping[str, Any], second: Mapping[str, Any]
) -> bool:
    """Identify duplicate candidates without using generated semantics.

    Two items are candidates for overlap resolution only when they came from
    different, overlapping source windows and their primary evidence intervals
    overlap in parent-text coordinates. Items from one segment remain distinct.
    """
    if first["segment_id"] == second["segment_id"]:
        return False
    if not _positive_interval_overlap(
        int(first["segment_start_char"]), int(first["segment_end_char"]),
        int(second["segment_start_char"]), int(second["segment_end_char"]),
    ):
        return False
    return _positive_interval_overlap(
        int(first["primary_evidence_parent_start_char"]),
        int(first["primary_evidence_parent_end_char"]),
        int(second["primary_evidence_parent_start_char"]),
        int(second["primary_evidence_parent_end_char"]),
    )


def _assign_overlap_groups(
    parent_id: str, route: str, variants: list[dict[str, Any]]
) -> None:
    """Assign connected overlap components using only source coordinates."""
    parents = list(range(len(variants)))

    def find(index: int) -> int:
        while parents[index] != index:
            parents[index] = parents[parents[index]]
            index = parents[index]
        return index

    def union(first: int, second: int) -> None:
        first_root, second_root = find(first), find(second)
        if first_root != second_root:
            parents[max(first_root, second_root)] = min(first_root, second_root)

    for first_index, first in enumerate(variants):
        for second_index in range(first_index + 1, len(variants)):
            if _cross_window_evidence_overlap(first, variants[second_index]):
                union(first_index, second_index)

    components: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for index, variant in enumerate(variants):
        components[find(index)].append(variant)
    for component in components.values():
        coordinate_members = sorted(
            (
                str(value["segment_id"]),
                int(value["item_index"]),
                int(value["primary_evidence_parent_start_char"]),
                int(value["primary_evidence_parent_end_char"]),
            )
            for value in component
        )
        group_id = sha256_text(canonical_json({
            "parent_document_id": parent_id,
            "route": route,
            "coordinate_members": coordinate_members,
            "grouping_rule": "cross_window_overlapping_parent_evidence_v1",
        }))
        for value in component:
            value["overlap_group_id"] = group_id
            value["overlap_grouping_rule"] = (
                "cross_window_overlapping_parent_evidence_v1"
            )


def build_record_variants(segments: list[dict[str, Any]]) -> list[dict[str, Any]]:
    variants: list[dict[str, Any]] = []
    parent_ids = {str(segment["parent_document_id"]) for segment in segments}
    routes = {str(segment["route"]) for segment in segments}
    if len(parent_ids) > 1 or len(routes) > 1:
        raise ValueError("record variants must be built for one parent and route")
    for segment in segments:
        if segment["processing_status"] not in {
            "processed_success",
            "processed_partial_withheld",
            "processed_all_withheld",
        }:
            continue
        route = str(segment["route"])
        items = (segment.get("extraction") or {})[LIST_KEYS[route]]
        for item_index, item in enumerate(items):
            variant_id = sha256_text(
                canonical_json(
                    {
                        "segment_id": segment["document_id"],
                        "item_index": item_index,
                        "item": item,
                    }
                )
            )
            offsets = parent_offsets(route, item, int(segment["segment_start_char"]))
            evidence_field = PRIMARY_EVIDENCE_FIELD[route]
            variants.append(
                {
                    "variant_id": variant_id,
                    "segment_id": segment["document_id"],
                    "segment_index": int(segment["segment_index"]),
                    "segment_start_char": int(segment["segment_start_char"]),
                    "segment_end_char": int(segment["segment_end_char"]),
                    "item_index": item_index,
                    "item": item,
                    "parent_offsets": offsets,
                    "primary_evidence_parent_start_char": offsets[
                        f"{evidence_field}_parent_start_char"
                    ],
                    "primary_evidence_parent_end_char": offsets[
                        f"{evidence_field}_parent_end_char"
                    ],
                    "semantic_payload_sha256": sha256_text(
                        canonical_json(semantic_payload(route, item))
                    ),
                    "exact_item_sha256": sha256_text(canonical_json(item)),
                }
            )
    if variants:
        _assign_overlap_groups(next(iter(parent_ids)), next(iter(routes)), variants)
    return variants


def resolve_parent_variants(
    parent_id: str,
    route: str,
    variants: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    groups: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for variant in variants:
        groups[str(variant["overlap_group_id"])].append(variant)
    resolved: list[dict[str, Any]] = []
    annotated: list[dict[str, Any]] = []
    for group_id, group in sorted(groups.items()):
        semantic_hashes = {str(value["semantic_payload_sha256"]) for value in group}
        if len(semantic_hashes) != 1:
            status = "conflict_withheld"
            method = "withheld_distinct_semantic_payloads"
            selected = None
        else:
            ordered = sorted(group, key=lambda value: _resolution_preference(route, value))
            selected = ordered[0]
            exact_hashes = {str(value["exact_item_sha256"]) for value in group}
            if len(group) == 1:
                status = "single_variant"
                method = "only_variant"
            elif len(exact_hashes) == 1:
                status = "equivalent_overlap"
                method = "stable_segment_key_among_exact_equivalents"
            else:
                status = "equivalent_semantics_distinct_exact_evidence"
                method = (
                    "prefer_exact_source_heading_then_shortest_exact_evidence_"
                    "then_stable_segment_key"
                    if route == "issues"
                    else "shortest_exact_evidence_then_stable_segment_key"
                )

        source_segments = sorted({str(value["segment_id"]) for value in group})
        for value in group:
            annotated.append(
                {
                    **value,
                    "overlap_resolution_status": status,
                    "overlap_resolution_method": method,
                    "group_release_eligible": selected is not None,
                    "selected_as_canonical": (
                        selected is not None and value["variant_id"] == selected["variant_id"]
                    ),
                }
            )
        if selected is None:
            continue
        record_id = sha256_text(
            canonical_json(
                {
                    "parent_document_id": parent_id,
                    "route": route,
                    "overlap_group_id": group_id,
                    "semantic_payload": semantic_payload(route, selected["item"]),
                }
            )
        )
        resolved.append(
            {
                "record_id": record_id,
                "parent_document_id": parent_id,
                "route": route,
                "canonical_segment_id": selected["segment_id"],
                "canonical_variant_id": selected["variant_id"],
                "all_source_segment_ids": source_segments,
                "all_variant_ids": sorted(str(value["variant_id"]) for value in group),
                "variant_count": len(group),
                "resolution_status": status,
                "resolution_method": method,
                "item": selected["item"],
                "parent_offsets": selected["parent_offsets"],
            }
        )
    return annotated, resolved


def enrich_segments(
    inputs: dict[str, dict[str, Any]],
    outputs: dict[str, dict[str, Any]],
    expected_generation_config_sha256: str | None = None,
    expected_generation_config: Mapping[str, Any] | None = None,
    *,
    repair_windows: Mapping[str, Mapping[str, Any]] | None = None,
) -> tuple[list[dict[str, Any]], dict[str, str]]:
    """Attach each output row to its source segment.

    `repair_windows` names the windows that checked repair attestations cover.
    Each one carries the identity of the attempt that produced its row, so the row
    is validated against that identity and the segment records the attempt.
    """
    for window in (repair_windows or {}).values():
        if (
            window.get("expected_generation_config_sha256") is None
            or window.get("expected_generation_config") is None
            or window.get("manifest_sha256") is None
        ):
            raise ValueError("a repair window lacks its attempt identity")
    missing = set(inputs) - set(outputs)
    extra = set(outputs) - set(inputs)
    if missing or extra:
        raise ValueError(
            f"source/output coverage differs: missing={len(missing)}, extra={len(extra)}"
        )
    parent_text = validate_parent_sources(inputs)
    enriched: list[dict[str, Any]] = []
    for document_id in sorted(inputs):
        source = inputs[document_id]
        output = outputs[document_id]
        window = (repair_windows or {}).get(document_id)
        repaired = window is not None and bool(window["repaired"])
        row_generation_sha256 = expected_generation_config_sha256
        row_generation_config = expected_generation_config
        if repaired and window is not None:
            row_generation_sha256 = window["expected_generation_config_sha256"]
            row_generation_config = window["expected_generation_config"]
        validation, status = validate_output_row(
            source,
            output,
            row_generation_sha256,
            row_generation_config,
            parent_text[str(source["parent_document_id"])],
            repair_identity=repaired,
        )
        enriched.append(
            {
                **source,
                "extraction": output.get("extraction"),
                "aligned_extraction_sha256": output.get(
                    "aligned_extraction_sha256"
                ),
                "model_extraction": output.get("model_extraction"),
                "model_extraction_sha256": output.get(
                    "model_extraction_sha256"
                ),
                "pre_alignment_model_item_guard": output.get(
                    "pre_alignment_model_item_guard"
                ),
                "array_item_cap_report": output.get("array_item_cap_report"),
                "model_item_repetition_report": output.get(
                    "model_item_repetition_report"
                ),
                "withheld_model_items": output.get("withheld_model_items") or [],
                "exact_alignment": output.get("exact_alignment"),
                "raw_output": output.get("raw_output"),
                **{
                    field: output[field]
                    for field in OUTPUT_INFERENCE_DISPLAY_FIELDS
                },
                **{
                    field: output[field]
                    for field in OUTPUT_RAW_STRUCTURAL_WHITESPACE_FIELDS
                },
                **{
                    field: output[field]
                    for field in OUTPUT_CANONICALIZATION_FIELDS
                },
                "parse_status": output.get("parse_status"),
                "processing_status": status,
                "validation": validation,
                "model_id": output["model_id"],
                "model_revision": output["model_revision"],
                "runtime_lock_sha256": output["runtime_lock_sha256"],
                "schema_version": output["schema_version"],
                "prompt_sha256": output["prompt_sha256"],
                "json_schema_sha256": output["json_schema_sha256"],
                "extraction_contract_sha256": output["extraction_contract_sha256"],
                "alignment_id": output["alignment_id"],
                "alignment_spec_sha256": output["alignment_spec_sha256"],
                "semantic_validation_spec_sha256": output[
                    "semantic_validation_spec_sha256"
                ],
                "endorsement_context_provenance_spec_sha256": output[
                    "endorsement_context_provenance_spec_sha256"
                ],
                "user_message_template_id": output[
                    "user_message_template_id"
                ],
                "user_message_template_sha256": output[
                    "user_message_template_sha256"
                ],
                "user_message_sha256": output["user_message_sha256"],
                "array_item_cap_spec_id": output["array_item_cap_spec_id"],
                "array_item_cap_spec_sha256": output[
                    "array_item_cap_spec_sha256"
                ],
                "route_array_item_cap": output["route_array_item_cap"],
                "model_item_repetition_spec_id": output[
                    "model_item_repetition_spec_id"
                ],
                "model_item_repetition_spec_sha256": output[
                    "model_item_repetition_spec_sha256"
                ],
                "temperature": output["temperature"],
                "thinking_enabled": output["thinking_enabled"],
                "input_truncated": output.get("input_truncated"),
                "input_tokens_used": output.get("input_tokens_used"),
                "input_chars_used": output.get("input_chars_used"),
                "max_output_tokens": output.get("max_output_tokens"),
                "generation_config_sha256": output["generation_config_sha256"],
                "structured_decoding_backend": output[
                    "structured_decoding_backend"
                ],
                "structured_decoding_disable_any_whitespace": output[
                    "structured_decoding_disable_any_whitespace"
                ],
                "finish_reason": output["finish_reason"],
                "stop_reason": output.get("stop_reason"),
                "output_tokens": output["output_tokens"],
                "compact_repair": output.get("compact_repair"),
                "record_variants": [],
                "resolved_records": [],
            }
        )
        if window is not None:
            enriched[-1]["repair_attempt"] = {
                "attempt_index": window["attempt_index"],
                "reason": REPAIR_REASON,
                "primary_generation_config_sha256": (
                    EXPECTED_GENERATION_CONFIG_SHA256
                ),
                "repair_generation_config_sha256": window[
                    "expected_generation_config_sha256"
                ],
                "repair_seed": window["repair_seed"],
                "repair_manifest_sha256": window["manifest_sha256"],
                "repair_output_file": window["repair_output_file"],
                "repaired": repaired,
            }

    by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for segment in enriched:
        by_parent[str(segment["parent_document_id"])].append(segment)
    by_id = {str(segment["document_id"]): segment for segment in enriched}
    for parent_id, segments in by_parent.items():
        route = str(segments[0]["route"])
        variants = build_record_variants(segments)
        annotated, resolved = resolve_parent_variants(parent_id, route, variants)
        for variant in annotated:
            by_id[str(variant["segment_id"])]["record_variants"].append(variant)
        for record in resolved:
            by_id[str(record["canonical_segment_id"])]["resolved_records"].append(record)
    for row in enriched:
        row["record_variants"].sort(key=lambda value: value["variant_id"])
        row["resolved_records"].sort(key=lambda value: value["record_id"])
    return enriched, parent_text


def load_round4_ledger_artifact(path: Path) -> dict[str, Any]:
    """Read the ledger the round-four builder wrote, with its private records.

    The builder writes two files side by side: the summary and parents in
    `endorsement_round4_record_ledger.json`, and one row per resolved record in
    the private JSONL. Both are needed, so this reads them together and checks
    the private file against the row count and digest the summary declares.
    """
    from src.candidate_endorsement_round4_release import (
        Round4ProjectionError,
        validate_endorsement_round4_record_ledger_private_rows,
    )

    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or set(payload) != {
        "summary", "parents", "private_file",
    }:
        raise ValueError("round-four ledger artifact has the wrong shape")
    spec = payload["private_file"]
    if not isinstance(spec, Mapping) or set(spec) != {"file", "rows", "sha256"}:
        raise ValueError("round-four ledger file spec differs")
    if spec["file"] != ENDORSEMENT_ROUND4_LEDGER_FILENAME:
        raise ValueError("round-four private file has the wrong name")
    private_path = Path(path).parent / str(spec["file"])
    if not private_path.is_file():
        raise ValueError(f"round-four private file is absent: {private_path}")
    if sha256_file(private_path) != spec["sha256"]:
        raise ValueError("round-four private file bytes differ from the ledger")
    records = list(read_jsonl_gz(private_path))
    if len(records) != int(spec["rows"]):
        raise ValueError("round-four private file row count differs")
    ledger = {"summary": payload["summary"], "parents": payload["parents"]}
    try:
        validate_endorsement_round4_record_ledger_private_rows(ledger)
    except Round4ProjectionError as error:
        raise ValueError(f"round-four ledger is not internally exact: {error}") from error
    return {
        "summary": payload["summary"],
        "parents": payload["parents"],
        "records": records,
        "private_path": private_path,
        "private_file": dict(spec),
    }


def apply_endorsement_round4_record_ledger(
    segments: list[dict[str, Any]], ledger: Mapping[str, Any]
) -> None:
    """Replace one-stage endorsement records with round-four ledger records.

    This mirrors `apply_endorsement_line_ledger` for the other architecture. The
    per-row overlay itself lives in the round-four module, so both callers write
    the same shape and the packager reads one shape.
    """
    from src.candidate_endorsement_round4_release import (
        ENDORSEMENT_ARCHITECTURE as ROUND4_ARCHITECTURE,
        apply_endorsement_round4_ledger,
    )

    parents = ledger.get("parents")
    if not isinstance(parents, list):
        raise ValueError("round-four ledger holds no parent summaries")
    parent_summaries = {
        str(item["parent_document_id"]): item for item in parents
    }
    records_by_segment: dict[str, list[Mapping[str, Any]]] = {}
    for record in ledger.get("records") or []:
        records_by_segment.setdefault(
            str(record["canonical_segment_id"]), []
        ).append(record)
    for values in records_by_segment.values():
        values.sort(key=lambda value: str(value["record_id"]))

    observed: set[str] = set()
    for segment in segments:
        route = str(segment["route"])
        if route != "endorsements":
            segment["extraction_architecture"] = "segment_window_v2_1"
            continue
        segment_id = str(segment["document_id"])
        parent_id = str(segment["parent_document_id"])
        observed.add(segment_id)
        parent = parent_summaries.get(parent_id)
        if not isinstance(parent, Mapping):
            raise ValueError(
                f"{segment_id}: endorsement parent lacks a round-four summary"
            )
        replaced = apply_endorsement_round4_ledger(
            segment, parent, records_by_segment.get(segment_id, [])
        )
        if replaced.get("extraction_architecture") != ROUND4_ARCHITECTURE:
            raise ValueError(f"{segment_id}: round-four overlay did not bind")
        segment.clear()
        segment.update(replaced)
    extra = set(records_by_segment) - observed
    if extra:
        raise ValueError(
            "round-four ledger names segments absent from the corpus: "
            + ", ".join(sorted(extra)[:5])
        )


def apply_endorsement_line_ledger(
    segments: list[dict[str, Any]], validated_ledger: Mapping[str, Any]
) -> None:
    """Replace one-stage endorsement records with replayed ledger records."""
    parent_summaries = validated_ledger.get("parent_summaries")
    resolved_by_segment = validated_ledger.get("resolved_records_by_segment")
    if not isinstance(parent_summaries, Mapping) or not isinstance(
        resolved_by_segment, Mapping
    ):
        raise ValueError("validated endorsement line-ledger projection is absent")
    observed_endorsement_segments: set[str] = set()
    for segment in segments:
        route = str(segment["route"])
        segment["extraction_architecture"] = (
            "endorsement_parent_line_ledger_v1"
            if route == "endorsements" else "segment_window_v2_1"
        )
        if route != "endorsements":
            continue
        segment_id = str(segment["document_id"])
        parent_id = str(segment["parent_document_id"])
        observed_endorsement_segments.add(segment_id)
        parent = parent_summaries.get(parent_id)
        if not isinstance(parent, Mapping):
            raise ValueError(
                f"{segment_id}: endorsement parent lacks ledger summary"
            )
        segment["superseded_one_stage_processing_status"] = segment[
            "processing_status"
        ]
        segment["record_variants"] = []
        segment["resolved_records"] = list(
            resolved_by_segment.get(segment_id, [])
        )
        public_complete = parent.get("public_coverage_complete") is True
        if public_complete:
            segment["processing_status"] = "processed_success"
        elif int(parent.get("aligned_record_count", 0)) > 0:
            segment["processing_status"] = "processed_partial_withheld"
        else:
            segment["processing_status"] = "processed_all_withheld"
        owner_counts = parent.get("owner_segment_model_line_counts") or {}
        carrier_counts = (
            parent.get("carrier_segment_aligned_record_counts") or {}
        )
        segment["endorsement_line_ledger"] = {
            "line_ledger_contract_sha256": get_line_ledger_contract()[
                "line_ledger_contract_sha256"
            ],
            "line_inventory_id": parent["line_inventory_id"],
            "owned_model_decision_line_count": int(
                owner_counts.get(segment_id, 0)
            ),
            "aligned_record_count": int(carrier_counts.get(segment_id, 0)),
            "parent_stage_a_batch_count": parent["stage_a_batch_count"],
            "parent_stage_a_decision_count": parent[
                "stage_a_decision_count"
            ],
            "parent_stage_a_unresolved_line_count": parent[
                "stage_a_unresolved_line_count"
            ],
            "parent_stage_b_batch_count": parent["stage_b_batch_count"],
            "parent_stage_b_anchor_count": parent["stage_b_anchor_count"],
            "parent_aligned_record_count": parent["aligned_record_count"],
            "parent_withheld_record_count": parent["withheld_record_count"],
            "parent_public_coverage_complete": public_complete,
        }
    extra = set(resolved_by_segment) - observed_endorsement_segments
    if extra:
        raise ValueError(
            "line-ledger records link to absent endorsement segments: "
            f"{sorted(extra)[:3]}"
        )


def omission_expected_type_matches(
    route: str, expected_type: Any, item: Mapping[str, Any]
) -> bool:
    """Match the handcoded record type without judging generated semantics."""
    expected = normalized_identity(expected_type).replace(" ", "_")
    if not expected:
        return False
    if route == "issues":
        return expected in {"issue", "issues", "issue_statement"} or expected == str(
            item.get("statement_kind") or ""
        )
    if route == "biography":
        return expected == str(item.get("field") or "")
    if route == "endorsements":
        return expected in {"endorsement", "endorsements"} or expected in {
            str(item.get("endorser_category") or ""),
            str(item.get("entity_type") or ""),
        }
    return False


def match_known_omissions(
    omissions: list[dict[str, Any]],
    segments: list[dict[str, Any]],
    parent_text: Mapping[str, str],
) -> dict[str, Any]:
    records_by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for segment in segments:
        for record in segment["resolved_records"]:
            records_by_parent[str(record["parent_document_id"])].append(record)
    rows: list[dict[str, Any]] = []
    for omission in omissions:
        parent_id = str(omission["parent_document_id"])
        route = str(omission["route"])
        source = parent_text[parent_id]
        target = str(omission["evidence"])
        occurrences: list[tuple[int, int]] = []
        if target:
            start = source.find(target)
            while start >= 0:
                occurrences.append((start, start + len(target)))
                start = source.find(target, start + 1)
        matches: list[str] = []
        for record in (records_by_parent.get(parent_id, []) if occurrences else []):
            if record["route"] != route:
                continue
            item = record["item"]
            if not omission_expected_type_matches(
                route, omission.get("expected_type"), item
            ):
                continue
            evidence_field = "evidence" if route != "endorsements" else "record_evidence"
            start_field = f"{evidence_field}_parent_start_char"
            end_field = f"{evidence_field}_parent_end_char"
            record_span = (
                int(record["parent_offsets"][start_field]),
                int(record["parent_offsets"][end_field]),
            )
            for target_span in occurrences:
                if max(record_span[0], target_span[0]) < min(
                    record_span[1], target_span[1]
                ):
                    matches.append(str(record["record_id"]))
                    break
        rows.append(
            {
                **omission,
                "source_evidence_exact_occurrences": len(occurrences),
                "matched_record_ids": sorted(set(matches)),
                "recovered": bool(matches),
                "human_confirmation_required": True,
                "expected_value_confirmation_required": bool(
                    str(omission.get("expected_value") or "").strip()
                ),
                "human_confirmed_semantic_recovery": None,
            }
        )
    recovered = sum(bool(row["recovered"]) for row in rows)
    return {
        "known_omissions": len(rows),
        "recovered": recovered,
        "all_recovered": recovered == len(rows),
        "human_confirmation_required": True,
        "human_confirmation_status": "pending",
        "matching_spec": KNOWN_OMISSION_MATCHING_SPEC,
        "matching_spec_sha256": KNOWN_OMISSION_MATCHING_SPEC_SHA256,
        "matching_rule": (
            "same route and handcoded record type; target evidence must occur exactly "
            "in transformed parent text; one exact occurrence span must overlap the "
            "record primary-evidence parent span; expected_value is retained only for "
            "independent human semantic confirmation"
        ),
        "items": rows,
    }


def pending_human_validation() -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "pending",
        "independent_human_only": False,
        "tuning_audit_excluded": True,
        "precision": {
            "biography": None,
            "issues": None,
            "endorsements": None,
        },
        "recall": {"biography": None, "issues": None, "endorsements": None},
        "known_omission_human_confirmation": {
            "expected_n": 11,
            "confirmed_n": 0,
            "all_confirmed": False,
            "items": [],
        },
        "gates_passed": False,
    }


def build_bundle(
    export_dir: Path,
    hive_output_dir: Path,
    output_dir: Path,
    known_omissions_path: Path | None = None,
    require_technical_pass: bool = False,
    *,
    endorsement_stage_a_export_dir: Path | None = None,
    endorsement_stage_a_output_dir: Path | None = None,
    endorsement_stage_a2_export_dir: Path | None = None,
    endorsement_stage_a2_output_dir: Path | None = None,
    endorsement_stage_a3_task_json: Path | None = None,
    endorsement_stage_a3_task_html: Path | None = None,
    endorsement_stage_a2_evaluation_report: Path | None = None,
    endorsement_stage_a3_results_json: Path | None = None,
    endorsement_stage_a3_imported_artifact_json: Path | None = None,
    endorsement_stage_b_export_dir: Path | None = None,
    endorsement_stage_b_output_dir: Path | None = None,
    endorsement_round4_ledger: Path | None = None,
    repair_attempts: Sequence[tuple[Path, Path]] | None = None,
) -> dict[str, Any]:
    # One bundle carries one endorsement architecture. The round-four ledger and
    # the line-ledger stage inputs are alternatives, never a mixture: the
    # packager reads the architecture from whichever block the manifest holds.
    line_ledger_inputs = (
        endorsement_stage_a_export_dir,
        endorsement_stage_a_output_dir,
        endorsement_stage_a2_export_dir,
        endorsement_stage_a2_output_dir,
        endorsement_stage_a3_task_json,
        endorsement_stage_a3_task_html,
        endorsement_stage_a2_evaluation_report,
        endorsement_stage_a3_results_json,
        endorsement_stage_a3_imported_artifact_json,
        endorsement_stage_b_export_dir,
        endorsement_stage_b_output_dir,
    )
    use_round4 = endorsement_round4_ledger is not None
    if use_round4 and any(value is not None for value in line_ledger_inputs):
        raise ValueError(
            "a round-four bundle takes no line-ledger stage inputs"
        )
    if not use_round4 and any(value is None for value in line_ledger_inputs):
        raise ValueError(
            "a line-ledger bundle needs every stage input, or pass "
            "--endorsement-round4-ledger instead"
        )
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(f"refusing nonempty output directory: {output_dir}")
    if not use_round4:
        # Fail before creating a bundle directory when the bounded extraction
        # dependency has not been human-coded and imported.
        _stage_a3_private_row(
            task_json=endorsement_stage_a3_task_json,
            task_html=endorsement_stage_a3_task_html,
            stage_a2_evaluation_report=endorsement_stage_a2_evaluation_report,
            results_json=endorsement_stage_a3_results_json,
            imported_artifact_json=endorsement_stage_a3_imported_artifact_json,
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    source_manifest, inputs = load_inputs(export_dir)
    selected_context_census = endorsement_segment_complete_line_census(
        inputs.values()
    )
    round4_ledger: dict[str, Any] | None = None
    ledger_private_rows: list[dict[str, Any]] = []
    validated_ledger: dict[str, Any] = {}
    if use_round4:
        round4_ledger = load_round4_ledger_artifact(endorsement_round4_ledger)
    else:
        ledger_private_rows, validated_ledger = load_endorsement_line_ledger_runs(
            inputs,
            endorsement_stage_a_export_dir,
            endorsement_stage_a_output_dir,
            endorsement_stage_a2_export_dir,
            endorsement_stage_a2_output_dir,
            endorsement_stage_a3_task_json,
            endorsement_stage_a3_task_html,
            endorsement_stage_a2_evaluation_report,
            endorsement_stage_a3_results_json,
            endorsement_stage_a3_imported_artifact_json,
            endorsement_stage_b_export_dir,
            endorsement_stage_b_output_dir,
        )
    outputs = load_outputs(hive_output_dir)
    run_metadata = validate_run_metadata(hive_output_dir)
    validate_run_metadata_source_coverage(source_manifest, run_metadata)
    generation_hash = str(run_metadata[0]["generation_config_sha256"])
    loaded_repairs: list[dict[str, Any]] = []
    superseded_primary_rows: dict[str, dict[str, Any]] = {}
    if repair_attempts:
        for manifest_path, attempt_output_dir in repair_attempts:
            loaded = load_repair_windows(
                manifest_path,
                repair_output_dir=attempt_output_dir,
                export_chunks_dir=export_dir / "chunks",
            )
            loaded["output_dir"] = str(attempt_output_dir)
            loaded["final_job_run_metadata"] = load_repair_run_metadata(
                attempt_output_dir,
                attempt_index=int(loaded["attempt_index"]),
                target_count=int(loaded["target_count"]),
            )
            loaded_repairs.append(loaded)
        # Keep the primary row each repair replaces. The primary run metadata
        # still counts it, so the accounting check needs it to stay exact.
        superseded_primary_rows = {
            document_id: copy.deepcopy(outputs[document_id])
            for attempt in loaded_repairs
            for document_id, window in attempt["windows"].items()
            if window["repaired"] and document_id in outputs
        }
        apply_repair_attempts(outputs, loaded_repairs)
    repair_windows: dict[str, Mapping[str, Any]] = {}
    for attempt in sorted(
        loaded_repairs, key=lambda item: int(item["attempt_index"])
    ):
        repair_windows.update(attempt["windows"])
    segments, parent_text = enrich_segments(
        inputs,
        outputs,
        generation_hash,
        run_metadata[0]["generation_config"],
        repair_windows=repair_windows or None,
    )
    if use_round4:
        from src.candidate_endorsement_round4_production import (
            get_round4_production_contract,
        )
        from src.candidate_endorsement_round4_release import (
            ENDORSEMENT_ARCHITECTURE as ROUND4_ARCHITECTURE,
        )

        apply_endorsement_round4_record_ledger(segments, round4_ledger)
        endorsement_route_contract = get_round4_production_contract()[
            "round4_production_contract_sha256"
        ]
        endorsement_architecture = ROUND4_ARCHITECTURE
    else:
        apply_endorsement_line_ledger(segments, validated_ledger)
        endorsement_route_contract = get_line_ledger_contract()[
            "line_ledger_contract_sha256"
        ]
        endorsement_architecture = "endorsement_parent_line_ledger_v1"
    # The packager checks every row, not only endorsement rows, against the
    # bundle's declared endorsement architecture and route contract. Nothing
    # stamped them before, which the packager's tests did not reveal because
    # their fixtures build rows by hand.
    for segment in segments:
        segment["endorsement_route_contract_sha256"] = endorsement_route_contract
        segment["endorsement_architecture"] = endorsement_architecture

    status_counts = Counter(str(row["processing_status"]) for row in segments)
    resolved_records = sum(len(row["resolved_records"]) for row in segments)
    raw_variants = sum(len(row["record_variants"]) for row in segments)
    conflict_groups = {
        str(variant["overlap_group_id"])
        for row in segments
        for variant in row["record_variants"]
        if variant["overlap_resolution_status"] == "conflict_withheld"
    }
    withheld_cap_documents = {
        str(row["document_id"])
        for row in segments
        if row.get("processing_status") == OUTPUT_CAP_WITHHELD_STATUS
    }
    attested_unrepaired = last_attempt_unrepaired_documents(loaded_repairs)
    if withheld_cap_documents - attested_unrepaired:
        raise ValueError(
            "cap-limited windows without a repair attestation that reports them "
            f"unrepaired: {sorted(withheld_cap_documents - attested_unrepaired)}"
        )
    if use_round4:
        # Round four has no coverage boolean. Its corpus gate is an accounting
        # gate: every Stage-B response is accounted for, and every gap carries a
        # typed reason. A parent can be publicly incomplete and still accounted.
        endorsement_coverage_gate_passes = (
            round4_ledger["summary"]["all_model_records_accounted"] is True
        )
    else:
        endorsement_coverage_gate_passes = (
            validated_ledger["summary"]["public_coverage_complete"] is True
        )
    technical_pass = (
        technical_segment_gate_passes(
            status_counts,
            len(segments),
            allow_withheld_output_cap=bool(withheld_cap_documents),
        )
        and endorsement_coverage_gate_passes
    )
    if require_technical_pass and not technical_pass:
        raise ValueError(
            "v2.1 technical gate failed: "
            f"statuses={dict(status_counts)}, overlap_conflicts={len(conflict_groups)}"
        )
    model_item_count = sum(
        int((row.get("exact_alignment") or {}).get("model_item_count", 0))
        for row in segments
    )
    aligned_item_count = sum(
        int((row.get("exact_alignment") or {}).get("aligned_item_count", 0))
        for row in segments
    )
    withheld_item_count = sum(
        int((row.get("exact_alignment") or {}).get("withheld_item_count", 0))
        for row in segments
    )
    if model_item_count != aligned_item_count + withheld_item_count:
        raise ValueError("v2.1 model-item alignment counts do not reconcile")
    private_output_summary = output_provenance_summary(segments)
    corpus_output_aggregate = all_output_aggregate(segments)
    metadata_aggregate = run_metadata_aggregate(run_metadata)
    replacement_segments = [
        row for row in segments
        if str(row["document_id"]) in superseded_primary_rows
    ]
    primary_equivalent_aggregate = primary_equivalent_output_aggregate(
        corpus_output_aggregate,
        [superseded_primary_rows[str(row["document_id"])] for row in replacement_segments],
        replacement_segments,
    )
    validate_run_metadata_against_all_output(
        metadata_aggregate,
        primary_equivalent_aggregate,
        "Hive run metadata aggregate",
    )
    segments_path = output_dir / SEGMENTS_FILENAME
    segment_rows = write_jsonl_gz(segments_path, segments)
    if use_round4:
        # The builder already wrote the private records; the bundle carries a
        # copy so it stands alone, and the digest is re-read from the copy.
        ledger_path = output_dir / ENDORSEMENT_ROUND4_LEDGER_FILENAME
        ledger_rows = write_jsonl_gz(ledger_path, round4_ledger["records"])
    else:
        ledger_path = output_dir / ENDORSEMENT_LINE_LEDGER_FILENAME
        ledger_rows = write_jsonl_gz(ledger_path, ledger_private_rows)
    line_ledger_path = ledger_path
    line_ledger_rows = ledger_rows
    human_path = output_dir / HUMAN_VALIDATION_FILENAME
    human_path.write_text(json.dumps(pending_human_validation(), indent=2) + "\n")
    parents_by_route = Counter(
        str(row["route"])
        for parent_id, row in {
            str(row["parent_document_id"]): row for row in segments
        }.items()
    )
    segments_by_route = Counter(str(row["route"]) for row in segments)
    records_by_route = Counter(
        str(record["route"])
        for row in segments
        for record in row["resolved_records"]
    )

    omission_report = None
    omission_file_entry = None
    if known_omissions_path is None:
        candidate = export_dir / "known_omissions.json"
        if candidate.exists():
            known_omissions_path = candidate
    if known_omissions_path is not None:
        omissions = json.loads(known_omissions_path.read_text())
        omission_report = match_known_omissions(omissions, segments, parent_text)
        report_path = output_dir / "known_omission_recovery.json"
        report_path.write_text(json.dumps(omission_report, indent=2, ensure_ascii=False) + "\n")
        omission_file_entry = {
            "file": report_path.name,
            "rows": len(omission_report["items"]),
            "sha256": sha256_file(report_path),
        }

    manifest = {
        "schema_version": SCHEMA_VERSION,
        "release_status": "withheld_validation",
        "source_corpus_sha256": source_manifest["source_corpus_sha256"],
        "source_export_manifest_sha256": sha256_file(export_dir / "manifest.json"),
        "source_export_upstream_full_manifest_sha256": source_manifest.get(
            "source_export_manifest_sha256"
        ),
        "source_text_transform": source_manifest["source_text_transform"],
        "endorsement_context_provenance": source_manifest[
            "endorsement_context_provenance"
        ],
        "endorsement_context_selected_segment_complete_line_census": (
            selected_context_census
        ),
        **(
            {
                "endorsement_round4_record_ledger": {
                    "summary": round4_ledger["summary"],
                    "parents": round4_ledger["parents"],
                    "private_file": {
                        "file": ENDORSEMENT_ROUND4_LEDGER_FILENAME,
                        "rows": ledger_rows,
                        "sha256": sha256_file(ledger_path),
                    },
                }
            }
            if use_round4
            else {
                "endorsement_line_ledger": {
                    "summary": validated_ledger["summary"],
                    "private_file": {
                        "file": ENDORSEMENT_LINE_LEDGER_FILENAME,
                        "rows": ledger_rows,
                        "sha256": sha256_file(ledger_path),
                    },
                }
            }
        ),
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
        "prompt_sha256": CONTRACT["prompt_sha256"],
        "json_schema_sha256": CONTRACT["json_schema_sha256"],
        "extraction_contract_sha256": CONTRACT["extraction_contract_sha256"],
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
        "generation_config": run_metadata[0]["generation_config"],
        "generation_config_sha256": generation_hash,
        "structured_decoding_backend": STRUCTURED_DECODING_BACKEND,
        "structured_decoding_disable_any_whitespace": (
            STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
        ),
        "private_output_provenance_summary": private_output_summary,
        "all_output_aggregate": corpus_output_aggregate,
        # What the primary run alone produced. It differs from the corpus
        # aggregate only for windows a seed-vary repair replaced, and it is what
        # the primary run metadata must be checked against.
        "primary_equivalent_output_aggregate": primary_equivalent_aggregate,
        "run_metadata_aggregate": metadata_aggregate,
        "parent_documents": len(parent_text),
        "parent_documents_by_route": dict(sorted(parents_by_route.items())),
        "segments": len(segments),
        "segments_by_route": dict(sorted(segments_by_route.items())),
        "raw_record_variants": raw_variants,
        "model_items": model_item_count,
        "aligned_items": aligned_item_count,
        "withheld_model_items": withheld_item_count,
        "resolved_records": resolved_records,
        "resolved_records_by_route": dict(sorted(records_by_route.items())),
        "withheld_overlap_conflict_groups": len(conflict_groups),
        "segment_processing_status": dict(sorted(status_counts.items())),
        "technical_segment_gate_passed": technical_pass,
        "seed_vary_repair": (
            [
                {
                    "attempt_index": attempt["attempt_index"],
                    "repair_seed": attempt["repair_seed"],
                    "repair_manifest_file": Path(attempt["manifest_path"]).name,
                    "repair_manifest_sha256": attempt["manifest_sha256"],
                    "repair_output_dir": attempt["output_dir"],
                    "reason": attempt["reason"],
                    "primary_generation_config_sha256": (
                        EXPECTED_GENERATION_CONFIG_SHA256
                    ),
                    "repair_generation_config_sha256": attempt[
                        "generation_config_sha256"
                    ],
                    "target_count": attempt["target_count"],
                    "repaired_count": attempt["repaired_count"],
                    "unrepaired_count": attempt["unrepaired_count"],
                    "final_job_run_metadata": _repair_run_metadata_entry(
                        attempt["final_job_run_metadata"]
                    ),
                    "windows": [
                        {
                            "document_id": document_id,
                            "chunk": window["chunk"],
                            "repaired": bool(window["repaired"]),
                        }
                        for document_id, window in sorted(
                            attempt["windows"].items()
                        )
                    ],
                }
                for attempt in sorted(
                    loaded_repairs,
                    key=lambda item: int(item["attempt_index"]),
                )
            ]
            if loaded_repairs
            else None
        ),
        "segments_file": {
            "file": SEGMENTS_FILENAME,
            "rows": segment_rows,
            "sha256": sha256_file(segments_path),
        },
        "human_validation_file": {
            "file": HUMAN_VALIDATION_FILENAME,
            "sha256": sha256_file(human_path),
        },
        "run_metadata": [
            {
                "file": row["metadata_file"],
                "sha256": row["metadata_sha256"],
                "shard_index": row.get("shard_index"),
                "shard_count": row.get("shard_count"),
                "generation_config_sha256": row["generation_config_sha256"],
                "structured_decoding_backend": row[
                    "structured_decoding_backend"
                ],
                "structured_decoding_disable_any_whitespace": row[
                    "structured_decoding_disable_any_whitespace"
                ],
                "array_item_cap_spec_id": row["array_item_cap_spec_id"],
                "array_item_cap_spec_sha256": row[
                    "array_item_cap_spec_sha256"
                ],
                "route_array_item_caps": row["route_array_item_caps"],
                "model_item_repetition_spec_id": row[
                    "model_item_repetition_spec_id"
                ],
                "model_item_repetition_spec_sha256": row[
                    "model_item_repetition_spec_sha256"
                ],
                "recovery_attestation_without_inference": row[
                    "recovery_attestation_without_inference"
                ],
                **{
                    field: row[field]
                    for field in RUN_METADATA_SCALAR_FIELDS
                },
                **{
                    field: row[field]
                    for field in RUN_METADATA_MAPPING_FIELDS
                },
                **{
                    field: row[field]
                    for field in RUN_METADATA_MAX_MAPPING_FIELDS
                },
            }
            for row in run_metadata
        ],
        "known_omission_gate": None if omission_report is None else omission_report["all_recovered"],
        "known_omission_file": omission_file_entry,
        "known_omission_matching_spec_sha256": (
            None
            if omission_report is None
            else KNOWN_OMISSION_MATCHING_SPEC_SHA256
        ),
    }
    (output_dir / "bundle_manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export-dir", type=Path, required=True)
    parser.add_argument("--hive-output-dir", type=Path, required=True)
    parser.add_argument(
        "--endorsement-stage-a-export-dir", type=Path, required=False
    )
    parser.add_argument(
        "--endorsement-stage-a-output-dir", type=Path, required=False
    )
    parser.add_argument(
        "--endorsement-stage-a2-export-dir", type=Path, required=False
    )
    parser.add_argument(
        "--endorsement-stage-a2-output-dir", type=Path, required=False
    )
    parser.add_argument(
        "--endorsement-stage-a3-task-json", type=Path, required=False
    )
    parser.add_argument(
        "--endorsement-stage-a3-task-html", type=Path, required=False
    )
    parser.add_argument(
        "--endorsement-stage-a2-evaluation-report", type=Path, required=False
    )
    parser.add_argument(
        "--endorsement-stage-a3-results-json", type=Path, required=False
    )
    parser.add_argument(
        "--endorsement-stage-a3-imported-artifact-json",
        type=Path,
        required=False,
    )
    parser.add_argument(
        "--endorsement-stage-b-export-dir", type=Path, required=False
    )
    parser.add_argument(
        "--endorsement-stage-b-output-dir", type=Path, required=False
    )
    parser.add_argument(
        "--endorsement-round4-ledger",
        type=Path,
        help=(
            "The round-four record ledger written by "
            "build_candidate_endorsement_round4_record_ledger.py. Its private "
            "record file must sit beside it. Given this, the bundle carries the "
            "round-four architecture and takes no line-ledger stage inputs."
        ),
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--known-omissions", type=Path)
    parser.add_argument(
        "--repair-manifest",
        type=Path,
        action="append",
        help=(
            "Seed-vary repair attestation. Repeat once per attempt, in ascending "
            "attempt order. The bundle then takes the repair row for every window "
            "an attestation reports as repaired, and records every attempt it "
            "names."
        ),
    )
    parser.add_argument(
        "--repair-output-dir",
        type=Path,
        action="append",
        help=(
            "Directory holding one repair run's published output chunks. Repeat "
            "in the same order as --repair-manifest."
        ),
    )
    parser.add_argument("--require-technical-pass", action="store_true")
    args = parser.parse_args()
    repair_manifests = list(args.repair_manifest or [])
    repair_output_dirs = list(args.repair_output_dir or [])
    if len(repair_manifests) != len(repair_output_dirs):
        parser.error(
            "--repair-manifest and --repair-output-dir must be given in pairs"
        )
    report = build_bundle(
        args.export_dir,
        args.hive_output_dir,
        args.output_dir,
        args.known_omissions,
        args.require_technical_pass,
        endorsement_stage_a_export_dir=(
            args.endorsement_stage_a_export_dir
        ),
        endorsement_stage_a_output_dir=(
            args.endorsement_stage_a_output_dir
        ),
        endorsement_stage_a2_export_dir=(
            args.endorsement_stage_a2_export_dir
        ),
        endorsement_stage_a2_output_dir=(
            args.endorsement_stage_a2_output_dir
        ),
        endorsement_stage_a3_task_json=(
            args.endorsement_stage_a3_task_json
        ),
        endorsement_stage_a3_task_html=(
            args.endorsement_stage_a3_task_html
        ),
        endorsement_stage_a2_evaluation_report=(
            args.endorsement_stage_a2_evaluation_report
        ),
        endorsement_stage_a3_results_json=(
            args.endorsement_stage_a3_results_json
        ),
        endorsement_stage_a3_imported_artifact_json=(
            args.endorsement_stage_a3_imported_artifact_json
        ),
        endorsement_stage_b_export_dir=(
            args.endorsement_stage_b_export_dir
        ),
        endorsement_stage_b_output_dir=(
            args.endorsement_stage_b_output_dir
        ),
        endorsement_round4_ledger=args.endorsement_round4_ledger,
        repair_attempts=(
            list(zip(repair_manifests, repair_output_dirs)) or None
        ),
    )
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
