#!/usr/bin/env python3
"""Build the six public candidate-extraction files from a frozen v2 bundle.

This script does not call a model. It verifies the frozen segment output against
the corrected raw corpus. It then removes all source text from the public files.
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit, urlparse

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from scripts.import_candidate_extraction_v2 import (  # noqa: E402
    CONTRACT as V2_CONTRACT,
    EXPECTED_GENERATION_CONFIG_SHA256,
    EXPECTED_GENERATION_SEED,
    EXPECTED_REPAIR_GENERATION_CONFIG_SHA256S,
    EXPECTED_REPAIR_GENERATION_SEEDS,
    EXPECTED_MAX_MODEL_LEN,
    EXPECTED_MAX_OUTPUT_TOKENS,
    EXPECTED_STRUCTURED_DECODING,
    FROZEN_SUFFIX_P_FULL_SOURCE_MANIFEST_SHA256,
    FROZEN_SUFFIX_P_SOURCE_CORPUS_SHA256,
    KNOWN_OMISSION_MATCHING_SPEC,
    KNOWN_OMISSION_MATCHING_SPEC_SHA256,
    RUN_METADATA_MAPPING_FIELDS,
    RUN_METADATA_MAX_MAPPING_FIELDS,
    OUTPUT_CAP_WITHHELD_STATUS,
    RUN_METADATA_SCALAR_FIELDS,
    TRUNCATED_PARSE_STATUS,
    _typed_equal,
    all_output_aggregate,
    build_record_variants,
    canonical_json,
    match_known_omissions,
    load_endorsement_line_ledger_bundle,
    load_endorsement_round4_ledger_bundle,
    output_provenance_summary,
    resolve_parent_variants,
    run_metadata_aggregate,
    technical_segment_gate_passes,
    validate_endorsement_context_manifest,
    validate_private_output_provenance,
    validate_parent_sources,
    validate_run_metadata_against_all_output,
    validate_run_metadata_accounting,
)
from src.candidate_endorsement_line_ledger import (  # noqa: E402
    get_line_ledger_contract,
)
from src.candidate_endorsement_round4_production import (  # noqa: E402
    get_round4_production_contract,
)
from src.candidate_endorsement_round4_release import (  # noqa: E402
    ENDORSEMENT_ARCHITECTURE as ROUND4_ENDORSEMENT_ARCHITECTURE,
    ROUND4_RECORD_RESOLUTION_METHOD,
    ROUND4_RECORD_RESOLUTION_STATUS,
    TARGET_BLOCK_ALIGNMENT_METHOD as ROUND4_TARGET_BLOCK_ALIGNMENT_METHOD,
    _parent_processing_status as round4_parent_processing_status,
)
from scripts.hive.extract_candidate_pages_vllm import (  # noqa: E402
    GENERATION_CONFIG_VERSION,
    RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID,
    RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256,
    STRUCTURED_DECODING_BACKEND,
    STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
)
from src.candidate_extraction_provenance import (  # noqa: E402
    OFFSET_COORDINATE_SYSTEM,
    SOURCE_TEXT_TRANSFORM_RULE,
    SOURCE_TEXT_TRANSFORM_VERSION,
)
from src.candidate_extraction import (  # noqa: E402
    ALIGNMENT_METHOD_PAGE_ROUTE,
    ALIGNMENT_METHOD_PROVIDED_PARENT,
    ALIGNMENT_METHOD_PROVIDED_SEGMENT,
    ALIGNMENT_METHOD_UNIQUE,
    ARRAY_ITEM_CAP_REACHED_STATUS,
    exact_occurrence_spans,
    ENDORSEMENT_CONTEXT_CLASSIFIER_ID,
    ENDORSEMENT_CONTEXT_CLASSIFIER_SPEC_SHA256,
    ENDORSEMENT_CONTEXT_PROVENANCE_VERSION,
    ENDORSEMENT_CONTEXT_PROVENANCE_SPEC_SHA256,
    ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC_SHA256,
    MODEL_ITEM_REPETITION_ERROR_STATUS,
    align_extraction_exact_spans,
    endorsement_context_provenance_errors,
    validate_extraction,
)


# The packager reads one schema version, chosen the same way the importer
# chooses it. Version 2.2 differs from 2.1 only in the alignment spec, so every
# table, check and validation call below applies unchanged to both.
SCHEMA_VERSION = os.environ.get("CANDIDATE_EXTRACTION_SCHEMA_VERSION", "2.1")
if SCHEMA_VERSION not in ("2.1", "2.2"):
    raise RuntimeError(f"this packager reads 2.1 and 2.2, not {SCHEMA_VERSION}")

ROUTES = ("issues", "biography", "endorsements")
# The two endorsement architectures.  The line ledger is the historical
# three-pass implementation.  Round four is the selected production
# implementation.  A bundle declares exactly one of them.
LINE_LEDGER_ENDORSEMENT_ARCHITECTURE = "endorsement_parent_line_ledger_v1"
ROUND4_LEDGER_MANIFEST_KEY = "endorsement_round4_record_ledger"
LIST_KEYS = {
    "issues": "issue_blocks",
    "biography": "facts",
    "endorsements": "endorsements",
}
PUBLIC_FILES = (
    "candidate_extraction_status.csv",
    "candidate_extraction_documents.parquet",
    "candidate_extraction_segments.parquet",
    "candidate_issue_statements.parquet",
    "candidate_biography_facts.parquet",
    "candidate_endorsements.parquet",
)

EXPECTED_CORE_RAW_SHA256 = (
    "c53d36b24b293ac5d47706aa67731f1c107d6a679cb948c9ad71a70b5c8ce7c3"
)
EXPECTED_MODEL_ID = "Qwen/Qwen3.6-27B-FP8"
EXPECTED_MODEL_REVISION = "e89b16ebf1988b3d6befa7de50abc2d76f26eb09"
EXPECTED_RUNTIME_LOCK_SHA256 = (
    "4f46101fa493e38b312af034b8d740c612bd0001bab4cb0f624f4a9e1c49f5b3"
)
# The approved extraction contract, one per schema version. Version 2.2 shares
# every generation input with 2.1; only the alignment spec, and therefore the
# contract hash, differs. Keeping both literals here means an accidental edit to
# any contract input still fails closed rather than silently changing the value.
APPROVED_EXTRACTION_CONTRACT_SHA256 = {
    "2.1": "82530ab0f4126e1640dcffbe48a66bffdfab1538fc40240b5f3e55ab0eed5574",
    "2.2": "b0bdd1662e204e855f6e63fe0c1ffc4e378b8ede55353a464b33f5ed6ed3343b",
}
EXPECTED_EXTRACTION_CONTRACT_SHA256 = APPROVED_EXTRACTION_CONTRACT_SHA256[
    SCHEMA_VERSION
]
# The official-government-host rule removes this many parent documents from the
# approved corpus. Asserting it means a change in the corpus or the rule fails
# closed rather than quietly shipping a different population.
EXPECTED_EXCLUDED_PARENT_DOCUMENTS = 631
EXTENSION_MANIFEST_FILENAME = "candidate_extraction_manifest.json"
EXPECTED_PARENT_COUNTS = {
    "issues": 14_326,
    "biography": 6_962,
    "endorsements": 2_583,
}
EXPECTED_SEGMENT_COUNTS = {
    "issues": 20_673,
    "biography": 8_444,
    "endorsements": 6_096,
}
EXPECTED_UNIVERSE_ROWS = 15_041
EXPECTED_TRANSFORMED_PARENT_DOCUMENTS = 23_856

HEX64 = re.compile(r"^[0-9a-f]{64}$")
WAYBACK_TARGET = re.compile(r"^/web/[^/]+/(https?://.+)$", re.IGNORECASE)
STATEMENT_KINDS = {
    "support", "oppose", "proposal", "past_action", "current_action", "unclear"
}
BIOGRAPHY_FIELDS = {
    "occupation", "education", "elected_office_experience",
    "public_service_experience", "military_service", "local_roots",
}
TIMING_VALUES = {"current", "former", "unclear"}
TIMING_REQUIRED_FIELDS = {
    "occupation", "elected_office_experience", "public_service_experience",
    "local_roots",
}
ENTITY_TYPES = {"individual", "organization", "unclear"}
ENDORSER_CATEGORIES = {
    "private_individual", "government_official", "party_official", "labor_union",
    "media_outlet", "other_organization", "other_individual", "unclear",
}
SUPPORT_MODES = {"explicit_text", "page_or_section_context"}
SOURCE_OBSERVABILITY = {
    "not_observed", "observed_empty", "observed_extractable"
}
EXTRACTION_STATUSES = {
    "observed_positive", "observed_zero", "not_extracted_no_source",
    "failed", "withheld_validation",
}

ISSUE_ITEM_FIELDS = {
    "source_heading", "source_heading_start_char", "source_heading_end_char",
    "claim", "statement_kind", "evidence", "evidence_start_char",
    "evidence_end_char",
    "source_model_item_id", "source_heading_exact_match_count",
    "source_heading_alignment_method", "evidence_exact_match_count",
    "evidence_alignment_method",
}
BIOGRAPHY_ITEM_FIELDS = {
    "field", "value", "timing", "relationship", "evidence",
    "evidence_start_char", "evidence_end_char",
    "source_model_item_id", "evidence_exact_match_count",
    "evidence_alignment_method",
}
ENDORSEMENT_ITEM_FIELDS = {
    "endorser_name", "entity_type", "endorser_category", "title",
    "affiliation", "jurisdiction", "record_evidence",
    "record_evidence_start_char", "record_evidence_end_char",
    "endorsement_context_evidence", "endorsement_context_evidence_start_char",
    "endorsement_context_evidence_end_char", "support_mode",
    "source_model_item_id", "record_evidence_exact_match_count",
    "record_evidence_alignment_method",
    "endorsement_context_evidence_exact_match_count",
    "endorsement_context_evidence_alignment_method",
    "endorsement_context_id", "endorsement_context_evidence_source_scope",
    "endorsement_context_evidence_parent_start_char",
    "endorsement_context_evidence_parent_end_char",
    "endorsement_context_record_distance_chars",
    "endorsement_context_qualifying_wayback_url",
    "endorsement_context_qualifying_original_url",
    "endorsement_context_qualifying_original_path",
}
ENDORSEMENT_LINE_LEDGER_PROVENANCE_FIELDS = {
    "line_inventory_id", "source_line_id", "stage_a_batch_id",
    "stage_a_slot", "stage_b_batch_id", "stage_b_slot",
    "model_record_index", "carrier_alignment_source_model_item_id",
    "inventory_owner_segment_id", "record_evidence_carrier_segment_id",
    "target_block_alignment_method", "target_block_exact_match_count",
    "record_evidence_parent_start_char", "record_evidence_parent_end_char",
}
ENDORSEMENT_LINE_LEDGER_ITEM_FIELDS = (
    ENDORSEMENT_ITEM_FIELDS | ENDORSEMENT_LINE_LEDGER_PROVENANCE_FIELDS
)

STATUS_COLUMNS = (
    "candidate_cycle_id", "route", "source_observability", "extraction_status",
    "n_raw_pages", "n_documents", "n_segments", "n_records",
    "n_withheld_overlap_conflict_groups",
)
DOCUMENT_COLUMNS = (
    "parent_document_id", "candidate_cycle_id", "candidate_icpsr", "state",
    "office", "year", "party", "route", "raw_text_sha256",
    "extraction_source_text_sha256", "extraction_source_n_chars",
    "n_raw_occurrences", "first_capture_timestamp", "last_capture_timestamp",
    "wayback_urls", "original_urls", "segment_count", "processed_segment_count",
    "record_count", "withheld_overlap_conflict_group_count",
    "processing_status", "schema_version", "model_id",
    "model_revision", "runtime_lock_sha256", "prompt_sha256",
    "json_schema_sha256", "extraction_contract_sha256",
    "generation_config_sha256", "source_corpus_sha256",
    "alignment_id", "alignment_spec_sha256",
    "semantic_validation_spec_sha256",
    "endorsement_context_provenance_spec_sha256", "model_item_count",
    "user_message_template_id", "user_message_template_sha256",
    "aligned_item_count", "withheld_item_count",
    "endorsement_context_provenance_version",
    "endorsement_context_url_classifier_id",
    "endorsement_context_url_classifier_spec_sha256",
    "endorsement_context_structural_heading_spec_sha256",
    "source_text_transform_version", "source_text_transform_rule",
    "offset_coordinate_system", "source_text_transform_changed",
)
SEGMENT_COLUMNS = (
    "segment_id", "parent_document_id", "candidate_cycle_id", "route",
    "segment_index", "segment_count", "segment_start_char", "segment_end_char",
    "segment_text_sha256", "processing_status", "schema_valid",
    "input_truncated", "finish_reason", "stop_reason", "output_tokens",
    "max_output_tokens", "extracted_record_count", "schema_version", "model_id",
    "model_revision", "runtime_lock_sha256", "prompt_sha256",
    "json_schema_sha256", "extraction_contract_sha256",
    "generation_config_sha256", "source_text_transform_version",
    "inference_display_escape_id", "inference_display_escape_spec_sha256",
    "json_string_control_canonicalizer_id",
    "json_string_control_canonicalizer_spec_sha256",
    "json_control_canonicalization_applied",
    "json_control_canonicalization_count",
    "offset_coordinate_system", "alignment_id", "alignment_spec_sha256",
    "semantic_validation_spec_sha256",
    "endorsement_context_provenance_spec_sha256",
    "exact_alignment_status",
    "user_message_template_id", "user_message_template_sha256",
    "model_item_count", "aligned_item_count", "withheld_item_count",
    "withheld_overlap_conflict_group_count",
    "endorsement_context_provenance_version", "endorsement_context_id",
    "segment_starts_mid_line", "segment_has_complete_line",
    "segment_first_complete_line_start_char",
    "segment_first_complete_line_parent_start_char",
    "segment_first_complete_line_inference_display_start_char",
    "segment_ends_mid_line", "segment_last_complete_line_end_char",
    "segment_last_complete_line_parent_end_char",
    "segment_last_complete_line_inference_display_end_char",
    "endorsement_context_source_scope",
    "endorsement_context_derivation_method",
    "endorsement_context_parent_start_char",
    "endorsement_context_parent_end_char",
    "endorsement_context_parent_exact_match_count",
    "endorsement_context_parent_occurrence_ordinal",
    "endorsement_context_line_index_zero_based",
    "endorsement_context_scope_parent_start_char",
    "endorsement_context_scope_parent_end_char",
    "endorsement_context_source_segment_id",
    "endorsement_context_local_start_char",
    "endorsement_context_local_end_char",
    "endorsement_context_record_to_context_distance_rule",
    "endorsement_context_qualifying_wayback_url",
    "endorsement_context_qualifying_original_url",
    "endorsement_context_qualifying_original_path",
    "endorsement_context_source_urls_sha256",
    "endorsement_context_url_classifier_id",
    "endorsement_context_url_classifier_spec_sha256",
    "endorsement_context_structural_heading_spec_sha256",
    "endorsement_context_no_literal_anchor_reason",
)
COMMON_RECORD_COLUMNS = (
    "record_id", "parent_document_id", "segment_id", "source_segment_ids",
    "candidate_cycle_id", "variant_count", "overlap_resolution",
    "overlap_resolution_method", "source_model_item_id",
)
ISSUE_COLUMNS = COMMON_RECORD_COLUMNS + (
    "source_heading", "source_heading_start_char", "source_heading_end_char",
    "source_heading_parent_start_char", "source_heading_parent_end_char",
    "claim", "statement_kind", "evidence", "evidence_start_char",
    "evidence_end_char", "evidence_parent_start_char",
    "evidence_parent_end_char", "source_heading_exact_match_count",
    "source_heading_alignment_method", "evidence_exact_match_count",
    "evidence_alignment_method",
)
BIOGRAPHY_COLUMNS = COMMON_RECORD_COLUMNS + (
    "field", "value", "timing", "relationship", "evidence",
    "evidence_start_char", "evidence_end_char", "evidence_parent_start_char",
    "evidence_parent_end_char", "evidence_exact_match_count",
    "evidence_alignment_method",
)
# `endorser_category` is validated but not released. The 2026-09-20 accuracy
# check found it wrong on seven of 33 sampled endorsement records, and the error
# is systematic rather than random: a sitting official is recorded as
# `other_individual` when the office appears in a section heading rather than on
# the record line. A user filtering on `government_official` would undercount
# officials badly. `title`, `affiliation`, `jurisdiction` and the evidence text
# are released, so a user can recode the category from the published evidence.
# Hanno decided on 2026-09-20 to withhold the field rather than ship it or
# re-run the categorisation.
ENDORSEMENT_COLUMNS = COMMON_RECORD_COLUMNS + (
    "endorser_name", "entity_type", "title",
    "affiliation", "jurisdiction", "record_evidence",
    "record_evidence_start_char", "record_evidence_end_char",
    "record_evidence_parent_start_char", "record_evidence_parent_end_char",
    "endorsement_context_evidence", "endorsement_context_evidence_start_char",
    "endorsement_context_evidence_end_char",
    "endorsement_context_evidence_parent_start_char",
    "endorsement_context_evidence_parent_end_char", "support_mode",
    "endorsement_context_id", "endorsement_context_evidence_source_scope",
    "endorsement_context_record_distance_chars",
    "endorsement_context_qualifying_wayback_url",
    "endorsement_context_qualifying_original_url",
    "endorsement_context_qualifying_original_path",
    "record_evidence_exact_match_count", "record_evidence_alignment_method",
    "endorsement_context_evidence_exact_match_count",
    "endorsement_context_evidence_alignment_method",
    "line_inventory_id", "source_line_id", "stage_a_batch_id",
    "stage_a_slot", "stage_b_batch_id", "stage_b_slot",
    "model_record_index", "carrier_alignment_source_model_item_id",
    "inventory_owner_segment_id", "record_evidence_carrier_segment_id",
    "target_block_alignment_method", "target_block_exact_match_count",
)


class ReleaseInputError(ValueError):
    """The frozen bundle cannot support a public release."""


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _require(condition: Any, message: str) -> None:
    if not bool(condition):
        raise ReleaseInputError(message)


def _require_hex64(value: Any, label: str) -> str:
    text = str(value or "")
    _require(bool(HEX64.fullmatch(text)), f"{label} must be a lower-case SHA-256")
    return text


def _optional_text(value: Any, label: str) -> str | None:
    if value is None:
        return None
    _require(isinstance(value, str) and bool(value.strip()), f"{label} is blank")
    return value


def _required_text(value: Any, label: str) -> str:
    text = _optional_text(value, label)
    _require(text is not None, f"{label} is missing")
    return str(text)


def _integer(value: Any, label: str) -> int:
    _require(isinstance(value, int) and not isinstance(value, bool), f"{label} is not an integer")
    return int(value)


def _bundle_path(bundle_dir: Path, spec: Any, label: str) -> tuple[Path, dict[str, Any]]:
    _require(isinstance(spec, dict), f"bundle manifest {label} must be an object")
    rel = Path(str(spec.get("file") or ""))
    _require(str(rel) not in {"", "."}, f"bundle manifest {label} has no file")
    _require(not rel.is_absolute() and ".." not in rel.parts, f"unsafe bundle path: {rel}")
    path = (bundle_dir / rel).resolve()
    _require(bundle_dir.resolve() in path.parents, f"bundle file is outside the bundle: {rel}")
    _require(path.is_file(), f"bundle file is absent: {rel}")
    expected_hash = _require_hex64(spec.get("sha256"), f"{label}.sha256")
    _require(sha256_file(path) == expected_hash, f"bundle file hash differs: {rel}")
    return path, spec


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    opener = gzip.open if path.suffix == ".gz" else open
    rows: list[dict[str, Any]] = []
    with opener(path, "rt", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            value = json.loads(line)
            _require(isinstance(value, dict), f"{path.name}:{line_number} is not an object")
            rows.append(value)
    return rows


def _load_human_validation(
    path: Path, omission_report: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, bool]]:
    report = json.loads(path.read_text(encoding="utf-8"))
    _require(isinstance(report, dict), "human validation report must be an object")
    if report.get("status") == "pending":
        return report, {route: False for route in ROUTES}
    _require(report.get("status") == "passed", "human validation status is not passed")
    _require(
        report.get("schema_version") == SCHEMA_VERSION,
        f"human validation schema is not v{SCHEMA_VERSION}",
    )
    _require(report.get("independent_human_only") is True, "validation is not human-only")
    _require(report.get("coding_source") == "human", "validation coding source is not human")
    _require(report.get("human_only_attestation") == "confirmed_at_scoring", "human-only attestation is absent")
    _require(report.get("tuning_audit_excluded") is True, "the tuning audit was not excluded")
    _require(report.get("technical_gate_passed") is True, "human validation technical gate failed")
    _require(report.get("gates_passed") is True, "human validation gates did not pass")
    omission_confirmation = report.get("known_omission_human_confirmation")
    _require(
        isinstance(omission_confirmation, dict),
        "human validation lacks known-omission confirmation",
    )
    _require(
        omission_confirmation.get("expected_n") == 11
        and omission_confirmation.get("confirmed_n") == 11
        and omission_confirmation.get("all_confirmed") is True,
        "not all 11 known omissions received human confirmation",
    )
    omission_items = omission_report.get("items") or []
    expected_omissions = {
        str(item.get("omission_id")): item for item in omission_items
    }
    confirmed_items = omission_confirmation.get("items")
    _require(
        isinstance(confirmed_items, list) and len(confirmed_items) == 11,
        "known-omission confirmation items differ",
    )
    _require(
        {str(item.get("omission_id")) for item in confirmed_items}
        == set(expected_omissions),
        "known-omission confirmation IDs differ",
    )
    for item in confirmed_items:
        omission_id = str(item["omission_id"])
        expected = expected_omissions[omission_id]
        _require(
            str(item.get("parent_document_id"))
            == str(expected.get("parent_document_id"))
            and str(item.get("route")) == str(expected.get("route")),
            f"known omission {omission_id} confirmation provenance differs",
        )
        matched = set(str(value) for value in expected.get("matched_record_ids") or [])
        reported_matched = set(
            str(value) for value in item.get("matched_record_ids") or []
        )
        confirmed = set(
            str(value) for value in item.get("confirmed_record_ids") or []
        )
        _require(
            bool(matched)
            and reported_matched == matched
            and bool(confirmed)
            and confirmed <= matched,
            f"known omission {omission_id} record confirmations differ",
        )
        _require(
            item.get("semantic_recovery_confirmed") is True,
            f"known omission {omission_id} semantic recovery is unconfirmed",
        )
        links = item.get("record_link_confirmations")
        _require(
            isinstance(links, list)
            and len(links) == len(matched)
            and {str(link.get("record_id")) for link in links} == matched,
            f"known omission {omission_id} record-link coverage differs",
        )
        confirmed_links = {
            str(link.get("record_id"))
            for link in links
            if link.get("semantic_recovery_confirmed") is True
            and link.get("recovery_evidence_confirmed") is True
            and link.get("pair_confirmed") is True
        }
        _require(
            confirmed_links == confirmed,
            f"known omission {omission_id} confirmed links differ",
        )
    route_reports = report.get("routes")
    _require(isinstance(route_reports, dict), "validation report lacks route results")
    minimum_precision = {"issues": 60, "biography": 60, "endorsements": 60}
    passed: dict[str, bool] = {}
    for route in ROUTES:
        result = route_reports.get(route)
        _require(isinstance(result, dict), f"validation report lacks {route}")
        checks = [
            int(result.get("precision_n", 0)) >= minimum_precision[route],
            float(result.get("all_dimension_accuracy", -1)) >= 0.90,
            float(result.get("minimum_subtype_accuracy", -1)) >= 0.80,
            int(result.get("recall_n", 0)) >= 20,
            float(result.get("complete_page_recall", -1)) >= 0.90,
            result.get("passed") is True,
        ]
        if route == "endorsements":
            optional_counts = result.get("nonnull_optional_fields") or {}
            checks.extend([
                float(result.get("sampled_presence_rate", -1)) == 1.0,
                float(result.get("sampled_combined_support_rate", -1)) == 1.0,
                all(int(optional_counts.get(field, 0)) >= 20 for field in (
                    "title", "affiliation", "jurisdiction"
                )),
            ])
        passed[route] = all(checks)
    return report, passed


def load_bundle(
    bundle_dir: Path, *, production: bool = True
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any], dict[str, bool]]:
    manifest_path = bundle_dir / "bundle_manifest.json"
    _require(manifest_path.is_file(), "frozen bundle lacks bundle_manifest.json")
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    _require(isinstance(manifest, dict), "bundle manifest must be an object")
    _require(
        str(manifest.get("schema_version")) == SCHEMA_VERSION,
        f"bundle schema is not v{SCHEMA_VERSION}",
    )
    if production:
        _require(
            manifest.get("source_corpus_sha256")
            == FROZEN_SUFFIX_P_SOURCE_CORPUS_SHA256,
            "bundle source corpus is not the frozen suffix-p corpus",
        )
        _require(
            manifest.get("source_export_manifest_sha256")
            == FROZEN_SUFFIX_P_FULL_SOURCE_MANIFEST_SHA256,
            "bundle source export is not the frozen suffix-p full export",
        )
        _require(
            manifest.get("source_export_upstream_full_manifest_sha256") is None,
            "production bundle must come directly from the frozen full export",
        )
    segments_path, segments_spec = _bundle_path(
        bundle_dir, manifest.get("segments_file"), "segments_file"
    )
    validation_path, _ = _bundle_path(
        bundle_dir, manifest.get("human_validation_file"), "human_validation_file"
    )
    omission_path, omission_spec = _bundle_path(
        bundle_dir, manifest.get("known_omission_file"), "known_omission_file"
    )
    rows = _read_jsonl(segments_path)
    _require(int(segments_spec.get("rows", -1)) == len(rows), "bundle segment row count differs")
    ledger_declared = isinstance(manifest.get("endorsement_line_ledger"), dict)
    round4_declared = isinstance(
        manifest.get(ROUND4_LEDGER_MANIFEST_KEY), dict
    )
    # Exactly one endorsement architecture may be declared.  A bundle that
    # declares both, or names an architecture that does not match its ledger,
    # is refused before any table is built.
    _require(
        not (ledger_declared and round4_declared),
        "bundle declares two endorsement architectures",
    )
    declared_architecture = manifest.get("endorsement_architecture")
    if ledger_declared or round4_declared:
        expected_architecture = (
            ROUND4_ENDORSEMENT_ARCHITECTURE if round4_declared
            else LINE_LEDGER_ENDORSEMENT_ARCHITECTURE
        )
        _require(
            declared_architecture in {None, expected_architecture},
            "bundle endorsement architecture differs from its ledger",
        )
    else:
        _require(
            declared_architecture is None,
            "bundle names an endorsement architecture without a ledger",
        )
    _require(
        ledger_declared or round4_declared or not production,
        "production bundle lacks an endorsement record ledger",
    )
    if ledger_declared:
        try:
            manifest["_validated_endorsement_line_ledger"] = (
                load_endorsement_line_ledger_bundle(
                    bundle_dir, manifest, rows
                )
            )
        except (OSError, EOFError, json.JSONDecodeError, ValueError) as error:
            raise ReleaseInputError(
                f"bundle endorsement line-ledger replay differs: {error}"
            ) from error
    if round4_declared:
        try:
            manifest["_validated_endorsement_round4_ledger"] = (
                load_endorsement_round4_ledger_bundle(
                    bundle_dir, manifest, rows
                )
            )
        except (OSError, EOFError, json.JSONDecodeError, ValueError) as error:
            raise ReleaseInputError(
                f"bundle endorsement round-four replay differs: {error}"
            ) from error
    recomputed_output_summary = output_provenance_summary(rows)
    _require(
        _typed_equal(
            manifest.get("private_output_provenance_summary"),
            recomputed_output_summary,
        ),
        "bundle private-output provenance summary differs",
    )
    _require(
        recomputed_output_summary.get(
            "raw_structural_whitespace_validator_id"
        )
        == RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID
        and recomputed_output_summary.get(
            "raw_structural_whitespace_validator_spec_sha256"
        )
        == RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256
        and recomputed_output_summary.get(
            "raw_structural_whitespace_gate_passed"
        )
        is True,
        "bundle raw structural-whitespace gate did not pass",
    )
    recomputed_all_output = all_output_aggregate(rows)
    _require(
        _typed_equal(manifest.get("all_output_aggregate"), recomputed_all_output),
        "bundle all-output aggregate differs",
    )
    _validate_run_metadata_entries(manifest)
    _validate_seed_vary_repair(manifest)
    _require(
        type(recomputed_all_output.get("array_item_cap_reached_outputs")) is int
        and recomputed_all_output["array_item_cap_reached_outputs"] == 0
        and type(
            recomputed_all_output.get("model_item_repetition_error_outputs")
        )
        is int
        and recomputed_all_output["model_item_repetition_error_outputs"] == 0,
        "bundle contains a pre-alignment model-item guard failure",
    )
    omission_report = json.loads(omission_path.read_text(encoding="utf-8"))
    omission_items = omission_report.get("items") if isinstance(omission_report, dict) else None
    _require(int(omission_spec.get("rows", -1)) == 11, "known omission manifest count is not 11")
    _require(isinstance(omission_items, list) and len(omission_items) == 11, "known omission report does not contain 11 items")
    _require(omission_report.get("known_omissions") == 11, "known omission report count differs")
    _require(omission_report.get("recovered") == 11, "known omission recovery count differs")
    _require(omission_report.get("all_recovered") is True, "known omission gate did not pass")
    _require(
        omission_report.get("matching_spec") == KNOWN_OMISSION_MATCHING_SPEC
        and omission_report.get("matching_spec_sha256")
        == KNOWN_OMISSION_MATCHING_SPEC_SHA256
        and manifest.get("known_omission_matching_spec_sha256")
        == KNOWN_OMISSION_MATCHING_SPEC_SHA256,
        "known omission matching policy provenance differs",
    )
    _require(all(item.get("recovered") is True for item in omission_items), "a known omission was not recovered")
    _require(
        omission_report.get("human_confirmation_required") is True
        and omission_report.get("human_confirmation_status") == "pending",
        "known omission report does not preserve the separate human gate",
    )
    omission_ids = [str(item.get("omission_id") or "") for item in omission_items]
    _require(all(omission_ids) and len(set(omission_ids)) == 11, "known omission IDs are missing or duplicated")
    resolved_ids = {
        str(record.get("record_id"))
        for row in rows
        for record in (row.get("resolved_records") or [])
    }
    for item in omission_items:
        matches = set(str(value) for value in (item.get("matched_record_ids") or []))
        _require(bool(matches), f"known omission {item.get('omission_id')} has no matched record")
        _require(matches <= resolved_ids, f"known omission {item.get('omission_id')} links to an unknown record")
        _require(
            int(item.get("source_evidence_exact_occurrences", 0)) > 0
            and item.get("human_confirmation_required") is True,
            f"known omission {item.get('omission_id')} lacks exact-source recovery provenance",
        )
    omission_derived_fields = {
        "source_evidence_exact_occurrences", "matched_record_ids", "recovered",
        "human_confirmation_required", "expected_value_confirmation_required",
        "human_confirmed_semantic_recovery",
    }
    original_omissions = [
        {key: value for key, value in item.items() if key not in omission_derived_fields}
        for item in omission_items
    ]
    parent_text = validate_parent_sources({
        str(row.get("document_id")): row for row in rows
    })
    recomputed_omissions = match_known_omissions(
        original_omissions, rows, parent_text
    )
    for stored, recomputed in zip(
        sorted(omission_items, key=lambda value: str(value["omission_id"])),
        sorted(
            recomputed_omissions["items"],
            key=lambda value: str(value["omission_id"]),
        ),
    ):
        for field in (
            "source_evidence_exact_occurrences", "matched_record_ids", "recovered",
            "expected_value_confirmation_required",
        ):
            _require(
                stored.get(field) == recomputed.get(field),
                f"known omission {stored.get('omission_id')} {field} differs from recomputation",
            )
    validation, route_passed = _load_human_validation(
        validation_path, omission_report
    )
    if all(route_passed.values()):
        _require(
            validation.get("source_corpus_sha256")
            == manifest.get("source_corpus_sha256"),
            "human validation used a different source corpus",
        )
        _require(
            manifest.get("human_validation_gate_passed") is True,
            "bundle manifest lacks the human validation gate",
        )
        _require(
            manifest.get("release_status") == "human_validation_passed",
            "bundle release status is not human-validation passed",
        )
    return manifest, rows, validation, route_passed


def _wayback_original(url: str) -> str:
    parts = urlsplit(url)
    _require(
        (parts.hostname or "").lower() == "web.archive.org",
        f"source URL does not use web.archive.org: {url}",
    )
    match = WAYBACK_TARGET.match(parts.path)
    _require(match is not None, f"source URL is not a Wayback capture: {url}")
    original = match.group(1)
    if parts.query:
        original += "?" + parts.query
    _require(urlsplit(original).scheme.lower() in {"http", "https"}, f"invalid original URL: {original}")
    return original


def _timestamp(value: Any, label: str) -> str:
    text = str(value or "").removesuffix(".0")
    _require(bool(re.fullmatch(r"[0-9]{8,14}", text)), f"{label} is not a Wayback timestamp")
    return text


def _load_core(core_release_dir: Path) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    universe_path = core_release_dir / "candidate_year_universe.csv"
    raw_path = core_release_dir / "raw_corpus.parquet"
    _require(universe_path.is_file(), "core release lacks candidate_year_universe.csv")
    _require(raw_path.is_file(), "core release lacks raw_corpus.parquet")
    universe = pd.read_csv(universe_path, dtype={"candidate_cycle_id": "string"}, low_memory=False)
    _require("candidate_cycle_id" in universe, "core universe lacks candidate_cycle_id")
    _require(universe["candidate_cycle_id"].notna().all(), "core universe has a missing candidate ID")
    _require(universe["candidate_cycle_id"].is_unique, "core universe candidate ID is not unique")
    required = {
        "candidate_cycle_id", "candidate_icpsr", "state", "office", "year",
        "party", "date", "snap_url", "page_type", "text_snap_content",
    }
    schema = set(pq.ParquetFile(raw_path).schema_arrow.names)
    _require(not (required - schema), f"raw corpus lacks columns: {sorted(required - schema)}")
    raw = pq.read_table(raw_path, columns=sorted(required)).to_pandas()
    # The population filter does NOT run here. The packager checks several
    # equalities between the bundle and the core corpus, and the bundle was
    # built from the unfiltered export, so filtering one side breaks them.
    # Official government pages are dropped from the released tables instead,
    # which is where the release population is defined. See _drop_excluded_hosts.
    return universe, raw, sha256_file(raw_path)


EXCLUDED_HOST_SUFFIX = ".gov"
EXCLUDED_HOST_RULE_ID = "official_government_host_not_a_campaign_website_v1"


def _url_host(url: Any) -> str:
    """Return the archived target's host, not the Wayback host."""
    text = str(url or "")
    if not text:
        return ""
    match = re.match(r"https?://web\.archive\.org/web/\d+[^/]*/(.+)", text)
    target = match.group(1) if match else text
    if "://" not in target:
        target = "http://" + target
    return urlparse(target).netloc.lower()


def _drop_excluded_hosts(raw: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Report which core rows sit on an official government host.

    Hanno approved dropping them on 2026-09-24. The population is campaign
    websites, and an official House or Senate page is a different object. Four
    of these hosts also carry the wrong candidate, for example
    `billnelson.senate.gov` filed under a Nebraska candidate named Nelson.

    This returns the kept rows and a report. The caller uses the report to
    filter the released tables; it must not filter the core corpus, because the
    packager checks equalities between the bundle and that corpus.
    """
    hosts = raw["snap_url"].map(_url_host)
    excluded = hosts.str.endswith(EXCLUDED_HOST_SUFFIX, na=False)
    target = raw.loc[excluded]
    report = {
        "rule_id": EXCLUDED_HOST_RULE_ID,
        "host_suffix": EXCLUDED_HOST_SUFFIX,
        "excluded_raw_rows": int(excluded.sum()),
        "excluded_hosts": sorted(set(hosts[excluded])),
        "excluded_candidate_years": sorted(
            set(target["candidate_cycle_id"].astype(str))
        ),
    }
    return raw.loc[~excluded].copy(), report


def excluded_parent_document_ids(raw: pd.DataFrame) -> tuple[set[str], dict[str, Any]]:
    """Return the parent IDs to drop from the released tables, and the report."""
    _kept, report = _drop_excluded_hosts(raw)
    hosts = raw["snap_url"].map(_url_host)
    excluded = raw.loc[hosts.str.endswith(EXCLUDED_HOST_SUFFIX, na=False)].copy()
    if excluded.empty:
        report["excluded_parent_documents"] = 0
        return set(), report
    # Only the three extraction routes become parent documents, so restrict to
    # them; otherwise the reported count includes page types the release never
    # carries and stops matching the released tables.
    excluded = excluded.loc[excluded["page_type"].isin(ROUTES)].copy()
    text = excluded["text_snap_content"].fillna("").astype(str)
    excluded = excluded.loc[text.str.strip().ne("")].copy()
    # itertuples renames a leading-underscore column to a positional name, so
    # the digest column must not start with one.
    excluded["raw_text_sha"] = (
        excluded["text_snap_content"].fillna("").astype(str).map(sha256_text)
    )
    ids = {
        sha256_text(
            f"{row.candidate_cycle_id}\n{row.page_type}\n{row.raw_text_sha}"
        )
        for row in excluded.itertuples()
    }
    report["excluded_parent_documents"] = len(ids)
    return ids, report


def _core_source_state(raw: pd.DataFrame) -> dict[tuple[str, str], dict[str, int]]:
    target = raw.loc[raw["page_type"].isin(ROUTES)].copy()
    target["_text"] = target["text_snap_content"].fillna("").astype(str)
    target["_extractable"] = target["_text"].str.strip().ne("")
    result: dict[tuple[str, str], dict[str, int]] = {}
    for key, rows in target.groupby(["candidate_cycle_id", "page_type"], sort=False):
        result[(str(key[0]), str(key[1]))] = {
            "n_raw_pages": int(len(rows)),
            "n_extractable_pages": int(rows["_extractable"].sum()),
        }
    return result


def _expected_core_documents(raw: pd.DataFrame) -> dict[str, dict[str, Any]]:
    target = raw.loc[raw["page_type"].isin(ROUTES)].copy()
    target["_raw_text"] = target["text_snap_content"].fillna("").astype(str)
    target = target.loc[target["_raw_text"].str.strip().ne("")].copy()
    target["_raw_text_sha256"] = target["_raw_text"].map(sha256_text)
    target["_parent_document_id"] = target.apply(
        lambda row: sha256_text(
            f"{row['candidate_cycle_id']}\n{row['page_type']}\n{row['_raw_text_sha256']}"
        ),
        axis=1,
    )
    expected: dict[str, dict[str, Any]] = {}
    for parent_id, rows in target.groupby("_parent_document_id", sort=True):
        metadata_columns = (
            "candidate_cycle_id", "candidate_icpsr", "state", "office", "year",
            "party", "page_type", "_raw_text", "_raw_text_sha256",
        )
        for column in metadata_columns:
            _require(rows[column].nunique(dropna=False) == 1, f"core parent {parent_id} has conflicting {column}")
        first = rows.iloc[0]
        dates = sorted({_timestamp(value, f"{parent_id}.date") for value in rows["date"]})
        urls = sorted(set(rows["snap_url"].dropna().astype(str)))
        _require(bool(urls), f"core parent {parent_id} has no source URL")
        expected[str(parent_id)] = {
            "parent_document_id": str(parent_id),
            "candidate_cycle_id": str(first["candidate_cycle_id"]),
            "candidate_icpsr": str(first["candidate_icpsr"]),
            "state": str(first["state"]),
            "office": str(first["office"]),
            "year": int(first["year"]),
            "party": str(first["party"]),
            "route": str(first["page_type"]),
            "raw_text_sha256": str(first["_raw_text_sha256"]),
            "source_text": str(first["_raw_text"]).replace("#+#", "\n"),
            "source_text_transform_changed": (
                str(first["_raw_text"]).replace("#+#", "\n")
                != str(first["_raw_text"])
            ),
            "n_raw_occurrences": int(len(rows)),
            "first_capture_timestamp": dates[0],
            "last_capture_timestamp": dates[-1],
            "source_urls": urls,
            "original_urls": sorted({_wayback_original(url) for url in urls}),
        }
    return expected


def _row_segment_id(row: dict[str, Any]) -> str:
    segment_id = str(row.get("segment_id") or row.get("document_id") or "")
    return _require_hex64(segment_id, "segment_id")


def _schema_valid(row: dict[str, Any]) -> bool:
    if "schema_valid" in row:
        return row.get("schema_valid") is True
    validation = row.get("validation")
    return isinstance(validation, dict) and validation.get("schema_valid") is True


def _endorsement_architecture(manifest: Mapping[str, Any]) -> str | None:
    """Return the declared endorsement architecture, or None when absent."""
    if isinstance(manifest.get("_validated_endorsement_round4_ledger"), dict):
        return ROUND4_ENDORSEMENT_ARCHITECTURE
    if isinstance(manifest.get("_validated_endorsement_line_ledger"), dict):
        return LINE_LEDGER_ENDORSEMENT_ARCHITECTURE
    return None


def _endorsement_route_contract(manifest: Mapping[str, Any]) -> str | None:
    """Return the frozen contract hash of the declared endorsement route."""
    architecture = _endorsement_architecture(manifest)
    if architecture == ROUND4_ENDORSEMENT_ARCHITECTURE:
        return get_round4_production_contract()[
            "round4_production_contract_sha256"
        ]
    if architecture == LINE_LEDGER_ENDORSEMENT_ARCHITECTURE:
        return get_line_ledger_contract()["line_ledger_contract_sha256"]
    return None


def _manifest_metadata(
    manifest: dict[str, Any], segment_rows: list[dict[str, Any]]
) -> dict[str, Any]:
    result: dict[str, Any] = {"schema_version": str(manifest.get("schema_version"))}
    ledger = manifest.get("_validated_endorsement_line_ledger")
    if ledger is not None:
        _require(
            isinstance(ledger, dict) and isinstance(ledger.get("summary"), dict),
            "bundle validated endorsement line ledger is absent",
        )
        contract = get_line_ledger_contract()
        ledger_summary = ledger["summary"]
        _require(
            _typed_equal(ledger_summary.get("line_ledger_contract"), contract)
            and ledger_summary.get("line_ledger_contract_sha256")
            == contract["line_ledger_contract_sha256"],
            "bundle endorsement line-ledger contract differs",
        )
        result["endorsement_route_contract_sha256"] = contract[
            "line_ledger_contract_sha256"
        ]
        result["endorsement_architecture"] = (
            LINE_LEDGER_ENDORSEMENT_ARCHITECTURE
        )
    round4_ledger = manifest.get("_validated_endorsement_round4_ledger")
    if round4_ledger is not None:
        _require(
            isinstance(round4_ledger, dict)
            and isinstance(round4_ledger.get("summary"), dict),
            "bundle validated endorsement round-four ledger is absent",
        )
        round4_contract = get_round4_production_contract()
        round4_summary = round4_ledger["summary"]
        _require(
            _typed_equal(
                round4_summary.get("round4_production_contract"),
                round4_contract,
            )
            and round4_summary.get("round4_production_contract_sha256")
            == round4_contract["round4_production_contract_sha256"],
            "bundle endorsement round-four contract differs",
        )
        result["endorsement_route_contract_sha256"] = round4_contract[
            "round4_production_contract_sha256"
        ]
        result["endorsement_architecture"] = ROUND4_ENDORSEMENT_ARCHITECTURE
    try:
        selected_context_census = validate_endorsement_context_manifest(
            manifest.get("endorsement_context_provenance"),
            segment_rows,
        )
    except ValueError as error:
        raise ReleaseInputError(
            f"bundle endorsement-context provenance differs: {error}"
        ) from error
    _require(
        _typed_equal(
            manifest.get(
                "endorsement_context_selected_segment_complete_line_census"
            ),
            selected_context_census,
        ),
        "bundle selected endorsement complete-line census differs",
    )
    upstream_source_manifest = manifest.get(
        "source_export_upstream_full_manifest_sha256"
    )
    _require(
        upstream_source_manifest is None
        or bool(HEX64.fullmatch(str(upstream_source_manifest))),
        "bundle upstream full source-manifest hash is invalid",
    )
    for field in (
        "model_id", "model_revision", "runtime_lock_sha256",
        "extraction_contract_sha256", "alignment_id",
        "alignment_spec_sha256", "semantic_validation_spec_sha256",
        "endorsement_context_provenance_spec_sha256",
        "user_message_template_id", "user_message_template_sha256",
        "inference_display_escape_id",
        "inference_display_escape_spec_sha256",
        "json_string_control_canonicalizer_id",
        "json_string_control_canonicalizer_spec_sha256",
        "array_item_cap_spec_id", "array_item_cap_spec_sha256",
        "model_item_repetition_spec_id",
        "model_item_repetition_spec_sha256",
    ):
        result[field] = _required_text(manifest.get(field), f"bundle manifest {field}")
    for field in (
        "runtime_lock_sha256", "extraction_contract_sha256",
        "alignment_spec_sha256", "semantic_validation_spec_sha256",
        "endorsement_context_provenance_spec_sha256",
        "user_message_template_sha256", "inference_display_escape_spec_sha256",
        "json_string_control_canonicalizer_spec_sha256",
        "array_item_cap_spec_sha256",
        "model_item_repetition_spec_sha256",
    ):
        _require_hex64(result[field], f"bundle manifest {field}")
    for field in ("prompt_sha256", "json_schema_sha256"):
        values = manifest.get(field)
        _require(isinstance(values, dict) and set(values) == set(ROUTES), f"bundle manifest {field} must map all routes")
        result[field] = {
            route: _require_hex64(values[route], f"bundle manifest {field}.{route}")
            for route in ROUTES
        }
    route_caps = manifest.get("route_array_item_caps")
    _require(
        _typed_equal(route_caps, V2_CONTRACT["route_array_item_caps"]),
        "bundle route array-item caps differ",
    )
    result["route_array_item_caps"] = dict(route_caps)
    _require(
        result["array_item_cap_spec_id"]
        == V2_CONTRACT["array_item_cap_spec_id"]
        and result["array_item_cap_spec_sha256"]
        == V2_CONTRACT["array_item_cap_spec_sha256"]
        and result["model_item_repetition_spec_id"]
        == V2_CONTRACT["model_item_repetition_spec_id"]
        and result["model_item_repetition_spec_sha256"]
        == V2_CONTRACT["model_item_repetition_spec_sha256"],
        "bundle model-item guard provenance differs",
    )
    _require(
        manifest.get("structured_decoding_backend")
        == STRUCTURED_DECODING_BACKEND,
        "bundle structured-decoding backend differs",
    )
    _require(
        manifest.get("structured_decoding_disable_any_whitespace")
        is STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
        "bundle structured-decoding whitespace policy differs",
    )
    result["structured_decoding_backend"] = STRUCTURED_DECODING_BACKEND
    result["structured_decoding_disable_any_whitespace"] = (
        STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE
    )
    generation_config = manifest.get("generation_config")
    generation_hash = _require_hex64(
        manifest.get("generation_config_sha256"),
        "bundle manifest generation_config_sha256",
    )
    _require(
        isinstance(generation_config, dict),
        "bundle manifest generation_config must be an object",
    )
    _require(
        sha256_text(canonical_json(generation_config)) == generation_hash,
        "bundle generation config hash differs",
    )
    expected_generation = {
        "generation_config_version": GENERATION_CONFIG_VERSION,
        "schema_version": result["schema_version"],
        "model_id": result["model_id"],
        "model_revision": result["model_revision"],
        "runtime_lock_sha256": result["runtime_lock_sha256"],
        "prompt_sha256": result["prompt_sha256"],
        "json_schema_sha256": result["json_schema_sha256"],
        "extraction_contract_sha256": result["extraction_contract_sha256"],
        "alignment_spec_sha256": result["alignment_spec_sha256"],
        "semantic_validation_spec_sha256": result[
            "semantic_validation_spec_sha256"
        ],
        "endorsement_context_provenance_spec_sha256": result[
            "endorsement_context_provenance_spec_sha256"
        ],
        "user_message_template_id": result["user_message_template_id"],
        "user_message_template_sha256": result[
            "user_message_template_sha256"
        ],
        "inference_display_escape_id": result[
            "inference_display_escape_id"
        ],
        "inference_display_escape_spec_sha256": result[
            "inference_display_escape_spec_sha256"
        ],
        "json_string_control_canonicalizer_id": result[
            "json_string_control_canonicalizer_id"
        ],
        "json_string_control_canonicalizer_spec_sha256": result[
            "json_string_control_canonicalizer_spec_sha256"
        ],
        "array_item_cap_spec_id": result["array_item_cap_spec_id"],
        "array_item_cap_spec_sha256": result["array_item_cap_spec_sha256"],
        "route_array_item_caps": result["route_array_item_caps"],
        "model_item_repetition_spec_id": result[
            "model_item_repetition_spec_id"
        ],
        "model_item_repetition_spec_sha256": result[
            "model_item_repetition_spec_sha256"
        ],
        "temperature": 0.0,
        "thinking_enabled": False,
        "compact_repair": False,
    }
    for field, expected in expected_generation.items():
        _require(
            _typed_equal(generation_config.get(field), expected),
            f"bundle generation config has wrong {field}",
        )
    _require(
        generation_config.get("structured_decoding")
        == EXPECTED_STRUCTURED_DECODING,
        "bundle structured-decoding identity differs",
    )
    for field, expected in (
        ("seed", EXPECTED_GENERATION_SEED),
        ("max_model_len", EXPECTED_MAX_MODEL_LEN),
        ("max_output_tokens", EXPECTED_MAX_OUTPUT_TOKENS),
    ):
        _require(
            generation_config.get(field) == expected,
            f"bundle generation config has wrong {field}",
        )
    _require(
        generation_hash == EXPECTED_GENERATION_CONFIG_SHA256,
        "bundle generation config is not the frozen v2.1 identity",
    )
    result["generation_config_sha256"] = generation_hash
    return result


def _validate_run_metadata_entries(manifest: dict[str, Any]) -> None:
    entries = manifest.get("run_metadata")
    _require(isinstance(entries, list) and bool(entries), "bundle lacks run metadata provenance")
    files: list[str] = []
    for entry in entries:
        _require(isinstance(entry, dict), "bundle run metadata entry is invalid")
        filename = _required_text(entry.get("file"), "run metadata file")
        _require(Path(filename).name == filename, "run metadata file name is unsafe")
        files.append(filename)
        _require_hex64(entry.get("sha256"), f"run metadata {filename} hash")
        _require(
            entry.get("generation_config_sha256")
            == manifest.get("generation_config_sha256"),
            f"run metadata {filename} generation identity differs",
        )
        _require(
            entry.get("structured_decoding_backend")
            == STRUCTURED_DECODING_BACKEND,
            f"run metadata {filename} structured-decoding backend differs",
        )
        _require(
            entry.get("structured_decoding_disable_any_whitespace")
            is STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
            f"run metadata {filename} structured-decoding whitespace policy differs",
        )
        for field in (
            "array_item_cap_spec_id",
            "array_item_cap_spec_sha256",
            "route_array_item_caps",
            "model_item_repetition_spec_id",
            "model_item_repetition_spec_sha256",
        ):
            _require(
                _typed_equal(entry.get(field), manifest.get(field)),
                f"run metadata {filename} {field} differs",
            )
        _require(
            isinstance(entry.get("recovery_attestation_without_inference"), bool),
            f"run metadata {filename} recovery attestation is absent",
        )
        for field in RUN_METADATA_SCALAR_FIELDS:
            _require(
                isinstance(entry.get(field), int)
                and not isinstance(entry.get(field), bool)
                and int(entry[field]) >= 0,
                f"run metadata {filename} {field} is invalid",
            )
        for field in RUN_METADATA_MAPPING_FIELDS:
            value = entry.get(field)
            _require(
                isinstance(value, dict)
                and all(
                    isinstance(count, int)
                    and not isinstance(count, bool)
                    and count >= 0
                    for count in value.values()
                ),
                f"run metadata {filename} {field} is invalid",
            )
        for field in RUN_METADATA_MAX_MAPPING_FIELDS:
            value = entry.get(field)
            _require(
                isinstance(value, dict)
                and set(value) == set(ROUTES)
                and all(
                    isinstance(count, int)
                    and not isinstance(count, bool)
                    and count >= 0
                    for count in value.values()
                ),
                f"run metadata {filename} {field} is invalid",
            )
        try:
            validate_run_metadata_accounting(entry, f"run metadata {filename}")
        except ValueError as error:
            raise ReleaseInputError(str(error)) from error
    _require(len(files) == len(set(files)), "bundle run metadata files are duplicated")
    shard_counts = {entry.get("shard_count") for entry in entries}
    if shard_counts == {None}:
        _require(
            len(entries) == 1
            and entries[0].get("shard_index") is None
            and entries[0]["file"] == "run_metadata.json",
            "unsharded run metadata coverage differs",
        )
    else:
        _require(
            len(shard_counts) == 1
            and None not in shard_counts
            and all(
                isinstance(value, int) and not isinstance(value, bool)
                for value in shard_counts
            ),
            "run metadata shard counts differ",
        )
        shard_count = next(iter(shard_counts))
        indexes = [entry.get("shard_index") for entry in entries]
        _require(
            shard_count > 0
            and all(
                isinstance(value, int) and not isinstance(value, bool)
                for value in indexes
            )
            and set(indexes) == set(range(shard_count))
            and all(
                entry["file"]
                == f"run_metadata_{entry['shard_index']:05d}.json"
                for entry in entries
            ),
            "run metadata shard coverage differs",
        )
    _require(
        sum(int(entry["documents_selected_for_shard"]) for entry in entries)
        == int(manifest.get("segments", -1)),
        "run metadata selected-document coverage differs from bundle segments",
    )
    observed_aggregate = run_metadata_aggregate(entries)
    _require(
        _typed_equal(manifest.get("run_metadata_aggregate"), observed_aggregate),
        "bundle run-metadata aggregate differs",
    )
    _require(
        isinstance(manifest.get("all_output_aggregate"), dict),
        "bundle all-output aggregate is absent",
    )
    # A window a seed-vary repair replaced shows its repair row in the corpus and
    # its primary row in the primary run metadata. The metadata is therefore
    # checked against what the primary run alone produced. A bundle with no
    # repair carries the same numbers in both.
    primary_equivalent = manifest.get("primary_equivalent_output_aggregate")
    if primary_equivalent is None and not manifest.get("seed_vary_repair"):
        # Without a repair the two aggregates are identical by construction, so
        # a bundle built before this field existed is still checked exactly.
        primary_equivalent = manifest.get("all_output_aggregate")
    _require(
        isinstance(primary_equivalent, dict),
        "bundle primary-equivalent output aggregate is absent",
    )
    try:
        validate_run_metadata_against_all_output(
            observed_aggregate,
            primary_equivalent,
            "bundle run metadata aggregate",
        )
    except ValueError as error:
        raise ReleaseInputError(str(error)) from error


def _repaired_window_identities(manifest: dict[str, Any]) -> dict[str, str]:
    """Map each window a repair recovered to that attempt's generation identity.

    A later attempt only covers what an earlier one left at the cap, so a window
    appears as repaired in at most one attempt. Two attempts claiming the same
    window would make the row's identity ambiguous, so that is refused.
    """
    identities: dict[str, str] = {}
    for attempt in manifest.get("seed_vary_repair") or []:
        for window in attempt.get("windows") or []:
            if window.get("repaired") is not True:
                continue
            document_id = str(window.get("document_id"))
            _require(
                document_id not in identities,
                f"{document_id} is recorded as repaired by two attempts",
            )
            identities[document_id] = str(
                attempt.get("repair_generation_config_sha256")
            )
    return identities


def _unrepaired_cap_windows(manifest: dict[str, Any]) -> frozenset[str]:
    """Return the windows the last repair attempt left at the output cap.

    These are the only rows the bundle may carry with a `length` finish reason.
    A window an earlier attempt recovered is not here, because a later attempt is
    scoped to what the previous one left, so the last attempt's unrepaired list is
    the surviving set. A bundle with no repair returns an empty set, which refuses
    every cap-limited row, as before.
    """
    attempts = manifest.get("seed_vary_repair")
    if not isinstance(attempts, list) or not attempts:
        return frozenset()
    last = max(attempts, key=lambda attempt: int(attempt.get("attempt_index", 0)))
    windows = last.get("windows")
    _require(isinstance(windows, list), "seed-vary repair attempt lacks its windows")
    unrepaired = frozenset(
        str(window.get("document_id"))
        for window in windows
        if isinstance(window, dict) and window.get("repaired") is False
    )
    _require(
        len(unrepaired) == int(last.get("unrepaired_count", -1)),
        "seed-vary repair unrepaired windows differ from the recorded count",
    )
    return unrepaired


def _validate_repair_run_metadata_entry(
    entry: Any,
    manifest: dict[str, Any],
    attempt_index: int,
    target_count: int,
    label: str,
) -> None:
    """Check one repair attempt's own run-metadata entry.

    The entry records the job that finished the attempt, so its this-run counters
    cover that job. Two fields cover the whole attempt: the selected-window count
    and the completeness attestation. Every identity field must be the repair
    identity for this attempt, and every specification field must equal the
    bundle's.
    """
    _require(isinstance(entry, dict), f"{label} run metadata is absent")
    filename = _required_text(entry.get("file"), f"{label} run metadata file")
    _require(
        Path(filename).name == filename,
        f"{label} run metadata file name is unsafe",
    )
    _require_hex64(entry.get("sha256"), f"{label} run metadata hash")
    _require(
        entry.get("attempt_index") == attempt_index,
        f"{label} run metadata names another attempt",
    )
    _require(
        entry.get("generation_config_sha256")
        == EXPECTED_REPAIR_GENERATION_CONFIG_SHA256S[attempt_index],
        f"{label} run metadata generation identity differs",
    )
    _require(
        entry.get("seed") == EXPECTED_REPAIR_GENERATION_SEEDS[attempt_index],
        f"{label} run metadata seed differs",
    )
    _require(
        entry.get("generation_config_sha256")
        != manifest.get("generation_config_sha256"),
        f"{label} run metadata carries the primary identity",
    )
    _require(
        entry.get("documents_selected_for_shard") == target_count,
        f"{label} run metadata selected windows differ from the target count",
    )
    _require(
        entry.get("outputs_complete_for_selected_shard") is True,
        f"{label} run metadata does not attest complete outputs",
    )
    _require(
        entry.get("recovery_attestation_without_inference") is False,
        f"{label} run metadata recovery attestation differs",
    )
    _require(
        entry.get("structured_decoding_backend") == STRUCTURED_DECODING_BACKEND,
        f"{label} run metadata structured-decoding backend differs",
    )
    _require(
        entry.get("structured_decoding_disable_any_whitespace")
        is STRUCTURED_DECODING_DISABLE_ANY_WHITESPACE,
        f"{label} run metadata structured-decoding whitespace policy differs",
    )
    for field in (
        "array_item_cap_spec_id",
        "array_item_cap_spec_sha256",
        "route_array_item_caps",
        "model_item_repetition_spec_id",
        "model_item_repetition_spec_sha256",
    ):
        _require(
            _typed_equal(entry.get(field), manifest.get(field)),
            f"{label} run metadata {field} differs",
        )
    for field in RUN_METADATA_SCALAR_FIELDS:
        _require(
            isinstance(entry.get(field), int)
            and not isinstance(entry.get(field), bool)
            and int(entry[field]) >= 0,
            f"{label} run metadata {field} is invalid",
        )
    for field in RUN_METADATA_MAPPING_FIELDS:
        value = entry.get(field)
        _require(
            isinstance(value, dict)
            and all(
                isinstance(count, int)
                and not isinstance(count, bool)
                and count >= 0
                for count in value.values()
            ),
            f"{label} run metadata {field} is invalid",
        )
    for field in RUN_METADATA_MAX_MAPPING_FIELDS:
        value = entry.get(field)
        _require(
            isinstance(value, dict)
            and set(value) == set(ROUTES)
            and all(
                isinstance(count, int)
                and not isinstance(count, bool)
                and count >= 0
                for count in value.values()
            ),
            f"{label} run metadata {field} is invalid",
        )
    try:
        validate_run_metadata_accounting(entry, f"{label} run metadata")
    except ValueError as error:
        raise ReleaseInputError(str(error)) from error


def _validate_seed_vary_repair(manifest: dict[str, Any]) -> None:
    """Check the seed-vary repair block, one entry per attempt.

    A repair changes the seed, so its generation identity differs from the frozen
    one. It therefore cannot enter the primary run-metadata list, which binds one
    identity across the bundle. This block carries the repair identity instead: the
    attestation for each attempt, the outcome of every window, and the run metadata
    of the job that finished the attempt.

    A bundle with no repair carries no block, and every check here is skipped.
    """
    attempts = manifest.get("seed_vary_repair")
    if attempts is None:
        return
    _require(
        isinstance(attempts, list) and bool(attempts),
        "bundle seed-vary repair block holds no attempt",
    )
    ordered: list[int] = []
    previous_unrepaired: int | None = None
    for attempt in attempts:
        _require(isinstance(attempt, dict), "seed-vary repair attempt is invalid")
        index = attempt.get("attempt_index")
        _require(
            isinstance(index, int)
            and not isinstance(index, bool)
            and index in EXPECTED_REPAIR_GENERATION_SEEDS,
            "seed-vary repair attempt index is not declared",
        )
        label = f"seed-vary repair attempt {index}"
        _require(
            not ordered or index > ordered[-1],
            f"{label} does not follow the previous attempt",
        )
        ordered.append(int(index))
        _require(
            attempt.get("repair_seed") == EXPECTED_REPAIR_GENERATION_SEEDS[index],
            f"{label} seed is not the declared seed",
        )
        _require(
            attempt.get("repair_generation_config_sha256")
            == EXPECTED_REPAIR_GENERATION_CONFIG_SHA256S[index],
            f"{label} generation identity is not the declared identity",
        )
        _require(
            attempt.get("primary_generation_config_sha256")
            == manifest.get("generation_config_sha256"),
            f"{label} primary identity differs from the bundle",
        )
        filename = _required_text(
            attempt.get("repair_manifest_file"), f"{label} attestation file"
        )
        _require(
            Path(filename).name == filename,
            f"{label} attestation file name is unsafe",
        )
        _require_hex64(
            attempt.get("repair_manifest_sha256"), f"{label} attestation hash"
        )
        _required_text(attempt.get("reason"), f"{label} reason")
        counts = {
            field: attempt.get(field)
            for field in ("target_count", "repaired_count", "unrepaired_count")
        }
        _require(
            all(
                isinstance(value, int)
                and not isinstance(value, bool)
                and value >= 0
                for value in counts.values()
            ),
            f"{label} counts are invalid",
        )
        _require(
            int(counts["repaired_count"]) + int(counts["unrepaired_count"])
            == int(counts["target_count"]),
            f"{label} counts do not reconcile",
        )
        windows = attempt.get("windows")
        _require(
            isinstance(windows, list)
            and len(windows) == int(counts["target_count"]),
            f"{label} window list differs from the target count",
        )
        document_ids: list[str] = []
        repaired = 0
        for window in windows:
            _require(isinstance(window, dict), f"{label} window is invalid")
            document_ids.append(
                _required_text(
                    window.get("document_id"), f"{label} window document ID"
                )
            )
            _required_text(window.get("chunk"), f"{label} window chunk")
            _require(
                isinstance(window.get("repaired"), bool),
                f"{label} window outcome is invalid",
            )
            repaired += 1 if window["repaired"] else 0
        _require(
            len(set(document_ids)) == len(document_ids),
            f"{label} names one window twice",
        )
        _require(
            repaired == int(counts["repaired_count"]),
            f"{label} repaired windows differ from the count",
        )
        if previous_unrepaired is not None:
            _require(
                int(counts["target_count"]) == previous_unrepaired,
                f"{label} does not cover the windows the previous attempt left "
                "at the cap",
            )
        previous_unrepaired = int(counts["unrepaired_count"])
        _validate_repair_run_metadata_entry(
            attempt.get("final_job_run_metadata"),
            manifest,
            int(index),
            int(counts["target_count"]),
            label,
        )


def _public_metadata(metadata: dict[str, Any], route: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "schema_version": metadata["schema_version"],
        "model_id": metadata["model_id"],
        "model_revision": metadata["model_revision"],
        "runtime_lock_sha256": metadata["runtime_lock_sha256"],
        "prompt_sha256": metadata["prompt_sha256"][route],
        "json_schema_sha256": metadata["json_schema_sha256"][route],
        "extraction_contract_sha256": metadata["extraction_contract_sha256"],
        "generation_config_sha256": metadata["generation_config_sha256"],
        "alignment_id": metadata["alignment_id"],
        "alignment_spec_sha256": metadata["alignment_spec_sha256"],
        "semantic_validation_spec_sha256": metadata[
            "semantic_validation_spec_sha256"
        ],
        "endorsement_context_provenance_spec_sha256": metadata[
            "endorsement_context_provenance_spec_sha256"
        ],
        "user_message_template_id": metadata["user_message_template_id"],
        "user_message_template_sha256": metadata[
            "user_message_template_sha256"
        ],
    }
    if route == "endorsements" and metadata.get(
        "endorsement_route_contract_sha256"
    ):
        result.update({
            "prompt_sha256": None,
            "json_schema_sha256": None,
            "extraction_contract_sha256": metadata[
                "endorsement_route_contract_sha256"
            ],
            "generation_config_sha256": None,
            "alignment_id": None,
            "alignment_spec_sha256": None,
            "user_message_template_id": None,
            "user_message_template_sha256": None,
        })
    return result


def _public_segment_output_provenance(
    route: str,
    *,
    line_ledger_enabled: bool,
    output_tokens: int,
    max_output_tokens: int,
    canonicalization_report: Mapping[str, Any],
) -> dict[str, Any]:
    """Project only representable per-segment generation provenance."""
    if route == "endorsements" and line_ledger_enabled:
        return {
            "output_tokens": None,
            "max_output_tokens": None,
            "json_control_canonicalization_applied": None,
            "json_control_canonicalization_count": None,
        }
    return {
        "output_tokens": output_tokens,
        "max_output_tokens": max_output_tokens,
        "json_control_canonicalization_applied": bool(
            canonicalization_report[
                "json_control_canonicalization_applied"
            ]
        ),
        "json_control_canonicalization_count": int(
            canonicalization_report[
                "json_control_canonicalization_count"
            ]
        ),
    }


_SEGMENT_CONTEXT_PROVENANCE_MAP = {
    "endorsement_context_provenance_version": "version",
    "endorsement_context_id": "context_id",
    "segment_starts_mid_line": "segment_starts_mid_line",
    "segment_has_complete_line": "segment_has_complete_line",
    "segment_first_complete_line_start_char": (
        "segment_first_complete_line_start_char"
    ),
    "segment_first_complete_line_parent_start_char": (
        "segment_first_complete_line_parent_start_char"
    ),
    "segment_first_complete_line_inference_display_start_char": (
        "segment_first_complete_line_inference_display_start_char"
    ),
    "segment_ends_mid_line": "segment_ends_mid_line",
    "segment_last_complete_line_end_char": (
        "segment_last_complete_line_end_char"
    ),
    "segment_last_complete_line_parent_end_char": (
        "segment_last_complete_line_parent_end_char"
    ),
    "segment_last_complete_line_inference_display_end_char": (
        "segment_last_complete_line_inference_display_end_char"
    ),
    "endorsement_context_source_scope": "source_scope",
    "endorsement_context_derivation_method": "context_derivation_method",
    "endorsement_context_parent_start_char": "context_parent_start_char",
    "endorsement_context_parent_end_char": "context_parent_end_char",
    "endorsement_context_parent_exact_match_count": (
        "context_parent_exact_match_count"
    ),
    "endorsement_context_parent_occurrence_ordinal": (
        "context_parent_occurrence_ordinal"
    ),
    "endorsement_context_line_index_zero_based": (
        "context_line_index_zero_based"
    ),
    "endorsement_context_scope_parent_start_char": (
        "context_scope_parent_start_char"
    ),
    "endorsement_context_scope_parent_end_char": (
        "context_scope_parent_end_char"
    ),
    "endorsement_context_source_segment_id": "context_source_segment_id",
    "endorsement_context_local_start_char": "context_local_start_char",
    "endorsement_context_local_end_char": "context_local_end_char",
    "endorsement_context_record_to_context_distance_rule": (
        "record_to_context_distance_rule"
    ),
    "endorsement_context_qualifying_wayback_url": "qualifying_wayback_url",
    "endorsement_context_qualifying_original_url": "qualifying_original_url",
    "endorsement_context_qualifying_original_path": "qualifying_original_path",
    "endorsement_context_source_urls_sha256": "source_urls_sha256",
    "endorsement_context_url_classifier_id": "url_classifier_id",
    "endorsement_context_url_classifier_spec_sha256": (
        "url_classifier_spec_sha256"
    ),
    "endorsement_context_structural_heading_spec_sha256": (
        "structural_heading_spec_sha256"
    ),
    "endorsement_context_no_literal_anchor_reason": "no_literal_anchor_reason",
}


def _public_segment_context_provenance(
    route: str, provenance: Any
) -> dict[str, Any]:
    if route != "endorsements":
        _require(
            provenance is None,
            "non-endorsement segment has endorsement-context provenance",
        )
        return {field: None for field in _SEGMENT_CONTEXT_PROVENANCE_MAP}
    _require(
        isinstance(provenance, dict),
        "endorsement segment lacks context provenance",
    )
    return {
        public_field: provenance.get(private_field)
        for public_field, private_field in _SEGMENT_CONTEXT_PROVENANCE_MAP.items()
    }


def _validate_row_metadata(
    row: dict[str, Any],
    metadata: dict[str, Any],
    segment_id: str,
    *,
    repair_generation_config_sha256: str | None = None,
) -> None:
    """Require every row to carry the bundle's declared identity.

    A window a seed-vary repair recovered carries its attempt's repair identity,
    which differs from the primary one only in the seed. The caller passes that
    identity for exactly those windows, taken from the bundle's repair
    attestation rather than from the row, so a row cannot claim a repair the
    bundle does not record.
    """
    route = str(row.get("route") or "")
    _require(route in ROUTES, f"{segment_id}: route is invalid")
    for field, expected in metadata.items():
        if field == "route_array_item_caps":
            continue
        if field in {"prompt_sha256", "json_schema_sha256"}:
            expected = expected[route]
        if (
            field == "generation_config_sha256"
            and repair_generation_config_sha256 is not None
        ):
            expected = repair_generation_config_sha256
        _require(
            _typed_equal(row.get(field), expected),
            f"{segment_id}: {field} differs from bundle manifest",
        )
    _require(
        row.get("route_array_item_cap")
        == metadata["route_array_item_caps"][route],
        f"{segment_id}: route array-item cap differs from bundle manifest",
    )


def _validate_exact_span(
    item: dict[str, Any], text_field: str, start_field: str, end_field: str,
    source_text: str, label: str, *, nullable: bool = False,
) -> tuple[str | None, int | None, int | None]:
    value = item.get(text_field)
    start = item.get(start_field)
    end = item.get(end_field)
    if value is None:
        _require(nullable, f"{label}.{text_field} is missing")
        _require(start is None and end is None, f"{label} has offsets without text")
        return None, None, None
    text = _required_text(value, f"{label}.{text_field}")
    start_int = _integer(start, f"{label}.{start_field}")
    end_int = _integer(end, f"{label}.{end_field}")
    _require(0 <= start_int < end_int <= len(source_text), f"{label} span is outside its segment")
    _require(source_text[start_int:end_int] == text, f"{label} span is not source-exact")
    return text, start_int, end_int


# The aligner emits this label when a two-stage ledger supplies the record's
# span: the ledger's record block pins which occurrence on the page belongs to
# the record, so the text need not be unique in the segment.
PARENT_PROJECTION_ALIGNMENT_METHOD = "provided_authenticated_parent_projection_v1"


def _two_stage_item_checks(
    item: dict[str, Any],
    source_text: str,
    label: str,
    *,
    endorsement_context_provenance: dict[str, Any] | None,
    segment_start_char: int,
    parent_source_text: str | None,
) -> None:
    """Run the parts of the frozen v2.1 validator that hold for any architecture.

    The frozen validator was written for one-stage records. Its combined verdict
    assumes one context per segment, carried on the segment's provenance, and it
    requires record and context text to be unique in the segment. A two-stage
    ledger gives each record its own context and pins which occurrence on the
    page is meant, so that combined verdict tests the wrong thing for it. Hanno
    chose on 2026-09-21 to validate these records by their own architecture's
    rules and to leave the frozen validator byte-identical.

    This keeps what does not depend on that model: the aligned field set, the
    integrity of the segment's context provenance against the page, the model
    schema, and a well-formed source identity. The record evidence must carry the
    parent-projection label with one counted match. Each record's evidence span
    and context span are then checked by `_validate_exact_span` and the parent
    span check, which require the stated span to reproduce its text exactly and
    ask nothing about uniqueness. The record-to-context distance is round four's
    own measurement, carried as a number.
    """
    from src.candidate_extraction import (
        V2_1_ALIGNED_ENDORSEMENT_CONTEXT_FIELDS,
        V2_1_ANCHORS,
        V2_1_MODEL_ITEM_FIELDS,
        _v2_1_model_item_schema_valid,
        endorsement_context_provenance_errors,
    )

    expected = (
        set(V2_1_MODEL_ITEM_FIELDS["endorsements"])
        | {"source_model_item_id"}
        | set(V2_1_ALIGNED_ENDORSEMENT_CONTEXT_FIELDS)
    )
    for text_field, start_field, end_field, _nullable in V2_1_ANCHORS["endorsements"]:
        expected.update({
            start_field, end_field,
            f"{text_field}_exact_match_count", f"{text_field}_alignment_method",
        })
    _require(set(item) == expected, f"{label} two-stage fields differ from v2.1")
    _require(
        isinstance(endorsement_context_provenance, dict),
        f"{label} lacks segment context provenance",
    )
    source_segment_id = endorsement_context_provenance.get("context_source_segment_id")
    provenance_errors = endorsement_context_provenance_errors(
        endorsement_context_provenance,
        parent_source_text=parent_source_text,
        segment_source_text=source_text,
        segment_id=str(source_segment_id) if source_segment_id is not None else None,
        segment_start_char=segment_start_char,
    )
    _require(
        not provenance_errors,
        f"{label} segment context provenance is not page-exact: {provenance_errors[:3]}",
    )
    base = {field: item.get(field) for field in V2_1_MODEL_ITEM_FIELDS["endorsements"]}
    _require(
        _v2_1_model_item_schema_valid("endorsements", base),
        f"{label} fails the v2.1 model schema",
    )
    _require_hex64(item.get("source_model_item_id"), f"{label}.source_model_item_id")
    _require(
        item.get("record_evidence_alignment_method") == PARENT_PROJECTION_ALIGNMENT_METHOD
        and item.get("record_evidence_exact_match_count") == 1,
        f"{label} two-stage record evidence alignment differs",
    )


def _validate_item(
    route: str,
    item: Any,
    source_text: str,
    label: str,
    *,
    endorsement_context_provenance: dict[str, Any] | None = None,
    segment_start_char: int = 0,
    parent_source_text: str | None = None,
    line_ledger_item: bool = False,
    round4_item: bool = False,
) -> dict[str, Any]:
    _require(isinstance(item, dict), f"{label} is not an object")
    expected_fields = {
        "issues": ISSUE_ITEM_FIELDS,
        "biography": BIOGRAPHY_ITEM_FIELDS,
        "endorsements": (
            ENDORSEMENT_LINE_LEDGER_ITEM_FIELDS
            if line_ledger_item else ENDORSEMENT_ITEM_FIELDS
        ),
    }[route]
    _require(set(item) == expected_fields, f"{label} fields differ: {sorted(set(item) ^ expected_fields)}")
    ledger_provenance: dict[str, Any] = {}
    two_stage_item = False
    if route == "endorsements" and line_ledger_item:
        ledger_provenance = {
            field: item[field]
            for field in ENDORSEMENT_LINE_LEDGER_PROVENANCE_FIELDS
        }
        item = {field: item[field] for field in ENDORSEMENT_ITEM_FIELDS}
        two_stage_item = (
            item.get("record_evidence_alignment_method")
            == PARENT_PROJECTION_ALIGNMENT_METHOD
        )
    if two_stage_item:
        _two_stage_item_checks(
            item,
            source_text,
            label,
            endorsement_context_provenance=endorsement_context_provenance,
            segment_start_char=segment_start_char,
            parent_source_text=parent_source_text,
        )
    else:
        item_validation = validate_extraction(
            route,
            {LIST_KEYS[route]: [item]},
            source_text,
            SCHEMA_VERSION,
            endorsement_context_provenance=endorsement_context_provenance,
            segment_start_char=segment_start_char,
            parent_source_text=parent_source_text,
        )
        _require(
            item_validation.get("schema_valid") is True
            and item_validation.get("n_supported") == 1,
            f"{label} fails v2.1 exact-alignment or semantic validation",
        )
    source_model_item_id = _require_hex64(
        item.get("source_model_item_id"), f"{label}.source_model_item_id"
    )
    if route == "issues":
        heading, heading_start, heading_end = _validate_exact_span(
            item, "source_heading", "source_heading_start_char",
            "source_heading_end_char", source_text, label, nullable=True,
        )
        claim = _required_text(item.get("claim"), f"{label}.claim")
        kind = str(item.get("statement_kind") or "")
        _require(kind in STATEMENT_KINDS, f"{label} has an invalid statement kind")
        evidence, evidence_start, evidence_end = _validate_exact_span(
            item, "evidence", "evidence_start_char", "evidence_end_char",
            source_text, label,
        )
        return {
            "source_model_item_id": source_model_item_id,
            "source_heading": heading,
            "source_heading_start_char": heading_start,
            "source_heading_end_char": heading_end,
            "claim": claim,
            "statement_kind": kind,
            "evidence": evidence,
            "evidence_start_char": evidence_start,
            "evidence_end_char": evidence_end,
            "source_heading_exact_match_count": _integer(
                item["source_heading_exact_match_count"],
                f"{label}.source_heading_exact_match_count",
            ),
            "source_heading_alignment_method": str(
                item["source_heading_alignment_method"]
            ),
            "evidence_exact_match_count": _integer(
                item["evidence_exact_match_count"],
                f"{label}.evidence_exact_match_count",
            ),
            "evidence_alignment_method": str(item["evidence_alignment_method"]),
        }
    if route == "biography":
        field = str(item.get("field") or "")
        _require(field in BIOGRAPHY_FIELDS, f"{label} has an excluded or invalid biography field")
        value = _required_text(item.get("value"), f"{label}.value")
        timing = item.get("timing")
        _require(timing is None or timing in TIMING_VALUES, f"{label} has invalid timing")
        if field in TIMING_REQUIRED_FIELDS:
            _require(timing in TIMING_VALUES, f"{label} requires timing")
        relationship = _optional_text(item.get("relationship"), f"{label}.relationship")
        if field == "local_roots":
            _require(relationship is not None, f"{label} local roots require a relationship")
        else:
            _require(relationship is None, f"{label} has relationship outside local roots")
        evidence, evidence_start, evidence_end = _validate_exact_span(
            item, "evidence", "evidence_start_char", "evidence_end_char",
            source_text, label,
        )
        return {
            "source_model_item_id": source_model_item_id,
            "field": field,
            "value": value,
            "timing": timing,
            "relationship": relationship,
            "evidence": evidence,
            "evidence_start_char": evidence_start,
            "evidence_end_char": evidence_end,
            "evidence_exact_match_count": _integer(
                item["evidence_exact_match_count"],
                f"{label}.evidence_exact_match_count",
            ),
            "evidence_alignment_method": str(item["evidence_alignment_method"]),
        }
    endorser_name = _required_text(item.get("endorser_name"), f"{label}.endorser_name")
    entity_type = str(item.get("entity_type") or "")
    category = str(item.get("endorser_category") or "")
    support_mode = str(item.get("support_mode") or "")
    _require(entity_type in ENTITY_TYPES, f"{label} has invalid entity_type")
    _require(category in ENDORSER_CATEGORIES, f"{label} has invalid endorser_category")
    _require(support_mode in SUPPORT_MODES, f"{label} has invalid support_mode")
    expected_entity = {
        "private_individual": "individual",
        "government_official": "individual",
        "party_official": "individual",
        "other_individual": "individual",
        "labor_union": "organization",
        "media_outlet": "organization",
        "other_organization": "organization",
    }.get(category)
    if expected_entity is not None:
        _require(entity_type == expected_entity, f"{label} category and entity type disagree")
    record_evidence, record_start, record_end = _validate_exact_span(
        item, "record_evidence", "record_evidence_start_char",
        "record_evidence_end_char", source_text, label,
    )
    validated_ledger_provenance: dict[str, Any] = {}
    if line_ledger_item:
        for field in (
            "line_inventory_id", "source_line_id", "stage_a_batch_id",
            "stage_b_batch_id", "carrier_alignment_source_model_item_id",
            "inventory_owner_segment_id",
            "record_evidence_carrier_segment_id",
        ):
            validated_ledger_provenance[field] = _require_hex64(
                ledger_provenance.get(field), f"{label}.{field}"
            )
        stage_a_slot = _required_text(
            ledger_provenance.get("stage_a_slot"), f"{label}.stage_a_slot"
        )
        stage_b_slot = _required_text(
            ledger_provenance.get("stage_b_slot"), f"{label}.stage_b_slot"
        )
        _require(
            bool(re.fullmatch(r"slot_(?:0[0-9]|1[0-5])", stage_a_slot)),
            f"{label}.stage_a_slot is invalid",
        )
        _require(
            bool(re.fullmatch(r"slot_0[0-7]", stage_b_slot)),
            f"{label}.stage_b_slot is invalid",
        )
        target_method = _required_text(
            ledger_provenance.get("target_block_alignment_method"),
            f"{label}.target_block_alignment_method",
        )
        # Each architecture positions its record block by its own method, so a
        # record may only claim the method of the architecture that produced it.
        allowed_target_methods = (
            {ROUND4_TARGET_BLOCK_ALIGNMENT_METHOD}
            if round4_item
            else {
                "unique_exact_target_block_substring_v1",
                "unique_exact_target_block_inference_display_substring_v1",
            }
        )
        _require(
            target_method in allowed_target_methods,
            f"{label} target-block alignment method is invalid",
        )
        target_match_count = _integer(
            ledger_provenance.get("target_block_exact_match_count"),
            f"{label}.target_block_exact_match_count",
        )
        _require(
            target_match_count == 1,
            f"{label} target-block exact-match count differs",
        )
        parent_record_start = _integer(
            ledger_provenance.get("record_evidence_parent_start_char"),
            f"{label}.record_evidence_parent_start_char",
        )
        parent_record_end = _integer(
            ledger_provenance.get("record_evidence_parent_end_char"),
            f"{label}.record_evidence_parent_end_char",
        )
        _require(
            parent_record_start == segment_start_char + int(record_start)
            and parent_record_end == segment_start_char + int(record_end),
            f"{label} ledger record-evidence parent offsets differ",
        )
        validated_ledger_provenance.update({
            "stage_a_slot": stage_a_slot,
            "stage_b_slot": stage_b_slot,
            "model_record_index": _integer(
                ledger_provenance.get("model_record_index"),
                f"{label}.model_record_index",
            ),
            "target_block_alignment_method": target_method,
            "target_block_exact_match_count": target_match_count,
            "record_evidence_parent_start_char": parent_record_start,
            "record_evidence_parent_end_char": parent_record_end,
        })
        _require(
            validated_ledger_provenance["model_record_index"] >= 0,
            f"{label}.model_record_index is negative",
        )
    context_scope = str(
        item.get("endorsement_context_evidence_source_scope") or ""
    )
    _require(
        context_scope
        in {"segment", "parent_document", "page_or_section_route_context"},
        f"{label} has invalid endorsement context scope",
    )
    context_parent_start = item.get(
        "endorsement_context_evidence_parent_start_char"
    )
    context_parent_end = item.get(
        "endorsement_context_evidence_parent_end_char"
    )
    context_start = item.get("endorsement_context_evidence_start_char")
    context_end = item.get("endorsement_context_evidence_end_char")
    context_evidence = item.get("endorsement_context_evidence")
    if context_scope == "segment":
        context_evidence, context_start, context_end = _validate_exact_span(
            item, "endorsement_context_evidence",
            "endorsement_context_evidence_start_char",
            "endorsement_context_evidence_end_char", source_text, label,
        )
        _require(
            context_parent_start == segment_start_char + int(context_start)
            and context_parent_end == segment_start_char + int(context_end),
            f"{label} segment context parent offsets differ",
        )
    elif context_scope == "parent_document":
        _require(
            parent_source_text is not None,
            f"{label} parent context lacks reconstructed parent text",
        )
        context_evidence = _required_text(
            context_evidence, f"{label}.endorsement_context_evidence"
        )
        _require(
            context_start is None and context_end is None,
            f"{label} parent context has segment-local offsets",
        )
        context_parent_start = _integer(
            context_parent_start,
            f"{label}.endorsement_context_evidence_parent_start_char",
        )
        context_parent_end = _integer(
            context_parent_end,
            f"{label}.endorsement_context_evidence_parent_end_char",
        )
        _require(
            0 <= context_parent_start < context_parent_end
            <= len(parent_source_text)
            and parent_source_text[context_parent_start:context_parent_end]
            == context_evidence,
            f"{label} parent context span is not source-exact",
        )
    else:
        _require(
            support_mode == "page_or_section_context"
            and context_evidence is None
            and context_start is None
            and context_end is None
            and context_parent_start is None
            and context_parent_end is None,
            f"{label} URL-route context has textual evidence or offsets",
        )
    return {
        "source_model_item_id": source_model_item_id,
        "endorser_name": endorser_name,
        "entity_type": entity_type,
        # endorser_category stays out of the public payload; see ENDORSEMENT_COLUMNS.
        "title": _optional_text(item.get("title"), f"{label}.title"),
        "affiliation": _optional_text(item.get("affiliation"), f"{label}.affiliation"),
        "jurisdiction": _optional_text(item.get("jurisdiction"), f"{label}.jurisdiction"),
        "record_evidence": record_evidence,
        "record_evidence_start_char": record_start,
        "record_evidence_end_char": record_end,
        "endorsement_context_evidence": context_evidence,
        "endorsement_context_evidence_start_char": context_start,
        "endorsement_context_evidence_end_char": context_end,
        "endorsement_context_evidence_parent_start_char": (
            context_parent_start
        ),
        "endorsement_context_evidence_parent_end_char": context_parent_end,
        "support_mode": support_mode,
        "endorsement_context_id": _require_hex64(
            item.get("endorsement_context_id"),
            f"{label}.endorsement_context_id",
        ),
        "endorsement_context_evidence_source_scope": context_scope,
        "endorsement_context_record_distance_chars": (
            None
            if item.get("endorsement_context_record_distance_chars") is None
            else _integer(
                item["endorsement_context_record_distance_chars"],
                f"{label}.endorsement_context_record_distance_chars",
            )
        ),
        "endorsement_context_qualifying_wayback_url": _optional_text(
            item.get("endorsement_context_qualifying_wayback_url"),
            f"{label}.endorsement_context_qualifying_wayback_url",
        ),
        "endorsement_context_qualifying_original_url": _optional_text(
            item.get("endorsement_context_qualifying_original_url"),
            f"{label}.endorsement_context_qualifying_original_url",
        ),
        "endorsement_context_qualifying_original_path": _optional_text(
            item.get("endorsement_context_qualifying_original_path"),
            f"{label}.endorsement_context_qualifying_original_path",
        ),
        "record_evidence_exact_match_count": _integer(
            item["record_evidence_exact_match_count"],
            f"{label}.record_evidence_exact_match_count",
        ),
        "record_evidence_alignment_method": str(
            item["record_evidence_alignment_method"]
        ),
        "endorsement_context_evidence_exact_match_count": _integer(
            item["endorsement_context_evidence_exact_match_count"],
            f"{label}.endorsement_context_evidence_exact_match_count",
        ),
        "endorsement_context_evidence_alignment_method": str(
            item["endorsement_context_evidence_alignment_method"]
        ),
        **validated_ledger_provenance,
    }


def _parent_payload(route: str, item: dict[str, Any], parent_start: int) -> dict[str, Any]:
    payload = dict(item)
    pairs = {
        "issues": (("source_heading_start_char", "source_heading_end_char"),
                   ("evidence_start_char", "evidence_end_char")),
        "biography": (("evidence_start_char", "evidence_end_char"),),
        "endorsements": (
            ("record_evidence_start_char", "record_evidence_end_char"),
            ("endorsement_context_evidence_start_char", "endorsement_context_evidence_end_char"),
        ),
    }[route]
    for start_field, end_field in pairs:
        parent_start_field = (
            f"{start_field.removesuffix('_start_char')}_parent_start_char"
        )
        parent_end_field = f"{end_field.removesuffix('_end_char')}_parent_end_char"
        if route == "endorsements" and start_field == (
            "endorsement_context_evidence_start_char"
        ):
            # The deterministic alignment already binds this anchor either to
            # this segment, an explicit parent span, or URL-route provenance.
            continue
        if payload[start_field] is None:
            payload[parent_start_field] = None
            payload[parent_end_field] = None
        else:
            payload[parent_start_field] = parent_start + int(payload[start_field])
            payload[parent_end_field] = parent_start + int(payload[end_field])
    return payload


def _arrow_schema(columns: tuple[str, ...]) -> pa.Schema:
    list_columns = {"wayback_urls", "original_urls", "source_segment_ids"}
    integer_columns = {
        "year", "n_raw_occurrences", "segment_count", "processed_segment_count",
        "record_count", "extraction_source_n_chars", "segment_index",
        "segment_start_char", "segment_end_char",
        "extracted_record_count", "variant_count", "source_heading_start_char",
        "source_heading_end_char", "source_heading_parent_start_char",
        "source_heading_parent_end_char", "evidence_start_char", "evidence_end_char",
        "evidence_parent_start_char", "evidence_parent_end_char",
        "record_evidence_start_char", "record_evidence_end_char",
        "record_evidence_parent_start_char", "record_evidence_parent_end_char",
        "endorsement_context_evidence_start_char",
        "endorsement_context_evidence_end_char",
        "endorsement_context_evidence_parent_start_char",
        "endorsement_context_evidence_parent_end_char",
        "output_tokens", "max_output_tokens",
        "model_item_count", "aligned_item_count", "withheld_item_count",
        "withheld_overlap_conflict_group_count",
        "n_withheld_overlap_conflict_groups",
        "endorsement_context_parent_start_char",
        "endorsement_context_parent_end_char",
        "endorsement_context_parent_exact_match_count",
        "endorsement_context_parent_occurrence_ordinal",
        "endorsement_context_line_index_zero_based",
        "segment_first_complete_line_start_char",
        "segment_first_complete_line_parent_start_char",
        "segment_first_complete_line_inference_display_start_char",
        "segment_last_complete_line_end_char",
        "segment_last_complete_line_parent_end_char",
        "segment_last_complete_line_inference_display_end_char",
        "endorsement_context_scope_parent_start_char",
        "endorsement_context_scope_parent_end_char",
        "endorsement_context_local_start_char",
        "endorsement_context_local_end_char",
        "endorsement_context_record_distance_chars",
        "source_heading_exact_match_count", "evidence_exact_match_count",
        "record_evidence_exact_match_count",
        "endorsement_context_evidence_exact_match_count",
        "model_record_index", "target_block_exact_match_count",
        "json_control_canonicalization_count",
    }
    boolean_columns = {
        "schema_valid", "input_truncated", "source_text_transform_changed",
        "json_control_canonicalization_applied",
        "segment_starts_mid_line", "segment_has_complete_line",
        "segment_ends_mid_line",
    }
    fields = []
    for column in columns:
        if column in list_columns:
            dtype = pa.list_(pa.string())
        elif column in integer_columns:
            dtype = pa.int64()
        elif column in boolean_columns:
            dtype = pa.bool_()
        else:
            dtype = pa.string()
        fields.append(pa.field(column, dtype, nullable=True))
    return pa.schema(fields)


TABLE_SCHEMAS = {
    "candidate_extraction_documents.parquet": _arrow_schema(DOCUMENT_COLUMNS),
    "candidate_extraction_segments.parquet": _arrow_schema(SEGMENT_COLUMNS),
    "candidate_issue_statements.parquet": _arrow_schema(ISSUE_COLUMNS),
    "candidate_biography_facts.parquet": _arrow_schema(BIOGRAPHY_COLUMNS),
    "candidate_endorsements.parquet": _arrow_schema(ENDORSEMENT_COLUMNS),
}


def _tables_from_inputs(
    universe: pd.DataFrame,
    raw: pd.DataFrame,
    raw_sha256: str,
    manifest: dict[str, Any],
    segment_rows: list[dict[str, Any]],
    route_passed: dict[str, bool],
) -> dict[str, Any]:
    manifest_raw_hash = _require_hex64(
        manifest.get("source_corpus_sha256"), "bundle source_corpus_sha256"
    )
    _require(manifest_raw_hash == raw_sha256, "bundle raw-corpus hash differs from the core release")
    unrepaired_cap_windows = _unrepaired_cap_windows(manifest)
    repaired_window_identities = _repaired_window_identities(manifest)
    metadata = _manifest_metadata(manifest, segment_rows)
    validated_ledger = manifest.get("_validated_endorsement_line_ledger")
    ledger_parent_summaries = (
        validated_ledger.get("parent_summaries", {})
        if isinstance(validated_ledger, dict) else {}
    )
    ledger_enabled = isinstance(validated_ledger, dict)
    round4_enabled = isinstance(
        manifest.get("_validated_endorsement_round4_ledger"), dict
    )
    round4_parent_summaries = (
        (manifest.get("_validated_endorsement_round4_ledger") or {}).get(
            "parent_summaries"
        )
        or {}
    )
    transform = manifest.get("source_text_transform")
    _require(isinstance(transform, dict), "bundle lacks source-transform provenance")
    _require(
        transform.get("version") == SOURCE_TEXT_TRANSFORM_VERSION
        and transform.get("rule") == SOURCE_TEXT_TRANSFORM_RULE
        and transform.get("offset_coordinate_system") == OFFSET_COORDINATE_SYSTEM,
        "bundle source-transform provenance differs",
    )
    expected_documents = _expected_core_documents(raw)
    transformed_documents = sum(
        bool(document["source_text_transform_changed"])
        for document in expected_documents.values()
    )
    _require(
        int(transform.get("transformed_exported_parent_documents", -1))
        == transformed_documents,
        "bundle transformed-parent count differs from the core corpus",
    )
    source_state = _core_source_state(raw)
    universe_ids = set(universe["candidate_cycle_id"].astype(str))
    expected_candidate_ids = {
        document["candidate_cycle_id"] for document in expected_documents.values()
    }
    _require(
        expected_candidate_ids <= universe_ids,
        "core extraction documents include candidates outside the universe",
    )

    segment_ids: set[str] = set()
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in segment_rows:
        segment_id = _row_segment_id(row)
        _require(segment_id not in segment_ids, f"duplicate segment ID: {segment_id}")
        segment_ids.add(segment_id)
        _validate_row_metadata(
            row,
            metadata,
            segment_id,
            repair_generation_config_sha256=repaired_window_identities.get(
                str(row.get("document_id"))
            ),
        )
        parent_id = _require_hex64(row.get("parent_document_id"), f"{segment_id}.parent_document_id")
        _require(parent_id in expected_documents, f"{segment_id} links to an unknown core document")
        grouped[parent_id].append(row)
    _require(set(grouped) == set(expected_documents), "bundle parent coverage differs from the corrected raw corpus")

    manifest_parent_count = int(manifest.get("parent_documents", -1))
    manifest_segment_count = int(manifest.get("segments", -1))
    _require(manifest_parent_count == len(grouped), "bundle parent count differs")
    _require(manifest_segment_count == len(segment_rows), "bundle segment count differs")
    for manifest_field, alignment_field in (
        ("model_items", "model_item_count"),
        ("aligned_items", "aligned_item_count"),
        ("withheld_model_items", "withheld_item_count"),
    ):
        observed = sum(
            int((row.get("exact_alignment") or {}).get(alignment_field, 0))
            for row in segment_rows
        )
        _require(
            int(manifest.get(manifest_field, -1)) == observed,
            f"bundle {manifest_field} count differs",
        )

    documents: list[dict[str, Any]] = []
    segments_public: list[dict[str, Any]] = []
    resolved_by_parent: dict[str, list[dict[str, Any]]] = {}
    all_variant_ids: set[str] = set()
    all_record_ids: set[str] = set()
    conflict_group_ids: set[str] = set()
    segment_lookup: dict[str, dict[str, Any]] = {}
    public_segment_lookup: dict[str, dict[str, Any]] = {}

    for parent_id, rows in sorted(grouped.items()):
        rows.sort(key=lambda row: (_integer(row.get("segment_index"), f"{parent_id}.segment_index"), _row_segment_id(row)))
        expected = expected_documents[parent_id]
        route = expected["route"]
        # Both endorsement architectures read the page through a two-stage
        # parent ledger instead of one-stage variant resolution, so the
        # branches below apply to either, with the parent summary taken from
        # whichever ledger the bundle declares.
        two_stage = route == "endorsements" and (ledger_enabled or round4_enabled)
        if route == "endorsements" and ledger_enabled:
            ledger_parent = ledger_parent_summaries.get(parent_id)
        elif route == "endorsements" and round4_enabled:
            ledger_parent = round4_parent_summaries.get(parent_id)
        else:
            ledger_parent = None
        if two_stage:
            _require(
                isinstance(ledger_parent, dict),
                f"{parent_id} lacks validated endorsement ledger coverage",
            )
        expected_count = len(rows)
        _require([int(row["segment_index"]) for row in rows] == list(range(expected_count)), f"{parent_id} segment indexes are not complete")
        _require(all(int(row.get("segment_count", -1)) == expected_count for row in rows), f"{parent_id} segment counts disagree")

        reconstructed = ""
        previous_end = 0
        previous_text = ""
        processing_failed = False
        processing_withheld = False
        for row in rows:
            segment_id = _row_segment_id(row)
            start = _integer(row.get("segment_start_char"), f"{segment_id}.segment_start_char")
            end = _integer(row.get("segment_end_char"), f"{segment_id}.segment_end_char")
            source_text = _required_text(row.get("source_text"), f"{segment_id}.source_text")
            _require(0 <= start < end and len(source_text) == end - start, f"{segment_id} has invalid segment bounds")
            segment_hash = _require_hex64(row.get("segment_text_sha256"), f"{segment_id}.segment_text_sha256")
            _require(sha256_text(source_text) == segment_hash, f"{segment_id} segment hash differs")
            expected_segment_id = sha256_text(f"{parent_id}\n{row['segment_index']}\n{start}\n{end}\n{segment_hash}")
            _require(segment_id == expected_segment_id, f"{segment_id} is not the deterministic segment ID")
            if int(row["segment_index"]) == 0:
                _require(start == 0, f"{parent_id} first segment does not start at zero")
                reconstructed = source_text
            else:
                _require(start <= previous_end, f"{parent_id} has a gap between segments")
                overlap = previous_end - start
                if overlap:
                    _require(previous_text[-overlap:] == source_text[:overlap], f"{parent_id} segment overlap differs")
                reconstructed += source_text[overlap:]
            previous_end, previous_text = end, source_text

            _require(str(row.get("candidate_cycle_id")) == expected["candidate_cycle_id"], f"{segment_id} candidate differs")
            _require(str(row.get("route")) == route, f"{segment_id} route differs")
            _require(str(row.get("raw_text_sha256")) == expected["raw_text_sha256"], f"{segment_id} parent text hash differs")
            _require(
                row.get("extraction_source_text_sha256")
                == sha256_text(expected["source_text"]),
                f"{segment_id} extraction-source hash differs",
            )
            _require(
                int(row.get("extraction_source_n_chars", -1))
                == len(expected["source_text"]),
                f"{segment_id} extraction-source length differs",
            )
            _require(
                row.get("source_text_transform_version")
                == SOURCE_TEXT_TRANSFORM_VERSION
                and row.get("source_text_transform_changed")
                is expected["source_text_transform_changed"],
                f"{segment_id} source-transform provenance differs",
            )
            for field in ("candidate_icpsr", "state", "office", "party"):
                _require(str(row.get(field)) == str(expected[field]), f"{segment_id} {field} differs")
            _require(int(row.get("year", -1)) == expected["year"], f"{segment_id} year differs")
            source_urls = sorted(set(str(value) for value in (row.get("source_urls") or [])))
            _require(source_urls == expected["source_urls"], f"{segment_id} source URLs differ")
            context_provenance = row.get("endorsement_context_provenance")
            if route == "endorsements":
                context_errors = endorsement_context_provenance_errors(
                    context_provenance,
                    parent_source_text=expected["source_text"],
                    segment_source_text=source_text,
                    segment_id=segment_id,
                    segment_start_char=start,
                    parent_document_id=parent_id,
                    source_urls=source_urls,
                )
                _require(
                    not context_errors,
                    f"{segment_id} endorsement context differs: "
                    + ", ".join(context_errors),
                )
            else:
                _require(
                    context_provenance is None,
                    f"{segment_id} non-endorsement context is present",
                )
            _require(int(row.get("n_raw_occurrences", -1)) == expected["n_raw_occurrences"], f"{segment_id} occurrence count differs")
            _require(_timestamp(row.get("first_capture_timestamp"), f"{segment_id}.first_capture") == expected["first_capture_timestamp"], f"{segment_id} first capture differs")
            _require(_timestamp(row.get("last_capture_timestamp"), f"{segment_id}.last_capture") == expected["last_capture_timestamp"], f"{segment_id} last capture differs")

            _require(row.get("temperature") == 0.0, f"{segment_id} temperature is not zero")
            _require(row.get("thinking_enabled") is False, f"{segment_id} used model thinking")
            _require(row.get("compact_repair") is False, f"{segment_id} used a legacy repair")
            withheld_for_output_cap = (
                row.get("parse_status") == TRUNCATED_PARSE_STATUS
                and row.get("finish_reason") == "length"
            )
            if withheld_for_output_cap:
                # The one permitted exception to the stop rule. The row must be a
                # cap-limited window that every approved repair attempt left at
                # the cap, and the bundle's repair attestation must say so.
                _require(
                    row.get("finish_reason") == "length",
                    f"{segment_id} is withheld for the output cap but did not"
                    " finish with length",
                )
                _require(
                    str(row.get("document_id")) in unrepaired_cap_windows,
                    f"{segment_id} is withheld for the output cap but no repair"
                    " attestation reports that window as unrepaired",
                )
                _require(
                    row.get("extraction") is None,
                    f"{segment_id} is withheld for the output cap but carries an"
                    " extraction",
                )
            else:
                _require(row.get("finish_reason") == "stop", f"{segment_id} did not finish with stop")
            output_tokens = _integer(row.get("output_tokens"), f"{segment_id}.output_tokens")
            max_output_tokens = _integer(
                row.get("max_output_tokens"), f"{segment_id}.max_output_tokens"
            )
            _require(
                0 < output_tokens <= max_output_tokens
                and max_output_tokens == EXPECTED_MAX_OUTPUT_TOKENS,
                f"{segment_id} output token accounting differs",
            )

            parsed_model_extraction, canonicalization_report = (
                validate_private_output_provenance(
                    row, source_text, segment_id
                )
            )

            parse_ok = row.get("parse_status") == "parsed"
            truncated = row.get("input_truncated") is True
            validation = row.get("validation")
            _require(isinstance(validation, dict), f"{segment_id} lacks validation results")
            alignment_status = "not_available_parse_failure"
            model_item_count: int | None = None
            aligned_item_count: int | None = None
            withheld_item_count: int | None = None
            if parse_ok:
                recomputed_extraction, recomputed_withheld, recomputed_alignment = (
                    align_extraction_exact_spans(
                        route,
                        parsed_model_extraction,
                        source_text,
                        endorsement_context_provenance=context_provenance,
                        segment_start_char=start,
                        schema_version=SCHEMA_VERSION,
                    )
                )
                expected_alignment_values = {
                    "extraction": recomputed_extraction,
                    "withheld_model_items": recomputed_withheld,
                    "exact_alignment": recomputed_alignment,
                    "model_extraction_sha256": recomputed_alignment[
                        "original_model_extraction_sha256"
                    ],
                    "aligned_extraction_sha256": recomputed_alignment[
                        "aligned_extraction_sha256"
                    ],
                }
                for field, value in expected_alignment_values.items():
                    _require(
                        _typed_equal(row.get(field), value),
                        f"{segment_id} {field} differs from deterministic alignment",
                    )
                recomputed_validation = validate_extraction(
                    route,
                    recomputed_extraction,
                    source_text,
                    SCHEMA_VERSION,
                    endorsement_context_provenance=context_provenance,
                    segment_start_char=start,
                    parent_source_text=expected["source_text"],
                )
                _require(
                    _typed_equal(validation, recomputed_validation),
                    f"{segment_id} validation differs from recomputation",
                )
                alignment_status = str(recomputed_alignment["status"])
                model_item_count = _integer(
                    recomputed_alignment["model_item_count"],
                    f"{segment_id}.model_item_count",
                )
                aligned_item_count = _integer(
                    recomputed_alignment["aligned_item_count"],
                    f"{segment_id}.aligned_item_count",
                )
                withheld_item_count = _integer(
                    recomputed_alignment["withheld_item_count"],
                    f"{segment_id}.withheld_item_count",
                )
                _require(
                    recomputed_alignment.get("all_model_items_accounted_for") is True
                    and model_item_count
                    == aligned_item_count + withheld_item_count,
                    f"{segment_id} model-item alignment counts do not reconcile",
                )
            schema_ok = validation.get("schema_valid") is True
            exact = validation.get("n_items") == validation.get("n_supported")
            full_input = (
                row.get("input_truncated") is False
                and int(row.get("input_chars_used", -1)) == len(source_text)
            )
            aligned_items_valid = parse_ok and schema_ok and exact and full_input
            if aligned_items_valid and alignment_status == "complete":
                expected_processing_status = "processed_success"
            elif truncated:
                expected_processing_status = "failed_truncated_input"
            elif row.get("parse_status") == ARRAY_ITEM_CAP_REACHED_STATUS:
                expected_processing_status = "failed_array_item_cap_reached"
            elif row.get("parse_status") == MODEL_ITEM_REPETITION_ERROR_STATUS:
                expected_processing_status = "failed_model_item_repetition_error"
            elif (
                row.get("parse_status") == TRUNCATED_PARSE_STATUS
                and row.get("finish_reason") == "length"
            ):
                expected_processing_status = "failed_output_cap_unrepaired"
            elif not parse_ok:
                expected_processing_status = "failed_parse"
            elif alignment_status == "partial_withheld":
                expected_processing_status = "processed_partial_withheld"
            elif alignment_status == "all_withheld":
                expected_processing_status = "processed_all_withheld"
            elif alignment_status == "invalid_schema":
                expected_processing_status = "failed_schema"
            elif not schema_ok:
                expected_processing_status = "failed_schema"
            else:
                expected_processing_status = "failed_exact_span"
            ledger_segment = None
            round4_segment = None
            if route == "endorsements" and round4_enabled:
                # Under round four an endorsement row's records come from the
                # parent line ledger, not from this one-stage window, so the
                # window's own outcome is superseded rather than current. That
                # is why a window which hit the output cap can still carry
                # records: the two-stage pipeline read the parent regardless.
                _require(
                    row.get("superseded_one_stage_processing_status")
                    == expected_processing_status,
                    f"{segment_id} superseded one-stage status differs",
                )
                round4_segment = row.get("endorsement_round4_record_ledger")
                _require(
                    isinstance(round4_segment, dict),
                    f"{segment_id} lacks its round-four projection",
                )
                round4_parent = round4_parent_summaries.get(
                    str(row.get("parent_document_id"))
                )
                _require(
                    isinstance(round4_parent, dict),
                    f"{segment_id} round-four parent summary is absent",
                )
                for field in (
                    "public_coverage_complete",
                    "aligned_record_count",
                    "withheld_record_count",
                    "unresolved_target_blocks",
                    "stage_a_unclear_lines",
                ):
                    _require(
                        _typed_equal(
                            round4_segment.get(field), round4_parent.get(field)
                        ),
                        f"{segment_id} round-four {field} differs from the ledger",
                    )
            if route == "endorsements" and ledger_enabled:
                _require(
                    row.get("superseded_one_stage_processing_status")
                    == expected_processing_status,
                    f"{segment_id} superseded one-stage status differs",
                )
                ledger_segment = row.get("endorsement_line_ledger")
                _require(
                    isinstance(ledger_segment, dict),
                    f"{segment_id} lacks endorsement ledger projection",
                )
                owner_counts = ledger_parent[
                    "owner_segment_model_line_counts"
                ]
                carrier_counts = ledger_parent[
                    "carrier_segment_aligned_record_counts"
                ]
                expected_ledger_segment = {
                    "line_ledger_contract_sha256": get_line_ledger_contract()[
                        "line_ledger_contract_sha256"
                    ],
                    "line_inventory_id": ledger_parent["line_inventory_id"],
                    "owned_model_decision_line_count": int(
                        owner_counts.get(segment_id, 0)
                    ),
                    "aligned_record_count": int(
                        carrier_counts.get(segment_id, 0)
                    ),
                    "parent_stage_a_batch_count": ledger_parent[
                        "stage_a_batch_count"
                    ],
                    "parent_stage_a_decision_count": ledger_parent[
                        "stage_a_decision_count"
                    ],
                    "parent_stage_a_unresolved_line_count": ledger_parent[
                        "stage_a_unresolved_line_count"
                    ],
                    "parent_stage_b_batch_count": ledger_parent[
                        "stage_b_batch_count"
                    ],
                    "parent_stage_b_anchor_count": ledger_parent[
                        "stage_b_anchor_count"
                    ],
                    "parent_aligned_record_count": ledger_parent[
                        "aligned_record_count"
                    ],
                    "parent_withheld_record_count": ledger_parent[
                        "withheld_record_count"
                    ],
                    "parent_public_coverage_complete": ledger_parent[
                        "public_coverage_complete"
                    ],
                }
                _require(
                    _typed_equal(ledger_segment, expected_ledger_segment),
                    f"{segment_id} endorsement ledger projection differs",
                )
                _require(
                    row.get("extraction_architecture")
                    == "endorsement_parent_line_ledger_v1",
                    f"{segment_id} endorsement extraction architecture differs",
                )
                if ledger_parent["public_coverage_complete"] is True:
                    current_processing_status = "processed_success"
                elif int(ledger_parent["aligned_record_count"]) > 0:
                    current_processing_status = "processed_partial_withheld"
                else:
                    current_processing_status = "processed_all_withheld"
                _require(
                    row.get("processing_status") == current_processing_status,
                    f"{segment_id} ledger processing status differs",
                )
                technical_success = current_processing_status == "processed_success"
                partial_withheld = (
                    current_processing_status == "processed_partial_withheld"
                )
                all_withheld = (
                    current_processing_status == "processed_all_withheld"
                )
            elif round4_segment is not None:
                # Round four defines a parent's status by its withheld records,
                # not by coverage, unlike the line ledger: a fully covered parent
                # with any withheld record is partial, so a withheld record is
                # never read as a measured zero. The rule is the round-four
                # module's own, recomputed from the counts rather than copied.
                current_processing_status = round4_parent_processing_status(
                    aligned_record_count=int(round4_segment["aligned_record_count"]),
                    withheld_record_count=int(round4_segment["withheld_record_count"]),
                    retry_required=False,
                )
                _require(
                    current_processing_status
                    == round4_parent_summaries[
                        str(row.get("parent_document_id"))
                    ].get("processing_status"),
                    f"{segment_id} round-four parent status differs from the ledger",
                )
                _require(
                    row.get("processing_status") == current_processing_status,
                    f"{segment_id} round-four processing status differs",
                )
                technical_success = current_processing_status == "processed_success"
                partial_withheld = (
                    current_processing_status == "processed_partial_withheld"
                )
                all_withheld = (
                    current_processing_status == "processed_all_withheld"
                )
            else:
                _require(
                    row.get("processing_status") == expected_processing_status,
                    f"{segment_id} processing status differs from its validation",
                )
                technical_success = expected_processing_status == "processed_success"
                partial_withheld = (
                    expected_processing_status == "processed_partial_withheld"
                )
                # A window every repair attempt left at the output cap releases
                # nothing, and Hanno approved on 2026-09-18 that it be withheld
                # rather than failed. It is therefore a fully withheld window: it
                # can never become "processed", and a candidate holding one can
                # never be reported as having zero records.
                all_withheld = expected_processing_status in {
                    "processed_all_withheld",
                    "failed_output_cap_unrepaired",
                }
            processing_withheld = processing_withheld or partial_withheld or all_withheld
            processing_failed = processing_failed or not (
                technical_success or partial_withheld or all_withheld
            )
            items: list[Any] = []
            if aligned_items_valid:
                extraction = row.get("extraction")
                _require(isinstance(extraction, dict), f"{segment_id} extraction is not an object")
                list_key = LIST_KEYS[route]
                _require(set(extraction) == {list_key}, f"{segment_id} extraction has unexpected keys")
                items = extraction[list_key]
                _require(isinstance(items, list), f"{segment_id} extraction list is invalid")
                for item_index, item in enumerate(items):
                    _validate_item(
                        route,
                        item,
                        source_text,
                        f"{segment_id}.item[{item_index}]",
                        endorsement_context_provenance=context_provenance,
                        segment_start_char=start,
                        parent_source_text=expected["source_text"],
                    )
            _require(isinstance(row.get("record_variants"), list), f"{segment_id} lacks record variants")
            _require(isinstance(row.get("resolved_records"), list), f"{segment_id} lacks resolved records")
            if two_stage:
                _require(
                    row["record_variants"] == [],
                    f"{segment_id} exposes superseded endorsement variants",
                )
                # The line ledger stores this segment's own record count. The
                # round-four block stores the parent's count on every segment,
                # so this segment's count is the records it carries itself.
                public_model_item_count = (
                    int(ledger_segment["aligned_record_count"])
                    if ledger_enabled
                    else len(row["resolved_records"])
                )
                public_aligned_item_count = public_model_item_count
                public_withheld_item_count = (
                    0 if ledger_parent["public_coverage_complete"] is True else None
                )
                public_alignment_status = (
                    "complete"
                    if ledger_parent["public_coverage_complete"] is True
                    else "partial_withheld"
                    if int(ledger_parent["aligned_record_count"]) > 0
                    else "all_withheld"
                )
                public_schema_ok = True
                public_truncated = False
                public_extracted_record_count = len(row["resolved_records"])
            else:
                public_model_item_count = model_item_count
                public_aligned_item_count = aligned_item_count
                public_withheld_item_count = withheld_item_count
                public_alignment_status = alignment_status
                public_schema_ok = schema_ok
                public_truncated = truncated
                public_extracted_record_count = len(items)
            public_output_provenance = _public_segment_output_provenance(
                route,
                line_ledger_enabled=two_stage,
                output_tokens=output_tokens,
                max_output_tokens=max_output_tokens,
                canonicalization_report=canonicalization_report,
            )
            segment_lookup[segment_id] = row

            public_segment = {
                "segment_id": segment_id,
                "parent_document_id": parent_id,
                "candidate_cycle_id": expected["candidate_cycle_id"],
                "route": route,
                "segment_index": int(row["segment_index"]),
                "segment_count": expected_count,
                "segment_start_char": start,
                "segment_end_char": end,
                "segment_text_sha256": segment_hash,
                "processing_status": (
                    "processed"
                    if route_passed[route]
                    and (technical_success or partial_withheld)
                    else "withheld_validation"
                    if technical_success or partial_withheld or all_withheld
                    else "failed"
                ),
                "schema_valid": public_schema_ok,
                "input_truncated": public_truncated,
                "finish_reason": "stop",
                "stop_reason": (
                    None if row.get("stop_reason") is None else str(row["stop_reason"])
                ),
                "output_tokens": public_output_provenance["output_tokens"],
                "max_output_tokens": public_output_provenance[
                    "max_output_tokens"
                ],
                "extracted_record_count": public_extracted_record_count,
                **_public_metadata(metadata, route),
                "source_text_transform_version": SOURCE_TEXT_TRANSFORM_VERSION,
                "inference_display_escape_id": metadata[
                    "inference_display_escape_id"
                ],
                "inference_display_escape_spec_sha256": metadata[
                    "inference_display_escape_spec_sha256"
                ],
                "json_string_control_canonicalizer_id": metadata[
                    "json_string_control_canonicalizer_id"
                ],
                "json_string_control_canonicalizer_spec_sha256": metadata[
                    "json_string_control_canonicalizer_spec_sha256"
                ],
                "json_control_canonicalization_applied": (
                    public_output_provenance[
                        "json_control_canonicalization_applied"
                    ]
                ),
                "json_control_canonicalization_count": (
                    public_output_provenance[
                        "json_control_canonicalization_count"
                    ]
                ),
                "offset_coordinate_system": OFFSET_COORDINATE_SYSTEM,
                "exact_alignment_status": public_alignment_status,
                "model_item_count": public_model_item_count,
                "aligned_item_count": public_aligned_item_count,
                "withheld_item_count": public_withheld_item_count,
                "withheld_overlap_conflict_group_count": 0,
                **_public_segment_context_provenance(
                    route, context_provenance
                ),
            }
            segments_public.append(public_segment)
            public_segment_lookup[segment_id] = public_segment
        _require(previous_end == len(expected["source_text"]), f"{parent_id} segments do not reach the source end")
        _require(reconstructed == expected["source_text"], f"{parent_id} segments do not reconstruct the core source")

        provided_variants = [
            variant for row in rows for variant in row["record_variants"]
        ]
        provided_resolved = [
            record for row in rows for record in row["resolved_records"]
        ]
        if two_stage:
            _require(
                provided_variants == [],
                f"{parent_id} retains superseded endorsement variants",
            )
            _require(
                len(provided_resolved)
                == int(ledger_parent["aligned_record_count"]),
                f"{parent_id} endorsement ledger record count differs",
            )
        else:
            computed_variants = build_record_variants(rows)
            computed_annotated, computed_resolved = resolve_parent_variants(
                parent_id, route, computed_variants
            )
            _require(
                sorted(canonical_json(value) for value in provided_variants)
                == sorted(canonical_json(value) for value in computed_annotated),
                f"{parent_id} record variants differ from deterministic resolution",
            )
            _require(
                sorted(canonical_json(value) for value in provided_resolved)
                == sorted(canonical_json(value) for value in computed_resolved),
                f"{parent_id} resolved records differ from deterministic resolution",
            )
        parent_conflict_group_ids: set[str] = set()
        conflict_segment_ids: set[str] = set()
        conflict_groups_by_segment: dict[str, set[str]] = defaultdict(set)
        for variant in provided_variants:
            variant_id = _require_hex64(variant.get("variant_id"), "variant_id")
            _require(variant_id not in all_variant_ids, f"duplicate variant ID: {variant_id}")
            all_variant_ids.add(variant_id)
            if variant.get("overlap_resolution_status") == "conflict_withheld":
                conflict_group_id = _require_hex64(
                    variant.get("overlap_group_id"), "overlap_group_id"
                )
                conflict_group_ids.add(conflict_group_id)
                parent_conflict_group_ids.add(conflict_group_id)
                conflict_segment_id = str(variant["segment_id"])
                conflict_segment_ids.add(conflict_segment_id)
                conflict_groups_by_segment[conflict_segment_id].add(
                    conflict_group_id
                )
        for record in provided_resolved:
            record_id = _require_hex64(record.get("record_id"), "record_id")
            _require(record_id not in all_record_ids, f"duplicate record ID: {record_id}")
            all_record_ids.add(record_id)
        resolved_by_parent[parent_id] = provided_resolved
        if parent_conflict_group_ids:
            processing_withheld = True
            for conflict_segment_id in conflict_segment_ids:
                private_segment = segment_lookup[conflict_segment_id]
                public_segment = public_segment_lookup[conflict_segment_id]
                public_segment["withheld_overlap_conflict_group_count"] = len(
                    conflict_groups_by_segment[conflict_segment_id]
                )
                if not route_passed[route] or not private_segment["resolved_records"]:
                    public_segment["processing_status"] = "withheld_validation"

        documents.append({
            **{key: expected[key] for key in (
                "parent_document_id", "candidate_cycle_id", "candidate_icpsr", "state",
                "office", "year", "party", "route", "raw_text_sha256",
                "n_raw_occurrences", "first_capture_timestamp", "last_capture_timestamp",
                "original_urls",
            )},
            "wayback_urls": expected["source_urls"],
            "extraction_source_text_sha256": sha256_text(expected["source_text"]),
            "extraction_source_n_chars": len(expected["source_text"]),
            "segment_count": expected_count,
            "processed_segment_count": sum(
                row.get("processing_status")
                in {
                    "processed_success",
                    "processed_partial_withheld",
                    "processed_all_withheld",
                }
                for row in rows
            ),
            "record_count": 0,
            "withheld_overlap_conflict_group_count": len(
                parent_conflict_group_ids
            ),
            "model_item_count": sum(
                int((row.get("exact_alignment") or {}).get("model_item_count", 0))
                for row in rows
            ) if not two_stage else int(
                ledger_parent["aligned_record_count"]
            ) + int(ledger_parent["withheld_record_count"]),
            "aligned_item_count": sum(
                int((row.get("exact_alignment") or {}).get("aligned_item_count", 0))
                for row in rows
            ) if not two_stage else int(
                ledger_parent["aligned_record_count"]
            ),
            "withheld_item_count": sum(
                int((row.get("exact_alignment") or {}).get("withheld_item_count", 0))
                for row in rows
            ) if not two_stage else int(
                ledger_parent["withheld_record_count"]
            ),
            "processing_status": (
                "failed"
                if processing_failed
                else "withheld_validation"
                if not route_passed[route]
                or (
                    processing_withheld
                    and not provided_resolved
                )
                else "processed"
            ),
            **_public_metadata(metadata, route),
            "source_corpus_sha256": raw_sha256,
            "endorsement_context_provenance_version": (
                ENDORSEMENT_CONTEXT_PROVENANCE_VERSION
                if route == "endorsements" else None
            ),
            "endorsement_context_url_classifier_id": (
                ENDORSEMENT_CONTEXT_CLASSIFIER_ID
                if route == "endorsements" else None
            ),
            "endorsement_context_url_classifier_spec_sha256": (
                ENDORSEMENT_CONTEXT_CLASSIFIER_SPEC_SHA256
                if route == "endorsements" else None
            ),
            "endorsement_context_structural_heading_spec_sha256": (
                ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC_SHA256
                if route == "endorsements" else None
            ),
            "source_text_transform_version": SOURCE_TEXT_TRANSFORM_VERSION,
            "source_text_transform_rule": SOURCE_TEXT_TRANSFORM_RULE,
            "offset_coordinate_system": OFFSET_COORDINATE_SYSTEM,
            "source_text_transform_changed": expected[
                "source_text_transform_changed"
            ],
        })

    public_records: dict[str, list[dict[str, Any]]] = {route: [] for route in ROUTES}
    document_record_counts: Counter[str] = Counter()
    allowed_resolution_statuses = {
        "single_variant", "equivalent_overlap",
        "equivalent_semantics_distinct_exact_evidence",
        "line_ledger_single_record",
        ROUND4_RECORD_RESOLUTION_STATUS,
    }
    for parent_id, resolved_records in sorted(resolved_by_parent.items()):
        route = expected_documents[parent_id]["route"]
        parent_segment_ids = {
            _row_segment_id(row) for row in grouped[parent_id]
        }
        for resolved in sorted(resolved_records, key=lambda value: value["record_id"]):
            _require(resolved.get("parent_document_id") == parent_id, f"{resolved.get('record_id')} parent link differs")
            _require(resolved.get("route") == route, f"{resolved.get('record_id')} route differs")
            canonical_segment_id = _require_hex64(
                resolved.get("canonical_segment_id"), "canonical_segment_id"
            )
            _require(canonical_segment_id in parent_segment_ids, f"{resolved.get('record_id')} canonical segment differs")
            source_segment_ids = sorted(set(str(value) for value in resolved.get("all_source_segment_ids") or []))
            _require(bool(source_segment_ids), f"{resolved.get('record_id')} lacks source segment links")
            _require(set(source_segment_ids) <= parent_segment_ids, f"{resolved.get('record_id')} has a cross-parent segment link")
            _require(canonical_segment_id in source_segment_ids, f"{resolved.get('record_id')} canonical segment is not a source segment")
            resolution_status = str(resolved.get("resolution_status") or "")
            resolution_method = _required_text(
                resolved.get("resolution_method"), "resolution_method"
            )
            _require(resolution_status in allowed_resolution_statuses, f"{resolved.get('record_id')} has invalid resolution status")
            variant_count = _integer(resolved.get("variant_count"), "variant_count")
            _require(variant_count == len(resolved.get("all_variant_ids") or []), f"{resolved.get('record_id')} variant count differs")
            canonical_row = segment_lookup[canonical_segment_id]
            is_line_ledger_record = (
                route == "endorsements" and ledger_enabled
            )
            is_round4_record = route == "endorsements" and round4_enabled
            _require(
                not (is_line_ledger_record and is_round4_record),
                f"{resolved.get('record_id')} declares two endorsement"
                " architectures",
            )
            if is_line_ledger_record:
                _require(
                    resolution_status == "line_ledger_single_record"
                    and resolution_method
                    == "two_stage_parent_line_ledger_v1"
                    and variant_count == 1,
                    f"{resolved.get('record_id')} ledger resolution differs",
                )
            if is_round4_record:
                # The record labels name round four, so a line-ledger label here
                # would describe the record with the architecture that did not
                # produce it.
                _require(
                    resolution_status == ROUND4_RECORD_RESOLUTION_STATUS
                    and resolution_method == ROUND4_RECORD_RESOLUTION_METHOD
                    and variant_count == 1,
                    f"{resolved.get('record_id')} round-four resolution differs",
                )
            valid_item = _validate_item(
                route,
                resolved.get("item"),
                str(canonical_row["source_text"]),
                f"{resolved.get('record_id')}.item",
                endorsement_context_provenance=canonical_row.get(
                    "endorsement_context_provenance"
                ),
                segment_start_char=int(canonical_row["segment_start_char"]),
                parent_source_text=expected_documents[parent_id]["source_text"],
                line_ledger_item=is_line_ledger_record or is_round4_record,
                round4_item=is_round4_record,
            )
            if is_line_ledger_record or is_round4_record:
                source_model_item_id = valid_item["source_model_item_id"]
                _require(
                    resolved.get("canonical_variant_id")
                    == source_model_item_id
                    and resolved.get("all_variant_ids")
                    == [source_model_item_id],
                    f"{resolved.get('record_id')} ledger source identity differs",
                )
                _require(
                    valid_item["record_evidence_carrier_segment_id"]
                    == canonical_segment_id,
                    f"{resolved.get('record_id')} ledger carrier differs",
                )
                _require(
                    valid_item["inventory_owner_segment_id"]
                    in source_segment_ids,
                    f"{resolved.get('record_id')} ledger inventory owner differs",
                )
            parent_payload = _parent_payload(
                route, valid_item, int(canonical_row["segment_start_char"])
            )
            expected_parent_offsets = {
                key: value for key, value in parent_payload.items()
                if "_parent_" in key
            }
            _require(
                resolved.get("parent_offsets") == expected_parent_offsets,
                f"{resolved.get('record_id')} parent offsets differ",
            )
            public_records[route].append({
                "record_id": _require_hex64(resolved.get("record_id"), "record_id"),
                "parent_document_id": parent_id,
                "segment_id": canonical_segment_id,
                "source_segment_ids": source_segment_ids,
                "candidate_cycle_id": expected_documents[parent_id]["candidate_cycle_id"],
                "variant_count": variant_count,
                "overlap_resolution": resolution_status,
                "overlap_resolution_method": resolution_method,
                **parent_payload,
            })
            document_record_counts[parent_id] += 1
    for document in documents:
        document["record_count"] = document_record_counts[document["parent_document_id"]]

    parent_counts = Counter(document["route"] for document in documents)
    segment_counts = Counter(segment["route"] for segment in segments_public)
    declared_parent_counts = {key: int(value) for key, value in (manifest.get("parent_documents_by_route") or {}).items()}
    declared_segment_counts = {key: int(value) for key, value in (manifest.get("segments_by_route") or {}).items()}
    _require(dict(parent_counts) == declared_parent_counts, "bundle parent route counts differ")
    _require(dict(segment_counts) == declared_segment_counts, "bundle segment route counts differ")
    processing_counts = dict(sorted(Counter(
        str(row.get("processing_status")) for row in segment_rows
    ).items()))
    declared_processing_counts = {
        key: int(value)
        for key, value in (manifest.get("segment_processing_status") or {}).items()
    }
    _require(processing_counts == declared_processing_counts, "bundle processing status counts differ")
    withheld_cap_rows = int(processing_counts.get(OUTPUT_CAP_WITHHELD_STATUS, 0))
    _require(
        withheld_cap_rows <= len(unrepaired_cap_windows),
        "bundle holds more cap-withheld rows than the repair attestation reports",
    )
    technical_pass = technical_segment_gate_passes(
        processing_counts,
        len(segment_rows),
        allow_withheld_output_cap=bool(withheld_cap_rows),
    )
    _require(
        manifest.get("technical_segment_gate_passed") is technical_pass,
        "bundle technical segment gate differs from segment rows",
    )
    _require(int(manifest.get("raw_record_variants", -1)) == len(all_variant_ids), "bundle raw variant count differs")
    _require(int(manifest.get("resolved_records", -1)) == len(all_record_ids), "bundle resolved record count differs")
    _require(
        int(manifest.get("withheld_overlap_conflict_groups", -1))
        == len(conflict_group_ids),
        "bundle conflict group count differs",
    )
    declared_record_counts = {
        key: int(value)
        for key, value in (manifest.get("resolved_records_by_route") or {}).items()
    }
    observed_record_counts = {
        route: len(public_records[route]) for route in ROUTES
        if public_records[route]
    }
    _require(observed_record_counts == declared_record_counts, "bundle record route counts differ")

    status_rows: list[dict[str, Any]] = []
    docs_by_key: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for document in documents:
        docs_by_key[(document["candidate_cycle_id"], document["route"])].append(document)
    segments_by_parent = Counter(segment["parent_document_id"] for segment in segments_public)
    records_by_parent = Counter()
    for route_rows in public_records.values():
        records_by_parent.update(row["parent_document_id"] for row in route_rows)
    for candidate_id in sorted(universe_ids):
        for route in ROUTES:
            source = source_state.get((candidate_id, route), {"n_raw_pages": 0, "n_extractable_pages": 0})
            key_documents = docs_by_key.get((candidate_id, route), [])
            n_documents = len(key_documents)
            n_segments = sum(segments_by_parent[document["parent_document_id"]] for document in key_documents)
            n_records = sum(records_by_parent[document["parent_document_id"]] for document in key_documents)
            if source["n_raw_pages"] == 0:
                observability = "not_observed"
            elif source["n_extractable_pages"] == 0:
                observability = "observed_empty"
            else:
                observability = "observed_extractable"
            if observability != "observed_extractable":
                _require(n_documents == n_segments == n_records == 0, f"{candidate_id}/{route} has extraction without source")
                extraction_status = "not_extracted_no_source"
            else:
                _require(n_documents > 0, f"{candidate_id}/{route} has an uncovered extractable source")
                statuses = {document["processing_status"] for document in key_documents}
                total_withheld_items = sum(
                    int(document["withheld_item_count"])
                    for document in key_documents
                )
                if "failed" in statuses:
                    extraction_status = "failed"
                elif not route_passed[route]:
                    extraction_status = "withheld_validation"
                elif n_records:
                    extraction_status = "observed_positive"
                elif total_withheld_items or "withheld_validation" in statuses:
                    extraction_status = "withheld_validation"
                else:
                    _require(
                        statuses == {"processed"},
                        f"{candidate_id}/{route} is zero without complete processing",
                    )
                    extraction_status = "observed_zero"
            status_rows.append({
                "candidate_cycle_id": candidate_id,
                "route": route,
                "source_observability": observability,
                "extraction_status": extraction_status,
                "n_raw_pages": int(source["n_raw_pages"]),
                "n_documents": n_documents,
                "n_segments": n_segments,
                "n_records": n_records,
                "n_withheld_overlap_conflict_groups": sum(
                    int(document["withheld_overlap_conflict_group_count"])
                    for document in key_documents
                ),
            })

    documents.sort(key=lambda row: (row["route"], row["parent_document_id"]))
    segments_public.sort(key=lambda row: (row["route"], row["parent_document_id"], row["segment_index"]))
    for values in public_records.values():
        values.sort(key=lambda row: (row["parent_document_id"], row["record_id"]))

    return {
        "candidate_extraction_status.csv": pd.DataFrame(status_rows, columns=STATUS_COLUMNS),
        "candidate_extraction_documents.parquet": pa.Table.from_pylist(documents, schema=TABLE_SCHEMAS["candidate_extraction_documents.parquet"]),
        "candidate_extraction_segments.parquet": pa.Table.from_pylist(segments_public, schema=TABLE_SCHEMAS["candidate_extraction_segments.parquet"]),
        "candidate_issue_statements.parquet": pa.Table.from_pylist(public_records["issues"], schema=TABLE_SCHEMAS["candidate_issue_statements.parquet"]),
        "candidate_biography_facts.parquet": pa.Table.from_pylist(public_records["biography"], schema=TABLE_SCHEMAS["candidate_biography_facts.parquet"]),
        "candidate_endorsements.parquet": pa.Table.from_pylist(public_records["endorsements"], schema=TABLE_SCHEMAS["candidate_endorsements.parquet"]),
    }


def filter_released_tables(
    tables: dict[str, Any], excluded_parents: set[str]
) -> dict[str, Any]:
    """Drop the excluded parents from every released table.

    The population rule applies to the released files, not to the bundle or the
    core corpus, so the packager's bundle-vs-core equalities stay intact. Each
    table keyed by `parent_document_id` is filtered; the status table is keyed
    by candidate-year and route, so it is left whole and the documents table
    carries the population.
    """
    if not excluded_parents:
        return tables
    filtered: dict[str, Any] = {}
    for name, table in tables.items():
        if name.endswith(".csv"):
            filtered[name] = table
            continue
        if "parent_document_id" not in table.column_names:
            filtered[name] = table
            continue
        keep = [
            value not in excluded_parents
            for value in table.column("parent_document_id").to_pylist()
        ]
        filtered[name] = table.filter(pa.array(keep))
    return filtered


def prepare_release_tables(
    core_release_dir: Path,
    bundle_dir: Path,
    *,
    production: bool = True,
    allow_withheld_validation: bool = False,
) -> tuple[dict[str, Any], dict[str, Any]]:
    universe, raw, raw_sha256 = _load_core(core_release_dir)
    manifest, rows, validation, route_passed = load_bundle(
        bundle_dir, production=production
    )
    if production:
        _validate_run_metadata_entries(manifest)
        _validate_seed_vary_repair(manifest)
        _require(raw_sha256 == EXPECTED_CORE_RAW_SHA256, "core raw-corpus hash is not the approved release hash")
        _require_hex64(manifest.get("source_export_manifest_sha256"), "source export manifest hash")
        _require(len(universe) == EXPECTED_UNIVERSE_ROWS, "core universe row count differs")
        _require(manifest.get("model_id") == EXPECTED_MODEL_ID, "bundle model ID is not approved")
        _require(manifest.get("model_revision") == EXPECTED_MODEL_REVISION, "bundle model revision is not approved")
        _require(manifest.get("runtime_lock_sha256") == EXPECTED_RUNTIME_LOCK_SHA256, "bundle runtime lock is not approved")
        _require(_typed_equal(manifest.get("prompt_sha256"), V2_CONTRACT["prompt_sha256"]), "bundle prompt hashes are not approved")
        _require(_typed_equal(manifest.get("json_schema_sha256"), V2_CONTRACT["json_schema_sha256"]), "bundle JSON schema hashes are not approved")
        _require(manifest.get("extraction_contract_sha256") == EXPECTED_EXTRACTION_CONTRACT_SHA256, "bundle extraction contract is not approved")
        _require(
            manifest.get("alignment_id") == V2_CONTRACT["alignment_id"]
            and manifest.get("alignment_spec_sha256")
            == V2_CONTRACT["alignment_spec_sha256"]
            and manifest.get("semantic_validation_spec_sha256")
            == V2_CONTRACT["semantic_validation_spec_sha256"]
            and manifest.get("endorsement_context_provenance_spec_sha256")
            == V2_CONTRACT[
                "endorsement_context_provenance_spec_sha256"
            ],
            "bundle alignment, semantic, or context provenance is not approved",
        )
        _require(
            manifest.get("user_message_template_id")
            == V2_CONTRACT["user_message_template_id"]
            and manifest.get("user_message_template_sha256")
            == V2_CONTRACT["user_message_template_sha256"],
            "bundle user-message template provenance is not approved",
        )
        _require(
            manifest.get("inference_display_escape_id")
            == V2_CONTRACT["inference_display_escape_id"]
            and manifest.get("inference_display_escape_spec_sha256")
            == V2_CONTRACT["inference_display_escape_spec_sha256"]
            and manifest.get("json_string_control_canonicalizer_id")
            == V2_CONTRACT["json_string_control_canonicalizer_id"]
            and manifest.get("json_string_control_canonicalizer_spec_sha256")
            == V2_CONTRACT[
                "json_string_control_canonicalizer_spec_sha256"
            ],
            "bundle display or JSON-control provenance is not approved",
        )
        _require(
            manifest.get("array_item_cap_spec_id")
            == V2_CONTRACT["array_item_cap_spec_id"]
            and manifest.get("array_item_cap_spec_sha256")
            == V2_CONTRACT["array_item_cap_spec_sha256"]
            and _typed_equal(
                manifest.get("route_array_item_caps"),
                V2_CONTRACT["route_array_item_caps"],
            )
            and manifest.get("model_item_repetition_spec_id")
            == V2_CONTRACT["model_item_repetition_spec_id"]
            and manifest.get("model_item_repetition_spec_sha256")
            == V2_CONTRACT["model_item_repetition_spec_sha256"],
            "bundle model-item guard provenance is not approved",
        )
        _require(manifest.get("technical_segment_gate_passed") is True, "bundle technical segment gate did not pass")
        ledger = manifest.get("_validated_endorsement_line_ledger")
        round4_ledger = manifest.get("_validated_endorsement_round4_ledger")
        if round4_ledger is not None:
            # Round four has no adjudication stage, so a corpus-wide coverage
            # boolean is not reachable.  The release instead requires complete
            # accounting: every model output accounted, no open infrastructure
            # failure, and a typed reason on every remaining gap.  Hanno
            # approved this gate on September 15, 2026.
            round4_summary = round4_ledger.get("summary")
            _require(
                isinstance(round4_summary, dict)
                and round4_summary.get("all_model_records_accounted") is True
                and round4_summary.get("unaccounted_outputs") == 0
                and round4_summary.get("unresolved_infrastructure_failures")
                == 0,
                "round-four endorsement accounting is incomplete",
            )
        else:
            _require(
                isinstance(ledger, dict)
                and isinstance(ledger.get("summary"), dict)
                and ledger["summary"].get("public_coverage_complete") is True,
                "two-stage endorsement line-ledger coverage is incomplete",
            )
        _require(manifest.get("known_omission_gate") is True, "bundle did not recover every known omission")
        if not allow_withheld_validation:
            _require(all(route_passed.values()), "human-only release validation did not pass")
    tables = _tables_from_inputs(universe, raw, raw_sha256, manifest, rows, route_passed)
    if production:
        document_counts = Counter(tables["candidate_extraction_documents.parquet"].column("route").to_pylist())
        segment_counts = Counter(tables["candidate_extraction_segments.parquet"].column("route").to_pylist())
        _require(dict(document_counts) == EXPECTED_PARENT_COUNTS, "public document route counts differ")
        _require(dict(segment_counts) == EXPECTED_SEGMENT_COUNTS, "public segment route counts differ")
        transformed_count = sum(
            tables["candidate_extraction_documents.parquet"]
            .column("source_text_transform_changed")
            .to_pylist()
        )
        _require(
            transformed_count == EXPECTED_TRANSFORMED_PARENT_DOCUMENTS,
            "public transformed-parent count differs",
        )
        statuses = tables["candidate_extraction_status.csv"]
        _require(len(statuses) == EXPECTED_UNIVERSE_ROWS * len(ROUTES), "status table is not rectangular")
        _require(not statuses["extraction_status"].eq("failed").any(), "release has a failed extraction status")
    # The population filter runs after the frozen corpus checks, so those checks
    # still prove the corpus is the approved one. Hanno approved dropping
    # official government pages on 2026-09-24. The expected count is asserted,
    # so a change in what the rule removes fails closed instead of passing.
    excluded_parents, excluded_host_report = excluded_parent_document_ids(raw)
    if production:
        _require(
            len(excluded_parents) == EXPECTED_EXCLUDED_PARENT_DOCUMENTS,
            "the official-government-host rule removes an unexpected count",
        )
    before = {
        name: int(len(value)) for name, value in tables.items()
    }
    tables = filter_released_tables(tables, excluded_parents)
    excluded_host_report["released_rows_before"] = before
    excluded_host_report["released_rows_after"] = {
        name: int(len(value)) for name, value in tables.items()
    }
    summary = {
        "schema_version": SCHEMA_VERSION,
        "source_corpus_sha256": raw_sha256,
        "endorsement_architecture": _endorsement_architecture(manifest),
        "endorsement_route_contract_sha256": _endorsement_route_contract(
            manifest
        ),
        "generation_config_sha256": manifest[
            "generation_config_sha256"
        ],
        "array_item_cap_spec_id": manifest["array_item_cap_spec_id"],
        "array_item_cap_spec_sha256": manifest[
            "array_item_cap_spec_sha256"
        ],
        "route_array_item_caps": manifest["route_array_item_caps"],
        "model_item_repetition_spec_id": manifest[
            "model_item_repetition_spec_id"
        ],
        "model_item_repetition_spec_sha256": manifest[
            "model_item_repetition_spec_sha256"
        ],
        "strict_json_parsed_outputs": manifest["all_output_aggregate"][
            "strict_json_parsed_outputs"
        ],
        "model_items_strictly_parsed": manifest["all_output_aggregate"][
            "model_items_strictly_parsed"
        ],
        "array_item_cap_reached_outputs": manifest["all_output_aggregate"][
            "array_item_cap_reached_outputs"
        ],
        "model_item_repetition_error_outputs": manifest[
            "all_output_aggregate"
        ]["model_item_repetition_error_outputs"],
        "pre_alignment_model_item_guard_gate_passed": (
            manifest["all_output_aggregate"]["array_item_cap_reached_outputs"]
            == 0
            and manifest["all_output_aggregate"][
                "model_item_repetition_error_outputs"
            ]
            == 0
        ),
        "raw_structural_whitespace_validator_id": (
            RAW_STRUCTURAL_WHITESPACE_VALIDATOR_ID
        ),
        "raw_structural_whitespace_validator_spec_sha256": (
            RAW_STRUCTURAL_WHITESPACE_VALIDATOR_SPEC_SHA256
        ),
        "raw_structural_whitespace_gate_passed": True,
        "human_validation_sha256": sha256_text(json.dumps(validation, sort_keys=True, separators=(",", ":"))),
        "rows": {
            name: int(len(value)) for name, value in tables.items()
        },
        "excluded_official_government_hosts": excluded_host_report,
        "route_validation_passed": route_passed,
        "release_ready": all(route_passed.values())
        and not tables["candidate_extraction_status.csv"]["extraction_status"].eq("failed").any(),
    }
    return tables, summary


def write_release_tables(tables: dict[str, Any], output_dir: Path) -> None:
    if output_dir.exists() and any(output_dir.iterdir()):
        raise FileExistsError(
            f"refusing nonempty extraction release directory: {output_dir}"
        )
    output_dir.mkdir(parents=True, exist_ok=True)
    temporary = Path(tempfile.mkdtemp(prefix="candidate-extraction-release-", dir=output_dir.parent))
    try:
        for name, table in tables.items():
            target = temporary / name
            if name.endswith(".csv"):
                table.to_csv(target, index=False)
            else:
                pq.write_table(table, target, compression="snappy")
        for name in PUBLIC_FILES:
            os.replace(temporary / name, output_dir / name)
    finally:
        shutil.rmtree(temporary, ignore_errors=True)


def extension_manifest_entries(output_dir: Path) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for name in PUBLIC_FILES:
        path = output_dir / name
        _require(path.is_file(), f"extension output is absent: {name}")
        if name.endswith(".csv"):
            rows, columns = pd.read_csv(path, low_memory=False).shape
        else:
            metadata = pq.ParquetFile(path).metadata
            rows, columns = metadata.num_rows, metadata.num_columns
        entries.append({
            "file": name,
            "rows": int(rows),
            "columns": int(columns),
            "bytes": path.stat().st_size,
            "sha256": sha256_file(path),
        })
    return entries


def build_release(
    core_release_dir: Path,
    bundle_dir: Path,
    output_dir: Path,
    *,
    production: bool = True,
    allow_withheld_validation: bool = False,
) -> dict[str, Any]:
    tables, summary = prepare_release_tables(
        core_release_dir,
        bundle_dir,
        production=production,
        allow_withheld_validation=allow_withheld_validation,
    )
    write_release_tables(tables, output_dir)
    entries = extension_manifest_entries(output_dir)
    summary["manifest_entries"] = entries
    # The release validator reads this inventory through --manifest, so the
    # packager writes it. Before 2026-09-27 the summary carried the entries but
    # nothing wrote the file, and the 2026-09-25 inventory had to be rebuilt by
    # hand, which is not reproducible.
    manifest_path = output_dir / EXTENSION_MANIFEST_FILENAME
    manifest_path.write_text(
        json.dumps(entries, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    summary["manifest_file"] = EXTENSION_MANIFEST_FILENAME
    return summary


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--core-release-dir", type=Path, required=True)
    parser.add_argument("--bundle-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument(
        "--allow-withheld-validation",
        action="store_true",
        help="Build a nonpublishable sampling package from pending human validation.",
    )
    args = parser.parse_args(argv)
    summary = build_release(
        args.core_release_dir,
        args.bundle_dir,
        args.output_dir,
        allow_withheld_validation=args.allow_withheld_validation,
    )
    print(json.dumps(summary, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
