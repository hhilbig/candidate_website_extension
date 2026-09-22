"""Production binding and frozen contract for round-four endorsement work.

The round-four development module defines the reviewed prompts, guided-output
schemas, and deterministic source checks.  That module stays unchanged, because
its binding hash is embedded in every development generation config and in the
frozen run metadata of the completed development jobs.

This module adds the production scope on top of those reviewed semantics.  It
binds the identical prompts and schemas to a production contract, states the
record projection rule, states the technical-failure classes, and states the
coverage rule.  Hanno selected the round-four implementation for production on
September 15, 2026, in step 1 of the release checklist.

Production integration and full expansion are authorized.  Release is not.
Release requires human validation and publication approval, which are later
steps in the same checklist.
"""
from __future__ import annotations

from typing import Any

from src.candidate_endorsement_round4 import (
    ENDORSEMENT_CATEGORIES,
    ENTITY_TYPES,
    MAX_NAME_PARTS,
    MAX_RECORDS_PER_STAGE_B_SLOT,
    OPTIONAL_FIELDS,
    ROUND4_DEVELOPMENT_BINDING_SHA256,
    ROUND4_STAGE_A_PROMPT_SHA256,
    ROUND4_STAGE_B_PROMPT_SHA256,
    ROUND4_VERSION,
    STAGE_A_ROLES,
    STAGE_A_SLOT_COUNT,
    STAGE_B_DISPOSITIONS,
    STAGE_B_SLOT_COUNT,
    sha256_json,
    stage_a_schema,
    stage_b_schema,
)
from src.candidate_extraction import (
    INFERENCE_DISPLAY_ESCAPE_ID,
    INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256,
    MODEL_ID,
    MODEL_REVISION,
    RUNTIME_LOCK_SHA256,
    get_extraction_contract,
)


ROUND4_PRODUCTION_VERSION = (
    "candidate_endorsement_round4_production_semantics_v1"
)
STAGE_A = "line_decision"
STAGE_B = "record_extraction"
PRODUCTION_STAGES = (STAGE_A, STAGE_B)

# The authority that moved round four from development to production.  The
# development module keeps its own binding, which must never change.
ROUND4_PRODUCTION_AUTHORITY = "hanno_decision_2026_09_15_docs_status_step_1"

ROUND4_PRODUCTION_BINDING = {
    "version": ROUND4_PRODUCTION_VERSION,
    "scope": "production_extraction_and_release_integration",
    "supersedes": ROUND4_VERSION,
    "stage_a_prompt_sha256": ROUND4_STAGE_A_PROMPT_SHA256,
    "stage_b_prompt_sha256": ROUND4_STAGE_B_PROMPT_SHA256,
    "development_labels_authoritative": False,
    "human_truth_established": False,
    "production_integration_authorized": True,
    "full_expansion_authorized": True,
    "release_authorized": False,
    "authority": ROUND4_PRODUCTION_AUTHORITY,
}
ROUND4_PRODUCTION_BINDING_SHA256 = sha256_json(ROUND4_PRODUCTION_BINDING)

# The exact development binding hash.  A production change must never alter
# the development artifacts, so the tests pin this value.
FROZEN_ROUND4_DEVELOPMENT_BINDING_SHA256 = (
    "a5c482b99244d3541e45c72621f358f60562c857e57220e28195cb480e4a8f40"
)

# The production authorization set reuses the exact key set of the completed
# development runs, so the two are directly comparable.
ROUND4_PRODUCTION_AUTHORIZATIONS = {
    "bounded_development_inference": False,
    "development_labels_authoritative": False,
    "human_truth": False,
    "production_integration": True,
    "full_expansion": True,
    "release": False,
}
ROUND4_PRODUCTION_ARTIFACT_SCOPE = "candidate_endorsement_round4_production"

# The exact scope string and authorization set of the completed development
# runs.  The adapter reads real development outputs during the replay, so
# these values must match those artifacts byte for byte.
ROUND4_DEVELOPMENT_ARTIFACT_SCOPE = (
    "candidate_endorsement_round4_development_only"
)
ROUND4_DEVELOPMENT_AUTHORIZATIONS = {
    "bounded_development_inference": True,
    "development_labels_authoritative": False,
    "human_truth": False,
    "production_integration": False,
    "full_expansion": False,
    "release": False,
}

# The runner accepts exactly these scope and authorization pairs.  It never
# accepts a mixed pair, so a development artifact can never enter the release
# path and a production artifact can never claim release authority.
ROUND4_ALLOWED_SCOPE_AUTHORIZATIONS = {
    ROUND4_DEVELOPMENT_ARTIFACT_SCOPE: dict(ROUND4_DEVELOPMENT_AUTHORIZATIONS),
    ROUND4_PRODUCTION_ARTIFACT_SCOPE: dict(ROUND4_PRODUCTION_AUTHORIZATIONS),
}

# Round four reads the frozen parent line inventory in its base scope.  The
# development input builder applied no route revision, so production must not
# apply one either.
ROUND4_LINE_INVENTORY_SCOPE_RULE = (
    "reconstruct_parent_inventory_base_scope_v1"
)

ANCHOR_ROLES = ("record_anchor", "compound_record_anchor")
CONTINUATION_ROLES = (
    "record_continuation_previous", "record_continuation_next",
)


def _schema_catalog_sha256(builder: Any, slot_count: int) -> str:
    return sha256_json({
        str(target_count): builder(target_count, slot_count)
        for target_count in range(1, slot_count + 1)
    })


ROUND4_STAGE_A_SCHEMA_CATALOG_SHA256 = _schema_catalog_sha256(
    stage_a_schema, STAGE_A_SLOT_COUNT
)
ROUND4_STAGE_B_SCHEMA_CATALOG_SHA256 = _schema_catalog_sha256(
    stage_b_schema, STAGE_B_SLOT_COUNT
)

ROUND4_RECORD_PROJECTION_SPEC = {
    "version": "candidate_endorsement_round4_record_projection_v1",
    "name_rule": (
        "join_validated_ordered_name_parts;_project_every_part_to_an_exact_"
        "ordered_nonoverlapping_source_span_inside_the_target_block"
    ),
    "name_match_tolerance": (
        "nfkc,_unicode_whitespace_collapse,_and_case_only;_punctuation_"
        "remains_significant;_partial_word_matches_are_rejected"
    ),
    "maximum_name_parts": MAX_NAME_PARTS,
    "optional_field_rule": (
        "accept_only_an_exact_contiguous_visible_substring;_otherwise_null_"
        "the_field_and_record_the_change_in_the_private_repair_ledger"
    ),
    "optional_fields": list(OPTIONAL_FIELDS),
    "record_evidence_rule": (
        "the_trimmed_record_block_text_projected_to_exact_parent_coordinates"
    ),
    "context_evidence_rule": (
        "deterministic_carrier_segment_endorsement_context_provenance;_never_"
        "model_output"
    ),
    "direct_support_context_rule": (
        "when_the_support_mode_is_explicit_text_and_the_carrier_segment_"
        "carries_no_segment_scoped_context_heading,_the_record_block_is_its_"
        "own_context_evidence_at_the_identical_span;_the_projection_is_"
        "verified_after_alignment_and_withheld_on_any_mismatch"
    ),
    "support_mode_rule": (
        "mask_identity_values_then_match_the_frozen_explicit_endorsement_"
        "pattern;_otherwise_page_or_section_context"
    ),
    "record_identity_rule": (
        "sha256_of_the_production_contract,_batch_id,_slot,_model_record_"
        "index,_and_the_exact_parsed_model_record"
    ),
    "records_per_slot_guided_maximum": MAX_RECORDS_PER_STAGE_B_SLOT,
    "semantic_rule": (
        "deterministic_code_never_creates,_repairs,_or_relabels_a_semantic_"
        "endorsement_record"
    ),
}
ROUND4_RECORD_PROJECTION_SPEC_SHA256 = sha256_json(
    ROUND4_RECORD_PROJECTION_SPEC
)

# The five allowed outcomes for one Stage-B batch response.  Nothing else is
# allowed.  A content-validation failure is deterministic at temperature zero
# with a fixed seed, so re-inference cannot repair it.
ROUND4_FAILURE_CLASSES = (
    "complete",
    "optional_field_repaired",
    "unresolved_target_block",
    "infrastructure_failure",
    "unlocalizable_schema_failure",
)

ROUND4_FAILURE_ACCOUNTING_SPEC = {
    "version": "candidate_endorsement_round4_failure_accounting_v1",
    "classes": list(ROUND4_FAILURE_CLASSES),
    "complete": {
        "detection": "technical_pass_is_true",
        "treatment": "accept_every_slot",
        "reinference_allowed": False,
    },
    "optional_field_repaired": {
        "detection": (
            "slot_isolated_replay_passes_after_optional_field_sanitization"
        ),
        "treatment": (
            "accept_the_slot;_null_the_unsupported_optional_field;_record_"
            "every_change_in_the_private_repair_ledger"
        ),
        "reinference_allowed": False,
        "reason": (
            "decoding_is_deterministic_at_temperature_zero_with_a_fixed_seed"
        ),
    },
    "unresolved_target_block": {
        "detection": "slot_isolated_replay_still_fails",
        "treatment": (
            "the_anchor_yields_no_record;_count_it_as_a_withheld_record_with_"
            "a_typed_reason;_enumerate_it_in_the_summary"
        ),
        "reinference_allowed": False,
        "measured_zero_allowed": False,
    },
    "infrastructure_failure": {
        "detection": (
            "missing_output_row,_generation_incomplete,_strict_json_parse_"
            "error,_or_truncated_input"
        ),
        "treatment": "bounded_retry;_blocks_the_overlay_build_while_open",
        "reinference_allowed": True,
    },
    "unlocalizable_schema_failure": {
        "detection": (
            "schema_invalid_at_batch_level_with_no_possible_slot_isolation"
        ),
        "treatment": "bounded_retry;_blocks_the_overlay_build_while_open",
        "reinference_allowed": True,
    },
    "never_a_measured_zero": (
        "a_parent_with_zero_aligned_records_and_at_least_one_withheld_record_"
        "reports_withheld_validation,_never_observed_zero"
    ),
    "retry_rule": {
        "maximum_attempts_per_batch": 2,
        "identical_prompt_schema_seed_and_model": True,
        "only_input_manifest_sha256_differs": True,
        "provenance_retention": (
            "every_attempt_is_retained_with_attempt_index,_input_manifest_"
            "sha256,_generation_config_sha256,_slurm_job_id,_raw_output_"
            "sha256,_and_the_typed_outcome;_the_accepted_attempt_is_marked"
        ),
        "exhausted_rule": (
            "a_batch_that_still_fails_after_two_attempts_blocks_the_release_"
            "and_is_reported_with_its_evidence"
        ),
    },
}
ROUND4_FAILURE_ACCOUNTING_SPEC_SHA256 = sha256_json(
    ROUND4_FAILURE_ACCOUNTING_SPEC
)

ROUND4_COVERAGE_SPEC = {
    "version": "candidate_endorsement_round4_coverage_v1",
    "architecture": "two_stage_without_model_or_human_line_adjudication",
    "per_parent_rule": (
        "a_parent_is_complete_when_every_in_scope_line_holds_a_terminal_stage_"
        "a_role_and_every_anchor_holds_a_resolved_stage_b_decision"
    ),
    "stage_a_unclear_rule": (
        "a_stage_a_unclear_line_makes_its_parent_incomplete;_round_four_has_"
        "no_adjudication_stage_that_can_clear_it"
    ),
    "unowned_continuation_rule": (
        "an_unowned_continuation_line_is_excluded_from_every_record_block_and_"
        "enumerated_in_the_summary"
    ),
    "not_endorsement_rule": (
        "a_not_endorsement_disposition_is_a_resolved_decision_and_keeps_its_"
        "parent_complete"
    ),
    "corpus_rule": (
        "the_release_requires_complete_accounting,_not_complete_coverage:_"
        "every_model_output_is_accounted,_no_infrastructure_failure_remains_"
        "open,_and_every_gap_carries_a_typed_reason_and_an_identifier"
    ),
    "corpus_gate_fields": [
        "all_model_records_accounted",
        "unaccounted_outputs",
        "unresolved_infrastructure_failures",
    ],
    "corpus_gate_authority": ROUND4_PRODUCTION_AUTHORITY,
}
ROUND4_COVERAGE_SPEC_SHA256 = sha256_json(ROUND4_COVERAGE_SPEC)

_BASE_V2_1_CONTRACT = get_extraction_contract("2.1")


def computed_round4_production_contract() -> dict[str, Any]:
    """Build the round-four production contract payload and its digest."""
    payload = {
        "contract_version": (
            "candidate_endorsement_round4_production_contract_v1"
        ),
        "base_schema_version": "2.1",
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
        "source_corpus_contract_sha256": _BASE_V2_1_CONTRACT[
            "extraction_contract_sha256"
        ],
        "unchanged_issue_prompt_sha256": _BASE_V2_1_CONTRACT["prompt_sha256"][
            "issues"
        ],
        "unchanged_issue_schema_sha256": _BASE_V2_1_CONTRACT[
            "json_schema_sha256"
        ]["issues"],
        "unchanged_biography_prompt_sha256": _BASE_V2_1_CONTRACT[
            "prompt_sha256"
        ]["biography"],
        "unchanged_biography_schema_sha256": _BASE_V2_1_CONTRACT[
            "json_schema_sha256"
        ]["biography"],
        "production_binding_sha256": ROUND4_PRODUCTION_BINDING_SHA256,
        "development_binding_sha256": ROUND4_DEVELOPMENT_BINDING_SHA256,
        "prompt_sha256": {
            STAGE_A: ROUND4_STAGE_A_PROMPT_SHA256,
            STAGE_B: ROUND4_STAGE_B_PROMPT_SHA256,
        },
        "json_schema_variant_version": (
            "candidate_endorsement_round4_fixed_slot_schema_v1"
        ),
        "json_schema_catalog_sha256": {
            STAGE_A: ROUND4_STAGE_A_SCHEMA_CATALOG_SHA256,
            STAGE_B: ROUND4_STAGE_B_SCHEMA_CATALOG_SHA256,
        },
        "slot_counts": {
            STAGE_A: STAGE_A_SLOT_COUNT,
            STAGE_B: STAGE_B_SLOT_COUNT,
        },
        "stage_a_roles": list(STAGE_A_ROLES),
        "stage_b_dispositions": list(STAGE_B_DISPOSITIONS),
        "entity_types": list(ENTITY_TYPES),
        "endorser_categories": list(ENDORSEMENT_CATEGORIES),
        "line_inventory_scope_rule": ROUND4_LINE_INVENTORY_SCOPE_RULE,
        "record_projection_spec_sha256": ROUND4_RECORD_PROJECTION_SPEC_SHA256,
        "failure_accounting_spec_sha256": (
            ROUND4_FAILURE_ACCOUNTING_SPEC_SHA256
        ),
        "coverage_spec_sha256": ROUND4_COVERAGE_SPEC_SHA256,
        "inference_display_escape_id": INFERENCE_DISPLAY_ESCAPE_ID,
        "inference_display_escape_spec_sha256": (
            INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256
        ),
    }
    return {
        **payload,
        "round4_production_contract_sha256": sha256_json(payload),
    }


# Pinned after the implementation and its tests pass.  An empty value keeps
# the contract explicitly open, which follows the line-ledger idiom.
FROZEN_ROUND4_PRODUCTION_CONTRACT_SHA256 = (
    "f8c755ce5097345cd5e99755c62f470ac6fdfdfadc116d4b8ecc41da7797522b"
)


def get_round4_production_contract() -> dict[str, Any]:
    """Return the production contract and enforce its frozen digest."""
    contract = computed_round4_production_contract()
    if (
        FROZEN_ROUND4_PRODUCTION_CONTRACT_SHA256
        and contract["round4_production_contract_sha256"]
        != FROZEN_ROUND4_PRODUCTION_CONTRACT_SHA256
    ):
        raise RuntimeError(
            "round-four production contract differs from the frozen hash"
        )
    return contract


def validate_scope_authorizations(
    artifact_scope: str, authorizations: Any,
) -> None:
    """Fail closed unless the scope and authorization pair is exactly allowed."""
    if type(artifact_scope) is not str:
        raise TypeError("artifact_scope must be a string")
    expected = ROUND4_ALLOWED_SCOPE_AUTHORIZATIONS.get(artifact_scope)
    if expected is None:
        raise ValueError(f"unknown round-four artifact scope: {artifact_scope}")
    if not isinstance(authorizations, dict):
        raise TypeError("authorizations must be a dictionary")
    if authorizations != expected:
        raise ValueError(
            "round-four authorizations do not match the artifact scope"
        )


__all__ = [
    "ANCHOR_ROLES",
    "CONTINUATION_ROLES",
    "FROZEN_ROUND4_DEVELOPMENT_BINDING_SHA256",
    "FROZEN_ROUND4_PRODUCTION_CONTRACT_SHA256",
    "PRODUCTION_STAGES",
    "ROUND4_ALLOWED_SCOPE_AUTHORIZATIONS",
    "ROUND4_COVERAGE_SPEC",
    "ROUND4_COVERAGE_SPEC_SHA256",
    "ROUND4_DEVELOPMENT_ARTIFACT_SCOPE",
    "ROUND4_DEVELOPMENT_AUTHORIZATIONS",
    "ROUND4_FAILURE_ACCOUNTING_SPEC",
    "ROUND4_FAILURE_ACCOUNTING_SPEC_SHA256",
    "ROUND4_FAILURE_CLASSES",
    "ROUND4_LINE_INVENTORY_SCOPE_RULE",
    "ROUND4_PRODUCTION_ARTIFACT_SCOPE",
    "ROUND4_PRODUCTION_AUTHORITY",
    "ROUND4_PRODUCTION_AUTHORIZATIONS",
    "ROUND4_PRODUCTION_BINDING",
    "ROUND4_PRODUCTION_BINDING_SHA256",
    "ROUND4_PRODUCTION_VERSION",
    "ROUND4_RECORD_PROJECTION_SPEC",
    "ROUND4_RECORD_PROJECTION_SPEC_SHA256",
    "ROUND4_STAGE_A_SCHEMA_CATALOG_SHA256",
    "ROUND4_STAGE_B_SCHEMA_CATALOG_SHA256",
    "STAGE_A",
    "STAGE_B",
    "computed_round4_production_contract",
    "get_round4_production_contract",
    "validate_scope_authorizations",
]
