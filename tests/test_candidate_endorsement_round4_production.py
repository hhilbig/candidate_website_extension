"""Tests for the round-four production binding and its frozen contract."""
from __future__ import annotations

import pytest

from src.candidate_endorsement_round4 import (
    ROUND4_DEVELOPMENT_BINDING,
    ROUND4_DEVELOPMENT_BINDING_SHA256,
    ROUND4_STAGE_A_PROMPT_SHA256,
    ROUND4_STAGE_B_PROMPT_SHA256,
)
from src.candidate_endorsement_round4_production import (
    FROZEN_ROUND4_DEVELOPMENT_BINDING_SHA256,
    FROZEN_ROUND4_PRODUCTION_CONTRACT_SHA256,
    ROUND4_COVERAGE_SPEC,
    ROUND4_DEVELOPMENT_ARTIFACT_SCOPE,
    ROUND4_DEVELOPMENT_AUTHORIZATIONS,
    ROUND4_FAILURE_ACCOUNTING_SPEC,
    ROUND4_FAILURE_CLASSES,
    ROUND4_PRODUCTION_ARTIFACT_SCOPE,
    ROUND4_PRODUCTION_AUTHORITY,
    ROUND4_PRODUCTION_AUTHORIZATIONS,
    ROUND4_PRODUCTION_BINDING,
    STAGE_A,
    STAGE_B,
    computed_round4_production_contract,
    get_round4_production_contract,
    validate_scope_authorizations,
)


def test_development_binding_is_unchanged():
    """The production work must never alter the development artifacts."""
    assert (
        ROUND4_DEVELOPMENT_BINDING_SHA256
        == FROZEN_ROUND4_DEVELOPMENT_BINDING_SHA256
    )
    assert ROUND4_DEVELOPMENT_BINDING["production_integration_authorized"] is False
    assert ROUND4_DEVELOPMENT_BINDING["release_authorized"] is False


def test_production_reuses_the_reviewed_round4_prompts():
    """Production must run the exact prompts the development review covered."""
    assert (
        ROUND4_STAGE_A_PROMPT_SHA256
        == "35a8a6cc907568fa6e4d7e19ec0e92cd2e926f1cec7e7fd99a027e1637549546"
    )
    assert (
        ROUND4_STAGE_B_PROMPT_SHA256
        == "bb51ffe6cd0c0f5d026cfb4e2f2448e8f98d4447613a724d40a7d67a034f56a4"
    )
    assert (
        ROUND4_PRODUCTION_BINDING["stage_a_prompt_sha256"]
        == ROUND4_STAGE_A_PROMPT_SHA256
    )
    assert (
        ROUND4_PRODUCTION_BINDING["stage_b_prompt_sha256"]
        == ROUND4_STAGE_B_PROMPT_SHA256
    )


def test_production_binding_authorizes_integration_but_not_release():
    binding = ROUND4_PRODUCTION_BINDING
    assert binding["production_integration_authorized"] is True
    assert binding["full_expansion_authorized"] is True
    assert binding["release_authorized"] is False
    assert binding["human_truth_established"] is False
    assert binding["development_labels_authoritative"] is False
    assert binding["authority"] == ROUND4_PRODUCTION_AUTHORITY


def test_production_contract_matches_the_frozen_hash():
    contract = get_round4_production_contract()
    assert (
        contract["round4_production_contract_sha256"]
        == FROZEN_ROUND4_PRODUCTION_CONTRACT_SHA256
    )
    assert contract["prompt_sha256"][STAGE_A] == ROUND4_STAGE_A_PROMPT_SHA256
    assert contract["prompt_sha256"][STAGE_B] == ROUND4_STAGE_B_PROMPT_SHA256
    assert contract["base_schema_version"] == "2.1"


def test_production_contract_is_deterministic():
    first = computed_round4_production_contract()
    second = computed_round4_production_contract()
    assert first == second


def test_get_contract_fails_closed_on_contract_drift(monkeypatch):
    monkeypatch.setattr(
        "src.candidate_endorsement_round4_production."
        "FROZEN_ROUND4_PRODUCTION_CONTRACT_SHA256",
        "0" * 64,
    )
    with pytest.raises(RuntimeError, match="differs from the frozen hash"):
        get_round4_production_contract()


def test_failure_accounting_defines_exactly_five_classes():
    assert len(ROUND4_FAILURE_CLASSES) == 5
    assert ROUND4_FAILURE_ACCOUNTING_SPEC["classes"] == list(
        ROUND4_FAILURE_CLASSES
    )
    for name in ROUND4_FAILURE_CLASSES:
        assert name in ROUND4_FAILURE_ACCOUNTING_SPEC


def test_deterministic_failures_are_never_reinferred():
    """Temperature zero with a fixed seed reproduces an identical output."""
    spec = ROUND4_FAILURE_ACCOUNTING_SPEC
    assert spec["optional_field_repaired"]["reinference_allowed"] is False
    assert spec["unresolved_target_block"]["reinference_allowed"] is False
    assert spec["infrastructure_failure"]["reinference_allowed"] is True
    assert spec["unlocalizable_schema_failure"]["reinference_allowed"] is True


def test_an_unresolved_block_is_never_a_measured_zero():
    spec = ROUND4_FAILURE_ACCOUNTING_SPEC
    assert spec["unresolved_target_block"]["measured_zero_allowed"] is False
    assert "never_a_measured_zero" in spec


def test_retry_rule_keeps_the_two_attempt_limit():
    retry = ROUND4_FAILURE_ACCOUNTING_SPEC["retry_rule"]
    assert retry["maximum_attempts_per_batch"] == 2
    assert retry["identical_prompt_schema_seed_and_model"] is True
    assert retry["only_input_manifest_sha256_differs"] is True


def test_coverage_spec_records_the_corpus_gate_fields():
    assert ROUND4_COVERAGE_SPEC["corpus_gate_fields"] == [
        "all_model_records_accounted",
        "unaccounted_outputs",
        "unresolved_infrastructure_failures",
    ]
    assert ROUND4_COVERAGE_SPEC["corpus_gate_authority"] == (
        ROUND4_PRODUCTION_AUTHORITY
    )


def test_scope_authorization_pairs_are_fail_closed():
    validate_scope_authorizations(
        ROUND4_PRODUCTION_ARTIFACT_SCOPE, dict(ROUND4_PRODUCTION_AUTHORIZATIONS)
    )
    validate_scope_authorizations(
        ROUND4_DEVELOPMENT_ARTIFACT_SCOPE,
        dict(ROUND4_DEVELOPMENT_AUTHORIZATIONS),
    )
    with pytest.raises(ValueError, match="unknown round-four artifact scope"):
        validate_scope_authorizations("something_else", {})


def test_development_constants_match_the_completed_runs():
    """The replay reads real development outputs, so these must match them."""
    assert ROUND4_DEVELOPMENT_ARTIFACT_SCOPE == (
        "candidate_endorsement_round4_development_only"
    )
    assert ROUND4_DEVELOPMENT_AUTHORIZATIONS == {
        "bounded_development_inference": True,
        "development_labels_authoritative": False,
        "human_truth": False,
        "production_integration": False,
        "full_expansion": False,
        "release": False,
    }
    assert set(ROUND4_PRODUCTION_AUTHORIZATIONS) == set(
        ROUND4_DEVELOPMENT_AUTHORIZATIONS
    )


def test_a_mixed_scope_and_authorization_pair_is_rejected():
    """A development artifact must never carry production authority."""
    with pytest.raises(ValueError, match="do not match the artifact scope"):
        validate_scope_authorizations(
            ROUND4_DEVELOPMENT_ARTIFACT_SCOPE,
            dict(ROUND4_PRODUCTION_AUTHORIZATIONS),
        )
    release_claim = dict(ROUND4_PRODUCTION_AUTHORIZATIONS)
    release_claim["release"] = True
    with pytest.raises(ValueError, match="do not match the artifact scope"):
        validate_scope_authorizations(
            ROUND4_PRODUCTION_ARTIFACT_SCOPE, release_claim
        )
