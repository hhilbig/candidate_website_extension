from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.diagnose_candidate_endorsement_round3_ai_first_cut import (
    REPO,
    _write_new,
    build_diagnostic,
    child_is_in_target,
    sha256_json,
)


def test_real_provisional_diagnostic_census_and_binding() -> None:
    report, ledger = build_diagnostic(REPO)

    assert report["census"] == {
        "parent_count": 40,
        "stage_b_batch_count": 298,
        "stage_b_anchor_count": 2_221,
        "provisional_child_count": 2_225,
        "model_record_count": 2_225,
        "technical_pass_batch_count": 245,
        "technical_fail_batch_count": 53,
        "technical_failure_parent_count": 16,
        "exact_name_and_type_match_count": 2_183,
        "exact_name_and_type_mismatch_count": 42,
        "exact_name_and_type_match_percent": 98.11236,
        "block_local_exact_name_and_type_match_count": 2_175,
        "block_local_exact_name_and_type_mismatch_count": 50,
        "block_local_exact_name_and_type_match_percent": 97.752809,
        "exact_anchor_multiset_match_count": 2_165,
        "exact_anchor_multiset_mismatch_count": 56,
        "parent_with_any_exact_multiset_disagreement_count": 18,
        "false_anchor_against_provisional_labels_count": 19,
        "multi_child_anchor_against_provisional_labels_count": 10,
        "unanchored_provisional_child_count": 12,
    }
    assert report["anchor_topology_counts"] == {
        "truth_0_prediction_1": 19,
        "truth_1_prediction_1": 2_192,
        "truth_2_prediction_1": 5,
        "truth_2_prediction_2": 4,
        "truth_3_prediction_1": 1,
    }
    assert len(ledger["disagreements"]) == 56
    assert len(ledger["unanchored_provisional_children"]) == 12
    assert report["private_disagreement_ledger_sha256"] == sha256_json(ledger)


def test_aggregate_report_is_nonidentifying_and_does_not_overauthorize() -> None:
    report, _ledger = build_diagnostic(REPO)
    serialized = json.dumps(report, sort_keys=True)

    assert "parent_document_id" not in serialized
    assert "canonical_name" not in serialized
    assert "model_records" not in serialized
    assert report["authorizations"] == {
        "prompt_and_rule_iteration": True,
        "bounded_fresh_validation_sample": True,
        "human_truth": False,
        "accuracy_claim": False,
        "production_integration": False,
        "full_expansion": False,
        "release": False,
    }


def test_child_must_be_fully_contained_in_record_block() -> None:
    target = {
        "record_block_lines": [
            {"parent_start_char": 10, "parent_end_char": 20},
            {"parent_start_char": 30, "parent_end_char": 40},
        ]
    }

    assert child_is_in_target({"name_spans": [[10, 15], [30, 35]]}, target)
    assert not child_is_in_target({"name_spans": [[10, 15], [25, 35]]}, target)


def test_write_new_refuses_to_overwrite(tmp_path: Path) -> None:
    path = tmp_path / "artifact.json"
    _write_new(path, {"first": True})

    with pytest.raises(FileExistsError, match="refusing to overwrite"):
        _write_new(path, {"second": True})
