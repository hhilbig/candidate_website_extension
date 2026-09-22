from __future__ import annotations

from collections import Counter
from pathlib import Path
import subprocess
import sys

import pytest

from scripts.evaluate_candidate_endorsement_round4_regression import (
    compare_parent_counters,
    predicted_parent_counters,
    provisional_parent_counters,
)


def test_provisional_span_projection_preserves_punctuation_and_multiplicity() -> None:
    source = "A.B. Smith and A.B. Smith\n"
    draft = {
        "responses": [{
            "parent_document_id": "parent",
            "true_zero": False,
            "children": [
                {"entity_type": "individual", "name_spans": [[0, 10]]},
                {"entity_type": "individual", "name_spans": [[15, 25]]},
            ],
        }]
    }

    counters = provisional_parent_counters(draft, {"parent": source})

    assert counters["parent"] == Counter({("a.b. smith", "individual"): 2})


def test_provisional_true_zero_must_equal_empty_children() -> None:
    draft = {
        "responses": [{
            "parent_document_id": "parent",
            "true_zero": True,
            "children": [{"entity_type": "individual", "name_spans": [[0, 5]]}],
        }]
    }

    with pytest.raises(ValueError, match="true-zero"):
        provisional_parent_counters(draft, {"parent": "Alice"})


def test_predictions_use_only_technical_pass_batches_and_target_slots() -> None:
    batches = [
        {
            "batch_id": "a" * 64,
            "parent_document_id": "p1",
            "target_slots": ["slot_00"],
        },
        {
            "batch_id": "b" * 64,
            "parent_document_id": "p2",
            "target_slots": ["slot_00"],
        },
    ]
    outputs = [
        {
            "batch_id": "a" * 64,
            "technical_pass": True,
            "model_output": {
                "slot_00": {
                    "disposition": "records",
                    "records": [{
                        "name_parts": ["Steve", "Nelson"],
                        "entity_type": "individual",
                    }],
                },
                "slot_01": {"disposition": "padding", "records": []},
            },
        },
        {"batch_id": "b" * 64, "technical_pass": False},
    ]

    predicted, dispositions, failures, repairs = predicted_parent_counters(
        batches, outputs
    )

    assert predicted["p1"] == Counter({("steve nelson", "individual"): 1})
    assert predicted["p2"] == Counter()
    assert dispositions == Counter({"records": 1})
    assert failures == ["b" * 64]
    assert repairs == []


def test_predictions_can_retain_parents_without_stage_b_batches() -> None:
    batch = {
        "batch_id": "a" * 64,
        "parent_document_id": "p1",
        "target_slots": ["slot_00"],
    }
    output = {
        "batch_id": "a" * 64,
        "technical_pass": True,
        "model_output": {
            "slot_00": {"disposition": "not_endorsement", "records": []},
        },
    }

    predicted, _dispositions, failures, _repairs = predicted_parent_counters(
        [batch], [output], parent_ids={"p1", "p2"}
    )

    assert predicted == {"p1": Counter(), "p2": Counter()}
    assert failures == []


def test_predictions_accept_only_deterministic_optional_field_nulling() -> None:
    batch = {
        "batch_id": "a" * 64,
        "parent_document_id": "p1",
        "target_slots": ["slot_00"],
    }
    record = {
        "name_parts": ["Alex Rivera"],
        "entity_type": "individual",
    }
    output = {
        "batch_id": "a" * 64,
        "technical_pass": False,
        "model_output": {
            "slot_00": {"disposition": "records", "records": [record]},
        },
        "stage_validation": {
            "schema_valid": True,
            "coverage_complete": True,
            "source_projection_valid": False,
            "errors": [
                "$.slot_00.records[0].title: "
                "not an exact contiguous visible substring"
            ],
            "source_projection": {
                "slot_00": [{
                    "record_index": 0,
                    "optional_field_changes": [{
                        "field": "title",
                        "original_value": "Mayor",
                        "replacement_value": None,
                        "reason": "not_exact_contiguous_visible_substring",
                    }],
                }]
            },
        },
    }

    predicted, dispositions, failures, repairs = predicted_parent_counters(
        [batch], [output]
    )

    assert predicted["p1"] == Counter({("alex rivera", "individual"): 1})
    assert dispositions == Counter({"records": 1})
    assert failures == []
    assert repairs[0]["nulling_operations"][0]["changes"][0][
        "replacement_value"
    ] is None


def test_failed_batch_retains_independently_valid_sibling_slot() -> None:
    common = dict(entity_type="individual", endorser_category="private_individual", title=None, affiliation=None, jurisdiction=None)
    batch = {"batch_id": "a" * 64, "parent_document_id": "p1", "stage": "record_extraction",
             "target_slots": ["slot_00", "slot_01"], "padding_slots": ["slot_02", "slot_03"],
             "targets": [{"slot": slot, "record_block_lines": [{"line_text": text, "line_terminator": "\n"}]} for slot, text in
                         [("slot_00", "Richard Corral"), ("slot_01", "Darryl and Amy Cherness")]]}
    output = {"batch_id": batch["batch_id"], "technical_pass": False,
              "model_output": {"slot_00": {"disposition": "records", "source_mentions_fully_accounted": True,
                                          "records": [{"name_parts": ["Richard Corral"], **common}]},
                               "slot_01": {"disposition": "records", "source_mentions_fully_accounted": True,
                                          "records": [{"name_parts": ["Darryl Cherness"], **common}]},
                               **{s: {"disposition": "padding", "source_mentions_fully_accounted": True, "records": []}
                                  for s in batch["padding_slots"]}}}
    ledger = []
    predicted, dispositions, failures, repairs = predicted_parent_counters([batch], [output], slot_salvage_ledger=ledger)
    assert predicted["p1"] == Counter({("richard corral", "individual"): 1})
    assert failures == [batch["batch_id"]]
    assert repairs == []
    assert ledger[0]["accepted_target_slots"] == ["slot_00"]
    assert ledger[0]["unresolved_target_slots"] == ["slot_01"]
    assert ledger[0]["retained_record_count"] == 1


def test_counter_comparison_is_multiset_exact() -> None:
    reference = {
        "p1": Counter({("alice", "individual"): 2}),
        "p2": Counter(),
    }
    predicted = {
        "p1": Counter({("alice", "individual"): 1, ("alice", "organization"): 1}),
        "p2": Counter(),
    }

    metrics, disagreements = compare_parent_counters(reference, predicted)

    assert metrics == {
        "parent_count": 2,
        "parent_exact_multiset_count": 1,
        "parent_exact_multiset_rate": 0.5,
        "reference_record_count": 2,
        "predicted_record_count": 2,
        "matched_name_type_count": 1,
        "reference_only_count": 1,
        "predicted_only_count": 1,
        "reference_recall": 0.5,
        "predicted_precision": 0.5,
        "disagreement_parent_count": 1,
    }
    assert disagreements[0]["parent_document_id"] == "p1"


def test_cli_resolves_repo_imports_outside_repo_working_directory(
    tmp_path: Path,
) -> None:
    script = (
        Path(__file__).resolve().parents[1]
        / "scripts/evaluate_candidate_endorsement_round4_regression.py"
    )
    result = subprocess.run(
        [
            sys.executable,
            str(script),
            "--input-dir",
            str(tmp_path / "missing-input"),
            "--output-dir",
            str(tmp_path / "missing-output"),
            "--expected-input-manifest-sha256",
            "0" * 64,
            "--report",
            str(tmp_path / "report.json"),
            "--ledger",
            str(tmp_path / "ledger.json"),
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode != 0
    assert "ModuleNotFoundError" not in result.stderr
    assert "chunks directory does not exist" in result.stderr
