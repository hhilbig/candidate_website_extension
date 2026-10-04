import copy

import pytest

from scripts import diagnose_candidate_endorsement_codex_review as d


def row(start, end, status="accepted_by_ai_review", kind="individual"):
    return {"entity_type": kind, "name_spans": [[start, end]], "support_spans": [[0, 20]],
            "endorsement_status": status, "ai_reviewed": True, "human_reviewed": False}


def test_uncertain_predictions_are_not_double_counted():
    source = "Ann Ann Bob         "
    references = [row(0, 3), row(4, 7, "uncertain")]
    predictions = [row(0, 3), row(4, 7), row(8, 11)]
    x = d.multiset_comparison(source, predictions, references, coordinates=False)
    assert x["prediction_matches_ai_accepted"] == 1
    assert x["prediction_matches_ai_uncertain"] == 1
    assert x["prediction_without_ai_reference"] == 1


def test_coordinates_and_normalized_multiset_differ():
    source = "Ann Ann             "
    exact = d.multiset_comparison(source, [row(0, 3)], [row(4, 7)], coordinates=True)
    normalized = d.multiset_comparison(source, [row(0, 3)], [row(4, 7)], coordinates=False)
    assert exact["prediction_matches_ai_accepted"] == 0
    assert normalized["prediction_matches_ai_accepted"] == 1


def test_empty_denominators_are_unavailable():
    x = d.multiset_comparison("", [], [], coordinates=True)
    assert x["accepted_reference_match_rate"] is None
    assert x["prediction_match_rate_all_provisional"] is None


def test_boundary_type_and_unresolved_relation():
    source = "Ann Smith           "
    x = d.disagreement(source, row(0, 9), [row(4, 9)], [{"unresolved_block_id": "b", "block_span": [0, 10]}])
    assert x["comparison_relation"] == "overlapping_name_boundaries_or_compound_identity"
    assert x["overlapping_unresolved_extraction_block_ids"] == ["b"]
    x = d.disagreement(source, row(0, 9), [row(0, 9, kind="organization")], [])
    assert x["comparison_relation"] == "same_coordinates_different_type"


def fixtures():
    source = "Ann Ann Bob         "
    source_hash = d.review.digest(source.encode())
    parent = {"parent_document_id": "p", "parent_source_text": source, "source_text_sha256": source_hash,
              "metadata": {"year": 2024}, "ai_children": [row(0, 3), row(8, 11)], "ai_unresolved_blocks": []}
    response = {"parent_document_id": "p", "source_text_sha256": source_hash, "metadata": parent["metadata"],
                "complete_source_ai_reviewed": True, "human_reviewed": False,
                "children": [row(0, 3), row(4, 7, "uncertain")], "uncertainties": []}
    reference = {"parent_count": 1, "source_task_sha256": "hash", "review_status": "ai_reviewed",
                 "human_reviewed": False, "authorizations": {"release": False}, "responses": [response],
                 "counts": {"reviewed_children": 2, "accepted_children": 1, "uncertain_children": 1}}
    return {"parents": [parent]}, reference


def test_complete_partition_and_source_preservation():
    task, reference = fixtures()
    original = copy.deepcopy(task)
    report, ledger = d.build_diagnostic(task, reference, "hash")
    assert report["human_reviewed"] is False
    assert report["reference_is_ai_not_human_truth"] is True
    assert report["comparison"]["exact_name_coordinates_and_type"]["prediction_without_ai_reference"] == 1
    assert len(ledger["parents"][0]["reference_only"]) == 1
    assert task == original


def test_aggregate_with_empty_parent():
    task, reference = fixtures()
    task["parents"].append({**copy.deepcopy(task["parents"][0]), "parent_document_id": "zero", "ai_children": []})
    reference["responses"].append({**copy.deepcopy(reference["responses"][0]), "parent_document_id": "zero", "children": []})
    reference["parent_count"] = 2
    report, _ = d.build_diagnostic(task, reference, "hash")
    assert report["comparison"]["exact_name_coordinates_and_type"]["prediction_count"] == 2


@pytest.mark.parametrize("change", ["human", "hash", "closure", "duplicate", "count"])
def test_invalid_reference_fails(change):
    task, reference = fixtures()
    if change == "human":
        reference["responses"][0]["children"][0]["human_reviewed"] = True
    elif change == "hash":
        reference["responses"][0]["source_text_sha256"] = "bad"
    elif change == "closure":
        reference["responses"] = []
    elif change == "duplicate":
        reference["responses"][0]["children"] *= 2
    else:
        reference["counts"]["reviewed_children"] = 9
    with pytest.raises(ValueError):
        d.build_diagnostic(task, reference, "hash")


def unconfirmed_task():
    task, _ = fixtures()
    parent = task["parents"][0]
    parent.update(source_n_chars=len(parent["parent_source_text"]), human_parent_confirmed=False)
    for child in parent["ai_children"]:
        child["human_confirmed"] = False
    return task


@pytest.mark.parametrize("field", ["parent_source_text", "source_text_sha256", "metadata",
                                    "parent_document_id", "source_n_chars"])
def test_paired_run_cannot_change_source(field):
    original = unconfirmed_task()
    candidate = copy.deepcopy(original)
    candidate["parents"][0][field] = "changed"
    with pytest.raises(ValueError, match="Rerun source differs"):
        d.require_same_sources(original, candidate)


def test_paired_run_preserves_unresolved_blocks_and_no_human_labels():
    original = unconfirmed_task()
    candidate = copy.deepcopy(original)
    candidate["parents"][0]["ai_unresolved_blocks"] = [{"human_reviewed": False, "block_span": [0, 3]}]
    d.require_same_sources(original, candidate)
    candidate["parents"][0]["ai_unresolved_blocks"][0]["human_reviewed"] = True
    with pytest.raises(ValueError, match="block has human confirmation"):
        d.require_same_sources(original, candidate)


def test_paired_run_rejects_closure_and_confirmed_child():
    original = unconfirmed_task()
    candidate = copy.deepcopy(original)
    candidate["parents"] = []
    with pytest.raises(ValueError, match="closure"):
        d.require_same_sources(original, candidate)
    candidate = copy.deepcopy(original)
    candidate["parents"][0]["ai_children"][0]["human_confirmed"] = True
    with pytest.raises(ValueError, match="child has human confirmation"):
        d.require_same_sources(original, candidate)


def test_paired_counts_distinguish_uncertain_and_unmatched():
    task, reference = fixtures()
    baseline, _ = d.build_diagnostic(task, reference, "hash")
    task["parents"][0]["ai_children"] = [row(0, 3), row(4, 7)]
    task["parents"][0]["ai_unresolved_blocks"] = [{"unresolved_block_id": "b", "block_span": [8, 11]}]
    candidate, _ = d.build_diagnostic(task, reference, "hash")
    paired = d.paired_change_summary(baseline, candidate)
    delta = paired["candidate_minus_baseline_counts"]["exact_name_coordinates_and_type"]
    assert delta["prediction_matches_ai_accepted"] == 0
    assert delta["prediction_matches_ai_uncertain"] == 1
    assert delta["prediction_without_ai_reference"] == -1
    assert paired["candidate_unresolved_blocks"] == 1
    assert paired["human_reviewed"] is False
    candidate["comparison"]["exact_name_coordinates_and_type"]["ai_accepted_reference_count"] += 1
    with pytest.raises(ValueError, match="denominator"):
        d.paired_change_summary(baseline, candidate)


def test_run_comparison_requires_pinned_inventory_before_reading():
    with pytest.raises(ValueError, match="inventory hash"):
        d.load_run_task({}, None, None, "hash", "not-a-hash")
