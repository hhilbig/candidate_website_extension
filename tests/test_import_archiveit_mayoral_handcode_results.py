import pandas as pd
import pytest

from scripts.import_archiveit_mayoral_handcode_results import (
    TASK_ID,
    build_adjudicated_claims,
    import_results,
    result_summary,
)


def _response(item_id, url_type, grade="A", corrected_url=""):
    answers = {
        "identity_match": "yes",
        "temporal_match": "yes",
        "accept_url": "yes",
        "url_type": url_type,
        "evidence_grade": grade,
    }
    if corrected_url:
        answers["corrected_url"] = corrected_url
    return {"item_id": item_id, "status": "complete", "answers": answers}


def test_import_results_marks_strict_outcomes_and_uses_correction():
    items = pd.DataFrame([
        {"item_id": "a", "candidate_year_id": "1__2001", "candidate_url": "http://wrapper/a"},
        {"item_id": "b", "candidate_year_id": "2__2001", "candidate_url": "http://shared/b"},
    ])
    results = {
        "task_id": TASK_ID,
        "format": "handcode-results-v1",
        "responses": [
            _response("a", "custom_domain", corrected_url="http://Candidate.example/Path"),
            _response("b", "shared_host_path", grade="C"),
        ],
    }
    imported = import_results(results, items)

    assert imported["accepted_url"].tolist() == [
        "http://Candidate.example/Path",
        "http://shared/b",
    ]
    assert imported["strict_exact_url"].tolist() == [True, False]
    assert imported["strict_custom_domain"].tolist() == [True, False]
    assert result_summary(imported)["strict_custom_domain_candidate_years"] == 1
    claims = build_adjudicated_claims(imported)
    assert claims["exact_normalized_url"].tolist() == [
        "http://Candidate.example/Path",
        "http://shared/b",
    ]
    assert claims["review_status"].tolist() == ["accepted", "accepted"]
    assert claims["acceptance"].tolist() == [True, True]
    assert claims["evidence_grade"].tolist() == ["A", "C"]


def test_import_results_refuses_incomplete_export():
    items = pd.DataFrame([
        {"item_id": "a", "candidate_year_id": "1__2001", "candidate_url": "http://a"}
    ])
    results = {
        "task_id": TASK_ID,
        "format": "handcode-results-v1",
        "responses": [{"item_id": "a", "status": "partial", "answers": {}}],
    }
    with pytest.raises(ValueError, match="Incomplete response"):
        import_results(results, items)


def test_import_results_accepts_an_explicit_batch_task_id():
    items = pd.DataFrame([
        {"item_id": "a", "candidate_year_id": "1__2001", "candidate_url": "http://a"}
    ])
    results = {
        "task_id": "archiveit_mayoral_batch20_audit_20260825",
        "format": "handcode-results-v1",
        "responses": [_response("a", "custom_domain")],
    }
    imported = import_results(
        results, items, expected_task_id="archiveit_mayoral_batch20_audit_20260825",
    )
    assert imported.iloc[0].strict_custom_domain


def test_rejected_handcode_grade_maps_to_schema_grade_d():
    items = pd.DataFrame([{
        "item_id": "a", "candidate_year_id": "1__2001", "candidate_url": "http://a"
    }])
    results = {
        "task_id": TASK_ID,
        "format": "handcode-results-v1",
        "responses": [{
            "item_id": "a",
            "status": "complete",
            "answers": {
                "identity_match": "no",
                "temporal_match": "no",
                "accept_url": "no",
                "url_type": "not_campaign_url",
                "evidence_grade": "reject",
            },
        }],
    }
    claims = build_adjudicated_claims(import_results(results, items))
    assert claims.iloc[0]["evidence_grade"] == "D"
    assert claims.iloc[0]["coded_evidence_grade"] == "reject"
    assert claims.iloc[0]["review_status"] == "rejected"
