import pandas as pd
import pytest

from scripts.evaluate_archiveit_collection_batch import evaluate_batch


def _inputs():
    config = {
        "batch_id": "batch",
        "universe_candidate_years": 100,
        "covered_candidate_years_before": 10,
        "collections": [
        {"collection_id": i, "collection_name": f"c{i}", "priority_tier": "test", "selection_reason": "test", "uncovered_candidates_before": 1}
        for i in range(1, 21)
    ]}
    evidence = pd.DataFrame([
        {
            "claim_id": f"q{i}",
            "candidate_year_id": f"new{i}__2020",
            "collection_id": i,
            "strict_coverage_eligible": False,
            "validation_reason": "adapter_pending_validation",
        }
        for i in range(1, 7)
    ])
    attempts = pd.DataFrame([
        {"source_id": f"s{i}", "contest_id": f"contest{i}", "collection_id": i, "source_status": "found" if i <= 6 else "no_result", "mayoral_seeds_considered": 1, "candidate_matches": int(i <= 6), "eligible_url_leads": int(i <= 6)}
        for i in range(1, 21)
    ])
    validation = pd.DataFrame([
        {"claim_id": f"q{i}", "proposed_evidence_grade": "A", "strict_coverage_eligible": True, "validation_reason": "grade_a"}
        for i in range(1, 7)
    ])
    existing = pd.DataFrame([
        {"candidate_year_id": "old__2020", "claim_origin": "legacy_baseline", "strict_custom_domain": False}
    ])
    audit_manifest = {
        "audit_items": 3,
        "sampled_marginal_grade_a": 3,
        "ambiguous_campaign_content": 0,
        "sampled_marginal_non_strict": 0,
        "selected_claim_ids": ["q1", "q2", "q3"],
    }
    return config, evidence, attempts, validation, existing, audit_manifest


def test_pending_audit_reports_six_marginal_proposals_without_final_gate():
    summary, collections = evaluate_batch(*_inputs())
    assert summary["marginal_proposed_strict_candidate_years"] == 6
    assert summary["decision"] == "pending_sampled_human_audit"
    assert summary["serper_requests"] == 0
    assert len(collections) == 20


def test_perfect_audit_puts_six_additions_in_one_more_batch_band():
    adjudication = pd.DataFrame([
        {
            "claim_id": f"q{i}",
            "audit_inclusion_reason": "sampled_marginal_grade_a",
            "candidate_year_id": f"new{i}__2020",
            "accepted_claim": True,
            "strict_custom_domain": True,
        }
        for i in range(1, 4)
    ])
    summary, _ = evaluate_batch(*_inputs(), audit_adjudication=adjudication)
    assert summary["audited_positive_precision"] == 1.0
    assert summary["sampled_strict_confirmation_rate"] == 1.0
    assert summary["final_marginal_strict_candidate_years"] == 6
    assert summary["covered_candidate_years_after_under_audit_rule"] == 16
    assert summary["decision"] == "run_one_more_small_archiveit_batch"


def test_incomplete_audit_is_rejected():
    adjudication = pd.DataFrame([{
        "claim_id": "q1",
        "audit_inclusion_reason": "sampled_marginal_grade_a",
        "candidate_year_id": "new1__2020",
        "accepted_claim": True,
        "strict_custom_domain": True,
    }])
    with pytest.raises(ValueError, match="row count"):
        evaluate_batch(*_inputs(), audit_adjudication=adjudication)


def test_explicit_coverage_roster_is_accepted_as_prebatch_input():
    inputs = list(_inputs())
    inputs[4] = pd.DataFrame([{"candidate_year_id": "new1__2020", "covered": True}])
    summary, _ = evaluate_batch(*inputs)
    assert summary["marginal_proposed_strict_candidate_years"] == 5
