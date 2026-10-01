import pandas as pd

from scripts.apply_archiveit_batch_audit_rule import apply_rule


def test_rule_distinguishes_direct_approval_from_sampled_inference():
    evidence = pd.DataFrame([
        {"claim_id": "a", "candidate_year_id": "1__2020", "collection_id": 1},
        {"claim_id": "b", "candidate_year_id": "2__2020", "collection_id": 2},
    ])
    validation = pd.DataFrame([
        {"claim_id": "a", "proposed_evidence_grade": "A", "strict_coverage_eligible": True, "validation_reason": "grade_a"},
        {"claim_id": "b", "proposed_evidence_grade": "A", "strict_coverage_eligible": True, "validation_reason": "grade_a"},
    ])
    adjudication = pd.DataFrame([{
        "claim_id": "a", "audit_inclusion_reason": "sampled_marginal_grade_a",
        "accepted_claim": True, "strict_identity": True, "strict_temporal": True,
        "strict_custom_domain": True, "coded_evidence_grade": "A",
        "coded_url_type": "custom_domain",
    }])
    manifest = {"audit_items": 1, "sampled_marginal_grade_a": 1, "selected_claim_ids": ["a"]}
    existing = pd.DataFrame(columns=["claim_origin", "strict_custom_domain", "candidate_year_id"])
    result, summary = apply_rule(evidence, validation, adjudication, manifest, existing)
    dispositions = result.set_index("claim_id")["batch_disposition"].to_dict()
    assert dispositions == {
        "a": "human_accepted",
        "b": "accepted_under_sampled_audit_rule",
    }
    assert summary["directly_human_accepted_strict_candidate_years"] == 1
    assert summary["inferred_strict_candidate_years"] == 1
    assert summary["marginal_strict_candidate_years"] == 2


def test_rule_handles_direct_ambiguity_review_without_sampled_positives():
    evidence = pd.DataFrame([{
        "claim_id": "a", "candidate_year_id": "1__2020", "collection_id": 1,
    }])
    validation = pd.DataFrame([{
        "claim_id": "a", "proposed_evidence_grade": "C",
        "strict_coverage_eligible": False,
        "validation_reason": "mayoral_campaign_not_confirmed",
    }])
    adjudication = pd.DataFrame([{
        "claim_id": "a", "audit_inclusion_reason": "ambiguous_campaign_content",
        "accepted_claim": True, "strict_identity": True, "strict_temporal": True,
        "strict_custom_domain": True, "coded_evidence_grade": "A",
        "coded_url_type": "custom_domain",
    }])
    manifest = {
        "audit_items": 1, "sampled_marginal_grade_a": 0,
        "selected_claim_ids": ["a"],
    }
    roster = pd.DataFrame([{"candidate_year_id": "covered__2020", "covered": True}])
    result, summary = apply_rule(evidence, validation, adjudication, manifest, roster)
    assert result.iloc[0]["batch_disposition"] == "human_accepted"
    assert result.iloc[0]["strict_under_batch_audit_rule"]
    assert summary["sampled_positive_precision"] is None
    assert summary["sampled_strict_confirmation_rate"] is None
    assert not summary["extrapolation_gate_passed"]
    assert summary["marginal_strict_candidate_years"] == 1
