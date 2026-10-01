import pandas as pd

from scripts.build_mayoral_prebatch_coverage_roster import build_roster


def test_roster_combines_direct_and_inferred_coverage_once():
    claims = pd.DataFrame([
        {"candidate_year_id": "a", "claim_origin": "legacy_baseline", "strict_custom_domain": False},
        {"candidate_year_id": "b", "claim_origin": "adapter", "strict_custom_domain": True},
        {"candidate_year_id": "c", "claim_origin": "adapter", "strict_custom_domain": False},
    ])
    dispositions = pd.DataFrame([
        {"candidate_year_id": "b", "strict_under_batch_audit_rule": True, "batch_disposition": "human_accepted"},
        {"candidate_year_id": "c", "strict_under_batch_audit_rule": True, "batch_disposition": "accepted_under_sampled_audit_rule"},
    ])
    roster = build_roster(claims, dispositions)
    assert roster["candidate_year_id"].tolist() == ["a", "b", "c"]
    assert roster.set_index("candidate_year_id").loc["c", "coverage_basis"] == "sampled_audit_rule_inferred"
    assert roster["covered"].all()
