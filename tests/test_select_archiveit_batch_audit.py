import pandas as pd

from scripts.select_archiveit_batch_audit import select_audit


def _existing():
    return pd.DataFrame([
        {"candidate_year_id": "covered__2020", "claim_origin": "legacy_baseline", "strict_custom_domain": False},
    ])


def _review():
    rows = []
    for index in range(5):
        rows.append({
            "claim_id": f"a{index}", "candidate_year_id": f"new{index}__2020",
            "evidence_grade": "A", "archive_validation_reason": "grade_a_exact_url_election_period_content_confirmed",
            "review_status": "pending", "collection_id": 100 + index,
        })
    rows.extend([
        {"claim_id": "amb", "candidate_year_id": "amb__2020", "evidence_grade": "C", "archive_validation_reason": "mayoral_campaign_not_confirmed", "review_status": "pending", "collection_id": 200},
        {"claim_id": "neg", "candidate_year_id": "neg__2020", "evidence_grade": "C", "archive_validation_reason": "no_collection_capture", "review_status": "pending", "collection_id": 201},
        {"claim_id": "covered", "candidate_year_id": "covered__2020", "evidence_grade": "A", "archive_validation_reason": "grade_a_exact_url_election_period_content_confirmed", "review_status": "pending", "collection_id": 202},
    ])
    return pd.DataFrame(rows)


def test_select_audit_samples_marginal_positives_all_ambiguities_and_negative():
    selected, manifest = select_audit(_review(), _existing(), positive_audit_n=3)
    assert len(selected) == 5
    assert manifest["sampled_marginal_grade_a"] == 3
    assert manifest["ambiguous_campaign_content"] == 1
    assert manifest["sampled_marginal_non_strict"] == 1
    assert manifest["marginal_grade_a_candidate_years"] == 5
    assert "covered" not in set(selected.claim_id)


def test_select_audit_is_deterministic_under_input_reordering():
    first, first_manifest = select_audit(_review(), _existing(), seed=20260825)
    second, second_manifest = select_audit(
        _review().sample(frac=1, random_state=8), _existing(), seed=20260825,
    )
    assert first.claim_id.tolist() == second.claim_id.tolist()
    assert first_manifest["selected_claim_ids"] == second_manifest["selected_claim_ids"]


def test_select_audit_accepts_explicit_prebatch_coverage_roster():
    roster = pd.DataFrame([{"candidate_year_id": "covered__2020", "covered": True}])
    selected, manifest = select_audit(_review(), roster, positive_audit_n=3)
    assert manifest["marginal_candidate_years"] == 7
    assert "covered" not in set(selected.claim_id)
