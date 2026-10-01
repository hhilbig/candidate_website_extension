import pandas as pd
import pytest

from scripts.import_mayoral_url_handcode_results import (
    build_adjudicated_claims,
    import_results,
    pilot_metrics,
)


def test_import_results_marks_only_strict_custom_domain():
    items = pd.DataFrame([
        {
            "item_id": "a", "candidate_year_id": "1__2001",
            "candidate_url": "http://Candidate.example/Path",
            "evidence_source_url": "https://source.example/a",
        },
        {
            "item_id": "b", "candidate_year_id": "2__2019",
            "candidate_url": "http://shared.example/b",
            "evidence_source_url": "https://source.example/b",
        },
    ])
    results = {
        "format": "handcode-results-v1",
        "responses": [
            {
                "item_id": "a", "status": "complete",
                "answers": {
                    "identity_match": "yes", "temporal_match": "yes", "accept_url": "yes",
                    "url_type": "custom_domain", "evidence_grade": "A",
                },
            },
            {
                "item_id": "b", "status": "complete",
                "answers": {
                    "identity_match": "yes", "temporal_match": "yes", "accept_url": "yes",
                    "url_type": "shared_host_path", "evidence_grade": "B",
                },
            },
        ],
    }
    imported = import_results(results, items)
    assert imported["strict_exact_url"].tolist() == [True, True]
    assert imported["strict_custom_domain"].tolist() == [True, False]
    claims = build_adjudicated_claims(imported)
    assert claims["review_status"].tolist() == ["accepted", "accepted"]
    assert claims["acceptance"].tolist() == [True, True]
    assert claims["parser_version"].nunique() == 1
    assert claims["claim_id"].is_unique


def test_adjudicated_claim_builder_rejects_accepted_blank_url():
    imported = pd.DataFrame([{
        "item_id": "blank", "candidate_year_id": "1__2001",
        "candidate_url": pd.NA, "accepted_claim": True,
        "identity_match": "yes", "temporal_match": "yes",
        "url_type": "custom_domain", "evidence_grade": "A",
    }])
    with pytest.raises(ValueError, match="have no URL"):
        build_adjudicated_claims(imported)


def test_adjudicated_claim_builder_adds_scheme_to_embedded_www_domain():
    imported = pd.DataFrame([{
        "item_id": "schemeless", "candidate_year_id": "1__2001",
        "candidate_url": "www.CandidateExample.com/Path",
        "accepted_claim": True,
        "identity_match": "yes", "temporal_match": "yes",
        "url_type": "custom_domain", "evidence_grade": "C",
    }])
    claims = build_adjudicated_claims(imported)
    assert claims.loc[0, "raw_url"] == "www.CandidateExample.com/Path"
    assert claims.loc[0, "exact_normalized_url"] == (
        "http://www.CandidateExample.com/Path"
    )
    assert claims.loc[0, "evidence_url"] == (
        "http://www.CandidateExample.com/Path"
    )


def test_pilot_metrics_use_population_strata():
    adjudicated = pd.DataFrame([
        {"candidate_year_id": "1__2001", "strict_custom_domain": True, "strict_exact_url": True, "lead_origin": "search_proposal", "accepted_claim": True},
        {"candidate_year_id": "2__2001", "strict_custom_domain": False, "strict_exact_url": False, "lead_origin": "search_proposal", "accepted_claim": False},
        {"candidate_year_id": "3__2019", "strict_custom_domain": False, "strict_exact_url": True, "lead_origin": "search_proposal", "accepted_claim": True},
        {"candidate_year_id": "4__2019", "strict_custom_domain": False, "strict_exact_url": False, "lead_origin": "negative_audit", "accepted_claim": False},
    ])
    manifest = pd.DataFrame([
        {"candidate_year_id": "1__2001", "period_stratum": "2000_2007", "profile_stratum": "no_profile", "query_variant": "a"},
        {"candidate_year_id": "2__2001", "period_stratum": "2000_2007", "profile_stratum": "no_profile", "query_variant": "a"},
        {"candidate_year_id": "3__2019", "period_stratum": "2015_2021", "profile_stratum": "profile", "query_variant": "a"},
        {"candidate_year_id": "4__2019", "period_stratum": "2015_2021", "profile_stratum": "profile", "query_variant": "a"},
    ])
    summary = pd.DataFrame([
        {"candidate_year_id": f"x{i}", "year": 2001, "ballotpedia_url": pd.NA, "has_validated_campaign_url": False}
        for i in range(8)
    ] + [
        {"candidate_year_id": f"y{i}", "year": 2019, "ballotpedia_url": "profile", "has_validated_campaign_url": False}
        for i in range(2)
    ])
    metrics = pilot_metrics(adjudicated, manifest, summary, repetitions=100, seed=1)
    assert metrics["metrics"]["strict_custom_domain"]["post_stratified_rate"] == 0.4
    assert metrics["metrics"]["strict_exact_url"]["post_stratified_rate"] == 0.5
    assert metrics["adjudication"] == {
        "reviewed_items": 4,
        "positive_leads_reviewed": 3,
        "accepted_positive_claims": 2,
        "positive_lead_precision": pytest.approx(2 / 3),
        "negative_audits_reviewed": 1,
        "negative_audit_false_negatives": 0,
    }
