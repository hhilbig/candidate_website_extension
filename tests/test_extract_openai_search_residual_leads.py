from __future__ import annotations

import json

import pandas as pd

from scripts.extract_openai_search_residual_leads import (
    extract_leads,
    validation_leads_from_targets,
)
from scripts.freeze_mayoral_search_temporal_validation import freeze_leads


def _inputs():
    manifest_rows = []
    screen_rows = []
    wrappers = []
    for index in range(100):
        candidate_id = f"candidate-{index}"
        attempt_id = f"attempt-{index}"
        manifest_rows.append({"sample_order": index + 1, "attempt_id": attempt_id, "candidate_year_id": candidate_id})
        screen_rows.append({
            "candidate_year_id": candidate_id, "candidate_contest_id": f"contest-{index}",
            "contest": f"contest-{index}", "full_name": f"Candidate {index}",
            "geo_name": "Example", "state_abb": "CA", "year": 2019,
            "month": 11, "period": "2018_2021",
        })
        value = {"candidate": f"Candidate {index}", "campaign_urls": [], "evidence_urls": [], "notes": "none"}
        if index == 0:
            value["campaign_urls"] = [
                "https://VoteCandidate.example/", "https://VoteCandidate.example/about",
                {"url": "https://candidateforcongress.example/", "evidence_match": "wrong office"},
            ]
            value["evidence_urls"] = [{"url": "https://news.example/item", "label": "article"}]
        text = json.dumps(value)
        response = {"output": [{"type": "message", "content": [{"type": "output_text", "text": text}]}]}
        wrappers.append({"attempt_id": attempt_id, "candidate_year_id": candidate_id, "raw_response_text": json.dumps(response)})
    return wrappers, pd.DataFrame(manifest_rows), pd.DataFrame(screen_rows)


def test_extracts_string_and_object_urls_and_collapses_candidate_domain() -> None:
    wrappers, manifest, screen = _inputs()
    outputs, claims, targets, audit = extract_leads(
        wrappers, manifest, screen, pd.DataFrame(columns=["candidate_year_id", "registrable_domain"])
    )
    assert len(outputs) == 100
    assert audit["campaign_url_claims"] == 3
    assert audit["wrong_office_campaign_claims"] == 1
    assert audit["eligible_campaign_claims"] == 2
    assert len(targets) == 1
    assert targets.iloc[0].campaign_claim_count == 2
    object_claim = claims.loc[claims.raw_url.eq("https://candidateforcongress.example/")].iloc[0]
    assert "evidence_match" in object_claim.item_metadata_json
    assert bool(object_claim.wrong_office_flag)
    validation = validation_leads_from_targets(targets)
    assert validation.columns.tolist()[0] == "contract_version"
    assert validation.iloc[0].extraction_method == "openai_model_explicit_campaign_url"
    assert validation.iloc[0].candidate_attribution_status == "identity_supported"
    frozen, decisions, _ = freeze_leads(
        validation, pd.DataFrame(columns=["page_link_id", "anchor_text"])
    )
    assert len(frozen) == 1
    assert bool(decisions.iloc[0].eligible_for_temporal_validation)
    assert decisions.iloc[0].eligibility_reason == "openai_model_explicit_campaign_url_pending_content_validation"


def test_preserves_serper_overlap_as_audit_not_a_filter() -> None:
    wrappers, manifest, screen = _inputs()
    serper = pd.DataFrame({"candidate_year_id": ["candidate-0"], "registrable_domain": ["votecandidate.example"]})
    _, _, targets, audit = extract_leads(wrappers, manifest, screen, serper)
    assert len(targets) == 1
    assert bool(targets.iloc[0].in_serper_candidate_domain)
    assert audit["serper_candidate_domain_overlap"] == 1


def test_known_profile_domain_is_preserved_but_not_eligible() -> None:
    wrappers, manifest, screen = _inputs()
    value = {
        "candidate": "Candidate 0",
        "campaign_urls": ["https://ballotpedia.org/Candidate_0"],
        "evidence_urls": [],
        "notes": "profile only",
    }
    response = {"output": [{"type": "message", "content": [{"type": "output_text", "text": json.dumps(value)}]}]}
    wrappers[0]["raw_response_text"] = json.dumps(response)
    _, claims, targets, audit = extract_leads(
        wrappers, manifest, screen,
        pd.DataFrame(columns=["candidate_year_id", "registrable_domain"]),
    )
    claim = claims.loc[claims.candidate_year_id.eq("candidate-0")].iloc[0]
    assert bool(claim.known_non_candidate_domain)
    assert claim.candidate_site_exclusion_reason == "known_election_or_profile_domain"
    assert not bool(claim.eligible_candidate_site_lead)
    assert audit["known_non_candidate_campaign_claims"] == 1
    assert targets.candidate_year_id.ne("candidate-0").all()


def test_malformed_and_blank_provider_outputs_are_retained_as_parse_errors() -> None:
    wrappers, manifest, screen = _inputs()
    malformed = {"output": [{"type": "message", "content": [
        {"type": "output_text", "text": '{"campaign_urls": ["https://bad.example/"'}
    ]}]}
    wrappers[0]["raw_response_text"] = json.dumps(malformed)
    wrappers[1]["raw_response_text"] = ""
    outputs, claims, targets, audit = extract_leads(
        wrappers, manifest, screen,
        pd.DataFrame(columns=["candidate_year_id", "registrable_domain"]),
    )
    errors = outputs.loc[outputs.parse_status.eq("parse_error")]
    assert len(errors) == 2
    assert audit["assistant_outputs_parse_error"] == 2
    assert claims.candidate_year_id.isin(["candidate-0", "candidate-1"]).sum() == 0
    assert targets.candidate_year_id.isin(["candidate-0", "candidate-1"]).sum() == 0
