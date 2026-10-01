import json

import pandas as pd
import pytest
import requests
from unittest.mock import Mock

from scripts.discover_mayoral_search_urls import (
    is_candidate_lead,
    is_candidate_shaped_domain,
    build_lead_queue,
    prepare_manifest,
    query_variants,
    request_search,
    result_evidence,
    results_warrant_fallback,
    retrieve,
)


def candidate(identifier, year, profile=False, found=False):
    return {
        "candidate_year_id": identifier,
        "candidate_contest_id": f"contest-{identifier}__candidate",
        "contest": f"contest-{identifier}",
        "full_name": f"Candidate {identifier}",
        "geo_name": "springfield",
        "state_abb": "IL",
        "year": year,
        "month": 4,
        "ballotpedia_url": "https://ballotpedia.org/x" if profile else None,
        "has_validated_campaign_url": found,
    }


def test_queries_include_identity_office_place_and_year():
    row = pd.Series(candidate("abc", 2017))
    queries = query_variants(row)
    assert queries[0]["query"] == '"Candidate abc" mayor "springfield" IL 2017 campaign website'
    assert queries[1]["query"] == '"Candidate abc for mayor" "springfield" 2017'


def test_queries_remove_nested_nickname_quotes():
    row = pd.Series({**candidate("abc", 2017), "full_name": 'Arthur "Art" Schultz'})
    assert query_variants(row)[0]["query"].startswith('"Arthur Art Schultz" mayor')


def test_manifest_is_stratified_deterministic_and_excludes_found_candidates():
    rows = []
    for year in (2001, 2010, 2019):
        for profile in (False, True):
            rows.extend(candidate(f"{year}-{profile}-{i}", year, profile) for i in range(4))
    rows.append(candidate("already-found", 2019, found=True))
    summary = pd.DataFrame(rows)
    first = prepare_manifest(summary, sample_per_stratum=2, variants=1)
    second = prepare_manifest(summary.sample(frac=1, random_state=1), sample_per_stratum=2, variants=1)
    assert len(first) == 12
    assert set(first.candidate_year_id) == set(second.candidate_year_id)
    assert "already-found" not in set(first.candidate_year_id)
    assert first.groupby(["period_stratum", "profile_stratum"]).size().eq(2).all()
    assert {"candidate_contest_id", "contest", "month"}.issubset(first.columns)
    assert first["month"].eq(4).all()


def test_manifest_preserves_missing_election_month_without_imputing_november():
    summary = pd.DataFrame([{**candidate("abc", 2007), "month": pd.NA}])
    manifest = prepare_manifest(summary, sample_per_stratum=1, variants=1)
    assert len(manifest) == 1
    assert pd.isna(manifest.iloc[0]["month"])


def test_search_lead_filter_rejects_platforms_but_keeps_unknown_domain():
    assert not is_candidate_lead("https://www.facebook.com/vote-example")
    assert not is_candidate_lead("https://en.wikipedia.org/wiki/Example")
    assert is_candidate_lead("https://exampleformayor.com/")


def test_direct_campaign_domain_is_decisive():
    row = candidate("abc", 2017)
    row.update({"full_name": "Greg Ballard", "geo_name": "Indianapolis"})
    item = {"url": "http://www.ballardformayor.com", "title": "Greg Ballard for Mayor", "snippet": ""}
    classified = {**item, **result_evidence(item, row)}
    assert classified["evidence_type"] == "candidate_domain"
    assert not results_warrant_fallback([classified])


def test_platform_can_carry_embedded_domain_without_becoming_candidate_url():
    row = candidate("abc", 2007)
    row.update({"full_name": "Greg Ballard", "geo_name": "Indianapolis"})
    item = {
        "url": "https://en.wikipedia.org/wiki/Greg_Ballard",
        "title": "Greg Ballard",
        "snippet": "2007 Indianapolis mayor campaign website http://www.ballardformayor.com",
    }
    classified = result_evidence(item, row)
    assert not classified["candidate_lead"]
    assert classified["evidence_type"] == "embedded_domain_evidence"
    assert classified["embedded_urls"] == ["http://www.ballardformayor.com"]


def test_generic_identity_result_triggers_fallback():
    row = candidate("abc", 2005)
    row.update({"full_name": "Ron Tussing", "geo_name": "Billings"})
    item = {"url": "https://example.com/election", "title": "Ron Tussing", "snippet": "Billings mayor in 2005"}
    classified = {**item, **result_evidence(item, row)}
    assert classified["evidence_type"] == "generic_identity_result"
    assert results_warrant_fallback([classified])


def test_provider_credit_error_is_fatal(monkeypatch):
    response = Mock(status_code=400, text='{"message":"Not enough credits"}')
    monkeypatch.setattr("scripts.discover_mayoral_search_urls.requests.post", lambda *a, **k: response)
    with pytest.raises(RuntimeError, match="Not enough credits"):
        request_search("serper", "query", "key", 10, 30)


def test_lead_queue_preserves_domain_and_source_provenance_and_deduplicates():
    record = {
        **candidate("abc", 2007), "full_name": "Greg Ballard", "geo_name": "Indianapolis",
        "query_variant": "campaign_website_year",
        "results": [{
            "rank": 1, "url": "https://en.wikipedia.org/wiki/Greg_Ballard",
            "title": "Greg Ballard", "snippet": "Campaign website http://ballardformayor.com",
            "evidence_type": "embedded_domain_evidence",
            "embedded_urls": ["http://ballardformayor.com"], "exact_year_context": True,
        }],
    }
    queue = build_lead_queue([record, record])
    assert len(queue) == 1
    assert queue.iloc[0]["proposed_campaign_url"] == "http://ballardformayor.com"
    assert queue.iloc[0]["evidence_source_url"].startswith("https://en.wikipedia.org")
    assert queue.iloc[0]["review_status"] == "pending"
    assert queue.iloc[0]["contest"] == "contest-abc"
    assert queue.iloc[0]["month"] == 4


def test_embedded_url_preserves_original_path_and_query_case():
    row = candidate("abc", 2007)
    row.update({"full_name": "Greg Ballard", "geo_name": "Indianapolis"})
    item = {
        "url": "https://news.example/story",
        "title": "Greg Ballard for mayor",
        "snippet": "Campaign website https://BallardForMayor.com/Issues?Ref=Election",
    }
    classified = result_evidence(item, row)
    assert classified["embedded_urls"] == [
        "https://BallardForMayor.com/Issues?Ref=Election"
    ]


def test_direct_wayback_lead_uses_original_target_and_keeps_wrapper_as_source():
    record = {
        **candidate("abc", 2007), "full_name": "Greg Ballard",
        "geo_name": "Indianapolis", "query_variant": "campaign_website_year",
        "results": [{
            "rank": 1,
            "url": (
                "https://web.archive.org/web/20071001000000/"
                "http://BallardForMayor.com/Issues"
            ),
            "title": "Greg Ballard for Mayor", "snippet": "Indianapolis 2007",
        }],
    }
    queue = build_lead_queue([record])
    assert queue.iloc[0]["proposed_campaign_url"] == (
        "http://BallardForMayor.com/Issues"
    )
    assert queue.iloc[0]["evidence_source_url"].startswith(
        "https://web.archive.org/web/"
    )


def test_source_evidence_without_domain_remains_reviewable():
    record = {
        **candidate("abc", 2014), "query_variant": "campaign_website_year",
        "results": [{
            "rank": 2, "url": "https://example.org/candidate-guide", "title": "Candidate guide",
            "snippet": "Candidate abc for mayor; visit the campaign website", "evidence_type": "source_evidence",
            "embedded_urls": [], "exact_year_context": True,
        }],
    }
    queue = build_lead_queue([record])
    assert queue.iloc[0]["proposed_campaign_url"] == ""
    assert queue.iloc[0]["evidence_source_url"] == "https://example.org/candidate-guide"


def test_contemporary_visit_language_promotes_embedded_campaign_domain():
    row = candidate("abc", 2011)
    row.update({"full_name": "Alvin Brown", "geo_name": "Jacksonville"})
    item = {
        "url": "https://news.example/interview", "title": "Alvin Brown discusses run",
        "snippet": "2011 Jacksonville mayor campaign: visit http://www.alvinformayor.com",
    }
    classified = result_evidence(item, row)
    assert classified["evidence_type"] == "embedded_domain_evidence"
    assert classified["embedded_urls"] == ["http://www.alvinformayor.com"]


def test_later_cycle_domain_does_not_suppress_fallback():
    row = candidate("abc", 2007)
    row.update({"full_name": "Greg Ballard", "geo_name": "Indianapolis"})
    item = {
        "url": "http://www.ballardformayor.com", "title": "Greg Ballard for Mayor 2011",
        "snippet": "Indianapolis reelection campaign in 2011",
    }
    classified = {**item, **result_evidence(item, row)}
    assert classified["evidence_type"] == "candidate_domain"
    assert classified["temporal_conflict"]
    assert results_warrant_fallback([classified])


def test_wrong_office_signal_does_not_suppress_fallback():
    row = candidate("abc", 2007)
    row.update({"full_name": "Greg Ballard", "geo_name": "Indianapolis"})
    item = {
        "url": "http://www.voteballard.com", "title": "Greg Ballard for Secretary of State",
        "snippet": "Campaign for Indiana secretary of state",
    }
    classified = {**item, **result_evidence(item, row)}
    assert classified["office_mismatch_signal"]
    assert results_warrant_fallback([classified])


def test_generic_embedded_news_url_is_not_promoted():
    row = candidate("abc", 2011)
    row.update({"full_name": "Alvin Brown", "geo_name": "Jacksonville"})
    item = {
        "url": "https://forum.example/post", "title": "Alvin Brown for mayor",
        "snippet": "Read more at http://www.jacksonville.com/news/metro/2011",
    }
    classified = result_evidence(item, row)
    assert classified["embedded_urls"] == []
    assert classified["evidence_type"] != "embedded_domain_evidence"


def test_www_only_candidate_domain_in_snippet_is_recognized():
    row = candidate("abc", 2013)
    row.update({"full_name": "Mike Duggan", "geo_name": "Detroit"})
    item = {
        "url": "https://news.example/story", "title": "Mike Duggan mayoral campaign",
        "snippet": "2013 Detroit campaign website www.dugganfordetroit.com",
    }
    classified = result_evidence(item, row)
    assert classified["embedded_urls"] == ["www.dugganfordetroit.com"]
    assert classified["evidence_type"] == "embedded_domain_evidence"


def test_for_mayor_mention_alone_is_not_source_evidence():
    row = candidate("abc", 2014)
    item = {
        "url": "https://news.example/results", "title": "Candidate abc for mayor",
        "snippet": "Springfield election results",
    }
    assert result_evidence(item, row)["evidence_type"] == "generic_identity_result"


def test_result_url_contributes_temporal_conflict_signal():
    row = candidate("abc", 2010)
    row.update({"full_name": "Marcie Hodge", "geo_name": "Oakland"})
    item = {
        "url": "https://example.org/2020/candidates/marcie-hodge", "title": "Marcie Hodge",
        "snippet": "Official campaign website for Oakland candidate",
    }
    assert result_evidence(item, row)["temporal_conflict"]


def test_other_office_candidate_domain_is_rejected():
    assert not is_candidate_shaped_domain(
        "http://brightforcongress.com", "Bobby Bright", "Montgomery"
    )


def test_paid_retrieval_refuses_insufficient_approval_before_api_key_check(tmp_path):
    manifest = pd.DataFrame([{"candidate_year_id": f"c{i}", "query_variant": "x", "query": "q"}
                             for i in range(1000)])
    with pytest.raises(ValueError, match="below worst-case search cost"):
        retrieve(manifest, tmp_path / "out.jsonl", "brave", 4.99, 10, 30, 0)


def test_transient_search_error_is_retryable_and_not_marked_complete(tmp_path, monkeypatch):
    manifest = pd.DataFrame([{
        **candidate("abc", 2007), "query_variant": "campaign_website_year",
        "query": "query",
    }])
    output = tmp_path / "results.jsonl"
    calls = []

    def fake_search(*_args):
        calls.append(1)
        if len(calls) == 1:
            raise requests.ConnectionError("temporary outage")
        return []

    monkeypatch.setenv("SERPER_API_KEY", "test-key")
    monkeypatch.setattr(
        "scripts.discover_mayoral_search_urls.request_search", fake_search
    )
    retrieve(manifest, output, "serper", 0.01, 10, 30, 0)
    retrieve(manifest, output, "serper", 0.01, 10, 30, 0)
    records = [json.loads(line) for line in output.read_text().splitlines()]
    assert [record["retrieval_status"] for record in records] == [
        "request_error", "ok"
    ]
    assert len(calls) == 2
