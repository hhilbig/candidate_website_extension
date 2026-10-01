import pandas as pd

import scripts.expand_mayoral_search_evidence as expansion
from scripts.expand_mayoral_search_evidence import (
    expand_queue,
    extract_candidate_links,
    merge_evidence_queue,
)


def test_extracts_candidate_shaped_anchor_and_rejects_news_domain():
    html = '''
    <p>Mike Duggan <a href="https://dugganfordetroit.com">Campaign website</a></p>
    <a href="https://detroitnews.com/elections">Coverage</a>
    '''
    links = extract_candidate_links(html, "https://news.example/guide", "Mike Duggan", "Detroit")
    assert links == [{
        "discovered_campaign_url": "https://dugganfordetroit.com",
        "extraction_method": "candidate_shaped_anchor",
    }]


def test_extracts_plain_text_candidate_domain():
    html = "<p>Visit www.votevirg.com! for Virg Bernero.</p>"
    links = extract_candidate_links(html, "https://news.example/", "Virg Bernero", "Lansing")
    assert links == [{
        "discovered_campaign_url": "www.votevirg.com",
        "extraction_method": "candidate_shaped_text",
    }]


def test_extracts_candidate_domain_from_meta_refresh():
    html = '<meta http-equiv="REFRESH" content="0;url=http://www.ryanfrazierforaurora.com">'
    links = extract_candidate_links(html, "http://frazierforcolorado.com", "Ryan Frazier", "Aurora")
    assert links == [{
        "discovered_campaign_url": "http://www.ryanfrazierforaurora.com",
        "extraction_method": "candidate_shaped_meta_refresh",
    }]


def test_extract_candidate_links_unwraps_wayback_target_and_preserves_case():
    html = (
        '<a href="https://web.archive.org/web/20210401000000/'
        'https://VoteDuggan.com/Issues">Campaign website</a>'
    )
    links = extract_candidate_links(
        html, "https://news.example/guide", "Mike Duggan", "Detroit"
    )
    assert links == [{
        "discovered_campaign_url": "https://VoteDuggan.com/Issues",
        "extraction_method": "candidate_shaped_anchor",
    }]


def test_temporal_conflict_is_expanded_and_uses_recorded_election_month(monkeypatch):
    queue = pd.DataFrame([{
        "candidate_year_id": "1__2021", "candidate_contest_id": "c1__1",
        "contest": "c1", "full_name": "Mike Duggan", "geo_name": "Detroit",
        "state_abb": "MI", "year": 2021, "month": 4,
        "evidence_source_url": "https://news.example/guide",
        "evidence_type": "source_evidence", "temporal_conflict": True,
        "office_mismatch_signal": False, "query_variant": "campaign_website_year",
        "result_rank": 1, "exact_year_context": False, "mentioned_years": "2020",
        "result_title": "Candidate guide", "result_snippet": "Campaign website",
        "proposed_campaign_url": "", "review_status": "pending", "review_note": "",
    }])

    class Response:
        text = "<p>No link in current page</p>"
        url = "https://news.example/guide"
        status_code = 200

        def raise_for_status(self):
            return None

    monkeypatch.setattr(expansion.requests.Session, "get", lambda *_a, **_k: Response())
    monkeypatch.setattr(
        expansion, "query_campaign_url_captures",
        lambda *_a, **_k: ["20210416000000", "20211116000000"],
    )
    monkeypatch.setattr(
        expansion, "fetch_text_with_curl",
        lambda *_a, **_k: '<a href="https://DugganForDetroit.com/Issues">Website</a>',
    )
    result = expand_queue(queue, use_wayback=True).iloc[0]
    assert result["source_snapshot_timestamp"] == "20210416000000"
    assert bool(result["temporal_conflict"])
    assert result["discovered_campaign_url"] == "https://DugganForDetroit.com/Issues"

    unified = merge_evidence_queue(queue, pd.DataFrame([result]))
    expanded = unified[unified["evidence_type"].eq("expanded_domain_evidence")].iloc[0]
    assert bool(expanded["temporal_conflict"])
    assert expanded["candidate_contest_id"] == "c1__1"
    assert expanded["month"] == 4
