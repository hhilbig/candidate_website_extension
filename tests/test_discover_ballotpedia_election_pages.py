import pandas as pd

from scripts.discover_ballotpedia_election_pages import (
    PAGE_STATUS_COLUMNS, ballotpedia_city_title, discover_election_pages,
    election_page_url,
)
from src.mayoral_discovery import (
    extract_ballotpedia_election_page_campaign_links, extract_ballotpedia_election_stage,
)


def test_election_page_url_uses_ballotpedia_title_pattern():
    assert election_page_url("atlanta", "GA", 2021) == (
        "https://ballotpedia.org/Mayoral_election_in_Atlanta,_Georgia_(2021)"
    )


def test_ballotpedia_city_title_preserves_compound_city_casing():
    assert ballotpedia_city_title("winston-salem") == "Winston-Salem"
    assert ballotpedia_city_title("lee's summit") == "Lee's_Summit"
    assert ballotpedia_city_title("mcallen") == "McAllen"
    assert ballotpedia_city_title("mckinney") == "McKinney"
    assert ballotpedia_city_title("boise city") == "Boise"
    assert ballotpedia_city_title("nashville-davidson county") == "Nashville"


def test_election_page_extractor_attributes_only_exact_candidate_section():
    html = """
    <h3>Andre Dickens</h3><h4>Campaign website</h4>
    <p>Text.<sup><a href="#cite_note-1">[1]</a></sup></p>
    <h3>Unmatched Person</h3><h4>Campaign website</h4>
    <p><a href="https://wrong.example">Official website</a></p>
    <ol><li id="cite_note-1"><a href="https://andreforatlanta.com/issues">Andre Dickens' campaign website</a></li></ol>
    """
    result = extract_ballotpedia_election_page_campaign_links(
        html, ["andre dickens", "felicia moore"]
    )
    assert len(result) == 1
    assert result[0]["full_name"] == "andre dickens"
    assert result[0]["evidence_url"] == "https://andreforatlanta.com/issues"


def test_election_page_extractor_does_not_cross_candidate_section_boundary():
    html = """
    <h3>Andre Dickens</h3><h4>Campaign website</h4><p>No link here.</p>
    <h3>Felicia Moore</h3><p><a href="https://felicia.example">Website</a></p>
    """
    assert extract_ballotpedia_election_page_campaign_links(
        html, ["andre dickens", "felicia moore"]
    ) == []


def test_election_stage_extractor_matches_recorded_month_only():
    html = """
    <h5 class="votebox-header-election-type">General runoff election for Mayor</h5>
    <p class="results_text">The runoff was held on December 15, 2020.</p>
    <h5 class="votebox-header-election-type">General election for Mayor</h5>
    <p class="results_text">Candidates ran on November 3, 2020.</p>
    """
    assert extract_ballotpedia_election_stage(html, 2020, 12) == {
        "source_election_stage":"verified_runoff_election",
        "source_election_stage_label":"General runoff election for Mayor",
        "source_election_date":"December 15, 2020",
        "source_election_stage_confidence":"high_date_matched_secondary_source",
    }
    assert extract_ballotpedia_election_stage(html, 2020, 10) == {}


def test_resume_prunes_stale_in_range_status_and_collapses_duplicates(tmp_path):
    universe = pd.DataFrame([
        {
            "contest":"current", "geo_name":"atlanta", "state_abb":"GA",
            "year":2021, "month":11, "in_discovery_scope":True,
            "candidate_year_id":"candidate-1", "full_name":"candidate one",
        }
    ])
    statuses = pd.DataFrame([
        {"contest":"current", "geo_name":"atlanta", "state_abb":"GA", "year":2021, "source_status":"error"},
        {"contest":"current", "geo_name":"atlanta", "state_abb":"GA", "year":2021, "source_status":"no_archived_page"},
        {"contest":"stale", "geo_name":"stale", "state_abb":"GA", "year":2021, "source_status":"no_archived_page"},
        {"contest":"older", "geo_name":"atlanta", "state_abb":"GA", "year":2014, "source_status":"no_archived_page"},
    ]).reindex(columns=PAGE_STATUS_COLUMNS)
    statuses.to_csv(tmp_path / "mayoral_source_status_election_pages.csv", index=False)

    _, result = discover_election_pages(
        universe, tmp_path, min_year=2015, max_pages=0, delay_seconds=0,
    )

    assert result["contest"].tolist() == ["current", "older"]
    assert result.loc[result.contest.eq("current"), "source_status"].item() == "no_archived_page"
