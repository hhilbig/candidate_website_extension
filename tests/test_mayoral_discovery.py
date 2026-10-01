import pandas as pd
import pytest
import time
import scripts.build_mayoral_url_discovery as discovery_script

from scripts.build_mayoral_url_discovery import candidate_shard, hard_deadline
from src.mayoral_discovery import (
    apply_stage_annotations,
    build_mayoral_universe,
    choose_near_election_snapshots,
    canonical_http_url,
    extract_ballotpedia_campaign_links,
    normalize_place_fips,
    verify_known_source_file,
    site_root_url,
    unwrap_wayback_url,
)


def test_stage_annotations_are_source_provenanced_and_leave_other_contests_unresolved(tmp_path):
    candidates = pd.DataFrame([
        {"geo_name":"alpha", "state_abb":"AA", "year":2021, "month":11,
         "election_stage_status":"stage_unresolved"},
        {"geo_name":"beta", "state_abb":"BB", "year":2021, "month":11,
         "election_stage_status":"stage_unresolved"},
    ])
    path = tmp_path / "stages.json"
    path.write_text(
        '[{"geo_name":"alpha","state_abb":"AA","year":2021,"month":11,'
        '"election_stage_status":"verified_general_election",'
        '"election_stage_source_url":"https://official.example/election",'
        '"election_stage_note":"Official general election."}]'
    )
    result = apply_stage_annotations(candidates, path).set_index("geo_name")
    assert result.loc["alpha", "election_stage_status"] == "verified_general_election"
    assert result.loc["alpha", "election_stage_source_url"].startswith("https://")
    assert result.loc["beta", "election_stage_status"] == "stage_unresolved"
    assert result.loc["beta", "election_stage_source_url"] == ""


def test_normalize_place_fips_restores_leading_zero():
    values = pd.Series(["667000", "0667000", "6037.0"], dtype="string")
    assert normalize_place_fips(values).tolist() == ["0667000", "0667000", "0006037"]


def test_build_mayoral_universe_preserves_missing_population(tmp_path):
    candidates = pd.DataFrame(
        [
            {
                "ledb_candid": 1,
                "full_name": "Alpha One",
                "firstname": "alpha",
                "lastname": "one",
                "fips": "0667000",
                "geo_name": "san francisco",
                "state_abb": "CA",
                "office_consolidated": "Mayor",
                "year": 2020,
                "month": 11,
                "contest": "c1",
                "votes": 10,
                "vote_share": 0.6,
                "winner": "win",
                "incumbent": 0,
                "ballotpedia_url": "https://ballotpedia.org/Alpha_One",
            },
            {
                "ledb_candid": 2,
                "full_name": "Beta Two",
                "firstname": "beta",
                "lastname": "two",
                "fips": "0667000",
                "geo_name": "san francisco",
                "state_abb": "CA",
                "office_consolidated": "Mayor",
                "year": 2020,
                "month": 11,
                "contest": "c1",
                "votes": 5,
                "vote_share": 0.4,
                "winner": "lose",
                "incumbent": 0,
                "ballotpedia_url": pd.NA,
            },
            {
                "ledb_candid": 3,
                "full_name": "Gamma Three",
                "firstname": "gamma",
                "lastname": "three",
                "fips": "9999999",
                "geo_name": "unknown",
                "state_abb": "ZZ",
                "office_consolidated": "Mayor",
                "year": 2020,
                "month": 11,
                "contest": "c2",
                "votes": 1,
                "vote_share": 1,
                "winner": "win",
                "incumbent": 0,
                "ballotpedia_url": pd.NA,
            },
        ]
    )
    cities = pd.DataFrame(
        [{"fips": "667000", "population_2020": 873965, "geo_type": "City"}]
    )
    candidate_path = tmp_path / "candidates.csv"
    city_path = tmp_path / "cities.csv"
    candidates.to_csv(candidate_path, index=False, encoding="latin-1")
    cities.to_csv(city_path, index=False, encoding="latin-1")

    result = build_mayoral_universe(str(candidate_path), str(city_path))

    scoped = result[result["in_discovery_scope"]]
    assert scoped["ledb_candid"].tolist() == [1, 2]
    assert scoped["candidate_year_id"].tolist() == ["1__2020", "2__2020"]
    unmatched = result[result["ledb_candid"].eq(3)].iloc[0]
    assert unmatched["population_status"] == "missing_population"
    assert not unmatched["in_discovery_scope"]


def test_known_population_gap_uses_source_provenanced_supplement(tmp_path):
    candidates = pd.DataFrame([
        {"ledb_candid": 1, "full_name": "A", "firstname": "a", "lastname": "a",
         "fips": "3420260", "geo_name": "edison", "state_abb": "NJ",
         "office_consolidated": "Mayor", "year": 2021, "month": 11,
         "contest": "c1", "votes": 2, "vote_share": .6, "winner": "win",
         "incumbent": 0, "ballotpedia_url": pd.NA},
        {"ledb_candid": 2, "full_name": "B", "firstname": "b", "lastname": "b",
         "fips": "3420260", "geo_name": "edison", "state_abb": "NJ",
         "office_consolidated": "Mayor", "year": 2021, "month": 11,
         "contest": "c1", "votes": 1, "vote_share": .4, "winner": "lose",
         "incumbent": 0, "ballotpedia_url": pd.NA},
    ])
    cities = pd.DataFrame([{"fips": "0000001", "population_2020": 1, "geo_type": "City"}])
    candidate_path, city_path = tmp_path / "c.csv", tmp_path / "p.csv"
    candidates.to_csv(candidate_path, index=False, encoding="latin-1")
    cities.to_csv(city_path, index=False, encoding="latin-1")
    result = build_mayoral_universe(str(candidate_path), str(city_path))
    assert result["population_2020"].eq(107_588).all()
    assert result["population_status"].eq("supplemented_proxy").all()
    assert result["in_discovery_scope"].all()
    assert result["population_source_url"].str.contains("census.gov").all()


def test_extracts_only_campaign_section_citations():
    html = """
    <html><body>
      <h4><span id="Campaign_website">Campaign website</span></h4>
      <p>The campaign said this.<sup><a href="#cite_note-site-1">1</a></sup></p>
      <h4><span id="News">News</span></h4>
      <p><a href="https://newspaper.example/story">News story</a></p>
      <ol>
        <li id="cite_note-site-1"><a href="https://candidateformayor.com/issues">Candidate for Mayor</a></li>
      </ol>
    </body></html>
    """
    result = extract_ballotpedia_campaign_links(html)
    assert result == [
        {
            "evidence_url": "https://candidateformayor.com/issues",
            "evidence_text": "Candidate for Mayor",
            "discovery_method": "ballotpedia_campaign_section_citation",
            "confidence": "high",
        }
    ]


def test_campaign_link_filter_excludes_social_and_news_direct_links():
    html = """
    <h3>Campaign website</h3>
    <p>
      <a href="https://votealpha.com">Website</a>
      <a href="https://votealpha.com/issues">Issues</a>
      <a href="https://localpaper.example/story">Candidate interview</a>
      <a href="https://facebook.com/votealpha">Facebook</a>
      <a href="https://nytimes.com/story">Article</a>
    </p>
    <h3>Biography</h3>
    """
    result = extract_ballotpedia_campaign_links(html)
    assert [row["evidence_url"] for row in result] == ["https://votealpha.com"]


def test_choose_snapshots_nearest_recorded_month():
    timestamps = [
        "20210101000000",
        "20211001000000",
        "20211116000000",
        "20220501000000",
        "bad",
    ]
    assert choose_near_election_snapshots(timestamps, 2021, 11, limit=2) == [
        "20211116000000",
        "20211001000000",
    ]


def test_hard_deadline_enforces_wall_clock_limit():
    with pytest.raises(TimeoutError):
        with hard_deadline(0.02):
            time.sleep(0.2)


def test_candidate_shards_are_stable_and_partitioned():
    identifiers = [f"candidate-{number}" for number in range(100)]
    assignments = [candidate_shard(value, 4) for value in identifiers]
    assert assignments == [candidate_shard(value, 4) for value in identifiers]
    assert set(assignments) == {0, 1, 2, 3}
    assert all(0 <= value < 4 for value in assignments)


def test_unknown_source_filename_returns_hash_without_published_check(tmp_path):
    path = tmp_path / "fixture.csv"
    path.write_bytes(b"abc")
    assert verify_known_source_file(str(path)) == (
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    )


def test_site_root_url_preserves_evidence_domain_only():
    assert site_root_url("https://www.alphaformayor.com/issues?id=1#top") == (
        "https://www.alphaformayor.com/"
    )


def test_wayback_target_is_unwrapped_before_url_normalization_and_keeps_case():
    wrapped = (
        "https://web.archive.org/web/20211001120000id_/"
        "https://VoteAlpha.COM/Issues/Jobs?Ref=Mayor"
    )
    assert unwrap_wayback_url(wrapped) == "https://VoteAlpha.COM/Issues/Jobs?Ref=Mayor"
    assert canonical_http_url(wrapped) == "https://VoteAlpha.COM/Issues/Jobs?Ref=Mayor"
    assert site_root_url(wrapped) == "https://VoteAlpha.COM/"


def test_archive_it_target_is_unwrapped():
    wrapped = (
        "https://wayback.archive-it.org/5903/20170101000000/"
        "http://ExampleForMayor.org/Platform"
    )
    assert canonical_http_url(wrapped) == "http://ExampleForMayor.org/Platform"


def test_snapshot_selection_requires_recorded_election_month():
    with pytest.raises(ValueError, match="Election month is required"):
        choose_near_election_snapshots(["20211116000000"], 2021, None)


def test_retry_pass_skips_and_preserves_previously_checked_snapshot(tmp_path, monkeypatch):
    universe = pd.DataFrame([{
        "candidate_year_id": "1__2021", "candidate_contest_id": "c__1",
        "ledb_candid": 1, "full_name": "Alpha", "geo_name": "city",
        "state_abb": "AA", "year": 2021, "month": 11,
        "ballotpedia_url": "https://ballotpedia.org/Alpha",
        "in_discovery_scope": True,
    }])
    timestamps = ["20210101000000", "20211116000000", "20211201000000"]
    fetched = []
    monkeypatch.setattr(discovery_script, "query_profile_snapshots", lambda *args, **kwargs: timestamps)
    monkeypatch.setattr(
        discovery_script,
        "fetch_archived_profile",
        lambda timestamp, *args, **kwargs: fetched.append(timestamp) or "<html></html>",
    )
    discovery_script.discover_ballotpedia_archive_urls(
        universe, output_dir=tmp_path, delay_seconds=0, snapshot_limit=1
    )
    first = fetched.copy()
    discovery_script.discover_ballotpedia_archive_urls(
        universe, output_dir=tmp_path, delay_seconds=0, snapshot_limit=3,
        retry_no_url=True,
    )
    status = pd.read_csv(tmp_path / "mayoral_source_status.csv").iloc[0]
    assert len(first) == 1
    assert len(fetched) == 3
    assert len(set(fetched)) == 3
    assert status["source_snapshots_checked"] == 3
    assert len(status["source_snapshot_timestamps_checked"].split(" | ")) == 3
