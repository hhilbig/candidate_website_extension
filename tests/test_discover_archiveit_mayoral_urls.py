import json
import sys
from pathlib import Path

import pandas as pd
import pytest
import requests

import scripts.discover_archiveit_mayoral_urls as archiveit
from scripts.discover_archiveit_mayoral_urls import (
    ATTEMPT_COLUMNS,
    COLLECTION_CONFIGS,
    EVIDENCE_COLUMNS,
    SEED_RECORD_COLUMNS,
    ArchiveItSourceUnavailable,
    classify_url,
    discover_archiveit,
    fetch_partner_seed_json,
    load_collection_configs,
    normalize_exact_url,
    parse_collection_seeds,
    site_root,
    unwrap_archive_url,
)


FIXTURES = Path(__file__).parent / "fixtures"


def load_fixture(collection_id):
    return json.loads((FIXTURES / f"archiveit_{collection_id}_seeds.json").read_text())


def candidate(identifier, name, city, state, year, month):
    contest = f"{city}_{state}_{year}_{month}_Mayor"
    return {
        "candidate_year_id": f"{identifier}__{year}",
        "candidate_contest_id": f"{contest}__{identifier}",
        "ledb_candid": identifier,
        "full_name": name,
        "geo_name": city,
        "state_abb": state,
        "year": year,
        "month": month,
        "contest": contest,
        "in_discovery_scope": True,
        "source_dataset": "American Local Government Elections Database",
        "source_doi": "10.17605/OSF.IO/MV5E6",
    }


def birmingham_universe():
    names = [
        (369, "cerissa brown"), (371, "chris woods"),
        (373, "darryl williams"), (329, "lashunda scales"),
        (382, "napoleon gonzalez"), (386, "philemon hill"),
        (391, "randall woodfin"), (359, "william bell"),
    ]
    return pd.DataFrame([
        candidate(identifier, name, "birmingham", "AL", 2021, 8)
        for identifier, name in names
    ])


def los_angeles_universe():
    return pd.DataFrame([
        candidate(1, "antonio villaraigosa", "los angeles", "CA", 2001, 6),
        candidate(2, "james hahn", "los angeles", "CA", 2001, 6),
        candidate(3, "eric garcetti", "los angeles", "CA", 2017, 3),
        candidate(4, "yuval kremer", "los angeles", "CA", 2017, 3),
    ])


def test_unwraps_wayback_and_ucla_embedded_targets_without_losing_path_case():
    wayback = (
        "https://web.archive.org/web/20210405010203id_/"
        "https://Candidate.Example/Issues/Plan.HTML?Ref=Mail"
    )
    ucla = (
        "http://digital.library.ucla.edu/websites/2009_999_006/"
        "web.mac.com/WalterMoore/ForMayor/Home.HTML?From=Seed"
    )
    assert unwrap_archive_url(wayback) == "https://Candidate.Example/Issues/Plan.HTML?Ref=Mail"
    assert normalize_exact_url(wayback) == "https://candidate.example/Issues/Plan.HTML?Ref=Mail"
    assert unwrap_archive_url(ucla) == "http://web.mac.com/WalterMoore/ForMayor/Home.HTML?From=Seed"
    assert site_root(ucla) == "http://web.mac.com/"


def test_url_contract_distinguishes_custom_shared_social_and_fundraising_leads():
    assert classify_url("https://www.CandidateForMayor.com/Issues") == "candidate_owned_custom_domain"
    assert classify_url("https://candidate.nationbuilder.com/issues") == "candidate_subdomain_or_shared_host_path"
    assert classify_url("https://facebook.com/candidate") == "excluded_social_lead"
    assert classify_url("https://secure.actblue.com/donate/candidate") == "excluded_fundraising_lead"


def test_ucla_fixture_uses_structured_mayoral_subject_and_preserves_seed_metadata():
    records = parse_collection_seeds(load_fixture(5903), COLLECTION_CONFIGS[5903])
    assert len(records) == 3
    assert list(records.columns) == SEED_RECORD_COLUMNS
    assert set(records.seed_id) == {"1001", "1002", "1003"}
    wrapped = records.loc[records.seed_id.eq("1001")].iloc[0]
    assert wrapped.raw_url.startswith("http://digital.library.ucla.edu/websites/")
    assert wrapped.exact_normalized_url == (
        "http://www.villaraigosaexample.com/Issues/Plan.HTML?Ref=Mail"
    )
    assert wrapped.target_url_source == "archive_wrapper_path"
    assert "Villaraigosa, Antonio R." in wrapped.metadata_creator
    assert json.loads(wrapped.source_metadata_json)["Coverage"][0]["value"] == "City of Los Angeles"
    assert json.loads(wrapped.source_seed_json)["canonical_url"].startswith(
        "http://digital.library.ucla.edu/websites/"
    )


def test_birmingham_fixture_matches_six_of_eight_candidates_and_emits_one_terminal_attempt():
    configs = {16766: COLLECTION_CONFIGS[16766]}
    evidence, attempts, seeds = discover_archiveit(
        birmingham_universe(), {16766: load_fixture(16766)}, configs=configs,
    )
    assert len(seeds) == 6
    assert len(evidence) == 6
    assert list(evidence.columns) == EVIDENCE_COLUMNS
    assert list(attempts.columns) == ATTEMPT_COLUMNS
    assert set(evidence.full_name) == {
        "cerissa brown", "chris woods", "lashunda scales", "philemon hill",
        "randall woodfin", "william bell",
    }
    assert evidence.identity_match.all()
    assert set(evidence.evidence_grade) == {"C"}
    assert not evidence.strict_coverage_eligible.any()
    assert set(evidence.temporal_validation_status) == {"pending_archive_validation"}
    assert len(attempts) == 1
    assert attempts.iloc[0].source_status == "found"
    assert attempts.iloc[0].candidate_matches == 6
    assert attempts.iloc[0].eligible_url_leads == 6


def test_los_angeles_matches_by_city_year_and_election_month_and_keeps_social_lead():
    configs = {5903: COLLECTION_CONFIGS[5903]}
    evidence, attempts, seeds = discover_archiveit(
        los_angeles_universe(), {5903: load_fixture(5903)}, configs=configs,
    )
    assert len(evidence) == 3
    assert set(evidence.candidate_year_id) == {"1__2001", "3__2017", "4__2017"}
    assert set(evidence.temporal_match) == {"exact_election_month"}
    assert evidence.loc[evidence.candidate_year_id.eq("1__2001"), "exact_normalized_url"].item().endswith(
        "/Issues/Plan.HTML?Ref=Mail"
    )
    social = evidence.loc[evidence.candidate_year_id.eq("4__2017")].iloc[0]
    assert social.url_type == "excluded_social_lead"
    assert social.review_status == "pending"
    assert len(attempts) == 2
    assert set(attempts.source_status) == {"found"}
    assert not seeds.match_status.eq("not_attempted").any()


def test_exact_source_month_prevents_primary_seed_from_being_assigned_to_general_contest():
    payload = [{
        "id": 55, "collection": 5903, "url": "https://candidate.example/",
        "metadata": {
            "Creator": [{"value": "Candidate, Alice"}],
            "Subject": [{"value": "Los Angeles Mayor"}, {"value": "Primary Election"}],
            "Coverage": [{"value": "City of Los Angeles"}],
            "Date of Election": [{"value": "2005-03-08"}],
        },
    }]
    universe = pd.DataFrame([
        candidate(5, "alice candidate", "los angeles", "CA", 2005, 5),
        candidate(6, "bob candidate", "los angeles", "CA", 2005, 5),
    ])
    evidence, attempts, seeds = discover_archiveit(
        universe, {5903: payload}, configs={5903: COLLECTION_CONFIGS[5903]},
    )
    assert evidence.empty
    assert seeds.iloc[0].match_status == "outside_target_contests"
    assert attempts.iloc[0].source_status == "no_result"


def test_unmatched_candidate_yields_match_unresolved_not_silent_no_result():
    payload = [{
        "id": 56, "collection": 5903, "url": "https://unknownformayor.example/",
        "metadata": {
            "Creator": [{"value": "Unknown, Ursula"}],
            "Subject": [{"value": "Los Angeles Mayor"}],
            "Coverage": [{"value": "City of Los Angeles"}],
            "Date of Election": [{"value": "2017-03-07"}],
        },
    }]
    evidence, attempts, seeds = discover_archiveit(
        los_angeles_universe(), {5903: payload},
        configs={5903: COLLECTION_CONFIGS[5903]},
    )
    assert evidence.empty
    assert "candidate_match_unresolved" in seeds.iloc[0].match_status
    row = attempts.loc[attempts.year.eq(2017)].iloc[0]
    assert row.source_status == "match_unresolved"


def test_missing_and_failed_collections_emit_one_terminal_attempt_per_contest_source():
    universe = pd.concat([los_angeles_universe().iloc[:2], birmingham_universe()])
    evidence, attempts, seeds = discover_archiveit(
        universe,
        {},
        collection_errors={5903: ("request_error", "HTTP 503")},
    )
    assert evidence.empty and seeds.empty
    assert len(attempts) == 2
    assert not attempts.duplicated(["source_id", "contest_id"]).any()
    by_collection = attempts.set_index("collection_id").source_status.to_dict()
    assert by_collection == {5903: "request_error", 16766: "source_unavailable"}
    assert set(attempts.source_status).issubset(archiveit.TERMINAL_ATTEMPT_STATUSES)


def test_claim_and_attempt_ids_are_stable_under_api_record_reordering():
    payload = load_fixture(16766)
    configs = {16766: COLLECTION_CONFIGS[16766]}
    first = discover_archiveit(birmingham_universe(), {16766: payload}, configs=configs)
    second = discover_archiveit(birmingham_universe(), {16766: list(reversed(payload))}, configs=configs)
    assert set(first[0].claim_id) == set(second[0].claim_id)
    assert first[1].attempt_id.tolist() == second[1].attempt_id.tolist()


def test_explicit_mayoral_collection_matches_candidate_hostname_without_seed_subject():
    config = archiveit.CollectionConfig(
        17435, "archive_it_collection_17435", "Cleveland 2021 Mayoral Race",
        "cleveland", "OH", 2021, 2021, 2021, True,
    )
    universe = pd.DataFrame([
        candidate(1, "justin bibb", "cleveland", "OH", 2021, 11),
        candidate(2, "kevin kelley", "cleveland", "OH", 2021, 11),
    ])
    payload = [
        {"id": 1, "collection": 17435, "url": "https://www.bibbforcle.com/", "metadata": {}},
        {"id": 2, "collection": 17435, "url": "https://www.kevinforcle.com/", "metadata": {}},
        {
            "id": 3, "collection": 17435,
            "url": "https://news.example/justin-bibb-wins-mayoral-election",
            "metadata": {"Title": [{"value": "Justin Bibb wins mayoral election"}]},
        },
    ]
    evidence, attempts, seeds = discover_archiveit(
        universe, {17435: payload}, configs={17435: config},
    )
    assert set(evidence.candidate_year_id) == {"1__2021", "2__2021"}
    assert set(evidence.exact_normalized_url) == {
        "https://www.bibbforcle.com/", "https://www.kevinforcle.com/",
    }
    assert len(seeds) == 3
    news = seeds.loc[seeds.seed_id.eq("3")].iloc[0]
    assert "unresolved" in news.match_status
    assert attempts.iloc[0].source_status == "found"


def test_non_mayoral_collection_still_requires_structured_subject():
    config = archiveit.CollectionConfig(
        999, "archive_it_collection_999", "Broad city archive",
        "cleveland", "OH", 2021, 2021, 2021,
    )
    records = parse_collection_seeds([
        {"id": 1, "collection": 999, "url": "https://www.bibbforcle.com/", "metadata": {}},
    ], config)
    assert records.empty


def test_load_collection_configs_accepts_batch_metadata_and_rejects_duplicates(tmp_path):
    config_path = tmp_path / "batch.json"
    config_path.write_text(json.dumps({"collections": [{
        "collection_id": 999,
        "source_id": "archive_it_collection_999",
        "collection_name": "Example mayoral archive",
        "geo_name": "Cleveland",
        "state_abb": "oh",
        "start_year": 2021,
        "end_year": 2021,
        "default_year": 2021,
        "collection_scoped_mayoral_seeds": True,
        "priority_tier": "direct_election",
        "selection_reason": "Extra batch metadata is retained in the manifest only.",
    }]}))
    configs = load_collection_configs(config_path)
    assert configs[999].geo_name == "cleveland"
    assert configs[999].state_abb == "OH"
    assert configs[999].collection_scoped_mayoral_seeds is True

    duplicate_path = tmp_path / "duplicate.json"
    duplicate_path.write_text(json.dumps({"collections": [
        {**json.loads(config_path.read_text())["collections"][0]},
        {**json.loads(config_path.read_text())["collections"][0]},
    ]}))
    with pytest.raises(ValueError, match="Duplicate"):
        load_collection_configs(duplicate_path)


def test_independent_seed_claims_survive_same_candidate_url_deduplication():
    payload = load_fixture(16766)
    duplicate = json.loads(json.dumps(payload[0]))
    duplicate["id"] = 9999999
    duplicate["created_date"] = "2021-06-01T00:00:00Z"
    evidence, _, _ = discover_archiveit(
        birmingham_universe(), {16766: [payload[0], duplicate]},
        configs={16766: COLLECTION_CONFIGS[16766]},
    )
    woodfin = evidence[evidence.candidate_year_id.eq("391__2021")]
    assert len(woodfin) == 2
    assert woodfin.claim_id.nunique() == 2
    assert woodfin.exact_normalized_url.nunique() == 1


class FakeResponse:
    def __init__(self, status_code, payload=None):
        self.status_code = status_code
        self.payload = payload

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def get(self, *_args, **_kwargs):
        self.calls += 1
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def test_live_fetch_retries_transient_failures(monkeypatch):
    session = FakeSession([
        requests.ConnectionError("temporary"), FakeResponse(503),
        FakeResponse(200, [{"id": 1}]),
    ])
    monkeypatch.setattr(archiveit.time, "sleep", lambda *_args: None)
    assert fetch_partner_seed_json(5903, attempts=3, delay_seconds=0, session=session) == [{"id": 1}]
    assert session.calls == 3


def test_live_fetch_marks_publicly_unavailable_collection_without_retry():
    session = FakeSession([FakeResponse(404)])
    with pytest.raises(ArchiveItSourceUnavailable):
        fetch_partner_seed_json(5903, attempts=3, delay_seconds=0, session=session)
    assert session.calls == 1


def test_cli_consumes_saved_json_and_writes_three_nonempty_contract_files(tmp_path, monkeypatch, capsys):
    universe_path = tmp_path / "universe.csv"
    birmingham_universe().to_csv(universe_path, index=False)
    output_dir = tmp_path / "output"
    fixture_path = FIXTURES / "archiveit_16766_seeds.json"
    monkeypatch.setattr(sys, "argv", [
        "discover_archiveit_mayoral_urls.py",
        "--universe", str(universe_path),
        "--output-dir", str(output_dir),
        "--collections", "16766",
        "--seed-json", f"16766={fixture_path}",
    ])
    archiveit.main()
    printed = json.loads(capsys.readouterr().out)
    assert printed["evidence_claims"] == 6
    for filename in (
        "mayoral_url_evidence_archiveit.csv",
        "mayoral_source_attempts_archiveit.csv",
        "mayoral_archiveit_seed_records.csv",
    ):
        path = output_dir / filename
        assert path.exists()
        assert path.stat().st_size > 100
