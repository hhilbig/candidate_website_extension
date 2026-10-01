import json

import pandas as pd
import pytest

from scripts.combine_mayoral_discovery import (
    build_candidate_summary,
    combine_manifest,
    combine_shards,
    coverage_metrics,
    coverage_by_year,
    coverage_by_stage,
    evidence_source_summary,
)


def _write_shard(root, index, universe, evidence, status):
    directory = root / f"shard_{index}"
    directory.mkdir(exist_ok=True)
    universe.to_csv(directory / "mayoral_candidate_universe.csv", index=False)
    evidence.to_csv(directory / "mayoral_url_evidence.csv", index=False)
    status.to_csv(directory / "mayoral_source_status.csv", index=False)


def test_combiner_requires_complete_status_and_builds_coverage(tmp_path):
    universe = pd.DataFrame([
        {"candidate_contest_id": "c1__1", "candidate_year_id": "1__2021", "contest": "c1", "ledb_candid": 1,
         "full_name": "Alpha", "geo_name": "city", "state_abb": "AA", "year": 2021,
         "month": 11,
         "population_2020": 200000, "population_status": "observed",
         "election_stage_status": "verified_general_election",
         "election_stage_source_url": "https://official.example/general",
         "election_stage_note": "Official general election.",
         "stage_scope_status": "verified_general", "in_verified_general_scope": True,
         "direct_election_status": "observed_candidate_vote_returns",
         "contest_validation_status": "valid_single_winner",
         "ballotpedia_url": "https://ballotpedia.org/Alpha", "in_discovery_scope": True,
         "source_dataset": "ALGEL", "source_doi": "doi"},
        {"candidate_contest_id": "c1__2", "candidate_year_id": "2__2021", "contest": "c1", "ledb_candid": 2,
         "full_name": "Beta", "geo_name": "city", "state_abb": "AA", "year": 2021,
         "month": 11,
         "population_2020": 200000, "population_status": "observed",
         "election_stage_status": "stage_unresolved_source_has_no_stage_field",
         "election_stage_source_url": "", "election_stage_note": "",
         "stage_scope_status": "stage_unresolved", "in_verified_general_scope": False,
         "direct_election_status": "observed_candidate_vote_returns",
         "contest_validation_status": "valid_single_winner",
         "ballotpedia_url": pd.NA, "in_discovery_scope": True,
         "source_dataset": "ALGEL", "source_doi": "doi"},
    ])
    evidence_columns = [
        "candidate_year_id", "evidence_url", "source_snapshot_timestamp", "year",
        "state_abb", "geo_name", "full_name", "url_source", "confidence",
    ]
    evidence = pd.DataFrame([["1__2021", "https://alphaformayor.com/issues", "20211101000000", 2021, "AA", "city", "Alpha", "archived profile", "high"]], columns=evidence_columns)
    empty_evidence = pd.DataFrame(columns=evidence_columns)
    status0 = pd.DataFrame(columns=["candidate_year_id", "source_status"])
    status1 = pd.DataFrame(columns=status0.columns)
    _write_shard(tmp_path, 0, universe, evidence, status0)
    _write_shard(tmp_path, 1, universe, empty_evidence, status1)

    with pytest.raises(ValueError, match="Discovery incomplete"):
        combine_shards(tmp_path, 2)

    status0 = pd.DataFrame([{"candidate_year_id": "1__2021", "source_status": "profile_checked"}])
    _write_shard(tmp_path, 0, universe, evidence, status0)
    pd.DataFrame([{
        "contest":"c1", "source_status":"page_checked",
        "source_profile_url":"https://ballotpedia.org/Mayoral_election_in_City",
        "source_snapshots_checked":1, "candidate_urls_found":1, "error":"",
    }]).to_csv(
        tmp_path / "shard_0" / "mayoral_source_status_election_pages.csv", index=False
    )
    combined_universe, combined_evidence, combined_status = combine_shards(tmp_path, 2)
    summary = build_candidate_summary(combined_universe, combined_evidence, combined_status)
    metrics = coverage_metrics(summary, combined_evidence)
    assert metrics["candidate_years_in_scope"] == 2
    assert metrics["candidate_years_with_validated_campaign_url"] == 1
    assert metrics["unique_campaign_hosts"] == 1
    assert metrics["url_yield_among_profiled_candidates"] == 1
    assert metrics["candidate_years_without_validated_campaign_url"] == 1
    assert metrics["verified_general_candidate_years"] == 1
    assert metrics["verified_general_candidate_years_with_validated_campaign_url"] == 1
    assert metrics["verified_general_url_coverage_rate"] == 1
    assert metrics["unresolved_candidate_years_by_reason"] == {
        "available_sources_checked_no_campaign_url_evidence": 1
    }
    assert metrics["candidate_years_with_election_page_checked"] == 2
    assert coverage_by_year(summary).iloc[0]["url_coverage_rate"] == 0.5
    stages = coverage_by_stage(summary).set_index("stage_scope_status")
    assert stages.loc["verified_general", "url_coverage_rate"] == 1
    assert stages.loc["stage_unresolved", "url_coverage_rate"] == 0
    assert evidence_source_summary(combined_evidence).iloc[0]["candidate_years"] == 1
    assert summary.set_index("candidate_year_id").loc["1__2021", "has_validated_campaign_url"]


def test_combiner_overlays_date_matched_election_page_stage(tmp_path):
    universe = pd.DataFrame([{
        "candidate_contest_id":"c1__1", "candidate_year_id":"1__2020",
        "contest":"c1", "ledb_candid":1, "full_name":"Alpha", "geo_name":"city",
        "state_abb":"AA", "year":2020, "month":11, "population_2020":200000,
        "population_status":"observed",
        "election_stage_status":"single_recorded_mayoral_contest_per_city_year_stage_not_explicit",
        "election_stage_source_url":"", "election_stage_note":"",
        "stage_scope_status":"stage_unresolved", "in_verified_general_scope":False,
        "direct_election_status":"observed_candidate_vote_returns",
        "contest_validation_status":"valid_single_winner", "ballotpedia_url":pd.NA,
        "in_discovery_scope":True, "source_dataset":"ALGEL", "source_doi":"doi",
    }])
    evidence = pd.DataFrame(columns=[
        "candidate_year_id", "evidence_url", "source_snapshot_timestamp", "year",
        "state_abb", "geo_name", "full_name", "url_source", "confidence",
    ])
    status = pd.DataFrame(columns=["candidate_year_id", "source_status"])
    _write_shard(tmp_path, 0, universe, evidence, status)
    pd.DataFrame([{
        "contest":"c1", "source_status":"page_checked",
        "source_profile_url":"https://ballotpedia.org/election", "source_snapshots_checked":1,
        "candidate_urls_found":0, "error":"",
        "source_election_stage":"verified_general_election",
        "source_election_stage_label":"General election for Mayor",
        "source_election_date":"November 3, 2020",
        "source_election_stage_confidence":"high_date_matched_secondary_source",
    }]).to_csv(tmp_path / "shard_0" / "mayoral_source_status_election_pages.csv", index=False)
    combined_universe, _, _ = combine_shards(tmp_path, 1)
    row = combined_universe.iloc[0]
    assert row["election_stage_status"] == "verified_general_election"
    assert row["stage_scope_status"] == "verified_general"
    assert bool(row["in_verified_general_scope"])
    assert row["election_stage_source_url"] == "https://ballotpedia.org/election"


def test_combiner_unwraps_wayback_and_preserves_independent_source_claims(tmp_path):
    universe = pd.DataFrame([{
        "candidate_contest_id":"c1__1", "candidate_year_id":"1__2021",
        "contest":"c1", "ledb_candid":1, "full_name":"Alpha", "geo_name":"city",
        "state_abb":"AA", "year":2021, "month":4, "population_2020":200000,
        "population_status":"observed", "election_stage_status":"stage_unresolved",
        "election_stage_source_url":"", "election_stage_note":"",
        "stage_scope_status":"stage_unresolved", "in_verified_general_scope":False,
        "direct_election_status":"observed_candidate_vote_returns",
        "contest_validation_status":"valid_single_winner",
        "ballotpedia_url":"https://ballotpedia.org/Alpha", "in_discovery_scope":True,
        "source_dataset":"ALGEL", "source_doi":"doi",
    }])
    wrapped = (
        "https://web.archive.org/web/20210401000000/"
        "https://AlphaForMayor.com/Issues?Ref=Election"
    )
    evidence = pd.DataFrame([
        {"candidate_year_id":"1__2021", "evidence_url":wrapped,
         "source_snapshot_timestamp":"20210401000000", "year":2021,
         "state_abb":"AA", "geo_name":"city", "full_name":"Alpha",
         "url_source":"source one", "source_profile_url":"https://one.example",
         "discovery_method":"official_link", "evidence_text":"Website", "confidence":"high"},
        {"candidate_year_id":"1__2021", "evidence_url":wrapped,
         "source_snapshot_timestamp":"20210401000000", "year":2021,
         "state_abb":"AA", "geo_name":"city", "full_name":"Alpha",
         "url_source":"source two", "source_profile_url":"https://two.example",
         "discovery_method":"official_link", "evidence_text":"Website", "confidence":"high"},
    ])
    status = pd.DataFrame([{"candidate_year_id":"1__2021", "source_status":"profile_checked"}])
    _write_shard(tmp_path, 0, universe, evidence, status)
    _, combined, _ = combine_shards(tmp_path, 1)
    assert len(combined) == 2
    assert combined["evidence_url"].eq(wrapped).all()
    assert combined["validated_campaign_url"].eq(
        "https://AlphaForMayor.com/Issues?Ref=Election"
    ).all()
    assert combined["campaign_site_url"].eq("https://AlphaForMayor.com/").all()
    assert combined["candidate_contest_id"].eq("c1__1").all()
    assert combined["contest"].eq("c1").all()
    assert combined["month"].eq(4).all()


def test_explicit_build_manifest_rejects_missing_duplicate_and_unexpected_files(tmp_path):
    universe = pd.DataFrame([{
        "candidate_contest_id":"c1__1", "candidate_year_id":"1__2021",
        "contest":"c1", "ledb_candid":1, "full_name":"Alpha", "geo_name":"city",
        "state_abb":"AA", "year":2021, "month":4, "population_2020":200000,
        "population_status":"observed", "election_stage_status":"stage_unresolved",
        "election_stage_source_url":"", "election_stage_note":"",
        "stage_scope_status":"stage_unresolved", "in_verified_general_scope":False,
        "direct_election_status":"observed_candidate_vote_returns",
        "contest_validation_status":"valid_single_winner", "ballotpedia_url":pd.NA,
        "in_discovery_scope":True, "source_dataset":"ALGEL", "source_doi":"doi",
    }])
    evidence = pd.DataFrame(columns=[
        "candidate_year_id", "evidence_url", "source_snapshot_timestamp", "year",
        "state_abb", "geo_name", "full_name", "url_source", "confidence",
    ])
    status = pd.DataFrame(columns=["candidate_year_id", "source_status"])
    _write_shard(tmp_path, 0, universe, evidence, status)
    manifest = tmp_path / "build.json"
    payload = {
        "universe_files":["shard_0/mayoral_candidate_universe.csv"],
        "evidence_files":["shard_0/mayoral_url_evidence.csv"],
        "status_files":["shard_0/mayoral_source_status.csv"],
        "election_page_status_files":[],
    }
    manifest.write_text(json.dumps(payload))
    assert len(combine_manifest(manifest)[0]) == 1
    registry = tmp_path / "registry.json"
    registry.write_text(json.dumps({"sources":[{
        "source_id":"fixture", "enabled":True,
        "output_files":[
            str(tmp_path / "shard_0" / "mayoral_url_evidence.csv"),
            str(tmp_path / "shard_0" / "mayoral_source_status.csv"),
        ],
    }]}))
    assert len(combine_manifest(manifest, registry)[0]) == 1

    # One legacy status file may be declared once for the build and once for
    # the contest-source attempt ledger.
    payload["attempt_status_files"] = ["shard_0/mayoral_source_status.csv"]
    manifest.write_text(json.dumps(payload))
    assert len(combine_manifest(manifest, registry)[0]) == 1
    payload["attempt_status_files"] *= 2
    manifest.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="within lists"):
        combine_manifest(manifest)
    payload["attempt_status_files"] = ["shard_0/mayoral_source_status.csv"]
    manifest.write_text(json.dumps(payload))

    registry.write_text(json.dumps({"sources":[{
        "source_id":"fixture", "enabled":False,
        "output_files":[
            str(tmp_path / "shard_0" / "mayoral_url_evidence.csv"),
            str(tmp_path / "shard_0" / "mayoral_source_status.csv"),
        ],
    }]}))
    with pytest.raises(ValueError, match="disabled or unregistered"):
        combine_manifest(manifest, registry)

    payload["evidence_files"] *= 2
    manifest.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="Duplicate build manifest"):
        combine_manifest(manifest)

    payload["evidence_files"] = ["shard_0/missing.csv"]
    manifest.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="files are missing"):
        combine_manifest(manifest)

    unexpected = tmp_path / "shard_0" / "other.csv"
    unexpected.write_text("x\n")
    payload["evidence_files"] = ["shard_0/other.csv"]
    manifest.write_text(json.dumps(payload))
    with pytest.raises(ValueError, match="Unexpected configured"):
        combine_manifest(manifest)
