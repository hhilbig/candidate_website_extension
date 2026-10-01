import json
from pathlib import Path

import pandas as pd
import pytest

from scripts.build_mayoral_source_attempt_ledger import (
    FILTER_REPORT_COLUMNS,
    LEDGER_PARSER_VERSION,
    TERMINAL_STATUSES,
    build_contest_source_universe,
    build_from_files,
    build_source_attempt_ledger,
    load_attempt_manifest,
)


PRECEDENCE = [
    "found", "request_error", "match_unresolved", "no_result",
    "source_unavailable",
]


def candidate(
    candidate_id, contest, city, state, year, month, in_scope=True,
    profile="https://ballotpedia.org/Candidate",
):
    return {
        "candidate_year_id": candidate_id,
        "candidate_contest_id": f"{contest}__{candidate_id}",
        "contest": contest,
        "geo_name": city,
        "state_abb": state,
        "place_fips": f"{len(candidate_id):07d}",
        "year": year,
        "month": month,
        "election_stage_status": "verified_general_election",
        "in_discovery_scope": in_scope,
        "ballotpedia_url": profile,
    }


def universe():
    return pd.DataFrame([
        candidate("la-a__2021", "la-2021", "los angeles", "CA", 2021, 6),
        candidate("la-b__2021", "la-2021", "los angeles", "CA", 2021, 6),
        candidate("bhm-a__2021", "bhm-2021", "birmingham", "AL", 2021, 8),
        candidate("sea-a__2001", "sea-2001", "seattle", "WA", 2001, 11),
        candidate("old__1999", "old-1999", "seattle", "WA", 1999, 11, False),
        candidate("old2__1998", "old-1998", "seattle", "WA", 1998, 11, False),
    ])


def standard_registry(attempt_path: Path):
    return [
        {
            "source_id": "linkage_only", "enabled": True,
            "attempt_tracking": False,
            "attempt_tracking_reason": "Candidate linkage is not a discovery attempt.",
        },
        {
            "source_id": "archive_source", "enabled": True,
            "attempt_tracking": True,
            "adapter": "scripts/source.py",
            "output_files": [str(attempt_path)],
            "years": {"start": 2021, "end": 2021},
            "jurisdictions": [
                {"geo_name": "los angeles", "state_abb": "CA"},
                {"geo_name": "birmingham", "state_abb": "AL"},
            ],
            "parser_version": "source-v2",
        },
    ]


def standard_attempts():
    return pd.DataFrame([
        {
            "attempt_id": "raw-la", "source_id": "archive_source",
            "contest_id": "la-2021", "source_status": "found",
            "parser_version": "source-v2", "retrieved_on": "2026-08-24",
            "error": "",
        },
        {
            "attempt_id": "raw-bhm", "source_id": "archive_source",
            "contest_id": "bhm-2021", "source_status": "no_result",
            "parser_version": "source-v2", "retrieved_on": "2026-08-24",
            "error": "",
        },
    ])


def test_builds_one_exact_terminal_row_per_contest_source_target(tmp_path):
    attempt_path = (tmp_path / "attempts.csv").resolve()
    registry = standard_registry(attempt_path)
    targets, ledger, filters = build_source_attempt_ledger(
        universe(), registry, {attempt_path: standard_attempts()}
    )
    assert len(targets) == len(ledger) == 2
    assert not targets.duplicated(["source_id", "contest_id"]).any()
    assert set(ledger["source_status"]) == {"found", "no_result"}
    assert set(ledger["source_status"]).issubset(TERMINAL_STATUSES)
    la = ledger.set_index("contest_id").loc["la-2021"]
    assert la["month"] == 6
    assert la["election_round"] == "verified_general_election"
    assert la["source_parser_versions"] == "source-v2"
    assert la["adapter"] == "scripts/source.py"
    assert la["ledger_parser_version"] == LEDGER_PARSER_VERSION
    assert len(la["attempt_id"]) == 64
    assert filters.columns.tolist() == FILTER_REPORT_COLUMNS


def test_scope_filters_are_explicit_including_drops_over_twenty_percent(tmp_path):
    path = (tmp_path / "attempts.csv").resolve()
    targets, filters, _ = build_contest_source_universe(
        universe(), standard_registry(path)
    )
    assert set(targets["contest_id"]) == {"la-2021", "bhm-2021"}
    large = filters[filters["exceeds_20_percent"]]
    assert not large.empty
    assert {
        "in_discovery_scope_candidate_rows", "years",
    }.issubset(set(large["filter_name"]))
    assert (large["drop_rate"] > 0.20).all()


@pytest.mark.parametrize(
    "mutator,match",
    [
        (lambda frame: frame.iloc[:1], "target mismatch"),
        (
            lambda frame: pd.concat([
                frame,
                pd.DataFrame([{
                    "source_id":"archive_source", "contest_id":"unexpected",
                    "source_status":"no_result", "parser_version":"source-v2",
                }]),
            ], ignore_index=True),
            "target mismatch",
        ),
        (
            lambda frame: pd.concat([frame, frame.iloc[[0]]], ignore_index=True),
            "Duplicate source-attempt target",
        ),
    ],
)
def test_fails_on_missing_unexpected_or_duplicate_targets(tmp_path, mutator, match):
    path = (tmp_path / "attempts.csv").resolve()
    with pytest.raises(ValueError, match=match):
        build_source_attempt_ledger(
            universe(), standard_registry(path), {path: mutator(standard_attempts())}
        )


def test_rejects_nonterminal_status_instead_of_guessing_semantics(tmp_path):
    path = (tmp_path / "attempts.csv").resolve()
    attempts = standard_attempts()
    attempts.loc[0, "source_status"] = "page_checked"
    with pytest.raises(ValueError, match="does not map"):
        build_source_attempt_ledger(
            universe(), standard_registry(path), {path: attempts}
        )


def test_tracking_false_requires_reason_and_enabled_sources_require_declaration(tmp_path):
    path = (tmp_path / "attempts.csv").resolve()
    missing_declaration = [{"source_id":"x", "enabled":True}]
    with pytest.raises(ValueError, match="must declare attempt_tracking"):
        build_contest_source_universe(universe(), missing_declaration)
    no_reason = [{
        "source_id":"x", "enabled":True, "attempt_tracking":False,
    }]
    with pytest.raises(ValueError, match="without a reason"):
        build_contest_source_universe(universe(), no_reason)


def seattle_universe():
    return pd.DataFrame([
        candidate("sea-a__2021", "sea-2021", "seattle", "WA", 2021, 11),
        candidate("sea-b__2021", "sea-2021", "seattle", "WA", 2021, 11),
        candidate("sea-a__2001", "sea-2001", "seattle", "WA", 2001, 11),
    ])


def test_legacy_mapping_uses_evidence_membership_not_checked_status_as_found(tmp_path):
    status_path = (tmp_path / "seattle_status.csv").resolve()
    evidence_path = (tmp_path / "seattle_evidence.csv").resolve()
    pd.DataFrame([{"candidate_year_id":"sea-a__2021"}]).to_csv(
        evidence_path, index=False
    )
    statuses = pd.DataFrame([
        {
            "year":2021, "source_status":"profile_checked",
            "matched_candidate_year_id":"sea-a__2021", "error":"",
        },
        {
            "year":2021, "source_status":"profile_checked",
            "matched_candidate_year_id":"sea-b__2021", "error":"",
        },
        {
            "year":2001, "source_status":"error",
            "matched_candidate_year_id":pd.NA, "error":"HTTP 500",
        },
    ])
    registry = [{
        "source_id":"seattle", "enabled":True, "attempt_tracking":True,
        "adapter":"scripts/discover_seattle_urls.py",
        "output_files":[str(status_path), str(evidence_path)],
        "years":[2001, 2021],
        "jurisdictions":[{"geo_name":"seattle", "state_abb":"WA"}],
        "attempt_status_mapping":{
            "mapping_version":"seattle-legacy-v1",
            "files":[str(status_path)],
            "key":{
                "candidate_year_id_column":"matched_candidate_year_id",
                "scope_year_unique":{"year_column":"year"},
            },
            "status_column":"source_status",
            "status_map":{
                "profile_checked":"no_result", "error":"request_error",
            },
            "found_evidence_files":[str(evidence_path)],
            "found_evidence_key":"candidate_year_id",
            "aggregation_precedence":PRECEDENCE,
            "parser_version":"seattle-legacy-v1",
            "error_column":"error",
        },
    }]
    targets, ledger, _ = build_source_attempt_ledger(
        seattle_universe(), registry, {status_path: statuses}
    )
    assert len(targets) == len(ledger) == 2
    outcomes = ledger.set_index("contest_id")["source_status"].to_dict()
    assert outcomes == {"sea-2001":"request_error", "sea-2021":"found"}
    row = ledger.set_index("contest_id").loc["sea-2021"]
    assert row["raw_source_statuses"] == "profile_checked"
    raw = json.loads(row["source_attempt_json"])
    assert len(raw) == 2
    assert sum(item["matched_candidate_year_id"] == "sea-a__2021" for item in raw) == 1


def test_manifest_rejects_missing_duplicate_and_non_csv_attempt_files(tmp_path):
    good = tmp_path / "attempt.csv"
    good.write_text("source_id,contest_id,source_status\n")
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({"attempt_status_files":["attempt.csv"]}))
    assert load_attempt_manifest(manifest) == [good.resolve()]

    manifest.write_text(json.dumps({
        "attempt_status_files":["attempt.csv", "attempt.csv"]
    }))
    with pytest.raises(ValueError, match="Duplicate"):
        load_attempt_manifest(manifest)

    manifest.write_text(json.dumps({"attempt_status_files":["missing.csv"]}))
    with pytest.raises(ValueError, match="Missing"):
        load_attempt_manifest(manifest)

    other = tmp_path / "attempt.json"
    other.write_text("[]")
    manifest.write_text(json.dumps({"attempt_status_files":["attempt.json"]}))
    with pytest.raises(ValueError, match="Unexpected"):
        load_attempt_manifest(manifest)


def test_file_build_writes_both_nonempty_outputs_and_reports_large_filters(tmp_path):
    universe_path = tmp_path / "universe.csv"
    registry_path = tmp_path / "registry.json"
    manifest_path = tmp_path / "manifest.json"
    attempt_path = (tmp_path / "attempts.csv").resolve()
    output_dir = tmp_path / "output"
    universe().to_csv(universe_path, index=False)
    standard_attempts().to_csv(attempt_path, index=False)
    registry_path.write_text(json.dumps(standard_registry(attempt_path)))
    manifest_path.write_text(json.dumps({"attempt_status_files":["attempts.csv"]}))
    summary = build_from_files(
        universe_path, registry_path, manifest_path, output_dir
    )
    assert summary["contest_source_targets"] == summary["source_attempts"] == 2
    assert summary["large_filter_reports"]
    for filename in (
        "mayoral_contest_universe.csv", "mayoral_source_attempt_ledger.csv",
        "mayoral_source_scope_filter_audit.csv",
    ):
        path = output_dir / filename
        assert path.exists()
        assert path.stat().st_size > 100


@pytest.fixture
def live_legacy_schema_fixture(tmp_path):
    rows = [
        candidate("bp-a__2021", "bp-2021", "boston", "MA", 2021, 11),
        candidate("bp-b__2021", "bp-2021", "boston", "MA", 2021, 11),
        candidate("ny-a__2005", "ny-2005", "new york", "NY", 2005, 11, profile=None),
        candidate("ny-b__2005", "ny-2005", "new york", "NY", 2005, 11, profile=None),
        candidate("sea-a__2005", "sea-2005", "seattle", "WA", 2005, 11, profile=None),
        candidate("sea-b__2005", "sea-2005", "seattle", "WA", 2005, 11, profile=None),
        candidate("sea-a__2001", "sea-2001", "seattle", "WA", 2001, 11, profile=None),
    ]
    status_paths = {
        "profiles": (tmp_path / "mayoral_source_status.csv").resolve(),
        "pages": (tmp_path / "mayoral_source_status_election_pages.csv").resolve(),
        "nyc": (tmp_path / "mayoral_source_status_nyc_cfb.csv").resolve(),
        "seattle": (tmp_path / "mayoral_source_status_seattle.csv").resolve(),
    }
    seattle_evidence = (tmp_path / "mayoral_url_evidence_seattle.csv").resolve()
    pd.DataFrame([{"candidate_year_id":"sea-a__2005"}]).to_csv(
        seattle_evidence, index=False
    )
    frames = {
        status_paths["profiles"]: pd.DataFrame([
            {"candidate_year_id":"bp-a__2021", "source_status":"profile_checked",
             "campaign_urls_found":1, "error":""},
            {"candidate_year_id":"bp-b__2021", "source_status":"no_archived_profile",
             "campaign_urls_found":0, "error":""},
        ]),
        status_paths["pages"]: pd.DataFrame([
            {"contest":"bp-2021", "source_status":"page_checked",
             "candidate_urls_found":1, "error":""},
            {"contest":"ny-2005", "source_status":"no_archived_page",
             "candidate_urls_found":0, "error":""},
            {"contest":"sea-2005", "source_status":"no_archived_page",
             "candidate_urls_found":0, "error":""},
            {"contest":"sea-2001", "source_status":"error",
             "candidate_urls_found":0, "error":"HTTP 503"},
        ]),
        status_paths["nyc"]: pd.DataFrame([
            {"year":2005, "source_status":"profile_checked",
             "matched_candidate_year_id":"ny-a__2005", "campaign_urls_found":1,
             "error":""},
            {"year":2005, "source_status":"candidate_match_unresolved",
             "matched_candidate_year_id":pd.NA, "campaign_urls_found":0,
             "error":""},
        ]),
        status_paths["seattle"]: pd.DataFrame([
            {"year":2005, "source_status":"profile_checked",
             "matched_candidate_year_id":"sea-a__2005", "error":""},
            {"year":2005, "source_status":"profile_checked",
             "matched_candidate_year_id":"sea-b__2005", "error":""},
            {"year":2001, "source_status":"error",
             "matched_candidate_year_id":pd.NA, "error":"HTTP 500"},
        ]),
    }
    common = {
        "enabled":True, "attempt_tracking":True,
        "aggregation_precedence":PRECEDENCE,
    }
    registry = [
        {
            "source_id":"profiles", **common,
            "adapter":"scripts/build_mayoral_url_discovery.py",
            "output_files":[str(status_paths["profiles"])],
            "years":{"start":2000, "end":2021},
            "jurisdictions":["United States"],
            "attempt_scope":{
                "all_in_scope":True,
                "candidate_filter":{"column":"ballotpedia_url", "operator":"notna"},
            },
            "attempt_status_mapping":{
                "mapping_version":"profiles-live-schema-v1",
                "files":[str(status_paths["profiles"])],
                "key":{"candidate_year_id_column":"candidate_year_id"},
                "status_map":{
                    "profile_checked":"no_result",
                    "no_archived_profile":"no_result",
                    "no_additional_snapshots":"no_result",
                },
                "found_count_column":"campaign_urls_found",
                "found_when_positive":True,
                "aggregation_precedence":PRECEDENCE,
                "parser_version":"profiles-legacy-v1",
            },
        },
        {
            "source_id":"pages", **common,
            "adapter":"scripts/discover_ballotpedia_election_pages.py",
            "output_files":[str(status_paths["pages"])],
            "years":{"start":2000, "end":2021},
            "jurisdictions":["United States"],
            "attempt_scope":{"all_in_scope":True},
            "attempt_status_mapping":{
                "mapping_version":"pages-live-schema-v1",
                "files":[str(status_paths["pages"])],
                "key":{"contest_id_column":"contest"},
                "status_map":{
                    "page_checked":"no_result", "no_archived_page":"no_result",
                    "error":"request_error",
                },
                "found_count_column":"candidate_urls_found",
                "found_when_positive":True,
                "aggregation_precedence":PRECEDENCE,
                "parser_version":"pages-legacy-v1",
            },
        },
        {
            "source_id":"nyc", **common,
            "adapter":"scripts/discover_nyc_cfb_urls.py",
            "output_files":[str(status_paths["nyc"])],
            "years":[2005],
            "jurisdictions":[{"geo_name":"new york", "state_abb":"NY"}],
            "attempt_status_mapping":{
                "mapping_version":"nyc-live-schema-v1",
                "files":[str(status_paths["nyc"])],
                "key":{
                    "candidate_year_id_column":"matched_candidate_year_id",
                    "scope_year_unique":{"year_column":"year"},
                },
                "status_map":{
                    "profile_checked":"no_result",
                    "candidate_match_unresolved":"match_unresolved",
                },
                "found_count_column":"campaign_urls_found",
                "found_when_positive":True,
                "aggregation_precedence":PRECEDENCE,
                "parser_version":"nyc-legacy-v1",
            },
        },
        {
            "source_id":"seattle", **common,
            "adapter":"scripts/discover_seattle_urls.py",
            "output_files":[str(status_paths["seattle"]), str(seattle_evidence)],
            "years":[2001, 2005],
            "jurisdictions":[{"geo_name":"seattle", "state_abb":"WA"}],
            "attempt_status_mapping":{
                "mapping_version":"seattle-live-schema-v1",
                "files":[str(status_paths["seattle"])],
                "key":{
                    "candidate_year_id_column":"matched_candidate_year_id",
                    "scope_year_unique":{"year_column":"year"},
                },
                "status_map":{
                    "profile_checked":"no_result",
                    "candidate_match_unresolved":"match_unresolved",
                    "error":"request_error",
                },
                "found_evidence_files":[str(seattle_evidence)],
                "found_evidence_key":"candidate_year_id",
                "aggregation_precedence":PRECEDENCE,
                "parser_version":"seattle-legacy-v1",
            },
        },
    ]
    return pd.DataFrame(rows), registry, frames


def test_all_four_live_legacy_schemas_map_to_exact_contest_targets(
    live_legacy_schema_fixture,
):
    live_universe, registry, frames = live_legacy_schema_fixture
    targets, ledger, _ = build_source_attempt_ledger(
        live_universe, registry, frames
    )
    assert len(targets) == len(ledger) == 8
    assert not ledger.duplicated(["source_id", "contest_id"]).any()
    outcomes = ledger.set_index(["source_id", "contest_id"])["source_status"].to_dict()
    assert outcomes[("profiles", "bp-2021")] == "found"
    assert outcomes[("pages", "sea-2001")] == "request_error"
    assert outcomes[("nyc", "ny-2005")] == "found"
    assert outcomes[("seattle", "sea-2005")] == "found"
    assert outcomes[("seattle", "sea-2001")] == "request_error"
