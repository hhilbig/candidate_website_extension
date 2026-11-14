from __future__ import annotations

import pandas as pd
import pytest

from scripts.build_universe_provenance import (
    build_candidate_universe,
    build_candidate_year_provenance,
    normalize_website_domain,
    normalize_website_url,
    summarize_raw_dataframe,
    website_cycle_id,
)


def _fec_records() -> pd.DataFrame:
    rows = [
        # Five ordinary House rows, one retained impossible source district.
        ("HGOOD", "GOOD, ALICE", "DEM", "2018", "CA", "H", "01", "C", "C", 2018),
        ("HNOURL", "NOURL, NORA", "REP", "2018", "CA", "H", "02", "C", "N", 2018),
        ("HINVALID", "INVALID, IVAN", "DEM", "2018", "CA", "H", "03", "C", "N", 2018),
        ("HVALID", "VALID, VERA", "REP", "2018", "CA", "H", "04", "C", "N", 2018),
        ("HGEOBAD", "GEOBAD, GINA", "DEM", "2018", "AL", "H", "88", "C", "N", 2018),
        # Explicitly excluded state-legislative record in the FEC cache.
        ("HTN", "STATE, SAM", "DEM", "2022", "TN", "H", "92", "C", "N", 2022),
        # One in-scope Senate record and one state-year without an election.
        ("SGOOD", "SENATE, SUE", "REP", "2002", "IA", "S", "03", "C", "C", 2002),
        ("SNORACE", "NORACE, NED", "REP", "2002", "CA", "S", "00", "C", "N", 2002),
        # House years before 2018 are outside this extension's population.
        ("HOLD", "OLD, OTTO", "DEM", "2016", "CA", "H", "01", "C", "C", 2016),
        # The reviewed exception appears in the 2020 cache with an older
        # candidate-election-year and is therefore not a same-year FEC row.
        ("HEXCEPT", "EXCEPT, ERIN", "REP", "2018", "AZ", "H", "02", "O", "P", 2020),
    ]
    columns = [
        "cand_id", "cand_name", "cand_pty_affiliation", "cand_election_yr",
        "cand_office_st", "cand_office", "cand_office_district", "cand_ici",
        "cand_status", "file_year",
    ]
    return pd.DataFrame(rows, columns=columns)


def _identity_overrides() -> pd.DataFrame:
    return pd.DataFrame([{
        "year": 2020,
        "office": "house",
        "state": "AZ",
        "source_cand_id": "HEXCEPT",
        "canonical_cand_id": "HEXCEPT",
        "action": "retain_ballot_override",
        "reason": "reviewed ballot exception",
    }])


def _universe_adjudication() -> pd.DataFrame:
    return pd.DataFrame([{
        "year": 2022,
        "office": "house",
        "state": "TN",
        "cand_id": "HTN",
        "action": "exclude",
        "reason": "state legislative campaign",
    }])


def _capture_adjudication() -> pd.DataFrame:
    return pd.DataFrame([{
        "year": 2018,
        "office": "house",
        "state": "CA",
        "cand_id": "HINVALID",
        "action": "exclude",
        "reason": "wrong website",
    }])


def _capture_date_adjudication() -> pd.DataFrame:
    return pd.DataFrame([{
        "year": 2020,
        "office": "house",
        "state": "AZ",
        "cand_id": "HEXCEPT",
        "action": "retain_date_range",
        "min_snap_day": "20201201",
        "max_snap_day": "20201201",
        "reason": "later dates changed office",
    }])


def _historical_roster() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "candidate_cycle_id": "HGOOD-2018",
            "website_url": "HTTPS://WWW.GoodExample.COM/Campaign/",
            "url_status": "valid",
            "on_ballot": True,
            "general_votes": 100,
        },
        {
            "candidate_cycle_id": "HINVALID-2018",
            "website_url": "https://none",
            "url_status": "missing_or_invalid",
            "on_ballot": False,
            "general_votes": pd.NA,
        },
        {
            "candidate_cycle_id": "HVALID-2018",
            "website_url": "https://www.valid-example.org/",
            "url_status": "valid",
            "on_ballot": False,
            "general_votes": pd.NA,
        },
        {
            "candidate_cycle_id": "SGOOD-2002",
            "website_url": "https://facebook.com/not-a-campaign-url",
            "url_status": "missing_or_invalid",
            "on_ballot": False,
            "general_votes": pd.NA,
        },
        {
            "candidate_cycle_id": "HEXCEPT-2020",
            "website_url": "http://except.example.com",
            "url_status": "valid",
            "on_ballot": True,
            "general_votes": 200,
        },
    ])


def _ballot_outcomes(ids: list[str]) -> pd.DataFrame:
    return pd.DataFrame({
        "candidate_cycle_id": ids,
        "on_ballot": False,
        "general_votes": pd.NA,
    })


def _universe(observed: set[str] | None = None) -> pd.DataFrame:
    expected_ids = [
        "HGEOBAD-2018", "HGOOD-2018", "HINVALID-2018", "HNOURL-2018",
        "HVALID-2018", "SGOOD-2002", "HEXCEPT-2020",
    ]
    return build_candidate_universe(
        fec_records=_fec_records(),
        senate_races={(2002, "IA")},
        identity_overrides=_identity_overrides(),
        universe_adjudication=_universe_adjudication(),
        capture_adjudication=_capture_adjudication(),
        historical_roster=_historical_roster(),
        observed_candidate_ids=observed or {"HGOOD-2018", "HEXCEPT-2020"},
        ballot_outcomes=_ballot_outcomes(expected_ids),
    )


def test_rectangular_universe_is_fec_first_and_id_keyed():
    universe = _universe()
    assert len(universe) == 7
    assert universe.candidate_cycle_id.is_unique
    assert "HTN-2022" not in set(universe.candidate_cycle_id)
    assert "SNORACE-2002" not in set(universe.candidate_cycle_id)
    assert "HOLD-2016" not in set(universe.candidate_cycle_id)
    assert universe.set_index("candidate_cycle_id").loc[
        "HEXCEPT-2020", "universe_source"
    ] == "ballot_override"

    by_id = universe.set_index("candidate_cycle_id")
    # Absence from the old URL-bearing roster does not remove population rows
    # or falsely become a historical "not found" result.
    assert by_id.loc["HNOURL-2018", "url_discovery_status"] == "legacy_no_url_record"
    assert not by_id.loc["HNOURL-2018", "historical_attempt_record"]
    assert pd.isna(by_id.loc["HNOURL-2018", "url_source"])
    assert by_id.loc["HVALID-2018", "url_source"] == "legacy_unknown"

    # Existing URL bytes survive; normalized fields are separate.
    assert by_id.loc["HGOOD-2018", "website_url"] == (
        "HTTPS://WWW.GoodExample.COM/Campaign/"
    )
    assert by_id.loc["HGOOD-2018", "normalized_website_url"] == (
        "https://goodexample.com/Campaign"
    )
    assert by_id.loc["HGOOD-2018", "website_cycle_id"] == "goodexample.com-2018"
    assert by_id.loc["HVALID-2018", "has_url"]
    assert not by_id.loc["HINVALID-2018", "has_url"]

    # Impossible source geography remains auditable without assigning a false
    # federal district or changing population membership.
    assert by_id.loc["HGEOBAD-2018", "source_district"] == "88"
    assert pd.isna(by_id.loc["HGEOBAD-2018", "district"])
    assert by_id.loc["HGEOBAD-2018", "geography_status"] == (
        "invalid_house_district"
    )
    assert pd.isna(by_id.loc["SGOOD-2002", "district"])
    assert by_id.loc["SGOOD-2002", "geography_status"] == "senate_not_applicable"

    # Reviewed historical ballot evidence is not overwritten by a fresh fuzzy
    # match supplied for otherwise-unmatched population rows.
    assert by_id.loc["HGOOD-2018", "on_ballot"]
    assert by_id.loc["HGOOD-2018", "general_votes"] == 100
    assert not by_id.loc["HVALID-2018", "on_ballot"]
    assert pd.isna(by_id.loc["HVALID-2018", "general_votes"])


def test_observed_release_candidates_must_map_to_population_by_id():
    with pytest.raises(ValueError, match="outside the approved universe"):
        _universe({"HGOOD-2018", "NOT-IN-UNIVERSE-2018"})


def test_website_normalization_is_deterministic_and_not_a_person_id():
    raw = "HTTPS://WWW2.Example.COM/Path/#fragment"
    assert normalize_website_url(raw) == "https://example.com/Path"
    assert normalize_website_domain(raw) == "example.com"
    assert website_cycle_id(raw, 2024) == "example.com-2024"
    assert website_cycle_id("https://none", 2024) is None


def test_raw_provenance_digest_is_invariant_to_input_order():
    raw = pd.DataFrame([
        {
            "candidate_cycle_id": "HGOOD-2018", "year": 2018,
            "date": "20181001120000", "urlkey": "https://goodexample.com",
            "snap_url": ("https://web.archive.org/web/20181001120000/"
                         "https://www.goodexample.com/"),
            "page_type": "homepage", "text_snap_content": "Home text",
        },
        {
            "candidate_cycle_id": "HGOOD-2018", "year": 2018,
            "date": "20181001130000", "urlkey": "https://goodexample.com",
            "snap_url": ("https://web.archive.org/web/20181001130000/"
                         "https://www.goodexample.com/issues"),
            "page_type": "issues", "text_snap_content": "Issue text",
        },
    ])
    first = summarize_raw_dataframe(raw).iloc[0]
    second = summarize_raw_dataframe(raw.iloc[::-1]).iloc[0]
    assert first.page_text_digest_sha256 == second.page_text_digest_sha256
    assert first.n_raw_page_rows == 2
    assert first.n_observed_snapshot_timestamps == 2
    assert first.n_observed_snapshot_days == 1
    assert first.n_distinct_page_urls == 2
    assert first.source_hosts == "goodexample.com"


def test_raw_provenance_tolerates_an_impossible_legacy_port():
    raw = pd.DataFrame([{
        "candidate_cycle_id": "HGOOD-2018", "year": 2018,
        "date": "20181001120000", "urlkey": "https://goodexample.com",
        "snap_url": ("https://web.archive.org/web/20181001120000/"
                     "http://goodexample.com:785102/issues"),
        "page_type": "issues", "text_snap_content": "Issue text",
    }])
    summary = summarize_raw_dataframe(raw).iloc[0]
    assert summary.n_distinct_page_urls == 1
    assert summary.source_hosts == "goodexample.com"


def test_provenance_statuses_explain_capture_and_measure_observability():
    universe = _universe()
    raw_pages = pd.DataFrame([
        {
            "candidate_cycle_id": "HGOOD-2018", "year": 2018,
            "date": "20181001120000", "urlkey": "https://goodexample.com",
            "snap_url": ("https://web.archive.org/web/20181001120000/"
                         "https://goodexample.com/"),
            "page_type": "homepage", "text_snap_content": "Usable text",
        },
        {
            "candidate_cycle_id": "HEXCEPT-2020", "year": 2020,
            "date": "20201201120000", "urlkey": "http://except.example.com",
            "snap_url": ("https://web.archive.org/web/20201201120000/"
                         "http://except.example.com/issues"),
            "page_type": "issues", "text_snap_content": "Short text",
        },
    ])
    raw_summary = summarize_raw_dataframe(raw_pages)
    panel = pd.DataFrame([
        {"candidate_cycle_id": "HGOOD-2018", "sel_date": "20181001120000",
         "text_quality": "usable"},
        {"candidate_cycle_id": "HEXCEPT-2020", "sel_date": "20201201120000",
         "text_quality": "thin"},
    ])
    coded = pd.DataFrame([
        {"candidate_cycle_id": "HGOOD-2018", "icpsr_n_char": 10.0,
         "icpsr_n_char_home": 5.0, "icpsr_n_char_preelec": 2.0,
         "icpsr_topic_Test": 1.0},
        {"candidate_cycle_id": "HEXCEPT-2020", "icpsr_n_char": pd.NA,
         "icpsr_n_char_home": pd.NA, "icpsr_n_char_preelec": pd.NA,
         "icpsr_topic_Test": pd.NA},
    ])
    provenance = build_candidate_year_provenance(
        universe, raw_summary, panel, coded, _capture_adjudication(),
        _capture_date_adjudication(),
    ).set_index("candidate_cycle_id")

    assert provenance.loc["HGOOD-2018", "observability_status"] == (
        "captured_usable_text"
    )
    assert provenance.loc["HEXCEPT-2020", "observability_status"] == (
        "captured_thin_text"
    )
    assert provenance.loc["HVALID-2018", "observability_status"] == (
        "valid_url_no_capture_unknown_reason"
    )
    assert provenance.loc["HNOURL-2018", "observability_status"] == (
        "legacy_no_url_record"
    )
    assert provenance.loc["SGOOD-2002", "observability_status"] == (
        "invalid_or_placeholder_url"
    )
    assert provenance.loc["HINVALID-2018", "observability_status"] == (
        "invalid_capture_excluded"
    )

    assert provenance.loc["HGOOD-2018", "all_text_measure_status"] == "observed"
    assert provenance.loc["HGOOD-2018", "homepage_measure_status"] == "observed"
    assert provenance.loc["HGOOD-2018", "preelection_measure_status"] == "observed"
    assert provenance.loc["HGOOD-2018", "topic_measure_status"] == "observed"
    assert provenance.loc["HEXCEPT-2020", "all_text_measure_status"] == (
        "insufficient_clean_text"
    )
    assert provenance.loc["HEXCEPT-2020", "homepage_measure_status"] == (
        "scope_not_observed"
    )
    assert provenance.loc["HEXCEPT-2020", "preelection_measure_status"] == (
        "scope_not_observed"
    )
    assert provenance.loc["HEXCEPT-2020", "topic_measure_status"] == (
        "insufficient_clean_text"
    )
    assert provenance.loc[
        "HEXCEPT-2020", "capture_date_adjudication_status"
    ] == "retained_approved_date_range"
    assert provenance.loc[
        "HEXCEPT-2020", "capture_date_min_snap_day"
    ] == "20201201"
    assert provenance.loc[
        "HEXCEPT-2020", "capture_date_max_snap_day"
    ] == "20201201"
    assert provenance.loc[
        "HEXCEPT-2020", "capture_date_adjudication_reason"
    ] == "later dates changed office"
    assert provenance.loc[
        "HGOOD-2018", "capture_date_adjudication_status"
    ] == "not_reviewed"
    assert pd.isna(provenance.loc[
        "HGOOD-2018", "capture_date_min_snap_day"
    ])

    # The release cannot support more specific negative collection claims.
    assert provenance.cdx_availability_status.eq("unknown").all()
    assert provenance.collection_failure_reason.eq("unknown").all()
    assert provenance.snapshot_cap_binding_status.eq("unknown").all()
    assert provenance.collection_method_version.eq("legacy_mixed_unknown").all()
    assert len(provenance) == len(universe)

    uncorrected_rule = _capture_date_adjudication()
    uncorrected_rule[["min_snap_day", "max_snap_day"]] = "20200105"
    with pytest.raises(ValueError, match="violate approved date range"):
        build_candidate_year_provenance(
            universe, raw_summary, panel, coded, _capture_adjudication(),
            uncorrected_rule,
        )


def test_invalid_capture_cannot_leak_back_into_substantive_provenance():
    universe = _universe()
    bad_raw = pd.DataFrame([{
        "candidate_cycle_id": "HINVALID-2018", "year": 2018,
        "date": "20181001120000", "urlkey": "https://wrong.example",
        "snap_url": "https://wrong.example", "page_type": "homepage",
        "text_snap_content": "Wrong site",
    }])
    with pytest.raises(ValueError, match="invalid captures remain"):
        build_candidate_year_provenance(
            universe,
            summarize_raw_dataframe(bad_raw),
            pd.DataFrame(columns=["candidate_cycle_id"]),
            pd.DataFrame(columns=["candidate_cycle_id"]),
            _capture_adjudication(),
        )
