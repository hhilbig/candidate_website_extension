"""Check observation dates, roster denominators, and resumable collection cadence."""
from __future__ import annotations

import csv
import json
from copy import deepcopy
from datetime import datetime, timezone

import pytest

from src.collection_2026_outputs import capture_distance_days, capture_side, due_live, pairing_rows, write_outputs


@pytest.fixture
def candidate():
    return {"candidate_id": "TEST1", "candidate": "Example Candidate", "state": "CA", "district": "01",
            "office": "house", "party": "D", "year": 2026, "website_url": "https://example.test/"}


@pytest.fixture
def event():
    return {"event_id": "test-primary", "date": "2026-06-15", "state": "CA", "office": "house",
            "districts": ["*"], "parties": ["D", "R"], "event_type": "primary", "round": "primary",
            "source_url": "https://authority.test/calendar", "verified_source_url": "https://authority.test/calendar",
            "verification_status": "state_verified", "timezones": ["America/Los_Angeles"]}


def page(identifier, timestamp, *, source="wayback", snapshot=None, **extra):
    row = {"page_id": identifier, "candidate_id": "TEST1", "snapshot_id": snapshot or identifier,
           "source": source, "original_url": "https://example.test/", "effective_url": "https://example.test/",
           "captured_at": timestamp, "fetched_at": "2026-09-14T12:00:00+00:00", "page_type": "homepage",
           "depth": 0, "status": "usable", "identity_status": "confirmed", "sha256": "identical-body",
           "n_char": 1000, "html_path": "html/test.html.gz", "text_path": "text/test.txt.gz"}
    row.update(extra)
    return row


def test_local_day_boundaries_and_both_thirty_day_edges(event):
    assert capture_side("2026-05-16T19:00:00Z", event) == "pre"
    assert capture_side("2026-05-15T19:00:00Z", event) == ""
    assert capture_side("2026-06-15T06:59:59Z", event) == "pre"
    assert capture_side("2026-06-15T07:00:00Z", event) == ""
    assert capture_side("2026-06-16T06:59:59Z", event) == ""
    assert capture_side("2026-06-16T07:00:00Z", event) == "post"
    assert capture_side("2026-07-15T19:00:00Z", event) == "post"
    assert capture_side("2026-07-16T19:00:00Z", event) == ""
    assert capture_distance_days("2026-05-16T19:00:00Z", event) == 30


def test_multiple_timezones_exclude_ambiguous_election_day(event):
    event["timezones"] = ["America/Chicago", "America/Denver"]
    assert capture_side("2026-06-15T05:30:00Z", event) == ""
    assert capture_side("2026-06-15T04:30:00Z", event) == "pre"
    assert capture_side("2026-06-16T05:30:00Z", event) == ""
    assert capture_side("2026-06-16T06:30:00Z", event) == "post"


def test_unknown_timezone_and_invalid_dates_are_conservative(event):
    event.pop("timezones")
    assert capture_side("2026-06-14T20:00:00Z", event) == ""
    assert capture_side("2026-06-13T12:00:00Z", event) == "pre"
    assert capture_side("2026-06-16T04:00:00Z", event) == ""
    assert capture_side("2026-06-17T12:00:00Z", event) == "post"
    assert capture_side("2026-06-13T12:00:00", event) == ""
    assert capture_side("invalid", event) == ""
    assert capture_distance_days(None, event) is None


def test_nearest_homepages_keep_identical_repeat_observations_and_mixed_source(candidate, event):
    pages = [page("older", "2026-06-13T19:00:00Z"), page("pre", "2026-06-14T19:00:00Z"),
             page("post", "2026-06-16T19:00:00Z", source="live"), page("later", "2026-06-20T19:00:00Z")]
    row = pairing_rows([candidate], [event], pages)[0]
    assert row["coverage_status"] == "paired"
    assert row["pre_homepage_page_id"] == "pre"
    assert row["post_homepage_page_id"] == "post"
    assert row["mixed_source_pair"] is True
    assert row["pair_homepage_span_days"] == 2
    assert row["participation_status"] == "unknown"
    assert len(pages) == 4


def test_archived_subpage_effective_timestamp_cannot_cross_election(candidate, event):
    pages = [page("pre", "2026-06-14T19:00:00Z", snapshot="site"),
             page("issues", "2026-06-16T19:00:00Z", snapshot="site", page_type="issues", depth=1,
                  requested_timestamp="20260614190000"),
             page("bio", "2026-06-14T22:00:00Z", snapshot="site", page_type="biography", depth=1)]
    row = pairing_rows([candidate], [event], pages)[0]
    assert row["pre_page_ids"] == ["pre", "bio"]
    assert row["pre_page_span_hours"] == 3
    assert row["coverage_status"] == "pre_only"
    assert row["post_missing_reason"] == "subpage_only"


def test_nearest_failed_capture_falls_back_and_identity_is_required(candidate, event):
    pages = [page("farther", "2026-06-12T19:00:00Z"),
             page("failed", "2026-06-14T19:00:00Z", status="http_error"),
             page("unknown", "2026-06-14T20:00:00Z", identity_status="unresolved"),
             page("rejected", "2026-06-16T19:00:00Z", identity_status="rejected")]
    row = pairing_rows([candidate], [event], pages)[0]
    assert row["pre_homepage_page_id"] == "farther"
    assert row["coverage_status"] == "pre_only"
    assert row["post_missing_reason"] == "unusable_content"


def test_primary_runoff_has_separate_pair_and_nonparticipant_remains(candidate, event):
    runoff = dict(event, event_id="test-runoff", date="2026-07-06", event_type="primary_runoff", round="runoff")
    candidate["event_participation"] = {"test-runoff": {
        "status": "not_participating", "source_url": "https://authority.test/result"}}
    pages = [page("pre", "2026-06-14T19:00:00Z"), page("post", "2026-06-16T19:00:00Z")]
    rows = pairing_rows([candidate], [event, runoff], pages)
    assert len(rows) == 2
    assert rows[0]["coverage_status"] == "paired"
    assert rows[1]["comparison_eligible"] is False
    assert rows[1]["coverage_status"] == "uncovered"
    assert rows[1]["pre_missing_reason"] == "confirmed_nonparticipant"


def test_missing_url_and_unsourced_candidate_identity_stay_explicit(candidate, event):
    candidate["website_url"] = ""
    row = pairing_rows([candidate], [event], [])[0]
    assert row["coverage_status"] == "uncovered"
    assert row["pre_missing_reason"] == "missing_website"
    candidate["website_url"] = "https://example.test/"
    unknown = page("unknown", "2026-06-14T19:00:00Z", status="identity_unresolved", identity_status="unresolved")
    row = pairing_rows([candidate], [event], [unknown])[0]
    assert row["pre_missing_reason"] == "identity_unresolved"


@pytest.mark.parametrize("failure", ["http_error", "request_error", "blocked", "oversize"])
def test_failed_fetch_is_not_mislabeled_identity_failure(candidate, event, failure):
    failed = page("failed", "2026-06-14T19:00:00Z", status=failure, identity_status="unresolved")
    row = pairing_rows([candidate], [event], [failed])[0]
    assert row["pre_missing_reason"] == "failed_requests"


def test_duplicate_ids_fail_instead_of_inflating_denominators(candidate, event):
    with pytest.raises(ValueError, match="candidate_id"):
        pairing_rows([candidate, candidate], [event], [])
    with pytest.raises(ValueError, match="event_id"):
        pairing_rows([candidate], [event, event], [])


def test_due_initial_monthly_targeted_and_missing_url(candidate, event):
    now = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
    assert due_live(candidate, [event], [], now)
    pages = [page("old", "2026-05-14T12:00:00Z", source="live")]
    assert not due_live(candidate, [event], pages, now)
    assert not due_live(candidate, [event], pages, datetime(2026, 5, 21, 12, tzinfo=timezone.utc))
    assert due_live(candidate, [event], pages, datetime(2026, 6, 13, 12, tzinfo=timezone.utc))
    pages = [page("yesterday", "2026-06-10T12:00:00Z", source="live")]
    assert not due_live(candidate, [event], pages, datetime(2026, 6, 11, 12, tzinfo=timezone.utc))
    assert not due_live(candidate, [event], pages, datetime(2026, 6, 10, 16, tzinfo=timezone.utc))
    candidate["website_url"] = ""
    assert not due_live(candidate, [event], [], now)


def test_failed_or_interrupted_live_root_does_not_advance_due_date(candidate, event):
    now = datetime(2026, 5, 20, 12, tzinfo=timezone.utc)
    pages = [page("failed", "2026-05-20T11:00:00Z", source="live", status="request_error"),
             page("incomplete", "2026-05-20T11:00:00Z", source="live", snapshot_complete=False),
             page("archive", "2026-05-20T11:00:00Z")]
    assert due_live(candidate, [event], pages, now)


def test_due_collection_endpoint_extends_through_later_runoff(candidate, event):
    now = datetime(2026, 12, 20, 12, tzinfo=timezone.utc)
    assert not due_live(candidate, [event], [], now)
    runoff = dict(event, event_id="test-late-runoff", date="2026-12-12", event_type="general_runoff", round="runoff")
    assert due_live(candidate, [event, runoff], [], now)
    assert not due_live(candidate, [event, runoff], [], datetime(2026, 12, 29, 12, tzinfo=timezone.utc))


def test_collection_end_date_caps_a_later_runoff(candidate, event):
    """Hanno set 2026-11-17 on 2026-09-30: a December runoff no longer extends collection."""
    from datetime import date
    runoff = dict(event, event_id="test-late-runoff", date="2026-12-12", event_type="general_runoff", round="runoff")
    end = date(2026, 11, 17)
    assert due_live(candidate, [event, runoff], [], datetime(2026, 11, 17, 18, tzinfo=timezone.utc), end)
    assert not due_live(candidate, [event, runoff], [], datetime(2026, 12, 11, 18, tzinfo=timezone.utc), end)


def test_nonparticipant_does_not_create_daily_cadence(candidate, event):
    candidate["event_participation"] = {event["event_id"]: {
        "status": "not_on_ballot", "source_url": "https://authority.test/ballot"}}
    pages = [page("yesterday", "2026-06-10T12:00:00Z", source="live")]
    assert not due_live(candidate, [event], pages, datetime(2026, 6, 11, 12, tzinfo=timezone.utc))


def test_exports_keep_full_denominator_and_distinguish_missing_archive_failure(tmp_path, candidate, event):
    no_url = dict(candidate, candidate_id="TEST2", website_url="")
    failed = dict(candidate, candidate_id="TEST3")
    no_event = dict(candidate, candidate_id="TEST4", state="AA")
    work = [{"key": "empty", "kind": "cdx_inventory", "candidate_id": "TEST1", "status": "empty"},
            {"key": "failed", "kind": "cdx_inventory", "candidate_id": "TEST3", "status": "retryable"}]
    report = write_outputs(tmp_path, [candidate, no_url, failed, no_event], [event], [], work)
    assert report["candidate_count"] == 4
    assert report["candidate_event_count"] == 3
    assert report["coverage_counts"]["uncovered"] == 3
    assert report["candidate_without_applicable_event_count"] == 1
    assert report["every_work_item_has_status"]
    assert not report["every_work_item_terminal"]
    rows = list(csv.DictReader((tmp_path / "candidate_event_pairs.csv").open()))
    assert rows[0]["pre_missing_reason"] == "missing_archives"
    assert rows[1]["pre_missing_reason"] == "missing_website"
    assert rows[2]["pre_missing_reason"] == "failed_requests"
    assert len(list(csv.DictReader((tmp_path / "candidate_coverage.csv").open()))) == 4
    assert (tmp_path / "page_corpus.csv").read_text().startswith("page_id,")
    assert json.loads((tmp_path / "coverage_report.json").read_text())["candidate_count"] == 4


def test_exports_are_repeatable_preserve_raw_errors_and_write_real_parquet(tmp_path, candidate, event):
    pytest.importorskip("pyarrow.parquet")
    import pyarrow.parquet as pq
    pages = [page("pre", "2026-06-14T19:00:00Z"), page("post", "2026-06-16T19:00:00Z"),
             page("failure", None, status="request_error", errors={"message": "timeout"})]
    original = deepcopy(pages)
    report = write_outputs(tmp_path, [candidate], [event], pages, [])
    csv_before = (tmp_path / "page_corpus.csv").read_bytes()
    write_outputs(tmp_path, [candidate], [event], pages, [])
    assert (tmp_path / "page_corpus.csv").read_bytes() == csv_before
    assert pages == original
    table = pq.read_table(tmp_path / "page_corpus.parquet")
    assert table.num_rows == 3
    assert "request_error" in table["status"].to_pylist()
    assert report["same_source_pair_count"] == 1
    assert report["mixed_source_pair_count"] == 0
    assert not list(tmp_path.glob(".*"))


def test_incomplete_homepage_does_not_create_main_pair_and_is_reported(tmp_path, candidate, event):
    incomplete = page('interrupted', '2026-06-14T19:00:00Z', source='live', snapshot_complete=False)
    row = pairing_rows([candidate], [event], [incomplete])[0]
    assert row['coverage_status'] == 'uncovered'
    assert row['pre_missing_reason'] == 'incomplete_snapshot'
    report = write_outputs(tmp_path, [candidate], [event], [incomplete], [])
    assert report['incomplete_snapshot_count'] == 1


def test_superseded_work_requires_completed_replacement(tmp_path, candidate, event):
    work = [{'key': 'old', 'kind': 'cdx_inventory', 'candidate_id': 'TEST1', 'status': 'superseded',
             'result': {'superseded_by': 'new'}},
            {'key': 'new', 'kind': 'cdx_inventory', 'candidate_id': 'TEST1', 'status': 'empty', 'result': {}}]
    report = write_outputs(tmp_path, [candidate], [event], [], work)
    assert report['every_work_item_terminal']
    rows = list(csv.DictReader((tmp_path / 'candidate_event_pairs.csv').open()))
    assert rows[0]['pre_missing_reason'] == 'missing_archives'
    work[0]['result']['superseded_by'] = 'missing'
    report = write_outputs(tmp_path, [candidate], [event], [], work)
    assert not report['every_work_item_terminal']


def test_targeted_copies_before_after_and_no_same_day_repeat(candidate, event):
    pages = [page("recent", "2026-06-12T19:00:00Z", source="live")]
    assert due_live(candidate, [event], pages, datetime(2026, 6, 14, 19, tzinfo=timezone.utc))
    pages = [page("pre", "2026-06-14T19:00:00Z", source="live")]
    assert not due_live(candidate, [event], pages, datetime(2026, 6, 14, 21, tzinfo=timezone.utc))
    assert not due_live(candidate, [event], pages, datetime(2026, 6, 15, 19, tzinfo=timezone.utc))
    assert due_live(candidate, [event], pages, datetime(2026, 6, 16, 19, tzinfo=timezone.utc))
    pages = [page("post", "2026-06-16T19:00:00Z", source="live")]
    assert not due_live(candidate, [event], pages, datetime(2026, 6, 17, 19, tzinfo=timezone.utc))


def test_general_election_gets_copies_on_the_day_before_and_after(candidate, event):
    """Hanno added the general election to the targeted copies on 2026-10-24."""
    from datetime import date
    general = dict(event, event_id="test-general", date="2026-11-03", event_type="general", round="general")
    end = date(2026, 11, 17)
    pages = [page("late_october", "2026-10-22T19:00:00Z", source="live")]
    assert not due_live(candidate, [general], pages, datetime(2026, 11, 1, 19, tzinfo=timezone.utc), end)
    assert due_live(candidate, [general], pages, datetime(2026, 11, 2, 19, tzinfo=timezone.utc), end)
    pages = [page("pre", "2026-11-02T19:00:00Z", source="live")]
    assert not due_live(candidate, [general], pages, datetime(2026, 11, 3, 19, tzinfo=timezone.utc), end)
    assert due_live(candidate, [general], pages, datetime(2026, 11, 4, 19, tzinfo=timezone.utc), end)
    pages = [page("post", "2026-11-04T19:00:00Z", source="live")]
    assert not due_live(candidate, [general], pages, datetime(2026, 11, 5, 19, tzinfo=timezone.utc), end)
    assert not due_live(candidate, [general], pages, datetime(2026, 11, 17, 19, tzinfo=timezone.utc), end)
    candidate["event_participation"] = {general["event_id"]: {
        "status": "not_on_ballot", "source_url": "https://authority.test/ballot"}}
    pages = [page("late_october", "2026-10-22T19:00:00Z", source="live")]
    assert not due_live(candidate, [general], pages, datetime(2026, 11, 2, 19, tzinfo=timezone.utc), end)
