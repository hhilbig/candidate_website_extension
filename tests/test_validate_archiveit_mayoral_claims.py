import json
from pathlib import Path

import pandas as pd
import requests

import scripts.validate_archiveit_mayoral_claims as validator
from scripts.validate_archiveit_mayoral_claims import (
    VALIDATION_KEY,
    VALIDATION_METHOD_VERSION,
    ArchiveItValidationRequestError,
    Capture,
    collection_timemap_url,
    parse_timemap,
    query_collection_timemap,
    validate_claims,
)


FIXTURES = Path(__file__).parent / "fixtures"
VALID_HTML = (FIXTURES / "archiveit_validation_valid.html").read_text()
MISMATCH_HTML = (FIXTURES / "archiveit_validation_identity_mismatch.html").read_text()


def timemap(name="valid"):
    return parse_timemap(json.loads(
        (FIXTURES / f"archiveit_validation_timemap_{name}.json").read_text()
    ))


def evidence_row(**updates):
    row = {
        "claim_id": "claim-1",
        "source_id": "archive_it_collection_999",
        "exact_normalized_url": "https://AliceForMayor.org/Issues/Plan.HTML?Ref=Mail",
        "collection_id": 999,
        "candidate_year_id": "1__2021",
        "candidate_contest_id": "contest-1__1",
        "contest_id": "contest-1",
        "full_name": "Alice Example",
        "geo_name": "springfield",
        "state_abb": "IL",
        "year": 2021,
        "month": 6,
        "url_type": "candidate_owned_custom_domain",
        "evidence_grade": "C",
        "strict_coverage_eligible": False,
        "temporal_validation_status": "pending_archive_validation",
        "review_status": "pending",
        "acceptance": "",
        "source_snapshot_timestamp": "",
    }
    row.update(updates)
    return row


def run_validation(tmp_path, monkeypatch, evidence, captures=None, html=VALID_HTML):
    captures = timemap() if captures is None else captures
    monkeypatch.setattr(
        validator, "query_collection_timemap",
        lambda *_args, **_kwargs: captures,
    )
    monkeypatch.setattr(
        validator, "fetch_collection_capture",
        lambda *_args, **_kwargs: html,
    )
    monkeypatch.setattr(validator.time, "sleep", lambda *_args: None)
    return validate_claims(
        pd.DataFrame(evidence),
        tmp_path / "validation.csv", tmp_path / "review.csv",
        delay_seconds=0,
    )


def test_grade_a_requires_exact_url_election_capture_identity_mayoral_campaign_and_city(
    tmp_path, monkeypatch,
):
    seen = []
    monkeypatch.setattr(
        validator, "query_collection_timemap",
        lambda collection, url, **_kwargs: seen.append((collection, url)) or timemap(),
    )
    monkeypatch.setattr(
        validator, "fetch_collection_capture",
        lambda *_args, **_kwargs: VALID_HTML,
    )
    monkeypatch.setattr(validator.time, "sleep", lambda *_args: None)
    validation, review = validate_claims(
        pd.DataFrame([evidence_row()]),
        tmp_path / "validation.csv", tmp_path / "review.csv", delay_seconds=0,
    )
    row = validation.iloc[0]
    assert seen == [(999, "https://aliceformayor.org/Issues/Plan.HTML?Ref=Mail")]
    assert tuple(row[column] for column in VALIDATION_KEY) == (
        "claim-1", "archive_it_collection_999",
        "https://AliceForMayor.org/Issues/Plan.HTML?Ref=Mail",
    )
    assert row.nearest_capture_timestamp == "20210614120000"
    assert row.nearest_capture_days_signed == -1
    assert row.within_headline_window
    assert row.within_sensitivity_window
    assert row.candidate_identity_match
    assert row.mayoral_campaign_match
    assert row.city_match
    assert row.domain_reuse_status == "same_candidate_only"
    assert row.proposed_evidence_grade == "A"
    assert row.strict_coverage_eligible
    assert review.iloc[0].evidence_grade == "A"
    assert review.iloc[0].review_status == "pending"
    assert review.iloc[0].source_snapshot_timestamp == "20210614120000"
    assert "B" not in set(validation.proposed_evidence_grade)


def test_mayor_president_is_recognized_as_a_mayoral_campaign():
    html = """
    <html><head><title>Sharon Weston Broome - Mayor President 2016</title></head>
    <body><p>One Baton Rouge United</p><p>Sharon for Mayor-President</p></body></html>
    """
    signals = validator.page_content_signals(
        html, "sharon weston broome", "baton rouge",
    )
    assert signals["candidate_identity_match"]
    assert signals["mayoral_campaign_match"]
    assert signals["city_match"]


def test_no_capture_remains_c_with_explicit_reason(tmp_path, monkeypatch):
    validation, review = run_validation(
        tmp_path, monkeypatch, [evidence_row()], captures=[],
    )
    row = validation.iloc[0]
    assert row.archive_query_status == "checked"
    assert row.capture_fetch_status == "not_attempted_no_capture"
    assert row.validation_reason == "no_collection_capture"
    assert row.proposed_evidence_grade == "C"
    assert not row.strict_coverage_eligible
    assert review.iloc[0].temporal_validation_status == "no_collection_capture"


def test_identity_mismatch_remains_c(tmp_path, monkeypatch):
    validation, _ = run_validation(
        tmp_path, monkeypatch, [evidence_row()],
        captures=[timemap()[1]], html=MISMATCH_HTML,
    )
    row = validation.iloc[0]
    assert not row.candidate_identity_match
    assert row.mayoral_campaign_match
    assert row.city_match
    assert row.validation_reason == "candidate_identity_not_confirmed"
    assert row.proposed_evidence_grade == "C"


def test_missing_month_is_recorded_without_query_or_imputation(tmp_path, monkeypatch):
    monkeypatch.setattr(
        validator, "query_collection_timemap",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not query")),
    )
    monkeypatch.setattr(validator.time, "sleep", lambda *_args: None)
    validation, _ = validate_claims(
        pd.DataFrame([evidence_row(month=pd.NA)]),
        tmp_path / "validation.csv", tmp_path / "review.csv", delay_seconds=0,
    )
    row = validation.iloc[0]
    assert pd.isna(row.month)
    assert row.archive_query_status == "not_attempted_missing_election_month"
    assert row.validation_reason == "missing_election_month_no_imputation"
    assert "no month was imputed" in row.error


def test_conflicting_content_within_two_year_reuse_window_blocks_grade_a(
    tmp_path, monkeypatch,
):
    captures = timemap("reuse")
    monkeypatch.setattr(
        validator, "query_collection_timemap", lambda *_args, **_kwargs: captures,
    )
    monkeypatch.setattr(
        validator, "fetch_collection_capture",
        lambda _collection, capture, **_kwargs: (
            MISMATCH_HTML if capture.timestamp == "20221201090000" else VALID_HTML
        ),
    )
    monkeypatch.setattr(validator.time, "sleep", lambda *_args: None)
    validation, _ = validate_claims(
        pd.DataFrame([evidence_row(exact_normalized_url="https://aliceformayor.org/")]),
        tmp_path / "validation.csv", tmp_path / "review.csv", delay_seconds=0,
    )
    row = validation.iloc[0]
    assert row.within_headline_window
    assert row.reuse_window_start == "2019-06-15"
    assert row.reuse_window_end == "2023-06-15"
    assert row.reuse_captures_after == 1
    assert row.reuse_probe_count == 1
    assert row.reuse_conflict_count == 1
    assert row.domain_reuse_status == "conflicting_content_detected"
    assert row.validation_reason == "domain_reuse_conflict"
    assert row.proposed_evidence_grade == "C"


def test_sensitivity_window_is_reported_but_does_not_replace_headline_rule(
    tmp_path, monkeypatch,
):
    capture = Capture(
        "20200201120000", "https://aliceformayor.org/", "text/html", 200, "old",
    )
    validation, _ = run_validation(
        tmp_path, monkeypatch,
        [evidence_row(exact_normalized_url="https://aliceformayor.org/")],
        captures=[capture],
    )
    row = validation.iloc[0]
    assert row.nearest_capture_days_absolute > 365
    assert not row.within_headline_window
    assert row.within_sensitivity_window
    assert row.validation_reason == "outside_headline_365_within_sensitivity"
    assert row.proposed_evidence_grade == "C"


def test_wayback_wrapper_is_unwrapped_for_collection_query_but_preserved_in_key():
    wrapper = (
        "https://wayback.archive-it.org/999/20210614120000id_/"
        "https://AliceForMayor.org/Issues/Plan.HTML?Ref=Mail"
    )
    url = collection_timemap_url(999, wrapper)
    assert url.startswith("https://wayback.archive-it.org/999/timemap/json?url=")
    assert "%2FIssues%2FPlan.HTML%3FRef%3DMail" in url
    assert "/cdx/" not in url


def test_rerun_prunes_stale_and_old_method_rows(tmp_path, monkeypatch):
    output = tmp_path / "validation.csv"
    prior = pd.DataFrame([
        {
            "claim_id": "stale", "source_id": "archive_it_collection_999",
            "exact_normalized_url": "https://stale.example/",
            "validation_context_hash": "x",
            "validation_method_version": VALIDATION_METHOD_VERSION,
            "archive_query_status": "checked",
        },
        {
            "claim_id": "claim-1", "source_id": "archive_it_collection_999",
            "exact_normalized_url": "https://AliceForMayor.org/Issues/Plan.HTML?Ref=Mail",
            "validation_context_hash": "old-context",
            "validation_method_version": "old_method",
            "archive_query_status": "checked",
        },
    ])
    prior.to_csv(output, index=False)
    monkeypatch.setattr(validator, "query_collection_timemap", lambda *_a, **_k: [])
    monkeypatch.setattr(validator.time, "sleep", lambda *_args: None)
    validation, review = validate_claims(
        pd.DataFrame([evidence_row()]), output, tmp_path / "review.csv",
        delay_seconds=0,
    )
    assert len(validation) == len(review) == 1
    assert validation.iloc[0].claim_id == "claim-1"
    persisted = pd.read_csv(output)
    assert len(persisted) == 1
    assert persisted.iloc[0].validation_method_version == VALIDATION_METHOD_VERSION


def test_request_error_row_is_retried_on_next_run(tmp_path, monkeypatch):
    calls = []

    def fail(*_args, **_kwargs):
        calls.append("fail")
        raise ArchiveItValidationRequestError("temporary")

    monkeypatch.setattr(validator, "query_collection_timemap", fail)
    monkeypatch.setattr(validator.time, "sleep", lambda *_args: None)
    first, _ = validate_claims(
        pd.DataFrame([evidence_row()]),
        tmp_path / "validation.csv", tmp_path / "review.csv", delay_seconds=0,
    )
    assert first.iloc[0].archive_query_status == "request_error"

    monkeypatch.setattr(
        validator, "query_collection_timemap",
        lambda *_args, **_kwargs: calls.append("success") or [],
    )
    second, _ = validate_claims(
        pd.DataFrame([evidence_row()]),
        tmp_path / "validation.csv", tmp_path / "review.csv", delay_seconds=0,
    )
    assert calls == ["fail", "success"]
    assert second.iloc[0].archive_query_status == "checked"


class FakeResponse:
    def __init__(self, status_code, payload, headers=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def test_collection_timemap_query_retries_transient_public_endpoint_failures(monkeypatch):
    payload = json.loads(
        (FIXTURES / "archiveit_validation_timemap_valid.json").read_text()
    )
    session = FakeSession([
        requests.ConnectionError("temporary"), FakeResponse(503, None),
        FakeResponse(200, payload),
    ])
    monkeypatch.setattr(validator.time, "sleep", lambda *_args: None)
    captures = query_collection_timemap(
        999, "https://AliceForMayor.org/Issues/Plan.HTML?Ref=Mail",
        attempts=3, delay_seconds=0, session=session,
    )
    assert len(session.calls) == 3
    assert len(captures) == 4
    assert all("wayback.archive-it.org/999/timemap/json" in call[0] for call in session.calls)


def test_transient_retries_use_exponential_backoff_and_retry_after(monkeypatch):
    payload = json.loads(
        (FIXTURES / "archiveit_validation_timemap_valid.json").read_text()
    )
    session = FakeSession([
        FakeResponse(429, None, {"Retry-After": "7"}),
        FakeResponse(503, None),
        FakeResponse(200, payload),
    ])
    sleeps = []
    monkeypatch.setattr(validator.time, "sleep", sleeps.append)
    captures = query_collection_timemap(
        999, "https://AliceForMayor.org/Issues/Plan.HTML?Ref=Mail",
        attempts=3, delay_seconds=2, session=session,
    )
    assert len(captures) == 4
    assert sleeps == [7.0, 4.0]
