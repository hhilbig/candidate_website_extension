from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pandas as pd
import pytest

import scripts.validate_search_lead_wayback_available_content as validator


EXACT_URL = "https://example.org/Campaign/Mayor?Ref=CityVote&A=UPPER"
SHARD_COUNT = 2


@pytest.mark.parametrize(
    ("name", "city", "heading"),
    [
        ("Al Flowers", "Minneapolis", "Al Flowers for Minneapolis Mayor"),
        ("Ty Walker", "Tulsa", "Ty Walker for Tulsa Mayor 2020"),
        ("Ken Houston", "Oakland", "Ken Houston 4 Mayor of Oakland"),
        ("Kenneth Dudley", "Provo", "Ken Dudley for Provo Mayor"),
        ("Ken Dudley", "Provo", "Kenneth Dudley for Provo Mayor"),
        ("Pat Harris", "St. Paul", "Pat Harris for Mayor of Saint Paul"),
        ("Pat Harris", "Saint Paul", "Pat Harris for Mayor of St. Paul"),
        ("David Briley", "Nashville-Davidson", "David Briley for Mayor of Nashville"),
        ("Lydia Ortega", "Plano", "Lydia Ortega: the mayor Plano needs"),
    ],
)
def test_content_signals_recognize_bounded_campaign_variants(name, city, heading):
    signals = validator.content_signals(f"<h1>{heading}</h1>", name, city)
    assert signals["candidate_identity_match"] is True
    assert signals["mayoral_campaign_match"] is True
    assert signals["city_match"] is True
    assert signals["content_signal_version"] == validator.CONTENT_SIGNAL_VERSION


@pytest.mark.parametrize(
    ("html", "name", "city", "failed_signal"),
    [
        ("Ken Johnson for Provo Mayor", "Kenneth Dudley", "Provo", "candidate_identity_match"),
        ("Jane Doe for Springfield Mayor; visiting Plano", "Jane Doe", "Plano", "mayoral_campaign_match"),
        ("Jane Doe: the mayor Springfield needs. Plano news", "Jane Doe", "Plano", "mayoral_campaign_match"),
        ("Jane Doe. The mayor Plano needs to approve this budget.", "Jane Doe", "Austin", "mayoral_campaign_match"),
        ("Jane Doe for Mayor of Saint Louis", "Jane Doe", "St. Paul", "city_match"),
        ("Jane Doe for Mayor of Davidson", "Jane Doe", "Nashville-Davidson", "city_match"),
        ("Jane Doe: agenda item 4, mayor's office, Plano", "Jane Doe", "Plano", "mayoral_campaign_match"),
        ("Jane Doe interviewed the mayor in Plano", "Jane Doe", "Plano", "mayoral_campaign_match"),
    ],
)
def test_content_signals_do_not_broaden_city_name_or_office_context(
    html, name, city, failed_signal
):
    assert validator.content_signals(html, name, city)[failed_signal] is False


def test_personal_resume_past_campaigns_are_not_current_campaign_evidence():
    html = (
        "<title>Sunshine Linda-marie Grund</title>"
        "<h1>Sunshine Linda-marie Grund</h1><h2>COMPANY EXPERIENCE</h2>"
        "<p>Consulting company</p><h2>PROJECT EXPERIENCE</h2><p>Volunteer projects</p>"
        "<h2>SPECIALIZED EXPERIENCE</h2><p>Orlando Mayoral Campaign, 2015 Candidate</p>"
        "<p>Orlando Mayoral Campaign, 2012 Candidate</p>"
    )
    signals = validator.content_signals(html, "Sunshine Linda-Marie Grund", "Orlando")
    assert signals["candidate_identity_match"] is True
    assert signals["city_match"] is True
    assert signals["resume_only_campaign_history"] is True
    assert signals["mayoral_campaign_match"] is False


@pytest.mark.parametrize("campaign_context", [
    "<title>Jane Doe for Mayor of Plano</title>",
    "<p>Paid for by Jane Doe for Mayor of Plano</p>",
    "<p>Join our campaign. Jane Doe for Mayor of Plano</p>",
])
def test_campaign_page_can_include_a_resume(campaign_context):
    html = campaign_context + "<h2>WORK EXPERIENCE</h2><h2>PROJECT EXPERIENCE</h2>"
    signals = validator.content_signals(html, "Jane Doe", "Plano")
    assert signals["mayoral_campaign_match"] is True
    assert signals["resume_only_campaign_history"] is False


def _lead(
    *,
    url_claim_id: str = "search-url-claim-1",
    discovery_candidate_year_id: str = "discovery-candidate__2018",
    discovery_candidate_contest_id: str = "contest__discovery",
    candidate_year_id: str = "attributed-candidate__2018",
    candidate_contest_id: str = "contest__attributed",
    exact_url: str = EXACT_URL,
    full_name: str = "Jane Doe",
    city: str = "Metroville",
    year: int = 2018,
    month: object = 5,
    url_type: str = "custom_domain",
    attribution_status: str = "identity_supported",
) -> dict[str, object]:
    row: dict[str, object] = {
        column: "" for column in validator.FROZEN_LEAD_COLUMNS
    }
    key = validator.validation_key(url_claim_id, candidate_year_id, exact_url)
    digest = validator.validation_hash(key)
    row.update(
        {
            "freeze_contract_version": validator.FREEZE_CONTRACT_VERSION,
            "validation_method_version": validator.VALIDATION_METHOD_VERSION,
            "validation_key": key,
            "validation_hash": digest,
            "lead_record_hash": "a" * 64,
            "candidate_context_hash": "b" * 64,
            "validation_shard_id": int(digest, 16) % SHARD_COUNT,
            "validation_shard_count": SHARD_COUNT,
            "eligible_for_temporal_validation": True,
            "eligibility_reason": (
                "coverage_url_type_and_identity_supported_pending_grade_d"
            ),
            "contract_version": (
                "mayoral-search-source-expansion-2010-2021-v1"
            ),
            "url_claim_id": url_claim_id,
            "url_entity_id": f"entity-{url_claim_id}",
            "result_claim_id": f"result-{url_claim_id}",
            "query_attempt_id": f"query-{url_claim_id}",
            "page_link_id": f"link-{url_claim_id}",
            "page_id": f"page-{url_claim_id}",
            "source_page_url": "https://citynews.example/elections/2018",
            "discovery_candidate_year_id": discovery_candidate_year_id,
            "discovery_candidate_contest_id": discovery_candidate_contest_id,
            "candidate_year_id": candidate_year_id,
            "candidate_contest_id": candidate_contest_id,
            "contest": "metroville__2018__mayor",
            "full_name": full_name,
            "geo_name": city,
            "state_abb": "XY",
            "year": year,
            "month": month,
            "in_candidate_screen": True,
            "raw_url": exact_url,
            "exact_normalized_url": exact_url,
            "site_root_url": "https://example.org/",
            "registrable_domain": "example.org",
            "url_type": url_type,
            "candidate_attribution_status": attribution_status,
            "candidate_attribution_reason": (
                "link_context_full_name_alias_unique_in_contest"
            ),
            "matched_candidate_ids_json": json.dumps([candidate_year_id]),
            "source_locator": "anchor:1",
            "extraction_method": "html_hyperlink",
            "evidence_text": "Jane Doe official campaign website",
            "evidence_grade": "D",
            "strict_coverage_eligible": False,
            "review_status": "pending",
            "acceptance": "",
            "parser_version": "search-source-page-parser-v2",
        }
    )
    return row


def _frame(*rows: dict[str, object]) -> pd.DataFrame:
    return pd.DataFrame(list(rows), columns=validator.FROZEN_LEAD_COLUMNS)


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_bundle(
    directory: Path, frame: pd.DataFrame
) -> tuple[Path, Path]:
    directory.mkdir(parents=True, exist_ok=True)
    leads_path = directory / "eligible_leads.csv"
    manifest_path = directory / "manifest.json"
    frame.to_csv(leads_path, index=False, lineterminator="\n")
    manifest = {
        "manifest_version": validator.FREEZE_MANIFEST_VERSION,
        "freeze_contract_version": validator.FREEZE_CONTRACT_VERSION,
        "validation_method_version": validator.VALIDATION_METHOD_VERSION,
        "key_definition": (
            "url_claim_id + U+001F + candidate_year_id + U+001F + "
            "exact_normalized_url"
        ),
        "required_identity_status": validator.IDENTITY_SUPPORTED_STATUS,
        "validation_shard_count": SHARD_COUNT,
        "coverage_url_types": sorted(validator.ELIGIBLE_URL_TYPES),
        "eligible_rows": len(frame),
        "validation_shard_rows": {
            str(shard_id): int(
                frame["validation_shard_id"].astype(int).eq(shard_id).sum()
            )
            for shard_id in range(SHARD_COUNT)
        },
        "eligible_output": {
            "columns": validator.FROZEN_LEAD_COLUMNS,
            "path": str(leads_path),
            "bytes": leads_path.stat().st_size,
            "sha256": _sha256_file(leads_path),
            "rows": len(frame),
        },
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return leads_path, manifest_path


def _capture(
    timestamp: str = "20180515120000",
    *,
    exact_url: str = EXACT_URL,
) -> validator.AvailableCapture:
    return validator.AvailableCapture(
        timestamp=timestamp,
        url=f"https://web.archive.org/web/{timestamp}/{exact_url}",
        status=200,
    )


class _ReplayResponse:
    def __init__(
        self,
        status_code: int,
        *,
        url: str,
        html: str = "",
        content_type: str = "text/html; charset=utf-8",
    ) -> None:
        self.status_code = status_code
        self.url = url
        self.text = html
        self.content = html.encode("utf-8")
        self.headers = {"content-type": content_type}


class _ReplaySession:
    def __init__(self, responses: list[_ReplayResponse]) -> None:
        self.responses = list(responses)
        self.calls: list[tuple[str, dict[str, object]]] = []

    def get(self, url: str, **kwargs: object) -> _ReplayResponse:
        self.calls.append((url, kwargs))
        if not self.responses:
            raise AssertionError(f"unexpected replay request: {url}")
        return self.responses.pop(0)


def _fetched_content(
    capture: validator.AvailableCapture,
    html: str,
    *,
    replay_source: str = "archive_it_all_fallback",
    effective_timestamp: str | None = None,
) -> validator.FetchedArchiveContent:
    effective = effective_timestamp or capture.timestamp
    primary_url = capture.url.replace(
        f"/web/{capture.timestamp}/", f"/web/{capture.timestamp}id_/"
    )
    if replay_source == "archive_it_all_fallback":
        requested_url = validator.archive_it_fallback_url(capture)
        effective_url = requested_url.replace(capture.timestamp, effective, 1)
        primary_error = "ArchiveAvailableRequestError: HTTP 503"
    else:
        requested_url = primary_url
        effective_url = primary_url.replace(capture.timestamp, effective, 1)
        primary_error = ""
    raw = html.encode("utf-8")
    return validator.FetchedArchiveContent(
        html=html,
        requested_timestamp=capture.timestamp,
        primary_requested_url=primary_url,
        requested_url=requested_url,
        effective_timestamp=effective,
        effective_url=effective_url,
        replay_source=replay_source,
        content_bytes=len(raw),
        content_sha256=hashlib.sha256(raw).hexdigest(),
        primary_error=primary_error,
    )


def _validate_one(row: dict[str, object]) -> dict[str, object]:
    return validator.validate_one_lead(
        row,
        timeout=1,
        attempts=1,
        delay_seconds=0,
        availability_cache={},
        capture_cache={},
    )


def _main_capture_only(
    exact_url: str, requested_timestamp: str, **kwargs: object
) -> validator.AvailableCapture | None:
    assert exact_url == EXACT_URL
    return _capture() if requested_timestamp == "20180515120000" else None


def test_manifest_hash_shard_and_discovery_attribution_provenance_gates(
    tmp_path: Path,
) -> None:
    lead = _lead()
    leads_path, manifest_path = _write_bundle(tmp_path / "valid", _frame(lead))
    loaded = validator.load_frozen_targets(
        leads_path, manifest_path, int(lead["validation_shard_id"])
    )

    assert len(loaded) == 1
    row = loaded.iloc[0]
    assert row.discovery_candidate_year_id != row.candidate_year_id
    assert row.discovery_candidate_contest_id != row.candidate_contest_id
    assert row.validation_key == validator.validation_key(
        lead["url_claim_id"],
        lead["candidate_year_id"],
        lead["exact_normalized_url"],
    )
    assert lead["discovery_candidate_year_id"] not in row.validation_key

    bad_hash = lead.copy()
    bad_hash["validation_hash"] = "0" * 64
    path, manifest = _write_bundle(tmp_path / "bad-hash", _frame(bad_hash))
    with pytest.raises(validator.SearchLeadWaybackValidationError, match="hash"):
        validator.load_frozen_targets(
            path, manifest, int(bad_hash["validation_shard_id"])
        )

    bad_shard = lead.copy()
    bad_shard["validation_shard_id"] = 1 - int(lead["validation_shard_id"])
    path, manifest = _write_bundle(tmp_path / "bad-shard", _frame(bad_shard))
    with pytest.raises(validator.SearchLeadWaybackValidationError, match="shard"):
        validator.load_frozen_targets(
            path, manifest, int(bad_shard["validation_shard_id"])
        )

    bad_record = lead.copy()
    bad_record["lead_record_hash"] = "not-a-hash"
    path, manifest = _write_bundle(tmp_path / "bad-record", _frame(bad_record))
    with pytest.raises(
        validator.SearchLeadWaybackValidationError, match="lead_record_hash"
    ):
        validator.load_frozen_targets(
            path, manifest, int(bad_record["validation_shard_id"])
        )

    for field in (
        "discovery_candidate_year_id",
        "discovery_candidate_contest_id",
        "candidate_year_id",
        "candidate_contest_id",
    ):
        bad_provenance = lead.copy()
        bad_provenance[field] = ""
        path, manifest = _write_bundle(
            tmp_path / f"blank-{field}", _frame(bad_provenance)
        )
        expected_error = "key/hash" if field == "candidate_year_id" else field
        with pytest.raises(
            validator.SearchLeadWaybackValidationError, match=expected_error
        ):
            validator.load_frozen_targets(
                path,
                manifest,
                int(bad_provenance["validation_shard_id"]),
            )


def test_manifest_file_digest_drift_is_rejected(tmp_path: Path) -> None:
    lead = _lead()
    leads_path, manifest_path = _write_bundle(tmp_path, _frame(lead))
    leads_path.write_text(
        leads_path.read_text(encoding="utf-8") + "\n", encoding="utf-8"
    )

    with pytest.raises(
        validator.SearchLeadWaybackValidationError, match="byte count|SHA-256"
    ):
        validator.load_frozen_targets(
            leads_path, manifest_path, int(lead["validation_shard_id"])
        )


def test_archive_available_url_preserves_exact_path_query_and_case() -> None:
    url = validator.archive_available_url(EXACT_URL, "20180515120000")
    parsed = urlsplit(url)
    query = parse_qs(parsed.query)

    assert f"{parsed.scheme}://{parsed.netloc}{parsed.path}" == (
        "https://archive.org/wayback/available"
    )
    assert query == {
        "url": [EXACT_URL],
        "timestamp": ["20180515120000"],
    }
    assert "/Campaign/Mayor?Ref=CityVote&A=UPPER" in query["url"][0]


def test_web_replay_error_triggers_one_bounded_archive_it_all_fallback() -> None:
    capture = _capture()
    primary_url = (
        "https://web.archive.org/web/20180515120000id_/"
        "https://example.org/Campaign/Mayor?Ref=CityVote&A=UPPER"
    )
    fallback_url = (
        "https://wayback.archive-it.org/all/20180515120000id_/"
        "https://example.org/Campaign/Mayor?Ref=CityVote&A=UPPER"
    )
    html = "<html><title>Jane Doe for Mayor of Metroville</title></html>"
    session = _ReplaySession(
        [
            _ReplayResponse(503, url=primary_url),
            _ReplayResponse(200, url=fallback_url, html=html),
        ]
    )

    fetched = validator.fetch_archive_capture(
        capture,
        timeout=1,
        attempts=1,
        delay_seconds=0,
        session=session,
    )

    assert [call[0] for call in session.calls] == [primary_url, fallback_url]
    assert fetched.requested_timestamp == capture.timestamp
    assert fetched.primary_requested_url == primary_url
    assert fetched.requested_url == fallback_url
    assert fetched.effective_timestamp == capture.timestamp
    assert fetched.effective_url == fallback_url
    assert fetched.replay_source == "archive_it_all_fallback"
    assert "HTTP 503" in fetched.primary_error
    assert fetched.html == html


def test_successful_web_replay_never_calls_archive_it_fallback() -> None:
    capture = _capture()
    primary_url = (
        "https://web.archive.org/web/20180515120000id_/"
        "https://example.org/Campaign/Mayor?Ref=CityVote&A=UPPER"
    )
    session = _ReplaySession(
        [
            _ReplayResponse(
                200,
                url=primary_url,
                html="<html><title>Jane Doe for Mayor of Metroville</title></html>",
            )
        ]
    )

    fetched = validator.fetch_archive_capture(
        capture,
        timeout=1,
        attempts=1,
        delay_seconds=0,
        session=session,
    )

    assert [call[0] for call in session.calls] == [primary_url]
    assert fetched.requested_url == fetched.primary_requested_url == primary_url
    assert fetched.effective_url == primary_url
    assert fetched.replay_source == "web_archive"
    assert fetched.primary_error == ""


def test_http_200_archive_it_rate_limit_page_is_a_replay_error() -> None:
    capture = _capture()
    primary_url = validator._capture_fetch_url(capture)
    fallback_url = validator.archive_it_fallback_url(capture)
    html = (
        "<html><title>Rate limit reached</title><h1>Rate limit reached</h1>"
        "<p>You've reached the limit for the number of requests that can be "
        "made in a short period of time. Contact Aitratelimit@archive.org</p></html>"
    )
    session = _ReplaySession([
        _ReplayResponse(503, url=primary_url),
        _ReplayResponse(200, url=fallback_url, html=html),
    ])
    with pytest.raises(validator.ArchiveAvailableRequestError, match="archive_service_rate_limit_page"):
        validator.fetch_archive_capture(
            capture, timeout=1, attempts=1, delay_seconds=0, session=session
        )
    assert validator.archive_response_error(html) == "archive_service_rate_limit_page"


@pytest.mark.parametrize("html", [
    "<title>Page unavailable</title><p>This historical page had an error.</p>",
    "<title>Rate limit reached</title><p>Our campaign donation service is busy.</p>",
    "<title>Jane Doe for Mayor</title><p>Article about the Archive-It rate limit reached message and Aitratelimit@archive.org.</p>",
])
def test_historical_page_is_not_an_archive_service_error(html):
    assert validator.archive_response_error(html) == ""


def test_fallback_redirect_preserves_requested_effective_provenance_and_timing(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    capture = _capture()
    fetched = _fetched_content(
        capture,
        "<html><title>Jane Doe for Mayor of Metroville</title></html>",
        effective_timestamp="20161215120000",
    )
    monkeypatch.setattr(validator, "query_archive_available", _main_capture_only)
    monkeypatch.setattr(
        validator, "fetch_archive_capture", lambda *args, **kwargs: fetched
    )

    result = _validate_one(_lead())

    assert result["capture_fetch_status"] == "fetched_archive_it_fallback"
    assert result["capture_requested_timestamp"] == "20180515120000"
    assert "all/20180515120000id_/" in result["capture_requested_url"]
    assert result["capture_effective_timestamp"] == "20161215120000"
    assert "all/20161215120000id_/" in result["capture_effective_url"]
    assert result["capture_replay_source"] == "archive_it_all_fallback"
    assert result["timing_capture_timestamp"] == "20161215120000"
    assert result["timing_capture_basis"] == "effective_replay_capture"
    assert result["within_headline_window"] is False
    assert result["within_sensitivity_window"] is True
    assert result["proposed_evidence_grade"] == "C"
    assert result["validation_reason"] == "outside_headline_365_within_sensitivity"


@pytest.mark.parametrize(
    "html",
    [
        "<html><title>Alex Roe for Mayor of Metroville</title></html>",
        "<html><title>Jane Doe for City Council in Metroville</title></html>",
        "<html><title>Jane Doe for Mayor of Elsewhere</title></html>",
    ],
)
def test_archive_it_fallback_never_emits_a_without_all_content_signals(
    monkeypatch: pytest.MonkeyPatch, html: str
) -> None:
    capture = _capture()
    monkeypatch.setattr(validator, "query_archive_available", _main_capture_only)
    monkeypatch.setattr(
        validator,
        "fetch_archive_capture",
        lambda *args, **kwargs: _fetched_content(capture, html),
    )

    result = _validate_one(_lead())

    assert result["capture_replay_source"] == "archive_it_all_fallback"
    assert result["proposed_evidence_grade"] == "C"
    assert result["strict_coverage_eligible_result"] is False


def test_reuse_fetch_summary_preserves_requested_and_effective_timestamps(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nearest = _capture()
    side = _capture("20160615120000")

    def query(
        exact_url: str, requested_timestamp: str, **kwargs: object
    ) -> validator.AvailableCapture | None:
        if requested_timestamp == "20180515120000":
            return nearest
        if requested_timestamp == "20160515120000":
            return side
        return None

    def fetch(
        capture: validator.AvailableCapture, **kwargs: object
    ) -> validator.FetchedArchiveContent:
        html = "<html><title>Jane Doe for Mayor of Metroville</title></html>"
        if capture.timestamp == side.timestamp:
            return _fetched_content(
                capture, html, effective_timestamp="20160701120000"
            )
        return _fetched_content(capture, html, replay_source="web_archive")

    monkeypatch.setattr(validator, "query_archive_available", query)
    monkeypatch.setattr(validator, "fetch_archive_capture", fetch)
    result = _validate_one(_lead())
    summaries = json.loads(result["reuse_fetch_summary_json"])

    assert len(summaries) == 1
    assert summaries[0]["requested_timestamp"] == "20160615120000"
    assert summaries[0]["effective_timestamp"] == "20160701120000"
    assert "all/20160615120000id_/" in summaries[0]["requested_url"]
    assert "all/20160701120000id_/" in summaries[0]["effective_url"]
    assert summaries[0]["replay_source"] == "archive_it_all_fallback"


def test_missing_month_makes_zero_archive_requests(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_query(*args: object, **kwargs: object) -> None:
        pytest.fail("missing election month must not issue an archive request")

    monkeypatch.setattr(validator, "query_archive_available", fail_query)
    result = _validate_one(_lead(month=""))

    assert result["availability_http_requests"] == 0
    assert result["availability_status"] == "not_attempted_missing_election_month"
    assert result["capture_fetch_status"] == "not_attempted_missing_election_month"
    assert result["election_reference_date"] == ""
    assert result["validation_reason"] == "missing_election_month_no_imputation"
    assert result["temporal_validation_status"] == "missing_election_month"
    assert result["proposed_evidence_grade"] == "C"
    assert result["strict_coverage_eligible_result"] is False


def test_no_capture_then_capture_retains_capture_and_records_disagreement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    primary_calls = 0

    def query(
        exact_url: str, requested_timestamp: str, **kwargs: object
    ) -> validator.AvailableCapture | None:
        nonlocal primary_calls
        if requested_timestamp == "20180515120000":
            primary_calls += 1
            return None if primary_calls == 1 else _capture()
        return None

    monkeypatch.setattr(validator, "query_archive_available", query)
    monkeypatch.setattr(
        validator,
        "fetch_archive_capture",
        lambda *args, **kwargs: (
            "<html><title>Jane Doe for Mayor of Metroville</title></html>"
        ),
    )
    result = _validate_one(_lead())
    primary_probe = json.loads(result["availability_probe_summary_json"])[0]

    assert primary_calls == 2
    assert result["availability_http_requests"] == 10
    assert result["nearest_capture_available"] is True
    assert result["nearest_capture_timestamp"] == "20180515120000"
    assert primary_probe["status"] == "checked"
    assert primary_probe["response_count"] == 2
    assert primary_probe["response_statuses"] == ["no_capture", "capture"]
    assert primary_probe["disagreement"] is True


def test_two_concordant_no_capture_responses_are_required_for_absence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def no_capture(*args: object, **kwargs: object) -> None:
        nonlocal calls
        calls += 1
        return None

    monkeypatch.setattr(validator, "query_archive_available", no_capture)
    result = _validate_one(_lead())
    probe = json.loads(result["availability_probe_summary_json"])[0]

    assert calls == result["availability_http_requests"] == 2
    assert result["availability_status"] == "no_capture"
    assert result["temporal_validation_status"] == "no_archive_capture"
    assert result["retryable"] is False
    assert probe["status"] == "no_capture"
    assert probe["response_count"] == 2
    assert probe["response_statuses"] == ["no_capture", "no_capture"]
    assert probe["disagreement"] is False


def test_no_capture_then_request_error_is_retryable_not_accepted_absence(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls = 0

    def mixed(*args: object, **kwargs: object) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            return None
        raise validator.ArchiveAvailableRequestError("second response failed")

    monkeypatch.setattr(validator, "query_archive_available", mixed)
    result = _validate_one(_lead())
    probe = json.loads(result["availability_probe_summary_json"])[0]

    assert calls == result["availability_http_requests"] == 2
    assert result["availability_status"] == "request_error"
    assert result["temporal_validation_status"] == "request_error_retryable"
    assert result["validation_reason"] == "availability_request_error_retryable"
    assert result["retryable"] is True
    assert probe["status"] == "request_error"
    assert probe["response_statuses"] == ["no_capture", "request_error"]
    assert probe["disagreement"] is True


def test_first_capture_short_circuits_probe_after_one_call(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    calls_by_timestamp: dict[str, int] = {}

    def query(
        exact_url: str, requested_timestamp: str, **kwargs: object
    ) -> validator.AvailableCapture | None:
        calls_by_timestamp[requested_timestamp] = (
            calls_by_timestamp.get(requested_timestamp, 0) + 1
        )
        if requested_timestamp == "20180515120000":
            return _capture()
        return None

    monkeypatch.setattr(validator, "query_archive_available", query)
    monkeypatch.setattr(
        validator,
        "fetch_archive_capture",
        lambda *args, **kwargs: (
            "<html><title>Jane Doe for Mayor of Metroville</title></html>"
        ),
    )
    result = _validate_one(_lead())
    probe = json.loads(result["availability_probe_summary_json"])[0]

    assert calls_by_timestamp["20180515120000"] == 1
    assert result["availability_http_requests"] == 9
    assert probe["response_count"] == 1
    assert probe["response_statuses"] == ["capture"]
    assert probe["disagreement"] is False


def test_available_capture_metadata_alone_never_creates_strict_coverage(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(validator, "query_archive_available", _main_capture_only)

    def fail_fetch(*args: object, **kwargs: object) -> str:
        raise validator.ArchiveAvailableRequestError("replay unavailable")

    monkeypatch.setattr(validator, "fetch_archive_capture", fail_fetch)
    result = _validate_one(_lead())

    assert result["nearest_capture_available"] is True
    assert result["nearest_capture_timestamp"] == "20180515120000"
    assert result["within_headline_window"] is True
    assert result["capture_fetch_status"] == "request_error"
    assert result["proposed_evidence_grade"] == "C"
    assert result["strict_coverage_eligible_result"] is False
    assert result["temporal_validation_status"] == "request_error_retryable"
    assert result["retryable"] is True


@pytest.mark.parametrize(
    "html,missing_signal,reason",
    [
        (
            "<html><title>Alex Roe for Mayor of Metroville</title></html>",
            "candidate_identity_match",
            "candidate_identity_not_confirmed",
        ),
        (
            "<html><title>Jane Doe for City Council in Metroville</title></html>",
            "mayoral_campaign_match",
            "mayoral_campaign_not_confirmed",
        ),
        (
            "<html><title>Jane Doe for Mayor of Elsewhere</title></html>",
            "city_match",
            "city_not_confirmed",
        ),
    ],
)
def test_grade_a_requires_fetched_identity_mayoral_and_city_signals(
    monkeypatch: pytest.MonkeyPatch,
    html: str,
    missing_signal: str,
    reason: str,
) -> None:
    monkeypatch.setattr(validator, "query_archive_available", _main_capture_only)
    monkeypatch.setattr(
        validator, "fetch_archive_capture", lambda *args, **kwargs: html
    )
    result = _validate_one(_lead())

    assert result["capture_fetch_status"] == "fetched"
    assert result[missing_signal] is False
    assert result["proposed_evidence_grade"] == "C"
    assert result["strict_coverage_eligible_result"] is False
    assert result["validation_reason"] == reason


def test_fetched_election_period_capture_with_all_signals_emits_grade_a(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(validator, "query_archive_available", _main_capture_only)
    monkeypatch.setattr(
        validator,
        "fetch_archive_capture",
        lambda *args, **kwargs: (
            "<html><head><title>Jane Doe for Mayor of Metroville</title></head>"
            "<body>Elect Jane Doe mayor of Metroville.</body></html>"
        ),
    )
    result = _validate_one(_lead())

    assert result["archive_query_target"] == EXACT_URL
    assert result["availability_status"] == "checked"
    assert result["availability_http_requests"] == 9
    assert result["within_headline_window"] is True
    assert result["candidate_identity_match"] is True
    assert result["mayoral_campaign_match"] is True
    assert result["city_match"] is True
    assert result["domain_reuse_status"] == "no_qualifying_side_capture"
    assert result["proposed_evidence_grade"] == "A"
    assert result["strict_coverage_eligible_result"] is True
    assert (
        result["temporal_validation_status"]
        == "grade_a_content_confirmed_pending_review"
    )
    assert result["retryable"] is False


def test_side_capture_for_other_candidate_blocks_grade_a(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    nearest = _capture()
    conflicting = _capture("20160615120000")

    def query(
        exact_url: str, requested_timestamp: str, **kwargs: object
    ) -> validator.AvailableCapture | None:
        assert exact_url == EXACT_URL
        if requested_timestamp == "20180515120000":
            return nearest
        if requested_timestamp == "20160515120000":
            return conflicting
        return None

    def fetch(
        capture: validator.AvailableCapture, **kwargs: object
    ) -> str:
        if capture.timestamp == nearest.timestamp:
            return "<html><title>Jane Doe for Mayor of Metroville</title></html>"
        return "<html><title>Alex Roe for Mayor of Metroville</title></html>"

    monkeypatch.setattr(validator, "query_archive_available", query)
    monkeypatch.setattr(validator, "fetch_archive_capture", fetch)
    result = _validate_one(_lead())

    assert result["reuse_window_start"] == "2016-05-15"
    assert result["reuse_window_end"] == "2020-05-15"
    assert result["reuse_capture_count"] == 1
    assert result["reuse_fetched_count"] == 1
    assert result["reuse_conflict_count"] == 1
    assert result["domain_reuse_status"] == "conflicting_content_detected"
    assert result["validation_reason"] == "domain_reuse_conflict"
    assert result["proposed_evidence_grade"] == "C"
    assert result["strict_coverage_eligible_result"] is False
    assert result["retryable"] is False


def test_primary_and_partial_side_request_errors_remain_retryable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def primary_error(*args: object, **kwargs: object) -> None:
        raise validator.ArchiveAvailableRequestError("primary unavailable")

    monkeypatch.setattr(validator, "query_archive_available", primary_error)
    primary = _validate_one(_lead())
    assert primary["availability_status"] == "request_error"
    assert primary["availability_http_requests"] == 1
    assert primary["validation_reason"] == "availability_request_error_retryable"
    assert primary["temporal_validation_status"] == "request_error_retryable"
    assert primary["retryable"] is True
    assert primary["proposed_evidence_grade"] == "C"

    def partial_side(
        exact_url: str, requested_timestamp: str, **kwargs: object
    ) -> validator.AvailableCapture | None:
        if requested_timestamp == "20180515120000":
            return _capture()
        if requested_timestamp == "20160515120000":
            raise validator.ArchiveAvailableRequestError("side unavailable")
        return None

    monkeypatch.setattr(validator, "query_archive_available", partial_side)
    monkeypatch.setattr(
        validator,
        "fetch_archive_capture",
        lambda *args, **kwargs: (
            "<html><title>Jane Doe for Mayor of Metroville</title></html>"
        ),
    )
    partial = _validate_one(_lead())
    assert partial["availability_status"] == "checked_with_partial_error"
    assert partial["capture_fetch_status"] == "fetched"
    assert partial["domain_reuse_status"] == "request_error"
    assert partial["validation_reason"] == "reuse_inspection_request_error_retryable"
    assert partial["temporal_validation_status"] == "request_error_retryable"
    assert partial["retryable"] is True
    assert partial["proposed_evidence_grade"] == "C"
    assert partial["strict_coverage_eligible_result"] is False


@pytest.mark.parametrize("stale_column", ["lead_record_hash", "candidate_context_hash"])
def test_retryable_rows_are_retried_and_stale_checkpoint_hashes_are_pruned(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    stale_column: str,
) -> None:
    lead = _lead()
    frozen = _frame(lead)
    shard_id = int(lead["validation_shard_id"])
    output = tmp_path / "validation.csv"
    calls = 0

    def transient_error(*args: object, **kwargs: object) -> None:
        nonlocal calls
        calls += 1
        raise validator.ArchiveAvailableRequestError("temporary")

    monkeypatch.setattr(validator, "query_archive_available", transient_error)
    first = validator.validate_leads(
        frozen,
        output,
        shard_id,
        timeout=1,
        attempts=1,
        delay_seconds=0,
    )
    assert calls == 1
    assert bool(first.iloc[0].retryable)

    second_calls = 0

    def no_capture(*args: object, **kwargs: object) -> None:
        nonlocal second_calls
        second_calls += 1
        return None

    monkeypatch.setattr(validator, "query_archive_available", no_capture)
    second = validator.validate_leads(
        frozen,
        output,
        shard_id,
        timeout=1,
        attempts=1,
        delay_seconds=0,
    )
    assert second_calls == 2
    assert second.iloc[0].availability_status == "no_capture"
    assert not bool(second.iloc[0].retryable)

    checkpoint = pd.read_csv(output, keep_default_na=False)
    checkpoint.loc[0, stale_column] = "0" * 64
    checkpoint.to_csv(output, index=False, lineterminator="\n")
    stale_calls = 0

    def count_revalidation(*args: object, **kwargs: object) -> None:
        nonlocal stale_calls
        stale_calls += 1
        return None

    monkeypatch.setattr(validator, "query_archive_available", count_revalidation)
    third = validator.validate_leads(
        frozen,
        output,
        shard_id,
        timeout=1,
        attempts=1,
        delay_seconds=0,
    )
    assert stale_calls == 2
    assert len(third) == 1
    assert third.iloc[0][stale_column] == lead[stale_column]


def test_completed_checkpoint_survives_csv_month_roundtrip(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    lead = _lead(month=11)
    frozen = _frame(lead)
    output = tmp_path / "validation.csv"
    completed = validator._base_result(lead)
    completed.update(
        {
            "availability_status": "no_capture",
            "capture_fetch_status": "not_attempted_no_capture",
            "domain_reuse_status": "not_inspected_no_capture",
            "temporal_validation_status": "no_archive_capture",
            "validation_reason": "no_archive_capture_confirmed",
            "retryable": False,
        }
    )
    pd.DataFrame([completed], columns=validator.VALIDATION_COLUMNS).to_csv(
        output, index=False, lineterminator="\n"
    )

    def unexpected_request(*args: object, **kwargs: object) -> None:
        raise AssertionError("completed checkpoint was unexpectedly revalidated")

    monkeypatch.setattr(validator, "query_archive_available", unexpected_request)
    result = validator.validate_leads(
        frozen,
        output,
        int(lead["validation_shard_id"]),
        timeout=1,
        attempts=1,
        delay_seconds=0,
    )

    assert len(result) == 1
    assert result.iloc[0].temporal_validation_status == "no_archive_capture"
    assert not bool(result.iloc[0].retryable)


def test_validation_shards_have_exact_key_equality_and_schema(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    leads = [
        _lead(
            url_claim_id=f"search-url-claim-{index}",
            discovery_candidate_year_id=f"discovery-{index}__2018",
            discovery_candidate_contest_id=f"discovery-contest-{index}",
            candidate_year_id=f"attributed-{index}__2018",
            candidate_contest_id=f"attributed-contest-{index}",
            exact_url=f"https://example.org/Candidate/{index}?Ref=Vote{index}",
        )
        for index in range(1, 9)
    ]
    frozen = _frame(*leads)
    monkeypatch.setattr(
        validator, "query_archive_available", lambda *args, **kwargs: None
    )

    for shard_id in range(SHARD_COUNT):
        expected = {
            str(row["validation_key"])
            for row in leads
            if int(row["validation_shard_id"]) == shard_id
        }
        output = tmp_path / f"validation_shard_{shard_id}.csv"
        result = validator.validate_leads(
            frozen,
            output,
            shard_id,
            timeout=1,
            attempts=1,
            delay_seconds=0,
        )
        assert result.columns.tolist() == validator.VALIDATION_COLUMNS
        assert set(result.validation_key.astype(str)) == expected
        assert not result.validation_key.duplicated().any()
        assert set(result.validation_shard_id.astype(int)) <= {shard_id}
        assert (
            pd.read_csv(output, keep_default_na=False).columns.tolist()
            == validator.VALIDATION_COLUMNS
        )
def test_accented_candidate_name_and_surname_only_campaign_are_distinct():
    full = validator.content_signals(
        '<h1>José Solorio for Mayor 2020</h1><p>Santa Ana</p>',
        'jose solorio', 'santa ana',
    )
    assert full['candidate_identity_match'] is True
    assert full['mayoral_campaign_match'] is True
    surname_only = validator.content_signals(
        '<h1>Solorio for Mayor 2020</h1><p>Santa Ana</p>',
        'jose solorio', 'santa ana',
    )
    assert surname_only['candidate_identity_match'] is False
    assert surname_only['mayoral_campaign_match'] is True
