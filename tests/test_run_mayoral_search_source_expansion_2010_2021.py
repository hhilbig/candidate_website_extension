import hashlib
import json
import threading
import time
from pathlib import Path
from urllib.parse import urlsplit

import pandas as pd
import pytest
import requests

import scripts.run_mayoral_search_source_expansion_2010_2021 as wave


class FakeSearchResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code
        self.content = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.text = self.content.decode("utf-8")

    def json(self):
        return self.payload


class FakeSearchSession:
    def __init__(self, payload=None, error=None):
        self.payload = payload or {"organic": []}
        self.error = error
        self.calls = []

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if self.error:
            raise self.error
        return FakeSearchResponse(self.payload)


class FakePageResponse:
    def __init__(self, content: bytes, url="https://springfieldnews.com/election/2010"):
        self.content = content
        self.url = url
        self.status_code = 200
        self.headers = {"content-type": "text/html; charset=utf-8"}

    def iter_content(self, chunk_size=65536):
        yield self.content


class FakePageSession:
    def __init__(self, content: bytes):
        self.content = content
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return FakePageResponse(self.content, url)


class ParallelPageTracker:
    def __init__(self):
        self.lock = threading.Lock()
        self.active_by_host = {}
        self.max_active_by_host = {}
        self.active_total = 0
        self.max_active_total = 0
        self.calls = []
        self.session_threads = {}


class ParallelPageSession:
    def __init__(self, tracker: ParallelPageTracker):
        self.tracker = tracker
        self.closed = False

    def get(self, url, **kwargs):
        host = wave.registrable_domain(url) or urlsplit(url).hostname
        thread_id = threading.get_ident()
        with self.tracker.lock:
            self.tracker.calls.append(url)
            self.tracker.session_threads.setdefault(id(self), set()).add(thread_id)
            self.tracker.active_by_host[host] = (
                self.tracker.active_by_host.get(host, 0) + 1
            )
            self.tracker.max_active_by_host[host] = max(
                self.tracker.max_active_by_host.get(host, 0),
                self.tracker.active_by_host[host],
            )
            self.tracker.active_total += 1
            self.tracker.max_active_total = max(
                self.tracker.max_active_total, self.tracker.active_total
            )
        time.sleep(0.03)
        with self.tracker.lock:
            self.tracker.active_by_host[host] -= 1
            self.tracker.active_total -= 1
        content = (
            f"<html><body><p>Source page {url}</p></body></html>".encode("utf-8")
        )
        return FakePageResponse(content, url)

    def close(self):
        self.closed = True


def _prepare(tmp_path: Path) -> Path:
    output = tmp_path / "wave"
    wave.prepare_wave(output_dir=output)
    return output


def _credible_results(candidate="Debbie Cimarossa"):
    results = []
    for rank in range(1, 6):
        results.append(
            {
                "position": rank,
                "title": f"{candidate} Springfield mayor election 2010",
                "link": f"https://springfieldnews{rank}.com/elections/2010/debbie",
                "snippet": f"{candidate} campaign website coverage in Springfield IL 2010",
                "date": "Oct 1, 2010",
            }
        )
    results.extend(
        [
            {
                "position": 6,
                "title": f"{candidate} for Springfield Mayor 2010",
                "link": "https://VoteDebbieCimarossaForMayor.com/Issues?Ref=Mail",
                "snippet": f"Official campaign website for {candidate}",
            },
            {
                "position": 7,
                "title": f"{candidate} Springfield mayor 2010",
                "link": "https://facebook.com/debbie",
                "snippet": f"{candidate} election campaign Springfield 2010",
            },
        ]
    )
    return results


def _contest_roster() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "candidate_year_id": "a__2010",
                "candidate_contest_id": "contest__a",
                "contest": "contest",
                "full_name": "Debbie Cimarossa",
                "geo_name": "Springfield",
                "state_abb": "IL",
                "year": 2010,
                "month": 11,
            },
            {
                "candidate_year_id": "b__2010",
                "candidate_contest_id": "contest__b",
                "contest": "contest",
                "full_name": "Alice Example",
                "geo_name": "Springfield",
                "state_abb": "IL",
                "year": 2010,
                "month": 11,
            },
        ]
    )


def _queue_row(**updates) -> dict[str, object]:
    row: dict[str, object] = {column: "" for column in wave.RANKED_QUEUE_COLUMNS}
    row.update(
        {
            "contract_version": wave.CONTRACT_VERSION,
            "result_claim_id": "result-1",
            "query_attempt_id": "query-1",
            "candidate_year_id": "a__2010",
            "candidate_contest_id": "contest__a",
            "contest": "contest",
            "full_name": "Debbie Cimarossa",
            "geo_name": "Springfield",
            "state_abb": "IL",
            "year": 2010,
            "month": 11,
            "result_rank": 1,
            "result_title": "Debbie Cimarossa for Springfield Mayor",
            "result_url": "https://springfieldnews.com/election/debbie",
            "result_snippet": "Debbie Cimarossa mayoral campaign in Springfield",
            "exact_normalized_result_url": (
                "https://springfieldnews.com/election/debbie"
            ),
            "candidate_attribution_status": "identity_supported",
            "candidate_attribution_reason": "fixture_unique_full_name",
            "result_classification": "credible_source_page",
            "eligible_for_page_fetch": True,
            "selected_for_page_fetch": True,
            "page_fetch_rank": 1,
            "page_selection_reason": (
                "top_three_identity_supported_election_source"
            ),
            "classifier_version": wave.RESULT_CLASSIFIER_VERSION,
        }
    )
    row.update(updates)
    return row


def test_prepare_emits_exact_480_stable_queries_and_preserves_missing_month(tmp_path):
    output = _prepare(tmp_path)
    manifest = pd.read_csv(output / "query_manifest.csv", keep_default_na=False)
    assert manifest.columns.tolist() == wave.QUERY_MANIFEST_COLUMNS
    assert len(manifest) == manifest.candidate_year_id.nunique() == 480
    assert manifest.query_attempt_id.nunique() == manifest.query_attempt_hash.nunique() == 480
    assert manifest["query"].str.endswith(" (campaign OR website)").all()
    assert manifest.loc[manifest.candidate_year_id.eq("45469__2020"), "month"].iloc[0] == ""
    assert manifest.loc[manifest.candidate_year_id.eq("45469__2020"), "query"].str.contains(
        " 2020 "
    ).all()
    first = manifest.iloc[0]
    expected_hash = wave.stable_hash(
        first.screen_sha256,
        first.candidate_year_id,
        first.candidate_contest_id,
        first.query,
        first.query_version,
        first.provider,
        first.country,
        first.language,
        first.results_requested,
    )
    assert first.query_attempt_hash == expected_hash
    assert first.query_attempt_id == f"serper_query_{expected_hash[:24]}"


def test_query_name_repairs_and_aliases_are_deterministic(tmp_path):
    output = _prepare(tmp_path)
    manifest = pd.read_csv(output / "query_manifest.csv", keep_default_na=False)
    repaired = manifest.loc[
        manifest.query_name_repaired.astype(str).str.casefold().eq("true")
    ]
    assert len(repaired) == 3
    assert set(repaired.query_name) == {
        "michael sánchez",
        "michael c. taylor",
        "janifer “jan” kulmann",
    }
    jan = repaired.loc[repaired.query_name.str.startswith("janifer")].iloc[0]
    assert '("janifer kulmann" OR "jan kulmann")' in jan.query
    assert wave.candidate_identity_clause('walter "skip" campbell') == (
        '("walter campbell" OR "skip campbell")'
    )
    assert wave.candidate_identity_clause("jacq'ue 'j' miller") == (
        '("jacq\'ue miller" OR "j miller")'
    )


@pytest.mark.parametrize(
    "name,expected",
    [
        (
            "michael christopher “mike” taylor",
            '("michael christopher taylor" OR "michael taylor" OR "mike taylor")',
        ),
        (
            "michael c. “mike” taylor",
            '("michael c. taylor" OR "michael taylor" OR "mike taylor")',
        ),
    ],
)
def test_middle_name_or_initial_adds_reduced_legal_alias_with_curly_nickname(
    name, expected
):
    assert wave.candidate_identity_clause(name) == expected


def test_env_loader_reads_only_serper_key_and_missing_key_prevents_request(tmp_path):
    env_file = tmp_path / ".env"
    env_file.write_text("OTHER_SECRET=nope\nSERPER_API_KEY='only-this'\n", encoding="utf-8")
    assert wave.load_serper_api_key(environ={}, env_path=env_file) == "only-this"

    output = _prepare(tmp_path / "missing")
    session = FakeSearchSession({"organic": []})
    with pytest.raises(wave.SearchExpansionError, match="no search request was sent"):
        wave.search_wave(
            output_dir=output,
            request_budget=1,
            environ={},
            env_path=tmp_path / "does-not-exist",
            session=session,
            delay_seconds=0,
        )
    assert session.calls == []
    assert pd.read_csv(output / "request_ledger.csv").empty


def test_search_makes_one_request_retains_full_raw_response_and_all_results(tmp_path):
    output = _prepare(tmp_path)
    payload = {"organic": _credible_results(), "credits": 2499, "searchParameters": {"q": "x"}}
    session = FakeSearchSession(payload)
    audit = wave.search_wave(
        output_dir=output,
        request_budget=1,
        environ={"SERPER_API_KEY": "secret"},
        session=session,
        delay_seconds=0,
    )
    assert len(session.calls) == 1
    assert "X-API-KEY" in session.calls[0][1]["headers"]
    ledger = pd.read_csv(output / "request_ledger.csv", keep_default_na=False)
    assert len(ledger) == 1
    assert ledger.iloc[0].request_status == "found"
    assert bool(ledger.iloc[0].terminal_for_run)
    raw = wave.read_raw_responses(output / "raw_responses.jsonl")
    assert len(raw) == 1
    assert raw[0]["raw_response"] == payload
    assert json.loads(raw[0]["raw_response_body"]) == payload


def test_search_raw_hash_ranking_cap_direct_retention_and_resume(tmp_path):
    output = _prepare(tmp_path)
    payload = {"organic": _credible_results(), "credits": 2499}
    session = FakeSearchSession(payload)
    wave.search_wave(
        output_dir=output,
        request_budget=1,
        environ={"SERPER_API_KEY": "secret"},
        session=session,
        delay_seconds=0,
    )
    raw = wave.read_raw_responses(output / "raw_responses.jsonl")[0]
    expected_body = json.dumps(payload, ensure_ascii=False)
    assert json.loads(raw["raw_response_body"]) == payload
    expected_hash = hashlib.sha256(expected_body.encode("utf-8")).hexdigest()
    assert raw["raw_response_sha256"] == expected_hash
    queue = pd.read_csv(output / "ranked_result_page_queue.csv", keep_default_na=False)
    assert len(queue) == 7
    assert queue.result_claim_id.nunique() == 7
    assert queue.selected_for_page_fetch.astype(str).str.casefold().eq("true").sum() == 3
    assert queue.page_selection_reason.eq("eligible_beyond_candidate_cap").sum() == 2
    direct = queue.loc[queue.result_classification.eq("direct_candidate_site")]
    assert len(direct) == 1
    assert direct.iloc[0].result_url == "https://VoteDebbieCimarossaForMayor.com/Issues?Ref=Mail"
    assert direct.iloc[0].exact_normalized_result_url == (
        "https://votedebbiecimarossaformayor.com/Issues?Ref=Mail"
    )
    audit = json.loads((output / "audit.json").read_text())
    assert audit
    assert audit["organic_result_rows"] == audit["ranked_queue_rows"] == 7
    assert audit["large_filter_audit"]["triggered"]

    second = FakeSearchSession({"organic": []})
    wave.search_wave(
        output_dir=output,
        request_budget=1,
        environ={"SERPER_API_KEY": "secret"},
        session=second,
        delay_seconds=0,
    )
    assert second.calls == []
    assert len(wave.read_raw_responses(output / "raw_responses.jsonl")) == 1


def test_request_error_is_terminal_for_run_and_not_reissued(tmp_path):
    output = _prepare(tmp_path)
    failed = FakeSearchSession(error=requests.ConnectTimeout("temporary"))
    wave.search_wave(
        output_dir=output,
        request_budget=1,
        environ={"SERPER_API_KEY": "secret"},
        session=failed,
        delay_seconds=0,
    )
    ledger = pd.read_csv(output / "request_ledger.csv", keep_default_na=False)
    assert len(failed.calls) == len(ledger) == 1
    assert ledger.iloc[0].request_status == "request_error"
    assert bool(ledger.iloc[0].terminal_for_run)
    assert bool(ledger.iloc[0].retryable)
    assert not bool(ledger.iloc[0].complete_for_wave)

    resumed = FakeSearchSession({"organic": []})
    wave.search_wave(
        output_dir=output,
        request_budget=1,
        environ={"SERPER_API_KEY": "secret"},
        session=resumed,
        delay_seconds=0,
    )
    assert resumed.calls == []


def test_page_fetch_preserves_content_hash_text_links_and_url_case(tmp_path):
    html = b"""
    <html><body><h1>Debbie Cimarossa Springfield mayor 2010</h1>
    <p>Debbie Cimarossa campaign website
    <a href="https://VoteDebbie.com/Issues?Ref=Mail">Official website</a></p>
    </body></html>
    """
    session = FakePageSession(html)
    row, links = wave._download_one_page(
        "https://springfieldnews.com/election/2010",
        ["result-1", "result-2"],
        tmp_path,
        timeout=10,
        maximum_bytes=100_000,
        session=session,
    )
    assert len(session.calls) == 1
    assert row["fetch_status"] == "fetched"
    assert row["content_sha256"] == hashlib.sha256(html).hexdigest()
    assert (tmp_path / row["local_file_path"]).read_bytes() == html
    assert (tmp_path / row["extracted_text_path"]).is_file()
    candidate_link = next(link for link in links if "VoteDebbie" in link["raw_url"])
    assert candidate_link["raw_url"] == "https://VoteDebbie.com/Issues?Ref=Mail"
    assert candidate_link["exact_normalized_url"] == "https://votedebbie.com/Issues?Ref=Mail"
    assert candidate_link["parser_version"] == wave.SOURCE_PAGE_PARSER_VERSION


def test_parallel_fetch_enforces_budget_host_serialization_and_parent_checkpoints(
    tmp_path, monkeypatch
):
    output = _prepare(tmp_path)
    urls = [
        "https://a.same-news.com/election/a",
        "https://b.same-news.com/election/b",
        "https://other-news.org/election/c",
        "https://third-news.net/election/d",
    ]
    queue = pd.DataFrame(
        [
            _queue_row(
                result_claim_id=f"result-{index}",
                result_url=url,
                exact_normalized_result_url=url,
                page_fetch_rank=index,
            )
            for index, url in enumerate(urls, 1)
        ],
        columns=wave.RANKED_QUEUE_COLUMNS,
    )
    queue.to_csv(output / "ranked_result_page_queue.csv", index=False)

    tracker = ParallelPageTracker()
    sessions = []

    def session_factory():
        created = ParallelPageSession(tracker)
        sessions.append(created)
        return created

    parent_thread = threading.get_ident()
    atomic_csv_threads = []
    original_atomic_csv = wave._atomic_csv

    def recording_atomic_csv(*args, **kwargs):
        atomic_csv_threads.append(threading.get_ident())
        return original_atomic_csv(*args, **kwargs)

    monkeypatch.setattr(wave, "_atomic_csv", recording_atomic_csv)
    monkeypatch.setattr(wave, "update_audit", lambda **kwargs: {"tested": True})
    wave.fetch_selected_pages(
        output_dir=output,
        page_request_budget=3,
        workers=3,
        delay_seconds=0,
        session_factory=session_factory,
    )

    downloaded = pd.read_csv(output / "downloaded_pages.csv", keep_default_na=False)
    assert len(tracker.calls) == len(downloaded) == 3
    assert downloaded.columns.tolist() == wave.DOWNLOADED_PAGE_COLUMNS
    assert downloaded.page_id.tolist() == sorted(downloaded.page_id.tolist())
    assert downloaded.fetch_status.eq("fetched").all()
    assert tracker.max_active_by_host["same-news.com"] == 1
    assert tracker.max_active_total >= 2
    assert all(len(thread_ids) == 1 for thread_ids in tracker.session_threads.values())
    assert all(created.closed for created in sessions)
    assert atomic_csv_threads and set(atomic_csv_threads) == {parent_thread}

    # Fetched pages are restart-terminal. A resumed four-request budget sends
    # exactly the one page that the first bounded call did not schedule.
    wave.fetch_selected_pages(
        output_dir=output,
        page_request_budget=4,
        workers=3,
        delay_seconds=0,
        session_factory=session_factory,
    )
    resumed = pd.read_csv(output / "downloaded_pages.csv", keep_default_na=False)
    assert len(tracker.calls) == len(resumed) == 4
    assert resumed.page_id.tolist() == sorted(resumed.page_id.tolist())


def test_direct_candidate_result_becomes_one_pending_d_lead_without_page_links():
    raw_url = "https://VoteDebbieCimarossa.com/Issues?Ref=Mail"
    exact_url = "https://votedebbiecimarossa.com/Issues?Ref=Mail"
    queue = pd.DataFrame(
        [
            _queue_row(
                result_url=raw_url,
                exact_normalized_result_url=exact_url,
                result_classification="direct_candidate_site",
                eligible_for_page_fetch=False,
                selected_for_page_fetch=False,
                page_fetch_rank="",
                page_selection_reason="ineligible_result_classification",
            )
        ],
        columns=wave.RANKED_QUEUE_COLUMNS,
    )
    leads = wave.build_url_leads(
        queue,
        pd.DataFrame(columns=wave.PAGE_LINK_COLUMNS),
        _contest_roster(),
        {"a__2010"},
    )

    assert len(leads) == leads.result_claim_id.nunique() == 1
    lead = leads.iloc[0]
    assert lead.result_claim_id == "result-1"
    assert lead.discovery_candidate_year_id == "a__2010"
    assert lead.candidate_year_id == "a__2010"
    assert lead.raw_url == raw_url
    assert lead.exact_normalized_url == exact_url
    assert lead.page_link_id == lead.page_id == lead.source_page_url == ""
    assert lead.extraction_method == "serper_direct_candidate_site_result"
    assert lead.candidate_attribution_status == "identity_supported"
    assert lead.evidence_grade == "D"
    assert not bool(lead.strict_coverage_eligible)
    assert lead.review_status == "pending"
    assert lead.acceptance == ""


def test_contest_page_preserves_discovery_candidate_when_link_names_other_candidate():
    roster = _contest_roster()
    source_url = "https://springfieldnews.com/election/2010"
    page_id = wave.stable_id("source_page", wave.CONTRACT_VERSION, source_url)
    queue = pd.DataFrame(
        [_queue_row(exact_normalized_result_url=source_url, result_url=source_url)],
        columns=wave.RANKED_QUEUE_COLUMNS,
    )
    link = {
        "page_link_id": "link-other-candidate",
        "page_id": page_id,
        "page_url": source_url,
        "anchor_text": "Alice Example campaign website",
        "surrounding_context": "Alice Example campaign website",
        "raw_url": "https://aliceexample.com/",
        "exact_normalized_url": "https://aliceexample.com/",
        "site_root_url": "https://aliceexample.com/",
        "registrable_domain": "aliceexample.com",
        "url_type": "custom_domain",
        "is_external_to_source": True,
        "source_locator": "anchor:2",
        "extraction_method": "html_hyperlink",
    }

    lead = wave.build_url_leads(
        queue, pd.DataFrame([link]), roster, {"a__2010"}
    ).iloc[0]

    assert lead.discovery_candidate_year_id == "a__2010"
    assert lead.candidate_year_id == "b__2010"
    assert lead.candidate_attribution_status == "identity_supported"


def test_candidate_shaped_hostname_alone_remains_match_unresolved():
    source_url = "https://springfieldnews.com/election/debbie"
    page_id = wave.stable_id("source_page", wave.CONTRACT_VERSION, source_url)
    queue = pd.DataFrame(
        [_queue_row(exact_normalized_result_url=source_url, result_url=source_url)],
        columns=wave.RANKED_QUEUE_COLUMNS,
    )
    raw_url = "https://VoteDebbieCimarossa.com/Issues?Ref=Mail"
    link = {
        "page_link_id": "link-hostname-only",
        "page_id": page_id,
        "page_url": source_url,
        "anchor_text": "Official campaign website",
        "surrounding_context": "Visit the official website",
        "raw_url": raw_url,
        "exact_normalized_url": (
            "https://votedebbiecimarossa.com/Issues?Ref=Mail"
        ),
        "site_root_url": "https://votedebbiecimarossa.com/",
        "registrable_domain": "votedebbiecimarossa.com",
        "url_type": "custom_domain",
        "is_external_to_source": True,
        "source_locator": "anchor:1",
        "extraction_method": "html_hyperlink",
    }
    leads = wave.build_url_leads(
        queue, pd.DataFrame([link]), _contest_roster(), {"a__2010"}
    )

    assert len(leads) == 1
    lead = leads.iloc[0]
    assert lead.raw_url == raw_url
    assert lead.candidate_attribution_status == "match_unresolved"
    assert (
        lead.candidate_attribution_reason
        == "candidate_shaped_url_is_lead_not_identity_proof"
    )
    assert json.loads(lead.matched_candidate_ids_json) == ["a__2010"]
    assert lead.evidence_grade == "D"
    assert not bool(lead.strict_coverage_eligible)


@pytest.mark.parametrize(
    "url,expected",
    [
        ("https://youtu.be/abc", "social_profile"),
        ("https://reddit.com/submit?url=x", "social_profile"),
        ("https://www.tumblr.com/share/link?url=x", "social_profile"),
        ("https://www.facebook.com%20%E2%80%BA%20candidate", "other_lead"),
        (
            "http://candidates.sos.state.mn.us/CandidateInformation.aspx?id=1",
            "officeholder_or_government_profile",
        ),
    ],
)
def test_search_lead_classifier_excludes_share_and_state_election_urls(url, expected):
    assert wave.classify_search_lead_url(url) == expected


@pytest.mark.parametrize(
    "url,expected_type",
    [
        ("https://ballotpedia.org/Debbie_Cimarossa", "evidence_profile"),
        ("https://en.wikipedia.org/wiki/Debbie_Cimarossa", "evidence_profile"),
        ("https://www.vote411.org/candidate-detail?id=123", "evidence_profile"),
        ("https://springfieldnews.com/elections/debbie", "evidence_source_page"),
        ("https://elections.university.edu/debbie", "evidence_source_page"),
        (
            "https://springfield.gov/mayor/debbie",
            "officeholder_or_government_profile",
        ),
    ],
)
def test_profile_and_evidence_urls_receive_noncoverage_types(url, expected_type):
    assert wave.classify_search_lead_url(url) == expected_type
    assert expected_type not in {
        "custom_domain",
        "candidate_subdomain",
        "shared_host_path",
    }


def test_profile_and_evidence_links_remain_pending_d_noncoverage_leads():
    source_url = "https://springfieldnews.com/election/debbie"
    page_id = wave.stable_id("source_page", wave.CONTRACT_VERSION, source_url)
    queue = pd.DataFrame(
        [_queue_row(exact_normalized_result_url=source_url, result_url=source_url)],
        columns=wave.RANKED_QUEUE_COLUMNS,
    )
    typed_urls = {
        "https://ballotpedia.org/Debbie_Cimarossa": "evidence_profile",
        "https://en.wikipedia.org/wiki/Debbie_Cimarossa": "evidence_profile",
        "https://springfieldnews.com/elections/debbie": "evidence_source_page",
        "https://springfield.gov/mayor/debbie": (
            "officeholder_or_government_profile"
        ),
    }
    links = []
    for index, raw_url in enumerate(typed_urls, 1):
        exact_url = wave.normalize_exact_url(raw_url)
        links.append(
            {
                "page_link_id": f"typed-link-{index}",
                "page_id": page_id,
                "page_url": source_url,
                "anchor_text": "Debbie Cimarossa profile",
                "surrounding_context": (
                    "Debbie Cimarossa candidate profile and campaign website"
                ),
                "raw_url": raw_url,
                "exact_normalized_url": exact_url,
                "site_root_url": wave.site_root_url(exact_url),
                "registrable_domain": wave.registrable_domain(exact_url),
                "url_type": wave.classify_url(exact_url),
                "is_external_to_source": True,
                "source_locator": f"anchor:{index}",
                "extraction_method": "html_hyperlink",
            }
        )
    leads = wave.build_url_leads(
        queue, pd.DataFrame(links), _contest_roster(), {"a__2010"}
    )

    assert len(leads) == len(typed_urls)
    actual_types = dict(zip(leads.raw_url, leads.url_type))
    assert actual_types == typed_urls
    assert leads.candidate_attribution_status.eq("identity_supported").all()
    assert leads.evidence_grade.eq("D").all()
    assert not leads.strict_coverage_eligible.astype(bool).any()
    assert leads.review_status.eq("pending").all()
    assert leads.acceptance.eq("").all()


def test_no_year_source_is_fetchable_but_explicit_other_year_is_conflict_retained():
    query_manifest = pd.DataFrame(
        [
            {
                "query_attempt_id": "query-1",
                "candidate_year_id": "a__2010",
                "candidate_contest_id": "contest__a",
                "contest": "contest",
                "full_name": "Debbie Cimarossa",
                "query_name": "Debbie Cimarossa",
                "geo_name": "Springfield",
                "state_abb": "IL",
                "year": 2010,
                "month": 11,
            }
        ]
    )
    organic = [
        {
            "position": 1,
            "title": "Debbie Cimarossa for Springfield Mayor",
            "link": "https://springfieldnews.com/elections/debbie",
            "snippet": "Debbie Cimarossa mayoral campaign in Springfield, IL",
        },
        {
            "position": 2,
            "title": "Debbie Cimarossa for Springfield Mayor in 2012",
            "link": "https://springfieldnews.com/elections/2012/debbie",
            "snippet": "Debbie Cimarossa mayoral campaign in Springfield, IL in 2012",
        },
    ]
    queue = wave.build_ranked_result_page_queue(
        query_manifest,
        [{"query_attempt_id": "query-1", "raw_response": {"organic": organic}}],
        _contest_roster(),
    ).sort_values("result_rank")

    assert len(queue) == 2
    no_year = queue.iloc[0]
    assert not bool(no_year.exact_year_signal)
    assert not bool(no_year.temporal_conflict)
    assert no_year.result_classification == "credible_source_page"
    assert bool(no_year.eligible_for_page_fetch)
    assert bool(no_year.selected_for_page_fetch)

    other_year = queue.iloc[1]
    assert not bool(other_year.exact_year_signal)
    assert bool(other_year.temporal_conflict)
    assert (
        other_year.result_classification
        == "credible_source_page_temporal_conflict"
    )
    assert not bool(other_year.eligible_for_page_fetch)
    assert not bool(other_year.selected_for_page_fetch)
    assert (
        other_year.page_selection_reason
        == "temporal_conflict_retained_for_review"
    )


def test_page_link_disposition_audit_accounts_for_every_selected_result_link_pair():
    source_url = "https://springfieldnews.com/election/debbie"
    page_id = wave.stable_id("source_page", wave.CONTRACT_VERSION, source_url)
    queue = pd.DataFrame(
        [_queue_row(exact_normalized_result_url=source_url, result_url=source_url)],
        columns=wave.RANKED_QUEUE_COLUMNS,
    )

    def page_link(
        link_id: str,
        raw_url: str,
        *,
        anchor: str,
        context: str,
        external: bool,
    ) -> dict[str, object]:
        exact_url = wave.normalize_exact_url(raw_url)
        return {
            "contract_version": wave.CONTRACT_VERSION,
            "page_link_id": link_id,
            "page_id": page_id,
            "page_url": source_url,
            "source_locator": f"anchor:{link_id}",
            "extraction_method": "html_hyperlink",
            "anchor_text": anchor,
            "surrounding_context": context,
            "raw_url": raw_url,
            "exact_normalized_url": exact_url,
            "site_root_url": wave.site_root_url(exact_url),
            "registrable_domain": wave.registrable_domain(exact_url),
            "url_type": wave.classify_url(exact_url),
            "is_external_to_source": external,
            "parser_version": wave.SOURCE_PAGE_PARSER_VERSION,
        }

    links = pd.DataFrame(
        [
            page_link(
                "emitted",
                "https://VoteDebbie.com/Issues?Ref=Mail",
                anchor="Debbie Cimarossa official website",
                context="Debbie Cimarossa campaign website",
                external=True,
            ),
            page_link(
                "internal",
                "https://springfieldnews.com/about",
                anchor="About this source",
                context="Internal navigation",
                external=False,
            ),
            page_link(
                "asset",
                "https://cdn.example.com/campaign-logo.png",
                anchor="Campaign logo",
                context="Image asset",
                external=True,
            ),
            page_link(
                "no-match",
                "https://example.com/contact",
                anchor="Read more",
                context="Additional information",
                external=True,
            ),
        ],
        columns=wave.PAGE_LINK_COLUMNS,
    )
    leads = wave.build_url_leads(
        queue, links, _contest_roster(), {"a__2010"}
    )
    audit = wave.page_link_disposition_audit(queue, links, leads)

    assert len(leads) == 1
    assert audit["evaluated_page_link_result_pairs"] == 4
    assert audit["disposition_counts"]["emitted_identity_supported_claim"] == 1
    assert audit["disposition_counts"]["internal_source_link"] == 1
    assert audit["disposition_counts"]["asset_extension"] == 1
    assert audit["disposition_counts"]["no_candidate_match"] == 1
    assert sum(audit["disposition_counts"].values()) == 4
    assert audit["exact_disposition_sum_agreement"] is True
    assert audit["emitted_page_url_claims"] == 1
    assert audit["not_emitted_pairs"] == 3
    assert audit["not_emitted_fraction"] == pytest.approx(0.75)
    assert audit["greater_than_20_percent_not_emitted"] is True


def test_full_contest_attribution_and_leads_remain_unapproved():
    roster = pd.DataFrame(
        [
            {
                "candidate_year_id": "a__2010",
                "candidate_contest_id": "contest__a",
                "contest": "contest",
                "full_name": "Debbie Cimarossa",
                "geo_name": "Springfield",
                "state_abb": "IL",
                "year": 2010,
                "month": 11,
            },
            {
                "candidate_year_id": "b__2010",
                "candidate_contest_id": "contest__b",
                "contest": "contest",
                "full_name": "Alice Example",
                "geo_name": "Springfield",
                "state_abb": "IL",
                "year": 2010,
                "month": 11,
            },
        ]
    )
    page_url = "https://springfieldnews.com/election/2010"
    page_id = wave.stable_id("source_page", wave.CONTRACT_VERSION, page_url)
    queue = pd.DataFrame(
        [
            {
                "selected_for_page_fetch": True,
                "exact_normalized_result_url": page_url,
                "result_claim_id": "result-1",
                "query_attempt_id": "query-1",
                "candidate_year_id": "a__2010",
                "candidate_attribution_status": "identity_supported",
                "contest": "contest",
            }
        ]
    )
    link = {
        "page_link_id": "link-1",
        "page_id": page_id,
        "page_url": page_url,
        "anchor_text": "Official website",
        "surrounding_context": "Debbie Cimarossa campaign website",
        "raw_url": "https://VoteDebbie.com/Issues?Ref=Mail",
        "exact_normalized_url": "https://votedebbie.com/Issues?Ref=Mail",
        "site_root_url": "https://votedebbie.com/",
        "registrable_domain": "votedebbie.com",
        "url_type": "custom_domain",
        "is_external_to_source": True,
        "source_locator": "anchor:1",
        "extraction_method": "html_hyperlink",
    }
    leads = wave.build_url_leads(queue, pd.DataFrame([link]), roster, {"a__2010"})
    assert len(leads) == 1
    lead = leads.iloc[0]
    assert lead.candidate_year_id == "a__2010"
    assert lead.candidate_attribution_status == "identity_supported"
    assert lead.raw_url == "https://VoteDebbie.com/Issues?Ref=Mail"
    assert lead.evidence_grade == "D"
    assert not bool(lead.strict_coverage_eligible)
    assert lead.review_status == "pending"
    assert lead.acceptance == ""

    ambiguous = dict(link)
    ambiguous["page_link_id"] = "link-2"
    ambiguous["surrounding_context"] = (
        "Debbie Cimarossa and Alice Example campaign websites"
    )
    unresolved = wave.build_url_leads(
        queue, pd.DataFrame([ambiguous]), roster, {"a__2010"}
    )
    assert len(unresolved) == 1
    assert unresolved.iloc[0].candidate_attribution_status == "match_unresolved"
    assert json.loads(unresolved.iloc[0].matched_candidate_ids_json) == [
        "a__2010",
        "b__2010",
    ]
