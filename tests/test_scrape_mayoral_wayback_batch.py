from __future__ import annotations

import json
import threading
from pathlib import Path

import pandas as pd
import requests

import scripts.scrape_mayoral_wayback_batch as scrape


def _target() -> dict[str, object]:
    return {
        "scrape_key": "target-1\x1fcandidate-1\x1fexample.org", "target_id": "target-1",
        "candidate_year_id": "candidate-1", "candidate_contest_id": "contest-1",
        "full_name": "Example Candidate", "geo_name": "Example", "state_abb": "CA",
        "year": 2019, "month": 11, "registrable_domain": "example.org",
        "election_reference_date": "2019-11-15", "max_pages_per_target": 2,
    }


def _variant() -> dict[str, object]:
    return {"variant_attempt_id": "attempt-1", "scrape_key": _target()["scrape_key"], "target_id": "target-1"}


def test_parse_cdx_and_select_nearest_distinct_paths_with_cap() -> None:
    payload = [["timestamp", "original", "statuscode", "mimetype", "digest", "length"]]
    payload += [
        ["20191101000000", "http://example.org/", "200", "text/html", "A", "100"],
        ["20191114000000", "https://www.example.org/", "200", "text/html", "B", "110"],
        ["20191001000000", "http://example.org/about", "200", "text/html", "C", "120"],
        ["20191113000000", "http://example.org/about", "200", "text/html", "D", "130"],
        ["20191112000000", "http://example.org/issues/jobs", "200", "text/html", "E", "140"],
    ]
    captures = scrape.parse_cdx_response(json.dumps(payload).encode(), _variant(), _target())
    targets = pd.DataFrame([_target()])
    queue, audit = scrape.build_fetch_queue(captures, targets)
    assert len(captures) == 5
    assert len(queue) == 2
    assert queue.iloc[0].page_identity == "/"
    assert set(queue.page_identity) == {"/", "/about"}
    assert audit["cap_drop_rows"] == 1
    assert audit["cap_drop_fraction"] > 0.20


class FakeResponse:
    def __init__(self, status: int, content: bytes = b"", url: str = "https://example"):
        self.status_code = status
        self.content = content
        self.url = url
        self.headers = {"Content-Type": "text/html"}


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def get(self, *args, **kwargs):
        self.calls += 1
        value = self.responses.pop(0)
        if isinstance(value, Exception):
            raise value
        return value


def test_request_retries_retryable_status_and_honors_success() -> None:
    session = FakeSession([FakeResponse(503), FakeResponse(200, b"ok")])
    sleeps = []
    outcome = scrape.request_with_retries(
        session, "https://example", params=None, timeout=(1, 1), attempts=3,
        rate_limiter=scrape.RateLimiter(0, sleep_fn=sleeps.append), sleep_fn=sleeps.append,
    )
    assert outcome.response.status_code == 200
    assert outcome.request_count == 2
    assert sleeps == [5.0]


def test_request_error_remains_retryable_in_fetch_checkpoint(tmp_path: Path) -> None:
    queue = pd.DataFrame([{column: "" for column in scrape.FETCH_QUEUE_COLUMNS}])
    queue.loc[0, ["capture_id", "scrape_key", "target_id", "candidate_year_id", "timestamp", "original_url", "replay_url"]] = [
        "capture-1", "key-1", "target-1", "candidate-1", "20191114000000",
        "http://example.org/", "https://web.archive.org/web/20191114000000id_/http://example.org/",
    ]
    scrape._atomic_csv(queue, tmp_path / "fetch_queue.csv", scrape.FETCH_QUEUE_COLUMNS)
    session = FakeSession([requests.ConnectTimeout("timeout")])
    audit = scrape.run_fetch(
        pd.DataFrame(), tmp_path, session=session, rate_limiter=scrape.RateLimiter(0),
        attempts_per_request=1, timeout=(1, 1), max_raw_bytes=100000,
        circuit_breaker_failures=5, circuit_breaker_sleep=0, sleep_fn=lambda _: None,
    )
    attempts = pd.read_csv(tmp_path / "fetch_attempts.csv")
    assert audit["retryable_rows"] == 1
    assert attempts.iloc[0].fetch_status == "request_error"
    assert bool(attempts.iloc[0].retryable)


def test_redirect_loop_is_terminal_and_not_retried(tmp_path: Path) -> None:
    queue = pd.DataFrame([{column: "" for column in scrape.FETCH_QUEUE_COLUMNS}])
    queue.loc[0, ["capture_id", "scrape_key", "target_id", "candidate_year_id", "timestamp", "original_url", "replay_url"]] = [
        "capture-1", "key-1", "target-1", "candidate-1", "20191114000000",
        "http://example.org/loop", "https://web.archive.org/web/20191114000000id_/http://example.org/loop",
    ]
    scrape._atomic_csv(queue, tmp_path / "fetch_queue.csv", scrape.FETCH_QUEUE_COLUMNS)
    session = FakeSession([requests.TooManyRedirects("loop")])
    first = scrape.run_fetch(
        pd.DataFrame(), tmp_path, session=session, rate_limiter=scrape.RateLimiter(0),
        attempts_per_request=3, timeout=(1, 1), max_raw_bytes=100000,
        circuit_breaker_failures=5, circuit_breaker_sleep=0, sleep_fn=lambda _: None,
    )
    second = scrape.run_fetch(
        pd.DataFrame(), tmp_path, session=session, rate_limiter=scrape.RateLimiter(0),
        attempts_per_request=1, timeout=(1, 1), max_raw_bytes=100000,
        circuit_breaker_failures=5, circuit_breaker_sleep=0, sleep_fn=lambda _: None,
    )
    attempt = pd.read_csv(tmp_path / "fetch_attempts.csv").iloc[0]
    assert first["retryable_rows"] == second["retryable_rows"] == 0
    assert attempt.fetch_status == "redirect_loop"
    assert not bool(attempt.retryable)
    assert session.calls == 1


def test_fetch_stores_raw_and_text_then_resumes_without_refetch(tmp_path: Path) -> None:
    queue = pd.DataFrame([{column: "" for column in scrape.FETCH_QUEUE_COLUMNS}])
    queue.loc[0, ["capture_id", "scrape_key", "target_id", "candidate_year_id", "timestamp", "original_url", "replay_url"]] = [
        "capture-1", "key-1", "target-1", "candidate-1", "20191114000000",
        "http://example.org/", "https://web.archive.org/web/20191114000000id_/http://example.org/",
    ]
    scrape._atomic_csv(queue, tmp_path / "fetch_queue.csv", scrape.FETCH_QUEUE_COLUMNS)
    response = FakeResponse(
        200, b"<html><body>Example Candidate for Mayor of Example</body></html>",
        "https://web.archive.org/web/20191114000000id_/http://example.org/",
    )
    session = FakeSession([response])
    first = scrape.run_fetch(
        pd.DataFrame(), tmp_path, session=session, rate_limiter=scrape.RateLimiter(0),
        attempts_per_request=1, timeout=(1, 1), max_raw_bytes=100000,
        circuit_breaker_failures=5, circuit_breaker_sleep=0, sleep_fn=lambda _: None,
    )
    second = scrape.run_fetch(
        pd.DataFrame(), tmp_path, session=session, rate_limiter=scrape.RateLimiter(0),
        attempts_per_request=1, timeout=(1, 1), max_raw_bytes=100000,
        circuit_breaker_failures=5, circuit_breaker_sleep=0, sleep_fn=lambda _: None,
    )
    assert first["fetched_rows"] == second["fetched_rows"] == 1
    assert session.calls == 1
    attempt = pd.read_csv(tmp_path / "fetch_attempts.csv").iloc[0]
    assert attempt.fetch_status == "fetched"
    assert (tmp_path / attempt.raw_html_path).stat().st_size > 0
    assert "Example Candidate" in (tmp_path / attempt.text_path).read_text()


def test_fetch_request_budget_leaves_remaining_keys_unattempted(tmp_path: Path) -> None:
    rows = []
    for index in range(2):
        row = {column: "" for column in scrape.FETCH_QUEUE_COLUMNS}
        row.update({
            "capture_id": f"capture-{index}", "scrape_key": f"key-{index}",
            "target_id": f"target-{index}", "candidate_year_id": f"candidate-{index}",
            "timestamp": "20191114000000", "original_url": f"http://example.org/{index}",
            "replay_url": f"https://web.archive.org/web/20191114000000id_/http://example.org/{index}",
        })
        rows.append(row)
    scrape._atomic_csv(pd.DataFrame(rows), tmp_path / "fetch_queue.csv", scrape.FETCH_QUEUE_COLUMNS)
    response = FakeResponse(404)
    session = FakeSession([response])
    audit = scrape.run_fetch(
        pd.DataFrame(), tmp_path, session=session, rate_limiter=scrape.RateLimiter(0),
        attempts_per_request=1, timeout=(1, 1), max_raw_bytes=100000,
        circuit_breaker_failures=5, circuit_breaker_sleep=0, request_budget=1,
        sleep_fn=lambda _: None,
    )
    assert audit["attempt_rows"] == 1
    assert audit["missing_attempt_rows"] == 1
    assert audit["stage"] == "fetch_partial"


def test_fetch_restart_recovers_durable_journal_without_refetch(tmp_path: Path) -> None:
    queue = pd.DataFrame([{column: "" for column in scrape.FETCH_QUEUE_COLUMNS}])
    queue.loc[0, ["capture_id", "scrape_key", "target_id", "candidate_year_id", "timestamp", "original_url", "replay_url"]] = [
        "capture-1", "key-1", "target-1", "candidate-1", "20191114000000",
        "http://example.org/", "https://web.archive.org/web/20191114000000id_/http://example.org/",
    ]
    scrape._atomic_csv(queue, tmp_path / "fetch_queue.csv", scrape.FETCH_QUEUE_COLUMNS)
    row = {column: "" for column in scrape.FETCH_ATTEMPT_COLUMNS}
    row.update({
        "capture_id": "capture-1", "scrape_key": "key-1", "target_id": "target-1",
        "candidate_year_id": "candidate-1", "fetch_status": "fetched", "retryable": False,
        "raw_html_bytes": 10, "text_characters": 5,
    })
    scrape._append_csv_row(row, tmp_path / "fetch_attempts.journal.csv", scrape.FETCH_ATTEMPT_COLUMNS)
    session = FakeSession([])
    audit = scrape.run_fetch(
        pd.DataFrame(), tmp_path, session=session, rate_limiter=scrape.RateLimiter(0),
        attempts_per_request=1, timeout=(1, 1), max_raw_bytes=100000,
        circuit_breaker_failures=5, circuit_breaker_sleep=0, sleep_fn=lambda _: None,
    )
    assert session.calls == 0
    assert audit["attempt_rows"] == 1
    assert audit["exact_queue_key_agreement"]
    assert not (tmp_path / "fetch_attempts.journal.csv").exists()
    assert pd.read_csv(tmp_path / "fetch_attempts.csv").iloc[0].fetch_status == "fetched"


def test_fetch_checkpoint_latest_journal_row_wins_then_compacts(tmp_path: Path) -> None:
    base = {column: "" for column in scrape.FETCH_ATTEMPT_COLUMNS}
    base.update({"capture_id": "capture-1", "fetch_status": "request_error", "retryable": True})
    latest = dict(base)
    latest.update({"fetch_status": "fetched", "retryable": False, "attempted_at_utc": "later"})
    scrape._atomic_csv(pd.DataFrame([base]), tmp_path / "fetch_attempts.csv", scrape.FETCH_ATTEMPT_COLUMNS)
    scrape._append_csv_row(latest, tmp_path / "fetch_attempts.journal.csv", scrape.FETCH_ATTEMPT_COLUMNS)
    recovered = scrape._read_fetch_checkpoint(
        tmp_path / "fetch_attempts.csv", tmp_path / "fetch_attempts.journal.csv"
    )
    assert len(recovered) == 1
    assert recovered.iloc[0].fetch_status == "fetched"
    scrape._compact_fetch_checkpoint(
        recovered, tmp_path / "fetch_attempts.csv", tmp_path / "fetch_attempts.journal.csv"
    )
    assert not (tmp_path / "fetch_attempts.journal.csv").exists()
    assert pd.read_csv(tmp_path / "fetch_attempts.csv").iloc[0].fetch_status == "fetched"


def test_parallel_fetch_records_every_key_once(tmp_path: Path, monkeypatch) -> None:
    rows = []
    for index in range(8):
        row = {column: "" for column in scrape.FETCH_QUEUE_COLUMNS}
        row.update({
            "capture_id": f"capture-{index}", "scrape_key": f"key-{index}",
            "target_id": f"target-{index}", "candidate_year_id": f"candidate-{index}",
            "timestamp": "20191114000000", "original_url": f"http://example.org/{index}",
            "replay_url": f"https://web.archive.org/web/20191114000000id_/http://example.org/{index}",
        })
        rows.append(row)
    scrape._atomic_csv(pd.DataFrame(rows), tmp_path / "fetch_queue.csv", scrape.FETCH_QUEUE_COLUMNS)
    lock = threading.Lock()
    calls = []

    class Always404:
        def get(self, url, **kwargs):
            with lock:
                calls.append(url)
            return FakeResponse(404, url=url)

    monkeypatch.setattr(scrape, "make_session", Always404)
    audit = scrape.run_fetch(
        pd.DataFrame(), tmp_path, session=Always404(), rate_limiter=scrape.RateLimiter(0),
        attempts_per_request=1, timeout=(1, 1), max_raw_bytes=100000,
        circuit_breaker_failures=5, circuit_breaker_sleep=0, workers=4,
        checkpoint_interval=3, sleep_fn=lambda _: None,
    )
    attempts = pd.read_csv(tmp_path / "fetch_attempts.csv")
    assert len(calls) == 8
    assert len(attempts) == attempts.capture_id.nunique() == 8
    assert set(attempts.fetch_status) == {"not_available"}
    assert audit["exact_queue_key_agreement"]
    assert audit["workers"] == 4


def test_fetch_streams_pending_rows_and_merges_updates_in_batches(
    tmp_path: Path, monkeypatch
) -> None:
    rows = []
    for index in range(5):
        row = {column: "" for column in scrape.FETCH_QUEUE_COLUMNS}
        row.update({
            "capture_id": f"capture-{index}", "scrape_key": f"key-{index}",
            "target_id": f"target-{index}", "candidate_year_id": f"candidate-{index}",
            "timestamp": "20191114000000", "original_url": f"http://example.org/{index}",
            "replay_url": f"https://web.archive.org/web/20191114000000id_/http://example.org/{index}",
        })
        rows.append(row)
    scrape._atomic_csv(pd.DataFrame(rows), tmp_path / "fetch_queue.csv", scrape.FETCH_QUEUE_COLUMNS)

    monkeypatch.setattr(
        scrape, "_replace_row",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("full table copy")),
    )
    session = FakeSession([FakeResponse(404) for _ in rows])
    audit = scrape.run_fetch(
        pd.DataFrame(), tmp_path, session=session, rate_limiter=scrape.RateLimiter(0),
        attempts_per_request=1, timeout=(1, 1), max_raw_bytes=100000,
        circuit_breaker_failures=5, circuit_breaker_sleep=0, workers=1,
        checkpoint_interval=2, sleep_fn=lambda _: None,
    )

    attempts = pd.read_csv(tmp_path / "fetch_attempts.csv")
    assert len(attempts) == 5
    assert attempts.capture_id.nunique() == 5
    assert audit["exact_queue_key_agreement"]
    assert not (tmp_path / "fetch_attempts.journal.csv").exists()


def test_pending_fetch_rows_honors_completed_keys_and_budget(tmp_path: Path) -> None:
    rows = []
    for index in range(4):
        row = {column: "" for column in scrape.FETCH_QUEUE_COLUMNS}
        row["capture_id"] = f"capture-{index}"
        rows.append(row)
    queue_path = tmp_path / "fetch_queue.csv"
    scrape._atomic_csv(pd.DataFrame(rows), queue_path, scrape.FETCH_QUEUE_COLUMNS)

    pending = scrape._pending_fetch_rows(queue_path, {"capture-0"}, request_budget=2)
    assert iter(pending) is pending
    assert [row["capture_id"] for row in pending] == ["capture-1", "capture-2"]


def test_merge_fetch_updates_replaces_only_matching_capture() -> None:
    base_rows = []
    for capture_id in ("capture-1", "capture-2"):
        row = {column: "" for column in scrape.FETCH_ATTEMPT_COLUMNS}
        row.update({"capture_id": capture_id, "fetch_status": "request_error"})
        base_rows.append(row)
    update = dict(base_rows[0])
    update["fetch_status"] = "fetched"

    merged = scrape._merge_fetch_updates(pd.DataFrame(base_rows), [update])

    assert len(merged) == 2
    assert merged.set_index("capture_id").loc["capture-1", "fetch_status"] == "fetched"
    assert merged.set_index("capture_id").loc["capture-2", "fetch_status"] == "request_error"
