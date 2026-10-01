import csv
import gzip
import hashlib
import json
import sqlite3

import pytest
import requests

import scripts.inventory_mayoral_accepted_urls as runner

CLOCK = [1_000_000.0]


class Response:
    def __init__(self, body=b"", status=200, headers=None, crash=False):
        self.body = body
        self.status_code = status
        self.headers = headers or {"Content-Type": "text/plain; charset=utf-8"}
        self.crash = crash
        self.closed = False

    def iter_content(self, chunk_size):
        for offset in range(0, len(self.body), chunk_size):
            yield self.body[offset:offset + chunk_size]
        if self.crash:
            raise KeyboardInterrupt()

    def close(self):
        self.closed = True


class Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        assert url == runner.ENDPOINT
        assert kwargs["allow_redirects"] is False
        assert kwargs["stream"] is True
        self.calls.append((url, kwargs))
        response = self.responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


def line(timestamp="20200101000000", original="https://example.org/0", digest="ABC"):
    return f"org,example)/0 {timestamp} {original} text/html 200 {digest} 123"


@pytest.fixture
def setup(tmp_path, monkeypatch):
    CLOCK[0] = 1_000_000.0
    monkeypatch.setattr(runner.time, "time", lambda: CLOCK[0])
    monkeypatch.setattr(runner.shutil, "disk_usage",
                        lambda _: type("Usage", (), {"free": 10_000_000_000})())

    def create(count=1):
        rows = [{"candidate_url_id": f"candidate_url_{i}", "candidate_year_id": f"{i}__2020",
                 "exact_normalized_url": f"https://example.org/{i}",
                 "proposed_window_start": "2019-01-01", "proposed_window_end": "2021-12-31"}
                for i in range(count)]
        queue = tmp_path / "queue.csv"
        with queue.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
            writer.writeheader()
            writer.writerows(rows)
        return queue, tmp_path / "output", rows

    return create


def run(args, session, count=None):
    def advance(seconds):
        CLOCK[0] += seconds
    return runner.run_inventory(args[0], args[1], count, session=session, sleep=advance)


def test_empty_plaintext_is_terminal_and_resume_skips(setup):
    args = setup()
    result = run(args, Session([Response(b"")]))
    assert result["statuses"] == {"EMPTY": 1}
    assert result["inventory_complete"]
    assert result["attempts"] == 1
    assert run(args, Session([]))["attempts"] == 1


def test_independent_verifier_checks_saved_bytes_and_rows(setup):
    from scripts.verify_mayoral_accepted_inventory import verify
    args = setup()
    run(args, Session([Response((line() + '\n').encode())]))
    result = verify(args[0], args[1])
    assert result['all_queries_terminal']
    assert result['accepted_cdx_rows'] == result['raw_response_hashes_verified'] == 1
    db = sqlite3.connect(args[1] / 'ledger.sqlite3')
    db.execute("UPDATE capture_rows SET digest='TAMPERED'")
    db.commit()
    db.close()
    with pytest.raises(AssertionError):
        verify(args[0], args[1])


@pytest.mark.parametrize("body,headers", [
    (b'{"error":"rate limited"}', {"Content-Type": "application/json"}),
    (b"<html>error</html>", {"Content-Type": "text/html"}),
])
def test_json_and_html_errors_are_not_empty(setup, body, headers):
    args = setup()
    result = run(args, Session([Response(body, headers=headers)]), 1)
    assert result["statuses"] == {"RETRYABLE": 1}
    assert not result["inventory_complete"]


def test_truncated_or_malformed_row_is_retryable(setup):
    args = setup()
    result = run(args, Session([Response(b"org,example)/0 20200101")]), 1)
    assert result["statuses"] == {"RETRYABLE": 1}
    assert result["capture_rows_all_attempts"] == 0


def test_continuation_key_decoded_once_and_pages_complete(setup):
    args = setup()
    first = (line() + "\n\norg%2Cexample%29%2F0+20200101000000%21\n").encode()
    second = (line("20200202000000") + "\n").encode()
    session = Session([Response(first), Response(second)])
    result = run(args, session)
    assert result["statuses"] == {"COMPLETE": 1}
    assert result["pages"] == 2
    assert result["capture_rows_accepted"] == 2
    assert result["page_chain_issues"] == []
    assert session.calls[1][1]["params"]["resumeKey"] == "org,example)/0 20200101000000!"
    assert "%2" not in session.calls[1][1]["params"]["resumeKey"]


def test_duplicate_rows_and_same_digest_all_timestamps_are_retained(setup):
    args = setup()
    body = "\n".join([line(), line(), line("20200202000000", digest="ABC")]) + "\n"
    result = run(args, Session([Response(body.encode())]))
    assert result["capture_rows_accepted"] == 3
    assert result["accepted_duplicate_rows_retained"] == 1
    db = sqlite3.connect(args[1]/"ledger.sqlite3")
    assert db.execute("SELECT COUNT(DISTINCT timestamp) FROM capture_rows WHERE digest='ABC'").fetchone()[0] == 2
    db.close()


def test_full_page_without_token_is_cautiously_incomplete(setup, monkeypatch):
    monkeypatch.setitem(runner.LIMITS, "page_rows", 2)
    args = setup()
    body = (line() + "\n" + line("20200202000000") + "\n").encode()
    result = run(args, Session([Response(body)]), 1)
    assert result["statuses"] == {"RETRYABLE": 1}
    assert result["capture_rows_all_attempts"] == 2
    assert result["capture_rows_accepted"] == 0


def test_resume_loop_is_detected(setup):
    args = setup()
    trailer = "\n\nkey%21\n"
    session = Session([Response((line()+trailer).encode()),
                       Response((line("20200202000000")+trailer).encode())])
    result = run(args, session, 2)
    assert result["statuses"] == {"RETRYABLE": 1}
    assert result["capture_rows_accepted"] == 1


def test_out_of_window_timestamp_and_invalid_original_rejected(setup):
    for body in (line("20230101000000"), line(original="not-a-url")):
        args = setup()
        result = run(args, Session([Response((body+"\n").encode())]), 1)
        assert result["statuses"] == {"RETRYABLE": 1}


def test_retries_are_fair_and_three_per_page(setup):
    args = setup(2)
    session = Session([Response(b"busy", 503), Response((line(original="https://example.org/1")+"\n").encode())])
    result = run(args, session, 2)
    assert result["statuses"] == {"RETRYABLE": 1, "COMPLETE": 1}
    for _ in range(2):
        db = sqlite3.connect(args[1]/"ledger.sqlite3")
        db.execute("UPDATE queries SET next_due=0")
        db.commit()
        db.close()
        result = run(args, Session([Response(b"busy", 503)]), 1)
    assert result["attempts"] == 4
    assert run(args, Session([]))["attempts"] == 4


def test_crash_reservation_survives_resume(setup):
    args = setup()
    with pytest.raises(KeyboardInterrupt):
        run(args, Session([Response((line()+"\n").encode(), crash=True)]))
    result = run(args, Session([]), 0)
    assert result["charged_decoded_bytes"] == runner.LIMITS["max_response_decoded_bytes"]
    assert result["statuses"] == {"RETRYABLE": 1}
    db = sqlite3.connect(args[1]/"ledger.sqlite3")
    assert db.execute("SELECT status FROM attempts").fetchone()[0] == "INTERRUPTED"
    db.close()


def test_completed_request_refunds_unused_reservation_and_raw_is_gzip(setup):
    args = setup()
    body = (line()+"\n").encode()
    result = run(args, Session([Response(body)]))
    assert result["charged_decoded_bytes"] == len(body)
    db = sqlite3.connect(args[1]/"ledger.sqlite3")
    raw_path, raw_hash = db.execute("SELECT raw_path,raw_sha256 FROM attempts").fetchone()
    db.close()
    saved = gzip.decompress((args[1]/raw_path).read_bytes())
    assert saved == body
    assert hashlib.sha256(saved).hexdigest() == raw_hash


def test_network_error_is_retryable_and_counted(setup):
    args = setup()
    result = run(args, Session([requests.Timeout("late")]), 1)
    assert result["attempts"] == 1
    assert result["statuses"] == {"RETRYABLE": 1}


def test_circuit_stops_after_five_consecutive_failures(setup):
    args = setup(6)
    result = run(args, Session([Response(b"busy", 503) for _ in range(5)]))
    assert result["attempts"] == 5
    assert result["stop_reason"] == "circuit_open"


def test_circuit_can_resume_after_cooldown(setup):
    args = setup(6)
    run(args, Session([Response(b"busy", 503) for _ in range(5)]))
    CLOCK[0] += 301
    result = run(args, Session([Response(b"")]), 1)
    assert result["attempts"] == 6
    assert result["statuses"]["EMPTY"] == 1


def test_budget_stops_before_request(setup, monkeypatch):
    monkeypatch.setitem(runner.LIMITS, "max_total_decoded_bytes", runner.LIMITS["max_response_decoded_bytes"])
    args = setup(2)
    first = Session([Response((line()+"\n").encode())])
    run(args, first, 1)
    db = sqlite3.connect(args[1]/"ledger.sqlite3")
    db.execute("UPDATE attempts SET charged_bytes=?", (runner.LIMITS["max_response_decoded_bytes"],))
    db.commit()
    db.close()
    session = Session([])
    result = run(args, session)
    assert not session.calls
    assert result["stop_reason"] == "decoded_budget"


def test_frozen_hash_contract_rejects_changed_queue(setup):
    args = setup()
    run(args, Session([]), 0)
    args[0].write_text(args[0].read_text()+"\n")
    with pytest.raises(ValueError, match="contract|hash"):
        run(args, Session([]), 0)


def test_request_contract_has_no_filter_or_collapse(setup):
    args = setup()
    session = Session([Response(b"")])
    run(args, session)
    params = session.calls[0][1]["params"]
    assert params["matchType"] == "exact"
    assert "output" not in params
    assert params["showResumeKey"] == "true"
    assert params["fl"] == ",".join(runner.FIELDS)
    assert "filter" not in params and "collapse" not in params


def test_one_second_delay_persists_across_resume(setup):
    args = setup(2)
    run(args, Session([Response(b"")]), 1)
    before = CLOCK[0]
    run(args, Session([Response(b"")]), 1)
    assert CLOCK[0] >= before + 1
