import json

import pytest
import requests

from scripts import expand_mayoral_saved_sources as m


def test_tranche_restart_keeps_interrupted_precharges_and_limits(tmp_path):
    db = m.connect(tmp_path)
    budget = m.TrancheBudget(db, "approved", 100, 2)
    token, reason = budget.reserve(70)
    assert token and not reason
    db.close()
    db = m.connect(tmp_path)
    budget = m.TrancheBudget(db, "approved", 100, 2)
    assert budget.reserve(31) == (None, "tranche_byte_limit")
    token2, _ = budget.reserve(30)
    budget.settle(token2, 5)
    assert budget.state["charged_bytes"] == 75
    assert budget.reserve(1) == (None, "bounded_run")
    with pytest.raises(ValueError, match="ceilings differ"):
        m.TrancheBudget(db, "approved", 101, 2)


def test_multiple_worker_reservations_cannot_exceed_bytes(tmp_path):
    budget = m.TrancheBudget(m.connect(tmp_path), "approved", 100, 10)
    first, _ = budget.reserve(60)
    assert budget.reserve(60) == (None, "tranche_byte_limit")
    budget.settle(first, 15)
    second, _ = budget.reserve(60)
    assert second
    assert budget.state["charged_bytes"] == 75


@pytest.mark.parametrize("failure", ["size", "timeout"])
def test_stream_counts_bytes_before_size_or_transport_failure(tmp_path, monkeypatch, failure):
    monkeypatch.setattr(m, "assert_public", lambda url: None)

    class Response:
        status_code = 200
        headers = {"Content-Type": "text/html"}

        def iter_content(self, chunk_size):
            yield b"12345"
            if failure == "timeout":
                raise requests.Timeout("partial stream")
            yield b"67890"

        def close(self):
            pass

    class Session:
        def get(self, *args, **kwargs):
            return Response()

    result, _ = m.fetch_document({"source_id": "s", "url": "https://example.org"},
                                 tmp_path, tmp_path, max_bytes=7, session=Session())
    assert result["downloaded_bytes"] == (10 if failure == "size" else 5)
    assert result["status"] == ("size_limit" if failure == "size" else "request_error")


def test_worker_source_limit_and_restart_do_not_overshoot(tmp_path, monkeypatch):
    run = tmp_path / "run"
    db = m.connect(run)
    candidate = {"candidate_year_id": "c1", "full_name": "Jane Doe", "contest": "race1",
                 "geo_name": "Boston", "state_abb": "MA", "year": "2017", "month": "11"}
    db.execute("INSERT INTO candidates VALUES(?,?)", ("c1", json.dumps(candidate)))
    for i in range(10):
        m.add_source(db, url=f"https://host{i}.example/source", candidate_id="c1", origin="test",
                     input_path="fixture", input_row=i)
    db.commit()
    db.close()
    monkeypatch.setattr(m, "extract_saved_snippets", lambda *a: None)
    monkeypatch.setattr(m, "repair_empty_response_statuses", lambda *a: None)
    monkeypatch.setattr(m, "refresh_attribution", lambda *a, **k: None)

    def fetch(source, *args, **kwargs):
        return {"status": "http_error", "retryable": False, "used_cache": False,
                "downloaded_bytes": 0, "content_bytes": 0, "request_count": 1,
                "ended_at": m.now(), "error": "HTTP 404", "http_status": 404}, None

    monkeypatch.setattr(m, "fetch_document", fetch)
    args = dict(workers=8, max_sources=3, tranche_id="approved", additional_bytes=1_000_000_000,
                min_free_bytes=0, host_delay=0, total_bytes=0)
    first = m.run_fetch(tmp_path, run, **args)
    second = m.run_fetch(tmp_path, run, **args)
    assert first["tranche"]["attempts_started"] == second["tranche"]["attempts_started"] == 3
    db = m.connect(run)
    assert db.execute("SELECT COUNT(*) FROM attempts").fetchone()[0] == 3
    assert second["stop_reason"] == "bounded_run"
