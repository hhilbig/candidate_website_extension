"""Compare streamed SQLite exports with the existing in-memory export contract."""
from __future__ import annotations

import csv
import gzip
import json

import pytest

from src.collection_2026 import Store
from src.collection_2026_outputs import _stream_export, _write_csv, write_outputs, write_store_outputs


def candidate(cid="TEST1", **extra):
    return dict(candidate_id=cid, candidate="Example Candidate", state="CA", district="01",
                office="house", party="D", year=2026, website_url="https://example.test/", **extra)


def event(eid="primary", date="2026-06-15"):
    return dict(event_id=eid, date=date, state="CA", office="house", districts=["*"],
                parties=["D", "R"], event_type="primary", round="primary",
                source_url="https://authority.test/calendar", timezones=["America/Los_Angeles"])


def page(pid, stamp, cid="TEST1", **extra):
    return dict(dict(page_id=pid, candidate_id=cid, snapshot_id=pid, source="wayback",
                     captured_at=stamp, page_type="homepage", depth=0, status="usable",
                     identity_status="confirmed", html_path="never-read.html.gz",
                     text_path="never-read.txt.gz", snapshot_complete=True), **extra)


def fixture_store(tmp_path):
    store = Store(tmp_path / "store")
    pages = [
        page("pre", "2026-06-14T19:00:00Z", snapshot_id="snapshot-pre", optional_first={"n": 1}),
        page("issue", "2026-06-14T20:00:00Z", snapshot_id="snapshot-pre", page_type="issues", depth=1),
        page("post", "2026-06-16T19:00:00Z", source="live"),
        page("failed", None, cid="TEST3", status="request_error", errors={"message": "timeout"},
             requested_event_id="primary", requested_side="pre", optional_later=["preserve", "this"]),
        page("nonevent", "2026-06-16T19:00:00Z", cid="TEST4", optional_null=None),
        page("orphan", "2026-06-16T19:00:00Z", cid="ORPHAN", snapshot_id="post", snapshot_complete=False),
        page("orphan-extra", "2026-06-16T20:00:00Z", cid="ORPHAN", snapshot_id="post", page_type="news", depth=1),
    ]
    for row in pages:
        store.page(row)
    work = [
        ("old", "cdx_inventory", "TEST1", "superseded", {"superseded_by": "replacement"}),
        ("replacement", "cdx_inventory", "TEST3", "empty", {}),
        ("failed", "archive_snapshot", "TEST3", "failed", {"errors": ["timeout"]}),
        ("pending", "archive_snapshot", "TEST3", "pending", {}),
        ("orphan-work", "archive_snapshot", "ORPHAN", "", {}),
    ]
    for key, kind, cid, status, result in work:
        store.add_work(key, kind, cid, {"candidate_url": "https://example.test/"})
        store.update(key, status, result)
    for index in range(5):
        row = dict(timestamp=f"202606{index + 10:02}190000", original=f"https://example.test/{index}",
                   mimetype="text/html", statuscode="200", digest="same-digest", offset=str(index), candidate_id="TEST1")
        if index == 4:
            row.update(late_field={"metadata": [1, 2]}, empty_field=None)
        store.capture("inventory", row)
    store.db.commit()
    done = store.attempt("failed", "https://example.test/")
    store.finish_attempt(done, "http_error", {"http_status": 503})
    store.attempt("pending", "https://example.test/second")
    return store


def test_streamed_outputs_match_all_existing_products_and_denominators(tmp_path, monkeypatch):
    pq = pytest.importorskip("pyarrow.parquet")
    store = fixture_store(tmp_path)
    candidates = [candidate(), dict(candidate("TEST2"), website_url=""), candidate("TEST3"),
                  dict(candidate("TEST4"), state="AA")]
    events = [event(), dict(event("runoff", "2026-07-06"), event_type="primary_runoff", round="runoff")]
    expected_root, actual_root = tmp_path / "expected", tmp_path / "actual"
    expected = write_outputs(expected_root, candidates, events, store.rows("pages"), store.rows("work"))
    _write_csv(expected_root / "capture_inventory.csv", store.rows("captures"),
               ("capture_id", "inventory_key", "timestamp", "original"))
    monkeypatch.setattr(store, "rows", lambda *args: pytest.fail("Stream export must not call Store.rows"))
    actual = write_store_outputs(actual_root, candidates, events, store, batch_size=2)
    for key, value in expected.items():
        if key not in {"generated_at", "outputs"}:
            assert actual[key] == value, key
    assert actual["distinct_candidates_by_event_coverage"]["paired"] == 1
    assert actual["distinct_candidates_by_event_coverage"]["pre_only"] == 1
    assert actual["candidate_coverage_count_note"] == expected["candidate_coverage_count_note"]
    assert actual["orphan_page_count"] == 2
    assert actual["capture_inventory_count"] == 5
    assert actual["attempts"] == 2
    assert actual["attempt_ids_unique"]
    assert actual["unfinished_attempts"] == 1
    for expected_csv in expected_root.glob("*.csv"):
        actual_bytes = (gzip.decompress((actual_root / "capture_inventory.csv.gz").read_bytes())
                        if expected_csv.name == "capture_inventory.csv" else (actual_root / expected_csv.name).read_bytes())
        assert actual_bytes == expected_csv.read_bytes(), expected_csv.name
    for expected_pq in expected_root.glob("*.parquet"):
        actual_path = actual_root / expected_pq.name
        assert pq.read_table(actual_path).equals(pq.read_table(expected_pq)), expected_pq.name
        metadata = pq.ParquetFile(actual_path).metadata
        assert all(metadata.row_group(i).num_rows <= 2 for i in range(metadata.num_row_groups))
    assert not (actual_root / "capture_inventory.parquet").exists()
    assert not list(actual_root.glob(".*"))
    inventory_rows = list(csv.DictReader(gzip.open(actual_root / "capture_inventory.csv.gz", "rt")))
    assert json.loads(inventory_rows[-1]["late_field"]) == {"metadata": [1, 2]}
    assert len({row["capture_id"] for row in inventory_rows}) == 5
    store.db.close()


def test_empty_store_exports_have_the_same_schemas_and_no_missing_denominator(tmp_path, monkeypatch):
    pq = pytest.importorskip("pyarrow.parquet")
    store = Store(tmp_path / "store")
    expected_root, actual_root = tmp_path / "expected", tmp_path / "actual"
    expected = write_outputs(expected_root, [dict(candidate(), website_url="")], [event()], [], [])
    monkeypatch.setattr(store, "rows", lambda *args: pytest.fail("No materialized store reads"))
    actual = write_store_outputs(actual_root, [dict(candidate(), website_url="")], [event()], store)
    for key in expected:
        if key not in {"generated_at", "outputs"}:
            assert actual[key] == expected[key], key
    for path in expected_root.glob("*.csv"):
        assert (actual_root / path.name).read_bytes() == path.read_bytes()
    assert pq.read_table(actual_root / "page_corpus.parquet").num_rows == 0
    assert pq.read_table(actual_root / "page_corpus.parquet").schema == pq.read_table(expected_root / "page_corpus.parquet").schema
    assert actual["capture_inventory_count"] == actual["attempts"] == actual["unfinished_attempts"] == 0
    assert actual["every_work_item_terminal"] and actual["every_work_item_has_status"]
    assert gzip.decompress((actual_root / "capture_inventory.csv.gz").read_bytes()).decode().strip() == "capture_id,inventory_key,timestamp,original"
    store.db.close()


def test_orphan_only_store_and_snapshot_edge_cases_match_existing_counts(tmp_path):
    store = Store(tmp_path / "store")
    for index, (snapshot, depth, complete) in enumerate([
        (None, 0, False), ("", 0, "0"), (0, 0, False), ("0", 0, True),
        ("repeated", "0", False), ("repeated", 0, False), ("floatdepth", 0.0, False),
    ]):
        store.page(page(str(index), "2026-06-14T19:00:00Z", snapshot_id=snapshot, depth=depth,
                        snapshot_complete=complete))
    expected = write_outputs(tmp_path / "expected", [], [], store.rows("pages"), [])
    actual = write_store_outputs(tmp_path / "actual", [], [], store, batch_size=2)
    for key in expected:
        if key not in {"generated_at", "outputs"}:
            assert actual[key] == expected[key], key
    assert actual["page_count"] == actual["orphan_page_count"] == 7
    store.db.close()


def test_stream_schema_includes_late_columns_without_a_full_row_list(tmp_path):
    pq = pytest.importorskip("pyarrow.parquet")
    calls = []
    def rows():
        calls.append(1)
        for index in range(103):
            yield {"page_id": str(index), **({"last_optional": ["late"]} if index == 102 else {})}
    result = _stream_export(tmp_path, "pages", rows, ["page_id"], batch_size=7)
    assert result["row_count"] == 103
    assert len(calls) == 2
    table = pq.read_table(tmp_path / "pages.parquet")
    assert table.num_rows == 103
    assert table["last_optional"][-1].as_py() == '["late"]'
    metadata = pq.ParquetFile(tmp_path / "pages.parquet").metadata
    assert metadata.num_row_groups == 15
    assert max(metadata.row_group(i).num_rows for i in range(metadata.num_row_groups)) == 7


def test_failed_stream_keeps_previous_files_and_removes_temporary_files(tmp_path):
    pytest.importorskip("pyarrow.parquet")
    _stream_export(tmp_path, "pages", lambda: iter([{"page_id": "old"}]), batch_size=1)
    previous = {path.name: path.read_bytes() for path in tmp_path.glob("*")}
    calls = []
    def changed_rows():
        calls.append(1)
        yield {"page_id": "new"}
        if len(calls) == 2:
            raise RuntimeError("interrupted SQLite export")
    with pytest.raises(RuntimeError, match="interrupted"):
        _stream_export(tmp_path, "pages", changed_rows, batch_size=1)
    assert {path.name: path.read_bytes() for path in tmp_path.glob("*")} == previous


def test_store_export_rejects_invalid_batch_size_and_duplicate_candidates(tmp_path):
    store = Store(tmp_path / "store")
    with pytest.raises(ValueError, match="batch_size"):
        write_store_outputs(tmp_path / "exports", [], [], store, batch_size=0)
    with pytest.raises(ValueError, match="candidate_id"):
        write_store_outputs(tmp_path / "exports", [candidate(), candidate()], [], store)
    store.db.close()


def test_gzip_inventory_is_atomic_reproducible_and_does_not_delete_legacy_csv(tmp_path):
    rows = lambda: iter([{'capture_id': 'one', 'original': 'https://example.test/about'}])
    legacy = tmp_path / 'capture_inventory.csv'
    legacy.write_text('legacy metadata\n')
    result = _stream_export(tmp_path, 'capture_inventory', rows, parquet=False, csv_gzip=True)
    expected = (tmp_path / 'capture_inventory.csv.gz').read_bytes()
    assert result['csv_compressed']
    assert len(list(csv.DictReader(gzip.open(result['csv_path'], 'rt')))) == 1
    _stream_export(tmp_path, 'capture_inventory', rows, parquet=False, csv_gzip=True)
    assert (tmp_path / 'capture_inventory.csv.gz').read_bytes() == expected
    passes = []
    def changed_rows():
        passes.append(1)
        yield {'capture_id': 'one'}
        if len(passes) == 2:
            yield {'capture_id': 'two'}
    with pytest.raises(RuntimeError, match='row count changed'):
        _stream_export(tmp_path, 'capture_inventory', changed_rows, parquet=False, csv_gzip=True)
    assert (tmp_path / 'capture_inventory.csv.gz').read_bytes() == expected
    assert legacy.read_text() == 'legacy metadata\n'
    assert not list(tmp_path.glob('.*'))
