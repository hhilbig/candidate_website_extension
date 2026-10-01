from pathlib import Path

import pandas as pd
import pytest

from scripts import build_mayoral_all_snapshot_fetch_queue as builder
from scripts import scrape_mayoral_wayback_batch as scrape


def capture(capture_id: str, target_id: str, timestamp: str, path: str) -> dict[str, object]:
    row = {column: "" for column in scrape.CAPTURE_COLUMNS}
    row.update({
        "capture_id": capture_id,
        "variant_attempt_id": f"variant-{capture_id}",
        "scrape_key": f"key-{target_id}",
        "target_id": target_id,
        "candidate_year_id": f"candidate-{target_id}",
        "timestamp": timestamp,
        "original_url": f"http://example.com{path}",
        "digest": f"digest-{capture_id}",
        "replay_url": f"https://web.archive.org/web/{timestamp}id_/http://example.com{path}",
        "capture_days_absolute": 10,
        "page_identity": path,
    })
    return row


def test_queue_preserves_every_capture_including_same_path_snapshots() -> None:
    captures = pd.DataFrame([
        capture("c1", "t1", "20200101000000", "/"),
        capture("c2", "t1", "20200201000000", "/"),
        capture("c3", "t1", "20200301000000", "/about"),
        capture("c4", "t2", "20200401000000", "/"),
    ]).reindex(columns=scrape.CAPTURE_COLUMNS)

    queue = builder.build_all_snapshot_queue(captures)

    assert queue.columns.tolist() == scrape.FETCH_QUEUE_COLUMNS
    assert set(queue.capture_id) == {"c1", "c2", "c3", "c4"}
    assert len(queue) == 4
    assert queue.capture_id.is_unique
    assert set(queue.selection_reason) == {"all_inventoried_unique_content_captures_uncapped"}
    assert set(queue.max_pages_per_target) == {-1}
    assert queue.loc[queue.target_id.eq("t1"), "target_queue_rank"].tolist() == [1, 2, 3]


def test_queue_rejects_duplicate_capture_keys() -> None:
    captures = pd.DataFrame([
        capture("c1", "t1", "20200101000000", "/"),
        capture("c1", "t1", "20200201000000", "/"),
    ]).reindex(columns=scrape.CAPTURE_COLUMNS)

    with pytest.raises(builder.AllSnapshotQueueError, match="duplicate"):
        builder.build_all_snapshot_queue(captures)


def test_queue_rejects_schema_drift() -> None:
    captures = pd.DataFrame([capture("c1", "t1", "20200101000000", "/")])
    captures = captures.drop(columns=[scrape.CAPTURE_COLUMNS[-1]])

    with pytest.raises(builder.AllSnapshotQueueError, match="schema"):
        builder.build_all_snapshot_queue(captures)
