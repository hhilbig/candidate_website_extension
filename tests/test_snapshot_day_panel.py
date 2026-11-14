"""Focused structural tests for the snapshot-day data product.

The production Manifesto/SVM fit is intentionally not trained here.  A small
deterministic classifier exercises document batching, class alignment, topic
sums, and unsupported-text behavior without turning the unit suite into a
multi-minute model integration test.
"""
from __future__ import annotations

import hashlib
import stat
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from scripts.build_snapshot_day_panel import (
    MEAN_COLUMNS,
    build_snapshot_day_panel,
    build_snapshot_day_rows,
    topic_columns,
)
from scripts.icpsr_replicate_coding import icpsr_clean_text, page_measures


TOPIC_NAMES = [f"Topic {index:02d}" for index in range(31)]


class FakeTopicPipeline:
    """Return deterministic, valid 31-class probabilities and record calls."""

    def __init__(self) -> None:
        self.documents: list[str] = []

    def predict_proba(self, documents: list[str]) -> np.ndarray:
        self.documents.extend(documents)
        base = np.arange(1, 32, dtype=float)
        probabilities = []
        for document in documents:
            shifted = np.roll(base, len(document) % len(base))
            probabilities.append(shifted / shifted.sum())
        return np.asarray(probabilities)


def _raw_row(
    *,
    date: str,
    page_type: str,
    text: str,
    page_url: str,
    candidate_cycle_id: str = "H0AA00100-2020",
    state: str = "AA",
) -> dict:
    cand_id = candidate_cycle_id.split("-")[0]
    return {
        "candidate_cycle_id": candidate_cycle_id,
        "candidate_icpsr": "Candidate, Alice",
        "name_key": "candidate_alice",
        "cand_id": cand_id,
        "cand_election_yr": "2020",
        "cand_status": "C",
        "state": state,
        "district": "1",
        "district_id": f"{state}01",
        "office": "house",
        "year": 2020,
        "party": "democrat",
        "stage": None,
        "data_source": "election_year_wayback",
        "candidate_year_stage": "",
        "universe_source": "same_year_fec",
        "on_ballot": False,
        "date": date,
        "page_type": page_type,
        "n_char": len(text),
        "n_words": len(text.split()),
        "text_snap_content": text,
        "urlkey": "https://alice.example",
        "snap_url": page_url,
    }


@pytest.fixture
def raw_pages() -> pd.DataFrame:
    homepage = (
        "Alice Candidate. We support public schools health care jobs and safe "
        "communities for every family."
    )
    issues = (
        "Policy Priorities. Our campaign will protect clean water improve roads "
        "and strengthen local democratic institutions."
    )
    return pd.DataFrame(
        [
            _raw_row(
                date="20201102120000",
                page_type="homepage",
                text=homepage,
                page_url="https://alice.example/",
            ),
            # A duplicate page body at a second timestamp tests exact-page hash
            # multiplicity separately from the distinct-hash count.
            _raw_row(
                date="20201102130000",
                page_type="homepage",
                text=homepage,
                page_url="https://alice.example/",
            ),
            _raw_row(
                date="20201102130000",
                page_type="issues",
                text=issues,
                page_url="https://alice.example/issues",
            ),
            # This page remains in the coverage rows but does not survive the
            # ICPSR clean-text rule (fewer than ten words, no sentence).
            _raw_row(
                date="20201104120000",
                page_type="other",
                text="loading",
                page_url="https://alice.example/loading",
            ),
        ]
    )


def _build(raw_pages: pd.DataFrame) -> tuple[pd.DataFrame, FakeTopicPipeline]:
    pipe = FakeTopicPipeline()
    ngrams = (
        {"alice": 0.25, "candidate": 0.5, "campaign": 0.75},
        {"alice", "candidate", "campaign"},
    )
    result = build_snapshot_day_rows(
        raw_pages,
        ngrams=ngrams,
        topic_model=(pipe, lambda text: text, TOPIC_NAMES),
        topic_names=TOPIC_NAMES,
    )
    return result, pipe


def test_unique_key_reconstruction_timing_and_status(raw_pages: pd.DataFrame) -> None:
    result, pipe = _build(raw_pages)
    key = ["candidate_cycle_id", "snap_day", "page_scope"]
    assert not result.duplicated(key).any()
    assert len(result) == 5

    for (_, day), rows in result.groupby(["candidate_cycle_id", "snap_day"]):
        all_row = rows[rows.page_scope.eq("all")].iloc[0]
        detail = rows[~rows.page_scope.eq("all")]
        assert len(detail) >= 1
        for column in (
            "n_pages_observed",
            "n_clean_valid_pages",
            "raw_n_char_total",
            "raw_n_words_total",
            "clean_n_char_total",
            "clean_n_words_total",
            "clean_n_tags_total",
            "clean_n_clean_tags_total",
        ):
            assert int(all_row[column]) == int(detail[column].sum())
        assert detail.n_pages_all_scopes.eq(all_row.n_pages_observed).all()

    pre = result[(result.snap_day == 20201102) & result.page_scope.eq("all")].iloc[0]
    assert pre.election_day == 20201103
    assert pre.days_to_election == 1
    assert bool(pre.pre_election)
    assert not bool(pre.post_election)
    assert pre.n_capture_timestamps == 2
    assert pre.first_capture_timestamp == "20201102120000"
    assert pre.last_capture_timestamp == "20201102130000"
    assert pre.page_types_present == ["homepage", "issues"]
    assert pre.source_hosts == ["alice.example"]
    assert len(pre.page_hashes_sha256) == 3
    assert pre.n_distinct_page_hashes == 2
    assert all(len(value) == 64 for value in pre.page_hashes_sha256)
    assert len(pre.day_digest_sha256) == 64

    post = result[(result.snap_day == 20201104) & result.page_scope.eq("all")].iloc[0]
    assert post.days_to_election == -1
    assert not bool(post.pre_election)
    assert bool(post.post_election)
    assert post.measure_status == "insufficient_clean_text"
    assert post.topic_status == "insufficient_clean_text"
    assert post[list(MEAN_COLUMNS)].isna().all()
    assert post[topic_columns(TOPIC_NAMES)].isna().all()

    supported = result.topic_status.eq("observed")
    topic_sums = result.loc[supported, topic_columns(TOPIC_NAMES)].sum(axis=1)
    np.testing.assert_allclose(topic_sums, 1.0, atol=1e-12, rtol=0)
    # Day 1 has all, homepage, and issues documents. Day 2 has no clean text.
    assert len(pipe.documents) == 3


def test_daily_means_match_independent_page_then_day_calculation(
    raw_pages: pd.DataFrame,
) -> None:
    result, _ = _build(raw_pages)
    observed = result[
        result.page_scope.eq("all") & result.measure_status.eq("observed")
    ]

    independent = []
    ngrams = (
        {"alice": 0.25, "candidate": 0.5, "campaign": 0.75},
        {"alice", "candidate", "campaign"},
    )
    for text in raw_pages.text_snap_content:
        cleaned = icpsr_clean_text(text)
        measures = page_measures(cleaned["text"], 200, ngrams, clean=False)
        if measures["n_char"] <= 0:
            continue
        independent.append(
            {
                "icpsr_n_char": measures["n_char"],
                "icpsr_n_words": measures["n_words"],
                "icpsr_n_tags": cleaned["n_tags"],
                "icpsr_n_clean_tags": cleaned["n_clean_tags"],
                "icpsr_ttr_approx": measures["ttr"],
                "icpsr_mattr_approx": measures["mattr"],
                "icpsr_entropy_approx": measures["entropy"],
            }
        )
    expected = pd.DataFrame(independent).mean()
    actual = observed[list(MEAN_COLUMNS)].mean()
    np.testing.assert_allclose(actual, expected, atol=1e-12, rtol=0)


def test_hashes_topics_and_rows_are_invariant_to_input_order(
    raw_pages: pd.DataFrame,
) -> None:
    first, _ = _build(raw_pages)
    shuffled, _ = _build(raw_pages.sample(frac=1, random_state=20260822))
    key = ["candidate_cycle_id", "snap_day", "page_scope"]
    first = first.sort_values(key).reset_index(drop=True)
    shuffled = shuffled.sort_values(key).reset_index(drop=True)
    pd.testing.assert_frame_equal(first, shuffled, check_dtype=False)


def test_election_day_is_in_inclusive_preelection_period(
    raw_pages: pd.DataFrame,
) -> None:
    election_day_page = raw_pages.iloc[[0]].copy()
    election_day_page["date"] = "20201103120000"
    result, _ = _build(election_day_page)
    all_row = result[result.page_scope.eq("all")].iloc[0]
    assert all_row.days_to_election == 0
    assert bool(all_row.pre_election)
    assert not bool(all_row.post_election)


def _write_candidate_row_groups(path: Path, frames: list[pd.DataFrame]) -> None:
    first = pa.Table.from_pandas(frames[0], preserve_index=False)
    writer = pq.ParquetWriter(path, first.schema)
    try:
        writer.write_table(first)
        for frame in frames[1:]:
            writer.write_table(
                pa.Table.from_pandas(frame, schema=first.schema, preserve_index=False)
            )
    finally:
        writer.close()


def test_streaming_file_builder_writes_schema_and_explicit_topic_status(
    tmp_path: Path, raw_pages: pd.DataFrame
) -> None:
    second = raw_pages.copy()
    second["candidate_cycle_id"] = "H0BB00100-2020"
    second["cand_id"] = "H0BB00100"
    second["state"] = "BB"
    second["district_id"] = "BB01"
    corpus = tmp_path / "raw.parquet"
    output = tmp_path / "panel_snapshot_day.parquet"
    _write_candidate_row_groups(corpus, [raw_pages, second])

    stats = build_snapshot_day_panel(
        corpus,
        output,
        topic_names=TOPIC_NAMES,
    )
    assert stats == {
        "input_pages": 8,
        "candidate_years": 2,
        "snapshot_rows": 10,
    }
    assert output.exists() and output.stat().st_size > 0
    assert stat.S_IMODE(output.stat().st_mode) == 0o644
    panel = pq.read_table(output).to_pandas()
    assert "text_snap_content" not in panel
    assert len(topic_columns(TOPIC_NAMES)) == 31
    assert set(topic_columns(TOPIC_NAMES)).issubset(panel.columns)
    assert not panel.duplicated(
        ["candidate_cycle_id", "snap_day", "page_scope"]
    ).any()
    assert set(panel.loc[panel.measure_status.eq("observed"), "topic_status"]) == {
        "not_computed"
    }
    assert panel[topic_columns(TOPIC_NAMES)].isna().all().all()

    with pytest.raises(FileExistsError, match="refusing to overwrite"):
        build_snapshot_day_panel(corpus, output, topic_names=TOPIC_NAMES)


def test_streaming_topic_cache_scores_repeated_documents_once(
    tmp_path: Path, raw_pages: pd.DataFrame
) -> None:
    second = raw_pages.copy()
    second["candidate_cycle_id"] = "H0BB00100-2020"
    second["cand_id"] = "H0BB00100"
    second["state"] = "BB"
    second["district_id"] = "BB01"
    corpus = tmp_path / "raw.parquet"
    output = tmp_path / "panel.parquet"
    _write_candidate_row_groups(corpus, [raw_pages, second])
    pipe = FakeTopicPipeline()

    build_snapshot_day_panel(
        corpus,
        output,
        topic_model=(pipe, lambda text: text, TOPIC_NAMES),
        topic_names=TOPIC_NAMES,
        topic_batch_size=1,
    )

    # Each candidate produces the same all/homepage/issues documents. The
    # second row group reuses the first group's three probability vectors.
    assert len(pipe.documents) == 3


def test_split_candidate_row_groups_are_rejected(
    tmp_path: Path, raw_pages: pd.DataFrame
) -> None:
    corpus = tmp_path / "split.parquet"
    output = tmp_path / "panel.parquet"
    _write_candidate_row_groups(corpus, [raw_pages.iloc[:2], raw_pages.iloc[2:]])
    with pytest.raises(ValueError, match="split across Parquet row groups"):
        build_snapshot_day_panel(corpus, output, topic_names=TOPIC_NAMES)
    assert not output.exists()


def test_raw_count_mismatch_is_not_silently_accepted(raw_pages: pd.DataFrame) -> None:
    broken = raw_pages.copy()
    broken.loc[0, "n_char"] += 1
    with pytest.raises(ValueError, match="do not reconstruct"):
        build_snapshot_day_rows(broken, topic_names=TOPIC_NAMES)


def test_page_hash_is_exact_sha256_of_preserved_text(raw_pages: pd.DataFrame) -> None:
    result, _ = _build(raw_pages.iloc[[0]])
    expected = hashlib.sha256(
        raw_pages.iloc[0].text_snap_content.encode("utf-8")
    ).hexdigest()
    assert result.loc[result.page_scope.eq("all"), "page_hashes_sha256"].iloc[0] == [
        expected
    ]
