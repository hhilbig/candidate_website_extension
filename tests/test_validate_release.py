"""Focused unit tests for release-integrity helpers."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from scripts.build_snapshot_day_panel import build_snapshot_day_rows
from scripts.validate_release import (
    _assert_frame_values_equal,
    _provenance_scalar_equal,
    _provenance_page_url,
    _scope_summary,
    validate_topics,
)


def _coded_topics() -> pd.DataFrame:
    base = [f"icpsr_topic_topic_{index}" for index in range(31)]
    home = [f"{column}_home" for column in base]
    rows = pd.DataFrame({
        "icpsr_n_char": [10.0, np.nan],
        "icpsr_n_words": [2.0, np.nan],
        "icpsr_n_char_home": [5.0, np.nan],
        "icpsr_n_words_home": [1.0, np.nan],
    })
    for index, column in enumerate(base):
        rows[column] = [1.0 if index == 0 else 0.0, np.nan]
    for index, column in enumerate(home):
        rows[column] = [1.0 if index == 1 else 0.0, np.nan]
    return rows


def test_coded_topic_validation_allows_data_driven_missingness() -> None:
    validate_topics(_coded_topics())


def test_coded_topic_validation_rejects_partial_support() -> None:
    rows = _coded_topics()
    rows.loc[1, "icpsr_n_words"] = 1
    with pytest.raises(AssertionError, match="partially missing"):
        validate_topics(rows)


def test_snapshot_digest_is_invariant_to_input_order() -> None:
    rows = pd.DataFrame([
        {
            "candidate_cycle_id": "H1AA00001-2020",
            "year": 2020,
            "date": "20200102010101",
            "page_type": "issues",
            "snap_url": "https://web.archive.org/web/20200102010101/https://example.org/issues",
            "urlkey": "example.org",
            "n_char": 4,
            "n_words": 1,
            "text_snap_content": "jobs",
        },
        {
            "candidate_cycle_id": "H1AA00001-2020",
            "year": 2020,
            "date": "20200102020202",
            "page_type": "homepage",
            "snap_url": "https://web.archive.org/web/20200102020202/https://example.org/",
            "urlkey": "example.org",
            "n_char": 5,
            "n_words": 1,
            "text_snap_content": "hello",
        },
    ])
    first = _scope_summary(rows, len(rows))
    second = _scope_summary(rows.iloc[::-1], len(rows))
    assert first == second
    assert first["page_types_present"] == ("homepage", "issues")

    built = build_snapshot_day_rows(rows, topic_names=[])
    all_scope = built.loc[built.page_scope.eq("all")].iloc[0]
    assert all_scope.day_digest_sha256 == first["day_digest_sha256"]
    assert all_scope.n_capture_timestamps == first["n_capture_timestamps"]
    assert all_scope.raw_n_char_total == first["raw_n_char_total"]
    assert all_scope.raw_n_words_total == first["raw_n_words_total"]
    assert tuple(all_scope.page_types_present) == first["page_types_present"]


def test_provenance_url_falls_back_when_snap_url_is_invalid() -> None:
    value = _provenance_page_url("::::", "www.Example.org/path/")
    assert value == "https://example.org/path"


def test_frame_value_comparison_ignores_equivalent_index_storage_dtype() -> None:
    expected = pd.DataFrame(
        {"value": [1]},
        index=pd.Index(pd.array(["H1AA00001-2020"], dtype="string")),
    )
    actual = pd.DataFrame(
        {"value": [1]},
        index=pd.Index(np.array(["H1AA00001-2020"], dtype=object)),
    )
    _assert_frame_values_equal(expected, actual)


def test_provenance_timestamp_comparison_normalizes_csv_float_storage() -> None:
    assert _provenance_scalar_equal(
        "first_capture_timestamp",
        20180121231517.0,
        "20180121231517",
    )
    assert _provenance_scalar_equal("first_capture_day", 20180121.0, "20180121")
