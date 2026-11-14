import pandas as pd
import pytest

from scripts.icpsr_replicate_coding import queue_topic_document
from scripts.rebuild_release import (
    _blank_unsupported_topics,
    _correct_raw_counts,
    _correct_raw_snapshot_count,
    _prepare_output_dir,
)


def test_raw_counts_are_recomputed_from_preserved_text():
    rows = pd.DataFrame({
        "text_snap_content": ["One two#+#Three", None],
        "n_char": [999, 999],
        "n_words": [999, 999],
        "n_tags": [0, 0],
        "n_clean_tags": [0, 0],
    })
    out = _correct_raw_counts(rows)
    assert out.n_char.tolist() == [15, 0]
    assert out.n_words.tolist() == [2, 0]
    assert out.n_tags.tolist() == [2, 1]
    assert out.n_clean_tags.tolist() == [0, 0]


def test_raw_snapshot_count_is_recomputed_after_date_filtering():
    rows = pd.DataFrame({
        "date": ["20140105010101", "20140105010101"],
        "n_snap": [109, 109],
    })
    out = _correct_raw_snapshot_count(rows)
    assert out.n_snap.tolist() == [1, 1]


def test_empty_topic_documents_are_not_queued():
    keys, kinds, docs = [], [], []
    assert not queue_topic_document(keys, kinds, docs, "A", "all", "  ")
    assert queue_topic_document(keys, kinds, docs, "B", "home", "text")
    assert (keys, kinds, docs) == (["B"], ["home"], ["text"])


def test_topic_scores_are_blank_without_supporting_text():
    topic_names = [f"icpsr_topic_{i}" for i in range(31)]
    home_names = [f"{name}_home" for name in topic_names]
    data = {
        "icpsr_n_char": [10.0, pd.NA],
        "icpsr_n_words": [2.0, pd.NA],
        "icpsr_n_char_home": [pd.NA, 5.0],
        "icpsr_n_words_home": [pd.NA, 1.0],
    }
    data.update({name: [1 / 31, 1 / 31] for name in topic_names + home_names})
    out = _blank_unsupported_topics(pd.DataFrame(data))
    assert out.loc[0, topic_names].notna().all()
    assert out.loc[1, topic_names].isna().all()
    assert out.loc[0, home_names].isna().all()
    assert out.loc[1, home_names].notna().all()


def test_rebuild_refuses_a_nonempty_output_directory(tmp_path):
    out = tmp_path / "release"
    out.mkdir()
    (out / "stale.csv").write_text("x\n1\n")
    with pytest.raises(FileExistsError, match="not empty"):
        _prepare_output_dir(out)
