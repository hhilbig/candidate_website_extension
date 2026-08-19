#!/usr/bin/env python3
"""Validate cross-file integrity of a staged release."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from scripts.build_panel import collapse_candidate

DATA_FILES = {
    "raw_corpus.parquet", "panel_candidate_year.csv",
    "panel_icpsr_compat.csv", "candidate_crosswalk.csv",
    "release_roster.csv",
}
EXPECTED_CANDIDATE_YEARS = 7_353
EXPECTED_ROSTER_ROWS = 9_848
EXPECTED_BALLOT_CANDIDATES = 3_032
EXPECTED_RAW_ROWS = 799_058


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_topics(coded: pd.DataFrame) -> None:
    base = [c for c in coded if c.startswith("icpsr_topic_")
            and not c.endswith("_home")]
    home = [c for c in coded if c.startswith("icpsr_topic_")
            and c.endswith("_home")]
    assert len(base) == len(home) == 31
    for cols, support_cols, expected_missing in (
        (base, ["icpsr_n_char", "icpsr_n_words"], 231),
        (home, ["icpsr_n_char_home", "icpsr_n_words_home"], 395),
    ):
        support = coded[support_cols].notna().all(axis=1)
        assert coded[support_cols].notna().nunique(axis=1).eq(1).all()
        assert coded.loc[support, cols].notna().all(axis=None)
        assert coded.loc[~support, cols].isna().all(axis=None)
        sums = coded.loc[support, cols].sum(axis=1)
        assert np.allclose(sums, 1, atol=1e-8)
        assert int((~support).sum()) == expected_missing


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("release_dir", type=Path)
    a = ap.parse_args()
    base = a.release_dir
    missing = DATA_FILES - {p.name for p in base.iterdir() if p.is_file()}
    assert not missing, f"missing release files: {sorted(missing)}"

    panel = pd.read_csv(base / "panel_candidate_year.csv", low_memory=False)
    coded = pd.read_csv(base / "panel_icpsr_compat.csv", low_memory=False)
    crosswalk = pd.read_csv(base / "candidate_crosswalk.csv", low_memory=False)
    roster = pd.read_csv(base / "release_roster.csv", low_memory=False)
    products = (panel, coded, crosswalk)
    ids = [set(d.candidate_cycle_id) for d in products]

    assert len(panel) == len(coded) == len(crosswalk) == EXPECTED_CANDIDATE_YEARS
    assert len(roster) == EXPECTED_ROSTER_ROWS
    assert int(coded.on_ballot.sum()) == EXPECTED_BALLOT_CANDIDATES
    assert ids[0] == ids[1] == ids[2]
    assert all(not d.candidate_cycle_id.duplicated().any() for d in products)
    assert not roster.candidate_cycle_id.duplicated().any()
    assert roster.captured.sum() == len(panel)
    assert ids[0] <= set(roster.candidate_cycle_id)
    captured_roster = roster[roster.captured].set_index("candidate_cycle_id")
    panel_ballot = coded.set_index("candidate_cycle_id")[["on_ballot", "general_votes"]]
    captured_roster = captured_roster.reindex(panel_ballot.index)
    assert captured_roster.on_ballot.equals(panel_ballot.on_ballot)
    votes_equal = (pd.to_numeric(captured_roster.general_votes, errors="coerce")
                   .fillna(-1).eq(pd.to_numeric(panel_ballot.general_votes,
                                               errors="coerce").fillna(-1)))
    assert votes_equal.all()
    assert coded.icpsr_compatible.equals(coded.on_ballot)
    assert panel.loc[~panel.on_ballot, "stage"].isna().all()
    assert panel.loc[~panel.on_ballot, "data_source"].eq("election_year_wayback").all()
    assert panel.loc[panel.on_ballot, "stage"].eq(2).all()
    assert panel.loc[panel.on_ballot, "data_source"].eq("general_wayback").all()
    regular = panel.universe_source.eq("same_year_fec")
    assert panel.loc[regular, "cand_election_yr"].astype(int).eq(
        panel.loc[regular, "year"]).all()
    shared = ["candidate_cycle_id", "cand_id", "cand_election_yr", "cand_status",
              "universe_source", "on_ballot", "stage", "data_source"]
    reference = panel[shared].sort_values("candidate_cycle_id").reset_index(drop=True)
    for product in (coded, crosswalk):
        other = product[shared].sort_values("candidate_cycle_id").reset_index(drop=True)
        pd.testing.assert_frame_equal(reference, other, check_dtype=False)
    validate_topics(coded)

    raw = pq.ParquetFile(base / "raw_corpus.parquet")
    raw_ids = set()
    tag_rows = 0
    nonzero_tags = 0
    reconstructed = []
    panel_index = panel.set_index("candidate_cycle_id")
    for i in range(raw.num_row_groups):
        d = raw.read_row_group(i).to_pandas()
        raw_ids.update(d.candidate_cycle_id.tolist())
        text = d.text_snap_content.fillna("").astype(str)
        assert d.n_char.eq(text.map(len)).all()
        assert d.n_words.eq(text.map(lambda value: len(value.split()))).all()
        tags = d.n_tags
        clean_tags = d.n_clean_tags
        assert tags.notna().all() and clean_tags.notna().all()
        assert tags.ge(1).all()
        assert clean_tags.ge(0).all()
        assert clean_tags.le(tags).all()
        tag_rows += len(tags)
        nonzero_tags += int(tags.gt(0).sum())
        candidate_ids = d.candidate_cycle_id.unique()
        assert len(candidate_ids) == 1
        candidate_id = candidate_ids[0]
        meta = panel_index.loc[candidate_id]
        panel_input = d.copy()
        for col in ("candidate", "state", "district", "office", "party"):
            panel_input[col] = meta[col]
        row = collapse_candidate(panel_input, "#+#")
        assert row is not None
        row["candidate_cycle_id"] = candidate_id
        reconstructed.append(row)
    assert raw_ids == ids[0]
    assert raw.metadata.num_rows == tag_rows == EXPECTED_RAW_ROWS
    assert nonzero_tags == raw.metadata.num_rows

    rebuilt = pd.DataFrame(reconstructed).set_index("candidate_cycle_id")
    check_cols = ["sel_date", "n_snapshots_available", "n_pages", "page_types",
                  "urlkey", "text", "n_char", "n_words", "text_quality"]
    expected = panel_index.loc[rebuilt.index, check_cols].copy()
    actual = rebuilt[check_cols].copy()
    expected["sel_date"] = expected.sel_date.astype(str).str.removesuffix(".0")
    actual["sel_date"] = actual.sel_date.astype(str).str.removesuffix(".0")
    pd.testing.assert_frame_equal(expected, actual, check_dtype=False)

    manifest = json.loads((base / "manifest.json").read_text())
    assert {x["file"] for x in manifest} == DATA_FILES
    for item in manifest:
        path = base / item["file"]
        assert item["bytes"] == path.stat().st_size
        assert item["sha256"] == sha256(path)
    print(f"PASS: {len(panel):,} candidate-years; {raw.metadata.num_rows:,} raw rows; "
          f"{len(roster):,} roster rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
