#!/usr/bin/env python3
"""Validate cross-file integrity of a staged release."""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

DATA_FILES = {
    "raw_corpus.parquet", "panel_candidate_year.csv",
    "panel_icpsr_compat.csv", "candidate_crosswalk.csv",
    "release_roster.csv",
}


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

    raw = pq.ParquetFile(base / "raw_corpus.parquet")
    raw_ids = set()
    for i in range(raw.num_row_groups):
        t = raw.read_row_group(i, columns=["candidate_cycle_id"])
        raw_ids.update(t.column(0).to_pylist())
    assert raw_ids == ids[0]

    manifest = json.loads((base / "manifest.json").read_text())
    assert {x["file"] for x in manifest} == DATA_FILES
    print(f"PASS: {len(panel):,} candidate-years; {raw.metadata.num_rows:,} raw rows; "
          f"{len(roster):,} roster rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
