#!/usr/bin/env python3
"""Drop Senate candidate-years from state-years that had no Senate election.

Senate seats rotate in thirds, so only about 33 states vote each cycle. The
roster was built from FEC candidate files, which list anyone with a registered
committee, and the builder treated the file year as the election year. The
result was 1,305 roster rows (25.7% of the Senate roster) in state-years with no
Senate race at all, of which 657 were scraped and coded.

Al Franken appears for MN 2010, 2016 and 2022; he was elected in 2008 and
resigned in 2018, and Minnesota held no Senate election in any of those years.

The race list comes from the MIT Election Data and Science Lab's statewide
Senate returns (Harvard Dataverse doi:10.7910/DVN/PEJ5QU, CC0), including
special elections. That file is complete over our window: 33-36 state races per
cycle, all 50 states present, each appearing 7-9 times across 2002-2024.

House needs no equivalent filter, since every district votes every cycle.

Usage: python scripts/drop_no_race_senate.py [--dry-run]
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parent.parent
SENATE_RETURNS = REPO / "data/external/1976-2024-senate-state.csv"
DELIV = REPO / "data/deliverable"


def senate_races() -> set[tuple[int, str]]:
    """(year, state) pairs that actually held a Senate general election."""
    s = pd.read_csv(SENATE_RETURNS, low_memory=False, encoding="latin-1")
    s.columns = [c.strip() for c in s.columns]
    g = s[s.stage.astype(str).str.upper() == "GEN"]
    return set(zip(g.year.astype(int), g.state_po.astype(str)))


def in_scope(df: pd.DataFrame, races: set, office_col="office",
             year_col="year", state_col="state") -> pd.Series:
    """True unless the row is a Senate candidate-year with no election."""
    is_sen = df[office_col].astype(str).str.lower() == "senate"
    key = list(zip(df[year_col].astype(int), df[state_col].astype(str)))
    has_race = pd.Series([k in races for k in key], index=df.index)
    return ~is_sen | has_race


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    races = senate_races()
    print(f"Senate state-years with a general election: {len(races):,}\n")

    # ---- the flat tables -------------------------------------------------
    targets = [
        ("release_roster.csv", DELIV / "release_roster.csv"),
        ("panel_icpsr_compat.csv", DELIV / "panel_icpsr_compat.csv"),
        ("panel_candidate_year.csv", DELIV / "panel_candidate_year.csv"),
        ("candidate_crosswalk.csv",
         REPO / "quality_reports/coverage_audit/csv/candidate_crosswalk.csv"),
    ]
    keep_keys: set[tuple] | None = None
    for label, path in targets:
        if not path.exists():
            print(f"{label:28} MISSING, skipped")
            continue
        df = pd.read_csv(path, low_memory=False)
        keep = in_scope(df, races)
        print(f"{label:28} {len(df):>7,} -> {int(keep.sum()):>7,}  "
              f"(drops {int((~keep).sum()):,})")
        if label == "panel_icpsr_compat.csv":
            k = df.loc[keep, ["candidate_icpsr", "state", "office", "year"]]
            keep_keys = set(map(tuple, k.astype(str).values))
        if not a.dry_run:
            shutil.copy2(path, path.with_suffix(path.suffix + ".prefilter"))
            df[keep].to_csv(path, index=False)

    # ---- the corpus ------------------------------------------------------
    corpus = DELIV / "raw_corpus_icpsr.parquet"
    if corpus.exists() and keep_keys is not None:
        pf = pq.ParquetFile(corpus)
        out = corpus.with_suffix(".filtered.parquet")
        kept = dropped = 0
        writer = None
        if not a.dry_run:
            writer = pq.ParquetWriter(out, pf.schema_arrow)
        for i in range(pf.num_row_groups):
            t = pf.read_row_group(i)
            d = t.to_pandas()
            if d.empty:
                continue
            k = (str(d.candidate_icpsr.iloc[0]), str(d.state.iloc[0]),
                 str(d.office.iloc[0]), str(int(d.year.iloc[0])))
            if k in keep_keys:
                kept += len(d)
                if writer is not None:
                    writer.write_table(t)
            else:
                dropped += len(d)
            if (i + 1) % 3000 == 0:
                print(f"  corpus row group {i+1}/{pf.num_row_groups}", flush=True)
        if writer is not None:
            writer.close()
        print(f"\n{'raw_corpus_icpsr.parquet':28} {kept+dropped:>7,} -> {kept:>7,} "
              f"page rows (drops {dropped:,})")
        if not a.dry_run:
            corpus.rename(corpus.with_suffix(".parquet.prefilter"))
            out.rename(corpus)

    if a.dry_run:
        print("\ndry run, nothing written")
    else:
        print("\nwritten. originals kept alongside with a .prefilter suffix")
    return 0


if __name__ == "__main__":
    sys.exit(main())
