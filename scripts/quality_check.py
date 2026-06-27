#!/usr/bin/env python3
"""
Quality / coverage report for one scraped office+year.

Reads the roster (denominator), the per-candidate output CSVs (page-level
text), and the progress file (snapshot-level status). Prints capture rate,
capture rate among valid-URL candidates, snapshots/candidate, pages/snapshot,
text richness, and corruption metrics.

Usage (from repo root, venv active):
    python scripts/quality_check.py --office senate --year 2024

The candidate data may live as an uncompressed dir
(data/snapshots/{office}/{year}/) — extract a tarball there first if needed.
"""
import argparse
import glob
import os
import sys

import pandas as pd

# Allow `python scripts/quality_check.py` from the repo root by putting the
# repo root (this file's parent's parent) on the path before importing src.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.scrape_wayback import _clean_campaign_url


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--office", required=True, choices=["house", "senate"])
    ap.add_argument("--year", required=True, type=int)
    args = ap.parse_args()

    roster_path = f"data/rosters/roster_{args.office}_{args.year}.csv"
    out_dir = f"data/snapshots/{args.office}/{args.year}"
    progress_path = f"data/progress/progress_roster_{args.office}_{args.year}.csv"

    roster = pd.read_csv(roster_path)
    n_roster = len(roster)
    n_valid = int(roster["website_url"].map(_clean_campaign_url).notna().sum())

    files = sorted(glob.glob(os.path.join(out_dir, "*.csv")))
    n_captured = len(files)

    cols = ["candidate", "date", "snap_url", "page_type", "n_char", "n_words"]
    frames = []
    for fp in files:
        try:
            frames.append(pd.read_csv(fp, usecols=lambda c: c in cols))
        except Exception as e:  # noqa: BLE001
            print(f"  WARN unreadable {os.path.basename(fp)}: {e}")
    pages = pd.concat(frames, ignore_index=True) if frames else pd.DataFrame(columns=cols)

    n_pages = len(pages)
    nchar = pd.to_numeric(pages["n_char"], errors="coerce").fillna(0) if n_pages else pd.Series(dtype=float)

    print(f"\n===== {args.office} {args.year} =====")
    print(f"Roster candidates            : {n_roster}")
    print(f"Valid-URL candidates         : {n_valid} ({100*n_valid/n_roster:.1f}%)")
    print(f"Captured (>=1 snap)          : {n_captured}  "
          f"({100*n_captured/n_roster:.1f}% of roster, "
          f"{100*n_captured/n_valid:.1f}% of valid-URL)")
    print(f"Total page rows              : {n_pages}")
    if n_pages:
        snaps = pages.groupby("candidate")["date"].nunique()
        ppsnap = pages.groupby(["candidate", "date"]).size()
        print(f"Snapshots/candidate          : median {snaps.median():.0f}  "
              f"mean {snaps.mean():.2f}  max {snaps.max()}")
        print(f"Pages/snapshot               : median {ppsnap.median():.0f}  "
              f"mean {ppsnap.mean():.2f}  max {ppsnap.max()}")
        print(f"Text richness (n_char)       : median {nchar.median():.0f}  "
              f"mean {nchar.mean():.0f}  max {nchar.max():.0f}")
        lt50 = int((nchar < 50).sum())
        print(f"Low-text rows (<50 chars)    : {lt50}  ({100*lt50/n_pages:.1f}%)")
        if "page_type" in pages.columns:
            pt = pages["page_type"].value_counts()
            print("Page-type mix                : " +
                  ", ".join(f"{k}={v}" for k, v in pt.head(6).items()))

    if os.path.exists(progress_path):
        prog = pd.read_csv(progress_path)

        def col_sum(name):
            if name not in prog.columns:
                return "n/a"
            return int(pd.to_numeric(prog[name], errors="coerce").fillna(0).sum())

        print(f"Progress rows (snapshots)    : {len(prog)}  "
              f"complete={col_sum('scrape_complete')}  error={col_sum('scrape_error')}")


if __name__ == "__main__":
    main()
