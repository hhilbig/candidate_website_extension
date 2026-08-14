#!/usr/bin/env python3
"""Build the release roster: every candidate we attempted, and what happened.

A public corpus of 10,601 captured candidate-years is not interpretable without
its denominator. Users need to know who we tried and failed to capture, and
whether the failure was "no website found" or "website found but nothing
archived" -- those imply different selection processes.

Output columns:
  candidate, state, district, office, year, party   -- the FEC roster
  website_url                                       -- URL found, if any
  has_url                                           -- a usable campaign URL was found
  captured                                          -- >=1 page of text was scraped

Usage: python scripts/build_release_roster.py [--out PATH]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))
from src.scrape_wayback import _clean_campaign_url  # noqa: E402
from drop_no_race_senate import senate_races, in_scope  # noqa: E402
ROSTERS = REPO / "data/rosters"
CROSSWALK = REPO / "quality_reports/coverage_audit/csv/candidate_crosswalk.csv"
KEY = ["candidate", "state", "office", "year"]


def load_rosters() -> pd.DataFrame:
    frames = []
    for p in sorted(ROSTERS.glob("roster_*.csv")):
        if "test" in p.stem or "recovery" in p.stem:
            continue
        df = pd.read_csv(p, dtype={"district": str})
        df["source_file"] = p.name
        frames.append(df)
    if not frames:
        sys.exit(f"no rosters found under {ROSTERS}")
    return pd.concat(frames, ignore_index=True)


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out", type=Path,
                    default=REPO / "data/deliverable/release_roster.csv")
    a = ap.parse_args()

    r = load_rosters()
    print(f"rosters: {len(r):,} candidate-years across "
          f"{r.source_file.nunique()} office-years")

    # Senate seats rotate in thirds, so only about 33 states vote each cycle.
    # The FEC candidate files list anyone with a registered committee, so a
    # roster built from them carries candidates in state-years with no election
    # at all -- Al Franken appears for MN 2010, 2016 and 2022. Drop them.
    before = len(r)
    r = r[in_scope(r, senate_races())].reset_index(drop=True)
    if before != len(r):
        print(f"dropped {before - len(r):,} Senate rows in state-years with no "
              f"Senate election")

    # "Usable URL" must mean exactly what the scraper meant, so use its own
    # cleaner rather than a lookalike test. It rejects placeholders like
    # https://none, email addresses, and social-media links.
    r["has_url"] = r.website_url.map(
        lambda u: _clean_campaign_url(u) is not None)

    # The FEC lists some candidates in two districts in the same cycle (filed in
    # both, or a redistricting artifact). The corpus keys without district, so
    # keep one row per candidate-year and prefer the informative one.
    before = len(r)
    r = (r.sort_values(["has_url"], ascending=False)
           .drop_duplicates(subset=KEY, keep="first")
           .sort_values(KEY).reset_index(drop=True))
    if before != len(r):
        print(f"collapsed {before - len(r)} duplicate district rows "
              f"to the corpus grain ({KEY})")

    cw = pd.read_csv(CROSSWALK, low_memory=False)
    captured = set(
        cw.candidate.astype(str).str.strip().str.lower() + "|"
        + cw.state.astype(str).str.strip().str.upper() + "|"
        + cw.office.astype(str) + "|" + cw.year.astype(str))
    rk = (r.candidate.astype(str).str.strip().str.lower() + "|"
          + r.state.astype(str).str.strip().str.upper() + "|"
          + r.office.astype(str) + "|" + r.year.astype(str))
    r["captured"] = rk.isin(captured)

    dup = r.duplicated(subset=KEY).sum()
    print(f"duplicate keys in roster: {dup}")
    print(f"with a usable URL: {r.has_url.sum():,} ({100*r.has_url.mean():.1f}%)")
    print(f"captured:          {r.captured.sum():,} "
          f"({100*r.captured.mean():.1f}% of roster, "
          f"{100*r[r.has_url].captured.mean():.1f}% of those with a URL)")

    print(f"\n{'office':<7} {'year':>5} {'roster':>7} {'has_url':>8} "
          f"{'captured':>9} {'% of URL':>9}")
    for (o, y), g in r.groupby(["office", "year"]):
        pct = 100 * g[g.has_url].captured.mean() if g.has_url.any() else float("nan")
        print(f"{o:<7} {y:>5} {len(g):>7,} {g.has_url.sum():>8,} "
              f"{g.captured.sum():>9,} {pct:>8.1f}%")

    cols = ["candidate", "state", "district", "office", "year", "party",
            "website_url", "has_url", "captured"]
    out = r[[c for c in cols if c in r.columns]]
    a.out.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(a.out, index=False)
    print(f"\nwrote {a.out}  ({len(out):,} rows, {len(out.columns)} cols)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
