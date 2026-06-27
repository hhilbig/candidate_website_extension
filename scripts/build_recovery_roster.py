#!/usr/bin/env python3
"""
Build a recovery mini-roster for one office+year.

Extracts the candidate URLs whose CDX query failed after 3 attempts
(transient Wayback "connection refused") from the year log, matches them
back to roster rows by re-running the scraper's own `_clean_campaign_url`
(byte-identical to what produced the failures), and writes a mini-roster of
just those rows. Re-running the scraper on that mini-roster with a fresh
progress file recovers the candidates a full-roster resume would re-fail.

Usage (from repo root, venv active):
    python scripts/build_recovery_roster.py --office senate --year 2024

Output: data/rosters/roster_{office}_{year}_recovery.csv
Exits non-zero if any failed URL cannot be matched to a roster row.
"""
import argparse
import os
import re
import sys

import pandas as pd

# Allow `python scripts/build_recovery_roster.py` from the repo root by putting
# the repo root (this file's parent's parent) on the path before importing src.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.scrape_wayback import _clean_campaign_url

FAIL_RE = re.compile(r"CDX query failed after 3 attempts for (\S+)")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--office", required=True, choices=["house", "senate"])
    ap.add_argument("--year", required=True, type=int)
    args = ap.parse_args()

    log_path = f"logs/{args.office}_{args.year}.log"
    roster_path = f"data/rosters/roster_{args.office}_{args.year}.csv"
    out_path = f"data/rosters/roster_{args.office}_{args.year}_recovery.csv"
    tag = f"[{args.office} {args.year}]"

    # 1. Failed URLs from the log (exact strings the scraper passed to CDX).
    failed = []
    with open(log_path) as f:
        for line in f:
            m = FAIL_RE.search(line.rstrip("\n"))
            if m:
                failed.append(m.group(1).strip())
    failed_set = set(failed)
    print(f"{tag} failed-URL log lines: {len(failed)}  unique: {len(failed_set)}")

    if not failed_set:
        print(f"{tag} no CDX failures in {log_path} — nothing to recover.")
        return 0

    # 2. Roster rows whose cleaned website_url matches a failed URL.
    roster = pd.read_csv(roster_path)
    roster["_cleaned"] = roster["website_url"].map(_clean_campaign_url)
    matched = roster[roster["_cleaned"].isin(failed_set)].copy()

    matched_cleaned = set(matched["_cleaned"].dropna())
    unmatched = failed_set - matched_cleaned
    print(f"{tag} roster rows matched: {len(matched)}  "
          f"distinct cleaned URLs matched: {len(matched_cleaned)}")

    if unmatched:
        print(f"{tag} ERROR {len(unmatched)} failed URLs matched NO roster row:")
        for u in sorted(unmatched):
            print(f"    {u}")
        print(f"{tag} refusing to write a partial recovery roster — investigate.")
        return 1

    # 3. Write the mini-roster (drop helper col; keep canonical schema).
    matched = matched.drop(columns=["_cleaned"])
    matched.to_csv(out_path, index=False)
    print(f"{tag} wrote {len(matched)} rows -> {out_path}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
