"""Write the Hive items for the page-level campaign check (scripts/hive/classify_mayoral_page_campaign.py).

One item is one usable page of a release candidate: the page that the site check credits to a finalist, inside the
365-day window, not excluded and not a duplicate. The item ID is the page_id, so labels join back to the corpus.
With --primaries (v6), the items also include the primary-period pages: pages that pass the page rules and the site
check, captured in the 365 days up to the primary, of a primary candidate or of the finalist linked to one. Their
election_date is the general election date (the prompt counts a primary of the same race as this campaign), or the
primary date where no general election is recorded (DC 2026). With --labels, pages that already carry a label are
left out.

Usage:
  python scripts/build_mayoral_page_check_items.py --release build/<release folder> --output <items.jsonl>
      [--primaries] [--labels release_site_check/page_campaign_check.csv]
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--release", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--primaries", action="store_true")
    parser.add_argument("--labels", type=Path, help="CSV page_id,label; labeled pages are left out")
    args = parser.parse_args()
    pages = pd.read_parquet(args.release / "mayoral_page_corpus.parquet",
                            columns=["page_id", "candidate_election_id", "original_url", "capture_timestamp", "text",
                                     "release_usable", "excluded", "site_check_passed"])
    people = pd.read_csv(args.release / "mayoral_finalists.csv", dtype=str, keep_default_na=False)
    people = people[["candidate_election_id", "candidate_name", "city_name", "state_abb", "election_date"]]
    keep = pages["release_usable"].copy()
    if args.primaries:
        primary = pd.read_csv(args.release / "mayoral_primary_candidates.csv", dtype=str, keep_default_na=False)
        general = pd.read_csv(args.release / "mayoral_elections.csv", dtype=str).set_index("election_id")["election_date"]
        own = primary.assign(race_date=[general.get(g, d) if g else d for g, d in
                                        zip(primary["general_election_id"], primary["election_date"])])
        people = pd.concat([people, own[["candidate_election_id", "candidate_name", "city_name", "state_abb"]]
                            .assign(election_date=own["race_date"])])
        source = own["finalist_candidate_election_id"].where(own["finalist_candidate_election_id"] != "",
                                                             own["candidate_election_id"])
        primary_date = dict(zip(source, own["election_date"]))
        day = pd.to_datetime(pages["capture_timestamp"].str[:8], format="%Y%m%d")
        until = pd.to_datetime(pages["candidate_election_id"].map(primary_date))
        in_period = (day <= until) & (day > until - pd.Timedelta(days=365))
        keep = keep | (in_period & ~pages["excluded"] & pages["site_check_passed"])
    if args.labels:
        labeled = set(pd.read_csv(args.labels, dtype=str)["page_id"])
        keep = keep & ~pages["page_id"].isin(labeled)
    pages = pages[keep]
    people = people.drop_duplicates("candidate_election_id").set_index("candidate_election_id")
    with args.output.open("w") as handle:
        for page in pages.itertuples():
            person = people.loc[page.candidate_election_id]
            stamp = page.capture_timestamp
            handle.write(json.dumps({
                "item_id": page.page_id, "candidate_name": person["candidate_name"], "city_name": person["city_name"],
                "state_abb": person["state_abb"], "election_date": person["election_date"],
                "capture_date": f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}", "url": page.original_url,
                "text": page.text.replace("#+#", "\n")}) + "\n")
    print(json.dumps({"items": len(pages), "candidates": int(pages["candidate_election_id"].nunique())}))

if __name__ == "__main__":
    main()
