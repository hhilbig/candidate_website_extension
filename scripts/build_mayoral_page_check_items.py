"""Write the Hive items for the page-level campaign check (scripts/hive/classify_mayoral_page_campaign.py).

One item is one usable page of a release candidate: the page that the site check credits to a finalist, inside the
365-day window, not excluded and not a duplicate. The item ID is the page_id, so labels join back to the corpus.

Usage:
  python scripts/build_mayoral_page_check_items.py --release build/<release folder> --output <items.jsonl>
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
    args = parser.parse_args()
    pages = pd.read_parquet(args.release / "mayoral_page_corpus.parquet",
                            columns=["page_id", "candidate_election_id", "original_url", "capture_timestamp", "text",
                                     "release_usable"])
    pages = pages[pages["release_usable"]]
    finalists = pd.read_csv(args.release / "mayoral_finalists.csv", dtype=str).set_index("candidate_election_id")
    with args.output.open("w") as handle:
        for page in pages.itertuples():
            person = finalists.loc[page.candidate_election_id]
            stamp = page.capture_timestamp
            handle.write(json.dumps({
                "item_id": page.page_id, "candidate_name": person["candidate_name"], "city_name": person["city_name"],
                "state_abb": person["state_abb"], "election_date": person["election_date"],
                "capture_date": f"{stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}", "url": page.original_url,
                "text": page.text.replace("#+#", "\n")}) + "\n")
    print(json.dumps({"items": len(pages), "finalists": int(pages["candidate_election_id"].nunique())}))


if __name__ == "__main__":
    main()
