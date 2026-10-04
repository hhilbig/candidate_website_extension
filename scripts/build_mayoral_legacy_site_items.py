"""Build Hive attribution items from a finished legacy-lead site scrape (stage 3L).

One item per target (finalist x domain): the root-most page saved within 365 days
before the election, plus the longest other in-window page, as text. The output feeds
scripts/hive/classify_mayoral_site_attribution.py. Run where the run directory lives.
"""
from __future__ import annotations

import argparse
import csv
import json
from datetime import date, datetime
from pathlib import Path


def pick(rows: list[dict], election: str) -> list[dict]:
    day = date.fromisoformat(election)
    def offset(row):
        return (datetime.strptime((row["effective_timestamp"] or row["timestamp"])[:8], "%Y%m%d").date() - day).days
    before = [r for r in rows if -365 <= offset(r) <= 0]
    if not before:
        return []
    root = min(before, key=lambda r: (len(r["original_url"]), -offset(r)))
    others = sorted((r for r in before if r["capture_id"] != root["capture_id"]), key=lambda r: -int(r["text_characters"]))
    return [root] + others[:1]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--targets", type=Path, required=True)
    parser.add_argument("--run-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    targets = list(csv.DictReader(args.targets.open()))
    by_target = {}
    for row in csv.DictReader((args.run_dir / "fetch_attempts.csv").open()):
        if row["fetch_status"] == "fetched" and int(row["text_characters"] or 0) > 100:
            by_target.setdefault(row["target_id"], []).append(row)
    written = no_pages = 0
    with args.output.open("w") as handle:
        for target in targets:
            chosen = pick(by_target.get(target["target_id"], []), target["election_date"])
            if not chosen:
                no_pages += 1
                continue
            handle.write(json.dumps({
                "item_id": target["target_id"], "candidate_name": target["full_name"], "city_name": target["geo_name"],
                "state_abb": target["state_abb"], "election_date": target["election_date"], "url": target["site_root_url"],
                "captures": [{"timestamp": r["effective_timestamp"] or r["timestamp"], "url": r["original_url"],
                              "text": (args.run_dir / r["text_path"]).read_text(errors="replace")[:6000]} for r in chosen],
                "gold": ""}) + "\n")
            written += 1
    print(json.dumps({"targets": len(targets), "items": written, "no_in_window_pages_before_election": no_pages}))


if __name__ == "__main__":
    main()
